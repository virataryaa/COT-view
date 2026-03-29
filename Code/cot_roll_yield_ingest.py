"""
COT Roll Yield Ingest
=====================
Fetches c2 and c7 settlement prices for all COT commodities via Refinitiv,
computes Roll Yield = (c2 - c7) / c7 * 100, saves to parquet.

Usage:
    python cot_roll_yield_ingest.py            # incremental
    python cot_roll_yield_ingest.py --full     # full from 2010-01-01

Output:
    ../Database/cot_roll_yield.parquet
    Columns: Date, Commodity, c2, c7, RollYield
"""

import argparse
import datetime
import logging
import sys
from pathlib import Path

import pandas as pd

LOG_DIR = Path(__file__).resolve().parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "cot_roll_yield.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

DB_DIR   = Path(__file__).resolve().parent.parent / "Database"
OUT_FILE = DB_DIR / "cot_roll_yield.parquet"
START_FULL = "2010-01-01"

PAIRS = {
    "CC":  ("CCc2",  "CCc7"),
    "KC":  ("KCc2",  "KCc7"),
    "SB":  ("SBc2",  "SBc7"),
    "CT":  ("CTc2",  "CTc7"),
    "LRC": ("LRCc2", "LRCc7"),
    "LCC": ("LCCc2", "LCCc7"),
}


def _fetch(rd, ric: str, start: str, end: str) -> pd.Series:
    """Fetch TR.SETTLEMENTPRICE for one RIC, return Series indexed by Date."""
    df = rd.get_history(
        universe=[ric],
        fields=["TR.SETTLEMENTPRICE"],
        start=start,
        end=end,
        interval="daily",
    )
    df.index = pd.to_datetime(df.index)
    # flatten MultiIndex if needed
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[1] if len(c) > 1 else c[0] for c in df.columns]
    # grab first column regardless of name
    return df.iloc[:, 0].dropna()


def main():
    parser = argparse.ArgumentParser(description="COT Roll Yield Ingest")
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()

    if OUT_FILE.exists() and not args.full:
        existing = pd.read_parquet(OUT_FILE, columns=["Date"])
        latest = pd.to_datetime(existing["Date"]).max()
        start = f"{latest.year}-01-01"
        log.info("Incremental from %s", start)
    else:
        start = START_FULL
        log.info("Full history from %s", start)

    end = datetime.date.today().isoformat()

    import refinitiv.data as rd
    rd.open_session()
    log.info("Session opened.")

    try:
        frames = []
        for comm, (r_c2, r_c7) in PAIRS.items():
            log.info("Fetching %s: %s / %s ...", comm, r_c2, r_c7)
            try:
                s_c2 = _fetch(rd, r_c2, start, end)
                s_c7 = _fetch(rd, r_c7, start, end)

                df = pd.DataFrame({"c2": s_c2, "c7": s_c7}).dropna()
                df["RollYield"] = (df["c2"] - df["c7"]) / df["c7"] * 100
                df.index.name = "Date"
                df = df.reset_index()
                df.insert(0, "Commodity", comm)
                frames.append(df)
                log.info("  → %s: %d rows, latest RY=%.2f%%",
                         comm, len(df), df["RollYield"].iloc[-1])
            except Exception as e:
                log.error("  ERROR %s: %s", comm, e)

        if not frames:
            log.error("No data fetched — aborting.")
            return

        new_df = pd.concat(frames, ignore_index=True)

        if OUT_FILE.exists() and not args.full:
            old_df = pd.read_parquet(OUT_FILE)
            merged = pd.concat([old_df, new_df], ignore_index=True)
            merged = merged.drop_duplicates(subset=["Commodity", "Date"], keep="last")
            merged = merged.sort_values(["Commodity", "Date"]).reset_index(drop=True)
        else:
            merged = new_df.sort_values(["Commodity", "Date"]).reset_index(drop=True)

        DB_DIR.mkdir(parents=True, exist_ok=True)
        merged.to_parquet(OUT_FILE, engine="pyarrow", index=False)
        log.info("Saved → %s  |  %d rows", OUT_FILE, len(merged))

    finally:
        rd.close_session()
        log.info("Session closed.")


if __name__ == "__main__":
    main()
