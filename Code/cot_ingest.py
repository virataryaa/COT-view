"""
Hardmine — COT CFTC Ingest
===========================
Downloads CIT and Disaggregated COT data from LSEG and saves to parquet.

Usage:
    python cot_ingest.py            # incremental (default)
    python cot_ingest.py --full     # full history from 2010-01-01

Output:
    ../Database/cot_cit.parquet
    ../Database/cot_disagg.parquet

Columns:
  CIT:    Date, Commodity, Comm Long, Comm Short, Spec Long, Spec Short,
          Spec Spread, Index Long, Index Short, Non Rep Long, Non Rep Short,
          Total OI, Px
  Disagg: Date, Commodity, Comm Long, Comm Short, Spec Long, Spec Short,
          Swap Spread, Other Long, Other Short, Non Rep Long, Non Rep Short,
          Total OI, Px
"""

import argparse
import datetime
import logging
import sys
from pathlib import Path

import pandas as pd

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "cot_ingest.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
DB_DIR       = Path(__file__).parent.parent / "Database"
CIT_FILE     = DB_DIR / "cot_cit.parquet"
DISAGG_FILE  = DB_DIR / "cot_disagg.parquet"

START_FULL   = "2010-01-01"

# ══════════════════════════════════════════════════════════════════════════════
# RIC MAPS
# ══════════════════════════════════════════════════════════════════════════════
# CIT — CFTC (ICE + CBOT softs)
# Columns: Comm Long/Short | Spec Long/Short/Spread | Index Long/Short | Non Rep Long/Short | Total OI | Px
CIT_COMMODITIES = {
    "CC": {
        "Comm Long":     "4073732CLNG",
        "Comm Short":    "4073732CSHT",
        "Spec Long":     "4073732NLNG",
        "Spec Short":    "4073732NSHT",
        "Spec Spread":   "4073732NSPD",
        "Index Long":    "4073732PLNG",
        "Index Short":   "4073732PSHT",
        "Non Rep Long":  "4073732RLNG",
        "Non Rep Short": "4073732RSHT",
        "Total OI":      "3CFTC073732OI",
        "Px RIC":        "CCc2",
    },
    "KC": {
        "Comm Long":     "4083731CLNG",
        "Comm Short":    "4083731CSHT",
        "Spec Long":     "4083731NLNG",
        "Spec Short":    "4083731NSHT",
        "Spec Spread":   "4083731NSPD",
        "Index Long":    "4083731PLNG",
        "Index Short":   "4083731PSHT",
        "Non Rep Long":  "4083731RLNG",
        "Non Rep Short": "4083731RSHT",
        "Total OI":      "3CFTC083731OI",
        "Px RIC":        "KCc2",
    },
    "SB": {
        "Comm Long":     "4080732CLNG",
        "Comm Short":    "4080732CSHT",
        "Spec Long":     "4080732NLNG",
        "Spec Short":    "4080732NSHT",
        "Spec Spread":   "4080732NSPD",
        "Index Long":    "4080732PLNG",
        "Index Short":   "4080732PSHT",
        "Non Rep Long":  "4080732RLNG",
        "Non Rep Short": "4080732RSHT",
        "Total OI":      "3CFTC080732OI",
        "Px RIC":        "SBc2",
    },
    "CT": {
        "Comm Long":     "4033661CLNG",
        "Comm Short":    "4033661CSHT",
        "Spec Long":     "4033661NLNG",
        "Spec Short":    "4033661NSHT",
        "Spec Spread":   "4033661NSPD",
        "Index Long":    "4033661PLNG",
        "Index Short":   "4033661PSHT",
        "Non Rep Long":  "4033661RLNG",
        "Non Rep Short": "4033661RSHT",
        "Total OI":      "3CFTC033661OI",
        "Px RIC":        "CTc2",
    },
}

CIT_COT_COLS = [
    "Comm Long", "Comm Short",
    "Spec Long", "Spec Short", "Spec Spread",
    "Index Long", "Index Short",
    "Non Rep Long", "Non Rep Short",
    "Total OI",
]

# Disaggregated — ICE LIFFE (LRC, LCC)
# Columns: Comm Long/Short | Spec Long/Short/Spread | Other Long/Short | Non Rep Long/Short | Total OI | Px
DISAGG_COMMODITIES = {
    "LRC": {
        "Comm Long":     "3LIFLRCPLNG",
        "Comm Short":    "3LIFLRCPSHT",
        "Spec Long":     "3LIFLRCMLNG",
        "Spec Short":    "3LIFLRCMSHT",
        "Swap Spread":   "3LIFLRCSSPD",
        "Other Long":    "3LIFLRCOLNG",
        "Other Short":   "3LIFLRCOSHT",
        "Non Rep Long":  "3LIFLRCRLNG",
        "Non Rep Short": "3LIFLRCRSHT",
        "Total OI":      "3LIFLRCOI",
        "Px RIC":        "LRCc2",
    },
    "LCC": {
        "Comm Long":     "3LIFLCCPLNG",
        "Comm Short":    "3LIFLCCPSHT",
        "Spec Long":     "3LIFLCCMLNG",
        "Spec Short":    "3LIFLCCMSHT",
        "Swap Spread":   "3LIFLCCSSPD",
        "Other Long":    "3LIFLCCOLNG",
        "Other Short":   "3LIFLCCOSHT",
        "Non Rep Long":  "3LIFLCCRLNG",
        "Non Rep Short": "3LIFLCCRSHT",
        "Total OI":      "3LIFLCCOI",
        "Px RIC":        "LCCc2",
    },
}

DISAGG_COT_COLS = [
    "Comm Long", "Comm Short",
    "Spec Long", "Spec Short", "Swap Spread",
    "Other Long", "Other Short",
    "Non Rep Long", "Non Rep Short",
    "Total OI",
]


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def _get_history(ld, universe: list, fields: list, start: str, end: str) -> pd.DataFrame:
    """Wrapper around ld.get_history — returns DataFrame with RIC columns."""
    df = ld.get_history(
        universe=universe,
        fields=fields,
        start=start,
        end=end,
        interval="daily",
        count=10000,
    )
    # Flatten MultiIndex columns if present
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [ric for ric, _ in df.columns]
    return df


def fetch_one(ld, comm: str, ric_map: dict, cot_cols: list,
              start: str, end: str, cot_field: str = "COMM_LAST") -> pd.DataFrame:
    """Fetch COT + price for one commodity, return long-format single DataFrame."""
    px_ric   = ric_map["Px RIC"]
    cot_rics = {col: ric for col, ric in ric_map.items() if col != "Px RIC"}

    log.info("  Fetching COT for %s (%d RICs, field=%s) ...", comm, len(cot_rics), cot_field)
    df_cot = _get_history(ld, list(cot_rics.values()), [cot_field], start, end)
    # Rename the fetched field column to the column name
    df_cot = df_cot.rename(columns=lambda c: c if c in cot_rics.values() else c)
    ric_to_col = {v: k for k, v in cot_rics.items()}
    df_cot = df_cot.rename(columns=ric_to_col)

    log.info("  Fetching Px for %s (%s) ...", comm, px_ric)
    df_px = _get_history(ld, [px_ric], ["TRDPRC_1"], start, end)
    df_px.columns = ["Px"]

    # Merge price onto COT (left join so we keep COT dates)
    df = df_cot.join(df_px, how="left")

    # Drop rows where all COT columns are NaN (non-reporting days)
    df = df.dropna(subset=cot_cols, how="all")

    # Ensure all expected columns exist
    for col in cot_cols + ["Px"]:
        if col not in df.columns:
            df[col] = float("nan")

    df.index.name = "Date"
    df = df.reset_index()
    df.insert(0, "Commodity", comm)

    log.info("  -> %s: %d rows", comm, len(df))
    return df


def fetch_all(ld, comm_map: dict, cot_cols: list,
              start: str, end: str, cot_field: str = "COMM_LAST") -> pd.DataFrame:
    frames = []
    for comm, ric_map in comm_map.items():
        try:
            frames.append(fetch_one(ld, comm, ric_map, cot_cols, start, end, cot_field))
        except Exception as e:
            log.error("  ERROR fetching %s: %s", comm, e)
    if not frames:
        raise RuntimeError("No data fetched for any commodity.")
    return pd.concat(frames, ignore_index=True)


def incremental_start(existing: pd.DataFrame) -> str:
    """Return start of latest year in DB (re-fetch full latest year to catch revisions)."""
    latest = pd.to_datetime(existing["Date"]).max()
    return f"{latest.year}-01-01"


def merge_and_dedup(old: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    merged = pd.concat([old, new], ignore_index=True)
    before = len(merged)
    merged = merged.drop_duplicates(subset=["Date", "Commodity"], keep="last")
    merged = merged.sort_values(["Commodity", "Date"]).reset_index(drop=True)
    log.info("  Dedup: %d -> %d rows (-%d)", before, len(merged), before - len(merged))
    return merged


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="COT CFTC Ingest")
    parser.add_argument("--full", action="store_true",
                        help=f"Full history from {START_FULL}")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("COT Ingest  |  %s", datetime.datetime.now().strftime("%Y-%m-%d %H:%M"))
    log.info("Mode: %s", "FULL" if args.full else "INCREMENTAL")

    DB_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.date.today().isoformat()

    import lseg.data as ld
    ld.open_session()
    log.info("LSEG session opened.")

    try:
        # ── CIT ──────────────────────────────────────────────────────────────
        log.info("--- CIT ---")
        if args.full or not CIT_FILE.exists():
            start_cit = START_FULL
        else:
            start_cit = incremental_start(pd.read_parquet(CIT_FILE, columns=["Date"]))
            log.info("Incremental CIT from %s", start_cit)

        new_cit = fetch_all(ld, CIT_COMMODITIES, CIT_COT_COLS, start_cit, today)

        if CIT_FILE.exists() and not args.full:
            old_cit = pd.read_parquet(CIT_FILE)
            cit_df  = merge_and_dedup(old_cit, new_cit)
        else:
            cit_df = new_cit.sort_values(["Commodity", "Date"]).reset_index(drop=True)

        cit_df.to_parquet(CIT_FILE, engine="pyarrow", index=False)
        log.info("CIT saved -> %s  |  %d rows", CIT_FILE, len(cit_df))

        # ── DISAGG ───────────────────────────────────────────────────────────
        log.info("--- Disaggregated ---")
        if args.full or not DISAGG_FILE.exists():
            start_dis = START_FULL
        else:
            start_dis = incremental_start(pd.read_parquet(DISAGG_FILE, columns=["Date"]))
            log.info("Incremental Disagg from %s", start_dis)

        # LIFFE disagg RICs use TRDPRC_1 (not COMM_LAST)
        new_dis = fetch_all(ld, DISAGG_COMMODITIES, DISAGG_COT_COLS, start_dis, today,
                            cot_field="TRDPRC_1")

        if DISAGG_FILE.exists() and not args.full:
            old_dis  = pd.read_parquet(DISAGG_FILE)
            dis_df   = merge_and_dedup(old_dis, new_dis)
        else:
            dis_df = new_dis.sort_values(["Commodity", "Date"]).reset_index(drop=True)

        dis_df.to_parquet(DISAGG_FILE, engine="pyarrow", index=False)
        log.info("Disagg saved -> %s  |  %d rows", DISAGG_FILE, len(dis_df))

    finally:
        ld.close_session()
        log.info("LSEG session closed.")

    log.info("=" * 60)


if __name__ == "__main__":
    main()
