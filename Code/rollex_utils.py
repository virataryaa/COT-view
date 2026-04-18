"""
rollex_utils.py — Loader for Rollex parquets (COT repo copy).
Parquets live in ../Database/ relative to this file.
"""

import pandas as pd
from pathlib import Path

_DB          = Path(__file__).parent.parent / "Database"
AVAILABLE    = ["KC", "RC", "CC", "LCC", "SB", "CT", "LSU", "OJ"]
_ALIASES     = {"LRC": "RC"}
_STALE_HOURS = 24


def load_rollex(comm: str) -> pd.DataFrame:
    comm = comm.upper()
    comm = _ALIASES.get(comm, comm)
    if comm not in AVAILABLE:
        raise ValueError(f"'{comm}' not recognised. Available: {AVAILABLE}")

    path = _DB / f"rollex_{comm}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"rollex_{comm}.parquet not found in {_DB}")

    df = pd.read_parquet(path)
    df.index.name = "Date"
    return df


def load_all_rollex() -> dict:
    result = {}
    for comm in AVAILABLE:
        try:
            result[comm] = load_rollex(comm)
        except FileNotFoundError as e:
            print(f"[rollex_utils] Skipping {comm}: {e}")
    return result
