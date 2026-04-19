"""
rollex_utils.py — Loader for Rollex parquets.
Fetches from GitHub (Rollex-New repo) at runtime; falls back to local parquets.
"""

import io
import pandas as pd
from pathlib import Path

_GITHUB_BASE = "https://raw.githubusercontent.com/virataryaa/Rollex-New/main/Database"
_LOCAL_DB    = Path(__file__).parent.parent / "Database"
AVAILABLE    = ["KC", "LRC", "CC", "LCC", "SB", "CT", "LSU", "OJ"]

# Rollex-New repo uses RC for Robusta; COT app uses LRC
_GITHUB_KEY  = {"LRC": "RC"}


def load_rollex(comm: str) -> pd.DataFrame:
    comm      = comm.upper()
    gh_key    = _GITHUB_KEY.get(comm, comm)
    gh_url    = f"{_GITHUB_BASE}/rollex_{gh_key}.parquet"
    local_path = _LOCAL_DB / f"rollex_{comm}.parquet"

    try:
        import urllib.request
        with urllib.request.urlopen(gh_url, timeout=10) as r:
            data = r.read()
        df = pd.read_parquet(io.BytesIO(data))
    except Exception:
        if not local_path.exists():
            raise FileNotFoundError(
                f"GitHub fetch failed and no local fallback for {comm}"
            )
        df = pd.read_parquet(local_path)

    df.index.name = "Date"
    return df
