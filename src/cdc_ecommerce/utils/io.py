from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def read_parquet_or_empty(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    ensure_parent(path)
    df.to_parquet(path, index=False)


def append_json(path: Path, payload: dict) -> None:
    ensure_parent(path)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, default=str))
        handle.write("\n")
