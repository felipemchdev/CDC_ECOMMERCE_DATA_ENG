from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def read_parquet_or_empty(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    with duckdb.connect() as conn:
        return conn.execute("SELECT * FROM read_parquet(?)", [str(path)]).df()


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    ensure_parent(path)
    tmp_path = path.with_suffix(".tmp.parquet")
    with duckdb.connect() as conn:
        conn.register("df_view", df)
        conn.execute("COPY df_view TO ? (FORMAT PARQUET)", [str(tmp_path)])
    tmp_path.replace(path)


def append_json(path: Path, payload: dict) -> None:
    ensure_parent(path)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, default=str))
        handle.write("\n")
