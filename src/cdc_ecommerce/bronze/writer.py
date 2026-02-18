from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

from cdc_ecommerce.config import Settings
from cdc_ecommerce.utils.io import write_parquet


def bronze_partition(settings: Settings, run_date: date) -> Path:
    return settings.bronze_root / f"event_date={run_date.isoformat()}"


def write_bronze_batch(events_df: pd.DataFrame, settings: Settings, run_date: date) -> Path:
    partition = bronze_partition(settings, run_date)
    partition.mkdir(parents=True, exist_ok=True)
    batch_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    path = partition / f"batch_{batch_id}.parquet"
    write_parquet(events_df, path)
    return path
