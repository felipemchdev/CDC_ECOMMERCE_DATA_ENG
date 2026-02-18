from __future__ import annotations

import json
import time
from datetime import date, datetime, timezone

from cdc_ecommerce.bronze.writer import write_bronze_batch
from cdc_ecommerce.config import Settings, get_settings
from cdc_ecommerce.gold.builder import build_gold
from cdc_ecommerce.ingestion.generator import generate_cdc_batch
from cdc_ecommerce.quality.checks import run_quality_checks
from cdc_ecommerce.silver.merge import SilverMerger
from cdc_ecommerce.utils.io import read_parquet_or_empty
from cdc_ecommerce.utils.logging import get_logger

logger = get_logger(__name__)


def run_pipeline_for_date(run_date: date, settings: Settings | None = None) -> dict:
    cfg = settings or get_settings()
    started = time.perf_counter()

    events_df = generate_cdc_batch(
        run_date,
        seed=cfg.seed,
        schema_version=cfg.schema_version,
        simulation_start_date=cfg.simulation_start_date,
    )
    bronze_path = write_bronze_batch(events_df, cfg, run_date)

    silver_merger = SilverMerger(cfg)
    silver_merge_metrics = silver_merger.merge_events(events_df)

    gold_row_counts = build_gold(cfg)
    silver_row_counts = run_quality_checks(cfg, silver_merge_metrics["processed_events_count"])

    finished = datetime.now(timezone.utc)
    runtime_seconds = round(time.perf_counter() - started, 4)

    freshness = {
        "silver": _silver_freshness_iso(cfg),
        "gold": finished.isoformat(),
    }

    metrics = {
        "run_date": run_date.isoformat(),
        "processed_events_count": int(silver_merge_metrics["processed_events_count"]),
        "runtime_seconds": runtime_seconds,
        "output_row_counts": {
            "silver": silver_row_counts,
            "gold": gold_row_counts,
        },
        "freshness": freshness,
        "bronze_batch_path": str(bronze_path),
        "finished_at": finished.isoformat(),
    }

    _write_metrics(cfg, metrics)
    logger.info("pipeline_run_completed", extra=metrics)
    return metrics


def backfill(start: date, end: date, settings: Settings | None = None) -> list[dict]:
    if end < start:
        raise ValueError("end date must be greater than or equal to start date")

    cfg = settings or get_settings()
    current = start
    outputs: list[dict] = []
    while current <= end:
        outputs.append(run_pipeline_for_date(current, cfg))
        current = current.fromordinal(current.toordinal() + 1)
    return outputs


def _silver_freshness_iso(settings: Settings) -> str | None:
    latest = None
    for path in settings.silver_root.glob("*.parquet"):
        if path.name.startswith("_"):
            continue
        try:
            frame = read_parquet_or_empty(path)
        except Exception:
            continue
        if frame.empty or "_last_event_ts" not in frame.columns:
            continue
        ts = frame["_last_event_ts"].dropna()
        if ts.empty:
            continue
        current_latest = ts.max()
        if latest is None or current_latest > latest:
            latest = current_latest
    return None if latest is None else str(latest)


def _write_metrics(settings: Settings, payload: dict) -> None:
    settings.metrics_root.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    path = settings.metrics_root / f"run_{payload['run_date']}_{stamp}.json"
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
