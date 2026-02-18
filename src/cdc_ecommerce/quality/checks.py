from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from cdc_ecommerce.config import Settings
from cdc_ecommerce.silver.merge import ENTITIES
from cdc_ecommerce.utils.io import read_parquet_or_empty


def run_quality_checks(settings: Settings, processed_events_count: int) -> dict[str, int]:
    silver_tables = {entity: read_parquet_or_empty(settings.silver_root / f"{entity}.parquet") for entity in ENTITIES}

    for entity in ("users", "products", "orders"):
        if silver_tables[entity].empty:
            raise ValueError(f"Quality check failed: {entity} current-state table is empty")

    users = silver_tables["users"]
    products = silver_tables["products"]
    orders = silver_tables["orders"]
    order_items = silver_tables["order_items"]
    payments = silver_tables["payments"]

    if not orders.empty and not users.empty:
        missing_users = set(orders["user_id"].dropna().astype(str)) - set(users["user_id"].dropna().astype(str))
        if missing_users:
            raise ValueError(f"Quality check failed: orders reference missing users ({len(missing_users)} keys)")

    if not order_items.empty:
        missing_orders = set(order_items["order_id"].dropna().astype(str)) - set(orders["order_id"].dropna().astype(str))
        if missing_orders:
            raise ValueError(f"Quality check failed: order_items reference missing orders ({len(missing_orders)} keys)")

        missing_products = set(order_items["product_id"].dropna().astype(str)) - set(products["product_id"].dropna().astype(str))
        if missing_products:
            raise ValueError(f"Quality check failed: order_items reference missing products ({len(missing_products)} keys)")

        if (order_items["qty"].fillna(0) <= 0).any():
            raise ValueError("Quality check failed: order_items.qty must be positive")

        if (order_items["unit_price"].fillna(0) < 0).any():
            raise ValueError("Quality check failed: order_items.unit_price must be non-negative")

    if not payments.empty and (payments["amount"].fillna(0) < 0).any():
        raise ValueError("Quality check failed: payments.amount must be non-negative")

    _volume_anomaly_check(settings.metrics_root, processed_events_count)

    return {entity: int(df.shape[0]) for entity, df in silver_tables.items()}


def _volume_anomaly_check(metrics_root: Path, processed_events_count: int) -> None:
    if processed_events_count == 0:
        return

    metrics_root.mkdir(parents=True, exist_ok=True)
    history = []
    for path in sorted(metrics_root.glob("run_*.json"))[-10:]:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            history.append(int(payload.get("processed_events_count", 0)))
        except Exception:
            continue

    if len(history) < 3:
        return

    avg = sum(history) / len(history)
    upper = avg * 3.0
    lower = max(1.0, avg * 0.25)
    if processed_events_count > upper or processed_events_count < lower:
        raise ValueError(
            "Quality check failed: processed event volume outside expected range "
            f"(count={processed_events_count}, lower={lower:.2f}, upper={upper:.2f})"
        )
