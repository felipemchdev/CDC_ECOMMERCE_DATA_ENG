from __future__ import annotations

import json

import pandas as pd

from cdc_ecommerce.silver.merge import SilverMerger
from cdc_ecommerce.utils.io import read_parquet_or_empty



def _events(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    df["event_ts"] = pd.to_datetime(df["event_ts"], utc=True)
    return df


def test_merge_is_idempotent(settings) -> None:
    merger = SilverMerger(settings)
    events = _events(
        [
            {
                "event_id": "e-001",
                "entity": "users",
                "operation": "I",
                "event_ts": "2026-01-01T00:00:00Z",
                "pk": "U000001",
                "payload": json.dumps(
                    {
                        "user_id": "U000001",
                        "name": "User One",
                        "email": "user1@example.com",
                        "region": "US",
                        "created_at": "2026-01-01T00:00:00Z",
                        "updated_at": "2026-01-01T00:00:00Z",
                        "is_deleted": False,
                    }
                ),
                "schema_version": 1,
            },
            {
                "event_id": "e-002",
                "entity": "users",
                "operation": "U",
                "event_ts": "2026-01-01T01:00:00Z",
                "pk": "U000001",
                "payload": json.dumps(
                    {
                        "updated_at": "2026-01-01T01:00:00Z",
                        "email": "user1.r1@example.com",
                    }
                ),
                "schema_version": 1,
            },
        ]
    )

    first = merger.merge_events(events)
    before = read_parquet_or_empty(settings.silver_root / "users.parquet")

    second = merger.merge_events(events)
    after = read_parquet_or_empty(settings.silver_root / "users.parquet")

    assert first["processed_events_count"] == 2
    assert second["processed_events_count"] == 0
    pd.testing.assert_frame_equal(before.sort_index(axis=1), after.sort_index(axis=1), check_like=True)


def test_updates_use_latest_event_ts(settings) -> None:
    merger = SilverMerger(settings)

    newer = _events(
        [
            {
                "event_id": "e-100",
                "entity": "users",
                "operation": "I",
                "event_ts": "2026-01-01T01:00:00Z",
                "pk": "U000100",
                "payload": json.dumps(
                    {
                        "user_id": "U000100",
                        "name": "User Hundred",
                        "email": "new@example.com",
                        "region": "US",
                        "created_at": "2026-01-01T01:00:00Z",
                        "updated_at": "2026-01-01T01:00:00Z",
                        "is_deleted": False,
                    }
                ),
                "schema_version": 1,
            },
            {
                "event_id": "e-101",
                "entity": "users",
                "operation": "U",
                "event_ts": "2026-01-01T02:00:00Z",
                "pk": "U000100",
                "payload": json.dumps(
                    {
                        "updated_at": "2026-01-01T02:00:00Z",
                        "email": "latest@example.com",
                    }
                ),
                "schema_version": 1,
            },
        ]
    )
    merger.merge_events(newer)

    late = _events(
        [
            {
                "event_id": "e-102",
                "entity": "users",
                "operation": "U",
                "event_ts": "2026-01-01T01:30:00Z",
                "pk": "U000100",
                "payload": json.dumps(
                    {
                        "updated_at": "2026-01-01T01:30:00Z",
                        "email": "old@example.com",
                    }
                ),
                "schema_version": 1,
            }
        ]
    )
    merger.merge_events(late)

    users = read_parquet_or_empty(settings.silver_root / "users.parquet")
    row = users[users["user_id"] == "U000100"].iloc[0]
    assert row["email"] == "latest@example.com"


def test_deletes_mark_rows_as_deleted(settings) -> None:
    merger = SilverMerger(settings)
    events = _events(
        [
            {
                "event_id": "e-200",
                "entity": "products",
                "operation": "I",
                "event_ts": "2026-01-02T00:00:00Z",
                "pk": "P000001",
                "payload": json.dumps(
                    {
                        "product_id": "P000001",
                        "name": "Item",
                        "category": "electronics",
                        "price": 10.0,
                        "currency": "USD",
                        "created_at": "2026-01-02T00:00:00Z",
                        "updated_at": "2026-01-02T00:00:00Z",
                        "is_deleted": False,
                    }
                ),
                "schema_version": 1,
            },
            {
                "event_id": "e-201",
                "entity": "products",
                "operation": "D",
                "event_ts": "2026-01-02T02:00:00Z",
                "pk": "P000001",
                "payload": json.dumps(
                    {
                        "updated_at": "2026-01-02T02:00:00Z",
                        "is_deleted": True,
                        "delete_mode": "soft",
                    }
                ),
                "schema_version": 1,
            },
        ]
    )

    merger.merge_events(events)

    products = read_parquet_or_empty(settings.silver_root / "products.parquet")
    row = products[products["product_id"] == "P000001"].iloc[0]
    assert bool(row["is_deleted"]) is True


def test_late_events_do_not_corrupt_order_state(settings) -> None:
    merger = SilverMerger(settings)

    ontime = _events(
        [
            {
                "event_id": "e-300",
                "entity": "orders",
                "operation": "I",
                "event_ts": "2026-01-03T10:00:00Z",
                "pk": "O00010001",
                "payload": json.dumps(
                    {
                        "order_id": "O00010001",
                        "user_id": "U000001",
                        "status": "paid",
                        "order_ts": "2026-01-03T10:00:00Z",
                        "updated_at": "2026-01-03T10:00:00Z",
                        "is_deleted": False,
                    }
                ),
                "schema_version": 1,
            },
            {
                "event_id": "e-301",
                "entity": "orders",
                "operation": "U",
                "event_ts": "2026-01-03T11:00:00Z",
                "pk": "O00010001",
                "payload": json.dumps(
                    {
                        "updated_at": "2026-01-03T11:00:00Z",
                        "status": "shipped",
                    }
                ),
                "schema_version": 1,
            },
        ]
    )
    merger.merge_events(ontime)

    late = _events(
        [
            {
                "event_id": "e-302",
                "entity": "orders",
                "operation": "U",
                "event_ts": "2026-01-03T10:30:00Z",
                "pk": "O00010001",
                "payload": json.dumps(
                    {
                        "updated_at": "2026-01-03T10:30:00Z",
                        "status": "cancelled",
                    }
                ),
                "schema_version": 1,
            }
        ]
    )
    merger.merge_events(late)

    orders = read_parquet_or_empty(settings.silver_root / "orders.parquet")
    row = orders[orders["order_id"] == "O00010001"].iloc[0]
    assert row["status"] == "shipped"
