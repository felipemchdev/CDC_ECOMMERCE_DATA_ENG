from __future__ import annotations

import pandas as pd

from cdc_ecommerce.gold.builder import build_gold
from cdc_ecommerce.utils.io import read_parquet_or_empty, write_parquet


def test_gold_metrics_from_small_fixture(settings) -> None:
    users = pd.DataFrame(
        [
            {"user_id": "U1", "name": "A", "email": "a@example.com", "region": "US", "created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-01T00:00:00Z", "is_deleted": False},
            {"user_id": "U2", "name": "B", "email": "b@example.com", "region": "BR", "created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-01T00:00:00Z", "is_deleted": False},
        ]
    )

    products = pd.DataFrame(
        [
            {"product_id": "P1", "name": "Widget", "category": "electronics", "price": 10.0, "currency": "USD", "created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-01T00:00:00Z", "is_deleted": False},
            {"product_id": "P2", "name": "Book", "category": "books", "price": 30.0, "currency": "USD", "created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-01T00:00:00Z", "is_deleted": False},
        ]
    )

    orders = pd.DataFrame(
        [
            {"order_id": "O1", "user_id": "U1", "status": "paid", "order_ts": "2026-01-01T10:00:00Z", "updated_at": "2026-01-01T10:00:00Z", "is_deleted": False},
            {"order_id": "O2", "user_id": "U1", "status": "refunded", "order_ts": "2026-01-01T11:00:00Z", "updated_at": "2026-01-01T12:00:00Z", "is_deleted": False},
            {"order_id": "O3", "user_id": "U2", "status": "shipped", "order_ts": "2026-01-02T09:00:00Z", "updated_at": "2026-01-02T10:00:00Z", "is_deleted": False},
            {"order_id": "O4", "user_id": "U1", "status": "shipped", "order_ts": "2026-01-02T11:00:00Z", "updated_at": "2026-01-02T11:30:00Z", "is_deleted": False},
        ]
    )

    order_items = pd.DataFrame(
        [
            {"order_item_id": "OI1", "order_id": "O1", "product_id": "P1", "qty": 2, "unit_price": 10.0, "created_at": "2026-01-01T10:00:01Z"},
            {"order_item_id": "OI2", "order_id": "O2", "product_id": "P2", "qty": 1, "unit_price": 30.0, "created_at": "2026-01-01T11:00:01Z"},
            {"order_item_id": "OI3", "order_id": "O3", "product_id": "P1", "qty": 1, "unit_price": 15.0, "created_at": "2026-01-02T09:00:01Z"},
            {"order_item_id": "OI4", "order_id": "O4", "product_id": "P2", "qty": 1, "unit_price": 25.0, "created_at": "2026-01-02T11:00:01Z"},
        ]
    )

    payments = pd.DataFrame(
        [
            {"payment_id": "PM1", "order_id": "O1", "method": "card", "amount": 20.0, "status": "captured", "created_at": "2026-01-01T10:01:00Z", "updated_at": "2026-01-01T10:01:00Z"},
            {"payment_id": "PM2", "order_id": "O2", "method": "card", "amount": 30.0, "status": "refunded", "created_at": "2026-01-01T11:01:00Z", "updated_at": "2026-01-01T12:01:00Z"},
        ]
    )

    write_parquet(users, settings.silver_root / "users.parquet")
    write_parquet(products, settings.silver_root / "products.parquet")
    write_parquet(orders, settings.silver_root / "orders.parquet")
    write_parquet(order_items, settings.silver_root / "order_items.parquet")
    write_parquet(payments, settings.silver_root / "payments.parquet")

    counts = build_gold(settings)

    daily_gmv = read_parquet_or_empty(settings.gold_root / "daily_gmv.parquet")
    refund_rate = read_parquet_or_empty(settings.gold_root / "refund_rate.parquet")
    top_products = read_parquet_or_empty(settings.gold_root / "top_products.parquet")
    retention = read_parquet_or_empty(settings.gold_root / "basic_retention.parquet")

    assert counts["daily_gmv"] == 2
    assert daily_gmv.loc[daily_gmv["date"] == "2026-01-01", "gmv"].iloc[0] == 50.0
    assert daily_gmv.loc[daily_gmv["date"] == "2026-01-02", "gmv"].iloc[0] == 40.0

    assert refund_rate.loc[refund_rate["date"] == "2026-01-01", "refund_rate"].iloc[0] == 0.5

    day1_top = top_products[top_products["date"] == "2026-01-01"].sort_values("revenue", ascending=False)
    assert day1_top.iloc[0]["product_id"] == "P2"

    day2_retention = retention[retention["date"] == "2026-01-02"].iloc[0]
    assert day2_retention["active_users"] == 2
    assert day2_retention["returning_users"] == 1
    assert day2_retention["retention_rate"] == 0.5
