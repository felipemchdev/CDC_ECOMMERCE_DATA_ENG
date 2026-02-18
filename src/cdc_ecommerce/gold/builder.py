from __future__ import annotations

from pathlib import Path

import pandas as pd

from cdc_ecommerce.config import Settings
from cdc_ecommerce.utils.io import read_parquet_or_empty, write_parquet


def build_gold(settings: Settings) -> dict[str, int]:
    settings.gold_root.mkdir(parents=True, exist_ok=True)

    users = read_parquet_or_empty(settings.silver_root / "users.parquet")
    products = read_parquet_or_empty(settings.silver_root / "products.parquet")
    orders = read_parquet_or_empty(settings.silver_root / "orders.parquet")
    order_items = read_parquet_or_empty(settings.silver_root / "order_items.parquet")

    daily_gmv = _daily_gmv(orders, order_items)
    orders_by_status = _orders_by_status(orders)
    refund_rate = _refund_rate(orders)
    top_products = _top_products(orders, order_items, products)
    retention = _basic_retention(orders)

    outputs = {
        "daily_gmv": daily_gmv,
        "orders_by_status": orders_by_status,
        "refund_rate": refund_rate,
        "top_products": top_products,
        "basic_retention": retention,
    }

    row_counts: dict[str, int] = {}
    for name, df in outputs.items():
        path = settings.gold_root / f"{name}.parquet"
        write_parquet(df, path)
        row_counts[name] = int(df.shape[0])

    return row_counts


def _normalized_orders(orders: pd.DataFrame) -> pd.DataFrame:
    if orders.empty:
        return orders
    frame = orders.copy()
    frame["order_ts"] = pd.to_datetime(frame["order_ts"], utc=True, errors="coerce")
    frame = frame[frame["order_ts"].notna()]
    frame = frame[~frame.get("is_deleted", False).fillna(False)]
    frame["date"] = frame["order_ts"].dt.date
    return frame


def _daily_gmv(orders: pd.DataFrame, order_items: pd.DataFrame) -> pd.DataFrame:
    if orders.empty or order_items.empty:
        return pd.DataFrame(columns=["date", "gmv", "orders_count"])

    valid_orders = _normalized_orders(orders)
    valid_orders = valid_orders[valid_orders["status"].isin(["paid", "shipped", "refunded"])]
    if valid_orders.empty:
        return pd.DataFrame(columns=["date", "gmv", "orders_count"])

    merged = valid_orders[["order_id", "date"]].merge(order_items[["order_id", "qty", "unit_price"]], on="order_id", how="inner")
    merged["line_revenue"] = merged["qty"].astype(float) * merged["unit_price"].astype(float)

    grouped = (
        merged.groupby("date", as_index=False)
        .agg(gmv=("line_revenue", "sum"), orders_count=("order_id", "nunique"))
        .sort_values("date")
        .reset_index(drop=True)
    )
    grouped["gmv"] = grouped["gmv"].round(2)
    return grouped


def _orders_by_status(orders: pd.DataFrame) -> pd.DataFrame:
    if orders.empty:
        return pd.DataFrame(columns=["date", "status", "count"])

    frame = _normalized_orders(orders)
    grouped = (
        frame.groupby(["date", "status"], as_index=False)
        .agg(count=("order_id", "nunique"))
        .sort_values(["date", "status"])
        .reset_index(drop=True)
    )
    return grouped


def _refund_rate(orders: pd.DataFrame) -> pd.DataFrame:
    if orders.empty:
        return pd.DataFrame(columns=["date", "refund_rate"]) 

    frame = _normalized_orders(orders)
    if frame.empty:
        return pd.DataFrame(columns=["date", "refund_rate"])

    refunded = frame[frame["status"] == "refunded"].groupby("date")["order_id"].nunique().rename("refunded_orders")
    paid_base = frame[frame["status"].isin(["paid", "shipped", "refunded"])].groupby("date")["order_id"].nunique().rename("paid_orders")

    result = pd.concat([refunded, paid_base], axis=1).fillna(0).reset_index()
    result["refund_rate"] = (result["refunded_orders"] / result["paid_orders"].replace(0, pd.NA)).fillna(0).round(6)
    return result[["date", "refund_rate"]].sort_values("date").reset_index(drop=True)


def _top_products(orders: pd.DataFrame, order_items: pd.DataFrame, products: pd.DataFrame) -> pd.DataFrame:
    if orders.empty or order_items.empty or products.empty:
        return pd.DataFrame(columns=["date", "product_id", "product_name", "revenue"])

    valid_orders = _normalized_orders(orders)
    valid_orders = valid_orders[valid_orders["status"].isin(["paid", "shipped", "refunded"])]

    if valid_orders.empty:
        return pd.DataFrame(columns=["date", "product_id", "product_name", "revenue"])

    product_names = products[["product_id", "name"]].rename(columns={"name": "product_name"})
    merged = (
        valid_orders[["order_id", "date"]]
        .merge(order_items[["order_id", "product_id", "qty", "unit_price"]], on="order_id", how="inner")
        .merge(product_names, on="product_id", how="left")
    )
    merged["revenue"] = (merged["qty"].astype(float) * merged["unit_price"].astype(float)).round(2)

    grouped = (
        merged.groupby(["date", "product_id", "product_name"], as_index=False)
        .agg(revenue=("revenue", "sum"))
        .sort_values(["date", "revenue"], ascending=[True, False])
    )
    grouped["rank"] = grouped.groupby("date")["revenue"].rank(method="first", ascending=False)
    result = grouped[grouped["rank"] <= 5].drop(columns=["rank"]).reset_index(drop=True)
    result["revenue"] = result["revenue"].round(2)
    return result


def _basic_retention(orders: pd.DataFrame) -> pd.DataFrame:
    if orders.empty:
        return pd.DataFrame(columns=["date", "active_users", "returning_users", "retention_rate"])

    frame = _normalized_orders(orders)
    frame = frame[frame["status"].isin(["paid", "shipped", "refunded"])]
    if frame.empty:
        return pd.DataFrame(columns=["date", "active_users", "returning_users", "retention_rate"])

    first_order = frame.groupby("user_id", as_index=False).agg(first_order_date=("date", "min"))
    joined = frame[["date", "user_id"]].drop_duplicates().merge(first_order, on="user_id", how="left")
    joined["is_returning"] = joined["first_order_date"] < joined["date"]

    grouped = joined.groupby("date", as_index=False).agg(
        active_users=("user_id", "nunique"),
        returning_users=("is_returning", "sum"),
    )
    grouped["retention_rate"] = (
        grouped["returning_users"] / grouped["active_users"].replace(0, pd.NA)
    ).fillna(0).round(6)
    return grouped.sort_values("date").reset_index(drop=True)
