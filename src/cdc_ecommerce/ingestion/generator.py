from __future__ import annotations

import json
import random
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone

import pandas as pd

START_DATE = date(2026, 1, 1)
COUNTRIES = ["US", "BR", "DE", "FR", "GB", "CA", "MX", "ES", "IT", "NL"]
CATEGORIES = ["electronics", "fashion", "home", "books", "sports", "beauty", "toys"]
CURRENCIES = ["USD", "EUR"]


@dataclass(frozen=True)
class SimulationShape:
    users_per_day_base: int = 12
    products_day_zero: int = 40
    orders_per_day_base: int = 30


def users_created_on(day_idx: int, shape: SimulationShape = SimulationShape()) -> int:
    return shape.users_per_day_base + (day_idx % 5)


def products_created_on(day_idx: int, shape: SimulationShape = SimulationShape()) -> int:
    if day_idx == 0:
        return shape.products_day_zero
    return 2 + (day_idx % 3)


def orders_created_on(day_idx: int, shape: SimulationShape = SimulationShape()) -> int:
    return shape.orders_per_day_base + ((day_idx % 7) * 4)


def cumulative_users(day_idx: int) -> int:
    if day_idx < 0:
        return 0
    return sum(users_created_on(idx) for idx in range(day_idx + 1))


def cumulative_products(day_idx: int) -> int:
    if day_idx < 0:
        return 0
    return sum(products_created_on(idx) for idx in range(day_idx + 1))


def user_id(user_num: int) -> str:
    return f"U{user_num:06d}"


def product_id(product_num: int) -> str:
    return f"P{product_num:06d}"


def order_id(day_idx: int, seq: int) -> str:
    return f"O{day_idx:04d}{seq:04d}"


def order_item_id(order_pk: str, item_seq: int) -> str:
    return f"OI{order_pk[1:]}{item_seq:02d}"


def payment_id(order_pk: str) -> str:
    return f"PM{order_pk[1:]}"


def _to_iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat()


def _day_index(target: date) -> int:
    idx = (target - START_DATE).days
    if idx < 0:
        raise ValueError(f"target date {target.isoformat()} is before simulation start {START_DATE.isoformat()}")
    return idx


def _day_start(target: date) -> datetime:
    return datetime.combine(target, time.min).replace(tzinfo=timezone.utc)


def _random_ts(rng: random.Random, target: date) -> datetime:
    return _day_start(target) + timedelta(seconds=rng.randint(0, 86_399))


def _maybe_late_ts(rng: random.Random, target: date) -> datetime:
    if rng.random() < 0.18:
        lag_days = 1 if rng.random() < 0.85 else 2
        late_day = target - timedelta(days=lag_days)
        return _day_start(late_day) + timedelta(seconds=rng.randint(0, 86_399))
    return _random_ts(rng, target)


def _user_name(user_num: int) -> str:
    return f"User {user_num:06d}"


def _user_email(user_num: int, rev: int = 0) -> str:
    suffix = "" if rev == 0 else f".r{rev}"
    return f"user{user_num:06d}{suffix}@example.com"


def _user_region(user_num: int, rev: int = 0) -> str:
    return COUNTRIES[(user_num + rev) % len(COUNTRIES)]


def _category(product_num: int) -> str:
    return CATEGORIES[product_num % len(CATEGORIES)]


def _base_price(product_num: int) -> float:
    return round(7 + ((product_num * 19) % 400) + ((product_num % 7) * 0.49), 2)


def _currency(product_num: int) -> str:
    return CURRENCIES[product_num % len(CURRENCIES)]


def _emit(
    events: list[dict],
    counter: int,
    batch_date: date,
    entity: str,
    operation: str,
    pk: str,
    event_ts: datetime,
    payload: dict,
    schema_version: int,
) -> int:
    events.append(
        {
            "event_id": f"{batch_date.strftime('%Y%m%d')}-{counter:06d}",
            "entity": entity,
            "operation": operation,
            "event_ts": event_ts,
            "pk": pk,
            "payload": json.dumps(payload, sort_keys=True),
            "schema_version": schema_version,
        }
    )
    return counter + 1


def generate_cdc_batch(batch_date: date, seed: int = 42, schema_version: int = 1) -> pd.DataFrame:
    day_idx = _day_index(batch_date)
    rng = random.Random(seed + (day_idx * 7_919))
    events: list[dict] = []
    counter = 0

    users_before = cumulative_users(day_idx - 1)
    users_today = users_created_on(day_idx)
    products_before = cumulative_products(day_idx - 1)
    products_today = products_created_on(day_idx)

    first_user = users_before + 1
    for user_num in range(first_user, first_user + users_today):
        created_ts = _random_ts(rng, batch_date)
        payload = {
            "user_id": user_id(user_num),
            "name": _user_name(user_num),
            "email": _user_email(user_num),
            "region": _user_region(user_num),
            "created_at": _to_iso(created_ts),
            "updated_at": _to_iso(created_ts),
            "is_deleted": False,
        }
        counter = _emit(events, counter, batch_date, "users", "I", payload["user_id"], created_ts, payload, schema_version)

    existing_users = cumulative_users(day_idx)
    update_users_count = min(max(2, existing_users // 20), 12)
    for user_num in rng.sample(range(1, existing_users + 1), k=update_users_count):
        ts = _maybe_late_ts(rng, batch_date)
        payload = {
            "updated_at": _to_iso(ts),
            "email": _user_email(user_num, rev=day_idx + 1),
            "region": _user_region(user_num, rev=day_idx + 1),
        }
        counter = _emit(events, counter, batch_date, "users", "U", user_id(user_num), ts, payload, schema_version)

    if day_idx % 6 == 0 and existing_users > 25:
        user_num = rng.randint(1, existing_users)
        ts = _random_ts(rng, batch_date)
        payload = {
            "updated_at": _to_iso(ts),
            "is_deleted": True,
            "delete_mode": "hard" if rng.random() < 0.2 else "soft",
        }
        counter = _emit(events, counter, batch_date, "users", "D", user_id(user_num), ts, payload, schema_version)

    first_product = products_before + 1
    for product_num in range(first_product, first_product + products_today):
        created_ts = _random_ts(rng, batch_date)
        payload = {
            "product_id": product_id(product_num),
            "name": f"Product {product_num:06d}",
            "category": _category(product_num),
            "price": _base_price(product_num),
            "currency": _currency(product_num),
            "created_at": _to_iso(created_ts),
            "updated_at": _to_iso(created_ts),
            "is_deleted": False,
        }
        counter = _emit(
            events,
            counter,
            batch_date,
            "products",
            "I",
            payload["product_id"],
            created_ts,
            payload,
            schema_version,
        )

    existing_products = cumulative_products(day_idx)
    update_products_count = min(max(2, existing_products // 18), 10)
    for product_num in rng.sample(range(1, existing_products + 1), k=update_products_count):
        ts = _maybe_late_ts(rng, batch_date)
        adjustment = 1 + (rng.uniform(-0.05, 0.09))
        payload = {
            "updated_at": _to_iso(ts),
            "price": round(max(0.01, _base_price(product_num) * adjustment), 2),
        }
        counter = _emit(events, counter, batch_date, "products", "U", product_id(product_num), ts, payload, schema_version)

    if day_idx % 8 == 0 and existing_products > 45:
        product_num = rng.randint(1, existing_products)
        ts = _random_ts(rng, batch_date)
        payload = {
            "updated_at": _to_iso(ts),
            "is_deleted": True,
            "delete_mode": "hard" if rng.random() < 0.15 else "soft",
        }
        counter = _emit(events, counter, batch_date, "products", "D", product_id(product_num), ts, payload, schema_version)

    order_count = orders_created_on(day_idx)
    for order_seq in range(order_count):
        pk = order_id(day_idx, order_seq)
        u_num = ((day_idx * 113) + (order_seq * 17)) % existing_users + 1
        user_pk = user_id(u_num)
        order_ts = _random_ts(rng, batch_date)
        created_payload = {
            "order_id": pk,
            "user_id": user_pk,
            "status": "created",
            "order_ts": _to_iso(order_ts),
            "updated_at": _to_iso(order_ts),
            "is_deleted": False,
        }
        counter = _emit(events, counter, batch_date, "orders", "I", pk, order_ts, created_payload, schema_version)

        item_count = 1 + rng.randint(0, 2)
        order_total = 0.0
        for item_seq in range(item_count):
            prod_num = ((day_idx * 41) + (order_seq * 5) + (item_seq * 3)) % existing_products + 1
            qty = 1 + rng.randint(0, 3)
            unit_price = round(_base_price(prod_num) * (1 + (day_idx % 4) * 0.01), 2)
            item_payload = {
                "order_item_id": order_item_id(pk, item_seq),
                "order_id": pk,
                "product_id": product_id(prod_num),
                "qty": qty,
                "unit_price": unit_price,
                "created_at": _to_iso(order_ts + timedelta(seconds=(item_seq + 1) * 3)),
            }
            counter = _emit(
                events,
                counter,
                batch_date,
                "order_items",
                "I",
                item_payload["order_item_id"],
                order_ts + timedelta(seconds=(item_seq + 1) * 3),
                item_payload,
                schema_version,
            )
            order_total += qty * unit_price

        next_ts = order_ts + timedelta(minutes=10 + rng.randint(0, 90))
        if rng.random() < 0.86:
            paid_payload = {
                "updated_at": _to_iso(next_ts),
                "status": "paid",
            }
            counter = _emit(events, counter, batch_date, "orders", "U", pk, next_ts, paid_payload, schema_version)

            payment_created_ts = next_ts + timedelta(minutes=2)
            payment_payload = {
                "payment_id": payment_id(pk),
                "order_id": pk,
                "method": ["card", "pix", "boleto", "paypal"][rng.randint(0, 3)],
                "amount": round(order_total, 2),
                "status": "captured",
                "created_at": _to_iso(payment_created_ts),
                "updated_at": _to_iso(payment_created_ts),
            }
            counter = _emit(
                events,
                counter,
                batch_date,
                "payments",
                "I",
                payment_payload["payment_id"],
                payment_created_ts,
                payment_payload,
                schema_version,
            )

            status_roll = rng.random()
            if status_roll < 0.65:
                shipped_ts = next_ts + timedelta(minutes=30 + rng.randint(0, 240))
                counter = _emit(
                    events,
                    counter,
                    batch_date,
                    "orders",
                    "U",
                    pk,
                    shipped_ts,
                    {"updated_at": _to_iso(shipped_ts), "status": "shipped"},
                    schema_version,
                )
            elif status_roll < 0.76:
                refund_ts = _maybe_late_ts(rng, batch_date)
                counter = _emit(
                    events,
                    counter,
                    batch_date,
                    "orders",
                    "U",
                    pk,
                    refund_ts,
                    {"updated_at": _to_iso(refund_ts), "status": "refunded"},
                    schema_version,
                )
                counter = _emit(
                    events,
                    counter,
                    batch_date,
                    "payments",
                    "U",
                    payment_payload["payment_id"],
                    refund_ts + timedelta(minutes=1),
                    {"updated_at": _to_iso(refund_ts + timedelta(minutes=1)), "status": "refunded"},
                    schema_version,
                )
        else:
            cancel_ts = next_ts
            counter = _emit(
                events,
                counter,
                batch_date,
                "orders",
                "U",
                pk,
                cancel_ts,
                {"updated_at": _to_iso(cancel_ts), "status": "cancelled"},
                schema_version,
            )

    if day_idx > 0:
        prev_day_orders = orders_created_on(day_idx - 1)
        sample_size = min(4, prev_day_orders)
        for seq in rng.sample(range(prev_day_orders), k=sample_size):
            pk = order_id(day_idx - 1, seq)
            ts = _maybe_late_ts(rng, batch_date)
            status = ["shipped", "cancelled", "refunded"][rng.randint(0, 2)]
            counter = _emit(
                events,
                counter,
                batch_date,
                "orders",
                "U",
                pk,
                ts,
                {"updated_at": _to_iso(ts), "status": status},
                schema_version,
            )
            if status == "refunded":
                counter = _emit(
                    events,
                    counter,
                    batch_date,
                    "payments",
                    "U",
                    payment_id(pk),
                    ts + timedelta(minutes=1),
                    {"updated_at": _to_iso(ts + timedelta(minutes=1)), "status": "refunded"},
                    schema_version,
                )

    if day_idx % 9 == 0 and day_idx > 2:
        prev_day = day_idx - 2
        seq = rng.randint(0, orders_created_on(prev_day) - 1)
        pk = order_id(prev_day, seq)
        ts = _random_ts(rng, batch_date)
        counter = _emit(
            events,
            counter,
            batch_date,
            "orders",
            "D",
            pk,
            ts,
            {"updated_at": _to_iso(ts), "is_deleted": True, "delete_mode": "soft"},
            schema_version,
        )

    df = pd.DataFrame(events)
    if df.empty:
        return df
    df["event_ts"] = pd.to_datetime(df["event_ts"], utc=True)
    return df.sort_values(["event_ts", "event_id"]).reset_index(drop=True)
