from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

import pandas as pd

from cdc_ecommerce.config import Settings
from cdc_ecommerce.quality.schema import Entity, Operation, validate_payload
from cdc_ecommerce.utils.io import read_parquet_or_empty, write_parquet

ENTITY_PK: dict[Entity, str] = {
    "users": "user_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "payments": "payment_id",
}

ENTITIES: tuple[Entity, ...] = ("users", "products", "orders", "order_items", "payments")


class SilverMerger:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.settings.silver_root.mkdir(parents=True, exist_ok=True)
        self.processed_events_path = self.settings.silver_root / "_processed_event_ids.parquet"

    def merge_events(self, events_df: pd.DataFrame) -> dict:
        if events_df.empty:
            return {
                "processed_events_count": 0,
                "output_row_counts": {entity: self._load_entity(entity).shape[0] for entity in ENTITIES},
            }

        processed_ids = self._load_processed_event_ids()
        deduped = events_df.sort_values(["event_ts", "event_id"]).drop_duplicates(subset=["event_id"], keep="first")
        fresh_events = deduped[~deduped["event_id"].isin(processed_ids)].copy()

        if fresh_events.empty:
            return {
                "processed_events_count": 0,
                "output_row_counts": {entity: self._load_entity(entity).shape[0] for entity in ENTITIES},
            }

        entity_row_counts: dict[str, int] = {}
        for entity in ENTITIES:
            subset = fresh_events[fresh_events["entity"] == entity].sort_values(["event_ts", "event_id"])
            merged = self._apply_entity_events(entity, subset.to_dict(orient="records"))
            self._save_entity(entity, merged)
            entity_row_counts[entity] = int(merged.shape[0])

        processed_ids.update(fresh_events["event_id"].tolist())
        self._save_processed_event_ids(processed_ids)

        return {
            "processed_events_count": int(fresh_events.shape[0]),
            "output_row_counts": entity_row_counts,
        }

    def _entity_path(self, entity: Entity) -> Path:
        return self.settings.silver_root / f"{entity}.parquet"

    def _load_entity(self, entity: Entity) -> pd.DataFrame:
        return read_parquet_or_empty(self._entity_path(entity))

    def _save_entity(self, entity: Entity, df: pd.DataFrame) -> None:
        write_parquet(df, self._entity_path(entity))

    def _load_processed_event_ids(self) -> set[str]:
        df = read_parquet_or_empty(self.processed_events_path)
        if df.empty or "event_id" not in df.columns:
            return set()
        return set(df["event_id"].astype(str).tolist())

    def _save_processed_event_ids(self, event_ids: Iterable[str]) -> None:
        df = pd.DataFrame({"event_id": sorted(set(event_ids))})
        write_parquet(df, self.processed_events_path)

    def _apply_entity_events(self, entity: Entity, events: list[dict]) -> pd.DataFrame:
        pk_col = ENTITY_PK[entity]
        current = self._load_entity(entity)
        state: dict[str, dict] = {}

        if not current.empty:
            for row in current.to_dict(orient="records"):
                key = str(row[pk_col])
                state[key] = row

        for event in events:
            event_id = str(event["event_id"])
            event_ts = _to_utc_ts(event["event_ts"])
            operation: Operation = event["operation"]
            pk = str(event["pk"])
            payload = validate_payload(entity, operation, event["payload"])

            existing = state.get(pk, {pk_col: pk})
            last_event_ts = _to_utc_ts(existing.get("_last_event_ts")) if existing.get("_last_event_ts") else None

            if last_event_ts is not None and event_ts < last_event_ts:
                continue

            if operation == "I":
                merged = {pk_col: pk, **payload}
                merged.setdefault("is_deleted", False)
            elif operation == "U":
                merged = dict(existing)
                merged.update(payload)
                merged.setdefault("is_deleted", False)
            else:
                merged = dict(existing)
                merged[pk_col] = pk
                merged["is_deleted"] = True
                merged["updated_at"] = payload.get("updated_at", event_ts.isoformat())
                if "delete_mode" in payload:
                    merged["delete_mode"] = payload["delete_mode"]

            merged["_last_event_ts"] = event_ts
            merged["_last_event_id"] = event_id
            merged["_schema_version"] = int(event["schema_version"])
            state[pk] = merged

        if not state:
            return pd.DataFrame(columns=[pk_col])

        merged_df = pd.DataFrame(state.values())
        if pk_col not in merged_df.columns:
            merged_df[pk_col] = merged_df.index.astype(str)

        for col in ["created_at", "updated_at", "order_ts", "_last_event_ts"]:
            if col in merged_df.columns:
                merged_df[col] = pd.to_datetime(merged_df[col], utc=True, errors="coerce")

        return merged_df.sort_values(pk_col).reset_index(drop=True)


def _to_utc_ts(value: object) -> pd.Timestamp:
    return pd.to_datetime(value, utc=True)
