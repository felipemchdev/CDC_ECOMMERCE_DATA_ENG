from __future__ import annotations

import json
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

Entity = Literal["users", "products", "orders", "order_items", "payments"]
Operation = Literal["I", "U", "D"]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class DeletePayload(StrictModel):
    updated_at: str
    is_deleted: bool = True
    delete_mode: Literal["soft", "hard"] = "soft"


class UserInsert(StrictModel):
    user_id: str
    name: str
    email: str
    region: str
    created_at: str
    updated_at: str
    is_deleted: bool = False


class UserUpdate(StrictModel):
    updated_at: str
    name: str | None = None
    email: str | None = None
    region: str | None = None
    is_deleted: bool | None = None


class ProductInsert(StrictModel):
    product_id: str
    name: str
    category: str
    price: float
    currency: str
    created_at: str
    updated_at: str
    is_deleted: bool = False


class ProductUpdate(StrictModel):
    updated_at: str
    name: str | None = None
    category: str | None = None
    price: float | None = None
    currency: str | None = None
    is_deleted: bool | None = None


class OrderInsert(StrictModel):
    order_id: str
    user_id: str
    status: Literal["created", "paid", "shipped", "cancelled", "refunded"]
    order_ts: str
    updated_at: str
    is_deleted: bool = False


class OrderUpdate(StrictModel):
    updated_at: str
    user_id: str | None = None
    status: Literal["created", "paid", "shipped", "cancelled", "refunded"] | None = None
    order_ts: str | None = None
    is_deleted: bool | None = None


class OrderItemInsert(StrictModel):
    order_item_id: str
    order_id: str
    product_id: str
    qty: int = Field(ge=1)
    unit_price: float = Field(ge=0)
    created_at: str


class PaymentInsert(StrictModel):
    payment_id: str
    order_id: str
    method: str
    amount: float = Field(ge=0)
    status: str
    created_at: str
    updated_at: str


class PaymentUpdate(StrictModel):
    updated_at: str
    method: str | None = None
    amount: float | None = Field(default=None, ge=0)
    status: str | None = None


_INSERT_MODELS: dict[Entity, type[BaseModel]] = {
    "users": UserInsert,
    "products": ProductInsert,
    "orders": OrderInsert,
    "order_items": OrderItemInsert,
    "payments": PaymentInsert,
}

_UPDATE_MODELS: dict[Entity, type[BaseModel]] = {
    "users": UserUpdate,
    "products": ProductUpdate,
    "orders": OrderUpdate,
    "order_items": OrderItemInsert,
    "payments": PaymentUpdate,
}


def validate_payload(entity: Entity, operation: Operation, payload_raw: str | dict[str, Any]) -> dict[str, Any]:
    payload = json.loads(payload_raw) if isinstance(payload_raw, str) else payload_raw

    try:
        if operation == "I":
            return _INSERT_MODELS[entity](**payload).model_dump()
        if operation == "U":
            if entity == "order_items":
                raise ValueError("order_items only supports insert operations")
            return _UPDATE_MODELS[entity](**payload).model_dump(exclude_none=True)
        return DeletePayload(**payload).model_dump(exclude_none=True)
    except (ValidationError, ValueError) as exc:
        raise ValueError(f"Invalid payload for entity={entity}, operation={operation}: {exc}") from exc
