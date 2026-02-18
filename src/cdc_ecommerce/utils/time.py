from __future__ import annotations

from datetime import date, datetime


def parse_date(raw: str) -> date:
    return datetime.strptime(raw, "%Y-%m-%d").date()
