from __future__ import annotations

from datetime import date, datetime


def parse_date(raw: str) -> date:
    return datetime.strptime(raw, "%Y-%m-%d").date()


def date_range(start: date, end: date) -> list[date]:
    out: list[date] = []
    current = start
    while current <= end:
        out.append(current)
        current = current.fromordinal(current.toordinal() + 1)
    return out
