from __future__ import annotations

from datetime import date

import pytest

from cdc_ecommerce.ingestion.generator import generate_cdc_batch


def test_generator_accepts_simulation_start_date() -> None:
    events = generate_cdc_batch(date(2021, 1, 1), seed=42, schema_version=1)
    assert not events.empty
    assert events["event_ts"].min().date() >= date(2020, 12, 30)


def test_generator_rejects_dates_before_simulation_start() -> None:
    with pytest.raises(ValueError, match="before simulation start 2021-01-01"):
        generate_cdc_batch(date(2020, 12, 31), seed=42, schema_version=1)
