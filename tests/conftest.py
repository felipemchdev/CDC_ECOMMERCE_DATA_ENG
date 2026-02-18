from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from cdc_ecommerce.config import Settings


@pytest.fixture()
def settings(tmp_path: Path) -> Settings:
    data_root = tmp_path / "data"
    bronze_root = data_root / "bronze"
    silver_root = data_root / "silver"
    gold_root = data_root / "gold"
    metrics_root = data_root / "metrics"
    for path in (bronze_root, silver_root, gold_root, metrics_root):
        path.mkdir(parents=True, exist_ok=True)

    return Settings(
        project_root=tmp_path,
        data_root=data_root,
        bronze_root=bronze_root,
        silver_root=silver_root,
        gold_root=gold_root,
        metrics_root=metrics_root,
        seed=42,
        schema_version=1,
        simulation_start_date=date(2021, 1, 1),
    )
