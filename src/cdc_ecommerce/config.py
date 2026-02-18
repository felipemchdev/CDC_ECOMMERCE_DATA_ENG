from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    project_root: Path
    data_root: Path
    bronze_root: Path
    silver_root: Path
    gold_root: Path
    metrics_root: Path
    seed: int = 42
    schema_version: int = 1
    simulation_start_date: date = date(2021, 1, 1)


def get_settings(project_root: Path | None = None) -> Settings:
    root = (project_root or Path.cwd()).resolve()
    data_root = root / "data"
    return Settings(
        project_root=root,
        data_root=data_root,
        bronze_root=data_root / "bronze",
        silver_root=data_root / "silver",
        gold_root=data_root / "gold",
        metrics_root=data_root / "metrics",
    )
