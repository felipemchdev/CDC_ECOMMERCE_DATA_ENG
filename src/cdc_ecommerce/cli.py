from __future__ import annotations

import json
from pathlib import Path

import typer

from cdc_ecommerce.config import get_settings
from cdc_ecommerce.pipeline import backfill as backfill_pipeline
from cdc_ecommerce.pipeline import run_pipeline_for_date
from cdc_ecommerce.utils.time import parse_date

app = typer.Typer(help="CDC e-commerce Medallion pipeline")


@app.command("run")
def run_command(
    date: str = typer.Option(..., help="Run date in YYYY-MM-DD format"),
    project_root: Path = typer.Option(Path("."), help="Project root path"),
) -> None:
    settings = get_settings(project_root.resolve())
    result = run_pipeline_for_date(parse_date(date), settings)
    typer.echo(json.dumps(result, indent=2, default=str))


@app.command("backfill")
def backfill_command(
    start: str = typer.Option(..., help="Start date in YYYY-MM-DD format"),
    end: str = typer.Option(..., help="End date in YYYY-MM-DD format"),
    project_root: Path = typer.Option(Path("."), help="Project root path"),
) -> None:
    settings = get_settings(project_root.resolve())
    results = backfill_pipeline(parse_date(start), parse_date(end), settings)
    typer.echo(json.dumps(results, indent=2, default=str))


def main() -> None:
    app()
