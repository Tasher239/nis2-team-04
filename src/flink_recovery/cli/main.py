import json
from pathlib import Path
from typing import Annotated

import typer

from flink_recovery.config import Settings
from flink_recovery.constants import (
    DEFAULT_RESTORE_PARALLELISM,
    DEFAULT_STOP_AFTER_S,
    PROTECTED_INPUT,
)
from flink_recovery.data.generator import PurchaseGenerator
from flink_recovery.data.jsonl import JsonLinesReader
from flink_recovery.data.run_directory import RunDirectory
from flink_recovery.domain.metrics import JournalAnalyzer
from flink_recovery.domain.purchases import AggregateRecord, Purchase
from flink_recovery.domain.validation import ResultValidator

app = typer.Typer(help="Flink rescaling and recovery experiment toolkit.", no_args_is_help=True)

OptionalPath = Annotated[Path | None, typer.Argument()]


def resolve_run_dir(run_dir: Path | None) -> RunDirectory:
    return RunDirectory(run_dir if run_dir is not None else Settings().output_path)


@app.command()
def generate() -> None:
    settings = Settings()
    if settings.input_path.resolve() == Path(PROTECTED_INPUT).resolve():
        raise typer.BadParameter(f"INPUT_PATH must not overwrite {PROTECTED_INPUT}")
    written = PurchaseGenerator(settings.user_count, settings.purchases_per_user).write(
        settings.input_path
    )
    typer.echo(json.dumps({"written": written, "path": str(settings.input_path)}, indent=2))


@app.command()
def run(savepoint: Annotated[Path | None, typer.Option()] = None) -> None:
    from flink_recovery.streaming.runner import JobRunner

    typer.echo(JobRunner(Settings()).run(savepoint).model_dump_json(indent=2))


@app.command()
def recovery_smoke(
    restore_parallelism: Annotated[int, typer.Option(min=1)] = DEFAULT_RESTORE_PARALLELISM,
    stop_after_s: Annotated[float, typer.Option(min=1)] = DEFAULT_STOP_AFTER_S,
) -> None:
    """Stop with a savepoint, restore, validate and measure one local experiment."""
    from flink_recovery.streaming.smoke import RecoverySmoke

    report = RecoverySmoke(Settings()).run(restore_parallelism, stop_after_s)
    typer.echo(report.model_dump_json(indent=2))
    raise typer.Exit(0 if report.check.ok else 1)


@app.command()
def check(run_dir: OptionalPath = None) -> None:
    events = JsonLinesReader(Settings().input_path).models(Purchase)
    report = ResultValidator().validate(events, resolve_run_dir(run_dir).records(AggregateRecord))
    typer.echo(report.model_dump_json(indent=2))
    raise typer.Exit(0 if report.ok else 1)


@app.command()
def metrics(run_dir: OptionalPath = None) -> None:
    report = JournalAnalyzer().analyze(resolve_run_dir(run_dir).journal())
    typer.echo(report.model_dump_json(indent=2))
