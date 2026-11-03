import traceback
from pathlib import Path

from pydantic import BaseModel, ConfigDict, NonNegativeFloat, NonNegativeInt, PositiveInt

from flink_recovery.config import Settings
from flink_recovery.data.jsonl import JsonLinesReader
from flink_recovery.data.run_directory import RunDirectory, portable_path
from flink_recovery.domain.journal import CommandEvent
from flink_recovery.domain.metrics import JournalAnalyzer, MetricsReport
from flink_recovery.domain.purchases import AggregateRecord, Purchase
from flink_recovery.domain.validation import ResultValidator, ValidationReport
from flink_recovery.streaming.runner import JobRunner, RunMetadata, SavepointMetadata


class RecoveryReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    input_path: str
    input_events: NonNegativeInt
    stop_after_s: NonNegativeFloat
    initial_parallelism: PositiveInt
    restore_parallelism: PositiveInt
    savepoint: SavepointMetadata
    restored_run: RunMetadata
    records_before_restore: NonNegativeInt
    records_after_restore: NonNegativeInt
    users_before_restore: NonNegativeInt
    subtasks_after_restore: list[int]
    check: ValidationReport
    metrics: MetricsReport


class RecoverySmoke:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._directory = RunDirectory(settings.output_path)

    def run(self, restore_parallelism: int, stop_after_s: float) -> RecoveryReport:
        if self._directory.exists():
            raise FileExistsError("Output directory must not exist; choose a new RUN_NAME")
        try:
            report = self._experiment(restore_parallelism, stop_after_s)
        except BaseException:
            self._directory.write_failure(traceback.format_exc())
            raise
        self._directory.write_model(self._directory.report_path, report)
        return report

    def _experiment(self, restore_parallelism: int, stop_after_s: float) -> RecoveryReport:
        settings = self._settings
        savepoint = JobRunner(settings).stop_with_savepoint(stop_after_s)
        restored = JobRunner(settings.model_copy(update={"parallelism": restore_parallelism}))
        metadata = restored.run(Path(savepoint.savepoint_path))
        events = JsonLinesReader(settings.input_path).models(Purchase)
        records = self._directory.records(AggregateRecord)
        journal = self._directory.journal()
        restart_ns = journal.of(CommandEvent.RESTORE_REQUESTED)[0].time_ns
        before = [r for r in records if r.processed_at_ns < restart_ns]
        after = [r for r in records if r.processed_at_ns >= restart_ns]
        if not before or not after:
            raise ValueError("Smoke requires nonempty output on both sides of restore")
        return RecoveryReport(
            input_path=portable_path(settings.input_path),
            input_events=len(events),
            stop_after_s=stop_after_s,
            initial_parallelism=settings.parallelism,
            restore_parallelism=restore_parallelism,
            savepoint=savepoint,
            restored_run=metadata,
            records_before_restore=len(before),
            records_after_restore=len(after),
            users_before_restore=len({r.user_id for r in before}),
            subtasks_after_restore=sorted({r.subtask for r in after}),
            check=ResultValidator().validate(events, records),
            metrics=JournalAnalyzer().analyze(journal),
        )
