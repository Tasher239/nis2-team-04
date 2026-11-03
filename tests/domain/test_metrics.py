import pytest

from flink_recovery.domain.journal import CommandEvent, CommandRecord, ProcessedRecord, RunJournal
from flink_recovery.domain.metrics import JournalAnalyzer


def journal(times_s: list[float], commands: list[tuple[CommandEvent, float]]) -> RunJournal:
    return RunJournal.from_parts(
        commands=[
            CommandRecord(event=event, time_ns=int(moment * 10**9)) for event, moment in commands
        ],
        records=[
            ProcessedRecord(processed_at_ns=int(moment * 10**9)) for moment in times_s
        ],
    )


def test_restore_and_window_boundary() -> None:
    run = journal(
        [1, 2, 6, 7, 8, 16],
        [
            (CommandEvent.RESTORE_REQUESTED, 4),
            (CommandEvent.JOB_FINISHED, 17),
        ],
    )
    report = JournalAnalyzer().analyze(run)
    assert report.pause_s == 4
    assert report.recovery_s == 2
    assert report.throughput_events_s == 0.3


def test_short_run_has_no_ten_second_rate() -> None:
    run = journal([1], [(CommandEvent.JOB_FINISHED, 2)])
    report = JournalAnalyzer().analyze(run)
    assert report.throughput_events_s is None
    assert report.recovery_s is None
    assert not report.window_complete


def test_missing_events_after_restore() -> None:
    run = journal([1], [(CommandEvent.RESTORE_REQUESTED, 2)])
    with pytest.raises(ValueError):
        JournalAnalyzer().analyze(run)


def test_no_restore_window_starts_at_first_event() -> None:
    run = journal([1, 2, 3], [(CommandEvent.JOB_FINISHED, 20)])
    report = JournalAnalyzer().analyze(run)
    assert report.processed_records == 3
    assert report.events_in_window == 3
    assert report.throughput_events_s == 0.3
    assert report.pause_s is None
