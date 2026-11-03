from typing import Self

from pydantic import BaseModel, ConfigDict, NonNegativeFloat, NonNegativeInt, model_validator

from flink_recovery.constants import (
    NANOSECONDS_PER_SECOND,
    THROUGHPUT_WINDOW_NS,
    THROUGHPUT_WINDOW_S,
)
from flink_recovery.domain.journal import CommandEvent, RunJournal


def _last_finish_ns(journal: RunJournal) -> int | None:
    finishes = [command.time_ns for command in journal.of(CommandEvent.JOB_FINISHED)]
    return max(finishes) if finishes else None


class RunPhases(BaseModel):
    model_config = ConfigDict(frozen=True)

    times_ns: tuple[NonNegativeInt, ...]
    restart_ns: NonNegativeInt | None = None
    last_before_restart_ns: NonNegativeInt | None = None
    window_start_ns: NonNegativeInt
    window_end_ns: NonNegativeInt
    finished_at_ns: NonNegativeInt | None = None

    @classmethod
    def from_journal(cls, journal: RunJournal) -> Self:
        times = sorted(record.processed_at_ns for record in journal.records)
        if not times:
            raise ValueError("No processed events found")
        restarts = journal.of(CommandEvent.RESTORE_REQUESTED)
        if not restarts:
            return cls(
                times_ns=tuple(times),
                window_start_ns=times[0],
                window_end_ns=times[0] + THROUGHPUT_WINDOW_NS,
                finished_at_ns=_last_finish_ns(journal),
            )
        if len(restarts) != 1:
            raise ValueError("Use one restoration per journal")
        restart = restarts[0].time_ns
        before = [time for time in times if time < restart]
        after = [time for time in times if time >= restart]
        if not before or not after:
            raise ValueError("Restoration needs events before and after the command")
        return cls(
            times_ns=tuple(times),
            restart_ns=restart,
            last_before_restart_ns=before[-1],
            window_start_ns=after[0],
            window_end_ns=after[0] + THROUGHPUT_WINDOW_NS,
            finished_at_ns=_last_finish_ns(journal),
        )

    @property
    def events_in_window(self) -> int:
        return sum(self.window_start_ns <= time < self.window_end_ns for time in self.times_ns)

    @property
    def window_complete(self) -> bool:
        return self.finished_at_ns is not None and self.finished_at_ns >= self.window_end_ns

    @property
    def pause_s(self) -> float | None:
        if self.restart_ns is None or self.last_before_restart_ns is None:
            return None
        return (self.window_start_ns - self.last_before_restart_ns) / NANOSECONDS_PER_SECOND

    @property
    def recovery_s(self) -> float | None:
        if self.restart_ns is None:
            return None
        return (self.window_start_ns - self.restart_ns) / NANOSECONDS_PER_SECOND


class MetricsReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    processed_records: NonNegativeInt
    pause_s: NonNegativeFloat | None = None
    recovery_s: NonNegativeFloat | None = None
    throughput_window_s: NonNegativeInt = THROUGHPUT_WINDOW_S
    events_in_window: NonNegativeInt
    window_complete: bool
    throughput_events_s: NonNegativeFloat | None = None

    @model_validator(mode="after")
    def _rate_only_when_window_complete(self) -> Self:
        if self.window_complete is not (self.throughput_events_s is not None):
            raise ValueError("throughput_events_s is defined only for a complete window")
        return self


class JournalAnalyzer:
    def analyze(self, journal: RunJournal) -> MetricsReport:
        return self.from_phases(RunPhases.from_journal(journal))

    @staticmethod
    def from_phases(phases: RunPhases) -> MetricsReport:
        events = phases.events_in_window
        return MetricsReport(
            processed_records=len(phases.times_ns),
            pause_s=phases.pause_s,
            recovery_s=phases.recovery_s,
            events_in_window=events,
            window_complete=phases.window_complete,
            throughput_events_s=events / THROUGHPUT_WINDOW_S if phases.window_complete else None,
        )
