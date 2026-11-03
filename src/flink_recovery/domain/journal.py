from collections.abc import Iterable
from enum import StrEnum
from typing import Self

from pydantic import BaseModel, ConfigDict, NonNegativeInt


class CommandEvent(StrEnum):
    EXECUTE_REQUESTED = "execute_requested"
    STOP_REQUESTED = "stop_requested"
    SAVEPOINT_COMPLETED = "savepoint_completed"
    RESTORE_REQUESTED = "restore_requested"
    JOB_FINISHED = "job_finished"


class CommandRecord(BaseModel):
    model_config = ConfigDict(frozen=True)

    event: CommandEvent
    time_ns: NonNegativeInt


class ProcessedRecord(BaseModel):
    model_config = ConfigDict(frozen=True)

    processed_at_ns: NonNegativeInt


class RunJournal(BaseModel):
    model_config = ConfigDict(frozen=True)

    commands: tuple[CommandRecord, ...]
    records: tuple[ProcessedRecord, ...]

    @classmethod
    def from_parts(
        cls,
        commands: Iterable[CommandRecord],
        records: Iterable[ProcessedRecord],
    ) -> Self:
        return cls(commands=tuple(commands), records=tuple(records))

    def of(self, event: CommandEvent) -> tuple[CommandRecord, ...]:
        return tuple(command for command in self.commands if command.event is event)
