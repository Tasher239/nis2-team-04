from pathlib import Path
from typing import TypeVar

from pydantic import BaseModel

from flink_recovery.constants import (
    COMMANDS_FILE,
    FAILURE_FILE,
    METADATA_FILE,
    REPORT_FILE,
    SAVEPOINT_FILE,
    SINK_FILE_GLOB,
)
from flink_recovery.data.jsonl import JsonLinesReader
from flink_recovery.domain.journal import CommandRecord, ProcessedRecord, RunJournal

ModelT = TypeVar("ModelT", bound=BaseModel)


def portable_path(path: Path) -> str:
    resolved = path.resolve()
    try:
        return str(resolved.relative_to(Path.cwd().resolve()))
    except ValueError:
        return str(resolved)


class RunDirectory:
    def __init__(self, path: Path) -> None:
        self._path = path

    @property
    def path(self) -> Path:
        return self._path

    @property
    def commands_path(self) -> Path:
        return self._path / COMMANDS_FILE

    @property
    def metadata_path(self) -> Path:
        return self._path / METADATA_FILE

    @property
    def savepoint_path(self) -> Path:
        return self._path / SAVEPOINT_FILE

    @property
    def report_path(self) -> Path:
        return self._path / REPORT_FILE

    @property
    def failure_path(self) -> Path:
        return self._path / FAILURE_FILE

    def exists(self) -> bool:
        return self._path.exists()

    def create(self) -> None:
        self._path.mkdir(parents=True, exist_ok=True)

    def part_files(self) -> list[Path]:
        return sorted(path for path in self._path.rglob(SINK_FILE_GLOB) if path.is_file())

    def records(self, model: type[ModelT]) -> list[ModelT]:
        rows: list[ModelT] = []
        for path in self.part_files():
            rows.extend(JsonLinesReader(path).models(model))
        return rows

    def commands(self) -> list[CommandRecord]:
        if not self.commands_path.exists():
            return []
        return JsonLinesReader(self.commands_path).models(CommandRecord)

    def journal(self) -> RunJournal:
        return RunJournal.from_parts(self.commands(), self.records(ProcessedRecord))

    def read_model(self, path: Path, model: type[ModelT]) -> ModelT:
        return model.model_validate_json(path.read_text())

    def write_model(self, path: Path, value: BaseModel) -> None:
        self.create()
        path.write_text(value.model_dump_json(indent=2) + "\n")

    def read_metadata(self, model: type[ModelT]) -> ModelT:
        return self.read_model(self.metadata_path, model)

    def write_metadata(self, metadata: BaseModel) -> None:
        self.write_model(self.metadata_path, metadata)

    def write_failure(self, text: str) -> None:
        self.create()
        self.failure_path.write_text(text.replace(str(Path.cwd().resolve()) + "/", ""))
