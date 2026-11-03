from pathlib import Path
from typing import TypeVar

from pydantic import BaseModel

from flink_recovery.constants import COMMANDS_FILE, METADATA_FILE, SINK_FILE_GLOB
from flink_recovery.data.jsonl import JsonLinesReader
from flink_recovery.domain.journal import CommandRecord, ProcessedRecord, RunJournal

ModelT = TypeVar("ModelT", bound=BaseModel)


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
        return RunJournal.from_parts(
            self.commands(), self.records(ProcessedRecord)
        )

    def read_metadata(self, model: type[ModelT]) -> ModelT:
        return model.model_validate_json(self.metadata_path.read_text())

    def write_metadata(self, metadata: BaseModel) -> None:
        self.create()
        self.metadata_path.write_text(metadata.model_dump_json(indent=2) + "\n")
