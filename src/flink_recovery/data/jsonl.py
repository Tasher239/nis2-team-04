import json
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import TypeVar

from pydantic import BaseModel

ModelT = TypeVar("ModelT", bound=BaseModel)


class JsonLinesReader:
    def __init__(self, path: Path) -> None:
        self._path = path

    def lines(self) -> Iterator[str]:
        return (line for line in self._path.read_text().splitlines() if line.strip())

    def objects(self) -> list[dict]:
        return [json.loads(line) for line in self.lines()]

    def models(self, model: type[ModelT]) -> list[ModelT]:
        return [model.model_validate(json.loads(line)) for line in self.lines()]


class JsonLinesWriter:
    def __init__(self, path: Path) -> None:
        self._path = path

    def write(self, rows: Iterable[BaseModel | dict]) -> int:
        text = "".join(
            json.dumps(row.model_dump(mode="json") if isinstance(row, BaseModel) else row) + "\n"
            for row in rows
        )
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(text)
        return len(text.splitlines())
