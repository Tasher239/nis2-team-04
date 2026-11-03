from pathlib import Path
from unittest.mock import Mock

import pytest

from flink_recovery.config import Settings
from flink_recovery.data.run_directory import RunDirectory
from flink_recovery.domain.journal import CommandEvent
from flink_recovery.streaming import runner
from flink_recovery.streaming.runner import (
    CanonicalSavepointStopper,
    CommandLog,
    JobRunner,
    SavepointMetadata,
    snapshot_files,
)


@pytest.mark.parametrize("prefix", ["file:", "file://", "file://localhost"])
def test_savepoint_uri_decodes_local_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, prefix: str
) -> None:
    snapshot = tmp_path / "данные с пробелом #100%" / "savepoint"
    snapshot.mkdir(parents=True)
    (snapshot / "_metadata").write_bytes(b"state")
    encoded_path = snapshot.as_uri().removeprefix("file://")
    monkeypatch.setattr(runner, "get_gateway", Mock())
    monkeypatch.setattr(runner, "wait_for", lambda future: prefix + encoded_path)

    path = CanonicalSavepointStopper(Mock()).stop(tmp_path)

    assert path == snapshot
    assert snapshot_files(path) == {"_metadata": 5}


@pytest.mark.parametrize("uri", ["hdfs:///savepoint", "file://remote-host/savepoint"])
def test_savepoint_uri_rejects_nonlocal_storage(
    monkeypatch: pytest.MonkeyPatch, uri: str
) -> None:
    monkeypatch.setattr(runner, "get_gateway", Mock())
    monkeypatch.setattr(runner, "wait_for", lambda future: uri)
    with pytest.raises(ValueError, match="local file system"):
        CanonicalSavepointStopper(Mock()).stop(Path("/tmp"))


def stopped_run(tmp_path: Path) -> tuple[Settings, Path]:
    settings = Settings(_env_file=None, output_root=tmp_path, run_name="stopped")
    directory = RunDirectory(settings.output_path)
    CommandLog(directory).record(CommandEvent.SAVEPOINT_COMPLETED)
    snapshot = tmp_path / "savepoint"
    snapshot.mkdir()
    (directory.path / "savepoint.json").write_text(
        SavepointMetadata(
            job_id="job",
            parallelism=2,
            savepoint_path=str(snapshot),
            savepoint_bytes=1,
            files={"_metadata": 1},
        ).model_dump_json()
    )
    return settings, snapshot


def test_commands_survive_new_logger_and_keep_original_times(tmp_path: Path) -> None:
    directory = RunDirectory(tmp_path)
    CommandLog(directory).record(CommandEvent.STOP_REQUESTED)
    first = directory.commands()[0]
    CommandLog(directory).record(CommandEvent.SAVEPOINT_COMPLETED)
    assert directory.commands()[0] == first
    assert [c.event for c in directory.commands()] == [
        CommandEvent.STOP_REQUESTED,
        CommandEvent.SAVEPOINT_COMPLETED,
    ]


def test_fresh_run_does_not_overwrite_stopped_output(tmp_path: Path) -> None:
    settings, _ = stopped_run(tmp_path)
    with pytest.raises(FileExistsError):
        JobRunner(settings).run()


def test_restore_rejects_snapshot_from_another_run(tmp_path: Path) -> None:
    settings, _ = stopped_run(tmp_path)
    other = tmp_path / "other-savepoint"
    other.mkdir()
    with pytest.raises(ValueError, match="does not belong"):
        JobRunner(settings).run(other)
    assert len(RunDirectory(settings.output_path).commands()) == 1


def test_restore_rejects_second_restore_before_writing_anything(tmp_path: Path) -> None:
    settings, snapshot = stopped_run(tmp_path)
    directory = RunDirectory(settings.output_path)
    CommandLog(directory).record(CommandEvent.RESTORE_REQUESTED)
    with pytest.raises(ValueError, match="one restoration"):
        JobRunner(settings).run(snapshot)
    assert len(directory.commands()) == 2
