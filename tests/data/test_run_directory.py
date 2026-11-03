import json
from pathlib import Path

from flink_recovery.data.run_directory import RunDirectory
from flink_recovery.domain.journal import CommandEvent, CommandRecord


def write_parts(directory: Path, rows: list[dict]) -> None:
    (directory / "nested").mkdir(parents=True)
    (directory / "nested" / "part-0").write_text(
        "".join(json.dumps(row) + "\n" for row in rows)
    )


def test_records_come_from_every_part_file(tmp_path: Path) -> None:
    write_parts(tmp_path, [{"processed_at_ns": 1}, {"processed_at_ns": 2}])
    (tmp_path / "commands.jsonl").write_text(
        json.dumps({"event": "job_finished", "time_ns": 5}) + "\n"
    )
    journal = RunDirectory(tmp_path).journal()
    assert [record.processed_at_ns for record in journal.records] == [1, 2]
    assert journal.of(CommandEvent.JOB_FINISHED) == (
        CommandRecord(event=CommandEvent.JOB_FINISHED, time_ns=5),
    )


def test_missing_commands_journal_is_empty(tmp_path: Path) -> None:
    write_parts(tmp_path, [{"processed_at_ns": 1}])
    assert RunDirectory(tmp_path).commands() == []


def test_metadata_round_trip(tmp_path: Path) -> None:
    from flink_recovery.streaming.runner import RunMetadata

    directory = RunDirectory(tmp_path / "run")
    metadata = RunMetadata(
        job_id="job",
        net_runtime_ms=10,
        parallelism=2,
        checkpoint_directory="output/checkpoints",
    )
    directory.write_metadata(metadata)
    assert directory.read_metadata(RunMetadata) == metadata
