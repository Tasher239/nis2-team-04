import sys
import time
from contextlib import suppress
from pathlib import Path
from urllib.parse import unquote, urlsplit

from pydantic import BaseModel, ConfigDict, NonNegativeInt
from pyflink.common import JobStatus
from pyflink.common.completable_future import CompletableFuture
from pyflink.common.job_client import JobClient
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.java_gateway import get_gateway

from flink_recovery.config import Settings
from flink_recovery.constants import (
    FLINK_OPERATION_TIMEOUT_S,
    JOB_NAME,
    POLL_INTERVAL_S,
    RUNNING_TIMEOUT_S,
    SAVEPOINT_METADATA_FILE,
    SAVEPOINTS_DIR,
)
from flink_recovery.data.run_directory import RunDirectory, portable_path
from flink_recovery.domain.journal import CommandEvent, CommandRecord
from flink_recovery.streaming.environment import FlinkEnvironmentFactory
from flink_recovery.streaming.pipeline import PurchaseTotalsPipeline


class RunMetadata(BaseModel):
    model_config = ConfigDict(frozen=True)

    job_id: str
    net_runtime_ms: NonNegativeInt
    parallelism: int
    checkpoint_directory: str
    restored_from: str | None = None


class SavepointMetadata(BaseModel):
    model_config = ConfigDict(frozen=True)

    job_id: str
    parallelism: int
    savepoint_path: str
    savepoint_bytes: NonNegativeInt
    files: dict[str, NonNegativeInt]
    format: str = "canonical"


def wait_for(future: CompletableFuture, timeout_s: float = FLINK_OPERATION_TIMEOUT_S):
    deadline = time.monotonic() + timeout_s
    while not future.done():
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Flink operation exceeded {timeout_s} seconds")
        time.sleep(POLL_INTERVAL_S)
    return future.result()


def snapshot_files(path: Path) -> dict[str, int]:
    return {str(p.relative_to(path)): p.stat().st_size for p in path.rglob("*") if p.is_file()}


class CommandLog:
    def __init__(self, run_directory: RunDirectory) -> None:
        self._directory = run_directory

    def record(self, event: CommandEvent) -> None:
        self._directory.create()
        record = CommandRecord(event=event, time_ns=time.time_ns())
        with self._directory.commands_path.open("a") as stream:
            stream.write(record.model_dump_json() + "\n")


class CanonicalSavepointStopper:
    def __init__(self, client: JobClient) -> None:
        self._client = client

    def stop(self, target: Path) -> Path:
        canonical = get_gateway().jvm.org.apache.flink.core.execution.SavepointFormatType.CANONICAL
        future = self._client._j_job_client.stopWithSavepoint(
            False, target.absolute().as_uri(), canonical
        )
        uri = wait_for(CompletableFuture(future, str))
        parsed = urlsplit(uri)
        if parsed.scheme != "file" or parsed.netloc not in ("", "localhost"):
            raise ValueError(f"Savepoint must be on the local file system: {uri}")
        return Path(unquote(parsed.path))


class JobRunner:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._run_directory = RunDirectory(settings.output_path)

    def _environment(self, savepoint: Path | None = None) -> StreamExecutionEnvironment:
        settings = self._settings
        env = FlinkEnvironmentFactory(
            parallelism=settings.parallelism,
            checkpoint_uri=settings.checkpoint_path.absolute().as_uri(),
            checkpoint_interval_ms=settings.checkpoint_interval_ms,
            python_executable=sys.executable,
            savepoint_uri=savepoint.resolve().as_uri() if savepoint is not None else None,
        ).create()
        PurchaseTotalsPipeline(
            env,
            settings.input_path.absolute().as_uri(),
            self._run_directory.path.absolute().as_uri(),
        ).build()
        return env

    def _check_restore(self, savepoint: Path) -> None:
        directory = self._run_directory
        if not savepoint.exists():
            raise FileNotFoundError(savepoint)
        if not directory.commands_path.exists():
            raise ValueError("Restore requires the original output directory and command log")
        saved = directory.read_model(directory.savepoint_path, SavepointMetadata)
        supplied = savepoint.parent if savepoint.name == SAVEPOINT_METADATA_FILE else savepoint
        if Path(saved.savepoint_path).resolve() != supplied.resolve():
            raise ValueError("Savepoint does not belong to this output directory")
        if any(c.event == CommandEvent.RESTORE_REQUESTED for c in directory.commands()):
            raise ValueError("Use one restoration per run directory")

    def run(self, savepoint: Path | None = None) -> RunMetadata:
        if savepoint is None and self._run_directory.exists():
            raise FileExistsError("Output directory must not exist; choose a new RUN_NAME")
        if savepoint is not None:
            self._check_restore(savepoint)
        env = self._environment(savepoint)
        log = CommandLog(self._run_directory)
        restoring = savepoint is not None
        log.record(CommandEvent.RESTORE_REQUESTED if restoring else CommandEvent.EXECUTE_REQUESTED)
        result = env.execute(JOB_NAME)
        log.record(CommandEvent.JOB_FINISHED)
        metadata = RunMetadata(
            job_id=str(result.get_job_id()),
            net_runtime_ms=result.get_net_runtime(),
            parallelism=self._settings.parallelism,
            checkpoint_directory=portable_path(self._settings.checkpoint_path),
            restored_from=portable_path(savepoint) if savepoint is not None else None,
        )
        self._run_directory.write_metadata(metadata)
        return metadata

    def _wait_running(self, client: JobClient) -> None:
        deadline = time.monotonic() + RUNNING_TIMEOUT_S
        while wait_for(client.get_job_status()) != JobStatus.RUNNING:
            if client.get_job_execution_result().done():
                wait_for(client.get_job_execution_result())
                raise RuntimeError("Job finished before reaching the stop point")
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Job did not reach RUNNING in {RUNNING_TIMEOUT_S} seconds")
            time.sleep(POLL_INTERVAL_S)

    def stop_with_savepoint(self, stop_after_s: float) -> SavepointMetadata:
        if self._run_directory.exists():
            raise FileExistsError("Output directory must not exist; choose a new RUN_NAME")
        env = self._environment()
        log = CommandLog(self._run_directory)
        log.record(CommandEvent.EXECUTE_REQUESTED)
        client = env.execute_async(JOB_NAME)
        try:
            self._wait_running(client)
            time.sleep(stop_after_s)
            log.record(CommandEvent.STOP_REQUESTED)
            target = self._settings.output_root / SAVEPOINTS_DIR / self._settings.run_name
            path = CanonicalSavepointStopper(client).stop(target)
            wait_for(client.get_job_execution_result())
            log.record(CommandEvent.SAVEPOINT_COMPLETED)
            files = snapshot_files(path)
            if SAVEPOINT_METADATA_FILE not in files or not sum(files.values()):
                raise ValueError("Savepoint is empty or missing _metadata")
            metadata = SavepointMetadata(
                job_id=str(client.get_job_id()),
                parallelism=self._settings.parallelism,
                savepoint_path=portable_path(path),
                savepoint_bytes=sum(files.values()),
                files=files,
            )
            self._run_directory.write_model(self._run_directory.savepoint_path, metadata)
            return metadata
        except BaseException:
            with suppress(Exception):
                wait_for(client.cancel())
            raise
