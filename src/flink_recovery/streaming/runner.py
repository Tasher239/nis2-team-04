import sys
import time

from pydantic import BaseModel, ConfigDict, NonNegativeInt

from flink_recovery.config import Settings
from flink_recovery.constants import JOB_NAME
from flink_recovery.data.jsonl import JsonLinesWriter
from flink_recovery.data.run_directory import RunDirectory
from flink_recovery.domain.journal import CommandEvent, CommandRecord
from flink_recovery.streaming.environment import FlinkEnvironmentFactory
from flink_recovery.streaming.pipeline import PurchaseTotalsPipeline


class RunMetadata(BaseModel):
    model_config = ConfigDict(frozen=True)

    job_id: str
    net_runtime_ms: NonNegativeInt
    parallelism: int
    checkpoint_directory: str


class CommandLog:
    def __init__(self) -> None:
        self._commands: list[CommandRecord] = []

    def record(self, event: CommandEvent) -> None:
        self._commands.append(CommandRecord(event=event, time_ns=time.time_ns()))

    def write(self, run_directory: RunDirectory) -> None:
        JsonLinesWriter(run_directory.commands_path).write(self._commands)


class JobRunner:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._run_directory = RunDirectory(settings.output_path)

    def run(self) -> RunMetadata:
        if self._run_directory.exists():
            raise FileExistsError("Output directory must not exist; choose a new RUN_NAME")
        settings = self._settings
        env = FlinkEnvironmentFactory(
            parallelism=settings.parallelism,
            checkpoint_uri=settings.checkpoint_path.absolute().as_uri(),
            checkpoint_interval_ms=settings.checkpoint_interval_ms,
            python_executable=sys.executable,
        ).create()
        PurchaseTotalsPipeline(
            env,
            settings.input_path.absolute().as_uri(),
            self._run_directory.path.absolute().as_uri(),
        ).build()
        log = CommandLog()
        log.record(CommandEvent.EXECUTE_REQUESTED)
        result = env.execute(JOB_NAME)
        log.record(CommandEvent.JOB_FINISHED)
        log.write(self._run_directory)
        metadata = RunMetadata(
            job_id=str(result.get_job_id()),
            net_runtime_ms=result.get_net_runtime(),
            parallelism=settings.parallelism,
            checkpoint_directory=str(settings.checkpoint_path),
        )
        self._run_directory.write_metadata(metadata)
        return metadata
