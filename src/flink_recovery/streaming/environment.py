from pyflink.common import Configuration
from pyflink.datastream import StreamExecutionEnvironment


class FlinkEnvironmentFactory:
    def __init__(
        self,
        parallelism: int,
        checkpoint_uri: str,
        checkpoint_interval_ms: int,
        python_executable: str,
        savepoint_uri: str | None = None,
    ) -> None:
        self._parallelism = parallelism
        self._checkpoint_uri = checkpoint_uri
        self._checkpoint_interval_ms = checkpoint_interval_ms
        self._python_executable = python_executable
        self._savepoint_uri = savepoint_uri

    def create(self) -> StreamExecutionEnvironment:
        config = Configuration()
        config.set_string("execution.checkpointing.dir", self._checkpoint_uri)
        if self._savepoint_uri is not None:
            config.set_string("execution.state-recovery.path", self._savepoint_uri)
        env = StreamExecutionEnvironment.get_execution_environment(config)
        env.set_parallelism(self._parallelism)
        env.set_python_executable(self._python_executable)
        env.enable_checkpointing(self._checkpoint_interval_ms)
        return env
