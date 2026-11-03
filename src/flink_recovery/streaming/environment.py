from pyflink.common import Configuration
from pyflink.datastream import StreamExecutionEnvironment


class FlinkEnvironmentFactory:
    def __init__(
        self,
        parallelism: int,
        checkpoint_uri: str,
        checkpoint_interval_ms: int,
        python_executable: str,
    ) -> None:
        self._parallelism = parallelism
        self._checkpoint_uri = checkpoint_uri
        self._checkpoint_interval_ms = checkpoint_interval_ms
        self._python_executable = python_executable

    def create(self) -> StreamExecutionEnvironment:
        config = Configuration()
        config.set_string("execution.checkpointing.dir", self._checkpoint_uri)
        env = StreamExecutionEnvironment.get_execution_environment(config)
        env.set_parallelism(self._parallelism)
        env.set_python_executable(self._python_executable)
        env.enable_checkpointing(self._checkpoint_interval_ms)
        return env
