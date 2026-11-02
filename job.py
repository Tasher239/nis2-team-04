import json
import os
import sys
from collections.abc import Iterator
from pathlib import Path

from dotenv import load_dotenv
from pyflink.common import Configuration, Encoder, Types, WatermarkStrategy
from pyflink.datastream import KeyedProcessFunction, StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import FileSink, FileSource, StreamFormat
from pyflink.datastream.functions import RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor


def parse_purchase(line: str) -> tuple[int, int, int]:
    event = json.loads(line)
    return event["event_id"], event["user_id"], event["amount_cents"]


class PurchaseTotals(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext) -> None:
        self.count = runtime_context.get_state(ValueStateDescriptor("count", Types.LONG()))
        self.total = runtime_context.get_state(ValueStateDescriptor("total", Types.LONG()))
        self.subtask = runtime_context.get_index_of_this_subtask()

    def process_element(
        self, event: tuple[int, int, int], ctx: KeyedProcessFunction.Context
    ) -> Iterator[str]:
        count = (self.count.value() or 0) + 1
        total = (self.total.value() or 0) + event[2]
        self.count.update(count)
        self.total.update(total)
        yield json.dumps({
            "event_id": event[0],
            "user_id": event[1],
            "amount_cents": event[2],
            "purchase_count": count,
            "total_cents": total,
            "subtask": self.subtask,
        })


def create_environment(
    parallelism: int, checkpoint_path: Path, checkpoint_interval_ms: int
) -> StreamExecutionEnvironment:
    if parallelism < 1:
        raise ValueError("PARALLELISM must be positive")
    config = Configuration()
    config.set_string("execution.checkpointing.dir", checkpoint_path.as_uri())
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(parallelism)
    env.set_python_executable(sys.executable)
    env.enable_checkpointing(checkpoint_interval_ms)
    return env


def build_pipeline(
    env: StreamExecutionEnvironment, input_path: Path, output_path: Path
) -> None:
    source = FileSource.for_record_stream_format(
        StreamFormat.text_line_format(), input_path.as_uri()
    ).build()
    lines = env.from_source(source, WatermarkStrategy.no_watermarks(), "purchases")
    lines.set_parallelism(1).uid("purchases-source")
    events = lines.map(
        parse_purchase,
        output_type=Types.TUPLE([Types.LONG(), Types.LONG(), Types.LONG()]),
    )
    totals = events.key_by(lambda event: event[1], key_type=Types.LONG()).process(
        PurchaseTotals(), output_type=Types.STRING()
    ).uid("purchase-totals")
    sink = FileSink.for_row_format(
        output_path.as_uri(), Encoder.simple_string_encoder("UTF-8")
    ).build()
    totals.sink_to(sink).uid("purchase-output")


def main() -> None:
    load_dotenv(Path(__file__).with_name(".env"))
    input_path = Path(os.environ["INPUT_PATH"]).resolve()
    output_path = (Path("output") / os.environ["RUN_NAME"]).resolve()
    parallelism = int(os.environ["PARALLELISM"])
    checkpoint_path = Path(os.environ["CHECKPOINT_PATH"]).resolve()
    checkpoint_interval_ms = int(os.environ["CHECKPOINT_INTERVAL_MS"])
    if output_path.exists():
        raise FileExistsError("Output directory must not exist; choose a new RUN_NAME")

    env = create_environment(parallelism, checkpoint_path, checkpoint_interval_ms)
    build_pipeline(env, input_path, output_path)
    result = env.execute("purchase-totals")
    metadata = {
        "job_id": str(result.get_job_id()),
        "net_runtime_ms": result.get_net_runtime(),
        "parallelism": parallelism,
        "checkpoint_directory": str(checkpoint_path),
    }
    (output_path / "run.json").write_text(json.dumps(metadata, indent=2) + "\n")
    print(json.dumps(metadata))


if __name__ == "__main__":
    main()
