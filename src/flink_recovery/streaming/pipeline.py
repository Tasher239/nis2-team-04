from pyflink.common import Encoder, Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import FileSink, FileSource, StreamFormat

from flink_recovery.constants import JOB_NAME, PARSE_UID, SINK_UID, SOURCE_UID, TOTALS_UID
from flink_recovery.streaming.functions import PurchaseTotalsFunction, parse_purchase


class PurchaseTotalsPipeline:
    def __init__(
        self,
        env: StreamExecutionEnvironment,
        input_uri: str,
        output_uri: str,
    ) -> None:
        self._env = env
        self._input_uri = input_uri
        self._output_uri = output_uri

    def build(self) -> None:
        source = FileSource.for_record_stream_format(
            StreamFormat.text_line_format(), self._input_uri
        ).build()
        lines = self._env.from_source(source, WatermarkStrategy.no_watermarks(), "purchases")
        lines.set_parallelism(1).uid(SOURCE_UID)
        events = lines.map(
            parse_purchase,
            output_type=Types.TUPLE([Types.LONG(), Types.LONG(), Types.LONG()]),
        ).uid(PARSE_UID)
        totals = (
            events.key_by(lambda event: event[1], key_type=Types.LONG())
            .process(PurchaseTotalsFunction(), output_type=Types.STRING())
            .uid(TOTALS_UID)
        )
        sink = FileSink.for_row_format(
            self._output_uri, Encoder.simple_string_encoder("UTF-8")
        ).build()
        totals.sink_to(sink).uid(SINK_UID)

    def execute(self) -> str:
        return str(self._env.execute(JOB_NAME).get_job_id())
