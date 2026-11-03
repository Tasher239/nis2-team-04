import json
import time
from collections.abc import Iterator

from pyflink.common import Types
from pyflink.datastream import KeyedProcessFunction
from pyflink.datastream.functions import RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor

from flink_recovery.constants import COUNT_STATE, TOTAL_STATE
from flink_recovery.domain.purchases import AggregateRecord


def parse_purchase(line: str) -> tuple[int, int, int]:
    event = json.loads(line)
    return event["event_id"], event["user_id"], event["amount_cents"]


class PurchaseTotalsFunction(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext) -> None:
        self.count = runtime_context.get_state(ValueStateDescriptor(COUNT_STATE, Types.LONG()))
        self.total = runtime_context.get_state(ValueStateDescriptor(TOTAL_STATE, Types.LONG()))
        self.subtask = runtime_context.get_index_of_this_subtask()

    def process_element(
        self, event: tuple[int, int, int], ctx: KeyedProcessFunction.Context
    ) -> Iterator[str]:
        count = (self.count.value() or 0) + 1
        total = (self.total.value() or 0) + event[2]
        self.count.update(count)
        self.total.update(total)
        yield AggregateRecord(
            event_id=event[0],
            user_id=event[1],
            amount_cents=event[2],
            purchase_count=count,
            total_cents=total,
            subtask=self.subtask,
            processed_at_ns=time.time_ns(),
        ).model_dump_json()
