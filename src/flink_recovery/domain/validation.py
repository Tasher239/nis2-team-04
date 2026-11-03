from collections import Counter
from collections.abc import Sequence

from pydantic import BaseModel, Field, computed_field

from flink_recovery.domain.purchases import AggregateRecord, Purchase, UserTotals


class ValidationReport(BaseModel):
    missing_event_ids: list[int] = Field(default_factory=list)
    unexpected_event_ids: list[int] = Field(default_factory=list)
    duplicate_events: int = 0
    event_errors: int = 0
    aggregate_errors: list[int] = Field(default_factory=list)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def ok(self) -> bool:
        return not (
            self.missing_event_ids
            or self.unexpected_event_ids
            or self.duplicate_events
            or self.event_errors
            or self.aggregate_errors
        )


class ResultValidator:
    def validate(
        self, events: Sequence[Purchase], records: Sequence[AggregateRecord]
    ) -> ValidationReport:
        source = {event.event_id: event for event in events}
        if len(source) != len(events):
            raise ValueError("Input contains duplicate event_id values")
        expected = self._expected_totals(events)
        seen = Counter(record.event_id for record in records)
        event_errors = sum(
            1
            for record in records
            if record.event_id in source
            and (
                record.user_id != source[record.event_id].user_id
                or record.amount_cents != source[record.event_id].amount_cents
            )
        )
        final = self._final_totals(records)
        users = sorted(expected.keys() | final.keys())
        return ValidationReport(
            missing_event_ids=sorted(source.keys() - seen.keys()),
            unexpected_event_ids=sorted(seen.keys() - source.keys()),
            duplicate_events=sum(count - 1 for count in seen.values()),
            event_errors=event_errors,
            aggregate_errors=[user for user in users if expected.get(user) != final.get(user)],
        )

    @staticmethod
    def _expected_totals(events: Sequence[Purchase]) -> dict[int, UserTotals]:
        expected: dict[int, UserTotals] = {}
        for event in events:
            expected[event.user_id] = expected.get(
                event.user_id, UserTotals.empty()
            ).add(event.amount_cents)
        return expected

    @staticmethod
    def _final_totals(records: Sequence[AggregateRecord]) -> dict[int, UserTotals]:
        final: dict[int, UserTotals] = {}
        for record in records:
            current = final.get(record.user_id)
            if current is None or record.purchase_count > current.purchase_count:
                final[record.user_id] = UserTotals(
                    purchase_count=record.purchase_count,
                    total_cents=record.total_cents,
                )
        return final
