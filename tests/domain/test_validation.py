import pytest

from flink_recovery.domain.purchases import AggregateRecord, Purchase
from flink_recovery.domain.validation import ResultValidator


def purchase(event_id: int, user_id: int, amount_cents: int) -> Purchase:
    return Purchase(event_id=event_id, user_id=user_id, amount_cents=amount_cents)


def record(source: Purchase, purchase_count: int, total_cents: int) -> AggregateRecord:
    return AggregateRecord(
        event_id=source.event_id,
        user_id=source.user_id,
        amount_cents=source.amount_cents,
        purchase_count=purchase_count,
        total_cents=total_cents,
        subtask=0,
        processed_at_ns=0,
    )


@pytest.fixture
def events() -> list[Purchase]:
    return [purchase(1, 101, 1200), purchase(2, 202, 3400), purchase(3, 101, 800)]


@pytest.fixture
def records(events: list[Purchase]) -> list[AggregateRecord]:
    return [
        record(events[0], 1, 1200),
        record(events[1], 1, 3400),
        record(events[2], 2, 2000),
    ]


def test_valid_output_in_different_file_order(events, records) -> None:
    assert ResultValidator().validate(events, records[::-1]).ok


def test_missing_event(events, records) -> None:
    report = ResultValidator().validate(events, records[1:])
    assert not report.ok
    assert report.missing_event_ids == [1]


def test_duplicate_event(events, records) -> None:
    report = ResultValidator().validate(events, records + [records[0]])
    assert not report.ok
    assert report.duplicate_events == 1


def test_wrong_aggregate(events, records) -> None:
    records[2] = record(events[2], 2, 2001)
    report = ResultValidator().validate(events, records)
    assert not report.ok
    assert report.aggregate_errors == [101]


def test_wrong_event_contents(events, records) -> None:
    records[0] = record(purchase(1, 101, 1201), 1, 1200)
    report = ResultValidator().validate(events, records)
    assert not report.ok
    assert report.event_errors == 1
