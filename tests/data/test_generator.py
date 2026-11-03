from pathlib import Path

import pytest

from flink_recovery.data.generator import PurchaseGenerator
from flink_recovery.data.jsonl import JsonLinesReader, JsonLinesWriter
from flink_recovery.domain.purchases import Purchase

SAMPLE_INPUT = Path(__file__).resolve().parents[2] / "data" / "purchases.jsonl"


def test_generation_is_deterministic(tmp_path: Path) -> None:
    first = tmp_path / "first.jsonl"
    second = tmp_path / "second.jsonl"
    PurchaseGenerator(user_count=5, purchases_per_user=4).write(first)
    PurchaseGenerator(user_count=5, purchases_per_user=4).write(second)
    assert first.read_bytes() == second.read_bytes()


def test_event_ids_are_consecutive(tmp_path: Path) -> None:
    path = tmp_path / "purchases.jsonl"
    written = PurchaseGenerator(user_count=3, purchases_per_user=2).write(path)
    events = JsonLinesReader(path).models(Purchase)
    assert written == 6
    assert [event.event_id for event in events] == [1, 2, 3, 4, 5, 6]


def test_round_trip(tmp_path: Path) -> None:
    path = tmp_path / "nested" / "events.jsonl"
    purchase = Purchase(event_id=1, user_id=7, amount_cents=500)
    assert JsonLinesWriter(path).write([purchase, purchase.model_dump()]) == 2
    assert JsonLinesReader(path).models(Purchase) == [purchase, purchase]


def test_rejects_invalid_event(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    path.write_text('{"event_id": 0, "user_id": 1, "amount_cents": 10}\n')
    with pytest.raises(ValueError):
        JsonLinesReader(path).models(Purchase)
