from collections.abc import Iterator
from pathlib import Path
from random import Random

from pydantic import PositiveInt

from flink_recovery.constants import DEFAULT_SEED, MAX_AMOUNT_CENTS, MIN_AMOUNT_CENTS
from flink_recovery.data.jsonl import JsonLinesWriter
from flink_recovery.domain.purchases import Purchase


class PurchaseGenerator:
    def __init__(
        self,
        user_count: PositiveInt,
        purchases_per_user: PositiveInt,
        seed: int = DEFAULT_SEED,
    ) -> None:
        self._user_count = user_count
        self._purchases_per_user = purchases_per_user
        self._seed = seed

    def events(self) -> Iterator[Purchase]:
        rng = Random(self._seed)
        for purchase_index in range(self._purchases_per_user):
            for user_id in range(1, self._user_count + 1):
                yield Purchase(
                    event_id=purchase_index * self._user_count + user_id,
                    user_id=user_id,
                    amount_cents=rng.randint(MIN_AMOUNT_CENTS, MAX_AMOUNT_CENTS),
                )

    def write(self, path: Path) -> int:
        return JsonLinesWriter(path).write(self.events())
