from typing import Self

from pydantic import BaseModel, ConfigDict, NonNegativeInt, PositiveInt


class Purchase(BaseModel):
    model_config = ConfigDict(frozen=True)

    event_id: PositiveInt
    user_id: PositiveInt
    amount_cents: int


class AggregateRecord(BaseModel):
    model_config = ConfigDict(frozen=True)

    event_id: PositiveInt
    user_id: PositiveInt
    amount_cents: int
    purchase_count: PositiveInt
    total_cents: int
    subtask: NonNegativeInt
    processed_at_ns: NonNegativeInt


class UserTotals(BaseModel):
    model_config = ConfigDict(frozen=True)

    purchase_count: NonNegativeInt = 0
    total_cents: int = 0

    @classmethod
    def empty(cls) -> Self:
        return cls()

    def add(self, amount_cents: int) -> Self:
        return self.model_copy(
            update={
                "purchase_count": self.purchase_count + 1,
                "total_cents": self.total_cents + amount_cents,
            }
        )
