from flink_recovery.domain.purchases import UserTotals


def test_add_does_not_mutate_receiver() -> None:
    zero = UserTotals.empty()
    once = zero.add(500)
    assert zero == UserTotals(purchase_count=0, total_cents=0)
    assert once == UserTotals(purchase_count=1, total_cents=500)


def test_add_accumulates() -> None:
    totals = UserTotals.empty().add(300).add(700)
    assert totals.purchase_count == 2
    assert totals.total_cents == 1000


def test_empty_is_identity() -> None:
    assert UserTotals.empty().add(1) == UserTotals(purchase_count=1, total_cents=1)
