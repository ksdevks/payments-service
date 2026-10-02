import random
import uuid
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from payments.domain import ChargeOutcome, Currency, Money, Payment
from payments.infrastructure.gateway import EmulatedGateway
from tests.fakes import InMemoryChargeLedger


def payment() -> Payment:
    return Payment.create(
        money=Money(Decimal("1.00"), Currency.RUB),
        description="x",
        metadata={},
        idempotency_key=str(uuid.uuid4()),
        request_fingerprint="f",
        webhook_url="https://example.com",
        now=datetime(2026, 1, 1, tzinfo=UTC),
    )


async def no_sleep(_: float) -> None:
    return None


async def test_recorded_outcome_survives_restart_and_config_change() -> None:
    ledger = InMemoryChargeLedger()
    p = payment()

    first = await EmulatedGateway(ledger, success_rate=1.0, sleep=no_sleep).charge(p)
    # Новый процесс с противоположной политикой: результат уже зафиксирован.
    again = await EmulatedGateway(ledger, success_rate=0.0, sleep=no_sleep).charge(p)

    assert first is again is ChargeOutcome.SUCCEEDED
    assert ledger.records == {p.id: ChargeOutcome.SUCCEEDED}


async def test_replay_does_not_process_again() -> None:
    ledger = InMemoryChargeLedger()
    p = payment()
    delays: list[float] = []

    async def record(delay: float) -> None:
        delays.append(delay)

    gateway = EmulatedGateway(ledger, sleep=record)
    await gateway.charge(p)
    await gateway.charge(p)

    assert len(delays) == 1


async def test_delay_bounds_and_success_rate() -> None:
    delays: list[float] = []

    async def record(delay: float) -> None:
        delays.append(delay)

    gateway = EmulatedGateway(InMemoryChargeLedger(), rng=random.Random(7), sleep=record)
    outcomes = [await gateway.charge(payment()) for _ in range(3000)]

    assert all(2.0 <= d <= 5.0 for d in delays)
    success_share = outcomes.count(ChargeOutcome.SUCCEEDED) / len(outcomes)
    assert 0.87 < success_share < 0.93


def test_invalid_configuration() -> None:
    with pytest.raises(ValueError):
        EmulatedGateway(InMemoryChargeLedger(), min_delay=5, max_delay=2)
    with pytest.raises(ValueError):
        EmulatedGateway(InMemoryChargeLedger(), success_rate=1.5)
