from decimal import Decimal

import pytest

from payments.application.commands import CreatePaymentCommand
from payments.application.errors import IdempotencyKeyConflictError
from payments.application.events import PAYMENTS_NEW_TOPIC, PaymentCreated
from payments.application.use_cases import CreatePayment
from payments.domain import Currency, PaymentStatus
from tests.fakes import FakeUnitOfWork, FixedClock, InMemoryStore


def command(**overrides) -> CreatePaymentCommand:
    data = {
        "idempotency_key": "key-1",
        "amount": Decimal("100.00"),
        "currency": Currency.RUB,
        "description": "Order 1",
        "webhook_url": "https://example.com/hook",
        "metadata": {"order_id": 1, "source": "web"},
    }
    data.update(overrides)
    return CreatePaymentCommand(**data)


def make_use_case(store: InMemoryStore) -> CreatePayment:
    return CreatePayment(lambda: FakeUnitOfWork(store), FixedClock())


async def test_creates_payment_and_outbox_message_atomically() -> None:
    store = InMemoryStore()

    result = await make_use_case(store).execute(command())

    assert result.created
    assert result.payment.status is PaymentStatus.PENDING
    assert list(store.payments) == [result.payment.id]
    assert len(store.outbox) == 1
    message = store.outbox[0]
    assert message.topic == PAYMENTS_NEW_TOPIC
    event = PaymentCreated.from_payload(message.payload)
    assert event.payment_id == result.payment.id
    assert message.id == event.event_id


async def test_replay_with_same_request_returns_existing_payment() -> None:
    store = InMemoryStore()
    use_case = make_use_case(store)
    first = await use_case.execute(command())

    # Та же сумма в другой записи и другой порядок ключей metadata - тот же запрос.
    second = await use_case.execute(
        command(amount=Decimal("100"), metadata={"source": "web", "order_id": 1})
    )

    assert not second.created
    assert second.payment.id == first.payment.id
    assert len(store.payments) == 1
    assert len(store.outbox) == 1


async def test_integral_numbers_in_metadata_are_the_same_request() -> None:
    store = InMemoryStore()
    use_case = make_use_case(store)
    first = await use_case.execute(command(metadata={"v": 1.0, "nested": [2.0, True]}))

    second = await use_case.execute(command(metadata={"v": 1, "nested": [2, True]}))

    assert not second.created
    assert second.payment.id == first.payment.id


async def test_same_key_with_different_request_is_conflict() -> None:
    store = InMemoryStore()
    use_case = make_use_case(store)
    await use_case.execute(command())

    with pytest.raises(IdempotencyKeyConflictError):
        await use_case.execute(command(amount=Decimal("101.00")))
    assert len(store.outbox) == 1


async def test_concurrent_insert_race_resolves_to_existing_payment() -> None:
    store = InMemoryStore()
    winner = await make_use_case(store).execute(command())

    class BlindFastPathUoW(FakeUnitOfWork):
        """Эмулирует гонку: быстрая проверка не видит запись, закоммиченную параллельно."""

        def __init__(self, store: InMemoryStore, blind: bool) -> None:
            super().__init__(store)
            if blind:
                self.payments.get_by_idempotency_key = self._miss

        @staticmethod
        async def _miss(key: str) -> None:
            return None

    calls = 0

    def factory() -> FakeUnitOfWork:
        nonlocal calls
        calls += 1
        return BlindFastPathUoW(store, blind=calls == 1)

    loser = CreatePayment(factory, FixedClock())
    result = await loser.execute(command())

    assert not result.created
    assert result.payment.id == winner.payment.id
    assert len(store.payments) == 1
    assert len(store.outbox) == 1
