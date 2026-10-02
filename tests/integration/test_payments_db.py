import asyncio
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncEngine

from payments.application.commands import CreatePaymentCommand
from payments.application.errors import IdempotencyKeyConflictError
from payments.application.events import PaymentCreated
from payments.application.ports import UnitOfWorkFactory
from payments.application.use_cases import CreatePayment, GetPayment, ProcessPayment
from payments.domain import ChargeOutcome, Currency, Payment, PaymentStatus
from payments.infrastructure.db.models import OutboxRecord, PaymentRecord
from tests.fakes import FakeGateway, FakeWebhookSender, FixedClock

pytestmark = pytest.mark.postgres


def command(**overrides: object) -> CreatePaymentCommand:
    data: dict[str, object] = {
        "idempotency_key": "key-1",
        "amount": Decimal("1250.50"),
        "currency": Currency.RUB,
        "description": "Order 42",
        "webhook_url": "https://example.com/hook",
        "metadata": {"order_id": 42, "tags": ["a", "b"]},
    }
    data.update(overrides)
    return CreatePaymentCommand(**data)  # type: ignore[arg-type]


async def count(engine: AsyncEngine, model: type) -> int:
    async with engine.connect() as connection:
        return int(await connection.scalar(select(func.count()).select_from(model)) or 0)


async def test_create_persists_payment_and_outbox(
    uow_factory: UnitOfWorkFactory, engine: AsyncEngine
) -> None:
    clock = FixedClock(datetime(2026, 3, 1, 10, 0, tzinfo=UTC))
    created = await CreatePayment(uow_factory, clock).execute(command())

    loaded = await GetPayment(uow_factory).execute(created.payment.id)

    assert loaded.money.amount == Decimal("1250.50")
    assert loaded.money.currency is Currency.RUB
    assert loaded.metadata == {"order_id": 42, "tags": ["a", "b"]}
    assert loaded.status is PaymentStatus.PENDING
    assert loaded.created_at == clock.now()
    assert await count(engine, OutboxRecord) == 1


async def test_parallel_requests_with_same_key_create_one_payment(
    uow_factory: UnitOfWorkFactory, engine: AsyncEngine
) -> None:
    use_case = CreatePayment(uow_factory, FixedClock())

    results = await asyncio.gather(*(use_case.execute(command()) for _ in range(10)))

    assert sum(r.created for r in results) == 1
    assert len({r.payment.id for r in results}) == 1
    assert await count(engine, PaymentRecord) == 1
    assert await count(engine, OutboxRecord) == 1


async def test_same_key_different_body_is_conflict(uow_factory: UnitOfWorkFactory) -> None:
    use_case = CreatePayment(uow_factory, FixedClock())
    await use_case.execute(command())

    with pytest.raises(IdempotencyKeyConflictError):
        await use_case.execute(command(amount=Decimal("1.00")))


async def _create(uow_factory: UnitOfWorkFactory) -> Payment:
    return (await CreatePayment(uow_factory, FixedClock()).execute(command())).payment


async def test_processing_persists_status_and_webhook_marker(
    uow_factory: UnitOfWorkFactory,
) -> None:
    payment = await _create(uow_factory)
    clock = FixedClock(datetime(2026, 3, 1, 10, 5, tzinfo=UTC))
    process = ProcessPayment(
        uow_factory, FakeGateway([ChargeOutcome.SUCCEEDED]), FakeWebhookSender(), clock
    )

    await process.execute(PaymentCreated(event_id=payment.id, payment_id=payment.id))

    stored = await GetPayment(uow_factory).execute(payment.id)
    assert stored.status is PaymentStatus.SUCCEEDED
    assert stored.processed_at == clock.now()
    assert stored.webhook_delivered_at == clock.now()


class SlowGateway(FakeGateway):
    """Держит паузу, чтобы оба обработчика успели увидеть pending."""

    async def charge(self, payment: Payment) -> ChargeOutcome:
        await asyncio.sleep(0.1)
        return await super().charge(payment)


async def test_parallel_processing_charges_and_applies_once(
    uow_factory: UnitOfWorkFactory,
) -> None:
    payment = await _create(uow_factory)
    event = PaymentCreated(event_id=payment.id, payment_id=payment.id)
    gateway = SlowGateway([ChargeOutcome.SUCCEEDED, ChargeOutcome.DECLINED])
    process = ProcessPayment(uow_factory, gateway, FakeWebhookSender(), FixedClock())

    results = await asyncio.gather(process.execute(event), process.execute(event))

    # Оба обработчика обратились к шлюзу, но списание одно, и результат один.
    assert len(gateway.effects) == 1
    assert sum(r.outcome_applied for r in results) == 1
    stored = await GetPayment(uow_factory).execute(payment.id)
    assert stored.status is PaymentStatus.SUCCEEDED
    assert {r.status for r in results} == {PaymentStatus.SUCCEEDED}
