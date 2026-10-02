import uuid
from decimal import Decimal

import pytest

from payments.application.commands import CreatePaymentCommand
from payments.application.errors import (
    GatewayUnavailableError,
    PaymentNotFoundError,
    WebhookTemporaryError,
)
from payments.application.events import PaymentCreated
from payments.application.use_cases import CreatePayment, ProcessPayment
from payments.domain import ChargeOutcome, Currency, Payment, PaymentStatus
from tests.fakes import (
    FakeGateway,
    FakeUnitOfWork,
    FakeWebhookSender,
    FixedClock,
    InMemoryStore,
)


async def prepare() -> tuple[InMemoryStore, PaymentCreated, FixedClock]:
    store = InMemoryStore()
    clock = FixedClock()
    await CreatePayment(lambda: FakeUnitOfWork(store), clock).execute(
        CreatePaymentCommand(
            idempotency_key="key-1",
            amount=Decimal("100.00"),
            currency=Currency.USD,
            description="Order 1",
            webhook_url="https://example.com/hook",
        )
    )
    event = PaymentCreated.from_payload(store.outbox[0].payload)
    return store, event, clock


def make_use_case(store, gateway, webhooks, clock) -> ProcessPayment:
    return ProcessPayment(lambda: FakeUnitOfWork(store), gateway, webhooks, clock)


async def test_success_path_charges_and_notifies() -> None:
    store, event, clock = await prepare()
    gateway, webhooks = FakeGateway([ChargeOutcome.SUCCEEDED]), FakeWebhookSender()

    result = await make_use_case(store, gateway, webhooks, clock).execute(event)

    assert result.outcome_applied and result.webhook_sent
    payment = store.payments[event.payment_id]
    assert payment.status is PaymentStatus.SUCCEEDED
    assert payment.processed_at == clock.now()
    assert payment.webhook_delivered_at == clock.now()
    url, notification = webhooks.attempts[0]
    assert url == "https://example.com/hook"
    assert notification.status is PaymentStatus.SUCCEEDED
    assert notification.to_payload()["amount"] == "100.00"


async def test_declined_payment_is_failed_and_still_notified() -> None:
    store, event, clock = await prepare()
    webhooks = FakeWebhookSender()

    result = await make_use_case(
        store, FakeGateway([ChargeOutcome.DECLINED]), webhooks, clock
    ).execute(event)

    assert result.status is PaymentStatus.FAILED
    assert webhooks.attempts[0][1].status is PaymentStatus.FAILED


async def test_webhook_failure_retry_does_not_charge_again() -> None:
    store, event, clock = await prepare()
    gateway = FakeGateway([ChargeOutcome.SUCCEEDED, ChargeOutcome.DECLINED])
    webhooks = FakeWebhookSender([WebhookTemporaryError("timeout")])
    use_case = make_use_case(store, gateway, webhooks, clock)

    with pytest.raises(WebhookTemporaryError):
        await use_case.execute(event)
    payment = store.payments[event.payment_id]
    assert payment.status is PaymentStatus.SUCCEEDED
    assert payment.webhook_delivered_at is None

    result = await use_case.execute(event)

    assert len(gateway.calls) == 1
    assert not result.outcome_applied and result.webhook_sent
    assert store.payments[event.payment_id].status is PaymentStatus.SUCCEEDED
    first, second = webhooks.attempts
    assert first[1].notification_id == second[1].notification_id


async def test_duplicate_message_after_full_processing_is_noop() -> None:
    store, event, clock = await prepare()
    gateway, webhooks = FakeGateway(), FakeWebhookSender()
    use_case = make_use_case(store, gateway, webhooks, clock)
    await use_case.execute(event)
    commits_before = store.commits

    result = await use_case.execute(event)

    assert not result.outcome_applied and not result.webhook_sent
    assert len(gateway.calls) == 1
    assert len(webhooks.attempts) == 1
    assert store.commits == commits_before


async def test_gateway_outage_leaves_payment_pending() -> None:
    store, event, clock = await prepare()
    webhooks = FakeWebhookSender()
    use_case = make_use_case(
        store, FakeGateway([GatewayUnavailableError("down")]), webhooks, clock
    )

    with pytest.raises(GatewayUnavailableError):
        await use_case.execute(event)

    assert store.payments[event.payment_id].status is PaymentStatus.PENDING
    assert webhooks.attempts == []


async def test_concurrent_duplicate_charges_once() -> None:
    store, event, clock = await prepare()
    gateway = FakeGateway([ChargeOutcome.SUCCEEDED])

    class RacingGateway:
        """Пока этот обработчик ждет шлюз, дубль сообщения успевает все закоммитить."""

        async def charge(self, payment: Payment) -> ChargeOutcome:
            outcome = await gateway.charge(payment)
            await make_use_case(store, gateway, FakeWebhookSender(), clock).execute(event)
            return outcome

    webhooks = FakeWebhookSender()
    result = await make_use_case(store, RacingGateway(), webhooks, clock).execute(event)

    assert len(gateway.calls) == 2
    assert len(gateway.effects) == 1
    assert not result.outcome_applied
    assert result.status is PaymentStatus.SUCCEEDED
    assert store.payments[event.payment_id].status is PaymentStatus.SUCCEEDED
    assert webhooks.attempts == []


async def test_commit_failure_after_charge_does_not_change_outcome() -> None:
    store, event, clock = await prepare()
    gateway = FakeGateway([ChargeOutcome.SUCCEEDED, ChargeOutcome.DECLINED])
    use_case = make_use_case(store, gateway, FakeWebhookSender(), clock)

    store.fail_commits = 1
    with pytest.raises(ConnectionError):
        await use_case.execute(event)
    assert store.payments[event.payment_id].status is PaymentStatus.PENDING

    result = await use_case.execute(event)

    # Повторный вызов шлюза вернул уже зафиксированный успех, а не новый бросок.
    assert len(gateway.calls) == 2
    assert gateway.effects == {event.payment_id: ChargeOutcome.SUCCEEDED}
    assert result.outcome_applied
    assert store.payments[event.payment_id].status is PaymentStatus.SUCCEEDED


async def test_unknown_payment_raises_not_found() -> None:
    store, _, clock = await prepare()
    use_case = make_use_case(store, FakeGateway(), FakeWebhookSender(), clock)

    with pytest.raises(PaymentNotFoundError):
        await use_case.execute(PaymentCreated(event_id=uuid.uuid4(), payment_id=uuid.uuid4()))
