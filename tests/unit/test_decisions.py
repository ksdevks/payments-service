"""Исполнение решений политики адаптером RabbitMQ, без брокера."""

import uuid
from decimal import Decimal

import pytest
from faststream.exceptions import NackMessage, RejectMessage

from payments.application.commands import CreatePaymentCommand
from payments.application.errors import GatewayUnavailableError, WebhookTemporaryError
from payments.application.events import PaymentCreated
from payments.application.use_cases import CreatePayment, ProcessPayment
from payments.domain import ChargeOutcome, Currency, PaymentStatus
from payments.infrastructure.messaging.consumer import resolve_decision
from payments.infrastructure.messaging.handler import (
    Ack,
    DeadLetter,
    PaymentCreatedHandler,
    Retry,
    RetryPolicy,
    parse_attempt,
)
from tests.fakes import (
    FakeGateway,
    FakeUnitOfWork,
    FakeWebhookSender,
    FixedClock,
    InMemoryStore,
)

EVENT = PaymentCreated(event_id=uuid.uuid4(), payment_id=uuid.uuid4())
POLICY = RetryPolicy.exponential(max_attempts=3, base_delay_ms=1000)


class Recorder:
    def __init__(self, *, fail_retry: bool = False, fail_dead_letter: bool = False) -> None:
        self.fail_retry = fail_retry
        self.fail_dead_letter = fail_dead_letter
        self.retries: list[Retry] = []
        self.dead: list[DeadLetter] = []
        self.sleeps: list[float] = []

    async def schedule_retry(self, retry: Retry) -> None:
        if self.fail_retry:
            raise ConnectionError("retry queue unavailable")
        self.retries.append(retry)

    async def dead_letter(self, failure: DeadLetter) -> None:
        if self.fail_dead_letter:
            raise ConnectionError("dlq unavailable")
        self.dead.append(failure)

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)

    async def resolve(self, decision: Ack | Retry | DeadLetter) -> None:
        await resolve_decision(
            decision,
            schedule_retry=self.schedule_retry,
            dead_letter=self.dead_letter,
            sleep=self.sleep,
        )


async def test_ack_and_scheduled_retry_complete_normally() -> None:
    recorder = Recorder()
    await recorder.resolve(Ack())
    await recorder.resolve(Retry(next_attempt=2, delay_ms=1000))
    assert recorder.retries == [Retry(next_attempt=2, delay_ms=1000)]
    assert recorder.sleeps == []


async def test_failed_retry_scheduling_waits_and_requeues() -> None:
    recorder = Recorder(fail_retry=True)
    with pytest.raises(NackMessage):
        await recorder.resolve(Retry(next_attempt=3, delay_ms=2000))
    assert recorder.sleeps == [2.0]


async def test_dead_letter_is_published_with_diagnostics() -> None:
    recorder = Recorder()
    failure = DeadLetter(attempt=3, error_type="GatewayUnavailableError", reason="down")
    await recorder.resolve(failure)
    assert recorder.dead == [failure]


async def test_failed_dead_letter_publish_falls_back_to_native_dlx() -> None:
    recorder = Recorder(fail_dead_letter=True)
    with pytest.raises(RejectMessage):
        await recorder.resolve(DeadLetter(attempt=1, error_type="X", reason="y"))


async def test_broken_retry_queue_keeps_budget_and_delays() -> None:
    """Сценарий из ревью: публикация retry падает на каждой доставке.

    Ожидаем ровно max_attempts бизнес-обработок, паузы по политике и DLQ,
    а не повтор обработки до брокерного лимита без задержек.
    """

    class AlwaysFails:
        calls = 0

        async def execute(self, event: PaymentCreated) -> None:
            AlwaysFails.calls += 1
            raise GatewayUnavailableError("down")

        async def is_completed(self, event: PaymentCreated) -> bool:
            return False

    handler = PaymentCreatedHandler(AlwaysFails(), POLICY)
    recorder = Recorder(fail_retry=True)
    delivery_count = 0

    for _ in range(10):
        headers = {"x-delivery-count": delivery_count} if delivery_count else {}
        decision = await handler.handle(EVENT, parse_attempt(headers))
        try:
            await recorder.resolve(decision)
        except NackMessage:
            delivery_count += 1  # брокер вернул сообщение и увеличил счетчик
            continue
        break

    assert AlwaysFails.calls == 3
    assert recorder.sleeps == [1.0, 2.0]
    assert len(recorder.dead) == 1
    assert recorder.dead[0].attempt == 3


async def test_lost_ack_after_successful_last_attempt_is_acknowledged() -> None:
    """Сценарий из ревью: третья попытка успешна, ACK потерян, сообщение вернулось."""
    store, clock = InMemoryStore(), FixedClock()
    await CreatePayment(lambda: FakeUnitOfWork(store), clock).execute(
        CreatePaymentCommand(
            idempotency_key="k",
            amount=Decimal("10.00"),
            currency=Currency.RUB,
            description="order",
            webhook_url="https://merchant.example/hook",
        )
    )
    event = PaymentCreated.from_payload(store.outbox[0].payload)
    webhooks = FakeWebhookSender([WebhookTemporaryError("503"), WebhookTemporaryError("503")])
    process = ProcessPayment(
        lambda: FakeUnitOfWork(store), FakeGateway([ChargeOutcome.SUCCEEDED]), webhooks, clock
    )
    handler = PaymentCreatedHandler(process, POLICY)
    recorder = Recorder()

    # Штатные попытки 1-3: две неудачи webhook и успех.
    for attempt in (1, 2, 3):
        await recorder.resolve(await handler.handle(event, parse_attempt({"x-attempt": attempt})))
    assert store.payments[event.payment_id].status is PaymentStatus.SUCCEEDED
    assert len(webhooks.attempts) == 3

    # ACK третьей попытки потерян: брокер вернул сообщение.
    redelivery = parse_attempt({"x-attempt": 3, "x-delivery-count": 1})
    decision = await handler.handle(event, redelivery)

    assert decision == Ack()
    assert recorder.dead == []
    assert len(webhooks.attempts) == 3
