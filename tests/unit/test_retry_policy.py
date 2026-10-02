import uuid

import pytest

from payments.application.errors import (
    GatewayUnavailableError,
    PaymentNotFoundError,
    WebhookRejectedError,
    WebhookTemporaryError,
)
from payments.application.events import PaymentCreated
from payments.domain import InvalidPaymentStateError
from payments.infrastructure.messaging.handler import (
    Ack,
    DeadLetter,
    PaymentCreatedHandler,
    Retry,
    RetryPolicy,
    parse_attempt,
)

POLICY = RetryPolicy.exponential(max_attempts=3, base_delay_ms=1000)
EVENT = PaymentCreated(event_id=uuid.uuid4(), payment_id=uuid.uuid4())


def test_exponential_delays() -> None:
    assert POLICY.delays_ms == (1000, 2000)
    assert RetryPolicy.exponential(5, 100).delays_ms == (100, 200, 400, 800)
    assert RetryPolicy.exponential(1, 100).delays_ms == ()


def test_policy_rejects_inconsistent_config() -> None:
    with pytest.raises(ValueError):
        RetryPolicy(max_attempts=3, delays_ms=(1000,))
    with pytest.raises(ValueError):
        RetryPolicy(max_attempts=0, delays_ms=())


def test_transient_errors_retry_until_exhausted() -> None:
    error = WebhookTemporaryError("timeout")
    assert POLICY.decide(error, 1) == Retry(next_attempt=2, delay_ms=1000)
    assert POLICY.decide(error, 2) == Retry(next_attempt=3, delay_ms=2000)
    final = POLICY.decide(error, 3)
    assert isinstance(final, DeadLetter)
    assert final.attempt == 3
    assert final.error_type == "WebhookTemporaryError"
    assert "attempts exhausted" in final.reason


def test_unknown_errors_are_treated_as_transient() -> None:
    # Падение БД или сети без классификации - временное, пока не доказано обратное.
    assert isinstance(POLICY.decide(ConnectionError("db down"), 1), Retry)


def test_permanent_errors_dead_letter_immediately() -> None:
    for error in (
        WebhookRejectedError("404"),
        PaymentNotFoundError("missing"),
        InvalidPaymentStateError("broken"),
    ):
        assert isinstance(POLICY.decide(error, 1), DeadLetter)


def test_parse_attempt() -> None:
    assert parse_attempt(None) == 1
    assert parse_attempt({}) == 1
    assert parse_attempt({"x-attempt": 2}) == 2
    assert parse_attempt({"x-attempt": b"3"}) == 3
    assert parse_attempt({"x-attempt": "garbage"}) == 1
    assert parse_attempt({"x-attempt": 0}) == 1


def test_redeliveries_count_as_attempts() -> None:
    # Возврат в очередь (nack, падение consumer'а) - израсходованная попытка.
    assert parse_attempt({"x-delivery-count": 1}) == 2
    assert parse_attempt({"x-attempt": 2, "x-delivery-count": 1}) == 3
    assert parse_attempt({"x-delivery-count": "garbage"}) == 1


class Processor:
    def __init__(self, error: Exception | None = None, *, completed: bool = False) -> None:
        self.error = error
        self.completed = completed
        self.calls = 0

    async def execute(self, event: PaymentCreated) -> None:
        self.calls += 1
        if self.error:
            raise self.error

    async def is_completed(self, event: PaymentCreated) -> bool:
        return self.completed


async def test_handler_acks_success() -> None:
    processor = Processor()
    assert await PaymentCreatedHandler(processor, POLICY).handle(EVENT, 1) == Ack()
    assert processor.calls == 1


async def test_unfinished_redelivery_beyond_budget_is_dead_lettered() -> None:
    processor = Processor(completed=False)
    decision = await PaymentCreatedHandler(processor, POLICY).handle(EVENT, 4)
    assert isinstance(decision, DeadLetter)
    assert decision.error_type == "AttemptsExhausted"
    assert processor.calls == 0


async def test_completed_redelivery_beyond_budget_is_acknowledged() -> None:
    # Последняя попытка прошла успешно, но ACK потерялся.
    processor = Processor(completed=True)
    assert await PaymentCreatedHandler(processor, POLICY).handle(EVENT, 4) == Ack()
    assert processor.calls == 0


async def test_handler_translates_failures_into_decisions() -> None:
    retrying = PaymentCreatedHandler(Processor(GatewayUnavailableError("down")), POLICY)
    assert await retrying.handle(EVENT, 1) == Retry(next_attempt=2, delay_ms=1000)
    assert isinstance(await retrying.handle(EVENT, 3), DeadLetter)
