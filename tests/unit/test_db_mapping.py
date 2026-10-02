"""Маппинг и разбор IntegrityError без БД."""

import uuid
from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy.exc import IntegrityError

from payments.application.events import OutboxMessage, PaymentCreated
from payments.domain import ChargeOutcome, Currency, Money, Payment
from payments.infrastructure.db.mappers import (
    outbox_to_record,
    payment_to_domain,
    payment_to_record,
    sync_payment_record,
)
from payments.infrastructure.db.models import IDEMPOTENCY_KEY_CONSTRAINT
from payments.infrastructure.db.uow import constraint_name

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def make_payment() -> Payment:
    return Payment.create(
        money=Money(Decimal("42.50"), Currency.EUR),
        description="Order",
        metadata={"nested": {"a": [1, 2]}},
        idempotency_key="key",
        request_fingerprint="f" * 64,
        webhook_url="https://example.com/hook",
        now=NOW,
    )


def test_payment_round_trip() -> None:
    payment = make_payment()
    restored = payment_to_domain(payment_to_record(payment))
    assert vars(restored) == vars(payment)


def test_sync_copies_only_mutable_state() -> None:
    payment = make_payment()
    record = payment_to_record(payment)
    payment.apply_charge_outcome(ChargeOutcome.SUCCEEDED, NOW)
    payment.mark_webhook_delivered(NOW)

    sync_payment_record(payment, record)

    assert record.status == "succeeded"
    assert record.processed_at == NOW
    assert record.webhook_delivered_at == NOW


def test_outbox_record_is_due_immediately() -> None:
    event = PaymentCreated(event_id=uuid.uuid4(), payment_id=uuid.uuid4())
    record = outbox_to_record(OutboxMessage.for_payment_created(event, NOW))
    assert record.id == event.event_id
    assert record.next_attempt_at == NOW
    assert record.attempts == 0


class _DriverError(Exception):
    def __init__(self, name: str | None) -> None:
        super().__init__("driver error")
        self.constraint_name = name


def _integrity_error(cause: BaseException) -> IntegrityError:
    try:
        raise RuntimeError("adapter") from cause
    except RuntimeError as adapter:
        return IntegrityError("INSERT", {}, adapter)


def test_constraint_name_is_found_in_cause_chain() -> None:
    error = _integrity_error(_DriverError(IDEMPOTENCY_KEY_CONSTRAINT))
    assert constraint_name(error) == IDEMPOTENCY_KEY_CONSTRAINT


def test_constraint_name_absent() -> None:
    assert constraint_name(_integrity_error(ValueError("other"))) is None
