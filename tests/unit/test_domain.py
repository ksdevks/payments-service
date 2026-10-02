import uuid
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from payments.domain import (
    ChargeOutcome,
    Currency,
    InvalidMoneyError,
    InvalidPaymentError,
    InvalidPaymentStateError,
    Money,
    Payment,
    PaymentStatus,
)

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def make_payment(**overrides) -> Payment:
    data = {
        "money": Money(Decimal("100.00"), Currency.RUB),
        "description": "Order 1",
        "metadata": {"order_id": 1},
        "idempotency_key": "key-1",
        "request_fingerprint": "fp",
        "webhook_url": "https://example.com/hook",
        "now": NOW,
    }
    data.update(overrides)
    return Payment.create(**data)


def test_money_normalizes_scale() -> None:
    assert Money(Decimal("10"), Currency.USD).amount == Decimal("10.00")
    assert str(Money(Decimal("10.5"), Currency.USD).amount) == "10.50"


def test_money_rejects_invalid_amounts() -> None:
    for raw in ("0", "-1", "1.001", "NaN", "Infinity", "10000000000000000.00"):
        with pytest.raises(InvalidMoneyError):
            Money(Decimal(raw), Currency.EUR)


def test_new_payment_is_pending() -> None:
    payment = make_payment()
    assert payment.status is PaymentStatus.PENDING
    assert payment.processed_at is None
    assert not payment.needs_webhook
    assert isinstance(payment.id, uuid.UUID)


def test_payment_rejects_naive_timestamp_and_empty_description() -> None:
    with pytest.raises(InvalidPaymentError):
        make_payment(now=datetime(2026, 1, 1))
    with pytest.raises(InvalidPaymentError):
        make_payment(description="")


def test_charge_outcome_maps_to_status() -> None:
    succeeded = make_payment()
    succeeded.apply_charge_outcome(ChargeOutcome.SUCCEEDED, NOW)
    assert succeeded.status is PaymentStatus.SUCCEEDED
    assert succeeded.processed_at == NOW
    assert succeeded.needs_webhook

    declined = make_payment()
    declined.apply_charge_outcome(ChargeOutcome.DECLINED, NOW)
    assert declined.status is PaymentStatus.FAILED


def test_final_status_cannot_change() -> None:
    payment = make_payment()
    payment.apply_charge_outcome(ChargeOutcome.SUCCEEDED, NOW)
    with pytest.raises(InvalidPaymentStateError):
        payment.apply_charge_outcome(ChargeOutcome.DECLINED, NOW)


def test_webhook_requires_processed_payment_and_is_idempotent() -> None:
    payment = make_payment()
    with pytest.raises(InvalidPaymentStateError):
        payment.mark_webhook_delivered(NOW)

    payment.apply_charge_outcome(ChargeOutcome.SUCCEEDED, NOW)
    payment.mark_webhook_delivered(NOW)
    later = datetime(2026, 1, 2, tzinfo=UTC)
    payment.mark_webhook_delivered(later)
    assert payment.webhook_delivered_at == NOW
    assert not payment.needs_webhook
