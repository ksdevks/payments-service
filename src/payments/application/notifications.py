from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from payments.domain import Currency, Payment, PaymentStatus

# Детерминированный id уведомления: одинаковый для всех повторов доставки,
# получатель может по нему дедуплицировать.
_NOTIFICATION_NAMESPACE = uuid.UUID("6f1c2a52-6c0e-4c7e-9d0b-6f7f5f3e9a10")
EVENT_TYPE = "payment.processed"


@dataclass(frozen=True, slots=True)
class PaymentNotification:
    notification_id: uuid.UUID
    payment_id: uuid.UUID
    status: PaymentStatus
    amount: Decimal
    currency: Currency
    processed_at: datetime

    @classmethod
    def from_payment(cls, payment: Payment) -> PaymentNotification:
        if payment.is_pending or payment.processed_at is None:
            raise ValueError(f"payment {payment.id} is not processed yet")
        return cls(
            notification_id=uuid.uuid5(
                _NOTIFICATION_NAMESPACE, f"{payment.id}:{payment.status}"
            ),
            payment_id=payment.id,
            status=payment.status,
            amount=payment.money.amount,
            currency=payment.money.currency,
            processed_at=payment.processed_at,
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "event": EVENT_TYPE,
            "notification_id": str(self.notification_id),
            "payment_id": str(self.payment_id),
            "status": self.status.value,
            "amount": str(self.amount),
            "currency": self.currency.value,
            "processed_at": self.processed_at.isoformat(),
        }
