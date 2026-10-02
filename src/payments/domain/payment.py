from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any

from payments.domain.errors import InvalidPaymentError, InvalidPaymentStateError
from payments.domain.money import Money

MAX_DESCRIPTION_LENGTH = 500
MAX_IDEMPOTENCY_KEY_LENGTH = 255
MAX_WEBHOOK_URL_LENGTH = 2048


class PaymentStatus(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class ChargeOutcome(StrEnum):
    """Бизнес-результат списания у шлюза. Технические сбои сюда не входят."""

    SUCCEEDED = "succeeded"
    DECLINED = "declined"


@dataclass(eq=False, kw_only=True)
class Payment:
    id: uuid.UUID
    money: Money
    description: str
    metadata: dict[str, Any]
    idempotency_key: str
    request_fingerprint: str
    webhook_url: str
    created_at: datetime
    status: PaymentStatus = PaymentStatus.PENDING
    processed_at: datetime | None = None
    webhook_delivered_at: datetime | None = None

    @classmethod
    def create(
        cls,
        *,
        money: Money,
        description: str,
        metadata: dict[str, Any],
        idempotency_key: str,
        request_fingerprint: str,
        webhook_url: str,
        now: datetime,
        payment_id: uuid.UUID | None = None,
    ) -> Payment:
        if not description or len(description) > MAX_DESCRIPTION_LENGTH:
            raise InvalidPaymentError("description must be 1..500 characters")
        if not idempotency_key or len(idempotency_key) > MAX_IDEMPOTENCY_KEY_LENGTH:
            raise InvalidPaymentError("idempotency key must be 1..255 characters")
        if not webhook_url or len(webhook_url) > MAX_WEBHOOK_URL_LENGTH:
            raise InvalidPaymentError("webhook url must be 1..2048 characters")
        if now.tzinfo is None:
            raise InvalidPaymentError("timestamps must be timezone-aware")
        return cls(
            id=payment_id or uuid.uuid4(),
            money=money,
            description=description,
            metadata=dict(metadata),
            idempotency_key=idempotency_key,
            request_fingerprint=request_fingerprint,
            webhook_url=webhook_url,
            created_at=now,
        )

    @property
    def is_pending(self) -> bool:
        return self.status is PaymentStatus.PENDING

    @property
    def needs_webhook(self) -> bool:
        return not self.is_pending and self.webhook_delivered_at is None

    def apply_charge_outcome(self, outcome: ChargeOutcome, at: datetime) -> None:
        if not self.is_pending:
            raise InvalidPaymentStateError(
                f"payment {self.id} is already {self.status}, cannot apply {outcome}"
            )
        self.status = (
            PaymentStatus.SUCCEEDED
            if outcome is ChargeOutcome.SUCCEEDED
            else PaymentStatus.FAILED
        )
        self.processed_at = at

    def mark_webhook_delivered(self, at: datetime) -> None:
        if self.is_pending:
            raise InvalidPaymentStateError(
                f"payment {self.id} is not processed, webhook cannot be delivered"
            )
        # Повторная отметка безопасна: фиксируем время первой успешной доставки.
        if self.webhook_delivered_at is None:
            self.webhook_delivered_at = at
