from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

PAYMENTS_NEW_TOPIC = "payments.new"


@dataclass(frozen=True, slots=True)
class PaymentCreated:
    """Интеграционное событие: платеж принят и ждет обработки."""

    event_id: uuid.UUID
    payment_id: uuid.UUID

    def to_payload(self) -> dict[str, str]:
        return {"event_id": str(self.event_id), "payment_id": str(self.payment_id)}

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> PaymentCreated:
        return cls(
            event_id=uuid.UUID(str(payload["event_id"])),
            payment_id=uuid.UUID(str(payload["payment_id"])),
        )


@dataclass(frozen=True, slots=True)
class OutboxMessage:
    id: uuid.UUID
    topic: str
    payload: dict[str, Any]
    created_at: datetime

    @classmethod
    def for_payment_created(cls, event: PaymentCreated, now: datetime) -> OutboxMessage:
        # id сообщения = id события: relay ставит его в message_id,
        # и дубль публикации однозначно опознается на стороне consumer.
        return cls(
            id=event.event_id,
            topic=PAYMENTS_NEW_TOPIC,
            payload=event.to_payload(),
            created_at=now,
        )
