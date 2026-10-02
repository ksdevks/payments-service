import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, HttpUrl

from payments.domain import Currency, Payment, PaymentStatus


class CreatePaymentRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2, examples=["1250.50"])
    currency: Currency
    description: str = Field(min_length=1, max_length=500)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl


class PaymentAcceptedResponse(BaseModel):
    payment_id: uuid.UUID
    status: PaymentStatus
    created_at: datetime

    @classmethod
    def from_domain(cls, payment: Payment) -> Self:
        return cls(payment_id=payment.id, status=payment.status, created_at=payment.created_at)


class PaymentResponse(BaseModel):
    payment_id: uuid.UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None

    @classmethod
    def from_domain(cls, payment: Payment) -> Self:
        return cls(
            payment_id=payment.id,
            amount=payment.money.amount,
            currency=payment.money.currency,
            description=payment.description,
            metadata=payment.metadata,
            status=payment.status,
            idempotency_key=payment.idempotency_key,
            webhook_url=payment.webhook_url,
            created_at=payment.created_at,
            processed_at=payment.processed_at,
        )


class ErrorResponse(BaseModel):
    code: str
    detail: str
