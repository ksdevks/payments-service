from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from payments.domain import Currency


@dataclass(frozen=True, slots=True)
class CreatePaymentCommand:
    idempotency_key: str
    amount: Decimal
    currency: Currency
    description: str
    webhook_url: str
    metadata: dict[str, Any] = field(default_factory=dict)
