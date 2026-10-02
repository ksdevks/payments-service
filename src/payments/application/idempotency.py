import hashlib
import json
from decimal import Decimal

from payments.application.commands import CreatePaymentCommand
from payments.domain.money import MINOR_UNIT


def request_fingerprint(command: CreatePaymentCommand) -> str:
    """Стабильный отпечаток тела запроса для проверки повторов по idempotency key.

    Сумма нормализуется (10 == 10.00), ключи metadata сортируются,
    поэтому семантически одинаковые запросы дают одинаковый отпечаток.
    """
    canonical = {
        "amount": str(Decimal(command.amount).quantize(MINOR_UNIT)),
        "currency": command.currency.value,
        "description": command.description,
        "metadata": command.metadata,
        "webhook_url": command.webhook_url,
    }
    encoded = json.dumps(
        canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str
    ).encode()
    return hashlib.sha256(encoded).hexdigest()
