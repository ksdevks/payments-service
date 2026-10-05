import hashlib
import json
from decimal import Decimal
from typing import Any

from payments.application.commands import CreatePaymentCommand
from payments.domain.money import MINOR_UNIT


def request_fingerprint(command: CreatePaymentCommand) -> str:
    """Стабильный отпечаток тела запроса для проверки повторов по idempotency key.

    Сумма нормализуется (10 == 10.00), ключи metadata сортируются, а
    целые числа в metadata приводятся к одному виду (1 == 1.0), поэтому
    семантически одинаковые запросы дают одинаковый отпечаток.
    """
    canonical = {
        "amount": str(Decimal(command.amount).quantize(MINOR_UNIT)),
        "currency": command.currency.value,
        "description": command.description,
        "metadata": _canonical(command.metadata),
        "webhook_url": command.webhook_url,
    }
    encoded = json.dumps(
        canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _canonical(value: Any) -> Any:
    """Приводит числа к единому виду: 1.0 и 1 в JSON - одно и то же значение."""
    if isinstance(value, bool):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, dict):
        return {key: _canonical(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_canonical(item) for item in value]
    return value
