"""Порты прикладного слоя. Реализации живут в infrastructure."""

from __future__ import annotations

import uuid
from collections.abc import Callable
from datetime import datetime
from types import TracebackType
from typing import Protocol, Self

from payments.application.events import OutboxMessage
from payments.application.notifications import PaymentNotification
from payments.domain import ChargeOutcome, Payment


class PaymentRepository(Protocol):
    async def add(self, payment: Payment) -> None: ...

    async def get(self, payment_id: uuid.UUID) -> Payment | None: ...

    async def get_for_update(self, payment_id: uuid.UUID) -> Payment | None:
        """Читает платеж с блокировкой строки до конца транзакции."""
        ...

    async def get_by_idempotency_key(self, key: str) -> Payment | None: ...


class OutboxRepository(Protocol):
    async def add(self, message: OutboxMessage) -> None: ...


class UnitOfWork(Protocol):
    """Одна транзакция. Выход из контекста без commit() = rollback.

    commit() поднимает DuplicateIdempotencyKeyError при нарушении
    уникальности idempotency key.
    """

    # Read-only свойства, а не атрибуты: так реализация может объявить
    # конкретный тип репозитория и остаться совместимой с протоколом.
    @property
    def payments(self) -> PaymentRepository: ...

    @property
    def outbox(self) -> OutboxRepository: ...

    async def __aenter__(self) -> Self: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None: ...

    async def commit(self) -> None: ...


UnitOfWorkFactory = Callable[[], UnitOfWork]


class PaymentGateway(Protocol):
    async def charge(self, payment: Payment) -> ChargeOutcome:
        """Списание. Отказ банка - это ChargeOutcome.DECLINED, а не исключение.

        Контракт: вызов идемпотентен по payment.id. Повторный вызов для того же
        платежа (параллельный или после рестарта) не создает второго списания
        и возвращает ранее зафиксированный результат. Технический сбой, после
        которого результат неизвестен, - GatewayUnavailableError: повтор вызова
        безопасен и вернет фактический результат.
        """
        ...


class WebhookSender(Protocol):
    async def send(self, url: str, notification: PaymentNotification) -> None:
        """WebhookTemporaryError - повторить, WebhookRejectedError - не повторять."""
        ...


class Clock(Protocol):
    def now(self) -> datetime: ...
