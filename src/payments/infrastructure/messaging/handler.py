"""Политика обработки сообщения payments.new без привязки к брокеру.

Обработчик возвращает решение (ack / retry / dead-letter), а адаптер
брокера его исполняет. Так политику ретраев можно проверить юнит-тестами.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from payments.application.errors import PaymentNotFoundError, PermanentError
from payments.application.events import PaymentCreated
from payments.domain import DomainError

logger = logging.getLogger(__name__)

ATTEMPT_HEADER = "x-attempt"
# Счетчик возвратов в очередь, который quorum-очередь RabbitMQ ведет сама:
# nack с requeue и обрыв соединения consumer'а до ack увеличивают его.
DELIVERY_COUNT_HEADER = "x-delivery-count"

# Повтор этих ошибок ничего не изменит: сообщение сразу уходит в DLQ.
NON_RETRYABLE: tuple[type[BaseException], ...] = (
    PermanentError,
    PaymentNotFoundError,
    DomainError,
)


class PaymentProcessor(Protocol):
    async def execute(self, event: PaymentCreated) -> Any: ...

    async def is_completed(self, event: PaymentCreated) -> bool:
        """Обработка уже завершена. Только чтение, без внешних вызовов."""
        ...


@dataclass(frozen=True, slots=True)
class Ack:
    pass


@dataclass(frozen=True, slots=True)
class Retry:
    next_attempt: int
    delay_ms: int


@dataclass(frozen=True, slots=True)
class DeadLetter:
    """Окончательный отказ с диагностикой, которая уйдет в заголовки DLQ."""

    attempt: int
    error_type: str
    reason: str


Decision = Ack | Retry | DeadLetter


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """max_attempts - общее число обработок, включая первую."""

    max_attempts: int
    delays_ms: tuple[int, ...]

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if len(self.delays_ms) != self.max_attempts - 1:
            raise ValueError("need exactly one delay per retry")

    @classmethod
    def exponential(cls, max_attempts: int, base_delay_ms: int) -> RetryPolicy:
        return cls(
            max_attempts=max_attempts,
            delays_ms=tuple(base_delay_ms * 2**i for i in range(max_attempts - 1)),
        )

    def decide(self, error: BaseException, attempt: int) -> Retry | DeadLetter:
        error_type = type(error).__name__
        if isinstance(error, NON_RETRYABLE):
            return DeadLetter(attempt, error_type, f"non-retryable: {error}")
        if attempt >= self.max_attempts:
            return DeadLetter(attempt, error_type, f"attempts exhausted: {error}")
        return Retry(next_attempt=attempt + 1, delay_ms=self.delays_ms[attempt - 1])


def _header_int(headers: Mapping[str, Any], name: str, default: int, minimum: int) -> int:
    raw = headers.get(name, default)
    if isinstance(raw, bytes):
        raw = raw.decode()
    try:
        return max(int(raw), minimum)
    except (TypeError, ValueError):
        logger.warning("malformed %s header %r, using %s", name, raw, default)
        return default


def parse_attempt(headers: Mapping[str, Any] | None) -> int:
    """Номер текущей обработки.

    x-attempt приходит в копии, запланированной через retry-очередь.
    x-delivery-count добавляет возвраты того же сообщения в очередь: если
    запланировать повтор не удалось и сообщение вернулось через nack, или
    consumer упал до ack, это тоже израсходованная попытка. Без этого
    возвраты обходили бы бюджет из max_attempts.
    """
    headers = headers or {}
    scheduled = _header_int(headers, ATTEMPT_HEADER, default=1, minimum=1)
    redeliveries = _header_int(headers, DELIVERY_COUNT_HEADER, default=0, minimum=0)
    return scheduled + redeliveries


class PaymentCreatedHandler:
    def __init__(self, processor: PaymentProcessor, policy: RetryPolicy) -> None:
        self._processor = processor
        self._policy = policy

    @property
    def policy(self) -> RetryPolicy:
        return self._policy

    async def handle(self, event: PaymentCreated, attempt: int) -> Decision:
        if attempt > self._policy.max_attempts:
            return await self._beyond_budget(event, attempt)
        try:
            await self._processor.execute(event)
        except Exception as exc:
            decision = self._policy.decide(exc, attempt)
            log = logger.error if isinstance(decision, DeadLetter) else logger.warning
            log(
                "payment %s event %s attempt %s failed -> %s",
                event.payment_id,
                event.event_id,
                attempt,
                decision,
                exc_info=isinstance(decision, DeadLetter),
            )
            return decision
        return Ack()

    async def _beyond_budget(self, event: PaymentCreated, attempt: int) -> Ack | DeadLetter:
        """Повторная доставка сверх бюджета: бизнес-шаг больше не выполняем.

        Но сначала проверяем, не завершена ли обработка: так бывает, когда
        последняя попытка прошла успешно, а ACK до брокера не дошел. Такое
        сообщение подтверждается, а не уходит в DLQ как упавшее.
        """
        try:
            completed = await self._processor.is_completed(event)
        except Exception as exc:
            logger.exception("cannot verify completion of payment %s", event.payment_id)
            return DeadLetter(attempt, type(exc).__name__, f"cannot verify completion: {exc}")
        if completed:
            logger.info(
                "payment %s already completed, redelivery %s acknowledged",
                event.payment_id,
                attempt,
            )
            return Ack()
        return DeadLetter(attempt, "AttemptsExhausted", "redelivered beyond max attempts")
