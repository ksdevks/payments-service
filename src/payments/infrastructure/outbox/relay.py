"""Outbox relay: переносит события из таблицы outbox в брокер.

Схема захвата: строка арендуется (next_attempt_at сдвигается на lease)
в короткой транзакции, публикация идет уже вне транзакции. Упавший relay
не держит блокировки: по истечении аренды строку заберет другой экземпляр.

Каждое сообщение отмечается опубликованным сразу после подтверждения брокера,
а не в конце пачки. Каждая операция шага ограничена общим дедлайном
(asyncio.timeout): публикация - publish_timeout, операция с хранилищем
целиком, включая получение соединения и коммит, - store_timeout. Поэтому
бюджет шага - настоящая верхняя граница, а не сумма таймаутов отдельных
запросов. Перед каждым шагом relay проверяет, что до конца аренды остается
не меньше бюджета шага; если нет, остаток пачки возвращается в очередь.
Перенос неудачной публикации и возврат защищены счетчиком attempts: он
увеличивается при каждом захвате и служит fencing-токеном, поэтому relay с
истекшей арендой не изменит строку, захваченную заново.
Гарантия - at-least-once, дубли гасит идемпотентный consumer.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
import uuid
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ClaimedMessage:
    id: uuid.UUID
    topic: str
    payload: dict[str, Any]
    attempts: int


class OutboxStore(Protocol):
    async def claim_batch(self, limit: int) -> Sequence[ClaimedMessage]: ...

    async def mark_published(self, ids: Sequence[uuid.UUID]) -> None: ...

    async def mark_failed(self, message: ClaimedMessage, error: str, retry_in: float) -> None:
        """Переносит попытку, только если строка не была захвачена заново."""
        ...

    async def release(self, messages: Sequence[ClaimedMessage]) -> None:
        """Досрочно возвращает незатронутые строки, если они не захвачены заново."""
        ...


class MessagePublisher(Protocol):
    async def publish(self, message: ClaimedMessage) -> None:
        """Возвращается только после подтверждения брокером (publisher confirm)."""
        ...


@dataclass(frozen=True, slots=True)
class RelaySettings:
    batch_size: int = 20
    poll_interval: float = 0.5
    retry_base: float = 1.0
    retry_max: float = 60.0
    lease_seconds: float = 60.0
    publish_timeout: float = 2.0
    store_timeout: float = 5.0

    def __post_init__(self) -> None:
        if self.lease_seconds < 2 * self.step_budget:
            raise ValueError("lease must cover at least two steps")

    @property
    def step_budget(self) -> float:
        """Худшее время шага: публикация и одна операция с хранилищем."""
        return self.publish_timeout + self.store_timeout

    def backoff(self, attempts: int) -> float:
        # Степень ограничивается до вычисления: 2.0 ** 1024 уже OverflowError.
        exponent = min(max(attempts - 1, 0), 62)
        return min(self.retry_base * 2.0**exponent, self.retry_max)


class OutboxRelay:
    def __init__(
        self,
        store: OutboxStore,
        publisher: MessagePublisher,
        settings: RelaySettings,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._store = store
        self._publisher = publisher
        self._settings = settings
        self._clock = clock

    async def run_once(self) -> int:
        """Обрабатывает одну пачку. Возвращает число захваченных сообщений."""
        # Отсчет до захвата: аренда в БД начинается не позже этого момента.
        started = self._clock()
        claimed = await self._bounded(
            self._store.claim_batch(self._settings.batch_size), self._settings.store_timeout
        )
        last_safe_start = started + self._settings.lease_seconds - self._settings.step_budget
        for index, message in enumerate(claimed):
            if self._clock() >= last_safe_start:
                rest = claimed[index:]
                logger.warning("lease is running out, releasing %s outbox messages", len(rest))
                await self._bounded(self._store.release(rest), self._settings.store_timeout)
                break
            try:
                await self._bounded(
                    self._publisher.publish(message), self._settings.publish_timeout
                )
            except Exception as exc:
                retry_in = self._settings.backoff(message.attempts)
                logger.warning(
                    "outbox message %s publish failed (attempt %s), retry in %.1fs: %r",
                    message.id,
                    message.attempts,
                    retry_in,
                    exc,
                )
                await self._bounded(
                    self._store.mark_failed(message, repr(exc)[:2000], retry_in),
                    self._settings.store_timeout,
                )
            else:
                await self._bounded(
                    self._store.mark_published([message.id]), self._settings.store_timeout
                )
        return len(claimed)

    @staticmethod
    async def _bounded[T](operation: Awaitable[T], limit: float) -> T:
        async with asyncio.timeout(limit):
            return await operation

    async def run(self, stop: asyncio.Event) -> None:
        logger.info("outbox relay started")
        while not stop.is_set():
            try:
                claimed = await self.run_once()
            except Exception:
                logger.exception("outbox relay iteration failed")
                claimed = 0
            # Полная пачка - вероятно, есть еще: забираем сразу, без паузы.
            if claimed < self._settings.batch_size:
                await _sleep_unless_stopped(stop, self._settings.poll_interval)
        logger.info("outbox relay stopped")


async def _sleep_unless_stopped(stop: asyncio.Event, delay: float) -> None:
    """Пауза между итерациями, прерываемая сигналом остановки."""
    with contextlib.suppress(TimeoutError):
        async with asyncio.timeout(delay):
            await stop.wait()
