"""Адаптер FastStream для payments.new: разбор сообщения и исполнение решений политики.

В DLQ сообщения попадают двумя путями. Отказ, обработанный политикой
(исчерпаны попытки, постоянная ошибка), публикуется в DLQ явно, с
заголовками диагностики: тип ошибки, причина, номер попытки. Все, что до
политики не дошло (невалидное тело, превышен x-delivery-limit, сбой самой
публикации в DLQ), уходит туда нативным dead-lettering с заголовком x-death.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

from faststream.exceptions import NackMessage, RejectMessage
from faststream.rabbit import RabbitBroker
from faststream.rabbit.annotations import RabbitMessage
from pydantic import BaseModel, ConfigDict

from payments.application.events import PaymentCreated
from payments.infrastructure.messaging.handler import (
    ATTEMPT_HEADER,
    Ack,
    DeadLetter,
    PaymentCreatedHandler,
    Retry,
    parse_attempt,
)
from payments.infrastructure.messaging.topology import Topology

logger = logging.getLogger(__name__)

# Диагностика в сообщениях, явно опубликованных в DLQ.
ERROR_TYPE_HEADER = "x-error-type"
REASON_HEADER = "x-dead-letter-reason"
ORIGINAL_QUEUE_HEADER = "x-original-queue"
DEAD_LETTERED_AT_HEADER = "x-dead-lettered-at"


class PaymentCreatedMessage(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)

    event_id: uuid.UUID
    payment_id: uuid.UUID

    def to_event(self) -> PaymentCreated:
        return PaymentCreated(event_id=self.event_id, payment_id=self.payment_id)


async def resolve_decision(
    decision: Ack | Retry | DeadLetter,
    *,
    schedule_retry: Callable[[Retry], Awaitable[None]],
    dead_letter: Callable[[DeadLetter], Awaitable[None]],
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> None:
    """Исполняет решение политики. Нормальный выход из функции означает ack.

    Если запланировать повтор не удалось, исходное сообщение нельзя ни
    подтвердить (повтор потеряется), ни отклонить (временная ошибка брокера
    отправила бы его в DLQ досрочно). Consumer сам выдерживает положенную
    задержку и возвращает сообщение через nack. Брокер увеличит
    x-delivery-count, и следующая обработка будет учтена как следующая
    попытка: бюджет и экспоненциальная задержка сохраняются. Ожидание
    в consumer'е - только аварийный режим, ограниченный наибольшей задержкой
    политики; штатный путь - TTL-очереди.
    """
    match decision:
        case Ack():
            return
        case Retry():
            try:
                await schedule_retry(decision)
            except Exception as exc:
                logger.warning(
                    "failed to schedule retry (attempt %s), waiting %s ms and requeueing: %r",
                    decision.next_attempt,
                    decision.delay_ms,
                    exc,
                )
                await sleep(decision.delay_ms / 1000)
                raise NackMessage(requeue=True) from exc
        case DeadLetter():
            try:
                await dead_letter(decision)
            except Exception as exc:
                # Диагностика теряется, само сообщение - нет: нативный DLX.
                logger.error("failed to publish to DLQ, rejecting to native DLX: %r", exc)
                raise RejectMessage() from exc


def register_payment_subscriber(
    broker: RabbitBroker,
    topology: Topology,
    handler: PaymentCreatedHandler,
    *,
    publish_timeout: float = 2.0,
) -> None:
    """Подписка на payments.new. retry=False: необработанное исключение -> reject -> DLQ."""

    @broker.subscriber(topology.main_queue, topology.exchange, retry=False)
    async def on_payment_created(body: PaymentCreatedMessage, message: RabbitMessage) -> None:
        attempt = parse_attempt(message.headers)
        decision = await handler.handle(body.to_event(), attempt)
        payload = body.model_dump(mode="json")

        async def schedule_retry(retry: Retry) -> None:
            # Сначала копия в retry-очередь, потом ack оригинала.
            # Падение между шагами даст дубль, а не потерю.
            retry_queue = topology.retry_queues[retry.delay_ms]
            await broker.publish(
                payload,
                exchange=topology.retry_exchange,
                routing_key=retry_queue.routing,
                message_id=message.message_id,
                headers={ATTEMPT_HEADER: retry.next_attempt},
                persist=True,
                mandatory=True,
                timeout=publish_timeout,
            )

        async def dead_letter(failure: DeadLetter) -> None:
            await broker.publish(
                payload,
                exchange=topology.dead_letter_exchange,
                routing_key=topology.dead_letter_queue.routing,
                message_id=message.message_id,
                headers={
                    ATTEMPT_HEADER: failure.attempt,
                    ERROR_TYPE_HEADER: failure.error_type,
                    REASON_HEADER: failure.reason[:1000],
                    ORIGINAL_QUEUE_HEADER: topology.main_queue.name,
                    DEAD_LETTERED_AT_HEADER: datetime.now(UTC).isoformat(),
                },
                persist=True,
                mandatory=True,
                timeout=publish_timeout,
            )

        await resolve_decision(decision, schedule_retry=schedule_retry, dead_letter=dead_letter)
