"""Топология RabbitMQ. Единственный источник правды по exchanges и очередям.

    payments -> payments.new -(reject)-> payments.dlx -> payments.new.dlq
    payments.new -(временная ошибка)-> payments.retry -> payments.new.retry.<delay>ms
    payments.new.retry.<delay>ms -(TTL истек)-> payments -> payments.new

Все очереди quorum. Dead-lettering у них включен в режиме at-least-once
(x-dead-letter-strategy + x-overflow=reject-publish): сообщение покидает
исходную очередь только после подтверждения целевой. У classic-очередей
перемещение через DLX выполняется без подтверждений и может потерять
сообщение. x-delivery-limit ограничивает число возвратов в очередь
(nack с requeue), после чего сообщение уходит в DLQ.

Отдельная очередь на каждую задержку, а не одна очередь с per-message TTL:
RabbitMQ истекает сообщения только с головы очереди, и короткий ретрай
застрял бы за длинным (head-of-line blocking). Задержка входит в имя очереди,
поэтому смена настроек создает новые очереди, а не PRECONDITION_FAILED.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from faststream.rabbit import (
    ExchangeType,
    QueueType,
    RabbitBroker,
    RabbitExchange,
    RabbitQueue,
)

from payments.application.events import PAYMENTS_NEW_TOPIC


def build_broker(url: str, **options: Any) -> RabbitBroker:
    """Брокер с publisher confirms и исключением на возврат mandatory-сообщения.

    on_return_raises=True обязателен: без него сообщение, которое брокер
    вернул из-за отсутствия привязки, все равно получает confirm, и publish
    завершается успешно. Outbox пометил бы такое событие опубликованным.
    """
    return RabbitBroker(url, publisher_confirms=True, on_return_raises=True, **options)


@dataclass(frozen=True)
class Topology:
    exchange: RabbitExchange
    retry_exchange: RabbitExchange
    dead_letter_exchange: RabbitExchange
    main_queue: RabbitQueue
    retry_queues: dict[int, RabbitQueue]
    dead_letter_queue: RabbitQueue

    @classmethod
    def build(
        cls,
        retry_delays_ms: tuple[int, ...],
        *,
        delivery_limit: int = 10,
        prefix: str = "payments",
    ) -> Topology:
        exchange = RabbitExchange(prefix, type=ExchangeType.DIRECT, durable=True)
        retry_exchange = RabbitExchange(
            f"{prefix}.retry", type=ExchangeType.DIRECT, durable=True
        )
        dlx = RabbitExchange(f"{prefix}.dlx", type=ExchangeType.DIRECT, durable=True)
        main_name = f"{prefix}.new"
        dlq_name = f"{main_name}.dlq"

        main_queue = RabbitQueue(
            main_name,
            durable=True,
            queue_type=QueueType.QUORUM,
            routing_key=PAYMENTS_NEW_TOPIC,
            arguments={
                "x-dead-letter-exchange": dlx.name,
                "x-dead-letter-routing-key": dlq_name,
                "x-delivery-limit": delivery_limit,
                # At-least-once dead-lettering требует reject-publish.
                "x-dead-letter-strategy": "at-least-once",
                "x-overflow": "reject-publish",
            },
        )
        retry_queues = {
            delay: RabbitQueue(
                f"{main_name}.retry.{delay}ms",
                durable=True,
                queue_type=QueueType.QUORUM,
                routing_key=f"{main_name}.retry.{delay}ms",
                arguments={
                    "x-message-ttl": delay,
                    "x-dead-letter-exchange": exchange.name,
                    "x-dead-letter-routing-key": PAYMENTS_NEW_TOPIC,
                    "x-dead-letter-strategy": "at-least-once",
                    "x-overflow": "reject-publish",
                },
            )
            for delay in sorted(set(retry_delays_ms))
        }
        dead_letter_queue = RabbitQueue(
            dlq_name, durable=True, queue_type=QueueType.QUORUM, routing_key=dlq_name
        )
        return cls(exchange, retry_exchange, dlx, main_queue, retry_queues, dead_letter_queue)


async def declare_topology(broker: RabbitBroker, topology: Topology) -> None:
    """Идемпотентно объявляет exchanges, очереди и привязки. Брокер должен быть подключен."""
    exchange = await broker.declare_exchange(topology.exchange)
    retry_exchange = await broker.declare_exchange(topology.retry_exchange)
    dlx = await broker.declare_exchange(topology.dead_letter_exchange)

    bindings = [
        (topology.main_queue, exchange),
        (topology.dead_letter_queue, dlx),
        *((queue, retry_exchange) for queue in topology.retry_queues.values()),
    ]
    for queue, target in bindings:
        declared = await broker.declare_queue(queue)
        await declared.bind(target, routing_key=queue.routing)
