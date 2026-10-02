"""Публикация событий outbox в RabbitMQ."""

from faststream.rabbit import RabbitBroker

from payments.infrastructure.messaging.topology import Topology
from payments.infrastructure.outbox import ClaimedMessage


class RabbitOutboxPublisher:
    def __init__(self, broker: RabbitBroker, topology: Topology, timeout: float = 2.0) -> None:
        self._broker = broker
        self._topology = topology
        self._timeout = timeout

    async def publish(self, message: ClaimedMessage) -> None:
        # Успех = confirm брокера. Возврат mandatory-сообщения (нет привязки)
        # поднимает исключение благодаря on_return_raises в build_broker,
        # и relay оставит событие неопубликованным.
        await self._broker.publish(
            message.payload,
            exchange=self._topology.exchange,
            routing_key=message.topic,
            message_id=str(message.id),
            persist=True,
            mandatory=True,
            timeout=self._timeout,
        )
