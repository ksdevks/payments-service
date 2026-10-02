"""Проверка топологии на живом RabbitMQ: TTL-ретраи, DLX и публикация из outbox."""

import asyncio
import uuid
from typing import Any

import pytest
from aio_pika.abc import AbstractIncomingMessage, AbstractQueue
from aio_pika.exceptions import DeliveryError

from payments.application.errors import GatewayUnavailableError, WebhookRejectedError
from payments.application.events import PAYMENTS_NEW_TOPIC, PaymentCreated
from payments.infrastructure.messaging.consumer import register_payment_subscriber
from payments.infrastructure.messaging.handler import PaymentCreatedHandler, RetryPolicy
from payments.infrastructure.messaging.publisher import RabbitOutboxPublisher
from payments.infrastructure.messaging.topology import Topology, build_broker, declare_topology
from payments.infrastructure.outbox import ClaimedMessage

pytestmark = pytest.mark.rabbitmq


class RecordingProcessor:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.seen: list[PaymentCreated] = []

    async def execute(self, event: PaymentCreated) -> None:
        self.seen.append(event)
        if self.error:
            raise self.error

    async def is_completed(self, event: PaymentCreated) -> bool:
        return False


async def wait_message(queue: AbstractQueue, within: float = 10.0) -> AbstractIncomingMessage:
    async with asyncio.timeout(within):
        while True:
            message = await queue.get(no_ack=True, fail=False)
            if message is not None:
                return message
            await asyncio.sleep(0.05)


def text(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


async def run_flow(
    rabbitmq_url: str, processor: RecordingProcessor, policy: RetryPolicy
) -> tuple[Topology, AbstractIncomingMessage]:
    topology = Topology.build(policy.delays_ms, prefix=f"test-{uuid.uuid4().hex[:8]}")
    broker = build_broker(rabbitmq_url)
    register_payment_subscriber(broker, topology, PaymentCreatedHandler(processor, policy))
    try:
        await broker.connect()
        await declare_topology(broker, topology)
        await broker.start()

        event = PaymentCreated(event_id=uuid.uuid4(), payment_id=uuid.uuid4())
        claimed = ClaimedMessage(
            id=event.event_id, topic=PAYMENTS_NEW_TOPIC, payload=event.to_payload(), attempts=1
        )
        await RabbitOutboxPublisher(broker, topology).publish(claimed)
        dlq = await broker.declare_queue(topology.dead_letter_queue)
        return topology, await wait_message(dlq)
    finally:
        await broker.stop()


async def test_transient_failure_is_retried_with_ttl_then_dead_lettered(
    rabbitmq_url: str,
) -> None:
    processor = RecordingProcessor(GatewayUnavailableError("down"))
    policy = RetryPolicy.exponential(max_attempts=3, base_delay_ms=100)

    topology, dead = await run_flow(rabbitmq_url, processor, policy)

    assert len(processor.seen) == 3
    assert len({e.event_id for e in processor.seen}) == 1
    headers = dead.headers or {}
    assert int(text(headers["x-attempt"])) == 3
    assert text(headers["x-error-type"]) == "GatewayUnavailableError"
    assert "attempts exhausted" in text(headers["x-dead-letter-reason"])
    assert text(headers["x-original-queue"]) == topology.main_queue.name


async def test_permanent_failure_skips_retries(rabbitmq_url: str) -> None:
    processor = RecordingProcessor(WebhookRejectedError("404"))
    policy = RetryPolicy.exponential(max_attempts=3, base_delay_ms=100)

    _, dead = await run_flow(rabbitmq_url, processor, policy)

    assert len(processor.seen) == 1
    headers = dead.headers or {}
    assert int(text(headers["x-attempt"])) == 1
    assert text(headers["x-error-type"]) == "WebhookRejectedError"


async def test_unroutable_publish_fails_instead_of_silent_loss(rabbitmq_url: str) -> None:
    topology = Topology.build((100,), prefix=f"test-{uuid.uuid4().hex[:8]}")
    broker = build_broker(rabbitmq_url)
    try:
        await broker.connect()
        await declare_topology(broker, topology)
        orphan = ClaimedMessage(
            id=uuid.uuid4(), topic="no.such.binding", payload={"k": "v"}, attempts=1
        )
        # Брокер подтверждает и возвращает сообщение: без on_return_raises
        # publish завершился бы успешно, а outbox пометил бы его опубликованным.
        with pytest.raises(DeliveryError):
            await RabbitOutboxPublisher(broker, topology).publish(orphan)
    finally:
        await broker.stop()


async def test_invalid_message_goes_to_dlq_natively(rabbitmq_url: str) -> None:
    """Сообщение, которое не дошло до политики (невалидное тело), не теряется."""
    processor = RecordingProcessor()
    policy = RetryPolicy.exponential(max_attempts=3, base_delay_ms=100)
    topology = Topology.build(policy.delays_ms, prefix=f"test-{uuid.uuid4().hex[:8]}")
    broker = build_broker(rabbitmq_url)
    register_payment_subscriber(broker, topology, PaymentCreatedHandler(processor, policy))
    try:
        await broker.connect()
        await declare_topology(broker, topology)
        await broker.start()
        await broker.publish(
            {"not": "a payment event"},
            exchange=topology.exchange,
            routing_key=PAYMENTS_NEW_TOPIC,
            persist=True,
            mandatory=True,
        )
        dlq = await broker.declare_queue(topology.dead_letter_queue)
        dead = await wait_message(dlq)
    finally:
        await broker.stop()

    assert processor.seen == []
    death = (dead.headers or {})["x-death"][0]  # type: ignore[index]
    assert text(death["reason"]) == "rejected"
