"""Outbox relay как отдельный процесс. Запуск: payments-relay"""

import asyncio
import logging
import signal

from payments.config import Settings
from payments.infrastructure.db import build_engine, build_session_factory
from payments.infrastructure.db.outbox_store import SqlAlchemyOutboxStore
from payments.infrastructure.messaging.handler import RetryPolicy
from payments.infrastructure.messaging.publisher import RabbitOutboxPublisher
from payments.infrastructure.messaging.topology import Topology, build_broker, declare_topology
from payments.infrastructure.outbox import OutboxRelay, RelaySettings
from payments.observability import configure_logging

logger = logging.getLogger(__name__)


async def run(settings: Settings) -> None:
    policy = RetryPolicy.exponential(settings.max_delivery_attempts, settings.retry_base_delay_ms)
    topology = Topology.build(policy.delays_ms, delivery_limit=settings.max_redeliveries)
    broker = build_broker(settings.rabbitmq_url)
    engine = build_engine(
        settings.database_url, pool_size=2, command_timeout=settings.db_command_timeout
    )

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    try:
        await broker.connect()
        await declare_topology(broker, topology)
        relay = OutboxRelay(
            SqlAlchemyOutboxStore(
                build_session_factory(engine), lease_seconds=settings.outbox_lease_seconds
            ),
            RabbitOutboxPublisher(broker, topology, timeout=settings.publish_timeout),
            RelaySettings(
                batch_size=settings.outbox_batch_size,
                poll_interval=settings.outbox_poll_interval,
                retry_base=settings.outbox_retry_base,
                retry_max=settings.outbox_retry_max,
                lease_seconds=settings.outbox_lease_seconds,
                publish_timeout=settings.publish_timeout,
                store_timeout=settings.db_command_timeout,
            ),
        )
        await relay.run(stop)
    finally:
        await broker.stop()
        await engine.dispose()


def main() -> None:
    settings = Settings()  # type: ignore[call-arg]
    configure_logging(settings.log_level)
    asyncio.run(run(settings))


if __name__ == "__main__":
    main()
