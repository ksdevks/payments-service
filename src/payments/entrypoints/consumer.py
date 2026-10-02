"""Consumer payments.new.

Запуск: faststream run payments.entrypoints.consumer:build_app --factory
"""

import httpx
from faststream import FastStream

from payments.application.use_cases import ProcessPayment
from payments.config import Settings
from payments.infrastructure.clock import SystemClock
from payments.infrastructure.db import build_engine, build_session_factory, build_uow_factory
from payments.infrastructure.db.gateway_ledger import SqlAlchemyChargeLedger
from payments.infrastructure.gateway import EmulatedGateway
from payments.infrastructure.messaging.consumer import register_payment_subscriber
from payments.infrastructure.messaging.handler import PaymentCreatedHandler, RetryPolicy
from payments.infrastructure.messaging.topology import Topology, build_broker, declare_topology
from payments.infrastructure.webhooks import DestinationGuard, HttpWebhookSender
from payments.observability import configure_logging


def build_app(settings: Settings | None = None) -> FastStream:
    settings = settings or Settings()  # type: ignore[call-arg]
    configure_logging(settings.log_level)

    policy = RetryPolicy.exponential(settings.max_delivery_attempts, settings.retry_base_delay_ms)
    topology = Topology.build(policy.delays_ms, delivery_limit=settings.max_redeliveries)
    broker = build_broker(settings.rabbitmq_url, max_consumers=settings.consumer_prefetch)

    engine = build_engine(
        settings.database_url,
        pool_size=settings.db_pool_size,
        command_timeout=settings.db_command_timeout,
    )
    session_factory = build_session_factory(engine)
    http = httpx.AsyncClient(timeout=settings.webhook_timeout, follow_redirects=False)
    process = ProcessPayment(
        uow_factory=build_uow_factory(session_factory),
        gateway=EmulatedGateway(
            SqlAlchemyChargeLedger(session_factory),
            min_delay=settings.gateway_min_delay,
            max_delay=settings.gateway_max_delay,
            success_rate=settings.gateway_success_rate,
        ),
        webhooks=HttpWebhookSender(
            http,
            deadline=settings.webhook_timeout,
            guard=DestinationGuard(settings.webhook_allowlist),
        ),
        clock=SystemClock(),
    )
    register_payment_subscriber(
        broker,
        topology,
        PaymentCreatedHandler(process, policy),
        publish_timeout=settings.publish_timeout,
    )

    app = FastStream(broker)

    @app.on_startup
    async def setup_topology() -> None:
        # Объявляем всю топологию до старта подписчика: retry- и DLQ-очереди
        # должны существовать к моменту первой ошибки.
        await broker.connect()
        await declare_topology(broker, topology)

    @app.after_shutdown
    async def release_resources() -> None:
        await http.aclose()
        await engine.dispose()

    return app
