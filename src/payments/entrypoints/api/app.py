"""HTTP API. Запуск: uvicorn payments.entrypoints.api:create_app --factory

API пишет только в PostgreSQL (платеж + outbox в одной транзакции)
и не зависит от доступности RabbitMQ: публикацией занимается relay.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from importlib.metadata import version

from fastapi import FastAPI
from sqlalchemy import text
from starlette.types import Lifespan

from payments.application.use_cases import CreatePayment, GetPayment
from payments.config import Settings
from payments.entrypoints.api.dependencies import ApiServices
from payments.entrypoints.api.errors import register_error_handlers
from payments.entrypoints.api.routes import health_router, payments_router
from payments.infrastructure.clock import SystemClock
from payments.infrastructure.db import build_engine, build_session_factory, build_uow_factory
from payments.observability import configure_logging


def build_api(
    *,
    api_key: str,
    docs_enabled: bool = False,
    lifespan: Lifespan[FastAPI] | None = None,
) -> FastAPI:
    """Собирает приложение без инфраструктуры. Сервисы кладутся в app.state.services.

    Swagger, ReDoc и OpenAPI-схема включаются явно: страница Swagger не умеет
    передавать ключ при загрузке схемы, а ТЗ требует ключ на всех эндпоинтах.
    """
    app = FastAPI(
        title="Payment Service",
        version=version("payments"),
        description="Асинхронный процессинг платежей: outbox, RabbitMQ, webhooks.",
        lifespan=lifespan,
        docs_url="/docs" if docs_enabled else None,
        redoc_url="/redoc" if docs_enabled else None,
        openapi_url="/openapi.json" if docs_enabled else None,
    )
    app.state.api_key = api_key
    register_error_handlers(app)
    app.include_router(payments_router)
    app.include_router(health_router)
    return app


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()  # type: ignore[call-arg]
    configure_logging(settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = build_engine(
            settings.database_url,
            pool_size=settings.db_pool_size,
            command_timeout=settings.db_command_timeout,
        )
        uow_factory = build_uow_factory(build_session_factory(engine))

        async def check_ready() -> None:
            async with engine.connect() as connection:
                await connection.execute(text("SELECT 1"))

        app.state.services = ApiServices(
            create_payment=CreatePayment(uow_factory, SystemClock()),
            get_payment=GetPayment(uow_factory),
            check_ready=check_ready,
        )
        try:
            yield
        finally:
            await engine.dispose()

    return build_api(
        api_key=settings.api_key.get_secret_value(),
        docs_enabled=settings.api_docs_enabled,
        lifespan=lifespan,
    )
