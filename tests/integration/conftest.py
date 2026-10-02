"""Интеграционные тесты против реальных PostgreSQL и RabbitMQ.

Тесты с маркером postgres идут, только если задан TEST_DATABASE_URL,
с маркером rabbitmq - если задан TEST_RABBITMQ_URL. Схема БД создается
alembic-миграциями, поэтому тесты проверяют и миграцию тоже.
"""

import os
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from payments.application.ports import UnitOfWorkFactory
from payments.infrastructure.db import build_engine, build_session_factory, build_uow_factory

DATABASE_URL = os.environ.get("TEST_DATABASE_URL")
RABBITMQ_URL = os.environ.get("TEST_RABBITMQ_URL")
ROOT = Path(__file__).resolve().parents[2]


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    requirements = {"postgres": DATABASE_URL, "rabbitmq": RABBITMQ_URL}
    for item in items:
        for marker, url in requirements.items():
            if item.get_closest_marker(marker) and not url:
                item.add_marker(pytest.mark.skip(reason=f"{marker} is not configured"))


@pytest.fixture(scope="session")
def migrated_database() -> Iterator[str]:
    assert DATABASE_URL
    os.environ["DATABASE_URL"] = DATABASE_URL
    config = Config(str(ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(ROOT / "migrations"))
    command.downgrade(config, "base")
    command.upgrade(config, "head")
    yield DATABASE_URL


@pytest.fixture
async def engine(migrated_database: str) -> AsyncIterator[AsyncEngine]:
    engine = build_engine(migrated_database, pool_size=20)
    async with engine.begin() as connection:
        await connection.execute(text("TRUNCATE payments, outbox, emulated_gateway_charges"))
    yield engine
    await engine.dispose()


@pytest.fixture
def session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return build_session_factory(engine)


@pytest.fixture
def uow_factory(session_factory: async_sessionmaker[AsyncSession]) -> UnitOfWorkFactory:
    return build_uow_factory(session_factory)


@pytest.fixture
def rabbitmq_url() -> str:
    assert RABBITMQ_URL
    return RABBITMQ_URL
