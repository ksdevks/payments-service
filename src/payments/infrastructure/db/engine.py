from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from payments.application.ports import UnitOfWorkFactory
from payments.infrastructure.db.uow import SqlAlchemyUnitOfWork


def build_engine(
    url: str, *, pool_size: int = 10, command_timeout: float = 5.0, echo: bool = False
) -> AsyncEngine:
    # command_timeout asyncpg ограничивает каждый запрос: зависшая БД не держит
    # обработчик бесконечно, а relay может рассчитывать бюджет шага.
    return create_async_engine(
        url,
        pool_pre_ping=True,
        pool_size=pool_size,
        pool_timeout=command_timeout,
        # timeout - установление соединения, command_timeout - каждый запрос.
        connect_args={"timeout": command_timeout, "command_timeout": command_timeout},
        echo=echo,
    )


def build_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    # expire_on_commit=False: записи не перечитываются после commit, домен уже в памяти.
    return async_sessionmaker(engine, expire_on_commit=False, autoflush=False)


def build_uow_factory(session_factory: async_sessionmaker[AsyncSession]) -> UnitOfWorkFactory:
    return lambda: SqlAlchemyUnitOfWork(session_factory)
