from types import TracebackType
from typing import Self

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payments.application.errors import DuplicateIdempotencyKeyError
from payments.infrastructure.db.models import IDEMPOTENCY_KEY_CONSTRAINT
from payments.infrastructure.db.repositories import (
    SqlAlchemyOutboxRepository,
    SqlAlchemyPaymentRepository,
)


def constraint_name(exc: IntegrityError) -> str | None:
    """Имя нарушенного ограничения из цепочки исключений драйвера.

    asyncpg кладет его в UniqueViolationError.constraint_name, а адаптер
    SQLAlchemy оборачивает это исключение через raise ... from.
    """
    error: BaseException | None = exc.orig
    while error is not None:
        name = getattr(error, "constraint_name", None)
        if isinstance(name, str):
            return name
        error = error.__cause__
    return None


class SqlAlchemyUnitOfWork:
    payments: SqlAlchemyPaymentRepository
    outbox: SqlAlchemyOutboxRepository

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory
        self._session: AsyncSession | None = None

    async def __aenter__(self) -> Self:
        if self._session is not None:
            raise RuntimeError("UnitOfWork is single-use, create a new one")
        self._session = self._session_factory()
        self.payments = SqlAlchemyPaymentRepository(self._session)
        self.outbox = SqlAlchemyOutboxRepository(self._session)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        # close() откатывает незакоммиченную транзакцию и возвращает соединение в пул.
        await self._require_session().close()

    async def commit(self) -> None:
        session = self._require_session()
        self.payments.sync()
        try:
            await session.commit()
        except IntegrityError as exc:
            await session.rollback()
            if constraint_name(exc) == IDEMPOTENCY_KEY_CONSTRAINT:
                raise DuplicateIdempotencyKeyError("idempotency key already exists") from exc
            raise

    def _require_session(self) -> AsyncSession:
        if self._session is None:
            raise RuntimeError("UnitOfWork is not entered")
        return self._session
