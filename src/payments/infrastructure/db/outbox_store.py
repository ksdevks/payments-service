import uuid
from collections.abc import Sequence
from datetime import timedelta

from sqlalchemy import ColumnElement, Interval, func, literal, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payments.infrastructure.db.models import OutboxRecord
from payments.infrastructure.outbox import ClaimedMessage


def _interval(seconds: float) -> ColumnElement[timedelta]:
    return literal(timedelta(seconds=seconds), Interval())


class SqlAlchemyOutboxStore:
    def __init__(
        self, session_factory: async_sessionmaker[AsyncSession], *, lease_seconds: float = 30.0
    ) -> None:
        self._session_factory = session_factory
        self._lease_seconds = lease_seconds

    async def claim_batch(self, limit: int) -> Sequence[ClaimedMessage]:
        """Атомарно арендует до limit готовых к отправке сообщений.

        SKIP LOCKED позволяет нескольким relay работать параллельно без
        пересечений: строки, захваченные чужой транзакцией, пропускаются.
        """
        due = (
            select(OutboxRecord.id)
            .where(
                OutboxRecord.published_at.is_(None),
                OutboxRecord.next_attempt_at <= func.now(),
            )
            .order_by(OutboxRecord.next_attempt_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
            .cte("due")
        )
        stmt = (
            update(OutboxRecord)
            .where(OutboxRecord.id == due.c.id)
            .values(
                next_attempt_at=func.now() + _interval(self._lease_seconds),
                attempts=OutboxRecord.attempts + 1,
            )
            .returning(
                OutboxRecord.id, OutboxRecord.topic, OutboxRecord.payload, OutboxRecord.attempts
            )
            .execution_options(synchronize_session=False)
        )
        async with self._session_factory() as session, session.begin():
            rows = (await session.execute(stmt)).all()
        return [
            ClaimedMessage(id=row.id, topic=row.topic, payload=row.payload, attempts=row.attempts)
            for row in rows
        ]

    async def mark_published(self, ids: Sequence[uuid.UUID]) -> None:
        # Без fencing: сообщение уже подтверждено брокером, отметить его может
        # и relay с истекшей арендой. Это убирает лишний дубль, а не создает.
        stmt = (
            update(OutboxRecord)
            .where(OutboxRecord.id.in_(ids), OutboxRecord.published_at.is_(None))
            .values(published_at=func.now(), last_error=None)
            .execution_options(synchronize_session=False)
        )
        async with self._session_factory() as session, session.begin():
            await session.execute(stmt)

    async def release(self, messages: Sequence[ClaimedMessage]) -> None:
        async with self._session_factory() as session, session.begin():
            for message in messages:
                await session.execute(
                    update(OutboxRecord)
                    .where(
                        OutboxRecord.id == message.id,
                        OutboxRecord.published_at.is_(None),
                        OutboxRecord.attempts == message.attempts,
                    )
                    .values(next_attempt_at=func.now())
                    .execution_options(synchronize_session=False)
                )

    async def mark_failed(self, message: ClaimedMessage, error: str, retry_in: float) -> None:
        stmt = (
            update(OutboxRecord)
            .where(
                OutboxRecord.id == message.id,
                OutboxRecord.published_at.is_(None),
                # Fencing: строку не захватывали заново после нашего claim.
                OutboxRecord.attempts == message.attempts,
            )
            .values(last_error=error, next_attempt_at=func.now() + _interval(retry_in))
            .execution_options(synchronize_session=False)
        )
        async with self._session_factory() as session, session.begin():
            await session.execute(stmt)
