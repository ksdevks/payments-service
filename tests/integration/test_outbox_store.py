import asyncio
import uuid
from datetime import UTC, datetime

import pytest
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payments.application.events import OutboxMessage, PaymentCreated
from payments.infrastructure.db.mappers import outbox_to_record
from payments.infrastructure.db.models import OutboxRecord
from payments.infrastructure.db.outbox_store import SqlAlchemyOutboxStore

pytestmark = pytest.mark.postgres


async def seed(session_factory: async_sessionmaker[AsyncSession], count: int) -> list[uuid.UUID]:
    # Время из прошлого: сообщения сразу готовы к отправке по часам БД.
    created = datetime(2026, 1, 1, tzinfo=UTC)
    messages = [
        OutboxMessage.for_payment_created(
            PaymentCreated(event_id=uuid.uuid4(), payment_id=uuid.uuid4()), created
        )
        for _ in range(count)
    ]
    async with session_factory() as session, session.begin():
        session.add_all(outbox_to_record(m) for m in messages)
    return [m.id for m in messages]


async def test_claim_leases_rows_and_counts_attempts(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    ids = await seed(session_factory, 3)
    store = SqlAlchemyOutboxStore(session_factory, lease_seconds=60)

    first = await store.claim_batch(10)
    second = await store.claim_batch(10)

    assert {m.id for m in first} == set(ids)
    assert all(m.attempts == 1 for m in first)
    assert second == []


async def test_parallel_claims_do_not_overlap(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await seed(session_factory, 50)
    store = SqlAlchemyOutboxStore(session_factory, lease_seconds=60)

    batches = await asyncio.gather(*(store.claim_batch(10) for _ in range(8)))

    claimed = [m.id for batch in batches for m in batch]
    assert len(claimed) == 50
    assert len(set(claimed)) == 50


async def test_expired_lease_is_reclaimed(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    [message_id] = await seed(session_factory, 1)
    store = SqlAlchemyOutboxStore(session_factory, lease_seconds=60)
    await store.claim_batch(10)

    # Эмулируем упавший relay: аренда истекла, публикация не отмечена.
    await expire_lease(session_factory, message_id)

    [reclaimed] = await store.claim_batch(10)
    assert reclaimed.id == message_id
    assert reclaimed.attempts == 2


async def test_published_and_failed_rows(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    published_id, failed_id = await seed(session_factory, 2)
    store = SqlAlchemyOutboxStore(session_factory, lease_seconds=60)
    claimed = {m.id: m for m in await store.claim_batch(10)}

    await store.mark_published([published_id])
    await store.mark_failed(claimed[failed_id], "ConnectionError()", retry_in=3600)

    rows = await load(session_factory)
    assert rows[published_id].published_at is not None
    assert rows[failed_id].published_at is None
    assert rows[failed_id].last_error == "ConnectionError()"
    # Ни опубликованное, ни отложенное на час сообщение повторно не захватываются.
    assert await store.claim_batch(10) == []


async def test_stale_owner_cannot_reschedule_reclaimed_row(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    [message_id] = await seed(session_factory, 1)
    store = SqlAlchemyOutboxStore(session_factory, lease_seconds=60)
    [stale] = await store.claim_batch(10)
    await expire_lease(session_factory, message_id)
    [fresh] = await store.claim_batch(10)

    # Relay с истекшей арендой поздно сообщает об ошибке - запись не меняется.
    await store.mark_failed(stale, "late error", retry_in=3600)

    row = (await load(session_factory))[message_id]
    assert row.last_error is None
    assert row.attempts == fresh.attempts == 2


async def test_release_returns_row_only_for_current_owner(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    [message_id] = await seed(session_factory, 1)
    store = SqlAlchemyOutboxStore(session_factory, lease_seconds=60)
    [first] = await store.claim_batch(10)

    await store.release([first])
    [second] = await store.claim_batch(10)  # возвращенная строка доступна сразу
    assert second.id == message_id
    assert second.attempts == 2

    await store.release([first])  # устаревший владелец ничего не меняет
    assert await store.claim_batch(10) == []  # строка все еще у второго владельца


async def load(
    session_factory: async_sessionmaker[AsyncSession],
) -> dict[uuid.UUID, OutboxRecord]:
    async with session_factory() as session:
        return {r.id: r for r in await session.scalars(select(OutboxRecord))}


async def expire_lease(
    session_factory: async_sessionmaker[AsyncSession], message_id: uuid.UUID
) -> None:
    async with session_factory() as session, session.begin():
        await session.execute(
            update(OutboxRecord)
            .where(OutboxRecord.id == message_id)
            .values(next_attempt_at=datetime(2026, 1, 1, tzinfo=UTC))
        )
