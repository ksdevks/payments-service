import asyncio
import uuid

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payments.domain import ChargeOutcome
from payments.infrastructure.db.gateway_ledger import SqlAlchemyChargeLedger
from payments.infrastructure.db.models import EmulatedGatewayChargeRecord

pytestmark = pytest.mark.postgres


async def test_first_record_wins_under_concurrency(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    ledger = SqlAlchemyChargeLedger(session_factory)
    payment_id = uuid.uuid4()
    outcomes = [ChargeOutcome.SUCCEEDED, ChargeOutcome.DECLINED] * 5

    results = await asyncio.gather(*(ledger.record(payment_id, o) for o in outcomes))

    assert len(set(results)) == 1
    assert await ledger.get(payment_id) == results[0]
    async with session_factory() as session:
        rows = await session.scalar(
            select(func.count()).select_from(EmulatedGatewayChargeRecord)
        )
    assert rows == 1


async def test_unknown_payment_has_no_outcome(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    assert await SqlAlchemyChargeLedger(session_factory).get(uuid.uuid4()) is None
