import uuid

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payments.domain import ChargeOutcome
from payments.infrastructure.db.models import EmulatedGatewayChargeRecord


class SqlAlchemyChargeLedger:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def get(self, payment_id: uuid.UUID) -> ChargeOutcome | None:
        async with self._session_factory() as session:
            outcome = await session.scalar(
                select(EmulatedGatewayChargeRecord.outcome).where(
                    EmulatedGatewayChargeRecord.payment_id == payment_id
                )
            )
        return ChargeOutcome(outcome) if outcome is not None else None

    async def record(self, payment_id: uuid.UUID, outcome: ChargeOutcome) -> ChargeOutcome:
        # Параллельная вставка того же ключа ждет коммита первой и превращается
        # в DO NOTHING; следующий SELECT (новый снимок в READ COMMITTED) видит
        # победивший результат.
        stmt = (
            insert(EmulatedGatewayChargeRecord)
            .values(payment_id=payment_id, outcome=outcome.value)
            .on_conflict_do_nothing(index_elements=[EmulatedGatewayChargeRecord.payment_id])
        )
        async with self._session_factory() as session, session.begin():
            await session.execute(stmt)
            stored = await session.scalar(
                select(EmulatedGatewayChargeRecord.outcome).where(
                    EmulatedGatewayChargeRecord.payment_id == payment_id
                )
            )
        assert stored is not None
        return ChargeOutcome(stored)
