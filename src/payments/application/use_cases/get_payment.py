import uuid

from payments.application.errors import PaymentNotFoundError
from payments.application.ports import UnitOfWorkFactory
from payments.domain import Payment


class GetPayment:
    def __init__(self, uow_factory: UnitOfWorkFactory) -> None:
        self._uow_factory = uow_factory

    async def execute(self, payment_id: uuid.UUID) -> Payment:
        async with self._uow_factory() as uow:
            payment = await uow.payments.get(payment_id)
        if payment is None:
            raise PaymentNotFoundError(f"payment {payment_id} not found")
        return payment
