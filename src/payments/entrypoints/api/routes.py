import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, Response, status

from payments.application.commands import CreatePaymentCommand
from payments.entrypoints.api.dependencies import Services, require_api_key
from payments.entrypoints.api.errors import ApiError
from payments.entrypoints.api.schemas import (
    CreatePaymentRequest,
    ErrorResponse,
    PaymentAcceptedResponse,
    PaymentResponse,
)

PAYMENTS_PREFIX = "/api/v1/payments"

payments_router = APIRouter(
    prefix=PAYMENTS_PREFIX,
    tags=["payments"],
    dependencies=[Depends(require_api_key)],
    responses={401: {"model": ErrorResponse}},
)
# ТЗ требует ключ на всех эндпоинтах, включая служебные. Healthcheck в compose
# и пробы оркестратора передают заголовок X-API-Key.
health_router = APIRouter(
    prefix="/health",
    tags=["health"],
    dependencies=[Depends(require_api_key)],
    responses={401: {"model": ErrorResponse}},
)


@payments_router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    responses={409: {"model": ErrorResponse}},
    summary="Принять платеж в обработку",
)
async def create_payment(
    body: CreatePaymentRequest,
    idempotency_key: Annotated[str, Header(min_length=1, max_length=255)],
    services: Services,
    response: Response,
) -> PaymentAcceptedResponse:
    result = await services.create_payment.execute(
        CreatePaymentCommand(
            idempotency_key=idempotency_key,
            amount=body.amount,
            currency=body.currency,
            description=body.description,
            webhook_url=str(body.webhook_url),
            metadata=body.metadata,
        )
    )
    response.headers["Location"] = f"{PAYMENTS_PREFIX}/{result.payment.id}"
    if not result.created:
        response.headers["Idempotent-Replayed"] = "true"
    return PaymentAcceptedResponse.from_domain(result.payment)


@payments_router.get(
    "/{payment_id}",
    responses={404: {"model": ErrorResponse}},
    summary="Получить платеж",
)
async def get_payment(payment_id: uuid.UUID, services: Services) -> PaymentResponse:
    return PaymentResponse.from_domain(await services.get_payment.execute(payment_id))


@health_router.get("/live", summary="Процесс жив")
async def live() -> dict[str, str]:
    return {"status": "ok"}


@health_router.get("/ready", summary="Готов принимать трафик (БД доступна)")
async def ready(services: Services) -> dict[str, str]:
    try:
        await services.check_ready()
    except Exception as exc:
        raise ApiError(503, "not_ready", f"database unavailable: {type(exc).__name__}") from exc
    return {"status": "ok"}
