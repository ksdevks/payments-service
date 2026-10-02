import secrets
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Annotated, cast

from fastapi import Depends, Request, Security
from fastapi.security import APIKeyHeader

from payments.application.use_cases import CreatePayment, GetPayment
from payments.entrypoints.api.errors import ApiError


@dataclass(frozen=True)
class ApiServices:
    create_payment: CreatePayment
    get_payment: GetPayment
    check_ready: Callable[[], Awaitable[None]]


def get_services(request: Request) -> ApiServices:
    return cast(ApiServices, request.app.state.services)


# Security scheme: Swagger показывает кнопку Authorize, а клиенты OpenAPI
# получают описание способа аутентификации. Проверку делаем сами, чтобы
# отвечать 401 в едином формате ошибок.
api_key_header = APIKeyHeader(
    name="X-API-Key",
    auto_error=False,
    description="Статический API-ключ сервиса",
)


async def require_api_key(
    request: Request,
    x_api_key: Annotated[str | None, Security(api_key_header)],
) -> None:
    expected = cast(str, request.app.state.api_key)
    # compare_digest: время сравнения не зависит от позиции первого расхождения.
    if x_api_key is None or not secrets.compare_digest(x_api_key.encode(), expected.encode()):
        raise ApiError(401, "unauthorized", "missing or invalid X-API-Key")


Services = Annotated[ApiServices, Depends(get_services)]
