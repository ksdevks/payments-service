from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum

from payments.domain.errors import InvalidMoneyError

MINOR_UNIT = Decimal("0.01")
# Соответствует NUMERIC(18, 2): 16 знаков до запятой.
MAX_AMOUNT = Decimal("9999999999999999.99")


class Currency(StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


@dataclass(frozen=True, slots=True)
class Money:
    amount: Decimal
    currency: Currency

    def __post_init__(self) -> None:
        if not isinstance(self.amount, Decimal) or not self.amount.is_finite():
            raise InvalidMoneyError("amount must be a finite Decimal")
        if self.amount <= 0:
            raise InvalidMoneyError("amount must be positive")
        if self.amount > MAX_AMOUNT:
            raise InvalidMoneyError("amount is too large")
        if self.amount != self.amount.quantize(MINOR_UNIT):
            raise InvalidMoneyError("amount must have at most 2 decimal places")
        if not isinstance(self.currency, Currency):
            raise InvalidMoneyError("unsupported currency")
        # Нормализуем представление: 10 и 10.00 - одна и та же сумма.
        object.__setattr__(self, "amount", self.amount.quantize(MINOR_UNIT))
