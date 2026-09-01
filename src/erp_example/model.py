"""Business types, and the small amount of arithmetic that needs no server."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, TypeAlias

# Money is stored in cents, so no rounding depends on operation order.
#
# A plain `int` is enough, and there is nothing else to reach for: Python
# integers are arbitrary-precision, so the same type covers a line total here
# and the `uint64` counts and file sizes the SDK hands back.
Cents: TypeAlias = int

# VAT rates in basis points: 2000 is 20.00%.
VAT_STANDARD = 2000
VAT_REDUCED = 1000

# Statuses are plain strings. They are written into documents and used as-is
# in query filters, so one constant per value keeps both sides in sync: a
# typo in a filter returns zero rows instead of failing.
QUOTE_SENT = "sent"
QUOTE_ACCEPTED = "accepted"
ORDER_PREPARING = "preparing"
ORDER_SHIPPED = "shipped"
INVOICE_ISSUED = "issued"
INVOICE_OVERDUE = "overdue"
INVOICE_PAID = "paid"


@dataclass(frozen=True)
class Customer:
    id: str
    name: str
    email: str
    city: str
    active: bool

    @classmethod
    def from_json(cls, raw: Any) -> Customer:
        return cls(**raw)


@dataclass(frozen=True)
class Supplier:
    id: str
    name: str
    email: str
    lead_time_days: int


@dataclass(frozen=True)
class Product:
    id: str
    reference: str
    name: str
    family: str
    unit_price: Cents
    vat_rate: int
    stock: int
    min_stock: int
    active: bool

    @classmethod
    def from_json(cls, raw: Any) -> Product:
        return cls(**raw)


@dataclass(frozen=True)
class Line:
    product_id: str
    name: str
    quantity: int
    unit_price: Cents
    vat_rate: int


@dataclass(frozen=True)
class Totals:
    net: Cents
    vat: Cents
    gross: Cents


@dataclass(frozen=True)
class Quote:
    id: str
    customer_id: str
    status: str
    date: str
    lines: list[Line]
    totals: Totals


@dataclass(frozen=True)
class Order:
    id: str
    customer_id: str
    quote_id: str
    status: str
    date: str
    lines: list[Line]
    totals: Totals


@dataclass(frozen=True)
class Invoice:
    id: str
    customer_id: str
    order_id: str
    status: str
    date: str
    due_date: str
    lines: list[Line]
    totals: Totals

    @classmethod
    def from_json(cls, raw: Any) -> Invoice:
        # `asdict` flattened the nested dataclasses into dicts on the way out,
        # so the way back has to rebuild them. Nothing does it for you: a
        # decoder is a plain callable, and this is all one is.
        return cls(
            id=raw["id"],
            customer_id=raw["customer_id"],
            order_id=raw["order_id"],
            status=raw["status"],
            date=raw["date"],
            due_date=raw["due_date"],
            lines=[Line(**line) for line in raw["lines"]],
            totals=Totals(**raw["totals"]),
        )


@dataclass(frozen=True)
class StockMove:
    id: str
    product_id: str
    #: `"in"` on a delivery from a supplier, `"out"` on a shipment.
    direction: str
    quantity: int
    source: str

    @classmethod
    def from_json(cls, raw: Any) -> StockMove:
        return cls(**raw)


def vat(net: Cents, rate: int) -> Cents:
    """VAT on a net amount, rounded to the nearest cent.

    Adding half a cent before the division is what does the rounding, so the
    division itself must only drop the remainder. `//` floors rather than
    truncating towards zero, which is the same thing here because no amount in
    this example is negative.
    """
    return (net * rate + 5_000) // 10_000


def totals(lines: Iterable[Line]) -> Totals:
    """Add up lines.

    VAT is computed per line and then summed, the way it is printed on the
    invoice: rounding once at the end would be off by a cent against the
    printed detail.
    """
    net = 0
    vat_total = 0
    for line in lines:
        line_net = line.unit_price * line.quantity
        net += line_net
        vat_total += vat(line_net, line.vat_rate)
    return Totals(net=net, vat=vat_total, gross=net + vat_total)


def money(amount: Cents) -> str:
    """`123456` becomes `"1234.56 EUR"`."""
    sign = "-" if amount < 0 else ""
    whole, cents = divmod(abs(amount), 100)
    return f"{sign}{whole}.{cents:02d} EUR"
