from erp_example.model import (
    VAT_REDUCED,
    VAT_STANDARD,
    Cents,
    Line,
    Totals,
    money,
    totals,
    vat,
)


def line(quantity: int, unit_price: Cents, vat_rate: int) -> Line:
    return Line(
        product_id="P-1",
        name="Product",
        quantity=quantity,
        unit_price=unit_price,
        vat_rate=vat_rate,
    )


def test_vat_rounds_to_the_nearest_cent() -> None:
    # 9.99 at 20% is 1.998: rounds up.
    assert vat(999, VAT_STANDARD) == 200
    # 0.01 at 10% is 0.001: rounds down to nothing.
    assert vat(1, VAT_REDUCED) == 0
    # Exactly half a cent rounds up.
    assert vat(5, VAT_REDUCED) == 1
    assert vat(0, VAT_STANDARD) == 0


def test_totals_sum_vat_line_by_line() -> None:
    # 3 x 9.99 = 29.97 net, 5.99 VAT; 2 x 45.50 = 91.00 net, 9.10 VAT.
    result = totals([line(3, 999, VAT_STANDARD), line(2, 4550, VAT_REDUCED)])
    assert result.net == 2997 + 9100
    assert result.vat == 599 + 910
    assert result.gross == result.net + result.vat


def test_totals_of_nothing_are_zero() -> None:
    assert totals([]) == Totals(net=0, vat=0, gross=0)


def test_money_is_readable() -> None:
    assert money(123_456) == "1234.56 EUR"
    assert money(5) == "0.05 EUR"
    assert money(0) == "0.00 EUR"
    assert money(-999) == "-9.99 EUR"
