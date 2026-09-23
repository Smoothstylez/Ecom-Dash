"""SC_VAT_TAX_REPORT-Parser, Steuerklassifikation und Return-Vererbung.

Belegt an der realen 84-Spalten-CSV (180 Zeilen = 163 SHIPMENT + 16 RETURN + 1 REFUND).

Verbindliche Reihenfolge (Korrektur): RETURN/REFUND wird zuerst als Korrektur-
transaktion erkannt und erbt die Originalklasse des SHIPMENT ueber
(Order ID, Shipment ID, SKU). Erst danach greifen die Regeln fuer neue SHIPMENTs.

Betragslogik (Korrektur): Amazons OUR_PRICE/SHIPPING/GIFTWRAP-Komponenten
inkl. negativer Promos sind die Primaerquelle. Nur wo Amazon keine Steuer
berechnet hat und unsere Logik 19 % setzt, wird net = round(gross / 1.19)
gerechnet. Nie net * 1.19.

`Tax Type` ist immer 'VAT' und `Tax Reporting Scheme` immer leer -> beide sind
KEINE Discriminators. Massgeblich ist `Tax Calculation Reason Code`.
"""
from __future__ import annotations

from app.services import amazon_tax_import as ati
from app.services.ust_schema import EU_TAX_REGIME_HOME_RATE, EU_TAX_REGIME_OSS, EU_TAX_REGIME_UNCONFIRMED

COMPONENTS = ("OUR_PRICE", "SHIPPING", "GIFTWRAP")


def _row(**overrides):
    base = {
        "Marketplace ID": "A1PA6795UKMFR9",
        "Merchant ID": "M-1",
        "Order Date": "2026-06-02T10:00:00Z",
        "Transaction Type": "SHIPMENT",
        "Is Invoice Corrected": "false",
        "Order ID": "333-1000001",
        "Shipment Date": "2026-06-03T00:00:00Z",
        "Shipment ID": "115943490522302",
        "Transaction ID": "TX-SHIP-1",
        "ASIN": "B000",
        "SKU": "SKU-A",
        "Quantity": "1",
        "Tax Calculation Date": "2026-06-03T00:00:00Z",
        "Tax Rate": "0.1900",
        "Product Tax Code": "A_GEN_NOTAX",
        "Currency": "EUR",
        "Tax Type": "VAT",
        "Tax Calculation Reason Code": "Taxable",
        "Tax Reporting Scheme": "",
        "Tax Collection Responsibility": "Seller",
        "Tax Address Role": "ship-to",
        "OUR_PRICE Tax Inclusive Selling Price": "29.90",
        "OUR_PRICE Tax Amount": "4.77",
        "OUR_PRICE Tax Exclusive Selling Price": "25.13",
        "OUR_PRICE Tax Inclusive Promo Amount": "0.00",
        "OUR_PRICE Tax Amount Promo": "0.00",
        "OUR_PRICE Tax Exclusive Promo Amount": "0.00",
        "SHIPPING Tax Inclusive Selling Price": "0.00",
        "SHIPPING Tax Amount": "0.00",
        "SHIPPING Tax Exclusive Selling Price": "0.00",
        "SHIPPING Tax Inclusive Promo Amount": "0.00",
        "SHIPPING Tax Amount Promo": "0.00",
        "SHIPPING Tax Exclusive Promo Amount": "0.00",
        "GIFTWRAP Tax Inclusive Selling Price": "0.00",
        "GIFTWRAP Tax Amount": "0.00",
        "GIFTWRAP Tax Exclusive Selling Price": "0.00",
        "GIFTWRAP Tax Inclusive Promo Amount": "0.00",
        "GIFTWRAP Tax Amount Promo": "0.00",
        "GIFTWRAP Tax Exclusive Promo Amount": "0.00",
        "Seller Tax Registration": "DE458504535",
        "Seller Tax Registration Jurisdiction": "DE",
        "Buyer Tax Registration": "",
        "Buyer Tax Registration Jurisdiction": "",
        "Buyer Tax Registration Type": "",
        "Converted Tax Amount": "0.00",
        "VAT Invoice Number": "DE60000J54EX3I",
        "Invoice Url": "https://sellercentral.amazon.de/document/download?x=1",
        "Export Outside EU": "false",
        "Ship From Country": "DE",
        "Ship To Country": "DE",
        "Return Fc Country": "",
        "Is Amazon Invoiced": "true",
        "Original VAT Invoice Number": "",
        "Invoice Correction Details": "",
    }
    base.update(overrides)
    return base


# ── Betragssummation: Amazon ist Primaerquelle ───────────────────────────────


def test_component_summation_adds_negative_promos():
    """Reale Zeile: OUR_PRICE 169.90/27.13/142.77 + SHIPPING 3.99/0.63/3.36
    + SHIPPING-Promo -3.99/-0.63/-3.36 -> 169.90 / 27.13 / 142.77."""
    row = _row(**{
        "OUR_PRICE Tax Inclusive Selling Price": "169.90",
        "OUR_PRICE Tax Amount": "27.13",
        "OUR_PRICE Tax Exclusive Selling Price": "142.77",
        "SHIPPING Tax Inclusive Selling Price": "3.99",
        "SHIPPING Tax Amount": "0.63",
        "SHIPPING Tax Exclusive Selling Price": "3.36",
        "SHIPPING Tax Inclusive Promo Amount": "-3.99",
        "SHIPPING Tax Amount Promo": "-0.63",
        "SHIPPING Tax Exclusive Promo Amount": "-3.36",
    })
    assert ati.sum_components(row) == (16990, 2713, 14277)


def test_component_summation_scales_all_three_components():
    row = _row(**{
        "OUR_PRICE Tax Inclusive Selling Price": "10.00",
        "OUR_PRICE Tax Amount": "1.60",
        "OUR_PRICE Tax Exclusive Selling Price": "8.40",
        "GIFTWRAP Tax Inclusive Selling Price": "2.00",
        "GIFTWRAP Tax Amount": "0.32",
        "GIFTWRAP Tax Exclusive Selling Price": "1.68",
    })
    gross_cents, tax_cents, net_cents = ati.sum_components(row)
    assert (gross_cents, tax_cents, net_cents) == (1200, 192, 1008)
    assert gross_cents == net_cents + tax_cents


def test_nonexistent_amount_columns_are_not_required():
    assert "Taxable Amount" not in _row()
    assert "NonTaxable Amount" not in _row()
    ati.sum_components(_row())


# ── SHIPMENT-Klassifikation ─────────────────────────────────────────────────


def test_de_shipment_keeps_amazons_own_tax_components():
    """148/148 DE-19%-Zeilen haben bereits korrekte Amazon-Steuer -> nicht neu rechnen."""
    result = ati.classify_amazon_tax_row(_row(), eu_tax_regime=EU_TAX_REGIME_UNCONFIRMED)
    assert result["tax_class"] == "de_b2c"
    assert result["net_source"] == "amazon_components"
    assert (result["gross_cents"], result["output_vat_cents"], result["net_cents"]) == (2990, 477, 2513)
    assert result["blocker"] is False


def test_de_shipment_with_domestic_buyer_vat_is_still_de_b2c():
    result = ati.classify_amazon_tax_row(
        _row(**{
            "Buyer Tax Registration": "DE123456789",
            "Buyer Tax Registration Type": "VAT",
            "Buyer Tax Registration Jurisdiction": "DE",
        }),
        eu_tax_regime=EU_TAX_REGIME_UNCONFIRMED,
    )
    assert result["tax_class"] == "de_b2c"
    assert result["is_domestic_b2b"] is True
    assert result["output_vat_cents"] == 477


def test_eu_b2b_is_intra_community_supply_not_generic_reverse_charge():
    """DE->EU mit gueltiger auslaendischer USt-ID = innergemeinschaftliche Lieferung."""
    row = _row(**{
        "Ship To Country": "AT",
        "Tax Rate": "0.0000",
        "Tax Calculation Reason Code": "Taxable",
        "Buyer Tax Registration": "ATU83097435",
        "Buyer Tax Registration Type": "VAT",
        "Buyer Tax Registration Jurisdiction": "AT",
        "OUR_PRICE Tax Inclusive Selling Price": "28.49",
        "OUR_PRICE Tax Amount": "0.00",
        "OUR_PRICE Tax Exclusive Selling Price": "28.49",
    })
    result = ati.classify_amazon_tax_row(row, eu_tax_regime=EU_TAX_REGIME_UNCONFIRMED)
    assert result["tax_class"] == "eu_b2b_intra_community_supply"
    assert result["output_vat_cents"] == 0
    assert result["net_cents"] == 2849
    assert result["net_source"] == "amazon_components"
    assert result["buyer_vat_number"] == "ATU83097435"
    assert result["blocker"] is False


def test_eu_b2b_requires_ship_from_de_ship_to_other_eu_vat_id_and_taxable():
    overrides = [
        {"Ship From Country": "CN"},
        {"Ship To Country": "DE"},
        {"Buyer Tax Registration": ""},
        {"Buyer Tax Registration Type": "BUSINESS"},
        {"Tax Calculation Reason Code": "NonTaxable"},
        {"Tax Rate": "0.1900"},
    ]
    for extra in overrides:
        row = _row(**{
            "Ship To Country": "AT",
            "Tax Rate": "0.0000",
            "Tax Calculation Reason Code": "Taxable",
            "Buyer Tax Registration": "ATU83097435",
            "Buyer Tax Registration Type": "VAT",
            **extra,
        })
        result = ati.classify_amazon_tax_row(row, eu_tax_regime=EU_TAX_REGIME_UNCONFIRMED)
        assert result["tax_class"] != "eu_b2b_intra_community_supply", extra


def test_eu_b2c_home_rate_splits_german_vat_out_of_gross():
    row = _row(**{
        "Ship To Country": "AT",
        "Tax Rate": "0.0000",
        "Tax Calculation Reason Code": "NonTaxable",
        "Is Amazon Invoiced": "false",
        "OUR_PRICE Tax Inclusive Selling Price": "29.90",
        "OUR_PRICE Tax Amount": "0.00",
        "OUR_PRICE Tax Exclusive Selling Price": "29.90",
    })
    result = ati.classify_amazon_tax_row(row, eu_tax_regime=EU_TAX_REGIME_HOME_RATE)
    assert result["tax_class"] == "eu_b2c_home_rate"
    assert result["net_source"] == "computed_home_rate"
    assert (result["gross_cents"], result["output_vat_cents"], result["net_cents"]) == (2990, 477, 2513)
    assert "AMAZON_VAT_CALCULATION_MISSING" in result["warnings"]
    assert result["blocker"] is False


def test_eu_b2c_home_rate_divides_rather_than_multiplies():
    row = _row(**{
        "Ship To Country": "AT",
        "Tax Rate": "0.0000",
        "Tax Calculation Reason Code": "NonTaxable",
        "OUR_PRICE Tax Inclusive Selling Price": "26.99",
        "OUR_PRICE Tax Amount": "0.00",
        "OUR_PRICE Tax Exclusive Selling Price": "26.99",
    })
    result = ati.classify_amazon_tax_row(row, eu_tax_regime=EU_TAX_REGIME_HOME_RATE)
    assert result["net_cents"] == 2268
    assert result["output_vat_cents"] == 431
    assert result["gross_cents"] == result["net_cents"] + result["output_vat_cents"]


def test_eu_b2c_is_unresolved_without_confirmed_regime():
    row = _row(**{
        "Ship To Country": "AT",
        "Tax Rate": "0.0000",
        "Tax Calculation Reason Code": "NonTaxable",
    })
    result = ati.classify_amazon_tax_row(row, eu_tax_regime=EU_TAX_REGIME_UNCONFIRMED)
    assert result["tax_class"] == "unresolved"
    assert result["output_vat_cents"] == 0
    assert result["blocker"] is True


def test_eu_b2c_is_unresolved_under_oss_regime():
    row = _row(**{
        "Ship To Country": "AT",
        "Tax Rate": "0.0000",
        "Tax Calculation Reason Code": "NonTaxable",
    })
    result = ati.classify_amazon_tax_row(row, eu_tax_regime=EU_TAX_REGIME_OSS)
    assert result["tax_class"] == "unresolved"
    assert result["blocker"] is True


def test_deemed_supplier_is_not_our_output_vat():
    result = ati.classify_amazon_tax_row(
        _row(**{"Tax Collection Responsibility": "Amazon"}), eu_tax_regime=EU_TAX_REGIME_UNCONFIRMED
    )
    assert result["tax_class"] == "deemed_supplier"
    assert result["output_vat_cents"] == 0
    assert result["blocker"] is False


def test_export_outside_eu_is_tax_free():
    result = ati.classify_amazon_tax_row(
        _row(**{"Export Outside EU": "true", "Ship To Country": "CH", "Tax Rate": "0.0000"}),
        eu_tax_regime=EU_TAX_REGIME_UNCONFIRMED,
    )
    assert result["tax_class"] == "export"
    assert result["output_vat_cents"] == 0
    assert result["blocker"] is False


def test_shipment_booking_date_is_shipment_date_not_order_date():
    result = ati.classify_amazon_tax_row(
        _row(**{"Order Date": "2026-06-02T10:00:00Z", "Shipment Date": "2026-07-03T00:00:00Z"}),
        eu_tax_regime=EU_TAX_REGIME_UNCONFIRMED,
    )
    assert result["booking_date"] == "2026-07-03"


def test_tax_type_and_tax_reporting_scheme_are_not_discriminators():
    row = _row(**{
        "Tax Type": "VAT",
        "Tax Reporting Scheme": "",
        "Tax Calculation Reason Code": "NonTaxable",
        "Tax Rate": "0.0000",
        "Ship To Country": "AT",
    })
    result = ati.classify_amazon_tax_row(row, eu_tax_regime=EU_TAX_REGIME_UNCONFIRMED)
    assert result["tax_class"] == "unresolved"
    assert result["reason_code"] == "NonTaxable"


# ── Task 5: RETURN/REFUND erbt die Originalklasse ───────────────────────────


def _shipment_at_b2c():
    """Ursprung: AT-B2C, Amazon Tax=0, NonTaxable -> eu_b2c_home_rate 25,13 / 4,77."""
    return _row(**{
        "Order ID": "115-100",
        "Shipment ID": "115679504167302",
        "SKU": "SKU-A",
        "Ship To Country": "AT",
        "Tax Rate": "0.0000",
        "Tax Calculation Reason Code": "NonTaxable",
        "Is Amazon Invoiced": "false",
        "OUR_PRICE Tax Inclusive Selling Price": "29.90",
        "OUR_PRICE Tax Amount": "0.00",
        "OUR_PRICE Tax Exclusive Selling Price": "29.90",
    })


def _return_at_b2c():
    """Retoure: Amazon Tax weiterhin 0, Betrag negativ. Muss -25,13/-4,77 ergeben."""
    return _row(**{
        "Transaction Type": "RETURN",
        "Transaction ID": "TX-RET-1",
        "Order ID": "115-100",
        "Shipment ID": "115679504167302",
        "SKU": "SKU-A",
        "Shipment Date": "2026-07-11T00:00:00Z",
        "Tax Calculation Date": "2026-07-11T00:00:00Z",
        "Ship To Country": "AT",
        "Tax Rate": "0.0000",
        "Tax Calculation Reason Code": "NonTaxable",
        "Is Amazon Invoiced": "false",
        "OUR_PRICE Tax Inclusive Selling Price": "-29.90",
        "OUR_PRICE Tax Amount": "0.00",
        "OUR_PRICE Tax Exclusive Selling Price": "-29.90",
    })


def test_return_inherits_original_class_via_order_shipment_sku():
    results = ati.link_and_inherit(
        [_shipment_at_b2c(), _return_at_b2c()],
        eu_tax_regime=EU_TAX_REGIME_HOME_RATE,
    )
    shipment, refund = results
    assert shipment["tax_class"] == "eu_b2c_home_rate"
    assert refund["tax_class"] == "eu_b2c_home_rate"
    assert refund["original_tax_class"] == "eu_b2c_home_rate"


def test_return_of_home_rate_sale_recomputes_german_vat_despite_zero_amazon_tax():
    """Beispiel: 29,90 Verkauf -> 25,13/4,77; Retoure mit Amazon Tax=0 -> -25,13/-4,77."""
    results = ati.link_and_inherit(
        [_shipment_at_b2c(), _return_at_b2c()],
        eu_tax_regime=EU_TAX_REGIME_HOME_RATE,
    )
    refund = results[1]
    assert refund["gross_cents"] == -2990
    assert refund["net_cents"] == -2513
    assert refund["output_vat_cents"] == -477
    assert refund["net_source"] == "computed_home_rate"


def test_return_booking_date_comes_from_the_return_row_not_the_order():
    results = ati.link_and_inherit(
        [_shipment_at_b2c(), _return_at_b2c()],
        eu_tax_regime=EU_TAX_REGIME_HOME_RATE,
    )
    assert results[0]["booking_date"] == "2026-06-03"
    assert results[1]["booking_date"] == "2026-07-11"


def test_eu_b2b_return_stays_at_zero_percent():
    shipment = _row(**{
        "Order ID": "115-100",
        "Shipment ID": "114221607728302",
        "SKU": "SKU-A",
        "Ship To Country": "AT",
        "Tax Rate": "0.0000",
        "Tax Calculation Reason Code": "Taxable",
        "Buyer Tax Registration": "ATU45558604",
        "Buyer Tax Registration Type": "VAT",
        "OUR_PRICE Tax Inclusive Selling Price": "28.56",
        "OUR_PRICE Tax Amount": "0.00",
        "OUR_PRICE Tax Exclusive Selling Price": "28.56",
    })
    refund = _row(**{
        "Transaction Type": "RETURN",
        "Transaction ID": "TX-RET-2",
        "Order ID": "115-100",
        "Shipment ID": "114221607728302",
        "SKU": "SKU-A",
        "Shipment Date": "2026-07-20T00:00:00Z",
        "Ship To Country": "AT",
        "Tax Rate": "0.0000",
        "Tax Calculation Reason Code": "Taxable",
        "Buyer Tax Registration": "ATU45558604",
        "Buyer Tax Registration Type": "VAT",
        "OUR_PRICE Tax Inclusive Selling Price": "-28.56",
        "OUR_PRICE Tax Amount": "0.00",
        "OUR_PRICE Tax Exclusive Selling Price": "-28.56",
    })
    results = ati.link_and_inherit([shipment, refund], eu_tax_regime=EU_TAX_REGIME_HOME_RATE)
    assert results[1]["tax_class"] == "eu_b2b_intra_community_supply"
    assert results[1]["output_vat_cents"] == 0
    assert results[1]["net_cents"] == -2856


def test_de_return_keeps_amazons_own_tax_amount_without_recomputing():
    shipment = _row(**{
        "Order ID": "115-100", "Shipment ID": "115943490522302", "SKU": "SKU-A",
        "OUR_PRICE Tax Inclusive Selling Price": "29.90",
        "OUR_PRICE Tax Amount": "4.77",
        "OUR_PRICE Tax Exclusive Selling Price": "25.13",
    })
    refund = _row(**{
        "Transaction Type": "RETURN",
        "Transaction ID": "TX-RET-3",
        "Order ID": "115-100",
        "Shipment ID": "115943490522302",
        "SKU": "SKU-A",
        "Shipment Date": "2026-07-15T00:00:00Z",
        "OUR_PRICE Tax Inclusive Selling Price": "-29.90",
        "OUR_PRICE Tax Amount": "-4.77",
        "OUR_PRICE Tax Exclusive Selling Price": "-25.13",
    })
    results = ati.link_and_inherit([shipment, refund], eu_tax_regime=EU_TAX_REGIME_UNCONFIRMED)
    assert results[1]["tax_class"] == "de_b2c"
    assert results[1]["net_source"] == "amazon_components"
    assert (results[1]["gross_cents"], results[1]["output_vat_cents"], results[1]["net_cents"]) == (-2990, -477, -2513)


def test_refund_is_treated_like_a_return():
    shipment = _row(**{"Order ID": "115-100", "Shipment ID": "114348314579302", "SKU": "SKU-A"})
    refund = _row(**{
        "Transaction Type": "REFUND",
        "Transaction ID": "TX-REF-1",
        "Order ID": "115-100",
        "Shipment ID": "114348314579302",
        "SKU": "SKU-A",
        "Shipment Date": "2026-07-18T00:00:00Z",
        "OUR_PRICE Tax Inclusive Selling Price": "-33.99",
        "OUR_PRICE Tax Amount": "-5.43",
        "OUR_PRICE Tax Exclusive Selling Price": "-28.56",
    })
    results = ati.link_and_inherit([shipment, refund], eu_tax_regime=EU_TAX_REGIME_UNCONFIRMED)
    assert results[1]["tax_class"] == "de_b2c"
    assert results[1]["transaction_type"] == "REFUND"


def test_export_return_inherits_export_class():
    """Korrektur: Auch Export-/Deemed-Supplier-Umsaetze werden von Returns ererbt."""
    shipment = _row(**{
        "Order ID": "115-100", "Shipment ID": "S-EXP", "SKU": "SKU-A",
        "Export Outside EU": "true", "Ship To Country": "CH", "Tax Rate": "0.0000",
    })
    refund = _row(**{
        "Transaction Type": "RETURN",
        "Transaction ID": "TX-RET-EXP",
        "Order ID": "115-100", "Shipment ID": "S-EXP", "SKU": "SKU-A",
        "Shipment Date": "2026-07-12T00:00:00Z",
        "Export Outside EU": "true", "Ship To Country": "CH", "Tax Rate": "0.0000",
        "OUR_PRICE Tax Inclusive Selling Price": "-20.00",
        "OUR_PRICE Tax Amount": "0.00",
        "OUR_PRICE Tax Exclusive Selling Price": "-20.00",
    })
    results = ati.link_and_inherit([shipment, refund], eu_tax_regime=EU_TAX_REGIME_UNCONFIRMED)
    assert results[1]["tax_class"] == "export"
    assert results[1]["output_vat_cents"] == 0


def test_deemed_supplier_return_inherits_deemed_supplier_class():
    shipment = _row(**{
        "Order ID": "115-100", "Shipment ID": "S-DS", "SKU": "SKU-A",
        "Tax Collection Responsibility": "Amazon",
    })
    refund = _row(**{
        "Transaction Type": "RETURN",
        "Transaction ID": "TX-RET-DS",
        "Order ID": "115-100", "Shipment ID": "S-DS", "SKU": "SKU-A",
        "Shipment Date": "2026-07-13T00:00:00Z",
        "Tax Collection Responsibility": "Amazon",
        "OUR_PRICE Tax Inclusive Selling Price": "-15.00",
        "OUR_PRICE Tax Amount": "0.00",
        "OUR_PRICE Tax Exclusive Selling Price": "-15.00",
    })
    results = ati.link_and_inherit([shipment, refund], eu_tax_regime=EU_TAX_REGIME_UNCONFIRMED)
    assert results[1]["tax_class"] == "deemed_supplier"


def test_return_with_unresolvable_link_is_flagged_and_blocks():
    refund = _row(**{
        "Transaction Type": "RETURN",
        "Transaction ID": "TX-RET-LOST",
        "Order ID": "999-000",
        "Shipment ID": "UNKNOWN",
        "SKU": "NOPE",
        "Shipment Date": "2026-07-14T00:00:00Z",
    })
    results = ati.link_and_inherit([refund], eu_tax_regime=EU_TAX_REGIME_UNCONFIRMED)
    assert results[0]["tax_class"] == "unresolved_return_link"
    assert results[0]["blocker"] is True


def test_return_is_recognised_before_any_shipment_rule():
    """Korrektur: Eine Retoure eines Deemed-Supplier-Umsatzes erbt zuerst,
    statt ueber 'Tax Collection Responsibility' neu klassifiziert zu werden."""
    refund = _row(**{
        "Transaction Type": "RETURN",
        "Transaction ID": "TX-RET-ORDERONLY",
        "Order ID": "115-100",
        "Shipment ID": "S-DE",
        "SKU": "SKU-A",
        "Tax Collection Responsibility": "Seller",
        "Shipment Date": "2026-07-16T00:00:00Z",
    })
    shipment = _row(**{
        "Order ID": "115-100", "Shipment ID": "S-DE", "SKU": "SKU-A",
        "Tax Collection Responsibility": "Amazon",
    })
    results = ati.link_and_inherit([refund, shipment], eu_tax_regime=EU_TAX_REGIME_UNCONFIRMED)
    by_type = {item["transaction_type"]: item for item in results}
    assert by_type["RETURN"]["tax_class"] == "deemed_supplier"
    assert by_type["RETURN"]["original_tax_class"] == "deemed_supplier"


# ── Parser ──────────────────────────────────────────────────────────────────

CSV_HEADER = (
    "Marketplace ID,Merchant ID,Order Date,Transaction Type,Is Invoice Corrected,Order ID,"
    "Shipment Date,Shipment ID,Transaction ID,ASIN,SKU,Quantity,Tax Calculation Date,Tax Rate,"
    "Product Tax Code,Currency,Tax Type,Tax Calculation Reason Code,Tax Reporting Scheme,"
    "Tax Collection Responsibility,Tax Address Role,Jurisdiction Level,Jurisdiction Name,"
    "OUR_PRICE Tax Inclusive Selling Price,OUR_PRICE Tax Amount,OUR_PRICE Tax Exclusive Selling Price,"
    "OUR_PRICE Tax Inclusive Promo Amount,OUR_PRICE Tax Amount Promo,OUR_PRICE Tax Exclusive Promo Amount,"
    "SHIPPING Tax Inclusive Selling Price,SHIPPING Tax Amount,SHIPPING Tax Exclusive Selling Price,"
    "SHIPPING Tax Inclusive Promo Amount,SHIPPING Tax Amount Promo,SHIPPING Tax Exclusive Promo Amount,"
    "GIFTWRAP Tax Inclusive Selling Price,GIFTWRAP Tax Amount,GIFTWRAP Tax Exclusive Selling Price,"
    "GIFTWRAP Tax Inclusive Promo Amount,GIFTWRAP Tax Amount Promo,GIFTWRAP Tax Exclusive Promo Amount,"
    "Seller Tax Registration,Seller Tax Registration Jurisdiction,Buyer Tax Registration,"
    "Buyer Tax Registration Jurisdiction,Buyer Tax Registration Type,Buyer E Invoice Account Id,"
    "Invoice Level Currency Code,Invoice Level Exchange Rate,Invoice Level Exchange Rate Date,"
    "Converted Tax Amount,VAT Invoice Number,Invoice Url,Export Outside EU,Ship From City,"
    "Ship From State,Ship From Country,Ship From Postal Code,Ship From Tax Location Code,"
    "Ship To City,Ship To State,Ship To Country,Ship To Postal Code,Ship To Location Code,"
    "Return Fc Country,Is Amazon Invoiced,Original VAT Invoice Number,Invoice Correction Details"
)


def _csv_row(**overrides):
    row = _row(**overrides)
    header = CSV_HEADER.split(",")
    return ",".join('"' + str(row.get(name, "")).replace('"', '""') + '"' for name in header)


def test_parser_reads_comma_separated_utf8_sig_file():
    text = "\ufeff" + CSV_HEADER + "\n" + _csv_row() + "\n" + _csv_row(**{"Transaction Type": "RETURN"})
    rows = ati.parse_sc_vat_tax_report(text)
    assert len(rows) == 2
    assert rows[0]["Transaction Type"] == "SHIPMENT"
    assert rows[1]["Transaction Type"] == "RETURN"


def test_parser_reads_tab_separated_file():
    text = CSV_HEADER.replace(",", "\t") + "\n" + _csv_row().replace('","', '"\t"')
    rows = ati.parse_sc_vat_tax_report(text)
    assert len(rows) == 1


def test_parser_requires_the_real_header_columns():
    import pytest

    with pytest.raises(ValueError):
        ati.parse_sc_vat_tax_report("not,a,tax,report\n1,2,3,4\n")


def test_booking_date_parses_amazon_dd_mon_yyyy_utc_format():
    """Amazon schreibt '01-Sep-2026 UTC', nicht ISO-8601."""
    row = _row(**{
        "Order Date": "02-Aug-2026 UTC",
        "Shipment Date": "14-Sep-2026 UTC",
        "Tax Calculation Date": "14-Sep-2026 UTC",
    })
    result = ati.classify_amazon_tax_row(row, eu_tax_regime=EU_TAX_REGIME_UNCONFIRMED)
    assert result["booking_date"] == "2026-09-14"


def test_booking_date_parses_iso_datetime_and_date_only():
    assert ati._date_token("2026-09-14T00:00:00Z") == "2026-09-14"
    assert ati._date_token("2026-09-14") == "2026-09-14"
    assert ati._date_token("14-Sep-2026 10:00:00 UTC") == "2026-09-14"
    assert ati._date_token("") == ""


def test_return_booking_date_uses_return_row_month_not_order_month():
    shipment = _shipment_at_b2c()
    refund = _return_at_b2c()
    results = ati.link_and_inherit([shipment, refund], eu_tax_regime=EU_TAX_REGIME_HOME_RATE)
    assert results[0]["booking_date"] == "2026-06-03"
    assert results[1]["booking_date"] == "2026-07-11"
