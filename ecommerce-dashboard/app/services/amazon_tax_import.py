"""SC_VAT_TAX_REPORT: Parser, Steuerklassifikation und Return-Vererbung.

Klassifikationsreihenfolge (verbindlich):
  1. RETURN/REFUND  -> als Korrekturtransaktion erkennen und die Klasse des
     Original-SHIPMENT ueber (Order ID, Shipment ID, SKU) erben
  2. Tax Collection Responsibility = Amazon -> deemed_supplier
  3. Export Outside EU                          -> export
  4. DE->DE mit Amazon-Steuersatz 19 %          -> de_b2c
  5. DE->EU, gueltige auslaendische USt-IdNr.   -> eu_b2b_intra_community_supply
  6. EU-B2C ohne USt-IdNr., Regime bestaetigt   -> eu_b2c_home_rate
  7. sonst                                      -> unresolved

Betragslogik: Amazons OUR_PRICE/SHIPPING/GIFTWRAP-Komponenten inkl. negativer
Promos sind die Primaerquelle. Nur wo Amazon keine Steuer berechnet hat und
unsere Logik 19 % setzt (eu_b2c_home_rate) wird aus dem Kundenendpreis
herausgerechnet: net = round_half_up(gross / 1.19), vat = gross - net.

`Tax Type` ist immer 'VAT' und `Tax Reporting Scheme` immer leer; beide sind
keine Discriminators. Massgeblich ist `Tax Calculation Reason Code`.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import sqlite3
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Iterable, Optional

from app.services.ust_schema import EU_TAX_REGIME_HOME_RATE

COMPONENTS = ("OUR_PRICE", "SHIPPING", "GIFTWRAP")
CORRECTION_TYPES = frozenset({"RETURN", "REFUND"})
REQUIRED_COLUMNS = ("Transaction ID", "Order ID", "SKU")

CLASS_DE_B2C = "de_b2c"
CLASS_EU_B2B = "eu_b2b_intra_community_supply"
CLASS_EU_B2C_HOME_RATE = "eu_b2c_home_rate"
CLASS_DEEMED_SUPPLIER = "deemed_supplier"
CLASS_EXPORT = "export"
CLASS_UNRESOLVED = "unresolved"
CLASS_RETURN_UNLINKED = "return_unlinked"
CLASS_UNRESOLVED_RETURN_LINK = "unresolved_return_link"

WARNING_VAT_CALC_MISSING = "AMAZON_VAT_CALCULATION_MISSING"

_HOME_RATE_DIVISOR = Decimal("1.19")
# EU-Fernabsatzschwelle (10.000 EUR), NICHT die §19-UStG-Grenze (100.000 EUR).
EU_DISTANCE_SELLING_THRESHOLD_CENTS = 10_000 * 100


class AmazonTaxImportError(Exception):
    def __init__(self, status_code: int, detail: str) -> None:
        self.status_code = status_code
        self.detail = detail
        super().__init__(detail)


def _text(value: Any) -> str:
    return str(value or "").strip()


def _amount_cents(value: Any) -> int:
    token = _text(value)
    if not token:
        return 0
    try:
        return int((Decimal(token) * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    except Exception:
        return 0


_DATE_FORMATS = ("%d-%b-%Y", "%Y-%m-%d", "%d-%m-%Y", "%d.%m.%Y", "%d/%m/%Y", "%m/%d/%Y")


def _date_token(value: Any) -> str:
    """Normalisiert Datumsfelder auf YYYY-MM-DD.

    Amazon schreibt im SC_VAT_TAX_REPORT ``01-Sep-2026 UTC`` bzw.
    ``01-Sep-2026 10:00:00 UTC``; andere Quellen liefern ISO-8601.
    """
    token = _text(value).replace("-Sept-", "-Sep-")
    if not token:
        return ""
    head = token[:-4] if token.endswith(" UTC") else token
    for candidate in (head, head[:11], head[:10]):
        for format_string in _DATE_FORMATS:
            try:
                return datetime.strptime(candidate, format_string).strftime("%Y-%m-%d")
            except ValueError:
                continue
    return ""


def split_home_rate_vat(gross_cents: int) -> tuple[int, int]:
    """Deutsche 19 % aus dem Kundenendpreis herausrechnen: net = gross / 1.19."""
    sign = -1 if gross_cents < 0 else 1
    absolute = abs(int(gross_cents))
    net = int((Decimal(absolute) / _HOME_RATE_DIVISOR).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    return sign * net, sign * (absolute - net)


def sum_components(row: dict[str, Any]) -> tuple[int, int, int]:
    """(Brutto, Steuer, Netto) in Cent ueber OUR_PRICE/SHIPPING/GIFTWRAP inkl. Promos."""
    gross_cents = tax_cents = net_cents = 0
    for component in COMPONENTS:
        gross_cents += _amount_cents(row.get(f"{component} Tax Inclusive Selling Price"))
        gross_cents += _amount_cents(row.get(f"{component} Tax Inclusive Promo Amount"))
        tax_cents += _amount_cents(row.get(f"{component} Tax Amount"))
        tax_cents += _amount_cents(row.get(f"{component} Tax Amount Promo"))
        net_cents += _amount_cents(row.get(f"{component} Tax Exclusive Selling Price"))
        net_cents += _amount_cents(row.get(f"{component} Tax Exclusive Promo Amount"))
    return gross_cents, tax_cents, net_cents


def _vat_rate(row: dict[str, Any]) -> float:
    try:
        return float(str(row.get("Tax Rate") or "0").strip() or 0)
    except ValueError:
        return 0.0


def _is_true(value: Any) -> bool:
    return _text(value).lower() in {"true", "1", "yes", "y"}


def _is_eu_country(country: str) -> bool:
    return country in EU_MEMBER_STATE_CODES and country != "DE"


EU_MEMBER_STATE_CODES = frozenset({
    "AT", "BE", "BG", "CY", "CZ", "DE", "DK", "EE", "ES", "FI", "FR", "GR", "HR",
    "HU", "IE", "IT", "LT", "LU", "LV", "MT", "NL", "PL", "PT", "RO", "SE", "SI", "SK",
})


def _home_rate_allowed(
    eu_tax_regime: str,
    eu_distance_prior_year_cents: int,
    eu_distance_current_year_cents: int,
) -> bool:
    """Heimsteuersatz nur bei bestaetigtem Regime UND Schwelle in BEIDEN Jahren."""
    if eu_tax_regime != EU_TAX_REGIME_HOME_RATE:
        return False
    threshold = EU_DISTANCE_SELLING_THRESHOLD_CENTS
    return (
        0 <= eu_distance_prior_year_cents < threshold
        and 0 <= eu_distance_current_year_cents < threshold
    )


def classify_amazon_tax_row(
    row: dict[str, Any],
    *,
    eu_tax_regime: str,
    eu_distance_prior_year_cents: int = 0,
    eu_distance_current_year_cents: int = 0,
) -> dict[str, Any]:
    """Klassifiziert eine Zeile OHNE Vererbung. RETURN/REFUND bleibt unverknuepft."""
    transaction_type = _text(row.get("Transaction Type")).upper()
    reason_code = _text(row.get("Tax Calculation Reason Code"))
    ship_from = _text(row.get("Ship From Country")).upper()
    ship_to = _text(row.get("Ship To Country")).upper()
    buyer_vat = _text(row.get("Buyer Tax Registration"))
    buyer_vat_type = _text(row.get("Buyer Tax Registration Type"))
    collection = _text(row.get("Tax Collection Responsibility"))
    is_correction = transaction_type in CORRECTION_TYPES

    gross_cents, tax_cents, net_cents = sum_components(row)
    booking_date = (
        _date_token(row.get("Shipment Date"))
        or ("" if is_correction else (
            _date_token(row.get("Tax Calculation Date")) or _date_token(row.get("Order Date"))))
    )

    result: dict[str, Any] = {
        "raw_row": row,
        "transaction_type": transaction_type,
        "order_id": _text(row.get("Order ID")),
        "shipment_id": _text(row.get("Shipment ID")),
        "seller_sku": _text(row.get("SKU")),
        "transaction_id": _text(row.get("Transaction ID")),
        "reason_code": reason_code,
        "ship_from_country": ship_from,
        "ship_to_country": ship_to,
        "buyer_vat_number": buyer_vat,
        "buyer_vat_type": buyer_vat_type,
        "seller_vat_number": _text(row.get("Seller Tax Registration")),
        "vat_rate": _vat_rate(row),
        "booking_date": booking_date,
        "gross_cents": gross_cents,
        "tax_cents": tax_cents,
        "net_cents": net_cents,
        "output_vat_cents": tax_cents,
        "net_source": "amazon_components",
        "tax_class": CLASS_UNRESOLVED,
        "original_tax_class": None,
        "is_domestic_b2b": False,
        "is_amazon_invoiced": _is_true(row.get("Is Amazon Invoiced")),
        "vat_invoice_number": _text(row.get("VAT Invoice Number")),
        "invoice_url": _text(row.get("Invoice Url")),
        "warnings": [],
        "blocker": False,
        "needs_inheritance": False,
    }
    if row.get("_source_match_conflict"):
        result.update(tax_class=CLASS_UNRESOLVED, output_vat_cents=0,
                      net_cents=gross_cents, blocker=True)
        result["warnings"].append("AMAZON_SOURCE_MATCH_AMBIGUOUS")
        return result

    # 1. RETURN/REFUND zuerst: Erben statt neu klassifizieren.
    if is_correction:
        result["tax_class"] = CLASS_RETURN_UNLINKED
        result["needs_inheritance"] = True
        result["blocker"] = True
        result["warnings"].append("AMAZON_RETURN_AWAITING_LINK")
        return result

    # 2. Amazon ist Steuerschuldner (Deemed Supplier).
    if collection.lower() == "amazon":
        result["tax_class"] = CLASS_DEEMED_SUPPLIER
        result["output_vat_cents"] = 0
        result["net_cents"] = gross_cents
        return result

    # 3. Ausfuhr ausserhalb der EU.
    if _is_true(row.get("Export Outside EU")) or (ship_to and ship_to not in EU_MEMBER_STATE_CODES):
        result["tax_class"] = CLASS_EXPORT
        result["output_vat_cents"] = 0
        result["net_cents"] = gross_cents
        return result

    # 4. Inlandsverkauf DE->DE mit 19 % (auch inlaendisches B2B bleibt 19 %).
    if (ship_from == "DE" and ship_to == "DE" and abs(result["vat_rate"] - 0.19) < 1e-9
        and not (row.get("_source_report_type") == "VAT_TRANSACTION"
                 and not _text((row.get("_avtr_raw") or {}).get("TOTAL_ACTIVITY_VALUE_VAT_AMT")))):
        result["tax_class"] = CLASS_DE_B2C
        result["is_domestic_b2b"] = bool(buyer_vat)
        return result

    # Before activation of VCS, AVTR still proves the customer's gross,
    # shipment and countries. Standard German goods remain taxable once the
    # seller is eligible; missing Amazon calculation is explicitly disclosed.
    avtr = row.get("_avtr_raw") or {}
    if (row.get("_source_report_type") == "VAT_TRANSACTION" and ship_from == ship_to == "DE"
        and collection.upper() == "SELLER" and not _text(avtr.get("TOTAL_ACTIVITY_VALUE_VAT_AMT"))
        and _text(avtr.get("PRODUCT_TAX_CODE")) in {"", "A_GEN_STANDARD"}
        and _text(row.get("Currency")) == "EUR"):
        net_home, vat_home = split_home_rate_vat(gross_cents)
        result.update(tax_class=CLASS_DE_B2C, net_source="computed_home_rate",
                      net_cents=net_home, output_vat_cents=vat_home, vat_rate=0.19)
        result["warnings"].append(WARNING_VAT_CALC_MISSING)
        return result

    # 5. Innergemeinschaftliche Lieferung DE->EU mit gueltiger fremder USt-IdNr.
    if (
        ship_from == "DE"
        and _is_eu_country(ship_to)
        and buyer_vat[:2].upper() in (EU_MEMBER_STATE_CODES - {"DE"}) | {"EL"}
        and len(buyer_vat.replace(" ", "")) > 4
        and buyer_vat_type.upper() == "VAT"
        and abs(result["vat_rate"]) < 1e-9
        and reason_code.lower() == "taxable"
    ):
        result["tax_class"] = CLASS_EU_B2B
        result["output_vat_cents"] = 0
        result["net_cents"] = gross_cents
        return result

    # 6./7. EU-B2C ohne USt-IdNr., Amazon hat keine Steuer berechnet.
    if (
        ship_from == "DE"
        and _is_eu_country(ship_to)
        and not buyer_vat
        and abs(result["vat_rate"]) < 1e-9
        and reason_code.lower() == "nontaxable"
    ):
        if _home_rate_allowed(
            eu_tax_regime, eu_distance_prior_year_cents, eu_distance_current_year_cents
        ):
            net_home, vat_home = split_home_rate_vat(gross_cents)
            result["tax_class"] = CLASS_EU_B2C_HOME_RATE
            result["net_source"] = "computed_home_rate"
            result["net_cents"] = net_home
            result["output_vat_cents"] = vat_home
            result["vat_rate"] = 0.19
            result["warnings"].append(WARNING_VAT_CALC_MISSING)
            return result
        result["tax_class"] = CLASS_UNRESOLVED
        result["output_vat_cents"] = 0
        result["net_cents"] = gross_cents
        result["blocker"] = True
        return result

    result["tax_class"] = CLASS_UNRESOLVED
    result["output_vat_cents"] = 0
    result["net_cents"] = gross_cents
    result["blocker"] = True
    return result


def _link_key(item: dict[str, Any]) -> tuple[str, str, str]:
    return (item["order_id"], item["shipment_id"], item["seller_sku"])


def link_and_inherit(
    rows: Iterable[dict[str, Any]],
    *,
    eu_tax_regime: str,
    eu_distance_prior_year_cents: int = 0,
    eu_distance_current_year_cents: int = 0,
) -> list[dict[str, Any]]:
    """Zweipass: SHIPMENTs klassifizieren, dann RETURN/REFUND vererben lassen."""
    classified = [
        classify_amazon_tax_row(
            row,
            eu_tax_regime=eu_tax_regime,
            eu_distance_prior_year_cents=eu_distance_prior_year_cents,
            eu_distance_current_year_cents=eu_distance_current_year_cents,
        )
        for row in rows
    ]
    shipments = list({_row_id(item["raw_row"]): item for item in classified
                      if item["transaction_type"] not in CORRECTION_TYPES}.values())
    exact: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    by_order_sku: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for item in shipments:
        exact.setdefault(_link_key(item), []).append(item)
        by_order_sku.setdefault((item["order_id"], item["seller_sku"]), []).append(item)

    for item in classified:
        if not item["needs_inheritance"]:
            continue
        exact_candidates = exact.get(_link_key(item), [])
        original = exact_candidates[0] if len(exact_candidates) == 1 else None
        if not exact_candidates:
            candidates = by_order_sku.get((item["order_id"], item["seller_sku"]), [])
            original = candidates[0] if len(candidates) == 1 else None
        if original is None:
            item["tax_class"] = CLASS_UNRESOLVED_RETURN_LINK
            item["blocker"] = True
            item["warnings"].append("AMAZON_RETURN_LINK_UNRESOLVED")
            continue

        inherited_class = original["tax_class"]
        item["original_tax_class"] = inherited_class
        item["tax_class"] = inherited_class
        item["vat_rate"] = original["vat_rate"]
        item["warnings"] = [w for w in item["warnings"] if w != "AMAZON_RETURN_AWAITING_LINK"]

        if inherited_class == CLASS_EU_B2C_HOME_RATE or (
            inherited_class == CLASS_DE_B2C and (
                original["net_source"] == "computed_home_rate" or (
                    item["raw_row"].get("_source_report_type") == "VAT_TRANSACTION"
                    and not _text((item["raw_row"].get("_avtr_raw") or {}).get("TOTAL_ACTIVITY_VALUE_VAT_AMT"))
                )
            )
        ):
            net_home, vat_home = split_home_rate_vat(item["gross_cents"])
            item["net_cents"] = net_home
            item["output_vat_cents"] = vat_home
            item["net_source"] = "computed_home_rate"
            if WARNING_VAT_CALC_MISSING not in item["warnings"]:
                item["warnings"].append(WARNING_VAT_CALC_MISSING)
            item["blocker"] = False
        elif inherited_class == CLASS_DE_B2C:
            # Amazons eigene Steuerkomponenten sind die Primaerquelle.
            item["output_vat_cents"] = item["tax_cents"]
            item["net_cents"] = item["gross_cents"] - item["tax_cents"]
            item["net_source"] = "amazon_components"
            item["blocker"] = False
        else:
            item["output_vat_cents"] = 0
            item["net_cents"] = item["gross_cents"]
            item["net_source"] = "amazon_components"
            item["blocker"] = original["blocker"]
    return classified


def parse_sc_vat_tax_report(text: str, *, delimiter: Optional[str] = None) -> list[dict[str, Any]]:
    body = text.lstrip("\ufeff")
    first_line = body.partition("\n")[0]
    if delimiter is None:
        delimiter = "\t" if "\t" in first_line else ","
    reader = csv.DictReader(io.StringIO(body), delimiter=delimiter)
    rows = [
        {str(key): str(value or "") for key, value in row.items() if key}
        for row in reader
    ]
    if not rows:
        raise ValueError("SC_VAT_TAX_REPORT enthaelt keine Datenzeilen")
    if {"TRANSACTION_EVENT_ID", "ACTIVITY_TRANSACTION_ID", "SELLER_SKU"}.issubset(rows[0]):
        normalized = []
        for raw in rows:
            kind = _text(raw.get("TRANSACTION_TYPE")).upper()
            if kind not in {"SALE", "RETURN", "REFUND"} or raw.get("SALES_CHANNEL") == "AMAZON_FEE":
                continue
            normalized.append({
                "Transaction ID": "AVTR:" + raw["ACTIVITY_TRANSACTION_ID"],
                "Order ID": raw["TRANSACTION_EVENT_ID"], "Shipment ID": raw["ACTIVITY_TRANSACTION_ID"],
                "SKU": raw["SELLER_SKU"], "Transaction Type": "SHIPMENT" if kind == "SALE" else kind,
                "Shipment Date": raw.get("TRANSACTION_DEPART_DATE") if kind == "SALE" else raw.get("TRANSACTION_COMPLETE_DATE"),
                "Order Date": "", "Tax Calculation Date": raw.get("TAX_CALCULATION_DATE", ""),
                "Ship From Country": raw.get("SALE_DEPART_COUNTRY") or raw.get("DEPARTURE_COUNTRY", ""),
                "Ship To Country": raw.get("SALE_ARRIVAL_COUNTRY") or raw.get("ARRIVAL_COUNTRY", ""),
                "Tax Rate": raw.get("PRICE_OF_ITEMS_VAT_RATE_PERCENT", ""),
                "Tax Calculation Reason Code": "unconfirmed",
                "Tax Collection Responsibility": raw.get("TAX_COLLECTION_RESPONSIBILITY", ""),
                "Buyer Tax Registration": raw.get("BUYER_VAT_NUMBER", ""), "Buyer Tax Registration Type": "VAT" if raw.get("BUYER_VAT_NUMBER") else "",
                "Export Outside EU": raw.get("EXPORT_OUTSIDE_EU", ""),
                "OUR_PRICE Tax Inclusive Selling Price": raw.get("TOTAL_ACTIVITY_VALUE_AMT_VAT_INCL", ""),
                "OUR_PRICE Tax Exclusive Selling Price": raw.get("TOTAL_ACTIVITY_VALUE_AMT_VAT_EXCL", ""),
                "OUR_PRICE Tax Amount": raw.get("TOTAL_ACTIVITY_VALUE_VAT_AMT", ""),
                "Currency": raw.get("TRANSACTION_CURRENCY_CODE", ""),
                "VAT Invoice Number": raw.get("VAT_INV_NUMBER", ""), "Invoice Url": raw.get("INVOICE_URL", ""),
                "_source_report_type": "VAT_TRANSACTION", "_avtr_raw": raw,
            })
        if not normalized:
            raise ValueError("VAT_TRANSACTION enthaelt keine Verkaeufe oder Erstattungen")
        return normalized
    missing = [name for name in REQUIRED_COLUMNS if name not in rows[0]]
    if missing:
        raise ValueError(
            "SC_VAT_TAX_REPORT mit Transaction ID, Order ID und SKU erforderlich"
        )
    return rows


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _row_id(raw_row: dict[str, Any]) -> str:
    identity = [_text(raw_row.get(key)) for key in
                ("Transaction ID", "Order ID", "Shipment ID", "SKU", "Transaction Type")]
    # Only unidentified legacy rows need a content-derived fallback.
    raw = json.dumps(identity if identity[0] else raw_row, ensure_ascii=True, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def import_sc_vat_tax_rows(classified_rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Persistiert klassifizierte Zeilen idempotent nach amazon_tax_rows.

    `amazon_tax_rows` wird von ust_schema in der Combined-DB angelegt und von
    ust_report.load_amazon_tax_rows dort gelesen -- also hier auch in die
    Combined-DB schreiben, nicht in die Amazon-FBA-DB.
    """
    from app.db import connect_combined_db

    inserted = 0
    skipped = 0
    classified_rows = list(classified_rows)
    incoming_groups: dict[tuple, set[str]] = {}
    for item in classified_rows:
        key = (item["order_id"], item["seller_sku"], item["transaction_type"], item["booking_date"],
               (item.get("raw_row") or {}).get("_source_report_type") == "VAT_TRANSACTION")
        incoming_groups.setdefault(key, set()).add(_row_id(item.get("raw_row") or {}))
    with connect_combined_db() as connection:
        for item in classified_rows:
            raw_row = item.get("raw_row") or {}
            row_id = _row_id(raw_row)
            counterparts = connection.execute(
                "SELECT id,raw_json FROM amazon_tax_rows WHERE order_id=? AND seller_sku=? "
                "AND transaction_type=? AND booking_date=?",
                (item["order_id"], item["seller_sku"], item["transaction_type"], item["booking_date"]),
            ).fetchall()
            supplemental = raw_row.get("_source_report_type") == "VAT_TRANSACTION"
            opposite = [r for r in counterparts if (json.loads(r["raw_json"]).get("_source_report_type") == "VAT_TRANSACTION") != supplemental]
            key = (item["order_id"], item["seller_sku"], item["transaction_type"], item["booking_date"], supplemental)
            unambiguous = len(opposite) == 1 and len(incoming_groups[key]) == 1
            if opposite and not unambiguous:
                raw_row = {**raw_row, "_source_match_conflict": True}
                item = {**item, "tax_class": CLASS_UNRESOLVED, "output_vat_cents": 0,
                        "net_cents": item["gross_cents"]}
            elif opposite and supplemental:
                skipped += 1
                continue
            existing = connection.execute(
                "SELECT id FROM amazon_tax_rows WHERE transaction_id=? AND order_id=? "
                "AND shipment_id=? AND seller_sku=? AND transaction_type=?",
                (item["transaction_id"], item["order_id"], item["shipment_id"], item["seller_sku"], item["transaction_type"]),
            ).fetchall() if item["transaction_id"] else []
            if not supplemental and unambiguous:
                existing = [*existing, *opposite]
            if existing:
                # Also collapse historical content-hash IDs on first reimport.
                connection.executemany("DELETE FROM amazon_tax_rows WHERE id=?", [(r["id"],) for r in existing])
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO amazon_tax_rows(
                    id, transaction_id, order_id, shipment_id, seller_sku, transaction_type,
                    tax_class, original_tax_class, tax_calculation_reason_code,
                    ship_from_country, ship_to_country, buyer_vat_number, buyer_vat_type,
                    seller_vat_number, vat_rate, booking_date, gross_cents, net_cents,
                    output_vat_cents, net_source, is_amazon_invoiced, vat_invoice_number,
                    invoice_url, raw_json, imported_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    row_id,
                    item["transaction_id"],
                    item["order_id"],
                    item["shipment_id"],
                    item["seller_sku"],
                    item["transaction_type"],
                    item["tax_class"],
                    item["original_tax_class"],
                    item["reason_code"],
                    item["ship_from_country"],
                    item["ship_to_country"],
                    item["buyer_vat_number"],
                    item["buyer_vat_type"],
                    item["seller_vat_number"],
                    item["vat_rate"],
                    item["booking_date"],
                    item["gross_cents"],
                    item["net_cents"],
                    item["output_vat_cents"],
                    item["net_source"],
                    1 if item["is_amazon_invoiced"] else 0,
                    item["vat_invoice_number"],
                    item["invoice_url"],
                    json.dumps(raw_row, ensure_ascii=True, sort_keys=True, default=str),
                    _utc_now(),
                ),
            )
            if cursor.rowcount and not existing:
                inserted += 1
            else:
                skipped += 1
        connection.commit()
    return {"inserted": inserted, "skipped": skipped, "total": inserted + skipped}


def request_sc_vat_tax_report(*, month: Optional[str] = None) -> dict[str, Any]:
    """Fordert SC_VAT_TAX_REPORT ueber die SP-API an (restricted, erfordert RDT)."""
    from app.services.importers import amazon_sp_api as db

    config, missing = db.load_amazon_sp_api_config()
    if config is None:
        return {"status": "skipped", "missing": missing}
    client = db.AmazonSpApiClient(config)
    db.init_amazon_fba_db()
    with db._connect() as connection:
        marketplace_ids = [
            str(row[0])
            for row in connection.execute(
                "SELECT marketplace_id FROM amazon_marketplaces ORDER BY marketplace_id"
            ).fetchall()
        ]
    if not marketplace_ids:
        return {"status": "skipped", "reason": "run an Amazon sync before requesting a report"}
    data_start_time = None
    data_end_time = None
    if month:
        start = datetime.strptime(month[:7], "%Y-%m")
        end = datetime(start.year + (start.month == 12), start.month % 12 + 1, 1)
        data_start_time = start.strftime("%Y-%m-%dT00:00:00Z")
        data_end_time = end.strftime("%Y-%m-%dT00:00:00Z")
    report_id = client.create_report(
        "SC_VAT_TAX_REPORT", marketplace_ids,
        data_start_time=data_start_time, data_end_time=data_end_time,
    )
    return {"status": "requested", "report_id": report_id, "report_type": "SC_VAT_TAX_REPORT"}


def import_report_document(report_id: str, *, report_type: str = "SC_VAT_TAX_REPORT") -> dict[str, Any]:
    """Laedt ein SC_VAT_TAX_REPORT-Dokument, klassifiziert und persistiert es."""
    from app.services.importers import amazon_sp_api as db
    from app.services import ust_report as _ust

    config, missing = db.load_amazon_sp_api_config()
    if config is None:
        return {"status": "skipped", "missing": missing}
    client = db.AmazonSpApiClient(config)
    report = client.get_report(report_id)
    status = _text(report.get("processingStatus"))
    document_id = _text(report.get("reportDocumentId"))
    if status not in {"DONE", "_DONE_"} or not document_id:
        return {"status": "pending", "report_id": report_id, "processing_status": status}

    rdt = None
    if db.report_requires_rdt(report_type):
        # Restricted Operations: das RDT ersetzt den LWA Token in x-amz-access-token.
        rdt = client.create_restricted_data_token([
            {"method": "GET", "path": f"/reports/2021-06-30/documents/{document_id}"}
        ])
    document = client.get_report_document(document_id, access_token=rdt)
    document_url = _text(document.get("url"))
    if not document_url:
        raise AmazonTaxImportError(502, "report document did not include a download URL")
    body = client.download_report_text(document_url, access_token=rdt)
    rows = parse_sc_vat_tax_report(body)
    eu_settings = _ust.get_eu_tax_settings()
    from app.db import connect_combined_db
    with connect_combined_db() as connection:
        originals = [json.loads(r["raw_json"]) for r in connection.execute(
            "SELECT raw_json FROM amazon_tax_rows WHERE transaction_type NOT IN ('RETURN','REFUND')")]
    incoming_ids = {_row_id(r) for r in rows}
    classified = [item for item in link_and_inherit(
        [*originals, *rows],
        eu_tax_regime=eu_settings["eu_tax_regime"],
        eu_distance_prior_year_cents=eu_settings["eu_distance_prior_year_cents"],
        eu_distance_current_year_cents=eu_settings["eu_distance_current_year_cents"],
    ) if _row_id(item["raw_row"]) in incoming_ids]
    persisted = import_sc_vat_tax_rows(classified)
    reclassify_all_rows(**eu_settings)
    return {
        "status": "imported",
        "report_id": report_id,
        "report_type": report_type,
        **persisted,
        "blockers": sum(1 for item in classified if item["blocker"]),
        "warnings": sum(len(item["warnings"]) for item in classified),
    }


def reclassify_all_rows(
    *,
    eu_tax_regime: str,
    eu_distance_prior_year_cents: int = 0,
    eu_distance_current_year_cents: int = 0,
) -> dict[str, Any]:
    """Ordnet alle gespeicherten Zeilen mit den aktuellen Einstellungen neu ein.

    Die Steuerklasse ist abhaengig von der EU-Verkaufsregel. Nachdem die Regel
    bestaetigt wurde (z. B. Fernabsatzgrenze), muessen bereits importierte
    Zeilen neu zugeordnet werden -- sonst bleibt 'unresolved' stehen, obwohl
    die Frage geklaert ist. Die Rohdaten bleiben unangetastet.
    """
    from app.db import connect_combined_db

    with connect_combined_db() as connection:
        stored = connection.execute(
            "SELECT id, raw_json FROM amazon_tax_rows ORDER BY id"
        ).fetchall()
    if not stored:
        return {"updated": 0, "total": 0}

    raw_rows = [json.loads(row["raw_json"]) for row in stored]
    classified = link_and_inherit(
        raw_rows,
        eu_tax_regime=eu_tax_regime,
        eu_distance_prior_year_cents=eu_distance_prior_year_cents,
        eu_distance_current_year_cents=eu_distance_current_year_cents,
    )
    by_id = {_row_id(item["raw_row"]): item for item in classified}

    updated = 0
    with connect_combined_db() as connection:
        for row in stored:
            item = by_id.get(_row_id(json.loads(row["raw_json"])))
            if item is None:
                continue
            connection.execute(
                """
                UPDATE amazon_tax_rows SET
                    tax_class = ?, original_tax_class = ?, net_source = ?, vat_rate = ?,
                    gross_cents = ?, net_cents = ?, output_vat_cents = ?, booking_date = ?
                WHERE id = ?
                """,
                (
                    item["tax_class"],
                    item["original_tax_class"],
                    item["net_source"],
                    item["vat_rate"],
                    item["gross_cents"],
                    item["net_cents"],
                    item["output_vat_cents"],
                    item["booking_date"],
                    row["id"],
                ),
            )
            updated += 1
    return {"updated": updated, "total": len(stored)}
