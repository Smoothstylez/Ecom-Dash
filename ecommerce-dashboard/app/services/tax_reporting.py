from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from app.config import BOOKKEEPING_DB_PATH
from app.db import connect_combined_db
from app.services.bookkeeping_full import _ensure_schema
from app.services.order_summaries import parse_iso


DEFAULT_PROFILE_ID = "default"
VAT_THRESHOLD_CENTS = 100_000 * 100


@dataclass(frozen=True)
class TaxSettings:
    tax_mode: str
    vat_effective_from: Optional[datetime]


def _normalize_tax_mode(value: Any) -> str:
    return "regular" if str(value or "").strip().lower() == "regular" else "small_business"


def get_tax_settings() -> TaxSettings:
    with connect_combined_db() as connection:
        row = connection.execute(
            "SELECT tax_mode, vat_effective_from FROM seller_profiles WHERE id = ? LIMIT 1",
            (DEFAULT_PROFILE_ID,),
        ).fetchone()
    if row is None:
        return TaxSettings(tax_mode="small_business", vat_effective_from=None)
    return TaxSettings(
        tax_mode=_normalize_tax_mode(row["tax_mode"]),
        vat_effective_from=parse_iso(row["vat_effective_from"]),
    )


def is_vat_applicable_for_order(order_date: Any, settings: TaxSettings) -> bool:
    if settings.tax_mode != "regular" or settings.vat_effective_from is None:
        return False
    parsed_order_date = parse_iso(order_date)
    if parsed_order_date is None:
        return False
    return parsed_order_date >= settings.vat_effective_from


def annotate_order_tax_fields(order: dict[str, Any], settings: TaxSettings) -> dict[str, Any]:
    sales_gross_cents = max(int(order.get("sales_gross_cents") or order.get("total_cents") or 0), 0)
    sales_vat_cents = max(int(order.get("sales_vat_cents") or 0), 0)
    sales_net_cents = max(int(order.get("sales_net_cents") or (sales_gross_cents - sales_vat_cents)), 0)
    purchase_cost_cents = max(int(order.get("purchase_cost_cents") or 0), 0)
    purchase_vat_cents = max(int(order.get("purchase_vat_cents") or 0), 0)
    purchase_is_vat_deductible = bool(order.get("purchase_is_vat_deductible"))

    vat_applicable = is_vat_applicable_for_order(order.get("order_date"), settings)
    deductible_purchase_vat_cents = purchase_vat_cents if vat_applicable and purchase_is_vat_deductible else 0
    purchase_effective_cost_cents = purchase_cost_cents - deductible_purchase_vat_cents
    if purchase_effective_cost_cents < 0:
        purchase_effective_cost_cents = 0

    order.update(
        {
            "sales_gross_cents": sales_gross_cents,
            "sales_vat_cents": sales_vat_cents,
            "sales_net_cents": sales_net_cents,
            "vat_applicable": vat_applicable,
            "output_vat_cents": sales_vat_cents if vat_applicable else 0,
            "purchase_vat_cents": purchase_vat_cents,
            "purchase_is_vat_deductible": purchase_is_vat_deductible,
            "deductible_purchase_vat_cents": deductible_purchase_vat_cents,
            "purchase_effective_cost_cents": purchase_effective_cost_cents,
            "vat_due_before_fee_invoices_cents": (sales_vat_cents if vat_applicable else 0) - deductible_purchase_vat_cents,
        }
    )
    return order


def _month_bounds(month: str) -> tuple[datetime, datetime]:
    token = str(month or "").strip()
    try:
        start = datetime.strptime(token, "%Y-%m").replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise ValueError("month must be YYYY-MM") from exc
    if start.month == 12:
        end = start.replace(year=start.year + 1, month=1)
    else:
        end = start.replace(month=start.month + 1)
    return start, end


def _load_monthly_fee_vat(month: str) -> list[dict[str, Any]]:
    if not BOOKKEEPING_DB_PATH.exists():
        return []
    month_start, month_end = _month_bounds(month)
    with sqlite3.connect(BOOKKEEPING_DB_PATH) as connection:
        connection.row_factory = sqlite3.Row
        if _ensure_schema(connection):
            connection.commit()
        rows = connection.execute(
            """
            SELECT id, provider, period_from, period_to, invoice_amount_cents, vat_amount_cents, status, notes
            FROM monthly_invoices
            WHERE period_from < ? AND period_to >= ?
            ORDER BY period_from ASC, provider ASC
            """,
            (
                month_end.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                month_start.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            ),
        ).fetchall()
    return [dict(row) for row in rows]


def _load_manual_input_vat(month: str) -> list[dict[str, Any]]:
    if not BOOKKEEPING_DB_PATH.exists():
        return []
    month_start, month_end = _month_bounds(month)
    with sqlite3.connect(BOOKKEEPING_DB_PATH) as connection:
        connection.row_factory = sqlite3.Row
        if _ensure_schema(connection):
            connection.commit()
        rows = connection.execute(
            """
            SELECT id, date, type, provider, counterparty_name, amount_gross, vat_amount, reference, notes
            FROM transactions
            WHERE direction = 'OUT'
              AND COALESCE(is_vat_deductible, 0) = 1
              AND COALESCE(vat_amount, 0) > 0
              AND date >= ?
              AND date < ?
            ORDER BY date ASC, id ASC
            """,
            (
                month_start.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                month_end.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            ),
        ).fetchall()
    return [dict(row) for row in rows]


RETURN_LIKE_STATUS_KEYWORDS = (
    "cancel",
    "cancelled",
    "canceled",
    "void",
    "return",
    "returned",
    "refund",
    "refunded",
    "rma",
    "revoked",
    "returning",
)


def is_fully_return_like_order(order: dict[str, Any]) -> bool:
    """True when an order is economically void (fully refunded/cancelled/voided).

    Mirrors the semantics of bookings._normalized_sync_amounts and
    analytics._normalize_order_metrics so that threshold and VAT reporting stay
    consistent with those views. Partially refunded orders are NOT return-like
    here; their reduced amounts already flow through the summaries.
    """
    tokens = [
        str(order.get(field) or "").strip().lower()
        for field in ("financial_status", "raw_status", "fulfillment_status")
    ]
    if any("partial" in token for token in tokens):
        return False
    return any(
        keyword in token
        for token in tokens
        if token
        for keyword in RETURN_LIKE_STATUS_KEYWORDS
    )


def build_threshold_candidate(orders: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Find the first order whose IN-YEAR cumulative gross crosses the VAT threshold.

    The §19 UStG threshold applies per calendar year, so accumulation resets on
    January 1st instead of carrying previous-year revenue forward.
    """
    ordered = sorted(
        orders,
        key=lambda item: (
            str(item.get("order_date") or ""),
            str(item.get("marketplace") or ""),
            str(item.get("order_id") or ""),
        ),
    )
    yearly_cumulative: dict[int, int] = {}
    for order in ordered:
        if is_fully_return_like_order(order):
            continue
        parsed_order_date = parse_iso(order.get("order_date"))
        year = parsed_order_date.year if parsed_order_date is not None else 0
        gross_cents = max(int(order.get("sales_gross_cents") or order.get("total_cents") or 0), 0)
        yearly_cumulative[year] = yearly_cumulative.get(year, 0) + gross_cents
        if yearly_cumulative[year] >= VAT_THRESHOLD_CENTS:
            return {
                "year": year,
                "marketplace": order.get("marketplace"),
                "order_id": order.get("order_id"),
                "external_order_id": order.get("external_order_id"),
                "order_date": order.get("order_date"),
                "sales_gross_cents": gross_cents,
                "cumulative_gross_cents": yearly_cumulative[year],
                "threshold_cents": VAT_THRESHOLD_CENTS,
            }
    return None


def build_vat_report(*, month: str, orders: list[dict[str, Any]]) -> dict[str, Any]:
    settings = get_tax_settings()
    month_start, month_end = _month_bounds(month)
    annotated_orders = [annotate_order_tax_fields(dict(order), settings) for order in orders]

    included_orders: list[dict[str, Any]] = []
    output_vat_total_cents = 0
    deductible_purchase_vat_total_cents = 0
    warnings: list[str] = []

    for order in annotated_orders:
        order_dt = parse_iso(order.get("order_date"))
        if order_dt is None or order_dt < month_start or order_dt >= month_end:
            continue
        if not bool(order.get("vat_applicable")):
            continue
        if is_fully_return_like_order(order):
            warnings.append(
                f"Order {order.get('external_order_id') or order.get('order_id')} ist voll erstattet/storniert und wird nicht als Bemessungsgrundlage gezählt."
            )
            continue
        if any(
            token in str(order.get(field) or "").strip().lower()
            for field in ("financial_status", "raw_status", "fulfillment_status")
            for token in ("refund", "cancel", "return")
        ):
            warnings.append(
                f"Order {order.get('external_order_id') or order.get('order_id')} enthaelt Refund/Cancel-Status und sollte steuerlich geprueft werden."
            )
        output_vat = max(int(order.get("output_vat_cents") or 0), 0)
        deductible_purchase_vat = max(int(order.get("deductible_purchase_vat_cents") or 0), 0)
        output_vat_total_cents += output_vat
        deductible_purchase_vat_total_cents += deductible_purchase_vat
        included_orders.append(
            {
                "marketplace": order.get("marketplace"),
                "order_id": order.get("order_id"),
                "external_order_id": order.get("external_order_id"),
                "order_date": order.get("order_date"),
                "customer": order.get("customer"),
                "sales_gross_cents": int(order.get("sales_gross_cents") or 0),
                "sales_net_cents": int(order.get("sales_net_cents") or 0),
                "sales_vat_cents": output_vat,
                "purchase_cost_cents": int(order.get("purchase_cost_cents") or 0),
                "purchase_vat_cents": int(order.get("purchase_vat_cents") or 0),
                "deductible_purchase_vat_cents": deductible_purchase_vat,
            }
        )

    monthly_fee_invoices = _load_monthly_fee_vat(month)
    monthly_fee_vat_total_cents = sum(max(int(item.get("vat_amount_cents") or 0), 0) for item in monthly_fee_invoices)

    manual_input_vat_rows = _load_manual_input_vat(month)
    manual_input_vat_total_cents = sum(max(int(item.get("vat_amount") or 0), 0) for item in manual_input_vat_rows)

    deductible_input_vat_total_cents = (
        deductible_purchase_vat_total_cents + monthly_fee_vat_total_cents + manual_input_vat_total_cents
    )

    if settings.tax_mode != "regular":
        warnings.append("Verkaeuferprofil steht nicht auf Regelbesteuerung.")
    if settings.vat_effective_from is None:
        warnings.append("Kein manueller USt-Startzeitpunkt gesetzt. Es werden keine Orders als USt-pflichtig behandelt.")

    return {
        "month": month,
        "settings": {
            "tax_mode": settings.tax_mode,
            "vat_effective_from": settings.vat_effective_from.replace(microsecond=0).isoformat().replace("+00:00", "Z")
            if settings.vat_effective_from is not None
            else None,
        },
        "threshold_candidate": build_threshold_candidate(annotated_orders),
        "orders": included_orders,
        "monthly_fee_invoices": [
            {
                "id": row.get("id"),
                "provider": row.get("provider"),
                "period_from": row.get("period_from"),
                "period_to": row.get("period_to"),
                "invoice_amount_cents": int(row.get("invoice_amount_cents") or 0),
                "vat_amount_cents": int(row.get("vat_amount_cents") or 0),
                "status": row.get("status"),
                "notes": row.get("notes"),
            }
            for row in monthly_fee_invoices
        ],
        "manual_input_vat_transactions": [
            {
                "id": row.get("id"),
                "date": row.get("date"),
                "type": row.get("type"),
                "provider": row.get("provider"),
                "counterparty_name": row.get("counterparty_name"),
                "amount_gross": int(row.get("amount_gross") or 0),
                "vat_amount": int(row.get("vat_amount") or 0),
                "reference": row.get("reference"),
                "notes": row.get("notes"),
            }
            for row in manual_input_vat_rows
        ],
        "totals": {
            "output_vat_total_cents": output_vat_total_cents,
            "deductible_purchase_vat_total_cents": deductible_purchase_vat_total_cents,
            "monthly_fee_vat_total_cents": monthly_fee_vat_total_cents,
            "manual_input_vat_total_cents": manual_input_vat_total_cents,
            "deductible_input_vat_total_cents": deductible_input_vat_total_cents,
            "vat_payable_total_cents": output_vat_total_cents - deductible_input_vat_total_cents,
        },
        "warnings": warnings,
    }
