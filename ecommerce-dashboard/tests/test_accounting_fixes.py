from __future__ import annotations

import sqlite3
from typing import Any

import pytest


class _RowShim:
    """Minimal sqlite3.Row stand-in for summary functions (key access only)."""

    def __init__(self, data: dict[str, Any]):
        self._data = data

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def keys(self):  # pragma: no cover - not used by summaries
        return list(self._data.keys())


def _shopify_row(**overrides: Any) -> _RowShim:
    base: dict[str, Any] = {
        "id": "1001",
        "name": "#1001",
        "raw_json": "{}",
        "financial_status": "paid",
        "fulfillment_status": "fulfilled",
        "total_price": "119.90",
        "total_tax": "0.00",
        "refund_amount_sum": 0,
        "fee_total": None,
        "net_total": None,
        "estimated_paypal_fee": None,
        "estimated_net_after_fee": None,
        "customer_first_name": "Max",
        "customer_last_name": "Muster",
        "customer_email": "",
        "email": "",
        "first_article": "Widget",
        "payment_method": "Shopify Payments",
        "currency": "EUR",
        "created_at": "2026-06-30T12:00:00Z",
        "line_items_count": 1,
    }
    base.update(overrides)
    return _RowShim(base)


def _kaufland_row(**overrides: Any) -> _RowShim:
    base: dict[str, Any] = {
        "id_order": "M16YXQ5",
        "raw_json": "{}",
        "units_price_sum": 8890.0,
        "revenue_gross_sum": 7509.0,
        "shipping_sum": 0.0,
        # Non-integer REAL aggregate in CENTS: 8890 * 19 / 119 = 1419.4117...
        # Regression for the x100 VAT inflation bug.
        "units_vat_sum": 1419.4117647058824,
        "shipping_vat_sum": 0.0,
        "refund_amount_sum": 0.0,
        "customer_name": "Max Muster",
        "first_article": "Widget",
        "ts_created_iso": "2026-06-30T12:00:00Z",
        "line_items_count": 1,
        "unit_status": "received",
    }
    base.update(overrides)
    return _RowShim(base)


# ── B1: Kaufland VAT aggregates are cents, never inflated ────────────────────


def test_kaufland_summary_vat_not_inflated_on_noninteger_aggregate() -> None:
    from app.services.order_summaries import kaufland_summary_from_row, to_kaufland_cents

    # The heuristic itself must stay ambiguous-proof for this input:
    assert to_kaufland_cents(1419.4117647058824) == 141941  # documents old trap

    summary = kaufland_summary_from_row(_kaufland_row())
    assert summary["sales_vat_cents"] == 1419
    assert summary["sales_gross_cents"] == 8890
    assert summary["sales_net_cents"] == 8890 - 1419


def test_kaufland_summary_integer_aggregate_unchanged() -> None:
    from app.services.order_summaries import kaufland_summary_from_row

    summary = kaufland_summary_from_row(_kaufland_row(units_vat_sum=3990.0))
    assert summary["sales_vat_cents"] == 3990


def test_kaufland_summary_refunds_reduce_gross_and_vat_proportionally() -> None:
    from app.services.order_summaries import kaufland_summary_from_row

    summary = kaufland_summary_from_row(
        _kaufland_row(refund_amount_sum=4000.0)
    )
    # gross_sales 8890 - refund 4000 = 4890; vat scaled 1419.41 * 4890/8890 = 780.72 -> 781
    assert summary["sales_gross_cents"] == 4890
    assert summary["sales_vat_cents"] == 781


# ── B2: fully refunded/voided shopify orders carry zero money ────────────────


@pytest.mark.parametrize("status", ["refunded", "voided", "cancelled"])
def test_shopify_fully_returned_orders_are_zeroed(status: str) -> None:
    from app.services.order_summaries import shopify_summary_from_row

    summary = shopify_summary_from_row(
        _shopify_row(financial_status=status, refund_amount_sum=119.90)
    )
    assert summary["total_cents"] == 0
    assert summary["sales_gross_cents"] == 0
    assert summary["sales_vat_cents"] == 0
    assert summary["sales_net_cents"] == 0
    assert summary["fees_cents"] == 0
    assert summary["after_fees_cents"] == 0
    # status stays visible for the UI
    assert summary["financial_status"] == status


def test_shopify_partially_refunded_keeps_reduced_amount() -> None:
    from app.services.order_summaries import shopify_summary_from_row

    summary = shopify_summary_from_row(
        _shopify_row(financial_status="partially_refunded", refund_amount_sum=20.0)
    )
    assert summary["total_cents"] == 9990
    assert summary["sales_gross_cents"] == 9990


def test_shopify_paid_order_untouched() -> None:
    from app.services.order_summaries import shopify_summary_from_row

    summary = shopify_summary_from_row(_shopify_row())
    assert summary["total_cents"] == 11990
    assert summary["sales_gross_cents"] == 11990


# ── B2 guard: threshold + VAT report exclude void orders themselves ─────────


def test_threshold_candidate_skips_return_like_orders() -> None:
    from app.services.tax_reporting import build_threshold_candidate

    orders = [
        {"order_date": "2026-06-01T00:00:00Z", "marketplace": "shopify", "order_id": "1",
         "sales_gross_cents": 60_000_00, "financial_status": "paid"},
        # refunded order would push over the threshold if counted
        {"order_date": "2026-06-02T00:00:00Z", "marketplace": "shopify", "order_id": "2",
         "sales_gross_cents": 50_000_00, "financial_status": "refunded"},
        {"order_date": "2026-06-03T00:00:00Z", "marketplace": "kaufland", "order_id": "3",
         "sales_gross_cents": 45_000_00, "financial_status": ""},
    ]
    candidate = build_threshold_candidate(orders)
    assert candidate is not None
    assert candidate["order_id"] == "3"
    assert candidate["cumulative_gross_cents"] == 105_000_00


def test_threshold_candidate_counts_partial_refund_reduced_amount() -> None:
    from app.services.tax_reporting import build_threshold_candidate

    orders = [
        {"order_date": "2026-06-01T00:00:00Z", "marketplace": "shopify", "order_id": "1",
         "sales_gross_cents": 99_000_00, "financial_status": "partially_refunded"},
        {"order_date": "2026-06-02T00:00:00Z", "marketplace": "kaufland", "order_id": "2",
         "sales_gross_cents": 2_000_00, "financial_status": ""},
    ]
    candidate = build_threshold_candidate(orders)
    assert candidate is not None
    assert candidate["order_id"] == "2"
    assert candidate["cumulative_gross_cents"] == 101_000_00


def test_threshold_candidate_resets_at_year_boundary() -> None:
    from app.services.tax_reporting import build_threshold_candidate

    orders = [
        # previous year revenue must NOT carry into the current year
        {"order_date": "2025-12-15T00:00:00Z", "marketplace": "shopify", "order_id": "1",
         "sales_gross_cents": 90_000_00, "financial_status": "paid"},
        {"order_date": "2026-01-05T00:00:00Z", "marketplace": "kaufland", "order_id": "2",
         "sales_gross_cents": 60_000_00, "financial_status": ""},
        {"order_date": "2026-01-06T00:00:00Z", "marketplace": "kaufland", "order_id": "3",
         "sales_gross_cents": 45_000_00, "financial_status": ""},
    ]
    candidate = build_threshold_candidate(orders)
    assert candidate is not None
    assert candidate["year"] == 2026
    assert candidate["order_id"] == "3"
    assert candidate["cumulative_gross_cents"] == 105_000_00


def test_is_fully_return_like_order_semantics() -> None:
    from app.services.tax_reporting import is_fully_return_like_order

    assert is_fully_return_like_order({"financial_status": "refunded"}) is True
    assert is_fully_return_like_order({"fulfillment_status": "cancelled"}) is True
    assert is_fully_return_like_order({"raw_status": "voided"}) is True
    assert is_fully_return_like_order({"financial_status": "partially_refunded"}) is False
    assert is_fully_return_like_order({"financial_status": "paid"}) is False
    assert is_fully_return_like_order({}) is False
