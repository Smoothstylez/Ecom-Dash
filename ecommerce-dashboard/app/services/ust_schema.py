"""Schema fuer den monatlichen USt-Report: Eingangsrechnungen, Amazon-Steuerzeilen,
Kaufland-Ratenkorrekturen und Report-Snapshots.

Ausschliesslich additive Migrationen (CREATE TABLE IF NOT EXISTS / ALTER TABLE),
passend zum Muster in app.db und app.services.amazon_procurement.
"""
from __future__ import annotations

import sqlite3

from app.db import _ensure_column

UST_TABLES: set[str] = {
    "input_vat_invoices",
    "input_vat_invoice_lines",
    "amazon_tax_rows",
    "kaufland_tax_overrides",
    "ust_reports",
}

EU_TAX_REGIME_UNCONFIRMED = "unconfirmed"
EU_TAX_REGIME_HOME_RATE = "home_rate_under_threshold"
EU_TAX_REGIME_OSS = "oss_destination"
EU_TAX_REGIMES: frozenset[str] = frozenset(
    {EU_TAX_REGIME_UNCONFIRMED, EU_TAX_REGIME_HOME_RATE, EU_TAX_REGIME_OSS}
)

UST_SCHEMA = """
CREATE TABLE IF NOT EXISTS input_vat_invoices(
 id TEXT PRIMARY KEY,
 provider TEXT NOT NULL,
 doc_type TEXT NOT NULL,
 invoice_number TEXT NOT NULL,
 invoice_date TEXT NOT NULL,
 received_date TEXT NOT NULL DEFAULT '',
 service_date TEXT NOT NULL DEFAULT '',
 period_from TEXT NOT NULL DEFAULT '',
 period_to TEXT NOT NULL DEFAULT '',
 currency TEXT NOT NULL DEFAULT 'EUR',
 gross_cents INTEGER NOT NULL,
 net_cents INTEGER NOT NULL,
 vat_cents INTEGER NOT NULL,
 deductible_vat_cents INTEGER NOT NULL DEFAULT 0,
 input_vat_status TEXT NOT NULL DEFAULT 'review_required',
 deduction_month TEXT NOT NULL,
 source TEXT NOT NULL DEFAULT 'manual',
 document_path TEXT NOT NULL DEFAULT '',
 sha256 TEXT NOT NULL DEFAULT '',
 notes TEXT NOT NULL DEFAULT '',
 created_at TEXT NOT NULL,
 UNIQUE(provider, invoice_number));
CREATE TABLE IF NOT EXISTS input_vat_invoice_lines(
 id TEXT PRIMARY KEY,
 invoice_id TEXT NOT NULL REFERENCES input_vat_invoices(id) ON DELETE CASCADE,
 label TEXT NOT NULL,
 category TEXT NOT NULL,
 gross_cents INTEGER NOT NULL,
 net_cents INTEGER NOT NULL,
 vat_cents INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS amazon_tax_rows(
 id TEXT PRIMARY KEY,
 transaction_id TEXT NOT NULL,
 order_id TEXT NOT NULL,
 shipment_id TEXT NOT NULL DEFAULT '',
 seller_sku TEXT NOT NULL,
 transaction_type TEXT NOT NULL,
 tax_class TEXT NOT NULL,
 original_tax_class TEXT,
 tax_calculation_reason_code TEXT NOT NULL DEFAULT '',
 ship_from_country TEXT,
 ship_to_country TEXT,
 buyer_vat_number TEXT,
 buyer_vat_type TEXT,
 seller_vat_number TEXT,
 vat_rate REAL NOT NULL DEFAULT 0,
 booking_date TEXT NOT NULL,
 gross_cents INTEGER NOT NULL,
 net_cents INTEGER NOT NULL,
 output_vat_cents INTEGER NOT NULL,
 net_source TEXT NOT NULL DEFAULT 'amazon_components',
 is_amazon_invoiced INTEGER NOT NULL DEFAULT 0,
 vat_invoice_number TEXT,
 invoice_url TEXT,
 raw_json TEXT NOT NULL,
 imported_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS kaufland_tax_overrides(
 id TEXT PRIMARY KEY,
 id_order_unit TEXT NOT NULL UNIQUE,
 from_rate REAL NOT NULL,
 to_rate REAL NOT NULL,
 gross_cents INTEGER NOT NULL,
 net_cents INTEGER NOT NULL,
 vat_cents INTEGER NOT NULL,
 reason TEXT NOT NULL,
 created_at TEXT NOT NULL,
 created_by TEXT NOT NULL DEFAULT 'admin');
CREATE TABLE IF NOT EXISTS ust_reports(
 id TEXT PRIMARY KEY,
 month TEXT NOT NULL,
 revision INTEGER NOT NULL DEFAULT 1,
 kind TEXT NOT NULL DEFAULT 'original',
 supersedes_id TEXT,
 status TEXT NOT NULL,
 snapshot_json TEXT NOT NULL,
 blockers_json TEXT NOT NULL DEFAULT '[]',
 warnings_json TEXT NOT NULL DEFAULT '[]',
 created_at TEXT NOT NULL,
 filed_at TEXT,
 UNIQUE(month, revision));
CREATE INDEX IF NOT EXISTS idx_input_vat_deduction ON input_vat_invoices(deduction_month, input_vat_status);
CREATE INDEX IF NOT EXISTS idx_input_vat_provider ON input_vat_invoices(provider, invoice_date);
CREATE INDEX IF NOT EXISTS idx_amazon_tax_booking ON amazon_tax_rows(booking_date, tax_class);
CREATE INDEX IF NOT EXISTS idx_amazon_tax_order ON amazon_tax_rows(order_id, shipment_id, seller_sku);
CREATE INDEX IF NOT EXISTS idx_ust_reports_month ON ust_reports(month, revision DESC);
"""


def init_ust_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(UST_SCHEMA)
    for column_name, column_sql in (
        ("eu_tax_regime", f"TEXT NOT NULL DEFAULT '{EU_TAX_REGIME_UNCONFIRMED}'"),
        ("eu_distance_prior_year_cents", "INTEGER NOT NULL DEFAULT 0"),
        ("eu_distance_current_year_cents", "INTEGER NOT NULL DEFAULT 0"),
    ):
        _ensure_column(connection, "seller_profiles", column_name, column_sql)
