from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import sqlite3
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Iterable, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from app.config import AMAZON_FBA_DB_PATH, AMAZON_SP_API_SECRETS_PATH, ensure_runtime_dirs


SP_API_EU_ENDPOINT = "https://sellingpartnerapi-eu.amazon.com"
LWA_TOKEN_ENDPOINT = "https://api.amazon.com/auth/o2/token"
DEFAULT_ORDER_LOOKBACK_DAYS = 30
PRIMARY_EU_FBA_MARKETPLACE_ID = "A1PA6795UKMFR9"


class AmazonSpApiError(RuntimeError):
    pass


@dataclass(frozen=True)
class AmazonSpApiConfig:
    client_id: str
    client_secret: str
    refresh_token: str


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _stable_id(prefix: str, value: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"ecom-dash:{prefix}:{value}"))


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"), default=str)


def _payload_hash(value: Any) -> str:
    return hashlib.sha256(_json_dumps(value).encode("utf-8")).hexdigest()


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _text(value: Any) -> str:
    return str(value or "").strip()


def _amount_cents(value: Any) -> int:
    if isinstance(value, dict):
        value = value.get("CurrencyAmount", value.get("amount", value.get("Amount")))
    try:
        return int((Decimal(str(value or "0")) * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    except (InvalidOperation, ValueError):
        return 0


def _currency(value: Any, fallback: str = "EUR") -> str:
    if isinstance(value, dict):
        value = value.get("CurrencyCode", value.get("currencyCode", fallback))
    token = _text(value).upper()
    return token if len(token) == 3 else fallback


def extract_catalog_item_images(payload: dict[str, Any]) -> dict[str, Any]:
    """Select the largest image for each catalog variant, preferring MAIN."""
    variants: dict[str, tuple[int, str]] = {}
    for marketplace_images in _as_list(payload.get("images")):
        for image in _as_list(_as_dict(marketplace_images).get("images")):
            image_payload = _as_dict(image)
            link = _text(image_payload.get("link"))
            variant = _text(image_payload.get("variant")) or "OTHER"
            if not link:
                continue
            try:
                height = int(image_payload.get("height") or 0)
            except (TypeError, ValueError):
                height = 0
            previous = variants.get(variant)
            if previous is None or height > previous[0]:
                variants[variant] = (height, link)

    ordered = [variants[key][1] for key in sorted(variants) if variants[key][1]]
    main = variants.get("MAIN")
    return {
        "image_url": main[1] if main else (ordered[0] if ordered else ""),
        "image_urls": ordered,
    }


def extract_modern_financial_breakdown(transaction: dict[str, Any]) -> dict[str, Any]:
    sales_cents = 0
    tax_cents = 0
    fees: list[dict[str, Any]] = []
    fee_containers = {"amazonfees", "expenses"}
    ignored_fee_types = {"amazonfees", "expenses", "base", "tax", "promo"}

    def walk(nodes: Any, scope: str) -> None:
        nonlocal sales_cents, tax_cents
        for raw in _as_list(nodes):
            node = _as_dict(raw)
            node_type = _text(node.get("breakdownType"))
            normalized_type = node_type.lower()
            amount = _as_dict(node.get("breakdownAmount"))
            amount_cents = _amount_cents(amount.get("currencyAmount"))
            if scope == "sales" and normalized_type == "tax":
                tax_cents += abs(amount_cents)
            elif scope == "fees" and normalized_type not in ignored_fee_types:
                if amount_cents:
                    fees.append({"type": node_type, "amount_cents": abs(amount_cents)})
            next_scope = scope
            if normalized_type == "sales":
                sales_cents += amount_cents
                next_scope = "sales"
            elif normalized_type in fee_containers:
                next_scope = "fees"
            walk(node.get("breakdowns"), next_scope)

    walk(transaction.get("breakdowns"), "")
    contexts = [_as_dict(context) for context in _as_list(transaction.get("contexts"))]
    deferred_context = next(
        (context for context in contexts if _text(context.get("contextType")) == "DeferredContext"),
        {},
    )
    identifiers = {
        _text(_as_dict(identifier).get("relatedIdentifierName")): _text(
            _as_dict(identifier).get("relatedIdentifierValue")
        )
        for identifier in _as_list(transaction.get("relatedIdentifiers"))
    }
    return {
        "sales_cents": sales_cents,
        "tax_cents": tax_cents,
        "fees": fees,
        "net_cents": _amount_cents(_as_dict(transaction.get("totalAmount")).get("currencyAmount")),
        "financial_finality": (
            "released" if _text(transaction.get("transactionStatus")).upper() == "RELEASED"
            else "deferred" if _text(transaction.get("transactionStatus")).upper() == "DEFERRED"
            else "pending"
        ),
        "maturity_date": _text(deferred_context.get("maturityDate")) or None,
        "order_id": identifiers.get("ORDER_ID") or None,
        "shipment_id": identifiers.get("SHIPMENT_ID") or None,
        "settlement_id": identifiers.get("SETTLEMENT_ID") or None,
    }


def normalize_amazon_address(value: Any) -> dict[str, Any]:
    payload = _as_dict(value)
    if not payload:
        return {}

    def pick(*keys: str) -> Optional[str]:
        for key in keys:
            value = _text(payload.get(key))
            if value:
                return value
        return None

    result = {
        "first_name": None,
        "last_name": None,
        "name": pick("Name", "name"),
        "company": pick("Company", "CompanyName", "companyName"),
        "address1": pick("AddressLine1", "addressLine1"),
        "address2": pick("AddressLine2", "addressLine2"),
        "street": pick("Street", "street"),
        "house_number": pick("HouseNumber", "houseNumber"),
        "postcode": pick("PostalCode", "postalCode", "postcode"),
        "city": pick("City", "city"),
        "state_or_region": pick("StateOrRegion", "stateOrRegion"),
        "country": pick("CountryCode", "countryCode", "country"),
        "country_code": pick("CountryCode", "countryCode", "country"),
        "phone": pick("Phone", "phone"),
    }
    return result if any(value is not None for value in result.values()) else {}


def _quantity(value: Any) -> int:
    if isinstance(value, dict):
        for key in ("quantity", "Quantity", "value", "Value", "totalReservedQuantity"):
            if key in value:
                return _quantity(value[key])
        return 0
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _primary_inbound_marketplace(marketplace_ids: Iterable[str]) -> str:
    ids = sorted(set(_text(value) for value in marketplace_ids if _text(value)))
    if PRIMARY_EU_FBA_MARKETPLACE_ID in ids:
        return PRIMARY_EU_FBA_MARKETPLACE_ID
    return ids[0] if ids else ""


def normalize_fba_status(status: str) -> dict[str, Any]:
    token = _text(status).upper()
    labels = {
        "WORKING": "In Vorbereitung",
        "READY_TO_SHIP": "Nicht versendet",
        "SHIPPED": "Versendet",
        "IN_TRANSIT": "Unterwegs",
        "DELIVERED": "Geliefert",
        "CHECKED_IN": "Eingecheckt",
        "RECEIVING": "Empfang läuft",
        "CLOSED": "Empfangen",
        "CANCELLED": "Storniert",
        "DELETED": "Gelöscht",
        "ERROR": "Fehler",
    }
    return {
        "label": labels.get(token, token.replace("_", " ").title() or "Unbekannt"),
        "received": token in {"RECEIVING", "CLOSED"},
        "inventory_eligible": token in {"RECEIVING", "CLOSED"},
    }


def normalize_shipment_items(items: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: dict[tuple[str, str], dict[str, Any]] = {}
    for raw in items:
        row = _as_dict(raw)
        seller_sku = _text(row.get("SellerSKU") or row.get("msku") or row.get("sellerSku") or row.get("seller_sku"))
        fnsku = _text(row.get("FulfillmentNetworkSKU") or row.get("fnsku"))
        if not seller_sku and not fnsku:
            continue
        key = (seller_sku, fnsku)
        item = normalized.setdefault(
            key,
            {
                "seller_sku": seller_sku,
                "fnsku": fnsku,
                "quantity_shipped": 0,
                "quantity_received": 0,
            },
        )
        item["quantity_shipped"] = max(item["quantity_shipped"], _quantity(row.get("QuantityShipped") or row.get("quantity") or row.get("quantity_shipped")))
        item["quantity_received"] = max(item["quantity_received"], _quantity(row.get("QuantityReceived") or row.get("quantityReceived") or row.get("quantity_received")))
        asin = _text(row.get("ASIN") or row.get("asin"))
        title = _text(row.get("Title") or row.get("title"))
        if asin:
            item["asin"] = asin
        if title:
            item["title"] = title
    return list(normalized.values())


def suggest_shipment_for_inbound_cost(
    amount_cents: int,
    currency: str,
    source_event: dict[str, Any],
    shipments: Iterable[dict[str, Any]],
) -> Optional[str]:
    """Return only an explicit shipment match; amount/date alone is never automatic."""
    direct_id = _text(source_event.get("shipment_id") or source_event.get("ShipmentId"))
    if direct_id and any(_text(row.get("shipment_id")) == direct_id for row in shipments):
        return direct_id
    return None


def extract_modern_inbound_costs(transactions: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    costs: list[dict[str, Any]] = []

    def walk_breakdowns(breakdowns: Any) -> Iterable[dict[str, Any]]:
        for raw in _as_list(breakdowns):
            breakdown = _as_dict(raw)
            yield breakdown
            yield from walk_breakdowns(breakdown.get("breakdowns"))

    for transaction in transactions:
        payload = _as_dict(transaction)
        for item in _as_list(payload.get("items")):
            item_payload = _as_dict(item)
            shipment_ids = {
                token
                for identifier in _as_list(item_payload.get("relatedIdentifiers"))
                for value in [_text(_as_dict(identifier).get("itemRelatedIdentifierValue") or _as_dict(identifier).get("relatedIdentifierValue"))]
                for token in value.split(":")
                if token.startswith("FBA")
            }
            if len(shipment_ids) != 1:
                continue
            shipment_id = next(iter(shipment_ids))
            for breakdown in walk_breakdowns(item_payload.get("breakdowns")):
                cost_type = _text(breakdown.get("breakdownType"))
                if not cost_type.lower().startswith("fbainbound"):
                    continue
                amount = _as_dict(breakdown.get("breakdownAmount"))
                amount_cents = abs(_amount_cents(amount.get("currencyAmount")))
                if not amount_cents:
                    continue
                costs.append({
                    "shipment_id": shipment_id,
                    "source_event_id": f"{_text(payload.get('transactionId'))}:{shipment_id}",
                    "cost_type": cost_type,
                    "amount_cents": amount_cents,
                    "currency": _currency(amount.get("currencyCode")),
                    "raw_json": _json_dumps(payload),
                })
    return costs


def _component_total(items: Any) -> int:
    total = 0
    for item in _as_list(items):
        payload = _as_dict(item)
        total += _amount_cents(payload.get("Amount", payload))
    return total


def _connect() -> sqlite3.Connection:
    ensure_runtime_dirs()
    AMAZON_FBA_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(AMAZON_FBA_DB_PATH)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def init_amazon_fba_db() -> None:
    with _connect() as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                applied_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sync_runs (
                id TEXT PRIMARY KEY,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                status TEXT NOT NULL,
                requested_scopes_json TEXT NOT NULL,
                summary_json TEXT,
                error_message TEXT
            );

            CREATE TABLE IF NOT EXISTS sync_cursors (
                scope TEXT PRIMARY KEY,
                cursor_value TEXT,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS raw_records (
                id TEXT PRIMARY KEY,
                sync_run_id TEXT,
                resource_type TEXT NOT NULL,
                external_id TEXT,
                payload_hash TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                captured_at TEXT NOT NULL,
                UNIQUE(resource_type, payload_hash),
                FOREIGN KEY (sync_run_id) REFERENCES sync_runs(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS amazon_marketplaces (
                marketplace_id TEXT PRIMARY KEY,
                name TEXT NOT NULL DEFAULT '',
                country_code TEXT NOT NULL DEFAULT '',
                domain_name TEXT NOT NULL DEFAULT '',
                default_currency TEXT NOT NULL DEFAULT '',
                participation_json TEXT NOT NULL DEFAULT '{}',
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS amazon_sync_settings (
                id TEXT PRIMARY KEY,
                marketplace_mode TEXT NOT NULL DEFAULT 'auto',
                selected_marketplace_ids TEXT NOT NULL DEFAULT '[]',
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS amazon_orders (
                amazon_order_id TEXT PRIMARY KEY,
                seller_order_id TEXT NOT NULL DEFAULT '',
                marketplace_id TEXT NOT NULL DEFAULT '',
                purchase_date TEXT,
                last_update_date TEXT,
                order_status TEXT NOT NULL DEFAULT '',
                fulfillment_channel TEXT NOT NULL DEFAULT '',
                sales_channel TEXT NOT NULL DEFAULT '',
                currency TEXT NOT NULL DEFAULT 'EUR',
                order_total_cents INTEGER NOT NULL DEFAULT 0,
                buyer_name TEXT NOT NULL DEFAULT '',
                buyer_email TEXT NOT NULL DEFAULT '',
                is_synthetic INTEGER NOT NULL DEFAULT 0,
                raw_json TEXT NOT NULL DEFAULT '{}',
                updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_amazon_orders_purchase_date
                ON amazon_orders(purchase_date DESC);
            CREATE INDEX IF NOT EXISTS idx_amazon_orders_marketplace
                ON amazon_orders(marketplace_id);

            CREATE TABLE IF NOT EXISTS amazon_order_items (
                id TEXT PRIMARY KEY,
                amazon_order_id TEXT NOT NULL,
                asin TEXT NOT NULL DEFAULT '',
                seller_sku TEXT NOT NULL DEFAULT '',
                title TEXT NOT NULL DEFAULT '',
                quantity_ordered INTEGER NOT NULL DEFAULT 0,
                quantity_shipped INTEGER NOT NULL DEFAULT 0,
                currency TEXT NOT NULL DEFAULT 'EUR',
                item_price_cents INTEGER NOT NULL DEFAULT 0,
                item_tax_cents INTEGER NOT NULL DEFAULT 0,
                image_url TEXT NOT NULL DEFAULT '',
                image_urls_json TEXT NOT NULL DEFAULT '[]',
                raw_json TEXT NOT NULL DEFAULT '{}',
                UNIQUE(amazon_order_id, asin, seller_sku),
                FOREIGN KEY (amazon_order_id) REFERENCES amazon_orders(amazon_order_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS amazon_financial_events (
                id TEXT PRIMARY KEY,
                event_type TEXT NOT NULL,
                amazon_order_id TEXT,
                settlement_id TEXT,
                posted_date TEXT,
                financial_finality TEXT NOT NULL DEFAULT 'pending',
                currency TEXT NOT NULL DEFAULT 'EUR',
                sales_cents INTEGER NOT NULL DEFAULT 0,
                fees_cents INTEGER NOT NULL DEFAULT 0,
                net_cents INTEGER NOT NULL DEFAULT 0,
                raw_json TEXT NOT NULL,
                UNIQUE(event_type, id),
                FOREIGN KEY (amazon_order_id) REFERENCES amazon_orders(amazon_order_id) ON DELETE SET NULL
            );

            CREATE INDEX IF NOT EXISTS idx_amazon_financial_events_order
                ON amazon_financial_events(amazon_order_id, posted_date);
            CREATE INDEX IF NOT EXISTS idx_amazon_financial_events_settlement
                ON amazon_financial_events(settlement_id);

            CREATE TABLE IF NOT EXISTS amazon_settlements (
                settlement_id TEXT PRIMARY KEY,
                processing_status TEXT NOT NULL DEFAULT '',
                original_total_cents INTEGER NOT NULL DEFAULT 0,
                currency TEXT NOT NULL DEFAULT 'EUR',
                fund_transfer_date TEXT,
                raw_json TEXT NOT NULL DEFAULT '{}',
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS amazon_report_jobs (
                report_id TEXT PRIMARY KEY,
                report_type TEXT NOT NULL,
                processing_status TEXT NOT NULL DEFAULT '',
                report_document_id TEXT,
                requested_at TEXT NOT NULL,
                completed_at TEXT,
                raw_json TEXT NOT NULL DEFAULT '{}',
                imported_at TEXT
            );

            CREATE TABLE IF NOT EXISTS amazon_financial_components (
                id TEXT PRIMARY KEY,
                event_id TEXT NOT NULL,
                component_type TEXT NOT NULL,
                amount_cents INTEGER NOT NULL,
                currency TEXT NOT NULL DEFAULT 'EUR',
                raw_json TEXT NOT NULL DEFAULT '{}',
                FOREIGN KEY (event_id) REFERENCES amazon_financial_events(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS amazon_auto_refresh_tasks (
                task_name TEXT PRIMARY KEY,
                last_started_at TEXT,
                last_finished_at TEXT,
                last_success_at TEXT,
                last_status TEXT NOT NULL DEFAULT 'never_started',
                last_error TEXT,
                backoff_level INTEGER NOT NULL DEFAULT 0,
                next_eligible_at TEXT
            );

            CREATE TABLE IF NOT EXISTS amazon_auto_refresh_lease (
                lease_name TEXT PRIMARY KEY,
                owner_id TEXT NOT NULL,
                expires_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS amazon_inventory_snapshots (
                id TEXT PRIMARY KEY,
                captured_at TEXT NOT NULL,
                marketplace_id TEXT NOT NULL DEFAULT '',
                seller_sku TEXT NOT NULL DEFAULT '',
                fnsku TEXT NOT NULL DEFAULT '',
                asin TEXT NOT NULL DEFAULT '',
                product_name TEXT NOT NULL DEFAULT '',
                fulfillable_quantity INTEGER NOT NULL DEFAULT 0,
                inbound_working_quantity INTEGER NOT NULL DEFAULT 0,
                inbound_shipped_quantity INTEGER NOT NULL DEFAULT 0,
                reserved_quantity INTEGER NOT NULL DEFAULT 0,
                unfulfillable_quantity INTEGER NOT NULL DEFAULT 0,
                raw_json TEXT NOT NULL DEFAULT '{}'
            );

            CREATE INDEX IF NOT EXISTS idx_amazon_inventory_snapshot_key
                ON amazon_inventory_snapshots(captured_at, fnsku, seller_sku);

            CREATE TABLE IF NOT EXISTS amazon_inbound_plans (
                id TEXT PRIMARY KEY,
                plan_id TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT '',
                destination_fulfillment_center_id TEXT NOT NULL DEFAULT '',
                raw_json TEXT NOT NULL DEFAULT '{}',
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS amazon_inbound_shipments (
                id TEXT PRIMARY KEY,
                shipment_id TEXT NOT NULL UNIQUE,
                plan_id TEXT NOT NULL DEFAULT '',
                shipment_name TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT '',
                destination_fulfillment_center_id TEXT NOT NULL DEFAULT '',
                raw_json TEXT NOT NULL DEFAULT '{}',
                inventory_eligible_at TEXT,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS amazon_inbound_shipment_items (
                id TEXT PRIMARY KEY,
                shipment_id TEXT NOT NULL,
                seller_sku TEXT NOT NULL DEFAULT '',
                fnsku TEXT NOT NULL DEFAULT '',
                asin TEXT NOT NULL DEFAULT '',
                title TEXT NOT NULL DEFAULT '',
                quantity_shipped INTEGER NOT NULL DEFAULT 0,
                quantity_received INTEGER NOT NULL DEFAULT 0,
                raw_json TEXT NOT NULL DEFAULT '{}',
                UNIQUE(shipment_id, seller_sku, fnsku),
                FOREIGN KEY (shipment_id) REFERENCES amazon_inbound_shipments(shipment_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS amazon_inbound_shipment_boxes (
                id TEXT PRIMARY KEY,
                shipment_id TEXT NOT NULL,
                box_id TEXT NOT NULL DEFAULT '',
                weight_value REAL,
                weight_unit TEXT NOT NULL DEFAULT '',
                length REAL,
                width REAL,
                height REAL,
                dimension_unit TEXT NOT NULL DEFAULT '',
                raw_json TEXT NOT NULL DEFAULT '{}',
                UNIQUE(shipment_id, box_id),
                FOREIGN KEY (shipment_id) REFERENCES amazon_inbound_shipments(shipment_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS amazon_inbound_transport_options (
                id TEXT PRIMARY KEY,
                shipment_id TEXT NOT NULL,
                option_id TEXT NOT NULL DEFAULT '',
                carrier TEXT NOT NULL DEFAULT '',
                shipping_solution TEXT NOT NULL DEFAULT '',
                shipping_mode TEXT NOT NULL DEFAULT '',
                quote_cents INTEGER,
                currency TEXT NOT NULL DEFAULT 'EUR',
                selected INTEGER NOT NULL DEFAULT 0,
                raw_json TEXT NOT NULL DEFAULT '{}',
                UNIQUE(shipment_id, option_id),
                FOREIGN KEY (shipment_id) REFERENCES amazon_inbound_shipments(shipment_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS amazon_inbound_costs (
                id TEXT PRIMARY KEY,
                shipment_id TEXT,
                source_event_id TEXT,
                cost_type TEXT NOT NULL,
                amount_cents INTEGER NOT NULL,
                currency TEXT NOT NULL DEFAULT 'EUR',
                status TEXT NOT NULL DEFAULT 'unassigned',
                allocation_method TEXT NOT NULL DEFAULT 'value',
                raw_json TEXT NOT NULL DEFAULT '{}',
                notes TEXT NOT NULL DEFAULT '',
                UNIQUE(source_event_id, cost_type),
                FOREIGN KEY (shipment_id) REFERENCES amazon_inbound_shipments(shipment_id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS amazon_inbound_invoices (
                id TEXT PRIMARY KEY,
                shipment_id TEXT NOT NULL,
                supplier_name TEXT NOT NULL DEFAULT '',
                invoice_number TEXT NOT NULL DEFAULT '',
                invoice_date TEXT,
                currency TEXT NOT NULL DEFAULT 'EUR',
                gross_cents INTEGER NOT NULL DEFAULT 0,
                net_cents INTEGER NOT NULL DEFAULT 0,
                vat_cents INTEGER NOT NULL DEFAULT 0,
                document_path TEXT NOT NULL DEFAULT '',
                notes TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                FOREIGN KEY (shipment_id) REFERENCES amazon_inbound_shipments(shipment_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS amazon_inbound_invoice_lines (
                id TEXT PRIMARY KEY,
                invoice_id TEXT NOT NULL,
                seller_sku TEXT NOT NULL DEFAULT '',
                fnsku TEXT NOT NULL DEFAULT '',
                asin TEXT NOT NULL DEFAULT '',
                title TEXT NOT NULL DEFAULT '',
                quantity INTEGER NOT NULL,
                net_cents INTEGER NOT NULL DEFAULT 0,
                vat_cents INTEGER NOT NULL DEFAULT 0,
                raw_json TEXT NOT NULL DEFAULT '{}',
                UNIQUE(invoice_id, seller_sku, fnsku),
                FOREIGN KEY (invoice_id) REFERENCES amazon_inbound_invoices(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS amazon_inbound_cost_allocations (
                id TEXT PRIMARY KEY,
                invoice_id TEXT NOT NULL,
                invoice_line_id TEXT NOT NULL,
                shipment_id TEXT NOT NULL,
                seller_sku TEXT NOT NULL DEFAULT '',
                fnsku TEXT NOT NULL DEFAULT '',
                quantity INTEGER NOT NULL,
                net_cents INTEGER NOT NULL DEFAULT 0,
                currency TEXT NOT NULL DEFAULT 'EUR',
                allocation_method TEXT NOT NULL DEFAULT 'invoice_line',
                created_at TEXT NOT NULL,
                UNIQUE(invoice_line_id),
                FOREIGN KEY (invoice_id) REFERENCES amazon_inbound_invoices(id) ON DELETE CASCADE,
                FOREIGN KEY (invoice_line_id) REFERENCES amazon_inbound_invoice_lines(id) ON DELETE CASCADE,
                FOREIGN KEY (shipment_id) REFERENCES amazon_inbound_shipments(shipment_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS procurement_batches (
                id TEXT PRIMARY KEY,
                reference TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'draft',
                currency TEXT NOT NULL DEFAULT 'EUR',
                received_at TEXT,
                notes TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS procurement_batch_lines (
                id TEXT PRIMARY KEY,
                batch_id TEXT NOT NULL,
                seller_sku TEXT NOT NULL DEFAULT '',
                fnsku TEXT NOT NULL DEFAULT '',
                asin TEXT NOT NULL DEFAULT '',
                title TEXT NOT NULL DEFAULT '',
                quantity INTEGER NOT NULL,
                allocation_basis TEXT NOT NULL DEFAULT 'value',
                FOREIGN KEY (batch_id) REFERENCES procurement_batches(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS supplier_invoices (
                id TEXT PRIMARY KEY,
                batch_id TEXT NOT NULL,
                supplier_name TEXT NOT NULL DEFAULT '',
                invoice_number TEXT NOT NULL DEFAULT '',
                invoice_date TEXT,
                currency TEXT NOT NULL DEFAULT 'EUR',
                gross_cents INTEGER NOT NULL DEFAULT 0,
                net_cents INTEGER NOT NULL DEFAULT 0,
                vat_cents INTEGER NOT NULL DEFAULT 0,
                input_vat_status TEXT NOT NULL DEFAULT 'review_required',
                document_path TEXT NOT NULL DEFAULT '',
                notes TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                UNIQUE(batch_id, supplier_name, invoice_number),
                FOREIGN KEY (batch_id) REFERENCES procurement_batches(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS supplier_invoice_lines (
                id TEXT PRIMARY KEY,
                invoice_id TEXT NOT NULL,
                seller_sku TEXT NOT NULL DEFAULT '',
                title TEXT NOT NULL DEFAULT '',
                quantity INTEGER NOT NULL DEFAULT 0,
                net_cents INTEGER NOT NULL DEFAULT 0,
                vat_cents INTEGER NOT NULL DEFAULT 0,
                gross_cents INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY (invoice_id) REFERENCES supplier_invoices(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS procurement_cost_allocations (
                id TEXT PRIMARY KEY,
                batch_id TEXT NOT NULL,
                invoice_id TEXT,
                cost_type TEXT NOT NULL,
                amount_cents INTEGER NOT NULL,
                currency TEXT NOT NULL DEFAULT 'EUR',
                allocation_method TEXT NOT NULL DEFAULT 'value',
                FOREIGN KEY (batch_id) REFERENCES procurement_batches(id) ON DELETE CASCADE,
                FOREIGN KEY (invoice_id) REFERENCES supplier_invoices(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS inventory_lots (
                id TEXT PRIMARY KEY,
                batch_line_id TEXT,
                inbound_shipment_id TEXT,
                inbound_shipment_item_id TEXT,
                seller_sku TEXT NOT NULL DEFAULT '',
                available_quantity INTEGER NOT NULL,
                unit_cost_cents INTEGER NOT NULL,
                cost_remainder_cents INTEGER NOT NULL DEFAULT 0,
                received_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (batch_line_id) REFERENCES procurement_batch_lines(id) ON DELETE CASCADE,
                FOREIGN KEY (inbound_shipment_id) REFERENCES amazon_inbound_shipments(shipment_id) ON DELETE CASCADE,
                FOREIGN KEY (inbound_shipment_item_id) REFERENCES amazon_inbound_shipment_items(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS fifo_allocations (
                id TEXT PRIMARY KEY,
                amazon_order_id TEXT NOT NULL,
                amazon_order_item_id TEXT,
                inventory_lot_id TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                unit_cost_cents INTEGER NOT NULL,
                allocated_cost_cents INTEGER NOT NULL DEFAULT 0,
                allocated_at TEXT NOT NULL,
                UNIQUE(amazon_order_id, amazon_order_item_id, inventory_lot_id),
                FOREIGN KEY (amazon_order_id) REFERENCES amazon_orders(amazon_order_id) ON DELETE CASCADE,
                FOREIGN KEY (amazon_order_item_id) REFERENCES amazon_order_items(id) ON DELETE SET NULL,
                FOREIGN KEY (inventory_lot_id) REFERENCES inventory_lots(id) ON DELETE RESTRICT
            );
            """
        )
        connection.execute(
            "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (?, ?)",
            (1, _utc_now()),
        )
        existing_columns = {
            str(row[1]) for row in connection.execute("PRAGMA table_info(amazon_order_items)").fetchall()
        }
        for column, definition in (
            ("image_url", "TEXT NOT NULL DEFAULT ''"),
            ("image_urls_json", "TEXT NOT NULL DEFAULT '[]'"),
        ):
            if column not in existing_columns:
                connection.execute(f"ALTER TABLE amazon_order_items ADD COLUMN {column} {definition}")
        inbound_cost_columns = {
            str(row[1]) for row in connection.execute("PRAGMA table_info(amazon_inbound_costs)").fetchall()
        }
        if "notes" not in inbound_cost_columns:
            connection.execute("ALTER TABLE amazon_inbound_costs ADD COLUMN notes TEXT NOT NULL DEFAULT ''")
        shipment_columns = {
            str(row[1]) for row in connection.execute("PRAGMA table_info(amazon_inbound_shipments)").fetchall()
        }
        if "inventory_eligible_at" not in shipment_columns:
            connection.execute("ALTER TABLE amazon_inbound_shipments ADD COLUMN inventory_eligible_at TEXT")
        connection.execute(
            "UPDATE amazon_inbound_shipments SET inventory_eligible_at = updated_at WHERE inventory_eligible_at IS NULL AND status IN ('RECEIVING', 'CLOSED')"
        )
        inventory_columns = {
            str(row[1]) for row in connection.execute("PRAGMA table_info(inventory_lots)").fetchall()
        }
        if "inbound_shipment_id" not in inventory_columns:
            connection.commit()
            connection.execute("PRAGMA foreign_keys = OFF")
            connection.execute("ALTER TABLE fifo_allocations RENAME TO fifo_allocations_legacy")
            connection.execute("ALTER TABLE inventory_lots RENAME TO inventory_lots_legacy")
            connection.execute(
                """
                CREATE TABLE inventory_lots (
                    id TEXT PRIMARY KEY,
                    batch_line_id TEXT,
                    inbound_shipment_id TEXT,
                    inbound_shipment_item_id TEXT,
                    seller_sku TEXT NOT NULL DEFAULT '',
                    available_quantity INTEGER NOT NULL,
                    unit_cost_cents INTEGER NOT NULL,
                    cost_remainder_cents INTEGER NOT NULL DEFAULT 0,
                    received_at TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (batch_line_id) REFERENCES procurement_batch_lines(id) ON DELETE CASCADE,
                    FOREIGN KEY (inbound_shipment_id) REFERENCES amazon_inbound_shipments(shipment_id) ON DELETE CASCADE,
                    FOREIGN KEY (inbound_shipment_item_id) REFERENCES amazon_inbound_shipment_items(id) ON DELETE SET NULL
                )
                """
            )
            connection.execute(
                """
                INSERT INTO inventory_lots(id, batch_line_id, seller_sku, available_quantity, unit_cost_cents, cost_remainder_cents, received_at, created_at)
                SELECT id, batch_line_id, seller_sku, available_quantity, unit_cost_cents, 0, received_at, created_at
                FROM inventory_lots_legacy
                """
            )
            connection.execute("DROP TABLE inventory_lots_legacy")
            connection.execute(
                """
                CREATE TABLE fifo_allocations (
                    id TEXT PRIMARY KEY,
                    amazon_order_id TEXT NOT NULL,
                    amazon_order_item_id TEXT,
                    inventory_lot_id TEXT NOT NULL,
                    quantity INTEGER NOT NULL,
                    unit_cost_cents INTEGER NOT NULL,
                    allocated_cost_cents INTEGER NOT NULL DEFAULT 0,
                    allocated_at TEXT NOT NULL,
                    UNIQUE(amazon_order_id, amazon_order_item_id, inventory_lot_id),
                    FOREIGN KEY (amazon_order_id) REFERENCES amazon_orders(amazon_order_id) ON DELETE CASCADE,
                    FOREIGN KEY (amazon_order_item_id) REFERENCES amazon_order_items(id) ON DELETE SET NULL,
                    FOREIGN KEY (inventory_lot_id) REFERENCES inventory_lots(id) ON DELETE RESTRICT
                )
                """
            )
            connection.execute(
                """
                INSERT INTO fifo_allocations(id, amazon_order_id, amazon_order_item_id, inventory_lot_id, quantity, unit_cost_cents, allocated_cost_cents, allocated_at)
                SELECT id, amazon_order_id, amazon_order_item_id, inventory_lot_id, quantity, unit_cost_cents, quantity * unit_cost_cents, allocated_at
                FROM fifo_allocations_legacy
                """
            )
            connection.execute("DROP TABLE fifo_allocations_legacy")
            connection.execute("PRAGMA foreign_keys = ON")
        lot_columns = {str(row[1]) for row in connection.execute("PRAGMA table_info(inventory_lots)").fetchall()}
        if "cost_remainder_cents" not in lot_columns:
            connection.execute("ALTER TABLE inventory_lots ADD COLUMN cost_remainder_cents INTEGER NOT NULL DEFAULT 0")
        allocation_columns = {str(row[1]) for row in connection.execute("PRAGMA table_info(fifo_allocations)").fetchall()}
        if "allocated_cost_cents" not in allocation_columns:
            connection.execute("ALTER TABLE fifo_allocations ADD COLUMN allocated_cost_cents INTEGER NOT NULL DEFAULT 0")
            connection.execute("UPDATE fifo_allocations SET allocated_cost_cents = quantity * unit_cost_cents WHERE allocated_cost_cents = 0")
        connection.commit()


def load_amazon_sp_api_config() -> tuple[Optional[AmazonSpApiConfig], list[str]]:
    client_id = _text(os.getenv("AMAZON_SP_API_CLIENT_ID"))
    client_secret = _text(os.getenv("AMAZON_SP_API_CLIENT_SECRET"))
    refresh_token = _text(os.getenv("AMAZON_SP_API_REFRESH_TOKEN"))

    if not (client_id and client_secret and refresh_token):
        try:
            raw = json.loads(AMAZON_SP_API_SECRETS_PATH.read_text(encoding="utf-8"))
        except FileNotFoundError:
            raw = {}
        except (OSError, json.JSONDecodeError):
            return None, ["secret file is unreadable"]

        payload = _as_dict(raw)
        client_id = client_id or _text(payload.get("client_id"))
        client_secret = client_secret or _text(payload.get("client_secret"))
        refresh_token = refresh_token or _text(payload.get("refresh_token"))

    missing = [name for name, value in (
        ("client_id", client_id),
        ("client_secret", client_secret),
        ("refresh_token", refresh_token),
    ) if not value]
    if missing:
        return None, missing
    return AmazonSpApiConfig(client_id=client_id, client_secret=client_secret, refresh_token=refresh_token), []


class AmazonSpApiClient:
    def __init__(self, config: AmazonSpApiConfig) -> None:
        self._config = config
        self._access_token = ""
        self._expires_at = 0.0
        self._rate_limits: dict[str, str] = {}

    @property
    def rate_limits(self) -> dict[str, str]:
        return dict(self._rate_limits)

    def _lwa_access_token(self) -> str:
        if self._access_token and time.time() < self._expires_at:
            return self._access_token

        form = urlencode({
            "grant_type": "refresh_token",
            "refresh_token": self._config.refresh_token,
            "client_id": self._config.client_id,
            "client_secret": self._config.client_secret,
        }).encode("utf-8")
        request = Request(LWA_TOKEN_ENDPOINT, data=form, method="POST")
        request.add_header("Content-Type", "application/x-www-form-urlencoded")
        try:
            with urlopen(request, timeout=30) as response:
                payload = _as_dict(json.loads(response.read().decode("utf-8")))
        except (HTTPError, URLError, OSError, json.JSONDecodeError) as exc:
            raise AmazonSpApiError(f"LWA token request failed: {exc}") from exc

        token = _text(payload.get("access_token"))
        if not token:
            raise AmazonSpApiError("LWA token response did not include access_token")
        self._access_token = token
        self._expires_at = time.time() + max(int(payload.get("expires_in") or 3600) - 60, 60)
        return token

    def request_json(
        self,
        path: str,
        *,
        params: Optional[dict[str, Any]] = None,
        method: str = "GET",
        body: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        query = urlencode(params or {}, doseq=True)
        url = f"{SP_API_EU_ENDPOINT}{path}"
        if query:
            url = f"{url}?{query}"
        data = _json_dumps(body).encode("utf-8") if body is not None else None
        request = Request(url, data=data, method=method)
        request.add_header("x-amz-access-token", self._lwa_access_token())
        request.add_header("Accept", "application/json")
        if body is not None:
            request.add_header("Content-Type", "application/json")
        try:
            with urlopen(request, timeout=60) as response:
                rate_limit = response.headers.get("x-amzn-RateLimit-Limit")
                if rate_limit:
                    self._rate_limits[path] = rate_limit
                return _as_dict(json.loads(response.read().decode("utf-8")))
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")[:500]
            raise AmazonSpApiError(f"SP-API {exc.code} for {path}: {body}") from exc
        except (URLError, OSError, json.JSONDecodeError) as exc:
            raise AmazonSpApiError(f"SP-API request failed for {path}: {exc}") from exc

    def marketplace_participations(self) -> dict[str, Any]:
        return self.request_json("/sellers/v1/marketplaceParticipations")

    def inventory_summaries(self, marketplace_id: str) -> list[dict[str, Any]]:
        payload = self.request_json(
            "/fba/inventory/v1/summaries",
            params={
                "details": "true",
                "granularityType": "Marketplace",
                "granularityId": marketplace_id,
                "marketplaceIds": marketplace_id,
            },
        )
        result: list[dict[str, Any]] = []
        while True:
            response_payload = _as_dict(payload.get("payload"))
            result.extend(_as_dict(item) for item in _as_list(response_payload.get("inventorySummaries")))
            next_token = _text(response_payload.get("nextToken"))
            if not next_token:
                break
            payload = self.request_json("/fba/inventory/v1/summaries", params={"nextToken": next_token})
        return result

    def orders(self, marketplace_ids: list[str], created_after: str, *, updated_after: Optional[str] = None) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
        if not marketplace_ids:
            return [], []
        result: list[dict[str, Any]] = []
        errors: list[dict[str, str]] = []
        for marketplace_id in marketplace_ids:
            try:
                params = {"MarketplaceIds": marketplace_id, "MaxResultsPerPage": 100}
                params["LastUpdatedAfter" if updated_after else "CreatedAfter"] = updated_after or created_after
                payload = self.request_json(
                    "/orders/v0/orders",
                    params=params,
                )
                while True:
                    response_payload = _as_dict(payload.get("payload"))
                    for item in _as_list(response_payload.get("Orders")):
                        order = _as_dict(item)
                        order.setdefault("MarketplaceId", marketplace_id)
                        result.append(order)
                    next_token = _text(response_payload.get("NextToken"))
                    if not next_token:
                        break
                    payload = self.request_json("/orders/v0/orders", params={"NextToken": next_token})
            except AmazonSpApiError as exc:
                errors.append({"marketplace_id": marketplace_id, "error": str(exc)})
                continue
        return result, errors

    def inbound_shipments(self, marketplace_ids: list[str]) -> list[dict[str, Any]]:
        statuses = ",".join((
            "WORKING", "READY_TO_SHIP", "SHIPPED", "RECEIVING", "CANCELLED",
            "DELETED", "CLOSED", "ERROR", "IN_TRANSIT", "DELIVERED", "CHECKED_IN",
        ))
        result: dict[str, dict[str, Any]] = {}
        # FBA inbound shipments are account-level in the EU seller account. Querying
        # every participating marketplace returns the same shipment list and burns
        # the low legacy inbound quota.
        marketplace_id = _primary_inbound_marketplace(marketplace_ids)
        for marketplace_id in [marketplace_id] if marketplace_id else []:
            payload = self.request_json(
                "/fba/inbound/v0/shipments",
                params={
                    "QueryType": "SHIPMENT",
                    "MarketplaceId": marketplace_id,
                    "ShipmentStatusList": statuses,
                },
            )
            seen_tokens: set[str] = set()
            while True:
                response_payload = _as_dict(payload.get("payload")) or payload
                for item in _as_list(response_payload.get("ShipmentData")):
                    shipment = _as_dict(item)
                    shipment_id = _text(shipment.get("ShipmentId"))
                    if shipment_id:
                        shipment.setdefault("MarketplaceId", marketplace_id)
                        result[shipment_id] = shipment
                next_token = _text(response_payload.get("NextToken"))
                if not next_token or next_token in seen_tokens:
                    break
                seen_tokens.add(next_token)
                payload = self.request_json(
                    "/fba/inbound/v0/shipments",
                    params={"QueryType": "NEXT_TOKEN", "NextToken": next_token},
                )
        return list(result.values())

    def shipment_items(self, shipment_id: str) -> list[dict[str, Any]]:
        path = f"/fba/inbound/v0/shipments/{shipment_id}/items"
        payload: Optional[dict[str, Any]] = None
        for attempt in range(3):
            try:
                payload = self.request_json(path)
                break
            except AmazonSpApiError as exc:
                if " 429 " not in str(exc) or attempt == 2:
                    raise
                time.sleep(1.5 * (attempt + 1))
        if payload is None:
            return []
        result: dict[tuple[str, str], dict[str, Any]] = {}
        seen_tokens: set[str] = set()
        while True:
            response_payload = _as_dict(payload.get("payload")) or payload
            for item in normalize_shipment_items(_as_list(response_payload.get("ItemData"))):
                key = (_text(item.get("seller_sku")), _text(item.get("fnsku")))
                result[key] = item
            next_token = _text(response_payload.get("NextToken"))
            if not next_token or next_token in seen_tokens:
                break
            seen_tokens.add(next_token)
            payload = self.request_json(path, params={"NextToken": next_token})
        return list(result.values())

    def bulk_inbound_shipment_items(self, marketplace_id: str, *, lookback_days: int = 730) -> dict[str, list[dict[str, Any]]]:
        now = datetime.now(timezone.utc)
        payload = self.request_json(
            "/fba/inbound/v0/shipmentItems",
            params={
                "QueryType": "DATE_RANGE",
                "MarketplaceId": marketplace_id,
                "LastUpdatedAfter": (now - timedelta(days=max(1, lookback_days))).isoformat().replace("+00:00", "Z"),
                "LastUpdatedBefore": now.isoformat().replace("+00:00", "Z"),
            },
        )
        grouped: dict[str, dict[tuple[str, str], dict[str, Any]]] = {}
        seen_tokens: set[str] = set()
        while True:
            response_payload = _as_dict(payload.get("payload")) or payload
            for raw in _as_list(response_payload.get("ItemData")):
                row = _as_dict(raw)
                shipment_id = _text(row.get("ShipmentId"))
                normalized = normalize_shipment_items([row])
                if not shipment_id or not normalized:
                    continue
                item = normalized[0]
                key = (_text(item.get("seller_sku")), _text(item.get("fnsku")))
                grouped.setdefault(shipment_id, {})[key] = item
            next_token = _text(response_payload.get("NextToken"))
            if not next_token or next_token in seen_tokens:
                break
            seen_tokens.add(next_token)
            payload = self.request_json(
                "/fba/inbound/v0/shipmentItems",
                params={"QueryType": "NEXT_TOKEN", "NextToken": next_token},
            )
        return {shipment_id: list(items.values()) for shipment_id, items in grouped.items()}

    def modern_inbound_shipments(self, marketplace_ids: list[str]) -> dict[str, dict[str, Any]]:
        enriched: dict[str, dict[str, Any]] = {}
        # Current EU FBA plans are created in the primary marketplace and include
        # the fulfillment-center routing for the whole inbound shipment.
        marketplace_id = _primary_inbound_marketplace(marketplace_ids)
        for marketplace_id in [marketplace_id] if marketplace_id else []:
            payload = self.request_json(
                "/inbound/fba/2024-03-20/inboundPlans",
                params={"marketplaceIds": marketplace_id},
            )
            plans = _as_list(payload.get("inboundPlans"))
            pagination = _as_dict(payload.get("pagination"))
            seen_tokens: set[str] = set()
            while plans:
                for plan_summary in plans:
                    plan_id = _text(_as_dict(plan_summary).get("inboundPlanId"))
                    if not plan_id:
                        continue
                    plan = self.request_json(f"/inbound/fba/2024-03-20/inboundPlans/{plan_id}")
                    for shipment_summary in _as_list(plan.get("shipments")):
                        shipment_id = _text(_as_dict(shipment_summary).get("shipmentId"))
                        if not shipment_id:
                            continue
                        try:
                            shipment = self.request_json(
                                f"/inbound/fba/2024-03-20/inboundPlans/{plan_id}/shipments/{shipment_id}"
                            )
                            items = self.request_json(
                                f"/inbound/fba/2024-03-20/inboundPlans/{plan_id}/shipments/{shipment_id}/items"
                            ).get("items", [])
                            boxes = self.request_json(
                                f"/inbound/fba/2024-03-20/inboundPlans/{plan_id}/shipments/{shipment_id}/boxes"
                            ).get("boxes", [])
                            options = self.request_json(
                                f"/inbound/fba/2024-03-20/inboundPlans/{plan_id}/transportationOptions",
                                params={"shipmentId": shipment_id},
                            ).get("transportationOptions", [])
                        except AmazonSpApiError:
                            continue
                        legacy_id = _text(shipment.get("shipmentConfirmationId"))
                        if legacy_id:
                            enriched[legacy_id] = {
                                "plan_id": plan_id,
                                "marketplace_id": marketplace_id,
                                "shipment": shipment,
                                "items": _as_list(items),
                                "boxes": _as_list(boxes),
                                "options": _as_list(options),
                            }
                next_token = _text(pagination.get("nextToken"))
                if not next_token or next_token in seen_tokens:
                    break
                seen_tokens.add(next_token)
                payload = self.request_json(
                    "/inbound/fba/2024-03-20/inboundPlans",
                    params={"marketplaceIds": marketplace_id, "paginationToken": next_token},
                )
                plans = _as_list(payload.get("inboundPlans"))
                pagination = _as_dict(payload.get("pagination"))
        return enriched

    def order_items(self, amazon_order_id: str) -> list[dict[str, Any]]:
        payload = self.request_json(f"/orders/v0/orders/{amazon_order_id}/orderItems")
        result: list[dict[str, Any]] = []
        while True:
            response_payload = _as_dict(payload.get("payload"))
            result.extend(_as_dict(item) for item in _as_list(response_payload.get("OrderItems")))
            next_token = _text(response_payload.get("NextToken"))
            if not next_token:
                break
            payload = self.request_json(f"/orders/v0/orders/{amazon_order_id}/orderItems", params={"NextToken": next_token})
        return result

    def catalog_item_images(self, asin: str, marketplace_id: str) -> dict[str, Any]:
        payload = self.request_json(
            f"/catalog/2022-04-01/items/{asin}",
            params={"marketplaceIds": marketplace_id, "includedData": "images"},
        )
        return extract_catalog_item_images(payload)

    def financial_events(self, posted_after: str) -> dict[str, Any]:
        payload = self.request_json("/finances/v0/financialEvents", params={"PostedAfter": posted_after})
        merged_events: dict[str, list[Any]] = {}
        while True:
            response_payload = _as_dict(payload.get("payload"))
            financial_events = _as_dict(response_payload.get("FinancialEvents"))
            for event_type, events in financial_events.items():
                if isinstance(events, list):
                    merged_events.setdefault(event_type, []).extend(events)
            next_token = _text(response_payload.get("NextToken"))
            if not next_token:
                break
            payload = self.request_json("/finances/v0/financialEvents", params={"NextToken": next_token})
        return {"payload": {"FinancialEvents": merged_events}}

    def financial_event_groups(self, started_after: str) -> list[dict[str, Any]]:
        payload = self.request_json(
            "/finances/v0/financialEventGroups",
            params={"FinancialEventGroupStartedAfter": started_after},
        )
        groups: list[dict[str, Any]] = []
        while True:
            response_payload = _as_dict(payload.get("payload"))
            groups.extend(_as_dict(item) for item in _as_list(response_payload.get("FinancialEventGroupList")))
            next_token = _text(response_payload.get("NextToken"))
            if not next_token:
                break
            payload = self.request_json("/finances/v0/financialEventGroups", params={"NextToken": next_token})
        return groups

    def financial_events_for_group(self, settlement_id: str) -> dict[str, Any]:
        return self.request_json(f"/finances/v0/financialEventGroups/{settlement_id}/financialEvents")

    def financial_transactions(self, posted_after: str, posted_before: str, marketplace_id: str) -> list[dict[str, Any]]:
        base_params = {
            "postedAfter": posted_after,
            "postedBefore": posted_before,
            "marketplaceId": marketplace_id,
            "pageSize": 100,
        }
        payload = self.request_json("/finances/2024-06-19/transactions", params=base_params)
        transactions: list[dict[str, Any]] = []
        seen_tokens: set[str] = set()
        while True:
            response_payload = _as_dict(payload.get("payload")) or payload
            transactions.extend(_as_dict(item) for item in _as_list(response_payload.get("transactions")))
            next_token = _text(response_payload.get("nextToken"))
            if not next_token or next_token in seen_tokens:
                break
            seen_tokens.add(next_token)
            payload = self.request_json(
                "/finances/2024-06-19/transactions",
                params={**base_params, "nextToken": next_token},
            )
        return transactions

    def create_settlement_report(self, marketplace_ids: list[str], started_after: str) -> str:
        payload = self.request_json(
            "/reports/2021-06-30/reports",
            method="POST",
            body={
                "reportType": "GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE",
                "marketplaceIds": marketplace_ids,
                "dataStartTime": started_after,
            },
        )
        report_id = _text(payload.get("reportId"))
        if not report_id:
            raise AmazonSpApiError("Reports API did not return reportId")
        return report_id

    def get_report(self, report_id: str) -> dict[str, Any]:
        return self.request_json(f"/reports/2021-06-30/reports/{report_id}")

    def get_report_document(self, document_id: str) -> dict[str, Any]:
        return self.request_json(f"/reports/2021-06-30/documents/{document_id}")

    def list_reports(self, report_type: str) -> list[dict[str, Any]]:
        payload = self.request_json(
            "/reports/2021-06-30/reports",
            params={"reportTypes": report_type, "pageSize": 100},
        )
        reports: list[dict[str, Any]] = []
        while True:
            reports.extend(_as_dict(item) for item in _as_list(payload.get("reports")))
            next_token = _text(payload.get("nextToken"))
            if not next_token:
                break
            payload = self.request_json("/reports/2021-06-30/reports", params={"nextToken": next_token, "pageSize": 100})
        return reports

    def download_report_text(self, url: str) -> str:
        try:
            with urlopen(url, timeout=90) as response:
                return response.read().decode("utf-8-sig")
        except (HTTPError, URLError, OSError, UnicodeDecodeError) as exc:
            raise AmazonSpApiError(f"report document download failed: {exc}") from exc


def _save_raw_record(connection: sqlite3.Connection, *, sync_run_id: str, resource_type: str, payload: Any, external_id: str = "") -> None:
    serialized = _json_dumps(payload)
    payload_hash = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
    connection.execute(
        """
        INSERT OR IGNORE INTO raw_records(id, sync_run_id, resource_type, external_id, payload_hash, payload_json, captured_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (_stable_id("raw", f"{resource_type}:{payload_hash}"), sync_run_id, resource_type, external_id, payload_hash, serialized, _utc_now()),
    )


def _upsert_marketplaces(connection: sqlite3.Connection, payload: dict[str, Any], *, active_only: bool = True) -> list[str]:
    result: list[str] = []
    for item in _as_list(payload.get("payload")):
        participation = _as_dict(item)
        marketplace = _as_dict(participation.get("marketplace"))
        marketplace_id = _text(marketplace.get("id"))
        # Seller participation also exposes non-marketplace service entries.
        # Inventory endpoints reject these with a misleading regional 403.
        if (
            not marketplace_id
            or _text(marketplace.get("name")).lower().startswith("non-amazon")
            or _text(marketplace.get("domainName")).lower() == "non-amazon"
        ):
            continue
        connection.execute(
            """
            INSERT INTO amazon_marketplaces(marketplace_id, name, country_code, domain_name, default_currency, participation_json, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(marketplace_id) DO UPDATE SET
                name=excluded.name, country_code=excluded.country_code, domain_name=excluded.domain_name,
                default_currency=excluded.default_currency, participation_json=excluded.participation_json, updated_at=excluded.updated_at
            """,
            (
                marketplace_id,
                _text(marketplace.get("name")),
                _text(marketplace.get("countryCode")),
                _text(marketplace.get("domainName")),
                _text(marketplace.get("defaultCurrencyCode")),
                _json_dumps(participation),
                _utc_now(),
            ),
        )
        is_participating = bool(_as_dict(participation.get("participation")).get("isParticipating"))
        if active_only and not is_participating:
            continue
        result.append(marketplace_id)
    return result


_VALID_MARKETPLACE_MODES = {"auto", "manual"}


def get_amazon_marketplace_settings() -> dict[str, Any]:
    init_amazon_fba_db()
    with _connect() as connection:
        connection.execute(
            "INSERT OR IGNORE INTO amazon_sync_settings(id, marketplace_mode, selected_marketplace_ids, updated_at) VALUES ('default', 'auto', '[]', ?)",
            (_utc_now(),),
        )
        connection.commit()
        row = connection.execute(
            "SELECT marketplace_mode, selected_marketplace_ids FROM amazon_sync_settings WHERE id = 'default'"
        ).fetchone()
    try:
        selected = json.loads(str(row["selected_marketplace_ids"]))
    except (TypeError, ValueError, json.JSONDecodeError):
        selected = []
    return {
        "marketplace_mode": str(row["marketplace_mode"]),
        "selected_marketplace_ids": [str(m) for m in selected] if isinstance(selected, list) else [],
    }


def set_amazon_marketplace_settings(*, marketplace_mode: str, selected_marketplace_ids: list[str]) -> dict[str, Any]:
    mode = str(marketplace_mode or "").strip().lower()
    if mode not in _VALID_MARKETPLACE_MODES:
        raise ValueError(f"marketplace_mode must be one of {sorted(_VALID_MARKETPLACE_MODES)}")
    normalized_ids = [str(m).strip() for m in selected_marketplace_ids if str(m).strip()]
    init_amazon_fba_db()
    with _connect() as connection:
        connection.execute(
            """
            INSERT INTO amazon_sync_settings(id, marketplace_mode, selected_marketplace_ids, updated_at)
            VALUES ('default', ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                marketplace_mode=excluded.marketplace_mode,
                selected_marketplace_ids=excluded.selected_marketplace_ids,
                updated_at=excluded.updated_at
            """,
            (mode, _json_dumps(normalized_ids), _utc_now()),
        )
        connection.commit()
    return {"marketplace_mode": mode, "selected_marketplace_ids": normalized_ids}


def _upsert_order(connection: sqlite3.Connection, order: dict[str, Any], *, synthetic: bool = False) -> None:
    order_id = _text(order.get("AmazonOrderId"))
    if not order_id:
        return
    total = _as_dict(order.get("OrderTotal"))
    connection.execute(
        """
        INSERT INTO amazon_orders(
            amazon_order_id, seller_order_id, marketplace_id, purchase_date, last_update_date,
            order_status, fulfillment_channel, sales_channel, currency, order_total_cents,
            buyer_name, buyer_email, is_synthetic, raw_json, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(amazon_order_id) DO UPDATE SET
            seller_order_id=CASE WHEN excluded.is_synthetic = 1 THEN amazon_orders.seller_order_id ELSE COALESCE(NULLIF(excluded.seller_order_id, ''), amazon_orders.seller_order_id) END,
            marketplace_id=CASE WHEN excluded.is_synthetic = 1 THEN amazon_orders.marketplace_id ELSE COALESCE(NULLIF(excluded.marketplace_id, ''), amazon_orders.marketplace_id) END,
            purchase_date=CASE WHEN excluded.is_synthetic = 1 THEN amazon_orders.purchase_date ELSE COALESCE(NULLIF(excluded.purchase_date, ''), amazon_orders.purchase_date) END,
            last_update_date=CASE WHEN excluded.is_synthetic = 1 THEN amazon_orders.last_update_date ELSE COALESCE(NULLIF(excluded.last_update_date, ''), amazon_orders.last_update_date) END,
            order_status=CASE WHEN excluded.is_synthetic = 1 THEN amazon_orders.order_status ELSE CASE WHEN excluded.order_status <> '' THEN excluded.order_status ELSE amazon_orders.order_status END END,
            fulfillment_channel=CASE WHEN excluded.is_synthetic = 1 THEN amazon_orders.fulfillment_channel ELSE CASE WHEN excluded.fulfillment_channel <> '' THEN excluded.fulfillment_channel ELSE amazon_orders.fulfillment_channel END END,
            sales_channel=CASE WHEN excluded.is_synthetic = 1 THEN amazon_orders.sales_channel ELSE CASE WHEN excluded.sales_channel <> '' THEN excluded.sales_channel ELSE amazon_orders.sales_channel END END,
            currency=CASE WHEN excluded.is_synthetic = 1 THEN amazon_orders.currency ELSE COALESCE(NULLIF(excluded.currency, ''), amazon_orders.currency) END,
            order_total_cents=CASE WHEN excluded.is_synthetic = 1 THEN amazon_orders.order_total_cents ELSE CASE WHEN excluded.order_total_cents <> 0 THEN excluded.order_total_cents ELSE amazon_orders.order_total_cents END END,
            buyer_name=CASE WHEN excluded.is_synthetic = 1 THEN amazon_orders.buyer_name ELSE CASE WHEN excluded.buyer_name <> '' THEN excluded.buyer_name ELSE amazon_orders.buyer_name END END,
            buyer_email=CASE WHEN excluded.is_synthetic = 1 THEN amazon_orders.buyer_email ELSE CASE WHEN excluded.buyer_email <> '' THEN excluded.buyer_email ELSE amazon_orders.buyer_email END END,
            is_synthetic=MIN(amazon_orders.is_synthetic, excluded.is_synthetic),
            raw_json=CASE WHEN excluded.is_synthetic = 1 AND amazon_orders.is_synthetic = 0 THEN amazon_orders.raw_json ELSE excluded.raw_json END,
            updated_at=excluded.updated_at
        """,
        (
            order_id,
            _text(order.get("SellerOrderId")),
            _text(order.get("MarketplaceId")),
            _text(order.get("PurchaseDate")),
            _text(order.get("LastUpdateDate")),
            _text(order.get("OrderStatus")),
            _text(order.get("FulfillmentChannel")),
            _text(order.get("SalesChannel")),
            _currency(total, _currency(order.get("CurrencyCode"))),
            _amount_cents(total),
            _text(order.get("BuyerInfo", {}).get("BuyerName") if isinstance(order.get("BuyerInfo"), dict) else order.get("BuyerName")),
            _text(order.get("BuyerInfo", {}).get("BuyerEmail") if isinstance(order.get("BuyerInfo"), dict) else order.get("BuyerEmail")),
            1 if synthetic else 0,
            _json_dumps(order),
            _utc_now(),
        ),
    )


def _upsert_order_items(
    connection: sqlite3.Connection,
    amazon_order_id: str,
    items: list[dict[str, Any]],
    image_map: Optional[dict[str, dict[str, Any]]] = None,
) -> None:
    image_map = image_map or {}
    for item in items:
        asin = _text(item.get("ASIN"))
        seller_sku = _text(item.get("SellerSKU"))
        if not asin and not seller_sku:
            continue
        item_id = _stable_id("amazon-order-item", f"{amazon_order_id}:{asin}:{seller_sku}")
        item_price = _as_dict(item.get("ItemPrice"))
        item_tax = _as_dict(item.get("ItemTax"))
        image_payload = image_map.get(asin, {})
        image_url = _text(item.get("image_url") or image_payload.get("image_url"))
        image_urls = item.get("image_urls") if isinstance(item.get("image_urls"), list) else image_payload.get("image_urls", [])
        connection.execute(
            """
            INSERT INTO amazon_order_items(id, amazon_order_id, asin, seller_sku, title, quantity_ordered, quantity_shipped, currency, item_price_cents, item_tax_cents, image_url, image_urls_json, raw_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(amazon_order_id, asin, seller_sku) DO UPDATE SET
                title=excluded.title, quantity_ordered=excluded.quantity_ordered, quantity_shipped=excluded.quantity_shipped,
                currency=excluded.currency, item_price_cents=excluded.item_price_cents, item_tax_cents=excluded.item_tax_cents,
                image_url=CASE WHEN excluded.image_url <> '' THEN excluded.image_url ELSE amazon_order_items.image_url END,
                image_urls_json=CASE WHEN excluded.image_urls_json <> '[]' THEN excluded.image_urls_json ELSE amazon_order_items.image_urls_json END,
                raw_json=excluded.raw_json
            """,
            (
                item_id, amazon_order_id, asin, seller_sku, _text(item.get("Title")), _quantity(item.get("QuantityOrdered")),
                _quantity(item.get("QuantityShipped")), _currency(item_price), _amount_cents(item_price), _amount_cents(item_tax),
                image_url, _json_dumps(image_urls), _json_dumps(item),
            ),
        )


def _upsert_inventory_snapshot(connection: sqlite3.Connection, *, marketplace_id: str, item: dict[str, Any]) -> None:
    details = _as_dict(item.get("inventoryDetails"))
    reserved = details.get("reservedQuantity")
    reserved_total = _as_dict(reserved).get("totalReservedQuantity") if isinstance(reserved, dict) else reserved
    snapshot_id = _stable_id("inventory", f"{_utc_now()}:{marketplace_id}:{_text(item.get('fnSku'))}:{_text(item.get('sellerSku'))}")
    connection.execute(
        """
        INSERT INTO amazon_inventory_snapshots(
            id, captured_at, marketplace_id, seller_sku, fnsku, asin, product_name,
            fulfillable_quantity, inbound_working_quantity, inbound_shipped_quantity,
            reserved_quantity, unfulfillable_quantity, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            snapshot_id, _utc_now(), marketplace_id, _text(item.get("sellerSku")), _text(item.get("fnSku")),
            _text(item.get("asin")), _text(item.get("productName")),
            _quantity(details.get("fulfillableQuantity")), _quantity(details.get("inboundWorkingQuantity")),
            _quantity(details.get("inboundShippedQuantity")), _quantity(reserved_total),
            _quantity(details.get("unfulfillableQuantity")), _json_dumps(item),
        ),
    )


def _upsert_inbound_shipment(
    connection: sqlite3.Connection,
    *,
    shipment: dict[str, Any],
    items: Iterable[dict[str, Any]],
    modern: Optional[dict[str, Any]] = None,
) -> None:
    shipment_id = _text(shipment.get("ShipmentId"))
    if not shipment_id:
        return
    modern_payload = _as_dict(modern)
    modern_shipment = _as_dict(modern_payload.get("shipment"))
    status = _text(modern_shipment.get("status") or shipment.get("ShipmentStatus"))
    destination = _as_dict(modern_shipment.get("destination"))
    destination_warehouse = _text(
        destination.get("warehouseId") or shipment.get("DestinationFulfillmentCenterId")
    )
    raw_payload = {"legacy": shipment, "modern": modern_shipment} if modern_shipment else {"legacy": shipment}
    updated_at = _utc_now()
    inventory_eligible_at = updated_at if normalize_fba_status(status)["inventory_eligible"] else None
    connection.execute(
        """
        INSERT INTO amazon_inbound_shipments(
            id, shipment_id, plan_id, shipment_name, status,
            destination_fulfillment_center_id, raw_json, inventory_eligible_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(shipment_id) DO UPDATE SET
            plan_id=CASE WHEN excluded.plan_id <> '' THEN excluded.plan_id ELSE amazon_inbound_shipments.plan_id END,
            shipment_name=CASE WHEN excluded.shipment_name <> '' THEN excluded.shipment_name ELSE amazon_inbound_shipments.shipment_name END,
            status=CASE WHEN excluded.status <> '' THEN excluded.status ELSE amazon_inbound_shipments.status END,
            destination_fulfillment_center_id=CASE WHEN excluded.destination_fulfillment_center_id <> '' THEN excluded.destination_fulfillment_center_id ELSE amazon_inbound_shipments.destination_fulfillment_center_id END,
            raw_json=excluded.raw_json,
            inventory_eligible_at=CASE
                WHEN amazon_inbound_shipments.inventory_eligible_at IS NOT NULL THEN amazon_inbound_shipments.inventory_eligible_at
                WHEN excluded.inventory_eligible_at IS NOT NULL THEN excluded.inventory_eligible_at
                ELSE NULL
            END,
            updated_at=excluded.updated_at
        """,
        (
            _stable_id("amazon-inbound-shipment", shipment_id),
            shipment_id,
            _text(modern_payload.get("plan_id")),
            _text(modern_shipment.get("name") or shipment.get("ShipmentName")),
            status,
            destination_warehouse,
            _json_dumps(raw_payload),
            inventory_eligible_at,
            updated_at,
        ),
    )

    normalized_items = normalize_shipment_items(items)
    if normalized_items:
        connection.execute("DELETE FROM amazon_inbound_shipment_items WHERE shipment_id = ?", (shipment_id,))
        for item in normalized_items:
            connection.execute(
                """
                INSERT INTO amazon_inbound_shipment_items(
                    id, shipment_id, seller_sku, fnsku, asin, title,
                    quantity_shipped, quantity_received, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    _stable_id("amazon-inbound-item", f"{shipment_id}:{item['seller_sku']}:{item['fnsku']}"),
                    shipment_id,
                    _text(item.get("seller_sku")),
                    _text(item.get("fnsku")),
                    _text(item.get("asin")),
                    _text(item.get("title")),
                    _quantity(item.get("quantity_shipped")),
                    _quantity(item.get("quantity_received")),
                    _json_dumps(item),
                ),
            )

    boxes = _as_list(modern_payload.get("boxes"))
    if boxes:
        connection.execute("DELETE FROM amazon_inbound_shipment_boxes WHERE shipment_id = ?", (shipment_id,))
        for index, box in enumerate(boxes):
            payload = _as_dict(box)
            weight = _as_dict(payload.get("weight"))
            dimensions = _as_dict(payload.get("dimensions"))
            box_id = _text(payload.get("boxId") or payload.get("packageId")) or f"box-{index + 1}"
            connection.execute(
                """
                INSERT INTO amazon_inbound_shipment_boxes(
                    id, shipment_id, box_id, weight_value, weight_unit,
                    length, width, height, dimension_unit, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    _stable_id("amazon-inbound-box", f"{shipment_id}:{box_id}"),
                    shipment_id,
                    box_id,
                    weight.get("value"),
                    _text(weight.get("unit")),
                    dimensions.get("length"),
                    dimensions.get("width"),
                    dimensions.get("height"),
                    _text(dimensions.get("unitOfMeasurement")),
                    _json_dumps(payload),
                ),
            )

    options = _as_list(modern_payload.get("options"))
    if options:
        connection.execute("DELETE FROM amazon_inbound_transport_options WHERE shipment_id = ?", (shipment_id,))
        selected_id = _text(modern_shipment.get("selectedTransportationOptionId"))
        for option in options:
            payload = _as_dict(option)
            quote = _as_dict(payload.get("quote")).get("cost")
            quote_payload = _as_dict(quote)
            option_id = _text(payload.get("transportationOptionId"))
            connection.execute(
                """
                INSERT INTO amazon_inbound_transport_options(
                    id, shipment_id, option_id, carrier, shipping_solution,
                    shipping_mode, quote_cents, currency, selected, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    _stable_id("amazon-inbound-transport", f"{shipment_id}:{option_id}"),
                    shipment_id,
                    option_id,
                    _text(_as_dict(payload.get("carrier")).get("name")),
                    _text(payload.get("shippingSolution")),
                    _text(payload.get("shippingMode")),
                    _amount_cents(quote_payload.get("amount")) if quote_payload else None,
                    _currency(quote_payload.get("code")) if quote_payload else "EUR",
                    int(option_id == selected_id),
                    _json_dumps(payload),
                ),
            )


def _event_components(event: dict[str, Any]) -> list[tuple[str, int, str, dict[str, Any]]]:
    components: list[tuple[str, int, str, dict[str, Any]]] = []
    for item in _as_list(event.get("ShipmentItemList")):
        for key in ("ItemChargeList", "ItemFeeList", "PromotionList", "ItemFeeAdjustmentList"):
            for component in _as_list(_as_dict(item).get(key)):
                payload = _as_dict(component)
                amount = _as_dict(payload.get("Amount"))
                components.append((key, _amount_cents(amount), _currency(amount), payload))
    for key in ("FeeList", "ChargeList", "AdjustmentItemList"):
        for component in _as_list(event.get(key)):
            payload = _as_dict(component)
            amount = _as_dict(
                payload.get("Amount", payload.get("FeeAmount", payload.get("ChargeAmount", payload.get("RecoveryAmount"))))
            )
            components.append((key, _amount_cents(amount), _currency(amount), payload))
    recovery_amount = _as_dict(event.get("RecoveryAmount"))
    if recovery_amount:
        components.append(("RecoveryAmount", _amount_cents(recovery_amount), _currency(recovery_amount), event))
    return components


def _iter_financial_events(payload: dict[str, Any]) -> Iterable[tuple[str, dict[str, Any]]]:
    financial_events = _as_dict(_as_dict(payload.get("payload")).get("FinancialEvents"))
    for event_type, events in financial_events.items():
        if not event_type.endswith("EventList"):
            continue
        for event in _as_list(events):
            if isinstance(event, dict):
                yield event_type, event


def _upsert_financial_event(connection: sqlite3.Connection, event_type: str, event: dict[str, Any]) -> None:
    order_id = _text(event.get("AmazonOrderId"))
    posted_date = _text(event.get("PostedDate"))
    settlement_id = _text(event.get("FinancialEventGroupId"))
    event_identity = dict(event)
    event_identity.pop("FinancialEventGroupId", None)
    event_id = _stable_id("financial-event", f"{event_type}:{_payload_hash(event_identity)}")
    components = _event_components(event)
    component_sum = sum(item[1] for item in components)
    sales_cents = sum(amount for component_type, amount, _currency_code, _raw in components if component_type in {"ItemChargeList", "ChargeList", "PromotionList"})
    fees_cents = abs(sum(amount for component_type, amount, _currency_code, _raw in components if "Fee" in component_type))
    net_cents = component_sum if components else _amount_cents(event.get("Amount"))
    currency = next((code for _kind, _amount, code, _raw in components if code), "EUR")

    if order_id:
        _upsert_order(connection, {"AmazonOrderId": order_id, "PurchaseDate": posted_date, "OrderStatus": "financial_event"}, synthetic=True)
    connection.execute(
        """
        INSERT INTO amazon_financial_events(id, event_type, amazon_order_id, settlement_id, posted_date, financial_finality, currency, sales_cents, fees_cents, net_cents, raw_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            settlement_id=excluded.settlement_id, posted_date=excluded.posted_date,
            currency=excluded.currency, sales_cents=excluded.sales_cents, fees_cents=excluded.fees_cents,
            net_cents=excluded.net_cents, raw_json=excluded.raw_json
        """,
        (event_id, event_type, order_id or None, settlement_id or None, posted_date or None, "released" if settlement_id else "deferred", currency, sales_cents, fees_cents, net_cents, _json_dumps(event)),
    )
    connection.execute("DELETE FROM amazon_financial_components WHERE event_id = ?", (event_id,))
    for index, (component_type, amount, component_currency, raw_component) in enumerate(components):
        connection.execute(
            "INSERT INTO amazon_financial_components(id, event_id, component_type, amount_cents, currency, raw_json) VALUES (?, ?, ?, ?, ?, ?)",
            (_stable_id("financial-component", f"{event_id}:{index}"), event_id, component_type, amount, component_currency, _json_dumps(raw_component)),
        )


def sync_inbound_finance_costs() -> int:
    init_amazon_fba_db()
    imported = 0
    with _connect() as connection:
        rows = connection.execute(
            "SELECT event_id, amount_cents, currency, raw_json FROM amazon_financial_components WHERE amount_cents <> 0"
        ).fetchall()
        for row in rows:
            try:
                raw = _as_dict(json.loads(row["raw_json"] or "{}"))
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            cost_type = _text(raw.get("FeeType") or raw.get("ChargeType"))
            if "fbainboundtransport" not in cost_type.lower():
                continue
            connection.execute(
                """
                INSERT INTO amazon_inbound_costs(
                    id, shipment_id, source_event_id, cost_type, amount_cents,
                    currency, status, allocation_method, raw_json
                ) VALUES (?, NULL, ?, ?, ?, ?, 'unassigned', 'value', ?)
                ON CONFLICT(source_event_id, cost_type) DO UPDATE SET
                    amount_cents=excluded.amount_cents,
                    currency=excluded.currency,
                    raw_json=excluded.raw_json
                """,
                (
                    _stable_id("amazon-inbound-finance-cost", f"{row['event_id']}:{cost_type}"),
                    str(row["event_id"]),
                    cost_type,
                    abs(int(row["amount_cents"] or 0)),
                    _currency(row["currency"]),
                    str(row["raw_json"] or "{}"),
                ),
            )
            imported += 1
        connection.commit()
    return imported


def sync_modern_inbound_costs(transactions: Iterable[dict[str, Any]]) -> int:
    init_amazon_fba_db()
    imported = 0
    with _connect() as connection:
        for cost in extract_modern_inbound_costs(transactions):
            shipment_id = _text(cost.get("shipment_id"))
            linked = bool(connection.execute(
                "SELECT 1 FROM amazon_inbound_shipments WHERE shipment_id = ?",
                (shipment_id,),
            ).fetchone())
            status = "actual" if linked else "unassigned"
            connection.execute(
                """
                INSERT INTO amazon_inbound_costs(
                    id, shipment_id, source_event_id, cost_type, amount_cents,
                    currency, status, allocation_method, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'value', ?)
                ON CONFLICT(source_event_id, cost_type) DO UPDATE SET
                    shipment_id=excluded.shipment_id,
                    amount_cents=excluded.amount_cents,
                    currency=excluded.currency,
                    status=excluded.status,
                    raw_json=excluded.raw_json
                """,
                (
                    _stable_id("amazon-modern-inbound-cost", f"{cost['source_event_id']}:{cost['cost_type']}"),
                    shipment_id if linked else None,
                    cost["source_event_id"],
                    cost["cost_type"],
                    cost["amount_cents"],
                    cost["currency"],
                    status,
                    cost["raw_json"],
                ),
            )
            if linked:
                connection.execute(
                    """
                    UPDATE amazon_inbound_costs
                    SET status = 'superseded'
                    WHERE shipment_id IS NULL AND status = 'unassigned'
                      AND cost_type = ? AND amount_cents = ? AND currency = ?
                    """,
                    (cost["cost_type"], cost["amount_cents"], cost["currency"]),
                )
            imported += 1
        connection.commit()
    return imported


def sync_modern_financial_transactions(transactions: Iterable[dict[str, Any]]) -> int:
    """Persist modern order transactions so order details use released/deferred finance data."""
    init_amazon_fba_db()
    imported = 0
    with _connect() as connection:
        for transaction in transactions:
            payload = _as_dict(transaction)
            transaction_id = _text(payload.get("transactionId"))
            if not transaction_id:
                continue
            identifiers = {
                _text(_as_dict(identifier).get("relatedIdentifierName")): _text(
                    _as_dict(identifier).get("relatedIdentifierValue")
                )
                for identifier in _as_list(payload.get("relatedIdentifiers"))
            }
            order_id = identifiers.get("ORDER_ID", "")
            if not order_id:
                continue
            if connection.execute(
                "SELECT 1 FROM amazon_orders WHERE amazon_order_id = ?", (order_id,)
            ).fetchone() is None:
                _upsert_order(
                    connection,
                    {
                        "AmazonOrderId": order_id,
                        "MarketplaceId": _text(_as_dict(payload.get("sellingPartnerMetadata")).get("marketplaceId")),
                        "PurchaseDate": _text(payload.get("postedDate")),
                        "OrderStatus": "financial_event",
                    },
                    synthetic=True,
                )

            sales_cents = 0
            expense_cents = 0
            currency = _currency(_as_dict(payload.get("totalAmount")))
            for breakdown in _as_list(payload.get("breakdowns")):
                breakdown_payload = _as_dict(breakdown)
                breakdown_type = _text(breakdown_payload.get("breakdownType")).lower()
                amount = _as_dict(breakdown_payload.get("breakdownAmount"))
                if breakdown_type == "sales":
                    sales_cents += _amount_cents(amount.get("currencyAmount"))
                elif breakdown_type == "expenses":
                    expense_cents += abs(_amount_cents(amount.get("currencyAmount")))
                currency = _currency(amount.get("currencyCode"), currency)

            status = _text(payload.get("transactionStatus")).upper()
            finality = "released" if status == "RELEASED" else "deferred" if status == "DEFERRED" else "pending"
            event_id = _stable_id("amazon-modern-finance", transaction_id)
            connection.execute(
                """
                INSERT INTO amazon_financial_events(
                    id, event_type, amazon_order_id, settlement_id, posted_date,
                    financial_finality, currency, sales_cents, fees_cents, net_cents, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    event_type=excluded.event_type, amazon_order_id=excluded.amazon_order_id,
                    settlement_id=excluded.settlement_id, posted_date=excluded.posted_date,
                    financial_finality=excluded.financial_finality, currency=excluded.currency,
                    sales_cents=excluded.sales_cents, fees_cents=excluded.fees_cents,
                    net_cents=excluded.net_cents, raw_json=excluded.raw_json
                """,
                (
                    event_id,
                    f"ModernTransaction:{_text(payload.get('transactionType')) or 'Unknown'}",
                    order_id,
                    identifiers.get("SETTLEMENT_ID") or None,
                    _text(payload.get("postedDate")) or None,
                    finality,
                    currency,
                    sales_cents,
                    expense_cents,
                    _amount_cents(_as_dict(payload.get("totalAmount")).get("currencyAmount")),
                    _json_dumps(payload),
                ),
            )
            breakdown = extract_modern_financial_breakdown(payload)
            connection.execute("DELETE FROM amazon_financial_components WHERE event_id = ?", (event_id,))
            normalized_components = [
                ("Sales", int(breakdown["sales_cents"]), {"ChargeType": "Sales"}),
                ("Tax", int(breakdown["tax_cents"]), {"ChargeType": "Tax"}),
                *[
                    (str(fee["type"]), int(fee["amount_cents"]), {"FeeType": str(fee["type"])})
                    for fee in breakdown["fees"]
                ],
            ]
            for index, (component_type, amount_cents, raw_component) in enumerate(normalized_components):
                if not amount_cents:
                    continue
                connection.execute(
                    "INSERT INTO amazon_financial_components(id, event_id, component_type, amount_cents, currency, raw_json) VALUES (?, ?, ?, ?, ?, ?)",
                    (_stable_id("financial-component", f"{event_id}:{index}"), event_id, component_type, amount_cents, currency, _json_dumps(raw_component)),
                )
            imported += 1
        connection.commit()
    return imported


def _upsert_settlement(connection: sqlite3.Connection, settlement: dict[str, Any]) -> str:
    settlement_id = _text(settlement.get("FinancialEventGroupId"))
    if not settlement_id:
        return ""
    original_total = _as_dict(settlement.get("OriginalTotal"))
    connection.execute(
        """
        INSERT INTO amazon_settlements(settlement_id, processing_status, original_total_cents, currency, fund_transfer_date, raw_json, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(settlement_id) DO UPDATE SET
            processing_status=excluded.processing_status, original_total_cents=excluded.original_total_cents,
            currency=excluded.currency, fund_transfer_date=excluded.fund_transfer_date,
            raw_json=excluded.raw_json, updated_at=excluded.updated_at
        """,
        (
            settlement_id, _text(settlement.get("ProcessingStatus")), _amount_cents(original_total), _currency(original_total),
            _text(settlement.get("FundTransferDate")) or None, _json_dumps(settlement), _utc_now(),
        ),
    )
    return settlement_id


def _import_settlement_report_row(connection: sqlite3.Connection, report_id: str, row: dict[str, str]) -> bool:
    order_id = _text(row.get("order-id"))
    transaction_type = _text(row.get("transaction-type"))
    if transaction_type.lower() == "payable to amazon":
        return False
    total_cents = _amount_cents(row.get("total-amount"))
    price_cents = _amount_cents(row.get("price-amount"))
    price_type = _text(row.get("price-type")).lower()
    fee_fields = (
        ("shipment_fee", "shipment-fee-amount"),
        ("order_fee", "order-fee-amount"),
        ("item_related_fee", "item-related-fee-amount"),
        ("misc_fee", "misc-fee-amount"),
        ("other_fee", "other-fee-amount"),
        ("other", "other-amount"),
    )
    fee_components = [
        (component_type, _amount_cents(row.get(column_name)))
        for component_type, column_name in fee_fields
        if _text(row.get(column_name))
    ]
    if not order_id and not transaction_type and not fee_components:
        return False
    posted_date = _text(row.get("posted-date"))
    merchant_order_id = _text(row.get("merchant-order-id"))
    currency = _text(row.get("currency")).upper() or "EUR"
    sales_cents = price_cents if price_type in {"principal", "shipping", "giftwrap", "promotion"} else 0
    if transaction_type.lower() in {"order", "refund"} and sales_cents == 0 and price_cents:
        sales_cents = price_cents
    fees_cents = sum(abs(amount) for _component_type, amount in fee_components if amount < 0)
    signed_components_cents = sum(amount for _component_type, amount in fee_components)
    if total_cents == 0 and (sales_cents or signed_components_cents):
        total_cents = sales_cents + signed_components_cents
    descriptor = " ".join(
        [transaction_type, _text(row.get("shipment-fee-type")), _text(row.get("order-fee-type")), _text(row.get("item-related-fee-type"))]
    ).lower()
    # The same settlement line can appear in overlapping report exports.
    # Use the line identity, not the report job ID, for idempotent imports.
    event_id = _stable_id("settlement-report-line", _payload_hash(row))

    _upsert_order(
        connection,
        {
            "AmazonOrderId": order_id,
            "SellerOrderId": merchant_order_id,
            "PurchaseDate": posted_date,
            "OrderStatus": "settlement_report",
            "FulfillmentChannel": "AFN" if "fba" in descriptor else "",
            "MarketplaceId": _text(row.get("marketplace-name")),
            "CurrencyCode": currency,
        },
        synthetic=True,
    )
    connection.execute(
        """
        INSERT INTO amazon_financial_events(id, event_type, amazon_order_id, settlement_id, posted_date, financial_finality, currency, sales_cents, fees_cents, net_cents, raw_json)
        VALUES (?, 'SettlementReportLine', ?, ?, ?, 'released', ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            sales_cents=excluded.sales_cents, fees_cents=excluded.fees_cents, net_cents=excluded.net_cents, raw_json=excluded.raw_json
        """,
        (event_id, order_id or None, _text(row.get("settlement-id")) or report_id, posted_date or None, currency, sales_cents, fees_cents, total_cents, _json_dumps(row)),
    )
    connection.execute("DELETE FROM amazon_financial_components WHERE event_id = ?", (event_id,))
    for index, (component_type, amount) in enumerate(fee_components):
        connection.execute(
            "INSERT INTO amazon_financial_components(id, event_id, component_type, amount_cents, currency, raw_json) VALUES (?, ?, ?, ?, ?, ?)",
            (_stable_id("settlement-component", f"{event_id}:{index}"), event_id, component_type, amount, currency, _json_dumps({"transaction_type": transaction_type, "amount": row.get(next(column for kind, column in fee_fields if kind == component_type), "")})),
        )
    return True


def _normalize_settlement_report_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    currencies: dict[str, str] = {}
    for row in rows:
        settlement_id = _text(row.get("settlement-id"))
        currency = _text(row.get("currency")).upper()
        if settlement_id and currency:
            currencies[settlement_id] = currency
    normalized: list[dict[str, str]] = []
    for row in rows:
        copy = dict(row)
        if not _text(copy.get("currency")):
            copy["currency"] = currencies.get(_text(copy.get("settlement-id")), "EUR")
        normalized.append(copy)
    return normalized


def sync_inbound_shipments(
    client: AmazonSpApiClient,
    marketplaces: list[str],
    sync_run_id: str,
    *,
    item_lookback_days: int = 730,
) -> dict[str, Any]:
    shipments = client.inbound_shipments(marketplaces)
    errors: list[dict[str, str]] = []
    legacy_items_by_shipment: dict[str, list[dict[str, Any]]] = {}
    bulk_items = getattr(client, "bulk_inbound_shipment_items", None)
    if callable(bulk_items) and marketplaces:
        try:
            legacy_items_by_shipment = bulk_items(
                _primary_inbound_marketplace(marketplaces), lookback_days=max(1, item_lookback_days)
            )
        except AmazonSpApiError as exc:
            errors.append({"scope": "inbound_items_bulk", "error": str(exc)})
    else:
        for shipment in shipments:
            shipment_id = _text(shipment.get("ShipmentId"))
            if not shipment_id:
                continue
            try:
                legacy_items_by_shipment[shipment_id] = client.shipment_items(shipment_id)
            except AmazonSpApiError as exc:
                legacy_items_by_shipment[shipment_id] = []
                errors.append({"scope": "inbound_items", "shipment_id": shipment_id, "error": str(exc)})
    modern_by_shipment: dict[str, dict[str, Any]] = {}
    try:
        modern_by_shipment = client.modern_inbound_shipments(marketplaces)
    except AmazonSpApiError as exc:
        errors.append({"scope": "modern_inbound", "error": str(exc)})

    item_count = 0
    with _connect() as connection:
        for shipment in shipments:
            shipment_id = _text(shipment.get("ShipmentId"))
            if not shipment_id:
                continue
            _save_raw_record(
                connection,
                sync_run_id=sync_run_id,
                resource_type="inbound_shipment",
                payload=shipment,
                external_id=shipment_id,
            )
            legacy_items = legacy_items_by_shipment.get(shipment_id, [])
            modern = modern_by_shipment.get(shipment_id)
            selected_items = _as_list(_as_dict(modern).get("items")) if modern else legacy_items
            if modern and not selected_items:
                selected_items = legacy_items
            normalized_items = normalize_shipment_items(selected_items)
            item_count += len(normalized_items)
            _upsert_inbound_shipment(connection, shipment=shipment, items=selected_items, modern=modern)
            if modern:
                _save_raw_record(
                    connection,
                    sync_run_id=sync_run_id,
                    resource_type="inbound_shipment_modern",
                    payload=modern,
                    external_id=shipment_id,
                )
        connection.commit()
    return {"shipments": len(shipments), "items": item_count, "errors": errors}


def request_settlement_report(*, lookback_days: int = 730) -> dict[str, Any]:
    init_amazon_fba_db()
    config, missing = load_amazon_sp_api_config()
    if config is None:
        return {"status": "skipped", "missing": missing}
    with _connect() as connection:
        marketplaces = [str(row[0]) for row in connection.execute("SELECT marketplace_id FROM amazon_marketplaces ORDER BY marketplace_id").fetchall()]
    if not marketplaces:
        return {"status": "skipped", "reason": "run an Amazon sync before requesting a report"}
    since = (datetime.now(timezone.utc) - timedelta(days=max(1, int(lookback_days)))).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    report_id = AmazonSpApiClient(config).create_settlement_report(marketplaces, since)
    with _connect() as connection:
        connection.execute(
            "INSERT OR REPLACE INTO amazon_report_jobs(report_id, report_type, processing_status, requested_at, raw_json) VALUES (?, 'GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE', 'IN_QUEUE', ?, '{}')",
            (report_id, _utc_now()),
        )
        connection.commit()
    return {"status": "requested", "report_id": report_id, "lookback_days": max(1, int(lookback_days))}


def import_settlement_report(report_id: str) -> dict[str, Any]:
    init_amazon_fba_db()
    config, missing = load_amazon_sp_api_config()
    if config is None:
        return {"status": "skipped", "missing": missing}
    client = AmazonSpApiClient(config)
    report = client.get_report(report_id)
    status = _text(report.get("processingStatus"))
    document_id = _text(report.get("reportDocumentId"))
    with _connect() as connection:
        connection.execute(
            "UPDATE amazon_report_jobs SET processing_status = ?, report_document_id = ?, completed_at = ?, raw_json = ? WHERE report_id = ?",
            (status, document_id or None, _utc_now() if status in {"DONE", "_DONE_"} else None, _json_dumps(report), report_id),
        )
        connection.commit()
    if status not in {"DONE", "_DONE_"} or not document_id:
        return {"status": status.lower() or "pending", "report_id": report_id}
    document = client.get_report_document(document_id)
    document_url = _text(document.get("url"))
    if not document_url:
        raise AmazonSpApiError("settlement report document did not include a download URL")
    rows = _normalize_settlement_report_rows([
        {str(key): str(value or "") for key, value in row.items() if key}
        for row in csv.DictReader(io.StringIO(client.download_report_text(document_url)), delimiter="\t")
    ])
    imported_rows = 0
    with _connect() as connection:
        for row in rows:
            if _import_settlement_report_row(connection, report_id, row):
                imported_rows += 1
        connection.execute("UPDATE amazon_report_jobs SET imported_at = ? WHERE report_id = ?", (_utc_now(), report_id))
        connection.commit()
    return {"status": "imported", "report_id": report_id, "order_financial_rows": imported_rows}


def build_amazon_fba_status() -> dict[str, Any]:
    config, missing = load_amazon_sp_api_config()
    payload: dict[str, Any] = {
        "configured": config is not None,
        "missing": missing,
        "database": {"path": str(AMAZON_FBA_DB_PATH), "exists": AMAZON_FBA_DB_PATH.exists()},
    }
    if not AMAZON_FBA_DB_PATH.exists():
        return payload
    try:
        with _connect() as connection:
            payload["counts"] = {
                "orders": int(connection.execute("SELECT COUNT(*) FROM amazon_orders").fetchone()[0]),
                "financial_events": int(connection.execute("SELECT COUNT(*) FROM amazon_financial_events").fetchone()[0]),
                "inventory_snapshots": int(connection.execute("SELECT COUNT(*) FROM amazon_inventory_snapshots").fetchone()[0]),
                "inbound_shipments": int(connection.execute("SELECT COUNT(*) FROM amazon_inbound_shipments").fetchone()[0]),
                "procurement_batches": int(connection.execute("SELECT COUNT(*) FROM procurement_batches").fetchone()[0]),
            }
            row = connection.execute("SELECT completed_at, status, summary_json, error_message FROM sync_runs ORDER BY started_at DESC LIMIT 1").fetchone()
            if row:
                payload["last_sync"] = dict(row)
    except sqlite3.Error as exc:
        payload["database_error"] = str(exc)
    return payload


def sync_amazon_fba(
    *,
    include_orders: bool = True,
    include_inventory: bool = True,
    include_finances: bool = True,
    include_inbound: bool = True,
    include_settlement_reports: bool = True,
    include_catalog_images: bool = True,
    include_all_marketplaces: bool = False,
    lookback_days: int = DEFAULT_ORDER_LOOKBACK_DAYS,
    lookback_minutes: Optional[int] = None,
) -> dict[str, Any]:
    init_amazon_fba_db()
    config, missing = load_amazon_sp_api_config()
    if config is None:
        return {"status": "skipped", "reason": "Amazon SP-API credentials are not configured", "missing": missing}

    scopes = {
        "orders": include_orders,
        "inventory": include_inventory,
        "finances": include_finances,
        "inbound": include_inbound,
        "settlement_reports": include_settlement_reports,
        "catalog_images": include_catalog_images,
        "include_all_marketplaces": include_all_marketplaces,
        "lookback_days": max(1, int(lookback_days)),
        "lookback_minutes": int(lookback_minutes) if lookback_minutes is not None else None,
    }
    sync_run_id = _stable_id("sync-run", f"{_utc_now()}:{_payload_hash(scopes)}")
    summary: dict[str, Any] = {
        "status": "success",
        "sync_run_id": sync_run_id,
        "marketplaces": 0,
        "orders": 0,
        "inventory_items": 0,
        "inbound_shipments": 0,
        "inbound_items": 0,
        "inbound_costs": 0,
        "modern_transactions": 0,
        "modern_order_events": 0,
        "financial_events": 0,
        "settlements": 0,
        "settlement_reports": 0,
        "settlement_report_rows": 0,
        "errors": [],
        "rate_limits": {},
    }
    client = AmazonSpApiClient(config)
    marketplace_sync_settings = get_amazon_marketplace_settings()
    try:
        with _connect() as connection:
            connection.execute("INSERT INTO sync_runs(id, started_at, status, requested_scopes_json) VALUES (?, ?, 'running', ?)", (sync_run_id, _utc_now(), _json_dumps(scopes)))
            participations = client.marketplace_participations()
            _save_raw_record(connection, sync_run_id=sync_run_id, resource_type="marketplace_participations", payload=participations)
            all_marketplaces = _upsert_marketplaces(connection, participations, active_only=False)
            if include_all_marketplaces:
                marketplaces = all_marketplaces
            elif marketplace_sync_settings["marketplace_mode"] == "manual":
                allowed = set(marketplace_sync_settings["selected_marketplace_ids"])
                marketplaces = [m for m in all_marketplaces if m in allowed]
            else:
                marketplaces = _upsert_marketplaces(connection, participations, active_only=True)
            summary["marketplaces"] = len(marketplaces)
            connection.commit()

        lookback = timedelta(minutes=max(1, int(lookback_minutes))) if lookback_minutes is not None else timedelta(days=max(1, int(lookback_days)))
        since = (datetime.now(timezone.utc) - lookback).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        if include_orders:
            orders, order_errors = client.orders(marketplaces, since, updated_after=since if lookback_minutes is not None else None)
            summary["errors"].extend({"scope": "orders", **err} for err in order_errors)
            with _connect() as connection:
                for order in orders:
                    _save_raw_record(connection, sync_run_id=sync_run_id, resource_type="order", payload=order, external_id=_text(order.get("AmazonOrderId")))
                    _upsert_order(connection, order)
                connection.commit()
            for order in orders:
                order_id = _text(order.get("AmazonOrderId"))
                if not order_id:
                    continue
                try:
                    items = client.order_items(order_id)
                except AmazonSpApiError as exc:
                    summary["errors"].append({"scope": "order_items", "amazon_order_id": order_id, "error": str(exc)})
                    continue
                image_map: dict[str, dict[str, Any]] = {}
                catalog_images = getattr(client, "catalog_item_images", None)
                if include_catalog_images and callable(catalog_images):
                    for item in items:
                        asin = _text(item.get("ASIN"))
                        if not asin:
                            continue
                        try:
                            image_map[asin] = catalog_images(asin, _text(order.get("MarketplaceId")))
                        except AmazonSpApiError as exc:
                            summary["errors"].append({"scope": "catalog_item_images", "amazon_order_id": order_id, "asin": asin, "error": str(exc)})
                with _connect() as connection:
                    for item in items:
                        _save_raw_record(connection, sync_run_id=sync_run_id, resource_type="order_item", payload=item, external_id=order_id)
                    _upsert_order_items(connection, order_id, items, image_map=image_map)
                    connection.commit()
            summary["orders"] = len(orders)

        if include_inbound:
            try:
                inbound_summary = sync_inbound_shipments(
                    client,
                    marketplaces,
                    sync_run_id,
                    item_lookback_days=max(1, int(lookback_minutes or lookback_days) // 1440) if lookback_minutes is not None else max(1, int(lookback_days)),
                )
                summary["inbound_shipments"] = int(inbound_summary.get("shipments") or 0)
                summary["inbound_items"] = int(inbound_summary.get("items") or 0)
                summary["errors"].extend(inbound_summary.get("errors") or [])
            except AmazonSpApiError as exc:
                summary["errors"].append({"scope": "inbound", "error": str(exc)})

        if include_inventory:
            for marketplace_id in marketplaces:
                try:
                    inventory = client.inventory_summaries(marketplace_id)
                except AmazonSpApiError as exc:
                    summary["errors"].append({"scope": "inventory", "marketplace_id": marketplace_id, "error": str(exc)})
                    continue
                with _connect() as connection:
                    for item in inventory:
                        _save_raw_record(connection, sync_run_id=sync_run_id, resource_type="inventory_summary", payload=item, external_id=_text(item.get("fnSku")))
                        _upsert_inventory_snapshot(connection, marketplace_id=marketplace_id, item=item)
                    connection.commit()
                summary["inventory_items"] += len(inventory)

        if include_finances:
            finances = client.financial_events(since)
            with _connect() as connection:
                _save_raw_record(connection, sync_run_id=sync_run_id, resource_type="financial_events", payload=finances)
                for event_type, event in _iter_financial_events(finances):
                    _upsert_financial_event(connection, event_type, event)
                    summary["financial_events"] += 1
                    connection.commit()
            settlements = client.financial_event_groups(since)
            with _connect() as connection:
                for settlement in settlements:
                    _save_raw_record(connection, sync_run_id=sync_run_id, resource_type="financial_event_group", payload=settlement)
                    _upsert_settlement(connection, settlement)
                connection.commit()
            summary["settlements"] = len(settlements)
            for settlement in settlements:
                settlement_id = _text(settlement.get("FinancialEventGroupId"))
                if not settlement_id:
                    continue
                try:
                    group_events = client.financial_events_for_group(settlement_id)
                except AmazonSpApiError as exc:
                    summary["errors"].append({"scope": "settlement_events", "settlement_id": settlement_id, "error": str(exc)})
                    continue
                with _connect() as connection:
                    _save_raw_record(connection, sync_run_id=sync_run_id, resource_type="settlement_financial_events", payload=group_events, external_id=settlement_id)
                    for event_type, event in _iter_financial_events(group_events):
                        event["FinancialEventGroupId"] = event.get("FinancialEventGroupId") or settlement_id
                        _upsert_financial_event(connection, event_type, event)
                        summary["financial_events"] += 1
                    connection.commit()

            if include_settlement_reports:
                try:
                    reports = client.list_reports("GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE")
                except AmazonSpApiError as exc:
                    reports = []
                    summary["errors"].append({"scope": "settlement_reports", "error": str(exc)})
                for report in reports:
                    if _text(report.get("processingStatus")) not in {"DONE", "_DONE_"}:
                        continue
                    report_id = _text(report.get("reportId"))
                    if not report_id:
                        continue
                    try:
                        imported = import_settlement_report(report_id)
                    except AmazonSpApiError as exc:
                        summary["errors"].append({"scope": "settlement_report", "report_id": report_id, "error": str(exc)})
                        continue
                    summary["settlement_reports"] += 1
                    summary["settlement_report_rows"] += int(imported.get("order_financial_rows") or 0)
            summary["inbound_costs"] = sync_inbound_finance_costs()
            transaction_marketplace = _primary_inbound_marketplace(marketplaces)
            if transaction_marketplace:
                transaction_now = datetime.now(timezone.utc) - timedelta(minutes=3)
                transaction_since = transaction_now - (timedelta(minutes=max(1, int(lookback_minutes))) if lookback_minutes is not None else timedelta(days=min(max(1, int(lookback_days)), 180)))
                try:
                    transactions = client.financial_transactions(
                        transaction_since.isoformat().replace("+00:00", "Z"),
                        transaction_now.isoformat().replace("+00:00", "Z"),
                        transaction_marketplace,
                    )
                    summary["modern_transactions"] = len(transactions)
                    summary["modern_order_events"] = sync_modern_financial_transactions(transactions)
                    summary["inbound_costs"] += sync_modern_inbound_costs(transactions)
                except AmazonSpApiError as exc:
                    summary["errors"].append({"scope": "modern_finance_transactions", "error": str(exc)})

        if summary["errors"]:
            summary["status"] = "partial"
    except AmazonSpApiError as exc:
        summary["status"] = "error"
        summary["errors"].append({"scope": "sp_api", "error": str(exc)})
    except Exception as exc:  # pragma: no cover - defensive sync boundary
        summary["status"] = "error"
        summary["errors"].append({"scope": "internal", "error": f"{type(exc).__name__}: {exc}"})

    summary["rate_limits"] = client.rate_limits
    with _connect() as connection:
        connection.execute(
            "UPDATE sync_runs SET completed_at = ?, status = ?, summary_json = ?, error_message = ? WHERE id = ?",
            (_utc_now(), summary["status"], _json_dumps(summary), _text(summary["errors"][0]["error"]) if summary["errors"] else None, sync_run_id),
        )
        connection.commit()
    return summary
