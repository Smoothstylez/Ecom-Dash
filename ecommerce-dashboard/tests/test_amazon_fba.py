from __future__ import annotations

import sqlite3
import json
import io
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError

import pytest


def test_fifo_allocation_projects_amazon_order(monkeypatch, tmp_path) -> None:
    import app.services.amazon_fba as amazon_fba
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()

    batch = amazon_fba.create_procurement_batch(
        reference="h10b-001",
        name="H10B FBA",
        lines=[{"seller_sku": "H10B", "title": "HIBREW H10B", "quantity": 2}],
    )
    lot = amazon_fba.create_inventory_lot(
        batch_line_id=batch["lines"][0]["id"],
        unit_cost_cents=12_500,
        received_at="2026-07-01T00:00:00Z",
    )
    assert lot["available_quantity"] == 2

    with importer._connect() as connection:
        connection.execute(
            """
            INSERT INTO amazon_orders(amazon_order_id, seller_order_id, purchase_date, order_status, fulfillment_channel, currency, order_total_cents, raw_json, updated_at)
            VALUES ('ORDER-1', 'ORDER-1', '2026-07-02T00:00:00Z', 'Shipped', 'AFN', 'EUR', 30000, '{}', '2026-07-02T00:00:00Z')
            """
        )
        connection.execute(
            """
            INSERT INTO amazon_order_items(
                id, amazon_order_id, seller_sku, title, quantity_ordered, quantity_shipped,
                currency, item_price_cents, item_tax_cents
            ) VALUES ('ITEM-1', 'ORDER-1', 'H10B', 'HIBREW H10B', 2, 2, 'EUR', 30000, 4790)
            """
        )
        connection.commit()

    result = amazon_fba.allocate_order_fifo("ORDER-1")
    assert result["allocated_cogs_cents"] == 25_000
    summary = amazon_fba.load_amazon_order_summaries()[0]
    assert summary["marketplace"] == "amazon"
    assert summary["fulfillment_channel"] == "AFN"
    assert summary["purchase_cost_cents"] == 25_000
    assert summary["profit_cents"] == 5_000
    assert summary["sales_gross_cents"] == 30_000
    assert summary["sales_vat_cents"] == 4_790
    assert summary["sales_net_cents"] == 25_210

    with importer._connect() as connection:
        remaining = connection.execute("SELECT available_quantity FROM inventory_lots WHERE id = ?", (lot["id"],)).fetchone()[0]
    assert remaining == 0


def test_amazon_order_detail_projects_available_address_and_catalog_image(monkeypatch, tmp_path) -> None:
    import app.services.amazon_fba as amazon_fba
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    with importer._connect() as connection:
        connection.execute(
            """
            INSERT INTO amazon_orders(
                amazon_order_id, seller_order_id, marketplace_id, purchase_date,
                order_status, fulfillment_channel, currency, order_total_cents,
                raw_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "ORDER-ADDRESS-1", "ORDER-ADDRESS-1", "A1PA6795UKMFR9",
                "2026-07-29T09:53:12Z", "Shipped", "AFN", "EUR", 14890,
                '{"ShippingAddress":{"City":"Diemelstadt","StateOrRegion":"Diemelstadt","PostalCode":"34474","CountryCode":"DE"}}',
                "2026-07-30T00:00:00Z",
            ),
        )
        connection.execute(
            """
            INSERT INTO amazon_order_items(
                id, amazon_order_id, asin, seller_sku, title, quantity_ordered,
                quantity_shipped, currency, item_price_cents, item_tax_cents,
                image_url, image_urls_json, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "ITEM-ADDRESS-1", "ORDER-ADDRESS-1", "B0D95XYL1R", "SKU-1",
                "Product", 1, 1, "EUR", 14890, 2377,
                "https://m.media-amazon.com/images/I/main.jpg",
                '["https://m.media-amazon.com/images/I/main.jpg"]', "{}",
            ),
        )
        connection.commit()

    detail = amazon_fba.get_amazon_order_detail("ORDER-ADDRESS-1")

    assert detail is not None
    assert detail["shipping_address"] == {
        "first_name": None,
        "last_name": None,
        "name": None,
        "company": None,
        "address1": None,
        "address2": None,
        "street": None,
        "house_number": None,
        "postcode": "34474",
        "city": "Diemelstadt",
        "state_or_region": "Diemelstadt",
        "country": "DE",
        "country_code": "DE",
        "phone": None,
    }
    assert detail["billing_address"] == {}
    assert detail["line_items"][0]["image_url"] == "https://m.media-amazon.com/images/I/main.jpg"


def test_catalog_item_images_extracts_main_and_large_variants() -> None:
    from app.services.importers.amazon_sp_api import extract_catalog_item_images

    result = extract_catalog_item_images({
        "images": [{
            "marketplaceId": "A1PA6795UKMFR9",
            "images": [
                {"variant": "MAIN", "link": "https://example.test/main.jpg", "height": 2000},
                {"variant": "MAIN", "link": "https://example.test/thumb.jpg", "height": 75},
                {"variant": "PT01", "link": "https://example.test/detail.jpg", "height": 2000},
            ],
        }],
    })

    assert result == {
        "image_url": "https://example.test/main.jpg",
        "image_urls": [
            "https://example.test/main.jpg",
            "https://example.test/detail.jpg",
        ],
    }


def test_modern_finance_transaction_projects_order_sales_and_fees(monkeypatch, tmp_path) -> None:
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    transaction = {
        "transactionType": "Shipment",
        "transactionId": "TX-1",
        "transactionStatus": "DEFERRED",
        "postedDate": "2026-07-30T06:34:53Z",
        "totalAmount": {"currencyAmount": 115.90, "currencyCode": "EUR"},
        "relatedIdentifiers": [
            {"relatedIdentifierName": "ORDER_ID", "relatedIdentifierValue": "ORDER-FINANCE-1"},
            {"relatedIdentifierName": "SETTLEMENT_ID", "relatedIdentifierValue": "SETTLEMENT-1"},
        ],
        "breakdowns": [
            {"breakdownType": "Sales", "breakdownAmount": {"currencyAmount": 148.90, "currencyCode": "EUR"}},
            {"breakdownType": "Expenses", "breakdownAmount": {"currencyAmount": -33.00, "currencyCode": "EUR"}},
        ],
    }

    imported = importer.sync_modern_financial_transactions([transaction])

    assert imported == 1
    with importer._connect() as connection:
        event = connection.execute(
            "SELECT amazon_order_id, financial_finality, sales_cents, fees_cents, net_cents FROM amazon_financial_events"
        ).fetchone()
    assert tuple(event) == ("ORDER-FINANCE-1", "deferred", 14890, 3300, 11590)


def test_deferred_released_finance_transaction_is_released(monkeypatch, tmp_path) -> None:
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    importer.sync_modern_financial_transactions([{
        "transactionType": "Shipment",
        "transactionId": "TX-DEFERRED-1",
        "transactionStatus": "DEFERRED_RELEASED",
        "postedDate": "2026-08-21T10:00:00Z",
        "totalAmount": {"currencyAmount": 10, "currencyCode": "EUR"},
        "relatedIdentifiers": [{"relatedIdentifierName": "ORDER_ID", "relatedIdentifierValue": "ORDER-DEFERRED-1"}],
        "breakdowns": [],
    }])

    with importer._connect() as connection:
        finality = connection.execute("SELECT financial_finality FROM amazon_financial_events").fetchone()[0]
    assert finality == "released"


def test_modern_finance_persists_shared_deferred_lifecycle_metadata(monkeypatch, tmp_path) -> None:
    import app.services.amazon_fba as amazon_fba
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    common = {
        "transactionType": "Shipment",
        "totalAmount": {"currencyAmount": 10, "currencyCode": "EUR"},
        "relatedIdentifiers": [{"relatedIdentifierName": "ORDER_ID", "relatedIdentifierValue": "ORDER-LIFECYCLE-1"}],
        "breakdowns": [],
    }
    importer.sync_modern_financial_transactions([
        {
            **common,
            "transactionId": "TX-DEFERRED",
            "transactionStatus": "DEFERRED_RELEASED",
            "postedDate": "2026-08-20T10:00:00Z",
            "contexts": [{"contextType": "DeferredContext", "deferralReason": "DD7", "maturityDate": "2026-08-27T10:00:00Z"}],
            "relatedIdentifiers": [
                {"relatedIdentifierName": "ORDER_ID", "relatedIdentifierValue": "ORDER-LIFECYCLE-1"},
                {"relatedIdentifierName": "RELEASE_TRANSACTION_ID", "relatedIdentifierValue": "TX-RELEASE"},
            ],
        },
        {
            **common,
            "transactionId": "TX-RELEASE",
            "transactionStatus": "RELEASED",
            "postedDate": "2026-08-27T10:00:00Z",
            "relatedIdentifiers": [
                {"relatedIdentifierName": "ORDER_ID", "relatedIdentifierValue": "ORDER-LIFECYCLE-1"},
                {"relatedIdentifierName": "DEFERRED_TRANSACTION_ID", "relatedIdentifierValue": "TX-DEFERRED"},
            ],
        },
    ])

    with importer._connect() as connection:
        rows = connection.execute(
            "SELECT transaction_id, lifecycle_id, deferral_reason, maturity_date FROM amazon_financial_events ORDER BY posted_date"
        ).fetchall()
    assert [tuple(row) for row in rows] == [
        ("TX-DEFERRED", "TX-DEFERRED", "DD7", "2026-08-27T10:00:00Z"),
        ("TX-RELEASE", "TX-DEFERRED", None, None),
    ]
    overview = amazon_fba.get_amazon_finance_overview()
    assert len(overview["events"]) == 1
    assert overview["events"][0]["financial_finality"] == "released"


def test_modern_finance_event_exposes_fee_breakdown(monkeypatch, tmp_path) -> None:
    import app.services.amazon_fba as amazon_fba
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    transaction = {
        "transactionType": "Shipment",
        "transactionId": "TX-BREAKDOWN-1",
        "transactionStatus": "DEFERRED",
        "postedDate": "2026-07-30T06:34:53Z",
        "totalAmount": {"currencyAmount": 115.90, "currencyCode": "EUR"},
        "relatedIdentifiers": [
            {"relatedIdentifierName": "ORDER_ID", "relatedIdentifierValue": "ORDER-BREAKDOWN-1"},
            {"relatedIdentifierName": "SHIPMENT_ID", "relatedIdentifierValue": "SHIPMENT-1"},
            {"relatedIdentifierName": "SETTLEMENT_ID", "relatedIdentifierValue": "SETTLEMENT-1"},
        ],
        "contexts": [{"contextType": "DeferredContext", "maturityDate": "2026-08-08T18:00:00Z"}],
        "breakdowns": [
            {"breakdownType": "Sales", "breakdownAmount": {"currencyAmount": 148.90, "currencyCode": "EUR"}, "breakdowns": [
                {"breakdownType": "Tax", "breakdownAmount": {"currencyAmount": 23.77, "currencyCode": "EUR"}}
            ]},
            {"breakdownType": "Expenses", "breakdownAmount": {"currencyAmount": -33.00, "currencyCode": "EUR"}, "breakdowns": [
                {"breakdownType": "AmazonFees", "breakdownAmount": {"currencyAmount": -33.00, "currencyCode": "EUR"}, "breakdowns": [
                    {"breakdownType": "Commission", "breakdownAmount": {"currencyAmount": -26.59, "currencyCode": "EUR"}},
                    {"breakdownType": "FBAPerUnitFulfillmentFee", "breakdownAmount": {"currencyAmount": -6.41, "currencyCode": "EUR"}},
                ]}
            ]},
        ],
    }

    importer.sync_modern_financial_transactions([transaction])
    detail = amazon_fba.get_amazon_order_detail("ORDER-BREAKDOWN-1")

    assert detail is not None
    assert detail["financial_events"][0]["financial_breakdown"] == {
        "sales_cents": 14890,
        "tax_cents": 2377,
        "fees": [
            {"type": "Commission", "amount_cents": 2659},
            {"type": "FBAPerUnitFulfillmentFee", "amount_cents": 641},
        ],
        "net_cents": 11590,
        "financial_finality": "deferred",
        "maturity_date": "2026-08-08T18:00:00Z",
        "order_id": "ORDER-BREAKDOWN-1",
        "shipment_id": "SHIPMENT-1",
        "settlement_id": "SETTLEMENT-1",
    }


def test_amazon_detail_projects_catalog_image_variants(monkeypatch, tmp_path) -> None:
    import app.services.amazon_fba as amazon_fba
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    with importer._connect() as connection:
        connection.execute(
            "INSERT INTO amazon_orders(amazon_order_id, seller_order_id, currency, order_total_cents, raw_json, updated_at) VALUES ('ORDER-IMAGE-1', 'ORDER-IMAGE-1', 'EUR', 1000, '{}', '2026-07-30T00:00:00Z')"
        )
        connection.execute(
            """
            INSERT INTO amazon_order_items(
                id, amazon_order_id, asin, seller_sku, title, currency,
                image_url, image_urls_json, raw_json
            ) VALUES ('ITEM-IMAGE-1', 'ORDER-IMAGE-1', 'ASIN-1', 'SKU-1', 'Product', 'EUR', ?, ?, '{}')
            """,
            ("https://example.test/main.jpg", '["https://example.test/main.jpg", "https://example.test/detail.jpg"]'),
        )
        connection.commit()

    detail = amazon_fba.get_amazon_order_detail("ORDER-IMAGE-1")

    assert detail is not None
    assert detail["line_items"][0]["image_url"] == "https://example.test/main.jpg"
    assert detail["line_items"][0]["image_urls"] == [
        "https://example.test/main.jpg",
        "https://example.test/detail.jpg",
    ]


def test_invoice_line_migration_backfills_gross_from_net_and_vat(monkeypatch, tmp_path) -> None:
    import app.services.importers.amazon_sp_api as importer

    db_path = tmp_path / "amazon.sqlite3"
    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", db_path)
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            "CREATE TABLE amazon_inbound_invoice_lines "
            "(id TEXT PRIMARY KEY, invoice_id TEXT, seller_sku TEXT, fnsku TEXT, "
            "asin TEXT, title TEXT, quantity INTEGER, net_cents INTEGER, vat_cents INTEGER, raw_json TEXT)"
        )
        connection.execute(
            "INSERT INTO amazon_inbound_invoice_lines VALUES "
            "('LINE-1', 'INV-1', 'SKU-1', 'FNSKU-1', '', '', 1, 1000, 190, '{}')"
        )

    importer.init_amazon_fba_db()

    with importer._connect() as connection:
        row = connection.execute(
            "SELECT gross_cents FROM amazon_inbound_invoice_lines WHERE id = 'LINE-1'"
        ).fetchone()

    assert row["gross_cents"] == 1190


def test_invoice_line_rejects_gross_not_equal_to_net_plus_vat(monkeypatch, tmp_path) -> None:
    import app.services.amazon_fba as amazon_fba
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()

    with pytest.raises(ValueError, match="gross_cents must equal net_cents plus vat_cents"):
        amazon_fba.add_inbound_invoice_line(
            invoice_id="INV-1", seller_sku="SKU-1", fnsku="FNSKU-1", asin="",
            title="Product", quantity=1, gross_cents=1200, net_cents=1000, vat_cents=190,
        )


def test_invoice_line_endpoint_persists_gross_cents(monkeypatch, tmp_path) -> None:
    from fastapi.testclient import TestClient

    import app.main as main_module
    import app.services.amazon_fba as amazon_fba
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    monkeypatch.setenv("APP_ADMIN_TOKEN", "test-token")
    importer.init_amazon_fba_db()
    with importer._connect() as connection:
        importer._upsert_inbound_shipment(
            connection,
            shipment={"ShipmentId": "FBA-API-LINE", "ShipmentStatus": "CLOSED"},
            items=[{"SellerSKU": "SKU-1", "FulfillmentNetworkSKU": "FNSKU-1", "QuantityShipped": 1, "QuantityReceived": 1}],
        )
    invoice = amazon_fba.add_inbound_invoice(
        shipment_id="FBA-API-LINE", supplier_name="Supplier", invoice_number="INV-API-LINE",
        invoice_date="2026-08-20", currency="EUR", gross_cents=1190,
        net_cents=1000, vat_cents=190, document_path="api-line.pdf",
    )

    response = TestClient(main_module.app).post(
        f"/api/amazon/inbound/invoices/{invoice['id']}/lines",
        json={
            "seller_sku": "SKU-1",
            "fnsku": "FNSKU-1",
            "quantity": 1,
            "gross_cents": 1190,
            "net_cents": 1000,
            "vat_cents": 190,
        },
        headers={"X-Admin-Token": "test-token"},
    )

    assert response.status_code == 200
    assert response.json()["line"]["gross_cents"] == 1190


def test_invoice_line_endpoint_requires_gross_net_vat_consistency(monkeypatch, tmp_path) -> None:
    from fastapi.testclient import TestClient

    import app.main as main_module
    import app.services.amazon_fba as amazon_fba
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    monkeypatch.setenv("APP_ADMIN_TOKEN", "test-token")
    importer.init_amazon_fba_db()
    with importer._connect() as connection:
        importer._upsert_inbound_shipment(
            connection,
            shipment={"ShipmentId": "FBA-API-VALIDATION", "ShipmentStatus": "CLOSED"},
            items=[{
                "SellerSKU": "SKU-1",
                "FulfillmentNetworkSKU": "FNSKU-1",
                "QuantityShipped": 1,
                "QuantityReceived": 1,
            }],
        )
    invoice = amazon_fba.add_inbound_invoice(
        shipment_id="FBA-API-VALIDATION", supplier_name="Supplier",
        invoice_number="INV-API-VALIDATION", invoice_date="2026-08-20",
        currency="EUR", gross_cents=950, net_cents=800, vat_cents=150,
        document_path="api-validation.pdf",
    )

    response = TestClient(main_module.app).post(
        f"/api/amazon/inbound/invoices/{invoice['id']}/lines",
        json={
            "seller_sku": "SKU-1",
            "fnsku": "FNSKU-1",
            "quantity": 1,
            "gross_cents": 1000,
            "net_cents": 900,
            "vat_cents": 50,
        },
        headers={"X-Admin-Token": "test-token"},
    )

    assert response.status_code == 400
    assert "gross_cents" in response.json()["detail"]


def test_invoice_line_endpoint_requires_vat_cents(monkeypatch, tmp_path) -> None:
    from fastapi.testclient import TestClient

    import app.main as main_module
    import app.services.amazon_fba as amazon_fba
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    monkeypatch.setenv("APP_ADMIN_TOKEN", "test-token")
    importer.init_amazon_fba_db()
    with importer._connect() as connection:
        importer._upsert_inbound_shipment(
            connection,
            shipment={"ShipmentId": "FBA-API-REQUIRED", "ShipmentStatus": "CLOSED"},
            items=[{
                "SellerSKU": "SKU-1",
                "FulfillmentNetworkSKU": "FNSKU-1",
                "QuantityShipped": 1,
                "QuantityReceived": 1,
            }],
        )
    invoice = amazon_fba.add_inbound_invoice(
        shipment_id="FBA-API-REQUIRED", supplier_name="Supplier",
        invoice_number="INV-API-REQUIRED", invoice_date="2026-08-20",
        currency="EUR", gross_cents=1000, net_cents=1000, vat_cents=0,
        document_path="api-required.pdf",
    )

    response = TestClient(main_module.app).post(
        f"/api/amazon/inbound/invoices/{invoice['id']}/lines",
        json={
            "seller_sku": "SKU-1",
            "fnsku": "FNSKU-1",
            "quantity": 1,
            "gross_cents": 1000,
            "net_cents": 1000,
        },
        headers={"X-Admin-Token": "test-token"},
    )

    assert response.status_code == 422
    assert any(error["loc"][-1] == "vat_cents" for error in response.json()["detail"])


def test_inbound_shipment_detail_projects_invoice_and_line_amounts(monkeypatch, tmp_path) -> None:
    from fastapi.testclient import TestClient

    import app.main as main_module
    import app.services.amazon_fba as amazon_fba
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    with importer._connect() as connection:
        importer._upsert_inbound_shipment(
            connection,
            shipment={"ShipmentId": "FBA-DETAIL", "ShipmentStatus": "CLOSED"},
            items=[{
                "SellerSKU": "SKU-1",
                "FulfillmentNetworkSKU": "FNSKU-1",
                "QuantityShipped": 1,
                "QuantityReceived": 1,
            }],
        )
    invoice = amazon_fba.add_inbound_invoice(
        shipment_id="FBA-DETAIL", supplier_name="Supplier", invoice_number="INV-DETAIL",
        invoice_date="2026-08-20", currency="EUR", gross_cents=1190,
        net_cents=1000, vat_cents=190, document_path="detail.pdf", notes="Invoice note",
    )
    amazon_fba.add_inbound_invoice_line(
        invoice_id=invoice["id"], seller_sku="SKU-1", fnsku="FNSKU-1", asin="ASIN-1",
        title="Product", quantity=1, gross_cents=1190, net_cents=1000, vat_cents=190,
    )

    response = TestClient(main_module.app).get("/api/amazon/inbound/shipments/FBA-DETAIL")

    assert response.status_code == 200
    detail = response.json()
    invoice_header = detail["invoices"][0]
    assert {key: invoice_header[key] for key in (
        "supplier_name", "invoice_number", "invoice_date", "currency", "gross_cents",
        "net_cents", "vat_cents", "document_path", "notes",
    )} == {
        "supplier_name": "Supplier",
        "invoice_number": "INV-DETAIL",
        "invoice_date": "2026-08-20",
        "currency": "EUR",
        "gross_cents": 1190,
        "net_cents": 1000,
        "vat_cents": 190,
        "document_path": "detail.pdf",
        "notes": "Invoice note",
    }
    invoice_line = detail["invoice_lines"][0]
    assert {key: invoice_line[key] for key in ("gross_cents", "net_cents", "vat_cents")} == {
        "gross_cents": 1190,
        "net_cents": 1000,
        "vat_cents": 190,
    }


def test_list_inbound_shipments_hides_cancelled_and_projects_cost_status(monkeypatch, tmp_path) -> None:
    import app.services.amazon_fba as amazon_fba
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    with importer._connect() as connection:
        for shipment_id, shipment_status in (
            ("FBA-CANCELLED", "CANCELLED"),
            ("FBA-MISSING", "CLOSED"),
            ("FBA-ENTERED", "CLOSED"),
            ("FBA-CONFIRMED", "CLOSED"),
        ):
            importer._upsert_inbound_shipment(
                connection,
                shipment={"ShipmentId": shipment_id, "ShipmentStatus": shipment_status},
                items=[{
                    "SellerSKU": "SKU-1",
                    "FulfillmentNetworkSKU": "FNSKU-1",
                    "QuantityShipped": 1,
                    "QuantityReceived": 1,
                }],
            )
    amazon_fba.add_inbound_invoice(
        shipment_id="FBA-ENTERED", supplier_name="Supplier", invoice_number="INV-E",
        invoice_date="2026-08-20", currency="EUR", gross_cents=1190,
        net_cents=1000, vat_cents=190, document_path="entered.pdf",
    )
    confirmed_invoice = amazon_fba.add_inbound_invoice(
        shipment_id="FBA-CONFIRMED", supplier_name="Supplier", invoice_number="INV-C",
        invoice_date="2026-08-20", currency="EUR", gross_cents=1190,
        net_cents=1000, vat_cents=190, document_path="confirmed.pdf",
    )
    amazon_fba.add_inbound_invoice_line(
        invoice_id=confirmed_invoice["id"], seller_sku="SKU-1", fnsku="FNSKU-1",
        asin="", title="Product", quantity=1, gross_cents=1190, net_cents=1000,
        vat_cents=190,
    )
    amazon_fba.confirm_inbound_product_costs("FBA-CONFIRMED")

    shipments = amazon_fba.list_inbound_shipments()

    assert {shipment["shipment_id"] for shipment in shipments} == {
        "FBA-MISSING", "FBA-ENTERED", "FBA-CONFIRMED",
    }
    assert {shipment["shipment_id"]: shipment["cost_status"] for shipment in shipments} == {
        "FBA-MISSING": "missing",
        "FBA-ENTERED": "entered",
        "FBA-CONFIRMED": "confirmed",
    }
    assert [shipment["shipment_id"] for shipment in amazon_fba.list_inbound_shipments("CANCELLED")] == [
        "FBA-CANCELLED"
    ]


def _fba_services(monkeypatch, tmp_path):
    import app.services.amazon_fba as amazon_fba
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    return amazon_fba, importer


def _closed_two_sku_shipment(importer, shipment_id: str) -> None:
    with importer._connect() as connection:
        importer._upsert_inbound_shipment(
            connection,
            shipment={"ShipmentId": shipment_id, "ShipmentStatus": "CLOSED"},
            items=[
                {"SellerSKU": "SKU-1", "FulfillmentNetworkSKU": "FNSKU-1", "QuantityShipped": 1, "QuantityReceived": 1},
                {"SellerSKU": "SKU-2", "FulfillmentNetworkSKU": "FNSKU-2", "QuantityShipped": 1, "QuantityReceived": 1},
            ],
        )


def _invoice(amazon_fba, shipment_id: str, number: str, gross: int, net: int, vat: int):
    return amazon_fba.add_inbound_invoice(
        shipment_id=shipment_id, supplier_name="Supplier", invoice_number=number,
        invoice_date="2026-08-20", currency="EUR", gross_cents=gross,
        net_cents=net, vat_cents=vat, document_path=f"{number}.pdf",
    )


def _line(amazon_fba, invoice_id: str, sku: str, fnsku: str, gross: int, net: int, vat: int) -> None:
    amazon_fba.add_inbound_invoice_line(
        invoice_id=invoice_id, seller_sku=sku, fnsku=fnsku, asin="", title=sku,
        quantity=1, gross_cents=gross, net_cents=net, vat_cents=vat,
    )


def test_confirm_inbound_product_costs_accepts_one_combined_invoice(monkeypatch, tmp_path) -> None:
    amazon_fba, importer = _fba_services(monkeypatch, tmp_path)
    _closed_two_sku_shipment(importer, "FBA-COMBINED")
    invoice = _invoice(amazon_fba, "FBA-COMBINED", "INV-C", gross=3570, net=3000, vat=570)
    _line(amazon_fba, invoice["id"], "SKU-1", "FNSKU-1", gross=1190, net=1000, vat=190)
    _line(amazon_fba, invoice["id"], "SKU-2", "FNSKU-2", gross=2380, net=2000, vat=380)

    result = amazon_fba.confirm_inbound_product_costs("FBA-COMBINED")

    assert {lot["seller_sku"]: lot["unit_cost_cents"] for lot in result["lots"]} == {"SKU-1": 1000, "SKU-2": 2000}


def test_confirm_inbound_product_costs_accepts_one_invoice_per_sku(monkeypatch, tmp_path) -> None:
    amazon_fba, importer = _fba_services(monkeypatch, tmp_path)
    _closed_two_sku_shipment(importer, "FBA-SEPARATE")
    invoice_one = _invoice(amazon_fba, "FBA-SEPARATE", "INV-1", gross=1190, net=1000, vat=190)
    invoice_two = _invoice(amazon_fba, "FBA-SEPARATE", "INV-2", gross=2380, net=2000, vat=380)
    _line(amazon_fba, invoice_one["id"], "SKU-1", "FNSKU-1", gross=1190, net=1000, vat=190)
    _line(amazon_fba, invoice_two["id"], "SKU-2", "FNSKU-2", gross=2380, net=2000, vat=380)

    result = amazon_fba.confirm_inbound_product_costs("FBA-SEPARATE")

    assert len(result["lots"]) == 2


def test_confirm_inbound_product_costs_rejects_invoice_header_line_total_mismatch(monkeypatch, tmp_path) -> None:
    amazon_fba, importer = _fba_services(monkeypatch, tmp_path)
    _closed_two_sku_shipment(importer, "FBA-MISMATCH")
    invoice = _invoice(amazon_fba, "FBA-MISMATCH", "INV-M", gross=1190, net=1000, vat=190)
    _line(amazon_fba, invoice["id"], "SKU-1", "FNSKU-1", gross=1189, net=1000, vat=189)

    with pytest.raises(ValueError, match="invoice line gross total must match invoice gross total"):
        amazon_fba.confirm_inbound_product_costs("FBA-MISMATCH")


def test_invoice_lines_allocate_exact_single_sku_cost(monkeypatch, tmp_path) -> None:
    import app.services.amazon_fba as amazon_fba
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    with importer._connect() as connection:
        importer._upsert_inbound_shipment(
            connection,
            shipment={"ShipmentId": "FBA-COST-1", "ShipmentStatus": "CLOSED"},
            items=[{"SellerSKU": "SKU-1", "FulfillmentNetworkSKU": "FNSKU-1", "QuantityShipped": 11, "QuantityReceived": 11}],
        )
    invoice = amazon_fba.add_inbound_invoice(
        shipment_id="FBA-COST-1", supplier_name="Supplier", invoice_number="INV-1",
        invoice_date="2026-07-01", currency="EUR", gross_cents=12100, net_cents=11000,
        vat_cents=1100, document_path="invoice.pdf",
    )
    amazon_fba.add_inbound_invoice_line(
        invoice_id=invoice["id"], seller_sku="SKU-1", fnsku="FNSKU-1", asin="ASIN-1",
        title="Product", quantity=11, gross_cents=12100, net_cents=11000, vat_cents=1100,
    )

    result = amazon_fba.confirm_inbound_product_costs("FBA-COST-1")

    assert result["lots"][0]["seller_sku"] == "SKU-1"
    assert result["lots"][0]["available_quantity"] == 11
    assert result["lots"][0]["unit_cost_cents"] == 1000


def test_multi_sku_cost_confirmation_rejects_missing_invoice_lines(monkeypatch, tmp_path) -> None:
    import app.services.amazon_fba as amazon_fba
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    with importer._connect() as connection:
        importer._upsert_inbound_shipment(
            connection,
            shipment={"ShipmentId": "FBA-COST-2", "ShipmentStatus": "CLOSED"},
            items=[
                {"SellerSKU": "SKU-1", "FulfillmentNetworkSKU": "FNSKU-1", "QuantityShipped": 11, "QuantityReceived": 11},
                {"SellerSKU": "SKU-2", "FulfillmentNetworkSKU": "FNSKU-2", "QuantityShipped": 11, "QuantityReceived": 11},
            ],
        )
    invoice = amazon_fba.add_inbound_invoice(
        shipment_id="FBA-COST-2", supplier_name="Supplier", invoice_number="INV-2",
        invoice_date="2026-07-01", currency="EUR", gross_cents=22000, net_cents=20000,
        vat_cents=2000, document_path="invoice.pdf",
    )

    with pytest.raises(ValueError, match="invoice lines"):
        amazon_fba.confirm_inbound_product_costs("FBA-COST-2")


def test_unreceived_shipment_cannot_create_product_lot(monkeypatch, tmp_path) -> None:
    import app.services.amazon_fba as amazon_fba
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    with importer._connect() as connection:
        importer._upsert_inbound_shipment(
            connection,
            shipment={"ShipmentId": "FBA-COST-3", "ShipmentStatus": "READY_TO_SHIP"},
            items=[{"SellerSKU": "SKU-1", "FulfillmentNetworkSKU": "FNSKU-1", "QuantityShipped": 11, "QuantityReceived": 0}],
        )
    invoice = amazon_fba.add_inbound_invoice(
        shipment_id="FBA-COST-3", supplier_name="Supplier", invoice_number="INV-3",
        invoice_date="2026-07-01", currency="EUR", gross_cents=11000, net_cents=10000,
        vat_cents=1000, document_path="invoice.pdf",
    )
    amazon_fba.add_inbound_invoice_line(
        invoice_id=invoice["id"], seller_sku="SKU-1", fnsku="FNSKU-1", asin="ASIN-1",
        title="Product", quantity=11, gross_cents=11000, net_cents=10000, vat_cents=1000,
    )

    with pytest.raises(ValueError, match="received"):
        amazon_fba.confirm_inbound_product_costs("FBA-COST-3")


def test_confirmed_fba_lot_is_consumed_by_amazon_order_fifo(monkeypatch, tmp_path) -> None:
    import app.services.amazon_fba as amazon_fba
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    with importer._connect() as connection:
        importer._upsert_inbound_shipment(
            connection,
            shipment={"ShipmentId": "FBA-FIFO-1", "ShipmentStatus": "CLOSED"},
            items=[{"SellerSKU": "SKU-1", "FulfillmentNetworkSKU": "FNSKU-1", "QuantityShipped": 11, "QuantityReceived": 11}],
        )
        connection.execute(
            "INSERT INTO amazon_orders(amazon_order_id, seller_order_id, currency, order_total_cents, raw_json, updated_at) VALUES ('ORDER-FIFO-1', 'ORDER-FIFO-1', 'EUR', 1000, '{}', '2026-07-30T00:00:00Z')"
        )
        connection.execute(
            "INSERT INTO amazon_order_items(id, amazon_order_id, seller_sku, title, quantity_ordered, quantity_shipped, currency, item_price_cents, item_tax_cents, raw_json) VALUES ('ITEM-FIFO-1', 'ORDER-FIFO-1', 'SKU-1', 'Product', 1, 1, 'EUR', 1000, 0, '{}')"
        )
        connection.commit()
    invoice = amazon_fba.add_inbound_invoice(
        shipment_id="FBA-FIFO-1", supplier_name="Supplier", invoice_number="INV-FIFO",
        invoice_date="2026-07-01", currency="EUR", gross_cents=12100, net_cents=11000,
        vat_cents=1100, document_path="invoice.pdf",
    )
    amazon_fba.add_inbound_invoice_line(
        invoice_id=invoice["id"], seller_sku="SKU-1", fnsku="FNSKU-1", asin="ASIN-1",
        title="Product", quantity=11, gross_cents=12100, net_cents=11000, vat_cents=1100,
    )
    amazon_fba.confirm_inbound_product_costs("FBA-FIFO-1")

    result = amazon_fba.allocate_order_fifo("ORDER-FIFO-1")
    detail = amazon_fba.get_amazon_order_detail("ORDER-FIFO-1")

    assert result["allocated_cogs_cents"] == 1000
    assert detail is not None
    assert detail["summary"]["purchase_cost_cents"] == 1000
    assert detail["fifo_allocations"][0]["inbound_shipment_id"] == "FBA-FIFO-1"


def test_synthetic_finance_order_does_not_overwrite_real_order_payload(monkeypatch, tmp_path) -> None:
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    real_order = {
        "AmazonOrderId": "ORDER-PAYLOAD-1",
        "MarketplaceId": "A1PA6795UKMFR9",
        "PurchaseDate": "2026-07-01T00:00:00Z",
        "OrderStatus": "Shipped",
        "FulfillmentChannel": "AFN",
        "SalesChannel": "Amazon.de",
        "OrderTotal": {"CurrencyCode": "EUR", "Amount": "148.90"},
        "ShippingAddress": {"City": "Diemelstadt", "PostalCode": "34474"},
    }
    with importer._connect() as connection:
        importer._upsert_order(connection, real_order)
        importer._upsert_order(
            connection,
            {"AmazonOrderId": "ORDER-PAYLOAD-1", "PurchaseDate": "2026-07-02T00:00:00Z", "OrderStatus": "financial_event"},
            synthetic=True,
        )
        row = connection.execute("SELECT * FROM amazon_orders WHERE amazon_order_id = 'ORDER-PAYLOAD-1'").fetchone()

    assert json.loads(row["raw_json"])["ShippingAddress"] == {"City": "Diemelstadt", "PostalCode": "34474"}
    assert row["order_status"] == "Shipped"
    assert row["marketplace_id"] == "A1PA6795UKMFR9"


def test_modern_finance_source_takes_precedence_over_settlement_report(monkeypatch, tmp_path) -> None:
    import app.services.amazon_fba as amazon_fba
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    with importer._connect() as connection:
        importer._upsert_order(connection, {"AmazonOrderId": "ORDER-DEDUP-1", "OrderTotal": {"CurrencyCode": "EUR", "Amount": "100.00"}})
        for event_id, event_type in (("MODERN-1", "ModernTransaction:Shipment"), ("REPORT-1", "SettlementReportLine")):
            connection.execute(
                "INSERT INTO amazon_financial_events(id, event_type, amazon_order_id, settlement_id, financial_finality, currency, sales_cents, fees_cents, net_cents, raw_json) VALUES (?, ?, ?, 'SETTLEMENT-1', 'released', 'EUR', 10000, 1000, 9000, '{}')",
                (event_id, event_type, "ORDER-DEDUP-1"),
            )
        connection.commit()

    summary = amazon_fba.load_amazon_order_summaries()[0]

    assert summary["total_cents"] == 10000
    assert summary["fees_cents"] == 1000
    detail = amazon_fba.get_amazon_order_detail("ORDER-DEDUP-1")
    assert detail is not None
    assert len(detail["financial_events"]) == 1


def test_modern_inbound_costs_keep_each_shipment_item_separate() -> None:
    from app.services.importers.amazon_sp_api import extract_modern_inbound_costs

    transaction = {
        "transactionId": "TX-MULTI-SHIPMENT-1",
        "items": [
            {
                "relatedIdentifiers": [{"itemRelatedIdentifierValue": "SKU-1:FBA-A"}],
                "breakdowns": [{
                    "breakdownType": "AmazonFees",
                    "breakdowns": [{"breakdownType": "FBAInboundTransportationFee", "breakdownAmount": {"currencyAmount": -10, "currencyCode": "EUR"}}],
                }],
            },
            {
                "relatedIdentifiers": [{"itemRelatedIdentifierValue": "SKU-2:FBA-B"}],
                "breakdowns": [{
                    "breakdownType": "AmazonFees",
                    "breakdowns": [{"breakdownType": "FBAInboundTransportationFee", "breakdownAmount": {"currencyAmount": -20, "currencyCode": "EUR"}}],
                }],
            },
        ],
    }

    costs = extract_modern_inbound_costs([transaction])

    assert {(cost["shipment_id"], cost["amount_cents"]) for cost in costs} == {("FBA-A", 1000), ("FBA-B", 2000)}


def test_modern_finance_components_are_visible_in_overview(monkeypatch, tmp_path) -> None:
    import app.services.amazon_fba as amazon_fba
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    importer.sync_modern_financial_transactions([{
        "transactionType": "Shipment",
        "transactionId": "TX-COMPONENTS-1",
        "transactionStatus": "DEFERRED",
        "postedDate": "2026-07-30T00:00:00Z",
        "totalAmount": {"currencyAmount": 115.90, "currencyCode": "EUR"},
        "relatedIdentifiers": [{"relatedIdentifierName": "ORDER_ID", "relatedIdentifierValue": "ORDER-COMPONENTS-1"}],
        "breakdowns": [
            {"breakdownType": "Sales", "breakdownAmount": {"currencyAmount": 148.90, "currencyCode": "EUR"}},
            {"breakdownType": "Expenses", "breakdownAmount": {"currencyAmount": -33.00, "currencyCode": "EUR"}, "breakdowns": [
                {"breakdownType": "AmazonFees", "breakdowns": [
                    {"breakdownType": "Commission", "breakdownAmount": {"currencyAmount": -26.59, "currencyCode": "EUR"}},
                    {"breakdownType": "FBAPerUnitFulfillmentFee", "breakdownAmount": {"currencyAmount": -6.41, "currencyCode": "EUR"}},
                ]}
            ]},
        ],
    }])

    overview = amazon_fba.get_amazon_finance_overview()

    names = {component["name"] for component in overview["events"][0]["components"]}
    assert {"Commission", "FBAPerUnitFulfillmentFee"}.issubset(names)


def test_inbound_receipt_timestamp_is_preserved_across_status_updates(monkeypatch, tmp_path) -> None:
    from app.services.importers.amazon_sp_api import _connect, _upsert_inbound_shipment, init_amazon_fba_db

    monkeypatch.setattr("app.services.importers.amazon_sp_api.AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    init_amazon_fba_db()
    with _connect() as connection:
        _upsert_inbound_shipment(
            connection,
            shipment={"ShipmentId": "FBA-RECEIPT-1", "ShipmentStatus": "RECEIVING"},
            items=[],
        )
        first = connection.execute("SELECT inventory_eligible_at FROM amazon_inbound_shipments WHERE shipment_id = 'FBA-RECEIPT-1'").fetchone()[0]
        _upsert_inbound_shipment(
            connection,
            shipment={"ShipmentId": "FBA-RECEIPT-1", "ShipmentStatus": "CLOSED"},
            items=[],
        )
        second = connection.execute("SELECT inventory_eligible_at FROM amazon_inbound_shipments WHERE shipment_id = 'FBA-RECEIPT-1'").fetchone()[0]

    assert first
    assert second == first


def test_delivered_shipment_cannot_create_fifo_lot(monkeypatch, tmp_path) -> None:
    import app.services.amazon_fba as amazon_fba
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    with importer._connect() as connection:
        importer._upsert_inbound_shipment(
            connection,
            shipment={"ShipmentId": "FBA-DELIVERED-1", "ShipmentStatus": "DELIVERED"},
            items=[{"SellerSKU": "SKU-1", "FulfillmentNetworkSKU": "FNSKU-1", "QuantityShipped": 1, "QuantityReceived": 1}],
        )
    invoice = amazon_fba.add_inbound_invoice(
        shipment_id="FBA-DELIVERED-1", supplier_name="Supplier", invoice_number="INV-DELIVERED",
        invoice_date="2026-07-01", currency="EUR", gross_cents=1100, net_cents=1000,
        vat_cents=100, document_path="invoice.pdf",
    )
    amazon_fba.add_inbound_invoice_line(
        invoice_id=invoice["id"], seller_sku="SKU-1", fnsku="FNSKU-1", asin="ASIN-1",
        title="Product", quantity=1, gross_cents=1100, net_cents=1000, vat_cents=100,
    )

    with pytest.raises(ValueError, match="received"):
        amazon_fba.confirm_inbound_product_costs("FBA-DELIVERED-1")


def test_partially_received_shipment_confirms_only_received_sku_costs(monkeypatch, tmp_path) -> None:
    import app.services.amazon_fba as amazon_fba
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    with importer._connect() as connection:
        importer._upsert_inbound_shipment(
            connection,
            shipment={"ShipmentId": "FBA-PARTIAL-1", "ShipmentStatus": "RECEIVING"},
            items=[
                {"SellerSKU": "SKU-RECEIVED", "FulfillmentNetworkSKU": "FNSKU-RECEIVED", "QuantityShipped": 2, "QuantityReceived": 2},
                {"SellerSKU": "SKU-PENDING", "FulfillmentNetworkSKU": "FNSKU-PENDING", "QuantityShipped": 3, "QuantityReceived": 0},
            ],
        )
    invoice = amazon_fba.add_inbound_invoice(
        shipment_id="FBA-PARTIAL-1", supplier_name="Supplier", invoice_number="INV-PARTIAL",
        invoice_date="2026-08-21", currency="EUR", gross_cents=1190, net_cents=1000,
        vat_cents=190, document_path="partial.pdf",
    )
    amazon_fba.add_inbound_invoice_line(
        invoice_id=invoice["id"], seller_sku="SKU-RECEIVED", fnsku="FNSKU-RECEIVED", asin="",
        title="Received", quantity=2, gross_cents=1190, net_cents=1000, vat_cents=190,
    )

    result = amazon_fba.confirm_inbound_product_costs("FBA-PARTIAL-1")

    assert [lot["seller_sku"] for lot in result["lots"]] == ["SKU-RECEIVED"]
    assert result["lots"][0]["available_quantity"] == 2


def test_inbound_invoice_rejects_mismatched_gross_net_and_vat(monkeypatch, tmp_path) -> None:
    import app.services.amazon_fba as amazon_fba
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    with importer._connect() as connection:
        importer._upsert_inbound_shipment(
            connection,
            shipment={"ShipmentId": "FBA-HEADER-1", "ShipmentStatus": "CLOSED"},
            items=[],
        )

    with pytest.raises(ValueError, match="gross_cents must equal net_cents plus vat_cents"):
        amazon_fba.add_inbound_invoice(
            shipment_id="FBA-HEADER-1", supplier_name="Supplier", invoice_number="INV-HEADER",
            invoice_date="2026-08-21", currency="EUR", gross_cents=1000, net_cents=900,
            vat_cents=50, document_path="header.pdf",
        )


def test_fifo_preserves_invoice_remainder_cents(monkeypatch, tmp_path) -> None:
    import app.services.amazon_fba as amazon_fba
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    with importer._connect() as connection:
        importer._upsert_inbound_shipment(
            connection,
            shipment={"ShipmentId": "FBA-ROUNDING-1", "ShipmentStatus": "CLOSED"},
            items=[{"SellerSKU": "SKU-1", "FulfillmentNetworkSKU": "FNSKU-1", "QuantityShipped": 3, "QuantityReceived": 3}],
        )
        connection.execute(
            "INSERT INTO amazon_orders(amazon_order_id, seller_order_id, currency, order_total_cents, raw_json, updated_at) VALUES ('ORDER-ROUNDING-1', 'ORDER-ROUNDING-1', 'EUR', 3000, '{}', '2026-07-30T00:00:00Z')"
        )
        connection.execute(
            "INSERT INTO amazon_order_items(id, amazon_order_id, seller_sku, title, quantity_ordered, quantity_shipped, currency, item_price_cents, item_tax_cents, raw_json) VALUES ('ITEM-ROUNDING-1', 'ORDER-ROUNDING-1', 'SKU-1', 'Product', 3, 3, 'EUR', 3000, 0, '{}')"
        )
        connection.commit()
    invoice = amazon_fba.add_inbound_invoice(
        shipment_id="FBA-ROUNDING-1", supplier_name="Supplier", invoice_number="INV-ROUNDING",
        invoice_date="2026-07-01", currency="EUR", gross_cents=1101, net_cents=1001,
        vat_cents=100, document_path="invoice.pdf",
    )
    amazon_fba.add_inbound_invoice_line(
        invoice_id=invoice["id"], seller_sku="SKU-1", fnsku="FNSKU-1", asin="ASIN-1",
        title="Product", quantity=3, gross_cents=1101, net_cents=1001, vat_cents=100,
    )
    amazon_fba.confirm_inbound_product_costs("FBA-ROUNDING-1")

    result = amazon_fba.allocate_order_fifo("ORDER-ROUNDING-1")

    assert result["allocated_cogs_cents"] == 1001


def test_confirmed_invoice_lines_are_locked(monkeypatch, tmp_path) -> None:
    import app.services.amazon_fba as amazon_fba
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    with importer._connect() as connection:
        importer._upsert_inbound_shipment(
            connection,
            shipment={"ShipmentId": "FBA-LOCK-1", "ShipmentStatus": "CLOSED"},
            items=[{"SellerSKU": "SKU-1", "FulfillmentNetworkSKU": "FNSKU-1", "QuantityShipped": 1, "QuantityReceived": 1}],
        )
    invoice = amazon_fba.add_inbound_invoice(
        shipment_id="FBA-LOCK-1", supplier_name="Supplier", invoice_number="INV-LOCK",
        invoice_date="2026-07-01", currency="EUR", gross_cents=1100, net_cents=1000,
        vat_cents=100, document_path="invoice.pdf",
    )
    amazon_fba.add_inbound_invoice_line(
        invoice_id=invoice["id"], seller_sku="SKU-1", fnsku="FNSKU-1", asin="ASIN-1",
        title="Product", quantity=1, gross_cents=1100, net_cents=1000, vat_cents=100,
    )
    amazon_fba.confirm_inbound_product_costs("FBA-LOCK-1")

    with pytest.raises(ValueError, match="confirmed"):
        amazon_fba.add_inbound_invoice_line(
            invoice_id=invoice["id"], seller_sku="SKU-1", fnsku="FNSKU-1", asin="ASIN-1",
            title="Changed", quantity=1, gross_cents=1320, net_cents=1200, vat_cents=120,
        )


def test_inventory_summary_deduplicates_marketplace_snapshots(monkeypatch, tmp_path) -> None:
    import app.services.amazon_fba as amazon_fba
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    with importer._connect() as connection:
        for marketplace_id in ("A1PA6795UKMFR9", "A13V1IB3VIYZZH"):
            connection.execute(
                """
                INSERT INTO amazon_inventory_snapshots(
                    id, captured_at, marketplace_id, seller_sku, fnsku, asin, product_name,
                    fulfillable_quantity, inbound_working_quantity, inbound_shipped_quantity,
                    reserved_quantity, unfulfillable_quantity, raw_json
                ) VALUES (?, '2026-07-23T10:00:00Z', ?, 'H10B', 'FNSKU-H10B', 'B0D95XYL1R', 'HIBREW H10B', 10, 5, 0, 0, 0, '{}')
                """,
                (marketplace_id, marketplace_id),
            )
        connection.commit()

    summary = amazon_fba.get_amazon_inventory_summary()
    assert summary["totals"]["unique_skus"] == 1
    assert summary["totals"]["fulfillable"] == 10
    assert summary["totals"]["inbound_working"] == 5


def test_orders_queries_each_marketplace_individually(monkeypatch) -> None:
    from app.services.importers.amazon_sp_api import AmazonSpApiClient, AmazonSpApiConfig

    client = AmazonSpApiClient(AmazonSpApiConfig("client", "secret", "refresh"))
    requested_marketplaces: list[str] = []

    def request_json(path, *, params=None, method="GET", body=None):
        assert path == "/orders/v0/orders"
        marketplace_id = params["MarketplaceIds"]
        assert isinstance(marketplace_id, str)
        requested_marketplaces.append(marketplace_id)
        return {"payload": {"Orders": [{"AmazonOrderId": f"ORDER-{marketplace_id}", "MarketplaceId": marketplace_id}]}}

    monkeypatch.setattr(client, "request_json", request_json)
    orders, errors = client.orders(["DE", "FR"], "2026-01-01T00:00:00Z")

    assert requested_marketplaces == ["DE", "FR"]
    assert [order["AmazonOrderId"] for order in orders] == ["ORDER-DE", "ORDER-FR"]
    assert errors == []


def test_order_items_retries_quota_exceeded(monkeypatch) -> None:
    import app.services.importers.amazon_sp_api as importer
    from app.services.importers.amazon_sp_api import AmazonSpApiClient, AmazonSpApiConfig, AmazonSpApiError

    client = AmazonSpApiClient(AmazonSpApiConfig("client", "secret", "refresh"))
    attempts = 0
    sleeps: list[float] = []

    def request_json(path, *, params=None, method="GET", body=None):
        nonlocal attempts
        assert path == "/orders/v0/orders/ORDER-1/orderItems"
        attempts += 1
        if attempts < 3:
            raise AmazonSpApiError("SP-API 429 for /orders/v0/orders/ORDER-1/orderItems")
        return {"payload": {"OrderItems": [{"ASIN": "B0TEST", "SellerSKU": "SKU-1"}]}}

    monkeypatch.setattr(client, "request_json", request_json)
    monkeypatch.setattr(importer.time, "sleep", lambda seconds: sleeps.append(seconds))

    assert client.order_items("ORDER-1") == [{"ASIN": "B0TEST", "SellerSKU": "SKU-1"}]
    assert attempts == 3
    assert sleeps == [1.5, 3.0]


def test_catalog_item_images_retries_quota_exceeded(monkeypatch) -> None:
    import app.services.importers.amazon_sp_api as importer
    from app.services.importers.amazon_sp_api import AmazonSpApiClient, AmazonSpApiConfig, AmazonSpApiError

    client = AmazonSpApiClient(AmazonSpApiConfig("client", "secret", "refresh"))
    attempts = 0
    sleeps: list[float] = []

    def request_json(path, *, params=None, method="GET", body=None):
        nonlocal attempts
        assert path == "/catalog/2022-04-01/items/B0TEST"
        attempts += 1
        if attempts == 1:
            raise AmazonSpApiError("SP-API 429 for /catalog/2022-04-01/items/B0TEST")
        return {"images": []}

    monkeypatch.setattr(client, "request_json", request_json)
    monkeypatch.setattr(importer.time, "sleep", lambda seconds: sleeps.append(seconds))

    assert client.catalog_item_images("B0TEST", "A1PA6795UKMFR9") == {"image_url": "", "image_urls": []}
    assert attempts == 2
    assert sleeps == [1.5]


def test_amazon_api_bucket_key_groups_dynamic_order_item_paths() -> None:
    from app.services.importers.amazon_sp_api import amazon_api_bucket_key

    assert amazon_api_bucket_key("/orders/v0/orders/111-222/orderItems") == "order_items"
    assert amazon_api_bucket_key("/orders/v0/orders/333-444/orderItems") == "order_items"
    assert amazon_api_bucket_key("/orders/v0/orders") == "orders"
    assert amazon_api_bucket_key("/catalog/2022-04-01/items/B0TEST") == "catalog"
    assert amazon_api_bucket_key("/fba/inventory/v1/summaries") == "default"


def test_amazon_api_bucket_reservation_refills_and_calculates_wait(monkeypatch, tmp_path) -> None:
    from datetime import datetime, timedelta, timezone

    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    start = datetime(2026, 8, 20, tzinfo=timezone.utc)

    assert importer.reserve_amazon_api_token("catalog", now=start) == 0.0
    assert importer.reserve_amazon_api_token("catalog", now=start) == 0.0
    assert importer.reserve_amazon_api_token("catalog", now=start) == 0.5
    assert importer.reserve_amazon_api_token("catalog", now=start + timedelta(seconds=0.5)) == 0.0


def test_sync_amazon_fba_fetches_only_missing_order_items_newest_first(monkeypatch, tmp_path) -> None:
    import app.services.importers.amazon_sp_api as importer
    from app.services.importers.amazon_sp_api import AmazonSpApiClient

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    monkeypatch.setattr(
        importer,
        "load_amazon_sp_api_config",
        lambda: (importer.AmazonSpApiConfig("c", "s", "r"), []),
    )
    importer.init_amazon_fba_db()
    with importer._connect() as connection:
        importer._upsert_order(
            connection,
            {"AmazonOrderId": "COMPLETE", "MarketplaceId": "DE", "PurchaseDate": "2026-08-18T10:00:00Z"},
        )
        importer._upsert_order_items(
            connection,
            "COMPLETE",
            [{"ASIN": "B0COMPLETE", "SellerSKU": "SKU-C"}],
        )
        connection.commit()

    monkeypatch.setattr(
        AmazonSpApiClient,
        "marketplace_participations",
        lambda self: {"payload": [{"marketplace": {"id": "DE"}, "participation": {"isParticipating": True}}]},
    )
    monkeypatch.setattr(
        AmazonSpApiClient,
        "orders",
        lambda self, *args, **kwargs: ([
            {"AmazonOrderId": "OLD", "MarketplaceId": "DE", "LastUpdateDate": "2026-08-18T10:00:00Z"},
            {"AmazonOrderId": "NEW", "MarketplaceId": "DE", "LastUpdateDate": "2026-08-19T10:00:00Z"},
            {"AmazonOrderId": "COMPLETE", "MarketplaceId": "DE", "LastUpdateDate": "2026-08-20T10:00:00Z"},
        ], []),
    )
    requested: list[str] = []
    monkeypatch.setattr(AmazonSpApiClient, "order_items", lambda self, order_id: requested.append(order_id) or [])

    importer.sync_amazon_fba(
        include_orders=True,
        include_inventory=False,
        include_finances=False,
        include_inbound=False,
        include_settlement_reports=False,
    )

    assert requested == ["NEW", "OLD"]


def test_sync_amazon_fba_backfills_stale_local_orders_outside_delta_window(monkeypatch, tmp_path) -> None:
    """Regression: an order whose header is already stored locally but whose
    items never got fetched (e.g. an earlier quota error) must still be
    repaired by a later delta sync, even when Amazon's Orders API no longer
    returns that order because its LastUpdateDate has aged out of the
    delta's lookback window (e.g. the auto-refresh 'orders' task's 20
    minute LastUpdatedAfter filter)."""
    import app.services.importers.amazon_sp_api as importer
    from app.services.importers.amazon_sp_api import AmazonSpApiClient

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    monkeypatch.setattr(
        importer,
        "load_amazon_sp_api_config",
        lambda: (importer.AmazonSpApiConfig("c", "s", "r"), []),
    )
    importer.init_amazon_fba_db()
    with importer._connect() as connection:
        # Simulates an order fetched days ago whose item fetch previously
        # failed; its LastUpdateDate is now well outside any short delta
        # window, so the Orders API delta call below will not return it.
        importer._upsert_order(
            connection,
            {
                "AmazonOrderId": "STALE",
                "MarketplaceId": "DE",
                "PurchaseDate": "2026-08-16T08:00:00Z",
                "LastUpdateDate": "2026-08-16T08:00:00Z",
            },
        )
        connection.commit()

    monkeypatch.setattr(
        AmazonSpApiClient,
        "marketplace_participations",
        lambda self: {"payload": [{"marketplace": {"id": "DE"}, "participation": {"isParticipating": True}}]},
    )
    # The delta call finds nothing new -- mirrors the auto-refresh "orders"
    # task's narrow 20-minute LastUpdatedAfter window.
    monkeypatch.setattr(AmazonSpApiClient, "orders", lambda self, *args, **kwargs: ([], []))
    requested: list[str] = []
    monkeypatch.setattr(AmazonSpApiClient, "order_items", lambda self, order_id: requested.append(order_id) or [])

    importer.sync_amazon_fba(
        include_orders=True,
        include_inventory=False,
        include_finances=False,
        include_inbound=False,
        include_settlement_reports=False,
        lookback_minutes=20,
    )

    assert requested == ["STALE"]


def test_orders_missing_items_caps_backlog_per_sync_to_avoid_unbounded_bursts(monkeypatch, tmp_path) -> None:
    """A large historical backlog (e.g. first install) must not schedule an
    unbounded number of order_items requests in a single sync pass -- each
    call is paced at 0.5 req/s, so hundreds of missing orders would otherwise
    turn one sync into a multi-hour blocking call. Cap the backlog processed
    per call and let the newest-first ordering + repeated sync cycles work
    through the rest over time."""
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    with importer._connect() as connection:
        for index in range(importer.MAX_ORDER_ITEMS_BACKFILL_PER_SYNC + 10):
            importer._upsert_order(
                connection,
                {
                    "AmazonOrderId": f"ORDER-{index:04d}",
                    "MarketplaceId": "DE",
                    "PurchaseDate": f"2026-08-{(index % 27) + 1:02d}T08:00:00Z",
                    "LastUpdateDate": f"2026-08-{(index % 27) + 1:02d}T08:00:00Z",
                },
            )
        connection.commit()
        missing = importer._orders_missing_items(connection, [])

    assert len(missing) == importer.MAX_ORDER_ITEMS_BACKFILL_PER_SYNC


def test_amazon_status_reports_pending_items_and_rate_limits(monkeypatch, tmp_path) -> None:
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    with importer._connect() as connection:
        importer._upsert_order(connection, {"AmazonOrderId": "MISSING", "MarketplaceId": "DE"})
        importer._upsert_order(connection, {"AmazonOrderId": "COMPLETE", "MarketplaceId": "DE"})
        importer._upsert_order_items(
            connection,
            "COMPLETE",
            [{"ASIN": "B0COMPLETE", "SellerSKU": "SKU-C"}],
        )
        connection.commit()

    status = importer.build_amazon_fba_status()

    assert status["pending_order_items"] == 1
    assert "order_items" in status["rate_limits"]


def test_sync_starts_rate_limited_marketplace_request_outside_database_transaction(monkeypatch, tmp_path) -> None:
    import app.services.importers.amazon_sp_api as importer
    from app.services.importers.amazon_sp_api import AmazonSpApiClient

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    monkeypatch.setattr(
        importer,
        "load_amazon_sp_api_config",
        lambda: (importer.AmazonSpApiConfig("client", "secret", "refresh"), []),
    )
    monkeypatch.setattr(AmazonSpApiClient, "_lwa_access_token", lambda self: "token")

    def marketplace_response(request, timeout):
        assert request.full_url.endswith("/sellers/v1/marketplaceParticipations")
        return _FakeJsonResponse({
            "payload": [{
                "marketplace": {"id": "DE", "name": "Amazon.de", "countryCode": "DE"},
                "participation": {"isParticipating": True},
            }],
        })

    monkeypatch.setattr(importer, "urlopen", marketplace_response)

    result = importer.sync_amazon_fba(
        include_orders=False,
        include_inventory=False,
        include_finances=False,
        include_inbound=False,
        include_settlement_reports=False,
    )

    assert result["status"] == "success"
    assert result["marketplaces"] == 1


def test_sync_amazon_fba_end_to_end_never_locks_database_across_full_pipeline(monkeypatch, tmp_path) -> None:
    """Exercises marketplaces + orders + order_items + catalog_images through the
    real AmazonSpApiClient.request_json() pacing path (not mocked at the method
    level), on a real file-backed SQLite database, so any future regression that
    re-introduces "network call while holding an open write transaction" fails
    with 'database is locked' instead of silently passing."""
    import app.services.importers.amazon_sp_api as importer
    from app.services.importers.amazon_sp_api import AmazonSpApiClient

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    monkeypatch.setattr(
        importer,
        "load_amazon_sp_api_config",
        lambda: (importer.AmazonSpApiConfig("client", "secret", "refresh"), []),
    )
    monkeypatch.setattr(AmazonSpApiClient, "_lwa_access_token", lambda self: "token")

    def fake_urlopen(request, timeout):
        url = request.full_url
        if url.endswith("/sellers/v1/marketplaceParticipations"):
            return _FakeJsonResponse({
                "payload": [{
                    "marketplace": {"id": "DE", "name": "Amazon.de", "countryCode": "DE"},
                    "participation": {"isParticipating": True},
                }],
            })
        if "/orders/v0/orders/" in url and url.endswith("/orderItems"):
            return _FakeJsonResponse({"payload": {"OrderItems": [{"ASIN": "B0TEST", "SellerSKU": "SKU-1"}]}})
        if url.startswith("https://sellingpartnerapi-eu.amazon.com/orders/v0/orders?"):
            return _FakeJsonResponse({"payload": {"Orders": [
                {"AmazonOrderId": "ORDER-1", "MarketplaceId": "DE", "PurchaseDate": "2026-08-19T00:00:00Z"},
                {"AmazonOrderId": "ORDER-2", "MarketplaceId": "DE", "PurchaseDate": "2026-08-18T00:00:00Z"},
            ]}})
        if url.startswith("https://sellingpartnerapi-eu.amazon.com/catalog/2022-04-01/items/"):
            return _FakeJsonResponse({"images": []})
        raise AssertionError(f"unexpected URL in test: {url}")

    monkeypatch.setattr(importer, "urlopen", fake_urlopen)

    result = importer.sync_amazon_fba(
        include_orders=True,
        include_inventory=False,
        include_finances=False,
        include_inbound=False,
        include_settlement_reports=False,
        include_catalog_images=True,
        lookback_days=30,
    )

    assert result["status"] == "success"
    assert result["orders"] == 2

    status = importer.build_amazon_fba_status()
    assert status["pending_order_items"] == 0


def test_connect_sets_a_generous_busy_timeout_for_brief_writer_contention(monkeypatch, tmp_path) -> None:
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")

    with importer._connect() as connection:
        busy_timeout_ms = connection.execute("PRAGMA busy_timeout").fetchone()[0]

    assert busy_timeout_ms >= 10000


class _FakeJsonResponse:
    def __init__(self, payload: dict, headers: dict[str, str] | None = None) -> None:
        self._payload = payload
        self.headers = headers or {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback) -> bool:
        return False

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")


def _quota_error(code: int, headers: dict[str, str]) -> HTTPError:
    return HTTPError(
        "https://sellingpartnerapi-eu.amazon.com/test",
        code,
        "quota exceeded",
        headers,
        io.BytesIO(b'{"errors":[{"code":"QuotaExceeded"}]}'),
    )


def test_request_json_waits_for_bucket_before_opening_request(monkeypatch) -> None:
    import app.services.importers.amazon_sp_api as importer
    from app.services.importers.amazon_sp_api import AmazonSpApiClient, AmazonSpApiConfig

    client = AmazonSpApiClient(AmazonSpApiConfig("client", "secret", "refresh"))
    waits: list[float] = []
    monkeypatch.setattr(client, "_lwa_access_token", lambda: "token")
    monkeypatch.setattr(importer, "reserve_amazon_api_token", lambda bucket: 2.0 if not waits else 0.0)
    monkeypatch.setattr(importer.time, "sleep", lambda seconds: waits.append(seconds))
    monkeypatch.setattr(importer, "urlopen", lambda request, timeout: _FakeJsonResponse({"payload": {}}))

    assert client.request_json("/orders/v0/orders/ORDER-1/orderItems") == {"payload": {}}
    assert waits == [2.0]


def test_request_json_records_retry_after_on_quota_error(monkeypatch) -> None:
    import app.services.importers.amazon_sp_api as importer
    from app.services.importers.amazon_sp_api import AmazonSpApiClient, AmazonSpApiConfig, AmazonSpApiError

    client = AmazonSpApiClient(AmazonSpApiConfig("client", "secret", "refresh"))
    recorded: list[tuple[str, float | None, str]] = []
    monkeypatch.setattr(client, "_lwa_access_token", lambda: "token")
    monkeypatch.setattr(importer, "reserve_amazon_api_token", lambda bucket: 0.0)
    monkeypatch.setattr(
        importer,
        "record_amazon_api_throttle",
        lambda bucket, *, retry_after_seconds, error: recorded.append((bucket, retry_after_seconds, error)),
    )

    def urlopen_quota_error(request, timeout):
        raise _quota_error(429, {"Retry-After": "7"})

    monkeypatch.setattr(importer, "urlopen", urlopen_quota_error)

    with pytest.raises(AmazonSpApiError):
        client.request_json("/orders/v0/orders/ORDER-1/orderItems")

    assert recorded[0][0] == "order_items"
    assert recorded[0][1] == 7.0


def test_settlement_report_line_imports_order_sales_and_fees(monkeypatch, tmp_path) -> None:
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    row = {
        "settlement-id": "SETTLEMENT-1",
        "posted-date": "2026-07-21T10:49:41+00:00",
        "order-id": "302-1301490-2524356",
        "merchant-order-id": "302-1301490-2524356",
        "marketplace-name": "Amazon.de",
        "currency": "EUR",
        "transaction-type": "Order",
        "price-type": "Principal",
        "price-amount": "152.90",
        "item-related-fee-type": "Commission",
        "item-related-fee-amount": "-26.76",
        "shipment-fee-type": "FBA Fulfillment Fee",
        "shipment-fee-amount": "-6.42",
        "total-amount": "119.72",
    }

    with importer._connect() as connection:
        assert importer._import_settlement_report_row(connection, "REPORT-1", row) is True
        assert importer._import_settlement_report_row(connection, "REPORT-2", row) is True
        event = connection.execute(
            "SELECT amazon_order_id, sales_cents, fees_cents, net_cents FROM amazon_financial_events"
        ).fetchone()
        event_count = connection.execute(
            "SELECT COUNT(*) FROM amazon_financial_events"
        ).fetchone()[0]
        order = connection.execute(
            "SELECT amazon_order_id, seller_order_id FROM amazon_orders"
        ).fetchone()

    assert tuple(event) == ("302-1301490-2524356", 15290, 3318, 11972)
    assert tuple(order) == ("302-1301490-2524356", "302-1301490-2524356")
    assert event_count == 1


def test_settlement_report_imports_account_level_fee_rows(monkeypatch, tmp_path) -> None:
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    with importer._connect() as connection:
        row = {
            "settlement-id": "SETTLEMENT-2",
            "posted-date": "2026-07-07T20:38:31+00:00",
            "transaction-type": "Inbound Transportation Fee",
            "currency": "EUR",
            "other-amount": "-44.00",
        }
        assert importer._import_settlement_report_row(connection, "REPORT-2", row) is True
        event = connection.execute(
            "SELECT amazon_order_id, fees_cents, net_cents FROM amazon_financial_events WHERE settlement_id = 'SETTLEMENT-2'"
        ).fetchone()

    assert tuple(event) == (None, 4400, -4400)


def test_finance_overview_prefers_settlement_report_over_service_fee_duplicate(monkeypatch, tmp_path) -> None:
    import app.services.amazon_fba as amazon_fba
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    with importer._connect() as connection:
        connection.executemany(
            """
            INSERT INTO amazon_financial_events(
                id, event_type, settlement_id, posted_date, financial_finality,
                currency, sales_cents, fees_cents, net_cents, raw_json
            ) VALUES (?, ?, ?, ?, 'released', 'EUR', 0, ?, ?, '{}')
            """,
            [
                ("api-fee", "ServiceFeeEventList", "GROUP-1", "2026-07-16T00:00:00Z", 4725, -4725),
                ("api-recovery", "DebtRecoveryEventList", "GROUP-1", "2026-07-16T00:00:00Z", 0, 4725),
                ("report-fee", "SettlementReportLine", "REPORT-1", "2026-07-07T00:00:00Z", 4725, -4725),
                ("report-recovery", "SettlementReportLine", "REPORT-1", "2026-07-18T00:00:00Z", 0, 4725),
            ],
        )
        connection.commit()

    overview = amazon_fba.get_amazon_finance_overview()
    assert [event["event_type"] for event in overview["events"]] == ["SettlementReportLine", "SettlementReportLine"]


def test_settlement_report_rows_inherit_currency_from_settlement_header() -> None:
    from app.services.importers.amazon_sp_api import _normalize_settlement_report_rows

    rows = _normalize_settlement_report_rows([
        {"settlement-id": "S-GBP", "currency": "GBP", "total-amount": "-29.75"},
        {"settlement-id": "S-GBP", "currency": "", "other-amount": "-29.75"},
    ])

    assert rows[1]["currency"] == "GBP"


def test_list_settlement_reports_paginates() -> None:
    from app.services.importers.amazon_sp_api import AmazonSpApiClient, AmazonSpApiConfig

    client = AmazonSpApiClient(AmazonSpApiConfig("client", "secret", "refresh"))
    calls: list[dict[str, object]] = []

    def request_json(path, *, params=None, method="GET", body=None):
        calls.append(params or {})
        if len(calls) == 1:
            return {"reports": [{"reportId": "R-1", "processingStatus": "DONE"}], "nextToken": "NEXT"}
        return {"reports": [{"reportId": "R-2", "processingStatus": "IN_QUEUE"}]}

    client.request_json = request_json
    reports = client.list_reports("GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE")

    assert [report["reportId"] for report in reports] == ["R-1", "R-2"]
    assert calls[0]["reportTypes"] == "GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE"
    assert calls[1]["nextToken"] == "NEXT"


def test_settlement_report_classifies_recovery_without_fee(monkeypatch, tmp_path) -> None:
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    with importer._connect() as connection:
        row = {
            "settlement-id": "SETTLEMENT-3",
            "posted-date": "2026-06-25T21:41:41+00:00",
            "transaction-type": "Successful charge",
            "currency": "GBP",
            "other-amount": "29.75",
        }
        assert importer._import_settlement_report_row(connection, "REPORT-3", row) is True
        event = connection.execute(
            "SELECT fees_cents, net_cents FROM amazon_financial_events WHERE settlement_id = 'SETTLEMENT-3'"
        ).fetchone()

    assert tuple(event) == (0, 2975)


def test_ready_to_ship_is_not_received() -> None:
    from app.services.importers.amazon_sp_api import normalize_fba_status

    assert normalize_fba_status("READY_TO_SHIP") == {
        "label": "Nicht versendet",
        "received": False,
        "inventory_eligible": False,
    }


def test_shipment_items_are_deduplicated_by_sku_and_fnsku() -> None:
    from app.services.importers.amazon_sp_api import normalize_shipment_items

    rows = normalize_shipment_items([
        {"SellerSKU": "SKU-1", "FulfillmentNetworkSKU": "FNSKU-1", "QuantityShipped": 6, "QuantityReceived": 0},
        {"SellerSKU": "SKU-1", "FulfillmentNetworkSKU": "FNSKU-1", "QuantityShipped": 6, "QuantityReceived": 0},
    ])

    assert rows == [{"seller_sku": "SKU-1", "fnsku": "FNSKU-1", "quantity_shipped": 6, "quantity_received": 0}]


def test_inbound_cost_without_shipment_id_is_unassigned() -> None:
    from app.services.importers.amazon_sp_api import suggest_shipment_for_inbound_cost

    assert suggest_shipment_for_inbound_cost(4725, "EUR", {}, []) is None


def test_inbound_shipments_include_unreceived_statuses_and_dedupe() -> None:
    from app.services.importers.amazon_sp_api import AmazonSpApiClient, AmazonSpApiConfig

    client = AmazonSpApiClient(AmazonSpApiConfig("client", "secret", "refresh"))
    calls: list[dict[str, object]] = []

    def request_json(path, *, params=None, method="GET", body=None):
        assert path == "/fba/inbound/v0/shipments"
        calls.append(params or {})
        if len(calls) == 1:
            return {"payload": {"ShipmentData": [
                {"ShipmentId": "FBA-1", "ShipmentStatus": "CLOSED"},
                {"ShipmentId": "FBA-2", "ShipmentStatus": "READY_TO_SHIP"},
            ], "NextToken": "NEXT"}}
        return {"payload": {"ShipmentData": [{"ShipmentId": "FBA-1", "ShipmentStatus": "CLOSED"}]}}

    client.request_json = request_json
    shipments = client.inbound_shipments(["DE"])

    assert [shipment["ShipmentId"] for shipment in shipments] == ["FBA-1", "FBA-2"]
    assert "READY_TO_SHIP" in calls[0]["ShipmentStatusList"]
    assert "CLOSED" in calls[0]["ShipmentStatusList"]
    assert calls[1]["QueryType"] == "NEXT_TOKEN"


def test_bulk_inbound_items_groups_all_shipments() -> None:
    from app.services.importers.amazon_sp_api import AmazonSpApiClient, AmazonSpApiConfig

    client = AmazonSpApiClient(AmazonSpApiConfig("client", "secret", "refresh"))
    calls: list[dict[str, object]] = []

    def request_json(path, *, params=None, method="GET", body=None):
        assert path == "/fba/inbound/v0/shipmentItems"
        calls.append(params or {})
        if len(calls) == 1:
            return {"payload": {"ItemData": [
                {"ShipmentId": "FBA-1", "SellerSKU": "SKU-1", "FulfillmentNetworkSKU": "FNSKU-1", "QuantityShipped": 11, "QuantityReceived": 11},
            ], "NextToken": "NEXT"}}
        return {"payload": {"ItemData": [
            {"ShipmentId": "FBA-2", "SellerSKU": "SKU-2", "FulfillmentNetworkSKU": "FNSKU-2", "QuantityShipped": 12, "QuantityReceived": 0},
        ]}}

    client.request_json = request_json
    grouped = client.bulk_inbound_shipment_items("DE", lookback_days=90)

    assert grouped["FBA-1"][0]["quantity_received"] == 11
    assert grouped["FBA-2"][0]["quantity_shipped"] == 12
    assert calls[0]["QueryType"] == "DATE_RANGE"
    assert calls[1]["QueryType"] == "NEXT_TOKEN"


def test_upsert_inbound_shipment_preserves_received_quantities_and_transport_quote(monkeypatch, tmp_path) -> None:
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    with importer._connect() as connection:
        importer._upsert_inbound_shipment(
            connection,
            shipment={
                "ShipmentId": "FBA-3",
                "ShipmentName": "FBA test",
                "ShipmentStatus": "IN_TRANSIT",
                "DestinationFulfillmentCenterId": "DTM1",
            },
            items=[{
                "SellerSKU": "SKU-1",
                "FulfillmentNetworkSKU": "FNSKU-1",
                "QuantityShipped": 12,
                "QuantityReceived": 0,
            }],
            modern={
                "plan_id": "PLAN-1",
                "shipment": {
                    "shipmentConfirmationId": "FBA-3",
                    "selectedTransportationOptionId": "OPTION-1",
                    "status": "IN_TRANSIT",
                },
                "options": [{
                    "transportationOptionId": "OPTION-1",
                    "carrier": {"name": "DHL"},
                    "shippingSolution": "AMAZON_PARTNERED_CARRIER",
                    "shippingMode": "GROUND_SMALL_PARCEL",
                    "quote": {"cost": {"amount": 3.74, "code": "EUR"}},
                }],
            },
        )
        shipment = connection.execute("SELECT shipment_id, status, plan_id FROM amazon_inbound_shipments").fetchone()
        item = connection.execute("SELECT quantity_shipped, quantity_received FROM amazon_inbound_shipment_items").fetchone()
        quote = connection.execute("SELECT carrier, quote_cents, selected FROM amazon_inbound_transport_options").fetchone()

    assert tuple(shipment) == ("FBA-3", "IN_TRANSIT", "PLAN-1")
    assert tuple(item) == (12, 0)
    assert tuple(quote) == ("DHL", 374, 1)


def test_list_inbound_shipments_exposes_not_sent_label(monkeypatch, tmp_path) -> None:
    import app.services.amazon_fba as amazon_fba
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    with importer._connect() as connection:
        importer._upsert_inbound_shipment(
            connection,
            shipment={"ShipmentId": "FBA-4", "ShipmentName": "Not sent", "ShipmentStatus": "READY_TO_SHIP"},
            items=[{"SellerSKU": "SKU-1", "FulfillmentNetworkSKU": "FNSKU-1", "QuantityShipped": 516, "QuantityReceived": 0}],
        )

    shipments = amazon_fba.list_inbound_shipments()

    assert shipments[0]["shipment_id"] == "FBA-4"
    assert shipments[0]["status_label"] == "Nicht versendet"
    assert shipments[0]["quantity_shipped"] == 516
    assert shipments[0]["quantity_received"] == 0


def test_sync_inbound_shipments_persists_legacy_and_modern_data(monkeypatch, tmp_path) -> None:
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    with importer._connect() as connection:
        connection.execute(
            "INSERT INTO sync_runs(id, started_at, status, requested_scopes_json) VALUES ('SYNC-1', '2026-07-30T00:00:00Z', 'running', '{}')"
        )
        connection.commit()

    class FakeClient:
        def inbound_shipments(self, marketplace_ids):
            return [{
                "ShipmentId": "FBA-5",
                "ShipmentName": "Transit",
                "ShipmentStatus": "IN_TRANSIT",
                "DestinationFulfillmentCenterId": "DTM1",
            }]

        def shipment_items(self, shipment_id):
            return [{"seller_sku": "SKU-1", "fnsku": "FNSKU-1", "quantity_shipped": 12, "quantity_received": 0}]

        def modern_inbound_shipments(self, marketplace_ids):
            return {
                "FBA-5": {
                    "plan_id": "PLAN-5",
                    "shipment": {"shipmentConfirmationId": "FBA-5", "status": "IN_TRANSIT"},
                    "items": [{"msku": "SKU-1", "fnsku": "FNSKU-1", "quantity": 12, "asin": "ASIN-1"}],
                    "boxes": [],
                    "options": [],
                }
            }

    summary = importer.sync_inbound_shipments(FakeClient(), ["DE"], "SYNC-1")

    assert summary == {"shipments": 1, "items": 1, "errors": []}
    with importer._connect() as connection:
        row = connection.execute("SELECT shipment_id, plan_id FROM amazon_inbound_shipments").fetchone()
        item = connection.execute("SELECT asin, quantity_received FROM amazon_inbound_shipment_items").fetchone()
    assert tuple(row) == ("FBA-5", "PLAN-5")
    assert tuple(item) == ("ASIN-1", 0)


def test_sync_inbound_finance_costs_keeps_account_level_fee_unassigned(monkeypatch, tmp_path) -> None:
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    with importer._connect() as connection:
        connection.execute(
            """
            INSERT INTO amazon_financial_events(
                id, event_type, settlement_id, financial_finality, currency,
                fees_cents, net_cents, raw_json
            ) VALUES ('EVENT-1', 'ServiceFeeEventList', 'SETTLEMENT-1', 'released', 'EUR', 4725, -4725, '{}')
            """
        )
        connection.execute(
            """
            INSERT INTO amazon_financial_components(id, event_id, component_type, amount_cents, currency, raw_json)
            VALUES ('COMP-1', 'EVENT-1', 'FeeList', -4400, 'EUR', '{"FeeType":"FBAInboundTransportationFee"}')
            """
        )
        connection.commit()

    importer.sync_inbound_finance_costs()

    with importer._connect() as connection:
        cost = connection.execute(
            "SELECT shipment_id, source_event_id, cost_type, amount_cents, status FROM amazon_inbound_costs"
        ).fetchone()
    assert tuple(cost) == (None, "EVENT-1", "FBAInboundTransportationFee", 4400, "unassigned")


def test_inbound_cost_and_invoice_attach_to_shipment(monkeypatch, tmp_path) -> None:
    import app.services.amazon_fba as amazon_fba
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    with importer._connect() as connection:
        importer._upsert_inbound_shipment(
            connection,
            shipment={"ShipmentId": "FBA-6", "ShipmentStatus": "CLOSED"},
            items=[{"SellerSKU": "SKU-1", "FulfillmentNetworkSKU": "FNSKU-1", "QuantityShipped": 11, "QuantityReceived": 11}],
        )

    cost = amazon_fba.add_inbound_cost(
        shipment_id="FBA-6",
        cost_type="supplier_product",
        amount_cents=12_500,
    )
    confirmed = amazon_fba.assign_inbound_cost(cost_id=cost["id"], shipment_id="FBA-6")
    invoice = amazon_fba.add_inbound_invoice(
        shipment_id="FBA-6",
        supplier_name="Supplier",
        invoice_number="INV-1",
        invoice_date="2026-07-01",
        currency="EUR",
        gross_cents=12_500,
        net_cents=10_504,
        vat_cents=1_996,
        document_path="storage/documents/amazon-fba/FBA-6/invoice.pdf",
    )

    assert confirmed["status"] == "confirmed"
    assert invoice["shipment_id"] == "FBA-6"
    assert amazon_fba.get_inbound_shipment("FBA-6")["invoices"][0]["invoice_number"] == "INV-1"
    assert len(amazon_fba.get_inbound_shipment("FBA-6")["costs"]) == 2


def test_modern_finance_transaction_matches_inbound_fee_to_fba_shipment() -> None:
    from app.services.importers.amazon_sp_api import extract_modern_inbound_costs

    transactions = [{
        "transactionId": "TX-1",
        "transactionType": "ServiceFee",
        "totalAmount": {"currencyAmount": -47.25, "currencyCode": "EUR"},
        "items": [{
            "relatedIdentifiers": [{
                "itemRelatedIdentifierName": "TRANSACTION_ID",
                "itemRelatedIdentifierValue": "1165325127202:FBA15M02LDQF",
            }],
            "breakdowns": [{
                "breakdownType": "AmazonFees",
                "breakdowns": [{
                    "breakdownType": "FBAInboundTransportationFee",
                    "breakdownAmount": {"currencyAmount": -44.0, "currencyCode": "EUR"},
                }, {
                    "breakdownType": "FBAInboundTransportationProgramFee",
                    "breakdownAmount": {"currencyAmount": -3.25, "currencyCode": "EUR"},
                }],
            }],
        }],
    }]

    costs = extract_modern_inbound_costs(transactions)

    assert {(cost["shipment_id"], cost["cost_type"], cost["amount_cents"]) for cost in costs} == {
        ("FBA15M02LDQF", "FBAInboundTransportationFee", 4400),
        ("FBA15M02LDQF", "FBAInboundTransportationProgramFee", 325),
    }


def test_financial_transactions_paginate() -> None:
    from app.services.importers.amazon_sp_api import AmazonSpApiClient, AmazonSpApiConfig

    client = AmazonSpApiClient(AmazonSpApiConfig("client", "secret", "refresh"))
    calls: list[dict[str, object]] = []

    def request_json(path, *, params=None, method="GET", body=None):
        assert path == "/finances/2024-06-19/transactions"
        calls.append(params or {})
        if len(calls) == 1:
            return {"payload": {"transactions": [{"transactionId": "TX-1"}], "nextToken": "NEXT"}}
        return {"payload": {"transactions": [{"transactionId": "TX-2"}]}}

    client.request_json = request_json
    transactions = client.financial_transactions("2026-01-01T00:00:00Z", "2026-07-01T00:00:00Z", "DE")

    assert [transaction["transactionId"] for transaction in transactions] == ["TX-1", "TX-2"]
    assert calls[0]["postedAfter"] == "2026-01-01T00:00:00Z"
    assert calls[1]["nextToken"] == "NEXT"


def test_modern_inbound_costs_are_persisted_on_exact_shipment(monkeypatch, tmp_path) -> None:
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    with importer._connect() as connection:
        importer._upsert_inbound_shipment(
            connection,
            shipment={"ShipmentId": "FBA15M02LDQF", "ShipmentStatus": "CLOSED"},
            items=[{"SellerSKU": "SKU-1", "FulfillmentNetworkSKU": "FNSKU-1", "QuantityShipped": 11, "QuantityReceived": 11}],
        )

    imported = importer.sync_modern_inbound_costs([{
        "transactionId": "TX-2",
        "items": [{
            "relatedIdentifiers": [{"itemRelatedIdentifierName": "TRANSACTION_ID", "itemRelatedIdentifierValue": "1165325127202:FBA15M02LDQF"}],
            "breakdowns": [{"breakdownType": "FBAInboundTransportationFee", "breakdownAmount": {"currencyAmount": -44, "currencyCode": "EUR"}}],
        }],
    }])

    assert imported == 1
    with importer._connect() as connection:
        cost = connection.execute("SELECT shipment_id, amount_cents, status FROM amazon_inbound_costs").fetchone()
    assert tuple(cost) == ("FBA15M02LDQF", 4400, "actual")


def test_add_inbound_cost_persists_notes(monkeypatch, tmp_path) -> None:
    import app.services.amazon_fba as amazon_fba
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    with importer._connect() as connection:
        importer._upsert_inbound_shipment(
            connection,
            shipment={"ShipmentId": "FBA-NOTES", "ShipmentStatus": "CLOSED"},
            items=[{"SellerSKU": "SKU-1", "FulfillmentNetworkSKU": "FNSKU-1", "QuantityShipped": 5, "QuantityReceived": 5}],
        )

    cost = amazon_fba.add_inbound_cost(
        shipment_id="FBA-NOTES",
        cost_type="supplier_product",
        amount_cents=5_000,
        notes="Zollgebuehr fuer Los 3",
    )

    with importer._connect() as connection:
        row = connection.execute("SELECT notes FROM amazon_inbound_costs WHERE id = ?", (cost["id"],)).fetchone()
    assert row["notes"] == "Zollgebuehr fuer Los 3"


def test_inbound_cost_router_request_model_matches_service_signature() -> None:
    import inspect

    from app.routers.amazon import InboundCostRequest
    from app.services.amazon_fba import add_inbound_cost

    service_params = set(inspect.signature(add_inbound_cost).parameters) - {"shipment_id"}
    request_fields = set(InboundCostRequest.model_fields)
    assert request_fields.issubset(service_params), (
        f"InboundCostRequest fields not accepted by add_inbound_cost: {request_fields - service_params}"
    )


def test_load_amazon_sp_api_config_prefers_env_vars_over_secrets_file(monkeypatch, tmp_path) -> None:
    import app.services.importers.amazon_sp_api as importer

    secrets_path = tmp_path / "amazon-sp-api-secrets.json"
    secrets_path.write_text(
        json.dumps({"client_id": "file-client", "client_secret": "file-secret", "refresh_token": "file-refresh"}),
        encoding="utf-8",
    )
    monkeypatch.setattr(importer, "AMAZON_SP_API_SECRETS_PATH", secrets_path)
    monkeypatch.setenv("AMAZON_SP_API_CLIENT_ID", "env-client")
    monkeypatch.setenv("AMAZON_SP_API_CLIENT_SECRET", "env-secret")
    monkeypatch.setenv("AMAZON_SP_API_REFRESH_TOKEN", "env-refresh")

    config, missing = importer.load_amazon_sp_api_config()

    assert missing == []
    assert config.client_id == "env-client"
    assert config.client_secret == "env-secret"
    assert config.refresh_token == "env-refresh"


def test_load_amazon_sp_api_config_falls_back_to_secrets_file_without_env_vars(monkeypatch, tmp_path) -> None:
    import app.services.importers.amazon_sp_api as importer

    secrets_path = tmp_path / "amazon-sp-api-secrets.json"
    secrets_path.write_text(
        json.dumps({"client_id": "file-client", "client_secret": "file-secret", "refresh_token": "file-refresh"}),
        encoding="utf-8",
    )
    monkeypatch.setattr(importer, "AMAZON_SP_API_SECRETS_PATH", secrets_path)
    monkeypatch.delenv("AMAZON_SP_API_CLIENT_ID", raising=False)
    monkeypatch.delenv("AMAZON_SP_API_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("AMAZON_SP_API_REFRESH_TOKEN", raising=False)

    config, missing = importer.load_amazon_sp_api_config()

    assert missing == []
    assert config.client_id == "file-client"
    assert config.client_secret == "file-secret"
    assert config.refresh_token == "file-refresh"


def test_load_amazon_sp_api_config_missing_everywhere_reports_missing(monkeypatch, tmp_path) -> None:
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_SP_API_SECRETS_PATH", tmp_path / "does-not-exist.json")
    monkeypatch.delenv("AMAZON_SP_API_CLIENT_ID", raising=False)
    monkeypatch.delenv("AMAZON_SP_API_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("AMAZON_SP_API_REFRESH_TOKEN", raising=False)

    config, missing = importer.load_amazon_sp_api_config()

    assert config is None
    assert set(missing) == {"client_id", "client_secret", "refresh_token"}


def test_orders_keeps_results_from_marketplaces_before_a_failing_one(monkeypatch) -> None:
    from app.services.importers.amazon_sp_api import AmazonSpApiClient, AmazonSpApiConfig, AmazonSpApiError

    client = AmazonSpApiClient(AmazonSpApiConfig("client", "secret", "refresh"))

    def request_json(path, *, params=None, method="GET", body=None):
        marketplace_id = params.get("MarketplaceIds")
        if marketplace_id == "FR":
            raise AmazonSpApiError("SP-API 429 for /orders/v0/orders: quota exceeded")
        return {"payload": {"Orders": [{"AmazonOrderId": f"ORDER-{marketplace_id}", "MarketplaceId": marketplace_id}]}}

    monkeypatch.setattr(client, "request_json", request_json)
    orders, errors = client.orders(["DE", "FR", "IT"], "2026-01-01T00:00:00Z")

    assert [order["AmazonOrderId"] for order in orders] == ["ORDER-DE", "ORDER-IT"]
    assert errors == [{"marketplace_id": "FR", "error": "SP-API 429 for /orders/v0/orders: quota exceeded"}]


def test_order_marketplace_errors_are_reported_in_sync_summary(monkeypatch, tmp_path) -> None:
    import app.services.importers.amazon_sp_api as importer
    from app.services.importers.amazon_sp_api import AmazonSpApiClient, AmazonSpApiError

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    monkeypatch.setattr(importer, "load_amazon_sp_api_config", lambda: (importer.AmazonSpApiConfig("c", "s", "r"), []))

    def fake_marketplace_participations(self):
        return {"payload": [
            {"marketplace": {"id": "DE", "name": "DE", "countryCode": "DE", "defaultCurrencyCode": "EUR", "domainName": "amazon.de"}, "participation": {}},
            {"marketplace": {"id": "FR", "name": "FR", "countryCode": "FR", "defaultCurrencyCode": "EUR", "domainName": "amazon.fr"}, "participation": {}},
        ]}

    def fake_orders(self, marketplace_ids, created_after, *, updated_after=None):
        return [{"AmazonOrderId": "ORDER-DE", "MarketplaceId": "DE"}], [
            {"marketplace_id": "FR", "error": "SP-API 429 for /orders/v0/orders: quota exceeded"}
        ]

    monkeypatch.setattr(AmazonSpApiClient, "marketplace_participations", fake_marketplace_participations)
    monkeypatch.setattr(AmazonSpApiClient, "orders", fake_orders)

    summary = importer.sync_amazon_fba(include_orders=True, include_inventory=False, include_finances=False, include_inbound=False)

    assert summary["orders"] == 1
    assert summary["status"] == "partial"
    assert {"scope": "orders", "marketplace_id": "FR", "error": "SP-API 429 for /orders/v0/orders: quota exceeded"} in summary["errors"]


def test_upsert_marketplaces_filters_to_participating_by_default(monkeypatch, tmp_path) -> None:
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    payload = {"payload": [
        {"marketplace": {"id": "DE", "name": "DE", "countryCode": "DE", "defaultCurrencyCode": "EUR", "domainName": "amazon.de"}, "participation": {"isParticipating": True}},
        {"marketplace": {"id": "FR", "name": "FR", "countryCode": "FR", "defaultCurrencyCode": "EUR", "domainName": "amazon.fr"}, "participation": {"isParticipating": False}},
        {"marketplace": {"id": "IT", "name": "IT", "countryCode": "IT", "defaultCurrencyCode": "EUR", "domainName": "amazon.it"}, "participation": {}},
    ]}

    with importer._connect() as connection:
        active = importer._upsert_marketplaces(connection, payload)
        connection.commit()
        all_ids = importer._upsert_marketplaces(connection, payload, active_only=False)
        connection.commit()
        stored = connection.execute("SELECT marketplace_id FROM amazon_marketplaces ORDER BY marketplace_id").fetchall()

    assert active == ["DE"]
    assert sorted(all_ids) == ["DE", "FR", "IT"]
    assert [row["marketplace_id"] for row in stored] == ["DE", "FR", "IT"]


def test_sync_amazon_fba_excludes_non_participating_marketplace_by_default(monkeypatch, tmp_path) -> None:
    import app.services.importers.amazon_sp_api as importer
    from app.services.importers.amazon_sp_api import AmazonSpApiClient

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    monkeypatch.setattr(importer, "load_amazon_sp_api_config", lambda: (importer.AmazonSpApiConfig("c", "s", "r"), []))

    def fake_marketplace_participations(self):
        return {"payload": [
            {"marketplace": {"id": "DE", "name": "DE", "countryCode": "DE", "defaultCurrencyCode": "EUR", "domainName": "amazon.de"}, "participation": {"isParticipating": True}},
            {"marketplace": {"id": "FR", "name": "FR", "countryCode": "FR", "defaultCurrencyCode": "EUR", "domainName": "amazon.fr"}, "participation": {"isParticipating": False}},
        ]}

    queried_marketplaces: list[list[str]] = []

    def fake_orders(self, marketplace_ids, created_after, *, updated_after=None):
        queried_marketplaces.append(list(marketplace_ids))
        return [], []

    monkeypatch.setattr(AmazonSpApiClient, "marketplace_participations", fake_marketplace_participations)
    monkeypatch.setattr(AmazonSpApiClient, "orders", fake_orders)

    importer.sync_amazon_fba(include_orders=True, include_inventory=False, include_finances=False, include_inbound=False)
    assert queried_marketplaces == [["DE"]]

    queried_marketplaces.clear()
    importer.sync_amazon_fba(include_orders=True, include_inventory=False, include_finances=False, include_inbound=False, include_all_marketplaces=True)
    assert queried_marketplaces == [["DE", "FR"]]


def test_amazon_marketplace_settings_default_to_auto(monkeypatch, tmp_path) -> None:
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()

    settings = importer.get_amazon_marketplace_settings()

    assert settings == {"marketplace_mode": "auto", "selected_marketplace_ids": []}


def test_amazon_marketplace_settings_round_trip(monkeypatch, tmp_path) -> None:
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()

    saved = importer.set_amazon_marketplace_settings(
        marketplace_mode="manual", selected_marketplace_ids=["A1PA6795UKMFR9"]
    )
    assert saved == {"marketplace_mode": "manual", "selected_marketplace_ids": ["A1PA6795UKMFR9"]}

    reloaded = importer.get_amazon_marketplace_settings()
    assert reloaded == saved


def test_amazon_marketplace_settings_rejects_invalid_mode(monkeypatch, tmp_path) -> None:
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()

    try:
        importer.set_amazon_marketplace_settings(marketplace_mode="bogus", selected_marketplace_ids=[])
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError for invalid marketplace_mode")


def test_sync_amazon_fba_uses_manual_marketplace_selection(monkeypatch, tmp_path) -> None:
    import app.services.importers.amazon_sp_api as importer
    from app.services.importers.amazon_sp_api import AmazonSpApiClient

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    monkeypatch.setattr(importer, "load_amazon_sp_api_config", lambda: (importer.AmazonSpApiConfig("c", "s", "r"), []))
    importer.init_amazon_fba_db()
    importer.set_amazon_marketplace_settings(marketplace_mode="manual", selected_marketplace_ids=["FR"])

    def fake_marketplace_participations(self):
        return {"payload": [
            {"marketplace": {"id": "DE", "name": "DE", "countryCode": "DE", "defaultCurrencyCode": "EUR", "domainName": "amazon.de"}, "participation": {"isParticipating": True}},
            {"marketplace": {"id": "FR", "name": "FR", "countryCode": "FR", "defaultCurrencyCode": "EUR", "domainName": "amazon.fr"}, "participation": {"isParticipating": False}},
        ]}

    queried: list[list[str]] = []

    def fake_orders(self, marketplace_ids, created_after, *, updated_after=None):
        queried.append(list(marketplace_ids))
        return [], []

    monkeypatch.setattr(AmazonSpApiClient, "marketplace_participations", fake_marketplace_participations)
    monkeypatch.setattr(AmazonSpApiClient, "orders", fake_orders)

    importer.sync_amazon_fba(include_orders=True, include_inventory=False, include_finances=False, include_inbound=False)

    assert queried == [["FR"]]


def test_sync_amazon_fba_include_all_marketplaces_bypasses_manual_selection(monkeypatch, tmp_path) -> None:
    import app.services.importers.amazon_sp_api as importer
    from app.services.importers.amazon_sp_api import AmazonSpApiClient

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    monkeypatch.setattr(importer, "load_amazon_sp_api_config", lambda: (importer.AmazonSpApiConfig("c", "s", "r"), []))
    importer.init_amazon_fba_db()
    importer.set_amazon_marketplace_settings(marketplace_mode="manual", selected_marketplace_ids=["FR"])

    def fake_marketplace_participations(self):
        return {"payload": [
            {"marketplace": {"id": "DE", "name": "DE", "countryCode": "DE", "defaultCurrencyCode": "EUR", "domainName": "amazon.de"}, "participation": {"isParticipating": True}},
            {"marketplace": {"id": "FR", "name": "FR", "countryCode": "FR", "defaultCurrencyCode": "EUR", "domainName": "amazon.fr"}, "participation": {"isParticipating": False}},
        ]}

    queried: list[list[str]] = []

    def fake_orders(self, marketplace_ids, created_after, *, updated_after=None):
        queried.append(list(marketplace_ids))
        return [], []

    monkeypatch.setattr(AmazonSpApiClient, "marketplace_participations", fake_marketplace_participations)
    monkeypatch.setattr(AmazonSpApiClient, "orders", fake_orders)

    importer.sync_amazon_fba(include_orders=True, include_inventory=False, include_finances=False, include_inbound=False, include_all_marketplaces=True)

    assert queried == [["DE", "FR"]]


def test_marketplace_settings_endpoints_round_trip(monkeypatch, tmp_path) -> None:
    from fastapi.testclient import TestClient

    import app.main as main_module
    import app.services.importers.amazon_sp_api as importer_module

    monkeypatch.setattr(importer_module, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    monkeypatch.setenv("APP_ADMIN_TOKEN", "test-token")

    importer_module.init_amazon_fba_db()
    with importer_module._connect() as connection:
        importer_module._upsert_marketplaces(
            connection,
            {"payload": [
                {"marketplace": {"id": "DE", "name": "Amazon.de", "countryCode": "DE", "defaultCurrencyCode": "EUR", "domainName": "amazon.de"}, "participation": {"isParticipating": True}},
            ]},
            active_only=False,
        )
        connection.commit()

    client = TestClient(main_module.app)

    response = client.get("/api/amazon/marketplace-settings")
    assert response.status_code == 200
    payload = response.json()
    assert payload["marketplace_mode"] == "auto"
    assert payload["marketplaces"] == [
        {"marketplace_id": "DE", "name": "Amazon.de", "country_code": "DE", "domain_name": "amazon.de", "is_participating": True}
    ]

    unauthorized = client.post("/api/amazon/marketplace-settings", json={"marketplace_mode": "manual", "selected_marketplace_ids": ["DE"]})
    assert unauthorized.status_code == 401

    saved = client.post(
        "/api/amazon/marketplace-settings",
        json={"marketplace_mode": "manual", "selected_marketplace_ids": ["DE"]},
        headers={"X-Admin-Token": "test-token"},
    )
    assert saved.status_code == 200
    assert saved.json()["marketplace_mode"] == "manual"

    rejected = client.post(
        "/api/amazon/marketplace-settings",
        json={"marketplace_mode": "manual", "selected_marketplace_ids": ["NOT-A-REAL-ID"]},
        headers={"X-Admin-Token": "test-token"},
    )
    assert rejected.status_code == 400


def test_list_amazon_sku_inventory_aggregates_sales_cogs_and_stock(monkeypatch, tmp_path) -> None:
    import app.services.amazon_fba as amazon_fba
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()

    batch = amazon_fba.create_procurement_batch(
        reference="sku-inv-1", name="SKU Inventory Test",
        lines=[{"seller_sku": "H10B", "title": "HIBREW H10B", "quantity": 5}],
    )
    lot = amazon_fba.create_inventory_lot(
        batch_line_id=batch["lines"][0]["id"], unit_cost_cents=10_000, received_at="2026-07-01T00:00:00Z",
    )

    with importer._connect() as connection:
        connection.execute(
            "INSERT INTO amazon_orders(amazon_order_id, seller_order_id, purchase_date, order_status, fulfillment_channel, currency, order_total_cents, raw_json, updated_at) "
            "VALUES ('ORDER-SKU-1', 'ORDER-SKU-1', '2026-07-15T00:00:00Z', 'Shipped', 'AFN', 'EUR', 30000, '{}', '2026-07-15T00:00:00Z')"
        )
        connection.execute(
            "INSERT INTO amazon_order_items(id, amazon_order_id, seller_sku, asin, title, quantity_ordered, quantity_shipped, currency, item_price_cents, item_tax_cents) "
            "VALUES ('ITEM-SKU-1', 'ORDER-SKU-1', 'H10B', 'B0H10B', 'HIBREW H10B', 2, 2, 'EUR', 30000, 4790)"
        )
        connection.execute(
            "INSERT INTO amazon_inventory_snapshots(id, captured_at, marketplace_id, seller_sku, fnsku, asin, product_name, fulfillable_quantity, inbound_working_quantity, inbound_shipped_quantity, reserved_quantity, unfulfillable_quantity, raw_json) "
            "VALUES ('SNAP-SKU-1', '2026-07-20T00:00:00Z', 'A1PA6795UKMFR9', 'H10B', 'FNSKU-H10B', 'B0H10B', 'HIBREW H10B', 3, 0, 0, 0, 0, '{}')"
        )
        connection.commit()

    amazon_fba.allocate_order_fifo("ORDER-SKU-1")

    items = amazon_fba.list_amazon_sku_inventory()
    assert len(items) == 1
    item = items[0]
    assert item["sku_key"] == "H10B"
    assert item["seller_sku"] == "H10B"
    assert item["title"] == "HIBREW H10B"
    assert item["quantity_sold"] == 2
    assert item["sales_cents"] == 30000
    assert item["tax_cents"] == 4790
    assert item["sales_net_cents"] == 25210
    assert item["fees_cents"] == 0
    assert item["cogs_cents"] == 20000
    assert item["margin_cents"] == 5210
    assert item["margin_percent"] == pytest.approx(20.7, abs=0.1)
    assert item["fulfillable_quantity"] == 3


def test_list_amazon_sku_inventory_profit_subtracts_allocated_amazon_fees(monkeypatch, tmp_path) -> None:
    """Profit ('Marge') must account for Amazon fees, not just revenue minus
    purchase cost -- otherwise it massively overstates real profitability
    (FBA fees + commission routinely run 20-30% of revenue)."""
    import app.services.amazon_fba as amazon_fba
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    with importer._connect() as connection:
        connection.execute(
            "INSERT INTO amazon_orders(amazon_order_id, seller_order_id, purchase_date, order_status, fulfillment_channel, currency, order_total_cents, raw_json, updated_at) "
            "VALUES ('ORDER-FEES-1', 'ORDER-FEES-1', '2026-07-15T00:00:00Z', 'Shipped', 'AFN', 'EUR', 10000, '{}', '2026-07-15T00:00:00Z')"
        )
        connection.execute(
            "INSERT INTO amazon_order_items(id, amazon_order_id, seller_sku, asin, title, quantity_ordered, quantity_shipped, currency, item_price_cents, item_tax_cents) "
            "VALUES ('ITEM-FEES-1', 'ORDER-FEES-1', 'FEESKU', 'B0FEES', 'Fee Test Product', 1, 1, 'EUR', 10000, 2000)"
        )
        connection.execute(
            "INSERT INTO amazon_financial_events(id, event_type, amazon_order_id, settlement_id, posted_date, financial_finality, currency, sales_cents, fees_cents, net_cents, raw_json) "
            "VALUES ('EVT-FEES-1', 'ModernTransaction:Shipment', 'ORDER-FEES-1', NULL, '2026-07-15T00:00:00Z', 'deferred', 'EUR', 10000, 1500, 8500, '{}')"
        )
        connection.commit()

    items = amazon_fba.list_amazon_sku_inventory()
    assert len(items) == 1
    item = items[0]
    assert item["sales_cents"] == 10000
    assert item["tax_cents"] == 2000
    assert item["sales_net_cents"] == 8000
    assert item["fees_cents"] == 1500
    assert item["cogs_cents"] == 0
    assert item["margin_cents"] == 6500
    assert item["margin_percent"] == pytest.approx(81.25, abs=0.1)


def test_list_amazon_sku_inventory_includes_unsold_stock_without_sales(monkeypatch, tmp_path) -> None:
    import app.services.amazon_fba as amazon_fba
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    with importer._connect() as connection:
        connection.execute(
            "INSERT INTO amazon_inventory_snapshots(id, captured_at, marketplace_id, seller_sku, fnsku, asin, product_name, fulfillable_quantity, inbound_working_quantity, inbound_shipped_quantity, reserved_quantity, unfulfillable_quantity, raw_json) "
            "VALUES ('SNAP-NEW-1', '2026-07-20T00:00:00Z', 'A1PA6795UKMFR9', 'NEWSKU', 'FNSKU-NEW', 'B0NEW', 'Brand New Product', 10, 5, 0, 0, 0, '{}')"
        )
        connection.commit()

    items = amazon_fba.list_amazon_sku_inventory()
    assert len(items) == 1
    item = items[0]
    assert item["sku_key"] == "NEWSKU"
    assert item["title"] == "Brand New Product"
    assert item["quantity_sold"] == 0
    assert item["sales_cents"] == 0
    assert item["margin_percent"] is None
    assert item["fulfillable_quantity"] == 10
    assert item["inbound_working_quantity"] == 5


def test_list_amazon_sku_inventory_excludes_fully_dormant_skus_by_default(monkeypatch, tmp_path) -> None:
    """A SKU with zero sales and zero current stock (e.g. only a stale
    order-item row from a long-canceled order for a discontinued product)
    carries no decision-relevant information and clutters the inventory
    view -- exclude it by default."""
    import app.services.amazon_fba as amazon_fba
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    with importer._connect() as connection:
        connection.execute(
            "INSERT INTO amazon_orders(amazon_order_id, seller_order_id, purchase_date, order_status, fulfillment_channel, currency, order_total_cents, raw_json, updated_at) "
            "VALUES ('ORDER-DORMANT-1', 'ORDER-DORMANT-1', '2026-06-01T00:00:00Z', 'Canceled', 'AFN', 'EUR', 0, '{}', '2026-06-01T00:00:00Z')"
        )
        connection.execute(
            "INSERT INTO amazon_order_items(id, amazon_order_id, seller_sku, asin, title, quantity_ordered, quantity_shipped, currency, item_price_cents, item_tax_cents) "
            "VALUES ('ITEM-DORMANT-1', 'ORDER-DORMANT-1', 'DORMANTSKU', 'B0DORMANT', 'Discontinued Product', 1, 0, 'EUR', 0, 0)"
        )
        connection.commit()

    default_items = amazon_fba.list_amazon_sku_inventory()
    assert default_items == []

    all_items = amazon_fba.list_amazon_sku_inventory(include_dormant=True)
    assert len(all_items) == 1
    assert all_items[0]["sku_key"] == "DORMANTSKU"


def test_list_amazon_sku_inventory_includes_image_url_from_order_items(monkeypatch, tmp_path) -> None:
    import app.services.amazon_fba as amazon_fba
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    with importer._connect() as connection:
        connection.execute(
            "INSERT INTO amazon_orders(amazon_order_id, seller_order_id, purchase_date, order_status, fulfillment_channel, currency, order_total_cents, raw_json, updated_at) "
            "VALUES ('ORDER-IMG-1', 'ORDER-IMG-1', '2026-07-01T00:00:00Z', 'Shipped', 'AFN', 'EUR', 1000, '{}', '2026-07-01T00:00:00Z')"
        )
        connection.execute(
            "INSERT INTO amazon_order_items(id, amazon_order_id, seller_sku, asin, title, quantity_ordered, quantity_shipped, currency, item_price_cents, item_tax_cents, image_url) "
            "VALUES ('ITEM-IMG-1', 'ORDER-IMG-1', 'IMGSKU', 'B0IMG', 'Product With Image', 1, 1, 'EUR', 1000, 0, 'https://example.test/image.jpg')"
        )
        connection.commit()

    items = amazon_fba.list_amazon_sku_inventory()
    assert len(items) == 1
    assert items[0]["image_url"] == "https://example.test/image.jpg"


def test_set_amazon_sku_hidden_persists_and_filters_default_listing(monkeypatch, tmp_path) -> None:
    import app.services.amazon_fba as amazon_fba
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    with importer._connect() as connection:
        connection.execute(
            "INSERT INTO amazon_inventory_snapshots(id, captured_at, marketplace_id, seller_sku, fnsku, asin, product_name, fulfillable_quantity, inbound_working_quantity, inbound_shipped_quantity, reserved_quantity, unfulfillable_quantity, raw_json) "
            "VALUES ('SNAP-HIDE-1', '2026-07-20T00:00:00Z', 'A1PA6795UKMFR9', 'HIDEMESKU', 'FNSKU-HIDE', 'B0HIDE', 'Hide Me Product', 5, 0, 0, 0, 0, '{}')"
        )
        connection.commit()

    assert len(amazon_fba.list_amazon_sku_inventory()) == 1

    amazon_fba.set_amazon_sku_hidden("HIDEMESKU", hidden=True)

    assert amazon_fba.list_amazon_sku_inventory() == []
    shown_with_hidden = amazon_fba.list_amazon_sku_inventory(include_hidden=True)
    assert len(shown_with_hidden) == 1
    assert shown_with_hidden[0]["hidden"] is True

    amazon_fba.set_amazon_sku_hidden("HIDEMESKU", hidden=False)
    assert len(amazon_fba.list_amazon_sku_inventory()) == 1


def test_amazon_sku_hidden_endpoint_round_trip(monkeypatch, tmp_path) -> None:
    from fastapi.testclient import TestClient

    import app.main as main_module
    import app.services.importers.amazon_sp_api as importer_module

    monkeypatch.setattr(importer_module, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    monkeypatch.setenv("APP_ADMIN_TOKEN", "test-token")
    importer_module.init_amazon_fba_db()
    with importer_module._connect() as connection:
        connection.execute(
            "INSERT INTO amazon_inventory_snapshots(id, captured_at, marketplace_id, seller_sku, fnsku, asin, product_name, fulfillable_quantity, inbound_working_quantity, inbound_shipped_quantity, reserved_quantity, unfulfillable_quantity, raw_json) "
            "VALUES ('SNAP-EP-1', '2026-07-20T00:00:00Z', 'A1PA6795UKMFR9', 'EPSKU', 'FNSKU-EP', 'B0EP', 'Endpoint Product', 5, 0, 0, 0, 0, '{}')"
        )
        connection.commit()

    client = TestClient(main_module.app)

    unauthorized = client.post("/api/amazon/inventory/skus/EPSKU/hidden", json={"hidden": True})
    assert unauthorized.status_code == 401

    hide = client.post(
        "/api/amazon/inventory/skus/EPSKU/hidden",
        json={"hidden": True},
        headers={"X-Admin-Token": "test-token"},
    )
    assert hide.status_code == 200

    listing = client.get("/api/amazon/inventory/skus")
    assert listing.json()["items"] == []

    listing_with_hidden = client.get("/api/amazon/inventory/skus?include_hidden=true")
    assert len(listing_with_hidden.json()["items"]) == 1
    assert listing_with_hidden.json()["items"][0]["hidden"] is True

    unhide = client.post(
        "/api/amazon/inventory/skus/EPSKU/hidden",
        json={"hidden": False},
        headers={"X-Admin-Token": "test-token"},
    )
    assert unhide.status_code == 200
    listing_again = client.get("/api/amazon/inventory/skus")
    assert len(listing_again.json()["items"]) == 1


def test_get_amazon_sku_detail_returns_none_for_unknown_sku(monkeypatch, tmp_path) -> None:
    import app.services.amazon_fba as amazon_fba
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()

    assert amazon_fba.get_amazon_sku_detail("DOES-NOT-EXIST") is None


def test_get_amazon_sku_detail_computes_fee_per_unit_and_associated_shipments(monkeypatch, tmp_path) -> None:
    import app.services.amazon_fba as amazon_fba
    import app.services.importers.amazon_sp_api as importer

    monkeypatch.setattr(importer, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer.init_amazon_fba_db()
    recent_purchase_date = (datetime.now(timezone.utc) - timedelta(days=5)).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    with importer._connect() as connection:
        connection.execute(
            "INSERT INTO amazon_orders(amazon_order_id, seller_order_id, purchase_date, order_status, fulfillment_channel, currency, order_total_cents, raw_json, updated_at) "
            "VALUES ('ORDER-DETAIL-1', 'ORDER-DETAIL-1', ?, 'Shipped', 'AFN', 'EUR', 20000, '{}', ?)",
            (recent_purchase_date, recent_purchase_date),
        )
        connection.execute(
            "INSERT INTO amazon_order_items(id, amazon_order_id, seller_sku, asin, title, quantity_ordered, quantity_shipped, currency, item_price_cents, item_tax_cents) "
            "VALUES ('ITEM-DETAIL-1', 'ORDER-DETAIL-1', 'DETAILSKU', 'B0DETAIL', 'Detail Product', 1, 1, 'EUR', 20000, 3190)"
        )
        connection.execute(
            "INSERT INTO amazon_financial_events(id, event_type, amazon_order_id, settlement_id, posted_date, financial_finality, currency, sales_cents, fees_cents, net_cents, raw_json) "
            "VALUES ('EVT-DETAIL-1', 'ModernTransaction:Shipment', 'ORDER-DETAIL-1', NULL, ?, 'deferred', 'EUR', 20000, 4000, 16000, '{}')",
            (recent_purchase_date,),
        )
        connection.execute(
            "INSERT INTO amazon_inbound_shipments(id, shipment_id, plan_id, shipment_name, status, destination_fulfillment_center_id, raw_json, updated_at) "
            "VALUES ('SHIP-DETAIL-1', 'SHIP-DETAIL-1', '', 'Batch 1', 'CLOSED', 'FC1', '{}', ?)",
            (recent_purchase_date,),
        )
        connection.execute(
            "INSERT INTO amazon_inbound_shipment_items(id, shipment_id, seller_sku, fnsku, asin, title, quantity_shipped, quantity_received, raw_json) "
            "VALUES ('SHIPITEM-DETAIL-1', 'SHIP-DETAIL-1', 'DETAILSKU', 'FNSKU-DETAIL', 'B0DETAIL', 'Detail Product', 10, 10, '{}')"
        )
        connection.commit()

    detail = amazon_fba.get_amazon_sku_detail("DETAILSKU")
    assert detail is not None
    assert detail["sku_key"] == "DETAILSKU"
    assert detail["fee_per_unit_cents"] == 4000
    assert detail["quantity_sold_last_30_days"] == 1
    assert detail["days_of_stock"] is not None
    assert len(detail["shipments"]) == 1
    assert detail["shipments"][0]["shipment_id"] == "SHIP-DETAIL-1"
    assert detail["shipments"][0]["quantity_received"] == 10


def test_amazon_sku_inventory_endpoints_round_trip(monkeypatch, tmp_path) -> None:
    from fastapi.testclient import TestClient

    import app.main as main_module
    import app.services.importers.amazon_sp_api as importer_module

    monkeypatch.setattr(importer_module, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    importer_module.init_amazon_fba_db()
    with importer_module._connect() as connection:
        connection.execute(
            "INSERT INTO amazon_inventory_snapshots(id, captured_at, marketplace_id, seller_sku, fnsku, asin, product_name, fulfillable_quantity, inbound_working_quantity, inbound_shipped_quantity, reserved_quantity, unfulfillable_quantity, raw_json) "
            "VALUES ('SNAP-API-1', '2026-07-20T00:00:00Z', 'A1PA6795UKMFR9', 'APISKU', 'FNSKU-API', 'B0API', 'API Test Product', 4, 0, 0, 0, 0, '{}')"
        )
        connection.commit()

    client = TestClient(main_module.app)

    listing = client.get("/api/amazon/inventory/skus")
    assert listing.status_code == 200
    items = listing.json()["items"]
    assert len(items) == 1
    assert items[0]["sku_key"] == "APISKU"

    detail = client.get("/api/amazon/inventory/skus/APISKU")
    assert detail.status_code == 200
    assert detail.json()["sku_key"] == "APISKU"

    missing = client.get("/api/amazon/inventory/skus/NOT-A-SKU")
    assert missing.status_code == 404
