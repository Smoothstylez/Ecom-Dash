from __future__ import annotations

import sqlite3
import json

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
        title="Product", quantity=11, net_cents=11000, vat_cents=1100,
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
        title="Product", quantity=11, net_cents=10000, vat_cents=1000,
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
        title="Product", quantity=11, net_cents=11000, vat_cents=1100,
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
        title="Product", quantity=1, net_cents=1000, vat_cents=100,
    )

    with pytest.raises(ValueError, match="received"):
        amazon_fba.confirm_inbound_product_costs("FBA-DELIVERED-1")


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
        title="Product", quantity=3, net_cents=1001, vat_cents=100,
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
        title="Product", quantity=1, net_cents=1000, vat_cents=100,
    )
    amazon_fba.confirm_inbound_product_costs("FBA-LOCK-1")

    with pytest.raises(ValueError, match="confirmed"):
        amazon_fba.add_inbound_invoice_line(
            invoice_id=invoice["id"], seller_sku="SKU-1", fnsku="FNSKU-1", asin="ASIN-1",
            title="Changed", quantity=1, net_cents=1200, vat_cents=120,
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
    orders = client.orders(["DE", "FR"], "2026-01-01T00:00:00Z")

    assert requested_marketplaces == ["DE", "FR"]
    assert [order["AmazonOrderId"] for order in orders] == ["ORDER-DE", "ORDER-FR"]


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
