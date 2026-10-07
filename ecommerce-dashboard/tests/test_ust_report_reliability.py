"""Behavioral regressions for the October VAT-report audit; all DBs isolated."""
import sqlite3
from datetime import datetime, timezone

import pytest

from app import config, db
from app.services import amazon_tax_import as amazon, ust_report as report, ust_documents
from app.services.importers import amazon_sp_api
from test_ust_report import KAUFLAND_SCHEMA


@pytest.fixture
def isolated(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "COMBINED_DB_PATH", tmp_path / "combined.sqlite3")
    monkeypatch.setattr(config, "BOOKKEEPING_DB_PATH", tmp_path / "bookkeeping.sqlite3")
    monkeypatch.setattr(config, "AMAZON_FBA_DB_PATH", tmp_path / "amazon.sqlite3")
    monkeypatch.setattr(amazon_sp_api, "AMAZON_FBA_DB_PATH", config.AMAZON_FBA_DB_PATH)
    monkeypatch.setattr(report, "KAUFLAND_DB_PATH", tmp_path / "kaufland.sqlite3")
    monkeypatch.setattr(config, "KAUFLAND_DB_PATH", report.KAUFLAND_DB_PATH)
    db.init_combined_db()
    amazon_sp_api.init_amazon_fba_db()
    with db.connect_combined_db() as c:
        c.execute("INSERT INTO seller_profiles(id,legal_name,tax_mode,vat_effective_from,created_at,updated_at) VALUES ('default','Audit','regular','2026-07-02T16:27:11Z','t','t')")
    c = sqlite3.connect(report.KAUFLAND_DB_PATH)
    c.executescript(KAUFLAND_SCHEMA)
    yield c
    c.close()


def unit(c, date="2026-07-05T12:00:00Z", rate=19):
    c.execute("INSERT INTO order_units(id_order_unit,id_order,ts_created_iso,status,price,shipping_rate,vat,is_marketplace_deemed_supplier,shipping_country,revenue_gross) VALUES ('u1','O1',?,'received','11900','0',?,0,'DE','10000')", (date, rate))
    c.commit()


def tax_row(**overrides):
    r = {"Transaction ID": "T1", "Order ID": "O1", "Shipment ID": "S1", "SKU": "SKU1",
         "Transaction Type": "SHIPMENT", "Ship From Country": "DE", "Ship To Country": "DE",
         "Tax Rate": "0.19", "Tax Calculation Reason Code": "Taxable",
         "Order Date": "2026-07-05", "Shipment Date": "2026-07-07", "Tax Calculation Date": "2026-07-05",
         "OUR_PRICE Tax Inclusive Selling Price": "119.00", "OUR_PRICE Tax Exclusive Selling Price": "100.00",
         "OUR_PRICE Tax Amount": "19.00", "Currency": "EUR"}
    return {**r, **overrides}


def import_rows(*rows):
    return amazon.import_sc_vat_tax_rows(amazon.link_and_inherit(rows, eu_tax_regime="unconfirmed"))


@pytest.mark.parametrize("rate", [0, 19])
def test_kaufland_before_exact_cutoff_has_no_output_vat(isolated, rate):
    unit(isolated, "2026-07-02T16:27:10Z", rate)
    r = report.load_kaufland_vat_rows("2026-07")[0]
    assert (r["tax_class"], r["gross_cents"], r["net_cents"], r["output_vat_cents"]) == ("pre_vat", 11900, 11900, 0)


def test_threshold_order_itself_is_taxable(isolated):
    unit(isolated, "2026-07-02T16:27:11Z")
    assert report.load_kaufland_vat_rows("2026-07")[0]["output_vat_cents"] == 1900
    assert report.get_vat_effective_from() == datetime(2026, 7, 2, 16, 27, 11, tzinfo=timezone.utc)


def test_refund_moves_tax_to_its_own_booking_month(isolated):
    unit(isolated)
    isolated.execute("INSERT INTO order_unit_refunds(id_order_unit,position,amount,raw_json) VALUES ('u1',0,'1190','{\"ts_created_iso\":\"2026-08-05T12:00:00Z\"}')")
    isolated.commit()
    assert report.build_ust_report("2026-07")["totals"]["output_vat_cents"] == 1900
    assert report.build_ust_report("2026-08")["totals"]["output_vat_cents"] == -190


def test_undated_refund_blocks_instead_of_revising_sale(isolated):
    unit(isolated)
    isolated.execute("INSERT INTO order_unit_refunds(id_order_unit,position,amount) VALUES ('u1',0,'1190')")
    isolated.commit()
    r = report.build_ust_report("2026-07")
    assert r["totals"]["output_vat_cents"] == 1900
    assert "KAUFLAND_REFUND_DATE_MISSING" in {b["code"] for b in r["blockers"]}


def test_amazon_pre_cutoff_and_later_refund_remain_tax_free(isolated):
    sale = tax_row(**{"Order Date": "2026-06-15", "Shipment Date": "2026-06-17"})
    refund = tax_row(**{"Transaction ID": "R1", "Transaction Type": "RETURN", "Shipment Date": "2026-08-05",
                        "OUR_PRICE Tax Inclusive Selling Price": "-119.00", "OUR_PRICE Tax Exclusive Selling Price": "-100.00", "OUR_PRICE Tax Amount": "-19.00"})
    import_rows(sale, refund)
    assert report.build_ust_report("2026-06")["totals"]["output_vat_cents"] == 0
    assert report.build_ust_report("2026-08")["totals"]["output_vat_cents"] == 0


def test_amazon_date_only_cutoff_day_is_not_silently_classified(isolated):
    import_rows(tax_row(**{"Order Date": "2026-07-02", "Shipment Date": "2026-07-02"}))
    assert "AMAZON_VAT_START_AMBIGUOUS" in {b["code"] for b in report.build_ust_report("2026-07")["blockers"]}


def test_finance_only_missing_tax_rows_warn_without_blocking_filing(isolated):
    with sqlite3.connect(config.AMAZON_FBA_DB_PATH) as c:
        c.execute("INSERT INTO amazon_orders(amazon_order_id,purchase_date,order_status,updated_at) VALUES ('UNREPORTED','2026-07-05T12:00:00Z','Shipped','t')")
    r = report.build_ust_report("2026-07")
    assert r["status"] == "ready"
    assert "AMAZON_TAX_DATA_INCOMPLETE" in {b["code"] for b in r["warnings"]}
    report.file_report("2026-07")


def test_shipping_in_next_month_does_not_create_false_purchase_month_gap(isolated):
    with sqlite3.connect(config.AMAZON_FBA_DB_PATH) as c:
        c.execute("INSERT INTO amazon_orders(amazon_order_id,purchase_date,order_status,updated_at) VALUES ('O1','2026-07-30T12:00:00Z','Shipped','t')")
    import_rows(tax_row(**{"Order Date": "2026-07-30", "Shipment Date": "2026-08-02"}))
    assert "AMAZON_TAX_DATA_INCOMPLETE" not in {b["code"] for b in report.build_ust_report("2026-07")["blockers"]}


def test_partial_tax_shipment_and_missing_refund_are_detected(isolated):
    import_rows(tax_row())
    with sqlite3.connect(config.AMAZON_FBA_DB_PATH) as c:
        c.execute("INSERT INTO amazon_orders(amazon_order_id,purchase_date,order_status,updated_at) VALUES ('O1','2026-07-05T12:00:00Z','Shipped','t')")
        for identifier, kind, date, amount in [("s1", "ShipmentEventList", "2026-07-07", 23800), ("r1", "RefundEventList", "2026-08-05", -11900)]:
            c.execute("INSERT INTO amazon_financial_events(id,event_type,amazon_order_id,posted_date,sales_cents,raw_json) VALUES (?,?,?,?,?,'{}')", (identifier, kind, "O1", date, amount))
    july = report.build_ust_report("2026-07")
    august = report.build_ust_report("2026-08")
    assert "AMAZON_TAX_DATA_INCOMPLETE" in {b["code"] for b in july["warnings"]}
    assert "AMAZON_TAX_DATA_INCOMPLETE" in {b["code"] for b in august["warnings"]}
    assert july["sections"]["amazon_reconciliation"]["amount_mismatches"][0]["source_gross_cents"] == 23800


def test_broken_amazon_source_blocks_without_crashing_report(isolated):
    import_rows(tax_row())
    with sqlite3.connect(config.AMAZON_FBA_DB_PATH) as c:
        c.execute("DROP TABLE amazon_orders")
    assert "AMAZON_SOURCE_UNAVAILABLE" in {b["code"] for b in report.build_ust_report("2026-07")["warnings"]}


@pytest.mark.parametrize("operation", ["request", "import"])
def test_amazon_report_permission_failure_is_an_explicit_gateway_error(monkeypatch, operation):
    from fastapi import HTTPException
    from app.routers import amazon as router

    def denied(*args, **kwargs):
        raise amazon_sp_api.AmazonSpApiError("SP-API 403: Unauthorized")

    target = "request_sc_vat_tax_report" if operation == "request" else "import_report_document"
    monkeypatch.setattr(amazon, target, denied)
    with pytest.raises(HTTPException) as exc:
        if operation == "request":
            router.api_request_amazon_tax_report({"month": "2026-07"})
        else:
            router.api_import_amazon_tax_report("report-id")
    assert exc.value.status_code == 502
    assert "403" in exc.value.detail


def test_metadata_change_and_old_hash_id_do_not_duplicate_sale(isolated):
    r = tax_row(**{"Invoice Url": "https://example.com/a"})
    import_rows(r)
    with db.connect_combined_db() as c:
        c.execute("UPDATE amazon_tax_rows SET id='legacy-content-hash'")
    import_rows({**r, "Invoice Url": "https://example.com/b"})
    with db.connect_combined_db() as c:
        rows = c.execute("SELECT invoice_url FROM amazon_tax_rows").fetchall()
    assert len(rows) == 1
    assert rows[0]["invoice_url"] == "https://example.com/b"
    amazon.reclassify_all_rows(eu_tax_regime="unconfirmed")
    assert report.build_ust_report("2026-07")["totals"]["output_vat_cents"] == 1900


@pytest.mark.parametrize("vat_number", ["DE123456789", "GB123456789", "123456789"])
def test_b2b_exemption_requires_foreign_eu_vat_prefix(vat_number):
    r = tax_row(**{"Ship To Country": "AT", "Tax Rate": "0", "Buyer Tax Registration": vat_number,
                  "Buyer Tax Registration Type": "VAT", "OUR_PRICE Tax Amount": "0.00", "OUR_PRICE Tax Exclusive Selling Price": "119.00"})
    assert amazon.classify_amazon_tax_row(r, eu_tax_regime="unconfirmed")["tax_class"] == "unresolved"


def bookkeeping():
    c = sqlite3.connect(config.BOOKKEEPING_DB_PATH)
    c.executescript("""
        CREATE TABLE transactions(id TEXT, direction TEXT, is_vat_deductible INTEGER, date TEXT,
            vat_amount INTEGER, provider TEXT, type TEXT, reference TEXT, source_key TEXT, document_id TEXT);
        CREATE TABLE monthly_invoices(id TEXT, provider TEXT, invoice_number TEXT, status TEXT,
            period_from TEXT, period_to TEXT, vat_amount_cents INTEGER, invoice_date TEXT,
            received_date TEXT, document_id TEXT);
    """)
    return c


def test_booked_invoice_deducted_once_when_available_not_each_service_month(isolated):
    with bookkeeping() as c:
        c.execute("INSERT INTO monthly_invoices VALUES ('I1','kaufland','INV1','approved','2026-06-01','2026-07-31',1900,'2026-08-01','2026-08-01','D1')")
    assert (report.sum_booked_input_vat("2026-06"), report.sum_booked_input_vat("2026-07"), report.sum_booked_input_vat("2026-08")) == (0, 0, 1900)
    r = report.build_ust_report("2026-08")
    assert r["sections"]["input_vat"]["kaufland_fees_cents"] == 1900
    assert r["totals"]["input_vat_cents"] == 1900


def test_booked_transaction_visible_in_detail_bucket(isolated):
    with bookkeeping() as c:
        c.execute("INSERT INTO transactions VALUES ('t1','OUT',1,'2026-08-05',1900,'other','PURCHASE','INV1','manual:t1','D1')")
    r = report.build_ust_report("2026-08")
    assert r["sections"]["input_vat"]["purchases_cents"] == 1900
    assert r["totals"]["input_vat_cents"] == 1900


def test_same_invoice_in_two_ledgers_and_booking_is_not_counted_twice(isolated):
    ust_documents.save_input_vat_invoice({"provider": "kaufland", "doc_type": "fee", "invoice_number": "INV1",
        "invoice_date": "2026-08-01", "period_to": "2026-07-31", "gross_cents": 11900,
        "net_cents": 10000, "vat_cents": 1900, "deductible_vat_cents": 1900, "input_vat_status": "confirmed"})
    with bookkeeping() as c:
        c.execute("INSERT INTO monthly_invoices VALUES ('I1','kaufland','INV1','approved','2026-07-01','2026-07-31',1900,'2026-08-01','2026-08-01','D1')")
        c.execute("INSERT INTO transactions VALUES ('t1','OUT',1,'2026-08-01',1900,'kaufland','FEE','INV1','platform-invoice:I1:basic','D1')")
    r = report.build_ust_report("2026-08")
    assert r["sections"]["input_vat"]["kaufland_fees_cents"] == 1900
    assert r["totals"]["input_vat_cents"] == 1900


def test_synthetic_finance_order_date_does_not_override_actual_pre_cutoff_date(isolated):
    with sqlite3.connect(config.AMAZON_FBA_DB_PATH) as c:
        c.execute("INSERT INTO amazon_orders(amazon_order_id,purchase_date,order_status,is_synthetic,updated_at) VALUES ('O1','2026-07-10T12:00:00Z','Shipped',1,'t')")
    import_rows(tax_row(**{"Order Date": "2026-06-15"}))
    assert report.build_ust_report("2026-07")["totals"]["output_vat_cents"] == 0


def test_amazon_undated_refund_never_uses_original_order_date(isolated):
    import_rows(tax_row(), tax_row(**{"Transaction ID": "R1", "Transaction Type": "RETURN",
        "Shipment Date": "", "Tax Calculation Date": "", "Order Date": "2026-07-05",
        "OUR_PRICE Tax Inclusive Selling Price": "-119.00", "OUR_PRICE Tax Exclusive Selling Price": "-100.00", "OUR_PRICE Tax Amount": "-19.00"}))
    r = report.build_ust_report("2026-07")
    assert r["totals"]["output_vat_cents"] == 1900
    assert "AMAZON_TRANSACTION_DATE_MISSING" in {b["code"] for b in r["blockers"]}


def test_ambiguous_original_shipments_block_refund(isolated):
    original = tax_row(**{"Order Date": "2026-06-15"})
    second = {**original, "Transaction ID": "T2"}
    correction = tax_row(**{"Transaction ID": "R1", "Transaction Type": "RETURN", "Shipment Date": "2026-08-05",
        "OUR_PRICE Tax Inclusive Selling Price": "-119.00", "OUR_PRICE Tax Exclusive Selling Price": "-100.00", "OUR_PRICE Tax Amount": "-19.00"})
    import_rows(original, second, correction)
    assert "UNRESOLVED_RETURN_LINK" in {b["code"] for b in report.build_ust_report("2026-08")["blockers"]}


def test_missing_july_shipment_blocks_july_even_if_purchase_june_and_release_august(isolated):
    with sqlite3.connect(config.AMAZON_FBA_DB_PATH) as c:
        c.execute("INSERT INTO amazon_orders(amazon_order_id,purchase_date,order_status,updated_at) VALUES ('O1','2026-06-30T12:00:00Z','Shipped','t')")
        for ident, date, state in [("d", "2026-07-03", "DEFERRED"), ("r", "2026-08-03", "RELEASED")]:
            c.execute("INSERT INTO amazon_financial_events(id,event_type,amazon_order_id,posted_date,sales_cents,lifecycle_id,raw_json) VALUES (?,'ModernTransaction:Shipment','O1',?,11900,'L1',?)", (ident, date, '{"transactionStatus":"' + state + '"}'))
    assert "AMAZON_TAX_DATA_INCOMPLETE" in {b["code"] for b in report.build_ust_report("2026-07")["warnings"]}


def test_settlement_sale_promotion_nets_sales_without_creating_refund_gap(isolated):
    import_rows(tax_row(**{"OUR_PRICE Tax Inclusive Selling Price": "109.00", "OUR_PRICE Tax Exclusive Selling Price": "91.60", "OUR_PRICE Tax Amount": "17.40"}))
    with sqlite3.connect(config.AMAZON_FBA_DB_PATH) as c:
        for ident, amount in [("principal", 11900), ("promo", -1000)]:
            c.execute("INSERT INTO amazon_financial_events(id,event_type,amazon_order_id,posted_date,sales_cents,raw_json) VALUES (?,'SettlementReportLine','O1','2026-07-07',?,'{\"transaction-type\":\"order\"}')", (ident, amount))
    assert "AMAZON_TAX_DATA_INCOMPLETE" not in {b["code"] for b in report.build_ust_report("2026-07")["blockers"]}


def test_rejected_legacy_and_approved_booked_invoice_create_explicit_conflict(isolated):
    ust_documents.save_input_vat_invoice({"provider": "kaufland", "doc_type": "fee", "invoice_number": "INV1",
        "invoice_date": "2026-08-01", "period_to": "2026-07-31", "gross_cents": 11900,
        "net_cents": 10000, "vat_cents": 1900, "deductible_vat_cents": 1900, "input_vat_status": "rejected"})
    with bookkeeping() as c:
        c.execute("INSERT INTO monthly_invoices VALUES ('I1','kaufland','INV1','approved','2026-07-01','2026-07-31',1900,'2026-08-01','2026-08-01','D1')")
    assert "INPUT_VAT_INVOICE_CONFLICT" in {b["code"] for b in report.build_ust_report("2026-08")["blockers"]}


def test_non_eur_booked_invoice_uses_proven_euro_vat(isolated):
    with bookkeeping() as c:
        c.execute("ALTER TABLE monthly_invoices ADD COLUMN currency TEXT DEFAULT 'EUR'")
        c.execute("ALTER TABLE monthly_invoices ADD COLUMN vat_cents_eur INTEGER")
        c.execute("INSERT INTO monthly_invoices(id,provider,invoice_number,status,period_from,period_to,vat_amount_cents,invoice_date,currency,vat_cents_eur) VALUES ('I1','amazon','GBP1','approved','2026-07-01','2026-07-31',475,'2026-08-01','GBP',551)")
    assert report.build_ust_report("2026-08")["totals"]["input_vat_cents"] == 551


def test_non_eur_transaction_without_euro_conversion_blocks(isolated):
    with bookkeeping() as c:
        c.execute("ALTER TABLE transactions ADD COLUMN currency TEXT DEFAULT 'EUR'")
        c.execute("INSERT INTO transactions(id,direction,is_vat_deductible,date,vat_amount,currency) VALUES ('t1','OUT',1,'2026-08-05',475,'GBP')")
    assert "INPUT_VAT_SOURCE_UNAVAILABLE" in {b["code"] for b in report.build_ust_report("2026-08")["blockers"]}


def test_amazon_september_spelling_keeps_shipment_and_return_in_correct_month(isolated):
    sale = tax_row(**{"Order Date": "31-Aug-2026 UTC", "Shipment Date": "01-Sept-2026 UTC", "Tax Calculation Date": "31-Aug-2026 UTC"})
    correction = {**sale, "Transaction ID": "R1", "Transaction Type": "RETURN", "Shipment Date": "20-Sept-2026 UTC",
        "OUR_PRICE Tax Inclusive Selling Price": "-119.00", "OUR_PRICE Tax Exclusive Selling Price": "-100.00", "OUR_PRICE Tax Amount": "-19.00"}
    import_rows(sale, correction)
    assert report.load_amazon_tax_rows("2026-08") == []
    rows = report.load_amazon_tax_rows("2026-09")
    assert [(r["transaction_type"], r["booking_date"]) for r in rows] == [("SHIPMENT", "2026-09-01"), ("RETURN", "2026-09-20")]


def test_modern_shipping_promo_rebate_is_removed_from_customer_gross_evidence(isolated):
    import json
    import_rows(tax_row(**{"OUR_PRICE Tax Inclusive Selling Price": "29.90", "OUR_PRICE Tax Exclusive Selling Price": "25.13", "OUR_PRICE Tax Amount": "4.77"}))
    raw = {"transactionStatus": "RELEASED", "breakdowns": [{"breakdownType": "Expenses", "breakdowns": [
        {"breakdownType": "PromoRebates", "breakdownAmount": {"currencyAmount": -3.35, "currencyCode": "EUR"}}]}]}
    with sqlite3.connect(config.AMAZON_FBA_DB_PATH) as c:
        c.execute("INSERT INTO amazon_financial_events(id,event_type,amazon_order_id,posted_date,sales_cents,raw_json) VALUES ('S1','ModernTransaction:Shipment','O1','2026-07-07',3325,?)", (json.dumps(raw),))
    assert "AMAZON_TAX_DATA_INCOMPLETE" not in {b["code"] for b in report.build_ust_report("2026-07")["blockers"]}


def test_avtr_fills_pre_vcs_domestic_sale_without_duplicating_vcs_row(isolated):
    import csv
    import io
    import_rows(tax_row(**{"Order ID": "EXISTING", "Shipment Date": "2026-07-22"}))
    rows = [{"TRANSACTION_TYPE": "SALE", "TRANSACTION_EVENT_ID": oid,
             "ACTIVITY_TRANSACTION_ID": activity, "SELLER_SKU": "SKU1", "SALES_CHANNEL": "AFN",
             "TRANSACTION_DEPART_DATE": "22-07-2026", "SALE_DEPART_COUNTRY": "DE", "SALE_ARRIVAL_COUNTRY": "DE",
             "TRANSACTION_CURRENCY_CODE": "EUR", "TOTAL_ACTIVITY_VALUE_AMT_VAT_INCL": "119.00",
             "TOTAL_ACTIVITY_VALUE_AMT_VAT_EXCL": "", "TOTAL_ACTIVITY_VALUE_VAT_AMT": "",
             "PRODUCT_TAX_CODE": "A_GEN_STANDARD", "TAX_COLLECTION_RESPONSIBILITY": "SELLER"}
            for oid, activity in [("MISSING", "AV1"), ("EXISTING", "AV2")]]
    with sqlite3.connect(config.AMAZON_FBA_DB_PATH) as c:
        for oid in ["MISSING", "EXISTING"]:
            c.execute("INSERT INTO amazon_orders(amazon_order_id,purchase_date,order_status,updated_at) VALUES (?,'2026-07-20T12:00:00Z','Shipped','t')", (oid,))
    text = io.StringIO()
    writer = csv.DictWriter(text, fieldnames=list(rows[0]))
    writer.writeheader()
    writer.writerows(rows)
    normalized = amazon.parse_sc_vat_tax_report(text.getvalue())
    import_rows(*normalized)
    import_rows(*normalized)
    report_rows = report.load_amazon_tax_rows("2026-07")
    assert len(report_rows) == 2
    assert sum(r["output_vat_cents"] for r in report_rows) == 3800
    assert {r["net_source"] for r in report_rows} == {"amazon_components", "computed_home_rate"}


def test_deferred_october_shipment_does_not_block_september_purchase_month(isolated):
    with sqlite3.connect(config.AMAZON_FBA_DB_PATH) as c:
        c.execute("INSERT INTO amazon_orders(amazon_order_id,purchase_date,order_status,updated_at) VALUES ('O1','2026-09-30T12:00:00Z','Shipped','t')")
        c.execute("INSERT INTO amazon_financial_events(id,event_type,amazon_order_id,posted_date,sales_cents,raw_json) VALUES ('d1','ModernTransaction:Shipment','O1','2026-10-01T12:00:00Z',11900,'{\"transactionStatus\":\"DEFERRED\"}')")
    assert "AMAZON_TAX_DATA_INCOMPLETE" not in {b["code"] for b in report.build_ust_report("2026-09")["blockers"]}
    assert "AMAZON_TAX_DATA_INCOMPLETE" in {b["code"] for b in report.build_ust_report("2026-10")["warnings"]}


def test_previous_service_month_fee_invoice_does_not_cover_current_fees(isolated):
    unit(isolated, "2026-08-05T12:00:00Z")
    ust_documents.save_input_vat_invoice({"provider": "kaufland", "doc_type": "fee", "invoice_number": "JULY-FEES",
        "invoice_date": "2026-08-01", "period_to": "2026-07-31", "gross_cents": 11900,
        "net_cents": 10000, "vat_cents": 1900, "deductible_vat_cents": 1900, "input_vat_status": "confirmed"})
    assert any(w["code"] == "MISSING_FEE_INVOICE" and w["provider"] == "kaufland" for w in report.build_ust_report("2026-08")["warnings"])


def test_amazon_fees_without_their_service_month_invoice_are_disclosed(isolated):
    import_rows(tax_row())
    with sqlite3.connect(config.AMAZON_FBA_DB_PATH) as c:
        c.execute("INSERT INTO amazon_financial_events(id,event_type,amazon_order_id,posted_date,sales_cents,fees_cents,raw_json) VALUES ('S1','ShipmentEventList','O1','2026-07-07',11900,2250,'{}')")
    assert any(w["code"] == "FINANCE_RECONCILIATION_INCOMPLETE" and w["provider"] == "amazon" for w in report.build_ust_report("2026-07")["warnings"])


def avtr_sale(**overrides):
    return tax_row(**{"Transaction ID": "AVTR:A1", "Shipment ID": "A1", "Tax Rate": "0",
        "Tax Collection Responsibility": "SELLER", "OUR_PRICE Tax Exclusive Selling Price": "",
        "OUR_PRICE Tax Amount": "", "_source_report_type": "VAT_TRANSACTION",
        "_avtr_raw": {"PRODUCT_TAX_CODE": "A_GEN_STANDARD", "TOTAL_ACTIVITY_VALUE_VAT_AMT": ""}, **overrides})


@pytest.mark.parametrize("primary_sale", [False, True])
def test_refund_inherits_computed_domestic_avtr_vat(isolated, primary_sale):
    sale = tax_row(**{"Shipment ID": "A1"}) if primary_sale else avtr_sale()
    refund = avtr_sale(**{"Transaction ID": "AVTR:R1", "Transaction Type": "RETURN", "Shipment Date": "2026-08-05",
        "OUR_PRICE Tax Inclusive Selling Price": "-119.00"})
    import_rows(sale, refund)
    assert report.build_ust_report("2026-07")["totals"]["output_vat_cents"] == 1900
    assert report.build_ust_report("2026-08")["totals"]["output_vat_cents"] == -1900


def test_avtr_rate_without_tax_amount_still_computes_vat(isolated):
    import_rows(avtr_sale(**{"Tax Rate": "0.19"}))
    r = report.load_amazon_tax_rows("2026-07")[0]
    assert (r["gross_cents"], r["net_cents"], r["output_vat_cents"]) == (11900, 10000, 1900)


def test_ambiguous_cross_report_matches_never_delete_multiple_shipments(isolated):
    import_rows(avtr_sale(), avtr_sale(**{"Transaction ID": "AVTR:A2", "Shipment ID": "A2"}))
    import_rows(tax_row())
    with db.connect_combined_db() as c:
        assert c.execute("SELECT COUNT(*) FROM amazon_tax_rows WHERE transaction_id LIKE 'AVTR:%'").fetchone()[0] == 2
    assert "AMAZON_UNRESOLVED" in {b["code"] for b in report.build_ust_report("2026-07")["blockers"]}


def test_unique_vcs_counterpart_replaces_avtr_even_after_amount_correction(isolated):
    import_rows(avtr_sale())
    import_rows(tax_row(**{"OUR_PRICE Tax Inclusive Selling Price": "118.00",
        "OUR_PRICE Tax Exclusive Selling Price": "99.16", "OUR_PRICE Tax Amount": "18.84"}))
    rows = report.load_amazon_tax_rows("2026-07")
    assert len(rows) == 1
    assert (rows[0]["gross_cents"], rows[0]["output_vat_cents"]) == (11800, 1884)


def test_partial_fee_invoice_does_not_hide_missing_remaining_fees(isolated):
    import_rows(tax_row())
    with sqlite3.connect(config.AMAZON_FBA_DB_PATH) as c:
        c.execute("INSERT INTO amazon_financial_events(id,event_type,amazon_order_id,posted_date,sales_cents,fees_cents,raw_json) VALUES ('S1','ShipmentEventList','O1','2026-07-07',11900,2250,'{}')")
    with bookkeeping() as c:
        c.execute("ALTER TABLE monthly_invoices ADD COLUMN invoice_amount_cents INTEGER")
        c.execute("INSERT INTO monthly_invoices(id,provider,invoice_number,status,period_from,period_to,vat_amount_cents,invoice_date,invoice_amount_cents) VALUES ('I1','amazon','PARTIAL','approved','2026-07-01','2026-07-31',190,'2026-07-31',1190)")
    assert any(w["code"] in {"FINANCE_RECONCILIATION_INCOMPLETE", "FEE_RECONCILIATION_DIFFERENCE"} and w["provider"] == "amazon" for w in report.build_ust_report("2026-07")["warnings"])


def test_customer_shipping_promotions_are_not_supplier_fee_invoice_costs(isolated):
    import json
    from app.services.ust_reconciliation import amazon_fee_cents

    raw = {"transactionStatus": "RELEASED", "breakdowns": [{"breakdownType": "Expenses", "breakdowns": [
        {"breakdownType": "PromoRebates", "breakdownAmount": {"currencyAmount": -3.35, "currencyCode": "EUR"}}]}]}
    with sqlite3.connect(config.AMAZON_FBA_DB_PATH) as c:
        c.execute("INSERT INTO amazon_financial_events(id,event_type,amazon_order_id,posted_date,sales_cents,fees_cents,raw_json) VALUES ('S1','ModernTransaction:Shipment','O1','2026-07-07',3325,1150,?)", (json.dumps(raw),))
    assert amazon_fee_cents("2026-07") == 815


def test_consumed_pre_vat_platform_services_have_no_deductible_vat(isolated):
    from app.services.ust_input_vat import FeeTaxContext

    row = {"provider": "kaufland", "doc_type": "fee", "vat_cents": 1900,
           "deductible_vat_cents": 1900, "period_from": "2026-06-01", "period_to": "2026-06-30",
           "invoice_date": "2026-07-01", "currency": "EUR"}
    result = FeeTaxContext(report.get_vat_effective_from()).assess(row)
    assert result["effective_deductible_vat_cents"] == 0
    assert result["eligibility_adjustment_cents"] == -1900
    assert row["deductible_vat_cents"] == 1900


def test_fee_credit_inherits_original_pre_vat_non_deductibility(isolated):
    from app.services.ust_input_vat import FeeTaxContext

    unit(isolated, "2026-06-15T12:00:00Z")
    isolated.execute("UPDATE order_units SET id_order_unit='old-unit',id_order='OLD'")
    unit(isolated, "2026-07-05T12:00:00Z")
    isolated.commit()
    row = {"provider": "kaufland", "doc_type": "fee", "vat_cents": 1900,
           "deductible_vat_cents": 1900, "period_from": "2026-07-01", "period_to": "2026-07-31",
           "invoice_date": "2026-08-01", "currency": "EUR",
           "lines": [{"position_key": "provision", "line_date": "2026-07-05", "order_ref": "O1/u1", "vat_cents": 3800},
                     {"position_key": "provision_storno", "line_date": "2026-07-10", "order_ref": "OLD/old-unit", "vat_cents": -1900}]}
    result = FeeTaxContext(report.get_vat_effective_from()).assess(row)
    assert result["effective_deductible_vat_cents"] == 3800
    assert result["eligibility_review_count"] == 0


def test_unknown_original_fee_credit_eligibility_requires_review(isolated):
    from app.services.ust_input_vat import FeeTaxContext

    row = {"provider": "kaufland", "doc_type": "fee", "vat_cents": 1900,
           "deductible_vat_cents": 1900, "period_from": "2026-07-01", "period_to": "2026-07-31",
           "invoice_date": "2026-08-01", "currency": "EUR",
           "lines": [{"position_key": "provision", "line_date": "2026-07-05", "vat_cents": 3800},
                     {"position_key": "provision_storno", "line_date": "2026-07-10", "order_ref": "UNKNOWN/missing", "vat_cents": -1900}]}
    result = FeeTaxContext(report.get_vat_effective_from()).assess(row)
    assert result["eligibility_review_count"] == 1


def test_report_excludes_confirmed_june_fees_received_before_july_vat_start(isolated):
    ust_documents.save_input_vat_invoice({"provider": "kaufland", "doc_type": "fee", "invoice_number": "JUNE-FEES",
        "invoice_date": "2026-07-01", "period_to": "2026-06-30", "gross_cents": 11900,
        "net_cents": 10000, "vat_cents": 1900, "deductible_vat_cents": 1900, "input_vat_status": "confirmed"})
    r = report.build_ust_report("2026-07")
    assert r["totals"]["input_vat_cents"] == 0
    assert r["sections"]["input_vat"]["eligibility_adjustment_cents"] == -1900


def test_no_order_placeholder_for_storage_fees_uses_proven_service_date(isolated):
    from app.services.ust_input_vat import FeeTaxContext

    row = {"provider": "amazon", "doc_type": "fee", "vat_cents": 1900,
           "deductible_vat_cents": 1900, "period_from": "2026-07-01", "period_to": "2026-07-31",
           "invoice_date": "2026-07-31", "currency": "EUR",
           "lines": [{"position_key": "fulfillment", "line_date": "2026-07-07", "order_ref": "-", "vat_cents": 1900}]}
    result = FeeTaxContext(report.get_vat_effective_from()).assess(row)
    assert result["effective_deductible_vat_cents"] == 1900
    assert result["eligibility_review_count"] == 0


def test_full_kaufland_monthly_statement_is_not_missing_due_to_order_fee_estimate(isolated):
    unit(isolated, "2026-08-05T12:00:00Z")
    ust_documents.save_input_vat_invoice({"provider": "kaufland", "doc_type": "fee", "invoice_number": "R0926-123",
        "invoice_date": "2026-09-01", "period_from": "2026-08-01", "period_to": "2026-08-31",
        "gross_cents": 1190, "net_cents": 1000, "vat_cents": 190, "deductible_vat_cents": 190,
        "input_vat_status": "confirmed"})
    warnings = report.build_ust_report("2026-08")["warnings"]
    assert not any(w["code"] == "MISSING_FEE_INVOICE" and w["provider"] == "kaufland" for w in warnings)


def test_fee_credit_without_lines_or_original_evidence_requires_review(isolated):
    from app.services.ust_input_vat import FeeTaxContext
    row = {"provider": "amazon", "doc_type": "fee", "vat_cents": -1900, "deductible_vat_cents": -1900,
           "period_from": "2026-08-01", "period_to": "2026-08-31", "currency": "EUR"}
    assert FeeTaxContext(report.get_vat_effective_from()).assess(row)["eligibility_review_count"] == 1


def test_explicit_fee_allocation_is_preserved_with_and_without_lines(isolated):
    from app.services.ust_input_vat import FeeTaxContext
    row = {"provider": "kaufland", "doc_type": "fee", "vat_cents": 1900, "deductible_vat_cents": 950,
           "period_from": "2026-06-01", "period_to": "2026-06-30", "currency": "EUR"}
    assert FeeTaxContext(report.get_vat_effective_from()).assess(row)["effective_deductible_vat_cents"] == 950


def test_unlinked_supplier_fee_credit_reverses_booked_input_vat(isolated):
    with bookkeeping() as c:
        c.execute("INSERT INTO transactions(id,direction,is_vat_deductible,date,vat_amount,type,provider) VALUES ('credit','IN',1,'2026-08-10',1900,'FEE','other')")
    assert report.build_ust_report("2026-08")["totals"]["input_vat_cents"] == -1900
