"""Kaufland-Bemessungsgrundlage, manuelle Steuersatzkorrektur und EU-Fernabsatzschwelle.

Kaufland `vat` ist ein PROZENTSATZ (19.0 / 0.0), KEIN Betrag -- verifiziert an
kaufland_data.sqlite3 (675x 19.0, 42x 0.0). `price` und `shipping_rate` sind
Cent-Betraege als TEXT. Bemessungsgrundlage ist das Kundenbrutto (price +
shipping_rate); die Steuer wird herausgerechnet, nicht aufgeschlagen.

Die 42 `vat=0`-Zeilen sind Falschfelder (revenue_gross < revenue_net) und werden
nach `vat_effective_from` manuell auf 19 % korrigierbar. Vor dem USt-Startdatum
ist 0 % korrekt (Kleinunternehmer).
"""
from __future__ import annotations

import sqlite3

import pytest

from app import db as combined_db
from app.services import ust_documents, ust_report, ust_schema

KAUFLAND_SCHEMA = """
CREATE TABLE order_units(
  id_order_unit TEXT PRIMARY KEY, id_order TEXT, ts_created_iso TEXT, status TEXT,
  price TEXT, revenue_gross TEXT, revenue_net TEXT, vat REAL, shipping_rate TEXT,
  storefront TEXT, is_marketplace_deemed_supplier INTEGER, shipping_country TEXT,
  raw_json TEXT NOT NULL DEFAULT '{}', synced_at_iso TEXT NOT NULL DEFAULT '');
CREATE TABLE order_unit_refunds(
  id INTEGER PRIMARY KEY AUTOINCREMENT, id_order_unit TEXT NOT NULL, position INTEGER NOT NULL,
  amount TEXT, reason TEXT, raw_json TEXT NOT NULL DEFAULT '{}', synced_at_iso TEXT NOT NULL DEFAULT '',
  UNIQUE(id_order_unit, position));
CREATE TABLE returns(
  id_return TEXT PRIMARY KEY, ts_created_iso TEXT, status TEXT, storefront TEXT,
  return_units_json TEXT, raw_json TEXT NOT NULL DEFAULT '{}', synced_at_iso TEXT NOT NULL DEFAULT '');
CREATE TABLE return_units(
  id_return_unit TEXT PRIMARY KEY, id_return TEXT NOT NULL, id_order_unit TEXT,
  ts_created_iso TEXT, status TEXT, note TEXT, reason TEXT, storefront TEXT,
  raw_json TEXT NOT NULL DEFAULT '{}', synced_at_iso TEXT NOT NULL DEFAULT '');
"""


@pytest.fixture
def combined(tmp_path, monkeypatch):
    monkeypatch.setattr(combined_db, "COMBINED_DB_PATH", tmp_path / "combined.sqlite3")
    combined_db.init_combined_db()
    return combined_db


@pytest.fixture
def kaufland(tmp_path, monkeypatch):
    connection = sqlite3.connect(tmp_path / "kaufland.sqlite3")
    connection.executescript(KAUFLAND_SCHEMA)
    monkeypatch.setattr(ust_report, "KAUFLAND_DB_PATH", tmp_path / "kaufland.sqlite3")
    yield connection
    connection.close()


def _unit(connection, unit_id, *, vat=19.0, price="26990", shipping_rate="0",
          created="2026-02-21T09:59:28Z", status="received", country="DE", order=None):
    connection.execute(
        "INSERT INTO order_units(id_order_unit,id_order,ts_created_iso,status,price,"
        "revenue_gross,revenue_net,vat,shipping_rate,is_marketplace_deemed_supplier,"
        "shipping_country) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (unit_id, order or f"O-{unit_id}", created, status, price,
         str(int(int(price) * 0.85)), str(int(int(price) * 0.85 / 1.19)),
         vat, shipping_rate, 0, country),
    )
    connection.commit()


def set_tax_settings(connection_factory, *, tax_mode="regular", vat_effective_from="2026-01-01"):
    with connection_factory() as connection:
        connection.execute(
            "INSERT OR REPLACE INTO seller_profiles(id,legal_name,tax_mode,vat_effective_from,"
            "eu_tax_regime,created_at,updated_at) VALUES ('default','Test',?,?,'unconfirmed','t','t')",
            (tax_mode, vat_effective_from),
        )


# ── Task 6: `vat` ist ein Prozentsatz ───────────────────────────────────────


def test_kaufland_vat_is_a_rate_not_an_amount(combined, kaufland):
    """vat=19 bedeutet 19 Prozent, nicht 19 Cent. Betrag: gross * 19 / 119."""
    set_tax_settings(combined.connect_combined_db)
    _unit(kaufland, "u1", vat=19.0, price="26990", created="2026-02-21T09:59:28Z")
    rows = ust_report.load_kaufland_vat_rows("2026-02")
    assert len(rows) == 1
    row = rows[0]
    assert row["vat_rate"] == 19.0
    assert row["gross_cents"] == 26990
    # 26990 * 19 / 119 = 4308.4 -> 4309; Netto = 22681
    assert row["output_vat_cents"] == 4309
    assert row["net_cents"] == 22681
    assert row["gross_cents"] == row["net_cents"] + row["output_vat_cents"]


def test_kaufland_tax_base_includes_shipping(combined, kaufland):
    set_tax_settings(combined.connect_combined_db)
    _unit(kaufland, "u1", vat=19.0, price="10000", shipping_rate="499")
    row = ust_report.load_kaufland_vat_rows("2026-02")[0]
    assert row["gross_cents"] == 10499
    assert row["net_cents"] + row["output_vat_cents"] == 10499


def test_kaufland_vat_zero_after_vat_start_needs_override(combined, kaufland):
    """Falschfeld: alle Artikel sollen 19 % haben. Ohne Korrektur blockiert es."""
    set_tax_settings(combined.connect_combined_db, vat_effective_from="2026-01-01")
    _unit(kaufland, "u1", vat=0.0, price="26990", created="2026-02-21T09:59:28Z")
    row = ust_report.load_kaufland_vat_rows("2026-02")[0]
    assert row["tax_class"] == "kaufland_rate_needs_override"
    assert row["blocker"] is True
    assert row["output_vat_cents"] == 0


def test_kaufland_vat_zero_before_vat_start_is_correct(combined, kaufland):
    """Vor dem USt-Start ist 0 % richtig (Kleinunternehmer) -- kein Fehler."""
    set_tax_settings(combined.connect_combined_db, vat_effective_from="2026-03-01")
    _unit(kaufland, "u1", vat=0.0, price="26990", created="2026-02-21T09:59:28Z")
    row = ust_report.load_kaufland_vat_rows("2026-02")[0]
    assert row["tax_class"] == "pre_vat"
    assert row["blocker"] is False
    assert row["output_vat_cents"] == 0


def test_manual_override_recomputes_german_vat(combined, kaufland):
    set_tax_settings(combined.connect_combined_db, vat_effective_from="2026-01-01")
    _unit(kaufland, "u1", vat=0.0, price="26990", created="2026-02-21T09:59:28Z")
    assert ust_report.load_kaufland_vat_rows("2026-02")[0]["blocker"] is True
    ust_report.save_kaufland_override("u1", to_rate=19.0, reason="Falschfeld, alle Artikel 19 %")
    row = ust_report.load_kaufland_vat_rows("2026-02")[0]
    assert row["tax_class"] == "de_b2c"
    assert row["blocker"] is False
    assert row["net_source"] == "computed_from_rate"
    assert row["rate_source"] == "override"
    assert row["net_cents"] == 22681
    assert row["output_vat_cents"] == 4309


def test_override_is_persisted_with_audit_trail(combined, kaufland):
    set_tax_settings(combined.connect_combined_db, vat_effective_from="2026-01-01")
    _unit(kaufland, "u1", vat=0.0, price="26990", created="2026-02-21T09:59:28Z")
    ust_report.save_kaufland_override("u1", to_rate=19.0, reason="Falschfeld")
    with combined.connect_combined_db() as connection:
        row = connection.execute("SELECT * FROM kaufland_tax_overrides WHERE id_order_unit='u1'").fetchone()
    assert row["from_rate"] == 0.0
    assert row["to_rate"] == 19.0
    assert row["reason"] == "Falschfeld"
    assert row["created_at"]


def test_bulk_override_covers_all_zero_rate_units_of_a_month(combined, kaufland):
    set_tax_settings(combined.connect_combined_db, vat_effective_from="2026-01-01")
    _unit(kaufland, "u1", vat=0.0, price="26990", created="2026-02-10T00:00:00Z")
    _unit(kaufland, "u2", vat=0.0, price="44990", created="2026-02-20T00:00:00Z")
    _unit(kaufland, "u3", vat=19.0, price="10000", created="2026-02-15T00:00:00Z")
    result = ust_report.bulk_override_zero_rates("2026-02", reason="alle 0-%-Felder auf 19 %")
    assert result["overridden"] == 2
    rows = {row["id_order_unit"]: row for row in ust_report.load_kaufland_vat_rows("2026-02")}
    assert all(rows[key]["blocker"] is False for key in ("u1", "u2", "u3"))
    assert rows["u1"]["rate_source"] == "override"
    assert rows["u3"]["rate_source"] == "api"


def test_cancelled_units_are_not_tax_base(combined, kaufland):
    set_tax_settings(combined.connect_combined_db, vat_effective_from="2026-01-01")
    _unit(kaufland, "u1", vat=19.0, price="26990", created="2026-02-10T00:00:00Z", status="cancelled")
    _unit(kaufland, "u2", vat=19.0, price="10000", created="2026-02-11T00:00:00Z", status="received")
    rows = ust_report.load_kaufland_vat_rows("2026-02")
    assert [row["id_order_unit"] for row in rows] == ["u2"]
    assert ust_report.load_kaufland_vat_rows("2026-02")[0]["gross_cents"] == 10000


def test_order_unit_refunds_net_against_the_sale_month(combined, kaufland):
    """order_unit_refunds hat kein eigenes Buchungsdatum; die Gutschrift mindert
    den Umsatz der Unit (wie in order_summaries.kaufland_summary_from_row)."""
    set_tax_settings(combined.connect_combined_db, vat_effective_from="2026-01-01")
    _unit(kaufland, "u1", vat=19.0, price="11900", created="2026-02-10T00:00:00Z")
    kaufland.execute(
        "INSERT INTO order_unit_refunds(id_order_unit,position,amount,reason) VALUES ('u1',0,'1190','Teilerstattung')"
    )
    kaufland.commit()
    row = ust_report.load_kaufland_vat_rows("2026-02")[0]
    assert row["refund_cents"] == 1190
    assert row["gross_cents"] == 10710
    assert row["net_cents"] + row["output_vat_cents"] == 10710


def test_kaufland_returns_fall_into_their_own_booking_month(combined, kaufland):
    """Retouren in dem Monat, in dem sie verbucht werden (returns.ts_created_iso)."""
    set_tax_settings(combined.connect_combined_db, vat_effective_from="2026-01-01")
    _unit(kaufland, "u1", vat=19.0, price="11900", created="2026-02-10T00:00:00Z")
    kaufland.execute(
        "INSERT INTO returns(id_return,ts_created_iso,status,storefront) "
        "VALUES ('r1','2026-03-15T10:00:00Z','completed','de')"
    )
    kaufland.execute(
        "INSERT INTO return_units(id_return_unit,id_return,id_order_unit,ts_created_iso,status) "
        "VALUES ('ru1','r1','u1','2026-03-15T10:00:00Z','received')"
    )
    kaufland.commit()
    assert ust_report.load_kaufland_vat_rows("2026-02") != []
    march = ust_report.load_kaufland_returns("2026-03")
    assert march["count"] == 1
    assert march["order_unit_ids"] == ["u1"]
    assert ust_report.load_kaufland_returns("2026-02")["count"] == 0


def test_kaufland_deemed_supplier_is_reported_separately(combined, kaufland):
    set_tax_settings(combined.connect_combined_db, vat_effective_from="2026-01-01")
    kaufland.execute(
        "INSERT INTO order_units(id_order_unit,id_order,ts_created_iso,status,price,revenue_gross,"
        "revenue_net,vat,shipping_rate,is_marketplace_deemed_supplier,shipping_country) "
        "VALUES ('u1','O-1','2026-02-10T00:00:00Z','received','11900','10000','8403',19.0,'0',1,'DE')"
    )
    kaufland.commit()
    row = ust_report.load_kaufland_vat_rows("2026-02")[0]
    assert row["tax_class"] == "deemed_supplier"
    assert row["output_vat_cents"] == 0


# ── Task 7: EU-Fernabsatzschwelle (10.000 EUR, NICHT §19 100.000 EUR) ─────


def test_distance_threshold_is_distinct_from_the_section_19_threshold():
    assert ust_report.EU_DISTANCE_SELLING_THRESHOLD_CENTS == 10_000 * 100
    from app.services.tax_reporting import VAT_THRESHOLD_CENTS

    assert ust_report.EU_DISTANCE_SELLING_THRESHOLD_CENTS != VAT_THRESHOLD_CENTS
    assert VAT_THRESHOLD_CENTS == 100_000 * 100


def test_home_rate_allowed_when_both_years_below_threshold():
    decision = ust_report.evaluate_eu_b2c_regime(
        eu_tax_regime=ust_schema.EU_TAX_REGIME_HOME_RATE,
        eu_distance_prior_year_cents=999_999,
        eu_distance_current_year_cents=999_999,
    )
    assert decision["allow_home_rate"] is True
    assert decision["threshold_cents"] == 10_000 * 100


def test_home_rate_blocked_when_prior_year_exceeds_threshold():
    decision = ust_report.evaluate_eu_b2c_regime(
        eu_tax_regime=ust_schema.EU_TAX_REGIME_HOME_RATE,
        eu_distance_prior_year_cents=10_000 * 100,
        eu_distance_current_year_cents=100,
    )
    assert decision["allow_home_rate"] is False


def test_home_rate_blocked_when_current_year_exceeds_threshold():
    decision = ust_report.evaluate_eu_b2c_regime(
        eu_tax_regime=ust_schema.EU_TAX_REGIME_HOME_RATE,
        eu_distance_prior_year_cents=100,
        eu_distance_current_year_cents=10_000 * 100,
    )
    assert decision["allow_home_rate"] is False


def test_home_rate_blocked_without_confirmed_regime():
    decision = ust_report.evaluate_eu_b2c_regime(
        eu_tax_regime=ust_schema.EU_TAX_REGIME_UNCONFIRMED,
        eu_distance_prior_year_cents=100,
        eu_distance_current_year_cents=100,
    )
    assert decision["allow_home_rate"] is False


def test_home_rate_blocked_under_oss_regime():
    decision = ust_report.evaluate_eu_b2c_regime(
        eu_tax_regime=ust_schema.EU_TAX_REGIME_OSS,
        eu_distance_prior_year_cents=100,
        eu_distance_current_year_cents=100,
    )
    assert decision["allow_home_rate"] is False


def test_distance_threshold_accumulates_eu_b2c_outside_de(combined, kaufland):
    set_tax_settings(combined.connect_combined_db, vat_effective_from="2026-01-01")
    with combined.connect_combined_db() as connection:
        connection.execute(
            "UPDATE seller_profiles SET eu_tax_regime='home_rate_under_threshold' WHERE id='default'"
        )
    _unit(kaufland, "at1", vat=0.0, price="2990", created="2026-02-10T00:00:00Z", country="AT")
    _unit(kaufland, "de1", vat=19.0, price="50000", created="2026-02-11T00:00:00Z", country="DE")
    summary = ust_report.load_eu_distance_summary("2026")
    assert summary["eu_b2c_gross_cents"] == 2990
    assert summary["includes_domestic"] is False


# ── Task 8: Report-Builder, Sperrlogik, Snapshots + Amendments ──────────────

from app.services import ust_documents, ust_schema


# ── Task 8: Report-Builder, Sperrlogik, Snapshots + Amendments ──────────────
# Harte Blocker (verhindern `filed`): AMAZON_UNRESOLVED,
#   KAUFLAND_RATE_NEEDS_OVERRIDE, INPUT_VAT_PENDING_REVIEW,
#   TAX_MODE_NOT_REGULAR, NO_VAT_START_DATE, UNRESOLVED_RETURN_LINK
# Weiche Warnung: MISSING_FEE_INVOICE + input_vat_incomplete. Nach Punkt 9 ist
#   das kein harter Blocker -- die fehlende Gebuehrenrechnung macht die
#   Ausgangs-USt nicht falsch. Nur die benannte Business-Regel
#   `block_filing_when_input_vat_incomplete` (Default false) macht Blocker daraus.

import json as _json

from app.services import ust_documents as _docs


def test_vat_payable_is_output_minus_input(combined, kaufland):
    set_tax_settings(combined.connect_combined_db, vat_effective_from="2026-01-01")
    _unit(kaufland, "u1", vat=19.0, price="11900", created="2026-02-10T00:00:00Z")
    _docs.save_input_vat_invoice({
        "provider": "kaufland", "doc_type": "fee", "invoice_number": "R0226-1",
        "invoice_date": "2026-03-01", "received_date": "2026-03-01",
        "period_from": "2026-02-01", "period_to": "2026-02-28",
        "gross_cents": 231899, "net_cents": 194873, "vat_cents": 37026,
        "deductible_vat_cents": 37026,
    })
    for row in _docs.list_input_vat_invoices(deduction_month="2026-03"):
        _docs.set_input_vat_status(row["id"], "confirmed")
    feb = ust_report.build_ust_report("2026-02")
    assert feb["totals"]["output_vat_cents"] == 1900
    assert feb["totals"]["input_vat_cents"] == 0
    assert feb["totals"]["vat_payable_cents"] == 1900
    march = ust_report.build_ust_report("2026-03")
    assert march["totals"]["input_vat_cents"] == 37026
    assert march["totals"]["vat_payable_cents"] == -37026


def test_input_vat_is_split_by_doc_type_and_provider(combined, kaufland):
    set_tax_settings(combined.connect_combined_db, vat_effective_from="2026-01-01")
    for provider, doc_type, number, gross, net, vat in (
        ("other", "purchase", "EK-1", 1100, 1000, 100),
        ("amazon", "fee", "A-1", 2200, 2000, 200),
        ("kaufland", "fee", "K-1", 4400, 4000, 400),
        ("kaufland", "other", "SO-1", 12, 10, 2),
    ):
        _docs.save_input_vat_invoice({
            "provider": provider, "doc_type": doc_type, "invoice_number": number,
            "invoice_date": "2026-03-05", "received_date": "2026-03-05",
            "gross_cents": gross, "net_cents": net, "vat_cents": vat,
            "deductible_vat_cents": vat,
        })
    for row in _docs.list_input_vat_invoices(deduction_month="2026-03"):
        _docs.set_input_vat_status(row["id"], "confirmed")
    section = ust_report.build_ust_report("2026-03")["sections"]["input_vat"]
    assert (section["purchases_cents"], section["amazon_fees_cents"],
            section["kaufland_fees_cents"], section["other_cents"]) == (100, 200, 400, 2)


def test_damage_compensation_is_nontaxable_without_input_vat(combined, kaufland):
    set_tax_settings(combined.connect_combined_db, vat_effective_from="2026-01-01")
    for number, gross in (("C0326-80490", 12597), ("C0726-91224", 5723)):
        _docs.save_input_vat_invoice({
            "provider": "kaufland", "doc_type": "damage_compensation", "invoice_number": number,
            "invoice_date": "2026-03-05", "received_date": "2026-03-05",
            "gross_cents": gross, "net_cents": gross, "vat_cents": 0, "deductible_vat_cents": 0,
        })
    section = ust_report.build_ust_report("2026-03")["sections"]["input_vat"]
    assert section["nontaxable_cents"] == 18320
    assert section["kaufland_fees_cents"] == 0


def _amazon_row(**overrides):
    row = {
        "id": "r1", "transaction_id": "T1", "order_id": "O1", "shipment_id": "S1",
        "seller_sku": "A", "transaction_type": "SHIPMENT", "tax_class": "unresolved",
        "original_tax_class": None, "net_source": "amazon_components", "vat_rate": 0.0,
        "booking_date": "2026-02-10", "gross_cents": 2699, "net_cents": 2699,
        "output_vat_cents": 0, "raw_json": "{}", "imported_at": "t",
    }
    row.update(overrides)
    return row


def _insert_amazon_rows(connection, rows):
    for row in rows:
        connection.execute(
            "INSERT INTO amazon_tax_rows(id,transaction_id,order_id,shipment_id,seller_sku,"
            "transaction_type,tax_class,original_tax_class,net_source,vat_rate,booking_date,"
            "gross_cents,net_cents,output_vat_cents,raw_json,imported_at) "
            "VALUES (:id,:transaction_id,:order_id,:shipment_id,:seller_sku,:transaction_type,"
            ":tax_class,:original_tax_class,:net_source,:vat_rate,:booking_date,:gross_cents,"
            ":net_cents,:output_vat_cents,:raw_json,:imported_at)", row)


def test_unclassified_amazon_rows_block_filing(combined, kaufland):
    set_tax_settings(combined.connect_combined_db, vat_effective_from="2026-01-01")
    with combined.connect_combined_db() as connection:
        _insert_amazon_rows(connection, [_amazon_row()])
    report = ust_report.build_ust_report("2026-02")
    assert report["status"] == "draft"
    assert any(b["code"] == "AMAZON_UNRESOLVED" for b in report["blockers"])
    with pytest.raises(_docs.UstDocumentError) as excinfo:
        ust_report.file_report("2026-02")
    assert excinfo.value.status_code == 409


def test_unresolved_return_link_blocks_filing(combined, kaufland):
    set_tax_settings(combined.connect_combined_db, vat_effective_from="2026-01-01")
    with combined.connect_combined_db() as connection:
        _insert_amazon_rows(connection, [_amazon_row(
            id="r2", transaction_type="RETURN", tax_class="unresolved_return_link",
            gross_cents=-2990, net_cents=-2990)])
    assert any(b["code"] == "UNRESOLVED_RETURN_LINK"
               for b in ust_report.build_ust_report("2026-02")["blockers"])


def test_kaufland_rate_override_blocks_filing(combined, kaufland):
    set_tax_settings(combined.connect_combined_db, vat_effective_from="2026-01-01")
    _unit(kaufland, "u1", vat=0.0, price="26990", created="2026-02-21T09:59:28Z")
    assert any(b["code"] == "KAUFLAND_RATE_NEEDS_OVERRIDE"
               for b in ust_report.build_ust_report("2026-02")["blockers"])
    with pytest.raises(_docs.UstDocumentError):
        ust_report.file_report("2026-02")


def test_pending_review_input_vat_blocks_filing(combined, kaufland):
    set_tax_settings(combined.connect_combined_db, vat_effective_from="2026-01-01")
    _docs.save_input_vat_invoice({
        "provider": "kaufland", "doc_type": "fee", "invoice_number": "K-1",
        "invoice_date": "2026-03-05", "received_date": "2026-03-05",
        "gross_cents": 4400, "net_cents": 4000, "vat_cents": 400, "deductible_vat_cents": 400,
    })
    assert any(b["code"] == "INPUT_VAT_PENDING_REVIEW"
               for b in ust_report.build_ust_report("2026-03")["blockers"])
    with pytest.raises(_docs.UstDocumentError):
        ust_report.file_report("2026-03")


def test_missing_fee_invoice_is_a_warning_not_a_hard_blocker(combined, kaufland):
    """Punkt 9: fehlende Gebuehrenrechnung -> input_vat_incomplete, aber KEIN Blocker."""
    set_tax_settings(combined.connect_combined_db, vat_effective_from="2026-01-01")
    _unit(kaufland, "u1", vat=19.0, price="11900", created="2026-02-10T00:00:00Z")
    report = ust_report.build_ust_report("2026-02")
    assert report["status"] == "ready"
    assert report["sections"]["input_vat"]["input_vat_incomplete"] is True
    assert any(w["code"] == "MISSING_FEE_INVOICE" for w in report["warnings"])
    assert not any(b["code"] == "MISSING_FEE_INVOICE" for b in report["blockers"])
    assert ust_report.file_report("2026-02")["status"] == "filed"


def test_named_business_rule_can_block_on_incomplete_input_vat(combined, kaufland):
    set_tax_settings(combined.connect_combined_db, vat_effective_from="2026-01-01")
    _unit(kaufland, "u1", vat=19.0, price="11900", created="2026-02-10T00:00:00Z")
    report = ust_report.build_ust_report("2026-02", block_filing_when_input_vat_incomplete=True)
    assert report["business_rules"]["block_filing_when_input_vat_incomplete"] is True
    assert any(b["code"] == "MISSING_FEE_INVOICE" for b in report["blockers"])
    with pytest.raises(_docs.UstDocumentError):
        ust_report.file_report("2026-02", block_filing_when_input_vat_incomplete=True)


def test_tax_mode_must_be_regular(combined, kaufland):
    set_tax_settings(combined.connect_combined_db, tax_mode="small_business",
                     vat_effective_from="2026-01-01")
    assert any(b["code"] == "TAX_MODE_NOT_REGULAR"
               for b in ust_report.build_ust_report("2026-02")["blockers"])


def test_missing_vat_start_date_blocks(combined, kaufland):
    set_tax_settings(combined.connect_combined_db, tax_mode="regular", vat_effective_from="")
    assert any(b["code"] == "NO_VAT_START_DATE"
               for b in ust_report.build_ust_report("2026-02")["blockers"])


def test_filed_snapshot_is_immutable_and_amendment_adds_a_revision(combined, kaufland):
    set_tax_settings(combined.connect_combined_db, vat_effective_from="2026-01-01")
    _unit(kaufland, "u1", vat=19.0, price="11900", created="2026-02-10T00:00:00Z")
    first = ust_report.file_report("2026-02")
    assert (first["revision"], first["kind"], first["status"]) == (1, "original", "filed")

    kaufland.execute(
        "INSERT INTO order_unit_refunds(id_order_unit,position,amount,reason) "
        "VALUES ('u1',0,'1190','spaet erkannt')"
    )
    kaufland.commit()
    second = ust_report.amend_report("2026-02")
    assert (second["revision"], second["kind"]) == (2, "amendment")
    assert second["supersedes_id"] == first["id"]

    with combined.connect_combined_db() as connection:
        rows = connection.execute(
            "SELECT revision, kind, snapshot_json FROM ust_reports WHERE month='2026-02' ORDER BY revision"
        ).fetchall()
    assert [row["revision"] for row in rows] == [1, 2]
    assert [row["kind"] for row in rows] == ["original", "amendment"]
    assert _json.loads(rows[0]["snapshot_json"])["sections"]["kaufland"]["revenue_after_returns_cents"] == 11900
    assert _json.loads(rows[1]["snapshot_json"])["sections"]["kaufland"]["revenue_after_returns_cents"] == 10710


def test_get_report_returns_latest_revision(combined, kaufland):
    set_tax_settings(combined.connect_combined_db, vat_effective_from="2026-01-01")
    _unit(kaufland, "u1", vat=19.0, price="11900", created="2026-02-10T00:00:00Z")
    ust_report.file_report("2026-02")
    ust_report.amend_report("2026-02")
    report = ust_report.get_ust_report("2026-02")
    assert (report["revision"], report["kind"], report["status"]) == (2, "amendment", "filed")


def test_list_report_months_shows_revision_and_kind(combined, kaufland):
    set_tax_settings(combined.connect_combined_db, vat_effective_from="2026-01-01")
    _unit(kaufland, "u1", vat=19.0, price="11900", created="2026-02-10T00:00:00Z")
    ust_report.file_report("2026-02")
    ust_report.amend_report("2026-02")
    entry = [item for item in ust_report.list_report_months() if item["month"] == "2026-02"][0]
    assert (entry["revision"], entry["kind"], entry["status"]) == (2, "amendment", "filed")
    assert entry["supersedes_id"]


def test_amazon_sections_split_output_vat_by_class(combined, kaufland):
    set_tax_settings(combined.connect_combined_db, vat_effective_from="2026-01-01")
    with combined.connect_combined_db() as connection:
        _insert_amazon_rows(connection, [
            _amazon_row(id="a1", tax_class="de_b2c", gross_cents=2990,
                        net_cents=2513, output_vat_cents=477),
            _amazon_row(id="a2", tax_class="eu_b2b_intra_community_supply",
                        gross_cents=2849, net_cents=2849, output_vat_cents=0),
            _amazon_row(id="a3", tax_class="eu_b2c_home_rate",
                        net_source="computed_home_rate", gross_cents=2990,
                        net_cents=2513, output_vat_cents=477),
        ])
    section = ust_report.build_ust_report("2026-02")["sections"]["amazon"]
    assert section["de_b2c"]["output_vat"] == 477
    assert section["de_b2c"]["count"] == 1
    assert section["eu_b2b_intra_community_supply"]["count"] == 1
    assert section["eu_b2b_intra_community_supply"]["output_vat"] == 0
    assert section["eu_b2c_home_rate"]["output_vat"] == 477
    assert section["unresolved"]["count"] == 0


def test_amazon_returns_are_reported_in_their_booking_month(combined, kaufland):
    set_tax_settings(combined.connect_combined_db, vat_effective_from="2026-01-01")
    with combined.connect_combined_db() as connection:
        _insert_amazon_rows(connection, [
            _amazon_row(id="a1", tax_class="de_b2c", booking_date="2026-02-10",
                        gross_cents=2990, net_cents=2513, output_vat_cents=477),
            _amazon_row(id="a2", transaction_type="RETURN", tax_class="de_b2c",
                        original_tax_class="de_b2c", booking_date="2026-03-08",
                        gross_cents=-2990, net_cents=-2513, output_vat_cents=-477),
        ])
    assert ust_report.build_ust_report("2026-02")["sections"]["amazon"]["returns"]["count"] == 0
    march = ust_report.build_ust_report("2026-03")["sections"]["amazon"]
    assert march["returns"]["count"] == 1
    assert march["returns"]["output_vat"] == -477
