from __future__ import annotations

import hashlib
import sqlite3
from io import BytesIO

import pytest

from app import db as combined_db
from app.services import ust_documents, ust_schema


@pytest.fixture
def connection(tmp_path, monkeypatch):
    monkeypatch.setattr(combined_db, "COMBINED_DB_PATH", tmp_path / "combined.sqlite3")
    combined_db.init_combined_db()
    with combined_db.connect_combined_db() as conn:
        yield conn


def _tables(conn: sqlite3.Connection) -> set[str]:
    return {str(row[0]) for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}


# ── Task 1: Schema + EU-Regime ───────────────────────────────────────────────


def test_ust_tables_exist_after_init(connection):
    assert ust_schema.UST_TABLES <= _tables(connection)


def test_init_is_idempotent(tmp_path, monkeypatch):
    monkeypatch.setattr(combined_db, "COMBINED_DB_PATH", tmp_path / "combined.sqlite3")
    combined_db.init_combined_db()
    combined_db.init_combined_db()
    with combined_db.connect_combined_db() as conn:
        assert ust_schema.UST_TABLES <= _tables(conn)


def test_input_vat_invoices_has_periodization_columns(connection):
    cols = _columns(connection, "input_vat_invoices")
    assert {
        "invoice_date",
        "received_date",
        "service_date",
        "period_from",
        "period_to",
        "deduction_month",
    } <= cols


def test_input_vat_invoices_rejects_duplicate_provider_invoice_number(connection):
    insert = (
        "INSERT INTO input_vat_invoices(id,provider,doc_type,invoice_number,invoice_date,"
        "received_date,gross_cents,net_cents,vat_cents,deduction_month,created_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?)"
    )
    connection.execute(
        insert,
        ("a", "kaufland", "fee", "R0226-23464200", "2026-03-01", "2026-03-01", 231899, 194873, 37026, "2026-03", "2026-03-01T00:00:00Z"),
    )
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            insert,
            ("b", "kaufland", "fee", "R0226-23464200", "2026-03-02", "2026-03-02", 1, 1, 0, "2026-03", "2026-03-02T00:00:00Z"),
        )
    connection.execute(
        insert,
        ("c", "amazon", "fee", "R0226-23464200", "2026-03-01", "2026-03-01", 1, 1, 0, "2026-03", "2026-03-01T00:00:00Z"),
    )


def test_input_vat_invoice_lines_cascade_on_delete(connection):
    connection.execute(
        "INSERT INTO input_vat_invoices(id,provider,doc_type,invoice_number,invoice_date,received_date,"
        "gross_cents,net_cents,vat_cents,deduction_month,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        ("inv", "kaufland", "fee", "X", "2026-03-01", "2026-03-01", 100, 84, 16, "2026-03", "t"),
    )
    connection.execute(
        "INSERT INTO input_vat_invoice_lines(id,invoice_id,label,category,gross_cents,net_cents,vat_cents) "
        "VALUES (?,?,?,?,?,?,?)",
        ("line", "inv", "provision", "fee_deductible", 100, 84, 16),
    )
    connection.execute("DELETE FROM input_vat_invoices WHERE id='inv'")
    assert connection.execute("SELECT COUNT(*) FROM input_vat_invoice_lines").fetchone()[0] == 0


def test_amazon_tax_rows_has_inheritance_columns(connection):
    cols = _columns(connection, "amazon_tax_rows")
    assert {
        "shipment_id",
        "transaction_type",
        "tax_class",
        "original_tax_class",
        "net_source",
        "tax_calculation_reason_code",
        "booking_date",
    } <= cols


def test_kaufland_tax_overrides_one_per_unit(connection):
    connection.execute(
        "INSERT INTO kaufland_tax_overrides(id,id_order_unit,from_rate,to_rate,gross_cents,net_cents,vat_cents,reason,created_at) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        ("o1", "u1", 0.0, 19.0, 26990, 22681, 4309, "Falschfeld", "t"),
    )
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            "INSERT INTO kaufland_tax_overrides(id,id_order_unit,from_rate,to_rate,gross_cents,net_cents,vat_cents,reason,created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            ("o2", "u1", 0.0, 19.0, 1, 1, 0, "doppelt", "t"),
        )


def test_ust_reports_revision_unique_per_month(connection):
    insert = (
        "INSERT INTO ust_reports(id,month,revision,kind,supersedes_id,status,snapshot_json,blockers_json,warnings_json,created_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?)"
    )
    connection.execute(insert, ("r1", "2026-07", 1, "original", None, "filed", "{}", "[]", "[]", "t"))
    connection.execute(insert, ("r2", "2026-07", 2, "amendment", "r1", "filed", "{}", "[]", "[]", "t"))
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(insert, ("r3", "2026-07", 1, "original", None, "draft", "{}", "[]", "[]", "t"))
    connection.execute(insert, ("r4", "2026-08", 1, "original", None, "draft", "{}", "[]", "[]", "t"))


def test_seller_profile_eu_regime_defaults_to_unconfirmed(connection):
    cols = _columns(connection, "seller_profiles")
    assert {"eu_tax_regime", "eu_distance_prior_year_cents", "eu_distance_current_year_cents"} <= cols
    connection.execute(
        "INSERT INTO seller_profiles(id,legal_name,created_at,updated_at) VALUES ('default','Test','t','t')"
    )
    row = connection.execute(
        "SELECT eu_tax_regime, eu_distance_prior_year_cents, eu_distance_current_year_cents "
        "FROM seller_profiles WHERE id='default'"
    ).fetchone()
    assert row["eu_tax_regime"] == "unconfirmed"
    assert row["eu_distance_prior_year_cents"] == 0
    assert row["eu_distance_current_year_cents"] == 0


def test_legacy_seller_profile_gains_eu_columns(tmp_path, monkeypatch):
    monkeypatch.setattr(combined_db, "COMBINED_DB_PATH", tmp_path / "combined.sqlite3")
    legacy = sqlite3.connect(tmp_path / "combined.sqlite3")
    legacy.executescript(
        "CREATE TABLE seller_profiles (id TEXT PRIMARY KEY, legal_name TEXT NOT NULL DEFAULT '', "
        "tax_mode TEXT NOT NULL DEFAULT 'small_business', created_at TEXT NOT NULL, updated_at TEXT NOT NULL);"
    )
    legacy.execute("INSERT INTO seller_profiles VALUES ('default','Alt','regular','t','t')")
    legacy.commit()
    legacy.close()
    combined_db.init_combined_db()
    combined_db.init_combined_db()
    with combined_db.connect_combined_db() as conn:
        row = conn.execute("SELECT tax_mode, eu_tax_regime FROM seller_profiles WHERE id='default'").fetchone()
    assert row["tax_mode"] == "regular"
    assert row["eu_tax_regime"] == "unconfirmed"


# ── Task 2: Vorsteuer-Periodisierung ─────────────────────────────────────────
# Verbindliche Regel (UStAE): Vorsteuer faellt in den ERSTEN Zeitraum, in dem
# Leistung UND ordnungsgemaesse Rechnung beide vorliegen.
#   service_month = month(service_date) or month(period_to or period_from or invoice_date)
#   docs_month    = month(received_date or invoice_date)
#   deduction_month = max(service_month, docs_month)


def test_deduction_month_prefers_real_service_date_over_invoice_date():
    """Lieferantenrechnung 28.08., Ware kommt erst 03.09. -> Vorsteuer September."""
    assert (
        ust_documents.resolve_deduction_month(
            invoice_date="2026-08-28", received_date="2026-08-28", service_date="2026-09-03"
        )
        == "2026-09"
    )


def test_delivery_date_is_accepted_as_service_date_alias():
    assert (
        ust_documents.resolve_deduction_month(
            invoice_date="2026-08-28", received_date="2026-08-28", delivery_date="2026-09-03"
        )
        == "2026-09"
    )


def test_service_date_beats_period_end_when_later():
    assert (
        ust_documents.resolve_deduction_month(
            invoice_date="2026-08-28",
            received_date="2026-08-28",
            period_from="2026-08-01",
            period_to="2026-08-31",
            service_date="2026-09-03",
        )
        == "2026-09"
    )


def test_monthly_fee_uses_period_to_as_service_end():
    """Kaufland R0226: Leistung 02/2026, Rechnung/Verfuegbarkeit 01.03. -> Maerz."""
    assert (
        ust_documents.resolve_deduction_month(
            invoice_date="2026-03-01",
            received_date="2026-03-01",
            period_from="2026-02-01",
            period_to="2026-02-28",
        )
        == "2026-03"
    )


def test_fee_service_end_beats_early_invoice():
    assert (
        ust_documents.resolve_deduction_month(
            invoice_date="2026-01-20",
            received_date="2026-01-20",
            period_from="2026-02-01",
            period_to="2026-02-28",
        )
        == "2026-02"
    )


def test_fee_invoice_arriving_within_service_period_stays_in_service_month():
    assert (
        ust_documents.resolve_deduction_month(
            invoice_date="2026-02-15",
            received_date="2026-02-15",
            period_from="2026-02-01",
            period_to="2026-02-28",
        )
        == "2026-02"
    )


def test_period_from_used_when_period_to_missing():
    assert (
        ust_documents.resolve_deduction_month(
            invoice_date="2026-05-10", received_date="2026-05-10", period_from="2026-06-01"
        )
        == "2026-06"
    )


def test_deduction_month_is_max_of_service_and_documents():
    assert (
        ust_documents.resolve_deduction_month(
            invoice_date="2026-02-01", received_date="2026-03-05", service_date="2026-02-20"
        )
        == "2026-03"
    )
    assert (
        ust_documents.resolve_deduction_month(
            invoice_date="2026-02-01", received_date="2026-02-15", service_date="2026-02-20"
        )
        == "2026-02"
    )


def test_invoice_date_is_never_assumed_to_be_the_service_date():
    assert (
        ust_documents.resolve_deduction_month(
            invoice_date="2026-08-28", received_date="2026-08-28", service_date="2026-09-03"
        )
        == "2026-09"
    )
    assert (
        ust_documents.resolve_deduction_month(invoice_date="2026-08-28", received_date="2026-08-28")
        == "2026-08"
    )


def _invoice_payload(**overrides):
    payload = {
        "provider": "kaufland",
        "doc_type": "fee",
        "invoice_number": "R0226-23464200",
        "invoice_date": "2026-08-28",
        "received_date": "2026-08-28",
        "service_date": None,
        "period_from": None,
        "period_to": None,
        "currency": "EUR",
        "gross_cents": 231899,
        "net_cents": 194873,
        "vat_cents": 37026,
        "deductible_vat_cents": 37026,
        "source": "manual",
        "notes": "",
    }
    payload.update(overrides)
    return payload


def test_save_invoice_persists_deduction_month(connection):
    row = ust_documents.save_input_vat_invoice(_invoice_payload())
    assert row["deduction_month"] == "2026-08"
    assert row["input_vat_status"] == "review_required"


def test_save_invoice_persists_service_date(connection):
    row = ust_documents.save_input_vat_invoice(_invoice_payload(service_date="2026-09-03"))
    assert row["service_date"] == "2026-09-03"
    assert row["deduction_month"] == "2026-09"


def test_save_invoice_rejects_duplicate_provider_invoice_number(connection):
    ust_documents.save_input_vat_invoice(_invoice_payload())
    with pytest.raises(ust_documents.UstDocumentError) as excinfo:
        ust_documents.save_input_vat_invoice(_invoice_payload(invoice_date="2026-09-01"))
    assert excinfo.value.status_code == 409


def test_save_invoice_same_number_other_provider_is_allowed(connection):
    ust_documents.save_input_vat_invoice(_invoice_payload())
    assert ust_documents.save_input_vat_invoice(_invoice_payload(provider="amazon"))["provider"] == "amazon"


def test_save_invoice_validates_gross_equals_net_plus_vat(connection):
    with pytest.raises(ust_documents.UstDocumentError) as excinfo:
        ust_documents.save_input_vat_invoice(_invoice_payload(gross_cents=100, net_cents=50, vat_cents=10))
    assert excinfo.value.status_code == 400


def test_save_invoice_validates_deductible_not_above_vat(connection):
    with pytest.raises(ust_documents.UstDocumentError):
        ust_documents.save_input_vat_invoice(_invoice_payload(deductible_vat_cents=37027))


def test_damage_compensation_carries_no_input_vat(connection):
    row = ust_documents.save_input_vat_invoice(
        _invoice_payload(
            doc_type="damage_compensation",
            invoice_number="C0326-80490",
            gross_cents=12597,
            net_cents=12597,
            vat_cents=0,
            deductible_vat_cents=0,
        )
    )
    assert row["vat_cents"] == 0
    assert row["deductible_vat_cents"] == 0
    assert row["input_vat_status"] == "non_deductible"


def test_damage_compensation_rejects_vat(connection):
    with pytest.raises(ust_documents.UstDocumentError):
        ust_documents.save_input_vat_invoice(
            _invoice_payload(
                doc_type="damage_compensation",
                invoice_number="C0726-91224",
                gross_cents=5723,
                net_cents=5000,
                vat_cents=723,
                deductible_vat_cents=0,
            )
        )


def test_editing_received_date_shifts_deduction_month(connection):
    row = ust_documents.save_input_vat_invoice(
        _invoice_payload(invoice_date="2026-08-28", received_date="2026-08-28", service_date="2026-09-03")
    )
    assert row["deduction_month"] == "2026-09"
    updated = ust_documents.update_input_vat_invoice(row["id"], {"received_date": "2026-10-05"})
    assert updated["received_date"] == "2026-10-05"
    assert updated["deduction_month"] == "2026-10"


def test_only_confirmed_invoices_count_as_input_vat(connection):
    ust_documents.save_input_vat_invoice(
        _invoice_payload(invoice_number="A-1", gross_cents=11900, net_cents=10000, vat_cents=1900, deductible_vat_cents=1900)
    )
    ust_documents.save_input_vat_invoice(
        _invoice_payload(invoice_number="A-2", gross_cents=17800, net_cents=14900, vat_cents=2900, deductible_vat_cents=2900)
    )
    rows = ust_documents.list_input_vat_invoices(deduction_month="2026-08")
    ust_documents.set_input_vat_status(rows[0]["id"], "confirmed")
    sums = ust_documents.sum_input_vat_by_deduction_month("2026-08")
    assert sums["kaufland_fees_cents"] == 1900
    assert sums["pending_review_cents"] == 2900
    assert sums["pending_review_count"] == 1


def test_sum_buckets_by_doc_type_and_provider(connection):
    ust_documents.save_input_vat_invoice(
        _invoice_payload(invoice_number="P-1", doc_type="purchase", provider="other",
                         gross_cents=1100, net_cents=1000, vat_cents=100, deductible_vat_cents=100)
    )
    ust_documents.save_input_vat_invoice(
        _invoice_payload(invoice_number="A-1", provider="amazon",
                         gross_cents=2200, net_cents=2000, vat_cents=200, deductible_vat_cents=200)
    )
    ust_documents.save_input_vat_invoice(
        _invoice_payload(invoice_number="K-1", provider="kaufland",
                         gross_cents=4400, net_cents=4000, vat_cents=400, deductible_vat_cents=400)
    )
    ust_documents.save_input_vat_invoice(
        _invoice_payload(invoice_number="C-1", doc_type="damage_compensation",
                         gross_cents=18320, net_cents=18320, vat_cents=0, deductible_vat_cents=0)
    )
    for row in ust_documents.list_input_vat_invoices(deduction_month="2026-08"):
        if row["doc_type"] != "damage_compensation":
            ust_documents.set_input_vat_status(row["id"], "confirmed")
    sums = ust_documents.sum_input_vat_by_deduction_month("2026-08")
    assert sums["purchases_cents"] == 100
    assert sums["amazon_fees_cents"] == 200
    assert sums["kaufland_fees_cents"] == 400
    assert sums["other_cents"] == 0
    assert sums["nontaxable_cents"] == 18320
    assert sums["pending_review_cents"] == 0


def test_document_file_is_stored_and_retrievable(connection, tmp_path, monkeypatch):
    monkeypatch.setattr(ust_documents, "BOOKKEEPING_DOCUMENTS_DIR", tmp_path / "documents")
    row = ust_documents.save_input_vat_invoice(
        _invoice_payload(), file_bytes=b"%PDF-1.4 test", filename="rechnung.pdf"
    )
    assert row["sha256"] == hashlib.sha256(b"%PDF-1.4 test").hexdigest()
    path, filename = ust_documents.get_document_file(row["id"])
    assert filename == "rechnung.pdf"
    assert path.read_bytes() == b"%PDF-1.4 test"


def test_duplicate_document_bytes_are_not_stored_twice(connection, tmp_path, monkeypatch):
    monkeypatch.setattr(ust_documents, "BOOKKEEPING_DOCUMENTS_DIR", tmp_path / "documents")
    ust_documents.save_input_vat_invoice(_invoice_payload(), file_bytes=b"%PDF-1.4 same", filename="a.pdf")
    with pytest.raises(ust_documents.UstDocumentError) as excinfo:
        ust_documents.save_input_vat_invoice(
            _invoice_payload(invoice_number="R0326-23591132"), file_bytes=b"%PDF-1.4 same", filename="b.pdf"
        )
    assert excinfo.value.status_code == 409
    assert len(list((tmp_path / "documents").rglob("*.pdf"))) == 1


def test_failed_save_leaves_no_orphan_file(connection, tmp_path, monkeypatch):
    monkeypatch.setattr(ust_documents, "BOOKKEEPING_DOCUMENTS_DIR", tmp_path / "documents")
    ust_documents.save_input_vat_invoice(_invoice_payload())
    before = set((tmp_path / "documents").rglob("*"))
    with pytest.raises(ust_documents.UstDocumentError):
        ust_documents.save_input_vat_invoice(
            _invoice_payload(), file_bytes=b"%PDF-1.4 orphan", filename="orphan.pdf"
        )
    assert set((tmp_path / "documents").rglob("*")) == before


def test_list_filters_by_deduction_month_and_provider(connection):
    ust_documents.save_input_vat_invoice(
        _invoice_payload(invoice_number="A-1", provider="amazon", service_date="2026-09-03")
    )
    ust_documents.save_input_vat_invoice(_invoice_payload(invoice_number="K-1", provider="kaufland"))
    assert len(ust_documents.list_input_vat_invoices(deduction_month="2026-08")) == 1
    assert len(ust_documents.list_input_vat_invoices(deduction_month="2026-09")) == 1
    assert len(ust_documents.list_input_vat_invoices(provider="amazon")) == 1
    assert len(ust_documents.list_input_vat_invoices(status="review_required")) == 2
