from __future__ import annotations

import sqlite3

import pytest

from app import db as combined_db
from app.services import ust_schema


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
