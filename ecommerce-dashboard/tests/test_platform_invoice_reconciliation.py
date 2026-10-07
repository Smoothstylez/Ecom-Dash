"""Haertung: keine Doppelbuchung, keine Fehlzuordnung, exakte Verrechnung.

Antwortet auf die konkrete Frage: wird richtig zugeordnet und verrechnet, und
kann etwas doppelt gebucht werden?

Geprueft wird ausschliesslich die Verrechnungslogik von `platform_invoices`
gegen eine minimal aufgebaute Datenbank -- dieselben Tabellen, dieselbe
Verbindung, kein Bootstrap aus `bookkeeping_full`.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from app.services import platform_invoices as pi

DDL = """
CREATE TABLE documents (
    id TEXT PRIMARY KEY, original_filename TEXT, stored_filename TEXT,
    file_path TEXT, mime_type TEXT, uploaded_at TEXT, notes TEXT);
CREATE TABLE transactions (
    id TEXT PRIMARY KEY, date TEXT, type TEXT, direction TEXT,
    amount_gross INTEGER, currency TEXT, vat_rate INTEGER, vat_amount INTEGER,
    amount_net INTEGER, is_vat_deductible INTEGER, provider TEXT,
    counterparty_name TEXT, category TEXT, reference TEXT, notes TEXT,
    order_id TEXT, document_id TEXT, template_id TEXT, payment_account_id TEXT,
    period_key TEXT, source TEXT, source_key TEXT UNIQUE, status TEXT,
    booking_class TEXT, created_at TEXT, updated_at TEXT);
CREATE TABLE monthly_invoices (
    id TEXT PRIMARY KEY, provider TEXT, period_from TEXT, period_to TEXT,
    invoice_amount_cents INTEGER, vat_amount_cents INTEGER, currency TEXT,
    calculated_sum_cents INTEGER, difference_cents INTEGER, document_id TEXT,
    notes TEXT, status TEXT, created_at TEXT, updated_at TEXT,
    invoice_number TEXT, invoice_date TEXT, doc_kind TEXT, doc_category TEXT,
    original_invoice_number TEXT, lines_json TEXT, parse_confidence REAL,
    needs_review_reasons TEXT, fx_rate TEXT, vat_cents_eur INTEGER);
CREATE TABLE monthly_invoice_transactions (
    invoice_id TEXT, transaction_id TEXT,
    PRIMARY KEY (invoice_id, transaction_id));
"""

SAMMEL = """\
Kaufland Marketplace GmbH
                                                                          Neckarsulm, 01.07.2026
Rechnungs Nr.: TEST-R0001-00000001
Datum                    Artikelbezeichnung                           Netto (EUR)   MwSt. (EUR)    MwSt.      Brutto (EUR)
01.06.2026               Provision zu Bestell-Nr.                           11,56          2,20      19%              13,76
                         TEST-A/1 "A"
06.06.2026               Monatliche Grundgebühr Basic                       39,95          7,59      19%               47,54
Summe 19%                                                            51,51          9,79                    61,30
"""

SAMMEL_ZWEI = SAMMEL.replace("TEST-R0001-00000001", "TEST-R0001-00000002")

EINZEL = """\
Kaufland Marketplace GmbH
                                                                                 Neckarsulm, 01.07.2026
Abrechnungsbeleg Nr.: TEST-C0001-00000001
Datum                     Artikelbezeichnung                                               Zahlbetrag (EUR)
03.06.2026                Fees for cancelled orders May 26                                              57,23
Summe                                                                                                   57,23
Es handelt sich um einen nicht steuerbaren Schadensersatz.
"""


@pytest.fixture()
def db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    path = tmp_path / "bookkeeping.sqlite3"
    connection = sqlite3.connect(path)
    connection.executescript(DDL)
    connection.commit()
    connection.close()
    monkeypatch.setattr(pi, "BOOKKEEPING_DB_PATH", path)
    yield path


def _seed(
    db_path: Path,
    *,
    key: str,
    cents: int,
    category: str = "fees",
    provider: str = "kaufland",
    day: str = "2026-06-05",
) -> str:
    """Vorhandene orderbezogene Provision -- darf nie angefasst werden."""
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            INSERT OR IGNORE INTO transactions (
                id, date, type, direction, amount_gross, currency,
                amount_net, is_vat_deductible, provider, category,
                source, source_key, status, booking_class, created_at, updated_at
            ) VALUES (?, ?, 'FEE', 'OUT', ?, 'EUR', ?, 1, ?, ?,
                      'api', ?, 'confirmed', 'automatic', ?, ?)
            """,
            (key, f"{day}T00:00:00Z", cents, cents, provider, category, key, day, day),
        )
    return key


def _rows(db_path: Path, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    try:
        return connection.execute(sql, params).fetchall()
    finally:
        connection.close()


def _total(db_path: Path, sql: str, params: tuple = ()) -> int:
    rows = _rows(db_path, sql, params)
    return int(rows[0][0] or 0) if rows else 0


def _anlegen(text: str):
    return pi.create_platform_invoice(parsed=pi.parse_uploaded_document(pdf_text=text))


def _alle_buchungen(db_path: Path) -> list[sqlite3.Row]:
    return _rows(
        db_path,
        "SELECT id, category, amount_gross, amount_net, vat_amount, is_vat_deductible, "
        "provider, reference, source_key FROM transactions ORDER BY created_at, id",
    )


class TestKeineDoppelbuchung:
    def test_doppelte_rechnungsnummer_wird_abgelehnt(self, db: Path) -> None:
        _anlegen(SAMMEL)
        with pytest.raises(Exception) as exc:
            _anlegen(SAMMEL)
        assert _total(db, "SELECT COUNT(*) FROM monthly_invoices") == 1
        assert _total(db, "SELECT COUNT(*) FROM transactions") == 0

    def test_doppelte_freigabe_bucht_nur_einmal(self, db: Path) -> None:
        _seed(db, key="prov-1", cents=1376)
        invoice = _anlegen(SAMMEL)
        pi.approve_platform_invoice(invoice["id"])
        pi.approve_platform_invoice(invoice["id"])
        pi.approve_platform_invoice(invoice["id"])
        # Seed + genau EINE Grundgebuehr. Keine dritte Zeile.
        assert _total(db, "SELECT COUNT(*) FROM transactions") == 2
        assert _total(db, "SELECT COUNT(*) FROM transactions WHERE category = 'base_fee'") == 1

    def test_korrekturbuchung_vervielfacht_sich_nicht(self, db: Path) -> None:
        """Der klassische Doppelfehler: Korrektur wird bei erneuter Freigabe wiederholt."""
        _seed(db, key="prov-1", cents=1000)  # 10,00 statt 61,30 -> Differenz 51,30
        invoice = _anlegen(SAMMEL)
        pi.approve_platform_invoice(invoice["id"])
        pi.approve_platform_invoice(invoice["id"])
        varianten = _rows(db, "SELECT amount_gross FROM transactions WHERE category = 'invoice_variance'")
        assert len(varianten) == 1, f"Korrekturbuchung mehrfach angelegt: {varianten}"
        # und sie ist genau einmal in der Summe enthalten
        assert _total(db, "SELECT COALESCE(SUM(CASE direction WHEN 'OUT' THEN amount_gross ELSE -amount_gross END), 0) FROM transactions") == 6130

    def test_zwei_rechnungen_im_selben_zeitraum_buchen_provision_nicht_doppelt(self, db: Path) -> None:
        """R- und C-Beleg teilen den Monat -- die Provision darf trotzdem nur einmal gezaehlt werden."""
        _seed(db, key="prov-1", cents=1376)
        _seed(db, key="prov-2", cents=857)
        sammel = _anlegen(SAMMEL)
        einzel = _anlegen(EINZEL)
        pi.approve_platform_invoice(sammel["id"])
        pi.approve_platform_invoice(einzel["id"])
        # 2 Seeds + Grundgebuehr + cancellation_fee + hoechstens EINE Korrektur
        # (fuer die Sammelrechnung, deren Rechnungsbetrag gilt).
        assert _total(db, "SELECT COUNT(*) FROM transactions") == 5
        varianten = _rows(db, "SELECT reference FROM transactions WHERE category = 'invoice_variance'")
        assert len(varianten) == 1, f"mehrere Korrekturen: {varianten}"
        # Die zweite Rechnung (Einzelbeleg) darf nichts Fremdes an sich ziehen.
        assert _rows(db, "SELECT calculated_sum_cents, difference_cents FROM monthly_invoices WHERE doc_kind = 'single'")[0]["difference_cents"] == 0
        assert _total(db, "SELECT COUNT(*) FROM transactions WHERE category IN ('fees','provision')") == 2


class TestRichtigeZuordnung:
    def test_provision_bleibt_beim_richtigen_provider(self, db: Path) -> None:
        _seed(db, key="kfl-1", cents=1376, provider="kaufland")
        _seed(db, key="amz-1", cents=500, provider="amazon")
        invoice = _anlegen(SAMMEL)  # provider kaufland
        preview = pi.preview_reconciliation(
            provider="kaufland",
            period_from="2026-06-01",
            period_to="2026-06-30",
            invoice_amount_cents=6130,
        )
        assert preview["expected_cents"] == 1376, "Amazon-Buchung ist in die Kaufland-Summe gelaufen"

    def test_provision_bleibt_im_richtigen_zeitraum(self, db: Path) -> None:
        _seed(db, key="juni", cents=1376, day="2026-06-05")
        _seed(db, key="juli", cents=857, day="2026-07-05")
        preview = pi.preview_reconciliation(
            provider="kaufland",
            period_from="2026-06-01",
            period_to="2026-06-30",
            invoice_amount_cents=6130,
        )
        assert preview["expected_cents"] == 1376, "Juli-Buchung ist in die Juni-Summe gelaufen"

    def test_nur_gebuehrenkategorien_zaehlen(self, db: Path) -> None:
        """Wareneinkauf oder Sonstiges duerfen den Abgleich nicht verschieben."""
        _seed(db, key="prov-1", cents=1376, category="fees")
        _seed(db, key="waren-1", cents=9999, category="cogs")
        _seed(db, key="eigen-1", cents=4444, category="sonstiges")
        preview = pi.preview_reconciliation(
            provider="kaufland",
            period_from="2026-06-01",
            period_to="2026-06-30",
            invoice_amount_cents=6130,
        )
        assert preview["expected_cents"] == 1376

    def test_einzahlungen_zaehlen_nicht(self, db: Path) -> None:
        _seed(db, key="prov-1", cents=1376)
        with sqlite3.connect(db) as connection:
            connection.execute(
                """
                INSERT INTO transactions (
                    id, date, type, direction, amount_gross, currency,
                    amount_net, is_vat_deductible, provider, category,
                    source, source_key, status, booking_class, created_at, updated_at
                ) VALUES ('sale-1', '2026-06-05T00:00:00Z', 'SALE', 'IN', 9999, 'EUR',
                          9999, 0, 'kaufland', 'sale', 'api', 'sale-1', 'confirmed',
                          'automatic', '2026-06-05', '2026-06-05')
                """
            )
        preview = pi.preview_reconciliation(
            provider="kaufland",
            period_from="2026-06-01",
            period_to="2026-06-30",
            invoice_amount_cents=6130,
        )
        assert preview["expected_cents"] == 1376


class TestExakteVerrechnung:
    def _abgleich(self, db_path: Path, invoice_id: str) -> tuple[int, int]:
        row = _rows(
            db_path,
            "SELECT calculated_sum_cents, difference_cents FROM monthly_invoices WHERE id = ?",
            (invoice_id,),
        )[0]
        return int(row["calculated_sum_cents"]), int(row["difference_cents"])

    def test_invariante_nach_freigabe_stimmt_die_summe_exakt(self, db: Path) -> None:
        """Kernversprechen: gebuchte Summe == Rechnungsbetrag, ohne Rest."""
        for seed_cents in (0, 1000, 2233, 5151, 6130, 7000):
            pfad = db.parent / f"{seed_cents}.sqlite3"
            pfad.write_bytes(b"")
            connection = sqlite3.connect(pfad)
            connection.executescript(DDL)
            connection.commit()
            connection.close()
            with pytest.MonkeyPatch.context() as mp:
                mp.setattr(pi, "BOOKKEEPING_DB_PATH", pfad)
                if seed_cents:
                    _seed(pfad, key=f"p{seed_cents}", cents=seed_cents)
                invoice = _anlegen(SAMMEL)
                result = pi.approve_platform_invoice(invoice["id"])
                gebucht = _total(pfad, "SELECT COALESCE(SUM(CASE direction WHEN 'OUT' THEN amount_gross ELSE -amount_gross END), 0) FROM transactions")
                assert gebucht == 6130, f"seed={seed_cents}: gebucht {gebucht} statt 6130"
                assert result["difference_cents"] == 0, f"seed={seed_cents}: Rest {result['difference_cents']}"

    def test_korrektur_ist_exakt_die_differenz(self, db: Path) -> None:
        _seed(db, key="prov-1", cents=1000)
        invoice = _anlegen(SAMMEL)
        pi.approve_platform_invoice(invoice["id"])
        varianten = _rows(db, "SELECT amount_gross, amount_net, vat_amount, is_vat_deductible, reference "
                              "FROM transactions WHERE category = 'invoice_variance'")
        assert len(varianten) == 1
        # Rechnung 61,30 - gebucht 10,00 - Grundgebuehr 47,54 = 3,76
        assert varianten[0]["amount_gross"] == 376
        assert varianten[0]["vat_amount"] == 0
        assert varianten[0]["is_vat_deductible"] == 0
        assert "Abweichung Sammelrechnung" in varianten[0]["reference"]

    def test_bestehende_buchungen_bleiben_unveraendert(self, db: Path) -> None:
        """Haerte Regel: Korrektur ergaenzt, sie veraendert nichts."""
        seed_id = _seed(db, key="prov-1", cents=1000)
        invoice = _anlegen(SAMMEL)
        pi.approve_platform_invoice(invoice["id"])
        seed = _rows(db, "SELECT amount_gross, amount_net, category, source_key FROM transactions WHERE id = ?", (seed_id,))[0]
        assert seed["amount_gross"] == 1000
        assert seed["amount_net"] == 1000
        assert seed["category"] == "fees"
        assert seed["source_key"] == "prov-1"

    def test_vorsteuer_der_grundgebuehr_stimmt_centgenau(self, db: Path) -> None:
        _seed(db, key="prov-1", cents=1376)
        invoice = _anlegen(SAMMEL)
        pi.approve_platform_invoice(invoice["id"])
        row = _rows(db, "SELECT amount_gross, amount_net, vat_amount, vat_rate, is_vat_deductible "
                        "FROM transactions WHERE category = 'base_fee'")[0]
        assert (row["amount_gross"], row["amount_net"], row["vat_amount"]) == (4754, 3995, 759)
        assert row["amount_net"] + row["vat_amount"] == row["amount_gross"]
        assert row["vat_rate"] == 19
        assert row["is_vat_deductible"] == 1

    def test_nicht_steuerbarer_beleg_hat_keine_vorsteuer(self, db: Path) -> None:
        invoice = _anlegen(EINZEL)
        pi.approve_platform_invoice(invoice["id"])
        row = _rows(db, "SELECT amount_gross, amount_net, vat_amount, is_vat_deductible, type "
                        "FROM transactions WHERE category = 'cancellation_fee'")[0]
        assert (row["amount_gross"], row["amount_net"], row["vat_amount"]) == (5723, 5723, 0)
        assert row["is_vat_deductible"] == 0
        assert row["type"] == "EXPENSE"

    def test_verrechnung_und_abweichung_gemeinsam(self, db: Path) -> None:
        """Gemischter Fall: Provision passt, Grundgebuehr fehlt als Buchung."""
        _seed(db, key="prov-1", cents=1376)
        _seed(db, key="prov-2", cents=857)
        invoice = _anlegen(SAMMEL)  # Rechnung 61,30, Provision 22,33 -> es fehlt 38,97
        result = pi.approve_platform_invoice(invoice["id"])
        gebucht = _total(db, "SELECT COALESCE(SUM(CASE direction WHEN 'OUT' THEN amount_gross ELSE -amount_gross END), 0) FROM transactions")
        assert gebucht == 6130
        assert result["difference_cents"] == 0
        assert result["had_variance"] is True
        # 2 Seeds + Grundgebuehr + Korrektur = 4 Zeilen, keine mehr
        assert _total(db, "SELECT COUNT(*) FROM transactions") == 4
