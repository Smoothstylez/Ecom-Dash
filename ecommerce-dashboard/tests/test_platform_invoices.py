"""Tests fuer Eingangsrechnungen: Ablage, Abgleich, Freigabe.

Prueft die drei Zusagen des Designs:
1. Die Verkaufsprovision bleibt unangetastet (keine Doppelbuchung).
2. Nur Grundgebühr/Ads/Einzelbelege werden neu gebucht.
3. Eine Abweichung wird als eigene, markierte Korrektur ausgeglichen.

Die DB wird im Test minimal aufgebaut -- es geht um die Logik von
`platform_invoices`, nicht um das Bootstrapping von `bookkeeping_full`.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from app.services import platform_invoices as pi
from app.services.invoice_parser import parse_invoice_text

DDL = """
CREATE TABLE orders(id TEXT PRIMARY KEY, provider TEXT, external_order_id TEXT);
CREATE TABLE documents (
    id TEXT PRIMARY KEY, original_filename TEXT, stored_filename TEXT,
    file_path TEXT, mime_type TEXT, uploaded_at TEXT, notes TEXT);
CREATE TABLE transactions (
    id TEXT PRIMARY KEY, date TEXT, type TEXT, direction TEXT,
    amount_gross INTEGER CHECK (amount_gross > 0), currency TEXT, vat_rate INTEGER, vat_amount INTEGER,
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
    PRIMARY KEY (invoice_id, transaction_id),
    FOREIGN KEY (invoice_id) REFERENCES monthly_invoices(id),
    FOREIGN KEY (transaction_id) REFERENCES transactions(id));
"""

RECHNUNG = """\
Kaufland Marketplace GmbH
                                                                          Neckarsulm, 01.07.2026
Rechnungs Nr.: TEST-R0001-00000001
Ihre Kunden-Nr: 1234567890 / Ihre USt.-ID Nr.: TEST-UST-00000001
Datum                    Artikelbezeichnung                           Netto (EUR)   MwSt. (EUR)    MwSt.      Brutto (EUR)
01.06.2026               Provision zu Bestell-Nr.                           11,56          2,20      19%              13,76
                         TEST-A/123456789012345 "Beispiel"
01.06.2026               Provision zu Bestell-Nr.                            7,20          1,37      19%               8,57
                         TEST-B/234567890123456 "Beispiel B"
06.06.2026               Monatliche Grundgebühr Basic                       39,95          7,59      19%               47,54
Summe 19%                                                            58,71         11,16                    69,87
Summe                                                                58,71         11,16                    69,87
Der Brutto-Rechnungsbetrag wurde mit dem Saldo Ihres Kreditorenkontos verrechnet.
"""

EINZELBELEG = """\
Kaufland Marketplace GmbH
                                                                                 Neckarsulm, 01.07.2026
Abrechnungsbeleg Nr.: TEST-C0001-00000001
Ihre Kunden-Nr: 1234567890 / Ihre USt.-ID Nr.: TEST-UST-00000001
Datum                     Artikelbezeichnung                                               Zahlbetrag (EUR)
03.06.2026                Fees for cancelled orders May 26                                              57,23
Summe                                                                                                   57,23
Es handelt sich um einen nicht steuerbaren Schadensersatz.
Die Summe wurde mit dem Saldo ihres Kreditorenkontos verrechnet.
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


def _seed_provision(db_path: Path, *, cents: int, category: str = "fees") -> str:
    """Vorhandene orderbezogene Provision -- darf nie angefasst werden."""
    with sqlite3.connect(db_path) as connection:
        connection.row_factory = sqlite3.Row
        transaction_id = f"seed-{category}-{cents}"
        connection.execute(
            """
            INSERT INTO transactions (
                id, date, type, direction, amount_gross, currency,
                amount_net, is_vat_deductible, provider, category,
                source, source_key, status, booking_class, created_at, updated_at
            ) VALUES (?, ?, 'FEE', 'OUT', ?, 'EUR', ?, 1, 'kaufland', ?,
                      'api', ?, 'confirmed', 'automatic', '2026-06-01', '2026-06-01')
            """,
            (transaction_id, "2026-06-05T00:00:00Z", cents, cents, category, transaction_id),
        )
    return transaction_id


def _count(db_path: Path, sql: str, params: tuple = ()) -> int:
    with sqlite3.connect(db_path) as connection:
        return int(connection.execute(sql, params).fetchone()[0])


def test_two_amazon_invoices_in_same_period_do_not_offset_each_other(db: Path) -> None:
    invoices = []
    for number, net, vat, gross in [("FBA-1", 10000, 1900, 11900), ("FBA-2", 20000, 3800, 23800)]:
        invoices.append(pi.create_platform_invoice(parsed={
            "provider": "amazon", "invoice_number": number, "invoice_date": "2026-07-31",
            "period_from": "2026-07-01", "period_to": "2026-07-31", "doc_kind": "consolidated",
            "doc_category": "fee", "gross_cents": gross, "net_cents": net, "vat_cents": vat,
            "currency": "EUR", "parse_confidence": 1.0, "needs_review_reasons": [],
            "lines": [{"position_key": "fulfillment", "label": "MCF FBA Pick & Pack Fee",
                       "net_cents": net, "vat_cents": vat, "gross_cents": gross, "vat_rate": 19}],
        }))
    for invoice in invoices:
        approved = pi.approve_platform_invoice(invoice["id"])
        assert approved["had_variance"] is False
    assert _count(db, "SELECT SUM(amount_gross) FROM transactions") == 35700
    assert _count(db, "SELECT COUNT(*) FROM transactions WHERE category='invoice_variance'") == 0
    pi.approve_platform_invoice(invoices[0]["id"])
    assert _count(db, "SELECT SUM(amount_gross) FROM transactions") == 35700


def test_original_amazon_fee_credit_is_stored_and_booked_with_negative_sign(db: Path) -> None:
    from test_invoice_parser import AMAZON_GUTSCHRIFT

    csv = 'Transaction Date,Transaction ID,Order ID,Fees Invoice Number,Marketplace,Fee ID,Total Fees (VAT-Inclusive)\n08/20/2026,R1,O1,TEST-CN-00000001,Amazon.de,Referral Fee,-98.07\n'
    parsed = pi.parse_uploaded_document(pdf_text=AMAZON_GUTSCHRIFT, csv_text=csv)
    assert parsed["parse_confidence"] == 1.0
    credit = pi.create_platform_invoice(parsed=parsed)
    assert credit["invoice_amount_cents"] == -9807
    assert credit["vat_amount_cents"] == -1566
    approved = pi.approve_platform_invoice(credit["id"])
    assert approved["status"] == "approved"
    assert _count(db, "SELECT SUM(CASE direction WHEN 'OUT' THEN amount_gross ELSE -amount_gross END) FROM transactions") == -9807
    assert _count(db, "SELECT SUM(CASE direction WHEN 'OUT' THEN vat_amount ELSE -vat_amount END) FROM transactions") == -1566
    with sqlite3.connect(db) as c:
        assert c.execute("SELECT direction,amount_gross,vat_amount FROM transactions").fetchone() == ("IN", 9807, 1566)
    pi.approve_platform_invoice(credit["id"])
    assert _count(db, "SELECT COUNT(*) FROM transactions") == 1
    assert _count(db, "SELECT COUNT(*) FROM transactions WHERE category='invoice_variance'") == 0


def commission_invoice(number, gross, *, provider="kaufland", refs=None):
    return {"provider": provider, "invoice_number": number, "invoice_date": "2026-08-31",
            "period_from": "2026-08-01", "period_to": "2026-08-31", "doc_kind": "consolidated",
            "doc_category": "fee", "currency": "EUR", "gross_cents": gross, "vat_cents": 0,
            "lines": [{"position_key": "provision", "gross_cents": gross, "net_cents": gross,
                       "vat_cents": 0, "order_ref": refs}]}


def seed_commission(path, ident, gross, direction="OUT", order_id=None, provider="kaufland"):
    with sqlite3.connect(path) as c:
        c.execute("INSERT INTO transactions(id,date,type,direction,amount_gross,currency,provider,category,source_key,order_id) VALUES (?,'2026-08-10','FEE',?,?,'EUR',?,'fees',?,?)",
                  (ident, direction, gross, provider, ident, order_id))


def test_existing_fee_reversal_is_linked_without_duplicate_variance(db):
    seed_commission(db, "charge", 23800)
    seed_commission(db, "reversal", 11900, "IN")
    parsed = commission_invoice("MIXED", 11900)
    parsed["lines"] = [{"position_key": "provision", "gross_cents": 23800},
                       {"position_key": "provision_storno", "gross_cents": -11900}]
    inv = pi.create_platform_invoice(parsed=parsed)
    approved = pi.approve_platform_invoice(inv["id"])
    assert approved["had_variance"] is False
    assert _count(db, "SELECT COUNT(*) FROM transactions") == 2


def test_amazon_external_order_reference_resolves_internal_bookkeeping_id(db):
    with sqlite3.connect(db) as c:
        c.execute("INSERT INTO orders VALUES ('internal-uuid','amazon','123-1234567-1234567')")
    seed_commission(db, "fee", 1190, order_id="internal-uuid", provider="amazon")
    inv = pi.create_platform_invoice(parsed=commission_invoice("EXTERNAL", 1190, provider="amazon", refs="123-1234567-1234567"))
    assert pi.approve_platform_invoice(inv["id"])["had_variance"] is False
    assert _count(db, "SELECT COUNT(*) FROM transactions") == 1


def test_late_automatic_fee_reapproval_appends_an_exact_idempotent_delta(db):
    inv = pi.create_platform_invoice(parsed=commission_invoice("LATE", 1190))
    pi.approve_platform_invoice(inv["id"])
    seed_commission(db, "late-fee", 500)
    pi.approve_platform_invoice(inv["id"])
    assert _count(db, "SELECT SUM(CASE direction WHEN 'OUT' THEN amount_gross ELSE -amount_gross END) FROM transactions") == 1190
    count = _count(db, "SELECT COUNT(*) FROM transactions")
    pi.approve_platform_invoice(inv["id"])
    assert _count(db, "SELECT COUNT(*) FROM transactions") == count


def test_zero_net_category_bucket_is_not_inserted(db):
    parsed = commission_invoice("ZERO-BUCKET", 11900, provider="amazon")
    parsed["lines"] = [{"position_key": "subscription", "gross_cents": 11900, "net_cents": 10000, "vat_cents": 1900},
                       {"position_key": "fulfillment", "gross_cents": 11900, "net_cents": 10000, "vat_cents": 1900},
                       {"position_key": "fulfillment", "gross_cents": -11900, "net_cents": -10000, "vat_cents": -1900}]
    inv = pi.create_platform_invoice(parsed=parsed)
    pi.approve_platform_invoice(inv["id"])
    assert _count(db, "SELECT COUNT(*) FROM transactions") == 1


class TestPrefillOhneBuchung:
    def test_parse_legt_nichts_an(self, db: Path) -> None:
        pi.parse_uploaded_document(pdf_text=RECHNUNG)
        assert _count(db, "SELECT COUNT(*) FROM monthly_invoices") == 0
        assert _count(db, "SELECT COUNT(*) FROM transactions") == 0

    def test_matching_fee_csv_replaces_stale_pdf_position_warning(self, db: Path) -> None:
        from test_invoice_parser import AMAZON_GEBUEHR

        text = AMAZON_GEBUEHR.replace("Gebuehren im Zusammenhang mit", "Unbekannte Abrechnungsposition").replace('"Versand durch Amazon"', '"Unbekannte Leistung"')
        pdf_only = pi.parse_uploaded_document(pdf_text=text)
        assert any(r.startswith("unbekannte_position") for r in pdf_only["needs_review_reasons"])
        csv = 'Transaction Date,Transaction ID,Order ID,Fees Invoice Number,Marketplace,Fee ID,Total Fees (VAT-Inclusive)\n2026-08-15,T1,O1,TEST-AEU-00000001,Amazon.de,MCF FBA Pick & Pack Fee,163.58\n'
        combined = pi.parse_uploaded_document(pdf_text=text, csv_text=csv)
        assert combined["parse_confidence"] == 1.0
        assert combined["needs_review_reasons"] == []
        assert (combined["net_cents"], combined["vat_cents"], combined["gross_cents"]) == (13746, 2612, 16358)
        assert _count(db, "SELECT COUNT(*) FROM transactions") == 0

    def test_mismatching_fee_csv_keeps_a_real_checksum_warning(self, db: Path) -> None:
        from test_invoice_parser import AMAZON_GEBUEHR

        csv = 'Transaction Date,Transaction ID,Order ID,Fees Invoice Number,Marketplace,Fee ID,Total Fees (VAT-Inclusive)\n2026-08-15,T1,O1,TEST-AEU-00000001,Amazon.de,MCF FBA Pick & Pack Fee,164.58\n'
        result = pi.parse_uploaded_document(pdf_text=AMAZON_GEBUEHR, csv_text=csv)
        assert "kontrollsumme_brutto_verfehlt" in result["needs_review_reasons"]
        assert result["parse_confidence"] < 1.0

    def test_fee_csv_inherits_native_invoice_currency(self, db: Path) -> None:
        from test_invoice_parser import AMAZON_ABO

        csv = 'Transaction Date,Transaction ID,Order ID,Fees Invoice Number,Marketplace,Fee ID,Total Fees (VAT-Inclusive)\n06/30/2026,T1,,TEST-AEU-00000002,Amazon.co.uk,Subscription Fee,29.75\n'
        result = pi.parse_uploaded_document(pdf_text=AMAZON_ABO, csv_text=csv)
        assert result["currency"] == "GBP"
        assert result["lines"][0]["currency"] == "GBP"
        assert result["vat_cents_eur"] == 551

    def test_vorbefuellung_und_vorschau(self, db: Path) -> None:
        _seed_provision(db, cents=2233)
        payload = pi.parse_uploaded_document(pdf_text=RECHNUNG)
        assert payload["invoice_number"] == "TEST-R0001-00000001"
        assert payload["gross_cents"] == 6987
        assert payload["preview"]["expected_cents"] == 2233
        assert payload["preview"]["difference_cents"] == 6987 - 2233

    def test_umsatzbeleg_wird_gekennzeichnet(self) -> None:
        payload = pi.parse_uploaded_document(
            pdf_text="GUTSCHRIFT\nRechnungsnummer: TEST-RT-0001\n"
            "Bestellnummer  Lieferdatum  Menge\n"
            "GESAMT: EUR 148,90\nEUR 125,13 19,00 EUR 23,77 EUR 23,77\n"
        )
        assert payload["doc_kind"] == "sales"


class TestAnlegenOhneBuchung:
    def test_anlegen_bucht_nichts(self, db: Path) -> None:
        payload = pi.parse_uploaded_document(pdf_text=RECHNUNG)
        invoice = pi.create_platform_invoice(parsed=payload)
        assert invoice["status"] == "needs_review"
        assert invoice["invoice_number"] == "TEST-R0001-00000001"
        assert invoice["doc_kind"] == "consolidated"
        assert _count(db, "SELECT COUNT(*) FROM transactions") == 0

    def test_einzelbeleg_landet_als_eigene_rechnung(self, db: Path) -> None:
        payload = pi.parse_uploaded_document(pdf_text=EINZELBELEG)
        invoice = pi.create_platform_invoice(parsed=payload)
        assert invoice["doc_kind"] == "single"
        assert invoice["doc_category"] == "cancellation_fee"
        assert _count(db, "SELECT COUNT(*) FROM monthly_invoices") == 1

    def test_einzelbeleg_kollidiert_nicht_mit_sammelrechnung(self, db: Path) -> None:
        """Beide teilen den Monat -- die Ueberlappungspruefung gilt nur fuer Monatsrechnungen."""
        pi.create_platform_invoice(parsed=pi.parse_uploaded_document(pdf_text=RECHNUNG))
        zweite = pi.create_platform_invoice(
            parsed=pi.parse_uploaded_document(pdf_text=EINZELBELEG)
        )
        assert zweite["doc_kind"] == "single"
        assert _count(db, "SELECT COUNT(*) FROM monthly_invoices") == 2


class TestFreigabe:
    def test_provision_bleibt_unangetastet(self, db: Path) -> None:
        seed_id = _seed_provision(db, cents=2233)
        invoice = pi.create_platform_invoice(
            parsed=pi.parse_uploaded_document(pdf_text=RECHNUNG)
        )
        pi.approve_platform_invoice(invoice["id"])

        # genau EINE Provision -- der Seed, keine zweite aus der Rechnung
        assert (
            _count(
                db,
                "SELECT COUNT(*) FROM transactions WHERE category IN ('fees','provision')",
            )
            == 1
        )
        with sqlite3.connect(db) as connection:
            connection.row_factory = sqlite3.Row
            row = connection.execute(
                "SELECT amount_gross FROM transactions WHERE id = ?", (seed_id,)
            ).fetchone()
        assert row["amount_gross"] == 2233

    def test_grundgebuehr_wird_gebucht(self, db: Path) -> None:
        _seed_provision(db, cents=2233)
        invoice = pi.create_platform_invoice(
            parsed=pi.parse_uploaded_document(pdf_text=RECHNUNG)
        )
        pi.approve_platform_invoice(invoice["id"])
        with sqlite3.connect(db) as connection:
            connection.row_factory = sqlite3.Row
            row = connection.execute(
                "SELECT * FROM transactions WHERE category = 'base_fee'"
            ).fetchone()
        assert row is not None
        assert row["type"] == "SUBSCRIPTION"
        assert row["amount_gross"] == 4754
        assert row["amount_net"] == 3995
        assert row["vat_amount"] == 759
        assert row["is_vat_deductible"] == 1

    def test_abweichung_wird_als_korrektur_gebucht(self, db: Path) -> None:
        # nur 22,33 statt 69,87 gebucht -> Abweichung 47,54 (die Grundgebühr)
        _seed_provision(db, cents=2233)
        invoice = pi.create_platform_invoice(
            parsed=pi.parse_uploaded_document(pdf_text=RECHNUNG)
        )
        result = pi.approve_platform_invoice(invoice["id"])

        with sqlite3.connect(db) as connection:
            connection.row_factory = sqlite3.Row
            variance = connection.execute(
                "SELECT * FROM transactions WHERE category = 'invoice_variance'"
            ).fetchall()
        # Die Grundgebühr wird gebucht, dadurch stimmt die Summe -- keine Korrektur noetig.
        assert variance == []
        assert result["had_variance"] is False
        assert result["difference_cents"] == 0

    def test_reine_abweichung_erzeugt_korrektur(self, db: Path) -> None:
        # gar nichts gebucht, aber eine erwartete Provision existiert nicht --
        # die Grundgebühr deckt sich nicht mit der Rechnung, weil die beiden
        # Provisionen fehlen. Die Korrektur greift.
        invoice = pi.create_platform_invoice(
            parsed=pi.parse_uploaded_document(pdf_text=RECHNUNG)
        )
        result = pi.approve_platform_invoice(invoice["id"])
        with sqlite3.connect(db) as connection:
            connection.row_factory = sqlite3.Row
            variance = connection.execute(
                "SELECT * FROM transactions WHERE category = 'invoice_variance'"
            ).fetchall()
            booked = connection.execute(
                "SELECT COALESCE(SUM(amount_gross), 0) AS t FROM transactions"
            ).fetchone()
        assert result["had_variance"] is True
        assert len(variance) == 1
        assert variance[0]["type"] == "ADJUSTMENT"
        assert "Abweichung Sammelrechnung" in variance[0]["reference"]
        assert variance[0]["is_vat_deductible"] == 0
        # nach der Korrektur: gebuchte Summe == Rechnungsbetrag
        assert booked["t"] == 6987
        assert result["difference_cents"] == 0

    def test_einzelbeleg_wird_ohne_vorsteuer_gebucht(self, db: Path) -> None:
        invoice = pi.create_platform_invoice(
            parsed=pi.parse_uploaded_document(pdf_text=EINZELBELEG)
        )
        pi.approve_platform_invoice(invoice["id"])
        with sqlite3.connect(db) as connection:
            connection.row_factory = sqlite3.Row
            row = connection.execute(
                "SELECT * FROM transactions WHERE category = 'cancellation_fee'"
            ).fetchone()
        assert row is not None
        assert row["type"] == "EXPENSE"
        assert row["amount_gross"] == 5723
        assert row["vat_amount"] == 0
        assert row["is_vat_deductible"] == 0

    def test_erneutes_freigeben_bucht_nicht_doppelt(self, db: Path) -> None:
        _seed_provision(db, cents=2233)
        invoice = pi.create_platform_invoice(
            parsed=pi.parse_uploaded_document(pdf_text=RECHNUNG)
        )
        pi.approve_platform_invoice(invoice["id"])
        pi.approve_platform_invoice(invoice["id"])
        assert _count(db, "SELECT COUNT(*) FROM transactions") == 2  # Seed + Grundgebuehr

    def test_umsatzbeleg_wird_erfasst_aber_nicht_gebucht(self, db: Path) -> None:
        payload = pi.parse_uploaded_document(
            pdf_text="GUTSCHRIFT\nRechnungsnummer: TEST-RT-0001\n"
            "Rechnungsdatum: 31.07.2026\nBestellnummer  x\n"
            "GESAMT: EUR 148,90\nEUR 125,13 19,00 EUR 23,77 EUR 23,77\n"
        )
        assert payload["doc_kind"] == "sales"
        invoice = pi.create_platform_invoice(parsed=payload)
        assert invoice["doc_kind"] == "sales"
        # erfassen duerfen wir, buchen nicht
        with pytest.raises(Exception):
            pi.approve_platform_invoice(invoice["id"])
        assert _count(db, "SELECT COUNT(*) FROM transactions") == 0
