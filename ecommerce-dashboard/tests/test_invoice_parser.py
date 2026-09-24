"""Tests fuer den Eingangsrechnungs-Parser.

Die Fixtures sind aus den echten Beleglayouts abgeleitet, verwenden aber
ausschliesslich TEST-* Rechnungsnummern -- echte Nummern duerfen nie in
Testdaten auftauchen (siehe Commit `e2c57ce`).
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from app.services.invoice_parser import (
    DOC_KIND_CONSOLIDATED,
    DOC_KIND_SALES,
    DOC_KIND_SINGLE,
    POSITION_ADVERTISING,
    POSITION_BASE_FEE,
    POSITION_CANCELLATION_FEE,
    POSITION_FEE_REFUND,
    POSITION_FULFILLMENT,
    POSITION_PROVISION,
    POSITION_PROVISION_STORNO,
    POSITION_REFUND_ADMIN,
    POSITION_SUBSCRIPTION,
    REVIEW_CHECKSUM_GROSS,
    REVIEW_MISSING_ORIGINAL,
    REVIEW_MISSING_PERIOD,
    REVIEW_NO_TEMPLATE,
    REVIEW_UNKNOWN_POSITION,
    TEMPLATE_AMAZON_ABO,
    TEMPLATE_AMAZON_GEBUEHR,
    TEMPLATE_AMAZON_RETAIL,
    TEMPLATE_AMAZON_STEUERGUTSCHRIFT,
    TEMPLATE_KAUFLAND_EINZEL,
    TEMPLATE_KAUFLAND_SAMMEL,
    TEMPLATE_UNKNOWN,
    classify_position,
    parse_amazon_fee_csv,
    parse_invoice_text,
    split_gross,
    to_cents,
    to_decimal,
)


KAUFLAND_SAMMEL = """\
Kaufland Marketplace GmbH - Stiftsbergstrasse 1 - 74172 Neckarsulm
Luis Test Einzelunternehmen
                                                                          Neckarsulm, 01.07.2026
Rechnungs Nr.: TEST-R0001-00000001
Ihre Kunden-Nr: 1234567890 / Ihre USt.-ID Nr.: TEST-UST-00000001
Datum                    Artikelbezeichnung                           Netto (EUR)   MwSt. (EUR)    MwSt.      Brutto (EUR)
01.06.2026               Storno Provision zu Bestell-Nr.                    -6,85         -1,30      19%              -8,15
                         TEST-A/123456789012345 "Beispiel Artikel A"
01.06.2026               Provision zu Bestell-Nr.                           11,56          2,20      19%              13,76
                         TEST-B/234567890123456 "Beispiel Artikel B"
06.06.2026               Monatliche Grundgebühr Basic                       39,95          7,59      19%               47,54
07.06.2026               Sponsored Product Ads Click Costs -                29,81          5,66      19%               35,47
Zwischensumme                                                         74,47         14,15                    88,62

Summe 19%                                                            74,47         14,15                    88,62

Summe                                                                74,47         14,15                    88,62


Der Brutto-Rechnungsbetrag wurde mit dem Saldo Ihres Kreditorenkontos verrechnet.
"""

KAUFLAND_EINZEL = """\
Kaufland Marketplace GmbH - Stiftsbergstrasse 1 - 74172 Neckarsulm
Luis Test Einzelunternehmen
                                                                                 Neckarsulm, 01.07.2026
Abrechnungsbeleg Nr.: TEST-C0001-00000001
Ihre Kunden-Nr: 1234567890 / Ihre USt.-ID Nr.: TEST-UST-00000001
Datum                     Artikelbezeichnung                                               Zahlbetrag (EUR)
03.06.2026                Fees for cancelled orders May 26                                              57,23
Summe                                                                                                   57,23
Es handelt sich um einen nicht steuerbaren Schadensersatz.
Die Summe wurde mit dem Saldo ihres Kreditorenkontos verrechnet.
"""

AMAZON_GEBUEHR = """\
                                                                                                                            RECHNUNG

Rechnungsdatum: 31/08/2026
Rechnungszeitraum: 01/08/2026 to 31/08/2026
Rechnungsnummer: TEST-AEU-00000001
Leistungsempfaenger:
Test Verkaeufer
UStID des Leistungsempfaengers: TEST-UST-00000001
UStID des Leistungserbringers: TEST-UST-00000002

                                                              Preis
Dienstleistung                                                                     USt.%            USt.             Gesamtsumme
                                                              (Ohne USt.)
Gebuehren im Zusammenhang mit
                                                              EUR 137.46            19.00%           EUR 26.12        EUR 163.58
"Versand durch Amazon"
Gesamtsumme                                                  EUR 137.46                             EUR 26.12        EUR 163.58

Ort der Betriebsstaette der sonstigen Leistung - DE
"""

AMAZON_ABO = """\
                                                                                                                                    INVOICE

Invoice Date: 30/06/2026
Invoice Period: 01/06/2026 to 30/06/2026
Invoice Number: TEST-AEU-00000002
Business Name:
Test Seller
                                                                           Supplier Name:
                                                                           Amazon Example Supplier
Business VAT Number: TEST-UST-00000001
                                                                           Supplier VAT Number: TEST-UST-00000002

                                                     Price
Description                                                                            VAT%            VAT                   Total
                                                     (VAT Exclusive)
Selling on Amazon Fees                              GBP 25.00                          19.00%          GBP 4.75              GBP 29.75
Total                                               GBP 25.00                                          GBP 4.75              GBP 29.75
                                                                                                        EUR 5.51

Exchange Rate: [1.160390 EUR / 1 GBP]

Place of establishment for services - DE
"""

AMAZON_GUTSCHRIFT = """\
                                                                                                            STEUERGUTSCHRIFT

Ausstellungsdatum der Gutschrift: 31/08/2026
Zeitraum der Gutschrift: 01/08/2026 to 31/08/2026
Gutschriftennummer: TEST-CN-00000001
Leistungsempfaenger:
Test Verkaeufer
UStID des Leistungsempfaengers: TEST-UST-00000001

                                                               Preis
Dienstleistung                                                                      USt.%         USt.                 Gesamtsumme
                                                               (Ohne USt.)
Erstattung von Verkaeufergebuehren                          -EUR 82.41            19.00%        -EUR 15.66           -EUR 98.07
Gesamtsumme                                                   -EUR 82.41                          -EUR 15.66           -EUR 98.07

Ursprüngliche Rechnungsnummer
TEST-AEU-00000001
"""

AMAZON_RETAIL = """\
                                                                                                                  GUTSCHRIFT
                                                                                                                             Test Kunde
UST-ID: TEST-UST-00000003

Rechnungsnummer: TEST-RT-00000001                                                                                 Rechnungsdatum: 31.07.2026
Bestellnummer         Lieferdatum   Menge ASIN-Beschreibung                                (ohne USt.)  %   (inkl. Ust.)   (inkl. USt.)
028-0000000-0000000   27.07.2026    1      TEST-ASIN-Beispielartikel                         125,13 19,00         148,90          148,90
                                                                                                                              GESAMT:         EUR 148,90
                                                                    Zwischensumme USt.
                                                                    (ohne USt.)     %
                                                                         EUR 125,13 19,00         EUR 23,77                  EUR 23,77
                                                                                                   GESAMT:                    EUR 23,77
"""

AMAZON_FEE_CSV = """\
"Transaction Date","Transaction ID","Order ID","Fees Invoice Number","Marketplace","Fee ID","Total Fees (VAT-Inclusive)"
"08/14/2026","114672253492302","305-6372875-6975503","TEST-AEU-00000001","Amazon.de","MCF FBA Pick & Pack Fee","3.7800"
"08/15/2026","114693525124302","303-9368026-4908360","TEST-AEU-00000001","Amazon.de","Referral Fee","26.5800"
"08/25/2026","114676110764302","304-7522604-6916366","TEST-AEU-00000001","Amazon.de","Refund Administration Fee","2.8300"
"""


class TestBetraegeUndRundung:
    def test_deutsche_und_englische_zahlen(self) -> None:
        assert to_decimal("1.403,37") == Decimal("1403.37")
        assert to_decimal("137.46") == Decimal("137.46")
        assert to_decimal("-6,85") == Decimal("-6.85")
        assert to_cents("1.403,37") == 140337
        assert to_cents("-EUR 82.41") == -8241

    def test_split_gross_rundet_je_position(self) -> None:
        # 1376 / 1.19 = 11.563...  -> 11.56 netto, 2.20 Vorsteuer
        assert split_gross(1376) == (1156, 220)
        assert split_gross(815) == (685, 130)
        assert split_gross(2319) == (1949, 370)

    def test_split_gross_ohne_satz(self) -> None:
        assert split_gross(5723, 0) == (5723, 0)


class TestKategorien:
    @pytest.mark.parametrize(
        ("label", "erwartet"),
        [
            ("Provision zu Bestell-Nr. TEST/1", POSITION_PROVISION),
            ("Storno Provision zu Bestell-Nr. TEST/1", POSITION_PROVISION_STORNO),
            ("Monatliche Grundgebühr Basic", POSITION_BASE_FEE),
            ("Sponsored Product Ads Click Costs -", POSITION_ADVERTISING),
            ("Selling on Amazon Fees", POSITION_SUBSCRIPTION),
            ("MCF FBA Pick & Pack Fee", POSITION_FULFILLMENT),
            ("FBA Inventory Storage", POSITION_FULFILLMENT),
            ("Inbound Transportation Charge", POSITION_FULFILLMENT),
            ("Refund Administration Fee", POSITION_REFUND_ADMIN),
            ("Erstattung von Verkaeufergebuehren", POSITION_FEE_REFUND),
            ("Fees for cancelled orders May 26", POSITION_CANCELLATION_FEE),
            ("Referral Fee", POSITION_PROVISION),
            ("Völlig unbekannt", "other"),
        ],
    )
    def test_classify(self, label: str, erwartet: str) -> None:
        assert classify_position(label) == erwartet


class TestKauflandSammelrechnung:
    def setup_method(self) -> None:
        self.invoice = parse_invoice_text(KAUFLAND_SAMMEL)

    def test_template_und_kopf(self) -> None:
        assert self.invoice.template == TEMPLATE_KAUFLAND_SAMMEL
        assert self.invoice.doc_kind == DOC_KIND_CONSOLIDATED
        assert self.invoice.provider == "kaufland"
        assert self.invoice.invoice_number == "TEST-R0001-00000001"
        assert self.invoice.invoice_date == "2026-07-01"
        assert self.invoice.currency == "EUR"

    def test_vier_positionen_mit_den_richtigen_kategorien(self) -> None:
        assert [line.position_key for line in self.invoice.lines] == [
            POSITION_PROVISION_STORNO,
            POSITION_PROVISION,
            POSITION_BASE_FEE,
            POSITION_ADVERTISING,
        ]

    def test_betraege_und_kontrollsumme(self) -> None:
        assert self.invoice.net_cents == 7447
        assert self.invoice.vat_cents == 1415
        assert self.invoice.gross_cents == 8862
        assert sum(line.gross_cents for line in self.invoice.lines) == 8862
        assert REVIEW_CHECKSUM_GROSS not in self.invoice.needs_review_reasons

    def test_leistungszeitraum_aus_den_positionsdaten(self) -> None:
        assert self.invoice.period_from == "2026-06-01"
        assert self.invoice.period_to == "2026-06-07"

    def test_storno_ist_negativ(self) -> None:
        assert self.invoice.lines[0].net_cents == -685
        assert self.invoice.lines[0].gross_cents == -815

    def test_confidence_hoch_ohne_warnungen(self) -> None:
        assert self.invoice.parse_confidence >= 0.9
        assert self.invoice.needs_review_reasons == []


class TestKauflandEinzelbeleg:
    def setup_method(self) -> None:
        self.invoice = parse_invoice_text(KAUFLAND_EINZEL)

    def test_erkennung_als_einzelbeleg_ohne_vorsteuer(self) -> None:
        assert self.invoice.template == TEMPLATE_KAUFLAND_EINZEL
        assert self.invoice.doc_kind == DOC_KIND_SINGLE
        assert self.invoice.invoice_number == "TEST-C0001-00000001"
        assert self.invoice.category_hint == "cancellation_fee"
        assert self.invoice.is_vat_deductible is False

    def test_eine_position_ohne_steuer(self) -> None:
        assert len(self.invoice.lines) == 1
        line = self.invoice.lines[0]
        assert line.position_key == POSITION_CANCELLATION_FEE
        assert line.gross_cents == 5723
        assert line.net_cents == 5723
        assert line.vat_cents == 0
        assert line.vat_rate is None

    def test_gesamt_und_periode(self) -> None:
        assert self.invoice.gross_cents == 5723
        assert self.invoice.period_from == "2026-06-03"


class TestAmazonGebuehr:
    """Der Fall, der im ersten Prototyp durchs Raster fiel: umbrechende
    Beschriftung und eine Gesamtsumme ohne Steuersatz."""

    def setup_method(self) -> None:
        self.invoice = parse_invoice_text(AMAZON_GEBUEHR)

    def test_erkennung_und_kopf(self) -> None:
        assert self.invoice.template == TEMPLATE_AMAZON_GEBUEHR
        assert self.invoice.provider == "amazon"
        assert self.invoice.doc_kind == DOC_KIND_CONSOLIDATED
        assert self.invoice.invoice_number == "TEST-AEU-00000001"
        assert self.invoice.invoice_date == "2026-08-31"
        assert self.invoice.period_from == "2026-08-01"
        assert self.invoice.period_to == "2026-08-31"

    def test_gesamtsumme_ohne_steuerwert_wird_gelesen(self) -> None:
        assert self.invoice.net_cents == 13746
        assert self.invoice.vat_cents == 2612
        assert self.invoice.gross_cents == 16358

    def test_position_wird_trotz_umbruch_gelesen(self) -> None:
        assert len(self.invoice.lines) == 1
        line = self.invoice.lines[0]
        assert line.net_cents == 13746
        assert line.vat_rate == 19
        assert line.gross_cents == 16358

    def test_kontrollsumme_passt(self) -> None:
        assert REVIEW_CHECKSUM_GROSS not in self.invoice.needs_review_reasons


class TestAmazonAbo:
    def setup_method(self) -> None:
        self.invoice = parse_invoice_text(AMAZON_ABO)

    def test_erkennung_als_englisches_abo_in_gbp(self) -> None:
        assert self.invoice.template == TEMPLATE_AMAZON_ABO
        assert self.invoice.currency == "GBP"
        assert self.invoice.invoice_number == "TEST-AEU-00000002"
        assert self.invoice.invoice_date == "2026-06-30"
        assert self.invoice.period_from == "2026-06-01"
        assert self.invoice.period_to == "2026-06-30"

    def test_betraege_in_gbp(self) -> None:
        assert self.invoice.net_cents == 2500
        assert self.invoice.vat_cents == 475
        assert self.invoice.gross_cents == 2975
        assert self.invoice.lines[0].position_key == POSITION_SUBSCRIPTION

    def test_fuer_die_ust_va_zaehlt_nur_die_eur_vorsteuer(self) -> None:
        assert self.invoice.fx_rate == "1.160390"
        assert self.invoice.vat_cents_eur == 551


class TestAmazonSteuergutschrift:
    def setup_method(self) -> None:
        self.invoice = parse_invoice_text(AMAZON_GUTSCHRIFT)

    def test_erkennung_mit_originalbezug(self) -> None:
        assert self.invoice.template == TEMPLATE_AMAZON_STEUERGUTSCHRIFT
        assert self.invoice.doc_kind == DOC_KIND_SINGLE
        assert self.invoice.invoice_number == "TEST-CN-00000001"
        assert self.invoice.original_invoice_number == "TEST-AEU-00000001"
        assert REVIEW_MISSING_ORIGINAL not in self.invoice.needs_review_reasons

    def test_negative_erstattung(self) -> None:
        assert self.invoice.net_cents == -8241
        assert self.invoice.vat_cents == -1566
        assert self.invoice.gross_cents == -9807
        assert self.invoice.lines[0].position_key == POSITION_FEE_REFUND
        assert self.invoice.lines[0].gross_cents == -9807


class TestAmazonRetailBeleg:
    def test_als_umsatzbeleg_markiert(self) -> None:
        invoice = parse_invoice_text(AMAZON_RETAIL)
        assert invoice.template == TEMPLATE_AMAZON_RETAIL
        assert invoice.doc_kind == DOC_KIND_SALES
        assert invoice.is_vat_deductible is False
        assert invoice.gross_cents == 14890
        assert invoice.vat_cents == 2377


class TestAmazonCsv:
    def test_zeilen_und_kategorien(self) -> None:
        lines = parse_amazon_fee_csv(AMAZON_FEE_CSV)
        assert [line.position_key for line in lines] == [
            POSITION_FULFILLMENT,
            POSITION_PROVISION,
            POSITION_REFUND_ADMIN,
        ]
        assert [line.gross_cents for line in lines] == [378, 2658, 283]

    def test_brutto_wird_in_netto_und_steuer_zerlegt(self) -> None:
        lines = parse_amazon_fee_csv(AMAZON_FEE_CSV)
        for line in lines:
            assert line.net_cents + line.vat_cents == line.gross_cents
            assert line.vat_rate == 19

    def test_order_referenz_und_datum(self) -> None:
        line = parse_amazon_fee_csv(AMAZON_FEE_CSV)[1]
        assert line.order_ref == "303-9368026-4908360"
        assert line.line_date == "2026-08-15"


class TestFehlerfaelle:
    def test_unbekanntes_template_bleibt_ungebucht(self) -> None:
        invoice = parse_invoice_text("Das ist keine Rechnung, sondern ein Einkaufszettel.")
        assert invoice.template == TEMPLATE_UNKNOWN
        assert invoice.doc_kind == "unknown"
        assert invoice.parse_confidence == 0.0
        assert REVIEW_NO_TEMPLATE in invoice.needs_review_reasons
        assert invoice.lines == []

    def test_kontrollsumme_wird_beanstandet(self) -> None:
        # Eine Position auf 99,99 ziehen, damit sie nicht mehr zur Summe passt.
        kaputt = KAUFLAND_SAMMEL.replace("13,76", "99,99", 1)
        invoice = parse_invoice_text(kaputt)
        assert REVIEW_CHECKSUM_GROSS in invoice.needs_review_reasons
        assert invoice.parse_confidence < 1.0

    def test_unbekannte_position_wird_einzeln_gemeldet(self) -> None:
        mit_unbekannt = KAUFLAND_SAMMEL.replace(
            "Sponsored Product Ads Click Costs -", "Rätselhafte Sondergebühr"
        )
        invoice = parse_invoice_text(mit_unbekannt)
        assert any(
            reason.startswith(REVIEW_UNKNOWN_POSITION) for reason in invoice.needs_review_reasons
        )

    def test_gutschrift_ohne_original_wird_beanstandet(self) -> None:
        ohne_bezug = AMAZON_GUTSCHRIFT.split("Ursprüngliche")[0]
        invoice = parse_invoice_text(ohne_bezug)
        assert REVIEW_MISSING_ORIGINAL in invoice.needs_review_reasons

    def test_zeitraum_fehlt_wird_beanstandet(self) -> None:
        ohne_zeitraum = AMAZON_GEBUEHR.replace(
            "Rechnungszeitraum: 01/08/2026 to 31/08/2026", ""
        )
        invoice = parse_invoice_text(ohne_zeitraum)
        assert REVIEW_MISSING_PERIOD in invoice.needs_review_reasons

    def test_to_dict_ist_serialisierbar(self) -> None:
        payload = parse_invoice_text(KAUFLAND_SAMMEL).to_dict()
        assert payload["invoice_number"] == "TEST-R0001-00000001"
        assert isinstance(payload["lines"], list)
        assert set(payload["lines"][0]) >= {"position_key", "net_cents", "gross_cents"}
