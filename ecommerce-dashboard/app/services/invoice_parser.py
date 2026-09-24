"""Parser fuer Eingangsrechnungen der Handelsplattformen (Kaufland, Amazon).

Er liest einen Beleg und liefert strukturierte Felder, Positionen und ein
Selbstbewusstsein (`parse_confidence`) plus konkrete Gruende, warum eine
manuelle Pruefung noetig ist (`needs_review_reasons`).

Leitgedanke: Der Parser darf nicht stillschweigend etwas falsch machen. Jedes
Template bringt eine Kontrollsumme mit; wird sie verfehlt, sinkt die
Confidence und die Rechnung bleibt auf `needs_review` stehen. Es wird nichts
gebucht, was nicht freigegeben wurde -- das ist Aufgabe des Aufrufers.

Die detaillierten Template-Regeln stehen in `docs/platform-invoice-parsing.md`.
"""
from __future__ import annotations

import csv
import io
import re
import subprocess
from dataclasses import asdict, dataclass, field
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Optional

TEMPLATE_KAUFLAND_SAMMEL = "KAUFLAND_SAMMELRECHNUNG"
TEMPLATE_KAUFLAND_EINZEL = "KAUFLAND_EINZELBELEG"
TEMPLATE_AMAZON_GEBUEHR = "AMAZON_GEBUEHR"
TEMPLATE_AMAZON_ABO = "AMAZON_ABO"
TEMPLATE_AMAZON_STEUERGUTSCHRIFT = "AMAZON_STEUERGUTSCHRIFT"
TEMPLATE_AMAZON_RETAIL = "AMAZON_RETAIL_GUTSCHRIFT"
TEMPLATE_UNKNOWN = "UNKNOWN"

TEMPLATES: frozenset[str] = frozenset(
    {
        TEMPLATE_KAUFLAND_SAMMEL,
        TEMPLATE_KAUFLAND_EINZEL,
        TEMPLATE_AMAZON_GEBUEHR,
        TEMPLATE_AMAZON_ABO,
        TEMPLATE_AMAZON_STEUERGUTSCHRIFT,
        TEMPLATE_AMAZON_RETAIL,
        TEMPLATE_UNKNOWN,
    }
)

PROVIDER_KAUFLAND = "kaufland"
PROVIDER_AMAZON = "amazon"
PROVIDER_OTHER = "other"

DOC_KIND_CONSOLIDATED = "consolidated"
DOC_KIND_SINGLE = "single"
DOC_KIND_SALES = "sales"
DOC_KIND_UNKNOWN = "unknown"

# Kategorien der Belegzeilen. Deckt Kaufland- und Amazon-Gebuehren ab.
POSITION_PROVISION = "provision"
POSITION_PROVISION_STORNO = "provision_storno"
POSITION_BASE_FEE = "base_fee"
POSITION_ADVERTISING = "advertising"
POSITION_SUBSCRIPTION = "subscription"
POSITION_FULFILLMENT = "fulfillment"
POSITION_REFUND_ADMIN = "refund_admin"
POSITION_CANCELLATION_FEE = "cancellation_fee"
POSITION_FEE_REFUND = "fee_refund"
POSITION_OTHER = "other"

POSITION_KEYS: frozenset[str] = frozenset(
    {
        POSITION_PROVISION,
        POSITION_PROVISION_STORNO,
        POSITION_BASE_FEE,
        POSITION_ADVERTISING,
        POSITION_SUBSCRIPTION,
        POSITION_FULFILLMENT,
        POSITION_REFUND_ADMIN,
        POSITION_CANCELLATION_FEE,
        POSITION_FEE_REFUND,
        POSITION_OTHER,
    }
)

REVIEW_NO_TEMPLATE = "kein_template_erkannt"
REVIEW_CHECKSUM_NET = "kontrollsumme_netto_verfehlt"
REVIEW_CHECKSUM_GROSS = "kontrollsumme_brutto_verfehlt"
REVIEW_UNKNOWN_POSITION = "unbekannte_position"
REVIEW_NO_LINES = "keine_positionen_gefunden"
REVIEW_MISSING_NUMBER = "rechnungsnummer_fehlt"
REVIEW_MISSING_ORIGINAL = "originalrechnung_fehlt"
REVIEW_MISSING_PERIOD = "leistungszeitraum_fehlt"

# Rundungstoleranz fuer die Kontrollsumme. Amazon rundet je Position, deshalb
# darf die Nettosumme um bis zu einen Cent je Position abweichen.
CHECKSUM_TOLERANCE_CENTS = 2

_HOME_RATE_PERCENT = 19
_HOME_RATE_DIVISOR = Decimal("1.19")


class InvoiceParseError(Exception):
    def __init__(self, status_code: int, detail: str) -> None:
        self.status_code = status_code
        self.detail = detail
        super().__init__(detail)


@dataclass(frozen=True)
class ParsedLine:
    position_key: str
    label: str
    line_date: Optional[str]
    net_cents: int
    vat_rate: Optional[int]
    vat_cents: int
    gross_cents: int
    currency: str
    line_count: int = 1
    order_ref: Optional[str] = None


@dataclass
class ParsedInvoice:
    template: str = TEMPLATE_UNKNOWN
    provider: str = PROVIDER_OTHER
    doc_kind: str = DOC_KIND_UNKNOWN
    category_hint: str = "other"
    invoice_number: Optional[str] = None
    invoice_date: Optional[str] = None
    period_from: Optional[str] = None
    period_to: Optional[str] = None
    currency: str = "EUR"
    fx_rate: Optional[str] = None
    vat_cents_eur: Optional[int] = None
    original_invoice_number: Optional[str] = None
    is_vat_deductible: bool = True
    lines: list[ParsedLine] = field(default_factory=list)
    net_cents: int = 0
    vat_cents: int = 0
    gross_cents: int = 0
    parse_confidence: float = 0.0
    needs_review_reasons: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["lines"] = [asdict(line) for line in self.lines]
        return payload


# ---------------------------------------------------------------------------
# Betraege und Daten
# ---------------------------------------------------------------------------


def _text(value: Any) -> str:
    return str(value if value is not None else "").strip()


def to_decimal(value: Any) -> Decimal:
    """`1.403,37` (de) und `137.46` (en) ergeben beide `1403.37` / `137.46`."""
    raw = _text(value).replace("\xa0", "").replace(" ", "")
    raw = raw.replace("EUR", "").replace("GBP", "").strip()
    if not raw:
        return Decimal("0")
    negative = raw.startswith("-")
    body = raw.lstrip("+-")
    if "," in body and "." in body:
        if body.rindex(",") > body.rindex("."):
            body = body.replace(".", "").replace(",", ".")
        else:
            body = body.replace(",", "")
    else:
        body = body.replace(",", ".")
    if not body:
        return Decimal("0")
    amount = Decimal(body)
    return -amount if negative else amount


def to_cents(value: Any) -> int:
    amount = value if isinstance(value, Decimal) else to_decimal(value)
    return int((amount * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def split_gross(gross_cents: int, rate: int = _HOME_RATE_PERCENT) -> tuple[int, int]:
    """Brutto in Netto/Vorsteuer zerlegen, je Position gerundet.

    Amazon und Kaufland runden beide je Position. Deshalb `brutto/1.19`
    statt `netto * 1.19`; sonst weicht die Nettosumme um Rundungsreste ab.
    """
    if rate <= 0:
        return gross_cents, 0
    gross = Decimal(gross_cents) / 100
    net = (gross / (Decimal(1) + Decimal(rate) / 100)).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )
    net_cents = to_cents(net)
    return net_cents, gross_cents - net_cents


def _iso_day_first(value: str) -> Optional[str]:
    token = _text(value)
    parts = re.split(r"[./]", token)
    if len(parts) != 3:
        return None
    try:
        day, month, year = (int(part) for part in parts)
    except ValueError:
        return None
    return f"{year:04d}-{month:02d}-{day:02d}"


def _iso_month_first(value: str) -> Optional[str]:
    token = _text(value)
    parts = re.split(r"[./]", token)
    if len(parts) != 3:
        return None
    try:
        month, day, year = (int(part) for part in parts)
    except ValueError:
        return None
    return f"{year:04d}-{month:02d}-{day:02d}"


# ---------------------------------------------------------------------------
# Zeilenklassifikation
# ---------------------------------------------------------------------------


def classify_position(label: Any) -> str:
    low = _text(label).lower()
    if not low:
        return POSITION_OTHER
    if "storno provision" in low or "storno-provision" in low:
        return POSITION_PROVISION_STORNO
    if "cancelled orders" in low:
        return POSITION_CANCELLATION_FEE
    if "grundgeb" in low:
        return POSITION_BASE_FEE
    if "sponsored" in low or "click costs" in low:
        return POSITION_ADVERTISING
    if "refund administration" in low:
        return POSITION_REFUND_ADMIN
    if "erstattung" in low or "credit" in low:
        return POSITION_FEE_REFUND
    if "subscription" in low or "selling on amazon" in low:
        return POSITION_SUBSCRIPTION
    if "pick & pack" in low or "inbound" in low or "inventory storage" in low:
        return POSITION_FULFILLMENT
    if "provision" in low or "referral" in low:
        return POSITION_PROVISION
    return POSITION_OTHER


def _order_ref(label: Any) -> Optional[str]:
    match = re.search(r"(?:Bestell-Nr\.|Bestellnummer)[\s:]*([0-9A-Za-z/_-]+)", _text(label))
    return match.group(1) if match else None


# ---------------------------------------------------------------------------
# Text-Erkennung
# ---------------------------------------------------------------------------


def detect_template(text: str) -> str:
    if "Abrechnungsbeleg Nr." in text:
        return TEMPLATE_KAUFLAND_EINZEL
    if "STEUERGUTSCHRIFT" in text or "Gutschriftennummer:" in text:
        return TEMPLATE_AMAZON_STEUERGUTSCHRIFT
    if "Rechnungs Nr." in text and "Netto (EUR)" in text:
        return TEMPLATE_KAUFLAND_SAMMEL
    if "Invoice Period:" in text and "Invoice Number:" in text:
        return TEMPLATE_AMAZON_ABO
    # `Rechnungszeitraum` ist optional -- wenn er fehlt, bleibt das Template
    # erkennbar und meldet ihn stattdessen als fehlend.
    if "Rechnungsnummer:" in text and "Dienstleistung" in text:
        return TEMPLATE_AMAZON_GEBUEHR
    if "GUTSCHRIFT" in text and "Bestellnummer" in text:
        return TEMPLATE_AMAZON_RETAIL
    return TEMPLATE_UNKNOWN


# Zwei Zeilenformate im Kaufland-PDF:
#   DD.MM.JJJJ  <Bezeichnung>  <Netto>  <MwSt>  19%  <Brutto>
#   DD.MM.JJJJ  <Bezeichnung>  <Zahlbetrag>          (Einzelbeleg)
_RE_KFL_LINE_VAT = re.compile(
    r"^(\d{2}\.\d{2}\.\d{4})\s+(.+?)\s{2,}"
    r"(-?[\d.]+,\d{2})\s+(-?[\d.]+,\d{2})\s+(\d+)%\s+(-?[\d.]+,\d{2})\s*$"
)
_RE_KFL_LINE_FLAT = re.compile(r"^(\d{2}\.\d{2}\.\d{4})\s+(.+?)\s{2,}(-?[\d.]+,\d{2})\s*$")

# Amazon: Betragszeile kann ohne Bezeichnung dastehen (Beschriftung bricht um).
#   <Bezeichnung>   EUR 137.46   19.00%   EUR 26.12   EUR 163.58
#                   EUR 137.46   19.00%   EUR 26.12   EUR 163.58
_AMOUNTS = (
    r"(-?)(?:EUR|GBP)\s*([\d.,]+)\s+(-?[\d.,]+)%\s+"
    r"(-?)(?:EUR|GBP)\s*([\d.,]+)\s+(-?)(?:EUR|GBP)\s*([\d.,]+)"
)
_RE_AMZ_LINE_LABEL = re.compile(rf"^(.+?)\s{{2,}}{_AMOUNTS}\s*$")
_RE_AMZ_LINE_BARE = re.compile(rf"^\s*{_AMOUNTS}\s*$")
# Die Gesamtsummen-Zeile fuehrt KEINEN Steuersatz:  Gesamtsumme  EUR 137.46  EUR 26.12  EUR 163.58
_RE_AMZ_TOTAL = re.compile(
    r"^\s*(?:Gesamtsumme|Total)\s+"
    r"(-?)(?:EUR|GBP)\s*([\d.,]+)\s+(?:(-?[\d.,]+)%\s+)?"
    r"(-?)(?:EUR|GBP)\s*([\d.,]+)\s+(-?)(?:EUR|GBP)\s*([\d.,]+)\s*$"
)

_RE_KFL_TOTAL = re.compile(
    r"^Summe 19%\s+(-?[\d.]+,\d{2})\s+(-?[\d.]+,\d{2})\s+(-?[\d.]+,\d{2})\s*$"
)
# `Summe  57,23` (Kaufland-Einzelbeleg) und `GESAMT: EUR 633,50` (Amazon-Retail).
_RE_FLAT_TOTAL = re.compile(r"^(?:GESAMT|Summe):?\s+(?:EUR\s*)?(-?[\d.,]+)\s*$")


def _money(sign: str, digits: str) -> int:
    """`sign` ist das Zeichen VOR dem Waehrungssymbol (`-EUR`), `digits` der Betrag.

    Beide Wege duerfen ein Minus tragen: Amazon setzt es vor das Symbol,
    Kaufland mitten in die Zahl. Deshalb nur negieren, wenn nicht schon negativ.
    """
    amount = to_cents(digits)
    if sign == "-" and amount > 0:
        amount = -amount
    return amount


def _make_line(
    label: str,
    net: int,
    rate: Optional[int],
    vat: int,
    gross: int,
    currency: str,
    line_date: Optional[str] = None,
    order_ref: Optional[str] = None,
) -> ParsedLine:
    return ParsedLine(
        position_key=classify_position(label),
        label=_text(label),
        line_date=line_date,
        net_cents=net,
        vat_rate=rate,
        vat_cents=vat,
        gross_cents=gross,
        currency=currency,
        order_ref=order_ref or _order_ref(label),
    )


# ---------------------------------------------------------------------------
# Template-Parser
# ---------------------------------------------------------------------------


def _parse_kaufland_sammel(text: str) -> ParsedInvoice:
    invoice = ParsedInvoice(
        template=TEMPLATE_KAUFLAND_SAMMEL,
        provider=PROVIDER_KAUFLAND,
        doc_kind=DOC_KIND_CONSOLIDATED,
        category_hint="fee",
        currency="EUR",
    )
    match = re.search(r"Rechnungs Nr\.:\s*(\S+)", text)
    if match:
        invoice.invoice_number = match.group(1)
    match = re.search(r"Neckarsulm,\s*(\d{2}\.\d{2}\.\d{4})", text)
    if match:
        invoice.invoice_date = _iso_day_first(match.group(1))

    for raw in text.splitlines():
        row = raw.rstrip()
        match = _RE_KFL_LINE_VAT.match(row)
        if match:
            line_date, label = match.group(1), match.group(2).strip()
            rate = int(match.group(5))
            invoice.lines.append(
                _make_line(
                    label,
                    _money("", match.group(3)),
                    rate,
                    _money("", match.group(4)),
                    _money("", match.group(6)),
                    "EUR",
                    line_date=_iso_day_first(line_date),
                )
            )
            continue
        match = _RE_KFL_TOTAL.match(row)
        if match:
            invoice.net_cents = _money("", match.group(1))
            invoice.vat_cents = _money("", match.group(2))
            invoice.gross_cents = _money("", match.group(3))

    service_dates = sorted(line.line_date for line in invoice.lines if line.line_date)
    if service_dates:
        invoice.period_from = service_dates[0]
        invoice.period_to = service_dates[-1]
    return invoice


def _parse_kaufland_einzel(text: str) -> ParsedInvoice:
    invoice = ParsedInvoice(
        template=TEMPLATE_KAUFLAND_EINZEL,
        provider=PROVIDER_KAUFLAND,
        doc_kind=DOC_KIND_SINGLE,
        category_hint="cancellation_fee",
        currency="EUR",
    )
    match = re.search(r"Abrechnungsbeleg Nr\.:\s*(\S+)", text)
    if match:
        invoice.invoice_number = match.group(1)
    match = re.search(r"Neckarsulm,\s*(\d{2}\.\d{2}\.\d{4})", text)
    if match:
        invoice.invoice_date = _iso_day_first(match.group(1))

    for raw in text.splitlines():
        row = raw.rstrip()
        match = _RE_KFL_LINE_FLAT.match(row)
        if match:
            label = match.group(2).strip()
            if "Zahlbetrag" in label or label.lower().startswith("datum"):
                continue
            gross = _money("", match.group(3))
            invoice.lines.append(
                _make_line(
                    label,
                    gross,
                    None,
                    0,
                    gross,
                    "EUR",
                    line_date=_iso_day_first(match.group(1)),
                )
            )
            continue
        match = _RE_FLAT_TOTAL.match(row)
        if match:
            gross = _money("", match.group(1))
            invoice.gross_cents = gross
            invoice.net_cents = gross
            invoice.vat_cents = 0

    invoice.is_vat_deductible = False
    service_dates = sorted(line.line_date for line in invoice.lines if line.line_date)
    if service_dates:
        invoice.period_from = service_dates[0]
        invoice.period_to = service_dates[-1]
    return invoice


def _parse_amazon_amounts(text: str) -> ParsedInvoice:
    """Gemeinsame Amazon-Logik fuer Gebuehren (EUR) und Abo (GBP)."""
    english = "Invoice Period:" in text
    invoice = ParsedInvoice(
        template=TEMPLATE_AMAZON_ABO if english else TEMPLATE_AMAZON_GEBUEHR,
        provider=PROVIDER_AMAZON,
        doc_kind=DOC_KIND_CONSOLIDATED,
        category_hint="fee",
        currency="GBP" if english else "EUR",
    )
    if english:
        match = re.search(r"Invoice Number:\s*(\S+)", text)
        if match:
            invoice.invoice_number = match.group(1)
        match = re.search(r"Invoice Date:\s*([\d./]+)", text)
        if match:
            invoice.invoice_date = _iso_day_first(match.group(1))
        match = re.search(
            r"Invoice Period:\s*([\d./]+)\s*to\s*([\d./]+)", text
        )
    else:
        match = re.search(r"Rechnungsnummer:\s*(\S+)", text)
        if match:
            invoice.invoice_number = match.group(1)
        match = re.search(r"Rechnungsdatum:\s*([\d./]+)", text)
        if match:
            invoice.invoice_date = _iso_day_first(match.group(1))
        match = re.search(
            r"Rechnungszeitraum:\s*([\d./]+)\s*to\s*([\d./]+)", text
        )
    if match:
        invoice.period_from = _iso_day_first(match.group(1))
        invoice.period_to = _iso_day_first(match.group(2))

    match = re.search(r"Exchange Rate:\s*\[([\d.]+)\s*EUR\s*/\s*1\s*GBP\]", text)
    if match:
        invoice.fx_rate = match.group(1)
    match = re.search(r"^\s*EUR\s+([\d.,]+)\s*$", text, re.MULTILINE)
    if match:
        invoice.vat_cents_eur = to_cents(match.group(1))

    carry_label = ""
    for raw in text.splitlines():
        row = raw.strip()
        if not row:
            continue
        match = _RE_AMZ_TOTAL.match(row)
        if match:
            invoice.net_cents = _money(match.group(1), match.group(2))
            invoice.vat_cents = _money(match.group(4), match.group(5))
            invoice.gross_cents = _money(match.group(6), match.group(7))
            carry_label = ""
            continue
        match = _RE_AMZ_LINE_LABEL.match(row)
        if match:
            label = match.group(1).strip()
            rate = int(to_decimal(match.group(4)))
            invoice.lines.append(
                _make_line(
                    label,
                    _money(match.group(2), match.group(3)),
                    rate,
                    _money(match.group(5), match.group(6)),
                    _money(match.group(7), match.group(8)),
                    invoice.currency,
                )
            )
            carry_label = ""
            continue
        match = _RE_AMZ_LINE_BARE.match(row)
        if match:
            label = carry_label or "Position"
            rate = int(to_decimal(match.group(3)))
            invoice.lines.append(
                _make_line(
                    label,
                    _money(match.group(1), match.group(2)),
                    rate,
                    _money(match.group(4), match.group(5)),
                    _money(match.group(6), match.group(7)),
                    invoice.currency,
                )
            )
            carry_label = ""
            continue
        # Beschriftungen brechen bei Amazon mehrzeilig um. Wir sammeln die
        # Zeilen VOR der Betragszeile; der Nachlauf danach geht in die
        # Kategorisierung der folgenden Zeile ein (CSV ist die sichere Quelle).
        carry_label = f"{carry_label} {row}".strip() if carry_label else row
    return invoice


def _parse_amazon_gutschrift(text: str) -> ParsedInvoice:
    invoice = ParsedInvoice(
        template=TEMPLATE_AMAZON_STEUERGUTSCHRIFT,
        provider=PROVIDER_AMAZON,
        doc_kind=DOC_KIND_SINGLE,
        category_hint="fee_refund",
        currency="EUR",
    )
    match = re.search(r"Gutschriftennummer:\s*(\S+)", text)
    if match:
        invoice.invoice_number = match.group(1)
    match = re.search(r"Ausstellungsdatum der Gutschrift:\s*([\d./]+)", text)
    if match:
        invoice.invoice_date = _iso_day_first(match.group(1))
    match = re.search(r"Zeitraum der Gutschrift:\s*([\d./]+)\s*to\s*([\d./]+)", text)
    if match:
        invoice.period_from = _iso_day_first(match.group(1))
        invoice.period_to = _iso_day_first(match.group(2))
    match = re.search(r"Ursprüngliche Rechnungsnummer\s*\n\s*(\S+)", text)
    if match:
        invoice.original_invoice_number = match.group(1)

    for raw in text.splitlines():
        row = raw.strip()
        if not row:
            continue
        # Die Gesamtsumme darf nicht als Position gelten -- zuerst pruefen.
        match = _RE_AMZ_TOTAL.match(row)
        if match:
            invoice.net_cents = _money(match.group(1), match.group(2))
            invoice.vat_cents = _money(match.group(4), match.group(5))
            invoice.gross_cents = _money(match.group(6), match.group(7))
            continue
        match = _RE_AMZ_LINE_LABEL.match(row)
        if match:
            rate = int(to_decimal(match.group(4)))
            invoice.lines.append(
                _make_line(
                    match.group(1).strip(),
                    _money(match.group(2), match.group(3)),
                    rate,
                    _money(match.group(5), match.group(6)),
                    _money(match.group(7), match.group(8)),
                    "EUR",
                )
            )
    return invoice


def _parse_amazon_retail(text: str) -> ParsedInvoice:
    """Kundenbeleg (Verkauf/Storno). Umsatzseite, keine Eingangsrechnung."""
    invoice = ParsedInvoice(
        template=TEMPLATE_AMAZON_RETAIL,
        provider=PROVIDER_AMAZON,
        doc_kind=DOC_KIND_SALES,
        category_hint="sales",
        currency="EUR",
    )
    match = re.search(r"Rechnungsnummer:\s*(\S+)", text)
    if match:
        invoice.invoice_number = match.group(1)
    match = re.search(r"Rechnungsdatum:\s*([\d.]+)", text)
    if match:
        invoice.invoice_date = _iso_day_first(match.group(1))
    match = _RE_FLAT_TOTAL.search(text)
    if match:
        invoice.gross_cents = _money("", match.group(1))
    # Die USt-Summenzeile fuehrt `Zwischensumme (ohne USt.)  %  USt.  USt. gesamt`.
    # Die vierte Spalte ist ALSO NICHT das Brutto -- das kommt allein aus GESAMT.
    match = re.search(r"EUR\s+([\d.]+,\d{2})\s+19,00\s+EUR\s+([\d.]+,\d{2})", text)
    if match:
        invoice.net_cents = to_cents(match.group(1))
        invoice.vat_cents = to_cents(match.group(2))
        if invoice.gross_cents == 0:
            invoice.gross_cents = invoice.net_cents + invoice.vat_cents
    invoice.is_vat_deductible = False
    return invoice


# ---------------------------------------------------------------------------
# Oeffentliche API
# ---------------------------------------------------------------------------


def extract_text(path: str | Path) -> str:
    """PDF-Text ueber `pdftotext -layout` (kein OCR noetig, die Belege sind digital)."""
    source = Path(path)
    if not source.exists():
        raise InvoiceParseError(404, f"Datei nicht gefunden: {source}")
    result = subprocess.run(
        ["pdftotext", "-layout", str(source), "-"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise InvoiceParseError(422, f"PDF nicht lesbar: {source.name}")
    return result.stdout


def parse_invoice_text(text: str) -> ParsedInvoice:
    """Kernparser: rein textbasiert, damit er ohne PDF-Abhaengigkeit testbar ist."""
    template = detect_template(text)
    if template == TEMPLATE_KAUFLAND_EINZEL:
        invoice = _parse_kaufland_einzel(text)
    elif template == TEMPLATE_KAUFLAND_SAMMEL:
        invoice = _parse_kaufland_sammel(text)
    elif template == TEMPLATE_AMAZON_STEUERGUTSCHRIFT:
        invoice = _parse_amazon_gutschrift(text)
    elif template == TEMPLATE_AMAZON_ABO:
        invoice = _parse_amazon_amounts(text)
    elif template == TEMPLATE_AMAZON_GEBUEHR:
        invoice = _parse_amazon_amounts(text)
    elif template == TEMPLATE_AMAZON_RETAIL:
        invoice = _parse_amazon_retail(text)
    else:
        invoice = ParsedInvoice(template=TEMPLATE_UNKNOWN)
        invoice.needs_review_reasons.append(REVIEW_NO_TEMPLATE)
    _finalize(invoice)
    return invoice


def parse_invoice_file(path: str | Path) -> ParsedInvoice:
    return parse_invoice_text(extract_text(path))


def parse_amazon_fee_csv(text: str) -> list[ParsedLine]:
    """Amazon stellt zur Gebuehrenrechnung eine Positions-CSV bereit.

    Spalten: Transaction Date, Transaction ID, Order ID, Fees Invoice Number,
    Marketplace, Fee ID, Total Fees (VAT-Inclusive). Die Betraege sind brutto.
    """
    reader = csv.DictReader(io.StringIO(text))
    lines: list[ParsedLine] = []
    for row in reader:
        label = _text(row.get("Fee ID"))
        if not label:
            continue
        gross = to_cents(row.get("Total Fees (VAT-Inclusive)") or 0)
        rate = _HOME_RATE_PERCENT
        net, vat = split_gross(gross, rate)
        lines.append(
            _make_line(
                label,
                net,
                rate,
                vat,
                gross,
                "EUR",
                line_date=_iso_month_first(row.get("Transaction Date") or ""),
                order_ref=_text(row.get("Order ID")) or None,
            )
        )
    return lines


def _merge_csv_lines(invoice: ParsedInvoice, lines: list[ParsedLine]) -> None:
    if not lines:
        return
    invoice.lines = lines
    if invoice.gross_cents == 0:
        invoice.gross_cents = sum(line.gross_cents for line in lines)
    if invoice.net_cents == 0:
        invoice.net_cents = sum(line.net_cents for line in lines)
    if invoice.vat_cents == 0:
        invoice.vat_cents = sum(line.vat_cents for line in lines)


def _finalize(invoice: ParsedInvoice) -> None:
    if not invoice.invoice_number:
        invoice.needs_review_reasons.append(REVIEW_MISSING_NUMBER)
    if not invoice.lines:
        invoice.needs_review_reasons.append(REVIEW_NO_LINES)
    if invoice.template == TEMPLATE_AMAZON_STEUERGUTSCHRIFT and not invoice.original_invoice_number:
        invoice.needs_review_reasons.append(REVIEW_MISSING_ORIGINAL)
    if invoice.doc_kind == DOC_KIND_CONSOLIDATED and not (invoice.period_from and invoice.period_to):
        invoice.needs_review_reasons.append(REVIEW_MISSING_PERIOD)

    unknown = [line.label for line in invoice.lines if line.position_key == POSITION_OTHER]
    for label in unknown:
        invoice.needs_review_reasons.append(f"{REVIEW_UNKNOWN_POSITION}:{label[:40]}")

    line_gross = sum(line.gross_cents for line in invoice.lines)
    line_net = sum(line.net_cents for line in invoice.lines)
    if invoice.lines and invoice.gross_cents and abs(line_gross - invoice.gross_cents) > CHECKSUM_TOLERANCE_CENTS:
        invoice.needs_review_reasons.append(REVIEW_CHECKSUM_GROSS)
    if invoice.lines and invoice.net_cents and abs(line_net - invoice.net_cents) > CHECKSUM_TOLERANCE_CENTS * max(
        len(invoice.lines), 1
    ):
        invoice.needs_review_reasons.append(REVIEW_CHECKSUM_NET)

    confidence = 1.0
    if invoice.template == TEMPLATE_UNKNOWN:
        confidence = 0.0
    else:
        confidence -= 0.35 * sum(
            1
            for reason in invoice.needs_review_reasons
            if reason in (REVIEW_CHECKSUM_GROSS, REVIEW_CHECKSUM_NET, REVIEW_NO_LINES, REVIEW_NO_TEMPLATE)
        )
        confidence -= 0.15 * sum(
            1 for reason in invoice.needs_review_reasons if reason.startswith(REVIEW_UNKNOWN_POSITION)
        )
        confidence -= 0.1 * sum(
            1
            for reason in invoice.needs_review_reasons
            if reason in (REVIEW_MISSING_NUMBER, REVIEW_MISSING_ORIGINAL, REVIEW_MISSING_PERIOD)
        )
    invoice.parse_confidence = round(max(confidence, 0.0), 3)
