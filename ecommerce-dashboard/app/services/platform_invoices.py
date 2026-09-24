"""Eingangsrechnungen: parsen, gegen die Buchungen abgleichen, freigeben.

Drei Stufen, bewusst getrennt:

1. `parse_uploaded_document` -- liest PDF/CSV und liefert eine Vorbefuellung.
   Legt **nichts** an. Der Aufrufer (Mensch oder Agent) sieht, was der Parser
   glaubt, und entscheidet.
2. `create_platform_invoice` -- legt die Rechnung im Status `needs_review` an.
   Weiterhin ohne jede Buchung.
3. `approve_platform_invoice` -- der einzige Weg ins Ledger. Er erzeugt die
   fehlenden Buchungen, gleicht gegen die bereits gebuchte Verkaufsprovision ab
   und bucht eine Abweichung als eigene, markierte Korrektur.

Die Verkaufsprovision selbst wird NICHT hier erzeugt -- sie laeuft bereits
orderbezogen aus `order_units` ins Ledger und bleibt unangetastet. Das ist die
freigegebene Entscheidung (b): die Automatik ist die Buchung, die Rechnung
testiert sie.

Haerte Regel aus dem Design: nur ergänzen. Keine bestehende Zeile wird
ueberschrieben; Abweichungen werden durch eine zusaetzliche Transaktion
ausgeglichen, nicht durch Aendern der bisherigen.

Sammelrechnungen (`R…`, `DE-AEU-…`) laufen ueber `create_monthly_invoice`,
das eine Ueberlappung pro Periode und Provider ausschliesst. Einzelbelege
(`C…`, Gutschriften) werden direkt angelegt -- sie teilen sich bewusst nicht
die Monatslogik, sonst wuerde der C-Beleg mit der R-Rechnung desselben Monats
kollidieren.
"""
from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Optional

from app.config import BOOKKEEPING_DB_PATH
from app.services import invoice_parser as parser
from app.services.bookkeeping_full import (
    BookkeepingServiceError,
    SAMMELRECHNUNG_PROVIDERS,
)

POSITION_TO_TRANSACTION_TYPE = {
    parser.POSITION_PROVISION: "FEE",
    parser.POSITION_PROVISION_STORNO: "FEE",
    parser.POSITION_BASE_FEE: "SUBSCRIPTION",
    parser.POSITION_ADVERTISING: "FEE",
    parser.POSITION_SUBSCRIPTION: "SUBSCRIPTION",
    parser.POSITION_FULFILLMENT: "FEE",
    parser.POSITION_REFUND_ADMIN: "FEE",
    parser.POSITION_CANCELLATION_FEE: "EXPENSE",
    parser.POSITION_FEE_REFUND: "FEE",
    parser.POSITION_OTHER: "EXPENSE",
}

# Kategorien, die eine Plattformgebuehrenrechnung ausmachen. `cancellation_fee`
# (Einzelbeleg) und `invoice_variance` (Korrektur) zaehlen nicht zur
# Erwartungssumme -- sie entstehen aus der Rechnung selbst.
EXPECTED_FEE_CATEGORIES = frozenset(
    {
        "fees",  # Kategorie der bestehenden orderbezogenen FEE-Buchungen
        parser.POSITION_PROVISION,
        parser.POSITION_PROVISION_STORNO,
        parser.POSITION_BASE_FEE,
        parser.POSITION_ADVERTISING,
        parser.POSITION_SUBSCRIPTION,
        parser.POSITION_FULFILLMENT,
        parser.POSITION_REFUND_ADMIN,
        parser.POSITION_FEE_REFUND,
    }
)
# Diese zaehlen zusaetzlich, sobald sie aus der Rechnung gebucht wurden.
BOOKED_AFTER_APPROVAL = frozenset(
    {"invoice_variance", parser.POSITION_CANCELLATION_FEE}
)

VARIANCE_CATEGORY = "invoice_variance"

# Ohne abziehbare Vorsteuer. `sales` ist ein Umsatzbeleg, `cancellation_fee` der
# Kaufland-Schadensersatz („nicht steuerbar", ohne ausgewiesene Umsatzsteuer).
NOT_DEDUCTIBLE_CATEGORIES = frozenset(
    {parser.POSITION_CANCELLATION_FEE, "sales", parser.POSITION_OTHER}
)


def _is_deductible(invoice: dict[str, Any]) -> bool:
    """Vorsteuerabzug ja/nein -- aus der Kategorie abgeleitet.

    Bewusst keine eigene Spalte: die Aussage haengt allein am Belegtyp, und der
    steht bereits in `doc_category`.
    """
    category = str(invoice.get("doc_category") or "")
    return category not in NOT_DEDUCTIBLE_CATEGORIES


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _iso_day(value: Any) -> str:
    """Schneidet jedes Datumsformat auf `YYYY-MM-DD` -- auch `...T00:00:00Z`."""
    token = str(value or "").strip()
    return token[:10] if token else ""


@contextmanager
def _open_db() -> Iterator[sqlite3.Connection]:
    if not BOOKKEEPING_DB_PATH.exists():
        raise BookkeepingServiceError(503, "bookkeeping database not available")
    connection = sqlite3.connect(BOOKKEEPING_DB_PATH)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    try:
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


# ---------------------------------------------------------------------------
# 1) Parsen -- legt nichts an
# ---------------------------------------------------------------------------


def parse_uploaded_document(
    *,
    pdf_path: Optional[str | Path] = None,
    pdf_text: Optional[str] = None,
    csv_text: Optional[str] = None,
) -> dict[str, Any]:
    """Liefert Vorbefuellung + Warnungen + Abgleich-Vorschau, ohne zu speichern."""
    if pdf_text is None:
        if pdf_path is None:
            raise BookkeepingServiceError(400, "PDF oder PDF-Text angeben")
        pdf_text = parser.extract_text(pdf_path)
    parsed = parser.parse_invoice_text(pdf_text)
    if csv_text:
        parser._merge_csv_lines(parsed, parser.parse_amazon_fee_csv(csv_text))
        parser._finalize(parsed)

    payload = parsed.to_dict()
    payload["period_from"] = parsed.period_from or parsed.invoice_date
    payload["period_to"] = parsed.period_to or parsed.invoice_date
    payload["doc_category"] = parsed.category_hint
    payload["preview"] = preview_reconciliation(
        provider=parsed.provider,
        period_from=payload["period_from"],
        period_to=payload["period_to"],
        invoice_amount_cents=parsed.gross_cents,
    )
    return payload


# ---------------------------------------------------------------------------
# Abgleich
# ---------------------------------------------------------------------------


def expected_fee_cents(
    connection: sqlite3.Connection,
    *,
    provider: str,
    period_from: str,
    period_to: str,
    include_booked: bool = False,
) -> int:
    """Was an Gebuehren im Zeitraum bereits gebucht ist.

    Vor der Freigabe zaehlt nur die laufende Automatik. Danach auch die aus der
    Rechnung gebuchten Positionen, damit die Differenz gegen null laeuft.
    """
    start = _iso_day(period_from)
    end = _iso_day(period_to) or start
    if not start:
        return 0
    categories = set(EXPECTED_FEE_CATEGORIES)
    if include_booked:
        categories |= set(BOOKED_AFTER_APPROVAL)
    placeholders = ",".join("?" for _ in categories)
    row = connection.execute(
        f"""
        SELECT COALESCE(SUM(amount_gross), 0) AS total
        FROM transactions
        WHERE direction = 'OUT'
          AND provider = ?
          AND substr(date, 1, 10) BETWEEN ? AND ?
          AND category IN ({placeholders})
        """,
        (provider, start, end, *sorted(categories)),
    ).fetchone()
    return int(row["total"])


def preview_reconciliation(
    *,
    provider: str,
    period_from: Optional[str],
    period_to: Optional[str],
    invoice_amount_cents: int,
) -> dict[str, Any]:
    """Abgleich-Vorschau gegen die bereits gebuchten Gebuehren."""
    if not period_from:
        return {
            "expected_cents": 0,
            "invoice_cents": invoice_amount_cents,
            "difference_cents": invoice_amount_cents,
            "has_expected": False,
        }
    with _open_db() as connection:
        expected = expected_fee_cents(
            connection,
            provider=provider,
            period_from=period_from,
            period_to=period_to,
        )
    return {
        "expected_cents": expected,
        "invoice_cents": invoice_amount_cents,
        "difference_cents": invoice_amount_cents - expected,
        "has_expected": expected != 0,
    }


def get_invoice(invoice_id: str) -> dict[str, Any]:
    """Liest eine Eingangsrechnung ueber dieselbe Verbindung wie das Schreiben.

    `get_monthly_invoice` bleibt fuer bestehende Clients unangetastet, oeffnet
    aber seine eigene Verbindung -- hier waeren es zwei Datenbanken. Der Lese-
    und Schreibweg muessen identisch sein.
    """
    with _open_db() as connection:
        row = connection.execute(
            "SELECT * FROM monthly_invoices WHERE id = ?", (invoice_id,)
        ).fetchone()
        if row is None:
            raise BookkeepingServiceError(404, "monthly invoice not found")
        payload = dict(row)
        lines = payload.get("lines_json")
        payload["lines"] = json.loads(lines) if lines else []
        reasons = payload.get("needs_review_reasons")
        payload["needs_review_reasons"] = json.loads(reasons) if reasons else []
        transactions = connection.execute(
            """
            SELECT t.id, t.date, t.type, t.direction, t.amount_gross, t.currency,
                   t.category, t.reference, t.notes, t.source, t.status
            FROM transactions t
            JOIN monthly_invoice_transactions mit ON mit.transaction_id = t.id
            WHERE mit.invoice_id = ?
            ORDER BY t.date ASC, t.created_at ASC
            """,
            (invoice_id,),
        ).fetchall()
        payload["transactions"] = [dict(item) for item in transactions]
    return payload


# ---------------------------------------------------------------------------
# 2) Anlegen -- ohne Buchung
# ---------------------------------------------------------------------------


def create_platform_invoice(
    *,
    parsed: dict[str, Any],
    document_id: Optional[str] = None,
    notes: str = "",
    invoice_amount_cents: Optional[int] = None,
    vat_amount_cents: Optional[int] = None,
) -> dict[str, Any]:
    """Legt die Rechnung im Status `needs_review` an. Es wird nichts gebucht."""
    provider = (parsed.get("provider") or "other").lower()
    if provider not in SAMMELRECHNUNG_PROVIDERS:
        provider = "other"
    period_from = _iso_day(parsed.get("period_from")) or _iso_day(parsed.get("invoice_date"))
    period_to = _iso_day(parsed.get("period_to")) or period_from
    if not period_from or not period_to:
        raise BookkeepingServiceError(
            422,
            "Leistungszeitraum fehlt -- kann nicht abgelegt werden "
            f"(period_from={parsed.get('period_from')!r} -> {period_from!r}, "
            f"period_to={parsed.get('period_to')!r} -> {period_to!r}, "
            f"invoice_date={parsed.get('invoice_date')!r})",
        )
    amount = (
        invoice_amount_cents
        if invoice_amount_cents is not None
        else int(parsed.get("gross_cents") or 0)
    )
    vat = (
        vat_amount_cents
        if vat_amount_cents is not None
        else int(parsed.get("vat_cents") or 0)
    )
    if amount <= 0:
        raise BookkeepingServiceError(422, "Rechnungsbetrag fehlt oder ist nicht positiv")
    if vat > amount:
        raise BookkeepingServiceError(422, "Vorsteuer darf den Rechnungsbetrag nicht uebersteigen")

    doc_kind = parsed.get("doc_kind") or parser.DOC_KIND_UNKNOWN
    with _open_db() as connection:
        duplicate = connection.execute(
            "SELECT id FROM monthly_invoices WHERE invoice_number = ? AND provider = ?",
            (parsed.get("invoice_number"), provider),
        ).fetchone()
        if duplicate is not None and parsed.get("invoice_number"):
            raise BookkeepingServiceError(
                409,
                f"Rechnungsnummer {parsed.get('invoice_number')} ist fuer "
                f"{provider} bereits erfasst",
            )
        invoice_id = _insert_invoice(
            connection,
            provider=provider,
            period_from=period_from,
            period_to=period_to,
            amount=amount,
            vat=vat,
            currency=parsed.get("currency") or "EUR",
            document_id=document_id,
            notes=notes,
            doc_kind=doc_kind,
        )
        _store_parsed_fields(connection, invoice_id, parsed, status="needs_review")
    return get_invoice(invoice_id)


def _insert_invoice(
    connection: sqlite3.Connection,
    *,
    provider: str,
    period_from: str,
    period_to: str,
    amount: int,
    vat: int,
    currency: str,
    document_id: Optional[str],
    notes: str,
    doc_kind: str,
) -> str:
    """Eigener Anlageweg.

    `create_monthly_invoice` bleibt fuer bestehende Clients unangetastet, hat
    aber zwei Eigenschaften, die hier nicht passen: Es schliesst Ueberlappungen
    pro Periode aus (ein `C…`-Beleg teilt sich bewusst den Monat mit der
    `R…`-Rechnung), und es verwaltet seine eigene Datenbankverbindung. Der
    Doppelschutz laeuft stattdessen ueber die Rechnungsnummer.
    """
    invoice_id = str(uuid.uuid4())
    now = _utc_now()
    connection.execute(
        """
        INSERT INTO monthly_invoices (
            id, provider, period_from, period_to,
            invoice_amount_cents, vat_amount_cents, currency,
            calculated_sum_cents, difference_cents, document_id,
            notes, status, created_at, updated_at, doc_kind
        ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, 'needs_review', ?, ?, ?)
        """,
        (
            invoice_id,
            provider,
            f"{period_from}T00:00:00Z",
            f"{period_to}T23:59:59Z",
            amount,
            vat,
            currency,
            document_id,
            notes or None,
            now,
            now,
            doc_kind,
        ),
    )
    return invoice_id


def _store_parsed_fields(
    connection: sqlite3.Connection,
    invoice_id: str,
    parsed: dict[str, Any],
    *,
    status: str,
) -> None:
    connection.execute(
        """
        UPDATE monthly_invoices
           SET invoice_number = ?,
               invoice_date = ?,
               doc_kind = ?,
               doc_category = ?,
               original_invoice_number = ?,
               lines_json = ?,
               parse_confidence = ?,
               needs_review_reasons = ?,
               fx_rate = ?,
               vat_cents_eur = ?,
               status = ?,
               updated_at = ?
         WHERE id = ?
        """,
        (
            parsed.get("invoice_number"),
            _iso_day(parsed.get("invoice_date")),
            parsed.get("doc_kind"),
            parsed.get("category_hint") or parsed.get("doc_category"),
            parsed.get("original_invoice_number"),
            json.dumps(parsed.get("lines") or [], ensure_ascii=False),
            parsed.get("parse_confidence"),
            json.dumps(parsed.get("needs_review_reasons") or [], ensure_ascii=False),
            parsed.get("fx_rate"),
            parsed.get("vat_cents_eur"),
            status,
            _utc_now(),
            invoice_id,
        ),
    )


# ---------------------------------------------------------------------------
# 3) Freigeben -- der einzige Weg ins Ledger
# ---------------------------------------------------------------------------


def approve_platform_invoice(
    invoice_id: str,
    *,
    create_missing_bookings: bool = True,
) -> dict[str, Any]:
    """Bucht fehlende Positionen, gleicht ab und markiert die Rechnung freigegeben.

    Es werden ausschliesslich NEUE Transaktionen erzeugt. Die bereits gebuchte
    Verkaufsprovision bleibt unangetastet; eine Abweichung wird als eigene
    `ADJUSTMENT`-Buchung ausgeglichen.
    """
    invoice = get_invoice(invoice_id)
    if invoice.get("doc_kind") == parser.DOC_KIND_SALES:
        raise BookkeepingServiceError(
            422, "Umsatzbelege werden nicht als Eingangsrechnung gebucht"
        )

    created_ids: list[str] = []
    with _open_db() as connection:
        if create_missing_bookings:
            created_ids = _create_line_bookings(connection, invoice)
        _link_transactions(connection, invoice, created_ids)

        period_from = _iso_day(invoice.get("period_from"))
        period_to = _iso_day(invoice.get("period_to"))
        invoice_amount = int(invoice.get("invoice_amount_cents") or 0)
        is_consolidated = invoice.get("doc_kind") != parser.DOC_KIND_SINGLE
        # Einzelbelege haben keinen Abgleich: sie sind eine eigene Rechnung mit
        # einer Position. Wuerde man hier gegen die Summe aller Gebuehren im
        # Zeitraum rechnen, zoegen sie Buchungen der Sammelrechnung an sich und
        # erzeugten eine falsche Korrektur.
        booked = (
            expected_fee_cents(
                connection,
                provider=invoice.get("provider") or "",
                period_from=period_from,
                period_to=period_to,
                include_booked=True,
            )
            if is_consolidated
            else invoice_amount
        )
        difference = invoice_amount - booked
        had_variance = False

        if difference != 0 and is_consolidated:
            variance_id = _create_variance_booking(
                connection, invoice, difference, booked
            )
            created_ids.append(variance_id)
            connection.execute(
                "INSERT OR IGNORE INTO monthly_invoice_transactions "
                "(invoice_id, transaction_id) VALUES (?, ?)",
                (invoice_id, variance_id),
            )
            had_variance = True
            booked = invoice_amount
            difference = 0

        connection.execute(
            """
            UPDATE monthly_invoices
               SET calculated_sum_cents = ?,
                   difference_cents = ?,
                   status = 'approved',
                   updated_at = ?
             WHERE id = ?
            """,
            (booked, difference, _utc_now(), invoice_id),
        )

    result = get_invoice(invoice_id)
    result["approved_transaction_ids"] = created_ids
    result["had_variance"] = had_variance
    return result


def _create_line_bookings(
    connection: sqlite3.Connection, invoice: dict[str, Any]
) -> list[str]:
    """Erzeugt je Positionstyp EINE Buchung.

    Provision und Storno-Provision werden bewusst NICHT erzeugt -- sie existieren
    bereits orderbezogen. Alles andere hat keine Automatik und entsteht hier.
    """
    lines = invoice.get("lines") or []
    if isinstance(lines, str):
        lines = json.loads(lines or "[]")
    skip = {parser.POSITION_PROVISION, parser.POSITION_PROVISION_STORNO}
    by_category: dict[str, dict[str, Any]] = {}
    for line in lines:
        key = line.get("position_key") or parser.POSITION_OTHER
        if key in skip:
            continue
        bucket = by_category.setdefault(
            key,
            {
                "label": line.get("label") or key,
                "net_cents": 0,
                "vat_cents": 0,
                "gross_cents": 0,
                "vat_rate": line.get("vat_rate"),
                "count": 0,
            },
        )
        bucket["net_cents"] += int(line.get("net_cents") or 0)
        bucket["vat_cents"] += int(line.get("vat_cents") or 0)
        bucket["gross_cents"] += int(line.get("gross_cents") or 0)
        bucket["count"] += 1

    created: list[str] = []
    for category, bucket in sorted(by_category.items()):
        created.append(
            _insert_transaction(
                connection,
                invoice=invoice,
                category=category,
                tx_type=POSITION_TO_TRANSACTION_TYPE.get(category, "EXPENSE"),
                amount_gross=bucket["gross_cents"],
                amount_net=bucket["net_cents"],
                vat_amount=bucket["vat_cents"],
                vat_rate=bucket["vat_rate"],
                is_vat_deductible=_is_deductible(invoice),
                reference=str(invoice.get("invoice_number") or invoice["id"]),
                notes=f"{bucket['count']} Position(en): {bucket['label']}",
                source_key=f"platform-invoice:{invoice['id']}:{category}",
            )
        )
    return created


def _create_variance_booking(
    connection: sqlite3.Connection,
    invoice: dict[str, Any],
    difference_cents: int,
    booked_cents: int,
) -> str:
    """Korrekturbuchung: Rechnungsbetrag gilt, die Abweichung bleibt sichtbar."""
    return _insert_transaction(
        connection,
        invoice=invoice,
        category=VARIANCE_CATEGORY,
        tx_type="ADJUSTMENT",
        amount_gross=difference_cents,
        amount_net=difference_cents,
        vat_amount=0,
        vat_rate=None,
        is_vat_deductible=False,
        reference=f"Abweichung Sammelrechnung {invoice.get('invoice_number') or invoice['id']}",
        notes=(
            f"Rechnungsbetrag {invoice.get('invoice_amount_cents')} ct gegen gebuchte "
            f"Gebuehren {booked_cents} ct -- Differenz {difference_cents} ct nach "
            f"Rechnung ausgeglichen. Eigene Freigabe erforderlich."
        ),
        source_key=f"platform-invoice:{invoice['id']}:variance",
    )


def _insert_transaction(
    connection: sqlite3.Connection,
    *,
    invoice: dict[str, Any],
    category: str,
    tx_type: str,
    amount_gross: int,
    amount_net: int,
    vat_amount: int,
    vat_rate: Optional[int],
    is_vat_deductible: bool,
    reference: str,
    notes: str,
    source_key: str,
) -> str:
    """Idempotent ueber `source_key` -- erneutes Freigeben bucht nicht doppelt."""
    existing = connection.execute(
        "SELECT id FROM transactions WHERE source_key = ?", (source_key,)
    ).fetchone()
    if existing is not None:
        return str(existing["id"])

    transaction_id = str(uuid.uuid4())
    now = _utc_now()
    day = (
        _iso_day(invoice.get("period_to"))
        or _iso_day(invoice.get("invoice_date"))
        or now[:10]
    )
    connection.execute(
        """
        INSERT OR IGNORE INTO transactions (
            id, date, type, direction, amount_gross, currency,
            vat_rate, vat_amount, amount_net, is_vat_deductible,
            provider, counterparty_name, category, reference, notes,
            order_id, document_id, template_id, payment_account_id, period_key,
            source, source_key, status, booking_class, created_at, updated_at
        ) VALUES (?, ?, ?, 'OUT', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                  NULL, ?, NULL, NULL, ?, 'manual', ?, 'confirmed', 'single', ?, ?)
        """,
        (
            transaction_id,
            f"{day}T00:00:00Z",
            tx_type,
            amount_gross,
            invoice.get("currency") or "EUR",
            vat_rate,
            vat_amount,
            amount_net,
            1 if is_vat_deductible else 0,
            invoice.get("provider") or "other",
            invoice.get("provider") or "other",
            category,
            reference,
            notes,
            invoice.get("document_id"),
            day[:7],
            source_key,
            now,
            now,
        ),
    )
    return transaction_id


def _link_transactions(
    connection: sqlite3.Connection,
    invoice: dict[str, Any],
    created_ids: list[str],
) -> None:
    """Verknuepft die orderbezogene Provision mit der Rechnung (Nachvollziehbarkeit)."""
    period_from = _iso_day(invoice.get("period_from"))
    period_to = _iso_day(invoice.get("period_to"))
    invoice_id = invoice["id"]
    # Nur Sammelrechnungen verknuepfen die orderbezogene Provision. Ein
    # Einzelbeleg hat damit nichts zu tun und darf sie nicht mitzaehlen.
    if period_from and invoice.get("doc_kind") != parser.DOC_KIND_SINGLE:
        rows = connection.execute(
            """
            SELECT id FROM transactions
            WHERE direction = 'OUT'
              AND provider = ?
              AND substr(date, 1, 10) BETWEEN ? AND ?
              AND category IN ('fees', ?, ?)
            """,
            (
                invoice.get("provider") or "",
                period_from,
                period_to or period_from,
                parser.POSITION_PROVISION,
                parser.POSITION_PROVISION_STORNO,
            ),
        ).fetchall()
        for row in rows:
            connection.execute(
                "INSERT OR IGNORE INTO monthly_invoice_transactions "
                "(invoice_id, transaction_id) VALUES (?, ?)",
                (invoice_id, row["id"]),
            )
    for transaction_id in created_ids:
        connection.execute(
            "INSERT OR IGNORE INTO monthly_invoice_transactions "
            "(invoice_id, transaction_id) VALUES (?, ?)",
            (invoice_id, transaction_id),
        )
