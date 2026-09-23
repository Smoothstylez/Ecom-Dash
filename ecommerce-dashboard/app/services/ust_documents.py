"""Eingangsrechnungs-Ledger fuer den monatlichen USt-Report.

Periodisierung der Vorsteuer (UStAE): Der Abzug faellt in den ERSTEN Zeitraum,
in dem Leistung UND ordnungsgemaesse Rechnung beide vorliegen.

    service_month = month(service_date)              # echtes Liefer-/Leistungsdatum
                  or month(period_to or period_from) # Monatsgebuehren: Leistungsende
                  or month(invoice_date)             # letzte Notloesung
    docs_month    = month(received_date or invoice_date)
    deduction_month = max(service_month, docs_month)

Das Rechnungsdatum gilt NICHT als Leistungsdatum: Lieferantenrechnung vom 28.08.
bei Warenlieferung am 03.09. ergibt Vorsteuer im September.
"""
from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

from app.config import BOOKKEEPING_DOCUMENTS_DIR
from app.db import connect_combined_db, sanitize_filename

PROVIDERS: frozenset[str] = frozenset({"amazon", "kaufland", "other"})
DOC_TYPES: frozenset[str] = frozenset({"fee", "purchase", "damage_compensation", "other"})
INPUT_VAT_STATUSES: frozenset[str] = frozenset(
    {"review_required", "confirmed", "rejected", "non_deductible"}
)

_COLUMNS = (
    "id", "provider", "doc_type", "invoice_number", "invoice_date", "received_date",
    "service_date", "period_from", "period_to", "currency", "gross_cents", "net_cents",
    "vat_cents", "deductible_vat_cents", "input_vat_status", "deduction_month", "source",
    "document_path", "sha256", "notes", "created_at",
)


class UstDocumentError(Exception):
    def __init__(self, status_code: int, detail: str) -> None:
        self.status_code = status_code
        self.detail = detail
        super().__init__(detail)


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _text(value: Any) -> str:
    return str(value or "").strip()


def _month_token(value: Any) -> str:
    token = _text(value)
    if not token:
        return ""
    for format_string in ("%Y-%m-%d", "%Y-%m"):
        try:
            return datetime.strptime(token[: len(format_string) + 2], format_string).strftime("%Y-%m")
        except ValueError:
            continue
    return ""


def resolve_deduction_month(
    *,
    invoice_date: Any,
    received_date: Any = None,
    service_date: Any = None,
    delivery_date: Any = None,
    period_from: Any = None,
    period_to: Any = None,
) -> str:
    """Erster Monat mit Leistung UND ordnungsgemaessem Beleg (UStAE)."""
    service_hint = _text(service_date) or _text(delivery_date)
    if service_hint:
        service_month = _month_token(service_hint)
    else:
        service_month = (
            _month_token(period_to) or _month_token(period_from) or _month_token(invoice_date)
        )
    docs_month = _month_token(received_date) or _month_token(invoice_date)
    return max(service_month, docs_month)


def _stable_id(prefix: str, value: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"ecom-dash:{prefix}:{value}"))


def _row_to_dict(row: Any) -> dict[str, Any]:
    return {key: row[key] for key in _COLUMNS}


def _int(payload: dict[str, Any], key: str) -> int:
    try:
        return int(payload.get(key) or 0)
    except (TypeError, ValueError):
        raise UstDocumentError(400, f"{key} muss eine ganze Zahl in Cent sein") from None


def _validate_payload(payload: dict[str, Any]) -> dict[str, Any]:
    provider = _text(payload.get("provider")).lower()
    doc_type = _text(payload.get("doc_type")).lower()
    invoice_number = _text(payload.get("invoice_number"))
    invoice_date = _text(payload.get("invoice_date"))
    if provider not in PROVIDERS:
        raise UstDocumentError(400, "provider muss amazon, kaufland oder other sein")
    if doc_type not in DOC_TYPES:
        raise UstDocumentError(400, "doc_type muss fee, purchase, damage_compensation oder other sein")
    if not invoice_number:
        raise UstDocumentError(400, "invoice_number fehlt")
    if not invoice_date:
        raise UstDocumentError(400, "invoice_date fehlt")

    gross_cents = _int(payload, "gross_cents")
    net_cents = _int(payload, "net_cents")
    vat_cents = _int(payload, "vat_cents")
    deductible_vat_cents = _int(payload, "deductible_vat_cents")
    if min(gross_cents, net_cents, vat_cents, deductible_vat_cents) < 0:
        raise UstDocumentError(400, "Betrage duerfen nicht negativ sein")
    if gross_cents != net_cents + vat_cents:
        raise UstDocumentError(400, "gross_cents muss net_cents + vat_cents ergeben")
    if deductible_vat_cents > vat_cents:
        raise UstDocumentError(400, "deductible_vat_cents darf vat_cents nicht uebersteigen")
    if doc_type == "damage_compensation" and vat_cents != 0:
        raise UstDocumentError(400, "nicht steuerbarer Schadensersatz darf keine Umsatzsteuer tragen")

    status = _text(payload.get("input_vat_status")).lower() or (
        "non_deductible" if doc_type == "damage_compensation" else "review_required"
    )
    if status not in INPUT_VAT_STATUSES:
        raise UstDocumentError(400, "unzulaessiger input_vat_status")

    received_date = _text(payload.get("received_date")) or _text(payload.get("invoice_date"))
    service_date = _text(payload.get("service_date")) or _text(payload.get("delivery_date"))
    period_from = _text(payload.get("period_from"))
    period_to = _text(payload.get("period_to"))

    return {
        "provider": provider,
        "doc_type": doc_type,
        "invoice_number": invoice_number,
        "invoice_date": invoice_date,
        "received_date": received_date,
        "service_date": service_date,
        "period_from": period_from,
        "period_to": period_to,
        "currency": _text(payload.get("currency")) or "EUR",
        "gross_cents": gross_cents,
        "net_cents": net_cents,
        "vat_cents": vat_cents,
        "deductible_vat_cents": deductible_vat_cents,
        "input_vat_status": status,
        "source": _text(payload.get("source")) or "manual",
        "notes": _text(payload.get("notes")),
        "deduction_month": resolve_deduction_month(
            invoice_date=invoice_date,
            received_date=received_date,
            service_date=service_date,
            period_from=period_from,
            period_to=period_to,
        ),
    }


def _write_document(invoice_id: str, file_bytes: bytes, filename: str) -> tuple[str, str]:
    digest = hashlib.sha256(file_bytes).hexdigest()
    safe_name = sanitize_filename(filename or "beleg")
    target = BOOKKEEPING_DOCUMENTS_DIR / "ust-documents" / invoice_id / f"{uuid.uuid4().hex}-{safe_name}"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(file_bytes)
    return str(target), digest


def _remove_document(document_path: str) -> None:
    """Loescht die Beleg-Datei samt leerem Rechnungsordner."""
    if not document_path:
        return
    path = Path(document_path)
    path.unlink(missing_ok=True)
    folder = path.parent
    try:
        folder.rmdir()
        folder.parent.rmdir()
    except OSError:
        pass


def save_input_vat_invoice(
    payload: dict[str, Any],
    *,
    file_bytes: Optional[bytes] = None,
    filename: str = "",
) -> dict[str, Any]:
    values = _validate_payload(payload)
    invoice_id = _stable_id(
        "ust-invoice", f"{values['provider']}:{values['invoice_number']}"
    )
    document_path = ""
    sha256 = ""
    try:
        if file_bytes is not None:
            if not file_bytes:
                raise UstDocumentError(400, "Beleg-Datei ist leer")
            document_path, sha256 = _write_document(invoice_id, file_bytes, filename)
            with connect_combined_db() as connection:
                duplicate = connection.execute(
                    "SELECT id FROM input_vat_invoices WHERE sha256 = ? AND id != ?",
                    (sha256, invoice_id),
                ).fetchone()
                if duplicate is not None:
                    raise UstDocumentError(409, "Beleg-Datei wurde bereits gespeichert")

        with connect_combined_db() as connection:
            connection.execute(
                """
                INSERT INTO input_vat_invoices(
                    id, provider, doc_type, invoice_number, invoice_date, received_date,
                    service_date, period_from, period_to, currency, gross_cents, net_cents,
                    vat_cents, deductible_vat_cents, input_vat_status, deduction_month,
                    source, document_path, sha256, notes, created_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    invoice_id, values["provider"], values["doc_type"], values["invoice_number"],
                    values["invoice_date"], values["received_date"], values["service_date"],
                    values["period_from"], values["period_to"], values["currency"],
                    values["gross_cents"], values["net_cents"], values["vat_cents"],
                    values["deductible_vat_cents"], values["input_vat_status"],
                    values["deduction_month"], values["source"], document_path, sha256,
                    values["notes"], _utc_now(),
                ),
            )
    except UstDocumentError:
        _remove_document(document_path)
        raise
    except Exception as exc:
        _remove_document(document_path)
        if "UNIQUE" in str(exc):
            raise UstDocumentError(409, "Rechnungsnummer fuer diesen Anbieter bereits vorhanden") from exc
        raise UstDocumentError(400, f"Rechnung konnte nicht gespeichert werden: {exc}") from exc

    return get_input_vat_invoice(invoice_id)


def get_input_vat_invoice(invoice_id: str) -> dict[str, Any]:
    with connect_combined_db() as connection:
        row = connection.execute(
            f"SELECT {', '.join(_COLUMNS)} FROM input_vat_invoices WHERE id = ?", (invoice_id,)
        ).fetchone()
    if row is None:
        raise UstDocumentError(404, "Rechnung nicht gefunden")
    return _row_to_dict(row)


def list_input_vat_invoices(
    *,
    deduction_month: Optional[str] = None,
    provider: Optional[str] = None,
    status: Optional[str] = None,
    month: Optional[str] = None,
) -> list[dict[str, Any]]:
    """Gefilterte Liste; `month` ist ein Alias fuer `deduction_month`."""
    selected_month = deduction_month or month
    clauses: list[str] = []
    params: list[Any] = []
    if selected_month:
        clauses.append("deduction_month = ?")
        params.append(selected_month)
    if provider:
        clauses.append("provider = ?")
        params.append(provider)
    if status:
        clauses.append("input_vat_status = ?")
        params.append(status)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with connect_combined_db() as connection:
        rows = connection.execute(
            f"SELECT {', '.join(_COLUMNS)} FROM input_vat_invoices {where} "
            "ORDER BY deduction_month ASC, invoice_date ASC, invoice_number ASC",
            params,
        ).fetchall()
    return [_row_to_dict(row) for row in rows]


def update_input_vat_invoice(invoice_id: str, fields: dict[str, Any]) -> dict[str, Any]:
    current = get_input_vat_invoice(invoice_id)
    allowed = {"input_vat_status", "received_date", "service_date", "delivery_date", "notes"}
    unknown = set(fields) - allowed
    if unknown:
        raise UstDocumentError(400, f"unzulaessige Felder: {', '.join(sorted(unknown))}")

    updates: dict[str, Any] = {}
    if "input_vat_status" in fields:
        status = _text(fields.get("input_vat_status")).lower()
        if status not in INPUT_VAT_STATUSES:
            raise UstDocumentError(400, "unzulaessiger input_vat_status")
        updates["input_vat_status"] = status
    for key in ("received_date", "notes"):
        if key in fields:
            updates[key] = _text(fields.get(key))
    if "service_date" in fields or "delivery_date" in fields:
        updates["service_date"] = _text(fields.get("service_date")) or _text(fields.get("delivery_date"))

    merged = {**current, **updates}
    updates["deduction_month"] = resolve_deduction_month(
        invoice_date=merged["invoice_date"],
        received_date=merged["received_date"],
        service_date=merged["service_date"],
        period_from=merged["period_from"],
        period_to=merged["period_to"],
    )

    assignments = ", ".join(f"{key} = ?" for key in updates)
    with connect_combined_db() as connection:
        connection.execute(
            f"UPDATE input_vat_invoices SET {assignments} WHERE id = ?",
            [*updates.values(), invoice_id],
        )
    return get_input_vat_invoice(invoice_id)


def set_input_vat_status(
    invoice_id: str, status: str, *, received_date: Optional[str] = None
) -> dict[str, Any]:
    fields: dict[str, Any] = {"input_vat_status": status}
    if received_date is not None:
        fields["received_date"] = received_date
    return update_input_vat_invoice(invoice_id, fields)


def get_document_file(invoice_id: str) -> tuple[Path, str]:
    row = get_input_vat_invoice(invoice_id)
    document_path = _text(row.get("document_path"))
    if not document_path:
        raise UstDocumentError(404, "Kein Beleg zu dieser Rechnung gespeichert")
    path = Path(document_path)
    if not path.exists():
        raise UstDocumentError(404, "Beleg-Datei fehlt auf der Festplatte")
    return path, path.name.split("-", 1)[-1] if "-" in path.name else path.name


def sum_input_vat_by_deduction_month(month: str) -> dict[str, int]:
    """Vorsteuerbloecke je Abzugsmonat; nur freigegebene Rechnungen sind abziehbar."""
    totals = {
        "purchases_cents": 0,
        "amazon_fees_cents": 0,
        "kaufland_fees_cents": 0,
        "other_cents": 0,
        "pending_review_cents": 0,
        "pending_review_count": 0,
        "nontaxable_cents": 0,
    }
    for row in list_input_vat_invoices(deduction_month=month):
        doc_type = row["doc_type"]
        provider = row["provider"]
        status = row["input_vat_status"]
        if status == "review_required":
            totals["pending_review_cents"] += int(row["vat_cents"])
            totals["pending_review_count"] += 1
        if doc_type == "damage_compensation":
            totals["nontaxable_cents"] += int(row["gross_cents"])
            continue
        if status != "confirmed":
            continue
        deductible = int(row["deductible_vat_cents"])
        if doc_type == "purchase":
            totals["purchases_cents"] += deductible
        elif doc_type == "fee" and provider == "amazon":
            totals["amazon_fees_cents"] += deductible
        elif doc_type == "fee" and provider == "kaufland":
            totals["kaufland_fees_cents"] += deductible
        else:
            totals["other_cents"] += deductible
    return totals


def add_input_vat_invoice_lines(invoice_id: str, lines: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    get_input_vat_invoice(invoice_id)
    saved: list[dict[str, Any]] = []
    with connect_combined_db() as connection:
        for index, line in enumerate(lines, start=1):
            line_id = _stable_id("ust-invoice-line", f"{invoice_id}:{index}:{_text(line.get('label'))}")
            connection.execute(
                "INSERT INTO input_vat_invoice_lines(id,invoice_id,label,category,gross_cents,net_cents,vat_cents) "
                "VALUES (?,?,?,?,?,?,?)",
                (
                    line_id,
                    invoice_id,
                    _text(line.get("label")) or f"position-{index}",
                    _text(line.get("category")) or "fee_deductible",
                    _int(line, "gross_cents"),
                    _int(line, "net_cents"),
                    _int(line, "vat_cents"),
                ),
            )
            saved.append({"id": line_id, **{k: line.get(k) for k in ("label", "category")}})
    return saved
