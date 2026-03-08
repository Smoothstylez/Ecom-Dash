from __future__ import annotations

import csv
import io
import json
import logging
import os
import shutil
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Optional
from zipfile import ZIP_DEFLATED, BadZipFile, ZipFile

from app.config import (
    ALLOWED_MARKETPLACES,
    BOOKKEEPING_DB_PATH,
    BOOKKEEPING_DOCUMENTS_DIR,
    COMBINED_DB_PATH,
    DATA_DIR,
    INVOICES_DIR,
    KAUFLAND_DB_PATH,
    PROJECT_ROOT,
    SHOPIFY_DB_PATH,
    STORAGE_DIR,
)
from app.db import fetch_enrichment_map, sanitize_filename
from app.services.bookkeeping_full import get_document_resolved_path, list_bookkeeping_transactions
from app.services.orders import list_all_orders_without_pagination

LOGGER = logging.getLogger(__name__)


@dataclass
class ExportArchive:
    file_path: Path
    download_name: str
    summary: dict[str, Any]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _iso_utc(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _timestamp_token(value: datetime) -> str:
    return value.strftime("%Y%m%dT%H%M%SZ")


def _parse_date_token(value: Optional[str], field_name: str) -> date:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field_name} is required (YYYY-MM-DD)")
    try:
        return datetime.fromisoformat(text).date()
    except ValueError as exc:
        raise ValueError(f"{field_name} must use YYYY-MM-DD") from exc


def _validate_period(from_date: Optional[str], to_date: Optional[str]) -> tuple[str, str]:
    start = _parse_date_token(from_date, "from")
    end = _parse_date_token(to_date, "to")
    if start > end:
        raise ValueError("from must be before or equal to to")
    return start.isoformat(), end.isoformat()


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _cents_to_eur(value: Any) -> str:
    cents = _safe_int(value)
    return f"{cents / 100:.2f}"


def _normalize_marketplace_filter(value: Optional[str]) -> Optional[str]:
    token = str(value or "").strip().lower()
    if not token:
        return None
    if token not in ALLOWED_MARKETPLACES:
        raise ValueError("marketplace must be shopify or kaufland")
    return token


def _create_temp_zip_path(prefix: str) -> Path:
    handle = tempfile.NamedTemporaryFile(prefix=prefix, suffix=".zip", delete=False)
    path = Path(handle.name)
    handle.close()
    return path


def cleanup_temp_export(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError:
        return


def _write_csv_to_zip(zip_file: ZipFile, arcname: str, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, delimiter=";", lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow({key: _safe_text(row.get(key)) for key in fieldnames})
    zip_file.writestr(arcname, buffer.getvalue().encode("utf-8-sig"))


def _directory_file_entries(root: Path) -> list[Path]:
    if not root.exists() or not root.is_dir():
        return []
    return sorted([path for path in root.rglob("*") if path.is_file()])


def _add_directory_to_zip(zip_file: ZipFile, source_dir: Path, arc_prefix: str) -> int:
    files = _directory_file_entries(source_dir)
    for file_path in files:
        relative = file_path.relative_to(source_dir).as_posix()
        zip_file.write(file_path, f"{arc_prefix}/{relative}")
    return len(files)


def _unique_arcname(candidate: str, used_paths: set[str]) -> str:
    pure = PurePosixPath(candidate)
    clean_candidate = pure.as_posix()
    if clean_candidate not in used_paths:
        used_paths.add(clean_candidate)
        return clean_candidate

    suffix = pure.suffix
    stem = pure.stem
    parent = "" if str(pure.parent) == "." else pure.parent.as_posix()
    index = 2
    while True:
        name = f"{stem}-{index}{suffix}"
        attempt = f"{parent}/{name}" if parent else name
        if attempt not in used_paths:
            used_paths.add(attempt)
            return attempt
        index += 1


def _resolve_combined_invoice_path(raw_path: str) -> Optional[Path]:
    text = str(raw_path or "").strip()
    if not text:
        return None

    candidate = Path(text)
    if candidate.is_absolute():
        return candidate

    project_based = (PROJECT_ROOT / candidate).resolve()
    if project_based.exists():
        return project_based

    invoice_based = (INVOICES_DIR / candidate.name).resolve()
    if invoice_based.exists():
        return invoice_based

    return project_based


def _tx_matches_query(tx: dict[str, Any], needle: str) -> bool:
    order = tx.get("order") if isinstance(tx.get("order"), dict) else {}
    document = tx.get("document") if isinstance(tx.get("document"), dict) else {}
    haystack = " ".join(
        [
            _safe_text(tx.get("id")),
            _safe_text(tx.get("type")),
            _safe_text(tx.get("provider")),
            _safe_text(tx.get("counterparty_name")),
            _safe_text(tx.get("reference")),
            _safe_text(tx.get("notes")),
            _safe_text(order.get("external_order_id")),
            _safe_text(document.get("original_filename")),
        ]
    ).lower()
    return needle in haystack


def create_full_backup_archive() -> ExportArchive:
    timestamp = _utc_now()
    token = _timestamp_token(timestamp)
    archive_path = _create_temp_zip_path("combined-full-backup-")

    db_entries = [
        ("combined", COMBINED_DB_PATH),
        ("shopify", SHOPIFY_DB_PATH),
        ("kaufland", KAUFLAND_DB_PATH),
        ("bookkeeping", BOOKKEEPING_DB_PATH),
    ]

    db_manifest: list[dict[str, Any]] = []
    invoices_count = 0
    bookkeeping_docs_count = 0

    with ZipFile(archive_path, mode="w", compression=ZIP_DEFLATED) as zip_file:
        for name, db_path in db_entries:
            exists = db_path.exists() and db_path.is_file()
            db_manifest.append(
                {
                    "name": name,
                    "path": str(db_path),
                    "exists": exists,
                }
            )
            if exists:
                zip_file.write(db_path, f"databases/{name}.sqlite3")

        invoices_count = _add_directory_to_zip(zip_file, INVOICES_DIR, "storage/invoices")
        bookkeeping_docs_count = _add_directory_to_zip(zip_file, BOOKKEEPING_DOCUMENTS_DIR, "storage/documents")

        manifest = {
            "kind": "full_backup",
            "generated_at": _iso_utc(timestamp),
            "databases": db_manifest,
            "documents": {
                "invoice_files": invoices_count,
                "bookkeeping_files": bookkeeping_docs_count,
            },
        }

        zip_file.writestr("manifest.json", json.dumps(manifest, ensure_ascii=True, indent=2).encode("utf-8"))
        zip_file.writestr(
            "README.txt",
            (
                "Combined Dashboard Full Backup\n"
                "\n"
                "Contains:\n"
                "- databases/: combined + source databases\n"
                "- storage/invoices/: order invoice files\n"
                "- storage/documents/: bookkeeping document files\n"
                "- manifest.json: snapshot metadata\n"
            ).encode("utf-8"),
        )

    return ExportArchive(
        file_path=archive_path,
        download_name=f"combined_full_backup_{token}.zip",
        summary={
            "generated_at": _iso_utc(timestamp),
            "database_count": len([entry for entry in db_manifest if entry.get("exists")]),
            "invoice_files": invoices_count,
            "bookkeeping_files": bookkeeping_docs_count,
        },
    )


def create_period_export_archive(
    *,
    from_date: Optional[str],
    to_date: Optional[str],
    marketplace: Optional[str],
    query: Optional[str],
) -> ExportArchive:
    normalized_from, normalized_to = _validate_period(from_date, to_date)
    market_filter = _normalize_marketplace_filter(marketplace)
    query_filter = str(query or "").strip()
    query_needle = query_filter.lower()

    timestamp = _utc_now()
    token = _timestamp_token(timestamp)
    archive_path = _create_temp_zip_path("combined-period-export-")

    orders = list_all_orders_without_pagination(
        from_date=normalized_from,
        to_date=normalized_to,
        marketplace=market_filter,
        query=query_filter or None,
    )

    tx_payload = list_bookkeeping_transactions(
        {
            "dateFrom": normalized_from,
            "dateTo": normalized_to,
            "provider": market_filter,
            "type": None,
            "direction": None,
            "hasDocument": None,
            "orderId": None,
            "templateId": None,
            "paymentAccountId": None,
        }
    )
    transactions = tx_payload.get("items") if isinstance(tx_payload.get("items"), list) else []
    if query_needle:
        transactions = [tx for tx in transactions if _tx_matches_query(tx, query_needle)]

    enrichment_map = fetch_enrichment_map()

    used_arc_paths: set[str] = set(
        {
            "README.txt",
            "manifest.json",
            "data/overview.csv",
            "data/orders.csv",
            "data/order_documents.csv",
            "data/bookkeeping_transactions.csv",
            "data/bookkeeping_documents.csv",
        }
    )
    file_transfers: list[tuple[Path, str]] = []

    order_rows: list[dict[str, Any]] = []
    order_document_rows: list[dict[str, Any]] = []

    for order in orders:
        market = str(order.get("marketplace") or "").strip().lower()
        order_id = str(order.get("order_id") or "").strip()
        external_order_id = str(order.get("external_order_id") or order_id).strip()
        join_key = f"{market}:{external_order_id}" if market and external_order_id else ""

        enrichment = enrichment_map.get((market, order_id), {})
        invoice_document_id = str(enrichment.get("invoice_document_id") or "").strip()
        invoice_original_filename = str(enrichment.get("original_filename") or "").strip()
        invoice_file_path = str(enrichment.get("file_path") or "").strip()

        invoice_export_path = ""
        invoice_exists = False
        resolved_invoice_path = _resolve_combined_invoice_path(invoice_file_path)
        if invoice_document_id and resolved_invoice_path is not None and resolved_invoice_path.exists() and resolved_invoice_path.is_file():
            invoice_exists = True
            safe_market = sanitize_filename(market or "market")
            safe_order = sanitize_filename(external_order_id or order_id or "order")
            safe_invoice_name = sanitize_filename(invoice_original_filename or resolved_invoice_path.name)
            invoice_export_path = _unique_arcname(
                f"belege/orders/{safe_market}/{safe_order}/{safe_invoice_name}",
                used_arc_paths,
            )
            file_transfers.append((resolved_invoice_path, invoice_export_path))

        order_rows.append(
            {
                "order_join_key": join_key,
                "marketplace": market,
                "order_id": order_id,
                "external_order_id": external_order_id,
                "order_date": order.get("order_date"),
                "customer": order.get("customer"),
                "article": order.get("article"),
                "fulfillment_status": order.get("fulfillment_status"),
                "payment_method": order.get("payment_method"),
                "currency": order.get("currency") or "EUR",
                "total_cents": _safe_int(order.get("total_cents")),
                "total_eur": _cents_to_eur(order.get("total_cents")),
                "fees_cents": _safe_int(order.get("fees_cents")),
                "fees_eur": _cents_to_eur(order.get("fees_cents")),
                "after_fees_cents": _safe_int(order.get("after_fees_cents")),
                "after_fees_eur": _cents_to_eur(order.get("after_fees_cents")),
                "purchase_cost_cents": _safe_int(order.get("purchase_cost_cents")),
                "purchase_cost_eur": _cents_to_eur(order.get("purchase_cost_cents")),
                "purchase_currency": order.get("purchase_currency") or "EUR",
                "purchase_supplier": order.get("purchase_supplier"),
                "purchase_notes": order.get("purchase_notes"),
                "profit_cents": _safe_int(order.get("profit_cents")),
                "profit_eur": _cents_to_eur(order.get("profit_cents")),
                "invoice_document_id": invoice_document_id,
                "invoice_original_filename": invoice_original_filename,
                "invoice_export_path": invoice_export_path,
            }
        )

        if invoice_document_id:
            order_document_rows.append(
                {
                    "order_join_key": join_key,
                    "marketplace": market,
                    "order_id": order_id,
                    "external_order_id": external_order_id,
                    "invoice_document_id": invoice_document_id,
                    "original_filename": invoice_original_filename,
                    "source_file_path": invoice_file_path,
                    "file_exists": "1" if invoice_exists else "0",
                    "export_path": invoice_export_path,
                }
            )

    bookkeeping_document_map: dict[str, dict[str, Any]] = {}
    bookkeeping_transaction_rows: list[dict[str, Any]] = []

    for tx in transactions:
        order_info = tx.get("order") if isinstance(tx.get("order"), dict) else {}
        document_info = tx.get("document") if isinstance(tx.get("document"), dict) else {}

        order_provider = str(order_info.get("provider") or "").strip().lower()
        order_external_order_id = str(order_info.get("external_order_id") or "").strip()
        order_join_key = f"{order_provider}:{order_external_order_id}" if order_provider and order_external_order_id else ""

        document_id = str(tx.get("document_id") or "").strip()
        document_export_path = ""
        if document_id:
            doc_entry = bookkeeping_document_map.get(document_id)
            if doc_entry is None:
                raw_doc_path = str(document_info.get("file_path") or "").strip()
                resolved_doc_path: Optional[Path] = None
                if raw_doc_path:
                    try:
                        resolved_doc_path = get_document_resolved_path(raw_doc_path)
                    except OSError:
                        resolved_doc_path = None

                doc_exists = bool(resolved_doc_path and resolved_doc_path.exists() and resolved_doc_path.is_file())
                original_name = str(document_info.get("original_filename") or document_info.get("stored_filename") or f"document-{document_id}")
                safe_doc_name = sanitize_filename(original_name)
                export_path = ""
                if doc_exists and resolved_doc_path is not None:
                    safe_doc_id = sanitize_filename(document_id)
                    export_path = _unique_arcname(
                        f"belege/bookkeeping/{safe_doc_id}_{safe_doc_name}",
                        used_arc_paths,
                    )
                    file_transfers.append((resolved_doc_path, export_path))

                doc_entry = {
                    "document_id": document_id,
                    "original_filename": original_name,
                    "stored_filename": str(document_info.get("stored_filename") or ""),
                    "mime_type": str(document_info.get("mime_type") or ""),
                    "uploaded_at": str(document_info.get("uploaded_at") or ""),
                    "source_file_path": raw_doc_path,
                    "file_exists": "1" if doc_exists else "0",
                    "export_path": export_path,
                }
                bookkeeping_document_map[document_id] = doc_entry

            if doc_entry is not None:
                document_export_path = str(doc_entry.get("export_path") or "")

        bookkeeping_transaction_rows.append(
            {
                "transaction_id": tx.get("id"),
                "date": tx.get("date"),
                "type": tx.get("type"),
                "direction": tx.get("direction"),
                "provider": tx.get("provider"),
                "counterparty_name": tx.get("counterparty_name"),
                "category": tx.get("category"),
                "reference": tx.get("reference"),
                "notes": tx.get("notes"),
                "status": tx.get("status"),
                "currency": tx.get("currency") or "EUR",
                "amount_gross_cents": _safe_int(tx.get("amount_gross")),
                "amount_gross_eur": _cents_to_eur(tx.get("amount_gross")),
                "order_db_id": tx.get("order_id"),
                "order_provider": order_provider,
                "order_external_order_id": order_external_order_id,
                "order_join_key": order_join_key,
                "document_id": document_id,
                "document_original_filename": document_info.get("original_filename"),
                "document_export_path": document_export_path,
                "template_id": tx.get("template_id"),
                "payment_account_id": tx.get("payment_account_id"),
                "source": tx.get("source"),
                "source_key": tx.get("source_key"),
            }
        )

    bookkeeping_document_rows = sorted(
        bookkeeping_document_map.values(),
        key=lambda item: (str(item.get("uploaded_at") or ""), str(item.get("document_id") or "")),
        reverse=True,
    )

    overview_rows = [
        {
            "metric": "orders_rows",
            "value": len(order_rows),
        },
        {
            "metric": "order_documents_rows",
            "value": len(order_document_rows),
        },
        {
            "metric": "bookkeeping_transactions_rows",
            "value": len(bookkeeping_transaction_rows),
        },
        {
            "metric": "bookkeeping_documents_rows",
            "value": len(bookkeeping_document_rows),
        },
        {
            "metric": "included_files",
            "value": len(file_transfers),
        },
    ]

    file_transfers_sorted = sorted(file_transfers, key=lambda item: item[1])

    with ZipFile(archive_path, mode="w", compression=ZIP_DEFLATED) as zip_file:
        _write_csv_to_zip(zip_file, "data/overview.csv", overview_rows, ["metric", "value"])
        _write_csv_to_zip(
            zip_file,
            "data/orders.csv",
            order_rows,
            [
                "order_join_key",
                "marketplace",
                "order_id",
                "external_order_id",
                "order_date",
                "customer",
                "article",
                "fulfillment_status",
                "payment_method",
                "currency",
                "total_cents",
                "total_eur",
                "fees_cents",
                "fees_eur",
                "after_fees_cents",
                "after_fees_eur",
                "purchase_cost_cents",
                "purchase_cost_eur",
                "purchase_currency",
                "purchase_supplier",
                "purchase_notes",
                "profit_cents",
                "profit_eur",
                "invoice_document_id",
                "invoice_original_filename",
                "invoice_export_path",
            ],
        )
        _write_csv_to_zip(
            zip_file,
            "data/order_documents.csv",
            order_document_rows,
            [
                "order_join_key",
                "marketplace",
                "order_id",
                "external_order_id",
                "invoice_document_id",
                "original_filename",
                "source_file_path",
                "file_exists",
                "export_path",
            ],
        )
        _write_csv_to_zip(
            zip_file,
            "data/bookkeeping_transactions.csv",
            bookkeeping_transaction_rows,
            [
                "transaction_id",
                "date",
                "type",
                "direction",
                "provider",
                "counterparty_name",
                "category",
                "reference",
                "notes",
                "status",
                "currency",
                "amount_gross_cents",
                "amount_gross_eur",
                "order_db_id",
                "order_provider",
                "order_external_order_id",
                "order_join_key",
                "document_id",
                "document_original_filename",
                "document_export_path",
                "template_id",
                "payment_account_id",
                "source",
                "source_key",
            ],
        )
        _write_csv_to_zip(
            zip_file,
            "data/bookkeeping_documents.csv",
            bookkeeping_document_rows,
            [
                "document_id",
                "original_filename",
                "stored_filename",
                "mime_type",
                "uploaded_at",
                "source_file_path",
                "file_exists",
                "export_path",
            ],
        )

        for source_path, arcname in file_transfers_sorted:
            if source_path.exists() and source_path.is_file():
                zip_file.write(source_path, arcname)

        manifest = {
            "kind": "period_export",
            "generated_at": _iso_utc(timestamp),
            "filters": {
                "from": normalized_from,
                "to": normalized_to,
                "marketplace": market_filter,
                "q": query_filter or None,
            },
            "counts": {
                "orders": len(order_rows),
                "order_documents": len(order_document_rows),
                "bookkeeping_transactions": len(bookkeeping_transaction_rows),
                "bookkeeping_documents": len(bookkeeping_document_rows),
                "files_included": len(file_transfers_sorted),
            },
        }
        zip_file.writestr("manifest.json", json.dumps(manifest, ensure_ascii=True, indent=2).encode("utf-8"))
        zip_file.writestr(
            "README.txt",
            (
                "Combined Dashboard Period Export\n"
                "\n"
                "Purpose:\n"
                "- Provide a date-scoped export with clear order/document mapping.\n"
                "\n"
                "Files:\n"
                "- data/overview.csv\n"
                "- data/orders.csv\n"
                "- data/order_documents.csv\n"
                "- data/bookkeeping_transactions.csv\n"
                "- data/bookkeeping_documents.csv\n"
                "\n"
                "Join Logic:\n"
                "- Use order_join_key (marketplace:external_order_id) to match orders and transactions.\n"
                "- document_export_path / export_path point to included files under belege/.\n"
                "\n"
                "Format:\n"
                "- CSV uses semicolon delimiter and UTF-8 BOM for direct Excel opening.\n"
            ).encode("utf-8"),
        )

    return ExportArchive(
        file_path=archive_path,
        download_name=f"combined_period_export_{normalized_from}_{normalized_to}_{token}.zip",
        summary={
            "generated_at": _iso_utc(timestamp),
            "orders": len(order_rows),
            "bookkeeping_transactions": len(bookkeeping_transaction_rows),
            "files_included": len(file_transfers_sorted),
        },
    )


# ── Restore from full backup ──────────────────────────────────────────

# Map archive database names to their runtime file paths.
_DB_RESTORE_MAP: dict[str, Path] = {
    "combined": COMBINED_DB_PATH,
    "shopify": SHOPIFY_DB_PATH,
    "kaufland": KAUFLAND_DB_PATH,
    "bookkeeping": BOOKKEEPING_DB_PATH,
}

# Map archive storage prefixes to their runtime directories.
_STORAGE_RESTORE_MAP: dict[str, Path] = {
    "storage/invoices": INVOICES_DIR,
    "storage/documents": BOOKKEEPING_DOCUMENTS_DIR,
}


@dataclass
class RestoreResult:
    success: bool
    summary: dict[str, Any]


def _validate_backup_manifest(manifest_raw: bytes) -> dict[str, Any]:
    """Parse and validate the manifest from a backup ZIP."""
    try:
        manifest = json.loads(manifest_raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ValueError(f"Ungueltige manifest.json: {exc}") from exc

    if not isinstance(manifest, dict):
        raise ValueError("manifest.json muss ein JSON-Objekt sein")

    kind = str(manifest.get("kind") or "").strip()
    if kind != "full_backup":
        raise ValueError(
            f"Nur Vollbackups koennen wiederhergestellt werden (kind={kind!r}, erwartet: 'full_backup')"
        )

    return manifest


def _create_pre_restore_safety_backup() -> Path:
    """Create a safety backup of all current databases and storage before restore."""
    timestamp = _utc_now()
    token = _timestamp_token(timestamp)
    safety_dir = Path(tempfile.mkdtemp(prefix="pre-restore-safety-"))
    safety_zip_path = safety_dir / f"pre_restore_safety_{token}.zip"

    with ZipFile(safety_zip_path, mode="w", compression=ZIP_DEFLATED) as zf:
        for name, db_path in _DB_RESTORE_MAP.items():
            if db_path.exists() and db_path.is_file():
                zf.write(db_path, f"databases/{name}.sqlite3")

        _add_directory_to_zip(zf, INVOICES_DIR, "storage/invoices")
        _add_directory_to_zip(zf, BOOKKEEPING_DOCUMENTS_DIR, "storage/documents")

        safety_manifest = {
            "kind": "pre_restore_safety",
            "generated_at": _iso_utc(timestamp),
            "reason": "automatic safety backup before restore",
        }
        zf.writestr("manifest.json", json.dumps(safety_manifest, indent=2).encode("utf-8"))

    # Move safety ZIP to a persistent location inside the project data dir
    safety_dest_dir = DATA_DIR / "restore_safety"
    safety_dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = safety_dest_dir / safety_zip_path.name
    shutil.move(str(safety_zip_path), str(dest_path))
    # Clean up the temp dir
    try:
        safety_dir.rmdir()
    except OSError:
        pass

    return dest_path


def _atomic_file_replace(source: Path, target: Path) -> None:
    """Copy source to target atomically (write to .tmp, then os.replace)."""
    target.parent.mkdir(parents=True, exist_ok=True)
    temp_path = target.with_suffix(f"{target.suffix}.tmp")
    try:
        shutil.copy2(source, temp_path)
        os.replace(temp_path, target)
    except PermissionError:
        # Fallback: direct copy (Windows file-locking edge case)
        if temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                pass
        shutil.copy2(source, target)


def _restore_databases_from_zip(zf: ZipFile, extract_dir: Path) -> dict[str, Any]:
    """Extract and replace runtime database files from the backup ZIP."""
    db_results: dict[str, Any] = {}

    for db_name, target_path in _DB_RESTORE_MAP.items():
        arc_path = f"databases/{db_name}.sqlite3"
        entry = {
            "target": str(target_path),
            "restored": False,
            "reason": "not_in_archive",
        }

        if arc_path not in zf.namelist():
            db_results[db_name] = entry
            continue

        # Extract to temp dir first
        extracted = extract_dir / arc_path
        zf.extract(arc_path, extract_dir)

        if not extracted.exists() or not extracted.is_file():
            entry["reason"] = "extraction_failed"
            db_results[db_name] = entry
            continue

        try:
            _atomic_file_replace(extracted, target_path)
            entry["restored"] = True
            entry["reason"] = "ok"
            entry["size_bytes"] = target_path.stat().st_size
        except Exception as exc:
            entry["reason"] = f"replace_failed: {type(exc).__name__}: {exc}"

        db_results[db_name] = entry

    return db_results


def _restore_storage_from_zip(zf: ZipFile, extract_dir: Path) -> dict[str, Any]:
    """Extract and restore storage files (invoices, documents) from the backup ZIP."""
    storage_results: dict[str, Any] = {}

    all_names = zf.namelist()

    for arc_prefix, target_dir in _STORAGE_RESTORE_MAP.items():
        matching = [n for n in all_names if n.startswith(arc_prefix + "/") and not n.endswith("/")]
        entry: dict[str, Any] = {
            "target_dir": str(target_dir),
            "files_in_archive": len(matching),
            "files_restored": 0,
            "errors": [],
        }

        if not matching:
            storage_results[arc_prefix] = entry
            continue

        # Ensure target dir exists
        target_dir.mkdir(parents=True, exist_ok=True)

        for arc_name in matching:
            # Compute the relative path within the storage directory
            relative = arc_name[len(arc_prefix) + 1:]  # strip "storage/invoices/"
            if not relative:
                continue

            dest_file = target_dir / relative
            try:
                zf.extract(arc_name, extract_dir)
                extracted = extract_dir / arc_name
                if extracted.exists() and extracted.is_file():
                    dest_file.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(extracted, dest_file)
                    entry["files_restored"] += 1
                else:
                    entry["errors"].append(f"extraction_missing: {arc_name}")
            except Exception as exc:
                entry["errors"].append(f"{arc_name}: {type(exc).__name__}: {exc}")

        storage_results[arc_prefix] = entry

    return storage_results


def restore_from_backup_archive(zip_file_path: Path) -> RestoreResult:
    """
    Restore from a full backup ZIP archive.

    Steps:
    1. Validate the ZIP and its manifest (must be kind=full_backup)
    2. Stop live sync background worker
    3. Create a safety backup of current state
    4. Extract and replace databases (atomic file replacement)
    5. Extract and restore storage files (invoices, documents)
    6. Restart live sync background worker
    7. Return detailed summary
    """
    timestamp = _utc_now()

    # ── 1. Validate the ZIP ──
    try:
        zf = ZipFile(zip_file_path, "r")
    except BadZipFile as exc:
        return RestoreResult(
            success=False,
            summary={"error": f"Ungueltige ZIP-Datei: {exc}", "timestamp": _iso_utc(timestamp)},
        )

    try:
        if "manifest.json" not in zf.namelist():
            return RestoreResult(
                success=False,
                summary={"error": "manifest.json fehlt in der ZIP-Datei", "timestamp": _iso_utc(timestamp)},
            )

        try:
            manifest = _validate_backup_manifest(zf.read("manifest.json"))
        except ValueError as exc:
            return RestoreResult(
                success=False,
                summary={"error": str(exc), "timestamp": _iso_utc(timestamp)},
            )

        # ── 2. Stop live sync ──
        from app.services.live_sync import start_live_sync_background_worker, stop_live_sync_background_worker

        LOGGER.info("Restore: stopping live sync background worker...")
        stop_live_sync_background_worker(timeout_seconds=10.0)

        # ── 3. Safety backup ──
        LOGGER.info("Restore: creating pre-restore safety backup...")
        try:
            safety_path = _create_pre_restore_safety_backup()
            LOGGER.info("Restore: safety backup created at %s", safety_path)
        except Exception as exc:
            LOGGER.exception("Restore: safety backup failed, aborting restore: %s", exc)
            # Restart live sync even on failure
            start_live_sync_background_worker()
            return RestoreResult(
                success=False,
                summary={
                    "error": f"Sicherheitsbackup fehlgeschlagen: {type(exc).__name__}: {exc}",
                    "timestamp": _iso_utc(timestamp),
                },
            )

        # ── 4 + 5. Extract to temp dir and restore ──
        extract_dir = Path(tempfile.mkdtemp(prefix="restore-extract-"))
        try:
            LOGGER.info("Restore: extracting and replacing databases...")
            db_results = _restore_databases_from_zip(zf, extract_dir)

            LOGGER.info("Restore: extracting and restoring storage files...")
            storage_results = _restore_storage_from_zip(zf, extract_dir)
        finally:
            # Clean up extraction temp dir
            try:
                shutil.rmtree(extract_dir, ignore_errors=True)
            except Exception:
                pass

        # ── 6. Restart live sync ──
        LOGGER.info("Restore: restarting live sync background worker...")
        start_live_sync_background_worker()

        # ── 7. Summary ──
        db_restored_count = sum(1 for v in db_results.values() if v.get("restored"))
        storage_restored_count = sum(v.get("files_restored", 0) for v in storage_results.values())
        storage_error_count = sum(len(v.get("errors", [])) for v in storage_results.values())

        summary = {
            "timestamp": _iso_utc(timestamp),
            "backup_generated_at": manifest.get("generated_at", "unknown"),
            "safety_backup_path": str(safety_path),
            "databases": db_results,
            "databases_restored": db_restored_count,
            "databases_total": len(_DB_RESTORE_MAP),
            "storage": storage_results,
            "storage_files_restored": storage_restored_count,
            "storage_errors": storage_error_count,
        }

        all_dbs_ok = all(v.get("restored") or v.get("reason") == "not_in_archive" for v in db_results.values())
        success = all_dbs_ok and storage_error_count == 0

        LOGGER.info(
            "Restore complete: success=%s, dbs=%d/%d, storage_files=%d, errors=%d",
            success,
            db_restored_count,
            len(_DB_RESTORE_MAP),
            storage_restored_count,
            storage_error_count,
        )

        return RestoreResult(success=success, summary=summary)

    finally:
        zf.close()
