from __future__ import annotations

import io
import json
import math
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from app.config import ALLOWED_MARKETPLACES, PROJECT_ROOT, SALES_INVOICES_DIR
from app.db import build_sales_invoice_storage_path, connect_combined_db, now_iso, sanitize_filename
from app.services.orders import _to_kaufland_cents, get_order_detail


DEFAULT_PROFILE_ID = "default"
DEFAULT_TEMPLATE_KEY = "clean"
SUPPORTED_TEMPLATE_KEYS = {"clean", "compact", "brand"}
SUPPORTED_TAX_MODES = {"small_business", "regular"}
EU_COUNTRY_CODES = {
    "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR",
    "DE", "GR", "HU", "IE", "IT", "LV", "LT", "LU", "MT", "NL",
    "PL", "PT", "RO", "SK", "SI", "ES", "SE",
}
SMALL_BUSINESS_NOTE = "Gemaess § 19 UStG wird keine Umsatzsteuer berechnet und daher nicht ausgewiesen."


class InvoiceServiceError(Exception):
    def __init__(self, status_code: int, detail: str, details: Any | None = None):
        super().__init__(detail)
        self.status_code = int(status_code)
        self.detail = detail
        self.details = details


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"))


def _safe_json_load(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    text = str(value or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _safe_json_load_list(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    text = str(value or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except (TypeError, json.JSONDecodeError):
        return []
    if not isinstance(parsed, list):
        return []
    return [item for item in parsed if isinstance(item, dict)]


def _text(value: Any) -> str:
    return str(value or "").strip()


def _upper_token(value: Any, fallback = "") -> str:
    token = _text(value).upper()
    return token or fallback


def _normalize_template_key(value: Any, fallback = DEFAULT_TEMPLATE_KEY) -> str:
    token = _text(value).lower()
    if token in SUPPORTED_TEMPLATE_KEYS:
        return token
    return fallback


def _normalize_tax_mode(value: Any) -> str:
    token = _text(value).lower()
    if token in SUPPORTED_TAX_MODES:
        return token
    return "small_business"


def _normalize_country_code(value: Any, fallback = "DE") -> str:
    token = _upper_token(value)
    if len(token) == 2 and token.isalpha():
        return token
    return fallback


def _to_int(value: Any, default = 0) -> int:
    if value is None or value == "":
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _to_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def _to_money_cents(value: Any) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(round(value * 100))
    text = _text(value).replace(" ", "")
    if not text:
        return 0
    normalized = text.replace(",", ".")
    try:
        if "." in normalized:
            return int(round(float(normalized) * 100))
        return int(normalized)
    except ValueError:
        return 0


def _optional_rate(value: Any) -> float | None:
    parsed = _to_float(value)
    if parsed is None or parsed < 0:
        return None
    return round(parsed, 4)


def _iso_date(value: Any) -> str:
    text = _text(value)
    if not text:
        return datetime.now(timezone.utc).date().isoformat()
    if len(text) >= 10 and text[4:5] == "-" and text[7:8] == "-":
        return text[:10]
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return datetime.now(timezone.utc).date().isoformat()
    return parsed.date().isoformat()


def _display_date(value: str) -> str:
    text = _text(value)
    try:
        parsed = datetime.fromisoformat(text[:10])
    except ValueError:
        return text or "-"
    return parsed.strftime("%d.%m.%Y")


def _currency_text(cents: int, currency = "EUR") -> str:
    amount = cents / 100
    whole = int(abs(amount))
    fraction = int(round((abs(amount) - whole) * 100))
    whole_text = f"{whole:,}".replace(",", ".")
    sign = "-" if amount < 0 else ""
    token = _upper_token(currency, "EUR")
    if token == "EUR":
        return f"{sign}EUR {whole_text},{fraction:02d}"
    return f"{sign}{whole_text},{fraction:02d} {token}"


def _pdf_escape_text(value: str) -> str:
    text = str(value or "")
    escaped = text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
    return escaped.encode("latin-1", errors="replace").decode("latin-1")


def _build_fallback_pdf(lines: list[str]) -> bytes:
    page_width = 595
    page_height = 842
    margin_left = 42
    margin_top = 48
    font_size = 10
    leading = 14
    lines_per_page = max(1, int((page_height - (margin_top * 2)) / leading))

    chunks: list[list[str]] = []
    for index in range(0, len(lines), lines_per_page):
        chunks.append(lines[index : index + lines_per_page])
    if not chunks:
        chunks = [["Rechnung"]]

    objects: list[bytes] = []

    def add_object(body: bytes) -> int:
        objects.append(body)
        return len(objects)

    font_id = add_object(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
    page_tree_id = add_object(b"<< /Type /Pages /Count 0 /Kids [] >>")

    page_ids: list[int] = []
    for chunk in chunks:
        stream_lines = [
            "BT",
            f"/F1 {font_size} Tf",
            f"{leading} TL",
            f"1 0 0 1 {margin_left} {page_height - margin_top} Tm",
        ]
        for idx, line in enumerate(chunk):
            safe_line = _pdf_escape_text(line)
            if idx == 0:
                stream_lines.append(f"({safe_line}) Tj")
            else:
                stream_lines.append("T*")
                stream_lines.append(f"({safe_line}) Tj")
        stream_lines.append("ET")
        stream_payload = "\n".join(stream_lines).encode("latin-1")
        content_id = add_object(
            b"<< /Length "
            + str(len(stream_payload)).encode("ascii")
            + b" >>\nstream\n"
            + stream_payload
            + b"\nendstream"
        )
        page_id = add_object(
            (
                f"<< /Type /Page /Parent {page_tree_id} 0 R "
                f"/MediaBox [0 0 {page_width} {page_height}] "
                f"/Resources << /Font << /F1 {font_id} 0 R >> >> "
                f"/Contents {content_id} 0 R >>"
            ).encode("ascii")
        )
        page_ids.append(page_id)

    kids = " ".join(f"{page_id} 0 R" for page_id in page_ids)
    objects[page_tree_id - 1] = (
        f"<< /Type /Pages /Count {len(page_ids)} /Kids [{kids}] >>".encode("ascii")
    )
    catalog_id = add_object(f"<< /Type /Catalog /Pages {page_tree_id} 0 R >>".encode("ascii"))

    result = bytearray(b"%PDF-1.4\n")
    offsets: list[int] = [0]
    for index, body in enumerate(objects, start=1):
        offsets.append(len(result))
        result.extend(f"{index} 0 obj\n".encode("ascii"))
        result.extend(body)
        result.extend(b"\nendobj\n")
    xref_offset = len(result)
    result.extend(f"xref\n0 {len(objects) + 1}\n".encode("ascii"))
    result.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        result.extend(f"{offset:010d} 00000 n \n".encode("ascii"))
    result.extend(
        (
            "trailer\n"
            f"<< /Size {len(objects) + 1} /Root {catalog_id} 0 R >>\n"
            "startxref\n"
            f"{xref_offset}\n"
            "%%EOF\n"
        ).encode("ascii")
    )
    return bytes(result)


def _build_simple_invoice_lines(draft: dict[str, Any]) -> list[str]:
    seller = draft.get("seller") if isinstance(draft.get("seller"), dict) else {}
    customer = draft.get("customer") if isinstance(draft.get("customer"), dict) else {}
    invoice = draft.get("invoice") if isinstance(draft.get("invoice"), dict) else {}
    totals = draft.get("totals") if isinstance(draft.get("totals"), dict) else {}
    items = draft.get("items") if isinstance(draft.get("items"), list) else []

    def address_lines_from_record(record: dict[str, Any]) -> list[str]:
        return [
            value
            for value in [
                _text(record.get("name")),
                _text(record.get("company")),
                _text(record.get("street")),
                _text(record.get("address_line2")),
                " ".join(part for part in [_text(record.get("postcode")), _text(record.get("city"))] if part),
                _text(record.get("country")),
            ]
            if value
        ]

    regular_items: list[dict[str, Any]] = []
    shipping_total = 0
    adjustment_total = 0
    for item in items:
        if not isinstance(item, dict):
            continue
        meta = item.get("meta") if isinstance(item.get("meta"), dict) else {}
        kind = _text(meta.get("kind")).lower()
        amount = _to_int(item.get("line_total_gross_cents"))
        if kind == "shipping":
            shipping_total += amount
            continue
        if kind == "adjustment":
            adjustment_total += amount
            continue
        regular_items.append(item)

    shipping_cents = shipping_total if shipping_total else max(_to_int(totals.get("shipping_cents")), 0)
    regular_subtotal_cents = sum(_to_int(item.get("line_total_gross_cents")) for item in regular_items)
    total_gross_cents = _to_int(totals.get("gross_cents"))

    currency = _upper_token(invoice.get("currency"), "EUR")
    invoice_number = _text(invoice.get("invoice_number")) or _text(invoice.get("invoice_number_preview"))
    order_number = _text(invoice.get("external_order_id")) or _text(invoice.get("order_id")) or "-"
    seller_address = address_lines_from_record(seller)
    billing_address = customer.get("billing_address") if isinstance(customer.get("billing_address"), dict) else {}
    shipping_address = customer.get("shipping_address") if isinstance(customer.get("shipping_address"), dict) else {}
    billing_lines = address_lines_from_record(billing_address)
    shipping_lines = address_lines_from_record(shipping_address)
    show_shipping = bool(shipping_lines) and shipping_lines != billing_lines

    lines: list[str] = []
    lines.append("Rechnung")
    if invoice_number:
        lines.append(f"Rechnungsnummer: {invoice_number}")
    lines.append(f"Bestellnummer: {order_number}")
    lines.append(f"Ausstellungsdatum: {_display_date(_text(invoice.get('invoice_date')))}")
    lines.append("")
    lines.append("Von")
    lines.append(_text(seller.get("legal_name")) or "-")
    lines.extend(seller_address)
    if _text(seller.get("phone")):
        lines.append(f"Telefon: {_text(seller.get('phone'))}")
    if _text(seller.get("vat_id")):
        lines.append(f"USt-IdNr.: {_text(seller.get('vat_id'))}")
    elif _text(seller.get("tax_number")):
        lines.append(f"Steuernr.: {_text(seller.get('tax_number'))}")
    lines.append("")
    lines.append("Rechnung an")
    if billing_lines:
        lines.extend(billing_lines)
    else:
        lines.append("-")

    if show_shipping:
        lines.append("")
        lines.append("Lieferung an")
        lines.extend(shipping_lines)

    lines.append("")
    lines.append("Bestelldetails")
    lines.append("Menge  Artikel                                              Betrag")
    for item in regular_items:
        quantity = max(_to_int(item.get("quantity"), 1), 1)
        amount_label = _currency_text(_to_int(item.get("line_total_gross_cents")), currency)
        title = _text(item.get("title")) or "Artikel"
        compact_title = title[:50]
        lines.append(f"{quantity:<6}{compact_title:<52}{amount_label:>14}")
    lines.append(f"Zwischensumme{'':<41}{_currency_text(regular_subtotal_cents, currency):>14}")
    lines.append(f"Versand{'':<47}{_currency_text(shipping_cents, currency):>14}")
    if adjustment_total:
        lines.append(f"Rabatt / Ausgleich{'':<33}{_currency_text(adjustment_total, currency):>14}")
    lines.append(f"Gesamtbetrag{'':<42}{_currency_text(total_gross_cents, currency):>14}")

    if _normalize_tax_mode(seller.get("tax_mode")) == "small_business":
        lines.append("")
        lines.append(f"Hinweis: {SMALL_BUSINESS_NOTE}")
    footer_note = _text(seller.get("footer_note"))
    if footer_note:
        lines.append(footer_note)
    if _text(seller.get("email")):
        lines.append(_text(seller.get("email")))
    return lines


def _resolve_sales_invoice_path(raw_path: str) -> Optional[Path]:
    text = _text(raw_path)
    if not text:
        return None
    candidate = Path(text)
    if candidate.is_absolute():
        if candidate.exists() and candidate.is_file():
            return candidate
        fallback = SALES_INVOICES_DIR / candidate.name
        if fallback.exists() and fallback.is_file():
            return fallback
        return None
    resolved = (PROJECT_ROOT / candidate).resolve()
    if resolved.exists() and resolved.is_file():
        return resolved
    fallback = SALES_INVOICES_DIR / candidate.name
    if fallback.exists() and fallback.is_file():
        return fallback
    return None


def _relative_project_path(path: Path) -> str:
    resolved = path.resolve()
    try:
        return resolved.relative_to(PROJECT_ROOT.resolve()).as_posix()
    except ValueError:
        return str(resolved)


def _default_seller_profile() -> dict[str, Any]:
    return {
        "id": DEFAULT_PROFILE_ID,
        "legal_name": "Luis Nöbauer",
        "street": "Bodenwöhrstraße 5",
        "address_line2": "",
        "postcode": "93055",
        "city": "Regensburg",
        "country": "DE",
        "email": "support@blockeria.de",
        "phone": "015254367380",
        "vat_id": "DE458504535",
        "tax_number": "",
        "tax_mode": "small_business",
        "invoice_prefix": "RE",
        "default_template": DEFAULT_TEMPLATE_KEY,
        "footer_note": "Bei Fragen erreichst du uns unter support@blockeria.de.",
        "payment_note": "",
        "eu_invoicing_enabled": False,
        "created_at": "",
        "updated_at": "",
    }


def _normalize_profile_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    source = payload if isinstance(payload, dict) else {}
    normalized = _default_seller_profile()
    normalized.update({
        "legal_name": _text(source.get("legal_name")),
        "street": _text(source.get("street")),
        "address_line2": _text(source.get("address_line2")),
        "postcode": _text(source.get("postcode")),
        "city": _text(source.get("city")),
        "country": _normalize_country_code(source.get("country")),
        "email": _text(source.get("email")),
        "phone": _text(source.get("phone")),
        "vat_id": _upper_token(source.get("vat_id")),
        "tax_number": _text(source.get("tax_number")),
        "tax_mode": _normalize_tax_mode(source.get("tax_mode")),
        "invoice_prefix": sanitize_filename(_upper_token(source.get("invoice_prefix"), "RE"))[:12] or "RE",
        "default_template": _normalize_template_key(source.get("default_template")),
        "footer_note": _text(source.get("footer_note")),
        "payment_note": _text(source.get("payment_note")),
        "eu_invoicing_enabled": bool(source.get("eu_invoicing_enabled")),
    })
    return normalized


def _apply_default_profile_fallbacks(payload: dict[str, Any]) -> dict[str, Any]:
    defaults = _default_seller_profile()
    normalized = dict(payload)
    for key, default_value in defaults.items():
        current = normalized.get(key)
        if isinstance(default_value, bool):
            if current is None:
                normalized[key] = default_value
            continue
        if isinstance(default_value, str) and not _text(current):
            normalized[key] = default_value
    normalized["eu_invoicing_enabled"] = bool(normalized.get("eu_invoicing_enabled"))
    return normalized


def _ensure_default_profile(connection: sqlite3.Connection) -> None:
    timestamp = now_iso()
    connection.execute(
        """
        INSERT OR IGNORE INTO seller_profiles (
            id, legal_name, street, address_line2, postcode, city, country,
            email, phone, vat_id, tax_number, tax_mode, invoice_prefix,
            default_template, footer_note, payment_note, eu_invoicing_enabled,
            created_at, updated_at
        ) VALUES (?, '', '', '', '', '', 'DE', '', '', '', '', 'small_business', 'RE', 'clean', '', '', 0, ?, ?)
        """,
        (DEFAULT_PROFILE_ID, timestamp, timestamp),
    )


def get_seller_profile() -> dict[str, Any]:
    with connect_combined_db() as connection:
        _ensure_default_profile(connection)
        connection.commit()
        row = connection.execute(
            "SELECT * FROM seller_profiles WHERE id = ? LIMIT 1",
            (DEFAULT_PROFILE_ID,),
        ).fetchone()
    if row is None:
        return _default_seller_profile()
    return _apply_default_profile_fallbacks(dict(row))


def save_seller_profile(payload: dict[str, Any] | None) -> dict[str, Any]:
    normalized = _normalize_profile_payload(payload)
    timestamp = now_iso()
    with connect_combined_db() as connection:
        _ensure_default_profile(connection)
        created_row = connection.execute(
            "SELECT created_at FROM seller_profiles WHERE id = ? LIMIT 1",
            (DEFAULT_PROFILE_ID,),
        ).fetchone()
        created_at = _text(created_row["created_at"] if created_row is not None else timestamp) or timestamp
        connection.execute(
            """
            UPDATE seller_profiles
            SET legal_name = ?, street = ?, address_line2 = ?, postcode = ?, city = ?,
                country = ?, email = ?, phone = ?, vat_id = ?, tax_number = ?,
                tax_mode = ?, invoice_prefix = ?, default_template = ?, footer_note = ?,
                payment_note = ?, eu_invoicing_enabled = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                normalized["legal_name"],
                normalized["street"],
                normalized["address_line2"],
                normalized["postcode"],
                normalized["city"],
                normalized["country"],
                normalized["email"],
                normalized["phone"],
                normalized["vat_id"],
                normalized["tax_number"],
                normalized["tax_mode"],
                normalized["invoice_prefix"],
                normalized["default_template"],
                normalized["footer_note"],
                normalized["payment_note"],
                1 if normalized["eu_invoicing_enabled"] else 0,
                timestamp,
                DEFAULT_PROFILE_ID,
            ),
        )
        connection.execute(
            "UPDATE seller_profiles SET created_at = ? WHERE id = ?",
            (created_at, DEFAULT_PROFILE_ID),
        )
        connection.commit()
    saved = get_seller_profile()
    saved["created_at"] = created_at
    return saved


def _address_from_record(address: dict[str, Any] | None, fallback_name = "") -> dict[str, Any]:
    record = address if isinstance(address, dict) else {}
    first_name = _text(record.get("first_name"))
    last_name = _text(record.get("last_name"))
    name = " ".join(part for part in [first_name, last_name] if part).strip()
    if not name:
        name = _text(record.get("name")) or fallback_name
    street = _text(record.get("street"))
    house_number = _text(record.get("house_number"))
    if street and house_number:
        street_line = f"{street} {house_number}".strip()
    else:
        street_line = street or _text(record.get("address1"))
    address_line2 = _text(record.get("address2")) or _text(record.get("additional_field"))
    country = _normalize_country_code(record.get("country_code") or record.get("country"), "")
    if not country:
        country = _upper_token(record.get("country"))
    return {
        "name": name,
        "company": _text(record.get("company")) or _text(record.get("company_name")),
        "street": street_line,
        "address_line2": address_line2,
        "postcode": _text(record.get("postcode")) or _text(record.get("zip")),
        "city": _text(record.get("city")),
        "country": country,
        "phone": _text(record.get("phone")),
    }


def _address_has_data(address: dict[str, Any]) -> bool:
    return any(_text(address.get(key)) for key in ("name", "street", "postcode", "city", "country", "company"))


def _customer_email(detail: dict[str, Any], order_record: dict[str, Any]) -> str:
    customer = detail.get("customer") if isinstance(detail.get("customer"), dict) else {}
    return (
        _text(customer.get("email"))
        or _text(order_record.get("customer_email"))
        or _text(order_record.get("email"))
    )


def _first_item_title(items: list[dict[str, Any]]) -> str:
    for item in items:
        title = _text(item.get("title"))
        if title:
            return title
    return "-"


def _existing_invoice_for_order(connection: sqlite3.Connection, marketplace: str, order_id: str) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT id, invoice_number, created_at
        FROM sales_invoices
        WHERE marketplace = ? AND source_order_id = ?
        LIMIT 1
        """,
        (marketplace, order_id),
    ).fetchone()
    return dict(row) if row is not None else None


def _next_invoice_number(connection: sqlite3.Connection, prefix: str, invoice_date: str) -> str:
    year = invoice_date[:4]
    number_prefix = f"{sanitize_filename(prefix.upper()) or 'RE'}-{year}-"
    rows = connection.execute(
        "SELECT invoice_number FROM sales_invoices WHERE invoice_number LIKE ?",
        (f"{number_prefix}%",),
    ).fetchall()
    highest = 0
    for row in rows:
        invoice_number = _text(row["invoice_number"])
        suffix = invoice_number.removeprefix(number_prefix)
        if suffix.isdigit():
            highest = max(highest, int(suffix))
    return f"{number_prefix}{highest + 1:06d}"


def _template_config(template_key: str) -> dict[str, Any]:
    token = _normalize_template_key(template_key)
    if token == "compact":
        return {
            "key": token,
            "label": "Compact",
            "title_size": 18,
            "section_gap": 18,
        }
    if token == "brand":
        return {
            "key": token,
            "label": "Brand",
            "title_size": 20,
            "section_gap": 22,
        }
    return {
        "key": DEFAULT_TEMPLATE_KEY,
        "label": "Clean",
        "title_size": 19,
        "section_gap": 20,
    }


def _shopify_items(detail: dict[str, Any]) -> tuple[list[dict[str, Any]], int]:
    items_raw = detail.get("line_items") if isinstance(detail.get("line_items"), list) else []
    items: list[dict[str, Any]] = []
    source_tax_cents = 0
    for raw_item in items_raw:
        if not isinstance(raw_item, dict):
            continue
        quantity = max(_to_int(raw_item.get("quantity"), 1), 1)
        unit_price_cents = _to_money_cents(raw_item.get("price"))
        discount_cents = _to_money_cents(raw_item.get("total_discount"))
        title = _text(raw_item.get("title")) or "Artikel"
        variant_title = _text(raw_item.get("variant_title"))
        if variant_title and variant_title.lower() != "default title":
            title = f"{title} / {variant_title}"
        raw_payload = _safe_json_load(raw_item.get("raw_json"))
        tax_lines = raw_payload.get("tax_lines") if isinstance(raw_payload.get("tax_lines"), list) else []
        tax_rate = None
        if tax_lines:
            tax_rate = _optional_rate(_to_float((tax_lines[0] if isinstance(tax_lines[0], dict) else {}).get("rate")) * 100 if _to_float((tax_lines[0] if isinstance(tax_lines[0], dict) else {}).get("rate")) is not None else None)
            for tax_line in tax_lines:
                if not isinstance(tax_line, dict):
                    continue
                source_tax_cents += _to_money_cents(tax_line.get("price"))
        line_total_cents = max((quantity * unit_price_cents) - discount_cents, 0)
        items.append({
            "position": len(items) + 1,
            "sku": _text(raw_item.get("sku")),
            "title": title,
            "quantity": quantity,
            "unit_price_gross_cents": unit_price_cents,
            "line_total_gross_cents": line_total_cents,
            "vat_rate": tax_rate,
            "meta": {
                "variant_title": variant_title,
                "vendor": _text(raw_item.get("vendor")),
                "fulfillment_status": _text(raw_item.get("fulfillment_status")),
                "discount_cents": discount_cents,
            },
        })
    return items, source_tax_cents


def _kaufland_items(detail: dict[str, Any]) -> tuple[list[dict[str, Any]], int]:
    units_raw = detail.get("units") if isinstance(detail.get("units"), list) else []
    items: list[dict[str, Any]] = []
    source_tax_cents = 0
    for raw_unit in units_raw:
        if not isinstance(raw_unit, dict):
            continue
        if _text(raw_unit.get("status")).lower() in {"cancelled", "canceled"}:
            continue
        unit_price_cents = _to_kaufland_cents(raw_unit.get("price")) or 0
        vat_rate = _optional_rate(raw_unit.get("vat"))
        if vat_rate:
            source_tax_cents += int(round(unit_price_cents * (vat_rate / (100 + vat_rate))))
        items.append({
            "position": len(items) + 1,
            "sku": _text(raw_unit.get("product_id_product")) or _text(raw_unit.get("id_offer")),
            "title": _text(raw_unit.get("product_title")) or "Artikel",
            "quantity": 1,
            "unit_price_gross_cents": unit_price_cents,
            "line_total_gross_cents": unit_price_cents,
            "vat_rate": vat_rate,
            "meta": {
                "id_order_unit": _text(raw_unit.get("id_order_unit")),
                "shipping_rate_cents": _to_kaufland_cents(raw_unit.get("shipping_rate")) or 0,
            },
        })
    return items, source_tax_cents


def _with_shipping_and_adjustments(
    items: list[dict[str, Any]],
    *,
    shipping_cents: int,
    target_total_cents: int,
) -> list[dict[str, Any]]:
    normalized = [dict(item) for item in items]
    if shipping_cents > 0:
        normalized.append({
            "position": len(normalized) + 1,
            "sku": "",
            "title": "Versand",
            "quantity": 1,
            "unit_price_gross_cents": shipping_cents,
            "line_total_gross_cents": shipping_cents,
            "vat_rate": None,
            "meta": {"kind": "shipping"},
        })
    current_total = sum(_to_int(item.get("line_total_gross_cents")) for item in normalized)
    diff_cents = target_total_cents - current_total
    if diff_cents != 0:
        normalized.append({
            "position": len(normalized) + 1,
            "sku": "",
            "title": "Rabatt / Ausgleich",
            "quantity": 1,
            "unit_price_gross_cents": diff_cents,
            "line_total_gross_cents": diff_cents,
            "vat_rate": None,
            "meta": {"kind": "adjustment"},
        })
    for index, item in enumerate(normalized, start=1):
        item["position"] = index
    return normalized


def _build_validation(
    *,
    seller: dict[str, Any],
    billing_address: dict[str, Any],
    shipping_address: dict[str, Any],
    items: list[dict[str, Any]],
    marketplace: str,
    detail: dict[str, Any],
    summary: dict[str, Any],
    customer_email: str,
    source_tax_cents: int,
    existing_invoice: dict[str, Any] | None,
) -> tuple[list[str], list[str], str]:
    blockers: list[str] = []
    warnings: list[str] = []
    selected_billing = billing_address if _address_has_data(billing_address) else shipping_address
    billing_source = "billing"
    if selected_billing is shipping_address and _address_has_data(shipping_address):
        billing_source = "shipping"
        warnings.append("Billing-Adresse fehlt oder ist leer. Die Versandadresse wird als Rechnungsadresse verwendet.")
    if not _address_has_data(selected_billing):
        blockers.append("Es liegt keine verwertbare Rechnungsanschrift fuer den Kunden vor.")
    if not items:
        blockers.append("Die Bestellung enthaelt keine fakturierbaren Positionen.")
    required_profile_fields = {
        "legal_name": "Verkaeufername fehlt im Profil.",
        "street": "Verkaeuferstrasse fehlt im Profil.",
        "postcode": "Verkaeufer-PLZ fehlt im Profil.",
        "city": "Verkaeufer-Ort fehlt im Profil.",
        "country": "Verkaeufer-Land fehlt im Profil.",
        "email": "Verkaeufer-E-Mail fehlt im Profil.",
    }
    for key, message in required_profile_fields.items():
        if not _text(seller.get(key)):
            blockers.append(message)
    if not _text(seller.get("vat_id")) and not _text(seller.get("tax_number")):
        blockers.append("Im Verkaeuferprofil wird mindestens Steuernummer oder USt-IdNr. benoetigt.")
    country = _normalize_country_code(selected_billing.get("country"), "")
    if not country:
        blockers.append("Das Zielland des Kunden konnte nicht sicher ermittelt werden.")
    elif country not in EU_COUNTRY_CODES:
        blockers.append("Nur Rechnungen innerhalb Deutschlands und der EU sind in dieser Version freigegeben.")
    elif country != "DE":
        warnings.append("EU-Bestellung erkannt. Bitte pruefen Sie vor der Finalisierung die steuerliche Behandlung manuell.")
    if not _text(selected_billing.get("name")) and not _text(selected_billing.get("company")):
        blockers.append("Der Name des Rechnungsempfaengers fehlt.")
    fulfillment_status = _text(summary.get("fulfillment_status")).lower()
    financial_status = _text(summary.get("financial_status")).lower()
    if any(token in fulfillment_status for token in ("refund", "cancel")) or any(token in financial_status for token in ("refund", "cancel")):
        warnings.append("Die Bestellung enthaelt einen stornierten oder erstatteten Status. Bitte pruefen, ob eine Rechnung oder eher eine Korrektur benoetigt wird.")
    deemed_supplier = False
    order_raw = detail.get("order_raw") if isinstance(detail.get("order_raw"), dict) else {}
    if marketplace == "kaufland":
        deemed_supplier = bool(order_raw.get("is_marketplace_deemed_supplier"))
        warnings.append("Kaufland-Kundenadressen und E-Mails sollten vor dem Versand der Rechnung kurz geprueft werden.")
    if deemed_supplier:
        blockers.append("Kaufland meldet die Bestellung als deemed-supplier-Fall. Dafuer wird in dieser Version keine Kundenrechnung erzeugt.")
    if _normalize_tax_mode(seller.get("tax_mode")) == "small_business" and source_tax_cents > 0:
        warnings.append("Die Quelldaten enthalten Steuerwerte, das Verkaeuferprofil ist aber auf Kleinunternehmer gestellt. Bitte pruefen Sie die Bestellung fachlich vor der Finalisierung.")
    if not customer_email:
        warnings.append("Es liegt keine Kunden-E-Mail vor. Fuer die Rechnung selbst ist das nicht zwingend, fuer den Versand kann sie aber fehlen.")
    if existing_invoice is not None:
        blockers.append(
            f"Fuer diese Bestellung existiert bereits eine Rechnung ({_text(existing_invoice.get('invoice_number')) or _text(existing_invoice.get('id'))})."
        )
    return blockers, warnings, billing_source


def _build_draft_internal(marketplace: str, order_id: str, template_key: str | None = None) -> dict[str, Any]:
    market = _text(marketplace).lower()
    if market not in ALLOWED_MARKETPLACES:
        raise InvoiceServiceError(400, "marketplace must be shopify or kaufland")
    normalized_order_id = _text(order_id)
    if not normalized_order_id:
        raise InvoiceServiceError(400, "order_id is required")

    detail = get_order_detail(market, normalized_order_id)
    if detail is None:
        raise InvoiceServiceError(404, "order not found")

    summary = detail.get("summary") if isinstance(detail.get("summary"), dict) else {}
    seller = get_seller_profile()
    selected_template = _template_config(template_key or seller.get("default_template"))
    order_record = detail.get("order") if isinstance(detail.get("order"), dict) else {}
    fallback_name = _text(summary.get("customer")) or _text((detail.get("customer") if isinstance(detail.get("customer"), dict) else {}).get("name"))
    shipping_address = _address_from_record(detail.get("shipping_address") if isinstance(detail.get("shipping_address"), dict) else {}, fallback_name)
    billing_address = _address_from_record(detail.get("billing_address") if isinstance(detail.get("billing_address"), dict) else {}, fallback_name)
    customer_email = _customer_email(detail, order_record)

    if market == "shopify":
        base_items, source_tax_cents = _shopify_items(detail)
        target_total_cents = _to_int(summary.get("total_cents"))
    else:
        base_items, source_tax_cents = _kaufland_items(detail)
        # Kaufland order summaries expose article gross and shipping separately.
        target_total_cents = _to_int(summary.get("total_cents")) + max(_to_int(summary.get("shipping_cents")), 0)

    items = _with_shipping_and_adjustments(
        base_items,
        shipping_cents=max(_to_int(summary.get("shipping_cents")), 0),
        target_total_cents=max(target_total_cents, 0),
    )
    total_gross_cents = sum(_to_int(item.get("line_total_gross_cents")) for item in items)
    customer_display_name = _text(billing_address.get("name")) or _text(shipping_address.get("name")) or fallback_name or customer_email or "Unbekannt"

    with connect_combined_db() as connection:
        _ensure_default_profile(connection)
        existing_invoice = _existing_invoice_for_order(connection, market, normalized_order_id)
        invoice_number_preview = _next_invoice_number(connection, _text(seller.get("invoice_prefix")) or "RE", datetime.now(timezone.utc).date().isoformat())

    blockers, warnings, billing_source = _build_validation(
        seller=seller,
        billing_address=billing_address,
        shipping_address=shipping_address,
        items=items,
        marketplace=market,
        detail=detail,
        summary=summary,
        customer_email=customer_email,
        source_tax_cents=source_tax_cents,
        existing_invoice=existing_invoice,
    )
    selected_billing = billing_address if billing_source == "billing" else shipping_address
    customer_country = _normalize_country_code(selected_billing.get("country"), "DE")
    invoice_date = datetime.now(timezone.utc).date().isoformat()
    delivery_date = _iso_date(summary.get("order_date"))

    return {
        "invoice": {
            "invoice_number_preview": invoice_number_preview,
            "invoice_date": invoice_date,
            "delivery_date": delivery_date,
            "currency": _upper_token(summary.get("currency"), "EUR"),
            "marketplace": market,
            "order_id": normalized_order_id,
            "external_order_id": _text(summary.get("external_order_id")) or normalized_order_id,
            "tax_treatment": _normalize_tax_mode(seller.get("tax_mode")),
        },
        "template": selected_template,
        "seller": seller,
        "customer": {
            "name": customer_display_name,
            "email": customer_email,
            "billing_address": selected_billing,
            "shipping_address": shipping_address,
            "country": customer_country,
        },
        "order": {
            "marketplace": market,
            "order_id": normalized_order_id,
            "external_order_id": _text(summary.get("external_order_id")) or normalized_order_id,
            "order_date": _text(summary.get("order_date")),
            "status": _text(summary.get("fulfillment_status")),
            "first_article": _first_item_title(items),
        },
        "items": items,
        "totals": {
            "gross_cents": total_gross_cents,
            "shipping_cents": max(_to_int(summary.get("shipping_cents")), 0),
            "source_tax_cents": max(source_tax_cents, 0),
        },
        "validation": {
            "blockers": blockers,
            "warnings": warnings,
            "billing_source": billing_source,
            "ready": not blockers,
        },
        "existing_invoice": existing_invoice,
    }


def build_invoice_draft(marketplace: str, order_id: str, template_key: str | None = None) -> dict[str, Any]:
    return _build_draft_internal(marketplace, order_id, template_key)


def list_invoices(
    *,
    from_date: str | None,
    to_date: str | None,
    marketplace: str | None,
    query: str | None,
    limit: int,
    offset: int,
) -> dict[str, Any]:
    clauses = ["1 = 1"]
    args: list[Any] = []
    from_token = _text(from_date)
    to_token = _text(to_date)
    if from_token:
        clauses.append("invoice_date >= ?")
        args.append(from_token)
    if to_token:
        clauses.append("invoice_date <= ?")
        args.append(to_token)
    market = _text(marketplace).lower()
    if market in ALLOWED_MARKETPLACES:
        clauses.append("marketplace = ?")
        args.append(market)
    search = _text(query).lower()
    if search:
        like = f"%{search}%"
        clauses.append(
            "(LOWER(invoice_number) LIKE ? OR LOWER(source_external_order_id) LIKE ? OR LOWER(customer_name) LIKE ?)"
        )
        args.extend([like, like, like])

    where_clause = " AND ".join(clauses)
    with connect_combined_db() as connection:
        total_row = connection.execute(
            f"SELECT COUNT(*) AS total FROM sales_invoices WHERE {where_clause}",
            tuple(args),
        ).fetchone()
        rows = connection.execute(
            f"""
            SELECT id, marketplace, source_order_id, source_external_order_id, invoice_number,
                   invoice_date, delivery_date, currency, customer_name, customer_country,
                   tax_country, tax_treatment, template_key, total_gross_cents, created_at, updated_at
            FROM sales_invoices
            WHERE {where_clause}
            ORDER BY invoice_date DESC, created_at DESC
            LIMIT ? OFFSET ?
            """,
            (*args, limit, offset),
        ).fetchall()
    items = [dict(row) for row in rows]
    return {
        "total": _to_int(total_row["total"] if total_row is not None else 0),
        "items": items,
        "limit": limit,
        "offset": offset,
    }


def get_invoice(invoice_id: str) -> dict[str, Any] | None:
    normalized_id = _text(invoice_id)
    if not normalized_id:
        return None
    with connect_combined_db() as connection:
        invoice_row = connection.execute(
            "SELECT * FROM sales_invoices WHERE id = ? LIMIT 1",
            (normalized_id,),
        ).fetchone()
        if invoice_row is None:
            return None
        item_rows = connection.execute(
            "SELECT * FROM sales_invoice_items WHERE invoice_id = ? ORDER BY position ASC",
            (normalized_id,),
        ).fetchall()
    payload = dict(invoice_row)
    payload["seller_snapshot"] = _safe_json_load(payload.get("seller_snapshot_json"))
    payload["customer_snapshot"] = _safe_json_load(payload.get("customer_snapshot_json"))
    payload["totals_snapshot"] = _safe_json_load(payload.get("totals_snapshot_json"))
    payload["validation_snapshot"] = _safe_json_load(payload.get("validation_snapshot_json"))
    payload["items"] = [
        {
            **dict(row),
            "meta": _safe_json_load(dict(row).get("meta_json")),
        }
        for row in item_rows
    ]
    return payload


def _render_invoice_pdf_bytes(draft: dict[str, Any], *, is_final: bool) -> bytes:
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.utils import simpleSplit
        from reportlab.pdfgen import canvas
    except ModuleNotFoundError:
        return _build_fallback_pdf(_build_simple_invoice_lines(draft))

    page_width, page_height = A4
    margin = 42
    top = page_height - margin
    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=A4)
    template = draft.get("template") if isinstance(draft.get("template"), dict) else _template_config(DEFAULT_TEMPLATE_KEY)
    title_size = int(template.get("title_size") or 19)
    section_gap = int(template.get("section_gap") or 20)
    seller = draft.get("seller") if isinstance(draft.get("seller"), dict) else {}
    customer = draft.get("customer") if isinstance(draft.get("customer"), dict) else {}
    invoice = draft.get("invoice") if isinstance(draft.get("invoice"), dict) else {}
    totals = draft.get("totals") if isinstance(draft.get("totals"), dict) else {}
    items = draft.get("items") if isinstance(draft.get("items"), list) else []

    def next_page() -> float:
        pdf.showPage()
        return top

    def ensure_space(y: float, height: float) -> float:
        if y - height < 70:
            return next_page()
        return y

    def draw_wrapped(x: float, y: float, text: str, width: float, size: int, *, leading: float | None = None, bold: bool = False, fill_rgb: tuple[float, float, float] | None = None) -> float:
        lines = simpleSplit(_text(text) or "-", "Helvetica-Bold" if bold else "Helvetica", size, width)
        line_height = leading or (size + 3)
        if fill_rgb is not None:
            pdf.setFillColorRGB(*fill_rgb)
        else:
            pdf.setFillColorRGB(0, 0, 0)
        pdf.setFont("Helvetica-Bold" if bold else "Helvetica", size)
        for line in lines:
            pdf.drawString(x, y, line)
            y -= line_height
        return y

    def section_title(y: float, title: str) -> float:
        pdf.setFillColorRGB(0, 0, 0)
        pdf.setFont("Helvetica-Bold", 10)
        pdf.drawString(margin, y, title)
        y -= 5
        pdf.setLineWidth(0.7)
        pdf.line(margin, y, page_width - margin, y)
        return y - 14

    def address_block(x: float, y: float, title: str, lines: list[str], width: float) -> float:
        pdf.setFont("Helvetica-Bold", 10)
        pdf.drawString(x, y, title)
        y -= 14
        for line in lines:
            y = draw_wrapped(x, y, line, width, 9)
        return y

    def differing_address_lines() -> tuple[list[str], bool]:
        shipping_address = customer.get("shipping_address") if isinstance(customer.get("shipping_address"), dict) else {}
        billing_address = customer.get("billing_address") if isinstance(customer.get("billing_address"), dict) else {}
        shipping_lines = addressLinesFromRecord(shipping_address)
        billing_lines = addressLinesFromRecord(billing_address)
        return shipping_lines, shipping_lines != billing_lines

    def addressLinesFromRecord(record: dict[str, Any]) -> list[str]:
        return [
            value
            for value in [
                _text(record.get("name")),
                _text(record.get("company")),
                _text(record.get("street")),
                _text(record.get("address_line2")),
                " ".join(part for part in [_text(record.get("postcode")), _text(record.get("city"))] if part),
                _text(record.get("country")),
            ]
            if value
        ]

    regular_items: list[dict[str, Any]] = []
    shipping_lines_total = 0
    adjustment_total = 0
    for item in items:
        meta = item.get("meta") if isinstance(item.get("meta"), dict) else {}
        kind = _text(meta.get("kind")).lower()
        amount = _to_int(item.get("line_total_gross_cents"))
        if kind == "shipping":
            shipping_lines_total += amount
            continue
        if kind == "adjustment":
            adjustment_total += amount
            continue
        regular_items.append(item)

    shipping_cents = shipping_lines_total if shipping_lines_total else max(_to_int(totals.get("shipping_cents")), 0)
    regular_subtotal_cents = sum(_to_int(item.get("line_total_gross_cents")) for item in regular_items)
    total_gross_cents = _to_int(totals.get("gross_cents"))
    invoice_number = _text(invoice.get("invoice_number")) or _text(invoice.get("invoice_number_preview"))
    seller_lines = [line for line in [_text(seller.get("legal_name")), *addressLinesFromRecord(seller)] if line]
    seller_phone = _text(seller.get("phone"))
    seller_vat_id = _text(seller.get("vat_id"))
    seller_tax_number = _text(seller.get("tax_number"))
    if seller_phone:
        seller_lines.append(f"Telefon: {seller_phone}")
    if seller_vat_id:
        seller_lines.append(f"USt-IdNr.: {seller_vat_id}")
    elif seller_tax_number:
        seller_lines.append(f"Steuernr.: {seller_tax_number}")
    billing_address = customer.get("billing_address") if isinstance(customer.get("billing_address"), dict) else {}
    billing_lines = addressLinesFromRecord(billing_address)
    shipping_lines, show_shipping_address = differing_address_lines()

    y = top
    pdf.setFillColorRGB(0, 0, 0)
    pdf.setFont("Helvetica-Bold", title_size)
    pdf.drawString(margin, y, "Rechnung")
    pdf.setFont("Helvetica", 9)
    y -= 16
    pdf.drawString(margin, y, f"Bestellnummer: {_text(invoice.get('external_order_id')) or _text(invoice.get('order_id')) or '-'}")
    y -= 13
    pdf.drawString(margin, y, f"Ausstellungsdatum: {_display_date(_text(invoice.get('invoice_date')))}")
    if invoice_number:
        y -= 13
        pdf.drawString(margin, y, f"Rechnungsnummer: {invoice_number}")
    y -= section_gap

    left_width = (page_width - (margin * 2) - 24) / 2
    left_y = address_block(margin, y, "Von", seller_lines, left_width)
    right_y = address_block(margin + left_width + 24, y, "Rechnung an", billing_lines or ["-"], left_width)
    y = min(left_y, right_y) - section_gap

    if show_shipping_address:
        y = ensure_space(y, 80)
        y = address_block(margin, y, "Lieferung an", shipping_lines or ["-"], page_width - (margin * 2))
        y -= section_gap

    y = ensure_space(y, 130)
    y = section_title(y, "Bestelldetails")
    pdf.setFont("Helvetica-Bold", 9)
    pdf.drawString(margin, y, "Menge")
    pdf.drawString(margin + 55, y, "Artikel")
    pdf.drawRightString(page_width - margin, y, "Betrag")
    y -= 8
    pdf.setLineWidth(0.5)
    pdf.line(margin, y, page_width - margin, y)
    y -= 14

    for item in regular_items:
        y = ensure_space(y, 40)
        pdf.setFillColorRGB(0, 0, 0)
        pdf.setFont("Helvetica", 9)
        pdf.drawString(margin, y, str(max(_to_int(item.get("quantity"), 1), 1)))
        description = _text(item.get("title")) or "Artikel"
        lines = simpleSplit(description, "Helvetica", 9, page_width - (margin * 2) - 155)
        pdf.drawRightString(page_width - margin, y, _currency_text(_to_int(item.get("line_total_gross_cents")), _text(invoice.get("currency")) or "EUR"))
        line_y = y
        for line in lines[:3]:
            pdf.drawString(margin + 55, line_y, line)
            line_y -= 11
        y = line_y - 3

    y -= 3
    pdf.setLineWidth(0.5)
    pdf.line(margin, y, page_width - margin, y)
    y -= 16

    y = ensure_space(y, 100)
    pdf.setFont("Helvetica", 9)
    pdf.drawString(margin, y, "Zwischensumme")
    pdf.drawRightString(page_width - margin, y, _currency_text(regular_subtotal_cents, _text(invoice.get("currency")) or "EUR"))
    y -= 14
    pdf.drawString(margin, y, "Versand")
    pdf.drawRightString(page_width - margin, y, _currency_text(shipping_cents, _text(invoice.get("currency")) or "EUR"))
    if adjustment_total:
        y -= 14
        pdf.drawString(margin, y, "Rabatt / Ausgleich")
        pdf.drawRightString(page_width - margin, y, _currency_text(adjustment_total, _text(invoice.get("currency")) or "EUR"))
    y -= 8
    pdf.line(margin, y, page_width - margin, y)
    y -= 16
    pdf.setFont("Helvetica-Bold", 10)
    pdf.drawString(margin, y, "Gesamtbetrag")
    pdf.drawRightString(page_width - margin, y, _currency_text(total_gross_cents, _text(invoice.get("currency")) or "EUR"))
    y -= 18

    pdf.setFont("Helvetica", 9)
    if _normalize_tax_mode(seller.get("tax_mode")) == "small_business":
        y = draw_wrapped(margin, y, f"Hinweis: {SMALL_BUSINESS_NOTE}", page_width - (margin * 2), 9, leading=12)
    if _text(seller.get("footer_note")):
        y -= 6
        y = draw_wrapped(margin, y, _text(seller.get("footer_note")), page_width - (margin * 2), 9, leading=12)

    pdf.setFont("Helvetica", 8)
    pdf.setFillColorRGB(0, 0, 0)
    footer_left = _text(seller.get("email"))
    if footer_left:
        pdf.drawString(margin, 28, footer_left)
    pdf.drawRightString(page_width - margin, 28, _text(invoice.get("marketplace")).capitalize() if _text(invoice.get("marketplace")) else "")
    pdf.save()
    buffer.seek(0)
    return buffer.getvalue()


def build_preview_pdf(marketplace: str, order_id: str, template_key: str | None = None) -> bytes:
    draft = _build_draft_internal(marketplace, order_id, template_key)
    return _render_invoice_pdf_bytes(draft, is_final=False)


def create_invoice(marketplace: str, order_id: str, template_key: str | None = None) -> dict[str, Any]:
    draft = _build_draft_internal(marketplace, order_id, template_key)
    validation = draft.get("validation") if isinstance(draft.get("validation"), dict) else {}
    blockers = validation.get("blockers") if isinstance(validation.get("blockers"), list) else []
    if blockers:
        raise InvoiceServiceError(400, "invoice draft contains blockers", {"blockers": blockers})

    seller = draft.get("seller") if isinstance(draft.get("seller"), dict) else {}
    customer = draft.get("customer") if isinstance(draft.get("customer"), dict) else {}
    invoice = draft.get("invoice") if isinstance(draft.get("invoice"), dict) else {}
    totals = draft.get("totals") if isinstance(draft.get("totals"), dict) else {}
    items = draft.get("items") if isinstance(draft.get("items"), list) else []
    now = now_iso()
    invoice_id = str(uuid.uuid4())
    invoice_date = _text(invoice.get("invoice_date")) or datetime.now(timezone.utc).date().isoformat()
    pdf_path: Path | None = None

    try:
        with connect_combined_db() as connection:
            _ensure_default_profile(connection)
            if _existing_invoice_for_order(connection, _text(invoice.get("marketplace")), _text(invoice.get("order_id"))) is not None:
                raise InvoiceServiceError(409, "invoice for this order already exists")
            invoice_number = _next_invoice_number(connection, _text(seller.get("invoice_prefix")) or "RE", invoice_date)
            draft_invoice = dict(invoice)
            draft_invoice["invoice_number"] = invoice_number
            draft["invoice"] = draft_invoice
            pdf_bytes = _render_invoice_pdf_bytes(draft, is_final=True)
            pdf_path = build_sales_invoice_storage_path(invoice_number)
            pdf_path.write_bytes(pdf_bytes)

            connection.execute(
                """
                INSERT INTO sales_invoices (
                    id, marketplace, source_order_id, source_external_order_id, invoice_number,
                    invoice_date, delivery_date, currency, customer_name, customer_country,
                    tax_country, tax_treatment, template_key, total_gross_cents,
                    seller_snapshot_json, customer_snapshot_json, totals_snapshot_json,
                    validation_snapshot_json, notes, pdf_path, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    invoice_id,
                    _text(draft_invoice.get("marketplace")),
                    _text(draft_invoice.get("order_id")),
                    _text(draft_invoice.get("external_order_id")),
                    invoice_number,
                    invoice_date,
                    _text(draft_invoice.get("delivery_date")),
                    _upper_token(draft_invoice.get("currency"), "EUR"),
                    _text(customer.get("name")) or "Unbekannt",
                    _normalize_country_code(customer.get("country"), "DE"),
                    _normalize_country_code(customer.get("country"), "DE"),
                    _normalize_tax_mode(seller.get("tax_mode")),
                    _text((draft.get("template") if isinstance(draft.get("template"), dict) else {}).get("key")) or DEFAULT_TEMPLATE_KEY,
                    _to_int(totals.get("gross_cents")),
                    _json_dumps(seller),
                    _json_dumps(customer),
                    _json_dumps(totals),
                    _json_dumps(validation),
                    _text(seller.get("footer_note")),
                    _relative_project_path(pdf_path),
                    now,
                    now,
                ),
            )
            for position, item in enumerate(items, start=1):
                connection.execute(
                    """
                    INSERT INTO sales_invoice_items (
                        id, invoice_id, position, sku, title, quantity,
                        unit_price_gross_cents, line_total_gross_cents, vat_rate, meta_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(uuid.uuid4()),
                        invoice_id,
                        position,
                        _text(item.get("sku")),
                        _text(item.get("title")) or "Artikel",
                        max(_to_int(item.get("quantity"), 1), 1),
                        _to_int(item.get("unit_price_gross_cents")),
                        _to_int(item.get("line_total_gross_cents")),
                        _optional_rate(item.get("vat_rate")),
                        _json_dumps(item.get("meta") if isinstance(item.get("meta"), dict) else {}),
                    ),
                )
            connection.commit()
    except sqlite3.IntegrityError as exc:
        if pdf_path is not None:
            pdf_path.unlink(missing_ok=True)
        raise InvoiceServiceError(409, "invoice for this order already exists") from exc
    except Exception:
        if pdf_path is not None:
            pdf_path.unlink(missing_ok=True)
        raise

    created = get_invoice(invoice_id)
    if created is None:
        raise InvoiceServiceError(500, "invoice could not be loaded after creation")
    return created


def get_invoice_pdf_response_payload(invoice_id: str) -> tuple[Path, str]:
    invoice = get_invoice(invoice_id)
    if invoice is None:
        raise InvoiceServiceError(404, "invoice not found")
    file_path = _resolve_sales_invoice_path(_text(invoice.get("pdf_path")))
    if file_path is None:
        raise InvoiceServiceError(404, "invoice pdf not found")
    download_name = sanitize_filename(f"{_text(invoice.get('invoice_number')) or invoice_id}.pdf")
    return file_path, download_name
