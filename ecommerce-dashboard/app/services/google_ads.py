from __future__ import annotations

import csv
import io
import json
import unicodedata
import uuid
from collections import defaultdict
from datetime import datetime
from typing import Any, Optional

from app.db import connect_combined_db, now_iso
from app.services.orders import list_all_orders_without_pagination


def _normalize_text(value: Any) -> str:
    text = str(value or "").strip()
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch)).strip()


def _normalize_key(value: Any) -> str:
    text = _normalize_text(value).lower()
    return "".join(ch for ch in text if ch.isalnum())


def _normalize_article_id(value: Any) -> str:
    text = _normalize_text(value).lower().strip()
    if not text or text in {"-", "--", "na", "n/a"}:
        return ""
    return text


def _normalize_title_key(value: Any) -> str:
    token = _normalize_key(value)
    return f"title:{token}" if token else ""


def _safe_money_to_cents(value: Any) -> int:
    raw = _normalize_text(value)
    if not raw or raw in {"-", "--"}:
        return 0
    compact = raw.replace(" ", "")
    if compact.startswith("(") and compact.endswith(")"):
        compact = f"-{compact[1:-1]}"
    normalized = compact.replace("€", "").replace("$", "").replace("£", "")
    if "," in normalized and "." in normalized:
        normalized = normalized.replace(".", "").replace(",", ".")
    else:
        normalized = normalized.replace(",", ".")
    try:
        amount = float(normalized)
    except ValueError:
        return 0
    return int(round(amount * 100))


def _decode_csv_bytes(content: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return content.decode(encoding)
        except UnicodeDecodeError:
            continue
    return content.decode("utf-8", errors="ignore")


def _detect_delimiter(text: str) -> str:
    sample = "\n".join(text.splitlines()[:8])
    comma = sample.count(",")
    semicolon = sample.count(";")
    return ";" if semicolon > comma else ","


def _csv_rows(text: str) -> list[list[str]]:
    delimiter = _detect_delimiter(text)
    reader = csv.reader(io.StringIO(text), delimiter=delimiter)
    rows: list[list[str]] = []
    for row in reader:
        if not row:
            continue
        if len(row) == 1 and not _normalize_text(row[0]):
            continue
        rows.append([str(cell) for cell in row])
    return rows


def _find_header_row(rows: list[list[str]], candidates: set[str]) -> int:
    for index, row in enumerate(rows[:24]):
        for cell in row:
            if _normalize_key(cell) in candidates:
                return index
    return -1


def _parse_assignment_csv(content: bytes) -> dict[str, Any]:
    text = _decode_csv_bytes(content)
    rows = _csv_rows(text)
    header_index = _find_header_row(rows, {"title", "produkt", "artikel", "id", "shopifyid"})
    if header_index < 0:
        raise ValueError("Assignment CSV: Kopfzeile nicht gefunden.")

    headers = rows[header_index]
    header_keys = [_normalize_key(value) for value in headers]
    title_idx = next((i for i, key in enumerate(header_keys) if key in {"title", "produkt", "artikel"}), -1)
    id_idx = next((i for i, key in enumerate(header_keys) if key == "id" or "shopifyid" in key), -1)

    if id_idx < 0:
        raise ValueError("Assignment CSV: ID-Spalte fehlt.")

    assignments: dict[str, dict[str, str]] = {}
    for row in rows[header_index + 1 :]:
        raw_id = row[id_idx] if id_idx < len(row) else ""
        article_id = _normalize_article_id(raw_id)
        if not article_id:
            continue
        title = _normalize_text(row[title_idx] if title_idx >= 0 and title_idx < len(row) else "")
        product_key = _normalize_title_key(title)
        if not product_key:
            product_key = f"id:{_normalize_key(article_id)}"
        label = title or article_id
        detail = f"{title} | {article_id}" if title else article_id
        assignments[article_id] = {
            "article_id": article_id,
            "product_title": title,
            "product_key": product_key,
            "product_label": label,
            "product_detail": detail,
        }

    return {
        "rows": list(assignments.values()),
        "count": len(assignments),
    }


def _parse_ads_report_csv(content: bytes) -> dict[str, Any]:
    text = _decode_csv_bytes(content)
    rows = _csv_rows(text)
    header_index = _find_header_row(rows, {"artikelid", "articleid"})
    if header_index < 0:
        raise ValueError("Google Ads CSV: Kopfzeile mit Artikel-ID nicht gefunden.")

    headers = rows[header_index]
    header_keys = [_normalize_key(value) for value in headers]
    id_idx = next((i for i, key in enumerate(header_keys) if key in {"artikelid", "articleid"}), -1)
    currency_idx = next(
        (
            i
            for i, key in enumerate(header_keys)
            if key in {"wahrungscode", "waehrungscode", "currencycode", "currency"}
        ),
        -1,
    )
    if id_idx < 0:
        raise ValueError("Google Ads CSV: Artikel-ID Spalte fehlt.")

    date_columns: list[tuple[int, str]] = []
    for idx, header in enumerate(headers):
        token = _normalize_text(header)
        if "_" not in token:
            continue
        day_part = token.split("_", 1)[0].strip()
        try:
            day = datetime.strptime(day_part, "%Y-%m-%d").date().isoformat()
        except ValueError:
            continue
        date_columns.append((idx, day))

    if not date_columns:
        raise ValueError("Google Ads CSV: Keine Tages-Kosten-Spalten gefunden.")

    report_days = sorted({day for _, day in date_columns})
    last_non_zero_day = ""
    daily_cost_map: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows[header_index + 1 :]:
        article_id = _normalize_article_id(row[id_idx] if id_idx < len(row) else "")
        if not article_id:
            continue
        currency = "EUR"
        if currency_idx >= 0 and currency_idx < len(row):
            maybe_currency = _normalize_text(row[currency_idx]).upper()
            if len(maybe_currency) == 3:
                currency = maybe_currency
        for col_idx, day in date_columns:
            raw_value = row[col_idx] if col_idx < len(row) else ""
            cost_cents = _safe_money_to_cents(raw_value)
            if cost_cents > 0 and day > last_non_zero_day:
                last_non_zero_day = day
            key = (article_id, day)
            entry = daily_cost_map.get(key)
            if entry is None:
                daily_cost_map[key] = {
                    "article_id": article_id,
                    "day": day,
                    "cost_cents": cost_cents,
                    "currency": currency,
                }
            else:
                entry["cost_cents"] = int(entry["cost_cents"]) + int(cost_cents)

    rows_payload = list(daily_cost_map.values())
    non_zero_rows = sum(1 for row in rows_payload if int(row["cost_cents"]) > 0)

    return {
        "rows": rows_payload,
        "count": len(rows_payload),
        "non_zero_count": non_zero_rows,
        "date_column_count": len(date_columns),
        "report_from_day": report_days[0] if report_days else "",
        "report_to_day": report_days[-1] if report_days else "",
        "last_non_zero_day": last_non_zero_day,
    }


def import_google_ads_data(
    *,
    report_content: Optional[bytes],
    report_filename: Optional[str],
    assignment_content: Optional[bytes],
    assignment_filename: Optional[str],
) -> dict[str, Any]:
    if not report_content and not assignment_content:
        raise ValueError("Mindestens eine CSV-Datei muss hochgeladen werden.")

    timestamp = now_iso()
    result: dict[str, Any] = {
        "report": None,
        "assignment": None,
        "imported_at": timestamp,
    }

    with connect_combined_db() as connection:
        if report_content:
            parsed_report = _parse_ads_report_csv(report_content)
            batch_id = str(uuid.uuid4())
            connection.execute(
                """
                INSERT INTO google_ads_import_batches (id, import_kind, source_filename, imported_at, meta_json)
                VALUES (?, 'report', ?, ?, ?)
                """,
                (
                    batch_id,
                    str(report_filename or ""),
                    timestamp,
                    json.dumps(
                        {
                            "rows": parsed_report["count"],
                            "non_zero_rows": parsed_report["non_zero_count"],
                            "date_columns": parsed_report["date_column_count"],
                            "report_from_day": parsed_report["report_from_day"],
                            "report_to_day": parsed_report["report_to_day"],
                            "last_non_zero_day": parsed_report["last_non_zero_day"],
                        },
                        ensure_ascii=True,
                    ),
                ),
            )
            connection.execute("DELETE FROM google_ads_daily_costs")
            for row in parsed_report["rows"]:
                connection.execute(
                    """
                    INSERT INTO google_ads_daily_costs (article_id, day, cost_cents, currency, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(article_id, day) DO UPDATE SET
                        cost_cents = excluded.cost_cents,
                        currency = excluded.currency,
                        updated_at = excluded.updated_at
                    """,
                    (
                        row["article_id"],
                        row["day"],
                        int(row["cost_cents"]),
                        row["currency"],
                        timestamp,
                    ),
                )
            result["report"] = {
                "filename": report_filename,
                "rows": parsed_report["count"],
                "non_zero_rows": parsed_report["non_zero_count"],
                "report_from_day": parsed_report["report_from_day"],
                "report_to_day": parsed_report["report_to_day"],
                "last_non_zero_day": parsed_report["last_non_zero_day"],
            }

        if assignment_content:
            parsed_assignment = _parse_assignment_csv(assignment_content)
            batch_id = str(uuid.uuid4())
            connection.execute(
                """
                INSERT INTO google_ads_import_batches (id, import_kind, source_filename, imported_at, meta_json)
                VALUES (?, 'assignment', ?, ?, ?)
                """,
                (
                    batch_id,
                    str(assignment_filename or ""),
                    timestamp,
                    json.dumps({"rows": parsed_assignment["count"]}, ensure_ascii=True),
                ),
            )
            connection.execute("DELETE FROM google_ads_product_assignments")
            for row in parsed_assignment["rows"]:
                connection.execute(
                    """
                    INSERT INTO google_ads_product_assignments (
                        article_id, product_title, product_key, product_label, product_detail, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(article_id) DO UPDATE SET
                        product_title = excluded.product_title,
                        product_key = excluded.product_key,
                        product_label = excluded.product_label,
                        product_detail = excluded.product_detail,
                        updated_at = excluded.updated_at
                    """,
                    (
                        row["article_id"],
                        row["product_title"],
                        row["product_key"],
                        row["product_label"],
                        row["product_detail"],
                        timestamp,
                    ),
                )
            result["assignment"] = {
                "filename": assignment_filename,
                "rows": parsed_assignment["count"],
            }

        connection.commit()

    return result


def _load_import_status() -> dict[str, Any]:
    with connect_combined_db() as connection:
        rows = connection.execute(
            """
            SELECT import_kind, source_filename, imported_at, meta_json
            FROM google_ads_import_batches
            ORDER BY imported_at DESC
            """
        ).fetchall()

    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        kind = str(row["import_kind"])
        if kind in latest:
            continue
        meta = {}
        raw_meta = row["meta_json"]
        if isinstance(raw_meta, str) and raw_meta.strip():
            try:
                loaded = json.loads(raw_meta)
                if isinstance(loaded, dict):
                    meta = loaded
            except json.JSONDecodeError:
                meta = {}
        latest[kind] = {
            "filename": row["source_filename"],
            "imported_at": row["imported_at"],
            "meta": meta,
        }
    return latest


def build_google_ads_analytics(
    *,
    from_date: Optional[str],
    to_date: Optional[str],
    query: Optional[str],
) -> dict[str, Any]:
    from_day = str(from_date or "").strip()[:10]
    to_day = str(to_date or "").strip()[:10]
    if from_day and to_day and from_day > to_day:
        from_day, to_day = to_day, from_day

    with connect_combined_db() as connection:
        assignments_rows = connection.execute(
            """
            SELECT article_id, product_title, product_key, product_label, product_detail
            FROM google_ads_product_assignments
            """
        ).fetchall()
        daily_rows = connection.execute(
            """
            SELECT article_id, day, cost_cents, currency
            FROM google_ads_daily_costs
            WHERE (? = '' OR day >= ?)
              AND (? = '' OR day <= ?)
            """,
            (from_day, from_day, to_day, to_day),
        ).fetchall()

    assignments_by_article: dict[str, dict[str, Any]] = {}
    for row in assignments_rows:
        assignments_by_article[str(row["article_id"])]= {
            "article_id": str(row["article_id"]),
            "product_title": str(row["product_title"] or ""),
            "product_key": str(row["product_key"] or ""),
            "product_label": str(row["product_label"] or ""),
            "product_detail": str(row["product_detail"] or ""),
        }

    orders = list_all_orders_without_pagination(
        from_date=from_date,
        to_date=to_date,
        marketplace="shopify",
        query=None,
    )

    order_by_product_key: dict[str, dict[str, int]] = defaultdict(
        lambda: {
            "order_count": 0,
            "revenue_total_cents": 0,
            "after_fees_total_cents": 0,
            "purchase_total_cents": 0,
            "profit_before_ads_cents": 0,
        }
    )
    for order in orders:
        title_key = _normalize_title_key(order.get("article"))
        if not title_key:
            continue
        bucket = order_by_product_key[title_key]
        revenue = int(order.get("total_cents") or 0)
        after_fees = int(order.get("after_fees_cents") or 0)
        purchase = int(order.get("purchase_cost_cents") or 0)
        profit_before = int(order.get("profit_cents") or (after_fees - purchase))
        bucket["order_count"] += 1
        bucket["revenue_total_cents"] += revenue
        bucket["after_fees_total_cents"] += after_fees
        bucket["purchase_total_cents"] += purchase
        bucket["profit_before_ads_cents"] += profit_before

    product_rows: dict[str, dict[str, Any]] = {}
    missing_assignments: dict[str, dict[str, Any]] = {}
    trend_map: dict[str, dict[str, int]] = defaultdict(lambda: {"ads_cost_cents": 0, "mapped_ads_cost_cents": 0})
    total_ads_cost = 0
    mapped_ads_cost = 0
    unmapped_ads_cost = 0

    for row in daily_rows:
        article_id = str(row["article_id"])
        day = str(row["day"])
        cost_cents = int(row["cost_cents"] or 0)
        assignment = assignments_by_article.get(article_id)
        if assignment:
            product_key = assignment["product_key"]
            product_label = assignment["product_label"]
            product_detail = assignment["product_detail"]
            mapped = True
        else:
            product_key = f"unmapped:{article_id}"
            product_label = article_id
            product_detail = "Keine Assignment-Zuordnung"
            mapped = False

        record = product_rows.get(product_key)
        if record is None:
            order_metrics = order_by_product_key.get(product_key, {})
            record = {
                "product_key": product_key,
                "product_label": product_label,
                "product_detail": product_detail,
                "mapped": mapped,
                "article_count": 0,
                "ads_cost_cents": 0,
                "order_count": int(order_metrics.get("order_count") or 0),
                "revenue_total_cents": int(order_metrics.get("revenue_total_cents") or 0),
                "after_fees_total_cents": int(order_metrics.get("after_fees_total_cents") or 0),
                "purchase_total_cents": int(order_metrics.get("purchase_total_cents") or 0),
                "profit_before_ads_cents": int(order_metrics.get("profit_before_ads_cents") or 0),
                "profit_after_ads_cents": int(order_metrics.get("profit_before_ads_cents") or 0),
            }
            product_rows[product_key] = record

        record["article_count"] = int(record["article_count"]) + 1
        record["ads_cost_cents"] = int(record["ads_cost_cents"]) + cost_cents
        record["profit_after_ads_cents"] = int(record["profit_before_ads_cents"]) - int(record["ads_cost_cents"])

        trend_map[day]["ads_cost_cents"] += cost_cents
        total_ads_cost += cost_cents
        if mapped:
            mapped_ads_cost += cost_cents
            trend_map[day]["mapped_ads_cost_cents"] += cost_cents
        else:
            unmapped_ads_cost += cost_cents
            missing = missing_assignments.get(article_id)
            if missing is None:
                missing = {"article_id": article_id, "ads_cost_cents": 0, "day_count": 0}
                missing_assignments[article_id] = missing
            missing["ads_cost_cents"] = int(missing["ads_cost_cents"]) + cost_cents
            missing["day_count"] = int(missing["day_count"]) + 1

    for product_key, order_metrics in order_by_product_key.items():
        if product_key in product_rows:
            continue
        product_rows[product_key] = {
            "product_key": product_key,
            "product_label": product_key.replace("title:", "") or "Shopify Artikel",
            "product_detail": "Shopify Artikel ohne Ads-Zuordnung",
            "mapped": True,
            "article_count": 0,
            "ads_cost_cents": 0,
            "order_count": int(order_metrics.get("order_count") or 0),
            "revenue_total_cents": int(order_metrics.get("revenue_total_cents") or 0),
            "after_fees_total_cents": int(order_metrics.get("after_fees_total_cents") or 0),
            "purchase_total_cents": int(order_metrics.get("purchase_total_cents") or 0),
            "profit_before_ads_cents": int(order_metrics.get("profit_before_ads_cents") or 0),
            "profit_after_ads_cents": int(order_metrics.get("profit_before_ads_cents") or 0),
        }

    query_token = _normalize_text(query).lower()
    products_list = list(product_rows.values())
    if query_token:
        products_list = [
            item
            for item in products_list
            if query_token in _normalize_text(item.get("product_label")).lower()
            or query_token in _normalize_text(item.get("product_detail")).lower()
            or query_token in _normalize_text(item.get("product_key")).lower()
        ]

    products_list.sort(
        key=lambda item: (
            int(item.get("ads_cost_cents") or 0),
            int(item.get("revenue_total_cents") or 0),
            int(item.get("order_count") or 0),
        ),
        reverse=True,
    )

    missing_list = list(missing_assignments.values())
    if query_token:
        missing_list = [
            item
            for item in missing_list
            if query_token in _normalize_text(item.get("article_id")).lower()
        ]
    missing_list.sort(key=lambda item: int(item.get("ads_cost_cents") or 0), reverse=True)

    trend_points = [
        {
            "day": day,
            "ads_cost_cents": values["ads_cost_cents"],
            "mapped_ads_cost_cents": values["mapped_ads_cost_cents"],
        }
        for day, values in sorted(trend_map.items())
    ]

    revenue_total = sum(int(item.get("revenue_total_cents") or 0) for item in products_list)
    profit_before_total = sum(int(item.get("profit_before_ads_cents") or 0) for item in products_list)
    profit_after_total = sum(int(item.get("profit_after_ads_cents") or 0) for item in products_list)

    roas = 0.0
    if total_ads_cost > 0:
        roas = round(revenue_total / total_ads_cost, 4)

    import_status = _load_import_status()
    report_status = import_status.get("report") or {}
    assignment_status = import_status.get("assignment") or {}

    return {
        "kpis": {
            "products_count": len(products_list),
            "orders_count": len(orders),
            "ads_cost_total_cents": total_ads_cost,
            "ads_cost_mapped_cents": mapped_ads_cost,
            "ads_cost_unmapped_cents": unmapped_ads_cost,
            "shopify_revenue_total_cents": revenue_total,
            "profit_before_ads_total_cents": profit_before_total,
            "profit_after_ads_total_cents": profit_after_total,
            "roas": roas,
            "missing_assignments_count": len(missing_list),
        },
        "products": products_list[:300],
        "missing_assignments": missing_list[:200],
        "trend": trend_points,
        "imports": {
            "report": report_status,
            "assignment": assignment_status,
        },
    }
