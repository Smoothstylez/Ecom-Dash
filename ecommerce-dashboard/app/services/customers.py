from __future__ import annotations

import json
import sqlite3
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from copy import deepcopy
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, cast

from app.config import KAUFLAND_DB_PATH, SHOPIFY_DB_PATH
from app.db import connect_combined_db, now_iso
from app.services.orders import list_all_orders_without_pagination


def _connect_readonly(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    return connection


def _safe_json_load(raw_text: Any) -> dict[str, Any]:
    if not isinstance(raw_text, str) or not raw_text.strip():
        return {}
    try:
        value = json.loads(raw_text)
    except (TypeError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _normalize_text(value: Any) -> str:
    text = str(value or "").strip()
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch)).strip()


def _normalize_key(value: Any) -> str:
    return "".join(ch for ch in _normalize_text(value).lower() if ch.isalnum())


def _normalize_email(value: Any) -> str:
    email = str(value or "").strip().lower()
    if "@" not in email or len(email) < 5:
        return ""
    return email


def _normalize_phone(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) < 6:
        return ""
    if raw.startswith("+"):
        return f"+{digits}"
    if digits.startswith("00") and len(digits) > 8:
        return f"+{digits[2:]}"
    return digits


def _first_non_empty(*values: Any) -> str:
    for value in values:
        token = str(value or "").strip()
        if token:
            return token
    return ""


def _compose_name(*values: Any) -> str:
    parts: list[str] = []
    for value in values:
        token = str(value or "").strip()
        if token:
            parts.append(token)
    return " ".join(parts).strip()


def _parse_iso(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _safe_pct(part: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round((part / total) * 100.0, 2)


CUSTOMER_LOCATION_CACHE_TTL_SECONDS = 180
_CUSTOMER_LOCATION_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}


COUNTRY_CENTROIDS: dict[str, tuple[float, float]] = {
    "DE": (51.1657, 10.4515),
    "AT": (47.5162, 14.5501),
    "CH": (46.8182, 8.2275),
    "FR": (46.2276, 2.2137),
    "IT": (41.8719, 12.5674),
    "ES": (40.4637, -3.7492),
    "PT": (39.3999, -8.2245),
    "NL": (52.1326, 5.2913),
    "BE": (50.5039, 4.4699),
    "LU": (49.8153, 6.1296),
    "PL": (51.9194, 19.1451),
    "CZ": (49.8175, 15.4730),
    "SK": (48.6690, 19.6990),
    "HU": (47.1625, 19.5033),
    "RO": (45.9432, 24.9668),
    "BG": (42.7339, 25.4858),
    "HR": (45.1000, 15.2000),
    "SI": (46.1512, 14.9955),
    "GR": (39.0742, 21.8243),
    "SE": (60.1282, 18.6435),
    "NO": (60.4720, 8.4689),
    "DK": (56.2639, 9.5018),
    "FI": (61.9241, 25.7482),
    "GB": (55.3781, -3.4360),
    "IE": (53.1424, -7.6921),
    "US": (37.0902, -95.7129),
    "CA": (56.1304, -106.3468),
    "AU": (-25.2744, 133.7751),
    "NZ": (-40.9006, 174.8860),
    "JP": (36.2048, 138.2529),
    "KR": (35.9078, 127.7669),
    "CN": (35.8617, 104.1954),
    "IN": (20.5937, 78.9629),
    "TR": (38.9637, 35.2433),
    "AE": (23.4241, 53.8478),
    "SA": (23.8859, 45.0792),
    "IL": (31.0461, 34.8516),
    "BR": (-14.2350, -51.9253),
    "AR": (-38.4161, -63.6167),
    "MX": (23.6345, -102.5528),
    "ZA": (-30.5595, 22.9375),
    "EG": (26.8206, 30.8025),
}


def _to_float(value: Any) -> Optional[float]:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if not (-180.0 <= numeric <= 180.0):
        return None
    return numeric


def _chunked(values: list[str], size: int = 450) -> list[list[str]]:
    if not values:
        return []
    return [values[i : i + size] for i in range(0, len(values), size)]


def _new_contact_payload() -> dict[str, Any]:
    return {
        "names": [],
        "emails": set(),
        "phones": set(),
        "external_ids": set(),
        "addresses": [],
    }


def _address_has_data(address: dict[str, Any]) -> bool:
    keys = ("full_name", "street", "postcode", "city", "country", "phone")
    return any(str(address.get(key) or "").strip() for key in keys)


def _address_key(address: dict[str, Any]) -> str:
    parts = [
        _normalize_key(address.get("street")),
        _normalize_key(address.get("postcode")),
        _normalize_key(address.get("city")),
        _normalize_key(address.get("country")),
    ]
    joined = "|".join(parts)
    return joined.strip("|")


def _extract_shopify_address(address: Any) -> dict[str, str]:
    row = address if isinstance(address, dict) else {}
    full_name = _compose_name(row.get("first_name"), row.get("last_name"))
    street = _compose_name(row.get("address1"), row.get("address2"))
    return {
        "full_name": _first_non_empty(full_name, row.get("name")),
        "street": street,
        "postcode": str(row.get("zip") or "").strip(),
        "city": str(row.get("city") or "").strip(),
        "country": _first_non_empty(row.get("country_code"), row.get("country")),
        "phone": str(row.get("phone") or "").strip(),
    }


def _extract_kaufland_address(row: sqlite3.Row, prefix: str) -> dict[str, str]:
    full_name = _compose_name(row[f"{prefix}_first_name"], row[f"{prefix}_last_name"])
    street = _compose_name(row[f"{prefix}_street"], row[f"{prefix}_house_number"])
    return {
        "full_name": full_name,
        "street": street,
        "postcode": str(row[f"{prefix}_postcode"] or "").strip(),
        "city": str(row[f"{prefix}_city"] or "").strip(),
        "country": str(row[f"{prefix}_country"] or "").strip(),
        "phone": str(row[f"{prefix}_phone"] or "").strip(),
    }


def _dedupe_addresses(addresses: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    output: list[dict[str, Any]] = []
    for address in addresses:
        key = _address_key(address)
        if not key or key in seen:
            continue
        seen.add(key)
        output.append(address)
    return output


def _load_shopify_contacts(order_ids: set[str]) -> dict[str, dict[str, Any]]:
    if not SHOPIFY_DB_PATH.exists() or not order_ids:
        return {}

    payload: dict[str, dict[str, Any]] = {}
    ids = sorted({str(order_id).strip() for order_id in order_ids if str(order_id).strip()})
    if not ids:
        return payload

    with _connect_readonly(SHOPIFY_DB_PATH) as connection:
        for chunk in _chunked(ids):
            placeholders = ",".join("?" for _ in chunk)
            rows = connection.execute(
                f"""
                SELECT
                    id,
                    raw_json,
                    customer_first_name,
                    customer_last_name,
                    customer_email,
                    email
                FROM orders
                WHERE id IN ({placeholders})
                """,
                tuple(chunk),
            ).fetchall()

            for row in rows:
                contact = _new_contact_payload()
                raw = _safe_json_load(row["raw_json"])
                customer = _as_dict(raw.get("customer"))
                shipping = _as_dict(raw.get("shipping_address"))
                billing = _as_dict(raw.get("billing_address"))
                default_address = _as_dict(customer.get("default_address"))

                preferred_name = _compose_name(row["customer_first_name"], row["customer_last_name"])
                fallback_name = _compose_name(customer.get("first_name"), customer.get("last_name"))
                shipping_name = _compose_name(shipping.get("first_name"), shipping.get("last_name"))
                billing_name = _compose_name(billing.get("first_name"), billing.get("last_name"))
                resolved_name = _first_non_empty(preferred_name, fallback_name, shipping_name, billing_name)
                if resolved_name:
                    contact["names"].append(resolved_name)

                for email in (
                    row["customer_email"],
                    row["email"],
                    customer.get("email"),
                    shipping.get("email"),
                    billing.get("email"),
                ):
                    normalized = _normalize_email(email)
                    if normalized:
                        contact["emails"].add(normalized)

                for phone in (
                    raw.get("phone"),
                    customer.get("phone"),
                    shipping.get("phone"),
                    billing.get("phone"),
                    default_address.get("phone"),
                ):
                    normalized = _normalize_phone(phone)
                    if normalized:
                        contact["phones"].add(normalized)

                customer_id = str(customer.get("id") or "").strip()
                if customer_id:
                    contact["external_ids"].add(customer_id)

                for address_raw in (shipping, billing, default_address):
                    extracted = _extract_shopify_address(address_raw)
                    if _address_has_data(extracted):
                        contact["addresses"].append(extracted)

                contact["addresses"] = _dedupe_addresses(contact["addresses"])
                payload[str(row["id"])] = contact

    return payload


def _load_kaufland_contacts(order_ids: set[str]) -> dict[str, dict[str, Any]]:
    if not KAUFLAND_DB_PATH.exists() or not order_ids:
        return {}

    payload: dict[str, dict[str, Any]] = {}
    ids = sorted({str(order_id).strip() for order_id in order_ids if str(order_id).strip()})
    if not ids:
        return payload

    with _connect_readonly(KAUFLAND_DB_PATH) as connection:
        buyer_email_map: dict[str, str] = {}
        for chunk in _chunked(ids):
            placeholders = ",".join("?" for _ in chunk)
            order_rows = connection.execute(
                f"""
                SELECT id_order, raw_json
                FROM orders
                WHERE id_order IN ({placeholders})
                """,
                tuple(chunk),
            ).fetchall()
            for order_row in order_rows:
                raw = _safe_json_load(order_row["raw_json"])
                buyer = _as_dict(raw.get("buyer"))
                email = _normalize_email(buyer.get("email"))
                if email:
                    buyer_email_map[str(order_row["id_order"])] = email

        for chunk in _chunked(ids):
            placeholders = ",".join("?" for _ in chunk)
            unit_rows = connection.execute(
                f"""
                SELECT
                    id_order,
                    buyer_id_buyer,
                    shipping_first_name,
                    shipping_last_name,
                    shipping_street,
                    shipping_house_number,
                    shipping_postcode,
                    shipping_city,
                    shipping_country,
                    shipping_phone,
                    billing_first_name,
                    billing_last_name,
                    billing_street,
                    billing_house_number,
                    billing_postcode,
                    billing_city,
                    billing_country,
                    billing_phone,
                    raw_json
                FROM order_units
                WHERE id_order IN ({placeholders})
                ORDER BY COALESCE(ts_created_iso, '') ASC, id_order_unit ASC
                """,
                tuple(chunk),
            ).fetchall()

            for row in unit_rows:
                order_id = str(row["id_order"])
                contact = payload.get(order_id)
                if contact is None:
                    contact = _new_contact_payload()
                    payload[order_id] = contact

                buyer_id = str(row["buyer_id_buyer"] or "").strip()
                if buyer_id:
                    contact["external_ids"].add(buyer_id)

                order_email = buyer_email_map.get(order_id)
                if order_email:
                    contact["emails"].add(order_email)

                shipping_name = _compose_name(row["shipping_first_name"], row["shipping_last_name"])
                billing_name = _compose_name(row["billing_first_name"], row["billing_last_name"])
                for maybe_name in (shipping_name, billing_name):
                    if maybe_name:
                        contact["names"].append(maybe_name)

                for phone in (row["shipping_phone"], row["billing_phone"]):
                    normalized = _normalize_phone(phone)
                    if normalized:
                        contact["phones"].add(normalized)

                shipping_address = _extract_kaufland_address(row, "shipping")
                if _address_has_data(shipping_address):
                    contact["addresses"].append(shipping_address)

                billing_address = _extract_kaufland_address(row, "billing")
                if _address_has_data(billing_address):
                    contact["addresses"].append(billing_address)

                raw_unit = _safe_json_load(row["raw_json"])
                buyer = _as_dict(raw_unit.get("buyer"))
                email = _normalize_email(buyer.get("email"))
                if email:
                    contact["emails"].add(email)

        for contact_payload in payload.values():
            contact_payload["addresses"] = _dedupe_addresses(contact_payload["addresses"])

    return payload


def _ensure_geo_cache_table(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS customer_geo_cache (
            location_key TEXT PRIMARY KEY,
            query_text TEXT NOT NULL,
            lat REAL,
            lng REAL,
            provider TEXT NOT NULL,
            resolved_at TEXT NOT NULL
        )
        """
    )


def _load_geo_cache(keys: list[str]) -> dict[str, dict[str, Any]]:
    if not keys:
        return {}
    payload: dict[str, dict[str, Any]] = {}
    with connect_combined_db() as connection:
        _ensure_geo_cache_table(connection)
        for chunk in _chunked(keys, size=450):
            placeholders = ",".join("?" for _ in chunk)
            rows = connection.execute(
                f"""
                SELECT location_key, lat, lng, provider
                FROM customer_geo_cache
                WHERE location_key IN ({placeholders})
                """,
                tuple(chunk),
            ).fetchall()
            for row in rows:
                payload[str(row["location_key"])] = {
                    "lat": row["lat"],
                    "lng": row["lng"],
                    "provider": str(row["provider"] or ""),
                }
    return payload


def _upsert_geo_cache(*, location_key: str, query_text: str, lat: Optional[float], lng: Optional[float], provider: str) -> None:
    with connect_combined_db() as connection:
        _ensure_geo_cache_table(connection)
        connection.execute(
            """
            INSERT INTO customer_geo_cache (location_key, query_text, lat, lng, provider, resolved_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(location_key) DO UPDATE SET
                query_text = excluded.query_text,
                lat = excluded.lat,
                lng = excluded.lng,
                provider = excluded.provider,
                resolved_at = excluded.resolved_at
            """,
            (location_key, query_text, lat, lng, provider, now_iso()),
        )
        connection.commit()


def _remote_geocode_city(*, city: str, country_code: str) -> Optional[tuple[float, float]]:
    query_name = city.strip()
    if not query_name:
        return None
    params = urllib.parse.urlencode(
        {
            "name": query_name,
            "count": "6",
            "language": "de",
            "format": "json",
        }
    )
    url = f"https://geocoding-api.open-meteo.com/v1/search?{params}"
    request = urllib.request.Request(url, headers={"User-Agent": "combined-dashboard/1.0"})
    try:
        with urllib.request.urlopen(request, timeout=1.8) as response:
            payload = json.loads(response.read().decode("utf-8", errors="ignore"))
    except (TimeoutError, OSError, urllib.error.URLError, json.JSONDecodeError):
        return None

    results = payload.get("results")
    if not isinstance(results, list):
        return None

    wanted_country = country_code.strip().upper()
    fallback: Optional[tuple[float, float]] = None
    for row in results:
        if not isinstance(row, dict):
            continue
        lat = _to_float(row.get("latitude"))
        lng = _to_float(row.get("longitude"))
        if lat is None or lng is None:
            continue
        row_country = str(row.get("country_code") or "").strip().upper()
        if wanted_country and row_country and row_country == wanted_country:
            return (lat, lng)
        if fallback is None:
            fallback = (lat, lng)

    return fallback


def _country_centroid(country_code: str) -> Optional[tuple[float, float]]:
    token = country_code.strip().upper()
    return COUNTRY_CENTROIDS.get(token)


def _normalize_country_code(value: Any) -> str:
    token = str(value or "").strip().upper()
    if len(token) == 2 and token.isalpha():
        return token
    return ""


def _valid_lat_lng(lat: Optional[float], lng: Optional[float]) -> bool:
    if lat is None or lng is None:
        return False
    return -90.0 <= lat <= 90.0 and -180.0 <= lng <= 180.0


def _load_shopify_order_locations(order_ids: set[str]) -> dict[str, dict[str, Any]]:
    if not SHOPIFY_DB_PATH.exists() or not order_ids:
        return {}

    payload: dict[str, dict[str, Any]] = {}
    ids = sorted({str(order_id).strip() for order_id in order_ids if str(order_id).strip()})
    if not ids:
        return payload

    with _connect_readonly(SHOPIFY_DB_PATH) as connection:
        for chunk in _chunked(ids):
            placeholders = ",".join("?" for _ in chunk)
            rows = connection.execute(
                f"""
                SELECT id, shipping_city, shipping_country, raw_json
                FROM orders
                WHERE id IN ({placeholders})
                """,
                tuple(chunk),
            ).fetchall()

            for row in rows:
                raw = _safe_json_load(row["raw_json"])
                shipping = _as_dict(raw.get("shipping_address"))

                lat = _to_float(shipping.get("latitude"))
                lng = _to_float(shipping.get("longitude"))
                if not _valid_lat_lng(lat, lng):
                    lat = None
                    lng = None

                city = _first_non_empty(shipping.get("city"), row["shipping_city"])
                country_code = _normalize_country_code(shipping.get("country_code"))
                country_label = _first_non_empty(shipping.get("country_code"), shipping.get("country"), row["shipping_country"])
                if not country_code:
                    country_code = _normalize_country_code(row["shipping_country"])

                payload[str(row["id"])] = {
                    "lat": lat,
                    "lng": lng,
                    "city": city,
                    "country_code": country_code,
                    "country": country_label,
                    "postcode": str(shipping.get("zip") or "").strip(),
                }

    return payload


def _load_kaufland_order_locations(order_ids: set[str]) -> dict[str, dict[str, Any]]:
    if not KAUFLAND_DB_PATH.exists() or not order_ids:
        return {}

    payload: dict[str, dict[str, Any]] = {}
    ids = sorted({str(order_id).strip() for order_id in order_ids if str(order_id).strip()})
    if not ids:
        return payload

    with _connect_readonly(KAUFLAND_DB_PATH) as connection:
        for chunk in _chunked(ids):
            placeholders = ",".join("?" for _ in chunk)
            rows = connection.execute(
                f"""
                SELECT id_order, shipping_city, shipping_country, shipping_postcode, raw_json
                FROM order_units
                WHERE id_order IN ({placeholders})
                ORDER BY COALESCE(ts_created_iso, '') ASC, id_order_unit ASC
                """,
                tuple(chunk),
            ).fetchall()

            for row in rows:
                order_id = str(row["id_order"])
                existing = payload.get(order_id)
                if existing is None:
                    existing = {
                        "lat": None,
                        "lng": None,
                        "city": "",
                        "country_code": "",
                        "country": "",
                        "postcode": "",
                    }
                    payload[order_id] = existing

                raw = _safe_json_load(row["raw_json"])
                shipping = _as_dict(raw.get("shipping_address"))

                city = _first_non_empty(existing.get("city"), shipping.get("city"), row["shipping_city"])
                country_label = _first_non_empty(existing.get("country"), shipping.get("country"), row["shipping_country"])
                country_code = _normalize_country_code(_first_non_empty(existing.get("country_code"), shipping.get("country"), row["shipping_country"]))
                postcode = _first_non_empty(existing.get("postcode"), shipping.get("postcode"), row["shipping_postcode"])

                existing["city"] = city
                existing["country"] = country_label
                existing["country_code"] = country_code
                existing["postcode"] = postcode

    return payload


def _customer_locations_cache_key(
    *,
    from_date: Optional[str],
    to_date: Optional[str],
    marketplace: Optional[str],
    query: Optional[str],
    status_filter: Optional[str],
) -> str:
    parts = [
        str(from_date or "").strip(),
        str(to_date or "").strip(),
        str(marketplace or "").strip().lower(),
        str(query or "").strip().lower(),
        str(status_filter or "").strip().lower(),
    ]
    return "|".join(parts)


def build_customer_locations_map(
    *,
    from_date: Optional[str],
    to_date: Optional[str],
    marketplace: Optional[str],
    query: Optional[str],
    status_filter: Optional[str],
    force_refresh: bool = False,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    cache_key = _customer_locations_cache_key(
        from_date=from_date,
        to_date=to_date,
        marketplace=marketplace,
        query=query,
        status_filter=status_filter,
    )

    if not force_refresh:
        cached = _CUSTOMER_LOCATION_CACHE.get(cache_key)
        if cached is not None:
            cached_at, cached_payload = cached
            if (time.time() - cached_at) <= CUSTOMER_LOCATION_CACHE_TTL_SECONDS:
                payload = deepcopy(cached_payload)
                summary = cast(dict[str, Any], _as_dict(payload.get("summary")))
                summary["cache_hit"] = True
                payload["summary"] = summary
                return payload

    orders = list_all_orders_without_pagination(
        from_date=from_date,
        to_date=to_date,
        marketplace=marketplace,
        query=query,
        status_filter=status_filter,
    )

    shopify_order_ids = {
        str(order.get("order_id") or "").strip()
        for order in orders
        if str(order.get("marketplace") or "").strip().lower() == "shopify" and str(order.get("order_id") or "").strip()
    }
    kaufland_order_ids = {
        str(order.get("order_id") or "").strip()
        for order in orders
        if str(order.get("marketplace") or "").strip().lower() == "kaufland" and str(order.get("order_id") or "").strip()
    }

    shopify_locations = _load_shopify_order_locations(shopify_order_ids)
    kaufland_locations = _load_kaufland_order_locations(kaufland_order_ids)

    order_candidates: dict[str, dict[str, Any]] = {}
    unresolved_keys: dict[str, dict[str, str]] = {}

    for order in orders:
        market = str(order.get("marketplace") or "").strip().lower()
        order_id = str(order.get("order_id") or "").strip()
        if market not in {"shopify", "kaufland"} or not order_id:
            continue

        location = shopify_locations.get(order_id) if market == "shopify" else kaufland_locations.get(order_id)
        if not isinstance(location, dict):
            location = {}

        lat = _to_float(location.get("lat"))
        lng = _to_float(location.get("lng"))
        city = str(location.get("city") or "").strip()
        country_code = _normalize_country_code(location.get("country_code"))
        country = str(location.get("country") or country_code or "").strip()
        postcode = str(location.get("postcode") or "").strip()

        order_key = f"{market}:{order_id}"
        if _valid_lat_lng(lat, lng):
            order_candidates[order_key] = {
                "lat": lat,
                "lng": lng,
                "city": city,
                "country_code": country_code,
                "country": country,
                "provider": "source_coordinates",
            }
            continue

        location_key = f"{country_code}|{_normalize_key(city)}|{_normalize_key(postcode)}"
        if not location_key.strip("|"):
            location_key = f"{country_code}|{_normalize_key(city)}"
        if not location_key.strip("|"):
            location_key = f"{country_code}|country"

        order_candidates[order_key] = {
            "lat": None,
            "lng": None,
            "city": city,
            "country_code": country_code,
            "country": country,
            "location_key": location_key,
            "provider": "unresolved",
        }
        unresolved_keys[location_key] = {
            "city": city,
            "country_code": country_code,
            "country": country,
            "query_text": ", ".join(part for part in (city, country_code or country) if part),
        }

    cache = _load_geo_cache(list(unresolved_keys.keys())) if unresolved_keys else {}
    remote_geocode_budget = 10
    geocode_attempts = 0
    geocode_successes = 0
    cache_location_hits = 0
    for location_key, candidate in unresolved_keys.items():
        cached = cache.get(location_key)
        if isinstance(cached, dict):
            lat = _to_float(cached.get("lat"))
            lng = _to_float(cached.get("lng"))
            if _valid_lat_lng(lat, lng):
                cache[location_key] = {
                    "lat": lat,
                    "lng": lng,
                    "provider": str(cached.get("provider") or "cache"),
                }
                cache_location_hits += 1
                continue

        city = str(candidate.get("city") or "").strip()
        country_code = _normalize_country_code(candidate.get("country_code"))
        query_text = str(candidate.get("query_text") or city or country_code).strip() or "unknown"

        lat: Optional[float] = None
        lng: Optional[float] = None
        provider = "unresolved"

        if city and remote_geocode_budget > 0:
            geocode_attempts += 1
            geo = _remote_geocode_city(city=city, country_code=country_code)
            remote_geocode_budget -= 1
            if geo is not None:
                lat, lng = geo
                provider = "open_meteo"
                geocode_successes += 1

        if not _valid_lat_lng(lat, lng):
            centroid = _country_centroid(country_code)
            if centroid is not None:
                lat, lng = centroid
                provider = "country_centroid"

        if _valid_lat_lng(lat, lng):
            cache[location_key] = {"lat": lat, "lng": lng, "provider": provider}
            _upsert_geo_cache(
                location_key=location_key,
                query_text=query_text,
                lat=lat,
                lng=lng,
                provider=provider,
            )
        else:
            cache[location_key] = {"lat": None, "lng": None, "provider": "unresolved"}

    points_map: dict[str, dict[str, Any]] = {}
    unresolved_orders = 0
    provider_order_counter: Counter[str] = Counter()

    for order in orders:
        market = str(order.get("marketplace") or "").strip().lower()
        order_id = str(order.get("order_id") or "").strip()
        if market not in {"shopify", "kaufland"} or not order_id:
            continue

        order_key = f"{market}:{order_id}"
        candidate = order_candidates.get(order_key, {})
        lat = _to_float(candidate.get("lat"))
        lng = _to_float(candidate.get("lng"))
        provider = str(candidate.get("provider") or "unresolved")

        if not _valid_lat_lng(lat, lng):
            location_key = str(candidate.get("location_key") or "")
            cached = cache.get(location_key, {})
            lat = _to_float(cached.get("lat"))
            lng = _to_float(cached.get("lng"))
            provider = str(cached.get("provider") or provider)

        if lat is None or lng is None:
            unresolved_orders += 1
            continue

        if not _valid_lat_lng(lat, lng):
            unresolved_orders += 1
            continue

        lat_value = cast(float, lat)
        lng_value = cast(float, lng)

        provider_order_counter[provider] += 1

        city = str(candidate.get("city") or "").strip()
        country_code = _normalize_country_code(candidate.get("country_code"))
        country = str(candidate.get("country") or country_code or "").strip()

        rounded_lat = round(lat_value, 3)
        rounded_lng = round(lng_value, 3)
        bucket_key = f"{rounded_lat}|{rounded_lng}|{city}|{country_code}|{country}"
        bucket = points_map.get(bucket_key)
        if bucket is None:
            bucket = {
                "lat": rounded_lat,
                "lng": rounded_lng,
                "city": city,
                "country_code": country_code,
                "country": country,
                "order_count": 0,
                "revenue_total_cents": 0,
                "profit_total_cents": 0,
                "marketplaces": Counter(),
                "provider": provider,
            }
            points_map[bucket_key] = bucket

        bucket["order_count"] += 1
        bucket["revenue_total_cents"] += int(order.get("total_cents") or 0)
        bucket["profit_total_cents"] += int(order.get("profit_cents") or 0)
        bucket["marketplaces"][market] += 1

    points: list[dict[str, Any]] = []
    for bucket in points_map.values():
        marketplaces_counter: Counter[str] = bucket.pop("marketplaces")
        marketplace_rows = [
            {"marketplace": token, "order_count": count}
            for token, count in marketplaces_counter.items()
        ]
        marketplace_rows.sort(key=lambda item: item["order_count"], reverse=True)
        dominant = marketplace_rows[0]["marketplace"] if marketplace_rows else ""
        points.append(
            {
                **bucket,
                "marketplaces": marketplace_rows,
                "dominant_marketplace": dominant,
                "weight": int(bucket.get("order_count") or 0),
            }
        )

    points.sort(
        key=lambda item: (
            int(item.get("order_count") or 0),
            int(item.get("revenue_total_cents") or 0),
        ),
        reverse=True,
    )

    generated_ms = int(round((time.perf_counter() - started_at) * 1000))
    payload = {
        "summary": {
            "orders_total": len(orders),
            "points_total": len(points),
            "resolved_source_coordinates_count": int(provider_order_counter.get("source_coordinates") or 0),
            "resolved_geocoded_count": int(provider_order_counter.get("open_meteo") or 0),
            "resolved_country_centroid_count": int(provider_order_counter.get("country_centroid") or 0),
            "unresolved_orders_count": unresolved_orders,
            "geocode_attempts": geocode_attempts,
            "geocode_successes": geocode_successes,
            "cache_location_hits": cache_location_hits,
            "cache_hit": False,
            "generated_in_ms": generated_ms,
        },
        "points": points,
    }

    _CUSTOMER_LOCATION_CACHE[cache_key] = (time.time(), deepcopy(payload))
    return payload


def _event_identity_tokens(event: dict[str, Any]) -> set[str]:
    tokens: set[str] = set()

    for email in event.get("emails", []):
        normalized = _normalize_email(email)
        if normalized:
            tokens.add(f"email:{normalized}")

    for phone in event.get("phones", []):
        normalized = _normalize_phone(phone)
        if normalized:
            tokens.add(f"phone:{normalized}")

    marketplace = str(event.get("marketplace") or "").strip().lower()
    for external_id in event.get("external_ids", []):
        token = _normalize_key(external_id)
        if token:
            tokens.add(f"ext:{marketplace}:{token}")

    name_key = _normalize_key(event.get("customer_name"))
    primary_address = _as_dict(event.get("primary_address"))
    address_key = _address_key(primary_address)
    if name_key and address_key:
        tokens.add(f"nameaddr:{name_key}:{address_key}")

    return tokens


def _union_find_find(parent: list[int], index: int) -> int:
    cursor = index
    while parent[cursor] != cursor:
        parent[cursor] = parent[parent[cursor]]
        cursor = parent[cursor]
    return cursor


def _union_find_union(parent: list[int], rank: list[int], left: int, right: int) -> None:
    left_root = _union_find_find(parent, left)
    right_root = _union_find_find(parent, right)
    if left_root == right_root:
        return
    if rank[left_root] < rank[right_root]:
        parent[left_root] = right_root
        return
    if rank[left_root] > rank[right_root]:
        parent[right_root] = left_root
        return
    parent[right_root] = left_root
    rank[left_root] += 1


def build_customers_overview(
    *,
    from_date: Optional[str],
    to_date: Optional[str],
    marketplace: Optional[str],
    query: Optional[str],
    status_filter: Optional[str],
    limit: int,
    offset: int,
) -> dict[str, Any]:
    orders = list_all_orders_without_pagination(
        from_date=from_date,
        to_date=to_date,
        marketplace=marketplace,
        query=None,
        status_filter=status_filter,
    )

    shopify_order_ids = {
        str(order.get("order_id") or "").strip()
        for order in orders
        if str(order.get("marketplace") or "").strip().lower() == "shopify" and str(order.get("order_id") or "").strip()
    }
    kaufland_order_ids = {
        str(order.get("order_id") or "").strip()
        for order in orders
        if str(order.get("marketplace") or "").strip().lower() == "kaufland" and str(order.get("order_id") or "").strip()
    }

    shopify_contacts = _load_shopify_contacts(shopify_order_ids)
    kaufland_contacts = _load_kaufland_contacts(kaufland_order_ids)

    events: list[dict[str, Any]] = []
    for order in orders:
        market = str(order.get("marketplace") or "").strip().lower()
        order_id = str(order.get("order_id") or "").strip()
        if not order_id or market not in {"shopify", "kaufland"}:
            continue

        contact = shopify_contacts.get(order_id) if market == "shopify" else kaufland_contacts.get(order_id)
        if not isinstance(contact, dict):
            contact = _new_contact_payload()

        names = [str(name).strip() for name in contact.get("names", []) if str(name).strip()]
        fallback_name = str(order.get("customer") or "").strip()
        if fallback_name:
            names.append(fallback_name)
        customer_name = _first_non_empty(*names, "Unbekannt")

        addresses = _dedupe_addresses(
            [
                address
                for address in contact.get("addresses", [])
                if isinstance(address, dict) and _address_has_data(address)
            ]
        )
        primary_address = addresses[0] if addresses else {}

        emails = sorted({email for email in contact.get("emails", set()) if _normalize_email(email)})
        phones = sorted({phone for phone in contact.get("phones", set()) if _normalize_phone(phone)})
        external_ids = sorted({str(value).strip() for value in contact.get("external_ids", set()) if str(value).strip()})

        events.append(
            {
                "marketplace": market,
                "order_id": order_id,
                "order_date": str(order.get("order_date") or "").strip(),
                "customer_name": customer_name,
                "all_names": names,
                "emails": emails,
                "phones": phones,
                "external_ids": external_ids,
                "addresses": addresses,
                "primary_address": primary_address,
                "article": str(order.get("article") or "").strip(),
                "revenue_total_cents": int(order.get("total_cents") or 0),
                "after_fees_total_cents": int(order.get("after_fees_cents") or 0),
                "purchase_total_cents": int(order.get("purchase_cost_cents") or 0),
                "profit_total_cents": int(order.get("profit_cents") or 0),
            }
        )

    if not events:
        return {
            "total": 0,
            "items": [],
            "limit": limit,
            "offset": offset,
            "kpis": {
                "customers_count": 0,
                "repeat_customers_count": 0,
                "repeat_customers_rate_pct": 0.0,
                "orders_total_count": 0,
                "avg_orders_per_customer": 0.0,
                "revenue_total_cents": 0,
                "avg_revenue_per_customer_cents": 0,
                "with_email_count": 0,
                "with_phone_count": 0,
                "with_address_count": 0,
                "cross_market_customers_count": 0,
                "shopify_customers_count": 0,
                "kaufland_customers_count": 0,
            },
        }

    parent = list(range(len(events)))
    rank = [0] * len(events)
    token_to_index: dict[str, int] = {}

    for index, event in enumerate(events):
        tokens = _event_identity_tokens(event)
        if not tokens:
            tokens.add(f"fallback:{event['marketplace']}:{event['order_id']}")
        for token in tokens:
            existing = token_to_index.get(token)
            if existing is None:
                token_to_index[token] = index
            else:
                _union_find_union(parent, rank, existing, index)

    grouped_indices: dict[int, list[int]] = defaultdict(list)
    for index in range(len(events)):
        root = _union_find_find(parent, index)
        grouped_indices[root].append(index)

    customers: list[dict[str, Any]] = []
    for group_indices in grouped_indices.values():
        name_counter: Counter[str] = Counter()
        email_counter: Counter[str] = Counter()
        phone_counter: Counter[str] = Counter()
        market_counter: Counter[str] = Counter()
        article_counter: Counter[str] = Counter()
        address_counter: Counter[str] = Counter()
        address_map: dict[str, dict[str, Any]] = {}

        order_seen: set[str] = set()
        revenue_total = 0
        after_fees_total = 0
        purchase_total = 0
        profit_total = 0
        first_order: Optional[datetime] = None
        last_order: Optional[datetime] = None

        for idx in group_indices:
            event = events[idx]
            event_market = str(event.get("marketplace") or "").strip().lower()
            event_order_id = str(event.get("order_id") or "").strip()
            order_key = f"{event_market}:{event_order_id}"
            if order_key in order_seen:
                continue
            order_seen.add(order_key)

            market_counter[event_market] += 1
            revenue_total += int(event.get("revenue_total_cents") or 0)
            after_fees_total += int(event.get("after_fees_total_cents") or 0)
            purchase_total += int(event.get("purchase_total_cents") or 0)
            profit_total += int(event.get("profit_total_cents") or 0)

            article = str(event.get("article") or "").strip()
            if article and article != "-":
                article_counter[article] += 1

            parsed_order_date = _parse_iso(event.get("order_date"))
            if parsed_order_date is not None:
                if first_order is None or parsed_order_date < first_order:
                    first_order = parsed_order_date
                if last_order is None or parsed_order_date > last_order:
                    last_order = parsed_order_date

            for name in event.get("all_names", []):
                token = str(name).strip()
                if token:
                    name_counter[token] += 1
            if not event.get("all_names"):
                token = str(event.get("customer_name") or "").strip()
                if token:
                    name_counter[token] += 1

            for email in event.get("emails", []):
                normalized = _normalize_email(email)
                if normalized:
                    email_counter[normalized] += 1

            for phone in event.get("phones", []):
                normalized = _normalize_phone(phone)
                if normalized:
                    phone_counter[normalized] += 1

            for address in event.get("addresses", []):
                if not isinstance(address, dict):
                    continue
                key = _address_key(address)
                if not key:
                    continue
                address_counter[key] += 1
                if key not in address_map:
                    address_map[key] = address

        marketplaces = sorted([token for token in market_counter.keys() if token])
        primary_name = ""
        if name_counter:
            primary_name = name_counter.most_common(1)[0][0]
        if not primary_name and email_counter:
            primary_name = email_counter.most_common(1)[0][0]
        if not primary_name and phone_counter:
            primary_name = phone_counter.most_common(1)[0][0]
        if not primary_name:
            primary_name = "Unbekannt"

        primary_address: dict[str, Any] = {}
        if address_counter:
            key = address_counter.most_common(1)[0][0]
            primary_address = address_map.get(key, {})

        email_list = [item[0] for item in email_counter.most_common(3)]
        phone_list = [item[0] for item in phone_counter.most_common(3)]
        top_articles = [item[0] for item in article_counter.most_common(3)]

        first_order_iso = first_order.replace(microsecond=0).isoformat().replace("+00:00", "Z") if first_order else ""
        last_order_iso = last_order.replace(microsecond=0).isoformat().replace("+00:00", "Z") if last_order else ""

        customers.append(
            {
                "customer_id": "",
                "customer_name": primary_name,
                "marketplaces": marketplaces,
                "order_count": len(order_seen),
                "repeat_customer": len(order_seen) > 1,
                "first_order_date": first_order_iso,
                "last_order_date": last_order_iso,
                "revenue_total_cents": revenue_total,
                "after_fees_total_cents": after_fees_total,
                "purchase_total_cents": purchase_total,
                "profit_total_cents": profit_total,
                "emails": email_list,
                "phones": phone_list,
                "primary_address": primary_address,
                "top_articles": top_articles,
            }
        )

    search_token = _normalize_text(query).lower()
    if search_token:
        filtered_customers: list[dict[str, Any]] = []
        for customer in customers:
            address = _as_dict(customer.get("primary_address"))
            haystack_values = [
                customer.get("customer_name"),
                " ".join(customer.get("emails", [])),
                " ".join(customer.get("phones", [])),
                " ".join(customer.get("marketplaces", [])),
                " ".join(customer.get("top_articles", [])),
                address.get("street"),
                address.get("postcode"),
                address.get("city"),
                address.get("country"),
            ]
            haystack = " ".join(_normalize_text(value).lower() for value in haystack_values)
            if search_token in haystack:
                filtered_customers.append(customer)
        customers = filtered_customers

    customers.sort(
        key=lambda item: (
            str(item.get("last_order_date") or ""),
            int(item.get("revenue_total_cents") or 0),
            int(item.get("order_count") or 0),
        ),
        reverse=True,
    )

    for index, customer in enumerate(customers, start=1):
        customer["customer_id"] = f"CUST-{index:05d}"

    total_customers = len(customers)
    repeat_customers = sum(1 for customer in customers if bool(customer.get("repeat_customer")))
    orders_total = sum(int(customer.get("order_count") or 0) for customer in customers)
    revenue_total = sum(int(customer.get("revenue_total_cents") or 0) for customer in customers)

    with_email_count = sum(1 for customer in customers if bool(customer.get("emails")))
    with_phone_count = sum(1 for customer in customers if bool(customer.get("phones")))
    with_address_count = sum(
        1
        for customer in customers
        if isinstance(customer.get("primary_address"), dict) and _address_has_data(customer.get("primary_address") or {})
    )

    cross_market_count = sum(1 for customer in customers if len(customer.get("marketplaces") or []) > 1)
    shopify_customers = sum(1 for customer in customers if "shopify" in (customer.get("marketplaces") or []))
    kaufland_customers = sum(1 for customer in customers if "kaufland" in (customer.get("marketplaces") or []))

    safe_offset = max(int(offset), 0)
    safe_limit = max(int(limit), 1)
    paginated = customers[safe_offset : safe_offset + safe_limit]

    avg_orders_per_customer = round((orders_total / total_customers), 2) if total_customers > 0 else 0.0
    avg_revenue_per_customer = int(round(revenue_total / total_customers)) if total_customers > 0 else 0

    return {
        "total": total_customers,
        "items": paginated,
        "limit": safe_limit,
        "offset": safe_offset,
        "kpis": {
            "customers_count": total_customers,
            "repeat_customers_count": repeat_customers,
            "repeat_customers_rate_pct": _safe_pct(repeat_customers, total_customers),
            "orders_total_count": orders_total,
            "avg_orders_per_customer": avg_orders_per_customer,
            "revenue_total_cents": revenue_total,
            "avg_revenue_per_customer_cents": avg_revenue_per_customer,
            "with_email_count": with_email_count,
            "with_phone_count": with_phone_count,
            "with_address_count": with_address_count,
            "cross_market_customers_count": cross_market_count,
            "shopify_customers_count": shopify_customers,
            "kaufland_customers_count": kaufland_customers,
        },
    }
