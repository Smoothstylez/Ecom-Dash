from __future__ import annotations

import json
from typing import Any, Optional

from app.services import orders as orders_service
from app.services.importers.kaufland_live import (
    KauflandLiveClient,
    KauflandLiveError,
    _connect as connect_kaufland_source_db,
    load_kaufland_live_config,
    replace_order_unit_tracking_numbers,
)
from app.services.importers.shopify_live import (
    ShopifyLiveClient,
    ShopifyLiveError,
    _connect as connect_shopify_source_db,
    delete_stale_order_rows,
    load_shopify_live_config,
    upsert_fulfillment,
    upsert_line_item,
    upsert_order,
    upsert_transaction,
)


KAUFLAND_CARRIER_OPTIONS = [
    "DHL",
    "DHL Express",
    "DPD",
    "GLS",
    "Hermes",
    "UPS",
    "Fedex",
    "Deutsche Post",
    "Evri",
    "Royal Mail",
    "Packeta",
    "PostNL",
    "InPost",
    "Other",
    "Other Hauler",
    "4PX",
    "Allekurier",
    "Amazon Logistics DE (Swiship)",
    "Amazon Shipping (IT)",
    "Ambro Express",
    "Asendia",
    "Asendia Germany",
    "Austrian Post",
    "Bejot Logistics",
    "BRT Bartolini",
    "Bursped",
    "Cargoline",
    "Cargo International",
    "China Post",
    "Chronopost",
    "Chukou1 Logistics",
    "Colissimo",
    "Colis Prive",
    "CNE Express",
    "Correos",
    "Cubyn",
    "Czech Post",
    "Dachser",
    "DHL 2 MH",
    "DHL Ecommerce",
    "DHL Freight",
    "DHL Hong Kong",
    "DHL Poland Domestic",
    "DPD Austria",
    "DPD Czech Republic",
    "DPD France",
    "DPD Hungary",
    "DPD Netherlands",
    "DPD Poland",
    "DPD Romania",
    "DPD Slovakia",
    "DPD UK",
    "DSV",
    "ECE",
    "Emons",
    "Flyt Express",
    "Gebrüder Weiss",
    "Gebrüder Weiss Germany",
    "Geis",
    "Geis Poland",
    "Geodis",
    "GEL",
    "GLS Czech Republic",
    "GLS Italy",
    "GLS Poland",
    "Go Express and Logistics",
    "Hellmann",
    "Hermes 2 MH",
    "Hong Kong Post",
    "Hua Han Logistics",
    "IDS Logistik",
    "Iloxx",
    "Iloxx Spedition",
    "Jersey Post",
    "Kuehne & Nagel",
    "La Poste",
    "Mondial Relay",
    "Nexive",
    "Nova Post",
    "Orlen Paczka",
    "Overseas Territory FR EMS",
    "Poland Post",
    "PPL",
    "Post Haste",
    "Post Italiane",
    "PostNL 3S",
    "Pressio",
    "Raben Group",
    "Redur Spain",
    "Rhenus",
    "Royal Shipments",
    "Sailpost",
    "Schenker",
    "SDA",
    "Seur",
    "SFC Service",
    "SGT Corriere Espresso",
    "Siodemka",
    "Slovak Parcel Service",
    "Slovakia Post",
    "SPT Furniture Logistic",
    "Spedition Guettler",
    "Spring GDS",
    "Suus",
    "Sunyou",
    "TNT",
    "TNT Click",
    "TNT France",
    "TNT Italy",
    "TopTrans",
    "trans-o-flex",
    "Trans FM",
    "UBI Smart Parcel",
    "Wanb Express",
    "WeDo Logistics",
    "Winit",
    "WnDirect",
    "Yanwen",
    "YDH",
    "dtl",
    "Yun Express",
    "Zufall",
]

SHOPIFY_CARRIER_OPTIONS = [
    "DHL",
    "DHL Express",
    "DPD",
    "GLS",
    "Hermes",
    "UPS",
    "FedEx",
    "USPS",
    "Deutsche Post",
    "Evri",
    "Royal Mail",
    "Packeta",
    "PostNL",
    "Inpost",
    "4PX",
    "AGS",
    "Amazon Logistics UK",
    "Amazon Logistics US",
    "An Post",
    "Anjun Logistics",
    "APC",
    "Asendia USA",
    "Australia Post",
    "Bonshaw",
    "BPost",
    "BPost International",
    "Canada Post",
    "Canpar",
    "CDL Last Mile",
    "China Post",
    "Chronopost",
    "Chukou1",
    "Colissimo",
    "Comingle",
    "Coordinadora",
    "Correios",
    "Correos",
    "CTT",
    "CTT Express",
    "Cyprus Post",
    "Delnext",
    "DHL eCommerce",
    "DHL eCommerce Asia",
    "DPD Local",
    "DPD UK",
    "DTD Express",
    "DX",
    "Eagle",
    "Estes",
    "First Global Logistics",
    "First Line",
    "FSC",
    "Fulfilla",
    "Guangdong Weisuyi Information Technology (WSE)",
    "Heppner Internationale Spedition GmbH & Co.",
    "Iceland Post",
    "IDEX",
    "Israel Post",
    "Japan Post (EN)",
    "Japan Post (JA)",
    "La Poste",
    "Lasership",
    "Latvia Post",
    "Lietuvos Paštas",
    "Logisters",
    "Lone Star Overnight",
    "M3 Logistics",
    "Meteor Space",
    "Mondial Relay",
    "New Zealand Post",
    "NinjaVan",
    "North Russia Supply Chain (Shenzhen) Co.",
    "OnTrac",
    "Pago Logistics",
    "Ping An Da Tengfei Express",
    "Pitney Bowes",
    "Portal PostNord",
    "Poste Italiane",
    "PostNord DK",
    "PostNord NO",
    "PostNord SE",
    "Purolator",
    "Qxpress",
    "Qyun Express",
    "Royal Shipments",
    "Sagawa (EN)",
    "Sagawa (JA)",
    "Sendle",
    "SF Express",
    "SFC Fulfillment",
    "SHREE NANDAN COURIER",
    "Singapore Post",
    "Southwest Air Cargo",
    "StarTrack",
    "Step Forward Freight",
    "Swiss Post",
    "TForce Final Mile",
    "Tinghao",
    "TNT",
    "Toll IPEC",
    "United Delivery Service",
    "Venipak",
    "We Post",
    "Whistl",
    "Wizmo",
    "WMYC",
    "Xpedigo",
    "XPO Logistics",
    "Yamato (EN)",
    "Yamato (JA)",
    "YiFan Express",
    "YunExpress",
]

KAUFLAND_TRACKING_OPTIONAL_CARRIERS = {"Other", "Other Hauler"}
KAUFLAND_SHIPPABLE_STATUSES = {"need_to_be_sent"}
KAUFLAND_SHIPPED_STATUSES = {"sent", "sent_and_autopaid", "received"}
SHOPIFY_SHIPPED_KEYWORDS = ("fulfilled", "shipped", "delivered", "success", "sent")
RETURN_LIKE_KEYWORDS = (
    "cancel",
    "cancelled",
    "canceled",
    "void",
    "return",
    "returned",
    "refund",
    "refunded",
    "partially_refunded",
    "rma",
    "revoked",
    "returning",
)


class OrderShipmentError(RuntimeError):
    pass


def _text(value: Any) -> str:
    return str(value or "").strip()


def _token(value: Any) -> str:
    return _text(value).lower()


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _is_return_like(value: Any) -> bool:
    normalized = _token(value)
    return bool(normalized) and any(keyword in normalized for keyword in RETURN_LIKE_KEYWORDS)


def _is_shopify_shipped_like(value: Any) -> bool:
    normalized = _token(value)
    return bool(normalized) and any(keyword in normalized for keyword in SHOPIFY_SHIPPED_KEYWORDS)


def _coerce_record(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _coerce_records(value: Any) -> list[dict[str, Any]]:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def build_shipment_capabilities(detail: dict[str, Any]) -> dict[str, Any]:
    summary = _coerce_record(detail.get("summary"))
    marketplace = _token(summary.get("marketplace"))

    if marketplace == "amazon":
        return {
            "available": False,
            "marketplace": "amazon",
            "mode": "amazon_fba",
            "carrier_options": [],
            "pending_units": [],
            "pending_units_count": 0,
            "requires_tracking_number": False,
            "reason": "Amazon-FBA-Bestellungen werden von Amazon versendet und koennen hier nicht manuell versendet werden.",
        }

    if marketplace == "kaufland":
        units = _coerce_records(detail.get("units"))
        pending_units = [
            {
                "id_order_unit": _text(unit.get("id_order_unit")),
                "product_title": _text(unit.get("product_title")) or "-",
                "status": _text(unit.get("status")) or "unknown",
            }
            for unit in units
            if _token(unit.get("status")) in KAUFLAND_SHIPPABLE_STATUSES
        ]
        if pending_units:
            return {
                "available": True,
                "marketplace": "kaufland",
                "mode": "bulk_order_units",
                "carrier_options": KAUFLAND_CARRIER_OPTIONS,
                "pending_units": pending_units,
                "pending_units_count": len(pending_units),
                "requires_tracking_number": True,
            }

        unit_statuses = [_token(unit.get("status")) for unit in units if _text(unit.get("status"))]
        reason = "Diese Kaufland-Bestellung hat keine versendbaren Order Units mehr."
        if unit_statuses and all(status == "open" for status in unit_statuses):
            reason = "Kaufland erlaubt den Versand erst, wenn die Order Units von 'open' auf 'need_to_be_sent' gewechselt sind."
        elif any(status in KAUFLAND_SHIPPED_STATUSES for status in unit_statuses):
            reason = "Diese Kaufland-Bestellung ist bereits als versendet markiert."
        elif any(_is_return_like(status) for status in unit_statuses):
            reason = "Stornierte oder erstattete Kaufland-Units koennen nicht versendet werden."
        return {
            "available": False,
            "marketplace": "kaufland",
            "mode": "bulk_order_units",
            "carrier_options": KAUFLAND_CARRIER_OPTIONS,
            "pending_units": [],
            "pending_units_count": 0,
            "requires_tracking_number": True,
            "reason": reason,
        }

    statuses = [
        summary.get("fulfillment_status"),
        summary.get("raw_status"),
        summary.get("financial_status"),
    ]
    if any(_is_return_like(value) for value in statuses):
        return {
            "available": False,
            "marketplace": "shopify",
            "mode": "fulfillment_order",
            "carrier_options": SHOPIFY_CARRIER_OPTIONS,
            "pending_units": [],
            "pending_units_count": 0,
            "requires_tracking_number": True,
            "reason": "Stornierte oder erstattete Shopify-Bestellungen werden nicht erneut versendet.",
        }
    if any(_is_shopify_shipped_like(value) for value in statuses):
        return {
            "available": False,
            "marketplace": "shopify",
            "mode": "fulfillment_order",
            "carrier_options": SHOPIFY_CARRIER_OPTIONS,
            "pending_units": [],
            "pending_units_count": 0,
            "requires_tracking_number": True,
            "reason": "Diese Shopify-Bestellung ist bereits als versendet markiert.",
        }

    line_items = _coerce_records(detail.get("line_items"))
    pending_units = [
        {
            "id": _text(item.get("id")) or f"line-{index}",
            "product_title": _text(item.get("title")) or "-",
            "status": _text(item.get("fulfillment_status")) or "open",
        }
        for index, item in enumerate(line_items)
    ]
    return {
        "available": True,
        "marketplace": "shopify",
        "mode": "fulfillment_order",
        "carrier_options": SHOPIFY_CARRIER_OPTIONS,
        "pending_units": pending_units,
        "pending_units_count": len(pending_units),
        "requires_tracking_number": True,
    }


def attach_shipment_capabilities(detail: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
    if not isinstance(detail, dict):
        return detail
    detail["shipment_capabilities"] = build_shipment_capabilities(detail)
    return detail


def _validate_carrier(carrier: str, allowed_values: list[str], marketplace: str) -> str:
    normalized = _text(carrier)
    if not normalized:
        raise OrderShipmentError("Bitte einen Versanddienstleister aus der Liste auswaehlen.")
    if normalized not in set(allowed_values):
        raise OrderShipmentError(f"Unbekannter Carrier fuer {marketplace}: {normalized}")
    return normalized


def _validate_tracking_number(marketplace: str, carrier: str, tracking_number: Optional[str]) -> str:
    normalized = _text(tracking_number)
    if marketplace == "kaufland" and carrier in KAUFLAND_TRACKING_OPTIONAL_CARRIERS:
        return normalized
    if not normalized:
        raise OrderShipmentError("Bitte eine Trackingnummer hinterlegen.")
    if any(character in normalized for character in "\n\r\t"):
        raise OrderShipmentError("Die Trackingnummer darf keine Zeilenumbrueche enthalten.")
    return normalized


def _persist_kaufland_shipment(order_id: str, unit_ids: list[str], carrier: str, tracking_number: str) -> None:
    tracking_numbers = [tracking_number] if tracking_number else []
    tracking_numbers_json = _json_dumps(tracking_numbers) if tracking_numbers else None
    with connect_kaufland_source_db() as connection:
        for unit_id in unit_ids:
            row = connection.execute(
                "SELECT raw_json FROM order_units WHERE id_order_unit = ? LIMIT 1",
                (unit_id,),
            ).fetchone()
            raw_json: dict[str, Any] = {}
            if row and _text(row["raw_json"]):
                try:
                    raw_json = _coerce_record(json.loads(str(row["raw_json"])))
                except (TypeError, ValueError, json.JSONDecodeError):
                    raw_json = {}
            raw_json["status"] = "sent"
            raw_json["carrier_code"] = carrier
            raw_json["tracking_numbers"] = tracking_numbers
            connection.execute(
                """
                UPDATE order_units
                SET status = ?, carrier_code = ?, tracking_numbers_json = ?, raw_json = ?, synced_at_iso = CURRENT_TIMESTAMP
                WHERE id_order_unit = ? AND id_order = ?
                """,
                ("sent", carrier, tracking_numbers_json, _json_dumps(raw_json), unit_id, order_id),
            )
            replace_order_unit_tracking_numbers(connection, unit_id, carrier, tracking_numbers)


def _ship_kaufland_order(order_id: str, detail: dict[str, Any], carrier: str, tracking_number: str) -> dict[str, Any]:
    capabilities = build_shipment_capabilities(detail)
    pending_units = _coerce_records(capabilities.get("pending_units"))
    if not pending_units:
        raise OrderShipmentError(_text(capabilities.get("reason")) or "Diese Kaufland-Bestellung ist nicht mehr versendbar.")

    config, missing, _summary = load_kaufland_live_config()
    if config is None:
        raise OrderShipmentError(
            "Kaufland-Zugangsdaten sind nicht konfiguriert: " + ", ".join(missing)
        )

    client = KauflandLiveClient(config)
    payload = {
        "carrier_code": carrier,
        "tracking_numbers": tracking_number,
    }
    shipped_unit_ids: list[str] = []
    try:
        for pending_unit in pending_units:
            unit_id = _text(pending_unit.get("id_order_unit"))
            if not unit_id:
                continue
            endpoint_url = f"{config.base_url.rstrip('/')}/order-units/{unit_id}/send"
            client.request_json("PATCH", endpoint_url, payload)
            shipped_unit_ids.append(unit_id)
    except Exception:
        if shipped_unit_ids:
            _persist_kaufland_shipment(order_id, shipped_unit_ids, carrier, tracking_number)
        raise

    _persist_kaufland_shipment(order_id, shipped_unit_ids, carrier, tracking_number)
    return {
        "marketplace": "kaufland",
        "carrier": carrier,
        "tracking_number": tracking_number,
        "shipped_unit_ids": shipped_unit_ids,
        "shipped_units_count": len(shipped_unit_ids),
        "playground": "playground" in _text(config.base_url).lower(),
        "base_url": config.base_url,
    }


def _line_items_for_fulfillment_order(fulfillment_order: dict[str, Any]) -> list[dict[str, int]]:
    line_items = _coerce_records(fulfillment_order.get("line_items"))
    payload_items: list[dict[str, int]] = []
    for line_item in line_items:
        item_id = line_item.get("id")
        fulfillable_quantity = line_item.get("fulfillable_quantity")
        try:
            normalized_id = int(item_id)
            normalized_quantity = int(fulfillable_quantity)
        except (TypeError, ValueError):
            continue
        if normalized_quantity <= 0:
            continue
        payload_items.append({"id": normalized_id, "quantity": normalized_quantity})
    return payload_items


def _refresh_shopify_order_from_live(client: ShopifyLiveClient, order_id: str) -> None:
    order = client.get_order(order_id)
    transactions = client.get_order_transactions(order_id)
    fulfillments = client.get_order_fulfillments(order_id)
    order["fulfillments"] = fulfillments
    line_items = _coerce_records(order.get("line_items"))
    with connect_shopify_source_db() as connection:
        upsert_order(connection, order)
        line_item_ids: list[str] = []
        for index, line_item in enumerate(line_items):
            result = upsert_line_item(connection, order_id, line_item, index)
            if result.get("ok") and _text(result.get("id")):
                line_item_ids.append(_text(result.get("id")))
        delete_stale_order_rows(connection, "order_line_items", order_id, line_item_ids)

        fulfillment_ids: list[str] = []
        for index, fulfillment in enumerate(fulfillments):
            result = upsert_fulfillment(connection, order_id, fulfillment, index)
            if result.get("ok") and _text(result.get("id")):
                fulfillment_ids.append(_text(result.get("id")))
        delete_stale_order_rows(connection, "order_fulfillments", order_id, fulfillment_ids)

        transaction_ids: list[str] = []
        for index, transaction in enumerate(transactions):
            result = upsert_transaction(connection, order_id, transaction, index)
            if result.get("ok") and _text(result.get("id")):
                transaction_ids.append(_text(result.get("id")))
        delete_stale_order_rows(connection, "order_transactions", order_id, transaction_ids)


def _ship_shopify_order(order_id: str, detail: dict[str, Any], carrier: str, tracking_number: str) -> dict[str, Any]:
    capabilities = build_shipment_capabilities(detail)
    if not bool(capabilities.get("available")):
        raise OrderShipmentError(_text(capabilities.get("reason")) or "Diese Shopify-Bestellung ist nicht mehr versendbar.")

    config, missing, _summary = load_shopify_live_config()
    if config is None:
        raise OrderShipmentError(
            "Shopify-Zugangsdaten sind nicht konfiguriert: " + ", ".join(missing)
        )

    client = ShopifyLiveClient(config)
    fulfillment_orders = client.get_order_fulfillment_orders(order_id)
    line_items_by_fulfillment_order: list[dict[str, Any]] = []
    fulfillment_order_ids: list[int] = []
    for fulfillment_order in fulfillment_orders:
        supported_actions = {
            _text(value)
            for value in fulfillment_order.get("supported_actions")
            if value is not None
        } if isinstance(fulfillment_order.get("supported_actions"), list) else set()
        if "create_fulfillment" not in supported_actions:
            continue
        try:
            fulfillment_order_id = int(fulfillment_order.get("id"))
        except (TypeError, ValueError):
            continue
        line_items = _line_items_for_fulfillment_order(fulfillment_order)
        if not line_items:
            continue
        line_items_by_fulfillment_order.append(
            {
                "fulfillment_order_id": fulfillment_order_id,
                "fulfillment_order_line_items": line_items,
            }
        )
        fulfillment_order_ids.append(fulfillment_order_id)

    if not line_items_by_fulfillment_order:
        raise OrderShipmentError(
            "Shopify meldet aktuell keine offenen Fulfillment Orders fuer diese Bestellung."
        )

    fulfillment = client.create_fulfillment(
        {
            "fulfillment": {
                "notify_customer": True,
                "tracking_info": {
                    "company": carrier,
                    "number": tracking_number,
                },
                "line_items_by_fulfillment_order": line_items_by_fulfillment_order,
            }
        }
    )
    _refresh_shopify_order_from_live(client, order_id)
    return {
        "marketplace": "shopify",
        "carrier": carrier,
        "tracking_number": tracking_number,
        "fulfillment_id": _text(fulfillment.get("id")),
        "fulfillment_order_ids": fulfillment_order_ids,
        "fulfillment_orders_count": len(fulfillment_order_ids),
    }


def submit_order_shipment(
    marketplace: str,
    order_id: str,
    *,
    carrier: str,
    tracking_number: Optional[str],
) -> dict[str, Any]:
    market = _token(marketplace)
    detail = orders_service.get_order_detail(market, order_id)
    if detail is None:
        raise OrderShipmentError("Bestellung wurde nicht gefunden.")

    if market == "kaufland":
        normalized_carrier = _validate_carrier(carrier, KAUFLAND_CARRIER_OPTIONS, "kaufland")
    elif market == "shopify":
        normalized_carrier = _validate_carrier(carrier, SHOPIFY_CARRIER_OPTIONS, "shopify")
    else:
        raise OrderShipmentError("Marketplace muss shopify oder kaufland sein.")

    normalized_tracking_number = _validate_tracking_number(market, normalized_carrier, tracking_number)
    try:
        if market == "kaufland":
            shipment_payload = _ship_kaufland_order(order_id, detail, normalized_carrier, normalized_tracking_number)
        else:
            shipment_payload = _ship_shopify_order(order_id, detail, normalized_carrier, normalized_tracking_number)
    except (KauflandLiveError, ShopifyLiveError) as exc:
        raise OrderShipmentError(str(exc)) from exc

    refreshed_detail = orders_service.get_order_detail(market, order_id)
    refreshed_detail = attach_shipment_capabilities(refreshed_detail)
    refreshed_summary = _coerce_record(refreshed_detail.get("summary")) if isinstance(refreshed_detail, dict) else {}
    return {
        "ok": True,
        "shipment": shipment_payload,
        "detail": refreshed_detail,
        "summary": refreshed_summary,
    }
