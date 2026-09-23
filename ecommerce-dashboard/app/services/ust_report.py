"""Kaufland-Bemessungsgrundlage, manuelle Steuersatzkorrektur, EU-Fernabsatzschwelle.

Kaufland `vat` ist ein PROZENTSATZ (19.0 / 0.0), kein Betrag. `price` und
`shipping_rate` sind Cent-Betraege als TEXT. Bemessungsgrundlage ist das
Kundenbrutto (price + shipping_rate) abzueglich Erstattungen; die Steuer wird
herausgerechnet (net = gross / 1.19), nie aufgeschlagen.

Die 42 `vat=0`-Zeilen sind Falschfelder (revenue_gross < revenue_net) und werden
nach `vat_effective_from` manuell auf 19 % korrigiert. Vor dem USt-Startdatum
ist 0 % korrekt (Kleinunternehmer) und wird nicht beanstandet.

EU-Fernabsatzschwelle: 10.000 EUR, in Vorjahr UND laufendem Jahr. NICHT zu
verwechseln mit der §19-UStG-Grenze (100.000 EUR).
"""
from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Iterable, Optional

from app.config import KAUFLAND_DB_PATH
from app.db import connect_combined_db
from app.services.ust_schema import EU_TAX_REGIME_HOME_RATE

EU_DISTANCE_SELLING_THRESHOLD_CENTS = 10_000 * 100
EU_MEMBER_STATE_CODES = frozenset({
    "AT", "BE", "BG", "CY", "CZ", "DK", "EE", "ES", "FI", "FR", "GR", "HR",
    "HU", "IE", "IT", "LT", "LU", "LV", "MT", "NL", "PL", "PT", "RO", "SE", "SI", "SK",
})

CLASS_KAUFLAND = "de_b2c"
CLASS_PRE_VAT = "pre_vat"
CLASS_NEEDS_OVERRIDE = "kaufland_rate_needs_override"
CLASS_DEEMED_SUPPLIER = "deemed_supplier"
CLASS_RETURN_LIKE = "return_like"

CANCELLED_STATES = ("cancelled", "canceled")


def _text(value: Any) -> str:
    return str(value or "").strip()


def _to_int(value: Any) -> int:
    if value is None or value == "":
        return 0
    try:
        return int(Decimal(str(value)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    except Exception:
        return 0


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _stable_id(prefix: str, value: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"ecom-dash:{prefix}:{value}"))


def _year_of(iso_value: str) -> str:
    return _text(iso_value)[:4]


def _month_of(iso_value: str) -> str:
    return _text(iso_value)[:7]


def split_vat_from_gross(gross_cents: int, rate_percent: float) -> tuple[int, int]:
    """net = gross / (1 + rate/100); vat = gross - net. Nie net * (1 + rate/100)."""
    sign = -1 if gross_cents < 0 else 1
    absolute = abs(int(gross_cents))
    factor = Decimal(1) + (Decimal(str(rate_percent)) / Decimal(100))
    if factor <= 0:
        return sign * absolute, 0
    net = int((Decimal(absolute) / factor).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    return sign * net, sign * (absolute - net)


def _connect_kaufland() -> sqlite3.Connection:
    connection = sqlite3.connect(KAUFLAND_DB_PATH)
    connection.row_factory = sqlite3.Row
    return connection


def _load_overrides() -> dict[str, dict[str, Any]]:
    with connect_combined_db() as connection:
        rows = connection.execute("SELECT * FROM kaufland_tax_overrides").fetchall()
    return {str(row["id_order_unit"]): dict(row) for row in rows}


def get_vat_effective_from() -> Optional[datetime]:
    with connect_combined_db() as connection:
        row = connection.execute(
            "SELECT vat_effective_from FROM seller_profiles WHERE id = 'default' LIMIT 1"
        ).fetchone()
    token = _text(row["vat_effective_from"]) if row else ""
    if not token:
        return None
    try:
        return datetime.strptime(token[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def load_kaufland_vat_rows(month: str) -> list[dict[str, Any]]:
    """Alle nicht stornierten Order-Units eines Monats mit Steuerbehandlung."""
    overrides = _load_overrides()
    vat_effective_from = get_vat_effective_from()
    with _connect_kaufland() as connection:
        rows = connection.execute(
            """
            SELECT u.id_order_unit, u.id_order, u.ts_created_iso, u.status, u.price, u.shipping_rate,
                   u.vat, u.is_marketplace_deemed_supplier, u.shipping_country,
                   COALESCE((SELECT SUM(CAST(NULLIF(r.amount, '') AS REAL)) FROM order_unit_refunds r
                             WHERE r.id_order_unit = u.id_order_unit), 0) AS refund_sum
            FROM order_units u
            WHERE substr(u.ts_created_iso, 1, 7) = ?
              AND COALESCE(u.status, '') NOT IN ('cancelled', 'canceled')
            ORDER BY u.ts_created_iso, u.id_order_unit
            """,
            (month,),
        ).fetchall()

    result: list[dict[str, Any]] = []
    for row in rows:
        shipping_country = _text(row["shipping_country"]).upper()
        price_cents = _to_int(row["price"])
        shipping_cents = _to_int(row["shipping_rate"])
        refund_cents = _to_int(row["refund_sum"])
        gross_cents = max(price_cents + shipping_cents - refund_cents, 0)
        rate = float(row["vat"] or 0)
        booking_date = _text(row["ts_created_iso"])[:10]
        override = overrides.get(str(row["id_order_unit"]))

        entry: dict[str, Any] = {
            "id_order_unit": str(row["id_order_unit"]),
            "order_id": str(row["id_order"] or ""),
            "booking_date": booking_date,
            "shipping_country": shipping_country,
            "vat_rate": rate,
            "gross_cents": gross_cents,
            "refund_cents": refund_cents,
            "net_source": "computed_from_rate",
            "rate_source": "api",
            "warnings": [],
        }

        if int(row["is_marketplace_deemed_supplier"] or 0):
            entry.update(
                tax_class=CLASS_DEEMED_SUPPLIER, net_cents=gross_cents,
                output_vat_cents=0, blocker=False,
            )
            result.append(entry)
            continue

        effective_rate = rate
        if override is not None:
            effective_rate = float(override["to_rate"])
            entry["rate_source"] = "override"
            entry["vat_rate"] = effective_rate

        if effective_rate >= 18.0:
            net_cents, vat_cents = split_vat_from_gross(gross_cents, effective_rate)
            entry.update(
                tax_class=CLASS_KAUFLAND, net_cents=net_cents,
                output_vat_cents=vat_cents, blocker=False,
            )
            result.append(entry)
            continue

        # rate == 0: vor dem USt-Start korrekt, danach Falschfeld.
        order_date = None
        try:
            order_date = datetime.strptime(booking_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            order_date = None
        if vat_effective_from is not None and order_date is not None and order_date < vat_effective_from:
            entry.update(
                tax_class=CLASS_PRE_VAT, net_cents=gross_cents,
                output_vat_cents=0, blocker=False,
            )
        else:
            entry.update(
                tax_class=CLASS_NEEDS_OVERRIDE, net_cents=gross_cents,
                output_vat_cents=0, blocker=True,
                warnings=["KAUFLAND_VAT_RATE_MISSING"],
            )
        result.append(entry)
    return result


def load_kaufland_returns(month: str) -> dict[str, Any]:
    """Retouren im Buchungsmonat (returns.ts_created_iso), nicht im Verkaufsmonat."""
    with _connect_kaufland() as connection:
        rows = connection.execute(
            """
            SELECT ru.id_return_unit, ru.id_return, ru.id_order_unit, ru.ts_created_iso
            FROM return_units ru
            WHERE substr(ru.ts_created_iso, 1, 7) = ?
            ORDER BY ru.ts_created_iso, ru.id_return_unit
            """,
            (month,),
        ).fetchall()
    order_unit_ids = [str(row["id_order_unit"]) for row in rows if row["id_order_unit"]]
    return {
        "count": len(rows),
        "order_unit_ids": order_unit_ids,
        "rows": [dict(row) for row in rows],
    }


def save_kaufland_override(
    id_order_unit: str,
    *,
    to_rate: float,
    reason: str,
    created_by: str = "admin",
    from_rate: Optional[float] = None,
) -> dict[str, Any]:
    """Manuelle Steuersatzkorrektur fuer Falschfelder (z. B. 0 % statt 19 %)."""
    unit_id = _text(id_order_unit)
    reason = _text(reason)
    if not unit_id:
        raise ValueError("id_order_unit fehlt")
    if not reason:
        raise ValueError("Eine Begruendung fuer die Korrektur ist erforderlich")
    if to_rate <= 0 or to_rate > 100:
        raise ValueError("to_rate muss zwischen 0 und 100 liegen")

    with _connect_kaufland() as connection:
        row = connection.execute(
            "SELECT price, shipping_rate, vat FROM order_units WHERE id_order_unit = ?", (unit_id,)
        ).fetchone()
    if row is None:
        raise ValueError("Order-Unit nicht gefunden")

    actual_from_rate = float(row["vat"] or 0) if from_rate is None else float(from_rate)
    gross_cents = max(_to_int(row["price"]) + _to_int(row["shipping_rate"]), 0)
    net_cents, vat_cents = split_vat_from_gross(gross_cents, to_rate)
    override_id = _stable_id("kaufland-override", f"{unit_id}:{to_rate}")

    with connect_combined_db() as connection:
        connection.execute(
            """
            INSERT INTO kaufland_tax_overrides(
                id, id_order_unit, from_rate, to_rate, gross_cents, net_cents, vat_cents,
                reason, created_at, created_by
            ) VALUES (?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(id_order_unit) DO UPDATE SET
                from_rate=excluded.from_rate, to_rate=excluded.to_rate,
                gross_cents=excluded.gross_cents, net_cents=excluded.net_cents,
                vat_cents=excluded.vat_cents, reason=excluded.reason,
                created_at=excluded.created_at, created_by=excluded.created_by
            """,
            (
                override_id, unit_id, actual_from_rate, float(to_rate), gross_cents,
                net_cents, vat_cents, reason, _utc_now(), _text(created_by) or "admin",
            ),
        )
    return {
        "id_order_unit": unit_id,
        "from_rate": actual_from_rate,
        "to_rate": float(to_rate),
        "gross_cents": gross_cents,
        "net_cents": net_cents,
        "vat_cents": vat_cents,
        "reason": reason,
    }


def bulk_override_zero_rates(
    month: str, *, reason: str, to_rate: float = 19.0, created_by: str = "admin"
) -> dict[str, Any]:
    """Korrigiert alle 0-%-Falschfelder eines Monats auf den Regelsatz."""
    rows = load_kaufland_vat_rows(month)
    overridden = 0
    for row in rows:
        if row["tax_class"] != CLASS_NEEDS_OVERRIDE:
            continue
        save_kaufland_override(
            row["id_order_unit"], to_rate=to_rate, reason=reason, created_by=created_by
        )
        overridden += 1
    return {"overridden": overridden, "month": month, "to_rate": float(to_rate)}


def evaluate_eu_b2c_regime(
    *,
    eu_tax_regime: str,
    eu_distance_prior_year_cents: int = 0,
    eu_distance_current_year_cents: int = 0,
) -> dict[str, Any]:
    """Heimsteuersatz nur bei bestaetigtem Regime UND Schwelle in BEIDEN Jahren."""
    threshold = EU_DISTANCE_SELLING_THRESHOLD_CENTS
    regime_ok = _text(eu_tax_regime) == EU_TAX_REGIME_HOME_RATE
    within = (
        0 <= int(eu_distance_prior_year_cents) < threshold
        and 0 <= int(eu_distance_current_year_cents) < threshold
    )
    if not regime_ok:
        reason = "eu_tax_regime ist nicht auf home_rate_under_threshold bestaetigt"
    elif not within:
        reason = "EU-Fernabsatzschwelle in Vorjahr oder laufendem Jahr erreicht"
    else:
        reason = "Heimsteuersatz zulaessig"
    return {
        "eu_tax_regime": _text(eu_tax_regime),
        "prior_year_cents": int(eu_distance_prior_year_cents),
        "current_year_cents": int(eu_distance_current_year_cents),
        "threshold_cents": threshold,
        "allow_home_rate": regime_ok and within,
        "reason": reason,
    }


def load_eu_distance_summary(year: str) -> dict[str, Any]:
    """EU-B2C-Umsaetze ausserhalb DE eines Kalenderjahres (Fernabsatzschwelle)."""
    kaufland_cents = 0
    try:
        with _connect_kaufland() as connection:
            rows = connection.execute(
                """
                SELECT price, shipping_rate, shipping_country FROM order_units
                WHERE substr(ts_created_iso, 1, 4) = ?
                  AND COALESCE(status, '') NOT IN ('cancelled', 'canceled')
                """,
                (year,),
            ).fetchall()
        for row in rows:
            country = _text(row["shipping_country"]).upper()
            if country == "DE" or country not in EU_MEMBER_STATE_CODES:
                continue
            kaufland_cents += max(_to_int(row["price"]) + _to_int(row["shipping_rate"]), 0)
    except sqlite3.Error:
        kaufland_cents = 0

    amazon_cents = 0
    try:
        with connect_combined_db() as connection:
            row = connection.execute(
                """
                SELECT COALESCE(SUM(gross_cents), 0) AS total FROM amazon_tax_rows
                WHERE substr(booking_date, 1, 4) = ?
                  AND ship_to_country != 'DE'
                  AND ship_to_country IN ({})
                """.format(",".join("?" * len(EU_MEMBER_STATE_CODES))),
                (year, *sorted(EU_MEMBER_STATE_CODES)),
            ).fetchone()
        amazon_cents = int(row["total"] or 0) if row else 0
    except sqlite3.Error:
        amazon_cents = 0

    return {
        "year": year,
        "eu_b2c_gross_cents": kaufland_cents + amazon_cents,
        "kaufland_cents": kaufland_cents,
        "amazon_cents": amazon_cents,
        "threshold_cents": EU_DISTANCE_SELLING_THRESHOLD_CENTS,
        "includes_domestic": False,
    }


def get_eu_tax_settings() -> dict[str, Any]:
    with connect_combined_db() as connection:
        row = connection.execute(
            "SELECT eu_tax_regime, eu_distance_prior_year_cents, eu_distance_current_year_cents "
            "FROM seller_profiles WHERE id = 'default' LIMIT 1"
        ).fetchone()
    if row is None:
        return {"eu_tax_regime": "unconfirmed", "eu_distance_prior_year_cents": 0,
                "eu_distance_current_year_cents": 0}
    return {
        "eu_tax_regime": _text(row["eu_tax_regime"]) or "unconfirmed",
        "eu_distance_prior_year_cents": int(row["eu_distance_prior_year_cents"] or 0),
        "eu_distance_current_year_cents": int(row["eu_distance_current_year_cents"] or 0),
    }


def set_eu_tax_settings(
    *,
    eu_tax_regime: Optional[str] = None,
    eu_distance_prior_year_cents: Optional[int] = None,
    eu_distance_current_year_cents: Optional[int] = None,
) -> dict[str, Any]:
    from app.services.ust_schema import EU_TAX_REGIMES

    updates: dict[str, Any] = {}
    if eu_tax_regime is not None:
        if _text(eu_tax_regime) not in EU_TAX_REGIMES:
            raise ValueError("unzulaessiges eu_tax_regime")
        updates["eu_tax_regime"] = _text(eu_tax_regime)
    if eu_distance_prior_year_cents is not None:
        updates["eu_distance_prior_year_cents"] = int(eu_distance_prior_year_cents)
    if eu_distance_current_year_cents is not None:
        updates["eu_distance_current_year_cents"] = int(eu_distance_current_year_cents)
    if not updates:
        return get_eu_tax_settings()
    assignments = ", ".join(f"{key} = ?" for key in updates)
    with connect_combined_db() as connection:
        connection.execute(
            f"UPDATE seller_profiles SET {assignments} WHERE id = 'default'", list(updates.values())
        )
    return get_eu_tax_settings()
