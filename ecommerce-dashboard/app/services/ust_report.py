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
        parsed = datetime.fromisoformat(token.replace("Z", "+00:00"))
        return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)
    except ValueError:
        return None


def load_kaufland_vat_rows(month: str, *, _include_refunds: bool = True) -> list[dict[str, Any]]:
    """Alle nicht stornierten Order-Units eines Monats mit Steuerbehandlung."""
    overrides = _load_overrides()
    vat_effective_from = get_vat_effective_from()
    with _connect_kaufland() as connection:
        rows = connection.execute(
            """
            SELECT u.id_order_unit, u.id_order, u.ts_created_iso, u.status, u.price, u.shipping_rate,
                   u.vat, u.is_marketplace_deemed_supplier, u.shipping_country, u.revenue_gross,
                   0 AS refund_sum
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
            "fees_cents": max(price_cents - _to_int(row["revenue_gross"]), 0),
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

        order_date = datetime.fromisoformat(_text(row["ts_created_iso"]).replace("Z", "+00:00"))
        if order_date.tzinfo is None:
            order_date = order_date.replace(tzinfo=timezone.utc)
        if vat_effective_from is not None and order_date < vat_effective_from:
            entry.update(tax_class=CLASS_PRE_VAT, net_cents=gross_cents,
                         output_vat_cents=0, blocker=False, vat_rate=0)
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

        entry.update(
            tax_class=CLASS_NEEDS_OVERRIDE, net_cents=gross_cents,
            output_vat_cents=0, blocker=True,
            warnings=["KAUFLAND_VAT_RATE_MISSING"],
        )
        result.append(entry)
    if not _include_refunds:
        return result
    # Corrections keep their own date and inherit the original sale's treatment.
    # Never use the import timestamp or a return-request timestamp as refund date.
    with _connect_kaufland() as connection:
        refunds = connection.execute(
            "SELECT r.*, u.ts_created_iso AS sale_date FROM order_unit_refunds r "
            "JOIN order_units u ON u.id_order_unit=r.id_order_unit "
            "WHERE substr(u.ts_created_iso,1,7) <= ?", (month,)
        ).fetchall()
    originals: dict[str, dict[str, Any]] = {r["id_order_unit"]: r for r in result}
    for refund in refunds:
        amount = abs(_to_int(refund["amount"]))
        if not amount:
            continue
        try:
            raw = _json.loads(refund["raw_json"] or "{}")
        except (ValueError, TypeError):
            raw = {}
        date = next((_text(raw.get(key)) for key in
                     ("booking_date", "ts_refunded_iso", "ts_created_iso", "created_at", "date")
                     if _text(raw.get(key))), "")
        if date:
            try:
                datetime.fromisoformat(date.replace("Z", "+00:00"))
            except ValueError:
                date = ""
        if not date:
            # Unknown corrections may affect any later unfiled month.
            result.append({"id_order_unit": str(refund["id_order_unit"]), "order_id": "",
                           "booking_date": "", "tax_class": "unresolved_refund_date",
                           "gross_cents": 0, "net_cents": 0, "output_vat_cents": 0,
                           "refund_cents": amount, "fees_cents": 0, "blocker": True,
                           "warnings": ["KAUFLAND_REFUND_DATE_MISSING"]})
            continue
        if _month_of(date) != month:
            continue
        unit_id = str(refund["id_order_unit"])
        if unit_id not in originals:
            sale_month = _month_of(refund["sale_date"])
            # Only sales are needed here, never recursively load their refunds.
            originals.update({r["id_order_unit"]: r for r in load_kaufland_vat_rows(sale_month, _include_refunds=False)})
        original = originals.get(unit_id)
        if original is None:
            continue
        entry = {**original, "booking_date": date[:10], "transaction_type": "REFUND",
                 "refund_cents": amount, "gross_cents": -amount, "fees_cents": 0}
        net, vat = split_vat_from_gross(-amount, original["vat_rate"]) if original["output_vat_cents"] else (-amount, 0)
        entry.update(net_cents=net, output_vat_cents=vat)
        result.append(entry)
    return result


def kaufland_returns_synced() -> bool:
    """True, wenn ein erfolgreicher Sync die Retouren tatsaechlich abgerufen hat.

    Kaufland hat moeglicherweise schlicht keine Retouren. Dann bedeutet 0 auch 0
    -- und eine Warnung waere ein Fehlalarm. Geprueft wird deshalb der Sync-
    Nachweis (`include_returns: true`, `status: success`), nicht nur die Zeilenzahl.
    """
    try:
        with _connect_kaufland() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS n FROM sync_runs "
                "WHERE status = 'success' AND summary_json LIKE '%\"include_returns\": true%'"
            ).fetchone()
        return int(row["n"] if row else 0) > 0
    except _sqlite3.Error:
        return False


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
    now = _utc_now()
    with connect_combined_db() as connection:
        # Die Profilzeile kann fehlen (frisch angelegtes Schema). Ohne sie wuerde
        # das UPDATE ins Leere laufen und die Einstellung stillschweigend verloren.
        connection.execute(
            "INSERT INTO seller_profiles(id, legal_name, created_at, updated_at) "
            "VALUES ('default', '', ?, ?) ON CONFLICT(id) DO NOTHING",
            (now, now),
        )
        connection.execute(
            f"UPDATE seller_profiles SET {assignments}, updated_at = ? WHERE id = 'default'",
            [*updates.values(), now],
        )
    settings = get_eu_tax_settings()
    # Steuerklassen haengen an der EU-Verkaufsregel: bereits importierte
    # Amazon-Zeilen muessen nachgezogen werden, sonst bleibt 'unresolved'.
    from app.services.amazon_tax_import import reclassify_all_rows

    reclassify_all_rows(
        eu_tax_regime=settings["eu_tax_regime"],
        eu_distance_prior_year_cents=settings["eu_distance_prior_year_cents"],
        eu_distance_current_year_cents=settings["eu_distance_current_year_cents"],
    )
    return settings


# ── Task 8: Report-Builder, Sperrlogik, Snapshots + Amendments ──────────────
# Sperrlogik (verbindlich):
#   hart (verhindert `filed`): AMAZON_UNRESOLVED, KAUFLAND_RATE_NEEDS_OVERRIDE,
#     INPUT_VAT_PENDING_REVIEW, TAX_MODE_NOT_REGULAR, NO_VAT_START_DATE,
#     UNRESOLVED_RETURN_LINK
#   weich (Warnung + input_vat_incomplete): MISSING_FEE_INVOICE. Nach Punkt 9
#     kein harter Blocker: die fehlende Gebuehrenrechnung macht die
#     Ausgangs-USt nicht falsch. Nur die benannte Business-Regel
#     `block_filing_when_input_vat_incomplete` (Default false) macht Blocker.
#
# Snapshots: `filed` ist unveranderlich. Korrekturen laufen ausschliesslich
# ueber `amend_report` als neue Revision (kind='amendment').

import json as _json
import sqlite3 as _sqlite3

from app.services import ust_documents as _docs
from app.services.tax_reporting import get_tax_settings

BLOCKER_AMAZON_UNRESOLVED = "AMAZON_UNRESOLVED"
BLOCKER_KAUFLAND_RATE = "KAUFLAND_RATE_NEEDS_OVERRIDE"
BLOCKER_INPUT_VAT_PENDING = "INPUT_VAT_PENDING_REVIEW"
BLOCKER_TAX_MODE = "TAX_MODE_NOT_REGULAR"
BLOCKER_NO_VAT_START = "NO_VAT_START_DATE"
BLOCKER_RETURN_LINK = "UNRESOLVED_RETURN_LINK"
WARNING_MISSING_FEE_INVOICE = "MISSING_FEE_INVOICE"

CORRECTION_TYPES = frozenset({"RETURN", "REFUND"})


def _shift_month(month: str, steps: int = 1) -> str:
    parsed = datetime.strptime(month[:7], "%Y-%m")
    year = parsed.year + (parsed.month - 1 + steps) // 12
    month_index = (parsed.month - 1 + steps) % 12 + 1
    return f"{year:04d}-{month_index:02d}"


def load_amazon_tax_rows(month: str) -> list[dict[str, Any]]:
    with connect_combined_db() as connection:
        rows = connection.execute(
            "SELECT * FROM amazon_tax_rows WHERE substr(booking_date, 1, 7) = ? OR booking_date='' "
            "ORDER BY booking_date, id",
            (month,),
        ).fetchall()
    from app.services.ust_reconciliation import apply_amazon_cutoff
    return apply_amazon_cutoff([dict(row) for row in rows], get_vat_effective_from())


def _bucket_amazon(rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    def empty() -> dict[str, Any]:
        return {"count": 0, "gross": 0, "net": 0, "output_vat": 0}

    classes = ("de_b2c", "eu_b2b_intra_community_supply", "eu_b2c_home_rate",
               "unresolved", "deemed_supplier", "export", "unresolved_return_link", "pre_vat")
    buckets: dict[str, Any] = {name: empty() for name in classes}
    buckets["returns"] = {"count": 0, "gross": 0, "net": 0, "output_vat": 0,
                          "by_original_class": {}}
    for row in rows:
        is_correction = _text(row.get("transaction_type")).upper() in CORRECTION_TYPES
        target = buckets["returns"] if is_correction else buckets.get(row["tax_class"])
        if target is None:
            target = buckets["unresolved"]
        target["count"] += 1
        target["gross"] += int(row.get("gross_cents") or 0)
        target["net"] += int(row.get("net_cents") or 0)
        target["output_vat"] += int(row.get("output_vat_cents") or 0)
        if is_correction:
            original = _text(row.get("original_tax_class")) or row.get("tax_class") or "unknown"
            target["by_original_class"][original] = target["by_original_class"].get(original, 0) + 1
    return buckets


def detect_missing_fee_invoices(month: str) -> list[dict[str, Any]]:
    """Gebuehren in `month` angefallen, aber keine freigegebene Gebuehrenrechnung."""
    findings: list[dict[str, Any]] = []
    from app.services.ust_reconciliation import fee_invoice_coverage, has_full_kaufland_statement
    try:
        coverage = fee_invoice_coverage()
    except sqlite3.Error:
        coverage = {"amazon": {}, "kaufland": {}}
    fees = {"kaufland": sum(int(row.get("fees_cents") or 0) for row in load_kaufland_vat_rows(month))}
    for provider, amount in fees.items():
        covered = int(coverage[provider].get(month, 0))
        if amount > 0 and covered < amount - 2:
            code = WARNING_MISSING_FEE_INVOICE if not covered else "FEE_RECONCILIATION_DIFFERENCE"
            if provider == "kaufland" and covered:
                try:
                    if has_full_kaufland_statement(month):
                        code = "FEE_SOURCE_ESTIMATE_DIFFERENCE"
                except sqlite3.Error:
                    pass
            findings.append({"code": code, "provider": provider,
                             "fees_cents": amount, "covered_invoice_gross_cents": covered,
                             "hint": f"{provider.title()}: Gebuehrenschaetzung aus Quelldaten und freigegebene Rechnungen stimmen noch nicht ueberein." if covered else
                                     f"{provider.title()}-Gebuehren angefallen, aber keine freigegebene Rechnung fuer diesen Leistungsmonat."})
    return findings


def build_ust_report(
    month: str, *, block_filing_when_input_vat_incomplete: bool = False
) -> dict[str, Any]:
    month = _text(month)[:7]
    settings = get_tax_settings()
    eu_settings = get_eu_tax_settings()

    kaufland_rows = load_kaufland_vat_rows(month)
    kaufland_returns = load_kaufland_returns(month)
    amazon_rows = load_amazon_tax_rows(month)
    amazon = _bucket_amazon(row for row in amazon_rows if row.get("booking_date"))
    from app.services.ust_input_vat import FeeTaxContext
    fee_context = FeeTaxContext(settings.vat_effective_from)
    input_vat = _docs.sum_input_vat_by_deduction_month(month, fee_tax_context=fee_context)
    from app.services.ust_reconciliation import VAT_BUCKETS, booked_input_vat, amazon_completeness
    booked_vat = booked_input_vat(month, fee_tax_context=fee_context)
    booked_fee_vat_cents = sum(int(booked_vat[key]) for key in VAT_BUCKETS)
    for key in VAT_BUCKETS:
        input_vat[key] += int(booked_vat[key])
    for provider, service_month in booked_vat["fee_service_months"].items():
        if service_month and not input_vat["fee_service_months"].get(provider):
            input_vat["fee_service_months"][provider] = service_month
    input_vat["pending_review_count"] += booked_vat["pending_review_count"]
    input_vat["eligibility_adjustment_cents"] += booked_vat["eligibility_adjustment_cents"]
    input_vat["eligibility_review_count"] += booked_vat["eligibility_review_count"]
    completeness = amazon_completeness(month)
    missing_fee = detect_missing_fee_invoices(month)

    kaufland_output_vat = sum(int(row["output_vat_cents"]) for row in kaufland_rows)
    kaufland_gross = sum(int(row["gross_cents"]) for row in kaufland_rows)
    kaufland_net = sum(int(row["net_cents"]) for row in kaufland_rows)
    amazon_output_vat = sum(
        amazon[name]["output_vat"] for name in
        ("de_b2c", "eu_b2b_intra_community_supply", "eu_b2c_home_rate", "returns")
    )
    output_vat_cents = kaufland_output_vat + amazon_output_vat
    input_vat_cents = sum(
        int(input_vat[key]) for key in
        ("purchases_cents", "amazon_fees_cents", "kaufland_fees_cents", "other_cents")
    )

    blockers: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    if completeness["source_error"]:
        warnings.append({"code": "AMAZON_SOURCE_UNAVAILABLE", "count": 1,
                         "hint": "Amazon-Quelle konnte nicht vollstaendig geprueft werden."})
    if completeness["missing_order_ids"] or completeness["amount_mismatches"]:
        warnings.append({"code": "AMAZON_TAX_DATA_INCOMPLETE",
                         "count": len(completeness["missing_order_ids"]) + len(completeness["amount_mismatches"]),
                         "hint": "Amazon-Lieferungen oder Erstattungen fehlen im Steuerreport; Monatsreports importieren."})
    ambiguous = sum(bool(r.get("cutoff_ambiguous")) for r in amazon_rows)
    if ambiguous:
        blockers.append({"code": "AMAZON_VAT_START_AMBIGUOUS", "count": ambiguous,
                         "hint": "Am USt-Starttag fehlt der eindeutige Bestellzeitpunkt."})
    undated_refunds = sum(r["tax_class"] == "unresolved_refund_date" for r in kaufland_rows)
    if undated_refunds:
        blockers.append({"code": "KAUFLAND_REFUND_DATE_MISSING", "count": undated_refunds,
                         "hint": "Kaufland-Erstattungen ohne belegtes Buchungsdatum; Monatszuordnung ungeklart."})
    if booked_vat["source_error"]:
        blockers.append({"code": "INPUT_VAT_SOURCE_UNAVAILABLE", "count": 1,
                         "hint": "Gebuchte Vorsteuer konnte nicht vollstaendig gelesen werden."})
    if booked_vat["conflicts"]:
        blockers.append({"code": "INPUT_VAT_INVOICE_CONFLICT", "count": len(booked_vat["conflicts"]),
                         "hint": "Doppelte Eingangsrechnung mit abweichender Freigabe, Vorsteuer oder Abzugsmonat."})
    if input_vat["eligibility_review_count"]:
        blockers.append({"code": "INPUT_VAT_ELIGIBILITY_UNRESOLVED", "count": input_vat["eligibility_review_count"],
                         "hint": "Gebuehren oder Gebuehrengutschriften koennen nicht eindeutig vor/nach USt-Beginn zugeordnet werden."})
    if input_vat["eligibility_adjustment_cents"]:
        warnings.append({"code": "INPUT_VAT_START_ADJUSTMENT", "count": 1,
                         "adjustment_cents": input_vat["eligibility_adjustment_cents"],
                         "hint": "Abziehbare Vorsteuer wurde an den USt-Beginn angepasst; Altumsatz-Gebuehrengutschriften erben die urspruengliche Behandlung."})
    undated_amazon = sum(not r.get("booking_date") for r in amazon_rows)
    if undated_amazon:
        blockers.append({"code": "AMAZON_TRANSACTION_DATE_MISSING", "count": undated_amazon,
                         "hint": "Amazon-Transaktion ohne belegtes Buchungsdatum; Zuordnung ungeklart."})
    # Undated transactions are shown as issues only, never netted against a
    # month's revenue based on the original order or the import timestamp.

    unresolved_amazon = [
        row for row in amazon_rows
        if _text(row.get("tax_class")) in ("unresolved", "unresolved_return_link")
    ]
    if any(_text(row.get("tax_class")) == "unresolved_return_link" for row in unresolved_amazon):
        blockers.append({"code": BLOCKER_RETURN_LINK,
                         "count": sum(1 for row in unresolved_amazon
                                      if _text(row.get("tax_class")) == "unresolved_return_link"),
                         "hint": "Retoure ohne eindeutigen Ursprungs-SHIPMENT."})
    if any(_text(row.get("tax_class")) == "unresolved" for row in unresolved_amazon):
        blockers.append({"code": BLOCKER_AMAZON_UNRESOLVED,
                         "count": sum(1 for row in unresolved_amazon
                                      if _text(row.get("tax_class")) == "unresolved"),
                         "hint": "Amazon-Sonderfaelle ohne bestaetigte Steuerbehandlung."})

    kaufland_overrides_pending = sum(
        1 for row in kaufland_rows if row["tax_class"] == CLASS_NEEDS_OVERRIDE
    )
    if kaufland_overrides_pending:
        blockers.append({"code": BLOCKER_KAUFLAND_RATE,
                         "count": kaufland_overrides_pending,
                         "hint": "Kaufland `vat=0` ist ein Falschfeld und muss auf 19 % gesetzt werden."})

    pending_review_count = int(input_vat.get("pending_review_count") or 0)
    if pending_review_count:
        blockers.append({"code": BLOCKER_INPUT_VAT_PENDING,
                         "count": pending_review_count,
                         "hint": "Eingangsrechnungen ohne Freigabe."})

    if _normalize_tax_mode_for_report(settings.tax_mode) != "regular":
        blockers.append({"code": BLOCKER_TAX_MODE, "count": 1,
                         "hint": "Verkaeuferprofil steht nicht auf Regelbesteuerung."})
    if settings.vat_effective_from is None:
        blockers.append({"code": BLOCKER_NO_VAT_START, "count": 1,
                         "hint": "Kein USt-Startzeitpunkt gesetzt."})

    fee_incomplete = [finding for finding in missing_fee if finding["code"] == WARNING_MISSING_FEE_INVOICE]
    input_vat_incomplete = pending_review_count > 0 or bool(fee_incomplete) or input_vat["eligibility_review_count"] > 0
    warnings.extend(missing_fee)
    from app.services.finance_reconciliation import reconcile_amazon_fees
    finance = reconcile_amazon_fees(month)
    if completeness['source_error'] or completeness['missing_order_ids'] or completeness['amount_mismatches']:
        finance['status'] = 'incomplete' if completeness['source_error'] else 'differences'
    if finance['status'] in {'differences', 'incomplete'}:
        warnings.append({'code': 'FINANCE_RECONCILIATION_INCOMPLETE' if finance['status'] == 'incomplete' else 'FEE_RECONCILIATION_DIFFERENCE',
            'provider': 'amazon', 'count': len(finance['issues']) + len(finance['details']) or 1,
            'hint': 'Finanzabgleich noch offen; die Steuerberechnung verwendet unverändert die Originalreports und bestätigten Belege.'})
    if any(row.get("net_source") == "computed_home_rate" for row in amazon_rows):
        warnings.append({"code": "AMAZON_VAT_CALCULATION_MISSING", "count": sum(
            1 for row in amazon_rows if row.get("net_source") == "computed_home_rate"),
            "hint": "Amazon hat keine Steuer berechnet; deutsche 19 % wurden selbst angesetzt."})
    returns_synced = kaufland_returns_synced()
    if kaufland_rows and kaufland_returns["count"] == 0 and not returns_synced:
        warnings.append({"code": "KAUFLAND_RETURNS_NOT_SYNCED", "count": 0,
                         "hint": "Die Kaufland-Retouren konnten nicht abgerufen werden — bitte Sync ausfuehren."})

    business_rules = {"block_filing_when_input_vat_incomplete": bool(block_filing_when_input_vat_incomplete)}
    if block_filing_when_input_vat_incomplete and fee_incomplete:
        blockers.extend(fee_incomplete)

    status = "draft" if blockers else "ready"
    return {
        "month": month,
        "revision": None,
        "kind": None,
        "supersedes_id": None,
        "status": status,
        "settings": {
            "tax_mode": settings.tax_mode,
            "vat_effective_from": settings.vat_effective_from.replace(microsecond=0).isoformat().replace("+00:00", "Z")
            if settings.vat_effective_from else None,
            "eu_tax_regime": eu_settings["eu_tax_regime"],
            "eu_distance_prior_year_cents": eu_settings["eu_distance_prior_year_cents"],
            "eu_distance_current_year_cents": eu_settings["eu_distance_current_year_cents"],
        },
        "business_rules": business_rules,
        "sections": {
            "kaufland": {
                "revenue_after_returns_cents": kaufland_gross,
                "net_cents": kaufland_net,
                "output_vat_cents": kaufland_output_vat,
                "rate_overrides_pending": kaufland_overrides_pending,
                "pre_vat_units_cents": sum(int(row["gross_cents"]) for row in kaufland_rows
                                           if row["tax_class"] == CLASS_PRE_VAT),
                "returns": kaufland_returns,
                "returns_synced": returns_synced,
                "rows": [{k: v for k, v in row.items() if k != "warnings"} for row in kaufland_rows],
            },
            "amazon": amazon,
            "amazon_reconciliation": completeness,
            "finance_reconciliation": finance,
            "input_vat": {
                **input_vat,
                "booked_fee_vat_cents": booked_fee_vat_cents,
            "input_vat_incomplete": input_vat_incomplete,
            },
        },
        "totals": {
            "output_vat_cents": output_vat_cents,
            "input_vat_cents": input_vat_cents,
            "vat_payable_cents": output_vat_cents - input_vat_cents,
        },
        "blockers": blockers,
        "warnings": warnings,
    }


def _normalize_tax_mode_for_report(value: Any) -> str:
    return "regular" if _text(value).lower() == "regular" else "small_business"


def _persist_report(month: str, report: dict[str, Any], *, kind: str,
                    supersedes_id: Optional[str]) -> dict[str, Any]:
    with connect_combined_db() as connection:
        row = connection.execute(
            "SELECT COALESCE(MAX(revision), 0) AS top FROM ust_reports WHERE month = ?", (month,)
        ).fetchone()
        revision = int(row["top"] or 0) + 1
        report_id = _stable_id("ust-report", f"{month}:{revision}")
        snapshot_json = _json.dumps(report, ensure_ascii=True, sort_keys=True, default=str)
        connection.execute(
            "INSERT INTO ust_reports(id,month,revision,kind,supersedes_id,status,snapshot_json,"
            "blockers_json,warnings_json,created_at,filed_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                report_id, month, revision, kind, supersedes_id, "filed", snapshot_json,
                _json.dumps(report.get("blockers") or [], ensure_ascii=True, default=str),
                _json.dumps(report.get("warnings") or [], ensure_ascii=True, default=str),
                _utc_now(), _utc_now(),
            ),
        )
        row = connection.execute(
            "SELECT * FROM ust_reports WHERE id = ?", (report_id,)
        ).fetchone()
    return dict(row)


def _latest_filed(month: str) -> Optional[dict[str, Any]]:
    with connect_combined_db() as connection:
        row = connection.execute(
            "SELECT * FROM ust_reports WHERE month = ? ORDER BY revision DESC LIMIT 1", (month,)
        ).fetchone()
    return dict(row) if row else None


def file_report(month: str, *, block_filing_when_input_vat_incomplete: bool = False) -> dict[str, Any]:
    month = _text(month)[:7]
    if _latest_filed(month) is not None:
        raise _docs.UstDocumentError(409, "Bericht ist bereits abgegeben; Korrekturen nur ueber amend_report.")
    report = build_ust_report(
        month, block_filing_when_input_vat_incomplete=block_filing_when_input_vat_incomplete
    )
    if report["blockers"]:
        codes = ", ".join(sorted({item["code"] for item in report["blockers"]}))
        raise _docs.UstDocumentError(409, f"Abgabe gesperrt: {codes}")
    report["status"] = "filed"
    return _persist_report(month, report, kind="original", supersedes_id=None)


def amend_report(month: str, *, block_filing_when_input_vat_incomplete: bool = False) -> dict[str, Any]:
    month = _text(month)[:7]
    previous = _latest_filed(month)
    if previous is None:
        raise _docs.UstDocumentError(409, "Fuer diesen Monat liegt noch keine Abgabe vor.")
    report = build_ust_report(
        month, block_filing_when_input_vat_incomplete=block_filing_when_input_vat_incomplete
    )
    if report["blockers"]:
        codes = ", ".join(sorted({item["code"] for item in report["blockers"]}))
        raise _docs.UstDocumentError(409, f"Berichtigung gesperrt: {codes}")
    report["status"] = "filed"
    return _persist_report(month, report, kind="amendment", supersedes_id=previous["id"])


def get_ust_report(month: str) -> dict[str, Any]:
    month = _text(month)[:7]
    stored = _latest_filed(month)
    if stored is not None:
        report = _json.loads(stored["snapshot_json"])
        report.update(
            id=stored["id"],
            revision=int(stored["revision"]),
            kind=stored["kind"],
            supersedes_id=stored["supersedes_id"],
            status="filed",
        )
        return report
    return build_ust_report(month)


def list_report_months() -> list[dict[str, Any]]:
    with connect_combined_db() as connection:
        rows = connection.execute(
            "SELECT * FROM ust_reports ORDER BY month, revision DESC"
        ).fetchall()
    return [
        {
            "month": row["month"],
            "revision": int(row["revision"]),
            "kind": row["kind"],
            "status": row["status"],
            "supersedes_id": row["supersedes_id"],
            "filed_at": row["filed_at"],
            "created_at": row["created_at"],
        }
        for row in rows
    ]



def sum_booked_input_vat(month: str) -> int:
    """Vorsteuer, die in den Buchungen bereits erfasst ist.

    Der USt-Report ist der Spiegel der Buchungen, keine zweite Wahrheit. Alles,
    was ueber `transactions` mit `is_vat_deductible` und ueber bereits
    freigegebene `monthly_invoices` laeuft, zaehlt hier mit. Altbestand aus
    `input_vat_invoices` bleibt zusaetzlich lesbar, bis er migriert ist.
    """
    from app.services.ust_reconciliation import VAT_BUCKETS, booked_input_vat
    totals = booked_input_vat(month)
    if totals["source_error"]:
        raise _docs.UstDocumentError(409, "Gebuchte Vorsteuer konnte nicht geprueft werden")
    return sum(int(totals[key]) for key in VAT_BUCKETS)
