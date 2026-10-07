"""Read-only source reconciliation and bookkeeping VAT for the USt report."""
from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from app import config
from app.db import connect_combined_db
from app.services import ust_documents

VAT_BUCKETS = ("purchases_cents", "amazon_fees_cents", "kaufland_fees_cents", "other_cents")


def _customer_gross(event) -> int:
    amount = int(event["sales_cents"])
    raw = json.loads(event["raw_json"] or "{}")
    if event["event_type"].startswith("ModernTransaction:"):
        def promotions(nodes):
            total = 0
            for node in nodes or []:
                if str(node.get("breakdownType") or "").lower() == "promorebates":
                    money = node.get("breakdownAmount") or {}
                    if money.get("currencyCode", "EUR") == "EUR":
                        total += int((Decimal(str(money.get("currencyAmount") or 0)) * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
                else:
                    total += promotions(node.get("breakdowns"))
            return total
        # Sales excludes the promotional expense, while the customer's taxable
        # gross is after that rebate. Fee expenses are never subtracted here.
        amount += promotions([n for n in raw.get("breakdowns", []) if n.get("breakdownType", "").lower() in {"expenses", "refunded expenses"}])
    elif event["event_type"] in {"ShipmentEventList", "RefundEventList"}:
        for item in raw.get("ShipmentItemList", raw.get("RefundItemList", [])):
            for promo in item.get("PromotionList", item.get("PromotionAdjustmentList", [])):
                money = promo.get("PromotionAmount") or {}
                if money.get("CurrencyCode", "EUR") == "EUR":
                    amount += int((Decimal(str(money.get("CurrencyAmount") or 0)) * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    return amount


@contextmanager
def _read(path):
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        yield connection
    finally:
        connection.close()


def _timestamp(value: str) -> datetime | None:
    try:
        value = str(value or "").strip().replace("-Sept-", "-Sep-")
        for fmt in ("%d-%b-%Y %H:%M:%S UTC", "%d-%b-%Y UTC"):
            try:
                return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                pass
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)
    except ValueError:
        return None


def apply_amazon_cutoff(rows: list[dict[str, Any]], cutoff: datetime | None) -> list[dict[str, Any]]:
    if cutoff is None:
        return rows
    with connect_combined_db() as c:
        shipments = [dict(r) for r in c.execute("SELECT * FROM amazon_tax_rows WHERE transaction_type NOT IN ('RETURN','REFUND')")]
    by_key = defaultdict(list)
    by_sku = defaultdict(list)
    for sale in shipments:
        by_key[(sale["order_id"], sale["shipment_id"], sale["seller_sku"])].append(sale)
        by_sku[(sale["order_id"], sale["seller_sku"])].append(sale)
    purchase_dates = {}
    if config.AMAZON_FBA_DB_PATH.exists():
        try:
            with _read(config.AMAZON_FBA_DB_PATH) as c:
                purchase_dates = {r["amazon_order_id"]: r["purchase_date"] for r in c.execute("SELECT amazon_order_id,purchase_date FROM amazon_orders WHERE COALESCE(is_synthetic,0)=0")}
        except sqlite3.Error:
            pass  # amazon_completeness adds a hard source-inspection blocker
    for row in rows:
        original = row
        correction = row["transaction_type"] in {"RETURN", "REFUND"}
        if correction:
            candidates = by_key[(row["order_id"], row["shipment_id"], row["seller_sku"])] or by_sku[(row["order_id"], row["seller_sku"])]
            if len(candidates) != 1:
                row.update(tax_class="unresolved_return_link", output_vat_cents=0, net_cents=row["gross_cents"])
                continue
            original = candidates[0]
        raw = json.loads(original["raw_json"])
        if raw.get("_source_match_conflict"):
            row.update(tax_class="unresolved", output_vat_cents=0, net_cents=row["gross_cents"])
            continue
        token = purchase_dates.get(original["order_id"]) or raw.get("Order Date") or ""
        date = _timestamp(token)
        if date is None:
            row["cutoff_ambiguous"] = True
            continue
        has_time = "T" in token or ":" in token
        if date.date() == cutoff.date() and not has_time and cutoff.time() != datetime.min.time():
            row["cutoff_ambiguous"] = True
            continue
        if date < cutoff:
            row.update(tax_class="pre_vat", net_cents=row["gross_cents"], output_vat_cents=0,
                       original_tax_class="pre_vat" if correction else row.get("original_tax_class"))
    return rows


def amazon_completeness(month: str) -> dict[str, Any]:
    """Purchase month is only a fallback; a known later shipment is not a gap.

    Finance amounts are checked across all imported months, because payout and
    shipment months need not coincide. They are evidence of missing rows, never
    a replacement for tax classification.
    """
    result = {"missing_order_ids": [], "amount_mismatches": [], "source_error": None}
    if not config.AMAZON_FBA_DB_PATH.exists():
        return result
    try:
        with connect_combined_db() as c:
            tax = [dict(r) for r in c.execute("SELECT order_id,transaction_type,gross_cents FROM amazon_tax_rows")]
        tax_sales = defaultdict(int)
        tax_refunds = defaultdict(int)
        for row in tax:
            target = tax_refunds if row["transaction_type"] in {"RETURN", "REFUND"} else tax_sales
            target[row["order_id"]] += abs(row["gross_cents"])
        with _read(config.AMAZON_FBA_DB_PATH) as c:
            orders = c.execute("SELECT amazon_order_id,purchase_date,order_status FROM amazon_orders WHERE substr(purchase_date,1,7)=?", (month,)).fetchall()
            result["missing_order_ids"] = sorted({r["amazon_order_id"] for r in orders
                if r["order_status"].lower() in {"shipped", "partiallyshipped"} and r["amazon_order_id"] not in tax_sales})
            tables = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if "amazon_financial_events" in tables:
                from app.services.amazon_fba import _canonical_financial_event_predicate
                def kind(event):
                    raw = json.loads(event["raw_json"] or "{}")
                    transaction = str(raw.get("transaction-type") or raw.get("transactionType") or "").lower()
                    if "refund" in event["event_type"].lower() or transaction == "refund":
                        return True
                    if event["event_type"] in {"ModernTransaction:Shipment", "ShipmentEventList", "SettlementReportLine"} or transaction in {"order", "shipment"}:
                        return False
                    return None

                query = "SELECT e.amazon_order_id,e.posted_date,e.sales_cents,e.currency,e.event_type,e.raw_json FROM amazon_financial_events e"
                operational_dates = defaultdict(list)
                for event in c.execute(query):
                    raw = json.loads(event["raw_json"] or "{}")
                    if event["event_type"] == "ModernTransaction:Shipment" and raw.get("transactionStatus") in {"DEFERRED", "DEFERRED_RELEASED"}:
                        operational_dates[event["amazon_order_id"]].append(str(event["posted_date"] or ""))
                # A still-deferred October shipment proves that a September
                # purchase has not produced September output VAT. A released
                # payout alone cannot supply that proof.
                result["missing_order_ids"] = [oid for oid in result["missing_order_ids"]
                    if not operational_dates[oid] or min(operational_dates[oid])[:7] <= month]
                events = c.execute(query + " WHERE " + _canonical_financial_event_predicate()).fetchall()
                sums = defaultdict(int)
                # Preserve operational-period evidence from earlier lifecycle
                # states, although only canonical states contribute amounts.
                relevant = {(e["amazon_order_id"], kind(e)) for e in c.execute(query + " WHERE substr(e.posted_date,1,7)=?", (month,))
                            if e["amazon_order_id"] and e["currency"] == "EUR" and e["sales_cents"] and kind(e) is not None}
                for event in events:
                    if not event["amazon_order_id"] or event["currency"] != "EUR" or not event["sales_cents"]:
                        continue
                    correction = kind(event)
                    if correction is None:
                        continue
                    key = (event["amazon_order_id"], correction)
                    gross = _customer_gross(event)
                    sums[key] += -gross if correction else gross
                for order_id, correction in sorted(relevant):
                    imported = (tax_refunds if correction else tax_sales).get(order_id, 0)
                    expected = sums[(order_id, correction)]
                    if abs(imported - expected) > 1:
                        result["amount_mismatches"].append({"order_id": order_id, "kind": "refund" if correction else "shipment", "source_gross_cents": expected, "tax_gross_cents": imported})
    except (sqlite3.Error, ValueError, TypeError) as exc:
        result["source_error"] = str(exc)
    return result


def _provider(value: Any) -> str:
    return "amazon" if value in {"amazon", "amazon_fba"} else str(value or "other")


def fee_invoice_coverage() -> dict[str, dict[str, int]]:
    """Proven EUR invoice gross per service month, not just invoice presence."""
    coverage = {"amazon": defaultdict(int), "kaufland": defaultdict(int)}
    included = set()
    for row in ust_documents.list_input_vat_invoices():
        if row["doc_type"] == "fee" and row["input_vat_status"] == "confirmed" and row["provider"] in coverage:
            if row["currency"] != "EUR":
                continue
            month = str(row["service_date"] or row["period_to"] or row["period_from"] or row["invoice_date"])[:7]
            coverage[row["provider"]][month] += int(row["gross_cents"])
            included.add((row["provider"], row["invoice_number"]))
    if config.BOOKKEEPING_DB_PATH.exists():
        with _read(config.BOOKKEEPING_DB_PATH) as c:
            tables = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if "monthly_invoices" in tables:
                for invoice in c.execute("SELECT * FROM monthly_invoices WHERE status='approved'"):
                    row = dict(invoice)
                    provider = _provider(row.get("provider"))
                    if provider not in coverage or (provider, row.get("invoice_number")) in included:
                        continue
                    gross = int(row.get("invoice_amount_cents") or 0)
                    if row.get("currency", "EUR") != "EUR":
                        if not row.get("fx_rate"):
                            continue
                        rate = Decimal(str(row["fx_rate"]))
                        vat = int(row.get("vat_amount_cents") or 0)
                        converted_vat = int((Decimal(vat) * rate).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
                        if row.get("vat_cents_eur") is None or abs(converted_vat - int(row["vat_cents_eur"])) > 2:
                            continue
                        gross = int((Decimal(gross) * rate).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
                    month = str(row.get("service_date") or row.get("period_to") or row.get("period_from") or row.get("invoice_date") or "")[:7]
                    coverage[provider][month] += gross
    return coverage


def amazon_fee_cents(month: str) -> int:
    if not config.AMAZON_FBA_DB_PATH.exists():
        return 0
    from app.services.amazon_fba import _canonical_financial_event_predicate
    with _read(config.AMAZON_FBA_DB_PATH) as c:
        tables = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if "amazon_financial_events" not in tables:
            return 0
        rows = c.execute("SELECT e.* FROM amazon_financial_events e WHERE substr(e.posted_date,1,7)=? AND e.currency='EUR' AND " + _canonical_financial_event_predicate(), (month,)).fetchall()
        total = sum(int(r["fees_cents"]) + (_customer_gross(r) - int(r["sales_cents"]) if r["event_type"].startswith("ModernTransaction:") else 0) for r in rows)
    # The importer stores fee costs positively and fee refunds negatively.
    return max(total, 0)


def has_full_kaufland_statement(month: str) -> bool:
    from calendar import monthrange
    year, number = (int(part) for part in month.split("-"))
    first, last = f"{month}-01", f"{month}-{monthrange(year, number)[1]:02d}"
    candidates = [r for r in ust_documents.list_input_vat_invoices(provider="kaufland")
                  if r["doc_type"] == "fee" and r["input_vat_status"] == "confirmed"]
    if config.BOOKKEEPING_DB_PATH.exists():
        with _read(config.BOOKKEEPING_DB_PATH) as c:
            tables = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if "monthly_invoices" in tables:
                candidates.extend(dict(r) for r in c.execute("SELECT * FROM monthly_invoices WHERE provider='kaufland' AND status='approved'"))
    return any(str(r.get("invoice_number") or "").startswith("R") and
               str(r.get("period_from") or "")[:10] == first and str(r.get("period_to") or "")[:10] == last for r in candidates)


def booked_input_vat(month: str, *, fee_tax_context=None) -> dict[str, Any]:
    result = {key: 0 for key in VAT_BUCKETS}
    result.update(pending_review_count=0, source_error=None, conflicts=[], fee_service_months={"amazon": None, "kaufland": None})
    result.update(eligibility_adjustment_cents=0, eligibility_review_count=0)
    if not config.BOOKKEEPING_DB_PATH.exists():
        return result
    legacy = ust_documents.list_input_vat_invoices()
    legacy_by_id = {(_provider(r["provider"]), r["invoice_number"]): r for r in legacy}
    legacy_ids = set(legacy_by_id)
    try:
        with _read(config.BOOKKEEPING_DB_PATH) as c:
            tables = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            invoices = [dict(r) for r in c.execute("SELECT * FROM monthly_invoices")] if "monthly_invoices" in tables else []
            transactions = [dict(r) for r in c.execute("SELECT * FROM transactions WHERE is_vat_deductible=1 AND vat_amount != 0")] if "transactions" in tables else []
            linked = {r[0] for r in c.execute("SELECT transaction_id FROM monthly_invoice_transactions")} if "monthly_invoice_transactions" in tables else set()
        invoice_documents = {r.get("document_id") for r in invoices if r.get("document_id")}
        invoice_refs = {(_provider(r.get("provider")), r.get("invoice_number")) for r in invoices if r.get("invoice_number")}
        for invoice in invoices:
            provider = _provider(invoice.get("provider"))
            deduction = ust_documents.resolve_deduction_month(
                invoice_date=invoice.get("invoice_date"), received_date=invoice.get("received_date"),
                service_date=invoice.get("service_date"), period_from=invoice.get("period_from"), period_to=invoice.get("period_to"))
            counterpart = legacy_by_id.get((provider, invoice.get("invoice_number")))
            if counterpart is not None:
                if month in {deduction, counterpart["deduction_month"]} and invoice["status"] == "approved":
                    amount = invoice.get("vat_cents_eur") if invoice.get("currency", "EUR") != "EUR" else invoice.get("vat_amount_cents")
                    if (counterpart["input_vat_status"] != "confirmed" or counterpart["deduction_month"] != deduction
                        or int(counterpart["deductible_vat_cents"]) != int(amount or 0)):
                        result["conflicts"].append({"provider": provider, "invoice_number": invoice.get("invoice_number")})
                continue
            if deduction != month:
                continue
            if invoice["status"] != "approved":
                if invoice.get("vat_amount_cents"):
                    result["pending_review_count"] += 1
                continue
            from app.services.platform_invoices import _is_deductible
            if not _is_deductible(invoice) or invoice.get("doc_kind") == "sales":
                continue
            key = f"{provider}_fees_cents" if provider in {"amazon", "kaufland"} else "other_cents"
            amount = invoice.get("vat_amount_cents")
            if invoice.get("currency", "EUR") != "EUR" and amount:
                amount = invoice.get("vat_cents_eur")
                if amount is None:
                    result["source_error"] = "Fremdwaehrungsrechnung ohne belegte EUR-Vorsteuer"
                    continue
            if fee_tax_context is not None:
                native_vat = int(invoice.get("vat_amount_cents") or 0)
                assessment = fee_tax_context.assess({**invoice, "provider": provider, "doc_type": "fee",
                    "vat_cents": native_vat, "deductible_vat_cents": int(amount or 0),
                    "lines": json.loads(invoice.get("lines_json") or "[]")})
                amount = assessment["effective_deductible_vat_cents"]
                result["eligibility_adjustment_cents"] += assessment["eligibility_adjustment_cents"]
                result["eligibility_review_count"] += assessment["eligibility_review_count"]
            result[key] += int(amount or 0)
            if provider in {"amazon", "kaufland"}:
                result["fee_service_months"][provider] = str(invoice.get("service_date") or invoice.get("period_to") or invoice.get("period_from") or "")[:7] or None
        for tx in transactions:
            kind = str(tx.get("type") or "").upper()
            if tx.get("direction") == "IN" and kind not in {"FEE", "EXPENSE", "SUBSCRIPTION", "COGS"}:
                continue
            provider = _provider(tx.get("provider"))
            identity = (provider, tx.get("reference"))
            if (str(tx.get("source_key") or "").startswith("platform-invoice:") or tx.get("id") in linked
                or tx.get("document_id") in invoice_documents or identity in invoice_refs or identity in legacy_ids):
                continue
            if str(tx.get("date") or "")[:7] != month:
                continue
            if tx.get("currency", "EUR") != "EUR":
                result["source_error"] = "Fremdwaehrungsbuchung ohne belegte EUR-Vorsteuer"
                continue
            key = "purchases_cents" if kind in {"PURCHASE", "COGS"} else (
                f"{provider}_fees_cents" if provider in {"amazon", "kaufland"} and kind in {"FEE", "EXPENSE", "ADVERTISING"} else "other_cents")
            result[key] += int(tx["vat_amount"]) * (-1 if tx.get("direction") == "IN" else 1)
    except (sqlite3.Error, ValueError, TypeError) as exc:
        result["source_error"] = str(exc)
    return result
