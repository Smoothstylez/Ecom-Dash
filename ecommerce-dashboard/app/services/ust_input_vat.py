"""Platform-fee eligibility across the seller's VAT transition.

Invoice availability determines the deduction month; eligibility is separate.
Order-related fee credits inherit the original order's treatment. Shared
monthly service fees use their documented service end, not the invoice date.
Raw invoice amounts and confirmations are never rewritten by this projection.
"""
from __future__ import annotations

import json
import sqlite3
from functools import lru_cache
from pathlib import Path

from app import config
from app.db import connect_combined_db
from app.services import invoice_parser
from app.services.ust_reconciliation import _read, _timestamp


@lru_cache(maxsize=128)
def _pdf_proof(path: str, mtime: int, size: int):
    return invoice_parser.parse_invoice_text(invoice_parser.extract_text(path)).to_dict()


class FeeTaxContext:
    def __init__(self, cutoff):
        self.cutoff = cutoff
        self._dates = {}

    def _order_date(self, provider, reference):
        if provider not in self._dates:
            dates = {}
            path = config.KAUFLAND_DB_PATH if provider == "kaufland" else config.AMAZON_FBA_DB_PATH
            if path.exists():
                try:
                    with _read(path) as c:
                        if provider == "kaufland":
                            dates = {str(r["id_order_unit"]): r["ts_created_iso"] for r in c.execute("SELECT id_order_unit,ts_created_iso FROM order_units")}
                        else:
                            dates = {r["amazon_order_id"]: r["purchase_date"] for r in c.execute("SELECT amazon_order_id,purchase_date FROM amazon_orders WHERE COALESCE(is_synthetic,0)=0")}
                except sqlite3.Error:
                    pass
            self._dates[provider] = dates
        key = str(reference or "").rsplit("/", 1)[-1] if provider == "kaufland" else reference
        return _timestamp(self._dates[provider].get(key, ""))

    def _date_eligibility(self, token):
        date = _timestamp(token)
        if date is None:
            return None
        if "T" in str(token) or ":" in str(token):
            return date >= self.cutoff
        if date.date() == self.cutoff.date() and self.cutoff.hour + self.cutoff.minute + self.cutoff.second:
            return None
        return date.date() >= self.cutoff.date()

    def _original_eligibility(self, provider, number, credit_date):
        """Only fully eligible/ineligible confirmed originals permit inheritance.

        Partial allocations require credit-level evidence. Credits cannot serve
        as originals, which also prevents circular credit-reference chains.
        """
        candidates = []
        with connect_combined_db() as c:
            tables = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if 'input_vat_invoices' in tables:
                candidates.extend(dict(r) for r in c.execute('SELECT * FROM input_vat_invoices WHERE provider=? AND invoice_number=?', (provider, number)))
        if config.BOOKKEEPING_DB_PATH.exists():
            with _read(config.BOOKKEEPING_DB_PATH) as c:
                tables = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                if 'monthly_invoices' in tables:
                    for raw in c.execute('SELECT * FROM monthly_invoices WHERE provider=? AND invoice_number=?', (provider, number)):
                        original = dict(raw)
                        vat = int(original.get('vat_amount_cents') or 0)
                        candidates.append({**original, 'vat_cents': vat, 'gross_cents': original.get('invoice_amount_cents'),
                            'deductible_vat_cents': original.get('vat_cents_eur') if original.get('currency') != 'EUR' else vat,
                            'lines': json.loads(original.get('lines_json') or '[]'),
                            'input_vat_status': 'confirmed' if original['status'] == 'approved' else 'review_required'})
        treatments = []
        for original in candidates:
            native = int(original.get('vat_cents') or 0)
            declared = int(original.get('deductible_vat_cents') or 0)
            date = _timestamp(original.get('invoice_date'))
            if (original.get('input_vat_status') != 'confirmed' or native <= 0
                or int(original.get('gross_cents') or 0) <= 0 or original.get('doc_kind') == 'sales'
                or original.get('doc_type', 'fee') != 'fee' or date is None
                or credit_date is None or date.date() > credit_date.date()):
                return None
            reference = original.get('vat_cents_eur') if original.get('currency') != 'EUR' else native
            if reference is None or declared not in {0, int(reference)}:
                return None
            assessment = self.assess(original)
            if assessment['eligibility_review_count']:
                return None
            effective = assessment['effective_deductible_vat_cents']
            if effective == 0:
                treatments.append(False)
            elif effective == declared:
                treatments.append(True)
            else:
                return None
        return treatments[0] if treatments and len(set(treatments)) == 1 else None

    def assess(self, row):
        declared = int(row.get("deductible_vat_cents", row.get("vat_cents", 0)) or 0)
        result = {"effective_deductible_vat_cents": declared,
                  "eligibility_adjustment_cents": 0, "eligibility_review_count": 0}
        if self.cutoff is None or row.get("doc_type", "fee") != "fee" or row.get("provider") not in {"amazon", "kaufland"}:
            return result
        native_vat = int(row.get("vat_cents") or 0)
        reference_vat = row["vat_cents_eur"] if row.get("vat_cents_eur") is not None else native_vat
        if declared != int(reference_vat or 0):
            return result  # explicit allocation takes precedence, with or without lines
        service_end = row.get("service_date") or row.get("period_to") or row.get("period_from")
        end = _timestamp(service_end or "")
        lines = row.get("lines") or []
        references = row.get('original_invoice_numbers') or json.loads(row.get('original_invoice_numbers_json') or '[]') or ([row['original_invoice_number']] if row.get('original_invoice_number') else [])
        if declared < 0 and references and not any(line.get('order_ref') for line in lines):
            treatments = [self._original_eligibility(row['provider'], number, _timestamp(row.get('invoice_date'))) for number in set(references)]
            if all(value is True for value in treatments):
                return result
            result.update(effective_deductible_vat_cents=0, eligibility_adjustment_cents=-declared,
                          eligibility_review_count=0 if all(value is False for value in treatments) else 1)
            return result
        if not lines and end is not None and end.date() < self.cutoff.date():
            result.update(effective_deductible_vat_cents=0, eligibility_adjustment_cents=-declared)
            return result
        path = Path(row.get("document_path") or "")
        if not lines and path.is_file():
            try:
                stat = path.stat()
                proof = _pdf_proof(str(path), stat.st_mtime_ns, stat.st_size)
                matches = proof["invoice_number"] == row.get("invoice_number") and proof["provider"] == row["provider"]
                matches = matches and all(int(proof[key]) == int(row[key]) for key in
                                          ("gross_cents", "net_cents", "vat_cents") if key in row)
                if matches and proof["parse_confidence"] == 1.0 and not proof["needs_review_reasons"]:
                    lines = proof["lines"]
            except (invoice_parser.InvoiceParseError, OSError, ValueError):
                pass
        if not lines:
            # Existing explicitly confirmed manual allocations remain usable.
            # A date-only service on the cutoff day cannot establish eligibility.
            if declared < 0:
                result.update(effective_deductible_vat_cents=0, eligibility_adjustment_cents=-declared,
                              eligibility_review_count=1)
            elif end is not None and end.date() == self.cutoff.date() and self._date_eligibility(service_end) is None and declared:
                result["eligibility_review_count"] = 1
            return result
        eligible_vat = 0
        excluded = 0
        unclear = 0
        for line in lines:
            vat = int(line.get("vat_cents") or 0)
            if not vat:
                continue
            kind = line.get("position_key") or line.get("category")
            reference = line.get("order_ref")
            if str(reference or "").strip().lower() in {"", "-", "null", "none", "n/a"}:
                reference = None
            eligibility = None
            if reference and kind in {"provision", "provision_storno", "fulfillment", "refund_admin", "fee_refund", "shipping_chargeback"}:
                original = self._order_date(row["provider"], reference)
                if original is not None:
                    eligibility = original >= self.cutoff
            elif kind in {"base_fee", "subscription"}:
                eligibility = self._date_eligibility(service_end)
            elif vat > 0:
                eligibility = self._date_eligibility(line.get("line_date") or service_end)
            if eligibility is True:
                eligible_vat += vat
            elif eligibility is False:
                excluded += 1
            else:
                unclear += 1
        if excluded or unclear:
            if row.get("currency", "EUR") != "EUR":
                if row.get("fx_rate"):
                    from decimal import Decimal, ROUND_HALF_UP
                    eligible_vat = int((Decimal(eligible_vat) * Decimal(str(row["fx_rate"]))).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
                else:
                    eligible_vat = 0
                    unclear += 1
            result.update(effective_deductible_vat_cents=eligible_vat,
                          eligibility_adjustment_cents=eligible_vat-declared,
                          eligibility_review_count=unclear)
        return result
