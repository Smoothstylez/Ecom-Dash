#!/usr/bin/env python3
import sqlite3
import json
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "ecommerce-dashboard" / "data"
SHOPIFY_DB = DB_PATH / "sources" / "shopify" / "shopify_data.sqlite3"
KAUFLAND_DB = DB_PATH / "sources" / "kaufland" / "kaufland_data.sqlite3"
BOOKKEEPING_DB = DB_PATH / "sources" / "bookkeeping" / "dashboard.sqlite3"

def connect_db(db_path):
    if not db_path.exists():
        return None
    return sqlite3.connect(db_path)

def validate_shopify_orders():
    errors = []
    passed = 0
    
    conn = connect_db(SHOPIFY_DB)
    if not conn:
        return 0, [{"entry_id": None, "type": "order_shopify", "error": "DB not found"}]
    
    cursor = conn.cursor()
    
    cursor.execute("SELECT id, customer_id, total_price, subtotal_price, total_tax, total_discounts FROM orders")
    orders = cursor.fetchall()
    
    cursor.execute("SELECT order_id, SUM(quantity) as total_qty FROM order_line_items GROUP BY order_id")
    items_qty = {row[0]: row[1] for row in cursor.fetchall()}
    
    for order_id, customer_id, total_price, subtotal_price, total_tax, total_discounts in orders:
        if not order_id:
            errors.append({"entry_id": None, "type": "order_shopify", "error": "id is null/empty"})
            continue
        
        cursor.execute("SELECT COUNT(*) FROM orders WHERE id = ?", (order_id,))
        if cursor.fetchone()[0] > 1:
            errors.append({"entry_id": order_id, "type": "order_shopify", "error": "id not unique"})
            continue
        
        if not customer_id:
            errors.append({"entry_id": order_id, "type": "order_shopify", "error": "customer_id missing"})
            continue
        
        try:
            tp = float(total_price or '0')
            if tp < 0:
                errors.append({"entry_id": order_id, "type": "order_shopify", "error": "total_price < 0"})
                continue
        except (ValueError, TypeError):
            errors.append({"entry_id": order_id, "type": "order_shopify", "error": "total_price invalid"})
            continue
        
        if order_id not in items_qty or items_qty[order_id] <= 0:
            errors.append({"entry_id": order_id, "type": "order_shopify", "error": "items qty <= 0 or missing"})
            continue
        
        try:
            sp = float(subtotal_price or '0')
            tt = float(total_tax or '0')
            td = float(total_discounts or '0')
            expected_total = round(sp + tt - td, 2)
            if abs(tp - expected_total) > 0.01:
                errors.append({"entry_id": order_id, "type": "order_shopify", "error": f"total_price mismatch: expected {expected_total}, got {tp}"})
                continue
        except (ValueError, TypeError) as e:
            errors.append({"entry_id": order_id, "type": "order_shopify", "error": f"price calculation error: {e}"})
            continue
        
        passed += 1
    
    conn.close()
    return passed, errors

def validate_kaufland_orders():
    errors = []
    passed = 0
    
    conn = connect_db(KAUFLAND_DB)
    if not conn:
        return 0, [{"entry_id": None, "type": "order_kaufland", "error": "DB not found"}]
    
    cursor = conn.cursor()
    
    cursor.execute("SELECT id_order, order_units_count FROM orders")
    orders = cursor.fetchall()
    
    cursor.execute("SELECT id_order, SUM(CAST(price AS REAL)) as total_price FROM order_units GROUP BY id_order")
    order_totals = {row[0]: row[1] for row in cursor.fetchall()}
    
    cursor.execute("SELECT id_order, COUNT(*) as unit_count FROM order_units GROUP BY id_order")
    units_qty = {row[0]: row[1] for row in cursor.fetchall()}
    
    for order_id, units_count in orders:
        if not order_id:
            errors.append({"entry_id": None, "type": "order_kaufland", "error": "id is null/empty"})
            continue
        
        cursor.execute("SELECT COUNT(*) FROM orders WHERE id_order = ?", (order_id,))
        if cursor.fetchone()[0] > 1:
            errors.append({"entry_id": order_id, "type": "order_kaufland", "error": "id not unique"})
            continue
        
        qty = units_qty.get(order_id, 0)
        if qty <= 0:
            errors.append({"entry_id": order_id, "type": "order_kaufland", "error": "items qty <= 0"})
            continue
        
        total = order_totals.get(order_id, 0) or 0
        if total < 0:
            errors.append({"entry_id": order_id, "type": "order_kaufland", "error": "total_price < 0"})
            continue
        
        passed += 1
    
    conn.close()
    return passed, errors

def validate_invoices():
    errors = []
    passed = 0
    
    conn = connect_db(BOOKKEEPING_DB)
    if not conn:
        return 0, [{"entry_id": None, "type": "invoice", "error": "DB not found"}]
    
    cursor = conn.cursor()
    cursor.execute("SELECT id, provider, invoice_amount_cents FROM monthly_invoices")
    invoices = cursor.fetchall()
    
    for inv_id, provider, invoice_amount in invoices:
        if not inv_id:
            errors.append({"entry_id": None, "type": "invoice", "error": "id is null/empty"})
            continue
        
        cursor.execute("SELECT COUNT(*) FROM monthly_invoices WHERE id = ?", (inv_id,))
        if cursor.fetchone()[0] > 1:
            errors.append({"entry_id": inv_id, "type": "invoice", "error": "id not unique"})
            continue
        
        if not provider:
            errors.append({"entry_id": inv_id, "type": "invoice", "error": "provider missing"})
            continue
        
        if invoice_amount is None or invoice_amount <= 0:
            errors.append({"entry_id": inv_id, "type": "invoice", "error": "invoice_amount_cents <= 0 or null"})
            continue
        
        passed += 1
    
    conn.close()
    return passed, errors

def validate_bookkeeping_transactions():
    errors = []
    passed = 0
    
    conn = connect_db(BOOKKEEPING_DB)
    if not conn:
        return 0, [{"entry_id": None, "type": "bookkeeping", "error": "DB not found"}]
    
    cursor = conn.cursor()
    cursor.execute("SELECT id, type, direction, amount_gross, date FROM transactions")
    transactions = cursor.fetchall()
    
    valid_types = {'SALE', 'REFUND', 'RETURN', 'COGS', 'EXPENSE', 'FEE', 'SUBSCRIPTION', 'ADJUSTMENT'}
    valid_directions = {'IN', 'OUT'}
    
    for trans_id, trans_type, direction, amount, date in transactions:
        if not trans_id:
            errors.append({"entry_id": None, "type": "bookkeeping", "error": "id is null/empty"})
            continue
        
        cursor.execute("SELECT COUNT(*) FROM transactions WHERE id = ?", (trans_id,))
        if cursor.fetchone()[0] > 1:
            errors.append({"entry_id": trans_id, "type": "bookkeeping", "error": "id not unique"})
            continue
        
        if trans_type not in valid_types:
            errors.append({"entry_id": trans_id, "type": "bookkeeping", "error": f"invalid type: {trans_type}"})
            continue
        
        if direction not in valid_directions:
            errors.append({"entry_id": trans_id, "type": "bookkeeping", "error": f"invalid direction: {direction}"})
            continue
        
        if amount is None:
            errors.append({"entry_id": trans_id, "type": "bookkeeping", "error": "amount_gross is null"})
            continue
        
        if not date:
            errors.append({"entry_id": trans_id, "type": "bookkeeping", "error": "date is null/empty"})
            continue
        
        if trans_type == "SALE" and direction != "IN":
            errors.append({"entry_id": trans_id, "type": "bookkeeping", "error": "SALE must have direction IN"})
            continue
        
        if trans_type in ("REFUND", "RETURN") and direction != "OUT":
            errors.append({"entry_id": trans_id, "type": "bookkeeping", "error": "REFUND/RETURN must have direction OUT"})
            continue
        
        passed += 1
    
    conn.close()
    return passed, errors

def main():
    results = {
        "orders": {
            "shopify": {"passed": 0, "failed": 0, "errors": []},
            "kaufland": {"passed": 0, "failed": 0, "errors": []}
        },
        "invoices": {"passed": 0, "failed": 0, "errors": []},
        "bookkeeping": {"passed": 0, "failed": 0, "errors": []},
        "summary": {
            "total_passed": 0,
            "total_failed": 0
        }
    }
    
    shopify_passed, shopify_errors = validate_shopify_orders()
    results["orders"]["shopify"]["passed"] = shopify_passed
    results["orders"]["shopify"]["failed"] = len(shopify_errors)
    results["orders"]["shopify"]["errors"] = shopify_errors
    
    kaufland_passed, kaufland_errors = validate_kaufland_orders()
    results["orders"]["kaufland"]["passed"] = kaufland_passed
    results["orders"]["kaufland"]["failed"] = len(kaufland_errors)
    results["orders"]["kaufland"]["errors"] = kaufland_errors
    
    invoice_passed, invoice_errors = validate_invoices()
    results["invoices"]["passed"] = invoice_passed
    results["invoices"]["failed"] = len(invoice_errors)
    results["invoices"]["errors"] = invoice_errors
    
    bk_passed, bk_errors = validate_bookkeeping_transactions()
    results["bookkeeping"]["passed"] = bk_passed
    results["bookkeeping"]["failed"] = len(bk_errors)
    results["bookkeeping"]["errors"] = bk_errors
    
    results["summary"]["total_passed"] = (
        shopify_passed + kaufland_passed + invoice_passed + bk_passed
    )
    results["summary"]["total_failed"] = (
        len(shopify_errors) + len(kaufland_errors) + len(invoice_errors) + len(bk_errors)
    )
    
    output_file = Path(__file__).parent / "detailed_entry_validation_results.json"
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2)
    
    print(f"Validation complete:")
    print(f"  Shopify orders: {shopify_passed} passed, {len(shopify_errors)} failed")
    print(f"  Kaufland orders: {kaufland_passed} passed, {len(kaufland_errors)} failed")
    print(f"  Invoices: {invoice_passed} passed, {len(invoice_errors)} failed")
    print(f"  Bookkeeping: {bk_passed} passed, {len(bk_errors)} failed")
    print(f"  Total: {results['summary']['total_passed']} passed, {results['summary']['total_failed']} failed")
    print(f"Results written to {output_file}")

if __name__ == "__main__":
    main()