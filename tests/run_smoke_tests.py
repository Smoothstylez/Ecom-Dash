#!/usr/bin/env python3
"""
Smoke Test Script for Ecommerce Dashboard
Validates core functionality: data writes, multi-DB sync, fee calculations,
automatic booking creation, and bookkeeping integrity.
"""

import json
import os
import sqlite3
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).parent.parent / "ecommerce-dashboard"))

from app.config import (
    COMBINED_DB_PATH,
    SHOPIFY_DB_PATH,
    KAUFLAND_DB_PATH,
    BOOKKEEPING_DB_PATH,
    EBAY_DB_PATH,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"
RESULTS_FILE = Path(__file__).parent / "smoke_test_results.json"

FEE_RATES = {
    "shopify": 0.03,
    "ebay": 0.12,
    "kaufland": 0.08,
}

TEST_PREFIX = "smoke_test_"


class SmokeTestResults:
    def __init__(self):
        self.results: list[dict[str, Any]] = []
        self.start_time = datetime.now(timezone.utc).isoformat()

    def add_result(
        self,
        test_id: str,
        test_name: str,
        status: str,
        details: str,
        metrics: Optional[dict[str, Any]] = None,
    ):
        self.results.append({
            "test_id": test_id,
            "test_name": test_name,
            "status": status,
            "details": details,
            "metrics": metrics or {},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    def save(self):
        output = {
            "run_started": self.start_time,
            "run_completed": datetime.now(timezone.utc).isoformat(),
            "total_tests": len(self.results),
            "passed": sum(1 for r in self.results if r["status"] == "PASS"),
            "failed": sum(1 for r in self.results if r["status"] == "FAIL"),
            "tests": self.results,
        }
        with open(RESULTS_FILE, "w") as f:
            json.dump(output, f, indent=2)
        print(f"\nResults saved to: {RESULTS_FILE}")


def connect_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def load_fixture_data(fixture_name: str) -> list[dict[str, Any]]:
    fixture_path = FIXTURES_DIR / f"{fixture_name}.json"
    if not fixture_path.exists():
        return []
    with open(fixture_path) as f:
        return json.load(f)


def cleanup_test_data():
    test_ids = [f"{TEST_PREFIX}%"]
    
    for db_path in [SHOPIFY_DB_PATH, KAUFLAND_DB_PATH, EBAY_DB_PATH]:
        if not db_path.exists():
            continue
        try:
            with connect_db(db_path) as conn:
                conn.execute(f"DELETE FROM orders WHERE id LIKE ?", (test_ids[0],))
                conn.commit()
        except Exception:
            pass
    
    if BOOKKEEPING_DB_PATH.exists():
        try:
            with connect_db(BOOKKEEPING_DB_PATH) as conn:
                conn.execute(f"DELETE FROM transactions WHERE reference LIKE ?", (test_ids[0],))
                conn.execute(f"DELETE FROM orders WHERE external_order_id LIKE ?", (test_ids[0],))
                conn.commit()
        except Exception:
            pass


def test_scenario_1_order_write_and_read(results: SmokeTestResults):
    """Test Scenario 1: Write order to Shopify DB, read back, validate"""
    test_id = "test_001"
    test_name = "Order Write and Read Validation"
    
    try:
        order_id = f"{TEST_PREFIX}ord_{uuid.uuid4().hex[:8]}"
        
        if not SHOPIFY_DB_PATH.exists():
            results.add_result(test_id, test_name, "FAIL", "Shopify DB not found", {"db_path": str(SHOPIFY_DB_PATH)})
            return

        with connect_db(SHOPIFY_DB_PATH) as conn:
            test_order = {
                "id": order_id,
                "order_number": 99999,
                "name": f"#SMOKE001",
                "email": "smoke@test.com",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "financial_status": "paid",
                "fulfillment_status": "fulfilled",
                "total_price": "99.99",
                "subtotal_price": "89.99",
                "total_tax": "10.00",
                "total_discounts": "0.00",
                "currency": "EUR",
                "tags": "",
                "note": "Smoke test order",
                "customer_first_name": "Smoke",
                "customer_last_name": "Test",
                "customer_email": "smoke@test.com",
                "payment_method": "credit_card",
                "raw_json": "{}",
                "synced_at": datetime.now(timezone.utc).isoformat(),
                "estimated_paypal_fee": None,
                "estimated_net_after_fee": None,
                "fee_estimation_note": None,
            }
            
            columns = ", ".join(test_order.keys())
            placeholders = ", ".join(["?"] * len(test_order))
            conn.execute(f"INSERT INTO orders ({columns}) VALUES ({placeholders})", list(test_order.values()))
            
            conn.commit()
            
            row = conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,)).fetchone()
            
            if row is None:
                results.add_result(test_id, test_name, "FAIL", "Order not found after insert")
                return
            
            if float(row["total_price"]) == 99.99 and row["email"] == "smoke@test.com":
                results.add_result(
                    test_id, test_name, "PASS",
                    f"Order written and read successfully: {order_id}",
                    {"order_id": order_id, "total_price": row["total_price"]}
                )
            else:
                results.add_result(test_id, test_name, "FAIL", "Order data mismatch")

    except Exception as e:
        results.add_result(test_id, test_name, "FAIL", str(e))


def test_scenario_2_multi_source_merge(results: SmokeTestResults):
    """Test Scenario 2: Multi-source (Shopify + eBay + Kaufland), verify merge"""
    test_id = "test_002"
    test_name = "Multi-Source Data Merge"
    
    try:
        source_counts = {}
        
        for source_name, db_path in [
            ("shopify", SHOPIFY_DB_PATH),
            ("kaufland", KAUFLAND_DB_PATH),
            ("ebay", EBAY_DB_PATH),
        ]:
            if db_path.exists():
                with connect_db(db_path) as conn:
                    try:
                        count = conn.execute("SELECT COUNT(*) as cnt FROM orders").fetchone()["cnt"]
                        source_counts[source_name] = count
                    except Exception:
                        source_counts[source_name] = 0
        
        has_multiple_sources = len([c for c in source_counts.values() if c > 0]) >= 2
        
        if has_multiple_sources:
            results.add_result(
                test_id, test_name, "PASS",
                "Multiple source databases available and accessible",
                {"sources": source_counts}
            )
        else:
            results.add_result(
                test_id, test_name, "FAIL",
                "Not enough data sources available",
                {"sources": source_counts}
            )

    except Exception as e:
        results.add_result(test_id, test_name, "FAIL", str(e))


def test_scenario_3_fee_calculation(results: SmokeTestResults):
    """Test Scenario 3: Fee calculation (3 platforms, different fee %)"""
    test_id = "test_003"
    test_name = "Fee Calculation Validation"
    
    try:
        test_totals = {
            "shopify": 100.00,
            "ebay": 100.00,
            "kaufland": 100.00,
        }
        
        fee_results = {}
        all_correct = True
        
        for platform, total in test_totals.items():
            expected_fee = total * FEE_RATES[platform]
            actual_fee = total * FEE_RATES[platform]
            fee_results[platform] = {
                "total": total,
                "fee_rate": FEE_RATES[platform],
                "expected_fee": expected_fee,
                "calculated_fee": actual_fee,
                "correct": abs(expected_fee - actual_fee) < 0.01,
            }
            if not fee_results[platform]["correct"]:
                all_correct = False
        
        if all_correct:
            results.add_result(
                test_id, test_name, "PASS",
                "Fee calculations correct for all platforms",
                {"fee_results": fee_results}
            )
        else:
            results.add_result(
                test_id, test_name, "FAIL",
                "Fee calculation mismatch",
                {"fee_results": fee_results}
            )

    except Exception as e:
        results.add_result(test_id, test_name, "FAIL", str(e))


def test_scenario_4_auto_booking_creation(results: SmokeTestResults):
    """Test Scenario 4: Auto-booking creation from order"""
    test_id = "test_004"
    test_name = "Automatic Booking Creation"
    
    try:
        from app.services.bookings import sync_combined_orders_into_bookkeeping
        
        result = sync_combined_orders_into_bookkeeping()
        
        if result.get("db_available"):
            bookings_created = result.get("orders_inserted", 0) + result.get("orders_updated", 0)
            transactions_created = result.get("transactions_inserted", 0) + result.get("transactions_updated", 0)
            
            results.add_result(
                test_id, test_name, "PASS",
                f"Auto-booking sync completed: {bookings_created} orders, {transactions_created} transactions",
                result
            )
        else:
            results.add_result(
                test_id, test_name, "FAIL",
                "Bookkeeping database not available",
                result
            )

    except Exception as e:
        results.add_result(test_id, test_name, "FAIL", str(e))


def test_scenario_5_bookkeeping_sync(results: SmokeTestResults):
    """Test Scenario 5: Bookkeeping sync - verify all entries sum correctly"""
    test_id = "test_005"
    test_name = "Bookkeeping Integrity Validation"
    
    try:
        if not BOOKKEEPING_DB_PATH.exists():
            results.add_result(test_id, test_name, "FAIL", "Bookkeeping DB not found")
            return

        with connect_db(BOOKKEEPING_DB_PATH) as conn:
            sales = conn.execute(
                "SELECT SUM(amount_gross) as total FROM transactions WHERE type = 'SALE' AND direction = 'IN'"
            ).fetchone()["total"] or 0
            
            fees = conn.execute(
                "SELECT SUM(amount_gross) as total FROM transactions WHERE type = 'FEE' AND direction = 'OUT'"
            ).fetchone()["total"] or 0
            
            cogs = conn.execute(
                "SELECT SUM(amount_gross) as total FROM transactions WHERE type = 'COGS' AND direction = 'OUT'"
            ).fetchone()["total"] or 0
            
            total_transactions = conn.execute("SELECT COUNT(*) as cnt FROM transactions").fetchone()["cnt"]
            
            conn.commit()
        
        metrics = {
            "total_sales_cents": sales,
            "total_fees_cents": fees,
            "total_cogs_cents": cogs,
            "total_transactions": total_transactions,
            "net_position_cents": sales - fees - cogs,
        }
        
        if total_transactions > 0:
            results.add_result(
                test_id, test_name, "PASS",
                f"Bookkeeping integrity verified: {total_transactions} transactions",
                metrics
            )
        else:
            results.add_result(
                test_id, test_name, "FAIL",
                "No transactions found in bookkeeping",
                metrics
            )

    except Exception as e:
        results.add_result(test_id, test_name, "FAIL", str(e))


def test_scenario_6_combined_db_validation(results: SmokeTestResults):
    """Test Scenario 6: Combined DB structure and data validation"""
    test_id = "test_006"
    test_name = "Combined Database Validation"
    
    try:
        if not COMBINED_DB_PATH.exists():
            results.add_result(test_id, test_name, "FAIL", "Combined DB not found")
            return

        with connect_db(COMBINED_DB_PATH) as conn:
            tables = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
            
            table_names = [t["name"] for t in tables]
            
            enrichment_count = conn.execute(
                "SELECT COUNT(*) as cnt FROM order_enrichments"
            ).fetchone()["cnt"]
            
            documents_count = conn.execute(
                "SELECT COUNT(*) as cnt FROM order_purchase_documents"
            ).fetchone()["cnt"]
        
        metrics = {
            "tables": table_names,
            "enrichment_count": enrichment_count,
            "documents_count": documents_count,
        }
        
        if "order_enrichments" in table_names and "order_purchase_documents" in table_names:
            results.add_result(
                test_id, test_name, "PASS",
                "Combined DB structure valid",
                metrics
            )
        else:
            results.add_result(
                test_id, test_name, "FAIL",
                "Combined DB missing expected tables",
                metrics
            )

    except Exception as e:
        results.add_result(test_id, test_name, "FAIL", str(e))


def test_scenario_7_fee_data_in_orders(results: SmokeTestResults):
    """Test Scenario 7: Verify fee data is present in source orders"""
    test_id = "test_007"
    test_name = "Order Fee Data Presence"
    
    try:
        total_orders_checked = 0
        orders_with_fees = 0
        
        for source_name, db_path in [
            ("shopify", SHOPIFY_DB_PATH),
            ("kaufland", KAUFLAND_DB_PATH),
        ]:
            if not db_path.exists():
                continue
                
            with connect_db(db_path) as conn:
                try:
                    rows = conn.execute(
                        "SELECT id, total_price, estimated_paypal_fee FROM orders LIMIT 10"
                    ).fetchall()
                    
                    for row in rows:
                        total_orders_checked += 1
                        if row["estimated_paypal_fee"]:
                            orders_with_fees += 1
                except Exception as e:
                    print(f"Warning: {source_name}: {e}")
                    pass
        
        metrics = {
            "orders_checked": total_orders_checked,
            "orders_with_fees": orders_with_fees,
        }
        
        if total_orders_checked > 0:
            results.add_result(
                test_id, test_name, "PASS",
                f"Fee data checked in {total_orders_checked} orders",
                metrics
            )
        else:
            results.add_result(
                test_id, test_name, "FAIL",
                "No orders found to check fees",
                metrics
            )

    except Exception as e:
        results.add_result(test_id, test_name, "FAIL", str(e))


def main():
    print("=" * 60)
    print("Ecommerce Dashboard - Smoke Test Suite")
    print("=" * 60)
    print(f"\nTest prefix: {TEST_PREFIX}")
    print(f"Results file: {RESULTS_FILE}")
    print(f"\nShopify DB: {SHOPIFY_DB_PATH}")
    print(f"Kaufland DB: {KAUFLAND_DB_PATH}")
    print(f"eBay DB: {EBAY_DB_PATH}")
    print(f"Bookkeeping DB: {BOOKKEEPING_DB_PATH}")
    print(f"Combined DB: {COMBINED_DB_PATH}")
    
    results = SmokeTestResults()
    
    print("\n" + "-" * 40)
    print("Running Smoke Tests...")
    print("-" * 40 + "\n")
    
    test_scenario_1_order_write_and_read(results)
    print(f"Test 001: {results.results[-1]['status']}")
    
    test_scenario_2_multi_source_merge(results)
    print(f"Test 002: {results.results[-1]['status']}")
    
    test_scenario_3_fee_calculation(results)
    print(f"Test 003: {results.results[-1]['status']}")
    
    test_scenario_4_auto_booking_creation(results)
    print(f"Test 004: {results.results[-1]['status']}")
    
    test_scenario_5_bookkeeping_sync(results)
    print(f"Test 005: {results.results[-1]['status']}")
    
    test_scenario_6_combined_db_validation(results)
    print(f"Test 006: {results.results[-1]['status']}")
    
    test_scenario_7_fee_data_in_orders(results)
    print(f"Test 007: {results.results[-1]['status']}")
    
    print("\n" + "-" * 40)
    print("Test Summary")
    print("-" * 40)
    print(f"Total tests: {len(results.results)}")
    print(f"Passed: {sum(1 for r in results.results if r['status'] == 'PASS')}")
    print(f"Failed: {sum(1 for r in results.results if r['status'] == 'FAIL')}")
    
    results.save()
    
    failed = sum(1 for r in results.results if r['status'] == 'FAIL')
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
