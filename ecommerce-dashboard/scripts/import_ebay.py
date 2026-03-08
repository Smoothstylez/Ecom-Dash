"""Import eBay orders from the Numbers spreadsheet into a normalised SQLite DB.

Usage:
    python scripts/import_ebay.py [path/to/Ebay-Spreadsheet-cleanup.numbers]

If no path is given, the script looks in the workspace root (one level above
Dashboard-Combined).

The output is written to data/sources/ebay/ebay_data.sqlite3.
"""

from __future__ import annotations

import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

try:
    import numbers_parser
except ImportError:
    print("ERROR: 'numbers_parser' package not installed. Run: pip install numbers-parser")
    sys.exit(1)


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "sources" / "ebay" / "ebay_data.sqlite3"

# ---------------------------------------------------------------------------
#  Fee / Profit calculation
# ---------------------------------------------------------------------------
# Gebühren = Preis × Provision × 1.19  (commission + 19% MwSt)
# Gewinn   = Preis - Gebühren - Ali Preis
_VAT_FACTOR = 1.19


def _safe_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _safe_str(val: Any) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip()
    return s if s else None


def _parse_date(val: Any) -> Optional[str]:
    """Return ISO date string YYYY-MM-DD from whatever the cell contains."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.strftime("%Y-%m-%d")
    s = str(val).strip()
    # Try ISO-like
    m = re.match(r"(\d{4}-\d{2}-\d{2})", s)
    if m:
        return m.group(1)
    return s if s else None


def _extract_customer_name(raw: Optional[str]) -> Optional[str]:
    """Extract just the name (first line) from the multi-line address block."""
    if not raw:
        return None
    first_line = raw.strip().split("\n")[0].strip()
    return first_line if first_line else None


# ---------------------------------------------------------------------------
#  Database
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS ebay_orders (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    shop            TEXT    NOT NULL,               -- eBay shop name (Brick-LAb, Blockschmiede, maxinba)
    category        TEXT    NOT NULL DEFAULT 'order', -- order | computer | return
    artikel         TEXT,
    kunde_name      TEXT,                           -- extracted first-line name
    kunde_full      TEXT,                           -- full address block (if available)
    order_number    TEXT,
    datum           TEXT,                           -- YYYY-MM-DD
    preis           REAL,                           -- selling price incl. MwSt
    gebuehren       REAL    NOT NULL DEFAULT 0,     -- eBay fees (provision * 1.19)
    nach_gebuehren  REAL,                           -- price after fees
    ali_preis       REAL,                           -- purchase / cost price
    provision_rate  REAL    NOT NULL DEFAULT 0,     -- commission rate as decimal (0.1 = 10%)
    gewinn          REAL,                           -- profit
    is_return       INTEGER NOT NULL DEFAULT 0,     -- 1 if this is a returned order
    imported_at     TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ebay_orders_shop     ON ebay_orders(shop);
CREATE INDEX IF NOT EXISTS idx_ebay_orders_datum     ON ebay_orders(datum);
CREATE INDEX IF NOT EXISTS idx_ebay_orders_category  ON ebay_orders(category);
CREATE INDEX IF NOT EXISTS idx_ebay_orders_order_num ON ebay_orders(order_number);

CREATE TABLE IF NOT EXISTS ebay_import_meta (
    id              INTEGER PRIMARY KEY CHECK (id = 1),
    source_file     TEXT    NOT NULL,
    imported_at     TEXT    NOT NULL,
    total_orders    INTEGER NOT NULL DEFAULT 0,
    total_returns   INTEGER NOT NULL DEFAULT 0,
    shops           TEXT                            -- comma-separated shop names
);
"""


def _init_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(_SCHEMA)
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
#  Parsing
# ---------------------------------------------------------------------------

def _cell_val(table: Any, row: int, col: int) -> Any:
    try:
        return table.cell(row, col).value
    except Exception:
        return None


def _is_empty_row(table: Any, row: int, ncols: int) -> bool:
    """A row is considered empty when all data cells (skip col 0 = row num) are blank."""
    for c in range(1, ncols):
        val = _cell_val(table, row, c)
        if val is not None and str(val).strip():
            return False
    return True


def _parse_brick_lab_table(table: Any, shop: str) -> list[dict]:
    """Sheet 0 (Brick-LAb): 8 cols — #, Artikel, Datum, Order Number, Preis, Ali Preis, Provision, Profit"""
    rows = []
    for r in range(1, table.num_rows):
        if _is_empty_row(table, r, table.num_cols):
            continue
        artikel = _safe_str(_cell_val(table, r, 1))
        if not artikel:
            continue

        preis = _safe_float(_cell_val(table, r, 4))
        ali_preis = _safe_float(_cell_val(table, r, 5))
        provision_rate = _safe_float(_cell_val(table, r, 6)) or 0.0
        profit_raw = _safe_float(_cell_val(table, r, 7))

        # Calculate fees
        gebuehren = (preis or 0) * provision_rate * _VAT_FACTOR if preis else 0
        nach_gebuehren = (preis or 0) - gebuehren
        gewinn = profit_raw if profit_raw is not None else (nach_gebuehren - (ali_preis or 0))

        rows.append({
            "shop": shop,
            "category": "order",
            "artikel": artikel,
            "kunde_name": None,
            "kunde_full": None,
            "order_number": _safe_str(_cell_val(table, r, 3)),
            "datum": _parse_date(_cell_val(table, r, 2)),
            "preis": preis,
            "gebuehren": round(gebuehren, 2),
            "nach_gebuehren": round(nach_gebuehren, 2),
            "ali_preis": ali_preis,
            "provision_rate": provision_rate,
            "gewinn": round(gewinn, 2) if gewinn is not None else None,
            "is_return": 0,
        })
    return rows


def _parse_full_table(table: Any, shop: str, category: str = "order", is_return: bool = False) -> list[dict]:
    """Sheets 1+2: 11 cols — #, Artikel, Kunde, Order Number, Datum, Preis, Gebühren, Nach Gebühren, Ali Preis, Provision, Gewinn/Profit"""
    rows = []
    for r in range(1, table.num_rows):
        if _is_empty_row(table, r, min(table.num_cols, 10)):
            continue
        artikel = _safe_str(_cell_val(table, r, 1))
        if not artikel:
            continue

        kunde_full = _safe_str(_cell_val(table, r, 2))
        preis = _safe_float(_cell_val(table, r, 5))
        gebuehren = _safe_float(_cell_val(table, r, 6)) or 0.0
        nach_gebuehren = _safe_float(_cell_val(table, r, 7))
        ali_preis = _safe_float(_cell_val(table, r, 8))
        provision_rate = _safe_float(_cell_val(table, r, 9)) or 0.0
        gewinn_raw = _safe_float(_cell_val(table, r, 10))

        # Recalculate if needed
        if nach_gebuehren is None and preis is not None:
            nach_gebuehren = preis - gebuehren
        if gewinn_raw is None and nach_gebuehren is not None and ali_preis is not None:
            gewinn_raw = nach_gebuehren - ali_preis

        rows.append({
            "shop": shop,
            "category": category,
            "artikel": artikel,
            "kunde_name": _extract_customer_name(kunde_full),
            "kunde_full": kunde_full,
            "order_number": _safe_str(_cell_val(table, r, 3)),
            "datum": _parse_date(_cell_val(table, r, 4)),
            "preis": preis,
            "gebuehren": round(gebuehren, 2),
            "nach_gebuehren": round(nach_gebuehren, 2) if nach_gebuehren is not None else None,
            "ali_preis": ali_preis,
            "provision_rate": provision_rate,
            "gewinn": round(gewinn_raw, 2) if gewinn_raw is not None else None,
            "is_return": 1 if is_return else 0,
        })
    return rows


def import_numbers_file(numbers_path: Path, db_path: Path = DB_PATH) -> dict:
    """Parse the Numbers file and insert all data into SQLite. Returns summary."""
    if not numbers_path.exists():
        raise FileNotFoundError(f"Numbers file not found: {numbers_path}")

    doc = numbers_parser.Document(str(numbers_path))
    all_rows: list[dict] = []
    shops_seen: set[str] = set()

    for sheet in doc.sheets:
        shop_name = sheet.name.strip()
        shops_seen.add(shop_name)

        for table in sheet.tables:
            table_name = table.name.strip()

            # Detect table type by header / position
            if shop_name == "Brick-LAb":
                # Simple format (no customer, no fees column)
                parsed = _parse_brick_lab_table(table, shop_name)
            elif "cksendung" in table_name.lower() or "rücksendung" in table_name.lower() or "ücksendung" in table_name.lower():
                parsed = _parse_full_table(table, shop_name, category="return", is_return=True)
            elif table_name.lower() == "computer":
                parsed = _parse_full_table(table, shop_name, category="computer")
            else:
                parsed = _parse_full_table(table, shop_name, category="order")

            all_rows.extend(parsed)

    # Write to DB
    conn = _init_db(db_path)
    now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    # Clear existing data (idempotent re-import)
    conn.execute("DELETE FROM ebay_orders")
    conn.execute("DELETE FROM ebay_import_meta")

    total_orders = 0
    total_returns = 0

    for row in all_rows:
        conn.execute(
            """INSERT INTO ebay_orders (
                shop, category, artikel, kunde_name, kunde_full, order_number,
                datum, preis, gebuehren, nach_gebuehren, ali_preis, provision_rate,
                gewinn, is_return, imported_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                row["shop"], row["category"], row["artikel"], row["kunde_name"],
                row["kunde_full"], row["order_number"], row["datum"], row["preis"],
                row["gebuehren"], row["nach_gebuehren"], row["ali_preis"],
                row["provision_rate"], row["gewinn"], row["is_return"], now_iso,
            ),
        )
        if row["is_return"]:
            total_returns += 1
        else:
            total_orders += 1

    conn.execute(
        """INSERT INTO ebay_import_meta (id, source_file, imported_at, total_orders, total_returns, shops)
           VALUES (1, ?, ?, ?, ?, ?)""",
        (str(numbers_path.name), now_iso, total_orders, total_returns, ",".join(sorted(shops_seen))),
    )

    conn.commit()
    conn.close()

    return {
        "db_path": str(db_path),
        "source_file": str(numbers_path),
        "total_orders": total_orders,
        "total_returns": total_returns,
        "shops": sorted(shops_seen),
        "imported_at": now_iso,
    }


# ---------------------------------------------------------------------------
#  CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if len(sys.argv) > 1:
        source = Path(sys.argv[1]).resolve()
    else:
        # Default: workspace root
        source = PROJECT_ROOT.parent / "Ebay-Spreadsheet-cleanup.numbers"

    print(f"Importing from: {source}")
    result = import_numbers_file(source)
    print(f"Done! {result['total_orders']} orders, {result['total_returns']} returns across {result['shops']}")
    print(f"Database: {result['db_path']}")
