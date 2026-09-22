"""Product-based purchasing ledger. Integer cents, immutable actions, explicit receipts.

Pool stock is independent of Amazon listings. Only received transfer quantities create
Amazon FIFO lots. All commands run in one SQLite transaction and accept a retry key.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from app.services.importers import amazon_sp_api as db

SCHEMA = """
CREATE TABLE IF NOT EXISTS pool_products(
 id TEXT PRIMARY KEY, name TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS pool_listings(
 marketplace_id TEXT NOT NULL, seller_sku TEXT NOT NULL, asin TEXT NOT NULL DEFAULT '',
 product_id TEXT NOT NULL REFERENCES pool_products(id),
 PRIMARY KEY(marketplace_id,seller_sku));
CREATE TABLE IF NOT EXISTS pool_invoices(
 id TEXT PRIMARY KEY, supplier TEXT NOT NULL, number TEXT NOT NULL, invoice_date TEXT NOT NULL,
 currency TEXT NOT NULL, fx_rate TEXT NOT NULL, fx_reference TEXT NOT NULL,
 document_path TEXT NOT NULL DEFAULT '', notes TEXT NOT NULL DEFAULT '',
 gross_cents INTEGER NOT NULL, net_cents INTEGER NOT NULL, vat_cents INTEGER NOT NULL,
 freight_cents INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL,
 UNIQUE(supplier,number));
CREATE TABLE IF NOT EXISTS pool_lines(
 id TEXT PRIMARY KEY, invoice_id TEXT NOT NULL REFERENCES pool_invoices(id),
 product_id TEXT NOT NULL REFERENCES pool_products(id), quantity INTEGER NOT NULL CHECK(quantity>0),
 gross_cents INTEGER NOT NULL, net_cents INTEGER NOT NULL, vat_cents INTEGER NOT NULL,
 deductible_vat_cents INTEGER, effective_cost_cents INTEGER NOT NULL,
 freight_cents INTEGER NOT NULL, tax_status TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS pool_receipts(
 id TEXT PRIMARY KEY, line_id TEXT NOT NULL REFERENCES pool_lines(id),
 received_at TEXT NOT NULL, quantity INTEGER NOT NULL CHECK(quantity>0),
 available_quantity INTEGER NOT NULL CHECK(available_quantity>=0),
 cost_cents INTEGER NOT NULL, available_cost_cents INTEGER NOT NULL,
 created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS pool_transfers(
 id TEXT PRIMARY KEY, shipment_id TEXT NOT NULL REFERENCES amazon_inbound_shipments(shipment_id),
 status TEXT NOT NULL CHECK(status IN ('reserved','dispatched','cancelled')),
 freight_cents INTEGER NOT NULL DEFAULT 0, source_cost_id TEXT UNIQUE,
 package_reference TEXT NOT NULL DEFAULT '', dispatched_at TEXT, created_at TEXT NOT NULL,
 marketplace_id TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS pool_transfer_lines(
 id TEXT PRIMARY KEY, transfer_id TEXT NOT NULL REFERENCES pool_transfers(id),
 receipt_id TEXT NOT NULL REFERENCES pool_receipts(id), shipment_item_id TEXT NOT NULL REFERENCES amazon_inbound_shipment_items(id),
 quantity INTEGER NOT NULL, received_quantity INTEGER NOT NULL DEFAULT 0,
 product_cost_cents INTEGER NOT NULL, freight_cents INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS pool_movements(
 id TEXT PRIMARY KEY, product_id TEXT NOT NULL REFERENCES pool_products(id),
 action TEXT NOT NULL, quantity INTEGER NOT NULL, cost_cents INTEGER NOT NULL,
 reference_id TEXT NOT NULL, occurred_at TEXT NOT NULL, reason TEXT NOT NULL DEFAULT '');
CREATE TABLE IF NOT EXISTS pool_payments(
 id TEXT PRIMARY KEY, invoice_id TEXT NOT NULL REFERENCES pool_invoices(id),
 paid_at TEXT NOT NULL, amount_cents INTEGER NOT NULL CHECK(amount_cents>0),
 account_reference TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS pool_commands(
 request_id TEXT PRIMARY KEY, operation TEXT NOT NULL, payload TEXT NOT NULL,
 result TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS pool_documents(
 id TEXT PRIMARY KEY, invoice_id TEXT NOT NULL REFERENCES pool_invoices(id),
 filename TEXT NOT NULL, document_path TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS pool_revisions(
 id TEXT PRIMARY KEY, line_id TEXT NOT NULL REFERENCES pool_lines(id),
 before_json TEXT NOT NULL, after_json TEXT NOT NULL, reason TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS pool_legacy_links(
 legacy_invoice_id TEXT PRIMARY KEY, invoice_id TEXT NOT NULL REFERENCES pool_invoices(id));
CREATE TABLE IF NOT EXISTS pool_tax_rows(
 id TEXT PRIMARY KEY, transaction_id TEXT NOT NULL, order_id TEXT NOT NULL,
 seller_sku TEXT NOT NULL, raw_json TEXT NOT NULL, imported_at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS pool_receipts_line ON pool_receipts(line_id,received_at);
CREATE INDEX IF NOT EXISTS pool_transfer_shipment ON pool_transfers(shipment_id,status);
"""


def init_schema(connection):
    connection.executescript(SCHEMA)
    columns = {row[1] for row in connection.execute('PRAGMA table_info(inventory_lots)')}
    for name, definition in [('pool_transfer_line_id', 'TEXT'), ('marketplace_id', "TEXT NOT NULL DEFAULT ''")]:
        if name not in columns:
            connection.execute(f'ALTER TABLE inventory_lots ADD COLUMN {name} {definition}')


def initialize():
    db.init_amazon_fba_db()
    with db._connect() as c:
        init_schema(c)


def uid():
    return str(uuid.uuid4())


def date(value: str) -> str:
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except (ValueError, AttributeError):
        raise ValueError('Ein gültiges Datum ist erforderlich') from None
    return parsed.replace(tzinfo=parsed.tzinfo or timezone.utc).astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')


def split_cents(total: int, weights: list[int]) -> list[int]:
    """Largest remainder; deterministic ties, exact sum, no floating point."""
    if total < 0 or any(w < 0 for w in weights):
        raise ValueError('Negative Kosten oder Gewichte sind nicht zulässig')
    denominator = sum(weights)
    if not denominator:
        if total:
            raise ValueError('Kostenverteilung benötigt Warenwerte oder manuelle Anteile')
        return [0] * len(weights)
    parts = [total * w // denominator for w in weights]
    order = sorted(range(len(weights)), key=lambda i: (-(total * weights[i] % denominator), i))
    for i in order[:total - sum(parts)]:
        parts[i] += 1
    return parts


def _move(c, product_id, action, quantity, cost, reference, occurred_at, reason=''):
    c.execute('INSERT INTO pool_movements VALUES (?,?,?,?,?,?,?,?)',
              (uid(), product_id, action, quantity, cost, reference, occurred_at, reason))


def command(operation: str, payload: dict, action):
    request_id = str(payload.get('request_id', '')).strip()
    if not request_id:
        raise ValueError('request_id ist für wiederholbare Aktionen erforderlich')
    encoded = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    initialize()
    with db._connect() as c:
        c.execute('BEGIN IMMEDIATE')
        previous = c.execute('SELECT * FROM pool_commands WHERE request_id=?', (request_id,)).fetchone()
        if previous:
            if previous['operation'] != operation or previous['payload'] != encoded:
                raise ValueError('request_id wurde bereits für eine andere Aktion verwendet')
            return json.loads(previous['result'])
        result = action(c)
        c.execute('INSERT INTO pool_commands VALUES (?,?,?,?,?)',
                  (request_id, operation, encoded, json.dumps(result), db._utc_now()))
        return result


def create_product(p):
    def action(c):
        name = str(p['name']).strip()
        if not name:
            raise ValueError('Produktname fehlt')
        product_id = uid()
        c.execute('INSERT INTO pool_products VALUES (?,?,?)', (product_id, name, db._utc_now()))
        return {'id': product_id, 'name': name}
    return command('product', p, action)


def map_listing(p):
    def action(c):
        if not c.execute('SELECT 1 FROM pool_products WHERE id=?', (p['product_id'],)).fetchone():
            raise ValueError('Produkt nicht gefunden')
        key = (p['marketplace_id'].strip(), p['seller_sku'].strip())
        if not all(key):
            raise ValueError('Marketplace und Seller-SKU sind erforderlich')
        previous = c.execute('SELECT * FROM pool_listings WHERE marketplace_id=? AND seller_sku=?', key).fetchone()
        if previous and previous['product_id'] != p['product_id']:
            raise ValueError('Bestehende Produktzuordnung darf nicht rückwirkend geändert werden')
        c.execute('INSERT INTO pool_listings VALUES (?,?,?,?) ON CONFLICT(marketplace_id,seller_sku) DO UPDATE SET asin=excluded.asin',
                  (*key, p.get('asin', ''), p['product_id']))
        return {'mapped': True}
    return command('listing', p, action)


def create_invoice(p):
    def action(c):
        supplier, number = p['supplier'].strip(), p['number'].strip()
        if not supplier or not number or not p['lines']:
            raise ValueError('Lieferant, Rechnungsnummer und Positionen sind erforderlich')
        currency = p.get('currency', 'EUR').upper()
        rate = Decimal(str(p.get('fx_rate') or '1'))
        if not rate.is_finite() or rate <= 0 or (currency != 'EUR' and not p.get('fx_reference')):
            raise ValueError('Fremdwährungen benötigen einen dokumentierten EUR-Kurs')
        if currency == 'EUR' and rate != 1:
            raise ValueError('EUR-Kurs muss 1 sein')
        def eur(value):
            return int((Decimal(value) * rate).quantize(Decimal('1'), rounding=ROUND_HALF_UP))
        lines, weights = [], []
        for raw in p['lines']:
            line = dict(raw)
            gross, net, vat = (int(line[k]) for k in ('gross_cents', 'net_cents', 'vat_cents'))
            if min(gross, net, vat) < 0 or gross != net + vat or int(line['quantity']) <= 0:
                raise ValueError('Position: Brutto muss Netto + Steuer entsprechen; Menge muss positiv sein')
            if not c.execute('SELECT 1 FROM pool_products WHERE id=?', (line['product_id'],)).fetchone():
                raise ValueError('Unbekanntes Produkt')
            deductible = line.get('deductible_vat_cents')
            if deductible is not None and not 0 <= int(deductible) <= vat:
                raise ValueError('Abziehbare Vorsteuer liegt außerhalb des Steuerbetrags')
            line['effective'] = eur(gross - int(deductible or 0))
            lines.append(line)
            weights.append(line['effective'])
        freight = int(p.get('freight_cents', 0))
        # Freight is an explicit economic EUR cost, with its own supporting document.
        parts = p.get('freight_allocations')
        if parts is None:
            parts = split_cents(freight, weights)
        if len(parts) != len(lines) or any(v < 0 for v in parts) or sum(parts) != freight:
            raise ValueError('Versandanteile müssen exakt den Versandkosten entsprechen')
        invoice_id = uid()
        c.execute('INSERT INTO pool_invoices VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                  (invoice_id, supplier, number, date(p['invoice_date']), currency, str(rate), p.get('fx_reference', ''),
                   '', p.get('notes', ''), sum(l['gross_cents'] for l in lines), sum(l['net_cents'] for l in lines),
                   sum(l['vat_cents'] for l in lines), freight, db._utc_now()))
        # Explicit column list keeps the schema version independent of tuple ordering.
        ids = []
        for line, shipping in zip(lines, parts):
            line_id = uid(); ids.append(line_id)
            deductible = line.get('deductible_vat_cents')
            c.execute('INSERT INTO pool_lines VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                      (line_id, invoice_id, line['product_id'], line['quantity'], line['gross_cents'], line['net_cents'],
                       line['vat_cents'], deductible, line['effective'], shipping,
                       'review_required' if deductible is None else 'confirmed'))
        return {'id': invoice_id, 'line_ids': ids}
    return command('invoice', p, action)


def receive_purchase(p):
    def action(c):
        line = c.execute('SELECT * FROM pool_lines WHERE id=?', (p['line_id'],)).fetchone()
        if not line:
            raise ValueError('Rechnungsposition nicht gefunden')
        quantity = int(p['quantity'])
        received = c.execute('SELECT COALESCE(SUM(quantity),0) FROM pool_receipts WHERE line_id=?', (line['id'],)).fetchone()[0]
        if quantity <= 0 or received + quantity > line['quantity']:
            raise ValueError('Wareneingang überschreitet offene Rechnungsmenge')
        total = line['effective_cost_cents'] + line['freight_cents']
        # Prefix differences distribute every cent exactly across partial receipts.
        cost = total * (received + quantity) // line['quantity'] - total * received // line['quantity']
        receipt_id, at = uid(), date(p['received_at'])
        c.execute('INSERT INTO pool_receipts VALUES (?,?,?,?,?,?,?,?)',
                  (receipt_id, line['id'], at, quantity, quantity, cost, cost, db._utc_now()))
        _move(c, line['product_id'], 'purchase_receipt', quantity, cost, receipt_id, at)
        return {'id': receipt_id, 'quantity': quantity, 'cost_cents': cost}
    return command('receipt', p, action)


def reserve_transfer(p):
    def action(c):
        shipment = c.execute('SELECT * FROM amazon_inbound_shipments WHERE shipment_id=?', (p['shipment_id'],)).fetchone()
        if not shipment or shipment['status'] in ('CANCELLED', 'DELETED'):
            raise ValueError('Sendung nicht gefunden oder storniert')
        if c.execute('SELECT 1 FROM inventory_lots WHERE inbound_shipment_id=? AND pool_transfer_line_id IS NULL', (p['shipment_id'],)).fetchone():
            raise ValueError('Sendung enthält bereits bestätigte Altkosten; keine doppelte Einbuchung möglich')
        transfer_id = uid()
        c.execute('INSERT INTO pool_transfers(id,shipment_id,status,package_reference,created_at,marketplace_id) VALUES (?,?,\'reserved\',?,?,?)',
                  (transfer_id, p['shipment_id'], p.get('package_reference', ''), db._utc_now(), p['marketplace_id']))
        if not p['lines']:
            raise ValueError('Mindestens eine Sendungsposition ist erforderlich')
        allocations = []
        for requested in p['lines']:
            item = c.execute('SELECT * FROM amazon_inbound_shipment_items WHERE id=? AND shipment_id=?',
                             (requested['shipment_item_id'], p['shipment_id'])).fetchone()
            if not item:
                raise ValueError('Sendungsposition nicht gefunden')
            mapping = c.execute('SELECT * FROM pool_listings WHERE marketplace_id=? AND seller_sku=?',
                                (p['marketplace_id'], item['seller_sku'])).fetchone()
            if not mapping:
                raise ValueError(f"Produktzuordnung fehlt für {item['seller_sku']}")
            quantity = int(requested['quantity'])
            used = c.execute("SELECT COALESCE(SUM(l.quantity),0) FROM pool_transfer_lines l JOIN pool_transfers t ON t.id=l.transfer_id WHERE l.shipment_item_id=? AND t.status!='cancelled'", (item['id'],)).fetchone()[0]
            if quantity <= 0 or used + quantity > item['quantity_shipped']:
                raise ValueError('Zuordnung überschreitet die Amazon-Sendungsmenge')
            receipts = c.execute('''SELECT r.* FROM pool_receipts r JOIN pool_lines l ON l.id=r.line_id
                WHERE l.product_id=? AND r.available_quantity>0 ORDER BY r.received_at,r.created_at,r.id''', (mapping['product_id'],)).fetchall()
            if requested.get('receipt_id'):
                receipts = [r for r in receipts if r['id'] == requested['receipt_id']]
            remaining = quantity
            for r in receipts:
                take = min(remaining, r['available_quantity'])
                if not take:
                    break
                cost = r['available_cost_cents'] * take // r['available_quantity']
                line_id = uid()
                c.execute('INSERT INTO pool_transfer_lines VALUES (?,?,?,?,?,0,?,0)', (line_id, transfer_id, r['id'], item['id'], take, cost))
                c.execute('UPDATE pool_receipts SET available_quantity=available_quantity-?, available_cost_cents=available_cost_cents-? WHERE id=?', (take, cost, r['id']))
                _move(c, mapping['product_id'], 'reserve', take, cost, line_id, db._utc_now())
                allocations.append({'id': line_id, 'quantity': take, 'cost_cents': cost})
                remaining -= take
            if remaining:
                raise ValueError(f"Nicht genügend Eigenbestand für {item['seller_sku']}")
        return {'id': transfer_id, 'allocations': allocations}
    return command('reserve', p, action)


def change_transfer(p):
    def action(c):
        t = c.execute('SELECT * FROM pool_transfers WHERE id=?', (p['transfer_id'],)).fetchone()
        if not t or t['status'] != 'reserved':
            raise ValueError('Nur reservierte Sendungen können versendet oder freigegeben werden')
        lines = c.execute('''SELECT t.*,l.product_id FROM pool_transfer_lines t JOIN pool_receipts r ON r.id=t.receipt_id
            JOIN pool_lines l ON l.id=r.line_id WHERE transfer_id=? ORDER BY t.id''', (t['id'],)).fetchall()
        if p['action'] == 'cancel':
            for l in lines:
                c.execute('UPDATE pool_receipts SET available_quantity=available_quantity+?,available_cost_cents=available_cost_cents+? WHERE id=?', (l['quantity'], l['product_cost_cents'], l['receipt_id']))
                _move(c, l['product_id'], 'release_reservation', l['quantity'], l['product_cost_cents'], l['id'], db._utc_now())
            c.execute("UPDATE pool_transfers SET status='cancelled' WHERE id=?", (t['id'],))
        elif p['action'] == 'dispatch':
            status = c.execute('SELECT status FROM amazon_inbound_shipments WHERE shipment_id=?',(t['shipment_id'],)).fetchone()[0]
            if status in ('CANCELLED','DELETED'):
                raise ValueError('Stornierte Amazon-Sendung kann nicht versendet werden')
            freight = int(p.get('freight_cents', 0))
            source_id = p.get('source_cost_id') or None
            if source_id:
                source = c.execute("SELECT * FROM amazon_inbound_costs WHERE id=? AND shipment_id=? AND status!='superseded'", (source_id, t['shipment_id'])).fetchone()
                if not source or source['currency'] != 'EUR' or abs(source['amount_cents']) != freight:
                    raise ValueError('Amazon-Kostenreferenz oder Betrag stimmt nicht überein')
            parts = p.get('freight_allocations')
            if parts is None:
                parts = split_cents(freight, [l['product_cost_cents'] for l in lines])
            if len(parts) != len(lines) or sum(parts) != freight or any(v < 0 for v in parts):
                raise ValueError('Ungültige Versandaufteilung')
            at = date(p['dispatched_at'])
            for l, part in zip(lines, parts):
                r = c.execute('SELECT received_at FROM pool_receipts WHERE id=?', (l['receipt_id'],)).fetchone()
                if at < r['received_at']:
                    raise ValueError('Versand darf nicht vor Wareneingang liegen')
                c.execute('UPDATE pool_transfer_lines SET freight_cents=? WHERE id=?', (part, l['id']))
                _move(c, l['product_id'], 'dispatch', l['quantity'], l['product_cost_cents'] + part, l['id'], at)
            c.execute("UPDATE pool_transfers SET status='dispatched',freight_cents=?,source_cost_id=?,dispatched_at=? WHERE id=?", (freight, source_id, at, t['id']))
        else:
            raise ValueError('Unbekannte Aktion')
        return {'id': t['id'], 'action': p['action']}
    return command('transfer', p, action)


def reconcile_receipts():
    initialize()
    issues, created = [], 0
    with db._connect() as c:
        c.execute('BEGIN IMMEDIATE')
        items = c.execute('''SELECT i.*,s.inventory_eligible_at,s.updated_at FROM amazon_inbound_shipment_items i
            JOIN amazon_inbound_shipments s ON s.shipment_id=i.shipment_id
            WHERE EXISTS(SELECT 1 FROM pool_transfer_lines l JOIN pool_transfers t ON t.id=l.transfer_id
                         WHERE l.shipment_item_id=i.id AND t.status='dispatched')''').fetchall()
        for item in items:
            lines = c.execute('''SELECT l.*,r.received_at,p.product_id,t.marketplace_id FROM pool_transfer_lines l
                JOIN pool_transfers t ON t.id=l.transfer_id JOIN pool_receipts r ON r.id=l.receipt_id
                JOIN pool_lines p ON p.id=r.line_id WHERE l.shipment_item_id=? AND t.status='dispatched'
                ORDER BY t.dispatched_at,t.created_at,l.id''', (item['id'],)).fetchall()
            booked = sum(l['received_quantity'] for l in lines)
            if item['quantity_received'] < booked:
                issues.append({'shipment_item_id': item['id'], 'reason': 'Amazon-Empfang nachträglich reduziert', 'quantity': booked - item['quantity_received']})
                continue
            pending = item['quantity_received'] - booked
            for l in lines:
                take = min(pending, l['quantity'] - l['received_quantity'])
                if take <= 0:
                    continue
                cost = l['product_cost_cents'] + l['freight_cents']
                before, after = l['received_quantity'], l['received_quantity'] + take
                part = cost * after // l['quantity'] - cost * before // l['quantity']
                lot_id = db._stable_id('pool-fba-lot', f"{l['id']}:{after}")
                at = item['inventory_eligible_at'] or item['updated_at']
                c.execute('''INSERT INTO inventory_lots(id,inbound_shipment_id,inbound_shipment_item_id,seller_sku,
                    available_quantity,unit_cost_cents,cost_remainder_cents,received_at,created_at,pool_transfer_line_id,marketplace_id)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)''',
                    (lot_id, item['shipment_id'], item['id'], item['seller_sku'], take, part // take, part % take,
                     at, db._utc_now(), l['id'], l['marketplace_id']))
                c.execute('UPDATE pool_transfer_lines SET received_quantity=? WHERE id=?', (after, l['id']))
                _move(c, l['product_id'], 'amazon_receipt', take, part, l['id'], at)
                pending -= take; created += take
            if pending:
                issues.append({'shipment_item_id': item['id'], 'reason': 'Empfang ohne Einkaufszuordnung', 'quantity': pending})
    return {'received_quantity': created, 'issues': issues}


def record_payment(p):
    def action(c):
        invoice = c.execute('SELECT * FROM pool_invoices WHERE id=?', (p['invoice_id'],)).fetchone()
        if not invoice or int(p['amount_cents']) <= 0 or not p['account_reference'].strip():
            raise ValueError('Rechnung, positiver Zahlungsbetrag und Zahlungsreferenz sind erforderlich')
        payment_id = uid()
        c.execute('INSERT INTO pool_payments VALUES (?,?,?,?,?,?)',
                  (payment_id, p['invoice_id'], date(p['paid_at']), int(p['amount_cents']), p['account_reference'], db._utc_now()))
        return {'id': payment_id, 'currency': invoice['currency']}
    return command('payment', p, action)


def adjust_stock(p):
    def action(c):
        r = c.execute('SELECT r.*,l.product_id FROM pool_receipts r JOIN pool_lines l ON l.id=r.line_id WHERE r.id=?', (p['receipt_id'],)).fetchone()
        quantity = int(p['quantity'])
        if not r or quantity <= 0 or quantity > r['available_quantity'] or not p['reason'].strip():
            raise ValueError('Korrektur benötigt verfügbaren Bestand, positive Menge und Begründung')
        cost = r['available_cost_cents'] * quantity // r['available_quantity']
        c.execute('UPDATE pool_receipts SET available_quantity=available_quantity-?,available_cost_cents=available_cost_cents-? WHERE id=?', (quantity, cost, r['id']))
        _move(c, r['product_id'], 'writeoff', -quantity, -cost, r['id'], date(p['occurred_at']), p['reason'])
        return {'quantity': quantity, 'cost_cents': cost}
    return command('adjustment', p, action)


def overview():
    initialize()
    with db._connect() as c:
        result = {key: [dict(r) for r in c.execute('SELECT * FROM ' + table)] for key, table in (
            ('products','pool_products'),('listings','pool_listings'),('invoices','pool_invoices'),
            ('lines','pool_lines'),('receipts','pool_receipts'),('transfers','pool_transfers'),
            ('transfer_lines','pool_transfer_lines'),('payments','pool_payments'),('documents','pool_documents'),('revisions','pool_revisions'))}
        result['available_costs'] = [dict(r) for r in c.execute("SELECT * FROM amazon_inbound_costs WHERE status!='superseded' AND currency='EUR'")]
        result['transfer_lines'].sort(key=lambda l: l['id'])
        result['movements'] = [dict(r) for r in c.execute('SELECT * FROM pool_movements ORDER BY occurred_at DESC,id LIMIT 500')]
        result['legacy_invoices'] = [dict(r) for r in c.execute('SELECT * FROM amazon_inbound_invoices WHERE id NOT IN (SELECT legacy_invoice_id FROM pool_legacy_links)')]
        result['shipment_items'] = [dict(r) for r in c.execute('SELECT i.*,s.status FROM amazon_inbound_shipment_items i JOIN amazon_inbound_shipments s USING(shipment_id)')]
        result['available_listings'] = [dict(r) for r in c.execute('''SELECT DISTINCT o.marketplace_id,i.seller_sku,i.asin,i.title FROM amazon_order_items i JOIN amazon_orders o USING(amazon_order_id)
            UNION SELECT DISTINCT marketplace_id,seller_sku,asin,product_name FROM amazon_inventory_snapshots''')]
        for product in result['products']:
            rows = c.execute('''SELECT r.* FROM pool_receipts r JOIN pool_lines l ON l.id=r.line_id WHERE l.product_id=?''', (product['id'],)).fetchall()
            quantity = sum(r['available_quantity'] for r in rows)
            value = sum(r['available_cost_cents'] for r in rows)
            product.update(available_quantity=quantity, available_cost_cents=value,
                           average_home_cost_cents=round(value / quantity) if quantity else None)
            states = c.execute("""SELECT t.status,SUM(tl.quantity) qty,SUM(tl.received_quantity) received
                FROM pool_transfer_lines tl JOIN pool_transfers t ON t.id=tl.transfer_id JOIN pool_receipts r ON r.id=tl.receipt_id
                JOIN pool_lines l ON l.id=r.line_id WHERE l.product_id=? GROUP BY t.status""", (product['id'],)).fetchall()
            product['reserved_quantity'] = sum(r['qty'] for r in states if r['status']=='reserved')
            product['in_transit_quantity'] = sum(r['qty']-r['received'] for r in states if r['status']=='dispatched')
            product['amazon_costed_quantity'] = c.execute("""SELECT COALESCE(SUM(il.available_quantity),0) FROM inventory_lots il
                JOIN pool_transfer_lines tl ON tl.id=il.pool_transfer_line_id JOIN pool_receipts r ON r.id=tl.receipt_id
                JOIN pool_lines l ON l.id=r.line_id WHERE l.product_id=?""", (product['id'],)).fetchone()[0]
            product['costs_complete'] = not c.execute("SELECT 1 FROM pool_lines WHERE product_id=? AND tax_status='review_required'", (product['id'],)).fetchone()
        result['issues'] = [dict(r) for r in c.execute('''SELECT i.id shipment_item_id,i.shipment_id,i.quantity_received,
            COALESCE(SUM(CASE WHEN t.status='dispatched' THEN l.received_quantity ELSE 0 END),0) booked_quantity
            FROM amazon_inbound_shipment_items i LEFT JOIN pool_transfer_lines l ON l.shipment_item_id=i.id
            LEFT JOIN pool_transfers t ON t.id=l.transfer_id GROUP BY i.id HAVING quantity_received!=booked_quantity''')]
    from app.services.amazon_financials import listing_metrics, shipment_metrics
    result["shipment_metrics"] = shipment_metrics()
    result['listing_metrics'] = listing_metrics()
    for product in result['products']:
        keys = {(m['marketplace_id'], m['seller_sku']) for m in result['listings'] if m['product_id'] == product['id']}
        metrics = [m for m in result['listing_metrics'] if (m['marketplace_id'], m['seller_sku']) in keys]
        for field in ('sales_net_cents','fees_net_cents','cogs_cents','margin_cents'):
            product[field] = sum(m[field] for m in metrics)
        product['margin_complete'] = product['costs_complete'] and all(m['margin_complete'] for m in metrics)
    return result


def reconcile_all():
    receipts = reconcile_receipts()
    from app.services.amazon_fba import allocate_order_fifo
    with db._connect() as c:
        orders = c.execute('SELECT amazon_order_id FROM amazon_orders ORDER BY purchase_date,amazon_order_id').fetchall()
    issues = []
    for order in orders:
        try:
            allocate_order_fifo(order['amazon_order_id'])
        except ValueError as exc:
            issues.append({'order_id': order['amazon_order_id'], 'reason': str(exc)})
    return {**receipts, 'cost_issues': issues}


def sync_bookkeeping():
    """Idempotent projection of documents and confirmed payments, never FIFO COGS.

    VAT reporting is intentionally not inferred from payment timing. The original
    invoice retains the tax data for the separate tax implementation.
    """
    from pathlib import Path
    from app.config import BOOKKEEPING_DB_PATH
    initialize()
    if not Path(BOOKKEEPING_DB_PATH).is_file():
        return {'status': 'unavailable', 'reason': 'Buchhaltungsdatenbank fehlt'}
    with db._connect() as c:
        c.execute('ATTACH DATABASE ? AS books', (str(BOOKKEEPING_DB_PATH),))
        names = {r[0] for r in c.execute("SELECT name FROM books.sqlite_master WHERE type='table'")}
        if not {'documents', 'transactions'}.issubset(names):
            return {'status': 'unavailable', 'reason': 'Buchhaltungsschema fehlt'}
        c.execute('BEGIN IMMEDIATE')
        docs = c.execute('SELECT * FROM pool_documents').fetchall()
        for doc in docs:
            c.execute('''INSERT OR IGNORE INTO books.documents(id,original_filename,stored_filename,file_path,mime_type,uploaded_at,notes)
                VALUES (?,?,?,?,?,?,?)''', (doc['id'], doc['filename'], Path(doc['document_path']).name,
                doc['document_path'], 'application/octet-stream', doc['created_at'], 'Amazon-Einkaufspool: '+doc['invoice_id']))
        payments = c.execute('''SELECT p.*,i.supplier,i.number,i.currency FROM pool_payments p JOIN pool_invoices i ON i.id=p.invoice_id''').fetchall()
        for payment in payments:
            tx_id = str(uuid.uuid5(uuid.NAMESPACE_URL, 'amazon-pool-payment:'+payment['id']))
            document = c.execute('SELECT id FROM pool_documents WHERE invoice_id=? ORDER BY created_at,id LIMIT 1', (payment['invoice_id'],)).fetchone()
            c.execute('''INSERT INTO books.transactions(id,date,type,direction,amount_gross,currency,provider,reference,document_id,notes,
                source,source_key,status,booking_class,is_vat_deductible,created_at,updated_at)
                VALUES (?,?,'COGS','OUT',?,?,?,?,?,?,'api',?,'confirmed','single',0,?,?)
                ON CONFLICT(id) DO UPDATE SET document_id=excluded.document_id''',
                (tx_id, payment['paid_at'], payment['amount_cents'], payment['currency'], payment['supplier'], payment['number'],
                 document['id'] if document else None, payment['account_reference']+'; Steuerzuordnung separat prüfen',
                 'amazon-pool-payment:'+payment['id'], payment['created_at'], db._utc_now()))
    return {'status': 'ok', 'documents': len(docs), 'payments': len(payments)}


def revise_line(p):
    """Audited cost correction with preview. No quantity or sale reassignment."""
    def action(c):
        line = c.execute('SELECT l.*,i.fx_rate FROM pool_lines l JOIN pool_invoices i ON i.id=l.invoice_id WHERE l.id=?', (p['line_id'],)).fetchone()
        if not line or not p['reason'].strip():
            raise ValueError('Position und Korrekturbegründung erforderlich')
        if p['expected_cost_cents'] != line['effective_cost_cents']:
            raise ValueError('Kosten wurden zwischenzeitlich geändert; Vorschau neu laden')
        gross, net, vat, deductible = [int(p[k]) for k in ('gross_cents','net_cents','vat_cents','deductible_vat_cents')]
        if min(gross,net,vat,deductible) < 0 or gross != net + vat or deductible > vat:
            raise ValueError('Ungültige Rechnungsbeträge / Vorsteuer')
        effective = int((Decimal(gross-deductible)*Decimal(line['fx_rate'])).quantize(Decimal('1'),rounding=ROUND_HALF_UP))
        old_total = line['effective_cost_cents'] + line['freight_cents']
        new_total = effective + line['freight_cents']
        receipts = c.execute('SELECT * FROM pool_receipts WHERE line_id=? ORDER BY received_at,created_at,id', (line['id'],)).fetchall()
        changes, position = [], 0
        for receipt in receipts:
            new_cost = new_total*(position+receipt['quantity'])//line['quantity'] - new_total*position//line['quantity']
            position += receipt['quantity']
            transfers = c.execute("SELECT l.* FROM pool_transfer_lines l JOIN pool_transfers t ON t.id=l.transfer_id WHERE receipt_id=? AND t.status!='cancelled' ORDER BY l.id", (receipt['id'],)).fetchall()
            lost = receipt['quantity'] - receipt['available_quantity'] - sum(t['quantity'] for t in transfers)
            weights = [receipt['available_quantity']] + [t['quantity'] for t in transfers] + [lost]
            parts = split_cents(new_cost, weights)
            changes.append({'receipt_id': receipt['id'], 'old_cost_cents': receipt['cost_cents'], 'new_cost_cents': new_cost})
            c.execute('UPDATE pool_receipts SET cost_cents=?,available_cost_cents=? WHERE id=?', (new_cost,parts[0],receipt['id']))
            for transfer, part in zip(transfers, parts[1:]):
                c.execute('UPDATE pool_transfer_lines SET product_cost_cents=? WHERE id=?', (part, transfer['id']))
                lots = c.execute('''SELECT l.*,COALESCE((SELECT SUM(quantity) FROM fifo_allocations a WHERE a.inventory_lot_id=l.id),0) sold
                    FROM inventory_lots l WHERE pool_transfer_line_id=? ORDER BY created_at,id''', (transfer['id'],)).fetchall()
                total = part + transfer['freight_cents']
                offset = 0
                for lot in lots:
                    qty = lot['available_quantity'] + lot['sold']
                    lot_cost = total*(offset+qty)//transfer['quantity'] - total*offset//transfer['quantity']; offset += qty
                    allocations = c.execute('SELECT * FROM fifo_allocations WHERE inventory_lot_id=? ORDER BY allocated_at,id', (lot['id'],)).fetchall()
                    unit, remainder = divmod(lot_cost,qty)
                    for allocation in allocations:
                        extra = min(remainder, allocation['quantity']); remainder -= extra
                        c.execute('UPDATE fifo_allocations SET unit_cost_cents=?,allocated_cost_cents=? WHERE id=?', (unit, unit*allocation['quantity']+extra, allocation['id']))
                    c.execute('UPDATE inventory_lots SET unit_cost_cents=?,cost_remainder_cents=? WHERE id=?', (unit,remainder,lot['id']))
        c.execute("UPDATE pool_lines SET gross_cents=?,net_cents=?,vat_cents=?,deductible_vat_cents=?,effective_cost_cents=?,tax_status='confirmed' WHERE id=?", (gross,net,vat,deductible,effective,line['id']))
        c.execute('''UPDATE pool_invoices SET gross_cents=(SELECT SUM(gross_cents) FROM pool_lines WHERE invoice_id=?),
            net_cents=(SELECT SUM(net_cents) FROM pool_lines WHERE invoice_id=?),vat_cents=(SELECT SUM(vat_cents) FROM pool_lines WHERE invoice_id=?) WHERE id=?''', (line['invoice_id'],)*4)
        result = {'line_id':line['id'], 'old_cost_cents':old_total, 'new_cost_cents':new_total, 'receipts':changes}
        if p.get('preview', True):
            # No command entry or changes escape this transaction.
            c.rollback()
            return result
        revision_id = uid()
        c.execute('INSERT INTO pool_revisions VALUES (?,?,?,?,?,?)', (revision_id,line['id'],json.dumps(dict(line)),json.dumps(p),p['reason'],db._utc_now()))
        _move(c,line['product_id'],'cost_correction',0,new_total-old_total,revision_id,db._utc_now(),p['reason'])
        return {**result,'revision_id':revision_id}
    if p.get('preview', True):
        initialize()
        with db._connect() as c:
            c.execute('BEGIN IMMEDIATE')
            return action(c)
    return command('revise_line',p,action)


def migrate_legacy_invoice(p):
    def action(c):
        old = c.execute('SELECT * FROM amazon_inbound_invoices WHERE id=?', (p['legacy_invoice_id'],)).fetchone()
        if not old:
            raise ValueError('Altrechnung nicht gefunden')
        previous = c.execute('SELECT invoice_id FROM pool_legacy_links WHERE legacy_invoice_id=?', (old['id'],)).fetchone()
        if previous:
            return {'id':previous['invoice_id']}
        if old['currency'] != 'EUR':
            raise ValueError('Fremdwährungs-Altrechnung benötigt zunächst eine dokumentierte manuelle Übernahme')
        lines = c.execute('SELECT * FROM amazon_inbound_invoice_lines WHERE invoice_id=? ORDER BY id', (old['id'],)).fetchall()
        if not lines or sum(l['gross_cents'] for l in lines) != old['gross_cents']:
            raise ValueError('Altrechnung ist noch nicht vollständig auf Positionen verteilt')
        invoice_id, transfer_id, at = uid(), uid(), date(p['received_at'])
        c.execute('INSERT INTO pool_invoices VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                  (invoice_id,old['supplier_name'],old['invoice_number'] or old['id'],old['invoice_date'] or at,'EUR','1','',old['document_path'],
                   'Übernahme Altrechnung '+old['id'],old['gross_cents'],old['net_cents'],old['vat_cents'],0,db._utc_now()))
        c.execute("INSERT INTO pool_transfers(id,shipment_id,status,dispatched_at,created_at,marketplace_id) VALUES (?,?,'dispatched',?,?,?)",(transfer_id,old['shipment_id'],at,db._utc_now(),p['marketplace_id']))
        for line in lines:
            mapping = c.execute('SELECT * FROM pool_listings WHERE marketplace_id=? AND seller_sku=?',(p['marketplace_id'],line['seller_sku'])).fetchone()
            item = c.execute('SELECT * FROM amazon_inbound_shipment_items WHERE shipment_id=? AND seller_sku=? AND fnsku=?',(old['shipment_id'],line['seller_sku'],line['fnsku'])).fetchone()
            if not mapping or not item:
                raise ValueError('Produkt- oder Sendungszuordnung fehlt für '+line['seller_sku'])
            line_id, receipt_id, transfer_line_id = uid(),uid(),uid()
            c.execute('INSERT INTO pool_lines VALUES (?,?,?,?,?,?,?,?,?,?,?)',(line_id,invoice_id,mapping['product_id'],line['quantity'],line['gross_cents'],line['net_cents'],line['vat_cents'],None,line['net_cents'],0,'review_required'))
            c.execute('INSERT INTO pool_receipts VALUES (?,?,?,?,?,?,?,?)',(receipt_id,line_id,at,line['quantity'],0,line['net_cents'],0,db._utc_now()))
            lots = c.execute('SELECT * FROM inventory_lots WHERE inbound_shipment_id=? AND seller_sku=?',(old['shipment_id'],line['seller_sku'])).fetchall()
            if any(l['pool_transfer_line_id'] for l in lots):
                raise ValueError('Bestand ist bereits einem Einkauf zugeordnet')
            c.execute('INSERT INTO pool_transfer_lines VALUES (?,?,?,?,?,?,?,0)',(transfer_line_id,transfer_id,receipt_id,item['id'],line['quantity'],line['quantity'] if lots else 0,line['net_cents']))
            for lot in lots:
                c.execute('UPDATE inventory_lots SET pool_transfer_line_id=?,marketplace_id=? WHERE id=?',(transfer_line_id,p['marketplace_id'],lot['id']))
            _move(c,mapping['product_id'],'legacy_import',line['quantity'],line['net_cents'],line_id,at,'Bestehende Kosten unverändert übernommen; Vorsteuer prüfen')
        if old['document_path']:
            c.execute('INSERT INTO pool_documents VALUES (?,?,?,?,?)',(uid(),invoice_id,'Altrechnung '+old['invoice_number'],old['document_path'],db._utc_now()))
        c.execute('INSERT INTO pool_legacy_links VALUES (?,?)',(old['id'],invoice_id))
        return {'id':invoice_id}
    return command('legacy_import',p,action)


def revise_transfer_cost(p):
    """Late carrier invoices update valuation, with preview and an audit command."""
    def action(c):
        transfer = c.execute('SELECT * FROM pool_transfers WHERE id=?',(p['transfer_id'],)).fetchone()
        if not transfer or transfer['status'] != 'dispatched' or not p['reason'].strip():
            raise ValueError('Versendete Zuordnung und Begründung erforderlich')
        if transfer['freight_cents'] != p['expected_freight_cents']:
            raise ValueError('Versandkosten wurden inzwischen geändert; Vorschau neu laden')
        total = int(p['freight_cents'])
        source_id = p.get('source_cost_id') or transfer['source_cost_id']
        if source_id:
            source = c.execute("SELECT * FROM amazon_inbound_costs WHERE id=? AND shipment_id=? AND status!='superseded'",(source_id,transfer['shipment_id'])).fetchone()
            if not source or source['currency']!='EUR' or abs(source['amount_cents'])!=total:
                raise ValueError('Amazon-Kostenreferenz passt nicht zu Sendung/Betrag')
        lines = c.execute('''SELECT tl.*,p.product_id FROM pool_transfer_lines tl JOIN pool_receipts r ON r.id=tl.receipt_id
            JOIN pool_lines p ON p.id=r.line_id WHERE tl.transfer_id=? ORDER BY tl.id''',(transfer['id'],)).fetchall()
        shares = p.get('freight_allocations')
        if shares is None:
            shares = split_cents(total,[l['product_cost_cents'] for l in lines])
        if len(shares)!=len(lines) or any(v<0 for v in shares) or sum(shares)!=total:
            raise ValueError('Versandanteile müssen den Gesamtkosten entsprechen')
        result = {'transfer_id':transfer['id'],'old_cost_cents':transfer['freight_cents'],'new_cost_cents':total,'allocations':[]}
        for line,share in zip(lines,shares):
            c.execute('UPDATE pool_transfer_lines SET freight_cents=? WHERE id=?',(share,line['id']))
            lots = c.execute('''SELECT l.*,COALESCE((SELECT SUM(quantity) FROM fifo_allocations a WHERE a.inventory_lot_id=l.id),0) sold
                FROM inventory_lots l WHERE pool_transfer_line_id=? ORDER BY created_at,id''',(line['id'],)).fetchall()
            offset = 0
            full_cost = line['product_cost_cents']+share
            for lot in lots:
                qty = lot['available_quantity']+lot['sold']
                cost = full_cost*(offset+qty)//line['quantity']-full_cost*offset//line['quantity']; offset+=qty
                unit,remainder=divmod(cost,qty)
                for allocation in c.execute('SELECT * FROM fifo_allocations WHERE inventory_lot_id=? ORDER BY allocated_at,id',(lot['id'],)).fetchall():
                    extra=min(remainder,allocation['quantity']); remainder-=extra
                    c.execute('UPDATE fifo_allocations SET unit_cost_cents=?,allocated_cost_cents=? WHERE id=?',(unit,unit*allocation['quantity']+extra,allocation['id']))
                c.execute('UPDATE inventory_lots SET unit_cost_cents=?,cost_remainder_cents=? WHERE id=?',(unit,remainder,lot['id']))
            _move(c,line['product_id'],'freight_correction',0,share-line['freight_cents'],line['id'],db._utc_now(),p['reason'])
            result['allocations'].append({'line_id':line['id'],'old_cost_cents':line['freight_cents'],'new_cost_cents':share})
        c.execute('UPDATE pool_transfers SET freight_cents=?,source_cost_id=? WHERE id=?',(total,source_id,transfer['id']))
        if p.get('preview',True):
            c.rollback()
        return result
    if p.get('preview',True):
        initialize()
        with db._connect() as c:
            c.execute('BEGIN IMMEDIATE')
            return action(c)
    return command('revise_transfer_cost',p,action)
