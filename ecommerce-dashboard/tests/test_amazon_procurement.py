import uuid
import pytest
from app.services import amazon_procurement as pool
from app.services.importers import amazon_sp_api as db


def key(**values):
    return {'request_id': str(uuid.uuid4()), **values}


@pytest.fixture
def setup(monkeypatch, tmp_path):
    monkeypatch.setattr(db, 'AMAZON_FBA_DB_PATH', tmp_path / 'amazon.sqlite3')
    pool.initialize()
    product = pool.create_product(key(name='MINI Ultra'))['id']
    for sku in ('SKU-A', 'SKU-B', 'SKU-C'):
        pool.map_listing(key(product_id=product, marketplace_id='DE', seller_sku=sku, asin=sku))
    with db._connect() as c:
        for shipment, sku, qty in [('S1','SKU-A',7),('S2','SKU-B',20)]:
            c.execute('INSERT INTO amazon_inbound_shipments(id,shipment_id,status,updated_at) VALUES (?,?,?,?)', (shipment,shipment,'SHIPPED','2026-09-03'))
            c.execute('INSERT INTO amazon_inbound_shipment_items(id,shipment_id,seller_sku,quantity_shipped) VALUES (?,?,?,?)', (shipment+'-item',shipment,sku,qty))
    return product


def purchase(product, qty, unit, number='1', deductible=0):
    p = key(supplier='Supplier',number=number,invoice_date='2026-09-01',lines=[dict(product_id=product,quantity=qty,gross_cents=qty*unit,net_cents=qty*unit,vat_cents=0,deductible_vat_cents=deductible)])
    invoice = pool.create_invoice(p)
    receipt = pool.receive_purchase(key(line_id=invoice['line_ids'][0],quantity=qty,received_at='2026-09-01'))
    return invoice, receipt


def test_split_purchases_across_listings_and_shipments(setup):
    purchase(setup,9,1000)
    first = pool.reserve_transfer(key(shipment_id='S1',marketplace_id='DE',lines=[dict(shipment_item_id='S1-item',quantity=7)]))
    assert pool.overview()['products'][0]['available_quantity'] == 2
    pool.change_transfer(key(transfer_id=first['id'],action='dispatch',dispatched_at='2026-09-02',freight_cents=101))
    purchase(setup,18,1200,'2')
    product = pool.overview()['products'][0]
    assert product['available_quantity'] == 20
    assert product['average_home_cost_cents'] == 1180
    second = pool.reserve_transfer(key(shipment_id='S2',marketplace_id='DE',lines=[dict(shipment_item_id='S2-item',quantity=20)]))
    assert sorted(a['quantity'] for a in second['allocations']) == [2,18]
    assert pool.overview()['products'][0]['available_quantity'] == 0
    with db._connect() as c:
        c.execute("UPDATE amazon_inbound_shipment_items SET quantity_received=4 WHERE shipment_id='S1'")
    assert pool.reconcile_receipts()['received_quantity'] == 4
    assert pool.reconcile_receipts()['received_quantity'] == 0
    with db._connect() as c:
        c.execute("UPDATE amazon_inbound_shipment_items SET quantity_received=7 WHERE shipment_id='S1'")
    assert pool.reconcile_receipts()['received_quantity'] == 3
    with db._connect() as c:
        assert c.execute('SELECT SUM(available_quantity*unit_cost_cents+cost_remainder_cents) FROM inventory_lots').fetchone()[0] == 7101


def test_retry_rollback_and_release(setup):
    invoice, receipt = purchase(setup,9,1000)
    payload = key(shipment_id='S1',marketplace_id='DE',lines=[dict(shipment_item_id='S1-item',quantity=7)])
    transfer = pool.reserve_transfer(payload)
    assert pool.reserve_transfer(payload) == transfer
    with pytest.raises(ValueError):
        pool.reserve_transfer(key(shipment_id='S2',marketplace_id='DE',lines=[dict(shipment_item_id='S2-item',quantity=20)]))
    assert pool.overview()['products'][0]['available_quantity'] == 2
    pool.change_transfer(key(transfer_id=transfer['id'],action='cancel'))
    assert pool.overview()['products'][0]['available_quantity'] == 9
    with pytest.raises(ValueError):
        pool.receive_purchase(key(line_id=invoice['line_ids'][0],quantity=1,received_at='2026-09-01'))


def test_rounding_and_unknown_vat(setup):
    assert pool.split_cents(100,[1,1,1]) == [34,33,33]
    purchase(setup,1,1190,deductible=None)
    assert not pool.overview()['products'][0]['costs_complete']


def test_refund_signs_and_historical_projection(setup):
    from app.services.amazon_financials import events
    transaction = {'transactionId':'refund','transactionType':'Refund','transactionStatus':'RELEASED',
                   'totalAmount':{'currencyAmount':-158.58,'currencyCode':'EUR'},
                   'breakdowns':[
                       {'breakdownType':'Refunded Sales','breakdownAmount':{'currencyAmount':-169.90},'breakdowns':[{'breakdownType':'Tax','breakdownAmount':{'currencyAmount':-27.13}}]},
                       {'breakdownType':'Refunded Expenses','breakdownAmount':{'currencyAmount':11.32},'breakdowns':[{'breakdownType':'AmazonFees','breakdownAmount':{'currencyAmount':11.32}}]}]}
    db.sync_modern_financial_transactions([transaction])
    with db._connect() as c:
        row = events(c)[0]
    assert row['sales_cents'] == -16990
    assert row['fees_cents'] == -1132
    assert row['sales_vat_cents'] == -2713
    assert row['unclassified_cents'] == 0
    assert row['fee_tax_complete'] is False


def test_correction_preview_and_audit(setup):
    invoice, receipt = purchase(setup,9,1000)
    transfer = pool.reserve_transfer(key(shipment_id='S1',marketplace_id='DE',lines=[dict(shipment_item_id='S1-item',quantity=7)]))
    pool.change_transfer(key(transfer_id=transfer['id'],action='dispatch',dispatched_at='2026-09-02'))
    with db._connect() as c:
        c.execute("UPDATE amazon_inbound_shipment_items SET quantity_received=7 WHERE shipment_id='S1'")
    pool.reconcile_receipts()
    p = key(line_id=invoice['line_ids'][0],expected_cost_cents=9000,gross_cents=10710,net_cents=9000,vat_cents=1710,deductible_vat_cents=0,reason='Vorsteuer nicht abziehbar',preview=True)
    assert pool.revise_line(p)['new_cost_cents'] == 10710
    assert pool.overview()['products'][0]['average_home_cost_cents'] == 1000
    p['preview'] = False
    pool.revise_line(p)
    assert pool.overview()['products'][0]['average_home_cost_cents'] == 1190
    with db._connect() as c:
        assert c.execute('SELECT SUM(available_quantity*unit_cost_cents+cost_remainder_cents) FROM inventory_lots').fetchone()[0] == 8330
        assert c.execute('SELECT COUNT(*) FROM pool_revisions').fetchone()[0] == 1
    assert pool.revise_line(p)['new_cost_cents'] == 10710


def test_fifo_partial_fulfillment_and_no_unshipped_consumption(setup):
    from app.services.amazon_fba import allocate_order_fifo
    purchase(setup,9,1000)
    transfer = pool.reserve_transfer(key(shipment_id='S1',marketplace_id='DE',lines=[dict(shipment_item_id='S1-item',quantity=7)]))
    pool.change_transfer(key(transfer_id=transfer['id'],action='dispatch',dispatched_at='2026-09-02'))
    with db._connect() as c:
        c.execute("UPDATE amazon_inbound_shipment_items SET quantity_received=7 WHERE shipment_id='S1'")
        db._upsert_order(c,{'AmazonOrderId':'O','MarketplaceId':'DE','PurchaseDate':'2026-09-05'})
        db._upsert_order_items(c,'O',[{'SellerSKU':'SKU-A','ASIN':'A','QuantityOrdered':3,'QuantityShipped':0}])
    pool.reconcile_receipts()
    assert allocate_order_fifo('O')['allocated_cogs_cents'] == 0
    with db._connect() as c:
        c.execute("UPDATE amazon_order_items SET quantity_shipped=1 WHERE amazon_order_id='O'")
    assert allocate_order_fifo('O')['allocated_cogs_cents'] == 1000
    with db._connect() as c:
        c.execute("UPDATE amazon_order_items SET quantity_shipped=3 WHERE amazon_order_id='O'")
    assert allocate_order_fifo('O')['allocated_cogs_cents'] == 2000
    assert allocate_order_fifo('O')['allocated_cogs_cents'] == 0
    with db._connect() as c:
        assert c.execute('SELECT SUM(quantity) FROM fifo_allocations').fetchone()[0] == 3


def test_source_refresh_preserves_pool_references(setup):
    purchase(setup,9,1000)
    pool.reserve_transfer(key(shipment_id='S1',marketplace_id='DE',lines=[dict(shipment_item_id='S1-item',quantity=7)]))
    with db._connect() as c:
        db._upsert_inbound_shipment(c,shipment={'ShipmentId':'S1','ShipmentStatus':'RECEIVING'},items=[{'SellerSKU':'SKU-A','QuantityShipped':7,'QuantityReceived':3}])
        assert c.execute("SELECT id FROM amazon_inbound_shipment_items WHERE shipment_id='S1'").fetchone()[0] == 'S1-item'
        assert c.execute('SELECT COUNT(*) FROM pool_transfer_lines').fetchone()[0] == 1


def test_payment_and_documents_project_once_without_fifo_expense(setup, monkeypatch, tmp_path):
    import sqlite3
    from app import config
    books = tmp_path/'books.sqlite3'
    monkeypatch.setattr(config,'BOOKKEEPING_DB_PATH',books)
    with sqlite3.connect(books) as c:
        c.executescript('''CREATE TABLE documents(id TEXT PRIMARY KEY,original_filename TEXT,stored_filename TEXT,file_path TEXT,mime_type TEXT,uploaded_at TEXT,notes TEXT);
        CREATE TABLE transactions(id TEXT PRIMARY KEY,date TEXT,type TEXT,direction TEXT,amount_gross INTEGER,currency TEXT,provider TEXT,reference TEXT,document_id TEXT,notes TEXT,source TEXT,source_key TEXT,status TEXT,booking_class TEXT,is_vat_deductible INTEGER,created_at TEXT,updated_at TEXT);''')
    invoice, _ = purchase(setup,9,1000)
    payment = key(invoice_id=invoice['id'],paid_at='2026-09-01',amount_cents=4000,account_reference='Karte 123, Teilzahlung')
    pool.record_payment(payment); pool.record_payment(payment)
    pool.sync_bookkeeping(); pool.sync_bookkeeping()
    with sqlite3.connect(books) as c:
        assert c.execute('SELECT COUNT(*),SUM(amount_gross) FROM transactions').fetchone() == (1,4000)


def test_unmatched_legacy_refund_not_hidden_by_modern_sale(setup):
    from app.services.amazon_financials import events
    with db._connect() as c:
        db._upsert_order(c,{'AmazonOrderId':'O'})
        db._upsert_financial_event(c,'RefundEventList',{'AmazonOrderId':'O','ShipmentItemAdjustmentList':[{'ItemChargeAdjustmentList':[{'ChargeAmount':{'CurrencyCode':'EUR','CurrencyAmount':-10}}]}]})
    db.sync_modern_financial_transactions([{'transactionId':'SALE','transactionType':'Shipment','relatedIdentifiers':[{'relatedIdentifierName':'ORDER_ID','relatedIdentifierValue':'O'}],'totalAmount':{'currencyAmount':100,'currencyCode':'EUR'},'breakdowns':[{'breakdownType':'Sales','breakdownAmount':{'currencyAmount':100}}]}])
    with db._connect() as c:
        found = events(c)
    assert len(found) == 2
    assert sum(e['sales_cents'] for e in found) == 9000


def test_confirmed_legacy_migration_retains_lot_and_fifo(setup):
    from app.services import amazon_fba as fba
    with db._connect() as c:
        c.execute("UPDATE amazon_inbound_shipments SET status='CLOSED' WHERE shipment_id='S1'")
        c.execute("UPDATE amazon_inbound_shipment_items SET quantity_received=7 WHERE shipment_id='S1'")
    invoice = fba.add_inbound_invoice(shipment_id='S1',supplier_name='Legacy',invoice_number='L1',invoice_date='2026-09-01',currency='EUR',gross_cents=7000,net_cents=7000,vat_cents=0,document_path='')
    fba.add_inbound_invoice_line(invoice_id=invoice['id'],seller_sku='SKU-A',fnsku='',asin='',title='',quantity=7,gross_cents=7000,net_cents=7000,vat_cents=0)
    old = fba.confirm_inbound_product_costs('S1')
    payload = key(legacy_invoice_id=invoice['id'],marketplace_id='DE',received_at='2026-09-01')
    migrated = pool.migrate_legacy_invoice(payload)
    assert pool.migrate_legacy_invoice(payload) == migrated
    assert pool.reconcile_receipts()['received_quantity'] == 0
    with db._connect() as c:
        assert c.execute('SELECT id FROM inventory_lots').fetchone()[0] == old['lots'][0]['id']
        assert c.execute('SELECT COUNT(*) FROM inventory_lots').fetchone()[0] == 1


def test_pool_api_validation_and_auth(setup,monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from app.routers.amazon_procurement import router
    app = FastAPI(); app.include_router(router)
    monkeypatch.setenv('APP_ADMIN_TOKEN','test-token')
    client = TestClient(app)
    assert client.get('/api/amazon/pool').status_code == 401
    headers = {'X-Admin-Token':'test-token'}
    assert client.get('/api/amazon/pool',headers=headers).status_code == 200
    assert client.post('/api/amazon/pool/products',headers=headers,json={'name':'missing key'}).status_code == 422
    assert client.post('/api/amazon/pool/receipts',headers=headers,json=key(line_id='missing',quantity=1,received_at='2026-09-01')).status_code == 400


def test_late_freight_reprices_received_stock_once(setup):
    purchase(setup,9,1000)
    transfer = pool.reserve_transfer(key(shipment_id='S1',marketplace_id='DE',lines=[dict(shipment_item_id='S1-item',quantity=7)]))
    pool.change_transfer(key(transfer_id=transfer['id'],action='dispatch',dispatched_at='2026-09-02'))
    with db._connect() as c:
        c.execute("UPDATE amazon_inbound_shipment_items SET quantity_received=7 WHERE shipment_id='S1'")
    pool.reconcile_receipts()
    p = key(transfer_id=transfer['id'],expected_freight_cents=0,freight_cents=103,reason='Frachtrechnung nachgereicht',preview=True)
    assert pool.revise_transfer_cost(p)['new_cost_cents'] == 103
    assert pool.overview()['transfers'][0]['freight_cents'] == 0
    p['preview'] = False
    pool.revise_transfer_cost(p); pool.revise_transfer_cost(p)
    with db._connect() as c:
        assert c.execute('SELECT SUM(available_quantity*unit_cost_cents+cost_remainder_cents) FROM inventory_lots').fetchone()[0] == 7103
