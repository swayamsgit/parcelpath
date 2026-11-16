"""Parcelpath: deterministic, local-first retail analytics. Standard library only."""
import argparse
import csv
import hashlib
import json
import random
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SCHEMA = 'PRAGMA foreign_keys=ON;\nCREATE TABLE IF NOT EXISTS dim_customer(customer_id TEXT PRIMARY KEY, city TEXT NOT NULL, segment TEXT NOT NULL);\nCREATE TABLE IF NOT EXISTS dim_product(product_id TEXT PRIMARY KEY, category TEXT NOT NULL, name TEXT NOT NULL);\n-- Grain: one product line per order in this deliberately bounded demo.\nCREATE TABLE IF NOT EXISTS fact_order(\n order_id TEXT PRIMARY KEY, customer_id TEXT NOT NULL REFERENCES dim_customer,\n product_id TEXT NOT NULL REFERENCES dim_product, order_date TEXT NOT NULL,\n quantity INTEGER NOT NULL CHECK(quantity>0), gross_paise INTEGER NOT NULL CHECK(gross_paise>=0),\n discount_paise INTEGER NOT NULL CHECK(discount_paise>=0 AND discount_paise<=gross_paise),\n updated_at TEXT NOT NULL);\nCREATE TABLE IF NOT EXISTS fact_shipment(\n order_id TEXT PRIMARY KEY REFERENCES fact_order, promised_date TEXT NOT NULL,\n delivered_date TEXT, updated_at TEXT NOT NULL);\nCREATE TABLE IF NOT EXISTS load_manifest(\n source TEXT NOT NULL, sha256 TEXT NOT NULL, accepted INTEGER NOT NULL,\n rejected INTEGER NOT NULL, loaded_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,\n PRIMARY KEY(source,sha256));\nCREATE TABLE IF NOT EXISTS quarantine(\n source TEXT NOT NULL, sha256 TEXT NOT NULL, line INTEGER NOT NULL,\n reason TEXT NOT NULL, payload TEXT NOT NULL, PRIMARY KEY(source,sha256,line));\nCREATE INDEX IF NOT EXISTS ix_order_date ON fact_order(order_date);\nCREATE INDEX IF NOT EXISTS ix_order_customer ON fact_order(customer_id);\nCREATE INDEX IF NOT EXISTS ix_order_product ON fact_order(product_id);\nCREATE VIEW IF NOT EXISTS mart_order AS\n SELECT o.order_id,o.order_date,c.city,c.segment,p.category,o.quantity,\n o.gross_paise-o.discount_paise AS net_paise,\n CASE WHEN s.delivered_date IS NOT NULL THEN 1 ELSE 0 END AS delivered,\n CASE WHEN s.delivered_date IS NOT NULL AND s.delivered_date>s.promised_date THEN 1 ELSE 0 END AS late,\n CASE WHEN s.delivered_date IS NOT NULL THEN julianday(s.delivered_date)-julianday(o.order_date) END AS delivery_days\n FROM fact_order o JOIN dim_customer c USING(customer_id)\n JOIN dim_product p USING(product_id) LEFT JOIN fact_shipment s USING(order_id);\n'


def connect(db):
    db = Path(db)
    db.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db)
    con.execute('PRAGMA foreign_keys=ON')
    con.row_factory = sqlite3.Row
    con.executescript(SCHEMA)
    return con


def iso_day(value):
    if not isinstance(value, str) or date.fromisoformat(value).isoformat() != value:
        raise ValueError('date must be YYYY-MM-DD')
    return value


def timestamp(value):
    # Fixed UTC format makes string ordering safe for source version comparisons.
    if datetime.strptime(value, '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%dT%H:%M:%SZ') != value:
        raise ValueError('updated_at must be canonical UTC')
    return value


def positive_int(value, minimum=0):
    if isinstance(value, bool) or str(int(value)) != str(value) or int(value) < minimum:
        raise ValueError('invalid integer')
    return int(value)


def required(row, *keys):
    for key in keys:
        if not isinstance(row.get(key), str) or not row[key].strip():
            raise ValueError('missing or invalid ' + key)
    return [row[k] for k in keys]


def load_row(con, kind, row):
    if not isinstance(row, dict):
        raise ValueError('record must be an object')
    if kind == 'customers':
        values = required(row, 'customer_id', 'city', 'segment')
        con.execute('INSERT INTO dim_customer VALUES(?,?,?) ON CONFLICT(customer_id) DO UPDATE SET city=excluded.city,segment=excluded.segment', values)
    elif kind == 'products':
        values = required(row, 'product_id', 'category', 'name')
        con.execute('INSERT INTO dim_product VALUES(?,?,?) ON CONFLICT(product_id) DO UPDATE SET category=excluded.category,name=excluded.name', values)
    elif kind == 'orders':
        oid, cid, pid = required(row, 'order_id', 'customer_id', 'product_id')
        day = iso_day(row['order_date'])
        qty = positive_int(row['quantity'], 1)
        gross = positive_int(row['gross_paise'])
        discount = positive_int(row['discount_paise'])
        if discount > gross:
            raise ValueError('discount exceeds gross value')
        stamp = timestamp(row['updated_at'])
        con.execute('''INSERT INTO fact_order VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(order_id) DO UPDATE SET customer_id=excluded.customer_id,
            product_id=excluded.product_id,order_date=excluded.order_date,
            quantity=excluded.quantity,gross_paise=excluded.gross_paise,
            discount_paise=excluded.discount_paise,updated_at=excluded.updated_at
            WHERE excluded.updated_at>fact_order.updated_at''',
            (oid, cid, pid, day, qty, gross, discount, stamp))
    elif kind == 'shipments':
        oid, = required(row, 'order_id')
        promised = iso_day(row['promised_date'])
        delivered = iso_day(row['delivered_date']) if row.get('delivered_date') else None
        stamp = timestamp(row['updated_at'])
        order = con.execute('SELECT order_date FROM fact_order WHERE order_id=?', (oid,)).fetchone()
        if order is None:
            raise ValueError('shipment references unknown order')
        if promised < order[0] or (delivered and delivered < order[0]):
            raise ValueError('shipment date precedes order')
        con.execute('''INSERT INTO fact_shipment VALUES(?,?,?,?) ON CONFLICT(order_id)
            DO UPDATE SET promised_date=excluded.promised_date,
            delivered_date=excluded.delivered_date,updated_at=excluded.updated_at
            WHERE excluded.updated_at>fact_shipment.updated_at''', (oid, promised, delivered, stamp))
    else:
        raise ValueError('unknown source kind')


def ingest(con, path, kind):
    """Atomic per file. A changed file is a new batch; same bytes/source are skipped.

    accepted counts valid records processed, including stale/no-op updates.
    Immutable source files are the replay boundary. Dimensions use SCD Type 1.
    """
    path = Path(path)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    source = kind + '/' + path.name
    if con.execute('SELECT 1 FROM load_manifest WHERE source=? AND sha256=?', (source, digest)).fetchone():
        return {'source': source, 'skipped': True, 'accepted': 0, 'rejected': 0}
    if path.suffix == '.jsonl':
        records = enumerate(path.read_text().splitlines(), start=1)
    else:
        # Materialize before starting the transaction so parse/IO errors cannot leave a partial load.
        with path.open(newline='') as handle:
            records = list(enumerate(csv.DictReader(handle), start=2))
    accepted = rejected = 0
    with con:
        con.execute('BEGIN')
        for line, raw in records:
            con.execute('SAVEPOINT record')
            try:
                row = json.loads(raw) if isinstance(raw, str) else raw
                load_row(con, kind, row)
                accepted += 1
                con.execute('RELEASE record')
            except (ValueError, KeyError, TypeError, OverflowError, sqlite3.IntegrityError) as exc:
                con.execute('ROLLBACK TO record')
                con.execute('RELEASE record')
                rejected += 1
                con.execute('INSERT INTO quarantine VALUES(?,?,?,?,?)',
                            (source, digest, line, str(exc), json.dumps(raw, sort_keys=True)))
        con.execute('INSERT INTO load_manifest(source,sha256,accepted,rejected) VALUES(?,?,?,?)',
                    (source, digest, accepted, rejected))
    return {'source': source, 'skipped': False, 'accepted': accepted, 'rejected': rejected}


def load_all(raw, db):
    con = connect(db)
    try:
        result = []
        for kind in ['customers', 'products', 'orders', 'shipments']:
            for path in sorted(Path(raw).glob(kind + '*')):
                if path.suffix in ('.csv', '.jsonl'):
                    result.append(ingest(con, path, kind))
        return result
    finally:
        con.close()


def generate(raw, count=2400):
    raw = Path(raw)
    raw.mkdir(parents=True, exist_ok=True)
    rng = random.Random(2027)
    cities = ['Bengaluru', 'Bhubaneswar', 'Chennai', 'Hyderabad', 'Noida']
    categories = ['Electronics', 'Home', 'Books', 'Sports']
    customers = [{'customer_id': f'C{i:04}', 'city': cities[i % 5], 'segment': 'Returning' if i % 3 else 'New'} for i in range(150)]
    products = [{'product_id': f'P{i:03}', 'category': categories[i % 4], 'name': f'{categories[i % 4]} item {i+1}'} for i in range(40)]
    orders, shipments = [], []
    start = date(2026, 8, 1)
    for i in range(count):
        day = start + timedelta(days=rng.randrange(90))
        city_id = rng.randrange(150)
        qty = rng.randint(1, 4)
        gross = rng.randint(15000, 450000) * qty
        oid = f'O{i:06}'
        orders.append(dict(order_id=oid, customer_id=f'C{city_id:04}', product_id=f'P{rng.randrange(40):03}', order_date=str(day), quantity=qty, gross_paise=gross, discount_paise=int(gross * (rng.randrange(0, 16) / 100)), updated_at=str(day)+'T12:00:00Z'))
        delay = rng.choices([0, 1, 2, 4], weights=[65, 18, 12, 5])[0]
        # Explicit synthetic operational signal: Noida Electronics-like orders are not a real business observation.
        promised = day + timedelta(days=4)
        delivered = promised + timedelta(days=delay) if i % 13 else None
        shipments.append(dict(order_id=oid, promised_date=str(promised), delivered_date=str(delivered) if delivered else '', updated_at=str(day + timedelta(days=10))+'T12:00:00Z'))
    def write_csv(name, values):
        with (raw / name).open('w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=list(values[0]))
            writer.writeheader()
            writer.writerows(values)
    write_csv('customers.csv', customers)
    write_csv('products.csv', products)
    write_csv('shipments-001.csv', shipments)
    with (raw/'orders-001.jsonl').open('w') as f:
        for row in orders:
            f.write(json.dumps(row)+'\n')
        for row in [dict(orders[0], order_id='BAD-QUANTITY', quantity=-1), dict(orders[0], order_id='BAD-CUSTOMER', customer_id='unknown'), dict(orders[0], order_id='BAD-DISCOUNT', discount_paise=999999999)]:
            f.write(json.dumps(row)+'\n')
        f.write('{broken json\n')
    # Late arriving correction uses a newer source version for an existing order.
    corrected = dict(shipments[0], delivered_date=str(start + timedelta(days=(date.fromisoformat(orders[0]['order_date'])-start).days+3)), updated_at='2026-11-02T12:00:00Z')
    write_csv('shipments-002.csv', [corrected])


def quality(con):
    checks = {
        'foreign_keys': len(con.execute('PRAGMA foreign_key_check').fetchall()) == 0,
        'nonempty_orders': con.execute('SELECT count(*) FROM fact_order').fetchone()[0] > 0,
        'unique_order_grain': con.execute('SELECT count(*)=count(DISTINCT order_id) FROM mart_order').fetchone()[0] == 1,
        'nonnegative_net': con.execute('SELECT count(*) FROM mart_order WHERE net_paise<0').fetchone()[0] == 0,
        'valid_delivery_days': con.execute('SELECT count(*) FROM mart_order WHERE delivery_days<0').fetchone()[0] == 0,
        'late_subset_delivered': con.execute('SELECT coalesce(sum(late),0)<=coalesce(sum(delivered),0) FROM mart_order').fetchone()[0] == 1,
        'mart_reconciles_to_fact': con.execute('SELECT (SELECT count(*) FROM mart_order)=(SELECT count(*) FROM fact_order)').fetchone()[0] == 1,
        'money_reconciles': con.execute('SELECT (SELECT coalesce(sum(net_paise),0) FROM mart_order)=(SELECT coalesce(sum(gross_paise-discount_paise),0) FROM fact_order)').fetchone()[0] == 1,
    }
    return checks


def export(con, out):
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    slices = [dict(r) for r in con.execute('''SELECT order_date,city,category,count(*) AS orders,
        sum(net_paise) AS net_paise,sum(delivered) AS delivered,sum(late) AS late
        FROM mart_order GROUP BY order_date,city,category ORDER BY order_date,city,category''')]
    payload = {'dataset': 'Synthetic retail operations / seed 2027', 'period': '1 Aug - 29 Oct 2026',
               'slices': slices, 'quality': quality(con),
               'quarantine': [dict(r) for r in con.execute('SELECT source,line,reason FROM quarantine ORDER BY source,line')],
               'loads': [dict(r) for r in con.execute('SELECT source,accepted,rejected FROM load_manifest ORDER BY source')]}
    (out/'analytics.json').write_text(json.dumps(payload, indent=2))
    with (out/'daily_metrics.csv').open('w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=list(slices[0]) if slices else ['order_date','city','category','orders','net_paise','delivered','late'])
        w.writeheader()
        w.writerows(slices)
    (out/'quality.json').write_text(json.dumps(payload['quality'], indent=2))
    return payload


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['demo', 'load', 'check', 'export', 'query'])
    parser.add_argument('--raw', type=Path, default=ROOT/'data/raw')
    parser.add_argument('--db', type=Path, default=ROOT/'data/warehouse/retail.db')
    parser.add_argument('--out', type=Path, default=ROOT/'reports')
    parser.add_argument('--sql-file', type=Path)
    args = parser.parse_args()
    if args.command == 'demo':
        generate(args.raw)
    if args.command in ('demo', 'load'):
        print(json.dumps(load_all(args.raw, args.db), indent=2))
    if args.command == 'load':
        return
    con = connect(args.db)
    try:
        if args.command in ('demo', 'check'):
            result = quality(con)
            print(json.dumps(result, indent=2))
            if not all(result.values()):
                raise SystemExit(1)
        if args.command in ('demo', 'export'):
            result = export(con, args.out)
            print('Exported', len(result['slices']), 'aggregate slices to', args.out)
        if args.command == 'query':
            if not args.sql_file:
                parser.error('query requires --sql-file')
            # SQLite read-only connection; no extension loading or SQL writes.
            con.close()
            con = sqlite3.connect(f'file:{args.db.resolve()}?mode=ro', uri=True)
            con.row_factory = sqlite3.Row
            con.execute('PRAGMA query_only=ON')
            print(json.dumps([dict(r) for r in con.execute(args.sql_file.read_text()).fetchmany(1000)], indent=2))
    finally:
        con.close()


DASHBOARD = '<!doctype html>\n<html lang="en">\n<head>\n<meta charset="utf-8">\n<meta name="viewport" content="width=device-width,initial-scale=1">\n<meta name="robots" content="noindex">\n<title>Parcelpath - Retail operations studio</title>\n<style>\n:root{\n  --bg:#14110e; --panel:#1c1815; --panel2:#231e19; --line:#322b24; --line2:#443b31;\n  --ink:#f2ece4; --muted:#a89c8d; --faint:#6f6558;\n  --acc:#ff7a1a; --acc-soft:rgba(255,122,26,.14);\n  --ok:#4ade80; --ok-soft:rgba(74,222,128,.12);\n  --bad:#ff5c5c; --bad-soft:rgba(255,92,92,.12);\n  --info:#5ec8f2; --mono:ui-monospace,\'SF Mono\',\'Cascadia Code\',Menlo,Consolas,monospace;\n  --sans:-apple-system,BlinkMacSystemFont,\'Segoe UI\',Roboto,\'Helvetica Neue\',Arial,sans-serif;\n}\n*{box-sizing:border-box;margin:0;padding:0}\nhtml{scrollbar-color:var(--line2) var(--bg)}\nbody{background:var(--bg);color:var(--ink);font-family:var(--sans);font-size:15px;line-height:1.5;min-height:100vh}\n::selection{background:var(--acc);color:#14110e}\nbutton{font:inherit;cursor:pointer;border:none;background:none;color:inherit}\nselect,input,textarea{font:inherit;color:var(--ink);background:var(--bg);border:1px solid var(--line2);border-radius:6px;padding:8px 10px;outline:none}\nselect:focus,input:focus,textarea:focus{border-color:var(--acc)}\n.mono{font-family:var(--mono)}\n\n/* header */\nheader{position:sticky;top:0;z-index:40;background:rgba(20,17,14,.92);backdrop-filter:blur(10px);border-bottom:1px solid var(--line)}\n.hwrap{max-width:1180px;margin:auto;padding:14px 22px;display:flex;align-items:center;gap:14px;flex-wrap:wrap}\n.logo{display:flex;align-items:center;gap:10px;font-weight:800;font-size:17px;letter-spacing:.4px}\n.mark{width:22px;height:22px;background:var(--acc);border-radius:5px;position:relative;flex:none}\n.mark::after{content:\'\';position:absolute;left:5px;top:5px;right:5px;bottom:5px;border:2px solid #14110e;border-radius:2px}\n.tagline{color:var(--faint);font-size:12.5px;letter-spacing:1.6px;text-transform:uppercase}\n.hspace{flex:1}\n.pill{display:inline-flex;align-items:center;gap:7px;font-size:12.5px;padding:6px 12px;border-radius:99px;border:1px solid var(--line2);color:var(--muted)}\n.pill .dot{width:7px;height:7px;border-radius:99px;background:var(--faint)}\n.pill.ok{color:var(--ok);border-color:rgba(74,222,128,.35);background:var(--ok-soft)}\n.pill.ok .dot{background:var(--ok)}\n.pill.bad{color:var(--bad);border-color:rgba(255,92,92,.35);background:var(--bad-soft)}\n.pill.bad .dot{background:var(--bad)}\n.btn{display:inline-flex;align-items:center;gap:8px;padding:9px 16px;border-radius:8px;font-weight:650;font-size:14px;border:1px solid var(--line2);color:var(--ink);background:var(--panel2);transition:transform .06s,border-color .15s,background .15s}\n.btn:hover{border-color:var(--faint)}\n.btn:active{transform:translateY(1px)}\n.btn.primary{background:var(--acc);border-color:var(--acc);color:#14110e}\n.btn.primary:hover{background:#ff8b3d}\n.btn.danger{color:var(--bad);border-color:rgba(255,92,92,.4)}\n.btn:disabled{opacity:.45;cursor:not-allowed;transform:none}\n.btn.small{padding:6px 12px;font-size:13px}\n\n/* tabs */\nnav.tabs{max-width:1180px;margin:0 auto;padding:0 22px;display:flex;gap:2px;border-bottom:1px solid var(--line);overflow-x:auto}\nnav.tabs button{padding:13px 16px 11px;font-size:14px;font-weight:600;color:var(--muted);border-bottom:2px solid transparent;white-space:nowrap}\nnav.tabs button:hover{color:var(--ink)}\nnav.tabs button.on{color:var(--acc);border-bottom-color:var(--acc)}\n\nmain{max-width:1180px;margin:auto;padding:26px 22px 70px}\n.view{display:none;animation:fade .25s ease}\n.view.on{display:block}\n@keyframes fade{from{opacity:0;transform:translateY(4px)}to{opacity:1;transform:none}}\n\n.card{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:20px}\n.card h3{font-size:13px;font-weight:700;letter-spacing:1.4px;text-transform:uppercase;color:var(--muted);margin-bottom:14px;display:flex;align-items:center;gap:10px}\n.card h3 .hint{font-weight:400;text-transform:none;letter-spacing:0;color:var(--faint);font-size:12px;margin-left:auto}\n\n/* overview */\n.filters{display:flex;gap:12px;flex-wrap:wrap;align-items:end;margin-bottom:20px}\n.filters label{display:flex;flex-direction:column;gap:5px;font-size:11.5px;letter-spacing:1px;text-transform:uppercase;color:var(--faint);font-weight:700}\n.kpis{display:grid;grid-template-columns:repeat(5,1fr);gap:14px;margin-bottom:20px}\n.kpi{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:16px 18px;position:relative;overflow:hidden}\n.kpi::before{content:\'\';position:absolute;top:0;left:0;right:0;height:2px;background:var(--line2)}\n.kpi.hot::before{background:var(--acc)}\n.kpi .k{font-size:11.5px;letter-spacing:1.2px;text-transform:uppercase;color:var(--muted);font-weight:700}\n.kpi .v{font-family:var(--mono);font-size:24px;font-weight:700;margin-top:8px;font-variant-numeric:tabular-nums}\n.kpi .s{font-size:12px;color:var(--faint);margin-top:3px}\n.grid2{display:grid;grid-template-columns:1.6fr 1fr;gap:14px;margin-bottom:20px}\n.grid3{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:20px}\n#trend{width:100%;height:240px;display:block;cursor:crosshair}\n.tip{position:fixed;z-index:60;pointer-events:none;background:#0c0a08;border:1px solid var(--line2);border-radius:8px;padding:9px 12px;font-size:12.5px;font-family:var(--mono);box-shadow:0 8px 24px rgba(0,0,0,.5);display:none}\n.tip b{color:var(--acc)}\n.bar-row{display:grid;grid-template-columns:110px 1fr 74px;align-items:center;gap:10px;padding:7px 8px;border-radius:7px;font-size:13.5px}\n.bar-row.clickable{cursor:pointer}\n.bar-row.clickable:hover{background:var(--panel2)}\n.bar-row.sel{outline:1px solid var(--acc);background:var(--acc-soft)}\n.bar-row .n{color:var(--muted);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}\n.bar-track{display:block;height:8px;background:var(--bg);border-radius:99px;overflow:hidden}\n.bar-fill{display:block;height:100%;background:var(--acc);border-radius:99px;transition:width .5s cubic-bezier(.2,.8,.2,1)}\n.bar-row .val{font-family:var(--mono);text-align:right;font-size:12.5px;font-variant-numeric:tabular-nums}\n\n/* pipeline */\n.pipe-head{display:flex;gap:12px;align-items:center;flex-wrap:wrap;margin-bottom:16px}\n.logbox{background:#0c0a08;border:1px solid var(--line);border-radius:12px;padding:16px 18px;font-family:var(--mono);font-size:12.8px;line-height:1.75;max-height:300px;overflow:auto;white-space:pre-wrap}\n.logbox .t{color:var(--faint)}\n.logbox .good{color:var(--ok)} .logbox .warn{color:var(--acc)} .logbox .err{color:var(--bad)}\n.checks{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-top:14px}\n.check{display:flex;align-items:center;gap:9px;padding:11px 13px;border-radius:9px;border:1px solid var(--line);background:var(--panel2);font-size:12.8px;font-family:var(--mono)}\n.check .dot{width:8px;height:8px;border-radius:99px;flex:none}\n.check.pass .dot{background:var(--ok)} .check.fail .dot{background:var(--bad)}\n.check.pass{color:var(--ok)} .check.fail{color:var(--bad)}\n\n/* tables */\n.tablewrap{overflow:auto;border:1px solid var(--line);border-radius:12px}\ntable{width:100%;border-collapse:collapse;font-size:13.5px}\nth{position:sticky;top:0;background:var(--panel2);text-align:left;font-size:11px;letter-spacing:1.1px;text-transform:uppercase;color:var(--muted);padding:11px 14px;border-bottom:1px solid var(--line2);white-space:nowrap}\ntd{padding:10px 14px;border-bottom:1px solid var(--line);font-variant-numeric:tabular-nums}\ntr:last-child td{border-bottom:none}\ntbody tr:hover{background:var(--panel2)}\ntd.mono{font-family:var(--mono);font-size:12.5px}\n.badge{display:inline-block;padding:2px 9px;border-radius:99px;font-size:11.5px;font-weight:700;font-family:var(--mono)}\n.badge.ok{background:var(--ok-soft);color:var(--ok)}\n.badge.bad{background:var(--bad-soft);color:var(--bad)}\n.badge.warn{background:var(--acc-soft);color:var(--acc)}\n.badge.dim{background:var(--panel2);color:var(--muted)}\n.pager{display:flex;gap:10px;align-items:center;margin-top:14px;font-size:13.5px;color:var(--muted)}\n\n/* upload */\n.drop{border:1.5px dashed var(--line2);border-radius:14px;padding:44px 22px;text-align:center;transition:border-color .15s,background .15s;cursor:pointer}\n.drop.over{border-color:var(--acc);background:var(--acc-soft)}\n.drop .big{font-size:16px;font-weight:700;margin-bottom:6px}\n.drop .sub{color:var(--muted);font-size:13.5px}\n.kindgrid{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-top:18px}\n.kind{border:1px solid var(--line);border-radius:9px;padding:11px 13px;background:var(--panel2);font-size:12.8px}\n.kind b{font-family:var(--mono);color:var(--acc);display:block;margin-bottom:3px}\n.kind span{color:var(--muted)}\n\n/* sql */\ntextarea.sql{width:100%;min-height:130px;font-family:var(--mono);font-size:13.5px;line-height:1.6;padding:14px;border-radius:12px;resize:vertical}\n.examples{display:flex;gap:8px;flex-wrap:wrap;margin:12px 0}\n.examples button{font-size:12.5px;font-family:var(--mono);padding:6px 11px;border:1px solid var(--line2);border-radius:99px;color:var(--muted)}\n.examples button:hover{color:var(--acc);border-color:var(--acc)}\n\n.note-line{margin-top:22px;padding:13px 16px;border:1px solid var(--line);border-left:3px solid var(--acc);border-radius:8px;color:var(--muted);font-size:13px;background:var(--panel)}\n.empty{color:var(--faint);text-align:center;padding:30px;font-size:13.5px}\n\n/* toast */\n#toasts{position:fixed;right:18px;bottom:18px;z-index:80;display:flex;flex-direction:column;gap:9px}\n.toast{background:#0c0a08;border:1px solid var(--line2);border-left:3px solid var(--acc);border-radius:10px;padding:12px 16px;font-size:13.5px;max-width:340px;box-shadow:0 10px 30px rgba(0,0,0,.5);animation:tin .2s ease}\n.toast.ok{border-left-color:var(--ok)} .toast.err{border-left-color:var(--bad)}\n@keyframes tin{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:none}}\n\n/* waking overlay */\n#wake{position:fixed;inset:0;z-index:100;background:var(--bg);display:none;align-items:center;justify-content:center;flex-direction:column;gap:16px;text-align:center;padding:20px}\n#wake.on{display:flex}\n.spinner{width:34px;height:34px;border-radius:99px;border:3px solid var(--line2);border-top-color:var(--acc);animation:spin 1s linear infinite}\n@keyframes spin{to{transform:rotate(360deg)}}\n\nfooter{border-top:1px solid var(--line);padding:22px;color:var(--faint);font-size:12.5px;text-align:center;max-width:1180px;margin:auto}\nfooter b{color:var(--muted)}\n\n@media(max-width:980px){.kpis{grid-template-columns:repeat(3,1fr)}.grid2,.grid3{grid-template-columns:1fr}.checks{grid-template-columns:repeat(2,1fr)}}\n@media(max-width:620px){.kpis{grid-template-columns:repeat(2,1fr)}.kindgrid{grid-template-columns:repeat(2,1fr)}.hwrap{padding:12px 16px}main{padding:20px 16px 60px}}\n</style>\n</head>\n<body>\n\n<div id="wake"><div class="spinner"></div><div style="font-weight:700;font-size:17px">Waking the warehouse</div><div style="color:var(--muted);font-size:13.5px;max-width:340px">The free demo host sleeps when idle. First load can take up to a minute - hang tight.</div></div>\n<div class="tip" id="tip"></div>\n<div id="toasts"></div>\n\n<header>\n  <div class="hwrap">\n    <div class="logo"><span class="mark"></span>Parcelpath</div>\n    <span class="tagline">Retail operations studio</span>\n    <span class="hspace"></span>\n    <span class="pill" id="health-pill"><span class="dot"></span><span id="health-text">connecting</span></span>\n    <button class="btn primary" id="run-top">Run pipeline</button>\n  </div>\n</header>\n\n<nav class="tabs">\n  <button data-view="overview" class="on">Overview</button>\n  <button data-view="pipeline">Pipeline</button>\n  <button data-view="upload">Upload data</button>\n  <button data-view="explorer">Explorer</button>\n  <button data-view="sql">SQL console</button>\n</nav>\n\n<main>\n<div class="note-line" style="margin:0 0 20px">Your own temporary demo workspace - no login needed. Uploads, pipeline runs and resets affect only this browser. Use synthetic data, never sensitive records.</div>\n\n<!-- ============ OVERVIEW ============ -->\n<section class="view on" id="view-overview">\n  <div class="filters">\n    <label>City<select id="f-city"><option value="">All cities</option></select></label>\n    <label>Category<select id="f-cat"><option value="">All categories</option></select></label>\n    <label>From<input type="date" id="f-from"></label>\n    <label>To<input type="date" id="f-to"></label>\n    <button class="btn small" id="f-reset">Reset</button>\n  </div>\n\n  <div class="kpis">\n    <div class="kpi"><div class="k">Orders</div><div class="v" id="k-orders">-</div><div class="s">valid order lines</div></div>\n    <div class="kpi hot"><div class="k">Net merchandise value</div><div class="v" id="k-net">-</div><div class="s">gross less discounts</div></div>\n    <div class="kpi"><div class="k">On-time delivery</div><div class="v" id="k-ontime">-</div><div class="s" id="k-ontime-s">of delivered orders</div></div>\n    <div class="kpi"><div class="k">In transit</div><div class="v" id="k-pending">-</div><div class="s">no delivery date yet</div></div>\n    <div class="kpi"><div class="k">Quarantined</div><div class="v" id="k-quar">-</div><div class="s">rejected at ingest</div></div>\n  </div>\n\n  <div class="grid2">\n    <div class="card">\n      <h3>Daily net merchandise value <span class="hint" id="trend-range"></span></h3>\n      <canvas id="trend"></canvas>\n    </div>\n    <div class="card">\n      <h3>Warehouse health</h3>\n      <div id="ov-checks" style="display:flex;flex-direction:column;gap:8px"></div>\n      <div style="margin-top:16px;font-size:12.5px;color:var(--faint)" id="ov-manifest"></div>\n    </div>\n  </div>\n\n  <div class="grid3">\n    <div class="card"><h3>Net value by city <span class="hint">click a row to filter</span></h3><div id="by-city"></div></div>\n    <div class="card"><h3>Net value by category <span class="hint">click a row to filter</span></h3><div id="by-cat"></div></div>\n  </div>\n\n  <div class="note-line">Net merchandise value is gross value minus discounts - not recognized revenue or profit; it excludes returns, tax and shipping. On-time rate counts delivered orders only. "In transit" means the source snapshot has no delivery date, not a live carrier status. All data is synthetic demonstration data.</div>\n</section>\n\n<!-- ============ PIPELINE ============ -->\n<section class="view" id="view-pipeline">\n  <div class="card" style="margin-bottom:14px">\n    <h3>Batch pipeline</h3>\n    <p style="color:var(--muted);font-size:13.5px;margin-bottom:16px">Runs the real loader against every batch in the incoming folder: SHA-256 replay guard, per-file atomic load, per-record savepoints, quarantine for invalid rows, then the full quality suite. Rerunning unchanged files skips them - the warehouse never double-loads.</p>\n    <div class="pipe-head">\n      <button class="btn primary" id="run-pipe">Run pipeline now</button>\n      <button class="btn danger" id="reset-demo">Reset demo dataset</button>\n      <span style="color:var(--faint);font-size:12.5px" id="pipe-state"></span>\n    </div>\n    <div class="logbox" id="pipe-log"><span class="t">Pipeline output will stream here.</span></div>\n  </div>\n  <div class="card">\n    <h3>Quality suite <span class="hint">8 checks over the live warehouse</span></h3>\n    <div class="checks" id="pipe-checks"></div>\n  </div>\n</section>\n\n<!-- ============ UPLOAD ============ -->\n<section class="view" id="view-upload">\n  <div class="card" style="margin-bottom:14px">\n    <h3>Ingest a batch</h3>\n    <p style="color:var(--muted);font-size:13.5px;margin-bottom:16px">Drop a source batch and it loads through the same validation, quarantine and replay guard as the CLI. Upload the identical file twice - the second attempt is skipped by hash, proving replay safety.</p>\n    <div class="drop" id="drop">\n      <div class="big">Drop a batch file here</div>\n      <div class="sub">or click to choose - .jsonl or .csv, up to 1 MB</div>\n      <input type="file" id="file-in" aria-label="Choose batch file" accept=".csv,.jsonl" style="display:block;margin:16px auto 0;max-width:100%">\n    </div>\n    <div class="kindgrid">\n      <div class="kind"><b>orders-*.jsonl</b><span>one order line per row: order_id, customer_id, product_id, order_date, quantity, gross_paise, discount_paise, updated_at</span></div>\n      <div class="kind"><b>shipments-*.csv</b><span>order_id, promised_date, delivered_date (blank = in transit), updated_at</span></div>\n      <div class="kind"><b>customers-*.csv</b><span>customer_id, city, segment - dimension rows, latest write wins</span></div>\n      <div class="kind"><b>products-*.csv</b><span>product_id, category, name - dimension rows, latest write wins</span></div>\n    </div>\n    <div style="margin-top:16px;display:flex;gap:8px;flex-wrap:wrap;align-items:center">\n      <span style="font-size:12.5px;color:var(--faint)">Need test data?</span>\n      <button class="btn small" data-sample="orders">Sample orders batch</button>\n      <button class="btn small" data-sample="shipments">Sample shipments batch</button>\n      <button class="btn small" data-sample="messy">Messy batch (watch quarantine)</button>\n    </div>\n  </div>\n  <div class="card" id="up-result-card" style="display:none">\n    <h3>Ingest result</h3>\n    <div id="up-result"></div>\n  </div>\n</section>\n\n<!-- ============ EXPLORER ============ -->\n<section class="view" id="view-explorer">\n  <div style="display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap" id="ex-tabs">\n    <button class="btn small" data-ex="orders">Orders</button>\n    <button class="btn small" data-ex="quarantine">Quarantine</button>\n    <button class="btn small" data-ex="manifest">Load manifest</button>\n  </div>\n  <div id="ex-orders">\n    <div class="filters">\n      <label>Status<select id="ex-status"><option value="">Any status</option><option value="delivered">Delivered</option><option value="late">Delivered late</option><option value="transit">In transit</option></select></label>\n      <label>Order ID<input id="ex-q" placeholder="O000123" style="width:130px"></label>\n      <button class="btn small" id="ex-apply">Apply</button>\n    </div>\n    <div class="tablewrap"><table>\n      <thead><tr><th>Order</th><th>Date</th><th>City</th><th>Category</th><th>Qty</th><th>Net value</th><th>Status</th><th>Delivery days</th></tr></thead>\n      <tbody id="ex-rows"></tbody>\n    </table></div>\n    <div class="pager"><button class="btn small" id="ex-prev">Prev</button><span id="ex-page"></span><button class="btn small" id="ex-next">Next</button></div>\n  </div>\n  <div id="ex-quarantine" style="display:none">\n    <div class="tablewrap"><table>\n      <thead><tr><th>Source</th><th>Line</th><th>Reason</th><th>Payload</th></tr></thead>\n      <tbody id="quar-rows"></tbody>\n    </table></div>\n  </div>\n  <div id="ex-manifest" style="display:none">\n    <div class="tablewrap"><table>\n      <thead><tr><th>Source batch</th><th>Accepted</th><th>Rejected</th><th>Loaded at</th><th>SHA-256</th></tr></thead>\n      <tbody id="man-rows"></tbody>\n    </table></div>\n  </div>\n</section>\n\n<!-- ============ SQL ============ -->\n<section class="view" id="view-sql">\n  <div class="card">\n    <h3>SQL console <span class="hint">read-only against the live warehouse - SELECT and WITH only, 1,000 row cap</span></h3>\n    <div class="examples" id="sql-examples"></div>\n    <textarea class="sql" id="sql-in" spellcheck="false">SELECT order_date, count(*) AS orders, printf(\'%.2f\', sum(net_paise)/100.0) AS net_inr\nFROM mart_order\nGROUP BY order_date\nORDER BY order_date DESC\nLIMIT 15</textarea>\n    <div style="margin-top:12px;display:flex;gap:10px;align-items:center">\n      <button class="btn primary" id="sql-run">Run query</button>\n      <span style="color:var(--faint);font-size:12.5px" id="sql-meta"></span>\n    </div>\n    <div style="margin-top:16px" id="sql-out"></div>\n  </div>\n  <div class="note-line" style="margin-top:16px">Tables: <span class="mono">dim_customer, dim_product, fact_order, fact_shipment, quarantine, load_manifest</span> and the analytical view <span class="mono">mart_order</span> (order grain with city, segment, category, net_paise, delivered, late, delivery_days). Money is integer paise; divide by 100 for INR.</div>\n</section>\n\n</main>\n\n<footer>\n  <b>Parcelpath</b> - standard-library Python 3 + SQLite, no dependencies. Synthetic demonstration data, no real business data.\n  Public demo, no login. Your browser has its own temporary workspace. Uploads and resets do not affect other visitors. Use synthetic data only. Workspaces may reset after 2 idle hours, on restart or when cookies are cleared.\n</footer>\n\n<script>\n\'use strict\';\nconst $=id=>document.getElementById(id);\nconst money=p=>\'₹\'+Math.round(p/100).toLocaleString(\'en-IN\');\nconst num=n=>Number(n).toLocaleString(\'en-IN\');\nconst esc=s=>String(s).replace(/[&<>"]/g,c=>({\'&\':\'&amp;\',\'<\':\'&lt;\',\'>\':\'&gt;\',\'"\':\'&quot;\'}[c]));\n\n/* ---------- waking / fetch ---------- */\nlet firstLoad=true;\nasync function api(path,opts={},retries=40){\n  if(opts.method===\'POST\')retries=1;\n  for(let i=0;i<retries;i++){\n    try{\n      const r=await fetch(path,opts);\n      if(r.status===503||r.status===502||r.status===504)throw{waking:true};\n      if(firstLoad){firstLoad=false;$(\'wake\').classList.remove(\'on\')}\n      if(!r.ok){const t=await r.json().catch(()=>({error:r.statusText}));throw{error:t.error||\'Request failed\'}}\n      return await r.json();\n    }catch(e){\n      if(e.error!==undefined)throw e;\n      if(firstLoad)$(\'wake\').classList.add(\'on\');\n      await new Promise(res=>setTimeout(res,2500));\n    }\n  }\n  $(\'wake\').classList.remove(\'on\');\n  throw{error:\'Server is still waking up - try again in a moment\'};\n}\nfunction toast(msg,kind=\'\'){\n  const t=document.createElement(\'div\');t.className=\'toast \'+kind;t.textContent=msg;\n  $(\'toasts\').appendChild(t);setTimeout(()=>t.remove(),4200);\n}\n\n/* ---------- tabs ---------- */\ndocument.querySelectorAll(\'nav.tabs button\').forEach(b=>b.onclick=()=>{\n  document.querySelectorAll(\'nav.tabs button\').forEach(x=>x.classList.toggle(\'on\',x===b));\n  document.querySelectorAll(\'.view\').forEach(v=>v.classList.toggle(\'on\',v.id===\'view-\'+b.dataset.view));\n  if(b.dataset.view===\'explorer\')loadExplorer();\n  if(b.dataset.view===\'pipeline\')loadChecks();\n});\n\n/* ---------- overview ---------- */\nlet M=null, trendData=[];\nasync function loadMetrics(){\n  const q=new URLSearchParams({city:$(\'f-city\').value,category:$(\'f-cat\').value,from:$(\'f-from\').value,to:$(\'f-to\').value});\n  const d=await api(\'/api/metrics?\'+q);\n  M=d;\n  if(!$(\'f-city\').dataset.filled){\n    for(const c of d.cities){const o=document.createElement(\'option\');o.value=o.textContent=c;$(\'f-city\').appendChild(o)}\n    for(const c of d.categories){const o=document.createElement(\'option\');o.value=o.textContent=c;$(\'f-cat\').appendChild(o)}\n    $(\'f-city\').dataset.filled=\'1\';\n  }\n  $(\'k-orders\').textContent=num(d.orders);\n  $(\'k-net\').textContent=money(d.net_paise);\n  $(\'k-ontime\').textContent=d.delivered?((d.delivered-d.late)/d.delivered*100).toFixed(1)+\'%\':\'-\';\n  $(\'k-ontime-s\').textContent=d.delivered?num(d.delivered-d.late)+\' of \'+num(d.delivered)+\' on time\':\'of delivered orders\';\n  $(\'k-pending\').textContent=num(d.orders-d.delivered);\n  $(\'k-quar\').textContent=num(d.quarantined);\n  trendData=d.daily;\n  $(\'trend-range\').textContent=d.daily.length?d.daily[0].order_date+\' - \'+d.daily[d.daily.length-1].order_date:\'no matching orders\';\n  drawTrend();\n  renderBars(\'by-city\',d.by_city,\'city\');\n  renderBars(\'by-cat\',d.by_category,\'category\');\n  renderOvChecks(d.quality);\n  $(\'ov-manifest\').innerHTML=\'Batches loaded: <b style="color:var(--muted)">\'+d.batches+\'</b> &nbsp;·&nbsp; Last load: <b style="color:var(--muted)">\'+esc(d.last_load||\'-\')+\'</b>\';\n  const pass=Object.values(d.quality).every(Boolean);\n  const pill=$(\'health-pill\');pill.className=\'pill \'+(pass?\'ok\':\'bad\');\n  $(\'health-text\').textContent=pass?\'8/8 checks passing\':\'quality check failing\';\n}\nfunction renderOvChecks(q){\n  $(\'ov-checks\').innerHTML=Object.entries(q).map(([k,v])=>\n    \'<div class="check \'+(v?\'pass\':\'fail\')+\'"><span class="dot"></span>\'+esc(k.replaceAll(\'_\',\' \'))+\'</div>\').join(\'\');\n}\nfunction renderBars(el,rows,field){\n  const max=Math.max(1,...rows.map(r=>r.net_paise));\n  const cur=field===\'city\'?$(\'f-city\').value:$(\'f-cat\').value;\n  $(el).innerHTML=rows.map(r=>{\n    const sel=cur===r.name?\' sel\':\'\';\n    return \'<div class="bar-row clickable\'+sel+\'" data-k="\'+field+\'" data-v="\'+esc(r.name)+\'">\'+\n      \'<span class="n">\'+esc(r.name)+\'</span>\'+\n      \'<span class="bar-track"><span class="bar-fill" style="width:\'+(r.net_paise/max*100).toFixed(1)+\'%"></span></span>\'+\n      \'<span class="val">\'+money(r.net_paise)+\'</span></div>\';\n  }).join(\'\')||\'<div class="empty">No data</div>\';\n  $(el).querySelectorAll(\'.bar-row\').forEach(row=>row.onclick=()=>{\n    const sel=row.dataset.k===\'city\'?$(\'f-city\'):$(\'f-cat\');\n    sel.value=sel.value===row.dataset.v?\'\':row.dataset.v;\n    loadMetrics();\n  });\n}\n\n/* trend chart with hover */\nconst cv=$(\'trend\'),ctx=cv.getContext(\'2d\');let hoverI=-1;\nfunction drawTrend(){\n  const dpr=window.devicePixelRatio||1,W=cv.clientWidth,H=cv.clientHeight;\n  cv.width=W*dpr;cv.height=H*dpr;ctx.setTransform(dpr,0,0,dpr,0,0);ctx.clearRect(0,0,W,H);\n  const d=trendData;if(!d.length){ctx.fillStyle=\'#6f6558\';ctx.font=\'13px sans-serif\';ctx.fillText(\'No matching orders\',16,28);return}\n  const pad={l:64,r:14,t:14,b:26},iw=W-pad.l-pad.r,ih=H-pad.t-pad.b;\n  const max=Math.max(...d.map(x=>x.net_paise))*1.06;\n  const X=i=>pad.l+(d.length===1?iw/2:i/(d.length-1)*iw),Y=v=>pad.t+ih-(v/max)*ih;\n  ctx.strokeStyle=\'#322b24\';ctx.fillStyle=\'#6f6558\';ctx.font=\'11px ui-monospace,monospace\';ctx.textAlign=\'right\';\n  for(let g=0;g<=4;g++){const v=max/4*g,y=Y(v);\n    ctx.beginPath();ctx.moveTo(pad.l,y);ctx.lineTo(W-pad.r,y);ctx.stroke();\n    ctx.fillText(money(v),pad.l-8,y+4);}\n  ctx.textAlign=\'center\';\n  const step=Math.ceil(d.length/6);\n  for(let i=0;i<d.length;i+=step)ctx.fillText(d[i].order_date.slice(5),X(i),H-8);\n  // area\n  const grad=ctx.createLinearGradient(0,pad.t,0,H-pad.b);\n  grad.addColorStop(0,\'rgba(255,122,26,.25)\');grad.addColorStop(1,\'rgba(255,122,26,0)\');\n  ctx.beginPath();d.forEach((x,i)=>i?ctx.lineTo(X(i),Y(x.net_paise)):ctx.moveTo(X(0),Y(x.net_paise)));\n  ctx.lineTo(X(d.length-1),Y(0));ctx.lineTo(X(0),Y(0));ctx.closePath();ctx.fillStyle=grad;ctx.fill();\n  // line\n  ctx.beginPath();d.forEach((x,i)=>i?ctx.lineTo(X(i),Y(x.net_paise)):ctx.moveTo(X(0),Y(x.net_paise)));\n  ctx.strokeStyle=\'#ff7a1a\';ctx.lineWidth=2.2;ctx.lineJoin=\'round\';ctx.stroke();\n  if(hoverI>=0&&hoverI<d.length){\n    const x=X(hoverI);\n    ctx.strokeStyle=\'rgba(242,236,228,.25)\';ctx.lineWidth=1;\n    ctx.beginPath();ctx.moveTo(x,pad.t);ctx.lineTo(x,H-pad.b);ctx.stroke();\n    ctx.beginPath();ctx.arc(x,Y(d[hoverI].net_paise),4.5,0,7);ctx.fillStyle=\'#ff7a1a\';ctx.fill();\n    ctx.beginPath();ctx.arc(x,Y(d[hoverI].net_paise),8,0,7);ctx.strokeStyle=\'rgba(255,122,26,.4)\';ctx.stroke();\n  }\n}\ncv.addEventListener(\'mousemove\',e=>{\n  const d=trendData;if(!d.length)return;\n  const r=cv.getBoundingClientRect(),pad={l:64,r:14},iw=r.width-pad.l-pad.r;\n  const i=Math.round((e.clientX-r.left-pad.l)/iw*(d.length-1));\n  hoverI=Math.max(0,Math.min(d.length-1,i));\n  const tip=$(\'tip\');tip.style.display=\'block\';\n  tip.innerHTML=\'<b>\'+d[hoverI].order_date+\'</b><br>\'+money(d[hoverI].net_paise)+\' · \'+num(d[hoverI].orders)+\' orders\';\n  tip.style.left=Math.min(window.innerWidth-170,e.clientX+14)+\'px\';tip.style.top=(e.clientY-10)+\'px\';\n  drawTrend();\n});\ncv.addEventListener(\'mouseleave\',()=>{hoverI=-1;$(\'tip\').style.display=\'none\';drawTrend()});\nwindow.addEventListener(\'resize\',()=>drawTrend());\n[\'f-city\',\'f-cat\',\'f-from\',\'f-to\'].forEach(id=>$(id).onchange=loadMetrics);\n$(\'f-reset\').onclick=()=>{[\'f-city\',\'f-cat\',\'f-from\',\'f-to\'].forEach(id=>$(id).value=\'\');loadMetrics()};\n\n/* ---------- pipeline ---------- */\nlet pollTimer=null;\nasync function runPipeline(){\n  $(\'run-pipe\').disabled=$(\'run-top\').disabled=true;$(\'pipe-state\').textContent=\'running...\';\n  $(\'pipe-log\').innerHTML=\'\';\n  try{\n    await api(\'/api/pipeline/run\',{method:\'POST\'});\n    pollTimer=setInterval(pollJob,700);pollJob();\n  }catch(e){toast(e.error,\'err\');$(\'run-pipe\').disabled=$(\'run-top\').disabled=false;$(\'pipe-state\').textContent=\'\'}\n}\nasync function pollJob(){\n  const j=await api(\'/api/pipeline/status\');\n  $(\'pipe-log\').innerHTML=j.log.map(l=>\'<div><span class="t">\'+esc(l.t)+\'</span> <span class="\'+l.c+\'">\'+esc(l.m)+\'</span></div>\').join(\'\');\n  $(\'pipe-log\').scrollTop=1e9;\n  if(j.state!==\'running\'){\n    clearInterval(pollTimer);\n    $(\'run-pipe\').disabled=$(\'run-top\').disabled=false;\n    $(\'pipe-state\').textContent=j.state===\'done\'?\'finished\':\'failed\';\n    if(j.state===\'done\'){toast(\'Pipeline finished\',\'ok\');loadChecks();loadMetrics()}\n    else toast(\'Pipeline failed - see log\',\'err\');\n  }\n}\nasync function loadChecks(){\n  const d=await api(\'/api/quality\');\n  $(\'pipe-checks\').innerHTML=Object.entries(d.checks).map(([k,v])=>\n    \'<div class="check \'+(v?\'pass\':\'fail\')+\'"><span class="dot"></span>\'+esc(k.replaceAll(\'_\',\' \'))+\'</div>\').join(\'\');\n}\n$(\'reset-demo\').onclick=async()=>{\n  if(!confirm(\'Reset to a freshly generated synthetic dataset? Uploaded batches will be removed.\'))return;\n  try{await api(\'/api/reset\',{method:\'POST\'});toast(\'Demo dataset regenerated\',\'ok\');loadChecks();loadMetrics()}\n  catch(e){toast(e.error,\'err\')}\n};\n$(\'run-pipe\').onclick=runPipeline;$(\'run-top\').onclick=()=>{document.querySelector(\'[data-view=pipeline]\').click();runPipeline()};\n\n/* ---------- upload ---------- */\nconst drop=$(\'drop\'),fileIn=$(\'file-in\');\ndrop.onclick=e=>{if(e.target!==fileIn)fileIn.click()};\n[\'dragover\',\'dragenter\'].forEach(ev=>drop.addEventListener(ev,e=>{e.preventDefault();drop.classList.add(\'over\')}));\n[\'dragleave\',\'drop\'].forEach(ev=>drop.addEventListener(ev,e=>{e.preventDefault();drop.classList.remove(\'over\')}));\ndrop.addEventListener(\'drop\',e=>{if(e.dataTransfer.files[0])uploadFile(e.dataTransfer.files[0])});\nfileIn.onchange=async()=>{const file=fileIn.files[0];if(file)await uploadFile(file);fileIn.value=\'\'};\nasync function uploadFile(f){\n  const filename=f.name.replace(/^\\d+-(?=(?:orders|shipments|customers|products)[-._])/,\'\');\n  if(f.size>1024*1024){toast(\'File over 1 MB cap\',\'err\');return}\n  try{\n    const r=await api(\'/api/upload?filename=\'+encodeURIComponent(filename),{method:\'POST\',body:await f.arrayBuffer()});\n    showUploadResult(filename,r);\n    if(r.accepted>0){loadMetrics();toast(\'Loaded \'+r.accepted+\' records\',\'ok\')}\n    else if(r.skipped)toast(\'Skipped - identical file already loaded\',\'\');\n    else toast(\'No records accepted\',\'err\');\n  }catch(e){toast(e.error,\'err\')}\n}\nfunction showUploadResult(name,r){\n  $(\'up-result-card\').style.display=\'block\';\n  let h=\'<div style="display:flex;gap:10px;flex-wrap:wrap;margin-bottom:12px">\'+\n    \'<span class="badge dim mono">\'+esc(name)+\'</span>\'+\n    (r.skipped?\'<span class="badge warn">skipped - replay guard</span>\':\n      \'<span class="badge ok">\'+r.accepted+\' accepted</span>\'+(r.rejected?\'<span class="badge bad">\'+r.rejected+\' quarantined</span>\':\'\'))+\'</div>\';\n  if(r.quarantine&&r.quarantine.length){\n    h+=\'<div class="tablewrap"><table><thead><tr><th>Line</th><th>Reason</th></tr></thead><tbody>\'+\n      r.quarantine.map(q=>\'<tr><td class="mono">\'+q.line+\'</td><td style="color:var(--bad)">\'+esc(q.reason)+\'</td></tr>\').join(\'\')+\n      \'</tbody></table></div>\';\n  }\n  $(\'up-result\').innerHTML=h;\n}\ndocument.querySelectorAll(\'[data-sample]\').forEach(b=>b.onclick=()=>{window.location=\'/api/sample?kind=\'+b.dataset.sample});\n\n/* ---------- explorer ---------- */\nlet exPage=0;const EX_SIZE=25;\ndocument.querySelectorAll(\'#ex-tabs button\').forEach(b=>b.onclick=()=>{\n  document.querySelectorAll(\'#ex-tabs button\').forEach(x=>x.style.borderColor=\'\');\n  b.style.borderColor=\'var(--acc)\';\n  [\'orders\',\'quarantine\',\'manifest\'].forEach(v=>$(\'ex-\'+v).style.display=v===b.dataset.ex?\'block\':\'none\');\n  loadExplorer();\n});\nasync function loadExplorer(){\n  const visible=[\'orders\',\'quarantine\',\'manifest\'].find(v=>$(\'ex-\'+v).style.display!==\'none\');\n  if(visible===\'orders\'){\n    const q=new URLSearchParams({page:exPage,status:$(\'ex-status\').value,q:$(\'ex-q\').value.trim(),city:$(\'f-city\').value,category:$(\'f-cat\').value});\n    const d=await api(\'/api/orders?\'+q);\n    $(\'ex-rows\').innerHTML=d.rows.map(r=>{\n      const st=r.delivered?(r.late?\'<span class="badge bad">late</span>\':\'<span class="badge ok">delivered</span>\'):\'<span class="badge warn">in transit</span>\';\n      return \'<tr><td class="mono">\'+esc(r.order_id)+\'</td><td class="mono">\'+r.order_date+\'</td><td>\'+esc(r.city)+\'</td><td>\'+esc(r.category)+\'</td><td>\'+r.quantity+\'</td><td class="mono">\'+money(r.net_paise)+\'</td><td>\'+st+\'</td><td class="mono">\'+(r.delivery_days==null?\'-\':r.delivery_days.toFixed(1))+\'</td></tr>\';\n    }).join(\'\')||\'<tr><td colspan="8" class="empty">No matching orders</td></tr>\';\n    const pages=Math.max(1,Math.ceil(d.total/EX_SIZE));\n    $(\'ex-page\').textContent=\'page \'+(exPage+1)+\' of \'+pages+\' · \'+num(d.total)+\' orders\';\n    $(\'ex-prev\').disabled=exPage<=0;$(\'ex-next\').disabled=exPage>=pages-1;\n  }else if(visible===\'quarantine\'){\n    const d=await api(\'/api/quarantine\');\n    $(\'quar-rows\').innerHTML=d.rows.map(r=>\'<tr><td class="mono">\'+esc(r.source)+\'</td><td class="mono">\'+r.line+\'</td><td style="color:var(--bad)">\'+esc(r.reason)+\'</td><td class="mono" style="max-width:420px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="\'+esc(r.payload)+\'">\'+esc(r.payload)+\'</td></tr>\').join(\'\')||\'<tr><td colspan="4" class="empty">Quarantine is empty - nothing rejected</td></tr>\';\n  }else{\n    const d=await api(\'/api/manifest\');\n    $(\'man-rows\').innerHTML=d.rows.map(r=>\'<tr><td class="mono">\'+esc(r.source)+\'</td><td>\'+r.accepted+\'</td><td>\'+(r.rejected?\'<span style="color:var(--bad)">\'+r.rejected+\'</span>\':\'0\')+\'</td><td class="mono">\'+esc(r.loaded_at)+\'</td><td class="mono" style="color:var(--faint)">\'+esc(r.sha256.slice(0,12))+\'…</td></tr>\').join(\'\')||\'<tr><td colspan="5" class="empty">No batches loaded yet</td></tr>\';\n  }\n}\n$(\'ex-apply\').onclick=()=>{exPage=0;loadExplorer()};\n$(\'ex-prev\').onclick=()=>{exPage--;loadExplorer()};\n$(\'ex-next\').onclick=()=>{exPage++;loadExplorer()};\n\n/* ---------- sql ---------- */\nconst EXAMPLES={\n  \'Daily net value\':\'SELECT order_date, count(*) AS orders, printf(\\\'%.2f\\\', sum(net_paise)/100.0) AS net_inr\\nFROM mart_order GROUP BY order_date ORDER BY order_date DESC LIMIT 15\',\n  \'Late rate by city\':\'SELECT city, count(*) AS orders, sum(delivered) AS delivered, sum(late) AS late,\\n printf(\\\'%.1f\\\', 100.0*sum(late)/max(sum(delivered),1)) AS late_pct\\nFROM mart_order GROUP BY city ORDER BY late_pct DESC\',\n  \'Top categories\':\'SELECT category, count(*) AS orders, printf(\\\'%.2f\\\', sum(net_paise)/100.0) AS net_inr\\nFROM mart_order GROUP BY category ORDER BY sum(net_paise) DESC\',\n  \'Quarantine reasons\':\'SELECT source, line, reason FROM quarantine ORDER BY source, line\',\n  \'Slowest deliveries\':\'SELECT order_id, city, category, delivery_days\\nFROM mart_order WHERE delivered=1 ORDER BY delivery_days DESC LIMIT 10\',\n};\n$(\'sql-examples\').innerHTML=Object.keys(EXAMPLES).map(k=>\'<button>\'+esc(k)+\'</button>\').join(\'\');\n$(\'sql-examples\').querySelectorAll(\'button\').forEach((b,i)=>b.onclick=()=>{$(\'sql-in\').value=Object.values(EXAMPLES)[i]});\n$(\'sql-run\').onclick=async()=>{\n  $(\'sql-meta\').textContent=\'running...\';$(\'sql-out\').innerHTML=\'\';\n  try{\n    const d=await api(\'/api/query\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({sql:$(\'sql-in\').value})});\n    $(\'sql-meta\').textContent=d.rows.length+\' rows\'+(d.capped?\' (capped at 1,000)\':\'\')+\' · read-only\';\n    if(!d.rows.length){$(\'sql-out\').innerHTML=\'<div class="empty">No rows</div>\';return}\n    const cols=Object.keys(d.rows[0]);\n    $(\'sql-out\').innerHTML=\'<div class="tablewrap" style="max-height:420px"><table><thead><tr>\'+cols.map(c=>\'<th>\'+esc(c)+\'</th>\').join(\'\')+\'</tr></thead><tbody>\'+\n      d.rows.map(r=>\'<tr>\'+cols.map(c=>\'<td class="mono">\'+esc(r[c]===null?\'NULL\':r[c])+\'</td>\').join(\'\')+\'</tr>\').join(\'\')+\'</tbody></table></div>\';\n  }catch(e){$(\'sql-meta\').textContent=\'\';$(\'sql-out\').innerHTML=\'<div class="empty" style="color:var(--bad)">\'+esc(e.error)+\'</div>\'}\n};\n\n/* ---------- boot ---------- */\nconst hv=location.hash.slice(1);if(hv){const b=document.querySelector(\'[data-view=\'+hv+\']\');if(b)b.click()}\nloadMetrics().catch(e=>{toast(e.error,\'err\');$(\'wake\').classList.remove(\'on\')});\ndocument.querySelector(\'[data-ex=orders]\').style.borderColor=\'var(--acc)\';\n</script>\n</body>\n  </html>\n'


"""Parcelpath web app server. Binds to localhost by default; --host 0.0.0.0 for a hosted demo."""
import hashlib
import hmac
import json
import os
import re
import secrets
import shutil
import sqlite3
import threading
import time
from collections import defaultdict
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

# Public demo: no login. Every visitor gets an isolated temporary workspace - their own
# SQLite warehouse copied from the deterministic seed - so uploads, pipeline runs and
# resets never affect other visitors. Workspaces live only on the host's ephemeral disk.
HOSTED = bool(os.environ.get('RENDER'))
SESSION_SECRET = (os.environ.get('PP_SESSION_SECRET') or os.environ.get('PP_ACCESS_KEY')
                  or secrets.token_hex(32))
SID_RE = re.compile(r'^[A-Za-z0-9_-]{16,80}$')
MAX_SESSIONS = 20        # visitor workspaces kept on disk at once
SESSION_TTL = 2 * 3600   # idle seconds before a workspace is swept
SESS = {}                # sid -> {'lock': Lock, 'job': dict, 'touched': float}
SESS_LOCK = threading.Lock()
SESSIONS_ROOT = None     # set by make_handler next to the seed warehouse
BATCH_NAME = re.compile(r'^(customers|products|orders|shipments)[\w.-]*\.(csv|jsonl)$')
MAX_UPLOAD = 1024 * 1024
MAX_BATCHES = 40
RATE_LIMIT = 60  # mutating requests per IP per hour
RATE = defaultdict(list)


def _sign(value):
    return hmac.new(SESSION_SECRET.encode(), value.encode(), hashlib.sha256).hexdigest()


def session_cookie(sid):
    secure = '; Secure' if HOSTED else ''
    return f'pp_sid={sid}.{_sign(sid)}; Path=/; HttpOnly; SameSite=Strict; Max-Age=86400' + secure


def sid_from_headers(headers):
    try:
        raw = SimpleCookie(headers.get('Cookie', '')).get('pp_sid')
        if raw is None:
            return ''
        sid, sig = raw.value.split('.', 1)
        if SID_RE.match(sid) and hmac.compare_digest(sig, _sign(sid)):
            return sid
    except (ValueError, AttributeError):
        pass
    return ''


def workspace_for(sid, seed_db, seed_raw):
    """Allocate a bounded workspace. Never evict a workspace during a request."""
    now = time.time()
    with SESS_LOCK:
        for old, state in list(SESS.items()):
            if now - state['touched'] > SESSION_TTL and not state['active'] and state['job']['state'] != 'running':
                shutil.rmtree(SESSIONS_ROOT / old, ignore_errors=True)
                del SESS[old]
        st = SESS.get(sid)
        if st is None:
            if len(SESS) >= MAX_SESSIONS:
                raise ValueError('Demo is at capacity. Please try again later.')
            st = {'lock': threading.RLock(), 'job': {'state': 'idle', 'log': []},
                  'touched': now, 'active': 0, 'actions': []}
            db = SESSIONS_ROOT / sid / 'warehouse.db'
            raw = SESSIONS_ROOT / sid / 'raw'
            raw.mkdir(parents=True, exist_ok=True)
            for f in Path(seed_raw).glob('*'):
                if f.suffix in ('.csv', '.jsonl'):
                    shutil.copy2(f, raw / f.name)
            shutil.copy2(seed_db, db)
            SESS[sid] = st
        st['touched'] = now
        st['active'] += 1
        return st, SESSIONS_ROOT / sid / 'warehouse.db', SESSIONS_ROOT / sid / 'raw'


def allowed(ip):
    now = time.time()
    RATE[ip] = [t for t in RATE[ip] if now - t < 3600]
    if len(RATE[ip]) >= RATE_LIMIT:
        return False
    RATE[ip].append(now)
    return True


def metrics(db, city='', category='', date_from='', date_to=''):
    con = sqlite3.connect(f'file:{Path(db).resolve()}?mode=ro', uri=True)
    con.row_factory = sqlite3.Row
    con.execute('PRAGMA query_only=ON')
    if hasattr(con, 'setlimit'):
        con.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, 1024 * 1024)
        con.setlimit(sqlite3.SQLITE_LIMIT_SQL_LENGTH, 20000)
    try:
        conditions, values = [], []
        for key, value in [('city', city), ('category', category)]:
            if value:
                conditions.append(key + '=?')
                values.append(value)
        if date_from:
            conditions.append('order_date>=?')
            values.append(date_from)
        if date_to:
            conditions.append('order_date<=?')
            values.append(date_to)
        where = (' WHERE ' + ' AND '.join(conditions)) if conditions else ''
        rows = [dict(r) for r in con.execute('SELECT * FROM mart_order' + where, values)]
        grouped = {}
        for field in ('city', 'category'):
            grouped[field] = [dict(r) for r in con.execute(
                f'SELECT {field} AS name, count(*) AS orders, sum(net_paise) AS net_paise '
                f'FROM mart_order{where} GROUP BY {field} ORDER BY net_paise DESC', values)]
        manifest = con.execute('SELECT count(*), max(loaded_at) FROM load_manifest').fetchone()
        return {'orders': len(rows), 'net_paise': sum(r['net_paise'] for r in rows),
                'delivered': sum(r['delivered'] for r in rows), 'late': sum(r['late'] for r in rows),
                'quarantined': con.execute('SELECT count(*) FROM quarantine').fetchone()[0],
                'batches': manifest[0], 'last_load': manifest[1],
                'quality': quality(con),
                'cities': [r[0] for r in con.execute('SELECT DISTINCT city FROM dim_customer ORDER BY city')],
                'categories': [r[0] for r in con.execute('SELECT DISTINCT category FROM dim_product ORDER BY category')],
                'by_city': grouped['city'], 'by_category': grouped['category'],
                'daily': [dict(r) for r in con.execute(
                    'SELECT order_date,count(*) AS orders,sum(net_paise) AS net_paise FROM mart_order'
                    + where + ' GROUP BY order_date ORDER BY order_date', values)]}
    finally:
        con.close()


def list_orders(db, page, status, needle, city, category):
    con = sqlite3.connect(f'file:{Path(db).resolve()}?mode=ro', uri=True)
    con.row_factory = sqlite3.Row
    con.execute('PRAGMA query_only=ON')
    if hasattr(con, 'setlimit'):
        con.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, 1024 * 1024)
        con.setlimit(sqlite3.SQLITE_LIMIT_SQL_LENGTH, 20000)
    try:
        conditions, values = [], []
        if status == 'delivered':
            conditions.append('delivered=1 AND late=0')
        elif status == 'late':
            conditions.append('late=1')
        elif status == 'transit':
            conditions.append('delivered=0')
        if needle:
            conditions.append('order_id LIKE ?')
            values.append(needle.replace('%', '') + '%')
        for key, value in [('city', city), ('category', category)]:
            if value:
                conditions.append(key + '=?')
                values.append(value)
        where = (' WHERE ' + ' AND '.join(conditions)) if conditions else ''
        total = con.execute('SELECT count(*) FROM mart_order' + where, values).fetchone()[0]
        rows = [dict(r) for r in con.execute(
            'SELECT * FROM mart_order' + where + ' ORDER BY order_date DESC, order_id LIMIT ? OFFSET ?',
            values + [25, page * 25])]
        return {'total': total, 'rows': rows}
    finally:
        con.close()


def run_query(db, sql):
    cleaned = sql.strip().rstrip(';').strip()
    if not re.match(r'^(select|with)\b', cleaned, re.I):
        raise ValueError('Read-only console: queries must start with SELECT or WITH.')
    if ';' in cleaned:
        raise ValueError('One statement at a time.')
    con = sqlite3.connect(f'file:{Path(db).resolve()}?mode=ro', uri=True)
    con.row_factory = sqlite3.Row
    con.execute('PRAGMA query_only=ON')
    if hasattr(con, 'setlimit'):
        con.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, 1024 * 1024)
        con.setlimit(sqlite3.SQLITE_LIMIT_SQL_LENGTH, 20000)
    deadline = time.monotonic() + 2
    con.set_progress_handler(lambda: time.monotonic() > deadline, 1000)
    try:
        rows = []
        size = 0
        for row in con.execute(cleaned):
            item = dict(row)
            size += len(json.dumps(item, default=str))
            if size > 1024 * 1024 or len(rows) >= 1001:
                break
            rows.append(item)
        return {'rows': rows[:1000], 'capped': len(rows) > 1000}
    finally:
        con.close()


def sample_batch(con, kind):
    """Small downloadable batch built from live dimensions so uploads exercise real paths."""
    customers = [r[0] for r in con.execute('SELECT customer_id FROM dim_customer LIMIT 5')]
    products = [r[0] for r in con.execute('SELECT product_id FROM dim_product LIMIT 3')]
    stamp = time.strftime('%Y%m%d%H%M%S')
    if kind == 'orders':
        rows = []
        for i in range(8):
            rows.append({'order_id': f'WEB-{stamp}-{i:02}', 'customer_id': customers[i % len(customers)],
                         'product_id': products[i % len(products)], 'order_date': '2026-10-29',
                         'quantity': 1 + i % 3, 'gross_paise': 25000 * (i + 1),
                         'discount_paise': 1000 * i, 'updated_at': '2026-10-29T12:00:00Z'})
        return f'orders-web-{stamp}.jsonl', '\n'.join(json.dumps(r) for r in rows) + '\n'
    if kind == 'messy':
        rows = [
            {'order_id': f'WEB-{stamp}-OK', 'customer_id': customers[0], 'product_id': products[0],
             'order_date': '2026-10-29', 'quantity': 2, 'gross_paise': 50000, 'discount_paise': 5000,
             'updated_at': '2026-10-29T12:00:00Z'},
            {'order_id': f'WEB-{stamp}-NEG', 'customer_id': customers[0], 'product_id': products[0],
             'order_date': '2026-10-29', 'quantity': -1, 'gross_paise': 50000, 'discount_paise': 0,
             'updated_at': '2026-10-29T12:00:00Z'},
            {'order_id': f'WEB-{stamp}-ORPHAN', 'customer_id': 'C9999', 'product_id': products[0],
             'order_date': '2026-10-29', 'quantity': 1, 'gross_paise': 10000, 'discount_paise': 0,
             'updated_at': '2026-10-29T12:00:00Z'},
            {'order_id': f'WEB-{stamp}-DATE', 'customer_id': customers[1], 'product_id': products[1],
             'order_date': '2026-02-30', 'quantity': 1, 'gross_paise': 10000, 'discount_paise': 0,
             'updated_at': '2026-10-29T12:00:00Z'}]
        return f'orders-messy-{stamp}.jsonl', '\n'.join(json.dumps(r) for r in rows) + '\n{broken json\n'
    # shipments sample: reference the three newest orders
    order_ids = [r[0] for r in con.execute('SELECT order_id FROM fact_order ORDER BY order_date DESC, order_id DESC LIMIT 3')]
    lines = ['order_id,promised_date,delivered_date,updated_at']
    for i, oid in enumerate(order_ids):
        delivered = '2026-11-04' if i else ''
        lines.append(f'{oid},2026-11-03,{delivered},2026-11-05T12:00:00Z')
    return f'shipments-web-{stamp}.csv', '\n'.join(lines) + '\n'


def pipeline_job(raw, db, job, lock):
    job['state'] = 'running'
    job['log'] = []

    def log(kind, message):
        job['log'].append({'t': time.strftime('%H:%M:%S'), 'c': kind, 'm': message})

    try:
        log('', 'Scanning this visitor workspace')
        files = [(kind, p) for kind in ['customers', 'products', 'orders', 'shipments']
                 for p in sorted(Path(raw).glob(kind + '*')) if p.suffix in ('.csv', '.jsonl')]
        if not files:
            log('warn', 'No source batches found.')
        with lock:
            con = connect(db)
            try:
                for kind, path in files:
                    result = ingest(con, path, kind)
                    if result['skipped']:
                        log('', f"{result['source']}: skipped (identical SHA-256, replay guard)")
                    else:
                        level = 'warn' if result['rejected'] else 'good'
                        log(level, f"{result['source']}: {result['accepted']} accepted, {result['rejected']} quarantined")
                log('', 'Running quality suite...')
                checks = quality(con)
                for name, passed in checks.items():
                    log('good' if passed else 'err', f"check {name}: {'PASS' if passed else 'FAIL'}")
            finally:
                con.close()
        orders = sum(1 for k, _ in files if k == 'orders')
        log('good', f'Done. {len(files)} batch files scanned ({orders} order batches).')
        job['state'] = 'done'
    except Exception as exc:  # surfaced in the UI log; warehouse stays consistent (atomic loads)
        log('err', 'Pipeline failed: ' + str(exc))
        job['state'] = 'error'


def make_handler(seed_db, seed_raw):
    global SESSIONS_ROOT
    SESSIONS_ROOT = Path(seed_db).parent / 'visitor-workspaces'
    SESSIONS_ROOT.mkdir(parents=True, exist_ok=True)
    for old in SESSIONS_ROOT.iterdir():
        if old.is_dir() and SID_RE.fullmatch(old.name):
            shutil.rmtree(old, ignore_errors=True)
    class Handler(BaseHTTPRequestHandler):
        server_version = 'Parcelpath/2.0'

        def end_headers(self):
            if getattr(self, 'new_sid', ''):
                self.send_header('Set-Cookie', session_cookie(self.new_sid))
            self.send_header('Referrer-Policy', 'no-referrer')
            self.send_header('X-Frame-Options', 'DENY')
            super().end_headers()

        def prepare_workspace(self, create=False):
            sid = sid_from_headers(self.headers)
            if not sid and not create:
                self.send_json({'error': 'Open the app first to start your demo workspace.'}, 403)
                return None
            if not sid:
                sid = secrets.token_urlsafe(24)
                self.new_sid = sid
            space = workspace_for(sid, seed_db, seed_raw)
            space[0]['lock'].acquire()
            return space

        def log_message(self, *args):
            pass

        def send_json(self, payload, status=200):
            body = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Cache-Control', 'no-store')
            self.send_header('X-Content-Type-Options', 'nosniff')
            self.send_header('X-Robots-Tag', 'noindex')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def send_html(self, html):
            body = html.encode()
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.send_header('Cache-Control', 'no-store')
            self.send_header('X-Content-Type-Options', 'nosniff')
            self.send_header('X-Robots-Tag', 'noindex')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def guard_mutation(self, st):
            now = time.time()
            st['actions'] = [t for t in st['actions'] if now - t < 3600]
            if len(st['actions']) >= RATE_LIMIT:
                self.send_json({'error': 'Action limit reached. Try again later.'}, 429)
                return False
            st['actions'].append(now)
            return True

        def do_GET(self):
            path = urlparse(self.path).path
            q = parse_qs(urlparse(self.path).query)
            first = lambda key, default='': q.get(key, [default])[0]
            st = None
            try:
                if path == '/healthz':
                    return self.send_json({'ok': True})
                if path != '/' and not path.startswith('/api/'):
                    return self.send_error(404)
                space = self.prepare_workspace(create=True)
                if space is None:
                    return
                st, db, raw = space
                if path == '/':
                    return self.send_html(DASHBOARD)
                if path == '/api/metrics':
                    return self.send_json(metrics(db, first('city'), first('category'), first('from'), first('to')))
                if path == '/api/orders':
                    return self.send_json(list_orders(db, max(0, int(first('page', '0') or 0)),
                                                      first('status'), first('q'), first('city'), first('category')))
                if path == '/api/quarantine':
                    con = connect(db)
                    try:
                        rows = [dict(r) for r in con.execute(
                            'SELECT source,line,reason,payload FROM quarantine ORDER BY source,line LIMIT 500')]
                    finally:
                        con.close()
                    return self.send_json({'rows': rows})
                if path == '/api/manifest':
                    con = connect(db)
                    try:
                        rows = [dict(r) for r in con.execute(
                            'SELECT source,accepted,rejected,loaded_at,sha256 FROM load_manifest ORDER BY loaded_at DESC LIMIT 200')]
                    finally:
                        con.close()
                    return self.send_json({'rows': rows})
                if path == '/api/quality':
                    con = connect(db)
                    try:
                        return self.send_json({'checks': quality(con)})
                    finally:
                        con.close()
                if path == '/api/pipeline/status':
                    return self.send_json(st['job'])
                if path == '/api/sample':
                    kind = first('kind')
                    if kind not in ('orders', 'shipments', 'messy'):
                        return self.send_json({'error': 'unknown sample kind'}, 400)
                    con = connect(db)
                    try:
                        name, content = sample_batch(con, kind)
                    finally:
                        con.close()
                    body = content.encode()
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/octet-stream')
                    self.send_header('Content-Disposition', f'attachment; filename="{name}"')
                    self.send_header('Content-Length', str(len(body)))
                    self.end_headers()
                    return self.wfile.write(body)
                self.send_error(404)
            except sqlite3.Error:
                self.send_json({'error': 'Warehouse unavailable - run the pipeline first.'}, 503)
            except (ValueError, KeyError) as exc:
                self.send_json({'error': str(exc)}, 400)
            finally:
                if st is not None:
                    st['lock'].release()
                    with SESS_LOCK:
                        st['active'] -= 1

        def do_POST(self):
            path = urlparse(self.path).path
            q = parse_qs(urlparse(self.path).query)
            st = None
            try:
                origin = self.headers.get('Origin')
                if origin and urlparse(origin).netloc != self.headers.get('Host'):
                    return self.send_json({'error': 'Cross-origin requests are not accepted.'}, 403)
                if path not in ('/api/pipeline/run', '/api/reset', '/api/upload', '/api/query'):
                    return self.send_error(404)
                space = self.prepare_workspace()
                if space is None:
                    return
                st, db, raw = space
                if not self.guard_mutation(st):
                    return
                if path == '/api/pipeline/run':
                    if st['job']['state'] == 'running':
                        return self.send_json({'error': 'Pipeline is already running.'}, 409)
                    st['job']['state'] = 'running'
                    threading.Thread(target=pipeline_job, args=(raw, db, st['job'], st['lock']), daemon=True).start()
                    return self.send_json({'started': True})
                if path == '/api/reset':
                    if st['job']['state'] == 'running':
                        return self.send_json({'error': 'Pipeline is running - wait for it to finish.'}, 409)
                    with st['lock']:
                        for p in Path(raw).glob('*'):
                            if p.suffix in ('.csv', '.jsonl'):
                                p.unlink()
                        Path(db).unlink(missing_ok=True)
                        generate(raw)
                        load_all(raw, db)
                    return self.send_json({'reset': True})
                if path == '/api/upload':
                    filename = q.get('filename', [''])[0]
                    match = BATCH_NAME.match(filename)
                    if not match:
                        return self.send_json({'error': 'File name must look like orders-007.jsonl, shipments-002.csv, customers-*.csv or products-*.csv'}, 400)
                    length = int(self.headers.get('Content-Length') or 0)
                    if length <= 0 or length > MAX_UPLOAD:
                        return self.send_json({'error': 'Empty file or over the 1 MB cap.'}, 400)
                    body = self.rfile.read(length)
                    target = Path(raw) / filename
                    if len(list(Path(raw).glob('*'))) >= MAX_BATCHES and not target.exists():
                        return self.send_json({'error': 'Demo batch folder is full - reset the demo dataset first.'}, 400)
                    with st['lock']:
                        if sum(f.stat().st_size for f in Path(raw).glob('*') if f.is_file()) + length > 5 * MAX_UPLOAD:
                            return self.send_json({'error': 'Workspace upload cap reached. Reset this demo first.'}, 400)
                        if target.exists() and target.read_bytes() != body:
                            return self.send_json({'error': 'Batch name already exists with different content. Use a new batch name.'}, 409)
                        target.write_bytes(body)
                        con = connect(db)
                        try:
                            result = ingest(con, target, match.group(1))
                            quar = [] if result['skipped'] else [dict(r) for r in con.execute(
                                'SELECT line,reason FROM quarantine WHERE source=? ORDER BY line LIMIT 20',
                                (result['source'],))]
                        finally:
                            con.close()
                    result['quarantine'] = quar
                    return self.send_json(result)
                if path == '/api/query':
                    length = int(self.headers.get('Content-Length') or 0)
                    if length <= 0 or length > 20000:
                        return self.send_json({'error': 'Missing or oversized query.'}, 400)
                    sql = json.loads(self.rfile.read(length))['sql']
                    if not isinstance(sql, str):
                        raise ValueError('SQL must be text.')
                    return self.send_json(run_query(db, sql))
                self.send_error(404)
            except sqlite3.Error as exc:
                self.send_json({'error': 'Warehouse request failed. Check the batch or SQL and try again.'}, 503)
            except (ValueError, KeyError, TypeError, UnicodeError) as exc:
                self.send_json({'error': str(exc)}, 400)
            finally:
                if st is not None:
                    st['lock'].release()
                    with SESS_LOCK:
                        st['active'] -= 1

    return Handler


def serve():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port', type=int, default=8000)
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--db', type=Path, default=ROOT / 'data/warehouse/retail.db')
    parser.add_argument('--raw', type=Path, default=ROOT / 'data/raw')
    args = parser.parse_args(__import__('sys').argv[2:])
    # Cold-start bootstrap: an empty warehouse rebuilds itself from the deterministic seed.
    if not args.db.exists():
        if not any(Path(args.raw).glob('*')):
            generate(args.raw)
        load_all(args.raw, args.db)
    print(f'Parcelpath: http://{args.host}:{args.port} (Ctrl+C to stop)', flush=True)
    ThreadingHTTPServer((args.host, args.port), make_handler(args.db, args.raw)).serve_forever()


if __name__ == '__main__':
    import sys
    serve() if len(sys.argv) > 1 and sys.argv[1] == 'serve' else main()
