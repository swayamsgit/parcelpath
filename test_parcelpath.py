import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from parcelpath import connect, generate, ingest, load_all, quality, export


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.raw = self.root/'raw'
        self.db = self.root/'warehouse.db'
        generate(self.raw, 100)
        load_all(self.raw, self.db)
        self.con = connect(self.db)

    def tearDown(self):
        self.con.close()
        self.tmp.cleanup()

    def order(self):
        return json.loads((self.raw/'orders-001.jsonl').read_text().splitlines()[0])

    def batch(self, name, rows):
        path = self.raw/name
        path.write_text('\n'.join(json.dumps(r) for r in rows))
        return ingest(self.con, path, 'orders')

    def test_row_counts_and_quarantine(self):
        self.assertEqual(self.con.execute('SELECT count(*) FROM fact_order').fetchone()[0], 100)
        self.assertEqual(self.con.execute('SELECT count(*) FROM quarantine').fetchone()[0], 4)
        self.assertTrue(all(quality(self.con).values()))

    def test_replay_is_idempotent(self):
        before = self.con.execute('SELECT sum(gross_paise) FROM fact_order').fetchone()[0]
        result = load_all(self.raw, self.db)
        self.assertTrue(all(r['skipped'] for r in result))
        self.assertEqual(self.con.execute('SELECT sum(gross_paise) FROM fact_order').fetchone()[0], before)

    def test_newer_update_applied_stale_ignored(self):
        row = self.order()
        newer = dict(row, gross_paise=888888, updated_at='2026-11-03T00:00:00Z')
        self.batch('orders-update.jsonl', [newer])
        self.batch('orders-old.jsonl', [dict(row, gross_paise=777777)])
        actual = self.con.execute('SELECT gross_paise FROM fact_order WHERE order_id=?', (row['order_id'],)).fetchone()[0]
        self.assertEqual(actual, 888888)

    def test_invalid_record_does_not_abort_good_record(self):
        row = self.order()
        result = self.batch('orders-extra.jsonl', [dict(row, order_id='new'), dict(row, order_id='bad', quantity=0)])
        self.assertEqual((result['accepted'], result['rejected']), (1, 1))
        self.assertIsNotNone(self.con.execute("SELECT 1 FROM fact_order WHERE order_id='new'").fetchone())

    def test_invalid_dates_and_money_and_orphans(self):
        row = self.order()
        rows = [dict(row, order_id='date', order_date='2026-02-30'), dict(row, order_id='decimal', gross_paise='10.5'), dict(row, order_id='orphan', product_id='missing')]
        self.assertEqual(self.batch('orders-invalid.jsonl', rows)['rejected'], 3)

    def test_export_reconciles(self):
        result = export(self.con, self.root/'out')
        self.assertEqual(sum(x['orders'] for x in result['slices']), 100)
        expected = self.con.execute('SELECT sum(gross_paise-discount_paise) FROM fact_order').fetchone()[0]
        self.assertEqual(sum(x['net_paise'] for x in result['slices']), expected)

    def test_empty_warehouse_quality_fails(self):
        con = connect(self.root/'empty.db')
        self.assertFalse(quality(con)['nonempty_orders'])
        con.close()

    def test_late_shipment_correction(self):
        row = self.con.execute("SELECT delivered_date,updated_at FROM fact_shipment WHERE order_id='O000000'").fetchone()
        self.assertIsNotNone(row[0])
        self.assertEqual(row[1], '2026-11-02T12:00:00Z')

    def test_file_failure_rolls_back_entire_batch(self):
        from unittest.mock import patch
        from parcelpath import load_row
        row = self.order()
        path = self.raw/'orders-crash.jsonl'
        path.write_text(json.dumps(dict(row,order_id='atomic-one'))+'\n'+json.dumps(dict(row,order_id='atomic-two')))
        calls = 0
        def crash(con, kind, record):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError('simulated process failure')
            return load_row(con,kind,record)
        with patch('parcelpath.load_row', side_effect=crash):
            with self.assertRaises(RuntimeError):
                ingest(self.con,path,'orders')
        self.assertIsNone(self.con.execute("SELECT 1 FROM fact_order WHERE order_id='atomic-one'").fetchone())
        self.assertIsNone(self.con.execute("SELECT 1 FROM load_manifest WHERE source='orders/orders-crash.jsonl'").fetchone())


if __name__ == '__main__':
    unittest.main()
