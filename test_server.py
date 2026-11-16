import json
import tempfile
import urllib.error
import urllib.request


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None
import unittest
from pathlib import Path
from parcelpath import (BATCH_NAME, generate, load_all, list_orders, metrics,
                        run_query, sample_batch, connect)


class ServerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.raw = self.root / 'raw'
        self.db = self.root / 'warehouse.db'
        generate(self.raw, 120)
        load_all(self.raw, self.db)

    def tearDown(self):
        self.tmp.cleanup()

    def test_metrics_shape_and_filters(self):
        m = metrics(self.db)
        self.assertEqual(m['orders'], 120)
        self.assertEqual(m['quarantined'], 4)
        self.assertTrue(all(m['quality'].values()))
        self.assertEqual(sum(r['orders'] for r in m['daily']), 120)
        self.assertEqual(sum(r['net_paise'] for r in m['by_city']), m['net_paise'])
        city = m['cities'][0]
        filtered = metrics(self.db, city=city)
        self.assertLess(filtered['orders'], m['orders'])
        self.assertTrue(all(r['city'] == city for r in
                            list_orders(self.db, 0, '', '', city, '')['rows']))

    def test_metrics_date_window(self):
        m = metrics(self.db, date_from='2026-10-01', date_to='2026-10-31')
        self.assertGreater(m['orders'], 0)
        self.assertLess(m['orders'], 120)
        self.assertTrue(all('2026-10' == r['order_date'][:7] for r in m['daily']))

    def test_list_orders_pagination_and_status(self):
        page = list_orders(self.db, 0, '', '', '', '')
        self.assertEqual(page['total'], 120)
        self.assertLessEqual(len(page['rows']), 25)
        late = list_orders(self.db, 0, 'late', '', '', '')
        self.assertTrue(all(r['late'] == 1 for r in late['rows']))
        transit = list_orders(self.db, 0, 'transit', '', '', '')
        self.assertTrue(all(r['delivered'] == 0 for r in transit['rows']))
        needle = list_orders(self.db, 0, '', page['rows'][0]['order_id'], '', '')
        self.assertEqual(needle['total'], 1)

    def test_query_guard(self):
        result = run_query(self.db, 'SELECT count(*) AS n FROM mart_order')
        self.assertEqual(result['rows'][0]['n'], 120)
        with self.assertRaises(ValueError):
            run_query(self.db, 'DELETE FROM fact_order')
        with self.assertRaises(ValueError):
            run_query(self.db, 'SELECT 1; DROP TABLE fact_order')
        capped = run_query(self.db, 'SELECT * FROM mart_order LIMIT 1200')
        self.assertFalse(capped['capped'])
        self.assertEqual(len(capped['rows']), 120)

    def test_upload_filename_pattern(self):
        for good in ('orders-002.jsonl', 'shipments-12.csv', 'customers.csv', 'products-new.csv'):
            self.assertIsNotNone(BATCH_NAME.match(good), good)
        for bad in ('evil.txt', '../orders.jsonl', 'manifest.jsonl', 'orders', '.csv'):
            self.assertIsNone(BATCH_NAME.match(bad), bad)

    def test_sample_batches_exercise_real_paths(self):
        con = connect(self.db)
        try:
            name, content = sample_batch(con, 'orders')
            target = self.raw / name
            target.write_text(content)
            from parcelpath import ingest
            result = ingest(con, target, 'orders')
            self.assertEqual((result['accepted'], result['rejected']), (8, 0))
            name, content = sample_batch(con, 'messy')
            target = self.raw / name
            target.write_text(content)
            result = ingest(con, target, 'orders')
            self.assertEqual(result['accepted'], 1)
            self.assertGreaterEqual(result['rejected'], 3)
        finally:
            con.close()


class PublicTests(unittest.TestCase):
    def setUp(self):
        import threading, parcelpath as pp
        from http.server import ThreadingHTTPServer
        self.pp = pp
        pp.SESS.clear()
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.raw, self.db = self.root / 'raw', self.root / 'seed.db'
        generate(self.raw, 50)
        load_all(self.raw, self.db)
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), pp.make_handler(self.db, self.raw))
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.base = 'http://127.0.0.1:' + str(self.server.server_address[1])

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.pp.SESS.clear()
        self.tmp.cleanup()

    def get(self, path, cookie=''):
        r = urllib.request.urlopen(urllib.request.Request(self.base + path, headers={'Cookie': cookie}))
        return r.read(), r.headers.get('Set-Cookie', '')

    def post(self, path, cookie='', body=b'', headers=None):
        h = {'Cookie': cookie}
        h.update(headers or {})
        return json.loads(urllib.request.urlopen(urllib.request.Request(self.base + path, data=body, headers=h)).read())

    def test_public_dashboard_cookie_and_forgery(self):
        html, cookie = self.get('/')
        self.assertIn(b'Your own temporary demo workspace', html)
        self.assertIn('HttpOnly', cookie)
        self.assertIn('SameSite=Strict', cookie)
        a = self.pp.sid_from_headers({'Cookie': cookie})
        self.assertTrue(a)
        _, forged = self.get('/', 'pp_sid=' + a + '.forged')
        self.assertNotEqual(self.pp.sid_from_headers({'Cookie': forged}), a)

    def test_two_visitors_upload_and_reset_isolation(self):
        _, a = self.get('/')
        _, b = self.get('/')
        sample, _ = self.get('/api/sample?kind=orders', a)
        first = self.post('/api/upload?filename=orders-test.jsonl', a, sample)
        self.assertEqual(first['accepted'], 8)
        self.assertTrue(self.post('/api/upload?filename=orders-test.jsonl', a, sample)['skipped'])
        self.assertEqual(json.loads(self.get('/api/metrics', a)[0])['orders'], 58)
        self.assertEqual(json.loads(self.get('/api/metrics', b)[0])['orders'], 50)
        self.post('/api/reset', b)
        self.assertEqual(json.loads(self.get('/api/metrics', a)[0])['orders'], 58)
        self.assertEqual(metrics(self.db)['orders'], 50)

    def test_no_session_and_cross_origin_blocked(self):
        with self.assertRaises(urllib.error.HTTPError) as err:
            self.post('/api/reset')
        self.assertEqual(err.exception.code, 403)
        _, a = self.get('/')
        with self.assertRaises(urllib.error.HTTPError) as err:
            self.post('/api/reset', a, headers={'Origin': 'https://outside.invalid'})
        self.assertEqual(err.exception.code, 403)

    def test_query_and_upload_guards(self):
        _, a = self.get('/')
        self.assertEqual(self.post('/api/query', a, json.dumps({'sql':'SELECT count(*) n FROM fact_order'}).encode())['rows'][0]['n'], 50)
        for path, body in [('/api/query', b'{"sql":"DELETE FROM fact_order"}'),
                           ('/api/upload?filename=../orders.jsonl', b'{}')]:
            with self.assertRaises(urllib.error.HTTPError):
                self.post(path, a, body)

    def test_pipeline_status_isolation(self):
        import time
        _, a = self.get('/')
        _, b = self.get('/')
        self.post('/api/pipeline/run', a)
        for _ in range(100):
            status = json.loads(self.get('/api/pipeline/status', a)[0])
            if status['state'] != 'running':
                break
            time.sleep(.01)
        self.assertEqual(status['state'], 'done')
        self.assertTrue(status['log'])
        self.assertEqual(json.loads(self.get('/api/pipeline/status', b)[0])['state'], 'idle')

    def test_capacity_and_idle_cleanup(self):
        _, a = self.get('/')
        sid = self.pp.sid_from_headers({'Cookie': a})
        self.pp.SESS[sid]['touched'] = 0
        self.get('/')
        self.assertNotIn(sid, self.pp.SESS)
        self.assertFalse((self.pp.SESSIONS_ROOT / sid).exists())


if __name__ == '__main__':
    unittest.main()
