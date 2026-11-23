import copy
import json
import logging
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'backend'))
from boost_monitor import Monitor, observation, compare, snapshot_key, load_config


class Store:
    def __init__(self):
        self.rows = {}
        self.fail_read = False
        self.fail_write = False
    def get(self, key):
        if self.fail_read:
            raise OSError('unavailable')
        return copy.deepcopy(self.rows.get(key))
    def put(self, key, value):
        if self.fail_write:
            raise OSError('unavailable')
        self.rows[key] = copy.deepcopy(value)
    def between(self, start, end):
        return [copy.deepcopy(v) for v in self.rows.values() if start <= v['day'] <= end]


class MonitoringTests(unittest.TestCase):
    def setUp(self):
        self.config = load_config()
        self.search = next(g for g in self.config['groups'] if g['mode'] == 'search')
        self.shelf = next(g for g in self.config['groups'] if g['mode'] == 'shelf')
        self.config['groups'] = [self.search, self.shelf]
        self.now = datetime(2026, 11, 23, 12, tzinfo=timezone.utc)
        self.store = Store()
        self.calls = []
        def search(*a, **kw):
            self.calls.append('search')
            return {'products': [{'id': self.search['targets'][0]['nm_id']}, {'id': 999}], 'source': 'napi', 'error': None}
        def shelf(*a, **kw):
            self.calls.append('shelf')
            return {'own_on_shelf': [{'nm_id': self.shelf['targets'][0]['nm_id'], 'position': 42}], 'own_scan_depth': 100, 'source': 'recom'}
        self.env = {'fetch_wb_search_catalog': search, 'fetch_wb_see_also_shelf': shelf, 'logger': logging.getLogger('test')}
    def monitor(self):
        return Monitor(lambda: self.env, self.store, self.config, lambda: self.now)
    def test_restart_and_manual_run_do_not_replace_success(self):
        self.monitor().run()
        prior = copy.deepcopy(self.store.rows)
        self.monitor().run(force=True)
        self.assertEqual(self.calls, ['search', 'shelf'])
        self.assertEqual(self.store.rows, prior)
    def test_failed_groups_resume_after_restart(self):
        shelf = self.env['fetch_wb_see_also_shelf']
        self.env['fetch_wb_see_also_shelf'] = lambda *a, **kw: {'error': '403'}
        self.monitor().run()
        self.assertFalse(self.store.rows[snapshot_key('2026-11-23', self.shelf['id'])]['ok'])
        self.env['fetch_wb_see_also_shelf'] = shelf
        self.monitor().run()
        snap = self.store.rows[snapshot_key('2026-11-23', self.shelf['id'])]
        self.assertTrue(snap['ok'])
        self.assertEqual(snap['attempts'], 2)
        self.assertEqual(self.calls.count('search'), 1)
    def test_storage_read_failure_does_not_overwrite_or_fetch(self):
        self.monitor().run()
        prior = copy.deepcopy(self.store.rows)
        self.calls.clear()
        self.store.fail_read = True
        self.monitor().run()
        self.assertEqual(self.calls, [])
        self.assertEqual(prior, self.store.rows)
    def test_storage_write_failure_not_counted(self):
        self.store.fail_write = True
        monitor = self.monitor()
        monitor.run()
        self.assertEqual(self.store.rows, {})
        self.assertEqual(monitor.health['processed'], 0)
        self.assertIn('error', monitor.health)
    def test_partial_search_error_is_not_absence(self):
        out = observation(self.search, {'products':[{'id':self.search['targets'][0]['nm_id']}], 'error':'429 page 2', 'source':'napi'}, self.config, 'now')
        self.assertFalse(out['ok'])
        self.assertTrue(all(r['status']=='error' for r in out['rows']))
    def test_empty_response_is_error(self):
        out = observation(self.search, {'products':[], 'source':'napi'}, self.config, 'now')
        self.assertFalse(out['ok'])
    def test_shelf_rank_is_full_depth_not_top_15(self):
        self.monitor().run()
        snap = self.store.rows[snapshot_key('2026-11-23', self.shelf['id'])]
        self.assertEqual(snap['rows'][0]['position'], 42)
        self.assertEqual(snap['rows'][0]['checked'], 100)
    def test_missing_day_is_not_substituted(self):
        monitor = self.monitor();monitor.run()
        report = monitor.report('2026-11-16','2026-11-23')
        self.assertTrue(all(r['before'] is None for r in report['rows']))
        self.assertEqual(report['coverage']['2026-11-16']['missing'], sum(len(g['targets']) for g in self.config['groups']))
    def test_different_shelf_or_search_source_not_compared(self):
        a = {'status':'found','position':40,'dest':-1,'depth':100,'source':'recom'}
        b = {**a,'position':2,'source':'similar'}
        self.assertIsNone(compare(a,b)['delta'])
        b['source']='recom'
        self.assertEqual(compare(a,b)['delta'],38)
        b.update(status='not_found',position=None)
        self.assertIsNone(compare(a,b)['delta'])
    def test_schedule_uses_moscow_day_and_catches_up(self):
        self.now = datetime(2026,11,23,22,tzinfo=timezone.utc)
        self.monitor().run() # 01:00 Moscow: scheduled collection waits until 10:00.
        self.assertEqual(self.calls, [])
        self.monitor().run(force=True)
        self.assertTrue(all(v['day']=='2026-11-24' for v in self.store.rows.values()))
    def test_seed_report_deduplicates_and_excludes_bad_queries(self):
        cfg=load_config()
        targets=[t for g in cfg['groups'] for t in g['targets']]
        self.assertEqual(len({t['nm_id'] for t in targets}),34)
        self.assertEqual(len(targets),201)
        self.assertEqual(len(cfg['excluded']),32)
        self.assertEqual(sum(g['source_own'] for g in cfg['groups'] if g['mode']=='shelf'),5)
        for g in cfg['groups']:
            self.assertEqual(len(g['targets']),len({t['nm_id'] for t in g['targets']}))

if __name__ == '__main__':
    unittest.main()
