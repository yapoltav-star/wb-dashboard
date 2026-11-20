import ast
from datetime import datetime
from pathlib import Path
import threading
import time
import types
import unittest

class CustomPeriodsTests(unittest.TestCase):
    def setUp(self):
        source = Path(__file__).resolve().parents[1] / 'backend/main.py'
        tree = ast.parse(source.read_text())
        endpoint = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'get_catalog_custom_comparison')
        endpoint.decorator_list = []
        class HTTPError(Exception):
            def __init__(self, status, message):
                self.status = status
        self.jobs = []
        jobs = self.jobs
        class Thread:
            def __init__(self, **kwargs):
                self.args = kwargs['args']
            def start(self):
                jobs.append(self.args)
        self.scope = dict(datetime=datetime, time=time, WB_TOKEN='test',
            _msk_now=lambda: datetime(2026,11,20), HTTPException=HTTPError,
            _CATALOG_CUSTOM={}, _CATALOG_CUSTOM_LOCK=threading.Lock(),
            _catalog_custom_refresh=lambda key: None, threading=types.SimpleNamespace(Thread=Thread))
        exec(compile(ast.Module(body=[endpoint], type_ignores=[]), str(source), 'exec'), self.scope)
        self.endpoint = self.scope['get_catalog_custom_comparison']
        self.error = HTTPError

    def test_non_adjacent_weekdays_and_deduplicated_polling(self):
        dates = ('2026-11-09','2026-11-13','2026-10-26','2026-10-30')
        self.assertTrue(self.endpoint(*dates)['syncing'])
        self.assertTrue(self.endpoint(*dates)['syncing'])
        self.assertEqual(len(self.jobs), 1)
        result = {'syncing':False,'current':{'123':12},'previous':{'123':8}}
        self.scope['_CATALOG_CUSTOM'][dates] = (time.time(),result)
        self.assertEqual(self.endpoint(*dates), result)

    def test_invalid_or_future_dates_rejected(self):
        for dates in [('2026-11-13','2026-11-09','2026-10-26','2026-10-30'),
                      ('2026-11-21','2026-11-22','2026-10-26','2026-10-30'),
                      ('','2026-11-13','2026-10-26','2026-10-30')]:
            with self.assertRaises(self.error):
                self.endpoint(*dates)
        self.assertEqual(self.jobs, [])

if __name__ == '__main__':
    unittest.main()
