"""Run the real endpoint in isolation from external API credentials and startup jobs."""
import ast
from pathlib import Path
import unittest

SOURCE = Path(__file__).resolve().parents[1] / 'backend/main.py'

class CatalogLoadingTests(unittest.TestCase):
    def test_warm_catalog_returns_without_rebuilding(self):
        tree = ast.parse(SOURCE.read_text())
        endpoint = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'get_wb_products')
        endpoint.decorator_list = []
        calls = []
        cache = {'products': [{'nm_id': 123}], 'sales_by_nm': {'123': {}}, 'syncing': False}
        def slow_build(*args):
            calls.append('blocking rebuild')
            return {'products': cache['products']}
        class Thread:
            def __init__(self, **kwargs):
                pass
            def start(self):
                calls.append('background refresh')
        import types, threading, time
        scope = dict(WB_PRODUCTS_CACHE=cache, build_wb_products_catalog=slow_build,
                     threading=types.SimpleNamespace(Thread=Thread), time=time,
                     refresh_wb_products_catalog=lambda: None,
                     _WB_PRODUCTS_REFRESH_LOCK=threading.Lock())
        exec(compile(ast.Module(body=[endpoint], type_ignores=[]), str(SOURCE), 'exec'), scope)
        result = scope['get_wb_products'](refresh=True)
        self.assertEqual(result['products'], [{'nm_id': 123}])
        self.assertNotIn('blocking rebuild', calls, 'Opening Products waits for external catalog requests')

if __name__ == '__main__':
    unittest.main()
