import ast
from datetime import date, timedelta
from pathlib import Path
import unittest

class ComparisonTests(unittest.TestCase):
    def test_completed_adjacent_windows(self):
        path = Path(__file__).resolve().parents[1] / 'backend/main.py'
        tree = ast.parse(path.read_text())
        function = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == '_catalog_comparison_add')
        scope = {}
        exec(compile(ast.Module(body=[function], type_ignores=[]), str(path), 'exec'), scope)
        today = date(2026, 1, 2)
        slot = {}
        for age in range(58):
            scope['_catalog_comparison_add'](slot, today-timedelta(days=age), today, age)
        for days in (1, 2, 3, 7, 28):
            with self.subTest(days=days):
                self.assertEqual(slot['comparisons'][str(days)], {
                    'current': sum(range(1, days+1)),
                    'previous': sum(range(days+1, 2*days+1)),
                })

if __name__ == '__main__':
    unittest.main()
