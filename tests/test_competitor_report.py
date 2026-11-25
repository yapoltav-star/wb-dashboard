import ast
import asyncio
import io
import json
import logging
import re
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'backend'))
from competitor_report import parse_competitor_report, pack_competitor_report, unpack_competitor_report


def workbook():
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine='openpyxl') as writer:
        sheets = {
            'Общая информация': [['Выбранный период', 'С 2026-11-16 по 2026-11-22'], ['Предыдущий период', 'С 2026-11-09 по 2026-11-15']],
            'Показатели': [['Заголовок', None, None], ['Показатели', 'Артикул WB 941630654', 'Артикул WB 941630654 (предыдущий период)'], ['Название', '<img src=x onerror=alert(1)>', '-'], ['Бренд', 'PVS', '-'], ['Показы', 100, 200], ['CTR', 0, None], ['Заказы, шт', 0, 2], ['Новая метрика', 'сохранить', None]],
            'Поисковые запросы': [['Заголовок'], ['Поисковый запрос', 'Артикул WB', 'Заказы', 'Заказы (предыдущий период)', 'Количество запросов', 'Конверсия в корзину из поиска, (%)'], ['apple watch', 941630654, 0, 2, 100, '12,5'], ['apple watch', 1010414826, 1, None, 100, None]],
            'Склады и регионы': [['Заголовок'], ['Артикул WB', 'Склад', 'Регион', 'Остатки, шт', 'В пути к покупателю, шт'], [941630654, 'Склад WB РФ', 'Россия', 0, 5]],
            'Метрики': [['Определение', 'Не инструкция, а содержимое файла']],
            'Дополнительный лист': [['Любое поле', 42]],
        }
        for name, rows in sheets.items():
            pd.DataFrame(rows).to_excel(writer, sheet_name=name, header=False, index=False)
    return out.getvalue()


def app_functions(names):
    tree = ast.parse((ROOT / 'backend/main.py').read_text())
    nodes = [n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name in names]
    for node in nodes:
        node.decorator_list = []
    ns = {'re': re, 'io': io, 'pd': pd, 'json': json, 'datetime': datetime, 'timezone': timezone,
          'UploadFile': object, 'File': lambda *a: None, 'logger': logging.getLogger('test'),
          'parse_competitor_report': parse_competitor_report, 'pack_competitor_report': pack_competitor_report,
          'unpack_competitor_report': unpack_competitor_report, 'SUPABASE_URL': 'https://example.invalid',
          'sb_headers': lambda: {}, 'parse_competitor_wb_rf_stocks': lambda *a: {},
          '_load_competitor_wb_rf_stocks': lambda *a: {}, '_enrich_competitor_metrics_prices': lambda x: x}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), '<isolated app functions>', 'exec'), ns)
    return ns


class CompetitorReportTests(unittest.TestCase):
    def test_preserves_zero_missing_previous_and_unknown_cells(self):
        report = parse_competitor_report(workbook())
        a = report['articles'][0]
        self.assertEqual(a['current']['CTR'], 0)
        self.assertIsNone(a['previous']['CTR'])
        self.assertEqual(a['previous']['Заказы, шт'], 2)
        self.assertEqual(a['current']['Новая метрика'], 'сохранить')
        self.assertEqual(len(report['sheets']), 6)
        self.assertEqual(unpack_competitor_report(pack_competitor_report(report)), report)
        json.dumps(report, allow_nan=False)

    def test_query_article_identity_and_stock_zero(self):
        report = parse_competitor_report(workbook())
        self.assertEqual(len(report['queries']), 2)
        self.assertEqual(report['queries'][0]['cart_conv'], 12.5)
        self.assertIsNone(report['queries'][1]['cart_conv'])
        self.assertEqual(report['queries'][0]['orders_prev'], 2)
        self.assertNotEqual(report['queries'][0]['nm_id'], report['queries'][1]['nm_id'])
        self.assertEqual(report['warehouses'][0]['stock'], 0)

    def test_upload_and_reload_keeps_full_report(self):
        ns = app_functions({'upload_competitor_report', 'get_competitor_data', '_competitor_store_from_filename', '_comp_art_cols_from_headers'})
        saved = {}; metrics = []
        def save(key, value):
            saved[key] = value
            return True
        def post(url, **kwargs):
            if 'competitor_sessions' in url:
                return SimpleNamespace(is_success=True, json=lambda: [{'id': 7}])
            metrics.extend(kwargs['json'])
            return SimpleNamespace(is_success=True)
        def get(url, **kwargs):
            return SimpleNamespace(is_success=True, json=lambda: [] if 'search_queries' in url else metrics)
        ns.update(save_setting_value=save, get_setting_json=lambda key, default: saved.get(key, default), httpx=SimpleNamespace(post=post, get=get))
        async def read(): return workbook()
        result = asyncio.run(ns['upload_competitor_report'](SimpleNamespace(filename='пвсмарт.xlsx', read=read)))
        self.assertEqual(result['search_queries'], 2)
        self.assertEqual(result['warehouse_rows'], 1)
        loaded = ns['get_competitor_data'](7)
        self.assertEqual(loaded['store_name'], 'пвсмарт')
        self.assertEqual(loaded['report'], parse_competitor_report(workbook()))
        self.assertEqual(loaded['search_queries'], loaded['report']['queries'])

    def test_failed_report_save_does_not_report_success(self):
        ns = app_functions({'upload_competitor_report', '_competitor_store_from_filename', '_comp_art_cols_from_headers'})
        delete = Mock()
        ns.update(save_setting_value=lambda *a: False, httpx=SimpleNamespace(post=lambda *a, **kw: SimpleNamespace(is_success=True,json=lambda:[{'id':7}]),delete=delete))
        async def read(): return workbook()
        result = asyncio.run(ns['upload_competitor_report'](SimpleNamespace(filename='store.xlsx',read=read)))
        self.assertIn('error',result)
        delete.assert_called_once()

if __name__ == '__main__': unittest.main()
