"""Lossless cell data plus structured views of WB comparison exports."""
import base64
import gzip
import io
import json
import math
import re
from datetime import date, datetime

import pandas as pd


def _cell(value):
    if value is None or (not isinstance(value, (list, dict)) and pd.isna(value)):
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if hasattr(value, 'item'):
        value = value.item()
    return value if isinstance(value, (str, int, float, bool)) else str(value)


def _number(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        n = float(str(value).replace('\xa0', '').replace(' ', '').replace(',', '.').rstrip('%'))
        return n if math.isfinite(n) else None
    except (ValueError, TypeError):
        return None


def _records(sheet, marker):
    rows = sheet['rows']
    header = next((i for i, row in enumerate(rows) if marker in row), None)
    if header is None:
        return []
    names = [str(v or f'Колонка {i + 1}') for i, v in enumerate(rows[header])]
    return [dict(zip(names, row)) for row in rows[header + 1:] if any(v is not None for v in row)]


def parse_competitor_report(contents):
    book = pd.read_excel(io.BytesIO(contents), sheet_name=None, header=None)
    sheets = [{'name': name, 'rows': [[_cell(v) for v in row] for row in frame.itertuples(index=False, name=None)]}
              for name, frame in book.items()]
    by_name = {s['name']: s for s in sheets}
    info = by_name.get('Общая информация', {}).get('rows', [])
    periods = {}
    for row in info:
        if len(row) > 1 and row[0] in ('Выбранный период', 'Предыдущий период'):
            periods['current' if row[0] == 'Выбранный период' else 'previous'] = str(row[1] or '')
    indicators = by_name.get('Показатели', {}).get('rows', [])
    if len(indicators) < 2:
        raise ValueError('В отчёте отсутствует лист Показатели с артикулами')
    articles = {}
    for col, value in enumerate(indicators[1]):
        header = str(value or '')
        match = re.search(r'артикул wb\s+(\d{7,12})', header, re.I)
        if not match or 'разница' in header.lower():
            continue
        nm = int(match[1])
        item = articles.setdefault(nm, {'nm_id': nm, 'current': {}, 'previous': {}})
        target = item['previous' if 'предыдущий' in header.lower() else 'current']
        for row in indicators[2:]:
            if row and row[0] is not None:
                target[str(row[0]).strip()] = row[col] if col < len(row) else None
    for item in articles.values():
        item['brand'] = item['current'].get('Бренд')
        item['name'] = item['current'].get('Название')
    queries = []
    fields = {'query_count': 'Количество запросов', 'avg_position': 'Средняя позиция по запросу',
              'card_opens': 'Переходы в карточку', 'cart_conv': 'Конверсия в корзину из поиска, (%)',
              'cart_adds': 'Положили в корзину', 'order_conv': 'Конверсия в заказ из поиска, (%)', 'orders': 'Заказы'}
    for row in _records(by_name.get('Поисковые запросы', {'rows': []}), 'Поисковый запрос'):
        nm = _number(row.get('Артикул WB'))
        if nm is None or not row.get('Поисковый запрос'):
            continue
        q = {'nm_id': int(nm), 'query': str(row['Поисковый запрос']), 'raw': row}
        for key, label in fields.items():
            q[key] = _number(row.get(label))
            q[key + '_prev'] = _number(row.get(label + ' (предыдущий период)'))
        queries.append(q)
    warehouses = []
    for row in _records(by_name.get('Склады и регионы', {'rows': []}), 'Артикул WB'):
        nm = _number(row.get('Артикул WB'))
        if nm is None:
            continue
        warehouses.append({'nm_id': int(nm), 'region': row.get('Регион'), 'warehouse': row.get('Склад'),
                           'stock': _number(row.get('Остатки, шт')), 'to_customer': _number(row.get('В пути к покупателю, шт')),
                           'from_customer': _number(row.get('В пути от покупателя, шт')),
                           'daily_orders': _number(row.get('Среднее количество заказов в день, шт')), 'raw': row})
    return {'version': 1, 'periods': periods, 'articles': list(articles.values()),
            'queries': queries, 'warehouses': warehouses, 'sheets': sheets}


def pack_competitor_report(report):
    return base64.b64encode(gzip.compress(json.dumps(report, ensure_ascii=False, allow_nan=False).encode())).decode()


def unpack_competitor_report(value):
    return json.loads(gzip.decompress(base64.b64decode(value))) if value else None
