"""Read the WB 'Оценка товара' export without changing the workbook."""
from io import BytesIO
import math
import re
from zipfile import ZipFile, BadZipFile


def parse_rating_report(content):
    from openpyxl import load_workbook
    try:
        with ZipFile(BytesIO(content)) as archive:
            if sum(item.file_size for item in archive.infolist()) > 40 * 1024 * 1024:
                raise ValueError('Слишком большой распакованный отчёт')
        workbook = load_workbook(BytesIO(content), read_only=True, data_only=True)
    except (BadZipFile, KeyError, OSError) as error:
        raise ValueError('Не удалось открыть Excel. Загрузите исходный отчёт WB в формате .xlsx') from error
    try:
        if 'Товары' not in workbook.sheetnames or 'Общая информация' not in workbook.sheetnames:
            raise ValueError('Нужен отчёт WB «Оценка товара» с листами «Товары» и «Общая информация»')
        # WB exports can declare a smaller dimension than their actual cells.
        for name in workbook.sheetnames:
            sheet = workbook[name]
            if hasattr(sheet, 'reset_dimensions'):
                sheet.reset_dimensions()
        period = None
        for row in workbook['Общая информация'].iter_rows(min_row=1, max_row=20, values_only=True):
            if row and str(row[0]).strip() == 'Выбранный период':
                period = str(row[1] or '').strip() if len(row)>1 else ''
        dates = re.findall(r'\d{4}-\d{2}-\d{2}', period or '')
        if len(dates) != 2:
            raise ValueError('В отчёте не найден выбранный период')
        headers = None
        items = []
        names = {'article':'Артикул продавца', 'nm_id':'Артикул WB', 'wb_rating':'Рейтинг по отзывам',
                 'total':'Все отзывы за период', 'excluded':'Отзывы, исключенные из рейтинга',
                 'moderation':'Отзывы, не прошедшие модерацию', 'deferred':'Все отложенные отзывы'}
        for star in range(1, 6):
            names['r'+str(star)] = 'Оценки '+str(star)
        def number(value, label, integer=True):
            if value is None or value == '':
                raise ValueError('Нет значения: '+label)
            try:
                value = float(str(value).replace('\u00a0','').replace(' ','').replace(',','.'))
            except (TypeError, ValueError):
                raise ValueError('Некорректное число: '+label)
            if not math.isfinite(value) or value < 0 or (integer and value != int(value)):
                raise ValueError('Некорректное число: '+label)
            return int(value) if integer else value
        for index, row in enumerate(workbook['Товары'].iter_rows(values_only=True), 1):
            if index > 20000:
                raise ValueError('Слишком много строк в отчёте')
            if headers is None:
                normalized = {str(value or '').strip().replace('ё','е'): i for i,value in enumerate(row)}
                if all(label.replace('ё','е') in normalized for label in names.values()):
                    headers = {key:normalized[label.replace('ё','е')] for key,label in names.items()}
                elif index >= 10:
                    raise ValueError('Не найдены обязательные столбцы отчёта «Оценка товара»')
                continue
            if not any(value is not None for value in row):
                continue
            raw_id = row[headers['nm_id']] if headers['nm_id'] < len(row) else None
            if raw_id is None:
                continue  # Footer or aggregate, never treat it as a product.
            nm_id = str(number(raw_id,'Артикул WB'))
            if nm_id == '0':
                raise ValueError('Артикул WB не может быть нулём')
            get = lambda key: row[headers[key]] if headers[key] < len(row) else None
            stars = {str(star):number(get('r'+str(star)), 'Оценки '+str(star)) for star in range(1,6)}
            total = number(get('total'), 'Все отзывы за период')
            if sum(stars.values()) != total:
                raise ValueError(f'Артикул {nm_id}: сумма оценок 1–5 не совпадает со всеми отзывами за период')
            rating = number(get('wb_rating'), 'Рейтинг по отзывам', False) if get('wb_rating') not in (None, '', 'Без рейтинга', '—', '-') else None
            if rating == 0:
                rating = None
            if rating is not None and not 1 <= rating <= 5:
                raise ValueError('Рейтинг WB должен быть от 1 до 5')
            items.append({'nm_id':nm_id, 'article':str(get('article') or nm_id).strip(), 'stars':stars,
                          'wb_rating':rating, 'total':total,
                          'excluded':number(get('excluded'),'Исключённые'),
                          'moderation':number(get('moderation'),'Не прошли модерацию'),
                          'deferred':number(get('deferred'),'Отложенные'),
                          'period_from':dates[0], 'period_to':dates[1]})
        if not items:
            raise ValueError('В отчёте нет товаров с оценками')
        if len({item['nm_id'] for item in items}) != len(items):
            raise ValueError('В отчёте повторяются артикулы WB. Выгрузите без дублирования строк')
        return items
    finally:
        workbook.close()


def apply_rating_reports(data, reports):
    """Scenario evidence comes only from reports, never from unfiltered API reviews."""
    products = {str(p['nm_id']):dict(p) for p in data.get('products', [])}
    for nm_id, report in reports.items():
        products.setdefault(nm_id, {'nm_id':nm_id, 'article':report['article']})
    for nm_id, item in products.items():
        report = reports.get(nm_id)
        item['report'] = report
        item['stars'] = report['stars'] if report else {str(star):0 for star in range(1,6)}
        # The report owns both the evidence and its historical rating when present.
        if report:
            item['wb_rating'] = report['wb_rating']
            item['article'] = report['article']
    return {**data, 'products':sorted(products.values(), key=lambda p:p['article']),
            'report_count':len(reports)}
