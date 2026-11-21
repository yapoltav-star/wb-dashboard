import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'backend'))
from rating_report import parse_rating_report, apply_rating_reports
from io import BytesIO
from zipfile import ZipFile
import unittest
from unittest.mock import patch
from types import SimpleNamespace

class Sheet:
    def __init__(self,rows):self.rows=rows
    def iter_rows(self,**kwargs):return iter(self.rows)

class Workbook(dict):
    @property
    def sheetnames(self):return list(self)
    def close(self):pass

class ReportTests(unittest.TestCase):
    def parse(self, total=19):
        headers=['Артикул продавца','Артикул WB','Рейтинг по отзывам','Все отзывы за период',
                 'Отзывы, исключенные из рейтинга','Отзывы, не прошедшие модерацию','Все отложенные отзывы',
                 'Оценки 1','Оценки 2','Оценки 3','Оценки 4','Оценки 5']
        book=Workbook({'Общая информация':Sheet([('Выбранный период','С 2025-11-21 по 2026-11-20')]),
            'Товары':Sheet([('Детальный отчёт',),headers,['Часы',123,4.7,total,100,13,50,2,0,0,0,17]])})
        content=BytesIO()
        with ZipFile(content,'w'):pass
        with patch.dict(sys.modules,{'openpyxl':SimpleNamespace(load_workbook=lambda *a,**kw:book)}):
            return parse_rating_report(content.getvalue())

    def test_stars_are_already_net_of_exclusions(self):
        row=self.parse()[0]
        self.assertEqual(sum(row['stars'].values()),19)
        self.assertEqual(row['excluded'],100)
        self.assertEqual(row['moderation'],13)
        self.assertEqual(row['deferred'],50)
        self.assertEqual(row['period_from'],'2025-11-21')
        self.assertEqual(row['wb_rating'],4.7)

    def test_inconsistent_report_rejected(self):
        with self.assertRaisesRegex(ValueError,'сумма оценок'):self.parse(total=20)

    def test_report_overrides_api_and_uncovered_counts_remain_unavailable(self):
        row=self.parse()[0]
        data={'groups':{},'products':[{'nm_id':'123','article':'Old','stars':{'5':999},'wb_rating':5},
                                   {'nm_id':'456','article':'Other','stars':{'5':888},'wb_rating':4.8}]}
        result=apply_rating_reports(data,{'123':row})
        mapped={p['nm_id']:p for p in result['products']}
        self.assertEqual(mapped['123']['stars']['5'],17)
        self.assertEqual(mapped['123']['wb_rating'],4.7)
        self.assertIsNone(mapped['456']['report'])
        self.assertEqual(sum(mapped['456']['stars'].values()),0)
        self.assertEqual(data['products'][0]['stars']['5'],999)

if __name__=='__main__':unittest.main()
