import ast
from pathlib import Path
from types import SimpleNamespace
import unittest
import logging

SOURCE = Path(__file__).resolve().parents[1] / 'backend/main.py'

def load_functions(names, scope):
    nodes = [n for n in ast.parse(SOURCE.read_text()).body if isinstance(n, ast.FunctionDef) and n.name in names]
    for n in nodes:
        n.decorator_list = []
    exec(compile(ast.Module(body=nodes,type_ignores=[]),str(SOURCE),'exec'),scope)
    return scope

def product(nm, orders=0, amount=0, opens=0, cart=0):
    return {'product':{'nmId':nm},'statistic':{'selected':{
        'orderCount':orders,'orderSum':amount,'openCount':opens,'cartCount':cart,
        'conversions':{'addToCartPercent':17,'cartToOrderPercent':5}}}}

class FunnelMetricsTests(unittest.TestCase):
    def scope(self, pages):
        self.calls=[]
        def post(url, **kwargs):
            self.calls.append(kwargs['json'])
            page=pages.pop(0)
            return SimpleNamespace(is_success=page is not None,status_code=429,text='rate limit',json=lambda:{'data':{'products':page}})
        return load_functions(['_funnel_products_range','_catalog_custom_orders','_catalog_funnel_period'],
            dict(WB_TOKEN='test',WB_ANALYTICS_URL='https://example.invalid',wb_headers=lambda:{},
                 httpx=SimpleNamespace(post=post),time=SimpleNamespace(sleep=lambda _:None),logger=logging.getLogger('test')))

    def test_same_period_totals_and_zero_order_carts(self):
        scope=self.scope([[product(1,419,1981351,45000,7800), product(2,0,0,769,48)]])
        rows=scope['_catalog_funnel_period']('2026-11-22','2026-11-22')
        self.assertEqual(sum(x['orders'] for x in rows.values()),419)
        self.assertEqual(sum(x['orders_sum'] for x in rows.values()),1981351)
        self.assertEqual(sum(x['cart'] for x in rows.values()),7848)
        self.assertEqual(sum(x['opens'] for x in rows.values()),45769)
        self.assertTrue(all(x['ctr'] is None and x['impressions'] is None for x in rows.values()))
        self.assertEqual(self.calls[0]['selectedPeriod'],{'start':'2026-11-22','end':'2026-11-22'})

    def test_later_page_failure_never_returns_partial_totals(self):
        scope=self.scope([[product(n,1,10) for n in range(1000)],None])
        with self.assertRaises(RuntimeError):
            scope['_catalog_custom_orders']('2026-11-22','2026-11-22')
        self.assertEqual(self.calls[-1]['offset'],1000)

    def test_pagination_includes_all_products(self):
        scope=self.scope([[product(n,1,10) for n in range(1000)],[product(1001,2,20)]])
        orders,amounts=scope['_catalog_custom_orders']('2026-11-22','2026-11-22')
        self.assertEqual(sum(orders.values()),1002)
        self.assertEqual(sum(amounts.values()),10020)

    def test_missing_required_amount_is_not_zero(self):
        row=product(1,1,100); del row['statistic']['selected']['orderSum']
        scope=self.scope([[row]])
        with self.assertRaises(KeyError):
            scope['_catalog_custom_orders']('2026-11-22','2026-11-22')

if __name__ == '__main__': unittest.main()
