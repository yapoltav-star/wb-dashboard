import unittest
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend"))
from datetime import datetime
import time

class RatingWorkspaceTests(unittest.TestCase):
    def setUp(self):
        class HTTPException(Exception):
            def __init__(self, code, message):
                self.status_code=code
        self.scope={'HTTPException':HTTPException, 'Request':object}
        path=Path(__file__).resolve().parents[1]/'backend/rating_workspace.py'
        exec(compile(path.read_text().replace('from fastapi import HTTPException, Request',''), str(path), 'exec'), self.scope)
        self.error=HTTPException

    def test_own_group_membership_and_deduplication(self):
        normal=self.scope['normalize_groups']
        self.assertEqual(normal({' Часы ':['123','123',456]}, {'123','456'}), {'Часы':['123','456']})
        with self.assertRaises(self.error): normal({'Часы':['999']},{'123'})
        with self.assertRaises(self.error): normal({'':['123']},{'123'})

    def test_imported_ratings_never_replace_missing_api_rating(self):
        routes={}
        class App:
            def get(self,path): return lambda fn: routes.setdefault(('GET',path),fn)
            def post(self,path): return lambda fn: routes.setdefault(('POST',path),fn)
        class Response:
            def __init__(self,data):self.data=data
            def raise_for_status(self):pass
            def json(self):return self.data
        saves=[]
        deps={
          '_dash_rpc':lambda *args:Response([{'nm_id':123,'article':'Часы','r5':17,'r1':2}]),
          'build_nm_to_vendor_map':lambda:{123:'Часы'},
          '_msk_now':lambda:datetime(2026,11,21,12),
          'fetch_own_stats_v3':lambda *args:{'123':{'feedback_rating':5,'api_feedback_rating':0}},
          'get_setting_json':lambda key,*args:{} if key=='rating_workspace_reports' else {'Моя склейка':['123']},
          'get_setting_raw':lambda *args:'21.11.2026',
          'save_setting_value':lambda key,groups:saves.append((key,groups)) or True,
        }
        self.scope['register_rating_workspace'](App(),lambda:deps)
        getter=routes['GET','/api/rating-workspace'];data=getter()
        deadline=time.time()+2
        while data['syncing'] and time.time()<deadline:
            time.sleep(.01);data=getter()
        self.assertFalse(data['syncing']);self.assertEqual(data['products'][0]['stars']['5'],0)
        self.assertIsNone(data['products'][0]['report'])
        self.assertIsNone(data['products'][0]['wb_rating'])
        self.assertFalse(data['wb_available'])
        routes['POST','/api/rating-workspace/groups']({'groups':{'Новая':['123']}})
        self.assertEqual(saves,[('rating_workspace_groups',{'Новая':['123']})])
        self.assertEqual(getter()['groups'],{'Новая':['123']})

if __name__=='__main__':unittest.main()
