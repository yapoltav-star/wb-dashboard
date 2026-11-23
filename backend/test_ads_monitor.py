import copy
import unittest
from unittest.mock import Mock
from ads_monitor import evaluate, DEFAULTS, Monitor, leader


class Incidents(unittest.TestCase):
    def run_step(self, state, status=9, budget=100, spend=0, now=100000):
        return evaluate(state, {1: {'name':'Часы', 'status':status, 'budget':budget}}, {1:{'spend':spend, 'views':spend*10}}, now, DEFAULTS)

    def test_initial_pause_is_not_incident(self):
        s = self.run_step({}, status=11, budget=0)
        self.assertEqual(s['campaigns']['1']['incidents'], {})

    def test_stop_and_recover(self):
        s = self.run_step({})
        s = self.run_step(s, status=11, budget=0, now=100300)
        incident = s['campaigns']['1']['incidents']['stopped']
        self.assertIn('исчерпан', incident['detail'])
        incident['task_id'] = 5
        s = self.run_step(s, status=11, budget=0, now=100600)
        self.assertEqual(s['campaigns']['1']['incidents']['stopped']['task_id'], 5)
        s = self.run_step(s, now=100900)
        self.assertNotIn('stopped', s['campaigns']['1']['incidents'])
        s = self.run_step(s, status=11, now=101200)
        self.assertEqual(s['campaigns']['1']['incidents']['stopped']['at'], 101200)

    def test_zero_budget_while_active_deduplicates(self):
        s = self.run_step({}, budget=0)
        s['campaigns']['1']['incidents']['stopped']['task_id'] = 5
        s = self.run_step(s, budget=0, now=100300)
        self.assertEqual(s['campaigns']['1']['incidents']['stopped']['task_id'], 5)

    def test_missing_campaign_is_not_stop(self):
        s = self.run_step({})
        updated = evaluate(s, {}, {}, 100300, DEFAULTS)
        self.assertEqual(updated['campaigns'], s['campaigns'])

    def test_spike(self):
        s = {}
        for i in range(10):
            s = self.run_step(s, spend=i*50, now=100000+i*300)
        self.assertNotIn('spike', s['campaigns']['1']['incidents'])
        for i in range(3):
            s = self.run_step(s, spend=450+(i+1)*300, now=103000+i*300)
        self.assertIn('spike', s['campaigns']['1']['incidents'])

    def test_reset_and_outage_do_not_create_spike(self):
        s = {}
        for i in range(10):
            s = self.run_step(s, spend=i*50, now=100000+i*300)
        for spend,now in [(0,103000),(10000,120000),(10000,172800)]:
            s = self.run_step(s, spend=spend, now=now)
            self.assertNotIn('spike', s['campaigns']['1']['incidents'])

    def test_ignored(self):
        s = evaluate({}, {1:{'name':'x','status':9,'budget':0}}, {}, 100000, {**DEFAULTS,'ignored_ids':[1]})
        self.assertEqual(s['campaigns']['1']['incidents'], {})

    def test_leader_ambiguity(self):
        people = [{'name':'Диля','id':1},{'name':'Диля','id':2,'role':'рук'}]
        self.assertEqual(leader(people)['id'], 2)
        people[1]['role'] = 'manager'
        with self.assertRaises(RuntimeError):
            leader(people)

    def test_storage_failure_blocks_delivery(self):
        http = Mock()
        http.get.side_effect = RuntimeError('storage unavailable')
        env = {'httpx':http, 'CRM_API_URL':'https://crm.example', 'WB_TOKEN':'test',
               'SUPABASE_URL':'https://db.example', 'sb_headers':lambda:{}, 'logger':Mock()}
        m = Monitor(lambda:env)
        m.run()
        self.assertFalse(m.health['ok'])
        http.post.assert_not_called()

    def test_missing_stats_do_not_reset_sample_baseline(self):
        s = self.run_step({})
        before = copy.deepcopy(s['campaigns']['1']['samples'])
        s = evaluate(s, {1:{'name':'Часы','status':9}}, {}, 100300, DEFAULTS)
        self.assertEqual(s['campaigns']['1']['samples'], before)

    def test_delivery_reconciles_after_post_timeout(self):
        state = self.run_step({}, budget=0)
        incident = state['campaigns']['1']['incidents']['stopped']
        marker = f'[dash:ads-alert:1:stopped:{int(incident["at"])}]'
        http = Mock()
        http.get.return_value.json.return_value = {'employees':[{'id':2,'name':'Диля','role':'рук'}], 'tasks':[{'id':7,'description':marker}]}
        env = {'httpx':http,'CRM_API_URL':'https://crm.example','_crm_headers':lambda:{},
               '_crm_iter_board_tasks':lambda b:b['tasks'], 'save_setting_value':Mock(return_value=True)}
        m = Monitor(lambda:env)
        m.deliver(state)
        http.post.assert_not_called()
        self.assertEqual(incident['task_id'], 7)

    def test_delivery_requests_telegram_once(self):
        state = self.run_step({}, budget=0)
        http = Mock()
        http.get.return_value.json.return_value = {'employees':[{'id':2,'name':'Диля','role':'рук'}], 'tasks':[]}
        http.post.return_value.json.return_value = {'id':7,'notified':True}
        env = {'httpx':http,'CRM_API_URL':'https://crm.example','_crm_headers':lambda:{},
               '_crm_iter_board_tasks':lambda b:b['tasks'], 'save_setting_value':Mock(return_value=True)}
        m = Monitor(lambda:env)
        m.deliver(state)
        m.deliver(state)
        self.assertEqual(http.post.call_count, 1)
        self.assertEqual(http.post.call_args.kwargs['json']['assignee_id'], 2)
        self.assertTrue(http.post.call_args.kwargs['json']['notify_now'])

if __name__ == '__main__':
    unittest.main()
