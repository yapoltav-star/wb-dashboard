"""Persistent campaign incidents. No campaign mutations; CRM owns notifications."""
from __future__ import annotations

import copy
import threading
import time
from datetime import datetime, timezone, timedelta

KEY = 'ads_monitor_v1'
CONFIG_KEY = 'ads_monitor_config'
DEFAULTS = {'enabled': True, 'spend_ratio': 3, 'min_spend': 500, 'ignored_ids': []}


def evaluate(state, campaigns, stats, now, config):
    """Only explicit successful observations are evidence; missing rows are unknown."""
    state = copy.deepcopy(state)
    records = state.setdefault('campaigns', {})
    day = datetime.fromtimestamp(now, timezone(timedelta(hours=3))).date().isoformat()
    for cid, campaign in campaigns.items():
        key = str(cid)
        rec = records.setdefault(key, {'samples': [], 'incidents': {}})
        rec['name'] = campaign['name']
        status = campaign['status']
        if cid in config.get('ignored_ids', []):
            rec.update(status=status, samples=[], incidents={})
            continue
        incidents = rec['incidents']
        previous = rec.get('status')
        # Existing paused campaigns at installation do not generate a backlog.
        if previous == 9 and status in (7, 11):
            incidents.setdefault('stopped', {'at': now, 'detail': 'Работавшая кампания остановлена. Проверьте бюджет и причину остановки.'})
        if status == 9 and campaign.get('budget', 1) > 0:
            incidents.pop('stopped', None)
        if campaign.get('budget') is not None:
            if campaign['budget'] <= 0 and (status == 9 or 'stopped' in incidents):
                incidents.setdefault('stopped', {'at': now})['detail'] = 'Бюджет кампании исчерпан (остаток 0 ₽). Проверьте пополнение и восстановление показов.'
            elif status == 9:
                incidents.pop('stopped', None)
        rec['status'] = status
        stat = stats.get(cid)
        if stat is None:
            continue
        point = {'at': now, 'day': day, 'spend': stat['spend'], 'views': stat['views']}
        samples = rec['samples']
        if samples and (samples[-1]['day'] != day or now - samples[-1]['at'] > 1800
                        or point['spend'] < samples[-1]['spend'] or point['views'] < samples[-1]['views']):
            samples = []
        samples = (samples + [point])[-18:]
        rec['samples'] = samples
        # Compare last ~15 minutes with at least 30 minutes of earlier observations.
        if len(samples) < 10 or status != 9:
            continue
        boundary = next((p for p in reversed(samples[:-1]) if now - p['at'] >= 840), None)
        if boundary is None or now - boundary['at'] > 1800:
            continue
        history = [p for p in samples if p['at'] <= boundary['at']]
        if len(history) < 7 or history[-1]['at'] - history[0]['at'] < 1680:
            continue
        baseline = (history[-1]['spend'] - history[0]['spend']) / (history[-1]['at'] - history[0]['at'])
        spend = point['spend'] - boundary['spend']
        minutes = (now-boundary['at'])/60
        rate = spend/(now-boundary['at'])
        if baseline > 0 and spend >= config['min_spend'] and rate >= baseline * config['spend_ratio']:
            incidents.setdefault('spike', {'at': now, 'detail': (
                f'За {minutes:.0f} мин расход {spend:.0f} ₽, показы +{point["views"]-boundary["views"]}. '
                f'Темп расходов {rate/baseline:.1f}× прежнего. Проверьте ставки, размещения и лимиты. '
                'Это сигнал возможного перерасхода; статистика WB может приходить с задержкой.')})
            rec['healthy'] = 0
        elif 'spike' in incidents:
            rec['healthy'] = rec.get('healthy', 0) + 1
            if rec['healthy'] >= 6 and now - incidents['spike']['at'] >= 21600:
                incidents.pop('spike', None)
    state['checked_at'] = now
    return state


def leader(employees):
    matches = [e for e in employees if e.get('active', True)
               and any(a in str(e.get('name', '')).lower() for a in ('диля', 'дилия', 'dilya'))]
    preferred = [e for e in matches if any(a in str(e.get('role', '')).lower() for a in ('рук', 'head', 'lead'))]
    matches = preferred or matches
    if len(matches) != 1:
        raise RuntimeError('Не удалось однозначно определить Дилю-руководителя в CRM')
    return matches[0]


class Monitor:
    def __init__(self, env):
        self.env = env
        self.lock = threading.Lock()
        self.health = {'ok': False, 'message': 'Ожидает первой проверки'}

    def load(self, key=KEY):
        e = self.env()
        # Do not turn a storage outage into an empty deduplication journal.
        r = e['httpx'].get(f"{e['SUPABASE_URL']}/rest/v1/settings", params={'key': f'eq.{key}', 'select': 'value'}, headers=e['sb_headers'](), timeout=15)
        r.raise_for_status()
        rows = r.json()
        value = rows[0]['value'] if rows else {}
        return e['json'].loads(value) if isinstance(value, str) else value

    def save(self, state):
        if not self.env()['save_setting_value'](KEY, state):
            raise RuntimeError('Не удалось сохранить журнал контроля рекламы')

    def collect(self, state):
        e = self.env()
        r = e['httpx'].get(f"{e['WB_PROMOTION_URL']}/api/advert/v2/adverts", params={'statuses': '7,9,11'}, headers=e['wb_headers'](), timeout=30)
        r.raise_for_status()
        campaigns = {}
        for c in e['_parse_advert_v2_items'](r.json()):
            cid = int(e['_advert_id_from_item'](c))
            campaigns[cid] = {'name': e['_advert_name_from_item'](c) or f'Кампания #{cid}', 'status': int(c['status'])}
        candidates = [cid for cid,c in campaigns.items() if c['status'] == 9 or (c['status'] == 11 and state.get('campaigns', {}).get(str(cid), {}).get('status') == 9)]
        warnings = []
        for i in range(0, len(candidates), 50):
            if i:
                time.sleep(3)
            try:
                r = e['httpx'].post(f"{e['WB_PROMOTION_URL']}/api/advert/v2/budget", json={'advertIds': candidates[i:i+50]}, headers=e['wb_headers'](), timeout=25)
                r.raise_for_status()
                for item in r.json()['adverts']:
                    cid = int(item['advertId'])
                    if cid in campaigns and item.get('total') is not None:
                        campaigns[cid]['budget'] = float(item['total'])
            except Exception:
                warnings.append('Бюджеты недоступны: причина остановки не подтверждена')
                break
        day = datetime.now(timezone(timedelta(hours=3))).date().isoformat()
        try:
            stats = e['fetch_ad_stats_by_campaign']([cid for cid,c in campaigns.items() if c['status'] == 9], day, day)
        except Exception:
            stats = {}
            warnings.append('Статистика недоступна: скачки расходов не проверены')
        return campaigns, stats, warnings

    def deliver(self, state):
        e = self.env()
        pending = [(cid,rec,kind,incident) for cid,rec in state.get('campaigns', {}).items()
                   for kind,incident in rec['incidents'].items() if not incident.get('task_id')]
        if not pending:
            return
        r = e['httpx'].get(f"{e['CRM_API_URL']}/api/board", headers=e['_crm_headers'](), timeout=25)
        r.raise_for_status()
        board = r.json()
        assignee = leader(board.get('employees', []))
        owner = next((v for v in board['employees'] if v.get('role') == 'owner'), assignee)
        tasks = list(e['_crm_iter_board_tasks'](board))
        for cid,rec,kind,incident in pending:
            marker = f'[dash:ads-alert:{cid}:{kind}:{int(incident["at"])}]'
            existing = next((t for t in tasks if marker in str(t.get('description', ''))), None)
            if existing:
                incident['task_id'] = existing['id']
                self.save(state)
                continue
            title = ('Проверить остановку рекламы' if kind == 'stopped' else 'Проверить скачок расходов') + f' · {rec["name"]}'
            description = (f'{marker}\nКампания #{cid}\n{incident["detail"]}\n'
                           f'https://cmp.wildberries.ru/campaigns/edit/{cid}\n'
                           'Если изменение плановое — отметьте это в задаче. Бот не меняет ставки и не пополняет бюджет.')
            payload = {'title': title[:500], 'description': description[:2000], 'articles': f'{rec["name"]} #{cid}'[:500],
                       'assignee_id': assignee['id'], 'assignee_ids': [assignee['id']], 'created_by_id': owner['id'],
                       'status': 'todo', 'kind': 'once', 'priority': 'normal', 'notify_now': True}
            r = e['httpx'].post(f"{e['CRM_API_URL']}/api/tasks", headers=e['_crm_headers'](), json=payload, timeout=30)
            r.raise_for_status()
            task = r.json()
            incident.update(task_id=task['id'], notified=task.get('notified'), notify_error=task.get('notify_error'))
            self.save(state)

    def run(self):
        if not self.lock.acquire(blocking=False):
            return
        try:
            e = self.env()
            if not e['CRM_API_URL'] or not e['WB_TOKEN']:
                self.health = {'ok': False, 'message': 'Не настроен доступ'}
                return
            config = {**DEFAULTS, **self.load(CONFIG_KEY)}
            if not config['enabled']:
                self.health = {'ok': False, 'message': 'Выключен или не настроен доступ'}
                return
            state = self.load()
            campaigns, stats, warnings = self.collect(state)
            state = evaluate(state, campaigns, stats, time.time(), config)
            self.save(state)  # Persist incident identity before any external side effect.
            self.deliver(state)
            self.health = {'ok': True, 'checked_at': state['checked_at'], 'campaigns': len(campaigns), 'budgets_checked': sum('budget' in c for c in campaigns.values()),
                           'stats_checked': len(stats), 'warnings': warnings,
                           'tasks': [i for r in state['campaigns'].values() for i in r['incidents'].values() if i.get('task_id')]}
        except Exception as exc:
            self.health = {'ok': False, 'error': type(exc).__name__, 'message': str(exc).split(' for url:')[0][:250]}
            self.env()['logger'].warning('Ads monitor: %s', self.health['error'])
        finally:
            self.lock.release()


def register_ads_monitor(app, env):
    monitor = Monitor(env)
    env()['scheduler'].add_job(monitor.run, 'interval', minutes=5, id='ads_monitor', max_instances=1,
                               coalesce=True, next_run_time=datetime.now(timezone.utc)+timedelta(seconds=90))

    @app.get('/api/ads-monitor-status')
    def status():
        return {**monitor.health, 'interval_minutes': 5, 'defaults': DEFAULTS}

    return monitor
