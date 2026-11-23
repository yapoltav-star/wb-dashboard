"""Daily, resumable WB position observations; settings storage survives deployments.

A snapshot is a measurement, never proof that Boost caused a ranking change.
Successful groups are immutable for the Moscow day. Failed groups alone are retried.
"""
from __future__ import annotations

import json
import threading
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

MSK = ZoneInfo('Europe/Moscow')
PREFIX = 'boost_rank_v1:'
VALID = {'found', 'not_found'}


def load_config():
    return json.loads(Path(__file__).with_name('boost_monitor_targets.json').read_text())


def snapshot_key(day, group_id):
    return f'{PREFIX}{day}:{group_id}'


def compare(before, after):
    if not before or not after:
        return {'label': 'Нет замера за одну из дат', 'delta': None}
    if before['status'] not in VALID or after['status'] not in VALID:
        return {'label': 'Ошибка замера — сравнение недоступно', 'delta': None}
    if any(before.get(k) != after.get(k) for k in ('dest', 'depth', 'source')):
        return {'label': 'Разные источники или условия замера', 'delta': None}
    if before['status'] == after['status'] == 'found':
        delta = before['position'] - after['position']
        return {'label': f'Выше на {delta}' if delta > 0 else f'Ниже на {-delta}' if delta < 0 else 'Без изменений', 'delta': delta}
    if before['status'] == after['status'] == 'not_found':
        return {'label': 'В обоих замерах не найден в проверенной части', 'delta': None}
    return {'label': 'Появился в проверенной части' if after['status'] == 'found' else 'Вышел из проверенной части', 'delta': None}


class SettingsStore:
    """Checked reads: a storage outage must not look like a missing snapshot."""
    def __init__(self, env):
        self.env = env

    def _get(self, params):
        e = self.env()
        response = e['httpx'].get(f"{e['SUPABASE_URL']}/rest/v1/settings", params=params,
                                 headers=e['sb_headers'](), timeout=20)
        response.raise_for_status()
        rows = response.json()
        if not isinstance(rows, list):
            raise RuntimeError('Некорректный ответ хранилища истории')
        return rows

    @staticmethod
    def _decode(value):
        return json.loads(value) if isinstance(value, str) else value

    def get(self, key):
        rows = self._get({'key': f'eq.{key}', 'select': 'value'})
        return self._decode(rows[0]['value']) if rows else None

    def put(self, key, value):
        if not self.env()['save_setting_value'](key, value):
            raise RuntimeError('Не удалось сохранить замер в базе. Результат не засчитан.')

    def between(self, start, end):
        rows, offset = [], 0
        while True:
            page = self._get({'and': f'(key.gte.{PREFIX}{start}:,key.lte.{PREFIX}{end}:~)',
                              'select': 'key,value', 'order': 'key.asc', 'limit': 500, 'offset': offset})
            rows.extend(self._decode(row['value']) for row in page)
            if len(page) < 500:
                return rows
            offset += len(page)


def observation(group, response, config, at):
    """Validate the checked prefix before allowing an absence observation."""
    mode = group['mode']
    depth = config[f'{mode}_depth']
    error = response.get('error')
    if mode == 'search':
        products = response.get('products') or []
        products = products[:depth]
        ids = [p.get('id') or p.get('nmId') or p.get('nmID') for p in products if isinstance(p, dict)]
        # An invalid row cannot be removed without moving subsequent positions.
        if len(ids) != len(products) or any(not str(n).isdigit() for n in ids):
            error = error or 'Некорректный порядок товаров в ответе WB'
        ordered = [(int(n), i) for i, n in enumerate(ids, 1) if str(n).isdigit()]
        checked = len(products)
        source = response.get('source')
    else:
        products = response.get('own_on_shelf') or []
        ordered = [(int(p['nm_id']), int(p['position'])) for p in products]
        checked = int(response.get('own_scan_depth') or 0)
        source = response.get('source')
        if source not in ('recom', 'similar'):
            error = error or 'Не определён тип полки'
    if not source or not checked:
        error = error or 'WB вернул пустую выдачу; отсутствие товара не подтверждено'
    positions = {}
    for nm, position in ordered:
        positions.setdefault(nm, position)
    rows = []
    for target in group['targets']:
        rank = positions.get(target['nm_id'])
        rows.append({**target, 'status': 'error' if error else 'found' if rank else 'not_found',
                     'position': None if error else rank, 'error': str(error)[:240] if error else None,
                     'dest': config['dest'], 'depth': depth, 'checked': checked, 'source': source,
                     'captured_at': at})
    return {'group_id': group['id'], 'mode': mode, 'query': group['query'],
            'source_nm_id': group['source_nm_id'], 'source_own': group['source_own'],
            'source_vendor_code': group['source_vendor_code'], 'captured_at': at,
            'ok': not bool(error), 'rows': rows}


class Monitor:
    def __init__(self, env, store=None, config=None, clock=None):
        self.env = env
        self.store = store or SettingsStore(env)
        self.config = config or load_config()
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.lock = threading.Lock()
        self.running = False
        self.health = {'message': 'Ожидает первого замера'}

    def run(self, force=False):
        if not self.lock.acquire(blocking=False):
            return
        self.running = True
        day = self.clock().astimezone(MSK).date().isoformat()
        try:
            if not force and self.clock().astimezone(MSK).hour < self.config['hour']:
                return
            self.health = {'message': 'Снимаем и сохраняем позиции', 'day': day, 'processed': 0}
            for index, group in enumerate(self.config['groups']):
                # A run crossing midnight leaves unfinished groups visibly missing,
                # rather than mislabelling tomorrow's observations as yesterday's.
                if self.clock().astimezone(MSK).date().isoformat() != day:
                    break
                key = snapshot_key(day, group['id'])
                prior = self.store.get(key)
                if prior and prior.get('ok'):
                    self.health['processed'] = index + 1
                    continue
                try:
                    e = self.env()
                    if group['mode'] == 'search':
                        response = e['fetch_wb_search_catalog'](group['query'], self.config['dest'], max_pages=5)
                    else:
                        own_map = {t['nm_id']: t['vendor_code'] for t in group['targets']}
                        response = e['fetch_wb_see_also_shelf'](group['source_nm_id'], self.config['dest'],
                                                               limit=15, own_map=own_map,
                                                               scan_limit=self.config['shelf_depth'])
                    snap = observation(group, response, self.config, self.clock().isoformat())
                except Exception as exc:
                    snap = observation(group, {'error': f'Ошибка запроса: {type(exc).__name__}'},
                                       self.config, self.clock().isoformat())
                if self.clock().astimezone(MSK).date().isoformat() != day:
                    break
                snap.update(day=day, attempts=(prior or {}).get('attempts', 0) + 1)
                # Each group commits independently. A restart resumes missing/errors.
                self.store.put(key, snap)
                self.health['processed'] = index + 1
            self.health['message'] = 'Проход завершён. Ошибочные замеры повторяются каждый час.'
            self.health['finished_at'] = self.clock().isoformat()
        except Exception as exc:
            self.health['message'] = 'Сбой хранения — замеры остановлены, сохранённая история не меняется'
            self.health['error'] = type(exc).__name__
            self.env()['logger'].warning('Boost rank monitoring failed: %s', type(exc).__name__)
        finally:
            self.running = False
            self.lock.release()

    def report(self, start=None, end=None):
        today = self.clock().astimezone(MSK).date()
        end = date.fromisoformat(end) if end else today
        start = date.fromisoformat(start) if start else end - timedelta(days=7)
        if not 0 <= (end-start).days <= 31 or end > today:
            raise ValueError('Выбери даты до сегодня с интервалом не более 31 дня')
        saved = self.store.between(start.isoformat(), end.isoformat())
        by_key = {(s['day'], s['group_id']): s for s in saved}
        days = [(start + timedelta(days=i)).isoformat() for i in range((end-start).days+1)]
        out, coverage = [], {}
        for day in days:
            counts = Counter()
            for group in self.config['groups']:
                snap = by_key.get((day, group['id']))
                counts.update(r['status'] for r in snap['rows']) if snap else counts.update({'missing': len(group['targets'])})
            coverage[day] = dict(counts)
        for group in self.config['groups']:
            samples = {day: {r['nm_id']: r for r in by_key.get((day, group['id']), {}).get('rows', [])} for day in days}
            for target in group['targets']:
                nm = target['nm_id']
                trail = {day: samples[day].get(nm) for day in days}
                first, last = trail[days[0]], trail[days[-1]]
                out.append({**target, 'mode': group['mode'], 'query': group['query'],
                            'source_nm_id': group['source_nm_id'], 'source_vendor_code': group['source_vendor_code'],
                            'source_own': group['source_own'], 'before': first, 'after': last,
                            'change': compare(first, last), 'history': trail})
        cfg = {k: v for k, v in self.config.items() if k != 'groups'}
        return {'config': cfg, 'from': days[0], 'to': days[-1], 'days': days, 'coverage': coverage,
                'running': self.running, 'health': dict(self.health), 'rows': out,
                'targets': len({r['nm_id'] for r in out}), 'groups': len(self.config['groups']),
                'search_pairs': sum(r['mode'] == 'search' for r in out),
                'shelf_pairs': sum(r['mode'] == 'shelf' for r in out),
                'shelf_sources': len({r['source_nm_id'] for r in out if r['mode'] == 'shelf'})}


_instance = None


def register_boost_monitor(app, env):
    # main.py is imported a second time by uvicorn's string entrypoint.
    # Keep a single worker/lock in this shared module in the single-process app.
    global _instance
    if _instance is None:
        _instance = Monitor(env)
        scheduler = env()['scheduler']
        _instance.scheduler = scheduler
        scheduler.add_job(_instance.run, 'cron', hour=_instance.config['hour'], minute=0, timezone=MSK,
                          id='boost_rank_daily', coalesce=True, max_instances=1, misfire_grace_time=3600)
        scheduler.add_job(_instance.run, 'interval', hours=1, id='boost_rank_retry',
                          next_run_time=datetime.now(timezone.utc)+timedelta(seconds=30),
                          coalesce=True, max_instances=1)
    monitor = _instance

    @app.get('/api/boost-position-history')
    def history(date_from: str | None = None, date_to: str | None = None):
        from fastapi import HTTPException
        try:
            result = monitor.report(date_from, date_to)
            job = monitor.scheduler.get_job('boost_rank_daily')
            result['next_run'] = job.next_run_time.isoformat() if job and job.next_run_time else None
            return result
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        except Exception:
            raise HTTPException(status_code=503, detail='История временно недоступна: ошибка чтения базы')

    @app.post('/api/boost-position-capture')
    def capture():
        if not monitor.running:
            threading.Thread(target=monitor.run, kwargs={'force': True}, daemon=True, name='boost-rank-manual').start()
        return {'accepted': True, 'message': 'Запущен замер недостающих и ошибочных связок. Успешные замеры дня сохраняются.'}

    return monitor
