"""Read-only rating evidence plus user-defined simulation groups."""
import threading
import time
from datetime import datetime, timedelta
from fastapi import HTTPException, Request


def normalize_groups(groups, allowed):
    if not isinstance(groups, dict):
        raise HTTPException(400, 'Ожидается список склеек')
    result = {}
    for name, members in groups.items():
        if not isinstance(name, str) or not name.strip() or len(name) > 120 or not isinstance(members, list):
            raise HTTPException(400, 'Проверьте название и состав склейки')
        if any(str(nm) not in allowed for nm in members):
            raise HTTPException(400, 'В склейке есть неизвестный артикул вашего кабинета')
        result[name.strip()] = list(dict.fromkeys(str(nm) for nm in members))
    return result


def register_rating_workspace(app, deps):
    cache = {'data': None, 'syncing': False, 'error': None, 'ts': 0}
    lock = threading.Lock()
    report_lock = threading.Lock()
    report_state = {'loaded':False, 'items':{}}

    def refresh():
        try:
            api = deps()
            with report_lock:
                if not report_state['loaded']:
                    report_state['items'] = api['get_setting_json']('rating_workspace_reports', {}) or {}
                    report_state['loaded'] = True
            response = api['_dash_rpc']('get_article_stats', {}, 30)
            response.raise_for_status()
            evidence = response.json()
            mapping = api['build_nm_to_vendor_map']()
            by_nm = {str(nm): {'nm_id': str(nm), 'article': article,
                     'stars': {str(star): 0 for star in range(1, 6)}, 'wb_rating': None}
                     for nm, article in mapping.items()}
            for row in evidence:
                nm = str(row.get('nm_id') or '')
                if not nm.isdigit():
                    continue
                item = by_nm.setdefault(nm, {'nm_id': nm, 'article': row.get('article') or nm,
                        'stars': {str(star): 0 for star in range(1, 6)}, 'wb_rating': None})
                for star in range(1, 6):
                    item['stars'][str(star)] += int(row.get('r'+str(star)) or 0)
            now = api['_msk_now']()
            ids = [int(nm) for nm in by_nm]
            official = {}
            # Existing seller-token Sales Funnel API; never import another shop's ratings.
            for offset in range(0, len(ids), 500):
                if offset:
                    time.sleep(21)
                official.update(api['fetch_own_stats_v3'](ids[offset:offset+500],
                    (now-timedelta(days=1)).strftime('%Y-%m-%d'), now.strftime('%Y-%m-%d')))
            for nm, item in by_nm.items():
                # fetch_own_stats_v3 can fall back to old imported ratings; do not use that field.
                rating = official.get(nm, {}).get('api_feedback_rating')
                item['wb_rating'] = rating if isinstance(rating, (int, float)) and 1 <= rating <= 5 else None
            saved = api['get_setting_json']('rating_workspace_groups', None)
            if not saved:
                reverse = {item['article']: nm for nm, item in by_nm.items()}
                response = api['_dash_get']('groups_config?select=name,articles&order=sort_order', 15)
                response.raise_for_status()
                saved = {g['name']: list(dict.fromkeys(reverse[a] for a in g.get('articles', []) if a in reverse))
                         for g in response.json()}
            data = {'products': sorted(by_nm.values(), key=lambda p: p['article']), 'groups': saved,
                    'updated_at': now.strftime('%d.%m.%Y %H:%M'),
                    'feedback_sync': api['get_setting_raw']('last_sync', None),
                    'wb_available': any(p['wb_rating'] is not None for p in by_nm.values())}
            with lock:
                cache.update(data=data, ts=time.time(), error=None)
        except Exception:
            deps()['logger'].exception('Rating workspace refresh failed')
            with lock:
                cache['error'] = 'Не удалось загрузить данные рейтинга. Попробуйте обновить позже.'
        finally:
            with lock:
                cache['syncing'] = False

    @app.get('/api/rating-workspace')
    def get_workspace(refresh_data: bool = False):
        with lock:
            if (refresh_data or cache['data'] is None or time.time()-cache['ts'] > 600) and not cache['syncing']:
                cache['syncing'] = True
                cache['error'] = None
                threading.Thread(target=refresh, daemon=True).start()
            from rating_report import apply_rating_reports
            return apply_rating_reports({**(cache['data'] or {'groups':{}}), 'syncing': cache['syncing'], 'error': cache['error']}, report_state['items'])

    @app.post('/api/rating-workspace/report')
    async def upload_workspace_report(request: Request):
        from rating_report import parse_rating_report
        from starlette.concurrency import run_in_threadpool
        form = await request.form()
        upload = form.get('file')
        if upload is None or not hasattr(upload, 'read') or not str(upload.filename or '').lower().endswith('.xlsx'):
            raise HTTPException(400, 'Выберите исходный отчёт WB в формате .xlsx')
        content = await upload.read(12 * 1024 * 1024 + 1)
        await upload.close()
        if len(content) > 12 * 1024 * 1024:
            raise HTTPException(400, 'Максимальный размер файла — 12 МБ')
        try:
            items = await run_in_threadpool(parse_rating_report, content)
        except Exception as error:
            deps()['logger'].warning('Rating report validation: %s', error)
            raise HTTPException(400, str(error) if isinstance(error, ValueError) else 'Не удалось прочитать файл Excel')
        api = deps()
        uploaded_at = api['_msk_now']().strftime('%d.%m.%Y %H:%M')
        filename = str(upload.filename or 'Отчёт WB.xlsx').replace('\\', '/').split('/')[-1][:200]
        def persist_report():
            with report_lock:
                old = report_state['items'] if report_state['loaded'] else (api['get_setting_json']('rating_workspace_reports', {}) or {})
                updated = {**old, **{item['nm_id']:{**item, 'uploaded_at':uploaded_at, 'filename':filename} for item in items}}
                if not api['save_setting_value']('rating_workspace_reports', updated):
                    raise HTTPException(502, 'Не удалось сохранить отчёт. Предыдущие данные не изменены')
                report_state.update(loaded=True, items=updated)
        await run_in_threadpool(persist_report)
        return {'count':len(items), 'nm_ids':[item['nm_id'] for item in items], 'uploaded_at':uploaded_at,
                'period_from':items[0]['period_from'], 'period_to':items[0]['period_to']}

    @app.post('/api/rating-workspace/groups')
    def save_workspace_groups(request: dict):
        with lock:
            data = cache['data']
            if data is None:
                raise HTTPException(409, 'Сначала дождитесь загрузки товаров')
            groups = normalize_groups(request.get('groups'), {p['nm_id'] for p in data['products']} | set(report_state['items']))
        if not deps()['save_setting_value']('rating_workspace_groups', groups):
            raise HTTPException(502, 'Не удалось сохранить склейки')
        with lock:
            cache['data']['groups'] = groups
        return {'groups': groups}
