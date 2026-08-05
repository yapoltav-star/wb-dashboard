"""Рост продаж (sales-pace) — темп cur vs prev, как на WB."""

from __future__ import annotations

import logging
import threading
import time
from datetime import date, datetime, timedelta, timezone
from typing import Callable

logger = logging.getLogger("ozon-dashboard.pace")

MSK = timezone(timedelta(hours=3))
SALES_PACE_PERIODS = ("day", "week", "weeks2", "month")

SALES_PACE_CACHE: dict = {
    "by_period": {},
    "syncing": False,
    "syncing_period": None,
    "error": None,
}

_pace_lock = threading.Lock()


def _msk_now() -> datetime:
    return datetime.now(MSK)


def _pace_cache_key(period: str, date_cur=None, date_prev=None) -> str:
    if period == "day" and date_cur:
        return f"day:{date_cur}:auto"
    return period


def _parse_ymd(s: str | None) -> date | None:
    if not s:
        return None
    try:
        return datetime.strptime(s.strip()[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _pace_windows(period: str, now: datetime, date_cur=None, date_prev=None) -> dict:
    today0 = now.replace(hour=0, minute=0, second=0, microsecond=0)

    if period == "day":
        dc = _parse_ymd(date_cur)
        if dc:
            cur_start = datetime(dc.year, dc.month, dc.day, tzinfo=MSK)
            cur_end = cur_start + timedelta(days=1) - timedelta(seconds=1)
            dp = _parse_ymd(date_prev) or (dc - timedelta(days=1))
            prev_start = datetime(dp.year, dp.month, dp.day, tzinfo=MSK)
            prev_end = prev_start + timedelta(days=1) - timedelta(seconds=1)
            return {
                "cur_start": cur_start,
                "cur_end": cur_end,
                "prev_start": prev_start,
                "prev_end": prev_end,
                "label_cur": dc.strftime("%d.%m.%Y"),
                "label_prev": dp.strftime("%d.%m.%Y"),
                "col_cur": dc.strftime("%d.%m"),
                "col_prev": dp.strftime("%d.%m"),
                "custom_dates": True,
            }
        cur_start = today0
        prev_start = today0 - timedelta(days=1)
        prev_end = prev_start + (now - cur_start)
        return {
            "cur_start": cur_start,
            "cur_end": now,
            "prev_start": prev_start,
            "prev_end": prev_end,
            "label_cur": f"сегодня до {now.strftime('%H:%M')}",
            "label_prev": f"вчера до {now.strftime('%H:%M')}",
            "col_cur": "Сегодня",
            "col_prev": "Вчера",
            "custom_dates": False,
        }

    if period == "week":
        cur_start = today0 - timedelta(days=today0.weekday())
        prev_start = cur_start - timedelta(days=7)
        prev_end = prev_start + (now - cur_start)
        return {
            "cur_start": cur_start,
            "cur_end": now,
            "prev_start": prev_start,
            "prev_end": prev_end,
            "label_cur": f"эта неделя ({cur_start.strftime('%d.%m')}–{now.strftime('%d.%m %H:%M')})",
            "label_prev": f"прошлая неделя ({prev_start.strftime('%d.%m')}–{prev_end.strftime('%d.%m %H:%M')})",
            "col_cur": "Текущий",
            "col_prev": "Прошлый",
            "custom_dates": False,
        }

    if period == "weeks2":
        cur_start = now - timedelta(days=14)
        prev_start = now - timedelta(days=28)
        prev_end = now - timedelta(days=14)
        return {
            "cur_start": cur_start,
            "cur_end": now,
            "prev_start": prev_start,
            "prev_end": prev_end,
            "label_cur": f"последние 14 дн. ({cur_start.strftime('%d.%m')}–{now.strftime('%d.%m')})",
            "label_prev": f"пред. 14 дн. ({prev_start.strftime('%d.%m')}–{prev_end.strftime('%d.%m')})",
            "col_cur": "Текущий",
            "col_prev": "Прошлый",
            "custom_dates": False,
        }

    # month
    cur_start = today0.replace(day=1)
    if cur_start.month == 1:
        prev_month_start = cur_start.replace(year=cur_start.year - 1, month=12)
    else:
        prev_month_start = cur_start.replace(month=cur_start.month - 1)
    try:
        prev_end = prev_month_start.replace(
            day=now.day, hour=now.hour, minute=now.minute, second=now.second, microsecond=0
        )
    except ValueError:
        if prev_month_start.month == 12:
            nxt = prev_month_start.replace(year=prev_month_start.year + 1, month=1, day=1)
        else:
            nxt = prev_month_start.replace(month=prev_month_start.month + 1, day=1)
        prev_end = nxt - timedelta(seconds=1)
        prev_end = prev_end.replace(hour=now.hour, minute=now.minute, second=now.second, microsecond=0)
    return {
        "cur_start": cur_start,
        "cur_end": now,
        "prev_start": prev_month_start,
        "prev_end": prev_end,
        "label_cur": f"этот месяц ({cur_start.strftime('%d.%m')}–{now.strftime('%d.%m %H:%M')})",
        "label_prev": f"прошлый месяц ({prev_month_start.strftime('%d.%m')}–{prev_end.strftime('%d.%m %H:%M')})",
        "col_cur": "Текущий",
        "col_prev": "Прошлый",
        "custom_dates": False,
    }


def _iso_z(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=MSK)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _count_posting_products(postings: list, since: datetime, until: datetime) -> dict[str, int]:
    """Сумма quantity по offer_id в окне (по created_at если есть)."""
    out: dict[str, int] = {}
    for p in postings:
        created = p.get("created_at") or p.get("in_process_at")
        if created:
            try:
                d = datetime.fromisoformat(str(created).replace("Z", "+00:00"))
                if d.tzinfo is None:
                    d = d.replace(tzinfo=timezone.utc)
                d = d.astimezone(MSK)
                if d < since or d > until:
                    continue
            except Exception:
                pass
        for prod in p.get("products") or []:
            offer = str(prod.get("offer_id") or "").strip()
            if not offer:
                sku = prod.get("sku")
                if sku is not None:
                    offer = f"sku:{sku}"
                else:
                    continue
            qty = int(prod.get("quantity") or 1)
            out[offer] = out.get(offer, 0) + qty
    return out


def fetch_fbo_postings(ozon_post: Callable, since: datetime, until: datetime, max_pages: int = 40) -> list:
    """POST /v3/posting/fbo/list (v2 отключат 1 июня 2026)."""
    items = []
    cursor = ""
    limit = 1000
    body_filter = {"since": _iso_z(since), "to": _iso_z(until)}
    for _ in range(max_pages):
        body: dict = {
            "filter": body_filter,
            "limit": limit,
            "sort_dir": "ASC",
            "with": {"analytics_data": False, "financial_data": False},
        }
        if cursor:
            body["cursor"] = cursor
        payload = ozon_post("/v3/posting/fbo/list", body)
        batch = payload.get("postings") or (payload.get("result") or {}).get("postings") or []
        items.extend(batch)
        cursor = payload.get("cursor") or (payload.get("result") or {}).get("cursor") or ""
        has_next = payload.get("has_next")
        if has_next is None:
            has_next = bool(cursor) and len(batch) >= limit
        if not batch or not has_next:
            break
        time.sleep(0.2)
    return items


def fetch_fbs_postings(ozon_post: Callable, since: datetime, until: datetime, max_pages: int = 40) -> list:
    """POST /v4/posting/fbs/list (v3 отключат 1 июня 2026)."""
    items = []
    cursor = ""
    limit = 1000
    body_filter = {
        "since": _iso_z(since),
        "to": _iso_z(until),
    }
    for _ in range(max_pages):
        body: dict = {
            "filter": body_filter,
            "limit": limit,
            "sort_dir": "ASC",
            "with": {"analytics_data": False, "financial_data": False, "translit": False},
        }
        if cursor:
            body["cursor"] = cursor
        payload = ozon_post("/v4/posting/fbs/list", body)
        batch = payload.get("postings") or []
        items.extend(batch)
        cursor = payload.get("cursor") or ""
        has_next = payload.get("has_next")
        if has_next is None:
            has_next = bool(cursor) and len(batch) >= limit
        if not batch or not has_next:
            break
        time.sleep(0.2)
    return items


def fetch_orders_windows(
    ozon_post: Callable, cur_start: datetime, cur_end: datetime, prev_start: datetime, prev_end: datetime
) -> tuple[dict[str, int], dict[str, int]]:
    """Заказы FBO+FBS по offer_id за два окна (точное время, как WB)."""
    since = min(prev_start, cur_start)
    until = max(prev_end, cur_end)
    fbo, fbs = [], []
    try:
        fbo = fetch_fbo_postings(ozon_post, since, until)
    except Exception as e:
        logger.warning("FBO postings for pace: %s", e)
    try:
        fbs = fetch_fbs_postings(ozon_post, since, until)
    except Exception as e:
        logger.warning("FBS postings for pace: %s", e)

    all_postings = fbo + fbs
    cur = _count_posting_products(all_postings, cur_start, cur_end)
    prev = _count_posting_products(all_postings, prev_start, prev_end)
    logger.info("pace orders: postings=%s cur_offers=%s prev_offers=%s", len(all_postings), len(cur), len(prev))
    return cur, prev


def fetch_analytics_sku(
    ozon_post: Callable, date_from: date, date_to: date, metrics: list[str]
) -> dict[str, dict]:
    """POST /v1/analytics/data → {sku_str: {metric: value}}."""
    by_sku: dict[str, dict] = {}
    offset = 0
    limit = 1000
    while True:
        payload = ozon_post(
            "/v1/analytics/data",
            {
                "date_from": date_from.isoformat(),
                "date_to": date_to.isoformat(),
                "metrics": metrics,
                "dimension": ["sku"],
                "limit": limit,
                "offset": offset,
            },
        )
        result = payload.get("result") or payload
        rows = result.get("data") or []
        for row in rows:
            dims = row.get("dimensions") or []
            vals = row.get("metrics") or []
            sku = str(dims[0].get("id") or "") if dims else ""
            if not sku:
                continue
            entry = by_sku.setdefault(sku, {})
            for i, m in enumerate(metrics):
                try:
                    entry[m] = entry.get(m, 0) + float(vals[i] if i < len(vals) else 0)
                except (TypeError, ValueError, IndexError):
                    pass
        if len(rows) < limit:
            break
        offset += limit
        if offset > 50000:
            break
        time.sleep(0.25)
    return by_sku


def sync_sales_pace(
    ozon_post: Callable,
    load_stock_totals: Callable,
    load_products: Callable,
    period: str = "day",
    date_cur: str | None = None,
    date_prev: str | None = None,
    load_ads_sku: Callable | None = None,
    load_stock_index: Callable | None = None,
) -> dict:
    period = period if period in SALES_PACE_PERIODS else "day"
    if period != "day":
        date_cur = date_prev = None
    cache_key = _pace_cache_key(period, date_cur, date_prev)

    if not _pace_lock.acquire(blocking=False):
        return {"ok": False, "error": "sync already running", "syncing": True}
    SALES_PACE_CACHE["syncing"] = True
    SALES_PACE_CACHE["syncing_period"] = cache_key
    SALES_PACE_CACHE["error"] = None
    try:
        now = _msk_now()
        win = _pace_windows(period, now, date_cur, date_prev)
        cur_start, cur_end = win["cur_start"], win["cur_end"]
        prev_start, prev_end = win["prev_start"], win["prev_end"]

        cur_ord, prev_ord = fetch_orders_windows(ozon_post, cur_start, cur_end, prev_start, prev_end)

        # Воронка Premium: показы, сессии на карточке (=переходы), корзина
        funnel_metrics = ["hits_view", "session_view_pdp", "hits_tocart", "ordered_units"]
        funnel_cur, funnel_prev = {}, {}
        funnel_ready = True
        try:
            funnel_cur = fetch_analytics_sku(
                ozon_post, cur_start.date(), cur_end.date(), funnel_metrics
            )
            time.sleep(0.4)
            funnel_prev = fetch_analytics_sku(
                ozon_post, prev_start.date(), prev_end.date(), funnel_metrics
            )
        except Exception as e:
            logger.warning("pace funnel analytics: %s", e)
            funnel_ready = False

        # CPM / рекламные показы из Performance API (опционально)
        ads_cur, ads_prev = {}, {}
        ads_ready = False
        if load_ads_sku:
            try:
                ads_cur = load_ads_sku(cur_start.date(), cur_end.date()) or {}
                time.sleep(0.2)
                ads_prev = load_ads_sku(prev_start.date(), prev_end.date()) or {}
                ads_ready = bool(ads_cur or ads_prev)
            except Exception as e:
                logger.warning("pace ads sku: %s", e)
                ads_ready = False

        # stock + product meta
        stock_by_offer: dict[str, dict] = {}
        try:
            for r in load_stock_totals() or []:
                offer = str(r.get("offer_id") or "")
                if offer:
                    stock_by_offer[offer] = {
                        "stock": int(r.get("stock_total") or 0),
                        "warehouses": 0,
                        "sku": r.get("sku"),
                        "name": r.get("name") or "",
                        "product_id": r.get("product_id"),
                    }
        except Exception as e:
            logger.warning("pace stock: %s", e)
        if load_stock_index:
            try:
                for offer, info in (load_stock_index() or {}).items():
                    entry = stock_by_offer.setdefault(
                        str(offer),
                        {"stock": 0, "warehouses": 0, "sku": None, "name": "", "product_id": None},
                    )
                    if info.get("stock") is not None:
                        entry["stock"] = int(info.get("stock") or 0)
                    entry["warehouses"] = int(info.get("warehouses") or 0)
                    if info.get("sku") is not None:
                        entry["sku"] = info.get("sku")
                    if info.get("name"):
                        entry["name"] = info.get("name")
            except Exception as e:
                logger.warning("pace stock index: %s", e)

        sku_to_offer: dict[str, str] = {}
        name_by_offer: dict[str, str] = {}
        try:
            for p in load_products() or []:
                offer = str(p.get("offer_id") or "")
                if not offer:
                    continue
                name_by_offer[offer] = p.get("name") or name_by_offer.get(offer) or ""
                if p.get("sku") is not None:
                    sku_to_offer[str(p["sku"])] = offer
                if offer not in stock_by_offer:
                    stock_by_offer[offer] = {
                        "stock": 0,
                        "sku": p.get("sku"),
                        "name": p.get("name") or "",
                        "product_id": p.get("product_id"),
                    }
        except Exception as e:
            logger.warning("pace products: %s", e)

        def _offer_from_sku(sku: str) -> str:
            return sku_to_offer.get(str(sku)) or f"sku:{sku}"

        # merge analytics funnel onto offers
        def funnel_for_offer(offer: str) -> tuple[dict, dict]:
            st = stock_by_offer.get(offer) or {}
            sku = st.get("sku")
            fc, fp = {}, {}
            if sku is not None and str(sku) in funnel_cur:
                fc = funnel_cur[str(sku)]
            if sku is not None and str(sku) in funnel_prev:
                fp = funnel_prev[str(sku)]
            return fc, fp

        # also map funnel skus that appear only in analytics
        for sku in set(funnel_cur) | set(funnel_prev):
            offer = _offer_from_sku(sku)
            if offer not in stock_by_offer:
                stock_by_offer[offer] = {"stock": 0, "sku": int(sku) if str(sku).isdigit() else sku, "name": "", "product_id": None}

        period_days = {"day": 1, "week": 7, "weeks2": 14, "month": 30}.get(period, 1)
        all_offers = set(cur_ord) | set(prev_ord)
        # include offers with funnel activity even if postings empty
        for sku in set(funnel_cur) | set(funnel_prev):
            all_offers.add(_offer_from_sku(sku))

        articles = []
        for offer in all_offers:
            o_t = int(cur_ord.get(offer, 0))
            o_y = int(prev_ord.get(offer, 0))
            # fallback orders from analytics if postings empty for this offer
            fc, fp = funnel_for_offer(offer)
            if o_t == 0 and o_y == 0:
                o_t = int(fc.get("ordered_units") or 0)
                o_y = int(fp.get("ordered_units") or 0)
            if o_t <= 0 and o_y <= 0:
                continue

            opens_t = int(fc.get("hits_view") or 0)
            opens_y = int(fp.get("hits_view") or 0)
            clicks_t = int(fc.get("session_view_pdp") or 0)
            clicks_y = int(fp.get("session_view_pdp") or 0)
            cart_t = int(fc.get("hits_tocart") or 0)
            cart_y = int(fp.get("hits_tocart") or 0)

            def _cr(num, den):
                if not den:
                    return None
                return round(100.0 * float(num) / float(den), 1)

            # CR как на WB: корзина ÷ переходы в карточку; fallback ÷ показы
            cr_den_t = clicks_t if clicks_t > 0 else opens_t
            cr_den_y = clicks_y if clicks_y > 0 else opens_y
            cr_t = _cr(cart_t, cr_den_t)
            cr_y = _cr(cart_y, cr_den_y)
            cr_delta = None
            if cr_t is not None and cr_y is not None:
                cr_delta = round(cr_t - cr_y, 1)

            meta = stock_by_offer.get(offer) or {}
            sku = meta.get("sku")
            sku_key = str(sku) if sku is not None else ""
            ac = ads_cur.get(sku_key) or {}
            ap = ads_prev.get(sku_key) or {}
            views_t = int(ac.get("views") or 0)
            views_y = int(ap.get("views") or 0)
            spend_t = float(ac.get("expense") or 0)
            spend_y = float(ap.get("expense") or 0)
            cpm_t = ac.get("cpm")
            cpm_y = ap.get("cpm")
            if cpm_t is None and views_t > 0:
                cpm_t = round(spend_t / views_t * 1000, 1)
            if cpm_y is None and views_y > 0:
                cpm_y = round(spend_y / views_y * 1000, 1)
            cpm_delta = None
            if ads_ready and cpm_t is not None and cpm_y is not None:
                cpm_delta = round(cpm_t - cpm_y, 1)

            stock_qty = int(meta.get("stock") or 0)
            wh_count = int(meta.get("warehouses") or 0)
            daily_orders = max(o_t, o_y, 0) / period_days if period_days else 0
            days_left = round(stock_qty / daily_orders, 1) if daily_orders > 0 else None
            if stock_qty <= 0:
                stock_flag = "oos"
            elif days_left is not None and days_left < 5:
                stock_flag = "low"
            else:
                stock_flag = "ok"

            opens_delta = (opens_t - opens_y) if funnel_ready else None
            clicks_delta = (clicks_t - clicks_y) if funnel_ready else None
            cart_delta = (cart_t - cart_y) if funnel_ready else None
            orders_delta = o_t - o_y
            funnel_down = False
            if funnel_ready:
                funnel_down = (
                    (opens_delta is not None and opens_delta < 0)
                    or (clicks_delta is not None and clicks_delta < 0)
                    or (cart_delta is not None and cart_delta < 0)
                    or (cr_delta is not None and cr_delta <= -1)
                )
            orders_down = orders_delta < 0
            stock_linked = (funnel_down or orders_down) and stock_flag in ("oos", "low")

            articles.append(
                {
                    "offer_id": offer,
                    "vendor_code": offer,
                    "nm_id": meta.get("product_id") or meta.get("sku"),
                    "sku": meta.get("sku"),
                    "name": meta.get("name") or name_by_offer.get(offer) or "",
                    "orders_today": o_t,
                    "orders_yesterday": o_y,
                    "orders_delta": orders_delta,
                    "opens_today": opens_t if funnel_ready else None,
                    "opens_yesterday": opens_y if funnel_ready else None,
                    "opens_delta": opens_delta,
                    "clicks_today": clicks_t if funnel_ready else None,
                    "clicks_yesterday": clicks_y if funnel_ready else None,
                    "clicks_delta": clicks_delta,
                    "cart_today": cart_t if funnel_ready else None,
                    "cart_yesterday": cart_y if funnel_ready else None,
                    "cart_delta": cart_delta,
                    "cart_cr_today": cr_t if funnel_ready else None,
                    "cart_cr_yesterday": cr_y if funnel_ready else None,
                    "cart_cr_delta": cr_delta if funnel_ready else None,
                    "views_today": views_t if ads_ready else None,
                    "views_yesterday": views_y if ads_ready else None,
                    "views_delta": (views_t - views_y) if ads_ready else None,
                    "spend_today": spend_t if ads_ready else None,
                    "spend_yesterday": spend_y if ads_ready else None,
                    "cpm_today": cpm_t if ads_ready else None,
                    "cpm_yesterday": cpm_y if ads_ready else None,
                    "cpm_delta": cpm_delta,
                    "stock": stock_qty,
                    "warehouses": wh_count,
                    "days_left": days_left,
                    "stock_flag": stock_flag,
                    "stock_linked": stock_linked,
                    "funnel_down": funnel_down,
                    "funnel_compare_ready": funnel_ready,
                    "ads_compare_ready": ads_ready,
                }
            )

        articles.sort(key=lambda a: a.get("orders_delta") or 0)

        payload = {
            "period": period,
            "cache_key": cache_key,
            "articles": articles,
            "as_of": now.strftime("%Y-%m-%d %H:%M"),
            "compare_as_of": None,
            "label_cur": win["label_cur"],
            "label_prev": win["label_prev"],
            "col_cur": win["col_cur"],
            "col_prev": win["col_prev"],
            "custom_dates": win.get("custom_dates", False),
            "date_cur": date_cur,
            "date_prev": date_prev,
            "today": now.strftime("%Y-%m-%d"),
            "yesterday": (now.date() - timedelta(days=1)).isoformat(),
            "now_time": now.strftime("%H:%M"),
            "updated_at": now.isoformat(),
            "funnel_ready": funnel_ready,
            "ads_ready": ads_ready,
            "syncing": False,
            "error": None,
        }
        SALES_PACE_CACHE["by_period"][cache_key] = payload
        logger.info("sales-pace[%s]: %s arts", cache_key, len(articles))
        return payload
    except Exception as e:
        logger.exception("sync_sales_pace failed")
        SALES_PACE_CACHE["error"] = str(e)
        raise
    finally:
        SALES_PACE_CACHE["syncing"] = False
        SALES_PACE_CACHE["syncing_period"] = None
        _pace_lock.release()


def get_cached_pace(period: str, date_cur=None, date_prev=None) -> dict | None:
    period = period if period in SALES_PACE_PERIODS else "day"
    if period != "day":
        date_cur = date_prev = None
    key = _pace_cache_key(period, date_cur, date_prev)
    data = SALES_PACE_CACHE["by_period"].get(key)
    if not data:
        return None
    out = dict(data)
    out["syncing"] = bool(SALES_PACE_CACHE.get("syncing") and SALES_PACE_CACHE.get("syncing_period") == key)
    if SALES_PACE_CACHE.get("error") and not out.get("articles"):
        out["error"] = SALES_PACE_CACHE["error"]
    return out
