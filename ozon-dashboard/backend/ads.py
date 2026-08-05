"""Реклама — Ozon Performance API (отдельные client_id / client_secret)."""

from __future__ import annotations

import logging
import os
import threading
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import httpx

logger = logging.getLogger("ozon-dashboard.ads")

PERF_API = "https://api-performance.ozon.ru"

ADS_CACHE: dict = {
    "campaigns": [],
    "daily": [],
    "expense": [],
    "updated_at": None,
    "syncing": False,
    "error": None,
    "configured": False,
}

_token: dict[str, Any] = {"access_token": None, "expires_at": 0.0}
_lock = threading.Lock()


def perf_configured() -> bool:
    return bool(os.getenv("OZON_PERF_CLIENT_ID", "").strip() and os.getenv("OZON_PERF_CLIENT_SECRET", "").strip())


def _get_token(force: bool = False) -> str:
    cid = os.getenv("OZON_PERF_CLIENT_ID", "").strip()
    secret = os.getenv("OZON_PERF_CLIENT_SECRET", "").strip()
    if not cid or not secret:
        raise RuntimeError(
            "Задай OZON_PERF_CLIENT_ID и OZON_PERF_CLIENT_SECRET "
            "(кабинет → API-ключи → вкладка Performance API)"
        )
    now = time.time()
    if not force and _token.get("access_token") and _token.get("expires_at", 0) > now + 60:
        return _token["access_token"]
    resp = httpx.post(
        f"{PERF_API}/api/client/token",
        json={"client_id": cid, "client_secret": secret, "grant_type": "client_credentials"},
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"Performance token: {resp.status_code} {resp.text[:300]}")
    data = resp.json()
    token = data.get("access_token")
    if not token:
        raise RuntimeError(f"Performance token empty: {data}")
    expires_in = int(data.get("expires_in") or 1800)
    _token["access_token"] = token
    _token["expires_at"] = now + expires_in
    return token


def perf_request(method: str, path: str, params: dict | None = None, json_body: dict | None = None, retries: int = 2):
    last_err = None
    for attempt in range(retries + 1):
        token = _get_token(force=attempt > 0)
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json", "Accept": "application/json"}
        url = f"{PERF_API}{path}"
        try:
            resp = httpx.request(method.upper(), url, headers=headers, params=params, json=json_body, timeout=60)
        except Exception as e:
            last_err = str(e)
            time.sleep(0.5)
            continue
        if resp.status_code == 401 and attempt < retries:
            _get_token(force=True)
            continue
        if resp.status_code == 429:
            time.sleep(1.5 * (attempt + 1))
            last_err = f"429 {resp.text[:200]}"
            continue
        if resp.status_code >= 400:
            raise RuntimeError(f"Performance {path}: {resp.status_code} {resp.text[:400]}")
        if resp.status_code == 204 or not resp.content:
            return {}
        return resp.json()
    raise RuntimeError(last_err or "Performance request failed")


def fetch_campaigns() -> list[dict]:
    out = []
    page = 1
    while page <= 20:
        data = perf_request("GET", "/api/client/campaign", params={"page": page, "pageSize": 100})
        # ответ может быть list или {list|campaigns|items}
        if isinstance(data, list):
            batch = data
        elif isinstance(data, dict):
            batch = data.get("list") or data.get("campaigns") or data.get("items") or data.get("result") or []
            if isinstance(batch, dict):
                batch = batch.get("campaigns") or batch.get("list") or []
        else:
            batch = []
        if not batch:
            break
        for c in batch:
            if not isinstance(c, dict):
                continue
            out.append({
                "id": c.get("id") or c.get("campaignId"),
                "title": c.get("title") or c.get("name") or "",
                "state": c.get("state") or c.get("status") or "",
                "advObjectType": c.get("advObjectType") or c.get("objectType") or "",
                "dailyBudget": c.get("dailyBudget") or c.get("budget"),
                "placement": c.get("placement"),
                "raw": {k: c.get(k) for k in ("id", "title", "state", "advObjectType", "fromDate", "toDate") if k in c},
            })
        if len(batch) < 100:
            break
        page += 1
    return out


def fetch_daily(date_from: str, date_to: str, campaign_ids: list | None = None) -> list:
    params: dict = {"dateFrom": date_from, "dateTo": date_to}
    if campaign_ids:
        params["campaignIds"] = ",".join(str(x) for x in campaign_ids[:50])
    data = perf_request("GET", "/api/client/statistics/daily", params=params)
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get("rows") or data.get("list") or data.get("statistics") or data.get("result") or []
    return []


def fetch_expense(date_from: str, date_to: str, campaign_ids: list | None = None) -> list:
    params: dict = {"dateFrom": date_from, "dateTo": date_to}
    if campaign_ids:
        params["campaignIds"] = ",".join(str(x) for x in campaign_ids[:50])
    data = perf_request("GET", "/api/client/statistics/expense", params=params)
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get("rows") or data.get("list") or data.get("expense") or data.get("result") or []
    return []


def sync_ads(period_days: int = 7) -> dict:
    if not _lock.acquire(blocking=False):
        ADS_CACHE["syncing"] = True
        return {"ok": False, "syncing": True}
    ADS_CACHE["syncing"] = True
    ADS_CACHE["error"] = None
    ADS_CACHE["configured"] = perf_configured()
    try:
        if not perf_configured():
            ADS_CACHE.update({
                "campaigns": [],
                "daily": [],
                "expense": [],
                "syncing": False,
                "error": "Нет OZON_PERF_CLIENT_ID / OZON_PERF_CLIENT_SECRET",
                "updated_at": datetime.now(timezone.utc).isoformat(),
            })
            return {"ok": False, "error": ADS_CACHE["error"]}

        period_days = max(1, min(int(period_days or 7), 30))
        today = date.today()
        date_from = (today - timedelta(days=period_days - 1)).isoformat()
        date_to = today.isoformat()

        campaigns = fetch_campaigns()
        ids = [c["id"] for c in campaigns if c.get("id")][:50]
        daily, expense = [], []
        try:
            daily = fetch_daily(date_from, date_to, ids or None)
        except Exception as e:
            logger.warning("ads daily: %s", e)
        try:
            expense = fetch_expense(date_from, date_to, ids or None)
        except Exception as e:
            logger.warning("ads expense: %s", e)

        ADS_CACHE.update({
            "campaigns": campaigns,
            "daily": daily if isinstance(daily, list) else [],
            "expense": expense if isinstance(expense, list) else [],
            "date_from": date_from,
            "date_to": date_to,
            "period_days": period_days,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "syncing": False,
            "error": None,
            "configured": True,
        })
        return {"ok": True, "campaigns": len(campaigns)}
    except Exception as e:
        logger.exception("ads sync")
        ADS_CACHE["error"] = str(e)
        ADS_CACHE["syncing"] = False
        return {"ok": False, "error": str(e)}
    finally:
        _lock.release()


def get_cached() -> dict:
    return {
        "campaigns": ADS_CACHE.get("campaigns") or [],
        "daily": ADS_CACHE.get("daily") or [],
        "expense": ADS_CACHE.get("expense") or [],
        "date_from": ADS_CACHE.get("date_from"),
        "date_to": ADS_CACHE.get("date_to"),
        "period_days": ADS_CACHE.get("period_days") or 7,
        "updated_at": ADS_CACHE.get("updated_at"),
        "syncing": bool(ADS_CACHE.get("syncing")),
        "error": ADS_CACHE.get("error"),
        "configured": bool(ADS_CACHE.get("configured") or perf_configured()),
    }
