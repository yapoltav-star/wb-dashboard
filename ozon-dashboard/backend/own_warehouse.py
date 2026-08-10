"""Остатки нашего склада — с WB-дашборда (уже с учётом загруженных отгрузок).

Не читаем Google Sheets напрямую: источник правды —
GET {WB_DASHBOARD_URL}/api/own-warehouse-stock
(там sheet + списания поставок/отгрузок из вкладки «Наш склад»).

Матчинг к Ozon: vendor_code ↔ offer_id.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any

import httpx

logger = logging.getLogger("ozon-dashboard.own-warehouse")

# production WB dashboard (как в frontend/index.html)
WB_DASHBOARD_URL = os.getenv(
    "WB_DASHBOARD_URL",
    "https://wb-dashboard-production-baf4.up.railway.app",
).rstrip("/")

OWN_WAREHOUSE_CACHE: dict[str, Any] = {
    "title": None,
    "as_of": None,
    "rows": [],
    "by_vendor": {},
    "shipments": [],
    "channel_summaries": [],
    "updated_at": None,
    "error": None,
    "syncing": False,
    "configured": bool(WB_DASHBOARD_URL),
    "source": "wb-dashboard",
    "wb_url": WB_DASHBOARD_URL or None,
}

_lock = threading.Lock()


def _normalize_payload(data: dict) -> dict:
    """Приводим ответ WB к полям, которые ждёт Ozon UI."""
    by_vendor = data.get("by_vendor") or {}
    # нормализуем ключи к строкам
    by_vendor = {str(k): v for k, v in by_vendor.items() if k is not None}

    rows = data.get("rows") or []
    norm_rows = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        vc = r.get("vendor_code")
        meta = by_vendor.get(str(vc), {}) if vc else {}
        stock = r.get("stock")
        if stock is None:
            stock = meta.get("stock", 0)
        fam = r.get("family_stock")
        if fam is None:
            fam = meta.get("family_stock", stock)
        norm_rows.append({
            "vendor_code": vc,
            "name": r.get("name") or r.get("model_name"),
            "stock": int(stock or 0),
            "family_stock": int(fam or 0) if fam is not None else int(stock or 0),
            "family": r.get("family") or meta.get("family") or ([vc] if vc else []),
            "root": r.get("model_root") or r.get("root") or meta.get("root"),
            "note": r.get("note"),
            "stock_sheet": r.get("stock_sheet"),
            "shipped": r.get("shipped") or 0,
        })

    return {
        "title": data.get("title") or "Наш склад (WB)",
        "as_of": data.get("as_of"),
        "rows": norm_rows,
        "by_vendor": by_vendor,
        "shipments": data.get("shipments") or [],
        "channel_summaries": data.get("channel_summaries") or [],
        "updated_at": data.get("updated_at"),
        "error": data.get("error"),
        "syncing": bool(data.get("syncing")),
        "configured": True,
        "source": "wb-dashboard",
        "wb_url": WB_DASHBOARD_URL,
    }


def fetch_from_wb_dashboard(*, refresh: bool = False, timeout: float = 60) -> dict:
    """Тянет остатки с живого WB-дашборда (с учётом отгрузок)."""
    if not WB_DASHBOARD_URL:
        raise RuntimeError(
            "WB_DASHBOARD_URL не задан — укажи URL WB-дашборда в Railway Variables"
        )

    if refresh:
        # просим WB пересобрать кэш (sheet + отгрузки)
        try:
            sync = httpx.post(
                f"{WB_DASHBOARD_URL}/api/sync-own-warehouse",
                timeout=30,
                follow_redirects=True,
            )
            if sync.status_code >= 400:
                logger.warning(
                    "WB sync-own-warehouse HTTP %s: %s",
                    sync.status_code,
                    sync.text[:200],
                )
            else:
                # даём WB время на refresh в фоне
                time.sleep(1.2)
        except Exception as e:
            logger.warning("WB sync-own-warehouse: %s", e)

    url = f"{WB_DASHBOARD_URL}/api/own-warehouse-stock"
    params = {"refresh": "true" if refresh else "false"}
    last_err = None
    for attempt in range(3):
        try:
            resp = httpx.get(url, params=params, timeout=timeout, follow_redirects=True)
            if resp.status_code >= 400:
                raise RuntimeError(f"WB dashboard HTTP {resp.status_code}: {resp.text[:200]}")
            data = resp.json()
            if not isinstance(data, dict):
                raise RuntimeError("WB dashboard вернул не JSON-объект")
            # если WB ещё синкает — подождём и повторим
            if data.get("syncing") and attempt < 2:
                time.sleep(1.5)
                continue
            return _normalize_payload(data)
        except Exception as e:
            last_err = e
            logger.warning("WB own-warehouse attempt %s: %s", attempt + 1, e)
            time.sleep(0.8)
    raise RuntimeError(f"Не удалось получить остатки с WB: {last_err}")


def refresh_own_warehouse_stock(*, force_wb_refresh: bool = True) -> dict:
    if not _lock.acquire(blocking=False):
        OWN_WAREHOUSE_CACHE["syncing"] = True
        return {**OWN_WAREHOUSE_CACHE, "syncing": True}
    OWN_WAREHOUSE_CACHE["syncing"] = True
    OWN_WAREHOUSE_CACHE["error"] = None
    try:
        data = fetch_from_wb_dashboard(refresh=force_wb_refresh)
        OWN_WAREHOUSE_CACHE.update(data)
        OWN_WAREHOUSE_CACHE["syncing"] = False
        logger.info(
            "own-warehouse from WB: %s rows, as_of=%s, shipments=%s",
            len(data.get("rows") or []),
            data.get("as_of"),
            len(data.get("shipments") or []),
        )
        return dict(OWN_WAREHOUSE_CACHE)
    except Exception as e:
        logger.exception("own-warehouse WB refresh")
        OWN_WAREHOUSE_CACHE["syncing"] = False
        OWN_WAREHOUSE_CACHE["error"] = str(e)
        return dict(OWN_WAREHOUSE_CACHE)
    finally:
        _lock.release()


def get_cached(refresh: bool = False) -> dict:
    if refresh or not OWN_WAREHOUSE_CACHE.get("rows"):
        if OWN_WAREHOUSE_CACHE.get("syncing"):
            return {**OWN_WAREHOUSE_CACHE, "syncing": True}
        return refresh_own_warehouse_stock(force_wb_refresh=refresh)
    return {
        "title": OWN_WAREHOUSE_CACHE.get("title"),
        "as_of": OWN_WAREHOUSE_CACHE.get("as_of"),
        "rows": OWN_WAREHOUSE_CACHE.get("rows") or [],
        "by_vendor": OWN_WAREHOUSE_CACHE.get("by_vendor") or {},
        "shipments": OWN_WAREHOUSE_CACHE.get("shipments") or [],
        "channel_summaries": OWN_WAREHOUSE_CACHE.get("channel_summaries") or [],
        "updated_at": OWN_WAREHOUSE_CACHE.get("updated_at"),
        "error": OWN_WAREHOUSE_CACHE.get("error"),
        "syncing": bool(OWN_WAREHOUSE_CACHE.get("syncing")),
        "configured": bool(WB_DASHBOARD_URL),
        "source": "wb-dashboard",
        "wb_url": WB_DASHBOARD_URL,
    }


def lookup_for_offer(offer_id: str) -> dict | None:
    vc = str(offer_id or "").strip()
    if not vc:
        return None
    by_v = OWN_WAREHOUSE_CACHE.get("by_vendor") or {}
    hit = by_v.get(vc)
    if hit:
        return hit
    low = vc.lower()
    for k, v in by_v.items():
        if str(k).lower() == low:
            return v
    return None
