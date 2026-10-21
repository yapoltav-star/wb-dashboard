"""Заказы — FBO v3 + FBS v4 отправления."""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import Callable

from sales_pace import MSK, fetch_fbo_postings, fetch_fbs_postings

logger = logging.getLogger("ozon-dashboard.orders")

ORDERS_CACHE: dict = {
    "postings": [],
    "updated_at": None,
    "syncing": False,
    "error": None,
    "days": 7,
    "since": None,
    "until": None,
}

_lock = threading.Lock()


def _norm_posting(p: dict, channel: str) -> dict:
    products = []
    for prod in p.get("products") or []:
        products.append({
            "offer_id": prod.get("offer_id") or "",
            "sku": prod.get("sku"),
            "name": prod.get("name") or "",
            "quantity": int(prod.get("quantity") or 1),
            "price": prod.get("price"),
        })
    qty = sum(x["quantity"] for x in products)
    return {
        "channel": channel,
        "posting_number": p.get("posting_number") or "",
        "order_number": p.get("order_number") or "",
        "order_id": p.get("order_id"),
        "status": p.get("status") or "",
        "substatus": p.get("substatus") or "",
        "created_at": p.get("in_process_at") or p.get("created_at") or p.get("shipment_date"),
        "shipment_date": p.get("shipment_date"),
        "warehouse": (
            ((p.get("delivery_method") or {}) if isinstance(p.get("delivery_method"), dict) else {}).get("warehouse")
            or ((p.get("analytics_data") or {}) if isinstance(p.get("analytics_data"), dict) else {}).get("warehouse_name")
            or ""
        ),
        "products": products,
        "qty": qty,
        "offer_ids": [x["offer_id"] for x in products if x["offer_id"]],
    }


def sync_orders(ozon_post: Callable, days: int = 7) -> dict:
    if not _lock.acquire(blocking=False):
        ORDERS_CACHE["syncing"] = True
        return {"ok": False, "syncing": True}
    ORDERS_CACHE["syncing"] = True
    ORDERS_CACHE["error"] = None
    try:
        days = max(1, min(int(days or 7), 30))
        now = datetime.now(MSK)
        since = (now - timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)
        until = now
        fbo, fbs = [], []
        try:
            fbo = fetch_fbo_postings(ozon_post, since, until)
        except Exception as e:
            logger.warning("orders FBO: %s", e)
        try:
            fbs = fetch_fbs_postings(ozon_post, since, until)
        except Exception as e:
            logger.warning("orders FBS: %s", e)

        rows = [_norm_posting(p, "FBO") for p in fbo] + [_norm_posting(p, "FBS") for p in fbs]
        rows.sort(key=lambda r: r.get("created_at") or "", reverse=True)

        ORDERS_CACHE.update({
            "postings": rows,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "syncing": False,
            "error": None,
            "days": days,
            "since": since.isoformat(),
            "until": until.isoformat(),
            "count_fbo": len(fbo),
            "count_fbs": len(fbs),
        })
        return {"ok": True, "count": len(rows)}
    except Exception as e:
        logger.exception("orders sync")
        ORDERS_CACHE["error"] = str(e)
        ORDERS_CACHE["syncing"] = False
        return {"ok": False, "error": str(e)}
    finally:
        _lock.release()


def get_cached() -> dict:
    return {
        "postings": ORDERS_CACHE.get("postings") or [],
        "updated_at": ORDERS_CACHE.get("updated_at"),
        "syncing": bool(ORDERS_CACHE.get("syncing")),
        "error": ORDERS_CACHE.get("error"),
        "days": ORDERS_CACHE.get("days") or 7,
        "since": ORDERS_CACHE.get("since"),
        "until": ORDERS_CACHE.get("until"),
        "count_fbo": ORDERS_CACHE.get("count_fbo") or 0,
        "count_fbs": ORDERS_CACHE.get("count_fbs") or 0,
    }
