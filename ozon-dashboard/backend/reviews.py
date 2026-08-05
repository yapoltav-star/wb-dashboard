"""Отзывы — ReviewAPI v2 (нужна подписка «Управление отзывами» или Premium Pro)."""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Callable

logger = logging.getLogger("ozon-dashboard.reviews")

REVIEWS_CACHE: dict = {
    "reviews": [],
    "counts": {},
    "updated_at": None,
    "syncing": False,
    "error": None,
    "premium_required": False,
}

_lock = threading.Lock()


def fetch_review_counts(ozon_post: Callable) -> dict:
    payload = ozon_post("/v2/review/count", {})
    return {
        "total": int(payload.get("total") or 0),
        "new": int(payload.get("new") or 0),
        "viewed": int(payload.get("viewed") or 0),
        "processed": int(payload.get("processed") or 0),
    }


def fetch_reviews(ozon_post: Callable, status: str | None = None, max_pages: int = 20) -> list[dict]:
    out = []
    last_id = ""
    for _ in range(max_pages):
        body: dict = {"limit": 100, "sort_dir": "DESC"}
        filters = {}
        if status and status != "ALL":
            filters["status"] = status
        if filters:
            body["filters"] = filters
        if last_id:
            body["last_id"] = last_id
        payload = ozon_post("/v2/review/list", body)
        batch = payload.get("reviews") or []
        for r in batch:
            out.append({
                "id": r.get("id"),
                "sku": r.get("sku"),
                "rating": r.get("rating"),
                "text": r.get("text") or "",
                "status": r.get("status") or "",
                "published_at": r.get("published_at"),
                "order_status": r.get("order_status"),
                "photos_amount": r.get("photos_amount") or 0,
                "videos_amount": r.get("videos_amount") or 0,
                "comments_amount": r.get("comments_amount") or 0,
                "is_rating_participant": bool(r.get("is_rating_participant")),
            })
        last_id = payload.get("last_id") or ""
        if not batch or not payload.get("has_next"):
            break
        time.sleep(0.1)
    return out


def sync_reviews(ozon_post: Callable, status: str = "ALL") -> dict:
    if not _lock.acquire(blocking=False):
        REVIEWS_CACHE["syncing"] = True
        return {"ok": False, "syncing": True}
    REVIEWS_CACHE["syncing"] = True
    REVIEWS_CACHE["error"] = None
    REVIEWS_CACHE["premium_required"] = False
    try:
        try:
            counts = fetch_review_counts(ozon_post)
        except Exception as e:
            msg = str(e)
            if any(x in msg for x in ("403", "7 ", "Permission", "Premium", "подписк", "subscription")):
                REVIEWS_CACHE.update({
                    "reviews": [],
                    "counts": {},
                    "syncing": False,
                    "premium_required": True,
                    "error": "Нужна подписка «Управление отзывами» или Premium Pro",
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                })
                return {"ok": False, "premium_required": True}
            raise

        reviews = fetch_reviews(ozon_post, status=status if status != "ALL" else None)
        REVIEWS_CACHE.update({
            "reviews": reviews,
            "counts": counts,
            "status_filter": status,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "syncing": False,
            "error": None,
            "premium_required": False,
        })
        return {"ok": True, "count": len(reviews)}
    except Exception as e:
        logger.exception("reviews sync")
        REVIEWS_CACHE["error"] = str(e)
        REVIEWS_CACHE["syncing"] = False
        return {"ok": False, "error": str(e)}
    finally:
        _lock.release()


def get_cached() -> dict:
    return {
        "reviews": REVIEWS_CACHE.get("reviews") or [],
        "counts": REVIEWS_CACHE.get("counts") or {},
        "updated_at": REVIEWS_CACHE.get("updated_at"),
        "syncing": bool(REVIEWS_CACHE.get("syncing")),
        "error": REVIEWS_CACHE.get("error"),
        "premium_required": bool(REVIEWS_CACHE.get("premium_required")),
        "status_filter": REVIEWS_CACHE.get("status_filter") or "ALL",
    }
