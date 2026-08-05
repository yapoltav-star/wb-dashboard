"""Отзывы — ReviewAPI v2 + рейтинги по товарам + склейки."""

from __future__ import annotations

import json
import logging
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Callable

logger = logging.getLogger("ozon-dashboard.reviews")

GROUPS_SETTING_KEY = "ozon_review_groups"
NEG_PERIODS = (1, 2, 3, 4, 5, 7, 14, 30)

REVIEWS_CACHE: dict = {
    "reviews": [],
    "ratings": [],
    "negative_counts": {},
    "groups": {},
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


def fetch_reviews(ozon_post: Callable, status: str | None = None, max_pages: int = 50) -> list[dict]:
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


def build_sku_map(products: list[dict]) -> dict[str, dict]:
    """sku (str) → карточка товара."""
    out: dict[str, dict] = {}
    for p in products or []:
        sku = p.get("sku")
        if sku is None or sku == "":
            continue
        key = str(sku)
        out[key] = {
            "offer_id": p.get("offer_id") or "",
            "name": p.get("name") or "",
            "product_id": p.get("product_id"),
            "primary_image": p.get("primary_image") or "",
            "sku": sku,
        }
    return out


def enrich_reviews(reviews: list[dict], sku_map: dict[str, dict]) -> list[dict]:
    for r in reviews:
        info = sku_map.get(str(r.get("sku") or "")) or {}
        r["offer_id"] = info.get("offer_id") or ""
        r["name"] = info.get("name") or ""
        r["product_id"] = info.get("product_id")
        r["primary_image"] = info.get("primary_image") or ""
        # ключ агрегации: артикул продавца, иначе SKU
        r["article"] = r["offer_id"] or str(r.get("sku") or "")
    return reviews


def _parse_dt(raw) -> datetime | None:
    if not raw:
        return None
    try:
        s = str(raw).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def compute_ratings(reviews: list[dict], products: list[dict] | None = None) -> list[dict]:
    """Рейтинг каждого товара отдельно (по offer_id / sku), без склейки."""
    buckets: dict[str, dict] = {}
    for r in reviews:
        art = r.get("article") or r.get("offer_id") or str(r.get("sku") or "")
        if not art:
            continue
        b = buckets.get(art)
        if not b:
            b = {
                "article": art,
                "offer_id": r.get("offer_id") or "",
                "sku": r.get("sku"),
                "name": r.get("name") or "",
                "primary_image": r.get("primary_image") or "",
                "r5": 0, "r4": 0, "r3": 0, "r2": 0, "r1": 0,
                "total": 0,
                "star_sum": 0,
            }
            buckets[art] = b
        stars = int(r.get("rating") or 0)
        if stars < 1 or stars > 5:
            continue
        b[f"r{stars}"] += 1
        b["total"] += 1
        b["star_sum"] += stars

    # товары без отзывов — тоже в список (для склеек / менеджера)
    for p in products or []:
        art = p.get("offer_id") or str(p.get("sku") or "")
        if not art or art in buckets:
            continue
        if p.get("archived"):
            continue
        buckets[art] = {
            "article": art,
            "offer_id": p.get("offer_id") or "",
            "sku": p.get("sku"),
            "name": p.get("name") or "",
            "primary_image": p.get("primary_image") or "",
            "r5": 0, "r4": 0, "r3": 0, "r2": 0, "r1": 0,
            "total": 0,
            "star_sum": 0,
        }

    out = []
    for b in buckets.values():
        total = b["total"]
        avg = round(b["star_sum"] / total, 2) if total else None
        out.append({
            "article": b["article"],
            "offer_id": b["offer_id"],
            "sku": b["sku"],
            "name": b["name"],
            "primary_image": b["primary_image"],
            "avg_rating": avg,
            "r5": b["r5"], "r4": b["r4"], "r3": b["r3"], "r2": b["r2"], "r1": b["r1"],
            "total": total,
            "excluded": 0,
        })
    out.sort(key=lambda x: (-(x["avg_rating"] or 0), -(x["total"] or 0), str(x["article"])))
    return out


def compute_negative_counts(reviews: list[dict], max_stars: int = 3) -> dict[str, list[dict]]:
    """Период (дни) → [{article, negative_count}]."""
    now = datetime.now(timezone.utc)
    result: dict[str, list[dict]] = {}
    for days in NEG_PERIODS:
        since = now - timedelta(days=days)
        counts: dict[str, int] = defaultdict(int)
        for r in reviews:
            stars = int(r.get("rating") or 0)
            if stars < 1 or stars > max_stars:
                continue
            dt = _parse_dt(r.get("published_at"))
            if not dt or dt < since:
                continue
            art = r.get("article") or r.get("offer_id") or str(r.get("sku") or "")
            if art:
                counts[art] += 1
        result[str(days)] = [
            {"article": a, "negative_count": n}
            for a, n in sorted(counts.items(), key=lambda x: -x[1])
        ]
    return result


def load_groups(get_setting: Callable) -> dict[str, list[str]]:
    raw = get_setting(GROUPS_SETTING_KEY, "{}")
    try:
        data = json.loads(raw) if isinstance(raw, str) else (raw or {})
    except Exception:
        data = {}
    if not isinstance(data, dict):
        return {}
    out: dict[str, list[str]] = {}
    for name, arts in data.items():
        if not name or not isinstance(arts, list):
            continue
        out[str(name)] = [str(a) for a in arts if a]
    return out


def save_groups(save_setting: Callable, groups: dict) -> dict[str, list[str]]:
    cleaned: dict[str, list[str]] = {}
    for name, arts in (groups or {}).items():
        if not name or name == "Без склейки":
            continue
        if not isinstance(arts, list):
            continue
        cleaned[str(name)] = [str(a) for a in arts if a]
    save_setting(GROUPS_SETTING_KEY, cleaned)
    REVIEWS_CACHE["groups"] = cleaned
    return cleaned


def filter_article_reviews(
    article: str,
    days: int = 5,
    stars: list[int] | None = None,
    limit: int = 50,
) -> list[dict]:
    stars_set = set(stars or [1, 2, 3])
    days = max(1, min(int(days or 5), 90))
    since = datetime.now(timezone.utc) - timedelta(days=days)
    out = []
    for r in REVIEWS_CACHE.get("reviews") or []:
        art = r.get("article") or r.get("offer_id") or str(r.get("sku") or "")
        if art != article:
            continue
        st = int(r.get("rating") or 0)
        if st not in stars_set:
            continue
        dt = _parse_dt(r.get("published_at"))
        if not dt or dt < since:
            continue
        out.append({
            "id": r.get("id"),
            "stars": st,
            "created_date": r.get("published_at"),
            "text": r.get("text") or "",
            "status": r.get("status") or "",
            "sku": r.get("sku"),
            "comments_amount": r.get("comments_amount") or 0,
        })
    out.sort(key=lambda x: x.get("created_date") or "", reverse=True)
    return out[:limit]


def sync_reviews(
    ozon_post: Callable,
    status: str = "ALL",
    products: list[dict] | None = None,
    get_setting: Callable | None = None,
) -> dict:
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
                    "ratings": [],
                    "negative_counts": {},
                    "counts": {},
                    "syncing": False,
                    "premium_required": True,
                    "error": "Нужна подписка «Управление отзывами» или Premium Pro",
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                })
                return {"ok": False, "premium_required": True}
            raise

        # для рейтингов и негатива всегда тянем все статусы
        reviews = fetch_reviews(ozon_post, status=None)
        sku_map = build_sku_map(products or [])
        enrich_reviews(reviews, sku_map)
        ratings = compute_ratings(reviews, products=products)
        neg = compute_negative_counts(reviews)
        groups = load_groups(get_setting) if get_setting else (REVIEWS_CACHE.get("groups") or {})

        REVIEWS_CACHE.update({
            "reviews": reviews,
            "ratings": ratings,
            "negative_counts": neg,
            "groups": groups,
            "counts": counts,
            "status_filter": status,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "syncing": False,
            "error": None,
            "premium_required": False,
        })
        return {"ok": True, "count": len(reviews), "ratings": len(ratings)}
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
        "ratings": REVIEWS_CACHE.get("ratings") or [],
        "negative_counts": REVIEWS_CACHE.get("negative_counts") or {},
        "groups": REVIEWS_CACHE.get("groups") or {},
        "counts": REVIEWS_CACHE.get("counts") or {},
        "updated_at": REVIEWS_CACHE.get("updated_at"),
        "syncing": bool(REVIEWS_CACHE.get("syncing")),
        "error": REVIEWS_CACHE.get("error"),
        "premium_required": bool(REVIEWS_CACHE.get("premium_required")),
        "status_filter": REVIEWS_CACHE.get("status_filter") or "ALL",
    }
