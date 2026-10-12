"""Отзывы — ReviewAPI v2 + рейтинг каждой карточки (не склейки)."""

from __future__ import annotations

import io
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


def fetch_reviews(ozon_post: Callable, status: str | None = None, max_pages: int = 200) -> list[dict]:
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


def _product_skus(p: dict) -> list:
    """Все SKU одной карточки (FBO + FBS в sources) — это всё ещё одна карточка, не склейка."""
    skus: list = []
    if p.get("sku") not in (None, ""):
        skus.append(p.get("sku"))
    for s in p.get("skus") or []:
        if s not in (None, ""):
            skus.append(s)
    sources = p.get("sources") or []
    if isinstance(sources, list):
        for src in sources:
            if isinstance(src, dict):
                if src.get("sku") not in (None, ""):
                    skus.append(src.get("sku"))
            elif src not in (None, ""):
                skus.append(src)
    out, seen = [], set()
    for s in skus:
        k = str(s)
        if k not in seen:
            seen.add(k)
            out.append(s)
    return out


def _card_info(p: dict) -> dict:
    skus = _product_skus(p)
    sku = p.get("sku") if p.get("sku") not in (None, "") else (skus[0] if skus else None)
    return {
        "offer_id": p.get("offer_id") or "",
        "name": p.get("name") or "",
        "product_id": p.get("product_id") or p.get("id"),
        "primary_image": p.get("primary_image") or "",
        "sku": sku,
        "skus": [str(s) for s in skus],
        "archived": bool(p.get("archived") or p.get("is_archived")),
    }


def _card_key(info: dict, sku=None) -> str:
    """Идентичность одной карточки Ozon. Склейка сюда не входит."""
    pid = info.get("product_id")
    if pid not in (None, "", 0):
        return f"p:{pid}"
    oid = str(info.get("offer_id") or "").strip()
    if oid:
        return f"o:{oid}"
    return f"s:{sku if sku not in (None, '') else ''}"


def build_sku_map(products: list[dict]) -> dict[str, dict]:
    """sku (str) → карточка товара. Все SKU одной карточки указывают на неё же."""
    out: dict[str, dict] = {}
    for p in products or []:
        info = _card_info(p)
        for s in info["skus"] or ([info["sku"]] if info.get("sku") not in (None, "") else []):
            out[str(s)] = info
    return out


def _info_to_product(item: dict) -> dict:
    """Ответ /v3/product/info/list → карточка для рейтинга."""
    sources = item.get("sources") or []
    skus = []
    if isinstance(sources, list):
        for src in sources:
            if isinstance(src, dict) and src.get("sku") not in (None, ""):
                skus.append(src.get("sku"))
    if item.get("sku") not in (None, ""):
        skus.append(item.get("sku"))
    primary = item.get("primary_image")
    if isinstance(primary, list) and primary:
        primary = primary[0]
    if not isinstance(primary, str):
        images = item.get("images") or []
        first = images[0] if isinstance(images, list) and images else None
        primary = first if isinstance(first, str) else ""
    return {
        "product_id": item.get("id") or item.get("product_id"),
        "offer_id": item.get("offer_id") or "",
        "sku": skus[0] if skus else item.get("sku"),
        "skus": list(dict.fromkeys(skus)),
        "name": item.get("name") or "",
        "primary_image": primary or "",
        "archived": bool(item.get("is_archived")),
        "sources": sources if isinstance(sources, list) else [],
    }


def fetch_products_by_sku(ozon_post: Callable, skus: list, chunk: int = 100) -> list[dict]:
    """POST /v3/product/info/list по SKU — привязать отзыв к конкретной карточке."""
    ids, seen = [], set()
    for s in skus or []:
        if s in (None, ""):
            continue
        try:
            n = int(s)
        except (TypeError, ValueError):
            continue
        if n in seen:
            continue
        seen.add(n)
        ids.append(n)
    out: list[dict] = []
    for i in range(0, len(ids), chunk):
        payload = ozon_post("/v3/product/info/list", {"sku": ids[i : i + chunk]})
        items = payload.get("items") or (payload.get("result") or {}).get("items") or []
        for it in items:
            out.append(_info_to_product(it))
        time.sleep(0.05)
    return out


def merge_products(base: list[dict], extra: list[dict]) -> list[dict]:
    """Слить каталог и карточки, найденные по SKU отзывов. Ключ — product_id, иначе offer_id."""
    by_pid: dict = {}
    by_offer: dict = {}
    order: list[dict] = []

    def put(p: dict):
        info = _card_info(p)
        pid = info.get("product_id")
        oid = str(info.get("offer_id") or "").strip()
        if pid not in (None, "", 0) and pid in by_pid:
            cur = by_pid[pid]
            cur_skus = list(dict.fromkeys((cur.get("skus") or []) + (info.get("skus") or [])))
            cur["skus"] = cur_skus
            if not cur.get("sku") and info.get("sku"):
                cur["sku"] = info["sku"]
            if not cur.get("name") and info.get("name"):
                cur["name"] = info["name"]
            if not cur.get("primary_image") and info.get("primary_image"):
                cur["primary_image"] = info["primary_image"]
            return
        if oid and pid in (None, "", 0) and oid in by_offer:
            return
        row = {
            "product_id": pid,
            "offer_id": oid,
            "sku": info.get("sku"),
            "skus": info.get("skus") or [],
            "name": info.get("name") or "",
            "primary_image": info.get("primary_image") or "",
            "archived": info.get("archived"),
        }
        order.append(row)
        if pid not in (None, "", 0):
            by_pid[pid] = row
        if oid:
            by_offer[oid] = row

    for p in (base or []) + (extra or []):
        put(p)
    return order


def enrich_reviews(reviews: list[dict], sku_map: dict[str, dict]) -> list[dict]:
    for r in reviews:
        info = sku_map.get(str(r.get("sku") or "")) or {}
        r["offer_id"] = info.get("offer_id") or r.get("offer_id") or ""
        r["name"] = info.get("name") or r.get("name") or ""
        r["product_id"] = info.get("product_id") if info.get("product_id") is not None else r.get("product_id")
        r["primary_image"] = info.get("primary_image") or r.get("primary_image") or ""
        # ключ карточки: артикул продавца этой карточки, не склейки
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
    """Рейтинг каждой карточки отдельно (product_id / offer_id / sku). Склейки не суммируем.

    В средний ★ идут только отзывы с is_rating_participant=true — как на карточке Ozon.
    FBO и FBS SKU одной карточки остаются одной строкой; разные product_id — разные строки.
    """
    sku_map = build_sku_map(products or [])
    buckets: dict[str, dict] = {}

    def ensure(info: dict, sku=None) -> dict:
        key = _card_key(info, sku)
        b = buckets.get(key)
        if b:
            if sku not in (None, "") and str(sku) not in (b.get("skus") or []):
                b.setdefault("skus", []).append(str(sku))
            if not b.get("name") and info.get("name"):
                b["name"] = info["name"]
            if not b.get("primary_image") and info.get("primary_image"):
                b["primary_image"] = info["primary_image"]
            if not b.get("offer_id") and info.get("offer_id"):
                b["offer_id"] = info["offer_id"]
                b["article"] = info["offer_id"]
            return b
        sku_val = info.get("sku") if info.get("sku") not in (None, "") else sku
        b = {
            "article": info.get("offer_id") or str(sku_val or ""),
            "offer_id": info.get("offer_id") or "",
            "sku": sku_val,
            "skus": list(info.get("skus") or ([str(sku_val)] if sku_val not in (None, "") else [])),
            "name": info.get("name") or "",
            "primary_image": info.get("primary_image") or "",
            "product_id": info.get("product_id"),
            "r5": 0, "r4": 0, "r3": 0, "r2": 0, "r1": 0,
            "total": 0,
            "star_sum": 0,
            "excluded": 0,
            "reviews_all": 0,
        }
        buckets[key] = b
        return b

    for r in reviews:
        sku = r.get("sku")
        info = sku_map.get(str(sku or "")) or {
            "offer_id": r.get("offer_id") or "",
            "name": r.get("name") or "",
            "product_id": r.get("product_id"),
            "primary_image": r.get("primary_image") or "",
            "sku": sku,
            "skus": [str(sku)] if sku not in (None, "") else [],
        }
        if not _card_key(info, sku).lstrip("s:"):
            continue
        b = ensure(info, sku)
        stars = int(r.get("rating") or 0)
        b["reviews_all"] += 1
        # нет поля — считаем участвующим; false — исключён из рейтинга карточки
        if r.get("is_rating_participant") is False:
            b["excluded"] += 1
            continue
        if stars < 1 or stars > 5:
            continue
        b[f"r{stars}"] += 1
        b["total"] += 1
        b["star_sum"] += stars

    for p in products or []:
        if p.get("archived") or p.get("is_archived"):
            continue
        info = _card_info(p)
        if not info.get("product_id") and not info.get("offer_id") and not info.get("sku"):
            continue
        ensure(info, info.get("sku"))

    out = []
    for b in buckets.values():
        total = b["total"]
        avg = round(b["star_sum"] / total, 2) if total else None
        out.append({
            "article": b["article"],
            "offer_id": b["offer_id"],
            "sku": b["sku"],
            "skus": b.get("skus") or [],
            "name": b["name"],
            "primary_image": b["primary_image"],
            "product_id": b.get("product_id"),
            "avg_rating": avg,
            "r5": b["r5"], "r4": b["r4"], "r3": b["r3"], "r2": b["r2"], "r1": b["r1"],
            "total": total,
            "excluded": b.get("excluded") or 0,
            "reviews_all": b.get("reviews_all") or 0,
        })
    out.sort(key=lambda x: (
        0 if x["avg_rating"] is not None else 1,
        -(x["avg_rating"] or 0),
        -(x["total"] or 0),
        str(x["article"]),
    ))
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
        catalog = list(products or [])
        review_skus = [r.get("sku") for r in reviews if r.get("sku") not in (None, "")]
        try:
            resolved = fetch_products_by_sku(ozon_post, review_skus) if review_skus else []
        except Exception as e:
            logger.warning("reviews: product/info/list by sku: %s", e)
            resolved = []
        catalog = merge_products(catalog, resolved)
        sku_map = build_sku_map(catalog)
        enrich_reviews(reviews, sku_map)
        ratings = compute_ratings(reviews, products=catalog)
        neg = compute_negative_counts(reviews)
        groups = load_groups(get_setting) if get_setting else (REVIEWS_CACHE.get("groups") or {})
        logger.info(
            "reviews sync: отзывов=%s карточек=%s с рейтингом=%s",
            len(reviews),
            len(ratings),
            sum(1 for x in ratings if x.get("avg_rating") is not None),
        )

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


def build_xlsx_bytes(rows: list[dict]) -> bytes:
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font
    except ImportError as e:
        raise RuntimeError("На сервере нет openpyxl — добавь в requirements.txt и задеплой") from e

    wb = Workbook()
    ws = wb.active
    ws.title = "Ratings"
    ws.append([
        "Артикул", "Название", "SKU", "product_id", "Рейтинг",
        "★5", "★4", "★3", "★2", "★1", "В рейтинге", "Исключено", "Всего отзывов",
    ])
    for cell in ws[1]:
        cell.font = Font(bold=True)
    for r in rows or []:
        ws.append([
            r.get("offer_id") or r.get("article"),
            r.get("name"),
            r.get("sku"),
            r.get("product_id"),
            r.get("avg_rating"),
            r.get("r5") or 0,
            r.get("r4") or 0,
            r.get("r3") or 0,
            r.get("r2") or 0,
            r.get("r1") or 0,
            r.get("total") or 0,
            r.get("excluded") or 0,
            r.get("reviews_all") or 0,
        ])
    for col, w in zip("ABCDEFGHIJKLM", [22, 46, 14, 12, 10, 8, 8, 8, 8, 8, 12, 12, 14]):
        ws.column_dimensions[col].width = w
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()
