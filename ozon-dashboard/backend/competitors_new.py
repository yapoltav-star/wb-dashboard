"""Конкуренты.new — Excel analytics_report (аналитика категории Ozon).

Как на WB «Сравнение карточек»: загрузил файл → сразу топ по показам,
топ по корзинам, топ по заказам. Хранится в Supabase settings.
"""

from __future__ import annotations

import io
import json
import logging
import re
import threading
import time
import urllib.parse
import uuid
import zipfile
from datetime import datetime, timezone
from typing import Callable

import httpx

import competitors as _comp

logger = logging.getLogger("ozon-dashboard.competitors-new")

KEY = "ozon_competitor_new"
THUMBS_KEY = "ozon_competitor_thumbs"
MAX_REPORTS = 12
TOP_LIMIT = 15

CACHE: dict = {
    "reports": [],  # meta list, newest first
    "active": None,  # full report with rows
    "loaded": False,
}

THUMB_CACHE: dict[str, str] = {}
THUMB_BYTES: dict[str, tuple[bytes, str]] = {}
_thumb_lock = threading.Lock()
_thumb_last = 0.0
_thumbs_loaded = False
_THUMB_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)
_OZON_IMG_HOST = re.compile(
    r"^(?:cdn\d*\.|ir(?:-\d+)?\.)?(?:ozone\.ru|ozonusercontent\.com)$",
    re.I,
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_json(get_setting: Callable, key: str, default):
    raw = get_setting(key, None)
    if raw is None or raw == "":
        return default
    try:
        data = json.loads(raw) if isinstance(raw, str) else raw
        return data if data is not None else default
    except Exception:
        return default


def _sku_from_url(url: str) -> str:
    u = str(url or "").strip()
    if not u:
        return ""
    m = re.search(r"/product/(?:[^/?]*?-)?(\d{6,})(?:/|$|\?)", u)
    if m:
        return m.group(1)
    m = re.search(r"/product/(\d+)", u)
    return m.group(1) if m else ""


def _cart_adds(search_views: int, cart_search_pct: float, card_views: int, cart_card_pct: float) -> int:
    """Ozon отдаёт «в корзину» как %, не штуки. Считаем штуки из воронки."""
    from_search = (search_views or 0) * (cart_search_pct or 0) / 100.0
    from_card = (card_views or 0) * (cart_card_pct or 0) / 100.0
    return int(round(from_search + from_card))


def parse_analytics_rows(rows: list[list], filename: str = "") -> dict:
    meta = _comp._meta_from_rows(rows)
    header_idx = None
    for i, row in enumerate(rows):
        if not row:
            continue
        joined = " ".join(str(x or "").lower() for x in row)
        if "название товара" in joined and "показы" in joined and "заказано" in joined:
            header_idx = i
            break
    if header_idx is None:
        raise ValueError(
            "не отчёт аналитики категории (нужны колонки «Название товара», «Показы», «Заказано»)"
        )

    header = [str(h or "").strip().lower() for h in rows[header_idx]]
    cols = {
        "name": _comp._find_col(header, "название товара"),
        "url": _comp._find_col(header, "ссылка"),
        "seller": _comp._find_col(header, "продавец"),
        "brand": _comp._find_col(header, "бренд"),
        "cat1": _comp._find_col(header, "категория 1"),
        "cat3": _comp._find_col(header, "категория 3"),
        "flag": _comp._find_col(header, "признак"),
        "ordered_sum": _comp._find_col(header, "заказано на сумму"),
        "dynamics": _comp._find_col(header, "динамика оборота", "динамика"),
        "ordered_qty": _comp._find_col(header, "заказано, штуки", "заказано, штук"),
        "avg_price": _comp._find_col(header, "средняя цена"),
        "min_price": _comp._find_col(header, "минимальная цена"),
        "buyout": _comp._find_col(header, "доля выкупа"),
        "stock": _comp._find_col(header, "остаток на конец"),
        "scheme": _comp._find_col(header, "схема работы"),
        "views": _comp._find_col(header, "показы всего", "показы"),
        "search_views": _comp._find_col(header, "просмотры в поиске"),
        "card_views": _comp._find_col(header, "просмотры карточки"),
        "cr_order": _comp._find_col(header, "конверсия из показа в заказ"),
        "cart_search": _comp._find_col(header, "в корзину из поиска"),
        "cart_card": _comp._find_col(header, "в корзину из карточки"),
        "ads_share": _comp._find_col(header, "доля рекламных расходов"),
    }
    if cols["name"] is None or cols["views"] is None:
        raise ValueError("в шапке нет «Название товара» или «Показы»")

    items = []
    for row in rows[header_idx + 1 :]:
        if not row:
            continue
        name = str(_comp._g(row, cols["name"]) or "").strip()
        if not name:
            continue
        if name.lower().startswith("среднее"):
            continue
        url = str(_comp._g(row, cols["url"]) or "").strip()
        sku = _sku_from_url(url)
        search_views = _comp._to_int(_comp._g(row, cols["search_views"]))
        card_views = _comp._to_int(_comp._g(row, cols["card_views"]))
        cart_search_pct = _comp._to_float(_comp._g(row, cols["cart_search"]))
        cart_card_pct = _comp._to_float(_comp._g(row, cols["cart_card"]))
        views = _comp._to_int(_comp._g(row, cols["views"]))
        ordered_qty = _comp._to_int(_comp._g(row, cols["ordered_qty"]))
        cart_adds = _cart_adds(search_views, cart_search_pct, card_views, cart_card_pct)
        if not sku and not views and not ordered_qty:
            continue
        items.append({
            "name": name,
            "url": url,
            "sku": sku,
            "seller": str(_comp._g(row, cols["seller"]) or "").strip(),
            "brand": str(_comp._g(row, cols["brand"]) or "").strip(),
            "cat1": str(_comp._g(row, cols["cat1"]) or "").strip(),
            "cat3": str(_comp._g(row, cols["cat3"]) or "").strip(),
            "flag": str(_comp._g(row, cols["flag"]) or "").strip(),
            "ordered_sum": _comp._to_float(_comp._g(row, cols["ordered_sum"])),
            "dynamics": _comp._to_float(_comp._g(row, cols["dynamics"])),
            "ordered_qty": ordered_qty,
            "avg_price": _comp._to_float(_comp._g(row, cols["avg_price"])),
            "min_price": _comp._to_float(_comp._g(row, cols["min_price"])),
            "buyout": _comp._to_float(_comp._g(row, cols["buyout"])),
            "stock": _comp._to_int(_comp._g(row, cols["stock"])),
            "scheme": str(_comp._g(row, cols["scheme"]) or "").strip(),
            "views": views,
            "search_views": search_views,
            "card_views": card_views,
            "cr_order": _comp._to_float(_comp._g(row, cols["cr_order"])),
            "cart_search_pct": cart_search_pct,
            "cart_card_pct": cart_card_pct,
            "cart_adds": cart_adds,
            "ads_share": _comp._to_float(_comp._g(row, cols["ads_share"])),
            "is_own": False,
            "primary_image": "",
        })

    if not items:
        raise ValueError("в файле нет товаров")

    return {
        "id": uuid.uuid4().hex[:12],
        "type": "analytics",
        "period": meta.get("period"),
        "category": meta.get("category"),
        "price_filter": meta.get("price_filter"),
        "formed_at": meta.get("formed_at"),
        "uploaded_at": _now_iso(),
        "filename": filename or "",
        "rows": items,
        "count": len(items),
    }


def parse_analytics_xlsx(content: bytes, filename: str = "") -> dict:
    try:
        z = zipfile.ZipFile(io.BytesIO(content))
    except zipfile.BadZipFile as e:
        raise ValueError("это не xlsx-файл") from e
    rows = _comp._sheet_rows(z, 0)
    if not rows:
        raise ValueError("пустой файл")
    return parse_analytics_rows(rows, filename)


def mark_own_rows(rows: list[dict], products: list[dict] | None) -> None:
    by_sku: dict[str, dict] = {}
    by_pid: dict[str, dict] = {}
    for p in products or []:
        if p.get("sku") is not None:
            by_sku[str(p["sku"])] = p
        if p.get("product_id") is not None:
            by_pid[str(p["product_id"])] = p
    for r in rows or []:
        sku = str(r.get("sku") or "")
        hit = by_sku.get(sku) or by_pid.get(sku)
        if hit:
            r["is_own"] = True
            img = str(hit.get("primary_image") or r.get("primary_image") or "").strip()
            r["primary_image"] = img
            r["offer_id"] = hit.get("offer_id") or ""
            if img and sku:
                THUMB_CACHE[sku] = _to_c200(img)
        else:
            r["is_own"] = False


def _meta_of(report: dict) -> dict:
    rows = report.get("rows") or []
    return {
        "id": report.get("id"),
        "period": report.get("period"),
        "category": report.get("category"),
        "price_filter": report.get("price_filter"),
        "formed_at": report.get("formed_at"),
        "uploaded_at": report.get("uploaded_at"),
        "filename": report.get("filename"),
        "count": report.get("count") or len(rows),
        "total_views": sum(int(r.get("views") or 0) for r in rows),
        "total_carts": sum(int(r.get("cart_adds") or 0) for r in rows),
        "total_orders": sum(int(r.get("ordered_qty") or 0) for r in rows),
        "total_sum": sum(float(r.get("ordered_sum") or 0) for r in rows),
        "own_count": sum(1 for r in rows if r.get("is_own")),
    }


def summarize() -> dict:
    reports = CACHE.get("reports") or []
    active = CACHE.get("active")
    metas = []
    for r in reports:
        if r.get("rows") is not None:
            metas.append(_meta_of(r))
        else:
            metas.append(r)
    return {
        "reports": metas,
        "active": active,
        "active_id": (active or {}).get("id"),
    }


def load_all(get_setting: Callable, products: list[dict] | None = None, active_id: str | None = None) -> dict:
    load_thumbs(get_setting)
    raw = get_setting(KEY, None)
    if raw is None and CACHE.get("loaded") and CACHE.get("reports"):
        want = str(active_id or "").strip()
        if want:
            found = next((r for r in (CACHE.get("reports") or []) if r.get("id") == want), None)
            if found:
                CACHE["active"] = found
        return summarize()
    if raw is None or raw == "":
        blob = {}
    else:
        try:
            blob = json.loads(raw) if isinstance(raw, str) else raw
        except Exception:
            blob = {}
        if not isinstance(blob, dict):
            blob = {}
    reports = blob.get("reports") if isinstance(blob, dict) else []
    if not isinstance(reports, list):
        reports = []
    want = str(active_id or blob.get("active_id") or "").strip()
    active = None
    metas = []
    for r in reports:
        if not isinstance(r, dict):
            continue
        if r.get("rows"):
            mark_own_rows(r.get("rows") or [], products)
            apply_thumbs(r.get("rows") or [])
        metas.append(r)
        if want and r.get("id") == want:
            active = r
    if active is None and metas:
        active = next((r for r in metas if r.get("rows")), metas[0])
    CACHE["reports"] = metas
    CACHE["active"] = active
    CACHE["loaded"] = True
    return summarize()


def save_report(
    save_setting: Callable,
    get_setting: Callable,
    payload: dict,
    products: list[dict] | None = None,
) -> dict:
    load_thumbs(get_setting)
    if products is not None:
        mark_own_rows(payload.get("rows") or [], products)
    apply_thumbs(payload.get("rows") or [])
    blob = _load_json(get_setting, KEY, {}) or {}
    reports = blob.get("reports") if isinstance(blob, dict) else []
    if not isinstance(reports, list):
        reports = []
    reports = [r for r in reports if isinstance(r, dict) and r.get("id") != payload.get("id")]
    reports.insert(0, payload)
    reports = reports[:MAX_REPORTS]
    store = {"reports": reports, "active_id": payload.get("id")}
    try:
        save_setting(KEY, store)
    except Exception as e:
        logger.warning("competitors-new persist: %s", e)
    CACHE["reports"] = reports
    CACHE["active"] = payload
    CACHE["loaded"] = True
    return summarize()


def set_active(save_setting: Callable, get_setting: Callable, report_id: str, products: list[dict] | None = None) -> dict:
    data = load_all(get_setting, products=products, active_id=report_id)
    reports = CACHE.get("reports") or []
    found = next((r for r in reports if r.get("id") == report_id), None)
    if not found:
        raise ValueError("отчёт не найден")
    CACHE["active"] = found
    store = {"reports": reports, "active_id": report_id}
    try:
        save_setting(KEY, store)
    except Exception as e:
        logger.warning("competitors-new set_active: %s", e)
    return summarize()


def delete_report(
    save_setting: Callable,
    get_setting: Callable,
    report_id: str | None = None,
    *,
    clear_all: bool = False,
    products: list[dict] | None = None,
) -> dict:
    blob = _load_json(get_setting, KEY, {}) or {}
    reports = blob.get("reports") if isinstance(blob, dict) else []
    if not isinstance(reports, list):
        reports = []
    if clear_all:
        reports = []
        active_id = None
    else:
        if not report_id:
            raise ValueError("нужен id отчёта")
        reports = [r for r in reports if isinstance(r, dict) and r.get("id") != report_id]
        active_id = blob.get("active_id")
        if active_id == report_id:
            active_id = reports[0]["id"] if reports else None
    store = {"reports": reports, "active_id": active_id}
    CACHE["reports"] = reports
    CACHE["active"] = next((r for r in reports if r.get("id") == active_id), reports[0] if reports else None)
    CACHE["loaded"] = True
    try:
        save_setting(KEY, store)
    except Exception as e:
        logger.warning("competitors-new delete: %s", e)
    if products is not None and CACHE.get("active"):
        mark_own_rows(CACHE["active"].get("rows") or [], products)
    return summarize()


def top_rows(rows: list[dict], field: str, limit: int = TOP_LIMIT) -> list[dict]:
    scored = []
    for r in rows or []:
        try:
            score = float(r.get(field) or 0)
        except (TypeError, ValueError):
            score = 0.0
        if score > 0:
            scored.append((score, r))
    scored.sort(key=lambda x: -x[0])
    return [r for _, r in scored[:limit]]


def _to_c200(url: str) -> str:
    if not url:
        return url
    url = re.sub(r"/c\d+/", "/c200/", url)
    if "/c200/" in url or "/wc50/" in url:
        return url
    return re.sub(
        r"(multimedia[^/]+)/(\d+\.(?:jpg|jpeg|webp))",
        r"\1/c200/\2",
        url,
        flags=re.I,
    )


def _ozon_img_ok(url: str) -> bool:
    host = (urllib.parse.urlparse(url).netloc or "").split(":")[0].lower()
    return bool(host and _OZON_IMG_HOST.match(host))


def _parse_ozon_imgs(html: str) -> list[str]:
    found: list[str] = []
    for m in re.finditer(
        r"https://(?:cdn1|ir(?:-\d+)?)\.ozone\.ru/s3/multimedia[^\"\\<>\s]+",
        html or "",
    ):
        u = m.group(0).replace("\\/", "/")
        u = u.split("&quot;")[0].split('"')[0].split("\\")[0]
        if "rp-photo" in u or "video-" in u or "abt-challenge" in u:
            continue
        if not re.search(r"\.(jpg|jpeg|webp)(?:$|\?)", u, re.I):
            continue
        if u not in found:
            found.append(u)
    return found


def load_thumbs(get_setting: Callable | None) -> None:
    global _thumbs_loaded
    if _thumbs_loaded:
        return
    _thumbs_loaded = True
    if not get_setting:
        return
    data = _load_json(get_setting, THUMBS_KEY, {}) or {}
    if not isinstance(data, dict):
        return
    for k, v in data.items():
        sku, url = str(k or "").strip(), str(v or "").strip()
        if sku and url and sku not in THUMB_CACHE:
            THUMB_CACHE[sku] = url


def persist_thumbs(save_setting: Callable | None) -> None:
    if not save_setting:
        return
    try:
        save_setting(THUMBS_KEY, dict(THUMB_CACHE))
    except Exception as e:
        logger.warning("thumbs persist: %s", e)


def apply_thumbs(rows: list[dict] | None) -> None:
    for r in rows or []:
        if r.get("primary_image"):
            continue
        sku = str(r.get("sku") or "").strip()
        url = THUMB_CACHE.get(sku) or ""
        if url:
            r["primary_image"] = url


def _yandex_lookup(sku: str, name: str) -> str | None:
    parts = []
    title = (name or "").strip()[:70]
    if title:
        parts.append(f'"{title}"')
    if sku:
        parts.append(sku)
    parts.append("site:ozon.ru")
    url = "https://yandex.ru/images/search?text=" + urllib.parse.quote(" ".join(parts))
    resp = httpx.get(
        url,
        headers={"User-Agent": _THUMB_UA, "Accept-Language": "ru-RU,ru;q=0.9"},
        timeout=12.0,
        follow_redirects=True,
    )
    if resp.status_code != 200 or not resp.text:
        return None
    imgs = _parse_ozon_imgs(resp.text)
    return _to_c200(imgs[0]) if imgs else None


def get_thumb(sku: str, name: str = "", get_setting: Callable | None = None) -> str | None:
    sku = str(sku or "").strip()
    if not sku.isdigit():
        return None
    if sku in THUMB_CACHE:
        return THUMB_CACHE[sku] or None
    load_thumbs(get_setting)
    if sku in THUMB_CACHE:
        return THUMB_CACHE[sku] or None
    with _thumb_lock:
        if sku in THUMB_CACHE:
            return THUMB_CACHE[sku] or None
        global _thumb_last
        wait = 0.4 - (time.monotonic() - _thumb_last)
        if wait > 0:
            time.sleep(wait)
        try:
            url = _yandex_lookup(sku, name) or ""
        except Exception as e:
            logger.warning("thumb %s: %s", sku, e)
            url = ""
        _thumb_last = time.monotonic()
        THUMB_CACHE[sku] = url
        return url or None


def fetch_thumb_bytes(
    sku: str,
    name: str = "",
    get_setting: Callable | None = None,
) -> tuple[bytes, str] | None:
    """Скачать JPEG/WebP с CDN и отдать байты — img src остаётся same-origin."""
    sku = str(sku or "").strip()
    if not sku.isdigit():
        return None
    cached = THUMB_BYTES.get(sku)
    if cached:
        return cached
    url = get_thumb(sku, name, get_setting=get_setting)
    if not url or not _ozon_img_ok(url):
        return None
    try:
        resp = httpx.get(
            url,
            headers={
                "User-Agent": _THUMB_UA,
                "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
                "Referer": "https://www.ozon.ru/",
            },
            timeout=8.0,
            follow_redirects=True,
        )
        data = resp.content or b""
        if resp.status_code != 200 or len(data) < 32:
            return None
        ctype = (resp.headers.get("content-type") or "").split(";")[0].strip().lower()
        if not ctype.startswith("image/"):
            if data[:3] == b"\xff\xd8\xff":
                ctype = "image/jpeg"
            elif data[:8] == b"\x89PNG\r\n\x1a\n":
                ctype = "image/png"
            elif data[:4] == b"RIFF" and data[8:12] == b"WEBP":
                ctype = "image/webp"
            else:
                return None
        THUMB_BYTES[sku] = (data, ctype)
        return THUMB_BYTES[sku]
    except Exception as e:
        logger.warning("thumb bytes %s: %s", sku, e)
        return None


def prefetch_thumbs(
    rows: list[dict] | None,
    save_setting: Callable | None = None,
    get_setting: Callable | None = None,
    limit: int = 40,
) -> None:
    load_thumbs(get_setting)
    ranked = sorted(rows or [], key=lambda r: -(int(r.get("views") or 0)))[:limit]
    for r in ranked:
        sku = str(r.get("sku") or "")
        get_thumb(sku, r.get("name") or "", get_setting=get_setting)
        if sku and THUMB_CACHE.get(sku):
            r["primary_image"] = r.get("primary_image") or THUMB_CACHE.get(sku)
            fetch_thumb_bytes(sku, r.get("name") or "", get_setting=get_setting)
    persist_thumbs(save_setting)
