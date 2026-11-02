"""Конкуренты.new — Excel analytics_report (аналитика категории Ozon).

Как на WB «Сравнение карточек»: загрузил файл → сразу топ по показам,
топ по корзинам, топ по заказам. Хранится в Supabase settings.
"""

from __future__ import annotations

import io
import json
import logging
import re
import uuid
import zipfile
from datetime import datetime, timezone
from typing import Callable

import competitors as _comp

logger = logging.getLogger("ozon-dashboard.competitors-new")

KEY = "ozon_competitor_new"
MAX_REPORTS = 12
TOP_LIMIT = 15

CACHE: dict = {
    "reports": [],  # meta list, newest first
    "active": None,  # full report with rows
    "loaded": False,
}


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
            r["primary_image"] = hit.get("primary_image") or r.get("primary_image") or ""
            r["offer_id"] = hit.get("offer_id") or ""
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
    if products is not None:
        mark_own_rows(payload.get("rows") or [], products)
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
