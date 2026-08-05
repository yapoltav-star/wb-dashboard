"""Цены и соинвест Ozon — аналог раздела «Цены и СПП» на WB.

На Ozon нет поля SPP. Ближайшие сигналы из Seller API:
- price / marketing_seller_price / marketing_price (если ещё отдаётся)
- marketing_actions (акции на карточке)
- price_indexes.color_index
- /v1/actions — список акций Ozon с участием
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Callable

logger = logging.getLogger("ozon-dashboard.coinvest")

COINVEST_CACHE: dict = {
    "articles": [],
    "actions": [],
    "updated_at": None,
    "syncing": False,
    "error": None,
    "note": None,
}

_lock = threading.Lock()


def _to_num(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _pct(part: float | None, whole: float | None) -> float | None:
    if part is None or whole is None or whole <= 0:
        return None
    return round((1.0 - part / whole) * 100.0, 1)


def _index_payload(raw: dict | None) -> dict:
    raw = raw or {}
    color = (raw.get("color_index") or "WITHOUT_INDEX") or "WITHOUT_INDEX"
    oz = raw.get("ozon_index_data") or {}
    sm = raw.get("self_marketplaces_index_data") or {}
    ex = raw.get("external_index_data") or {}
    return {
        "color_index": color,
        "ozon_min_price": _to_num(oz.get("min_price")),
        "ozon_index": _to_num(oz.get("price_index_value")),
        "self_min_price": _to_num(sm.get("min_price")),
        "self_index": _to_num(sm.get("price_index_value")),
        "external_min_price": _to_num(ex.get("min_price")),
        "external_index": _to_num(ex.get("price_index_value")),
    }


def fetch_all_prices(ozon_post: Callable) -> list[dict]:
    """POST /v5/product/info/prices — все цены кабинета."""
    items: list[dict] = []
    cursor = ""
    last_id = ""
    for _ in range(200):
        body: dict = {
            "filter": {"visibility": "ALL"},
            "limit": 1000,
        }
        # API принимал и cursor, и last_id в разных ревизиях — шлём оба.
        if cursor:
            body["cursor"] = cursor
        if last_id:
            body["last_id"] = last_id
        payload = ozon_post("/v5/product/info/prices", body, timeout=90)
        batch = payload.get("items") or (payload.get("result") or {}).get("items") or []
        if not batch:
            break
        items.extend(batch)
        cursor = payload.get("cursor") or (payload.get("result") or {}).get("cursor") or ""
        last_id = payload.get("last_id") or (payload.get("result") or {}).get("last_id") or ""
        if not cursor and not last_id:
            break
        time.sleep(0.15)
    return items


def fetch_action_products(ozon_post: Callable, action_id: int, limit_pages: int = 20) -> list[dict]:
    """POST /v1/actions/products — товары уже в акции."""
    out: list[dict] = []
    offset = 0
    limit = 100
    for _ in range(limit_pages):
        payload = ozon_post(
            "/v1/actions/products",
            {"action_id": int(action_id), "limit": limit, "offset": offset},
            timeout=60,
        )
        result = payload.get("result") or payload
        products = result.get("products") or []
        if not products:
            break
        for p in products:
            out.append({
                "product_id": int(p.get("id") or p.get("product_id") or 0),
                "price": _to_num(p.get("price")),
                "action_price": _to_num(p.get("action_price")),
                "max_action_price": _to_num(p.get("max_action_price")),
                "stock": p.get("stock"),
            })
        total = result.get("total")
        offset += limit
        if total is not None and offset >= int(total):
            break
        if len(products) < limit:
            break
        time.sleep(0.1)
    return out


def normalize_price_item(item: dict, action_by_pid: dict[int, list[dict]]) -> dict:
    price_block = item.get("price") or {}
    if not isinstance(price_block, dict):
        price_block = {}
    ma = item.get("marketing_actions") or {}
    if not isinstance(ma, dict):
        ma = {}
    actions_raw = ma.get("actions") or []
    action_titles = []
    for a in actions_raw:
        if isinstance(a, dict) and a.get("title"):
            action_titles.append(str(a.get("title")))
    idx = _index_payload(item.get("price_indexes") if isinstance(item.get("price_indexes"), dict) else {})

    price = _to_num(price_block.get("price"))
    old_price = _to_num(price_block.get("old_price"))
    min_price = _to_num(price_block.get("min_price"))
    msp = _to_num(price_block.get("marketing_seller_price"))
    mp = _to_num(price_block.get("marketing_price"))  # может быть null после 11.2025

    seller_disc = _pct(msp, price) if msp is not None else None
    total_disc = _pct(mp, price) if mp is not None else None
    if mp is not None and msp is not None and price and price > 0 and msp >= mp:
        coinvest_pct = round((msp - mp) / price * 100.0, 1)
        coinvest_rub = round(msp - mp, 2)
    elif mp is not None and price is not None and price > 0 and (msp is None or msp == price):
        coinvest_pct = _pct(mp, price)
        coinvest_rub = round(price - mp, 2) if mp is not None else None
    else:
        coinvest_pct = None
        coinvest_rub = None

    pid = int(item.get("product_id") or 0)
    in_actions = action_by_pid.get(pid) or []
    best_action_price = None
    for ap in in_actions:
        apv = ap.get("action_price")
        if apv is None:
            continue
        best_action_price = apv if best_action_price is None else min(best_action_price, apv)

    if coinvest_pct is None and best_action_price is not None and price and price > 0:
        base = msp if msp is not None else price
        if base > best_action_price:
            coinvest_rub = round(base - best_action_price, 2)
            coinvest_pct = round((base - best_action_price) / price * 100.0, 1)
            if mp is None:
                mp = best_action_price
                total_disc = _pct(mp, price)

    return {
        "product_id": pid,
        "offer_id": item.get("offer_id") or "",
        "price": price,
        "old_price": old_price,
        "min_price": min_price,
        "marketing_seller_price": msp,
        "marketing_price": mp,
        "seller_discount_pct": seller_disc,
        "total_discount_pct": total_disc,
        "coinvest_pct": coinvest_pct,
        "coinvest_rub": coinvest_rub,
        "auto_action_enabled": bool(price_block.get("auto_action_enabled")),
        "auto_add_to_ozon_actions": bool(price_block.get("auto_add_to_ozon_actions_list_enabled")),
        "ozon_actions_exist": bool(ma.get("ozon_actions_exist")),
        "action_titles": action_titles[:5],
        "in_actions_count": len(in_actions),
        "action_price": best_action_price,
        "color_index": idx["color_index"],
        "ozon_min_price": idx["ozon_min_price"],
        "ozon_index": idx["ozon_index"],
        "commissions": item.get("commissions") if isinstance(item.get("commissions"), dict) else {},
    }


def sync_coinvest(
    ozon_post: Callable,
    ozon_get: Callable | None = None,
    load_products: Callable | None = None,
) -> dict:
    """Синк цен + акций → COINVEST_CACHE."""
    if not _lock.acquire(blocking=False):
        COINVEST_CACHE["syncing"] = True
        return {"ok": False, "error": "sync already running", "syncing": True}

    COINVEST_CACHE["syncing"] = True
    COINVEST_CACHE["error"] = None
    try:
        prices = fetch_all_prices(ozon_post)
        actions = []
        try:
            if ozon_get:
                actions = fetch_actions_via_get(ozon_get)
        except Exception as e:
            logger.warning("actions list failed: %s", e)

        # продукты в участвующих акциях (ограничим топ по числу участников)
        participating = [a for a in actions if a.get("is_participating") and a.get("id")]
        participating.sort(key=lambda a: -int(a.get("participating_products_count") or 0))
        action_by_pid: dict[int, list[dict]] = {}
        for a in participating[:15]:
            aid = int(a["id"])
            try:
                prods = fetch_action_products(ozon_post, aid)
            except Exception as e:
                logger.warning("action %s products failed: %s", aid, e)
                continue
            title = a.get("title") or ""
            for p in prods:
                pid = int(p.get("product_id") or 0)
                if not pid:
                    continue
                row = {**p, "action_id": aid, "action_title": title}
                action_by_pid.setdefault(pid, []).append(row)
            time.sleep(0.1)

        # имена из products, если есть
        name_by_offer = {}
        name_by_pid = {}
        if load_products:
            try:
                for pr in load_products() or []:
                    if pr.get("offer_id"):
                        name_by_offer[pr["offer_id"]] = pr.get("name") or ""
                    if pr.get("product_id") is not None:
                        name_by_pid[int(pr["product_id"])] = pr.get("name") or ""
            except Exception as e:
                logger.warning("load_products for coinvest: %s", e)

        articles = []
        for it in prices:
            row = normalize_price_item(it, action_by_pid)
            row["name"] = name_by_pid.get(row["product_id"]) or name_by_offer.get(row["offer_id"]) or ""
            articles.append(row)

        has_mp = any(a.get("marketing_price") is not None for a in articles)
        note = None
        if not has_mp:
            note = (
                "Ozon с ноября 2025 не отдаёт marketing_price (цену на витрине) в Seller API. "
                "Соинвест % считаем по action_price акций и marketing_seller_price; "
                "полная цена покупателя — только с витрины/финансов."
            )

        COINVEST_CACHE.update({
            "articles": articles,
            "actions": actions,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "syncing": False,
            "error": None,
            "note": note,
            "count": len(articles),
        })
        logger.info("coinvest sync done: %s prices, %s actions", len(articles), len(actions))
        return {"ok": True, "count": len(articles), "actions": len(actions)}
    except Exception as e:
        logger.exception("coinvest sync failed")
        COINVEST_CACHE["error"] = str(e)
        COINVEST_CACHE["syncing"] = False
        return {"ok": False, "error": str(e)}
    finally:
        _lock.release()


def fetch_actions_via_get(ozon_get: Callable) -> list[dict]:
    payload = ozon_get("/v1/actions")
    result = payload.get("result") or payload.get("actions") or payload
    if isinstance(result, dict):
        actions = result.get("actions") or result.get("items") or []
    elif isinstance(result, list):
        actions = result
    else:
        actions = []
    out = []
    for a in actions:
        if not isinstance(a, dict):
            continue
        out.append({
            "id": a.get("id"),
            "title": a.get("title") or a.get("name") or "",
            "action_type": a.get("action_type") or a.get("type") or "",
            "date_start": a.get("date_start") or a.get("dateStart"),
            "date_end": a.get("date_end") or a.get("dateEnd"),
            "freeze_date": a.get("freeze_date"),
            "is_participating": bool(a.get("is_participating") or a.get("isParticipating")),
            "potential_products_count": int(a.get("potential_products_count") or a.get("potentialProductsCount") or 0),
            "participating_products_count": int(
                a.get("participating_products_count") or a.get("participatingProductsCount") or 0
            ),
            "banned_products_count": int(a.get("banned_products_count") or 0),
        })
    return out


def get_cached() -> dict:
    return {
        "articles": COINVEST_CACHE.get("articles") or [],
        "actions": COINVEST_CACHE.get("actions") or [],
        "updated_at": COINVEST_CACHE.get("updated_at"),
        "syncing": bool(COINVEST_CACHE.get("syncing")),
        "error": COINVEST_CACHE.get("error"),
        "note": COINVEST_CACHE.get("note"),
        "count": len(COINVEST_CACHE.get("articles") or []),
    }
