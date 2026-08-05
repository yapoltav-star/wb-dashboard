"""Ozon Partners dashboard.

Разделы: товары, остатки/поставки, рост продаж, цены и соинвест.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import httpx
from fastapi import Body, FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

import ads
import coinvest as coin
import competitors as comp
import finance as fin
import orders as ordmod
import reviews as revs
import sales_pace as pace

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ozon-dashboard")

app = FastAPI(title="Ozon Partners Dashboard")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

OZON_CLIENT_ID = os.getenv("OZON_CLIENT_ID", "").strip()
OZON_API_KEY = os.getenv("OZON_API_KEY", "").strip()
SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip()

OZON_API = "https://api-seller.ozon.ru"

FRONTEND_DIR = Path(__file__).resolve().parent / "frontend"

_sync_lock = threading.Lock()
_sync_state = {
    "running": False,
    "kind": None,
    "last_error": None,
    "last_started": None,
    "last_finished": None,
    "last_count": 0,
}


def ozon_headers() -> dict:
    if not OZON_CLIENT_ID or not OZON_API_KEY:
        raise HTTPException(
            status_code=503,
            detail="Задай OZON_CLIENT_ID и OZON_API_KEY в переменных окружения",
        )
    return {
        "Client-Id": OZON_CLIENT_ID,
        "Api-Key": OZON_API_KEY,
        "Content-Type": "application/json",
    }


def sb_headers(prefer: str | None = "resolution=merge-duplicates") -> dict:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise HTTPException(
            status_code=503,
            detail="Задай SUPABASE_URL и SUPABASE_KEY в переменных окружения",
        )
    h = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }
    if prefer:
        h["Prefer"] = prefer
    return h


def sb_get(path: str, params: dict | None = None, prefer: str | None = None):
    return httpx.get(
        f"{SUPABASE_URL}/rest/v1/{path}",
        headers=sb_headers(prefer),
        params=params or {},
        timeout=30,
    )


def sb_upsert(table: str, rows: list[dict], on_conflict: str | None = None) -> httpx.Response:
    if not rows:
        return None
    params = {}
    if on_conflict:
        params["on_conflict"] = on_conflict
    return httpx.post(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers=sb_headers("resolution=merge-duplicates,return=minimal"),
        params=params,
        json=rows,
        timeout=90,
    )


def sb_delete(table: str, params: dict) -> None:
    resp = httpx.delete(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers={**sb_headers(None), "Prefer": "return=minimal"},
        params=params,
        timeout=60,
    )
    if resp.status_code >= 400:
        logger.warning("delete %s failed: %s %s", table, resp.status_code, resp.text[:200])


def save_setting(key: str, value) -> None:
    payload = {
        "key": key,
        "value": value if isinstance(value, str) else json.dumps(value, ensure_ascii=False),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    resp = sb_upsert("settings", [payload], on_conflict="key")
    if resp is not None and not resp.is_success:
        logger.warning("settings upsert failed: %s %s", resp.status_code, resp.text[:300])


def get_setting(key: str, default=None):
    try:
        resp = sb_get("settings", {"key": f"eq.{key}", "select": "value"})
        if resp.is_success and resp.json():
            return resp.json()[0]["value"]
    except Exception as e:
        logger.warning("get_setting(%s): %s", key, e)
    return default


def get_setting_int(key: str, default: int) -> int:
    raw = get_setting(key, str(default))
    try:
        return max(1, int(raw))
    except (TypeError, ValueError):
        return default


def ozon_post(path: str, body: dict | None = None, timeout: float = 90, retries: int = 3) -> dict:
    url = f"{OZON_API}{path}"
    last_err = None
    for attempt in range(retries):
        resp = httpx.post(url, headers=ozon_headers(), json=body if body is not None else {}, timeout=timeout)
        if resp.status_code == 429:
            time.sleep(1.5 * (attempt + 1))
            last_err = f"429 {resp.text[:200]}"
            continue
        if resp.status_code >= 400:
            detail = resp.text[:500]
            logger.error("Ozon %s → %s %s", path, resp.status_code, detail)
            raise HTTPException(status_code=502, detail=f"Ozon API {path}: {resp.status_code} {detail}")
        return resp.json()
    raise HTTPException(status_code=502, detail=f"Ozon API {path}: {last_err}")


def ozon_get(path: str, params: dict | None = None, timeout: float = 60, retries: int = 3) -> dict:
    url = f"{OZON_API}{path}"
    last_err = None
    for attempt in range(retries):
        resp = httpx.get(url, headers=ozon_headers(), params=params or {}, timeout=timeout)
        if resp.status_code == 429:
            time.sleep(1.5 * (attempt + 1))
            last_err = f"429 {resp.text[:200]}"
            continue
        if resp.status_code >= 400:
            detail = resp.text[:500]
            logger.error("Ozon GET %s → %s %s", path, resp.status_code, detail)
            raise HTTPException(status_code=502, detail=f"Ozon API {path}: {resp.status_code} {detail}")
        return resp.json()
    raise HTTPException(status_code=502, detail=f"Ozon API {path}: {last_err}")


def _to_int(v, default: int = 0) -> int:
    try:
        if v is None or v == "":
            return default
        return int(float(v))
    except (TypeError, ValueError):
        return default


def _to_num(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _first_image(item: dict) -> str | None:
    primary = item.get("primary_image")
    if isinstance(primary, list) and primary:
        return primary[0]
    if isinstance(primary, str) and primary:
        return primary
    images = item.get("images") or []
    if isinstance(images, list) and images:
        first = images[0]
        return first if isinstance(first, str) else None
    return None


# ── Products ───────────────────────────────────────────────────────────────


def fetch_all_product_ids(visibility: str = "ALL") -> list[dict]:
    items: list[dict] = []
    last_id = ""
    while True:
        payload = ozon_post(
            "/v3/product/list",
            {"filter": {"visibility": visibility}, "last_id": last_id, "limit": 1000},
        )
        result = payload.get("result") or payload
        batch = result.get("items") or []
        items.extend(batch)
        last_id = result.get("last_id") or ""
        total = result.get("total")
        if not batch or not last_id:
            break
        if total is not None and len(items) >= int(total):
            break
    return items


def fetch_product_info(product_ids: list[int]) -> list[dict]:
    out: list[dict] = []
    for i in range(0, len(product_ids), 100):
        part = product_ids[i : i + 100]
        payload = ozon_post("/v3/product/info/list", {"product_id": part})
        items = payload.get("items") or (payload.get("result") or {}).get("items") or []
        out.extend(items)
    return out


def normalize_product(list_row: dict, info: dict | None) -> dict:
    info = info or {}
    product_id = int(info.get("id") or list_row.get("product_id") or 0)
    sources = info.get("sources") or []
    sku = None
    if sources and isinstance(sources, list):
        sku = sources[0].get("sku")
    if sku is None:
        sku = info.get("sku")

    statuses = info.get("statuses") or {}
    visibility = None
    if isinstance(statuses, dict):
        visibility = statuses.get("status_name") or statuses.get("moderate_status")

    images = info.get("images") or []
    if not isinstance(images, list):
        images = []

    return {
        "product_id": product_id,
        "offer_id": info.get("offer_id") or list_row.get("offer_id") or "",
        "sku": sku,
        "name": info.get("name") or "",
        "barcode": (info.get("barcodes") or [None])[0] if info.get("barcodes") else info.get("barcode"),
        "description_category_id": info.get("description_category_id"),
        "type_id": info.get("type_id"),
        "currency_code": info.get("currency_code") or "RUB",
        "price": _to_num(info.get("price")),
        "old_price": _to_num(info.get("old_price")),
        "marketing_price": _to_num(info.get("marketing_price")),
        "vat": info.get("vat"),
        "primary_image": _first_image(info),
        "images": images,
        "statuses": statuses if isinstance(statuses, dict) else {},
        "visibility": visibility,
        "has_fbo_stocks": bool(list_row.get("has_fbo_stocks")),
        "has_fbs_stocks": bool(list_row.get("has_fbs_stocks")),
        "archived": bool(list_row.get("archived") or info.get("is_archived")),
        "is_discounted": bool(list_row.get("is_discounted") or info.get("is_discounted")),
        "volume_weight": _to_num(info.get("volume_weight")),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def _begin_sync(kind: str) -> bool:
    if not _sync_lock.acquire(blocking=False):
        return False
    _sync_state["running"] = True
    _sync_state["kind"] = kind
    _sync_state["last_error"] = None
    _sync_state["last_started"] = datetime.now(timezone.utc).isoformat()
    return True


def _end_sync(ok_count: int | None = None, error: str | None = None):
    if error:
        _sync_state["last_error"] = error
    if ok_count is not None:
        _sync_state["last_count"] = ok_count
    _sync_state["last_finished"] = datetime.now(timezone.utc).isoformat()
    _sync_state["running"] = False
    _sync_state["kind"] = None
    _sync_lock.release()


def _sync_products_unlocked() -> int:
    listed: dict[int, dict] = {}
    for visibility in ("ALL", "ARCHIVED"):
        for row in fetch_all_product_ids(visibility):
            pid = int(row.get("product_id") or 0)
            if not pid:
                continue
            row = dict(row)
            row["archived"] = visibility == "ARCHIVED" or bool(row.get("archived"))
            listed[pid] = row

    product_ids = list(listed.keys())
    infos = fetch_product_info(product_ids) if product_ids else []
    info_by_id = {}
    for it in infos:
        iid = it.get("id") or it.get("product_id")
        if iid is not None:
            info_by_id[int(iid)] = it

    rows = [normalize_product(listed[pid], info_by_id.get(pid)) for pid in product_ids]
    wrote = 0
    for i in range(0, len(rows), 200):
        batch = rows[i : i + 200]
        resp = sb_upsert("products", batch, on_conflict="product_id")
        if resp is not None and not resp.is_success:
            raise HTTPException(
                status_code=502,
                detail=f"Supabase products upsert: {resp.status_code} {resp.text[:400]}",
            )
        wrote += len(batch)

    save_setting("last_products_sync", datetime.now(timezone.utc).isoformat())
    save_setting("products_count", str(wrote))
    logger.info("products sync done: %s", wrote)
    return wrote


def sync_products() -> dict:
    if not _begin_sync("products"):
        return {"ok": False, "error": "sync already running"}
    try:
        wrote = _sync_products_unlocked()
        _end_sync(ok_count=wrote)
        return {"ok": True, "count": wrote}
    except HTTPException as e:
        _end_sync(error=str(e.detail))
        raise
    except Exception as e:
        logger.exception("products sync failed")
        _end_sync(error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


# ── Stocks ─────────────────────────────────────────────────────────────────


def load_stock_index() -> dict[str, dict]:
    """offer_id → {stock, warehouses} из stock_totals + stocks.

    warehouses = число складов, где free_to_sell/present > 0.
    """
    by_offer: dict[str, dict] = {}
    try:
        for r in _sb_select_all(
            "stock_totals",
            "offer_id,product_id,sku,name,stock_total,primary_image",
        ) or []:
            offer = str(r.get("offer_id") or "")
            if not offer:
                continue
            by_offer[offer] = {
                "stock": int(r.get("stock_total") or 0),
                "warehouses": 0,
                "sku": r.get("sku"),
                "name": r.get("name") or "",
                "primary_image": r.get("primary_image") or "",
                "product_id": r.get("product_id"),
            }
    except Exception as e:
        logger.warning("load_stock_index totals: %s", e)
        return by_offer
    try:
        for r in _sb_select_all(
            "stocks",
            "offer_id,free_to_sell,present",
        ) or []:
            offer = str(r.get("offer_id") or "")
            if not offer:
                continue
            qty = r.get("free_to_sell")
            if qty is None:
                qty = r.get("present") or 0
            try:
                qty = int(qty or 0)
            except (TypeError, ValueError):
                qty = 0
            if qty <= 0:
                continue
            entry = by_offer.setdefault(offer, {"stock": 0, "warehouses": 0})
            entry["warehouses"] = int(entry.get("warehouses") or 0) + 1
    except Exception as e:
        logger.warning("load_stock_index warehouses: %s", e)
    return by_offer


def load_products_from_db() -> list[dict]:
    """Все неархивные товары с sku из Supabase (пагинация)."""
    out: list[dict] = []
    offset = 0
    while True:
        resp = httpx.get(
            f"{SUPABASE_URL}/rest/v1/products",
            headers=sb_headers(None),
            params={
                "select": "product_id,offer_id,sku,name,primary_image,archived",
                "archived": "eq.false",
                "order": "product_id.asc",
                "limit": "1000",
                "offset": str(offset),
            },
            timeout=60,
        )
        if not resp.is_success:
            raise HTTPException(status_code=502, detail=f"products load: {resp.status_code} {resp.text[:300]}")
        batch = resp.json() or []
        out.extend(batch)
        if len(batch) < 1000:
            break
        offset += 1000
    return out


def fetch_product_info_stocks() -> list[dict]:
    """POST /v4/product/info/stocks — FBO/FBS totals по карточкам."""
    items: list[dict] = []
    cursor = ""
    while True:
        body: dict = {
            "filter": {"visibility": "ALL"},
            "limit": 1000,
        }
        # v4 использует cursor
        if cursor:
            body["cursor"] = cursor
        payload = ozon_post("/v4/product/info/stocks", body)
        result = payload.get("result") or payload
        batch = result.get("items") or []
        items.extend(batch)
        cursor = result.get("cursor") or ""
        logger.info("v4/product/info/stocks batch=%s total=%s", len(batch), len(items))
        if not batch or not cursor:
            break
    return items


def fetch_fbo_stocks_by_warehouse(skus: list[str]) -> list[dict]:
    """POST /v1/analytics/stocks — FBO по складам, до 100 sku."""
    out: list[dict] = []
    for i in range(0, len(skus), 100):
        chunk = skus[i : i + 100]
        if not chunk:
            continue
        payload = ozon_post(
            "/v1/analytics/stocks",
            {"skus": chunk, "warehouse_type": "ALL"},
        )
        items = payload.get("items") or (payload.get("result") or {}).get("items") or []
        out.extend(items)
        time.sleep(0.25)
    return out


def fetch_fbs_stocks_by_warehouse(skus: list[int | str]) -> list[dict]:
    """POST /v2/product/info/stocks-by-warehouse/fbs (v1 устарел)."""
    out: list[dict] = []
    cleaned: list[str] = []
    for s in skus:
        if s is None or s == "" or s == "None":
            continue
        cleaned.append(str(s))
    # уникальные, батчами по 100
    cleaned = list(dict.fromkeys(cleaned))
    for i in range(0, len(cleaned), 100):
        chunk = cleaned[i : i + 100]
        if not chunk:
            continue
        cursor = ""
        while True:
            body: dict = {"sku": chunk, "limit": 1000}
            if cursor:
                body["cursor"] = cursor
            payload = ozon_post("/v2/product/info/stocks-by-warehouse/fbs", body)
            result = payload.get("result") or payload
            items = (
                result.get("stocks")
                or result.get("items")
                or payload.get("stocks")
                or payload.get("items")
                or []
            )
            if isinstance(items, dict):
                items = items.get("items") or items.get("stocks") or []
            out.extend(items)
            has_next = bool(result.get("has_next"))
            cursor = result.get("cursor") or ""
            if not has_next or not cursor or not items:
                break
            time.sleep(0.15)
        time.sleep(0.2)
    return out


def fetch_ordered_units(date_from: date, date_to: date) -> dict[str, int]:
    """POST /v1/analytics/data — ordered_units по sku за окно."""
    by_sku: dict[str, int] = {}
    offset = 0
    limit = 1000
    while True:
        payload = ozon_post(
            "/v1/analytics/data",
            {
                "date_from": date_from.isoformat(),
                "date_to": date_to.isoformat(),
                "metrics": ["ordered_units"],
                "dimension": ["sku"],
                "limit": limit,
                "offset": offset,
            },
        )
        result = payload.get("result") or payload
        rows = result.get("data") or []
        for row in rows:
            dims = row.get("dimensions") or []
            metrics = row.get("metrics") or []
            sku = None
            if dims:
                sku = str(dims[0].get("id") or dims[0].get("name") or "")
            qty = _to_int(metrics[0] if metrics else 0)
            if sku:
                by_sku[sku] = by_sku.get(sku, 0) + qty
        totals = result.get("totals")
        logger.info("analytics/data offset=%s rows=%s skus=%s", offset, len(rows), len(by_sku))
        if len(rows) < limit:
            break
        offset += limit
        if offset > 50000:
            break
        time.sleep(0.3)
    return by_sku


def _stock_qty_from_analytics_item(it: dict) -> tuple[int, int, int]:
    """free_to_sell, reserved, promised из analytics/stocks."""
    free = _to_int(
        it.get("free_to_sell_amount")
        or it.get("available_stock_count")
        or it.get("free_stock_count")
        or it.get("present")
    )
    reserved = _to_int(it.get("reserved_amount") or it.get("reserved"))
    promised = _to_int(it.get("promised_amount") or it.get("promised") or it.get("stock_defect"))
    return free, reserved, promised


def sync_stocks() -> dict:
    """Остатки FBO/FBS + заказы за окно → stock_totals + stocks."""
    if not _begin_sync("stocks"):
        return {"ok": False, "error": "sync already running"}
    try:
        period_days = get_setting_int("sales_window_days", 14)
        date_to = date.today()
        date_from = date_to - timedelta(days=period_days - 1)

        products = load_products_from_db()
        if not products:
            _sync_products_unlocked()
            products = load_products_from_db()

        by_offer: dict[str, dict] = {}
        by_sku: dict[str, dict] = {}
        for p in products:
            offer = str(p.get("offer_id") or "").strip()
            sku = p.get("sku")
            if not offer:
                continue
            row = {
                "offer_id": offer,
                "product_id": p.get("product_id"),
                "sku": sku,
                "name": p.get("name") or "",
                "primary_image": p.get("primary_image"),
                "fbo_present": 0,
                "fbo_reserved": 0,
                "fbs_present": 0,
                "fbs_reserved": 0,
                "stock_total": 0,
                "ordered_qty": 0,
                "period_days": period_days,
                "period_start": date_from.isoformat(),
                "period_end": date_to.isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            by_offer[offer] = row
            if sku is not None:
                by_sku[str(sku)] = row

        # 1) totals FBO/FBS
        stock_items = fetch_product_info_stocks()
        for it in stock_items:
            offer = str(it.get("offer_id") or "").strip()
            product_id = it.get("product_id")
            row = by_offer.get(offer)
            if not row and product_id:
                # найти по product_id
                for r in by_offer.values():
                    if r.get("product_id") == product_id:
                        row = r
                        break
            if not row:
                row = {
                    "offer_id": offer or f"pid:{product_id}",
                    "product_id": product_id,
                    "sku": None,
                    "name": "",
                    "primary_image": None,
                    "fbo_present": 0,
                    "fbo_reserved": 0,
                    "fbs_present": 0,
                    "fbs_reserved": 0,
                    "stock_total": 0,
                    "ordered_qty": 0,
                    "period_days": period_days,
                    "period_start": date_from.isoformat(),
                    "period_end": date_to.isoformat(),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
                by_offer[row["offer_id"]] = row

            for st in it.get("stocks") or []:
                typ = (st.get("type") or "").lower()
                present = _to_int(st.get("present"))
                reserved = _to_int(st.get("reserved"))
                if typ == "fbo":
                    row["fbo_present"] = present
                    row["fbo_reserved"] = reserved
                elif typ == "fbs":
                    row["fbs_present"] = present
                    row["fbs_reserved"] = reserved

            # sku из stocks item если есть
            for st in it.get("stocks") or []:
                if st.get("sku") and not row.get("sku"):
                    row["sku"] = st.get("sku")
                    by_sku[str(row["sku"])] = row

        for row in by_offer.values():
            row["stock_total"] = _to_int(row["fbo_present"]) + _to_int(row["fbs_present"])
            if row.get("sku") is not None:
                by_sku[str(row["sku"])] = row

        # 2) FBO by warehouse
        sku_list = [s for s in by_sku.keys() if s and s != "None"]
        warehouse_rows: list[dict] = []
        now_iso = datetime.now(timezone.utc).isoformat()

        if sku_list:
            fbo_items = fetch_fbo_stocks_by_warehouse(sku_list)
            for it in fbo_items:
                sku = str(it.get("sku") or "")
                row = by_sku.get(sku)
                offer = row["offer_id"] if row else str(it.get("offer_id") or sku)
                wh_name = (
                    it.get("warehouse_name")
                    or it.get("cluster_name")
                    or it.get("warehouse")
                    or "FBO"
                )
                wh_id = it.get("warehouse_id") or it.get("cluster_id")
                free, reserved, promised = _stock_qty_from_analytics_item(it)
                present = free + reserved
                warehouse_rows.append(
                    {
                        "product_id": row.get("product_id") if row else None,
                        "sku": _to_int(sku, None) if sku.isdigit() else row.get("sku") if row else None,
                        "offer_id": offer,
                        "warehouse_id": _to_int(wh_id, None) if wh_id is not None else None,
                        "warehouse_name": f"FBO · {wh_name}",
                        "channel": "fbo",
                        "present": present,
                        "reserved": reserved,
                        "free_to_sell": free,
                        "promised": promised,
                        "ordered_qty": 0,
                        "updated_at": now_iso,
                    }
                )

        # 3) FBS by warehouse (не валим весь синк, если FBS недоступен)
        fbs_items: list[dict] = []
        if sku_list:
            try:
                fbs_items = fetch_fbs_stocks_by_warehouse(sku_list)
            except HTTPException as e:
                logger.warning("FBS warehouse stocks skipped: %s", e.detail)
            except Exception as e:
                logger.warning("FBS warehouse stocks skipped: %s", e)

        for it in fbs_items:
            sku = str(it.get("sku") or "")
            row = by_sku.get(sku)
            offer = row["offer_id"] if row else str(it.get("offer_id") or sku)
            stock_list = it.get("stocks")
            if not stock_list:
                stock_list = [it]
            for st in stock_list:
                wh_name = st.get("warehouse_name") or it.get("warehouse_name") or "FBS"
                wh_id = st.get("warehouse_id") or it.get("warehouse_id")
                present = _to_int(st.get("present"))
                reserved = _to_int(st.get("reserved"))
                free = present - reserved if present >= reserved else present
                warehouse_rows.append(
                    {
                        "product_id": row.get("product_id") if row else None,
                        "sku": _to_int(sku) if sku.isdigit() else (row.get("sku") if row else None),
                        "offer_id": offer,
                        "warehouse_id": _to_int(wh_id) if wh_id is not None else None,
                        "warehouse_name": f"FBS · {wh_name}",
                        "channel": "fbs",
                        "present": present,
                        "reserved": reserved,
                        "free_to_sell": free,
                        "promised": _to_int(st.get("waiting")),
                        "ordered_qty": 0,
                        "updated_at": now_iso,
                    }
                )

        # 4) orders by sku
        ordered = fetch_ordered_units(date_from, date_to)
        for sku, qty in ordered.items():
            row = by_sku.get(str(sku))
            if row:
                row["ordered_qty"] = qty

        # 5) распределить заказы по складам пропорционально free_to_sell/present
        stock_by_offer: dict[str, int] = {}
        for wr in warehouse_rows:
            offer = wr["offer_id"]
            qty = wr.get("free_to_sell") or wr.get("present") or 0
            stock_by_offer[offer] = stock_by_offer.get(offer, 0) + qty

        for wr in warehouse_rows:
            offer = wr["offer_id"]
            total_stock = stock_by_offer.get(offer, 0)
            total_orders = 0
            row = by_offer.get(offer)
            if row:
                total_orders = _to_int(row.get("ordered_qty"))
            elif wr.get("sku") is not None:
                r2 = by_sku.get(str(wr["sku"]))
                if r2:
                    total_orders = _to_int(r2.get("ordered_qty"))
            if total_stock > 0 and total_orders > 0:
                share = (wr.get("free_to_sell") or wr.get("present") or 0) / total_stock
                wr["ordered_qty"] = int(round(total_orders * share))
            else:
                wr["ordered_qty"] = 0

        # 6) write DB — replace warehouse rows
        sb_delete("stocks", {"id": "gte.0"})
        wrote_wh = 0
        for i in range(0, len(warehouse_rows), 200):
            batch = warehouse_rows[i : i + 200]
            # unique upsert key
            resp = sb_upsert("stocks", batch, on_conflict="offer_id,warehouse_name,channel")
            if resp is not None and not resp.is_success:
                # fallback without on_conflict if index missing
                resp = sb_upsert("stocks", batch)
            if resp is not None and not resp.is_success:
                raise HTTPException(
                    status_code=502,
                    detail=f"stocks upsert: {resp.status_code} {resp.text[:400]}",
                )
            wrote_wh += len(batch)

        totals = list(by_offer.values())
        wrote_tot = 0
        for i in range(0, len(totals), 200):
            batch = totals[i : i + 200]
            resp = sb_upsert("stock_totals", batch, on_conflict="offer_id")
            if resp is not None and not resp.is_success:
                raise HTTPException(
                    status_code=502,
                    detail=f"stock_totals upsert: {resp.status_code} {resp.text[:400]}",
                )
            wrote_tot += len(batch)

        save_setting("last_stocks_sync", datetime.now(timezone.utc).isoformat())
        save_setting("stocks_warehouse_count", str(wrote_wh))
        save_setting("stocks_totals_count", str(wrote_tot))
        _end_sync(ok_count=wrote_tot)
        logger.info("stocks sync done totals=%s warehouses=%s", wrote_tot, wrote_wh)
        return {
            "ok": True,
            "totals": wrote_tot,
            "warehouses": wrote_wh,
            "period_days": period_days,
            "orders_skus": len(ordered),
        }
    except HTTPException as e:
        _end_sync(error=str(e.detail))
        raise
    except Exception as e:
        logger.exception("stocks sync failed")
        _end_sync(error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


def _sb_select_all(table: str, select: str, extra: dict | None = None) -> list:
    out = []
    offset = 0
    while True:
        params = {"select": select, "limit": "1000", "offset": str(offset)}
        if extra:
            params.update(extra)
        resp = httpx.get(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers=sb_headers(None),
            params=params,
            timeout=60,
        )
        if not resp.is_success:
            raise HTTPException(status_code=502, detail=f"{table}: {resp.status_code} {resp.text[:300]}")
        batch = resp.json() or []
        out.extend(batch)
        if len(batch) < 1000:
            break
        offset += 1000
    return out


# ── Routes ─────────────────────────────────────────────────────────────────


@app.get("/api/status")
def status():
    configured = bool(OZON_CLIENT_ID and OZON_API_KEY and SUPABASE_URL and SUPABASE_KEY)
    last_sync = None
    last_stocks = None
    products_count = 0
    db_ok = False
    db_error = None
    if SUPABASE_URL and SUPABASE_KEY:
        try:
            last_sync = get_setting("last_products_sync")
            last_stocks = get_setting("last_stocks_sync")
            raw_count = get_setting("products_count", "0")
            try:
                products_count = int(raw_count)
            except (TypeError, ValueError):
                products_count = 0
            resp = httpx.get(
                f"{SUPABASE_URL}/rest/v1/products?select=product_id",
                headers={**sb_headers(None), "Prefer": "count=exact", "Range": "0-0"},
                timeout=10,
            )
            if resp.status_code < 400:
                db_ok = True
                cr = resp.headers.get("content-range", "")
                if "/" in cr:
                    products_count = int(cr.split("/")[-1] or products_count)
            else:
                db_error = f"HTTP {resp.status_code}: {resp.text[:200]}"
        except Exception as e:
            db_error = str(e)
            logger.warning("status db check: %s", e)
    else:
        db_error = "Нет SUPABASE_URL или SUPABASE_KEY в Railway Variables"

    return {
        "status": "ok",
        "marketplace": "ozon",
        "configured": configured,
        "db_ok": db_ok,
        "db_error": db_error,
        "supabase_url": SUPABASE_URL or None,
        "has_ozon_creds": bool(OZON_CLIENT_ID and OZON_API_KEY),
        "has_perf_creds": ads.perf_configured(),
        "last_products_sync": last_sync,
        "last_stocks_sync": last_stocks,
        "products_count": products_count,
        "sales_window_days": get_setting_int("sales_window_days", 14) if db_ok else 14,
        "target_coverage_days": get_setting_int("target_coverage_days", 30) if db_ok else 30,
        "sync": dict(_sync_state),
    }


@app.post("/api/sync-products")
def trigger_sync_products():
    return sync_products()


@app.post("/api/sync-stocks")
def trigger_sync_stocks():
    return sync_stocks()


@app.post("/api/save-setting")
def save_setting_endpoint(body: dict = Body(...)):
    key = body.get("key")
    value = body.get("value")
    if key not in ("sales_window_days", "target_coverage_days"):
        raise HTTPException(status_code=400, detail="unsupported key")
    try:
        ivalue = max(1, int(value))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="value must be int")
    save_setting(key, str(ivalue))
    return {"ok": True, "key": key, "value": ivalue}


@app.get("/api/products")
def list_products(
    q: str = "",
    archived: str = "active",
    limit: int = 200,
    offset: int = 0,
):
    limit = max(1, min(limit, 500))
    offset = max(0, offset)
    params: dict = {
        "select": "product_id,offer_id,sku,name,price,old_price,marketing_price,primary_image,visibility,has_fbo_stocks,has_fbs_stocks,archived,updated_at",
        "order": "offer_id.asc",
        "limit": str(limit),
        "offset": str(offset),
    }
    if archived == "active":
        params["archived"] = "eq.false"
    elif archived == "archived":
        params["archived"] = "eq.true"

    if q.strip():
        qq = q.strip().replace(",", " ").replace("%", "")
        if qq.isdigit():
            params["or"] = f"(offer_id.ilike.*{qq}*,name.ilike.*{qq}*,product_id.eq.{qq},sku.eq.{qq})"
        else:
            params["or"] = f"(offer_id.ilike.*{qq}*,name.ilike.*{qq}*)"

    try:
        resp = httpx.get(
            f"{SUPABASE_URL}/rest/v1/products",
            headers={**sb_headers(None), "Prefer": "count=exact"},
            params=params,
            timeout=30,
        )
    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=f"Не достучались до Supabase ({SUPABASE_URL}): {e}",
        ) from e
    if not resp.is_success:
        raise HTTPException(status_code=502, detail=f"Supabase: {resp.status_code} {resp.text[:300]}")

    total = 0
    cr = resp.headers.get("content-range", "")
    if "/" in cr:
        try:
            total = int(cr.split("/")[-1])
        except ValueError:
            total = len(resp.json())
    else:
        total = len(resp.json())

    items = resp.json() or []
    try:
        stock_idx = load_stock_index()
    except Exception:
        stock_idx = {}
    for it in items:
        offer = str(it.get("offer_id") or "")
        st = stock_idx.get(offer) or {}
        it["stock"] = int(st.get("stock") or 0)
        it["warehouses"] = int(st.get("warehouses") or 0)

    return {"items": items, "total": total, "limit": limit, "offset": offset}


@app.get("/api/stocks")
def get_stocks():
    """Данные для матрицы остатков / рекомендаций поставок."""
    try:
        totals = _sb_select_all(
            "stock_totals",
            "offer_id,product_id,sku,name,primary_image,fbo_present,fbo_reserved,fbs_present,fbs_reserved,stock_total,ordered_qty,period_days,period_start,period_end,updated_at",
            {"order": "offer_id.asc"},
        )
        warehouses = _sb_select_all(
            "stocks",
            "offer_id,product_id,sku,warehouse_id,warehouse_name,channel,present,reserved,free_to_sell,promised,ordered_qty,updated_at",
            {"order": "warehouse_name.asc"},
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e)) from e

    return {
        "totals": totals,
        "warehouses": warehouses,
        "sales_window_days": get_setting_int("sales_window_days", 14),
        "target_coverage_days": get_setting_int("target_coverage_days", 30),
        "last_stocks_sync": get_setting("last_stocks_sync"),
    }


@app.get("/api/sales-pace")
def get_sales_pace(period: str = "day", date_cur: str | None = None, date_prev: str | None = None):
    period = period if period in pace.SALES_PACE_PERIODS else "day"
    cached = pace.get_cached_pace(period, date_cur, date_prev)
    if cached:
        return cached
    # автозапуск синка в фоне
    if not pace.SALES_PACE_CACHE.get("syncing"):
        threading.Thread(
            target=_run_pace_sync,
            kwargs={"period": period, "date_cur": date_cur, "date_prev": date_prev},
            daemon=True,
        ).start()
    return {
        "period": period,
        "articles": [],
        "syncing": True,
        "error": None,
        "label_cur": "",
        "label_prev": "",
        "col_cur": "Сейчас",
        "col_prev": "Было",
        "funnel_ready": False,
        "ads_ready": False,
    }


@app.post("/api/sync-sales-pace")
def trigger_sales_pace(period: str = "day", date_cur: str | None = None, date_prev: str | None = None):
    if pace.SALES_PACE_CACHE.get("syncing"):
        return {"ok": True, "syncing": True}
    threading.Thread(
        target=_run_pace_sync,
        kwargs={"period": period, "date_cur": date_cur, "date_prev": date_prev},
        daemon=True,
    ).start()
    return {"ok": True, "syncing": True}


def _run_pace_sync(period: str = "day", date_cur: str | None = None, date_prev: str | None = None):
    try:
        pace.sync_sales_pace(
            ozon_post=ozon_post,
            load_stock_totals=lambda: _sb_select_all(
                "stock_totals",
                "offer_id,product_id,sku,name,stock_total",
            ),
            load_stock_index=load_stock_index,
            load_products=load_products_from_db,
            period=period,
            date_cur=date_cur,
            date_prev=date_prev,
            load_ads_sku=ads.fetch_ads_sku_stats if ads.perf_configured() else None,
            get_setting=get_setting,
            save_setting=save_setting,
        )
    except Exception as e:
        logger.exception("pace sync thread: %s", e)
        pace.SALES_PACE_CACHE["error"] = str(e)
        pace.SALES_PACE_CACHE["syncing"] = False


def _run_coinvest_sync():
    try:
        coin.sync_coinvest(
            ozon_post=ozon_post,
            ozon_get=ozon_get,
            load_products=load_products_from_db,
            load_manual_prices=load_manual_site_prices,
            load_stock_index=load_stock_index,
        )
    except Exception as e:
        logger.exception("coinvest sync thread: %s", e)
        coin.COINVEST_CACHE["error"] = str(e)
        coin.COINVEST_CACHE["syncing"] = False


def load_manual_site_prices() -> dict[str, float]:
    raw = get_setting(coin.MANUAL_PRICES_KEY, "{}")
    try:
        data = json.loads(raw or "{}")
    except (TypeError, ValueError, json.JSONDecodeError):
        data = {}
    out: dict[str, float] = {}
    if not isinstance(data, dict):
        return out
    for k, v in data.items():
        try:
            out[str(k)] = float(v)
        except (TypeError, ValueError):
            continue
    return out


def save_manual_site_price(offer_id: str, site_price: float | None) -> dict[str, float]:
    data = load_manual_site_prices()
    offer_id = str(offer_id).strip()
    if site_price is None:
        data.pop(offer_id, None)
    else:
        data[offer_id] = float(site_price)
    save_setting(coin.MANUAL_PRICES_KEY, json.dumps(data, ensure_ascii=False))
    return data


@app.get("/api/coinvest")
def get_coinvest(refresh: str = ""):
    cached = coin.get_cached()
    if refresh or (not cached["articles"] and not cached["syncing"] and not cached["error"]):
        if not coin.COINVEST_CACHE.get("syncing"):
            threading.Thread(target=_run_coinvest_sync, daemon=True).start()
        cached = coin.get_cached()
        cached["syncing"] = True
    return cached


@app.post("/api/sync-coinvest")
def trigger_coinvest():
    if coin.COINVEST_CACHE.get("syncing"):
        return {"ok": True, "syncing": True}
    threading.Thread(target=_run_coinvest_sync, daemon=True).start()
    return {"ok": True, "syncing": True}


@app.post("/api/coinvest/site-price")
def set_coinvest_site_price(payload: dict = Body(...)):
    """Ручная цена на сайте (то, что видишь на ozon.ru)."""
    offer_id = str(payload.get("offer_id") or "").strip()
    if not offer_id:
        raise HTTPException(status_code=400, detail="offer_id обязателен")
    raw = payload.get("site_price", payload.get("customer_price"))
    if raw is None or raw == "":
        site_price = None
    else:
        try:
            site_price = float(str(raw).replace(" ", "").replace(",", "."))
        except (TypeError, ValueError) as e:
            raise HTTPException(status_code=400, detail="Некорректная цена") from e
        if site_price <= 0:
            site_price = None
    save_manual_site_price(offer_id, site_price)
    article = coin.apply_manual_price_cached(offer_id, site_price)
    return {
        "ok": True,
        "offer_id": offer_id,
        "site_price": site_price,
        "article": article,
    }


def _run_orders_sync(days: int = 7):
    try:
        ordmod.sync_orders(ozon_post=ozon_post, days=days)
    except Exception as e:
        logger.exception("orders sync thread: %s", e)
        ordmod.ORDERS_CACHE["error"] = str(e)
        ordmod.ORDERS_CACHE["syncing"] = False


@app.get("/api/orders")
def get_orders(days: int = 7):
    cached = ordmod.get_cached()
    if not cached["postings"] and not cached["syncing"] and not cached["error"]:
        if not ordmod.ORDERS_CACHE.get("syncing"):
            threading.Thread(target=_run_orders_sync, kwargs={"days": days}, daemon=True).start()
        cached = ordmod.get_cached()
        cached["syncing"] = True
    return cached


@app.post("/api/sync-orders")
def trigger_orders(days: int = 7):
    if ordmod.ORDERS_CACHE.get("syncing"):
        return {"ok": True, "syncing": True}
    threading.Thread(target=_run_orders_sync, kwargs={"days": days}, daemon=True).start()
    return {"ok": True, "syncing": True}


def _run_finance_sync(period_days: int = 7):
    try:
        fin.sync_finance(ozon_post=ozon_post, period_days=period_days, with_compensation=True)
    except Exception as e:
        logger.exception("finance sync thread: %s", e)
        fin.FINANCE_CACHE["error"] = str(e)
        fin.FINANCE_CACHE["syncing"] = False


@app.get("/api/finance")
def get_finance(days: int = 7):
    cached = fin.get_cached()
    if not cached["days"] and not cached["syncing"] and not cached["error"]:
        if not fin.FINANCE_CACHE.get("syncing"):
            threading.Thread(target=_run_finance_sync, kwargs={"period_days": days}, daemon=True).start()
        cached = fin.get_cached()
        cached["syncing"] = True
    return cached


@app.post("/api/sync-finance")
def trigger_finance(days: int = 7):
    if fin.FINANCE_CACHE.get("syncing"):
        return {"ok": True, "syncing": True}
    threading.Thread(target=_run_finance_sync, kwargs={"period_days": days}, daemon=True).start()
    return {"ok": True, "syncing": True}


def _run_reviews_sync(status: str = "ALL"):
    try:
        products = []
        try:
            products = load_products_from_db()
        except Exception as e:
            logger.warning("reviews: products load skipped: %s", e)
        # подтянуть склейки из settings перед синкингом
        try:
            revs.REVIEWS_CACHE["groups"] = revs.load_groups(get_setting)
        except Exception:
            pass
        revs.sync_reviews(
            ozon_post=ozon_post,
            status=status,
            products=products,
            get_setting=get_setting,
        )
    except Exception as e:
        logger.exception("reviews sync thread: %s", e)
        revs.REVIEWS_CACHE["error"] = str(e)
        revs.REVIEWS_CACHE["syncing"] = False


@app.get("/api/reviews")
def get_reviews(status: str = "ALL"):
    cached = revs.get_cached()
    if not cached.get("groups"):
        try:
            cached["groups"] = revs.load_groups(get_setting)
            revs.REVIEWS_CACHE["groups"] = cached["groups"]
        except Exception:
            pass
    if not cached["reviews"] and not cached["syncing"] and not cached["error"] and not cached["premium_required"]:
        if not revs.REVIEWS_CACHE.get("syncing"):
            threading.Thread(target=_run_reviews_sync, kwargs={"status": status}, daemon=True).start()
        cached = revs.get_cached()
        cached["syncing"] = True
    return cached


@app.post("/api/sync-reviews")
def trigger_reviews(status: str = "ALL"):
    if revs.REVIEWS_CACHE.get("syncing"):
        return {"ok": True, "syncing": True}
    threading.Thread(target=_run_reviews_sync, kwargs={"status": status}, daemon=True).start()
    return {"ok": True, "syncing": True}


@app.get("/api/article-reviews")
def article_reviews(article: str, days: int = 5, stars: str = "1,2,3", limit: int = 50):
    """Негативные отзывы по одному артикулу за период (из кэша синка)."""
    if not article:
        raise HTTPException(status_code=400, detail="article required")
    star_list = []
    for part in str(stars or "").split(","):
        part = part.strip()
        if part.isdigit():
            star_list.append(int(part))
    if not star_list:
        star_list = [1, 2, 3]
    feedbacks = revs.filter_article_reviews(
        article=article,
        days=days,
        stars=star_list,
        limit=min(max(limit, 1), 200),
    )
    return {"article": article, "days": days, "stars": star_list, "feedbacks": feedbacks}


@app.get("/api/review-groups")
def get_review_groups():
    groups = revs.load_groups(get_setting)
    revs.REVIEWS_CACHE["groups"] = groups
    return {"groups": groups}


@app.post("/api/save-review-groups")
def save_review_groups(body: dict = Body(...)):
    groups = body.get("groups") or {}
    if not isinstance(groups, dict):
        raise HTTPException(status_code=400, detail="groups must be object")
    saved = revs.save_groups(save_setting, groups)
    return {"ok": True, "groups": saved, "count": len(saved)}


@app.get("/api/competitors")
def get_competitors():
    """Все отчёты конкурентов из общей базы (Supabase settings)."""
    products = []
    try:
        products = load_products_from_db()
    except Exception:
        pass
    try:
        data = comp.load_all(get_setting, products=products)
    except Exception as e:
        logger.warning("competitors load: %s", e)
        data = {
            "position": None, "brands": None, "brand_details": [],
            "brand_detail_map": {}, "hidden_brands": [], "own_brand": "",
        }
    return data


@app.post("/api/upload-competitors")
async def upload_competitors(file: UploadFile = File(...)):
    """Автодетект: конкурентная позиция / бренды / детальный отчёт бренда → Supabase."""
    name = file.filename or "competitors.xlsx"
    if not name.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="нужен файл .xlsx")
    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="пустой файл")
    try:
        payload = comp.detect_and_parse(content, filename=name)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.exception("competitors parse")
        raise HTTPException(status_code=400, detail=f"не удалось прочитать файл: {e}") from e
    products = []
    try:
        products = load_products_from_db()
    except Exception as e:
        logger.warning("competitors products: %s", e)
    data = comp.save_parsed(save_setting, get_setting, payload, products=products)
    return {"ok": True, "uploaded_type": payload.get("type"), "brand": payload.get("brand"), **data}


@app.post("/api/delete-competitors")
def delete_competitors(body: dict = Body(...)):
    """Удалить отчёт. kind=position|brands|brand_detail, для brand_detail нужен brand."""
    kind = str(body.get("kind") or "").strip()
    brand = body.get("brand")
    try:
        data = comp.delete_report(save_setting, get_setting, kind=kind, brand=brand)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return {"ok": True, **data}


@app.post("/api/competitors-hide-brand")
def competitors_hide_brand(body: dict = Body(...)):
    """Скрыть/показать бренд(ы) в общей базе. body: {brand|brands, hide?: bool, clear_all?: bool}."""
    brand = body.get("brand")
    brands = body.get("brands")
    hide = body.get("hide", True)
    clear_all = bool(body.get("clear_all"))
    try:
        data = comp.set_brand_hidden(
            save_setting,
            get_setting,
            brand=brand,
            hide=bool(hide),
            brands=brands if isinstance(brands, list) else None,
            clear_all=clear_all,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return {"ok": True, **data}


@app.post("/api/competitors-own-brand")
def competitors_own_brand(body: dict = Body(...)):
    """Сохранить «мой бренд» для сравнения в отчёте бренда."""
    brand = body.get("brand")
    data = comp.set_own_brand(save_setting, get_setting, brand)
    return {"ok": True, **data}


def _run_ads_sync(days: int = 7):
    try:
        ads.sync_ads(period_days=days)
    except Exception as e:
        logger.exception("ads sync thread: %s", e)
        ads.ADS_CACHE["error"] = str(e)
        ads.ADS_CACHE["syncing"] = False


@app.get("/api/ads")
def get_ads(days: int = 7):
    cached = ads.get_cached()
    if not cached["campaigns"] and not cached["syncing"] and not cached["error"] and cached["configured"]:
        if not ads.ADS_CACHE.get("syncing"):
            threading.Thread(target=_run_ads_sync, kwargs={"days": days}, daemon=True).start()
        cached = ads.get_cached()
        cached["syncing"] = True
    return cached


@app.post("/api/sync-ads")
def trigger_ads(days: int = 7):
    if ads.ADS_CACHE.get("syncing"):
        return {"ok": True, "syncing": True}
    threading.Thread(target=_run_ads_sync, kwargs={"days": days}, daemon=True).start()
    return {"ok": True, "syncing": True}


@app.get("/")
@app.get("/index.html")
def root_index():
    index = FRONTEND_DIR / "index.html"
    if index.exists():
        return FileResponse(index, media_type="text/html; charset=utf-8")
    return HTMLResponse("<h1>frontend missing</h1>", status_code=404)


if (FRONTEND_DIR / "index.html").exists():
    app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", 8080))
    uvicorn.run("main:app", host="0.0.0.0", port=port, timeout_keep_alive=30)
