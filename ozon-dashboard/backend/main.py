"""Ozon Partners dashboard — MVP skeleton.

Первая итерация: синк карточек через Seller API + SPA с разделом «Товары».
Дальше: остатки FBO/FBS, заказы, отзывы, реклама (Performance API), финансы.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

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
        timeout=60,
    )


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


def ozon_post(path: str, body: dict, timeout: float = 60) -> dict:
    url = f"{OZON_API}{path}"
    resp = httpx.post(url, headers=ozon_headers(), json=body, timeout=timeout)
    if resp.status_code >= 400:
        detail = resp.text[:500]
        logger.error("Ozon %s → %s %s", path, resp.status_code, detail)
        raise HTTPException(status_code=502, detail=f"Ozon API {path}: {resp.status_code} {detail}")
    return resp.json()


def fetch_all_product_ids(visibility: str = "ALL") -> list[dict]:
    """POST /v3/product/list — пагинация по last_id."""
    items: list[dict] = []
    last_id = ""
    while True:
        payload = ozon_post(
            "/v3/product/list",
            {
                "filter": {"visibility": visibility},
                "last_id": last_id,
                "limit": 1000,
            },
        )
        result = payload.get("result") or payload
        batch = result.get("items") or []
        items.extend(batch)
        last_id = result.get("last_id") or ""
        total = result.get("total")
        logger.info(
            "product/list visibility=%s batch=%s total_so_far=%s api_total=%s last_id=%s",
            visibility,
            len(batch),
            len(items),
            total,
            last_id[:24] if last_id else "",
        )
        if not batch or not last_id:
            break
        if total is not None and len(items) >= int(total):
            break
    return items


def fetch_product_info(product_ids: list[int]) -> list[dict]:
    """POST /v3/product/info/list — до 1000 id за раз."""
    out: list[dict] = []
    chunk = 100
    for i in range(0, len(product_ids), chunk):
        part = product_ids[i : i + chunk]
        payload = ozon_post("/v3/product/info/list", {"product_id": part})
        items = payload.get("items") or (payload.get("result") or {}).get("items") or []
        out.extend(items)
    return out


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


def _to_num(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def sync_products() -> dict:
    if not _sync_lock.acquire(blocking=False):
        return {"ok": False, "error": "sync already running"}

    _sync_state["running"] = True
    _sync_state["last_error"] = None
    _sync_state["last_started"] = datetime.now(timezone.utc).isoformat()
    try:
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
        # upsert батчами
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
        _sync_state["last_count"] = wrote
        _sync_state["last_finished"] = datetime.now(timezone.utc).isoformat()
        logger.info("products sync done: %s", wrote)
        return {"ok": True, "count": wrote}
    except HTTPException as e:
        _sync_state["last_error"] = str(e.detail)
        raise
    except Exception as e:
        logger.exception("products sync failed")
        _sync_state["last_error"] = str(e)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        _sync_state["running"] = False
        _sync_lock.release()


@app.get("/api/status")
def status():
    configured = bool(OZON_CLIENT_ID and OZON_API_KEY and SUPABASE_URL and SUPABASE_KEY)
    last_sync = None
    products_count = 0
    db_ok = False
    db_error = None
    if SUPABASE_URL and SUPABASE_KEY:
        try:
            last_sync = get_setting("last_products_sync")
            raw_count = get_setting("products_count", "0")
            try:
                products_count = int(raw_count)
            except (TypeError, ValueError):
                products_count = 0
            # точный count из таблицы, если доступна
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
    elif not SUPABASE_URL or not SUPABASE_KEY:
        db_error = "Нет SUPABASE_URL или SUPABASE_KEY в Railway Variables"

    return {
        "status": "ok",
        "marketplace": "ozon",
        "configured": configured,
        "db_ok": db_ok,
        "db_error": db_error,
        "supabase_url": SUPABASE_URL or None,
        "has_ozon_creds": bool(OZON_CLIENT_ID and OZON_API_KEY),
        "last_products_sync": last_sync,
        "products_count": products_count,
        "sync": dict(_sync_state),
    }


@app.post("/api/sync-products")
def trigger_sync_products():
    return sync_products()


@app.get("/api/products")
def list_products(
    q: str = "",
    archived: str = "active",
    limit: int = 200,
    offset: int = 0,
):
    """Список товаров из Supabase."""
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
        # offer_id / name / product_id
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

    return {"items": resp.json(), "total": total, "limit": limit, "offset": offset}


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
