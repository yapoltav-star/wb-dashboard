import httpx
import os
import io
import json
import time
import logging
from datetime import datetime, timedelta, timezone, date
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
import pandas as pd
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

WB_TOKEN = os.getenv("WB_TOKEN", "")
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
WB_FEEDBACKS_URL = "https://feedbacks-api.wildberries.ru"
WB_ANALYTICS_URL = "https://seller-analytics-api.wildberries.ru"
WB_STATISTICS_URL = "https://statistics-api.wildberries.ru"
WB_SUPPLIES_URL = "https://supplies-api.wildberries.ru"
WB_PROMOTION_URL = "https://advert-api.wildberries.ru"
WB_CALENDAR_URL = "https://dp-calendar-api.wildberries.ru"
WB_CONTENT_URL = "https://content-api.wildberries.ru"

# Спец-строки в ответе WB warehouse_remains, которые на самом деле не склады,
# а агрегаты — переносим их в отдельные поля stock_totals вместо списка складов.
STOCK_SPECIAL_FIELDS = {
    "В пути до получателей": "in_way_to_client",
    "В пути возвраты на склад WB": "in_way_from_client",
    "Всего находится на складах": "quantity_warehouses_full",
}

def sb_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }

def wb_headers():
    return {"Authorization": WB_TOKEN}

def upsert_feedbacks(feedbacks: list):
    if not feedbacks:
        return 0
    url = f"{SUPABASE_URL}/rest/v1/feedbacks"
    resp = httpx.post(url, json=feedbacks, headers=sb_headers(), timeout=30)
    if not resp.is_success:
        logger.error(f"Supabase upsert error: {resp.status_code} {resp.text[:200]}")
        return 0
    return len(feedbacks)

def fetch_feedbacks_page(is_answered: bool, skip: int):
    resp = httpx.get(
        f"{WB_FEEDBACKS_URL}/api/v1/feedbacks",
        headers=wb_headers(),
        params={"isAnswered": str(is_answered).lower(), "take": 5000, "skip": skip, "order": "dateDesc"},
        timeout=30
    )
    if not resp.is_success:
        logger.error(f"WB API error {resp.status_code}")
        return []
    return resp.json().get("data", {}).get("feedbacks", [])

def fetch_archive_page(skip: int):
    resp = httpx.get(
        f"{WB_FEEDBACKS_URL}/api/v1/feedbacks/archive",
        headers=wb_headers(),
        params={"isAnswered": "true", "take": 5000, "skip": skip, "order": "dateDesc"},
        timeout=30
    )
    if not resp.is_success:
        logger.error(f"WB archive error {resp.status_code}")
        return []
    return resp.json().get("data", {}).get("feedbacks", [])

def is_supplemented(f: dict) -> bool:
    return bool(f.get("isEdited") or f.get("supplementedFeedbackId"))

def process_feedback(f: dict, nm_to_vendor: dict = None) -> dict:
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=365)
    pd_data = f.get("productDetails", {})
    nm_id = pd_data.get("nmId")
    supplier_article = pd_data.get("supplierArticle", "")
    # Если WB не вернул supplierArticle — берём из нашей карты nm_id→vendor_code
    # чтобы не хранить отзыв как "208715116" вместо "000Braslet1"
    if not supplier_article and nm_id and nm_to_vendor:
        supplier_article = nm_to_vendor.get(nm_id, "")
    article = supplier_article or str(nm_id or "")
    date_str = f.get("createdDate") or f.get("updatedDate") or ""
    try:
        date = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except:
        date = now
    return {
        "id": f.get("id", ""),
        "article": article,
        "nm_id": nm_id,
        "stars": f.get("productValuation", 0),
        "created_date": date.isoformat(),
        "is_old": date < cutoff,
        "is_answered": bool(f.get("answer")),
        "text": (f.get("text") or "")[:500],
        "updated_at": now.isoformat()
    }

def sync_all():
    if not WB_TOKEN:
        logger.error("WB_TOKEN not set")
        return
    logger.info("Starting sync...")
    total = 0

    # Строим карту nmId → vendor_code из stock_totals чтобы исправить артикулы
    # у которых WB не вернул supplierArticle (тогда они попадают как "208715116" вместо "000Braslet1")
    nm_to_vendor = {}
    try:
        st = httpx.get(
            f"{SUPABASE_URL}/rest/v1/stock_totals?select=nm_id,vendor_code",
            headers=sb_headers(), timeout=15
        )
        if st.is_success:
            nm_to_vendor = {r["nm_id"]: r["vendor_code"] for r in st.json() if r.get("nm_id") and r.get("vendor_code")}
            logger.info(f"sync_all: nm_to_vendor map built: {len(nm_to_vendor)} entries")
    except Exception as e:
        logger.error(f"sync_all: failed to build nm_to_vendor: {e}")

    for is_answered in [True, False]:
        skip = 0
        while skip <= 199990:
            batch = fetch_feedbacks_page(is_answered, skip)
            if not batch:
                break
            processed = [process_feedback(f, nm_to_vendor) for f in batch if f.get("id") and not is_supplemented(f)]
            total += upsert_feedbacks(processed)
            logger.info(f"  answered={is_answered} skip={skip} saved={len(processed)}")
            skip += len(batch)
            if len(batch) < 5000:
                break
            time.sleep(0.3)

    skip = 0
    while skip <= 199990:
        batch = fetch_archive_page(skip)
        if not batch:
            break
        processed = [process_feedback(f, nm_to_vendor) for f in batch if f.get("id")]
        total += upsert_feedbacks(processed)
        logger.info(f"  archive skip={skip} saved={len(processed)}")
        skip += len(batch)
        if len(batch) < 5000:
            break
        time.sleep(0.3)

    now_str = datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M")
    httpx.post(
        f"{SUPABASE_URL}/rest/v1/settings",
        json={"key": "last_sync", "value": now_str, "updated_at": datetime.now(timezone.utc).isoformat()},
        headers=sb_headers(), timeout=10
    )
    logger.info(f"Sync complete. Total: {total}")
    # sync_ratings_official временно отключён — ищем правильный эндпоинт WB API для feedbackRating
    # Рейтинги загружаются вручную через xlsx ("Оценка товара")

def sync_ratings_official():
    """Временно отключена — эндпоинт WB для feedbackRating не найден.
    Рейтинги берутся из xlsx файла оценок через /api/upload-ratings."""
    logger.info("sync_ratings_official: skipped (endpoint not configured)")
    return

# ---------- Остатки на складах (WB Analytics: warehouse_remains report) ----------

def create_stock_report():
    resp = httpx.get(
        f"{WB_ANALYTICS_URL}/api/v1/warehouse_remains",
        headers=wb_headers(), params={"groupByNm": "true"}, timeout=30
    )
    if not resp.is_success:
        logger.error(f"WB stock report create error {resp.status_code} {resp.text[:200]}")
        return None
    return resp.json().get("data", {}).get("taskId")

def wait_stock_report(task_id: str, max_wait: int = 180) -> bool:
    elapsed = 0
    while elapsed < max_wait:
        time.sleep(5)
        elapsed += 5
        resp = httpx.get(
            f"{WB_ANALYTICS_URL}/api/v1/warehouse_remains/tasks/{task_id}/status",
            headers=wb_headers(), timeout=15
        )
        status = resp.json().get("data", {}).get("status") if resp.is_success else f"http_{resp.status_code}"
        logger.info(f"Stock report status (t+{elapsed}s): {status}")
        if status == "done":
            return True
    return False

def download_stock_report(task_id: str) -> list:
    resp = httpx.get(
        f"{WB_ANALYTICS_URL}/api/v1/warehouse_remains/tasks/{task_id}/download",
        headers=wb_headers(), timeout=60
    )
    if not resp.is_success:
        logger.error(f"WB stock report download error {resp.status_code} {resp.text[:200]}")
        return []
    data = resp.json()
    if not isinstance(data, list):
        logger.error(f"Unexpected stock report shape ({type(data).__name__}): {str(data)[:300]}")
        return []
    logger.info(f"Stock report download: {len(data)} items" + (f", raw snippet: {resp.text[:300]}" if not data else ""))
    if data:
        logger.info(f"Stock report sample item 0: {json.dumps(data[0], ensure_ascii=False)[:600]}")
        if len(data) > 1:
            logger.info(f"Stock report sample item 1: {json.dumps(data[1], ensure_ascii=False)[:600]}")
    return data

def process_stock_items(items: list):
    now = datetime.now(timezone.utc).isoformat()
    totals, warehouses = [], []
    for it in items:
        nm_id = it.get("nmId")
        if not nm_id:
            continue
        row = {
            "nm_id": nm_id,
            "vendor_code": it.get("vendorCode", ""),
            "subject_name": it.get("subjectName", ""),
            "brand": it.get("brand", ""),
            "volume": it.get("volume"),
            "in_way_to_client": 0,
            "in_way_from_client": 0,
            "quantity_warehouses_full": 0,
            "updated_at": now,
        }
        for w in it.get("warehouses", []):
            name, qty = w.get("warehouseName"), w.get("quantity", 0)
            field = STOCK_SPECIAL_FIELDS.get(name)
            if field:
                row[field] = qty
            else:
                warehouses.append({"nm_id": nm_id, "warehouse_name": name, "quantity": qty, "updated_at": now})
        totals.append(row)
    return totals, warehouses

def upsert_stock(totals: list, warehouses: list) -> int:
    saved = 0
    for i in range(0, len(totals), 200):
        batch = totals[i:i + 200]
        resp = httpx.post(f"{SUPABASE_URL}/rest/v1/stock_totals", json=batch, headers=sb_headers(), timeout=30)
        if resp.is_success:
            saved += len(batch)
        else:
            logger.error(f"stock_totals upsert error {resp.status_code} {resp.text[:200]}")
    # Полная перезаливка детализации по складам — проще, чем строить составной upsert-ключ
    httpx.delete(f"{SUPABASE_URL}/rest/v1/stock_warehouses?id=gte.0", headers=sb_headers(), timeout=15)
    for i in range(0, len(warehouses), 500):
        batch = warehouses[i:i + 500]
        resp = httpx.post(f"{SUPABASE_URL}/rest/v1/stock_warehouses", json=batch, headers=sb_headers(), timeout=30)
        if not resp.is_success:
            logger.error(f"stock_warehouses insert error {resp.status_code} {resp.text[:200]}")
    return saved

def sync_stock():
    if not WB_TOKEN:
        logger.error("WB_TOKEN not set")
        return
    logger.info("Starting stock sync...")
    task_id = create_stock_report()
    if not task_id:
        return
    if not wait_stock_report(task_id):
        logger.error("Stock report generation timed out")
        return
    time.sleep(5)  # небольшой буфер: статус иногда становится "done" чуть раньше, чем файл реально готов к скачиванию
    items = download_stock_report(task_id)
    if not items:
        logger.info("Stock report empty on first download, retrying once after 15s")
        time.sleep(15)
        items = download_stock_report(task_id)
    totals, warehouses = process_stock_items(items)
    saved = upsert_stock(totals, warehouses)
    httpx.post(
        f"{SUPABASE_URL}/rest/v1/settings",
        json={"key": "last_stock_sync", "value": datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M"),
              "updated_at": datetime.now(timezone.utc).isoformat()},
        headers=sb_headers(), timeout=10
    )
    logger.info(f"Stock sync complete. Articles: {saved}, warehouse rows: {len(warehouses)}")

# ---------- Остатки нашего склада (Google Sheets) ----------
OWN_WAREHOUSE_SHEET_ID = os.getenv(
    "OWN_WAREHOUSE_SHEET_ID",
    "1Lhoy4s_KX0pWndsd3Y5oCOjTFCtfEfVUM4AgtBv4Crc",
)
OWN_WAREHOUSE_GID = os.getenv("OWN_WAREHOUSE_GID", "1829622647")
OWN_WAREHOUSE_CACHE = {
    "title": None,
    "as_of": None,
    "rows": [],
    "updated_at": None,
    "error": None,
    "syncing": False,
}

def _parse_int_cell(v):
    s = str(v or "").strip().replace("\xa0", "").replace(" ", "").replace(",", ".")
    if not s or s.lower() in ("nan", "none", "-"):
        return None
    try:
        return int(float(s))
    except Exception:
        return None

def fetch_own_warehouse_stock() -> dict:
    """Тянет CSV из Google Sheets «Остатки на складе».
    Берём только 1-ю таблицу (до ИТОГО / «Принято на склад»), без блоков принято/обмен.
    Строим семьи артикулов: пустые строки-артикулы под основным (044→037) делят остаток."""
    import csv as _csv
    import re as _re
    url = (
        f"https://docs.google.com/spreadsheets/d/{OWN_WAREHOUSE_SHEET_ID}"
        f"/export?format=csv&gid={OWN_WAREHOUSE_GID}"
    )
    resp = httpx.get(url, timeout=30, follow_redirects=True)
    if not resp.is_success:
        raise RuntimeError(f"Google Sheets HTTP {resp.status_code}")
    text = resp.text
    if not text.strip() or text.lstrip().startswith("<!"):
        raise RuntimeError("Таблица недоступна (нужен доступ «все у кого есть ссылка»)")

    rows_raw = list(_csv.reader(io.StringIO(text)))
    if len(rows_raw) < 2:
        raise RuntimeError("Пустая таблица")

    title = (rows_raw[0][0] if rows_raw[0] else "").strip()
    as_of = None
    m = _re.search(r"(\d{1,2})[./](\d{1,2})[./](\d{2,4})", title)
    if m:
        d, mo, y = m.group(1), m.group(2), m.group(3)
        if len(y) == 2:
            y = "20" + y
        as_of = f"{int(d):02d}.{int(mo):02d}.{y}"

    header = [str(h).strip().lower() for h in rows_raw[1]]

    def find_col(*needles):
        for i, h in enumerate(header):
            for n in needles:
                if n in h:
                    return i
        return None

    col_vc = find_col("артикул продавца", "артикул")
    col_name = find_col("наименование", "название")
    col_stock = find_col("остататки на складе", "остатки на складе")
    col_note = find_col("примечание")
    if col_stock is None and len(header) > 11:
        col_stock = 11
    if col_vc is None:
        col_vc = 1
    if col_name is None:
        col_name = 2

    # ── Только 1-я таблица ──
    raw_rows = []
    for r in rows_raw[2:]:
        if not r or not any(str(c).strip() for c in r):
            continue
        pn = str(r[0]).strip() if r else ""
        joined = " ".join(str(c).lower() for c in r)
        if pn.upper().startswith("ИТОГО") or "принято на склад" in joined:
            break
        if pn.replace("\\", "") in ("П/Н", "ПН") and "артикул" not in joined:
            break

        def cell(i):
            return str(r[i]).strip() if i is not None and i < len(r) else ""

        vc = cell(col_vc)
        name = cell(col_name)
        note = cell(col_note)
        stock_raw = cell(col_stock)
        stock = _parse_int_cell(stock_raw)
        if not vc and not name:
            continue
        raw_rows.append({
            "vendor_code": vc or None,
            "name": name or None,
            "stock": stock if stock is not None else 0,
            "note": note or None,
            "has_stock_cell": bool(stock_raw),
        })

    # Личный остаток по артикулу (сумма, если vc повторяется)
    personal = {}
    for row in raw_rows:
        vc = row["vendor_code"]
        if not vc:
            continue
        personal[vc] = personal.get(vc, 0) + (row["stock"] or 0)

    # Семьи: основной (есть имя или ячейка остатка) + следующие «голые» артикулы без имени
    # Пример: 044_LK_GT5Pro_black_O (380) → 037_G7Pro_black_O (пусто) делят 380
    families = []  # [{root, members:[], name}]
    cur = None
    for row in raw_rows:
        vc = row["vendor_code"]
        if not vc:
            continue
        is_main = bool(row["name"]) or row["has_stock_cell"]
        if is_main:
            if cur:
                families.append(cur)
            cur = {"root": vc, "members": [vc], "name": row["name"]}
        else:
            if cur is None:
                cur = {"root": vc, "members": [vc], "name": None}
            elif vc not in cur["members"]:
                cur["members"].append(vc)
    if cur:
        families.append(cur)

    # Если артикул попал в несколько семей — объединяем
    parent = {}
    def find(x):
        if parent.get(x, x) != x:
            parent[x] = find(parent[x])
        return parent.get(x, x)
    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    for fam in families:
        root = fam["root"]
        parent.setdefault(root, root)
        for m in fam["members"]:
            parent.setdefault(m, m)
            union(root, m)

    root_members = {}
    for vc in personal:
        parent.setdefault(vc, vc)
        r = find(vc)
        root_members.setdefault(r, [])
        if vc not in root_members[r]:
            root_members[r].append(vc)
    # также члены семей без личного остатка
    for fam in families:
        for m in fam["members"]:
            parent.setdefault(m, m)
            r = find(m)
            root_members.setdefault(r, [])
            if m not in root_members[r]:
                root_members[r].append(m)

    auto_by_vendor = {}
    for root, members in root_members.items():
        fam_stock = sum(personal.get(m, 0) for m in members)
        for m in members:
            auto_by_vendor[m] = {
                "stock": personal.get(m, 0),
                "family_stock": fam_stock,
                "family": list(members),
                "root": root,
            }

    # Имена моделей (наименование корня семьи)
    name_by_vc = {}
    for row in raw_rows:
        vc = row["vendor_code"]
        if vc and row.get("name"):
            name_by_vc[vc] = row["name"]

    model_map = get_setting_json("own_wh_model_map", {}) or {}
    by_vendor, models = _apply_own_wh_model_map(auto_by_vendor, personal, name_by_vc, model_map)

    out = []
    seen_vc = set()
    for row in raw_rows:
        vc = row["vendor_code"]
        if vc and vc in seen_vc and not row["name"] and not row["has_stock_cell"]:
            continue
        if vc:
            seen_vc.add(vc)
        meta = by_vendor.get(vc, {}) if vc else {}
        out.append({
            "vendor_code": vc,
            "name": row["name"],
            "model_name": meta.get("model_name") or row["name"],
            "model_root": meta.get("root"),
            "model_manual": bool(vc and vc in model_map),
            "stock": meta.get("stock", row["stock"] or 0),
            "family_stock": meta.get("family_stock", row["stock"] or 0),
            "family": meta.get("family", [vc] if vc else []),
            "note": row["note"],
        })

    return {
        "title": title or "Остатки на складе",
        "as_of": as_of,
        "rows": out,
        "by_vendor": by_vendor,
        "models": models,
        "model_map": model_map,
        "personal": personal,
        "name_by_vc": name_by_vc,
        "auto_by_vendor": auto_by_vendor,
        "updated_at": datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M"),
        "error": None,
    }


def _apply_own_wh_model_map(auto_by_vendor: dict, personal: dict, name_by_vc: dict, model_map: dict):
    """Пересобирает семьи с учётом ручных привязок артикул → корень модели.
    model_map: {vendor_code: root_vendor_code}. Если root == vendor — отдельно."""
    model_map = {str(k): str(v) for k, v in (model_map or {}).items() if k and v}

    all_vcs = set(auto_by_vendor.keys()) | set(personal.keys()) | set(model_map.keys())
    # эффективный корень
    root_of = {}
    for vc in all_vcs:
        if vc in model_map:
            root_of[vc] = model_map[vc]
        else:
            root_of[vc] = (auto_by_vendor.get(vc) or {}).get("root") or vc

    def resolve(vc, depth=0):
        if depth > 8:
            return vc
        r = root_of.get(vc, vc)
        if r == vc:
            return vc
        # если корень сам переназначен — идём дальше
        rr = root_of.get(r, r)
        if rr != r:
            return resolve(r, depth + 1)
        return r

    groups = {}
    for vc in all_vcs:
        r = resolve(vc)
        groups.setdefault(r, [])
        if vc not in groups[r]:
            groups[r].append(vc)

    by_vendor = {}
    models = []
    for root, members in sorted(groups.items(), key=lambda x: x[0]):
        members = sorted(members)
        fam_stock = sum(personal.get(m, 0) for m in members)
        model_name = name_by_vc.get(root) or (auto_by_vendor.get(root) or {}).get("model_name")
        if not model_name:
            # любое имя из членов
            for m in members:
                if name_by_vc.get(m):
                    model_name = name_by_vc[m]
                    break
        if not model_name:
            model_name = root
        models.append({
            "root": root,
            "name": model_name,
            "members": members,
            "family_stock": fam_stock,
        })
        for m in members:
            by_vendor[m] = {
                "stock": personal.get(m, 0),
                "family_stock": fam_stock,
                "family": members,
                "root": root,
                "model_name": model_name,
                "manual": m in model_map,
            }
    models.sort(key=lambda x: (x["name"] or "").lower())
    return by_vendor, models


def _rebuild_own_wh_from_cache():
    """Пересчитывает by_vendor/rows из кэша + актуальной model_map (без повторного Google Sheets)."""
    auto = OWN_WAREHOUSE_CACHE.get("auto_by_vendor") or {}
    personal = OWN_WAREHOUSE_CACHE.get("personal") or {}
    name_by_vc = OWN_WAREHOUSE_CACHE.get("name_by_vc") or {}
    if not auto and not personal:
        return False
    model_map = get_setting_json("own_wh_model_map", {}) or {}
    by_vendor, models = _apply_own_wh_model_map(auto, personal, name_by_vc, model_map)
    OWN_WAREHOUSE_CACHE["by_vendor"] = by_vendor
    OWN_WAREHOUSE_CACHE["models"] = models
    OWN_WAREHOUSE_CACHE["model_map"] = model_map
    # обновить поля в rows
    rows = OWN_WAREHOUSE_CACHE.get("rows") or []
    new_rows = []
    for row in rows:
        vc = row.get("vendor_code")
        meta = by_vendor.get(vc, {}) if vc else {}
        new_rows.append({
            **row,
            "model_name": meta.get("model_name") or row.get("name"),
            "model_root": meta.get("root"),
            "model_manual": bool(vc and vc in model_map),
            "stock": meta.get("stock", row.get("stock") or 0),
            "family_stock": meta.get("family_stock", row.get("stock") or 0),
            "family": meta.get("family", [vc] if vc else []),
        })
    OWN_WAREHOUSE_CACHE["rows"] = new_rows
    return True


def refresh_own_warehouse_stock():
    OWN_WAREHOUSE_CACHE["syncing"] = True
    OWN_WAREHOUSE_CACHE["error"] = None
    try:
        data = fetch_own_warehouse_stock()
        OWN_WAREHOUSE_CACHE.update(data)
        OWN_WAREHOUSE_CACHE["syncing"] = False
        logger.info(f"own-warehouse: {len(data['rows'])} rows, as_of={data.get('as_of')}")
    except Exception as e:
        logger.error(f"own-warehouse refresh error: {e}")
        OWN_WAREHOUSE_CACHE["syncing"] = False
        OWN_WAREHOUSE_CACHE["error"] = str(e)

@app.get("/api/own-warehouse-stock")
def get_own_warehouse_stock(refresh: bool = False):
    """Остатки нашего склада из Google Sheets."""
    if refresh or not OWN_WAREHOUSE_CACHE.get("rows"):
        if OWN_WAREHOUSE_CACHE.get("syncing"):
            return {**OWN_WAREHOUSE_CACHE, "syncing": True}
        refresh_own_warehouse_stock()
    else:
        # подтянуть актуальные ручные привязки моделей
        if not _rebuild_own_wh_from_cache():
            refresh_own_warehouse_stock()
    return {
        "title": OWN_WAREHOUSE_CACHE.get("title"),
        "as_of": OWN_WAREHOUSE_CACHE.get("as_of"),
        "rows": OWN_WAREHOUSE_CACHE.get("rows") or [],
        "by_vendor": OWN_WAREHOUSE_CACHE.get("by_vendor") or {},
        "models": OWN_WAREHOUSE_CACHE.get("models") or [],
        "model_map": OWN_WAREHOUSE_CACHE.get("model_map") or {},
        "updated_at": OWN_WAREHOUSE_CACHE.get("updated_at"),
        "error": OWN_WAREHOUSE_CACHE.get("error"),
        "syncing": OWN_WAREHOUSE_CACHE.get("syncing", False),
    }

@app.post("/api/own-warehouse-set-model")
async def own_warehouse_set_model(request: dict):
    """Привязать артикул к модели (корню семьи) или сбросить на авто из таблицы.
    body: {vendor_code, root} — root=null|'' сброс на авто; root=vendor_code — отдельно;
    root=другой артикул — в его семью."""
    vc = (request.get("vendor_code") or "").strip()
    if not vc:
        return {"error": "vendor_code required"}
    root = request.get("root")
    model_map = get_setting_json("own_wh_model_map", {}) or {}
    reset = request.get("reset") or root is None or root == ""
    if reset:
        model_map.pop(vc, None)
    else:
        root = str(root).strip()
        if not root:
            model_map.pop(vc, None)
        else:
            model_map[vc] = root
    if not save_setting_value("own_wh_model_map", model_map):
        return {"error": "не удалось сохранить в settings"}
    # если кэш пуст — подтянем лист
    if not OWN_WAREHOUSE_CACHE.get("auto_by_vendor"):
        refresh_own_warehouse_stock()
    else:
        _rebuild_own_wh_from_cache()
    return {
        "status": "ok",
        "model_map": OWN_WAREHOUSE_CACHE.get("model_map") or model_map,
        "by_vendor": OWN_WAREHOUSE_CACHE.get("by_vendor") or {},
        "models": OWN_WAREHOUSE_CACHE.get("models") or [],
        "rows": OWN_WAREHOUSE_CACHE.get("rows") or [],
    }

@app.post("/api/sync-own-warehouse")
def sync_own_warehouse():
    import threading
    if OWN_WAREHOUSE_CACHE.get("syncing"):
        return {"status": "already_running"}
    threading.Thread(target=refresh_own_warehouse_stock, daemon=True).start()
    return {"status": "started"}

# ---------- Рекомендации по поставкам: заказы + продажи по складам (WB Statistics API) ----------
# Заказано — /api/v1/supplier/orders, Выкупили — /api/v1/supplier/sales (только saleID, начинающиеся
# на "S" — это продажи; "R" — возврат, "D" — доплата, их не считаем). Текущий остаток берём из уже
# собранной stock_warehouses (результат sync_stock). Объединяем по nm_id + warehouseName.

def fetch_supplier_feed(endpoint: str, date_from_iso: str, max_pages: int = 5) -> list:
    all_rows = []
    cursor = date_from_iso
    for _ in range(max_pages):
        resp = httpx.get(
            f"{WB_STATISTICS_URL}{endpoint}",
            headers=wb_headers(), params={"dateFrom": cursor}, timeout=60
        )
        if not resp.is_success:
            logger.error(f"WB {endpoint} error {resp.status_code} {resp.text[:200]}")
            break
        batch = resp.json()
        if not batch:
            break
        all_rows.extend(batch)
        cursor = batch[-1].get("lastChangeDate", cursor)
        if len(batch) < 50000:
            break
        time.sleep(61)  # лимит — 1 запрос в минуту на этот метод
    return all_rows

def get_setting_int(key: str, default: int) -> int:
    try:
        resp = httpx.get(f"{SUPABASE_URL}/rest/v1/settings?key=eq.{key}&select=value", headers=sb_headers(), timeout=10)
        if resp.is_success and resp.json():
            return int(resp.json()[0]["value"])
    except Exception:
        pass
    return default

def get_setting_raw(key: str, default=None):
    try:
        resp = httpx.get(
            f"{SUPABASE_URL}/rest/v1/settings?key=eq.{key}&select=value",
            headers=sb_headers(), timeout=10
        )
        if resp.is_success and resp.json():
            return resp.json()[0]["value"]
    except Exception:
        pass
    return default

def get_setting_json(key: str, default=None):
    if default is None:
        default = {}
    raw = get_setting_raw(key, None)
    if raw is None:
        return default
    try:
        import json as _json
        val = _json.loads(raw) if isinstance(raw, str) else raw
        return val if val is not None else default
    except Exception:
        return default

def save_setting_value(key: str, value) -> bool:
    import json as _json
    payload = {
        "key": key,
        "value": value if isinstance(value, str) else _json.dumps(value, ensure_ascii=False),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        resp = httpx.post(
            f"{SUPABASE_URL}/rest/v1/settings?on_conflict=key",
            json=payload,
            headers=sb_headers(), timeout=10
        )
        return resp.is_success
    except Exception as e:
        logger.error(f"save_setting_value({key}) error: {e}")
        return False

def parse_wb_dt(s: str):
    """WB отдаёт даты в orders/sales без таймзоны (например '2026-06-10T10:00:00').
    Нормализуем всё к naive datetime, чтобы сравнения не падали на tz-aware/naive."""
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is not None:
            dt = dt.replace(tzinfo=None)
        return dt
    except (ValueError, TypeError):
        return None

def fetch_planned_supplies_qty() -> dict:
    """Возвращает {nmId: суммарное количество} по поставкам (FBW), у которых дата
    поставки (supplyDate) попадает в ближайшие 7 дней, и которые ещё не приняты складом
    (factDate пусто) — то есть реально едут/запланированы, а не черновик и не уже приехали."""
    try:
        resp = httpx.post(
            f"{WB_SUPPLIES_URL}/api/v1/supplies",
            headers=wb_headers(), params={"limit": 1000, "offset": 0},
            json={}, timeout=30
        )
        if not resp.is_success:
            logger.error(f"WB supplies list error {resp.status_code} {resp.text[:300]}")
            return {}
        supplies = resp.json()
        if not isinstance(supplies, list):
            logger.error(f"WB supplies list unexpected shape: {str(supplies)[:300]}")
            return {}
    except Exception as e:
        logger.error(f"WB supplies list exception: {e}")
        return {}

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    horizon = now + timedelta(days=7)
    qualifying = []
    for s in supplies:
        if s.get("factDate"):
            continue
        sd = s.get("supplyDate")
        if not sd:
            continue
        try:
            d = datetime.fromisoformat(str(sd).replace("Z", "+00:00")).replace(tzinfo=None)
        except (ValueError, TypeError):
            continue
        if now <= d <= horizon:
            sid, is_preorder = s.get("supplyID"), False
            if not sid:
                sid, is_preorder = s.get("preorderID"), True
            if sid:
                qualifying.append((sid, is_preorder))

    logger.info(f"Planned FBW supplies in next 7 days: {len(qualifying)}")
    planned = {}
    for sid, is_preorder in qualifying:
        try:
            params = {"limit": 1000, "offset": 0}
            if is_preorder:
                params["isPreorderID"] = "true"
            gresp = httpx.get(
                f"{WB_SUPPLIES_URL}/api/v1/supplies/{sid}/goods",
                headers=wb_headers(), params=params, timeout=20
            )
            if not gresp.is_success:
                logger.error(f"WB supply goods error supply={sid} {gresp.status_code} {gresp.text[:200]}")
                continue
            for item in gresp.json():
                nm = item.get("nmID") or item.get("nmId")
                qty = item.get("quantity", 0) or 0
                if nm:
                    planned[nm] = planned.get(nm, 0) + qty
        except Exception as e:
            logger.error(f"WB supply goods exception supply={sid}: {e}")
        time.sleep(0.1)
    return planned

def sync_supply():
    if not WB_TOKEN:
        logger.error("WB_TOKEN not set")
        return
    logger.info("Starting supply (orders/sales) sync...")
    window_days = get_setting_int("sales_window_days", 14)
    cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=window_days)
    date_from = cutoff.strftime("%Y-%m-%dT00:00:00")

    orders = fetch_supplier_feed("/api/v1/supplier/orders", date_from)
    time.sleep(61)  # отдельный лимит 1 запрос/мин на каждый метод
    sales = fetch_supplier_feed("/api/v1/supplier/sales", date_from)
    logger.info(f"Supply sync: fetched {len(orders)} order rows, {len(sales)} sale rows")

    agg = {}  # (nm_id, warehouseName) -> {"ordered":int,"buyout":int,"vendor_code":str}
    nm_to_barcode = {}
    for o in orders:
        d = parse_wb_dt(o.get("date", ""))
        if d is None or d < cutoff:
            continue
        key = (o.get("nmId"), o.get("warehouseName"))
        a = agg.setdefault(key, {"ordered": 0, "buyout": 0, "vendor_code": ""})
        a["ordered"] += 1
        if o.get("supplierArticle"):
            a["vendor_code"] = o["supplierArticle"]
        if o.get("barcode") and o.get("nmId"):
            nm_to_barcode[o["nmId"]] = o["barcode"]

    for s in sales:
        if not str(s.get("saleID", "")).startswith("S"):
            continue  # пропускаем возвраты (R) и доплаты (D)
        d = parse_wb_dt(s.get("date", ""))
        if d is None or d < cutoff:
            continue
        key = (s.get("nmId"), s.get("warehouseName"))
        a = agg.setdefault(key, {"ordered": 0, "buyout": 0, "vendor_code": ""})
        a["buyout"] += 1
        if s.get("supplierArticle"):
            a["vendor_code"] = s["supplierArticle"]
        if s.get("barcode") and s.get("nmId"):
            nm_to_barcode[s["nmId"]] = s["barcode"]

    try:
        st = httpx.get(f"{SUPABASE_URL}/rest/v1/stock_totals?select=nm_id,vendor_code", headers=sb_headers(), timeout=15)
        nm_to_vendor = {r["nm_id"]: r["vendor_code"] for r in st.json()} if st.is_success else {}
    except Exception as e:
        logger.error(f"sync_supply: stock_totals fetch error {e}")
        nm_to_vendor = {}

    try:
        sw = httpx.get(f"{SUPABASE_URL}/rest/v1/stock_warehouses?select=nm_id,warehouse_name,quantity", headers=sb_headers(), timeout=20)
        stock_map = {(r["nm_id"], r["warehouse_name"]): r["quantity"] for r in sw.json()} if sw.is_success else {}
    except Exception as e:
        logger.error(f"sync_supply: stock_warehouses fetch error {e}")
        stock_map = {}

    planned_map = fetch_planned_supplies_qty()

    keys = set(agg.keys()) | set(stock_map.keys())
    now = datetime.now(timezone.utc).isoformat()
    rows = []
    for nm_id, wh in keys:
        if not nm_id or not wh:
            continue
        a = agg.get((nm_id, wh), {"ordered": 0, "buyout": 0, "vendor_code": ""})
        rows.append({
            "vendor_code": nm_to_vendor.get(nm_id) or a["vendor_code"] or str(nm_id),
            "nm_id": nm_id,
            "barcode": nm_to_barcode.get(nm_id),
            "planned_supply_qty": planned_map.get(nm_id, 0),
            "warehouse_name": wh,
            "ordered_qty": a["ordered"],
            "buyout_qty": a["buyout"],
            "current_stock": stock_map.get((nm_id, wh), 0),
            "period_days": window_days,
            "period_start": None,
            "period_end": None,
            "updated_at": now,
        })

    httpx.delete(f"{SUPABASE_URL}/rest/v1/supply_report?id=gte.0", headers=sb_headers(), timeout=15)
    saved = 0
    for i in range(0, len(rows), 300):
        batch = rows[i:i + 300]
        resp = httpx.post(f"{SUPABASE_URL}/rest/v1/supply_report", json=batch, headers=sb_headers(), timeout=30)
        if resp.is_success:
            saved += len(batch)
        else:
            logger.error(f"supply_report insert error {resp.status_code} {resp.text[:300]}")

    httpx.post(
        f"{SUPABASE_URL}/rest/v1/settings",
        json={"key": "last_supply_sync", "value": datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M"),
              "updated_at": now},
        headers=sb_headers(), timeout=10
    )
    logger.info(f"Supply sync complete. Rows saved: {saved}")

# ---------- Продвижение (реклама) ----------

AD_TYPE_NAMES = {
    3: "Карточка товара", 4: "Каталог+поиск", 5: "Карточка",
    6: "Каталог", 8: "Автоматическая", 9: "Поиск"
}

ADS_CACHE = {
    "campaigns": [],
    "updated_at": None,
    "syncing": False,
    "error": None,
    "progress": None,
    "window_days": None,
    "period_begin": None,
    "period_end": None,
}

def _parse_advert_v2_items(payload) -> list:
    if payload is None:
        return []
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("adverts", "data", "items"):
            val = payload.get(key)
            if isinstance(val, list):
                return val
            if isinstance(val, dict) and isinstance(val.get("adverts"), list):
                return val["adverts"]
    return []

def _advert_id_from_item(camp: dict):
    return camp.get("advertId") or camp.get("advert_id") or camp.get("id")

def _advert_name_from_item(camp: dict) -> str:
    """В v2 название лежит в settings.name; в старых ответах — в name."""
    name = (camp.get("name") or "").strip()
    if name:
        return name
    settings = camp.get("settings")
    if isinstance(settings, dict):
        name = (settings.get("name") or "").strip()
        if name:
            return name
    return ""

def _advert_type_from_item(camp: dict) -> int:
    type_id = camp.get("type") or camp.get("type_id")
    if type_id:
        return int(type_id)
    settings = camp.get("settings") if isinstance(camp.get("settings"), dict) else {}
    # иногда тип оплаты/ставки вместо type — оставим 0
    return int(settings.get("type") or 0)

def fetch_campaigns_meta(include_finished: bool = False) -> dict:
    """Активные (9) и на паузе (11). Имена — как в кабинете WB (settings.name)."""
    allowed = (9, 11, 7) if include_finished else (9, 11)
    statuses = "9,11,7" if include_finished else "9,11"
    meta = {}

    # 1) основной путь: /api/advert/v2/adverts
    try:
        param_sets = [
            {"statuses": statuses},
            {"statuses": statuses, "payment_type": "cpm"},
            {"statuses": statuses, "payment_type": "cpc"},
        ]
        for params in param_sets:
            resp = httpx.get(
                f"{WB_PROMOTION_URL}/api/advert/v2/adverts",
                headers=wb_headers(),
                params=params,
                timeout=30,
            )
            if not resp.is_success:
                logger.warning(f"advert/v2/adverts {resp.status_code} params={params}: {resp.text[:200]}")
                continue
            items = _parse_advert_v2_items(resp.json())
            if items and not meta:
                # лог структуры один раз — чтобы видеть поля, если снова сломается
                sample = items[0] if isinstance(items[0], dict) else {}
                logger.info(
                    f"advert/v2 sample keys={list(sample.keys())[:20]} "
                    f"settings_keys={list((sample.get('settings') or {}).keys())[:15] if isinstance(sample.get('settings'), dict) else None}"
                )
            for camp in items:
                if not isinstance(camp, dict):
                    continue
                aid = _advert_id_from_item(camp)
                if not aid:
                    continue
                status = camp.get("status")
                if status is not None and status not in allowed:
                    continue
                type_id = _advert_type_from_item(camp)
                type_name = AD_TYPE_NAMES.get(type_id, f"Тип {type_id}" if type_id else "")
                name = _advert_name_from_item(camp)
                prev = meta.get(aid) or {}
                meta[aid] = {
                    "type_id": type_id or prev.get("type_id") or 0,
                    "type_name": type_name or prev.get("type_name") or "",
                    "status": status if status is not None else prev.get("status"),
                    "name": name or prev.get("name") or "",
                }
            # не break: cpm+cpc могут дополнять друг друга
    except Exception as e:
        logger.error(f"WB advert/v2/adverts exception: {e}")

    # 2) fallback: список id из count
    if not meta:
        try:
            resp = httpx.get(f"{WB_PROMOTION_URL}/adv/v1/promotion/count", headers=wb_headers(), timeout=20)
            if resp.is_success:
                for group in (resp.json() or {}).get("adverts", []) or []:
                    type_id = group.get("type", 0)
                    type_name = AD_TYPE_NAMES.get(type_id, f"Тип {type_id}")
                    status = group.get("status")
                    if status not in allowed:
                        continue
                    for item in group.get("advert_list", []) or []:
                        aid = item.get("advertId")
                        if aid:
                            meta[aid] = {
                                "type_id": type_id,
                                "type_name": type_name,
                                "status": status,
                                "name": "",
                            }
        except Exception as e:
            logger.error(f"WB promotion/count exception: {e}")

    enrich_campaign_names(meta)
    for aid, m in meta.items():
        if not (m.get("name") or "").strip() or is_placeholder_campaign_name(m.get("name") or "", aid):
            # последний шанс — не оставляем голое «Кампания»
            if not (m.get("name") or "").strip() or m.get("name") == "Кампания":
                m["name"] = (m.get("type_name") and f"{m['type_name']} #{aid}") or f"#{aid}"
    return meta

def enrich_campaign_names(meta: dict) -> None:
    """Дотягивает названия через /api/advert/v2/adverts?ids=... и старый promotion/adverts."""
    need = [
        aid for aid, m in meta.items()
        if is_placeholder_campaign_name(m.get("name") or "", aid)
    ]
    if not need:
        return

    for i in range(0, len(need), 50):
        batch = need[i:i + 50]
        got = False
        try:
            resp = httpx.get(
                f"{WB_PROMOTION_URL}/api/advert/v2/adverts",
                headers=wb_headers(),
                params={"ids": ",".join(str(x) for x in batch)},
                timeout=20,
            )
            if resp.is_success:
                for camp in _parse_advert_v2_items(resp.json()):
                    if not isinstance(camp, dict):
                        continue
                    aid = _advert_id_from_item(camp)
                    if aid and aid in meta:
                        name = _advert_name_from_item(camp)
                        if name:
                            meta[aid]["name"] = name
                            got = True
                        type_id = _advert_type_from_item(camp)
                        if type_id:
                            meta[aid]["type_id"] = type_id
                            meta[aid]["type_name"] = AD_TYPE_NAMES.get(type_id, meta[aid].get("type_name"))
            else:
                logger.warning(f"advert/v2 names {resp.status_code}: {resp.text[:200]}")
        except Exception as e:
            logger.warning(f"campaign names v2 skip: {e}")

        # старый endpoint — на случай если v2 без settings.name
        still = [aid for aid in batch if is_placeholder_campaign_name(meta[aid].get("name") or "", aid)]
        if still:
            try:
                resp = httpx.post(
                    f"{WB_PROMOTION_URL}/adv/v1/promotion/adverts",
                    json=still, headers=wb_headers(), timeout=20
                )
                if resp.is_success:
                    for camp in resp.json() or []:
                        aid = camp.get("advertId") or camp.get("advert_id")
                        if aid and aid in meta:
                            name = (camp.get("name") or "").strip()
                            if name:
                                meta[aid]["name"] = name
                                got = True
            except Exception as e:
                logger.warning(f"campaign names v1 skip: {e}")
        if i + 50 < len(need):
            time.sleep(0.3)
        logger.info(f"enrich names batch {i//50+1}: need={len(batch)} got_any={got}")

def is_placeholder_campaign_name(name: str, aid) -> bool:
    """True если имя — заглушка, а не название из кабинета WB."""
    n = (name or "").strip()
    if not n:
        return True
    if n in ("Кампания", "Без названия", "Неизвестно"):
        return True
    if n == f"#{aid}" or n == f"Кампания #{aid}":
        return True
    if n.endswith(f" #{aid}"):
        return True
    return False

def fetch_ad_stats_by_campaign(ids: list, begin_date: str, end_date: str) -> dict:
    """Тянет /adv/v3/fullstats → {campaign_id: метрики}. Пауза 20с только между батчами."""
    agg = {}
    errors = []
    total_batches = max(1, (len(ids) + 49) // 50)
    for i in range(0, len(ids), 50):
        batch = ids[i:i + 50]
        batch_no = i // 50 + 1
        ADS_CACHE["progress"] = f"статистика WB {batch_no}/{total_batches}"
        try:
            resp = httpx.get(
                f"{WB_PROMOTION_URL}/adv/v3/fullstats",
                headers=wb_headers(),
                params={"ids": ",".join(str(x) for x in batch), "beginDate": begin_date, "endDate": end_date},
                timeout=60,
            )
            if resp.status_code == 429:
                logger.warning("WB fullstats 429 — ждём 22с и повторяем")
                ADS_CACHE["progress"] = f"лимит WB, ждём… ({batch_no}/{total_batches})"
                time.sleep(22)
                resp = httpx.get(
                    f"{WB_PROMOTION_URL}/adv/v3/fullstats",
                    headers=wb_headers(),
                    params={"ids": ",".join(str(x) for x in batch), "beginDate": begin_date, "endDate": end_date},
                    timeout=60,
                )
            if not resp.is_success:
                msg = f"fullstats {resp.status_code}: {resp.text[:300]}"
                logger.error(f"WB {msg}")
                errors.append(msg)
                # при 429 на базовом тарифе дальше бессмысленно долбить
                if resp.status_code == 429:
                    break
                continue
            campaigns = resp.json()
        except Exception as e:
            logger.error(f"WB fullstats exception: {e}")
            errors.append(str(e))
            continue

        for camp in campaigns or []:
            campaign_id = camp.get("advertId")
            if not campaign_id:
                continue
            views = int(camp.get("views") or 0)
            clicks = int(camp.get("clicks") or 0)
            atbs = int(camp.get("atbs") or 0)
            orders = int(camp.get("orders") or 0)
            spend = float(camp.get("sum") or 0)
            revenue = float(camp.get("sum_price") or 0)

            if not (views or clicks or orders or spend) and camp.get("days"):
                for day in camp.get("days") or []:
                    views += int(day.get("views") or 0)
                    clicks += int(day.get("clicks") or 0)
                    atbs += int(day.get("atbs") or 0)
                    orders += int(day.get("orders") or 0)
                    spend += float(day.get("sum") or 0)
                    revenue += float(day.get("sum_price") or 0)

            a = agg.setdefault(campaign_id, {
                "views": 0, "clicks": 0, "atbs": 0,
                "orders": 0, "spend": 0.0, "revenue": 0.0,
            })
            a["views"] += views
            a["clicks"] += clicks
            a["atbs"] += atbs
            a["orders"] += orders
            a["spend"] += spend
            a["revenue"] += revenue

        if i + 50 < len(ids):
            time.sleep(20)
    if errors and not agg:
        raise RuntimeError("; ".join(errors[:3]))
    return agg

def _ads_period_dates():
    """Период статистики рекламы: ads_date_from/to или окно ads_window_days (макс. 31 день)."""
    today = datetime.now(timezone.utc).date()
    raw_from = get_setting_raw("ads_date_from", None)
    raw_to = get_setting_raw("ads_date_to", None)
    begin = end = None
    try:
        if raw_from:
            begin = datetime.strptime(str(raw_from)[:10], "%Y-%m-%d").date()
        if raw_to:
            end = datetime.strptime(str(raw_to)[:10], "%Y-%m-%d").date()
    except Exception:
        begin = end = None
    if begin and end:
        if end < begin:
            begin, end = end, begin
        # лимит WB fullstats — 31 день
        if (end - begin).days > 30:
            begin = end - timedelta(days=30)
        return begin, end
    window_days = get_setting_int("ads_window_days", 7)
    window_days = min(max(window_days, 1), 31)
    end = today
    begin = end - timedelta(days=window_days - 1)
    return begin, end

def sync_ads():
    """Синк рекламы: список кампаний с затратами/показами/ДРР/заказами."""
    if not WB_TOKEN:
        ADS_CACHE["error"] = "WB_TOKEN не задан"
        logger.error("WB_TOKEN not set")
        return
    if ADS_CACHE.get("syncing"):
        return
    ADS_CACHE["syncing"] = True
    ADS_CACHE["error"] = None
    ADS_CACHE["progress"] = "список кампаний…"
    try:
        logger.info("Starting ads (promotion) sync...")
        begin_date, end_date = _ads_period_dates()
        window_days = (end_date - begin_date).days + 1

        # только активные + пауза — иначе тянем сотни завершённых и ждём по 20с на батч
        campaigns_meta = fetch_campaigns_meta(include_finished=False)
        if not campaigns_meta:
            ADS_CACHE["error"] = "Нет активных или на паузе кампаний"
            logger.info("Ads sync: no eligible campaigns")
            return

        logger.info(f"Ads sync: {len(campaigns_meta)} campaigns, {begin_date}…{end_date}")
        agg = fetch_ad_stats_by_campaign(
            list(campaigns_meta.keys()), begin_date.isoformat(), end_date.isoformat()
        )

        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()
        campaigns = []
        rows = []
        for campaign_id, meta in campaigns_meta.items():
            a = agg.get(campaign_id) or {
                "views": 0, "clicks": 0, "atbs": 0, "orders": 0, "spend": 0.0, "revenue": 0.0,
            }
            views, clicks, atbs, orders = a["views"], a["clicks"], a["atbs"], a["orders"]
            spend, revenue = round(a["spend"], 2), round(a["revenue"], 2)
            drr = round(spend / revenue * 100, 2) if revenue else 0
            ctr = round(clicks / views * 100, 2) if views else 0
            cpc = round(spend / clicks, 2) if clicks else 0
            cr = round(orders / clicks * 100, 2) if clicks else 0
            name = meta.get("name") or f"#{campaign_id}"
            item = {
                "campaign_id": campaign_id,
                "campaign_name": name,
                "campaign_type": meta.get("type_name") or "Неизвестно",
                "views": views,
                "clicks": clicks,
                "atbs": atbs,
                "orders": orders,
                "spend": spend,
                "revenue": revenue,
                "drr": drr,
                "ctr": ctr,
                "cpc": cpc,
                "cr": cr,
                "cv_atb": round(atbs / clicks * 100, 2) if clicks else 0,
                "cv_ord": round(orders / atbs * 100, 2) if atbs else 0,
                "period_days": window_days,
                "updated_at": now_iso,
                "vendor_code": name,
                "nm_id": campaign_id,
            }
            campaigns.append(item)
            rows.append(item)

        campaigns.sort(key=lambda c: (-(c["spend"] or 0), str(c["campaign_name"])))

        ADS_CACHE["campaigns"] = campaigns
        ADS_CACHE["updated_at"] = now.strftime("%d.%m.%Y %H:%M")
        ADS_CACHE["window_days"] = window_days
        ADS_CACHE["period_begin"] = begin_date.isoformat()
        ADS_CACHE["period_end"] = end_date.isoformat()
        ADS_CACHE["error"] = None
        ADS_CACHE["progress"] = None

        if rows:
            try:
                httpx.delete(f"{SUPABASE_URL}/rest/v1/ad_stats?id=gte.0", headers=sb_headers(), timeout=15)
                saved = 0
                for i in range(0, len(rows), 200):
                    batch = rows[i:i + 200]
                    resp = httpx.post(f"{SUPABASE_URL}/rest/v1/ad_stats", json=batch, headers=sb_headers(), timeout=30)
                    if resp.is_success:
                        saved += len(batch)
                    else:
                        logger.error(f"ad_stats insert error {resp.status_code} {resp.text[:300]}")
                logger.info(f"Ads sync: saved {saved}/{len(rows)} rows to supabase")
            except Exception as e:
                logger.error(f"Ads sync supabase write error: {e}")

        httpx.post(
            f"{SUPABASE_URL}/rest/v1/settings",
            json={"key": "last_ads_sync", "value": ADS_CACHE["updated_at"], "updated_at": now_iso},
            headers=sb_headers(), timeout=10,
        )
        logger.info(f"Ads sync complete. Campaigns: {len(campaigns)}")
    except Exception as e:
        logger.error(f"sync_ads error: {e}")
        ADS_CACHE["error"] = str(e)
        ADS_CACHE["progress"] = None
    finally:
        ADS_CACHE["syncing"] = False
        ADS_CACHE["progress"] = None

@app.get("/api/ads")
def get_ads(refresh: bool = False):
    """Список кампаний с метриками. refresh=1 — запустить синк."""
    if refresh and not ADS_CACHE.get("syncing"):
        import threading
        threading.Thread(target=sync_ads, daemon=True).start()
    camps = ADS_CACHE.get("campaigns") or []
    # если кэш пуст — подтянем из supabase (после рестарта)
    if not camps and not ADS_CACHE.get("syncing"):
        try:
            ads = httpx.get(f"{SUPABASE_URL}/rest/v1/ad_stats?select=*", headers=sb_headers(), timeout=20)
            if ads.is_success:
                raw = ads.json() or []
                by = {}
                for r in raw:
                    cid = r.get("campaign_id")
                    if cid is None:
                        continue
                    if cid not in by:
                        by[cid] = {
                            "campaign_id": cid,
                            "campaign_name": r.get("campaign_name") or f"#{cid}",
                            "campaign_type": r.get("campaign_type") or "",
                            "views": 0, "clicks": 0, "atbs": 0, "orders": 0,
                            "spend": 0.0, "revenue": 0.0, "drr": 0,
                            "vendor_code": r.get("campaign_name") or str(cid),
                            "nm_id": cid,
                        }
                    c = by[cid]
                    c["views"] += r.get("views") or 0
                    c["clicks"] += r.get("clicks") or 0
                    c["atbs"] += r.get("atbs") or 0
                    c["orders"] += r.get("orders") or 0
                    c["spend"] += float(r.get("spend") or 0)
                    c["revenue"] += float(r.get("revenue") or 0)
                for c in by.values():
                    c["spend"] = round(c["spend"], 2)
                    c["revenue"] = round(c["revenue"], 2)
                    c["drr"] = round(c["spend"] / c["revenue"] * 100, 2) if c["revenue"] else 0
                camps = sorted(by.values(), key=lambda x: (-x["spend"], str(x["campaign_name"])))
                ADS_CACHE["campaigns"] = camps
        except Exception as e:
            logger.error(f"get_ads supabase fallback: {e}")
    return {
        "campaigns": camps,
        "ad_stats": camps,  # алиас под старый фронт
        "updated_at": ADS_CACHE.get("updated_at"),
        "syncing": ADS_CACHE.get("syncing", False),
        "error": ADS_CACHE.get("error"),
        "progress": ADS_CACHE.get("progress"),
        "window_days": ADS_CACHE.get("window_days"),
        "period_begin": ADS_CACHE.get("period_begin"),
        "period_end": ADS_CACHE.get("period_end"),
    }

# ---------- Календарь акций (Промо) ----------
# Данные акций держим в кэше процесса: WB жёстко лимитирует Календарь акций
# (10 запросов / 6 сек), а на построение матрицы нужно 2 запроса на каждую акцию.
# Обновляется по кнопке, по расписанию и при первом обращении к вкладке.
PROMO_CACHE = {"promotions": [], "articles": [], "updated_at": None, "syncing": False, "error": None}
PROMO_RATE_DELAY = 0.7  # пауза между запросами к Календарю акций (лимит WB: интервал 600 мс)

def fetch_calendar_promotions(start_dt: str, end_dt: str) -> list:
    """Список акций, доступных для участия, за период [start_dt, end_dt]."""
    promotions, offset, limit = [], 0, 1000
    while True:
        try:
            resp = httpx.get(
                f"{WB_CALENDAR_URL}/api/v1/calendar/promotions",
                headers=wb_headers(),
                params={"startDateTime": start_dt, "endDateTime": end_dt,
                        "allPromo": "false", "limit": limit, "offset": offset},
                timeout=30,
            )
        except Exception as e:
            logger.error(f"calendar promotions fetch error: {e}")
            break
        if not resp.is_success:
            logger.error(f"calendar promotions error {resp.status_code} {resp.text[:200]}")
            break
        batch = (resp.json().get("data") or {}).get("promotions") or []
        promotions.extend(batch)
        if len(batch) < limit:
            break
        offset += limit
        time.sleep(PROMO_RATE_DELAY)
    return promotions

def fetch_promotion_nomenclatures(promotion_id: int, in_action: bool) -> list:
    """Товары акции: in_action=False — можно добавить (есть planPrice для входа), True — уже участвуют."""
    noms, offset, limit = [], 0, 1000
    while True:
        try:
            resp = httpx.get(
                f"{WB_CALENDAR_URL}/api/v1/calendar/promotions/nomenclatures",
                headers=wb_headers(),
                params={"promotionID": promotion_id, "inAction": str(in_action).lower(),
                        "limit": limit, "offset": offset},
                timeout=30,
            )
        except Exception as e:
            logger.error(f"promo nomenclatures fetch error (promo {promotion_id}): {e}")
            break
        if not resp.is_success:
            # 400/404 — у акции просто нет подходящих товаров, это не критично
            if resp.status_code not in (400, 404):
                logger.error(f"promo nomenclatures error {resp.status_code} {resp.text[:200]}")
            break
        batch = (resp.json().get("data") or {}).get("nomenclatures") or []
        noms.extend(batch)
        if len(batch) < limit:
            break
        offset += limit
        time.sleep(PROMO_RATE_DELAY)
    return noms

def fetch_promotions_details(ids: list) -> dict:
    """Детали акций по ID → {promo_id: {...}}. Работает и для автоакций (в отличие от nomenclatures)."""
    out = {}
    for i in range(0, len(ids), 50):
        batch = ids[i:i + 50]
        try:
            resp = httpx.get(
                f"{WB_CALENDAR_URL}/api/v1/calendar/promotions/details",
                headers=wb_headers(),
                params=[("promotionIDs", str(x)) for x in batch],
                timeout=30,
            )
        except Exception as e:
            logger.error(f"promo details fetch error: {e}")
            continue
        if not resp.is_success:
            logger.error(f"promo details error {resp.status_code} {resp.text[:200]}")
            continue
        for d in ((resp.json().get("data") or {}).get("promotions") or []):
            pid = d.get("id")
            if pid is not None:
                out[pid] = d
        if i + 50 < len(ids):
            time.sleep(PROMO_RATE_DELAY)
    return out

def build_promo_nm_to_vendor() -> dict:
    """nm_id → артикул продавца: сначала из stock_totals, добираем из feedbacks."""
    nm_to_vendor = {}
    try:
        st = httpx.get(f"{SUPABASE_URL}/rest/v1/stock_totals?select=nm_id,vendor_code", headers=sb_headers(), timeout=15)
        if st.is_success:
            nm_to_vendor = {r["nm_id"]: r["vendor_code"] for r in st.json() if r.get("nm_id") and r.get("vendor_code")}
    except Exception as e:
        logger.error(f"sync_promotions: stock_totals fetch error {e}")
    try:
        fb = httpx.get(
            f"{SUPABASE_URL}/rest/v1/feedbacks?select=nm_id,article&nm_id=not.is.null&article=not.is.null",
            headers=sb_headers(), timeout=15
        )
        if fb.is_success:
            for r in fb.json():
                nm, art = r.get("nm_id"), r.get("article", "")
                if nm and art and nm not in nm_to_vendor:
                    nm_to_vendor[nm] = art
    except Exception as e:
        logger.error(f"sync_promotions: feedbacks fallback error {e}")
    return nm_to_vendor

def sync_promotions():
    """Строит матрицу: акции × артикулы, с ценой входа и разницей к текущей цене."""
    if not WB_TOKEN:
        logger.error("WB_TOKEN not set")
        PROMO_CACHE["error"] = "WB_TOKEN не задан"
        return
    if PROMO_CACHE.get("syncing"):
        logger.info("Promotions sync already running")
        return
    PROMO_CACHE["syncing"] = True
    PROMO_CACHE["error"] = None
    try:
        logger.info("Starting promotions (calendar) sync...")
        window_days = min(get_setting_int("promo_window_days", 60), 365)
        now = datetime.now(timezone.utc)
        start_dt = now.strftime("%Y-%m-%dT00:00:00Z")
        end_dt = (now + timedelta(days=window_days)).strftime("%Y-%m-%dT23:59:59Z")

        promos = fetch_calendar_promotions(start_dt, end_dt)
        logger.info(f"Promotions sync: {len(promos)} promotions in window")

        nm_to_vendor = build_promo_nm_to_vendor()

        # Детали по всем акциям — работают и для автоакций (участие товаров, охват, буст)
        promo_ids = [p.get("id") for p in promos if p.get("id") is not None]
        details = fetch_promotions_details(promo_ids) if promo_ids else {}

        promotions_out, articles = [], {}
        for p in promos:
            pid = p.get("id")
            if pid is None:
                continue
            start = p.get("startDateTime", "")
            end = p.get("endDateTime", "")
            days_to_start = None
            try:
                sd = datetime.fromisoformat(start.replace("Z", "+00:00"))
                days_to_start = (sd.date() - now.date()).days
            except Exception:
                pass
            ptype = p.get("type", "regular")
            d = details.get(pid, {})
            ranging = d.get("ranging") or []
            max_boost = max((r.get("boost", 0) or 0 for r in ranging), default=0)
            promotions_out.append({
                "id": pid,
                "name": p.get("name", f"#{pid}"),
                "start": start,
                "end": end,
                "type": ptype,
                "days_to_start": days_to_start,
                "in_total": d.get("inPromoActionTotal"),
                "in_leftovers": d.get("inPromoActionLeftovers"),
                "not_in_total": d.get("notInPromoActionTotal"),
                "not_in_leftovers": d.get("notInPromoActionLeftovers"),
                "participation": d.get("participationPercentage"),
                "exceptions": d.get("exceptionProductsCount"),
                "boost": max_boost,
            })
            # Список товаров с ценами входа доступен только для обычных акций.
            # Для автоакций WB не отдаёт номенклатуры — пропускаем, чтобы не ловить 400.
            if ptype != "regular":
                continue
            for in_action in (True, False):
                time.sleep(PROMO_RATE_DELAY)
                for n in fetch_promotion_nomenclatures(pid, in_action):
                    nm = n.get("id")
                    if nm is None:
                        continue
                    price = n.get("price")
                    plan_price = n.get("planPrice")
                    delta = round(price - plan_price) if (price is not None and plan_price is not None) else None
                    entry = articles.setdefault(nm, {
                        "nm_id": nm,
                        "vendor_code": nm_to_vendor.get(nm) or str(nm),
                        "cells": {},
                    })
                    entry["cells"][str(pid)] = {
                        "in_action": bool(n.get("inAction")),
                        "price": price,
                        "plan_price": plan_price,
                        "discount": n.get("discount"),
                        "plan_discount": n.get("planDiscount"),
                        "delta": delta,
                    }

        articles_list = sorted(articles.values(), key=lambda a: str(a["vendor_code"]))
        PROMO_CACHE["promotions"] = promotions_out
        PROMO_CACHE["articles"] = articles_list
        PROMO_CACHE["updated_at"] = datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M")
        logger.info(f"Promotions sync complete. Promotions: {len(promotions_out)}, articles: {len(articles_list)}")
    except Exception as e:
        logger.error(f"sync_promotions error: {e}")
        PROMO_CACHE["error"] = str(e)
    finally:
        PROMO_CACHE["syncing"] = False

@app.get("/api/promotions")
def get_promotions():
    # первый заход на вкладку — запускаем сбор данных в фоне
    if not PROMO_CACHE["updated_at"] and not PROMO_CACHE["syncing"]:
        import threading
        threading.Thread(target=sync_promotions, daemon=True).start()
    return {
        "promotions": PROMO_CACHE["promotions"],
        "articles": PROMO_CACHE["articles"],
        "updated_at": PROMO_CACHE["updated_at"],
        "syncing": PROMO_CACHE["syncing"],
        "error": PROMO_CACHE["error"],
    }

@app.post("/api/sync-promotions")
def trigger_promotions_sync():
    import threading
    threading.Thread(target=sync_promotions, daemon=True).start()
    return {"status": "started"}

def _parse_promo_excel_name(filename: str) -> str:
    """Из имени файла WB: «...для акции_<название>_<дата время>.xlsx»."""
    import re as _re
    name = (filename or "").rsplit("/", 1)[-1]
    name = _re.sub(r"\.xlsx?$", "", name, flags=_re.I)
    m = _re.search(r"для акции[_\s]+(.+?)_\d{1,2}\.\d{1,2}\.\d{2,4}", name, flags=_re.I)
    if m:
        return m.group(1).strip(" _-")
    m = _re.search(r"акци[ия][_\s]+(.+)$", name, flags=_re.I)
    if m:
        return m.group(1).strip(" _-")
    return name or "Акция"

@app.post("/api/upload-promo-excel")
async def upload_promo_excel(file: UploadFile = File(...)):
    """Парсит xlsx «Все товары подходящие для акции_…» из Календаря акций WB.
    Возвращает список артикулов с ценами/участием — фронт сам сортирует и хранит сессии."""
    try:
        from openpyxl import load_workbook
        import re as _re

        contents = await file.read()
        if not contents:
            return {"error": "Пустой файл"}

        wb = load_workbook(io.BytesIO(contents), data_only=False)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            return {"error": "Пустой лист"}

        # Ищем строку заголовков
        header_i = None
        header = None
        for i, row in enumerate(rows[:15]):
            vals = [str(c or "").strip().lower() for c in row]
            joined = " | ".join(vals)
            if "артикул" in joined and ("планов" in joined or "участ" in joined or "wb" in joined):
                header_i = i
                header = [str(c or "").strip() for c in row]
                break
        if header_i is None:
            return {"error": "Не найдены заголовки (нужны колонки артикул / плановая цена). Проверь, что это файл из Календаря акций."}

        def find_col(*needles):
            for j, h in enumerate(header):
                hl = h.lower()
                if all(n.lower() in hl for n in needles):
                    return j
            return None

        col_in = find_col("участ")  # «Товар уже участвует в акции»
        col_brand = find_col("бренд")
        col_subject = find_col("предмет")
        col_name = find_col("наименование")
        col_vc = find_col("артикул поставщика") or find_col("артикул продавца")
        col_nm = find_col("артикул wb") or find_col("артикул вб")
        col_turn = find_col("оборачиваемость")
        col_stock_wb = find_col("остаток", "складах") or find_col("остаток товара на складах")
        col_stock_seller = find_col("остаток", "продавца")
        col_plan = find_col("плановая")
        col_price = find_col("текущая розничная") or find_col("розничная цена")
        col_cur_disc = find_col("текущая скидка")
        col_load_disc = find_col("загружаемая скидка")
        col_status = find_col("статус")

        if col_vc is None and col_nm is None:
            return {"error": "Нет колонки артикула поставщика / WB"}

        def cell(row, idx):
            if idx is None or idx >= len(row):
                return None
            v = row[idx]
            if v is None:
                return None
            if isinstance(v, str) and not v.strip():
                return None
            return v

        def to_num(v):
            if v is None:
                return None
            if isinstance(v, (int, float)):
                return float(v)
            s = str(v).replace("\xa0", "").replace(" ", "").replace(",", ".").replace("%", "")
            try:
                return float(s)
            except Exception:
                return None

        def to_bool_in_action(v):
            if v is None:
                return False
            if isinstance(v, bool):
                return v
            s = str(v).strip().lower()
            return s in ("да", "yes", "true", "1", "участвует")

        articles = []
        for row in rows[header_i + 1:]:
            if not row or not any(c is not None and str(c).strip() for c in row):
                continue
            vc = cell(row, col_vc)
            nm = cell(row, col_nm)
            if vc is None and nm is None:
                continue
            price = to_num(cell(row, col_price))  # розничная до скидки продавца
            plan = to_num(cell(row, col_plan))
            cur_disc = to_num(cell(row, col_cur_disc))
            # Цена «как на сайте» = розничная минус скидка продавца (6000−18% = 4920)
            price_sale = None
            if price is not None:
                if cur_disc is not None:
                    price_sale = round(price * (100.0 - cur_disc) / 100.0)
                else:
                    price_sale = price
            delta = None
            if price_sale is not None and plan is not None:
                delta = int(round(price_sale - plan))
            turn = to_num(cell(row, col_turn))
            stock_wb = to_num(cell(row, col_stock_wb)) or 0
            stock_seller = to_num(cell(row, col_stock_seller)) or 0
            in_action = to_bool_in_action(cell(row, col_in))
            # слабые/пустые: оборачиваемость 999 у WB = нет продаж
            is_weak = (turn is not None and turn >= 999) or (stock_wb + stock_seller <= 0 and (turn is None or turn >= 100))
            need = (not in_action) and (delta is not None and delta > 0) and not is_weak
            if need:
                priority = "need"
            elif is_weak:
                priority = "weak"
            elif in_action:
                priority = "in"
            else:
                priority = "other"

            articles.append({
                "vendor_code": str(vc).strip() if vc is not None else str(nm),
                "nm_id": int(nm) if isinstance(nm, (int, float)) else (int(to_num(nm)) if to_num(nm) else None),
                "name": str(cell(row, col_name) or "") or None,
                "brand": str(cell(row, col_brand) or "") or None,
                "subject": str(cell(row, col_subject) or "") or None,
                "in_action": in_action,
                "plan_price": plan,
                "price": price,
                "price_sale": price_sale,
                "delta": delta,
                "turnover": turn,
                "stock_wb": stock_wb,
                "stock_seller": stock_seller,
                "stock": stock_wb + stock_seller,
                "cur_discount": cur_disc,
                "load_discount": to_num(cell(row, col_load_disc)),
                "status": str(cell(row, col_status) or "") or None,
                "priority": priority,
            })

        # Сортировка: нужные сверху → в акции → прочие → слабые снизу; внутри по −₽
        order = {"need": 0, "in": 1, "other": 2, "weak": 3}
        articles.sort(key=lambda a: (order.get(a["priority"], 9), a["delta"] if a.get("delta") is not None else 10**12, str(a["vendor_code"])))

        promo_name = _parse_promo_excel_name(file.filename or "")
        need_n = sum(1 for a in articles if a["priority"] == "need")
        in_n = sum(1 for a in articles if a["in_action"])
        weak_n = sum(1 for a in articles if a["priority"] == "weak")

        return {
            "promo_name": promo_name,
            "filename": file.filename,
            "articles": articles,
            "stats": {
                "total": len(articles),
                "need": need_n,
                "in_action": in_n,
                "weak": weak_n,
            },
        }
    except Exception as e:
        logger.error(f"upload-promo-excel error: {e}")
        return {"error": str(e)}

@app.post("/api/upload-ratings")
async def upload_ratings(file: UploadFile = File(...)):
    """
    Принимает xlsx файл "Оценка товара" из WB Partners и сохраняет в Supabase.
    Лист "Товары" содержит правильные данные с учётом исключённых отзывов.
    """
    try:
        contents = await file.read()
        xl = pd.ExcelFile(io.BytesIO(contents))

        # Ищем лист с детальными данными по артикулам, перебирая ВСЕ листы
        # и проверяя где встречается заголовок "Артикул продавца".
        # WB называет этот лист по-разному в зависимости от настроек отчёта:
        # "Товары", "Детализация по артикулам", и т.д.
        sheet_name = None
        header_row = None
        df = None
        for s in xl.sheet_names:
            tmp = pd.read_excel(io.BytesIO(contents), sheet_name=s, header=None)
            for i, row in tmp.iterrows():
                vals = [str(v).strip() for v in row.values]
                if any('артикул продавца' in v.lower() for v in vals):
                    sheet_name = s
                    header_row = i
                    df = tmp
                    break
            if sheet_name:
                break

        logger.info(f"Detected sheet: {sheet_name}, header_row: {header_row} (sheets in file: {xl.sheet_names})")

        if header_row is None:
            return {"error": f"Не найден заголовок 'Артикул продавца' ни на одном листе. Листы в файле: {xl.sheet_names}. Проверь что загружаешь файл 'Оценка товара' из WB Partners → Аналитика."}

        df.columns = df.iloc[header_row].str.strip()
        df = df.iloc[header_row + 1:].reset_index(drop=True)
        df = df.dropna(subset=[df.columns[0]])

        # Маппинг колонок
        col_map = {}
        for c in df.columns:
            cl = str(c).lower().strip()
            if 'артикул продавца' in cl:
                col_map['article'] = c
            elif 'артикул wb' in cl:
                col_map['nm_id'] = c
            elif 'название' in cl:
                col_map['name'] = c
            elif 'рейтинг по отзывам' in cl and 'выше' not in cl:
                col_map['wb_rating'] = c
            elif 'все отзывы за период' in cl or 'всего' in cl:
                col_map['reviews_total'] = c
            elif 'оценки 5' in cl:
                col_map['r5'] = c
            elif 'оценки 4' in cl:
                col_map['r4'] = c
            elif 'оценки 3' in cl:
                col_map['r3'] = c
            elif 'оценки 2' in cl:
                col_map['r2'] = c
            elif 'оценки 1' in cl:
                col_map['r1'] = c
            elif 'исключен' in cl:
                col_map['excluded'] = c

        logger.info(f"Column mapping: {col_map}")

        if 'article' not in col_map:
            return {"error": f"Не найдена колонка 'Артикул продавца'. Найденные колонки: {list(df.columns)}"}

        now = datetime.now(timezone.utc).isoformat()
        rows = []
        for _, row in df.iterrows():
            article = str(row.get(col_map.get('article', ''), '') or '').strip()
            if not article or article == 'nan':
                continue

            def safe_int(key):
                try:
                    v = row.get(col_map.get(key, ''))
                    return int(float(v)) if v and str(v) != 'nan' else 0
                except:
                    return 0

            def safe_int_abs(key):
                return abs(safe_int(key))

            def safe_float(key):
                try:
                    v = row.get(col_map.get(key, ''))
                    return float(v) if v and str(v) != 'nan' else None
                except:
                    return None

            r5v = safe_int('r5') or 0
            r4v = safe_int('r4') or 0
            r3v = safe_int('r3') or 0
            r2v = safe_int('r2') or 0
            r1v = safe_int('r1') or 0
            reviews_total = safe_int('reviews_total') or 0
            star_sum = r5v+r4v+r3v+r2v+r1v

            # Если reviews_total не заполнен но звёзды есть — считаем из них
            if not reviews_total and star_sum > 0:
                reviews_total = star_sum

            # wb_rating берём из колонки "Рейтинг по отзывам"
            # Если там '-' или пусто — считаем из звёзд сами
            wb_rating = safe_float('wb_rating')
            if wb_rating is None and star_sum > 0:
                wb_rating = round((r5v*5+r4v*4+r3v*3+r2v*2+r1v) / star_sum, 2)

            rows.append({
                "article": article,
                "nm_id": safe_int('nm_id') or None,
                "name": str(row.get(col_map.get('name', ''), '') or '').strip() or None,
                "wb_rating": wb_rating,
                "reviews_total": reviews_total,
                "r5": r5v, "r4": r4v, "r3": r3v, "r2": r2v, "r1": r1v,
                "excluded": safe_int_abs('excluded'),
                "updated_at": now
            })

        if not rows:
            return {"error": "Не найдено строк с данными"}

        # Добавляем source='xlsx' каждой строке
        for r in rows:
            r["source"] = "xlsx"

        # Удаляем только НЕ-ручные строки (manual сохраняем)
        httpx.delete(
            f"{SUPABASE_URL}/rest/v1/ratings_official?source=neq.manual",
            headers={**sb_headers(), "Prefer": "return=minimal"}, timeout=15
        )
        # Также удаляем строки без source (старые данные)
        httpx.delete(
            f"{SUPABASE_URL}/rest/v1/ratings_official?source=is.null",
            headers={**sb_headers(), "Prefer": "return=minimal"}, timeout=15
        )

        # Получаем какие артикулы уже заняты ручными записями — их пропускаем
        manual_resp = httpx.get(
            f"{SUPABASE_URL}/rest/v1/ratings_official?source=eq.manual&select=article",
            headers=sb_headers(), timeout=10
        )
        manual_articles = {r["article"] for r in (manual_resp.json() if manual_resp.is_success else [])}
        rows = [r for r in rows if r["article"] not in manual_articles]

        saved = 0
        for i in range(0, len(rows), 100):
            batch = rows[i:i+100]
            resp = httpx.post(
                f"{SUPABASE_URL}/rest/v1/ratings_official",
                json=batch,
                headers=sb_headers(),
                timeout=30
            )
            if resp.is_success:
                saved += len(batch)
            else:
                logger.error(f"Supabase error: {resp.status_code} {resp.text[:300]}")

        # Обновляем время загрузки
        httpx.post(
            f"{SUPABASE_URL}/rest/v1/settings",
            json={"key": "last_ratings_upload", "value": datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M"), "updated_at": now},
            headers=sb_headers(), timeout=10
        )

        return {"status": "ok", "saved": saved, "total_rows": len(rows)}

    except Exception as e:
        logger.error(f"Upload error: {e}")
        return {"error": str(e)}

@app.post("/api/upload-competitor-report")
async def upload_competitor_report(file: UploadFile = File(...)):
    """Принимает xlsx «Сравнение карточек» и парсит только лист Показатели."""
    try:
        import re as _re
        contents = await file.read()

        # ── Период из Общая информация ──
        period_begin = period_end = None
        try:
            df_info = pd.read_excel(io.BytesIO(contents), sheet_name='Общая информация', header=None)
            for _, row in df_info.iterrows():
                if 'Выбранный период' in str(row.iloc[0]):
                    dates = _re.findall(r'\d{4}-\d{2}-\d{2}', str(row.iloc[1]))
                    if len(dates) >= 2:
                        period_begin, period_end = dates[0], dates[1]
                    break
        except Exception:
            pass

        # ── Читаем Показатели ──
        df = pd.read_excel(io.BytesIO(contents), sheet_name='Показатели', header=None)
        headers = [str(v) for v in df.iloc[1].values]

        # Колонки артикулов (не Разница, не предыдущий)
        art_cols = []
        for j, h in enumerate(headers):
            if 'Артикул WB' in h and 'предыдущий' not in h.lower() and 'Разница' not in h:
                m = _re.search(r'(\d{7,10})', h)
                if m:
                    art_cols.append((j, int(m.group(1))))

        if not art_cols:
            return {"error": "Не найдены артикулы в листе Показатели. Проверь формат файла."}

        # Артикулы из файла НЕ помечаем как «мой» — свой артикул добавляется вручную через поиск

        def cell(i, j):
            v = str(df.iloc[i, j]).strip()
            return None if v in ('nan','None','NaT','-','') else v

        def to_num(i, j):
            v = cell(i, j)
            if not v: return None
            try: return float(v.replace('\xa0','').replace(' ','').replace(',','.').replace('%',''))
            except: return None

        def find_row(label):
            for i in range(2, len(df)):
                if str(df.iloc[i, 0]).strip() == label:
                    return i
            return None

        METRICS = {
            'name': (['Название'], True),
            'brand': (['Бренд'], True),
            'card_rating': (['Рейтинг карточки'], False),
            'feedback_rating': (['Рейтинг по отзывам'], False),
            'reviews_count': (['Количество отзывов'], False),
            'price': (['Минимальная цена со скидкой (по размерам), ₽','Цена с учётом скидок, ₽'], False),
            'median_price': (['Медианная цена покупателя, ₽','Медианная цена покупателя'], False),
            'delivery_time': (['Среднее время доставки'], True),
            'avg_position': (['Средняя позиция','Средняя позиция в поиске'], False),
            'views': (['Показы'], False),
            'card_opens': (['Переход в карточку, шт','Переходы в карточку, шт'], False),
            'ctr': (['CTR'], False),
            'cart_adds': (['Добавления в корзину, шт'], False),
            'cart_conv': (['Конверсия в корзину, %'], False),
            'orders': (['Заказы, шт'], False),
            'order_conv': (['Конверсия в заказ, %'], False),
            'buyouts': (['Выкупы, шт'], False),
            'buyout_pct': (['Процент выкупа'], False),
            'cancels': (['Отмены, шт'], False),
        }
        INT_FIELDS = {'reviews_count','views','card_opens','cart_adds','orders','buyouts','cancels'}

        row_idx = {}
        for field, (labels, _) in METRICS.items():
            for label in labels:
                ri = find_row(label)
                if ri is not None:
                    row_idx[field] = ri
                    break

        # ── Создаём сессию ──
        session_row = {"period_begin": period_begin, "period_end": period_end}
        sess_resp = httpx.post(
            f"{SUPABASE_URL}/rest/v1/competitor_sessions",
            json=session_row,
            headers={**sb_headers(), "Prefer": "return=representation"},
            timeout=15
        )
        if not sess_resp.is_success:
            return {"error": f"Ошибка БД ({sess_resp.status_code}): {sess_resp.text[:200]}. Выполни competitor_tables.sql в Supabase."}
        try:
            session_id = sess_resp.json()[0]["id"]
        except Exception as e:
            return {"error": f"Ошибка сессии: {e}. Ответ: {sess_resp.text[:150]}"}

        # ── Сохраняем метрики ──
        now = datetime.now(timezone.utc).isoformat()
        rows = []
        for j, nm_id in art_cols:
            r = {"session_id": session_id, "nm_id": nm_id, "is_own": False, "updated_at": now}
            for field, (_, is_str) in METRICS.items():
                ri = row_idx.get(field)
                if ri is None:
                    r[field] = None
                elif is_str:
                    r[field] = cell(ri, j)
                else:
                    v = to_num(ri, j)
                    r[field] = int(v) if (v is not None and field in INT_FIELDS) else v
            rows.append(r)

        mr = httpx.post(f"{SUPABASE_URL}/rest/v1/competitor_metrics",
                        json=rows, headers=sb_headers(), timeout=20)
        if not mr.is_success:
            return {"error": f"Ошибка сохранения ({mr.status_code}): {mr.text[:200]}"}

        brands = sorted({(r.get("brand") or "").strip() for r in rows if (r.get("brand") or "").strip()})
        period = f"{period_begin or '?'} — {period_end or '?'}"
        logger.info(f"upload-competitor: session={session_id}, {len(rows)} articles, period={period}, brands={brands}")
        return {
            "status": "ok",
            "session_id": session_id,
            "period": period,
            "period_begin": period_begin,
            "period_end": period_end,
            "brands": brands,
            "articles": len(rows),
            "search_queries": 0,
        }

    except Exception as e:
        logger.error(f"upload-competitor error: {e}")
        import traceback; logger.error(traceback.format_exc())
        return {"error": str(e)}

@app.get("/api/search-own-articles")
def search_own_articles(q: str = ""):
    """Поиск своих артикулов по артикулу продавца (vendorCode).
    Сначала Content API WB (textSearch), иначе — ratings/feedbacks/stock с фильтром
    «не подставляй nmId вместо артикула продавца»."""
    q = (q or "").strip().rstrip(".…").strip()
    for ch in ("\\", "%", ",", "(", ")"):
        q = q.replace(ch, "")
    q = q.strip()

    def is_real_vendor(vc, nm_id=None) -> bool:
        vc = (vc or "").strip()
        if not vc:
            return False
        if nm_id is not None and vc == str(nm_id):
            return False
        return True

    # ── 1) Content API: настоящий vendorCode + поиск по префиксу/тексту ──
    if WB_TOKEN:
        try:
            filt = {"withPhoto": -1}
            if q:
                filt["textSearch"] = q
            resp = httpx.post(
                f"{WB_CONTENT_URL}/content/v2/get/cards/list",
                headers=wb_headers(),
                json={
                    "settings": {
                        "sort": {"ascending": True},
                        "filter": filt,
                        "cursor": {"limit": 80},
                    }
                },
                timeout=20,
            )
            if resp.is_success:
                cards = resp.json().get("cards") or []
                out, seen = [], set()
                ql = q.lower()
                for c in cards:
                    nm = c.get("nmID") or c.get("nmId")
                    vc = (c.get("vendorCode") or "").strip()
                    if not nm or not is_real_vendor(vc, nm) or nm in seen:
                        continue
                    if ql and not vc.lower().startswith(ql) and ql not in vc.lower():
                        continue
                    seen.add(nm)
                    out.append({"nm_id": nm, "vendor_code": vc})
                # Префиксные совпадения выше «содержит»
                if ql:
                    out.sort(key=lambda a: (
                        0 if a["vendor_code"].lower().startswith(ql) else 1,
                        a["vendor_code"].lower(),
                    ))
                else:
                    out.sort(key=lambda a: a["vendor_code"].lower())
                if out:
                    return out[:50]
            else:
                logger.warning(f"search-own content-api {resp.status_code}: {resp.text[:200]}")
        except Exception as e:
            logger.warning(f"search-own content-api error: {e}")

    # ── 2) Fallback из БД: ratings_official → feedbacks → stock_totals ──
    by_nm = {}

    def add_row(nm, vc, prefer=False):
        if not nm or not is_real_vendor(vc, nm):
            return
        vc = vc.strip()
        cur = by_nm.get(nm)
        if cur is None or (prefer and not cur.get("prefer")):
            by_nm[nm] = {"nm_id": nm, "vendor_code": vc, "prefer": prefer}

    try:
        params = {"select": "nm_id,article", "nm_id": "not.is.null", "limit": "80"}
        if q:
            params["article"] = f"ilike.{q}*"
            params["order"] = "article.asc"
        else:
            params["order"] = "article.asc"
            params["limit"] = "50"
        r = httpx.get(f"{SUPABASE_URL}/rest/v1/ratings_official", params=params,
                      headers=sb_headers(), timeout=10)
        if r.is_success:
            for row in r.json() or []:
                add_row(row.get("nm_id"), row.get("article"), prefer=True)
    except Exception as e:
        logger.warning(f"search-own ratings fallback: {e}")

    try:
        params = {"select": "nm_id,article", "nm_id": "not.is.null", "article": "not.is.null", "limit": "120"}
        if q:
            params["article"] = f"ilike.{q}*"
        r = httpx.get(f"{SUPABASE_URL}/rest/v1/feedbacks", params=params,
                      headers=sb_headers(), timeout=10)
        if r.is_success:
            for row in r.json() or []:
                add_row(row.get("nm_id"), row.get("article"), prefer=True)
    except Exception as e:
        logger.warning(f"search-own feedbacks fallback: {e}")

    try:
        params = {"select": "nm_id,vendor_code", "limit": "80"}
        if q:
            params["vendor_code"] = f"ilike.{q}*"
            params["order"] = "vendor_code.asc"
        else:
            params["order"] = "vendor_code.asc"
            params["limit"] = "50"
        r = httpx.get(f"{SUPABASE_URL}/rest/v1/stock_totals", params=params,
                      headers=sb_headers(), timeout=10)
        if r.is_success:
            for row in r.json() or []:
                add_row(row.get("nm_id"), row.get("vendor_code"), prefer=False)
    except Exception as e:
        logger.warning(f"search-own stock fallback: {e}")

    out = [{"nm_id": v["nm_id"], "vendor_code": v["vendor_code"]} for v in by_nm.values()]
    ql = q.lower()
    if ql:
        out = [a for a in out if a["vendor_code"].lower().startswith(ql) or ql in a["vendor_code"].lower()]
        out.sort(key=lambda a: (
            0 if a["vendor_code"].lower().startswith(ql) else 1,
            a["vendor_code"].lower(),
        ))
    else:
        out.sort(key=lambda a: a["vendor_code"].lower())
    return out[:50]

def fetch_own_stats_v3(nm_ids: list[int], date_from: str, date_to: str) -> dict:
    """Метрики своих артикулов за период из WB Sales Funnel v3.
    Возвращает словарь {str(nm_id): {...метрики под таблицу сравнения...}}.
    Одна карточка = один запрос ко всем nm_ids сразу (щадим лимиты WB: 3 req/min)."""
    if not WB_TOKEN or not nm_ids:
        return {}
    try:
        body = {
            "selectedPeriod": {"start": date_from, "end": date_to},
            "nmIds": nm_ids,
            "brandNames": [], "subjectIds": [], "tagIds": [],
            "orderBy": {"field": "orderSum", "mode": "desc"},
            "limit": max(len(nm_ids), 20), "offset": 0,
        }
        resp = httpx.post(
            f"{WB_ANALYTICS_URL}/api/analytics/v3/sales-funnel/products",
            headers=wb_headers(), json=body, timeout=40
        )
        if not resp.is_success:
            logger.error(f"own-stats v3 error {resp.status_code} {resp.text[:250]}")
            return {}
        products = resp.json().get("data", {}).get("products", []) or []
    except Exception as e:
        logger.error(f"own-stats v3 exception: {e}")
        return {}

    # Кол-во отзывов нет в воронке — добираем из ratings_official одним запросом
    reviews = {}
    try:
        ids_csv = ",".join(str(i) for i in nm_ids)
        rq = httpx.get(
            f"{SUPABASE_URL}/rest/v1/ratings_official?nm_id=in.({ids_csv})&select=nm_id,wb_rating,reviews_total",
            headers=sb_headers(), timeout=10
        )
        if rq.is_success:
            for r in rq.json():
                reviews[r["nm_id"]] = r
    except Exception:
        pass

    out = {}
    for p in products:
        prod = p.get("product", {}) or {}
        sel = (p.get("statistic", {}) or {}).get("selected", {}) or {}
        conv = sel.get("conversions", {}) or {}
        nm = prod.get("nmId")
        if nm is None:
            continue
        rev = reviews.get(nm, {})
        fb = prod.get("feedbackRating")
        out[str(nm)] = {
            "nm_id": nm,
            "vendor_code": prod.get("vendorCode") or str(nm),
            "brand": prod.get("brandName") or "",
            "name": prod.get("title") or "",
            "is_own": True,
            # Карточка
            "feedback_rating": fb if fb else rev.get("wb_rating"),
            "card_rating": prod.get("productRating"),
            "reviews_count": rev.get("reviews_total"),
            "price": sel.get("avgPrice"),
            # median_price / avg_position / ctr в воронке WB отсутствуют → остаются "—"
            # Воронка
            "card_opens": sel.get("openCount"),
            "cart_adds": sel.get("cartCount"),
            "orders": sel.get("orderCount"),
            "orders_sum": sel.get("orderSum"),
            "buyouts": sel.get("buyoutCount"),
            "cancels": sel.get("cancelCount"),
            "cart_conv": conv.get("addToCartPercent"),
            "order_conv": conv.get("cartToOrderPercent"),
            "buyout_pct": conv.get("buyoutPercent"),
        }
    return out

@app.get("/api/own-articles-period-stats")
def own_articles_period_stats(nm_ids: str, date_from: str, date_to: str):
    """Метрики своих артикулов за период (WB Sales Funnel v3). nm_ids — через запятую.
    Ответ: {str(nm_id): {...}} — под колонки таблицы сравнения."""
    ids = []
    for x in nm_ids.split(","):
        x = x.strip()
        if x.isdigit():
            ids.append(int(x))
    return fetch_own_stats_v3(ids, date_from, date_to)

@app.get("/api/own-article-period-stats")
def own_article_period_stats(nm_id: int, date_from: str, date_to: str):
    """Статистика одного своего артикула за период (WB Sales Funnel v3)."""
    stats = fetch_own_stats_v3([nm_id], date_from, date_to)
    return stats.get(str(nm_id))

@app.delete("/api/competitor-session/{session_id}")
def delete_competitor_session(session_id: int):
    """Удаляет сессию и все связанные метрики (каскадно через ON DELETE CASCADE)."""
    try:
        resp = httpx.delete(
            f"{SUPABASE_URL}/rest/v1/competitor_sessions?id=eq.{session_id}",
            headers={**sb_headers(), "Prefer": "return=minimal"}, timeout=15
        )
        return {"status": "ok"} if resp.is_success else {"error": resp.text[:200]}
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/competitor-sessions")
def get_competitor_sessions():
    """Список загруженных сессий сравнения (+ уникальные бренды из метрик)."""
    try:
        resp = httpx.get(
            f"{SUPABASE_URL}/rest/v1/competitor_sessions?select=*&order=uploaded_at.desc",
            headers=sb_headers(), timeout=15
        )
        if not resp.is_success:
            return []
        sessions = resp.json() or []
        if not sessions:
            return []

        # Бренды из competitor_metrics — чтобы подпись файла была «14–20 июля · Brand»
        brands_by_sid = {}
        try:
            ids = ",".join(str(s["id"]) for s in sessions if s.get("id") is not None)
            if ids:
                br = httpx.get(
                    f"{SUPABASE_URL}/rest/v1/competitor_metrics?session_id=in.({ids})&select=session_id,brand",
                    headers=sb_headers(), timeout=15
                )
                if br.is_success:
                    for row in br.json() or []:
                        sid = row.get("session_id")
                        brand = (row.get("brand") or "").strip()
                        if sid is None or not brand:
                            continue
                        brands_by_sid.setdefault(sid, [])
                        if brand not in brands_by_sid[sid]:
                            brands_by_sid[sid].append(brand)
        except Exception as e:
            logger.warning(f"competitor-sessions brands enrich: {e}")

        for s in sessions:
            s["brands"] = brands_by_sid.get(s.get("id"), [])
        return sessions
    except Exception:
        return []

@app.get("/api/competitor-data/{session_id}")
def get_competitor_data(session_id: int):
    """Метрики и поисковые запросы по сессии."""
    try:
        metrics = httpx.get(
            f"{SUPABASE_URL}/rest/v1/competitor_metrics?session_id=eq.{session_id}&select=*",
            headers=sb_headers(), timeout=15
        )
        queries = httpx.get(
            f"{SUPABASE_URL}/rest/v1/competitor_search_queries?session_id=eq.{session_id}&select=*&order=query_count.desc",
            headers=sb_headers(), timeout=15
        )
        return {
            "metrics": metrics.json() if metrics.is_success else [],
            "search_queries": queries.json() if queries.is_success else []
        }
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/my-article-stats")
def my_article_stats(begin: str, end: str, nm_ids: str = ""):
    """Тянет метрики своих артикулов с WB Analytics API за указанный период.
    nm_ids — через запятую, пусто = все артикулы продавца."""
    if not WB_TOKEN:
        return {"error": "WB_TOKEN не задан"}
    try:
        body = {
            "period": {"begin": begin, "end": end},
            "brandNames": [], "objectIDs": [], "tagIDs": [],
            "nmIDs": [int(x) for x in nm_ids.split(",") if x.strip()] if nm_ids else [],
            "timezone": "Europe/Moscow",
            "page": 1
        }
        resp = httpx.post(
            f"{WB_ANALYTICS_URL}/api/analytics/v2/nm-report/detail",
            headers=wb_headers(), json=body, timeout=30
        )
        logger.info(f"my-article-stats: {resp.status_code} snippet={resp.text[:300]}")
        if not resp.is_success:
            return {"error": f"WB API {resp.status_code}: {resp.text[:200]}"}
        data = resp.json()
        cards = data.get("data", {}).get("cards") or []

        # Строим маппинг nm_id → vendor_code
        st = httpx.get(f"{SUPABASE_URL}/rest/v1/stock_totals?select=nm_id,vendor_code", headers=sb_headers(), timeout=15)
        nm_to_vc = {r["nm_id"]: r["vendor_code"] for r in st.json()} if st.is_success else {}

        result = []
        for c in cards:
            nm_id = c.get("nmID")
            stats = c.get("statistics", {}).get("selectedPeriod", {})
            result.append({
                "nm_id": nm_id,
                "vendor_code": nm_to_vc.get(nm_id) or str(nm_id),
                "brand": c.get("brandName", ""),
                "name": c.get("objectName", ""),
                "views": stats.get("openCardCount", 0),
                "card_opens": stats.get("openCardCount", 0),
                "cart_adds": stats.get("addToCartCount", 0),
                "orders": stats.get("ordersCount", 0),
                "orders_sum": stats.get("ordersSumRub", 0),
                "buyouts": stats.get("buyoutsCount", 0),
                "buyout_pct": stats.get("buyoutPercent", 0),
                "cancels": stats.get("cancelCount", 0),
                "ctr": round(stats.get("addToCartCount", 0) / stats.get("openCardCount", 1) * 100, 1) if stats.get("openCardCount") else 0,
                "cart_conv": round(stats.get("ordersCount", 0) / stats.get("addToCartCount", 1) * 100, 1) if stats.get("addToCartCount") else 0,
                "order_conv": round(stats.get("buyoutsCount", 0) / stats.get("ordersCount", 1) * 100, 1) if stats.get("ordersCount") else 0,
                "is_own": True,
            })
        return result
    except Exception as e:
        logger.error(f"my-article-stats error: {e}")
        return {"error": str(e)}

def sync_article_daily_stats(days: int = 30):
    """Тянет дневную статистику по своим артикулам с WB Analytics API.
    Без Jam — максимум 7 дней. С Jam — до 365 дней.
    Поля: openCardCount, addToCartCount, ordersCount, buyoutsCount и т.д."""
    if not WB_TOKEN:
        return
    logger.info(f"sync_article_daily_stats: fetching last {days} days...")

    end_dt = datetime.now(timezone.utc).date()
    begin_dt = end_dt - timedelta(days=days)

    # Используем nm-report/detail/history — статистика по дням для nmId
    try:
        resp = httpx.post(
            f"{WB_ANALYTICS_URL}/api/v2/nm-report/detail/history",
            headers=wb_headers(),
            json={
                "nmIDs": [],
                "period": {
                    "begin": begin_dt.isoformat(),
                    "end": end_dt.isoformat()
                },
                "aggregationLevel": "day"
            },
            timeout=60
        )
        if not resp.is_success:
            logger.error(f"sync_daily: WB error {resp.status_code} {resp.text[:300]}")
            return
        data = resp.json()
        logger.info(f"sync_daily: response snippet: {str(data)[:400]}")
        cards = data.get("data", []) or data if isinstance(data, list) else []
        if not cards:
            logger.info("sync_daily: no data returned")
            return
    except Exception as e:
        logger.error(f"sync_daily: exception {e}")
        return

    # Строим nm_id→vendor_code из stock_totals
    try:
        st = httpx.get(f"{SUPABASE_URL}/rest/v1/stock_totals?select=nm_id,vendor_code", headers=sb_headers(), timeout=15)
        nm_to_vc = {r["nm_id"]: r["vendor_code"] for r in st.json()} if st.is_success else {}
    except Exception:
        nm_to_vc = {}

    now = datetime.now(timezone.utc).isoformat()
    rows = []
    for card in cards:
        nm_id = card.get("nmID") or card.get("nmId")
        history = card.get("history") or card.get("days") or []
        for day in history:
            dt = day.get("dt") or day.get("date")
            if not dt or not nm_id:
                continue
            dt_str = str(dt)[:10]
            oc = day.get("openCardCount", 0) or 0
            atc = day.get("addToCartCount", 0) or 0
            ord_ = day.get("ordersCount", 0) or 0
            ord_sum = day.get("ordersSumRub", 0) or 0
            buy = day.get("buyoutsCount", 0) or 0
            buy_sum = day.get("buyoutsSumRub", 0) or 0
            can = day.get("cancelCount", 0) or 0

            rows.append({
                "nm_id": nm_id,
                "vendor_code": nm_to_vc.get(nm_id) or str(nm_id),
                "dt": dt_str,
                "open_card": int(oc),
                "add_to_cart": int(atc),
                "orders": int(ord_),
                "orders_sum": float(ord_sum),
                "buyouts": int(buy),
                "buyouts_sum": float(buy_sum),
                "cancels": int(can),
                "ctr": round(atc / oc * 100, 2) if oc else 0,
                "cart_conv": round(ord_ / atc * 100, 2) if atc else 0,
                "order_conv": round(buy / ord_ * 100, 2) if ord_ else 0,
                "buyout_pct": round(buy / (buy + can) * 100, 2) if (buy + can) else 0,
                "updated_at": now
            })

    logger.info(f"sync_daily: {len(rows)} day-rows to upsert")
    if not rows:
        return

    # Upsert по (nm_id, dt) — обновляем если уже есть
    headers_up = {**sb_headers(), "Prefer": "resolution=merge-duplicates"}
    for i in range(0, len(rows), 500):
        r = httpx.post(
            f"{SUPABASE_URL}/rest/v1/article_daily_stats?on_conflict=nm_id,dt",
            json=rows[i:i+500], headers=headers_up, timeout=30
        )
        if not r.is_success:
            logger.error(f"sync_daily insert error: {r.status_code} {r.text[:200]}")

@app.get("/api/article-daily-stats")
def article_daily_stats(days: int = 30):
    """Дневная статистика по своим артикулам за последние N дней."""
    try:
        dt_from = (datetime.now(timezone.utc).date() - timedelta(days=days)).isoformat()
        resp = httpx.get(
            f"{SUPABASE_URL}/rest/v1/article_daily_stats?dt=gte.{dt_from}&select=*&order=dt.asc",
            headers=sb_headers(), timeout=20
        )
        return resp.json() if resp.is_success else []
    except Exception as e:
        return {"error": str(e)}

@app.post("/api/sync-daily-stats")
def trigger_daily_sync(days: int = 30):
    import threading
    threading.Thread(target=sync_article_daily_stats, args=(days,), daemon=True).start()
    return {"status": "started", "days": days}

# ---------- Рост продаж: темп к прошлому периоду (день/неделя/2 недели/месяц) ----------
# Заказы — точно по времени из Statistics API.
# Воронка: для «день» — почасовые снимки; для недели/месяца — selectedPeriod vs pastPeriod.
SALES_PACE_CACHE = {
    "by_period": {},  # period -> payload
    "syncing": False,
    "syncing_period": None,
    "error": None,
}
SALES_PACE_SNAPS_KEY = "sales_pace_funnel_snaps"
SALES_PACE_PERIODS = ("day", "week", "weeks2", "month")

def _msk_now():
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Europe/Moscow")).replace(tzinfo=None)
    except Exception:
        return datetime.utcnow() + timedelta(hours=3)

def _pace_windows(period: str, now: datetime) -> dict:
    """Окна текущего и прошлого периода (конец текущего = now, прошлый — той же длины)."""
    today0 = now.replace(hour=0, minute=0, second=0, microsecond=0)
    if period == "day":
        cur_start = today0
        prev_start = today0 - timedelta(days=1)
        prev_end = prev_start + (now - cur_start)
        return {
            "cur_start": cur_start, "cur_end": now,
            "prev_start": prev_start, "prev_end": prev_end,
            "label_cur": f"сегодня до {now.strftime('%H:%M')}",
            "label_prev": f"вчера до {now.strftime('%H:%M')}",
            "use_snaps": True,
        }
    if period == "week":
        # понедельник текущей недели
        cur_start = today0 - timedelta(days=today0.weekday())
        prev_start = cur_start - timedelta(days=7)
        prev_end = prev_start + (now - cur_start)
        return {
            "cur_start": cur_start, "cur_end": now,
            "prev_start": prev_start, "prev_end": prev_end,
            "label_cur": f"эта неделя ({cur_start.strftime('%d.%m')}–{now.strftime('%d.%m %H:%M')})",
            "label_prev": f"прошлая неделя ({prev_start.strftime('%d.%m')}–{prev_end.strftime('%d.%m %H:%M')})",
            "use_snaps": False,
        }
    if period == "weeks2":
        cur_start = now - timedelta(days=14)
        prev_start = now - timedelta(days=28)
        prev_end = now - timedelta(days=14)
        return {
            "cur_start": cur_start, "cur_end": now,
            "prev_start": prev_start, "prev_end": prev_end,
            "label_cur": f"последние 14 дн. ({cur_start.strftime('%d.%m')}–{now.strftime('%d.%m')})",
            "label_prev": f"пред. 14 дн. ({prev_start.strftime('%d.%m')}–{prev_end.strftime('%d.%m')})",
            "use_snaps": False,
        }
    # month — с 1-го числа до сейчас vs прошлый месяц до того же дня/времени
    cur_start = today0.replace(day=1)
    if cur_start.month == 1:
        prev_month_start = cur_start.replace(year=cur_start.year - 1, month=12)
    else:
        prev_month_start = cur_start.replace(month=cur_start.month - 1)
    try:
        prev_end = prev_month_start.replace(day=now.day, hour=now.hour, minute=now.minute, second=now.second)
    except ValueError:
        # 31-е → последний день прошлого месяца
        if prev_month_start.month == 12:
            nxt = prev_month_start.replace(year=prev_month_start.year + 1, month=1, day=1)
        else:
            nxt = prev_month_start.replace(month=prev_month_start.month + 1, day=1)
        prev_end = nxt - timedelta(seconds=1)
        prev_end = prev_end.replace(hour=now.hour, minute=now.minute, second=now.second, microsecond=0)
    return {
        "cur_start": cur_start, "cur_end": now,
        "prev_start": prev_month_start, "prev_end": prev_end,
        "label_cur": f"этот месяц ({cur_start.strftime('%d.%m')}–{now.strftime('%d.%m %H:%M')})",
        "label_prev": f"прошлый месяц ({prev_month_start.strftime('%d.%m')}–{prev_end.strftime('%d.%m %H:%M')})",
        "use_snaps": False,
    }

def _funnel_products_range(start_str: str, end_str: str, nm_ids: list = None) -> dict:
    """Воронка за период → {nm_id: {opens, cart, orders, vendor_code, name}}."""
    if not WB_TOKEN:
        return {}
    out = {}
    offset = 0
    limit = 1000
    for _ in range(20):
        body = {
            "selectedPeriod": {"start": start_str, "end": end_str},
            "nmIds": nm_ids or [],
            "brandNames": [], "subjectIds": [], "tagIds": [],
            "orderBy": {"field": "orderCount", "mode": "desc"},
            "limit": limit, "offset": offset,
        }
        try:
            resp = httpx.post(
                f"{WB_ANALYTICS_URL}/api/analytics/v3/sales-funnel/products",
                headers=wb_headers(), json=body, timeout=40
            )
            if not resp.is_success:
                logger.error(f"sales-pace funnel error {resp.status_code} {resp.text[:200]}")
                break
            products = resp.json().get("data", {}).get("products", []) or []
        except Exception as e:
            logger.error(f"sales-pace funnel exception: {e}")
            break
        if not products:
            break
        for p in products:
            prod = p.get("product", {}) or {}
            sel = (p.get("statistic", {}) or {}).get("selected", {}) or {}
            nm = prod.get("nmId")
            if nm is None:
                continue
            out[int(nm)] = {
                "nm_id": int(nm),
                "vendor_code": prod.get("vendorCode") or str(nm),
                "name": prod.get("title") or "",
                "opens": int(sel.get("openCount") or 0),
                "cart": int(sel.get("cartCount") or 0),
                "orders": int(sel.get("orderCount") or 0),
            }
        if len(products) < limit:
            break
        offset += limit
        time.sleep(0.7)
    return out

def _funnel_products_day(day_str: str, nm_ids: list = None) -> dict:
    return _funnel_products_range(day_str, day_str, nm_ids)

def sync_sales_pace(period: str = "day"):
    """Считает темп продаж за выбранный период."""
    period = period if period in SALES_PACE_PERIODS else "day"
    if not WB_TOKEN:
        SALES_PACE_CACHE["error"] = "WB_TOKEN не задан"
        return
    if SALES_PACE_CACHE.get("syncing"):
        return
    SALES_PACE_CACHE["syncing"] = True
    SALES_PACE_CACHE["syncing_period"] = period
    SALES_PACE_CACHE["error"] = None
    try:
        now = _msk_now()
        win = _pace_windows(period, now)
        cur_start, cur_end = win["cur_start"], win["cur_end"]
        prev_start, prev_end = win["prev_start"], win["prev_end"]
        cur_s = cur_start.strftime("%Y-%m-%d")
        cur_e = cur_end.strftime("%Y-%m-%d")
        prev_s = prev_start.strftime("%Y-%m-%d")
        prev_e = prev_end.strftime("%Y-%m-%d")

        # ── Заказы ──
        date_from = prev_start.strftime("%Y-%m-%dT00:00:00")
        orders = fetch_supplier_feed("/api/v1/supplier/orders", date_from, max_pages=5)
        cur_ord, prev_ord, vc_from_orders = {}, {}, {}
        for o in orders:
            nm = o.get("nmId")
            if not nm:
                continue
            d = parse_wb_dt(o.get("date", ""))
            if d is None:
                continue
            if o.get("supplierArticle"):
                vc_from_orders[nm] = o["supplierArticle"]
            if cur_start <= d <= cur_end:
                cur_ord[nm] = cur_ord.get(nm, 0) + 1
            elif prev_start <= d <= prev_end:
                prev_ord[nm] = prev_ord.get(nm, 0) + 1

        funnel_cur, funnel_prev = {}, {}
        compare_as_of = None
        funnel_ready = True

        if win.get("use_snaps"):
            # день: снимок воронки
            funnel_cur = _funnel_products_day(cur_s)
            hour_key = now.strftime("%Y-%m-%dT%H")
            snaps = get_setting_json(SALES_PACE_SNAPS_KEY, []) or []
            if not isinstance(snaps, list):
                snaps = []
            snap_payload = {
                "hour_key": hour_key,
                "as_of": now.strftime("%Y-%m-%d %H:%M"),
                "day": cur_s,
                "products": {
                    str(nm): {"opens": v["opens"], "cart": v["cart"], "orders": v["orders"]}
                    for nm, v in funnel_cur.items()
                },
            }
            snaps = [s for s in snaps if s.get("hour_key") != hour_key]
            snaps.append(snap_payload)
            cutoff_day = (cur_start - timedelta(days=3)).strftime("%Y-%m-%d")
            snaps = [s for s in snaps if (s.get("day") or "") >= cutoff_day]
            snaps.sort(key=lambda s: s.get("hour_key") or "")
            save_setting_value(SALES_PACE_SNAPS_KEY, snaps)

            yest_str = prev_s
            target_yest_hour = prev_end.strftime("%Y-%m-%dT%H")
            yest_snap = None
            for s in snaps:
                if s.get("day") == yest_str and (s.get("hour_key") or "") <= target_yest_hour:
                    yest_snap = s
            if yest_snap is None:
                yest_candidates = [s for s in snaps if s.get("day") == yest_str]
                if yest_candidates:
                    yest_snap = min(
                        yest_candidates,
                        key=lambda s: abs(
                            (datetime.strptime(s["hour_key"], "%Y-%m-%dT%H") - prev_end.replace(minute=0, second=0, microsecond=0)).total_seconds()
                        ) if s.get("hour_key") else 10**9
                    )
            funnel_prev_raw = (yest_snap or {}).get("products") or {}
            funnel_prev = {}
            for k, v in funnel_prev_raw.items():
                try:
                    funnel_prev[int(k)] = v
                except Exception:
                    pass
            compare_as_of = (yest_snap or {}).get("as_of")
            funnel_ready = bool(yest_snap)
        else:
            # неделя / 2 недели / месяц — два запроса воронки по диапазонам дат
            funnel_cur = _funnel_products_range(cur_s, cur_e)
            time.sleep(0.7)
            funnel_prev = _funnel_products_range(prev_s, prev_e)
            compare_as_of = f"{prev_s}–{prev_e}"
            funnel_ready = True

        try:
            st = httpx.get(
                f"{SUPABASE_URL}/rest/v1/stock_totals?select=nm_id,vendor_code",
                headers=sb_headers(), timeout=15
            )
            nm_to_vendor = {r["nm_id"]: r["vendor_code"] for r in st.json()} if st.is_success else {}
        except Exception:
            nm_to_vendor = {}

        # только артикулы с заказами в текущем или прошлом окне
        all_nms = set(cur_ord) | set(prev_ord)
        articles = []
        for nm in all_nms:
            ft = funnel_cur.get(nm) or {}
            fy = funnel_prev.get(nm) or {}
            o_t = cur_ord.get(nm, 0)
            o_y = prev_ord.get(nm, 0)
            if o_t <= 0 and o_y <= 0:
                continue
            opens_t = int(ft.get("opens") or 0)
            opens_y = int(fy.get("opens") or 0)
            cart_t = int(ft.get("cart") or 0)
            cart_y = int(fy.get("cart") or 0)
            articles.append({
                "nm_id": nm,
                "vendor_code": ft.get("vendor_code") or fy.get("vendor_code") or nm_to_vendor.get(nm) or vc_from_orders.get(nm) or str(nm),
                "name": ft.get("name") or fy.get("name") or "",
                "orders_today": o_t,
                "orders_yesterday": o_y,
                "orders_delta": o_t - o_y,
                "opens_today": opens_t,
                "opens_yesterday": opens_y,
                "opens_delta": opens_t - opens_y if funnel_ready else None,
                "clicks_today": opens_t,
                "clicks_yesterday": opens_y,
                "clicks_delta": opens_t - opens_y if funnel_ready else None,
                "cart_today": cart_t,
                "cart_yesterday": cart_y,
                "cart_delta": cart_t - cart_y if funnel_ready else None,
                "funnel_compare_ready": funnel_ready,
            })

        articles.sort(key=lambda a: (a["orders_delta"], a["orders_today"], str(a["vendor_code"])))

        payload = {
            "period": period,
            "articles": articles,
            "as_of": now.strftime("%d.%m.%Y %H:%M"),
            "compare_as_of": compare_as_of,
            "label_cur": win["label_cur"],
            "label_prev": win["label_prev"],
            "today": cur_s,
            "yesterday": prev_s,
            "now_time": now.strftime("%H:%M"),
            "updated_at": datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M"),
            "funnel_ready": funnel_ready,
            "error": None,
        }
        SALES_PACE_CACHE.setdefault("by_period", {})[period] = payload
        SALES_PACE_CACHE["syncing"] = False
        SALES_PACE_CACHE["syncing_period"] = None
        logger.info(f"sales-pace[{period}]: {len(articles)} arts, {win['label_cur']}")
    except Exception as e:
        logger.error(f"sync_sales_pace({period}) error: {e}")
        SALES_PACE_CACHE["error"] = str(e)
        SALES_PACE_CACHE["syncing"] = False
        SALES_PACE_CACHE["syncing_period"] = None
    finally:
        SALES_PACE_CACHE["syncing"] = False
        SALES_PACE_CACHE["syncing_period"] = None

@app.get("/api/sales-pace")
def get_sales_pace(period: str = "day", refresh: bool = False):
    period = period if period in SALES_PACE_PERIODS else "day"
    by = SALES_PACE_CACHE.get("by_period") or {}
    cached = by.get(period)
    if refresh or not cached:
        if not SALES_PACE_CACHE.get("syncing"):
            import threading
            threading.Thread(target=sync_sales_pace, args=(period,), daemon=True).start()
    cached = (SALES_PACE_CACHE.get("by_period") or {}).get(period) or {}
    return {
        "period": period,
        "articles": cached.get("articles") or [],
        "as_of": cached.get("as_of"),
        "compare_as_of": cached.get("compare_as_of"),
        "label_cur": cached.get("label_cur"),
        "label_prev": cached.get("label_prev"),
        "today": cached.get("today"),
        "yesterday": cached.get("yesterday"),
        "now_time": cached.get("now_time"),
        "updated_at": cached.get("updated_at"),
        "funnel_ready": cached.get("funnel_ready"),
        "syncing": SALES_PACE_CACHE.get("syncing", False) and SALES_PACE_CACHE.get("syncing_period") == period,
        "error": SALES_PACE_CACHE.get("error") or cached.get("error"),
    }

@app.post("/api/sync-sales-pace")
async def trigger_sales_pace_sync(period: str = "day"):
    import threading
    period = period if period in SALES_PACE_PERIODS else "day"
    if SALES_PACE_CACHE.get("syncing"):
        return {"status": "already_running", "period": SALES_PACE_CACHE.get("syncing_period")}
    threading.Thread(target=sync_sales_pace, args=(period,), daemon=True).start()
    return {"status": "started", "period": period}

scheduler = BackgroundScheduler()
scheduler.add_job(sync_all, "interval", minutes=30, id="sync")
scheduler.add_job(sync_stock, "interval", hours=3, id="sync_stock")
scheduler.add_job(sync_supply, "interval", hours=4, id="sync_supply")
scheduler.add_job(sync_ads, "interval", hours=4, id="sync_ads")
scheduler.add_job(lambda: sync_article_daily_stats(30), "interval", hours=6, id="sync_daily")
scheduler.add_job(sync_promotions, "interval", hours=6, id="sync_promotions")
scheduler.add_job(lambda: sync_sales_pace("day"), "interval", hours=1, id="sync_sales_pace")
scheduler.start()

FRONTEND_CANDIDATES = [
    Path(__file__).resolve().parent.parent / "frontend",  # repo/frontend
    Path(__file__).resolve().parent / "frontend",         # backend/frontend
    Path.cwd() / "frontend",
    Path.cwd().parent / "frontend",
]

def _resolve_frontend_dir():
    for p in FRONTEND_CANDIDATES:
        if (p / "index.html").exists():
            return p
    return FRONTEND_CANDIDATES[0]

FRONTEND_DIR = _resolve_frontend_dir()
logger.info(f"FRONTEND_DIR={FRONTEND_DIR} exists={FRONTEND_DIR.exists()} index={(FRONTEND_DIR / 'index.html').exists()}")

@app.get("/")
def root():
    index = FRONTEND_DIR / "index.html"
    if index.exists():
        return FileResponse(index, media_type="text/html; charset=utf-8")
    tried = [str(p) for p in FRONTEND_CANDIDATES]
    return {"status": "ok", "hint": "frontend/index.html not found", "tried": tried}

@app.get("/index.html")
def root_index():
    index = FRONTEND_DIR / "index.html"
    if index.exists():
        return FileResponse(index, media_type="text/html; charset=utf-8")
    return HTMLResponse("<h1>frontend missing</h1>", status_code=404)

if (FRONTEND_DIR / "index.html").exists():
    app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")

@app.get("/api/status")
def status():
    try:
        resp = httpx.get(
            f"{SUPABASE_URL}/rest/v1/settings?key=eq.last_sync&select=value",
            headers=sb_headers(), timeout=5
        )
        last_sync = resp.json()[0]["value"] if resp.is_success and resp.json() else None
        count_resp = httpx.get(
            f"{SUPABASE_URL}/rest/v1/feedbacks?select=id",
            headers={**sb_headers(), "Prefer": "count=exact"}, timeout=5
        )
        total = int(count_resp.headers.get("content-range", "0/0").split("/")[-1])
    except:
        last_sync = None
        total = 0
    return {"status": "ok", "last_sync": last_sync, "total_feedbacks": total}

@app.post("/api/sync")
def trigger_sync():
    import threading
    threading.Thread(target=sync_all, daemon=True).start()
    return {"status": "started"}

@app.post("/api/save-manual-rating")
async def save_manual_rating(request: dict):
    """Сохраняет ручной рейтинг (разбивку по звёздам) для артикула без данных."""
    article = request.get("article")
    nm_id = request.get("nm_id")
    r5 = int(request.get("r5") or 0)
    r4 = int(request.get("r4") or 0)
    r3 = int(request.get("r3") or 0)
    r2 = int(request.get("r2") or 0)
    r1 = int(request.get("r1") or 0)
    if not article:
        return {"error": "article required"}
    total = r5 + r4 + r3 + r2 + r1
    wb_rating = round((r5*5 + r4*4 + r3*3 + r2*2 + r1*1) / total, 2) if total else 0
    row = {
        "article": article, "nm_id": nm_id,
        "wb_rating": wb_rating, "reviews_total": total,
        "r5": r5, "r4": r4, "r3": r3, "r2": r2, "r1": r1,
        "excluded": 0, "source": "manual",
        "updated_at": datetime.now(timezone.utc).isoformat()
    }
    try:
        # Удаляем старую запись если есть, вставляем новую
        httpx.delete(
            f"{SUPABASE_URL}/rest/v1/ratings_official?article=eq.{article}",
            headers={**sb_headers(), "Prefer": "return=minimal"}, timeout=10
        )
        resp = httpx.post(
            f"{SUPABASE_URL}/rest/v1/ratings_official",
            json=[row], headers=sb_headers(), timeout=15
        )
        if not resp.is_success:
            return {"error": f"DB error: {resp.status_code} {resp.text[:200]}"}
        return {"status": "ok", "wb_rating": wb_rating, "total": total}
    except Exception as e:
        return {"error": str(e)}

@app.post("/api/sync-stock")
def trigger_stock_sync():
    import threading
    threading.Thread(target=sync_stock, daemon=True).start()
    return {"status": "started"}

@app.post("/api/sync-supply")
def trigger_supply_sync():
    import threading
    threading.Thread(target=sync_supply, daemon=True).start()
    return {"status": "started"}

def sync_stock_then_supply():
    sync_stock()
    sync_supply()

@app.post("/api/sync-supply-full")
def trigger_supply_full_sync():
    """Обновляет остатки (для текущих остатков по складам), затем заказы/продажи (для рекомендаций) — одной кнопкой."""
    import threading
    threading.Thread(target=sync_stock_then_supply, daemon=True).start()
    return {"status": "started"}

@app.post("/api/sync-ads")
def trigger_ads_sync():
    import threading
    if ADS_CACHE.get("syncing"):
        return {"status": "already_running"}
    threading.Thread(target=sync_ads, daemon=True).start()
    return {"status": "started"}

@app.post("/api/save-setting")
async def save_setting(request: dict):
    """Сохраняет произвольную настройку (например target_coverage_days) в таблицу settings."""
    key = request.get("key")
    value = request.get("value")
    if not key:
        return {"error": "key required"}
    try:
        resp = httpx.post(
            f"{SUPABASE_URL}/rest/v1/settings?on_conflict=key",
            json={"key": key, "value": str(value), "updated_at": datetime.now(timezone.utc).isoformat()},
            headers=sb_headers(), timeout=10
        )
        if not resp.is_success:
            return {"error": f"Supabase error: {resp.status_code} {resp.text[:200]}"}
        return {"status": "ok"}
    except Exception as e:
        return {"error": str(e)}

# ---------- Proxy endpoints: фронтенд обращается только к Railway, ----------
# ---------- никогда напрямую к Supabase (для пользователей у которых ----------
# ---------- Supabase плохо доступен напрямую). Railway сам ходит в Supabase. ----------

@app.get("/api/dashboard-data")
def dashboard_data():
    """Отдаёт все данные нужные дашборду одним запросом: группы + рейтинги + отзывы(агрегат) + негатив за периоды + last sync"""
    result = {"groups": [], "ratings": [], "feedback_stats": [], "negative_counts": {}, "settings": {}, "stock_totals": [], "stock_warehouses": [], "supply_report": [], "ad_stats": []}

    try:
        gr = httpx.get(
            f"{SUPABASE_URL}/rest/v1/groups_config?select=name,articles,sort_order&order=sort_order",
            headers=sb_headers(), timeout=15
        )
        if gr.is_success:
            result["groups"] = gr.json()
    except Exception as e:
        logger.error(f"dashboard-data groups error: {e}")

    try:
        rr = httpx.get(
            f"{SUPABASE_URL}/rest/v1/ratings_official?select=*",
            headers=sb_headers(), timeout=15
        )
        if rr.is_success:
            result["ratings"] = rr.json()
    except Exception as e:
        logger.error(f"dashboard-data ratings error: {e}")

    try:
        fr = httpx.post(
            f"{SUPABASE_URL}/rest/v1/rpc/get_article_stats",
            json={}, headers=sb_headers(), timeout=20
        )
        if fr.is_success:
            result["feedback_stats"] = fr.json()
    except Exception as e:
        logger.error(f"dashboard-data feedback_stats error: {e}")

    # Негатив за 5/7/30 дней, для звёзд 1-2-3 (фронт сам выберет нужный порог звёзд и период)
    for days in [1, 2, 3, 4, 5, 7, 14, 30]:
        try:
            nr = httpx.post(
                f"{SUPABASE_URL}/rest/v1/rpc/get_negative_counts",
                json={"days_back": days, "max_stars": 3},
                headers=sb_headers(), timeout=20
            )
            if nr.is_success:
                result["negative_counts"][str(days)] = nr.json()
        except Exception as e:
            logger.error(f"dashboard-data negative_counts({days}) error: {e}")

    try:
        sr = httpx.get(
            f"{SUPABASE_URL}/rest/v1/settings?select=key,value",
            headers=sb_headers(), timeout=10
        )
        if sr.is_success:
            for row in sr.json():
                result["settings"][row["key"]] = row["value"]
    except Exception as e:
        logger.error(f"dashboard-data settings error: {e}")

    try:
        st = httpx.get(
            f"{SUPABASE_URL}/rest/v1/stock_totals?select=*",
            headers=sb_headers(), timeout=15
        )
        if st.is_success:
            result["stock_totals"] = st.json()
    except Exception as e:
        logger.error(f"dashboard-data stock_totals error: {e}")

    try:
        sw = httpx.get(
            f"{SUPABASE_URL}/rest/v1/stock_warehouses?select=*",
            headers=sb_headers(), timeout=15
        )
        if sw.is_success:
            result["stock_warehouses"] = sw.json()
    except Exception as e:
        logger.error(f"dashboard-data stock_warehouses error: {e}")

    try:
        spr = httpx.get(
            f"{SUPABASE_URL}/rest/v1/supply_report?select=*",
            headers=sb_headers(), timeout=20
        )
        if spr.is_success:
            result["supply_report"] = spr.json()
    except Exception as e:
        logger.error(f"dashboard-data supply_report error: {e}")

    try:
        if ADS_CACHE.get("campaigns"):
            result["ad_stats"] = ADS_CACHE["campaigns"]
        else:
            ads = httpx.get(
                f"{SUPABASE_URL}/rest/v1/ad_stats?select=*",
                headers=sb_headers(), timeout=20
            )
            if ads.is_success:
                result["ad_stats"] = ads.json()
    except Exception as e:
        logger.error(f"dashboard-data ad_stats error: {e}")

    return result

@app.get("/api/article-feedbacks")
def article_feedbacks(article: str, days: int = 30, max_stars: int = 3, limit: int = 50):
    """
    Возвращает тексты отзывов по конкретному артикулу за последние N дней,
    с оценкой <= max_stars, отсортированные по дате (новые сверху).
    Используется при раскрытии артикула в таблице товаров.
    """
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        resp = httpx.get(
            f"{SUPABASE_URL}/rest/v1/feedbacks",
            params={
                "article": f"eq.{article}",
                "stars": f"lte.{max_stars}",
                "created_date": f"gte.{cutoff}",
                "select": "id,stars,created_date,text,is_answered",
                "order": "created_date.desc",
                "limit": str(limit),
            },
            headers=sb_headers(), timeout=15
        )
        if not resp.is_success:
            return {"error": f"Supabase error: {resp.status_code} {resp.text[:200]}"}
        return {"feedbacks": resp.json()}
    except Exception as e:
        logger.error(f"article-feedbacks error: {e}")
        return {"error": str(e)}

@app.post("/api/save-groups")
async def save_groups(request: dict):
    """
    Сохраняет конфигурацию склеек. Ожидает {"groups": {"Название": ["арт1","арт2"], ...}}
    """
    groups = request.get("groups", {})
    try:
        del_resp = httpx.delete(
            f"{SUPABASE_URL}/rest/v1/groups_config?id=gte.1",
            headers=sb_headers(), timeout=15
        )
        rows = [{"name": name, "articles": articles, "sort_order": i + 1}
                for i, (name, articles) in enumerate(groups.items())]
        if rows:
            ins_resp = httpx.post(
                f"{SUPABASE_URL}/rest/v1/groups_config",
                json=rows,
                headers={**sb_headers(), "Prefer": "return=minimal"},
                timeout=15
            )
            if not ins_resp.is_success:
                return {"error": f"Insert failed: {ins_resp.status_code} {ins_resp.text[:200]}"}
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"save-groups error: {e}")
        return {"error": str(e)}

# ---------- Финансы: себестоимость остатков ----------
COST_PRICES_KEY = "cost_prices"
COST_META_KEY = "cost_prices_meta"

def _parse_cost_number(v):
    if v is None:
        return None
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return float(v) if float(v) >= 0 else None
    s = str(v).strip().replace("\xa0", " ").replace(" ", "").replace(",", ".")
    s = s.replace("₽", "").replace("руб.", "").replace("руб", "")
    if not s or s.lower() in ("nan", "none", "-", "—"):
        return None
    try:
        n = float(s)
        return n if n >= 0 else None
    except Exception:
        return None

def _norm_vendor_key(v):
    return str(v or "").strip()

def _parse_header_date(v):
    """Парсит дату из заголовка колонки (datetime / '2026-03-30' / '30.03.2026')."""
    if v is None or v == "":
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    s = str(v).strip()
    if not s or s.lower() in ("по умолчанию", "default", "sku", "артикул", "наименование", "размер"):
        return None
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y", "%Y.%m.%d"):
        try:
            return datetime.strptime(s[:10], fmt).date()
        except Exception:
            pass
    try:
        # excel serial sometimes comes as number string
        n = float(s)
        if 30000 < n < 60000:
            from datetime import date as _date
            return (_date(1899, 12, 30) + timedelta(days=int(n)))
    except Exception:
        pass
    return None

def _effective_cost_from_history(default_cost, dated_costs, as_of=None):
    """
    dated_costs: [(date, cost), ...] — только даты, где цена явно задана.
    Берём последнюю дату <= as_of, иначе default.
    """
    as_of = as_of or datetime.now(timezone.utc).date()
    applicable = [(d, c) for d, c in dated_costs if d is not None and d <= as_of and c is not None]
    if applicable:
        d, c = max(applicable, key=lambda x: x[0])
        return c, d.isoformat()
    if default_cost is not None:
        return default_cost, None
    return None, None

def _cost_entry_value(entry):
    """Достаёт актуальную себестоимость из float или объекта."""
    if entry is None:
        return None
    if isinstance(entry, dict):
        return _parse_cost_number(entry.get("cost"))
    return _parse_cost_number(entry)

def _cost_entry_meta(entry):
    if isinstance(entry, dict):
        return {
            "cost": _parse_cost_number(entry.get("cost")),
            "default": _parse_cost_number(entry.get("default")),
            "as_of": entry.get("as_of"),
        }
    c = _parse_cost_number(entry)
    return {"cost": c, "default": c, "as_of": None}

def parse_cost_price_workbook(contents: bytes, as_of=None):
    """
    Формат листа «Себестоимость»:
      row1: SKU | Артикул | … | По умолчанию | По умолчанию | 2026-03-30 | 2026-03-30 | …
      row2:          …        | Себестоимость | Фулфилмент | Себестоимость | Фулфилмент | …
    Для остатков берём только «Себестоимость»: default + последняя дата <= сегодня.
    """
    from openpyxl import load_workbook
    as_of = as_of or datetime.now(timezone.utc).date()
    wb = load_workbook(io.BytesIO(contents), data_only=True, read_only=True)
    # предпочитаем лист с «себестоим» в названии
    ws = None
    for name in wb.sheetnames:
        if "себестоим" in name.lower() or "cost" in name.lower():
            ws = wb[name]
            break
    if ws is None:
        ws = wb[wb.sheetnames[0]]

    rows_iter = ws.iter_rows(values_only=True)
    try:
        h1 = next(rows_iter)
        h2 = next(rows_iter)
    except StopIteration:
        wb.close()
        return {"by_vendor": {}, "by_nm": {}}, {"error": "Пустой файл"}

    # колонки себестоимости: (col_idx, date_or_None_for_default)
    cost_cols = []
    for i, (top, sub) in enumerate(zip(h1, h2)):
        sub_l = str(sub or "").strip().lower()
        top_s = str(top or "").strip().lower()
        if "себестоим" not in sub_l and "себестоим" not in top_s and "cost" not in sub_l:
            # default pair sometimes has sub only
            if top_s == "по умолчанию" and ("себестоим" in sub_l or sub_l == ""):
                # только если сосед/этот — себестоимость; skip fulfillment
                if "фулфил" in sub_l or "fulfill" in sub_l:
                    continue
            else:
                continue
        if "фулфил" in sub_l or "fulfill" in sub_l:
            continue
        d = _parse_header_date(top)
        is_default = d is None and ("умолчан" in top_s or top_s in ("", "none", "nan"))
        if d is None and not is_default and "умолчан" not in top_s:
            # заголовок не дата и не default — пропускаем
            continue
        cost_cols.append((i, d))  # d=None → default

    # если по sub-заголовку не нашли — ищем пары «По умолчанию»/даты где чётные = себес
    if not cost_cols:
        for i, top in enumerate(h1):
            top_s = str(top or "").strip().lower()
            d = _parse_header_date(top)
            if "умолчан" in top_s:
                # первая из пары default = себес (col 4), вторая фулфилмент
                # определяем: если следующий top такой же — это пара, берём только первый
                prev_same = i > 0 and str(h1[i - 1] or "").strip().lower() == top_s
                if prev_same:
                    continue  # вторая колонка пары
                cost_cols.append((i, None))
            elif d is not None:
                prev_d = _parse_header_date(h1[i - 1]) if i > 0 else None
                if prev_d == d:
                    continue  # fulfillment twin
                cost_cols.append((i, d))

    # артикул продавца + SKU (nm_id WB)
    vc_col = 1
    sku_col = 0
    for i, top in enumerate(h1):
        t = str(top or "").strip().lower()
        if t == "артикул" or "артикул продавца" in t:
            vc_col = i
        if t == "sku" or t in ("nm_id", "nmid", "код нм", "номенклатура"):
            sku_col = i

    default_idxs = [i for i, d in cost_cols if d is None]
    dated_idxs = [(i, d) for i, d in cost_cols if d is not None]

    by_vendor = {}
    by_nm = {}
    for row in rows_iter:
        if not row or vc_col >= len(row):
            continue
        vc = _norm_vendor_key(row[vc_col])
        if not vc or vc.lower() in ("артикул", "nan", "none"):
            continue
        nm_id = None
        if sku_col is not None and sku_col < len(row) and row[sku_col] not in (None, ""):
            try:
                nm_id = int(float(str(row[sku_col]).strip()))
            except Exception:
                nm_id = None
        default_cost = None
        for i in default_idxs:
            if i < len(row):
                c = _parse_cost_number(row[i])
                if c is not None:
                    default_cost = c
                    break
        dated = []
        for i, d in dated_idxs:
            if i < len(row) and row[i] not in (None, ""):
                c = _parse_cost_number(row[i])
                if c is not None:
                    dated.append((d, c))
        eff, as_of_used = _effective_cost_from_history(default_cost, dated, as_of)
        if eff is None:
            continue
        entry = {
            "cost": round(eff, 4),
            "default": round(default_cost, 4) if default_cost is not None else None,
            "as_of": as_of_used,
            "vendor_code": vc,
            "nm_id": nm_id,
            "history": (
                ([{"date": None, "cost": round(default_cost, 4)}] if default_cost is not None else [])
                + [{"date": d.isoformat(), "cost": round(c, 4)} for d, c in sorted(dated, key=lambda x: x[0])]
            ),
        }
        by_vendor[vc] = entry
        if nm_id is not None:
            by_nm[str(nm_id)] = entry
    wb.close()
    return {"by_vendor": by_vendor, "by_nm": by_nm}, {
        "format": "dated_cost_matrix",
        "default_cols": len(default_idxs),
        "date_cols": len(dated_idxs),
        "as_of": as_of.isoformat(),
        "vendors": len(by_vendor),
        "nms": len(by_nm),
    }

@app.post("/api/upload-costs")
async def upload_costs(file: UploadFile = File(...)):
    """
    Excel себестоимости:
    - формат с датами (По умолчанию + колонки дат Себестоимость/Фулфилмент)
    - или простой файл Артикул + Себестоимость
    Актуальная цена остатков = последняя себестоимость с датой <= сегодня, иначе «По умолчанию».
    Матчинг остатков WB: по артикулу продавца и по SKU (nm_id).
    """
    try:
        contents = await file.read()
        name = (file.filename or "").lower()
        by_vendor, by_nm = {}, {}
        parse_meta = {}

        if name.endswith(".xlsx") or name.endswith(".xls") or not name.endswith(".csv"):
            try:
                parsed, parse_meta = parse_cost_price_workbook(contents)
                by_vendor = (parsed or {}).get("by_vendor") or {}
                by_nm = (parsed or {}).get("by_nm") or {}
            except Exception as e:
                logger.warning(f"dated cost parse failed, fallback: {e}")
                by_vendor, by_nm, parse_meta = {}, {}, {"dated_error": str(e)}

        if not by_vendor:
            if name.endswith(".csv"):
                df = pd.read_csv(io.BytesIO(contents), dtype=str, sep=None, engine="python")
            else:
                xl = pd.ExcelFile(io.BytesIO(contents))
                best = None
                for s in xl.sheet_names:
                    tmp = pd.read_excel(io.BytesIO(contents), sheet_name=s, header=None, dtype=str)
                    for i, row in tmp.iterrows():
                        vals = [str(v).strip().lower() for v in row.values]
                        joined = " | ".join(vals)
                        if "артикул" in joined and ("себестоим" in joined or "cost" in joined or "умолчан" in joined):
                            best = (i, tmp)
                            break
                    if best:
                        break
                if not best:
                    tmp = pd.read_excel(io.BytesIO(contents), sheet_name=0, header=None, dtype=str)
                    best = (0, tmp)
                header_row, tmp = best
                tmp.columns = [str(c).strip() for c in tmp.iloc[header_row].tolist()]
                df = tmp.iloc[header_row + 1:].reset_index(drop=True)

            cols = {str(c).strip().lower(): c for c in df.columns}
            def find_col(*needles):
                for low, orig in cols.items():
                    for n in needles:
                        if n in low:
                            return orig
                return None
            col_vc = find_col("артикул продавца") or find_col("артикул") or find_col("vendor")
            col_sku = find_col("sku", "nm_id", "nmid")
            col_cost = find_col("по умолчанию") or find_col("себестоим", "cost", "закуп") or find_col("цена")
            if not col_vc or not col_cost:
                return {
                    "error": "Не удалось прочитать файл. Нужен Excel как cost_price: SKU + Артикул + По умолчанию + даты.",
                    "columns": list(df.columns.astype(str)),
                    "parse_meta": parse_meta,
                }
            for _, row in df.iterrows():
                vc = _norm_vendor_key(row.get(col_vc))
                if not vc or vc.lower() in ("nan", "none", "артикул"):
                    continue
                cost = _parse_cost_number(row.get(col_cost))
                if cost is None:
                    continue
                nm_id = None
                if col_sku is not None:
                    try:
                        nm_id = int(float(str(row.get(col_sku)).strip()))
                    except Exception:
                        nm_id = None
                entry = {
                    "cost": round(cost, 4),
                    "default": round(cost, 4),
                    "as_of": None,
                    "vendor_code": vc,
                    "nm_id": nm_id,
                    "history": [],
                }
                by_vendor[vc] = entry
                if nm_id is not None:
                    by_nm[str(nm_id)] = entry
            parse_meta["format"] = "simple"

        if not by_vendor:
            return {"error": "В файле не найдено артикулов с себестоимостью", "parse_meta": parse_meta}

        payload = {"_v": 2, "by_vendor": by_vendor, "by_nm": by_nm}
        save_setting_value(COST_PRICES_KEY, payload)
        meta = {
            "filename": file.filename,
            "uploaded_at": datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M"),
            "rows_in_file": len(by_vendor),
            "total_articles": len(by_vendor),
            "nm_mapped": len(by_nm),
            "format": parse_meta.get("format"),
            "as_of": parse_meta.get("as_of"),
            "date_cols": parse_meta.get("date_cols"),
        }
        save_setting_value(COST_META_KEY, meta)

        sample_vc = "039_DT10_mini_gold_O"
        sample = by_vendor.get(sample_vc)
        return {
            "status": "ok",
            "loaded": len(by_vendor),
            "total": len(by_vendor),
            "nm_mapped": len(by_nm),
            "meta": meta,
            "sample": {sample_vc: sample} if sample else None,
        }
    except Exception as e:
        logger.error(f"upload-costs error: {e}")
        return {"error": str(e)}

def _load_cost_indexes():
    """Возвращает (by_vendor, by_nm) из settings — поддерживает старый и новый формат."""
    raw = get_setting_json(COST_PRICES_KEY, {}) or {}
    if not isinstance(raw, dict):
        return {}, {}
    if raw.get("_v") == 2 or ("by_vendor" in raw or "by_nm" in raw):
        by_vendor = raw.get("by_vendor") or {}
        by_nm = raw.get("by_nm") or {}
    else:
        # старый формат: {vendor: entry|float}
        by_vendor, by_nm = {}, {}
        for k, v in raw.items():
            if str(k).startswith("_"):
                continue
            vc = _norm_vendor_key(k)
            if not vc:
                continue
            meta = _cost_entry_meta(v)
            if meta["cost"] is None:
                continue
            entry = {**meta, "vendor_code": vc, "nm_id": None}
            if isinstance(v, dict):
                entry["nm_id"] = v.get("nm_id")
                entry["history"] = v.get("history") or []
                if entry["nm_id"] is not None:
                    by_nm[str(entry["nm_id"])] = entry
            by_vendor[vc] = entry
    # нормализуем meta
    out_v, out_n = {}, {}
    for vc, v in by_vendor.items():
        m = _cost_entry_meta(v)
        if m["cost"] is None:
            continue
        entry = {
            **m,
            "vendor_code": (v.get("vendor_code") if isinstance(v, dict) else None) or vc,
            "nm_id": v.get("nm_id") if isinstance(v, dict) else None,
        }
        out_v[_norm_vendor_key(vc)] = entry
        if entry.get("nm_id") is not None:
            out_n[str(entry["nm_id"])] = entry
    for nm, v in by_nm.items():
        m = _cost_entry_meta(v)
        if m["cost"] is None:
            continue
        entry = {
            **m,
            "vendor_code": (v.get("vendor_code") if isinstance(v, dict) else None) or "",
            "nm_id": int(nm) if str(nm).isdigit() else (v.get("nm_id") if isinstance(v, dict) else None),
        }
        out_n[str(nm)] = entry
        if entry["vendor_code"]:
            out_v.setdefault(_norm_vendor_key(entry["vendor_code"]), entry)
    return out_v, out_n

@app.get("/api/finance")
def get_finance():
    """Себестоимость остатков: WB + наш склад (актуальная цена на сегодня)."""
    by_vendor, by_nm = _load_cost_indexes()
    meta = get_setting_json(COST_META_KEY, {}) or {}

    def resolve_cost(vendor_code=None, nm_id=None):
        vc = _norm_vendor_key(vendor_code)
        if vc and vc in by_vendor:
            return by_vendor[vc]
        if nm_id is not None and str(nm_id) in by_nm:
            return by_nm[str(nm_id)]
        return {}

    # WB остатки — vendor_code в stock_totals часто пустой, матчим по nm_id (SKU из файла)
    wb_rows = []
    try:
        st = httpx.get(
            f"{SUPABASE_URL}/rest/v1/stock_totals?select=nm_id,vendor_code,quantity_warehouses_full,in_way_to_client,in_way_from_client,subject_name",
            headers=sb_headers(), timeout=20,
        )
        if st.is_success:
            for r in st.json() or []:
                nm_id = r.get("nm_id")
                qty = int(r.get("quantity_warehouses_full") or 0)
                if qty <= 0:
                    continue
                cm = resolve_cost(r.get("vendor_code"), nm_id)
                cost = cm.get("cost")
                value = round(qty * cost, 2) if cost is not None else None
                seller = _norm_vendor_key(cm.get("vendor_code") or r.get("vendor_code")) or ""
                wb_rows.append({
                    "vendor_code": seller or (str(nm_id) if nm_id else ""),
                    "nm_id": nm_id,
                    "name": r.get("subject_name") or "",
                    "qty": qty,
                    "cost": cost,
                    "cost_default": cm.get("default"),
                    "cost_as_of": cm.get("as_of"),
                    "value": value,
                    "in_way": int(r.get("in_way_to_client") or 0) + int(r.get("in_way_from_client") or 0),
                })
    except Exception as e:
        logger.error(f"finance stock_totals: {e}")

    # Наш склад
    own = OWN_WAREHOUSE_CACHE.get("rows") or []
    if not own and not OWN_WAREHOUSE_CACHE.get("syncing"):
        try:
            refresh_own_warehouse_stock()
            own = OWN_WAREHOUSE_CACHE.get("rows") or []
        except Exception as e:
            logger.error(f"finance own-wh refresh: {e}")

    own_rows = []
    seen_own = set()
    for r in own:
        vc = _norm_vendor_key(r.get("vendor_code"))
        if not vc or vc in seen_own:
            continue
        seen_own.add(vc)
        qty = int(r.get("stock") or 0)
        if qty <= 0:
            continue
        cm = resolve_cost(vc, None)
        cost = cm.get("cost")
        value = round(qty * cost, 2) if cost is not None else None
        own_rows.append({
            "vendor_code": vc,
            "name": r.get("name") or "",
            "qty": qty,
            "cost": cost,
            "cost_default": cm.get("default"),
            "cost_as_of": cm.get("as_of"),
            "value": value,
            "family_stock": r.get("family_stock"),
        })

    def summarize(rows):
        with_cost = [x for x in rows if x.get("value") is not None]
        without = [x for x in rows if x.get("value") is None]
        return {
            "total_value": round(sum(x["value"] for x in with_cost), 2),
            "total_qty": sum(x["qty"] for x in rows),
            "qty_with_cost": sum(x["qty"] for x in with_cost),
            "qty_without_cost": sum(x["qty"] for x in without),
            "articles": len(rows),
            "articles_without_cost": len(without),
        }

    wb_sum = summarize(wb_rows)
    own_sum = summarize(own_rows)
    wb_rows.sort(key=lambda x: (-(x["value"] or 0), str(x["vendor_code"])))
    own_rows.sort(key=lambda x: (-(x["value"] or 0), str(x["vendor_code"])))

    return {
        "costs_count": len(by_vendor),
        "nm_mapped": len(by_nm),
        "meta": meta,
        "wb": {**wb_sum, "rows": wb_rows},
        "own": {
            **own_sum,
            "rows": own_rows,
            "as_of": OWN_WAREHOUSE_CACHE.get("as_of"),
            "updated_at": OWN_WAREHOUSE_CACHE.get("updated_at"),
        },
        "grand_total": round(wb_sum["total_value"] + own_sum["total_value"], 2),
        "costs": sorted(
            [
                {
                    "vendor_code": e.get("vendor_code") or vc,
                    "nm_id": e.get("nm_id"),
                    "cost": e.get("cost"),
                    "default": e.get("default"),
                    "as_of": e.get("as_of"),
                    "manual": bool(e.get("manual")),
                }
                for vc, e in by_vendor.items()
            ],
            key=lambda x: str(x.get("vendor_code") or ""),
        ),
    }

@app.post("/api/finance/cost")
async def save_finance_cost(request: dict):
    """Ручное изменение себестоимости по артикулу продавца и/или nm_id."""
    vc = _norm_vendor_key(request.get("vendor_code"))
    nm_raw = request.get("nm_id")
    nm_id = None
    if nm_raw not in (None, ""):
        try:
            nm_id = int(float(str(nm_raw).strip()))
        except Exception:
            return {"error": "Некорректный nm_id"}
    cost = _parse_cost_number(request.get("cost"))
    if cost is None:
        return {"error": "Укажи себестоимость числом ≥ 0"}
    if not vc and nm_id is None:
        return {"error": "Нужен артикул продавца или nm_id"}

    raw = get_setting_json(COST_PRICES_KEY, {}) or {}
    if not isinstance(raw, dict):
        raw = {}
    if raw.get("_v") == 2 or "by_vendor" in raw or "by_nm" in raw:
        by_vendor = dict(raw.get("by_vendor") or {})
        by_nm = dict(raw.get("by_nm") or {})
    else:
        by_vendor, by_nm = {}, {}
        for k, v in raw.items():
            if str(k).startswith("_"):
                continue
            key = _norm_vendor_key(k)
            if not key:
                continue
            m = _cost_entry_meta(v)
            entry = {
                **m,
                "vendor_code": key,
                "nm_id": v.get("nm_id") if isinstance(v, dict) else None,
                "history": v.get("history") if isinstance(v, dict) else [],
            }
            by_vendor[key] = entry
            if entry.get("nm_id") is not None:
                by_nm[str(entry["nm_id"])] = entry

    # найти существующую запись
    prev = None
    if vc and vc in by_vendor:
        prev = by_vendor[vc]
    elif nm_id is not None and str(nm_id) in by_nm:
        prev = by_nm[str(nm_id)]
        if not vc:
            vc = _norm_vendor_key(prev.get("vendor_code"))

    today = datetime.now(timezone.utc).date().isoformat()
    prev_default = None
    prev_history = []
    if isinstance(prev, dict):
        prev_default = _parse_cost_number(prev.get("default"))
        prev_history = list(prev.get("history") or [])
        if not vc:
            vc = _norm_vendor_key(prev.get("vendor_code"))
        if nm_id is None and prev.get("nm_id") is not None:
            nm_id = prev.get("nm_id")

    if not vc:
        vc = f"nm_{nm_id}" if nm_id is not None else ""
    if prev_default is None:
        prev_default = cost

    entry = {
        "cost": round(cost, 4),
        "default": round(prev_default, 4) if prev_default is not None else round(cost, 4),
        "as_of": today,
        "vendor_code": vc,
        "nm_id": nm_id,
        "manual": True,
        "history": prev_history + [{"date": today, "cost": round(cost, 4), "manual": True}],
    }
    by_vendor[vc] = entry
    if nm_id is not None:
        by_nm[str(nm_id)] = entry

    save_setting_value(COST_PRICES_KEY, {"_v": 2, "by_vendor": by_vendor, "by_nm": by_nm})
    meta = get_setting_json(COST_META_KEY, {}) or {}
    if isinstance(meta, dict):
        meta["last_manual_edit"] = datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M")
        meta["total_articles"] = len(by_vendor)
        save_setting_value(COST_META_KEY, meta)

    return {"status": "ok", "entry": entry}

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8080))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
