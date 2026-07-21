import httpx
import os
import io
import json
import time
import logging
from datetime import datetime, timedelta, timezone
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd

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

def fetch_campaigns_meta() -> dict:
    """Возвращает {advertId: {name, type_id, type_name}} для активных/паузных/завершённых кампаний."""
    try:
        resp = httpx.get(f"{WB_PROMOTION_URL}/adv/v1/promotion/count", headers=wb_headers(), timeout=20)
        if not resp.is_success:
            logger.error(f"WB promotion/count error {resp.status_code} {resp.text[:300]}")
            return {}
        data = resp.json()
    except Exception as e:
        logger.error(f"WB promotion/count exception: {e}")
        return {}

    meta = {}
    for group in data.get("adverts", []):
        type_id = group.get("type", 0)
        type_name = AD_TYPE_NAMES.get(type_id, f"Тип {type_id}")
        if group.get("status") in (7, 9, 11):
            for item in group.get("advert_list", []):
                aid = item.get("advertId")
                if aid:
                    meta[aid] = {
                        "type_id": type_id,
                        "type_name": type_name,
                        "name": f"{type_name} #{aid}"  # имя уточним отдельным запросом
                    }

    # Получаем имена кампаний батчами по 50
    all_ids = list(meta.keys())
    for i in range(0, len(all_ids), 50):
        batch = all_ids[i:i+50]
        try:
            resp = httpx.post(
                f"{WB_PROMOTION_URL}/adv/v1/promotion/adverts",
                json=batch, headers=wb_headers(), timeout=20
            )
            if resp.is_success:
                for camp in resp.json() or []:
                    aid = camp.get("advertId")
                    if aid and aid in meta:
                        name = camp.get("name", "").strip()
                        if name:
                            meta[aid]["name"] = name
        except Exception as e:
            logger.error(f"WB promotion/adverts exception: {e}")
        if i + 50 < len(all_ids):
            time.sleep(1)
    return meta

def fetch_ad_stats_per_campaign(ids: list, begin_date: str, end_date: str) -> dict:
    """Тянет /adv/v3/fullstats и возвращает {(nm_id, campaign_id): {...метрики...}}."""
    agg = {}
    for i in range(0, len(ids), 50):
        batch = ids[i:i + 50]
        try:
            resp = httpx.get(
                f"{WB_PROMOTION_URL}/adv/v3/fullstats",
                headers=wb_headers(),
                params={"ids": ",".join(str(x) for x in batch), "beginDate": begin_date, "endDate": end_date},
                timeout=30
            )
            if not resp.is_success:
                logger.error(f"WB fullstats error {resp.status_code} {resp.text[:300]}")
                continue
            campaigns = resp.json()
        except Exception as e:
            logger.error(f"WB fullstats exception: {e}")
            continue

        for camp in campaigns or []:
            campaign_id = camp.get("advertId")
            if not campaign_id:
                continue
            for day in camp.get("days", []):
                for app in day.get("apps", []):
                    for nm in app.get("nms", []):
                        nm_id = nm.get("nmId")
                        if not nm_id:
                            continue
                        key = (nm_id, campaign_id)
                        a = agg.setdefault(key, {
                            "views": 0, "clicks": 0, "atbs": 0,
                            "orders": 0, "spend": 0.0, "revenue": 0.0
                        })
                        a["views"]   += nm.get("views", 0) or 0
                        a["clicks"]  += nm.get("clicks", 0) or 0
                        a["atbs"]    += nm.get("atbs", 0) or 0
                        a["orders"]  += nm.get("orders", 0) or 0
                        a["spend"]   += nm.get("sum", 0) or 0
                        # sum_price = выручка (стоимость заказов) — для ДРР
                        a["revenue"] += nm.get("sum_price", 0) or 0

        if i + 50 < len(ids):
            time.sleep(7)  # лимит 3 запроса / 20 сек
    return agg

def sync_ads():
    if not WB_TOKEN:
        logger.error("WB_TOKEN not set")
        return
    logger.info("Starting ads (promotion) sync...")
    window_days = get_setting_int("ads_window_days", 30)
    window_days = min(window_days, 31)
    end_date = datetime.now(timezone.utc).date()
    begin_date = end_date - timedelta(days=window_days - 1)

    campaigns_meta = fetch_campaigns_meta()
    if not campaigns_meta:
        logger.info("Ads sync: no eligible campaigns")
        httpx.delete(f"{SUPABASE_URL}/rest/v1/ad_stats?id=gte.0", headers=sb_headers(), timeout=15)
        return

    logger.info(f"Ads sync: {len(campaigns_meta)} campaigns")
    agg = fetch_ad_stats_per_campaign(list(campaigns_meta.keys()), begin_date.isoformat(), end_date.isoformat())

    try:
        st = httpx.get(f"{SUPABASE_URL}/rest/v1/stock_totals?select=nm_id,vendor_code", headers=sb_headers(), timeout=15)
        nm_to_vendor = {r["nm_id"]: r["vendor_code"] for r in st.json()} if st.is_success else {}
    except Exception as e:
        logger.error(f"sync_ads: stock_totals fetch error {e}")
        nm_to_vendor = {}

    # Добираем маппинг из feedbacks (там article = артикул продавца) для тех nm_id которых нет в stock_totals
    try:
        fb = httpx.get(
            f"{SUPABASE_URL}/rest/v1/feedbacks?select=nm_id,article&nm_id=not.is.null&article=not.is.null",
            headers=sb_headers(), timeout=15
        )
        if fb.is_success:
            for r in fb.json():
                nm = r.get("nm_id")
                art = r.get("article", "")
                if nm and art and nm not in nm_to_vendor:
                    nm_to_vendor[nm] = art
    except Exception as e:
        logger.error(f"sync_ads: feedbacks fallback error {e}")

    now = datetime.now(timezone.utc).isoformat()
    rows = []
    for (nm_id, campaign_id), a in agg.items():
        camp = campaigns_meta.get(campaign_id, {})
        views, clicks, atbs, orders = a["views"], a["clicks"], a["atbs"], a["orders"]
        spend, revenue = round(a["spend"], 2), round(a["revenue"], 2)
        ctr  = round(clicks / views * 100, 2) if views else 0
        cpc  = round(spend / clicks, 2) if clicks else 0
        cr   = round(orders / clicks * 100, 2) if clicks else 0
        cv_atb  = round(atbs / clicks * 100, 2) if clicks else 0   # клики → корзина
        cv_ord  = round(orders / atbs * 100, 2) if atbs else 0     # корзина → заказ
        drr  = round(spend / revenue * 100, 2) if revenue else 0
        rows.append({
            "vendor_code":   nm_to_vendor.get(nm_id) or str(nm_id),
            "nm_id":         nm_id,
            "campaign_id":   campaign_id,
            "campaign_name": camp.get("name", f"#{campaign_id}"),
            "campaign_type": camp.get("type_name", "Неизвестно"),
            "views": views, "clicks": clicks, "atbs": atbs,
            "orders": orders, "spend": spend, "revenue": revenue,
            "ctr": ctr, "cpc": cpc, "cr": cr,
            "cv_atb": cv_atb, "cv_ord": cv_ord, "drr": drr,
            "period_days": window_days,
            "updated_at": now,
        })

    httpx.delete(f"{SUPABASE_URL}/rest/v1/ad_stats?id=gte.0", headers=sb_headers(), timeout=15)
    saved = 0
    for i in range(0, len(rows), 300):
        batch = rows[i:i + 300]
        resp = httpx.post(f"{SUPABASE_URL}/rest/v1/ad_stats", json=batch, headers=sb_headers(), timeout=30)
        if resp.is_success:
            saved += len(batch)
        else:
            logger.error(f"ad_stats insert error {resp.status_code} {resp.text[:300]}")

    httpx.post(
        f"{SUPABASE_URL}/rest/v1/settings",
        json={"key": "last_ads_sync", "value": datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M"), "updated_at": now},
        headers=sb_headers(), timeout=10
    )
    logger.info(f"Ads sync complete. Rows saved: {saved}")

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

# ---------- Рост продаж: темп «сегодня до сейчас» vs «вчера до того же времени» ----------
# Заказы — точно по времени из Statistics API.
# Воронка (показы/корзина/заказы в аналитике) обновляется у WB раз в час — копим снимки
# и сравниваем с ближайшим снимком сутки назад.
SALES_PACE_CACHE = {
    "articles": [], "as_of": None, "compare_as_of": None,
    "syncing": False, "error": None, "updated_at": None,
}
SALES_PACE_SNAPS_KEY = "sales_pace_funnel_snaps"

def _msk_now():
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Europe/Moscow")).replace(tzinfo=None)
    except Exception:
        return datetime.utcnow() + timedelta(hours=3)

def _funnel_products_day(day_str: str, nm_ids: list = None) -> dict:
    """Воронка за один день → {nm_id: {opens, cart, orders, vendor_code, name}}."""
    if not WB_TOKEN:
        return {}
    out = {}
    offset = 0
    limit = 1000
    for _ in range(20):
        body = {
            "selectedPeriod": {"start": day_str, "end": day_str},
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

def sync_sales_pace():
    """Считает темп продаж по артикулам и сохраняет снимок воронки."""
    if not WB_TOKEN:
        SALES_PACE_CACHE["error"] = "WB_TOKEN не задан"
        return
    if SALES_PACE_CACHE.get("syncing"):
        return
    SALES_PACE_CACHE["syncing"] = True
    SALES_PACE_CACHE["error"] = None
    try:
        now = _msk_now()
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        yest_start = today_start - timedelta(days=1)
        yest_same = yest_start + (now - today_start)
        today_str = today_start.strftime("%Y-%m-%d")
        yest_str = yest_start.strftime("%Y-%m-%d")

        # ── Заказы: точно до текущего времени / до того же времени вчера ──
        date_from = yest_start.strftime("%Y-%m-%dT00:00:00")
        orders = fetch_supplier_feed("/api/v1/supplier/orders", date_from, max_pages=3)
        today_ord = {}
        yest_ord = {}
        vc_from_orders = {}
        for o in orders:
            nm = o.get("nmId")
            if not nm:
                continue
            d = parse_wb_dt(o.get("date", ""))
            if d is None:
                continue
            if o.get("supplierArticle"):
                vc_from_orders[nm] = o["supplierArticle"]
            if today_start <= d <= now:
                today_ord[nm] = today_ord.get(nm, 0) + 1
            elif yest_start <= d <= yest_same:
                yest_ord[nm] = yest_ord.get(nm, 0) + 1

        # ── Воронка сегодня (кумулятив с начала дня) ──
        funnel_today = _funnel_products_day(today_str)

        # Снимки: сохраняем текущую воронку, ищем вчерашний около того же часа
        hour_key = now.strftime("%Y-%m-%dT%H")
        snaps = get_setting_json(SALES_PACE_SNAPS_KEY, []) or []
        if not isinstance(snaps, list):
            snaps = []
        snap_payload = {
            "hour_key": hour_key,
            "as_of": now.strftime("%Y-%m-%d %H:%M"),
            "day": today_str,
            "products": {
                str(nm): {"opens": v["opens"], "cart": v["cart"], "orders": v["orders"]}
                for nm, v in funnel_today.items()
            },
        }
        snaps = [s for s in snaps if s.get("hour_key") != hour_key]
        snaps.append(snap_payload)
        # храним ~3 суток почасовых снимков
        cutoff_day = (today_start - timedelta(days=3)).strftime("%Y-%m-%d")
        snaps = [s for s in snaps if (s.get("day") or "") >= cutoff_day]
        snaps.sort(key=lambda s: s.get("hour_key") or "")
        save_setting_value(SALES_PACE_SNAPS_KEY, snaps)

        target_yest_hour = yest_same.strftime("%Y-%m-%dT%H")
        yest_snap = None
        for s in snaps:
            if s.get("day") == yest_str and (s.get("hour_key") or "") <= target_yest_hour:
                yest_snap = s
        # если нет снимка ≤ часа — возьмём любой за вчера ближайший
        if yest_snap is None:
            yest_candidates = [s for s in snaps if s.get("day") == yest_str]
            if yest_candidates:
                yest_snap = min(
                    yest_candidates,
                    key=lambda s: abs(
                        (datetime.strptime(s["hour_key"], "%Y-%m-%dT%H") - yest_same.replace(minute=0, second=0, microsecond=0)).total_seconds()
                    ) if s.get("hour_key") else 10**9
                )

        funnel_yest = (yest_snap or {}).get("products") or {}
        compare_as_of = (yest_snap or {}).get("as_of")

        # vendor map fallback
        try:
            st = httpx.get(
                f"{SUPABASE_URL}/rest/v1/stock_totals?select=nm_id,vendor_code",
                headers=sb_headers(), timeout=15
            )
            nm_to_vendor = {r["nm_id"]: r["vendor_code"] for r in st.json()} if st.is_success else {}
        except Exception:
            nm_to_vendor = {}

        all_nms = set(today_ord) | set(yest_ord) | set(funnel_today) | {int(k) for k in funnel_yest.keys() if str(k).isdigit()}
        articles = []
        for nm in all_nms:
            ft = funnel_today.get(nm) or {}
            fy = funnel_yest.get(str(nm)) or funnel_yest.get(nm) or {}
            o_t = today_ord.get(nm, 0)
            o_y = yest_ord.get(nm, 0)
            opens_t = int(ft.get("opens") or 0)
            opens_y = int(fy.get("opens") or 0)
            cart_t = int(ft.get("cart") or 0)
            cart_y = int(fy.get("cart") or 0)
            # для заказов приоритет — Statistics (точнее по времени)
            articles.append({
                "nm_id": nm,
                "vendor_code": ft.get("vendor_code") or nm_to_vendor.get(nm) or vc_from_orders.get(nm) or str(nm),
                "name": ft.get("name") or "",
                "orders_today": o_t,
                "orders_yesterday": o_y,
                "orders_delta": o_t - o_y,
                "opens_today": opens_t,
                "opens_yesterday": opens_y,
                "opens_delta": opens_t - opens_y if yest_snap else None,
                # openCount у WB = переходы в карточку (клики)
                "clicks_today": opens_t,
                "clicks_yesterday": opens_y,
                "clicks_delta": opens_t - opens_y if yest_snap else None,
                "cart_today": cart_t,
                "cart_yesterday": cart_y,
                "cart_delta": cart_t - cart_y if yest_snap else None,
                "funnel_compare_ready": bool(yest_snap),
            })

        # падение заказов — выше
        articles.sort(key=lambda a: (a["orders_delta"], a["orders_today"], str(a["vendor_code"])))

        SALES_PACE_CACHE.update({
            "articles": articles,
            "as_of": now.strftime("%d.%m.%Y %H:%M"),
            "compare_as_of": compare_as_of,
            "today": today_str,
            "yesterday": yest_str,
            "now_time": now.strftime("%H:%M"),
            "updated_at": datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M"),
            "funnel_snaps": len([s for s in snaps if s.get("day") == yest_str]),
            "syncing": False,
            "error": None,
        })
        logger.info(f"sales-pace: {len(articles)} arts, as_of={SALES_PACE_CACHE['as_of']}, yest_snap={compare_as_of}")
    except Exception as e:
        logger.error(f"sync_sales_pace error: {e}")
        SALES_PACE_CACHE["error"] = str(e)
        SALES_PACE_CACHE["syncing"] = False
    finally:
        SALES_PACE_CACHE["syncing"] = False

@app.get("/api/sales-pace")
def get_sales_pace(refresh: bool = False):
    if refresh or not SALES_PACE_CACHE.get("articles"):
        if not SALES_PACE_CACHE.get("syncing"):
            import threading
            threading.Thread(target=sync_sales_pace, daemon=True).start()
    return {
        "articles": SALES_PACE_CACHE.get("articles") or [],
        "as_of": SALES_PACE_CACHE.get("as_of"),
        "compare_as_of": SALES_PACE_CACHE.get("compare_as_of"),
        "today": SALES_PACE_CACHE.get("today"),
        "yesterday": SALES_PACE_CACHE.get("yesterday"),
        "now_time": SALES_PACE_CACHE.get("now_time"),
        "updated_at": SALES_PACE_CACHE.get("updated_at"),
        "funnel_snaps": SALES_PACE_CACHE.get("funnel_snaps"),
        "syncing": SALES_PACE_CACHE.get("syncing", False),
        "error": SALES_PACE_CACHE.get("error"),
    }

@app.post("/api/sync-sales-pace")
def trigger_sales_pace_sync():
    import threading
    if SALES_PACE_CACHE.get("syncing"):
        return {"status": "already_running"}
    threading.Thread(target=sync_sales_pace, daemon=True).start()
    return {"status": "started"}

scheduler = BackgroundScheduler()
scheduler.add_job(sync_all, "interval", minutes=30, id="sync")
scheduler.add_job(sync_stock, "interval", hours=3, id="sync_stock")
scheduler.add_job(sync_supply, "interval", hours=4, id="sync_supply")
scheduler.add_job(sync_ads, "interval", hours=4, id="sync_ads")
scheduler.add_job(lambda: sync_article_daily_stats(30), "interval", hours=6, id="sync_daily")
scheduler.add_job(sync_promotions, "interval", hours=6, id="sync_promotions")
scheduler.add_job(sync_sales_pace, "interval", hours=1, id="sync_sales_pace")
scheduler.start()

@app.get("/")
def root():
    return {"status": "ok"}

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
            f"{SUPABASE_URL}/rest/v1/settings",
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

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8080))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
