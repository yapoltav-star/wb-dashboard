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

def process_feedback(f: dict) -> dict:
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=365)
    pd_data = f.get("productDetails", {})
    article = pd_data.get("supplierArticle", "") or str(pd_data.get("nmId", ""))
    date_str = f.get("createdDate") or f.get("updatedDate") or ""
    try:
        date = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except:
        date = now
    return {
        "id": f.get("id", ""),
        "article": article,
        "nm_id": pd_data.get("nmId"),
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

    for is_answered in [True, False]:
        skip = 0
        while skip <= 199990:
            batch = fetch_feedbacks_page(is_answered, skip)
            if not batch:
                break
            processed = [process_feedback(f) for f in batch if f.get("id") and not is_supplemented(f)]
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
        processed = [process_feedback(f) for f in batch if f.get("id")]
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

            rows.append({
                "article": article,
                "nm_id": safe_int('nm_id') or None,
                "name": str(row.get(col_map.get('name', ''), '') or '').strip() or None,
                "wb_rating": safe_float('wb_rating'),
                "reviews_total": safe_int('reviews_total'),
                "r5": safe_int('r5'),
                "r4": safe_int('r4'),
                "r3": safe_int('r3'),
                "r2": safe_int('r2'),
                "r1": safe_int('r1'),
                "excluded": safe_int_abs('excluded'),
                "updated_at": now
            })

        if not rows:
            return {"error": "Не найдено строк с данными"}

        # Сохраняем в Supabase батчами по 100
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

scheduler = BackgroundScheduler()
scheduler.add_job(sync_all, "interval", minutes=30, id="sync")
scheduler.add_job(sync_stock, "interval", hours=3, id="sync_stock")
scheduler.add_job(sync_supply, "interval", hours=4, id="sync_supply")
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
    result = {"groups": [], "ratings": [], "feedback_stats": [], "negative_counts": {}, "settings": {}, "stock_totals": [], "stock_warehouses": [], "supply_report": []}

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
