import httpx
import os
import io
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

# ---------- Proxy endpoints: фронтенд обращается только к Railway, ----------
# ---------- никогда напрямую к Supabase (для пользователей у которых ----------
# ---------- Supabase плохо доступен напрямую). Railway сам ходит в Supabase. ----------

@app.get("/api/dashboard-data")
def dashboard_data():
    """Отдаёт все данные нужные дашборду одним запросом: группы + рейтинги + отзывы(агрегат) + негатив за периоды + last sync"""
    result = {"groups": [], "ratings": [], "feedback_stats": [], "negative_counts": {}, "settings": {}}

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
