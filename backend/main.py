import httpx
import os
import time
import logging
from datetime import datetime, timedelta, timezone
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

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
    """Returns True if this feedback was supplemented (updated) by another one.
    WB marks supplemented feedbacks with 'isEdited' or they have a supplement link.
    The original feedback (before supplement) should NOT count towards rating.
    """
    # WB API field: if feedback has been supplemented, it has 'editedAt' or similar
    # The original review that was supplemented has a newer version
    return bool(f.get("isEdited") or f.get("supplementedFeedbackId"))

def process_feedback(f: dict) -> dict:
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=365)
    pd = f.get("productDetails", {})
    article = pd.get("supplierArticle", "") or str(pd.get("nmId", ""))
    date_str = f.get("createdDate") or f.get("updatedDate") or ""
    try:
        date = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except:
        date = now
    return {
        "id": f.get("id", ""),
        "article": article,
        "nm_id": pd.get("nmId"),
        "stars": f.get("productValuation", 0),
        "created_date": date.isoformat(),
        "is_old": date < cutoff,
        "is_answered": bool(f.get("answer")),
        "is_supplemented": is_supplemented(f),
        "text": (f.get("text") or "")[:500],
        "updated_at": now.isoformat()
    }

def sync_all():
    if not WB_TOKEN:
        logger.error("WB_TOKEN not set")
        return
    logger.info("Starting sync...")
    total = 0

    # Answered + unanswered
    for is_answered in [True, False]:
        skip = 0
        while skip <= 199990:
            batch = fetch_feedbacks_page(is_answered, skip)
            if not batch:
                break
            # Filter out supplemented (original) feedbacks - they don't count for rating
            processed = [process_feedback(f) for f in batch if f.get("id") and not is_supplemented(f)]
            total += upsert_feedbacks(processed)
            logger.info(f"  answered={is_answered} skip={skip} saved={len(processed)}")
            skip += len(batch)
            if len(batch) < 5000:
                break
            time.sleep(0.3)

    # Archive
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

    # Save last sync time
    now_str = datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M")
    httpx.post(
        f"{SUPABASE_URL}/rest/v1/settings",
        json={"key": "last_sync", "value": now_str, "updated_at": datetime.now(timezone.utc).isoformat()},
        headers=sb_headers(), timeout=10
    )
    logger.info(f"Sync complete. Total: {total}")

# Scheduler - every 30 minutes
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

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8080))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
