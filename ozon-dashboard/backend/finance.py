"""Финансы — начисления (accrual) + компенсации; поиск соинвеста в ₽."""

from __future__ import annotations

import logging
import threading
import time
from datetime import date, datetime, timedelta, timezone
from typing import Callable

logger = logging.getLogger("ozon-dashboard.finance")

FINANCE_CACHE: dict = {
    "days": [],
    "types": [],
    "coinvest_types": [],
    "by_type": [],
    "coinvest_total": 0,
    "total": 0,
    "compensation": None,
    "updated_at": None,
    "syncing": False,
    "error": None,
    "period_days": 7,
}

_lock = threading.Lock()

COINVEST_HINTS = (
    "соинвест",
    "скидк",
    "компенсац",
    "балл",
    "акци",
    "promo",
    "discount",
    "за счёт ozon",
    "за счет ozon",
    "marketing",
    "продвижен",
)


def _money(v) -> float:
    if v is None:
        return 0.0
    if isinstance(v, dict):
        return _money(v.get("amount"))
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _is_coinvest_type(name: str, desc: str) -> bool:
    blob = f"{name} {desc}".lower()
    return any(h in blob for h in COINVEST_HINTS)


def fetch_accrual_types(ozon_post: Callable) -> list[dict]:
    payload = ozon_post("/v1/finance/accrual/types", {})
    rows = payload.get("accrual_types") or []
    out = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        name = str(r.get("name") or "")
        desc = str(r.get("description") or "")
        out.append({
            "id": int(r.get("id") or 0),
            "name": name,
            "description": desc,
            "is_coinvest": _is_coinvest_type(name, desc),
        })
    return out


def fetch_accruals_day(ozon_post: Callable, day: date, max_pages: int = 50) -> list[dict]:
    out = []
    last_id = ""
    for _ in range(max_pages):
        body = {"date": day.isoformat()}
        if last_id:
            body["last_id"] = last_id
        payload = ozon_post("/v1/finance/accrual/by-day", body)
        batch = payload.get("accruals") or []
        out.extend(batch)
        last_id = payload.get("last_id") or ""
        if not batch or not last_id:
            break
        time.sleep(0.08)
    return out


def _extract_fees(acc: dict) -> list[dict]:
    """Разворачивает type_id + amount из accrual."""
    rows = []
    item_fees = acc.get("item_fees") or {}
    for block in item_fees.get("fees") or []:
        sku = block.get("sku")
        for fee in block.get("fees") or []:
            rows.append({
                "type_id": int(fee.get("type_id") or 0),
                "amount": _money(fee.get("accrued")),
                "sku": sku,
                "date": acc.get("date"),
                "posting": (acc.get("posting") or {}).get("posting_number") if isinstance(acc.get("posting"), dict) else None,
            })
    non = acc.get("non_item_fee")
    if isinstance(non, dict):
        # иногда плоская структура
        tid = non.get("type_id")
        if tid is not None:
            rows.append({
                "type_id": int(tid),
                "amount": _money(non.get("accrued") or non.get("amount") or acc.get("total_amount")),
                "sku": None,
                "date": acc.get("date"),
                "posting": None,
            })
        for fee in non.get("fees") or []:
            rows.append({
                "type_id": int(fee.get("type_id") or 0),
                "amount": _money(fee.get("accrued")),
                "sku": None,
                "date": acc.get("date"),
                "posting": None,
            })
    if not rows:
        rows.append({
            "type_id": 0,
            "amount": _money(acc.get("total_amount")),
            "sku": None,
            "date": acc.get("date"),
            "posting": (acc.get("posting") or {}).get("posting_number") if isinstance(acc.get("posting"), dict) else None,
            "category": acc.get("accrued_category"),
        })
    return rows


def request_compensation_report(ozon_post: Callable, ym: str) -> dict:
    """Асинхронный XLSX: Финансы → Компенсации."""
    payload = ozon_post("/v1/finance/compensation", {"date": ym, "language": "RU"})
    result = payload.get("result") or payload
    code = result.get("code") if isinstance(result, dict) else None
    return {"code": code}


def poll_report(ozon_post: Callable, code: str, tries: int = 20) -> dict:
    for i in range(tries):
        payload = ozon_post("/v1/report/info", {"code": code})
        result = payload.get("result") or payload
        if isinstance(result, list) and result:
            result = result[0]
        if not isinstance(result, dict):
            time.sleep(1.5)
            continue
        status = (result.get("status") or "").lower()
        if status == "success":
            return {
                "status": status,
                "file": result.get("file"),
                "code": result.get("code") or code,
                "report_type": result.get("report_type"),
                "error": result.get("error"),
            }
        if status in ("failed", "error"):
            return {"status": status, "error": result.get("error") or "report failed", "code": code}
        time.sleep(1.2 + i * 0.1)
    return {"status": "timeout", "code": code, "error": "Отчёт не успел сформироваться"}


def sync_finance(ozon_post: Callable, period_days: int = 7, with_compensation: bool = True) -> dict:
    if not _lock.acquire(blocking=False):
        FINANCE_CACHE["syncing"] = True
        return {"ok": False, "syncing": True}
    FINANCE_CACHE["syncing"] = True
    FINANCE_CACHE["error"] = None
    try:
        period_days = max(1, min(int(period_days or 7), 31))
        types = []
        try:
            types = fetch_accrual_types(ozon_post)
        except Exception as e:
            logger.warning("accrual types: %s", e)
        type_by_id = {t["id"]: t for t in types}
        coinvest_ids = {t["id"] for t in types if t.get("is_coinvest")}

        today = datetime.now(timezone(timedelta(hours=3))).date()
        day_rows = []
        fee_agg: dict[int, float] = {}
        coinvest_total = 0.0
        grand_total = 0.0

        for i in range(period_days):
            d = today - timedelta(days=i)
            try:
                accs = fetch_accruals_day(ozon_post, d)
            except Exception as e:
                logger.warning("accrual %s: %s", d, e)
                day_rows.append({"date": d.isoformat(), "total": 0, "coinvest": 0, "count": 0, "error": str(e)})
                continue
            day_total = 0.0
            day_coin = 0.0
            for acc in accs:
                for fee in _extract_fees(acc):
                    amt = fee["amount"]
                    tid = fee["type_id"]
                    day_total += amt
                    fee_agg[tid] = fee_agg.get(tid, 0.0) + amt
                    if tid in coinvest_ids:
                        day_coin += amt
                        coinvest_total += amt
            grand_total += day_total
            day_rows.append({
                "date": d.isoformat(),
                "total": round(day_total, 2),
                "coinvest": round(day_coin, 2),
                "count": len(accs),
            })
            time.sleep(0.05)

        by_type = []
        for tid, amt in sorted(fee_agg.items(), key=lambda x: -abs(x[1])):
            meta = type_by_id.get(tid) or {"id": tid, "name": f"type:{tid}", "description": "", "is_coinvest": tid in coinvest_ids}
            by_type.append({
                **meta,
                "amount": round(amt, 2),
            })

        compensation = None
        if with_compensation:
            ym = today.strftime("%Y-%m")
            try:
                req = request_compensation_report(ozon_post, ym)
                if req.get("code"):
                    compensation = poll_report(ozon_post, req["code"])
                    compensation["period"] = ym
                else:
                    compensation = {"status": "empty", "period": ym, "error": "Нет code в ответе"}
            except Exception as e:
                compensation = {"status": "error", "period": ym, "error": str(e)}

        FINANCE_CACHE.update({
            "days": day_rows,
            "types": types,
            "coinvest_types": [t for t in types if t.get("is_coinvest")],
            "by_type": by_type[:80],
            "coinvest_total": round(coinvest_total, 2),
            "total": round(grand_total, 2),
            "compensation": compensation,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "syncing": False,
            "error": None,
            "period_days": period_days,
        })
        return {"ok": True, "coinvest_total": round(coinvest_total, 2)}
    except Exception as e:
        logger.exception("finance sync")
        FINANCE_CACHE["error"] = str(e)
        FINANCE_CACHE["syncing"] = False
        return {"ok": False, "error": str(e)}
    finally:
        _lock.release()


def get_cached() -> dict:
    return {
        "days": FINANCE_CACHE.get("days") or [],
        "types": FINANCE_CACHE.get("types") or [],
        "coinvest_types": FINANCE_CACHE.get("coinvest_types") or [],
        "by_type": FINANCE_CACHE.get("by_type") or [],
        "coinvest_total": FINANCE_CACHE.get("coinvest_total") or 0,
        "total": FINANCE_CACHE.get("total") or 0,
        "compensation": FINANCE_CACHE.get("compensation"),
        "updated_at": FINANCE_CACHE.get("updated_at"),
        "syncing": bool(FINANCE_CACHE.get("syncing")),
        "error": FINANCE_CACHE.get("error"),
        "period_days": FINANCE_CACHE.get("period_days") or 7,
    }
