"""Остатки нашего физического склада — та же Google Sheets, что у WB-дашборда.

Матчинг к Ozon: артикул продавца (vendor_code) ↔ offer_id.
"""

from __future__ import annotations

import csv
import io
import logging
import os
import re
import threading
from datetime import datetime, timezone
from typing import Any

import httpx

logger = logging.getLogger("ozon-dashboard.own-warehouse")

OWN_WAREHOUSE_SHEET_ID = os.getenv(
    "OWN_WAREHOUSE_SHEET_ID",
    "1Lhoy4s_KX0pWndsd3Y5oCOjTFCtfEfVUM4AgtBv4Crc",
).strip()
OWN_WAREHOUSE_GID = os.getenv("OWN_WAREHOUSE_GID", "1829622647").strip()

OWN_WAREHOUSE_CACHE: dict[str, Any] = {
    "title": None,
    "as_of": None,
    "rows": [],
    "by_vendor": {},
    "updated_at": None,
    "error": None,
    "syncing": False,
    "configured": bool(OWN_WAREHOUSE_SHEET_ID),
}

_lock = threading.Lock()


def _parse_int_cell(v):
    s = str(v or "").strip().replace("\xa0", "").replace(" ", "").replace(",", ".")
    if not s or s.lower() in ("nan", "none", "-"):
        return None
    try:
        return int(float(s))
    except Exception:
        return None


def fetch_own_warehouse_stock() -> dict:
    """CSV из Google Sheets «Остатки на складе» (1-я таблица до ИТОГО)."""
    if not OWN_WAREHOUSE_SHEET_ID:
        raise RuntimeError("OWN_WAREHOUSE_SHEET_ID не задан")

    gid = OWN_WAREHOUSE_GID or "0"
    url = (
        f"https://docs.google.com/spreadsheets/d/{OWN_WAREHOUSE_SHEET_ID}"
        f"/export?format=csv&gid={gid}"
    )
    resp = httpx.get(url, timeout=30, follow_redirects=True)
    if not resp.is_success:
        raise RuntimeError(f"Google Sheets HTTP {resp.status_code}")
    text = resp.text
    if not text.strip() or text.lstrip().startswith("<!"):
        raise RuntimeError("Таблица недоступна (нужен доступ «все, у кого есть ссылка»)")

    rows_raw = list(csv.reader(io.StringIO(text)))
    if len(rows_raw) < 2:
        raise RuntimeError("Пустая таблица")

    title = (rows_raw[0][0] if rows_raw[0] else "").strip()
    as_of = None
    m = re.search(r"(\d{1,2})[./](\d{1,2})[./](\d{2,4})", title)
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

    personal: dict[str, int] = {}
    for row in raw_rows:
        vc = row["vendor_code"]
        if not vc:
            continue
        personal[vc] = personal.get(vc, 0) + (row["stock"] or 0)

    # семьи: основной + следующие «голые» артикулы
    families = []
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

    parent: dict[str, str] = {}

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
        for mem in fam["members"]:
            parent.setdefault(mem, mem)
            union(root, mem)

    root_members: dict[str, list[str]] = {}
    for vc in personal:
        parent.setdefault(vc, vc)
        r = find(vc)
        root_members.setdefault(r, [])
        if vc not in root_members[r]:
            root_members[r].append(vc)
    for fam in families:
        for mem in fam["members"]:
            parent.setdefault(mem, mem)
            r = find(mem)
            root_members.setdefault(r, [])
            if mem not in root_members[r]:
                root_members[r].append(mem)

    by_vendor: dict[str, dict] = {}
    for root, members in root_members.items():
        fam_stock = sum(personal.get(m, 0) for m in members)
        for mem in members:
            by_vendor[mem] = {
                "stock": personal.get(mem, 0),
                "family_stock": fam_stock,
                "family": list(members),
                "root": root,
            }

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
            "stock": meta.get("stock", row["stock"] or 0),
            "family_stock": meta.get("family_stock", row["stock"] or 0),
            "family": meta.get("family", [vc] if vc else []),
            "root": meta.get("root"),
            "note": row["note"],
        })

    return {
        "title": title or "Остатки на складе",
        "as_of": as_of,
        "rows": out,
        "by_vendor": by_vendor,
        "updated_at": datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M"),
        "error": None,
        "configured": True,
        "sheet_id": OWN_WAREHOUSE_SHEET_ID,
    }


def refresh_own_warehouse_stock() -> dict:
    if not _lock.acquire(blocking=False):
        OWN_WAREHOUSE_CACHE["syncing"] = True
        return {**OWN_WAREHOUSE_CACHE, "syncing": True}
    OWN_WAREHOUSE_CACHE["syncing"] = True
    OWN_WAREHOUSE_CACHE["error"] = None
    try:
        data = fetch_own_warehouse_stock()
        OWN_WAREHOUSE_CACHE.update(data)
        OWN_WAREHOUSE_CACHE["syncing"] = False
        logger.info(
            "own-warehouse: %s rows, as_of=%s",
            len(data.get("rows") or []),
            data.get("as_of"),
        )
        return dict(OWN_WAREHOUSE_CACHE)
    except Exception as e:
        logger.exception("own-warehouse refresh")
        OWN_WAREHOUSE_CACHE["syncing"] = False
        OWN_WAREHOUSE_CACHE["error"] = str(e)
        return dict(OWN_WAREHOUSE_CACHE)
    finally:
        _lock.release()


def get_cached(refresh: bool = False) -> dict:
    if refresh or not OWN_WAREHOUSE_CACHE.get("rows"):
        if OWN_WAREHOUSE_CACHE.get("syncing"):
            return {**OWN_WAREHOUSE_CACHE, "syncing": True}
        return refresh_own_warehouse_stock()
    return {
        "title": OWN_WAREHOUSE_CACHE.get("title"),
        "as_of": OWN_WAREHOUSE_CACHE.get("as_of"),
        "rows": OWN_WAREHOUSE_CACHE.get("rows") or [],
        "by_vendor": OWN_WAREHOUSE_CACHE.get("by_vendor") or {},
        "updated_at": OWN_WAREHOUSE_CACHE.get("updated_at"),
        "error": OWN_WAREHOUSE_CACHE.get("error"),
        "syncing": bool(OWN_WAREHOUSE_CACHE.get("syncing")),
        "configured": bool(OWN_WAREHOUSE_SHEET_ID),
    }


def lookup_for_offer(offer_id: str) -> dict | None:
    """Остаток нашего склада по артикулу Ozon (offer_id = vendor_code в таблице)."""
    vc = str(offer_id or "").strip()
    if not vc:
        return None
    by_v = OWN_WAREHOUSE_CACHE.get("by_vendor") or {}
    hit = by_v.get(vc)
    if hit:
        return hit
    # мягкий матч без регистра
    low = vc.lower()
    for k, v in by_v.items():
        if str(k).lower() == low:
            return v
    return None
