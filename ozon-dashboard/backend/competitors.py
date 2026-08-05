"""Конкуренты — отчёт Ozon «Конкурентная позиция» (Excel)."""

from __future__ import annotations

import io
import json
import logging
import re
import zipfile
from datetime import datetime, timezone
from typing import Callable
from xml.etree import ElementTree as ET

logger = logging.getLogger("ozon-dashboard.competitors")

SETTING_KEY = "ozon_competitor_position"
NS = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}

COMP_CACHE: dict = {
    "period": None,
    "category": None,
    "uploaded_at": None,
    "filename": None,
    "rows": [],
    "error": None,
}


def _col_index(ref: str) -> int:
    letters = re.match(r"([A-Z]+)", ref or "A")
    if not letters:
        return 0
    n = 0
    for ch in letters.group(1):
        n = n * 26 + (ord(ch) - 64)
    return n - 1


def _shared_strings(z: zipfile.ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in z.namelist():
        return []
    root = ET.fromstring(z.read("xl/sharedStrings.xml"))
    out = []
    for si in root.findall("m:si", NS):
        texts = [t.text or "" for t in si.iter("{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t")]
        out.append("".join(texts))
    return out


def _sheet_rows(z: zipfile.ZipFile) -> list[list]:
    """Читает первый лист xlsx без openpyxl (Ozon-файлы часто ломают styles)."""
    ss = _shared_strings(z)
    wb = ET.fromstring(z.read("xl/workbook.xml"))
    sheets = wb.findall("m:sheets/m:sheet", NS)
    if not sheets:
        raise ValueError("в файле нет листов")
    rid = sheets[0].attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
    rels = ET.fromstring(z.read("xl/_rels/workbook.xml.rels"))
    target = None
    for rel in rels:
        if rel.attrib.get("Id") == rid:
            target = rel.attrib.get("Target")
            break
    if not target:
        target = "worksheets/sheet1.xml"
    path = "xl/" + target.lstrip("/")
    if path.startswith("xl/xl/"):
        path = path[3:]
    root = ET.fromstring(z.read(path))

    def cell_val(c):
        t = c.attrib.get("t")
        v = c.find("m:v", NS)
        if v is None or v.text is None:
            is_el = c.find("m:is", NS)
            if is_el is not None:
                texts = [t.text or "" for t in is_el.iter("{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t")]
                return "".join(texts)
            return None
        if t == "s":
            try:
                return ss[int(v.text)]
            except Exception:
                return v.text
        return v.text

    rows = []
    for row in root.findall("m:sheetData/m:row", NS):
        cells = {}
        max_i = -1
        for c in row.findall("m:c", NS):
            i = _col_index(c.attrib.get("r", "A"))
            cells[i] = cell_val(c)
            max_i = max(max_i, i)
        vals = [cells.get(i) for i in range(max_i + 1)] if max_i >= 0 else []
        rows.append(vals)
    return rows


def _to_float(v) -> float:
    if v is None or v == "":
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).replace("\xa0", "").replace(" ", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0


def _to_int(v) -> int:
    return int(round(_to_float(v)))


def parse_competitor_xlsx(content: bytes, filename: str = "") -> dict:
    """Парсит Excel «Конкурентная позиция» из кабинета Ozon."""
    try:
        z = zipfile.ZipFile(io.BytesIO(content))
    except zipfile.BadZipFile as e:
        raise ValueError("это не xlsx-файл") from e

    rows_raw = _sheet_rows(z)
    if not rows_raw:
        raise ValueError("пустой файл")

    period = None
    category = None
    header_idx = None
    for i, row in enumerate(rows_raw):
        if not row:
            continue
        a0 = str(row[0] or "").strip()
        if a0.lower().startswith("период"):
            period = a0.split(":", 1)[-1].strip() if ":" in a0 else a0
        elif a0.lower().startswith("категория"):
            category = a0.split(":", 1)[-1].strip() if ":" in a0 else a0
        elif a0 in ("№", "N", "No", "#") or (len(row) >= 6 and "артикул" in " ".join(str(x or "").lower() for x in row)):
            # заголовок таблицы
            header_idx = i
            break

    if header_idx is None:
        raise ValueError("не нашёл таблицу (ожидался отчёт «Конкурентная позиция»)")

    header = [str(h or "").strip().lower() for h in rows_raw[header_idx]]

    def find_col(*names):
        for n in names:
            for i, h in enumerate(header):
                if n in h:
                    return i
        return None

    c_rank = find_col("№", "no", "#")
    c_comp = find_col("назван конкурент", "конкурент")
    c_name = find_col("назван товар", "товар")
    c_url = find_col("ссылка")
    c_offer = find_col("артикул продавца")
    c_sku = find_col("артикул ozon", "sku")
    c_cat2 = find_col("категория 2")
    c_cat3 = find_col("категория 3")
    c_sum = find_col("заказано на сумму", "сумм")
    c_qty = find_col("заказано товар", "заказано")

    if c_sku is None and c_offer is None:
        raise ValueError("нет колонок артикулов — это другой отчёт?")

    items = []
    for row in rows_raw[header_idx + 1:]:
        if not row or all(v is None or str(v).strip() == "" for v in row):
            continue

        def g(idx):
            if idx is None or idx >= len(row):
                return None
            return row[idx]

        sku = str(g(c_sku) or "").strip()
        offer = str(g(c_offer) or "").strip()
        name = str(g(c_name) or "").strip()
        if not sku and not offer and not name:
            continue
        url = str(g(c_url) or "").strip()
        if not url and sku:
            url = f"https://www.ozon.ru/product/{sku}/"
        items.append({
            "rank": _to_int(g(c_rank)) if g(c_rank) not in (None, "") else len(items) + 1,
            "competitor": str(g(c_comp) or "").strip(),
            "name": name,
            "url": url,
            "offer_id": offer,
            "sku": sku,
            "cat2": str(g(c_cat2) or "").strip(),
            "cat3": str(g(c_cat3) or "").strip(),
            "ordered_sum": _to_float(g(c_sum)),
            "ordered_qty": _to_int(g(c_qty)),
            "is_own": False,
        })

    if not items:
        raise ValueError("в таблице нет строк товаров")

    items.sort(key=lambda x: (-x["ordered_sum"], -x["ordered_qty"], x["rank"]))
    for i, it in enumerate(items, 1):
        it["rank"] = i

    return {
        "period": period,
        "category": category,
        "uploaded_at": datetime.now(timezone.utc).isoformat(),
        "filename": filename or "",
        "rows": items,
        "count": len(items),
    }


def mark_own_rows(payload: dict, products: list[dict] | None) -> dict:
    """Помечает свои товары по sku / offer_id из каталога."""
    own_sku = set()
    own_offer = set()
    for p in products or []:
        if p.get("sku") is not None:
            own_sku.add(str(p["sku"]))
        if p.get("offer_id"):
            own_offer.add(str(p["offer_id"]))
    for r in payload.get("rows") or []:
        r["is_own"] = (str(r.get("sku") or "") in own_sku) or (str(r.get("offer_id") or "") in own_offer)
    return payload


def save_report(save_setting: Callable, payload: dict) -> dict:
    save_setting(SETTING_KEY, {
        "period": payload.get("period"),
        "category": payload.get("category"),
        "uploaded_at": payload.get("uploaded_at"),
        "filename": payload.get("filename"),
        "rows": payload.get("rows") or [],
    })
    COMP_CACHE.clear()
    COMP_CACHE.update({
        "period": payload.get("period"),
        "category": payload.get("category"),
        "uploaded_at": payload.get("uploaded_at"),
        "filename": payload.get("filename"),
        "rows": payload.get("rows") or [],
        "error": None,
    })
    return get_cached()


def load_report(get_setting: Callable) -> dict:
    raw = get_setting(SETTING_KEY, None)
    if not raw:
        COMP_CACHE.update({"rows": [], "period": None, "category": None, "uploaded_at": None, "filename": None, "error": None})
        return get_cached()
    try:
        data = json.loads(raw) if isinstance(raw, str) else (raw or {})
    except Exception:
        data = {}
    COMP_CACHE.update({
        "period": data.get("period"),
        "category": data.get("category"),
        "uploaded_at": data.get("uploaded_at"),
        "filename": data.get("filename"),
        "rows": data.get("rows") or [],
        "error": None,
    })
    return get_cached()


def get_cached() -> dict:
    rows = COMP_CACHE.get("rows") or []
    total_sum = sum(float(r.get("ordered_sum") or 0) for r in rows)
    total_qty = sum(int(r.get("ordered_qty") or 0) for r in rows)
    own_n = sum(1 for r in rows if r.get("is_own"))
    brands: dict[str, float] = {}
    for r in rows:
        b = r.get("competitor") or "—"
        brands[b] = brands.get(b, 0) + float(r.get("ordered_sum") or 0)
    top_brands = sorted(
        [{"name": k, "ordered_sum": v} for k, v in brands.items()],
        key=lambda x: -x["ordered_sum"],
    )[:10]
    return {
        "period": COMP_CACHE.get("period"),
        "category": COMP_CACHE.get("category"),
        "uploaded_at": COMP_CACHE.get("uploaded_at"),
        "filename": COMP_CACHE.get("filename"),
        "rows": rows,
        "count": len(rows),
        "total_sum": total_sum,
        "total_qty": total_qty,
        "own_count": own_n,
        "top_brands": top_brands,
        "error": COMP_CACHE.get("error"),
    }
