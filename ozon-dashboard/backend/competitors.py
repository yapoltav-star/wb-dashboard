"""Конкуренты Ozon — Excel-отчёты в общей базе (Supabase settings).

Типы:
1. competitive_position — «Конкурентная позиция» (товары категории)
2. brands — сводка по брендам (лист «Бренды»)
3. brand_detail — детальный отчёт по одному бренду (воронка/реклама)
"""

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

KEY_POSITION = "ozon_competitor_position"
KEY_BRANDS = "ozon_competitor_brands"
KEY_BRAND_DETAILS = "ozon_competitor_brand_details"
KEY_HIDDEN_BRANDS = "ozon_competitor_hidden_brands"
NS = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}

CACHE: dict = {
    "position": None,
    "brands": None,
    "brand_details": {},  # brand -> report
    "hidden_brands": [],  # list of brand names (exact casing from hide action)
    "loaded": False,
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def _sheet_rows(z: zipfile.ZipFile, sheet_index: int = 0) -> list[list]:
    ss = _shared_strings(z)
    wb = ET.fromstring(z.read("xl/workbook.xml"))
    sheets = wb.findall("m:sheets/m:sheet", NS)
    if not sheets:
        raise ValueError("в файле нет листов")
    if sheet_index >= len(sheets):
        sheet_index = 0
    rid = sheets[sheet_index].attrib.get(
        "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"
    )
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
        is_el = c.find("m:is", NS)
        if is_el is not None:
            texts = [t.text or "" for t in is_el.iter("{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t")]
            return "".join(texts)
        if v is None or v.text is None:
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


def _sheet_names(z: zipfile.ZipFile) -> list[str]:
    wb = ET.fromstring(z.read("xl/workbook.xml"))
    return [sh.attrib.get("name") or "" for sh in wb.findall("m:sheets/m:sheet", NS)]


def _to_float(v) -> float:
    if v is None or v == "":
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace("\xa0", "").replace(" ", "").replace("%", "").replace(",", ".")
    if s.lower() in ("нет данных", "—", "-", "n/a"):
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def _to_int(v) -> int:
    return int(round(_to_float(v)))


def _g(row, idx):
    if idx is None or idx >= len(row):
        return None
    return row[idx]


def _find_col(header: list[str], *names: str) -> int | None:
    for n in names:
        n = n.lower()
        for i, h in enumerate(header):
            if n in h:
                return i
    return None


def _meta_from_rows(rows: list[list]) -> dict:
    meta = {"period": None, "category": None, "brand": None, "price_filter": None, "sort": None}
    for row in rows[:8]:
        if not row:
            continue
        a0 = str(row[0] or "").strip()
        b0 = str(row[1] or "").strip() if len(row) > 1 else ""
        low = a0.lower()
        if low.startswith("период"):
            meta["period"] = (b0 or (a0.split(":", 1)[-1].strip() if ":" in a0 else a0)) or None
        elif "категория" in low:
            meta["category"] = b0 or (a0.split(":", 1)[-1].strip() if ":" in a0 else None)
        elif low.startswith("бренд"):
            meta["brand"] = b0 or (a0.split(":", 1)[-1].strip() if ":" in a0 else None)
        elif low.startswith("цена"):
            meta["price_filter"] = b0 or a0
        elif low.startswith("сортировка"):
            meta["sort"] = b0 or a0
        elif low.startswith("дата формирования"):
            meta["formed_at"] = b0 or None
    return meta


# ── parsers ──────────────────────────────────────────────────────────────

def parse_competitive_position(rows: list[list], filename: str = "") -> dict:
    meta = _meta_from_rows(rows)
    header_idx = None
    for i, row in enumerate(rows):
        if not row:
            continue
        joined = " ".join(str(x or "").lower() for x in row)
        a0 = str(row[0] or "").strip()
        if a0 in ("№", "N", "#") or ("артикул ozon" in joined and "заказано" in joined):
            header_idx = i
            break
    if header_idx is None:
        raise ValueError("не таблица «Конкурентная позиция»")

    header = [str(h or "").strip().lower() for h in rows[header_idx]]
    c_rank = _find_col(header, "№", "#")
    c_comp = _find_col(header, "конкурент")
    c_name = _find_col(header, "название товара", "товар")
    c_url = _find_col(header, "ссылка")
    c_offer = _find_col(header, "артикул продавца")
    c_sku = _find_col(header, "артикул ozon")
    c_cat2 = _find_col(header, "категория 2")
    c_cat3 = _find_col(header, "категория 3")
    c_sum = _find_col(header, "заказано на сумму", "сумм")
    c_qty = _find_col(header, "заказано товар")

    items = []
    for row in rows[header_idx + 1:]:
        if not row or all(v is None or str(v).strip() == "" for v in row):
            continue
        sku = str(_g(row, c_sku) or "").strip()
        offer = str(_g(row, c_offer) or "").strip()
        name = str(_g(row, c_name) or "").strip()
        if not sku and not offer and not name:
            continue
        url = str(_g(row, c_url) or "").strip()
        if not url and sku:
            url = f"https://www.ozon.ru/product/{sku}/"
        items.append({
            "rank": _to_int(_g(row, c_rank)) if _g(row, c_rank) not in (None, "") else len(items) + 1,
            "competitor": str(_g(row, c_comp) or "").strip(),
            "name": name,
            "url": url,
            "offer_id": offer,
            "sku": sku,
            "cat2": str(_g(row, c_cat2) or "").strip(),
            "cat3": str(_g(row, c_cat3) or "").strip(),
            "ordered_sum": _to_float(_g(row, c_sum)),
            "ordered_qty": _to_int(_g(row, c_qty)),
            "is_own": False,
        })
    if not items:
        raise ValueError("пустая таблица конкурентной позиции")
    items.sort(key=lambda x: (-x["ordered_sum"], -x["ordered_qty"]))
    for i, it in enumerate(items, 1):
        it["rank"] = i
    return {
        "type": "position",
        "period": meta.get("period"),
        "category": meta.get("category"),
        "uploaded_at": _now_iso(),
        "filename": filename or "",
        "rows": items,
        "count": len(items),
    }


def parse_brands_report(rows: list[list], filename: str = "") -> dict:
    meta = _meta_from_rows(rows)
    header_idx = None
    for i, row in enumerate(rows):
        if not row:
            continue
        joined = " ".join(str(x or "").lower() for x in row)
        if "бренд" in joined and ("заказано на сумму" in joined or "доля в категории" in joined):
            header_idx = i
            break
    if header_idx is None:
        raise ValueError("не сводка по брендам")

    header = [str(h or "").strip().lower() for h in rows[header_idx]]
    # skip description row if present
    start = header_idx + 1
    if start < len(rows):
        desc = " ".join(str(x or "").lower() for x in (rows[start] or []))
        if "данные по брендам" in desc or "суммарная стоимость" in desc:
            start += 1

    c_brand = _find_col(header, "бренд")
    c_sum = _find_col(header, "заказано на сумму")
    c_dyn = _find_col(header, "динамика")
    c_qty = _find_col(header, "заказано товар")
    c_price = _find_col(header, "средняя цена")
    c_sellers = _find_col(header, "число продавцов", "продавцов")
    c_clusters = _find_col(header, "число кластеров", "кластер")
    c_buyout = _find_col(header, "доля выкупа", "выкуп")
    c_share = _find_col(header, "доля в категории")

    items = []
    for row in rows[start:]:
        if not row:
            continue
        brand = str(_g(row, c_brand) or "").strip()
        if not brand or brand.lower().startswith("данн"):
            # sometimes brand empty but sum present — skip empty brand names that are None
            if _g(row, c_brand) is None and _to_float(_g(row, c_sum)) > 0:
                brand = "—"
            else:
                continue
        items.append({
            "brand": brand,
            "ordered_sum": _to_float(_g(row, c_sum)),
            "dynamics": _to_float(_g(row, c_dyn)),
            "ordered_qty": _to_int(_g(row, c_qty)),
            "avg_price": _to_float(_g(row, c_price)),
            "sellers": _to_int(_g(row, c_sellers)),
            "clusters": _to_int(_g(row, c_clusters)),
            "buyout_share": _to_float(_g(row, c_buyout)),
            "category_share": _to_float(_g(row, c_share)),
            "has_detail": False,
        })
    if not items:
        raise ValueError("пустая таблица брендов")
    items.sort(key=lambda x: -x["ordered_sum"])
    for i, it in enumerate(items, 1):
        it["rank"] = i
    return {
        "type": "brands",
        "period": meta.get("period"),
        "category": meta.get("category"),
        "price_filter": meta.get("price_filter"),
        "sort": meta.get("sort"),
        "uploaded_at": _now_iso(),
        "filename": filename or "",
        "rows": items,
        "count": len(items),
    }


def parse_brand_detail(rows: list[list], filename: str = "") -> dict:
    meta = _meta_from_rows(rows)
    header_idx = None
    for i, row in enumerate(rows):
        if not row:
            continue
        joined = " ".join(str(x or "").lower() for x in row)
        if "название товара" in joined and "заказано на сумму" in joined and ("показы" in joined or "продавец" in joined):
            header_idx = i
            break
    if header_idx is None:
        raise ValueError("не детальный отчёт по бренду")

    header = [str(h or "").strip().lower() for h in rows[header_idx]]
    cols = {
        "name": _find_col(header, "название товара"),
        "url": _find_col(header, "ссылка"),
        "seller": _find_col(header, "продавец"),
        "brand": _find_col(header, "бренд"),
        "flag": _find_col(header, "признак"),
        "ordered_sum": _find_col(header, "заказано на сумму"),
        "dynamics": _find_col(header, "динамика оборота", "динамика"),
        "ordered_qty": _find_col(header, "заказано, штуки", "заказано, штук"),
        "avg_price": _find_col(header, "средняя цена"),
        "min_price": _find_col(header, "минимальная цена"),
        "buyout": _find_col(header, "доля выкупа"),
        "lost_sales": _find_col(header, "упущенные продажи"),
        "days_oos": _find_col(header, "дней без остатка"),
        "avg_daily_sum": _find_col(header, "среднесуточные продажи, ₽"),
        "avg_daily_qty": _find_col(header, "среднесуточные продажи, штуки"),
        "stock": _find_col(header, "остаток на конец"),
        "scheme": _find_col(header, "схема работы"),
        "volume": _find_col(header, "объем товара"),
        "shows": _find_col(header, "показы всего", "показы"),
        "search_views": _find_col(header, "просмотры в поиске"),
        "card_views": _find_col(header, "просмотры карточки"),
        "cr_order": _find_col(header, "конверсия из показа в заказ"),
        "cart_search": _find_col(header, "в корзину из поиска"),
        "cart_card": _find_col(header, "в корзину из карточки"),
        "promo_discount": _find_col(header, "скидка за счет акций"),
        "promo_share": _find_col(header, "доля суммы заказов по акциям"),
        "days_promo": _find_col(header, "дней в акциях"),
        "days_ads": _find_col(header, "дней с продвижением"),
        "ads_share": _find_col(header, "доля рекламных расходов"),
        "created": _find_col(header, "дата создания"),
    }

    averages = None
    items = []
    brand_guess = meta.get("brand")

    for row in rows[header_idx + 1:]:
        if not row:
            continue
        name = str(_g(row, cols["name"]) or "").strip()
        if not name:
            continue
        url = str(_g(row, cols["url"]) or "").strip()
        sku = ""
        m = re.search(r"/product/(\d+)", url)
        if m:
            sku = m.group(1)
        rec = {
            "name": name,
            "url": url,
            "sku": sku,
            "seller": str(_g(row, cols["seller"]) or "").strip(),
            "brand": str(_g(row, cols["brand"]) or "").strip() or brand_guess or "",
            "flag": str(_g(row, cols["flag"]) or "").strip(),
            "ordered_sum": _to_float(_g(row, cols["ordered_sum"])),
            "dynamics_raw": str(_g(row, cols["dynamics"]) or "").strip(),
            "dynamics": _to_float(_g(row, cols["dynamics"])),
            "ordered_qty": _to_int(_g(row, cols["ordered_qty"])),
            "avg_price": _to_float(_g(row, cols["avg_price"])),
            "min_price": _to_float(_g(row, cols["min_price"])),
            "buyout": _to_float(_g(row, cols["buyout"])),
            "lost_sales": _to_float(_g(row, cols["lost_sales"])),
            "days_oos": str(_g(row, cols["days_oos"]) or "").strip(),
            "avg_daily_sum": _to_float(_g(row, cols["avg_daily_sum"])),
            "avg_daily_qty": _to_float(_g(row, cols["avg_daily_qty"])),
            "stock": _to_int(_g(row, cols["stock"])),
            "scheme": str(_g(row, cols["scheme"]) or "").strip(),
            "volume": _to_float(_g(row, cols["volume"])),
            "shows": _to_int(_g(row, cols["shows"])),
            "search_views": _to_int(_g(row, cols["search_views"])),
            "card_views": _to_int(_g(row, cols["card_views"])),
            "cr_order": _to_float(_g(row, cols["cr_order"])),
            "cart_search": _to_float(_g(row, cols["cart_search"])),
            "cart_card": _to_float(_g(row, cols["cart_card"])),
            "promo_discount": _to_float(_g(row, cols["promo_discount"])),
            "promo_share": _to_float(_g(row, cols["promo_share"])),
            "days_promo": str(_g(row, cols["days_promo"]) or "").strip(),
            "days_ads": str(_g(row, cols["days_ads"]) or "").strip(),
            "ads_share": _to_float(_g(row, cols["ads_share"])),
            "created": str(_g(row, cols["created"]) or "").strip(),
            "is_average": name.lower().startswith("среднее"),
        }
        if not brand_guess and rec["brand"]:
            brand_guess = rec["brand"]
        if rec["is_average"]:
            averages = rec
            continue
        items.append(rec)

    if not items:
        raise ValueError("в отчёте бренда нет товаров")
    items.sort(key=lambda x: -x["ordered_sum"])
    for i, it in enumerate(items, 1):
        it["rank"] = i

    brand = (brand_guess or meta.get("brand") or "Без бренда").strip()
    return {
        "type": "brand_detail",
        "brand": brand,
        "period": meta.get("period"),
        "category": meta.get("category"),
        "formed_at": meta.get("formed_at"),
        "uploaded_at": _now_iso(),
        "filename": filename or "",
        "averages": averages,
        "rows": items,
        "count": len(items),
    }


def detect_and_parse(content: bytes, filename: str = "") -> dict:
    try:
        z = zipfile.ZipFile(io.BytesIO(content))
    except zipfile.BadZipFile as e:
        raise ValueError("это не xlsx-файл") from e

    names = _sheet_names(z)
    # предпочитаем лист «Бренды» / «Конкурентная позиция», иначе первый
    sheet_idx = 0
    for i, n in enumerate(names):
        low = (n or "").strip().lower()
        if low in ("бренды", "бренд") or low.startswith("конкурент"):
            sheet_idx = i
            break
    rows = _sheet_rows(z, sheet_idx)
    if not rows:
        raise ValueError("пустой файл")

    joined_head = " ".join(
        " ".join(str(c or "") for c in (r or [])).lower()
        for r in rows[:12]
    )
    errors: list[str] = []

    def try_parse(fn, label):
        try:
            return fn(rows, filename)
        except Exception as e:
            errors.append(f"{label}: {e}")
            return None

    # 1) сводка брендов
    if any((n or "").strip().lower() in ("бренды", "бренд") for n in names) or (
        "доля в категории" in joined_head and ("число продавцов" in joined_head or "бренд" in joined_head)
        and "название товара" not in joined_head
    ):
        got = try_parse(parse_brands_report, "brands")
        if got:
            return got

    # 2) детальный отчёт бренда (воронка / показы)
    if "название товара" in joined_head and (
        "показы" in joined_head or "доля рекламных" in joined_head or "просмотры карточки" in joined_head
    ):
        got = try_parse(parse_brand_detail, "brand_detail")
        if got:
            return got

    # 3) конкурентная позиция
    if "конкурент" in joined_head or ("артикул ozon" in joined_head and "заказано" in joined_head):
        got = try_parse(parse_competitive_position, "position")
        if got:
            return got

    for fn, label in (
        (parse_brand_detail, "brand_detail"),
        (parse_brands_report, "brands"),
        (parse_competitive_position, "position"),
    ):
        got = try_parse(fn, label)
        if got:
            return got
    raise ValueError("неизвестный формат Excel. " + "; ".join(errors[:3]))


# ── storage ──────────────────────────────────────────────────────────────

def _load_json(get_setting: Callable, key: str, default):
    raw = get_setting(key, None)
    if raw is None or raw == "":
        return default
    try:
        data = json.loads(raw) if isinstance(raw, str) else raw
        return data if data is not None else default
    except Exception:
        return default


def mark_own_rows(rows: list[dict], products: list[dict] | None) -> None:
    own_sku = {str(p["sku"]) for p in (products or []) if p.get("sku") is not None}
    own_offer = {str(p["offer_id"]) for p in (products or []) if p.get("offer_id")}
    for r in rows or []:
        r["is_own"] = (str(r.get("sku") or "") in own_sku) or (str(r.get("offer_id") or "") in own_offer)


def _normalize_hidden(raw) -> list[str]:
    if not raw:
        return []
    if isinstance(raw, dict):
        raw = raw.get("brands") or raw.get("hidden") or []
    if not isinstance(raw, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for b in raw:
        name = str(b or "").strip()
        if not name:
            continue
        key = name.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(name)
    return out


def _hidden_set(hidden: list[str] | None) -> set[str]:
    return {str(b).lower() for b in (hidden or []) if b}


def load_all(get_setting: Callable, products: list[dict] | None = None) -> dict:
    position = _load_json(get_setting, KEY_POSITION, None)
    brands = _load_json(get_setting, KEY_BRANDS, None)
    details = _load_json(get_setting, KEY_BRAND_DETAILS, {}) or {}
    hidden = _normalize_hidden(_load_json(get_setting, KEY_HIDDEN_BRANDS, []))
    if not isinstance(details, dict):
        details = {}
    if position is not None and not (position.get("rows") or []):
        position = None
    if brands is not None and not (brands.get("rows") or []):
        brands = None

    if position and products is not None:
        mark_own_rows(position.get("rows") or [], products)
    hidden_l = _hidden_set(hidden)
    if brands:
        detail_keys = {k.lower() for k in details}
        for r in brands.get("rows") or []:
            b = str(r.get("brand") or "")
            r["has_detail"] = b.lower() in detail_keys
            r["is_hidden"] = b.lower() in hidden_l

    CACHE["position"] = position
    CACHE["brands"] = brands
    CACHE["brand_details"] = details
    CACHE["hidden_brands"] = hidden
    CACHE["loaded"] = True
    return summarize()


def summarize() -> dict:
    position = CACHE.get("position")
    brands = CACHE.get("brands")
    details = CACHE.get("brand_details") or {}
    hidden = list(CACHE.get("hidden_brands") or [])
    hidden_l = _hidden_set(hidden)

    pos_rows = (position or {}).get("rows") or []
    brand_rows = (brands or {}).get("rows") or []
    for r in brand_rows:
        r["is_hidden"] = str(r.get("brand") or "").lower() in hidden_l
    visible_rows = [r for r in brand_rows if not r.get("is_hidden")]

    return {
        "position": {
            "period": (position or {}).get("period"),
            "category": (position or {}).get("category"),
            "uploaded_at": (position or {}).get("uploaded_at"),
            "filename": (position or {}).get("filename"),
            "rows": pos_rows,
            "count": len(pos_rows),
            "total_sum": sum(float(r.get("ordered_sum") or 0) for r in pos_rows),
            "total_qty": sum(int(r.get("ordered_qty") or 0) for r in pos_rows),
            "own_count": sum(1 for r in pos_rows if r.get("is_own")),
        } if position else None,
        "brands": {
            "period": (brands or {}).get("period"),
            "category": (brands or {}).get("category"),
            "uploaded_at": (brands or {}).get("uploaded_at"),
            "filename": (brands or {}).get("filename"),
            "rows": brand_rows,
            "count": len(visible_rows),
            "total_count": len(brand_rows),
            "hidden_count": len(brand_rows) - len(visible_rows),
            "total_sum": sum(float(r.get("ordered_sum") or 0) for r in visible_rows),
            "with_detail": sum(1 for r in visible_rows if r.get("has_detail")),
        } if brands else None,
        "hidden_brands": hidden,
        "brand_details": [
            {
                "brand": d.get("brand"),
                "period": d.get("period"),
                "category": d.get("category"),
                "uploaded_at": d.get("uploaded_at"),
                "filename": d.get("filename"),
                "count": d.get("count") or len(d.get("rows") or []),
                "total_sum": sum(float(r.get("ordered_sum") or 0) for r in (d.get("rows") or [])),
                "is_hidden": str(d.get("brand") or "").lower() in hidden_l,
            }
            for d in sorted(details.values(), key=lambda x: -(sum(float(r.get("ordered_sum") or 0) for r in (x.get("rows") or []))))
        ],
        "brand_detail_map": details,
    }


def save_parsed(save_setting: Callable, get_setting: Callable, payload: dict, products: list[dict] | None = None) -> dict:
    typ = payload.get("type")
    if typ == "position":
        if products is not None:
            mark_own_rows(payload.get("rows") or [], products)
        save_setting(KEY_POSITION, {
            "period": payload.get("period"),
            "category": payload.get("category"),
            "uploaded_at": payload.get("uploaded_at"),
            "filename": payload.get("filename"),
            "rows": payload.get("rows") or [],
        })
        CACHE["position"] = payload
    elif typ == "brands":
        # preserve has_detail flags from existing details; hidden list stays as-is
        details = CACHE.get("brand_details") or _load_json(get_setting, KEY_BRAND_DETAILS, {}) or {}
        hidden = _normalize_hidden(
            CACHE.get("hidden_brands") or _load_json(get_setting, KEY_HIDDEN_BRANDS, [])
        )
        detail_keys = {k.lower() for k in details}
        hidden_l = _hidden_set(hidden)
        for r in payload.get("rows") or []:
            b = str(r.get("brand") or "")
            r["has_detail"] = b.lower() in detail_keys
            r["is_hidden"] = b.lower() in hidden_l
        save_setting(KEY_BRANDS, {
            "period": payload.get("period"),
            "category": payload.get("category"),
            "price_filter": payload.get("price_filter"),
            "sort": payload.get("sort"),
            "uploaded_at": payload.get("uploaded_at"),
            "filename": payload.get("filename"),
            "rows": payload.get("rows") or [],
        })
        CACHE["brands"] = payload
        CACHE["hidden_brands"] = hidden
    elif typ == "brand_detail":
        brand = payload.get("brand") or "Без бренда"
        details = CACHE.get("brand_details") or _load_json(get_setting, KEY_BRAND_DETAILS, {}) or {}
        if not isinstance(details, dict):
            details = {}
        # normalize key by exact brand name
        # remove old case-variants
        for k in list(details.keys()):
            if k.lower() == brand.lower() and k != brand:
                details.pop(k, None)
        details[brand] = payload
        save_setting(KEY_BRAND_DETAILS, details)
        CACHE["brand_details"] = details
        # update has_detail on brands list
        brands = CACHE.get("brands") or _load_json(get_setting, KEY_BRANDS, None)
        if brands:
            for r in brands.get("rows") or []:
                r["has_detail"] = str(r.get("brand") or "").lower() == brand.lower() or str(r.get("brand") or "").lower() in {x.lower() for x in details}
            save_setting(KEY_BRANDS, brands)
            CACHE["brands"] = brands
        if CACHE.get("hidden_brands") is None:
            CACHE["hidden_brands"] = _normalize_hidden(_load_json(get_setting, KEY_HIDDEN_BRANDS, []))
    else:
        raise ValueError(f"unknown type {typ}")
    CACHE["loaded"] = True
    if CACHE.get("hidden_brands") is None:
        CACHE["hidden_brands"] = _normalize_hidden(_load_json(get_setting, KEY_HIDDEN_BRANDS, []))
    return summarize()


def delete_report(save_setting: Callable, get_setting: Callable, kind: str, brand: str | None = None) -> dict:
    kind = (kind or "").strip().lower()
    if kind in ("position", "competitive_position", "competitors"):
        save_setting(KEY_POSITION, {})
        CACHE["position"] = None
    elif kind == "brands":
        save_setting(KEY_BRANDS, {})
        CACHE["brands"] = None
    elif kind in ("brand_detail", "brand"):
        if not brand:
            raise ValueError("нужен brand")
        details = CACHE.get("brand_details") or _load_json(get_setting, KEY_BRAND_DETAILS, {}) or {}
        for k in list(details.keys()):
            if k.lower() == brand.lower():
                details.pop(k, None)
        save_setting(KEY_BRAND_DETAILS, details)
        CACHE["brand_details"] = details
        brands = CACHE.get("brands") or _load_json(get_setting, KEY_BRANDS, None)
        if brands:
            for r in brands.get("rows") or []:
                r["has_detail"] = str(r.get("brand") or "").lower() in {x.lower() for x in details}
            save_setting(KEY_BRANDS, brands)
            CACHE["brands"] = brands
    else:
        raise ValueError("kind: position | brands | brand_detail")
    return summarize()


def set_brand_hidden(
    save_setting: Callable,
    get_setting: Callable,
    brand: str | None = None,
    *,
    hide: bool = True,
    brands: list[str] | None = None,
    clear_all: bool = False,
) -> dict:
    """Скрыть / показать бренды. Список в общей базе (все устройства)."""
    if not CACHE.get("loaded"):
        load_all(get_setting)

    if clear_all:
        hidden: list[str] = []
    else:
        hidden = _normalize_hidden(
            CACHE.get("hidden_brands") or _load_json(get_setting, KEY_HIDDEN_BRANDS, [])
        )
        names: list[str] = []
        if brands:
            names.extend(str(b).strip() for b in brands if str(b).strip())
        if brand and str(brand).strip():
            names.append(str(brand).strip())
        if not names:
            raise ValueError("нужен brand или brands")
        for name in names:
            key = name.lower()
            if hide:
                if key not in _hidden_set(hidden):
                    hidden.append(name)
            else:
                hidden = [b for b in hidden if b.lower() != key]

    save_setting(KEY_HIDDEN_BRANDS, hidden)
    CACHE["hidden_brands"] = hidden
    return summarize()


# backward-compatible aliases used by older main.py
def parse_competitor_xlsx(content: bytes, filename: str = "") -> dict:
    return detect_and_parse(content, filename)


def save_report(save_setting: Callable, payload: dict) -> dict:
    # legacy
    CACHE["position"] = payload
    save_setting(KEY_POSITION, {
        "period": payload.get("period"),
        "category": payload.get("category"),
        "uploaded_at": payload.get("uploaded_at"),
        "filename": payload.get("filename"),
        "rows": payload.get("rows") or [],
    })
    return summarize()["position"] or {}


def load_report(get_setting: Callable) -> dict:
    load_all(get_setting)
    return summarize()["position"] or {
        "period": None, "category": None, "uploaded_at": None,
        "filename": None, "rows": [], "count": 0, "total_sum": 0, "total_qty": 0, "own_count": 0,
    }


def get_cached() -> dict:
    if not CACHE.get("loaded"):
        return {
            "period": None, "category": None, "uploaded_at": None,
            "filename": None, "rows": [], "count": 0, "total_sum": 0, "total_qty": 0, "own_count": 0,
        }
    pos = summarize().get("position") or {}
    # keep old shape for any leftover callers
    return {
        **pos,
        "rows": pos.get("rows") or [],
    }
