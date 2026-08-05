"""Цены с клиентской витрины Ozon (ozon.ru) — аналог card.wb.ru на WB.

Seller API больше не отдаёт marketing_price / customer_price без Premium Pro.
Берём JSON composer-api той же страницы, что видит покупатель:
  GET https://www.ozon.ru/api/composer-api.bx/page/json/v2?url=/product/{sku}/

Парсим виджет webPrice (cardPrice / price / originalPrice) и fallback SEO LD+JSON.
Опционально: OZON_CLIENT_PROXY, curl_cffi (лучше проходит антибот).
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote

import httpx

logger = logging.getLogger("ozon-dashboard.client-prices")

COMPOSER = "https://www.ozon.ru/api/composer-api.bx/page/json/v2?url="
HOME = "https://www.ozon.ru/"

_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
    "Origin": "https://www.ozon.ru",
    "Referer": "https://www.ozon.ru/",
}


def _price_text_to_num(text) -> float | None:
    if text is None:
        return None
    if isinstance(text, (int, float)):
        return float(text) if float(text) > 0 else None
    digits = re.sub(r"[^\d]", "", str(text))
    if not digits:
        return None
    try:
        return float(digits)
    except ValueError:
        return None


def _widget(page: dict, name: str) -> dict | None:
    ws = page.get("widgetStates") or {}
    for key, raw in ws.items():
        if str(key).split("-", 1)[0] != name:
            continue
        try:
            return json.loads(raw) if isinstance(raw, str) else raw
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
    return None


def _parse_composer_page(page: dict, sku: int) -> dict | None:
    """webPrice → client_price; fallback SEO Product offers."""
    price_w = _widget(page, "webPrice") or {}
    card = _price_text_to_num(price_w.get("cardPrice"))
    regular = _price_text_to_num(price_w.get("price"))
    old = _price_text_to_num(price_w.get("originalPrice"))

    # Предпочитаем карточную (то, что крупно на PDP), иначе обычную
    client = card if card is not None else regular

    if client is None:
        seo = page.get("seo") or {}
        for sc in seo.get("script") or []:
            if not isinstance(sc, dict):
                continue
            raw = sc.get("innerHTML")
            if not raw:
                continue
            try:
                ld = json.loads(raw)
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            offers = ld.get("offers") if isinstance(ld, dict) else None
            if isinstance(offers, dict):
                client = _price_text_to_num(offers.get("price") or offers.get("lowPrice"))
                if old is None:
                    old = _price_text_to_num(offers.get("highPrice"))
            elif isinstance(offers, list) and offers:
                client = _price_text_to_num(offers[0].get("price"))
            if client is not None:
                break

    if client is None:
        return None

    heading = _widget(page, "webProductHeading") or {}
    return {
        "sku": int(sku),
        "client_price": client,
        "card_price": card,
        "regular_price": regular,
        "old_price": old,
        "available": price_w.get("isAvailable"),
        "name": heading.get("title") or (page.get("seo") or {}).get("title") or "",
        "source": "ozon.ru",
    }


def _http_get(url: str, timeout: float = 25.0) -> tuple[int, str]:
    """GET с curl_cffi (если есть) или httpx. Учитывает OZON_CLIENT_PROXY."""
    proxy = (os.environ.get("OZON_CLIENT_PROXY") or "").strip() or None
    try:
        from curl_cffi import requests as creq  # type: ignore

        r = creq.get(
            url,
            headers=_BROWSER_HEADERS,
            impersonate="chrome124",
            timeout=timeout,
            proxies={"http": proxy, "https": proxy} if proxy else None,
            allow_redirects=True,
        )
        return int(r.status_code), r.text or ""
    except ImportError:
        pass
    except Exception as e:
        logger.debug("curl_cffi get failed: %s", e)

    proxies = proxy
    with httpx.Client(
        headers=_BROWSER_HEADERS,
        timeout=timeout,
        follow_redirects=True,
        proxy=proxies,
    ) as client:
        r = client.get(url)
        return int(r.status_code), r.text or ""


def fetch_one_client_price(sku: int) -> dict | None:
    """Одна карточка: /product/{sku}/ через composer-api."""
    path = f"/product/{int(sku)}/"
    url = COMPOSER + quote(path, safe="")
    try:
        status, text = _http_get(url)
    except Exception as e:
        logger.warning("client price sku=%s: %s", sku, e)
        return None
    if status != 200:
        logger.debug("client price sku=%s HTTP %s", sku, status)
        return None
    if "incidentId" in text[:500] or "Access Denied" in text[:500]:
        return None
    try:
        page = json.loads(text)
    except json.JSONDecodeError:
        return None
    if not isinstance(page, dict) or not (page.get("widgetStates") or page.get("seo")):
        return None
    return _parse_composer_page(page, int(sku))


def fetch_client_prices(skus: list[int], max_workers: int | None = None) -> tuple[dict[int, dict], str | None]:
    """Цены с витрины. Возвращает ({sku: info}, source_or_None).

    Как на WB: без Seller API. На Ozon нет batch-nm — ходим по SKU параллельно.
    """
    uniq: list[int] = []
    seen: set[int] = set()
    for s in skus:
        try:
            si = int(s)
        except (TypeError, ValueError):
            continue
        if si and si not in seen:
            seen.add(si)
            uniq.append(si)
    if not uniq:
        return {}, None

    workers = max_workers
    if workers is None:
        try:
            workers = max(1, min(8, int(os.environ.get("OZON_CLIENT_WORKERS") or 6)))
        except ValueError:
            workers = 6

    by_sku: dict[int, dict] = {}
    # лёгкий прогрев куки (часто нужен abt_data)
    try:
        _http_get(HOME, timeout=15)
        time.sleep(0.3)
    except Exception:
        pass

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(fetch_one_client_price, sku): sku for sku in uniq}
        for fut in as_completed(futs):
            sku = futs[fut]
            try:
                info = fut.result()
            except Exception as e:
                logger.warning("client price worker sku=%s: %s", sku, e)
                continue
            if info and info.get("client_price") is not None:
                by_sku[int(sku)] = info

    source = "ozon.ru" if by_sku else None
    logger.info("client prices: %s/%s skus from ozon.ru", len(by_sku), len(uniq))
    return by_sku, source
