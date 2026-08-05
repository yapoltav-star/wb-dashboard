"""Цены с клиентской витрины Ozon (ozon.ru) — аналог card.wb.ru на WB.

Seller API больше не отдаёт marketing_price / customer_price без Premium Pro.
Берём JSON composer-api той же страницы, что видит покупатель:
  GET https://www.ozon.ru/api/composer-api.bx/page/json/v2?url=/product/{sku}/

Парсим виджет webPrice (cardPrice / price / originalPrice) и fallback SEO LD+JSON.

OZON_CLIENT_PROXY — RU mobile/residential (proxy.market). Важна одна sticky-сессия:
куки антибота шарим на все SKU (не параллелим вслепую).
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from urllib.parse import quote, urlparse

import httpx

logger = logging.getLogger("ozon-dashboard.client-prices")

COMPOSER = "https://www.ozon.ru/api/composer-api.bx/page/json/v2?url="
HOME = "https://www.ozon.ru/"

_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "Origin": "https://www.ozon.ru",
    "Referer": "https://www.ozon.ru/",
    "sec-ch-ua": '"Chromium";v="126", "Google Chrome";v="126", "Not.A/Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}

_lock = threading.Lock()
LAST_PROBE: dict = {}


def normalize_proxy_url(raw: str | None) -> str | None:
    """Приводит строку прокси к URL для httpx/curl_cffi."""
    s = (raw or "").strip()
    if not s:
        return None
    if "://" in s:
        return s

    def _hostport(part: str) -> bool:
        if ":" not in part:
            return False
        host, port = part.rsplit(":", 1)
        return bool(host) and port.isdigit()

    if "@" in s:
        left, right = s.split("@", 1)
        if _hostport(left) and not _hostport(right):
            return f"http://{right}@{left}"
        if _hostport(right):
            return f"http://{left}@{right}"
        return f"http://{s}"

    parts = s.split(":")
    if len(parts) == 4 and parts[1].isdigit():
        host, port, user, password = parts
        return f"http://{user}:{password}@{host}:{port}"
    if len(parts) == 2 and parts[1].isdigit():
        return f"http://{s}"
    logger.warning("OZON_CLIENT_PROXY: нераспознанный формат")
    return s


def proxy_configured() -> bool:
    return bool(normalize_proxy_url(os.environ.get("OZON_CLIENT_PROXY")))


def proxy_public_info() -> dict:
    """Без секретов — для статуса/баннера."""
    raw = normalize_proxy_url(os.environ.get("OZON_CLIENT_PROXY"))
    if not raw:
        return {"configured": False}
    try:
        u = urlparse(raw)
        host = u.hostname or ""
        port = u.port
        return {
            "configured": True,
            "scheme": u.scheme or "http",
            "host": host,
            "port": port,
            "has_auth": bool(u.username),
        }
    except Exception:
        return {"configured": True, "host": "?", "has_auth": True}


def _proxy_candidates() -> list[str]:
    """http и socks5 варианты одной и той же строки."""
    base = normalize_proxy_url(os.environ.get("OZON_CLIENT_PROXY"))
    if not base:
        return []
    out = [base]
    if base.startswith("http://"):
        out.append("socks5://" + base[len("http://") :])
        out.append("socks5h://" + base[len("http://") :])
    elif base.startswith("socks5://"):
        out.append("http://" + base[len("socks5://") :])
    return out


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


def _classify_body(status: int, text: str) -> str:
    head = (text or "")[:800]
    if status in (401, 407):
        return "proxy_auth"
    if "incidentId" in head or "fab_" in head:
        return "ozon_antibot"
    if status == 403:
        return "forbidden"
    if status == 200 and ("widgetStates" in head or '"seo"' in head):
        return "ok"
    if status == 200 and ("Доступ ограничен" in head or "antibot" in head.lower() or "нет соединения" in head.lower()):
        return "ozon_antibot"
    if status != 200:
        return f"http_{status}"
    return "unknown"


class _Session:
    """Одна sticky-сессия: proxy + cookies на весь синк."""

    def __init__(self, proxy: str | None):
        self.proxy = proxy
        self.backend = None
        self._creq = None
        self._httpx = None
        self._init()

    def _init(self):
        proxies = {"http": self.proxy, "https": self.proxy} if self.proxy else None
        try:
            from curl_cffi import requests as creq  # type: ignore

            self._creq = creq.Session()
            if proxies:
                self._creq.proxies = proxies
            self.backend = "curl_cffi"
            return
        except Exception as e:
            logger.info("curl_cffi unavailable (%s), httpx", e)

        self._httpx = httpx.Client(
            headers={k: v for k, v in _BROWSER_HEADERS.items()},
            timeout=35.0,
            follow_redirects=True,
            proxy=self.proxy,
        )
        self.backend = "httpx"

    def get(self, url: str, *, accept: str = "text/html,application/xhtml+xml,*/*;q=0.8") -> tuple[int, str]:
        headers = {**_BROWSER_HEADERS, "Accept": accept}
        if self._creq is not None:
            r = self._creq.get(
                url,
                headers=headers,
                impersonate="chrome124",
                timeout=35,
                allow_redirects=True,
            )
            return int(r.status_code), r.text or ""
        assert self._httpx is not None
        r = self._httpx.get(url, headers=headers)
        return int(r.status_code), r.text or ""

    def close(self):
        try:
            if self._creq is not None:
                self._creq.close()
        except Exception:
            pass
        try:
            if self._httpx is not None:
                self._httpx.close()
        except Exception:
            pass


def _warmup(sess: _Session) -> dict:
    """Прогрев главной — антибот пишет abt_data / cookies."""
    try:
        status, text = sess.get(HOME, accept="text/html,application/xhtml+xml,*/*;q=0.8")
    except Exception as e:
        return {"ok": False, "step": "home", "error": str(e)[:300]}
    kind = _classify_body(status, text)
    time.sleep(1.2)
    return {
        "ok": kind == "ok" or status == 200,
        "step": "home",
        "status": status,
        "kind": kind,
        "len": len(text or ""),
        "snippet": (text or "")[:160].replace("\n", " "),
    }


def _fetch_sku(sess: _Session, sku: int) -> tuple[dict | None, dict]:
    path = f"/product/{int(sku)}/"
    url = COMPOSER + quote(path, safe="")
    try:
        status, text = sess.get(url, accept="application/json, text/plain, */*")
    except Exception as e:
        return None, {"sku": sku, "ok": False, "error": str(e)[:300]}
    kind = _classify_body(status, text)
    meta = {"sku": sku, "status": status, "kind": kind, "len": len(text or "")}
    if kind != "ok":
        meta["snippet"] = (text or "")[:160].replace("\n", " ")
        return None, meta
    try:
        page = json.loads(text)
    except json.JSONDecodeError:
        meta["kind"] = "bad_json"
        return None, meta
    info = _parse_composer_page(page, int(sku)) if isinstance(page, dict) else None
    meta["ok"] = bool(info and info.get("client_price") is not None)
    if info:
        meta["client_price"] = info["client_price"]
    return info, meta


def probe_client_access(sample_sku: int | None = None) -> dict:
    """Диагностика прокси + одного SKU. Без секретов."""
    info = proxy_public_info()
    result = {
        "proxy": info,
        "tried": [],
        "ok": False,
        "hint": None,
    }
    candidates = _proxy_candidates() or [None]
    sku = sample_sku
    if sku is None:
        try:
            sku = int(os.environ.get("OZON_PROBE_SKU") or "0") or None
        except ValueError:
            sku = None

    for proxy in candidates:
        label = (proxy.split("@")[-1] if proxy and "@" in proxy else (proxy or "direct"))
        entry = {"proxy_endpoint": label, "backend": None}
        sess = None
        try:
            sess = _Session(proxy)
            entry["backend"] = sess.backend
            warm = _warmup(sess)
            entry["home"] = warm
            if sku:
                _info, meta = _fetch_sku(sess, int(sku))
                entry["product"] = meta
                if meta.get("ok"):
                    result["ok"] = True
                    result["client_price"] = meta.get("client_price")
                    result["working_proxy"] = label
                    result["tried"].append(entry)
                    break
            elif warm.get("kind") == "ok" or (warm.get("status") == 200 and warm.get("kind") != "ozon_antibot"):
                # без sku — хотя бы home
                if warm.get("kind") != "ozon_antibot" and warm.get("kind") != "proxy_auth":
                    result["ok"] = warm.get("status") == 200
            result["tried"].append(entry)
            if entry.get("home", {}).get("kind") == "proxy_auth":
                result["hint"] = "Прокси не принял логин/пароль (401/407). Сверь строку OZON_CLIENT_PROXY."
                break
            if entry.get("product", {}).get("kind") == "ozon_antibot" or entry.get("home", {}).get("kind") == "ozon_antibot":
                result["hint"] = (
                    "Прокси жив, но Ozon антибот. Нужны: страна Россия, липкая сессия, "
                    "и строка из «Создать список прокси» (не сырой логин пула)."
                )
        except Exception as e:
            entry["error"] = str(e)[:300]
            result["tried"].append(entry)
            err = str(e).lower()
            if "407" in err or "auth" in err or "proxy" in err:
                result["hint"] = "Ошибка авторизации/подключения к прокси. Проверь логин, пароль, порт."
        finally:
            if sess:
                sess.close()

    if not result["ok"] and not result["hint"]:
        if not info.get("configured"):
            result["hint"] = "OZON_CLIENT_PROXY не задан."
        else:
            result["hint"] = "Витрина не открылась. Открой /api/client-proxy-probe после Redeploy."

    with _lock:
        LAST_PROBE.clear()
        LAST_PROBE.update(result)
    return result


def fetch_client_prices(
    skus: list[int], max_workers: int | None = None
) -> tuple[dict[int, dict], str | None, dict]:
    """Цены с витрины. Возвращает ({sku: info}, source, diag)."""
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

    diag: dict = {"proxy": proxy_public_info(), "fetched": 0, "total": len(uniq), "error": None}
    if not uniq:
        return {}, None, diag

    # С прокси — последовательно на одной сессии (липкий IP + куки).
    # Без прокси — тоже последовательно: антибот иначе режет.
    by_sku: dict[int, dict] = {}
    last_fail = None
    working_proxy = None

    for proxy in _proxy_candidates() or [None]:
        sess = None
        try:
            sess = _Session(proxy)
            warm = _warmup(sess)
            diag["warmup"] = {k: warm.get(k) for k in ("status", "kind", "len", "error")}
            if warm.get("kind") == "proxy_auth":
                diag["error"] = "proxy_auth"
                last_fail = "proxy_auth"
                continue
            ok_n = 0
            for i, sku in enumerate(uniq):
                info, meta = _fetch_sku(sess, sku)
                if info and info.get("client_price") is not None:
                    by_sku[int(sku)] = info
                    ok_n += 1
                else:
                    last_fail = meta.get("kind") or meta.get("error") or "fail"
                    if meta.get("kind") in ("proxy_auth", "ozon_antibot") and ok_n == 0 and i < 3:
                        # на старте уже блок — пробуем другой proxy scheme
                        break
                if (i + 1) % 20 == 0:
                    time.sleep(0.4)
                else:
                    time.sleep(0.12)
            if by_sku:
                working_proxy = proxy
                break
        except Exception as e:
            last_fail = str(e)[:200]
            diag["error"] = last_fail
            logger.warning("client prices session failed: %s", e)
        finally:
            if sess:
                sess.close()

    diag["fetched"] = len(by_sku)
    diag["last_fail"] = last_fail
    if working_proxy:
        try:
            diag["working_endpoint"] = working_proxy.split("@")[-1]
        except Exception:
            pass
    source = "ozon.ru" if by_sku else None
    logger.info("client prices: %s/%s skus (fail=%s)", len(by_sku), len(uniq), last_fail)
    with _lock:
        LAST_PROBE["last_sync"] = diag
    return by_sku, source, diag
