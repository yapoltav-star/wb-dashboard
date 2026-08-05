"""Браузерный забор цен ozon.ru через Playwright (+ прокси).

Простой HTTP (curl_cffi) Ozon режет антиботом даже с мобильным IP.
Как на практике у парсеров: один Chromium проходит проверку, дальше
fetch JSON composer-api из контекста страницы.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Callable
from urllib.parse import urlparse

logger = logging.getLogger("ozon-dashboard.client-browser")

HOME = "https://www.ozon.ru/"
API = "https://www.ozon.ru/api/composer-api.bx/page/json/v2?url="


def _playwright_proxy(proxy_url: str | None) -> dict | None:
    if not proxy_url:
        return None
    u = urlparse(proxy_url)
    if not u.hostname or not u.port:
        return None
    scheme = u.scheme if u.scheme in ("http", "https", "socks5") else "http"
    server = f"{scheme}://{u.hostname}:{u.port}"
    out: dict = {"server": server}
    if u.username:
        out["username"] = u.username
    if u.password:
        out["password"] = u.password
    return out


def browser_available() -> bool:
    try:
        from playwright.sync_api import sync_playwright  # noqa: F401

        return True
    except ImportError:
        return False


def fetch_pages_via_browser(
    paths: list[str],
    proxy_url: str | None,
    parse_page: Callable[[dict, int], dict | None],
    sku_of_path: Callable[[str], int],
    challenge_wait_ms: int = 10000,
) -> tuple[dict[int, dict], dict]:
    """Открыть ozon.ru в Chromium (через прокси), затем fetch composer JSON.

    Returns ({sku: info}, diag).
    """
    diag: dict = {"backend": "playwright", "ok": False, "fetched": 0}
    if not paths:
        return {}, diag
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        diag["error"] = "playwright_not_installed"
        return {}, diag

    pw_proxy = _playwright_proxy(proxy_url)
    diag["proxy"] = bool(pw_proxy)
    by_sku: dict[int, dict] = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
            ],
        )
        context_kwargs: dict = {
            "viewport": {"width": 1365, "height": 900},
            "locale": "ru-RU",
            "user_agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/126.0.0.0 Safari/537.36"
            ),
        }
        if pw_proxy:
            context_kwargs["proxy"] = pw_proxy
        context = browser.new_context(**context_kwargs)
        page = context.new_page()
        try:
            page.goto(HOME, wait_until="domcontentloaded", timeout=90000)
            page.wait_for_timeout(challenge_wait_ms)
            title = page.title() or ""
            diag["home_title"] = title[:80]
            if any(x in title.lower() for x in ("antibot", "доступ", "ограничен", "соединения")):
                # ещё подождём — иногда challenge долгий
                page.wait_for_timeout(8000)
                title = page.title() or ""
                diag["home_title"] = title[:80]

            # проверка: один fetch
            test = page.evaluate(
                """async (url) => {
                    const r = await fetch(url, { headers: { accept: 'application/json' } });
                    const t = await r.text();
                    return { status: r.status, len: t.length, head: t.slice(0, 120) };
                }""",
                API + quote_path("/"),
            )
            diag["home_api"] = test
            if int(test.get("status") or 0) in (403, 307) or "incidentId" in str(test.get("head") or ""):
                diag["error"] = "ozon_antibot_browser"
                # всё равно пробуем товары — иногда home режут, PDP пускают

            for i, path in enumerate(paths):
                sku = sku_of_path(path)
                url = API + quote_path(path)
                try:
                    body = page.evaluate(
                        """async (url) => {
                            const r = await fetch(url, { headers: { accept: 'application/json' } });
                            return { status: r.status, text: await r.text() };
                        }""",
                        url,
                    )
                except Exception as e:
                    logger.warning("browser fetch sku=%s: %s", sku, e)
                    continue
                status = int(body.get("status") or 0)
                text = body.get("text") or ""
                if status != 200 or "incidentId" in text[:400]:
                    if i == 0:
                        diag["first_product"] = {
                            "sku": sku,
                            "status": status,
                            "head": text[:120],
                        }
                    continue
                try:
                    data = json.loads(text)
                except json.JSONDecodeError:
                    continue
                info = parse_page(data, sku) if isinstance(data, dict) else None
                if info and info.get("client_price") is not None:
                    by_sku[int(sku)] = info
                if (i + 1) % 15 == 0:
                    time.sleep(0.3)
        except Exception as e:
            diag["error"] = str(e)[:300]
            logger.warning("browser session failed: %s", e)
        finally:
            try:
                context.close()
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass

    diag["fetched"] = len(by_sku)
    diag["ok"] = bool(by_sku)
    return by_sku, diag


def quote_path(path: str) -> str:
    from urllib.parse import quote

    return quote(path, safe="")
