"""
Телеграм-бот к дашборду. Только чтение.

Бот не меняет ничего в WB, Ozon и Supabase — он ходит в те же функции, что и
сайт, и отдаёт результат текстом. Живёт в том же процессе, что и FastAPI,
на том же сервере Railway. Отдельный VPS для него не нужен.

Переменные окружения:
  TELEGRAM_BOT_TOKEN        токен от @BotFather (без него бот не стартует)
  TELEGRAM_ALLOWED_CHAT_IDS белый список chat_id через запятую
  ANTHROPIC_API_KEY         опционально: включает ответы на вопросы текстом
  ANTHROPIC_MODEL           опционально: модель, по умолчанию claude-sonnet-4-5
"""

import html
import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta

import httpx

logger = logging.getLogger(__name__)

ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_VERSION = "2023-06-01"
TG_CHUNK = 3800
HISTORY_TURNS = 8
TOOL_STEPS_LIMIT = 6

_API: dict = {}
_HISTORY: dict = {}
_HISTORY_LOCK = threading.Lock()
_STARTED = False


# ---------- конфиг ----------

def _token() -> str:
    return (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()


def _allowed_ids() -> set:
    raw = os.getenv("TELEGRAM_ALLOWED_CHAT_IDS") or ""
    out = set()
    for part in raw.replace(";", ",").split(","):
        part = part.strip()
        if part:
            try:
                out.add(int(part))
            except ValueError:
                logger.warning(f"telegram: не число в TELEGRAM_ALLOWED_CHAT_IDS: {part!r}")
    return out


def _anthropic_key() -> str:
    return (os.getenv("ANTHROPIC_API_KEY") or "").strip()


def _anthropic_model() -> str:
    return (os.getenv("ANTHROPIC_MODEL") or "claude-sonnet-4-5").strip()


# ---------- транспорт ----------

def _tg(method: str, payload: dict = None, timeout: float = 35):
    url = f"https://api.telegram.org/bot{_token()}/{method}"
    r = httpx.post(url, json=payload or {}, timeout=timeout)
    r.raise_for_status()
    return r.json()


def _send(chat_id: int, text: str):
    text = text or "Пусто."
    while text:
        chunk, text = text[:TG_CHUNK], text[TG_CHUNK:]
        try:
            _tg("sendMessage", {
                "chat_id": chat_id,
                "text": chunk,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            })
        except Exception as e:
            logger.error(f"telegram send: {e}")
            return


def _typing(chat_id: int):
    try:
        _tg("sendChatAction", {"chat_id": chat_id, "action": "typing"}, timeout=10)
    except Exception:
        pass


# ---------- форматирование ----------

def _esc(v) -> str:
    return html.escape(str(v if v is not None else ""))


def _money(v) -> str:
    try:
        return f"{float(v):,.0f}".replace(",", " ") + " ₽"
    except Exception:
        return "—"


def _num(v) -> str:
    try:
        return f"{int(v):,}".replace(",", " ")
    except Exception:
        return "—"


def _pre(lines: list) -> str:
    body = "\n".join(_esc(x) for x in lines)
    return f"<pre>{body}</pre>"


def _compact(obj, max_list: int = 12, depth: int = 0):
    """Режет длинные списки и глубокую вложенность, чтобы не жечь токены."""
    if depth > 4:
        return "…"
    if isinstance(obj, dict):
        return {k: _compact(v, max_list, depth + 1) for k, v in obj.items()}
    if isinstance(obj, list):
        head = [_compact(x, max_list, depth + 1) for x in obj[:max_list]]
        if len(obj) > max_list:
            head.append(f"…ещё {len(obj) - max_list}")
        return head
    return obj


def _slim(d: dict) -> dict:
    if not isinstance(d, dict):
        return {}
    return {k: v for k, v in d.items() if v is None or isinstance(v, (str, int, float, bool))}


# ---------- инструменты (только чтение) ----------

def tool_products(query: str = "", only_low_cover: bool = False, limit: int = 15) -> dict:
    data = _API["wb_products"]() or {}
    items = data.get("products") or []
    q = (query or "").strip().lower()

    rows = []
    for p in items:
        blob = f"{p.get('vendor_code') or ''} {p.get('name') or ''} {p.get('nm_id') or ''}".lower()
        if q and q not in blob:
            continue
        s7 = int(p.get("sales_7d") or 0)
        stock = int(p.get("stock") or 0)
        per_day = s7 / 7.0
        cover = round(stock / per_day, 1) if per_day > 0 else None
        rows.append({
            "vendor_code": p.get("vendor_code"),
            "nm_id": p.get("nm_id"),
            "name": p.get("name"),
            "client_price": p.get("client_price"),
            "stock": stock,
            "warehouse_count": p.get("warehouse_count"),
            "channels": p.get("channels"),
            "sales_yesterday": int(p.get("sales_yesterday") or 0),
            "sales_7d": s7,
            "sales_28d": int(p.get("sales_28d") or 0),
            "days_cover": cover,
        })

    if only_low_cover:
        rows = [r for r in rows if r["days_cover"] is not None and r["days_cover"] < 14]
        rows.sort(key=lambda r: (r["days_cover"], -r["sales_7d"]))
    else:
        rows.sort(key=lambda r: -r["sales_7d"])

    return {
        "matched": len(rows),
        "items": rows[: max(1, min(int(limit or 15), 40))],
        "stock_updated_at": data.get("stock_updated_at"),
        "prices_updated_at": data.get("prices_updated_at"),
        "sales_updated_at": data.get("sales_updated_at"),
    }


def tool_geography(days: int = 28, channel: str = "all",
                   date_from: str = None, date_to: str = None) -> dict:
    if not date_from and not date_to:
        today = date.today()
        date_to = today.isoformat()
        date_from = (today - timedelta(days=max(1, int(days or 28)) - 1)).isoformat()
    agg = _API["orders_geo"](date_from=date_from, date_to=date_to, channel=channel or "all") or {}
    return {
        "period": {"from": date_from, "to": date_to, "channel": channel or "all"},
        "summary": agg.get("summary") or {},
        "top_warehouses": (agg.get("by_warehouse") or [])[:10],
        "top_regions": (agg.get("by_region") or [])[:10],
        "top_cities": (agg.get("by_city") or [])[:15],
        "top_articles": (agg.get("by_article") or [])[:10],
        "meta": agg.get("meta") or {},
        "data_range": {
            "min": (agg.get("filters") or {}).get("date_min"),
            "max": (agg.get("filters") or {}).get("date_max"),
            "cached_orders": (agg.get("filters") or {}).get("total_cached"),
        },
    }


def tool_fbs_speed(days: int = 14) -> dict:
    d = _API["fbs_speed"](days=max(1, int(days or 14))) or {}
    out = {k: v for k, v in d.items() if k not in ("orders_sample", "warehouses")}
    out["warehouses"] = (d.get("warehouses") or [])[:10]
    return out


def tool_sales_pace(period: str = "day") -> dict:
    if period not in ("day", "week", "weeks2", "month"):
        period = "day"
    d = _API["sales_pace"](period=period) or {}
    return {
        "period": d.get("period"),
        "period_name": d.get("period_name"),
        "label_cur": d.get("label_cur"),
        "label_prev": d.get("label_prev"),
        "as_of": d.get("as_of"),
        "updated_at": d.get("updated_at"),
        "syncing": d.get("syncing"),
        "articles": [_slim(a) for a in (d.get("articles") or [])[:15]],
    }


def tool_own_warehouse() -> dict:
    d = _API["own_warehouse"]() or {}
    return _compact(d, max_list=15)


TOOLS = {
    "products": tool_products,
    "geography": tool_geography,
    "fbs_speed": tool_fbs_speed,
    "sales_pace": tool_sales_pace,
    "own_warehouse": tool_own_warehouse,
}

TOOL_SCHEMAS = [
    {
        "name": "products",
        "description": (
            "Товары Wildberries: цена для покупателя, остаток, число складов, канал "
            "FBW/FBS и продажи за вчера, 7 и 28 дней. Плюс days_cover — на сколько дней "
            "хватит остатка при текущем темпе. Используй для вопросов про остатки, цены, "
            "продажи конкретных артикулов и про то, что скоро закончится."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Часть артикула, названия или nm_id"},
                "only_low_cover": {"type": "boolean", "description": "Только то, чего хватит меньше чем на 14 дней"},
                "limit": {"type": "integer", "description": "Сколько строк вернуть, максимум 40"},
            },
        },
    },
    {
        "name": "geography",
        "description": (
            "География заказов FBS и FBW: сводка, склады отгрузки, регионы и города "
            "назначения, топ артикулов. Данные из ленты заказов и Statistics API."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "days": {"type": "integer", "description": "Глубина периода в днях, по умолчанию 28"},
                "channel": {"type": "string", "enum": ["all", "FBS", "FBW"]},
                "date_from": {"type": "string", "description": "YYYY-MM-DD, перебивает days"},
                "date_to": {"type": "string", "description": "YYYY-MM-DD"},
            },
        },
    },
    {
        "name": "fbs_speed",
        "description": (
            "Скорость отгрузки FBS и экономика коэффициента kC: медиана часов от заказа "
            "до сканирования, распределение по порогам 13/42/48 часов, бонусы и штрафы в рублях."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"days": {"type": "integer", "description": "По умолчанию 14"}},
        },
    },
    {
        "name": "sales_pace",
        "description": "Темп продаж: текущий период против предыдущего, по артикулам.",
        "input_schema": {
            "type": "object",
            "properties": {"period": {"type": "string", "enum": ["day", "week", "weeks2", "month"]}},
        },
    },
    {
        "name": "own_warehouse",
        "description": "Остатки на собственном складе из Google Sheets, включая позиции без артикула продавца.",
        "input_schema": {"type": "object", "properties": {}},
    },
]

SYSTEM_PROMPT = (
    "Ты помощник селлера Wildberries и Ozon. Категория — смарт-часы. "
    "Отвечаешь в телеграме, поэтому коротко: несколько предложений или компактный список, "
    "без заголовков и таблиц.\n\n"
    "Всегда бери цифры из инструментов, никогда не выдумывай. Если инструмент вернул пусто "
    "или устаревшие данные — скажи об этом прямо.\n\n"
    "Что важно знать про экономику: комиссия FBS 37%, FBW 32,5%. С 7 августа 2026 действует "
    "коэффициент kC за скорость отгрузки: до 13 часов минус 5 пунктов, от 13 до 42 часов "
    "минус 3,5 пункта, от 42 до 48 базовая ставка, дальше штрафы. Отгрузка считается от "
    "создания заказа до сканирования на складе.\n\n"
    "У тебя только чтение. Если просят что-то изменить — цену, остаток, карточку — объясни, "
    "что это делается на сайте дашборда, и не пытайся."
)


def _run_tool(name: str, args: dict) -> dict:
    fn = TOOLS.get(name)
    if not fn:
        return {"error": f"нет инструмента {name}"}
    try:
        return _compact(fn(**(args or {})))
    except Exception as e:
        logger.error(f"telegram tool {name}: {e}")
        return {"error": str(e)}


# ---------- ответы на команды ----------

HELP = (
    "Дашборд на связи. Только чтение, ничего не меняю.\n\n"
    "<b>Команды</b>\n"
    "/stock — что скоро закончится\n"
    "/stock 042 — остаток и продажи по артикулу\n"
    "/sales — топ продаж за 7 дней\n"
    "/geo — география заказов за 28 дней\n"
    "/geo 7 fbs — то же за 7 дней только по FBS\n"
    "/fbs — скорость отгрузки и экономика kC\n"
    "/sklad — свой склад\n"
    "/reset — забыть контекст разговора\n"
)

HELP_LLM_OFF = (
    "\nВопросы обычным текстом пока выключены: не задан ANTHROPIC_API_KEY. "
    "Команды выше работают без него."
)


def cmd_stock(arg: str) -> str:
    q = (arg or "").strip()
    data = tool_products(query=q, only_low_cover=not q, limit=12)
    items = data.get("items") or []
    if not items:
        return "Ничего не нашёл." if q else "Нечему заканчиваться: позиций с запасом меньше 14 дней нет."

    out = [f"<b>{'Поиск: ' + _esc(q) if q else 'Заканчивается — меньше 14 дней запаса'}</b>", ""]
    for r in items:
        cover = r.get("days_cover")
        if cover is None:
            cover_s = "продаж нет"
        elif cover < 1:
            cover_s = "кончилось"
        else:
            cover_s = f"хватит на {cover:.0f} дн"
        out.append(f"<code>{_esc(r.get('vendor_code'))}</code>")
        out.append(f"{cover_s} · остаток {_num(r.get('stock'))} на {_num(r.get('warehouse_count'))} скл "
                   f"· {_num(r.get('sales_7d'))} шт за 7 дн")
    out.append(f"\nОстатки обновлены: {_esc(data.get('stock_updated_at') or '—')}")
    return "\n".join(out)


def cmd_sales() -> str:
    data = tool_products(limit=12)
    items = data.get("items") or []
    if not items:
        return "Продаж не вижу — возможно, данные ещё синхронизируются."

    out = ["<b>Топ продаж за 7 дней</b>", ""]
    for i, r in enumerate(items, 1):
        out.append(f"{i}. <code>{_esc(r.get('vendor_code'))}</code>")
        out.append(f"вчера {_num(r.get('sales_yesterday'))} · 7 дн {_num(r.get('sales_7d'))} "
                   f"· 28 дн {_num(r.get('sales_28d'))} · остаток {_num(r.get('stock'))}")
    out.append(f"\nПродажи обновлены: {_esc(data.get('sales_updated_at') or '—')}")
    return "\n".join(out)


def cmd_geo(arg: str) -> str:
    parts = (arg or "").split()
    days, channel = 28, "all"
    for p in parts:
        if p.isdigit():
            days = int(p)
        elif p.upper() in ("FBS", "FBW"):
            channel = p.upper()

    d = tool_geography(days=days, channel=channel)
    s = d.get("summary") or {}
    if not s.get("total_orders"):
        return ("За этот период заказов в кэше нет. Данные подтягиваются на сайте "
                "в разделе «Остатки и поставки» — кнопкой синхронизации или загрузкой ленты заказов.")

    period = d.get("period") or {}
    out = [
        f"<b>География заказов, {_esc(period.get('from'))} — {_esc(period.get('to'))}"
        f"{'' if channel == 'all' else ', ' + channel}</b>",
        f"Заказов {_num(s.get('total_orders'))} на {_money(s.get('total_revenue'))}, "
        f"средний чек {_money(s.get('avg_order_price'))}",
        f"FBS {_num(s.get('fbs_orders'))} ({s.get('fbs_share_pct')}%) · "
        f"FBW {_num(s.get('fbw_orders'))} ({s.get('fbw_share_pct')}%)",
    ]

    whs = (d.get("top_warehouses") or [])[:5]
    if whs:
        lines = [f"{str(w.get('warehouse') or '')[:24]:<25}{w.get('orders') or 0:>6}{str(w.get('share_pct')) + '%':>7}"
                 for w in whs]
        out.append("\n<b>Склады отгрузки</b>" + _pre(lines))

    regs = (d.get("top_regions") or [])[:6]
    if regs:
        lines = [f"{str(r.get('region') or '')[:24]:<25}{r.get('total_orders') or 0:>6}{str(r.get('share_pct')) + '%':>7}"
                 for r in regs]
        out.append("<b>Регионы назначения</b>" + _pre(lines))

    note = (d.get("meta") or {}).get("city_note")
    if note:
        out.append(_esc(note))
    return "\n".join(out)


def cmd_fbs(arg: str) -> str:
    days = int(arg.strip()) if (arg or "").strip().isdigit() else 14
    d = tool_fbs_speed(days=days)
    if d.get("error"):
        return f"Не смог собрать отчёт: {_esc(d['error'])}"
    if not d.get("total_orders"):
        return f"За {days} дней сборочных заданий FBS не нашлось."

    bc = d.get("bracket_counts") or {}
    delivered = d.get("delivered_orders") or 0
    fast = int(bc.get("fast_13") or 0)
    out = [
        f"<b>Скорость FBS за {days} дн.</b>",
        f"Заказов {_num(d.get('total_orders'))}, отгружено {_num(delivered)}, "
        f"в работе {_num(d.get('pending_orders'))}",
        f"Медиана отгрузки {d.get('median_hours')} ч",
        f"До 13 ч: {_num(fast)}"
        + (f" ({round(fast / delivered * 100)}% отгруженных)" if delivered else ""),
        f"13–42 ч: {_num(bc.get('norm_42'))} · 42–48 ч: {_num(bc.get('base_48'))}",
        f"Просрочка свыше 48 ч: {_num((bc.get('fine_54') or 0) + (bc.get('fine_60') or 0) + (bc.get('fine_over') or 0))}",
        "",
        f"Бонус {_money(d.get('total_bonus_rub'))}, штраф {_money(d.get('total_fine_rub'))}",
        f"<b>Итого {_money(d.get('net_profit_rub'))}</b>",
    ]
    whs = (d.get("warehouses") or [])[:5]
    if whs:
        lines = [f"{str(w.get('warehouse_name') or '')[:22]:<23}"
                 f"{w.get('total_orders') or 0:>6}"
                 f"{str(w.get('median_hours')) + 'ч':>7}" for w in whs]
        out.append("\n<b>По складам</b>" + _pre(lines))
    return "\n".join(out)


def cmd_sklad() -> str:
    try:
        d = _API["own_warehouse"]() or {}
    except Exception as e:
        return f"Свой склад не отвечает: {_esc(e)}"
    items = d.get("items") or d.get("products") or []
    total = sum(int(i.get("qty") or i.get("quantity") or 0) for i in items) if isinstance(items, list) else 0
    return (f"<b>Свой склад</b>\nПозиций {_num(len(items) if isinstance(items, list) else 0)}, "
            f"единиц {_num(total)}\nОбновлено: {_esc(d.get('updated_at') or '—')}")


# ---------- вопросы обычным текстом ----------

def _history(chat_id: int) -> list:
    with _HISTORY_LOCK:
        return list(_HISTORY.get(chat_id) or [])


def _remember(chat_id: int, messages: list):
    with _HISTORY_LOCK:
        _HISTORY[chat_id] = messages[-HISTORY_TURNS * 2:]


def ask_llm(chat_id: int, question: str) -> str:
    key = _anthropic_key()
    if not key:
        return "Вопросы текстом выключены: не задан ANTHROPIC_API_KEY. Работают команды из /help."

    messages = _history(chat_id) + [{"role": "user", "content": question}]
    headers = {
        "x-api-key": key,
        "anthropic-version": ANTHROPIC_VERSION,
        "content-type": "application/json",
    }

    for _ in range(TOOL_STEPS_LIMIT):
        try:
            r = httpx.post(ANTHROPIC_URL, headers=headers, timeout=120, json={
                "model": _anthropic_model(),
                "max_tokens": 1500,
                "system": SYSTEM_PROMPT,
                "tools": TOOL_SCHEMAS,
                "messages": messages,
            })
        except Exception as e:
            return f"Не достучался до Anthropic: {_esc(e)}"

        if r.status_code != 200:
            detail = r.text[:300]
            return f"Anthropic вернул {r.status_code}. {_esc(detail)}"

        data = r.json()
        blocks = data.get("content") or []

        if data.get("stop_reason") == "tool_use":
            messages.append({"role": "assistant", "content": blocks})
            results = []
            for b in blocks:
                if b.get("type") == "tool_use":
                    _typing(chat_id)
                    out = _run_tool(b.get("name"), b.get("input") or {})
                    results.append({
                        "type": "tool_result",
                        "tool_use_id": b.get("id"),
                        "content": json.dumps(out, ensure_ascii=False)[:20000],
                    })
            messages.append({"role": "user", "content": results})
            continue

        text = "".join(b.get("text") or "" for b in blocks if b.get("type") == "text").strip()
        messages.append({"role": "assistant", "content": text or "…"})
        _remember(chat_id, messages)
        return _esc(text) or "Пустой ответ."

    return "Запутался в данных, переспроси конкретнее."


# ---------- маршрутизация ----------

def handle_message(chat_id: int, text: str):
    text = (text or "").strip()
    if not text:
        return
    _typing(chat_id)

    cmd, _, arg = text.partition(" ")
    cmd = cmd.lower().split("@")[0]

    try:
        if cmd in ("/start", "/help"):
            reply = HELP + ("" if _anthropic_key() else HELP_LLM_OFF)
        elif cmd == "/stock":
            reply = cmd_stock(arg)
        elif cmd == "/sales":
            reply = cmd_sales()
        elif cmd == "/geo":
            reply = cmd_geo(arg)
        elif cmd == "/fbs":
            reply = cmd_fbs(arg)
        elif cmd == "/sklad":
            reply = cmd_sklad()
        elif cmd == "/reset":
            with _HISTORY_LOCK:
                _HISTORY.pop(chat_id, None)
            reply = "Контекст очищен."
        elif cmd.startswith("/"):
            reply = "Такой команды нет. /help покажет список."
        else:
            reply = ask_llm(chat_id, text)
    except Exception as e:
        logger.error(f"telegram handle: {e}")
        reply = f"Сломался на этом вопросе: {_esc(e)}"

    _send(chat_id, reply)


def _process_update(upd: dict, allowed: set):
    msg = upd.get("message") or upd.get("edited_message") or {}
    chat = msg.get("chat") or {}
    chat_id = chat.get("id")
    text = msg.get("text")
    if not chat_id or not text:
        return

    if not allowed:
        _send(chat_id, f"Белый список пуст. Добавь в переменные окружения:\n"
                       f"<code>TELEGRAM_ALLOWED_CHAT_IDS={chat_id}</code>")
        return
    if chat_id not in allowed:
        logger.warning(f"telegram: отклонён chat_id {chat_id}")
        return

    handle_message(chat_id, text)


def _poll_loop():
    pool = ThreadPoolExecutor(max_workers=3, thread_name_prefix="tg")
    offset = None
    backoff = 1

    try:
        me = _tg("getMe", timeout=15).get("result") or {}
        logger.info(f"telegram: бот @{me.get('username')} запущен")
    except Exception as e:
        logger.error(f"telegram: getMe не прошёл, бот выключен: {e}")
        return

    while True:
        try:
            payload = {"timeout": 25, "allowed_updates": ["message"]}
            if offset is not None:
                payload["offset"] = offset
            res = _tg("getUpdates", payload, timeout=40)
            backoff = 1
            allowed = _allowed_ids()
            for upd in res.get("result") or []:
                offset = int(upd.get("update_id", 0)) + 1
                pool.submit(_process_update, upd, allowed)
        except Exception as e:
            logger.error(f"telegram poll: {e}")
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)


def start_bot(api: dict):
    """Запускает бота фоновым потоком. api — словарь функций чтения из main."""
    global _STARTED
    if _STARTED:
        return False
    if not _token():
        logger.info("telegram: TELEGRAM_BOT_TOKEN не задан, бот не запущен")
        return False

    required = ("wb_products", "orders_geo", "fbs_speed", "sales_pace", "own_warehouse")
    missing = [k for k in required if k not in api]
    if missing:
        logger.warning(f"telegram: не переданы источники {missing}, часть команд не сработает")

    _API.update(api)
    _STARTED = True
    threading.Thread(target=_poll_loop, daemon=True, name="telegram-bot").start()
    return True
