"""
Телеграм-бот к дашборду. Только чтение.

Бот не меняет ничего в WB, Ozon и Supabase — он ходит в те же функции, что и
сайт, и отдаёт результат текстом. Живёт в том же процессе, что и FastAPI,
на том же сервере Railway. Отдельный VPS для него не нужен.

Переменные окружения:
  TELEGRAM_BOT_TOKEN        токен от @BotFather (без него бот не стартует)
  TELEGRAM_ALLOWED_CHAT_IDS белый список chat_id через запятую
  OPENAI_API_KEY            опционально: вопросы текстом + распознавание голоса (Whisper)
  OPENAI_MODEL              опционально: модель, по умолчанию gpt-4o
  OPENAI_TRANSCRIBE_MODEL   опционально: модель STT, по умолчанию whisper-1
  ANTHROPIC_API_KEY         альтернатива OpenAI для текста (голос всё равно нужен OpenAI)
  ANTHROPIC_MODEL           опционально: модель, по умолчанию claude-sonnet-4-5
  LLM_PROVIDER              openai или anthropic, если заданы оба ключа
"""

import html
import json
import logging
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

import httpx

logger = logging.getLogger(__name__)

ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_VERSION = "2023-06-01"
OPENAI_URL = "https://api.openai.com/v1/chat/completions"
OPENAI_TRANSCRIBE_URL = "https://api.openai.com/v1/audio/transcriptions"
_OPENAI_TOKEN_PARAM = "max_completion_tokens"
# GPT-5.x не даёт использовать функции в chat/completions без reasoning_effort=none.
_OPENAI_EXTRA = {"reasoning_effort": "none"}
TG_CHUNK = 3800
HISTORY_TURNS = 8
TOOL_STEPS_LIMIT = 6
VOICE_MAX_BYTES = 24 * 1024 * 1024  # Whisper принимает до 25 МБ

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


def _openai_key() -> str:
    return (os.getenv("OPENAI_API_KEY") or "").strip()


def _openai_model() -> str:
    return (os.getenv("OPENAI_MODEL") or "gpt-5.6-terra").strip()


def _transcribe_model() -> str:
    return (os.getenv("OPENAI_TRANSCRIBE_MODEL") or "whisper-1").strip()


def _provider() -> str:
    """Какой моделью отвечать. Пусто — вопросы текстом выключены."""
    forced = (os.getenv("LLM_PROVIDER") or "").strip().lower()
    if forced == "openai" and _openai_key():
        return "openai"
    if forced == "anthropic" and _anthropic_key():
        return "anthropic"
    if _openai_key():
        return "openai"
    if _anthropic_key():
        return "anthropic"
    return ""


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


def _download_tg_file(file_id: str) -> tuple:
    """Скачать файл Telegram по file_id → (bytes, filename)."""
    meta = _tg("getFile", {"file_id": file_id}, timeout=30)
    info = meta.get("result") or {}
    path = info.get("file_path") or ""
    if not path:
        raise RuntimeError("Telegram не вернул file_path")
    size = int(info.get("file_size") or 0)
    if size and size > VOICE_MAX_BYTES:
        raise RuntimeError(f"Файл слишком большой ({size} байт), лимит Whisper 25 МБ")
    url = f"https://api.telegram.org/file/bot{_token()}/{path}"
    r = httpx.get(url, timeout=60)
    r.raise_for_status()
    if len(r.content) > VOICE_MAX_BYTES:
        raise RuntimeError("Файл слишком большой для распознавания")
    name = path.rsplit("/", 1)[-1] or "voice.ogg"
    return r.content, name


def _transcribe_audio(content: bytes, filename: str) -> str:
    """OpenAI Whisper: голос → текст. Нужен OPENAI_API_KEY."""
    key = _openai_key()
    if not key:
        raise RuntimeError("Нет OPENAI_API_KEY — голос распознаю только через Whisper")
    # Голосовые Telegram — ogg/opus; у audio часто mp3/m4a.
    lower = (filename or "").lower()
    if lower.endswith((".ogg", ".oga", ".opus")):
        mime = "audio/ogg"
    elif lower.endswith(".mp3"):
        mime = "audio/mpeg"
    elif lower.endswith((".m4a", ".mp4")):
        mime = "audio/mp4"
    elif lower.endswith(".wav"):
        mime = "audio/wav"
    else:
        mime = "application/octet-stream"
        if "." not in (filename or ""):
            filename = (filename or "voice") + ".ogg"
    r = httpx.post(
        OPENAI_TRANSCRIBE_URL,
        headers={"Authorization": f"Bearer {key}"},
        data={
            "model": _transcribe_model(),
            "language": "ru",
            "response_format": "json",
        },
        files={"file": (filename, content, mime)},
        timeout=120,
    )
    if r.status_code != 200:
        raise RuntimeError(f"Whisper HTTP {r.status_code}: {r.text[:300]}")
    text = ((r.json() or {}).get("text") or "").strip()
    return text


def _extract_voice_media(msg: dict):
    """voice (кружок/запись) или audio (файл). video_note не трогаем."""
    for key in ("voice", "audio"):
        media = msg.get(key)
        if isinstance(media, dict) and media.get("file_id"):
            return media
    return None


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


# Цвета в артикулах продавца обычно на английском (gold/black), а в чате пишут
# по-русски («042 Голд»). Синонимы сводим к одному канону.
_COLOR_CANON = {
    "gold": "gold", "голд": "gold", "голда": "gold",
    "золото": "gold", "золотой": "gold", "золотая": "gold",
    "золотые": "gold", "золотых": "gold", "зол": "gold",
    "black": "black", "блэк": "black", "блек": "black",
    "черный": "black", "чёрный": "black", "черные": "black",
    "чёрные": "black", "черн": "black", "чёрн": "black",
    "grey": "grey", "gray": "grey", "грей": "grey",
    "серый": "grey", "серые": "grey", "сер": "grey",
    "графит": "grey", "graphite": "grey",
    "pink": "pink", "пинк": "pink", "розовый": "pink",
    "розовые": "pink", "розов": "pink", "rose": "pink",
    "silver": "silver", "сильвер": "silver",
    "серебро": "silver", "серебряный": "silver", "серебряные": "silver",
    "white": "white", "вайт": "white", "белый": "white", "белые": "white", "бел": "white",
    "red": "red", "ред": "red", "красный": "red", "красные": "red", "красн": "red",
    "green": "green", "грин": "green", "зеленый": "green", "зелёный": "green",
    "зеленые": "green", "зелёные": "green", "зелен": "green",
    "beige": "beige", "беж": "beige", "бежевый": "beige",
    "blue": "blue", "блю": "blue", "синий": "blue", "синие": "blue", "син": "blue",
}


def _norm_txt(s: str) -> str:
    return (s or "").lower().replace("ё", "е").strip()


def _split_tokens(s: str) -> list:
    return [t for t in re.split(r"[\s_\-/,+]+", _norm_txt(s)) if t]


def _parse_product_query(query: str) -> list:
    """Список токенов; каждый токен — список допустимых вариантов (OR)."""
    out = []
    for raw in _split_tokens(query):
        canon = _COLOR_CANON.get(raw)
        if canon:
            # Любой синоним цвета из словаря с тем же каноном.
            alts = sorted({k for k, v in _COLOR_CANON.items() if v == canon} | {canon})
            out.append(alts)
        else:
            out.append([raw])
    return out


def _token_hits_product(tok: str, vc: str, name: str, nm: str, segments: set) -> bool:
    if not tok:
        return False
    if tok == nm or tok in segments:
        return True
    if tok in vc or tok in name:
        return True
    # Артикул продавца: «042» → 042_S11_middle_gold_O
    if vc.startswith(tok) or vc.startswith(tok + "_"):
        return True
    for seg in segments:
        if seg == tok or seg.startswith(tok):
            return True
    # «42» тоже находит «042_…»
    if tok.isdigit():
        for width in (3, 4):
            padded = tok.zfill(width)
            if padded != tok and _token_hits_product(padded, vc, name, nm, segments):
                return True
    return False


def _product_matches_query(product: dict, token_alts: list) -> bool:
    if not token_alts:
        return True
    vc = _norm_txt(str(product.get("vendor_code") or ""))
    name = _norm_txt(str(product.get("name") or ""))
    nm = str(product.get("nm_id") or "")
    segments = set(_split_tokens(vc)) | set(_split_tokens(name))
    if nm:
        segments.add(nm)
    # Цветовой сегмент артикула тоже канонизируем (gold/золотые → gold).
    expanded = set(segments)
    for seg in list(segments):
        canon = _COLOR_CANON.get(seg)
        if canon:
            expanded.add(canon)
    for alts in token_alts:
        if not any(_token_hits_product(a, vc, name, nm, expanded) for a in alts):
            return False
    return True


# ---------- инструменты (только чтение) ----------

def tool_products(query: str = "", only_low_cover: bool = False, limit: int = 15) -> dict:
    """Полные календарные периоды и полный остаток по всем складам WB."""
    data = _API["wb_products"]() or {}
    items = data.get("products") or []
    token_alts = _parse_product_query(query)

    rows = []
    for p in items:
        if not _product_matches_query(p, token_alts):
            continue
        s7 = int(p.get("sales_7d") or 0)
        stock = int(p.get("stock") or 0)
        per_day = s7 / 7.0
        cover = round(stock / per_day, 1) if per_day > 0 else None
        yest = int(p.get("sales_yesterday") or 0)
        s28 = int(p.get("sales_28d") or 0)
        rows.append({
            "vendor_code": p.get("vendor_code"),
            "nm_id": p.get("nm_id"),
            "name": p.get("name"),
            "client_price": p.get("client_price"),
            # Полный остаток FBW+FBS по всем складам (не урезанный «Рост продаж»).
            "stock_total": stock,
            "warehouse_count": p.get("warehouse_count"),
            "channels": p.get("channels"),
            # Это заказы (orders), не выкупы. Периоды — полные календарные сутки МСК.
            "orders_yesterday_full": yest,
            "orders_7d": s7,
            "orders_28d": s28,
            "days_cover": cover,
        })

    if only_low_cover:
        rows = [r for r in rows if r["days_cover"] is not None and r["days_cover"] < 14]
        rows.sort(key=lambda r: (r["days_cover"], -r["orders_7d"]))
    else:
        rows.sort(key=lambda r: -r["orders_7d"])

    out = {
        "matched": len(rows),
        "query": (query or "").strip() or None,
        "items": rows[: max(1, min(int(limit or 15), 40))],
        "stock_updated_at": data.get("stock_updated_at"),
        "prices_updated_at": data.get("prices_updated_at"),
        "orders_updated_at": data.get("sales_updated_at"),
        "period_note": (
            "orders_yesterday_full — полные вчерашние сутки по Москве; "
            "orders_7d / orders_28d включают сегодняшний неполный день. "
            "stock_total — сумма по всем складам."
        ),
        "query_hint": (
            "Два идентификатора одной карточки: артикул продавца (vendor_code) и артикул WB "
            "(nm_id). В query — любой из них, либо коротко «042 голд» / «042 gold»."
        ),
    }
    # Без заказов days_cover посчитать не из чего, и «ничего не заканчивается»
    # было бы враньём — пусть модель скажет правду.
    if not any((p.get("sales_7d") or 0) > 0 for p in items):
        out["warning"] = ("Данные о заказах ещё не загрузились после перезапуска сервера, "
                          "поэтому запас в днях посчитать нельзя. Остатки при этом верные. "
                          "Обновление занимает пару минут, можно переспросить позже.")
        out["orders_available"] = False
    else:
        out["orders_available"] = True
    return out


def _msk_now():
    """Сервер живёт в UTC, а бизнес — в Москве. После 21:00 MSK это разные даты."""
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Europe/Moscow")).replace(tzinfo=None)
    except Exception:
        return datetime.utcnow() + timedelta(hours=3)


def tool_geography(days: int = 28, channel: str = "all",
                   date_from: str = None, date_to: str = None) -> dict:
    if not date_from and not date_to:
        today = _msk_now().date()
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
    """Срез «к этому часу» vs такой же час прошлого периода — не полные сутки."""
    if period not in ("day", "week", "weeks2", "month"):
        period = "day"
    d = _API["sales_pace"](period=period) or {}
    articles = []
    for a in (d.get("articles") or [])[:15]:
        articles.append({
            "vendor_code": a.get("vendor_code"),
            "nm_id": a.get("nm_id"),
            "name": a.get("name"),
            "orders_current_window": int(a.get("orders_today") or 0),
            "orders_previous_same_hours": int(a.get("orders_yesterday") or 0),
            "orders_delta": a.get("orders_delta"),
            "cart_cr_current_window": a.get("cart_cr_today"),
            "cart_cr_previous_same_hours": a.get("cart_cr_yesterday"),
        })
    return {
        "period": d.get("period"),
        "period_name": d.get("period_name"),
        "label_current_window": d.get("label_cur"),
        "label_previous_same_hours": d.get("label_prev"),
        "as_of": d.get("as_of"),
        "time_cutoff": d.get("time_cutoff"),
        "mode_hint": d.get("mode_hint"),
        "updated_at": d.get("updated_at"),
        "syncing": d.get("syncing"),
        "window_warning": (
            "Это НЕ полные сутки. orders_previous_same_hours — заказы вчера "
            "только до того же часа, что сейчас (см. label_previous_same_hours). "
            "Для полного вчера, 7/28 дней и полного остатка вызывай products."
        ),
        "articles": articles,
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
            "Главный источник абсолютных цифр: полный остаток (stock_total по всем складам), "
            "заказы за полные вчерашние сутки (orders_yesterday_full), за 7 и 28 дней, "
            "цена, каналы FBW/FBS, days_cover. Используй для «сколько вчера», «за неделю», "
            "«какой остаток», «что заканчивается», топа артикулов. Это заказы, не выкупы."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": (
                        "Поиск по артикулу продавца (042_S11_middle_gold_O), артикулу WB/nm_id "
                        "(941630654), названию. Можно коротко: «042 голд», «042 gold», «046 серый». "
                        "Все слова должны совпасть; цвет по-русски понимается."
                    ),
                },
                "only_low_cover": {"type": "boolean", "description": "Только то, чего хватит меньше чем на 14 дней"},
                "limit": {"type": "integer", "description": "Сколько строк вернуть, максимум 40"},
            },
        },
    },
    {
        "name": "geography",
        "description": (
            "География заказов FBS и FBW: сводка, склады отгрузки, регионы и города "
            "назначения, топ артикулов. Данные из ленты заказов и Statistics API. "
            "С Statistics API в «city» часто лежит область, не населённый пункт."
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
        "description": (
            "Только сравнение темпа: текущее окно часов против такого же окна прошлого "
            "периода (сегодня до HH:MM vs вчера до HH:MM). НЕ используй для «сколько было "
            "вчера целиком», полного остатка или топа за 7 дней — для этого products. "
            "Поля orders_previous_same_hours нельзя называть «продажами за вчера» без "
            "оговорки про час."
        ),
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

SYSTEM_PROMPT_BASE = (
    "Ты помощник селлера Wildberries и Ozon. Категория — смарт-часы. "
    "Отвечаешь в телеграме, поэтому коротко: несколько предложений или компактный список, "
    "без заголовков и таблиц.\n\n"
    "Всегда бери цифры из инструментов, никогда не выдумывай. Перед ответом с цифрами "
    "вызови нужный инструмент. Если инструмент вернул пусто или устаревшие данные — "
    "скажи об этом прямо.\n\n"
    "Как выбирать инструмент:\n"
    "• products — остаток, полное вчера, 7/28 дней, что заканчивается, топ артикулов.\n"
    "• sales_pace — только «как идёт сегодня против вчера к этому часу».\n"
    "• geography / fbs_speed / own_warehouse — по теме вопроса.\n\n"
    "Важные ловушки:\n"
    "• Цифры в products — это заказы, не выкупы. Говори «заказов», если не уверена.\n"
    "• Из sales_pace никогда не говори «вчера N», если не добавила «до HH:MM». "
    "Полное вчера — только orders_yesterday_full из products.\n"
    "• Остаток бери только из stock_total (products), не из других отчётов.\n"
    "• Артикул продавца (vendor_code, например 042_S11_middle_gold_O) и артикул WB "
    "(nm_id, например 941630654) — разные поля одной карточки; в ответе при возможности "
    "называй оба. В query можно передать любой из них или коротко «042 голд».\n\n"
    "Что важно знать про экономику: комиссия FBS 37%, FBW 32,5%. С 7 августа 2026 действует "
    "коэффициент kC за скорость отгрузки: до 13 часов минус 5 пунктов, от 13 до 42 часов "
    "минус 3,5 пункта, от 42 до 48 базовая ставка, дальше штрафы. Отгрузка считается от "
    "создания заказа до сканирования на складе.\n\n"
    "У тебя только чтение. Если просят что-то изменить — цену, остаток, карточку — объясни, "
    "что это делается на сайте дашборда, и не пытайся."
)

WEEKDAYS = ("понедельник", "вторник", "среда", "четверг", "пятница", "суббота", "воскресенье")


def _system_prompt() -> str:
    now = _msk_now()
    return (
        f"{SYSTEM_PROMPT_BASE}\n\n"
        f"Сейчас {now.strftime('%d.%m.%Y %H:%M')} по Москве, {WEEKDAYS[now.weekday()]}. "
        f"«Сегодня» это {now.strftime('%d.%m.%Y')}, «вчера» — "
        f"{(now - timedelta(days=1)).strftime('%d.%m.%Y')}. Не путай их местами: "
        f"данные за сегодняшний день всегда неполные, день ещё идёт."
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
    "/stock 042 голд — то же по номеру и цвету\n"
    "/sales — топ заказов за 7 дней (полные сутки)\n"
    "/geo — география заказов за 28 дней\n"
    "/geo 7 fbs — то же за 7 дней только по FBS\n"
    "/fbs — скорость отгрузки и экономика kC\n"
    "/sklad — свой склад\n"
    "/reset — забыть контекст разговора\n"
    "\nМожно писать текстом или <b>голосом</b> — распознаю и отвечу.\n"
)

HELP_LLM_OFF = (
    "\nВопросы обычным текстом пока выключены: нужен OPENAI_API_KEY или ANTHROPIC_API_KEY. "
    "Команды выше работают без ключа."
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
        out.append(f"<code>{_esc(r.get('vendor_code'))}</code> · nm {_esc(r.get('nm_id'))}")
        out.append(f"{cover_s} · остаток {_num(r.get('stock_total'))} на {_num(r.get('warehouse_count'))} скл "
                   f"· {_num(r.get('orders_7d'))} зак. за 7 дн")
    out.append(f"\nОстатки обновлены: {_esc(data.get('stock_updated_at') or '—')}")
    return "\n".join(out)


def cmd_sales() -> str:
    data = tool_products(limit=12)
    items = data.get("items") or []
    if not items:
        return "Заказов не вижу — возможно, данные ещё синхронизируются."

    out = ["<b>Топ заказов за 7 дней</b>", ""]
    for i, r in enumerate(items, 1):
        out.append(f"{i}. <code>{_esc(r.get('vendor_code'))}</code> · nm {_esc(r.get('nm_id'))}")
        out.append(f"вчера {_num(r.get('orders_yesterday_full'))} · 7 дн {_num(r.get('orders_7d'))} "
                   f"· 28 дн {_num(r.get('orders_28d'))} · остаток {_num(r.get('stock_total'))}")
    out.append(f"\nЗаказы обновлены: {_esc(data.get('orders_updated_at') or '—')}")
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


def _ask_anthropic(chat_id: int, working: list) -> tuple:
    headers = {
        "x-api-key": _anthropic_key(),
        "anthropic-version": ANTHROPIC_VERSION,
        "content-type": "application/json",
    }
    force_tools = True
    for _ in range(TOOL_STEPS_LIMIT):
        payload = {
            "model": _anthropic_model(),
            "max_tokens": 1500,
            "system": _system_prompt(),
            "tools": TOOL_SCHEMAS,
            "messages": working,
        }
        if force_tools:
            payload["tool_choice"] = {"type": "any"}
        try:
            r = httpx.post(ANTHROPIC_URL, headers=headers, timeout=120, json=payload)
        except Exception as e:
            return None, f"Не достучался до Anthropic: {e}"
        if r.status_code != 200:
            return None, f"Anthropic вернул {r.status_code}. {r.text[:300]}"

        data = r.json()
        blocks = data.get("content") or []
        force_tools = False
        if data.get("stop_reason") == "tool_use":
            working.append({"role": "assistant", "content": blocks})
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
            working.append({"role": "user", "content": results})
            continue

        text = "".join(b.get("text") or "" for b in blocks if b.get("type") == "text").strip()
        return text, None
    return None, "Слишком много шагов, переспроси конкретнее."


def _openai_tools() -> list:
    return [{
        "type": "function",
        "function": {
            "name": t["name"],
            "description": t["description"],
            "parameters": t["input_schema"],
        },
    } for t in TOOL_SCHEMAS]


def _openai_call(body: dict):
    """Поколения моделей ждут разные параметры: у 5.x это max_completion_tokens и
    reasoning_effort=none для функций, у 4.x — max_tokens и никакого reasoning.
    Подбираем по ответу самого API и запоминаем на процесс."""
    global _OPENAI_TOKEN_PARAM, _OPENAI_EXTRA
    headers = {
        "Authorization": f"Bearer {_openai_key()}",
        "Content-Type": "application/json",
    }
    r = None
    for _ in range(3):
        payload = dict(body)
        payload[_OPENAI_TOKEN_PARAM] = 1500
        payload.update(_OPENAI_EXTRA)
        r = httpx.post(OPENAI_URL, headers=headers, timeout=120, json=payload)
        if r.status_code != 400:
            return r

        try:
            err = (r.json() or {}).get("error") or {}
        except Exception:
            return r
        param = err.get("param") or ""

        if param in ("max_tokens", "max_completion_tokens"):
            _OPENAI_TOKEN_PARAM = (
                "max_tokens" if _OPENAI_TOKEN_PARAM == "max_completion_tokens"
                else "max_completion_tokens"
            )
            logger.info(f"openai: переключаюсь на {_OPENAI_TOKEN_PARAM}")
            continue
        if param in _OPENAI_EXTRA:
            _OPENAI_EXTRA = {k: v for k, v in _OPENAI_EXTRA.items() if k != param}
            logger.info(f"openai: модель не принимает {param}, убираю")
            continue
        return r
    return r


def _ask_openai(chat_id: int, working: list) -> tuple:
    messages = [{"role": "system", "content": _system_prompt()}] + working
    force_tools = True

    for _ in range(TOOL_STEPS_LIMIT):
        body = {
            "model": _openai_model(),
            "tools": _openai_tools(),
            "messages": messages,
        }
        # Первый ход — обязательно инструмент: иначе модель путает окна «Рост продаж»
        # с полным вчера из каталога и отвечает цифрами на память.
        if force_tools:
            body["tool_choice"] = "required"
        try:
            r = _openai_call(body)
        except Exception as e:
            return None, f"Не достучался до OpenAI: {e}"
        if r.status_code != 200:
            return None, f"OpenAI вернул {r.status_code}. {r.text[:300]}"

        msg = ((r.json().get("choices") or [{}])[0]).get("message") or {}
        calls = msg.get("tool_calls") or []
        force_tools = False
        if calls:
            messages.append(msg)
            for c in calls:
                _typing(chat_id)
                fn = c.get("function") or {}
                try:
                    args = json.loads(fn.get("arguments") or "{}")
                except Exception:
                    args = {}
                out = _run_tool(fn.get("name"), args)
                messages.append({
                    "role": "tool",
                    "tool_call_id": c.get("id"),
                    "content": json.dumps(out, ensure_ascii=False)[:20000],
                })
            continue

        return (msg.get("content") or "").strip(), None
    return None, "Слишком много шагов, переспроси конкретнее."


def ask_llm(chat_id: int, question: str) -> str:
    provider = _provider()
    if not provider:
        return ("Вопросы текстом выключены: не задан ни OPENAI_API_KEY, ни ANTHROPIC_API_KEY. "
                "Команды из /help работают без ключа.")

    history = _history(chat_id)
    working = history + [{"role": "user", "content": question}]
    runner = _ask_openai if provider == "openai" else _ask_anthropic
    text, err = runner(chat_id, list(working))

    if err:
        low = err.lower()
        if "404" in low or "does not exist" in low or "model_not_found" in low:
            err += "\n\nПохоже на неверное имя модели. Список доступных — /api/llm-models на сайте."
        return _esc(err)
    if not text:
        return "Пустой ответ."

    # В историю кладём только чистые текстовые реплики: формат tool-вызовов
    # у провайдеров разный, и смешивать их между переключениями нельзя.
    _remember(chat_id, history + [
        {"role": "user", "content": question},
        {"role": "assistant", "content": text},
    ])
    return _esc(text)


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
            reply = HELP + ("" if _provider() else HELP_LLM_OFF)
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
    if not chat_id:
        return

    text = (msg.get("text") or "").strip()
    media = _extract_voice_media(msg)
    if not text and not media:
        return

    if not allowed:
        _send(chat_id, f"Белый список пуст. Добавь в переменные окружения:\n"
                       f"<code>TELEGRAM_ALLOWED_CHAT_IDS={chat_id}</code>")
        return
    if chat_id not in allowed:
        logger.warning(f"telegram: отклонён chat_id {chat_id}")
        return

    if media and not text:
        if not _openai_key():
            _send(chat_id, "Голосовые пока не разбираю: нужен OPENAI_API_KEY (Whisper). "
                           "Текстом или командами можно и без него.")
            return
        _typing(chat_id)
        try:
            raw, filename = _download_tg_file(media["file_id"])
            text = _transcribe_audio(raw, filename)
        except Exception as e:
            logger.error(f"telegram voice: {e}")
            _send(chat_id, f"Не разобрал голос: {_esc(e)}")
            return
        if not text:
            _send(chat_id, "Тишина или не разобрал — повтори голосом или напиши текстом.")
            return
        # Показываем, что услышали, чтобы сразу было видно опечатки Whisper.
        _send(chat_id, f"Услышал: <i>{_esc(text)}</i>")

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


def list_models(contains: str = "") -> dict:
    """Список моделей, доступных настроенному ключу. Нужен, чтобы не гадать с id."""
    provider = _provider()
    if not provider:
        return {"error": "ключ не задан: ни OPENAI_API_KEY, ни ANTHROPIC_API_KEY"}

    if provider == "openai":
        url = "https://api.openai.com/v1/models"
        headers = {"Authorization": f"Bearer {_openai_key()}"}
        current = _openai_model()
    else:
        url = "https://api.anthropic.com/v1/models?limit=100"
        headers = {"x-api-key": _anthropic_key(), "anthropic-version": ANTHROPIC_VERSION}
        current = _anthropic_model()

    try:
        r = httpx.get(url, headers=headers, timeout=30)
    except Exception as e:
        return {"provider": provider, "error": str(e)}
    if r.status_code != 200:
        return {"provider": provider, "error": f"HTTP {r.status_code}: {r.text[:300]}"}

    ids = sorted({m.get("id") for m in (r.json().get("data") or []) if m.get("id")})
    needle = (contains or "").strip().lower()
    if needle:
        ids = [i for i in ids if needle in i.lower()]
    return {
        "provider": provider,
        "current_model": current,
        "current_available": current in ids if not needle else None,
        "count": len(ids),
        "models": ids,
    }


def bot_status() -> dict:
    """Диагностика без утечки секретов: что настроено и жив ли поток опроса."""
    alive = any(t.name == "telegram-bot" and t.is_alive() for t in threading.enumerate())
    token = _token()
    return {
        "token_set": bool(token),
        "token_tail": token[-4:] if token else None,
        "allowed_chat_ids": sorted(_allowed_ids()),
        "whitelist_configured": bool(_allowed_ids()),
        "llm_provider": _provider() or None,
        "llm_enabled": bool(_provider()),
        "model": {"openai": _openai_model, "anthropic": _anthropic_model}[_provider()]()
                 if _provider() else None,
        "voice_stt": bool(_openai_key()),
        "transcribe_model": _transcribe_model() if _openai_key() else None,
        "started": _STARTED,
        "polling": alive,
        "sources": sorted(_API.keys()),
    }


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
