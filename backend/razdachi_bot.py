"""Бот «Раздачи» (@razdachi_team_bot) по прежней схеме.

Новая раздача: название, артикул, «По ключу» или «Через конкурента», план,
«Поставить». Доска, «Я раздала», закрытые. Ставят задачи Елизавета, Диля и Босс.
Остальные только отмечают свои штуки.

Живёт в том же процессе, что и дашборд, и читает RAZDACHI_BOT_TOKEN.
Это другой токен, не TELEGRAM_BOT_TOKEN. Окно старой программы на компьютере
должно быть закрыто: Телеграм пускает только одного слушателя.
"""

from __future__ import annotations

import hashlib
import hmac
import html
import json
import logging
import os
import re
import threading
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx

logger = logging.getLogger("razdachi")

MOSCOW = ZoneInfo("Europe/Moscow")
STATE_KEY = "razdachi_bot_state"
STATE_PATH = Path(__file__).resolve().parent / "data" / "razdachi_state.json"
SETTER_NAMES = ("елизавета", "диля", "диле", "босс", "ярослав")
MENU = ("Новая раздача", "Доска", "Кто раздал", "Команда", "Закрытые", "Имя")
PLAN_MAX = 500
NM_RE = re.compile(r"catalog/(\d{6,12})")
USERNAME_RE = re.compile(r"[A-Za-z][A-Za-z0-9_]{2,31}")

_LOCK = threading.Lock()
_STORE: "Store | None" = None
_SESSIONS: dict = {}
_STARTED = False
_POLLING = False
_CONFLICT = False
_USERNAME = ""
_MODE = ""


class Conflict(Exception):
    pass


def esc(value) -> str:
    return html.escape(str(value or ""), quote=False)


def moscow_now() -> datetime:
    return datetime.now(MOSCOW)


def normalize_name(value: str) -> str:
    return " ".join((value or "").strip().split()).casefold()


def is_setter(name: str) -> bool:
    return canon(name) in SETTER_NAMES


def canon(value: str) -> str:
    text = (value or "").replace("\xa0", " ").replace("ё", "е").replace("Ё", "е")
    return " ".join(text.split()).casefold()


def menu_key(value: str) -> str:
    key = canon(value)
    if "новая раздача" in key:
        return "Новая раздача"
    aliases = {
        "доска": "Доска",
        "кто раздал": "Кто раздал",
        "команда": "Команда",
        "закрытые": "Закрытые",
        "имя": "Имя",
    }
    return aliases.get(key, "")


def is_username(value: str) -> bool:
    text = (value or "").strip()
    if text.startswith("@"):
        return True
    return bool(USERNAME_RE.fullmatch(text))


def clean_name(value: str) -> str:
    name = " ".join((value or "").replace("\n", " ").split())
    if len(name) < 2 or len(name) > 24:
        return ""
    if name in MENU or name.startswith("/"):
        return ""
    return name


def command_of(value: str) -> str:
    text = (value or "").strip()
    if not text.startswith("/"):
        return ""
    return text.split()[0].split("@", 1)[0].lower()


def parse_nm(value: str) -> str:
    text = (value or "").strip()
    if re.fullmatch(r"\d{6,12}", text):
        return text
    found = NM_RE.search(text)
    return found.group(1) if found else ""


def parse_count(value: str) -> int | None:
    text = (value or "").strip()
    if not re.fullmatch(r"\d{1,4}", text):
        return None
    return int(text)


def same_day(stamp: str, now: datetime) -> bool:
    if not stamp:
        return False
    try:
        moment = datetime.fromisoformat(stamp)
    except ValueError:
        return False
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=ZoneInfo("UTC"))
    return moment.astimezone(MOSCOW).date() == now.astimezone(MOSCOW).date()


def total_qty(task: dict) -> int:
    return sum(int(mark.get("qty") or 0) for mark in task.get("marks") or [])


def qty_of(task: dict, chat_id: int) -> int:
    for mark in task.get("marks") or []:
        if int(mark.get("chat_id") or 0) == int(chat_id):
            return int(mark.get("qty") or 0)
    return 0


def others_qty(task: dict, chat_id: int) -> int:
    return total_qty(task) - qty_of(task, chat_id)


def channel_line(task: dict, *, short: bool) -> str:
    query = task.get("query") or ""
    if task.get("channel") == "keyword":
        return f"ключ {query}" if short else f"По ключу: {query}"
    return f"конк. {query}" if short else f"Через конкурента: {query}"


def board_line(task: dict) -> str:
    done = total_qty(task)
    plan = int(task.get("plan") or 0)
    prefix = "✓ " if plan and done >= plan else ""
    return f"{prefix}{task.get('title') or ''} {done}/{plan} {channel_line(task, short=True)}".strip()


def wb_link(nm: str) -> str:
    safe = esc(nm)
    return f'<a href="https://www.wildberries.ru/catalog/{safe}/detail.aspx">{safe}</a>'


def plan_line(task: dict) -> str:
    plan = int(task.get("plan") or 0)
    done = total_qty(task)
    if done <= 0:
        return f"План {plan} · пока 0"
    return f"План {plan} · раздали {done} · ещё {max(plan - done, 0)}"


def channel_html(task: dict) -> str:
    query = str(task.get("query") or "")
    if task.get("channel") == "keyword":
        return f"По ключу: {esc(query)}"
    if re.fullmatch(r"\d{6,12}", query):
        return f"Через конкурента: {wb_link(query)}"
    return f"Через конкурента: {esc(query)}"


def notice_text(task: dict, header: str, *, show_from: bool = False) -> str:
    """Короткая строка в чат: что случилось, название, артикул, канал и план."""
    lines = [
        esc(header),
        esc(task.get("title")),
        wb_link(str(task.get("nm") or "")),
        channel_html(task),
        plan_line(task),
    ]
    if show_from:
        lines.append(f"От: {esc(task.get('setter'))}")
    return "\n".join(lines)


def posted_text(task: dict, header: str | None = None) -> str:
    """Карточка по схеме: название над артикулом, канал, кто поставил, план, кто отметил."""
    if header is None:
        header = "Закрыта" if task.get("status") == "closed" else "Новая раздача"
    lines = [
        esc(header),
        esc(task.get("title")),
        wb_link(str(task.get("nm") or "")),
        channel_html(task),
        f"От: {esc(task.get('setter'))}",
        plan_line(task),
    ]
    marks = [mark for mark in task.get("marks") or [] if int(mark.get("qty") or 0) > 0]
    if marks:
        lines.extend(f"{esc(mark.get('name'))} {int(mark.get('qty') or 0)}" for mark in marks)
    else:
        lines.append("Пока никто не отметил")
    return "\n".join(lines)


def detail_text(task: dict) -> str:
    lines = []
    if task.get("status") == "closed":
        lines.append("Закрыта")
    lines.extend([
        esc(task.get("title")),
        wb_link(str(task.get("nm") or "")),
        channel_html(task),
        f"От: {esc(task.get('setter'))}",
        "",
        plan_line(task),
    ])
    marks = [mark for mark in task.get("marks") or [] if int(mark.get("qty") or 0) > 0]
    if marks:
        lines.extend(f"{esc(mark.get('name'))} {int(mark.get('qty') or 0)}" for mark in marks)
    else:
        lines.append("Пока никто не отметил")
    return "\n".join(lines)


def card_text(task: dict, *, preview: bool = False) -> str:
    if preview:
        return notice_text(task, "Новая раздача", show_from=True)
    return detail_text(task)


def open_markup(task: dict) -> dict:
    return inline([[("Открыть", f"card:{task['id']}")]])


def over_plan(plan: int) -> str:
    return f"В плане {plan}. Больше не записываю."


def clip(value: str, limit: int = 60) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


def reply_kb(name: str) -> dict:
    if is_setter(name):
        rows = [["Доска", "Кто раздал"], ["Новая раздача", "Команда"], ["Закрытые", "Имя"]]
    else:
        rows = [["Доска", "Закрытые"], ["Имя"]]
    return {
        "keyboard": [[{"text": item} for item in row] for row in rows],
        "resize_keyboard": True,
        "is_persistent": True,
    }


def inline(rows: list) -> dict:
    return {
        "inline_keyboard": [
            [{"text": text, "callback_data": data} for text, data in row]
            for row in rows
        ]
    }


def chunk(items: list, size: int) -> list:
    return [items[i:i + size] for i in range(0, len(items), size)]


def send(chat_id: int, text: str, markup=None, remember: str | None = None) -> dict:
    return {"op": "send", "chat_id": chat_id, "text": text, "markup": markup, "remember": remember}


def edit(chat_id: int, message_id: int, text: str, markup=None, remember: str | None = None) -> dict:
    return {
        "op": "edit",
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "markup": markup,
        "remember": remember,
    }


def alert(text: str = "", show: bool = False) -> dict:
    return {"op": "alert", "text": text, "show": show}


def _session(sessions: dict, chat_id: int) -> dict:
    found = sessions.get(chat_id)
    if not found:
        found = {"step": "", "draft": {}}
        sessions[chat_id] = found
    return found


def _clear(sessions: dict, chat_id: int) -> None:
    sessions[chat_id] = {"step": "", "draft": {}}


def sessions_from(raw) -> dict:
    found = {}
    if not isinstance(raw, dict):
        return found
    for key, value in raw.items():
        try:
            chat_id = int(key)
        except (TypeError, ValueError):
            continue
        if isinstance(value, dict):
            found[chat_id] = {
                "step": str(value.get("step") or ""),
                "draft": dict(value.get("draft") or {}),
            }
    return found


class Store:
    def __init__(self, data: dict | None = None, path: Path | None = None, persist: bool = False):
        self.path = path
        self.persist = persist
        self.now = moscow_now
        raw = data or {"users": {}, "tasks": [], "seq": 0}
        self.users = {str(key): value for key, value in (raw.get("users") or {}).items()}
        self.tasks = list(raw.get("tasks") or [])
        self.seq = int(raw.get("seq") or 0)
        self.sessions = sessions_from(raw.get("sessions"))

    def to_dict(self) -> dict:
        return {
            "users": self.users,
            "tasks": self.tasks,
            "seq": self.seq,
            "sessions": {str(key): value for key, value in self.sessions.items()},
        }

    def pull(self) -> None:
        """Подтянуть доску и черновик. Иначе второй процесс не понимает кнопку «Новая раздача»."""
        fresh = _remote_load()
        if not isinstance(fresh, dict):
            return
        self.users = {str(key): value for key, value in (fresh.get("users") or {}).items()}
        self.tasks = list(fresh.get("tasks") or [])
        self.seq = int(fresh.get("seq") or 0)
        self.sessions = sessions_from(fresh.get("sessions"))

    def user(self, chat_id: int) -> dict | None:
        return self.users.get(str(chat_id))

    def set_name(self, chat_id: int, name: str) -> dict:
        record = {"chat_id": int(chat_id), "name": name}
        self.users[str(chat_id)] = record
        self.save()
        return record

    def task(self, task_id: str) -> dict | None:
        for item in self.tasks:
            if item.get("id") == task_id:
                return item
        return None

    def create_task(self, *, chat_id: int, name: str, draft: dict) -> dict:
        self.seq += 1
        task = {
            "id": f"t{self.seq}",
            "title": draft["title"],
            "nm": draft["nm"],
            "channel": draft["channel"],
            "query": draft["query"],
            "plan": int(draft["plan"]),
            "setter": name,
            "setter_chat": int(chat_id),
            "status": "open",
            "marks": [],
            "messages": {},
            "created_at": self.now().isoformat(),
        }
        self.tasks.append(task)
        self.tasks = self.tasks[-400:]
        self.save()
        return task

    def remember(self, task_id: str, chat_id: int, message_id: int) -> None:
        task = self.task(task_id)
        if not task or not message_id:
            return
        task.setdefault("messages", {})[str(chat_id)] = int(message_id)
        self.save()

    def forget_message(self, task_id: str, chat_id: int) -> None:
        task = self.task(task_id)
        if not task:
            return
        task.get("messages", {}).pop(str(chat_id), None)
        self.save()

    def set_mark(self, task: dict, chat_id: int, name: str, qty: int) -> None:
        qty = max(0, int(qty))
        marks = task.setdefault("marks", [])
        if qty == 0:
            task["marks"] = [mark for mark in marks if int(mark.get("chat_id") or 0) != int(chat_id)]
        else:
            for mark in marks:
                if int(mark.get("chat_id") or 0) == int(chat_id):
                    mark["qty"] = qty
                    mark["name"] = name
                    mark["at"] = self.now().isoformat()
                    break
            else:
                marks.append({
                    "chat_id": int(chat_id),
                    "name": name,
                    "qty": qty,
                    "at": self.now().isoformat(),
                })
        self.save()

    def save(self) -> None:
        if not self.persist:
            return
        payload = json.dumps(self.to_dict(), ensure_ascii=False)
        if self.path:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.path.write_text(payload, encoding="utf-8")
        _remote_save(payload)


def load_store() -> Store:
    data = _remote_load()
    if data is None and STATE_PATH.exists():
        try:
            data = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("razdachi: локальное состояние не прочиталось: %s", exc)
            data = None
    return Store(data if isinstance(data, dict) else None, path=STATE_PATH, persist=True)


def _remote_headers() -> dict | None:
    url = (os.getenv("SUPABASE_URL") or "").strip().rstrip("/")
    key = (os.getenv("SUPABASE_KEY") or "").strip()
    if not url or not key:
        return None
    return {
        "url": url,
        "headers": {
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates",
        },
    }


def _remote_load() -> dict | None:
    spec = _remote_headers()
    if not spec:
        return None
    try:
        response = httpx.get(
            f"{spec['url']}/rest/v1/settings?key=eq.{STATE_KEY}&select=value",
            headers=spec["headers"],
            timeout=15,
        )
        if not response.is_success or not response.json():
            return None
        raw = response.json()[0].get("value")
        if isinstance(raw, str):
            raw = json.loads(raw)
        return raw if isinstance(raw, dict) else None
    except Exception as exc:
        logger.warning("razdachi: не прочитал состояние: %s", exc)
        return None


def _remote_save(payload: str) -> None:
    spec = _remote_headers()
    if not spec:
        return
    try:
        response = httpx.post(
            f"{spec['url']}/rest/v1/settings?on_conflict=key",
            headers=spec["headers"],
            json={
                "key": STATE_KEY,
                "value": payload,
                "updated_at": datetime.now(ZoneInfo("UTC")).isoformat(),
            },
            timeout=15,
        )
        if not response.is_success:
            logger.warning("razdachi: не сохранил состояние: %s", response.status_code)
    except Exception as exc:
        logger.warning("razdachi: не сохранил состояние: %s", exc)


def card_markup(task: dict, name: str) -> dict:
    task_id = task["id"]
    if task.get("status") == "closed":
        if is_setter(name):
            return inline([[("Вернуть в работу", f"back:{task_id}")]])
        return inline([])
    rows = [[("Я раздала", f"give:{task_id}")]]
    if is_setter(name):
        rows.append([("Изменить", f"edit:{task_id}"), ("Закрыть", f"close:{task_id}")])
        if total_qty(task) == 0:
            rows.append([("Удалить", f"del:{task_id}")])
    return inline(rows)


def number_rows(task_id: str, start: int, end: int, prefix: str, extra: tuple | None = None) -> dict:
    buttons = [(str(number), f"{prefix}:{task_id}:{number}") for number in range(start, end + 1)]
    rows = chunk(buttons, 4)
    if extra:
        rows.append([extra])
    return inline(rows)


def give_markup(task: dict) -> dict | None:
    left = int(task["plan"]) - total_qty(task)
    if left <= 0:
        return None
    shown = min(left, 8)
    extra = ("Другое число", f"addmore:{task['id']}") if left > 8 else None
    rows = number_rows(task["id"], 1, shown, "add", ("Мои", f"mine:{task['id']}"))
    if extra:
        rows["inline_keyboard"].append([{"text": extra[0], "callback_data": extra[1]}])
    return rows


def mine_markup(task: dict, chat_id: int) -> dict:
    allowed = int(task["plan"]) - others_qty(task, chat_id)
    shown = min(max(allowed, 0), 8)
    extra = ("Другое число", f"minemore:{task['id']}") if allowed > 8 else None
    rows = number_rows(task["id"], 0, shown, "set", extra)
    return rows


def refresh(store: Store, task: dict) -> list:
    actions = []
    for chat_key, message_id in list((task.get("messages") or {}).items()):
        try:
            chat_id = int(chat_key)
        except (TypeError, ValueError):
            continue
        user = store.user(chat_id)
        name = user["name"] if user else ""
        actions.append(edit(chat_id, int(message_id), posted_text(task), card_markup(task, name)))
    return actions


def hello_actions(store: Store, sessions: dict, chat_id: int) -> list:
    user = store.user(chat_id)
    if not user:
        _clear(sessions, chat_id)
        _session(sessions, chat_id)["step"] = "name"
        return [send(chat_id, "Напиши имя, как подписать в отметках. Например: Маша.")]
    _clear(sessions, chat_id)
    return [send(chat_id, f"Привет, {esc(user['name'])}.", reply_kb(user["name"]))]


def accept_name(store: Store, sessions: dict, chat_id: int, text: str) -> list:
    if is_username(text):
        return [send(chat_id, "Юзернейм не приму. Напиши имя, например Маша.")]
    name = clean_name(text)
    if not name:
        return [send(chat_id, "Напиши имя, как подписать в отметках. Например: Маша. Юзернейм не приму.")]
    pending = (_session(sessions, chat_id).get("draft") or {}).get("next")
    store.set_name(chat_id, name)
    _clear(sessions, chat_id)
    actions = [send(chat_id, f"Привет, {esc(name)}.", reply_kb(name))]
    if pending == "new" and is_setter(name):
        sessions[chat_id] = {"step": "title", "draft": {}}
        actions.append(send(chat_id, "Своё название, например 046 Black."))
    elif pending == "new":
        actions.append(send(chat_id, "Ставить задачи могут Елизавета, Диля и Босс."))
    return actions


def list_tasks(store: Store, chat_id: int, status: str) -> list:
    tasks = [task for task in store.tasks if task.get("status") == status]
    tasks.sort(key=lambda task: task.get("created_at") or "", reverse=True)
    if not tasks:
        text = "Доска пустая." if status == "open" else "Закрытых нет."
        return [send(chat_id, text)]
    user = store.user(chat_id)
    name = user["name"] if user else ""
    actions = []
    for task in tasks[:20]:
        actions.append(send(
            chat_id,
            posted_text(task),
            card_markup(task, name),
            remember=task["id"],
        ))
    return actions


def today_text(store: Store) -> str:
    now = store.now()
    totals: dict[str, int] = {}
    for task in store.tasks:
        for mark in task.get("marks") or []:
            qty = int(mark.get("qty") or 0)
            if qty <= 0 or not same_day(mark.get("at") or "", now):
                continue
            name = mark.get("name") or ""
            totals[name] = totals.get(name, 0) + qty
    if not totals:
        return "Сегодня ещё никто не отметил."
    lines = ["Кто раздал сегодня"]
    for name, qty in sorted(totals.items(), key=lambda item: (-item[1], item[0].casefold())):
        lines.append(f"{esc(name)} — {qty}")
    return "\n".join(lines)


def team_text(store: Store) -> str:
    lines = ["Ставят задачи", "Елизавета, Диля, Босс (Ярослав)", "", "В боте сейчас"]
    people = sorted(store.users.values(), key=lambda user: user.get("name", "").casefold())
    if not people:
        lines.append("Пока никого.")
    else:
        for user in people:
            role = "ставит" if is_setter(user.get("name") or "") else "отмечает"
            lines.append(f"{esc(user.get('name'))} — {role}")
    return "\n".join(lines)


def help_text() -> str:
    return "\n".join([
        "/start — меню",
        "/board — доска",
        "/new — новая раздача (для тех, кто ставит задачи)",
        "/today — кто раздал сегодня",
        "/cancel — отменить черновик",
        "/help — это",
        "",
        "Новая раздача: название, артикул, «По ключу» или «Через конкурента», план, «Поставить».",
        "«Я раздала» прибавляет число к своим. «Мои» ставит свою цифру заново, 0 снимает отметку.",
        "Больше плана бот не записывает.",
    ])


def on_menu(store: Store, sessions: dict, chat_id: int, text: str) -> list:
    cmd = command_of(text)
    label = text.strip()
    if cmd == "/start":
        return hello_actions(store, sessions, chat_id)
    if cmd == "/help":
        return [send(chat_id, help_text())]
    if cmd == "/cancel":
        _clear(sessions, chat_id)
        user = store.user(chat_id)
        markup = reply_kb(user["name"]) if user else None
        return [send(chat_id, "Отменил.", markup)]

    if cmd == "/new" or label == "Новая раздача":
        user = store.user(chat_id)
        if not user:
            sessions[chat_id] = {"step": "name", "draft": {"next": "new"}}
            return [send(
                chat_id,
                "Новая раздача. Сначала напиши имя, как подписать в отметках. Например: Маша.",
            )]
        if not is_setter(user["name"]):
            return [send(
                chat_id,
                "Ставить задачи могут Елизавета, Диля и Босс (Ярослав).",
                reply_kb(user["name"]),
            )]
        sessions[chat_id] = {"step": "title", "draft": {}}
        return [send(
            chat_id,
            "Новая раздача. Своё название, например 046 Black.",
            reply_kb(user["name"]),
        )]

    user, missing = _require_user(store, sessions, chat_id)
    if missing:
        return missing
    if cmd == "/board" or label == "Доска":
        _clear(sessions, chat_id)
        return list_tasks(store, chat_id, "open")
    if label == "Закрытые":
        _clear(sessions, chat_id)
        return list_tasks(store, chat_id, "closed")
    if label == "Имя":
        sessions[chat_id] = {"step": "name", "draft": {}}
        return [send(chat_id, "Напиши имя, как подписать в отметках. Например: Маша.")]
    if cmd == "/today" or label == "Кто раздал":
        if not is_setter(user["name"]):
            return [send(chat_id, "Это смотрят те, кто ставит задачи.")]
        _clear(sessions, chat_id)
        return [send(chat_id, today_text(store))]
    if label == "Команда":
        if not is_setter(user["name"]):
            return [send(chat_id, "Команду смотрят те, кто ставит задачи.")]
        _clear(sessions, chat_id)
        return [send(chat_id, team_text(store))]
    return [send(chat_id, "Нажми кнопку внизу или /start.")]


def _require_user(store: Store, sessions: dict, chat_id: int) -> tuple[dict | None, list]:
    user = store.user(chat_id)
    if user:
        return user, []
    _session(sessions, chat_id)["step"] = "name"
    return None, [send(chat_id, "Сначала напиши имя, как подписать в отметках. Например: Маша.")]


def _query_prompt(channel: str) -> str:
    if channel == "keyword":
        return "Напиши само слово."
    return "Артикул конкурента: цифры или ссылка."


def _apply_query(draft: dict, text: str) -> str:
    if draft.get("channel") == "keyword":
        query = " ".join(text.split())
        if not query or query.startswith("/") or len(query) > 80:
            return ""
        draft["query"] = query
        return query
    nm = parse_nm(text)
    if not nm:
        return ""
    draft["query"] = nm
    return nm


def _finish_edit(store: Store, sessions: dict, chat_id: int, task: dict) -> list:
    _clear(sessions, chat_id)
    return refresh(store, task) + [send(chat_id, "Изменил.")]


def on_step(store: Store, sessions: dict, chat_id: int, text: str, message_id: int | None) -> list:
    user, missing = _require_user(store, sessions, chat_id)
    if missing:
        return accept_name(store, sessions, chat_id, text) if _session(sessions, chat_id)["step"] == "name" else missing
    step = _session(sessions, chat_id)
    draft = step["draft"]
    current = step["step"]

    if current == "name":
        return accept_name(store, sessions, chat_id, text)
    if current == "title":
        title = " ".join(text.split())
        if not title or title.startswith("/") or len(title) > 80:
            return [send(chat_id, "Своё название, например 046 Black.")]
        draft["title"] = title
        step["step"] = "nm"
        return [send(chat_id, "Артикул Wildberries: цифры или ссылка на карточку.")]
    if current == "nm":
        nm = parse_nm(text)
        if not nm:
            return [send(chat_id, "Нужны цифры артикула или ссылка на карточку.")]
        draft["nm"] = nm
        step["step"] = "channel"
        return [send(chat_id, "Дальше «По ключу» или «Через конкурента».", inline([
            [("По ключу", "ch:k"), ("Через конкурента", "ch:c")],
        ]))]
    if current == "channel":
        folded = text.casefold()
        if "ключ" in folded:
            channel = "keyword"
        elif "конкур" in folded:
            channel = "competitor"
        else:
            return [send(chat_id, "Нажми «По ключу» или «Через конкурента».")]
        draft["channel"] = channel
        step["step"] = "query"
        return [send(chat_id, _query_prompt(channel))]
    if current in ("query", "edit_query"):
        if not _apply_query(draft, text):
            hint = _query_prompt(draft.get("channel") or "")
            return [send(chat_id, hint)]
        if current == "edit_query":
            task = store.task(draft.get("task_id") or "")
            if not task:
                _clear(sessions, chat_id)
                return [send(chat_id, "Этой карточки уже нет.")]
            task["channel"] = draft["channel"]
            task["query"] = draft["query"]
            store.save()
            return _finish_edit(store, sessions, chat_id, task)
        step["step"] = "plan"
        return [send(chat_id, "Сколько штук в плане?")]
    if current == "plan":
        plan = parse_count(text)
        if plan is None or plan < 1 or plan > PLAN_MAX:
            return [send(chat_id, "Напиши число штук, например 3.")]
        draft["plan"] = plan
        step["step"] = "confirm"
        preview = {
            "title": draft["title"],
            "nm": draft["nm"],
            "channel": draft["channel"],
            "query": draft["query"],
            "plan": plan,
            "setter": user["name"],
            "marks": [],
            "status": "open",
        }
        return [send(chat_id, posted_text(preview, "Новая раздача"), inline([
            [("Поставить", "post")],
            [("Отмена", "drop")],
        ]))]
    if current == "confirm":
        return [send(chat_id, "Нажми «Поставить» или «Отмена».")]
    if current == "edit_title":
        task = store.task(draft.get("task_id") or "")
        title = " ".join(text.split())
        if not task:
            _clear(sessions, chat_id)
            return [send(chat_id, "Этой карточки уже нет.")]
        if not title or len(title) > 80:
            return [send(chat_id, "Напиши название. Например: 046 Black.")]
        task["title"] = title
        store.save()
        return _finish_edit(store, sessions, chat_id, task)
    if current == "edit_nm":
        task = store.task(draft.get("task_id") or "")
        nm = parse_nm(text)
        if not task:
            _clear(sessions, chat_id)
            return [send(chat_id, "Этой карточки уже нет.")]
        if not nm:
            return [send(chat_id, "Нужны цифры артикула или ссылка на карточку.")]
        task["nm"] = nm
        store.save()
        return _finish_edit(store, sessions, chat_id, task)
    if current == "edit_plan":
        task = store.task(draft.get("task_id") or "")
        plan = parse_count(text)
        if not task:
            _clear(sessions, chat_id)
            return [send(chat_id, "Этой карточки уже нет.")]
        if plan is None or plan < 1 or plan > PLAN_MAX:
            return [send(chat_id, "Напиши число штук, например 3.")]
        if plan < total_qty(task):
            return [send(chat_id, f"Уже отмечено {total_qty(task)}. План меньше не ставлю.")]
        task["plan"] = plan
        store.save()
        return _finish_edit(store, sessions, chat_id, task)
    if current == "add_qty":
        count = parse_count(text)
        task = store.task(draft.get("task_id") or "")
        if not task or count is None:
            return [send(chat_id, "Напиши число.")]
        return _add_mark(store, sessions, chat_id, task, count, None, user["name"])
    if current == "mine_qty":
        count = parse_count(text)
        task = store.task(draft.get("task_id") or "")
        if not task or count is None:
            return [send(chat_id, "Напиши число. 0 снимает отметку.")]
        return _set_mark(store, sessions, chat_id, task, count, None, user["name"])
    return [send(chat_id, "Нажми кнопку внизу или /start.")]


def _add_mark(store, sessions, chat_id, task, count, message_id, name) -> list:
    if task.get("status") != "open":
        return [alert("Она закрыта.", True)]
    if count < 1:
        return [alert("Напиши число больше нуля.", True)]
    plan = int(task["plan"])
    new_qty = qty_of(task, chat_id) + count
    if others_qty(task, chat_id) + new_qty > plan:
        return [alert(over_plan(plan), True)]
    store.set_mark(task, chat_id, name, new_qty)
    _clear(sessions, chat_id)
    note = f"Записал: {esc(name)} {new_qty}. На карточке {total_qty(task)}/{plan}."
    actions = refresh(store, task)
    actions.extend(_mark_notice(store, task, f"{name} · +{count} шт"))
    if message_id:
        actions.append(edit(chat_id, message_id, note, inline([])))
    else:
        actions.append(send(chat_id, note))
    return actions


def _set_mark(store, sessions, chat_id, task, count, message_id, name) -> list:
    if task.get("status") != "open":
        return [alert("Она закрыта.", True)]
    plan = int(task["plan"])
    if others_qty(task, chat_id) + count > plan:
        return [alert(over_plan(plan), True)]
    store.set_mark(task, chat_id, name, count)
    _clear(sessions, chat_id)
    if count == 0:
        note = "Снял отметку."
    else:
        note = f"Записал: {esc(name)} {count}. На карточке {total_qty(task)}/{plan}."
    actions = refresh(store, task)
    if message_id:
        actions.append(edit(chat_id, message_id, note, inline([])))
    else:
        actions.append(send(chat_id, note))
    return actions


def _mark_notice(store: Store, task: dict, header: str) -> list:
    text = notice_text(task, header, show_from=False)
    markup = open_markup(task)
    return [send(int(user["chat_id"]), text, markup) for user in store.users.values()]


def _open_card(store: Store, chat_id: int, task: dict, name: str) -> list:
    return [send(chat_id, card_text(task), card_markup(task, name), remember=task["id"])]


def on_callback(store: Store, sessions: dict, chat_id: int, data: str, message_id: int | None) -> list:
    user, missing = _require_user(store, sessions, chat_id)
    if missing:
        return missing + [alert()]
    parts = (data or "").split(":")
    op = parts[0]
    step = _session(sessions, chat_id)

    if op == "ch" and len(parts) == 2:
        channel = "keyword" if parts[1] == "k" else "competitor" if parts[1] == "c" else ""
        if not channel or step["step"] not in ("channel", "edit_channel"):
            return [alert("Сейчас это не нужно.", True)]
        step["draft"]["channel"] = channel
        prompt = _query_prompt(channel)
        if step["step"] == "channel":
            step["step"] = "query"
        else:
            step["step"] = "edit_query"
        if message_id:
            return [edit(chat_id, message_id, prompt), alert()]
        return [send(chat_id, prompt), alert()]

    if op == "post":
        draft = step["draft"]
        if step["step"] != "confirm" or not all(draft.get(key) for key in ("title", "nm", "channel", "query", "plan")):
            return [alert("Черновик потерялся. Нажми «Новая раздача» ещё раз.", True)]
        if not is_setter(user["name"]):
            return [alert("Ставить задачи могут Елизавета, Диля и Босс.", True)]
        task = store.create_task(chat_id=chat_id, name=user["name"], draft=draft)
        _clear(sessions, chat_id)
        text = posted_text(task, "Новая раздача")
        actions = []
        if message_id:
            actions.append(edit(
                chat_id, message_id, text, card_markup(task, user["name"]), remember=task["id"],
            ))
        else:
            actions.append(send(
                chat_id, text, card_markup(task, user["name"]), remember=task["id"],
            ))
        for other in store.users.values():
            if int(other["chat_id"]) == int(chat_id):
                continue
            actions.append(send(
                int(other["chat_id"]),
                text,
                card_markup(task, other["name"]),
                remember=task["id"],
            ))
        actions.append(alert("Поставил.", False))
        return actions

    if op == "drop":
        _clear(sessions, chat_id)
        if message_id:
            return [edit(chat_id, message_id, "Отменил.", inline([])), alert()]
        return [send(chat_id, "Отменил."), alert()]

    task_id = parts[1] if len(parts) > 1 else ""
    task = store.task(task_id) if task_id else None

    if op == "card":
        if not task:
            return [alert("Этой карточки уже нет.", True)]
        return _open_card(store, chat_id, task, user["name"]) + [alert()]

    if op == "give":
        if not task or task.get("status") != "open":
            return [alert("Этой карточки уже нет." if not task else "Она закрыта.", True)]
        markup = give_markup(task)
        if markup is None:
            return [alert("План уже собран.", True)]
        text = "Сколько добавить? Кнопки до остатка плана."
        if message_id:
            return [send(chat_id, text, markup), alert()]
        return [send(chat_id, text, markup), alert()]

    if op == "add" and len(parts) == 3:
        count = parse_count(parts[2])
        if not task or count is None:
            return [alert("Кнопка устарела. Открой доску ещё раз.", True)]
        return _add_mark(store, sessions, chat_id, task, count, message_id, user["name"]) + [alert()]

    if op == "addmore":
        if not task:
            return [alert("Этой карточки уже нет.", True)]
        sessions[chat_id] = {"step": "add_qty", "draft": {"task_id": task_id}}
        left = int(task["plan"]) - total_qty(task)
        return [send(chat_id, f"Напиши число, сколько добавить. Осталось {left}."), alert()]

    if op == "mine":
        if not task or task.get("status") != "open":
            return [alert("Она закрыта." if task else "Этой карточки уже нет.", True)]
        return [send(chat_id, "Сколько поставить за себя? 0 снимает отметку.", mine_markup(task, chat_id)), alert()]

    if op == "minemore":
        if not task:
            return [alert("Этой карточки уже нет.", True)]
        sessions[chat_id] = {"step": "mine_qty", "draft": {"task_id": task_id}}
        return [send(chat_id, "Напиши свою цифру. 0 снимает отметку."), alert()]

    if op == "set" and len(parts) == 3:
        count = parse_count(parts[2])
        if not task or count is None:
            return [alert("Кнопка устарела. Открой доску ещё раз.", True)]
        return _set_mark(store, sessions, chat_id, task, count, message_id, user["name"]) + [alert()]

    if op == "edit":
        if not task:
            return [alert("Этой карточки уже нет.", True)]
        if not is_setter(user["name"]) or task.get("status") != "open":
            return [alert("Меняет тот, кто ставит задачи.", True)]
        return [send(chat_id, "Что изменить?", inline([
            [("Название", f"ed:t:{task_id}"), ("Артикул", f"ed:n:{task_id}")],
            [("Канал", f"ed:c:{task_id}"), ("План", f"ed:p:{task_id}")],
        ])), alert()]

    if op == "ed" and len(parts) == 3:
        field, task_id = parts[1], parts[2]
        task = store.task(task_id)
        if not task or not is_setter(user["name"]):
            return [alert("Меняет тот, кто ставит задачи.", True)]
        prompts = {
            "t": ("edit_title", "Новое название. Например: 046 Black."),
            "n": ("edit_nm", "Новый артикул: цифры или ссылка."),
            "c": ("edit_channel", "Дальше «По ключу» или «Через конкурента»."),
            "p": ("edit_plan", "Новый план, штук."),
        }
        if field not in prompts:
            return [alert("Кнопка устарела.", True)]
        step_name, prompt = prompts[field]
        sessions[chat_id] = {"step": step_name, "draft": {"task_id": task_id}}
        if field == "c":
            return [send(chat_id, prompt, inline([
                [("По ключу", "ch:k"), ("Через конкурента", "ch:c")],
            ])), alert()]
        return [send(chat_id, prompt), alert()]

    if op == "close":
        if not task or not is_setter(user["name"]):
            return [alert("Закрывает тот, кто ставит задачи.", True)]
        task["status"] = "closed"
        store.save()
        return refresh(store, task) + [alert("Закрыл.")]

    if op == "back":
        if not task or not is_setter(user["name"]):
            return [alert("Возвращает тот, кто ставит задачи.", True)]
        task["status"] = "open"
        store.save()
        return refresh(store, task) + [alert("Снова в работе.")]

    if op == "del":
        if not task or not is_setter(user["name"]):
            return [alert("Удаляет тот, кто ставит задачи.", True)]
        if total_qty(task) > 0:
            return [alert("Удалить можно, пока никто не отметил.", True)]
        if not message_id:
            return [alert("Открой карточку ещё раз.", True)]
        return [edit(chat_id, message_id, f"Удалить «{esc(task['title'])}»?", inline([
            [("Удалить", f"delok:{task_id}"), ("Оставить", f"delno:{task_id}")],
        ])), alert()]

    if op == "delno":
        if not task:
            return [alert("Этой карточки уже нет.", True)]
        if message_id:
            return [edit(chat_id, message_id, card_text(task), card_markup(task, user["name"])), alert()]
        return [alert()]

    if op == "delok":
        if not task or not is_setter(user["name"]):
            return [alert("Удаляет тот, кто ставит задачи.", True)]
        if total_qty(task) > 0:
            return [alert("Удалить можно, пока никто не отметил.", True)]
        note = f"Удалено: {esc(task['title'])}"
        actions = [
            edit(int(chat_key), int(mid), note, inline([]))
            for chat_key, mid in (task.get("messages") or {}).items()
        ]
        store.tasks = [item for item in store.tasks if item.get("id") != task["id"]]
        store.save()
        if message_id and not any(act.get("message_id") == message_id for act in actions):
            actions.append(edit(chat_id, message_id, note, inline([])))
        return actions + [alert("Удалил.")]

    return [alert("Кнопка устарела. Нажми /start.", True)]


def reduce(store: Store, sessions: dict, event: dict) -> list:
    chat_id = int(event["chat_id"])
    text = event.get("text") or ""
    message_id = event.get("message_id")
    if event.get("kind") == "callback":
        return on_callback(store, sessions, chat_id, text, message_id)
    if not text.strip():
        return [send(chat_id, "Напиши текстом.")]
    label = text.strip()
    known = menu_key(label)
    if command_of(label) or known:
        return on_menu(store, sessions, chat_id, known or label)
    if not store.user(chat_id):
        return accept_name(store, sessions, chat_id, label)
    return on_step(store, sessions, chat_id, label, message_id)


def _token() -> str:
    return (os.getenv("RAZDACHI_BOT_TOKEN") or "").strip()


def _safe(error: object, token: str) -> str:
    text = str(error)
    if token:
        text = text.replace(token, "***")
    return text[:300]


def _tg(token: str, method: str, payload: dict | None = None, timeout: float = 35) -> dict:
    response = httpx.post(
        f"https://api.telegram.org/bot{token}/{method}",
        json=payload or {},
        timeout=timeout,
    )
    try:
        data = response.json()
    except Exception:
        data = {}
    if response.status_code == 409 or data.get("error_code") == 409:
        raise Conflict(data.get("description") or "409")
    description = data.get("description") or ""
    if response.status_code >= 400 or data.get("ok") is False:
        if "message is not modified" in description:
            return data
        raise RuntimeError(description or response.text[:300])
    return data


def _event_from_update(update: dict) -> dict | None:
    if "message" in update:
        message = update["message"]
        chat = message.get("chat") or {}
        if chat.get("type") not in (None, "private"):
            return None
        return {
            "kind": "text",
            "chat_id": chat.get("id"),
            "text": message.get("text") or "",
            "message_id": message.get("message_id"),
            "callback_id": None,
        }
    if "callback_query" in update:
        query = update["callback_query"]
        message = query.get("message") or {}
        chat = message.get("chat") or {}
        chat_id = chat.get("id") or (query.get("from") or {}).get("id")
        return {
            "kind": "callback",
            "chat_id": chat_id,
            "text": query.get("data") or "",
            "message_id": message.get("message_id"),
            "callback_id": query.get("id"),
        }
    return None


def _apply(token: str, store: Store, event: dict, actions: list) -> None:
    alerts = [item for item in actions if item.get("op") == "alert"]
    if event.get("callback_id"):
        text = alerts[0]["text"] if alerts else ""
        show = bool(alerts and alerts[0].get("show") and text)
        try:
            _tg(token, "answerCallbackQuery", {
                "callback_query_id": event["callback_id"],
                "text": text[:180],
                "show_alert": show,
            })
        except Exception as exc:
            logger.warning("razdachi answer: %s", _safe(exc, token))
    for action in actions:
        op = action.get("op")
        if op not in ("send", "edit"):
            continue
        payload = {
            "chat_id": action["chat_id"],
            "text": action["text"] or "Пусто.",
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
            "link_preview_options": {"is_disabled": True},
        }
        if action.get("markup") is not None:
            payload["reply_markup"] = action["markup"]
        method = "sendMessage" if op == "send" else "editMessageText"
        if op == "edit":
            payload["message_id"] = action["message_id"]
        try:
            result = _tg(token, method, payload)
        except Exception as exc:
            logger.warning("razdachi %s: %s", method, _safe(exc, token))
            if action.get("remember") and op == "edit":
                with _LOCK:
                    store.forget_message(action["remember"], action["chat_id"])
            continue
        if not action.get("remember"):
            continue
        message = (result or {}).get("result") or {}
        message_id = message.get("message_id") or action.get("message_id")
        if message_id:
            with _LOCK:
                store.remember(action["remember"], action["chat_id"], int(message_id))


def _handle_update(update: dict) -> None:
    event = _event_from_update(update)
    if not event or event.get("chat_id") is None or _STORE is None:
        return
    with _LOCK:
        _STORE.pull()
        actions = reduce(_STORE, _STORE.sessions, event)
        _STORE.save()
    _apply(_token(), _STORE, event, actions)


def _poll_loop() -> None:
    global _POLLING, _CONFLICT, _USERNAME
    token = _token()
    _POLLING = True
    try:
        me = _tg(token, "getMe", timeout=15).get("result") or {}
        _USERNAME = me.get("username") or ""
        logger.info("razdachi: бот @%s запущен", _USERNAME)
        try:
            _tg(token, "deleteWebhook", {"drop_pending_updates": False}, timeout=15)
        except Exception as exc:
            logger.warning("razdachi deleteWebhook: %s", _safe(exc, token))
        offset = None
        backoff = 2
        while True:
            try:
                payload = {"timeout": 25, "allowed_updates": ["message", "callback_query"]}
                if offset is not None:
                    payload["offset"] = offset
                result = _tg(token, "getUpdates", payload, timeout=40)
                backoff = 2
                _CONFLICT = False
                for update in result.get("result") or []:
                    offset = int(update.get("update_id") or 0) + 1
                    try:
                        _handle_update(update)
                    except Exception as exc:
                        logger.error("razdachi update: %s", _safe(exc, token))
            except Conflict:
                _CONFLICT = True
                logger.warning(
                    "razdachi: другой процесс уже читает этого бота. "
                    "Жду, пока закроют окно на компьютере."
                )
                time.sleep(15)
            except Exception as exc:
                logger.error("razdachi poll: %s", _safe(exc, token))
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)
    finally:
        _POLLING = False


def _public_base() -> str:
    explicit = (os.getenv("RAZDACHI_WEBHOOK_BASE") or "").strip().rstrip("/")
    if explicit:
        return explicit if explicit.startswith("http") else f"https://{explicit}"
    domain = (os.getenv("RAILWAY_PUBLIC_DOMAIN") or "").strip()
    if domain:
        return f"https://{domain}"
    return ""


def webhook_secret() -> str:
    token = _token()
    if not token:
        return ""
    return hashlib.sha256(f"razdachi-webhook:{token}".encode()).hexdigest()[:32]


def webhook_ok(header: str | None) -> bool:
    secret = webhook_secret()
    if not secret or not header or len(header) != len(secret):
        return False
    return hmac.compare_digest(secret, header)


def handle_webhook(update: dict) -> None:
    if not isinstance(update, dict):
        return
    if _STORE is None:
        start()
    if _STORE is None:
        return
    try:
        _handle_update(update)
    except Exception as exc:
        logger.error("razdachi webhook: %s", _safe(exc, _token()))


def _arm_webhook(token: str) -> bool:
    global _POLLING, _USERNAME, _MODE
    base = _public_base()
    if not base:
        return False
    me = _tg(token, "getMe", timeout=15).get("result") or {}
    _USERNAME = me.get("username") or ""
    url = f"{base}/api/razdachi-webhook/{webhook_secret()}"
    _tg(token, "setWebhook", {
        "url": url,
        "secret_token": webhook_secret(),
        "allowed_updates": ["message", "callback_query"],
        "drop_pending_updates": False,
    }, timeout=15)
    _POLLING = True
    _MODE = "webhook"
    logger.info("razdachi: бот @%s слушает сайт", _USERNAME)
    return True


def status() -> dict:
    store = _STORE
    tasks = store.tasks if store else []
    return {
        "token_set": bool(_token()),
        "token_tail": _token()[-4:] if _token() else None,
        "username": _USERNAME or None,
        "mode": _MODE or None,
        "polling": _POLLING,
        "conflict": _CONFLICT,
        "users": len(store.users) if store else 0,
        "open_tasks": sum(1 for task in tasks if task.get("status") == "open"),
        "closed_tasks": sum(1 for task in tasks if task.get("status") == "closed"),
    }


def publish_competitor_task(
    *,
    title: str,
    nm: str,
    competitor_nm: str,
    plan: int,
    setter: str = "Босс",
) -> dict:
    """Карточка «Через конкурента» с сайта: та же доска, что у кнопки «Поставить»."""
    if _STORE is None:
        start()
    if _STORE is None:
        return {"ok": False, "error": "Бот раздач не запущен. Нужен RAZDACHI_BOT_TOKEN."}
    title = " ".join(str(title or "").split())
    nm = parse_nm(str(nm or "")) or (str(nm or "").strip() if re.fullmatch(r"\d{6,12}", str(nm or "").strip()) else "")
    competitor_nm = parse_nm(str(competitor_nm or "")) or (
        str(competitor_nm or "").strip() if re.fullmatch(r"\d{6,12}", str(competitor_nm or "").strip()) else ""
    )
    try:
        plan = int(plan)
    except (TypeError, ValueError):
        plan = 0
    if not title:
        return {"ok": False, "error": "Нужно своё название."}
    if not nm or not competitor_nm:
        return {"ok": False, "error": "Нужны артикул и артикул конкурента."}
    if plan < 1 or plan > PLAN_MAX:
        return {"ok": False, "error": "Напиши число штук, например 3."}
    with _LOCK:
        _STORE.pull()
        task = _STORE.create_task(
            chat_id=0,
            name=setter,
            draft={
                "title": title[:80],
                "nm": nm,
                "channel": "competitor",
                "query": competitor_nm,
                "plan": plan,
            },
        )
        text = posted_text(task, "Новая раздача")
        actions = [
            send(int(user["chat_id"]), text, card_markup(task, user.get("name") or ""), remember=task["id"])
            for user in _STORE.users.values()
        ]
        task_id = task["id"]
        line = board_line(task)
    token = _token()
    if token and actions:
        _apply(token, _STORE, {}, actions)
    return {"ok": True, "id": task_id, "sent": len(actions), "line": line}


def _webhook_watch() -> None:
    last_error = ""
    while True:
        time.sleep(25)
        token = _token()
        base = _public_base()
        secret = webhook_secret()
        if not token or not base or not secret:
            continue
        url = f"{base}/api/razdachi-webhook/{secret}"
        try:
            info = (_tg(token, "getWebhookInfo", timeout=15).get("result") or {})
            if (info.get("url") or "") != url:
                _arm_webhook(token)
                logger.warning("razdachi: снова слушаю сайт, кнопка «Новая раздача» доходит")
            error = str(info.get("last_error_message") or "")
            if error and error != last_error:
                last_error = error
                logger.warning("razdachi: телеграм не доставил обновление: %s", error[:180])
        except Exception as exc:
            logger.warning("razdachi watch: %s", _safe(exc, token))


def start() -> bool:
    global _STARTED, _STORE, _MODE
    token = _token()
    other = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    if not token:
        logger.info("razdachi: RAZDACHI_BOT_TOKEN не задан, бот раздач не запущен")
        return False
    if other and token == other:
        logger.error("razdachi: RAZDACHI_BOT_TOKEN совпадает с TELEGRAM_BOT_TOKEN, не запускаю")
        return False
    with _LOCK:
        if _STORE is None:
            _STORE = load_store()
        if _STARTED:
            return True
        _STARTED = True
    try:
        if _arm_webhook(token):
            threading.Thread(target=_webhook_watch, daemon=True, name="razdachi-hook").start()
            return True
    except Exception as exc:
        logger.warning("razdachi webhook не встал, читаю сам: %s", _safe(exc, token))
    _MODE = "poll"
    threading.Thread(target=_poll_loop, daemon=True, name="razdachi-bot").start()
    return True
