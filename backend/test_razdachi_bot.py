"""Схема бота «Раздачи» без Телеграма."""

import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import razdachi_bot as bot


MOSCOW = ZoneInfo("Europe/Moscow")
NOW = datetime(2026, 11, 20, 21, 0, tzinfo=MOSCOW)


def say(store, sessions, chat, text, message_id=10):
    return bot.reduce(store, sessions, {
        "kind": "text", "chat_id": chat, "text": text, "message_id": message_id,
    })


def tap(store, sessions, chat, data, message_id=11):
    return bot.reduce(store, sessions, {
        "kind": "callback", "chat_id": chat, "text": data, "message_id": message_id,
    })


def labels(markup):
    found = []
    for row in (markup or {}).get("inline_keyboard") or []:
        for button in row:
            found.append(button["text"])
    return found


def reply_labels(markup):
    found = []
    for row in (markup or {}).get("keyboard") or []:
        for button in row:
            found.append(button["text"])
    return found


class RazdachiBotTest(unittest.TestCase):
    def setUp(self):
        self.store = bot.Store()
        self.store.now = lambda: NOW
        self.sessions = {}

    def join(self, chat, name):
        say(self.store, self.sessions, chat, "/start")
        return say(self.store, self.sessions, chat, name)

    def test_name_rejects_username_and_greets(self):
        say(self.store, self.sessions, 1, "/start")
        rejected = say(self.store, self.sessions, 1, "@boss")
        self.assertIn("Юзернейм не приму", rejected[0]["text"])
        latin = say(self.store, self.sessions, 1, "Boss")
        self.assertIn("Юзернейм не приму", latin[0]["text"])
        hello = say(self.store, self.sessions, 1, "Босс")
        self.assertEqual(hello[0]["text"], "Привет, Босс.")
        self.assertIn("Новая раздача", reply_labels(hello[0]["markup"]))
        self.assertIn("Кто раздал", reply_labels(hello[0]["markup"]))

    def test_marker_has_no_setter_buttons(self):
        hello = self.join(2, "Маша")
        names = reply_labels(hello[0]["markup"])
        self.assertEqual(names, ["Доска", "Закрытые", "Имя"])
        refused = say(self.store, self.sessions, 2, "Новая раздача")
        self.assertIn("Елизавета, Диля и Босс", refused[0]["text"])

    def test_competitor_task_matches_old_board_line(self):
        self.join(1, "Босс")
        say(self.store, self.sessions, 1, "Новая раздача")
        say(self.store, self.sessions, 1, "048 блэк")
        say(self.store, self.sessions, 1, "https://www.wildberries.ru/catalog/2219327613/detail.aspx")
        tap(self.store, self.sessions, 1, "ch:c")
        say(self.store, self.sessions, 1, "1421308560")
        preview = say(self.store, self.sessions, 1, "3")
        self.assertIn("Проверь и поставь на доску.", preview[0]["text"])
        self.assertIn("Через конкурента: 1421308560", preview[0]["text"])
        self.assertIn("От: Босс", preview[0]["text"])
        self.assertIn("Поставить", labels(preview[0]["markup"]))
        tap(self.store, self.sessions, 1, "post", message_id=20)
        task = self.store.tasks[0]
        self.assertEqual(bot.board_line(task), "048 блэк 0/3 конк. 1421308560")
        self.assertIn("Пока никто не отметил", bot.card_text(task))
        board = say(self.store, self.sessions, 1, "Доска")
        self.assertEqual(labels(board[0]["markup"]), ["048 блэк 0/3 конк. 1421308560"])

    def test_keyword_line_and_broadcast_to_marker(self):
        self.join(2, "Маша")
        self.join(1, "Босс")
        say(self.store, self.sessions, 1, "Новая раздача")
        say(self.store, self.sessions, 1, "046 Black")
        say(self.store, self.sessions, 1, "2219327613")
        say(self.store, self.sessions, 1, "по ключу")
        say(self.store, self.sessions, 1, "смарт часы женские")
        say(self.store, self.sessions, 1, "3")
        posted = tap(self.store, self.sessions, 1, "post", message_id=21)
        task = self.store.tasks[0]
        self.assertEqual(bot.board_line(task), "046 Black 0/3 ключ смарт часы женские")
        chats = [item["chat_id"] for item in posted if item["op"] == "send"]
        self.assertIn(2, chats)
        marker = next(item for item in posted if item["op"] == "send" and item["chat_id"] == 2)
        self.assertIn("Я раздала", labels(marker["markup"]))
        self.assertNotIn("Закрыть", labels(marker["markup"]))
        boss = next(item for item in posted if item["op"] == "edit")
        self.assertIn("Закрыть", labels(boss["markup"]))
        self.assertIn("Удалить", labels(boss["markup"]))

    def test_marks_add_until_plan_then_mine_clears(self):
        self.join(1, "Босс")
        self.join(2, "Маша")
        say(self.store, self.sessions, 1, "Новая раздача")
        say(self.store, self.sessions, 1, "048 блэк")
        say(self.store, self.sessions, 1, "2219327613")
        tap(self.store, self.sessions, 1, "ch:c")
        say(self.store, self.sessions, 1, "1421308560")
        say(self.store, self.sessions, 1, "3")
        tap(self.store, self.sessions, 1, "post")
        task_id = self.store.tasks[0]["id"]
        tap(self.store, self.sessions, 2, f"give:{task_id}")
        added = tap(self.store, self.sessions, 2, f"add:{task_id}:2")
        self.assertTrue(any("Записал" in item["text"] for item in added))
        self.assertEqual(bot.total_qty(self.store.tasks[0]), 2)
        again = tap(self.store, self.sessions, 2, f"add:{task_id}:2")
        self.assertTrue(any(item["text"] == "В плане 3. Больше не записываю." for item in again))
        self.assertEqual(bot.total_qty(self.store.tasks[0]), 2)
        full = tap(self.store, self.sessions, 2, f"give:{task_id}")
        tap(self.store, self.sessions, 2, f"add:{task_id}:1")
        blocked = tap(self.store, self.sessions, 2, f"give:{task_id}")
        self.assertTrue(any(item["text"] == "План уже собран." for item in blocked))
        tap(self.store, self.sessions, 2, f"mine:{task_id}")
        cleared = tap(self.store, self.sessions, 2, f"set:{task_id}:0")
        self.assertTrue(any("Снял отметку" in item["text"] for item in cleared))
        self.assertEqual(bot.total_qty(self.store.tasks[0]), 0)
        self.assertIn("Пока никто не отметил", bot.card_text(self.store.tasks[0]))

    def test_today_counts_only_todays_marks(self):
        self.test_marks_add_until_plan_then_mine_clears()
        task = self.store.tasks[0]
        task["marks"] = [{
            "chat_id": 2,
            "name": "Маша",
            "qty": 2,
            "at": datetime(2026, 11, 19, 12, 0, tzinfo=MOSCOW).isoformat(),
        }]
        self.assertEqual(bot.today_text(self.store), "Сегодня ещё никто не отметил.")
        task["marks"][0]["at"] = NOW.isoformat()
        self.store.save = lambda: None
        text = say(self.store, self.sessions, 1, "Кто раздал")[0]["text"]
        self.assertIn("Маша — 2", text)
        hidden = say(self.store, self.sessions, 2, "/today")[0]["text"]
        self.assertIn("ставит задачи", hidden)

    def test_delete_only_before_marks_and_close_reopens(self):
        self.join(1, "Босс")
        say(self.store, self.sessions, 1, "Новая раздача")
        say(self.store, self.sessions, 1, "048 блэк")
        say(self.store, self.sessions, 1, "2219327613")
        tap(self.store, self.sessions, 1, "ch:c")
        say(self.store, self.sessions, 1, "1421308560")
        say(self.store, self.sessions, 1, "3")
        tap(self.store, self.sessions, 1, "post", message_id=30)
        task_id = self.store.tasks[0]["id"]
        self.store.remember(task_id, 1, 30)
        tap(self.store, self.sessions, 1, f"del:{task_id}", message_id=30)
        tap(self.store, self.sessions, 1, f"delok:{task_id}", message_id=30)
        self.assertEqual(self.store.tasks, [])

        say(self.store, self.sessions, 1, "Новая раздача")
        say(self.store, self.sessions, 1, "048 блэк")
        say(self.store, self.sessions, 1, "2219327613")
        tap(self.store, self.sessions, 1, "ch:k")
        say(self.store, self.sessions, 1, "часы")
        say(self.store, self.sessions, 1, "2")
        tap(self.store, self.sessions, 1, "post", message_id=31)
        task_id = self.store.tasks[0]["id"]
        self.store.remember(task_id, 1, 31)
        tap(self.store, self.sessions, 1, f"add:{task_id}:1", message_id=40)
        denied = tap(self.store, self.sessions, 1, f"del:{task_id}", message_id=31)
        self.assertTrue(any("пока никто не отметил" in item["text"] for item in denied))
        tap(self.store, self.sessions, 1, f"close:{task_id}")
        self.assertEqual(self.store.tasks[0]["status"], "closed")
        closed = say(self.store, self.sessions, 1, "Закрытые")
        self.assertIn("048 блэк 1/2", labels(closed[0]["markup"])[0])
        tap(self.store, self.sessions, 1, f"back:{task_id}")
        self.assertEqual(self.store.tasks[0]["status"], "open")

    def test_state_roundtrip(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "state.json"
            store = bot.Store(path=path, persist=True)
            store.now = lambda: NOW
            store.set_name(1, "Босс")
            store.create_task(chat_id=1, name="Босс", draft={
                "title": "048 блэк",
                "nm": "2219327613",
                "channel": "competitor",
                "query": "1421308560",
                "plan": 3,
            })
            loaded = bot.Store(path=path, persist=False)
            raw = path.read_text(encoding="utf-8")
            loaded = bot.Store(__import__("json").loads(raw))
            self.assertEqual(loaded.user(1)["name"], "Босс")
            self.assertEqual(bot.board_line(loaded.tasks[0]), "048 блэк 0/3 конк. 1421308560")

    def test_team_lists_roles(self):
        self.join(1, "Босс")
        self.join(2, "Маша")
        text = say(self.store, self.sessions, 1, "Команда")[0]["text"]
        self.assertIn("Елизавета, Диля, Босс", text)
        self.assertIn("Босс — ставит", text)
        self.assertIn("Маша — отмечает", text)


if __name__ == "__main__":
    unittest.main()
