"""Схема бота «Раздачи» без Телеграма."""

import json
import os
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
        self.assertEqual(reply_labels(hello[0]["markup"]), [
            "Доска", "Кто раздал", "Новая раздача", "Команда", "Закрытые", "Имя",
        ])

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
        self.assertTrue(preview[0]["text"].startswith("Новая раздача\n048 блэк\n"))
        self.assertIn("catalog/2219327613", preview[0]["text"])
        self.assertIn("Через конкурента: ", preview[0]["text"])
        self.assertIn("catalog/1421308560", preview[0]["text"])
        self.assertIn("План 3 · пока 0", preview[0]["text"])
        self.assertIn("От: Босс", preview[0]["text"])
        self.assertEqual(labels(preview[0]["markup"]), ["Поставить", "Отмена"])
        posted = tap(self.store, self.sessions, 1, "post", message_id=20)
        task = self.store.tasks[0]
        self.assertEqual(bot.board_line(task), "048 блэк 0/3 конк. 1421308560")
        self.assertIn("Пока никто не отметил", bot.card_text(task))
        edited = next(item for item in posted if item["op"] == "edit")
        self.assertIn("Я раздала", labels(edited["markup"]))
        self.assertIn("Закрыть", labels(edited["markup"]))
        self.assertIn("Пока никто не отметил", edited["text"])
        self.assertIn("План 3 · пока 0", edited["text"])
        board = say(self.store, self.sessions, 1, "Доска")
        self.assertTrue(board[0]["text"].startswith("Новая раздача"))
        self.assertIn("Пока никто не отметил", board[0]["text"])
        self.assertIn("Я раздала", labels(board[0]["markup"]))

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
        self.assertEqual(labels(marker["markup"]), ["Я раздала"])
        self.assertIn("По ключу: смарт часы женские", marker["text"])
        self.assertIn("Пока никто не отметил", marker["text"])
        boss = next(item for item in posted if item["op"] == "edit")
        self.assertIn("Я раздала", labels(boss["markup"]))
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
        self.assertTrue(any(item["text"].startswith("Маша · +2 шт") for item in added))
        self.assertTrue(any("План 3 · раздали 2 · ещё 1" in item["text"] for item in added))
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
        self.assertTrue(closed[0]["text"].startswith("Закрыта"))
        self.assertIn("048 блэк", closed[0]["text"])
        self.assertIn("План 2 · раздали 1 · ещё 1", closed[0]["text"])
        self.assertEqual(labels(closed[0]["markup"]), ["Вернуть в работу"])
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

    def test_new_button_keeps_the_draft(self):
        self.join(1, "Босс")
        self.store.sessions = self.sessions
        asked = say(self.store, self.store.sessions, 1, "Новая раздача")
        self.assertIn("Своё название", asked[0]["text"])
        raw = json.loads(json.dumps(self.store.to_dict()))
        loaded = bot.Store(raw)
        say(loaded, loaded.sessions, 1, "048 блэк")
        self.assertEqual(loaded.sessions[1]["step"], "nm")
        self.assertEqual(loaded.sessions[1]["draft"]["title"], "048 блэк")

    def test_webhook_secret_accepts_the_same_value(self):
        previous = os.environ.get("RAZDACHI_BOT_TOKEN")
        os.environ["RAZDACHI_BOT_TOKEN"] = "123456:test-token"
        try:
            secret = bot.webhook_secret()
            self.assertTrue(bot.webhook_ok(secret))
            self.assertFalse(bot.webhook_ok("нет"))
            self.assertFalse(bot.webhook_ok(""))
        finally:
            if previous is None:
                os.environ.pop("RAZDACHI_BOT_TOKEN", None)
            else:
                os.environ["RAZDACHI_BOT_TOKEN"] = previous

    def test_new_button_asks_name_then_title(self):
        asked = say(self.store, self.sessions, 7, "Новая раздача")
        self.assertIn("Маша", asked[0]["text"])
        followed = say(self.store, self.sessions, 7, "Босс")
        self.assertTrue(any("Своё название" in item["text"] for item in followed))
        self.assertEqual(self.sessions[7]["step"], "title")

    def test_shelf_button_posts_competitor_card(self):
        self.join(2, "Маша")
        self.join(1, "Босс")
        bot._STORE = self.store
        try:
            result = bot.publish_competitor_task(
                title="048 блэк",
                nm="2219327613",
                competitor_nm="1421308560",
                plan=3,
            )
        finally:
            bot._STORE = None
        self.assertTrue(result["ok"])
        self.assertEqual(result["line"], "048 блэк 0/3 конк. 1421308560")
        self.assertEqual(result["sent"], 2)
        task = self.store.tasks[0]
        self.assertEqual(task["channel"], "competitor")
        self.assertEqual(task["setter"], "Босс")
        self.assertIn("Пока никто не отметил", bot.posted_text(task))
        self.assertEqual(bot.publish_competitor_task(
            title="", nm="2219327613", competitor_nm="1421308560", plan=3,
        )["ok"], False)

    def test_team_lists_roles(self):
        self.join(1, "Босс")
        self.join(2, "Маша")
        text = say(self.store, self.sessions, 1, "Команда")[0]["text"]
        self.assertIn("Елизавета, Диля, Босс", text)
        self.assertIn("Босс — ставит", text)
        self.assertIn("Маша — отмечает", text)


if __name__ == "__main__":
    unittest.main()
