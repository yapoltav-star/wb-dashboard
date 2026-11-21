"""Replay Telegram updates through the real handler, without external services."""
import copy
import json
import unittest
from unittest.mock import patch

import razdachi_bot as bot


class TelegramHarness(unittest.TestCase):
    def setUp(self):
        self.saved = None
        self.calls = []
        self.messages = {}
        self.mid = 0
        self.store = bot.Store(persist=True)
        for target, value in (
            ("_STORE", self.store), ("_token", lambda: "test"),
            ("_remote_load", lambda: copy.deepcopy(self.saved)),
            ("_remote_save", self.save), ("_tg", self.telegram),
        ):
            p = patch.object(bot, target, value)
            p.start()
            self.addCleanup(p.stop)
        self.text(1, "/start")
        self.text(1, "Ярослав")
        self.text(2, "/start")
        self.text(2, "Маша")

    def save(self, payload):
        self.saved = json.loads(payload)

    def telegram(self, token, method, payload=None, **kwargs):
        payload = copy.deepcopy(payload or {})
        self.calls.append((method, payload))
        if method in ("sendMessage", "editMessageText"):
            if method == "sendMessage":
                self.mid += 1
                payload["message_id"] = self.mid
            self.messages[(payload["chat_id"], payload["message_id"])] = payload
        return {"ok": True, "result": {"message_id": payload.get("message_id")}}

    def update(self, update):
        self.calls.clear()
        bot.handle_webhook(update)
        return self.calls

    def text(self, chat, text):
        return self.update({"message": {"chat": {"id": chat, "type": "private"},
                                        "message_id": 1000, "text": text}})

    def press(self, chat, label):
        for (cid, mid), message in reversed(list(self.messages.items())):
            if cid != chat:
                continue
            for row in message.get("reply_markup", {}).get("inline_keyboard", []):
                for button in row:
                    if button["text"] == label:
                        return self.callback(chat, button["callback_data"], mid)
        self.fail(f"No visible button {label!r} for chat {chat}")

    def callback(self, chat, data, mid=1000):
        return self.update({"callback_query": {"id": "cb", "from": {"id": chat},
            "data": data, "message": {"chat": {"id": chat, "type": "private"},
                                      "message_id": mid}}})

    def create(self, channel="Через конкурента", plan="12"):
        self.text(1, "/new")
        for text in ("Часы & тест", "2219327613"):
            self.text(1, text)
        self.press(1, channel)
        self.text(1, "1421308560" if channel == "Через конкурента" else "часы женские")
        self.text(1, plan)
        self.press(1, "Поставить")
        self.assertEqual(len(self.store.tasks), 1)
        self.assertEqual(set(self.store.tasks[0]["messages"]), {"1", "2"})

    def delivered_text(self):
        return "\n".join(p.get("text", "") for m, p in self.calls
                         if m in ("sendMessage", "editMessageText", "answerCallbackQuery"))


class TelegramFlowTest(TelegramHarness):
    def test_complete_flow_and_all_edit_fields(self):
        for channel in ("Через конкурента", "По ключу"):
            with self.subTest(channel=channel):
                self.store.tasks.clear()
                self.store.save()
                self.create(channel)
                self.press(2, "Я раздала")
                self.press(2, "2")
                self.assertEqual(bot.total_qty(self.store.tasks[0]), 2)
                self.text(1, "Кто раздал")
                self.assertIn("Маша — 2", self.delivered_text())
                self.text(1, "Команда")
                self.assertIn("Ярослав — ставит", self.delivered_text())
                for label, value, field in (("Название", "Другие часы", "title"),
                        ("Артикул", "12345678", "nm"), ("План", "15", "plan")):
                    self.press(1, "Изменить")
                    self.press(1, label)
                    self.text(1, value)
                    self.assertEqual(str(self.store.tasks[0][field]), value)
                self.press(1, "Изменить")
                self.press(1, "Канал")
                self.press(1, "По ключу")
                self.text(1, "новый ключ")
                self.assertEqual(self.store.tasks[0]["query"], "новый ключ")
                self.press(2, "Я раздала")
                self.press(2, "Другое число")
                self.text(2, "9")
                self.assertEqual(bot.total_qty(self.store.tasks[0]), 11)
                self.press(2, "Мои")
                self.press(2, "0")
                self.assertEqual(bot.total_qty(self.store.tasks[0]), 0)
                self.press(2, "Я раздала")
                self.press(2, "1")
                self.press(1, "Закрыть")
                self.assertEqual(self.store.tasks[0]["status"], "closed")
                self.text(2, "Доска")
                self.assertIn("Доска пустая", self.delivered_text())
                self.text(1, "Закрытые")
                self.assertIn("Другие часы", self.delivered_text())
                self.press(1, "Вернуть в работу")
                self.assertEqual(self.store.tasks[0]["status"], "open")

    def test_full_plan_still_has_correction_button(self):
        self.create(plan="1")
        self.press(2, "Я раздала")
        self.press(2, "1")
        self.text(2, "/start")
        self.text(2, "Доска")
        card = next(p for m, p in self.calls if m == "sendMessage")
        labels = [b["text"] for row in card["reply_markup"]["inline_keyboard"] for b in row]
        self.assertIn("Мои", labels)
        self.press(2, "Мои")
        self.press(2, "0")
        self.assertEqual(bot.total_qty(self.store.tasks[0]), 0)

    def test_invalid_manual_quantity_gets_a_text_reply(self):
        self.create()
        self.press(2, "Я раздала")
        self.press(2, "Другое число")
        for qty in ("0", "13"):
            self.text(2, qty)
            self.assertTrue(any(m == "sendMessage" for m, p in self.calls), qty)
            self.assertEqual(bot.total_qty(self.store.tasks[0]), 0)

    def test_closed_task_cannot_be_edited_by_pending_input(self):
        self.create()
        self.press(1, "Изменить")
        self.press(1, "Название")
        self.press(1, "Закрыть")
        self.text(1, "Недопустимое изменение")
        self.assertEqual(self.store.tasks[0]["title"], "Часы & тест")
        self.assertIn("закрыта", self.delivered_text())

    def test_cancel_delete_keep_and_name(self):
        self.text(1, "/new")
        self.text(1, "/cancel")
        self.assertEqual(self.store.sessions[1]["step"], "")
        self.create()
        self.press(1, "Удалить")
        self.press(1, "Оставить")
        self.assertEqual(len(self.store.tasks), 1)
        self.press(1, "Удалить")
        self.press(1, "Удалить")
        self.assertEqual(self.store.tasks, [])
        self.text(2, "Имя")
        self.text(2, "Мария")
        self.assertEqual(self.store.user(2)["name"], "Мария")
        self.text(1, "/help")
        self.assertIn("/new", self.delivered_text())

    def test_manual_correction_and_closed_stale_buttons(self):
        self.create()
        self.press(2, "Мои")
        self.press(2, "Другое число")
        self.text(2, "13")
        self.assertIn("Больше не записываю", self.delivered_text())
        self.text(2, "10")
        self.assertEqual(bot.total_qty(self.store.tasks[0]), 10)
        task_id = self.store.tasks[0]["id"]
        self.press(1, "Закрыть")
        for data in (f"addmore:{task_id}", f"minemore:{task_id}",
                     f"ed:t:{task_id}", f"add:{task_id}:1", f"set:{task_id}:0"):
            self.callback(1, data)
            self.assertIn("закрыта", self.delivered_text())
            self.assertEqual(bot.total_qty(self.store.tasks[0]), 10)

    def test_marker_cannot_edit_close_reopen_or_delete(self):
        self.create()
        task_id = self.store.tasks[0]["id"]
        before = copy.deepcopy(self.store.tasks)
        for data in (f"edit:{task_id}", f"ed:p:{task_id}", f"close:{task_id}",
                     f"back:{task_id}", f"del:{task_id}", f"delok:{task_id}"):
            self.callback(2, data)
            self.assertEqual(self.store.tasks, before)
            self.assertTrue(self.delivered_text())

    def test_confirmation_cancel_and_duplicate_post(self):
        self.create()
        self.callback(1, "post")
        self.assertEqual(len(self.store.tasks), 1)
        self.text(1, "/new")
        self.text(1, "Отменяемая")
        self.text(1, "12345678")
        self.press(1, "По ключу")
        self.text(1, "часы")
        self.text(1, "3")
        self.press(1, "Отмена")
        self.assertEqual(len(self.store.tasks), 1)
        self.assertEqual(self.store.sessions[1]["step"], "")

    def test_quantities_with_units_like_original_bot(self):
        self.create(plan="12 шт")
        self.press(2, "Я раздала")
        self.press(2, "Другое число")
        self.text(2, "9 штук")
        self.assertEqual(bot.total_qty(self.store.tasks[0]), 9)

    def test_board_can_reach_tasks_beyond_first_page(self):
        self.create()
        draft = {"title": "Ещё", "nm": "12345678", "channel": "keyword", "query": "часы", "plan": 3}
        for i in range(20):
            self.store.create_task(chat_id=1, name="Ярослав", draft=draft)
        self.text(1, "Доска")
        self.press(1, "Дальше →")
        cards = [p for m, p in self.calls if m == "sendMessage" and "План " in p.get("text", "")]
        self.assertEqual(len(cards), 1)
        self.assertIn("Часы &amp; тест", cards[0]["text"])
        self.press(1, "← Назад")
        self.assertTrue(any(m == "sendMessage" and "Новая раздача" in p.get("text", "") for m, p in self.calls))
