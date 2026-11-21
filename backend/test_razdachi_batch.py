import unittest
from datetime import datetime
from unittest.mock import patch
from zoneinfo import ZoneInfo

import razdachi_bot as bot
import test_razdachi_flow as flow
from razdachi_batch import parse_batch


PLAN = """052 голд
22.11
через него Смарт часы умные gold 11 Series Watch для андроид и iphone SmartX 1098099840 купить за 2 540 ₽ в интернет-магазине Wildberries - 5 шт, смарт часы женские
23.11
Смарт часы умные 11 series Smart Watch наручные SmartX 1808747385 купить за 2 860 ₽ в интернет-магазине Wildberries - 3 шт, смарт часы женские
052 грей
21.11
Смарт часы умные 11 series Smart Watch наручные SmartX 1808747385 купить за 2 860 ₽ в интернет-магазине Wildberries - 2 шт, смарт часы женские
2 шт по ключу смарт часы женские
048 блэк
21.11
через Смарт часы умные 11 series Smart Watch наручные SmartX 1808747516 купить за 2 860 ₽ в интернет-магазине Wildberries - 3 шт, смарт часы мужские
22.11
через него Смарт часы умные 11 series Smart Watch наручные SmartX 1808747516 купить за 2 860 ₽ в интернет-магазине Wildberries - 3 шт, смарт часы мужские
051 голд
через него Смарт часы женские Стильное время 2026 846581366 купить за 4 478 ₽ в интернет-магазине Wildberries - 5 шт, смарт часы женские
054 блэк
21.11
через него Смарт часы умные 11 series Smart Watch 46мм SmartX 1808747313 купить за 3 855 ₽ в интернет-магазине Wildberries - 3 шт,смарт часы мужские
"""
CATALOG = {2219327613: "048_PVS_black_O", 12345670: "052_gold", 12345671: "052_grey",
           12345672: "051_gold", 12345673: "054_black"}


class BatchParserTest(unittest.TestCase):
    def test_exact_plan_totals_dates_keywords(self):
        items, errors = parse_batch(PLAN, 2026)
        self.assertEqual(errors, [])
        self.assertEqual(len(items), 8)
        self.assertEqual(sum(i["plan"] for i in items), 26)
        self.assertEqual([i["plan"] for i in items], [5, 3, 2, 2, 3, 3, 5, 3])
        self.assertEqual(items[0]["query"], "1098099840")
        self.assertEqual(items[0]["keyword"], "смарт часы женские")
        self.assertEqual(items[3]["channel"], "keyword")
        self.assertEqual(items[3]["query"], "смарт часы женские")
        self.assertEqual(items[3]["planned_date"], "2026-11-21")
        self.assertEqual(items[6]["planned_date"], "")
        self.assertEqual(items[7]["planned_date"], "2026-11-21")

    def test_invalid_rows_do_not_silently_disappear(self):
        for source in ("048 блэк\n31.11\n2 шт по ключу часы",
                       "048 блэк\nчерез него - 2 шт, часы",
                       "048 блэк\n12345678 и 87654321 - 2 шт, часы",
                       "048 блэк\n0 шт по ключу часы",
                       "048 блэк\n2 шт по ключу часы\n051 голд\nзабыто количество"):
            with self.subTest(source=source):
                self.assertTrue(parse_batch(source, 2026)[1])


class BatchFlowTest(flow.TelegramHarness):
    def setUp(self):
        super().setUp()
        self.store.now = lambda: datetime(2026, 11, 21, tzinfo=ZoneInfo("Europe/Moscow"))
        p = patch.object(bot, "_CATALOG_PROVIDER", return_value=CATALOG)
        p.start()
        self.addCleanup(p.stop)

    def test_preview_then_publish_and_finish_task(self):
        self.text(1, PLAN)
        self.assertEqual(self.store.tasks, [])
        self.assertIn("051 голд", self.delivered_text())
        self.assertEqual(self.store.sessions[1]["step"], "batch_date")
        self.text(1, "22.11")
        self.assertIn("8 раздач, 26 шт", self.delivered_text())
        self.assertIn("052 голд — 8 шт", self.delivered_text())
        self.assertIn("052 грей — 4 шт", self.delivered_text())
        self.assertFalse(any(p.get("chat_id") == 2 for m, p in self.calls))
        self.press(1, "Создать 8 раздач")
        self.assertEqual(len(self.store.tasks), 8)
        self.assertEqual(sum(t["plan"] for t in self.store.tasks), 26)
        self.assertEqual(self.store.tasks[6]["planned_date"], "2026-11-22")
        self.assertIn("Ключ: смарт часы женские", bot.posted_text(self.store.tasks[0]))
        self.assertEqual(set(self.store.tasks[0]["messages"]), {"1", "2"})
        self.press(1, "Создать 8 раздач")
        self.assertEqual(len(self.store.tasks), 8)
        self.press(2, "Я раздала")
        self.press(2, "1")
        self.assertEqual(bot.total_qty(self.store.tasks[-1]), 1)
        self.press(1, "Закрыть")
        self.assertEqual(self.store.tasks[-1]["status"], "closed")

    def test_undated_cancel_and_marker(self):
        self.text(2, PLAN)
        self.assertEqual(self.store.tasks, [])
        self.text(1, PLAN)
        self.press(1, "Без даты")
        self.assertIn("без даты", self.delivered_text())
        self.press(1, "Отмена")
        self.press(1, "Создать 8 раздач")
        self.assertEqual(self.store.tasks, [])

    def test_missing_product_requires_manual_article(self):
        with patch.object(bot, "_CATALOG_PROVIDER", return_value={}):
            self.text(1, "052 голд\n22.11\n2 шт по ключу часы")
            self.assertEqual(self.store.sessions[1]["step"], "batch_product")
            self.text(1, "12345678")
            self.press(1, "Создать 1 раздачу")
            self.assertEqual(self.store.tasks[0]["nm"], "12345678")

    def test_invalid_batch_does_not_publish_partial_result(self):
        self.text(1, PLAN.replace("23.11", "31.11"))
        self.assertIn("Исправь", self.delivered_text())
        self.assertEqual(self.store.tasks, [])

    def test_old_confirmation_cannot_publish_replacement_list(self):
        source = "052 голд\n22.11\n2 шт по ключу часы"
        self.text(1, source)
        old_nonce = self.store.sessions[1]["draft"]["nonce"]
        self.text(1, source.replace("2 шт", "3 шт"))
        self.callback(1, f"batch:{old_nonce}:post")
        self.assertEqual(self.store.tasks, [])
        self.press(1, "Создать 1 раздачу")
        self.assertEqual(self.store.tasks[0]["plan"], 3)

    def test_ambiguous_catalog_needs_choice(self):
        with patch.object(bot, "_CATALOG_PROVIDER", return_value={12345670: "052_gold", 12345679: "052_PVS_gold"}):
            self.text(1, "052 голд\n22.11\n2 шт по ключу часы")
            self.assertEqual(self.store.sessions[1]["step"], "batch_product")
            self.assertEqual(self.store.tasks, [])
            self.press(1, "052_PVS_gold · 12345679")
            self.press(1, "Создать 1 раздачу")
            self.assertEqual(self.store.tasks[0]["nm"], "12345679")
