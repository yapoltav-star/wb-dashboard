import unittest
from unittest.mock import patch

import razdachi_bot as bot
import test_razdachi_flow as flow


class ProductMatchingTest(unittest.TestCase):
    def test_real_dashboard_code_and_aliases(self):
        with patch.object(bot, "_CATALOG_PROVIDER", return_value={
            2219327613: "048_PVS_black_O", 1473681561: "048_PVS11_gold_withbras_O",
            12345678: "049_PVS_black_O",
        }):
            for query in ("0.48 black", "048 black", "048 блэк", "0,48 чёрный", "048black"):
                with self.subTest(query=query):
                    self.assertEqual([p["nm"] for p in bot.product_matches(query)], ["2219327613"])
            self.assertEqual(bot.product_matches("048 white"), [])
            self.assertEqual(bot.product_matches("48"), [])

    def test_catalog_failure_preserves_manual_path(self):
        with patch.object(bot, "_CATALOG_PROVIDER", side_effect=RuntimeError("unavailable")):
            self.assertEqual(bot.product_matches("048 black"), [])


class ProductFlowTest(flow.TelegramHarness):
    def setUp(self):
        super().setUp()
        p = patch.object(bot, "_CATALOG_PROVIDER", return_value={2219327613: "048_PVS_black_O"})
        p.start()
        self.addCleanup(p.stop)

    def test_short_title_selects_article_then_completes_creation(self):
        self.text(1, "/new")
        self.text(1, "0.48 black")
        self.assertEqual(self.store.sessions[1]["step"], "product")
        self.assertEqual(self.store.tasks, [])
        self.press(1, "048_PVS_black_O · 2219327613")
        self.assertEqual(self.store.sessions[1]["draft"]["nm"], "2219327613")
        self.press(1, "По ключу")
        self.text(1, "часы")
        self.text(1, "3")
        self.press(1, "Поставить")
        self.assertEqual(self.store.tasks[0]["nm"], "2219327613")
        self.assertEqual(self.store.tasks[0]["title"], "048_PVS_black_O")

    def test_ambiguous_and_stale_selection(self):
        with patch.object(bot, "_CATALOG_PROVIDER", return_value={
            2219327613: "048_PVS_black_O", 12345678: "048_SmartX_black",
        }):
            self.text(1, "/new")
            self.text(1, "048 блэк")
            self.assertEqual(len(self.store.sessions[1]["draft"]["products"]), 2)
            self.assertNotIn("nm", self.store.sessions[1]["draft"])
            self.callback(1, "product:99999999")
            self.assertNotIn("nm", self.store.sessions[1]["draft"])
            self.text(1, "/cancel")
            self.callback(1, "product:2219327613")
            self.assertNotIn("nm", self.store.sessions[1]["draft"])

    def test_short_name_in_article_step_and_manual_override(self):
        self.text(1, "/new")
        self.text(1, "Своя подпись")
        self.text(1, "0.48 black")
        self.press(1, "Ввести артикул вручную")
        self.text(1, "12345678")
        self.assertEqual(self.store.sessions[1]["draft"]["nm"], "12345678")
        self.assertEqual(self.store.sessions[1]["step"], "channel")
