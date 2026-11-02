"""Parser for Конкуренты.new — analytics_report.xlsx."""
from __future__ import annotations

from pathlib import Path

import competitors_new as cnew

SAMPLE = Path("/home/ubuntu/.cursor/projects/workspace/uploads/analytics_report_2026-11-02_11_30_da2f.xlsx")


def test_parse_rows_minimal():
    rows = [
        ["Дата формирования:", "02.11.26"],
        ["Период отчета:", "7 дней"],
        ["Категория 3 уровня:", "Смарт-часы"],
        ["Цена:", "0-9999 ₽"],
        [],
        [
            "Название товара", "Ссылка на товар", "Продавец", "Бренд",
            "Заказано на сумму, ₽", "Заказано, штуки", "Средняя цена, ₽",
            "Показы всего", "Просмотры в поиске и каталоге", "Просмотры карточки",
            "В корзину из поиска и каталога, %", "В корзину из карточки, %",
        ],
        ["Среднее значение по товарам", None, None, None, 100, 10, 10, 1000, 100, 50, 1, 10],
        [
            "Часы A", "https://www.ozon.ru/product/1111111111", "Seller", "BrandA",
            50000, 20, 2500, 10000, 4000, 1000, 0.5, 10,
        ],
        [
            "Часы B", "https://www.ozon.ru/product/2222222222", "Seller", "BrandB",
            90000, 80, 1125, 5000, 2000, 800, 1.0, 20,
        ],
    ]
    got = cnew.parse_analytics_rows(rows, filename="demo.xlsx")
    assert got["type"] == "analytics"
    assert got["category"] == "Смарт-часы"
    assert got["period"] == "7 дней"
    assert got["count"] == 2
    a, b = got["rows"]
    assert a["sku"] == "1111111111"
    # 4000*0.5/100 + 1000*10/100 = 20 + 100 = 120
    assert a["cart_adds"] == 120
    assert a["views"] == 10000
    assert b["ordered_qty"] == 80
    # 2000*1/100 + 800*20/100 = 20 + 160 = 180
    assert b["cart_adds"] == 180
    top_views = cnew.top_rows(got["rows"], "views", 1)
    assert top_views[0]["sku"] == "1111111111"
    top_carts = cnew.top_rows(got["rows"], "cart_adds", 1)
    assert top_carts[0]["sku"] == "2222222222"
    top_orders = cnew.top_rows(got["rows"], "ordered_qty", 1)
    assert top_orders[0]["sku"] == "2222222222"


def test_save_and_delete_memory():
    cnew.CACHE.update({"reports": [], "active": None, "loaded": False})
    payload = cnew.parse_analytics_rows([
        ["Категория 3 уровня:", "Тест"],
        ["Название товара", "Ссылка на товар", "Показы всего", "Заказано, штуки", "Заказано на сумму, ₽"],
        ["Товар", "https://www.ozon.ru/product/3333333333", 50, 3, 1000],
    ])
    store = {}

    def save(key, value):
        store[key] = value

    def get(key, default=None):
        v = store.get(key, default)
        return v

    data = cnew.save_report(save, get, payload, products=[{"sku": 3333333333, "primary_image": "http://img", "offer_id": "art"}])
    assert data["active"]["rows"][0]["is_own"] is True
    assert data["active"]["rows"][0]["primary_image"] == "http://img"
    assert len(data["reports"]) == 1
    deleted = cnew.delete_report(save, get, report_id=payload["id"])
    assert deleted["active"] is None
    assert deleted["reports"] == []


def test_thumb_helpers():
    html = r'https://ir.ozone.ru/s3/multimedia-1-q/8511204230.jpg&quot; other'
    imgs = cnew._parse_ozon_imgs(html)
    assert imgs and imgs[0].endswith("8511204230.jpg")
    assert "/c200/" in cnew._to_c200(imgs[0])
    assert "/c200/" in cnew._to_c200("https://ir.ozone.ru/s3/multimedia-1-i/c1000/7627962258.jpg")
    assert cnew._parse_ozon_imgs("https://ir.ozone.ru/s3/rp-photo-1/x.jpg") == []
    assert cnew._ozon_img_ok("https://ir.ozone.ru/s3/multimedia-1-q/c200/1.jpg")
    assert not cnew._ozon_img_ok("https://evil.example/x.jpg")


def test_fetch_thumb_bytes():
    cnew.THUMB_CACHE.clear()
    cnew.THUMB_BYTES.clear()
    cnew.THUMB_CACHE["4115126740"] = "https://ir.ozone.ru/s3/multimedia-1-q/c200/8511204230.jpg"

    class Fake:
        status_code = 200
        content = b"\xff\xd8\xff" + b"x" * 80
        headers = {"content-type": "image/jpeg"}

    orig = cnew.httpx.get

    def fake_get(url, **kwargs):
        assert "ir.ozone.ru" in url
        return Fake()

    cnew.httpx.get = fake_get
    try:
        got = cnew.fetch_thumb_bytes("4115126740")
        assert got and got[1] == "image/jpeg"
        assert got[0].startswith(b"\xff\xd8\xff")
        again = cnew.fetch_thumb_bytes("4115126740")
        assert again[0] is got[0]
        cnew.THUMB_CACHE["1"] = "https://evil.example/x.jpg"
        assert cnew.fetch_thumb_bytes("1") is None
    finally:
        cnew.httpx.get = orig
        cnew.THUMB_CACHE.clear()
        cnew.THUMB_BYTES.clear()


def test_real_analytics_report():
    if not SAMPLE.exists():
        print("skip real file")
        return
    payload = cnew.parse_analytics_xlsx(SAMPLE.read_bytes(), filename=SAMPLE.name)
    assert payload["count"] >= 100
    assert payload["category"]
    views = cnew.top_rows(payload["rows"], "views", 3)
    carts = cnew.top_rows(payload["rows"], "cart_adds", 3)
    orders = cnew.top_rows(payload["rows"], "ordered_qty", 3)
    assert views[0]["views"] >= views[-1]["views"]
    assert carts[0]["cart_adds"] > 0
    assert orders[0]["ordered_qty"] > 0
    print(
        "parsed", payload["count"],
        "cat", payload["category"],
        "top views", views[0]["name"][:40], views[0]["views"],
        "top carts", carts[0]["name"][:40], carts[0]["cart_adds"],
        "top orders", orders[0]["name"][:40], orders[0]["ordered_qty"],
    )


if __name__ == "__main__":
    test_parse_rows_minimal()
    test_save_and_delete_memory()
    test_thumb_helpers()
    test_fetch_thumb_bytes()
    test_real_analytics_report()
    print("ok")
