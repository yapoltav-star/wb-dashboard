# Ozon Partners Dashboard

Отдельный дашборд продавца Ozon (не смешивать с WB).

Структура и деплой — как у WB-дашборда: FastAPI на Railway + Supabase + SPA.

## С чего начать (чеклист)

1. **Создай пустой GitHub-репозиторий** `ozon-dashboard` (или вынеси папку `ozon-dashboard/` из этого репо).
2. **Supabase** → New project → SQL Editor → выполни `supabase/schema.sql`.
3. **Токены Ozon**: кабинет продавца → Настройки → Seller API → `Client-Id` + `Api-Key`.
4. **Railway** → Deploy from GitHub → репо `wb-dashboard`.
   - **Root Directory:** `ozon-dashboard/backend`  ← обязательно, не `ozon-dashboard`
   - **Branch:** `cursor/ozon-dashboard-skeleton-74b8` (пока не смержили в main)
   - Регион: **EU West (Amsterdam)** — задан в `railway.toml` (`europe-west4-drams3a`)
5. Variables:

```
OZON_CLIENT_ID=...
OZON_API_KEY=...
SUPABASE_URL=https://xxxx.supabase.co
SUPABASE_KEY=...   # service_role, не anon
```

6. Открой сайт → **Обновить товары** — первый синк карточек.

## MVP

| Раздел | Статус |
|--------|--------|
| Товары | работает |
| Остатки / рекомендации поставок (FBO+FBS, дни, поставить) | работает |
| Рост продаж (темп cur vs prev) | работает |
| Цены и соинвест (цены v5 + Premium details + акции) | работает |
| Заказы (FBO v3 + FBS v4) | работает |
| Отзывы (ReviewAPI v2) | работает (нужна подписка) |
| Реклама (Performance API) | работает (отдельные ключи) |
| Финансы (accrual + компенсации) | работает |

### Цены и соинвест

Аналог WB «Цены и СПП». **Цена на сайте** берётся с клиентской карточки `ozon.ru`
(composer-api), как `card.wb.ru` на WB. Соинвест = цена продавца (с акциями) − цена на сайте.

Seller API больше не отдаёт `marketing_price`. Premium `/v1/product/prices/details` —
запасной путь, если витрина с сервера недоступна (антибот/гео). Тогда задай `OZON_CLIENT_PROXY`
или задеплой ближе к RU.
### Реклама

Отдельные переменные Railway:
- `OZON_PERF_CLIENT_ID`
- `OZON_PERF_CLIENT_SECRET`

Кабинет → API-ключи → вкладка **Performance API**. Seller Api-Key сюда не подходит.

### Остатки — после деплоя

Если проект Supabase уже был создан раньше — выполни ещё `supabase/stocks_migration.sql`.

На сайте: вкладка **Остатки** → **Обновить остатки**.

Формулы как у WB:
- дневной темп = заказы / окно продаж
- дней хватит = остаток / темп
- поставить = темп × целевой запас − остаток
- «Весь Ozon» = FBO + FBS

## Дальше по приоритету

1. ~~Остатки FBO/FBS~~
2. ~~Цены / соинвест~~
3. ~~Заказы / отправления~~
4. ~~Отзывы~~
5. ~~Performance API (реклама)~~
6. ~~Финансы / начисления~~
7. Поставки FBO (черновики / таймслоты)
8. Возвраты

## Локальный запуск

```bash
cd ozon-dashboard/backend
cp ../.env.example .env   # или export переменных
pip install -r requirements.txt
python main.py
```

Открой http://localhost:8080

## Важно

- **Отдельный Supabase** от WB — схемы разные.
- **Отдельный Railway** — один сервис = один кабинет Ozon.
- При правках SPA: правишь `frontend/index.html`, копируй в `backend/frontend/index.html`.
