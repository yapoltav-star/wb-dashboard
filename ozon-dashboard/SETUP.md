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
| Цены и соинвест (цены v5 + индекс + акции) | работает |
| Заказы / отзывы / реклама / финансы | заглушки |

### Цены и соинвест

Аналог WB «Цены и СПП», но механика другая: на Ozon это **соинвест** (совместное финансирование скидки в акциях), а не СПП постоянного покупателя.

Данные: `/v5/product/info/prices` + `/v1/actions` (+ товары в акциях). Витринная `marketing_price` с ноября 2025 API часто не отдаёт — тогда соинвест считается по `action_price`.

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
2. Заказы / отправления FBS
3. Отзывы и рейтинг
4. Performance API (реклама)
5. Финансы / транзакции

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
