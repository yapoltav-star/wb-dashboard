# Ozon Partners Dashboard

Отдельный дашборд продавца Ozon (не смешивать с WB).

Структура и деплой — как у WB-дашборда: FastAPI на Railway + Supabase + SPA.

## С чего начать (чеклист)

1. **Создай пустой GitHub-репозиторий** `ozon-dashboard` (или вынеси папку `ozon-dashboard/` из этого репо).
2. **Supabase** → New project → SQL Editor → выполни `supabase/schema.sql`.
3. **Токены Ozon**: кабинет продавца → Настройки → Seller API → `Client-Id` + `Api-Key`.
4. **Railway** → Deploy from GitHub → Root Directory: `backend` (если репо = содержимое этой папки).
5. Variables:

```
OZON_CLIENT_ID=...
OZON_API_KEY=...
SUPABASE_URL=https://xxxx.supabase.co
SUPABASE_KEY=...   # service_role, не anon
```

6. Открой сайт → **Обновить товары** — первый синк карточек.

## MVP (эта итерация)

| Раздел | Статус |
|--------|--------|
| Товары (list + info → Supabase) | работает |
| Остатки FBO/FBS | заготовка таблицы `stocks` |
| Заказы / отзывы / реклама / финансы | заглушки в UI |

## Дальше по приоритету

1. Остатки FBO (`/v1/analytics/stocks` или warehouse methods) + FBS
2. Заказы / отправления FBS
3. Отзывы и рейтинг
4. Performance API (реклама) — отдельные Client-Id / Secret
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
