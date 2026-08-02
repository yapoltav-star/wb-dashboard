# WB Dashboard — установка своей копии

Один репозиторий = один кабинет WB. У каждого свой Railway + свой Supabase + свой токен.

## 1. GitHub

1. Создай пустой репозиторий (например `wb-dashboard`).
2. Получи доступ к коду от владельца (push в твой remote или fork).
3. Клонируй свой репо локально.

## 2. Supabase

1. [supabase.com](https://supabase.com) → New project.
2. SQL Editor → вставь файл `supabase/schema.sql` → Run.
   Если проект уже был создан раньше — для истории цен дополнительно выполни `supabase/price_snapshots.sql`.
3. Project Settings → API:
   - `Project URL` → это `SUPABASE_URL`
   - `service_role` key (secret) → это `SUPABASE_KEY`  
   **Не** anon key — бэкенд ходит с service_role.

## 3. Токен Wildberries

В кабинете продавца → Настройки → Доступ к API → создать токен.

Нужны категории примерно такие (как у основного дашборда):

- Контент
- Цены и скидки
- Статистика
- Аналитика
- Отзывы и вопросы
- Продвижение (реклама)
- Поставки / склады (если есть в списке)
- Маркетплейс — остатки FBS (склады продавца)

Скопируй токен → `WB_TOKEN` (без слова Bearer).

## 4. Railway

1. [railway.app](https://railway.app) → New Project → Deploy from GitHub (твой репо).
2. Root Directory: `backend` (там `main.py` и `nixpacks.toml`).
3. Variables:

```
WB_TOKEN=...
SUPABASE_URL=https://xxxx.supabase.co
SUPABASE_KEY=...
```

Опционально (остатки своего склада из Google Sheets):

```
OWN_WAREHOUSE_SHEET_ID=...
OWN_WAREHOUSE_GID=...
```

Таблица Sheets должна быть доступна по ссылке («все, у кого есть ссылка»).

4. Generate Domain → открой URL.

## 5. Первый запуск

1. Открой сайт → разделы сами начнут тянуть данные (или жми «Обновить»).
2. Отзывы: дождись синка (десятки минут на большом кабинете).
3. Остатки / поставки / реклама / СПП — кнопки обновления в соответствующих вкладках.
4. Финансы: PIN по умолчанию `1997` (файл `frontend/index.html`, константа `FINANCE_PIN`) — смени под себя и задеплой снова.
5. Склейки и кредиты — заполняй уже свои, чужих данных в шаблоне нет.

## 6. Что НЕ копировать у друга

- Его `WB_TOKEN`, Supabase, Railway variables
- Его Google Sheet
- Его домен / CRM

Код общий — кабинеты и секреты раздельные.

## Структура репо

```
backend/          ← деплой Railway (FastAPI)
  main.py
  frontend/       ← копия SPA для раздачи с бэка
  requirements.txt
  nixpacks.toml
frontend/         ← исходник SPA (при правках копируй в backend/frontend/)
supabase/
  schema.sql      ← схема БД
.env.example
SETUP.md
```

## Если что-то красное

| Симптом | Что проверить |
|--------|----------------|
| Пустой сайт / 502 | Логи Railway, Root Directory = `backend` |
| Нет данных | `WB_TOKEN`, права категорий API |
| Ошибки БД / 500 на dashboard-data | schema.sql накатили? service_role key? |
| «Наш склад» пустой | Не задан sheet id — это нормально, вкладка опциональна |
| СПП пустой | Токен с «Цены и скидки» + кнопка Обновить |
