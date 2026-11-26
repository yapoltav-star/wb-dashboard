-- Миграция для уже созданного ozon-dashboard Supabase
-- SQL Editor → вставить → Run

-- Итоги по артикулу (весь Ozon)
create table if not exists public.stock_totals (
  offer_id       text primary key,
  product_id     bigint,
  sku            bigint,
  name           text,
  primary_image  text,
  fbo_present    integer default 0,
  fbo_reserved   integer default 0,
  fbs_present    integer default 0,
  fbs_reserved   integer default 0,
  stock_total    integer default 0,
  ordered_qty    integer default 0,
  period_days    integer,
  period_start   date,
  period_end     date,
  updated_at     timestamptz default now()
);
create index if not exists stock_totals_sku_idx on public.stock_totals (sku);

-- Детализация по складам
alter table public.stocks add column if not exists free_to_sell integer default 0;
alter table public.stocks add column if not exists ordered_qty integer default 0;
alter table public.stocks add column if not exists promised integer default 0;

drop index if exists stocks_unique_idx;
create unique index if not exists stocks_unique_idx
  on public.stocks (offer_id, warehouse_name, channel);
