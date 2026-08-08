-- ozon-dashboard — схема для нового проекта Supabase
-- SQL Editor → New query → вставить целиком → Run

create extension if not exists "pgcrypto";

create table public.settings (
  key        text primary key,
  value      text,
  updated_at timestamptz default now()
);

create table public.products (
  product_id     bigint primary key,
  offer_id       text,
  sku            bigint,
  name           text,
  barcode        text,
  description_category_id bigint,
  type_id        bigint,
  currency_code  text,
  price          numeric,
  old_price      numeric,
  marketing_price numeric,
  vat            text,
  primary_image  text,
  images         jsonb default '[]'::jsonb,
  statuses       jsonb default '{}'::jsonb,
  visibility     text,
  has_fbo_stocks boolean default false,
  has_fbs_stocks boolean default false,
  archived       boolean default false,
  is_discounted  boolean default false,
  volume_weight  numeric,
  updated_at     timestamptz default now()
);
create index products_offer_id_idx on public.products (offer_id);
create index products_sku_idx on public.products (sku);
create index products_archived_idx on public.products (archived);

-- Итоги по артикулу (весь Ozon = FBO + FBS)
create table public.stock_totals (
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
create index stock_totals_sku_idx on public.stock_totals (sku);

-- Остатки по складам FBO / FBS
create table public.stocks (
  id             bigserial primary key,
  product_id     bigint,
  sku            bigint,
  offer_id       text,
  warehouse_id   bigint,
  warehouse_name text,
  channel        text, -- fbo | fbs
  present        integer default 0,
  reserved       integer default 0,
  free_to_sell   integer default 0,
  promised       integer default 0,
  ordered_qty    integer default 0,
  updated_at     timestamptz default now()
);
create index stocks_product_id_idx on public.stocks (product_id);
create index stocks_offer_id_idx on public.stocks (offer_id);
create unique index stocks_unique_idx
  on public.stocks (offer_id, warehouse_name, channel);
