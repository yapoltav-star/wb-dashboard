-- ozon-dashboard — схема для нового проекта Supabase
-- SQL Editor → New query → вставить целиком → Run

create extension if not exists "pgcrypto";

create table public.settings (
  key        text primary key,
  value      text,
  updated_at timestamptz default now()
);

-- Карточки товаров Ozon (основа для остатков, цен, отзывов)
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

-- Заготовка под остатки (FBO/FBS) — заполним во 2-й итерации
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
  updated_at     timestamptz default now()
);
create index stocks_product_id_idx on public.stocks (product_id);
create index stocks_offer_id_idx on public.stocks (offer_id);
create unique index stocks_unique_idx
  on public.stocks (sku, warehouse_id, channel);
