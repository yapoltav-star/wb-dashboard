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

-- Отзывы (как feedbacks на WB): API Ozon → эта таблица → сайт читает отсюда
create table public.reviews (
  id                    text primary key,
  article               text,
  sku                   bigint,
  product_id            bigint,
  name                  text,
  primary_image         text,
  stars                 integer,
  created_date          timestamptz,
  text                  text,
  status                text,
  order_status          text,
  is_rating_participant boolean default true,
  photos_amount         integer default 0,
  videos_amount         integer default 0,
  comments_amount       integer default 0,
  updated_at            timestamptz default now()
);
create index reviews_article_idx on public.reviews (article);
create index reviews_sku_idx on public.reviews (sku);
create index reviews_product_id_idx on public.reviews (product_id);
create index reviews_created_date_idx on public.reviews (created_date);

-- Посчитанный рейтинг каждой карточки (как ratings_official на WB)
create table public.ratings (
  article        text primary key,
  product_id     bigint,
  sku            bigint,
  skus           jsonb default '[]'::jsonb,
  name           text,
  primary_image  text,
  avg_rating     numeric,
  r5             integer default 0,
  r4             integer default 0,
  r3             integer default 0,
  r2             integer default 0,
  r1             integer default 0,
  total          integer default 0,
  excluded       integer default 0,
  reviews_all    integer default 0,
  updated_at     timestamptz default now()
);
create index ratings_product_id_idx on public.ratings (product_id);

-- Склейки (как groups_config на WB)
create table public.groups_config (
  id         bigserial primary key,
  name       text not null,
  articles   jsonb not null default '[]'::jsonb,
  sort_order integer default 0
);

create or replace function public.get_article_stats()
returns table (
  article      text,
  product_id   bigint,
  sku          bigint,
  avg_rating   numeric,
  r5           bigint,
  r4           bigint,
  r3           bigint,
  r2           bigint,
  r1           bigint,
  total        bigint,
  excluded     bigint,
  reviews_all  bigint
)
language sql stable as $$
  select
    r.article,
    (array_agg(r.product_id order by r.created_date desc nulls last)
      filter (where r.product_id is not null))[1] as product_id,
    (array_agg(r.sku order by r.created_date desc nulls last)
      filter (where r.sku is not null))[1] as sku,
    round(
      (
        sum(case when r.is_rating_participant and r.stars = 5 then 5
                 when r.is_rating_participant and r.stars = 4 then 4
                 when r.is_rating_participant and r.stars = 3 then 3
                 when r.is_rating_participant and r.stars = 2 then 2
                 when r.is_rating_participant and r.stars = 1 then 1
                 else 0 end)::numeric
        / nullif(count(*) filter (where r.is_rating_participant and r.stars between 1 and 5), 0)
      ),
      2
    ) as avg_rating,
    count(*) filter (where r.is_rating_participant and r.stars = 5) as r5,
    count(*) filter (where r.is_rating_participant and r.stars = 4) as r4,
    count(*) filter (where r.is_rating_participant and r.stars = 3) as r3,
    count(*) filter (where r.is_rating_participant and r.stars = 2) as r2,
    count(*) filter (where r.is_rating_participant and r.stars = 1) as r1,
    count(*) filter (where r.is_rating_participant and r.stars between 1 and 5) as total,
    count(*) filter (where r.is_rating_participant is false) as excluded,
    count(*) as reviews_all
  from public.reviews r
  where r.article is not null and r.article <> ''
  group by r.article
  order by r.article;
$$;

create or replace function public.get_negative_counts(
  days_back integer default 7,
  max_stars integer default 3
)
returns table (
  article         text,
  negative_count  bigint
)
language sql stable as $$
  select r.article, count(*)::bigint as negative_count
  from public.reviews r
  where r.article is not null
    and r.article <> ''
    and r.stars <= max_stars
    and r.created_date >= (now() - make_interval(days => days_back))
  group by r.article;
$$;
