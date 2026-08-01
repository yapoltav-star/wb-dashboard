-- wb-dashboard — схема для нового проекта Supabase
-- SQL Editor → New query → вставить целиком → Run

create extension if not exists "pgcrypto";

create table public.settings (
  key        text primary key,
  value      text,
  updated_at timestamptz default now()
);

create table public.feedbacks (
  id           text primary key,
  article      text,
  nm_id        bigint,
  stars        integer,
  created_date timestamptz,
  is_old       boolean default false,
  is_answered  boolean default false,
  text         text,
  updated_at   timestamptz default now()
);
create index feedbacks_article_idx on public.feedbacks (article);
create index feedbacks_nm_id_idx on public.feedbacks (nm_id);
create index feedbacks_created_date_idx on public.feedbacks (created_date);

create table public.stock_totals (
  nm_id                     bigint primary key,
  vendor_code               text,
  subject_name              text,
  brand                     text,
  volume                    numeric,
  in_way_to_client          integer default 0,
  in_way_from_client        integer default 0,
  quantity_warehouses_full  integer default 0,
  updated_at                timestamptz default now()
);

create table public.stock_warehouses (
  id             bigserial primary key,
  nm_id          bigint not null,
  warehouse_name text,
  quantity       integer default 0,
  updated_at     timestamptz default now()
);
create index stock_warehouses_nm_id_idx on public.stock_warehouses (nm_id);

create table public.supply_report (
  id                  bigserial primary key,
  vendor_code         text,
  nm_id               bigint,
  barcode             text,
  planned_supply_qty  integer default 0,
  warehouse_name      text,
  ordered_qty         integer default 0,
  buyout_qty          integer default 0,
  current_stock       integer default 0,
  period_days         integer,
  period_start        date,
  period_end          date,
  updated_at          timestamptz default now()
);
create index supply_report_nm_id_idx on public.supply_report (nm_id);

create table public.ad_stats (
  id            bigserial primary key,
  campaign_id   bigint,
  campaign_name text,
  campaign_type text,
  views         bigint default 0,
  clicks        bigint default 0,
  atbs          bigint default 0,
  orders        bigint default 0,
  spend         numeric default 0,
  revenue       numeric default 0,
  drr           numeric default 0,
  ctr           numeric default 0,
  cpc           numeric default 0,
  cr            numeric default 0,
  cv_atb        numeric default 0,
  cv_ord        numeric default 0,
  period_days   integer,
  vendor_code   text,
  nm_id         bigint,
  updated_at    timestamptz default now()
);

create table public.article_daily_stats (
  nm_id        bigint not null,
  vendor_code  text,
  dt           date not null,
  open_card    integer default 0,
  add_to_cart  integer default 0,
  orders       integer default 0,
  orders_sum   numeric default 0,
  buyouts      integer default 0,
  buyouts_sum  numeric default 0,
  cancels      integer default 0,
  ctr          numeric default 0,
  cart_conv    numeric default 0,
  order_conv   numeric default 0,
  buyout_pct   numeric default 0,
  updated_at   timestamptz default now(),
  primary key (nm_id, dt)
);

create table public.ratings_official (
  id             bigserial primary key,
  article        text not null unique,
  nm_id          bigint,
  name           text,
  wb_rating      numeric,
  reviews_total  integer default 0,
  r5             integer default 0,
  r4             integer default 0,
  r3             integer default 0,
  r2             integer default 0,
  r1             integer default 0,
  excluded       integer default 0,
  source         text,
  updated_at     timestamptz default now()
);

create table public.groups_config (
  id         bigserial primary key,
  name       text not null,
  articles   jsonb not null default '[]'::jsonb,
  sort_order integer default 0
);

create table public.competitor_sessions (
  id           bigserial primary key,
  period_begin date,
  period_end   date,
  uploaded_at  timestamptz default now()
);

create table public.competitor_metrics (
  id              bigserial primary key,
  session_id      bigint not null references public.competitor_sessions(id) on delete cascade,
  nm_id           bigint,
  is_own          boolean default false,
  name            text,
  brand           text,
  card_rating     numeric,
  feedback_rating numeric,
  reviews_count   integer,
  price           numeric,
  median_price    numeric,
  delivery_time   text,
  avg_position    numeric,
  views           integer,
  card_opens      integer,
  ctr             numeric,
  cart_adds       integer,
  cart_conv       numeric,
  orders          integer,
  order_conv      numeric,
  buyouts         integer,
  buyout_pct      numeric,
  cancels         integer,
  updated_at      timestamptz default now()
);

create table public.competitor_search_queries (
  id                bigserial primary key,
  session_id        bigint not null references public.competitor_sessions(id) on delete cascade,
  query             text,
  query_count       integer default 0,
  query_count_prev  integer default 0,
  cart_conv_by_nm   jsonb default '{}'::jsonb,
  updated_at        timestamptz default now()
);

-- Снимки цен (кабинет + витрина) при каждом sync СПП
create table if not exists public.price_snapshots (
  id            bigserial primary key,
  nm_id         bigint not null,
  vendor_code   text,
  price         numeric,
  sale_price    numeric,
  client_price  numeric,
  spp           numeric,
  captured_at   timestamptz not null default now()
);
create index if not exists price_snapshots_nm_captured_idx
  on public.price_snapshots (nm_id, captured_at desc);
create index if not exists price_snapshots_captured_idx
  on public.price_snapshots (captured_at desc);

create or replace function public.get_article_stats()
returns table (
  article     text,
  nm_id       bigint,
  avg_rating  numeric,
  r5          bigint,
  r4          bigint,
  r3          bigint,
  r2          bigint,
  r1          bigint,
  total       bigint,
  excluded    bigint,
  old_count   bigint,
  old_pct     numeric
)
language sql stable as $$
  select
    f.article,
    (array_agg(f.nm_id order by f.created_date desc nulls last)
      filter (where f.nm_id is not null))[1] as nm_id,
    round(
      (
        sum(case when f.stars = 5 then 5 when f.stars = 4 then 4
                 when f.stars = 3 then 3 when f.stars = 2 then 2
                 when f.stars = 1 then 1 else 0 end)::numeric
        / nullif(count(*) filter (where f.stars between 1 and 5), 0)
      ),
      2
    ) as avg_rating,
    count(*) filter (where f.stars = 5) as r5,
    count(*) filter (where f.stars = 4) as r4,
    count(*) filter (where f.stars = 3) as r3,
    count(*) filter (where f.stars = 2) as r2,
    count(*) filter (where f.stars = 1) as r1,
    count(*) as total,
    0::bigint as excluded,
    count(*) filter (where f.is_old) as old_count,
    round(100.0 * count(*) filter (where f.is_old) / nullif(count(*), 0), 2) as old_pct
  from public.feedbacks f
  where f.article is not null and f.article <> ''
  group by f.article
  order by f.article;
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
  select f.article, count(*)::bigint as negative_count
  from public.feedbacks f
  where f.article is not null
    and f.article <> ''
    and f.stars <= max_stars
    and f.created_date >= (now() - make_interval(days => days_back))
  group by f.article;
$$;

grant usage on schema public to anon, authenticated, service_role;
grant all on all tables in schema public to service_role;
grant all on all sequences in schema public to service_role;
grant execute on function public.get_article_stats() to anon, authenticated, service_role;
grant execute on function public.get_negative_counts(integer, integer) to anon, authenticated, service_role;
