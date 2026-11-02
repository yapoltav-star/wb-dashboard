-- Миграция: отзывы и оценки Ozon как на WB (API → Supabase → сайт)
-- SQL Editor проекта https://supabase.com/dashboard/project/bonfckavgdbgwteddxsk/sql/new
-- Вставить целиком → Run

create table if not exists public.reviews (
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
create index if not exists reviews_article_idx on public.reviews (article);
create index if not exists reviews_sku_idx on public.reviews (sku);
create index if not exists reviews_product_id_idx on public.reviews (product_id);
create index if not exists reviews_created_date_idx on public.reviews (created_date);

create table if not exists public.ratings (
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
create index if not exists ratings_product_id_idx on public.ratings (product_id);

create table if not exists public.groups_config (
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
