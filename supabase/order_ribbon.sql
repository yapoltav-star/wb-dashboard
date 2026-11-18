-- Лента заказов: с какого склада куда уехал товар.
-- SQL Editor → Run один раз. Повторный запуск безопасен.

create table if not exists public.order_ribbon (
  order_id          text primary key,
  ordered_at        timestamptz,
  order_date        date,
  article           text,
  nm_id             bigint,
  name              text,
  channel           text,
  warehouse_type    text,
  src_region        text,
  src_warehouse     text,
  seller_warehouse  text,
  dest_region       text,
  dest_city         text,
  price             numeric,
  status            text,
  source            text,
  updated_at        timestamptz default now()
);
create index if not exists order_ribbon_date_idx on public.order_ribbon (order_date desc);
create index if not exists order_ribbon_src_wh_idx on public.order_ribbon (src_warehouse);
create index if not exists order_ribbon_dest_idx on public.order_ribbon (dest_region, dest_city);

grant all on table public.order_ribbon to service_role;
