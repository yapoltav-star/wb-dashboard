-- История цен для вкладки «Цены и СПП»
-- Supabase → SQL Editor → Run

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

grant all on public.price_snapshots to service_role;
grant all on sequence public.price_snapshots_id_seq to service_role;
