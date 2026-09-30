-- Единый физический склад (WB + Ozon).
-- Сейчас приложение хранит остатки/движения в public.settings (JSON),
-- этот файл — целевая нормализованная схема на будущее.
-- SQL Editor → Run (опционально)

create table if not exists public.own_stock (
  vendor_code  text primary key,          -- канонический артикул (как на WB / в Sheets)
  name         text,
  qty          integer not null default 0, -- актуальный остаток на нашем складе
  family_root  text,
  updated_at   timestamptz default now()
);
create index if not exists own_stock_family_root_idx on public.own_stock (family_root);

-- Алиасы: offer_id Ozon / другой SKU → канонический vendor_code
create table if not exists public.own_sku_aliases (
  alias_sku     text primary key,
  vendor_code   text not null references public.own_stock(vendor_code) on delete cascade,
  marketplace   text, -- wb | ozon | other
  updated_at    timestamptz default now()
);

-- Активные и архивные документы движений
create table if not exists public.own_stock_docs (
  id            text primary key,
  doc_type      text not null,   -- receipt | shipment
  channel       text,            -- in | fbw | fbs | ozon_fbo | ozon_fbs
  marketplace   text,            -- wb | ozon | shared
  filename      text,
  note          text,
  total_qty     integer default 0,
  articles      integer default 0,
  items         jsonb not null default '[]'::jsonb,
  created_at    timestamptz default now(),
  archived_at   timestamptz,     -- null = активный оверлей; иначе в архиве
  archive_id    text,            -- группа пятничного сброса
  archive_note  text
);
create index if not exists own_stock_docs_active_idx
  on public.own_stock_docs (doc_type) where archived_at is null;
create index if not exists own_stock_docs_archive_idx
  on public.own_stock_docs (archive_id, archived_at desc);

-- Снимки инвентаризации (пятница / Sheets)
create table if not exists public.own_stock_inventories (
  id            text primary key,
  as_of         text,
  note          text,
  source        text default 'sheets', -- sheets | manual
  snapshot      jsonb not null default '{}'::jsonb, -- by_vendor
  created_at    timestamptz default now()
);
