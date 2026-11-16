-- Остаток конкурента на «Склад WB РФ» из Excel «Сравнение карточек».
-- Выполни в Supabase SQL Editor, если колонки ещё нет.
alter table public.competitor_metrics
  add column if not exists stock_wb_rf integer;
