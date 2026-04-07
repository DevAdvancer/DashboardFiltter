alter table if exists public.po_details
    add column if not exists is_hidden boolean default false,
    add column if not exists hidden_at timestamptz null,
    add column if not exists hidden_by_label text null;

update public.po_details
set is_hidden = false
where is_hidden is null;

alter table if exists public.po_details
    alter column is_hidden set default false;

alter table if exists public.po_details
    alter column is_hidden set not null;

create index if not exists idx_po_details_is_hidden
    on public.po_details (is_hidden);
