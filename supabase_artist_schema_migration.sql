-- Supabase migration for the stabilized FastAPI music discovery backend.
-- Run this before deploying the updated app.py so upserts write complete rows.

create table if not exists public.artists (
    name text primary key
);

alter table public.artists
    add column if not exists mbid text,
    add column if not exists spotify_id text,
    add column if not exists followers bigint,
    add column if not exists popularity integer,
    add column if not exists listeners bigint,
    add column if not exists playcount bigint,
    add column if not exists genres text[] default '{}',
    add column if not exists tags text[] default '{}',
    add column if not exists score integer,
    add column if not exists score_breakdown jsonb default '{}'::jsonb,
    add column if not exists match_score numeric,
    add column if not exists genre_match_score numeric,
    add column if not exists genre_families text[] default '{}',
    add column if not exists discovered_from text,
    add column if not exists growth_signal integer,
    add column if not exists growth_signal_reason text,
    add column if not exists url text,
    add column if not exists image text;

create unique index if not exists artists_name_unique_idx
    on public.artists (name);

create table if not exists public.seeds (
    name text primary key
);

create unique index if not exists seeds_name_unique_idx
    on public.seeds (name);
