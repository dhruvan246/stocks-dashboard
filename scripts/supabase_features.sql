-- ============================================================================
-- STOCKSWORLD site features backed by Supabase (project nebjnsndgrhumnkuipqy):
--
--   1. sw_kv         — small named JSON documents ("one notebook page per key"):
--                      WATCHLIST (starred stocks + notes), TRIAGE (discovery
--                      interesting/rejected marks), SETTINGS (cross-device
--                      preference sync), INSURER_INBOX (manual PAT entries the
--                      nightly CI applies), PRESETS (spare for future use).
--   2. sw_page_views — one counter per (IST day, page): which pages get used.
--   3. sw_picks_log  — one row per (data-day, strategy): that day's Today's
--                      Picks with entry prices — the append-only memory that
--                      powers live forward-tracking of saved strategies.
--
-- Same security model as the existing backtest history (bt_public/bt_owner_set):
-- reads are public; writes go through SECURITY DEFINER functions that check the
-- site's public write token. The token is deliberately public (open site, no
-- login); daily GitHub backups are the recovery story. Tables have RLS enabled
-- with NO policies, so the anon key cannot touch them except via these RPCs.
--
-- DEPLOY (one time): Supabase dashboard → SQL Editor → paste this whole file →
-- Run. Safe to re-run (CREATE OR REPLACE / IF NOT EXISTS). Until deployed, the
-- site's sw-sync.js falls back to browser-local storage, so nothing breaks.
-- ============================================================================

-- ---------------------------------------------------------------- 1. sw_kv --
create table if not exists sw_kv (
  k          text primary key,
  payload    jsonb not null default '[]'::jsonb,
  updated_at timestamptz not null default now()
);
alter table sw_kv enable row level security;

-- Whitelist: only these documents exist. Extend by re-running with a new key.
create or replace function sw_kv_ok(k text) returns boolean
language sql immutable as $$
  select k in ('WATCHLIST','TRIAGE','SETTINGS','INSURER_INBOX','PRESETS')
$$;

create or replace function sw_kv_get(k text) returns jsonb
language sql security definer stable as $$
  select payload from sw_kv where sw_kv.k = sw_kv_get.k
$$;

create or replace function sw_kv_set(secret text, k text, payload jsonb) returns boolean
language plpgsql security definer as $$
#variable_conflict use_variable
begin
  if secret <> 'sw_owner_8Kq2Lm9Xp4Rt7v' or not sw_kv_ok(k) then return false; end if;
  if pg_column_size(payload) > 2000000 then return false; end if;  -- 2 MB sanity cap
  insert into sw_kv as t (k, payload, updated_at) values (k, payload, now())
  on conflict (k) do update set payload = excluded.payload, updated_at = now();
  return true;
end $$;

-- Race-free single-item PREPEND (same advisory-lock pattern as bt_append):
-- concurrent adds from two devices serialize server-side, nobody's entry lost.
create or replace function sw_kv_append(secret text, k text, item jsonb, cap int default 500)
returns boolean
language plpgsql security definer as $$
#variable_conflict use_variable
declare cur jsonb; merged jsonb;
begin
  if secret <> 'sw_owner_8Kq2Lm9Xp4Rt7v' or not sw_kv_ok(k) then return false; end if;
  perform pg_advisory_xact_lock(hashtext('sw_kv_' || k));
  cur := sw_kv_get(k);
  if cur is null or jsonb_typeof(cur) <> 'array' then cur := '[]'::jsonb; end if;
  merged := jsonb_build_array(item) || cur;                       -- newest first
  if jsonb_array_length(merged) > cap then
    merged := (select coalesce(jsonb_agg(e order by n), '[]'::jsonb)
               from jsonb_array_elements(merged) with ordinality as t(e, n)
               where n <= cap);
  end if;
  return sw_kv_set(secret, k, merged);
end $$;

-- --------------------------------------------------------- 2. sw_page_views --
create table if not exists sw_page_views (
  day  date not null,
  page text not null,
  hits int  not null default 0,
  primary key (day, page)
);
alter table sw_page_views enable row level security;

-- Open increment (that's the point — every visit counts). IST day boundary.
create or replace function sw_pv_hit(page text) returns boolean
language plpgsql security definer as $$
#variable_conflict use_variable
begin
  if page is null or length(page) < 1 or length(page) > 80 then return false; end if;
  insert into sw_page_views (day, page, hits)
  values ((now() at time zone 'Asia/Kolkata')::date, page, 1)
  on conflict (day, page) do update set hits = sw_page_views.hits + 1;
  return true;
end $$;

create or replace function sw_pv_stats(days int default 90) returns jsonb
language sql security definer stable as $$
  select coalesce(jsonb_agg(jsonb_build_object('day', day, 'page', page, 'hits', hits)
                            order by day desc, hits desc), '[]'::jsonb)
  from sw_page_views
  where day >= (now() at time zone 'Asia/Kolkata')::date - days
$$;

-- ---------------------------------------------------------- 3. sw_picks_log --
create table if not exists sw_picks_log (
  day        date  not null,          -- the DATA date the picks were computed for
  strat_id   text  not null,          -- saved-strategy id
  payload    jsonb not null,          -- {name, picks:[{s,p}], ...} incl. entry prices
  created_at timestamptz not null default now(),
  primary key (day, strat_id)
);
alter table sw_picks_log enable row level security;

-- Upsert (idempotent: weekend/holiday reruns hit the same data-day and rewrite it).
create or replace function sw_picks_set(secret text, day_in date, sid text, payload jsonb)
returns boolean
language plpgsql security definer as $$
#variable_conflict use_variable
begin
  if secret <> 'sw_owner_8Kq2Lm9Xp4Rt7v' then return false; end if;
  if sid is null or length(sid) > 120 or day_in is null then return false; end if;
  if pg_column_size(payload) > 200000 then return false; end if;
  insert into sw_picks_log as t (day, strat_id, payload) values (day_in, sid, payload)
  on conflict (day, strat_id) do update set payload = excluded.payload, created_at = now();
  return true;
end $$;

create or replace function sw_picks_get(sid text default null, days int default 800)
returns jsonb
language sql security definer stable as $$
  select coalesce(jsonb_agg(jsonb_build_object('day', day, 'sid', strat_id, 'p', payload)
                            order by day asc), '[]'::jsonb)
  from sw_picks_log
  where (sid is null or strat_id = sid)
    and day >= (now() at time zone 'Asia/Kolkata')::date - days
$$;

-- ------------------------------------------------------------------ grants --
grant execute on function sw_kv_ok(text)                          to anon, authenticated;
grant execute on function sw_kv_get(text)                         to anon, authenticated;
grant execute on function sw_kv_set(text, text, jsonb)            to anon, authenticated;
grant execute on function sw_kv_append(text, text, jsonb, int)    to anon, authenticated;
grant execute on function sw_pv_hit(text)                         to anon, authenticated;
grant execute on function sw_pv_stats(int)                        to anon, authenticated;
grant execute on function sw_picks_set(text, date, text, jsonb)   to anon, authenticated;
grant execute on function sw_picks_get(text, int)                 to anon, authenticated;

-- ------------------------------------------------------- quick self-test ----
-- select sw_pv_hit('selftest.html');            -- expect: t
-- select sw_pv_stats(1);                        -- expect: [{"day":...,"page":"selftest.html","hits":1}]
-- select sw_kv_set('sw_owner_8Kq2Lm9Xp4Rt7v','WATCHLIST','[]'::jsonb);  -- expect: t
-- select sw_kv_get('WATCHLIST');                -- expect: []
