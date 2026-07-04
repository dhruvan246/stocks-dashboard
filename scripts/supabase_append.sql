-- ============================================================================
-- Atomic, race-free single-item prepend for the OPEN shared Backtest History
-- and Saved Strategies (Supabase project nebjnsndgrhumnkuipqy).
--
-- WHY: the site used to save by REWRITING the whole array (bt_owner_set /
-- bt_strats_set with the full payload). Two visitors saving in the same instant
-- each read the array, add their entry, and write it back — whoever writes
-- SECOND overwrites the first (last-write-wins → the other person's entry is
-- silently lost).
--
-- FIX: these functions read + prepend + cap the array ENTIRELY server-side,
-- inside one call, under a transaction advisory lock — so concurrent adds
-- serialize and nobody's entry is lost. They reuse the existing bt_public /
-- bt_owner_set (and the strategies pair), so NO table names are assumed here;
-- if your storage schema ever changes, these keep working as long as those
-- four public functions do.
--
-- DEPLOY (one time): Supabase dashboard → SQL Editor → paste this whole file →
-- Run. Safe to re-run (CREATE OR REPLACE). Until it's deployed the site falls
-- back to the old whole-array push automatically, so nothing breaks in between.
-- ============================================================================

create or replace function bt_append(secret text, item jsonb, cap int default 300)
returns boolean
language plpgsql
security definer
as $$
declare
  cur jsonb;
  merged jsonb;
begin
  -- serialize concurrent appends so no read-modify-write can race
  perform pg_advisory_xact_lock(hashtext('sw_bt_history'));
  cur := to_jsonb(bt_public());
  if cur is null or jsonb_typeof(cur) <> 'array' then
    cur := '[]'::jsonb;
  end if;
  merged := jsonb_build_array(item) || cur;                 -- newest first
  if jsonb_array_length(merged) > cap then
    merged := (
      select coalesce(jsonb_agg(e order by n), '[]'::jsonb)
      from jsonb_array_elements(merged) with ordinality as t(e, n)
      where n <= cap
    );
  end if;
  return bt_owner_set(secret, merged);
end
$$;

create or replace function bt_strats_append(secret text, item jsonb, cap int default 300)
returns boolean
language plpgsql
security definer
as $$
declare
  cur jsonb;
  merged jsonb;
begin
  perform pg_advisory_xact_lock(hashtext('sw_bt_strategies'));
  cur := to_jsonb(bt_strats_public());
  if cur is null or jsonb_typeof(cur) <> 'array' then
    cur := '[]'::jsonb;
  end if;
  merged := jsonb_build_array(item) || cur;
  if jsonb_array_length(merged) > cap then
    merged := (
      select coalesce(jsonb_agg(e order by n), '[]'::jsonb)
      from jsonb_array_elements(merged) with ordinality as t(e, n)
      where n <= cap
    );
  end if;
  return bt_strats_set(secret, merged);
end
$$;

-- Let the public (publishable / anon) key call them, same as the existing RPCs.
grant execute on function bt_append(text, jsonb, int)        to anon, authenticated;
grant execute on function bt_strats_append(text, jsonb, int) to anon, authenticated;

-- ---------------------------------------------------------------------------
-- Quick self-test (optional). Adds a throwaway row, then you can delete it from
-- the History page as owner, or just leave it — it's harmless.
--   select bt_append('sw_owner_8Kq2Lm9Xp4Rt7v',
--                    '{"id":"selftest","ts":0,"label":"append self-test","cfg":{"start":"2020-01-01","end":"2020-02-01"},"m":{}}'::jsonb);
-- Expected result: t (true). If it errors, send me the message and the output of:
--   select pg_get_functiondef('bt_public'::regproc), pg_get_functiondef('bt_owner_set'::regproc);
-- ---------------------------------------------------------------------------
