// Shared Backtest-History for STOCKSWORLD.
// OPEN TO ADD: every visitor auto-loads the shared history and can run a backtest to add to it
// (no login, no code). ERASING is owner-only in the UI: the Delete / Clear-all controls render only
// in a browser that holds 'bt_owner_key' (set once via ?ownerkey=…), so visitors can add but not
// wipe. (Writes are open at the API level — the owner-key just gates the erase UI; daily backups
// cover recovery.) The publishable key + write token below are public on purpose.
(function (g) {
  'use strict';
  const URL = 'https://nebjnsndgrhumnkuipqy.supabase.co';
  const ANON = 'sb_publishable_MDlQwiVc5deii91__UNeDg_z9r4Fk98';
  const WRITE = 'sw_owner_8Kq2Lm9Xp4Rt7v';
  const HIST_KEY = 'bt_history', OWNER_KEY = 'bt_owner_key', CAP = 300;
  let _sb = null;
  function client() { if (!_sb && g.supabase) { try { _sb = g.supabase.createClient(URL, ANON); } catch (e) { console.warn('supabase init', e); } } return _sb; }
  const _local = () => { try { return JSON.parse(localStorage.getItem(HIST_KEY) || '[]'); } catch (e) { return []; } };
  const _saveLocal = a => { try { localStorage.setItem(HIST_KEY, JSON.stringify(a.slice(0, CAP))); } catch (e) {} };
  const ownerKey = () => { try { return localStorage.getItem(OWNER_KEY) || ''; } catch (e) { return ''; } };
  // Read the shared history — auto-loaded for everyone.
  async function pull() {
    const sb = client(); if (!sb) return _local();
    try { const { data, error } = await sb.rpc('bt_public'); if (error) throw error;
      const remote = Array.isArray(data) ? data : (data || []); _saveLocal(remote); return remote;
    } catch (e) { console.warn('bt pull', e && e.message || e); return _local(); }
  }
  // Write the shared history — open (anyone can add; the owner can also erase via the UI).
  // push() REWRITES the whole array — use it only for owner erase/clear/reorder. For a plain
  // ADD use append() below, which is race-free.
  async function push() {
    const sb = client(); if (!sb) return false;
    try { const { data, error } = await sb.rpc('bt_owner_set', { secret: WRITE, payload: _local() }); if (error) throw error; return data === true; }
    catch (e) { console.warn('bt push', e && e.message || e); return false; }
  }
  // Race-free single-item ADD: the server reads+prepends+caps the shared array under a lock, so two
  // visitors saving at the same instant can't clobber each other (whole-array push() could). The caller
  // keeps the local mirror. If bt_append isn't deployed yet, fall back to the old whole-array push().
  async function append(item) {
    const sb = client(); if (!sb) return false;
    try { const { data, error } = await sb.rpc('bt_append', { secret: WRITE, item: item, cap: CAP }); if (error) throw error; return data === true; }
    catch (e) { console.warn('bt append → push fallback', e && e.message || e); return push(); }
  }
  // ---- Shared SAVED STRATEGIES (OPEN to add: every visitor reads AND publishes; erase is owner-only in the UI, like history) ----
  const STRAT_KEY = 'bt_strategies';
  const _localStr = () => { try { return JSON.parse(localStorage.getItem(STRAT_KEY) || '[]'); } catch (e) { return []; } };
  const _saveLocalStr = a => { try { localStorage.setItem(STRAT_KEY, JSON.stringify(a.slice(0, CAP))); } catch (e) {} };
  async function pullStrategies() {
    const sb = client(); if (!sb) return _localStr();
    try {
      const { data, error } = await sb.rpc('bt_strats_public'); if (error) throw error;
      const remote = Array.isArray(data) ? data : (data || []);
      // first run: if the shared list is empty but this (owner) browser has strategies, seed it up
      if (!remote.length && ownerKey() && _localStr().length) { await pushStrategies(); return _localStr(); }
      _saveLocalStr(remote); return remote;
    } catch (e) { console.warn('strat pull', e && e.message || e); return _localStr(); }
  }
  async function pushStrategies() {
    const sb = client(); if (!sb) return false;
    try { const { data, error } = await sb.rpc('bt_strats_set', { secret: WRITE, payload: _localStr() }); if (error) throw error; return data === true; }
    catch (e) { console.warn('strat push', e && e.message || e); return false; }
  }
  // Race-free single-strategy ADD (mirror of append() for the strategies list); whole-array
  // pushStrategies() fallback if bt_strats_append isn't deployed yet.
  async function appendStrategy(item) {
    const sb = client(); if (!sb) return false;
    try { const { data, error } = await sb.rpc('bt_strats_append', { secret: WRITE, item: item, cap: CAP }); if (error) throw error; return data === true; }
    catch (e) { console.warn('strat append → push fallback', e && e.message || e); return pushStrategies(); }
  }
  // ---- Full-result SNAPSHOTS (one row per run, fetched on demand — kept OUT of the whole-array
  //      history push so a heavy snapshot never bloats history sync). Backed by bt_snapshots table. ----
  async function snapGet(id) {
    const sb = client(); if (!sb || !id) return null;
    try { const { data, error } = await sb.rpc('bt_snap_get', { snap_id: id }); if (error) throw error; return data || null; }
    catch (e) { console.warn('snap get', e && e.message || e); return null; }
  }
  async function snapSet(id, payload) {
    const sb = client(); if (!sb || !id) return false;
    try { const { data, error } = await sb.rpc('bt_snap_set', { secret: WRITE, snap_id: id, payload }); if (error) throw error; return data === true; }
    catch (e) { console.warn('snap set', e && e.message || e); return false; }
  }
  g.btSync = { pull, push, append, pullStrategies, pushStrategies, appendStrategy, snapGet, snapSet, isOwner: () => !!ownerKey(), configured: () => !!client(),
    setOwnerKey: k => { try { k ? localStorage.setItem(OWNER_KEY, k) : localStorage.removeItem(OWNER_KEY); } catch (e) {} } };
  // Owner-UI unlock: open any page with ?ownerkey=YOURKEY once on a PC to reveal the Delete/Clear controls there.
  try {
    const u = new URL(location.href), ok = u.searchParams.get('ownerkey');
    if (ok) { localStorage.setItem(OWNER_KEY, ok); u.searchParams.delete('ownerkey'); history.replaceState(null, '', u.pathname + u.search + u.hash); }
  } catch (e) {}
})(window);
