// Cross-device Backtest-History sync via Supabase.
// Privacy: the public anon key below can do NOTHING on its own — the table has RLS with no
// policies, so access is only through two SECURITY-DEFINER functions (bt_get/bt_set) that require
// your secret SYNC CODE. The sync code is entered per device and stored only in that browser's
// localStorage (never committed, never sent anywhere except as the RPC argument). So your history
// is exactly as private as your sync code.
(function (g) {
  'use strict';
  const URL  = 'https://nebjnsndgrhumnkuipqy.supabase.co';
  const ANON = 'sb_publishable_MDlQwiVc5deii91__UNeDg_z9r4Fk98';   // publishable key — safe to embed (RLS + RPC-gated)
  const CODE_KEY = 'bt_sync_code', HIST_KEY = 'bt_history', CAP = 300;
  let _sb = null;
  function client() {
    if (!_sb && g.supabase && ANON.indexOf('PLACEHOLDER') < 0) {
      try { _sb = g.supabase.createClient(URL, ANON); } catch (e) { console.warn('supabase init', e); }
    }
    return _sb;
  }
  const code    = () => { try { return localStorage.getItem(CODE_KEY) || ''; } catch (e) { return ''; } };
  const setCode = c => { try { c ? localStorage.setItem(CODE_KEY, c.trim()) : localStorage.removeItem(CODE_KEY); } catch (e) {} };
  const _local  = () => { try { return JSON.parse(localStorage.getItem(HIST_KEY) || '[]'); } catch (e) { return []; } };
  const _saveLocal = a => { try { localStorage.setItem(HIST_KEY, JSON.stringify(a.slice(0, CAP))); } catch (e) {} };
  // union by id, newest first, capped — so no device ever loses another device's entries
  function _merge(a, b) {
    const m = {};
    [].concat(a || [], b || []).forEach(x => { if (x && x.id) m[x.id] = x; });
    return Object.values(m).sort((x, y) => (y.ts || 0) - (x.ts || 0)).slice(0, CAP);
  }
  // Pull remote -> merge with local -> save local -> (push back if local had extras) -> return merged.
  async function pull() {
    const c = code(), sb = client(), local = _local();
    if (!c || !sb) return local;
    try {
      const { data, error } = await sb.rpc('bt_get', { code: c });
      if (error) throw error;
      const remote = Array.isArray(data) ? data : (data || []);
      const merged = _merge(remote, local);
      _saveLocal(merged);
      if (merged.length > remote.length) { try { await sb.rpc('bt_set', { code: c, payload: merged }); } catch (e) {} }
      return merged;
    } catch (e) { console.warn('btPull', e && e.message || e); return local; }
  }
  // Push current local history to remote (fire-and-forget after a save).
  async function push() {
    const c = code(), sb = client();
    if (!c || !sb) return false;
    try { const { error } = await sb.rpc('bt_set', { code: c, payload: _local() }); if (error) throw error; return true; }
    catch (e) { console.warn('btPush', e && e.message || e); return false; }
  }
  g.btSync = { code, setCode, pull, push, configured: () => !!client() };
})(window);
