// Public Backtest-History for STOCKSWORLD.
// Everyone who opens the site auto-loads the OWNER's showcase history — no login, no code to type.
// Only the owner's own device(s) can UPDATE it: a one-time secret 'bt_owner_key' kept in that
// browser's localStorage is checked server-side by bt_owner_set(); visitors don't have it, so they
// are read-only. The publishable key below is public-safe (RLS on; access only via the 2 functions).
(function (g) {
  'use strict';
  const URL = 'https://nebjnsndgrhumnkuipqy.supabase.co';
  const ANON = 'sb_publishable_MDlQwiVc5deii91__UNeDg_z9r4Fk98';
  const HIST_KEY = 'bt_history', OWNER_KEY = 'bt_owner_key', CAP = 300;
  let _sb = null;
  function client() { if (!_sb && g.supabase) { try { _sb = g.supabase.createClient(URL, ANON); } catch (e) { console.warn('supabase init', e); } } return _sb; }
  const _local = () => { try { return JSON.parse(localStorage.getItem(HIST_KEY) || '[]'); } catch (e) { return []; } };
  const _saveLocal = a => { try { localStorage.setItem(HIST_KEY, JSON.stringify(a.slice(0, CAP))); } catch (e) {} };
  const ownerKey = () => { try { return localStorage.getItem(OWNER_KEY) || ''; } catch (e) { return ''; } };
  function _merge(a, b) { const m = {}; [].concat(a || [], b || []).forEach(x => { if (x && x.id) m[x.id] = x; }); return Object.values(m).sort((x, y) => (y.ts || 0) - (x.ts || 0)).slice(0, CAP); }
  // Public read — the owner's showcase history, loaded automatically for every visitor.
  async function pull() {
    const sb = client(); if (!sb) return _local();
    try {
      const { data, error } = await sb.rpc('bt_public'); if (error) throw error;
      const remote = Array.isArray(data) ? data : (data || []);
      if (ownerKey()) {                       // owner device: keep public canonical but never lose un-pushed local runs
        const merged = _merge(remote, _local()); _saveLocal(merged);
        if (merged.length > remote.length) push();
        return merged;
      }
      _saveLocal(remote); return remote;       // visitor: just show the public showcase
    } catch (e) { console.warn('bt pull', e && e.message || e); return _local(); }
  }
  // Owner write — only fires on a device that holds the owner key (visitors: no-op).
  async function push() {
    const sb = client(), k = ownerKey(); if (!sb || !k) return false;
    try { const { data, error } = await sb.rpc('bt_owner_set', { secret: k, payload: _local() }); if (error) throw error; return data === true; }
    catch (e) { console.warn('bt push', e && e.message || e); return false; }
  }
  g.btSync = { pull, push, isOwner: () => !!ownerKey(), configured: () => !!client(),
    setOwnerKey: k => { try { k ? localStorage.setItem(OWNER_KEY, k) : localStorage.removeItem(OWNER_KEY); } catch (e) {} } };
})(window);
