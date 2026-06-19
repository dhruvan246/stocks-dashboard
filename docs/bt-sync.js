// Shared, OPEN Backtest-History for STOCKSWORLD.
// Every visitor auto-loads the one shared history AND can update/overwrite it — run a backtest to
// add one, or Delete/Clear to remove. No login, no code, no owner. The publishable key + write
// token below are public on purpose (open write); RLS is on so access is only via the 2 functions.
(function (g) {
  'use strict';
  const URL = 'https://nebjnsndgrhumnkuipqy.supabase.co';
  const ANON = 'sb_publishable_MDlQwiVc5deii91__UNeDg_z9r4Fk98';
  const WRITE = 'sw_owner_8Kq2Lm9Xp4Rt7v';   // open write token — public on purpose (anyone may update)
  const HIST_KEY = 'bt_history', CAP = 300;
  let _sb = null;
  function client() { if (!_sb && g.supabase) { try { _sb = g.supabase.createClient(URL, ANON); } catch (e) { console.warn('supabase init', e); } } return _sb; }
  const _local = () => { try { return JSON.parse(localStorage.getItem(HIST_KEY) || '[]'); } catch (e) { return []; } };
  const _saveLocal = a => { try { localStorage.setItem(HIST_KEY, JSON.stringify(a.slice(0, CAP))); } catch (e) {} };
  // Read the shared history — auto-loaded for everyone.
  async function pull() {
    const sb = client(); if (!sb) return _local();
    try { const { data, error } = await sb.rpc('bt_public'); if (error) throw error;
      const remote = Array.isArray(data) ? data : (data || []); _saveLocal(remote); return remote;
    } catch (e) { console.warn('bt pull', e && e.message || e); return _local(); }
  }
  // Write the shared history — open to everyone.
  async function push() {
    const sb = client(); if (!sb) return false;
    try { const { data, error } = await sb.rpc('bt_owner_set', { secret: WRITE, payload: _local() }); if (error) throw error; return data === true; }
    catch (e) { console.warn('bt push', e && e.message || e); return false; }
  }
  g.btSync = { pull, push, configured: () => !!client() };
})(window);
