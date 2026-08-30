// STOCKSWORLD site-features sync — tiny fetch-based Supabase client (no supabase-js).
// Loaded on EVERY page by theme.js. Powers: watchlist/notes, discovery triage,
// cross-device settings sync, insurer inbox, page-view counter, picks-log reads.
//
// Model (same as bt-sync.js): reads public; writes carry the site's public write
// token (open site, no login — daily GitHub backups are the recovery story).
// OFFLINE / SQL-not-deployed: every kv falls back to a browser-local mirror and
// re-pushes when the backend is reachable again ("dirty flag"), so features keep
// working locally and heal themselves.
(function (g) {
  'use strict';
  if (g.swSync) return; // theme.js auto-loads this everywhere — guard double include
  const URL = 'https://nebjnsndgrhumnkuipqy.supabase.co/rest/v1/rpc/';
  const ANON = 'sb_publishable_MDlQwiVc5deii91__UNeDg_z9r4Fk98';
  const WRITE = 'sw_owner_8Kq2Lm9Xp4Rt7v';
  const OWNER_KEY = 'bt_owner_key'; // same owner unlock as the backtest pages

  async function rpc(fn, args) {
    const r = await fetch(URL + fn, {
      method: 'POST',
      headers: { apikey: ANON, Authorization: 'Bearer ' + ANON, 'Content-Type': 'application/json' },
      body: JSON.stringify(args || {}),
    });
    if (!r.ok) throw new Error(fn + ' HTTP ' + r.status);
    const t = await r.text();
    return t ? JSON.parse(t) : null;
  }

  // ---- local mirror helpers -------------------------------------------------
  const _get = k => { try { return JSON.parse(localStorage.getItem('swkv_' + k) || 'null'); } catch (e) { return null; } };
  const _put = (k, v) => { try { localStorage.setItem('swkv_' + k, JSON.stringify(v)); } catch (e) {} };
  const _dirty = k => { try { localStorage.setItem('swkv_dirty_' + k, '1'); } catch (e) {} };
  const _clean = k => { try { localStorage.removeItem('swkv_dirty_' + k); } catch (e) {} };
  const _isDirty = k => { try { return !!localStorage.getItem('swkv_dirty_' + k); } catch (e) { return false; } };

  const ownerKey = () => { try { return localStorage.getItem(OWNER_KEY) || ''; } catch (e) { return ''; } };

  // ---- kv documents (WATCHLIST / TRIAGE / SETTINGS / INSURER_INBOX / PRESETS)
  // kvGet: remote wins (and refreshes the mirror); mirror when offline.
  async function kvGet(k) {
    try {
      if (_isDirty(k)) { // local changes never yet pushed — push first so they aren't lost
        const ok = await rpc('sw_kv_set', { secret: WRITE, k, payload: _get(k) || [] });
        if (ok === true) _clean(k);
      }
      const v = await rpc('sw_kv_get', { k });
      if (v != null) { _put(k, v); return v; }
      return _get(k) || [];
    } catch (e) { return _get(k) || []; }
  }
  // kvSet: mirror always; remote best-effort (dirty flag heals on next kvGet).
  async function kvSet(k, payload) {
    _put(k, payload); _dirty(k);
    try {
      const ok = await rpc('sw_kv_set', { secret: WRITE, k, payload });
      if (ok === true) { _clean(k); return true; }
      return false;
    } catch (e) { return false; }
  }
  // kvAppend: race-free server-side prepend; offline falls back to local prepend.
  async function kvAppend(k, item, cap) {
    const cur = _get(k) || [];
    _put(k, [item].concat(cur).slice(0, cap || 500));
    try {
      const ok = await rpc('sw_kv_append', { secret: WRITE, k, item, cap: cap || 500 });
      if (ok === true) { const v = await rpc('sw_kv_get', { k }); if (v != null) _put(k, v); return true; }
      _dirty(k); return false;
    } catch (e) { _dirty(k); return false; }
  }

  // ---- page-view counter (fire-and-forget, once per page per session) -------
  function pvHit() {
    try {
      const page = (location.pathname.split('/').pop() || 'index.html').toLowerCase();
      if (sessionStorage.getItem('swpv_' + page)) return;
      sessionStorage.setItem('swpv_' + page, '1');
      rpc('sw_pv_hit', { page }).catch(() => {});
    } catch (e) {}
  }
  const pvStats = days => rpc('sw_pv_stats', { days: days || 90 });

  // ---- picks log (written by the daily CI logger; pages only read) ----------
  const picksGet = (sid, days) => rpc('sw_picks_get', { sid: sid || null, days: days || 800 });
  const picksSet = (day, sid, payload) => rpc('sw_picks_set', { secret: WRITE, day_in: day, sid, payload });

  // ---- cross-device settings sync -------------------------------------------
  // Syncs a whitelist of localStorage keys through kv 'SETTINGS' ([{k,v,ts}]).
  // Pull-on-load: any remote entry newer than our local stamp is applied.
  // Push-on-leave: keys whose value changed during this visit get stamped + pushed.
  // Only OWNER browsers push (visitors keep local-only settings); everyone pulls nothing
  // sensitive — these are UI preferences.
  const SETTINGS_KEYS = []; // filled by theme.js via swSync.syncSettings([...keys])
  const _sTs = k => { try { return +(localStorage.getItem('swset_ts_' + k) || 0); } catch (e) { return 0; } };
  const _sStamp = (k, ts) => { try { localStorage.setItem('swset_ts_' + k, String(ts)); } catch (e) {} };
  let _snap = {}, _remoteExtra = [];   // _remoteExtra: SETTINGS entries this page's key list doesn't know
  async function syncSettings(keys) {
    if (keys && keys.length) SETTINGS_KEYS.splice(0, SETTINGS_KEYS.length, ...keys);
    if (!SETTINGS_KEYS.length) return;
    try {
      const remote = await kvGet('SETTINGS');
      const map = {}; (Array.isArray(remote) ? remote : []).forEach(e => { if (e && e.k) map[e.k] = e; });
      // Remember the entries we DON'T track so a later push can carry them through unchanged —
      // pushSettings used to replace the whole doc with only its own keys, so any tab running an
      // older cached theme.js silently DELETED every newer synced key (measured 2026-08-30: a
      // pre-v122 tab's pagehide push dropped mix_state_v1 minutes after it was added).
      _remoteExtra = (Array.isArray(remote) ? remote : []).filter(e => e && e.k && SETTINGS_KEYS.indexOf(e.k) < 0);
      SETTINGS_KEYS.forEach(k => {
        const r = map[k];
        if (r && r.ts > _sTs(k)) { // remote newer → apply
          try { r.v == null ? localStorage.removeItem(k) : localStorage.setItem(k, r.v); } catch (e) {}
          _sStamp(k, r.ts);
        }
      });
    } catch (e) {}
    _snap = {}; SETTINGS_KEYS.forEach(k => { try { _snap[k] = localStorage.getItem(k); } catch (e) {} });
  }
  async function pushSettings() {
    if (!SETTINGS_KEYS.length || !ownerKey()) return;
    let changed = false;
    SETTINGS_KEYS.forEach(k => { let v = null; try { v = localStorage.getItem(k); } catch (e) {}
      if (v !== _snap[k]) { _sStamp(k, Date.now()); _snap[k] = v; changed = true; } });
    if (!changed) return;
    // MERGE, never replace: our keys, plus the remote-doc entries we don't track (kept verbatim
    // from the last pull — no network read here; this can run on pagehide).
    const out = SETTINGS_KEYS.map(k => { let v = null; try { v = localStorage.getItem(k); } catch (e) {}
      return { k, v, ts: _sTs(k) || Date.now() }; }).concat(_remoteExtra);
    await kvSet('SETTINGS', out);
  }

  g.swSync = { rpc, kvGet, kvSet, kvAppend, pvHit, pvStats, picksGet, picksSet,
    syncSettings, pushSettings, isOwner: () => !!ownerKey(),
    setOwnerKey: k => { try { k ? localStorage.setItem(OWNER_KEY, k) : localStorage.removeItem(OWNER_KEY); } catch (e) {} } };

  // Owner unlock via ?ownerkey=… (same convention as bt-sync.js; harmless if both run)
  try {
    const u = new URL(location.href), ok = u.searchParams.get('ownerkey');
    if (ok) { localStorage.setItem(OWNER_KEY, ok); u.searchParams.delete('ownerkey'); history.replaceState(null, '', u.pathname + u.search + u.hash); }
  } catch (e) {}

  // auto-init: count the visit; flush changed settings when leaving
  pvHit();
  addEventListener('pagehide', () => { pushSettings(); });
})(window);
