/* strategies-panel.js — the Saved-Strategies engine as a mountable module.
   window.mountStrategies(container) builds the UI + runs picks/basket-buy inside it.
   Needs (host loads first): @supabase, bt-names.js, bt-sync.js, bt-identity.js, backtest-engine.js. */
(function(){
  'use strict';
  if (window.mountStrategies) return;
  var mounted = false;
  var SP_CSS = "\n.spwrap{font-size:13px;line-height:1.5}\n.spwrap .sp-top{display:flex;align-items:baseline;justify-content:space-between;gap:10px;flex-wrap:wrap;margin-bottom:2px}\n.spwrap .sp-h{font-size:14px;font-weight:800;letter-spacing:-.01em;margin:0}\n.spwrap .sp-actions{display:flex;gap:7px;align-items:center;flex-wrap:wrap}\n.spwrap .sp-sub{font-size:11.5px;color:var(--text-3);line-height:1.45;margin:4px 0}\n.spwrap .sp-input{flex:1;padding:7px 9px;font-size:13px;border:1px solid var(--border);border-radius:8px;background:var(--surface-2);color:var(--text)}\n.spwrap .sym{color:var(--text-3);font-size:11px}\n.spwrap .badge{font-size:9px;font-weight:800;letter-spacing:.05em;padding:1px 5px;border-radius:4px;background:var(--surface-2);color:var(--text-3)}\n.spwrap .zpill{font-size:11.5px;font-weight:700;padding:4px 10px;border-radius:20px;background:var(--surface-2);color:var(--text-2);border:1px solid var(--border)}\n.spwrap .zpill.ok{color:var(--up)} .spwrap .zpill.warn{color:#c98500}\n.spwrap .sblk{border-top:1px solid var(--border);padding:12px 4px 6px}\n.spwrap .sblk:first-child{border-top:0;padding-top:4px}\n.spwrap .shead{display:flex;flex-wrap:wrap;align-items:baseline;gap:6px 12px;margin-bottom:6px}\n.spwrap .shead .nm2{font-size:13.5px;font-weight:800}\n.spwrap .tag{font-size:9.5px;font-weight:800;letter-spacing:.06em;padding:2px 6px;border-radius:4px;white-space:nowrap}\n.spwrap .tag.keep{background:color-mix(in srgb,var(--up) 16%,transparent);color:var(--up)}\n.spwrap .tag.new{background:color-mix(in srgb,var(--buy) 16%,transparent);color:var(--buy)}\n.spwrap .twrap{overflow-x:auto;-webkit-overflow-scrolling:touch}\n.spwrap table{width:100%;border-collapse:collapse;font-size:12.5px}\n.spwrap th{text-align:right;font-weight:700;color:var(--text-3);font-size:10.5px;text-transform:uppercase;letter-spacing:.05em;padding:7px 9px;border-bottom:1px solid var(--border);white-space:nowrap;background:var(--surface)}\n.spwrap th:first-child,.spwrap td:first-child{text-align:left}\n.spwrap td{padding:7px 9px;border-bottom:1px solid var(--border);text-align:right;white-space:nowrap}\n.spwrap td:nth-child(2),.spwrap th:nth-child(2){text-align:left}\n.spwrap .empty{text-align:center;color:var(--text-3);font-size:12.5px;padding:30px 10px}\n.spwrap .bal{border:1px solid var(--border);border-radius:12px;padding:12px 13px;margin:10px 0 4px;background:var(--surface-2)}\n.spwrap .bal-h{display:flex;align-items:baseline;gap:9px;flex-wrap:wrap;margin-bottom:8px}\n.spwrap .bal-h b{font-size:13.5px}\n.spwrap .bal-h .sub{font-size:11.5px;color:var(--text-3)}\n.spwrap .bal-h .go{margin-left:auto}\n.spwrap .snum{display:inline-block;font-size:9.5px;font-weight:800;background:var(--surface);border:1px solid var(--border);border-radius:4px;padding:0 4px;margin-left:3px;color:var(--text-3)}\n.spwrap .khelp{font-size:11.5px;color:var(--text-3);line-height:1.5;margin-top:10px}\n.spwrap .up{color:var(--up)} .spwrap .down{color:var(--down)}\n.spwrap .btn,#zbDlg .btn{border:1px solid var(--border);background:var(--surface);color:var(--text-2);border-radius:9px;padding:6px 11px;font-size:12.5px;font-weight:600;cursor:pointer;white-space:nowrap;display:inline-block}\n.spwrap .btn:hover,#zbDlg .btn:hover{background:var(--surface-2);color:var(--text)}\n.spwrap .btn.on,#zbDlg .btn.on{background:var(--buy);border-color:var(--buy);color:#fff}\n#ktoast{position:fixed;left:50%;bottom:24px;transform:translateX(-50%);z-index:300;background:var(--text);color:var(--bg);padding:8px 14px;border-radius:9px;font-size:12.5px;font-weight:600;opacity:0;pointer-events:none;transition:opacity .25s;max-width:90vw;text-align:center}\n#ktoast.show{opacity:1}\n.zchip{display:inline-block;padding:2px 7px;border-radius:20px;font-size:10.5px;font-weight:700;background:var(--surface-2);border:1px solid var(--border)}\n.zchip.ok{color:var(--up);border-color:var(--up)} .zchip.bad{color:var(--down);border-color:var(--down)} .zchip.open{color:var(--buy);border-color:var(--buy)}\n.zmsg{font-size:11px;color:var(--down);white-space:normal;text-align:left;max-width:280px}\n#zbWrap{position:fixed;inset:0;z-index:200;background:rgba(0,0,0,.5);display:none;align-items:center;justify-content:center;padding:16px}\n#zbWrap.open{display:flex}\n#zbDlg{width:min(600px,100%);max-height:92vh;overflow:auto;padding:16px;background:var(--surface);border:1px solid var(--border);border-radius:14px;box-shadow:var(--shadow)}\n#zbDlg h3{margin:0 0 2px;font-size:14px}\n#zbDlg .sub{font-size:11.5px;color:var(--text-3)}\n#zbDlg .krow{display:flex;gap:8px;margin-top:10px}\n#zbDlg label{flex:1;font-size:11px;font-weight:700;color:var(--text-3);text-transform:uppercase;letter-spacing:.05em}\n#zbDlg input,#zbDlg select{width:100%;margin-top:4px;padding:7px 9px;font-size:13.5px;font-weight:600;border:1px solid var(--border);border-radius:8px;background:var(--surface-2);color:var(--text)}\n#zbDlg table{margin-top:10px;width:100%;border-collapse:collapse;font-size:12.5px}\n#zbDlg th{font-size:10.5px;color:var(--text-3);text-transform:uppercase;padding:7px 9px;text-align:right;border-bottom:1px solid var(--border);white-space:nowrap}\n#zbDlg th:first-child,#zbDlg td:first-child{text-align:left}\n#zbDlg td{padding:6px 9px;border-bottom:1px solid var(--border);text-align:right}\n#zbDlg td input.zbq{width:80px;margin:0;padding:5px 7px;text-align:right}\n#zbDlg td input.zbl{width:92px;margin:0;padding:5px 7px;text-align:right}\n#zbTbl:not(.lim) .limcol{display:none}\n";
  var SP_DOM = "<div class=\"spwrap\"><div class=\"sp-top\"><h2 class=\"sp-h\">Saved strategies \u2014 what each would buy today</h2><div class=\"sp-actions\"><span id=\"zStatus\" class=\"zpill\">Zerodha: checking\u2026</span><button class=\"btn\" id=\"btnZLogin\" style=\"display:none\">Login to Zerodha \u25b8</button><button class=\"btn\" id=\"btnZSetup\" title=\"Kite worker URL\">\u2699</button><button class=\"btn\" id=\"spFavToggle\" title=\"Show only your \u2b50 favourite strategies\"></button><button class=\"btn\" id=\"spMode\" title=\"Rebalance picks rank on the LAST CLOSE \u2014 the official screen the backtest uses. Live picks re-rank the price-based factors (52w high/low distance, returns, momentum) at the CURRENT market price \u2014 fundamentals and holdings stay as filed. Near the close of a rebalance day the two converge.\"></button><button class=\"btn\" id=\"spSide\"></button><button class=\"btn on\" id=\"btnLoadAll\">\ud83c\udfaf Load all picks</button></div></div><div class=\"sp-sub\">Every strategy saved on the dashboard, one block each. Picks are the screen\u2019s top names as of the latest close; prices go live during market hours. \u26a1 buys the whole basket on Zerodha \u2014 you always confirm first.</div><div class=\"sp-sub\" id=\"status\"></div><div id=\"zSetupBox\" style=\"display:none\"><div class=\"khelp\">One-time per browser: your Kite worker URL (the portfolio/terminal shares it automatically).</div><div style=\"display:flex;gap:7px;max-width:520px;margin-top:6px\"><input id=\"zWUrl\" placeholder=\"https://\u2026workers.dev\" class=\"sp-input\"><button class=\"btn on\" id=\"zWSave\">Save</button></div></div><div id=\"buyall\"></div><div id=\"cards\"><div class=\"empty\">Loading saved strategies\u2026</div></div></div>";
  function injectCSS(){ if (document.getElementById('sp-css')) return; var s=document.createElement('style'); s.id='sp-css'; s.textContent=SP_CSS; document.head.appendChild(s); }
  window.mountStrategies = function(container, opts){
    if (mounted) return; mounted = true;
    injectCSS();
    container.innerHTML = SP_DOM;
    if (!document.getElementById('ktoast')){ var _kt=document.createElement('div'); _kt.id='ktoast'; document.body.appendChild(_kt); }
const $ = id => document.getElementById(id);
const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const zinr = v => (v == null || !isFinite(v)) ? '—' : '₹' + Math.round(v).toLocaleString('en-IN');
function ktoast(msg, ms){ const t = $('ktoast'); t.textContent = msg; t.classList.add('show');
  clearTimeout(ktoast._t); ktoast._t = setTimeout(() => t.classList.remove('show'), ms || 2800); }
function marketOpen(){ const d = new Date(Date.now() + 330*60000); const dow = d.getUTCDay();
  if (dow === 0 || dow === 6) return false; const m = d.getUTCHours()*60 + d.getUTCMinutes(); return m >= 555 && m <= 930; }



/* ---------- saved strategies: shared list + this browser's private ones, ONE card per unique identity ---------- */
function loadLS(k){ try { return JSON.parse(localStorage.getItem(k) || '[]'); } catch(e){ return []; } }
function privStrategies(){ return loadLS('bt_private_strategies').map(s => Object.assign({}, s, { _priv: true })); }
function strategies(){
  const priv = privStrategies();
  if (!priv.length) return loadLS('bt_strategies');
  const pk = new Set(priv.map(s => identityKey(s.cfg)));
  return loadLS('bt_strategies').filter(s => !pk.has(identityKey(s.cfg))).concat(priv);
}
function uniqStrategies(){
  const seen = new Map();
  for (const s of strategies()){
    const k = identityKey(s.cfg), cur = seen.get(k);
    if (!cur || (s.ts || 0) > (cur.ts || 0)) seen.set(k, s);
  }
  return [...seen.values()];
}

/* ---------- ⭐ favourites (same store the whole site uses; synced via SETTINGS elsewhere) ---------- */
function loadFavs(){ try { return new Set(JSON.parse(localStorage.getItem('bt_fav_strategies') || '[]')); } catch(e){ return new Set(); } }
function isFavCfg(favs, c){ return favs.has(identityKey(c)) || (typeof ruleKey === 'function' && favs.has(ruleKey(c))); }
let FAVONLY = (function(){ try { return localStorage.getItem('sp_fav_only') !== '0'; } catch(e){ return true; } })();
/* terminal.html loads no theme.js/sw-sync, so pull the synced favourites once here — remote-newer-wins
   with the SAME swset_ts stamp sw-sync uses, so the two never fight. Fail-silent offline. */
async function refreshFavsFromSettings(){
  try {
    const r = await fetch('https://nebjnsndgrhumnkuipqy.supabase.co/rest/v1/rpc/sw_kv_get', { method:'POST',
      headers: { apikey:'sb_publishable_MDlQwiVc5deii91__UNeDg_z9r4Fk98', Authorization:'Bearer sb_publishable_MDlQwiVc5deii91__UNeDg_z9r4Fk98', 'Content-Type':'application/json' },
      body: JSON.stringify({ k:'SETTINGS' }) });
    const doc = await r.json();
    const e = (Array.isArray(doc) ? doc : []).find(x => x && x.k === 'bt_fav_strategies');
    if (!e) return;
    const stamp = +(localStorage.getItem('swset_ts_bt_fav_strategies') || 0);
    if (e.ts > stamp){ e.v == null ? localStorage.removeItem('bt_fav_strategies') : localStorage.setItem('bt_fav_strategies', e.v);
      localStorage.setItem('swset_ts_bt_fav_strategies', String(e.ts)); renderCards(); }
  } catch(e){}
}

/* ---------- lazy engine ---------- */
let ENGINE_READY = false, ENGINE_LOADING = null;
async function ensureEngine(){
  if (ENGINE_READY) return true;
  if (!ENGINE_LOADING) ENGINE_LOADING = loadEngineData(m => { $('status').textContent = m || ''; })
    .then(() => { ENGINE_READY = true; $('status').textContent = `Data ${SF.start} → ${SF.end}.`; })
    .catch(() => { ENGINE_LOADING = null; });
  await ENGINE_LOADING;
  return ENGINE_READY;
}

/* ---------- live quotes (site-wide worker + key) ---------- */
let LIVE = null, LIVE_TIMER = null;
const liveQ = sym => (LIVE && LIVE.data && LIVE.data[sym]) || null;
async function fetchLive(){
  const wurl = (function(){ try { return localStorage.getItem('live_worker_url') || ''; } catch(e){ return ''; } })();
  const syms = [...new Set(Object.values(PICKS).flatMap(p => p.rows.map(r => r.sym))
    .concat(Object.values(FEED.byKey || {}).flatMap(f => f.rows.map(h => h.sym))))];
  if (!wurl || !syms.length) return;
  try {
    const sep = wurl.includes('?') ? '&' : '?';
    const res = await fetch(wurl + sep + 'symbols=' + encodeURIComponent(syms.join(',')));
    const d = await res.json();
    if (d && d.data){ LIVE = { ts: d.asOf || Date.now(), data: d.data }; renderCards();
      if ($('zbWrap') && $('zbWrap').classList.contains('open')) zbLiveTick(); }
  } catch(e){}
}
function startLiveLoop(){
  if (LIVE_TIMER) return;
  LIVE_TIMER = setInterval(() => { if (!document.hidden && marketOpen() && Object.keys(PICKS).length){ fetchLive(); if (PICKMODE === 'live') liveRerankAll(); } }, 60000);
}

/* ---------- picks ---------- */
const PICKS = {};
/* ---- borderline detection (user 2026-09-01: which slot to buy LAST on rebalance day) ----
   A pick is "borderline" when intraday price moves could still flip it by the close: the
   rank-N vs rank-N+1 gap on a PRICE-SENSITIVE sort factor is small, or a pick sits within a
   whisker of a price-sensitive filter cut (e.g. d52<=10 with d52 at 9.4). Fundamental factors
   (diiPct, profit growth) cannot move intraday and are deliberately excluded. Bands are
   heuristic surfacing thresholds — the tooltip shows the measured numbers. */
const PS_BAND = { d52: 1.2, d52_low_pct: 4, ret1m: 1.5, ret3m: 2, ret6m: 2.5, ret12m: 3, changePercent: 0.7, rsi: 2 };
function borderMap(cfg, all){
  const N = cfg.topN, out = {};
  const last = all[N - 1], next = all[N];
  if (last && next && (cfg.sortBy in PS_BAND)){
    const a = fieldVal(last, cfg.sortBy), b = fieldVal(next, cfg.sortBy);
    if (a != null && b != null && Math.abs(a - b) <= PS_BAND[cfg.sortBy])
      out[N] = 'rank #' + N + ' vs #' + (N + 1) + ': ' + cfg.sortBy + ' gap ' + Math.abs(a - b).toFixed(1) + ' \u2014 price moves can flip this slot by the close';
  }
  (cfg.filters || []).forEach(f => { if (!(f.field in PS_BAND)) return;
    for (let i = 0; i < N && i < all.length; i++){ const v = fieldVal(all[i], f.field);
      if (v != null && Math.abs(v - f.val) <= PS_BAND[f.field])
        out[i + 1] = (out[i + 1] ? out[i + 1] + ' \u00b7 ' : '') + f.field + ' ' + (+v).toFixed(1) + ' sits near the ' + f.op + ' ' + f.val + ' cut'; } });
  return out;
}
function screenOne(it){
  const all = screenAsOf(it.cfg, SF.end), picks = all.slice(0, it.cfg.topN), bd = borderMap(it.cfg, all);
  PICKS[it.id] = { asOf: SF.end, rows: picks.map((r, i) => ({ rank: i+1, sym: r.sym, tkr: r.tkr, bd: bd[i+1] || null,
    px: (META[r.tkr] && META[r.tkr].raw) ? META[r.tkr].raw : r.price })) };
}
/* ---------- LIVE re-ranking — the ENGINE'S OWN overlay (unified 2026-08-31) ----------
   The first version approximated live factors with ratio maths and its own candidate quotes —
   and disagreed with all-picks on borderline names (OFSS/JINDALSAW), because all-picks uses the
   engine's applyLiveOverlay: whole-universe quotes WITH the retry rounds Yahoo needs (~15% of a
   first pass drops silently), spliced into SERIES as a real bar so every factor recomputes
   exactly. One implementation, shared with all-picks, so the two pages cannot diverge again.
   Rebalance mode is untouched: screening at SF.end never sees the spliced live bar. */
  let PICKMODE = (function(){ try { return localStorage.getItem('sp_pick_mode') || 'reb'; } catch(e){ return 'reb'; } })();
  let LIVEOV = { ts: 0, date: null, n: 0 };
  async function ensureLiveOverlay(cfgs, force){
    if (!force && LIVEOV.ts && Date.now() - LIVEOV.ts < 55000) return;
    const r = await applyLiveOverlay(cfgs, m => { $('status').textContent = m || ''; });
    LIVEOV = { ts: Date.now(), date: (r && r.date) || SF.end, n: (r && r.n) || 0 };
  }
  function screenLiveOne(it){
    const all = screenAsOf(it.cfg, LIVEOV.date || SF.end), picks = all.slice(0, it.cfg.topN), bd = borderMap(it.cfg, all);
    PICKS[it.id] = { asOf: SF.end, live: true, liveTs: LIVEOV.ts, rows: picks.map((r, i) => ({ rank: i + 1, sym: r.sym, tkr: r.tkr, bd: bd[i+1] || null,
      px: r.price })) };   // the spliced bar IS the live price; rebalance mode still shows META.raw
  }
  async function screenPick(it){
    if (PICKMODE === 'live'){ await ensureLiveOverlay([it.cfg]); screenLiveOne(it); }
    else screenOne(it);
  }
  let LIVE_RERANK = false;
  async function liveRerankAll(){
    if (LIVE_RERANK) return; LIVE_RERANK = true;
    try {
      const its = strategies().filter(x => PICKS[x.id] && PICKS[x.id].live);
      if (its.length){ await ensureLiveOverlay(its.map(x => x.cfg), true);
        its.forEach(screenLiveOne); renderCards(); }
    } finally { LIVE_RERANK = false; }
  }
async function loadPicks(id){
  const it = strategies().find(x => x.id === id); if (!it) return;
  if (!await ensureEngine()){ ktoast('Could not load market data — try again'); return; }
  await screenPick(it); renderCards(); fetchLive(); startLiveLoop();
}
$('btnLoadAll').onclick = async () => {
  const favs = loadFavs();
  const all = uniqStrategies();
  const nFav = all.filter(it => isFavCfg(favs, it.cfg)).length;
  const list = (FAVONLY && nFav > 0) ? all.filter(it => isFavCfg(favs, it.cfg)) : all;
  if (!list.length) return;
  $('btnLoadAll').disabled = true;
  if (!await ensureEngine()){ $('btnLoadAll').disabled = false; ktoast('Could not load market data'); return; }
  if (PICKMODE === 'live'){ try { await ensureLiveOverlay(list.map(x => x.cfg), true); } catch(e){} }
  for (const it of list){ $('status').textContent = 'Screening ' + (it.name || '') + '…';
    await new Promise(r => setTimeout(r, 0));
    if (PICKMODE === 'live') screenLiveOne(it); else screenOne(it); }
  $('status').textContent = list.length + ' strategies screened as of ' + SF.end +
    (PICKMODE === 'live' ? ' — re-ranked LIVE (fundamentals as filed; ranking updates every minute while the market is open).' : '.');
  $('btnLoadAll').disabled = false;
  renderCards(); fetchLive(); startLiveLoop();
};

/* ---------- Zerodha plumbing (shares the portfolio page's localStorage on this origin) ---------- */
const Z = { connected: false, user: null, held: new Set(), hold: {} };
/* Which strategies' baskets were SENT to Zerodha today — so buying 8 in a row stays legible
   (user 2026-08-30: "show me label basket bought or i will be confused which of 8 are done").
   Marked the moment orders leave for Zerodha (direct API or the Kite popup); browser-local
   (same device that buys), auto-expires at midnight, and the chip un-marks on click if a
   basket was cancelled on Zerodha's page. */
const zbDayKey = () => { const d = new Date(); return d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0') + '-' + String(d.getDate()).padStart(2, '0'); };
function zbBoughtSet(){ try { const j = JSON.parse(localStorage.getItem('sp_bought_v1') || '{}'); return new Set(j[zbDayKey()] || []); } catch(e){ return new Set(); } }
function zbSetBought(id, on){ try { const k = zbDayKey(), a = zbBoughtSet(); on ? a.add(id) : a.delete(id);
    localStorage.setItem('sp_bought_v1', JSON.stringify({ [k]: [...a] })); } catch(e){} renderCards(); }
/* Zerodha's SEBI static-IP rule (Apr-2026) rejects API order placement from a non-whitelisted IP —
   our Cloudflare worker has no static IP, so direct /order can't work here; the Kite basket popup
   (you confirm on Zerodha's page) has no such requirement. Detect that reject and fall back. */
function ipBlocked(msg){ return /no ips|static ip|whitelist|allowed ip/i.test(msg || ''); }
const zWorker = () => { try { return (localStorage.getItem('pf_kite_worker') || '').replace(/\/+$/, ''); } catch(e){ return ''; } };
const zToken  = () => { try { return localStorage.getItem('pf_token') || ''; } catch(e){ return ''; } };
async function zFetch(path, opts){
  const o = Object.assign({}, opts || {});
  o.headers = Object.assign({ 'X-PF-Token': zToken() }, o.headers || {});
  try { const r = await fetch(zWorker() + path, o);
    let j = null; try { j = await r.json(); } catch(e){}
    return { st: r.status, j };
  } catch(e){ return { st: 0, j: null }; }
}
function zPill(txt, cls){ const b = $('zStatus'); b.textContent = txt; b.className = 'zpill' + (cls ? ' ' + cls : ''); }
async function zInit(){
  $('btnZLogin').style.display = 'none';
  if (!zWorker() || !zToken()){ zPill('Zerodha: not set up here', 'warn'); return; }
  const { st, j } = await zFetch('/status');
  if (st !== 200 || !j){ zPill('Zerodha: worker unreachable', 'warn'); return; }
  if (!j.connected){ Z.connected = false; zPill('Zerodha: not connected today', 'warn');
    $('btnZLogin').style.display = ''; return; }
  Z.connected = true; Z.user = j.user || '';
  zPill('🟢 ' + Z.user, 'ok');
  await zHoldRefresh();
  renderCards();
}
/* Kite /holdings, kept as PER-PRODUCT buckets: an MTF-funded holding has top-level quantity 0 —
   the real qty sits in the mtf object (user-caught 2026-08-27). Sells must name the bucket:
   product MTF closes the MTF position, CNC sells demat shares. Pledged (collateral) is NOT
   sellable without unpledging, so it stays out of both buckets on purpose. */
async function zHoldRefresh(){
  const h = await zFetch('/holdings');
  if (h.st === 200 && h.j && h.j.data){
    Z.hold = {};
    h.j.data.forEach(r => { Z.hold[r.tradingsymbol] = { mtf: ((r.mtf || {}).quantity || 0), cnc: (r.quantity || 0) + (r.t1_quantity || 0) }; });
    Z.held = new Set(h.j.data.filter(r => ((r.quantity||0)+(r.t1_quantity||0)+(r.collateral_quantity||0)+((r.mtf||{}).quantity||0)) > 0)
                             .map(r => r.tradingsymbol));
  }
}
$('btnZLogin').onclick = () => {
  const k = (function(){ try { return localStorage.getItem('pf_kite_key') || ''; } catch(e){ return ''; } })();
  if (!k){ ktoast('No API key saved in this browser — do the daily login from the portfolio page once', 5000); return; }
  location.href = 'https://kite.zerodha.com/connect/login?v=3&api_key=' + encodeURIComponent(k);
};
$('btnZSetup').onclick = () => { const b = $('zSetupBox'); b.style.display = b.style.display === 'none' ? '' : 'none'; $('zWUrl').value = zWorker(); };
$('zWSave').onclick = () => { const v = $('zWUrl').value.trim().replace(/\/+$/, '');
  if (!/^https:\/\/.+/.test(v)){ ktoast('That does not look like an https worker URL'); return; }
  try { localStorage.setItem('pf_kite_worker', v); } catch(e){}
  $('zSetupBox').style.display = 'none'; zInit(); };
zInit();

/* ---------- strategy blocks (rebalance-calendar anatomy) ---------- */
function cardMeta(cfg){
  const bits = ['Top ' + (cfg.topN || '?')];
  if (cfg.indexName) bits.push(String(cfg.indexName).replace('__FNO__', 'F&O'));
  return bits.join(' · ');
}
function renderCards(){
  const favs = loadFavs();
  const favOrder = (function(){ try { return JSON.parse(localStorage.getItem('bt_fav_strategies') || '[]'); } catch(e){ return []; } })();
  const favNum = cfg => { let i = favOrder.indexOf(identityKey(cfg)); if (i < 0 && typeof ruleKey === 'function') i = favOrder.indexOf(ruleKey(cfg)); return i + 1; };
  const all = uniqStrategies();
  // favourites float to the top; the toggle narrows to just them (default on when stars exist)
  const nFav = all.filter(it => isFavCfg(favs, it.cfg)).length;
  const tg = $('spFavToggle');
  if (tg){ tg.style.display = nFav ? '' : 'none';
    tg.textContent = FAVONLY ? ('\u2b50 Favourites (' + nFav + ')') : ('All (' + all.length + ')');
    tg.classList.toggle('on', FAVONLY && !!nFav); }
  const useFav = FAVONLY && nFav > 0;
  const list = (useFav ? all.filter(it => isFavCfg(favs, it.cfg)) : all)
    .slice().sort((a, b) => (isFavCfg(favs, b.cfg) ? 1 : 0) - (isFavCfg(favs, a.cfg) ? 1 : 0));
  if (!list.length){ $('cards').innerHTML = '<div class="empty">No saved strategies found.</div>'; return; }
  const bought = zbBoughtSet();
  const h = list.map(it => {
    const en = (typeof strategyEnglish === 'function') ? strategyEnglish(it.cfg) : '';
    const disp = en || nameWithBasis(it.name, it.cfg);
    if (SIDE === 'sell') return sellCardHTML(it, disp, favNum);
    const p = PICKS[it.id];
    let body = '';
    if (p){
      body = '<div class="twrap"><table><thead><tr><th>#</th><th>Pick</th><th>Live ₹</th><th>Day %</th></tr></thead><tbody>' +
        p.rows.map(r => {
          const q = liveQ(r.sym); const px = q && q.ltp != null ? q.ltp : r.px;
          const chg = q && q.ltp != null && q.prevClose ? (q.ltp / q.prevClose - 1) * 100 : null;
          return '<tr><td class="sym">' + r.rank + '</td>' +
            '<td><b>' + esc(r.sym) + '</b> ' + (Z.held.has(r.sym) ? '<span class="tag keep">held</span>' : '<span class="tag new">new</span>') +
            (r.bd ? ' <span class="tag" style="background:color-mix(in srgb,#c98500 18%,transparent);color:#c98500" title="' + esc(r.bd) + '">borderline</span>' : '') +
            (q && q.ltp != null ? '' : ' <span class="badge">EOD</span>') + '</td>' +
            '<td>₹' + (+px).toFixed(2) + '</td>' +
            '<td class="' + (chg == null ? 'sym' : chg >= 0 ? 'up' : 'down') + '">' + (chg == null ? '—' : (chg >= 0 ? '+' : '') + chg.toFixed(2) + '%') + '</td></tr>';
        }).join('') + '</tbody></table></div>' +
        (p.rows.some(r => r.bd) ? '<div class="khelp">\u26a0 borderline = the screen can still flip this pick by the close (hover it for the numbers) \u2014 on a rebalance day buy that slot LAST (~3:25 IST), the safe ones early.</div>' : '');
    }
    return '<div class="sblk"><div class="shead">' + (favNum(it.cfg) ? '<span class="snum" style="font-size:11px;background:var(--buy);color:#fff;border-color:var(--buy);padding:1px 6px;margin:0 4px 0 0">#' + favNum(it.cfg) + '</span>' : '') + '<span class="nm2" title="Code-name: ' + esc(nameWithBasis(it.name, it.cfg)) + '">' + (isFavCfg(loadFavs(), it.cfg) ? '\u2b50 ' : '') + esc(disp) + '</span>' +
      (it._priv ? '<span class="tag new">private</span>' : '') +
      (bought.has(it.id) ? '<span class="tag keep" data-unbought="' + esc(it.id) + '" title="Basket sent to Zerodha today — click if that was cancelled" style="cursor:pointer">✓ bought today</span>' : '') +
      '<span class="sym">' + esc(cardMeta(it.cfg)) + (p ? (p.live ? ' · LIVE picks · close ' + esc(p.asOf) + ' + prices ' + (p.liveTs ? new Date(p.liveTs).toTimeString().slice(0, 5) : 'now') : ' · picks as of ' + esc(p.asOf)) : '') + '</span>' +
      (p && p.live ? '<span class="tag new">LIVE</span>' : '') +
      '<span style="margin-left:auto; display:flex; gap:6px">' +
      '<button class="btn" data-load="' + esc(it.id) + '">🎯 ' + (p ? 'Refresh' : 'Picks') + '</button>' +
      (p ? (BUYSLICER[it.id]
              ? '<button class="btn on" data-basket="' + esc(it.id) + '">⚡ ' + BUYSLICER[it.id].i + '/' + BUYSLICER[it.id].n + '</button>'
              : (bought.has(it.id)
                   ? '<button class="btn" disabled title="Already bought today — click the ✓ bought-today chip to re-enable" style="opacity:.5;cursor:not-allowed;color:var(--up)">✓ Bought</button>'
                   : '<button class="btn on" data-basket="' + esc(it.id) + '">⚡ Buy basket</button>')) : '') +
      '</span></div>' + body + '</div>';
  }).join('');
  $('cards').innerHTML = h;
  renderBuyAll(list);
  for (const id in BUYSLICER){
    const el = (id === '__all__') ? $('balGo') : (id === '__residual__') ? $('residGo')
      : (document.querySelector('[data-basket="' + id + '"]') || document.querySelector('[data-sellbasket="' + id + '"]'));
    BUYSLICER[id].btn = el || null; if (el) el.textContent = (BUYSLICER[id].sell ? '\ud83d\udd3b ' : '⚡ ') + BUYSLICER[id].i + '/' + BUYSLICER[id].n; }
}
/* ---------- THIS REBALANCE: every stock to buy, in one place (user 2026-08-31) ----------
   Eight strategies × three picks means eight blocks to read, and the same stock often appears in
   several of them — the actual shopping list is neither obvious nor deduplicated. This merges
   every loaded strategy's picks into ONE table: a row per stock, which strategies want it
   (numbered as they appear below), the money they contribute together, and the quantity that
   buys at the live price. Money per strategy = the ₹ amount last used in its ⚡ dialog
   (remembered per strategy); strategies with no amount yet are named so you can set one.
   "Buy all" runs the same sliced, NSE-only, tick-aware engine as a single basket. */
function buyAllAgg(list){
  const agg = {}, missing = [];
  list.forEach((it, i) => {
    const p = PICKS[it.id]; if (!p || !p.rows.length) return;
    const amt = zbaGet(it.id);
    if (!amt) missing.push(i + 1);
    /* engine sizing (2026-09-01): a HOLD strategy re-buys nothing it already holds — only the
       new entries get money. The strategy's amount is spread over those entries alone. */
    const held = heldFor(it.cfg);
    const holdKeep = held && held.rows.length && held.method !== 'reset';
    const heldSet = holdKeep ? new Set(held.rows.map(x => x.sym)) : null;
    const rows2 = holdKeep ? p.rows.filter(r => !heldSet.has(r.sym)) : p.rows;
    if (!rows2.length) return;
    const per = amt ? amt / rows2.length : 0;
    rows2.forEach(r => {
      const q = liveQ(r.sym), px = (q && q.ltp != null) ? q.ltp : r.px;
      const cap = zbCap(r.sym), contrib = cap > 0 ? Math.min(per, cap) : per;   // per-basket ₹ cap (HFCL)
      const a = agg[r.sym] = agg[r.sym] || { sym: r.sym, px: px, amt: 0, from: [], capped: false };
      a.px = px; a.amt += contrib; if (cap > 0 && contrib < per) a.capped = true; a.from.push(i + 1);
    });
  });
  const rows = Object.values(agg).map(a => Object.assign(a, { qty: (a.amt > 0 && a.px > 0) ? Math.floor(a.amt / a.px) : 0 }))
    .sort((x, y) => (y.amt - x.amt) || (x.sym < y.sym ? -1 : 1));
  return { rows: rows, missing: missing };
}
function renderBuyAll(list){
  const box = $('buyall'); if (!box) return;
  if (SIDE === 'sell'){ box.innerHTML = '<div class="khelp" style="margin:6px 4px 10px">Adopted timing (backtested \u2248+10pp/yr vs buying next morning): <b>sell the exits near the close of T\u22121</b> \u2014 the session BEFORE month-end, on that day\u2019s near-final \u26a1 live picks \u00b7 <b>buy the entries near the month-end close</b>, sized from the sell proceeds.</div>'; return; }
  const residHTML = renderResidual();
  const withPicks = list.filter(it => PICKS[it.id] && PICKS[it.id].rows.length);
  if (!withPicks.length){ box.innerHTML = residHTML; wireResidGo(); return; }
  const agg = buyAllAgg(list), rows = agg.rows;
  const totAmt = rows.reduce((s, r) => s + r.amt, 0);
  const anyQty = rows.some(r => r.qty > 0);
  const B = BUYSLICER['__all__'];
  box.innerHTML = residHTML + '<div class="bal"><div class="bal-h"><b>This rebalance · ' + rows.length + ' stocks to buy</b>' +
    '<span class="sub">from ' + withPicks.length + ' ' + (withPicks.length === 1 ? 'strategy' : 'strategies') +
      (totAmt ? ' · ' + zinr(totAmt) : '') +
      (agg.missing.length ? ' · no amount set for ' + agg.missing.map(n => '#' + n).join(', ') : '') + '</span>' +
    '<span class="go"></span></div>' +   // Buy-all button REMOVED (user 2026-09-01): one round-robin queue for ~₹35 Cr ≈ 141 slices × 150s ≈ 6h — buy per-strategy (8 parallel ⚡ slicers) instead; the table stays as the consolidated checklist
    '<div class="twrap"><table><thead><tr><th>Stock</th><th>Strategies</th><th>Live ₹</th><th>Qty</th><th>Amount</th></tr></thead><tbody>' +
    rows.map(r => '<tr><td><b>' + esc(r.sym) + '</b> ' + (Z.held.has(r.sym) ? '<span class="tag keep">held</span>' : '<span class="tag new">new</span>') + '</td>' +
      '<td class="sym">' + r.from.map(n => '<span class="snum">#' + n + '</span>').join('') + (r.from.length > 1 ? ' <b>×' + r.from.length + '</b>' : '') + '</td>' +
      '<td>₹' + (+r.px).toFixed(2) + '</td>' +
      '<td>' + (r.qty ? r.qty.toLocaleString('en-IN') : '—') + '</td>' +
      '<td>' + (r.amt ? zinr(r.amt) : '—') + (r.capped ? ' <span class="sym" title="capped per basket">cap</span>' : '') + '</td></tr>').join('') +
    '</tbody></table></div>' +
    '<div class="khelp">Numbered by the strategy blocks below. A stock several strategies want is bought once, for the combined amount. Set a strategy’s ₹ amount once in its ⚡ dialog — it is remembered.</div></div>';
  const go = $('balGo');
  if (go) go.onclick = () => {
    if (BUYSLICER['__all__']){ const S = BUYSLICER['__all__'];
      buyStop('__all__', 'Buying stopped — ' + S.i + '/' + S.n + ' slices sent, rest kept'); return; }
    buyAllStart(rows);
  };
  wireResidGo();
}
function renderResidual(){
  let res = []; try { res = (zbaDoc().residual || []).filter(r => r && r.sym && +r.qty > 0); } catch(e){}
  if (!res.length) return '';
  const B = BUYSLICER['__residual__'];
  return '<div class="bal" style="border:1px solid var(--buy)"><div class="bal-h"><b>\u26a1 Buy the remaining \u2014 exact quantities</b>' +
    '<span class="sub">' + res.length + ' stocks \u00b7 HFCL as delivery (CNC), the rest MTF</span>' +
    '<span class="go"><button class="btn on" id="residGo">' + (B ? '\u26a1 ' + B.i + '/' + B.n : '\u26a1 Buy remaining') + '</button></span></div>' +
    '<div class="twrap"><table><thead><tr><th>Stock</th><th>Qty</th><th>Product</th></tr></thead><tbody>' +
    res.map(r => '<tr><td><b>' + esc(r.sym) + '</b></td><td>' + Math.floor(+r.qty).toLocaleString('en-IN') + '</td><td>' + esc(r.product || 'MTF') + '</td></tr>').join('') +
    '</tbody></table></div><div class="khelp">One tap buys exactly these \u2014 sliced, NSE, limit \u22640.5% above live. HFCL routes to CNC because MTF is blocked for it.</div></div>';
}
function wireResidGo(){ const g = $('residGo'); if (!g) return;
  g.onclick = () => { if (BUYSLICER['__residual__']){ const S = BUYSLICER['__residual__']; buyStop('__residual__', 'Stopped'); return; } buyResidual(); }; }
async function buyResidual(){
  let res = []; try { res = (zbaDoc().residual || []).filter(r => r && r.sym && +r.qty > 0); } catch(e){}
  if (!res.length){ ktoast('Nothing remaining set'); return; }
  const g = $('residGo');
  if (g && g.dataset.arm !== '1'){ g.dataset.arm = '1'; g.textContent = 'Confirm buy ' + res.length + ' stocks ?';
    clearTimeout(buyResidual._t); buyResidual._t = setTimeout(() => { g.dataset.arm = ''; g.textContent = '\u26a1 Buy remaining'; }, 8000); return; }
  if (g) g.dataset.arm = '';
  if (!Z.connected){ ktoast('Zerodha not connected'); return; }
  await loadTicks();
  const orders = res.map(r => { const q = liveQ(r.sym); return { variety:'regular', validity:'DAY', tag:'swresid',
    tradingsymbol: r.sym, exchange:'NSE', transaction_type:'BUY', order_type:'MARKET', quantity: Math.floor(+r.qty),
    product: (r.product || 'MTF'), _px: (q && q.ltp != null ? q.ltp : (+r.px || 0)) }; });
  const slices = buySlices(orders);
  if (BUYSLICER['__residual__']) buyStop('__residual__');
  BUYSLICER['__residual__'] = { slices: slices, i: 0, n: slices.length, btn: null, t: 0 };
  ktoast('Buying ' + res.length + ' remaining in ' + slices.length + ' slices (HFCL as CNC)', 6500);
  buyFire('__residual__');
  renderCards();
}
async function buyAllStart(rows){
  const live = rows.filter(r => r.qty > 0);
  if (!live.length){ ktoast('No quantities yet — set each strategy’s ₹ amount in its ⚡ dialog first', 5200); return; }
  const go = $('balGo');
  const est = live.reduce((s, r) => s + r.qty * r.px, 0);
  if (go && go.dataset.arm !== '1'){ go.dataset.arm = '1';
    go.textContent = 'Confirm BUY ' + live.length + ' stocks ≈ ' + zinr(est) + ' ?';
    clearTimeout(buyAllStart._t); buyAllStart._t = setTimeout(() => { go.dataset.arm = ''; go.textContent = '⚡ Buy all ' + rows.length; }, 8000);
    return; }
  if (go) go.dataset.arm = '';
  await loadTicks();
  const orders = live.map(r => ({ variety: 'regular', validity: 'DAY', tag: 'swbasket', tradingsymbol: r.sym,
    exchange: 'NSE', transaction_type: 'BUY', order_type: 'MARKET', quantity: r.qty, product: 'MTF', _px: r.px }));
  if (!Z.connected || Z.directBlocked){ ktoast('Not connected for paced slices — use each strategy’s ⚡ dialog to send via the Zerodha basket', 5600); return; }
  const slices = buySlices(orders);
  BUYSLICER['__all__'] = { slices: slices, i: 0, n: slices.length, btn: null, t: 0 };
  ktoast('Buying all ' + live.length + ' stocks in ' + slices.length + ' liquidity-sized slices (1% of each stock\u2019s 10-day traded value, \u20b95L\u2013\u20b91Cr) every ' + sliceGap() + 's, each a limit \u2264' + sliceRng() + '% above live \u2014 keep this tab open; tap the counter to stop', 7000);
  renderCards();
  buyFire('__all__');
}
$('cards').addEventListener('click', e => {
  const u = e.target.closest('[data-unbought]'); if (u){ zbSetBought(u.dataset.unbought, false); ktoast('Un-marked — it shows as not bought again'); return; }
  const l = e.target.closest('[data-load]'); if (l){ l.textContent = '⏳…'; loadPicks(l.dataset.load); return; }
  const us = e.target.closest('[data-unsold]'); if (us){ zbSetSold(us.dataset.unsold, false); ktoast('Un-marked — it shows as not sold again'); return; }
  const sb = e.target.closest('[data-sellbasket]'); if (sb){
    const sid = sb.dataset.sellbasket;
    if (BUYSLICER[sid]){ const S = BUYSLICER[sid];
      buyStop(sid, 'Selling stopped — ' + S.i + '/' + S.n + ' slices sent, rest kept'); return; }
    sellBasketStart(sid); return; }
  const b = e.target.closest('[data-basket]'); if (b){
    if (BUYSLICER[b.dataset.basket]){ const B = BUYSLICER[b.dataset.basket];
      buyStop(b.dataset.basket, 'Buying stopped — ' + B.i + '/' + B.n + ' slices sent, rest kept'); return; }
    zBasketOpen(b.dataset.basket); }
});

/* ---------- basket dialog (identical flow to the portfolio page) ---------- */
let ZB = { rows: [] };
function ensureZbDlg(){
  if ($('zbWrap')) return;
  const w = document.createElement('div'); w.id = 'zbWrap';
  w.innerHTML = '<div class="card" id="zbDlg">' +
    '<h3 id="zbTitle"></h3><div class="sub" style="padding:0" id="zbSub"></div>' +
    '<div class="krow"><label>₹ to deploy<input id="zbAmt" type="number" min="0" step="10000" placeholder="e.g. 500000"></label>' +
    '<label>Split ₹ as<select id="zbMode"><option value="value">Order value</option><option value="margin">Funds used (margin)</option></select></label></div>' +
    '<div class="krow"><label>Product<select id="zbProd"><option value="MTF">MTF (margin)</option><option value="CNC">Delivery (CNC)</option><option value="MIS">Intraday (MIS)</option></select></label>' +
    '<label>Order type<select id="zbType"><option value="MARKET">Market</option><option value="LIMIT">Limit (set price per row)</option></select></label></div>' +
    '<div class="twrap"><table id="zbTbl"></table></div>' +
    '<div class="krow" style="justify-content:flex-end">' +
    '<button class="btn" id="zbCancel">Cancel</button>' +
    '<button class="btn" id="zbKite" title="Review and confirm all of them on Zerodha’s own basket page">Kite basket</button>' +
    '<button class="btn on" id="zbGo" style="display:none">Place all ▸</button></div>' +
    '<div class="khelp">Market or limit orders, equal ₹ split across ticked rows. On <b>Limit</b>, set a price per row — the <b>Live ₹</b> column keeps updating so you can pick it. “Uses ₹” is Zerodha’s LIVE margin per order — on MTF the smaller figure your funds actually pay; “funds used” makes your amount mean exactly that. You confirm with a second click before anything is placed.</div></div>';
  document.body.appendChild(w);
  w.addEventListener('click', e => { if (e.target === w) w.classList.remove('open'); });
  $('zbCancel').onclick = () => $('zbWrap').classList.remove('open');
  $('zbAmt').addEventListener('input', () => { zbAlloc(); zbRender(); zbMarginSoon(true); });
  $('zbMode').addEventListener('change', () => { zbAlloc(); zbRender(); zbMarginSoon(true); });
  $('zbProd').addEventListener('change', () => { ZB.rows.forEach(r => { r.mps = 0; r.margin = null; }); zbAlloc(); zbRender(); zbMarginSoon(true); });
  $('zbType').addEventListener('change', () => { const lim = $('zbType').value === 'LIMIT';
    ZB.rows.forEach(r => { if (lim && !(r.limit > 0)) r.limit = r.px; }); zbRender(); zbArmReset(); });
  $('zbTbl').addEventListener('input', e => {
    const q = e.target.closest('input.zbq'); if (q){ const i = +q.dataset.i, r = ZB.rows[i]; r.qty = Math.max(0, Math.floor(+q.value || 0)); r.margin = null;
      zbRowPaint(i); zbFoot(); zbArmReset(); zbMarginSoon(false); return; }
    const l = e.target.closest('input.zbl'); if (l){ ZB.rows[+l.dataset.i].limit = Math.max(0, +l.value || 0); zbArmReset(); } });
  $('zbTbl').addEventListener('change', e => { const c = e.target.closest('input[type=checkbox]'); if (!c) return;
    ZB.rows[+c.dataset.i].on = c.checked; zbAlloc(); zbRender(); zbMarginSoon(true); });
  $('zbKite').onclick = () => { const o = zbOrders(); if (!o.length){ ktoast('Nothing to buy — set an amount first'); return; }
    if (kiteSend(o)){ zbSetBought(ZB.id, true); $('zbWrap').classList.remove('open'); } };
  $('zbGo').onclick = zbPlaceAll;
}
function zbOrders(){
  const type = ($('zbType') && $('zbType').value) || 'MARKET';
  return ZB.rows.filter(r => r.on && r.qty > 0).map(r => Object.assign({ variety:'regular', validity:'DAY', tag:'swbasket',
    tradingsymbol: r.sym, exchange:'NSE', transaction_type:'BUY', order_type: type,
    quantity: r.qty, product: $('zbProd').value }, type === 'LIMIT' ? { price: (r.limit > 0 ? r.limit : r.px) } : {}));
}
function zbAlloc(){
  const amt = +$('zbAmt').value || 0, mode = ($('zbMode') && $('zbMode').value) || 'value';
  const on = ZB.rows.filter(r => r.on && r.px > 0), per = on.length ? amt / on.length : 0;
  ZB.rows.forEach(r => { r.st = ''; r.msg = '';
    if (!(r.on && r.px > 0)){ r.qty = 0; r.margin = null; return; }
    const perShare = (mode === 'margin' && r.mps > 0) ? r.mps : r.px;
    const cap = zbCap(r.sym), val = (cap > 0 && mode !== 'margin') ? Math.min(per, cap) : per;   // HFCL ₹1 Cr cap
    r.qty = Math.floor(val / perShare); r.margin = null; });
  if (ZB.id && amt) zbaSet(ZB.id, amt);
}
function zbFoot(){
  const rows = ZB.rows.filter(r => r.on && r.qty > 0);
  const est = rows.reduce((s, r) => s + r.qty * (r.px || 0), 0);
  const haveM = rows.length && rows.every(r => r.margin != null);
  const mar = rows.reduce((s, r) => s + (r.margin || 0), 0), chg = rows.reduce((s, r) => s + (r.chg || 0), 0);
  const f = $('zbTot'); if (f) f.innerHTML = rows.length + ' orders · ≈ <b>' + zinr(est) + '</b>' +
    (haveM ? ' · blocks ≈ <b>' + zinr(mar) + '</b> of funds' + (chg ? ' + ~₹' + Math.round(chg).toLocaleString('en-IN') + ' charges' : '') :
     (ZB.merr ? ' · <span class="down">margin check: ' + esc(ZB.merr) + '</span>' : ''));
}
function zbMarginSoon(realloc){ clearTimeout(ZB.mt); ZB.mt = setTimeout(() => zbFetchMargins(realloc), 600); }
async function zbFetchMargins(realloc){
  if (!Z.connected || !$('zbWrap') || !$('zbWrap').classList.contains('open')) return;
  const orders = zbOrders();
  if (!orders.length){ ZB.rows.forEach(r => r.margin = null); zbFoot(); return; }
  const seq = ZB.mseq = (ZB.mseq || 0) + 1;
  const { st, j } = await zFetch('/margincalc', { method:'POST', headers:{ 'Content-Type':'application/json' },
    body: JSON.stringify(orders.map(o => ({ exchange:o.exchange, tradingsymbol:o.tradingsymbol, product:o.product, quantity:o.quantity }))) });
  if (seq !== ZB.mseq || !$('zbWrap').classList.contains('open')) return;
  if (st === 0 && !ZB.mretry){ ZB.mretry = 1; setTimeout(() => zbFetchMargins(realloc), 1500); return; }
  ZB.mretry = 0;
  if (st !== 200 || !j || !j.data){ ZB.merr = (j && j.message) || ('HTTP ' + st); zbFoot(); return; }
  ZB.merr = null;
  let di = 0;
  ZB.rows.forEach(r => { if (r.on && r.qty > 0){ const d = j.data[di++] || {};
      r.margin = (d.total != null ? +d.total : null); r.chg = (d.charges && +d.charges.total) || 0;
      if (r.margin != null) r.mps = r.margin / r.qty; }
    else r.margin = null; });
  if (realloc && $('zbMode').value === 'margin'){ zbAlloc(); zbRender(); zbMarginSoon(false); return; }
  zbRender();
}
function zbArmReset(){ const b = $('zbGo'); if (b && b.dataset.arm === '1'){ b.dataset.arm = ''; b.textContent = 'Place all ▸'; } }
function zbRowPaint(i){
  const tr = $('zbTbl').querySelectorAll('tbody tr')[i], r = ZB.rows[i]; if (!tr) return;
  tr.children[5].innerHTML = r.qty ? zinr(r.qty * r.px) : '—';
  tr.children[6].innerHTML = (Z.connected && r.qty) ? '…' : '—';
}
/* live-price tick while the basket dialog is open — so you can pick a limit price */
function zbLiveTick(){
  const trs = $('zbTbl') ? $('zbTbl').querySelectorAll('tbody tr') : [];
  ZB.rows.forEach((r, i) => { const q = liveQ(r.sym); if (q && q.ltp != null){ r.px = q.ltp; r.live = true; }
    const tr = trs[i]; if (!tr) return;
    tr.children[2].innerHTML = r.px ? '₹' + (+r.px).toFixed(2) : '—';
    tr.children[5].innerHTML = r.qty ? zinr(r.qty * r.px) : '—'; });
  zbFoot();
}
function zbRender(){
  const lim = ($('zbType') && $('zbType').value) === 'LIMIT';
  let h = '<thead><tr><th></th><th>Pick</th><th>Live ₹</th><th class="limcol">Limit ₹</th><th>Qty</th><th>≈ Cost</th><th title="What Zerodha will actually block — its live margin API">Uses ₹</th><th></th></tr></thead><tbody>';
  ZB.rows.forEach((r, i) => {
    const stChip = r.st === 'COMPLETE' ? '<span class="zchip ok">COMPLETE</span>'
      : r.st === 'REJECTED' ? '<span class="zchip bad" title="' + esc(r.msg) + '">REJECTED</span>'
      : r.st === 'fail' ? '<span class="zchip bad" title="' + esc(r.msg) + '">FAILED</span>'
      : r.st ? '<span class="zchip open">' + esc(r.st) + '</span>' : '';
    h += '<tr><td><input type="checkbox" data-i="' + i + '"' + (r.on ? ' checked' : '') + '></td>' +
      '<td><b>' + esc(r.sym) + '</b>' + (r.kept ? ' <span class="tag keep">kept \u2014 riding, not re-bought</span>' : (Z.held.has(r.sym) ? ' <span class="tag keep">held</span>' : '')) + (r.live ? '' : ' <span class="badge">EOD</span>') + '</td>' +
      '<td>' + (r.px ? '₹' + (+r.px).toFixed(2) : '—') + '</td>' +
      '<td class="limcol"><input class="zbl" type="number" min="0" step="0.05" data-i="' + i + '" value="' + ((r.limit > 0 ? r.limit : r.px) || 0).toFixed(2) + '"' + (r.on ? '' : ' disabled') + '></td>' +
      '<td><input class="zbq" type="number" min="0" step="1" data-i="' + i + '" value="' + r.qty + '"' + (r.on ? '' : ' disabled') + '></td>' +
      '<td>' + (r.qty ? zinr(r.qty * r.px) : '—') + '</td>' +
      '<td>' + (r.margin != null ? zinr(r.margin) : (Z.connected && r.qty ? '…' : '—')) + '</td>' +
      '<td>' + stChip + (r.st === 'REJECTED' || r.st === 'fail' ? '<div class="zmsg">' + esc(r.msg) + '</div>' : '') + '</td></tr>';
  });
  h += '</tbody><tfoot><tr><td colspan="8" style="text-align:right" id="zbTot"></td></tr></tfoot>';
  $('zbTbl').innerHTML = h; $('zbTbl').classList.toggle('lim', lim); zbFoot();
  $('zbGo').style.display = Z.connected ? '' : 'none';
}
function zBasketOpen(id){
  const it = strategies().find(x => x.id === id), p = PICKS[id];
  if (!it || !p || !p.rows.length){ ktoast('Load the picks first'); return; }
  ensureZbDlg();
  ZB = { id, rows: p.rows.map(r => { const q = liveQ(r.sym); const px = (q && q.ltp != null) ? q.ltp : r.px;
    return { sym: r.sym, px: px, limit: px, live: !!(q && q.ltp != null),
             on: true, qty: 0, st: '', msg: '', margin: null, mps: 0, chg: 0 }; }) };
  $('zbTitle').textContent = 'Buy the basket';
  $('zbSub').textContent = ((typeof strategyEnglish === 'function' && strategyEnglish(it.cfg)) || nameWithBasis(it.name, it.cfg)) + ' — ' + p.rows.length + ' picks as of ' + p.asOf + '. You confirm before anything is placed.';
  $('zbProd').value = 'MTF';
  if ($('zbType')) $('zbType').value = 'MARKET';
  $('zbAmt').value = zbaGet(id) || '';
  /* Month 2+ engine sizing (user 2026-09-01): kept winners are untouched — untick them; the
     amount prefills to the EXITS' current value scaled entries/openSlots, so the dialog's equal
     split across the ticked rows IS the engine's per-slot funding. Reset strategies instead
     prefill the whole book's value (sell all, fresh equal split). Editable as ever. */
  (function(){
    const held = heldFor(it.cfg); if (!held || !held.rows.length) return;
    const px = h => { const q = liveQ(h.sym); return (q && q.ltp != null) ? +q.ltp : (h.avg || 0); };
    if (held.method === 'reset'){
      const all = held.rows.reduce((s, h) => s + h.qty * px(h), 0);
      if (all > 0) $('zbAmt').value = Math.round(all);
      return;
    }
    const heldSet = new Set(held.rows.map(h => h.sym));
    let stay = 0; ZB.rows.forEach(r => { if (heldSet.has(r.sym)){ r.on = false; r.kept = true; stay++; } });
    if (!stay) return;
    const pickSet = new Set(p.rows.map(r => r.sym));
    const exitVal = held.rows.filter(h => !pickSet.has(h.sym)).reduce((s, h) => s + h.qty * px(h), 0);
    const openSlots = Math.max(1, (held.topN || p.rows.length) - stay);
    const entries = ZB.rows.filter(r => r.on).length;
    if (exitVal > 0 && entries) $('zbAmt').value = Math.round(exitVal * entries / openSlots);
    $('zbSub').textContent += ' · kept winners stay unticked; amount = the exits\u2019 value per open slot (engine sizing) — edit to your actual sell proceeds.';
  })();
  zbAlloc(); zbRender();
  $('zbWrap').classList.add('open');
  zbMarginSoon(true);
}
/* ---------- sliced basket buying (user 2026-08-31: "same mechanism i.e. slicing and nse only
   placement i want for my basket buying") ----------
   Mirrors the terminal's sell slicer: each stock's quantity splits into orders of at most
   ₹<slice>L, fired every <gap>s ROUND-ROBIN across the basket's stocks (so all names build
   evenly), each a LIMIT pegged ≤<rng>% ABOVE a fresh live price on the stock's real tick grid
   (tick_sizes.json + ticks learned from rejections). Knobs are the same localStorage trio the
   account view edits. Exchange is always NSE (zbOrders pins it). The strategy's ⚡ button shows
   n/N progress; tapping it mid-run stops the remaining slices. Every accepted slice's fate is
   read back from the order book, so exchange rejections surface here with their reason. */
/* ---- per-strategy ₹ amounts, synced ACROSS DEVICES (user 2026-09-01: "i should see same from
   my mobile as well as mac"). They used to live only in this browser (sw_zb_amt_<id>). Now the
   whole map lives in a TOKEN-GATED pf_feed row (<pf_token>.zbamts) — the same private channel as
   the portfolio holdings, deliberately NOT the public SETTINGS doc: position sizing must never
   sit in a world-readable store. Whole-map last-writer-wins by ts (amounts are edited by one
   person, occasionally). Boot pulls the row and applies it when newer; every edit pushes back
   (debounced). No token / offline / old browser → localStorage keeps working alone. */
const ZBA_LS = 'sw_zb_amts_v1';
const ZBA_URL = 'https://nebjnsndgrhumnkuipqy.supabase.co/rest/v1/rpc/';
const ZBA_ANON = 'sb_publishable_MDlQwiVc5deii91__UNeDg_z9r4Fk98';
const ZBA_SECRET = 'sw_owner_8Kq2Lm9Xp4Rt7v';        // same public write secret sw-sync.js ships
function zbaRow(){ try { const t = localStorage.getItem('pf_token') || ''; return t ? t + '.zbamts' : ''; } catch (e){ return ''; } }
function zbaDoc(){ try { const d = JSON.parse(localStorage.getItem(ZBA_LS) || 'null'); if (d && d.amts) return d; } catch (e){} return { ts: 0, amts: {}, caps: {}, sliceCaps: {} }; }
/* Per-stock ₹ cap applied to EACH basket that holds the stock (user 2026-09-01: HFCL lost its MTF
   leverage — 1.0x/100% margin — so cap it at ₹1 Cr/basket while every other pick stays at full
   ₹1.47 Cr, no per-strategy shrink). Lives in the same synced row (caps:{SYM:rupees}); clear it to
   remove. Order-value cap only (value mode); margin mode is left untouched. */
function zbCap(sym){ try { return +((zbaDoc().caps || {})[sym]) || 0; } catch (e){ return 0; } }
function zbaGet(id){ const v = +zbaDoc().amts[id] || 0; if (v) return v;
  try { return +(localStorage.getItem('sw_zb_amt_' + id) || 0); } catch (e){ return 0; } }      // pre-sync saves
function zbaSet(id, amt){ const d = zbaDoc(); d.amts[id] = amt; d.ts = Date.now();
  try { localStorage.setItem(ZBA_LS, JSON.stringify(d)); } catch (e){}
  try { localStorage.setItem('sw_zb_amt_' + id, String(amt)); } catch (e){}
  clearTimeout(zbaSet._t); zbaSet._t = setTimeout(zbaPush, 1500); }
async function zbaPush(){ const row = zbaRow(); if (!row || typeof CompressionStream === 'undefined') return;
  try {
    let doc = zbaDoc();
    try {   // merge with what the row holds so a one-key edit never clobbers the rest
      const r0 = await fetch(ZBA_URL + 'pf_feed_get?token=' + encodeURIComponent(row) + '&apikey=' + ZBA_ANON, { cache: 'no-store' });
      if (r0.ok){ const j0 = await r0.json();
        if (j0 && j0.z && typeof DecompressionStream !== 'undefined'){
          const by = Uint8Array.from(atob(j0.z), c => c.charCodeAt(0));
          const st0 = new Blob([by]).stream().pipeThrough(new DecompressionStream('gzip'));
          const rem0 = JSON.parse(await new Response(st0).text());
          if (rem0 && rem0.amts){ doc = zbaMerge(rem0, doc); try { localStorage.setItem(ZBA_LS, JSON.stringify(doc)); } catch (e){} }
        } }
    } catch (e){}
    const st = new Blob([JSON.stringify(doc)]).stream().pipeThrough(new CompressionStream('gzip'));
    const buf = new Uint8Array(await new Response(st).arrayBuffer());
    let b = ''; buf.forEach(x => b += String.fromCharCode(x));
    await fetch(ZBA_URL + 'pf_feed_set', { method: 'POST',
      headers: { apikey: ZBA_ANON, Authorization: 'Bearer ' + ZBA_ANON, 'Content-Type': 'application/json' },
      body: JSON.stringify({ secret: ZBA_SECRET, token: row, payload: { z: btoa(b) } }) });
  } catch (e){} }
async function zbaPull(){ const row = zbaRow(); if (!row || typeof DecompressionStream === 'undefined') return;
  try {
    const r = await fetch(ZBA_URL + 'pf_feed_get?token=' + encodeURIComponent(row) + '&apikey=' + ZBA_ANON, { cache: 'no-store' });
    if (!r.ok) return; const j = await r.json(); if (!j || !j.z) return;
    const bytes = Uint8Array.from(atob(j.z), c => c.charCodeAt(0));
    const st = new Blob([bytes]).stream().pipeThrough(new DecompressionStream('gzip'));
    const rem = JSON.parse(await new Response(st).text());
    if (rem && rem.amts) try { localStorage.setItem(ZBA_LS, JSON.stringify(zbaMerge(zbaDoc(), rem))); } catch (e){}
  } catch (e){} }
/* Merge two amount docs: the newer doc's values win where both name a strategy, but a strategy
   only ONE doc names always survives — so an edit made before this device ever pulled can never
   wipe the other seven seeded amounts (whole-map last-writer-wins did exactly that). */
function zbaMerge(a, b){ const newer = (b.ts || 0) >= (a.ts || 0) ? b : a, older = newer === b ? a : b;
  return { ts: Math.max(a.ts || 0, b.ts || 0), amts: Object.assign({}, older.amts, newer.amts),
           caps: Object.assign({}, older.caps || {}, newer.caps || {}),
           sliceCaps: Object.assign({}, older.sliceCaps || {}, newer.sliceCaps || {}),
           residual: (newer.residual !== undefined ? newer.residual : older.residual) }; }

/* ================= SELL BASKETS (user 2026-09-01) =================
   Month-end mirror of the buy side. The card shows EVERY stock the strategy holds (exact
   per-strategy quantities from the portfolio feed — a shared stock sells only THIS strategy's
   share); the button sells ONLY the exits (dropped from the new picks). Stocks staying next
   month render greyed and are never touched. A RESET strategy sells everything, always —
   even a stock picked again — because reset re-enters fresh equal thirds. */
let SIDE = 'buy';                     // 'buy' | 'sell' — the top-of-panel selector
const FEED = { ts: 0, byKey: null };  // pf_feed holdings row -> per-strategy held quantities
async function feedPull(force){
  const tok = zToken(); if (!tok || typeof DecompressionStream === 'undefined') return null;
  if (!force && FEED.byKey && Date.now() - FEED.ts < 120000) return FEED.byKey;
  try {
    const r = await fetch(ZBA_URL + 'pf_feed_get?token=' + encodeURIComponent(tok) + '&apikey=' + ZBA_ANON, { cache: 'no-store' });
    if (!r.ok) return FEED.byKey;
    const j = await r.json(); if (!j || !j.z) return FEED.byKey;
    const by = Uint8Array.from(atob(j.z), c => c.charCodeAt(0));
    const st = new Blob([by]).stream().pipeThrough(new DecompressionStream('gzip'));
    const doc = JSON.parse(await new Response(st).text());
    const map = {};
    (doc.portfolios || []).forEach(pf => {
      if (!pf.strategy || pf.archived) return;
      const rows = (doc.holdings || []).filter(h => h.pf === pf.id)
        .map(h => ({ sym: String(h.sym || '').replace(/\.(NS|BO)$/, ''), qty: Math.floor(+h.qty || 0), avg: +h.avg || 0 }))
        .filter(h => h.sym && h.qty > 0);
      map[identityKey(pf.strategy)] = { pfId: pf.id, method: (pf.strategy.method || 'hold'),
                                        topN: (pf.strategy.topN || 3), rows: rows };
    });
    FEED.ts = Date.now(); FEED.byKey = map;
    if (SIDE === 'sell') renderCards();
  } catch (e){}
  return FEED.byKey;
}
function heldFor(cfg){ try { return (FEED.byKey || {})[identityKey(cfg)] || null; } catch (e){ return null; } }
function zbSoldSet(){ try { const j = JSON.parse(localStorage.getItem('sp_sold_v1') || '{}'); return new Set(j[zbDayKey()] || []); } catch (e){ return new Set(); } }
function zbSetSold(id, on){ try { const k = zbDayKey(), a = zbSoldSet(); on ? a.add(id) : a.delete(id);
    localStorage.setItem('sp_sold_v1', JSON.stringify({ [k]: [...a] })); } catch (e){} renderCards(); }
function sellCardHTML(it, disp, favNum){
  const sold = zbSoldSet();
  const held = heldFor(it.cfg);
  const p = PICKS[it.id];
  const isReset = !!(held && held.method === 'reset');
  const pickSet = (p && p.rows.length) ? new Set(p.rows.map(r => r.sym)) : null;
  let body = '', btn = '';
  if (!held) body = '<div class="khelp">No holdings feed in this browser yet (needs the pf token) — or this strategy has no live book.</div>';
  else if (!held.rows.length) body = '<div class="khelp">Nothing held under this strategy.</div>';
  else {
    const rows = held.rows.map(h => {
      const q = liveQ(h.sym), px = (q && q.ltp != null) ? +q.ltp : null;
      return { h: h, px: px, stays: !isReset && !!(pickSet && pickSet.has(h.sym)), val: px != null ? h.qty * px : null };
    });
    const exits = rows.filter(r => !r.stays);
    const est = exits.reduce((s, r) => s + (r.val || 0), 0);
    body = '<div class="twrap"><table><thead><tr><th>Stock</th><th>Held</th><th>Live \u20b9</th><th>Value</th><th></th></tr></thead><tbody>' +
      rows.map(r => '<tr' + (r.stays ? ' style="opacity:.45"' : '') + '><td><b>' + esc(r.h.sym) + '</b></td>' +
        '<td>' + r.h.qty.toLocaleString('en-IN') + '</td>' +
        '<td>' + (r.px != null ? '\u20b9' + r.px.toFixed(2) : '\u2014') + '</td>' +
        '<td>' + (r.val != null ? zinr(r.val) : '\u2014') + '</td>' +
        '<td>' + (r.stays ? '<span class="tag keep">stays \u2014 not sold</span>'
                : (pickSet || isReset ? '<span class="tag" style="background:color-mix(in srgb,var(--down) 16%,transparent);color:var(--down)">' + (isReset ? 'reset \u2014 sell' : 'EXIT \u2014 sell') + '</span>'
                : '<span class="badge">load picks</span>')) + '</td></tr>').join('') +
      '</tbody></table></div>' +
      '<div class="khelp">' + (isReset
        ? 'Reset strategy: the whole basket sells every rebalance and re-enters fresh \u2014 even a stock picked again.'
        : (pickSet ? exits.length + ' exit' + (exits.length === 1 ? '' : 's') + ' to sell' + (est ? ' \u2248 ' + zinr(est) : '') + ' \u00b7 greyed rows stay for next month and are never sold.'
                   : 'Load the picks (\ud83c\udfaf) first \u2014 without them the exits are unknown, so nothing can be sold.')) + '</div>';
    const B = BUYSLICER[it.id];
    if (B && B.sell) btn = '<button class="btn on" data-sellbasket="' + esc(it.id) + '">\ud83d\udd3b ' + B.i + '/' + B.n + '</button>';
    else if (sold.has(it.id)) btn = '<button class="btn" disabled style="opacity:.5;cursor:not-allowed;color:var(--down)" title="Sold today \u2014 click the \u2713 sold-today chip to re-enable">\u2713 Sold</button>';
    else if ((pickSet || isReset) && exits.length) btn = '<button class="btn on" style="background:var(--down);border-color:var(--down)" data-sellbasket="' + esc(it.id) + '">\ud83d\udd3b Sell ' + (isReset ? 'all ' : '') + exits.length + '</button>';
  }
  return '<div class="sblk"><div class="shead">' + (favNum(it.cfg) ? '<span class="snum" style="font-size:11px;background:var(--down);color:#fff;border-color:var(--down);padding:1px 6px;margin:0 4px 0 0">#' + favNum(it.cfg) + '</span>' : '') +
    '<span class="nm2" title="Code-name: ' + esc(nameWithBasis(it.name, it.cfg)) + '">' + esc(disp) + '</span>' +
    (sold.has(it.id) ? '<span class="tag keep" data-unsold="' + esc(it.id) + '" title="Sell basket sent today \u2014 click if that was cancelled" style="cursor:pointer">\u2713 sold today</span>' : '') +
    '<span class="sym">' + (held ? esc(held.pfId + ' \u00b7 ' + held.method) : '') + (p ? ' \u00b7 picks as of ' + esc(p.asOf) : '') + '</span>' +
    '<span style="margin-left:auto;display:flex;gap:6px">' +
    '<button class="btn" data-load="' + esc(it.id) + '">\ud83c\udfaf ' + (p ? 'Refresh' : 'Picks') + '</button>' + btn + '</span></div>' + body + '</div>';
}
async function sellBasketStart(id){
  const it = strategies().find(x => x.id === id); if (!it) return;
  const held = heldFor(it.cfg);
  if (!held || !held.rows.length){ ktoast('No holdings on record for this strategy'); return; }
  const isReset = held.method === 'reset';
  const p = PICKS[id];
  if (!isReset && (!p || !p.rows.length)){ ktoast('Load the picks first \u2014 exits are unknown without them'); return; }
  const pickSet = new Set(((p && p.rows) || []).map(r => r.sym));
  const exits = held.rows.filter(h => isReset || !pickSet.has(h.sym));
  if (!exits.length){ ktoast('Nothing to sell \u2014 every holding stays next month'); return; }
  const btn = document.querySelector('[data-sellbasket="' + id + '"]');
  if (btn && btn.dataset.arm !== '1'){ btn.dataset.arm = '1';
    btn.textContent = 'Confirm SELL ' + exits.length + (isReset ? ' (reset: all)' : ' exit' + (exits.length === 1 ? '' : 's')) + ' ?';
    clearTimeout(sellBasketStart._t); sellBasketStart._t = setTimeout(() => { btn.dataset.arm = ''; renderCards(); }, 8000); return; }
  if (btn) btn.dataset.arm = '';
  if (!Z.connected){ ktoast('Zerodha not connected'); return; }
  await loadTicks();
  await zHoldRefresh();                 // fresh per-product buckets right before selling
  const orders = [], short = [];
  exits.forEach(h => {
    const q = liveQ(h.sym), px = (q && q.ltp != null) ? +q.ltp : (h.avg || 0);
    const bk = Z.hold[h.sym] || { mtf: 0, cnc: 0 };
    let mq = Math.min(h.qty, bk.mtf), cq = Math.min(h.qty - mq, bk.cnc);
    if (mq + cq < h.qty) short.push(h.sym + ' (' + (h.qty - mq - cq) + ')');
    const base = { variety: 'regular', validity: 'DAY', tag: 'swsell', tradingsymbol: h.sym,
                   exchange: 'NSE', transaction_type: 'SELL', order_type: 'MARKET', _px: px };
    if (mq > 0) orders.push(Object.assign({}, base, { quantity: mq, product: 'MTF' }));
    if (cq > 0) orders.push(Object.assign({}, base, { quantity: cq, product: 'CNC' }));
  });
  if (!orders.length){ ktoast('Zerodha shows no sellable shares for these exits \u2014 nothing sent', 6000); return; }
  if (short.length) ktoast('\u26a0 fewer sellable shares than the ledger for ' + short.join(', ') + ' \u2014 selling what is there', 7000);
  const slices = buySlices(orders);
  if (BUYSLICER[id]) buyStop(id);
  BUYSLICER[id] = { slices: slices, i: 0, n: slices.length, btn: btn || null, t: 0, sell: true };
  zbSetSold(id, true);
  ktoast('Selling ' + exits.length + ' stock' + (exits.length === 1 ? '' : 's') + ' in ' + slices.length +
    ' slices \u2014 each a limit \u2264' + sliceRng() + '% BELOW live on NSE, MTF shares as MTF, demat as CNC; tap the counter to stop', 7000);
  buyFire(id);
  renderCards();
}

const TICKMEM = {};
let TICKS_LOADED = false;
function loadTicks(){
  if (TICKS_LOADED) return Promise.resolve(); TICKS_LOADED = true;
  return fetch('./tick_sizes.json', { cache: 'no-store' }).then(r => r.json())
    .then(d => { const t = (d && d.t) || {}; for (const k in t) if (!(k in TICKMEM)) TICKMEM[k] = t[k]; })
    .catch(() => { TICKS_LOADED = false; });
}
function tickFromMsg(m){ const x = /TICK\s*\[\s*([0-9.]+)\s*\]/i.exec(m || '') || /tick size for this scrip?t is\s*([0-9.]+)/i.exec(m || ''); return x ? parseFloat(x[1]) || 0 : 0; }
const sliceLakh = () => { const v = parseFloat(localStorage.getItem('sw_sell_slice_lakh')) || 25; return v > 0 ? v : 25; };
const sliceGap  = () => { const v = parseInt(localStorage.getItem('sw_sell_gap_s'), 10); return (v >= 3 && v <= 900) ? v : 150; };
const sliceRng  = () => { const v = parseFloat(localStorage.getItem('sw_sell_rng_pct')); return (v >= 0 && v <= 5) ? v : 0.5; };
function buyLimitPx(sym, px){ const t = TICKMEM[sym] || 0.05; return +((Math.ceil(px * (1 + sliceRng() / 100) / t)) * t).toFixed(2); }
function sellLimitPx(sym, px){ const t = TICKMEM[sym] || 0.05; return +((Math.floor(px * (1 - sliceRng() / 100) / t)) * t).toFixed(2); }
function freshLtp(sym){
  const w = (function(){ try { return (localStorage.getItem('live_worker_url') || '').trim(); } catch(e){ return ''; } })();
  if (!w) return Promise.resolve(null);
  const sep = w.includes('?') ? '&' : '?';
  return fetch(w + sep + 'symbols=' + encodeURIComponent(sym), { cache: 'no-store' }).then(r => r.json())
    .then(d => { const q = d && d.data && d.data[sym]; return (q && q.ltp != null) ? +q.ltp : null; }).catch(() => null);
}
const BUYSLICER = {};   // strategy id -> {slices, i, n, btn, t}
/* Liquidity-sized slices (user 2026-09-01: "instead of 25 lakhs, do it according to avg volume").
   A flat ₹25L was both too timid for ₹1,000-Cr/day names (ATHERENERG) and too chunky for
   ₹18-Cr/day ones (CAPLIPOINT). Per-stock cap = 1% of the 10-day average traded value from the
   engine's turnover series (₹ lakh/day, already loaded on this tab), clamped to ₹5L–₹1Cr.
   ~1% of ADV every 150s ≈ 1.5× the market's own per-beat volume — small, and the ≤0.5% limit
   band still bounds price. No turnover data (fresh listing, odd symbol) → the ₹L knob as before. */
const ADVCAP = {};
function advSliceCap(sym){
  if (sym in ADVCAP) return ADVCAP[sym];
  const baked = +((zbaDoc().sliceCaps || {})[sym]) || 0;   // live-liquidity cap baked at market open (see zbamts row)
  if (baked > 0){ ADVCAP[sym] = baked; return baked; }
  let cap = sliceLakh() * 1e5;                                   // fallback: the flat knob
  try {
    let tkr = null; for (const t in META){ if ((META[t].symbol || t) === sym){ tkr = t; break; } }
    if (tkr){ let sum = 0, n = 0;
      for (let off = dayOff(SF.end); off > dayOff(SF.end) - 20 && n < 10; off--){
        const v = turnoverAt(tkr, off); if (v > 0){ sum += v; n++; } }
      if (n) cap = Math.min(100e5, Math.max(5e5, 0.01 * (sum / n) * 1e5));   // 1% of ADV10, ₹5L–₹1Cr
    }
  } catch (e){}
  ADVCAP[sym] = cap; return cap;
}
function buySlices(orders){
  const per = {};
  orders.forEach(o => { const cap = advSliceCap(o.tradingsymbol);
    const px = o._px || o.price || 0;
    const chunk = Math.min(80000, px > 0 ? Math.max(1, Math.floor(cap / px)) : o.quantity);   // hard 80k-share cap: Zerodha refuses single orders >=1,00,000 (5-level market-depth limit)
    const list = []; let q = o.quantity;
    while (q > 0){ const take = Math.min(chunk, q); q -= take; list.push(Object.assign({}, o, { quantity: take })); }
    per[o.tradingsymbol + '|' + (o.product || '')] = list; });
  const out = []; let more = true, round = 0;       // round-robin; TAG each slice with its round #
  while (more){ more = false; for (const k in per){ const l = per[k]; if (l.length){ const sl = l.shift(); sl._round = round; out.push(sl); if (l.length) more = true; } } round++; }
  return out;
}
function buyStop(id, msg){ const B = BUYSLICER[id]; if (!B) return; clearTimeout(B.t); delete BUYSLICER[id];
  if (msg) ktoast(msg, 6500); renderCards(); }
/* A per-stock failure SKIPS that stock (drops its remaining slices) and continues the rest,
   instead of killing the whole basket (user 2026-09-01). Failed names are reported at the end. */
function buySkipStock(id, o0, reason){
  const B = BUYSLICER[id]; if (!B) return;
  (B.failed = B.failed || []).push(o0.tradingsymbol);
  const head = B.slices.slice(0, B.i), tail = B.slices.slice(B.i).filter(x => x.tradingsymbol !== o0.tradingsymbol);
  B.slices = head.concat(tail); B.n = B.slices.length;
  ktoast(o0.tradingsymbol + ' failed (' + reason + ') — skipped, continuing with the rest', 5200);
  if (B.i >= B.slices.length){ buyDone(id); return; }
  if (B.btn) B.btn.textContent = '⚡ ' + B.i + '/' + B.n;
  B.t = setTimeout(() => buyFire(id), 1000);
}
function buyDone(id){ const B = BUYSLICER[id]; if (!B) return;
  const f = B.failed && B.failed.length ? [...new Set(B.failed)] : [];
  buyStop(id, 'Basket done — ' + B.n + ' slices sent' + (f.length ? (B.sell ? ' · FAILED (sell separately): ' : ' · FAILED (buy separately): ') + f.join(', ') : ' (unfilled tails rest at their limits)')); }
function buyFire(id){
  const B = BUYSLICER[id]; if (!B) return;
  if (B.i >= B.slices.length){ buyDone(id); return; }
  const o0 = B.slices[B.i];
  freshLtp(o0.tradingsymbol).then(ltp => {
    const px = (ltp || o0._px || 0), o = Object.assign({}, o0); delete o._px;
    if (px > 0 && sliceRng() > 0){ o.order_type = 'LIMIT'; o.price = (o.transaction_type === 'SELL' ? sellLimitPx : buyLimitPx)(o.tradingsymbol, px); }
    zFetch('/order', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(o) }).then(res => {
      const st = res.st, j = res.j, msg = (j && j.message) || ('HTTP ' + st);
      if (st === 200 && j && j.data && j.data.order_id){
        const oid = j.data.order_id;
        B.t = setTimeout(() => { zFetch('/orders').then(ob => {
          const row = ((ob.j && ob.j.data) || []).filter(x => x.order_id === oid).pop();
          const ost = ((row && row.status) || '').toUpperCase(), omsg = (row && row.status_message) || '';
          if (ost === 'REJECTED'){
            const tk = tickFromMsg(omsg);
            if (tk > 0 && !o0._tickRetry){ TICKMEM[o0.tradingsymbol] = tk; o0._tickRetry = 1;
              ktoast(o0.tradingsymbol + ': tick is ' + tk + ' — re-pricing and retrying', 4200);
              B.t = setTimeout(() => buyFire(id), 1200); return; }
            buySkipStock(id, o0, 'rejected: ' + (omsg || 'no reason')); return;
          }
          B.i++; if (B.btn) B.btn.textContent = '⚡ ' + B.i + '/' + B.n;
          if (B.i >= B.slices.length) buyDone(id);
          else { const sameRound = B.slices[B.i] && B.slices[B.i]._round === o0._round;   // o0 = slice just filled
                 const wait = sameRound ? 3 : Math.max(3, sliceGap() - 2);                // gap ONLY between rounds
                 B.t = setTimeout(() => buyFire(id), wait * 1000); }
        }); }, 1800);
      }
      else if (ipBlocked(msg) || st === 0){ Z.directBlocked = true;
        buyStop(id, 'Static-IP rule — remaining slices need the Zerodha basket popup: reopen ⚡ and use "Kite basket"'); }
      else {
        if (o0.transaction_type !== 'SELL' && /MTF/i.test(msg) && /(block|not allowed|not permitted|blocked)/i.test(msg) && !o0._cncRetry){ o0._cncRetry = 1; o0.product = 'CNC';
          ktoast(o0.tradingsymbol + ': MTF blocked - buying as CNC (delivery) instead', 4500);
          B.t = setTimeout(() => buyFire(id), 1000); return; }
        const tk = tickFromMsg(msg);
        if (tk > 0 && !o0._tickRetry){ TICKMEM[o0.tradingsymbol] = tk; o0._tickRetry = 1;
          ktoast(o0.tradingsymbol + ': tick is ' + tk + ' (Zerodha) — re-pricing and retrying', 4200);
          B.t = setTimeout(() => buyFire(id), 1200); return; }
        buySkipStock(id, o0, msg); }
    });
  });
}
async function zbPlaceAll(){
  const orders = zbOrders();
  if (!orders.length){ ktoast('Nothing to buy — set an amount first'); return; }
  const b = $('zbGo'), est = orders.reduce((s, o) => { const r = ZB.rows.find(x => x.sym === o.tradingsymbol); return s + o.quantity * ((r && r.px) || 0); }, 0);
  if (b.dataset.arm !== '1'){ b.dataset.arm = '1';
    b.textContent = 'Confirm ' + orders.length + ' BUY orders ≈ ' + zinr(est) + ' ?';
    clearTimeout(ZB.t); ZB.t = setTimeout(zbArmReset, 8000); return; }
  b.dataset.arm = '';
  if (Z.directBlocked){ if (kiteSend(orders)){ zbSetBought(ZB.id, true); $('zbWrap').classList.remove('open'); } return; }
  await loadTicks();
  orders.forEach(o => { const r = ZB.rows.find(x => x.sym === o.tradingsymbol); o._px = (r && r.px) || o.price || 0; });
  const slices = buySlices(orders);
  if (BUYSLICER[ZB.id]) buyStop(ZB.id);
  BUYSLICER[ZB.id] = { slices: slices, i: 0, n: slices.length, btn: null, t: 0 };
  zbSetBought(ZB.id, true);
  $('zbWrap').classList.remove('open');
  ktoast('Buying in ' + slices.length + ' liquidity-sized slices (1% of the stock\u2019s 10-day traded value, \u20b95L\u2013\u20b91Cr each) every ' + sliceGap() + 's, each a limit \u2264' + sliceRng() + '% above live \u2014 keep this tab open; tap the \u26a1 counter to stop', 6500);
  renderCards();
  buyFire(ZB.id);
}
function kiteSend(orders){
  const key = (function(){ try { return localStorage.getItem('pf_kite_key') || ''; } catch(e){ return ''; } })();
  if (!key){ ktoast('No Kite API key in this browser — save it once on the portfolio page', 5000); return false; }
  if (orders.length > 10){ ktoast('Zerodha baskets take at most 10 orders — send in parts'); return false; }
  const w = window.open('about:blank', 'kite_basket');
  if (!w){ ktoast('Your browser blocked the Zerodha tab — allow pop-ups, then send again', 5200); return false; }
  const f = document.createElement('form');
  f.method = 'post'; f.action = 'https://kite.zerodha.com/connect/basket'; f.target = 'kite_basket'; f.style.display = 'none';
  const a = document.createElement('input'); a.type = 'hidden'; a.name = 'api_key'; a.value = key;
  const b = document.createElement('input'); b.type = 'hidden'; b.name = 'data'; b.value = JSON.stringify(orders);
  f.append(a, b); document.body.appendChild(f); f.submit(); f.remove();
  ktoast(orders.length + ' orders sent — review and confirm in the Zerodha tab', 4000);
  return true;
}

/* ---------- boot ---------- */
(async function boot(){
  const tg = $('spFavToggle');
  if (tg) tg.onclick = () => { FAVONLY = !FAVONLY; try { localStorage.setItem('sp_fav_only', FAVONLY ? '1' : '0'); } catch(e){} renderCards(); };
  const mb = $('spMode');
  const mLbl = () => { if (mb){ mb.textContent = PICKMODE === 'live' ? '⚡ Live picks' : '📅 Rebalance picks'; mb.classList.toggle('on', PICKMODE === 'live'); } };
  if (mb) mb.onclick = async () => {
    PICKMODE = PICKMODE === 'live' ? 'reb' : 'live';
    try { localStorage.setItem('sp_pick_mode', PICKMODE); } catch(e){}
    mLbl();
    const ids = Object.keys(PICKS);
    if (ids.length && await ensureEngine()){
      for (const id of ids){ const it = strategies().find(x => x.id === id); if (it) await screenPick(it); }
      renderCards(); fetchLive();
    }
  };
  mLbl();
  const sb2 = $('spSide');
  const sLbl = () => { if (sb2){ sb2.textContent = SIDE === 'sell' ? '\ud83d\udd3b Sell baskets' : '\ud83d\uded2 Buy baskets'; sb2.classList.toggle('on', SIDE === 'sell'); } };
  if (sb2) sb2.onclick = async () => {
    SIDE = SIDE === 'sell' ? 'buy' : 'sell'; sLbl(); renderCards();
    if (SIDE === 'sell'){ await feedPull(true); renderCards(); fetchLive(); }
  };
  sLbl();
  renderCards();
  refreshFavsFromSettings();
  zbaPull();   // synced \u20b9 amounts (token-gated row) \u2014 lands before any basket dialog opens
  feedPull();  // per-strategy held quantities \u2014 the sell view + engine-sized buys read these
  if (window.btSync){ try { await btSync.pullStrategies(); renderCards(); } catch(e){} }
})();
  };
})();