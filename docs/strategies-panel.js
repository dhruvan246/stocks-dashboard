/* strategies-panel.js — the Saved-Strategies engine as a mountable module.
   window.mountStrategies(container) builds the UI + runs picks/basket-buy inside it.
   Needs (host loads first): @supabase, bt-names.js, bt-sync.js, bt-identity.js, backtest-engine.js. */
(function(){
  'use strict';
  if (window.mountStrategies) return;
  var mounted = false;
  var SP_CSS = "\n.spwrap{font-size:13px;line-height:1.5}\n.spwrap .sp-top{display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap;margin-bottom:4px}\n.spwrap .sp-h{font-size:15px;font-weight:800;letter-spacing:-.015em;margin:0;display:flex;align-items:center;gap:9px}\n.spwrap .sp-h::before{content:\"\";width:4px;height:15px;border-radius:3px;flex:none;background:linear-gradient(180deg,var(--accent),color-mix(in srgb,var(--accent) 45%,transparent))}\n.spwrap .sp-actions{display:flex;gap:7px;align-items:center;flex-wrap:wrap}\n.spwrap .sp-sub{font-size:11.5px;color:var(--text-3);line-height:1.45;margin:4px 0}\n.spwrap .sp-input{flex:1;padding:7px 9px;font-size:13px;border:1px solid var(--border);border-radius:8px;background:var(--surface-2);color:var(--text);font-family:inherit}\n.spwrap .sym{color:var(--text-3);font-size:11px;font-weight:600}\n.spwrap .badge{font-size:9px;font-weight:800;letter-spacing:.05em;padding:1px 5px;border-radius:4px;background:var(--surface-2);color:var(--text-3);border:1px solid var(--border)}\n.spwrap .zpill{display:inline-flex;align-items:center;gap:6px;font-size:11.5px;font-weight:700;padding:5px 11px;border-radius:999px;background:color-mix(in srgb,var(--surface-2) 70%,transparent);color:var(--text-2);border:1px solid var(--border);white-space:nowrap}\n.spwrap .zpill.ok{color:var(--up);border-color:color-mix(in srgb,var(--up) 40%,transparent)} .spwrap .zpill.warn{color:#c98500;border-color:color-mix(in srgb,#c98500 40%,transparent)}\n.spwrap .sblk{border:1px solid var(--border);border-radius:14px;padding:13px 14px 11px;margin:10px 0;background:linear-gradient(180deg,color-mix(in srgb,var(--surface-2) 40%,transparent),transparent)}\n.spwrap .shead{display:flex;flex-wrap:wrap;align-items:center;gap:6px 10px;margin-bottom:8px}\n.spwrap .shead .nm2{font-size:13.5px;font-weight:800;letter-spacing:-.01em}\n.spwrap .tag{font-size:9.5px;font-weight:800;letter-spacing:.06em;padding:2.5px 7px;border-radius:5px;white-space:nowrap;text-transform:uppercase}\n.spwrap .tag.keep{background:color-mix(in srgb,var(--up) 16%,transparent);color:var(--up)}\n.spwrap .tag.new{background:color-mix(in srgb,var(--accent) 16%,transparent);color:var(--accent)}\n.spwrap .tag.off{background:var(--surface-2);color:var(--text-3);border:1px solid var(--border)}\n.spwrap .tag.warn{background:color-mix(in srgb,#c98500 18%,transparent);color:#c98500}\n.spwrap .tag.exit{background:color-mix(in srgb,var(--down) 16%,transparent);color:var(--down)}\n.spwrap .twrap{overflow-x:auto;-webkit-overflow-scrolling:touch;border:1px solid var(--border);border-radius:10px;background:var(--surface)}\n.spwrap table{width:100%;border-collapse:collapse;font-size:12.5px}\n.spwrap th{text-align:right;font-weight:700;color:var(--text-3);font-size:10px;text-transform:uppercase;letter-spacing:.08em;padding:8px 10px;border-bottom:1px solid var(--border);white-space:nowrap;background:color-mix(in srgb,var(--surface-2) 45%,var(--surface))}\n.spwrap th:first-child,.spwrap td:first-child{text-align:left}\n.spwrap td{padding:8px 10px;border-bottom:1px solid var(--border-2);text-align:right;white-space:nowrap;font-variant-numeric:tabular-nums}\n.spwrap tbody tr:last-child td{border-bottom:none}\n.spwrap td:nth-child(2),.spwrap th:nth-child(2){text-align:left}\n.spwrap .empty{text-align:center;color:var(--text-3);font-size:12.5px;padding:30px 10px}\n.spwrap .bal{border:1px solid var(--border);border-radius:14px;padding:13px 14px;margin:10px 0 4px;background:color-mix(in srgb,var(--surface-2) 45%,transparent)}\n.spwrap .bal-h{display:flex;align-items:center;gap:9px;flex-wrap:wrap;margin-bottom:8px}\n.spwrap .bal-h b{font-size:13.5px}\n.spwrap .bal-h .sub{font-size:11.5px;color:var(--text-3)}\n.spwrap .bal-h .go{margin-left:auto}\n.spwrap .snum{display:inline-block;font-size:9.5px;font-weight:800;background:var(--surface);border:1px solid var(--border);border-radius:4px;padding:0 4px;margin-left:3px;color:var(--text-3)}\n.spwrap .khelp{font-size:11.5px;color:var(--text-3);line-height:1.5;margin-top:10px}\n.spwrap .up{color:var(--up)} .spwrap .down{color:var(--down)}\n.spwrap .btn,#zbDlg .btn{border:1px solid var(--border);background:color-mix(in srgb,var(--surface-2) 70%,transparent);color:var(--text-2);border-radius:10px;padding:6px 11px;font-size:12.5px;font-weight:600;cursor:pointer;white-space:nowrap;display:inline-flex;align-items:center;gap:6px;transition:.15s;font-family:inherit;line-height:1.3}\n.spwrap .btn:hover,#zbDlg .btn:hover{border-color:color-mix(in srgb,var(--accent) 55%,transparent);color:var(--text)}\n.spwrap .btn.on,#zbDlg .btn.on{background:linear-gradient(135deg,var(--accent),color-mix(in srgb,var(--accent) 70%,#7c5cd6));border-color:transparent;color:#fff}\n.spwrap .btn.sell{background:var(--down);border-color:var(--down);color:#fff}\n.spwrap .btn:disabled{cursor:not-allowed;opacity:.55;transform:none}\n#ktoast{position:fixed;left:50%;bottom:24px;transform:translateX(-50%);z-index:300;background:var(--text);color:var(--bg);padding:9px 14px;border-radius:10px;font-size:12.5px;font-weight:600;opacity:0;pointer-events:none;transition:opacity .25s;max-width:90vw;text-align:center}\n#ktoast.show{opacity:1}\n.zchip{display:inline-block;padding:2px 7px;border-radius:20px;font-size:10.5px;font-weight:700;background:var(--surface-2);border:1px solid var(--border)}\n.zchip.ok{color:var(--up);border-color:var(--up)} .zchip.bad{color:var(--down);border-color:var(--down)} .zchip.open{color:var(--accent);border-color:var(--accent)}\n.zmsg{font-size:11px;color:var(--down);white-space:normal;text-align:left;max-width:280px}\n#zbWrap{position:fixed;inset:0;z-index:200;background:rgba(5,8,14,.55);backdrop-filter:blur(4px);display:none;align-items:center;justify-content:center;padding:16px}\n#zbWrap.open{display:flex}\n#zbDlg{width:min(620px,100%);max-height:92vh;overflow:auto;padding:18px;background:var(--surface);border:1px solid var(--border);border-radius:16px;box-shadow:var(--shadow)}\n#zbDlg h3{margin:0 0 2px;font-size:14.5px;font-weight:800;letter-spacing:-.01em}\n#zbDlg .sub{font-size:11.5px;color:var(--text-3)}\n#zbDlg .krow{display:flex;gap:8px;margin-top:10px}\n#zbDlg label{flex:1;font-size:10.5px;font-weight:800;color:var(--text-3);text-transform:uppercase;letter-spacing:.06em}\n#zbDlg input,#zbDlg select{width:100%;margin-top:4px;padding:7px 9px;font-size:13.5px;font-weight:600;border:1px solid var(--border);border-radius:8px;background:var(--surface-2);color:var(--text);font-family:inherit}\n#zbDlg table{margin-top:10px;width:100%;border-collapse:collapse;font-size:12.5px}\n#zbDlg th{font-size:10px;letter-spacing:.06em;color:var(--text-3);text-transform:uppercase;padding:7px 9px;text-align:right;border-bottom:1px solid var(--border);white-space:nowrap}\n#zbDlg th:first-child,#zbDlg td:first-child{text-align:left}\n#zbDlg td{padding:6px 9px;border-bottom:1px solid var(--border-2);text-align:right;font-variant-numeric:tabular-nums}\n#zbDlg td input.zbq{width:80px;margin:0;padding:5px 7px;text-align:right}\n#zbDlg td input.zbl{width:92px;margin:0;padding:5px 7px;text-align:right}\n#zbDlg .khelp{font-size:11.5px;color:var(--text-3);line-height:1.5;margin-top:10px}\n#zbTbl:not(.lim) .limcol{display:none}\n#ldWrap{position:fixed;inset:0;z-index:200;background:rgba(5,8,14,.55);backdrop-filter:blur(4px);display:none;align-items:center;justify-content:center;padding:16px}\n#ldWrap.open{display:flex}\n#ldDlg{width:min(680px,100%);max-height:92vh;overflow:auto;padding:18px;background:var(--surface);border:1px solid var(--border);border-radius:16px;box-shadow:var(--shadow)}\n#ldDlg h3{margin:0 0 2px;font-size:14.5px;font-weight:800;letter-spacing:-.01em}\n#ldDlg .sub{font-size:11.5px;color:var(--text-3);line-height:1.5}\n#ldDlg h4{margin:12px 0 2px;font-size:10.5px;font-weight:800;text-transform:uppercase;letter-spacing:.06em;color:var(--text-3)}\n#ldDlg .twrap{overflow-x:auto;-webkit-overflow-scrolling:touch;border:1px solid var(--border);border-radius:10px}\n#ldDlg table{width:100%;border-collapse:collapse;font-size:12.5px}\n#ldDlg th{font-size:10px;letter-spacing:.06em;color:var(--text-3);text-transform:uppercase;padding:6px 8px;text-align:right;border-bottom:1px solid var(--border);white-space:nowrap}\n#ldDlg th:first-child,#ldDlg td:first-child{text-align:left}\n#ldDlg td{padding:5px 8px;border-bottom:1px solid var(--border-2);text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap}\n#ldDlg tbody tr:last-child td{border-bottom:none}\n#ldDlg .krow{display:flex;gap:8px;margin-top:12px}\n#ldDlg .khelp{font-size:11.5px;color:var(--text-3);line-height:1.5;margin-top:10px}\n#ldDlg .warn{color:#c98500}\n#ldDlg .up{color:var(--up)} #ldDlg .down{color:var(--down)}\n#ldDlg .btn{border:1px solid var(--border);background:color-mix(in srgb,var(--surface-2) 70%,transparent);color:var(--text-2);border-radius:10px;padding:6px 11px;font-size:12.5px;font-weight:600;cursor:pointer;white-space:nowrap;display:inline-flex;align-items:center;gap:6px;transition:.15s;font-family:inherit;line-height:1.3}\n#ldDlg .btn.on{background:linear-gradient(135deg,var(--accent),color-mix(in srgb,var(--accent) 70%,#7c5cd6));border-color:transparent;color:#fff}\n#ldDlg .btn:disabled{cursor:not-allowed;opacity:.55}\n\n.spwrap .sp-cnt{font-size:11px;font-weight:800;background:var(--surface-2);border-radius:20px;padding:1px 8px;color:var(--text-2);letter-spacing:0}\n.spwrap .spchips{display:flex;gap:6px;align-items:center;overflow-x:auto;-webkit-overflow-scrolling:touch;padding:2px 0;margin:6px 0 10px;scrollbar-width:none}\n.spwrap .spchips::-webkit-scrollbar{display:none}\n.spwrap .spchips button{white-space:nowrap;background:color-mix(in srgb,var(--surface-2) 70%,transparent);border:1px solid var(--border);color:var(--text-2);border-radius:999px;padding:5px 12px;font:inherit;font-size:12px;font-weight:700;cursor:pointer}\n.spwrap .spchips button:hover{border-color:color-mix(in srgb,var(--accent) 55%,transparent);color:var(--text)}\n.spwrap .spchips button.on{background:color-mix(in srgb,var(--accent) 16%,transparent);border-color:color-mix(in srgb,var(--accent) 45%,transparent);color:var(--accent)}\n.spwrap .spchips .n{font-size:10.5px;font-weight:800;opacity:.75;margin-left:5px}\n.spwrap .bal tfoot td{font-weight:800;border-top:1px solid var(--border);background:color-mix(in srgb,var(--surface-2) 45%,transparent)}";
  var SP_DOM = "<div class=\"spwrap\"><div class=\"sp-top\"><h2 class=\"sp-h\">Strategies<span class=\"sp-cnt\" id=\"spCnt\"></span></h2><div class=\"sp-actions\"><span id=\"zStatus\" class=\"zpill\">Zerodha: checking…</span><button class=\"btn\" id=\"btnZLogin\" style=\"display:none\">Login to Zerodha ▸</button><button class=\"btn\" id=\"btnZSetup\" title=\"Kite worker URL\">Worker</button><button class=\"btn\" id=\"spMode\" title=\"Rebalance picks rank on the LAST CLOSE — the official screen the backtest uses. Live picks re-rank the price-based factors (52w high/low distance, returns, momentum) at the CURRENT market price — fundamentals and holdings stay as filed. Near the close of a rebalance day the two converge.\"></button><button class=\"btn\" id=\"spSide\"></button><button class=\"btn on\" id=\"btnLoadAll\">Load all picks</button></div></div><div class=\"sp-sub\">One block per saved strategy: the screen’s top picks as of the latest close, live prices during market hours, and a basket order for the whole block — you confirm before anything is placed.</div><div class=\"sp-sub\" id=\"status\"></div><div id=\"zSetupBox\" style=\"display:none\"><div class=\"khelp\">One-time per browser: your Kite worker URL (the portfolio/terminal shares it automatically).</div><div style=\"display:flex;gap:7px;max-width:520px;margin-top:6px\"><input id=\"zWUrl\" placeholder=\"https://…workers.dev\" class=\"sp-input\"><button class=\"btn on\" id=\"zWSave\">Save</button></div></div><div class=\"spchips\" id=\"spChips\"></div><div id=\"buyall\"></div><div id=\"cards\"><div class=\"empty\">Loading saved strategies…</div></div></div>";
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
/* Chips over the saved list — All · ★ Favourites · ✓ Bought/Sold today · Private — the same
   filter + totals treatment the holdings / positions / orders tabs got (user 2026-09-02). Exactly
   one chip is on. ★ keeps the persisted sp_fav_only; the other buckets are session state
   (SP_FILT) and a bucket that empties (its chip disappears) falls back to All / ★. */
let SP_FILT = '';
function spMarkSet(){ return SIDE === 'sell' ? zbSoldSet() : zbBoughtSet(); }
function spBuckets(all, favs){
  const mark = spMarkSet();
  return { all: all.length, fav: all.filter(it => isFavCfg(favs, it.cfg)).length,
           mark: all.filter(it => mark.has(it.id)).length, priv: all.filter(it => it._priv).length };
}
function spList(all, favs){
  const B = spBuckets(all, favs);
  if (SP_FILT && !B[SP_FILT]) SP_FILT = '';
  const mark = spMarkSet();
  let l = all;
  if (SP_FILT === 'mark') l = all.filter(it => mark.has(it.id));
  else if (SP_FILT === 'priv') l = all.filter(it => it._priv);
  else if (FAVONLY && B.fav > 0) l = all.filter(it => isFavCfg(favs, it.cfg));
  return l.slice().sort((a, b) => (isFavCfg(favs, b.cfg) ? 1 : 0) - (isFavCfg(favs, a.cfg) ? 1 : 0));   // ★ float to the top
}
function spActive(B){ return SP_FILT || ((FAVONLY && B.fav > 0) ? 'fav' : 'all'); }
function spChipName(k){ return k === 'fav' ? '★ Favourites' : k === 'mark' ? (SIDE === 'sell' ? '✓ Sold' : '✓ Bought') : k === 'priv' ? 'Private' : 'All'; }
/* footer label for a totals row: "Total" on All, else "Total · ★ Favourites 8 of 45" */
function spTotLbl(list){
  const all = uniqStrategies(), k = spActive(spBuckets(all, loadFavs()));
  return k === 'all' ? 'Total' : 'Total · ' + spChipName(k) + ' ' + list.length + ' of ' + all.length;
}
function spChipsRender(all, B, list){
  const act = spActive(B);
  const defs = [['all', 'All', B.all], ['fav', spChipName('fav'), B.fav], ['mark', spChipName('mark'), B.mark], ['priv', 'Private', B.priv]].filter(d => d[2] > 0);
  const el = $('spChips'); if (el) el.innerHTML = defs.map(d => '<button data-spf="' + d[0] + '" class="' + (act === d[0] ? 'on' : '') + '">' + esc(d[1]) + '<span class="n">' + d[2] + '</span></button>').join('');
  const c = $('spCnt'); if (c) c.textContent = list.length + (list.length !== all.length ? ' / ' + all.length : '');
}
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
/* ---------- live ticks + depth from the box (v3 ticker, user 2026-09-24) ----------
   Every 4 s while connected, cloud on and the market open: GET /ticks?i=<the symbols on screen>. The box
   opens Kite's WebSocket on demand and answers from memory. Ticks overlay LIVE.data (ltp, prevClose from
   the tick's close, bid/ask, day volume, 5-min volume) so every price cell updates tick-fresh, and the
   \u26a1 dialog / sell tables show bid/ask beside the price. */
const TK = { at: 0, ws: '', timer: null };
const partPct = () => { try { const v = parseInt(localStorage.getItem('sw_part_pct'), 10); return (v >= 0 && v <= 50) ? v : 10; } catch(e){ return 10; } };
function tickSyms(){ return [...new Set(Object.values(PICKS).flatMap(p => p.rows.map(r => r.sym)).concat(Object.values(FEED.byKey || {}).flatMap(f => f.rows.map(h => h.sym))).concat((ZB.rows || []).map(r => r.sym)))]; }
async function fetchTicks(){
  if (!cloudOn() || !Z.connected || !marketOpen() || document.hidden) return;
  const syms = tickSyms(); if (!syms.length) return;
  const r = await zFetch('/ticks?i=' + encodeURIComponent(syms.join(',')));
  if (r.st !== 200 || !r.j || !r.j.ok) return;
  TK.at = Date.now(); TK.ws = r.j.ws || '';
  const d = r.j.data || {}; let n = 0;
  if (!LIVE) LIVE = { ts: Date.now(), data: {} };
  for (const sym in d){ const q = d[sym]; if (!(q.ltp > 0)) continue; n++;
    LIVE.data[sym] = Object.assign(LIVE.data[sym] || {}, { ltp: q.ltp, prevClose: q.close > 0 ? q.close : (LIVE.data[sym] || {}).prevClose, bid: q.bid, ask: q.ask, bq: q.bq, aq: q.aq, vol: q.vol, vol5m: q.vol5m, tick: q.ts || q.at }); }
  if (n){ LIVE.ts = Date.now(); renderCards(); if ($('zbWrap') && $('zbWrap').classList.contains('open')) zbLiveTick(); }
}
function startTickLoop(){ if (TK.timer) return; TK.timer = setInterval(fetchTicks, 4000); }
const baCell = sym => { const q = liveQ(sym); return (q && q.bid > 0 && q.ask > 0) ? ' <span class="sym" title="best bid / best ask (live depth)">' + (+q.bid).toFixed(2) + '/' + (+q.ask).toFixed(2) + '</span>' : ''; };
function startLiveLoop(){
  if (LIVE_TIMER) return;
  LIVE_TIMER = setInterval(() => { if (!document.hidden && marketOpen()){ zbaPull(); if (Object.keys(PICKS).length){ fetchLive(); if (PICKMODE === 'live') liveRerankAll(); } } }, 60000);
  startTickLoop(); fetchTicks();
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
/* factor columns for a strategy's pick table (user 2026-09-22): the sort field + every filter
   field, each pick's live value shown, so you see WHY a stock is picked and where it sits vs the
   cut. Picks already arrive in sort order (screenAsOf sorts by sortBy). */
const SHORT_FIELD = { diiPct:'DII%', fiiPct:'FII%', diiChgPp:'ΔDII', fiiChgPp:'ΔFII',
  d52:'52wHi%', d52_low_pct:'52wLo%', rsi:'RSI', accel:'Accel%', changePercent:'Chg%',
  ret1m:'1m%', ret3m:'3m%', ret6m:'6m%', ret12m:'12m%', dma50:'50DMA%', dma200:'200DMA%',
  profitYoyPct:'NP-YoY%', profitTTM:'TTM-NP%', profitStreak:'NP-strk', profitAccel:'NP-accel',
  profitBase:'NP-base', mcap:'Mcap', hist_mcap:'HMcap', indRank:'IndRk', vol:'Vol%', delivPct:'Deliv%' };
const OP_SYM = { '<=':'≤', '>=':'≥', '<':'<', '>':'>', '=':'=', '==':'=' };
function pickCols(cfg){
  const out = [], idx = {};
  const push = (field, m) => { if (field in idx) Object.assign(out[idx[field]], m); else { idx[field] = out.length; out.push(Object.assign({ field: field }, m)); } };
  if (cfg.sortBy) push(cfg.sortBy, { sortCol: true, dir: cfg.dir });
  (cfg.filters || []).forEach(f => push(f.field, { op: f.op, val: f.val }));
  return out;
}
function pickFV(r, cols){ const o = {}; cols.forEach(c => { o[c.field] = r ? fieldVal(r, c.field) : null; }); return o; }
function fldLabel(field){ return SHORT_FIELD[field] || (typeof FIELD_LABEL !== 'undefined' && FIELD_LABEL[field]) || field; }
function fmtFV(v){ if (v == null || !isFinite(v)) return '—'; return Number.isInteger(v) ? String(v) : (Math.abs(v) >= 1000 ? Math.round(v).toLocaleString('en-IN') : (+v).toFixed(1)); }
function pickColHead(cols){ return cols.map(function(c){
  return '<th title="' + esc(fldLabel(c.field) + (c.op ? ' filter ' + (OP_SYM[c.op] || c.op) + ' ' + c.val : '') + (c.sortCol ? ' · SORT ' + (c.dir === 'high' ? 'high first' : 'low first') : '')) + '">' +
    esc(fldLabel(c.field)) +
    (c.sortCol ? ' <span class="sym">' + (c.dir === 'high' ? '▼' : '▲') + '</span>' : '') +
    (c.op ? ' <span class="sym">' + (OP_SYM[c.op] || c.op) + c.val + '</span>' : '') + '</th>'; }).join(''); }
function passOp(v, op, t){ if (v == null || !isFinite(v)) return true; switch(op){ case '<=': return v<=t; case '>=': return v>=t; case '<': return v<t; case '>': return v>t; case '=': case '==': return v===t; } return true; }
function pickColCells(cols, r, mark){ return cols.map(function(c){
  const v = (r && r.fv) ? r.fv[c.field] : null;
  const fail = mark && c.op && v != null && !passOp(v, c.op, c.val);
  return '<td' + (c.sortCol ? ' style="font-weight:700"' : '') + (fail ? ' class="down"' : '') + '>' + fmtFV(v) + '</td>'; }).join(''); }
/* live factor rows for EVERY universe stock at the pick date, so a HELD stock that's an exit (and so
   not in the top-N picks) still gets its factor values. One entry per strategy, refreshed when the
   date / live-rerank changes. */
const FACT_CACHE = {};
function factorMap(it){
  if (typeof SF === 'undefined' || !SF || !SF.end) return {};   // zba31: engine data not loaded yet (no picks) — the sell side must still render; factor cells show —
  const p = PICKS[it.id];
  const date = (p && p.live) ? (LIVEOV.date || SF.end) : SF.end;
  const stamp = date + '|' + ((p && p.liveTs) || '');
  const c = FACT_CACHE[it.id]; if (c && c.stamp === stamp) return c.m;
  const m = {};
  try { (factorsAt(dayOff(date), it.cfg) || []).forEach(r => { const sym = (META[r.tkr] && META[r.tkr].symbol) || r.tkr; if (sym) m[sym] = r; }); } catch(e){}
  FACT_CACHE[it.id] = { stamp: stamp, m: m }; return m;
}
function screenOne(it){
  const all = screenAsOf(it.cfg, SF.end), picks = all.slice(0, it.cfg.topN), bd = borderMap(it.cfg, all), cols = pickCols(it.cfg);
  PICKS[it.id] = { asOf: SF.end, cols: cols, edge: edgeRows(it.cfg, all), rows: picks.map((r, i) => ({ rank: i+1, sym: r.sym, tkr: r.tkr, bd: bd[i+1] || null,
    px: (META[r.tkr] && META[r.tkr].raw) ? META[r.tkr].raw : r.price, fv: pickFV(r, cols) })) };
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
    const all = screenAsOf(it.cfg, LIVEOV.date || SF.end), picks = all.slice(0, it.cfg.topN), bd = borderMap(it.cfg, all), cols = pickCols(it.cfg);
    PICKS[it.id] = { asOf: SF.end, live: true, liveTs: LIVEOV.ts, cols: cols, edge: edgeRows(it.cfg, all), rows: picks.map((r, i) => ({ rank: i + 1, sym: r.sym, tkr: r.tkr, bd: bd[i+1] || null,
      px: r.price, fv: pickFV(r, cols) })) };   // the spliced bar IS the live price; rebalance mode still shows META.raw
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
  const list = spList(all, favs);   // the LISTED strategies — the chips narrow this too
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
/* Marks are keyed by the REBALANCE (its T date), not the calendar day (user 2026-09-23): a basket
   sent on T must still read "sent" on T+1, or the button re-arms against a ledger that still lists
   the sold exits — and for a stock two strategies share, that re-armed basket would sell the OTHER
   strategy's shares. Each mark also records what was SENT per strategy ({SYM: qty}), so the buy leg
   can tell a sold-then-kept stock (buy it back) from a genuinely kept one. Synced in the same
   token-gated row; whole-field newer-wins like every other field there. */
function zbRebKey(){ return rebalWindow().tIso; }
function zbMarkDoc(field){ try { const d = zbaDoc()[field]; if (d && d.k === zbRebKey()) return { k: d.k, ids: Array.isArray(d.ids) ? d.ids.slice() : [], syms: Object.assign({}, d.syms || {}) }; } catch(e){} return { k: zbRebKey(), ids: [], syms: {} }; }
function zbMarkReb(field, id, on, sent){ try {
    const m = zbMarkDoc(field), a = new Set(m.ids);
    on ? a.add(id) : a.delete(id);
    if (on && sent){ const cur = m.syms[id] = Object.assign({}, m.syms[id] || {}); for (const k in sent) cur[k] = (+cur[k] || 0) + (+sent[k] || 0); } else if (!on) delete m.syms[id];
    const d = zbaDoc(); d[field] = { k: m.k, ids: [...a], syms: m.syms }; d.ts = Date.now();
    localStorage.setItem(ZBA_LS, JSON.stringify(d));
    clearTimeout(zbaSet._t); zbaSet._t = setTimeout(zbaPush, 1200);
  } catch(e){} renderCards(); }
function zbSentSyms(field, id){ return (zbMarkDoc(field).syms || {})[id] || {}; }
function zbBoughtSet(){ return new Set(zbMarkDoc('boughtReb').ids); }
function zbSetBought(id, on, sent){ zbMarkReb('boughtReb', id, on, sent); }
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
  cloudProbe(true);
  if (!j.connected){ Z.connected = false; zPill('Zerodha: not connected today', 'warn');
    $('btnZLogin').style.display = ''; return; }
  Z.connected = true; Z.user = j.user || '';
  zPill('● ' + Z.user, 'ok');
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
    h.j.data.forEach(r => { Z.hold[r.tradingsymbol] = { mtf: ((r.mtf || {}).quantity || 0), cnc: (r.quantity || 0) + (r.t1_quantity || 0), coll: (r.collateral_quantity || 0) }; });
    Z.held = new Set(h.j.data.filter(r => ((r.quantity||0)+(r.t1_quantity||0)+(r.collateral_quantity||0)+((r.mtf||{}).quantity||0)) > 0)
                             .map(r => r.tradingsymbol));
  }
}
$('btnZLogin').onclick = () => {
  const k = (function(){ try { return localStorage.getItem('pf_kite_key') || ''; } catch(e){ return ''; } })();
  if (!k){ ktoast('No API key saved in this browser — connect once from Positions & funds', 5000); return; }
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
/* ================= REBALANCE WIZARD (user 2026-09-23, world-class #2) =================
   One card at the top of the panel that knows which leg today is (sell day T, buy days T+1..T+3, the
   eve, or off-window) and lists live checks for it — connected, cloud slicer, books, picks + the right
   screen mode, exits/stragglers/buy-backs, timing, baskets sent, proceeds captured, leverage checked,
   ledger updated — each with a one-tap fix. Pure computation in wizardSteps() (unit-tested), HTML in
   renderWizard(), re-rendered with every renderCards(). */
const istNow = () => new Date(Date.now() + 330 * 60000);
const hhmm = d => String(d.getUTCHours()).padStart(2, '0') + ':' + String(d.getUTCMinutes()).padStart(2, '0');
const LST = { at: 0, j: null, busy: false };   // v4: the box's ledger status (today's order-book snapshot, tags, last apply), refreshed by ledgerStatus()
/* 'Fills capture' row: the box snapshots the day's order book at 15:35 IST (needs the Kite token armed) — the reconcile reads those */
function wizSnap(S, now){
  if (!cloudOn()) return; const L = LST.j;
  if (!L){ S('snap', 'off', 'Fills capture: checking the box\u2026', '', ''); return; }
  if (L.snapToday){ const sn = (L.snapshots || []).find(x => x.date === L.today) || {}; S('snap', 'ok', 'Today\u2019s order book captured on the box (' + (sn.orders || 0) + ' fill' + (sn.orders === 1 ? '' : 's') + (sn.captured ? ', ' + String(sn.captured).slice(11, 16) : '') + ')', 'the reconcile reads these \u00b7 tap to refresh after late fills', 'capture'); }
  else if (hhmm(now) < (L.snapAt || '15:35')) S('snap', 'info', 'The box captures today\u2019s fills at ' + (L.snapAt || '15:35'), 'stay logged in to Zerodha until then', Z.connected ? 'capture' : '');
  else S('snap', 'warn', 'Today\u2019s fills not captured yet', Z.connected ? 'capture now \u2014 before midnight (Kite forgets yesterday\u2019s orders)' : 'needs Zerodha connected', Z.connected ? 'capture' : 'login');
}
function wizardList(){ return spList(uniqStrategies(), loadFavs()); }
function wizardSteps(){
  const RW = rebalWindow(), list = wizardList(), now = istNow(), steps = [];
  const S = (k, st, label, detail, act) => steps.push({ k: k, st: st, label: label, detail: detail || '', act: act || '' });
  const okc = c => c ? 'ok' : 'bad';
  const t0 = (function(){ const d = new Date(RW.tIso + 'T00:00:00Z'); let q = new Date(d.getTime() - 864e5); while (isOff(q)) q = new Date(q.getTime() - 864e5); return q.toISOString().slice(0, 10); })();
  const today = now.toISOString().slice(0, 10);
  const leg = RW.sellIn ? 'sell' : RW.buyIn ? 'buy' : (today === t0 ? 'eve' : 'off');
  if (leg === 'off' || leg === 'eve'){
    S('when', 'info', leg === 'eve' ? 'Tomorrow is the sell day' : 'Next rebalance', 'sell the exits near the ' + RW.tlab + ' close \u00b7 buy the entries the morning of ' + RW.t1lab);
    S('cal', RW.calMissing.length ? 'warn' : (RW.calLoaded ? 'ok' : 'warn'), RW.calLoaded ? ('NSE holiday calendar: ' + (RW.calYears.join(', ') || 'none') + (RW.calMissing.length ? ' \u2014 no list yet for ' + RW.calMissing.join(', ') : '')) : 'NSE holiday calendar not loaded \u2014 weekends only', RW.calMissing.length ? 'add next year\u2019s list to docs/nse_holidays.json (published by NSE in December)' : 'sessions skip exchange holidays');
    return { leg: leg, RW: RW, steps: steps };
  }
  const books = list.filter(it => { const h = heldFor(it.cfg); return h && h.rows.length; });
  const loaded = list.filter(it => PICKS[it.id] && PICKS[it.id].rows.length);
  if (RW.calMissing.length || !RW.calLoaded) S('cal', 'warn', RW.calLoaded ? 'NSE holiday list missing for ' + RW.calMissing.join(', ') : 'NSE holiday calendar not loaded', 'dates assume weekends only \u2014 check the exchange calendar');
  S('zerodha', okc(Z.connected), Z.connected ? 'Zerodha connected \u2014 ' + Z.user : 'Zerodha not connected', Z.connected ? 'session ends 6:00 AM tomorrow' : 'daily login needed before anything can be sent', Z.connected ? '' : 'login');
  S('cloud', cloudOn() ? 'ok' : (CLOUD.ok === false ? 'warn' : 'off'), cloudOn() ? 'Cloud slicer on' : (CLOUD.ok === false ? 'Cloud slicer unavailable \u2014 baskets slice in this tab' : (cloudWanted() ? 'Cloud slicer: checking\u2026' : 'Cloud slicer switched off \u2014 in-tab slicing')), cloudOn() ? 'baskets keep running if this tab closes' : (CLOUD.ok === false ? 'keep this tab open while a basket runs' : ''), (!cloudOn() && CLOUD.ok !== false) ? 'cloud' : '');
  S('feed', okc(books.length), books.length ? books.length + ' strategy books loaded' : 'No strategy books \u2014 holdings feed missing', books.length ? '' : 'needs the pf token in this browser');
  S('picks', (loaded.length === list.length && list.length) ? 'ok' : 'bad', 'Picks loaded ' + loaded.length + '/' + list.length, '', loaded.length === list.length ? '' : 'load');
  if (leg === 'sell'){
    const mkt = marketOpen();
    const fresh = loaded.filter(it => livePicksOk(PICKS[it.id]));
    S('mode', !loaded.length ? 'off' : (fresh.length === loaded.length ? 'ok' : (mkt ? 'bad' : 'warn')),
      PICKMODE === 'live' ? 'Live picks \u2014 ' + fresh.length + '/' + loaded.length + ' fresh (< 3 min)' : 'Rebalance picks selected \u2014 today\u2019s sells need \u26a1 Live picks',
      mkt ? 'the ' + RW.tlab + ' close screen bakes only this evening' : 'market closed \u2014 live re-rank pauses', PICKMODE === 'live' ? (fresh.length === loaded.length ? '' : 'loadall') : 'live');
    let nEx = 0, val = 0, nBd = 0, todo = 0, sent = 0, withEx = 0, startBy = null, slices = 0; const sold = zbSoldSet();
    books.forEach(it => { const X = sellExits(it); if (!X.known) return;
      const ex = X.exits; if (!ex.length) return; withEx++; nEx += ex.length; val += X.est;
      nBd += X.rows.filter(r => r.bd).length;
      const td = ex.filter(r => r.remain == null ? true : r.remain > 0); todo += td.length;
      if (sold.has(it.id)) sent++;
      const rt = td.length ? sellRuntime(td.map(r => ({ h: { sym: r.h.sym, qty: (r.remain != null ? r.remain : r.h.qty), avg: r.h.avg }, px: r.px }))) : null;
      if (rt){ slices += rt.tot; if (rt.startBy && (!startBy || rt.startBy < startBy)) startBy = rt.startBy; } });
    S('exits', nEx ? 'info' : (loaded.length ? 'ok' : 'off'), nEx + ' exit' + (nEx === 1 ? '' : 's') + ' across ' + withEx + ' strateg' + (withEx === 1 ? 'y' : 'ies') + (val ? ' \u2248 ' + zinr(val) : ''), nBd ? nBd + ' borderline \u2014 sell those last (~3:25)' : (loaded.length ? 'no borderline names' : 'load picks first'), nEx ? 'sellside' : '');
    const late = !!(startBy && hhmm(now) > startBy);
    S('timing', !loaded.length ? 'off' : (!todo ? 'ok' : (late ? 'warn' : 'info')), todo ? (slices + ' slice' + (slices === 1 ? '' : 's') + ' still to send' + (startBy ? ' \u2014 start by ' + startBy + ' for a 3:28 finish' : '')) : (withEx ? 'All exit shares sent' : 'Nothing to sell'), late ? 'past the start-by time \u2014 start now' : ('now ' + hhmm(now) + ' IST'), todo ? 'sellside' : '');
    S('sent', withEx ? (sent === withEx ? 'ok' : (sent ? 'warn' : 'bad')) : 'off', 'Sell baskets sent ' + sent + '/' + withEx, '', sent < withEx ? 'sellside' : '');
    const cap = books.filter(it => proceedsOf(it.id)).length;
    S('proceeds', sent ? (cap >= sent ? 'ok' : 'warn') : 'off', 'Sell proceeds captured ' + cap + '/' + sent, (sent && cap < sent) ? 'auto-captures ~2 min after a basket finishes \u00b7 \u21bb proceeds chip to redo' : 'funds tomorrow\u2019s buys', '');
    wizSnap(S, now);
  } else {
    const legs = loaded.map(it => buyLeg(PICKS[it.id], RW)), okN = legs.filter(l => l.ok).length, bad = legs.find(l => !l.ok);
    S('mode', !loaded.length ? 'off' : (okN === loaded.length ? 'ok' : 'bad'),
      !loaded.length ? 'Load the picks first' : (okN === loaded.length) ? 'Official ' + RW.tlab + ' close screen on all ' + okN : (PICKMODE === 'live' ? 'Live picks selected \u2014 buys need Rebalance picks dated ' + RW.tlab : 'Waiting for the ' + RW.tlab + ' close in the data (' + okN + '/' + loaded.length + ' ready)'),
      bad ? bad.msg : 'the screen the backtest holds', okN === loaded.length ? '' : (PICKMODE === 'live' ? 'reb' : 'loadall'));
    let strag = 0, backs = 0, entries = 0, withEnt = 0, bought = 0; const bt = zbBoughtSet();
    books.forEach(it => { const p = PICKS[it.id]; if (!p || !p.rows.length) return;
      const X = sellExits(it); strag += X.exits.filter(r => r.remain == null ? true : r.remain > 0).length;
      const rb = rebuyRows(it, p); backs += rb.length;
      const held = heldFor(it.cfg), hs = new Set(held.rows.map(h => h.sym)), en = p.rows.filter(r => !hs.has(r.sym)).length + rb.length;
      if (en){ withEnt++; entries += en; if (bt.has(it.id)) bought++; } });
    S('strag', strag ? 'warn' : (loaded.length ? 'ok' : 'off'), strag ? strag + ' straggler' + (strag === 1 ? '' : 's') + ' to sell (kept on ' + RW.tlab + ', out of the final screen)' : 'No stragglers', '', strag ? 'sellside' : '');
    S('backs', backs ? 'warn' : (loaded.length ? 'ok' : 'off'), backs ? backs + ' buy-back' + (backs === 1 ? '' : 's') + ' (sold on ' + RW.tlab + ', still in the final screen)' : 'No buy-backs needed', backs ? 'pre-ticked in the \u26a1 dialog at the sold quantity' : '', backs ? 'buyside' : '');
    const cap = books.filter(it => proceedsOf(it.id)).length;
    S('proceeds', books.length ? (cap === books.length ? 'ok' : 'warn') : 'off', 'Actual sell proceeds for ' + cap + '/' + books.length + ' strategies', cap < books.length ? 'the rest size from today\u2019s prices (an estimate)' : 'buys sized from real fills', '');
    S('lev', LEV ? (LEV.blocked.length ? 'warn' : 'ok') : 'bad', LEV ? ('Leverage checked ' + hhmm(new Date(LEV.at + 330 * 60000)) + (LEV.blocked.length ? ' \u2014 MTF blocked: ' + LEV.blocked.join(', ') : ' \u2014 no MTF-blocked entrants')) : 'Leverage not checked yet', (LEV && LEV.blocked.length) ? 'those need the FULL amount in cash (auto-CNC)' : '', LEV ? '' : 'lev');
    S('bought', withEnt ? (bought === withEnt ? 'ok' : (bought ? 'warn' : 'bad')) : (loaded.length ? 'ok' : 'off'), withEnt ? 'Buy baskets sent ' + bought + '/' + withEnt + ' (' + entries + ' entr' + (entries === 1 ? 'y' : 'ies') + ')' : 'Nothing to buy', '', bought < withEnt ? 'buyside' : '');
    const ledgerDone = !!books.length && books.every(it => { const p = PICKS[it.id]; if (!p || !p.rows.length) return false; const held = heldFor(it.cfg), hs = new Set(held.rows.map(h => h.sym)); return !p.rows.some(r => !hs.has(r.sym)) && !held.rows.some(h => !p.rows.some(r => r.sym === h.sym)); });
    wizSnap(S, now);
    S('ledger', ledgerDone ? 'ok' : 'warn', ledgerDone ? 'Books match the official screen' : 'Books not updated yet \u2014 reconcile from the fills', ledgerDone ? 'the cloud ledger is current' : 'the box reconciles from the captured fills; you confirm before it writes', ledgerDone ? '' : 'reconcile');
  }
  return { leg: leg, RW: RW, steps: steps };
}
const WZ_ICON = { ok: '\u2713', warn: '\u26a0', bad: '\u2717', info: '\u2022', off: '\u2013' };
const WZ_ACT = { login: 'Login', cloud: 'Cloud on', load: 'Load picks', loadall: 'Refresh picks', live: 'Live picks', reb: 'Rebalance picks', sellside: 'Sell side', buyside: 'Buy side', lev: 'Check leverage', capture: 'Capture now', reconcile: 'Reconcile books' };
function renderWizard(){
  let box = $('spWizard');
  if (!box){ const ch = $('spChips'); if (!ch) return; box = document.createElement('div'); box.id = 'spWizard'; ch.insertAdjacentElement('beforebegin', box); }
  let W; try { W = wizardSteps(); } catch(e){ box.innerHTML = ''; return; }
  const RW = W.RW, live = (W.leg === 'sell' || W.leg === 'buy');
  const title = W.leg === 'sell' ? 'Sell day \u2014 ' + RW.tlab + ' (month-end close)' : W.leg === 'buy' ? 'Buy day \u2014 ' + (RW.planned ? RW.t1lab + ' morning' : 'buffer day (planned ' + RW.t1lab + ')') : W.leg === 'eve' ? 'Rebalance tomorrow' : 'Rebalance';
  const done = W.steps.filter(x => x.st === 'ok').length, total = W.steps.filter(x => x.st !== 'info' && x.st !== 'off').length;
  box.innerHTML = '<div class="bal wz"><div class="bal-h"><b>' + esc(title) + '</b><span class="sub">' + (live ? done + ' of ' + total + ' checks green' : esc(W.steps[0].detail)) + '</span></div>' +
    (live ? W.steps : W.steps.slice(1)).map(x => '<div class="wz-row wz-' + x.st + '"><span class="wz-ic">' + WZ_ICON[x.st] + '</span><span class="wz-l"><b>' + esc(x.label) + '</b>' + (x.detail ? ' <span class="sym">' + esc(x.detail) + '</span>' : '') + '</span>' + (x.act ? '<button class="btn wz-b" data-wz="' + x.act + '">' + esc(WZ_ACT[x.act]) + '</button>' : '') + '</div>').join('') + '</div>';
  if (live) ledgerStatus();   // v4: keep the box's ledger status fresh on the live legs (throttled inside)
}
function wizardAct(a){
  if (a === 'login'){ const b = $('btnZLogin'); if (b) b.click(); }
  else if (a === 'cloud'){ try { localStorage.setItem('sw_cloud_slicer', '1'); } catch(e){} cloudProbe(true); }
  else if (a === 'load' || a === 'loadall'){ const b = $('btnLoadAll'); if (b) b.click(); }
  else if (a === 'live' || a === 'reb'){ if ((a === 'live') !== (PICKMODE === 'live')) $('spMode').click(); }
  else if (a === 'sellside' || a === 'buyside'){ if ((a === 'sellside') !== (SIDE === 'sell')) $('spSide').click(); }
  else if (a === 'capture'){ ledgerCapture(); }
  else if (a === 'reconcile'){ ledgerOpen(); }
  else if (a === 'lev'){ if (SIDE !== 'buy') $('spSide').click(); setTimeout(() => { const g = $('levGo'); if (g) g.click(); else ktoast('Load the picks first \u2014 the leverage check needs the entrants'); }, 300); }
}
function renderCards(){
  if (document.querySelector('#cards [data-arm="1"], #buyall [data-arm="1"]')) return;   // an armed Sell/Buy confirm is showing — don't rebuild under it
  const favs = loadFavs();
  const favOrder = (function(){ try { return JSON.parse(localStorage.getItem('bt_fav_strategies') || '[]'); } catch(e){ return []; } })();
  const favNum = cfg => { let i = favOrder.indexOf(identityKey(cfg)); if (i < 0 && typeof ruleKey === 'function') i = favOrder.indexOf(ruleKey(cfg)); return i + 1; };
  const all = uniqStrategies();
  // favourites float to the top; the chips narrow the list (★ on by default when stars exist)
  const list = spList(all, favs);
  spChipsRender(all, spBuckets(all, favs), list);
  if (!list.length){ $('cards').innerHTML = '<div class="empty">No saved strategies found.</div>'; renderBuyAll(list); return; }
  const bought = zbBoughtSet();
  const RW = rebalWindow();
  const h = list.map(it => {
    const en = (typeof strategyEnglish === 'function') ? strategyEnglish(it.cfg) : '';
    const disp = en || nameWithBasis(it.name, it.cfg);
    if (SIDE === 'sell') return sellCardHTML(it, disp, favNum);
    const p = PICKS[it.id];
    const leg = RW.buyIn ? buyLeg(p, RW) : null, backs = p ? rebuyRows(it, p) : [];
    let body = '';
    if (p){
      const cols = p.cols || [];
      body = '<div class="twrap"><table><thead><tr><th>#</th><th>Pick</th><th>Live \u20b9</th><th>Day %</th>' + pickColHead(cols) + '</tr></thead><tbody>' +
        p.rows.map(r => {
          const q = liveQ(r.sym); const px = q && q.ltp != null ? q.ltp : r.px;
          const chg = q && q.ltp != null && q.prevClose ? (q.ltp / q.prevClose - 1) * 100 : null;
          const bk = backs.find(b => b.sym === r.sym);
          return '<tr><td class="sym">' + r.rank + '</td>' +
            '<td><b>' + esc(r.sym) + '</b> ' + (bk ? '<span class="tag" style="background:color-mix(in srgb,#c98500 18%,transparent);color:#c98500" title="Sold on the ' + esc(RW.tlab) + ' close but still in the official screen \u2014 buy the same ' + bk.qty.toLocaleString('en-IN') + ' back">buy back</span>' : (Z.held.has(r.sym) ? '<span class="tag keep">held</span>' : '<span class="tag new">new</span>')) +
            (q && q.ltp != null ? '' : ' <span class="badge">EOD</span>') + '</td>' +
            '<td>\u20b9' + (+px).toFixed(2) + baCell(r.sym) + '</td>' +
            '<td class="' + (chg == null ? 'sym' : chg >= 0 ? 'up' : 'down') + '">' + (chg == null ? '\u2014' : (chg >= 0 ? '+' : '') + chg.toFixed(2) + '%') + '</td>' + pickColCells(cols, r) + '</tr>';
        }).join('') + '</tbody></table></div>' +
        (leg && !leg.ok ? '<div class="khelp">\u26a0 <b>' + esc(leg.msg) + '</b> \u2014 buying is locked until then.</div>' : '');
    }
    return '<div class="sblk"><div class="shead">' + (favNum(it.cfg) ? '<span class="snum" style="font-size:11px;background:var(--buy);color:#fff;border-color:var(--buy);padding:1px 6px;margin:0 4px 0 0">#' + favNum(it.cfg) + '</span>' : '') + '<span class="nm2" title="Code-name: ' + esc(nameWithBasis(it.name, it.cfg)) + '">' + (isFavCfg(loadFavs(), it.cfg) ? '\u2605 ' : '') + esc(disp) + '</span>' +
      (it._priv ? '<span class="tag new">private</span>' : '') +
      (bought.has(it.id) ? '<span class="tag keep" data-unbought="' + esc(it.id) + '" title="Basket sent to Zerodha this rebalance \u2014 click if that was cancelled" style="cursor:pointer">\u2713 bought</span>' : '') +
      '<span class="sym">' + esc(cardMeta(it.cfg)) + (p ? (p.live ? ' \u00b7 LIVE picks \u00b7 close ' + esc(p.asOf) + ' + prices ' + (p.liveTs ? new Date(p.liveTs).toTimeString().slice(0, 5) : 'now') : ' \u00b7 picks as of ' + esc(p.asOf)) : '') + '</span>' +
      (p && p.live ? '<span class="tag new">LIVE</span>' : '') +
      '<span style="margin-left:auto; display:flex; gap:6px">' +
      '<button class="btn" data-load="' + esc(it.id) + '">' + (p ? 'Refresh picks' : 'Picks') + '</button>' +
      (p ? (BUYSLICER[it.id]
              ? '<button class="btn on" data-basket="' + esc(it.id) + '">Buying ' + BUYSLICER[it.id].i + '/' + BUYSLICER[it.id].n + '</button>'
              : (bought.has(it.id)
                   ? '<button class="btn" disabled title="Already bought this rebalance \u2014 click the \u2713 bought chip to re-enable" style="opacity:.5;cursor:not-allowed;color:var(--up)">\u2713 Bought</button>'
                   : (!RW.buyIn
                        ? '<span class="tag off" title="Buy baskets act only on the morning after month-end \u2014 ' + esc(RW.t1lab) + ' (buffer to ' + esc(RW.t3lab) + '), on the official ' + esc(RW.tlab) + ' close screen, funded by the sell proceeds. Off-window this is a preview only.">Locked \u00b7 arms ' + esc(RW.t1lab) + '</span>'
                        : (leg.ok ? '<button class="btn on" data-basket="' + esc(it.id) + '">Buy basket</button>'
                                  : '<span class="tag warn" title="' + esc(leg.msg) + '">' + (p.live ? 'Rebalance picks required' : 'Waiting for ' + esc(RW.tlab) + ' close') + '</span>')))) : '') +
      '</span></div>' + body + '</div>';
  }).join('');
  $('cards').innerHTML = h;
  renderBuyAll(list);
  renderWizard();
  for (const id in BUYSLICER){
    const el = (id === '__all__') ? $('balGo') : (id === '__residual__') ? $('residGo') : (id === '__exitall__') ? $('exitAllGo') : (id === '__reenter__') ? $('reenterGo')
      : (document.querySelector('[data-basket="' + id + '"]') || document.querySelector('[data-sellbasket="' + id + '"]'));
    BUYSLICER[id].btn = el || null; if (el) el.textContent = (BUYSLICER[id].sell ? 'Selling ' : 'Buying ') + BUYSLICER[id].i + '/' + BUYSLICER[id].n + (BUYSLICER[id].remote ? ' \u2601' : ''); }
}
/* ---------- THIS REBALANCE: every stock to buy, in one place (user 2026-08-31) ----------
   Eight strategies × three picks means eight blocks to read, and the same stock often appears in
   several of them — the actual shopping list is neither obvious nor deduplicated. This merges
   every loaded strategy's picks into ONE table: a row per stock, which strategies want it
   (numbered as they appear below), the money they contribute together, and the quantity that
   buys at the live price. Money per strategy = the ₹ amount last used in its ⚡ dialog
   (remembered per strategy); strategies with no amount yet are named so you can set one.
   "Buy all" runs the same sliced, NSE-only, tick-aware engine as a single basket. */
/* The buy leg trusts ONE screen: rebalance mode, dated exactly T — the official month-end close the
   backtest holds. A live re-rank on T+1 prices is a different screen; a pick date before T means the
   evening bake has not landed yet (~20:45 IST on T, retry 08:55 IST next morning). */
function buyLeg(p, RW){
  if (!p || !p.rows.length) return { ok: false, msg: 'Load the picks first' };
  if (p.live) return { ok: false, msg: 'Switch to Rebalance picks \u2014 buys use the official ' + RW.tlab + ' close screen, not a live re-rank' };
  if (p.asOf !== RW.tIso) return { ok: false, msg: 'Waiting for the ' + RW.tlab + ' close in the data (picks are as of ' + p.asOf + '; it lands ~8:45 pm, retry 8:55 am) \u2014 refresh picks' };
  return { ok: true, msg: '' };
}
/* Buy-backs (user 2026-09-23): a stock this strategy SOLD on T that the official T-close screen still
   holds. The ledger still lists it as held, so plain sizing would silently skip it; the demat shortfall
   (what the keeping strategies should own minus what Zerodha holds) says it is gone. Same quantity
   back — the backtest never sold it. Funded by its own sale (see buyAllAgg). */
function rebuyRows(it, p){
  const held = heldFor(it.cfg); if (!held || !held.rows.length || held.method === 'reset' || !p || !p.rows.length) return [];
  const PR = proceedsOf(it.id), sold = zbSentSyms('soldReb', it.id), out = [];
  p.rows.forEach(r => { const h = held.rows.find(x => x.sym === r.sym); if (!h) return;
    const soldQ = (PR && PR.filled && PR.filled[r.sym] != null) ? +PR.filled[r.sym] : (+sold[r.sym] || 0); if (soldQ <= 0) return;
    let q = Math.min(h.qty, soldQ);
    if (Z.connected){ const gap = Math.max(0, keeperQty(r.sym, it) + h.qty - dematQty(r.sym)); q = Math.min(q, gap); }
    if (q > 0) out.push({ sym: r.sym, qty: q, px: r.px }); });
  return out;
}
function buyAllAgg(list){
  const agg = {}, missing = [];
  let nAct = 0, nEst = 0;
  list.forEach((it, i) => {
    const p = PICKS[it.id]; if (!p || !p.rows.length) return;
    /* engine sizing (2026-09-01, FIXED): a HOLD strategy funds each NEW entry from its EXIT proceeds ÷ open
       slots — never a fresh full amount. RESET sells its whole book, split topN ways. Proceeds = the ACTUAL
       sell fills captured on T (Option A, user 2026-09-23) when present, else the exits' value at today's
       prices (an estimate — the exits were sold at the T close). Only a strategy with no held book yet (a
       first-ever buy) falls back to the ₹ amount typed in its dialog. */
    const held = heldFor(it.cfg);
    const isReset = !!(held && held.method === 'reset');
    const holdKeep = held && held.rows.length && !isReset;
    const heldSet = holdKeep ? new Set(held.rows.map(x => x.sym)) : null;
    const rows2 = holdKeep ? p.rows.filter(r => !heldSet.has(r.sym)) : p.rows;
    const backs = holdKeep ? rebuyRows(it, p) : [];
    const PR = proceedsOf(it.id);
    let per = 0;
    if (held && held.rows.length){
      const pxOf = h => { const q = liveQ(h.sym); return (q && q.ltp != null) ? +q.ltp : (h.avg || 0); };
      PR ? nAct++ : nEst++;
      if (isReset){ const book = PR ? PR.amt : held.rows.reduce((s, h) => s + h.qty * pxOf(h), 0);
        per = book / (held.topN || p.rows.length); }
      else { const pickSet = new Set(p.rows.map(r => r.sym));
        const backVal = backs.reduce((s, b) => s + ((PR && PR.val && PR.val[b.sym]) || 0), 0);
        const exitVal = PR ? Math.max(0, PR.amt - backVal) : held.rows.filter(h => !pickSet.has(h.sym)).reduce((s, h) => s + h.qty * pxOf(h), 0);
        const stays = held.rows.filter(h => pickSet.has(h.sym)).length;
        per = exitVal / Math.max(1, (held.topN || p.rows.length) - stays); }
    } else { const amt = zbaGet(it.id); if (!amt) missing.push(i + 1); per = amt && rows2.length ? amt / rows2.length : 0; }
    const add = (sym, px0, contrib, back) => {
      const q = liveQ(sym), px = (q && q.ltp != null) ? q.ltp : px0;
      const a = agg[sym] = agg[sym] || { sym: sym, px: px, amt: 0, from: [], capped: false, back: false };
      a.px = px; a.amt += contrib; a.from.push(i + 1); if (back) a.back = true; return a; };
    rows2.forEach(r => { const cap = zbCap(r.sym), contrib = cap > 0 ? Math.min(per, cap) : per;   // per-basket ₹ cap (HFCL)
      const a = add(r.sym, r.px, contrib, false); if (cap > 0 && contrib < per) a.capped = true; });
    backs.forEach(b => { const q = liveQ(b.sym), px = (q && q.ltp != null) ? q.ltp : b.px; add(b.sym, b.px, b.qty * px, true); });
  });
  const rows = Object.values(agg).map(a => Object.assign(a, { qty: (a.amt > 0 && a.px > 0) ? Math.floor(a.amt / a.px) : 0 }))
    .sort((x, y) => (y.amt - x.amt) || (x.sym < y.sym ? -1 : 1));
  return { rows: rows, missing: missing, nAct: nAct, nEst: nEst };
}
/* ================= FILLS & SLIPPAGE (user 2026-09-24, world-class #3) =================
   Cloud baskets (kite-relay v2.1) follow every sent order to its fill and remember the arrival price
   the limit was pegged to, so each basket reports filled vs sent, value, slippage vs arrival (bps and
   rupees, value-weighted; positive = cost) and time to complete. With no cloud basket today the panel
   falls back to the Zerodha order book grouped by our basket tags (fills only — no arrival price). */
const dayStartIST = () => { const d = istNow(); return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()) - 330 * 60000; };
const FILL_TERM = new Set(['COMPLETE', 'CANCELLED', 'REJECTED']);
const FILLS_OB = { at: 0, rows: null, busy: false };
function tagLabel(tag){
  if (/^ss/.test(tag)){ const it = strategies().find(x => sellTag(x.id) === tag); return it ? (((typeof strategyEnglish === 'function' && strategyEnglish(it.cfg)) || it.name || it.id) + ' \u00b7 exits') : tag; }
  if (/^sb/.test(tag)){ const it = strategies().find(x => buyTag(x.id) === tag); return it ? (((typeof strategyEnglish === 'function' && strategyEnglish(it.cfg)) || it.name || it.id) + ' \u00b7 entries') : tag; }
  return { swbasket: 'Buy baskets (in-tab)', swresid: 'Buy remaining', swexitall: 'Exit all', swreenter: 'Re-enter', swcloud: 'Cloud basket', swtest: 'Rehearsal' }[tag] || tag;
}
async function fillsFromOrderBook(){
  if (!Z.connected || FILLS_OB.busy || Date.now() - FILLS_OB.at < 60000) return;
  FILLS_OB.busy = true;
  try {
    const r = await zFetch('/orders'), list = (r.j && r.j.data) || [], by = {};
    list.forEach(o => { const tag = String(o.tag || ''); if (!/^(ss|sw)/.test(tag)) return;
      const k = tag + '|' + o.transaction_type, a = by[k] = by[k] || { label: tagLabel(tag), side: o.transaction_type, filled: 0, value: 0, open: 0, n: 0 };
      const f = +o.filled_quantity || 0, px = +o.average_price || 0; a.filled += f; a.value += f * px; a.n++;
      if (!FILL_TERM.has(String(o.status || '').toUpperCase())) a.open++; });
    FILLS_OB.rows = Object.values(by).map(a => Object.assign(a, { avg: a.filled ? a.value / a.filled : 0 })).sort((x, y) => y.value - x.value);
    FILLS_OB.at = Date.now();
  } catch(e){} finally { FILLS_OB.busy = false; }
  renderCards();
}
const cloudJobsToday = () => (CLOUD.jobs || []).filter(j => (j.created || 0) >= dayStartIST()).sort((a, b) => (b.created || 0) - (a.created || 0));
function fillsKick(){ if (!cloudJobsToday().length) fillsFromOrderBook(); }
function slipCell(f){
  if (!f || f.slipBps == null) return '<td>\u2014</td>';
  const cost = f.slipRs > 0, cls = cost ? 'down' : (f.slipRs < 0 ? 'up' : '');
  return '<td class="' + cls + '" title="value-weighted vs the arrival price each slice was pegged to">' + (cost ? 'cost ' : (f.slipRs < 0 ? 'gain ' : '')) + zinr(Math.abs(f.slipRs)) + ' \u00b7 ' + Math.abs(f.slipBps) + ' bps</td>';
}
function fillsHTML(){
  const jobs = cloudJobsToday();
  if (!jobs.length){
    const ob = FILLS_OB.rows; if (!ob || !ob.length) return '';
    const tv = ob.reduce((s, r) => s + r.value, 0);
    return '<div class="bal"><div class="bal-h"><b>Fills today</b><span class="sub">from the Zerodha order book, by basket tag \u00b7 slippage vs arrival needs a cloud basket</span></div>' +
      '<div class="twrap"><table><thead><tr><th>Basket</th><th>Side</th><th>Filled</th><th>Avg \u20b9</th><th>Value</th></tr></thead><tbody>' +
      ob.map(r => '<tr><td><b>' + esc(r.label) + '</b></td><td>' + esc(r.side) + '</td><td>' + r.filled.toLocaleString('en-IN') + (r.open ? ' <span class="sym">+' + r.open + ' open</span>' : '') + '</td><td>' + (r.avg ? r.avg.toFixed(2) : '\u2014') + '</td><td>' + zinr(r.value) + '</td></tr>').join('') +
      '</tbody><tfoot><tr><td>Total</td><td></td><td></td><td></td><td>' + zinr(tv) + '</td></tr></tfoot></table></div></div>';
  }
  let tv = 0, ts = 0, hasSlip = false;
  const rows = jobs.map(j => { const f = j.fill || {}; tv += f.value || 0; if (f.slipBps != null){ ts += f.slipRs || 0; hasSlip = true; }
    const dur = (f.firstAt && f.lastAt) ? Math.max(1, Math.round((f.lastAt - f.firstAt) / 60000)) : null;
    const st = j.status === 'running' ? (j.i + '/' + j.n + ' \u2601') : j.status;
    return '<tr><td><b>' + esc(j.label || j.id) + '</b></td><td>' + esc(j.side) + '</td><td>' + esc(st) + (f.open ? ' <span class="sym">' + f.open + ' open</span>' : '') + '</td>' +
      '<td>' + (f.filledQty || 0).toLocaleString('en-IN') + ' / ' + (f.sentQty || 0).toLocaleString('en-IN') + '</td><td>' + zinr(f.value || 0) + '</td>' + slipCell(f) +
      '<td>' + (dur != null ? dur + ' min' : '\u2014') + '</td></tr>'; }).join('');
  return '<div class="bal"><div class="bal-h"><b>Fills today</b><span class="sub">' + jobs.length + ' cloud basket' + (jobs.length === 1 ? '' : 's') + ' \u00b7 slippage = filled price vs the arrival price each slice was pegged to (positive = cost)</span></div>' +
    '<div class="twrap"><table><thead><tr><th>Basket</th><th>Side</th><th>Status</th><th>Filled / sent</th><th>Value</th><th>Slippage</th><th>Took</th></tr></thead><tbody>' + rows +
    '</tbody><tfoot><tr><td>Total</td><td></td><td></td><td></td><td>' + zinr(tv) + '</td><td class="' + (ts > 0 ? 'down' : ts < 0 ? 'up' : '') + '">' + (hasSlip ? ((ts > 0 ? 'cost ' : ts < 0 ? 'gain ' : '') + zinr(Math.abs(ts)) + (tv ? ' \u00b7 ' + Math.round(Math.abs(ts) / tv * 1e4) + ' bps' : '')) : '\u2014') + '</td><td></td></tr></tfoot></table></div></div>';
}
function renderBuyAll(list){
  const box = $('buyall'); if (!box) return;
  if (SIDE === 'sell'){ box.innerHTML = renderSellAll(list) + renderExitAll() + '<div class="khelp" style="margin:6px 4px 10px">Timing (user 2026-09-23): <b>sell the exits near the close of ' + esc(rebalWindow().tlab) + '</b> \u2014 the month-end session, on that day\u2019s near-final \u26a1 live picks \u00b7 <b>buy the entries the next morning (' + esc(rebalWindow().t1lab) + ')</b> on the official month-end close screen, funded by the captured sell proceeds. A stock kept on month-end that drops out of the final screen sells the next morning as a straggler.</div>' + fillsHTML(); wireExitAll(); fillsKick(); return; }
  const residHTML = renderResidual();
  const withPicks = list.filter(it => PICKS[it.id] && PICKS[it.id].rows.length);
  if (!withPicks.length){ box.innerHTML = renderReenter() + residHTML + fillsHTML(); wireResidGo(); wireReenter(); fillsKick(); return; }
  const agg = buyAllAgg(list), rows = agg.rows;
  const totAmt = rows.reduce((s, r) => s + r.amt, 0), totQty = rows.reduce((s, r) => s + (r.qty || 0), 0);
  const anyQty = rows.some(r => r.qty > 0);
  const B = BUYSLICER['__all__'];
  box.innerHTML = renderReenter() + residHTML + '<div class="bal"><div class="bal-h"><b>This rebalance · ' + rows.length + ' stocks to buy</b>' +
    '<span class="sub">from ' + withPicks.length + ' ' + (withPicks.length === 1 ? 'strategy' : 'strategies') +
      (totAmt ? ' · ' + zinr(totAmt) : '') +
      (agg.missing.length ? ' · no amount set for ' + agg.missing.map(n => '#' + n).join(', ') : '') + '</span>' +
    '<span class="go"><button class="btn" id="levGo" title="Zerodha margincalc on every entrant \u2014 catches MTF-blocked names that will need FULL cash via the auto-CNC fallback">Check leverage</button></span></div><div id="levOut"></div>' +   // Buy-all button REMOVED (user 2026-09-01): one round-robin queue for ~₹35 Cr ≈ 141 slices × 150s ≈ 6h — buy per-strategy (8 parallel ⚡ slicers) instead; the table stays as the consolidated checklist
    '<div class="twrap"><table><thead><tr><th>Stock</th><th>Strategies</th><th>Live ₹</th><th>Qty</th><th>Amount</th></tr></thead><tbody>' +
    rows.map(r => '<tr><td><b>' + esc(r.sym) + '</b> ' + (r.back ? '<span class="tag" style="background:color-mix(in srgb,#c98500 18%,transparent);color:#c98500" title="Sold on the month-end close but still in the official screen \u2014 restore the same quantity">buy back</span>' : (Z.held.has(r.sym) ? '<span class="tag keep">held</span>' : '<span class="tag new">new</span>')) + '</td>' +
      '<td class="sym">' + r.from.map(n => '<span class="snum">#' + n + '</span>').join('') + (r.from.length > 1 ? ' <b>×' + r.from.length + '</b>' : '') + '</td>' +
      '<td>₹' + (+r.px).toFixed(2) + '</td>' +
      '<td>' + (r.qty ? r.qty.toLocaleString('en-IN') : '—') + '</td>' +
      '<td>' + (r.amt ? zinr(r.amt) : '—') + (r.capped ? ' <span class="sym" title="capped per basket">cap</span>' : '') + '</td></tr>').join('') +
    '</tbody><tfoot><tr><td>' + esc(spTotLbl(list)) + '</td><td class="sym">' + rows.length + ' stock' + (rows.length === 1 ? '' : 's') + '</td><td></td>' +
    '<td>' + (totQty ? totQty.toLocaleString('en-IN') : '—') + '</td><td>' + (totAmt ? zinr(totAmt) : '—') + '</td></tr></tfoot></table></div>' +
    '<div class="khelp">Numbered by the strategy blocks below. Amounts are the engine’s rebalance sizing — each new entry funded by its strategy’s EXIT proceeds: ' + (agg.nAct ? '<b>actual sell fills captured on ' + esc(rebalWindow().tlab) + '</b> for ' + agg.nAct + (agg.nEst ? ', ' : ' ') : '') + (agg.nEst ? '<b>estimated at today’s prices</b> for ' + agg.nEst + ' (no proceeds captured — the exits were sold at the month-end close) ' : '') + (agg.nAct || agg.nEst ? 'strateg' + ((agg.nAct + agg.nEst) === 1 ? 'y' : 'ies') + '; ' : '') + 'a first-ever buy with no holdings falls back to the ₹ amount in its ⚡ dialog. A stock several strategies want is bought once, for the combined amount; <b>buy back</b> = sold on the month-end close but still in the official screen.</div></div>' + fillsHTML();
  fillsKick();
  const lg = $('levGo'); if (lg) lg.onclick = () => zbLevSweep(rows);
  const go = $('balGo');
  if (go) go.onclick = () => {
    if (BUYSLICER['__all__']){ const S = BUYSLICER['__all__'];
      buyStop('__all__', 'Buying stopped — ' + S.i + '/' + S.n + ' slices sent, rest kept'); return; }
    buyAllStart(rows);
  };
  wireResidGo(); wireReenter();
}
function renderResidual(){
  let res = []; try { res = (zbaDoc().residual || []).filter(r => r && r.sym && +r.qty > 0); } catch(e){}
  if (!res.length) return '';
  const B = BUYSLICER['__residual__'];
  return '<div class="bal" style="border:1px solid var(--buy)"><div class="bal-h"><b>Buy the remaining \u2014 exact quantities</b>' +
    '<span class="sub">' + res.length + ' stocks \u00b7 HFCL as delivery (CNC), the rest MTF</span>' +
    '<span class="go"><button class="btn on" id="residGo">' + (B ? 'Buying ' + B.i + '/' + B.n : 'Buy remaining') + '</button></span></div>' +
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
    clearTimeout(buyResidual._t); buyResidual._t = setTimeout(() => { g.dataset.arm = ''; g.textContent = 'Buy remaining'; }, 8000); return; }
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
/* Pre-buy leverage sweep (user 2026-09-01): /margincalc each entrant at its planned quantity.
   Leverage is MEASURED from the same response (order value \u00f7 blocked margin) \u2014 no assumed
   field. \u22641.05x = MTF refused for that scrip: the buy slicer's auto-CNC fallback will then
   need the FULL amount in cash, so see it BEFORE firing, not mid-basket. */
let LEV = null;   // last leverage sweep {at, n, blocked[]} — the rebalance wizard shows it
async function zbLevSweep(rows){
  const el = $('levOut');
  if (!Z.connected){ ktoast('Zerodha not connected'); return; }
  const flist = rows.filter(r => r.px > 0);
  if (!flist.length){ ktoast('Nothing to check \u2014 load picks first'); return; }
  if (el) el.innerHTML = '<div class="khelp">checking ' + flist.length + ' entrants\u2026</div>';
  const body = flist.map(r => ({ exchange: 'NSE', tradingsymbol: r.sym, product: 'MTF',
                                 quantity: Math.max(1, r.qty || (r.amt > 0 ? Math.floor(r.amt / r.px) : 0) || 1) }));
  const { st, j } = await zFetch('/margincalc', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  if (st !== 200 || !j || !j.data){ if (el) el.innerHTML = '<div class="khelp down">margin check failed: ' + esc((j && j.message) || ('HTTP ' + st)) + '</div>'; return; }
  let h = '', cash = 0, val = 0; const blocked = [];
  j.data.forEach((d, i) => { const r = flist[i]; if (!r) return;
    const v = body[i].quantity * r.px, tot = (d && d.total != null) ? +d.total : null;
    const lev = (tot && tot > 0) ? v / tot : null;
    val += v; cash += (tot || 0);
    const blk = lev != null && lev <= 1.05; if (blk) blocked.push(r.sym);
    h += '<tr><td><b>' + esc(r.sym) + '</b>' + (blk ? ' <span class="tag" style="background:color-mix(in srgb,var(--down) 16%,transparent);color:var(--down)">MTF blocked \u2014 full cash</span>' : '') + '</td>' +
         '<td>' + (lev != null ? lev.toFixed(2) + 'x' : '\u2014') + '</td><td>' + zinr(tot) + '</td><td>' + zinr(v) + '</td></tr>'; });
  LEV = { at: Date.now(), n: flist.length, blocked: blocked.slice() }; renderWizard();
  if (el) el.innerHTML = '<div class="bal"><div class="twrap"><table><thead><tr><th>Entrant</th><th>Leverage</th><th>Blocks \u20b9</th><th>Order \u20b9</th></tr></thead><tbody>' + h + '</tbody></table></div>' +
    '<div class="khelp">' + (blocked.length ? '\u26a0 <b>' + blocked.join(', ') + '</b>: MTF refused \u2014 the auto-CNC fallback will need the FULL amount in cash. ' : 'No MTF-blocked entrants. ') +
    'At these quantities: order \u2248 ' + zinr(val) + ' \u00b7 blocks \u2248 ' + zinr(cash) + ' of funds.</div></div>';
}
/* ================= EXIT-ALL + RE-ENTER (user 2026-09-15) =================
   A market-crash panic path, deliberately OUTSIDE the rebalance-window lock (a fall can hit any
   day). Exit-all sells EVERY holding in the connected account (live /holdings buckets: MTF as MTF,
   demat as CNC; pledged shares can't be sold here), sliced by ADV exactly like a sell basket, and
   SNAPSHOTS what it sold to the synced row (exitSnap). Re-enter reads that snapshot and buys the
   same stocks+quantities back, whenever the user chooses. Never window-locked; always two-tap. */
function exitSnapRows(){ try { const x = zbaDoc().exitSnap; return (x && Array.isArray(x.rows) ? x.rows : []).filter(r => r && r.sym && +r.qty > 0); } catch (e){ return []; } }
function exitSnapSet(rows){ const d = zbaDoc(); d.exitSnap = { ts: Date.now(), rows: rows }; d.ts = Date.now();
  try { localStorage.setItem(ZBA_LS, JSON.stringify(d)); } catch (e){}
  clearTimeout(zbaSet._t); zbaSet._t = setTimeout(zbaPush, 1200); }
function exitAllRows(){
  const out = [];
  for (const sym in (Z.hold || {})){ const b = Z.hold[sym]; const sell = (b.mtf || 0) + (b.cnc || 0);
    if (sell <= 0) continue; const q = liveQ(sym), px = (q && q.ltp != null) ? +q.ltp : 0;
    out.push({ sym: sym, mtf: b.mtf || 0, cnc: b.cnc || 0, coll: b.coll || 0, sell: sell, px: px, val: sell * px }); }
  return out.sort((a, b) => b.val - a.val);
}
function renderExitAll(){
  const rows = exitAllRows(), n = rows.length, est = rows.reduce((s, r) => s + (r.val || 0), 0);
  const B = BUYSLICER['__exitall__'];
  const note = !Z.connected ? 'Connect Zerodha (Positions &amp; funds) to arm this.'
    : n ? 'Account <b>' + esc(Z.user || '?') + '</b> · ' + n + ' holding' + (n === 1 ? '' : 's') + (est ? ' · ≈ ' + zinr(est) : '') + ' — sold in ADV slices, limit ≤' + sliceRng() + '% below live.'
        : 'No sellable holdings in this account right now.';
  const btn = B ? '<button class="btn sell" id="exitAllGo">Selling ' + B.i + '/' + B.n + '</button>'
    : (Z.connected && n ? '<button class="btn sell" id="exitAllGo">Exit all ' + n + '</button>' : '');
  return '<div class="bal" style="border:1px solid var(--down)"><div class="bal-h">' +
    '<b style="color:var(--down)">🚨 Exit everything</b><span class="sub">' + note + ' <b>Ignores the rebalance window.</b></span>' +
    '<span class="go">' + btn + '</span></div>' +
    '<div class="khelp">One tap arms, a second within 8s fires. Every holding sells at once (round-robin ADV slices), MTF as MTF + demat as CNC; pledged shares stay. What it sells is saved so you can re-enter the same book later from the Buy side. Tap the counter to stop.</div></div>';
}
function wireExitAll(){ const g = $('exitAllGo'); if (!g) return;
  g.onclick = () => { if (BUYSLICER['__exitall__']){ const S = BUYSLICER['__exitall__']; buyStop('__exitall__', 'Exit-all stopped — ' + S.i + '/' + S.n + ' slices sent, rest kept'); return; } exitAllStart(); }; }
async function exitAllStart(){
  if (!Z.connected){ ktoast('Zerodha not connected'); return; }
  await zHoldRefresh();                                   // sell the FRESH account, not a stale render
  const rows = exitAllRows();
  if (!rows.length){ ktoast('No sellable holdings in this account'); return; }
  const est = rows.reduce((s, r) => s + (r.val || 0), 0), g = $('exitAllGo');
  if (g && g.dataset.arm !== '1'){ g.dataset.arm = '1';
    g.textContent = 'CONFIRM sell ALL ' + rows.length + ' in ' + (Z.user || 'this account') + (est ? ' ≈ ' + zinr(est) : '') + ' ?';
    clearTimeout(exitAllStart._t); exitAllStart._t = setTimeout(() => { g.dataset.arm = ''; renderCards(); }, 8000); return; }
  if (g) g.dataset.arm = '';
  await loadTicks();
  const orders = [], snap = [], pledged = [];
  rows.forEach(r => {
    const base = { variety: 'regular', validity: 'DAY', tag: 'swexitall', tradingsymbol: r.sym, exchange: 'NSE', transaction_type: 'SELL', order_type: 'MARKET', _px: r.px };
    if (r.mtf > 0){ orders.push(Object.assign({}, base, { quantity: r.mtf, product: 'MTF' })); snap.push({ sym: r.sym, qty: r.mtf, product: 'MTF' }); }
    if (r.cnc > 0){ orders.push(Object.assign({}, base, { quantity: r.cnc, product: 'CNC' })); snap.push({ sym: r.sym, qty: r.cnc, product: 'CNC' }); }
    if (r.coll > 0) pledged.push(r.sym + ' (' + r.coll + ')');
  });
  if (!orders.length){ ktoast('Nothing sellable'); return; }
  exitSnapSet(snap);                                     // persist the book BEFORE firing, so re-entry survives a reload
  if (pledged.length) ktoast('⚠ pledged shares NOT sold (unpledge first): ' + pledged.join(', '), 8000);
  const slices = buySlices(orders);
  if (BUYSLICER['__exitall__']) buyStop('__exitall__');
  BUYSLICER['__exitall__'] = { slices: slices, i: 0, n: slices.length, btn: g || null, t: 0, sell: true };
  ktoast('EXIT ALL: selling ' + rows.length + ' holdings in ' + slices.length + ' slices, limit ≤' + sliceRng() + '% below live — keep this tab open; tap the counter to stop', 8000);
  buyFire('__exitall__');
  renderCards();
}
function renderReenter(){
  const rows = exitSnapRows(); if (!rows.length) return '';
  const B = BUYSLICER['__reenter__'];
  const bySym = {}; rows.forEach(r => { bySym[r.sym] = (bySym[r.sym] || 0) + Math.floor(+r.qty); });
  const syms = Object.keys(bySym).sort(), n = syms.length;
  const when = (function(){ try { const t = zbaDoc().exitSnap.ts; return t ? new Date(t).toLocaleDateString('en-GB', { day:'numeric', month:'short' }) : ''; } catch(e){ return ''; } })();
  const btn = B ? '<button class="btn on" id="reenterGo">Buying ' + B.i + '/' + B.n + '</button>'
    : '<button class="btn on" id="reenterGo">Re-enter ' + n + '</button>';
  return '<div class="bal" style="border:1px solid var(--buy)"><div class="bal-h">' +
    '<b>↩ Re-enter the exit basket</b><span class="sub">' + n + ' stock' + (n === 1 ? '' : 's') + ' you sold' + (when ? ' on ' + esc(when) : '') + ' — same quantities, bought back sliced. Ignores the rebalance window.</span>' +
    '<span class="go">' + btn + ' <button class="btn" id="reenterX" title="Forget this exit basket">✕</button></span></div>' +
    '<div class="twrap"><table><thead><tr><th>Stock</th><th>Qty to buy back</th></tr></thead><tbody>' +
    syms.map(x => '<tr><td><b>' + esc(x) + '</b></td><td>' + bySym[x].toLocaleString('en-IN') + '</td></tr>').join('') +
    '</tbody></table></div><div class="khelp">One tap arms, a second buys back exactly these — ADV slices, limit ≤' + sliceRng() + '% above live, same product as sold (MTF→MTF, demat→CNC; an MTF-blocked name auto-routes to CNC). ✕ forgets the basket.</div></div>';
}
function wireReenter(){ const g = $('reenterGo'), x = $('reenterX');
  if (x) x.onclick = () => { if (BUYSLICER['__reenter__']){ ktoast('Stop the re-entry first'); return; } exitSnapSet([]); ktoast('Exit basket forgotten'); renderCards(); };
  if (!g) return;
  g.onclick = () => { if (BUYSLICER['__reenter__']){ const S = BUYSLICER['__reenter__']; buyStop('__reenter__', 'Re-entry stopped — ' + S.i + '/' + S.n + ' slices sent, rest kept'); return; } reenterStart(); }; }
async function reenterStart(){
  const rows = exitSnapRows(); if (!rows.length){ ktoast('No saved exit basket'); return; }
  if (!Z.connected){ ktoast('Zerodha not connected'); return; }
  const g = $('reenterGo'), n = new Set(rows.map(r => r.sym)).size;
  if (g && g.dataset.arm !== '1'){ g.dataset.arm = '1'; g.textContent = 'CONFIRM buy back ' + n + ' ?';
    clearTimeout(reenterStart._t); reenterStart._t = setTimeout(() => { g.dataset.arm = ''; renderCards(); }, 8000); return; }
  if (g) g.dataset.arm = '';
  await loadTicks();
  const orders = rows.map(r => { const q = liveQ(r.sym); return { variety:'regular', validity:'DAY', tag:'swreenter',
    tradingsymbol: r.sym, exchange:'NSE', transaction_type:'BUY', order_type:'MARKET', quantity: Math.floor(+r.qty),
    product: (r.product || 'MTF'), _px: (q && q.ltp != null ? +q.ltp : 0) }; });
  const slices = buySlices(orders);
  if (BUYSLICER['__reenter__']) buyStop('__reenter__');
  BUYSLICER['__reenter__'] = { slices: slices, i: 0, n: slices.length, btn: g || null, t: 0 };
  ktoast('Re-entering ' + n + ' stocks in ' + slices.length + ' slices, limit ≤' + sliceRng() + '% above live — tap the counter to stop; ✕ forgets it after', 8000);
  buyFire('__reenter__');
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
  const cp = e.target.closest('[data-capture]'); if (cp){ captureProceeds(cp.dataset.capture); return; }
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
    if (!buyLegGuard()) return;
    if (kiteSend(o)){ zbSetBought(ZB.id, true, sentMap(o)); $('zbWrap').classList.remove('open'); } };
  $('zbGo').onclick = zbPlaceAll;
}
function zbOrders(){
  const type = ($('zbType') && $('zbType').value) || 'MARKET';
  return ZB.rows.filter(r => r.on && r.qty > 0).map(r => Object.assign({ variety:'regular', validity:'DAY', tag: (ZB.id && !/^__/.test(String(ZB.id))) ? buyTag(ZB.id) : 'swbasket',
    tradingsymbol: r.sym, exchange:'NSE', transaction_type:'BUY', order_type: type,
    quantity: r.qty, product: $('zbProd').value }, type === 'LIMIT' ? { price: (r.limit > 0 ? r.limit : r.px) } : {}));
}
function zbAlloc(){
  const amt = +$('zbAmt').value || 0, mode = ($('zbMode') && $('zbMode').value) || 'value';
  const on = ZB.rows.filter(r => r.on && r.px > 0 && !r.fixed), per = on.length ? amt / on.length : 0;
  ZB.rows.forEach(r => { r.st = ''; r.msg = '';
    if (r.fixed){ r.margin = null; r.qty = r.on ? (r.backQty || r.qty) : 0; return; }   // buy-back: fixed quantity, funded by its own sale
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
    tr.children[2].innerHTML = r.px ? '₹' + (+r.px).toFixed(2) + baCell(r.sym) : '—';
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
      '<td><b>' + esc(r.sym) + '</b>' + (r.back ? ' <span class="tag" style="background:color-mix(in srgb,#c98500 18%,transparent);color:#c98500" title="Sold on the month-end close but still in the official screen \u2014 same quantity back">buy back</span>' : r.kept ? ' <span class="tag keep">kept \u2014 riding, not re-bought</span>' : (Z.held.has(r.sym) ? ' <span class="tag keep">held</span>' : '')) + (r.live ? '' : ' <span class="badge">EOD</span>') + '</td>' +
      '<td>' + (r.px ? '₹' + (+r.px).toFixed(2) + baCell(r.sym) : '—') + '</td>' +
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
  $('zbTitle').textContent = 'Buy the basket' + (cloudOn() ? ' \u2601' : '');
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
    const RW = rebalWindow(), PR = proceedsOf(id);
    const px = h => { const q = liveQ(h.sym); return (q && q.ltp != null) ? +q.ltp : (h.avg || 0); };
    if (held.method === 'reset'){
      const all = PR ? PR.amt : held.rows.reduce((s, h) => s + h.qty * px(h), 0);
      if (all > 0) $('zbAmt').value = Math.round(all);
      $('zbSub').textContent += PR ? ' \u00b7 amount = actual sell proceeds captured on ' + RW.tlab + ' (' + zinr(PR.amt) + ').'
                                   : ' \u00b7 amount = the book\u2019s value at today\u2019s prices (no proceeds captured) \u2014 edit to your actual sell proceeds.';
      return;
    }
    const heldSet = new Set(held.rows.map(h => h.sym)), backs = rebuyRows(it, p);
    let stay = 0; ZB.rows.forEach(r => { const b = backs.find(x => x.sym === r.sym);
      if (b){ r.on = true; r.fixed = true; r.back = true; r.qty = b.qty; r.backQty = b.qty; }
      else if (heldSet.has(r.sym)){ r.on = false; r.kept = true; stay++; } });
    stay += backs.length;   // a buy-back is a stay whose shares are restored, not an open slot
    if (!stay && !PR) return;
    const pickSet = new Set(p.rows.map(r => r.sym));
    const backVal = backs.reduce((s, b) => s + ((PR && PR.val && PR.val[b.sym]) || 0), 0);
    const exitVal = PR ? Math.max(0, PR.amt - backVal) : held.rows.filter(h => !pickSet.has(h.sym)).reduce((s, h) => s + h.qty * px(h), 0);
    const openSlots = Math.max(1, (held.topN || p.rows.length) - stay);
    const entries = ZB.rows.filter(r => r.on && !r.fixed).length;
    if (exitVal > 0 && entries) $('zbAmt').value = Math.round(exitVal * entries / openSlots);
    $('zbSub').textContent += ' \u00b7 kept winners stay unticked' +
      (backs.length ? '; ' + backs.map(b => b.sym + ' \u00d7 ' + b.qty.toLocaleString('en-IN')).join(', ') + ' = buy back (sold on ' + RW.tlab + ', stayed in the final screen)' : '') +
      '; amount = ' + (PR ? 'actual sell proceeds captured on ' + RW.tlab + ' (' + zinr(exitVal) + ')' : 'the exits\u2019 value at today\u2019s prices (no proceeds captured \u2014 the exits were sold at the ' + RW.tlab + ' close)') + ' per open slot \u2014 edit if needed.';
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
           residual: (newer.residual !== undefined ? newer.residual : older.residual),
           boughtDay: (newer.boughtDay && (!older.boughtDay || String(newer.boughtDay.d) >= String(older.boughtDay.d))) ? newer.boughtDay : older.boughtDay,
           soldDay:   (newer.soldDay   && (!older.soldDay   || String(newer.soldDay.d)   >= String(older.soldDay.d)))   ? newer.soldDay   : older.soldDay,
           exitSnap:  (newer.exitSnap !== undefined ? newer.exitSnap : older.exitSnap),
           soldReb:   zbMergeReb(newer.soldReb, older.soldReb), boughtReb: zbMergeReb(newer.boughtReb, older.boughtReb),
           proceeds:  zbMergeReb(newer.proceeds, older.proceeds) }; }
function zbMergeReb(n, o){ return (n && (!o || String(n.k) >= String(o.k))) ? n : o; }

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
    const stratIds = new Set((doc.portfolios || []).filter(pf => pf.strategy && !pf.archived).map(pf => pf.id));
    const tot = {};
    (doc.holdings || []).forEach(h => { if (!stratIds.has(h.pf)) return;
      const b = String(h.sym || '').replace(/\.(NS|BO)$/, ''); tot[b] = (tot[b] || 0) + Math.floor(+h.qty || 0); });
    FEED.symTot = tot;
    FEED.ts = Date.now(); FEED.byKey = map;
    cloudTags();
    if (SIDE === 'sell') renderCards();
  } catch (e){}
  return FEED.byKey;
}
function heldFor(cfg){ try { return (FEED.byKey || {})[identityKey(cfg)] || null; } catch (e){ return null; } }
function zbSoldSet(){ return new Set(zbMarkDoc('soldReb').ids); }
function zbSetSold(id, on, sent){ zbMarkReb('soldReb', id, on, sent); }
function edgeRows(cfg, all){ return all.slice(0, (cfg.topN || 3) + 3).map((r, i) => ({ sym: r.sym, rank: i + 1, v: cfg.sortBy ? fieldVal(r, cfg.sortBy) : null })); }
/* Shares of `sym` that OTHER strategies still own: each one's ledger quantity minus what it already SENT
   to Zerodha this rebalance (its actual fills once captured). `it` may sell only what the demat holds
   BEYOND these — so a re-armed basket can never eat another strategy's shares (user 2026-09-23), a
   keeper is protected without needing its picks loaded, and an exit that never filled still shows as
   remaining. (Shares held outside every strategy book look like an unfilled exit; the persisted sold
   mark is what guards those — the button does not re-arm by itself.) */
function keeperQty(sym, it){
  const me = identityKey(it.cfg); let q = 0;
  uniqStrategies().forEach(o => { if (identityKey(o.cfg) === me) return;
    const hb = heldFor(o.cfg); if (!hb) return;
    const row = hb.rows.find(h => h.sym === sym); if (!row) return;
    const PR = proceedsOf(o.id), sent = zbSentSyms('soldReb', o.id);
    const gone = (PR && PR.filled && PR.filled[sym] != null) ? +PR.filled[sym] : (+sent[sym] || 0);
    q += Math.max(0, row.qty - gone); });
  return q;
}
function dematQty(sym){ const b = Z.hold[sym]; return b ? (b.mtf + b.cnc) : 0; }   // pledged shares excluded: not sellable
/* Borderline for a HELD stock on the sell day (mirror of borderMap, user 2026-09-23): a STAY at the cut
   can drop out by the close, an EXIT just outside (rank N+1 on a price-sensitive sort, or failing a
   price-sensitive filter by a whisker) can climb back in. Sell the clear exits first, these last. */
function heldBorder(it, p, h, stays, fv){
  const cfg = it.cfg, N = cfg.topN || 3, band = PS_BAND[cfg.sortBy];
  if (stays){ const r = p.rows.find(x => x.sym === h.sym); return (r && r.bd) || null; }
  const notes = [], e = p.edge || [], me = e.find(x => x.sym === h.sym), nth = e.find(x => x.rank === N);
  if (me && nth && band != null && me.rank === N + 1 && me.v != null && nth.v != null && Math.abs(me.v - nth.v) <= band)
    notes.push('rank #' + (N + 1) + ' vs #' + N + ': ' + cfg.sortBy + ' gap ' + Math.abs(me.v - nth.v).toFixed(1) + ' \u2014 could re-enter by the close');
  (cfg.filters || []).forEach(f => { if (!(f.field in PS_BAND) || !fv) return; const v = fv[f.field];
    if (v != null && !passOp(v, f.op, f.val) && Math.abs(v - f.val) <= PS_BAND[f.field])
      notes.push('fails ' + f.field + ' ' + f.op + ' ' + f.val + ' by ' + Math.abs(v - f.val).toFixed(1) + ' \u2014 could pass by the close'); });
  return notes.length ? notes.join(' \u00b7 ') : null;
}
/* Per-strategy order tag (Kite: alphanumeric, ≤20 chars) so the day's SELL fills can be attributed back
   to the strategy that sold them — the actual proceeds then fund that strategy's T+1 buys (Option A). */
function sellTag(id){ return ('ss' + String(id).replace(/[^a-zA-Z0-9]/g, '')).slice(0, 20); }
function buyTag(id){ return ('sb' + String(id).replace(/[^a-zA-Z0-9]/g, '')).slice(0, 20); }   // per-strategy BUY tag (ledger in the cloud, 2026-09-24): the box attributes each fill to its strategy exactly
function proceedsOf(id){ try { const P = zbaDoc().proceeds; return (P && P.k === zbRebKey() && P.by && P.by[id]) ? P.by[id] : null; } catch(e){ return null; } }
async function captureProceeds(id, quiet){
  if (!Z.connected && !cloudOn()) return;
  const tag = sellTag(id); let amt = 0, n = 0, src = 'order book'; const filled = {}, val = {};
  if (Z.connected){ const ob = await zFetch('/orders');
    if (ob.st === 200 && ob.j && ob.j.data) ob.j.data.forEach(o => { if (o.transaction_type !== 'SELL' || o.tag !== tag) return; const fq = +o.filled_quantity || 0; if (fq <= 0) return;
      const v = fq * (+o.average_price || 0); amt += v; n++; filled[o.tradingsymbol] = (filled[o.tradingsymbol] || 0) + fq; val[o.tradingsymbol] = (val[o.tradingsymbol] || 0) + v; }); }
  if (!n){ const c = await proceedsFromCloud(id);   // v4: yesterday's cloud basket still knows its fills (Kite's order book is today-only)
    if (!c){ if (!quiet) ktoast('No SELL fills found for this strategy yet \u2014 nothing captured', 5000); return; }
    amt = c.amt; n = c.n; Object.assign(filled, c.filled); Object.assign(val, c.val); src = 'cloud basket fills'; }
  try { const d = zbaDoc(); const P = (d.proceeds && d.proceeds.k === zbRebKey()) ? d.proceeds : { k: zbRebKey(), by: {} };
    P.by[id] = { amt: Math.round(amt), n: n, filled: filled, val: val, at: Date.now() }; d.proceeds = P; d.ts = Date.now();
    localStorage.setItem(ZBA_LS, JSON.stringify(d)); clearTimeout(zbaSet._t); zbaSet._t = setTimeout(zbaPush, 1200); } catch(e){}
  if (!quiet) ktoast('Captured ' + zinr(amt) + ' of actual sell proceeds (' + n + ' fill' + (n === 1 ? '' : 's') + ', ' + src + ') \u2014 this funds the strategy\u2019s ' + rebalWindow().t1lab + ' buys', 6500);
  renderCards();
}
/* the strategy's finished SELL cloud jobs of this rebalance (from the session before T on) -> per-order fills */
async function proceedsFromCloud(id){
  const t0 = Date.parse(zbRebKey() + 'T00:00:00+05:30') - 4 * 86400e3;
  const jobs = (CLOUD.jobs || []).filter(j => jobSid(j.id) === id && j.side === 'SELL' && j.status !== 'running' && j.fill && j.fill.filledQty > 0 && (j.created || 0) >= t0);
  if (!jobs.length) return null;
  let amt = 0, n = 0; const filled = {}, val = {};
  for (const j of jobs){ const r = await zFetch('/jobs/' + encodeURIComponent(j.id)); const sent = (r.j && r.j.job && r.j.job.sent) || [];
    sent.forEach(o => { const fq = +o.filled || 0; if (fq <= 0) return; const v = fq * (+o.avg || 0); amt += v; n++; filled[o.sym] = (filled[o.sym] || 0) + fq; val[o.sym] = (val[o.sym] || 0) + v; }); }
  return n ? { amt: amt, n: n, filled: filled, val: val } : null;
}
function sellRuntime(exitRows){
  let mx = 0, tot = 0;
  exitRows.forEach(r => { const px = (r.px != null ? r.px : (r.h.avg || 0)), q = r.h.qty || 0;
    if (!(px > 0) || !(q > 0)) return;
    const chunk = Math.min(80000, Math.max(1, Math.floor(advSliceCap(r.h.sym) / px)));
    const n = Math.ceil(q / chunk); mx = Math.max(mx, n); tot += n; });
  if (!tot) return null;
  const mins = Math.max(1, Math.round(((mx - 1) * sliceGap() + tot * 3) / 60));
  const t = 15 * 60 + 28 - mins;
  return { tot: tot, mins: mins, startBy: (t > 9 * 60 && mins > 3) ? (Math.floor(t / 60) + ':' + String(t % 60).padStart(2, '0')) : null };
}
function livePicksOk(p){ return !!(p && p.live && p.liveTs && Date.now() - p.liveTs < 180000); }
/* Rebalance legs (user 2026-09-23, replaces the T-1 / T convention): SELL the exits near the
   close of T = the month-end session, on that day's near-final ⚡ live picks (the T-close screen
   only bakes ~20:45 IST). BUY the entries the NEXT session (T+1) in the morning, on the official
   T-close screen (Rebalance picks). The buy leg stays armed two more weekdays as a buffer (no NSE
   holiday calendar here — a holiday on T+1 must not strand the buys); sells on those days are
   STRAGGLERS only (held stocks that dropped out of the official screen but were not sold on T).
   Any other day the view is informational. Sessions skip weekends AND NSE trading holidays
   (docs/nse_holidays.json — the exchange's holiday master, one list per year, refreshed each
   December; a year with no list falls back to weekends only and the wizard says so). The window
   is keyed by T (tIso): the sold / bought marks live for the whole window, not a calendar day. */
const HOL = { set: new Set(), years: [], loaded: false };
async function loadHolidays(){
  try { const r = await fetch('./nse_holidays.json', { cache: 'no-store' }); const d = await r.json();
    HOL.set = new Set(Object.values(d.holidays || {}).flatMap(y => Object.keys(y))); HOL.years = (d.years || Object.keys(d.holidays || {})).map(String); HOL.loaded = true; }
  catch(e){ HOL.loaded = false; }
  renderCards();
}
const isoDay = d => d.toISOString().slice(0, 10);
const isOff = d => d.getUTCDay() % 6 === 0 || HOL.set.has(isoDay(d));      // weekend or NSE holiday
function rebalWindow(){
  const now = new Date(Date.now() + 330 * 60000);
  const y = now.getUTCFullYear(), m = now.getUTCMonth();
  const wk = isOff;
  const lastTD = (yy, mm) => { let t = new Date(Date.UTC(yy, mm + 1, 0)); while (wk(t)) t = new Date(t.getTime() - 864e5); return t; };
  const nextTD = d => { let t = new Date(d.getTime() + 864e5); while (wk(t)) t = new Date(t.getTime() + 864e5); return t; };
  const d0 = Date.UTC(y, m, now.getUTCDate());
  const legs = t => { const t1 = nextTD(t), t2 = nextTD(t1), t3 = nextTD(t2); return { t: t, t1: t1, t3: t3 }; };
  let L = legs(lastTD(y, m - 1));                                        // last month's buy leg can spill into this month
  if (!(d0 >= +L.t && d0 <= +L.t3)) L = legs(lastTD(y, m));
  const lab = d => d.toLocaleDateString('en-GB', { day: 'numeric', month: 'short', timeZone: 'UTC' });
  const iso = d => d.toISOString().slice(0, 10);
  const sellIn = d0 === +L.t, buyIn = d0 >= +L.t1 && d0 <= +L.t3;
  const yrs = [...new Set([iso(L.t).slice(0, 4), iso(L.t3).slice(0, 4)])], calMissing = yrs.filter(yy => !HOL.years.includes(yy));
  return { in: sellIn || buyIn, sellIn: sellIn, buyIn: buyIn, planned: d0 === +L.t1,
           tIso: iso(L.t), t1Iso: iso(L.t1), tlab: lab(L.t), t1lab: lab(L.t1), t3lab: lab(L.t3),
           calYears: HOL.years.slice(), calMissing: calMissing, calLoaded: HOL.loaded };
}
/* exit rows for one strategy: the card's table AND the sell-side summary read this */
function sellExits(it){
  const RW = rebalWindow();
  const held = heldFor(it.cfg), p = PICKS[it.id];
  const isReset = !!(held && held.method === 'reset');
  const pickSet = (p && p.rows.length) ? new Set(p.rows.map(r => r.sym)) : null;
  const cols = pickCols(it.cfg), fmap = (held && held.rows.length) ? factorMap(it) : {};
  const sent = zbSentSyms('soldReb', it.id);
  /* the screen each leg trusts: T = today's live picks (the T-close screen bakes only this evening);
     T+1.. = the OFFICIAL T-close screen (rebalance mode, dated exactly T) — stragglers are judged on it */
  let legOk = true, legMsg = '';
  if (!isReset && p && RW.sellIn && marketOpen() && !livePicksOk(p)){ legOk = false;
    legMsg = p.live ? 'Live picks are stale \u2014 refresh them' : 'Switch to \u26a1 Live picks \u2014 the ' + RW.tlab + ' close screen bakes only this evening'; }
  if (!isReset && p && RW.buyIn && (p.live || p.asOf !== RW.tIso)){ legOk = false;
    legMsg = p.live ? 'Switch to Rebalance picks \u2014 stragglers are judged on the official ' + RW.tlab + ' close screen' : 'Waiting for the ' + RW.tlab + ' close in the data (picks are as of ' + p.asOf + ') \u2014 refresh picks'; }
  const rows = (held && held.rows.length) ? held.rows.map(h => {
      const q = liveQ(h.sym), px = (q && q.ltp != null) ? +q.ltp : null;
      const zh = Z.connected ? Z.hold[h.sym] : null;
      const zt = zh ? (zh.mtf + zh.cnc + (zh.coll || 0)) : null;
      const lt = (FEED.symTot || {})[h.sym];
      const mism = (zt != null && lt != null && zt !== lt) ? (zt - lt) : null;   // demat vs strategy-ledger, both ways
      const stays = !isReset && !!(pickSet && pickSet.has(h.sym));
      const fv = pickFV(fmap[h.sym], cols);
      const keep = Z.connected ? keeperQty(h.sym, it) : null;
      const remain = (Z.connected && !stays) ? Math.min(h.qty, Math.max(0, dematQty(h.sym) - keep)) : null;   // shares still to sell
      const bd = (!isReset && p && p.live && RW.sellIn) ? heldBorder(it, p, h, stays, fv) : null;
      return { h: h, px: px, mism: mism, stays: stays, val: px != null ? h.qty * px : null, fv: fv, keep: keep, remain: remain, sent: +sent[h.sym] || 0, bd: bd };
    }) : [];
  const exits = rows.filter(r => !r.stays);
  return { held, p, isReset, pickSet, cols, rows, exits, est: exits.reduce((s, r) => s + (r.val || 0), 0), known: !!(pickSet || isReset), legOk, legMsg, RW };
}
function renderSellAll(list){
  let nEx = 0, est = 0, nKnown = 0, nUnknown = 0, nBook = 0;
  list.forEach(it => { const x = sellExits(it); if (!x.held || !x.rows.length) return; nBook++;
    if (x.known){ nKnown++; nEx += x.exits.length; est += x.est; } else nUnknown++; });
  return '<div class="bal"><div class="bal-h"><b>This rebalance · ' + nEx + ' exit' + (nEx === 1 ? '' : 's') + ' to sell</b>' +
    '<span class="sub">' + (nBook ? ('across ' + nKnown + ' of ' + nBook + ' strateg' + (nBook === 1 ? 'y' : 'ies') + ' with a book' + (est ? ' · ≈ ' + zinr(est) : '') +
      (nUnknown ? ' · ' + nUnknown + ' need picks loaded' : '')) : 'no strategy book in this browser yet') + ' · ' + esc(spTotLbl(list)) + '</span></div></div>';
}
function sellCardHTML(it, disp, favNum){
  const X = sellExits(it), RW = X.RW, held = X.held, p = X.p, isReset = X.isReset, pickSet = X.pickSet;
  const isSold = zbSoldSet().has(it.id), PR = proceedsOf(it.id);
  let body = '', btn = '';
  if (!held) body = '<div class="khelp">No holdings feed in this browser yet (needs the pf token) \u2014 or this strategy has no live book.</div>';
  else if (!held.rows.length) body = '<div class="khelp">Nothing held under this strategy.</div>';
  else {
    const rows = X.rows, exits = X.exits, est = X.est;
    const todo = exits.filter(r => r.remain == null ? true : r.remain > 0);      // shares still to sell (demat-aware)
    const rt = (pickSet || isReset) && todo.length ? sellRuntime(todo.map(r => ({ h: { sym: r.h.sym, qty: (r.remain != null ? r.remain : r.h.qty), avg: r.h.avg }, px: r.px }))) : null;
    const rtTxt = rt ? ' \u00b7 \u2248 ' + rt.tot + ' slice' + (rt.tot === 1 ? '' : 's') + ', ~' + rt.mins + ' min at ' + sliceGap() + 's gap' + (rt.startBy ? ' \u2014 start by ' + rt.startBy + ' for a 3:28 finish' : '') : '';
    body = '<div class="twrap"><table><thead><tr><th>Stock</th><th>Held</th><th>Live \u20b9</th><th>Value</th>' + pickColHead(X.cols) + '<th></th></tr></thead><tbody>' +
      rows.map(r => '<tr' + (r.stays ? ' style="opacity:.45"' : '') + '><td><b>' + esc(r.h.sym) + '</b>' +
        (r.bd ? ' <span class="tag" style="background:color-mix(in srgb,#c98500 18%,transparent);color:#c98500" title="' + esc(r.bd) + '">borderline</span>' : '') + '</td>' +
        '<td>' + r.h.qty.toLocaleString('en-IN') + (r.sent ? ' <span class="sym" title="sent to Zerodha this rebalance">sent ' + r.sent.toLocaleString('en-IN') + '</span>' : '') + '</td>' +
        '<td>' + (r.px != null ? '\u20b9' + r.px.toFixed(2) + baCell(r.h.sym) : '\u2014') + '</td>' +
        '<td>' + (r.val != null ? zinr(r.val) : '\u2014') + '</td>' + pickColCells(X.cols, r, true) +
        '<td>' + (r.mism != null ? '<span class="tag" style="background:color-mix(in srgb,#c98500 18%,transparent);color:#c98500" title="Zerodha demat holds ' + (r.h.qty + r.mism) + ' vs ' + r.h.qty + ' in the strategy ledger \u2014 sold already, or bonus/split/rename? The sell quantity is capped at what the demat holds beyond the keeping strategies.">demat ' + (r.mism > 0 ? '+' : '') + r.mism + '</span> ' : '') +
        (r.stays ? '<span class="tag keep">stays \u2014 not sold</span>'
                : (pickSet || isReset
                    ? (r.remain === 0 ? '<span class="tag keep" title="Zerodha holds none of these beyond what the keeping strategies own">sold \u2713</span>'
                       : '<span class="tag" style="background:color-mix(in srgb,var(--down) 16%,transparent);color:var(--down)">' + (isReset ? 'reset \u2014 sell' : (RW.buyIn ? 'STRAGGLER \u2014 sell' : 'EXIT \u2014 sell')) + (r.remain != null && r.remain < r.h.qty ? ' ' + r.remain.toLocaleString('en-IN') : '') + '</span>')
                    : '<span class="badge">load picks</span>')) + '</td></tr>').join('') +
      '</tbody></table></div>' +
      '<div class="khelp">' + (isReset
        ? 'Reset strategy: the whole basket sells every rebalance and re-enters fresh \u2014 even a stock picked again. Sells near the ' + RW.tlab + ' close; re-enters the morning of ' + RW.t1lab + '.' + rtTxt
        : (pickSet ? todo.length + ' ' + (RW.buyIn ? 'straggler' : 'exit') + (todo.length === 1 ? '' : 's') + ' to sell' + (est && !RW.buyIn ? ' \u2248 ' + zinr(est) : '') + rtTxt + ' \u00b7 greyed rows stay for next month and are never sold.'
                   : 'Load the picks (\ud83c\udfaf) first \u2014 without them the exits are unknown, so nothing can be sold.')) +
      (rows.some(r => r.bd) ? '<br>\u26a0 <b>borderline</b> = could still flip by the close (hover for the numbers) \u2014 sell the clear exits first, these last (~3:25 IST). A borderline stay you keep shows up on ' + RW.t1lab + ' as a straggler if it drops out.' : '') +
      (PR ? '<br>\u2713 Actual sell proceeds captured: <b>' + zinr(PR.amt) + '</b> (' + PR.n + ' fill' + (PR.n === 1 ? '' : 's') + ', ' + new Date(PR.at).toTimeString().slice(0, 5) + ') \u2014 funds the ' + RW.t1lab + ' buys.' : (isSold && RW.sellIn ? '<br>Proceeds are captured from the order book ~2 min after the basket finishes; tap \u21bb proceeds to redo it (before midnight \u2014 Kite forgets yesterday\u2019s orders).' : '')) +
      (!X.legOk && pickSet ? '<br>\u26a0 <b>' + esc(X.legMsg) + '</b> \u2014 selling is locked until then.' : '') + '</div>';
    const B = BUYSLICER[it.id];
    if (B && B.sell) btn = '<button class="btn sell" data-sellbasket="' + esc(it.id) + '">Selling ' + B.i + '/' + B.n + '</button>';
    else if ((pickSet || isReset) && exits.length && !RW.in) btn = '<span class="tag off" title="Sell baskets act only on the rebalance window \u2014 exits near the ' + esc(RW.tlab) + ' close (month-end), stragglers from ' + esc(RW.t1lab) + '. Until then this list is informational.">Locked \u00b7 arms ' + esc(RW.tlab) + '</span>';
    else if ((pickSet || isReset) && exits.length && !X.legOk) btn = '<span class="tag warn" title="' + esc(X.legMsg) + '">' + (RW.buyIn ? 'Rebalance picks required' : 'Live picks required') + '</span>';
    else if (isSold && !todo.length) btn = '<button class="btn" disabled style="opacity:.5;cursor:not-allowed;color:var(--down)" title="Sold this rebalance \u2014 click the \u2713 sold chip to re-enable">\u2713 Sold</button>';
    else if ((pickSet || isReset) && todo.length) btn = '<button class="btn sell" data-sellbasket="' + esc(it.id) + '">Sell ' + (isSold || RW.buyIn ? 'remaining ' : (isReset ? 'all ' : '')) + todo.length + (isReset ? '' : (todo.length === 1 ? ' exit' : ' exits')) + '</button>';
  }
  return '<div class="sblk"><div class="shead">' + (favNum(it.cfg) ? '<span class="snum" style="font-size:11px;background:var(--down);color:#fff;border-color:var(--down);padding:1px 6px;margin:0 4px 0 0">#' + favNum(it.cfg) + '</span>' : '') +
    '<span class="nm2" title="Code-name: ' + esc(nameWithBasis(it.name, it.cfg)) + '">' + esc(disp) + '</span>' +
    (isSold ? '<span class="tag keep" data-unsold="' + esc(it.id) + '" title="Sell basket sent this rebalance (' + esc(RW.tlab) + ') \u2014 click if that was cancelled" style="cursor:pointer">\u2713 sold</span>' : '') +
    (isSold && Z.connected && RW.sellIn ? '<span class="tag" data-capture="' + esc(it.id) + '" title="Re-read today\u2019s order book for this strategy\u2019s SELL fills" style="cursor:pointer">\u21bb proceeds</span>' : '') +
    '<span class="sym">' + (held ? esc(held.pfId + ' \u00b7 ' + held.method) : '') + (p ? ' \u00b7 picks as of ' + esc(p.asOf) + (p.live ? ' + live' : '') : '') + '</span>' +
    '<span style="margin-left:auto;display:flex;gap:6px">' +
    '<button class="btn" data-load="' + esc(it.id) + '">' + (p ? 'Refresh picks' : 'Picks') + '</button>' + btn + '</span></div>' + body + '</div>';
}
async function sellBasketStart(id){
  const it = strategies().find(x => x.id === id); if (!it) return;
  const X = sellExits(it), held = X.held, isReset = X.isReset, p = X.p, RW = X.RW;
  if (!held || !held.rows.length){ ktoast('No holdings on record for this strategy'); return; }
  if (!isReset && (!p || !p.rows.length)){ ktoast('Load the picks first \u2014 exits are unknown without them'); return; }
  if (!RW.in){ ktoast('\ud83d\udd12 Sell baskets act only on the rebalance window \u2014 exits near the ' + RW.tlab + ' close (month-end), stragglers from ' + RW.t1lab + '. Nothing sent.', 7500); return; }
  if (!X.legOk){ ktoast('\u26a0 ' + X.legMsg + ' \u2014 selling is locked until then', 7500); return; }
  if (!Z.connected){ ktoast('Zerodha not connected'); return; }
  const btn = document.querySelector('[data-sellbasket="' + id + '"]');
  if (btn && btn.dataset.arm !== '1'){
    const todo0 = X.exits.filter(r => r.remain == null ? true : r.remain > 0);
    if (!todo0.length){ ktoast(X.exits.length ? 'Nothing left to sell \u2014 Zerodha holds none of these exits beyond what the keeping strategies own' : 'Nothing to sell \u2014 every holding stays next month'); return; }
    btn.dataset.arm = '1';
    btn.textContent = 'Confirm SELL ' + todo0.length + (isReset ? ' (reset: all)' : ' exit' + (todo0.length === 1 ? '' : 's')) + ' ?';
    clearTimeout(sellBasketStart._t); sellBasketStart._t = setTimeout(() => { btn.dataset.arm = ''; renderCards(); }, 8000); return; }
  if (btn) btn.dataset.arm = '';
  await loadTicks();
  await zHoldRefresh();                 // fresh per-product buckets right before selling
  const todo = sellExits(it).exits.filter(r => r.remain > 0);   // remaining quantities off the fresh demat
  if (!todo.length){ ktoast('Nothing left to sell \u2014 Zerodha holds none of these exits beyond what the keeping strategies own', 6000); renderCards(); return; }
  const orders = [], short = [], sent = {};
  todo.forEach(r => {
    const h = r.h, px = (r.px != null ? r.px : (h.avg || 0));
    const bk = Z.hold[h.sym] || { mtf: 0, cnc: 0 };
    const mq = Math.min(r.remain, bk.mtf), cq = Math.min(r.remain - mq, bk.cnc);   // MTF position first, then demat (CNC)
    if (r.remain < h.qty) short.push(h.sym + ' (' + (h.qty - r.remain) + (r.keep ? ' kept by other strategies' : ' not in demat') + ')');
    const base = { variety: 'regular', validity: 'DAY', tag: sellTag(id), tradingsymbol: h.sym,
                   exchange: 'NSE', transaction_type: 'SELL', order_type: 'MARKET', _px: px };
    if (mq > 0) orders.push(Object.assign({}, base, { quantity: mq, product: 'MTF' }));
    if (cq > 0) orders.push(Object.assign({}, base, { quantity: cq, product: 'CNC' }));
    if (mq + cq > 0) sent[h.sym] = mq + cq;
  });
  if (!orders.length){ ktoast('Zerodha shows no sellable shares for these exits \u2014 nothing sent', 6000); return; }
  if (short.length) ktoast('\u26a0 selling fewer shares than the ledger for ' + short.join(', '), 7000);
  const slices = buySlices(orders);
  if (BUYSLICER[id]) buyStop(id);
  BUYSLICER[id] = { slices: slices, i: 0, n: slices.length, btn: btn || null, t: 0, sell: true };
  zbSetSold(id, true, sent);
  ktoast('Selling ' + todo.length + ' stock' + (todo.length === 1 ? '' : 's') + ' in ' + slices.length +
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
/* ================= CLOUD SLICER (user 2026-09-23: "execution that survives the tab") =================
   The slicer can run on the static-IP relay VM (kite-relay.js v2) instead of this tab: the tab submits
   the whole sliced basket ONCE (POST /jobs through the worker), the VM fires the slices on its own clock
   with the same rules (fresh LTP, limit ≤rng% off on the tick grid, tick / MTF→CNC retries, skip-and-
   continue), and EVERY device sees the counter and can Stop (GET /jobs, POST /jobs/<id>/stop). Falls back
   to in-tab slicing when the worker or VM is not upgraded (GET /jobs → 404/503) or the ☁ toggle is off.
   A job id carries the strategy id (slug~stamp) so any device maps it back to its card. The same
   per-strategy sell tags travel with the slices, so proceeds capture is unchanged. */
const CLOUD = { ok: null, at: 0, seen: {}, timer: null };
const cloudWanted = () => { try { return localStorage.getItem('sw_cloud_slicer') !== '0'; } catch(e){ return true; } };
const cloudOn = () => !!CLOUD.ok && cloudWanted();
async function cloudProbe(force){
  if (!force && CLOUD.at && Date.now() - CLOUD.at < 600000) return CLOUD.ok;
  const r = await zFetch('/jobs');
  CLOUD.ok = !!(r.st === 200 && r.j && r.j.ok && r.j.v >= 2); CLOUD.at = Date.now();
  if (CLOUD.ok){ cloudApply(r.j.jobs || []); cloudTags(); ledgerStatus(); }
  cloudChip(); return CLOUD.ok;
}
const jobSlug = s => String(s).replace(/[^\w.:-]/g, '-').slice(0, 60);
function jobSid(jobId){ const pre = String(jobId).split('~')[0];
  if (/^__/.test(pre)) return pre;
  const it = strategies().find(x => jobSlug(x.id) === pre); return it ? it.id : pre; }
async function cloudSubmit(id){
  const B = BUYSLICER[id]; if (!B || !B.slices || !B.slices.length) return;
  const it = strategies().find(x => x.id === id);
  const label = it ? ((typeof strategyEnglish === 'function' && strategyEnglish(it.cfg)) || it.name || id) : ({ __exitall__: 'Exit all', __reenter__: 'Re-enter', __residual__: 'Buy remaining', __all__: 'Buy all' }[id] || id);
  const jobId = jobSlug(id) + '~' + Date.now().toString(36);
  const body = { id: jobId, label: String(label).slice(0, 80), device: ((navigator.platform || '') + ' ' + new Date().toTimeString().slice(0, 5)).slice(0, 40),
    pfId: ((typeof heldFor === 'function' && it && heldFor(it.cfg)) || {}).pfId || '', sid: String(id),   // v4: the box books this basket's fills to that portfolio
    gapS: sliceGap(), rngPct: sliceRng(), peg: 'touch', partPct: partPct(),
    slices: B.slices.map(s => ({ tradingsymbol: s.tradingsymbol, transaction_type: s.transaction_type, quantity: s.quantity, product: s.product,
      tag: s.tag, px: +s._px || 0, tick: TICKMEM[s.tradingsymbol] || 0.05, round: s._round })) };
  let r = null;
  for (let k = 0; k < 2 && !(r && r.st === 200); k++)
    r = await zFetch('/jobs', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });   // same id twice = idempotent
  if (!(r && r.st === 200 && r.j && r.j.ok)){
    const why = (r && r.j && (r.j.error || r.j.message)) || ('HTTP ' + (r && r.st));
    if (r && (r.st === 404 || r.st === 502 || r.st === 503 || r.st === 0)){ CLOUD.ok = false; cloudChip(); }
    delete BUYSLICER[id]; renderCards();
    ktoast('☁ Cloud slicer refused (' + why + ') — NOTHING was sent. Tap again to ' + (CLOUD.ok ? 'retry' : 'slice in this tab instead') + '.', 9000); return; }
  Object.assign(B, { remote: true, jobId: jobId, i: r.j.job.i, n: r.j.job.n, slices: [] });
  CLOUD.seen[jobId] = 'running';
  ktoast('☁ Sent to the cloud slicer — ' + B.n + ' slices keep firing even if this tab closes; limits pegged to the live bid/ask' + (partPct() ? ', each slice ≤ ' + partPct() + '% of the last 5 min’s volume' : '') + '; any device can stop it', 8000);
  cloudLoop(true); renderCards();
}
function cloudApply(jobs){
  CLOUD.jobs = jobs;                                                                        // the fills panel reads these
  let changed = false;
  jobs.forEach(j => {
    const sid = jobSid(j.id), was = CLOUD.seen[j.id];
    if (j.status === 'running'){
      const B = BUYSLICER[sid];
      if (B && !B.remote) return;                                                           // this tab is slicing that one locally
      if (!B || B.jobId !== j.id){ BUYSLICER[sid] = { remote: true, jobId: j.id, i: j.i, n: j.n, sell: j.side === 'SELL', btn: null, t: 0, slices: [] }; changed = true; }
      else if (B.i !== j.i || B.n !== j.n){ B.i = j.i; B.n = j.n; changed = true; }
    } else if (was === 'running'){
      const B = BUYSLICER[sid]; if (B && B.jobId === j.id){ delete BUYSLICER[sid]; changed = true; }
      ktoast('☁ ' + (j.side === 'SELL' ? 'Sell' : 'Buy') + ' basket ' + j.status + ' — ' + j.i + '/' + j.n + ' slices sent' +
        (j.failed && j.failed.length ? ' · FAILED (' + (j.side === 'SELL' ? 'sell' : 'buy') + ' separately): ' + j.failed.join(', ') : ''), 8000);
      if (j.side === 'SELL' && !/^__/.test(sid) && j.status === 'done') setTimeout(() => captureProceeds(sid), 120000);   // actual proceeds fund the T+1 buys
    }
    CLOUD.seen[j.id] = j.status;
  });
  if (changed) renderCards();
}
function cloudLoop(now){
  clearTimeout(CLOUD.timer);
  const anyRemote = Object.values(BUYSLICER).some(B => B.remote);
  const tick = async () => {
    if (!document.hidden && cloudWanted() && zWorker() && zToken()){
      const r = await zFetch('/jobs');
      if (r.st === 200 && r.j && r.j.ok){ if (!CLOUD.ok){ CLOUD.ok = true; cloudChip(); } cloudApply(r.j.jobs || []); }
    }
    cloudLoop();
  };
  CLOUD.timer = setTimeout(tick, now ? 800 : (anyRemote ? 5000 : 30000));
}
function cloudChip(){ const b = $('spCloud'); if (!b) return;
  b.textContent = '☁ ' + (CLOUD.ok === null ? 'cloud?' : !CLOUD.ok ? 'cloud n/a' : cloudWanted() ? 'cloud on' : 'cloud off');
  b.classList.toggle('on', cloudOn());
  b.title = CLOUD.ok === false ? 'Cloud slicer not reachable (worker / Oracle box not upgraded?) — baskets slice in this tab'
          : cloudWanted() ? 'Baskets run on the Oracle box and keep going if this tab closes — tap to switch to in-tab slicing'
          : 'In-tab slicing — tap to run baskets on the Oracle box'; }
/* ================= LEDGER IN THE CLOUD (user "go" 2026-09-24, world-class #6) =================
   The strategy books' master copy is the cloud holdings row; the Oracle box (kite-relay.js v4 +
   ledger_core.js) reconciles it from the fills after each rebalance. This tab: (1) tells the box which
   order tag belongs to which ledger portfolio, (2) shows the box's fills-capture status in the wizard,
   (3) opens the reconcile PLAN from the box and applies it only on a second tap. The Mac pulls the result
   at its next Publish; nothing here writes holdings.json. */
function cloudTags(){
  if (!cloudOn() || !FEED.byKey || Date.now() - (CLOUD.tagsAt || 0) < 6 * 3600e3) return;
  const tags = {};
  uniqStrategies().forEach(it => { const h = heldFor(it.cfg); if (h && h.pfId){ tags[sellTag(it.id)] = h.pfId; tags[buyTag(it.id)] = h.pfId; } });
  if (!Object.keys(tags).length) return;
  CLOUD.tagsAt = Date.now();
  zFetch('/ledger/tags', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ tags: tags }) }).catch(() => {});
}
async function ledgerStatus(force){
  if (!cloudWanted() || !zWorker() || !zToken()) return;
  if (!force && (LST.busy || Date.now() - LST.at < 300000)) return;
  LST.busy = true;
  try { const r = await zFetch('/ledger/status'); if (r.st === 200 && r.j && r.j.ok){ LST.j = r.j; LST.at = Date.now(); renderWizard(); } }
  catch(e){} finally { LST.busy = false; }
}
async function ledgerCapture(){
  const r = await zFetch('/ledger/capture', { method: 'POST' });
  if (r.st === 200 && r.j && r.j.ok){ ktoast('\u2713 Order book captured on the box: ' + r.j.orders + ' fill' + (r.j.orders === 1 ? '' : 's') + ' for ' + r.j.date, 6000); LST.at = 0; ledgerStatus(true); }
  else ktoast('Capture failed: ' + ledgerErr(r), 7000);
}
function ledgerErr(r){
  const c = r && r.j && r.j.code, m = (r && r.j && (r.j.error || r.j.message)) || ('HTTP ' + (r && r.st));
  if (c === 'NO_PF') return 'the box has not seen the portfolio token yet \u2014 it arrives with every call; try again in a moment';
  if (c === 'NO_LEG' || c === 'NO_MODEL') return m + ' (the official screen lands with the nightly model at 22:30 IST \u2014 reconcile after that)';
  if (c === 'CAP') return m;
  if (r && r.st === 404) return 'the worker or the box is not upgraded for the ledger (404) \u2014 deploy kite-worker.js and kite-relay.js v4';
  return m;
}
function pfName(pf){ try { const k = Object.keys(FEED.byKey || {}).find(x => FEED.byKey[x].pfId === pf); const it = k && uniqStrategies().find(x => identityKey(x.cfg) === k);
  return it ? ((typeof strategyEnglish === 'function' && strategyEnglish(it.cfg)) || it.name || pf) : pf; } catch(e){ return pf; } }
const LD = { reb: '', plan: null, t: 0 };
function ensureLdDlg(){
  if ($('ldWrap')) return;
  const w = document.createElement('div'); w.id = 'ldWrap';
  w.innerHTML = '<div class="card" id="ldDlg"><h3 id="ldTitle"></h3><div class="sub" id="ldSub"></div><div id="ldBody"></div>' +
    '<div class="krow" style="justify-content:flex-end"><button class="btn" id="ldCancel">Close</button><button class="btn on" id="ldGo" style="display:none">Apply to the books</button></div></div>';
  document.body.appendChild(w);
  w.addEventListener('click', e => { if (e.target === w) w.classList.remove('open'); });
  $('ldCancel').onclick = () => $('ldWrap').classList.remove('open');
  $('ldGo').onclick = ledgerApply;
}
const bsym = v => String(v || '').replace(/\.(NS|BO)$/, '');
function ledgerPlanHTML(P){
  const RW = rebalWindow(), n = v => (+v).toLocaleString('en-IN');
  let h = '<div class="sub">' + esc(P.src) + ' \u00b7 fills ' + esc(P.window[0]) + ' \u2192 ' + esc(P.window[1]) + ': <b>' + P.fills.orders + '</b> filled order' + (P.fills.orders === 1 ? '' : 's') +
    ' (' + P.fills.tagged + ' tagged to a strategy' + (P.fills.fromJobs ? ', ' + P.fills.fromJobs + ' from cloud baskets' : '') + ')' +
    (P.snapshots.length ? ' \u00b7 order-book snapshots: ' + esc(P.snapshots.join(', ')) : ' \u00b7 <b class="warn">no order-book snapshot in the window yet</b>') +
    (P.applied ? ' \u00b7 applied before: ' + P.applied.runs + ' run' + (P.applied.runs === 1 ? '' : 's') + ', last ' + esc(P.applied.at || '') : '') + '</div>';
  if (P.consistent) h += '<p style="margin:10px 0 4px"><b>\u2713 The books already match the official ' + esc(RW.tlab) + ' screen.</b> Nothing to write.</p>';
  if (P.trades.length) h += '<h4>Trades to book (' + P.trades.length + ')</h4><div class="twrap"><table><thead><tr><th>Strategy</th><th>Stock</th><th>Qty</th><th>Bought \u2192 sold</th><th>P&amp;L</th></tr></thead><tbody>' +
    P.trades.map(t => '<tr><td>' + esc(pfName(t.pf)) + '</td><td>' + esc(t.invSym) + '</td><td>' + n(t.qty) + '</td><td>' + (+t.openPx).toFixed(2) + ' \u2192 ' + (+t.closePx).toFixed(2) + '</td><td class="' + (t.pnl >= 0 ? 'up' : 'down') + '">' + (t.pnl >= 0 ? '+' : '\u2212') + zinr(Math.abs(t.pnl)) + ' (' + (t.gainPct >= 0 ? '+' : '') + (+t.gainPct).toFixed(1) + '%)</td></tr>').join('') + '</tbody></table></div>';
  if (P.opened.length) h += '<h4>Rows to open (' + P.opened.length + ')</h4><div class="twrap"><table><thead><tr><th>Strategy</th><th>Stock</th><th>Qty</th><th>Avg \u20b9</th><th>Date</th></tr></thead><tbody>' +
    P.opened.map(r => '<tr><td>' + esc(pfName(r.pf)) + '</td><td>' + esc(bsym(r.sym)) + '</td><td>' + n(r.qty) + '</td><td>' + (+r.avg).toFixed(2) + '</td><td>' + esc(r.date) + '</td></tr>').join('') + '</tbody></table></div>';
  if (P.changed.length) h += '<h4>Rows changed</h4><div class="sub">' + P.changed.map(c => esc(pfName(c.pf)) + ' ' + esc(bsym(c.sym)) + ' \u2192 ' + n(c.qty) + ' @ ' + (+c.avg).toFixed(2)).join(' \u00b7 ') + '</div>';
  if (P.removed.length) h += '<h4>Rows closed</h4><div class="sub">' + P.removed.map(c => esc(pfName(c.pf)) + ' ' + esc(bsym(c.sym)) + ' \u00d7 ' + n(c.qty)).join(' \u00b7 ') + '</div>';
  if (Object.keys(P.proceeds || {}).length) h += '<div class="sub" style="margin-top:8px">Attributed sell proceeds: ' + Object.keys(P.proceeds).map(k => esc(pfName(k)) + ' ' + zinr(P.proceeds[k])).join(' \u00b7 ') + '</div>';
  if (P.topups.length) h += '<div class="sub">Tagged top-ups: ' + esc(P.topups.join(', ')) + '</div>';
  if (P.ignored.length) h += '<div class="sub">Ignored (not a strategy exit or entry): ' + esc(P.ignored.join(', ')) + '</div>';
  if (P.warnings.length) h += '<h4 class="warn">Warnings (' + P.warnings.length + ')</h4>' + P.warnings.map(w => '<div class="sub warn">\u26a0 ' + esc(w) + '</div>').join('');
  h += '<div class="khelp">Computed on the box from the cloud holdings row, the official ' + esc(RW.tlab) + ' screen and every fill it captured. Applying writes the new books to the cloud row and this rebalance\u2019s closed trades to its trade log; the Mac pulls both at its next Publish. Re-applying later books only what is new (stragglers, repairs).</div>';
  return h;
}
async function ledgerOpen(){
  ensureLdDlg(); const RW = rebalWindow(); LD.reb = RW.tIso; LD.plan = null;
  $('ldTitle').textContent = 'Reconcile the strategy books \u2014 ' + RW.tlab; $('ldSub').textContent = 'Reading the fills and the official screen from the box\u2026';
  $('ldBody').innerHTML = ''; const g = $('ldGo'); g.style.display = 'none'; g.dataset.arm = ''; g.disabled = false; g.textContent = 'Apply to the books';
  $('ldWrap').classList.add('open');
  const r = await zFetch('/ledger/plan?reb=' + encodeURIComponent(LD.reb));
  if (r.st !== 200 || !r.j || !r.j.ok){ $('ldSub').textContent = ''; $('ldBody').innerHTML = '<p class="warn">\u26a0 ' + esc(ledgerErr(r)) + '</p>'; return; }
  LD.plan = r.j; $('ldSub').textContent = r.j.consistent ? '' : 'Nothing is written until you confirm.'; $('ldBody').innerHTML = ledgerPlanHTML(r.j);
  if (!r.j.consistent) g.style.display = '';
}
async function ledgerApply(){
  const g = $('ldGo'), P = LD.plan; if (!P || g.disabled) return;
  if (!g.dataset.arm){ g.dataset.arm = '1'; g.textContent = 'Tap again to write ' + P.trades.length + ' trade' + (P.trades.length === 1 ? '' : 's') + ' + ' + P.opened.length + ' row' + (P.opened.length === 1 ? '' : 's');
    clearTimeout(LD.t); LD.t = setTimeout(() => { g.dataset.arm = ''; g.textContent = 'Apply to the books'; }, 8000); return; }
  clearTimeout(LD.t); g.disabled = true; g.textContent = 'Writing\u2026';
  const r = await zFetch('/ledger/apply?reb=' + encodeURIComponent(LD.reb), { method: 'POST' });
  g.disabled = false; g.dataset.arm = ''; g.textContent = 'Apply to the books';
  if (r.st !== 200 || !r.j || !r.j.ok){ ktoast('Not written: ' + ledgerErr(r), 9000); return; }
  $('ldWrap').classList.remove('open');
  ktoast(r.j.written ? '\u2713 Books written on the box \u2014 ' + r.j.trades.length + ' trade' + (r.j.trades.length === 1 ? '' : 's') + ' booked, ' + r.j.opened.length + ' row' + (r.j.opened.length === 1 ? '' : 's') + ' opened' + (r.j.error ? ' \u00b7 ' + r.j.error : '') : '\u2713 Books already matched \u2014 nothing written', 9000);
  LST.at = 0; ledgerStatus(true); FEED.ts = 0; await feedPull(true); renderCards();
}
function buyStop(id, msg){ const B = BUYSLICER[id]; if (!B) return; clearTimeout(B.t);
  if (B.remote && B.jobId){ zFetch('/jobs/' + encodeURIComponent(B.jobId) + '/stop', { method: 'POST' }); CLOUD.seen[B.jobId] = 'stopped'; }   // stops it on the VM, from any device
  delete BUYSLICER[id];
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
  if (B.btn) B.btn.textContent = (B.sell ? 'Selling ' : 'Buying ') + B.i + '/' + B.n;
  B.t = setTimeout(() => buyFire(id), 1000);
}
function buyDone(id){ const B = BUYSLICER[id]; if (!B) return;
  if (B.sell && !/^__/.test(id)) setTimeout(() => captureProceeds(id), 120000);   // Option A: actual proceeds fund the T+1 buys
  const f = B.failed && B.failed.length ? [...new Set(B.failed)] : [];
  buyStop(id, 'Basket done — ' + B.n + ' slices sent' + (f.length ? (B.sell ? ' · FAILED (sell separately): ' : ' · FAILED (buy separately): ') + f.join(', ') : ' (unfilled tails rest at their limits)')); }
function buyFire(id){
  const B = BUYSLICER[id]; if (!B) return;
  if (B.remote) return;                                                                         // cloud job: the VM fires it, cloudLoop keeps the counter
  if (cloudOn() && B.i === 0 && !B.cloudTried && B.slices && B.slices.length){ B.cloudTried = 1; cloudSubmit(id); return; }
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
          B.i++; if (B.btn) B.btn.textContent = (B.sell ? 'Selling ' : 'Buying ') + B.i + '/' + B.n;
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
function buyLegGuard(){
  const RW = rebalWindow();
  if (!RW.buyIn){ ktoast('🔒 Buy baskets act only on the morning after month-end — ' + RW.t1lab + ' (buffer to ' + RW.t3lab + '), on the official ' + RW.tlab + ' close screen. Nothing placed.', 7500); return false; }
  const leg = buyLeg(PICKS[ZB.id], RW);
  if (!leg.ok){ ktoast('⚠ ' + leg.msg + ' — buying is locked until then', 7500); return false; }
  return true;
}
function sentMap(orders){ const m = {}; orders.forEach(o => { m[o.tradingsymbol] = (m[o.tradingsymbol] || 0) + (+o.quantity || 0); }); return m; }
async function zbPlaceAll(){
  if (!buyLegGuard()) return;
  const orders = zbOrders();
  if (!orders.length){ ktoast('Nothing to buy — set an amount first'); return; }
  const b = $('zbGo'), est = orders.reduce((s, o) => { const r = ZB.rows.find(x => x.sym === o.tradingsymbol); return s + o.quantity * ((r && r.px) || 0); }, 0);
  if (b.dataset.arm !== '1'){ b.dataset.arm = '1';
    b.textContent = 'Confirm ' + orders.length + ' BUY orders ≈ ' + zinr(est) + ' ?';
    clearTimeout(ZB.t); ZB.t = setTimeout(zbArmReset, 8000); return; }
  b.dataset.arm = '';
  if (Z.directBlocked){ if (kiteSend(orders)){ zbSetBought(ZB.id, true, sentMap(orders)); $('zbWrap').classList.remove('open'); } return; }
  await loadTicks();
  orders.forEach(o => { const r = ZB.rows.find(x => x.sym === o.tradingsymbol); o._px = (r && r.px) || o.price || 0; });
  const slices = buySlices(orders);
  if (BUYSLICER[ZB.id]) buyStop(ZB.id);
  BUYSLICER[ZB.id] = { slices: slices, i: 0, n: slices.length, btn: null, t: 0 };
  zbSetBought(ZB.id, true, sentMap(orders));
  $('zbWrap').classList.remove('open');
  ktoast('Buying in ' + slices.length + ' liquidity-sized slices (1% of the stock\u2019s 10-day traded value, \u20b95L\u2013\u20b91Cr each) every ' + sliceGap() + 's, each a limit \u2264' + sliceRng() + '% above live \u2014 keep this tab open; tap the \u26a1 counter to stop', 6500);
  renderCards();
  buyFire(ZB.id);
}
function kiteSend(orders){
  const key = (function(){ try { return localStorage.getItem('pf_kite_key') || ''; } catch(e){ return ''; } })();
  if (!key){ ktoast('No Kite API key in this browser — save it in Positions & funds → Connect', 5000); return false; }
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
  const ch = $('spChips');
  if (ch) ch.onclick = e => { const b = e.target.closest('[data-spf]'); if (!b) return;
    const k = b.dataset.spf;
    if (k === 'fav'){ FAVONLY = true; SP_FILT = ''; } else if (k === 'all'){ FAVONLY = false; SP_FILT = ''; } else SP_FILT = k;
    try { localStorage.setItem('sp_fav_only', FAVONLY ? '1' : '0'); } catch(e){}
    renderCards(); };
  const mb = $('spMode');
  const mLbl = () => { if (mb){ mb.textContent = PICKMODE === 'live' ? 'Live picks' : 'Rebalance picks'; mb.classList.toggle('on', PICKMODE === 'live'); } };
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
  const sLbl = () => { if (sb2){ sb2.textContent = SIDE === 'sell' ? 'Sell side' : 'Buy side'; sb2.classList.toggle('on', SIDE === 'sell'); } };
  if (sb2) sb2.onclick = async () => {
    SIDE = SIDE === 'sell' ? 'buy' : 'sell'; sLbl(); renderCards();
    if (SIDE === 'sell'){ await feedPull(true); renderCards(); fetchLive(); }
  };
  sLbl();
  if (sb2 && !$('spCloud')){ const cb = document.createElement('button'); cb.id = 'spCloud'; cb.className = 'btn'; cb.style.marginLeft = '4px';
    cb.onclick = () => { try { localStorage.setItem('sw_cloud_slicer', cloudWanted() ? '0' : '1'); } catch(e){} cloudChip(); if (cloudWanted()) cloudProbe(true); };
    sb2.insertAdjacentElement('afterend', cb); cloudChip(); }
  if (!$('spWizardCss')){ const st = document.createElement('style'); st.id = 'spWizardCss';
    st.textContent = '.wz .wz-row{display:flex;align-items:center;gap:8px;padding:5px 4px;border-top:1px solid color-mix(in srgb,currentColor 9%,transparent)}.wz .wz-ic{width:18px;text-align:center;font-weight:700;flex:none}.wz .wz-l{flex:1;min-width:0;font-size:12.5px;line-height:1.35}.wz .wz-b{margin-left:auto;white-space:nowrap;flex:none}.wz-ok .wz-ic{color:var(--up)}.wz-bad .wz-ic{color:var(--down)}.wz-warn .wz-ic{color:#c98500}.wz-info .wz-ic,.wz-off .wz-ic{opacity:.55}';
    document.head.appendChild(st); }
  document.addEventListener('click', e => { const b = e.target.closest('#spWizard [data-wz]'); if (b) wizardAct(b.dataset.wz); });
  loadHolidays();
  cloudLoop();
  document.addEventListener('visibilitychange', () => { if (!document.hidden) cloudLoop(true); });   // a phone opened mid-basket updates at once
  renderCards();
  refreshFavsFromSettings();
  zbaPull();   // synced \u20b9 amounts (token-gated row) \u2014 lands before any basket dialog opens
  feedPull();  // per-strategy held quantities \u2014 the sell view + engine-sized buys read these
  if (window.btSync){ try { await btSync.pullStrategies(); renderCards(); } catch(e){} }
})();
  };
})();