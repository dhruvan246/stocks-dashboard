/* strategies-panel.js — the Saved-Strategies engine as a mountable module.
   window.mountStrategies(container) builds the UI + runs picks/basket-buy inside it.
   Needs (host loads first): @supabase, bt-names.js, bt-sync.js, bt-identity.js, backtest-engine.js. */
(function(){
  'use strict';
  if (window.mountStrategies) return;
  var mounted = false;
  var SP_CSS = "\n.spwrap{font-size:13px;line-height:1.5}\n.spwrap .sp-top{display:flex;align-items:baseline;justify-content:space-between;gap:10px;flex-wrap:wrap;margin-bottom:2px}\n.spwrap .sp-h{font-size:14px;font-weight:800;letter-spacing:-.01em;margin:0}\n.spwrap .sp-actions{display:flex;gap:7px;align-items:center;flex-wrap:wrap}\n.spwrap .sp-sub{font-size:11.5px;color:var(--text-3);line-height:1.45;margin:4px 0}\n.spwrap .sp-input{flex:1;padding:7px 9px;font-size:13px;border:1px solid var(--border);border-radius:8px;background:var(--surface-2);color:var(--text)}\n.spwrap .sym{color:var(--text-3);font-size:11px}\n.spwrap .badge{font-size:9px;font-weight:800;letter-spacing:.05em;padding:1px 5px;border-radius:4px;background:var(--surface-2);color:var(--text-3)}\n.spwrap .zpill{font-size:11.5px;font-weight:700;padding:4px 10px;border-radius:20px;background:var(--surface-2);color:var(--text-2);border:1px solid var(--border)}\n.spwrap .zpill.ok{color:var(--up)} .spwrap .zpill.warn{color:#c98500}\n.spwrap .sblk{border-top:1px solid var(--border);padding:12px 4px 6px}\n.spwrap .sblk:first-child{border-top:0;padding-top:4px}\n.spwrap .shead{display:flex;flex-wrap:wrap;align-items:baseline;gap:6px 12px;margin-bottom:6px}\n.spwrap .shead .nm2{font-size:13.5px;font-weight:800}\n.spwrap .tag{font-size:9.5px;font-weight:800;letter-spacing:.06em;padding:2px 6px;border-radius:4px;white-space:nowrap}\n.spwrap .tag.keep{background:color-mix(in srgb,var(--up) 16%,transparent);color:var(--up)}\n.spwrap .tag.new{background:color-mix(in srgb,var(--buy) 16%,transparent);color:var(--buy)}\n.spwrap .twrap{overflow-x:auto;-webkit-overflow-scrolling:touch}\n.spwrap table{width:100%;border-collapse:collapse;font-size:12.5px}\n.spwrap th{text-align:right;font-weight:700;color:var(--text-3);font-size:10.5px;text-transform:uppercase;letter-spacing:.05em;padding:7px 9px;border-bottom:1px solid var(--border);white-space:nowrap;background:var(--surface)}\n.spwrap th:first-child,.spwrap td:first-child{text-align:left}\n.spwrap td{padding:7px 9px;border-bottom:1px solid var(--border);text-align:right;white-space:nowrap}\n.spwrap td:nth-child(2),.spwrap th:nth-child(2){text-align:left}\n.spwrap .empty{text-align:center;color:var(--text-3);font-size:12.5px;padding:30px 10px}\n.spwrap .khelp{font-size:11.5px;color:var(--text-3);line-height:1.5;margin-top:10px}\n.spwrap .up{color:var(--up)} .spwrap .down{color:var(--down)}\n.spwrap .btn,#zbDlg .btn{border:1px solid var(--border);background:var(--surface);color:var(--text-2);border-radius:9px;padding:6px 11px;font-size:12.5px;font-weight:600;cursor:pointer;white-space:nowrap;display:inline-block}\n.spwrap .btn:hover,#zbDlg .btn:hover{background:var(--surface-2);color:var(--text)}\n.spwrap .btn.on,#zbDlg .btn.on{background:var(--buy);border-color:var(--buy);color:#fff}\n#ktoast{position:fixed;left:50%;bottom:24px;transform:translateX(-50%);z-index:300;background:var(--text);color:var(--bg);padding:8px 14px;border-radius:9px;font-size:12.5px;font-weight:600;opacity:0;pointer-events:none;transition:opacity .25s;max-width:90vw;text-align:center}\n#ktoast.show{opacity:1}\n.zchip{display:inline-block;padding:2px 7px;border-radius:20px;font-size:10.5px;font-weight:700;background:var(--surface-2);border:1px solid var(--border)}\n.zchip.ok{color:var(--up);border-color:var(--up)} .zchip.bad{color:var(--down);border-color:var(--down)} .zchip.open{color:var(--buy);border-color:var(--buy)}\n.zmsg{font-size:11px;color:var(--down);white-space:normal;text-align:left;max-width:280px}\n#zbWrap{position:fixed;inset:0;z-index:200;background:rgba(0,0,0,.5);display:none;align-items:center;justify-content:center;padding:16px}\n#zbWrap.open{display:flex}\n#zbDlg{width:min(600px,100%);max-height:92vh;overflow:auto;padding:16px;background:var(--surface);border:1px solid var(--border);border-radius:14px;box-shadow:var(--shadow)}\n#zbDlg h3{margin:0 0 2px;font-size:14px}\n#zbDlg .sub{font-size:11.5px;color:var(--text-3)}\n#zbDlg .krow{display:flex;gap:8px;margin-top:10px}\n#zbDlg label{flex:1;font-size:11px;font-weight:700;color:var(--text-3);text-transform:uppercase;letter-spacing:.05em}\n#zbDlg input,#zbDlg select{width:100%;margin-top:4px;padding:7px 9px;font-size:13.5px;font-weight:600;border:1px solid var(--border);border-radius:8px;background:var(--surface-2);color:var(--text)}\n#zbDlg table{margin-top:10px;width:100%;border-collapse:collapse;font-size:12.5px}\n#zbDlg th{font-size:10.5px;color:var(--text-3);text-transform:uppercase;padding:7px 9px;text-align:right;border-bottom:1px solid var(--border);white-space:nowrap}\n#zbDlg th:first-child,#zbDlg td:first-child{text-align:left}\n#zbDlg td{padding:6px 9px;border-bottom:1px solid var(--border);text-align:right}\n#zbDlg td input.zbq{width:80px;margin:0;padding:5px 7px;text-align:right}\n#zbDlg td input.zbl{width:92px;margin:0;padding:5px 7px;text-align:right}\n#zbTbl:not(.lim) .limcol{display:none}\n";
  var SP_DOM = "<div class=\"spwrap\"><div class=\"sp-top\"><h2 class=\"sp-h\">Saved strategies \u2014 what each would buy today</h2><div class=\"sp-actions\"><span id=\"zStatus\" class=\"zpill\">Zerodha: checking\u2026</span><button class=\"btn\" id=\"btnZLogin\" style=\"display:none\">Login to Zerodha \u25b8</button><button class=\"btn\" id=\"btnZSetup\" title=\"Kite worker URL\">\u2699</button><button class=\"btn\" id=\"spFavToggle\" title=\"Show only your \u2b50 favourite strategies\"></button><button class=\"btn on\" id=\"btnLoadAll\">\ud83c\udfaf Load all picks</button></div></div><div class=\"sp-sub\">Every strategy saved on the dashboard, one block each. Picks are the screen\u2019s top names as of the latest close; prices go live during market hours. \u26a1 buys the whole basket on Zerodha \u2014 you always confirm first.</div><div class=\"sp-sub\" id=\"status\"></div><div id=\"zSetupBox\" style=\"display:none\"><div class=\"khelp\">One-time per browser: your Kite worker URL (the portfolio/terminal shares it automatically).</div><div style=\"display:flex;gap:7px;max-width:520px;margin-top:6px\"><input id=\"zWUrl\" placeholder=\"https://\u2026workers.dev\" class=\"sp-input\"><button class=\"btn on\" id=\"zWSave\">Save</button></div></div><div id=\"cards\"><div class=\"empty\">Loading saved strategies\u2026</div></div></div>";
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
  const syms = [...new Set(Object.values(PICKS).flatMap(p => p.rows.map(r => r.sym)))];
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
  LIVE_TIMER = setInterval(() => { if (!document.hidden && marketOpen() && Object.keys(PICKS).length) fetchLive(); }, 60000);
}

/* ---------- picks ---------- */
const PICKS = {};
function screenOne(it){
  const picks = screenAsOf(it.cfg, SF.end).slice(0, it.cfg.topN);
  PICKS[it.id] = { asOf: SF.end, rows: picks.map((r, i) => ({ rank: i+1, sym: r.sym, tkr: r.tkr,
    px: (META[r.tkr] && META[r.tkr].raw) ? META[r.tkr].raw : r.price })) };
}
async function loadPicks(id){
  const it = strategies().find(x => x.id === id); if (!it) return;
  if (!await ensureEngine()){ ktoast('Could not load market data — try again'); return; }
  screenOne(it); renderCards(); fetchLive(); startLiveLoop();
}
$('btnLoadAll').onclick = async () => {
  const favs = loadFavs();
  const all = uniqStrategies();
  const nFav = all.filter(it => isFavCfg(favs, it.cfg)).length;
  const list = (FAVONLY && nFav > 0) ? all.filter(it => isFavCfg(favs, it.cfg)) : all;
  if (!list.length) return;
  $('btnLoadAll').disabled = true;
  if (!await ensureEngine()){ $('btnLoadAll').disabled = false; ktoast('Could not load market data'); return; }
  for (const it of list){ $('status').textContent = 'Screening ' + (it.name || '') + '…';
    await new Promise(r => setTimeout(r, 0)); screenOne(it); }
  $('status').textContent = list.length + ' strategies screened as of ' + SF.end + '.';
  $('btnLoadAll').disabled = false;
  renderCards(); fetchLive(); startLiveLoop();
};

/* ---------- Zerodha plumbing (shares the portfolio page's localStorage on this origin) ---------- */
const Z = { connected: false, user: null, held: new Set() };
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
  const h = await zFetch('/holdings');
  if (h.st === 200 && h.j && h.j.data)
    Z.held = new Set(h.j.data.filter(r => ((r.quantity||0)+(r.t1_quantity||0)+(r.collateral_quantity||0)+((r.mtf||{}).quantity||0)) > 0)
                             .map(r => r.tradingsymbol));
  renderCards();
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
  const h = list.map(it => {
    const en = (typeof strategyEnglish === 'function') ? strategyEnglish(it.cfg) : '';
    const disp = en || nameWithBasis(it.name, it.cfg);
    const p = PICKS[it.id];
    let body = '';
    if (p){
      body = '<div class="twrap"><table><thead><tr><th>#</th><th>Pick</th><th>Live ₹</th><th>Day %</th></tr></thead><tbody>' +
        p.rows.map(r => {
          const q = liveQ(r.sym); const px = q && q.ltp != null ? q.ltp : r.px;
          const chg = q && q.ltp != null && q.prevClose ? (q.ltp / q.prevClose - 1) * 100 : null;
          return '<tr><td class="sym">' + r.rank + '</td>' +
            '<td><b>' + esc(r.sym) + '</b> ' + (Z.held.has(r.sym) ? '<span class="tag keep">held</span>' : '<span class="tag new">new</span>') +
            (q && q.ltp != null ? '' : ' <span class="badge">EOD</span>') + '</td>' +
            '<td>₹' + (+px).toFixed(2) + '</td>' +
            '<td class="' + (chg == null ? 'sym' : chg >= 0 ? 'up' : 'down') + '">' + (chg == null ? '—' : (chg >= 0 ? '+' : '') + chg.toFixed(2) + '%') + '</td></tr>';
        }).join('') + '</tbody></table></div>';
    }
    return '<div class="sblk"><div class="shead"><span class="nm2" title="Code-name: ' + esc(nameWithBasis(it.name, it.cfg)) + '">' + (isFavCfg(loadFavs(), it.cfg) ? '\u2b50 ' : '') + esc(disp) + '</span>' +
      (it._priv ? '<span class="tag new">private</span>' : '') +
      '<span class="sym">' + esc(cardMeta(it.cfg)) + (p ? ' · picks as of ' + esc(p.asOf) : '') + '</span>' +
      '<span style="margin-left:auto; display:flex; gap:6px">' +
      '<button class="btn" data-load="' + esc(it.id) + '">🎯 ' + (p ? 'Refresh' : 'Picks') + '</button>' +
      (p ? '<button class="btn on" data-basket="' + esc(it.id) + '">⚡ Buy basket</button>' : '') +
      '</span></div>' + body + '</div>';
  }).join('');
  $('cards').innerHTML = h;
}
$('cards').addEventListener('click', e => {
  const l = e.target.closest('[data-load]'); if (l){ l.textContent = '⏳…'; loadPicks(l.dataset.load); return; }
  const b = e.target.closest('[data-basket]'); if (b) zBasketOpen(b.dataset.basket);
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
    if (kiteSend(o)) $('zbWrap').classList.remove('open'); };
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
    r.qty = Math.floor(per / perShare); r.margin = null; });
  if (ZB.id && amt) try { localStorage.setItem('sw_zb_amt_' + ZB.id, String(amt)); } catch(e){}
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
      '<td><b>' + esc(r.sym) + '</b>' + (Z.held.has(r.sym) ? ' <span class="tag keep">held</span>' : '') + (r.live ? '' : ' <span class="badge">EOD</span>') + '</td>' +
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
  $('zbAmt').value = (function(){ try { return localStorage.getItem('sw_zb_amt_' + id) || ''; } catch(e){ return ''; } })();
  zbAlloc(); zbRender();
  $('zbWrap').classList.add('open');
  zbMarginSoon(true);
}
async function zbPlaceAll(){
  const orders = zbOrders();
  if (!orders.length){ ktoast('Nothing to buy — set an amount first'); return; }
  const b = $('zbGo'), est = orders.reduce((s, o) => { const r = ZB.rows.find(x => x.sym === o.tradingsymbol); return s + o.quantity * ((r && r.px) || 0); }, 0);
  if (b.dataset.arm !== '1'){ b.dataset.arm = '1';
    b.textContent = 'Confirm ' + orders.length + ' BUY orders ≈ ' + zinr(est) + ' ?';
    clearTimeout(ZB.t); ZB.t = setTimeout(zbArmReset, 8000); return; }
  b.dataset.arm = '';
  if (Z.directBlocked){ if (kiteSend(orders)) $('zbWrap').classList.remove('open'); return; }
  b.disabled = true; b.textContent = 'Placing…';
  let bailed = false;
  for (const o of orders){
    const r = ZB.rows.find(x => x.sym === o.tradingsymbol); r.st = '…'; zbRender();
    const { st, j } = await zFetch('/order', { method:'POST', headers:{ 'Content-Type':'application/json' }, body: JSON.stringify(o) });
    const msg = (j && j.message) || ('HTTP ' + st);
    if (st === 200 && j && j.data && j.data.order_id){ r.st = 'sent'; r.oid = j.data.order_id; }
    else if (ipBlocked(msg) || st === 0){ Z.directBlocked = true; bailed = true; r.st = ''; break; }   // static-IP reject: stop, use the popup
    else { r.st = 'fail'; r.msg = msg; }
    zbRender();
  }
  b.disabled = false; b.textContent = 'Place all ▸';
  if (bailed){
    ZB.rows.forEach(r => { if (r.st === '…') r.st = ''; }); zbRender();
    ktoast('Direct API orders need a whitelisted static IP — opening the Zerodha basket to confirm instead…', 5500);
    kiteSend(orders); return;
  }
  await new Promise(r => setTimeout(r, 1800));
  const o = await zFetch('/orders'), list = (o.j && o.j.data) || [];
  ZB.rows.forEach(r => { const row = r.oid && list.find(x => x.order_id === r.oid);
    if (row){ r.st = row.status; r.msg = row.status_message || ''; } });
  zbRender(); b.disabled = false; b.textContent = 'Place all ▸';
  const okN = ZB.rows.filter(r => r.st === 'COMPLETE').length;
  ktoast(okN + '/' + orders.length + ' orders complete — see the table for the rest', 6000);
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
  renderCards();
  refreshFavsFromSettings();
  if (window.btSync){ try { await btSync.pullStrategies(); renderCards(); } catch(e){} }
})();
  };
})();