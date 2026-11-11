/* F&O plan panel for docs/terminal.html (user 2026-11-11: "create fno tab as well and buy buttons like i hv for mtf").
   The plan = the backtested "choice B on 15%" on five stock futures (CDSL, CANBK, POWERINDIA, GODREJPROP, BHEL):
     - POT = the money set aside for this plan only (user's choice: a separate pot, MTF/other holdings ignored).
       It is typed in once (today's value; the user already holds all five), then carried forward by Zerodha's own
       daily mark-to-market (m2m) of these futures:  pot(today) = pot(last record) + qty·(today's prev close − record
       price) + today's m2m.  A record is only carried when every quantity is unchanged since it was written —
       otherwise the panel asks for today's pot value instead of guessing.
     - At each day's close (first look after 15:35 IST, by this page or the local 4 PM alert job):
         positions > 4x pot          → SELL back to 4x (and a buy-back waits for the pot to rise 5% off its low)
         buy-back waiting, pot ≥ low×1.05 → BUY back up to 3.5x
         pot > its high              → new high, dip-buy re-armed
         armed and pot ≥15% under high → DIP BUY up to 3.5x, disarmed until a new high
       (same order as the backtest engine sim5.run5(trig=15, Lb=3.5, bb=True)).
     - Rolls: within N days of expiry, sell the month's contract and buy the next month's (next month's own lot).
   Orders are sent by the user's two clicks (arm → confirm) — or, ONLY while the user has switched the AUTO-PILOT on
   in this tab (user 2026-11-11: "Yes, build auto-pilot"; ON until switched off; dip buy + buy-back, sell to 4x and
   the monthly roll), by the cloud robot scripts/fno_autopilot.mjs, which runs window.fnoAuto below — the same plan
   and order functions as the buttons:  14:30 IST roll (a held contract ≤ 3 trading days from expiry),  15:20 IST
   rule check on the live pot (the user chose 3:20 PM over next-morning) and its orders before the 15:30 close.
   Claude never switches it on.
   State lives in the token-addressed synced row <pf_token>.fnopot (same store/secret as the basket amounts), so the
   phone, the laptop and the local alert job (~/stocks-cache/zerodha-ACH754/fno_alert.py) see one pot. */
(function () {
  'use strict';
  const LS = 'sw_fno_pot_v1';
  const SB = 'https://nebjnsndgrhumnkuipqy.supabase.co/rest/v1/rpc/';
  const ANON = 'sb_publishable_MDlQwiVc5deii91__UNeDg_z9r4Fk98';
  const SECRET = 'sw_owner_8Kq2Lm9Xp4Rt7v';          // same public write secret strategies-panel.js / sw-sync.js ship
  const DEF = { syms: ['CDSL', 'CANBK', 'POWERINDIA', 'GODREJPROP', 'BHEL'], trig: 15, buyTo: 3.5, maxLev: 4, bb: 5,
                maxLots: 10, rollDays: 7, gapMs: 1200 };
  let AUTOARM = false, AUTOT = 0, READY = false;
  let API = null, EL = null, POS = null, POSERR = '', CON = null, PREVIEW = null, ARMED = false, ARMT = 0, RUN = null, TIMER = 0;

  /* ---------- small helpers ---------- */
  const esc = s => String(s == null ? '' : s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
  const ist = () => new Date(Date.now() + 330 * 60000);
  const today = () => ist().toISOString().slice(0, 10);
  const istMin = () => { const d = ist(); return d.getUTCHours() * 60 + d.getUTCMinutes(); };
  const weekday = () => { const g = ist().getUTCDay(); return g >= 1 && g <= 5; };
  function money(v) {
    if (v == null || !isFinite(v)) return '—';
    const a = Math.abs(v), s = v < 0 ? '−' : '';
    if (a >= 1e7) return s + '₹' + (a / 1e7).toFixed(2) + ' Cr';
    if (a >= 1e5) return s + '₹' + (a / 1e5).toFixed(2) + ' L';
    return s + '₹' + Math.round(a).toLocaleString('en-IN');
  }
  const signed = v => (v == null || !isFinite(v)) ? '—' : (v >= 0 ? '+' : '') + money(v).replace('−', '−');
  const pct = (v, d) => (v == null || !isFinite(v)) ? '—' : (v * 100).toFixed(d == null ? 1 : d) + '%';
  const toast = (m, ms) => { try { API.ktoast(m, ms); } catch (e) {} };
  const daysTo = exp => Math.round((Date.parse(exp + 'T00:00:00Z') - Date.parse(today() + 'T00:00:00Z')) / 864e5);
  const parseRs = s => { s = String(s || '').replace(/[₹,\s]/g, '').toLowerCase(); let m = 1;
    if (/cr$/.test(s)) { m = 1e7; s = s.replace(/cr$/, ''); } else if (/l(akh)?$/.test(s)) { m = 1e5; s = s.replace(/l(akh)?$/, ''); }
    const v = parseFloat(s); return isFinite(v) ? v * m : NaN; };

  /* ---------- synced doc ---------- */
  function blank() { return { v: 1, cfg: Object.assign({ ts: 0 }, DEF), rec: null, st: { ts: 0, H: null, Hd: '', armed: true, pend: false, low: null, evalDate: '', due: [] }, log: [], auto: { ts: 0, on: false, at: 0 } }; }
  function docGet() { try { const d = JSON.parse(localStorage.getItem(LS) || 'null'); if (d && d.v === 1) return fill(d); } catch (e) {} return blank(); }
  function fill(d) { const b = blank(); d.cfg = Object.assign(b.cfg, d.cfg || {}); d.st = Object.assign(b.st, d.st || {}); d.auto = Object.assign(b.auto, d.auto || {}); d.log = d.log || []; return d; }
  function merge(a, b) {          // newest section wins; the log is a union by time
    a = fill(a); b = fill(b);
    const pick = (x, y, k) => ((y || {})[k] || 0) >= ((x || {})[k] || 0) ? y : x;
    const L = {}; a.log.concat(b.log).forEach(e => { L[e.t + '|' + e.k] = e; });
    return { v: 1, cfg: pick(a.cfg, b.cfg, 'ts'), rec: (a.rec || b.rec) ? pick(a.rec, b.rec, 't') : null,
             st: pick(a.st, b.st, 'ts'), auto: pick(a.auto, b.auto, 'ts'), log: Object.values(L).sort((x, y) => x.t - y.t).slice(-80) };
  }
  function row() { try { const t = localStorage.getItem('pf_token') || ''; return t ? t + '.fnopot' : ''; } catch (e) { return ''; } }
  async function gunzipB64(z) { const by = Uint8Array.from(atob(z), c => c.charCodeAt(0));
    return JSON.parse(await new Response(new Blob([by]).stream().pipeThrough(new DecompressionStream('gzip'))).text()); }
  async function pull() {
    const r0 = row(); if (!r0 || typeof DecompressionStream === 'undefined') return false;
    try {
      const r = await fetch(SB + 'pf_feed_get?token=' + encodeURIComponent(r0) + '&apikey=' + ANON, { cache: 'no-store' });
      if (!r.ok) return false; const j = await r.json(); if (!j || !j.z) return true;
      const rem = await gunzipB64(j.z); if (rem && rem.v === 1) localStorage.setItem(LS, JSON.stringify(merge(docGet(), rem)));
      return true;
    } catch (e) { return false; }
  }
  async function push() {
    const r0 = row(); if (!r0 || typeof CompressionStream === 'undefined') return;
    try {
      await pull();                                   // merge first: a one-section edit never clobbers the rest
      const buf = new Uint8Array(await new Response(new Blob([JSON.stringify(docGet())]).stream().pipeThrough(new CompressionStream('gzip'))).arrayBuffer());
      let b = ''; buf.forEach(x => b += String.fromCharCode(x));
      await fetch(SB + 'pf_feed_set', { method: 'POST', headers: { apikey: ANON, Authorization: 'Bearer ' + ANON, 'Content-Type': 'application/json' },
        body: JSON.stringify({ secret: SECRET, token: r0, payload: { z: btoa(b) } }) });
    } catch (e) {}
  }
  function save(d, now) { try { localStorage.setItem(LS, JSON.stringify(d)); } catch (e) {} clearTimeout(save._t); save._t = setTimeout(push, now ? 0 : 1500); }
  function logAdd(d, k, msg) { d.log.push({ t: Date.now(), k, msg }); d.log = d.log.slice(-80); }

  /* ---------- data: contracts (CI file) + Zerodha positions ---------- */
  async function loadContracts() {
    if (CON) return CON;
    try { const r = await fetch('./fno_contracts.json', { cache: 'no-store' }); if (r.ok) CON = await r.json(); } catch (e) {}
    return CON;
  }
  let HOL = null;
  async function loadHolidays() {
    if (HOL) return HOL;
    try { const r = await fetch('./nse_holidays.json', { cache: 'no-store' }); if (r.ok) { const j = await r.json();
      HOL = new Set(Object.values(j.holidays || {}).flatMap(y => Object.keys(y))); } } catch (e) {}
    return HOL || new Set();
  }
  // trading days from tomorrow up to and including the expiry day (expiry tomorrow = 1; today = 0)
  function tdTo(exp) { const H = HOL || new Set(); let n = 0, t = Date.parse(today() + 'T00:00:00Z'); const e = Date.parse(exp + 'T00:00:00Z');
    if (!(e >= t)) return 0;
    while (t < e) { t += 864e5; const d = new Date(t), g = d.getUTCDay(); if (g !== 0 && g !== 6 && !H.has(d.toISOString().slice(0, 10))) n++; }
    return n; }
  function contracts(sym) { const f = (CON && CON.f && CON.f[sym]) || []; return f.map(c => ({ ts: c[0], exp: c[1], lot: c[2], settle: c[3], close: c[4] })); }
  function conOf(ts) { for (const s in (CON && CON.f) || {}) for (const c of CON.f[s]) if (c[0] === ts) return { sym: s, ts, exp: c[1], lot: c[2], settle: c[3] }; return null; }
  function symOf(ts, syms) { const m = /^([A-Z0-9&-]+?)(\d{2}[A-Z]{3})FUT$/.exec(ts || ''); return m && syms.includes(m[1]) ? m[1] : null; }
  async function loadPositions() {
    const r = await API.acctFetch('/positions');
    if (!r || r.st !== 200 || !r.j || r.j.status !== 'success') {
      POS = null; POSERR = r && r.st === 403 ? 'login' : (r && r.st === -1 ? 'worker' : (r && r.st === 401 ? 'token' : 'down'));
      return null;
    }
    const syms = docGet().cfg.syms, P = {};
    (r.j.data.net || []).forEach(p => {
      if (p.exchange !== 'NFO') return; const s = symOf(p.tradingsymbol, syms); if (!s) return;
      const o = P[p.tradingsymbol] || (P[p.tradingsymbol] = { sym: s, ts: p.tradingsymbol, q: 0, oq: 0, ltp: +p.last_price || 0, close: +p.close_price || 0, m2m: 0, pnl: 0 });
      o.q += +p.quantity || 0; o.oq += +p.overnight_quantity || 0; o.m2m += +p.m2m || 0; o.pnl += +p.pnl || 0;
      if (+p.last_price) o.ltp = +p.last_price; if (+p.close_price) o.close = +p.close_price;
    });
    POS = P; POSERR = ''; return P;
  }

  /* ---------- the pot ---------- */
  function pnlSum(P) { let s = 0; for (const k in P) s += P[k].pnl; return s; }   // Zerodha's "P&L" (since the buy price)
  function m2mSum(P) { let s = 0; for (const k in P) s += P[k].m2m; return s; }
  function potCalc(d, P) {
    const rec = d.rec; if (!rec || !P) return null;
    const m = m2mSum(P);
    if (rec.date === today()) return { pot: rec.pot + (m - (rec.m2m || 0)), ok: true, bad: [], m2m: m };
    const bad = []; let open = rec.pot;
    for (const ts in rec.rows) { const r = rec.rows[ts]; if (!r.q) continue; const p = P[ts];
      if (!p || p.oq !== r.q || !(p.close > 0)) { bad.push(ts); continue; }
      open += r.q * (p.close - r.px); }
    for (const ts in P) if (P[ts].oq && !(rec.rows[ts] && rec.rows[ts].q)) bad.push(ts);
    return { pot: open + m, ok: !bad.length, bad, m2m: m, open };
  }
  function exposure(P) { let s = 0; for (const k in P) s += Math.abs(P[k].q) * P[k].ltp; return s; }
  function recordFrom(P, pot) { const rows = {}; for (const ts in P) rows[ts] = { q: P[ts].q, px: P[ts].ltp };
    return { t: Date.now(), date: today(), pot, m2m: m2mSum(P), rows }; }
  function sameQty(rec, P) { if (!rec) return false; const a = Object.keys(rec.rows).filter(k => rec.rows[k].q), b = Object.keys(P).filter(k => P[k].q);
    return a.length === b.length && b.every(k => rec.rows[k] && rec.rows[k].q === P[k].q); }

  /* close evaluation — the backtest's order of checks, once per trading day after 15:35 IST */
  function evalClose(d, a, pos, who) {
    const c = d.cfg, st = d.st, due = [];
    if (pos > c.maxLev * a) { due.push({ k: 'sell', msg: 'Positions ' + money(pos) + ' are above ' + c.maxLev + 'x of the pot ' + money(a) }); st.pend = true; st.low = a; }
    if (st.pend) { st.low = Math.min(st.low, a);
      if (a >= st.low * (1 + c.bb / 100)) { due.push({ k: 'bb', msg: 'Pot is ' + c.bb + '% above its low ' + money(st.low) + ' after the forced sell' }); st.pend = false; } }
    if (st.H == null || a > st.H) { st.H = a; st.Hd = today(); st.armed = true; }
    const dd = 1 - a / st.H;
    if (st.armed && dd * 100 >= c.trig) { due.push({ k: 'dip', msg: 'Pot closed ' + pct(dd) + ' below its high ' + money(st.H) }); st.armed = false; }
    st.due = due.map(x => Object.assign({ d: today() }, x)); st.evalDate = today(); st.ts = Date.now();
    logAdd(d, 'eval', who + ' close check: pot ' + money(a) + ', positions ' + money(pos) + ' (' + (pos / a).toFixed(2) + 'x)' + (due.length ? ' → ' + due.map(x => x.k).join(', ') : ''));
  }

  /* ---------- order plans ---------- */
  function heldBySym(P, syms) {     // the nearest-expiry held contract per stock (+ every held one)
    const out = {}; syms.forEach(s => out[s] = { all: [] });
    for (const ts in P) { const p = P[ts]; if (!p.q) continue; const c = conOf(ts) || { exp: '', lot: 0 };
      out[p.sym].all.push(Object.assign({ exp: c.exp, lot: c.lot }, p)); }
    syms.forEach(s => { out[s].all.sort((x, y) => (x.exp || '').localeCompare(y.exp || '')); out[s].near = out[s].all[0] || null; });
    return out;
  }
  const ord = (ts, side, q, sym, px, note) => ({ tradingsymbol: ts, exchange: 'NFO', transaction_type: side, order_type: 'MARKET', quantity: q, product: 'NRML', _sym: sym, _px: px, _note: note || '' });
  function planBuyUpTo(d, P, a, kind) {
    const c = d.cfg, pos = exposure(P), amt = Math.min(c.buyTo * a - pos, c.maxLev * a - pos), H = heldBySym(P, c.syms), out = [], skip = [];
    if (!(amt > 0)) return { orders: [], note: 'No room: positions ' + money(pos) + ' are already at or above ' + c.buyTo + 'x of the pot (' + money(c.buyTo * a) + ').' };
    c.syms.forEach(s => { const h = H[s].near, cs = contracts(s); const tgt = h || (cs[0] && { ts: cs[0].ts, ltp: cs[0].settle, lot: cs[0].lot });
      if (!tgt || !tgt.lot || !tgt.ltp) { skip.push(s); return; }
      const lots = Math.floor(amt / c.syms.length / (tgt.ltp * tgt.lot)); if (lots > 0) out.push(ord(tgt.ts, 'BUY', lots * tgt.lot, s, tgt.ltp, lots + ' lot' + (lots > 1 ? 's' : ''))); });
    return { orders: out, note: (kind === 'bb' ? 'Buy-back' : 'Dip buy') + ': add ' + money(amt) + ' of positions (up to ' + c.buyTo + 'x of ' + money(a) + '), split equally.' + (skip.length ? ' No lot size/price for ' + skip.join(', ') + '.' : '') };
  }
  function planSell4x(d, P, a) {
    const c = d.cfg, pos = exposure(P); if (!(pos > c.maxLev * a)) return { orders: [], note: 'Positions ' + money(pos) + ' are within ' + c.maxLev + 'x of the pot (' + money(c.maxLev * a) + ') — nothing to sell.' };
    const f = c.maxLev * a / pos, out = [];
    for (const ts in P) { const p = P[ts]; if (!(p.q > 0)) continue; const lot = (conOf(ts) || {}).lot; if (!lot) continue;
      const nq = Math.floor(p.q * f / lot) * lot; if (p.q - nq > 0) out.push(ord(ts, 'SELL', p.q - nq, p.sym, p.ltp, ((p.q - nq) / lot) + ' lots')); }
    return { orders: out, note: 'Sell back to ' + c.maxLev + 'x: positions ' + money(pos) + ' → about ' + money(c.maxLev * a) + ' (each stock cut by the same share, whole lots).' };
  }
  function planRoll(d, P, inWin) {
    const c = d.cfg, H = heldBySym(P, c.syms), out = [], lines = [];
    inWin = inWin || (h => daysTo(h.exp) <= c.rollDays);
    c.syms.forEach(s => { H[s].all.forEach(h => { if (!h.exp || !inWin(h) || !(h.q > 0)) return;
      const nx = contracts(s).find(x => x.exp > h.exp); if (!nx) { lines.push(s + ': next month not in the contract list yet'); return; }
      const nq = Math.round(h.q / nx.lot) * nx.lot;
      out.push(ord(h.ts, 'SELL', h.q, s, h.ltp, 'roll out'));
      if (nq > 0) out.push(ord(nx.ts, 'BUY', nq, s, nx.settle, 'roll in · ' + (nq / nx.lot) + ' lots of ' + nx.lot + (nx.lot !== h.lot ? ' (lot changes from ' + h.lot + ')' : ''))); }); });
    return { orders: out, note: out.length ? 'Roll: sell this month, buy next month (same size, next month\'s lot).' + (lines.length ? ' ' + lines.join('; ') : '') : 'No contract is within ' + c.rollDays + ' days of expiry.' };
  }
  function planEqual(d, P) {
    const c = d.cfg, H = heldBySym(P, c.syms), e = {}; c.syms.forEach(s => e[s] = H[s].all.reduce((t, h) => t + h.q * h.ltp, 0));
    const out = [];
    c.syms.forEach(s => { const others = c.syms.filter(x => x !== s), tgt = others.reduce((t, x) => t + e[x], 0) / others.length, h = H[s].near;
      const cs = contracts(s), k = h || (cs[0] && { ts: cs[0].ts, ltp: cs[0].settle, lot: cs[0].lot }); if (!k || !k.lot) return;
      const lots = Math.round((tgt - e[s]) / (k.ltp * k.lot)); if (lots >= 1) out.push(ord(k.ts, 'BUY', lots * k.lot, s, k.ltp, lots + ' lots → ≈ ' + money(e[s] + lots * k.lot * k.ltp) + ' (others avg ' + money(tgt) + ')')); });
    return { orders: out, note: out.length ? 'Make equal: top up any stock that sits a lot or more below the average of the other four.' : 'All five are within about a lot of each other — nothing to top up.' };
  }
  function slices(o, maxLots) {      // big orders go in pieces of at most maxLots lots (exchange freeze limits)
    const lot = (conOf(o.tradingsymbol) || {}).lot || 1, cap = Math.max(1, maxLots) * lot, out = []; let left = o.quantity;
    while (left > 0) { const q = Math.min(cap, left); out.push(Object.assign({}, o, { quantity: q })); left -= q; }
    return out;
  }

  /* ---------- placing (two clicks: arm, then confirm) ---------- */
  async function placeAll(kind) {
    if (!PREVIEW || !PREVIEW.orders.length || RUN) return;
    await sendOrders(kind, PREVIEW.orders, 'you');
    toast(RUN.ok + ' of ' + RUN.n + ' orders placed' + (RUN.fail.length ? ' · ' + RUN.fail.length + ' problem(s)' : ''), 6000);
    try { API.refresh(); } catch (e) {}
    setTimeout(() => { RUN = null; PREVIEW = null; refresh(); }, 2500);
  }
  // who = 'you' (buttons) or 'auto-pilot' (robot: no Zerodha-basket fallback — a headless page can't use it)
  async function sendOrders(kind, orders, who) {
    const d = docGet(), list = []; orders.forEach(o => slices(o, d.cfg.maxLots).forEach(x => list.push(x)));
    RUN = { kind, n: list.length, done: 0, ok: 0, fail: [], stop: false, placed: [], failed: [] }; render();
    for (const o of list) {
      if (RUN.stop) break;
      const body = { variety: 'regular', validity: 'DAY', tag: 'fnoplan', tradingsymbol: o.tradingsymbol, exchange: 'NFO', transaction_type: o.transaction_type, order_type: 'MARKET', quantity: o.quantity, product: 'NRML' };
      const r = await API.acctFetch('/order', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
      const msg = (r && r.j && r.j.message) || ('HTTP ' + (r && r.st));
      if (r && r.st === 200 && r.j && r.j.data && r.j.data.order_id) { RUN.ok++; RUN.placed.push(o.transaction_type + ' ' + o.quantity + ' ' + o.tradingsymbol); }
      else if (who === 'you' && (API.ipBlocked(msg) || (r && r.st === 0))) {      // static-IP rule: hand the rest to Zerodha's basket (max 10)
        const rest = list.slice(RUN.done).map(x => { const y = Object.assign({}, x); Object.keys(y).forEach(k => k[0] === '_' && delete y[k]); return y; });
        API.kiteSend(rest.slice(0, 10)); RUN.fail.push('direct orders blocked — ' + Math.min(10, rest.length) + ' sent to the Zerodha basket' + (rest.length > 10 ? ', ' + (rest.length - 10) + ' NOT sent (basket max 10)' : ''));
        RUN.done = list.length; break;
      } else { RUN.fail.push(o.transaction_type + ' ' + o.quantity + ' ' + o.tradingsymbol + ': ' + msg); RUN.failed.push(o); }
      RUN.done++; render();
      if (RUN.done < list.length) await new Promise(res => setTimeout(res, d.cfg.gapMs));
    }
    const dd = docGet(); logAdd(dd, who === 'you' ? 'orders' : 'auto', (who === 'you' ? '' : 'Auto-pilot ') + kind + ': ' + RUN.ok + ' of ' + RUN.n + ' orders placed' + (RUN.fail.length ? '; ' + RUN.fail.join(' | ') : ''));
    // a due stays open while any order failed: the robot's next pass re-plans it from the fresh positions (only the rest)
    if (RUN.ok && ['dip', 'bb', 'sell'].includes(kind) && (who === 'you' || !RUN.fail.length)) dd.st.due = (dd.st.due || []).filter(x => x.k !== kind), dd.st.ts = Date.now();
    // robot: remember exactly the refused orders, so the next pass re-sends only those (not a fresh, smaller re-plan)
    else if (who !== 'you' && RUN.fail.length && ['dip', 'bb', 'sell'].includes(kind)) { (dd.st.due || []).forEach(x => { if (x.k === kind) x.retry = RUN.failed.slice(); }); dd.st.ts = Date.now(); }
    save(dd, true);
    return RUN;
  }

  /* ---------- render ---------- */
  function css() {
    if (document.getElementById('fnoCss')) return;
    const s = document.createElement('style'); s.id = 'fnoCss';
    s.textContent = `
.fnw{padding:14px 16px 90px;max-width:1100px}
.fnw h2{font-size:17px;margin:0 0 2px;font-weight:800}.fnw .sub{color:var(--text-3);font-size:12px;margin-bottom:12px}
.fnw .kp{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin:10px 0 14px}
.fnw .k{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:10px 12px}
.fnw .k .l{font-size:10.5px;font-weight:700;color:var(--text-3);text-transform:uppercase;letter-spacing:.06em}
.fnw .k .v{font-size:18px;font-weight:800;margin-top:3px;font-variant-numeric:tabular-nums}.fnw .k .s{font-size:11.5px;color:var(--text-2);margin-top:2px}
.fnw .up{color:var(--up)}.fnw .dn{color:var(--down)}
.fnw .box{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:12px 14px;margin:0 0 12px;font-size:13px;line-height:1.5}
.fnw .box.warn{border-color:var(--down);background:var(--down-soft)}.fnw .box.due{border-color:var(--accent);background:var(--buy-soft)}
.fnw .bt{font-size:11px;font-weight:800;color:var(--text-3);text-transform:uppercase;letter-spacing:.07em;margin:0 0 6px}
.fnw .tw{overflow-x:auto;border:1px solid var(--border);border-radius:12px;background:var(--surface);margin-bottom:12px}
.fnw table{width:100%;border-collapse:collapse;font-size:12.5px;font-variant-numeric:tabular-nums}
.fnw th{text-align:right;font-size:10px;font-weight:700;color:var(--text-3);text-transform:uppercase;letter-spacing:.07em;padding:8px 10px;border-bottom:1px solid var(--border);white-space:nowrap}
.fnw td{text-align:right;padding:8px 10px;border-bottom:1px solid var(--border-2);white-space:nowrap}
.fnw th:first-child,.fnw td:first-child{text-align:left}.fnw tfoot td{font-weight:800}
.fnw .mono{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:11.5px;color:var(--text-2)}
.fnw .acts{display:flex;flex-wrap:wrap;gap:8px;margin:4px 0 12px}
.fnw .fb{border:1px solid var(--border);background:var(--surface-2);color:var(--text);border-radius:9px;padding:9px 13px;font-size:12.5px;font-weight:700;cursor:pointer}
.fnw .fb.pri{background:var(--accent);border-color:var(--accent);color:#fff}.fnw .fb.sel{background:var(--sell);border-color:var(--sell);color:#fff}
.fnw .fb.armed{background:#d98200;border-color:#d98200;color:#fff}.fnw .fb:disabled{opacity:.5;cursor:default}
.fnw .acts2{display:flex;flex-direction:column;gap:6px;margin:6px 0 12px}.fnw .arow{display:flex;gap:10px;align-items:center;flex-wrap:wrap}.fnw .arow .fb{min-width:180px;text-align:left}.fnw .arow .sub{margin:0;flex:1;min-width:160px}
.fnw .fb.on{outline:2px solid var(--accent)}
.fnw input{border:1px solid var(--border);background:var(--surface-2);color:var(--text);border-radius:8px;padding:8px 10px;font-size:13px;width:150px}
.fnw .row{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin:6px 0}
.fnw .bs{font-weight:800}.fnw .bs.b{color:var(--buy)}.fnw .bs.s{color:var(--sell)}
.fnw .log{font-size:11.5px;color:var(--text-2);max-height:160px;overflow:auto}
.fnw details summary{cursor:pointer;font-weight:700;font-size:12.5px;margin:8px 0}
@media (max-width:640px){.fnw{padding:12px 12px 90px}.fnw .k .v{font-size:16px}.fnw input{width:120px}}`;
    document.head.appendChild(s);
  }
  function posTable(d, P, a) {
    const c = d.cfg, H = heldBySym(P, c.syms); let te = 0, tm = 0, tp = 0; const tot = exposure(P);
    const rows = c.syms.map(s => {
      const hs = H[s].all; if (!hs.length) return '<tr><td><b>' + s + '</b></td><td class="mono">no position</td><td></td><td></td><td></td><td></td><td></td><td></td><td></td></tr>';
      return hs.map(h => { const e = Math.abs(h.q) * h.ltp; te += e; tm += h.m2m; tp += h.pnl; const dt = h.exp ? daysTo(h.exp) : null;
        return '<tr><td><b>' + s + '</b></td><td class="mono">' + esc(h.ts) + '</td><td class="secret">' + (h.lot ? (h.q / h.lot) + ' × ' + h.lot : h.q) + '</td><td>' + (h.ltp ? h.ltp.toFixed(2) : '—') +
          '</td><td class="secret">' + money(e) + '</td><td>' + (tot ? pct(e / tot, 0) : '—') + '</td><td class="secret ' + (h.m2m >= 0 ? 'up' : 'dn') + '">' + signed(h.m2m) + '</td><td class="secret ' + (h.pnl >= 0 ? 'up' : 'dn') + '">' + signed(h.pnl) + '</td><td' + (dt != null && dt <= c.rollDays ? ' class="dn"' : '') + '>' + (h.exp ? h.exp.slice(5) + ' · ' + dt + 'd' : '—') + '</td></tr>'; }).join('');
    }).join('');
    return '<div class="tw"><table><thead><tr><th>Stock</th><th>Contract</th><th>Lots</th><th>LTP</th><th>Exposure</th><th>Share</th><th>Today</th><th>P&amp;L</th><th>Expiry</th></tr></thead><tbody>' + rows +
      '</tbody><tfoot><tr><td>Total</td><td></td><td></td><td></td><td class="secret">' + money(te) + '</td><td>' + (a ? (te / a).toFixed(2) + 'x' : '') + '</td><td class="secret ' + (tm >= 0 ? 'up' : 'dn') + '">' + signed(tm) + '</td><td class="secret ' + (tp >= 0 ? 'up' : 'dn') + '">' + signed(tp) + '</td><td></td></tr></tfoot></table></div>';
  }
  function previewHTML() {
    if (!PREVIEW) return '';
    const o = PREVIEW.orders, d = docGet();
    let h = '<div class="box due"><div class="bt">' + esc(PREVIEW.title) + '</div><div>' + esc(PREVIEW.note) + '</div>';
    if (o.length) {
      let tv = 0; h += '<div class="tw" style="margin-top:8px"><table><thead><tr><th>Stock</th><th>Contract</th><th>Side</th><th>Qty</th><th>≈ Price</th><th>≈ Value</th><th>Note</th></tr></thead><tbody>' +
        o.map(x => { const v = x.quantity * (x._px || 0); tv += v; const n = slices(x, d.cfg.maxLots).length;
          return '<tr><td><b>' + x._sym + '</b></td><td class="mono">' + esc(x.tradingsymbol) + '</td><td class="bs ' + (x.transaction_type === 'BUY' ? 'b">BUY' : 's">SELL') + '</td><td class="secret">' + x.quantity.toLocaleString('en-IN') + (n > 1 ? ' <span class="mono">(' + n + ' orders)</span>' : '') + '</td><td>' + (x._px ? x._px.toFixed(2) : '—') + '</td><td class="secret">' + money(v) + '</td><td class="mono">' + esc(x._note) + '</td></tr>'; }).join('') +
        '</tbody><tfoot><tr><td>Total</td><td></td><td></td><td></td><td></td><td class="secret">' + money(tv) + '</td><td></td></tr></tfoot></table></div>';
      if (RUN) h += '<div class="row"><b>Placing ' + RUN.done + ' / ' + RUN.n + '</b> · ' + RUN.ok + ' placed' + (RUN.fail.length ? ' · <span class="dn">' + esc(RUN.fail.join(' | ')) + '</span>' : '') + ' <button class="fb sel" data-f="stop">Stop</button></div>';
      else h += '<div class="row"><button class="fb ' + (ARMED ? 'armed' : 'pri') + '" data-f="place">' + (ARMED ? 'Confirm — place these ' + o.reduce((t, x) => t + slices(x, d.cfg.maxLots).length, 0) + ' market orders on Zerodha?' : 'Place orders') + '</button><button class="fb" data-f="cancel">Cancel</button></div>' +
        '<div class="sub" style="margin:4px 0 0">Market orders, NRML, sent one by one ' + (d.cfg.gapMs / 1000) + 's apart, in pieces of at most ' + d.cfg.maxLots + ' lots. Prices shown are the last price — fills can differ.</div>';
    }
    return h + '</div>';
  }
  function render() {
    if (!EL) return;
    const d = docGet(), c = d.cfg, P = POS;
    let h = '<div class="fnw"><h2>F&amp;O plan · B on 15%</h2><div class="sub">' + c.syms.join(' · ') + ' — buy on a ' + c.trig + '% pot fall up to ' + c.buyTo + 'x, sell back to ' + c.maxLev + 'x, buy back ' + c.bb + '% off the low. Orders go out only by your two clicks, or by the auto-pilot while you have it switched on.</div>';
    if (!P) {
      const why = { login: 'Zerodha isn\'t logged in today — log in from the Positions tab.', worker: 'Zerodha isn\'t connected on this device — set it up in the Positions tab.', token: 'This device has no access token — open your private link once.', down: 'Couldn\'t reach Zerodha just now — retrying.' }[POSERR] || 'Loading your F&O positions…';
      EL.innerHTML = h + '<div class="box">' + why + '</div></div>'; return;
    }
    const calc = potCalc(d, P), pos = exposure(P);
    if (!d.rec || (calc && !calc.ok)) {
      h += '<div class="box' + (d.rec ? ' warn' : '') + '"><div class="bt">' + (d.rec ? 'Pot needs today\'s value' : 'Start tracking the pot') + '</div>' +
        (d.rec ? 'The pot can\'t be carried forward from ' + esc(d.rec.date) + ': these quantities changed since then — ' + esc(calc.bad.join(', ')) + '. Type what the pot is worth now.' :
          'You already hold these futures (below). Type what the pot — your own money for this plan only — is worth right now. Optionally type its highest value so far; if left empty, today\'s value is the high.') +
        '<div class="row"><input id="fnPot" placeholder="Pot now, e.g. 2.5 Cr"> ' + (d.rec ? '' : '<input id="fnHigh" placeholder="High so far (optional)">') + '<button class="fb pri" data-f="start">' + (d.rec ? 'Set pot' : 'Start') + '</button></div>' +
        '<div class="sub">Positions now: ' + money(pos) + '. Write amounts like 2.5 Cr, 80 L or 25000000.</div></div>';
      EL.innerHTML = h + posTable(d, P, null) + '</div>'; return;
    }
    const a = calc.pot, st = d.st, H = Math.max(st.H || a, a), dd = 1 - a / H, lev = a > 0 ? pos / a : null;
    h += '<div class="kp">' +
      '<div class="k"><div class="l">Pot value (live)</div><div class="v secret">' + money(a) + '</div><div class="s secret ' + (calc.m2m >= 0 ? 'up' : 'dn') + '">today ' + signed(calc.m2m) + '</div><div class="s secret ' + (pnlSum(P) >= 0 ? 'up' : 'dn') + '">P&amp;L since buy ' + signed(pnlSum(P)) + '</div></div>' +
      '<div class="k"><div class="l">High</div><div class="v secret">' + money(H) + '</div><div class="s">' + (st.Hd ? 'set ' + esc(st.Hd) : 'from setup') + '</div></div>' +
      '<div class="k"><div class="l">Fall from high</div><div class="v ' + (dd * 100 >= c.trig ? 'dn' : '') + '">' + pct(dd) + '</div><div class="s">dip buy at ' + c.trig + '%</div></div>' +
      '<div class="k"><div class="l">Positions</div><div class="v secret">' + money(pos) + '</div><div class="s ' + (lev > c.maxLev ? 'dn' : '') + '">' + (lev != null ? lev.toFixed(2) + 'x of the pot' : '') + '</div></div>' +
      '<div class="k"><div class="l">Room to ' + c.buyTo + 'x</div><div class="v secret">' + money(Math.max(0, c.buyTo * a - pos)) + '</div><div class="s">4x line ' + money(c.maxLev * a) + '</div></div></div>';
    const lines = [];
    if (st.armed) lines.push('Dip buy is <b>armed</b>: it fires if the pot closes at or below <b class="secret">' + money(H * (1 - c.trig / 100)) + '</b> (' + c.trig + '% under the high).');
    else lines.push('Dip buy is <b>used</b> — it re-arms when the pot closes above its high <b class="secret">' + money(H) + '</b>.');
    if (st.pend) lines.push('After the forced sell, a <b>buy-back</b> waits for the pot to close at <b class="secret">' + money(st.low * (1 + c.bb / 100)) + '</b> (' + c.bb + '% over its low ' + money(st.low) + ').');
    lines.push('A forced sell is due if positions close above ' + c.maxLev + 'x — today that is ' + money(c.maxLev * a) + '.');
    lines.push('Last close check: ' + (st.evalDate ? esc(st.evalDate) : 'none yet') + ' (' + (d.auto.on ? 'the auto-pilot checks at 3:20 PM; if it could not, ' : '') + 'runs after 15:35 IST, from this page or the 4 PM phone alert).');
    h += '<div class="box"><div class="bt">Rule status</div>' + lines.map(x => '<div>' + x + '</div>').join('') + '</div>';
    h += autoBox(d);
    (st.due || []).forEach(x => { h += '<div class="box due"><div class="bt">Due since ' + esc(x.d) + '</div>' + esc(x.msg) + ' — <button class="fb pri" data-f="plan" data-k="' + x.k + '">' + ({ dip: 'Prepare dip buy', bb: 'Prepare buy-back', sell: 'Prepare sell to ' + c.maxLev + 'x' }[x.k]) + '</button> <button class="fb" data-f="dismiss" data-k="' + x.k + '">Dismiss</button></div>'; });
    const H2 = heldBySym(P, c.syms), rollSoon = c.syms.some(s => H2[s].all.some(h => h.exp && daysTo(h.exp) <= c.rollDays));
    if (rollSoon) h += '<div class="box due"><div class="bt">Roll window</div>A contract expires within ' + c.rollDays + ' days. Stock futures settle by delivery, so roll before expiry. <button class="fb pri" data-f="plan" data-k="roll">Prepare roll</button></div>';
    h += previewHTML();
    h += '<div class="bt">Positions</div>' + posTable(d, P, a);
    h += '<div class="bt">Actions — each unlocks only when the rule calls for it, and shows the orders first</div><div class="acts2">' +
      ['dip:Dip buy to ' + c.buyTo + 'x', 'bb:Buy back to ' + c.buyTo + 'x', 'sell:Sell to ' + c.maxLev + 'x', 'roll:Roll to next month', 'equal:Make the five equal'].map(x => { const [k, l] = x.split(':'), g = gate(d, P, a, k);
        return '<div class="arow"><button class="fb' + (g.ok ? ' pri' : '') + (PREVIEW && PREVIEW.k === k ? ' on' : '') + '" data-f="plan" data-k="' + k + '"' + (RUN || !g.ok ? ' disabled' : '') + '>' + (g.ok ? '' : '🔒 ') + l + '</button><span class="sub">' + g.why + '</span></div>'; }).join('') + '</div>';
    h += '<details><summary>Add money · reset pot · settings</summary><div class="box">' +
      '<div class="row"><input id="fnAdd" placeholder="Added, e.g. 25 L"><button class="fb" data-f="add">Add money to pot</button><span class="sub" style="margin:0">raises the pot, its high and its low by the same amount, so the fall % is unchanged</span></div>' +
      '<div class="row"><input id="fnPot" placeholder="Pot now"><button class="fb" data-f="start">Set pot value</button><span class="sub" style="margin:0">overrides today\'s value (keeps the high)</span></div>' +
      '<div class="row">Max lots per order <input id="fnML" value="' + c.maxLots + '" style="width:70px"> Roll window (days) <input id="fnRD" value="' + c.rollDays + '" style="width:70px"><button class="fb" data-f="cfg">Save</button></div></div></details>';
    h += '<details><summary>History</summary><div class="box log">' + (d.log.slice().reverse().map(e => '<div>' + new Date(e.t + 330 * 60000).toISOString().slice(0, 16).replace('T', ' ') + ' · ' + esc(e.msg) + '</div>').join('') || 'Nothing yet.') + '</div></details>';
    h += '<div class="sub">Pot carried by Zerodha\'s daily mark-to-market; charges and leverage cost are not deducted here. Contract list ' + esc((CON && CON.asof) || '—') + ' (NSE bhavcopy).</div></div>';
    const fo = document.activeElement && EL.contains(document.activeElement) && document.activeElement.tagName === 'INPUT' ? document.activeElement.id : '';
    if (fo) return;                                  // never repaint over a field the user is typing in
    EL.innerHTML = h;
  }

  // a button is usable only when the rule says so (user 2026-11-11: "should be enabled only when such thing happens")
  function gate(d, P, a, k) {
    const c = d.cfg, st = d.st, due = (st.due || []).map(x => x.k), H = Math.max(st.H || a, a);
    if (k === 'dip') return due.includes('dip') ? { ok: true, why: 'Due now — the pot closed ' + c.trig + '% or more under its high.' }
      : st.armed ? { ok: false, why: 'Unlocks if the pot closes at or below <b class="secret">' + money(H * (1 - c.trig / 100)) + '</b> (' + c.trig + '% under its high ' + money(H) + ').' }
      : { ok: false, why: 'Already used — re-arms once the pot closes above its high ' + money(H) + '.' };
    if (k === 'bb') return due.includes('bb') ? { ok: true, why: 'Due now — the pot is ' + c.bb + '% above its low after the forced sell.' }
      : st.pend ? { ok: false, why: 'Unlocks when the pot closes at <b class="secret">' + money(st.low * (1 + c.bb / 100)) + '</b> (' + c.bb + '% over its low ' + money(st.low) + ').' }
      : { ok: false, why: 'Only after a forced sell to ' + c.maxLev + 'x.' };
    if (k === 'sell') return due.includes('sell') ? { ok: true, why: 'Due now — positions closed above ' + c.maxLev + 'x of the pot.' }
      : { ok: false, why: 'Unlocks if positions close above ' + c.maxLev + 'x of the pot (today ' + money(c.maxLev * a) + '; now ' + money(exposure(P)) + ').' };
    if (k === 'roll') { const Hs = heldBySym(P, c.syms); let near = null;
      c.syms.forEach(s => Hs[s].all.forEach(h => { if (h.exp && h.q > 0 && (!near || h.exp < near)) near = h.exp; }));
      return near && daysTo(near) <= c.rollDays ? { ok: true, why: 'Expiry ' + esc(near) + ' is within ' + c.rollDays + ' days.' }
        : { ok: false, why: 'Unlocks ' + c.rollDays + ' days before expiry' + (near ? ' (' + esc(near) + ' → from ' + new Date(Date.parse(near + 'T00:00:00Z') - c.rollDays * 864e5).toISOString().slice(0, 10) + ')' : '') + '.' }; }
    const e = planEqual(d, P);
    return e.orders.length ? { ok: true, why: 'One or more stocks sit a lot or more below the other four.' } : { ok: false, why: 'All five are within about a lot of each other.' };
  }
  function autoBox(d) {
    const au = d.auto, last = d.log.filter(e => e.k === 'auto').slice(-3).reverse();
    const when = t => new Date(t + 330 * 60000).toISOString().slice(0, 16).replace('T', ' ');
    return '<div class="box' + (au.on ? ' due' : '') + '"><div class="bt">Auto-pilot · ' + (au.on ? 'ON' : 'OFF') + '</div>' +
      (au.on ? '<div>Switched on ' + esc(when(au.at)) + '. Each trading day the cloud robot:</div>' : '<div>When switched on, the cloud robot does the rule\'s orders for you each trading day:</div>') +
      '<div>· <b>2:30 PM</b> — rolls any contract that is 3 trading days or less from expiry (next month\'s lot size).</div>' +
      '<div>· <b>3:20 PM</b> — checks the rule with the live pot and, if due, places the sell to ' + d.cfg.maxLev + 'x, buy-back or dip buy before the 3:30 close.</div>' +
      '<div class="sub" style="margin:4px 0 0">You get a phone alert each time it acts or hits a problem. It needs your daily Zerodha login; your Mac can be off. "Make the five equal" stays a button. ' +
      (au.on ? 'It stays on until you switch it off.' : 'Nothing is placed by itself while it is off.') + '</div>' +
      '<div class="row">' + (au.on ? '<button class="fb" data-f="autooff">Switch OFF</button>' : '<button class="fb pri" data-f="autoon">' + (AUTOARM ? 'Tap again to switch ON' : 'Switch ON') + '</button>') + '</div>' +
      (last.length ? '<div class="sub" style="margin:4px 0 0">' + last.map(e => esc(when(e.t)) + ' · ' + esc(e.msg)).join('<br>') + '</div>' : '') + '</div>';
  }

  /* ---------- auto-pilot (the robot calls these; nothing here runs unless the user switched it ON) ---------- */
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const oline = o => o.transaction_type + ' ' + o.quantity + ' ' + o.tradingsymbol + (o._note ? ' (' + o._note + ')' : '');
  function autoStatus() {
    const d = docGet(), P = POS, calc = P && d.rec ? potCalc(d, P) : null, H = P ? heldBySym(P, d.cfg.syms) : null;
    return { ready: READY, on: !!d.auto.on, started: !!d.rec, potOk: !!(calc && calc.ok), err: POSERR, contracts: P ? Object.keys(P).filter(k => P[k].q).length : 0,
             evalDate: d.st.evalDate, due: (d.st.due || []).map(x => x.k), rollDue: H ? d.cfg.syms.reduce((n, s) => n + H[s].all.filter(h => h.exp && h.q > 0 && tdTo(h.exp) <= 3).length, 0) : 0 };
  }
  // leg 'roll' (14:30) or 'rule' (15:20). dry = plan only, change nothing. anyTime = skip the clock window (tests only).
  async function autoRun(leg, o) {
    o = o || {}; const dry = !!o.dry, res = { leg, dry, done: [], skipped: [], errors: [], note: '' };
    if (RUN) { res.errors.push('orders already running on this page'); return res; }
    await loadContracts(); await loadHolidays(); await pull();
    let d = docGet();
    if (!dry && !d.auto.on) { res.note = 'auto-pilot is OFF'; return res; }
    if (!d.rec) { res.note = 'the pot is not started'; return res; }
    const m = istMin(), win = leg === 'roll' ? [14 * 60, 15 * 60 + 25] : [15 * 60 + 15, 15 * 60 + 28];
    if (!dry && !o.anyTime && (m < win[0] || m > win[1])) { res.note = 'outside the ' + leg + ' window'; return res; }
    let P = await loadPositions(); if (!P) { res.errors.push('Zerodha: ' + ({ login: 'not logged in today', worker: 'no worker', token: 'token refused', down: 'not reachable' }[POSERR] || POSERR)); return res; }
    let calc = potCalc(d, P);
    if (!calc || !calc.ok) { res.errors.push('the pot needs today\'s value (quantities changed: ' + ((calc && calc.bad) || []).join(', ') + ') — set it in the F&O tab'); return res; }
    if (calc.ok && !dry && (!sameQty(d.rec, P) || d.rec.date !== today())) { d.rec = recordFrom(P, calc.pot); save(d, true); }
    const go = async (k, plan) => {
      if (!plan.orders.length) { res.skipped.push(k + ': ' + plan.note); return; }
      if (dry) { res.done.push({ k, note: plan.note, orders: plan.orders.map(oline) }); return; }
      await pull(); if (!docGet().auto.on) { res.note = 'switched OFF before ' + k; return; }
      const R = await sendOrders(k, plan.orders, 'auto-pilot'); RUN = null;
      res.done.push({ k, note: plan.note, ok: R.ok, n: R.n, orders: R.placed }); R.fail.forEach(f => res.errors.push(k + ': ' + f));
    };
    if (leg === 'roll') { await go('roll', planRoll(d, P, h => tdTo(h.exp) <= 3)); return res; }
    if (dry) d = JSON.parse(JSON.stringify(d));
    if (d.st.evalDate !== today()) { evalClose(d, calc.pot, exposure(P), 'auto-pilot 3:20 PM'); if (!dry) save(d, true); }
    const due = (d.st.due || []).filter(x => x.d === today()).map(x => x.k);
    res.note = 'pot ' + money(calc.pot) + ', positions ' + money(exposure(P)) + ' (' + (exposure(P) / calc.pot).toFixed(2) + 'x)' + (due.length ? ', due: ' + due.join(', ') : ', nothing due');
    for (const k of ['sell', 'bb', 'dip']) {
      if (!due.includes(k)) continue;
      if (!dry && res.done.length) { await sleep(4000); P = (await loadPositions()) || P; }       // re-plan from fresh positions
      const dn = dry ? d : docGet(), a = dry ? calc.pot : ((potCalc(dn, P) || {}).pot || calc.pot);
      const rt = ((dn.st.due || []).find(x => x.k === k) || {}).retry;
      const plan = rt && rt.length ? { orders: rt, note: 'retry of the ' + rt.length + ' order(s) refused earlier today' }
                 : k === 'sell' ? planSell4x(dn, P, a) : planBuyUpTo(dn, P, a, k);
      await go(k, plan);
      if (!dry && !plan.orders.length) { const x = docGet(); x.st.due = (x.st.due || []).filter(y => y.k !== k); x.st.ts = Date.now(); logAdd(x, 'auto', 'Auto-pilot ' + k + ': nothing to place — ' + plan.note); save(x, true); }
    }
    return res;
  }
  function autoLog(msg) { const d = docGet(); logAdd(d, 'auto', msg); save(d, true); }

  /* ---------- refresh loop ---------- */
  async function refresh() {
    if (!EL || !document.body.contains(EL)) return;
    await loadContracts(); const P = await loadPositions(); const d = docGet();
    if (P && d.rec) {
      const calc = potCalc(d, P);
      if (calc && calc.ok) {
        const moved = !sameQty(d.rec, P);
        if (moved || d.rec.date !== today() || Date.now() - d.rec.t > 5 * 60000) { d.rec = recordFrom(P, calc.pot); save(d); }
        if (weekday() && istMin() >= 15 * 60 + 35 && d.st.evalDate !== today()) { evalClose(d, calc.pot, exposure(P), 'page'); save(d, true); }
      }
    }
    render();
  }
  function onClick(e) {
    const b = e.target.closest('[data-f]'); if (!b || !EL.contains(b)) return;
    const f = b.dataset.f, d = docGet(), P = POS || {};
    if (f === 'start') {
      const v = parseRs((EL.querySelector('#fnPot') || {}).value); if (!(v > 0)) { toast('Type the pot value, e.g. 2.5 Cr'); return; }
      const hi = parseRs((EL.querySelector('#fnHigh') || {}).value);
      const first = !d.rec; d.rec = recordFrom(P, v);
      if (first) { d.st.H = hi > v ? hi : v; d.st.Hd = hi > v ? '' : today(); d.st.armed = true; d.st.ts = Date.now(); }
      logAdd(d, 'pot', (first ? 'Started' : 'Pot set') + ' at ' + money(v) + (first ? ', high ' + money(d.st.H) : '')); save(d, true); render(); return;
    }
    if (f === 'add') { const v = parseRs((EL.querySelector('#fnAdd') || {}).value); if (!(v > 0) || !d.rec) { toast('Type the amount added, e.g. 25 L'); return; }
      d.rec.pot += v; d.rec.t = Date.now(); d.st.H = (d.st.H || 0) + v; if (d.st.pend && d.st.low != null) d.st.low += v; d.st.ts = Date.now();
      logAdd(d, 'add', 'Added ' + money(v) + ' to the pot'); save(d, true); render(); return; }
    if (f === 'cfg') { const ml = Math.floor(+EL.querySelector('#fnML').value), rd = Math.floor(+EL.querySelector('#fnRD').value);
      if (ml >= 1) d.cfg.maxLots = ml; if (rd >= 0) d.cfg.rollDays = rd; d.cfg.ts = Date.now(); save(d, true); toast('Saved'); render(); return; }
    if (f === 'dismiss') { d.st.due = (d.st.due || []).filter(x => x.k !== b.dataset.k); d.st.ts = Date.now(); logAdd(d, 'dismiss', 'Dismissed ' + b.dataset.k); save(d, true); render(); return; }
    if (f === 'plan') {
      const calc = potCalc(d, P); if (!calc || !calc.ok) return; const k = b.dataset.k, a = calc.pot;
      const g = gate(d, P, a, k); if (!g.ok) { toast(g.why.replace(/<[^>]+>/g, '')); return; }
      const r = k === 'dip' || k === 'bb' ? planBuyUpTo(d, P, a, k) : k === 'sell' ? planSell4x(d, P, a) : k === 'roll' ? planRoll(d, P) : planEqual(d, P);
      PREVIEW = Object.assign({ k, title: { dip: 'Dip buy', bb: 'Buy-back', sell: 'Sell to ' + d.cfg.maxLev + 'x', roll: 'Roll to next month', equal: 'Make the five equal' }[k] }, r);
      ARMED = false; render(); return;
    }
    if (f === 'autoon') {
      if (!AUTOARM) { AUTOARM = true; clearTimeout(AUTOT); AUTOT = setTimeout(() => { AUTOARM = false; render(); }, 8000); render(); return; }
      AUTOARM = false; clearTimeout(AUTOT); d.auto = { ts: Date.now(), on: true, at: Date.now() };
      logAdd(d, 'auto', 'Auto-pilot switched ON by you'); save(d, true); toast('Auto-pilot is ON', 4000); render(); return;
    }
    if (f === 'autooff') { d.auto = { ts: Date.now(), on: false, at: Date.now() }; logAdd(d, 'auto', 'Auto-pilot switched OFF by you'); save(d, true); toast('Auto-pilot is OFF', 4000); render(); return; }
    if (f === 'cancel') { PREVIEW = null; ARMED = false; render(); return; }
    if (f === 'stop') { if (RUN) RUN.stop = true; return; }
    if (f === 'place') {
      if (!ARMED) { ARMED = true; clearTimeout(ARMT); ARMT = setTimeout(() => { ARMED = false; render(); }, 8000); render(); return; }
      ARMED = false; clearTimeout(ARMT); placeAll(PREVIEW.k);
    }
  }
  async function mount(el, api) {
    API = api; css();
    if (EL !== el) { EL = el; el.addEventListener('click', onClick); }
    render(); await pull(); await loadHolidays(); await refresh(); READY = true;
    clearInterval(TIMER); TIMER = setInterval(() => { if (!document.hidden && EL && EL.offsetParent !== null && !RUN) refresh(); }, 20000);
  }
  window.mountFno = mount;
  window.fnoAuto = { status: autoStatus, run: autoRun, log: autoLog, flush: () => push() };
  window.__fnoTest = { tdTo, setHol: h => { HOL = new Set(h); }, potCalc, evalClose, planBuyUpTo, planSell4x, planRoll, planEqual, merge, parseRs, slices, blank, setCon: c => { CON = c; } };
})();
