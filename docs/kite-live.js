/* Zerodha live prices for owner pages (terminal, portfolio, ideas) — user 2026-10-13 "zerodha live prices ...
   which is not 15 min delayed".

   How: the owner's Kite worker (localStorage.pf_kite_worker, token localStorage.pf_token sent as X-PF-Token) prices a
   1-share CNC MARKET buy through Kite's order-margin calculator at the live LTP — the free Kite 'Personal' app refuses
   /quote but allows this (terminal.html livePx, measured 2026-09-29). Works for NSE and BSE.

   Zerodha's name for a stock: NSE trade-for-trade (BE) and SME (SM) stocks trade as SYMBOL-BE / SYMBOL-SM — the plain
   name is refused (HFCL-BE, 2026-09-28). The list of non-EQ NSE symbols is docs/nse_series.json (rebuilt every evening
   by scripts/ideas/breakout.py). A BSE code is turned into BSE's trading symbol from docs/bse_universe.json.

   API:  swKite.connected()                       -> true when this device has a worker URL and a token
         swKite.prices(items, {ref})              -> Promise<{prices: {id: ltp}, state, n, total, miss: [{id, why}], st, msg}>
           items: [{id, ex: 'NSE'|'BSE', sym}]    sym = NSE symbol, BSE symbol or BSE code
           ref:   {id: price} optional — a Kite price 20%+ away from it is distrusted (same guard as the terminal)
   A basket Kite refuses is retried in tens, then one stock at a time, so one symbol Kite rejects never costs the rest. */
(function () {
  var SITE = 'https://dhruvan246.github.io/stocks-dashboard/';
  var SERIES = null, BSESYM = null;
  function cfg() {
    try { return { w: (localStorage.getItem('pf_kite_worker') || '').replace(/\/+$/, ''), t: localStorage.getItem('pf_token') || '' }; }
    catch (e) { return { w: '', t: '' }; }
  }
  function getJSON(u) { return fetch(u, { cache: 'no-store' }).then(function (r) { return r.ok ? r.json() : null; }).catch(function () { return null; }); }
  function loadSeries() {
    if (SERIES) return Promise.resolve(SERIES);
    return getJSON(SITE + 'nse_series.json?t=' + Date.now()).then(function (j) { SERIES = (j && j.s) || {}; return SERIES; });
  }
  function loadBse() {
    if (BSESYM) return Promise.resolve(BSESYM);
    return getJSON(SITE + 'bse_universe.json').then(function (j) {
      BSESYM = {}; ((j && j.rows) || []).forEach(function (r) { BSESYM[String(r[0])] = r[1]; }); return BSESYM;
    });
  }
  function kiteName(it) {
    var s = String(it.sym || '').toUpperCase();
    if (it.ex === 'BSE') return /^\d+$/.test(s) ? (BSESYM[s] || s) : s;
    var ser = SERIES[s];
    return ser && ser !== 'EQ' ? s + '-' + ser : s;
  }
  function call(c, items) {
    var body = items.map(function (it) {
      return { exchange: it.ex, tradingsymbol: it._k, transaction_type: 'BUY', variety: 'regular', product: 'CNC', order_type: 'MARKET', quantity: 1 };
    });
    return fetch(c.w + '/margincalc', { method: 'POST', cache: 'no-store', headers: { 'Content-Type': 'application/json', 'X-PF-Token': c.t }, body: JSON.stringify(body) })
      .then(function (r) { return r.json().then(function (j) { return { st: r.status, j: j }; }, function () { return { st: r.status, j: null }; }); })
      .then(function (x) { return { st: x.st, data: (x.j && x.j.data) || [], msg: (x.j && (x.j.message || x.j.error)) || '' }; })
      .catch(function (e) { return { st: 0, data: [], msg: String(e).slice(0, 80) }; });
  }
  async function prices(items, opts) {
    opts = opts || {};
    var ref = opts.ref || {}, c = cfg(), out = { prices: {}, state: 'none', n: 0, total: 0, miss: [] };
    if (!c.w || !c.t) return out;
    var seen = {}, list = [];
    items.forEach(function (it) { if (it && it.sym && !seen[it.id]) { seen[it.id] = 1; list.push({ id: it.id, ex: it.ex === 'BSE' ? 'BSE' : 'NSE', sym: it.sym }); } });
    out.total = list.length;
    if (!list.length) return out;
    await loadSeries();
    if (list.some(function (it) { return it.ex === 'BSE' && /^\d+$/.test(String(it.sym)); })) await loadBse();
    list.forEach(function (it) { it._k = kiteName(it); });
    var why = {}, lastSt = 0, lastMsg = '';
    function take(part, data) {
      part.forEach(function (it, k) {
        var d = data[k], v = d ? Math.round((+d.var || +d.total || 0) * 100) / 100 : 0, r = ref[it.id];
        if (d && d.tradingsymbol && d.tradingsymbol !== it._k) { why[it.id] = 'answer out of order'; return; }
        if (v > 0 && !(r > 0 && Math.abs(v / r - 1) > 0.2)) { out.prices[it.id] = v; out.n++; }
        else why[it.id] = v > 0 ? 'Kite price ' + v + ' is 20%+ away from ' + r : 'no price in Kite\'s answer';
      });
    }
    for (var i = 0; i < list.length; i += 40) {
      var part = list.slice(i, i + 40), r = await call(c, part);
      if (r.st === 200) { take(part, r.data); continue; }
      lastSt = r.st; lastMsg = r.msg;
      if (r.st === 401 || r.st === 403 || r.st === 0) break;          // token / Kite session / worker down: retrying will not help
      for (var k = 0; k < part.length; k += 10) {
        var sub = part.slice(k, k + 10), r2 = await call(c, sub);
        if (r2.st === 200) { take(sub, r2.data); continue; }
        for (var m = 0; m < sub.length; m++) {
          var r1 = await call(c, [sub[m]]);
          if (r1.st === 200) take([sub[m]], r1.data);
          else { lastSt = r1.st; lastMsg = r1.msg; why[sub[m].id] = (r1.st ? 'HTTP ' + r1.st : '') + (r1.msg ? ': ' + r1.msg : ''); }
        }
      }
    }
    list.forEach(function (it) { if (out.prices[it.id] == null) out.miss.push({ id: it.id, name: it._k, why: why[it.id] || (lastSt ? 'HTTP ' + lastSt + (lastMsg ? ': ' + lastMsg : '') : 'not priced') }); });
    out.state = out.n ? 'ok' : 'fail'; out.st = lastSt; out.msg = lastMsg;
    return out;
  }
  window.swKite = { connected: function () { var c = cfg(); return !!(c.w && c.t); }, prices: prices };
})();
