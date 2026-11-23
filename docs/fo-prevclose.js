/* Futures "previous close" that does not move after the market shuts (user 2026-11-23: "terminal is showing wrong
   pnl today").  Zerodha's /positions m2m for an overnight future = qty x (ltp - close_price), and AFTER THE CLOSE
   Zerodha swaps close_price to TODAY's close — on 23-Nov (Nov expiry day) it had already happened by 15:50, so
   BHEL Dec showed close 418.50 (today) instead of 435.40 (Friday's settlement) and the five futures read +2.6 L
   for a day that really lost 17.7 L.  This rebuilds the right base from NSE's own bhavcopy settlement
   (docs/fno_contracts.json: settle of its asof day, and prev.s = the day before) for the trading day before
   today, and corrects only the overnight part:  m2m_true = m2m + overnightQty x (kiteClose - prevSettle).
   When Zerodha's close is still right the correction is 0.  Unknown base (file stale, non-trading day, before
   09:15) -> Zerodha's own numbers, unchanged.  Used by terminal.html (account view) and fno-panel.js;
   ~/stocks-cache/zerodha-ACH754/fo_prevclose.py mirrors it for the 15:50 / 4 PM alerts. */
(function () {
  var CON = null, HOL = null, P = null;
  function ist() { return new Date(Date.now() + 330 * 60000); }
  function trading(iso) { var g = new Date(iso + 'T00:00:00Z').getUTCDay(); return g !== 0 && g !== 6 && !(HOL && HOL.has(iso)); }
  function prevTD(iso) { var t = Date.parse(iso + 'T00:00:00Z'); for (var i = 0; i < 15; i++) { t -= 864e5; var d = new Date(t).toISOString().slice(0, 10); if (trading(d)) return d; } return ''; }
  function baseMap(con, todayIso, mins) {
    if (!con || !con.f || !trading(todayIso) || mins < 9 * 60 + 15) return null;
    var pd = prevTD(todayIso), m = {};
    if (con.asof === pd) { for (var s in con.f) con.f[s].forEach(function (c) { if (c[3] > 0) m[c[0]] = c[3]; }); return m; }
    if (con.asof === todayIso && con.prev && con.prev.asof === pd) return con.prev.s || null;
    return null;
  }
  function map() { var d = ist(); return baseMap(CON, d.toISOString().slice(0, 10), d.getUTCHours() * 60 + d.getUTCMinutes()); }
  /* one Kite net-position row -> {m2m, close}: the corrected day m2m and the base it is measured from */
  function fix(r, mp) {
    var m2m = r.m2m != null ? +r.m2m : null, kc = +r.close_price || 0;
    if (r.exchange !== 'NFO' || m2m == null) return { m2m: m2m, close: kc };
    mp = mp === undefined ? map() : mp;
    var pc = mp && mp[r.tradingsymbol], oq = +r.overnight_quantity || 0;
    if (!(pc > 0) || !(kc > 0) || !oq) return { m2m: m2m, close: kc };
    return { m2m: m2m + oq * (kc - pc), close: pc };
  }
  function load() {
    if (P) return P;
    P = Promise.all([
      fetch('./fno_contracts.json', { cache: 'no-store' }).then(function (r) { return r.ok ? r.json() : null; }).catch(function () { return null; }),
      fetch('./nse_holidays.json', { cache: 'no-store' }).then(function (r) { return r.ok ? r.json() : null; }).catch(function () { return null; })
    ]).then(function (a) {
      CON = a[0];
      var h = new Set(); try { Object.values((a[1] && a[1].holidays) || {}).forEach(function (y) { Object.keys(y).forEach(function (k) { h.add(k); }); }); } catch (e) {}
      HOL = h; return true;
    });
    return P;
  }
  window.foPrevClose = { load: load, fix: fix, map: map, _baseMap: baseMap, _set: function (c, h) { CON = c; HOL = new Set(h || []); P = Promise.resolve(true); } };
})();
