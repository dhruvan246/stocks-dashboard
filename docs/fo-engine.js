/* fo-engine.js — EOD index-options backtest engine (options-backtest.html).
 *
 * Data: docs/fo/{SYM}_{YYYY}.bin slices built by scripts/build_fo_store.py from
 * NSE F&O bhavcopy (real exchange EOD closes/settles per strike) + official NSE
 * index OHLC. DAILY granularity: entries/exits happen at day CLOSE, SL/TP checked
 * once per day (close mode) or against the day's option high/low (hilo mode).
 * No minute data exists here — intraday-time features are surfaced as disabled.
 *
 * All prices ₹ (floats, converted from paise ints). P&L in ₹ using dated lot sizes.
 */
(function () {
  'use strict';

  // ---- dated lot-size schedules (as StockMock's settings modal lists them; user-overridable)
  var LOTS = {
    NIFTY: [['2021-07-22', 75], ['2024-04-25', 50], ['2025-12-26', 25], ['2025-12-30', 75], ['9999', 65]],
    BANKNIFTY: [['2018-10-25', 40], ['2020-07-22', 20], ['2023-07-20', 25], ['2025-01-29', 15], ['2025-06-26', 30], ['2025-12-30', 35], ['9999', 30]],
    FINNIFTY: [['2024-07-23', 40], ['2025-01-28', 25], ['2025-12-30', 65], ['9999', 60]],
    MIDCPNIFTY: [['2024-07-22', 75], ['2025-01-27', 50], ['2025-06-26', 120], ['2025-12-30', 140], ['9999', 120]]
  };
  function lotSize(sym, date, override) {
    if (override) return override;
    var sch = LOTS[sym] || [['9999', 1]];
    for (var i = 0; i < sch.length; i++) if (date <= sch[i][0]) return sch[i][1];
    return sch[sch.length - 1][1];
  }

  // ---- binary slice parsing --------------------------------------------------
  var cache = {};            // 'SYM_YYYY' -> array of day objects
  function parseSlice(buf) {
    var dv = new DataView(buf);
    if (String.fromCharCode(dv.getUint8(0), dv.getUint8(1), dv.getUint8(2), dv.getUint8(3)) !== 'FOB1')
      throw new Error('bad magic');
    var hlen = dv.getUint32(4, true);
    var hdr = JSON.parse(new TextDecoder().decode(new Uint8Array(buf, 8, hlen)));
    var body = new Int32Array(buf.slice(8 + hlen));
    var out = [], p = 0;
    for (var di = 0; di < hdr.dates.length; di++) {
      var exps = [];
      var dh = hdr.days[di];
      for (var ei = 0; ei < dh.length; ei++) {
        var exp = dh[ei][0], fut = dh[ei][1], n = dh[ei][2];
        var strikes = [], map = {};
        for (var si = 0; si < n; si++) {
          var k = body[p] / 100;
          var ce = body[p + 3] >= 0 ? { h: body[p + 1] / 100, l: body[p + 2] / 100, c: body[p + 3] / 100, s: body[p + 4] / 100, v: body[p + 5], oi: body[p + 6] } : null;
          var pe = body[p + 9] >= 0 ? { h: body[p + 7] / 100, l: body[p + 8] / 100, c: body[p + 9] / 100, s: body[p + 10] / 100, v: body[p + 11], oi: body[p + 12] } : null;
          strikes.push(k); map[k] = { ce: ce, pe: pe };
          p += 13;
        }
        exps.push({
          exp: exp, n: n, strikes: strikes, map: map,
          fut: fut && fut.length ? { h: fut[0] / 100, l: fut[1] / 100, c: fut[2] / 100, s: fut[3] / 100, v: fut[4], oi: fut[5] } : null
        });
      }
      out.push({
        date: hdr.dates[di],
        spot: hdr.spot[di] / 100 || null,
        spotO: (hdr.spotO && hdr.spotO[di] / 100) || null,
        spotH: (hdr.spotH && hdr.spotH[di] / 100) || null,
        spotL: (hdr.spotL && hdr.spotL[di] / 100) || null,
        exps: exps
      });
    }
    return out;
  }

  function fetchSlice(sym, year) {
    var key = sym + '_' + year;
    if (cache[key]) return Promise.resolve(cache[key]);
    return fetch('fo/' + key + '.bin.gz').then(function (r) {
      if (!r.ok) return [];               // year not in store (pre-listing etc.)
      // stored gzipped (Pages doesn't compress .bin); browsers inflate natively
      var stream = r.body.pipeThrough(new DecompressionStream('gzip'));
      return new Response(stream).arrayBuffer().then(function (b) { cache[key] = parseSlice(b); return cache[key]; });
    });
  }

  // load range -> {days:[...sorted], byDate:{}} ; onProg(loaded,total)
  function loadRange(sym, fromIso, toIso, onProg) {
    var y0 = +fromIso.slice(0, 4), y1 = +toIso.slice(0, 4);
    var years = []; for (var y = y0; y <= y1; y++) years.push(y);
    var done = 0;
    return Promise.all(years.map(function (y) {
      return fetchSlice(sym, y).then(function (d) { done++; if (onProg) onProg(done, years.length); return d; });
    })).then(function (parts) {
      var days = [];
      parts.forEach(function (p) {
        p.forEach(function (d) { if (d.date >= fromIso && d.date <= toIso) days.push(d); });
      });
      days.sort(function (a, b) { return a.date < b.date ? -1 : 1; });
      var byDate = {}; days.forEach(function (d, i) { d.idx = i; byDate[d.date] = d; });
      return { days: days, byDate: byDate };
    });
  }

  // ---- per-day helpers -------------------------------------------------------
  function futExps(day) {                 // expiries >= day, sorted
    return day.exps.filter(function (e) { return e.exp >= day.date; });
  }
  function classifyExpiry(day, which) {   // 'w' | 'nw' | 'm'
    var ex = futExps(day);
    if (!ex.length) return null;
    if (which === 'w') return ex[0];
    if (which === 'nw') return ex[1] || null;
    var m0 = ex[0].exp.slice(0, 7), best = null;
    ex.forEach(function (e) { if (e.exp.slice(0, 7) === m0) best = e; });
    return best;                          // last expiry inside nearest expiry's month
  }
  function atmBasis(day, basis, expObj) {
    if (basis === 'fut') {
      if (expObj && expObj.fut) return expObj.fut.c;
      var ex = futExps(day);
      for (var i = 0; i < ex.length; i++) if (ex[i].fut) return ex[i].fut.c;
      return day.spot;
    }
    return day.spot || (expObj && expObj.fut ? expObj.fut.c : null);
  }
  function nearestStrike(expObj, target) {
    var best = null, bd = Infinity;
    for (var i = 0; i < expObj.strikes.length; i++) {
      var d = Math.abs(expObj.strikes[i] - target);
      if (d < bd) { bd = d; best = expObj.strikes[i]; }
    }
    return best;
  }
  function q(expObj, k, t) { var r = expObj.map[k]; return r ? (t === 'CE' ? r.ce : r.pe) : null; }
  function straddlePrem(day, expObj, basis) {
    var atm = nearestStrike(expObj, atmBasis(day, basis, expObj));
    if (atm == null) return null;
    var c = q(expObj, atm, 'CE'), p = q(expObj, atm, 'PE');
    return (c && p && c.c > 0 && p.c > 0) ? c.c + p.c : null;
  }
  // resolve a leg's strike on entry day. returns {k, quote} or null
  function resolveStrike(day, expObj, leg, basis, naIsTradedOnly) {
    if (leg.type === 'FUT') return expObj.fut ? { k: 0, quote: expObj.fut } : null;
    var t = leg.type, k = null;
    var base = atmBasis(day, basis, expObj);
    if (base == null) return null;
    if (leg.method === 'atm') {                       // ATM point offset (₹ points)
      k = nearestStrike(expObj, base + (+leg.val || 0));
    } else if (leg.method === 'atm_p') {              // ATM percent offset
      k = nearestStrike(expObj, base * (1 + (+leg.val || 0) / 100));
    } else if (leg.method === 'atm_sp') {             // Straddle-width offset: base ± val×SP
      var sp0 = straddlePrem(day, expObj, basis);
      if (sp0 == null) return null;
      k = nearestStrike(expObj, base + (+leg.val || 0) * sp0);
    } else if (leg.method === 'cp' || leg.method === 'sp') {
      var target = +leg.val || 0;
      if (leg.method === 'sp') {
        var sp = straddlePrem(day, expObj, basis);
        if (sp == null) return null;
        target = sp * target / 100;                   // % of straddle premium
      }
      var best = null, bd = Infinity;
      for (var i = 0; i < expObj.strikes.length; i++) {
        var qq = q(expObj, expObj.strikes[i], t);
        if (!qq || qq.c <= 0 || (naIsTradedOnly && !qq.v)) continue;
        var cmp = leg.hcmp;                           // hedge comparators reuse this fn
        if (cmp === '>=' && qq.c < target) continue;
        if (cmp === '<=' && qq.c > target) continue;
        var d = Math.abs(qq.c - target);
        if (d < bd) { bd = d; best = expObj.strikes[i]; }
      }
      k = best;
    }
    if (k == null) return null;
    var quote = q(expObj, k, t);
    if (!quote || quote.c <= 0) return null;
    if (naIsTradedOnly && !quote.v) return null;      // untraded => NA
    return { k: k, quote: quote };
  }

  // ---- execution -------------------------------------------------------------
  // cfg: see page. Returns {trades, stats, daily}
  function run(cfg, data, vixMap) {
    var days = data.days;
    var naStrict = cfg.naStrict !== false;            // vol==0 ⇒ NA (default on)
    var slip = (cfg.slippage != null ? cfg.slippage : 0.5) / 100;
    var trades = [];
    var openUntil = -1;                               // non-overlapping trades

    function entryFilterOK(day, i) {
      var dow = new Date(day.date + 'T00:00:00').getDay();     // 1..5
      if (cfg.entryDays && cfg.entryDays.length && cfg.entryDays.indexOf(dow) < 0) return false;
      if (cfg.vix && vixMap) {
        var v = vixMap[day.date];
        if (v != null && (v < cfg.vix[0] || v > cfg.vix[1])) return false;
        if (v == null && cfg.vixStrict) return false;
      }
      if (cfg.gapFilters && cfg.gapFilters.length) {
        var prev = i > 0 ? days[i - 1] : null;
        for (var fi = 0; fi < cfg.gapFilters.length; fi++) {
          var f = cfg.gapFilters[fi], val = null;
          if (f.field === 'gap') {
            if (!prev || !day.spotO || !prev.spot) return false;
            val = (day.spotO - prev.spot) / prev.spot * 100;
          } else if (f.field === 'pdhl') {            // vs prev-day High/Low band
            if (!prev || !prev.spotH || !day.spotO) return false;
            val = day.spotO > prev.spotH ? (day.spotO - prev.spotH) / prev.spotH * 100 :
                  day.spotO < prev.spotL ? (day.spotO - prev.spotL) / prev.spotL * 100 : 0;
          } else if (f.field === 'spotchg') {
            if (!prev || !prev.spot || !day.spot) return false;
            val = (day.spot - prev.spot) / prev.spot * 100;
          }
          if (val == null) return false;
          if (f.cmp === '>=' ? val < f.v : val > f.v) return false;
        }
      }
      if (cfg.entryDTE != null && cfg.entryDTE !== '') {
        var eo = classifyExpiry(day, (cfg.legs[0] && cfg.legs[0].expiry) || 'w');
        if (!eo) return false;
        if (dteDays(day.date, eo.exp) !== +cfg.entryDTE) return false;
      }
      return true;
    }

    function dteDays(d0, d1) {            // trading-day distance via store index
      var a = data.byDate[d0], b = data.byDate[d1];
      if (a && b) return b.idx - a.idx;
      var n = 0, t = new Date(d0), e = new Date(d1);   // fallback: weekday count
      while (t < e) { t.setDate(t.getDate() + 1); var w = t.getDay(); if (w !== 0 && w !== 6) n++; }
      return n;
    }

    for (var i = 0; i < days.length; i++) {
      if (i <= openUntil) continue;
      var day = days[i];
      if (!entryFilterOK(day, i)) continue;
      var t = tryEnter(day, i);
      if (t) { trades.push(t); openUntil = t.exitIdx; }
    }

    function legEntryPrice(quote, side) {             // slippage: worse fill
      return side === 'B' ? quote.c * (1 + slip) : quote.c * (1 - slip);
    }
    function legExitPrice(px, side) {
      return side === 'B' ? px * (1 - slip) : px * (1 + slip);
    }

    function mkLeg(day, legCfg, basis) {
      var expObj = classifyExpiry(day, legCfg.expiry || 'w');
      if (!expObj) return null;
      var rs = resolveStrike(day, expObj, legCfg, basis, naStrict);
      if (!rs) return null;
      var entry = legEntryPrice(rs.quote, legCfg.side);
      var leg = {
        cfg: legCfg, exp: expObj.exp, k: rs.k, type: legCfg.type, side: legCfg.side,
        lots: +legCfg.lots || 1, entry: entry, entryDate: day.date,
        exitPx: null, exitDate: null, exitWhy: null, closed: false,
        reLeft: legCfg.re ? +legCfg.re.n : 0, cost: entry,
        slPx: null, tpPx: null, trailAnchor: entry, marks: []
      };
      setLevels(leg, day);
      return leg;
    }
    function setLevels(leg, day) {
      var c = leg.cfg;
      if (c.sl) {
        if (c.sl.u === '%') leg.slPx = leg.side === 'S' ? leg.cost * (1 + c.sl.v / 100) : leg.cost * (1 - c.sl.v / 100);
        else if (c.sl.u === 'pt') leg.slPx = leg.side === 'S' ? leg.cost + +c.sl.v : leg.cost - +c.sl.v;
        else if (c.sl.u === 'spot%' || c.sl.u === 'spotpt') {
          var s0 = day.spot; leg.slSpot = { u: c.sl.u, v: +c.sl.v, base: s0 };
          leg.slPx = null;
        }
      }
      if (c.tp) {
        if (c.tp.u === '%') leg.tpPx = leg.side === 'S' ? leg.cost * (1 - c.tp.v / 100) : leg.cost * (1 + c.tp.v / 100);
        else leg.tpPx = leg.side === 'S' ? leg.cost - +c.tp.v : leg.cost + +c.tp.v;
      }
    }

    function dayQuote(day, leg) {
      for (var ei = 0; ei < day.exps.length; ei++) {
        if (day.exps[ei].exp !== leg.exp) continue;
        if (leg.type === 'FUT') return day.exps[ei].fut;
        return q(day.exps[ei], leg.k, leg.type);
      }
      return null;
    }

    function tryEnter(day, i) {
      var basis = cfg.basis || 'spot';
      var legs = [], hedges = [];
      for (var li = 0; li < cfg.legs.length; li++) {
        var lc = cfg.legs[li];
        var leg = mkLeg(day, lc, basis);
        if (!leg) { if (naStrict) return null; else continue; }   // NA strike => skip day
        leg.id = 'L' + (li + 1);
        legs.push(leg);
        if (lc.hedge) {
          var hc = {
            lots: lc.lots, side: lc.side === 'S' ? 'B' : 'S', type: lc.type,
            method: 'cp', val: lc.hedge.prem, hcmp: lc.hedge.cmp,
            expiry: lc.hedge.expiry || lc.expiry, sl: null, tp: null, re: null
          };
          var hl = mkLeg(day, hc, basis);
          if (hl) { hl.id = leg.id + 'H'; hl.isHedge = true; hl.parent = leg.id; hedges.push(hl); }
        }
      }
      if (!legs.length) return null;
      var all = legs.concat(hedges);
      var lot = lotSize(cfg.index, day.date, cfg.lotOverride);
      var maxExp = all.reduce(function (m, l) { return l.exp > m ? l.exp : m; }, '0');

      var trade = {
        entryDate: day.date, entryIdx: i, expiry: maxExp,
        dte: dteDays(day.date, legs[0].exp), lot: lot,
        vix: vixMap ? vixMap[day.date] : null,
        spotIn: day.spot, legs: all, pnl: 0, maxUp: 0, maxDn: 0,
        exitDate: null, exitIdx: null, exitWhy: '-', daily: []
      };

      var protectFloor = null;    // ₹ locked
      var stratTrailAnchor = 0;

      function legDir(l) { return l.side === 'B' ? 1 : -1; }
      function legPnl(l, px) { return (px - l.entry) * legDir(l) * l.lots * lot; }
      function openPnl(dayD, marks) {
        var s = 0;
        all.forEach(function (l) {
          if (l.closed) { s += legPnl(l, l.exitPx); return; }
          var qd = marks[l.id];
          if (qd != null) s += legPnl(l, qd);
        });
        return s;
      }
      var entryCP = 0;
      legs.forEach(function (l) { if (l.type !== 'FUT') entryCP += l.entry; });

      // walk forward
      var lastMarkable = i;
      for (var j = i + 1; j < days.length; j++) {
        var d = days[j];
        if (d.date > maxExp) break;
        lastMarkable = j;
        var marks = {}, quotes = {};
        all.forEach(function (l) {
          var qd = dayQuote(d, l);
          quotes[l.id] = qd;
          if (qd) {
            if (d.date === l.exp && l.type !== 'FUT') {
              // bhavcopy settle on expiry day = UNDERLYING final settlement (measured
              // in both eras) -> option exit value is intrinsic at that level.
              var S = qd.s || d.spot || 0;
              marks[l.id] = l.type === 'CE' ? Math.max(0, S - l.k) : Math.max(0, l.k - S);
            } else if (d.date === l.exp) {
              marks[l.id] = qd.s || qd.c;          // futures final settlement
            } else {
              marks[l.id] = qd.c;
            }
          }
          else if (l.marks.length) marks[l.id] = l.marks[l.marks.length - 1];   // carry last
          else marks[l.id] = l.entry;
          if (!l.closed) l.marks.push(marks[l.id]);
        });
        var hitAny = false, exitAll = null;

        // per-leg SL/TP/trail
        for (var li2 = 0; li2 < all.length; li2++) {
          var l = all[li2];
          if (l.closed || l.isHedge) continue;
          var qd = quotes[l.id], px = marks[l.id];
          var hi = qd ? qd.h : px, lo = qd ? qd.l : px;
          var useHiLo = cfg.slCheck === 'hilo' && qd && (qd.h || qd.l);
          var why = null, fill = px;

          // spot-based SL
          if (l.slSpot && d.spot) {
            var mv = l.slSpot.u === 'spot%' ? Math.abs(d.spot - l.slSpot.base) / l.slSpot.base * 100
                                            : Math.abs(d.spot - l.slSpot.base);
            if (mv >= l.slSpot.v) { why = 'SL'; fill = px; }
          }
          if (!why && l.slPx != null) {
            if (l.side === 'S' ? (useHiLo ? hi >= l.slPx : px >= l.slPx)
                               : (useHiLo ? lo <= l.slPx : px <= l.slPx)) {
              why = 'SL'; fill = useHiLo ? l.slPx : px;
            }
          }
          if (!why && l.tpPx != null) {
            if (l.side === 'S' ? (useHiLo ? lo <= l.tpPx : px <= l.tpPx)
                               : (useHiLo ? hi >= l.tpPx : px >= l.tpPx)) {
              why = 'TP'; fill = useHiLo ? l.tpPx : px;
            }
          }
          // trailing SL: every X% favourable move => move SL Y%
          if (!why && l.cfg.trail && l.slPx != null) {
            var fav = l.side === 'S' ? (l.trailAnchor - px) / l.trailAnchor * 100
                                     : (px - l.trailAnchor) / l.trailAnchor * 100;
            var steps = Math.floor(fav / l.cfg.trail.x);
            if (steps > 0) {
              var mvSl = l.slPx * (l.cfg.trail.y / 100) * steps;
              l.slPx = l.side === 'S' ? l.slPx - mvSl : l.slPx + mvSl;
              l.trailAnchor = px;
            }
          }
          if (why) {
            hitAny = true;
            l.closed = true; l.exitPx = legExitPrice(fill, l.side); l.exitDate = d.date; l.exitWhy = why;
            if (why === 'SL' && cfg.moveSLtoCost) {
              all.forEach(function (o) { if (!o.closed && !o.isHedge && o.id !== l.id) { o.slPx = o.cost; } });
            }
            if (why === 'SL' && l.reLeft > 0) {
              // re-entry next day close (EOD adaptation)
              var nd = days[j + 1];
              if (nd && nd.date <= l.exp) {
                var mode = l.cfg.re.mode;
                var nq = null, nk = l.k;
                if (mode === 'REI') {
                  var eo2 = null;
                  for (var xx = 0; xx < nd.exps.length; xx++) if (nd.exps[xx].exp === l.exp) eo2 = nd.exps[xx];
                  if (eo2) { var rs2 = resolveStrike(nd, eo2, l.cfg, basis, naStrict); if (rs2) { nk = rs2.k; nq = rs2.quote; } }
                } else {
                  var tmp = { exp: l.exp, k: l.k, type: l.type };
                  nq = dayQuote(nd, tmp);
                }
                if (nq && nq.c > 0 && (!naStrict || nq.v)) {
                  var nl = {
                    cfg: l.cfg, exp: l.exp, k: nk, type: l.type, side: l.side, lots: l.lots,
                    entry: legEntryPrice(nq, l.side), entryDate: nd.date,
                    exitPx: null, exitDate: null, exitWhy: null, closed: false,
                    reLeft: l.reLeft - 1, cost: 0, slPx: null, tpPx: null,
                    trailAnchor: 0, marks: [], id: l.id + 'R', isRe: true, parent: l.id
                  };
                  nl.cost = mode === 'RECOST' ? nl.entry : l.cost;
                  nl.trailAnchor = nl.entry;
                  setLevels(nl, nd);
                  if (mode !== 'RECOST') { nl.cost = l.cost; setLevels(nl, nd); nl.cost = nl.entry; }
                  all.push(nl);
                }
              }
            }
            if (cfg.squareOff === 'all') exitAll = why;
          }
        }
        // close hedges whose parent closed and no live re-entry of parent
        all.forEach(function (h) {
          if (!h.isHedge || h.closed) return;
          var liveParent = all.some(function (o) { return !o.closed && !o.isHedge && (o.id === h.parent || o.parent === h.parent); });
          if (!liveParent) { h.closed = true; h.exitPx = legExitPrice(marks[h.id], h.side); h.exitDate = d.date; h.exitWhy = 'HG'; }
        });

        var pnlNow = openPnl(d, marks);
        trade.daily.push({ date: d.date, pnl: pnlNow, spot: d.spot });
        if (pnlNow > trade.maxUp) trade.maxUp = pnlNow;
        if (pnlNow < trade.maxDn) trade.maxDn = pnlNow;

        // strategy-level TP/SL (₹ MTM or combined-premium %)
        function stratHit(kind, c) {
          if (!c) return false;
          if (c.u === 'mtm') return kind === 'tp' ? pnlNow >= +c.v : pnlNow <= -Math.abs(+c.v);
          var cpNow = 0;
          legs.forEach(function (l) { if (l.type !== 'FUT') cpNow += l.closed ? l.exitPx : (marks[l.id] || 0); });
          var chg = (cpNow - entryCP) / entryCP * 100;
          return kind === 'tp' ? (legs[0].side === 'S' ? chg <= -c.v : chg >= +c.v)
                               : (legs[0].side === 'S' ? chg >= +c.v : chg <= -c.v);
        }
        if (!exitAll && stratHit('tp', cfg.stratTP)) exitAll = 'MTM TP';
        if (!exitAll && stratHit('sl', cfg.stratSL)) exitAll = 'MTM SL';

        // protect-the-profits
        if (!exitAll && cfg.protect) {
          var P = cfg.protect;
          if (protectFloor == null && pnlNow >= +P.trigger) {
            protectFloor = +P.lock;
            stratTrailAnchor = pnlNow;
          }
          if (protectFloor != null && (P.mode === 'trail' || P.mode === 'locktrail')) {
            while (pnlNow - stratTrailAnchor >= +P.trailX) { protectFloor += +P.trailY; stratTrailAnchor += +P.trailX; }
          }
          if (protectFloor != null && pnlNow <= protectFloor) exitAll = 'Protect';
        }

        var expiryToday = all.every(function (l) { return l.closed || l.exp === d.date; });
        var holdHit = cfg.holdDays != null && cfg.holdDays !== '' && (j - i) >= +cfg.holdDays;
        var dteExit = cfg.exitDTE != null && cfg.exitDTE !== '' && dteDays(d.date, legs[0].exp) <= +cfg.exitDTE;
        var allClosed = all.every(function (l) { return l.closed; });

        if (exitAll || expiryToday || holdHit || dteExit || allClosed) {
          all.forEach(function (l) {
            if (l.closed) return;
            var fill = marks[l.id];
            l.closed = true;
            l.exitPx = (d.date === l.exp) ? fill : legExitPrice(fill, l.side);  // settle: no slip
            l.exitDate = d.date;
            l.exitWhy = exitAll ? exitAll : (d.date === l.exp ? 'Expiry' : (holdHit ? 'Hold' : (dteExit ? 'DTE' : 'End')));
          });
          trade.exitDate = d.date; trade.exitIdx = j;
          trade.exitWhy = exitAll || (expiryToday ? 'Expiry' : (holdHit ? 'Hold' : (dteExit ? 'DTE' : 'Legs done')));
          break;
        }
      }
      if (!trade.exitDate) {              // ran out of data (open trade at range end)
        var lastD = days[lastMarkable];
        all.forEach(function (l) {
          if (l.closed) return;
          var qd = dayQuote(lastD, l);
          l.closed = true; l.exitPx = qd ? legExitPrice(qd.c, l.side) : l.entry; l.exitDate = lastD.date; l.exitWhy = 'Open@End';
        });
        trade.exitDate = lastD.date; trade.exitIdx = lastMarkable; trade.exitWhy = 'Open@End';
      }
      trade.pnl = 0;
      all.forEach(function (l) { trade.pnl += (l.exitPx - l.entry) * (l.side === 'B' ? 1 : -1) * l.lots * lot; });
      trade.hedgeCost = 0;
      all.forEach(function (l) { if (l.isHedge) trade.hedgeCost += (l.exitPx - l.entry) * (l.side === 'B' ? 1 : -1) * l.lots * lot; });
      // margin estimate (rough, labeled beta in UI)
      var short = 0, longPrem = 0;
      all.forEach(function (l) {
        var notional = (trade.spotIn || 0) * l.lots * lot;
        if (l.type === 'FUT' || l.side === 'S') short += notional * 0.10;
        else longPrem += l.entry * l.lots * lot;
      });
      trade.margin = Math.round(short + longPrem);
      return trade;
    }

    return { trades: trades, stats: computeStats(trades, cfg, data), cfg: cfg };
  }

  // ---- stats -----------------------------------------------------------------
  function computeStats(trades, cfg, data) {
    var inc = trades.filter(function (t) { return t.include !== false; });
    var s = { n: inc.length, total: 0, wins: 0, losses: 0, maxP: null, maxL: null,
              avgWin: 0, avgLoss: 0, streakW: 0, streakL: 0, streakDist: {}, margin: 0,
              mdd: 0, mddStart: null, mddEnd: null, mddRecov: null, expectancy: null,
              hedgeCost: 0, tpHits: 0, slHits: 0, trailHits: 0, byExit: {} };
    var cum = 0, peak = 0, peakDate = null, curW = 0, curL = 0;
    var trough = 0, mddStartD = null, inDD = false, mddCandidate = { dd: 0 };
    var equity = [];
    inc.forEach(function (t) {
      s.total += t.pnl;
      if (t.pnl >= 0) { s.wins++; s.avgWin += t.pnl; curW++; if (curL) { s.streakDist['L' + curL] = (s.streakDist['L' + curL] || 0) + 1; curL = 0; } }
      else { s.losses++; s.avgLoss += t.pnl; curL++; if (curW) { s.streakDist['W' + curW] = (s.streakDist['W' + curW] || 0) + 1; curW = 0; } }
      s.streakW = Math.max(s.streakW, curW); s.streakL = Math.max(s.streakL, curL);
      if (s.maxP == null || t.pnl > s.maxP) s.maxP = t.pnl;
      if (s.maxL == null || t.pnl < s.maxL) s.maxL = t.pnl;
      s.margin = Math.max(s.margin, t.margin || 0);
      s.hedgeCost += t.hedgeCost || 0;
      s.byExit[t.exitWhy] = (s.byExit[t.exitWhy] || 0) + 1;
      if (/MTM TP/.test(t.exitWhy)) s.tpHits++;
      if (/MTM SL/.test(t.exitWhy)) s.slHits++;
      if (/Protect/.test(t.exitWhy)) s.trailHits++;
      cum += t.pnl;
      equity.push({ date: t.exitDate, cum: cum, pnl: t.pnl, entry: t.entryDate });
      if (cum > peak) {
        if (inDD && mddCandidate.dd < s.mdd) {}
        peak = cum; peakDate = t.exitDate;
        if (inDD) { inDD = false; }
      }
      var dd = cum - peak;
      if (dd < s.mdd) { s.mdd = dd; s.mddStart = peakDate; s.mddEnd = t.exitDate; }
    });
    if (curW) s.streakDist['W' + curW] = (s.streakDist['W' + curW] || 0) + 1;
    if (curL) s.streakDist['L' + curL] = (s.streakDist['L' + curL] || 0) + 1;
    if (s.wins) s.avgWin /= s.wins;
    if (s.losses) s.avgLoss /= s.losses;
    if (s.wins && s.losses) {
      var wp = s.wins / s.n;
      s.expectancy = (wp * s.avgWin + (1 - wp) * s.avgLoss) / Math.abs(s.avgLoss);
    }
    // recovery: first equity point after mddEnd where cum regains prior peak
    if (s.mdd < 0) {
      var pk = -Infinity, seenEnd = false, rec = null;
      var cum2 = 0, pkAtStart = 0;
      equity.forEach(function (e) { if (e.date <= s.mddStart) pkAtStart = Math.max(pkAtStart, e.cum); });
      for (var i2 = 0; i2 < equity.length; i2++) {
        var e = equity[i2];
        if (e.date > s.mddEnd && e.cum >= pkAtStart) { rec = e.date; break; }
      }
      s.mddRecov = rec;                    // null => running
    }
    s.retToMdd = s.mdd < 0 ? (s.total / Math.abs(s.mdd)) : null;
    s.equity = equity;

    // day-of-week × year and month × year breakups
    s.dayWise = {}; s.monthWise = {};
    inc.forEach(function (t) {
      var y = t.entryDate.slice(0, 4);
      var dow = new Date(t.entryDate + 'T00:00:00').getDay();
      (s.dayWise[y] = s.dayWise[y] || {})[dow] = (s.dayWise[y][dow] || 0) + t.pnl;
      var m = +t.entryDate.slice(5, 7);
      (s.monthWise[y] = s.monthWise[y] || {})[m] = (s.monthWise[y][m] || 0) + t.pnl;
    });
    // monthly aggregates
    var months = {};
    inc.forEach(function (t) { var k = t.entryDate.slice(0, 7); months[k] = (months[k] || 0) + t.pnl; });
    var mvals = Object.keys(months).map(function (k) { return months[k]; });
    s.avgMonthly = mvals.length ? mvals.reduce(function (a, b) { return a + b; }, 0) / mvals.length : 0;
    s.nMonths = mvals.length;
    return s;
  }

  window.FOEngine = {
    loadRange: loadRange, run: run, lotSize: lotSize, LOTS: LOTS,
    classifyExpiry: classifyExpiry, computeStats: computeStats, _cache: cache
  };
})();
