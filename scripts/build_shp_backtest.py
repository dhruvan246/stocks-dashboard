# -*- coding: utf-8 -*-
"""Backtest FII/DII-accumulation strategies from the shareholding-pattern history.

Hypothesis under test (user's): "stocks where FII raises stake every quarter keep rising."

METHOD (point-in-time honest, survivorship-free — DATA_RUNBOOK.md §22):
  - Signals from scripts/shp_history.json (per-stock quarterly FII/DII % + each filing's
    ACTUAL submission date). A quarter's holdings are knowable only after the company files.
  - Rebalance day R(Q) = first trading day >= Q + 22 calendar days (SEBI filing deadline is
    21 days; by then ~all patterns are public). A filing submitted after R(Q) is EXCLUDED
    from that quarter's signal (sub_date <= R gate) — no look-ahead.
  - Universe = point-in-time Nifty 500 (scripts/_n500_master_history.json, nearest-prior
    snapshot per rebalance date) — no survivorship bias, no untradable microcaps. Filing-time
    tickers resolved to price-series keys via scripts/_rename_map.json (transitive).
  - Signal "K-streak": stake change >= +MIN_STEP percentage points in EACH of the K most
    recent quarter-over-quarter pairs (adjacent calendar quarters, no gaps), and latest
    stake >= MIN_LEVEL % (kills 0.00->0.05 rounding noise). "cut" variants mirror with <=.
  - Portfolio: equal-weight all qualifiers at R (or top-N by cumulative streak change),
    held to the next rebalance. Delisted/halted mid-quarter -> exits at last available
    close. No costs/slippage (same convention as the site's other backtests); price-only
    returns both sides (benchmark Nifty 500 is a price index too).
  - NAV daily from the survivorship-free bin closes (corp-action adjusted).

Output: docs/shp_backtest.json
  { updated, asof, firstReb, note, bench:{dates,nav}, variants:[{key,label,note,dates,nav,
    stats:{cagr,tot,mdd,rebs,avgHold,winPct,bestQ,worstQ,cagrBench},
    rebals:[{qe,date,n,ret,bret,picks:[[sym,cum,ret]...]}],
    next:{qe,date,forming,n,picks:[[sym,cum]...]}}] }

Run:
  SF_BIN=<fresh bin> python -X utf8 scripts/build_shp_backtest.py     (CI: data release asset)
  python -X utf8 scripts/build_shp_backtest.py                        (local: docs bin — STALE, dev only)
"""
import os, sys, json, gzip, datetime
from bisect import bisect_left, bisect_right

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
BIN = os.environ.get("SF_BIN") or os.path.join(ROOT, "docs", "sf_stock_data.bin")
HIST = os.path.join(HERE, "shp_history.json")
MEMB = os.path.join(HERE, "_n500_master_history.json")
RENAME = os.path.join(HERE, "_rename_map.json")
BENCH = os.path.join(ROOT, "docs", "nifty500.json")
OUT = os.path.join(ROOT, "docs", "shp_backtest.json")

MIN_STEP = 0.05      # pp per quarter to count as a raise (2dp data; > rounding noise)
MIN_LEVEL = 1.0      # latest stake must be >= this % (signal must be about a real holder)
# SEBI changed the disclosure format at Sep-2022 (DR holdings folded into investor categories,
# Institutions split Domestic/Foreign). Values on each side are as-filed, but the Jun->Sep-2022
# DELTA is a reclassification, not a stake change (INFY "gains" its 14.2% ADR block) — no signal
# may use that leg; streaks crossing it break and the strategy sits in cash if nothing qualifies.
FORMAT_BOUNDARY = ("2022-06-30", "2022-09-30")
MIN_HOLD = 5         # a rebalance needs this many qualifiers, else hold cash (flat NAV)
REB_LAG_DAYS = 22    # rebalance this many calendar days after quarter end
PICKS_CAP = 60       # picks stored per rebalance (page shows them)

VARIANTS = [
    ("fii2",      "FII raising 2 qtrs",        "fii", +1, 2, None),
    ("fii3",      "FII raising 3 qtrs",        "fii", +1, 3, None),
    ("fii2top20", "FII raising 2 qtrs · Top 20","fii", +1, 2, 20),
    ("dii2",      "DII raising 2 qtrs",        "dii", +1, 2, None),
    ("both2",     "FII & DII both raising 2q", "both", +1, 2, None),
    ("fiicut2",   "FII cutting 2 qtrs (inverse)","fii", -1, 2, None),
    # the FAIR yardstick: any equal-weight N500 basket beat the cap-weighted index in 2020-26,
    # so a signal only "works" if it also beats THIS baseline (same dates, same equal weighting).
    ("ewall",     "Every N500 stock (equal-weight)", "ew", +1, 2, None),
]

def load_bin():
    D = json.loads(gzip.decompress(open(BIN, "rb").read()))
    return D["data"], D.get("end", "?")

def resolve_map(data):
    """filing-time ticker -> bin key (transitive renames; identity when series exists)."""
    try:
        ren = json.load(open(RENAME, encoding="utf-8"))
    except Exception:
        ren = {}
    def res(sym):
        s, seen = sym, set()
        while s not in data and s in ren and s not in seen:
            seen.add(s); s = ren[s]
        return s if s in data else (sym if sym in data else None)
    return res

def main():
    data, bin_end = load_bin()
    print("bin: %d symbols, end %s" % (len(data), bin_end), flush=True)
    hist = json.load(open(HIST, encoding="utf-8"))
    syms = [s for s in hist if not s.startswith("_")]
    qes = sorted({qe for s in syms for qe in hist[s]})
    print("history: %d symbols, %d quarters %s..%s" % (len(syms), len(qes), qes[0], qes[-1]), flush=True)

    raw = json.load(open(MEMB, encoding="utf-8"))
    snaps = sorted((int(k.replace("-", "")), frozenset(v)) for k, v in raw.items())
    snap_dates = [s[0] for s in snaps]
    res = resolve_map(data)

    # benchmark: iso -> level
    bpx = json.load(open(BENCH, encoding="utf-8"))["px"]
    bkeys = sorted(bpx)

    # global daily axis (int dates) from the benchmark (dense, matches NSE calendar)
    axis = [int(k.replace("-", "")) for k in bkeys]

    def axis_at_or_after(dint):
        i = bisect_left(axis, dint)
        return axis[i] if i < len(axis) else None

    # per-symbol close lookup: last close ON or BEFORE a date (None before listing)
    cache = {}
    def close_at(bk, dint):
        e = cache.get(bk)
        if e is None:
            d = data[bk]; e = (d["d"], d["c"]); cache[bk] = e
        ds, cs = e
        i = bisect_right(ds, dint) - 1
        while i >= 0 and not cs[i]: i -= 1
        return cs[i] if i >= 0 else None

    def members_asof(dint):
        i = max(0, bisect_right(snap_dates, dint) - 1)
        return snaps[i][1]

    # rebalance calendar: quarters that have BOTH a signal history and a rebalance day on the axis
    rebs = []  # (qe_iso, reb_int)
    for qe in qes:
        d = datetime.date.fromisoformat(qe) + datetime.timedelta(days=REB_LAG_DAYS)
        r = axis_at_or_after(int(d.strftime("%Y%m%d")))
        if r: rebs.append((qe, r))
    today_int = int(datetime.date.today().strftime("%Y%m%d"))

    def cell(sym, qe): return hist[sym].get(qe) if sym in hist else None

    def prev_qe(qe):
        """CALENDAR-previous quarter end (never 'previous available' — gaps must break streaks)."""
        y, m = int(qe[:4]), int(qe[5:7])
        m -= 3
        if m <= 0: y, m = y - 1, 12
        return "%04d-%02d-%d" % (y, m, {3: 31, 6: 30, 9: 30, 12: 31}[m])

    def streak_val(sym, qe0, metric, sign, K, reb_int):
        """cum change over K consecutive CALENDAR-quarter raises ending at qe0 (None if absent)."""
        reb_iso = "%04d-%02d-%02d" % (reb_int // 10000, reb_int // 100 % 100, reb_int % 100)
        idx = {"fii": 1, "dii": 2}
        def val(c, m): return c[idx[m]]
        cum, q = 0.0, qe0
        for k in range(K):
            pq = prev_qe(q)
            if (pq, q) == FORMAT_BOUNDARY: return None   # delta across the format change is not comparable
            a = cell(sym, q); b = cell(sym, pq)
            if not a or not b: return None
            if k == 0 and str(a[5]) > reb_iso: return None   # latest quarter must be FILED by reb day
            mets = ("fii", "dii") if metric == "both" else (metric,)
            for m in mets:
                d = val(a, m) - val(b, m)
                if sign > 0 and d < MIN_STEP: return None
                if sign < 0 and d > -MIN_STEP: return None
            cum += (val(a, "fii") - val(b, "fii")) if metric != "dii" else (val(a, "dii") - val(b, "dii"))
            q = pq
        lm = "dii" if metric == "dii" else "fii"
        if sign > 0 and val(cell(sym, qe0), lm) < MIN_LEVEL: return None   # raises: must END >= 1%
        if sign < 0 and val(cell(sym, q), lm) < MIN_LEVEL: return None     # cuts: must START >= 1%
        return round(cum, 2)

    def qualifiers(qe0, metric, sign, K, reb_int):
        mem = members_asof(reb_int)
        out = []
        if metric == "ew":   # baseline: EVERY point-in-time member with a price (no signal)
            seen = set()
            for sym in mem:
                bk = res(sym) or (sym if sym in data else None)
                if not bk or bk in seen: continue
                if close_at(bk, reb_int) is None: continue
                seen.add(bk); out.append((sym, bk, 0.0))
            return out
        for sym in syms:
            bk = res(sym)
            if not bk or (bk not in mem and sym not in mem): continue
            cum = streak_val(sym, qe0, metric, sign, K, reb_int)
            if cum is None: continue
            if close_at(bk, reb_int) is None: continue
            out.append((sym, bk, cum))
        out.sort(key=lambda t: -abs(t[2]))
        return out

    # ---- simulate each variant over the common axis ----
    first_qi = None  # first quarter index usable by the deepest variant runs per-variant instead
    results = []
    for key, label, metric, sign, K, topn in VARIANTS:
        navs, navd = [], []
        holdings = None   # list of (bk, entry_close, weight_value)
        nav = 100.0
        rebal_rows, held_counts = [], []
        entry_nav = {}
        prev_reb_nav = None; prev_reb_bench = None
        per_reb_returns = []
        started = False
        for qi in range(K + 1, len(qes)):
            qe, reb = None, None
            for q, r in rebs:
                if q == qes[qi]: qe, reb = q, r
            if not qe or reb > today_int: continue
            qual = qualifiers(qe, metric, sign, K, reb)
            if topn: qual = qual[:topn]
            if not started and len(qual) < MIN_HOLD: continue
            # mark existing book to this reb day, realize NAV
            if holdings:
                tot = sum(w * ((close_at(bk, reb) or ec) / ec) for bk, ec, w in holdings)
                nav = tot
            # record the completed rebalance period's return on the PREVIOUS row
            if rebal_rows and prev_reb_nav:
                rr = nav / prev_reb_nav - 1
                rebal_rows[-1]["ret"] = round(rr * 100, 2)
                b0 = bpx.get(rebal_rows[-1]["date"]); b1 = bpx.get(iso_of(reb))
                if b0 and b1: rebal_rows[-1]["bret"] = round((b1 / b0 - 1) * 100, 2)
                per_reb_returns.append((rr, (b1 / b0 - 1) if (b0 and b1) else None))
            started = True
            # per-pick forward return needs next reb; fill later (baseline stores no picks — no signal)
            picks = [] if metric == "ew" else [[s, c] for s, bk, c in qual[:PICKS_CAP]]
            rebal_rows.append({"qe": qe, "date": iso_of(reb), "n": len(qual), "picks": picks,
                               "_qual": [(s, bk, c) for s, bk, c in qual]})
            held_counts.append(len(qual))
            if len(qual) >= max(1, MIN_HOLD if not topn else 1):
                w = nav / len(qual)
                holdings = [(bk, close_at(bk, reb), w) for s, bk, c in qual]
            else:
                holdings = []   # cash
            prev_reb_nav = nav
            navd.append(reb); navs.append(round(nav, 2))
        # daily NAV between rebalances
        dates_out, nav_out = [], []
        if rebal_rows:
            start_int = int(rebal_rows[0]["date"].replace("-", ""))
            ai = bisect_left(axis, start_int)
            ri = -1
            book, base = [], 100.0
            reb_ints = [int(r["date"].replace("-", "")) for r in rebal_rows]
            nav_at_reb = {}
            nav_run = 100.0
            for x in range(ai, len(axis)):
                dint = axis[x]
                if ri + 1 < len(reb_ints) and dint >= reb_ints[ri + 1]:
                    # mark to this reb, rotate
                    if book:
                        nav_run = sum(w * ((close_at(bk, dint) or ec) / ec) for bk, ec, w in book)
                    ri += 1
                    qual = rebal_rows[ri]["_qual"]
                    nav_at_reb[reb_ints[ri]] = nav_run
                    if qual:
                        w = nav_run / len(qual)
                        book = [(bk, close_at(bk, dint), w) for s, bk, c in qual]
                    else:
                        book = []
                cur = sum(w * ((close_at(bk, dint) or ec) / ec) for bk, ec, w in book) if book else nav_run
                dates_out.append(dint); nav_out.append(round(cur, 3))
                nav_run = nav_run if book else nav_run
            # per-pick forward returns + final row return
            for i, row in enumerate(rebal_rows):
                r0 = reb_ints[i]
                r1 = reb_ints[i + 1] if i + 1 < len(reb_ints) else min(axis[-1], today_int)
                for p in row["picks"]:
                    bk = res(p[0])
                    c0, c1 = close_at(bk, r0), close_at(bk, r1)
                    p.append(round((c1 / c0 - 1) * 100, 1) if (c0 and c1) else None)
                if "ret" not in row:
                    tot = nav_out[-1]
                    base_n = nav_at_reb.get(r0)
                    row["ret"] = round((tot / base_n - 1) * 100, 2) if base_n else None
                    b0, b1 = bpx.get(row["date"]), bpx.get(iso_of(r1))
                    row["bret"] = round((b1 / b0 - 1) * 100, 2) if (b0 and b1) else None
                del row["_qual"]

        # stats
        stats = {}
        if nav_out:
            yrs = max(0.25, (date_of(dates_out[-1]) - date_of(dates_out[0])).days / 365.25)
            stats["cagr"] = round(((nav_out[-1] / 100.0) ** (1 / yrs) - 1) * 100, 1)
            stats["tot"] = round(nav_out[-1] - 100, 1)
            peak, mdd = 0.0, 0.0
            for v in nav_out:
                peak = max(peak, v); mdd = min(mdd, v / peak - 1)
            stats["mdd"] = round(mdd * 100, 1)
            stats["rebs"] = len(rebal_rows)
            stats["avgHold"] = round(sum(held_counts) / len(held_counts), 1) if held_counts else 0
            wins = [1 for r in rebal_rows if r.get("ret") is not None and r.get("bret") is not None and r["ret"] > r["bret"]]
            comp = [r for r in rebal_rows if r.get("ret") is not None and r.get("bret") is not None]
            stats["winPct"] = round(100 * len(wins) / len(comp), 0) if comp else None
            rets = [r["ret"] for r in rebal_rows if r.get("ret") is not None]
            stats["bestQ"] = max(rets) if rets else None
            stats["worstQ"] = min(rets) if rets else None
            b0 = bpx.get(iso_of(dates_out[0])); b1 = bpx.get(iso_of(dates_out[-1]))
            stats["cagrBench"] = round((((b1 / b0) ** (1 / yrs)) - 1) * 100, 1) if (b0 and b1) else None

        # next-rebalance preview from the newest (possibly still-filing) quarter
        nxt = None
        qi = len(qes) - 1
        if qi >= K and metric != "ew":
            qe = qes[qi]
            d = datetime.date.fromisoformat(qe) + datetime.timedelta(days=REB_LAG_DAYS)
            while d.weekday() >= 5: d += datetime.timedelta(days=1)   # next weekday (informational)
            nreb = int(d.strftime("%Y%m%d"))                          # may be beyond the price axis (future)
            if nreb > today_int:
                qual = qualifiers(qe, metric, sign, K, today_int + 1)  # filings known today
                if topn: qual = qual[:topn]
                nxt = {"qe": qe, "date": d.isoformat(), "forming": True, "n": len(qual),
                       "picks": [[s, c] for s, bk, c in qual[:PICKS_CAP]]}
        results.append({"key": key, "label": label, "metric": metric, "sign": sign, "k": K,
                        "topn": topn, "dates": dates_out, "nav": nav_out, "stats": stats,
                        "rebals": rebal_rows, "next": nxt})
        print("%-10s rebs=%s avgHold=%s CAGR=%s%% (bench %s%%) mdd=%s%%" %
              (key, stats.get("rebs"), stats.get("avgHold"), stats.get("cagr"),
               stats.get("cagrBench"), stats.get("mdd")), flush=True)

    # common benchmark curve over the widest variant span
    all_dates = sorted({d for v in results for d in v["dates"][:1]} | {d for v in results for d in v["dates"][-1:]})
    if all_dates:
        lo, hi = all_dates[0], all_dates[-1]
        bd = [d for d in axis if lo <= d <= hi and iso_of(d) in bpx]
        b0 = bpx[iso_of(bd[0])]
        bench = {"dates": bd, "nav": [round(bpx[iso_of(d)] / b0 * 100, 2) for d in bd]}
    else:
        bench = {"dates": [], "nav": []}

    ist = datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)
    out = {"updated": ist.strftime("%Y-%m-%d %H:%M IST"), "binEnd": bin_end,
           "quarters": qes, "params": {"minStep": MIN_STEP, "minLevel": MIN_LEVEL,
           "rebLagDays": REB_LAG_DAYS, "minHold": MIN_HOLD, "universe": "Nifty 500 (point-in-time)"},
           "bench": bench, "variants": results}
    json.dump(out, open(OUT, "w", encoding="utf-8"), separators=(",", ":"))
    print("WROTE %s (%.0f KB)" % (os.path.normpath(OUT), os.path.getsize(OUT) / 1e3), flush=True)

def iso_of(dint): return "%04d-%02d-%02d" % (dint // 10000, dint // 100 % 100, dint % 100)
def date_of(dint): return datetime.date(dint // 10000, dint // 100 % 100, dint % 100)

if __name__ == "__main__":
    main()
