#!/usr/bin/env python3
"""Momentum + results screen for docs/ideas.html (runbook §144d).

The rules are the ones measured from the buy/sell record of a mid/small-cap model portfolio
(169 buys, 145 sells, Sep-2019 to Sep-2026). They were derived by analysis, not taken from its
stated method. A stock is on the BUY list when ALL of these hold on the screen date:

  * 3-month return ranks in the top 20% of every NSE stock that traded that day (percentile >= 80)
  * close is within 10% of its 52-week high
  * the newest quarter already announced is a profit, up >= 25% on the same quarter last year,
    and the trailing four quarters are in profit (a shrinking loss is not growth — MBECL 2026-09)
  * that result was announced in the last 45 days
  * market cap between Rs 500 cr and Rs 20,000 cr (today's market cap)

A stock is on the FADED list when it was on the buy list at any screen in the last 90 days and now
ranks below the 50th percentile on 6-month return, OR sits more than 20% under its 52-week high.
That is the state the measured portfolio usually sold in.

Output: docs/ideas/momentum.json (daily).
`--validate` adds docs/ideas/momentum_validation.json: the same rules run at every month-end
since 2019-09 on the survivorship-free price store (docs/sf_stock_data.bin, which keeps
delisted names), with each month's picks priced 3 and 6 months later against the eligible
universe. Past market caps of delisted companies are not in our data, so the backtest replaces the
market-cap band with "not in the Nifty 100 on that date, median daily turnover >= Rs 50 lakh".

Inputs (all already in the repo): docs/stock_data.bin, docs/sf_fundamentals.json,
docs/sector_classification.json, docs/dash_slim.bin (index rosters), docs/ideas/theme_map.json,
docs/ideas/govt.json. Stdlib only.
"""
import argparse, bisect, datetime as dt, gzip, json, os, statistics, sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DOCS = os.path.join(ROOT, "docs")

# Measured 2026-09-23 against the portfolio's own 169 buys (session scratchpad; the portfolio file
# is not in the repo): how many buys each rule would have matched on the day of purchase.
RECALL = dict(measured="2026-09-23", buys=169, all_rules=29, momentum=101, near_high=108, earnings=91,
              recent_result=107, mcap_band=157)

RULES = dict(p3_min=80, off52_max=10, pat_yoy_min=25, result_days_max=45,
             mcap_min=500, mcap_max=20000, faded_p6_below=50, faded_off52_below=20, faded_lookback_days=90)


def load_gz_json(path):
    with open(path, "rb") as fh:
        return json.loads(gzip.decompress(fh.read()))


def load_json(path, default=None):
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except FileNotFoundError:
        return default


def dint(d):
    return d.year * 10000 + d.month * 100 + d.day


# ---------- fundamentals: point-in-time, consolidated first, standalone fallback ----------
# Row = [quarter_end, pat_std, ann_std, pat_con, ann_con]; ann 0/None = date unknown, never visible
# (runbook §91). A consolidated series whose newest visible quarter trails standalone's by more than
# 12 months is dead at that date and is skipped (same rule as backtest-engine.js _conFreshEnough).
def _last_visible(arr, ni, ai, di):
    for q in reversed(arr):
        if q[ni] is not None and q[ai] and 0 < q[ai] <= di:
            return q[0]
    return None


def _months(a, b):
    return (a // 10000 - b // 10000) * 12 + (a // 100 % 100 - b // 100 % 100)


def _qe_back(qe, k):
    y, m = qe // 10000, qe // 100 % 100
    m -= 3 * k
    while m <= 0:
        m += 12
        y -= 1
    return y * 100 + m  # yyyymm


def fundamentals_at(arr, di):
    """Newest announced quarter's PAT YoY %, trailing-4Q PAT, announce date — or None."""
    if not arr:
        return None
    c, s = _last_visible(arr, 3, 4, di), _last_visible(arr, 1, 2, di)
    tries = [(3, 4), (1, 2)] if c is not None and (s is None or _months(s, c) <= 12) else [(1, 2)]
    for ni, ai in tries:
        vis = [q for q in arr if q[ni] is not None and q[ai] and 0 < q[ai] <= di]
        if not vis:
            continue
        cur = max(vis, key=lambda q: q[0])
        bym = {q[0] // 100: q[ni] for q in vis}
        base = bym.get(_qe_back(cur[0], 4))
        yoy = None if not base else (cur[ni] - base) / abs(base) * 100
        want = [_qe_back(cur[0], k) for k in range(4)]
        ttm = sum(bym[w] for w in want) if all(w in bym for w in want) else None
        return dict(qe=cur[0], ann=cur[ai], pat=cur[ni], yoy=yoy, ttm=ttm, basis="con" if ni == 3 else "std")
    return None


# ---------- price helpers over {d:[...], p:[...]} with d = day offsets ----------
class Px:
    def __init__(self, d, p):
        self.d, self.p = d, p

    def idx(self, day, tol):
        i = bisect.bisect_right(self.d, day) - 1
        return i if i >= 0 and day - self.d[i] <= tol else -1

    def at(self, day, tol=10):
        i = self.idx(day, tol)
        return self.p[i] if i >= 0 else None

    def ret(self, day, lb):
        a, b = self.at(day, 7), self.at(day - lb)
        return a / b - 1 if a and b and b > 0 else None

    def off52(self, day):
        i = self.idx(day, 7)
        if i < 0:
            return None
        j = bisect.bisect_left(self.d, day - 365)
        hi = max(self.p[j:i + 1])
        return (self.p[i] / hi - 1) * 100 if hi > 0 else None

    def jump(self, day, back=365):
        """A one-day move outside 0.6x..1.8x inside the window: an unadjusted split/bonus or a
        relisting (INDIAGLYCO 2026-09-02 0.21x, MBECL 2026-09-01 3.42x after months frozen). The
        52-week high and the 3-month return are meaningless across it, so such names are held out."""
        i = self.idx(day, 7)
        j = max(1, bisect.bisect_left(self.d, day - back))
        p = self.p
        return any(p[k - 1] > 0 and not (0.6 <= p[k] / p[k - 1] <= 1.8) for k in range(j, i + 1)) if i >= 0 else False

    def ath_off(self, day):
        """Distance from the all-time high in our history; None when the history holds a one-day
        jump outside 0.6x..1.8x (an unadjusted split/bonus would fake the high)."""
        i = self.idx(day, 7)
        if i < 0:
            return None
        p = self.p
        if any(p[k - 1] > 0 and not (0.6 <= p[k] / p[k - 1] <= 1.8) for k in range(1, i + 1)):
            return None
        return (p[i] / max(p[:i + 1]) - 1) * 100


def percentile_map(rets):
    vals = sorted(rets.values())
    n = len(vals)
    return {k: 100.0 * bisect.bisect_left(vals, v) / n for k, v in rets.items()} if n else {}


# ---------- live screen ----------
def live(out_path):
    sd = load_gz_json(os.path.join(DOCS, "stock_data.bin"))
    base = dt.datetime.utcfromtimestamp(sd["startTs"]).date()
    meta = sd["meta"]
    series = {k: Px(v["d"], v["p"]) for k, v in sd["series"].items() if v.get("d")}
    fund = load_json(os.path.join(DOCS, "sf_fundamentals.json"), {})
    cls = load_json(os.path.join(DOCS, "sector_classification.json"), {})
    tmap = load_json(os.path.join(DOCS, "ideas", "theme_map.json"), {}) or {}
    govt = load_json(os.path.join(DOCS, "ideas", "govt.json"), {}) or {}
    progs = {}
    for tid, th in (tmap.get("themes") or {}).items():
        for c in th.get("companies", []):
            progs.setdefault(c.get("symbol"), []).append((tid, th.get("name", tid)))
    fresh_themes = {t for r in govt.get("releases", []) for t in r.get("themes", [])}

    today = max(series["RELIANCE.NS"].d) if "RELIANCE.NS" in series else max(max(s.d) for s in series.values())

    def screen(day, detail=False):
        nse = {k: v for k, v in series.items() if k.endswith(".NS")}
        p3 = percentile_map({k: r for k, s in nse.items() if (r := s.ret(day, 91)) is not None})
        p6 = percentile_map({k: r for k, s in nse.items() if (r := s.ret(day, 182)) is not None})
        vals3 = sorted(r for k, s in nse.items() if (r := s.ret(day, 91)) is not None)
        di = dint(base + dt.timedelta(day))
        passed, rows, held = set(), {}, set()
        for k, s in series.items():
            m = meta.get(k) or {}
            sym = m.get("symbol") or k.rsplit(".", 1)[0]
            if k.endswith(".BO") and sym + ".NS" in series:
                continue
            mc, last = m.get("mcap"), m.get("latest")
            price = s.at(day, 7)
            if not (mc and last and price):
                continue
            mcap = price / 100 * (mc / last)  # p is paise-like (x100); today's share count x that day's adjusted close
            r3 = s.ret(day, 91)
            if r3 is None:
                continue
            if s.jump(day):
                held.add(k)
                continue
            pct3 = p3.get(k)
            if pct3 is None:  # BSE-only name: rank its return against the NSE distribution
                pct3 = 100.0 * bisect.bisect_left(vals3, r3) / len(vals3) if vals3 else None
            off = s.off52(day)
            price_ok = (pct3 is not None and pct3 >= RULES["p3_min"] and off is not None and off >= -RULES["off52_max"]
                        and RULES["mcap_min"] <= mcap <= RULES["mcap_max"])
            if not (price_ok or detail):
                continue  # fundamentals are the slow part: only read them for price-qualified names
            f = fundamentals_at(fund.get(sym), di)
            ann_days = None
            if f and f["ann"]:
                a = f["ann"]
                ann_days = (base + dt.timedelta(day) - dt.date(a // 10000, a // 100 % 100, a % 100)).days
            ok = (pct3 is not None and pct3 >= RULES["p3_min"] and off is not None and off >= -RULES["off52_max"]
                  and f is not None and f["yoy"] is not None and f["yoy"] >= RULES["pat_yoy_min"] and f["pat"] > 0
                  and f["ttm"] is not None and f["ttm"] > 0 and ann_days is not None and ann_days <= RULES["result_days_max"]
                  and RULES["mcap_min"] <= mcap <= RULES["mcap_max"])
            if ok:
                passed.add(k)
            if detail:
                rows[k] = dict(k=k, sym=sym, mcap=mcap, pct3=pct3, pct6=p6.get(k), off=off, f=f, ann_days=ann_days,
                               r3=r3, price=price, ok=ok)
        return passed, rows, held

    passed, rows, held = screen(today, detail=True)
    # past screens for the FADED list: every trading day in the look-back window
    days = sorted({d for d in series["RELIANCE.NS"].d if today - RULES["faded_lookback_days"] <= d < today}) if "RELIANCE.NS" in series else []
    seen = {}
    for d in days:
        for k in screen(d)[0]:
            seen.setdefault(k, d)

    def row_out(r):
        s = series[r["k"]]
        f = r["f"] or {}
        c = cls.get(r["k"]) or {}
        pe = r["mcap"] / f["ttm"] if f.get("ttm") and f["ttm"] > 0 else None
        first = s.d[0]
        return dict(symbol=r["sym"], name=(meta.get(r["k"]) or {}).get("name") or r["sym"], exch=r["k"].rsplit(".", 1)[1],
                    theme=next((t for t in (c.get("igroup"), (meta.get(r["k"]) or {}).get("industry")) if t and t not in ("Unknown", "-")), "Unclassified"),
                    mcap=round(r["mcap"]), close=round(r["price"] / 100, 2),
                    ret_3m=round(r["r3"] * 100, 1), pct_3m=round(r["pct3"], 1) if r["pct3"] is not None else None,
                    pct_6m=round(r["pct6"], 1) if r["pct6"] is not None else None,
                    off_52w=round(r["off"], 1) if r["off"] is not None else None,
                    off_ath=(round(a, 1) if (a := s.ath_off(today)) is not None else None),
                    pe=round(pe, 1) if pe else None,
                    pat_yoy=round(f["yoy"], 1) if f.get("yoy") is not None else None,
                    pat_q=f.get("pat"), quarter=f.get("qe"), basis=f.get("basis"),
                    result_date=(f"{f['ann'] // 10000}-{f['ann'] // 100 % 100:02d}-{f['ann'] % 100:02d}" if f.get("ann") else None),
                    result_days=r["ann_days"], listed_days=today - first,
                    programmes=[n for _, n in progs.get(r["sym"], [])],
                    fresh_govt=any(t in fresh_themes for t, _ in progs.get(r["sym"], [])))

    picks = sorted((row_out(rows[k]) for k in passed), key=lambda x: -(x["pct_3m"] or 0))
    faded = []
    for k, d0 in seen.items():
        r = rows.get(k)
        if not r or k in passed:
            continue
        why = []
        if r["pct6"] is not None and r["pct6"] < RULES["faded_p6_below"]:
            why.append(f"6-month rank {r['pct6']:.0f} (below {RULES['faded_p6_below']})")
        if r["off"] is not None and r["off"] < -RULES["faded_off52_below"]:
            why.append(f"{-r['off']:.0f}% under its 52-week high")
        if why:
            o = row_out(r)
            o["first_seen"] = str(base + dt.timedelta(d0))
            o["why"] = why
            faded.append(o)
    faded.sort(key=lambda x: x["off_52w"] if x["off_52w"] is not None else 0)
    themes = {}
    for p in picks:
        themes.setdefault(p["theme"], []).append(p["symbol"])
    out = dict(
        asof=str(base + dt.timedelta(today)),
        built=dt.datetime.now(dt.timezone(dt.timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %H:%M IST"),
        rules=RULES, universe_nse=len([k for k in series if k.endswith(".NS")]),
        picks=picks, faded=faded,
        themes=sorted(({"theme": t, "n": len(v), "symbols": v} for t, v in themes.items()), key=lambda x: -x["n"]),
        screened_days=len(days) + 1,
        held_out=sorted(k.rsplit(".", 1)[0] for k in held),
    )
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(out, fh, ensure_ascii=False, separators=(",", ":"))
    print(f"momentum: asof {out['asof']} · {len(picks)} picks · {len(faded)} faded · {len(themes)} themes -> {out_path}")


# ---------- backtest on the survivorship-free store ----------
def validate(out_path):
    sf = load_gz_json(os.path.join(DOCS, "sf_stock_data.bin"))
    data = sf["data"]
    fund = load_json(os.path.join(DOCS, "sf_fundamentals.json"), {})
    slim = load_gz_json(os.path.join(DOCS, "dash_slim.bin"))
    n100 = slim["indicesHistory"].get("Nifty 100", [])
    idxm = load_json(os.path.join(DOCS, "index_monthly.json"), {}) or {}
    sc = next((x["closes"] for x in idxm.get("indices", []) if x.get("label") == "Nifty Smallcap 250"), {})

    def toord(i):
        return dt.date(i // 10000, i // 100 % 100, i % 100).toordinal()

    S = {}
    for sym, v in data.items():
        if not v.get("d"):
            continue
        S[sym] = (Px([toord(x) for x in v["d"]], v["c"]), v["d"], v.get("t") or [])
    end = dt.date(int(sf["end"][:4]), int(sf["end"][5:7]), int(sf["end"][8:10]))

    def roster(date_s):
        cur = None
        for e in n100:
            if e["effectiveDate"] <= date_s:
                cur = e
        return set(cur["symbols"]) if cur else set()

    def sc_at(d):
        v = (sc.get(str(d.year)) or [None] * 12)[d.month - 1]
        return v

    def month_end(y, m):
        return dt.date(y + m // 12, m % 12 + 1, 1) - dt.timedelta(days=1)

    def add_months(d, k):
        m = d.month - 1 + k
        return month_end(d.year + m // 12, m % 12 + 1)

    months, d = [], dt.date(2019, 9, 30)
    while d <= end - dt.timedelta(days=95):
        months.append(d)
        d = add_months(d, 1)
    res = []
    for me in months:
        day = me.toordinal()
        di = dint(me)
        rets3 = {}
        for sym, (px, _, _) in S.items():
            r = px.ret(day, 91)
            if r is not None:
                rets3[sym] = r
        p3 = percentile_map(rets3)
        big = roster(me.isoformat())
        elig, picks = [], []
        for sym in rets3:
            px, draw, t = S[sym]
            if sym in big:
                continue
            i = px.idx(day, 7)
            j = bisect.bisect_left(px.d, day - 182)
            tv = [x for x in t[j:i + 1] if x is not None]
            if not tv or statistics.median(tv) < 50:  # Rs 50 lakh/day
                continue
            elig.append(sym)
            off = px.off52(day)
            f = fundamentals_at(fund.get(sym), di)
            if px.jump(day):
                continue
            if not (p3[sym] >= RULES["p3_min"] and off is not None and off >= -RULES["off52_max"] and f
                    and f["yoy"] is not None and f["yoy"] >= RULES["pat_yoy_min"] and f["pat"] > 0
                    and f["ttm"] and f["ttm"] > 0):
                continue
            a = f["ann"]
            if (me - dt.date(a // 10000, a // 100 % 100, a % 100)).days > RULES["result_days_max"]:
                continue
            picks.append(sym)

        def fwd(syms, h):
            out = []
            for s_ in syms:
                px = S[s_][0]
                a = px.at(day, 7)
                i = bisect.bisect_right(px.d, day + h) - 1  # delisted: its last close stands
                b = px.p[i] if i >= 0 else None
                if a and b:
                    out.append(b / a - 1)
            return statistics.mean(out) * 100 if out else None

        row = dict(date=me.isoformat(), picks=len(picks), eligible=len(elig),
                   pick3=fwd(picks, 91), univ3=fwd(elig, 91), pick6=fwd(picks, 182), univ6=fwd(elig, 182))
        s0, s3 = sc_at(me), sc_at(add_months(me, 3))
        row["sc250_3"] = (s3 / s0 - 1) * 100 if s0 and s3 else None
        res.append(row)

    def agg(key, bkey):
        pairs = [(r[key], r[bkey]) for r in res if r[key] is not None and r[bkey] is not None and r["picks"] >= 3]
        if not pairs:
            return None
        return dict(months=len(pairs), avg=round(statistics.mean(p for p, _ in pairs), 2),
                    bench=round(statistics.mean(b for _, b in pairs), 2),
                    beat=sum(p > b for p, b in pairs))
    out = dict(built=dt.date.today().isoformat(), data_end=sf["end"], rules=RULES, recall=RECALL,
               size_rule="not in the Nifty 100 on that date, median daily turnover >= Rs 50 lakh over 6 months",
               summary=dict(h3_vs_universe=agg("pick3", "univ3"), h6_vs_universe=agg("pick6", "univ6"),
                            h3_vs_sc250=agg("pick3", "sc250_3"),
                            median_picks=statistics.median(r["picks"] for r in res) if res else None),
               months=[{k: (round(v, 2) if isinstance(v, float) else v) for k, v in r.items()} for r in res])
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(out, fh, ensure_ascii=False, separators=(",", ":"))
    print("validation:", json.dumps(out["summary"]))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(DOCS, "ideas", "momentum.json"))
    ap.add_argument("--validate", action="store_true", help="also run the month-end backtest (needs docs/sf_stock_data.bin)")
    ap.add_argument("--skip-live", action="store_true")
    a = ap.parse_args()
    if not a.skip_live:
        live(a.out)
    if a.validate:
        validate(os.path.join(os.path.dirname(a.out), "momentum_validation.json"))


if __name__ == "__main__":
    sys.exit(main())
