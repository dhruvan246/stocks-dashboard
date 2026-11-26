#!/usr/bin/env python3
"""Past years for docs/daily-moves.html (Rewind beyond the last 260 sessions) -> docs/dm_hist/.

build_daily_moves.py writes the LIVE file (last 260 sessions, today's members). This script writes one file per
calendar year from 2003, in the same shape, so the page can show any past window with the members of that date:

  docs/dm_hist/<Y>.bin    gzip JSON: day axis of the year (plus one base day = the last session before it),
                          every roster's member lists in force during the year (dated snapshots), index levels,
                          FII / DII / FII index-futures rows, and per stock the close and rolling 52-week high / low
  docs/dm_hist/<Y>x.bin   hover details: volume, delivery %, RSI-14 daily / weekly / monthly
  docs/dm_hist/index.json years, their first / last day, and the whole day calendar (for stepping across years)

Reads (never fetches): SF_BIN (full adjusted daily bars; daily from 2002-01-02), docs/dash_slim.bin
(indicesHistory + fnoHistory = dated member snapshots), scripts/_rename_map.json (old ticker -> current ticker),
docs/idx_ohlc/_closes.bin (index closes), docs/fii_dii.json, docs/fii_fo.json.

Rules (same as build_daily_moves.py unless said):
  * Members on a date = the snapshot with the latest effectiveDate <= that date. A snapshot symbol is looked up in
    the price store under its own name, else through _rename_map.json (chains followed). Symbols found under
    neither (NSE's DUMMY demerger placeholders, REITs, a few 2003 names) are listed per roster in "un".
  * Year universe = every member of any roster snapshot in force during the year. Day axis of the year = dates on
    which at least half of that universe has a bar.
  * 2003 is the first year: the store's daily bars start 2002-01-02, so every 2003 day has a full year behind its
    52-week high / low.
  * Index level = the close in _closes.bin (all 28 rosters have a series). 52-week high / low of the index = of its
    CLOSES, only where a close exists on the day and the series started at least a year before it.
  * Prices are the store's split/bonus-adjusted closes AS OF THE BUILD. A later split rescales the whole past, so a
    year file built earlier sits on an older basis: the page rescales a stock at the seam between two files using
    the day both hold (every file's base day is the previous file's last day).
  * Ex-dividend dots are not carried (actions.json keeps ~30 days).

Run:  SF_BIN=/path/fresh.bin python3 -X utf8 scripts/build_daily_moves_hist.py            every year
      ... --years 2025,2026                                                                only these
      ... --topup     (CI, daily) rebuild a year only when its file is missing or no longer overlaps the live
                      file by 20 sessions; otherwise writes nothing
"""
import os, sys, json, gzip, datetime as dt
from bisect import bisect_right, bisect_left
from collections import deque

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import build_daily_moves as B      # noqa: E402

DOCS = B.DOCS
OUTDIR = os.path.join(DOCS, "dm_hist")
FIRST_YEAR = 2003
KEY_OF = dict(B.KEY_FALLBACK)


def as_int(iso_s):
    return int(iso_s.replace("-", ""))


def rolling_hl(d, c, h, l):
    """52-week high / low on every bar: bars dated after (bar date minus one year) up to the bar; None closes skipped"""
    n = len(d)
    hh, ll = [None] * n, [None] * n
    qh, ql = deque(), deque()
    for i in range(n):
        if c[i] is not None:
            hv = h[i] if i < len(h) and h[i] is not None else c[i]
            lv = l[i] if i < len(l) and l[i] is not None else c[i]
            while qh and qh[-1][1] <= hv:
                qh.pop()
            qh.append((i, hv))
            while ql and ql[-1][1] >= lv:
                ql.pop()
            ql.append((i, lv))
        lo = bisect_right(d, B.year_before(d[i]))
        while qh and qh[0][0] < lo:
            qh.popleft()
        while ql and ql[0][0] < lo:
            ql.popleft()
        if c[i] is not None:
            hh[i], ll[i] = qh[0][1], ql[0][1]
    return hh, ll


def main():
    args = sys.argv[1:]
    topup = "--topup" in args
    only = None
    if "--years" in args:
        only = {int(x) for x in args[args.index("--years") + 1].split(",")}
    if not os.path.exists(B.BIN):
        sys.exit("missing " + B.BIN)

    if topup:      # decide from the small files first, so the daily run usually stops before loading the store
        try:
            live_days = B.gz_load(B.OUT)["days"]
            idx = json.load(open(os.path.join(OUTDIR, "index.json"), encoding="utf-8"))
        except Exception as e:
            sys.exit("topup: cannot read the live file or dm_hist/index.json (%s) - run a full build once" % e)
        need = set()
        edge = live_days[min(20, len(live_days) - 1)]
        last_y, live_last = live_days[-1] // 10000, live_days[-1]
        for y in range(live_days[0] // 10000, last_y + 1):
            m = idx["years"].get(str(y))
            if not m or not os.path.exists(os.path.join(OUTDIR, "%d.bin" % y)):
                need.add(y)
            elif m["last"] < edge and m["last"] < min(live_last, y * 10000 + 1231) and any(d // 10000 == y and d > m["last"] for d in live_days):
                need.add(y)
        if not need:
            print("dm_hist topup: every year file still overlaps the live file - nothing to do")
            return
        only = need
        print("dm_hist topup: rebuilding", sorted(need))

    D = B.gz_load(B.BIN)
    S = B.gz_load(B.SLIM)
    data, smeta, slim_meta = D["data"], D.get("meta", {}), S.get("meta") or {}
    rmap = json.load(open(os.path.join(HERE, "_rename_map.json"), encoding="utf-8"))

    def resolve(s):
        seen = set()
        while s not in data or not data[s].get("d"):
            if s in seen or s not in rmap:
                return None
            seen.add(s)
            s = rmap[s]
        return s

    # ---- rosters: dated snapshots, symbols resolved to the store's ticker
    H = dict(S.get("indicesHistory") or {})
    if S.get("fnoHistory"):
        H[B.FNO_NAME] = S["fnoHistory"]
    rost = {}
    for name, snaps in H.items():
        out = []
        for sn in sorted(snaps, key=lambda x: x["effectiveDate"]):
            ok, un = [], []
            for x in sn["symbols"]:
                r = resolve(x)
                (ok if r else un).append(r or x)
            out.append((as_int(sn["effectiveDate"]), sorted(set(ok)), sorted(set(un))))
        rost[name] = out

    def in_force(name, lo, hi):
        """snapshots of a roster in force at any time in [lo, hi]: the last one effective <= lo plus those inside"""
        sn = rost[name]
        effs = [e for e, _, _ in sn]
        a = max(0, bisect_right(effs, lo) - 1)
        b = bisect_right(effs, hi)
        return [x for x in sn[a:b] if x[0] <= hi]

    # ---- index closes
    C = json.loads(gzip.decompress(open(os.path.join(DOCS, "idx_ohlc", "_closes.bin"), "rb").read()))
    d0 = dt.date.fromisoformat(C["d0"])
    cdays = [d0]
    for g in C["dd"]:
        cdays.append(cdays[-1] + dt.timedelta(days=g))
    cdays = [x.year * 10000 + x.month * 100 + x.day for x in cdays]
    cpos = {x: i for i, x in enumerate(cdays)}
    by_name = {(x.get("n") or "").lower(): x.get("k") for x in (B.js_load("indices.json") or {}).get("indices", [])}
    series = {}
    for name in rost:
        if name == B.FNO_NAME:
            continue
        key = by_name.get(name.lower()) or KEY_OF.get(name)
        h = C["idx"].get(key) if key else None
        if not h:
            print("no close series for roster", name)
            continue
        dts, vals = [], []
        for j, v in enumerate(h["c"]):
            if v is not None:
                dts.append(cdays[h["s"] + j]); vals.append(v / 100.0)
        series[name] = (dts, vals, dict(zip(dts, vals)))

    # ---- flows
    fd = {as_int(r["date"]): r for r in (B.js_load("fii_dii.json") or {}).get("rows", [])}
    fo = {as_int(r["date"]): r for r in (B.js_load("fii_fo.json") or {}).get("rows", [])}

    end_int = as_int(D["end"])
    last_year = end_int // 10000
    years = [y for y in range(FIRST_YEAR, last_year + 1)]

    # ---- per-year universe and day axis (always for every year: index.json carries the whole calendar)
    univ, axis = {}, {}
    for y in [FIRST_YEAR - 1] + years:
        lo, hi = y * 10000 + 101, y * 10000 + 1231
        u = set()
        for name in rost:
            for _, ok, _ in in_force(name, lo, hi):
                u |= set(ok)
        univ[y] = sorted(u)
        cnt = {}
        for s in u:
            d = data[s]["d"]
            for x in d[bisect_left(d, lo):bisect_right(d, hi)]:
                cnt[x] = cnt.get(x, 0) + 1
        half = len(u) / 2.0
        axis[y] = sorted(x for x, c in cnt.items() if c >= half)
    for y in years:
        if len(axis[y]) < 30 and y != last_year:
            sys.exit("day axis of %d too short (%d)" % (y, len(axis[y])))

    cache = {}

    def prep(s):
        if s not in cache:
            r = data[s]
            d, c = r["d"], r["c"]
            hh, ll = rolling_hl(d, c, r.get("h") or c, r.get("l") or c)
            cache[s] = (d, c, hh, ll, r.get("v") or [], r.get("dv") or [],
                        B.rsi_series(d, c, None), B.rsi_series(d, c, B.week_key), B.rsi_series(d, c, B.month_key))
        return cache[s]

    os.makedirs(OUTDIR, exist_ok=True)
    built = D["end"]
    try:
        old_idx = json.load(open(os.path.join(OUTDIR, "index.json"), encoding="utf-8"))
    except Exception:
        old_idx = {"years": {}}
    meta_years = dict(old_idx.get("years") or {})
    for y in years:
        if only is not None and y not in only:
            continue
        if not axis[y]:
            continue
        days = [axis[y - 1][-1]] + axis[y]
        lo, hi = days[0], days[-1]
        stocks, extra = {}, {}
        for s in univ[y]:
            d, c, hh, ll, v, dv, rd, rw, rm = prep(s)
            a = bisect_left(d, lo)
            pos = {d[i]: i for i in range(a, bisect_right(d, hi))}
            cc, h2, l2, vv, dd_, r1, r2, r3 = [], [], [], [], [], [], [], []
            for day in days:
                i = pos.get(day)
                if i is None or c[i] is None:
                    for arr in (cc, h2, l2, vv, dd_, r1, r2, r3):
                        arr.append(None)
                    continue
                cc.append(int(round(c[i] * 100)))
                h2.append(int(round(hh[i] * 100)))
                l2.append(int(round(ll[i] * 100)))
                vv.append(int(v[i]) if i < len(v) and v[i] is not None else None)
                dd_.append(round(dv[i], 1) if i < len(dv) and dv[i] is not None else None)
                r1.append(None if rd[i] is None else round(rd[i], 1))
                r2.append(None if rw[i] is None else round(rw[i], 1))
                r3.append(None if rm[i] is None else round(rm[i], 1))
            if all(x is None for x in cc):
                continue          # a member with no bar in the whole year: reported through "nobar"
            m = smeta.get(s, {})
            sm = slim_meta.get(s + ".NS") or slim_meta.get(s + ".BO") or {}
            stocks[s] = {"n": m.get("name") or sm.get("name") or s,
                         "i": (m.get("ind") or sm.get("industry") or sm.get("sector") or "Uncategorized").strip(),
                         "c": cc, "hh": h2, "ll": l2}
            extra[s] = {"v": vv, "dv": dd_, "r": r1, "rw": r2, "rm": r3}
        idx = {}
        for name in rost:
            sn = in_force(name, lo, hi)
            if not sn:
                continue
            ent = {"snaps": [[e, ok] for e, ok, _ in sn]}
            un = {str(e): u for e, _, u in sn if u}
            if un:
                ent["un"] = un
            nobar = sorted({s for _, ok, _ in sn for s in ok if s not in stocks})
            if nobar:
                ent["nobar"] = nobar
            if name in series:
                dts, vals, px = series[name]
                ent["lv"] = [px.get(x) for x in days]
                ih, il = [], []
                for x in days:
                    if px.get(x) is None or dts[0] > B.year_before(x):
                        ih.append(None); il.append(None); continue
                    win = vals[bisect_right(dts, B.year_before(x)):bisect_right(dts, x)]
                    ih.append(max(win)); il.append(min(win))
                ent["hh"], ent["ll"], ent["hl"] = ih, il, "close"
            idx[name] = ent
        fii, dii, fl, fs = [], [], [], []
        for x in days:
            r = fd.get(x) or {}
            fii.append(r.get("fiiNet")); dii.append(r.get("diiNet"))
            f = (((fo.get(x) or {}).get("oi") or {}).get("FII") or {}).get("futIdx")
            fl.append(f[0] if f else None); fs.append(f[1] if f else None)
        core = {"v": 1, "year": y, "built": built, "end": B.iso(days[-1]), "days": days, "idx": idx, "stocks": stocks,
                "fii": fii, "dii": dii, "fl": fl, "fs": fs, "exd": {}}
        b1, _ = B.gz_dump(core, os.path.join(OUTDIR, "%d.bin" % y))
        b2, _ = B.gz_dump({"v": 1, "year": y, "built": built, "end": B.iso(days[-1]), "days": days, "stocks": extra},
                          os.path.join(OUTDIR, "%dx.bin" % y))
        meta_years[str(y)] = {"first": days[1], "last": days[-1], "base": days[0], "n": len(days) - 1, "built": built,
                              "stocks": len(stocks)}
        un_all = sorted({s for e in idx.values() for u in (e.get("un") or {}).values() for s in u})
        nb_all = sorted({s for e in idx.values() for s in e.get("nobar", [])})
        print("%d  %d days %s..%s  %d stocks  %d B + %d B  not in store: %s  no bar: %s"
              % (y, len(days) - 1, B.iso(days[1]), B.iso(days[-1]), len(stocks), b1, b2,
                 ",".join(un_all) or "-", ",".join(nb_all) or "-"))
    cal = [axis[FIRST_YEAR - 1][-1]] + [x for y in years for x in axis[y]]
    doc = {"v": 1, "first": FIRST_YEAR, "years": {k: meta_years[k] for k in sorted(meta_years)}, "days": cal}
    tmp = os.path.join(OUTDIR, "index.json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(doc, f, separators=(",", ":"))
        f.write("\n")
    os.replace(tmp, os.path.join(OUTDIR, "index.json"))
    print("index.json: %d years, %d days %s..%s" % (len(doc["years"]), len(cal) - 1, B.iso(cal[1]), B.iso(cal[-1])))


if __name__ == "__main__":
    main()
