#!/usr/bin/env python3
"""Build the data behind docs/daily-moves.html (the stock x day heatmap grid).

Reads (never fetches):
  docs/sf_stock_data.bin    full adjusted daily bars (CI: the fresh `data` release asset)   [SF_BIN]
  docs/dash_slim.bin        index rosters (indicesHistory, latest snapshot) + today's F&O list [DASH_SLIM]
  docs/nifty.json, nifty500.json, nifty_bank.json, nifty_microcap250.json   long daily index closes
  docs/indices_hist.json    daily closes of every NSE index (short history) + docs/indices.json (names, 52w)
  docs/fii_dii.json         exchange provisional FII / DII cash net, Rs crore
  docs/fii_fo.json          participant-wise open interest (FII index futures long / short contracts)
  docs/actions.json         ex-dates (dividends only are kept)

Writes two gzip-JSON files (mtime 0, so an unchanged build is byte-identical):
  docs/daily_moves.bin      what the grid needs at first paint: day axis, rosters, index levels, flows,
                            and per stock the close and the rolling 52-week high / low of every day
  docs/daily_moves_x.bin    loaded on first hover: volume, delivery % and RSI-14 on daily / weekly /
                            monthly bars for every stock and day

Rules:
  * Day axis = the last N+1 dates on which at least half the universe has a bar (the first one is only
    the base for the first day's change).
  * Close = the store's split/bonus-adjusted close. No dividend adjustment (runbook §174).
  * 52-week high / low on day t = highest intraday high / lowest intraday low over the bars dated after
    (t minus one year) up to t. A bar without a high/low falls back to its close.
  * RSI = Wilder, 14 periods, seeded with the simple average of the first 14 changes, run over the whole
    stored history. Weekly / monthly RSI on day t = completed weeks / months plus the running period
    ending at day t's close. Fewer than 14 changes -> null.
  * A stock with no bar on a day is null there. Nothing is filled forward.
  * Ex-dividend dates accumulate: actions.json only keeps ~30 past days, so dates already in the
    previous daily_moves.bin are carried over (inside the window).

Run:  python3 -X utf8 scripts/build_daily_moves.py
      SF_BIN=/path/fresh.bin python3 -X utf8 scripts/build_daily_moves.py
"""
import os, sys, json, gzip, io
from bisect import bisect_right
from datetime import date

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
DOCS = os.path.join(ROOT, "docs")
BIN = os.environ.get("SF_BIN") or os.path.join(DOCS, "sf_stock_data.bin")
SLIM = os.environ.get("DASH_SLIM") or os.path.join(DOCS, "dash_slim.bin")
OUT = os.path.join(DOCS, "daily_moves.bin")
OUTX = os.path.join(DOCS, "daily_moves_x.bin")

N = 260            # sessions shown (about one year); one more is kept as the base of the first change
RSI_N = 14

# our roster name -> docs file holding its long daily close history
LONG_PX = {"Nifty 50": "nifty.json", "Nifty 500": "nifty500.json", "Nifty Bank": "nifty_bank.json",
           "Nifty Microcap 250": "nifty_microcap250.json"}
# roster names whose NSE key cannot be found by matching indices.json's display name
KEY_FALLBACK = {"Nifty Smallcap 100": "NIFTY SMLCAP 100", "Nifty Smallcap 250": "NIFTY SMLCAP 250",
                "Nifty LargeMidcap 250": "NIFTY LARGEMID250", "Nifty Consumer Durables": "NIFTY CONSR DURBL",
                "Nifty Oil & Gas": "NIFTY OIL AND GAS", "Nifty Microcap 250": "NIFTY MICROCAP250"}
FNO_NAME = "F&O stocks"


def gz_load(p):
    with open(p, "rb") as f:
        return json.loads(gzip.decompress(f.read()))


def js_load(name):
    p = os.path.join(DOCS, name)
    if not os.path.exists(p):
        return None
    with open(p, encoding="utf-8") as f:
        return json.load(f)


def gz_dump(obj, p):
    raw = json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", compresslevel=9, mtime=0) as g:
        g.write(raw)
    tmp = p + ".tmp"
    with open(tmp, "wb") as f:
        f.write(buf.getvalue())
    os.replace(tmp, p)
    return len(buf.getvalue()), len(raw)


def iso(d):               # 20261120 -> "2026-11-20"
    return "%04d-%02d-%02d" % (d // 10000, d // 100 % 100, d % 100)


def year_before(d):       # 20261120 -> 20251120 (29 Feb -> 28 Feb)
    y, m, dd = d // 10000 - 1, d // 100 % 100, d % 100
    if m == 2 and dd == 29:
        dd = 28
    return y * 10000 + m * 100 + dd


class Wilder:
    """Wilder RSI state over a stream of changes; peek() = RSI if `ch` were the next change."""
    __slots__ = ("n", "g", "l")

    def __init__(self):
        self.n = 0; self.g = 0.0; self.l = 0.0

    def _next(self, ch):
        up, dn = (ch, 0.0) if ch > 0 else (0.0, -ch)
        n = self.n + 1
        if n < RSI_N:
            return n, self.g + up, self.l + dn
        if n == RSI_N:
            return n, (self.g + up) / RSI_N, (self.l + dn) / RSI_N
        return n, (self.g * (RSI_N - 1) + up) / RSI_N, (self.l * (RSI_N - 1) + dn) / RSI_N

    def push(self, ch):
        self.n, self.g, self.l = self._next(ch)

    def peek(self, ch):
        n, g, l = self._next(ch)
        if n < RSI_N:
            return None
        if l == 0:
            return 100.0 if g > 0 else 50.0
        return 100.0 - 100.0 / (1.0 + g / l)

    def value(self):
        if self.n < RSI_N:
            return None
        if self.l == 0:
            return 100.0 if self.g > 0 else 50.0
        return 100.0 - 100.0 / (1.0 + self.g / self.l)


def rsi_series(dates, closes, period_key):
    """RSI on every bar. period_key=None -> daily bars. Otherwise bars are grouped by period_key(date)
    and the value on a bar = completed periods + the running period ending at that bar's close."""
    out = [None] * len(dates)
    w = Wilder()
    if period_key is None:
        prev = None
        for i, c in enumerate(closes):
            if c is None:
                continue
            if prev is not None:
                w.push(c - prev)
                out[i] = w.value()
            prev = c
        return out
    prev_close = None      # close of the last completed period
    cur_key = None; cur_last = None
    for i, c in enumerate(closes):
        if c is None:
            continue
        k = period_key(dates[i])
        if cur_key is not None and k != cur_key:      # the running period just completed
            if prev_close is not None:
                w.push(cur_last - prev_close)
            prev_close = cur_last
        cur_key = k; cur_last = c
        if prev_close is not None:
            out[i] = w.peek(c - prev_close)
    return out


def week_key(d):
    y, wk, _ = date(d // 10000, d // 100 % 100, d % 100).isocalendar()
    return y * 100 + wk


def month_key(d):
    return d // 100


def main():
    if not os.path.exists(BIN):
        sys.exit("missing " + BIN)
    D = gz_load(BIN)
    S = gz_load(SLIM)
    data, smeta = D["data"], D.get("meta", {})
    end = D["end"]

    # ---- rosters (latest snapshot of each index) + F&O list
    rosters = {}
    for name, snaps in (S.get("indicesHistory") or {}).items():
        if snaps:
            rosters[name] = {"eff": snaps[-1]["effectiveDate"], "m": list(snaps[-1]["symbols"])}
    fno = [s for s in (S.get("fnoToday") or [])]
    if fno:
        rosters[FNO_NAME] = {"eff": None, "m": fno}
    universe = set()
    for r in rosters.values():
        universe |= set(r["m"])
    missing = sorted(s for s in universe if s not in data or not data[s].get("d"))
    universe = sorted(s for s in universe if s not in missing)
    if len(universe) < 300:
        sys.exit("universe too small (%d): rosters unreadable?" % len(universe))

    # ---- day axis: dates on which at least half the universe has a bar
    cnt = {}
    for s in universe:
        for d in data[s]["d"][-(N + 80):]:
            cnt[d] = cnt.get(d, 0) + 1
    half = len(universe) / 2.0
    days = sorted(d for d, c in cnt.items() if c >= half)[-(N + 1):]
    if len(days) < 30:
        sys.exit("day axis too short (%d)" % len(days))
    if iso(days[-1]) != end:
        print("WARNING: last axis day %s != data end %s" % (iso(days[-1]), end))
    L = len(days)

    # ---- per stock
    slim_meta = S.get("meta") or {}
    stocks, extra = {}, {}
    for s in universe:
        r = data[s]
        d, c = r["d"], r["c"]
        h = r.get("h") or c
        l = r.get("l") or c
        v = r.get("v") or []
        dv = r.get("dv") or []
        rd = rsi_series(d, c, None)
        rw = rsi_series(d, c, week_key)
        rm = rsi_series(d, c, month_key)
        pos = {dd: i for i, dd in enumerate(d)}
        cc, hh, ll, vv, dd_, r1, r2, r3 = [], [], [], [], [], [], [], []
        for day in days:
            i = pos.get(day)
            if i is None or c[i] is None:
                for a in (cc, hh, ll, vv, dd_, r1, r2, r3):
                    a.append(None)
                continue
            lo = bisect_right(d, year_before(day))
            hi_vals = [(h[j] if j < len(h) and h[j] is not None else c[j]) for j in range(lo, i + 1) if c[j] is not None]
            lo_vals = [(l[j] if j < len(l) and l[j] is not None else c[j]) for j in range(lo, i + 1) if c[j] is not None]
            cc.append(int(round(c[i] * 100)))
            hh.append(int(round(max(hi_vals) * 100)))
            ll.append(int(round(min(lo_vals) * 100)))
            vv.append(int(v[i]) if i < len(v) and v[i] is not None else None)
            dd_.append(round(dv[i], 1) if i < len(dv) and dv[i] is not None else None)
            r1.append(None if rd[i] is None else round(rd[i], 1))
            r2.append(None if rw[i] is None else round(rw[i], 1))
            r3.append(None if rm[i] is None else round(rm[i], 1))
        m = smeta.get(s, {})
        sm = slim_meta.get(s + ".NS") or slim_meta.get(s + ".BO") or {}
        stocks[s] = {"n": m.get("name") or sm.get("name") or s,
                     "i": (m.get("ind") or sm.get("industry") or sm.get("sector") or "Uncategorized").strip(),
                     "c": cc, "hh": hh, "ll": ll}
        extra[s] = {"v": vv, "dv": dd_, "r": r1, "rw": r2, "rm": r3}

    # ---- index levels
    idx_json = js_load("indices.json") or {}
    by_name = {(x.get("n") or "").lower(): x for x in idx_json.get("indices", [])}
    hist = (js_load("indices_hist.json") or {}).get("daily", {})
    idx_asof = idx_json.get("asof") or idx_json.get("updated")
    unmapped = []
    for name, r in rosters.items():
        if name == FNO_NAME:
            continue
        key = (by_name.get(name.lower()) or {}).get("k") or KEY_FALLBACK.get(name)
        px = {}
        if key:
            for dt, row in hist.items():
                if row.get(key) is not None:
                    px[dt] = row[key]
        long_file = LONG_PX.get(name)
        if long_file:
            lp = (js_load(long_file) or {}).get("px") or {}
            for dt, val in lp.items():
                if val is not None:
                    px[dt] = val
        if not px:
            unmapped.append(name)
            continue
        r["lv"] = [px.get(iso(d)) for d in days]
        # rolling 52-week high / low of the CLOSE, only where a full year of closes sits behind the day
        dts = sorted(px)
        if long_file and len(dts) > 300:
            ih, il = [], []
            vals = [px[x] for x in dts]
            for d in days:
                hi_i = bisect_right(dts, iso(d))
                lo_i = bisect_right(dts, iso(year_before(d)))
                win = vals[lo_i:hi_i]
                ih.append(max(win) if win and px.get(iso(d)) is not None else None)
                il.append(min(win) if win and px.get(iso(d)) is not None else None)
            r["hh"], r["ll"], r["hl"] = ih, il, "close"
        x = by_name.get(name.lower()) or ({} if not key else next((y for y in idx_json.get("indices", []) if y.get("k") == key), {}))
        # NSE's own 52-week high / low (intraday): true for the latest day only
        if x.get("yh") is not None and x.get("yl") is not None:
            r["yh"], r["yl"] = x["yh"], x["yl"]
    if unmapped:
        print("no level history for:", ", ".join(unmapped))

    # ---- flows
    fd = {r["date"]: r for r in (js_load("fii_dii.json") or {}).get("rows", [])}
    fo = {r["date"]: r for r in (js_load("fii_fo.json") or {}).get("rows", [])}
    fii, dii, fl, fs = [], [], [], []
    for d in days:
        r = fd.get(iso(d)) or {}
        fii.append(r.get("fiiNet")); dii.append(r.get("diiNet"))
        f = (((fo.get(iso(d)) or {}).get("oi") or {}).get("FII") or {}).get("futIdx")
        fl.append(f[0] if f else None); fs.append(f[1] if f else None)

    # ---- ex-dividend dates (accumulated)
    exd = {}
    if os.path.exists(OUT):
        try:
            for s, lst in (gz_load(OUT).get("exd") or {}).items():
                for d, amt in lst:
                    exd.setdefault(s, {})[d] = amt
        except Exception as e:      # an unreadable previous file only loses the carried-over dots
            print("previous daily_moves.bin not readable (%s): ex-dates start fresh" % e)
    for row in (js_load("actions.json") or {}).get("rows", []):
        if len(row) >= 7 and row[3] == "D" and row[0]:
            d = int(row[0].replace("-", ""))
            exd.setdefault(row[1], {})[d] = row[6]
    uni = set(universe)
    dayset = set(days)
    exd_out = {}
    for s, m in exd.items():
        if s in uni:
            lst = sorted([d, a] for d, a in m.items() if d in dayset)
            if lst:
                exd_out[s] = lst

    core = {"v": 1, "end": iso(days[-1]), "days": days, "idx": rosters, "stocks": stocks,
            "fii": fii, "dii": dii, "fl": fl, "fs": fs, "exd": exd_out, "missing": missing,
            "idxAsof": idx_asof}
    b1, r1_ = gz_dump(core, OUT)
    b2, r2_ = gz_dump({"v": 1, "end": iso(days[-1]), "days": days, "stocks": extra}, OUTX)
    print("daily_moves.bin   %d stocks x %d days  %s..%s  %d B gz (%d raw)" % (len(stocks), L, iso(days[0]), iso(days[-1]), b1, r1_))
    print("daily_moves_x.bin %d B gz (%d raw)" % (b2, r2_))
    print("rosters %d, not in price store: %s" % (len(rosters), ", ".join(missing) or "none"))
    print("flows: FII/DII on %d of %d days, FII index futures on %d" % (sum(x is not None for x in fii), L, sum(x is not None for x in fl)))


if __name__ == "__main__":
    main()
