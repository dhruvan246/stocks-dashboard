# -*- coding: utf-8 -*-
"""
Precompute DAILY MARKET BREADTH for the Nifty 500 (point-in-time membership).

Output: docs/market_breadth.json -- minified JSON the Market Mood page reads:
  { "updated":"YYYY-MM-DD", "dataEnd":"YYYY-MM-DD", "source":"...",
    "dates":[YYYYMMDD ints...],
    "pct200":[% of eligible members closing ABOVE their 200-DMA, 1dp],
    "n200":[eligible members that day (>=200 daily closes)],
    "adv":[members that closed UP vs their previous close],
    "dec":[members that closed DOWN],
    "hi":[members making a NEW 52-week high (close == max of last 252 sessions)],
    "lo":[members making a NEW 52-week low],
    "n52":[members eligible for the 52w test (>=252 daily closes)] }

METHOD / CONVENTIONS
  - Same inputs as build_nifty500_turnover.py: docs/sf_stock_data.bin (survivorship-free,
    closes are corp-action adjusted by build_sf_data.py) + scripts/_n500_master_history.json
    (19 archived full constituent lists 2006..date; nearest-prior snapshot per date).
  - DAILY ERA ONLY: the bin is weekly-sampled before top-level `dailyFrom` (2018-01-01).
    A 200-DMA over weekly samples would span ~4 years, so every symbol's series is sliced
    at dailyFrom and the output starts once the 52-week window has filled for most members
    (n52 >= MIN_UNIVERSE, i.e. ~Jan-2019).
  - Windows are OBSERVATION-based (200 / 252 of the symbol's own trading days, including
    today) — same convention as the backtest engine's d52 (snap-to-trading-day, bc532f9).
  - New 52w high = today's close equals the max close of its trailing 252-session window
    (ties count); low symmetrical. adv/dec compare each member's close to its own previous
    observed close (flat closes count as neither).
  - A member contributes only on dates it was a point-in-time member AND traded. Glitch
    dates with <MIN_OBS_DATE member observations are dropped.

Run:  python -X utf8 scripts/build_market_breadth.py
      SF_BIN=/path/to/fresh.bin python -X utf8 scripts/build_market_breadth.py
"""
import os, json, gzip
from bisect import bisect_right
from collections import deque
from datetime import date

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
BIN = os.environ.get("SF_BIN") or os.path.join(ROOT, "docs", "sf_stock_data.bin")
MEMB = os.path.join(HERE, "_n500_master_history.json")
OUT = os.path.join(ROOT, "docs", "market_breadth.json")

DMA_WIN = 200        # sessions in the moving average (includes today)
HL_WIN = 252         # sessions in the 52-week high/low window (includes today)
MIN_UNIVERSE = 300   # output starts when this many members pass the 52w eligibility
MIN_OBS_DATE = 50    # drop glitch dates with fewer member observations than this


def main():
    D = json.loads(gzip.decompress(open(BIN, "rb").read()))
    data, end_iso = D["data"], D["end"]
    daily_from = int(D.get("dailyFrom", "2018-01-01").replace("-", ""))
    print("bin: %d symbols, dailyFrom=%d, end=%s" % (len(data), daily_from, end_iso), flush=True)

    # --- membership snapshots (nearest-prior per date) ---
    raw = json.load(open(MEMB))
    snaps = sorted((int(k.replace("-", "")), frozenset(v)) for k, v in raw.items())
    snap_dates = [s[0] for s in snaps]
    snap_sets = [s[1] for s in snaps]
    union = set().union(*snap_sets)
    print("membership: %d snapshots, union %d symbols" % (len(snaps), len(union)), flush=True)

    # --- global trading-day axis = union of member dates in the daily era ---
    all_dates = set()
    for sym in union:
        e = data.get(sym)
        if e:
            all_dates.update(d for d in e["d"] if d >= daily_from)
    dates = sorted(all_dates)
    didx = {d: i for i, d in enumerate(dates)}
    n = len(dates)
    print("axis: %d trading days %d..%d" % (n, dates[0], dates[-1]), flush=True)

    # which membership snapshot applies on each axis date (nearest prior; earliest as floor)
    snap_of = [max(0, bisect_right(snap_dates, d) - 1) for d in dates]

    above200 = [0]*n; n200 = [0]*n
    adv = [0]*n; dec = [0]*n
    hi = [0]*n; lo = [0]*n; n52 = [0]*n
    obs = [0]*n

    for sym in union:
        e = data.get(sym)
        if not e:
            continue
        ds, cs = e["d"], e["c"]
        s200 = 0.0            # rolling sum of last DMA_WIN closes
        win = deque()         # the closes in the DMA window
        mx = deque(); mn = deque()   # monotonic (value) deques of (idx, close) for HL_WIN
        k = 0                 # observation counter within the daily era
        prev = None
        for j in range(len(ds)):
            d = ds[j]
            if d < daily_from:
                continue
            c = cs[j]
            if not c or c <= 0:
                continue
            gi = didx[d]
            k += 1
            # rolling 200-sum
            win.append(c); s200 += c
            if len(win) > DMA_WIN:
                s200 -= win.popleft()
            # rolling 252 max/min (monotonic deques keyed by observation number)
            while mx and mx[-1][1] <= c: mx.pop()
            mx.append((k, c))
            while mn and mn[-1][1] >= c: mn.pop()
            mn.append((k, c))
            while mx[0][0] <= k - HL_WIN: mx.popleft()
            while mn[0][0] <= k - HL_WIN: mn.popleft()

            if sym in snap_sets[snap_of[gi]]:
                obs[gi] += 1
                if prev is not None:
                    if c > prev: adv[gi] += 1
                    elif c < prev: dec[gi] += 1
                if k >= DMA_WIN:
                    n200[gi] += 1
                    if c > s200 / DMA_WIN: above200[gi] += 1
                if k >= HL_WIN:
                    n52[gi] += 1
                    if c >= mx[0][1]: hi[gi] += 1
                    if c <= mn[0][1]: lo[gi] += 1
            prev = c

    # --- trim: start once the 52w universe is representative; drop glitch dates ---
    start = next((i for i in range(n) if n52[i] >= MIN_UNIVERSE), None)
    if start is None:
        raise SystemExit("breadth universe never reached MIN_UNIVERSE — check inputs")
    keep = [i for i in range(start, n) if obs[i] >= MIN_OBS_DATE]
    pick = lambda a: [a[i] for i in keep]

    out = {
        "updated": end_iso,
        "dataEnd": end_iso,
        "source": "NSE bhavcopy closes (corp-action adjusted), point-in-time Nifty 500; daily era %d+" % daily_from,
        "dmaWin": DMA_WIN, "hlWin": HL_WIN,
        "dates": pick(dates),
        "pct200": [round(100.0 * above200[i] / n200[i], 1) if n200[i] else None for i in keep],
        "n200": pick(n200),
        "adv": pick(adv), "dec": pick(dec),
        "hi": pick(hi), "lo": pick(lo), "n52": pick(n52),
    }
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(out, f, separators=(",", ":"))
    kb = os.path.getsize(OUT) / 1024.0
    print("Wrote %s (%.0f KB, %d days %d..%d)" % (OUT, kb, len(keep), out["dates"][0], out["dates"][-1]), flush=True)

    # --- sanity report ---
    p, dts = out["pct200"], out["dates"]
    imin = min(range(len(p)), key=lambda i: p[i]); imax = max(range(len(p)), key=lambda i: p[i])
    print("latest %d: pct200=%.1f%% (n=%d)  adv/dec=%d/%d  hi/lo=%d/%d (n52=%d)" %
          (dts[-1], p[-1], out["n200"][-1], out["adv"][-1], out["dec"][-1], out["hi"][-1], out["lo"][-1], out["n52"][-1]), flush=True)
    print("pct200 min %.1f%% on %d | max %.1f%% on %d" % (p[imin], dts[imin], p[imax], dts[imax]), flush=True)
    worst_lo = max(range(len(p)), key=lambda i: out["lo"][i])
    print("most new 52w lows: %d on %d | most new highs: %d on %d" %
          (out["lo"][worst_lo], dts[worst_lo], max(out["hi"]), dts[max(range(len(p)), key=lambda i: out["hi"][i])]), flush=True)


if __name__ == "__main__":
    main()
