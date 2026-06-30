# -*- coding: utf-8 -*-
"""
Precompute a monthly TOTAL TRADED VALUE (turnover) time series for the Nifty 500.

Output: docs/nifty500_turnover.json  -- a small minified JSON the chart reads:
  { "unit":"cr", "maWindow":12, "updated":"YYYY-MM-DD",
    "source":"...", "months":[...], "value":[<crore>...], "ma":[<crore>...] }

For each calendar month we sum, across the point-in-time Nifty 500 constituents,
each member's (mean observed daily turnover that month) * (number of distinct
market trading days that month). The mean*D form normalises the pre-2018
weekly-sampled deep-history era so monthly totals aren't understated; in the
daily era mean*D == sum.

INPUTS (both verified against their real on-disk format, not assumed):

  docs/sf_stock_data.bin  -- gzip JSON written by build_sf_data.py. Top-level:
      { "start","dailyFrom","end",  "meta":{SYM:{...}},  "data":{SYM:{ "d":[YYYYMMDD ints],
        "t":[turnover], "c":[...], "v":[volume], ... }} }
    *** TURNOVER UNIT IS MIXED, by source file, NOT uniform lakhs ***
      - Old NSE zip bhavcopy (TOTTRDVAL)  -> turnover in raw RUPEES   (≈ all of 1996-2019 + a
        few stray days like 2022-08-08/09 where NSE served the old file).
      - New sec_bhavdata_full (TURNOVER_LACS) -> turnover in LAKHS    (≈ all of 2020-01-01+, plus
        a scattering of stray new-file days in 2013-2019).
    The two are interleaved by which file NSE returned that day, so a single date cutoff is wrong.
    The unit is a WHOLE-DAY property: every stock on a given date shares one unit. So we classify
    PER DATE, not per row. For each date take the median of (turnover/volume) over liquid rows
    (volume>=10000): that ratio is ~ the average traded price. In a rupees-day it's in the
    hundreds (>>1); in a lakhs-day it's price/1e5 (<<1). A per-row test fails on sub-Re-1 penny
    stocks (e.g. RASOYPR ~Re0.9: rupees turnover but t/v<1, which would wrongly read as lakhs and
    inflate it 1e5x); the per-date median over many liquid stocks is immune to that. When a date's
    liquid sample is too thin (<5 rows, common in the pre-2018 weekly-sampled era) or the median
    lands in an ambiguous band, we fall back to the era rule (>=2020-01-01 => lakhs). Rupees-days
    are /1e5 to lakhs; everything is summed in LAKHS, then /100 -> CRORE.

  scripts/_n500_master_history.json  -- {"YYYY-MM-DD":[SYMBOL,...]}, 19 archived NSE/
    niftyindices full constituent lists, 2006-11-08 .. 2026-06-12. Symbols match the bin's
    data keys. (A few junk entries -- a leaked 'Symbol' header, DUMMYVEDL* placeholders --
    simply aren't in data and drop out.)

SIMPLIFICATION (v1): members(M) = the snapshot whose date is the latest <= last day of
  month M (nearest-prior). We do NOT replay the _n500_changes.json change-events between
  snapshots. Total turnover is dominated by large, stable members, so membership drift
  barely moves the total; this keeps the pipeline simple and robust.

Run:  python -X utf8 build_nifty500_turnover.py
"""
import os, json, gzip, statistics
from collections import defaultdict
from datetime import date

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
BIN = os.path.join(ROOT, "docs", "sf_stock_data.bin")
MEMB = os.path.join(HERE, "_n500_master_history.json")
OUT = os.path.join(ROOT, "docs", "nifty500_turnover.json")

START_YM = 200910        # first calendar month emitted (YYYYMM)
MA_WINDOW = 12           # trailing simple-moving-average window, in months
MIN_TRADING_DAYS = 10    # drop a trailing month with fewer distinct observed trading days
LIQ_VOL = 10000          # min volume for a row to inform the per-date unit verdict
MIN_LIQ = 5              # need this many liquid rows to trust the per-date median
ERA_LAKHS_FROM = 20200101  # fallback rule when the per-date sample is thin/ambiguous


def classify_units(data):
    """Decide LAKHS vs RUPEES once PER TRADING DATE -> {YYYYMMDD: is_lakhs(bool)}.

    median(turnover/volume) over liquid rows ~ avg traded price: >>1 in a rupees-day,
    <<1 (price/1e5) in a lakhs-day. Thin sample or an ambiguous median (0.1..10) ->
    fall back to the era rule (date >= ERA_LAKHS_FROM => lakhs)."""
    ratios = defaultdict(list)
    for e in data.values():
        ds, ts = e["d"], e["t"]
        vs = e.get("v") or [0] * len(ds)
        for i in range(len(ds)):
            v = vs[i] if i < len(vs) else 0
            if ts[i] > 0 and v >= LIQ_VOL:
                ratios[ds[i]].append(ts[i] / v)
    is_lakhs = {}
    for ymd, rs in ratios.items():
        if len(rs) >= MIN_LIQ:
            med = statistics.median(rs)
            if med < 0.1:
                is_lakhs[ymd] = True;  continue
            if med > 10:
                is_lakhs[ymd] = False; continue
        is_lakhs[ymd] = ymd >= ERA_LAKHS_FROM
    return is_lakhs


def last_day_of_month(ym):
    y, m = ym // 100, ym % 100
    if m == 12:
        return date(y, 12, 31)
    return date(y, m + 1, 1).fromordinal(date(y, m + 1, 1).toordinal() - 1)


def month_iter(start_ym, end_ym):
    ym = start_ym
    while ym <= end_ym:
        yield ym
        y, m = ym // 100, ym % 100
        ym = (y + 1) * 100 + 1 if m == 12 else y * 100 + (m + 1)


def main():
    D = json.loads(gzip.decompress(open(BIN, "rb").read()))
    data = D["data"]
    end_iso = D.get("end")  # data-freshness stamp written by build_sf_data.py
    end_dt = date.fromisoformat(end_iso)
    print("bin: %d symbols, data end = %s" % (len(data), end_iso), flush=True)

    # --- membership snapshots, sorted by date ---
    raw = json.load(open(MEMB))
    snaps = sorted((date.fromisoformat(k), set(v)) for k, v in raw.items())
    print("membership: %d snapshots, %s .. %s" %
          (len(snaps), snaps[0][0].isoformat(), snaps[-1][0].isoformat()), flush=True)

    def members_for(lastday):
        """Nearest-prior snapshot: latest snapshot dated <= lastday (fallback: earliest)."""
        chosen = snaps[0][1]
        for sd, syms in snaps:
            if sd <= lastday:
                chosen = syms
            else:
                break
        return chosen

    # --- per-date unit verdict (lakhs vs rupees), computed once ---
    is_lakhs = classify_units(data)
    n_l = sum(1 for v in is_lakhs.values() if v)
    print("unit verdict: %d dates classified (%d lakhs-days, %d rupees-days)" %
          (len(is_lakhs), n_l, len(is_lakhs) - n_l), flush=True)

    # --- per-symbol monthly aggregates + true distinct trading days per month ---
    # sym_month[sym][ym] = (sum_turnover_lakhs, n_obs)  -> mean = sum/n
    # trading_days[ym]   = set of distinct YYYYMMDD seen across ALL symbols that month
    sym_month = defaultdict(lambda: defaultdict(lambda: [0.0, 0]))
    trading_days = defaultdict(set)
    for sym, e in data.items():
        ds, ts = e["d"], e["t"]
        sm = sym_month[sym]
        for i in range(len(ds)):
            ymd = ds[i]
            ym = ymd // 100
            trading_days[ym].add(ymd)
            t = ts[i]
            if t <= 0:
                continue
            # normalise to LAKHS using the whole-day unit verdict
            lk = t if is_lakhs.get(ymd, ymd >= ERA_LAKHS_FROM) else t / 1e5
            cell = sm[ym]
            cell[0] += lk
            cell[1] += 1

    # --- include the CURRENT month as a running MONTH-TO-DATE total (sum of the
    #     1st -> latest trading day). It grows every day and becomes "complete" once
    #     the data rolls into the next month. The LAST month in the series is always
    #     the live/partial one; every earlier month is a finished full month. ---
    end_ym = end_dt.year * 100 + end_dt.month   # month of the latest data -> include it (live)

    # --- build the monthly total series (last month = month-to-date) ---
    months, value = [], []
    for ym in month_iter(START_YM, end_ym):
        D_m = len(trading_days.get(ym, ()))
        if D_m == 0:
            continue  # no market data at all this month
        lastday = last_day_of_month(ym)
        members = members_for(lastday)
        total_lakhs = 0.0
        for s in members:
            cell = sym_month.get(s, {}).get(ym)
            if cell and cell[1] > 0:
                mean_l = cell[0] / cell[1]
                # mean(observed daily turnover) * (#trading days). For the live month
                # D_m = days seen SO FAR, so this equals the raw month-to-date sum.
                total_lakhs += mean_l * D_m
        months.append("%04d-%02d" % (ym // 100, ym % 100))
        value.append(round(total_lakhs / 100.0))  # lakhs -> crore, whole crore

    # the last month is the live, still-accumulating one (month-to-date)
    partial_last = len(months) > 0
    partial_days = len(trading_days.get(end_ym, ())) if partial_last else 0

    # --- trailing 12-month SMA. For the live partial month, average the 12 COMPLETE
    #     months *before* it so a half-finished month doesn't drag the trend down. ---
    ma = []
    n = len(value)
    for i in range(n):
        if partial_last and i == n - 1:
            window = value[max(0, i - MA_WINDOW): i]       # 12 complete months before the live one
        else:
            window = value[max(0, i - MA_WINDOW + 1): i + 1]
        ma.append(round(sum(window) / len(window)) if window else value[i])

    out = {
        "unit": "cr",
        "maWindow": MA_WINDOW,
        "updated": end_iso,             # latest data date — the live month is current to here
        "dataEnd": end_iso,
        "partialLast": partial_last,    # last month is month-to-date (still accumulating)
        "partialDays": partial_days,    # trading days summed so far in the live month
        "source": "NSE bhavcopy turnover, point-in-time Nifty 500 (nearest-snapshot)",
        "months": months,
        "value": value,
        "ma": ma,
    }
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(out, f, separators=(",", ":"))
    print("Wrote %s  (%d months, %s .. %s)" %
          (OUT, len(months), months[0], months[-1]), flush=True)

    # --- sanity report ---
    by_year = defaultdict(list)
    for m, v in zip(months, value):
        by_year[m[:4]].append(v)
    print("Yearly average of monthly value (cr):", flush=True)
    for y in sorted(by_year):
        ys = by_year[y]
        print("  %s: %d  (n=%d)" % (y, round(sum(ys) / len(ys)), len(ys)), flush=True)
    print("latest month %s: value=%d cr  ma=%d cr" % (months[-1], value[-1], ma[-1]), flush=True)
    print("min value=%d cr  max value=%d cr" % (min(value), max(value)), flush=True)


if __name__ == "__main__":
    main()
