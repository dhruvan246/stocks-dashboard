# -*- coding: utf-8 -*-
"""Aggregate the 'results season' chart payload (Trendlyne-style): per quarter, the MEDIAN
YoY % across reporting companies for Revenue, Operating Profit and PAT, plus the count of
companies that declared results.

Universe (confirmed with the user): currently-listed companies with median daily turnover
>= Rs 1 crore over the last ~250 sessions — a clean, reproducible 'investable' set whose
per-quarter reporter counts (~1.2k-1.4k) bracket Trendlyne's headline numbers. Micro/illiquid
names are dropped; the median is robust either way.

Bases:
  PAT  -> fundamentals.json (owners-attributable consolidated where filed, else standalone) —
          same basis the backtest uses. ALL sectors.
  Rev / Operating profit -> revop_fundamentals.json. Banks/NBFCs/insurers EXCLUDED (no comparable
          revenue-from-operations / operating profit), matching Trendlyne.
YoY needs the year-ago quarter present on the SAME basis with a POSITIVE base (a negative/zero
base makes the % meaningless); such companies are dropped from that metric's median only.

Out: docs/results_season.json (tiny; inlined into the dashboard by build_compressed.py).
Run:  python -X utf8 build_results_season.py
"""
import os, json, gzip, statistics

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
# Read the DAILY-maintained web copies (the cron upserts these), falling back to the scripts/ source
# copies for a local full rebuild. PAT = sf_fundamentals.json (owners basis), rev/op = sf_revop.json.
FUND = os.path.join(ROOT, "docs", "sf_fundamentals.json")
if not os.path.exists(FUND): FUND = os.path.join(HERE, "fundamentals.json")
REVOP = os.path.join(ROOT, "docs", "sf_revop.json")
if not os.path.exists(REVOP): REVOP = os.path.join(HERE, "revop_fundamentals.json")
BIN = os.path.join(ROOT, "docs", "sf_stock_data.bin")
OUT = os.path.join(ROOT, "docs", "results_season.json")

TURN_FLOOR_CR = 1.0     # Rs crore/day median turnover
TURN_WINDOW = 250       # sessions

MON = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def quarter_label(qe):
    return "%s %d" % (MON[(qe // 100) % 100], qe // 10000)


def yago(qe):
    return qe - 10000


def build_universe():
    D = json.loads(gzip.decompress(open(BIN, "rb").read()))
    meta, data = D["meta"], D["data"]
    U = set()
    for s, m in meta.items():
        if not isinstance(m, dict) or m.get("alive") is False:
            continue
        ser = data.get(s)
        if not ser:
            continue
        c, v = ser.get("c"), ser.get("v")
        if not c or not v:
            continue
        n = min(len(c), len(v))
        vals = [c[i] * v[i] / 1e7 for i in range(max(0, n - TURN_WINDOW), n) if c[i] and v[i]]
        vals = [x for x in vals if x > 0]
        if vals and statistics.median(vals) >= TURN_FLOOR_CR:
            U.add(s)
    return U, D.get("end")


def consistent(cur, base):
    """Pick (cur,base) on one basis: consolidated if both present, else standalone, else None.
    cur/base are (std, con) tuples."""
    cs, cc = cur
    bs, bc = base
    if cc is not None and bc is not None:
        return cc, bc
    if cs is not None and bs is not None:
        return cs, bs
    return None


def median_yoy(pairs):
    ys = []
    for cv, bv in pairs:
        if bv is not None and bv > 0 and cv is not None:
            ys.append((cv - bv) / bv * 100.0)
    return (round(statistics.median(ys), 1) if ys else None), len(ys)


def main():
    U, end = build_universe()
    print("universe (alive, turnover>=%.1f cr): %d" % (TURN_FLOOR_CR, len(U)))

    fund = json.load(open(FUND))
    revop = json.load(open(REVOP))

    # PAT per symbol: qe -> (npStd, npCon)
    pat = {}
    for s, rows in fund.items():
        pat[s] = {r[0]: (r[1], r[3]) for r in rows}

    # quarters: standard quarter-ends from Jun-2023 to the latest with enough U reporters
    cand = []
    y, m = 2023, 6
    while (y, m) <= (2026, 12):
        cand.append(y * 10000 + m * 100 + {3: 31, 6: 30, 9: 30, 12: 31}[m])
        m += 3
        if m > 12:
            m = 3; y += 1
    quarters = []
    for qe in cand:
        rep = sum(1 for s in U if qe in pat.get(s, {}) and any(x is not None for x in pat[s][qe]))
        if rep >= 200:
            quarters.append(qe)

    out = []
    for qe in quarters:
        ya = yago(qe)
        reported = 0
        pat_pairs, rev_pairs, op_pairs = [], [], []
        for s in U:
            pq = pat.get(s, {})
            cur_pat = pq.get(qe)
            if cur_pat and any(x is not None for x in cur_pat):
                reported += 1
            base_pat = pq.get(ya)
            if cur_pat and base_pat:
                pr = consistent(cur_pat, base_pat)
                if pr:
                    pat_pairs.append(pr)
            rv = revop.get(s)
            if rv:
                cq, bq = rv.get(str(qe)), rv.get(str(ya))
                if cq and bq and not cq[6] and not bq[6]:   # both non-financial
                    pr = consistent((cq[0], cq[1]), (bq[0], bq[1]))   # revStd, revCon
                    if pr:
                        rev_pairs.append(pr)
                    po = consistent((cq[2], cq[3]), (bq[2], bq[3]))   # opStd, opCon
                    if po:
                        op_pairs.append(po)
        rev_m, rev_n = median_yoy(rev_pairs)
        op_m, op_n = median_yoy(op_pairs)
        pat_m, pat_n = median_yoy(pat_pairs)
        out.append({"qe": qe, "label": quarter_label(qe), "reported": reported,
                    "rev": {"median": rev_m, "n": rev_n},
                    "op": {"median": op_m, "n": op_n},
                    "pat": {"median": pat_m, "n": pat_n}})
        print("  %s  reported=%d  rev=%s(%d)  op=%s(%d)  pat=%s(%d)"
              % (quarter_label(qe), reported, rev_m, rev_n, op_m, op_n, pat_m, pat_n))

    payload = {
        "universe": "Currently-listed, median daily turnover >= Rs 1 cr (last ~250 sessions)",
        "universeSize": len(U),
        "basis": "Consolidated where filed (PAT owners-attributable), else standalone; YoY vs year-ago quarter (positive base). Revenue & Operating profit exclude banks/NBFCs/insurers.",
        "dataAsOf": end,
        "quarters": out,
    }
    json.dump(payload, open(OUT, "w"), separators=(",", ":"))
    print("Wrote %s (%d quarters)" % (OUT, len(out)))


if __name__ == "__main__":
    main()
