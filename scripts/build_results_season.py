# -*- coding: utf-8 -*-
"""Aggregate the 'results season' chart payload: per quarter, the MEDIAN YoY % across reporting
companies for Revenue, Operating Profit and PAT, + the count that declared results — for MANY
universes: an 'all liquid' set PLUS every NSE index (point-in-time membership).

Universes:
  - "liquid": currently-listed companies with median daily turnover >= Rs 1 cr (~250 sessions).
  - each index in scripts/indices_history.json (Nifty 50 / 500 / Midcap / Smallcap / sectoral …):
    members are POINT-IN-TIME — whoever was in the index at that quarter's most recent rebalance
    (membersAsOf), so there is no survivorship bias (today's members are NOT applied to 2019).

Bases:
  PAT  -> sf_fundamentals.json (owners-attributable consolidated where filed, else standalone).
  Rev / Operating profit -> sf_revop.json. Banks/NBFCs EXCLUDED per metric (no comparable revenue /
          operating profit) — so financial-heavy sectoral indexes show PAT only.
YoY needs the year-ago quarter on the SAME basis with a POSITIVE base.

Out: docs/results_season.json = { defaultUniverse, basis, dataAsOf, universes:[{key,label,note,quarters}] }
Run:  python -X utf8 build_results_season.py
"""
import os, json, gzip, statistics

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
FUND = os.path.join(ROOT, "docs", "sf_fundamentals.json")
if not os.path.exists(FUND): FUND = os.path.join(HERE, "fundamentals.json")
REVOP = os.path.join(ROOT, "docs", "sf_revop.json")
if not os.path.exists(REVOP): REVOP = os.path.join(HERE, "revop_fundamentals.json")
BIN = os.path.join(ROOT, "docs", "sf_stock_data.bin")
INDICES = os.path.join(HERE, "indices_history.json")
RENAME = os.path.join(HERE, "_rename_map.json")
OUT = os.path.join(ROOT, "docs", "results_season.json")

TURN_FLOOR_CR = 1.0
TURN_WINDOW = 250
MON = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def quarter_label(qe):
    return "%s %d" % (MON[(qe // 100) % 100], qe // 10000)


def yago(qe):
    return qe - 10000


def iso(qe):
    s = str(qe)
    return "%s-%s-%s" % (s[:4], s[4:6], s[6:])


def build_liquid_universe():
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
    cs, cc = cur
    bs, bc = base
    if cc is not None and bc is not None:
        return cc, bc
    if cs is not None and bs is not None:
        return cs, bs
    return None


MIN_N = 5   # don't publish a median computed over fewer than this many companies (not robust)


def median_yoy(pairs):
    ys = [(cv - bv) / bv * 100.0 for cv, bv in pairs if bv is not None and bv > 0 and cv is not None]
    return (round(statistics.median(ys), 1) if len(ys) >= MIN_N else None), len(ys)


def agg_quarter(members, qe, pat, revop):
    """Return (quarter-dict, reported) for one member set + quarter."""
    ya = yago(qe)
    reported = 0
    pat_pairs, rev_pairs, op_pairs = [], [], []
    for s in members:
        pq = pat.get(s)
        if pq:
            cur_pat = pq.get(qe)
            if cur_pat and (cur_pat[0] is not None or cur_pat[1] is not None):
                reported += 1
                base_pat = pq.get(ya)
                if base_pat:
                    pr = consistent(cur_pat, base_pat)
                    if pr:
                        pat_pairs.append(pr)
        rv = revop.get(s)
        if rv:
            cq, bq = rv.get(str(qe)), rv.get(str(ya))
            if cq and bq and not cq[6] and not bq[6]:
                pr = consistent((cq[0], cq[1]), (bq[0], bq[1]))
                if pr:
                    rev_pairs.append(pr)
                po = consistent((cq[2], cq[3]), (bq[2], bq[3]))
                if po:
                    op_pairs.append(po)
    rev_m, rev_n = median_yoy(rev_pairs)
    op_m, op_n = median_yoy(op_pairs)
    pat_m, pat_n = median_yoy(pat_pairs)
    return {"qe": qe, "label": quarter_label(qe), "reported": reported,
            "rev": {"median": rev_m, "n": rev_n},
            "op": {"median": op_m, "n": op_n},
            "pat": {"median": pat_m, "n": pat_n}}, reported


def main():
    U, end = build_liquid_universe()
    print("liquid universe (alive, turnover>=%.1f cr): %d" % (TURN_FLOOR_CR, len(U)))

    fund = json.load(open(FUND))
    revop = json.load(open(REVOP))
    indices = json.load(open(INDICES, encoding="utf-8"))
    try:
        rename = json.load(open(RENAME, encoding="utf-8"))
    except Exception:
        rename = {}

    pat = {s: {r[0]: (r[1], r[3]) for r in rows} for s, rows in fund.items()}

    # candidate quarter-ends: Mar-2019 (earliest with a 2018 year-ago base) to latest
    cand = []
    y, m = 2019, 3
    while (y, m) <= (2026, 12):
        cand.append(y * 10000 + m * 100 + {3: 31, 6: 30, 9: 30, 12: 31}[m])
        m += 3
        if m > 12:
            m = 3; y += 1

    def snap_as_of(snaps, ymd):
        chosen = None
        for snp in snaps:
            if snp.get("effectiveDate", "9") <= ymd:
                chosen = snp
        return {rename.get(s, s) for s in chosen["symbols"]} if chosen else set()

    def build(label, key, note, member_fn, min_rep):
        qs = []
        for qe in cand:
            members = member_fn(qe)
            if not members:
                continue
            row, reported = agg_quarter(members, qe, pat, revop)
            if reported >= min_rep:
                qs.append(row)
        return {"key": key, "label": label, "note": note, "quarters": qs}

    universes = []
    # 1) the liquid universe (default)
    universes.append(build(
        "All liquid stocks (₹1cr+/day)", "liquid",
        "Currently-listed companies trading ≥ ₹1 cr/day (median, ~250 sessions) — %d names." % len(U),
        lambda qe: U, 200))
    # 2) every index, point-in-time membership
    for index, snaps in indices.items():
        if not isinstance(snaps, list) or not snaps:
            continue
        universes.append(build(
            index, index,
            "Point-in-time %s members at each quarter's rebalance (survivorship-free)." % index,
            (lambda snps: (lambda qe: snap_as_of(snps, iso(qe))))(snaps), 5))

    universes = [u for u in universes if u["quarters"]]
    for u in universes:
        q = u["quarters"]
        print("  %-24s %2d quarters  latest %s reported=%d" % (
            u["label"], len(q), q[-1]["label"] if q else "-", q[-1]["reported"] if q else 0))

    payload = {
        "defaultUniverse": "liquid",
        "basis": "Median YoY vs the year-ago quarter (positive base). PAT = consolidated owners-attributable "
                 "where filed, else standalone. Revenue & Operating profit exclude banks/NBFCs. Index universes "
                 "use point-in-time membership (whoever was in the index at each quarter's rebalance).",
        "dataAsOf": end,
        "universes": universes,
    }
    json.dump(payload, open(OUT, "w"), separators=(",", ":"))
    print("Wrote %s (%d universes)" % (OUT, len(universes)))


if __name__ == "__main__":
    main()
