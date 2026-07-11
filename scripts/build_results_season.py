# -*- coding: utf-8 -*-
"""Aggregate the 'results season' chart payload: per quarter, the YoY % across reporting companies
for Revenue, Operating Profit (op = PBET+FC+Dep−OI = Trendlyne's 'Oper Profit', paisa-matched — NOT
their proprietary 'EBIDT') and PAT, + a detail-table EBIT (= op − Dep), + the count that declared
results — for MANY universes: an 'all liquid' set PLUS every NSE index (point-in-time membership).
Each metric carries BOTH median and total.

Universes:
  - "liquid": currently-listed companies with median daily turnover >= Rs 1 cr (~250 sessions).
  - each index in scripts/indices_history.json (Nifty 50 / 500 / Midcap / Smallcap / sectoral …):
    members are POINT-IN-TIME — whoever was in the index at that quarter's most recent rebalance
    (membersAsOf), so there is no survivorship bias (today's members are NOT applied to 2019).

Bases:
  PAT  -> sf_fundamentals.json (owners-attributable consolidated where filed, else standalone).
  Rev / Operating profit -> sf_revop.json. Banks/NBFCs INCLUDED (bank rev = interest earned, op =
          pre-provision operating profit; NBFC rev = core revenue, op = EBITDA) — matches Trendlyne.
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


def median_yoy(pairs, min_n=MIN_N):
    ys = [(cv - bv) / bv * 100.0 for cv, bv in pairs if bv is not None and bv > 0 and cv is not None]
    return (round(statistics.median(ys), 1) if len(ys) >= min_n else None), len(ys)


def _cpref(con, std):
    """Consolidated-preferred pick: con where filed, else standalone."""
    return con if con is not None else std


def agg_total(pairs, min_n=MIN_N):
    """Aggregate (total-based, Trendlyne-style) growth: sum(now)/sum(ago)-1, on the pairs given
    (CONSOLIDATED-preferred basis, Trendlyne-match). Aggregate only companies PROFITABLE IN BOTH periods (base AND current > 0) —
    a loss/near-zero base blows the ratio up (mid/small-cap PAT read 72%), and filtering base-only made
    it swing the other way (Nifty 500 PAT 1.9%, Smallcap -15%). Both>0 is the standard stable 'total
    profit growth'. Revenue/op are unaffected (both always > 0)."""
    pairs = [(n, a) for n, a in pairs if a > 0 and n > 0]
    sn = sum(n for n, a in pairs)
    sa = sum(a for n, a in pairs)
    return (round((sn / sa - 1) * 100, 1) if (sa > 0 and len(pairs) >= min_n) else None), len(pairs)


def agg_quarter(members, qe, pat, revop, min_n=MIN_N, include_fin=False):
    """Return (quarter-dict, reported). Each metric carries BOTH:
      median  = median of per-company YoY (consolidated-preferred, positive base) — 'typical company'
      total   = aggregate sum(now)/sum(ago)-1 on CONSOLIDATED-preferred basis — the index P&L (matches Trendlyne)."""
    ya = yago(qe)
    reported = 0
    pat_pairs, rev_pairs, op_pairs, ebit_pairs = [], [], [], []   # median (con-pref, positive base)
    pat_tot, rev_tot, op_tot, ebit_tot = [], [], [], []           # total (con-preferred, both>0)
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
                    # TOTAL is CONSOLIDATED-preferred (con where filed, else std) — Trendlyne's
                    # result-analysis cards + per-stock table use consolidated (TCS Q1FY27 rev
                    # 72,275 = con, not std 59,553); con-preferred reconciles to their cards to the
                    # decimal, standalone did not.
                    cp, bp = _cpref(cur_pat[1], cur_pat[0]), _cpref(base_pat[1], base_pat[0])
                    if cp is not None and bp is not None:
                        pat_tot.append((cp, bp))
        rv = revop.get(s)
        if rv:
            cq, bq = rv.get(str(qe)), rv.get(str(ya))
            # Financials INCLUDED when include_fin (banks: rev = interest earned, op = pre-provision;
            # NBFCs: core rev + EBITDA) — matches Trendlyne's index cards. Mixed universes without the
            # flag exclude them (industrial rev/EBITDA aren't comparable to bank interest income).
            if cq and bq and (include_fin or (not cq[6] and not bq[6])):
                pr = consistent((cq[0], cq[1]), (bq[0], bq[1]))
                if pr:
                    rev_pairs.append(pr)
                po = consistent((cq[2], cq[3]), (bq[2], bq[3]))
                if po:
                    op_pairs.append(po)
                # EBIT (after-dep operating profit) lives at idx 7/8 — defensive for legacy 7-elem rows
                ce_s, ce_c = (cq[7] if len(cq) > 7 else None), (cq[8] if len(cq) > 8 else None)
                be_s, be_c = (bq[7] if len(bq) > 7 else None), (bq[8] if len(bq) > 8 else None)
                pe = consistent((ce_s, ce_c), (be_s, be_c))
                if pe:
                    ebit_pairs.append(pe)
                cr_r, br_r = _cpref(cq[1], cq[0]), _cpref(bq[1], bq[0])          # revenue con-pref
                if cr_r is not None and br_r is not None:
                    rev_tot.append((cr_r, br_r))
                cr_o, br_o = _cpref(cq[3], cq[2]), _cpref(bq[3], bq[2])          # EBITDA con-pref
                if cr_o is not None and br_o is not None:
                    op_tot.append((cr_o, br_o))
                cr_e, br_e = _cpref(ce_c, ce_s), _cpref(be_c, be_s)             # EBIT con-pref
                if cr_e is not None and br_e is not None:
                    ebit_tot.append((cr_e, br_e))
    rev_m, rev_n = median_yoy(rev_pairs, min_n);   rev_t, rev_tn = agg_total(rev_tot, min_n)
    op_m, op_n = median_yoy(op_pairs, min_n);       op_t, op_tn = agg_total(op_tot, min_n)
    ebit_m, ebit_n = median_yoy(ebit_pairs, min_n); ebit_t, ebit_tn = agg_total(ebit_tot, min_n)
    pat_m, pat_n = median_yoy(pat_pairs, min_n);    pat_t, pat_tn = agg_total(pat_tot, min_n)
    return {"qe": qe, "label": quarter_label(qe), "reported": reported,
            "rev": {"median": rev_m, "n": rev_n, "total": rev_t, "tn": rev_tn},
            "op": {"median": op_m, "n": op_n, "total": op_t, "tn": op_tn},
            "ebit": {"median": ebit_m, "n": ebit_n, "total": ebit_t, "tn": ebit_tn},
            "pat": {"median": pat_m, "n": pat_n, "total": pat_t, "tn": pat_tn}}, reported


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

    def build(label, key, note, member_fn, min_rep, include_fin=False):
        qs = []
        last_qe = last_members = None; last_rep = 0   # newest quarter with any reporter (cand is chronological)
        for qe in cand:
            members = member_fn(qe)
            if not members:
                continue
            row, reported = agg_quarter(members, qe, pat, revop, include_fin=include_fin)   # robust MIN_N floor
            if reported > 0:
                last_qe, last_members, last_rep = qe, members, reported
            if reported >= min_rep:
                qs.append(row)
        # The CURRENT quarter shows from its very first filing and stays "in progress" until HALF
        # the universe has reported. Crossing min_rep alone isn't enough: early reporters are
        # bank-heavy, so an index can pass the gate (e.g. 6 of 500) while rev/EBITDA still have
        # fewer than MIN_N non-financial samples — the medians suppress to null and the chart
        # renders blank bars with no in-progress tag (live bug, Nifty 500 Q1FY27). While partial,
        # recompute over whatever sample exists (min_n=1) and flag it for the dimmed/amber UI.
        if last_qe is not None and last_rep < 0.5 * len(last_members):
            row, _ = agg_quarter(last_members, last_qe, pat, revop, min_n=1, include_fin=include_fin)
            row["partial"] = True
            qs = [r for r in qs if r["qe"] != last_qe] + [row]
        qs.sort(key=lambda r: r["qe"])
        return {"key": key, "label": label, "note": note, "quarters": qs, "fin": include_fin}

    # Financials are INCLUDED in every universe's rev/op totals (bank rev = interest earned, op =
    # pre-provision operating profit; NBFC rev = core revenue, op = EBITDA) — matches Trendlyne's
    # index result-analysis cards to the paisa (Nifty 500 Q1FY27: rev 13.8, oper profit 13.3).
    # Verified 2026-07-10. The op bar for a mixed universe therefore sums industrial EBITDA with
    # bank pre-provision profit — that aggregate == Trendlyne's "Oper Profit" card (their separate
    # "EBIDT" card is a proprietary before-depreciation calc we don't reproduce).
    universes = []
    # 1) the liquid universe (default)
    universes.append(build(
        "All liquid stocks (₹1cr+/day)", "liquid",
        "Currently-listed companies trading ≥ ₹1 cr/day (median, ~250 sessions) — %d names." % len(U),
        lambda qe: U, 200, include_fin=True))
    # 2) every index, point-in-time membership
    FIN_UNIVERSES = {"Nifty Bank", "Nifty PSU Bank"}
    for index, snaps in indices.items():
        if not isinstance(snaps, list) or not snaps:
            continue
        note = ("Point-in-time %s members at each quarter's rebalance (survivorship-free)."
                " Revenue = interest earned; the Operating Profit bar = pre-provision operating profit." % index
                if index in FIN_UNIVERSES else
                "Point-in-time %s members at each quarter's rebalance (survivorship-free)."
                " Banks/NBFCs are included: their revenue = interest earned, operating profit = pre-provision." % index)
        universes.append(build(
            index, index, note,
            (lambda snps: (lambda qe: snap_as_of(snps, iso(qe))))(snaps), 5, include_fin=True))

    universes = [u for u in universes if u["quarters"]]
    for u in universes:
        q = u["quarters"]
        print("  %-24s %2d quarters  latest %s reported=%d" % (
            u["label"], len(q), q[-1]["label"] if q else "-", q[-1]["reported"] if q else 0))

    payload = {
        "defaultUniverse": "liquid",
        "basis": "YoY vs the year-ago quarter (positive base). PAT = consolidated owners-attributable where "
                 "filed, else standalone. Total = consolidated-preferred aggregate (matches Trendlyne). Revenue "
                 "& Operating profit include banks/NBFCs (bank rev = interest earned, op = pre-provision profit). "
                 "Index universes use point-in-time membership (whoever was in the index at each quarter's rebalance).",
        "dataAsOf": end,
        "universes": universes,
    }
    json.dump(payload, open(OUT, "w"), separators=(",", ":"))
    print("Wrote %s (%d universes)" % (OUT, len(universes)))


if __name__ == "__main__":
    main()
