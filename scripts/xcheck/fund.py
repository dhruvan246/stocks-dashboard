"""Quarterly-results checks. Values in Rs crore; basis s = standalone, c = consolidated (owners' share for PAT).

fund_vision_xbrl  Reader A: the XBRL store (sf_fundamentals PAT authority, sf_revop revenue).
                  Reader B: vision_fills.json — a model's read of the filing PDF (a different document of the same
                  filing, read by a different method). Same quarter, same basis.
fund_pbt_tax      Reader A: stored PAT (sf_fundamentals). Reader B: the filing's own PBT and tax lines (fin/<SYM>.json
                  `x`, NSE XBRL detail) — PAT = PBT − tax (consolidated: + associates − minority, whichever
                  presentation the filer used). Catches owners/total mix-ups, wrong-quarter and scale slips.
fund_eps_shares   Reader A: stored PAT. Reader B: the company's own basic EPS × shares outstanding at quarter-end
                  (scripts/shares_history.json, the SHP filing's share count). EPS is computed by the company from
                  its weighted share count, so 5 % slack; a 10x/100x gap is a scale slip on one side."""
import datetime, json, os
from . import common as C

BASES = (("s", 1, 0), ("c", 3, 1))          # (x key, sf_fundamentals PAT index, sf_revop revenue index)


def _near(a, b, rel, absl):
    return abs(a - b) <= max(absl, rel * max(abs(a), abs(b)))


def _in_scope(sym, qe, spans_cache):
    sp = spans_cache.get(sym)
    if sp is None: sp = spans_cache[sym] = C.member_spans(sym)
    return any(f <= qe and (t is None or qe < t) for f, t in sp)


def _fund():
    raw = C.jload(os.path.join(C.DOCS, "sf_fundamentals.json"))
    return {s: {r[0]: r for r in rows} for s, rows in raw.items() if isinstance(rows, list)}


class FundVisionXbrl(C.Check):
    id = "fund_vision_xbrl"
    title = "Quarterly revenue & profit: XBRL store vs PDF (vision) read"
    domain = "Quarterly results"
    reader_a = "sf_fundamentals (PAT) / sf_revop (revenue) — NSE XBRL"
    reader_b = "vision_fills.json — model read of the filing PDF"
    scope = "Nifty 500 members at quarter-end; quarters where both readers hold the same basis"
    tolerance = "within 1 % or Rs 0.5 cr"

    def run(self):
        fund = _fund(); rev = C.jload(os.path.join(C.DOCS, "sf_revop.json"))
        vis = C.jload(os.path.join(C.DOCS, "vision_fills.json")); spans = {}
        for sym, qs in vis.items():
            for q, v in qs.items():
                qe = int(q)
                if not _in_scope(sym, qe, spans): continue
                b = (v.get("basis") or "").lower()
                ix = {"s": (1, 0), "c": (3, 1)}.get(b)
                if not ix: continue
                pairs = []
                fr = (fund.get(sym) or {}).get(qe)
                if fr is not None and fr[ix[0]] is not None and v.get("pat") is not None: pairs.append(("pat", fr[ix[0]], v["pat"]))
                rr = (rev.get(sym) or {}).get(q)
                if rr and rr[ix[1]] is not None and v.get("rev") is not None: pairs.append(("rev", rr[ix[1]], v["rev"]))
                for fld, a, bv in pairs:
                    self.compared += 1
                    if _near(a, bv, 0.01, 0.5): self.agree += 1; continue
                    ratio = a / bv if bv else None
                    self.add("%s|%d|%s|%s" % (sym, qe, b, fld), sym=sym, qe=C.iso(qe), basis=b, field=fld, a=a, b=bv,
                             ratio=round(ratio, 4) if ratio else None, weight=round(abs(a - bv), 2))


class FundPbtTax(C.Check):
    id = "fund_pbt_tax"
    title = "Stored profit vs the filing's own PBT − tax"
    domain = "Quarterly results"
    reader_a = "sf_fundamentals PAT (owners' share for consolidated)"
    reader_b = "fin/<SYM>.json x: PBT, tax, associates, minority (NSE XBRL detail lines)"
    scope = "Nifty 500 members at quarter-end, quarters with PBT and tax filed"
    tolerance = "any presentation within 2 % or Rs 1 cr"

    def run(self):
        fund = _fund(); ever = C.ever_members(); spans = {}
        for sym in sorted(ever):
            p = os.path.join(C.DOCS, "fin", sym + ".json")
            if not os.path.exists(p): continue
            try: x = (json.load(open(p, encoding="utf-8")).get("x") or {})
            except ValueError: continue
            for q, bases in x.items():
                qe = int(q)
                if not _in_scope(sym, qe, spans): continue
                fr = (fund.get(sym) or {}).get(qe)
                if fr is None: continue
                for bk, pi, _ in BASES:
                    d = bases.get(bk) or {}
                    pat = fr[pi]
                    if pat is None or d.get("pbt") is None or d.get("tax") is None: continue
                    pbt, tax = d["pbt"], d["tax"]; asc = d.get("assoc") or 0.0; nci = d.get("nci") or 0.0
                    cands = {"pbt-tax": pbt - tax}
                    if bk == "c":
                        cands.update({"pbt-tax+assoc-nci": pbt - tax + asc - nci, "pbt-tax-nci": pbt - tax - nci,
                                      "pbt-tax+assoc": pbt - tax + asc})
                    self.compared += 1
                    if any(_near(pat, v, 0.02, 1.0) for v in cands.values()): self.agree += 1; continue
                    best = min(cands.items(), key=lambda kv: abs(kv[1] - pat))
                    self.add("%s|%d|%s" % (sym, qe, bk), sym=sym, qe=C.iso(qe), basis=bk, pat=pat, pbt=pbt, tax=tax,
                             assoc=asc or None, nci=nci or None, closest=[best[0], round(best[1], 2)],
                             ratio=round(pat / best[1], 4) if best[1] else None, weight=round(abs(pat - best[1]), 2))


class FundEpsShares(C.Check):
    id = "fund_eps_shares"
    title = "Stored profit vs the company's EPS × shares outstanding"
    domain = "Quarterly results"
    reader_a = "sf_fundamentals PAT (owners' share for consolidated)"
    reader_b = "fin x eps_b (company-computed basic EPS) × shares_history (SHP share count at quarter-end)"
    scope = "Nifty 500 members at quarter-end, quarters with basic EPS and a share count"
    tolerance = "within 5 % or Rs 1 cr (weighted vs period-end shares)"

    def run(self):
        fund = _fund(); ever = C.ever_members(); spans = {}
        sh = C.jload(os.path.join(C.HERE, "shares_history.json"))
        for sym in sorted(ever):
            p = os.path.join(C.DOCS, "fin", sym + ".json"); shs = sh.get(sym) or {}
            if not os.path.exists(p) or not shs: continue
            try: x = (json.load(open(p, encoding="utf-8")).get("x") or {})
            except ValueError: continue
            for q, bases in x.items():
                qe = int(q)
                if not _in_scope(sym, qe, spans): continue
                fr = (fund.get(sym) or {}).get(qe); cnt = (shs.get(C.iso(qe)) or [None])[0]
                if fr is None or not cnt: continue
                for bk, pi, _ in BASES:
                    d = bases.get(bk) or {}; pat = fr[pi]; eps = d.get("eps_b")
                    if pat is None or eps is None or eps == 0: continue
                    implied = eps * cnt / 1e7
                    self.compared += 1
                    if _near(pat, implied, 0.05, 1.0): self.agree += 1; continue
                    ratio = pat / implied if implied else None
                    self.add("%s|%d|%s" % (sym, qe, bk), sym=sym, qe=C.iso(qe), basis=bk, pat=pat, eps=eps, shares=cnt,
                             implied=round(implied, 2), ratio=round(ratio, 4) if ratio else None,
                             weight=round(abs(pat - implied), 2))


class FundCellShape(C.Check):
    """§230. Store-intrinsic screen: no second document, the store's own rows are the second reader. Each kind is a
    footprint a known writer bug left (runbook §230): an exact 0.00 (all-zero placeholder XBRL / 'not found' stored as 0),
    consolidated == standalone to the paisa while the company files a different consolidated figure in a neighbouring
    quarter (std copied into con), and the same profit two quarters running (a stale or wrong-quarter column).
    (An announcement date equal to the SEBI deadline is NOT screened here: many companies really file on that day —
    HINDALCO Jun-20 on 14 Aug 2020. The deadline stamp is screened locally against the XBRL's own timestamp,
    scripts/screen_profit_xbrl.py.)"""
    id = "fund_cell_shape"
    title = "Quarterly profit cells with a writer-bug footprint"
    max_listed = 2000
    domain = "Quarterly results"
    reader_a = "sf_fundamentals profit cell"
    reader_b = "the same stock's neighbouring quarters"
    scope = "Nifty 500 members at quarter-end"
    tolerance = "exact (to the paisa / day)"

    def run(self):
        raw = C.jload(os.path.join(C.DOCS, "sf_fundamentals.json")); spans = {}
        for sym, rows in raw.items():
            if not isinstance(rows, list): continue
            rows = sorted(rows, key=lambda r: r[0])
            for i, r in enumerate(rows):
                qe = r[0]
                if not _in_scope(sym, qe, spans): continue
                self.compared += 1; hit = False
                for b, vi, ai in (("s", 1, 2), ("c", 3, 4)):
                    v, ann = r[vi], r[ai]
                    if v is not None and v == 0:
                        hit = True; self.add("%s|%d|%s|zero" % (sym, qe, b), sym=sym, qe=C.iso(qe), basis=b, field="zero", a=v, b=None)
                if r[1] is not None and r[3] is not None and r[1] == r[3] and r[1] != 0:
                    near = [x for x in rows[max(0, i - 2):i + 3] if x is not r and x[1] is not None and x[3] is not None]
                    if any(x[1] != x[3] for x in near):
                        hit = True; self.add("%s|%d|c|con_eq_std" % (sym, qe), sym=sym, qe=C.iso(qe), basis="c", field="con_eq_std", a=r[3], b=r[1])
                if i:
                    p = rows[i - 1]
                    for b, vi in (("s", 1), ("c", 3)):
                        if r[vi] is not None and p[vi] is not None and r[vi] == p[vi] and abs(r[vi]) >= 1:
                            hit = True; self.add("%s|%d|%s|dup" % (sym, qe, b), sym=sym, qe=C.iso(qe), basis=b, field="same_as_prev_quarter", a=r[vi], b=p[vi])
                if not hit: self.agree += 1


class FundPatMirror(C.Check):
    """§239b. Two stores hold each quarter's net profit: sf_fundamentals (the authority) and the mirror in sf_revop
    slots 4/5, which several writers filled from their own read of the filing. Where they differ, one of them misread
    the filing — on 2026-11-17 the main store was the wrong side in 163 of the first 262 cells judged. Listed:
      store_diff      the two stores differ right now (a writer landed a different figure since the last sync);
      second_reading  sync_pat_mirror.py has since copied the main figure into the mirror, and kept the mirror's own
                      figure in scripts/pat_mirror_disagree.json — the disagreement is still unjudged.
    A listed cell is a CANDIDATE: read its own filing, then heal sf_fundamentals through fund_cell_fix.json (the cell
    closes itself) or accept it here when the main store is the one the filing supports. Newest quarters first."""
    id = "fund_pat_mirror"
    title = "Net profit: main store vs the second reading kept in the results mirror"
    max_listed = 1000
    domain = "Quarterly results"
    reader_a = "sf_fundamentals profit cell"
    reader_b = "sf_revop profit mirror (slots 4/5), or its figure kept by sync_pat_mirror.py before the copy"
    scope = "every stock, every quarter, standalone and consolidated, where both stores hold a profit figure"
    tolerance = "Rs 0.011 cr"

    def run(self):
        fund = _fund(); revop = C.jload(os.path.join(C.DOCS, "sf_revop.json"))
        sp = os.path.join(C.HERE, "pat_mirror_disagree.json")
        side = (C.jload(sp).get("cells") or {}) if os.path.exists(sp) else {}
        seen = set(); n = {"store_diff": 0, "second_reading": 0}

        def flag(sym, qe, bk, f, m, kind, since=None):
            n[kind] += 1
            self.add("%s|%d|%s" % (sym, qe, bk), sym=sym, qe=C.iso(qe), basis=bk, field=kind, main=f, mirror=m,
                     diff=round(f - m, 2), since=since, weight=qe + min(abs(f - m), 9999.0) / 10000.0)

        for sym, rows in fund.items():
            rv = revop.get(sym) or {}
            for qe, fr in rows.items():
                rr = rv.get(str(qe))
                if not rr: continue
                for bk, pi, ri in BASES:
                    f = fr[pi] if len(fr) > pi else None
                    m = rr[4 + ri] if len(rr) > 4 + ri else None
                    if f is None or m is None: continue
                    self.compared += 1
                    k = "%s|%d|%s" % (sym, qe, bk)
                    if abs(f - m) > 0.011: seen.add(k); flag(sym, qe, bk, f, m, "store_diff")
                    elif k in side and abs(f - side[k]["reader"]) > 0.011:
                        seen.add(k); flag(sym, qe, bk, f, side[k]["reader"], "second_reading", side[k].get("first_seen"))
                    else: self.agree += 1
        self.note("stores differ now: %d · second reading kept, unjudged: %d" % (n["store_diff"], n["second_reading"]))


class FundNewResults(C.Check):
    """§239. The newest two quarters of EVERY stock (not only Nifty 500), store-intrinsic, kept SHORT so it is read to
    zero after each results season. fund_pbt_tax / fund_eps_shares list thousands of cells across all history ranked
    by size, so a fresh wrong quarter never surfaced there (CPPLUS Sep-26 con 327.71 = the half-year figure, real
    185.51 — the filer's XBRL carried the half year under the quarter's owners tag; found by the user, not by us).
    A cell is listed only on a footprint that normal accounting does not produce:
      ytd_copy     stored profit = this quarter's own PBT - tax PLUS the stock's previous 1-3 stored quarters
                   (the year-to-date column stored as the quarter);
      pbt_tax+eps  stored profit matches no PBT - tax presentation AND, divided by the filing's own basic EPS,
                   implies a share count >25 % away from the same stock's median over its previous quarters
                   (minority interest, regulatory deferrals and discontinued operations move the first test but
                   not the second, so the pair is what separates a wrong cell from a presentation difference).
    A listed cell is a CANDIDATE: read its filing, then heal through fund_cell_fix.json or accept it."""
    id = "fund_new_results"
    title = "Newest two quarters, every stock: profit cells that fail the filing's own arithmetic"
    max_listed = 1000
    domain = "Quarterly results"
    reader_a = "sf_fundamentals profit cell (newest two quarter-ends)"
    reader_b = "the same filing's PBT, tax and basic EPS (fin x) + the stock's own earlier quarters"
    scope = "every stock with a results-detail row, standalone and consolidated, the two newest quarter-ends in the store"
    tolerance = "PBT - tax within 2 % or Rs 0.1 cr; EPS-implied shares within -20 % / +25 % of the stock's median"

    def run(self):
        import glob, statistics
        fund = _fund()
        qs = sorted({q for d in fund.values() for q in d})[-2:]
        self.note("quarters checked: " + ", ".join(C.iso(q) for q in qs))
        for p in sorted(glob.glob(os.path.join(C.DOCS, "fin", "*.json"))):
            try: d = json.load(open(p, encoding="utf-8"))
            except ValueError: continue
            sym, x = d.get("sym"), d.get("x") or {}
            rows = fund.get(sym) or {}
            for qe in qs:
                fr = rows.get(qe)
                if not fr: continue
                prevq = sorted(q for q in rows if q < qe)[-3:][::-1]            # newest first
                for bk, pi, _ in BASES:
                    pat = fr[pi] if len(fr) > pi else None
                    dd = (x.get(str(qe)) or {}).get(bk)
                    if pat is None or not dd or dd.get("pbt") is None or dd.get("tax") is None: continue
                    pbt, tax = dd["pbt"], dd["tax"]; asc = dd.get("assoc") or 0.0; nci = dd.get("nci") or 0.0
                    cands = [pbt - tax]
                    if bk == "c": cands += [pbt - tax + asc - nci, pbt - tax - nci, pbt - tax + asc]
                    self.compared += 1
                    best = min(cands, key=lambda v: abs(v - pat))
                    if any(_near(pat, v, 0.02, 0.1) for v in cands): self.agree += 1; continue
                    kind = None; run_ = 0.0
                    for q2 in prevq:                                            # stored = own quarter + earlier ones
                        v2 = rows[q2][pi] if len(rows[q2]) > pi else None
                        if v2 is None: break
                        run_ += v2
                        if any(_near(pat, v + run_, 0.01, 0.05) for v in cands): kind = "ytd_copy"; break
                    eps = dd.get("eps_b"); eps_pat = None
                    if eps and abs(eps) >= 0.05 and abs(pat) >= 0.2:
                        hist = []
                        for q2, r2 in rows.items():
                            if not (qe - 20000 <= q2 < qe): continue
                            e2 = ((x.get(str(q2)) or {}).get(bk) or {}).get("eps_b")
                            p2 = r2[pi] if len(r2) > pi else None
                            if e2 and abs(e2) >= 0.05 and p2 and abs(p2) >= 0.2 and p2 / e2 > 0: hist.append(p2 / e2)
                        if len(hist) >= 3:
                            m = statistics.median(hist); eps_pat = round(eps * m, 2)
                            if not kind and not (0.8 <= (pat / eps) / m <= 1.25): kind = "pbt_tax+eps"
                    if not kind: self.agree += 1; continue
                    self.add("%s|%d|%s" % (sym, qe, bk), sym=sym, qe=C.iso(qe), basis=bk, field=kind, pat=pat,
                             pbt=pbt, tax=tax, pbt_less_tax=round(best, 2), eps=eps, eps_implies=eps_pat,
                             weight=round(abs(pat - best), 2))
