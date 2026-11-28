"""Results-season fill checks (runbook §251): were the newest two quarters FILLED, and filled CORRECTLY?

Every stock (not only Nifty 500), standalone and consolidated, the two newest calendar quarter-ends.

fill_missing   what should be in the store and is not:
    declared_no_numbers    the results feed shows the company filed this quarter's results more than GRACE_DAYS ago
                           and there is no profit anywhere: not in the main store (sf_fundamentals; bse_fundamentals
                           for a BSE-only name) and not in the PDF read;
    declared_pdf_only      same, but the PDF read (vision_fills.json) holds the figures — the site shows them from that
                           overlay while the main store, which the backtests and screens read, is still empty.
                           `filer` = half-yearly marks an SME that files halves (their main-store rows come from the
                           §213 half-year job, so they arrive as one batch);
    period_unread          a results filing whose stated period could not be read (feed qe = 0), older than GRACE_DAYS;
    prev_not_this          the store holds one of the stock's previous two periods but nothing for this one, the filing
                           deadline (+3 days) has passed, and the feed shows no filing either. A half-yearly filer (rows
                           only for March and September over the last two years) is expected only for those two;
    one_basis              the row holds one basis only while the stock's previous quarter held both;
    no_revenue / no_op     profit is stored but revenue is not / operating profit is not while the previous quarter had it.
fill_vision    what is in the store and may be wrong: stored profit, revenue and operating profit against the PDF read
               in vision_fills.json, same quarter, same basis. Differences that LOOK like a definition or a unit gap are
               listed apart (the "Likely definition / units" tab) — they are still unread candidates, never accepted by
               category (runbook §0 first rule):
    fin_definition   revenue / operating profit of a bank, NBFC or insurer (the two readers define them differently);
    unit             the two figures are a power of ten apart AND the stored one is in line with the stock's own
                     previous quarter (so the PDF read is the likelier unit slip). A power-of-ten gap where the STORED
                     figure is out of line with its previous quarter stays in the open list (unit_store).

Report-only. A listed cell is a CANDIDATE: read its own filing, then heal through the ledgers or accept it in
scripts/xcheck_accept.json with what was read."""
import datetime, json, math, os, sys
from . import common as C
from .fund import _fund, _near

GRACE_DAYS = 3            # a filing younger than this is still in the normal fill pipeline (XBRL lands within ~1 day)
PRI = {"declared_no_numbers": 9, "declared_pdf_only": 7, "period_unread": 8, "one_basis": 6, "no_revenue": 5, "no_op": 4, "prev_not_this": 3,
       "pat": 9, "unit_store": 8, "rev": 6, "op": 5, "fin_definition": 2, "unit": 1}


def today_ist():
    """IST calendar day; FILL_TODAY=YYYY-MM-DD overrides (tests and replays)."""
    o = os.environ.get("FILL_TODAY")
    if o: return datetime.date.fromisoformat(o)
    return (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=5, minutes=30)).date()


def quarters(today=None):
    """The four newest calendar quarter-ends as ymd ints, newest first (the last two serve only as 'previous')."""
    d = today or today_ist(); out = []
    while len(out) < 4:
        for y, m, dd in ((d.year, 12, 31), (d.year, 9, 30), (d.year, 6, 30), (d.year, 3, 31), (d.year - 1, 12, 31)):
            q = datetime.date(y, m, dd)
            if q < d: break
        out.append(C.ymd(q)); d = q
    return out


def deadline(qe):
    """SEBI LODR reg. 33 filing deadline + 3 days (the same window season_state.py closes on)."""
    return C.day(qe) + datetime.timedelta(days=(60 if qe // 100 % 100 == 3 else 45) + 3)


def weight(rank, cls, size):
    """Newest quarter first, then class, then size."""
    return rank * 1e12 + PRI.get(cls, 0) * 1e10 + min(abs(size or 0.0), 9.9e9)


def index_tags(qe):
    """{sym: 'Nifty 50' | 'Nifty 500' | 'index member'} — point-in-time, the snapshot in force at the quarter-end."""
    ih = C.jload(os.path.join(C.HERE, "indices_history.json")); iso = C.iso(qe); tag = {}
    try: ren = C.jload(os.path.join(C.HERE, "_rename_map.json"))
    except Exception: ren = {}
    def members(name):
        best = None
        for s in ih.get(name) or []:
            if s.get("effectiveDate", "9") <= iso: best = s
        return {ren.get(x, x) for x in best["symbols"]} if best else set()
    for name, snaps in ih.items():
        if isinstance(snaps, list) and name not in ("Nifty 50", "Nifty 500"):
            for s in members(name): tag[s] = "index member"
    for s in members("Nifty 500"): tag[s] = "Nifty 500"
    for s in members("Nifty 50"): tag[s] = "Nifty 50"
    return tag


def half_yearly(qrows, qe):
    """True when the stock's stored rows over the two years to `qe` are only March / September periods (an SME that
    files half-yearly). One row or none = cadence unknown = treated as quarterly."""
    ms = [q // 100 % 100 for q, r in qrows.items() if qe - 20000 <= q <= qe and _has(r)]
    return len(ms) >= 2 and all(m in (3, 9) for m in ms)


def _has(row):
    return row is not None and (row[1] is not None or (len(row) > 3 and row[3] is not None))


class FillMissing(C.Check):
    id = "fill_missing"
    title = "Newest two quarters, every stock: results filed or due that are not in the store"
    max_listed = 3000
    domain = "Results season"
    reader_a = "the main store: sf_fundamentals + sf_revop (bse_fundamentals for BSE-only names)"
    reader_b = "the exchange results feed (results_feed.json) and the stock's own previous quarter"
    scope = "every stock, the two newest calendar quarter-ends, standalone and consolidated"
    tolerance = "a filing counts once it is %d days old; 'previous quarter held, this one empty' counts after the filing deadline + 3 days" % GRACE_DAYS

    def run(self):
        sys.path.insert(0, C.HERE)
        import results_pending as RP
        today = today_ist(); qs = quarters(today); cut = (today - datetime.timedelta(days=GRACE_DAYS)).isoformat()
        fund = _fund(); revop = C.jload(os.path.join(C.DOCS, "sf_revop.json"))
        vis = C.jload(os.path.join(C.DOCS, "vision_fills.json"))
        bpx = (C.jload(os.path.join(C.DOCS, "bse_fundamentals.json")) or {}).get("px", {})
        try: ren = C.jload(os.path.join(C.HERE, "_rename_map.json"))
        except Exception: ren = {}
        n = {}
        self.note("quarters checked: %s · today %s" % (", ".join(C.iso(q) for q in qs[:2]), today.isoformat()))

        def frow(sym, qe):
            for s in (sym, ren.get(sym)):
                r = (fund.get(s) or {}).get(qe) if s else None
                if r is not None: return r
            return None

        for rank, qe in ((2, qs[0]), (1, qs[1])):
            i = qs.index(qe); tags = index_tags(qe); listed = set()

            def prev_of(qrows):
                """(qe, row) of the stock's previous period held in the store, or (None, None)."""
                hy = half_yearly(qrows, qe)
                if hy and qe // 100 % 100 not in (3, 9): return None, None, hy
                for q in ([qs[i + 2]] if hy else qs[i + 1:i + 3]):
                    if _has(qrows.get(q)): return q, qrows[q], hy
                return None, None, hy

            def flag(sym, cls, size=0.0, **kw):
                n[cls] = n.get(cls, 0) + 1; listed.add(sym)
                self.add("%s|%d|%s%s" % (sym, qe, cls, "|" + kw["basis"] if kw.get("basis") else ""), sym=sym, qe=C.iso(qe), field=cls, index=tags.get(sym),
                         weight=weight(rank, cls, (size or 0.0) + (5e9 if tags.get(sym) else 0.0)), **kw)

            # 1. declared in the feed, nothing in the store
            _, rows = RP.classify(qe, unknown=(rank == 2))
            for e in rows:
                if e["ann"] > cut: continue
                sym = e["sym"]
                if e["status"] == "unknown_qe":
                    self.compared += 1
                    flag(sym, "period_unread", e.get("mcap"), name=e["name"], exch=e["exch"], declared=e["ann"], pdf=e.get("pdf") or None)
                    continue
                self.compared += 1
                if e["exch"] == "BSE": ok = e["status"] == "filled"
                else: ok = _has(frow(sym, qe))
                if ok: self.agree += 1; continue
                vc = (vis.get(sym) or {}).get(str(qe)) or {}
                hy = bool(vc.get("hy")) or half_yearly(fund.get(sym) or {}, qe)
                if e["exch"] != "BSE" and vc.get("pat") is not None:
                    flag(sym, "declared_pdf_only", (e.get("mcap") or 0.0) / (1e4 if hy else 1.0), name=e["name"], exch=e["exch"],
                         declared=e["ann"], pdf=e.get("pdf") or None, filer="half-yearly" if hy else "quarterly",
                         pdf_read="profit %s (%s)" % (vc.get("pat"), vc.get("basis")))
                else:
                    flag(sym, "declared_no_numbers", e.get("mcap"), name=e["name"], exch=e["exch"], declared=e["ann"],
                         pdf=e.get("pdf") or None, filer="half-yearly" if hy else "quarterly", status=e["status"])
            declared = {e["sym"] for e in rows}

            # 2. the store's own rows for this quarter: one basis only, profit without revenue / operating profit
            for sym, qrows in fund.items():
                r = qrows.get(qe); pq, p, hy = prev_of(qrows)
                if not _has(r):
                    if p is not None and sym not in declared and sym not in listed and today > deadline(qe):
                        if _has((fund.get(ren.get(sym) or "") or {}).get(qe)): continue      # an old ticker: the renamed key holds it
                        self.compared += 1
                        later = any(q > qe and _has(x) for q, x in qrows.items())
                        flag(sym, "prev_not_this", abs(p[3] if p[3] is not None else p[1]) + (1e9 if later else 0.0), exch="NSE",
                             prev=(p[3] if p[3] is not None else p[1]), prev_qe=C.iso(pq), filer="half-yearly" if hy else "quarterly",
                             after="a later period is held (a hole)" if later else "nothing later is held")
                    continue
                if sym not in declared: self.compared += 1
                clean = sym not in listed
                rr = (revop.get(sym) or {}).get(str(qe)); pr = (revop.get(sym) or {}).get(str(pq)) if pq else None
                for bk, pi, ri in (("s", 1, 0), ("c", 3, 1)):
                    if r[pi] is None:
                        if p is not None and p[pi] is not None and p[4 - pi] is not None and r[4 - pi] is not None:
                            clean = False; flag(sym, "one_basis", abs(p[pi]), basis=bk, exch="NSE", prev=p[pi], prev_qe=C.iso(pq),
                                                held=r[4 - pi])
                        continue
                    if not rr or rr[ri] is None:
                        clean = False; flag(sym, "no_revenue", abs(r[pi]), basis=bk, exch="NSE", profit=r[pi])
                    elif rr[2 + ri] is None and pr and pr[2 + ri] is not None:
                        clean = False; flag(sym, "no_op", abs(r[pi]), basis=bk, exch="NSE", profit=r[pi], prev=pr[2 + ri], prev_qe=C.iso(pq))
                if clean and sym not in declared: self.agree += 1
        hyn = sum(1 for f in self.findings if f["field"] == "declared_pdf_only" and f.get("filer") == "half-yearly")
        self.note("of declared_pdf_only, half-yearly (SME) filers: %d" % hyn)
        order = ["declared_no_numbers", "declared_pdf_only", "period_unread", "one_basis", "no_revenue", "no_op", "prev_not_this"]
        self.note(" · ".join("%s %d" % (k, n.get(k, 0)) for k in order))


class FillVision(C.Check):
    id = "fill_vision"
    title = "Newest two quarters, every stock: stored profit, revenue and operating profit vs the PDF read"
    max_listed = 3000
    explained_label = "Likely definition / units (unread)"
    domain = "Results season"
    reader_a = "sf_fundamentals (profit) / sf_revop (revenue, operating profit)"
    reader_b = "vision_fills.json — a model's read of the filing PDF, same quarter, same basis"
    scope = "every stock with a PDF read, the two newest calendar quarter-ends"
    tolerance = "within 1 % or Rs 0.05 cr"

    def run(self):
        fund = _fund(); revop = C.jload(os.path.join(C.DOCS, "sf_revop.json"))
        vis = C.jload(os.path.join(C.DOCS, "vision_fills.json"))
        co = (C.jload(os.path.join(C.DOCS, "quarterly_results.json")) or {}).get("co", {})
        qs = quarters(); n = {}; per = {}
        self.note("quarters checked: " + ", ".join(C.iso(q) for q in qs[:2]))
        xcache = {}

        def xof(sym):
            if sym not in xcache:
                try: xcache[sym] = (json.load(open(os.path.join(C.DOCS, "fin", sym + ".json"), encoding="utf-8")).get("x") or {})
                except (OSError, ValueError): xcache[sym] = {}
            return xcache[sym]

        for rank, qe in ((2, qs[0]), (1, qs[1])):
            i = qs.index(qe)
            for sym, cells in vis.items():
                v = cells.get(str(qe))
                if not v: continue
                bk = (v.get("basis") or "").lower()
                if bk not in ("s", "c"): continue
                pi, ri = (1, 0) if bk == "s" else (3, 1)
                fr = (fund.get(sym) or {}).get(qe); rr = (revop.get(sym) or {}).get(str(qe))
                pq = next((q for q in qs[i + 1:i + 3] if _has((fund.get(sym) or {}).get(q))), qs[i + 1])
                fp = (fund.get(sym) or {}).get(pq); rp = (revop.get(sym) or {}).get(str(pq))
                c = co.get(sym) or {}; fin = bool(c.get("f")) or c.get("s") == "Financial Services"
                trio = (("pat", fr[pi] if fr else None, v.get("pat"), fp[pi] if fp else None),
                        ("rev", rr[ri] if rr else None, v.get("rev"), rp[ri] if rp else None),
                        ("op", rr[2 + ri] if rr else None, v.get("op"), rp[2 + ri] if rp else None))
                for fld, a, b, prev in trio:
                    if a is None or b is None: continue
                    self.compared += 1; k = (qe, fld); per.setdefault(k, [0, 0]); per[k][0] += 1
                    if _near(a, b, 0.01, 0.05): self.agree += 1; per[k][1] += 1; continue
                    ratio = a / b if b else None; cls = fld; why = None; p10 = None
                    if ratio and ratio > 0:
                        lg = math.log10(ratio)
                        if round(lg) != 0 and abs(lg - round(lg)) < 0.0065: p10 = int(round(lg))       # within 1.5 % of 10^k
                    if p10 is not None:
                        inline = prev not in (None, 0) and a / prev > 0 and 0.2 <= a / prev <= 5
                        if inline:
                            cls = "unit"; why = "stored ÷ PDF read = 10^%d; the stored figure is in line with the previous quarter (%s) — unread" % (p10, prev)
                        else: cls = "unit_store"
                    elif fin and fld in ("rev", "op"):
                        cls = "fin_definition"; why = "bank / NBFC / insurer: the two readers define revenue and operating profit differently — unread"
                    extra = {}
                    if fld == "pat":                    # which reading does the filing's own PBT - tax support?
                        dd = (xof(sym).get(str(qe)) or {}).get(bk) or {}
                        if dd.get("pbt") is not None and dd.get("tax") is not None:
                            base = dd["pbt"] - dd["tax"]; asc = dd.get("assoc") or 0.0; nci = dd.get("nci") or 0.0
                            cands = [base] + ([base + asc - nci, base - nci, base + asc] if bk == "c" else [])
                            sa = any(_near(a, x, 0.02, 0.1) for x in cands); sb = any(_near(b, x, 0.02, 0.1) for x in cands)
                            extra = {"pbt_less_tax": round(min(cands, key=lambda x: abs(x - b)), 2),
                                     "supports": "both" if sa and sb else "store" if sa else "PDF read" if sb else "neither"}
                    n[cls] = n.get(cls, 0) + 1
                    self.add("%s|%d|%s|%s" % (sym, qe, bk, fld), sym=sym, qe=C.iso(qe), basis=bk, field=fld, cls=cls, a=a, b=b,
                             ratio=round(ratio, 4) if ratio else None, prev=prev, half_year=v.get("hy") or None,
                             explained=why, weight=weight(rank, cls, abs(a - b)), **extra)
        order = ["pat", "unit_store", "rev", "op", "fin_definition", "unit"]
        self.note(" · ".join("%s %d" % (k, n.get(k, 0)) for k in order))
        self.note(" · ".join("%s %s: %d agree / %d differ" % (C.iso(q), f, per[(q, f)][1], per[(q, f)][0] - per[(q, f)][1])
                             for q in qs[:2] for f in ("pat", "rev", "op") if (q, f) in per))
