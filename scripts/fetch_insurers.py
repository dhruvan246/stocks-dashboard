# -*- coding: utf-8 -*-
"""AUTOMATIC insurer quarterly net-profit fill (replaces the manual INSURER_EXTRACTION_PLAYBOOK grind).

Insurers (LICI/SBILIFE/HDFCLIFE/ICICIPRULI/ICICIGI/GICRE/NIACL/STARHEALTH/GODIGIT/NIVABUPA/MFSL) file
IRDAI-format results, so `update_fundamentals.py` (standard XBRL P&L) gets NOTHING for them. This script
closes that gap unattended:

  1. DISCOVER  — for each insurer, list its recent BSE result filings; any quarter-end that we don't yet
                 have a consolidated value for is a target (so a newly-filed quarter is picked up next run).
  2. FETCH     — download the filing attachment (BSE announcement path — the genuine company PDF, NOT the
                 entity-poisoned FinancialResult API), identity-guarded by company-name tokens.
  3. READ      — render the Shareholders' P&L page(s) and read Profit-after-tax via the vision API
                 (insurer_vision.py), for BOTH the current and the year-ago quarter, con + std.
  4. VERIFY    — ANCHOR: the read year-ago value must match our stored same-quarter-last-year con
                 (playbook's primary check) AND the current value must fall in the insurer's plausible
                 range. Never store an unanchored guess (brand-new IPO with no year-ago -> range-only,
                 flagged). Owner-attributable con; std==con for no-subsidiary insurers.
  5. APPLY     — fill-only into docs/sf_fundamentals.json AND scripts/fundamentals.json (con=idx3,
                 con-date=idx4; std=idx1 for no-sub), then the caller commits/pushes.

Needs ANTHROPIC_API_KEY (repo secret) + pymupdf. No key/deps -> no-op (insurers stay gapped, as before).

Run:
  python -X utf8 scripts/fetch_insurers.py                 # fill real gaps (daily cron)
  python -X utf8 scripts/fetch_insurers.py --months 6      # widen the discovery window
  python -X utf8 scripts/fetch_insurers.py --only HDFCLIFE,GICRE
  python -X utf8 scripts/fetch_insurers.py --verify 20260331   # re-read a KNOWN quarter, print vs stored, DO NOT write
"""
import os, sys, re, json, time, argparse
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bse_vision as V
import insurer_vision as IV
import fitz

HERE = os.path.dirname(os.path.abspath(__file__))
DOCS_FUND = os.path.join(HERE, "..", "docs", "sf_fundamentals.json")
SRC_FUND  = os.path.join(HERE, "fundamentals.json")
FLAG = os.path.join(HERE, "..", "docs", ".fund_updated")   # signals the workflow that data changed
LOG  = os.path.join(HERE, "_insurer_log.json")

MON = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

# Per-insurer config: BSE scripcode is resolved from bse_scrips.json; here we keep the identity tokens
# (guard that the fetched PDF is really this company) + plausible quarterly-PAT range in Rs cr (a coarse
# unit/row sanity gate — the year-ago anchor is the real check) + whether con differs from std.
INSURERS = {
    "LICI":       {"ident": ["LIFE INSURANCE CORPORATION"],            "range": (200, 25000),  "sub": True},
    "SBILIFE":    {"ident": ["SBI LIFE"],                              "range": (30, 2000),    "sub": False},
    "HDFCLIFE":   {"ident": ["HDFC LIFE"],                             "range": (80, 900),     "sub": True},
    "ICICIPRULI": {"ident": ["ICICI PRUDENTIAL"],                     "range": (80, 1200),    "sub": False},
    "ICICIGI":    {"ident": ["ICICI LOMBARD"],                        "range": (30, 1800),    "sub": False},
    "GICRE":      {"ident": ["GENERAL INSURANCE CORPORATION", "GIC"], "range": (10, 4500),    "sub": True},
    "NIACL":      {"ident": ["NEW INDIA ASSURANCE"],                  "range": (-200, 2500),  "sub": True},
    "STARHEALTH": {"ident": ["STAR HEALTH"],                          "range": (5, 900),      "sub": False},
    "GODIGIT":    {"ident": ["GO DIGIT", "DIGIT GENERAL"],           "range": (10, 400),     "sub": False},
    "NIVABUPA":   {"ident": ["NIVA BUPA"],                            "range": (-200, 500),   "sub": False},
    "MFSL":       {"ident": ["MAX FINANCIAL"],                        "range": (-150, 500),   "sub": True},
}

PL_HINT = re.compile(r"(shareholder|profit after tax|profit for the (period|quarter|year)"
                     r"|profit and loss account|profit & loss account)", re.I)

# Insurers file quarterly results under "Outcome of Board Meeting" as often as "Financial Result", so
# bse_vision.is_result (which only matches the literal "financial result") misses them. Accept either,
# vetoing the non-result board actions. False positives (a board outcome with no P&L) are harmless —
# render_pl_pngs rejects any attachment without a Shareholders' P&L page.
_RESULT_VETO = re.compile(r"(xbrl|investor|press release|presentation|earnings call|transcript"
                          r"|intimation|newspaper|analyst|audio|postal|agm|dividend|annual report"
                          r"|allotment|scrutiniz)", re.I)
_RESULT_HIT = re.compile(r"(financial result|outcome of board meeting|board meeting outcome"
                         r"|(?:un)?audited.*result)", re.I)


def is_result_filing(r):
    blob = ((r.get("SUBCATNAME", "") or "") + " " + (r.get("NEWSSUB", "") or "")).lower()
    if _RESULT_VETO.search(blob):
        return False
    return bool(_RESULT_HIT.search(blob))


def qe_label(qe):
    return "quarter ended %d %s %d" % (qe % 100, MON[(qe // 100) % 100], qe // 10000)


def qe_from_ann(a):
    """Announce-month -> the quarter-end just reported (result filings land 1-2 months after quarter end)."""
    y, m = a // 10000, (a // 100) % 100
    if 7 <= m <= 9:  return y * 10000 + 630
    if 10 <= m <= 12: return y * 10000 + 930
    if 1 <= m <= 3:  return (y - 1) * 10000 + 1231
    if 4 <= m <= 6:  return y * 10000 + 331
    return 0


def conval(fund, sym, qe):
    for r in fund.get(sym, []):
        if r[0] == qe:
            return r[3]
    return None


def datebound(o, code, lo, hi):
    """Result filings for a scrip in [lo,hi] (YYYYMMDD strings) -> [(annInt, attachment, headline), ...]."""
    out = []
    for pg in range(1, 4):
        u = ("https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w?pageno=%d&strCat=-1"
             "&strPrevDate=%s&strScrip=%s&strSearch=P&strToDate=%s&strType=C" % (pg, lo, code, hi))
        try:
            rows = json.loads(V.get(o, u)).get("Table", [])
        except Exception:
            break
        for r in rows:
            if is_result_filing(r) and r.get("ATTACHMENTNAME"):
                a = re.sub(r"[^0-9]", "", (r.get("NEWS_DT") or ""))[:8]
                out.append((int(a) if a else 0, r["ATTACHMENTNAME"], r.get("NEWSSUB", "") or ""))
        if len(rows) < 50:
            break
    return sorted(set(out), reverse=True)   # newest first


def fetch_pdf(o, att):
    for base in ("AttachHis", "AttachLive"):
        try:
            d = V.get(o, "https://www.bseindia.com/xml-data/corpfiling/%s/%s" % (base, att), b=True)
            if d[:4] == b"%PDF":
                return d
        except Exception:
            pass
    return None


# ---- NSE fallback (for insurers whose BSE board-outcome attachment is only a cover letter, e.g. LICI) ----
_NSE = {"s": None}
_NSE_GOOD = re.compile(r"financial result|integrated filing|outcome of board", re.I)
_NSE_BAD = re.compile(r"newspaper|analyst|investor (presentation|meet)|intimation|schedule|transcript"
                      r"|press release", re.I)


def _nse_session():
    if _NSE["s"] is None:
        from curl_cffi import requests as cr
        s = cr.Session(impersonate="chrome")
        try:
            s.get("https://www.nseindia.com/", timeout=45)
        except Exception:
            pass
        _NSE["s"] = s
    return _NSE["s"]


def _ddmmyyyy(qe, plus):
    import datetime
    y, m = qe // 10000, (qe // 100) % 100
    dt = datetime.date(y, m, 28) + datetime.timedelta(days=plus)
    return "%02d-%02d-%04d" % (dt.day, dt.month, dt.year)


def nse_result_pdfs(sym, qe):
    """Yield (annInt, pdfbytes) for NSE financial-result filings that map to quarter `qe`, best (most
    consolidated-P&L pages) first. Mirrors fetch_nse.py. Used only when BSE yields no P&L page."""
    try:
        s = _nse_session()
        ref = {"Referer": "https://www.nseindia.com/get-quotes/equity?symbol=%s" % sym}
        try: s.get(ref["Referer"], timeout=45)
        except Exception: pass
        url = ("https://www.nseindia.com/api/corporate-announcements?index=equities&symbol=%s"
               "&from_date=%s&to_date=%s" % (sym, _ddmmyyyy(qe, 5), _ddmmyyyy(qe, 170)))
        j = s.get(url, headers=ref, timeout=45).json()
    except Exception as ex:
        print("    nse list err:", str(ex)[:70]); return
    cands = []
    for rec in j or []:
        desc = str(rec.get("desc", "")); blob = desc + " " + str(rec.get("attchmntText", ""))
        if not _NSE_GOOD.search(blob) or _NSE_BAD.search(desc):
            continue
        f = rec.get("attchmntFile", "") or ""
        if not f.lower().endswith(".pdf"):
            continue
        m = re.match(r"(\d{2})-([A-Za-z]{3})-(\d{4})", str(rec.get("an_dt", "")))
        anni = 0
        if m:
            mo = {"jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,"jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12}[m.group(2).lower()]
            anni = int(m.group(3)) * 10000 + mo * 100 + int(m.group(1))
            if qe_from_ann(anni) != qe:
                continue
        try: sz = float(str(rec.get("attFileSize", "0")).split()[0])
        except Exception: sz = 0
        cands.append((sz, anni, f))
    cands.sort(reverse=True)
    for sz, anni, f in cands[:5]:
        try:
            r = _nse_session().get(f, headers={"Referer": "https://www.nseindia.com/"}, timeout=60)
            if r.content[:4] == b"%PDF":
                yield anni, r.content
        except Exception:
            pass


_PAT_TERM = re.compile(r"profit\s*/?\s*\(?\s*(loss\)?\s*)?(after tax|for the (period|quarter|year))"
                       r"|profit after tax", re.I)
_DEC = re.compile(r"\d[\d,]*\.\d\d")


def render_pl_pngs(pdf, ident_tokens):
    """Identity-check the PDF, then render its Shareholders' P&L page(s) to PNG for the vision read.
    Handles BOTH typeset filings (pick the P&L text pages) and SCANNED filings (LIC files image-only
    results — render the statement pages after the cover so vision can read them)."""
    try:
        doc = fitz.open(stream=pdf, filetype="pdf")
    except Exception:
        return None, "open-err"
    N = min(len(doc), 40)
    texts = [doc[p].get_text() for p in range(N)]
    full = "".join(texts).upper()
    has_text = bool(full.strip())
    # Identity guard only meaningful when there IS a text layer; scanned filings verified by the model.
    if has_text and ident_tokens and not any(tk.upper() in full for tk in ident_tokens):
        return None, "identity-mismatch"

    # Strong P&L text pages: a P&L hint + an actual PAT row or a dense numeric table (typeset filings).
    strong = [p for p in range(N) if PL_HINT.search(texts[p])
              and (_PAT_TERM.search(texts[p]) or len(_DEC.findall(texts[p])) >= 6)]
    if strong:
        pages = strong
    else:
        # Scanned (or number-less text) filing: the results tables are images with no text to locate them.
        # LIC's big annual filing buries the consolidated Shareholders' P&L deep (after the standalone
        # results + both auditor reports) and off-by-one page selection misses it, so hand the model ALL
        # the image statement pages (cap 16) — it reliably finds "Profit after tax" among them, and Haiku
        # vision over ~15 low-DPI pages costs a couple of cents. Only LIC files this way; quarterly filings
        # are small so this stays cheap.
        img_pages = [p for p in range(N) if doc[p].get_images() and len(texts[p].strip()) < 500]
        if img_pages:
            pages = img_pages[:16]
        elif not has_text:
            pages = list(range(min(N, 12)))
        else:
            pages = [p for p in range(N) if PL_HINT.search(texts[p])]   # weak fallback
    if not pages:
        return None, "no-pl-page"
    dpi = 150 if len(pages) > 8 else 185          # lower DPI when sending many pages (keeps tokens sane)
    pngs = [doc[p].get_pixmap(dpi=dpi).tobytes("png") for p in pages[:16]]
    return pngs, "ok"


def anchored(read_v, stored_v):
    """True if a vision-read value matches a stored value within max(3%, Rs 5cr)."""
    if read_v is None or stored_v is None:
        return False
    return abs(read_v - stored_v) <= max(abs(stored_v) * 0.03, 5.0)


def load_fund(path):
    return json.load(open(path, encoding="utf-8"))


def dump_fund(path, d):
    json.dump(d, open(path, "w", encoding="utf-8"), ensure_ascii=False, separators=(",", ":"))


def set_cell(fund, sym, qe, con, con_ann, std=None, std_ann=None):
    """Fill-only: create/extend the [qe, std, annStd, con, annCon] row without clobbering existing values."""
    rows = fund.setdefault(sym, [])
    row = next((r for r in rows if r[0] == qe), None)
    if row is None:
        row = [qe, None, None, None, None]
        rows.append(row)
        rows.sort(key=lambda r: r[0])
    changed = False
    if row[3] is None and con is not None:
        row[3] = round(con, 2); row[4] = con_ann; changed = True
    if std is not None and row[1] is None:
        row[1] = round(std, 2); row[2] = std_ann or con_ann; changed = True
    return changed


def process(sym, targets, o, docs, src, verify=False):
    """For one insurer, read+verify each target quarter. Returns list of result dicts (and applies unless verify)."""
    cfg = INSURERS[sym]
    code = scrips.get(sym)
    results = []
    if not code:
        return [{"sym": sym, "qe": q, "status": "no-scripcode"} for q in targets]
    lo = str(min(targets) // 10000 * 10000 + 401)          # from ~Apr of the earliest target's FY-ish window
    hi = str(max(targets) + 300)                            # a few months after the latest target's quarter-end
    try:
        filings = datebound(o, code, lo, hi)
    except Exception as ex:
        return [{"sym": sym, "qe": q, "status": "fetch-err:" + str(ex)[:40]} for q in targets]

    for qe in sorted(targets, reverse=True):
        # Candidate PDFs: BSE result/board-outcome attachments first, then NSE (some insurers' BSE
        # board-outcome attachment is only a cover letter — LICI — so the P&L lives on NSE).
        def candidate_pdfs():
            for annd, att, sub in [(a, att, sub) for (a, att, sub) in filings if qe_from_ann(a) == qe][:4]:
                pdf = fetch_pdf(o, att); time.sleep(1.2)
                if pdf:
                    yield annd, pdf
            for annd, pdf in nse_result_pdfs(sym, qe):
                yield annd, pdf

        picked = None; saw_pdf = False
        for annd, pdf in candidate_pdfs():
            saw_pdf = True
            pngs, why = render_pl_pngs(pdf, cfg["ident"])
            if not pngs:
                continue
            v = IV.read_insurer(sym, qe_label(qe), qe_label(qe - 10000), pngs, cfg["sub"])
            if v is None:
                results.append({"sym": sym, "qe": qe, "status": "no-vision(key/deps?)"}); picked = "done"; break
            if not (v.get("ok") and v.get("company_matches")):
                continue
            picked = (annd, v); break
        if picked in (None, "done"):
            if picked is None:
                results.append({"sym": sym, "qe": qe, "status": "no-filing" if not saw_pdf else "unreadable"})
            continue

        annd, v = picked
        cur_con = v["cur"]["con"]; cur_std = v["cur"]["std"]
        yago_con = v["yago"]["con"]
        stored_yago = conval(docs, sym, qe - 10000)
        lo_r, hi_r = cfg["range"]
        range_ok = cur_con is not None and lo_r <= cur_con <= hi_r
        anchor_ok = anchored(yago_con, stored_yago)
        # Accept: current in range AND (year-ago anchors, OR we have no year-ago to anchor against — new IPO).
        accept = range_ok and (anchor_ok or stored_yago is None)
        rec = {"sym": sym, "qe": qe, "ann": annd, "cur_con": cur_con, "cur_std": cur_std,
               "yago_con": yago_con, "stored_yago": stored_yago, "anchor_ok": anchor_ok,
               "range_ok": range_ok, "accept": bool(accept),
               "status": "OK" if accept else ("no-anchor" if not anchor_ok else "out-of-range")}
        # std: no-subsidiary insurers => std==con; with-sub => only if the read std also anchors on stored std.
        std_fill = None
        if accept:
            if not cfg["sub"]:
                std_fill = cur_con
            elif cur_std is not None and anchored(v["yago"]["std"], _stored_std(docs, sym, qe - 10000)):
                std_fill = cur_std
        rec["std_fill"] = std_fill
        results.append(rec)
        if accept and not verify:
            c1 = set_cell(docs, sym, qe, cur_con, annd, std_fill, annd)
            c2 = set_cell(src,  sym, qe, cur_con, annd, std_fill, annd)
            rec["written"] = bool(c1 or c2)
    return results


def _stored_std(fund, sym, qe):
    for r in fund.get(sym, []):
        if r[0] == qe:
            return r[1]
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", type=int, default=5, help="discovery window (months back)")
    ap.add_argument("--only", default="", help="comma list of symbols to restrict to")
    ap.add_argument("--verify", type=int, default=0, help="re-read this quarter-end (YYYYMMDD), compare, DO NOT write")
    args = ap.parse_args()

    global scrips
    scrips = json.load(open(os.path.join(HERE, "bse_scrips.json")))["by_id"]

    docs = load_fund(DOCS_FUND)
    src  = load_fund(SRC_FUND) if os.path.exists(SRC_FUND) else docs
    o = V.session(); time.sleep(0.5)

    only = set(x.strip().upper() for x in args.only.split(",") if x.strip())
    syms = [s for s in INSURERS if (not only or s in only)]

    # Build targets. --verify: the given quarter for every insurer (comparison run, no write).
    # Normal: quarters in the discovery window that have NO stored consolidated value yet.
    if args.verify:
        plan = {s: [args.verify] for s in syms}
    else:
        import datetime
        today = datetime.date.today()
        recent_qes = []
        for k in range(0, args.months + 3):                  # candidate quarter-ends over the window
            m = today.month - k
            y = today.year
            while m <= 0:
                m += 12; y -= 1
            qend = {1: (y - 1, 12, 31), 2: (y - 1, 12, 31), 3: (y - 1, 12, 31),
                    4: (y, 3, 31), 5: (y, 3, 31), 6: (y, 3, 31),
                    7: (y, 6, 30), 8: (y, 6, 30), 9: (y, 6, 30),
                    10: (y, 9, 30), 11: (y, 9, 30), 12: (y, 9, 30)}[m]
            recent_qes.append(qend[0] * 10000 + qend[1] * 100 + qend[2])
        recent_qes = sorted(set(recent_qes))
        plan = {}
        for s in syms:
            gaps = [q for q in recent_qes if conval(docs, s, q) is None]
            if gaps:
                plan[s] = gaps

    if not plan:
        print("No insurer gaps in the discovery window — nothing to do."); return

    all_res = []
    for s in sorted(plan):
        res = process(s, plan[s], o, docs, src, verify=bool(args.verify))
        all_res.extend(res)
        for r in res:
            if args.verify:
                d = (r.get("cur_con") - r["stored_yago"]) if False else None
                print("  %-11s %d  read_con=%-9s stored=%-9s  yago_read=%-9s yago_stored=%-9s  %s"
                      % (r["sym"], r["qe"], r.get("cur_con"), conval(docs, r["sym"], r["qe"]),
                         r.get("yago_con"), r.get("stored_yago"), r["status"]))
            else:
                print("  %-11s %d  con=%-9s std=%-9s anchor=%s range=%s -> %s%s"
                      % (r["sym"], r["qe"], r.get("cur_con"), r.get("std_fill"),
                         r.get("anchor_ok"), r.get("range_ok"), r["status"],
                         " [WRITTEN]" if r.get("written") else ""))

    json.dump({"ts": int(time.time()), "results": all_res}, open(LOG, "w"))

    if not args.verify:
        written = [r for r in all_res if r.get("written")]
        if written:
            dump_fund(DOCS_FUND, docs)
            if os.path.exists(SRC_FUND):
                dump_fund(SRC_FUND, src)
            open(FLAG, "w").write(str(int(time.time())))   # tell the workflow to rebuild + commit
            print("FILLED %d insurer quarter(s): %s"
                  % (len(written), ", ".join("%s %d" % (r["sym"], r["qe"]) for r in written)))
        else:
            print("No insurer values filled this run (no accepted reads).")


if __name__ == "__main__":
    main()
