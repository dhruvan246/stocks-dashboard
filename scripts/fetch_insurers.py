# -*- coding: utf-8 -*-
"""AUTOMATIC insurer quarterly net-profit fill — FREE (text-layer + double-anchor, NO paid API).

Insurers (LICI/SBILIFE/HDFCLIFE/ICICIPRULI/ICICIGI/GICRE/NIACL/STARHEALTH/GODIGIT/NIVABUPA/MFSL) file
IRDAI-format results, so `update_fundamentals.py` (standard XBRL P&L) gets NOTHING for them. This script
closes that gap unattended, using ONLY free tools (PyMuPDF text extraction + optional rapidocr for the
rare scanned filing) — the same text-anchor method the Nifty-500 backfill uses (congap_recover.py):

  1. DISCOVER  — for each insurer, list its recent BSE result filings; any quarter-end that we don't yet
                 have a consolidated value for is a target (so a newly-filed quarter is picked up next run).
  2. FETCH     — download the filing attachment (BSE announcement path — the genuine company PDF, NOT the
                 entity-poisoned FinancialResult API; NSE fallback for LIC's cover-only BSE attachment).
  3. READ      — text-extract the Shareholders' P&L rows (Profit-after-tax / "attributable to owners");
                 OCR (rapidocr, free) only the rare scanned page.
  4. VERIFY    — DOUBLE-ANCHOR (never guesses): accept the current-quarter value ONLY when, under one
                 consistent unit scale (÷1 / ÷100 / ÷10), the row's preceding-quarter column matches our
                 stored prior-quarter con AND its year-ago column matches our stored year-ago con (each
                 within max(3%, Rs 2cr)). Anchoring on the stored OWNERS-con series auto-selects the
                 owner-attributable row for insurers with minority interest. No anchor -> SKIP (flagged).
  5. APPLY     — fill-only into docs/sf_fundamentals.json AND scripts/fundamentals.json (con=idx3,
                 con-date=idx4; std=idx1 for no-sub / when it also anchors), then the caller commits/pushes.

Deps: pymupdf (required) + curl_cffi (NSE fallback) + rapidocr-onnxruntime/onnxruntime/numpy (scanned
filings only; degrades gracefully to text-only if absent). NO ANTHROPIC_API_KEY, no per-read cost.

Run:
  python -X utf8 scripts/fetch_insurers.py                 # fill real gaps (daily cron)
  python -X utf8 scripts/fetch_insurers.py --months 6      # widen the discovery window
  python -X utf8 scripts/fetch_insurers.py --only HDFCLIFE,GICRE
  python -X utf8 scripts/fetch_insurers.py --verify 20260331   # re-read a KNOWN quarter, print vs stored, NO write
"""
import os, sys, re, json, time, argparse, urllib.request, http.cookiejar, gzip
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fitz

_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
       "Chrome/120 Safari/537.36")


def bse_session():
    o = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
    try:
        o.open(urllib.request.Request("https://www.bseindia.com/", headers={"User-Agent": _UA}), timeout=30).read()
    except Exception:
        pass
    return o


def bse_get(o, u, b=False):
    r = o.open(urllib.request.Request(u, headers={"User-Agent": _UA, "Referer": "https://www.bseindia.com/"}), timeout=60)
    raw = r.read()
    if r.headers.get("Content-Encoding") == "gzip":
        raw = gzip.decompress(raw)
    return raw if b else raw.decode("utf-8", "replace")

HERE = os.path.dirname(os.path.abspath(__file__))
DOCS_FUND = os.path.join(HERE, "..", "docs", "sf_fundamentals.json")
SRC_FUND  = os.path.join(HERE, "fundamentals.json")
FLAG = os.path.join(HERE, "..", "docs", ".fund_updated")
LOG  = os.path.join(HERE, "_insurer_log.json")

# Per-insurer config: identity tokens (guard the fetched PDF is really this company) + plausible
# quarterly-PAT range in Rs cr (a coarse secondary gate — the double-anchor is the real check) +
# whether con differs from std (no-sub insurers store std==con).
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

# --- result-filing discovery (insurers file results as "Outcome of Board Meeting" as often as
# "Financial Result", so accept either; veto the non-result board actions) ---
_RESULT_VETO = re.compile(r"(xbrl|investor|press release|presentation|earnings call|transcript"
                          r"|intimation|newspaper|analyst|audio|postal|agm|dividend|annual report"
                          r"|allotment|scrutiniz)", re.I)
_RESULT_HIT = re.compile(r"(financial result|outcome of board meeting|board meeting outcome"
                         r"|(?:un)?audited.*result)", re.I)

# --- P&L row parsing ---
_NUM = re.compile(r"^\(?-?[\d,]+\.?\d*\)?$")
_OWN = re.compile(r"(owners|equity ?holders|equityholders|attributab)", re.I)
_PAT_ROW = re.compile(r"profit\s*/?\s*\(?\s*loss\)?\s*(after tax|for the (period|quarter|year))"
                      r"|profit after tax|profit for the (period|quarter|year)", re.I)
_BAD_ROW = re.compile(r"before tax|comprehensive|segment|exceptional|carried to|balance sheet|margin"
                      r"|operating|per share|earnings per|eps|ratio|nominal|paid.?up|dividend"
                      r"|non-controlling|minority|reserve", re.I)


def is_result_filing(r):
    blob = ((r.get("SUBCATNAME", "") or "") + " " + (r.get("NEWSSUB", "") or "")).lower()
    if _RESULT_VETO.search(blob):
        return False
    return bool(_RESULT_HIT.search(blob))


def qe_from_ann(a):
    y, m = a // 10000, (a // 100) % 100
    if 7 <= m <= 9:  return y * 10000 + 630
    if 10 <= m <= 12: return y * 10000 + 930
    if 1 <= m <= 3:  return (y - 1) * 10000 + 1231
    if 4 <= m <= 6:  return y * 10000 + 331
    return 0


def prevq(qe):
    y, md = qe // 10000, qe % 10000
    return {331: (y - 1) * 10000 + 1231, 630: y * 10000 + 331,
            930: y * 10000 + 630, 1231: y * 10000 + 930}[md]


def conval(fund, sym, qe):
    for r in fund.get(sym, []):
        if r[0] == qe:
            return r[3]
    return None


def stdval(fund, sym, qe):
    for r in fund.get(sym, []):
        if r[0] == qe:
            return r[1]
    return None


def _tv(w):
    w = w.replace(",", "").replace("(", "-").replace(")", "")
    try:
        return float(w)
    except Exception:
        return None


def datebound(o, code, lo, hi):
    out = []
    for pg in range(1, 4):
        u = ("https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w?pageno=%d&strCat=-1"
             "&strPrevDate=%s&strScrip=%s&strSearch=P&strToDate=%s&strType=C" % (pg, lo, code, hi))
        try:
            rows = json.loads(bse_get(o, u)).get("Table", [])
        except Exception:
            break
        for r in rows:
            if is_result_filing(r) and r.get("ATTACHMENTNAME"):
                a = re.sub(r"[^0-9]", "", (r.get("NEWS_DT") or ""))[:8]
                out.append((int(a) if a else 0, r["ATTACHMENTNAME"], r.get("NEWSSUB", "") or ""))
        if len(rows) < 50:
            break
    return sorted(set(out), reverse=True)


def fetch_pdf(o, att):
    for base in ("AttachHis", "AttachLive"):
        try:
            d = bse_get(o, "https://www.bseindia.com/xml-data/corpfiling/%s/%s" % (base, att), b=True)
            if d[:4] == b"%PDF":
                return d
        except Exception:
            pass
    return None


# ---- NSE fallback (LIC's BSE board-outcome attachment is only a cover letter) ----
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
    """Yield (annInt, pdfbytes) for NSE result filings mapping to quarter `qe`. Used when BSE has no P&L."""
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


# ---- free OCR fallback (scanned filings only; degrades to text-only if rapidocr absent) ----
_OCR = {"e": None, "off": False}


def _ocr_words(page):
    """Return page words as (x0,y0,x1,y1,text) in PDF-point space, via rapidocr. [] if OCR unavailable."""
    if _OCR["off"]:
        return []
    try:
        if _OCR["e"] is None:
            from rapidocr_onnxruntime import RapidOCR
            _OCR["e"] = RapidOCR()
    except Exception:
        _OCR["off"] = True
        return []
    dpi = 200; scale = 72.0 / dpi
    try:
        res, _ = _OCR["e"](page.get_pixmap(dpi=dpi).tobytes("png"))
    except Exception:
        return []
    words = []
    for box, text, score in (res or []):
        xs = [p[0] for p in box]; ys = [p[1] for p in box]
        words.append((min(xs) * scale, min(ys) * scale, max(xs) * scale, max(ys) * scale, text))
    return words


def _profit_rows(words):
    """Group words into lines; return [(isOwnersRow, [numbers left->right]), ...] for PAT-ish rows."""
    if not words:
        return []
    ws = sorted(words, key=lambda w: (round(w[1]), w[0]))
    lines = []; cur = []; cy = None
    for w in ws:
        if cy is None or abs(w[1] - cy) <= 4:
            cur.append(w)
        else:
            lines.append(cur); cur = [w]
        cy = w[1]
    if cur:
        lines.append(cur)
    out = []
    for ln in lines:
        cells = sorted(ln, key=lambda w: w[0])
        label = " ".join(w[4] for w in cells).lower()
        if _BAD_ROW.search(label):
            continue
        isown = bool(_OWN.search(label))
        ispft = bool(_PAT_ROW.search(label))
        if not (isown or ispft):
            continue
        nums = [_tv(w[4]) for w in cells if _NUM.match(w[4].replace(",", ""))]
        nums = [v for v in nums if v is not None]
        if len(nums) < 3:
            continue
        out.append((isown, nums))
    return out


def rows_from_pdf(pdf, ident_tokens, ocr=False):
    """Parse a filing -> [(isConsolidated, isOwnersRow, [nums]), ...]. None on identity mismatch.
    ocr=False (default, fast): text layer only — image pages are skipped (they're charts/annexures in a
    typeset filing). ocr=True: also OCR the image pages (free rapidocr) — used as a second pass only when
    the text layer yielded no anchor, i.e. a genuinely SCANNED filing (LIC's annual)."""
    try:
        doc = fitz.open(stream=pdf, filetype="pdf")
    except Exception:
        return None
    N = min(len(doc), 60)
    texts = [doc[p].get_text() for p in range(N)]
    if ident_tokens:
        full = " ".join(texts)
        if full.strip() and not any(t.upper() in full.upper() for t in ident_tokens):
            return None
    rows = []
    con = False           # insurer filings lead with standalone; anchoring corrects any mis-tag anyway
    ocr_used = 0
    for p in range(N):
        low = texts[p].lower()
        if "consolidated" in low:
            con = True
        elif re.search(r"standalone\s+(statement|financial|results|unaudited|audited|ind)", low) and "consolidated" not in low:
            con = False
        if texts[p].strip():
            words = doc[p].get_text("words")
        elif ocr and ocr_used < 20:
            words = _ocr_words(doc[p]); ocr_used += 1
        else:
            continue
        for isown, nums in _profit_rows(words):
            rows.append((con, isown, nums))
    return rows


def double_anchor(nums, cprev, cyago):
    """Find the [current, preceding-quarter, year-ago] column triple by SLIDING a window across the row:
    the standard insurer layout is [cur-Q, prev-Q, yago-Q, cur-YTD, prev-YTD], but rows often carry a
    leading serial/note number (e.g. '29 Profit after tax 80,464 57,674 81,351') that shifts fixed
    positions. So we scan for any adjacent pair matching (prev, yago) under one unit scale and take the
    value immediately to its left as the current quarter. Requires BOTH neighbours to match — a
    mis-grouped/garbled row essentially cannot pass. Returns None if nothing anchors."""
    if cprev is None or cyago is None or abs(cprev) < 3 or abs(cyago) < 3:
        return None

    def close(a, b):
        return abs(a - b) <= max(abs(b) * 0.03, 2.0)

    for div in (1.0, 100.0, 10.0):
        c = [v / div for v in nums]
        for i in range(1, len(c) - 1):
            if close(c[i], cprev) and close(c[i + 1], cyago):
                return {"cur": round(c[i - 1], 2), "prev": round(c[i], 2),
                        "yago": round(c[i + 1], 2), "div": div}
    return None


def anchor_series(rows, cprev, cyago, prefer_con, require_con=False):
    """Find the first row that double-anchors on (cprev, cyago). Rank owners-con rows first.
    require_con: only consider rows on a CONSOLIDATED page — used for insurers WITH subsidiaries so a
    look-alike standalone row (std ~= con) can never be filled as the owners-consolidated value."""
    pool = [r for r in rows if r[0]] if require_con else rows

    def rank(r):
        iscon, isown, _ = r
        return (0 if (iscon == prefer_con and isown) else
                1 if iscon == prefer_con else
                2 if isown else 3)
    for iscon, isown, nums in sorted(pool, key=rank):
        a = double_anchor(nums, cprev, cyago)
        if a:
            return a
    return None


def load_fund(path):
    return json.load(open(path, encoding="utf-8"))


def dump_fund(path, d):
    json.dump(d, open(path, "w", encoding="utf-8"), ensure_ascii=False, separators=(",", ":"))


def set_cell(fund, sym, qe, con, con_ann, std=None, std_ann=None):
    """Fill-only: create/extend [qe, std, annStd, con, annCon] without clobbering existing values."""
    rows = fund.setdefault(sym, [])
    row = next((r for r in rows if r[0] == qe), None)
    if row is None:
        row = [qe, None, None, None, None]
        rows.append(row); rows.sort(key=lambda r: r[0])
    changed = False
    if row[3] is None and con is not None:
        row[3] = round(con, 2); row[4] = con_ann; changed = True
    if std is not None and row[1] is None:
        row[1] = round(std, 2); row[2] = std_ann or con_ann; changed = True
    return changed


def extract(pdf, cfg, docs, sym, qe):
    """Return {cur_con, yago_con, cur_std, div} for quarter `qe` from a filing, or None if unanchored."""
    cprev_con = conval(docs, sym, prevq(qe)); cyago_con = conval(docs, sym, qe - 10000)
    rows = rows_from_pdf(pdf, cfg["ident"], ocr=False)     # fast: text layer only
    if rows is None:
        return None
    # For insurers WITH subsidiaries, the owners-consolidated value differs from standalone — require a
    # consolidated-page row so a look-alike standalone number can't be filled as con.
    a = anchor_series(rows, cprev_con, cyago_con, prefer_con=True, require_con=cfg["sub"])
    if not a:
        # Text layer yielded no anchor — retry with OCR on image pages (scanned filings, e.g. LIC).
        rows_ocr = rows_from_pdf(pdf, cfg["ident"], ocr=True)
        if rows_ocr:
            rows = rows_ocr
            a = anchor_series(rows, cprev_con, cyago_con, prefer_con=True, require_con=cfg["sub"])
    if not a:
        return None
    out = {"cur_con": a["cur"], "yago_con": a["yago"], "div": a["div"], "cur_std": None}
    # std: no-sub -> std==con; with-sub -> only when it independently double-anchors on stored std.
    if not cfg["sub"]:
        out["cur_std"] = a["cur"]
    else:
        s = anchor_series(rows, stdval(docs, sym, prevq(qe)), stdval(docs, sym, qe - 10000), prefer_con=False)
        if s:
            out["cur_std"] = s["cur"]
    return out


def process(sym, targets, o, docs, src, verify=False):
    cfg = INSURERS[sym]
    code = scrips.get(sym)
    results = []
    if not code:
        return [{"sym": sym, "qe": q, "status": "no-scripcode"} for q in targets]
    lo = str(min(targets) // 10000 * 10000 + 401)
    hi = str(max(targets) + 300)
    try:
        filings = datebound(o, code, lo, hi)
    except Exception as ex:
        return [{"sym": sym, "qe": q, "status": "fetch-err:" + str(ex)[:40]} for q in targets]

    for qe in sorted(targets, reverse=True):
        def candidate_pdfs():
            for annd, att, sub in [(a, att, sub) for (a, att, sub) in filings if qe_from_ann(a) == qe][:4]:
                pdf = fetch_pdf(o, att); time.sleep(1.0)
                if pdf:
                    yield annd, pdf
            for annd, pdf in nse_result_pdfs(sym, qe):
                yield annd, pdf

        picked = None; saw_pdf = False
        for annd, pdf in candidate_pdfs():
            saw_pdf = True
            r = extract(pdf, cfg, docs, sym, qe)
            if r:
                picked = (annd, r); break
        if not picked:
            results.append({"sym": sym, "qe": qe, "status": "no-filing" if not saw_pdf else "unanchored"})
            continue

        annd, r = picked
        cur_con = r["cur_con"]; cur_std = r["cur_std"]
        lo_r, hi_r = cfg["range"]
        range_ok = cur_con is not None and lo_r <= cur_con <= hi_r
        accept = range_ok            # a double-anchored value that's also in-range
        rec = {"sym": sym, "qe": qe, "ann": annd, "cur_con": cur_con, "std_fill": (cur_std if accept else None),
               "yago_con": r["yago_con"], "stored_yago": conval(docs, sym, qe - 10000),
               "range_ok": range_ok, "accept": bool(accept),
               "status": "OK" if accept else "out-of-range"}
        results.append(rec)
        if accept and not verify:
            c1 = set_cell(docs, sym, qe, cur_con, annd, cur_std, annd)
            c2 = set_cell(src,  sym, qe, cur_con, annd, cur_std, annd)
            rec["written"] = bool(c1 or c2)
    return results


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", type=int, default=5, help="discovery window (months back)")
    ap.add_argument("--only", default="", help="comma list of symbols to restrict to")
    ap.add_argument("--verify", type=int, default=0, help="re-read this quarter-end (YYYYMMDD), compare, NO write")
    args = ap.parse_args()

    global scrips
    scrips = json.load(open(os.path.join(HERE, "bse_scrips.json")))["by_id"]

    docs = load_fund(DOCS_FUND)
    src  = load_fund(SRC_FUND) if os.path.exists(SRC_FUND) else docs
    o = bse_session(); time.sleep(0.5)

    only = set(x.strip().upper() for x in args.only.split(",") if x.strip())
    syms = [s for s in INSURERS if (not only or s in only)]

    if args.verify:
        plan = {s: [args.verify] for s in syms}
    else:
        import datetime
        today = datetime.date.today()
        recent_qes = []
        for k in range(0, args.months + 3):
            m = today.month - k; y = today.year
            while m <= 0:
                m += 12; y -= 1
            qend = {1: (y - 1, 12, 31), 2: (y - 1, 12, 31), 3: (y - 1, 12, 31),
                    4: (y, 3, 31), 5: (y, 3, 31), 6: (y, 3, 31),
                    7: (y, 6, 30), 8: (y, 6, 30), 9: (y, 6, 30),
                    10: (y, 9, 30), 11: (y, 9, 30), 12: (y, 9, 30)}[m]
            recent_qes.append(qend[0] * 10000 + qend[1] * 100 + qend[2])
        recent_qes = sorted(set(recent_qes))
        plan = {s: [q for q in recent_qes if conval(docs, s, q) is None] for s in syms}
        plan = {s: g for s, g in plan.items() if g}

    if not plan:
        print("No insurer gaps in the discovery window — nothing to do."); return

    all_res = []
    for s in sorted(plan):
        res = process(s, plan[s], o, docs, src, verify=bool(args.verify))
        all_res.extend(res)
        for r in res:
            if args.verify:
                stored = conval(docs, r["sym"], r["qe"])
                rc = r.get("cur_con")
                match = "match" if (rc is not None and stored is not None
                                    and abs(rc - stored) <= max(abs(stored) * 0.01, 1.0)) else "DIFF"
                print("  %-11s %d  read_con=%-9s stored_con=%-9s  -> %s  [%s]"
                      % (r["sym"], r["qe"], rc, stored, r["status"],
                         match if r["status"] == "OK" else "-"))
            else:
                print("  %-11s %d  con=%-9s std=%-9s range=%s -> %s%s"
                      % (r["sym"], r["qe"], r.get("cur_con"), r.get("std_fill"),
                         r.get("range_ok"), r["status"], " [WRITTEN]" if r.get("written") else ""))

    json.dump({"ts": int(time.time()), "results": all_res}, open(LOG, "w"))

    if not args.verify:
        written = [r for r in all_res if r.get("written")]
        if written:
            dump_fund(DOCS_FUND, docs)
            if os.path.exists(SRC_FUND):
                dump_fund(SRC_FUND, src)
            open(FLAG, "w").write(str(int(time.time())))
            print("FILLED %d insurer quarter(s): %s"
                  % (len(written), ", ".join("%s %d" % (r["sym"], r["qe"]) for r in written)))
        else:
            print("No insurer values filled this run (no accepted reads).")


if __name__ == "__main__":
    main()
