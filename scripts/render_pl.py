# -*- coding: utf-8 -*-
"""Render the consolidated-P&L page(s) of a company's BSE-filed result to PNG for a vision read.
For scanned filings where the text layer has no attribution block. Locates the page by OCR/text
keyword scan (consolidated + owners/non-controlling), renders at high DPI, prints stored value + paths.

Usage: python3 scripts/render_pl.py OUTDIR SYM|QE [SYM|QE ...]
"""
import os, sys, re, time, datetime, json
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fitz
import fetch_insurers as FI

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
scrips = json.load(open(os.path.join(HERE, "bse_scrips.json")))["by_id"]
fund = json.load(open(os.path.join(ROOT, "docs", "sf_fundamentals.json")))

KW = re.compile(r"non[- ·]?controlling|minorit|attributable|owners of|equity holder", re.I)
CON = re.compile(r"consolidat", re.I)
PL = re.compile(r"profit.{0,30}(period|year|after tax|before tax)|total income|comprehensive", re.I)


def storedcon(s, q):
    return next((r[3] for r in fund.get(s, []) if r[0] == q), None)


def find_and_render(o, sym, qe, outdir, dpi=230):
    code = scrips.get(sym)
    stored = storedcon(sym, qe)
    if not code:
        return {"sym": sym, "qe": qe, "err": "no-scripcode"}
    qd = datetime.date(qe // 10000, (qe // 100) % 100, qe % 100)
    lo = (qd + datetime.timedelta(days=1)).strftime("%Y%m%d")
    hi = (qd + datetime.timedelta(days=210)).strftime("%Y%m%d")
    filings = FI.datebound(o, code, lo, hi)
    cand = sorted([(a, att, sub) for a, att, sub in filings if FI.qe_from_ann(a) == qe])
    if not cand:
        return {"sym": sym, "qe": qe, "stored": stored, "err": "no-filing"}
    for annd, att, sub in cand[:4]:
        pdf = FI.fetch_pdf(o, att); time.sleep(0.6)
        if not pdf:
            continue
        try:
            doc = fitz.open(stream=pdf, filetype="pdf")
        except Exception:
            continue
        N = min(len(doc), 45)
        pages_score = []
        for p in range(N):
            t = doc[p].get_text()
            if t.strip():
                txt = t
            else:
                txt = " ".join(w[4] for w in FI._ocr_words(doc[p]))
            score = 0
            if KW.search(txt): score += 2
            if CON.search(txt): score += 1
            if PL.search(txt): score += 1
            # count numeric density (a real P&L table)
            if len(re.findall(r"\d[\d,]*\.\d\d", txt)) >= 6: score += 1
            if score >= 3:
                pages_score.append((score, p))
        pages_score.sort(reverse=True)
        rendered = []
        for score, p in pages_score[:3]:
            pix = doc[p].get_pixmap(dpi=dpi)
            fn = os.path.join(outdir, f"{sym}_{qe}_p{p}.png")
            pix.save(fn)
            rendered.append((p, score, fn))
        if rendered:
            return {"sym": sym, "qe": qe, "stored": stored, "ann": annd, "att": att,
                    "pages": rendered}
    return {"sym": sym, "qe": qe, "stored": stored, "err": "no-attribution-page"}


def main():
    outdir = sys.argv[1]
    os.makedirs(outdir, exist_ok=True)
    cells = []
    for a in sys.argv[2:]:
        parts = a.split("|")
        cells.append((parts[0], int(parts[1])))
    o = FI.bse_session(); time.sleep(0.5)
    for sym, qe in cells:
        r = find_and_render(o, sym, qe, outdir)
        if r.get("pages"):
            print(f"{sym} {qe}  stored={r['stored']}  ann={r['ann']}")
            for p, sc, fn in r["pages"]:
                print(f"    page {p} (score {sc}): {fn}")
        else:
            print(f"{sym} {qe}  stored={r.get('stored')}  -> {r.get('err')}")


if __name__ == "__main__":
    main()
