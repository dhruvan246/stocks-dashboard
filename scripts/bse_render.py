# -*- coding: utf-8 -*-
"""Download a BSE company's OWN result filing and render its P&L pages to PNGs for VISION reading
(OCR mangles digits on these scanned filings — vision is accurate). Identity-guarded like fetch_bse_fund.

Usage: python -X utf8 scripts/bse_render.py <scripcode> [outdir]
Prints the PNG paths it wrote (the P&L-bearing pages). A caller (or agent) then reads them and extracts
Revenue from Operations + Profit for the period for the quarter(s) shown, minding the unit (lakh/crore).
"""
import os, sys, io, re, time, datetime, zipfile
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bse_fetch as B
import fitz

RESULT_HEAD = re.compile(r"result|outcome of (the )?board|financial", re.I)
PL_HINT = re.compile(r"profit|revenue from oper|total income|earnings per", re.I)

# RESULT_HEAD's bare "financial" also matches "Chief Financial Officer", "financial year", etc, so a CFO
# appointment filed the same day as the results outranked them and — with callers taking only the newest
# match — the real P&L was never fetched (INTEGRAEN Q1FY27). Drop those, and rank an explicit results
# headline above a bare board-outcome cover letter, which often carries no P&L of its own.
NOT_RESULT = re.compile(r"chief financial officer|\bcfo\b|key managerial|annual report|annual general meeting"
                        r"|newspaper (publication|advertisement)|trading window|book closure"
                        r"|certificate under regulation|resignation|appointment", re.I)
STRONG_RESULT = re.compile(r"financial results?|results? for the (quarter|period|half|year)", re.I)
BOARD_OUTCOME = re.compile(r"outcome of (the )?board", re.I)

def _rank(hd):
    if STRONG_RESULT.search(hd): return 0      # "Unaudited Financial Results for the quarter ended ..."
    if BOARD_OUTCOME.search(hd): return 1      # cover letter — may or may not embed the statement
    return 2

def announcements(op, code, months=5):
    hi = datetime.date.today(); lo = hi - datetime.timedelta(days=30 * months)
    url = ("https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w?pageno=1&strCat=-1"
           "&strPrevDate=%s&strToDate=%s&strScrip=%s&strSearch=P&strType=C&subcategory=-1"
           % (lo.strftime("%Y%m%d"), hi.strftime("%Y%m%d"), code))
    try:
        tab = __import__("json").loads(B.get(op, url)).get("Table", []) or []
    except Exception:
        return []
    rows = [(str(r.get("NEWS_DT") or "")[:10], r.get("ATTACHMENTNAME"), str(r.get("HEADLINE") or ""))
            for r in tab if r.get("ATTACHMENTNAME") and RESULT_HEAD.search(str(r.get("HEADLINE") or ""))
            and not NOT_RESULT.search(str(r.get("HEADLINE") or ""))]
    return sorted(rows, key=lambda t: _rank(t[2]))      # stable: newest-first order kept within each rank

def fetch_pdf(op, att):
    for base in ("https://www.bseindia.com/xml-data/corpfiling/AttachLive/",
                 "https://www.bseindia.com/xml-data/corpfiling/AttachHis/"):
        try:
            raw = B.get(op, base + att, b=True)
            if raw[:4] == b"%PDF": return raw
        except Exception: pass
    return None

def main():
    code = sys.argv[1]
    outdir = sys.argv[2] if len(sys.argv) > 2 else os.path.join(os.environ.get("TEMP", "/tmp"), "bse_render")
    os.makedirs(outdir, exist_ok=True)
    op = B.session(); time.sleep(1)
    wrote = []
    for annd, att, hd in announcements(op, code)[:3]:
        raw = fetch_pdf(op, att)
        if not raw: continue
        try: doc = fitz.open(stream=raw, filetype="pdf")
        except Exception: continue
        for pi in range(min(len(doc), 8)):
            txt = doc[pi].get_text()
            # a scanned page has no text; render it. a text page: render only if it hints at a P&L.
            if txt.strip() and not PL_HINT.search(txt): continue
            png = doc[pi].get_pixmap(dpi=210).tobytes("png")
            p = os.path.join(outdir, "%s_%s_p%d.png" % (code, annd.replace("-", ""), pi))
            open(p, "wb").write(png); wrote.append((p, annd, hd[:70]))
            if len(wrote) >= 6: break
        if wrote: break
    for p, annd, hd in wrote:
        print("%s\t%s\t%s" % (p, annd, hd))
    if not wrote: print("NO_PDF")

if __name__ == "__main__":
    main()
