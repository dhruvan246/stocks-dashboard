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

# MATCH ON HEADLINE + NEWSSUB, NEVER HEADLINE ALONE. BSE's HEADLINE is often useless — GYANDEV filed its
# June quarter under the headline "Please refer the attachment", NAM under "Pursuant to provision of
# Regulation 30 & 33 of SEBI (LODR)" — while NEWSSUB carries the real description ("Unaudited Financial
# Results For The Quarter Ended 30.06.2026"). Headline-only matching dropped both real filings, so the
# renderer fell back to an older quarter's PDF and the names looked like they hadn't reported at all.
# fetch_bse_results.qe_from_head() already reads both fields, which is why the feed knew better than we did.
#
# RESULT_HEAD's bare "financial" also matches "Chief Financial Officer", so a CFO appointment filed the
# same day as the results could outrank them and — with callers taking only the newest match — the real
# P&L was never fetched (INTEGRAEN Q1FY27). NOT_RESULT drops those, but only when nothing in the text
# says "financial results": a single filing that announces results AND an appointment must stay.
NOT_RESULT = re.compile(r"chief financial officer|\bcfo\b|key managerial|annual report|annual general meeting"
                        r"|newspaper (publication|advertisement)|trading window|book closure"
                        r"|certificate under regulation|resignation|appointment"
                        # "Voting Results and Scrutinizer's Report of the EGM" matches bare RESULT_HEAD's
                        # "result" and has no parseable quarter-end date, so it fell through the period
                        # check and got rendered as if it were the P&L (KALYANI, 2026-08 run: rendered its
                        # scrutinizer's report instead of the June-quarter results 3 announcements older).
                        r"|scrutinizer|postal ballot|extraordinary general meeting|e-?voting", re.I)
STRONG_RESULT = re.compile(r"financial results?|results? for the (quarter|period|half|year)", re.I)
BOARD_OUTCOME = re.compile(r"outcome of (the )?board|board meeting outcome", re.I)

def _rank(txt):
    if STRONG_RESULT.search(txt): return 0     # "Unaudited Financial Results for the quarter ended ..."
    if BOARD_OUTCOME.search(txt): return 1     # cover letter — may or may not embed the statement
    return 2

def _candidate(txt):
    """True if this announcement could be the results filing. STRONG wins outright so a combined
    results+appointment filing is never dropped by NOT_RESULT."""
    if STRONG_RESULT.search(txt): return True
    return bool(RESULT_HEAD.search(txt)) and not NOT_RESULT.search(txt)

def announcements(op, code, months=5):
    hi = datetime.date.today(); lo = hi - datetime.timedelta(days=30 * months)
    url = ("https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w?pageno=1&strCat=-1"
           "&strPrevDate=%s&strToDate=%s&strScrip=%s&strSearch=P&strType=C&subcategory=-1"
           % (lo.strftime("%Y%m%d"), hi.strftime("%Y%m%d"), code))
    try:
        tab = __import__("json").loads(B.get(op, url)).get("Table", []) or []
    except Exception:
        return []
    rows = []
    for r in tab:
        if not r.get("ATTACHMENTNAME"): continue
        txt = "%s | %s" % (str(r.get("HEADLINE") or ""), str(r.get("NEWSSUB") or ""))
        if _candidate(txt):
            rows.append((str(r.get("NEWS_DT") or "")[:10], r["ATTACHMENTNAME"], txt.strip(" |")))
    # NEWEST FIRST, rank only as a same-day tie-break. Ranking across dates would pull an older quarter's
    # tidily-titled filing ahead of today's vaguely-titled one — which is exactly how GYANDEV/NAM ended up
    # rendering their March quarter. Within one date, a real results filing still beats a CFO notice.
    return sorted(rows, key=lambda t: (t[0], -_rank(t[2])), reverse=True)

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
