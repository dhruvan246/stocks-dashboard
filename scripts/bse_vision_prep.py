# -*- coding: utf-8 -*-
"""Prep step for the scheduled vision-fill routine. Finds companies — NSE **and** BSE-only — that FILED a
result for the current quarter but whose numbers we don't have yet, renders each one's P&L pages to PNGs,
and writes a manifest. A Claude routine then READS those PNGs (vision) and fills the numbers via
merge_bse_vision.py. This is the safety net that guarantees no declared result stays "numbers being
parsed" forever — NSE names whose XBRL hasn't posted, and scanned BSE micro-caps OCR can't read, both
get read from their own filing PDF.

No API key needed — the reading is done by the routine's own Claude, on the user's plan.

Output: <outdir>/manifest.json = [{exch:"NSE"|"BSE", sym, scrip, name, mcap, pngs:[abs paths]}], NSE first
then biggest-mcap.

Run: python -X utf8 scripts/bse_vision_prep.py [--limit N] [--outdir DIR]
"""
import os, sys, re, json, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fitz, bse_render
import bse_fetch as B
import fetch_announcements as FA
from results_pending import find_pending      # shared with build_results_coverage.py

HERE = os.path.dirname(os.path.abspath(__file__))
D = os.path.join(HERE, "..", "docs")
NSE_PDF = "https://nsearchives.nseindia.com/corporate/"

def norm(s): return re.sub(r"(limited|ltd)$", "", re.sub(r"[^a-z0-9]", "", str(s).lower()))

def render_pdf_pages(raw):
    try: doc = fitz.open(stream=raw, filetype="pdf")
    except Exception: return []
    out = []
    for pi in range(min(len(doc), 8)):
        txt = doc[pi].get_text()
        if txt.strip() and not bse_render.PL_HINT.search(txt): continue
        out.append(doc[pi].get_pixmap(dpi=200).tobytes("png"))
        if len(out) >= 4: break
    return out

def pdf_period(raw):
    """Read the reporting quarter the FILING itself states (not the — sometimes wrong — NSE caption).
    Late Q4/annual filings are frequently mislabelled by NSE as the current quarter; the audit report/
    P&L always says 'quarter ... ended <date>'. Returns a quarter-end int (e.g. 20260331) or 0."""
    try: doc = fitz.open(stream=raw, filetype="pdf")
    except Exception: return 0
    txt = " ".join(doc[pi].get_text() for pi in range(min(len(doc), 4)))
    return FA.parse_qe(txt)

def enrich_scrips(limit):
    """BSE-only companies we already cover but that are MISSING the year-ago quarter (filled by the
    fast OCR pass, which only grabbed the current quarter) — re-render so vision can add YoY/QoQ."""
    bf = json.load(open(os.path.join(D, "bse_fundamentals.json"), encoding="utf-8"))["px"]
    univ = {str(r[0]): r for r in json.load(open(os.path.join(D, "bse_universe.json"), encoding="utf-8"))["rows"]}
    out = []
    for scrip, qs in bf.items():
        if "20260630" in qs and "20250630" not in qs and scrip in univ:   # has current, missing year-ago
            r = univ[scrip]; out.append((scrip, (r[1].upper(), r[2], r[6])))
    out.sort(key=lambda kv: -(kv[1][2] or 0))
    return out[:limit]

def main():
    limit = int(sys.argv[sys.argv.index("--limit") + 1]) if "--limit" in sys.argv else 25
    outdir = sys.argv[sys.argv.index("--outdir") + 1] if "--outdir" in sys.argv else os.path.join(os.environ.get("TEMP", "/tmp"), "bse_pending")
    os.makedirs(outdir, exist_ok=True)
    if "--enrich" in sys.argv:                       # re-render already-covered names missing year-ago
        qe, nse, bse = 20260630, [], enrich_scrips(limit)
    else:
        qe, nse, bse = find_pending(limit)
    print("target quarter %d — pending: %d NSE, %d BSE-only" % (qe, len(nse), len(bse)))
    manifest = []

    # ---- NSE (fetch each announcement PDF from nsearchives) ----
    qfix = {}   # "SYM|YYYY-MM-DD" -> real quarter-end, when NSE's caption quarter is wrong
    if nse:
        import build_fundamentals as NB
        jar = NB.nse_jar(); hdr = {"User-Agent": NB.UA, "Referer": "https://www.nseindia.com/"}
        for sym, name, mcap, fn, fdate in nse:
            if not fn: continue
            url = fn if str(fn).startswith("http") else NSE_PDF + fn
            try: raw = NB._get(url, headers=hdr, jar=jar, timeout=60, binary=True)
            except Exception as ex: print("  NSE %s fetch err %s" % (sym, str(ex)[:40])); continue
            if not raw or raw[:4] != b"%PDF": continue
            real = pdf_period(raw)                             # what the filing itself says
            if real and real != qe:                           # NSE caption mislabelled the quarter
                qfix["%s|%s" % (sym, fdate)] = real
                print("  qfix NSE %-11s caption=%d -> filing=%d (skip)" % (sym, qe, real)); continue
            pngs = []
            for i, png in enumerate(render_pdf_pages(raw)):
                p = os.path.join(outdir, "NSE_%s_p%d.png" % (sym, i)); open(p, "wb").write(png); pngs.append(p)
            if pngs:
                manifest.append({"exch": "NSE", "sym": sym, "scrip": "", "name": name, "mcap": mcap, "pngs": pngs})
                print("  rendered NSE %-11s (%d pages)" % (sym, len(pngs)))
            time.sleep(0.4)
    # merge quarter corrections into the persistent side-file (write_results_feed applies them hourly)
    fixp = os.path.join(D, "feed_qe_fix.json")
    try: allfix = json.load(open(fixp, encoding="utf-8"))
    except Exception: allfix = {}
    allfix.update(qfix)
    if qfix:
        json.dump(allfix, open(fixp, "w", encoding="utf-8"), ensure_ascii=False, separators=(",", ":"))
        print("WROTE %s: +%d quarter fixes (%d total)" % (os.path.normpath(fixp), len(qfix), len(allfix)))

    # ---- BSE-only (via bse_fetch) ----
    if bse:
        op = B.session(); time.sleep(1)
        for scrip, (tkr, name, mcap) in bse:
            pngs = []
            for annd, att, hd in bse_render.announcements(op, scrip)[:1]:
                raw = bse_render.fetch_pdf(op, att)
                if not raw: continue
                for i, png in enumerate(render_pdf_pages(raw)):
                    p = os.path.join(outdir, "BSE_%s_p%d.png" % (scrip, i)); open(p, "wb").write(png); pngs.append(p)
                if pngs: break
            if pngs:
                manifest.append({"exch": "BSE", "sym": tkr, "scrip": scrip, "name": name, "mcap": mcap, "pngs": pngs})
                print("  rendered BSE %s %-11s (%d pages)" % (scrip, tkr, len(pngs)))

    json.dump(manifest, open(os.path.join(outdir, "manifest.json"), "w"))
    print("WROTE %s/manifest.json: %d companies ready to vision-read" % (outdir, len(manifest)))

if __name__ == "__main__":
    main()
