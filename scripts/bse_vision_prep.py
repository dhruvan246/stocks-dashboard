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

# The results table can sit deep in a filing — banks bury it behind a 8-10 page joint-auditors' report —
# and PL_HINT matches those auditor pages too ("net profit/(loss) after tax"). Scanning only the first 8
# pages and keeping the first 4 hits therefore rendered the review report and never reached the numbers
# (CENTRALBK Q1FY27: consolidated table on page 10 of 31). Scan wider and rank by NUMERIC DENSITY: a real
# results table is wall-to-wall figures (98-196 numeric tokens/page) while prose pages that merely mention
# profit have far fewer (<=32), so the table wins regardless of where in the filing it sits.
#
# A text-less page is a SCANNED image we can't score — it may be the table or may be a scanned review
# report. It must sit BETWEEN the two: above prose (we can't rule it out) but below a confirmed table
# (never crowd out real numbers). TELGE Q1FY27 is the case that pins this down — tables on pages 4 and
# 11, four blank scanned pages around them; ranking blanks first rendered all four blanks and neither
# table. In an all-scanned filing every page ties here and doc order is preserved, as before.
NUM_TOK = re.compile(r"\d[\d,]{2,}")
SCAN_SCORE = 40        # observed: real tables 49-196 numeric tokens/page, prose <=32

def render_pdf_pages(raw):
    try: doc = fitz.open(stream=raw, filetype="pdf")
    except Exception: return []
    cands = []
    for pi in range(min(len(doc), 24)):
        txt = doc[pi].get_text()
        if not txt.strip():
            score = SCAN_SCORE                      # scanned page — no text to judge, worth a look
        elif bse_render.PL_HINT.search(txt):
            score = len(NUM_TOK.findall(txt))       # P&L-ish prose vs an actual table of numbers
        else:
            continue
        cands.append((-score, pi))
    cands.sort()
    keep = sorted(pi for _, pi in cands[:4])
    return [doc[pi].get_pixmap(dpi=200).tobytes("png") for pi in keep]

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

# NSE rate-limits archive downloads per IP: after a burst it answers 403 (or 429). Before 2026-07-19
# the prep just printed "fetch err 403" and `continue`d — so one throttle abandoned that name for the
# whole run, and because the NEXT scheduled run could hit the same wall at the same point, big filers
# (AXISBANK/KOTAKBANK/PNB/INDIACEM/JKCEMENT, 2026-07-18) sat unfilled across BOTH runs. Fix: never skip
# on a retryable error — wait (growing backoff), re-warm the NSE session cookie (a fresh jar clears the
# throttle), and try again. Only a hard 404 / non-retryable status gives up. A 200 that isn't a %PDF
# (NSE error/te stub) is treated as retryable too.
def _nse_warm(NB):
    """Cookie jar warmed on BOTH the home page and the announcements page — nsearchives
    checks that the session has 'visited' the referer page before it serves an attachment."""
    jar = NB.nse_jar()   # home + financial-results
    try:
        NB._get("https://www.nseindia.com/companies-listing/corporate-filings-announcements",
                headers={"User-Agent": NB.UA, "Accept": "text/html"}, jar=jar, timeout=20)
    except Exception:
        pass
    return jar

RETRYABLE = {403, 429, 500, 502, 503, 504}
def _nse_pdf_with_retry(NB, url, hdr, jar_box, sym, tries=3):   # brief ride-out; BSE fallback covers a hard block
    import urllib.error
    delay = 5
    for attempt in range(1, tries + 1):
        reason = None
        try:
            raw = NB._get(url, headers=hdr, jar=jar_box[0], timeout=60, binary=True)
            if raw and raw[:4] == b"%PDF":
                return raw
            reason = "non-PDF (%d bytes)" % (len(raw) if raw else 0)
        except urllib.error.HTTPError as ex:
            reason = "HTTP %s" % ex.code
            if ex.code not in RETRYABLE:
                print("  NSE %-11s hard %s — not retryable" % (sym, reason)); return None
        except Exception as ex:
            reason = type(ex).__name__
        if attempt < tries:
            print("  NSE %-11s %s — retry %d/%d in %ds (re-warm cookie)"
                  % (sym, reason, attempt, tries - 1, delay))
            time.sleep(delay)
            try: jar_box[0] = _nse_warm(NB)   # fresh cookies + pause clears NSE's per-IP throttle
            except Exception: pass
            delay = min(delay * 2, 60)
    return None

def _bse_fallback(sym, by_id, op_box, outdir, qe):
    """When NSE's nsearchives hard-403s a scripted download, a DUAL-LISTED name can be read off
    BSE (AttachLive/AttachHis) instead — BSE doesn't block us, so this is what actually rescues the
    big banks/large-caps (AXISBANK/KOTAK/PNB/INDIACEM/JKCEMENT, 2026-07-18) when the NSE fetch fails.
    Numbers still route to vision_fills as an NSE fill (manifest exch stays 'NSE'); we only borrow the
    BSE PDF. Returns rendered NSE_<sym>_pN.png paths, or [] (NSE-only SME, or BSE has no matching filing).
    Same quarter tripwire as the BSE branch: never render a filing whose stated period isn't the target."""
    scrip = by_id.get(sym)
    if not scrip:
        return []                              # NSE-only (e.g. an SME) — no BSE copy exists
    import bse_fetch as B, bse_render
    if op_box[0] is None:
        op_box[0] = B.session(); time.sleep(1)
    op = op_box[0]
    try:
        anns = bse_render.announcements(op, str(scrip))[:4]
    except Exception as ex:
        print("  NSE %-11s BSE-fallback error %s" % (sym, type(ex).__name__)); return []
    for annd, att, hd in anns:
        raw = bse_render.fetch_pdf(op, att)
        if not raw:
            continue
        real = pdf_period(raw)
        if real and real != qe:                # wrong quarter's filing — try the next candidate
            continue
        pngs = []
        for i, png in enumerate(render_pdf_pages(raw)):
            p = os.path.join(outdir, "NSE_%s_p%d.png" % (sym, i)); open(p, "wb").write(png); pngs.append(p)
        if pngs:
            return pngs
    return []

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
        try: by_id = json.load(open(os.path.join(HERE, "bse_scrips.json"), encoding="utf-8"))["by_id"]
        except Exception: by_id = {}          # SYM -> BSE scrip, for the dual-listed fallback
        bse_op_box = [None]                    # lazy BSE session, only opened if an NSE fetch fails
        # nsearchives 403s scripted downloads unless the request looks like a real browser click
        # from the announcements page (Referer + Sec-Fetch + a session cookie warmed on that page).
        hdr = {"User-Agent": NB.UA,
               "Referer": "https://www.nseindia.com/companies-listing/corporate-filings-announcements",
               "Accept": "application/pdf,*/*", "Accept-Language": "en-US,en;q=0.9",
               "Sec-Fetch-Site": "same-site", "Sec-Fetch-Mode": "navigate", "Sec-Fetch-Dest": "document"}
        jar_box = [_nse_warm(NB)]     # mutable holder so a retry can swap in a fresh cookie jar
        for sym, name, mcap, fn, fdate in nse:
            if not fn: continue
            url = fn if str(fn).startswith("http") else NSE_PDF + fn
            raw = _nse_pdf_with_retry(NB, url, hdr, jar_box, sym)
            if not raw:
                # NSE blocked the download — read the SAME result off BSE (dual-listed names)
                fb = _bse_fallback(sym, by_id, bse_op_box, outdir, qe)
                if fb:
                    manifest.append({"exch": "NSE", "sym": sym, "scrip": "", "name": name, "mcap": mcap, "pngs": fb})
                    print("  rendered NSE %-11s via BSE fallback (%d pages)" % (sym, len(fb)))
                else:
                    print("  NSE %-11s UNFETCHED (NSE 403 + no BSE copy) — left pending" % sym)
                continue
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
            time.sleep(1.5)   # space downloads out so we don't trip NSE's per-IP rate limit
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
            # try the next-best announcement when one yields no P&L pages (a board-outcome cover letter
            # often has none) — costs extra BSE hits only on the names that would otherwise stay empty
            for annd, att, hd in bse_render.announcements(op, scrip)[:3]:
                raw = bse_render.fetch_pdf(op, att)
                if not raw: continue
                # TRIPWIRE: never hand vision a PDF for the wrong quarter. If the filing states a period and
                # it isn't the one we're filling, we picked the WRONG ANNOUNCEMENT — that is a fetch bug on
                # our side, NOT evidence the company skipped the quarter. Rendering it anyway is how
                # GYANDEV/NAM (2026-07-17) got read off their March PDF and reported as "never filed June",
                # for names that had filed that morning. Skip to the next candidate and say so out loud.
                # period 0 = scanned/no text layer: we genuinely can't tell, so don't block on it.
                real = pdf_period(raw)
                if real and real != qe:
                    print("  ⚠ BSE %-11s %s: filing states %d, want %d — wrong announcement, trying next"
                          % (tkr, annd, real, qe))
                    continue
                for i, png in enumerate(render_pdf_pages(raw)):
                    p = os.path.join(outdir, "BSE_%s_p%d.png" % (scrip, i)); open(p, "wb").write(png); pngs.append(p)
                if pngs: break
            if pngs:
                manifest.append({"exch": "BSE", "sym": tkr, "scrip": scrip, "name": name, "mcap": mcap, "pngs": pngs})
                print("  rendered BSE %s %-11s (%d pages)" % (scrip, tkr, len(pngs)))
            else:
                print("  ✗ BSE %s %-11s: no candidate yielded a %d P&L — NOT proof it didn't file; check "
                      "the announcement pick (runbook 17) before concluding anything" % (scrip, tkr, qe))

    json.dump(manifest, open(os.path.join(outdir, "manifest.json"), "w"))
    print("WROTE %s/manifest.json: %d companies ready to vision-read" % (outdir, len(manifest)))

if __name__ == "__main__":
    main()
