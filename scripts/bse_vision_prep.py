# -*- coding: utf-8 -*-
"""Prep step for the scheduled vision-fill routine. Finds BSE-only companies that FILED a result for the
current quarter but whose numbers we don't have yet, renders each one's P&L pages to PNGs, and writes a
manifest. A Claude routine then READS those PNGs (vision) and fills the numbers via merge_bse_vision.py.

No API key needed — the reading is done by the routine's own Claude, on the user's plan.

Output: <outdir>/manifest.json = [{scrip, ticker, name, mcap, pngs:[abs paths]}], biggest mcap first.

Run: python -X utf8 scripts/bse_vision_prep.py [--limit N] [--outdir DIR]
"""
import os, sys, re, json, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bse_fetch as B
import fitz, bse_render

HERE = os.path.dirname(os.path.abspath(__file__))
D = os.path.join(HERE, "..", "docs")

def norm(s): return re.sub(r"(limited|ltd)$", "", re.sub(r"[^a-z0-9]", "", str(s).lower()))

def pending_scrips(limit):
    qr = json.load(open(os.path.join(D, "quarterly_results.json"), encoding="utf-8"))
    qe = qr["quarters"][0]
    feed = json.load(open(os.path.join(D, "results_feed.json"), encoding="utf-8"))["rows"]
    univ = {r[1].upper(): r for r in json.load(open(os.path.join(D, "bse_universe.json"), encoding="utf-8"))["rows"]}
    bf = json.load(open(os.path.join(D, "bse_fundamentals.json"), encoding="utf-8"))["px"] if os.path.exists(os.path.join(D, "bse_fundamentals.json")) else {}
    out = {}
    for r in feed:
        if r[3] != qe: continue                      # feed row's parsed quarter must be the target
        tkr = r[0].upper(); u = univ.get(tkr)
        if not u: continue                            # BSE-only universe names only
        scrip = str(u[0])
        if scrip in bf and str(qe) in bf[scrip]: continue   # already have this quarter
        if scrip in out: continue
        out[scrip] = (tkr, u[2], u[6])                # ticker, name, mcap
    ordered = sorted(out.items(), key=lambda kv: -(kv[1][2] or 0))
    return qe, ordered[:limit]

def main():
    limit = int(sys.argv[sys.argv.index("--limit") + 1]) if "--limit" in sys.argv else 25
    outdir = sys.argv[sys.argv.index("--outdir") + 1] if "--outdir" in sys.argv else os.path.join(os.environ.get("TEMP", "/tmp"), "bse_pending")
    os.makedirs(outdir, exist_ok=True)
    qe, pend = pending_scrips(limit)
    print("target quarter %d — %d pending BSE-only result(s) to render" % (qe, len(pend)))
    op = B.session(); time.sleep(1)
    manifest = []
    for scrip, (tkr, name, mcap) in pend:
        pngs = []
        for annd, att, hd in bse_render.announcements(op, scrip)[:1]:
            raw = bse_render.fetch_pdf(op, att)
            if not raw: continue
            try: doc = fitz.open(stream=raw, filetype="pdf")
            except Exception: continue
            for pi in range(min(len(doc), 8)):
                txt = doc[pi].get_text()
                if txt.strip() and not bse_render.PL_HINT.search(txt): continue
                p = os.path.join(outdir, "%s_p%d.png" % (scrip, pi))
                open(p, "wb").write(doc[pi].get_pixmap(dpi=200).tobytes("png")); pngs.append(p)
                if len(pngs) >= 4: break
            if pngs: break
        if pngs:
            manifest.append({"scrip": scrip, "ticker": tkr, "name": name, "mcap": mcap, "pngs": pngs})
            print("  rendered %s %-12s (%d pages)" % (scrip, tkr, len(pngs)))
    json.dump(manifest, open(os.path.join(outdir, "manifest.json"), "w"))
    print("WROTE %s/manifest.json: %d companies ready to vision-read" % (outdir, len(manifest)))

if __name__ == "__main__":
    main()
