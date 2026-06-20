# -*- coding: utf-8 -*-
"""Render the standalone-P&L page of a bank's BSE results filings to PNG for direct vision reading
(used when the text layer is scanned/garbled so OCR-locate fails). Renders page 1-2 (where the
quarterly results table sits) at high DPI. Run: python render_deepgap.py SYM qe1 qe2 ..."""
import bse_vision as V, bse_text as T, fitz, json, os, sys
SC = json.load(open("bse_scrips.json"))["by_id"]
OUT = "_deepgap"; os.makedirs(OUT, exist_ok=True)
def main():
    sym = sys.argv[1]; want = set(int(x) for x in sys.argv[2:])
    code = SC[sym]; o = V.session()
    fl = sorted(V.filings(o, code, pages=30, since="20180101"))
    done = set()
    for ann, att in fl:
        qe = T.qe_from_ann(ann)
        if qe not in want or qe in done: continue
        pdf = None
        for base in ("AttachHis", "AttachLive"):
            try:
                d = V.get(o, "https://www.bseindia.com/xml-data/corpfiling/%s/%s" % (base, att), b=True)
                if d[:4] == b"%PDF": pdf = d; break
            except Exception: pass
        if not pdf: print("%d: no-pdf" % qe, flush=True); continue
        doc = fitz.open(stream=pdf, filetype="pdf")
        for p in range(min(2, len(doc))):
            pix = doc[p].get_pixmap(dpi=200)
            fn = os.path.join(OUT, "%s_%d_p%d.png" % (sym, qe, p))
            pix.save(fn)
        done.add(qe); print("%d: rendered %d pages (pdf %d pp)" % (qe, min(2, len(doc)), len(doc)), flush=True)
    print("rendered:", sorted(done), flush=True)
if __name__ == "__main__":
    main()
