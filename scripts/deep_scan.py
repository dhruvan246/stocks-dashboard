# -*- coding: utf-8 -*-
"""Deep-scan scanned BSE results PDFs: for each wanted quarter, fetch the results filing(s), OCR
every page to FIND the standalone P&L table page (scored by count of P&L line-item keywords, which
separates the real results table from cover letters / press releases / auditor reports), then render
THAT page at high DPI for a clean single vision read. Writes _deepgap/<SYM>_<qe>.png (the table page).
Run: python -X utf8 deep_scan.py SYM qe1 qe2 ..."""
import bse_vision as V, bse_text as T, fitz, json, os, sys, numpy as np
SC = json.load(open("bse_scrips.json"))["by_id"]; OUT = "_deepgap"; os.makedirs(OUT, exist_ok=True)
KW = ["interest earned","income from operation","total income","operating profit","provision",
      "profit before tax","exceptional","tax expense","net profit","profit for the period",
      "profit after tax","earnings per","total expenditure","operating expenses"]
def score(txt):
    t = txt.lower(); return sum(1 for k in KW if k in t)
def main():
    sym = sys.argv[1]; want = [int(x) for x in sys.argv[2:]]; code = SC[sym]; o = V.session()
    fl = sorted(V.filings(o, code, pages=30, since="20180101"))
    byq = {}
    for ann, att in fl:
        qe = T.qe_from_ann(ann); byq.setdefault(qe, []).append(att)
    for qe in want:
        atts = byq.get(qe, [])
        best = None  # (score, pageimg_bytes, page_no, npp)
        for att in atts[:3]:
            pdf = None
            for base in ("AttachHis", "AttachLive"):
                try:
                    d = V.get(o, "https://www.bseindia.com/xml-data/corpfiling/%s/%s" % (base, att), b=True)
                    if d[:4] == b"%PDF": pdf = d; break
                except Exception: pass
            if not pdf: continue
            doc = fitz.open(stream=pdf, filetype="pdf")
            for p in range(min(len(doc), 12)):
                pg = doc[p]
                txt = pg.get_text()
                if len(txt.strip()) < 60:  # scanned page -> OCR it
                    res, _ = V.OCR(pg.get_pixmap(dpi=150).tobytes("png"))
                    txt = " ".join(b[1] for b in res) if res else ""
                s = score(txt)
                if s >= 4 and (best is None or s > best[0]):
                    best = (s, pdf, p, len(doc))
        if not best:
            print("%d: NO table page found (atts=%d)" % (qe, len(atts)), flush=True); continue
        s, pdf, p, npp = best
        doc = fitz.open(stream=pdf, filetype="pdf")
        doc[p].get_pixmap(dpi=300).save(os.path.join(OUT, "%s_%d.png" % (sym, qe)))
        print("%d: table page %d/%d score=%d -> %s_%d.png" % (qe, p, npp, s, sym, qe), flush=True)
if __name__ == "__main__":
    main()
