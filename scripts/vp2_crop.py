# -*- coding: utf-8 -*-
"""Phase-2 vision cropper for consolidated-gap recovery. For each residual gap (v==null in
_congap_recovered.json) that has a cached _vpdf PDF, render a readable composite: a label bar
(sym, quarter, stored prev/yago for cross-check) + the column-header band + the consolidated
profit/owners rows band, at high DPI. The agent reads these and accepts a value only when the
sibling columns match the stored neighbors (or the visible header date confirms the column).
Renders the next BATCH (recent + material first) not already rendered/read.
Run: python -X utf8 vp2_crop.py [BATCH]
"""
import os, sys, json, re
import fitz, cv2, numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
data = json.load(open(os.path.join(HERE, "..", "docs", "sf_fundamentals.json")))
rec = json.load(open(os.path.join(HERE, "_congap_recovered.json")))
VP2 = os.path.join(HERE, "_vp2"); os.makedirs(VP2, exist_ok=True)
READF = os.path.join(HERE, "_vp2_read.json")
read_done = json.load(open(READF)) if os.path.exists(READF) else {}
BATCH = int(sys.argv[1]) if len(sys.argv) > 1 else 15

def prevq(qe):
    y, md = qe // 10000, qe % 10000
    return {331:(y-1)*10000+1231, 630:y*10000+331, 930:y*10000+630, 1231:y*10000+930}[md]
def conval(s, qe):
    for r in data.get(s, []):
        if r[0] == qe: return r[3]
    return None

AUD = re.compile(r'(independent auditor|auditor.?s report|limited review|review report|we have audited|'
                 r'we draw attention|emphasis of matter|our (conclusion|opinion|review)|based on our (review|audit)|'
                 r'to the (board|members)|key (standalone )?financial)', re.I)
PFT = re.compile(r'profit\s*/?\s*\(?\s*loss\)?\s*(after tax|for the (period|quarter|year))', re.I)
DEC = re.compile(r'\d[\d,]*\.\d\d')
SEG = re.compile(r'(segment (revenue|result|report|asset|liabilit)|disclosures? in compliance|'
                 r'debt[\s-]*equity|ratio\b.*\btimes|coverage ratio|balance sheet|cash flow)', re.I)
REV = re.compile(r'(revenue from operations|total income|total revenue)', re.I)

def find_con_page(doc):
    """Find the CONSOLIDATED P&L STATEMENT page: must have P&L markers (revenue + a profit row),
    NOT a ratios/segment/auditor/balance-sheet page. Falls back to a revenue-bearing page when the
    profit-row label is OCR-garbled."""
    pl = []; dense = []
    for p in range(min(len(doc), 30)):
        t = doc[p].get_text(); low = t.lower()
        if "consolidated" not in low or AUD.search(t) or SEG.search(t): continue
        nnum = len(DEC.findall(t))
        if nnum < 6 or not REV.search(t): continue
        if "profit for the" in low or PFT.search(t): pl.append((nnum, p))
        else: dense.append((nnum, p))            # P&L page with a garbled profit-row label
    if pl: pl.sort(reverse=True); return pl[0][1]
    if dense: dense.sort(reverse=True); return dense[0][1]
    return None

NPAT = re.compile(r'(net\s+)?profit\s*/?\s*\(?\s*loss\)?\s*.{0,18}(after tax|for the (period|quarter|year))', re.I)

def profit_band(pg):
    """y-range covering the net-profit-after-tax row through the owners/NCI attribution rows."""
    H = pg.rect.height; ys = []
    for b in pg.get_text("dict")["blocks"]:
        for ln in b.get("lines", []):
            t = " ".join(s["text"] for s in ln["spans"]); low = t.lower()
            if len(DEC.findall(t)) < 2: continue
            if "before" in low or "comprehensive" in low or "exceptional" in low: continue
            if (NPAT.search(t) or "profit for the" in low or "profit/(loss) for" in low or "profit /(loss) for" in low
                    or "owners of" in low or "non-controlling" in low or "equity holder" in low):
                ys.append(ln["bbox"][1] / H)
    if ys: return max(0.04, min(ys) - 0.045), min(0.96, max(ys) + 0.05)
    return 0.40, 0.84

def render(sym, q, pdfpath):
    """Render the WHOLE consolidated P&L table region (header + all rows) as one image — robust to
    layout (never cuts the net-profit row). Label bar carries stored neighbors for cross-check."""
    try: doc = fitz.open(pdfpath)
    except Exception: return None
    p = find_con_page(doc)
    if p is None: return None
    pg = doc[p]; H = pg.rect.height; W = pg.rect.width
    y0, y1 = profit_band(pg)
    # Side-by-side detection: standalone AND consolidated both as headers in the top band -> crop the
    # label strip + the consolidated (right) half only, so the consolidated digits stay large/readable.
    toptext = " ".join(w[4] for w in pg.get_text("words") if w[1] / H < 0.33).lower()
    sbs = "standalone" in toptext and "consolidated" in toptext
    conx = 0.52 if sbs else 0.30
    def piece(x0, x1, a, b):
        pm = pg.get_pixmap(dpi=300, clip=fitz.Rect(W * x0, H * a, W * x1, H * b))
        im = np.frombuffer(pm.samples, np.uint8).reshape(pm.height, pm.width, pm.n)
        return cv2.cvtColor(im, cv2.COLOR_RGB2BGR) if pm.n == 3 else cv2.cvtColor(im, cv2.COLOR_RGBA2BGR)
    def band(a, b):
        if sbs:
            L = piece(0.0, 0.30, a, b); R = piece(conx, 1.0, a, b)
            h = min(L.shape[0], R.shape[0]); sep = np.full((h, 14, 3), 200, np.uint8)
            return cv2.hconcat([L[:h], sep, R[:h]])
        return piece(0.0, 1.0, a, b)
    Wt = 2200
    def rw(im): return cv2.resize(im, (Wt, int(im.shape[0] * Wt / im.shape[1])))
    try:
        hdr = rw(band(0.10, 0.27)); prof = rw(band(y0, y1))
    except Exception: return None
    pq, yq = prevq(q), q - 10000
    bar = np.full((58, Wt, 3), 30, np.uint8)
    cv2.putText(bar, "%s  Qend=%d   stored prev(%d)=%s  yago(%d)=%s" % (sym, q, pq, conval(sym, pq), yq, conval(sym, yq)),
                (10, 40), cv2.FONT_HERSHEY_SIMPLEX, 0.9, (255, 255, 255), 2)
    out = np.vstack([bar, hdr, np.full((6, Wt, 3), 210, np.uint8), prof])
    fn = os.path.join(VP2, "%s_%d.png" % (sym, q)); cv2.imwrite(fn, out)
    return fn

def main():
    # residuals with cached PDF, not yet vision-read; recent + material first
    cand = []
    for k, v in rec.items():
        if v.get("v") is not None: continue
        if k in read_done: continue
        s, q = k.rsplit("|", 1); q = int(q)
        pdfp = os.path.join(HERE, "_vpdf", "%s_%d.pdf" % (s, q))
        if not os.path.exists(pdfp): continue
        mag = max(abs(conval(s, prevq(q)) or 0), abs(conval(s, q - 10000) or 0))
        cand.append((q, mag, s, pdfp))
    cand.sort(key=lambda x: (-x[0], -x[1]))
    manifest = []
    for q, mag, s, pdfp in cand:
        if len(manifest) >= BATCH: break
        fn = render(s, q, pdfp)
        if fn: manifest.append({"sym": s, "qe": q, "prevq": prevq(q), "yagoq": q - 10000,
                                "prev": conval(s, prevq(q)), "yago": conval(s, q - 10000), "img": os.path.basename(fn)})
    json.dump(manifest, open(os.path.join(VP2, "manifest.json"), "w"), indent=1)
    print("rendered %d crops; %d cached residuals remain after this batch" % (len(manifest), len([c for c in cand]) - len(manifest)))
    for m in manifest: print("  %s_%d.png  prev=%s yago=%s" % (m["sym"], m["qe"], m["prev"], m["yago"]))

if __name__ == "__main__":
    main()
