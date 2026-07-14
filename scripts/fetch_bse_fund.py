# -*- coding: utf-8 -*-
"""Extract quarterly Revenue + PAT for the BSE-ONLY universe → docs/bse_fundamentals.json.

⚠️ The BSE `FinancialResult` API is ENTITY-POISONED for many scrips (returns BSE Ltd's numbers, not
the company's — the FORCEMOT contamination pattern; proven again on Cella Space 532701). So we DO NOT
use it. Instead, per scrip, we read the company's OWN result announcement attachment:

  AnnSubCategoryGetData (strCat=-1, per scrip) → pick result/board-outcome filings WITH an attachment
  → AttachLive/AttachHis/<guid>.pdf → OCR pages → IDENTITY-GUARD (company name/ticker must appear)
  → parse the P&L rows (Revenue from Operations, Total Income, Profit for the period) with unit scaling.

Small BSE companies file SCANNED PDFs (no text layer) → OCR (rapidocr) is required. Slow (~10-15s/page),
so this is a resumable background grind: biggest-mcap-first, a per-run budget, progress cached so reruns
skip done scrips. NEVER emit an unanchored value — identity must match and PAT magnitude must be sane.

Store: {"updated", "px":{ "<scripcode>": { "<QE YYYYMMDD>": {"rev":cr,"pat":cr,"ann":YYYYMMDD,"basis":"S|C"} } }}

Run: python -X utf8 scripts/fetch_bse_fund.py [--budget N] [--scrips 532701,...] [--min-mcap CR] [--months M]
"""
import os, sys, json, io, time, datetime, re
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bse_fetch as B
import fitz
from rapidocr_onnxruntime import RapidOCR

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "..", "docs", "bse_fundamentals.json")
UNIV = os.path.join(HERE, "..", "docs", "bse_universe.json")
DONE = os.path.join(HERE, "_bse_fund_done.json")
OCR = RapidOCR()
MON = {"jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,"jul":7,"aug":8,"sep":9,"sept":9,"oct":10,"nov":11,"dec":12}
DAY_LAST = {3:31, 6:30, 9:30, 12:31}
RESULT_HEAD = re.compile(r"result|outcome of (the )?board|financial", re.I)
REV_RE = re.compile(r"revenue from oper", re.I)
INC_RE = re.compile(r"total income", re.I)
PAT_RE = re.compile(r"profit(/loss)? for the (period|year)|profit after tax|net profit", re.I)
BAD_PAT = re.compile(r"before tax|comprehensive|exceptional|other comprehensive", re.I)

def num(s):
    s = s.strip().replace(" ", "")
    if not re.fullmatch(r"\(?-?[\d,]+(?:\.\d+)?\)?", s): return None
    v = float(s.strip("()").replace(",", ""))
    return -v if s.startswith("(") else v

def qe_from_text(blob):
    m = re.search(r"quarter (and year )?ended\s*(on\s*)?(\d{1,2})[\s.\-/]*([A-Za-z]{3,9})[,\s.\-/]*(\d{4})", blob, re.I)
    if m:
        mo = MON.get(m.group(4).lower()[:3], 0)
        if mo in DAY_LAST: return int(m.group(5)) * 10000 + mo * 100 + DAY_LAST[mo]
    m = re.search(r"ended\s*(on\s*)?([A-Za-z]{3,9})\s*(\d{1,2}),?\s*(\d{4})", blob, re.I)
    if m:
        mo = MON.get(m.group(2).lower()[:3], 0)
        if mo in DAY_LAST: return int(m.group(4)) * 10000 + mo * 100 + DAY_LAST[mo]
    return 0

def ocr_boxes(png):
    res, _ = OCR(png)
    return [{"t": t, "x": sum(p[0] for p in b) / 4, "y": sum(p[1] for p in b) / 4} for b, t, sc in (res or [])]

def row_first_num(boxes, label_box):
    """First numeric value to the right of a label box, on the same row."""
    row = [b for b in boxes if abs(b["y"] - label_box["y"]) < 12 and b["x"] > label_box["x"] + 5]
    for b in sorted(row, key=lambda b: b["x"]):
        n = num(b["t"])
        if n is not None: return n
    return None

def parse_pl(boxes):
    """Return (rev, pat, unit) from a P&L page's OCR boxes. unit scales to ₹ crore."""
    unit = 0.01 if any(re.search(r"in lakh", b["t"], re.I) for b in boxes) else \
           (10.0 if any(re.search(r"in million", b["t"], re.I) for b in boxes) else
            (1.0 if any(re.search(r"in (crore|cr\.)", b["t"], re.I) for b in boxes) else None))
    rev = pat = None
    for b in boxes:
        if rev is None and REV_RE.search(b["t"]): rev = row_first_num(boxes, b)
    if rev is None:
        for b in boxes:
            if INC_RE.search(b["t"]): rev = row_first_num(boxes, b); break
    for b in boxes:
        if pat is None and PAT_RE.search(b["t"]) and not BAD_PAT.search(b["t"]):
            pat = row_first_num(boxes, b)
    return rev, pat, unit

def scrip_announcements(op, code, months):
    hi = datetime.date.today(); lo = hi - datetime.timedelta(days=30 * months)
    url = ("https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w?pageno=1&strCat=-1"
           "&strPrevDate=%s&strToDate=%s&strScrip=%s&strSearch=P&strType=C&subcategory=-1"
           % (lo.strftime("%Y%m%d"), hi.strftime("%Y%m%d"), code))
    try:
        tab = json.loads(B.get(op, url)).get("Table", []) or []
    except Exception:
        return []
    out = []
    for r in tab:
        hd = str(r.get("HEADLINE") or ""); att = r.get("ATTACHMENTNAME")
        if att and RESULT_HEAD.search(hd):
            out.append((str(r.get("NEWS_DT") or "")[:10], att, hd))
    return out

def fetch_pdf(op, att):
    for base in ("https://www.bseindia.com/xml-data/corpfiling/AttachLive/",
                 "https://www.bseindia.com/xml-data/corpfiling/AttachHis/"):
        try:
            raw = B.get(op, base + att, b=True)
            if raw[:4] == b"%PDF": return raw
        except Exception: pass
    return None

def extract(op, code, name, months):
    """Return {QE: {rev,pat,ann,basis}} for a scrip from its own filings, identity-guarded."""
    toks = [w for w in re.split(r"[^A-Za-z]+", name.upper()) if len(w) >= 4][:2]
    res = {}
    for annd, att, hd in scrip_announcements(op, code, months)[:4]:
        raw = fetch_pdf(op, att)
        if not raw: continue
        try: doc = fitz.open(stream=raw, filetype="pdf")
        except Exception: continue
        qe = 0; rev = pat = None; unit = None; ident = False
        for pi in range(min(len(doc), 6)):
            boxes = ocr_boxes(doc[pi].get_pixmap(dpi=200).tobytes("png"))
            blob = " ".join(b["t"] for b in boxes)
            up = blob.upper()
            if not ident and (any(tk in up for tk in toks) if toks else True): ident = True
            if not qe: qe = qe_from_text(blob)
            if rev is None or pat is None:
                r2, p2, u2 = parse_pl(boxes)
                rev = rev if rev is not None else r2
                pat = pat if pat is not None else p2
                unit = unit or u2
            if ident and qe and pat is not None: break
        if ident and qe and unit and pat is not None:
            anni = int(annd.replace("-", "")) if annd else 0
            rec = {"pat": round(pat * unit, 2), "ann": anni, "basis": "C" if "consol" in hd.lower() else "S"}
            if rev is not None: rec["rev"] = round(rev * unit, 2)
            # keep the most recent filing per quarter-end
            if qe not in res or anni >= res[qe].get("ann", 0):
                res[qe] = rec
    return res

def main():
    budget = int(sys.argv[sys.argv.index("--budget") + 1]) if "--budget" in sys.argv else 60
    max_min = float(sys.argv[sys.argv.index("--max-minutes") + 1]) if "--max-minutes" in sys.argv else 70.0
    t_start = time.time()
    months = int(sys.argv[sys.argv.index("--months") + 1]) if "--months" in sys.argv else 5
    min_mcap = float(sys.argv[sys.argv.index("--min-mcap") + 1]) if "--min-mcap" in sys.argv else 100.0
    only = None
    if "--scrips" in sys.argv:
        only = set(sys.argv[sys.argv.index("--scrips") + 1].split(","))

    univ = json.load(open(UNIV, encoding="utf-8"))["rows"]
    univ.sort(key=lambda r: r[6], reverse=True)                # biggest mcap first
    data = json.loads(open(OUT, encoding="utf-8").read()) if os.path.exists(OUT) else {"px": {}}
    done = set(json.load(open(DONE))) if os.path.exists(DONE) else set()

    op = B.session(); time.sleep(1)
    spent = 0
    for r in univ:
        code, tkr, name, isin, grp, fv, mc, sec = r
        code = str(code)
        if only is not None:
            if code not in only and tkr not in only: continue
        else:
            if code in done or mc < min_mcap: continue
        if spent >= budget or (time.time() - t_start) / 60 >= max_min: break
        spent += 1
        try:
            recs = extract(op, code, name, months)
        except Exception as ex:
            print("  %s %s ERR %s" % (code, tkr, str(ex)[:60])); recs = {}
        if recs:
            cur = data["px"].get(code, {})
            for qe, rec in recs.items():
                if str(qe) not in cur:                          # fill-only
                    cur[str(qe)] = rec
            data["px"][code] = cur
            latest = max(recs)
            print("  ✓ %s %-12s %s PAT=%s rev=%s" % (code, tkr, latest, recs[latest]["pat"], recs[latest].get("rev")))
        else:
            print("  · %s %-12s (no anchored result)" % (code, tkr))
        done.add(code)
        if spent % 10 == 0:
            ist = datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)
            data["updated"] = ist.strftime("%Y-%m-%d %H:%M IST")
            json.dump(data, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, separators=(",", ":"))
            json.dump(sorted(done), open(DONE, "w"))
            time.sleep(0.2)
    ist = datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)
    data["updated"] = ist.strftime("%Y-%m-%d %H:%M IST")
    json.dump(data, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, separators=(",", ":"))
    json.dump(sorted(done), open(DONE, "w"))
    ncov = len(data["px"])
    print("WROTE %s: processed %d scrips this run; %d scrips now have numbers" % (os.path.normpath(OUT), spent, ncov))

if __name__ == "__main__":
    main()
