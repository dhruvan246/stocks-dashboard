# -*- coding: utf-8 -*-
"""Apply the vision-extracted revenue/op cells (_b3_reads.json insurers, _bank_reads.json banks,
_resid_reads.json residual) into docs/sf_revop.json + scripts/revop_fundamentals.json, fill-only,
with the tracked provenance ledger scripts/vision_rev_fills.json (campaign convention).

Re-anchors at apply time: the stored sf_fundamentals PAT for (sym,qe,basis) must still match the
pat_seen recorded at read time within max(3%, 2cr) — else the cell is SKIPPED (listed at the end).
Idempotent + re-runnable after a reset-and-replay. fin flag: insurers/banks get row[6]=1.

Run: python -X utf8 scripts/_apply_reads.py [--dry]
"""
import os, sys, json, glob

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
DOCS = os.path.join(ROOT, "docs", "sf_revop.json")
SCR = os.path.join(HERE, "revop_fundamentals.json")
FUND = os.path.join(ROOT, "docs", "sf_fundamentals.json")
LEDGER = os.path.join(HERE, "vision_rev_fills.json")

SRC_FILES = [("_b3_reads.json", True), ("_bank_reads.json", True), ("_resid_reads.json", False),
             ("_vision_reads.json", None), ("_insurer_reads.json", True)]
# (file, fin_flag): insurers+banks are financial-format (row[6]=1); residual regular cos are not.
# fin_flag None -> the cell carries its own "fin" (the vision tail mixes banks and industrials).

# Sharded OCR workers (_ocr_revop.py --shard I/N --out-suffix _sN) each write their own reads
# file so they cannot clobber one another; glob them back in so no shard's cells are missed.
SRC_FILES.append(("_nsearch_reads.json", None))     # NSE archived-HTML pull; cell carries its own fin
SRC_FILES.append(("_nsexbrl_reads.json", None))     # NSE per-filing XBRL (2018-19)
SRC_FILES.append(("_mc_reads.json", None))          # Moneycontrol browser-driven (PAT-anchored per cell)
SRC_FILES.append(("_screener_reads.json", None))    # Screener.in annual-total derivation (see _screener_annual.py)
SRC_FILES.append(("_bsedet_reads.json", None))      # BSE detailed-results JSON (as-filed, _bse_detres.py)
# Companies delisted from EQUITY but still filing under BSE's DEBT segment (listed NCDs keep
# Reg-33/52 obligations alive): their results never appear under the equity scrip code, so the
# equity-side routes all report "no filing". Look the issuer up in ListofScripData?segment=Debt.
SRC_FILES.append(("_debt_reads.json", None))
for _p in sorted(glob.glob(os.path.join(HERE, "_nsexbrl_reads_*.json"))):
    SRC_FILES.append((os.path.basename(_p), None))
for _p in sorted(glob.glob(os.path.join(HERE, "_nsearch_reads_*.json"))):
    SRC_FILES.append((os.path.basename(_p), None))
for _p in sorted(glob.glob(os.path.join(HERE, "_resid_reads_*.json"))):
    SRC_FILES.append((os.path.basename(_p), False))
for _p in sorted(glob.glob(os.path.join(HERE, "_bank_reads_*.json"))):
    SRC_FILES.append((os.path.basename(_p), True))


def close(a, b):
    if a is None or b is None:
        return False
    return abs(a - b) <= max(2.0, 0.03 * max(abs(a), abs(b)))


def main():
    dry = "--dry" in sys.argv
    fund = json.load(open(FUND))
    fmap = {s: {r[0]: r for r in rows} for s, rows in fund.items()}
    revop = json.load(open(DOCS))
    scr = json.load(open(SCR)) if os.path.exists(SCR) else {}
    ledger = json.load(open(LEDGER)) if os.path.exists(LEDGER) else {}

    applied, skipped = [], []
    for fname, fin in SRC_FILES:
        p = os.path.join(HERE, fname)
        if not os.path.exists(p):
            continue
        reads = json.load(open(p))
        for sym, cells in reads.items():
            for qe_s, centry in cells.items():
              # a cell may carry BOTH bases as a list (PEL via successor entity) — replay-safe,
              # unlike sequential single-basis passes which the batch reset discards
              for c in (centry if isinstance(centry, list) else [centry]):
                qe = int(qe_s)
                basis = c.get("basis", "std")
                if c.get("rev") is None:
                    continue
                # re-anchor vs stored PAT
                row = fmap.get(sym, {}).get(qe)
                stored_pat = None
                if row:
                    stored_pat = row[1] if basis == "std" else (row[3] if len(row) > 3 else None)
                if not close(stored_pat, c.get("pat_seen")):
                    skipped.append((sym, qe, basis, "anchor drift stored=%s seen=%s" % (stored_pat, c.get("pat_seen"))))
                    continue
                ri, oi_ = (0, 2) if basis == "std" else (1, 3)
                pi = 4 if basis == "std" else 5
                for data in (revop, scr):
                    d = data.setdefault(sym, {})
                    cell = d.get(qe_s) or [None] * 9
                    if len(cell) < 9:
                        cell = cell + [None] * (9 - len(cell))
                    if cell[ri] is None:                       # fill-only
                        cell[ri] = c["rev"]
                    if c.get("op") is not None and cell[oi_] is None:
                        cell[oi_] = c["op"]
                    if cell[pi] is None and stored_pat is not None:
                        cell[pi] = stored_pat                  # PAT mirror slot
                    isfin = c.get("fin", 0) if fin is None else fin
                    if isfin:
                        cell[6] = 1
                    elif cell[6] is None:
                        cell[6] = 0
                    d[qe_s] = cell
                key = "%s|%d" % (sym, qe)
                ent = ledger.setdefault(key, {})
                ent[basis] = {"rev": c["rev"], "op": c.get("op"),
                              "src": "vision-manual band3/4 2026-07-27: " + c.get("src", "")}
                applied.append((sym, qe, basis))

    print("applied %d cells, skipped %d" % (len(applied), len(skipped)))
    for s in skipped:
        print("  SKIP", s)
    if not dry:
        json.dump(revop, open(DOCS, "w"), separators=(",", ":"))
        json.dump(scr, open(SCR, "w"), separators=(",", ":"))
        json.dump(ledger, open(LEDGER, "w"), indent=0, sort_keys=True)
        print("written: sf_revop.json, revop_fundamentals.json, vision_rev_fills.json")


if __name__ == "__main__":
    main()
