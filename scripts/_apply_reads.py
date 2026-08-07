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


PRE2015_LEDGERS = [os.path.join(HERE, "pre2015_reads_d.json"),   # STEP D: BSE detres 2008-14
                   os.path.join(HERE, "pre2015_reads_n.json"),   # STEP N: NSE archive 2005-07 + residue
                   os.path.join(HERE, "pre2015_reads_w.json"),   # STEP W: archived NSE eod/results.jsp 2002-04
                   os.path.join(HERE, "pre2015_reads_a.json"),   # STEP A: NSE annual-minus-3-siblings derivation
                   os.path.join(HERE, "pre2015_reads_e.json")]   # STEP E: detres EPS-recon re-gate of D's unread-EPS refusals


def _load_pre2015_reads():
    """Merge every step's ledger (same cell shape, disjoint (sym,qe) universes by
    construction -- STEP N's gap universe explicitly excludes cells STEP D already
    landed) into one dict. A symbol appearing in both files just gets its per-qe
    keys unioned."""
    out = {}
    for p in PRE2015_LEDGERS:
        if not os.path.exists(p):
            continue
        for sym, cells in json.load(open(p, encoding="utf8")).items():
            out.setdefault(sym, {}).update(cells)
    return out


def main_pre2015():
    """PRE2015_CAMPAIGN STEP D+N applier (scripts/PRE2015_CAMPAIGN.md, LANDING RULES).
    Separate code path from the default mode above: almost no pre-2015 cell has a
    stored PAT to anchor against, so unlike the default flow this one CREATES the
    PAT row in docs/sf_fundamentals.json when the harvest ledger's gate (S/X/F/E)
    proved it, rather than requiring one to already exist (gate S is the one case
    where a stored PAT already exists -- re-verified here, not just trusted from
    harvest time, in case another session/CI filled it in the meantime). Only
    touches STD slots: pre-2015 con was optional (Clause 41) and neither detres nor
    this campaign's NSE route carry a landed con reading, so this route never
    writes con (LANDING RULES 3). Fill-only, idempotent, safe to re-run after a
    reset (the batch_push pattern). Reads every ledger in PRE2015_LEDGERS (STEP D's
    pre2015_reads_d.json + STEP N's pre2015_reads_n.json) so one applier serves
    every step without change.
    Run: python -X utf8 scripts/_apply_reads.py --pre2015 [--dry]
    """
    dry = "--dry" in sys.argv
    reads = _load_pre2015_reads()
    fund = json.load(open(FUND))
    revop = json.load(open(DOCS))
    scr = json.load(open(SCR)) if os.path.exists(SCR) else {}

    applied, skipped, fund_new = [], [], 0
    touched_syms = set()
    for sym, cells in reads.items():
        frows = fund.setdefault(sym, [])
        fmap = {r[0]: r for r in frows}
        for qe_s, c in cells.items():
            qe = int(qe_s)
            gate = c.get("gate")
            pat = c.get("pat")
            if c.get("basis", "std") != "std":
                skipped.append((sym, qe, "non-std basis unsupported in pre2015 mode"))
                continue

            row = fmap.get(qe)
            if gate == "S":
                if row is None or row[1] is None:
                    skipped.append((sym, qe, "gate-S but no stored PAT at apply time (drifted?)"))
                    continue
                if not close(row[1], pat):
                    skipped.append((sym, qe, "gate-S anchor drift at apply: stored=%s read=%s" % (row[1], pat)))
                    continue
                stored_pat = row[1]
            elif gate in ("F", "E", "X", "A"):
                if row is not None and row[1] is not None:
                    # a PAT has landed since harvest time (another route/session) --
                    # re-anchor instead of blindly inserting a duplicate row
                    if not close(row[1], pat):
                        skipped.append((sym, qe, "gate-%s but stored PAT now present and disagrees: stored=%s read=%s"
                                        % (gate, row[1], pat)))
                        continue
                    stored_pat = row[1]
                else:
                    newrow = [qe, pat, c.get("ann"), None, None]
                    frows.append(newrow)
                    fmap[qe] = newrow
                    fund_new += 1
                    stored_pat = pat
            else:
                skipped.append((sym, qe, "unknown/missing gate %r" % gate))
                continue

            for data in (revop, scr):
                d = data.setdefault(sym, {})
                cell = d.get(qe_s) or [None] * 9
                if len(cell) < 9:
                    cell = cell + [None] * (9 - len(cell))
                if cell[0] is None and c.get("rev") is not None:
                    cell[0] = c["rev"]
                if cell[2] is None and c.get("op") is not None:
                    cell[2] = c["op"]
                if cell[4] is None:
                    cell[4] = stored_pat
                if c.get("fin"):
                    cell[6] = 1
                elif cell[6] is None:
                    cell[6] = 0
                d[qe_s] = cell
            applied.append((sym, qe, gate))
            touched_syms.add(sym)

    for sym in touched_syms:
        fund[sym].sort(key=lambda r: r[0])

    from collections import Counter
    gc = Counter(g for _, _, g in applied)
    print("pre2015: applied %d cells (%s) | %d new fundamentals rows | skipped %d" % (
        len(applied), ", ".join("%s=%d" % kv for kv in sorted(gc.items())), fund_new, len(skipped)))
    for s in skipped:
        print("  SKIP", s)
    if not dry:
        json.dump(fund, open(FUND, "w"), separators=(",", ":"))
        json.dump(revop, open(DOCS, "w"), separators=(",", ":"))
        json.dump(scr, open(SCR, "w"), separators=(",", ":"))
        print("written: sf_fundamentals.json, sf_revop.json, revop_fundamentals.json")


def main():
    if "--pre2015" in sys.argv:
        return main_pre2015()
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
