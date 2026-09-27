# -*- coding: utf-8 -*-
"""Take the ALIAS TARGET's NSE-era rows off a BSE-ticker collision key (runbook §197).

WHY. docs/bse_alias_collisions.json lists BSE-only dashboard tickers that are also a FORMER NSE ticker of
another company (WORTH = Worth Investment on BSE; NSE's WORTH was Worth Peripherals until 2025-10-10). The
NSE results pipeline stored that other company's filings under the old NSE symbol — sf_revop['WORTH'] is
Worth Peripherals' 2020-09..2025-06, identical to WORTHPERI's own rows — so every site consumer that reads a
store by the bare ticker (the stock page slice, discovery buckets) served Worth Peripherals' numbers under
Worth Investment's name. Runbook §30 step 4 is the rule for a rename's old key: its rows belong under the
current key, and the old key goes.

WHAT IT DOES, per collision key OLD -> TARGET, per store:
  1. PROOF the rows are TARGET's (key level): at least one quarter's headline agrees with TARGET's own stored
     row (revenue / operating profit / EBIT, or standalone PAT) and NO quarter agrees with the BSE company's
     own filings (docs/bse_fundamentals.json px[scrip]). Unproven keys (AZTEC: 2023-26 rows under a ticker whose
     target stopped filing in 2009) are left untouched and named.
  2. MOVE, fill-only, only where the served headline corroborates it (every field both rows carry agrees,
     ignoring slot 5 — the never-rendered PAT mirror — and slot 6, the bank flag):
       revop rows  -> fill TARGET's null fields for that quarter, or add the quarter TARGET lacks
       xbrl_extra  -> add the quarter, or the basis block (s/c), TARGET lacks — whole blocks only, a block
                      is one filing's detail and is never blended with another filing's fields
     TARGET's existing values are never changed. Standalone-PAT rows (fund) are never moved: TARGET already
     holds every quarter, and PAT authority stays with its own rows.
  3. DROP everything else and the OLD key itself. Every removed value is written to
     scripts/bse_alias_collision_retractions.json with its action, so the retraction is reversible.
Idempotent: a second run finds no OLD rows and writes nothing.

Run:  python3 scripts/retract_bse_alias_collision_rows.py [--apply]     (default: dry run, prints the plan)
"""
import gzip, json, os, sys, time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
LEDGER = os.path.join(ROOT, "docs", "bse_alias_collisions.json")
OUT = os.path.join(HERE, "bse_alias_collision_retractions.json")
FUND_STORES = [os.path.join(ROOT, "docs", "sf_fundamentals.json"), os.path.join(HERE, "fundamentals.json")]
REVOP_STORES = [os.path.join(ROOT, "docs", "sf_revop.json"), os.path.join(HERE, "revop_fundamentals.json")]
XTRA = os.path.join(HERE, "xbrl_extra.json.gz")
AGREE_IDX = (0, 1, 2, 3, 4, 7, 8)       # revop slots that must agree; 5 = PAT mirror (never rendered), 6 = fin flag
FILL_IDX = (0, 1, 2, 3, 4, 7, 8)


def _load(p):
    raw = open(p, "rb").read()
    return json.loads(gzip.decompress(raw) if p.endswith(".gz") else raw)


def _save(p, d):
    blob = json.dumps(d, separators=(",", ":")).encode("utf-8")
    open(p, "wb").write(gzip.compress(blob, 9) if p.endswith(".gz") else blob)


def _same(a, b):
    return a is not None and b is not None and abs(float(a) - float(b)) < 1e-9


def agrees(a, b):
    """Every slot in AGREE_IDX that both rows carry is equal, and they share at least one."""
    shared = [i for i in AGREE_IDX if i < len(a) and i < len(b) and a[i] is not None and b[i] is not None]
    return bool(shared) and all(_same(a[i], b[i]) for i in shared)


def main():
    apply = "--apply" in sys.argv
    coll = _load(LEDGER)["collisions"]
    served_rv = _load(REVOP_STORES[0])
    served_fd = _load(FUND_STORES[0])
    bse_px = _load(os.path.join(ROOT, "docs", "bse_fundamentals.json")).get("px", {})
    fund = {p: _load(p) for p in FUND_STORES}
    revop = {p: _load(p) for p in REVOP_STORES}
    xtra = _load(XTRA)
    log = _load(OUT) if os.path.exists(OUT) else {"_README": __doc__.split("\n\n")[0].strip(), "runs": []}
    run = {"at": time.strftime("%Y-%m-%d %H:%M"), "keys": {}}
    changed = set()

    for old, c in sorted(coll.items()):
        tgt = c["target"]
        present = [os.path.relpath(p, ROOT) for p, d in list(fund.items()) + list(revop.items()) if d.get(old)] + \
                  (["scripts/xbrl_extra.json.gz"] if xtra.get(old) else [])
        if not present:
            continue
        # ---- 1. proof, on the served stores ------------------------------------------------------------------
        hits = [q for q, r in (served_rv.get(old) or {}).items() if (served_rv.get(tgt) or {}).get(q)
                and agrees(r, served_rv[tgt][q])]
        tf = {r[0]: r for r in served_fd.get(tgt) or []}
        hits += [str(r[0]) for r in served_fd.get(old) or [] if r[0] in tf and _same(r[1], tf[r[0]][1])]
        own = bse_px.get(str(c["bse_code"])) or {}
        clash = [q for q, r in (served_rv.get(old) or {}).items() if own.get(q) and
                 (_same(own[q].get("rev"), r[0]) or _same(own[q].get("rev"), r[1]))]
        clash += [str(r[0]) for r in served_fd.get(old) or [] if own.get(str(r[0])) and
                  (_same(own[str(r[0])].get("pat"), r[1]) or _same(own[str(r[0])].get("pat"), r[3]))]
        if not hits or clash:
            print("%-10s -> %-10s NOT PROVEN (agree-with-target %d, agree-with-own-BSE-filings %d) — left as is: %s"
                  % (old, tgt, len(set(hits)), len(set(clash)), ", ".join(present)))
            run["keys"][old] = {"target": tgt, "action": "left: not proven to be the target's rows",
                                "agree_target": sorted(set(hits)), "agree_own_bse": sorted(set(clash))}
            continue
        rec = run["keys"].setdefault(old, {"target": tgt, "proof_quarters": sorted(set(hits))[:8],
                                           "proof_count": len(set(hits)), "stores": {}})
        corroborated = {q for q, r in (served_rv.get(old) or {}).items()
                        if (served_rv.get(tgt) or {}).get(q) and agrees(r, served_rv[tgt][q])}
        # ---- 2/3. per store -----------------------------------------------------------------------------------
        for p, d in fund.items():
            rows = d.pop(old, None)
            if rows:
                rec["stores"][os.path.relpath(p, ROOT)] = [{"row": r, "action": "dropped (target holds the quarter)"
                                                            if r[0] in {x[0] for x in d.get(tgt) or []}
                                                            else "dropped (fund rows are never moved)"} for r in rows]
                changed.add(p)
        for p, d in revop.items():
            rows = d.pop(old, None)
            if not rows:
                continue
            out = []
            t = d.setdefault(tgt, {})
            for q, r in sorted(rows.items()):
                cur = t.get(q)
                ref = (served_rv.get(tgt) or {}).get(q)
                if cur is None and ref is not None and agrees(r, ref):
                    t[q] = list(r); out.append({"q": q, "row": r, "action": "moved (target lacked the quarter here; "
                                                                           "agrees with the served target row)"})
                elif cur is not None and agrees(r, cur):
                    filled = [i for i in FILL_IDX if i < len(r) and r[i] is not None and cur[i] is None]
                    if filled:
                        cur = list(cur)
                        for i in filled:
                            cur[i] = r[i]
                        t[q] = cur
                    out.append({"q": q, "row": r, "action": "merged, filled slots %s" % filled if filled
                                else "dropped (duplicate of the target row)"})
                else:
                    out.append({"q": q, "row": r, "action": "dropped (disagrees with the target row %s)" % cur})
            if not t:
                d.pop(tgt, None)
            rec["stores"][os.path.relpath(p, ROOT)] = out
            changed.add(p)
        cells = xtra.pop(old, None)
        if cells:
            out = []
            t = xtra.setdefault(tgt, {})
            for q, cell in sorted(cells.items()):
                if q not in corroborated:
                    out.append({"q": q, "cell": cell, "action": "dropped (no served headline ties this filing to the target)"})
                    continue
                # a basis block is ONE filing's detail: move whole blocks the target lacks, never blend two filings
                tc = t.setdefault(q, {})
                added = [b for b in cell if b not in tc and cell[b]]
                for b in added:
                    tc[b] = cell[b]
                if not tc:
                    t.pop(q)
                out.append({"q": q, "cell": cell, "action": "moved basis %s" % ",".join(added) if added
                            else "dropped (target holds this quarter's %s detail)" % ",".join(sorted(cell))})
            if not t:
                xtra.pop(tgt, None)
            rec["stores"]["scripts/xbrl_extra.json.gz"] = out
            changed.add(XTRA)
        n = {s: len(v) for s, v in rec["stores"].items()}
        mv = sum(1 for v in rec["stores"].values() for e in v if e["action"].startswith(("moved", "merged, filled")))
        print("%-10s -> %-10s proven by %d quarter(s); rows per store %s; %d moved/merged into %s, rest dropped"
              % (old, tgt, rec["proof_count"], n, mv, tgt))

    if not changed:
        print("nothing to retract (already applied)")
        return 0
    if not apply:
        print("DRY RUN — re-run with --apply to write %d store(s) + %s" % (len(changed), os.path.relpath(OUT, ROOT)))
        return 0
    for p in changed:
        _save(p, xtra if p == XTRA else (fund[p] if p in fund else revop[p]))
    log["runs"].append(run)
    with open(OUT, "w", encoding="utf-8") as fh:
        json.dump(log, fh, indent=1, ensure_ascii=False)
        fh.write("\n")
    print("wrote %s + %s" % (", ".join(os.path.relpath(p, ROOT) for p in sorted(changed)), os.path.relpath(OUT, ROOT)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
