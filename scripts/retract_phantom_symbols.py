# -*- coding: utf-8 -*-
"""HTML-ESCAPED PHANTOM SYMBOLS — retract the keys from the fundamentals stores  (2026-08-26)

THE CLASS. `docs/sf_fundamentals.json` and its `scripts/fundamentals.json` twin carry keys that no
exchange ever listed: `M&AMP;M`, `SURANAT&AMP;P`, `IL&AMP;FSENGG`, ... An `&` was HTML-escaped to
`&amp;` in ingestion and then upper-cased with the ticker, so `M&M` got a second, unreadable key.
`merge_escaped_phantom_symbols.py` (2026-08-11) moved what it could into the readable keys but
deliberately left the phantom keys in place — "a separate decision with its own blast radius".
This is that decision.

★ WHY THEY ARE NOT HARMLESS. `M&AMP;M` and `M&AMP;MFIN` are phantoms of two LIVE Nifty names, and a
symbol->exchange-code sweep resolves them happily (Moneycontrol's autosuggest answers for the
unescaped name — `_mc_codes.json` holds BOTH `M&M` and `M&AMP;M` mapped to sc_id `MM`). An ann=0
sweep nearly DATED them on 2026-08-26, which would have made duplicate rows *more* usable to the
backtest engine. The key is the landmine; removing it is the fix.

★ NOTHING IS DELETED UNMEASURED. Every phantom cell is classified against the real symbol first
(following the rename chain — `GET&D`->`GVT&D`, `L&TFH`->`LTF` — because unescaping alone lands on a
dead key), and the verdict for every cell is journalled to scripts/phantom_symbol_retract.json
BEFORE the key is dropped. Four verdicts:

    DUP        phantom slot == the real symbol's slot                     -> nothing to lose
    SUBSET     phantom slot empty where the real symbol has a value       -> nothing to lose
    UNIQUE     phantom has a value the real symbol LACKS                  -> MERGED (fill-only)
    CONTESTED  both hold a value and they DISAGREE                        -> value preserved in the
               ledger, NOT merged (fill-only never overwrites) and NOT adjudicated here

⚠️ CONTESTED IS NOT "STALE JUNK" — MEASURED 2026-08-26. Both sides are filing-sourced: the phantom's
con cells trace to per-filing BSE XBRL in `xbrl_comparative_fills.json` (`..._WEB_2_x` = the
consolidated XBRL of the same filing whose `..._xml` gave the standalone). So the 25 contested cells
are a genuine two-reader dispute in the §108/§109 restated-vintage family, and adjudicating them is a
separate campaign. This tool moves them from an unreadable-but-live key into an explicit journal; it
does not decide them. Six of the 25 (IL&FSENGG) are the known XBRL `owners=0` mis-tag class that
`apply_owners_full.py` already guards against, so their phantom 0.0 is a filer artefact, not a read.

★ THE MERGE USES THE PROJECT'S EXISTING GATE, NOT A NEW ONE. `merge_escaped_phantom_symbols` proves a
phantom is the same company by ITS OWN OVERLAP (>=3 agreeing values AND <15% disagreement across
every revop slot plus both PAT slots) — never by the name resembling one. That module is imported
here so there is ONE gate, not a second opinion that can drift from it. A phantom the gate REFUSES
gets its unique values preserved in the ledger and left unmerged, exactly as that script does.

★ A MERGE ALSO NEEDS AN ANN DATE, AND THE CONVENTION IS MEASURED, NOT INVENTED. `con` is never
stored without `annCon` in this file (measured: 60,810 con cells, zero with annCon null), and 1,954
of the 1,978 already-applied `mc_pat_fills` con cells carry `annCon == annStd` — the same filing.
So a merged con takes the row's own annStd. No annStd, no merge.

★ A HELD CELL ASSERTS ABSENCE. Before merging into any slot this refuses if any ledger claims that
(sym, qe, basis) is `held` — writing there would resurrect a cell CI holds down (runbook §111g).

★★ AND THE LEDGERS HAVE TO MOVE WITH THE DATA. `mc_pat_fills.json` is registered in the BLOCKING
`verify_fills_live.py`, and six of its entries are keyed by a phantom. Drop the payload key while the
ledger still claims those cells and the next CI run reports MISSING and goes red. Those entries are
re-keyed to the real symbol here, and only when the real symbol demonstrably holds the value
(post-merge) — a re-key onto an empty slot would red CI just as surely.

SCOPE. The two fundamentals stores only. `docs/sf_revop.json` carries the same 13 phantom keys and
they still hold 232 values the gate would recover; retracting those needs that merge to run first
and is left open on purpose (see the runbook section). The price bins were scanned and are CLEAN.

Run:  python3 -X utf8 scripts/retract_phantom_symbols.py [--apply]
"""
import html
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(HERE, "fill2020_tools"))
import merge_escaped_phantom_symbols as GATE   # ONE gate, imported — never re-implemented

FUND = os.path.join(ROOT, "docs", "sf_fundamentals.json")
TWIN = os.path.join(HERE, "fundamentals.json")
REVOP = os.path.join(ROOT, "docs", "sf_revop.json")
RENAMES = os.path.join(HERE, "_rename_map.json")
MCPAT = os.path.join(HERE, "mc_pat_fills.json")
XBRLC = os.path.join(HERE, "xbrl_comparative_fills.json")
OUT = os.path.join(HERE, "phantom_symbol_retract.json")

SLOT = {1: "std", 3: "con"}
ANN = {1: 2, 3: 4}


def is_phantom(key):
    """Generic: any key an HTML unescape would change. Catches &AMP;, &amp;, &#38; alike."""
    return html.unescape(key) != key


def resolve(key, stores, renames):
    """Phantom key -> the key that actually holds the company's data, following renames."""
    cur = html.unescape(key)
    chain = [cur]
    for _ in range(6):
        if any(cur in s and s[cur] for s in stores):
            return cur, chain
        nxt = renames.get(cur)
        if not nxt or nxt in chain:
            break
        chain.append(nxt)
        cur = nxt
    return (cur if any(cur in s and s[cur] for s in stores) else None), chain


def held_cells():
    """{(SYM, qe, basis)} that some ledger asserts ABSENT. Writing there resurrects a held cell."""
    out = set()
    for fn in os.listdir(HERE):
        if not fn.endswith(".json"):
            continue
        try:
            d = json.load(open(os.path.join(HERE, fn)))
        except Exception:
            continue
        if not isinstance(d, dict):
            continue
        for k, v in d.items():
            if not isinstance(k, str) or not isinstance(v, dict) or not v.get("held"):
                continue
            p = k.split("|")
            if len(p) < 2:
                continue
            try:
                qe = int(p[1])
            except ValueError:
                continue
            out.add((p[0], qe, p[2] if len(p) > 2 else None))
    return out


def main():
    apply_it = "--apply" in sys.argv
    fund = json.load(open(FUND))
    twin = json.load(open(TWIN))
    revop = json.load(open(REVOP))
    renames = json.load(open(RENAMES))
    mcpat = json.load(open(MCPAT))
    xbrlc = json.load(open(XBRLC))
    holds = held_cells()

    phantoms = sorted({k for st in (fund, twin) for k in st if is_phantom(k)})
    print("phantom keys in the fundamentals stores: %d" % len(phantoms))

    # ---- the gate, computed by the SHARED module's rule over revop + both PAT slots -------------
    fmap = {s: {r[0]: r for r in rows} for s, rows in fund.items()}
    verdicts = {}
    for ph in phantoms:
        target, chain = resolve(ph, (fund, twin, revop), renames)
        agree = dis = 0
        if target:
            for q, r in (revop.get(ph) or {}).items():
                trow = (revop.get(target) or {}).get(q)
                for i, pv in enumerate(r):
                    if pv is None:
                        continue
                    tv = trow[i] if trow and len(trow) > i else None
                    if tv is None:
                        continue
                    agree, dis = (agree + 1, dis) if GATE.close(tv, pv) else (agree, dis + 1)
            for r in fund.get(ph, []):
                trow = (fmap.get(target) or {}).get(r[0])
                for i in (1, 3):
                    pv = r[i]
                    if pv is None:
                        continue
                    tv = trow[i] if trow else None
                    if tv is None:
                        continue
                    agree, dis = (agree + 1, dis) if GATE.close(tv, pv) else (agree, dis + 1)
        rate = dis / float(agree + dis) if (agree + dis) else 1.0
        verdicts[ph] = (target, chain, agree, dis, rate,
                        agree >= GATE.MIN_AGREE and rate < GATE.MAX_DIS_RATE)

    # ★ THE JOURNAL IS APPEND-ONLY. A second --apply run finds zero phantoms, so `ledger` is empty --
    # dumping it would erase the record of the first run, which is the ONLY place the retracted values
    # still exist. Caught by an idempotence re-run on 2026-08-26 (it wrote `{}` over 239 cells).
    ledger = json.load(open(OUT)) if os.path.exists(OUT) else {}
    merges, counts = [], {"DUP": 0, "SUBSET": 0, "UNIQUE": 0, "CONTESTED": 0}
    for ph in phantoms:
        target, chain, agree, dis, rate, ok = verdicts[ph]
        print("  %-16s -> %-12s agree %3d disagree %2d (%4.1f%%)  gate=%s"
              % (ph, target or "(NO TARGET)", agree, dis, 100 * rate, "PASS" if ok else "REFUSED"))
        for store_name, store in (("docs/sf_fundamentals.json", fund), ("scripts/fundamentals.json", twin)):
            tmap = {r[0]: r for r in store.get(target, [])} if target else {}
            for r in store.get(ph, []):
                qe = r[0]
                trow = tmap.get(qe)
                for i in (1, 3):
                    pv, tv = r[i], (trow[i] if trow else None)
                    if pv is None and tv is None:
                        continue
                    key = "%s|%d|%s|%s" % (ph, qe, SLOT[i], store_name.split("/")[0])
                    if pv is None:
                        verdict = "SUBSET"
                    elif tv is None:
                        verdict = "UNIQUE"
                    elif GATE.close(tv, pv):
                        verdict = "DUP"
                    else:
                        verdict = "CONTESTED"
                    counts[verdict] += 1
                    entry = {"verdict": verdict, "phantom": ph, "real": target, "qe": qe,
                             "basis": SLOT[i], "store": store_name,
                             "phantom_value": pv, "real_value": tv}
                    if chain[1:]:
                        entry["rename_chain"] = chain
                    prov = (xbrlc.get("%s|%d" % (ph, qe)) or {}).get("pat_%s" % SLOT[i])
                    if prov:
                        entry["phantom_source"] = prov.get("src")
                    if verdict == "UNIQUE":
                        ai = ANN[i]
                        ann = trow[ANN[1]] if trow else None
                        blocked = None
                        if not ok:
                            blocked = ("phantom-to-target agreement too weak to trust its unique "
                                       "values: %d agree, %d disagree (%.1f%%)" % (agree, dis, 100 * rate))
                        elif (target, qe, SLOT[i]) in holds:
                            blocked = "a ledger HOLDS this cell absent — merging would resurrect it"
                        elif not ann:
                            blocked = "no annStd on the target row; con is never stored without annCon"
                        if blocked:
                            entry["action"] = "PRESERVED-NOT-MERGED"
                            entry["why"] = blocked
                        else:
                            entry["action"] = "MERGED"
                            entry["ann_written"] = ann
                            entry["why"] = ("value absent from %s; phantom proved same company by "
                                            "its own overlap (%d agree, %d disagree, %.1f%% < %.0f%%); "
                                            "annCon takes the row's own annStd (the filing date)"
                                            % (target, agree, dis, 100 * rate, 100 * GATE.MAX_DIS_RATE))
                            merges.append((store, target, qe, i, ai, pv, ann, store_name))
                    else:
                        entry["action"] = "RETRACTED"
                    ledger[key] = entry

    print("\ncell verdicts across both stores: %s" % counts)
    print("fill-only merges into the real symbol: %d" % len(merges))
    for _s, t, qe, i, _ai, pv, ann, sn in merges:
        print("   MERGE %-12s %d %-3s = %-8s ann=%s   [%s]" % (t, qe, SLOT[i], pv, ann, sn))

    # ---- ledger re-key: mc_pat_fills is BLOCKING; it must not outlive the key it points at ------
    rekeys, rekey_blocked = [], []
    merged_now = {(t, qe, SLOT[i]) for _s, t, qe, i, _ai, _pv, _a, sn in merges
                  if sn == "docs/sf_fundamentals.json"}
    for k in sorted(mcpat):
        p = k.split("|")
        if len(p) != 3 or not is_phantom(p[0]):
            continue
        target = verdicts.get(p[0], (None,))[0]
        if not target:
            rekey_blocked.append((k, "no target")); continue
        qe, basis = int(p[1]), p[2]
        row = (fmap.get(target) or {}).get(qe)
        i = 3 if basis == "con" else 1
        live = row[i] if row else None
        if live is None and (target, qe, basis) not in merged_now:
            rekey_blocked.append((k, "target slot is empty — a re-key here would report MISSING"))
            continue
        rekeys.append((k, "%s|%d|%s" % (target, qe, basis)))
    print("\nmc_pat_fills.json re-keys (BLOCKING ledger): %d" % len(rekeys))
    for a, b in rekeys:
        print("   %-28s -> %s" % (a, b))
    for a, why in rekey_blocked:
        print("   !! LEFT AS-IS %-24s (%s)" % (a, why))

    if not phantoms:
        print("\nnothing to do — no phantom keys in either fundamentals store. "
              "Journal left untouched (%d cells from earlier runs)." % len(ledger))
        return
    if not apply_it:
        print("\n(dry run — re-run with --apply)")
        return

    for store, target, qe, i, ai, pv, ann, _sn in merges:
        for r in store[target]:
            if r[0] == qe and r[i] is None:
                r[i] = pv
                if r[ai] is None:
                    r[ai] = ann
    for k, nk in rekeys:
        mcpat[nk] = dict(mcpat.pop(k), rekeyed_from=k,
                         rekeyed_why="phantom key retracted from the fundamentals stores 2026-08-26")
    for ph in phantoms:
        fund.pop(ph, None)
        twin.pop(ph, None)

    json.dump(fund, open(FUND, "w"), separators=(",", ":"))
    json.dump(twin, open(TWIN, "w"), separators=(",", ":"))
    json.dump(mcpat, open(MCPAT, "w"), indent=1, sort_keys=True)
    json.dump(ledger, open(OUT, "w"), indent=1, sort_keys=True)
    print("\nAPPLIED — %d phantom keys retracted, %d cells journalled, %d merged, %d ledger re-keys"
          % (len(phantoms), len(ledger), len(merges), len(rekeys)))


if __name__ == "__main__":
    main()
