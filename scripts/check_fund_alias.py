# -*- coding: utf-8 -*-
"""GUARD for the two baked FUND_ALIAS copies drifting away from scripts/_rename_map.json.

FUND_ALIAS maps a DEAD old ticker to the CURRENT one so fundamentals lookups (FUND, SHPD)
resolve for a stock's old-name era — without it every renamed stock returns null profit for
that era and is silently dropped from any profit-based screen. It is baked, byte-identical,
into TWO places (docs/stock-backtest.html carries its OWN inline engine, runbook 39 step 2):

    docs/backtest-engine.js      const FUND_ALIAS = {...};
    docs/stock-backtest.html     const FUND_ALIAS = {...};

_rename_map.json keeps growing (build_sf_data.py regenerates it on a full rebuild; runbook
30 step 4b adds an entry by hand after every rename) and NOTHING kept the two baked copies in
step with it. This script is that missing step.

THE RULE (measured 2026-08-10 against live META, not assumed):
an entry `old -> target` is EXPECTED when all three hold —
  1. target = _rename_map chained to its end (LTI->LTIM->LTM yields LTI->LTM), target != old;
  2. old is NOT alive in META — ABSENT counts, and that is the whole point: once a rename's
     MANUAL_MERGE lands, build_sf_data merges the old series into the new key and the old
     symbol LEAVES the bin entirely. Measured on live META: GUJGASLTD, MIRCELECTR, LYPSAGEMS,
     ZOMATO, LTIM, PVR, SRTRANSFIN are all ABSENT, only TATAMOTORS survives as dead. A rule
     that required `old in META` would miss nearly every real rename;
  3. target IS alive in META — the alias must point somewhere the engine can actually resolve.

Condition 2 is also the REUSED-TICKER guard: a recycled symbol that now belongs to a different
live company is alive in META, so it is never aliased onto the old company's fundamentals.

Reporting is fill-only. Entries baked but not expected (BSES->RELINFRA and ~400 more, where the
TARGET is itself dead) are the older hand-curated layer — harmless, every consumer is
miss-guarded, and they are counted but never flagged or removed. Only two things are reported:
MISSING expected entries, and CONFLICTS where a baked entry points at a different target than
the rename map does.

Run:
    python3 scripts/check_fund_alias.py            # report; exit 1 on drift
    python3 scripts/check_fund_alias.py --write    # apply (union, never removes) + node --check
    SF_BIN=path python3 scripts/check_fund_alias.py    # read META from a bin instead of the index

META source: docs/search_index.json, which build_search_index.py cuts from the same payload meta
the engine loads — verified 2026-08-10 to carry exactly the live universe (4468 rows / 2463 alive
vs sf_meta.json nTot=4468 nDead=2005). SF_BIN reads a bin's meta directly instead.
"""
import json, os, re, subprocess, sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
RENAME_MAP = os.path.join(HERE, "_rename_map.json")
INDEX = os.path.join(ROOT, "docs", "search_index.json")
TARGETS = [os.path.join(ROOT, "docs", "backtest-engine.js"),
           os.path.join(ROOT, "docs", "stock-backtest.html")]

# The constant is one line in both files; the capture must stay anchored so a stray
# `FUND_ALIAS[sym]` lookup elsewhere in the file can never be mistaken for the definition.
CONST_RE = re.compile(r"^const FUND_ALIAS = (\{.*?\});$", re.M)

# A META that lost its alive flags would make rule 3 reject everything and rule 2 accept
# everything. That is not hypothetical: on 2026-08-02 a dead HTML scrape marked EVERY symbol
# alive=False (see the sf-data alive/industry corruption incident). Refuse to judge on it.
MIN_SYMBOLS = 1000
MIN_ALIVE = 500


def _fmt(d):
    """The exact on-disk serialisation of both baked copies (verified byte-identical)."""
    return json.dumps(d, separators=(",", ":"), sort_keys=True)


def load_meta():
    """-> (alive:set, total:int, source:str). SF_BIN reads a bin's meta; else the search index."""
    binpath = os.environ.get("SF_BIN")
    if binpath:
        sys.path.insert(0, HERE)
        from build_search_index import _scan_top_level
        meta = _scan_top_level(binpath, ["meta"])["meta"]
        return ({s for s, m in meta.items() if (m or {}).get("alive")}, len(meta),
                "bin meta %s" % os.path.basename(binpath))
    with open(INDEX, encoding="utf-8") as fh:
        idx = json.load(fh)
    rows = idx["s"]
    return ({r[0] for r in rows if r[2] == 1}, len(rows), "docs/search_index.json")


def resolve(old, rmap):
    """Follow old -> new -> ... to the end of the chain (cycle-safe)."""
    seen, target = {old}, rmap[old]
    while target in rmap and rmap[target] not in seen:
        seen.add(target)
        target = rmap[target]
    return target


def expected_alias(rmap, alive):
    out = {}
    for old in rmap:
        target = resolve(old, rmap)
        if target != old and old not in alive and target in alive:
            out[old] = target
    return out


def read_baked(path):
    with open(path, encoding="utf-8") as fh:
        text = fh.read()
    m = CONST_RE.search(text)
    if not m:
        raise ValueError("no `const FUND_ALIAS = {...};` line in %s" % path)
    return json.loads(m.group(1)), text, m


def audit():
    """-> report dict. Importable so check_feeds.py can surface drift on the health board."""
    with open(RENAME_MAP, encoding="utf-8") as fh:
        rmap = json.load(fh)
    alive, total, source = load_meta()
    rep = {"ok": True, "status": "ok", "detail": "", "meta_source": source,
           "meta_symbols": total, "meta_alive": len(alive), "rename_map": len(rmap),
           "missing": {}, "conflicts": {}, "extra": 0, "baked": 0, "identical": True}

    if total < MIN_SYMBOLS or len(alive) < MIN_ALIVE:
        rep.update(ok=False, status="error",
                   detail="META looks broken (%d symbols, %d alive) — refusing to judge FUND_ALIAS "
                          "against it; check the alive flags before trusting any verdict"
                          % (total, len(alive)))
        return rep

    baked = []
    for path in TARGETS:
        try:
            baked.append((path, read_baked(path)[0]))
        except Exception as e:
            rep.update(ok=False, status="error", detail=str(e)[:200])
            return rep

    if _fmt(baked[0][1]) != _fmt(baked[1][1]):
        rep["identical"] = False
        a, b = baked[0][1], baked[1][1]
        rep.update(ok=False, status="mismatch",
                   detail="the two baked copies differ (%d vs %d entries, %d keys not in both) — "
                          "they must stay byte-identical (runbook 39 step 2)"
                          % (len(a), len(b), len(set(a) ^ set(b))))
        return rep

    cur = baked[0][1]
    exp = expected_alias(rmap, alive)
    rep["baked"] = len(cur)
    rep["expected"] = len(exp)
    rep["missing"] = {o: t for o, t in exp.items() if o not in cur}
    rep["conflicts"] = {o: (cur[o], exp[o]) for o in exp if o in cur and cur[o] != exp[o]}
    rep["extra"] = len(set(cur) - set(exp))
    if rep["missing"] or rep["conflicts"]:
        bits = []
        if rep["missing"]:
            bits.append("%d rename(s) missing from the baked FUND_ALIAS (%s)"
                        % (len(rep["missing"]),
                           ", ".join("%s->%s" % kv for kv in sorted(rep["missing"].items())[:5])
                           + (" ..." if len(rep["missing"]) > 5 else "")))
        if rep["conflicts"]:
            bits.append("%d entry(ies) point at the wrong current name (%s)"
                        % (len(rep["conflicts"]),
                           ", ".join("%s: baked %s, map says %s" % (o, b, e)
                                     for o, (b, e) in sorted(rep["conflicts"].items())[:3])))
        rep.update(ok=False, status="drift",
                   detail="; ".join(bits) + " — fix: python3 scripts/check_fund_alias.py --write")
    return rep


def write(rep):
    """Union the expected set into both copies. Fill-only: never drops a baked entry."""
    cur, _, _ = read_baked(TARGETS[0])
    merged = dict(cur)
    merged.update(rep["missing"])
    for old, (_baked, expected) in rep["conflicts"].items():
        merged[old] = expected
    payload = _fmt(merged)
    for path in TARGETS:
        _, text, m = read_baked(path)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(text[:m.start(1)] + payload + text[m.end(1):])
    js = TARGETS[0]
    subprocess.run(["node", "--check", js], check=True)
    print("wrote %d entries (+%d missing, %d conflicts fixed) to both copies; node --check %s OK"
          % (len(merged), len(rep["missing"]), len(rep["conflicts"]), os.path.basename(js)))
    return merged


def main():
    do_write = "--write" in sys.argv
    rep = audit()
    print("META: %s — %d symbols, %d alive | _rename_map: %d | baked: %d"
          % (rep["meta_source"], rep["meta_symbols"], rep["meta_alive"],
             rep["rename_map"], rep["baked"]))
    if rep["status"] in ("error", "mismatch"):
        print("!! %s: %s" % (rep["status"], rep["detail"]))
        return 1
    print("expected %d | missing %d | conflicts %d | extra (older hand-curated layer, kept) %d"
          % (rep["expected"], len(rep["missing"]), len(rep["conflicts"]), rep["extra"]))
    for old, target in sorted(rep["missing"].items()):
        print("  MISSING   %s -> %s" % (old, target))
    for old, (b, e) in sorted(rep["conflicts"].items()):
        print("  CONFLICT  %s -> baked %s, rename map says %s" % (old, b, e))
    if rep["ok"]:
        print("FUND_ALIAS is in step with _rename_map.json (both copies byte-identical)")
        return 0
    if do_write:
        write(rep)
        return 0
    print("\nfix: python3 scripts/check_fund_alias.py --write")
    return 1


if __name__ == "__main__":
    sys.exit(main())
