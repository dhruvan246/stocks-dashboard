# -*- coding: utf-8 -*-
"""Three-way merge so a CI refresh cannot clobber fills that landed while it was running.

THE BUG THIS FIXES (observed 2026-08-06). refresh-fundamentals.yml snapshots the payload files at
the start of its commit step, then inside its push-retry loop does:

    git fetch origin main && git reset --hard origin/main
    cp /tmp/sf_revop.json docs/sf_revop.json          # <-- blind restore of a STALE snapshot
    git add ... && git commit && git push

The reset correctly picks up whatever other writers landed, and the `cp` immediately throws it
away. A backfill session pushed 193 consolidated-revenue cells at ~12:5x; the 13:02 refresh had
snapshotted before that and silently reverted every one of them. The values survived only because
scripts/revop_fundamentals.json (the ledger) is deliberately NOT committed from CI.

WHY NOT A PLAIN FILL-ONLY MERGE. "Copy any non-null from the snapshot, keep origin otherwise" would
preserve concurrent fills but would also RESURRECT cells that CI deliberately nulled --
revop_sanity.py exists precisely to strip junk (a wrong-scale fill, a duplicate), and those nulls
must stick. Fill-only cannot tell "CI removed this" from "CI never had it".

THE THREE-WAY MERGE. With
    base   = the file as checked out at job start (before this job touched it)
    ours   = the file this job rebuilt
    theirs = the file currently on origin (after the reset)
decide per SLOT:
    ours != base  ->  this job changed the value on purpose  ->  take OURS (nulls included)
    ours == base  ->  this job did not touch it              ->  take THEIRS (concurrent fill survives)

That keeps every deliberate CI change, including deletions, while never discarding another writer's
work in a cell CI had no opinion about.

DELETIONS BY ANOTHER WRITER (2026-11-18, runbook §214b batch 3 / §199 open item 3). A whole ROW (quarter) or a
whole SYMBOL KEY that BASE held and origin no longer holds was removed by another writer while this job ran
(a session dropping a phantom quarter with a `held` journal, a §30 rename merge deleting the old ticker). The
old rule "origin lacks the row -> CI's row wins" put it straight back: the 14:46 IST refresh of 2026-11-18,
built on 1ee1184de, re-added the three MIIL phantom quarters ff9b773818 had dropped 7 minutes earlier, and
verify_fills_live then reported them RESURRECTED. Now such a row / key STAYS REMOVED (origin's state wins;
the next run starts from origin and, if its sources still produce the row, adds it as a new row). A row BASE
did not hold is still a CI addition and lands as before. Safety valve: if a run would drop more than
MAX_DROP_KEYS keys or MAX_DROP_ROWS rows, origin looks truncated rather than edited, so the merge keeps the
old union behaviour for that file and says so.

Handles both payload shapes:
    sf_revop.json        {sym: {qe: [9 slots]}}
    sf_fundamentals.json {sym: [[qe, stdPAT, stdAnn, conPAT, conAnn], ...]}

Usage:  python3 scripts/ci_preserve_merge.py <base> <ours> <theirs> <out>
Exit 0 always writes <out>; on any unexpected shape it falls back to OURS (previous behaviour), so
a surprise can never make the refresh worse than it was.
"""
import copy
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fund_dup_guard


def _rows_to_map(rows):
    """qe -> row. NOTE this is LAST-wins, while every consumer of sf_fundamentals reads the FIRST
    match (`next((r for r in rows if r[0] == qe), None)`). On a store carrying two rows for one
    quarter the two disagree, so the merge would write into a row nobody reads -- which is why
    main() deduplicates every input before merging (see scripts/fund_dup_guard.py)."""
    return {r[0]: r for r in rows if isinstance(r, list) and r}


def _predup(name, obj):
    """Merge conflict-free duplicate quarters out of a listrows payload, loudly. Never raises:
    this script's contract is that a surprise can never make the refresh worse than it was."""
    try:
        n, conflicts = fund_dup_guard.dedup(obj)
    except Exception as ex:                       # pragma: no cover - defensive, see contract above
        print("ci_preserve_merge: dedup skipped for %s (%s: %s)" % (name, type(ex).__name__, ex))
        return obj
    if n or conflicts:
        print("ci_preserve_merge: %s carried duplicate quarters -- %d merged, %d left as value "
              "conflicts%s" % (name, n, len(conflicts),
                               "".join("\n    CONFLICT %s %s %s" % (c["sym"], c["qe"], c["conflicts"])
                                       for c in conflicts)))
    return obj


MAX_DROP_KEYS = 25      # more than this many symbol keys removed on origin mid-run = truncation, not an edit
MAX_DROP_ROWS = 1500    # likewise for rows (a §30 rename merge removes one key of ~100 rows)
DROPPED = []            # (sym, qe or None) removed on origin while this job ran, kept removed; reported by main()


def _origin_removed(base, theirs, sym, qe=None):
    """True when BASE held this key/row and origin (THEIRS) no longer does: another writer deleted it."""
    if qe is None:
        return sym in base and sym not in theirs
    return sym in theirs and qe in base and qe not in theirs


def merge_listrows(base, ours, theirs, respect_removals=True):
    """sf_fundamentals shape: {sym: [[qe, ...], ...]}"""
    out = copy.deepcopy(theirs)
    changed = kept = 0
    for sym, orows in ours.items():
        if respect_removals and _origin_removed(base, theirs, sym):
            DROPPED.append((sym, None))            # whole key removed on origin mid-run -> stays removed
            continue
        om, bm = _rows_to_map(orows), _rows_to_map(base.get(sym, []))
        tm = _rows_to_map(out.get(sym, []))
        for qe, orow in om.items():
            brow, trow = bm.get(qe), tm.get(qe)
            if trow is None and brow is not None and sym in theirs and respect_removals:
                DROPPED.append((sym, qe))          # row removed on origin mid-run -> stays removed
                continue
            if trow is None:                       # a row this run ADDED (BASE lacked it) -> CI's row wins
                out.setdefault(sym, []).append(list(orow))
                changed += 1
                continue
            n = max(len(orow), len(trow))
            while len(trow) < n:
                trow.append(None)
            for i in range(1, min(n, len(orow))):
                bv = brow[i] if brow and i < len(brow) else None
                if orow[i] != bv:                  # CI changed this slot on purpose
                    if trow[i] != orow[i]:
                        trow[i] = orow[i]
                        changed += 1
                else:                              # CI untouched -> keep whatever origin has
                    kept += 1
    return out, changed, kept


def merge_qmaps(base, ours, theirs, respect_removals=True):
    """sf_revop shape: {sym: {qe: [slots]}}"""
    out = copy.deepcopy(theirs)
    changed = kept = 0
    for sym, qm in ours.items():
        if respect_removals and _origin_removed(base, theirs, sym):
            DROPPED.append((sym, None))
            continue
        for qe, orow in qm.items():
            brow = (base.get(sym) or {}).get(qe)
            trow = (out.get(sym) or {}).get(qe)
            if trow is None and brow is not None and sym in theirs and respect_removals:
                DROPPED.append((sym, qe))
                continue
            if trow is None:
                out.setdefault(sym, {})[qe] = list(orow)
                changed += 1
                continue
            n = max(len(orow), len(trow))
            while len(trow) < n:
                trow.append(None)
            for i in range(min(n, len(orow))):
                bv = brow[i] if brow and i < len(brow) else None
                if orow[i] != bv:
                    if trow[i] != orow[i]:
                        trow[i] = orow[i]
                        changed += 1
                else:
                    kept += 1
            out[sym][qe] = trow
    return out, changed, kept


def main():
    base_p, ours_p, theirs_p, out_p = sys.argv[1:5]
    ours = json.load(open(ours_p))
    try:
        base = json.load(open(base_p))
    except Exception:
        base = {}
    try:
        theirs = json.load(open(theirs_p))
    except Exception:
        theirs = {}
    try:
        sample = next(iter(ours.values())) if ours else None
        if isinstance(sample, dict):
            merge = merge_qmaps
        elif isinstance(sample, list):
            base = _predup("base", base)
            ours = _predup("ours", ours)
            theirs = _predup("theirs", theirs)
            merge = merge_listrows
        else:
            raise ValueError("unrecognised payload shape")
        respect = bool(theirs) and bool(base)     # an unreadable origin/base copy: no removal can be inferred
        out, ch, kept = merge(base, ours, theirs, respect)
        nk = sum(1 for _, q in DROPPED if q is None); nr = len(DROPPED) - nk
        if nk > MAX_DROP_KEYS or nr > MAX_DROP_ROWS:
            print("::warning::ci_preserve_merge %s: origin lacks %d keys / %d rows this run's base held -- looks "
                  "truncated, not edited; keeping the old union behaviour for this file" % (out_p.split("/")[-1], nk, nr))
            del DROPPED[:]
            out, ch, kept = merge(base, ours, theirs, False)
        elif DROPPED:
            print("ci_preserve_merge %s: %d key(s) / %d row(s) removed on origin while this job ran stay removed: %s%s"
                  % (out_p.split("/")[-1], nk, nr, ", ".join("%s%s" % (s_, "" if q is None else "|%s" % q) for s_, q in DROPPED[:20]),
                     " ..." if len(DROPPED) > 20 else ""))
        if merge is merge_listrows:
            out = _predup("merged", out)
    except Exception as ex:
        # Never make the refresh worse than the old blind copy: fall back to OURS.
        print("ci_preserve_merge: FALLBACK to ours (%s: %s)" % (type(ex).__name__, ex))
        json.dump(ours, open(out_p, "w"), separators=(",", ":"))
        return
    json.dump(out, open(out_p, "w"), separators=(",", ":"))
    print("ci_preserve_merge %s: %d slots from this run, %d left as origin had them"
          % (out_p.split("/")[-1], ch, kept))


if __name__ == "__main__":
    main()
