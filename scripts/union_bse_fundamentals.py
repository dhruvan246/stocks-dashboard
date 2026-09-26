# -*- coding: utf-8 -*-
"""Cell-level UNION of a job's docs/bse_fundamentals.json into the current one (runbook §181b).

refresh-bse.yml used to `cp` its whole file over origin's at commit time, silently erasing every quarter another writer
(bse-results-xbrl.yml, the bse-fund-history merges, vision merges) landed while it ran. This merges instead:
cells only the job has are ADDED; where both hold a cell the CURRENT (origin) one wins — the job's own fetch is
fill-only, so a differing cell means someone else changed it after the job checked out.

Run: python3 scripts/union_bse_fundamentals.py <job_copy.json> [target=docs/bse_fundamentals.json]
"""
import json, os, sys

HERE = os.path.dirname(os.path.abspath(__file__))


def main():
    mine_p = sys.argv[1]
    tgt_p = sys.argv[2] if len(sys.argv) > 2 else os.path.join(HERE, "..", "docs", "bse_fundamentals.json")
    mine = json.load(open(mine_p, encoding="utf-8"))
    try:
        cur = json.load(open(tgt_p, encoding="utf-8"))
    except (OSError, ValueError):
        cur = {"px": {}}
    px = cur.setdefault("px", {})
    added = kept = 0
    for code, qmap in (mine.get("px") or {}).items():
        dst = px.setdefault(code, {})
        for qe, cell in qmap.items():
            if qe in dst:
                kept += dst[qe] != cell
                continue
            dst[qe] = cell; added += 1
    if mine.get("updated") and mine["updated"] > (cur.get("updated") or ""):
        cur["updated"] = mine["updated"]
    json.dump(cur, open(tgt_p, "w", encoding="utf-8"), ensure_ascii=False, separators=(",", ":"))
    print("union_bse_fundamentals: +%d cells from the job; %d differing cells kept as current" % (added, kept))


if __name__ == "__main__":
    main()
