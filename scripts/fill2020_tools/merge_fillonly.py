"""Fill-only semantic merge of my sf_revop / revop_fundamentals work onto a newer origin/main.

A textual rebase of these minified single-line JSONs always conflicts because CI rewrites them every
few minutes. The MEANING of this change is "fill cells that were empty", so the correct merge is:
start from origin's (newer) file and copy in only the cells where mine has a value and origin does
not. CI's newer values always win; nothing of theirs is overwritten.

argv: <mine.json> <theirs.json> <out.json>
"""
import json
import sys

mine_p, theirs_p, out_p = sys.argv[1:4]
mine = json.load(open(mine_p, encoding="utf-8"))
theirs = json.load(open(theirs_p, encoding="utf-8"))

added = skipped_theirs_has = new_rows = 0
for sym, qmap in mine.items():
    tgt = theirs.setdefault(sym, {})
    for qe, row in qmap.items():
        trow = tgt.get(qe)
        if trow is None:
            tgt[qe] = list(row)
            new_rows += 1
            added += sum(1 for v in row if v is not None)
            continue
        for i, val in enumerate(row):
            if val is None:
                continue
            if i >= len(trow):
                continue
            if trow[i] is None:
                trow[i] = val
                added += 1
            elif trow[i] != val:
                skipped_theirs_has += 1

json.dump(theirs, open(out_p, "w", encoding="utf-8"), separators=(",", ":"))
print(f"merged -> {out_p}: {added} cells added, {new_rows} new rows, "
      f"{skipped_theirs_has} left as origin had them (origin wins)")
