#!/bin/bash
# FILL-2020 Phase 3 -- per-company, per-quarter-batch fetch grind.
#
# WHY BATCHED BY QUARTER: backfill_revop_gaps writes its JSONs only when the whole invocation
# finishes. A company with many gap quarters can exceed any sane wall-clock cap, and being killed
# mid-run banks NOTHING -- M&M burned 900s across 26 quarters and left the tree clean. So each
# invocation covers a SMALL slice of quarters (QSTEP), which reliably completes and saves. A company
# with 26 open quarters becomes ~4 short invocations instead of one that never lands.
# The tool is fill-only, so re-running a slice is always safe.
set -u
cd C:/Users/dhruv/stocks-wt/fill2020 || exit 1

CAP=420          # per-invocation wall clock
QSTEP=7          # quarters per invocation

N=$(python -X utf8 -c "import json;print(len(json.load(open('scripts/_p3_syms.json'))))")
NQ=$(python -X utf8 -c "import json;print(len(json.load(open('scripts/_p3_qes.json'))))")
echo "PHASE3 START: $N companies, $NQ quarters, ${QSTEP}q per invocation, ${CAP}s cap"

i=0
while [ "$i" -lt "$N" ]; do
  SYM=$(python -X utf8 -c "import json;print(json.load(open('scripts/_p3_syms.json'))[$i])")
  TOTAL=0
  j=0
  while [ "$j" -lt "$NQ" ]; do
    QS=$(python -X utf8 -c "
import json;q=json.load(open('scripts/_p3_qes.json'))[$j:$j+$QSTEP];print(','.join(str(x) for x in q))")
    [ -z "$QS" ] && break
    OUT=$(timeout "$CAP" python -X utf8 scripts/backfill_revop_gaps.py --qe "$QS" \
          --only "$SYM" --fin --retry-skips --rescue 2>&1)
    rc=$?
    got=$(printf '%s' "$OUT" | grep -oE "^DONE: filled [0-9]+" | grep -oE "[0-9]+$")
    [ -n "$got" ] && TOTAL=$((TOTAL + got))
    [ "$rc" -eq 124 ] && echo "    [$i] $SYM q@$j capped"
    j=$((j + QSTEP))
  done
  echo "[$i/$N] $SYM :: filled $TOTAL cells"
  i=$((i + 1))
done
echo "PHASE3 SWEEP COMPLETE"
