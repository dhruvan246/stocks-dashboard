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
cd "${FILL2020_WT:-$HOME/stocks-wt/fill2020}" || exit 1
# worktrees carry no venv of their own -- use the main checkout's interpreter (has fitz/curl_cffi)
PY="${PYTHON:-$HOME/stocks-dashboard/.venv/bin/python}"

CAP=420          # per-invocation wall clock
QSTEP=13         # quarters per invocation (was 7) -- --rescue is OFF below so each gap quarter is
                 # ~2 search windows x <=4 PDFs x 14 pages instead of ~4 windows x <=10 PDFs x 40
                 # pages, cheap enough to widen the slice (2 invocations/company instead of 4,
                 # halving BSE-session-bootstrap overhead) without reintroducing the M&M-class
                 # kill-loses-everything risk a QSTEP=26 single-shot would bring back

# Portable stand-in for GNU `timeout` (absent on this Mac, no Homebrew -- see mac-setup memory).
# SIGTERMs the child after $1 seconds; exit code 124 on timeout, mirroring `timeout`'s convention
# (the caller below checks `-eq 124`), else the child's own exit code.
run_timeout() {
  local cap="$1"; shift
  (
    "$@" &
    pid=$!
    (sleep "$cap"; kill -TERM "$pid" 2>/dev/null) &
    watcher=$!
    wait "$pid" 2>/dev/null
    rc=$?
    kill "$watcher" 2>/dev/null
    wait "$watcher" 2>/dev/null
    [ "$rc" -ge 128 ] && rc=124
    exit "$rc"
  )
}

N=$($PY -X utf8 -c "import json;print(len(json.load(open('scripts/_p3_syms.json'))))")
NQ=$($PY -X utf8 -c "import json;print(len(json.load(open('scripts/_p3_qes.json'))))")
echo "PHASE3 START: $N companies, $NQ quarters, ${QSTEP}q per invocation, ${CAP}s cap"

i=0
while [ "$i" -lt "$N" ]; do
  SYM=$($PY -X utf8 -c "import json;print(json.load(open('scripts/_p3_syms.json'))[$i])")
  TOTAL=0
  j=0
  while [ "$j" -lt "$NQ" ]; do
    QS=$($PY -X utf8 -c "
import json;q=json.load(open('scripts/_p3_qes.json'))[$j:$j+$QSTEP];print(','.join(str(x) for x in q))")
    [ -z "$QS" ] && break
    OUT=$(run_timeout "$CAP" "$PY" -X utf8 scripts/backfill_revop_gaps.py --qe "$QS" \
          --only "$SYM" --fin --retry-skips 2>&1)
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
