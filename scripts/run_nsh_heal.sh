#!/bin/bash
# NSH HEAL RUNNER — restore missing shareholder counts, staged, dry by default.
#
# Everything here is deliberately re-derived AT RUN TIME rather than read from a plan file:
# another session is writing scripts/shp_history.json today, so a symbol list computed an hour
# ago may already be wrong. Merging against a stale copy is how the July tangle happened.
#
#   bash scripts/run_nsh_heal.sh            # fetch + stage + DRY RUN (writes nothing to history)
#   bash scripts/run_nsh_heal.sh --apply    # same, then commit the fills
#
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(dirname "$HERE")"
STAGE="$HERE/shp_history_stage.json"
WORK="${NSH_WORK:-/private/tmp/claude-501/-Users-dhruvan-stocks-dashboard/a1d1d1ad-a4f0-49a1-8453-a0bb20fa7f58/scratchpad/shpverify/p6}"
mkdir -p "$WORK"
APPLY=""; MERGE_ONLY=""
for arg in "$@"; do
  [ "$arg" = "--apply" ] && APPLY="--apply"
  [ "$arg" = "--merge-only" ] && MERGE_ONLY=1
done

cd "$REPO" || exit 1
echo "== 1. sync + concurrency check =================================================="
git fetch origin -q
BLOB=$(git rev-parse --short origin/main:scripts/shp_history.json)
echo "   origin shp_history blob : $BLOB   (was b2bed157 when this heal was planned)"
[ "$BLOB" != "b2bed157" ] && echo "   ⚠ THE FILE MOVED — the other session pushed. Lists are re-derived below, so this is"
[ "$BLOB" != "b2bed157" ] && echo "     safe, but read their message before applying."
git checkout -q "$(git rev-parse origin/main)" -- scripts/shp_history.json 2>/dev/null || true
echo "   worktree history refreshed to current origin"

echo "== 2. re-derive which cells still lack a count (against CURRENT origin) ========="
python3 - "$WORK" "$REPO" <<'PY'
import json, sys, os, collections
work, repo = sys.argv[1], sys.argv[2]
H = json.load(open(os.path.join(repo, "scripts", "shp_history.json"), encoding="utf-8"))
plan = collections.defaultdict(list)
for sym, d in H.items():
    if sym.startswith("_"):
        continue
    for qe, c in d.items():
        if qe >= "2016-06-30" and not (len(c) > 6 and c[6]):
            plan[qe].append(sym)
plan = {q: sorted(v) for q, v in sorted(plan.items())}
json.dump(plan, open(os.path.join(work, "plan.json"), "w"), indent=0)
tot = sum(len(v) for v in plan.values())
print("   %d quarters, %d symbol-fetches" % (len(plan), tot))
for q, v in plan.items():
    if len(v) >= 100:
        print("      %s  %5d" % (q, len(v)))
PY

# --merge-only reuses an existing, already-validated staging file. Without it, --apply refetches
# all ~5k filings just to reproduce a stage that was already built and dry-run checked — 45 wasted
# minutes and 5k needless requests to NSE. Dry-run first, then apply with --merge-only.
if [ -n "$MERGE_ONLY" ]; then
  echo "== 3. staged reparse SKIPPED (--merge-only; reusing $STAGE) ====================="
  [ -s "$STAGE" ] || { echo "   ! no staging file — run without --merge-only first"; exit 1; }
  echo "   stage: $(wc -c < "$STAGE" | tr -d ' ') bytes"
else
echo "== 3. staged reparse (per quarter, only the symbols that need it) ==============="
rm -f "$STAGE"
python3 - "$WORK" "$HERE" <<'PY'
import json, os, sys, subprocess
work, here = sys.argv[1], sys.argv[2]
plan = json.load(open(os.path.join(work, "plan.json")))
stage = os.path.join(here, "shp_history_stage.json")
for i, (qe, syms) in enumerate(sorted(plan.items()), 1):
    if not syms:
        continue
    print("   [%d/%d] %s  (%d symbols)" % (i, len(plan), qe, len(syms)), flush=True)
    cmd = ["python3", "-X", "utf8", os.path.join(here, "fetch_shareholding.py"),
           "--reparse", "--quarters", qe, "--symbols", ",".join(syms), "--hist", stage]
    r = subprocess.run(cmd, capture_output=True, text=True)
    tail = [l for l in (r.stdout or "").splitlines() if "cells" in l or "filings" in l]
    for l in tail[-2:]:
        print("        " + l.strip())
    if r.returncode:
        print("        ! exit %d — %s" % (r.returncode, (r.stderr or "")[-200:]))
PY

fi

echo "== 4. merge (slot 6 only; refuses any cell whose percentages moved) ============="
python3 -X utf8 "$HERE/_shp_merge_nsh.py" --stage "$STAGE" $APPLY | tee "$WORK/merge_report.txt"
echo
echo "report: $WORK/merge_report.txt"
[ -z "$APPLY" ] && echo "DRY RUN — nothing written. Re-run with --apply once the numbers look right."
