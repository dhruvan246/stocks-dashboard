#!/bin/bash
# Full Strategy Phases Lab refresh, end to end (DATA_RUNBOOK §7.4).
#   ./scripts/gridmega_phases_all.sh [wantEnd] [jobs]
# Waits for the day's sf-data refresh, stages _live ONCE, purges every artifact derived from the
# previous staging, then runs all 11 windows × 5 basket variants and builds the 5 page JSONs.
set -uo pipefail
cd "$(dirname "$0")/.."
WANT="${1:-}"
JOBS="${2:-4}"

sfend() { curl -s "https://dhruvan246.github.io/sf-data/sf_meta.json?t=$(date +%s)" \
          | python3 -c "import json,sys;print(json.load(sys.stdin)['end'])" 2>/dev/null; }

if [ -n "$WANT" ]; then
  echo "$(date +'%H:%M') waiting for sf-data end=$WANT (cron 20:45 IST, best-effort)…"
  for _ in $(seq 1 75); do
    E=$(sfend); [ "$E" = "$WANT" ] && break
    echo "$(date +'%H:%M')   still $E"; sleep 60
  done
fi
# FORCE_END pins the window end regardless of what the live feed has reached, for when you want
# results now rather than after the day's publish. The stager still takes whatever is live, so all
# 11 windows share one snapshot; a later bar simply goes unused by a window that ends before it.
END="${FORCE_END:-$(sfend)}"
echo "$(date +'%H:%M') proceeding with window end=$END (live feed at $(sfend))"

# One staging for every window. Everything derived from the OLD staging must go, above all the
# factor caches — they are keyed by window+universe, not by data revision, so a stale cache would
# silently feed yesterday's factor values into today's grid.
python3 scripts/gridmega_fetch_live.py || exit 1
rm -f scripts/_gridmega_cache_*.json.gz scripts/_gridmega_all_*.csv.gz \
      scripts/_gridmega_top_*.json scripts/_gridmega_sel_*.json scripts/_gridmega_selidx*.json
echo "$(date +'%H:%M') artifacts purged; starting grids"

python3 scripts/gridmega_phases_run.py "$END" --jobs "$JOBS" || exit 1

for V in "" _h5 _r3 _h3 _fno_h3; do
  echo "=== building strategy_phases${V}.json ==="
  GRID_END="$END" node --max-old-space-size=6144 scripts/gridmega_phases_build.js "$V" || exit 1
done
echo "$(date +'%H:%M') ALL DONE (data end $END)"
