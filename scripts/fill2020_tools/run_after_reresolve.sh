#!/bin/zsh
# Sequential re-sweep after the resolver fix recovered 59 symbols (commit b1b07ef0).
# SEQUENTIAL ON PURPOSE: revenue then PAT, never both at once — one client on the endpoint is the
# rate discipline the whole Moneycontrol route depends on. Already-ledgered cells are skipped and
# already-cached series cost no request, so the marginal work is the 59 newly-resolved symbols.
set -u
cd "$(dirname "$0")/../.." || exit 1
echo "=== REVENUE (mc_fill_all_history) $(date +'%H:%M:%S') ==="
python3 -X utf8 scripts/fill2020_tools/mc_fill_all_history.py
echo
echo "=== PAT (mc_pat_fetch) $(date +'%H:%M:%S') ==="
python3 -X utf8 scripts/fill2020_tools/mc_pat_fetch.py
echo
echo "=== DONE $(date +'%H:%M:%S') ==="
