@echo off
REM One-click resume of the full revenue-backfill chain (2026-07-22).
REM Order: current-N500 sweep -> ever-member (since-2020) sweep -> sanity nulls -> season rebuild.
cd /d C:\Users\dhruv\stocks-dashboard
echo [1/4] Resuming current-Nifty500 sweep (resumable; already-filled cells skip instantly)...
python -X utf8 scripts\backfill_revop_gaps.py --n500 --all-quarters --fin --retry-skips
echo [2/4] Ever-member (ex-N500 since 2020) sweep...
python -X utf8 scripts\backfill_revop_gaps.py --symfile scripts\_n500_ever_extra.json --all-quarters --fin
echo [3/4] Sanity pass (null scale-spikes / duplicate mis-attributions)...
python -X utf8 scripts\revop_sanity.py
echo [4/4] Rebuilding results_season.json...
python -X utf8 scripts\build_results_season.py
echo ALL DONE - tell Claude to commit and push.
pause
