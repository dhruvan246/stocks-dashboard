# -*- coding: utf-8 -*-
"""Bake docs/liquid_universe.json — the turnover universe behind the Season Trends default view.

WHY THIS EXISTS: build_results_season.py runs in refresh-fundamentals.yml (every 30 min), which has
no fresh price bin — only the COMMITTED docs/sf_stock_data.bin, and that one is frozen on purpose
(refresh-market-mood overwrites it in the runner but never commits it; the real bin is ~193 MB, past
GitHub's 100 MB file cap, so it can never be committed fresh). So the season chart was picking its
default universe from a snapshot that stopped advancing on 2026-06-13 while prices ran to 2026-08-07
— 4.8% of the universe wrong (41 newly-liquid names missing, 28 gone-illiquid still counted), drifting
further every day. Downloading the 193 MB release asset 48×/day to fix that is absurd; this sidecar is
~20 KB and rides the once-daily job that already HAS the fresh bin (refresh-backtest-data.yml, right
after the append step).

Out: docs/liquid_universe.json = {asOf, floorCr, window, symbols:[...]}
Run:  python3 -X utf8 scripts/bake_liquid_universe.py          # uses docs/sf_stock_data.bin
      SF_BIN=/tmp/sf_stock_data.bin python3 -X utf8 scripts/bake_liquid_universe.py
"""
import os, sys, json

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from build_results_season import scan_bin_universe, load_rename, TURN_FLOOR_CR, TURN_WINDOW, BIN, LIQ


def main():
    src = os.environ.get("SF_BIN") or BIN
    U, end = scan_bin_universe(src, load_rename())
    if not U:
        print("REFUSING to write an empty universe (src=%s)" % src)
        return 1
    if not end:
        print("REFUSING to write a universe with no `end` date (src=%s)" % src)
        return 1
    json.dump({"asOf": end, "floorCr": TURN_FLOOR_CR, "window": TURN_WINDOW,
               "symbols": sorted(U)}, open(LIQ, "w"), separators=(",", ":"))
    print("Wrote %s: %d symbols, asOf=%s (src=%s)" % (LIQ, len(U), end, src))
    return 0


if __name__ == "__main__":
    sys.exit(main())
