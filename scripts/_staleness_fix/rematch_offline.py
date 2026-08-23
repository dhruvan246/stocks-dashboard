#!/usr/bin/env python3
"""Re-run match_targets over the CACHED raw rows (v3_raw*.jsonl) — no BSE traffic.
This is what PLAN F4 bought: a classifier tweak re-matches 2,399 symbols in seconds instead of
a 90-minute crawl. Preserves each symbol's scripcode/error metadata from fetch_results.json and
replaces only its matches. Run after ANY change to classify.py, then re-run apply_redating.py."""
import json, os, sys, importlib

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import classify
importlib.reload(classify)
import fetch_and_match as fm
importlib.reload(fm)   # rebind fm.classify_row/row_dates to the reloaded classify

results = json.load(open(os.path.join(HERE, 'fetch_results.json')))
targets = json.load(open(os.path.join(HERE, 'target_list.json')))

seen = set()
rematched = 0
for shard in ('v3_raw0.jsonl', 'v3_raw1.jsonl'):
    p = os.path.join(HERE, shard)
    if not os.path.exists(p):
        continue
    for line in open(p):
        rec = json.loads(line)
        sym = rec['sym']
        if sym in seen or sym not in targets:
            continue
        seen.add(sym)
        entry = results.get(sym)
        if entry is None or entry.get('error'):
            continue
        entry['matches'] = fm.match_targets(rec['rows'], targets[sym])
        rematched += 1

json.dump(results, open(os.path.join(HERE, 'fetch_results.json'), 'w'))
print(f'rematched {rematched} symbols from raw cache '
      f'({len(seen)} raw records seen, {len(results)} total in results)')
