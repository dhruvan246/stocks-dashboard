#!/usr/bin/env python3
"""Fetch since-inception returns for every Direct-Growth Indian mutual fund.

Source: mfapi.in (free, no auth required, returns full NAV history per scheme).

For each scheme we extract: inception date, inception NAV, latest NAV, latest
date. Computes total return % and CAGR. Combined with the AMFI category from
the parser, this is what populates the Mutual Funds tab.

Output: scripts/mutual_funds.json (compact format ready for dashboard).
"""
import subprocess, json, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

OUT = Path("/sessions/loving-nifty-rubin/mnt/outputs/dhruvan-stocks-repo/scripts/mutual_funds.json")
SCHEMES = json.loads(open("/tmp/schemes_direct_growth.json").read())
print(f"Fetching NAV history for {len(SCHEMES)} schemes...")

def fetch_one(scheme):
    """Fetch full NAV history via mfapi.in, return inception + latest NAV info."""
    code = scheme['code']
    try:
        r = subprocess.run(
            ["curl","-s","--max-time","20",
             f"https://api.mfapi.in/mf/{code}"],
            capture_output=True, timeout=25)
        d = json.loads(r.stdout.decode("utf-8", errors="ignore"))
        if d.get('status') != 'SUCCESS' or not d.get('data'):
            return None
        data = d['data']  # newest first
        latest = data[0]
        inception = data[-1]
        nav_latest    = float(latest['nav'])
        nav_inception = float(inception['nav'])
        if nav_inception <= 0: return None
        # Parse dates DD-MM-YYYY
        d_inc = datetime.strptime(inception['date'], '%d-%m-%Y')
        d_lat = datetime.strptime(latest['date'],    '%d-%m-%Y')
        days = (d_lat - d_inc).days
        years = days / 365.25
        total_ret = (nav_latest - nav_inception) / nav_inception * 100
        cagr = ((nav_latest / nav_inception) ** (1 / years) - 1) * 100 if years > 0.1 else 0
        meta = d.get('meta', {})
        return {
            'code':       code,
            'name':       scheme['name'],
            'short':      scheme['name'].replace(' - DIRECT - Growth', '').replace(' - Direct Plan - Growth', '').replace(' Direct Plan-Growth', '').replace(' - Direct Plan - Growth Option', '').replace(' Direct - Growth', '').strip(),
            'amc':        meta.get('fund_house') or scheme.get('amc'),
            'category':   meta.get('scheme_category') or scheme['category'],
            'isin':       scheme.get('isin1') or '',
            'inceptionDate': d_inc.strftime('%Y-%m-%d'),
            'latestDate':    d_lat.strftime('%Y-%m-%d'),
            'inceptionNav':  round(nav_inception, 4),
            'latestNav':     round(nav_latest, 4),
            'totalReturnPct': round(total_ret, 2),
            'cagrPct':        round(cagr, 2),
            'years':          round(years, 2),
        }
    except Exception:
        return None

results = []
failed = 0
BATCH = 200
# Resume if possible
if OUT.exists():
    existing = json.loads(OUT.read_text())
    done_codes = {r['code'] for r in existing}
    SCHEMES = [s for s in SCHEMES if s['code'] not in done_codes]
    results = existing
    print(f"  resuming: {len(done_codes)} already done, {len(SCHEMES)} to go")

for batch_start in range(0, len(SCHEMES), BATCH):
    chunk = SCHEMES[batch_start:batch_start+BATCH]
    with ThreadPoolExecutor(max_workers=12) as pool:
        for fut in as_completed([pool.submit(fetch_one, s) for s in chunk]):
            r = fut.result()
            if r is None: failed += 1
            else: results.append(r)
    # Save after each batch so we don't lose progress
    OUT.write_text(json.dumps(results, separators=(",", ":")))
    done = min(batch_start+BATCH, len(SCHEMES))
    print(f"  [{done}/{len(SCHEMES)}]  ok={len(results)} fail={failed}", flush=True)

print(f"\nDone: {len(results)} succeeded, {failed} failed")

# Sort by CAGR descending
results.sort(key=lambda r: -(r['cagrPct'] or 0))
OUT.write_text(json.dumps(results, separators=(",", ":")))
print(f"Saved → {OUT}  ({OUT.stat().st_size / 1024:.1f} KB)")
