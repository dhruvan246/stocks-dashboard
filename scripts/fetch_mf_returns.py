#!/usr/bin/env python3
"""Fetch returns for every Direct-Growth Indian mutual fund.

Source: mfapi.in (free, no auth required, returns full NAV history per scheme).

Computes returns at multiple lookback periods:
  - 1d, 1w, 1m, 3m, 6m: absolute %
  - 1y: absolute %
  - 3y, 5y, 10y: CAGR (annualized)
  - since inception: total + CAGR
Plus the AMFI scheme category, inception date, and latest NAV.

Output: scripts/mutual_funds.json
"""
import subprocess, json, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

OUT = Path("/sessions/loving-nifty-rubin/mnt/outputs/dhruvan-stocks-repo/scripts/mutual_funds.json")
SCHEMES = json.loads(open("/tmp/schemes_direct_growth.json").read())
print(f"Fetching {len(SCHEMES)} schemes (returns at 1d/1w/1m/3m/6m/1y/3y/5y/10y/inception)...")

LOOKBACKS_DAYS = {
    'r1d':   1,
    'r1w':   7,
    'r1m':   30,
    'r3m':   91,
    'r6m':   182,
    'r1y':   365,
    'r3y':   3*365,
    'r5y':   5*365,
    'r10y': 10*365,
}
# Maximum gap between requested lookback and the actual NAV we matched. Beyond
# this we return null — otherwise sparse-NAV schemes (segregated side-pockets,
# illiquid debt) would show the SAME return at every horizon because every
# lookback finds the same single old NAV point.
STALENESS_TOLERANCE_DAYS = {
    'r1d':   7,   # weekend/holiday OK, but not weeks
    'r1w':   7,
    'r1m':   30,
    'r3m':   30,
    'r6m':   60,
    'r1y':   60,
    'r3y':  180,
    'r5y':  180,
    'r10y': 180,
}
ANNUALIZE_AFTER_DAYS = 365  # any period > 1 year is reported as CAGR

def nav_at_or_before(data, target_dt):
    """data is descending by date. Return (nav, date) at the most recent date
    that is <= target_dt, or None if no such date exists."""
    for entry in data:
        try:
            d = datetime.strptime(entry['date'], '%d-%m-%Y')
        except: continue
        if d <= target_dt:
            try:
                v = float(entry['nav'])
                return (v, d) if v > 0 else None
            except: return None
    return None

def fetch_one(scheme):
    code = scheme['code']
    try:
        r = subprocess.run(
            ["curl","-s","--max-time","25",f"https://api.mfapi.in/mf/{code}"],
            capture_output=True, timeout=30)
        d = json.loads(r.stdout.decode("utf-8", errors="ignore"))
        if d.get('status') != 'SUCCESS' or not d.get('data'): return None
        data = d['data']  # newest first
        latest = data[0]
        nav_latest = float(latest['nav'])
        if nav_latest <= 0: return None
        d_lat = datetime.strptime(latest['date'], '%d-%m-%Y')

        # Inception
        inception = data[-1]
        nav_inc = float(inception['nav'])
        if nav_inc <= 0: return None
        d_inc = datetime.strptime(inception['date'], '%d-%m-%Y')
        inc_days = (d_lat - d_inc).days
        inc_years = inc_days / 365.25
        inc_total = (nav_latest - nav_inc) / nav_inc * 100
        inc_cagr  = ((nav_latest / nav_inc) ** (1 / inc_years) - 1) * 100 if inc_years > 0.1 else 0

        # Returns at each lookback
        returns = {}
        for label, days in LOOKBACKS_DAYS.items():
            if inc_days < days:
                returns[label] = None
                continue
            target = d_lat - timedelta(days=days)
            hit = nav_at_or_before(data, target)
            if hit is None:
                returns[label] = None
                continue
            nav_then, d_then = hit
            actual_days = (d_lat - d_then).days
            if actual_days < 1:
                returns[label] = None
                continue
            # Staleness check: the matched NAV must be within tolerance of the
            # requested lookback. Otherwise the data is too sparse for a
            # meaningful return at this horizon.
            if actual_days > days + STALENESS_TOLERANCE_DAYS[label]:
                returns[label] = None
                continue
            if days <= ANNUALIZE_AFTER_DAYS:
                returns[label] = round((nav_latest - nav_then) / nav_then * 100, 2)
            else:
                yrs = actual_days / 365.25
                cagr = ((nav_latest / nav_then) ** (1 / yrs) - 1) * 100
                returns[label] = round(cagr, 2)

        meta = d.get('meta', {})
        return {
            'code': code,
            'name': scheme['name'],
            'short': scheme['name']
                .replace(' - DIRECT - Growth', '')
                .replace(' - Direct Plan - Growth Option', '')
                .replace(' - Direct Plan - Growth', '')
                .replace(' Direct Plan-Growth', '')
                .replace(' Direct - Growth', '')
                .replace('- Direct (G)', '')
                .strip(),
            'amc':       meta.get('fund_house') or scheme.get('amc'),
            'category':  meta.get('scheme_category') or scheme['category'],
            'isin':      scheme.get('isin1') or '',
            'inceptionDate': d_inc.strftime('%Y-%m-%d'),
            'latestDate':    d_lat.strftime('%Y-%m-%d'),
            'inceptionNav':  round(nav_inc, 4),
            'latestNav':     round(nav_latest, 4),
            'totalReturnPct': round(inc_total, 2),
            'cagrPct':        round(inc_cagr, 2),
            'years':          round(inc_years, 2),
            **returns,
        }
    except Exception:
        return None

results = []
failed = 0
BATCH = 200
if OUT.exists():
    try:
        existing = json.loads(OUT.read_text())
        if existing and 'r1y' in existing[0]:
            done = {r['code'] for r in existing}
            SCHEMES = [s for s in SCHEMES if s['code'] not in done]
            results = existing
            print(f"  resuming: {len(done)} already in new format, {len(SCHEMES)} to go")
    except Exception:
        pass

for batch_start in range(0, len(SCHEMES), BATCH):
    chunk = SCHEMES[batch_start:batch_start+BATCH]
    with ThreadPoolExecutor(max_workers=12) as pool:
        for fut in as_completed([pool.submit(fetch_one, s) for s in chunk]):
            r = fut.result()
            if r is None: failed += 1
            else: results.append(r)
    OUT.write_text(json.dumps(results, separators=(",", ":")))
    done = min(batch_start+BATCH, len(SCHEMES))
    print(f"  [{done}/{len(SCHEMES)}]  ok={len(results)} fail={failed}", flush=True)

print(f"\nDone: {len(results)} succeeded, {failed} failed")
results.sort(key=lambda r: -(r['cagrPct'] or 0))
OUT.write_text(json.dumps(results, separators=(",", ":")))
print(f"Saved → {OUT}  ({OUT.stat().st_size / 1024:.1f} KB)")
