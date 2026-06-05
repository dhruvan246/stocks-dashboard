#!/usr/bin/env python3
"""Refresh returns for the mutual-fund universe (Direct-Growth plans).

Designed to run unattended in CI (GitHub Actions) with no external inputs.

The scheme universe is SEEDED from the existing scripts/mutual_funds.json, so
fund identities (category, name, AMC, ISIN, inception) stay stable across runs.
Each scheme's full NAV history is re-fetched from mfapi.in to recompute fresh
returns at 1d / 1w / 1m / 3m / 6m / 1y / 3y / 5y / 10y + since inception.

Resilience: a scheme that fails to fetch (mfapi hiccup / rate-limit) keeps its
PREVIOUS record instead of being dropped, so the universe never shrinks on a
transient error. On a totally fresh checkout with no seed file, it exits early.

Source: mfapi.in (free, no auth, full NAV history per scheme).
Output: scripts/mutual_funds.json
"""
import subprocess, json, os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT  = ROOT / "scripts" / "mutual_funds.json"

if not OUT.exists():
    raise SystemExit(f"Seed file {OUT} not found — cannot determine fund universe.")

existing = json.loads(OUT.read_text(encoding="utf-8"))
OLD = {r["code"]: r for r in existing}
SCHEMES = list(existing)

# Optional cap for local testing: MF_LIMIT=20 python scripts/fetch_mf_returns.py
LIMIT = int(os.environ.get("MF_LIMIT", "0") or "0")
if LIMIT > 0:
    SCHEMES = SCHEMES[:LIMIT]

print(f"Refreshing {len(SCHEMES)} schemes (returns at 1d/1w/1m/3m/6m/1y/3y/5y/10y/inception)...")

LOOKBACKS_DAYS = {
    'r1d': 1, 'r1w': 7, 'r1m': 30, 'r3m': 91, 'r6m': 182,
    'r1y': 365, 'r3y': 3*365, 'r5y': 5*365, 'r10y': 10*365,
}
# Maximum gap between requested lookback and the actual NAV we matched. Beyond
# this we return null — otherwise sparse-NAV schemes (segregated side-pockets,
# illiquid debt) would show the SAME return at every horizon because every
# lookback finds the same single old NAV point.
STALENESS_TOLERANCE_DAYS = {
    'r1d': 7, 'r1w': 7, 'r1m': 30, 'r3m': 30, 'r6m': 60,
    'r1y': 60, 'r3y': 180, 'r5y': 180, 'r10y': 180,
}
ANNUALIZE_AFTER_DAYS = 365  # any period > 1 year is reported as CAGR
STALE_AFTER_DAYS = 5        # latest NAV older than this -> flag the fund "stale"

def nav_at_or_before(data, target_dt):
    """data is descending by date. Return (nav, date) at the most recent date
    that is <= target_dt, or None if no such date exists."""
    for entry in data:
        try:
            d = datetime.strptime(entry['date'], '%d-%m-%Y')
        except Exception:
            continue
        if d <= target_dt:
            try:
                v = float(entry['nav'])
                return (v, d) if v > 0 else None
            except Exception:
                return None
    return None

def fetch_one(scheme):
    code = scheme['code']
    try:
        r = subprocess.run(
            ["curl", "-s", "--max-time", "25", f"https://api.mfapi.in/mf/{code}"],
            capture_output=True, timeout=30)
        d = json.loads(r.stdout.decode("utf-8", errors="ignore"))
        if d.get('status') != 'SUCCESS' or not d.get('data'):
            return None
        data = d['data']  # newest first
        latest = data[0]
        nav_latest = float(latest['nav'])
        if nav_latest <= 0:
            return None
        d_lat = datetime.strptime(latest['date'], '%d-%m-%Y')

        # Inception
        inception = data[-1]
        nav_inc = float(inception['nav'])
        if nav_inc <= 0:
            return None
        d_inc = datetime.strptime(inception['date'], '%d-%m-%Y')
        inc_days = (d_lat - d_inc).days
        inc_years = inc_days / 365.25
        inc_total = (nav_latest - nav_inc) / nav_inc * 100
        inc_cagr = ((nav_latest / nav_inc) ** (1 / inc_years) - 1) * 100 if inc_years > 0.1 else 0

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
        stale_days = (datetime.now(timezone.utc).replace(tzinfo=None) - d_lat).days
        return {
            'code': code,
            'name': scheme['name'],
            # Keep the already-cleaned short label / AMC / category / isin from the
            # seed record; refresh only the price + return fields below.
            'short':    scheme.get('short') or scheme['name'],
            'amc':      scheme.get('amc') or meta.get('fund_house'),
            'category': scheme.get('category') or meta.get('scheme_category'),
            'isin':     scheme.get('isin', ''),
            'inceptionDate': d_inc.strftime('%Y-%m-%d'),
            'latestDate':    d_lat.strftime('%Y-%m-%d'),
            'inceptionNav':  round(nav_inc, 4),
            'latestNav':     round(nav_latest, 4),
            'totalReturnPct': round(inc_total, 2),
            'cagrPct':        round(inc_cagr, 2),
            'years':          round(inc_years, 2),
            'stale':          stale_days > STALE_AFTER_DAYS,
            'staleDays':      max(stale_days, 0),
            **returns,
        }
    except Exception:
        return None

results = []
refreshed = failed = kept = 0
BATCH = 200
for batch_start in range(0, len(SCHEMES), BATCH):
    chunk = SCHEMES[batch_start:batch_start + BATCH]
    with ThreadPoolExecutor(max_workers=12) as pool:
        futs = {pool.submit(fetch_one, s): s for s in chunk}
        for fut in as_completed(futs):
            r = fut.result()
            if r is None:
                # Keep the previous record so the universe never shrinks.
                prev = OLD.get(futs[fut]['code'])
                if prev is not None:
                    results.append(prev)
                    kept += 1
                failed += 1
            else:
                results.append(r)
                refreshed += 1
    done = min(batch_start + BATCH, len(SCHEMES))
    print(f"  [{done}/{len(SCHEMES)}]  refreshed={refreshed} kept-stale={kept} fail={failed}", flush=True)

# Add any schemes we skipped via MF_LIMIT back unchanged, so a capped test run
# never erases the rest of the universe.
if LIMIT > 0:
    seen = {r['code'] for r in results}
    for r in existing:
        if r['code'] not in seen:
            results.append(r)

print(f"\nDone: {refreshed} refreshed, {kept} kept stale, {failed} failed -> {len(results)} total")
results.sort(key=lambda r: -(r.get('cagrPct') or 0))
OUT.write_text(json.dumps(results, separators=(",", ":")), encoding="utf-8")
print(f"Saved -> {OUT}  ({OUT.stat().st_size / 1024:.1f} KB)")
