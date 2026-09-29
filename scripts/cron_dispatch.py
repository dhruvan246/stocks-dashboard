# -*- coding: utf-8 -*-
"""Start scheduled workflows on time — GitHub's own cron runs 4-7 h late in this repo (runbook §217).

Measured 2026-09-27..29 over 45 scheduled workflows: median start 4-7 h after the slot (refresh-bse 6 h, portfolio-feed
ran 2 of 32 slots, ci-janitor 8 of 83, bse-live's 09:10 IST slot not at all). So schedules no longer live in `on:
schedule:`. Each workflow carries its slots as comment lines

    # dispatch-cron: "20 4 * * *"

(UTC, standard 5-field cron) plus `repository_dispatch: types: [tick-<file stem>]`. cron-dispatch.yml runs this script
on every cron-job.org tick (~5 min). For each workflow it takes the most recent slot in (now-180 min, now-2 min]; if
the workflow has no run created since one minute before that slot (any trigger — a manual or cron-job.org run counts),
it sends repository_dispatch "tick-<stem>" with client_payload {"schedule": "<cron>", "slot": "<ISO UTC>"}. A missed
tick is caught by the next one; a slot older than 3 h is dropped rather than run absurdly late.

Workflows that read github.event.schedule to pick a slot-specific branch must also read
github.event.client_payload.schedule (refresh-shareholding does).

Usage: python3 scripts/cron_dispatch.py [--dry-run] [--now 2026-09-29T03:42:00Z]
Needs GITHUB_TOKEN (actions: read, contents: write — repository_dispatch) and GITHUB_REPOSITORY.
"""
import argparse
import datetime as dt
import glob
import json
import os
import re
import sys
import urllib.parse
import urllib.request

WINDOW_MIN = 180   # a slot older than this is skipped, not run hours late
GRACE_MIN = 2      # leave a just-passed slot to any external trigger aimed at the same minute
CRON_RE = re.compile(r'^\s*#\s*dispatch-cron:\s*"([^"]+)"', re.M)


# ---- minimal 5-field cron matcher (numbers, *, ranges, lists, steps; DOM/DOW OR-ed like POSIX cron) ----
def _field(spec, lo, hi):
    vals = set()
    for part in spec.split(','):
        step = 1
        if '/' in part:
            part, s = part.split('/')
            step = int(s)
        if part == '*':
            a, b = lo, hi
        elif '-' in part:
            a, b = map(int, part.split('-'))
        else:
            a = b = int(part)
            if step > 1:
                b = hi
        vals.update(range(a, b + 1, step))
    return vals


class Cron:
    def __init__(self, expr):
        f = expr.split()
        if len(f) != 5:
            raise ValueError('bad cron: %r' % expr)
        self.expr = expr
        self.mi, self.h = _field(f[0], 0, 59), _field(f[1], 0, 23)
        self.dom, self.mon = _field(f[2], 1, 31), _field(f[3], 1, 12)
        self.dow = {d % 7 for d in _field(f[4], 0, 7)}
        self.dom_star, self.dow_star = f[2] == '*', f[4] == '*'

    def match(self, t):
        if t.minute not in self.mi or t.hour not in self.h or t.month not in self.mon:
            return False
        dom_ok, dow_ok = t.day in self.dom, (t.isoweekday() % 7) in self.dow
        if self.dom_star or self.dow_star:
            return dom_ok and dow_ok
        return dom_ok or dow_ok

    def last_slot(self, lo, hi):
        """Latest minute t with lo < t <= hi that matches, else None."""
        t = hi.replace(second=0, microsecond=0)
        while t > lo:
            if self.match(t):
                return t
            t -= dt.timedelta(minutes=1)
        return None


def api(path, method='GET', body=None):
    req = urllib.request.Request('https://api.github.com' + path, method=method,
                                 data=json.dumps(body).encode() if body is not None else None)
    req.add_header('Authorization', 'Bearer ' + os.environ['GITHUB_TOKEN'])
    req.add_header('Accept', 'application/vnd.github+json')
    req.add_header('X-GitHub-Api-Version', '2022-11-28')
    with urllib.request.urlopen(req, timeout=30) as r:
        raw = r.read()
        return json.loads(raw) if raw else None


def recent_runs(repo, since):
    """Every run created since `since`, across all workflows (paginated; the per-page cap is 100)."""
    runs, page = [], 1
    q = urllib.parse.quote('>=' + since.strftime('%Y-%m-%dT%H:%M:%SZ'))
    while page <= 10:
        j = api('/repos/%s/actions/runs?per_page=100&page=%d&created=%s' % (repo, page, q))
        batch = j.get('workflow_runs') or []
        runs += batch
        if len(batch) < 100 or len(runs) >= (j.get('total_count') or 0):
            break
        page += 1
    return runs


def load_schedules():
    out = {}
    for f in sorted(glob.glob('.github/workflows/*.yml')):
        crons = CRON_RE.findall(open(f, encoding='utf-8').read())
        if crons:
            out[f] = [Cron(c) for c in crons]
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--now', help='pretend time (ISO UTC) for testing')
    a = ap.parse_args()
    now = (dt.datetime.fromisoformat(a.now.replace('Z', '+00:00')) if a.now
           else dt.datetime.now(dt.timezone.utc))
    repo = os.environ.get('GITHUB_REPOSITORY', 'dhruvan246/stocks-dashboard')
    sched = load_schedules()
    lo, hi = now - dt.timedelta(minutes=WINDOW_MIN), now - dt.timedelta(minutes=GRACE_MIN)

    due = {}
    for f, crons in sched.items():
        best = None
        for c in crons:
            s = c.last_slot(lo, hi)
            if s and (best is None or s > best[0]):
                best = (s, c.expr)
        if best:
            due[f] = best
    print('%s UTC: %d workflows carry dispatch-cron lines, %d have a slot in the last %d min'
          % (now.strftime('%Y-%m-%d %H:%M'), len(sched), len(due), WINDOW_MIN))
    if not due:
        return

    runs = recent_runs(repo, min(s for s, _ in due.values()) - dt.timedelta(minutes=1))
    sent = fails = 0
    for f, (slot, expr) in sorted(due.items()):
        cutoff = slot - dt.timedelta(minutes=1)
        mine = [r for r in runs if r.get('path') == f and
                dt.datetime.fromisoformat(r['created_at'].replace('Z', '+00:00')) >= cutoff]
        stem = os.path.basename(f)[:-4]
        if mine:
            print('  ok    %-34s slot %s (%s) — already ran (%s, %s)'
                  % (stem, slot.strftime('%H:%M'), expr, mine[-1]['event'], mine[-1]['created_at'][11:16]))
            continue
        print('  START %-34s slot %s (%s)%s' % (stem, slot.strftime('%H:%M'), expr, ' [dry-run]' if a.dry_run else ''))
        if a.dry_run:
            continue
        try:
            api('/repos/%s/dispatches' % repo, 'POST',
                {'event_type': 'tick-' + stem,
                 'client_payload': {'schedule': expr, 'slot': slot.strftime('%Y-%m-%dT%H:%M:%SZ')}})
            sent += 1
        except Exception as e:
            fails += 1
            print('::warning::dispatch failed for %s: %s' % (stem, e))
    print('dispatched %d, failed %d' % (sent, fails))
    if fails:
        sys.exit(1)


if __name__ == '__main__':
    main()
