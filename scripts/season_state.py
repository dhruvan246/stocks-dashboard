# -*- coding: utf-8 -*-
"""Results-season state — IN or OFF — from the results feed's filing counts and the quarter calendar (runbook §222).

One answer for every reader, so the whole cadence moves together without anyone flipping crons by hand:
  * scripts/cron_dispatch.py picks each workflow's `# dispatch-cron[in-season]:` / `[off-season]:` slots by it
    (refresh-fundamentals every 30 min vs hourly; refresh-results-hourly hourly vs 6×/day);
  * the bse-vision-fill cloud routine — once the user applies the §222 one-time edit in the routines UI (4-slot cron + the
    prompt in scripts/routine_prompts/) — runs `--vision-slot` first thing: off-season its three daytime slots exit at once;
  * build_results_coverage.py stamps it into docs/results_coverage.json for the Results coverage page.

Inputs (nothing over the network): docs/results_feed.json — {updated, rows}, row = [SYM, name, 'YYYY-MM-DD HH:MM:SS' (IST),
qe, headline, pdf] — docs/results_calendar.json (confirmed board meetings; missing/unreadable = counted as none) — and the clock. Counts are result filings per IST calendar day, NSE + BSE merged.

Rule (STATELESS — the same inputs always give the same answer; no memory, nothing to get stuck):
  quarter  = the latest quarter end before today (31 Mar / 30 Jun / 30 Sep / 31 Dec)
  deadline = quarter end + 45 days (SEBI LODR reg. 33) — + 60 days for the audited March quarter: 14 Aug / 14 Nov / 14 Feb / 30 May
  window   = [quarter end + 9 days, deadline + 3 days]. Measured on four seasons (sf_fundamentals announcement dates): the first
             filers land on day 9-14 after the quarter end; the first WORKING day after the deadline floods once more
             (Mon 17-Aug-2026: 236 NSE names; Mon 17-Nov-2025: 222) and the day after collapses (2 and 7). +3 reaches that
             Monday for a Fri/Sat/Sun deadline.
  IN  when (today in window AND filings since the window opened >= IGNITE_CUM) calendar + the season has measurably begun
        OR (today in window AND result board meetings CONFIRMED for today >= CAL_DAY)  the exchange calendar (§228, below)
        OR (yesterday + the day before >= HOT_TWO_DAYS)                        counts alone: the post-deadline tail, any flood
        OR (any of today / yesterday / the day before >= HOT_DAY)
  OFF otherwise.
  Calendar trigger (runbook §228, 2026-10-12): docs/results_calendar.json — row = [SYM, name, 'YYYY-MM-DD', purpose] — lists
  the board meetings the exchange has CONFIRMED. Counting only purposes naming results: off-season days carry 1-7 (16-23 Sep
  2026: 2-4/day; Fri 9 Oct: 7; Sat 10 Oct: 3), the first real day of a season 13-20 (Tue 14 Jul 2026: 13; Mon 12 Oct 2026: 20),
  then 27-576. CAL_DAY = 10 sits between. Why it is needed: on Mon 12-Oct-2026 the filing counts said OFF all day (37 since
  the window opened vs IGNITE_CUM 40) while 20 companies — HCLTECH, ICICIAMC, RBLBANK, GSFC… — were confirmed for that day,
  so the feed refreshed 4-hourly on a results day. The window guard stays: a count outside the window never opens a season.
  The ignition count starts at the window's opening day, not the quarter end: the previous quarter's late filers trickle
  in at 3-7/day all off-season (Oct 1-6 2026: 20 rows), and that trickle alone must not open a season — at 3-5/day it
  takes 4-7 days of window to reach IGNITE_CUM (20 since 2026-10-12, user's call; 40 before), by which time the calendar
  trigger below has long since opened it on the first real day. Inside the window the count
  is monotone through the day and the season, so weekends and holiday lulls cannot flip the state back.
Replayed on the Jun-2026, Mar-2026, Dec-2025 and Sep-2025 seasons (scripts/test_season_state.py): IN within a day or two of
the first real filing day, OFF 1-2 days after the collapse; weekend dips inside the window cannot flip it. Today's count is
included, so the state can only move OFF→IN during a day; IN→OFF happens at midnight IST.
A stale feed (newest row older than STALE_DAYS) is reported; inside the window it still reads IN — an extra read is cheaper
than a missed one.

CLI:  python3 scripts/season_state.py                 # JSON report
      python3 scripts/season_state.py --state         # prints "in" or "off"
      python3 scripts/season_state.py --vision-slot   # "RUN ..." or "SKIP ..." for the bse-vision-fill slot that just fired
      --now 2026-08-17T04:00:00Z   --feed PATH   --calendar PATH   --force in|off      (tests and replays only)
"""
import argparse
import datetime as dt
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
FEED = os.path.join(HERE, "..", "docs", "results_feed.json")
CAL = os.path.join(HERE, "..", "docs", "results_calendar.json")
IST = dt.timezone(dt.timedelta(hours=5, minutes=30))

WINDOW_OPEN_DAYS = 9      # first filers land on day 9-14 after the quarter end
WINDOW_CLOSE_DAYS = 3     # the first working day after the deadline still floods; the day after collapses
IGNITE_CUM = 20           # filings since the window opened that mark the season as begun (inside the window); 40 until 2026-10-12 (user: "make it 20 from 40")
HOT_DAY = 30              # one day this busy is season, whatever the calendar says (off-season days run 0-7)
HOT_TWO_DAYS = 40         # yesterday + the day before
CAL_DAY = 10              # result board meetings confirmed for today on the exchange calendar (off-season 1-7, season 13+)
STALE_DAYS = 3

# The vision reader's target slot set (UTC) = 13:15 / 16:15 / 21:15 / 00:15 IST — the cron the user's §222 edit gives the
# bse-vision-fill routine. VISION_CRON is checked against this table by the test. 00:15 is the anchor the 2026-09-27
# measurement chose (the 22:30 IST feed top-up lands 23:31-23:38); 21:15 follows refresh-bse's 20:10 grind (5-12 min);
# all at :45 so a slot sees the :30 fundamentals pass. Off-season only the 00:15 slot does work (--vision-slot).
VISION_CRON = "45 7,10,15,18 * * *"
VISION_SLOTS_UTC = {"in": [(7, 45), (10, 45), (15, 45), (18, 45)], "off": [(18, 45)]}


def quarter_end_before(d):
    """Latest quarter end strictly before date d."""
    for y, m, dd in ((d.year, 12, 31), (d.year, 9, 30), (d.year, 6, 30), (d.year, 3, 31), (d.year - 1, 12, 31)):
        qe = dt.date(y, m, dd)
        if qe < d:
            return qe


def calendar(d):
    qe = quarter_end_before(d)
    deadline = qe + dt.timedelta(days=60 if qe.month == 3 else 45)
    return {"qe": qe, "deadline": deadline,
            "open": qe + dt.timedelta(days=WINDOW_OPEN_DAYS), "close": deadline + dt.timedelta(days=WINDOW_CLOSE_DAYS)}


def load_feed(path=None):
    d = json.load(open(path or FEED, encoding="utf-8"))
    return d.get("rows") or [], d.get("updated")


def load_calendar(path=None):
    """Confirmed board-meeting rows from docs/results_calendar.json; [] when the file is missing or unreadable (never raises —
    the calendar is a second signal, not a dependency)."""
    try:
        d = json.load(open(path or CAL, encoding="utf-8"))
        return d.get("rows") or []
    except Exception:
        return []


def meeting_counts(cal_rows):
    """{'YYYY-MM-DD': confirmed board meetings whose stated purpose names results} (row field 2 = date, 3 = purpose)."""
    by = {}
    for r in cal_rows:
        if isinstance(r, (list, tuple)) and len(r) > 3 and isinstance(r[2], str) and "result" in str(r[3] or "").lower():
            by[r[2][:10]] = by.get(r[2][:10], 0) + 1
    return by


def daily_counts(rows):
    """{'YYYY-MM-DD': filings} by the IST filing day (row field 2)."""
    by = {}
    for r in rows:
        ts = r[2] if isinstance(r, (list, tuple)) and len(r) > 2 else None
        if isinstance(ts, str) and len(ts) >= 10:
            by[ts[:10]] = by.get(ts[:10], 0) + 1
    return by


def _ist_slot(h, m):
    t = (h * 60 + m + 330) % 1440
    return "%02d:%02d" % (t // 60, t % 60)


def evaluate(now_utc=None, feed_path=None, rows=None, force=None, cal_path=None, cal_rows=None):
    now = now_utc or dt.datetime.now(dt.timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=dt.timezone.utc)
    today = now.astimezone(IST).date()
    cal = calendar(today)
    updated = None
    if rows is None:
        rows, updated = load_feed(feed_path)
    by = daily_counts(rows)
    meetings = meeting_counts(cal_rows if cal_rows is not None else load_calendar(cal_path))
    key = lambda d: d.isoformat()
    d1, d2 = today - dt.timedelta(days=1), today - dt.timedelta(days=2)
    c0, c1, c2 = by.get(key(today), 0), by.get(key(d1), 0), by.get(key(d2), 0)
    cum = sum(n for day, n in by.items() if key(cal["open"]) <= day <= key(today))
    newest = max(by) if by else None
    stale = newest is None or (today - dt.date.fromisoformat(newest)).days > STALE_DAYS
    in_window = cal["open"] <= today <= cal["close"]

    why = []
    if in_window and cum >= IGNITE_CUM:
        why.append("calendar window and %d filings since it opened on %s" % (cum, cal["open"]))
    m0 = meetings.get(key(today), 0)
    if in_window and m0 >= CAL_DAY:
        why.append("%d companies' result board meetings confirmed for today on the exchange calendar" % m0)
    if in_window and stale:
        why.append("calendar window, feed stale (newest row %s) — fail open" % newest)
    if c1 + c2 >= HOT_TWO_DAYS:
        why.append("%d filings over the last two days" % (c1 + c2))
    hot = max(c0, c1, c2)
    if hot >= HOT_DAY:
        why.append("%d filings in one day" % hot)
    state = "in" if why else "off"
    if not why:
        why.append("window open but only %d filings since it opened on %s" % (cum, cal["open"]) if in_window
                   else "outside the %s to %s window" % (cal["open"], cal["close"]))
    if force in ("in", "off"):
        state = force
        why.insert(0, "FORCED --force %s" % force)
    reason = "; ".join(why)
    counts = {"today": c0, "yesterday": c1, "day_before": c2, "since_window_open": cum, "meetings_today": m0}
    summary = "%s — %s; filings today %d / yesterday %d / day before %d; meetings confirmed today %d; feed newest %s%s" % (
        state.upper(), reason, c0, c1, c2, m0, newest, " (STALE)" if stale else "")
    return {
        "state": state, "reason": reason, "summary": summary,
        "today_ist": key(today), "qe": key(cal["qe"]), "deadline": key(cal["deadline"]),
        "window": [key(cal["open"]), key(cal["close"])], "in_window": in_window,
        "counts": counts, "feed_updated": updated, "feed_newest_day": newest, "feed_stale": stale,
        "thresholds": {"ignite_cum": IGNITE_CUM, "hot_day": HOT_DAY, "hot_two_days": HOT_TWO_DAYS, "cal_day": CAL_DAY},
        "vision": {"cron_utc": VISION_CRON,
                   "slots_ist": [_ist_slot(h, m) for h, m in VISION_SLOTS_UTC[state]],
                   "slots_utc": ["%02d:%02d" % s for s in VISION_SLOTS_UTC[state]],
                   "runs_per_day": len(VISION_SLOTS_UTC[state])},
    }


def vision_slot(now_utc, state):
    """(run?, slot) for the bse-vision-fill run that just fired: the latest cron slot in the last 3 h, RUN if that
    slot is active in this state. No slot in range = a manual run → RUN."""
    t = now_utc.replace(second=0, microsecond=0)
    for back in range(0, 181):
        tt = t - dt.timedelta(minutes=back)
        if (tt.hour, tt.minute) in VISION_SLOTS_UTC["in"]:
            return (tt.hour, tt.minute) in VISION_SLOTS_UTC[state], "%02d:%02d UTC (%s IST)" % (tt.hour, tt.minute, _ist_slot(tt.hour, tt.minute))
    return True, "no scheduled slot in the last 3 h (manual run)"


def parse_now(s):
    if not s:
        return dt.datetime.now(dt.timezone.utc)
    s = s.replace("Z", "+00:00")
    t = dt.datetime.fromisoformat(s)
    return t if t.tzinfo else t.replace(tzinfo=dt.timezone.utc)


def main():
    ap = argparse.ArgumentParser(description="results-season state (runbook §222)")
    ap.add_argument("--now", help="pretend time, ISO UTC (tests)")
    ap.add_argument("--feed", help="alternative results_feed.json (tests)")
    ap.add_argument("--calendar", help="alternative results_calendar.json (tests)")
    ap.add_argument("--force", choices=["in", "off"], help="override the verdict (tests)")
    ap.add_argument("--state", action="store_true", help='print just "in" or "off"')
    ap.add_argument("--vision-slot", action="store_true", help="RUN/SKIP for the vision-fill slot that just fired")
    a = ap.parse_args()
    now = parse_now(a.now)
    try:
        st = evaluate(now, a.feed, force=a.force, cal_path=a.calendar)
    except Exception as ex:
        if a.vision_slot:
            print("RUN — season_state could not evaluate (%s): failing open" % ex)
            return
        raise
    if a.vision_slot:
        ok, slot = vision_slot(now, st["state"])
        print("%s slot %s — season %s" % ("RUN" if ok else "SKIP", slot, st["summary"]))
        return
    if a.state:
        print(st["state"])
        return
    print(json.dumps(st, indent=1, ensure_ascii=False))


if __name__ == "__main__":
    main()
