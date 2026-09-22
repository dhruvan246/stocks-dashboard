"""Real IST timestamps for the ideas tool (scripts/ideas/*).

The daily routine runs on a cloud VM whose clock is UTC; the user's Mac runs IST. So
`datetime.datetime.now().strftime('... IST')` — what every builder here used to do — wrote a
UTC time wearing an IST label on every cloud run: signals.json was stamped "2026-09-22 14:06 IST"
for a run that happened at 19:36 IST. Every timestamp and every "today" these scripts write now
goes through this module, so the label is always true wherever the script runs.

Stdlib only and no tzdata lookup: IST is a fixed UTC+05:30 with no daylight saving.
"""
import datetime

IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30), 'IST')


def now():
    """Timezone-aware 'now' in IST."""
    return datetime.datetime.now(IST)


def stamp():
    """'YYYY-MM-DD HH:MM IST' — the timestamp format this repo writes everywhere (CLAUDE.md)."""
    return now().strftime('%Y-%m-%d %H:%M IST')


def today():
    """Today's date in IST — not the container's date, which is a day behind before 05:30 IST."""
    return now().date()
