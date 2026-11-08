"""Weekend / holiday dates the exchanges really trade (Diwali Muhurat, budget weekends) — scripts/special_sessions.json.
A per-day fetcher that skips weekends lets these through: `if d.weekday() >= 5 and d not in special_sessions.DATES`."""
import datetime, json, os

try:
    DATES = {datetime.date.fromisoformat(k) for k in json.load(open(os.path.join(
        os.path.dirname(os.path.abspath(__file__)), 'special_sessions.json'), encoding='utf-8'))['sessions']}
except Exception:   # an unreadable list must never stop a fetcher; weekends are then skipped as before
    DATES = set()
