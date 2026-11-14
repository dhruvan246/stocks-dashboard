"""Half-year-only filers: skip them in the daily vision runs, re-read them once at month end.

Why (user, 2026-11-14: "skip them and run them directly once a month end"): ~430 SME filers print only
half-year columns, so every bse-vision-fill run rendered and vision-read them (~59 image sub-agents a run)
and every read came back ok:false "half-year columns only". That re-reading was most of the routine's cost.

Ledger scripts/vision_halfyear_skip.json: {"EXCH|id": {"since": "YYYY-MM-DD", "qe": first quarter seen, "note": "..."}}
  id = NSE symbol, or BSE scrip code. ONE key per company, valid for ALL quarters, current and future
  (user, 2026-11-14: "add these lines in bse vision fill for all future quarters"). Written by
  merge_bse_vision.py from the reader's own ok:false note; the key is removed as soon as ANY quarter of
  that company is read ok (it has started printing quarterly columns), so it re-enters the daily runs.
Prep (bse_vision_prep.py) drops ledgered names from the pending lists EXCEPT on the month-end slot:
  the run that starts on the LAST calendar day of the month before 06:00 IST (the 00:15 IST slot, which
  runs both in and off season), or any run given --include-halfyear. Nothing is ever deleted: a skipped
  name stays pending on the coverage page and is retried every month end.
"""
import datetime as dt
import json
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))
PATH = os.path.join(HERE, "vision_halfyear_skip.json")
IST = dt.timezone(dt.timedelta(hours=5, minutes=30))
HALF = re.compile(r"half[\s-]?year|half[\s-]?yearly|six[\s-]?month|\b6[\s-]?m(onths?)?\b|\bH1\b|\bH2\b", re.I)


def load():
    try:
        return json.load(open(PATH, encoding="utf-8"))
    except Exception:
        return {}


def save(led):
    json.dump(dict(sorted(led.items())), open(PATH, "w", encoding="utf-8"), ensure_ascii=False, indent=0)


def key(exch, ident, qe=None):          # qe kept in the signature for callers; the key is per company
    return "%s|%s" % (exch, str(ident).upper())


def month_end_slot(now=None):
    now = (now or dt.datetime.now(dt.timezone.utc)).astimezone(IST)
    return (now + dt.timedelta(days=1)).day == 1 and now.hour < 6


def include_all(argv):
    return "--include-halfyear" in argv or month_end_slot()


def is_halfyear_note(rec):
    if rec.get("ok"):
        return False
    return bool(HALF.search(str(rec.get("note") or "")))


def filter_pending(nse, bse, qe, led):
    """Drop ledgered companies (any quarter, incl. qe and later). nse rows = (sym, ...); bse rows = (scrip, (SYM, name, mcap))."""
    n2 = [r for r in nse if key("NSE", r[0], qe) not in led]
    b2 = [r for r in bse if key("BSE", r[0], qe) not in led]
    return n2, b2, (len(nse) - len(n2)) + (len(bse) - len(b2))


def update_from_reads(items, today=None):
    """Add ok:false half-year reads, drop ok reads. Returns (added, removed)."""
    led = load()
    today = today or dt.datetime.now(IST).strftime("%Y-%m-%d")
    add = rem = 0
    for it in items:
        exch = (it.get("exch") or "").upper()
        ident = it.get("scrip") if exch == "BSE" else (it.get("sym") or it.get("ticker"))
        qe = str(it.get("qe") or "")
        if not (exch in ("NSE", "BSE") and ident and qe):
            continue
        k = key(exch, ident, qe)
        if it.get("ok"):
            if led.pop(k, None) is not None:
                rem += 1
        elif is_halfyear_note(it) and k not in led:
            led[k] = {"since": today, "qe": qe, "sym": (it.get("sym") or "").upper(), "note": str(it.get("note") or "")[:160]}
            add += 1
    save(led)
    return add, rem
