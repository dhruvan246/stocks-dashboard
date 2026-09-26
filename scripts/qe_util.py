# -*- coding: utf-8 -*-
"""Quarter-end date helpers shared by the BSE results readers (fetch_bse_fund, bse_vision_prep,
merge_bse_vision). A quarter end is an int YYYYMMDD on a calendar quarter end (0331/0630/0930/1231).

Nothing here knows which quarter is "current" — callers derive that from the filing itself (its
printed period, else its announcement date), so the readers never go stale at a season change.
"""
import datetime

_ENDS = (331, 630, 930, 1231)
_MON = ["", "January", "February", "March", "April", "May", "June", "July", "August", "September",
        "October", "November", "December"]


def is_qe(qe):
    return isinstance(qe, int) and qe % 10000 in _ENDS and 1990 <= qe // 10000 <= 2100


def prevq(qe):
    """20260930 -> 20260630; 20260331 -> 20251231."""
    y, md = qe // 10000, qe % 10000
    return {331: (y - 1) * 10000 + 1231, 630: y * 10000 + 331,
            930: y * 10000 + 630, 1231: y * 10000 + 930}[md]


def yago(qe):
    """Same quarter one year earlier: 20260930 -> 20250930."""
    return qe - 10000


def label(qe):
    """20260930 -> '30 September 2026' (the wording results tables print)."""
    return "%d %s %d" % (qe % 100, _MON[(qe // 100) % 100], qe // 10000)


def last_qe_before(yyyymmdd):
    """Latest quarter end STRICTLY before a date (int YYYYMMDD or 'YYYY-MM-DD'). A result filed on
    2026-10-20 is for the quarter ended 20260930. Returns 0 for an unparseable date."""
    try:
        s = str(yyyymmdd).replace("-", "")[:8]
        d = datetime.date(int(s[:4]), int(s[4:6]), int(s[6:8]))
    except Exception:
        return 0
    cands = [(d.year - 1) * 10000 + 1231] + [d.year * 10000 + md for md in _ENDS]
    return max(c for c in cands if datetime.date(c // 10000, c // 100 % 100, c % 100) < d)
