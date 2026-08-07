# -*- coding: utf-8 -*-
"""Address a statement's columns by their PRINTED PERIOD DATE, with no stored value to anchor on.

Why this is needed. Every reader in this campaign identifies the target column by matching a value
we ALREADY store (§58 column anchor). That works beautifully for revenue, because consolidated PAT
is usually present to anchor on -- and it fails completely for the 2,744 pre-2020 consolidated
revenue cells whose con PAT is ALSO missing. No stored value, no anchor, no read: measured, every
one of 203 successful pre-2020 reads came from the anchored pool and ZERO from the unanchored one.

The way out is the thing §55b was always pointing at: the statement prints its own period dates in
the header. Parse those and the column is identified by what the document SAYS it is, not by what we
happen to already know.

    header:  "Quarter ended 31.12.2018 | 30.09.2018 | 31.12.2017 | Nine months ended 31.12.2018"
    -> {20181231: x=412, 20180930: x=478, 20171231: x=544, ...}

CUMULATIVE COLUMNS ARE THE TRAP. A quarterly statement prints year-to-date columns beside the
quarterly ones, often ending on the SAME date -- "Quarter ended 31.12.2018" and "Nine months ended
31.12.2018" both key to 20181231. Taking the wrong one lands a 9-month figure as a quarter, which is
exactly the cumulative defect this campaign spent the night healing (58 cells). So a date is only
usable when the span word above it says quarter/three-months, and any column whose span reads
nine/six/half/year-to-date/twelve is EXCLUDED, not silently preferred against.

Self-validating: the caller checks the mapping on the STANDALONE statement first (where a stored
value does exist), and only trusts the consolidated page if the same parsing reproduced standalone.

STATUS 2026-08-07: NOT WIRED INTO ANY WRITER -- it is not good enough yet, and the measurements say
so. Over 602 cached statement pages:
    54% of text pages yielded any quarter date;
    1.5 quarter-columns per page on average, where a real quarterly statement has 3-5, so it is
      finding FRAGMENTS of header rows rather than whole ones;
    several hits came from narrative notes ("profit for the quarter en...") rather than table
      headers, i.e. a date in prose is being mistaken for a column;
    1 of 8 sampled pages produced a value at the mapped column.
What it needs before it can be trusted: restrict date detection to a header BAND (the contiguous
lines above the first numeric row) instead of the whole page, group same-y dates into one header row
and require >=2 of them, and treat a lone date on a page as prose until proven otherwise.
Committed as groundwork so the next pass starts from measurements rather than from scratch.
"""
import re

# 31.12.2018 | 31/12/2018 | 31-12-2018 | 31.12.18
NUMERIC = re.compile(r"^(\d{1,2})[./-](\d{1,2})[./-](\d{2,4})$")
# 31st December, 2018 / December 31, 2018 / 31 Dec 2018
MONTHS = {m: i for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"], 1)}
DAYMON = re.compile(r"^(\d{1,2})(?:st|nd|rd|th)?$", re.I)
YEAR = re.compile(r"^(20\d{2})$")

# span words that mean this column is NOT a single quarter
CUMULATIVE = re.compile(r"nine|six|half|year\s*to\s*date|y\.?t\.?d|twelve|\byear\s+ended|"
                        r"period\s+ended.*month", re.I)
QUARTERLY = re.compile(r"quarter|three\s*month|3\s*month", re.I)
LAST_DAY = {3: 31, 6: 30, 9: 30, 12: 31}


def _norm(d, m, y):
    y = int(y)
    if y < 100:
        y += 2000
    d, m = int(d), int(m)
    if not (1 <= m <= 12) or not (1 <= d <= 31):
        return None
    # snap to the quarter end it belongs to; statements print the true period end
    if m in LAST_DAY and d >= 28:
        return y * 10000 + m * 100 + LAST_DAY[m]
    return None


def dates_on(page, ytol=3.0):
    """-> [(qe, x_right, y, span_text)] for every period date printed on the page."""
    words = page.get_text("words")
    if not words:
        return []
    rows = {}
    for x0, y0, x1, y1, w, *_ in words:
        rows.setdefault(round(y0 / ytol), []).append((x0, x1, w))
    lines = []
    for key in sorted(rows):
        toks = sorted(rows[key], key=lambda t: t[0])
        lines.append((key * ytol, toks))

    out = []
    for li, (y, toks) in enumerate(lines):
        # the span wording usually sits on the 1-3 lines above the dates
        ctx = " ".join(w for _lj, ts in lines[max(0, li - 3):li + 1] for _a, _b, w in ts)
        for i, (x0, x1, w) in enumerate(toks):
            qe = None
            m = NUMERIC.match(w.strip().rstrip(","))
            if m:
                qe = _norm(*m.groups())
            else:
                dm = DAYMON.match(w.strip().rstrip(","))
                if dm and i + 2 < len(toks):
                    mon = toks[i + 1][2].strip(",.").lower()[:3]
                    yr = YEAR.match(toks[i + 2][2].strip(",."))
                    if mon in MONTHS and yr:
                        qe = _norm(dm.group(1), MONTHS[mon], yr.group(1))
                if qe is None and w.strip(",.").lower()[:3] in MONTHS and i + 2 < len(toks):
                    dd = DAYMON.match(toks[i + 1][2].strip(","))
                    yr = YEAR.match(toks[i + 2][2].strip(",."))
                    if dd and yr:
                        qe = _norm(dd.group(1), MONTHS[w.strip(",.").lower()[:3]], yr.group(1))
            if qe:
                out.append((qe, x1, y, ctx))
    return out


def quarter_columns(page):
    """-> {qe: x_right} for columns that are a SINGLE QUARTER.

    A cumulative column ending on the same date is dropped rather than competing, because taking it
    silently converts a nine-month figure into 'the quarter' -- the defect class this campaign
    healed 58 of.
    """
    best = {}
    for qe, x, _y, ctx in dates_on(page):
        if CUMULATIVE.search(ctx) and not QUARTERLY.search(ctx):
            continue
        # a date seen more than once at different x: keep the leftmost (statements print the
        # current quarter first, cumulative blocks to its right)
        if qe not in best or x < best[qe]:
            best[qe] = x
    return best


BASIS_CON = re.compile(r"c[o0]ns[o0]lidated", re.I)
BASIS_NOT_CON = re.compile(r"n[o0]n[\s-]*c[o0]ns[o0]lidated|un[\s-]*c[o0]ns[o0]lidated|"
                           r"standal[o0]ne", re.I)


def page_basis(page):
    """'con' / 'std' / None from the page's own wording. Glyph-tolerant (§51: a->o, t->l)."""
    t = page.get_text()[:1500]
    has_con = bool(BASIS_CON.search(t))
    has_not = bool(BASIS_NOT_CON.search(t))
    if has_con and not has_not:
        return "con"
    if has_not and not has_con:
        return "std"
    if has_con and has_not:
        # both words present: a combined statement, or "Standalone and Consolidated" cover text.
        return None
    return None
