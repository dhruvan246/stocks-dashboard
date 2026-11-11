# -*- coding: utf-8 -*-
"""
Daily FII (FPI) equity buy / sell / net from NSDL, 2020-01-01 -> today, for the
fii-dii.html daily table's older rows (user, 2026-11-11: "add 2020 -> now").

Source: NSDL "Daily Trends in FPI Investments" archive (public, no key)
  https://www.fpi.nsdl.co.in/web/Reports/Archive.aspx
  One POST per date returns every reporting day of that month so far, plus the
  month's "Total for <Month>" row. We keep only Equity -> Stock Exchange
  (gross purchases, gross sales, net), i.e. FPI trades on the exchanges;
  primary market, debt and hybrid are left out.

What these numbers are (shown on the page):
  - custodian-reported FPI trades. NOT the exchanges' same-day "provisional" FII
    figures that docs/fii_dii.json carries; the two series differ day to day.
  - NSDL reports the trades of the PREVIOUS trading day(s). Each row is placed on
    the NSE trading day(s) it covers: every NSE session (docs/nifty.json) from the
    previous report date up to the day before this report date. When NSDL is shut
    on a trading day, the next report covers two sessions; the row then carries
    both dates and sits on the later one.
  - no DII figures (NSDL reports foreign investors only).

Checks (the file is not written when any fails):
  - for every month fetched, the daily rows add up to NSDL's own
    "Total for <Month>" row (buy, sell and net, to the paisa)
  - every row maps to at least one NSE session; a row whose sessions are not yet
    in nifty.json (calendar lag) is left for the next run, never guessed

Output docs/fii_nsdl_daily.json:
  {"updated", "source", "rows": [[tradeDate, buy, sell, net, reportDate(, [dates covered])], ...]}

Run:
  python3 scripts/fetch_fii_nsdl.py            # top-up: re-reads the last 2 months + this one
  python3 scripts/fetch_fii_nsdl.py --full     # rebuild Dec-2019 -> today
"""
import os, re, sys, json, time, html, datetime, calendar
import urllib.request, urllib.parse, http.cookiejar

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
OUT = os.path.join(ROOT, "docs", "fii_nsdl_daily.json")
NIFTY = os.path.join(ROOT, "docs", "nifty.json")
URL = "https://www.fpi.nsdl.co.in/web/Reports/Archive.aspx"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
START = "2020-01-01"

_jar = http.cookiejar.CookieJar()
_op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(_jar))
_form = {}


def _req(data=None):
    r = urllib.request.Request(URL, data=data, headers={"User-Agent": UA, "Referer": URL})
    return _op.open(r, timeout=60).read().decode("utf-8", "replace")


def _fields(h):
    return {n: html.unescape(v) for n, v in
            re.findall(r'name="(__VIEWSTATE|__VIEWSTATEGENERATOR|__EVENTVALIDATION)"[^>]*value="([^"]*)"', h)}


def _num(s):
    s = s.strip()
    neg = s.startswith("(")
    v = float(s.strip("()").replace(",", ""))
    return -v if neg else v


def fetch_month(day):
    """Report 'up to <day>' -> (list of (reportDate, buy, sell, net), monthTotal (buy, sell, net) or None)."""
    global _form
    ds = day.strftime("%d-%b-%Y")
    last = None
    for attempt in range(4):
        try:
            if not _form:
                _form = _fields(_req())
            p = dict(_form)
            p.update({"__EVENTTARGET": "btnSubmit1", "__EVENTARGUMENT": "", "txtDate": ds,
                      "hdnDate": ds, "hdnFlag": "", "HdnValexceldata": ""})
            h = _req(urllib.parse.urlencode(p).encode())
            _form = _fields(h) or _form
            break
        except Exception as e:
            last = e
            _form = {}
            time.sleep(3 * (attempt + 1))
    else:
        raise RuntimeError("NSDL fetch failed for %s: %s" % (ds, last))
    t = re.sub(r"<[^>]+>", "|", h)
    t = re.sub(r"\s+", " ", t)
    t = re.sub(r"(\| ?)+", "|", t)
    if "Daily Trends in FPI Investments" not in t:
        raise RuntimeError("NSDL page for %s has no report table" % ds)
    daily_part, _, rest = t.partition("Total for")
    rows = []
    for m in re.finditer(r"\|(\d\d-\w{3}-\d{4})\|Equity\|Stock Exchange\|([\d.,()]+)\|([\d.,()]+)\|([\d.,()]+)\|", daily_part):
        rd = datetime.datetime.strptime(m.group(1), "%d-%b-%Y").date().isoformat()
        rows.append((rd, _num(m.group(2)), _num(m.group(3)), _num(m.group(4))))
    tot = None
    m = re.match(r" ?\w+\|Equity\|Stock Exchange\|([\d.,()]+)\|([\d.,()]+)\|([\d.,()]+)\|", rest)
    if m:
        tot = (_num(m.group(1)), _num(m.group(2)), _num(m.group(3)))
    return rows, tot


def month_ends(first, last):
    y, mo = first.year, first.month
    while (y, mo) <= (last.year, last.month):
        d = datetime.date(y, mo, calendar.monthrange(y, mo)[1])
        yield min(d, last)
        y, mo = (y + 1, 1) if mo == 12 else (y, mo + 1)


def main():
    full = "--full" in sys.argv
    today = datetime.date.today()
    old = {}
    if os.path.exists(OUT) and not full:
        prev = json.load(open(OUT, encoding="utf-8"))
        for r in prev["rows"]:
            old[r[4]] = (r[4], r[1], r[2], r[3])           # keyed by report date
        if prev.get("anchor"):                              # report before the first kept row bounds its sessions
            old[prev["anchor"]] = (prev["anchor"], 0, 0, 0)
    if full or not old:
        first = datetime.date(2019, 12, 1)                  # Dec-2019: the report before 2-Jan-2020 places it
    else:
        m = today.month - 2
        first = datetime.date(today.year + (m - 1) // 12, (m - 1) % 12 + 1, 1)

    reports = dict(old)
    problems = []
    for d in month_ends(first, today):
        rows, tot = fetch_month(d)
        if rows:
            if tot is None:
                problems.append("%s: no 'Total for' row" % d)
            else:
                s = [round(sum(r[i] for r in rows), 2) for i in (1, 2, 3)]
                if any(abs(a - b) > 0.05 for a, b in zip(s, tot)):
                    problems.append("%s: daily sum %s != NSDL month total %s" % (d.strftime("%b-%Y"), s, tot))
        ym = d.strftime("%Y-%m")
        for k in [k for k in reports if k[:7] == ym]:        # this month is re-read whole
            del reports[k]
        for r in rows:
            reports[r[0]] = r
        print("  %s: %d reporting days" % (d.strftime("%b-%Y"), len(rows)), flush=True)
        time.sleep(0.7)
    if problems:
        print("REFUSING to write:\n  " + "\n  ".join(problems))
        sys.exit(1)

    sessions = sorted(json.load(open(NIFTY, encoding="utf-8"))["px"])
    rep = sorted(reports.values())
    out, held, anchor = [], 0, None
    for i, (rd, b, s, n) in enumerate(rep):
        if i == 0:
            continue                                        # no earlier report to bound its sessions
        cov = [x for x in sessions if rep[i - 1][0] <= x < rd]
        if not cov:
            held += 1                                       # calendar not there yet -> next run
            continue
        if cov[-1] < START:
            anchor = rd
            continue
        row = [cov[-1], b, s, n, rd]
        if len(cov) > 1:
            row.append(cov)
        out.append(row)
    if held > 1:
        print("REFUSING to write: %d reports map to no NSE session (nifty.json gap?)" % held)
        sys.exit(1)
    if len(out) < 100:
        print("REFUSING to write: only %d rows" % len(out))
        sys.exit(1)
    reps = [r[0] for r in rep]
    anchor = reps[reps.index(out[0][4]) - 1]               # the report that bounds the first row's sessions
    doc = {"updated": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
           "source": "NSDL Daily Trends in FPI Investments, Equity / Stock Exchange (" + URL + ")",
           "anchor": anchor,
           "rows": out}
    with open(OUT, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, separators=(",", ":"))
    print("wrote %d rows %s -> %s (%d multi-session)" % (len(out), out[0][0], out[-1][0],
                                                        sum(1 for r in out if len(r) > 5)))


if __name__ == "__main__":
    main()
