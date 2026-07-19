# -*- coding: utf-8 -*-
"""Fetch NSE corporate announcements (equities, ALL symbols) for a rolling window and write
docs/announcements.json for the Announcements page. Reuses build_fundamentals' CI-proven NSE
session (plain urllib + Chrome UA + cookie warmup — same as the daily fundamentals cron).

- Window: last WINDOW_DAYS days, fetched in CHUNK_DAYS chunks (small chunks keep each
  response modest and let one bad chunk fail without losing the run).
- Self-healing: merges with the existing file (a failed chunk today keeps yesterday's rows),
  then trims to the window. Refuses to overwrite good data if the result looks broken (<MIN_ROWS).
- Output schema (compact arrays):
  {"updated","from","to","rows":[[symbol, company, "YYYY-MM-DD HH:MM:SS", category, caption, file], ...]}
  `file` is the attachment with the common "https://nsearchives.nseindia.com/corporate/" prefix
  stripped (the page re-adds it); other/absolute URLs are kept as-is.

Run: python -X utf8 scripts/fetch_announcements.py
"""
import os, sys, json, datetime, re
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import build_fundamentals as B  # _get / nse_jar / UA

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "..", "docs", "announcements.json")
WINDOW_DAYS = 31
CHUNK_DAYS = 7
INDICES = ("equities", "sme")   # mainboard + SME/Emerge — SME board carries thin-name result filings
MIN_ROWS = 200          # a 31-day window always has thousands; fewer = broken fetch, keep old file
CAPTION_MAX = 500
PDF_PREFIX = "https://nsearchives.nseindia.com/corporate/"
MON = {"jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,"jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12}

def ddmmyyyy(d): return "%02d-%02d-%04d" % (d.day, d.month, d.year)

def parse_dt(rec):
    """an_dt '11-Jul-2026 18:07:44' or sort_date '2026-07-11 18:07:44' -> ISO 'YYYY-MM-DD HH:MM:SS'."""
    s = str(rec.get("sort_date") or "")
    m = re.match(r"(\d{4}-\d{2}-\d{2})[ T]?(\d{2}:\d{2}(:\d{2})?)?", s)
    if m: return m.group(1) + " " + ((m.group(2) or "00:00") + ":00")[:8]
    s = str(rec.get("an_dt") or "")
    m = re.match(r"(\d{2})-([A-Za-z]{3})-(\d{4})\s*(\d{2}:\d{2}(:\d{2})?)?", s)
    if not m: return None
    mo = MON.get(m.group(2).lower())
    if not mo: return None
    return "%s-%02d-%s %s" % (m.group(3), mo, m.group(1), ((m.group(4) or "00:00") + ":00")[:8])

def key_of(r): return "|".join((r[0], r[2], r[3], r[5]))

def main():
    today = datetime.date.today()
    start = today - datetime.timedelta(days=WINDOW_DAYS - 1)
    jar = B.nse_jar()
    hdr = {"User-Agent": B.UA, "Accept": "application/json, text/plain, */*",
           "Referer": "https://www.nseindia.com/companies-listing/corporate-filings-announcements"}
    rows, errs = {}, 0
    d = start
    while d <= today:
        e = min(d + datetime.timedelta(days=CHUNK_DAYS - 1), today)
        # NSE files mainboard (index=equities) and SME/Emerge (index=sme) announcements on separate
        # boards — query BOTH or every SME result filing (VINEETLAB, KARNIKA, …) is invisible to us
        # and the Trendlyne count reads permanently higher. See DATA_RUNBOOK §14/§31.
        for idx in INDICES:
            url = ("https://www.nseindia.com/api/corporate-announcements?index=%s"
                   "&from_date=%s&to_date=%s" % (idx, ddmmyyyy(d), ddmmyyyy(e)))
            try:
                j = json.loads(B._get(url, headers=hdr, jar=jar, timeout=90))
            except Exception as ex:
                print("ERR chunk %s..%s [%s]: %s" % (d, e, idx, ex)); errs += 1; j = []
            n = 0
            for rec in (j if isinstance(j, list) else []):
                sym = str(rec.get("symbol") or "").strip().upper()
                dt = parse_dt(rec)
                if not sym or not dt: continue
                cat = re.sub(r"\s+", " ", str(rec.get("desc") or "")).strip() or "Others"
                cap = re.sub(r"\s+", " ", str(rec.get("attchmntText") or "")).strip()
                if len(cap) > CAPTION_MAX: cap = cap[:CAPTION_MAX - 1].rstrip() + "…"
                f = str(rec.get("attchmntFile") or "").strip()
                if f.startswith(PDF_PREFIX): f = f[len(PDF_PREFIX):]
                co = re.sub(r"\s+", " ", str(rec.get("sm_name") or "")).strip()
                r = [sym, co, dt, cat, cap, f]
                rows[key_of(r)] = r; n += 1
            print("chunk %s..%s [%s] -> %d recs" % (d, e, idx, n))
        d = e + datetime.timedelta(days=1)

    fresh = len(rows)
    # merge yesterday's file so a partially-failed fetch never loses rows
    if os.path.exists(OUT):
        try:
            for r in json.load(open(OUT, encoding="utf-8")).get("rows", []):
                if isinstance(r, list) and len(r) == 6: rows.setdefault(key_of(r), r)
        except Exception as ex:
            print("WARN old file unreadable:", ex)

    lo = start.isoformat()
    allrows = [r for r in rows.values() if r[2][:10] >= lo]
    allrows.sort(key=lambda r: (r[2], r[0]), reverse=True)
    if len(allrows) < MIN_ROWS:
        print("ABORT: only %d rows (fresh %d, errs %d) — keeping existing file" % (len(allrows), fresh, errs))
        sys.exit(1)

    ist = datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)
    out = {"updated": ist.strftime("%Y-%m-%d %H:%M IST"), "from": lo, "to": today.isoformat(), "rows": allrows}
    json.dump(out, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, separators=(",", ":"))

    cats = {}
    for r in allrows: cats[r[3]] = cats.get(r[3], 0) + 1
    print("WROTE %s: %d rows (%d fresh), %d categories, %.1f MB" %
          (os.path.normpath(OUT), len(allrows), fresh, len(cats), os.path.getsize(OUT) / 1e6))
    for c, n in sorted(cats.items(), key=lambda kv: -kv[1])[:40]:
        print("  %5d  %s" % (n, c))

    write_results_feed(allrows)

# --- Results feed (docs/results_feed.json) — the tiny slice the Quarterly Results page polls ---
RESULT_CAP_RE = re.compile(r"financial\s+results?\s+for\s+the\s+(?:period|quarter|year)|"
                           r"submitted.{0,40}financial\s+results?", re.I)
RESULT_CAT_RE = re.compile(r"^financial\s+result", re.I)
# ⚠️ A results filing in Jul/Aug/Sep is often a LATE March (Q4/annual) result, not the current June
# quarter — so read the reporting period per filing and ANCHOR on an "ended" clause (never assume the
# current season). Snap to a quarter-end month; 0 (no badge) when the period isn't stated.
_DAY_LAST = {3: 31, 6: 30, 9: 30, 12: 31}
_ENDED_RE = re.compile(r"end(?:ed|ing)\s+(?:on\s+)?(.{0,30}?\d{4})", re.I)
_DMY_RE = re.compile(r"(\d{1,2})(?:st|nd|rd|th)?[\s,]+([A-Za-z]{3,9})[,\s]+(\d{4})", re.I)
_MDY_RE = re.compile(r"([A-Za-z]{3,9})\s+(\d{1,2}),?\s+(\d{4})", re.I)
_NUM_RE = re.compile(r"(\d{1,2})[.\-/](\d{1,2})[.\-/](\d{4})")

def _qe_mk(mo, y): return (y * 10000 + mo * 100 + _DAY_LAST[mo]) if mo in _DAY_LAST else 0

def parse_qe(*texts):
    h = " ".join(str(t or "") for t in texts)
    for mm in _ENDED_RE.finditer(h):
        seg = mm.group(1)
        m = _DMY_RE.search(seg)
        if m:
            qe = _qe_mk(MON.get(m.group(2).lower()[:3], 0), int(m.group(3)))
            if qe: return qe
        m = _MDY_RE.search(seg)
        if m:
            qe = _qe_mk(MON.get(m.group(1).lower()[:3], 0), int(m.group(3)))
            if qe: return qe
        m = _NUM_RE.search(seg)
        if m:
            qe = _qe_mk(int(m.group(2)), int(m.group(3)))
            if qe: return qe
    return 0

def write_results_feed(allrows):
    """Slice results-filing announcements into a small standalone JSON (same schema, + parsed
    quarter-end when the caption states it). Kept tiny so the page can poll it cheaply.

    ⚠️ This rebuilds the NSE rows from scratch. BSE rows are added AFTERWARD by fetch_bse_results.py,
    so we must PRESERVE the existing BSE rows here — else, on any run where the (flaky) BSE fetch fails,
    the committed feed would silently lose every BSE company. A BSE row is identified by its filing URL
    (bseindia.com). De-dup NSE vs preserved by (symbol, date)."""
    outp = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "docs", "results_feed.json")
    try:
        prev = json.load(open(outp, encoding="utf-8")).get("rows", [])
    except Exception:
        prev = []
    kept_bse = [r for r in prev if isinstance(r, list) and len(r) >= 6 and "bseindia.com" in str(r[5])]
    # Quarter corrections: NSE sometimes captions a late Q4/annual filing as the current quarter. The
    # daily vision-prep reads the filing PDF's real period and records "SYM|YYYY-MM-DD" -> real qe here.
    try:
        qfix = json.load(open(os.path.join(os.path.dirname(outp), "feed_qe_fix.json"), encoding="utf-8"))
    except Exception:
        qfix = {}

    feed = []
    for r in allrows:
        cat, cap = r[3], r[4] or ""
        if not (RESULT_CAT_RE.search(cat) or
                (cat == "Outcome of Board Meeting" and RESULT_CAP_RE.search(cap))):
            continue
        qe = qfix.get("%s|%s" % (r[0], str(r[2])[:10])) or parse_qe(cap)
        feed.append([r[0], r[1], r[2], qe, (cap[:220] + "…") if len(cap) > 221 else cap, r[5]])
    have = set((r[0], r[2][:10]) for r in feed)
    for r in kept_bse:                                   # re-add surviving BSE rows the NSE set lacks
        if (r[0], r[2][:10]) not in have: feed.append(r)
    # trim to a rolling 31-day window (matches fetch_bse_results) so preserved BSE rows don't accrete
    cut = (datetime.date.today() - datetime.timedelta(days=31)).isoformat()
    feed = [r for r in feed if r[2][:10] >= cut]
    feed.sort(key=lambda r: (r[2], r[0]), reverse=True)
    ist = datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)
    json.dump({"updated": ist.strftime("%Y-%m-%d %H:%M IST"), "rows": feed},
              open(outp, "w", encoding="utf-8"), ensure_ascii=False, separators=(",", ":"))
    print("WROTE %s: %d results-feed rows (%d preserved BSE)" % (os.path.normpath(outp), len(feed), len(kept_bse)))

if __name__ == "__main__":
    main()
