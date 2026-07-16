# -*- coding: utf-8 -*-
"""Fetch the NSE CORPORATE-ACTIONS calendar (ex-dates for dividends, bonuses, splits,
rights, buybacks) -> docs/actions.json for the Ex-Dates Calendar page.

SOURCE (urllib + cookie warmup — the announcements cron's CI-proven session):
  /api/corporates-corporateActions?index=equities&from_date=DD-MM-YYYY&to_date=DD-MM-YYYY
  fields: symbol, comp, series, subject ("Dividend - Rs 2 Per Share"), exDate, recDate, ...
  No params = only ~today's ex-dates, so ALWAYS pass the window (PAST_DAYS back for the
  "recent" view + FWD_DAYS forward for the calendar). STATELESS rebuild each run.

Enrichment: dividend amount parsed from the subject; yield% = amount / latest close
(dash_slim meta — ⚠️ '.NS'-keyed, re-key by bare symbol, runbook §26).

Output docs/actions.json:
  {"updated","from","to",
   "rows":[[exISO, sym, name, kind, subject, recISO|null, divAmt|null, yieldPct|null],...]}
  kind: D dividend | B bonus | S split | R rights | BB buyback | O other (AGM/EGM etc.)

Run: python -X utf8 scripts/fetch_actions.py
"""
import os, sys, json, gzip, re, datetime
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import build_fundamentals as B  # _get / nse_jar / UA

HERE = os.path.dirname(os.path.abspath(__file__))
DOCS = os.path.join(HERE, "..", "docs")
OUT = os.path.join(DOCS, "actions.json")
SLIM = os.path.join(DOCS, "dash_slim.bin")
PAST_DAYS, FWD_DAYS = 30, 75
MIN_ROWS = 10
MON = {"jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,"jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12}

def iso(s):
    m = re.match(r"^(\d{1,2})-([A-Za-z]{3})-(\d{4})", str(s or "").strip())
    if not m: return None
    mo = MON.get(m.group(2).lower())
    return "%s-%02d-%02d" % (m.group(3), mo, int(m.group(1))) if mo else None

def kind_of(subject):
    s = str(subject or "").lower()
    if "dividend" in s: return "D"
    if "bonus" in s: return "B"
    if "split" in s or "sub-division" in s or "subdivision" in s or "sub division" in s: return "S"
    if "right" in s: return "R"
    if "buy back" in s or "buyback" in s or "buy-back" in s: return "BB"
    return "O"

def div_amt(subject):
    m = re.search(r"r[se]\.?\s*([\d]+(?:\.\d+)?)", str(subject or "").lower())
    return float(m.group(1)) if m else None

def main():
    today = datetime.date.today()
    f = today - datetime.timedelta(days=PAST_DAYS)
    t = today + datetime.timedelta(days=FWD_DAYS)
    jar = B.nse_jar()
    hdr = {"User-Agent": B.UA, "Accept": "application/json, text/plain, */*",
           "Referer": "https://www.nseindia.com/companies-listing/corporate-filings-actions"}
    url = ("https://www.nseindia.com/api/corporates-corporateActions?index=equities"
           "&from_date=%02d-%02d-%04d&to_date=%02d-%02d-%04d" %
           (f.day, f.month, f.year, t.day, t.month, t.year))
    try:
        j = json.loads(B._get(url, headers=hdr, jar=jar, timeout=90))
    except Exception as ex:
        print("fetch FAILED (%s) — keeping the previous file" % ex, flush=True)
        sys.exit(1)
    data = j if isinstance(j, list) else (j.get("data") or [])
    if len(data) < MIN_ROWS:
        print("suspiciously few rows (%d) — keeping the previous file" % len(data), flush=True)
        sys.exit(1)

    meta = {}
    try:
        raw = json.loads(gzip.decompress(open(SLIM, "rb").read())).get("meta") or {}
        meta = {(v.get("symbol") or k.split(".")[0]).upper(): v for k, v in raw.items()}
    except Exception:
        print("WARN: dash_slim.bin unreadable — no yield enrichment", flush=True)

    rows, seen = [], set()
    for r in data:
        ex = iso(r.get("exDate"))
        sym = str(r.get("symbol") or "").strip().upper()
        subject = " ".join(str(r.get("subject") or "").split())
        if not (ex and sym and subject): continue
        key = (ex, sym, subject.lower())
        if key in seen: continue
        seen.add(key)
        k = kind_of(subject)
        amt = div_amt(subject) if k == "D" else None
        px = (meta.get(sym) or {}).get("latest")
        yld = round(amt / px * 100, 2) if (amt and px) else None
        rows.append([ex, sym, str(r.get("comp") or "").strip(), k, subject,
                     iso(r.get("recDate")), amt, yld])
    rows.sort(key=lambda r: (r[0], r[1]))

    ist = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=5, minutes=30)
    with open(OUT, "w", encoding="utf-8") as fo:
        json.dump({"updated": ist.strftime("%Y-%m-%d %H:%M"), "from": f.isoformat(),
                   "to": t.isoformat(), "rows": rows}, fo, separators=(",", ":"), ensure_ascii=False)
    n_fut = sum(1 for r in rows if r[0] >= today.isoformat())
    import collections
    kinds = collections.Counter(r[3] for r in rows)
    print("Wrote %s (%.0f KB): %d actions (%d upcoming) %s..%s | kinds %s" %
          (OUT, os.path.getsize(OUT) / 1024.0, len(rows), n_fut, f, t, dict(kinds)), flush=True)
    for r in [r for r in rows if r[0] >= today.isoformat()][:5]:
        print("  %s %s %s %s%s" % (r[0], r[1], r[4][:40], ("yield %.1f%%" % r[7]) if r[7] else "", ""), flush=True)

if __name__ == "__main__":
    main()
