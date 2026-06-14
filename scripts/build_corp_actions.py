# -*- coding: utf-8 -*-
"""Fetch NSE's OFFICIAL split/bonus ratios and write scripts/corp_actions.json
({SYMBOL: [[exYmd, factor], ...]}). The price builds (build_sf_data / update_sf_data)
apply these EXACT factors on each ex-date instead of inferring the factor from the
overnight price drop — which silently breaks whenever a stock moves on the ex-date
(e.g. Adani Power's 1:5 split read as 1:4 after a +20% ex-date pop) or when a small
bonus (1:4 / 1:5 / 1:10) only dips the price <25% and gets ignored entirely.

Cheap (≈9 API calls) — safe to run daily before update_sf_data.py.
Run: python -X utf8 build_corp_actions.py
"""
import os, re, json, datetime
import build_fundamentals as F

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "corp_actions.json")


def official_factor(subj):
    """Return (price_factor, label) for a split/bonus subject, else (None, None).
    Split: face value Rs X -> Rs Y  => factor Y/X.   Bonus B:A (B new per A held) => A/(A+B)."""
    s = (subj or "").lower()
    m = re.search(r'from\s*(?:rs\.?\s*)?([\d.]+).*?to\s*(?:rs\.?\s*)?([\d.]+)', s)
    if m and ("split" in s or "sub-division" in s or "sub division" in s):
        x, y = float(m.group(1)), float(m.group(2))
        if x and 0 < y < x:
            return y / x, "split (FV %s->%s)" % (m.group(1), m.group(2))
    m = re.search(r'bonus[^0-9]*(\d+)\s*:\s*(\d+)', s)
    if m:
        b, a = int(m.group(1)), int(m.group(2))
        if a + b and b <= 50 and a <= 250:           # ignore absurd parses (stray digits)
            return a / (a + b), "bonus %d:%d" % (b, a)
    return None, None


def fetch():
    jar = F.nse_jar()
    h = {"User-Agent": F.UA, "Accept": "application/json", "Referer": "https://www.nseindia.com/"}
    cmap = {}
    for yr in range(2016, datetime.date.today().year + 1):
        url = ("https://www.nseindia.com/api/corporates-corporateActions?index=equities"
               "&from_date=01-01-%d&to_date=31-12-%d" % (yr, yr))
        try:
            d = json.loads(F._get(url, headers=h, jar=jar, timeout=40))
            rows = d if isinstance(d, list) else d.get("data", [])
        except Exception as e:
            print("  %d: fetch failed (%s)" % (yr, str(e)[:40])); continue
        n = 0
        for r in rows:
            f, _ = official_factor(r.get("subject") or r.get("purpose") or "")
            if f and 0.05 < f < 0.95:
                ex = F.iso(r.get("exDate"))
                if ex:
                    cmap.setdefault(r.get("symbol"), {})[int(ex)] = round(f, 6)
                    n += 1
        print("  %d: %d split/bonus events" % (yr, n))
    return cmap


def main():
    cmap = fetch()
    # dict-per-symbol -> sorted list, dedup on ex-date
    out = {sym: sorted([k, v] for k, v in d.items()) for sym, d in cmap.items()}
    tmp = OUT + ".tmp"
    json.dump(out, open(tmp, "w"))
    os.replace(tmp, OUT)
    print("Wrote %s: %d symbols, %d events" % (OUT, len(out), sum(len(v) for v in out.values())))


if __name__ == "__main__":
    main()
