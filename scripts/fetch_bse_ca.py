# -*- coding: utf-8 -*-
"""BSE corporate actions (bonus + split) for the BSE-only universe → scripts/bse_ca.json  (runbook §228d, 2026-10-13).

WHY. docs/bse_prices.bin keeps BSE-only closes exactly as BSE printed them, and BSE's bhavcopy PREVCLOSE is never
ex-adjusted (build_bse_sme_backfill.adjust_series, measured 2026-09-23), so a bonus or a split shows on every BSE-only
chart as a cliff (ALSTONE 13-Dec-2022 169.85 → 14-Dec-2022 1.75: bonus 9:1 AND split 10→1 the same day). NSE names get
their adjustment from the engine; BSE-only names got none (user 2026-10-13: "add the bonus/split adjustment layer for
bse-only charts too"). This file is the announced-action source build_bse_slices.py adjusts from — an action is applied
there only when the tape confirms it on the ex-date (§149 crash-vs-CA rule: never divide out a fall nobody announced).

SOURCE. api.bseindia.com/BseIndiaAPI/api/CorporateAction/w?scripcode=<code> → Table2 rows {purpose_code, Ex_date,
purpose}: 'BN ' "Bonus issue A:B" (factor (A+B)/B), 'SS ' "Stock Split From Rs.X/- to Rs.Y/-" (factor X/Y). Dividends
('DP', §174), rights ('ET', §206: manual ledger), buybacks ('BGM') and meetings are not actions on the price series here.
Table1 lists bonuses only (ALSTONE's split sat in Table2 alone) — Table2 is read, Table1 ignored.

STORE. {"updated", "co": {code: {"chk": YYYYMMDD, "acts": [[ex YYYYMMDD, "bonus"|"split", factor, text], …]}}}.
A scrip is re-listed after RECHECK_DAYS (30) — new actions keep appearing; --budget N calls per run (default 300), the
oldest checks first; --codes C1,C2 forces those scrips (any age). Plain urllib with the shared browser header set
(bse_fetch, §181), one request at a time, 0.7 s apart; a refusal (403/HTML) stops the run and writes what it has.

Run: python3 -X utf8 scripts/fetch_bse_ca.py [--budget N] [--codes C1,C2] [--out PATH]
"""
import os, sys, re, json, time, datetime
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import bse_headers as BH  # noqa: F401  (§181 BSE headers)
import bse_fetch as B

DOCS = os.path.join(HERE, "..", "docs")
OUT = os.path.join(HERE, "bse_ca.json")
URL = "https://api.bseindia.com/BseIndiaAPI/api/CorporateAction/w?scripcode=%s"
RECHECK_DAYS = 30
MONTHS = {m: i for i, m in enumerate(("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"), 1)}
RE_BONUS = re.compile(r"bonus\s*(?:issue)?\s*(\d+(?:\.\d+)?)\s*:\s*(\d+(?:\.\d+)?)", re.I)
RE_SPLIT = re.compile(r"split\s*from\s*(?:rs|re)\.?\s*(\d+(?:\.\d+)?)\s*/?-?\s*to\s*(?:rs|re)\.?\s*(\d+(?:\.\d+)?)", re.I)


def ymd(s):
    """'14 Dec 2022' → 20221214 (0 when unreadable)."""
    m = re.match(r"\s*(\d{1,2})\s+([A-Za-z]{3})[a-z]*\s+(\d{4})", s or "")
    if not m or m.group(2).lower() not in MONTHS:
        return 0
    return int(m.group(3)) * 10000 + MONTHS[m.group(2).lower()] * 100 + int(m.group(1))


def parse_actions(j):
    """[[ex, type, factor, text], …] from one CorporateAction response — bonus and split only, ex-date required."""
    out = []
    for r in (j.get("Table2") or []):
        pc = (r.get("purpose_code") or "").strip().upper()
        txt = re.sub(r"\s+", " ", (r.get("purpose") or "").strip())
        ex = ymd(r.get("Ex_date") or "")
        if not ex:
            continue
        if pc == "BN":
            m = RE_BONUS.search(txt)
            if m and float(m.group(2)) > 0:
                a, b = float(m.group(1)), float(m.group(2))
                out.append([ex, "bonus", round((a + b) / b, 6), txt])
        elif pc == "SS":
            m = RE_SPLIT.search(txt)
            if m and float(m.group(2)) > 0 and float(m.group(1)) > float(m.group(2)):
                out.append([ex, "split", round(float(m.group(1)) / float(m.group(2)), 6), txt])
    out.sort()
    return out


def load():
    try:
        return json.load(open(OUT, encoding="utf-8"))
    except (OSError, ValueError):
        return {"updated": "", "co": {}}


def main():
    a = sys.argv[1:]
    arg = lambda k, d=None: a[a.index(k) + 1] if k in a else d
    budget = int(arg("--budget", 300)); out_p = arg("--out", OUT)
    forced = set((arg("--codes") or "").split(",")) - {""}
    today = datetime.date.today(); tday = int(today.strftime("%Y%m%d"))
    stale = int((today - datetime.timedelta(days=RECHECK_DAYS)).strftime("%Y%m%d"))
    st = load(); co = st.setdefault("co", {})
    codes = {str(r[0]) for r in json.load(open(os.path.join(DOCS, "bse_universe.json"), encoding="utf-8"))["rows"]}
    try:
        import gzip
        codes |= set(json.loads(gzip.decompress(open(os.path.join(DOCS, "bse_prices.bin"), "rb").read()))["px"])
    except (OSError, ValueError):
        pass
    todo = sorted(forced) if forced else sorted((c for c in codes if int((co.get(c) or {}).get("chk") or 0) < stale),
                                                 key=lambda c: int((co.get(c) or {}).get("chk") or 0))[:budget]
    op = B.session(); time.sleep(1)
    done = acts = 0; refused = None
    for code in todo:
        try:
            raw = B.get(op, URL % code)
            j = json.loads(raw if isinstance(raw, str) else raw.decode("utf8", "ignore"))
        except Exception as e:
            msg = str(e)
            if "403" in msg or "429" in msg or "Access Denied" in msg or "HTML" in msg or "blocked" in msg.lower():
                refused = msg[:80]; break                     # BSE refused this client: stop, keep what was fetched
            print("  %s: %s" % (code, msg[:80])); time.sleep(1.5); continue
        if not isinstance(j, dict):
            print("  %s: not a JSON object" % code); continue
        al = parse_actions(j)
        co[code] = {"chk": tday, "acts": al}
        done += 1; acts += len(al)
        time.sleep(0.7)
    st["updated"] = (datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:%M IST")
    json.dump(st, open(out_p, "w", encoding="utf-8"), separators=(",", ":"), sort_keys=True)
    print("bse_ca: %d scrips checked this run (%d actions), %d scrips in the store, %d with actions → %s%s"
          % (done, acts, len(co), sum(1 for v in co.values() if v.get("acts")), os.path.relpath(out_p),
             ("  REFUSED: " + refused) if refused else ""))


if __name__ == "__main__":
    main()
