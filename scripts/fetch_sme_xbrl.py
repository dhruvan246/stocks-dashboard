# -*- coding: utf-8 -*-
"""SME + long-period balance-sheet backfill for xbrl_extra (runbook §148).

WHY. The deep ledger held NO balance sheet for the NSE SME board and none from any filing whose
reporting period is longer than a quarter:
  * SME companies file HALF-YEARLY results in the NON-Ind-AS taxonomy (TangibleAssets, not
    PropertyPlantAndEquipment) — the builder knew only the Ind-AS names;
  * build_xbrl_extra.parse_file returned None for any OneD > 100 days, so 877 cached six-month
    INTEGRATED filings (481 companies, 172 of them SME) contributed nothing;
  * the SME filings from 2024 were never fetched at all (the nightly lists a 14-day window).
NSE lists SME XBRL only from the FY24 year-end results (filed Apr-2024); 2020-2023 SME rows carry
the placeholder link ".../corporate/xbrl/-" (measured 2026-09-23).

WHAT. Three steps, each resumable:
  --list    month-by-month SME results lists, 2024-01 → today, from BOTH endpoints
            (corporates-financial-results: the 2024 NONINDAS files; integrated-filing-results: the
            2025+ INTEGRATED files) → scripts/_sme_xbrl_list.json (gitignored)
  --fetch   download each listed XBRL into scripts/_xbrl_cache_sme/ (a SEPARATE cache: build_revop
            reads the main cache and trusts the context block, which in 2024 SME files says Jul-Sep
            for an Apr-Sep half — so these files must never reach it) and record every filename in
            scripts/xbrl_sme_files.json (tracked; build_xbrl_extra routes a listed Half-yearly /
            Yearly file to the BS-only parser)
  --merge   parse the SME cache + the main cache's long-period files with build_xbrl_extra and
            merge ONLY the BS-only rows, FILL-ONLY, into scripts/xbrl_extra.json.gz

Run from a worktree:
  XBRL_CACHE=/Users/dhruvan/stocks-dashboard/scripts/_xbrl_cache \
  SME_CACHE=/Users/dhruvan/stocks-dashboard/scripts/_xbrl_cache_sme \
  python3 -X utf8 scripts/fetch_sme_xbrl.py --list --fetch --merge [--dry]
"""
import os, sys, json, time, gzip, datetime, argparse
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
LIST = os.path.join(HERE, "_sme_xbrl_list.json")
LEDGER = os.path.join(HERE, "xbrl_sme_files.json")
SME_CACHE = os.environ.get("SME_CACHE") or os.path.join(HERE, "_xbrl_cache_sme")
GZ = os.path.join(HERE, "xbrl_extra.json.gz")
REF = "https://www.nseindia.com/companies-listing/corporate-filings-financial-results"
START = datetime.date(2024, 1, 1)


def canon(url):
    import re
    return re.sub(r"[^A-Za-z0-9]", "_", url.rsplit("/", 1)[-1])


class NSE:
    """curl_cffi Chrome session (the transport update_fundamentals / xtra_nightly already use)."""
    def __init__(self):
        self.s = None

    def get(self, url, timeout=120):
        from curl_cffi import requests as cr
        for attempt in range(4):
            try:
                if self.s is None:
                    self.s = cr.Session(impersonate="chrome")
                    self.s.get("https://www.nseindia.com/", timeout=30)
                    self.s.get(REF, timeout=30)
                r = self.s.get(url, headers={"Referer": REF}, timeout=timeout)
                if r.status_code == 200:
                    return r.text
                raise RuntimeError("HTTP %d" % r.status_code)
            except Exception as e:
                self.s = None
                print("   retry %d %s: %s" % (attempt + 1, url[-70:], str(e)[:70]), flush=True)
                time.sleep(5 * (attempt + 1))
        raise RuntimeError("gave up: " + url)


def months(a, b):
    d = a
    while d <= b:
        nxt = (d.replace(day=28) + datetime.timedelta(days=4)).replace(day=1)
        yield d, min(nxt - datetime.timedelta(days=1), b)
        d = nxt


def do_list(nse):
    rows = {}
    try:
        rows = {r["xbrl"]: r for r in json.load(open(LIST))}
    except (OSError, ValueError):
        pass
    today = datetime.date.today()
    for a, b in months(START, today):
        f, t = a.strftime("%d-%m-%Y"), b.strftime("%d-%m-%Y")
        got = 0
        for per in ("Half-Yearly", "Annual", "Quarterly"):
            u = ("https://www.nseindia.com/api/corporates-financial-results?index=sme&period=%s"
                 "&from_date=%s&to_date=%s" % (per, f, t))
            j = json.loads(nse.get(u))
            for r in (j if isinstance(j, list) else j.get("data", [])):
                xb = (r.get("xbrl") or "")
                if xb.lower().endswith(".xml"):
                    rows.setdefault(xb, {"xbrl": xb, "symbol": r.get("symbol"), "toDate": r.get("toDate"),
                                         "fromDate": r.get("fromDate"), "cons": r.get("consolidated"),
                                         "period": per, "src": "cfr", "bcast": r.get("broadCastDate")})
                    got += 1
            time.sleep(1.2)
        page = 1
        while True:
            u = ("https://www.nseindia.com/api/integrated-filing-results?index=sme&period=Quarterly"
                 "&from_date=%s&to_date=%s&page=%d&size=200" % (f, t, page))
            j = json.loads(nse.get(u))
            d = j.get("data", []) if isinstance(j, dict) else j
            for r in d or []:
                xb = (r.get("xbrl") or "")
                if xb.lower().endswith(".xml") and "governance" not in (r.get("type") or "").lower():
                    rows.setdefault(xb, {"xbrl": xb, "symbol": r.get("symbol"), "toDate": r.get("qe_Date") or r.get("toDate"),
                                         "period": r.get("period"), "src": "ifr", "bcast": r.get("broadcast_Date")})
                    got += 1
            total = (j.get("totalCount") or 0) if isinstance(j, dict) else 0
            if not d or page * 200 >= total or page > 50:
                break
            page += 1
            time.sleep(1.2)
        print("list %s..%s: +%d rows seen, %d unique so far" % (f, t, got, len(rows)), flush=True)
        json.dump(list(rows.values()), open(LIST, "w"))
    return list(rows.values())


def do_fetch(nse, rows):
    os.makedirs(SME_CACHE, exist_ok=True)
    have = set(os.listdir(SME_CACHE))
    try:
        ledger = set(json.load(open(LEDGER)))
    except (OSError, ValueError):
        ledger = set()
    new = fail = 0
    for i, r in enumerate(rows):
        fn = canon(r["xbrl"])
        ledger.add(fn)
        if fn in have:
            continue
        try:
            xml = nse.get(r["xbrl"], timeout=90)
        except Exception as e:
            fail += 1; print("  FAIL %s %s" % (r.get("symbol"), str(e)[:60])); continue
        if len(xml) < 500 or "<xbrli:xbrl" not in xml[:3000]:
            fail += 1; print("  BAD  %s %s (%d bytes)" % (r.get("symbol"), fn[:40], len(xml))); continue
        with open(os.path.join(SME_CACHE, fn), "w", encoding="utf-8") as fh:
            fh.write(xml)
        have.add(fn); new += 1
        if new % 100 == 0:
            print("  fetched %d (%d/%d listed, %d failed)" % (new, i + 1, len(rows), fail), flush=True)
            json.dump(sorted(ledger), open(LEDGER, "w"), indent=0)
        time.sleep(0.8)
    json.dump(sorted(ledger), open(LEDGER, "w"), indent=0)
    print("fetch: +%d files, %d failed, cache %d, ledger %d" % (new, fail, len(have), len(ledger)))


def do_merge(dry):
    import build_xbrl_extra as BX           # reads xbrl_sme_files.json at import — fetch first
    data = json.loads(gzip.decompress(open(GZ, "rb").read()))
    jobs = [(SME_CACHE, f) for f in os.listdir(SME_CACHE)] if os.path.isdir(SME_CACHE) else []
    main = BX.CACHE
    for f in os.listdir(main):           # the main cache's long-period files (INTEGRATED halves)
        if f.startswith("INTEGRATED"):
            jobs.append((main, f))
    jobs.sort(key=lambda j: BX.ts_key(j[1]))
    stats = {"files": 0, "bso": 0, "cells_new": 0, "fields_filled": 0, "fields_kept": 0}
    per_src = {}
    for d, f in jobs:
        try:
            r = BX.parse_file(os.path.join(d, f), f)
        except Exception:
            r = None
        stats["files"] += 1
        if not r or not r.get("bso"):
            continue
        stats["bso"] += 1
        cell = data.setdefault(r["sym"], {}).setdefault(str(r["qe"]), {})
        for b in ("s", "c"):
            if not r[b]:
                continue
            if b not in cell:
                stats["cells_new"] += 1
                per_src[d == main] = per_src.get(d == main, 0) + 1
            tgt = cell.setdefault(b, {})
            for k, v in r[b].items():
                if k in tgt:
                    stats["fields_kept"] += 1
                else:
                    tgt[k] = v; stats["fields_filled"] += 1
    print("merge: %s | new basis-cells from SME cache %d, from main-cache long files %d"
          % (stats, per_src.get(False, 0), per_src.get(True, 0)))
    if not dry:
        blob = json.dumps(data, separators=(",", ":")).encode("utf-8")
        open(GZ, "wb").write(gzip.compress(blob, 9))
        print("wrote", GZ)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--fetch", action="store_true")
    ap.add_argument("--merge", action="store_true")
    ap.add_argument("--dry", action="store_true")
    a = ap.parse_args()
    nse = NSE()
    rows = None
    if a.list:
        rows = do_list(nse)
    if a.fetch:
        rows = rows if rows is not None else json.load(open(LIST))
        do_fetch(nse, rows)
    if a.merge:
        do_merge(a.dry)


if __name__ == "__main__":
    main()
