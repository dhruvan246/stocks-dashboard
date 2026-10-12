# -*- coding: utf-8 -*-
"""Build docs/bse_universe.json — the BSE-ONLY equity universe (stocks listed on BSE but NOT on NSE).

Our core price/fundamentals dataset is NSE-keyed, so ~2,700 BSE-only companies (e.g. Cella Space,
NSDL) never appear in the tool. This file is the backbone for covering them: one row per BSE-only
active-equity scrip with the metadata the pages need (ticker, name, ISIN, group, mcap, sector).

Sources (both bulk, no per-scrip loop for the base list):
- BSE `ListofScripData` — ALL active equity scrips (SCRIP_CD, scrip_id, ISIN, GROUP, FACE_VALUE, Mktcap…).
- NSE `EQUITY_L.csv` — the NSE ISIN set to subtract (BSE-only = ISIN not on NSE).

Sector is enriched per-scrip from BSE `ComHeadernew` (Industry), newest/biggest first, budgeted so a run
stays quick; already-known sectors are cached in scripts/_bse_sectors.json and reused.

SUSPENDED scrips that still report (2026-10-12, user: "add suspended scrips like alstone to the bse universe"): BSE
`ListofScripData … status=Suspended` (1,094 scrips on 12-Oct-2026) joins the universe when the scrip is BSE-only AND its
result archive (`Result_Arch_ng`, the §178 listing) shows a financial result FILED within SUS_DAYS (400) of today —
ALSTONE 539277 (XT, filed Sep-26 on 7-Oct-2026) yes, Futuristic Offshore 500154 (last filing Jun-17) no. The per-scrip
check is cached in scripts/_bse_suspended.json {code: {"chk": checked, "last": latest filing YYYYMMDD, "q": label}} and
re-done after SUS_RECHECK (60) days, SUS_BUDGET checks per run (--suspended-budget N); until checked a scrip stays out.
The row shape is unchanged (8 columns); the file carries the qualifying codes under a top-level "suspended" list.

SCRIP MAP (2026-10-12, user: "rebuild the bse scrip map too so suspended scrips get detail"): scripts/bse_scrips.json
{by_id: {TICKER: code}, by_isin: {ISIN: code}} is the key every XBRL detail is filed under (xbrl_extra, the §178 route's
code2tk). It was a static copy of the Active master, so a universe row outside it (312 on 12-Oct-2026, 102 of them the
suspended scrips above) could get numbers but never detail. extend_scrip_map() now adds every universe row the map lacks,
ADD-ONLY: an existing ticker or ISIN is never re-pointed; a ticker the NSE tape owns, a ticker in the §203 conflict
ledger, or a row without a real ISIN is skipped (printed). The map is written only when something was added.

Output: {"updated","count", "rows":[[scrip_cd, ticker, name, isin, group, faceval, mcap_cr, sector], …]}
        sorted by market cap desc. Guard: refuses to overwrite a good file with a tiny/failed fetch.

Run: python -X utf8 scripts/build_bse_universe.py [--sector-budget N]
"""
import os as _o, sys as _s; _s.path.insert(0, _o.path.dirname(_o.path.abspath(__file__))); import bse_headers as BH  # §181 BSE headers
import os, sys, json, io, csv, time, datetime
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bse_fetch as B
import bse_names as BN   # §204: BSE's "-$" scrip-name marker never reaches a published name

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "..", "docs", "bse_universe.json")
SEC_CACHE = os.path.join(HERE, "_bse_sectors.json")
NSE_CACHE = os.path.join(HERE, "_nse_universe.json")
MIN_ROWS = 800                      # a broken fetch must never clobber a good universe

def nse_isin_set():
    """NSE equity ISINs (to subtract). Cached; refreshed from EQUITY_L.csv when possible."""
    import build_fundamentals as NB
    jar = NB.nse_jar()
    hdr = {"User-Agent": NB.UA, "Accept": "*/*",
           "Referer": "https://www.nseindia.com/market-data/securities-available-for-trading"}
    for url in ("https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv",
                "https://archives.nseindia.com/content/equities/EQUITY_L.csv"):
        try:
            raw = NB._get(url, headers=hdr, jar=jar, timeout=60)
            isin = set(); syms = set()
            for row in csv.DictReader(io.StringIO(raw)):
                iz = (row.get(" ISIN NUMBER") or row.get("ISIN NUMBER") or "").strip()
                sy = (row.get("SYMBOL") or "").strip()
                if iz: isin.add(iz)
                if sy: syms.add(sy)
            if len(isin) > 1500:
                json.dump({"isin": sorted(isin), "sym": sorted(syms)}, open(NSE_CACHE, "w"))
                return isin
        except Exception as ex:
            print("NSE list fetch failed (%s), trying next/cache" % str(ex)[:60])
    if os.path.exists(NSE_CACHE):
        return set(json.load(open(NSE_CACHE))["isin"])
    raise SystemExit("no NSE ISIN set available")

def bse_active_equity(op):
    r = B.get(op, "https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w"
                  "?Group=&Scripcode=&industry=&segment=Equity&status=Active")
    return json.loads(r if isinstance(r, str) else r.decode("utf8", "ignore"))

def sector_of(op, code, cache):
    k = str(code)
    if k in cache: return cache[k]
    try:
        r = B.get(op, "https://api.bseindia.com/BseIndiaAPI/api/ComHeadernew/w"
                      "?quotetype=EQ&scripcode=%s&seriesid=" % code)
        j = json.loads(r if isinstance(r, str) else r.decode("utf8", "ignore"))
        sec = (j.get("Industry") or "").strip() or "Other"
    except Exception:
        sec = ""            # leave blank → retry next run
    cache[k] = sec
    return sec

SEEN = os.path.join(HERE, "bse_seen_scrips.json")
SEEN_DAYS = 120
SUS_CACHE = os.path.join(HERE, "_bse_suspended.json")
SUS_DAYS, SUS_RECHECK, SUS_BUDGET = 400, 60, 200
RESULT_LIST = "https://api.bseindia.com/BseIndiaAPI/api/Result_Arch_ng/w?scrip_cd=%s"


def bse_suspended_equity(op):
    r = B.get(op, "https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w"
                  "?Group=&Scripcode=&industry=&segment=Equity&status=Suspended")
    return json.loads(r if isinstance(r, str) else r.decode("utf8", "ignore"))


def latest_result_filing(op, code):
    """YYYYMMDD of the newest result filing in the scrip's BSE result archive (0 = none), and its quarter label."""
    r = B.get(op, RESULT_LIST % code)
    rows = (json.loads(r if isinstance(r, str) else r.decode("utf8", "ignore")) or {}).get("Table") or []
    best, lab = 0, ""
    for x in rows:
        fd = (x.get("Filing_Date_Time") or "")[:10].replace("-", "")
        if fd.isdigit() and int(fd) > best:
            best, lab = int(fd), x.get("Quarter") or ""
    return best, lab


def suspended_extra(op, allsus, have, nse, cache, budget, today=None):
    """API-shaped rows for Suspended BSE-only scrips that filed a result within SUS_DAYS — checked against the result
    archive, `budget` fresh checks per run, the rest from the cache. Same exclusions as seen_extra (ISIN on NSE, its issuer,
    a ticker the NSE tape already owns)."""
    today = today or datetime.date.today()
    tday = int(today.strftime("%Y%m%d"))
    floor = int((today - datetime.timedelta(days=SUS_DAYS)).strftime("%Y%m%d"))
    stale = int((today - datetime.timedelta(days=SUS_RECHECK)).strftime("%Y%m%d"))
    tape_keys, tape_isin = nse_tape_meta()
    nse = set(nse) | tape_isin
    nse_iss = {i[:7] for i in nse if i.startswith("INE") and i[7:9] == "01"}
    out, checked = [], 0
    for x in sorted(allsus, key=mcap, reverse=True):            # biggest first, so a budget cut lands on the tail
        code = str(x.get("SCRIP_CD")); isin = (x.get("ISIN_NUMBER") or "").strip(); tk = (x.get("scrip_id") or "").strip()
        if not isin or code in have or isin in nse or isin[:7] in nse_iss or tk.upper() in tape_keys:
            continue
        c = cache.get(code)
        if (not c or int(c.get("chk") or 0) < stale) and checked < budget:
            try:
                last, lab = latest_result_filing(op, code)
                cache[code] = {"chk": tday, "last": last, "q": lab}; c = cache[code]; checked += 1
                time.sleep(0.6)
            except Exception as ex:
                print("  suspended %s: listing failed (%s)" % (code, str(ex)[:60]))
        if c and int(c.get("last") or 0) >= floor:
            out.append(dict(x, _sus=1))
    print("suspended: %d on BSE's list, %d checked this run, %d qualify (result filed within %d days)"
          % (len(allsus), checked, len(out), SUS_DAYS))
    return out


def nse_tape_meta():
    """(symbols, ISINs) of the NSE tape from the committed docs/sf_stock_data.bin — only its trailing "meta" object is
    decoded (0.5 s; the bars are never parsed). Empty sets if the file is absent."""
    p = os.path.join(HERE, "..", "docs", "sf_stock_data.bin")
    try:
        import gzip
        b = gzip.decompress(open(p, "rb").read())
        m, _ = json.JSONDecoder().raw_decode(b[b.rfind(b'"meta":') + 7:].decode("utf-8"))
    except (OSError, ValueError):
        return set(), set()
    return {k.upper() for k in m}, {v["isin"] for v in m.values() if v.get("isin")}


def seen_extra(allbse, nse, today=None):
    """Rows for BSE equities that TRADED within SEEN_DAYS (scripts/bse_seen_scrips.json, written by fetch_bse_bhav from
    each day's bhavcopy) but are absent from the Active list, ISIN not on NSE (exact, or the issuer of an NSE equity ISIN).
    §172: 77 such scrips traded on 21-Sep-2026 (groups XT/Z, once-a-week surveillance) with no presence on the site.
    Same shape as an API row; Mktcap 0 (unknown), FACE_VALUE 0, name/group from the bhavcopy."""
    try:
        seen = json.load(open(SEEN, encoding="utf-8"))
    except (OSError, ValueError):
        return []
    today = today or datetime.date.today()
    floor = int((today - datetime.timedelta(days=SEEN_DAYS)).strftime("%Y%m%d"))
    have = {str(x.get("SCRIP_CD")) for x in allbse}
    tape_keys, tape_isin = nse_tape_meta()
    nse = set(nse) | tape_isin
    nse_iss = {i[:7] for i in nse if i.startswith("INE") and i[7:9] == "01"}
    out = []
    for code, (tk, name, isin, grp, last) in sorted(seen.items()):
        if code in have or last < floor or isin in nse or isin[:7] in nse_iss: continue
        # a symbol the NSE tape already owns (dead on NSE but still printing on BSE: RAMAPETRO, KANDAGIRI; BZ names
        # outside EQUITY_L: BLUECHIP, LASA) is not BSE-only — never a second page for it. A ticker coincidence with an
        # unrelated NSE key also lands here: that errs toward leaving a scrip out, never toward a duplicate.
        if tk.upper() in tape_keys: continue
        out.append({"SCRIP_CD": int(code), "scrip_id": tk, "Scrip_Name": name, "ISIN_NUMBER": isin, "GROUP": grp,
                    "FACE_VALUE": 0, "Mktcap": 0, "_seen": last})
    return out


def mcap(x):
    try: return round(float(x.get("Mktcap") or 0), 2)
    except Exception: return 0.0


SCRIPS = os.path.join(HERE, "bse_scrips.json")


def extend_scrip_map(rows, path=SCRIPS):
    """Add-only: map every universe row (code, ticker, name, isin, …) the scrip map lacks. Returns (added, skipped)."""
    import bse_resolve
    d = json.load(open(path, encoding="utf-8")); by_id = d.setdefault("by_id", {}); by_isin = d.setdefault("by_isin", {})
    tape_keys, _ = nse_tape_meta(); conf = bse_resolve.conflicts()
    added, skipped = 0, []
    for r in rows:
        code, tk, isin = int(r[0]), str(r[1] or "").upper(), str(r[3] or "").upper()
        if not tk or len(isin) != 12 or not isin.startswith("IN"):
            skipped.append((tk or code, "no ISIN")); continue
        if tk in tape_keys or tk in conf:
            skipped.append((tk, "NSE tape key" if tk in tape_keys else "conflict ledger")); continue
        if tk not in by_id and isin not in by_isin:
            by_id[tk] = code; by_isin[isin] = code; added += 1
        elif tk not in by_id and int(by_isin[isin]) == code:
            by_id[tk] = code; added += 1
        elif isin not in by_isin and int(by_id[tk]) == code:
            by_isin[isin] = code; added += 1
    if added:
        json.dump(d, open(path, "w", encoding="utf-8"), ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    print("scrip map: +%d entries (by_id %d, by_isin %d); skipped %d%s"
          % (added, len(by_id), len(by_isin), len(skipped), (": " + ", ".join("%s (%s)" % x for x in skipped[:12])) if skipped else ""))
    return added, skipped

def main():
    budget = 400
    if "--sector-budget" in sys.argv:
        budget = int(sys.argv[sys.argv.index("--sector-budget") + 1])
    sus_budget = SUS_BUDGET
    if "--suspended-budget" in sys.argv:
        sus_budget = int(sys.argv[sys.argv.index("--suspended-budget") + 1])
    op = B.session(); time.sleep(1)
    nse = nse_isin_set()
    allbse = bse_active_equity(op)
    bse_only = [x for x in allbse if (x.get("ISIN_NUMBER") or "").strip() and
                (x.get("ISIN_NUMBER") or "").strip() not in nse]
    extra = seen_extra(allbse, nse)
    bse_only += extra
    sus_cache = json.load(open(SUS_CACHE)) if os.path.exists(SUS_CACHE) else {}
    try:
        allsus = bse_suspended_equity(op)
    except Exception as ex:
        allsus = []; print("suspended list fetch failed (%s) — none added this run" % str(ex)[:60])
    sus = suspended_extra(op, allsus, {str(x.get("SCRIP_CD")) for x in bse_only}, nse, sus_cache, sus_budget)
    json.dump(sus_cache, open(SUS_CACHE, "w"), separators=(",", ":"), sort_keys=True)
    bse_only += sus
    bse_only.sort(key=mcap, reverse=True)
    print("BSE active equity %d; BSE-only %d (incl. %d traded but not on the Active list, %d suspended still reporting)"
          % (len(allbse), len(bse_only), len(extra), len(sus)))

    cache = json.load(open(SEC_CACHE)) if os.path.exists(SEC_CACHE) else {}
    spent = 0
    rows = []
    for x in bse_only:
        code = x["SCRIP_CD"]
        sec = cache.get(str(code), "")
        if not sec and spent < budget and (mcap(x) > 0 or x.get("_seen") or x.get("_sus")):   # enrich biggest-first within budget
            sec = sector_of(op, code, cache); spent += 1
            if spent % 50 == 0:
                json.dump(cache, open(SEC_CACHE, "w")); time.sleep(0.2)
        try: fv = round(float(x.get("FACE_VALUE") or 0), 2)
        except Exception: fv = 0
        rows.append([int(code), (x.get("scrip_id") or "").strip().upper(),
                     BN.clean_scrip_name(x.get("Scrip_Name")), (x.get("ISIN_NUMBER") or "").strip(),
                     (x.get("GROUP") or "").strip(), fv, mcap(x), sec])
    json.dump(cache, open(SEC_CACHE, "w"))

    if len(rows) < MIN_ROWS and os.path.exists(OUT):
        raise SystemExit("ABORT: only %d rows — keeping existing bse_universe.json" % len(rows))

    ist = datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)
    out = {"updated": ist.strftime("%Y-%m-%d %H:%M IST"), "count": len(rows),
           "sectors_known": sum(1 for r in rows if r[7]), "rows": rows,
           "suspended": sorted(int(x["SCRIP_CD"]) for x in sus)}      # still-reporting suspended scrips among the rows
    json.dump(out, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, separators=(",", ":"))
    print("WROTE %s: %d BSE-only stocks (%d with sector; +%d sectors this run)"
          % (os.path.normpath(OUT), len(rows), out["sectors_known"], spent))
    try:
        extend_scrip_map(rows)
    except Exception as ex:
        print("scrip map not extended (%s)" % str(ex)[:80])

if __name__ == "__main__":
    main()
