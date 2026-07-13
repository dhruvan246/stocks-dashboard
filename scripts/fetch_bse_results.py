# -*- coding: utf-8 -*-
"""Merge BSE-sourced result filings + forthcoming-result dates into the Quarterly Results page's
two side-files, so coverage spans NSE **and** BSE-only companies (~all listed names, like the big sites).

Runs AFTER the NSE fetchers (fetch_announcements.py writes docs/results_feed.json; fetch_results_calendar.py
writes docs/results_calendar.json). This ADDS BSE rows that the NSE feed/calendar don't already have:
- `AnnSubCategoryGetData?strCat=Result`  → just-declared results filings (guid PDF, headline, quarter)
- `Corpforthresults?strCategory=Result`  → forthcoming result dates (BSE ticker + meeting date)

SAFE / SELF-HEALING: purely additive and wrapped so any BSE failure leaves the NSE files untouched.
De-duped against existing rows by (symbol, date) — a dual-listed company already carried by NSE is skipped,
so we prefer the NSE row (which has a clean ticker + price data). BSE-only names use the BSE ticker
(from the page-URL slug / short_name) and render as non-clickable feed/calendar entries (no price data),
which is exactly what other sites show for thin BSE names.

Reuses bse_fetch's cookie-warmed session (api.bseindia.com). Resolver: bse_scrips.json['by_id'] (SYM→scripcode).

Run: python -X utf8 scripts/fetch_bse_results.py
"""
import os, sys, json, re, time, datetime
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bse_fetch as B

HERE = os.path.dirname(os.path.abspath(__file__))
DOCS = os.path.join(HERE, "..", "docs")
FEED = os.path.join(DOCS, "results_feed.json")
CAL = os.path.join(DOCS, "results_calendar.json")
FEED_WINDOW_DAYS = 31
BSE_ATT = "https://www.bseindia.com/xml-data/corpfiling/AttachLive/"
MON = {"jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,"jul":7,"aug":8,"sep":9,"sept":9,"oct":10,"nov":11,"dec":12}
# ⚠️ A results filing in Jul/Aug/Sep is very often a LATE March (Q4/annual) result, NOT the current
# June quarter — of BSE result filings in a recent 30-day window, 72 said March vs 31 June. So NEVER
# assume the current season: read the reporting period per filing, snapped to a quarter-end month, and
# ANCHOR strictly on an "ended/ending" clause so a board-meeting date ("held on July 1, 2026") can't leak in.
DAY_LAST = {3: 31, 6: 30, 9: 30, 12: 31}
ENDED_RE = re.compile(r"end(?:ed|ing)\s+(?:on\s+)?(.{0,30}?\d{4})", re.I)
DMY_RE = re.compile(r"(\d{1,2})(?:st|nd|rd|th)?[\s,]+([A-Za-z]{3,9})[,\s]+(\d{4})", re.I)   # 31st March 2026
MDY_RE = re.compile(r"([A-Za-z]{3,9})\s+(\d{1,2}),?\s+(\d{4})", re.I)                        # March 31, 2026
NUM_RE = re.compile(r"(\d{1,2})[.\-/](\d{1,2})[.\-/](\d{4})")                                # 31.03.2026

def _mk(mo, y):
    return (y * 10000 + mo * 100 + DAY_LAST[mo]) if (mo in DAY_LAST) else 0

def _seg_qe(seg):
    m = DMY_RE.search(seg)
    if m: return _mk(MON.get(m.group(2).lower()[:3], 0), int(m.group(3)))
    m = MDY_RE.search(seg)
    if m: return _mk(MON.get(m.group(1).lower()[:3], 0), int(m.group(3)))
    m = NUM_RE.search(seg)
    if m: return _mk(int(m.group(2)), int(m.group(3)))     # dd.mm.yyyy
    return 0

def qe_from_head(*texts):
    """Reporting quarter-end (int YYYYMMDD) parsed from an 'ended <date>' clause in the filing text.
    Returns 0 when the period isn't stated — the page then shows NO quarter badge (never a wrong one)."""
    h = " ".join(str(t or "") for t in texts)
    for m in ENDED_RE.finditer(h):
        qe = _seg_qe(m.group(1))
        if qe: return qe
    return 0

def parse_dt(s):
    """'2026-07-11T15:54:42.027' -> 'YYYY-MM-DD HH:MM:SS'."""
    m = re.match(r"(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})", str(s or ""))
    if m: return m.group(1) + " " + m.group(2)
    m = re.match(r"(\d{4}-\d{2}-\d{2})", str(s or ""))
    return (m.group(1) + " 00:00:00") if m else None

def parse_cal_date(s):
    """'13 Jul 2026' -> '2026-07-13'."""
    m = re.match(r"(\d{1,2})\s+([A-Za-z]{3,9})\s+(\d{4})", str(s or "").strip())
    if not m: return None
    mo = MON.get(m.group(2).lower()[:3])
    return "%s-%02d-%02d" % (m.group(3), mo, int(m.group(1))) if mo else None

def ticker_from_url(url, fallback):
    """BSE page URL '.../avenue-supermarts-ltd/dmart/540376/' -> 'DMART'."""
    parts = [p for p in str(url or "").rstrip("/").split("/") if p]
    for p in reversed(parts):
        if p.isdigit() or "." in p or p in ("stock-share-price", "www.bseindia.com", "https:", "http:"): continue
        if re.fullmatch(r"[A-Za-z][A-Za-z0-9&\-]{1,18}", p): return p.upper().replace("-", "")
    return re.sub(r"[^A-Z0-9]", "", str(fallback or "").upper())[:16] or "BSE"

def load(p, default):
    try: return json.load(open(p, encoding="utf-8"))
    except Exception: return default

def nname(s):
    """Normalized company name for cross-exchange dedup: 'Indbank Merchant Banking Services Ltd'
    == 'INDBANK MERCHANT BANKING SERVICES LIMITED' (a dual-listed co can reach us with a DIFFERENT
    fallback ticker from BSE, e.g. INDBNK vs NSE's INDBANK — symbol dedup alone double-counts it)."""
    s = re.sub(r"[^a-z0-9]", "", str(s or "").lower())
    return re.sub(r"(limited|ltd)$", "", s)

def main():
    by_id = json.load(open(os.path.join(HERE, "bse_scrips.json"), encoding="utf-8"))["by_id"]
    rev = {int(v): k for k, v in by_id.items()}          # scripcode -> NSE symbol
    o = B.session(); time.sleep(1)
    today = datetime.date.today()

    # ---- 1. result filings feed ----
    lo = today - datetime.timedelta(days=FEED_WINDOW_DAYS - 1)
    F, T = lo.strftime("%Y%m%d"), today.strftime("%Y%m%d")
    bse_rows, page, total = [], 1, None
    while page <= 60:
        url = ("https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w?pageno=%d"
               "&strCat=Result&strPrevDate=%s&strToDate=%s&strScrip=&strSearch=P&strType=C&subcategory=-1"
               % (page, F, T))
        try:
            j = json.loads(B.get(o, url)); tab = j.get("Table", []) or []
            if total is None: total = (j.get("Table1") or [{}])[0].get("ROWCNT", 0)
        except Exception as ex:
            print("BSE feed page %d ERR: %s" % (page, ex)); break
        if not tab: break
        bse_rows.extend(tab)
        if total and len(bse_rows) >= total: break
        page += 1; time.sleep(0.4)
    print("BSE result filings fetched:", len(bse_rows), "(reported total %s)" % total)

    feed = load(FEED, {"updated": "", "rows": []})
    have = set((r[0], r[2][:10]) for r in feed.get("rows", []) if isinstance(r, list) and len(r) >= 3)
    have_names = set(nname(r[1]) for r in feed.get("rows", []) if isinstance(r, list) and len(r) >= 2)
    added = 0
    for r in bse_rows:
        try: sc = int(r.get("SCRIP_CD"))
        except Exception: continue
        dt = parse_dt(r.get("NEWS_DT") or r.get("DT_TM"))
        if not dt: continue
        sym = rev.get(sc) or ticker_from_url(r.get("NSURL"), r.get("SLONGNAME"))
        if (sym, dt[:10]) in have: continue           # already carried by the NSE feed
        # a dual-listed co can reach us under a DIFFERENT ticker than the NSE row (INDBNK vs
        # INDBANK, resolver or fallback) — dedup by normalized company name too, unconditionally
        if nname(r.get("SLONGNAME")) in have_names: continue
        have.add((sym, dt[:10])); have_names.add(nname(r.get("SLONGNAME")))
        att = str(r.get("ATTACHMENTNAME") or "").strip()
        file = (BSE_ATT + att) if att else ""
        cap = re.sub(r"\s+", " ", str(r.get("HEADLINE") or r.get("NEWSSUB") or "")).strip()
        if len(cap) > 220: cap = cap[:219] + "…"
        feed.setdefault("rows", []).append([sym, re.sub(r"\s+", " ", str(r.get("SLONGNAME") or sym)).strip(),
                                            dt, qe_from_head(r.get("HEADLINE"), r.get("NEWSSUB")), cap, file])
        added += 1
    # trim to window + sort newest-first
    lo_iso = lo.isoformat()
    feed["rows"] = sorted((r for r in feed["rows"] if r[2][:10] >= lo_iso),
                          key=lambda r: (r[2], r[0]), reverse=True)
    ist = datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)
    feed["updated"] = ist.strftime("%Y-%m-%d %H:%M IST")
    json.dump(feed, open(FEED, "w", encoding="utf-8"), ensure_ascii=False, separators=(",", ":"))
    print("results_feed.json: +%d BSE rows -> %d total" % (added, len(feed["rows"])))

    # ---- 2. forthcoming results calendar ----
    try:
        fr = json.loads(B.get(o, "https://api.bseindia.com/BseIndiaAPI/api/Corpforthresults/w?scripcode=&strCategory=Result"))
    except Exception as ex:
        print("BSE forthcoming ERR:", ex); fr = []
    cal = load(CAL, None)
    if cal and isinstance(fr, list) and fr:
        chave = set((r[0], r[2]) for r in cal.get("rows", []) if isinstance(r, list) and len(r) >= 3)
        cnames = set(nname(r[1]) for r in cal.get("rows", []) if isinstance(r, list) and len(r) >= 2)
        cadd = 0
        for r in fr:
            d = parse_cal_date(r.get("meeting_date"))
            if not d or d < today.isoformat(): continue
            try: sc = int(r.get("scrip_Code"))
            except Exception: sc = None
            sym = (sc and rev.get(sc)) or str(r.get("short_name") or "").strip().upper() or ticker_from_url(r.get("URL"), r.get("Long_Name"))
            if (sym, d) in chave: continue
            if nname(r.get("Long_Name")) in cnames: continue
            chave.add((sym, d)); cnames.add(nname(r.get("Long_Name")))
            cal.setdefault("rows", []).append([sym, re.sub(r"\s+", " ", str(r.get("Long_Name") or sym)).strip(),
                                               d, "Financial Results"])
            cadd += 1
        cal["rows"] = sorted(cal["rows"], key=lambda r: (r[2], r[0]))
        json.dump(cal, open(CAL, "w", encoding="utf-8"), ensure_ascii=False, separators=(",", ":"))
        print("results_calendar.json: +%d BSE rows -> %d total" % (cadd, len(cal["rows"])))
    else:
        print("results_calendar.json: skipped (no NSE calendar file yet or empty BSE fetch)")

if __name__ == "__main__":
    main()
