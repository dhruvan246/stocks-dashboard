# -*- coding: utf-8 -*-
"""
Build a POINT-IN-TIME quarterly net-profit dataset for the stock backtest, to add
StockView's two fundamental factors: profitYoyPct (Net Profit Qtr Growth YoY %) and
profitBase (year-ago-quarter net profit).

Source = NSE (free, official):
  - corporates-financial-results?symbol=X&period=Quarterly  → every quarterly filing
    with its broadcast/announcement date (point-in-time!), quarter period, and XBRL link.
  - XBRL  → ProfitLossForPeriod for the quarter. NSE Ind-AS context convention:
    context "OneD" = STANDALONE current quarter, "FourD" = CONSOLIDATED current quarter
    (we also fall back to any context whose period == the quarter).

Output: scripts/fundamentals.json = { SYM: [ [qEndYYYYMMDD, npStd_cr, annStd, npCon_cr, annCon], ... ] }
(np in ₹ crore; ann = announcement date YYYYMMDD; null where a basis wasn't filed). Sorted by
quarter end. Standalone and consolidated each carry their OWN announcement date (they're often
filed separately), so the backtest can honour StockView's Standalone/Consolidated basis toggle
point-in-time.

Run:  python -X utf8 build_fundamentals.py [SYM1 SYM2 ...]   (default = a small test set)
Cache: scripts/_xbrl_cache/ (gitignored via scripts/_*). Resumable.
"""
import os, sys, re, json, time, gzip, threading, concurrent.futures, urllib.request, http.cookiejar

MIN_QE = 20170101   # skip quarters ending before this — NSE's XBRL archive is sparse pre-2016
                    # and the backtest default starts 2020 (year-ago bases need ~2018). Cuts the 404 storm.

HERE = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(HERE, "_xbrl_cache"); os.makedirs(CACHE, exist_ok=True)
OUT = os.path.join(HERE, "fundamentals.json")
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
MONTHS = {m: i for i, m in enumerate(
    ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"], 1)}

def _get(url, headers=None, jar=None, timeout=30, binary=False):
    req = urllib.request.Request(url, headers=headers or {"User-Agent": UA})
    op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar)) if jar is not None else urllib.request.build_opener()
    r = op.open(req, timeout=timeout); data = r.read()
    if r.headers.get("Content-Encoding") == "gzip": data = gzip.decompress(data)
    return data if binary else data.decode("utf-8", "replace")

def nse_jar():
    jar = http.cookiejar.CookieJar()
    for u in ("https://www.nseindia.com/",
              "https://www.nseindia.com/companies-listing/corporate-filings-financial-results"):
        try: _get(u, headers={"User-Agent": UA, "Accept": "text/html"}, jar=jar, timeout=20)
        except Exception: pass
    return jar

def iso(d):  # "16-Jan-2025 20:20" or "31-Mar-2024" -> "20240331"
    m = re.match(r"(\d{1,2})-([A-Za-z]{3})-(\d{4})", d or "")
    if not m: return None
    return "%04d%02d%02d" % (int(m.group(3)), MONTHS[m.group(2).title()], int(m.group(1)))

def xbrl_profit(xml):
    """Return (np_standalone_cr, np_consolidated_cr) for the CURRENT QUARTER from one filing.

    NSE Ind-AS quirk: context PERIOD dates are unreliable (a 9-month value can be tagged with
    the quarter's dates), so we don't trust them. What IS reliable:
      - context 'OneD' = the current 3-month quarter (its basis varies by filing type);
      - each context carries a NatureOfReportStandaloneConsolidated fact (Standalone/Consolidated);
      - in a COMBINED filing, 'FourD' is the consolidated current quarter (different nature to OneD);
        in a single-basis filing, 'FourD' is the YTD of the SAME nature → ignore it.
    """
    nat = {}
    for m in re.finditer(r'NatureOfReportStandaloneConsolidated contextRef="([^"]+)"[^>]*>([^<]+)<', xml):
        nat[m.group(1)] = m.group(2).strip().lower()
    plp = {}
    for m in re.finditer(r'<in-bse-fin:ProfitLossForPeriod contextRef="([^"]+)"[^>]*>([^<]+)<', xml):
        if m.group(1) not in plp:
            plp[m.group(1)] = round(float(m.group(2)) / 1e7, 2)   # rupees -> crore
    std = con = None
    one, one_nat = plp.get("OneD"), nat.get("OneD", "")
    if one is not None:
        if "consol" in one_nat: con = one
        else: std = one                                  # standalone or unlabelled
    four, four_nat = plp.get("FourD"), nat.get("FourD", "")
    if four is not None and four_nat != one_nat:         # combined filing: other basis, current Q
        if "consol" in four_nat: con = con if con is not None else four
        else: std = std if std is not None else four
    return std, con

def qstart(qe):
    """First day of the 3-month quarter ENDING on qe (ISO yyyy-mm-dd). Q4 filings also carry
    the annual period — we always want the 3-month window, so derive it from the end date."""
    y, mo = int(qe[:4]), int(qe[5:7])
    sm = mo - 2
    sy = y
    if sm <= 0: sm += 12; sy -= 1
    return "%04d-%02d-01" % (sy, sm)

def fetch_symbol(sym, jar):
    h = {"User-Agent": UA, "Accept": "application/json",
         "Referer": "https://www.nseindia.com/companies-listing/corporate-filings-financial-results"}
    url = "https://www.nseindia.com/api/corporates-financial-results?index=equities&symbol=%s&period=Quarterly" % sym
    try:
        rows = json.loads(_get(url, headers=h, jar=jar, timeout=30))
    except Exception as e:
        print("  %s: list FAIL %s" % (sym, e)); return None
    # group ALL filings per quarter (standalone & consolidated are often separate filings)
    byq = {}
    for r in rows:
        qe = iso(r.get("toDate")); ann = iso(r.get("broadCastDate") or r.get("filingDate"))
        xb = r.get("xbrl", "")
        if not qe or not xb.startswith("http"): continue
        byq.setdefault(qe, []).append({"ann": ann or "99999999", "xbrl": xb})
    out = []
    for qe in sorted(byq):
        if int(qe) < MIN_QE: continue       # skip ancient quarters (no XBRL archive / not needed)
        std = con = None; annStd = annCon = None
        for f in sorted(byq[qe], key=lambda x: x["ann"]):   # earliest filing first
            if std is not None and con is not None: break
            cf = os.path.join(CACHE, re.sub(r"[^A-Za-z0-9]", "_", f["xbrl"].rsplit("/", 1)[-1]))
            try:
                if os.path.exists(cf) and os.path.getsize(cf) > 500:
                    xml = open(cf, encoding="utf-8").read()
                else:
                    xml = _get(f["xbrl"], headers={"User-Agent": UA, "Referer": "https://www.nseindia.com/"}, timeout=30)
                    open(cf, "w", encoding="utf-8").write(xml); time.sleep(0.15)
            except Exception:
                continue
            s, c = xbrl_profit(xml)
            a = None if f["ann"] == "99999999" else int(f["ann"])
            if std is None and s is not None: std, annStd = s, a
            if con is None and c is not None: con, annCon = c, a
        if std is None and con is None: continue
        out.append([int(qe), std, annStd, con, annCon])
    return out

def load_index(name):
    """Fetch an NSE index constituent list, e.g. nifty500 / nifty100 / nifty50."""
    slug = {"nifty500": "ind_nifty500list.csv", "nifty100": "ind_nifty100list.csv",
            "nifty200": "ind_nifty200list.csv", "nifty50": "ind_nifty50list.csv"}.get(name)
    if not slug: return []
    txt = _get("https://nsearchives.nseindia.com/content/indices/" + slug,
               headers={"User-Agent": UA, "Referer": "https://www.nseindia.com/"}, timeout=30)
    syms = []
    for line in txt.splitlines()[1:]:
        cols = line.split(",")
        if len(cols) >= 3 and cols[2].strip(): syms.append(cols[2].strip())
    return syms

def main():
    args = sys.argv[1:]
    if args and args[0].lower() in ("nifty500", "nifty100", "nifty200", "nifty50"):
        syms = load_index(args[0].lower())
        print("Universe %s: %d symbols" % (args[0], len(syms)))
    else:
        syms = args or ["RELIANCE", "TCS", "HDFCBANK", "INFY", "CGCL", "TATAMOTORS"]
    data = {}
    if os.path.exists(OUT):
        try: data = json.load(open(OUT))
        except Exception: pass
    todo = [s for s in syms if not (s in data and data[s])]
    print("  %d symbols, %d already built, %d to fetch" % (len(syms), len(syms) - len(todo), len(todo)))

    _tl = threading.local()
    def worker_jar():
        if not getattr(_tl, "jar", None): _tl.jar = nse_jar()
        return _tl.jar
    def do_sym(sym):
        rec = fetch_symbol(sym, worker_jar())
        if rec is None:                        # cookie likely stale — re-warm this thread once
            _tl.jar = nse_jar(); rec = fetch_symbol(sym, _tl.jar)
        return sym, rec

    lock = threading.Lock(); done = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
        for sym, rec in ex.map(do_sym, todo):
            done += 1
            if rec:
                with lock:
                    data[sym] = rec
                    if done % 10 == 0 or done == len(todo):
                        json.dump(data, open(OUT, "w"), separators=(",", ":"))
                print("  [%d/%d] %s: %d quarters  latest=%s npStd=%s npCon=%s" % (
                    done, len(todo), sym, len(rec), rec[-1][0], rec[-1][1], rec[-1][3]))
    json.dump(data, open(OUT, "w"), separators=(",", ":"))
    # web-facing copy (committed, loaded by the backtest page)
    docs = os.path.join(os.path.dirname(HERE), "docs", "sf_fundamentals.json")
    json.dump(data, open(docs, "w"), separators=(",", ":"))
    sz = os.path.getsize(docs) / 1024
    print("Wrote %s (%d symbols) + docs/sf_fundamentals.json (%.0f KB)" % (OUT, len(data), sz))

if __name__ == "__main__":
    main()
