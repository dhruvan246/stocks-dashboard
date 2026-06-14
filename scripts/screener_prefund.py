# -*- coding: utf-8 -*-
"""Fetch PRE-IPO quarterly net profit for recent IPOs from Screener (the DRHP/restated history
that NSE/BSE filing APIs don't carry). Writes a SEPARATE staging file screener_pre.json; merged
into sf_fundamentals.json later (pre-IPO quarters become YoY bases, ann date null -> never used
as the 'current' quarter, only as the year-ago reference, so point-in-time-safe).

Cross-validates the OVERLAPPING (post-listing) quarters against our NSE data — only trusts a
symbol's pre-IPO quarters if the overlap matches.

Run: python -X utf8 screener_prefund.py SYM1 SYM2 ...   (or @file.txt)
"""
import urllib.request, json, gzip, re, html, os, time, sys

UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36'
HERE = os.path.dirname(os.path.abspath(__file__))
OUTF = os.path.join(HERE, "screener_pre.json")
NSEF = os.path.join(os.path.dirname(HERE), "docs", "sf_fundamentals.json")
MON = {"Jan": "0331?", "Mar": "0331", "Jun": "0630", "Sep": "0930", "Dec": "1231"}  # Mar=Q4(qe Mar31)

def get(url):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        r = urllib.request.urlopen(req, timeout=25); raw = r.read()
    except urllib.error.HTTPError as e:
        if e.code == 429: return "429"          # rate-limited
        raise
    if r.headers.get("Content-Encoding") == "gzip": raw = gzip.decompress(raw)
    return raw.decode("utf-8", "replace")

def qe_of(label):           # "Mar 2025" -> 20250331
    mon, yr = label.split()
    mm = MON.get(mon)
    return int(yr + mm) if mm and "?" not in mm else None

def fetch(sym):
    try: t = get("https://www.screener.in/company/%s/" % sym)
    except Exception: return None
    if t == "429": return "429"
    m = re.search(r'id="quarters".*?</section>', t, re.S)
    if not m: return None
    sec = m.group(0)
    heads = re.findall(r'<th[^>]*>\s*([A-Za-z]{3} \d{4})\s*</th>', sec)
    row = re.search(r'<td[^>]*class="text"[^>]*>\s*(?:<button[^>]*>)?\s*Net Profit.*?</td>(.*?)</tr>', sec, re.S)
    if not row: return None
    vals = [html.unescape(re.sub(r'<[^>]+>', '', c)).strip().replace(',', '') for c in re.findall(r'<td[^>]*>(.*?)</td>', row.group(1), re.S)]
    out = {}
    for lab, v in zip(heads, vals):
        qe = qe_of(lab)
        try: out[qe] = float(v)
        except Exception: pass
    return {q: n for q, n in out.items() if q}

def main():
    a = sys.argv[1:]
    if a and a[0].startswith("@"):
        syms = [l.strip() for l in open(a[0][1:]) if l.strip()]
    else:
        syms = a
    nse = json.load(open(NSEF))
    store = json.load(open(OUTF)) if os.path.exists(OUTF) else {}
    consec = 0
    for sym in syms:
        if sym in store: continue                      # resumable
        prim = fetch(sym)
        if prim == "429":
            print("%-12s 429 — backoff 90s" % sym); time.sleep(90); prim = fetch(sym)
            if prim == "429":
                consec += 1
                if consec >= 4: print("repeated 429 — Screener throttling; stop, rerun later to resume"); break
                continue
        consec = 0
        if not prim:
            print("%-12s no data" % sym); time.sleep(4); continue
        have = {r[0]: (r[1], r[3]) for r in nse.get(sym, [])}   # NSE post-listing quarters (validation)
        ok = bad = 0
        for qe, npc in prim.items():
            if qe in have:
                ref = have[qe][1] if have[qe][1] is not None else have[qe][0]
                if ref is not None and abs(ref - npc) <= max(2, abs(ref) * 0.05): ok += 1
                elif ref is not None: bad += 1
        verdict = "MATCH" if ok and not bad else ("MISMATCH" if bad else "no-overlap")
        store[sym] = {"np": {str(k): v for k, v in prim.items()}, "overlap": "%d/%d" % (ok, ok + bad), "verdict": verdict}
        json.dump(store, open(OUTF, "w"), indent=0)
        pre = sorted(q for q in prim if not have or q < min(have))
        print("%-12s %d q | overlap %d/%d %s | pre-IPO: %s" % (sym, len(prim), ok, ok + bad, verdict, pre[:5]))
        time.sleep(5)                                  # gentle — Screener 429s easily
    print("staged %d symbols -> screener_pre.json" % len(store))

if __name__ == "__main__":
    main()
