# -*- coding: utf-8 -*-
"""Per-stock FII/DII holdings from NSE quarterly shareholding-pattern (SHP) filings.

Pipeline (DATA_RUNBOOK.md section 22):
  1. Master list per quarter-end: /api/corporate-share-holdings-master?index=equities
     &from_date=<QE>&to_date=<QE>  (the window filters on the pattern's AS-ON date, so one
     call per quarter-end returns every company's filing for that quarter; ~2,900/qtr).
  2. Each filing carries an XBRL url (nsearchives) — parse the category percentage facts:
       promoter  = ShareholdingOfPromoterAndPromoterGroupMember
       public    = PublicShareholdingMember
       FII       = InstitutionsForeignMember      (FPI I+II, FDI, other foreign)
       DII       = InstitutionsDomesticMember     (MF, insurance, banks, PF, AIF, ...)
       MF        = MutualFundsOrUTIMember
       insurance = InsuranceCompaniesMember
     Values are pure fractions (0.1867 = 18.67%); anchored via the total/prom+pub ≈ 100 check.
     Only the NEW (post-2021) format has the Domestic/Foreign split — unparseable filings are
     skipped and logged, never guessed.
  3. Merge fill-or-newer-submission-wins into scripts/shp_history.json:
       {"_names": {SYM: co-name}, SYM: {"YYYY-MM-DD"(QE): [prom, fii, dii, mf, ins, "sub-date"]}}
  4. Build docs/shareholding.json (slim page feed, aligned quarter arrays + mcap/sector join)
     and docs/shp_meta.json (tiny freshness marker committed every run, feeds.json watches it).

Runs:
  python -X utf8 scripts/fetch_shareholding.py                # daily top-up (last 3 QEs, new/revised only)
  python -X utf8 scripts/fetch_shareholding.py --backfill 4   # one-time deep fill (most-recent quarter first)
  python -X utf8 scripts/fetch_shareholding.py --feed-only    # rebuild docs feed from history, no network

Self-healing: a failed master call skips that quarter (history keeps yesterday's cells); XBRL
download/parse failures skip that company; the history write is add/update-only and ABORTs if
the merged file would lose cells. Resumable: flushes history every FLUSH_EVERY parses.
"""
import os, sys, json, re, gzip, datetime, threading, time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import build_fundamentals as B  # _get / nse_jar / UA (CI-proven NSE session)

HERE = os.path.dirname(os.path.abspath(__file__))
HIST = os.path.join(HERE, "shp_history.json")
OUT = os.path.join(HERE, "..", "docs", "shareholding.json")
META_OUT = os.path.join(HERE, "..", "docs", "shp_meta.json")
SLIM = os.path.join(HERE, "..", "docs", "dash_slim.bin")
CLASSIF = os.path.join(HERE, "..", "docs", "sector_classification.json")

TOPUP_QES = 3          # daily run: current season + 2 back (late filers / revisions)
FEED_QUARTERS = 8      # quarters shipped to the page
THREADS = 6            # parallel XBRL downloads (nsearchives is a static host)
FLUSH_EVERY = 150      # persist history every N new cells (resumable backfill)
MIN_FEED_ROWS = 500    # never overwrite a good feed with a near-empty one

REF = "https://www.nseindia.com/companies-listing/corporate-filings-shareholding-pattern"
MON = {"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
       "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12}

# XBRL category member -> output slot
MEMBERS = {
    "ShareholdingOfPromoterAndPromoterGroupMember": "prom",
    "PublicShareholdingMember": "pub",
    "InstitutionsForeignMember": "fii",
    "InstitutionsDomesticMember": "dii",
    "MutualFundsOrUTIMember": "mf",
    "InsuranceCompaniesMember": "ins",
    "ShareholdingPatternMember": "total",
}

def iso_date(s):
    """'15-JUL-2026' / '15-Jul-2026 15:04:38' -> '2026-07-15' (None if unparseable)."""
    m = re.match(r"\s*(\d{1,2})-([A-Za-z]{3})-(\d{4})", str(s or ""))
    if not m: return None
    mo = MON.get(m.group(2).upper())
    return "%s-%02d-%02d" % (m.group(3), mo, int(m.group(1))) if mo else None

def last_qes(n, today=None):
    """The n most recent quarter-end dates on/before today, most recent first."""
    d = today or datetime.date.today()
    out = []
    y, m = d.year, ((d.month - 1) // 3) * 3  # last completed quarter's end month
    if m == 0: y, m = y - 1, 12
    for _ in range(n):
        day = {3: 31, 6: 30, 9: 30, 12: 31}[m]
        out.append(datetime.date(y, m, day).isoformat())
        m -= 3
        if m == 0: y, m = y - 1, 12
    return out

def load_hist():
    if os.path.exists(HIST):
        try:
            return json.load(open(HIST, encoding="utf-8"))
        except Exception as e:
            print("WARN history unreadable (%s) — starting empty" % e)
    return {"_names": {}}

def cells_of(h):
    return sum(len(v) for k, v in h.items() if not k.startswith("_") and isinstance(v, dict))

_flush_lock = threading.Lock()
def save_hist(h):
    with _flush_lock:
        tmp = HIST + ".tmp"
        json.dump(h, open(tmp, "w", encoding="utf-8"), ensure_ascii=False, separators=(",", ":"))
        os.replace(tmp, HIST)

# ------------------------------------------------------------------ NSE fetch
def fetch_master(jar, qe_iso):
    """All SHP filings whose as-on date == qe. Returns [] on failure (self-healing)."""
    d = datetime.date.fromisoformat(qe_iso)
    dd = "%02d-%02d-%04d" % (d.day, d.month, d.year)
    url = ("https://www.nseindia.com/api/corporate-share-holdings-master?index=equities"
           "&from_date=%s&to_date=%s" % (dd, dd))
    hdr = {"User-Agent": B.UA, "Accept": "application/json, text/plain, */*", "Referer": REF}
    try:
        j = json.loads(B._get(url, headers=hdr, jar=jar, timeout=120))
        recs = j if isinstance(j, list) else j.get("data", [])
        return recs if isinstance(recs, list) else []
    except Exception as e:
        print("ERR master %s: %r" % (qe_iso, e))
        return []

def fetch_xbrl(url, jar):
    """XBRL from nsearchives — plain fetch first (static host), session fallback."""
    hdr = {"User-Agent": B.UA, "Accept": "*/*", "Referer": REF}
    try:
        return B._get(url, headers=hdr, jar=None, timeout=120)
    except Exception:
        return B._get(url, headers=hdr, jar=jar, timeout=120)

# ------------------------------------------------------------------ XBRL parse
def parse_shp(txt, qe_iso):
    """-> dict(prom, fii, dii, mf, ins) as % (2dp) or None if not anchored.
    Category facts sit in contexts with exactly ONE explicit member and no typed member
    (typed members = the named >1% shareholders)."""
    root = ET.fromstring(txt)
    strip = lambda t: t.split("}", 1)[-1]
    ctx = {}  # id -> member localname | None(invalid)
    for c in root.iter():
        if strip(c.tag) != "context": continue
        mems, typed = [], False
        for m in c.iter():
            st = strip(m.tag)
            if st == "explicitMember":
                mems.append((m.text or "").split(":")[-1].strip())
            elif st == "typedMember":
                typed = True
        ctx[c.get("id")] = mems[0] if (not typed and len(mems) == 1) else None

    vals = {}
    for f in root.iter():
        if strip(f.tag) != "ShareholdingAsAPercentageOfTotalNumberOfShares": continue
        slot = MEMBERS.get(ctx.get(f.get("contextRef")) or "")
        if not slot: continue
        try:
            v = float(str(f.text).strip())
        except (TypeError, ValueError):
            continue
        vals[slot] = v  # duplicate facts for a member are identical in practice; last wins

    if not vals: return None
    prom, pub = vals.get("prom"), vals.get("pub")
    if prom is None and pub is None: return None
    # scale anchor: total ≈ 1 (fractions, the norm) or ≈ 100 (percent filers)
    anchor = vals.get("total")
    if anchor is None: anchor = (prom or 0) + (pub or 0)
    if 0.90 <= anchor <= 1.10: scale = 100.0
    elif 90.0 <= anchor <= 110.0: scale = 1.0
    else: return None
    prom = (prom or 0) * scale
    pub = (pub if pub is not None else max(0.0, 100.0 - prom)) * (scale if vals.get("pub") is not None else 1)
    out = {"prom": prom, "pub": pub}
    for k in ("fii", "dii", "mf", "ins"):
        out[k] = (vals.get(k) or 0.0) * scale
    if out["fii"] + out["dii"] > out["pub"] + 2.0: return None      # institutions can't exceed public
    if not (98.0 <= out["prom"] + out["pub"] <= 102.0): return None  # partition sanity (emp trusts ≤~2%)
    return {k: round(v, 2) for k, v in out.items()}

# ------------------------------------------------------------------ main fetch
def refresh_quarters(qes):
    jar = B.nse_jar()
    hist = load_hist()
    names = hist.setdefault("_names", {})
    before = cells_of(hist)
    stats = []

    for qe in qes:
        recs = fetch_master(jar, qe)
        # newest submission per symbol wins (revisions re-file the same (sym, qe))
        best = {}
        for r in recs:
            sym = str(r.get("symbol") or "").strip().upper()
            sub = iso_date(r.get("submissionDate")) or iso_date(r.get("broadcastDate"))
            xb = str(r.get("xbrl") or "").strip()
            if not sym or not sub or not xb.lower().startswith("http"): continue
            cur = best.get(sym)
            if cur is None or sub >= cur["sub"]:
                best[sym] = {"sub": sub, "xb": xb, "name": re.sub(r"\s+", " ", str(r.get("name") or "")).strip()}
        todo = []
        for sym, r in best.items():
            have = (hist.get(sym) or {}).get(qe)
            if have and str(have[5]) >= r["sub"]: continue  # already have this or a newer submission
            todo.append((sym, r))
        print("%s: %d filings, %d new/revised to parse" % (qe, len(best), len(todo)))
        stats.append((qe, len(best), len(todo)))

        done = skip = 0
        def work(item):
            sym, r = item
            try:
                txt = fetch_xbrl(r["xb"], jar)
                return sym, r, parse_shp(txt, qe)
            except Exception as e:
                return sym, r, ("ERR", repr(e))
        with ThreadPoolExecutor(max_workers=THREADS) as ex:
            for fut in as_completed([ex.submit(work, it) for it in todo]):
                sym, r, res = fut.result()
                if isinstance(res, dict):
                    hist.setdefault(sym, {})[qe] = [res["prom"], res["fii"], res["dii"],
                                                    res["mf"], res["ins"], r["sub"]]
                    if r["name"]: names[sym] = r["name"]
                    done += 1
                    if done % FLUSH_EVERY == 0:
                        save_hist(hist)
                        print("  ... %d/%d parsed (flushed)" % (done, len(todo)))
                else:
                    skip += 1
                    why = res[1] if isinstance(res, tuple) else "no-anchor/old-format"
                    if skip <= 12: print("  SKIP %s %s: %s" % (sym, qe, why))
        print("%s: +%d cells, %d skipped" % (qe, done, skip))
        save_hist(hist)

    after = cells_of(hist)
    if after < before:
        print("ABORT: history would shrink %d -> %d — not writing" % (before, after))
        sys.exit(1)
    save_hist(hist)
    print("history: %d cells (%+d), %d symbols" % (after, after - before, sum(1 for k in hist if not k.startswith("_"))))
    return stats

# ------------------------------------------------------------------ page feed
def build_feed():
    hist = load_hist()
    names = hist.get("_names", {})
    meta = {}
    try:
        slim = json.loads(gzip.decompress(open(SLIM, "rb").read()))
        for k, m in (slim.get("meta") or {}).items():
            sym = str(m.get("symbol") or k.split(".")[0]).upper()
            meta[sym] = (m.get("name"), m.get("mcap"))
    except Exception as e:
        print("WARN dash_slim unavailable (%s) — names from master, no mcap" % e)
    try:
        cl = json.load(open(CLASSIF, encoding="utf-8"))
        sector = {s: (v.get("macro") or "") for s, v in cl.items()}
    except Exception:
        sector = {}

    all_qes = sorted({qe for s, qs in hist.items() if not s.startswith("_") for qe in qs}, reverse=True)
    quarters = all_qes[:FEED_QUARTERS]
    rows = []
    for sym, qs in hist.items():
        if sym.startswith("_") or not isinstance(qs, dict): continue
        cells = [qs.get(qe) or 0 for qe in quarters]
        if not any(cells): continue
        nm, mc = meta.get(sym, (None, None))
        rows.append([sym, nm or names.get(sym) or sym, mc or 0, sector.get(sym) or "", cells])
    rows.sort(key=lambda r: -(r[2] or 0))
    if len(rows) < MIN_FEED_ROWS and os.path.exists(OUT) and os.path.getsize(OUT) > 200000:
        print("ABORT feed: only %d rows — keeping existing docs/shareholding.json" % len(rows))
        return False
    ist = datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)
    out = {"updated": ist.strftime("%Y-%m-%d %H:%M IST"), "quarters": quarters, "rows": rows}
    json.dump(out, open(OUT, "w", encoding="utf-8"), ensure_ascii=False, separators=(",", ":"))
    print("WROTE %s: %d rows x %d quarters, %.1f KB" %
          (os.path.normpath(OUT), len(rows), len(quarters), os.path.getsize(OUT) / 1e3))
    return True

def write_meta(stats):
    """Tiny always-changing marker so feeds.json can watch pipeline liveness cheaply."""
    ist = datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)
    hist = load_hist()
    out = {"checked": ist.strftime("%Y-%m-%d %H:%M IST"), "cells": cells_of(hist),
           "quarters": {qe: n for qe, n, _ in stats}}
    json.dump(out, open(META_OUT, "w", encoding="utf-8"), separators=(",", ":"))
    print("WROTE %s" % os.path.normpath(META_OUT))

if __name__ == "__main__":
    args = sys.argv[1:]
    if "--feed-only" in args:
        build_feed()
    else:
        n = TOPUP_QES
        if "--backfill" in args:
            n = int(args[args.index("--backfill") + 1])
        qes = last_qes(n)
        print("quarter-ends:", ", ".join(qes))
        stats = refresh_quarters(qes)
        build_feed()
        write_meta(stats)
