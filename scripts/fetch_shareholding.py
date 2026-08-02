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
       {"_names": {SYM: co-name}, SYM: {"YYYY-MM-DD"(QE): [prom, fii, dii, mf, ins, "sub-date", nsh?]}}
     nsh = total number of shareholders (ShareholdingPatternMember context), appended only when
     the filing carries it — cells written before 2026-07-16 have 6 slots, readers index 0-5 + optional 6.
  4. Build docs/shareholding.json (slim page feed, aligned quarter arrays + mcap/sector join)
     and docs/shp_meta.json (tiny freshness marker committed every run, feeds.json watches it).

Runs:
  python -X utf8 scripts/fetch_shareholding.py                # daily top-up (last 3 QEs, new/revised only)
  python -X utf8 scripts/fetch_shareholding.py --backfill 4   # one-time deep fill (most-recent quarter first)
  python -X utf8 scripts/fetch_shareholding.py --backfill 4 --reparse  # re-fetch even unchanged filings (schema upgrades)
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

# XBRL category member -> output slot.
# NEW format (quarters >= 2022-09-30): explicit Institutions (Domestic) / (Foreign) split.
# OLD format (<= 2022-06-30): ONE InstitutionsMember bucket with per-type rows (values in
# PERCENT, not fractions) — FII = the Institutions FPI row (+FVCI), DII = the domestic rows.
# OtherInstitutionsMember assignment was CALIBRATED on the Jun→Sep-2022 seam (see runbook §22).
MEMBERS = {
    "ShareholdingOfPromoterAndPromoterGroupMember": "prom",
    "PublicShareholdingMember": "pub",
    "InstitutionsForeignMember": "fii",
    "InstitutionsDomesticMember": "dii",
    "MutualFundsOrUTIMember": "mf",
    "InsuranceCompaniesMember": "ins",
    "ShareholdingPatternMember": "total",
    # old-format members (collected when present; combined in parse_shp)
    "InstitutionsMember": "o_inst",
    "InstitutionsForeignPortfolioInvestorMember": "o_fpi",
    "ForeignVentureCapitalInvestorsMember": "o_fvci",
    "MutualFundsOrUtiMember": "o_mf",          # note Uti vs UTI casing difference
    "AlternativeInvestmentFundsMember": "o_aif",
    "VentureCapitalFundsMember": "o_vcf",
    "FinancialInstitutionOrBanksMember": "o_bank",
    "ProvidentFundsOrPensionFundsMember": "o_pf",
    "OtherInstitutionsMember": "o_other",
    # non-promoter-non-public bucket (employee benefit trusts / DR custodians) — sits OUTSIDE
    # "public" in the SEBI partition. Needed so no-promoter companies with a big ESOP trust
    # (ETERNAL 4.73%, SWIGGY, ...) pass the partition sanity instead of being skipped.
    "SharesHeldByNonPromoterNonPublicShareholdersMember": "npnp",
    "EmployeeBenefitsTrustsMember": "trust",
    "SharesHeldByEmployeeTrustsMember": "trust",   # old-format name
}
# Where does the old format's "Other institutions" row belong? True → DII, False → FII.
# Calibrated 2026-07-17 on the format-boundary seam (Jun-2022 old vs Sep-2022 new, all stocks).
OLD_OTHER_TO_DII = True

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
        tmp = HIST + ".tmp.%d" % os.getpid()   # pid-suffixed: concurrent runs must not steal each other's tmp
        json.dump(h, open(tmp, "w", encoding="utf-8"), ensure_ascii=False, separators=(",", ":"))
        # Windows: os.replace fails (WinError 5) while ANY other process holds the target
        # open for reading (a builder/page-feed run loading the json). Retry briefly.
        for i in range(12):
            try:
                os.replace(tmp, HIST)
                return
            except PermissionError:
                time.sleep(1 + i)
        os.replace(tmp, HIST)  # last try — surface the error if it truly won't release

# Coverage campaign STEP 5: historical backfills the NSE master API (fetch_master, below) can't
# reach — it only ever serves a recent rolling window, never historical quarters (confirmed:
# querying it for old quarter-ends returns 0 rows even though those quarters plainly have filings).
# Ledgers, applied fill-only in ORDER (earlier ledgers win where quarters overlap), idempotent:
#   1. shp_fill_hist_2016_2019.json.gz — BSE SHPQNewFormat XBRL (fetch_shp_bse_hist.py). Real
#      per-filing dates. 2016-03..2019-06.
#   2. shp_fill_hist_2010_2016.json.gz — Wayback-archived Moneycontrol Clause-35 pages
#      (fetch_shp_wayback_mc.py). 2010-12..2016-03. ⚠ sub-dates are the QE+21d SEBI-deadline
#      CONVENTION (no real dates exist anywhere — BSE deleted them); each cell carries the
#      approx tag in the ledger's provenance slot, dropped on merge like the scripcode tag.
BSE_HIST_LEDGERS = [os.path.join(HERE, "shp_fill_hist_2016_2019.json.gz"),
                    os.path.join(HERE, "shp_fill_hist_2010_2016.json.gz")]
def apply_bse_hist_ledger(h):
    n_total = 0
    for path in BSE_HIST_LEDGERS:
        if not os.path.exists(path): continue
        try:
            with gzip.open(path, "rt", encoding="utf-8") as fh:
                fills = json.load(fh).get("fills", {})
        except Exception as e:
            print("%s unreadable (%s) — skipped" % (os.path.basename(path), e)); continue
        n = 0
        for sym, qs in fills.items():
            dest = h.setdefault(sym, {})
            for qe, cell in qs.items():
                if qe in dest: continue   # fill-only — never overwrite an existing cell
                dest[qe] = cell[:7] if cell[6] is not None else cell[:6]   # drop the ledger-only provenance tag (cell[7])
                n += 1
        if n: print("%s applied: %d cells" % (os.path.basename(path), n))
        n_total += n
    return n_total

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
    nsh = None  # total no. of shareholders (whole-company context)
    for f in root.iter():
        tag = strip(f.tag)
        if tag == "NumberOfShareholders":
            if (ctx.get(f.get("contextRef")) or "") == "ShareholdingPatternMember":
                try:
                    nsh = int(float(str(f.text).strip()))
                except (TypeError, ValueError):
                    pass
            continue
        if tag != "ShareholdingAsAPercentageOfTotalNumberOfShares": continue
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
    # scale anchor: total ≈ 1 (fractions, new format) or ≈ 100 (percent, old format + some filers)
    anchor = vals.get("total")
    if anchor is None: anchor = (prom or 0) + (pub or 0)
    if 0.90 <= anchor <= 1.10: scale = 100.0
    elif 90.0 <= anchor <= 110.0: scale = 1.0
    else: return None

    is_new = ("fii" in vals) or ("dii" in vals)          # explicit Domestic/Foreign facts
    is_old = (not is_new) and ("o_inst" in vals)         # single Institutions bucket
    if not (is_new or is_old): return None               # unknown vintage — SKIP, never zero-fill

    prom = (prom or 0) * scale
    pub = (pub if pub is not None else max(0.0, 100.0 - prom)) * (scale if vals.get("pub") is not None else 1)
    out = {"prom": prom, "pub": pub}
    if is_new:
        for k in ("fii", "dii", "mf", "ins"):
            out[k] = (vals.get(k) or 0.0) * scale
    else:
        g = lambda k: (vals.get(k) or 0.0) * scale
        fii = g("o_fpi") + g("o_fvci")
        dii = g("o_mf") + g("o_aif") + g("o_vcf") + g("o_bank") + g("ins") + g("o_pf")
        other = g("o_other")
        if OLD_OTHER_TO_DII: dii += other
        else: fii += other
        # old bucket must reconcile: fii+dii ≈ total Institutions (± rounding)
        if abs((fii + dii) - g("o_inst")) > 0.35: return None
        out.update({"fii": fii, "dii": dii, "mf": g("o_mf"), "ins": g("ins")})
    if out["fii"] + out["dii"] > out["pub"] + 2.0: return None      # institutions can't exceed public
    # partition sanity: promoter + public + non-promoter-non-public ≈ 100. The third bucket
    # (ESOP trusts / DR custodians) can be large for no-promoter companies (ETERNAL 4.73%).
    extra = max(vals.get("npnp") or 0.0, vals.get("trust") or 0.0) * scale
    if not (98.0 <= out["prom"] + out["pub"] + extra <= 102.0): return None
    out = {k: round(v, 2) for k, v in out.items()}
    if nsh and nsh > 0: out["nsh"] = nsh
    return out

# ------------------------------------------------------------------ main fetch
def refresh_quarters(qes, reparse=False):
    jar = B.nse_jar()
    hist = load_hist()
    apply_bse_hist_ledger(hist)   # STEP 5 2016-2019 backfill — fill-only, no-ops once applied
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
            if have and str(have[5]) >= r["sub"] and not reparse: continue  # already have this or newer
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
                    cell = [res["prom"], res["fii"], res["dii"], res["mf"], res["ins"], r["sub"]]
                    if res.get("nsh"): cell.append(res["nsh"])
                    hist.setdefault(sym, {})[qe] = cell
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

def build_engine_feed():
    """docs/shp_engine.json — the backtest engines' point-in-time FII/DII series:
    {SYM: [[qeInt, fii, dii, subInt], ...] sorted by quarter}. ALL quarters (not the page's 8);
    the engines gate on subInt <= rebalance date so there is no look-ahead."""
    hist = load_hist()
    out = {}
    for sym, qs in hist.items():
        if sym.startswith("_") or not isinstance(qs, dict): continue
        rows = []
        for qe, c in qs.items():
            try:
                rows.append([int(qe.replace("-", "")), c[1], c[2], int(str(c[5]).replace("-", ""))])
            except (ValueError, TypeError, IndexError):
                continue
        if rows: out[sym] = sorted(rows)
    ep = os.path.join(HERE, "..", "docs", "shp_engine.json")
    if len(out) < MIN_FEED_ROWS and os.path.exists(ep) and os.path.getsize(ep) > 200000:
        print("ABORT engine feed: only %d symbols — keeping existing shp_engine.json" % len(out))
        return
    json.dump(out, open(ep, "w", encoding="utf-8"), separators=(",", ":"))
    print("WROTE %s: %d symbols, %.0f KB" % (os.path.normpath(ep), len(out), os.path.getsize(ep) / 1e3))

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
    if "--hist" in args:   # write to an alternate history file (staging for conflict-free backfills)
        HIST = os.path.abspath(args[args.index("--hist") + 1])
        print("history file:", HIST)
    if "--feed-only" in args:
        build_feed()
        build_engine_feed()
    else:
        n = TOPUP_QES
        if "--backfill" in args:
            n = int(args[args.index("--backfill") + 1])
        if "--quarters" in args:
            qes = [q.strip() for q in args[args.index("--quarters") + 1].split(",") if q.strip()]
        else:
            qes = last_qes(n)
        print("quarter-ends:", ", ".join(qes))
        stats = refresh_quarters(qes, reparse="--reparse" in args)
        if "--hist" in args:
            print("(staging run — docs feed/meta NOT rebuilt)")
        else:
            build_feed()
            build_engine_feed()
            write_meta(stats)
