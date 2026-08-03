# -*- coding: utf-8 -*-
"""
Coverage campaign STEP 5 (full-depth): FII/DII holdings 2010-12 -> 2016-03 from Wayback-archived
Moneycontrol shareholding-pattern pages.

Why this source (probe trail in scripts/COVERAGE_CAMPAIGN.md progress log + memory
`project-stocks-shp-bse-backfill`): BSE deleted all pre-2016 SHP data from its current systems
(quarter labels exist, every data endpoint returns empty templates); NSE's APIs are rolling-window
only; BSE's own aspx pages were never archived with old qtrids. Moneycontrol's old company-facts
pages, however, server-rendered the FULL Clause-35 table (Promoter total, MF/UTI, FI/Banks,
Insurance, FII rows + Institutions subtotal) and the Wayback Machine holds ~134k captures of them
across ~7,000 companies, dense from 2011 to 2016, with explicit per-quarter URLs
(/company-facts/<slug>/shareholding-pattern/<scId>/<qtrid>.00 — the qtrid numbering IS BSE's own:
29=Mar-2001, 75=Sep-2012, 88=Dec-2015, 90=Jun-2016).

DATE CONVENTION (decided 2026-08-02, user driving full coverage; flagged per-cell): these pages
carry no submission dates (BSE deleted them). Visibility date = QUARTER-END + 21 DAYS — the
Clause-35 filing deadline of that era. Every cell in this ledger is marked approx; regenerating
with a different lag is a one-flag re-run (--lag N). Monthly-rebalance backtests behave identically
for +21 vs +30 (both reveal the quarter at the following month-end).

Identity guard: every parsed page must state the expected BSE scripcode / NSE symbol / ISIN in its
header line ("BSE: 500325|NSE: RELIANCE|ISIN: INE002A01018") — mismatch -> page discarded (same
philosophy as bse_fetch.py's identity guard).

Subcommands (run in order; everything cached under scripts/_shp_wb_cache/, all resumable):
  python fetch_shp_wayback_mc.py map        # N500 2009-2016 members -> MC (slug, scId), autosuggest + CDX-slug name match
  python fetch_shp_wayback_mc.py frontier   # build the (slug, qtrid) -> capture fetch list from the CDX census
  python fetch_shp_wayback_mc.py sample 8   # fetch+parse a few pages, print results (prototype check)
  python fetch_shp_wayback_mc.py harvest    # full paced fetch (hours; single-thread, 429-backoff; resumable)
  python fetch_shp_wayback_mc.py ledger     # seam-calibrate column convention vs 2016+ XBRL cells, write ledger
"""
import os, sys, json, re, gzip, time, datetime, urllib.request, urllib.parse
from collections import defaultdict, Counter

HERE = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(HERE, "_shp_wb_cache"); os.makedirs(CACHE, exist_ok=True)
PAGES = os.path.join(CACHE, "pages"); os.makedirs(PAGES, exist_ok=True)
CDX_FILE = os.path.join(CACHE, "cdx_mc.json")
MAP_FILE = os.path.join(CACHE, "map.json")
FRONTIER_FILE = os.path.join(CACHE, "frontier.json")
PARSED_FILE = os.path.join(CACHE, "parsed.json")
LEDGER_OUT = os.path.join(HERE, "shp_fill_hist_2010_2016.json.gz")
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
LAG_DAYS = 21 if "--lag" not in sys.argv else int(sys.argv[sys.argv.index("--lag") + 1])

def http_get(url, timeout=60, accept="text/html"):
    r = urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": UA, "Accept": accept}), timeout=timeout)
    raw = r.read()
    if r.headers.get("Content-Encoding") == "gzip": raw = gzip.decompress(raw)
    return raw.decode("utf-8", "replace")

def qe_of_qtrid(q):
    """BSE quarter-id -> quarter-end date. 29 = Mar-2001, +1 per quarter (75=Sep-2012, 90=Jun-2016)."""
    m = 3 + 3 * (q - 29)
    y = 2001 + (m - 1) // 12
    mm = ((m - 1) % 12) + 1
    return datetime.date(y, mm, {3: 31, 6: 30, 9: 30, 12: 31}[mm])

def qtrid_of_label(label):
    """'September 2012' -> 75. Returns None if not a clean quarter label."""
    m = re.match(r"(March|June|September|December)\s+(20\d\d)$", label.strip())
    if not m: return None
    mm = {"March": 3, "June": 6, "September": 9, "December": 12}[m.group(1)]
    return 29 + (int(m.group(2)) - 2001) * 4 + (mm - 3) // 3

def norm_name(s):
    s = re.sub(r"\b(ltd|limited|the|co|company|corp|corporation|india|&)\b", "", (s or "").lower())
    return re.sub(r"[^a-z0-9]", "", s)

# ---------------------------------------------------------------- map
def cmd_map():
    D = json.loads(gzip.decompress(urllib.request.urlopen(urllib.request.Request(
        "https://dhruvan246.github.io/stocks-dashboard/dash_slim.bin", headers={"User-Agent": UA}), timeout=90).read()))
    snaps = [s for s in D["indicesHistory"]["Nifty 500"] if "2009-01-01" <= s["effectiveDate"] <= "2017-01-01"]
    members = set()
    for s in snaps: members.update(s["symbols"])
    members.discard("Symbol")
    print("N500 members in any 2009-2016 snapshot:", len(members), flush=True)

    # normalize old symbols to CURRENT bin keys via the rename map (transitive)
    try:
        ren = json.load(open(os.path.join(HERE, "_rename_map.json")))
    except Exception:
        ren = {}
    def cur_sym(s):
        seen = set()
        while s in ren and s not in seen:
            seen.add(s); s = ren[s]
        return s
    targets = {}
    for s in members: targets.setdefault(cur_sym(s), set()).add(s)
    print("distinct current-key targets:", len(targets), flush=True)

    scrips = json.load(open(os.path.join(HERE, "bse_scrips.json"), encoding="utf-8"))
    by_id = scrips["by_id"]
    isin_of = {}
    for isin, v in scrips.get("by_isin", {}).items():
        isin_of[v if isinstance(v, str) else str(v)] = isin

    # company names for the name-match fallback (shp_history _names + slug list from CDX)
    try:
        names = json.load(open(os.path.join(HERE, "shp_history.json"), encoding="utf-8")).get("_names", {})
    except Exception:
        names = {}
    cdx = json.load(open(CDX_FILE))
    slug_index = {}
    for orig, ts in cdx:
        m = re.search(r"company-facts/([^/]+)/shareholding", orig)
        if m: slug_index.setdefault(norm_name(m.group(1)), m.group(1))

    out = json.load(open(MAP_FILE)) if os.path.exists(MAP_FILE) else {}
    todo = [t for t in sorted(targets) if t not in out]
    print("to map:", len(todo), flush=True)
    for i, sym in enumerate(todo):
        entry = None
        code = by_id.get(sym)
        for q in {sym} | targets[sym]:
            try:
                t = http_get("https://www.moneycontrol.com/mccode/common/autosuggestion_solr.php?classic=true&query=%s&type=1&format=json"
                             % urllib.parse.quote(str(q)), timeout=25)
                for cand in json.loads(t) or []:
                    dis = cand.get("pdt_dis_nm") or ""
                    link = cand.get("link_src") or ""
                    scid = cand.get("sc_id") or ""
                    mslug = re.search(r"stockpricequote/[^/]+/([^/]+)/", link)
                    # identity: displayed line carries "ISIN, NSESYM, SCRIPCODE"
                    ok = False
                    if code and re.search(r"\b%d\b" % code, dis): ok = True
                    if re.search(r"\b%s\b" % re.escape(str(q)), dis): ok = True
                    if ok and mslug and scid:
                        entry = {"slug": mslug.group(1), "scid": scid, "via": "autosuggest:%s" % q}
                        break
            except Exception:
                pass
            if entry: break
            time.sleep(0.35)
        if not entry:
            nm = names.get(sym) or ""
            slug = slug_index.get(norm_name(nm)) or slug_index.get(norm_name(sym))
            if slug:
                entry = {"slug": slug, "scid": None, "via": "cdx-name-match"}
        out[sym] = entry
        if (i + 1) % 25 == 0:
            json.dump(out, open(MAP_FILE, "w"), indent=0)
            print("  ...%d/%d mapped (%d ok)" % (i + 1, len(todo), sum(1 for v in out.values() if v)), flush=True)
    json.dump(out, open(MAP_FILE, "w"), indent=0)
    ok = sum(1 for v in out.values() if v)
    print("MAP DONE: %d/%d resolved (%d unmapped — reported, not guessed)" % (ok, len(out), len(out) - ok), flush=True)

# ---------------------------------------------------------------- frontier
def cmd_frontier():
    cdx = json.load(open(CDX_FILE))
    mp = json.load(open(MAP_FILE))
    want_slugs = {v["slug"]: sym for sym, v in mp.items() if v}
    print("mapped slugs:", len(want_slugs), flush=True)

    # captures per slug: explicit-qtrid ones directly keyed; no-qtrid kept as candidates
    explicit = defaultdict(dict)   # slug -> qtrid -> (ts, original)
    generic = defaultdict(list)    # slug -> [(ts, original)]
    for orig, ts in cdx:
        m = re.search(r"company-facts/([^/]+)/shareholding-pattern/([^/?#]+)(?:/(\d+))?", orig)
        if not m: continue
        slug = m.group(1)
        if slug not in want_slugs: continue
        q = m.group(3)
        if q:
            q = int(q)
            if 29 <= q <= 89 and (q not in explicit[slug] or ts < explicit[slug][q][0]):
                explicit[slug][q] = (ts, orig)
        else:
            generic[slug].append((ts, orig))

    frontier = []
    for slug, sym in want_slugs.items():
        for q, (ts, orig) in explicit[slug].items():
            frontier.append({"sym": sym, "slug": slug, "qtrid": q, "ts": ts, "url": orig})
        # generic captures fill quarters the explicit set misses: spread them by capture date, keep
        # at most one per quarter-of-capture (the page shows the then-latest quarter)
        seen_q = set(explicit[slug])
        by_qtr_bucket = {}
        for ts, orig in sorted(generic[slug]):
            d = datetime.date(int(ts[:4]), int(ts[4:6]), int(ts[6:8]))
            approx_q = 29 + (d.year - 2001) * 4 + (d.month - 1) // 3 - 1   # quarter probably displayed
            if approx_q in seen_q or approx_q in by_qtr_bucket: continue
            by_qtr_bucket[approx_q] = (ts, orig)
        for q, (ts, orig) in by_qtr_bucket.items():
            frontier.append({"sym": sym, "slug": slug, "qtrid": None, "ts": ts, "url": orig})
    json.dump(frontier, open(FRONTIER_FILE, "w"), indent=0)
    n_exp = sum(1 for f in frontier if f["qtrid"] is not None)
    print("FRONTIER: %d fetches (%d explicit-qtrid, %d generic)" % (len(frontier), n_exp, len(frontier) - n_exp), flush=True)

# ---------------------------------------------------------------- parse
ROW_LABELS = {
    "prom": r"Total shareholding of Promoter and Promoter Group\s*\(A\)",
    "mf": r"Mutual Funds\s*/\s*UTI",
    "banks": r"Financial Institutions\s*/\s*Banks",
    "govt": r"Central Government\s*/\s*State Government",
    "ins": r"Insurance Companies",
    "fii": r"Foreign Institutional Investors",
    "qfi": r"Qualified Foreign Investor",
    "inst_sub": r"\(1\)\s*Institutions.*?Sub Total",   # handled specially below
}

def parse_mc_page(html, expect_code=None, expect_syms=(), slug=None):
    """-> dict(qtrid, cols{slot: (pctAB, pctABC)}) or None. Identity-guarded."""
    # identity, strongest first: "BSE: 500325|NSE: RELIANCE" line (2012+ pages); fallback for the
    # 2011-era pages (no codes line): the <title>'s company name must match the URL slug we chose
    # ("3M India >> Shareholding Pattern - ..." vs slug "3mindia") — the slug itself was identity-
    # guarded against scripcode/ISIN at map time.
    ident = re.search(r"BSE:\s*(\d{6})\s*(?:\||&#124;)?\s*NSE:\s*([A-Z0-9&\-]+)", html)
    tm = re.search(r"<title>\s*([^<]*?)\s*(?:&gt;&gt;|>>)\s*Shareholding Pattern\s*-\s*((?:March|June|September|December)\s+20\d\d)", html)
    if not tm:
        tm2 = re.search(r"Shareholding Pattern\s*-\s*((?:March|June|September|December)\s+20\d\d)", html)
        if not tm2: return None
        title_co, qlabel = None, tm2.group(1)
    else:
        title_co, qlabel = tm.group(1), tm.group(2)
    if ident:
        code, nsym = int(ident.group(1)), ident.group(2).strip().upper()
        if expect_code and code != expect_code and nsym not in {s.upper() for s in expect_syms}:
            return None
    elif expect_code:
        tco = norm_name(title_co or "")
        tsl = norm_name(slug or "")
        if not tco or not tsl or not (tco in tsl or tsl in tco):
            return None
    q = qtrid_of_label(qlabel)
    if not q: return None

    import html as _html
    text = re.sub(r"<[^>]+>", "\x01", html)
    cells = [re.sub(r"[\s\xa0]+", " ", _html.unescape(c)).strip() for c in text.split("\x01")]
    cells = [c for c in cells if c]

    def row_nums(i):
        nums = []
        for c2 in cells[i + 1:i + 9]:
            c2 = c2.replace(",", "").strip()
            if c2 in ("-", ""): nums.append(None); continue
            if re.fullmatch(r"\d+(?:\.\d+)?", c2): nums.append(float(c2))
            else: break
        return nums

    def pair(nums):
        # row shape: n_holders, total_shares, demat_shares, pct(A+B)[, pct(A+B+C)][, pledged...]
        if len(nums) >= 5: return (nums[3], nums[4])
        if len(nums) == 4: return (nums[3], nums[3])
        return None

    def find_row(label_rx, lo=0, hi=None):
        rx = re.compile(label_rx, re.I)
        for i in range(lo, hi if hi is not None else len(cells)):
            if rx.search(cells[i]):
                p = pair(row_nums(i))
                if p: return p
        return None

    out = {}
    # promoter total: anywhere on the page (unique label)
    p = find_row(ROW_LABELS["prom"])
    if p: out["prom"] = p
    # institution rows ONLY inside the "(1) Institutions" ... "Sub Total" block — for PSUs the
    # PROMOTER block also contains a "Central Government / State Government(s)" row (President of
    # India), which must not be read as the institutional-government row.
    blk_lo = blk_hi = None
    for rx in (r"\(1\)\s*Institutions?\s*$", r"^Institutions$"):   # prefer the explicit (1) marker
        for i, c in enumerate(cells):
            if re.search(rx, c, re.I):
                blk_lo = i
                for j in range(i + 1, min(i + 160, len(cells))):
                    if re.fullmatch(r"Sub\s*Total", cells[j], re.I):
                        blk_hi = j + 1; break
                break
        if blk_lo is not None: break
    if blk_lo is not None:
        hi = blk_hi if blk_hi is not None else min(blk_lo + 160, len(cells))
        for slot in ("mf", "banks", "govt", "ins", "fii", "qfi"):
            p = find_row(ROW_LABELS[slot], blk_lo, hi)
            if p: out[slot] = p
        if blk_hi is not None:
            p = pair(row_nums(blk_hi - 1))
            if p: out["inst_sub"] = p
    if "fii" not in out and "mf" not in out: return None
    return {"qtrid": q, "cols": out}

# ---------------------------------------------------------------- fetch machinery
def wb_fetch(ts, orig):
    # Pages are cached GZIPPED — the raw-HTML cache hit 1.23 GB and filled the disk mid-run
    # (OSError 28) on 2026-08-03. Plain .html files from before that are still read if present.
    key = re.sub(r"[^A-Za-z0-9]+", "_", orig[-90:]) + "_" + ts
    cf = os.path.join(PAGES, key + ".html.gz")
    legacy = os.path.join(PAGES, key + ".html")
    if os.path.exists(cf):
        with gzip.open(cf, "rt", encoding="utf-8") as fh: return fh.read(), True
    if os.path.exists(legacy):
        return open(legacy, encoding="utf-8").read(), True
    url = "https://web.archive.org/web/%sid_/%s" % (ts, orig)
    delay = 60
    for attempt in range(6):
        try:
            html = http_get(url, timeout=90)
            with gzip.open(cf + ".tmp", "wt", encoding="utf-8", compresslevel=6) as fh: fh.write(html)
            os.replace(cf + ".tmp", cf)
            return html, False
        except urllib.error.HTTPError as e:
            if e.code in (429, 503):
                print("  rate-limited (%d), sleeping %ds" % (e.code, delay), flush=True)
                time.sleep(delay); delay = min(delay * 2, 600)
            elif e.code == 404:
                with gzip.open(cf, "wt", encoding="utf-8") as fh: fh.write("")   # negative-cache
                return "", False
            else:
                raise
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as e:
            # transient network trouble (Wayback outage windows) — brief in-run retry; if it
            # persists, raise so the caller logs FETCH FAIL and a later re-run picks it up
            if attempt >= 2: raise
            print("  net-error (%r), sleeping 25s" % e, flush=True)
            time.sleep(25)
    raise RuntimeError("gave up on %s" % url)

def cmd_sample(n):
    frontier = json.load(open(FRONTIER_FILE))
    scrips = json.load(open(os.path.join(HERE, "bse_scrips.json"), encoding="utf-8"))["by_id"]
    picks = frontier[:: max(1, len(frontier) // n)][:n]
    for f in picks:
        html, cached = wb_fetch(f["ts"], f["url"])
        res = parse_mc_page(html, expect_code=scrips.get(f["sym"]), expect_syms=(f["sym"],), slug=f["slug"])
        print(f["sym"], f["slug"], "qtrid=", f["qtrid"], "ts=", f["ts"], "->",
              (dict(qtrid=res["qtrid"], **{k: v for k, v in res["cols"].items()}) if res else "PARSE-NONE"),
              flush=True)
        if not cached: time.sleep(1.3)

def cmd_harvest():
    frontier = json.load(open(FRONTIER_FILE))
    scrips = json.load(open(os.path.join(HERE, "bse_scrips.json"), encoding="utf-8"))["by_id"]
    parsed = json.load(open(PARSED_FILE)) if os.path.exists(PARSED_FILE) else {}
    done_urls = set(parsed.get("_done", []))
    cells = parsed.get("cells", {})
    n_new = 0
    for i, f in enumerate(frontier):
        uk = f["ts"] + "|" + f["url"]
        if uk in done_urls: continue
        try:
            html, cached = wb_fetch(f["ts"], f["url"])
        except Exception as e:
            print("  FETCH FAIL %s: %r" % (f["url"][:90], e), flush=True)
            continue
        res = parse_mc_page(html, expect_code=scrips.get(f["sym"]), expect_syms=(f["sym"],), slug=f["slug"]) if html else None
        if res:
            q = res["qtrid"]
            if 29 <= q <= 89:   # pre-Jun-2016 only; 2016+ comes from the XBRL ledger
                key = "%s|%d" % (f["sym"], q)
                if key not in cells:
                    cells[key] = {"cols": res["cols"], "src": "wb:%s:%s" % (f["ts"], f["slug"])}
                    n_new += 1
        done_urls.add(uk)
        if not cached: time.sleep(1.25)
        if (i + 1) % 100 == 0:
            parsed = {"_done": sorted(done_urls), "cells": cells}
            json.dump(parsed, open(PARSED_FILE, "w"))
            print("  ...%d/%d fetched, %d cells" % (i + 1, len(frontier), len(cells)), flush=True)
    json.dump({"_done": sorted(done_urls), "cells": cells}, open(PARSED_FILE, "w"))
    print("HARVEST DONE: %d cells across %d fetches" % (len(cells), len(done_urls)), flush=True)

# ---------------------------------------------------------------- ledger (+ seam calibration)
def cmd_ledger():
    parsed = json.load(open(PARSED_FILE))
    cells = parsed["cells"]
    hist = json.load(open(os.path.join(HERE, "shp_history.json"), encoding="utf-8"))

    # ⚠ SOURCE DEFECT, found by this gate on the first run (2026-08-03): for qtrid 88 (Dec-2015)
    # and 89 (Mar-2016) Moneycontrol's own pages carry an EMPTY "Foreign Institutional Investors"
    # row — all "-" — for EVERY company including large caps with obvious foreign holding (verified
    # on ACC, both quarters). MC's pipeline evidently broke when SEBI restructured the format; the
    # data is absent at source, not mis-parsed. FII fill-rate is 94-99% for every qtrid <= 87 and
    # exactly 0% at 88/89. Those two quarters are DROPPED — writing them would fabricate fii=0 for
    # ~840 company-quarters, the precise failure mode parse_shp's "never zero-default" rule exists
    # to prevent. Effective ledger window is therefore Dec-2010 .. Sep-2015.
    USABLE_MAX_Q = 87

    # SEAM CALIBRATION. The two candidate columns are "% of (A+B)" and "% of (A+B+C)", where C is
    # the GDR/custodian block; they are IDENTICAL for companies without GDRs, so the choice only
    # matters for the GDR subset — which is where this measures. Anchor = the trusted 2016 XBRL
    # cells; nearest usable harvested quarter is Sep-2015, so a ~3-quarter drift is baked into the
    # absolute numbers and only the RELATIVE AB-vs-ABC comparison is meaningful.
    best = {}
    for key, c in cells.items():
        sym, q = key.split("|"); q = int(q)
        if q > USABLE_MAX_Q: continue
        p = c["cols"].get("fii")
        if not p or (p[0] is None and p[1] is None): continue
        if sym not in best or q > best[sym][0]: best[sym] = (q, p)
    diffs = {"AB": [], "ABC": []}
    gdr = {"AB": [], "ABC": []}
    for sym, (q, p) in best.items():
        anchor = None
        for qe in ("2016-06-30", "2016-09-30"):
            v = (hist.get(sym) or {}).get(qe)
            if v: anchor = v; break
        if not anchor: continue
        ab, abc = p[0], p[1]
        if ab is None or abc is None: continue
        diffs["AB"].append(abs(ab - anchor[1])); diffs["ABC"].append(abs(abc - anchor[1]))
        if abs(ab - abc) > 0.5:                       # GDR company: the columns actually differ
            gdr["AB"].append(abs(ab - anchor[1])); gdr["ABC"].append(abs(abc - anchor[1]))
    def med(v):
        v = sorted(v); return v[len(v) // 2] if v else None
    mAB, mABC, gAB, gABC = med(diffs["AB"]), med(diffs["ABC"]), med(gdr["AB"]), med(gdr["ABC"])
    print("seam vs 2016 XBRL fii — all n=%d: AB=%s ABC=%s | GDR-subset n=%d: AB=%s ABC=%s"
          % (len(diffs["AB"]), mAB, mABC, len(gdr["AB"]), gAB, gABC), flush=True)
    if mAB is None or len(diffs["AB"]) < 50:
        raise SystemExit("STOP: too little seam overlap (%d) to calibrate" % len(diffs["AB"]))
    if gABC is None or len(gdr["AB"]) < 5:
        raise SystemExit("STOP: no GDR-subset overlap — cannot discriminate the two column conventions")
    use_abc = gABC <= gAB
    col = 1 if use_abc else 0
    print("chosen column convention:", "%of(A+B+C)" if use_abc else "%of(A+B)", flush=True)
    # Drift over ~3 quarters is expected; a definition mismatch would show up far larger than this.
    if min(mAB, mABC) > 4.0:
        raise SystemExit("STOP: best convention still has median seam %.2fpp — definitions don't line up, refusing to write" % min(mAB, mABC))

    fills = defaultdict(dict)
    dropped = Counter()
    for key, c in cells.items():
        sym, q = key.split("|"); q = int(q)
        if q > USABLE_MAX_Q:
            dropped["qtr88-89-source-has-no-fii"] += 1; continue
        qe = qe_of_qtrid(q)
        cols = c["cols"]
        def val(slot):
            p = cols.get(slot)
            if not p: return None
            return p[col] if p[col] is not None else p[1 - col]
        # fii is the primary factor AND proof the institutions block parsed — never default it to
        # 0.0 (that is indistinguishable from a genuine zero holding and silently poisons screens).
        fii = val("fii")
        if fii is None:
            dropped["no-fii"] += 1; continue
        prom = val("prom")
        if prom is None:
            dropped["no-promoter-total"] += 1; continue
        mf = val("mf") or 0.0
        dii = mf + (val("banks") or 0.0) + (val("ins") or 0.0)
        # reconciliation gate mirroring parse_shp: itemized institutions ≈ subtotal (when present)
        inst = val("inst_sub")
        if inst is not None:
            item_sum = fii + dii + (val("govt") or 0.0) + (val("qfi") or 0.0)
            if abs(item_sum - inst) > 1.0:
                dropped["inst-recon"] += 1; continue
        if not (0.0 <= prom <= 100.0) or not (0.0 <= fii <= 100.0) or not (0.0 <= dii <= 100.0):
            dropped["out-of-range"] += 1; continue
        sub = (qe + datetime.timedelta(days=LAG_DAYS)).isoformat()
        fills[sym][qe.isoformat()] = [round(prom, 2), round(fii, 2), round(dii, 2), round(mf, 2),
                                       round(val("ins") or 0.0, 2), sub, None,
                                       c["src"] + ":approx+%dd" % LAG_DAYS]
    total = sum(len(v) for v in fills.values())
    meta = {"window": ["2010-12-31", "2015-09-30"], "source": "wayback moneycontrol company-facts",
            "date_convention": "QE+%dd (SEBI Clause-35 deadline) — APPROXIMATE, no real filing dates exist" % LAG_DAYS,
            "column_convention": "%of(A+B+C)" if use_abc else "%of(A+B)",
            "seam_median_pp": {"all_AB": mAB, "all_ABC": mABC, "gdr_AB": gAB, "gdr_ABC": gABC}, "companies": len(fills), "cells": total,
            "dropped": dict(dropped)}
    with gzip.open(LEDGER_OUT, "wt", encoding="utf-8") as fh:
        json.dump({"_meta": meta, "fills": fills}, fh, separators=(",", ":"))
    print("LEDGER: %d companies, %d cells -> %s" % (len(fills), total, LEDGER_OUT), flush=True)
    print(json.dumps(meta, indent=1), flush=True)

if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd == "map": cmd_map()
    elif cmd == "frontier": cmd_frontier()
    elif cmd == "sample": cmd_sample(int(sys.argv[2]) if len(sys.argv) > 2 else 6)
    elif cmd == "harvest": cmd_harvest()
    elif cmd == "ledger": cmd_ledger()
    else: print(__doc__)
