# -*- coding: utf-8 -*-
"""Shareholding (promoter / FII / DII / MF / insurance) and per-quarter SHARE COUNTS for every NSE-listed stock —
main board AND the SME platform — from NSE's own quarterly shareholding-pattern XBRL.  DATA_RUNBOOK §180.

Why this exists (user, 2026-09-26): "fill market cap and shareholding for all the stocks in our dashboard".
Measured on origin 47af401f1 before the fill (cells 2020Q1-2026Q2 vs quarters each stock traded):
  N500 99.2% · NSE main non-N500 94.7% · NSE SME 0.0% (548 symbols) · BSE-only 0.5% (2,179 symbols).
Share counts existed only as ONE latest number per symbol (shares_outstanding.json), so no stock had a
market-cap history.

Stages:
  master   <cache>/master/{equities,sme}_<QE>.json   NSE corporate-share-holdings-master, QE -> QE+180d filing
                                                     season, rows kept only when as-on == QE (§22 step 1)
  download <cache>/xbrl_nse_all/<SYM>_<QE>.xml.gz     every quarter-end filing listed; files already on disk
                                                     from earlier campaigns are REUSED, never re-downloaded
  build    scripts/shp_fill_allstocks.json.gz        fill ledger (BSE_HIST_LEDGERS, applied fill-only, LAST)
           scripts/_shp_allstocks_holds.json         every cell NOT written + the reason
           scripts/shares_history.json               {SYM: {QE: [shares, visible-date, src]}}
  The build stage is offline and read-only on the store; `fetch_shareholding.py --apply-ledgers` lands it.

Gates (each one names the past defect it prevents; checklist compiled from the runbook 2026-09-26):
  * parse = fetch_shareholding.parse_shp UNCHANGED (anchor ladder, partition [98,102], fii+dii <= pub+2,
    old-format +-0.35 reconciliation, 4dp share-count precision pass, DR line out of FII §151).
  * PROVEN ZERO (the only new rule): parse_shp refuses filings that carry no institution members at all,
    because "no institutions" and "unknown vintage" look alike (§22b — zero-DEFAULTING is forbidden). When
    the Public block's NumberOfShares EQUALS Non-institutions + Government (parent row) share for share, and
    promoter + public (+ the employee-trust / non-promoter-non-public bucket) is in [98,102], the document's own
    arithmetic (Public = Institutions + Government + Non-institutions, both the 2015 and the 2022 form) PROVES
    institutions = 0. Such a cell is written as fii = dii = mf = ins = 0 (zero_proof()).
    Hold-out test 2026-09-26 (see runbook §180): the equality never holds on any filing reporting an
    institutional holding; a 0.011pp percentage tolerance instead DID (59 filings holding 0.01-0.02%).
  * 2022-form filings whose public block does not close in SHARES (pub != inst-dom + inst-for + govt +
    non-inst) -> HOLD.
  * old-format (<= Jun-2022 submissions) filings carrying an institutional "Any Other" row or a
    non-institutional row LABELLED as a domestic/foreign institution (§158 R1/R2, §159) -> HOLD: the
    row-level placement heals exist only for Nifty-500 names and are not re-run here.
  * mf, ins <= dii + 0.05; prom + fii + dii <= 100.5.
  * nsh gate (§22g-2): shareholder count < 5% of the max over strictly earlier quarters -> HOLD (wrong class),
    unless the same filings show the share capital collapsing (insolvency capital reduction) or the symbol's event
    is on the cell_fix accept list -> the share-count-proven percentages are written WITHOUT the doubtful count.
  * continuity (§127g/§127j): fii > 5pp or dii > 10pp away from EVERY stored/candidate neighbour within two
    quarters -> HOLD; an exact 0 beside a neighbour > 1% without a partition proof -> HOLD.
  * date = the calendar day of NSE's broadcast (visible_iso, midnight rule §149), EXCEPT when the broadcast is a
    re-stamp of the original document (document created by the day after submission, broadcast more than two days
    after submission; §142h bulk re-stamps) -> the submission day. A document created after the submission is a
    re-filing and keeps its own broadcast day, so a re-filing's numbers are never served from the original's date
    (no look-ahead, §142j/§142k). Lag < 0 -> HOLD.
  * identity: the file's ISIN issuer (isin[:7]) must match an ISIN the symbol traded under (tape meta +
    the 2020+ bhavcopy symbol->ISIN map); the file's own Symbol tag, when present, must be the key or a
    rename-map relative of it. A quarter already stored under the key OR any former ticker (rename map +
    engine FUND_ALIAS) is skipped — never duplicated under a second key.
  * fill-only: a stored cell is never touched (the ledger mechanism enforces it too).
Share counts: parse_shares (whole-company NumberOfShares, fully-paid fallback), same identity gate; a count
that sits below 1/5 or above 5x BOTH neighbours within two quarters is HELD (stub / wrong-class filings).

Run:  python3 scripts/fetch_shp_allstocks.py master|download|build [--cache DIR]   (default cache ~/stocks-cache/shp/all_fill)
"""
import os, sys, re, json, gzip, time, argparse, collections, datetime
import xml.etree.ElementTree as ET

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import fetch_shareholding as F                                   # noqa: E402

CACHE = os.path.expanduser("~/stocks-cache/shp/all_fill")
LEDGER = os.path.join(HERE, "shp_fill_allstocks.json.gz")
HOLDS = os.path.join(HERE, "_shp_allstocks_holds.json")
SHARES_HIST = os.path.join(HERE, "shares_history.json")
FIRST_QE, LAST_QE = "2020-03-31", "2026-06-30"
STRIP = lambda t: t.split("}", 1)[-1]
DOM_LBL = re.compile(r"QIB|qualified institutional|insurance|insurer|provident|pension|\bNPS\b|NBFC|non.?banking|"
                     r"financial institution|\bbanks?\b|alternat\w* investment|\bAIF\b|mutual fund", re.I)
FOR_LBL = re.compile(r"\bFIIs?\b|\bFPIs?\b|foreign portfolio|foreign institution|\bQFI\b|FVCI|private equity|sovereign", re.I)


def qes():
    out = []
    for y in range(2020, 2027):
        for md in ("03-31", "06-30", "09-30", "12-31"):
            q = "%d-%s" % (y, md)
            if FIRST_QE <= q <= LAST_QE: out.append(q)
    return out
QES = qes()


GOV_PARENTS = ("GovernmentsMember", "GovermentsMember", "CentralGovernmentOrStateGovernmentSOrPresidentOfIndiaMember",
               "CentralGovernmentOrStateGovernmentSMember")
THIRD = ("SharesHeldByNonPromoterNonPublicShareholdersMember", "EmployeeBenefitsTrustsMember",
         "SharesHeldByEmployeeTrustsMember", "TradingMembersAndAssociatesOfTradingMembers")


def zero_proof(pct, shc):
    """True when the filing's own SHARE COUNTS prove that institutions hold nothing:
    Public == Non-institutions + Government (parent row only; its sub-rows would double count), share for share,
    and promoter + public + the non-promoter-non-public bucket closes to [98, 102]. Public is Institutions +
    Government + Non-institutions in both the 2015 and the 2022 form, so the equality leaves 0 for institutions."""
    prom = pct.get("ShareholdingOfPromoterAndPromoterGroupMember")
    pub = pct.get("PublicShareholdingMember")
    sp, sn = shc.get("PublicShareholdingMember"), shc.get("NonInstitutionsMember")
    if None in (prom, pub, sp, sn) or sp <= 0: return False
    gov = max([shc.get(k, 0.0) for k in GOV_PARENTS] + [0.0])
    if abs(sp - (sn + gov)) >= 0.5: return False                     # share counts are integers
    tot = pct.get("ShareholdingPatternMember")
    third = max([pct.get(k, 0.0) for k in THIRD] + [0.0])
    a = tot if tot is not None else prom + pub + third
    sc = 100.0 if 0.9 <= a <= 1.1 else 1.0
    return 98.0 <= (prom + pub + third) * sc <= 102.0 or 98.0 <= (prom + pub) * sc <= 102.0


# ------------------------------------------------------------------------------------------ document analysis
def analyse(txt, qe):
    """-> dict(cell, fmt, zero_proof, partition_closes, ambiguity, flags, isin, symtag, shares, nsh)."""
    if isinstance(txt, str): txt = txt.encode("utf-8")
    head = txt[:8000].decode("utf-8", "ignore")
    m = re.search(r'xmlns:in-(?:bse-shp|capmkt)="([^"]+)"', head)
    schema = m.group(1) if m else ""
    root = ET.fromstring(txt)
    ctx = {}
    for c in root.iter():
        if STRIP(c.tag) != "context": continue
        mems, typed = [], False
        for x in c.iter():
            st = STRIP(x.tag)
            if st == "explicitMember": mems.append((x.text or "").split(":")[-1].strip())
            elif st == "typedMember": typed = True
        ctx[c.get("id")] = (mems, typed)
    pct, shc, holders, symtag, isin = {}, {}, {}, None, None
    inst_lbl, non_lbl = [], []
    for f in root.iter():
        t = STRIP(f.tag)
        if t == "Symbol" and f.text and not symtag: symtag = f.text.strip().upper()
        elif t == "ISIN" and f.text and not isin: isin = f.text.strip().upper()
        elif t == "CategoryOfOtherInstitutions" and f.text: inst_lbl.append(f.text.strip())
        elif t in ("CategoryOfOtherNonInstitutions", "CategoryOfOtherIndianShareholders",
                   "CategoryOfOtherForeignShareholders") and f.text: non_lbl.append(f.text.strip())
        elif t == "NumberOfShareholders":
            mems, typed = ctx.get(f.get("contextRef"), ([], True))
            if not typed and len(mems) == 1 and mems[0] in ("ShareholdingPatternMember", "PublicShareholdingMember"):
                try: holders[mems[0]] = int(float(f.text))
                except (TypeError, ValueError): pass
        elif t in ("ShareholdingAsAPercentageOfTotalNumberOfShares", "NumberOfShares"):
            mems, typed = ctx.get(f.get("contextRef"), ([], True))
            if typed or len(mems) != 1: continue
            try: (pct if t.startswith("Share") else shc)[mems[0]] = float(f.text)
            except (TypeError, ValueError): pass
    mem = set(pct)
    fmt = "new" if {"InstitutionsDomesticMember", "InstitutionsForeignMember"} & mem else \
          ("old" if "InstitutionsMember" in mem else "unknown")
    cell = F.parse_shp(root, qe)
    shares = F.parse_shares(root)
    out = {"fmt": fmt, "schema": schema, "isin": isin, "symtag": symtag, "shares": shares, "flags": [],
           "ambiguity": [], "zero_proof": False, "partition_closes": None, "cell": cell}
    prom = pct.get("ShareholdingOfPromoterAndPromoterGroupMember")
    pub = pct.get("PublicShareholdingMember")
    tot = pct.get("ShareholdingPatternMember")
    non = pct.get("NonInstitutionsMember")
    def scale_of():
        a = tot if tot is not None else ((prom or 0) + (pub or 0))
        return 100.0 if 0.9 <= a <= 1.1 else 1.0
    sh_pub = shc.get("PublicShareholdingMember")
    out["_pct"], out["_shc"] = pct, shc                          # kept for the hold-out test; dropped before output
    if cell is None and fmt == "unknown" and zero_proof(pct, shc):
        s = scale_of()
        out["zero_proof"] = True
        out["cell"] = {"prom": round(prom * s, 4), "pub": round(pub * s, 4), "fii": 0.0, "dii": 0.0, "mf": 0.0, "ins": 0.0}
        # shareholder count, same rule as parse_shp: whole-company count, dropped when below the public count
        nsh, nsh_pub = holders.get("ShareholdingPatternMember"), holders.get("PublicShareholdingMember")
        if nsh and nsh > 0 and not (nsh_pub and nsh < nsh_pub): out["cell"]["nsh"] = nsh
    if fmt == "new" and sh_pub is not None:
        blocks = ("InstitutionsDomesticMember", "InstitutionsForeignMember",
                  "CentralGovernmentOrStateGovernmentSOrPresidentOfIndiaMember",
                  "CentralGovernmentOrStateGovernmentSMember", "GovernmentsMember", "GovermentsMember",
                  "NonInstitutionsMember")
        out["partition_closes"] = abs(sh_pub - sum(shc.get(k, 0.0) for k in blocks)) < 0.5   # shares are integers
    if fmt == "old":
        o_other = pct.get("OtherInstitutionsMember") or 0.0
        if o_other or inst_lbl:
            out["ambiguity"].append("R1 institutional Any-Other %.4f %s" % (o_other, inst_lbl[:3]))
        for l in sorted(set(non_lbl)):
            if DOM_LBL.search(l): out["ambiguity"].append("R2 non-institution row labelled domestic institution: " + l)
            elif FOR_LBL.search(l): out["ambiguity"].append("R2-FII non-institution row labelled foreign institution: " + l)
    return out


# ------------------------------------------------------------------------------------------ inputs
def load_inputs(cache):
    hist = json.load(open(os.path.join(HERE, "shp_history.json"), encoding="utf-8"))
    rename = json.load(open(os.path.join(HERE, "_rename_map.json"), encoding="utf-8"))
    js = open(os.path.join(REPO, "docs", "backtest-engine.js"), encoding="utf-8").read()
    m = re.search(r"const FUND_ALIAS\s*=\s*(\{.*?\});", js, re.S)
    fund_alias = json.loads(m.group(1)) if m else {}
    rel = collections.defaultdict(set)                           # every rename relative of a symbol
    for mp in (rename, fund_alias):
        for old, new in mp.items():
            if isinstance(new, str) and new != old:
                rel[old].add(new); rel[new].add(old)
    def relatives(sym):
        seen, todo = {sym}, [sym]
        while todo:
            for n in rel.get(todo.pop(), ()):
                if n not in seen: seen.add(n); todo.append(n)
        return seen - {sym}
    isins = collections.defaultdict(set)
    p = os.path.expanduser("~/stocks-cache/univ/nse_sym_isin_2020.json")
    if os.path.exists(p):
        for s, lst in json.load(open(p)).items():
            isins[s.upper()] |= {i.upper() for i in lst}
    p = os.path.expanduser("~/stocks-cache/univ/sf_recent_now.bin")
    if os.path.exists(p):
        tape = json.loads(gzip.open(p).read())
        for s, mt in (tape.get("meta") or {}).items():
            if mt.get("isin"): isins[s.upper()].add(str(mt["isin"]).upper())
    man = {}
    for mp in ("manifest_nse.json", "manifest_nse_all.json"):
        f = os.path.join(cache, mp)
        if os.path.exists(f):
            for k, v in json.load(open(f)).items():
                if "error" in v: continue
                if k in man and mp == "manifest_nse_all.json" and v.get("reused"): continue
                path = v.get("path") or os.path.join(cache, "xbrl_nse", k.replace("|", "_") + ".xml")
                if os.path.exists(path) and os.path.getsize(path) > 0:
                    man[k] = dict(v, path=path)
    return hist, relatives, isins, man


def read_doc(path):
    b = open(path, "rb").read()
    return gzip.decompress(b) if path.endswith(".gz") else b


# ------------------------------------------------------------------------------------------ master + download
def stage_master(cache):
    import build_fundamentals as B
    jar = B.nse_jar(); os.makedirs(os.path.join(cache, "master"), exist_ok=True)
    for idx in ("equities", "sme"):
        for qe in QES:
            fn = os.path.join(cache, "master", "%s_%s.json" % (idx, qe))
            if os.path.exists(fn) and qe < QES[-1]: continue              # the newest season keeps filling
            recs = None
            for attempt in range(3):
                try: recs = F.fetch_master(jar, qe, index=idx); break
                except Exception as e: print("  retry %s %s: %s" % (idx, qe, e)); time.sleep(3)
            if recs is None: print("FAILED master %s %s" % (idx, qe)); continue
            json.dump(recs, open(fn, "w"))
            print("master %s %s: %d rows, %d as-on this quarter" % (idx, qe, len(recs),
                  sum(1 for r in recs if F.iso_date(r.get("date")) == qe)))
            time.sleep(0.4)


def stage_download(cache, workers=4):
    import threading, traceback
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import build_fundamentals as B
    M, X, MP = os.path.join(cache, "master"), os.path.join(cache, "xbrl_nse_all"), os.path.join(cache, "manifest_nse_all.json")
    os.makedirs(X, exist_ok=True)
    have = {}                                                   # reuse every file an earlier pass already holds
    d1 = os.path.join(cache, "xbrl_nse")
    if os.path.isdir(d1):
        for f in os.listdir(d1):
            if os.path.getsize(os.path.join(d1, f)) > 0:
                s_, q_ = f[:-4].rsplit("_", 1); have["%s|%s" % (s_, q_)] = os.path.join(d1, f)
    idx2 = os.path.join(cache, "xbrl_nse2_index.json")          # FII session's NSE cache, indexed by Symbol/as-on
    if os.path.exists(idx2):
        base = os.path.expanduser("~/stocks-cache/shp/fii_session/shp_src/xbrl_nse2")
        for f, (s_, isin_, q_) in json.load(open(idx2)).items():
            if s_ and q_: have.setdefault("%s|%s" % (s_, q_), os.path.join(base, f))
    want = {}
    for f in sorted(os.listdir(M)):
        idx, qe = f[:-5].split("_", 1)
        if qe not in QES: continue
        for r in json.load(open(os.path.join(M, f))):
            sym = str(r.get("symbol") or "").upper().strip()
            if F.iso_date(r.get("date")) != qe or not sym or not str(r.get("xbrl") or "").startswith("http"): continue
            k, sub = "%s|%s" % (sym, qe), F.visible_iso(r) or ""
            if k not in want or sub >= want[k]["sub"]:
                want[k] = {"url": r["xbrl"], "sub": sub, "submissionDate": r.get("submissionDate"),
                           "broadcastDate": r.get("broadcastDate"), "revised": str(r.get("revisedData") or ""), "idx": idx}
    man = json.load(open(MP)) if os.path.exists(MP) else {}
    for k, v in want.items():
        if k in have and k not in man: man[k] = dict(v, path=have[k], reused=True)
    todo = [(k, v) for k, v in want.items() if k not in man or "error" in man[k]]
    print("download: %d listed, %d reused from disk, %d to fetch" % (len(want), sum(1 for k in want if k in have), len(todo)))
    jar = B.nse_jar(); lk = threading.Lock(); fail = []
    def one(k, v):
        last = None
        for attempt in range(3):
            try:
                b = F.fetch_xbrl(v["url"], jar); b = b.encode("utf-8") if isinstance(b, str) else b
                if len(b) < 2000 or b"Shareholding" not in b: raise ValueError("short/odd body %d" % len(b))
                path = os.path.join(X, k.replace("|", "_") + ".xml.gz"); open(path, "wb").write(gzip.compress(b))
                return dict(v, path=path, bytes=len(b))
            except Exception as e:
                last = "%s: %s" % (type(e).__name__, str(e)[:100])
                if "404" in last: break
                time.sleep(1 + attempt)
        return dict(v, error=last)
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(one, k, v): k for k, v in todo}
        for n, fut in enumerate(as_completed(futs), 1):
            k = futs[fut]
            try: v = fut.result()
            except Exception: v = {"error": traceback.format_exc()[-150:]}
            with lk:
                man[k] = v
                if "error" in v: fail.append(k)
                if n % 1000 == 0: json.dump(man, open(MP, "w")); print("  %d/%d (%d failed)" % (n, len(todo), len(fail)))
    json.dump(man, open(MP, "w"))
    print("download done: %d fetched, %d failed (404 = NSE lists the filing but serves no file)" % (len(todo) - len(fail), len(fail)))


# ------------------------------------------------------------------------------------------ build
def build(cache):
    hist, relatives, isins, man = load_inputs(cache)
    # Re-runs must reproduce the ledger: cells this ledger itself landed are treated as NOT stored (otherwise a
    # rebuild after landing sees every cell as "already stored" and writes an empty ledger that CI then applies).
    if os.path.exists(LEDGER):
        prev = json.load(gzip.open(LEDGER, "rt", encoding="utf-8")).get("fills", {})
        for s_, qs_ in prev.items():
            for q_ in qs_:
                if q_ in (hist.get(s_) or {}): del hist[s_][q_]
        print("previous ledger: %d cells treated as not yet stored" % sum(len(v) for v in prev.values()))
    t0 = time.time()
    docs = {}                                                    # (sym, qe) -> analysis + meta
    for k, v in man.items():
        sym, qe = k.split("|")
        if not (FIRST_QE <= qe <= LAST_QE): continue
        try:
            a = analyse(read_doc(v["path"]), qe)
        except Exception as e:
            a = {"error": "%s: %s" % (type(e).__name__, str(e)[:80])}
        a.pop("_pct", None); a.pop("_shc", None)
        a.update({"sub": v.get("sub"), "submissionDate": v.get("submissionDate"), "idx": v.get("idx"),
                  "revised": str(v.get("revised") or "").lower() == "revised",
                  "src": "nse:" + str(v.get("url", "")).rsplit("/", 1)[-1]})
        docs[(sym, qe)] = a
    print("analysed %d filings in %.0fs" % (len(docs), time.time() - t0))

    def issuers(isl): return {i[:7] for i in isl}
    def norm_isin(i):
        """Filer typos seen in the corpus: letter O for zero, letter I for one (INEOMTP01013 for INE0MTP01013).
        Used ONLY for an exact 12-character match against an ISIN the symbol really traded under."""
        return i[:3] + i[3:11].replace("O", "0").replace("I", "1") + i[11:] if i else i

    def identity(sym, a):
        """-> (reason or None, extra former tickers). The NSE master pairs `sym` with this document; the document's
        own ISIN must agree with an ISIN `sym` (or a rename relative) traded under — issuer prefix, or the full ISIN
        after the O/I typo repair. A different Symbol tag inside the file is then a former ticker of the same
        company (renames missing from the maps: LAWSIKHO->ADDICTIVE, SILLYMONKS->CRESTO, same ISIN)."""
        fam = {sym} | relatives(sym)
        known = set().union(*[isins.get(x, set()) for x in fam])
        fi, tag = a.get("isin"), a.get("symtag")
        tag_ok = tag and tag != "NOTLISTED" and tag not in fam
        if fi:
            if known:
                if fi[:7] not in issuers(known) and norm_isin(fi) not in known:
                    return "file ISIN %s does not match %s's ISINs %s" % (fi, sym, sorted(known)[:3]), []
            elif not (tag_ok and (fi[:7] in issuers(isins.get(tag, set())) or norm_isin(fi) in isins.get(tag, set()))):
                return "no ISIN on record for %s and the file's own symbol does not vouch for %s" % (sym, fi), []
            extra = [tag] if tag_ok and (fi[:7] in issuers(isins.get(tag, set())) or norm_isin(fi) in isins.get(tag, set())) else []
            return None, extra
        if tag and tag not in fam and tag != "NOTLISTED":
            return "file carries no ISIN and its Symbol %s is not %s" % (tag, sym), []
        if not known and not tag:
            return "no ISIN on either side to prove identity", []
        return None, []

    def visibility(a):
        """-> (date, note). Default = calendar day of NSE's broadcast (visible_iso, midnight rule §149). NSE keeps ONE
        record per (symbol, as-on) and re-stamps its broadcastDate on re-publication and in bulk system passes
        (§142h: 2022-01-06/07 ...), so a broadcast far after the submission day can be a re-stamp of the ORIGINAL
        document. The file name carries the document's creation stamp (SHP_<id>_<ddmmyyyyHHMMSS>_WEB.xml): when the
        document was created no later than the day after the submission day, it IS the original filing and is dated
        by the submission day (the §142i day-precision fallback). A document created later is a re-filing: it keeps
        its own later broadcast day, so a re-filing's numbers are never served from the original's date (§142j/k).
        The creation stamp is used only to classify original-vs-refiling, never as a date (memory: not a filing time)."""
        bday = a.get("sub")
        subd = F.iso_date(a.get("submissionDate"))
        m = re.search(r"_(\d{2})(\d{2})(\d{4})\d{6}_WEB\.xml$", a.get("src") or "")
        if not (bday and subd and m): return bday, ""
        try:
            made = datetime.date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
            sd, bd = datetime.date.fromisoformat(subd), datetime.date.fromisoformat(bday)
        except ValueError:
            return bday, ""
        if made <= sd + datetime.timedelta(days=1) and bd > sd + datetime.timedelta(days=2):
            return subd, "dated-by-submission(broadcast %s is a re-stamp of the original)" % bday
        return bday, ""

    # ---- series view for neighbour gates: stored cells + parsed candidates
    series = collections.defaultdict(dict)
    for sym, qs in hist.items():
        if sym.startswith("_") or not isinstance(qs, dict): continue
        for qe, c in qs.items():
            series[sym][qe] = (c[1], c[2], c[6] if len(c) > 6 else None, "store")
    for (sym, qe), a in docs.items():
        c = a.get("cell")
        if c and qe not in series[sym]:
            series[sym][qe] = (c["fii"], c["dii"], c.get("nsh"), "cand")

    # A NEW store key that equals an unrelated BSE-only scrip id changes what build_stock_fin folds into that slug
    # (its "taken" set counts shareholding keys): hold the whole symbol rather than silently move a page's data.
    slug = lambda x: re.sub(r"[^A-Za-z0-9._-]", "_", x)
    claimed = {slug(k) for k in hist if not k.startswith("_")}
    for fn in ("sf_fundamentals.json", "sf_revop.json"):
        try: claimed |= {slug(k) for k in json.load(open(os.path.join(REPO, "docs", fn), encoding="utf-8"))}
        except Exception as e: print("WARN %s unreadable (%s)" % (fn, e))
    bs = json.load(open(os.path.join(HERE, "bse_scrips.json"), encoding="utf-8"))
    code_isin = {str(v): k for k, v in bs.get("by_isin", {}).items()}
    bse_clash = {}
    for sym_, code_ in bs.get("by_id", {}).items():
        if sym_ in hist or slug(sym_) in claimed: continue
        bi = code_isin.get(str(code_), "")
        if bi and isins.get(sym_) and bi[:7] not in issuers(isins[sym_]):
            bse_clash[sym_] = "ticker %s is also BSE scrip %s (%s), a different company; landing would change the " \
                              "fundamentals build_stock_fin folds into that slug" % (sym_, code_, bi)

    accept_syms = set((json.load(open(os.path.join(HERE, "shp_cell_fix.json"), encoding="utf-8")).get("accept") or {}))
    share_series = collections.defaultdict(dict)               # total shares per filing, for the capital-collapse test
    for (s_, q_), a_ in docs.items():
        if a_.get("shares"): share_series[s_][q_] = [a_["shares"]]
    fills, holds = collections.defaultdict(dict), collections.defaultdict(dict)
    stat = collections.Counter(); why = collections.Counter()
    for (sym, qe), a in sorted(docs.items()):
        def hold(reason):
            holds[sym][qe] = {"why": reason, "src": a.get("src"), "cell": a.get("cell"), "fmt": a.get("fmt")}
            stat[(a.get("idx"), "hold")] += 1; why[reason.split(":")[0][:60]] += 1
        stored = hist.get(sym) or {}
        if qe in stored: stat[(a.get("idx"), "already stored")] += 1; continue
        if "error" in a: hold("unreadable XML: " + a["error"]); continue
        if sym in bse_clash: hold("slug collision: " + bse_clash[sym]); continue
        bad, extra = identity(sym, a)
        if bad: hold("identity: " + bad); continue
        former = [x for x in (relatives(sym) | set(extra)) if qe in (hist.get(x) or {})]
        if former: stat[(a.get("idx"), "stored under a former ticker")] += 1; continue
        c = a.get("cell")
        if c is None: hold("parser refused (format %s) and no partition proof" % a["fmt"]); continue
        if a["ambiguity"]: hold("old-format row needing row-level placement: " + "; ".join(a["ambiguity"])[:160]); continue
        if a["fmt"] == "new" and a["partition_closes"] is False: hold("public block does not close"); continue
        if c["mf"] > c["dii"] + 0.05 or c["ins"] > c["dii"] + 0.05: hold("mf/ins exceeds dii"); continue
        if c["prom"] + c["fii"] + c["dii"] > 100.5: hold("promoter + fii + dii exceeds 100"); continue
        sub, how = visibility(a)
        if not sub: hold("no visibility date"); continue
        lag = (datetime.date.fromisoformat(sub) - datetime.date.fromisoformat(qe)).days
        if lag < 0: hold("visibility date %s before quarter end" % sub); continue
        nsh = c.get("nsh")
        earlier = [v[2] for q, v in series[sym].items() if q < qe and v[2]]
        nsh_note = ""
        if nsh and earlier and nsh < 0.05 * max(earlier):
            # §22g-2: a collapsed holder count can mean the filing describes another share class. When the SAME
            # filings show the share capital itself collapsing (insolvency capital reduction: PUNJLLOYD 335.6 M -> 0.5 M
            # shares, acquirer 95 %) or the symbol's event is already adjudicated on the cell_fix accept list
            # (DSKULKARNI), the percentages stand on share counts; only the holder count is doubtful -> write the cell
            # WITHOUT it. Anything else is held.
            n_now = a.get("shares") or 0
            n_before = [v[0] for q, v in (share_series.get(sym) or {}).items() if q < qe and v]
            collapsed = bool(n_now and n_before and n_now < 0.5 * max(n_before))
            if collapsed or sym in accept_syms:
                nsh_note = " nsh-withheld(%d vs %d earlier; %s)" % (nsh, max(earlier), "capital collapse" if collapsed else "accept-list event")
                nsh = None
            else:
                hold("nsh gate: %d vs %d earlier" % (nsh, max(earlier))); continue
        i = QES.index(qe)
        nb = [series[sym][QES[j]] for j in (i - 2, i - 1, i + 1, i + 2)
              if 0 <= j < len(QES) and QES[j] in series[sym]]
        if nb and all(abs(c["fii"] - n[0]) > 5 for n in nb): hold("continuity: fii >5pp from every neighbour"); continue
        if nb and all(abs(c["dii"] - n[1]) > 10 for n in nb): hold("continuity: dii >10pp from every neighbour"); continue
        proven = a["zero_proof"] or a["partition_closes"]
        if not proven and any(c[s] == 0.0 and nb and any(n[ix] > 1.0 for n in nb) for s, ix in (("fii", 0), ("dii", 1))):
            hold("exact 0 beside a neighbour above 1% without a partition proof"); continue
        tag = a["src"] + (" proven-zero" if a["zero_proof"] else "") + (" nse-refiling" if a["revised"] else "") \
              + (" " + how if how else "") + (" lag%dd" % lag if lag > 120 else "") + nsh_note
        fills[sym][qe] = [round(c["prom"], 4), round(c["fii"], 4), round(c["dii"], 4), round(c["mf"], 4),
                          round(c["ins"], 4), sub, nsh if nsh else None, tag]
        stat[(a.get("idx"), "FILL")] += 1

    # ---- share counts: one per (sym, qe), identity-gated, stub/wrong-class screen against neighbours
    sh = collections.defaultdict(dict)
    for (sym, qe), a in docs.items():
        n = a.get("shares")
        if "error" in a or not n or n <= 0 or identity(sym, a)[0]: continue
        sh[sym][qe] = [int(n), visibility(a)[0], a["src"]]
    sh_hold = 0
    for sym, qs in sh.items():
        ks = sorted(qs)
        bad = []                                                 # decide on the untouched series, drop afterwards
        for idx, qe in enumerate(ks):
            nb = [qs[ks[j]][0] for j in (idx - 2, idx - 1, idx + 1, idx + 2) if 0 <= j < len(ks)]
            n = qs[qe][0]
            if nb and all(n < x / 5 or n > x * 5 for x in nb):
                holds[sym].setdefault(qe, {})["shares"] = "share count %d is >5x off every neighbour %s" % (n, nb)
                bad.append(qe)
        for qe in bad:
            qs[qe] = None; sh_hold += 1
    sh = {s: {q: v for q, v in qs.items() if v} for s, qs in sh.items()}
    sh = {s: qs for s, qs in sh.items() if qs}

    built = (datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:%M IST")
    n_cells = sum(len(v) for v in fills.values())
    led = {"_meta": {"source": "NSE corporate-share-holdings-master XBRL (equities + SME boards)",
                     "built": built, "runbook": "§180", "symbols": len(fills), "cells": n_cells,
                     "rule": "fill-only; parse_shp unchanged + partition-proven zeros; holds in _shp_allstocks_holds.json"},
           "fills": {s: dict(sorted(q.items())) for s, q in sorted(fills.items())}}
    with gzip.open(LEDGER, "wt", encoding="utf-8") as fh:
        json.dump(led, fh, separators=(",", ":"), sort_keys=False)
    json.dump({"_meta": {"built": built, "cells": sum(len(v) for v in holds.values())},
               "holds": {s: dict(sorted(q.items())) for s, q in sorted(holds.items()) if q}},
              open(HOLDS, "w", encoding="utf-8"), indent=0, default=str)
    json.dump({"_meta": {"built": built, "source": "NSE SHP XBRL whole-company NumberOfShares (parse_shares)",
                         "shape": "{SYM: {QE: [shares, visible_date, src]}}", "symbols": len(sh),
                         "cells": sum(len(v) for v in sh.values()), "held": sh_hold, "runbook": "§180"},
               **{s: dict(sorted(q.items())) for s, q in sorted(sh.items())}},
              open(SHARES_HIST, "w", encoding="utf-8"), separators=(",", ":"))
    print("LEDGER %s: %d cells / %d symbols" % (os.path.basename(LEDGER), n_cells, len(fills)))
    for k in sorted(stat, key=str): print("  ", k, stat[k])
    print("hold reasons:", dict(why.most_common()))
    print("SHARES %s: %d cells / %d symbols, %d held" % (os.path.basename(SHARES_HIST),
          sum(len(v) for v in sh.values()), len(sh), sh_hold))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("stage", choices=["master", "download", "build"])
    ap.add_argument("--cache", default=CACHE)
    a = ap.parse_args()
    {"master": stage_master, "download": stage_download, "build": build}[a.stage](a.cache)
