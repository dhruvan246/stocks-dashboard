# -*- coding: utf-8 -*-
"""Quarterly-results XBRL from BSE → fill-only numbers for BSE-only stocks AND for NSE stocks whose quarter NSE never
served (runbook §178). Replaces Claude-vision reads wherever the company filed XBRL on BSE (Jun-2018 onward).

ROUTE (measured 2026-09-26, memory reference-bse-results-xbrl-route):
  list   api.bseindia.com/BseIndiaAPI/api/Result_Arch_ng/w?scrip_cd=<code>  → {"Table":[{stand_xbrl_link | conso_xbrl_link,
         Filing_Date_Time, Quarter, qtr, Status, …}]}
  file   www.bseindia.com + link  (/XBRLFILES/FourOneUploadDocument/… to Dec-2024, /XBRLFILES/IFIndasUploadDocument/… 2025+)
One plain, honestly identified client, one request at a time (runbook §164b BSE access rule). When the api answers
403 the run prints BSE-REFUSED and writes nothing — it never retries harder, never impersonates.

IDENTITY comes from the FILE, not the listing: the XML's ScripCode must equal the requested scrip; its OneD context gives
the period (3-month = quarter; ~6-month = half-year, kept only for SME half-yearly filers, stored with h=1); NatureOf-
Report gives the basis. Values: build_fundamentals.xbrl_profit (PAT) + build_revop.xbrl_revop (revenue/op/ebit) +
build_xbrl_extra.parse_file (detail) — the same parsers every NSE filing goes through.

TARGETS
  • scripts/bse_xbrl_nse_targets.json {NSE_SYM: {"code": scrip, "q": [qe…]}} — NSE main-board point-in-time quarters NSE
    never listed (built by the §175 coverage tool; ISIN-joined to the BSE scrip).
  • every BSE-only scrip in docs/bse_universe.json: quarters 2020-03-31 → latest due missing from bse_fundamentals.
State: scripts/_bse_xbrl_state.json {code: last YYYYMMDD listed} — a scrip is re-listed at most every 20 days.

Two stages so an unattended CI push can never clobber a concurrent writer (the minified-JSON rule):
  --fetch [--budget N] [--out FILLS]   list + download + parse → FILLS json (touches no store)
  --apply FILLS                        fill-only merge into docs/bse_fundamentals.json, docs/sf_fundamentals.json,
                                       docs/sf_revop.json, scripts/revop_fundamentals.json, scripts/xbrl_extra.json.gz
  --from-dir DIR                       offline test: treat DIR's *.xml as downloaded files (no listing → ann 0)
"""
import os, re, sys, json, gzip, time, datetime, urllib.request, urllib.error

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
DOCS = os.path.join(ROOT, "docs")
sys.path.insert(0, HERE)

UA = "stocks-dashboard data refresh (github.com/dhruvan246/stocks-dashboard)"
LIST = "https://api.bseindia.com/BseIndiaAPI/api/Result_Arch_ng/w?scrip_cd=%s"
WWW = "https://www.bseindia.com"
STATE = os.path.join(HERE, "_bse_xbrl_state.json")
NSE_T = os.path.join(HERE, "bse_xbrl_nse_targets.json")
FLOOR = 20200331
RELIST_DAYS = 20
RE_CTX = re.compile(r'<xbrli:context id="OneD">.*?<xbrli:startDate>([\d-]+)</xbrli:startDate>\s*<xbrli:endDate>([\d-]+)<', re.S)
RE_SCRIP = re.compile(r"<in-(?:capmkt|bse-fin):ScripCode[^>]*>\s*([^<\s]+)\s*<")
RE_NAT = re.compile(r"NatureOfReportStandaloneConsolidated[^>]*>\s*([^<]+)<")
RE_ISIN = re.compile(r"<in-(?:capmkt|bse-fin):ISIN[^>]*>\s*([A-Z0-9]{12})\s*<")


class Refused(RuntimeError):
    pass


def get(url, want_json=False):
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Referer": "https://www.bseindia.com/",
                                               "Accept": "application/json" if want_json else "*/*"})
    try:
        b = urllib.request.urlopen(req, timeout=60).read()
    except urllib.error.HTTPError as e:
        if e.code in (401, 403, 429):
            raise Refused("HTTP %d %s" % (e.code, url))
        raise
    if b[:200].lstrip().lower().startswith((b"<!doctype", b"<html")) or b"Access Denied" in b[:2000]:
        raise Refused("HTML/denied body for %s" % url)
    return b


def ymd(d):
    return int(d.strftime("%Y%m%d"))


def due_quarters(today):
    """Quarter ends from FLOOR whose results are due (qe + 45 d, Mar + 60 d)."""
    out, y = [], FLOOR // 10000
    while True:
        for md in (331, 630, 930, 1231):
            q = y * 10000 + md
            if q < FLOOR: continue
            lag = 60 if md == 331 else 45
            if datetime.date(y, md // 100, md % 100) + datetime.timedelta(days=lag) > today:
                return out
            out.append(q)
        y += 1


def ann_of(s):
    """Filing_Date_Time → YYYYMMDD calendar day (midnight rule, §149). Unparseable → 0 (unknown, never guessed)."""
    s = (s or "").strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S",
                "%d-%m-%Y %H:%M:%S", "%d %b %Y %H:%M:%S", "%d-%b-%Y %H:%M:%S", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return ymd(datetime.datetime.strptime(s[:len(datetime.datetime.now().strftime(fmt))] if "%f" not in fmt else s, fmt))
        except ValueError:
            continue
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", s)
    return int("".join(m.groups())) if m else 0


def read_file(xml, code):
    """(qe, days, basis 'S'|'C', isin) from the file itself, or None when it is not this scrip's periodic result."""
    sc = RE_SCRIP.search(xml)
    if not sc or sc.group(1).strip() != str(code):
        return None
    m = RE_CTX.search(xml)
    if not m:
        return None
    s, e = (datetime.date.fromisoformat(x) for x in m.groups())
    nat = (RE_NAT.search(xml) or [None, ""])
    nat = nat.group(1).strip().lower() if hasattr(nat, "group") else ""
    basis = "C" if nat.startswith("consol") else "S"
    isin = RE_ISIN.search(xml)
    return ymd(e), (e - s).days + 1, basis, (isin.group(1) if isin else "")


def parse_values(xml, basis):
    import build_fundamentals as B
    from build_revop import xbrl_revop
    hint = "Consolidated" if basis == "C" else "Standalone"
    try:
        std, con = B.xbrl_profit(xml, basis_hint=hint)
    except Exception:
        std = con = None
    try:
        rs, os_, es, rc, oc, ec, fin = xbrl_revop(xml, basis_hint=hint)
    except Exception:
        rs = os_ = es = rc = oc = ec = None; fin = 0
    return {"pat_s": std, "pat_c": con, "rev_s": rs, "rev_c": rc, "op_s": os_, "op_c": oc, "ebit_s": es,
            "ebit_c": ec, "fin": fin}


def detail(path, fname, sym):
    import build_xbrl_extra as X
    try:
        r = X.parse_file(path, fname, sym_override=sym)
    except Exception:
        return None
    if not r or not (r.get("s") or r.get("c")):
        return None
    return {"s": r.get("s") or {}, "c": r.get("c") or {}}


def targets(today):
    """[(code, target_sym or None, kind, sme, [missing qe])] — NSE targets first, then BSE-only by mcap."""
    import fetch_bse_fund as bf
    out = []
    if os.path.exists(NSE_T):
        for sym, t in sorted(json.load(open(NSE_T)).items()):
            out.append((str(t["code"]), sym, "nse", False, sorted(int(q) for q in t["q"])))
    univ = json.load(open(os.path.join(DOCS, "bse_universe.json")))["rows"]
    univ.sort(key=lambda r: r[6] or 0, reverse=True)
    px = json.load(open(bf.OUT, encoding="utf-8")).get("px", {}) if os.path.exists(bf.OUT) else {}
    due = due_quarters(today)
    for r in univ:
        code = str(r[0]); sme = (r[4] or "") in ("M", "MT", "MS")
        have = {int(q) for q in (px.get(code) or {}) if str(q).isdigit()}
        miss = [q for q in due if q not in have and not (sme and q % 10000 in (630, 1231))]
        if miss:
            out.append((code, None, "bse", sme, miss))
    return out


def fetch(budget, fills_path, from_dir=None):
    import xbrl_symbol
    today = datetime.date.today()
    state = json.load(open(STATE)) if os.path.exists(STATE) else {}
    bs = json.load(open(os.path.join(HERE, "bse_scrips.json")))
    code2tk = {str(v): k for k, v in bs["by_id"].items()}
    fills = []
    tmpd = os.path.join(os.environ.get("RUNNER_TEMP") or "/tmp", "bse_xbrl_dl"); os.makedirs(tmpd, exist_ok=True)
    tlist = targets(today)
    print("targets: %d scrips (%d NSE-quarter targets, %d BSE-only)" %
          (len(tlist), sum(1 for t in tlist if t[2] == "nse"), sum(1 for t in tlist if t[2] == "bse")))
    listed = files_ok = 0
    if from_dir:                                             # offline test: every xml in the dir, code from the file
        jobs = []
        for f in sorted(os.listdir(from_dir)):
            if f.endswith(".xml"):
                x = open(os.path.join(from_dir, f), errors="replace").read()
                sc = RE_SCRIP.search(x)
                if sc: jobs.append((sc.group(1).strip(), [(os.path.join(from_dir, f), f, "")]))
        tmap = {c: (s, k, sme, q) for c, s, k, sme, q in tlist}
        for code, files in jobs:
            s, k, sme, q = tmap.get(code, (None, "bse", False, None))
            fills += handle(code, s, sme, q, files, code2tk, xbrl_symbol)
        json.dump(fills, open(fills_path, "w")); print("fills (offline):", len(fills)); return
    for code, sym, kind, sme, miss in tlist:
        if listed >= budget:
            break
        last = int(state.get(code) or 0)
        if last and (today - datetime.date(last // 10000, last // 100 % 100, last % 100)).days < RELIST_DAYS:
            continue
        try:
            body = get(LIST % code, want_json=True)
        except Refused as e:
            print("BSE-REFUSED: %s — nothing fetched this run (%d scrips listed before the refusal)" % (e, listed))
            break
        except Exception as e:
            print("  %s list error %s" % (code, str(e)[:80])); continue
        listed += 1; state[code] = ymd(today)
        try:
            rows = (json.loads(body).get("Table") or [])
        except ValueError:
            print("  %s listing not JSON" % code); continue
        want = set(miss)
        files = []
        for row in rows:
            for key in ("stand_xbrl_link", "conso_xbrl_link"):
                link = (row.get(key) or "").strip()
                if not link.lower().endswith(".xml"):
                    continue                                 # empty, or the bare /XBRLFILES/…/ directory (= no file)
                files.append((link, row.get("Filing_Date_Time") or "", row.get("Quarter") or ""))
        seen, dl = set(), []
        for link, fdt, qlabel in files:
            if link in seen: continue
            seen.add(link)
            fname = link.rsplit("/", 1)[-1]; p = os.path.join(tmpd, fname)
            if not os.path.exists(p):
                try:
                    open(p, "wb").write(get(WWW + link))
                except Refused as e:
                    print("  BSE-REFUSED file %s: %s" % (fname, e)); break
                except Exception:
                    continue
                time.sleep(0.5)
            dl.append((p, fname, fdt))
        got = handle(code, sym, sme, miss, dl, code2tk, xbrl_symbol)
        fills += got; files_ok += len(dl)
        time.sleep(0.8)
    json.dump(state, open(STATE, "w"), separators=(",", ":"), sort_keys=True)
    json.dump(fills, open(fills_path, "w"))
    print("listed %d scrips, %d files read, %d fills → %s" % (listed, files_ok, len(fills), fills_path))


def handle(code, sym, sme, miss, dl, code2tk, xbrl_symbol):
    """Parse downloaded files for one scrip → fill records for missing quarters only."""
    out = []
    want = set(miss) if miss else None
    for p, fname, fdt in dl:
        xml = open(p, encoding="utf-8", errors="replace").read()
        info = read_file(xml, code)
        if not info:
            continue
        qe, days, basis, isin = info
        half = 170 <= days <= 190
        if not (80 <= days <= 100 or (half and sme)):
            continue                                         # FY / nine-month / YTD, or a half-year of a quarterly filer
        if want is not None and qe not in want:
            continue
        tgt = sym or xbrl_symbol.resolve("NOTLISTED", xml)   # an NSE listing of the same ISIN owns the page
        kind = "nse" if tgt else "bse"
        vals = parse_values(xml, basis)
        rec = {"code": code, "qe": qe, "basis": basis, "half": half, "ann": ann_of(fdt), "file": fname,
               "kind": kind, "sym": tgt or code2tk.get(code), **vals}
        if not half and rec["sym"]:
            rec["detail"] = detail(p, fname, rec["sym"])
        out.append(rec)
    return out


def apply(fills_path):
    from build_revop import strip_lender_ebit
    fills = json.load(open(fills_path))
    P = {"bf": os.path.join(DOCS, "bse_fundamentals.json"), "sf": os.path.join(DOCS, "sf_fundamentals.json"),
         "rv": os.path.join(DOCS, "sf_revop.json"), "rl": os.path.join(HERE, "revop_fundamentals.json"),
         "x": os.path.join(HERE, "xbrl_extra.json.gz")}
    bfd = json.load(open(P["bf"], encoding="utf-8")); px = bfd.setdefault("px", {})
    sf = json.load(open(P["sf"])); rv = json.load(open(P["rv"])); rl = json.load(open(P["rl"]))
    xl = json.loads(gzip.decompress(open(P["x"], "rb").read()))
    tape_keys = set()
    try:
        b = gzip.decompress(open(os.path.join(DOCS, "sf_stock_data.bin"), "rb").read())
        tape_keys = set(json.JSONDecoder().raw_decode(b[b.rfind(b'"meta":') + 7:].decode())[0])
    except (OSError, ValueError):
        pass
    C = {"bse q": 0, "nse pat": 0, "nse rev": 0, "detail q": 0, "detail f": 0, "skip ticker clash": 0}
    for f in fills:
        qe, basis = f["qe"], f["basis"]
        pat = f["pat_c"] if basis == "C" else f["pat_s"]
        rev = f["rev_c"] if basis == "C" else f["rev_s"]
        if f["kind"] == "bse":
            cur = px.setdefault(str(f["code"]), {})
            old = cur.get(str(qe))
            if (pat is not None or rev is not None) and (old is None or (old.get("basis") == "S" and basis == "C"
                                                                         and old.get("src") == "bse-xbrl")):
                if old is None or old.get("src") == "bse-xbrl":   # consolidated outranks standalone within this route only
                    rec = {"pat": pat, "ann": f["ann"] or 0, "basis": basis, "src": "bse-xbrl"}
                    if rev is not None: rec["rev"] = rev
                    if f["half"]: rec["h"] = 1
                    cur[str(qe)] = rec; C["bse q"] += 1
        else:
            sym = f["sym"]; ann = f["ann"] or None
            rows = sf.setdefault(sym, [])
            row = next((r for r in rows if r[0] == qe), None)
            s_, c_ = f["pat_s"], f["pat_c"]
            if row is None and (s_ is not None or c_ is not None):
                rows.append([qe, s_, ann if s_ is not None else None, c_, ann if c_ is not None else None])
                rows.sort(key=lambda r: r[0]); C["nse pat"] += 1
            elif row is not None:
                if s_ is not None and row[1] is None: row[1], row[2] = s_, ann; C["nse pat"] += 1
                if c_ is not None and row[3] is None: row[3], row[4] = c_, ann; C["nse pat"] += 1
            for store in (rv, rl):
                d = store.setdefault(sym, {})
                rr = list(d.get(str(qe)) or [None, None, None, None, None, None, 0, None, None]); rr += [None] * (9 - len(rr))
                before = list(rr)
                for i, v in ((0, f["rev_s"]), (1, f["rev_c"]), (2, f["op_s"]), (3, f["op_c"]), (4, s_), (5, c_),
                             (7, f["ebit_s"]), (8, f["ebit_c"])):
                    if rr[i] is None and v is not None: rr[i] = v
                if f["fin"]: rr[6] = 1
                strip_lender_ebit(sym, rr)
                if rr != before:
                    d[str(qe)] = rr
                    if store is rv: C["nse rev"] += 1
        dt = f.get("detail")
        if dt and f["sym"]:
            if f["kind"] == "bse" and f["sym"].upper() in tape_keys:
                C["skip ticker clash"] += 1; continue          # a BSE ticker that is also an NSE key (§76) — never mix
            cell = xl.setdefault(f["sym"], {}).setdefault(str(qe), {})
            new_q = not cell
            for b in ("s", "c"):
                for k, v in (dt.get(b) or {}).items():
                    if v is not None and k not in cell.get(b, {}):
                        cell.setdefault(b, {})[k] = v; C["detail f"] += 1   # FILL-ONLY: an NSE XBRL value always wins
            if not cell:
                del xl[f["sym"]][str(qe)]
            elif new_q:
                C["detail q"] += 1
    bfd["updated"] = (datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:%M IST")
    json.dump(bfd, open(P["bf"], "w", encoding="utf-8"), ensure_ascii=False, separators=(",", ":"))
    json.dump(sf, open(P["sf"], "w"), separators=(",", ":"))
    json.dump(rv, open(P["rv"], "w"), separators=(",", ":"))
    json.dump(rl, open(P["rl"], "w"), separators=(",", ":"))
    open(P["x"], "wb").write(gzip.compress(json.dumps(xl, separators=(",", ":")).encode(), 9))
    print("apply:", C)


if __name__ == "__main__":
    a = sys.argv[1:]
    arg = lambda k, d=None: a[a.index(k) + 1] if k in a else d
    if "--fetch" in a:
        fetch(int(arg("--budget", 300)), arg("--out", os.path.join(os.environ.get("RUNNER_TEMP") or "/tmp", "bse_xbrl_fills.json")),
              arg("--from-dir"))
    elif "--apply" in a:
        apply(arg("--apply"))
    else:
        print(__doc__)
