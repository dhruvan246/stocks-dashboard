# -*- coding: utf-8 -*-
"""FII/DII backfill from BSE's OWN ShareholdingPattern.aspx — the live route nobody probed.

Discovery (2026-08-11): SHPQNewFormat rows for pre-XBRL quarters carry a `navigateurl`
pointing at https://www.bseindia.com/corporates/ShareholdingPattern.aspx?scripcd=<code>
&flag_qtr=1&qtrid=<q>.00&Flag=<New|Old> — and that page STILL SERVES:
  Flag=New : Clause-35 category table (same layout MC mirrored), Jun-2006 (q50) -> Mar-2016+
  Flag=Old : 1997-format table (FIIS / MF and UTI / Banks-FI-Insurance lump), <= Mar-2006 (q49)
qtrid is GLOBAL: qtrid = (year-2001)*4 + {Mar:29, Jun:30, Sep:31, Dec:32}[month].

Derivation rules are the SAME as fetch_shp_wayback_mc.cmd_ledger (both read the same
Clause-35 table): col = %of(A+B+C); dii = mf + banks + ins; inst reconciliation gate at 1pp;
fii NEVER zero-defaulted; prom fallback via pubtot>=99 complement. Flag=Old adds:
dii = mf + the Banks/FI/Insurance LUMP, ins stored as None (inside the lump — writing 0.0
would fabricate "no insurance holding"), reconciliation |mf+lump+fii - inst_sub| <= 0.10.

Stages:
  python3 fetch_shp_bse_aspx.py frontier   # missing member-qtr cells x scripcode -> frontier.json
  python3 fetch_shp_bse_aspx.py pilot N    # fetch+parse a stratified sample, gate vs stored cells
  python3 fetch_shp_bse_aspx.py harvest    # full frontier -> ledger shp_fill_bse_aspx.json.gz
Writes ONLY inside its --dir (default: alongside this script). Read-only on the repo.
"""
import os, sys, json, re, gzip, time, datetime, subprocess, argparse, threading
from collections import defaultdict, Counter
from concurrent.futures import ThreadPoolExecutor
from curl_cffi import requests as cr

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = "/Users/dhruvan/stocks-dashboard"
H_API = {"Referer": "https://www.bseindia.com/", "Accept": "application/json, text/plain, */*"}
H_HTML = {"Referer": "https://www.bseindia.com/", "Accept": "text/html,application/xhtml+xml"}
LAG_DAYS = 21
QOFF = {3: 29, 6: 30, 9: 31, 12: 32}
_lk = threading.Lock()


def gitshow(path):
    r = subprocess.run(["git", "show", "origin/main:" + path], capture_output=True, cwd=REPO)
    if r.returncode:
        sys.exit("git show failed for %s" % path)
    return json.loads(r.stdout)


def qtrid_of(qe):
    y, m = int(qe[:4]), int(qe[5:7])
    return (y - 2001) * 4 + QOFF[m]


def norm_name(s):
    return re.sub(r"[^a-z0-9]", "", (s or "").lower().replace("limited", "").replace("ltd", ""))


# ---------------------------------------------------------------- frontier
def cmd_frontier(dirp):
    ih = gitshow("scripts/indices_history.json")
    rmap = gitshow("scripts/_rename_map.json")
    hist = gitshow("scripts/shp_history.json")
    skipf = gitshow("scripts/shp_no_filing.json").get("cells", {})
    names = hist.get("_names", {})
    try:
        override = gitshow("scripts/_shp_scripcode_override.json")
    except SystemExit:
        override = {}

    def norm(s):
        seen = set()
        while s in rmap and s not in seen and rmap[s] != s:
            seen.add(s); s = rmap[s]
        return s

    have = defaultdict(dict)
    for k, v in hist.items():
        if not k.startswith("_") and isinstance(v, dict):
            have[norm(k)].update(v)
    skip = {(norm(s), qe) for s, qs in skipf.items() for qe in qs}

    snaps = sorted((s["effectiveDate"], [norm(x) for x in s["symbols"] if not x.startswith("DUMMY")])
                   for s in ih["Nifty 500"])

    def members(qe):
        best = []
        for ed, syms in snaps:
            if ed <= qe: best = syms
            else: break
        return best

    qes = ["%d%s" % (y, s) for y in range(2002, 2017) for s in ("-03-31", "-06-30", "-09-30", "-12-31")]
    qes = [q for q in qes if "2002-12-31" <= q <= "2016-03-31"]
    missing = []
    for qe in qes:
        if qe in ("2015-12-31", "2016-03-31"):
            continue      # qtrid 88/89: BSE's own table has the seam defect (fabricated FII 0.00 /
                          # broken inst recon — pilot-measured on ITC + 4 others). The MC-derivation
                          # seam route owns those cells; a raw read here would poison them.
        for s in members(qe):
            if (s, qe) in skip: continue
            if qe not in have.get(s, {}):
                missing.append((s, qe))
    # the two named post-window holes this route was measured to serve
    for s, qe in (("MONSANTO", "2016-09-30"), ("JMTAUTOLTD", "2016-09-30")):
        if qe not in have.get(s, {}):
            missing.append((s, qe))

    # scripcode resolution: full master incl. Delisted/Suspended (status BLANK — §22f), + override
    mfile = os.path.join(dirp, "bse_master_all.json")
    if os.path.exists(mfile) and os.path.getsize(mfile) > 1e6:
        master = json.load(open(mfile))
    else:
        r = cr.get("https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w?Group=&Scripcode=&industry=&segment=Equity&status=",
                   headers=H_API, impersonate="chrome", timeout=120)
        master = json.loads(r.text)
        json.dump(master, open(mfile, "w"))
    by_id = {}
    for row in master:
        sid = str(row.get("scrip_id") or "").strip().upper()
        if sid and sid not in by_id:
            by_id[sid] = (int(row["SCRIP_CD"]), row.get("Scrip_Name") or row.get("SCRIP_NAME") or "")
    n_override = 0
    front = []
    unresolved = Counter()
    for s, qe in missing:
        code, bname = None, ""
        if s in override:
            code = int(override[s]["scripcode"] if isinstance(override[s], dict) else override[s])
            n_override += 1
        elif s in by_id:
            code, bname = by_id[s]
        if code is None:
            unresolved[s] += 1
            continue
        front.append({"sym": s, "qe": qe, "code": code, "qtrid": qtrid_of(qe),
                      "bname": bname, "lname": names.get(s, "")})
    json.dump(front, open(os.path.join(dirp, "frontier.json"), "w"), indent=0)
    json.dump({k: v for k, v in sorted(unresolved.items())},
              open(os.path.join(dirp, "unresolved.json"), "w"), indent=1)
    print("MISSING member-qtr cells: %d   frontier (scripcode resolved): %d  (override %d)"
          % (len(missing), len(front), n_override))
    print("UNRESOLVED symbols: %d (%d cells) -> unresolved.json"
          % (len(unresolved), sum(unresolved.values())))
    per_era = Counter(f["qe"][:4] for f in front)
    print("frontier by year:", dict(sorted(per_era.items())))


# ---------------------------------------------------------------- fetch + parse
def fetch_page(dirp, code, qtrid, flag):
    cf = os.path.join(dirp, "cache", "%d_%d_%s.html.gz" % (code, qtrid, flag))
    if os.path.exists(cf):
        with gzip.open(cf, "rt", encoding="utf-8", errors="replace") as fh:
            return fh.read(), True
    u = ("https://www.bseindia.com/corporates/ShareholdingPattern.aspx"
         "?scripcd=%d&flag_qtr=1&qtrid=%d.00&Flag=%s" % (code, qtrid, flag))
    for attempt in range(3):
        try:
            r = cr.get(u, headers=H_HTML, impersonate="chrome", timeout=45)
            if r.status_code == 200 and len(r.text) > 3000:   # 162-byte 302 trap: never trust tiny bodies
                os.makedirs(os.path.dirname(cf), exist_ok=True)
                with gzip.open(cf, "wt", encoding="utf-8") as fh:
                    fh.write(r.text)
                return r.text, False
        except Exception:
            pass
        time.sleep(3 * (attempt + 1))
    return None, False


ROW_LABELS = {   # ported verbatim from fetch_shp_wayback_mc (same Clause-35 table)
    "prom": r"Total shareholding of Promoter and Promoter Group\s*\(A\)",
    "mf": r"Mutual Funds\s*/\s*UTI",
    "banks": r"Financial Institutions\s*/\s*Banks",
    "govt": r"Central Government\s*/\s*State Government",
    "ins": r"Insurance Companies",
    "fii": r"Foreign Institutional Investors",
    "qfi": r"Qualified Foreign Investor",
    "vcf": r"^Venture Capital Funds",
    "fvci": r"Foreign Venture Capital Investors",
    "pubtot": r"Total Public shareholding\s*\(B\)",
}


def _cells(html):
    text = re.sub(r"<[^>]+>", "\x01", html)
    import html as _h
    cs = [re.sub(r"[\s\xa0]+", " ", _h.unescape(c)).strip() for c in text.split("\x01")]
    return [c for c in cs if c]


def parse_new(html):
    """Clause-35 aspx table -> cols{slot: (pctAB, pctABC)}; page-name identity returned too."""
    cells = _cells(html)
    nm = ""
    for i, c in enumerate(cells):
        if c == "Shareholding Pattern" and i + 1 < len(cells):
            nm = cells[i + 1] if cells[i + 1] != "Shareholding Pattern" else (cells[i + 2] if i + 2 < len(cells) else "")
            break

    def row_nums(i):
        nums = []
        for c2 in cells[i + 1:i + 9]:
            c2 = c2.replace(",", "").strip()
            if c2 in ("-", ""): nums.append(None); continue
            if re.fullmatch(r"\d+(?:\.\d+)?", c2): nums.append(float(c2))
            else: break
        return nums

    def pair(nums):
        if len(nums) >= 5: return (nums[3], nums[4])
        if len(nums) == 4: return (nums[3], nums[3])
        return None

    def find_row(rxs, lo=0, hi=None):
        rx = re.compile(rxs, re.I)
        for i in range(lo, hi if hi is not None else len(cells)):
            if rx.search(cells[i]):
                p = pair(row_nums(i))
                if p: return p
        return None

    out = {}
    p = find_row(ROW_LABELS["prom"])
    if p: out["prom"] = p
    p = find_row(ROW_LABELS["pubtot"])
    if p: out["pubtot"] = p
    blk_lo = blk_hi = None
    for rxs in (r"\(1\)\s*Institutions?\s*$", r"^Institutions$"):
        for i, c in enumerate(cells):
            if re.search(rxs, c, re.I):
                blk_lo = i
                for j in range(i + 1, min(i + 160, len(cells))):
                    if re.fullmatch(r"Sub\s*Total", cells[j], re.I):
                        blk_hi = j + 1; break
                break
        if blk_lo is not None: break
    if blk_lo is not None:
        hi = blk_hi if blk_hi is not None else min(blk_lo + 160, len(cells))
        for slot in ("mf", "banks", "govt", "ins", "fii", "qfi", "vcf", "fvci"):
            p = find_row(ROW_LABELS[slot], blk_lo, hi)
            if p: out[slot] = p
        if blk_hi is not None:
            p = pair(row_nums(blk_hi - 1))
            if p: out["inst_sub"] = p
    if "fii" not in out and "mf" not in out and "inst_sub" not in out:
        return None, nm
    return out, nm


def parse_old(html):
    """1997-format table -> dict(prom, fii, mf, lump, inst_sub) in % of grand total."""
    cells = _cells(html)
    nm = ""
    for i, c in enumerate(cells):
        if c == "Shareholding Pattern" and i + 1 < len(cells):
            nm = cells[i + 1] if cells[i + 1] != "Shareholding Pattern" else (cells[i + 2] if i + 2 < len(cells) else "")
            break

    def val_after(rxs, lo=0, hi=None):
        rx = re.compile(rxs, re.I)
        for i in range(lo, hi if hi is not None else len(cells)):
            if rx.search(cells[i]):
                nums = []
                for c2 in cells[i + 1:i + 4]:
                    c2 = c2.replace(",", "").strip()
                    if re.fullmatch(r"\d+(?:\.\d+)?", c2): nums.append(float(c2))
                    else: break
                if len(nums) >= 2: return (i, nums[1])     # (shares, pct) -> pct
        return (None, None)

    out = {}
    # promoter block: "Promoter's Holding ... Sub Total"
    ip = next((i for i, c in enumerate(cells) if re.search(r"Promoter'?s? Holding", c, re.I)), None)
    inp = next((i for i, c in enumerate(cells) if re.search(r"Non Promoter'?s? Holding", c, re.I)), None)
    if ip is not None and inp is not None and inp > ip:
        _, v = val_after(r"^Sub\s*Total$", ip, inp)
        if v is not None: out["prom"] = v
    lo = inp if inp is not None else 0
    _, v = val_after(r"^FIIS?\b", lo);                                out["fii"] = v
    _, v = val_after(r"Mutual Funds? and UTI", lo);                   out["mf"] = v
    _, v = val_after(r"Banks\s*,?\s*Financial Institutions?\s*,?\s*Insurance", lo); out["lump"] = v
    ii = next((i for i, c in enumerate(cells[lo:], lo) if re.search(r"Institutional Investors?$", c, re.I)), None)
    if ii is not None:
        _, v = val_after(r"^Sub\s*Total$", ii)
        if v is not None: out["inst_sub"] = v
    if out.get("fii") is None and out.get("mf") is None:
        return None, nm
    return out, nm


def cell_of(fr, dirp, neigh=None):
    """fetch + parse + derive one frontier cell -> (status, cell|None, detail)"""
    code, q, qe, sym = fr["code"], fr["qtrid"], fr["qe"], fr["sym"]
    flag_first = "New" if q >= 50 else "Old"
    html, _ = fetch_page(dirp, code, q, flag_first)
    used = flag_first
    cols = nm = None
    if html:
        cols, nm = (parse_new if flag_first == "New" else parse_old)(html)
    if cols is None:
        flag2 = "Old" if flag_first == "New" else "New"
        html2, _ = fetch_page(dirp, code, q, flag2)
        if html2:
            c2, nm2 = (parse_new if flag2 == "New" else parse_old)(html2)
            if c2 is not None:
                cols, nm, used, html = c2, nm2, flag2, html2
    if cols is None:
        return ("absent", None, "no category rows either flag")

    # identity: page company name vs ledger/master name (era renames make this fuzzy — containment)
    pn, ln, bn = norm_name(nm), norm_name(fr.get("lname")), norm_name(fr.get("bname"))
    if pn and (ln or bn) and not (ln and (ln in pn or pn in ln)) and not (bn and (bn in pn or pn in bn)):
        return ("identity", None, "page='%s' vs '%s'/'%s'" % (nm, fr.get("lname"), fr.get("bname")))

    sub = (datetime.date(*map(int, qe.split("-"))) + datetime.timedelta(days=LAG_DAYS)).isoformat()
    src = "bseaspx:%d:%d:%s" % (code, q, used)
    if used == "New":
        def val(slot):
            p = cols.get(slot)
            if not p: return None
            return p[1] if p[1] is not None else p[0]     # %of(A+B+C), stored convention
        fii = val("fii")
        if fii is None:
            return ("no-fii", None, "fii row absent/dashes")
        prom = val("prom")
        if prom is None:
            pt = cols.get("pubtot")
            pab = pt[0] if pt and pt[0] is not None else None
            if pab is not None and pab >= 99.0:
                prom = round(max(0.0, 100.0 - pab), 2)
            else:
                return ("no-prom", None, "no promoter total")
        mf = val("mf") or 0.0
        dii = mf + (val("banks") or 0.0) + (val("ins") or 0.0)
        inst = val("inst_sub")
        if inst is not None:
            if abs(fii + dii + (val("govt") or 0.0) + (val("qfi") or 0.0) - inst) > 1.0:
                return ("recon", None, "inst recon fail")
        ins = round(val("ins") or 0.0, 2)
    else:
        fii, mf, lump, prom, inst = cols.get("fii"), cols.get("mf"), cols.get("lump"), cols.get("prom"), cols.get("inst_sub")
        if fii is None:
            # 1997 format omits empty rows. Absent FIIS = fii 0 ONLY when the block's own
            # arithmetic proves it: inst_sub == mf + lump to a print unit. Else refuse (§57).
            if inst is not None and abs(inst - ((mf or 0.0) + (lump or 0.0))) <= 0.15:
                fii = 0.0
            else:
                return ("no-fii", None, "FIIS row absent, residual unproven")
        if prom is None:
            return ("no-prom", None, "promoter subtotal absent")
        mf = mf or 0.0
        dii = mf + (lump or 0.0)
        if inst is not None and abs((mf + (lump or 0.0) + fii) - inst) > 0.15:
            return ("recon", None, "old recon fail %.2f vs %.2f" % (mf + (lump or 0.0) + fii, inst))
        ins = None                                        # inside the lump — never fabricate 0.0
    if not (0 <= prom <= 100 and 0 <= fii <= 100 and 0 <= dii <= 100):
        return ("range", None, "out of range")
    # fabricated-zero guard (MONSANTO Sep-16 class): an exact fii 0.00 beside a stored
    # neighbour holding >1% is the seam defect wearing a company suit — refuse, never write.
    if fii == 0.0 and neigh:
        nb = [neigh[k] for k in ((sym, _adj(qe, -1)), (sym, _adj(qe, +1))) if k in neigh]
        if any(v > 1.0 for v in nb):
            return ("zero-vs-neighbour", None, "fii 0.00 beside stored %.2f" % max(nb))
    return ("ok", [round(prom, 2), round(fii, 2), round(dii, 2), round(mf, 2), ins, sub, None, src],
            used)


def _adj(qe, step):
    y, m = int(qe[:4]), int(qe[5:7])
    m += 3 * step
    y, m = y + (m - 1) // 12, (m - 1) % 12 + 1
    return "%04d-%02d-%s" % (y, m, {3: "31", 6: "30", 9: "30", 12: "31"}[m])


# ---------------------------------------------------------------- pilot / harvest
def run(dirp, front, workers, tag):
    hist = gitshow("scripts/shp_history.json")
    neigh = {}
    for s, qs in hist.items():
        if s.startswith("_") or not isinstance(qs, dict):
            continue
        for qe, v in qs.items():
            if isinstance(v, list) and len(v) > 1 and v[1] is not None:
                neigh[(s, qe)] = v[1]
    res, stats = {}, Counter()
    t0 = time.time()

    def one(fr):
        st, cell, det = cell_of(fr, dirp, neigh)
        with _lk:
            stats[st] += 1
            if st == "ok":
                res.setdefault(fr["sym"], {})[fr["qe"]] = cell
            elif stats[st] <= 8:
                print("  [%s] %s %s: %s" % (st, fr["sym"], fr["qe"], det), flush=True)
        time.sleep(0.4)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(one, front))
    n = sum(len(v) for v in res.values())
    print("\n%s: %d cells parsed OK of %d fetched in %.0fs — %s"
          % (tag, n, len(front), time.time() - t0, dict(stats)), flush=True)

    # overlap gate: everything we parsed that ALREADY exists in the ledger must reproduce
    diffs = []
    for sym, qs in res.items():
        for qe, cell in qs.items():
            old = (hist.get(sym) or {}).get(qe)
            if old:
                diffs.append((abs(cell[1] - old[1]), abs(cell[2] - old[2]), sym, qe, cell[1], old[1]))
    if diffs:
        bad = [d for d in diffs if d[0] > 0.11 or d[1] > 0.11]
        diffs.sort(reverse=True)
        print("OVERLAP GATE: %d overlap cells, %d disagree beyond 0.11pp" % (len(diffs), len(bad)))
        for d in (bad or diffs[:5])[:10]:
            print("   dFII=%.2f dDII=%.2f %s %s aspx=%.2f stored=%.2f" % d)
    return res, stats, diffs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["frontier", "pilot", "harvest"])
    ap.add_argument("n", nargs="?", type=int, default=60)
    ap.add_argument("--dir", default=HERE)
    ap.add_argument("--workers", type=int, default=4)
    a = ap.parse_args()
    os.makedirs(os.path.join(a.dir, "cache"), exist_ok=True)
    if a.cmd == "frontier":
        cmd_frontier(a.dir); return
    front = json.load(open(os.path.join(a.dir, "frontier.json")))
    if a.cmd == "pilot":
        # stratified: spread across years, plus deliberate OVERLAP cells (already-stored) as the gate
        hist = gitshow("scripts/shp_history.json")
        by_year = defaultdict(list)
        for f in front:
            by_year[f["qe"][:4]].append(f)
        take = []
        per = max(2, a.n // (len(by_year) + 4))
        for y in sorted(by_year):
            take += by_year[y][:per]
        # overlap sample: stored cells re-fetched deliberately (not in frontier — build ad hoc)
        rmap = gitshow("scripts/_rename_map.json")
        ov = []
        for sym in ("RELIANCE", "ITC", "HDFCBANK", "INFY", "TATASTEEL", "CAPF", "SUNPHARMA", "WIPRO"):
            qs = hist.get(sym) or {}
            for qe in sorted(qs):
                if qe <= "2016-03-31" and len(ov) < 24:
                    code = {"RELIANCE": 500325, "ITC": 500875, "HDFCBANK": 500180, "INFY": 500209,
                            "TATASTEEL": 500470, "CAPF": 532938, "SUNPHARMA": 524715, "WIPRO": 507685}[sym]
                    ov.append({"sym": sym, "qe": qe, "code": code, "qtrid": qtrid_of(qe),
                               "bname": sym, "lname": sym})
        print("pilot: %d frontier cells + %d overlap cells" % (len(take), len(ov)))
        run(a.dir, take + ov, a.workers, "PILOT")
        return
    if a.cmd == "harvest":
        res, stats, diffs = run(a.dir, front, a.workers, "HARVEST")
        out = os.path.join(a.dir, "shp_fill_bse_aspx.json.gz")
        with gzip.open(out, "wt", encoding="utf-8") as fh:
            json.dump(res, fh)
        print("ledger -> %s  (%d syms, %d cells)" % (out, len(res), sum(len(v) for v in res.values())))


if __name__ == "__main__":
    main()
