# -*- coding: utf-8 -*-
"""Build scripts/row_periods.json — which result rows cover SIX months (or twelve), proven from each company's own
filings (runbook §191).

WHY
  The stock page's Financial-detail card sums the result rows inside a 12-month year to get annual sales / operating
  profit (Ratios tab: debtor / inventory / payable days, ROCE; Cash-flow tab: CFO/OP). Rows are keyed by quarter-end
  only, so a quarter and a half-year look the same. An SME half-yearly filer's Sep row (Apr-Sep) + Mar row (Oct-Mar)
  IS its year (§181d); a quarterly filer's Dec + Mar quarters are only half of one (AARNAV Mar-2026: 231 cr read as a
  year's sales, debtor days 207). The page now counts a year only when its rows tile the 12 months, and a row is one
  quarter unless this list proves otherwise.

PROOF (arithmetic, never a label — 'Half yearly' / 'Yearly' name the FILING, and headers and context blocks both lie:
KSHITIJPOL and TARACHAND filed quarter figures in 'Half yearly' files, ABINFRA's 'Oct-Mar' Yearly OneD is its Jan-Mar
quarter, MAIDEN's Mar-2024 OneD is the whole year, DHARNI's Sep-2023 OneD is the Jul-Sep quarter):
  For the year ending Mar y, a Mar-y filing of basis b with OneD revenue h2 and FourD (full-year) revenue FY, and a
  Sep-(y-1) filing of any basis with an OneD or FourD revenue h1, form a PAIR when h1 + h2 == FY.
  * a stored Sep row is 6 months when its revenue equals such an h1 read from a filing of its own basis;
  * a stored Mar row is 6 months when its revenue equals such an h2 (the filing of its own basis), else 12 months when
    it equals that filing's FY figure and NOT its OneD figure (a Jan-Mar quarter equals the FY when Apr-Dec sold
    nothing — VCL Mar-2025 — so an FY that is also the OneD proves nothing);
  * a quarter-end is written only when EVERY revenue figure stored there (standalone and consolidated) is proven with
    the same length — a Mar row holding the H2 on one basis and the full year on the other proves nothing;
  * no pair counts for a basis whose rows of that year include a Jun or Dec row and already sum to its printed FY as
    quarters: then H1 + H2 == FY held only because some quarters were zero (BOHRAIND, SRPL: 0 + 0 == 0).
  * a pair with a ZERO half proves nothing by itself (0 + x == x holds for an all-zero placeholder column too — VIVO
    FY26 0 + 3.25 = 3.25, ASLIND 0 + 0 = 0; runbook §194): it counts only when PAT closes the same way from the same
    columns with all three figures non-zero (AAYUSHBULL FY23 revenue 0 + 13.23 = 13.23, PAT 0.06 + 0.20 = 0.26), tested
    to the filings' own rounding (fetch_bse_results_xbrl.closes).
  Revenue is what is matched: PAT tags are unreliable in these files (consolidated SME Yearly files print PAT 0.0 beside
  real revenue), and every flow the card sums over a year is revenue-based or sits on the same row.
  BSE SME HALF-YEARS PROVEN BY THE FETCHER (§194): docs/bse_fundamentals.json cells with h=1 carry the arithmetic that
  proved them ("pf": h1 + h2 = fy, PAT, the two filings). Their files usually live only on a CI runner, so this builder
  re-checks that arithmetic itself — the h flag alone is a label and never counts — and marks the row when the revenue
  the slice publishes is the proven half (entries tagged "src": "bse-pf", re-derived every run, never carried forward).

OUTPUT  {SYM: {qEnd: {"m": 6|12, "s": revStd, "c": revCon, "f": [evidence file names]}}} for the symbols of
docs/fin. docs/fin/<SYM>.json gets `pd` = {qEnd: m} from build_stock_fin.py, which re-checks "s"/"c" against the
revenue it is about to publish — a row a later writer changed loses its mark (the year goes blank, never wrong).

INPUTS  (local caches — gitignored; run on the Mac after new SME half-year results are loaded, §181d / §191)
  --sme-cache DIR   NSE SME XBRL cache (default scripts/_xbrl_cache_sme, or $SME_CACHE) — fetch_sme_xbrl.py, §148
  --bse-dir DIR     BSE results XBRL files (default ~/stocks-cache/bse_sme_xbrl) — Result_Arch_ng links, §178/§191
  --fin DIR         the published slices to read stored revenue from (default docs/fin)
  Entries whose evidence files are not in the scanned dirs are carried forward unchanged (they cannot be re-checked
  here, and the builder still re-validates their revenue).

Run: python3 scripts/build_row_periods.py [--dry]
"""
import argparse, gzip, json, os, re, sys
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
OUT = os.path.join(HERE, "row_periods.json")

TAGS = ("ReportingQuarter", "DateOfStartOfReportingPeriod", "DateOfEndOfReportingPeriod",
        "NatureOfReportStandaloneConsolidated", "Symbol", "ISIN", "ScripCode")
RE_TAG = {k: re.compile(r"<[\w-]+:%s(?:\s[^>]*)?>\s*([^<]*?)\s*<" % k) for k in TAGS}
RE_REV = {c: re.compile(r'<[\w-]+:RevenueFromOperations contextRef="%s"[^>]*>([^<]*)<' % c) for c in ("OneD", "FourD")}
RE_PAT = {c: re.compile(r'<[\w-]+:ProfitLossFor(?:The)?Period contextRef="%s"[^>]*>([^<]*)<' % c) for c in ("OneD", "FourD")}
RE_END = re.compile(r'<xbrli:context id="OneD">.*?<xbrli:endDate>([\d-]+)<', re.S)


def read(path):
    b = open(path, "rb").read()
    return (gzip.decompress(b) if b[:2] == b"\x1f\x8b" else b).decode("utf-8", "replace")


def crore(m):
    try:
        return round(float(m.group(1)) / 1e7, 2) if m and m.group(1).strip() else None
    except ValueError:
        return None


def rupees(m):
    try:
        return round(float(m.group(1))) if m and m.group(1).strip() else None
    except ValueError:
        return None


def parse(path):
    """Header facts + OneD / FourD revenue (₹ crore) of one results XBRL file, or None if unreadable."""
    try:
        s = read(path)
    except Exception:
        return None
    h = {k: (m.group(1).strip() if (m := r.search(s)) else None) for k, r in RE_TAG.items()}
    end = h["DateOfEndOfReportingPeriod"] or ((m.group(1)) if (m := RE_END.search(s)) else None)
    if not end or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", end):
        return None
    return {"f": os.path.basename(path), "qe": int(end.replace("-", "")), "rq": h["ReportingQuarter"] or "",
            "b": "c" if (h["NatureOfReportStandaloneConsolidated"] or "").lower().startswith("consol") else "s",
            "sym": (h["Symbol"] or "").upper(), "isin": h["ISIN"] or "", "code": h["ScripCode"] or "",
            "one": crore(RE_REV["OneD"].search(s)), "four": crore(RE_REV["FourD"].search(s)),
            "raw": {c: (rupees(RE_REV[c].search(s)), rupees(RE_PAT[c].search(s))) for c in ("OneD", "FourD")}}


def same_sum(a, b):          # the §181d year gate: H1 + H2 == FY
    return abs(a - b) <= max(0.02, 0.005 * abs(b))


def same_row(a, b):          # a stored figure equals the filing's (both 2-dp crore)
    return abs(a - b) <= 0.02


def proves(s, col, m):
    """A revenue pair (Sep filing s's column col + Mar filing m's OneD == its FourD) proves the year unless a half is zero:
    then only a PAT closure from the same columns, all three figures non-zero, counts (0 + x == x holds for an all-zero
    placeholder column as well — runbook §194)."""
    from fetch_bse_results_xbrl import closes
    (r1, p1), (r2, p2), pf = s["raw"][col], m["raw"]["OneD"], m["raw"]["FourD"][1]
    if r1 and r2:
        return True
    return bool(p1 and p2 and pf) and closes(p1, p2, pf)


def pf_ok(qe, cell):
    """Re-check the arithmetic a BSE h=1 cell carries (fetch_bse_results_xbrl.proof) — never the flag itself: h1 + h2 ==
    fy with neither half zero unless PAT closes non-zero the same way; 'fy' (H2 = FY - H1) needs the Mar filing's own
    OneD to have repeated the year (or been empty) and 0 < h1 < fy; the stored revenue must be h1 (Sep) or h2 (Mar)."""
    pf = cell.get("pf") or {}
    h1, h2, fy, how = pf.get("h1"), pf.get("h2"), pf.get("fy"), pf.get("how")
    rev = cell.get("rev")
    if None in (h1, h2, fy, rev) or how not in ("pair", "fy") or not same_sum(h1 + h2, fy):
        return False
    if how == "pair" and not (h1 and h2):
        p = pf.get("pat") or [None, None, None]
        if not (len(p) == 3 and all(p) and same_sum(p[0] + p[1], p[2])):
            return False
    if how == "fy" and not (0 < h1 < fy and (pf.get("m1") is None or same_row(pf["m1"], fy) or pf["m1"] == 0)):
        return False
    return same_row(rev, h1 if qe % 10000 == 930 else h2)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sme-cache", default=os.environ.get("SME_CACHE") or os.path.join(HERE, "_xbrl_cache_sme"))
    ap.add_argument("--bse-dir", default=os.path.expanduser("~/stocks-cache/bse_sme_xbrl"))
    ap.add_argument("--fin", default=os.path.join(ROOT, "docs", "fin"))
    ap.add_argument("--out", default=OUT)
    ap.add_argument("--dry", action="store_true")
    a = ap.parse_args()

    paths = []
    for d, kind in ((a.sme_cache, "nse"), (a.bse_dir, "bse")):
        if not os.path.isdir(d):
            print("WARN: %s missing — %s filings not scanned" % (d, kind))
            continue
        paths += [(os.path.join(d, f), kind) for f in sorted(os.listdir(d))
                  if not f.startswith(("list_", ".")) and not f.endswith(".json")]
    if not any(k == "nse" for _, k in paths):
        sys.exit("ABORT: no NSE SME XBRL files to read (set --sme-cache / SME_CACHE) — refusing to rewrite the list")
    with ProcessPoolExecutor(8) as ex:
        facts = list(ex.map(parse, [p for p, _ in paths], chunksize=64))
    scanned = {os.path.basename(p) for p, _ in paths}

    # filing -> the symbol its page is published under
    rmap = json.load(open(os.path.join(HERE, "_rename_map.json"), encoding="utf-8"))
    def norm(s):
        seen = set()
        while s in rmap and s not in seen and rmap[s] != s:
            seen.add(s); s = rmap[s]
        return s
    code2tk = {str(v): k for k, v in json.load(open(os.path.join(HERE, "bse_scrips.json"), encoding="utf-8"))["by_id"].items()}
    have = {f[:-5] for f in os.listdir(a.fin) if f.endswith(".json")}
    sys.path.insert(0, HERE)
    from build_stock_fin import slug, nse_tape_isin
    isin2sym = {}
    for s_, i_ in nse_tape_isin().items():
        isin2sym.setdefault(i_, s_)
    files = defaultdict(list)               # (sym, qe) -> [fact]
    unmatched = 0
    for (p, kind), x in zip(paths, facts):
        if not x:
            continue
        sym = None
        if kind == "bse":
            sym = code2tk.get(x["code"])
        elif x["sym"] and x["sym"] not in ("NA", "NOTLISTED", "-"):
            sym = norm(x["sym"])
        if (not sym or slug(sym) not in have) and x["isin"]:
            sym = isin2sym.get(x["isin"], sym)
        if not sym or slug(sym) not in have:
            unmatched += 1
            continue
        files[(sym, x["qe"])].append(x)

    out, n_rows = {}, 0
    for sym in sorted({s for s, _ in files}):
        F = json.load(open(os.path.join(a.fin, slug(sym) + ".json"), encoding="utf-8"))
        rv = F.get("revop") or {}
        marks = {}
        for y in sorted({qe // 10000 + (1 if qe % 10000 == 930 else 0) for s_, qe in files if s_ == sym
                         and qe % 10000 in (930, 331)}):
            sep, mar = (y - 1) * 10000 + 930, y * 10000 + 331
            S, M = files.get((sym, sep), []), files.get((sym, mar), [])
            h1s = [(f[k], f, c) for f in S for k, c in (("one", "OneD"), ("four", "FourD")) if f[k] is not None]
            pairs = [(h1, f1, m) for m in M if m["one"] is not None and m["four"] is not None
                     for h1, f1, c1 in h1s if same_sum(h1 + m["one"], m["four"]) and proves(f1, c1, m)]
            # QUARTERS THAT ALREADY TILE THE YEAR veto the pair: when a basis also holds a Jun or Dec row and its rows
            # sum to that basis's printed FY as quarters, H1 + H2 == FY held only because quarters were zero
            # (BOHRAIND / SRPL sell nothing: 0 + 0 == 0 "proved" their Sep and Mar quarters were half-years).
            # A filer of both quarters and half-years (QMSMEDI: Q1 + H1 + Q3 + H2 = 224 cr vs FY 152) is not vetoed.
            veto = set()
            for b, i in (("s", 0), ("c", 1)):
                vals = {q: rv[str(q)][i] for q in ((y - 1) * 10000 + 630, sep, (y - 1) * 10000 + 1231, mar)
                        if len(rv.get(str(q)) or ()) > i and rv[str(q)][i] is not None}
                if any(q % 10000 in (630, 1231) for q in vals) and any(
                        same_sum(sum(vals.values()), m["four"]) for m in M if m["b"] == b and m["four"] is not None):
                    veto.add(b)
            for qe in (sep, mar):
                row = rv.get(str(qe))
                if not row:
                    continue
                got, ev = {}, set()
                for b, i in (("s", 0), ("c", 1)):
                    v = row[i] if len(row) > i else None
                    if v is None:
                        continue
                    m_ = None
                    if b in veto:
                        pass
                    elif qe == sep:
                        hit = [(f1, m) for h1, f1, m in pairs if f1["b"] == b and same_row(v, h1)]
                        if hit: m_ = 6; ev |= {hit[0][0]["f"], hit[0][1]["f"]}
                    else:
                        hit = [(f1, m) for h1, f1, m in pairs if m["b"] == b and same_row(v, m["one"])]
                        if hit:
                            m_ = 6; ev |= {hit[0][0]["f"], hit[0][1]["f"]}
                        else:
                            # the full year only when it cannot also be the filing's own OneD figure: VCL Mar-2025's
                            # Jan-Mar quarter 5.25 equals its FY because Apr-Dec sold nothing — that row is a quarter
                            fy = [m for m in M if m["b"] == b and m["four"] is not None and same_row(v, m["four"])
                                  and not (m["one"] is not None and same_row(v, m["one"]))]
                            if fy: m_ = 12; ev.add(fy[0]["f"])
                    got[b] = m_
                ms = set(got.values())
                if got and len(ms) == 1 and None not in ms:
                    marks[str(qe)] = {"m": ms.pop(), "s": row[0], "c": row[1] if len(row) > 1 else None, "f": sorted(ev)}
        if marks:
            out[sym] = marks; n_rows += len(marks)

    # BSE SME half-years the fetcher proved (h=1 + pf, runbook §194) whose files this run did not pair: re-check the
    # arithmetic they carry and mark the row when the slice publishes exactly the proven revenue on that basis, the
    # other basis is empty or the same figure, and the basis's rows of the year do not already tile it as quarters.
    n_pf = 0
    bf = json.load(open(os.path.join(ROOT, "docs", "bse_fundamentals.json"), encoding="utf-8")).get("px", {})
    for code, cells in sorted(bf.items()):
        sym = code2tk.get(str(code))
        if not sym or slug(sym) not in have or not isinstance(cells, dict):
            continue
        rv = None
        for qe, c in sorted(cells.items()):
            if not (str(qe).isdigit() and isinstance(c, dict) and c.get("h") == 1 and pf_ok(int(qe), c)):
                continue
            if qe in out.get(sym, {}):
                continue                                    # the files already decided this row
            if rv is None:
                rv = json.load(open(os.path.join(a.fin, slug(sym) + ".json"), encoding="utf-8")).get("revop") or {}
            row = rv.get(qe) or []
            i = 1 if c.get("basis") == "C" else 0
            v, other = (row[i] if len(row) > i else None), (row[1 - i] if len(row) > 1 - i else None)
            if v is None or not same_row(v, c["rev"]) or (other is not None and not same_row(other, v)):
                continue
            y = int(qe) // 10000 + (1 if int(qe) % 10000 == 930 else 0)
            qs = [str(q) for q in ((y - 1) * 10000 + 630, (y - 1) * 10000 + 930, (y - 1) * 10000 + 1231, y * 10000 + 331)]
            vals = [rv[q][i] for q in qs if len(rv.get(q) or ()) > i and rv[q][i] is not None]
            if any(len(rv.get(q) or ()) > i and rv[q][i] is not None for q in (qs[0], qs[2])) and \
                    same_sum(sum(vals), c["pf"]["fy"]):
                continue                                    # quarters already tile the year (the file rule's veto)
            out.setdefault(sym, {})[qe] = {"m": 6, "s": row[0] if row else None, "c": row[1] if len(row) > 1 else None,
                                          "f": sorted(c["pf"].get("f") or []), "src": "bse-pf"}
            n_rows += 1; n_pf += 1

    # carry forward entries this run could not re-check (their evidence files were not scanned); "bse-pf" entries are
    # re-derived from docs/bse_fundamentals.json every run, so one that no longer proves is dropped, not carried
    old = {}
    if os.path.exists(a.out):
        old = {k: v for k, v in json.load(open(a.out, encoding="utf-8")).items() if not k.startswith("_")}
    kept = 0
    for sym, qs in old.items():
        for qe, e in qs.items():
            if qe not in out.get(sym, {}) and not (set(e.get("f") or ()) & scanned) and e.get("src") != "bse-pf":
                out.setdefault(sym, {})[qe] = e; kept += 1
    added = sum(1 for s in out for q in out[s] if q not in old.get(s, {}))
    dropped = sum(1 for s in old for q in old[s] if q not in out.get(s, {}))
    n6 = sum(1 for s in out for q in out[s] if out[s][q]["m"] == 6)
    print("filings read %d (%d undated, %d not on a published page); rows proven %d (6-month %d, 12-month %d) on "
          "%d symbols, %d of them from BSE h=1 proofs; carried forward %d; vs the committed list: +%d −%d"
          % (len(paths), sum(1 for x in facts if not x), unmatched, n_rows + kept, n6, n_rows + kept - n6,
             len(out), n_pf, kept, added, dropped))
    if a.dry:
        return
    doc = {"_note": "Result rows proven to cover 6 or 12 months (runbook §191, §194) — built by scripts/build_row_periods.py; "
                    "never edit by hand. {SYM: {qEnd: {m: months, s/c: the revenue proven, f: evidence filings, "
                    "src: 'bse-pf' when proven by a BSE h=1 cell's arithmetic}}}"}
    doc.update({s: dict(sorted(out[s].items())) for s in sorted(out)})
    with open(a.out, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, ensure_ascii=False, separators=(",", ":"))
        fh.write("\n")
    print("wrote", os.path.relpath(a.out, ROOT))


if __name__ == "__main__":
    main()
