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
  Revenue only: PAT tags are unreliable in these files (consolidated SME Yearly files print PAT 0.0 beside real
  revenue), and every flow the card sums over a year is revenue-based or sits on the same row.

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
RE_END = re.compile(r'<xbrli:context id="OneD">.*?<xbrli:endDate>([\d-]+)<', re.S)


def read(path):
    b = open(path, "rb").read()
    return (gzip.decompress(b) if b[:2] == b"\x1f\x8b" else b).decode("utf-8", "replace")


def crore(m):
    try:
        return round(float(m.group(1)) / 1e7, 2) if m and m.group(1).strip() else None
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
            "one": crore(RE_REV["OneD"].search(s)), "four": crore(RE_REV["FourD"].search(s))}


def same_sum(a, b):          # the §181d year gate: H1 + H2 == FY
    return abs(a - b) <= max(0.02, 0.005 * abs(b))


def same_row(a, b):          # a stored figure equals the filing's (both 2-dp crore)
    return abs(a - b) <= 0.02


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
            h1s = [(v, f) for f in S for v in {f["one"], f["four"]} if v is not None]
            pairs = [(h1, f1, m) for m in M if m["one"] is not None and m["four"] is not None
                     for h1, f1 in h1s if same_sum(h1 + m["one"], m["four"])]
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

    # carry forward entries this run could not re-check (their evidence files were not scanned)
    old = {}
    if os.path.exists(a.out):
        old = {k: v for k, v in json.load(open(a.out, encoding="utf-8")).items() if not k.startswith("_")}
    kept = 0
    for sym, qs in old.items():
        for qe, e in qs.items():
            if qe not in out.get(sym, {}) and not (set(e.get("f") or ()) & scanned):
                out.setdefault(sym, {})[qe] = e; kept += 1
    added = sum(1 for s in out for q in out[s] if q not in old.get(s, {}))
    dropped = sum(1 for s in old for q in old[s] if q not in out.get(s, {}))
    n6 = sum(1 for s in out for q in out[s] if out[s][q]["m"] == 6)
    print("filings read %d (%d undated, %d not on a published page); rows proven %d (6-month %d, 12-month %d) on "
          "%d symbols; carried forward %d; vs the committed list: +%d −%d"
          % (len(paths), sum(1 for x in facts if not x), unmatched, n_rows + kept, n6, n_rows + kept - n6,
             len(out), kept, added, dropped))
    if a.dry:
        return
    doc = {"_note": "Result rows proven to cover 6 or 12 months (runbook §191) — built by scripts/build_row_periods.py; "
                    "never edit by hand. {SYM: {qEnd: {m: months, s/c: the revenue proven, f: evidence filings}}}"}
    doc.update({s: dict(sorted(out[s].items())) for s in sorted(out)})
    with open(a.out, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, ensure_ascii=False, separators=(",", ":"))
        fh.write("\n")
    print("wrote", os.path.relpath(a.out, ROOT))


if __name__ == "__main__":
    main()
