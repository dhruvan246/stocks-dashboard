# -*- coding: utf-8 -*-
"""Build docs/fin/<SLUG>.json — the per-stock FINANCIALS slice for docs/stock.html.

WHY SEPARATE FROM THE PRICE SLICE (build_stock_slices.py)
  Prices move for every stock every trading day, so price slices are force-pushed
  to the sf-data Pages repo (committing 40 MB/day here would be repo suicide).
  Financials move on a completely different clock: one company at a time, whenever
  it files. Only that company's file changes, so these CAN be committed — git only
  stores the handful of blobs that actually changed — and rebuilding them the
  moment a results workflow lands keeps the page as fresh as the feed. Folding
  them into the daily price push instead would have left the quarterly table up to
  24 h stale in the middle of results season.

WHAT IT REPLACES
  stock.html used to pull sf_fundamentals.json (3.2 MB) + sf_revop.json (3.9 MB) +
  shareholding.json (0.9 MB) — 8 MB of whole-market data — to fill one company's
  three tables. A fin slice is ~2 KB.

  fund   point-in-time quarterly net profit, OWNERS-attributable
         [[qEndYYYYMMDD, npStd, annStd, npCon, annCon], …]  (sf_fundamentals.json)
  revop  {qEnd: [revStd, revCon, opStd, opCon, patStd, patCon, fin, ebitStd, ebitCon]}
         (sf_revop.json — op = EBITDA, fin=1 marks banks/NBFCs; idx4/idx5 are a PAT
         MIRROR only — incomplete, never rendered; PAT authority is `fund`, runbook §70)
  shpQ   quarter-end dates, newest first ] the stock's row of the quarterly
  shp    the matching holding cells       ] shareholding-pattern feed
  shpH   FULL shareholding history, oldest first (scripts/shp_history.json —
         back to 2019-09-30): [[qEndISO, prom%, fii%, dii%, mf%, ins%,
         subDate, nShareholders], …]. shpQ/shp (8 quarters) stay for readers
         that predate it; the page prefers shpH when present.
  x      DEEP quarter detail from the XBRL re-parse (scripts/xbrl_extra.json[.gz],
         built by build_xbrl_extra.py): {qEnd: {s:{...}, c:{...}}} — EPS, interest/
         depreciation/tax/exceptional, balance sheet, cash flow (+cf_d period days),
         segments, bank NPA/CET1/ROA, audited flag. ₹ crore / ₹ per share / %.

RENAMES
  Fundamentals are keyed by a company's CURRENT ticker while its price history
  keeps trading under the name of the day (TATAMOTORS→TMPV, PVR→PVRINOX, …), so a
  slice is also written under every OLD symbol that resolves into a live one —
  the same fallback fundFor()/FUND_ALIAS does in backtest-engine.js. Without it
  every renamed stock's page would show "no quarterly earnings on file".

Run: python scripts/build_stock_fin.py [--out DIR]   (--out: local verification
     builds that must not touch the committed docs/fin/)
"""
import argparse, gzip, json, os, re, shutil, sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
DOCS = os.path.join(ROOT, "docs")
OUT  = os.path.join(DOCS, "fin")

FUND_J  = os.path.join(DOCS, "sf_fundamentals.json")
REVOP_J = os.path.join(DOCS, "sf_revop.json")
SHP_J   = os.path.join(DOCS, "shareholding.json")
SHPH_J  = os.path.join(HERE, "shp_history.json")
XTRA_J  = os.path.join(HERE, "xbrl_extra.json")      # local build output…
XTRA_GZ = XTRA_J + ".gz"                             # …the committed copy CI reads
RENAME  = os.path.join(HERE, "_rename_map.json")

# per-quarter detail fields the PAGE consumes — the rest of the ledger stays local-only
XTRA_KEEP = {"eps_b", "eps_d", "oi", "fc", "dep", "tax", "exc", "pbt", "emp", "mat",
             "assets", "eq", "borr", "cash", "invnt", "rec", "pay", "ppe", "cwip", "invst",
             "cfo", "cfi", "cff", "capex", "divp", "cf_d", "seg",
             "gnpa_pct", "nnpa_pct", "cet1", "car", "roa", "dep_amt", "adv", "int_exp",
             "aud", "qual"}

_UNSAFE = re.compile(r"[^A-Za-z0-9._-]")


def slug(sym):
    """Filename for a symbol. Mirrored by slugSym() in docs/stock.html."""
    return _UNSAFE.sub("_", sym)


def load(path, what):
    if not os.path.exists(path):
        print("WARN: %s missing — %s will be absent from every slice" % (os.path.basename(path), what))
        return {}
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=OUT, help="output dir (default docs/fin — CI path)")
    args = ap.parse_args()
    out_dir = args.out

    fund  = load(FUND_J,  "net profit")
    revop = load(REVOP_J, "revenue/margins")
    shpj  = load(SHP_J,   "shareholding")
    shph  = load(SHPH_J,  "shareholding history")
    if not fund and not revop:
        sys.exit("ABORT: neither sf_fundamentals.json nor sf_revop.json could be read")
    if fund and len(fund) < 2000:
        sys.exit("ABORT: sf_fundamentals.json has only %d symbols — refusing to publish truncated slices" % len(fund))

    shp_q = shpj.get("quarters") or []
    shp_rows = {r[0]: r for r in (shpj.get("rows") or []) if r}

    # full history, oldest first: {SYM:{QE:[prom,fii,dii,mf,ins,subDate,nsh?]}} → [[QE,…7 fields], …]
    hist_rows = {}
    for sym, qmap in shph.items():
        if sym.startswith("_") or not isinstance(qmap, dict):
            continue
        rows = [[qe] + list((qmap[qe] or [])[:7]) for qe in sorted(qmap)]
        if rows:
            hist_rows[sym] = rows

    # deep XBRL detail — prefer the local build output, fall back to the committed .gz (CI path)
    xtra = {}
    src = XTRA_J if os.path.exists(XTRA_J) else (XTRA_GZ if os.path.exists(XTRA_GZ) else None)
    if src:
        try:
            raw = open(src, "rb").read()
            if src.endswith(".gz"):
                raw = gzip.decompress(raw)
            for sym, qs in json.loads(raw).items():
                keep = {}
                for qe, cell in qs.items():
                    kc = {}
                    for b in ("s", "c"):
                        d = cell.get(b)
                        if d:
                            kd = {k: v for k, v in d.items() if k in XTRA_KEEP}
                            if kd:
                                kc[b] = kd
                    if kc:
                        keep[qe] = kc
                if keep:
                    xtra[sym] = keep
            print("deep XBRL detail: %d symbols (from %s)" % (len(xtra), os.path.basename(src)))
        except Exception as e:
            print("WARN: could not read %s (%s) — deep detail absent from every slice" % (os.path.basename(src), e))
    else:
        print("WARN: xbrl_extra.json[.gz] missing — deep detail absent from every slice")

    # Insights card — operating KPIs read from the company's own presentations (runbook §137).
    # scripts/kpi_insights/<SLUG>.json is the ledger; the slice carries it compacted, verbatim
    # values, with the per-cell provenance [attachment, page, label, as_printed] the ⓘ shows.
    kpi = {}
    kpi_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "kpi_insights")
    if os.path.isdir(kpi_dir):
        for fn in sorted(os.listdir(kpi_dir)):
            if not fn.endswith(".json") or fn.startswith("_"):
                continue
            try:
                L = json.load(open(os.path.join(kpi_dir, fn), encoding="utf-8"))
            except Exception as e:
                print("WARN: kpi ledger %s unreadable (%s) — skipped" % (fn, e))
                continue
            mets = []
            for m in L.get("metrics") or []:
                if not (m.get("y") or m.get("q")):
                    continue
                mets.append({"n": m.get("name", ""), "u": m.get("unit", ""), "k": m.get("kind", "level"),
                             "y": m.get("y") or {}, "q": m.get("q") or {}, "src": m.get("src") or {}})
            if not mets or not L.get("sym"):
                continue
            docs = {}
            for att, d in (L.get("docs") or {}).items():
                e = {"d": d.get("date"), "t": d.get("title", ""), "k": d.get("kind")}
                if d.get("url"):
                    e["u"] = d["url"]
                docs[att] = e
            kpi[L["sym"]] = {"fy": L.get("fy_end_month", 3), "u": L.get("updated"), "m": mets, "docs": docs}
        print("insights (kpi): %d symbols" % len(kpi))

    aliases = {}
    if os.path.exists(RENAME):
        try:
            aliases = json.load(open(RENAME, "r", encoding="utf-8"))
        except Exception as e:
            print("WARN: could not read _rename_map.json (%s) — renamed tickers will show no financials" % e)

    # every symbol that has data, plus each old name that resolves into one
    syms = set(fund) | set(revop) | set(shp_rows) | set(hist_rows)
    for old, new in aliases.items():
        if new in syms:
            syms.add(old)

    def resolve(src, sym):
        v = src.get(sym)
        if v:
            return v
        alias = aliases.get(sym)
        return src.get(alias) if alias else None

    if os.path.isdir(out_dir):
        shutil.rmtree(out_dir)         # drop slices for symbols that left the feeds
    os.makedirs(out_dir, exist_ok=True)

    seen, written, total = {}, 0, 0
    for sym in sorted(syms):
        sl = slug(sym)
        if sl in seen:
            sys.exit("ABORT: slug collision %r: %s and %s" % (sl, seen[sl], sym))
        seen[sl] = sym

        payload = {"sym": sym}
        f = resolve(fund, sym)
        if f:
            payload["fund"] = f
        r = resolve(revop, sym)
        if r:
            payload["revop"] = r
        row = resolve(shp_rows, sym)
        if row and row[4]:
            payload["shpQ"] = shp_q
            payload["shp"] = row[4]
        h = resolve(hist_rows, sym)
        if h:
            payload["shpH"] = h
        xt = resolve(xtra, sym)
        if xt:
            payload["x"] = xt
        kp = resolve(kpi, sym)
        if kp:
            payload["kpi"] = kp
        if len(payload) == 1:
            continue                   # nothing on file for this ticker

        blob = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
        with open(os.path.join(out_dir, sl + ".json"), "w", encoding="utf-8") as fh:
            fh.write(blob)
        written += 1
        total += len(blob.encode())

    print("fin slices: %d symbols (%d with profit, %d with revenue, %d with SHP, %d with SHP history), "
          "%.1f MB raw, avg %.1f KB"
          % (written, len(fund), len(revop), len(shp_rows), len(hist_rows), total / 1e6,
             total / max(written, 1) / 1024))


if __name__ == "__main__":
    main()
