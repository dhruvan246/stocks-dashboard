# -*- coding: utf-8 -*-
"""Build the per-stock PRICE slices behind docs/stock.html's instant first paint.

WHY THIS EXISTS
  stock.html used to boot the whole backtest engine before it could draw a single
  company: stock_data.bin (17 MB) + the two sf-data halves (115 MB) +
  sf_fundamentals.json + shp_engine.json — ~137 MB of downloads and a ~250 MB
  JSON.parse, to show one page. Screener.in-class pages ship tens of KB. A slice
  carries EXACTLY what one stock's page reads, so the page opens in one request.

SHAPE
  The slice is written in the shape backtest-engine.js builds in memory, already
  normalised (exact x100 highs/lows, delivery % as a true percent), so the page
  installs it as a one-symbol SERIES/META/TURN and every math helper — hl52,
  rsi14, computeTech, retPctAt — runs against it unchanged. No parallel maths.

  sv    schema version (bump when the reader must change)
  sym / name / ind / alive / raw     header fields; raw = last UNADJUSTED close
  ts    START_TS the day offsets are measured from
  end   data end date (the page's "as of")
  d0/dd first day offset + per-bar gaps (delta-encoded: a long run of 1s and 3s
        gzips to almost nothing, where ascending absolute offsets do not)
  p     split/bonus-adjusted close x100, FULL history (the chart + every return)
  k     length of the tail arrays below
  h/l   exact intraday high/low x100  ] last k bars only: the deepest window any
  v     traded shares                 ] stat on the page looks back is 52 weeks,
  dv    delivery %                    ] so old bars would be dead weight
  t     turnover (₹ lacs)             ]
  mcap / mcapAt   market cap in ₹ cr and the close it was struck at (the page
        re-prices it off its own last close so a stale bake can't skew P/E)
  chips / fno     index memberships as of `end`, precomputed — this is the only
        reason the page ever pulled the 17 MB stock_data.bin

OUTPUT
  <outdir>/stk/<SLUG>.json  one per symbol (~8 KB average, 21 KB for a 30-year
  name) plus <outdir>/stk_meta.json carrying the data version.

  These are force-pushed to the sf-data Pages repo next to the split bins — every
  stock's slice changes every trading day, so committing them here would grow the
  repo by ~40 MB/day. Financials are NOT in here: they change on their own
  (results) cadence and live in docs/fin/ — see build_stock_fin.py.

  SLUG = symbol with anything outside [A-Za-z0-9._-] replaced by "_" (23 symbols
  carry & or +: M&M, L&T, SRERAYHY+H …). docs/stock.html applies the same rule.

Run: python scripts/build_stock_slices.py [--out DIR] [--only SYM,SYM]
"""
import argparse, gzip, json, os, re, shutil, sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
DOCS = os.path.join(ROOT, "docs")
# In CI the in-repo bin is the one update_sf_data.py just refreshed. Locally that copy
# can be weeks old, so SF_BIN=<path> lets you point at a freshly downloaded asset.
SF_BIN   = os.environ.get("SF_BIN") or os.path.join(DOCS, "sf_stock_data.bin")
CORE_BIN = os.path.join(DOCS, "stock_data.bin")

TAIL = 400          # bars carrying h/l/v/dv/t — 52 weeks (~250 sessions) is the deepest window
SCHEMA = 1

_UNSAFE = re.compile(r"[^A-Za-z0-9._-]")


def slug(sym):
    """Filename for a symbol. Mirrored by slugSym() in docs/stock.html."""
    return _UNSAFE.sub("_", sym)


def members_as_of(snaps, date_str):
    """Point-in-time membership: the newest snapshot effective on or before date_str
    (falling back to the oldest, as backtest-engine.js's lastSnap does)."""
    best = None
    for s in snaps or ():
        if s.get("effectiveDate", "") <= date_str and (best is None or s["effectiveDate"] > best["effectiveDate"]):
            best = s
    if best is None and snaps:
        best = snaps[0]
    return set(best.get("symbols") or ()) if best else set()


def build_slice(sym, o, m, end, ts, chips, fno, core):
    n = len(o["d"])
    k = min(TAIL, n)
    # Day offsets exactly as backtest-engine.js computes them (floor((utc - startTs)/DAY)),
    # then delta-encoded — a run of 1s and 3s gzips to nothing, absolute offsets do not.
    offs = [(_days(y) * 86400 - ts) // 86400 for y in o["d"]]
    p = [int(round(c * 100)) for c in o["c"]]

    out = {
        "sv": SCHEMA, "sym": sym,
        "name": (m.get("name") or sym), "ind": (m.get("ind") or m.get("industry") or ""),
        "alive": 1 if m.get("alive") else 0, "raw": m.get("raw"),
        "ts": ts, "end": end,
        "d0": offs[0], "dd": [offs[i] - offs[i - 1] for i in range(1, n)],
        "p": p, "k": k,
    }

    # --- tail arrays: passed through VERBATIM ------------------------------------------------
    # The client applies the very same transform loadSF() does, so a slice cannot drift from the
    # whole-market path. Re-deriving values here instead cost real accuracy the first time round:
    # rounding turnover to an int put a 40% error on penny stocks (TVVISION 0.957 -> 0.571 ₹ lacs),
    # and converting the per-mil high/low into x100 paise moved 52-week lows on sub-₹10 names.
    if o.get("h") and o.get("l"):
        out["hl"] = 1                                              # exact intraday high/low…
        out["h"] = [int(round(x * 100)) for x in o["h"][-k:]]      # …stored x100, as loadSF does
        out["l"] = [int(round(x * 100)) for x in o["l"][-k:]]
    elif o.get("hb") and o.get("lb"):
        out["hb"] = o["hb"][-k:]                                   # legacy per-mil offsets from close
        out["lb"] = o["lb"][-k:]
    if o.get("v"):
        out["v"] = o["v"][-k:]
    if o.get("dv"):
        out["dv"] = o["dv"][-k:]                                   # x10 unless hl=1 — client divides
    if o.get("t"):
        out["t"] = [x or 0 for x in o["t"][-k:]]

    cm = core.get(sym + ".NS") or core.get(sym)
    if cm and cm.get("mcap"):
        out["mcap"] = round(cm["mcap"], 2)
        if cm.get("latest"):
            out["mcapAt"] = cm["latest"]
    if chips:
        out["chips"] = chips
    if fno:
        out["fno"] = 1
    return out


_EPOCH_CACHE = {}


def _days(yyyymmdd):
    """Days since the Unix epoch for a yyyymmdd int (cached — 5.9 M lookups)."""
    v = _EPOCH_CACHE.get(yyyymmdd)
    if v is None:
        import datetime
        y = int(yyyymmdd)
        v = (datetime.date(y // 10000, (y // 100) % 100, y % 100) - datetime.date(1970, 1, 1)).days
        _EPOCH_CACHE[yyyymmdd] = v
    return v


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(HERE, "_sfsplit"),
                    help="directory to write stk/ into (default: the sf-data push staging dir)")
    ap.add_argument("--only", default="", help="comma-separated symbols (debugging)")
    args = ap.parse_args()

    if not os.path.exists(SF_BIN):
        sys.exit("ABORT: %s missing — run scripts/update_sf_data.py first" % SF_BIN)

    print("reading %s …" % os.path.basename(SF_BIN), flush=True)
    SF = json.loads(gzip.decompress(open(SF_BIN, "rb").read()))
    data, meta, end = SF["data"], SF.get("meta", {}), SF.get("end") or ""
    if len(data) < 3000:
        sys.exit("ABORT: sf payload has only %d symbols — refusing to publish truncated slices" % len(data))

    # stock_data.bin supplies the time base, index/F&O membership and market cap. Reading
    # it here is what lets the PAGE stop reading it: 17 MB of downloads become 3 fields.
    print("reading stock_data.bin …", flush=True)
    CORE = json.loads(gzip.decompress(open(CORE_BIN, "rb").read()))
    ts = CORE["startTs"]
    core_meta = CORE.get("meta", {})
    idx_hist = CORE.get("indicesHistory", {})
    fno_syms = members_as_of(CORE.get("fnoHistory", []), end)
    idx_mem = {name: members_as_of(snaps, end) for name, snaps in idx_hist.items()}
    print("  ts=%d  end=%s  indices=%d  fno=%d" % (ts, end, len(idx_mem), len(fno_syms)), flush=True)

    outdir = os.path.join(args.out, "stk")
    if os.path.isdir(outdir):
        shutil.rmtree(outdir)          # drop slices for symbols that vanished from the payload
    os.makedirs(outdir, exist_ok=True)

    only = {s.strip().upper() for s in args.only.split(",") if s.strip()}
    syms = sorted(s for s in data if not only or s in only)

    seen_slug, total, biggest = {}, 0, ("", 0)
    for i, sym in enumerate(syms):
        sl = slug(sym)
        if sl in seen_slug:
            sys.exit("ABORT: slug collision %r: %s and %s" % (sl, seen_slug[sl], sym))
        seen_slug[sl] = sym
        chips = sorted(name for name, mem in idx_mem.items() if sym in mem)
        payload = build_slice(sym, data[sym], meta.get(sym, {}), end, ts,
                              chips, sym in fno_syms, core_meta)
        blob = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
        with open(os.path.join(outdir, sl + ".json"), "w", encoding="utf-8") as fh:
            fh.write(blob)
        total += len(blob.encode())
        if len(blob) > biggest[1]:
            biggest = (sym, len(blob))
        if i and i % 1000 == 0:
            print("  %d/%d" % (i, len(syms)), flush=True)

    with open(os.path.join(args.out, "stk_meta.json"), "w", encoding="utf-8") as fh:
        json.dump({"end": end, "n": len(syms), "sv": SCHEMA}, fh, separators=(",", ":"))

    print("stk slices: %d symbols, %.1f MB raw (avg %.1f KB), biggest %s %.0f KB, end=%s"
          % (len(syms), total / 1e6, total / len(syms) / 1024, biggest[0], biggest[1] / 1024, end))


if __name__ == "__main__":
    main()
