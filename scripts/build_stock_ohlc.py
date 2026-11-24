# -*- coding: utf-8 -*-
"""Per-stock OPEN / HIGH / LOW history for the candle view on docs/chart.html.

WHY A SEPARATE FILE
  The stock page's slice (build_stock_slices.py → stk/<SLUG>.json) carries the close for the full history
  but high/low only for the last 400 bars and no open at all — that is all the stock page reads, and its
  whole point is to stay small. Candles need all three for every bar, so they go in a file of their own
  that only chart.html fetches, and only when the candle view is switched on.

SHAPE  <outdir>/ohlc/<SLUG>.json   (SLUG = build_stock_slices.slug — the same rule the slices use)
  sv    schema version
  sym   symbol
  end   data end date (must equal the slice's `end` — the reader checks the bar COUNT, see below)
  n     number of bars = len(slice.p). The arrays below are bar-for-bar aligned with slice.p / slice.dd,
        so the reader takes its dates from the slice and never needs them twice.
  o/h/l the bar's adjusted open / high / low MINUS its adjusted close, in the slice's own unit (price x100,
        build_stock_slices._px100). Stored as a difference because the three sit within a few percent of
        the close: small numbers gzip to a fraction of the absolute prices. open = (p[i] + o[i]) / 100.
  A symbol whose payload has no op/h/l arrays (or arrays of a different length) gets NO file — the page
  then draws that symbol as a line and says so. Nothing is estimated.

Values are passed through from sf_stock_data.bin exactly as update_sf_data.py stored them (split / bonus /
rights / demerger adjusted with the close). The only check made here is counted and printed, never "fixed":
bars where high < max(open, close) or low > min(open, close).

Run: python3 scripts/build_stock_ohlc.py [--out DIR] [--only SYM,SYM]
     SF_BIN=<path> reads a tape other than docs/sf_stock_data.bin (as build_stock_slices does)
"""
import argparse, gzip, json, os, shutil, sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from build_stock_slices import SF_BIN, _px100, slug   # one slug rule, one price unit

SCHEMA = 1


def _diff(a, b):
    """a - b in x100 units, ints kept as ints (same rounding as _px100)."""
    v = round(a - b, 4)
    return int(v) if v == int(v) else v


def build(sym, o, end):
    c = o.get("c") or []
    op, h, l = o.get("op"), o.get("h"), o.get("l")
    n = len(o.get("d") or [])
    if not n or len(c) != n or not op or not h or not l or len(op) != n or len(h) != n or len(l) != n:
        return None, 0
    p = [_px100(x) for x in c]
    oo, hh, ll, bad = [], [], [], 0
    for i in range(n):
        if op[i] is None or h[i] is None or l[i] is None or c[i] is None:
            return None, 0
        po, ph, pl = _px100(op[i]), _px100(h[i]), _px100(l[i])
        if ph < max(po, p[i]) or pl > min(po, p[i]):
            bad += 1
        oo.append(_diff(po, p[i])); hh.append(_diff(ph, p[i])); ll.append(_diff(pl, p[i]))
    return {"sv": SCHEMA, "sym": sym, "end": end, "n": n, "o": oo, "h": hh, "l": ll}, bad


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(HERE, "_sfsplit"),
                    help="directory to write ohlc/ into (default: the sf-data push staging dir)")
    ap.add_argument("--only", default="", help="comma-separated symbols (debugging)")
    args = ap.parse_args()
    if not os.path.exists(SF_BIN):
        sys.exit("ABORT: %s missing — run scripts/update_sf_data.py first" % SF_BIN)
    print("reading %s …" % os.path.basename(SF_BIN), flush=True)
    SF = json.loads(gzip.decompress(open(SF_BIN, "rb").read()))
    data, end = SF["data"], SF.get("end") or ""
    only = {s.strip().upper() for s in args.only.split(",") if s.strip()}
    if not only and len(data) < 3000:
        sys.exit("ABORT: sf payload has only %d symbols — refusing to publish truncated OHLC" % len(data))
    outdir = os.path.join(args.out, "ohlc")
    if os.path.isdir(outdir):
        shutil.rmtree(outdir)
    os.makedirs(outdir, exist_ok=True)
    wrote = skipped = total = badbars = badsyms = 0
    seen = {}
    for sym in sorted(s for s in data if not only or s in only):
        sl = slug(sym)
        if sl in seen:
            sys.exit("ABORT: slug collision %r: %s and %s" % (sl, seen[sl], sym))
        seen[sl] = sym
        out, bad = build(sym, data[sym], end)
        if out is None:
            skipped += 1; continue
        if bad:
            badbars += bad; badsyms += 1
        blob = json.dumps(out, separators=(",", ":"))
        with open(os.path.join(outdir, sl + ".json"), "w", encoding="utf-8") as fh:
            fh.write(blob)
        wrote += 1; total += len(blob)
    print("ohlc: %d files (%.1f MB), %d symbols skipped (no open/high/low arrays), end %s" % (wrote, total / 1e6, skipped, end))
    print("ohlc: %d bars in %d symbols have high < max(open,close) or low > min(open,close) — passed through as stored" % (badbars, badsyms))


if __name__ == "__main__":
    main()
