# -*- coding: utf-8 -*-
"""
Heal meta.alive / meta.ind / meta.name on an already-built sf_stock_data.bin, without redoing
the multi-hour bhavcopy rebuild.

Root cause (found 2026-08-02): build_sf_data.py's "currently listed" lookup used to scrape a
<script id="compressedData"> blob out of docs/nse-bse-dashboard.html. That page was refactored
to load its data from dash_slim.bin instead, so the blob has not existed for a while — the
scrape's bare `except` silently left the lookup EMPTY, and every full rebuild since then wrote
alive=False + industry="Unknown" for EVERY symbol (verified live: RELIANCE/TCS/INFY included).
build_sf_data.py now reads dash_slim.bin directly (see the commit that added this script); this
script re-derives the same fields for a bin that was already built with the broken lookup,
so the fix doesn't require re-fetching 30 years of bhavcopies.

Run: python3 -X utf8 scripts/patch_sf_alive.py [bin_path] [dash_slim_path]
Defaults: docs/sf_stock_data.bin, docs/dash_slim.bin
"""
import os, sys, json, gzip

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
DOCS = os.path.join(ROOT, "docs")


def main():
    bin_path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(DOCS, "sf_stock_data.bin")
    slim_path = sys.argv[2] if len(sys.argv) > 2 else os.path.join(DOCS, "dash_slim.bin")

    slim = json.loads(gzip.decompress(open(slim_path, "rb").read()))
    cur = {}
    for k, m in (slim.get("meta") or {}).items():
        sym = m.get("symbol") or k.split(".")[0]
        cur[sym] = {"name": m.get("name"), "industry": m.get("industry") or m.get("sector")}
    if not cur:
        sys.exit("ABORT: currently-listed universe (%s meta) came out EMPTY — refusing to mark "
                  "every symbol dead." % slim_path)
    print("currently-listed universe: %d symbols (from %s)" % (len(cur), slim_path))

    big = json.loads(gzip.decompress(open(bin_path, "rb").read()))
    meta = big["meta"]
    before_alive = sum(1 for m in meta.values() if m.get("alive"))

    changed = 0
    for sym, m in meta.items():
        c = cur.get(sym)
        new_alive = sym in cur
        new_ind = (c or {}).get("industry") or "Unknown"
        new_name = (c or {}).get("name") or sym
        if m.get("alive") != new_alive or m.get("ind") != new_ind or m.get("name") != new_name:
            changed += 1
        m["alive"] = new_alive
        m["ind"] = new_ind
        m["name"] = new_name

    after_alive = sum(1 for m in meta.values() if m.get("alive"))
    print("meta entries: %d   changed: %d   alive before: %d   alive after: %d"
          % (len(meta), changed, before_alive, after_alive))
    # Sanity circuit-breaker: a healthy 30-year survivorship-free universe should have a solid
    # majority currently listed. Anything under half smells like the same empty-`cur` failure mode
    # this script exists to fix — refuse to publish rather than trade one bad state for another.
    if after_alive < len(meta) * 0.5:
        sys.exit("ABORT: only %d/%d symbols would be marked alive (<50%%) — suspiciously low, "
                  "refusing to write. Investigate before re-running." % (after_alive, len(meta)))

    blob = gzip.compress(json.dumps(big, separators=(",", ":")).encode(), 6)
    open(bin_path, "wb").write(blob)
    print("Wrote %s (%.2f MB)" % (bin_path, len(blob) / 1048576))


if __name__ == "__main__":
    main()
