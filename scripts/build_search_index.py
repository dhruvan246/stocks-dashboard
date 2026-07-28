# -*- coding: utf-8 -*-
"""Build docs/search_index.json — the tiny symbol/name index behind the site-wide
🔍 search box (theme.js injects it into every page's header).

Universe = EXACTLY what docs/stock.html can render, i.e. every symbol in the
survivorship-free payload's meta (NSE + BSE-only + delisted, ~5k names). Anything
outside it would 404 on the stock page, so it must never be offered.

Why stream instead of json.loads: sf_stock_data.bin decompresses to ~700 MB of
price arrays. We only want the small "meta" object, so the gzip stream is read
incrementally and brace-matched — seconds and a few MB of RAM, in CI or locally.

mcap (for ranking — big names first) comes from docs/dash_slim.bin, whose meta is
keyed SYMBOL.NS and carries mcap for the live NSE universe.

Output (compact, ~250 KB raw / ~80 KB gzipped by Pages):
  {"v": "<sf end date>", "ind": [...industry names...],
   "s": [[symbol, name, alive(0|1), mcap_cr(int), ind_idx], ...]}   # mcap desc

Run: python scripts/build_search_index.py
"""
import gzip, json, os, sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
# In CI the in-repo bin is the one update_sf_data.py just refreshed from the release
# asset. Locally that copy can be weeks old, so SF_BIN=<path> lets you point this at a
# freshly downloaded asset without disturbing the working file other jobs may be using.
SF   = os.environ.get("SF_BIN") or os.path.join(ROOT, "docs", "sf_stock_data.bin")
SLIM = os.path.join(ROOT, "docs", "dash_slim.bin")
OUT  = os.path.join(ROOT, "docs", "search_index.json")

CHUNK = 1 << 22   # 4 MB of decompressed text per read


def _scan_top_level(path, wanted):
    """Return {key: parsed_value} for the given TOP-LEVEL keys of a gzipped JSON
    object, reading the stream once and only materialising those values."""
    found, want = {}, set(wanted)
    buf, capturing, key = "", None, None
    depth, in_str, esc = 0, False, False
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        while want:
            block = fh.read(CHUNK)
            if not block:
                break
            if capturing is None:
                # Look for `"key":` at the top level. The needle is unique enough
                # here (meta/end are not nested key names in this payload).
                buf += block
                hit = None
                for k in want:
                    i = buf.find('"%s":' % k)
                    if i >= 0 and (hit is None or i < hit[1]):
                        hit = (k, i)
                if hit is None:
                    buf = buf[-64:]          # keep a tail so a split needle still matches
                    continue
                key = hit[0]
                block = buf[hit[1] + len(key) + 3:]
                buf, capturing = "", []
                depth, in_str, esc = 0, False, False
            # capture one JSON value (object / string / number)
            i, n = 0, len(block)
            while i < n:
                c = block[i]
                if in_str:
                    if esc:            esc = False
                    elif c == "\\":    esc = True
                    elif c == '"':     in_str = False
                elif c == '"':         in_str = True
                elif c in "{[":        depth += 1
                elif c in "}]":        depth -= 1
                elif c == "," and depth == 0:
                    capturing.append(block[:i])
                    found[key] = json.loads("".join(capturing).strip())
                    want.discard(key)
                    buf, capturing = block[i + 1:], None
                    break
                i += 1
                if depth == 0 and c in "}]":
                    capturing.append(block[:i])
                    found[key] = json.loads("".join(capturing).strip())
                    want.discard(key)
                    buf, capturing = block[i:], None
                    break
            else:
                capturing.append(block)
    return found


def main():
    if not os.path.exists(SF):
        sys.exit("ABORT: %s missing — run the SF refresh first" % SF)
    got = _scan_top_level(SF, ("meta", "end"))
    meta = got.get("meta") or {}
    end  = got.get("end") or ""
    if len(meta) < 3000:
        sys.exit("ABORT: sf meta has only %d symbols — refusing to publish a truncated index" % len(meta))

    mcap = {}
    if os.path.exists(SLIM):
        try:
            sm = json.loads(gzip.decompress(open(SLIM, "rb").read())).get("meta", {})
            for tkr, m in sm.items():
                s = m.get("symbol") or tkr.split(".")[0]
                if m.get("mcap"):
                    mcap[s] = m["mcap"]
        except Exception as e:
            print("WARN: could not read mcap from dash_slim.bin: %s" % e)

    inds, ind_ix, rows = [], {}, []
    for sym, m in meta.items():
        name = (m.get("name") or sym).strip()
        ind  = (m.get("ind") or m.get("industry") or "").strip()
        if ind not in ind_ix:
            ind_ix[ind] = len(inds); inds.append(ind)
        rows.append([sym, name, 1 if m.get("alive") else 0,
                     int(round(mcap.get(sym, 0))), ind_ix[ind]])
    # Rank once, here: live before delisted, named before nameless (the payload has
    # no company name for long-dead or obscure listings — those are noise in a
    # suggestion list), then biggest first. The client keeps this order within each
    # match tier, so no sorting happens per keystroke.
    rows.sort(key=lambda r: (-r[2], 0 if r[1] != r[0] else 1, -r[3], r[0]))

    payload = {"v": end, "ind": inds, "s": rows}
    blob = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    open(OUT, "w", encoding="utf-8").write(blob)
    print("search_index.json: %d symbols (%d live, %d with mcap), %.0f KB raw, end=%s"
          % (len(rows), sum(r[2] for r in rows), len(mcap), len(blob.encode()) / 1024, end))


if __name__ == "__main__":
    main()
