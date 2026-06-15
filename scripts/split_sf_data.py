# -*- coding: utf-8 -*-
"""Split docs/sf_stock_data.bin (the full survivorship-free price file, >100MB so it can't be
committed to GitHub directly) into two by-symbol halves, each <100MB. These are force-pushed to
the dedicated dhruvan246.github.io/sf-data/ repo (same origin as the site -> no CORS) by the daily
refresh workflow. Writes _sfsplit/sf_stock_data_1.bin, _2.bin, sf_meta.json.

Run: python split_sf_data.py
"""
import json, gzip, os

HERE = os.path.dirname(os.path.abspath(__file__)); ROOT = os.path.dirname(HERE)
SRC = os.path.join(ROOT, "docs", "sf_stock_data.bin")
OUT = os.path.join(HERE, "_sfsplit"); os.makedirs(OUT, exist_ok=True)

def main():
    D = json.loads(gzip.decompress(open(SRC, "rb").read()))
    syms = sorted(D["data"].keys()); half = len(syms) // 2
    other = {k: v for k, v in D.items() if k not in ("data", "meta")}
    meta = D.get("meta", {})
    for part, grp in ((1, syms[:half]), (2, syms[half:])):
        obj = dict(other)
        obj["data"] = {s: D["data"][s] for s in grp}
        obj["meta"] = {s: meta[s] for s in grp if s in meta}
        raw = gzip.compress(json.dumps(obj, separators=(",", ":")).encode(), 9)
        open(os.path.join(OUT, "sf_stock_data_%d.bin" % part), "wb").write(raw)
        print("part %d: %d symbols, %.1f MB" % (part, len(grp), len(raw) / 1048576), flush=True)
    json.dump({"end": D["end"]}, open(os.path.join(OUT, "sf_meta.json"), "w"))
    print("split done; end=%s" % D["end"], flush=True)

if __name__ == "__main__":
    main()
