# -*- coding: utf-8 -*-
"""Split docs/sf_stock_data.bin (the full survivorship-free price file, >100MB so it can't be
committed to GitHub directly) into N by-symbol chunks, each <95MB (parts = ceil(total_gz/95MB) —
grows automatically as the dataset grows, e.g. 3 parts once true-daily bars extend back to 2002).
These are force-pushed to the dedicated dhruvan246.github.io/sf-data/ repo (same origin as the
site -> no CORS) by the daily refresh workflow. Writes _sfsplit/sf_stock_data_1.bin.._N.bin, sf_meta.json.

Run: python split_sf_data.py
"""
import json, gzip, os, hashlib

HERE = os.path.dirname(os.path.abspath(__file__)); ROOT = os.path.dirname(HERE)
SRC = os.path.join(ROOT, "docs", "sf_stock_data.bin")
OUT = os.path.join(HERE, "_sfsplit"); os.makedirs(OUT, exist_ok=True)
CAP = 95 * 1024 * 1024   # sf-data force-pushes each part as a single git blob; GitHub hard-caps at 100MB

def main():
    D = json.loads(gzip.decompress(open(SRC, "rb").read()))
    data = D["data"]
    # GUARD: never publish an UN-merged build (renamed tickers split into stub series). If the
    # rename merge didn't run, ETERNAL (ex-ZOMATO) has only ~post-rename days and ZOMATO still
    # exists as its own series. Fail loud so the workflow stops instead of pushing bad data to sf-data.
    et = data.get("ETERNAL")
    if "ZOMATO" in data or not et or len(et.get("d", [])) < 1000:
        raise SystemExit("ABORT: bin looks UN-merged (ZOMATO present or ETERNAL history short) — refusing to publish")
    syms = sorted(data.keys())
    other = {k: v for k, v in D.items() if k not in ("data", "meta")}
    meta = D.get("meta", {})

    # Probe the FULL payload's compressed size to decide how many parts are needed (contiguous
    # alphabetical chunks, same layout convention as the old fixed-half split — symbol order doesn't
    # correlate with per-symbol history length, so this balances byte-size about as well as any split).
    full_gz = gzip.compress(json.dumps({"data": data, "meta": meta}, separators=(",", ":")).encode(), 6)
    n_parts = max(2, -(-len(full_gz) // CAP))
    chunk = -(-len(syms) // n_parts)
    groups = [syms[i:i + chunk] for i in range(0, len(syms), chunk)]
    print("full payload %.1f MB compressed -> %d parts" % (len(full_gz) / 1048576, n_parts), flush=True)

    fp = hashlib.sha1()
    for part, grp in enumerate(groups, 1):
        obj = dict(other)
        obj["data"] = {s: D["data"][s] for s in grp}
        obj["meta"] = {s: meta[s] for s in grp if s in meta}
        payload = json.dumps(obj, separators=(",", ":")).encode()
        fp.update(payload)          # fingerprint the DATA, before compression (see the rev note below)
        # mtime=0: gzip stamps the CURRENT TIME into its header by default, so byte-identical data
        # compressed twice produced different files. Pinning it keeps the published file byte-stable,
        # which also lets HTTP ETags/CDNs treat an unchanged rebuild as genuinely unchanged.
        raw = gzip.compress(payload, 9, mtime=0)
        if len(raw) > CAP:
            print("  WARNING part %d is %.1f MB (>95MB cap) — increase n_parts" % (part, len(raw) / 1048576), flush=True)
        open(os.path.join(OUT, "sf_stock_data_%d.bin" % part), "wb").write(raw)
        print("part %d: %d symbols, %.1f MB" % (part, len(grp), len(raw) / 1048576), flush=True)
    # CONTENT fingerprint, not just `end`: a heal/backfill run (e.g. the delivery-% ledgers) rewrites
    # history WITHOUT advancing `end`, and the browser keys its IndexedDB copy of these 100+ MB parts
    # on this file. Keyed on `end` alone, every client that had already cached the day kept serving
    # the PRE-heal bytes forever — the 2002-2019 delivery backfill was invisible on the site until the
    # next new trading day. `rev` hashes the PAYLOAD, not the gzip container (whose header carries a
    # timestamp — hashing the compressed bytes made rev change on every rebuild, which would have made
    # every client re-download ~115 MB for identical data). So it changes exactly when the data does.
    rev = fp.hexdigest()[:10]
    json.dump({"end": D["end"], "rev": rev, "parts": len(groups)}, open(os.path.join(OUT, "sf_meta.json"), "w"))
    print("split done; end=%s rev=%s parts=%d" % (D["end"], rev, len(groups)), flush=True)

if __name__ == "__main__":
    main()
