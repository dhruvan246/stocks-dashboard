# -*- coding: utf-8 -*-
"""Land a Claude reader's annual BS/CF reads into scripts/annual_bscf.json, GATE-ENFORCED.

The no-key vision loop is: fetch_annual_bscf.py --prep renders each vision-needed filer's
balance-sheet + cash-flow pages and writes a manifest -> a Claude routine READS the PNGs with
native vision (no API key, the repo's proven pattern) and writes back the numbers per manifest
entry -> this script lands them.

The holdout gate is enforced HERE, not on trust: each symbol's manifest carries a 'validate'
entry for a year we DO hold from XBRL, with that year's XBRL 'key' (Total Assets + PP&E). A
symbol's FILL years are landed ONLY if the reader's numbers for the validate year match the key
to <=1%. A symbol whose read is wrong (or whose reader hallucinated) fails and lands NOTHING.

Input (arg1): the reader's output — a JSON list of the manifest entries with the read fields added:
  [{"sym","fy","role":"validate"|"fill","basis":"c|s","key":{...}(validate only),
    "assets","sc","oeq","borr","blt","bst","ppe","cwip","gw","intg","invst","rec","pay","invnt",
    "cfo","cfi","cff","capex","cf_tax"}]   — ₹ crore, null a field the statement doesn't print.
Run: python -X utf8 scripts/merge_annual_bscf.py <reader_output.json>
"""
import json, os, sys
HERE = os.path.dirname(os.path.abspath(__file__))
LEDGER = os.path.join(HERE, "annual_bscf.json")
FIELDS = {"assets", "sc", "oeq", "borr", "blt", "bst", "ppe", "cwip", "gw", "intg", "invst",
          "rec", "pay", "invnt", "cfo", "cfi", "cff", "capex", "cf_tax"}

def gate_ok(read, key):
    for f in ("assets", "ppe"):
        r, k = read.get(f), (key or {}).get(f)
        if r is None or not k or abs(r - k) / abs(k) > 0.01:
            return False
    return True

def main():
    reads = json.load(open(sys.argv[1], encoding="utf-8"))
    bysym = {}
    for e in reads:
        bysym.setdefault(e["sym"], []).append(e)
    ledger = json.load(open(LEDGER)) if os.path.exists(LEDGER) else {}
    landed = 0; trusted = 0; rejected = []
    for sym, entries in sorted(bysym.items()):
        val = next((e for e in entries if e.get("role") == "validate"), None)
        if not val or not gate_ok(val, val.get("key")):
            rejected.append(sym); continue
        trusted += 1
        for e in entries:
            if e.get("role") != "fill": continue
            cell = {"b": e.get("basis", "c"), "m": "vision", "src": e.get("src", "")}
            cell.update({f: e[f] for f in FIELDS if e.get(f) is not None})
            if cell.get("assets") is None: continue
            ledger.setdefault(sym, {})["%d0331" % int(e["fy"])] = cell
            landed += 1
    json.dump(ledger, open(LEDGER, "w"), separators=(",", ":"), sort_keys=True)
    print("trusted %d symbols, landed %d fill-years. gate-rejected %d: %s"
          % (trusted, landed, len(rejected), rejected[:12]))

if __name__ == "__main__":
    main()
