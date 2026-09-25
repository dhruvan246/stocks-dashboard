# -*- coding: utf-8 -*-
"""Re-read LANDED annual BS/CF cells from their OWN filing with the current text reader, and emit
merge entries (runbook §168). Re-run it after any reader improvement: every landed cell records its
source attachment (src), so nothing needs the BSE announcements API — only the PDF host.

For each ledger cell (scripts/annual_bscf.json) in scope:
  1. fetch its src PDF (cache dir, else www.bseindia.com/xml-data, >= BSE_MIN_GAP s apart);
  2. locate() the statement on the cell's OWN basis and text_read() it;
  3. ANCHOR: the re-read Total Assets must equal the stored one within 0.5% at one unit scale — the same
     statement, the same unit — or the cell is skipped (a scan / a different page);
  4. emit a 'supplement' (fields the cell lacks: iuad, capex, cf_tax, cfo/cfi/cff) and, for TEXT cells, a
     'correct' entry (the old parser's cash-flow misreads — merge_annual_bscf.correct() decides, with
     evidence only).
Writes a JSON list for merge_annual_bscf.py and prints what it saw. Reads nothing else, writes nothing else.

Run: python3 -X utf8 scripts/reread_annual_bscf.py --out entries.json [--only SYM,SYM] [--syms-file f.json]
     [--cache DIR] [--no-fetch]
"""
import json, os, sys
HERE = os.path.dirname(os.path.abspath(__file__))
_args = sys.argv[1:]; sys.argv = [sys.argv[0]]
sys.path.insert(0, HERE)
import fetch_annual_bscf as F

SUPP_FIELDS = ("iuad", "capex", "cf_tax", "cfo", "cfi", "cff")

def main(args):
    out = args[args.index("--out") + 1]
    only = set(args[args.index("--only") + 1].split(",")) if "--only" in args else None
    if "--syms-file" in args:
        only = set(json.load(open(args[args.index("--syms-file") + 1])))
    cache = args[args.index("--cache") + 1] if "--cache" in args else None
    fetch = "--no-fetch" not in args
    led = json.load(open(F.LEDGER))
    o = None; entries = []; seen = {}
    for sym in sorted(led):
        if only is not None and sym not in only:
            continue
        for q, c in sorted(led[sym].items()):
            if c.get("v") or not str(c.get("src", "")).startswith("bse:"):
                seen["skip-v-or-nosrc"] = seen.get("skip-v-or-nosrc", 0) + 1; continue
            att = c["src"][4:]; pdf = None
            p = os.path.join(cache, att) if cache else None
            if p and os.path.exists(p):
                pdf = open(p, "rb").read()
            elif fetch:
                o = o or F.session(); pdf = F.download(o, att)
                if pdf and p:
                    open(p, "wb").write(pdf)
            if not pdf:
                seen["no-pdf"] = seen.get("no-pdf", 0) + 1; continue
            fy = int(q[:4]); b = c.get("b", "c")
            loc = F.locate(pdf, fy, b)
            if not loc or loc[0] != b:
                seen["no-locate-on-basis"] = seen.get("no-locate-on-basis", 0) + 1; continue
            r = F.text_read(pdf, loc[1], loc[2])
            ks = [r["_unit"]] if r.get("_unit") else [1.0, 10.0, 100.0, 1e4, 1e7]
            ks = [k for k in ks if r.get("assets") and c.get("assets") and abs(r["assets"] / k - c["assets"]) <= 0.005 * abs(c["assets"])]
            if not ks:
                seen["anchor-miss"] = seen.get("anchor-miss", 0) + 1; continue
            k = ks[0]; cfk = r.get("_cf_unit") or k
            s = F.scale_text(r, k)
            net = round(r["_cf_net"] / cfk, 2) if r.get("_cf_net") is not None else None
            fx = round(r["_cf_fx"] / cfk, 2) if r.get("_cf_fx") is not None else None
            base = {"sym": sym, "fy": fy, "basis": b, "src": c["src"], "assets": s.get("assets"),
                    "cf_net": net, "cf_fx": fx, "cf_ok": r.get("_cf_ok"), "cf_layout": r.get("_cf_layout")}
            sup = dict(base, role="supplement", **{f: s.get(f) for f in SUPP_FIELDS})
            entries.append(sup)
            if c.get("m") == "text":
                entries.append(dict(base, role="correct", **{f: s.get(f) for f in ("cfo", "cfi", "cff", "cf_tax")}))
            seen["read"] = seen.get("read", 0) + 1
    json.dump(entries, open(out, "w"), indent=0)
    print("re-read:", seen, "-> %d entries in %s" % (len(entries), out))

if __name__ == "__main__":
    main(_args)
