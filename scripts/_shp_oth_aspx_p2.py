# -*- coding: utf-8 -*-
"""SW-2 phase 2 — the PRE-XBRL selectable era (Dec-2013..Mar-2016 quarters, the ones SW-1's
announcement-stream dating made PIT-visible): fetch BSE's own Clause-35 aspx table for every
shp_history cell there and census the institutions block, so the Any-Other inflation class can
be adjudicated exactly like the XBRL era (see _shp_other_inst_sweep.py).

stage fetch:  aspx pages cached to _aspx_p2/cache (local), census to _shp_oth_p2_census.jsonl:
              per cell {stored, rows(parse_new), domestic_def = mf+banks+ins(+vcf), anyoth}.
stage adjudicate/apply live in _shp_other_inst_sweep-style logic run from the analysis session
(kept separate: the aspx page carries no holder names inline — classification uses the
Jun-2016 XBRL name census via same-size block continuity, plus the curated name_verdicts).
"""
import os, sys, json, time, argparse, threading
from concurrent.futures import ThreadPoolExecutor, as_completed

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.argv_backup, sys.argv = sys.argv, ["p2"]
import fetch_shp_bse_aspx as A
import fetch_shp_bse_hist as H
sys.argv = sys.argv_backup

A.CACHE_ONLY = False
DIRP = os.path.join(HERE, "_aspx_p2")
CENSUS = os.path.join(HERE, "_shp_oth_p2_census.jsonl")
QES = ["2013-12-31", "2014-03-31", "2014-06-30", "2014-09-30", "2014-12-31",
       "2015-03-31", "2015-06-30", "2015-09-30", "2015-12-31", "2016-03-31"]
_lk = threading.Lock()


def fetch_one(sym, qe, code, stored):
    qtrid = A.qtrid_of(qe)
    html, cached = A.fetch_page(DIRP, code, qtrid, "New")
    if not html:
        return {"sym": sym, "qe": qe, "code": code, "absent": "no-page"}
    try:
        parsed = A.parse_new(html)
    except Exception as e:
        return {"sym": sym, "qe": qe, "code": code, "absent": "parse-fail %r" % (e,)}
    if not parsed:
        return {"sym": sym, "qe": qe, "code": code, "absent": "no-table"}
    rows, name = parsed
    return {"sym": sym, "qe": qe, "code": code, "page_name": name, "stored": stored,
            "rows": {k: v for k, v in rows.items()}}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--threads", type=int, default=2)
    ap.add_argument("--limit", type=int, default=0)
    a = ap.parse_args()
    hist = json.load(open(os.path.join(HERE, "shp_history.json"), encoding="utf-8"))
    names = hist.get("_names", {})
    cmap, by_name = H.build_codemap(names)
    done = set()
    if os.path.exists(CENSUS):
        for line in open(CENSUS, encoding="utf-8"):
            try:
                r = json.loads(line)
                done.add((r["sym"], r["qe"]))
            except Exception:
                pass
    todo = []
    for sym, qs in hist.items():
        if sym.startswith("_") or not isinstance(qs, dict):
            continue
        code = None
        for qe in QES:
            cell = qs.get(qe)
            if not isinstance(cell, list) or (sym, qe) in done:
                continue
            if code is None:
                code = H.resolve(sym, cmap, by_name, names)
            if code is None:
                continue
            todo.append((sym, qe, code, cell))
    if a.limit:
        todo = todo[:a.limit]
    print("p2 fetch: %d cells, %d threads" % (len(todo), a.threads), flush=True)
    fh = open(CENSUS, "a", encoding="utf-8")
    n = 0
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=a.threads) as ex:
        futs = [ex.submit(fetch_one, *t) for t in todo]
        for fu in as_completed(futs):
            r = fu.result()
            with _lk:
                fh.write(json.dumps(r, default=str) + "\n")
                n += 1
                if n % 200 == 0:
                    fh.flush()
                    print("  %d/%d (%.1f/s)" % (n, len(todo), n / max(1e-9, time.time() - t0)),
                          flush=True)
    fh.close()
    print("p2 fetch done: %d" % n)


if __name__ == "__main__":
    main()
