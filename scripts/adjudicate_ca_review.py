# -*- coding: utf-8 -*-
"""Adjudicate scripts/ca_review_evidence.json (DATA_RUNBOOK §161g). Rules fixed BEFORE the evidence
was read; every verdict names the records it rests on. Prints a summary and writes
scripts/ca_review_verdicts.json. `--apply` also writes the resulting ledger entries.

  F   = factor baked into the published bins at the boundary (a -> b)
  raw = the exchange's own close-to-close move (bhavcopy when fetched, else the audit's value)

Verdicts
  REAL            an EXCHANGE record (NSE either board, or BSE) of a split/bonus/consolidation whose
                  factor (same parser as build_corp_actions) matches F within 3% (same-day records
                  multiplied). The adjustment is right; --apply records the factor in
                  corp_actions_hist.json so it is officially backed from now on.
  WRONG_FACTOR    an exchange split/bonus record exists but its factor differs from F by > 3%.
  OTHER_ACTION    an exchange record of a demerger / scheme / rights issue and none of a split/bonus —
                  the move is real value leaving (or a rights dilution), not a split.
  PHANTOM         NO split/bonus/consolidation/demerger/scheme/rights on ANY reachable exchange feed,
                  with the NSE feed proven to know the company (covered: >=1 row of any purpose), AND
                  Yahoo's split-event record covering both dates holds no split in the window, AND the
                  bhavcopy raw move (when fetched) confirms the move. From 2006 (dense NSE feed) that
                  suffices; BEFORE 2006 the NSE feed is too sparse to prove absence, so BSE must also
                  have been reached and show no split/bonus. --apply adds it to phantom_crashes.json
                  (self_heal restores the raw move on the next run).
  YAHOO_SPLIT     no exchange record, but Yahoo records a split matching F within 3% — conflict
                  between a third party and the exchange; left as is, listed.
  UNRESOLVED      a source needed for any verdict above was unreachable or did not cover the dates.
"""
import os, sys, json, collections
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
_argv = sys.argv; sys.argv = _argv[:1]
import build_corp_actions as BCA
sys.argv = _argv

RIGHTS_KW = ("rights",)
TOL = 0.03


def match(f, F): return f is not None and F and abs(f / F - 1) <= TOL


def day_products(rows):   # multiply same-ex-date split/bonus factors (a combined bonus + split)
    by = collections.defaultdict(lambda: 1.0)
    for r in rows: by[r["ex"]] *= r["factor"]
    return dict(by)


def yahoo_splits(yev, a, b):
    out = []
    for k in ("ns", "bo"):
        y = yev.get(k) or {}
        for s in y.get("splits") or []:
            if a - 15 <= s["date"] <= b + 15 and s.get("num") and s.get("den"):
                out.append({"src": k, "date": s["date"], "factor": s["den"] / s["num"]})
    return out


def yahoo_covered(yev):
    return any((yev.get(k) or {}).get("status") == "ok" and (yev.get(k) or {}).get("covered") for k in ("ns", "bo"))


def verdict(e):
    F = e["F"]; a, b = e["a"], e["b"]
    nse = e.get("nse") or {}
    rows = nse.get("rows") or []
    split_rows = [r for r in rows if r.get("factor")]
    other_rows = [r for r in rows if not r.get("factor") and (r.get("demerger") or any(k in (r.get("subject") or "").lower() for k in RIGHTS_KW))]
    bse = e.get("bse") or {}
    bse_ok = bse.get("status") == "ok"
    bse_split, bse_other = [], []
    for r in bse.get("rows") or []:
        p = r.get("purpose") or ""
        f, _ = BCA.official_factor(p)
        if f: bse_split.append({"ex": r.get("ex"), "factor": f, "subject": p})
        elif BCA.is_demerger(p) or "rights" in p.lower(): bse_other.append({"ex": r.get("ex"), "subject": p})
    ys = yahoo_splits(e.get("yahoo_events") or {}, a, b)
    raw = e.get("raw_ratio_bhav") or e.get("raw")
    raw_ok = e.get("raw_ratio_bhav") is None or abs(e["raw_ratio_bhav"] / e["raw"] - 1) <= 0.05

    nse_prod = day_products(split_rows)
    if any(match(f, F) for f in nse_prod.values()) or any(match(r["factor"], F) for r in bse_split):
        return "REAL", {"nse": split_rows, "bse": bse_split}
    if split_rows or bse_split:
        return "WRONG_FACTOR", {"nse": split_rows, "bse": bse_split}
    if other_rows or bse_other:
        return "OTHER_ACTION", {"nse": other_rows, "bse": bse_other}
    if any(match(y["factor"], F) for y in ys):
        return "YAHOO_SPLIT", {"yahoo": ys}
    need = []
    if not nse.get("covered"): need.append("NSE feed has no rows for this company")
    if not yahoo_covered(e.get("yahoo_events") or {}): need.append("Yahoo does not cover both dates")
    if ys: need.append("Yahoo lists a non-matching split %s" % ys)
    if not raw_ok: need.append("bhavcopy raw move %s disagrees with audit raw %s" % (e.get("raw_ratio_bhav"), e["raw"]))
    if b < 20060101:
        if not bse_ok: need.append("pre-2006: BSE record needed and not reached (%s)" % bse.get("status"))
    if need: return "UNRESOLVED", {"missing": need}
    return "PHANTOM", {"nse_status": nse.get("status"), "yahoo": {k: (e["yahoo_events"].get(k) or {}).get("status") for k in ("ns", "bo")},
                       "bse": bse.get("status"), "raw_bhav": e.get("raw_ratio_bhav")}


def main():
    E = json.load(open(os.path.join(HERE, "ca_review_evidence.json")))["events"]
    res = []
    for e in E:
        v, why = verdict(e)
        res.append({"sym": e["sym"], "a": e["a"], "b": e["b"], "F": e["F"], "raw": e["raw"],
                    "raw_bhav": e.get("raw_ratio_bhav"), "verdict": v, "evidence": why})
    json.dump({"rules": __doc__, "verdicts": res}, open(os.path.join(HERE, "ca_review_verdicts.json"), "w"), indent=0)
    era = lambda y: "pre2006" if y < 20060101 else "2006-15" if y < 20160101 else "2016+"
    c = collections.Counter((r["verdict"], era(r["b"])) for r in res)
    for k, v in sorted(c.items()): print("%-14s %-8s %d" % (k[0], k[1], v))
    if "--apply" in sys.argv:
        pc_p = os.path.join(HERE, "phantom_crashes.json"); pc = json.load(open(pc_p))
        n_pc = 0
        for r in res:
            if r["verdict"] == "PHANTOM":
                v = pc.setdefault(r["sym"], [])
                if r["b"] not in v: v.append(r["b"]); v.sort(); n_pc += 1
        json.dump(dict(sorted(pc.items())), open(pc_p, "w"), indent=0)
        h_p = os.path.join(HERE, "corp_actions_hist.json"); H = json.load(open(h_p)); n_h = 0
        for r in res:
            if r["verdict"] != "REAL": continue
            recs = r["evidence"]["nse"] or r["evidence"]["bse"]
            best = min(recs, key=lambda x: abs((x["factor"]) / r["F"] - 1))
            ex = int(best["ex"]) if str(best["ex"]).isdigit() else r["b"]
            lst = H.setdefault("factors", {}).setdefault(r["sym"], [])
            if not any(int(x[0]) == ex for x in lst):
                lst.append([ex, round(best["factor"], 6)]); lst.sort(); n_h += 1
        json.dump(H, open(h_p, "w"))
        print("applied: %d phantom_crashes entries, %d corp_actions_hist factors" % (n_pc, n_h))


if __name__ == "__main__":
    main()
