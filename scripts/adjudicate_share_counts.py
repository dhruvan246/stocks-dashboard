# -*- coding: utf-8 -*-
"""Share-count witness for the UNRESOLVED 2016+ review events (DATA_RUNBOOK §161i).

A split/bonus multiplies a company's share count by 1/F; a crash leaves it unchanged. The company's own
results filings give the count independently of any price: shares = PAT / basic EPS (docs/fin/<SYM>.json:
`fund` = [qe, PAT std, ann std, PAT con, ann con], `x[qe][s|c].eps_b`, 2018+). Ind AS 33 restates EPS
for a split/bonus retrospectively in every result ANNOUNCED after it, so:
  pre  = quarters whose results were announced BEFORE the event (count at the old basis)
  post = quarters ending AFTER the event
Rules, fixed before the run:
  - usable quarter: |EPS| >= 0.50 (smaller EPS is rounding-dominated) and PAT != 0; same basis (s or c) both sides
  - windows: pre quarters ending within 9 months before the event, post within 9 months after; >= 2 each side
  - step = median(post shares) / median(pre shares); each side's quarters must agree within 5% (stable count)
  REAL_FILING  step within 10% of 1/F                       -> keep the adjustment; record F in corp_actions_hist
  PHANTOM_FILING step within 10% of 1.0 (count unchanged)   -> keep the raw move (phantom_crashes.json)
  otherwise stays UNRESOLVED, with the measured step.
Run: python3 scripts/adjudicate_share_counts.py [--apply]
"""
import os, sys, json, datetime, statistics
HERE = os.path.dirname(os.path.abspath(__file__)); ROOT = os.path.dirname(HERE)


def od(y): return datetime.date(y // 10000, y // 100 % 100, y % 100)


def counts(fin, basis):
    """[(qe, ann, shares)] for one basis from a docs/fin file."""
    out = []
    fund = {int(r[0]): r for r in fin.get("fund") or []}
    for q, x in (fin.get("x") or {}).items():
        q = int(q); b = (x or {}).get(basis) or {}; eps = b.get("eps_b"); r = fund.get(q)
        if not r or eps is None or abs(eps) < 0.5: continue
        pat, ann = (r[1], r[2]) if basis == "s" else (r[3], r[4] if len(r) > 4 else None)
        if not pat or not ann: continue
        out.append((q, int(ann), pat / eps))
    return sorted(out)


def judge(sym, b, F):
    p = os.path.join(ROOT, "docs", "fin", sym + ".json")
    if not os.path.exists(p): return "UNRESOLVED", {"why": "no results-filing data for this company"}
    fin = json.load(open(p)); ev = od(b); best = None
    for basis in ("s", "c"):
        C = counts(fin, basis)
        pre = [s for q, a, s in C if a < b and (ev - od(q)).days <= 275]
        post = [s for q, a, s in C if q > b and (od(q) - ev).days <= 275]
        if len(pre) < 2 or len(post) < 2: continue
        mp, mq = statistics.median(pre), statistics.median(post)
        stable = all(abs(s / mp - 1) <= 0.05 for s in pre) and all(abs(s / mq - 1) <= 0.05 for s in post)
        cand = {"basis": basis, "pre_cr": [round(s, 3) for s in pre], "post_cr": [round(s, 3) for s in post],
                "step": round(mq / mp, 4), "expected_split_step": round(1 / F, 4), "stable": stable}
        if stable: best = cand; break
        best = best or cand
    if not best: return "UNRESOLVED", {"why": "fewer than 2 usable quarters (|EPS|>=0.5) on a side within 9 months"}
    if not best["stable"]: return "UNRESOLVED", dict(best, why="share counts unstable within a side (>5%)")
    if abs(best["step"] * F - 1) <= 0.10: return "REAL_FILING", best
    if abs(best["step"] - 1) <= 0.10: return "PHANTOM_FILING", best
    return "UNRESOLVED", dict(best, why="share-count step matches neither the split (1/F) nor no-change")


def main():
    VP = os.path.join(HERE, "ca_review_verdicts.json"); VV = json.load(open(VP))
    todo = [v for v in VV["verdicts"] if v["verdict"] == "UNRESOLVED" and v["b"] >= 20160101]
    res = []
    for v in todo:
        verdict, ev = judge(v["sym"], v["b"], v["F"])
        res.append((v, verdict, ev))
        print("%-15s %-11s %d F=%.3f %s" % (verdict, v["sym"], v["b"], v["F"],
              {k: ev[k] for k in ("step", "expected_split_step", "why") if k in ev}))
    print({k: sum(1 for r in res if r[1] == k) for k in ("REAL_FILING", "PHANTOM_FILING", "UNRESOLVED")})
    if "--apply" in sys.argv:
        H = json.load(open(os.path.join(HERE, "corp_actions_hist.json")))
        PC = json.load(open(os.path.join(HERE, "phantom_crashes.json")))
        for v, verdict, ev in res:
            if verdict == "UNRESOLVED":
                v["evidence"].setdefault("missing", []).append("share-count witness: %s" % ev.get("why"))
                if "step" in ev: v["evidence"]["share_count"] = ev
                continue
            v["verdict"] = verdict; v["evidence"] = {"share_count": ev, "earlier": v["evidence"]}
            if verdict == "REAL_FILING":
                lst = H["factors"].setdefault(v["sym"], [])
                if not any(abs(int(x[0]) - v["b"]) <= 3 for x in lst): lst.append([v["b"], round(v["F"], 6)]); lst.sort()
            else:
                lst = PC.setdefault(v["sym"], [])
                if v["b"] not in lst: lst.append(v["b"]); lst.sort()
        json.dump(VV, open(VP, "w"), indent=0)
        json.dump(H, open(os.path.join(HERE, "corp_actions_hist.json"), "w"), indent=0)
        json.dump(dict(sorted(PC.items())), open(os.path.join(HERE, "phantom_crashes.json"), "w"), indent=0)
        print("applied")


if __name__ == "__main__":
    main()
