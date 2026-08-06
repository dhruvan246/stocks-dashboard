"""FILL-2020 scoreboard: empty rev/PAT cells for the point-in-time Nifty 500, std vs consolidated.

Survivorship-free: membership is read per quarter-end from the nearest-prior snapshot in
scripts/indices_history.json (the exact json build_compressed.py embeds into stock_data.bin), so a
company counts only while it actually WAS a member, and delisted/merged constituents still count for
the quarters they were in.

Run from the repo root:   python -X utf8 scripts/fill2020_tools/audit_coverage.py [--json OUT]

WARNING: run it against a checkout synced to origin/main (`git fetch origin && git reset --hard
origin/main` in a throwaway worktree). The shared checkout drifts and other sessions leave these
files dirty -- measuring the local copy is how the first pass of this audit got wrong numbers.
"""
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
WINDOW_START = 20191231          # campaign scope: "2020 -> date"
BASELINE = {"revS": 222, "revC": 1502, "patS": 6, "patC": 19}   # measured 2026-08-05, pre-campaign
LAST_DAY = {3: 31, 6: 30, 9: 30, 12: 31}


def load(rel):
    with open(os.path.join(ROOT, rel), encoding="utf-8") as f:
        return json.load(f)


idx_hist = load("scripts/indices_history.json")
rename_map = load("scripts/_rename_map.json")
snaps = sorted(idx_hist["Nifty 500"], key=lambda s: s["effectiveDate"])

fund = {s: {int(r[0]): (r[1], r[3]) for r in rows if len(r) > 3}
        for s, rows in load("docs/sf_fundamentals.json").items()}
revop = {s: {int(q): (v[0], v[1]) for q, v in d.items()}
         for s, d in load("docs/sf_revop.json").items()}


def resolve(sym, target):
    """Chase the rename chain until the symbol is a key of `target` (old ticker -> current)."""
    cur, seen = sym, set()
    while cur not in target:
        if cur in seen or cur not in rename_map:
            return None
        seen.add(cur)
        cur = rename_map[cur]
    return cur


def members_at(qe):
    ds = "%04d-%02d-%02d" % (qe // 10000, (qe // 100) % 100, qe % 100)
    best = None
    for s in snaps:
        if s["effectiveDate"] <= ds:
            best = s
        else:
            break
    # DUMMY* are placeholder slots the index build inserts, not companies
    return [x for x in best["symbols"] if not x.upper().startswith("DUMMY")]


def quarter_ends(first=20150331, last=20260630):
    out = []
    for y in range(first // 10000, last // 10000 + 1):
        for m in (3, 6, 9, 12):
            qe = y * 10000 + m * 100 + LAST_DAY[m]
            if first <= qe <= last:
                out.append(qe)
    return out


# Companies PROVEN to have stopped filing consolidated (NSE: zero Consolidated filings from that
# quarter on). Their consolidated cells are NOT gaps -- there is nothing to fill -- so counting them
# as "missing" makes REMOVING fabricated data look like a regression. Reported separately instead.
# User instruction 2026-08-06: "stop showing 3mindia in such lists and other stocks which stopped
# posting consolidated long back".
try:
    NO_CON = load("scripts/no_con_filing.json")["stopped_filing_con"]
except Exception:
    NO_CON = {}


def files_con(sym, qe):
    start = NO_CON.get(sym)
    return not (start and qe >= start)


rows = []
for qe in quarter_ends():
    mem = members_at(qe)
    c = {"rev_std": 0, "rev_con": 0, "pat_std": 0, "pat_con": 0}
    na = {"pat_con": 0, "rev_con": 0}      # not-applicable: company does not file consolidated
    for sym in mem:
        fk = resolve(sym, fund)
        std, con = fund[fk].get(qe, (None, None)) if fk else (None, None)
        if std is None:
            c["pat_std"] += 1
        if con is None:
            if files_con(fk or sym, qe):
                c["pat_con"] += 1
            else:
                na["pat_con"] += 1
        rk = resolve(sym, revop)
        rs, rc = revop[rk].get(qe, (None, None)) if rk else (None, None)
        if rs is None:
            c["rev_std"] += 1
        if rc is None:
            if files_con(rk or sym, qe):
                c["rev_con"] += 1
            else:
                na["rev_con"] += 1
    rows.append({"qe": qe, "members": len(mem),
                 **{k + "_empty": v for k, v in c.items()},
                 **{k + "_na": v for k, v in na.items()}})

tot = {"revS": 0, "revC": 0, "patS": 0, "patC": 0}
for r in rows:
    if WINDOW_START <= r["qe"] <= 20260331:      # Jun-2026 excluded: still filing season
        tot["revS"] += r["rev_std_empty"]
        tot["revC"] += r["rev_con_empty"]
        tot["patS"] += r["pat_std_empty"]
        tot["patC"] += r["pat_con_empty"]

print("quarter    members |  rev std  rev con |  pat std  pat con")
for r in rows:
    print("%8d %8d | %8d %8d | %8d %8d" % (r["qe"], r["members"], r["rev_std_empty"],
                                           r["rev_con_empty"], r["pat_std_empty"], r["pat_con_empty"]))

start, now = sum(BASELINE.values()), sum(tot.values())
print("\nCAMPAIGN WINDOW %d..20260331 (Jun-2026 excluded, still filing)" % WINDOW_START)
print("%-7s %7s %7s %7s" % ("field", "start", "now", "closed"))
for k in ("revS", "revC", "patS", "patC"):
    print("%-7s %7d %7d %7d" % (k, BASELINE[k], tot[k], BASELINE[k] - tot[k]))
print("%-7s %7d %7d %7d  (%.0f%% closed)" % ("TOTAL", start, now, start - now,
                                             100.0 * (start - now) / start))
na_p = sum(r["pat_con_na"] for r in rows if WINDOW_START <= r["qe"] <= 20260331)
na_r = sum(r["rev_con_na"] for r in rows if WINDOW_START <= r["qe"] <= 20260331)
if na_p or na_r:
    print("\nNOT APPLICABLE (company stopped filing consolidated - nothing to fill):"
          "  patC %d, revC %d" % (na_p, na_r))
    print("  These are excluded from the gap counts above. Ledger: scripts/no_con_filing.json")

if "--json" in sys.argv:
    out = sys.argv[sys.argv.index("--json") + 1]
    with open(out, "w", encoding="utf-8") as f:
        json.dump({"rows": rows, "window_totals": tot, "baseline": BASELINE}, f, indent=1)
    print("wrote", out)
