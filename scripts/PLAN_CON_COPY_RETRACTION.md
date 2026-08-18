# PLAN: Con-copy retraction — "a con slot holds only filing-backed values"

**User decision 2026-08-18** (verbatim intent): the std→con copy convention "was a mistake —
correct that now"; the 513-symbol PAT class and the 23.9% rev class: "solve them"; scope:
"do all" (= full option B retraction + wrong-value fixes, not just the provable-wrong subset).
Standing rules apply in full: no assumptions; every verdict per-name; nothing marked N/A
without the user's confirmation; §39 ship gate; §38 concurrency.

## The defect class

`docs/sf_fundamentals.json` / `docs/sf_revop.json` hold "consolidated" values for
symbol-quarters where **no consolidated filing existed**, because several writers copied
std→con on a pattern in OUR OWN STORE (con==std for N quarters ⇒ "no subsidiary"), never
checking the exchange record. TCIEXP is the exposing case: store carried con back to 2016;
the company's first-ever consolidated result is Q4FY23 (filed 27-May-2023), and its FY18/FY19/
FY20 annual reports state outright it had **no subsidiary, holding, JV or associate** (AR17-18
p30; AR18-19 p28 + MGT-9 §III = "N.A"; AR19-20 p36). Under s.129(3) no CFS can exist for
those years.

**Scale (measured 2026-08-18):** 513/3,950 syms with ≥8q exact con==std PAT runs (323 never
diverge; 190 diverge later). Rev: 12,459/52,145 both-basis quarters equal (23.9%); 494 syms
with ≥8q rev runs. ≥8 is the SCREEN threshold, not the class boundary — final classification
is per-cell against the floor, threshold-free.

## Writers (inventory 2026-08-18)

| writer | status |
|---|---|
| `update_fundamentals.py` no-sub autofill (cron, born e1bbc49ff 2026-06-21) | **KILLED**, pushed 096474c9b — tombstone comment at the site |
| `nosub_pat_fills.json` — 163 entries / 28 syms, applied 2026-08-06 "no-sub-identity-bulk" | ledger of an earlier campaign session's bulk copies — **retract** |
| `nosub_rev_pre2020_fills.json` — 1,205 / 122 | same, revenue side — **retract** |
| `apply_nosub_constd.py`, `apply_nosub_constd2.py`, `_nosub_con_lag_apply.py`, `nosub_rev_derive.py`, `nosub_insurer_rev.py` | one-shot scripts, in NO workflow (verified) — tombstone so nobody re-runs them |
| deep-history vendor imports (pre-ledger, e.g. TCIEXP 2016-18) | untracked — classified by floor, not by writer |
| `verify_fills_live.py:36-41` counts three nosub ledgers as legitimate backing | strip those registry rows WITH the retraction |

## The two gates (user: "these 2 gates are awesome")

**Gate 1 — exchange first-con floor.** Per symbol, the first Consolidated row in the NSE
quarterly stream (BSE titles as fallback reader): `first_con_qe` + `first_con_filed`, plus
the stream's `reach_from`/`reach_to`. Rule: **no con value may exist for a quarter earlier
than the first con filing's earliest covered period**, and any con cell's annCon must be a
real filing date. Comparative nuance: a first con filing can carry (or explicitly exclude —
TCIEXP's note) comparative quarters; the floor is the earliest period the filing actually
covers, not blindly its own quarter.
Harvest: `scratchpad/harvest_con_floor.py` → `con_floor_harvest.jsonl` over the 842-symbol
union (513 PAT ∪ 494 rev ∪ 129 ledger). Restartable. Promote to
`scripts/con_basis_floor.json` after validation; then wire as a WRITE GATE in
update_fundamentals.py and the fill tooling.
Known traps honoured: NSE list reach ~2016+ (cells older than reach_from are NOT adjudicable
by this reader — need BSE/AR); silent truncation (stale reach_to); `&` symbols URL-encoded;
a symbol with ZERO con rows ever = candidate genuine no-sub (N/A route), never "retract to
empty and forget".

**Gate 2 — MoneyControl corroboration** (user-proposed after seeing MC show TCIEXP con only
from Mar-23, agreeing exactly with the exchange floor). MC consolidated-quarterly earliest
visible quarter as an independent reader for the floor. Reality checks: MC 403'd scripted
fetches today and rate-limits aggressively (one name burned a session on 2026-08-16);
aggregators are one vendor upstream. Use as READER #2 on names where NSE is inconclusive
(pre-reach cells, truncated lists, zero-row names) via the browser route, throttled; never
as the sole basis for a negative.

## Classification (per cell, after floors land)

- `qe < floor coverage start` → **fabricated** → retract con (PAT+rev+op+annCon) → the hole
  is then adjudicated: genuine no-sub era ⇒ N/A ledger entry (per-name evidence, USER
  APPROVES the sheet before any N/A is written); post-mandate era with real filings ⇒
  normal fill queue.
- `qe ≥ floor` and con==std exactly → **verify, don't assume**: TCIEXP's 5 post-sub quarters
  (20230331-20240331) proved GENUINE — AOC-I: subsidiary turnover 0, PAT (0.03). Exact
  equality after the floor is possible with a dormant sub. Check against the filing before
  touching anything.
- `qe ≥ floor` and con missing → ordinary fill work (existing campaign).

## Retraction mechanics — a retracted cell COMES BACK unless every ledger agrees

Order per batch: (1) strip the covering entries from nosub_* ledgers + the
verify_fills_live registry rows; (2) write the retraction to BOTH twins (docs + scripts) for
PAT and rev; (3) record every retracted cell in `scripts/con_copy_retractions.json`
(sym|qe → {was, floor evidence, retracted_on}) so appliers/heals can refuse to rewrite;
(4) rebuild, diff on the SHARED date grid only, expect pure-con params to REGRESS honestly —
that regression is the point, not a bug; (5) §39 + live verify.

## Status

- [x] Writer killed + pushed (096474c9b)
- [ ] Gate-1 harvest running (task #17)
- [ ] Gate-2 MC reader design (throttled, browser route)
- [ ] nosub machinery tombstones + registry strip (task #18)
- [ ] Classification sheets → user sign-off (retraction + N/A)
- [ ] Batched retraction + rebuild + live verify
- [ ] Re-scan at threshold-free floor classification (the ≥8 screen missed short runs)
