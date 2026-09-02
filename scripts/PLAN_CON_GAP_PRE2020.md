# PLAN — pre-2020 CONSOLIDATED PAT gaps ("path A": no con quarter visible at the screen)  — handoff 2026-09-02

**Standing rules apply in full:** CLAUDE.md concurrency contract (own worktree, own files, ledgers not derived
files, push recipe), DATA_RUNBOOK §0 golden rules, **NO ASSUMPTIONS / NO GUESSWORK** (every number below was
measured on the LIVE page 2026-09-02 12:40 IST; re-measure before acting), profit basis = OWNERS-attributable,
never `apply_total_pat.py`. Read DATA_RUNBOOK §0, §39, §51, §53 (+53a/53b), §54b, §81 (+81d), §85 (+85d),
§96, §97b, §100 BEFORE touching anything.

## 1. Why this plan exists

The quantmac walk-back (memory `project-stocks-quantmac-walkback-2026-09-02`, record file in that session's
scratchpad) found that on a "Consolidated" strategy the engine runs FULLY on standalone for every Nifty-500
name that has no consolidated quarter visible at the screen date (`profitMetrics` path A —
`_conFreshEnough()` false → `tries=[[1,2]]`). Measured on the live page, Nifty 500 members at the screen:

| screen | members with NO con quarter visible | std-only names |
|---|---|---|
| 2009-01-30 | **500 / 501** | 500 |
| 2015-01-30 | 384 / 501 | 384 |
| 2019-01-31 | 60 / 501 | 60 |
| 2026-05-29 | 0 / 500 | 0 |

User (2026-09-02): *"fill them in a new session."* This file is that session's brief.

Context for the value of the work: quantmac ALSO falls back to standalone where its CONS is missing
("consolidated earnings are sparse before 2021-01-01"), so on these names both engines already agree on basis.
Of 1,319 only-theirs name-months 2009-23, a strict pair-match found **62** where quantmac held a consolidated
figure we lack. Filling changes the quantmac match only there; the larger value is the fidelity of OUR OWN
consolidated strategies and an honest coverage page.

## 2. THE WALL — measured before, do not re-discover it

* **§51a — quarterly consolidated results only became compulsory from FY2020 (Apr-2019).** Before that most
  companies filed standalone quarterly + consolidated ANNUAL. In `sf_fundamentals`, 285 companies' con series
  starts Jun-2019 and 142 more Sep-2019. "Do NOT plan a fetch campaign for pre-2019 con cells."
* **§53 — measured 2015Q1–2019Q4:** 2,979 gap cells across 311 companies swept against the NSE results-archive
  list API (declares basis per row, serves delisted symbols, 0 errors): **79 cells (2.7%) in 25 companies had a
  consolidated quarterly filing; 42 landed after gates.** 777 consolidated ANNUAL rows exist but an annual splits
  into quarters only when 3 of 4 siblings are known (§45) — annuals are a GATE, not a source.
* **2009–2014 has NOT been measured.** That is job 1 (memory: measure source reach FIRST).
* **con = std is fabrication unless no-subsidiary is proven** (§51c IOB: when IOB finally reports con it
  DIVERGES from std; the con-copy retraction campaign §67 — 379 cells still open — is what a wrong identity fill
  costs). "Differs from standalone" is NOT "a consolidated table exists" (§100).
* **§85 — Moneycontrol's consolidated table silently falls back to standalone.** An MC con row identical to the
  std row is UNRESOLVED, never evidence of con (memory `feedback-aggregator-identical-is-unresolved`).
* **§96a — NOT-APPLICABLE IS NOT MISSING.** The honest closure for most of these cells is an N/A ledger entry
  that the coverage matrix subtracts from the denominator, not a number.

## 3. The worklist (`scripts/con_gap_worklist_2026-09-02.json`, measured LIVE 2026-09-02)

Per symbol: `months` (Nifty-500 member-months 2009-01..2021-12 where std was visible and con was not),
`first`/`last` screen month, `conFirst` (first quarter with a con PAT, null = never), `conRows`,
`stdQtrsNoCon` (std quarters 2007-12..conFirst with no con), `qFrom`/`qTo`.

| total | |
|---|---|
| symbols | **761** |
| member-months on standalone | 45,614 |
| std quarters without a con value | 22,208 |
| symbols that never carry a con row | 103 |

First-con-year histogram: 2009:11 · 2010:3 · 2011:5 · 2012:42 · 2013:36 · 2014:25 · **2015:134** · 2016:19 ·
2017:15 · **2018:249** · 2019:38 · 2020:4 · 2021:6 · 2022-24:21 · 2025:37 · 2026:13 · never:103.

Screen-month totals (members with no con visible): 2009 ≈490 · 2010 ≈487 · 2011 ≈488 · 2012 481→439 ·
2013 437→407 · 2014 404→383 · 2015 383→264 (drop at May-2015) · 2016 264→234 · 2017 232→223 ·
2018 221→61 (drop at Aug-2018) · 2019 60→43 · 2020 ≈45 · 2021 46→36.

⚠️ The top of the list by member-months (COLPAL, CUB, GILLETTE, KARURVYSYA, PGHH, PGHL, HONAUT, ASTRAZEN,
VSTIND — 69 std quarters each, con starts **Mar-2025**) is dominated by single-entity filers. Before treating
their pre-2025 cells as gaps, READ what their 2025 con rows are (the walk-back found "con = std copies from
Mar-2025" for the HOMEFIRST/GVT&D class) — a company with no subsidiaries has no consolidated result to fill,
and the right closure is N/A with the evidence string, not a fetch.

## 4. Route, in order

0. `git worktree add --detach ~/stocks-wt/con-gap-pre2020 origin/main`. Never work in the shared checkout.
1. **MEASURE REACH FIRST — 2009-2014.** For every worklist symbol with `qFrom < 20150331`:
   * NSE results-archive list API per §53 (`fill2020_tools/nse_con_discover.py`, resumable inventory) — does ANY
     row declare Consolidated + Quarterly in 2009-2014? Record per (sym, qe). Check first whether the archive
     reaches 2009 at all (§53 measured 2015+ only) — if it does not, say so and measure the BSE announcements
     archive instead (`strCat=-1` for 2009; `Result` returns nothing that old — walk-back finding).
   * `agg_sources.mc_quarters(sym, con=True)` for the same symbols — keep ONLY rows whose PAT differs from the
     same-quarter std (identical = unresolved, §85). MC deep feed reaches ~1997 (§91f, memory
     `reference-moneycontrol-deep-history`).
   * Output: the FILLABLE set with the evidence that a consolidated quarterly document exists, and the
     N/A set with the evidence that none does. Report the reach number before filling anything.
2. **FILL only the fillable set** through the gated readers, cell by cell with provenance:
   * NSE archive detail page, GATE X + **GATE S'** (§53a: validate the document family through the std sibling you
     already hold, then read the con page); refuse `PAT == 0.00` blank templates, block on a failing EPS recon,
     reject cumulative YTD rows (§53b).
   * Where NSE has no page (pre-2015): BSE filing PDF → text layer / vision (§17b/§60/§75 traps: glyph-substituted
     text layers, triple-rendered text, side-by-side statements — `page_basis()` must answer 'both').
   * `read_con_pat_nse.py --apply` merges FILL-ONLY; every refusal with its reason into
     `scripts/con_pat_nse_reads.json`. Owners-attributable PAT (memory `project-stocks-profit-basis`).
   * Fill BOTH twins where the same page carries revenue (§39 1b twin-file side-effect: a new con PAT opens a
     revCon gap in `sf_revop` for the same quarter).
3. **CLOSE the rest as N/A with evidence** — the archive index is NEGATIVE evidence (§54b); route the cells into
   the N/A ledger the coverage matrix reads (§96a `na` arrays in `build_coverage_matrix.js`) so the page stops
   counting them as holes. A held cell asserts absence (memory `feedback-held-cell-asserts-absence`) — only
   after the 2nd reader has looked.
4. **Rebuild derived files via the ledgers** (CLAUDE.md rule 5 — never hand-edit `docs/sf_fundamentals.json`),
   commit file-scoped, push with the fetch/rebase loop, **re-verify LIVE ~20 min later** (an in-flight CI run
   may have raced you).
5. **Prove it moved a count (§39 1b).** Re-run the in-page measurement below on the LIVE page: the 2009-01 /
   2015-01 / 2019-01 path-A counts must DROP by exactly the symbols filled, and the coverage matrix must show
   `profitYoyCon` covered ↑ / N/A ↑ / holes ↓ with NO parameter regressing.

## 5. Measurement snippet (paste in the live stock-backtest.html console after a `bt_load` run)

```js
(()=>{ const members=iso=>{ const m=membersAsOf('Nifty 500', iso); return Array.isArray(m)?m:(m instanceof Set?[...m]:Object.keys(m||{})); };
 const paths=(dateInt, iso)=>{ const out={n:0,A_noConVisible_stdOnly:0,B_conCommitted_pending:0,B2_conCommitted_full:0,C_conYoyNull_stdFull:0,D_deadCon_stdOnly:0};
   for(const sym of members(iso)){ const arr=fundFor(sym); if(!arr||!arr.length) continue; out.n++;
     if(_lastVisibleQe(arr,3,4,dateInt)==null){ out.A_noConVisible_stdOnly++; continue; }
     if(!_conFreshEnough(arr,dateInt)){ out.D_deadCon_stdOnly++; continue; }
     const con=profitMetrics(sym,dateInt,'conOnly'); if(!con){ out.C_conYoyNull_stdFull++; continue; }
     if(con.ttm==null||con.accel==null) out.B_conCommitted_pending++; else out.B2_conCommitted_full++; }
   return out; };
 return { '2009-01-30':paths(20090130,'2009-01-30'), '2015-01-30':paths(20150130,'2015-01-30'), '2019-01-31':paths(20190131,'2019-01-31'), '2026-05-29':paths(20260529,'2026-05-29') }; })()
```
Baseline 2026-09-02 (engine e14/e15, same data): A = 500 / 384 / 60 / 0; C = 1 / 27 / 182 / 3; B = 0 / 36 / 29 / 32;
D = 0 / 2 / 9 / 3.

## 6. What NOT to do
* Do not copy std into con, ever. Do not derive a quarter from an annual without 3 known siblings (§45).
* Do not accept an MC con row equal to std. Do not hard-code an aggregator row (§85d).
* Do not edit `docs/sf_fundamentals.json` / `scripts/fundamentals.json` by hand; do not run `apply_total_pat.py`.
* Do not plan by cell count (22,208) — plan by the measured reach. Do not say "unfillable" (§57) — say
  "never filed, evidence: <archive index / filing text>" or "filled from <doc>".
* Do not run this in the shared checkout; do not `git add -A`.

## 7. Report back (to the user, layman terms + the log)
1. The 2009-2014 reach number (cells with a consolidated quarterly filing / cells in scope), per year.
2. Cells filled, with provenance; cells closed N/A, with evidence class; cells refused, with reason.
3. The before/after path-A counts and the coverage-matrix deltas (covered / N/A / holes) — LIVE.
4. Whether the quantmac N=20 match moved (re-run the harness recipe in the walk-back memory), and where.
