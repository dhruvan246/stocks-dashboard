# PLAN — 4dp + all §22j/§22k fixes across the ENTIRE SHP dataset (2002 → date)

Written 2026-08-16 by the session that shipped §22j/§22k. Self-contained: execute without that
conversation. Read DATA_RUNBOOK §22 (esp. 22j, 22k), §38, §39 first. NO ASSUMPTIONS — every
boundary below was measured; re-verify anything you extend.

## Goal

Every cell in `scripts/shp_history.json` (88,767 cells, 2002-12-31 → date) carries FII/DII at
**4dp computed from share counts**, plus event-driven SHPs wherever a source exists. The engines
already consume both (shp_engine.json rows sort by date; `shpAt` gates on sub-date; event rows
delta vs latest-visible — backtest-engine.js + stock-backtest.html, ENGINE_VER e5).

## Measured state (2026-08-16, all verified live)

| era | cells | 4dp now | quarterly source | event source |
|---|---|---|---|---|
| 2021-Q3 → 2026 | ~37k | 30–39%/yr (the targeted low-value cells) | NSE XBRL — DONE (a3279d68) | NSE — DONE (6e3d2dc8, 2,650 rows) |
| 2016-Q3 → 2021-Q2 | ~19k | ~0–19% | **BSE XBRL** (first file 2016-07-20) — TODO Phase A | none known |
| 2002 → 2016-Q2 | ~27k | 0% | **BSE aspx HTML** carries (shares, pct) pairs — TODO Phase B | none known |

Hard boundaries measured, do not re-litigate:
- NSE master serves ~5 years: 1,795 as-on rows for 2021-09-30, cliff to 87 for 2021-06-30.
- BSE `SHPQNewFormat/w?scripcode=` lists all quarters; rows have real `XbrlFile` from Jun-2016
  only (RELIANCE: 45 with, 61 without). ⚠️ `xbrlurl` is truthy even when there is NO file —
  gate on `XbrlFile` (§22f).
- Pre-2016 rows carry `navigateurl` → ShareholdingPattern.aspx (needs `flag_qtr=1&Flag=New`,
  curl_cffi impersonate; bare scripcd+qtrid 404s). Tables hold `(shares, pct)` per category —
  `fetch_shp_bse_aspx.py:271` currently returns only the pct. Two formats: Clause-35 and
  1997-format (parsers exist for both).

## Non-negotiable mechanics (violations broke things before)

1. **Worktree, never this checkout**: `git worktree add --detach ~/stocks-wt/shp4dp-full origin/main`.
   One writer per tree. Push via fetch→rebase→push loop; conflicts on shp files = the daily
   workflow raced you → MERGE (union by (sym, as-on/qe), newest sub wins), never clobber.
2. **Ledger, never direct writes** for new fills: write `scripts/shp_refine_4dp.json.gz`
   `{"fills": {SYM: {QE: [prom,fii,dii,mf,ins,sub,nsh|None,src]}}}`, applied by a new
   `apply_refine_ledger()` in fetch_shareholding.py with **refine-only semantics**: replace a
   stored cell ONLY when `_cell_eq(stored, new)` (tolerance CELL_TOL=0.0100001 exists — same
   value, more precision). Disagreements go to a report file for human adjudication, NEVER
   auto-applied. Apply order: after `apply_bse_hist_ledger`, BEFORE `apply_cell_fix` (cell_fix
   outranks everything).
3. **The denominator is NOT the whole-company share count** (§22j) — it includes partly-paid +
   DRs. For XBRL, reuse `parse_shp` unmodified (it infers the filer's base; HDFCBANK 44.05 →
   38.16 was the failure mode). For aspx, the printed pct column defines the base: recompute
   `shares/base` where `base = shares_of_largest_row / (its pct/100)`, and gate: every
   recomputed pct must match the printed pct within 0.01 — else write the report row, skip.
4. **Identity**: scripcode via `_bse_master_all.json` + `_shp_scripcode_override.json`; a
   scrip_id equal to the ticker is a coincidence (gate on ISIN); era names — the aspx page-name
   identity gate in fetch_shp_bse_aspx.py stays ON.
5. **Idempotency proof**: run every applier TWICE — second pass must change 0 cells (§ "a heal
   that re-applies is a nightly rewrite").
6. **§18 reset-replay**: any NEW tracked file the daily workflow will touch must ride the
   cp-to-/tmp + cp-back + git-add lists in refresh-shareholding.yml (shp_events.json already
   does, guarded with `[ -f ]`).
7. **§39 gate before "done"**: coverage-by-year table before/after; re-parse ~15 KNOWN large
   caps and require zero moving >0.02pp; verify LIVE via curl ~20 min after push (nightly CI
   can race you); prove one pre-2016 cell against its source document by hand.

## Phases

**A — BSE XBRL refine, 2016-Q3 → 2021-Q2 (~19k cells, est. 1.5–3h)**
Extend `fetch_shp_bse_hist.py` with `--refine`: target every (sym, qe) in range where the stored
cell exists but fii AND dii are 2dp-only (`round(v,2)==v`), quarter has a real `XbrlFile`.
Fetch → `FS.parse_shp` → ledger. It already imports FS and caches per-scripcode quarter lists.
Newest quarter first. THREADS≤6. Then apply + idempotency + measure.

**B — aspx 4dp, 2002 → 2016-Q2 (~27k cells; parser work + est. 2–5h fetch)**
Upgrade the two aspx table parsers to also return share counts (keep `nums[0]`, today discarded
at :271) + the base per rule 3. **Pilot first**: 20 docs stratified across years and both
formats; require recomputed-vs-printed agreement within 0.01 on 20/20 before harvesting. Reuse
the existing frontier/cache/identity plumbing. Same ledger + report + idempotency.

**C — events before 2021: source hunt, time-boxed**
No known endpoint. NSE master is exhausted; BSE SHPQNewFormat is quarterly-only. Time-box ~30
min: probe BSE corp-announcements API (category "Shareholding Pattern") for mid-quarter
attachments with parseable XBRL/HTML. If nothing lands, record "no source found <date>" in
§22k and STOP — do not fabricate reach.

**D — residue accounting**
After A+B, re-measure 4dp-by-year. Remaining 2dp cells = sources with no share counts
(trendlyne/thirdparty/wayback-MC ledger fills). List their count per year in §22j as the
documented floor. Do NOT delete or degrade them.

**E — ship**
`--apply-ledgers` in the worktree → guard_feed → rebuild feeds → commit (data files together,
message with the numbers) → push loop → dispatch pages.yml → LIVE curl verify (ITI 2022-03-31
dii=0.0077 still; one Phase-A cell; one Phase-B cell) → remove worktree → update §22j reach
table + memory (`project-stocks-shp-4dp-full` — update, don't duplicate). ENGINE_VER: not
needed for data-only (engine e5 already renders 4dp); bump ONLY if engine code changes.

## Open items inherited (do not lose)

- CCCL 2023-03-31: re-parse found a different filing (sub 2023-04-05) than cell_fix recorded
  (2023-04-21) — needs human adjudication; currently stored = re-parsed, ledger warns.
- HINDALC0 (zero, not O) in fnoHistory source — fix upstream when membership rebuilds.
- PRUDMOULI/BCG/BFUTILITIE/DATAMATICS cell_fix WARNs: ledger `was` no longer matches after 4dp;
  entries need re-adjudication or `was` refresh at 4dp.
- `fetch_shp_seam_trendlyne.py` + `fetch_shp_bse_aspx.py` own parsers stay 2dp until Phase B.

## Traps that already bit (asserted by measurement, keep them in mind)

Silent endpoint caps read as absence · curl exit 0 saves the error body (validate magic/size) ·
BSE nav table can serve ANOTHER company (500790) · 162-byte bse.json = a 302 · aggregator CON
falls back to std · quarantine gate must run BEFORE banking share counts (`nsh_gate` order) ·
two sessions writing shp_history corrupts it (stage via --hist if unsure) · a 2s quarter in a
driver loop = the endpoint fell off, not "done" (log row counts, alert on <10% of median).
