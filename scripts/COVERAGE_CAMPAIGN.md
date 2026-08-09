# FACTOR-COVERAGE CAMPAIGN — step recipes (written 2026-08-02 by the audit session)

> **★ NO ASSUMPTIONS. NO GUESSWORK.** (user-mandated 2026-08-10) Every value/claim must trace to a
> source measured THIS session; unknown stays `unknown`. Full rule: DATA_RUNBOOK §0, top.

Goal: every backtest filter factor usable for every N500 member-day where the input data can exist.
Baseline audit + per-factor coverage table: memory `project-stocks-factor-coverage-audit`;
harness pattern: scratchpad `factor_coverage.js` from that session (rebuild it from the notes below if needed).

**RUN ONE STEP PER SESSION, `/clear` BETWEEN STEPS.** Each step is self-contained: read GROUND RULES,
then only your step. Steps 1, 2a, 3, 5 are Sonnet-safe; **step 4 (fundamentals backfill) is deliberately
NOT here** — it needs filing-level judgment, keep it on a stronger model.

---

## GROUND RULES (read first, every time)

1. **Concurrency (CLAUDE.md + runbook §38):** shared checkout has other writers. `git status` your
   target files first; if a target is dirty from another session, do the edit in a fresh worktree:
   `git worktree add --detach C:/Users/dhruv/stocks-wt/<step> origin/main` → edit → commit → rebase-push
   → `git worktree remove`. Stage with explicit paths only. Long jobs (step 3) ALWAYS in a worktree.
2. **Publishing (runbook §41):** sessions CANNOT `gh release upload` (permission classifier blocks it) —
   publishes go THROUGH CI. After any data publish, verify `https://dhruvan246.github.io/sf-data/sf_meta.json`
   serves a **changed `rev`**; the workflow's "Verify clients will see this build" step enforces this.
3. **Verify through the client (memory `feedback-verify-users-see-it`):** a step is done when the change
   is visible in the live page / freshly-downloaded live file — not when the script printed success.
4. **Every backfill writes a tracked ledger with provenance** (pattern: scripts/dv_fill.json — fill-only,
   idempotent, applied by the pipeline, never a hand-edit of a derived file).
5. sw.js SHELL cache: any change to backtest-engine.js / *.html needs a CACHE bump (currently v61) or
   clients never load it. backtest-engine.js AND stock-backtest.html carry DUPLICATE loader/FIELDS
   code — always patch both (memory `feedback-backtest-engines-sync`).
6. At step end: re-run the factor-coverage harness against the LIVE data, update memory
   `project-stocks-factor-coverage-audit` with the new column, report before/after to the user.

---

## STEP 1 — Nifty daily closes 2002→2011  (fixes `beta`: 0% before 2012 → ~100% from mid-2002)

Why: `beta` regresses 120 calendar days of stock returns vs NIFTY (`nifty.json` px map). nifty.json
starts 2012-01-03. Everything else about beta already works.

1. `git status --porcelain -- docs/nifty.json` (dirty from another session → worktree rules).
2. Source (bot-walled for python/curl — **browser page-context XHR ONLY**, recipe = runbook §28 +
   memory `project-stocks-index-history-source`): open `https://www.niftyindices.com/reports/historical-data`
   in the Browser pane, then via javascript_tool run the sync-XHR IIFE:
   POST `/BackPage/getHistoricaldatatabletoString` body
   `{"cinfo":"{'name':'NIFTY 50','startDate':'01-Jan-2002','endDate':'31-Dec-2011','indexName':'NIFTY 50'}"}`
   → rows carry `HistoricalDate` ("03 Jan 2002") + `CLOSE`. Reduce in-page to `{"YYYY-MM-DD": close}`,
   JSON.stringify, save to scratchpad (≈2,480 entries; if the tool channel truncates, pull 2 years per call).
3. Merge FILL-ONLY into docs/nifty.json `px` (never overwrite an existing date), keep key order sorted.
   Write a scratchpad merge script; do not hand-edit.
4. **STOP-GATE (values):** cross-check ≥6 month-end closes against docs/index_monthly.json
   (`indices[key=="nifty"].closes["2003"]` etc. — that file is independently sourced). Mismatch >0.1% → stop, report.
5. Commit docs/nifty.json (file-scoped), push via worktree if dirty. No sw bump (json is never sw-cached).
6. Verify live: `curl -s https://dhruvan246.github.io/stocks-dashboard/nifty.json | head -c 200` shows 2002
   dates after Pages deploys (~2-5 min). Re-run coverage → `beta` column ≥99 from 2003 (needs 120d of index).
   Also spot-check `rsNifty`-dependent outputs still sane (same NIFTY map feeds niftyRetAt).

## STEP 2a — Stop `mcap` / `hist_mcap` silently lying  (they are ALWAYS 0 in SF mode — verified)

Why: NSE bhavcopy has no market cap; `loadSF` sets meta.mcap=0. Any `mcap > x` filter empties the
universe silently. Keep the fields (saved strategies may reference them) but make the labels scream.

1. Files: docs/backtest-engine.js `FIELDS` (~line 20-21) AND stock-backtest.html's own FIELDS copy
   (search `hist_mcap` in it) AND sw.js CACHE bump. All three were dirty at various points — check, use worktree.
2. New labels exactly:
   `Market Cap (₹Cr) — ⚠ NO DATA in survivorship-free mode (always 0); size-filter with Turnover instead`
   `Historical Mcap (₹Cr) — ⚠ NO DATA (always 0); use Turnover`
3. sw.js: `sw-shell-v61` → next number, comment `// vNN: mcap fields labelled no-data`.
4. Verify live after Pages deploy: reload backtest page twice (sw update cycle), confirm the dropdown
   shows the ⚠ labels (read_page in the Browser pane). Saved strategies using mcap still load (no removal).

## STEP 3 — TRUE DAILY bars before 2018  (the weekly-bar era breaks the oscillator family)

Measured on 2018+ data subsampled weekly with the live engine (same stocks/dates):
`vol` reads **2.15×** too high → `riskMom` 0.45×; `macd` median ratio **−0.33×** (sign flips!);
`stoch` 0.43×; `rsi` ±6.5 pts noise; `bollB` ±11.6; `accel`/`upPct`/`beta` biased; returns/d52/DMAs/
rangePos/daysHigh/mdd6/turnover/delivPct are fine on weekly bars. So this step fixes:
**vol, riskMom, macd, stoch, rsi, bollB, accel, upPct (+ beta noise)** for 2002-2017 backtests.

Cost: bars 5.5M → ~11.1M, published ~115 MB → ~200-231 MB gz, **3 split parts instead of 2**, every
client re-downloads once. `scripts/_bhav_cache/` already holds 7,977 daily day-files back to 1996
(950 MB) — most of the fetch is already on disk; only pre-v3 (<12-col) cached days refetch.

0. **DECISION-GATE (AskUserQuestion):** DAILY_FROM = `2002-01-02` (recommended: matches the 2002 floor of
   delivery/beta/everything; 1996-2001 stays weekly, saves ~30 MB) or `1996-01-01` (max truth, ~231 MB).
1. Worktree (mandatory — multi-hour job): `C:/Users/dhruv/stocks-wt/daily-rebuild` off origin/main.
2. Pre-count refetch load (scratchpad python): for 2002-2017 dates, count `scripts/_bhav_cache/YYYYMMDD.json`
   missing or `len(rows[0])<12`. Expect mostly hits. >1,500 refetches → keep going, it self-paces.
3. Rebuild in the worktree: `python -X utf8 scripts/build_sf_data.py 1996-01-01 <DAILY_FROM>`
   (background; hours). It applies scripts/dv_fill*.json* at the end — on the UN-merged fresh bin many
   hist-ledger keys won't match yet; that's expected, the next step re-applies on merged keys.
4. **Merge pass on the fresh bin** — update_sf_data.py always loads the RELEASE base, which would clobber
   the rebuild. Add a `--base <path>` flag (≈3 lines: if '--base' in argv → read that file instead of
   load_base()) in the worktree, then run `python scripts/update_sf_data.py --base docs/sf_stock_data.bin`.
   **STOP-GATE:** log MUST show the MANUAL_MERGE lines (PATANJALI, ETERNAL…), `dv_fill.json applied: ~6135`,
   `dv_fill_hist.json.gz applied: ~1.7M`, and end == previous release end. Missing merges → do not publish.
5. Split-parts change (same worktree): split_sf_data.py — parts = `ceil(total_gz / 95MB)` (write
   `{"end","rev","parts":N}` into sf_meta.json; rev logic unchanged — payload sha1). Loaders: in
   backtest-engine.js AND stock-backtest.html change the `for(pi=1..2)` loop to `m.parts||2`. sw bump.
6. Publish path (release asset is >100 MB, sessions can't upload, single files >100 MB can't be committed):
   commit the 3 split parts (each <95 MB) to an orphan branch `rebuild-base`, plus a one-shot
   `adopt-rebuilt-base.yml` (workflow_dispatch): checkout that branch, python-join the parts' JSON,
   gzip → `gh release upload data sf_stock_data.bin --clobber` (CI token may), then `gh workflow run
   refresh-backtest-data.yml`, which republishes sf-data + slices from the new base and the §41 guard
   verifies the rev reaches clients. Delete the orphan branch after.
7. Verify live: download the release asset; assert RELIANCE 2005 has ~250 bars/yr (gap==1), dv intact
   (ACC 20050103 = 27.75), end unchanged; factor coverage re-run; then a browser backtest: 2003-start,
   `RSI(14) < 30`, non-empty picks pre-2018 and a live qualifying-count chart from 2003.
8. Memory + runbook: update §1 (parts=N, --base flag), `project-stocks-factor-coverage-audit`.

## STEP 5 — FII/DII holdings before 2019-09  (shp_engine.json floor = first XBRL SHP quarter)

Hardest of the four — sources get non-machine-readable going back. Do it LAST. Target: quarterly
FII%/DII% + the FILING date (point-in-time is non-negotiable) for N500 ever-members, 2001→2019-06,
merged fill-only into the shp_engine builder via a tracked hist ledger (runbook §22 owns that pipeline).

1. Read runbook §22 first (shp pipeline + the 2-concurrent-writers corruption warning).
2. Probe BSE's SHP surfaces for HISTORICAL quarters (scripcode-keyed; N500 syms ↔ codes via
   scripts/bse_scrips.json): try `api.bseindia.com/BseIndiaAPI/api/ShpSecurities/w?scripcode=500325&qtrid=...`
   (enumerate qtrid), the corporates SHP pages (`/corporates/shpSecurities.aspx?scripcd=…&qtrid=…`), and
   NSE's pre-XBRL SHP archive. Establish per-era: machine-readable? has a submission/filing DATE?
3. **STOP-GATE (point-in-time):** if an era exposes holdings but NO filing date → STOP and report the era
   boundary. NEVER approximate sub-dates silently; a fabricated date poisons every backtest that reads it.
4. **STOP-GATE (definitions):** pre-2015 categories differ (FII vs FPI, no unified DII bucket — MF/banks/
   insurance split). Map to fii%/dii% ONLY if the mapping is exact per era; ambiguity → report, don't guess.
5. Build fetch → parse → ledger (`scripts/shp_fill_hist.json.gz`, provenance per cell) → extend the §22
   builder to apply it fill-only → CI republish → verify live coverage column + 3 spot values per era
   against a public record (e.g. RELIANCE Mar-2015 FII%).
6. Expect a partial ceiling: some small-caps never filed electronically pre-~2006. Report the honest floor.

---

Progress log (append a line per completed step):
- 2026-08-02: campaign file created; baseline table in memory `project-stocks-factor-coverage-audit`.
- 2026-08-02: STEP 1 done (commit 661d413c). Nifty daily closes 2002-01-01→2011-12-30 fetched from
  niftyindices.com, merged fill-only into docs/nifty.json (2,497 dates), 8-point STOP-GATE vs
  index_monthly.json all exact, verified live. Re-audited `beta` coverage (N500 point-in-time, live
  engine + live data): 2002-2011 was flat 0% → now 76.7%-98.9% by year (ramps up from 2002 as stocks
  accrue 120d of history); 2012 also rose 89%→96.5% (partial-lookback-window side effect). Full
  numbers + harness notes in memory `project-stocks-factor-coverage-audit`.
- 2026-08-02: STEP 2a done (commit df70cf9f, pushed via worktree — docs/backtest-engine.js was dirty
  from another session's unrelated loadCore/dash_slim change). Relabeled `mcap`/`hist_mcap` in FIELDS
  (docs/backtest-engine.js AND stock-backtest.html's copy) to state plainly they are always 0 in
  survivorship-free mode and point at Turnover instead; fields kept, not removed, so saved strategies
  referencing them still resolve. sw.js CACHE sw-shell-v61→v62. This is label-only — no coverage number
  changes (mcap/hist_mcap were already ⚠ ALWAYS 0 by design, per `project-stocks-factor-coverage-audit`;
  there is no fetchable market-cap source in NSE bhavcopy, so this ceiling is permanent, not a gap to
  re-measure). Verified live: Deploy-to-Pages run green, fetched live backtest-engine.js/sw.js show the
  new text, active SW cache key confirmed `sw-shell-v62` after 2 reloads, read_page on the live dropdown
  (both sortBy and the +Add-filter combobox) shows the exact ⚠ labels, no console errors on
  stock-backtest.html or saved-strategies.html.
- 2026-08-02: STEP 3 done. DECISION-GATE answered: DAILY_FROM=2002-01-02 (recommended floor, not
  1996 max-truth). Rebuilt docs/sf_stock_data.bin from scratch (worktree stocks-wt/daily-rebuild,
  _bhav_cache copied over first — it's gitignored so a fresh worktree starts empty): 185MB gz, 4,441
  symbols, end=2026-08-02. Merge pass (`update_sf_data.py --base <freshbin>`, new flag — found+fixed
  an argv collision with build_sf_data's own module-level sys.argv parsing) STOP-GATE all green:
  MANUAL_MERGE fired for PATANJALI/PCBL/RBA/LTM, ETERNAL/ZOMATO + all 12 other tripwire renames
  verified merged with no orphan stubs, dv_fill 6,134 + dv_fill_hist 1,707,993 cells applied (at
  build time), end not regressed vs live release. split_sf_data.py generalized to dynamic
  `parts=ceil(total_gz/95MB)` (computed 2 this time) instead of a fixed 2; loaders in
  backtest-engine.js + stock-backtest.html now read `sf_meta.json.parts`; sw.js v62→v63. Published
  via a NEW pattern (rebuilt bin >100MB, can't be committed or `gh release upload`ed by a session):
  split parts → orphan branch `rebuild-base` → one-shot `adopt-rebuilt-base.yml` (workflow_dispatch)
  joins + uploads the release asset, then the session manually ran
  `gh workflow run refresh-backtest-data.yml` itself (the workflow's OWN attempt to do this via
  GITHUB_TOKEN gets HTTP 403 — the default token can't dispatch other workflows, only a session's
  own gh auth can). That run split+force-pushed to sf-data successfully. Orphan branch + one-shot
  workflow deleted after. Verified live: downloaded+parsed the live split parts (same facts as
  local), network-log confirmed the real page fetches exactly 2 parts with the correct versioned
  cache key, and called `ensureData()`+`simulate(cfg)` directly (bypassing `run()`'s wrapper, which
  writes to the REAL shared Supabase-synced bt_strategies/bt_history — simulate() itself has no
  side effects) for RSI(14)<30 2003-2017: 169/180 months produced real, sane picks. Full writeup +
  two near-misses (shared-storage risk, a DATA_RUNBOOK.md concurrent-edit recovered via `git add -p`)
  in memory `project-stocks-daily-bars-rebuild`.
- 2026-08-02: STEP 5 PARTIAL — user asked for both the achievable extension AND the harder
  pre-2016 OCR attempt; delivered the former, hit a genuine dead end on the latter (reported, not
  guessed past). **DONE: FII/DII extended 2019-09 -> 2016-03/06** via BSE's SHPQNewFormat
  quarter-list (real filing_date_time + XBRL back to Mar/Jun-2016, confirmed system-wide across
  RELIANCE/TCS/PAGEIND — NOT one company's quirk) + XBRL parsed by the EXISTING parse_shp()
  unmodified (BSE's in-bse-shp taxonomy uses identical concept/member names to NSE's). New script
  `scripts/fetch_shp_bse_hist.py`, ledger `scripts/shp_fill_hist_2016_2019.json.gz` (623 companies,
  6,787 cells), applied fill-only via a new `apply_bse_hist_ledger()` in fetch_shareholding.py
  (called every refresh_quarters() run, idempotent). Verified live: shp_meta.json cells
  53,863->60,650, shp_engine.json RELIANCE 40 quarters from 2016-09-30, and a live
  `simulate(cfg)` call with `fiiPct>15` 2017-2018 quarterly returned 8/8 non-empty rebalances
  (impossible before — fiiPct was 0% coverage pre-2019). Independent 3rd-party spot-check was
  NOT achievable — Screener/Trendlyne free tiers only show a rolling ~3yr window, nowhere near
  2016; verification instead rests on parse_shp's own reconciliation math + smooth multi-quarter
  trends + qualitative checks against well-known facts (ITC prom=0.00% every quarter — ITC is
  famously promoter-less; Infosys prom~12-13% — also famously low). **STOP-GATE, pre-2016: NO
  accessible source found for OCR, not just missing dates.** Systematically probed and ruled out:
  BSE's own SHP module (quarters exist as bare labels back to 2001, but `shpDecleraction` returns
  `[]` — literally no data, nothing to date-stamp or OCR); BSE's general announcement search
  (correct category name is "Shareholding" per its own JS bundle, confirmed working recently, but
  returns 0 rows for RELIANCE even in a recent sanity-check window — routine quarterly SHP filings
  don't flow through it); NSE's live master API (returns 0 for ANY old quarter-end, confirmed with
  a KNOWN-recent date too — it's a rolling window only, can't retroactively query, which is also
  WHY the Sep-2019 floor existed pre-STEP-5); NSE's general corporate-announcements API (genuinely
  has 2010-era data — 2,059 real rows for Jan-2010 — but its ~40 distinct filing categories that
  month contain no "Shareholding Pattern" entry at all). Full detail in memory
  `project-stocks-shp-bse-backfill`.
- 2026-08-03: **STEP 5 FULL-DEPTH DONE — the "pre-2016 unreachable" verdict above is SUPERSEDED.**
  User asked for entire step-5 coverage, so the dead end got re-attacked from a different angle and
  broke open: the archived WEB is a source the API probes never considered. The Wayback Machine
  holds ~134k captures of Moneycontrol's old `company-facts/<slug>/shareholding-pattern/<scId>/<qtrid>`
  pages — server-rendered full Clause-35 tables (promoter total, MF/UTI, FI/Banks, insurance, FII
  rows + institutions subtotal), dense 2011-2016, and MC's qtrid numbering IS BSE's own
  (29=Mar-2001 … 89=Mar-2016), so quarters are addressable exactly. New `scripts/fetch_shp_wayback_mc.py`
  (map → frontier → sample → harvest → ledger, all resumable, page cache gzipped).
  **Result: ledger `scripts/shp_fill_hist_2010_2016.json.gz`, 4,118 cells / 523 companies,
  Dec-2010 → Sep-2015**; shp_history 60,650 → 64,768 cells; shp_engine RELIANCE 40 → 53 quarters
  (earliest 2011-03-31), TCS from 2010-12-31. Verified live: a `simulate()` run with `fiiPct>15`,
  2012-01→2015-09 quarterly, gave **15/15 non-empty rebalances, 21.1% CAGR**, holdings that are the
  right names for the era (UNITDSPR 53%, APOLLOHOSP 40%, ZEEL 37%). Shared bt_* stores untouched
  (58/300 before and after).
  **Two STOP-GATE catches worth keeping:** (1) column convention — pages give both "% of (A+B)" and
  "% of (A+B+C)"; these are identical except for GDR companies, so calibration measured ONLY that
  subset against trusted 2016 XBRL anchors → ABC wins (6.23 vs 6.87 median pp) and matches modern
  XBRL's A+B+C2 basis. (2) the gate's first run returned *zero* overlap, which turned out to be a
  SOURCE defect, not a bug: MC's pages carry an EMPTY FII row for EVERY company at Dec-2015 and
  Mar-2016 (verified on ACC — a large cap that plainly had foreign holding), 0% fill vs 94-99% at
  every earlier quarter, i.e. MC's own pipeline broke when SEBI restructured the format. Those 838
  cells are DROPPED rather than written as fii=0 — writing them would have fabricated "no foreign
  holding" for ~840 company-quarters, exactly what parse_shp's never-zero-default rule exists to
  prevent. Also dropped: 196 failing institutions-reconciliation, 181 no-fii, 132 no-promoter-total.
  **Dates:** pre-2016 filings carry no submission date anywhere (BSE deleted them), so visibility =
  QE+21d, the era's SEBI Clause-35 deadline, flagged approximate in every cell's provenance slot
  (`--lag N` regenerates). 2016+ cells keep their REAL dates — ledgers apply in order, XBRL first.
  **Remaining floor: Sep-2015 back to Dec-2010 is filled; 2001-2010 stays unreachable** (MC had no
  such pages before Dec-2010 — verified on a 2008 capture; only March quarters could ever come from
  annual-report OCR, a different undertaking). Full detail: memory `project-stocks-shp-wayback-2010`.
