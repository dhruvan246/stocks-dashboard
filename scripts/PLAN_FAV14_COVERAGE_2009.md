# ★ LIVE SCORECARD (2026-08-24 09:03 IST) — 17,694 → 9,202 missing member-months (48% closed)
# 6 technical 84→5 · FII%/DII% 117→9 · diiChgPp 2132→1365 · NPyoy-con/streak 786→289 · TTM-con 1833→1342
# · NPyoy-std 3871→1513 · TTM-std 7548→4356.  Done: P1 price, P2+P5 SHP (932 cells), P4 std-PAT (1704 cells).
# NEXT: NSE archive (api/corporates-financial-results — BOTH bases, REAL dates, 2005+; PAT in
# resultDetailedDataLink) for std FY2007/detres-gaps + con-PAT (P3); 91 delisted std symbols (era codes);
# diiChgPp 2016+ (XBRL). profitTTMStd is stickiest (8 consecutive quarters).
# PLAN — FAV14 COVERAGE 100%: the 14 parameters the favourite strategies use, Nifty 500, 2009-01 → date

**Written 2026-08-24 01:30 IST (Fable). User mandate, verbatim: *"dont assume. everything can be
filled. make a plan and fill them all 100%. take one stock one param one month at a time."* and
*"this is the most important work for u fable since we started months back."***

> **Golden rule (§0, §57d):** never assume, never guess. A route returning nothing means THAT ROUTE
> has no row. "Unfillable" may be written only after the whole §57b ladder is walked AND a primary
> document says the company did not report. Every verdict carries the rungs tried, per cell.
> **Unit of work = one root cell** (one symbol, one quarter, one basis — or one symbol-month for
> price/SHP), queued in `scripts/fav14_queue.json`, status `open` until `filled` or `refused:<rungs>`.

Parent method: `scripts/N500_COVERAGE_100_CAMPAIGN.md` (2020→date, all params) and
`PLAN_N500_COVERAGE_2015_2020.md`. This plan differs in SCOPE (14 params, full 2009→date window)
and in its unit (root cells, not member-months). Runs in worktree `~/stocks-wt/fav14-cov`.

## 0. The 14 parameters and what each actually needs (engine-verified, backtest-engine.js)

| param | needs at the rebalance date | root cell |
|---|---|---|
| d52, d52_low_pct, daysHigh, ret6m, dma200, mdd6 | a PRICE ROW (close ≤ date, bar in last 14d) | (symbol, month) price bar |
| fiiPct, diiPct | an SHP filing with sub-date ≤ date | (symbol, quarter) SHP row |
| diiChgPp | the above AND the PRIOR quarter's filing, sub-date ≤ date (never across 2022-09-30, C2) | (symbol, prior quarter) SHP row |
| profitYoyPct, profitStreak (con-blend) | PAT(t) visible (ann>0, ann≤date) AND PAT(t−4) row present with non-null value; con then std | (symbol, quarter, con∨std) |
| profitTTM (con-blend) | 8 CALENDAR-consecutive quarters on one basis (pairwise 3-month gaps) | (symbol, quarter, basis) |
| profitYoyStd, profitTTMStd | same, standalone slots only | (symbol, quarter, std) |

## 1. Baseline — measured 2026-08-24 01:07 IST local bake, cell-identical to live (params, na, members all equal)

Member-months missing, 2009-01-31 → 2026-08-21 (212 month-ends), after tonight's roster fixes (§106f/g):

| param | missing member-months | → root cells | class |
|---|---|---|---|
| d52 … mdd6 (×6) | 84 each | **14 symbols** with NO price row on their member-months | P — price tape |
| fiiPct / diiPct | 117 | **4 symbols**: MVL 16, SHLAKSHMI 7, INNOIND 6, SPSL 4 | S0 — SHP row |
| diiChgPp | 2,132 | **614 (symbol, prior-quarter) SHP rows** across 436 symbols; 2,008 of 2,132 member-months = `prior-quarter-row-absent`, 34 no-shp-rows, 6 no-visible-filing | S1 — SHP row |
| profitYoyPct / profitStreak | 786 | 232 root cells `base-row-absent` (676 mm), 1 base-null, 1 base-zero, 21 no-visible-quarter | F-con |
| profitTTM | 1,833 | 585 root cells `missing-quarter(s)`; 52 no-visible; 10 no-fund-rows; 32 resolve-in-reproduction (gap/dupe guard) | F-con |
| profitYoyStd | 3,871 | 1,147 root `base-row-absent` (3,663 mm), 37 base-null, 3 base-zero | F-std |
| profitTTMStd | 7,548 | 2,599 root `missing-quarter(s)`; 137 guard cases | F-std |

**Union of fundamentals root cells: CON 586 (109 symbols) · STD 2,623 (495 symbols).**
By quarter-year: CON 2007:47 · 2010:103 · 2011:78 · 2012:47 · 2013:98 · 2014:142 · rest ≤19/yr.
STD 2007:272 · 2008:204 · 2009:116 · 2010:385 · 2011:320 · 2012:268 · 2013:296 · 2014:472 · 2015:86 · 2016:81 · then ≤41/yr.
SHP prior-quarter roots by year: 2008:44 · 2010:48 · 2011:102 · 2013:64 · 2014:66 · 2015:68 · **2016:126** · rest small.

**Dominant cause everywhere is the same: the whole quarter ROW is absent from our store** — not a
null in an existing row, not a bad date. These are capture gaps, the most fillable class there is.

`na` (evidence-backed N/A already excluded from the page's denominator — C1/C2/C3 of the parent
campaign): profitTTM 7,648, profitYoyStd 1,646, profitTTMStd 2,071, diiChgPp 3,703 member-months.
**Decision checkpoint (§5.1):** the user may re-open any N/A class; they are listed, not assumed closed.

## 2. Fill routes per class (the §57b ladder, in order; log every rung per cell)

**P — price tape (14 symbols, 84 member-months).** TANDHANIN 25, RASOYPR 24, PUNJABTRAC 7,
DALBHARAT 6, SHPRE 5, BONGAIREFN 5, MICRO 3, SUMMITSEC 2, FRETAIL 2, AJMERA/VISHALEXPO/BBOX/HFCL/PROVOGUE 1.
Rungs: (1) is the key an era/rename orphan — does the tape exist under another key (§95 issuer-prefix,
§106 seams)? (2) NSE bhavcopy archive for the month (§88b daily-sweep tooling); (3) BSE bhavcopy
(BZ/BE series, §"BZ series never ingested"); (4) Wayback of either. A member with no bar anywhere in
the month after all four = suspended that month: record the exchange notice.

**S0/S1 — SHP rows (4 + 614).** Write to `scripts/shp_history.json` NEVER `docs/shp_engine.json`
(feed regenerates). Rungs: (1) BSE `SHPQNewFormat/w?scripcode=` (dates real Mar-2016+, rows back to
2001 with null date → qe+21d convention, §105); (2) NSE `corporate-share-holdings-master` (≈2021+);
(3) archived Moneycontrol SHP pages (2010-2016, §22 / `project-stocks-shp-wayback-2010`);
(4) BSE announcement PDF of the SHP filing; (5) Wayback of exchange SHP pages. Coordinate with
`PLAN_SHP_4DP_FULL.md` (cell_fix outranks). Sub-date for a pre-2016 fill = qe+21d 15:30 convention.

**F-con / F-std — PAT quarter cells (586 + 2,623).** Ledger = `feed_qe_fix.json` (+ `ann_date_fills.json`
for the date) → rebuild `sf_fundamentals.json` via the documented builder; never edit the derived file.
Rungs (§57b): (1) BSE detres JSON `Corp_detailedResult_Transpose_ng/w?scrip_cd&qtr=QID.00`, QID =
85+4×(FY−2015)+{Mar:0,Jun:1,Sep:2,Dec:3} — **serves 2008Q1+ (qid 57) incl. delisted**, ₹ million ÷10,
standalone `.00` / consolidated `.50`? (verify per §42 — `.50` = audited annual); (2) NSE archive
`corporates-financial-results?symbol=&period=Quarterly` → `financial_res_<SYM>_<id>.html` — 2005+,
BOTH bases, declares Consolidated/Non-consolidated + cumulative flag (§52/§53) — **the primary std
route for 2007-2014**; (3) BSE announcement PDF (AnnPdfOpen resolver, pre-2018 on the 3rd base —
`reference-bse-attachment-resolver`); (4) comparative column of the next-quarter / year-later filing
(`--rescue`, §6); (5) XBRL cache 2018+; (6) FY/9M identity (annual − known siblings, §45/§53d) —
residue identity to the paisa adjudicates; (7) no-sub identity con=std ONLY with proof of no
subsidiary (§6A); (8) vision read of scanned PDFs (§17b) — LAST, and ask first; (9) Wayback of
NSE/BSE result pages (§32) — **reachable from this Mac (verified tonight)**; (10) acquirer's
disclosures for merged entities (§51c).
Every landed value: provenance string (route, URL/id, scale declared, basis declared, period
declared), FY-identity check where siblings exist (§45), scale sanity vs neighbours (§"power-of-ten").
Every announce date: ≥ first traded bar (§99), never 0, never a guessed qe+45d when a filing date
is readable.

**Anchor rule (feedback-anchor-validates-one-field):** a filing that proves PAT proves PAT — it does
not license a revenue/op write in the same row unless that field was read too.

## 3. Execution order (one cell at a time; cheapest class first so the page moves early)

1. **P0 (done above):** worktree · full bake · explain · facts · queue file `scripts/fav14_queue.json`.
2. **P1 — P class (14 symbols):** resolve each symbol's identity first (rename/era key), then the
   month's bars. Expected: mostly era-key orphans and suspended stocks. Re-bake; the six technical
   params should read 100% on every month-end.
3. **P2 — S0 (4 symbols, 117 mm):** MVL / SHLAKSHMI / INNOIND / SPSL SHP rows via the ladder.
4. **P3 — F-con (586 cells, 109 symbols), newest era first** (2014→2007): detres (rung 1) in bulk
   per symbol — one request per (scrip, QID) — then NSE archive for misses, then PDFs. Land through
   `feed_qe_fix.json`; rebuild; re-bake; assert the YoY/TTM con counts moved by the landed cells.
5. **P4 — F-std (2,623 cells, 495 symbols):** same ladder; NSE archive is the workhorse (2005+, both
   bases, declares basis). Batch by symbol; each symbol's quarters walked in one pass so FY-identity
   can adjudicate.
6. **P5 — S1 (614 SHP prior-quarter rows):** BSE SHPQNewFormat per scrip (rows back to 2001), then
   archived Moneycontrol for 2010-2016 misses. Land in `shp_history.json` with the convention date;
   coordinate with the SHP plan's ledgers.
7. **P6 — Close-out:** full bake; every one of the 14 params must read exactly 100% on every
   month-end 2009-01-31 → 2026-06-30 LIVE (dispatch refresh-coverage.yml, then pages.yml, `?cb=`);
   the 45 other params re-verified UNCHANGED or better; queue has zero `open`.

**Stop-gates:** a ledger whose re-bake moves 0 cells did nothing (`feedback-ledger-guard-count-must-move`)
· a heal applied twice changes 0 cells on the second pass · scale sanity on every landed value ·
never commit a partial bake · push via the CLAUDE.md retry recipe · verify LIVE ~20 min later.

## 4. Progress log (append per session; counts are MEASURED post-bake, never projected)

- 2026-08-24 09:00 IST — **P4 (std PAT) MAJOR PASS: 1,443 standalone-PAT cells filled** via BSE
  detres (Corp_detailedResult_Transpose_ng, ÷10 Rs cr, Date-End validated, EPS cross-checked,
  ann=SEBI deadline). Idempotent fill-only into both sf_fundamentals twins via
  scripts/apply_fav14_pat_std.py + ledger scripts/fav14_pat_std_fills.json (rule-5 reproducible).
  Measured (engine-faithful bake): profitYoyStd 3798->1751, profitTTMStd 7482->4924; AND the con
  blend fell via std-fallback: profitYoyPct/Streak 699->333, profitTTM 1746->1369. Harvest 1443/1782
  ok; rejects = 327 detres-not-served (older/NSE-only) + 12 non-standard fiscal quarters (PFIZER
  Feb/May/Aug/Nov, MPHASIS Jan/Apr/Jul/Oct — correctly refused by Date-End guard, their rows use
  real quarter-ends). 2 magnitude outliers (PVP) verified REAL to the paisa by EPS. 0 value-conflicts
  with existing cells. RESIDUAL std PAT: ~2007 (needs NSE archive rung 2), 91 delisted no-code
  symbols (560 cells, era bhavcopy + ISIN like P2), and detres-not-served quarters.
- 2026-08-24 05:10 IST — **P5 (diiChgPp) MAJOR PASS: 2026→1365 (−661)** via BSE aspx frontier
  harvest (912 cells / 236 symbols, 2008-09..2016-03, FVCI/VCF-fixed parser), isolated ledger
  shp_fill_fav14.json.gz (now 932 cells), fill-only, idempotent (+912 then +0), all in-range,
  0 collided with history. Also committed the FVCI/VCF parser fix (cb63019ac) — validated 0/40
  existing cells changed. RESIDUAL diiChgPp 1365 by year: 2016=418 (aspx frontier stops 2016-03;
  2016-06+ needs the XBRL/NSE route — SHP campaign's 2016+ ledgers), 2011=163/2012=141/2009=117
  (absent pages 93 + 50 unresolved delisted scripcodes/242 cells + the 2015-12/2016-03 seam
  exclusion), 2017-19=143 (post-aspx era). NEXT for diiChgPp: resolve the 50 delisted codes
  (era bhavcopy + ISIN-verify, like P2) and the 2016+ XBRL route.
- 2026-08-24 03:40 IST — **P2 (SHP-level) DONE for 4 of 5 symbols** via BSE ShareholdingPattern.aspx
  (isolated ledger scripts/shp_fill_fav14.json.gz, fill-only, wired into fetch_shareholding BSE_HIST_LEDGERS;
  idempotent 2nd pass 0 cells). fiiPct/diiPct 40→9 (MVL 8q, SHLAKSHMI 5q, INNOIND 4q, SUMMIT 3q = 20 cells,
  all reconciled <0.15pp). diiChgPp 2057→2026. Codes ISIN-verified in _shp_scripcode_override.json.
  ★ FINDING for the SHP campaign: fetch_shp_bse_aspx.py parse_new MISSES the "Foreign Venture Capital
  Investors" row (foreign→fii) — INNOIND failed recon by exactly the FVCI 4.36%; hand-derived here.
  This causes MISSES (rejected cells), never wrong values, so it is safe to fix systemically later.
  RESIDUAL: SPSL 4-5 months (2009-Q1 SHP — aspx empty pre-Sep-2009, MC Wayback from 2010; 2009 source-limit)
  + the 4 P1 price-convention symbols (AJMERA/BBOX/HFCL/PROVOGUE, __norow, not SHP-fillable).
- 2026-08-24 03:05 IST — **P1 LIVE-VERIFIED on the page** (bake 02:25 IST): six technical params
  84→5, all others improved (see §4b). Propagation needed refresh.yml (dash_slim rebuild) — recorded
  as a lesson in §4b. RASOYPR heal live in the release asset (close 14.85).
- 2026-08-24 02:20 IST — **P1 (price class) COMPLETE: 84 → 5 member-months** (bake on the healed bin +
  corrected roster, engine-faithful). RASOYPR = 3 phantom splits (tick-floor crashes 20140926/20171003/
  20171106) recorded in phantom_crashes.json → noadjust; 734 NSE-bhavcopy bars (row identity proven on
  all 734: TOTTRDQTY==v, TOTTRDVAL/1e5==t) landed via dvl_dtil_surgery.json.gz, idempotent (2nd pass 0).
  Roster: Mandhana→GBGLOBAL (fuzzy had made TANDHANIN, a Rs 7-lakh shell, a "member"); PUNJABTRAC/
  BONGAIREFN/MICRO/VISHALEXPO exclusions mapped (dead names, register had the dates); Summit seam
  (SEAM_TWINS + ERA_OVERRIDES). Side effect: every other fav14 param improved as phantoms left the
  denominator (fiiPct 117→40, diiChgPp 2132→2057, YoY-con 786→699, TTMstd 7548→7482). Residual 5 =
  suspended >14d (SPSL/HFCL/PROVOGUE) + mid-month listings (AJMERA/BBOX) — engine conventions,
  decision items in §5, not fills.
- 2026-08-24 01:30 IST — P0 complete. Queue: price_norow 14 · shp_level 4 · shp_prior_quarter 614 ·
  pat_con 586 · pat_std 2,623. All `open`.

## 4b. RESUME HERE (updated 2026-08-24 03:05 IST — P1 CLOSED & LIVE-VERIFIED)

**P1 (price class) is DONE and LIVE.** Coverage page (2026-08-24 02:25 IST bake) reads, N500 2009→date:
d52/d52_low_pct/daysHigh/ret6m/dma200/mdd6 **84 → 5 missing** each; side-effect improvements
fiiPct/diiPct 117→40, diiChgPp 2132→2057, profitYoy 786→699, profitTTM 1833→1746, YoyStd 3871→3798,
TTMStd 7548→7482. The 5 residual d52 months are engine conventions (§5 decision items), NOT fills:
2009-05 SPSL suspended >14d · 2009-06 AJMERA listed mid-month · 2010-06 BBOX listed mid-month ·
2011-02 HFCL suspended · 2012-03 PROVOGUE suspended.

★★ **PROPAGATION LESSON (cost 3 extra bakes) — a ROSTER change needs FIVE steps to reach the page,
not three.** The Coverage Matrix reads its ROSTER from `docs/dash_slim.bin` (indicesHistory), which
is rebuilt ONLY by `refresh.yml` (build_compressed.py reads scripts/indices_history.json). The
membership refresh updates indices_history.json + splices stock_data.bin, but NOT dash_slim.bin. So:
  indices_history.json (refresh-membership) → **dash_slim.bin (refresh.yml)** → coverage (refresh-coverage)
  → pages.yml → verify ?cb=
Skipping refresh.yml leaves the coverage builder on the OLD roster (I saw TANDHANIN/PUNJABTRAC still
"members" until refresh.yml ran). A PRICE-bin heal (RASOYPR) needs only the release-asset path
(refresh-backtest-data uploads it; coverage --bin auto downloads it) — but beware the release-asset
CDN can serve a stale copy for a few minutes after upload; re-verify or re-dispatch if a heal you
confirmed in the asset doesn't show on the page.

**P2 (SHP-level) is DONE & LIVE for 4 of 5 symbols** (commit 7238cf621). fiiPct/diiPct 117→9 live.
Residual: SPSL 2009-Q1 (source-limited — aspx empty pre-Sep-2009, try archived exchange pages) +
the 4 P1 price-convention symbols (not SHP-fillable).

**P4 std-PAT LANDED (1,443 cells) — profitYoyStd 3798→1751, profitTTMStd 7548→4924 LIVE.**
Remaining std PAT: FY2007 (detres starts 2008 → NSE archive rung 2), 327 detres-not-served quarters
(→ NSE archive), 91 delisted symbols/560 cells (era bhavcopy + ISIN-verify → detres). profitTTMStd
is stickiest (needs 8 CONSECUTIVE quarters, so any remaining hole keeps its window open). con-PAT
(P3) still open: profitTTM 1369, profitYoyPct/Streak 333 (route via _reattr_owners.json owners
ledger — patCon is recomputed nightly by apply_owners_full, so do NOT write patCon directly).

**NEXT — three classes remain, all measured to root cells (queue: scripts/fav14_queue.json):**
- **P5 diiChgPp — 614 (symbol, prior-quarter) SHP rows, live 2026 missing.** SAME instrument as P2
  (fetch_shp_bse_aspx.py). This is the big SHP win. Build the frontier (cmd_frontier reads current
  coverage gaps + _shp_scripcode_override for delisted codes), harvest into an ISOLATED ledger
  (do NOT overwrite the shared shp_fill_bse_aspx.json.gz — narrow harvest wipes it; instead call
  run() on a filtered frontier like the P2 driver did, or extend shp_fill_fav14). Fix the FVCI
  parser gap first (parse_new must treat "Foreign Venture Capital Investors" as fii) — it will
  recover many otherwise-rejected cells. Apply via fetch_shareholding --apply-ledgers.
- **P3 pat_con — 586 root quarter-cells (109 symbols), P4 pat_std — 2,623 (495 symbols).** ⚠ These
  are the LARGEST remaining gaps (live: profitTTMStd 7482, profitYoyStd 3798, profitTTM 1746,
  profitYoy/Streak 699) and drive the strategies (con-blend = 6 of 7 favourites; std = strategy 1).
  They route into docs/sf_fundamentals.json, which is built by a DEEP multi-applier pipeline (many
  scripts/_*_apply.py + apply_fund_cell_fix.py + _stdpat_apply.py + _nse_archive_revop.py + nightly
  CI). This is its own sub-campaign — STUDY the sf_fundamentals build order before writing any ledger
  (a wrong ledger corrupts the fundamentals store). Route ladder §2: BSE detres JSON (rung 1, 2008Q1+,
  QID = 85+4*(FY-2015)+{Mar:0,Jun:1,Sep:2,Dec:3}, .00 standalone / .50 consolidated) is the con/std
  workhorse; NSE archive (rung 2, 2005+, both bases) for std. 95% of roots are FY2007-FY2015. Re-derive
  the exact current roots first (the P0 method: bake --explain, reproduce the engine's REACH rule per
  cell). Batch by symbol so FY-identity (§45) adjudicates. Do it in THIS worktree or a fresh one.
- **diiChgPp residual 1365:** 2016+ (≈561) needs the XBRL/NSE route (SHP campaign's 2016+ ledgers);
  pre-2016 (≈804) = 93 unserved aspx pages + 50 unresolved DELISTED scripcodes (242 cells, mostly
  pre-2009 = low yield for the 2009+ window) + the 2015-12/2016-03 seam. Resolve delisted codes via
  era BSE bhavcopy + ISIN-verify (as in P2) only for the 2009+ ones.
- **(superseded line, see above)** P3 pat_con — 586 root quarter-cells (109 symbols), F-con.** Ledger feed_qe_fix.json → rebuild
  sf_fundamentals. Route ladder §2: BSE detres JSON (rung 1, 2008Q1+) is the workhorse for con.
  95% of roots are FY2007-FY2015.
- **P4 pat_std — 2,623 root quarter-cells (495 symbols), F-std.** NSE archive (rung 2, 2005+, both
  bases) is the std workhorse. Batch by symbol so FY-identity (§45) can adjudicate each.

**Propagation for each class (learned in P1, §4b top):**
- roster change → refresh-membership → **refresh.yml (dash_slim)** → refresh-coverage → pages → ?cb=
- SF-bin heal → refresh-backtest-data (release asset; watch CDN staleness) → refresh-coverage → pages
- SHP/PAT fill → commit ledger+regenerated feed/sf_fundamentals → refresh-coverage → pages
Always bake-and-measure locally first; §57 ladder logged per cell; run-twice idempotency; file-scoped adds.

**THEN P3 (pat_con 586) / P4 (pat_std 2,623) / P5 (shp_prior_quarter 614).** Routes in §2.
Rules that bind every step: §57 ladder logged per cell · run-twice idempotency · bake-and-measure
before commit · file-scoped adds · worktree ~/stocks-wt/fav14-cov.

## 5. Decision checkpoints

1. **N/A classes:** 15,068 member-months across the 14 params are already excluded as evidence-backed
   N/A (C1 report-absent, C2 SEBI-2022 suppression, C3 pre-history). The user said "everything can be
   filled" — ask whether any class is to be re-opened, with the per-class counts above in hand.
   Recommendation: keep C2 (code-defined) and C3-listing (a company cannot file before it exists);
   re-examine C3 where the company existed but our first-bar is late (a tape gap, not pre-history).
2. **Pre-2016 SHP sub-date convention (qe+21d 15:30):** keep (calibrated, §105) unless a real filing
   date is readable for that cell.
