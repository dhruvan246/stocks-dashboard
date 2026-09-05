# PRE-2009 standalone-revenue campaign — plan + learnings handoff

Written 2026-08-25 by the session that closed 2009→2026 (std-rev now 98–100% every year).
This is the **next** campaign: the same job for **2002–2008**. Read this whole file before touching data.

## 0. The standing rules (they outrank everything below)
- **NO ASSUMPTIONS, NO GUESSWORK.** Every value written traces to something *measured this session*.
  Can't measure it → record "unknown". A plausible guess presented as fact is worse than an admitted gap.
- Read `scripts/DATA_RUNBOOK.md` first. Own worktree; file-scoped `git add` (NEVER `-A` / `-u` / `.`).
- Verify pushes **by content on origin**, not by exit code.

## 1. The bounded problem (measured 2026-08-25, whole store)

| Year | missing revStd **with** PAT anchor | **no** PAT anchor |
|---|---|---|
| 2002 | 437 | 0 |
| 2003 | 257 | 0 |
| 2004 | 338 | 0 |
| 2005 | 44 | 0 |
| 2006 | 32 | 0 |
| 2007 | 274 | 0 |
| 2008 | 119 | 0 |
| **total** | **1,501 across 322 symbols** | **0** |

Every missing cell HAS a PAT anchor → the anchor gate works everywhere. Biggest symbols:
M&M(19) ATLASCYCLE(19) SMLMAH(17) UNITECH(13) DENABANK(13) SHASUNPHAR/SCHAEFFLER/FEDERALBNK/
SUNDRMBRAK/STYRENIX/SWARAJENG/DALMIASUG/J&KBANK/KTKBANK/SUNDRMFAST/FOSECOIND/KANSAINER/SAMTEL/
WYETH/ZANDUREALT(12 each).

### ⚠️ The coverage ceiling — measure it before promising a number
N500 point-in-time coverage (last full scan): 2002 rev 1.2% / **PAT 23.7%**; 2003 56.1/70.4;
2004 67.3/83.4; 2005 80.5/90.6; 2006 93.4/95.1; 2007 91.1/96.3; 2008 91.3/97.8.
**revStd coverage cannot exceed patStd coverage**, because a fill needs an anchor. In 2002 the
binding constraint is missing fundamentals ROWS, not missing revenue. Filling all 437 anchored 2002
cells still leaves 2002 near ~23%. That residual is a *different* campaign (no-row / PAT-side; see
the pre-2015 tooling's "STEP F: NSE-archive EPS-recon for cells with no stored PAT anchor").
**Do not promise "2002 → 100%".** Measure `--explain` first, state the ceiling honestly.

## 2. Routes, in descending yield (all proven this session)

1. **MC as-filed deep batch** — reaches ~1997, serves the AS-FILED vintage (§108-safe).
   `python -X utf8 scripts/_mc_batch_fill.py --cells <cells.json> --emit <emit.json>`
   → `scripts/_mc_add.py < emit.json` → `scripts/_apply_reads.py`.
   Cells file shape: `{"alive":[SYM,...],"cells":{SYM:[qe,...]}}`.
   This filled ~2,000 cells in 2009-2026. **Expect it to be the workhorse again.**
2. **NSE archive** `scripts/_nse_archive_revop.py --gaps <gaps.json> --only SYM,SYM --out-suffix _x`
   (gaps shape `{SYM:[qe,...]}`). Pre-2018, keyed by the **NSE symbol** → survives BSE code changes.
   Declared scale + declared bank/non-bank. **CHECK `scripts/_nsearch_cache/` FIRST** — see §4.
3. **MC-raw `'--'` ⇒ nil.** Holdcos / pre-operational / suspended filers genuinely file NIL operating
   revenue. Fill **0** — `build_coverage_matrix.js:911/917` counts 0 as present (no zeroIsNull on the
   basis path). Confirmed nil this session: CAIRN COALINDIA IRB RAIN PVP UTVSOF SUMMIT RPOWER SFCL
   ONELIFECAP ORISSAMINE RTNPOWER UJJIVAN COFFEEDAY. Expect MANY more pre-2009.
4. **Own BSE filing read** — comparative columns + period identities: annual−9M, H1−Q2, 9M−Q2−Q3.
   Each identity must close on BOTH revenue and PAT before you trust it.
5. **Old-name / old-code identity** — MC `sc_id` under the FORMER name (PVP→`SSI`), BSE quote-search
   `api.bseindia.com/Msource/1D/getQouteSearch.aspx?Type=EQ&text=<name>&flag=gq`. **Gate on ISIN.**

## 3. The gates (non-negotiable — a batch without all four is not shippable)

1. **PAT anchor at apply time.** `_apply_reads.py` re-anchors every cell against stored
   `sf_fundamentals` npStd. ~1% rejection is healthy. **Anchor refusals are FINDINGS, not failures** —
   every refusal this session exposed a real defect (see §5).
   ⚠️ **Exact match beats tolerance.** The helper's `close()` allows `max(2.0, 3%)`; LICI Mar-22 slipped
   through at 1.6% and was a basis-swap. When both bases are plausible, demand the exact print.
2. **Per-symbol revenue convention**, determined against the symbol's OWN existing revStd — the
   applier does NOT gate the revenue LINE. `rev_total` for industrials; **Interest Earned** for banks
   (CORPBANK, CSBBANK, CENTRALBK, EQUITASBNK all confirmed). Pre-2009 is bank-heavy (DENABANK,
   FEDERALBNK, J&KBANK, KTKBANK, SOUTHBANK in the top-25) — **expect the bank convention often.**
3. **Series continuity** — filled-median vs existing-median per symbol; flag >3× / <0.33× and EXPLAIN
   each. Benign causes seen: growth, decline, demerger, and a median dragged to ~0 by later
   defunct-era zeros. None of ~40 flags this session was a real error, but each was checked.
4. **Cell-level diff vs origin before commit**: expect exactly N fills, **0 strays, 0 overwrites**.

## 4. Traps that cost time this session (do not re-learn these)

- **`_apply_reads.py` WRITES BY DEFAULT.** `--dry` prevents; `--apply` is a no-op flag. Always
  `git reset --hard origin/main` → re-stage → re-apply → diff, so a stale copy can't mask a peer's work.
- **NSE 403 on re-fetch ≠ unreachable.** OSWALGREEN's 2007 pages were already in
  `scripts/_nsearch_cache/`. **Read the cache before declaring a page unreachable.**
- **`--out-suffix` seeds the out file from the shared `_nsearch_reads.json`** (hundreds of symbols).
  Slim it back to your `--only` symbols before committing, or provenance is garbage.
- **Wrong ENTITY inside the right bundle**: a subsidiary's results filed under the parent's scrip
  (UJJIVAN→Ujjivan SFB; COFFEEDAY→Coffee Day Global). Disambiguate by **CIN / entity name on the audit
  report**. A dividend-recommendation filing is the listed parent's.
- **Wrong COMPANY by name**: OSWALGREEN vs Oswal Agro Mills (`sc_id OAM`, PATs -4.23/0.45). Gate on ISIN.
- **Round stored PATs are not automatically defects** — DALMIABHA's filings print whole crores.
- **Never heal on ONE disagreeing reader.** OSWALGREEN Mar-2007 (single reader 23.04 vs stored 23.69,
  unaudited Q4) was deliberately LEFT OPEN. That is the correct outcome, not a failure.
- **Held cells assert absence** (e.g. RAIN 2014-09 con) — they must stay absent or CI reds.
- A peer's MC-sourced fills are **MC-vintage (tier-B)**; an as-filed read outranks them — overwrite freely.

## 5. Defect classes to expect (each anchor refusal is a candidate)

- **§108 restated-vintage** — NSE keeps BOTH vintages; **earliest `filingDate` = as-filed** (§109a).
  Healed this session: DALMIABHA Sep/Dec-15, MTNL Mar-16, ASHIANA FY16 (whole row), THOMASCOOK,
  OSWALGREEN Dec-14. Pre-2009 predates Ind-AS, so expect FEWER of these — but re-filings still exist.
- **Basis duplication/swap** — std slot holding the con value or vice-versa (LICI ×5). If one slot is
  wrong, **check the other basis AND the revenue slot for the same quarter** (JSL Jun-22 was double).
- **Plain wrong value, no reader support** (JSL, EQUITASBNK, MINDACORP, BLUESTARCO).
- **Impossible arithmetic** — BLUESTARCO's 25.57 on PBT 63.48 implies a 60% tax rate ⇒ defect.
- **Whole-row contamination** — ASHIANA FY16 had all four quarters wrong; check siblings.

Heals go through ledgers, never raw edits: `fund_cell_fix.json` + `apply_fund_cell_fix.py` (npStd/npCon,
`basis` key matters) and `revop_cell_fix.json` + `apply_revop_cell_fix.py` (revStd/revCon). Both are
`was`-guarded and idempotent. State the primary document AND a second independent source in `why`.

## 6. Working loop

1. `node --max-old-space-size=8192 scripts/build_coverage_matrix.js --explain nifty-500 \
    --explain-from 2002-01-01 --explain-to 2008-12-31 --explain-out <out.json> --out <dir>` (~3 min).
2. Enumerate missing cells (npStd present, revStd absent) for the named symbols.
3. Batch via MC → validate 4 gates → commit → push (rebase loop) → **verify on origin by content**.
4. NSE archive for MC-less / renamed symbols; MC-raw nil check; filing reads for the rest.
5. Re-measure with `--explain` and report the honest number **plus the PAT-row ceiling**.
6. Record every un-fillable cell in `scripts/_campaign_suspects_2026_08_24.json` with the exact next step.

## 7. Known-open from the 2009+ campaign (do NOT re-hunt; different era)
OSWALGREEN 2007-03 (needs 2nd reader — **this one IS pre-2009, in scope**), AJMERA 2009-03 (no source),
ABSLAMC 2020-06 + AETHER 2020-12 (DRHP-only).

---

# ✅ CAMPAIGN RUN 2026-08-26 — 1,662 cells landed. READ THIS BEFORE RE-OPENING ANY ROUTE.

**All numbers below were measured, and every landed cell was verified BY CONTENT on origin.**

## What landed
| batch | route | cells |
|---|---|---|
| 1 | MC deep feed, era gate (>=3 agreeing / 0 disagreeing within 2002-2008) | 450 |
| 2 | NSE archived filing pages | 165 |
| 3 | NSE archived pages after the `R_PAT_SIGNED` dead-code fix | 95 |
| 4 | MC era gate, re-run against the std-PAT campaign's new anchors | 64 |
| 5 | MC per-cell **window** gate (+/-2yr, >=5 agreeing / 0 disagreeing) | 519 |
| 6 | MC window gate widened to >=3 agreeing | 370 |
| — | **retracted** ANANTRAJ 2008-03 (see suspects) | −1 |
| | **TOTAL** | **1,662** |

By year: 2000:178 2001:344 2002:282 2003:179 2004:213 2005:71 2006:45 2007:248 2008:102.

## The scope correction that matters most — enumerate ROOT CELLS, not a date window
`revStd` at a month-end is "the latest quarter announced on or before that date", so a Jan-2002
month-end is served by a **2001** quarter. The original 2002-2008 framing could not see them.
Re-enumerated as root cells (any anchored quarter with no revStd, no date floor) the gap was
**2,441 across 353 symbols, 1,067 of them in 2000-2001**. The store floors at 2000; nothing earlier
exists. Always re-enumerate this way.

## Coverage AFTER (N500 point-in-time, `build_coverage_matrix.js --explain`, 2026-08-26)
| year | revStd | patStd = **the ceiling** | revStd as % of its ceiling |
|---|---|---|---|
| 2002 | 20.4% | 43.4% | 47% |
| 2003 | 63.7% | 75.6% | 84% |
| 2004 | 74.2% | 86.5% | 86% |
| 2005 | 85.3% | 92.5% | 92% |
| 2006 | 94.5% | 96.0% | 98% |
| 2007 | 95.6% | 96.8% | 99% |
| 2008 | 97.1% | 97.8% | 99% |

2006-2008 are at their ceiling — nothing meaningful is left there. **2002 is the weak year and it is
NOT a revenue problem**: revenue cannot exceed PAT coverage, and 2002 PAT is 43.4%. The binding
constraint is missing fundamentals ROWS. patStd itself moved (23.7% -> 43.4% in 2002) because the
concurrent std-PAT campaign was creating rows throughout; **do not attribute that to this campaign.**

## Route verdicts — MEASURED, do not re-litigate without new evidence
* **Moneycontrol deep feed = the workhorse, and its revenue reach is HIGHEST in the deep era**:
  it carries a revenue value for 86.5% of 2000 root-gap cells and 86.1% of 2001, falling to 68.7%
  (2004) and 29.4% (2008). The claim "MC is a PAT source, not a revenue source, pre-2009" is FALSE
  and comes from checking `rev_total`: the Clause-41 label "Total Income From Operations" only
  starts ~2008-06, while "Net Sales/Income from operations" is present back to 1997.
* **NSE archived filings floor at 2005-03-31** — measured across all 219 then-gap symbols, earliest
  filing per symbol min AND median both 2005-03-31. The route CANNOT reach 2002-04. It closed
  2007-08 almost completely.
* **Trendlyne is dead for this era** — `quarterlyDataDump` returns 13 quarters, 2023-06 onward
  (measured on CENTENKA, RELIANCE, NATIONALUM, FEDERALBNK). Do not re-probe.
* **MC carries NO revenue row at all for BANKS pre-2009** (only PAT and Depreciation; measured on
  FEDERALBNK, KTKBANK). The "Interest Earned" convention the plan expected is simply absent here,
  so banks are an NSE/filing-route problem, never an MC one.

## The gate, and why the obvious one is wrong
MC's pre-2009 "Net Sales" is **gross of excise duty** for many manufacturers while our stored revStd
is net — CENTENKA 0.82-0.86, LINDEINDIA 0.90, RELIANCE 0.91, NATIONALUM 0.92-0.94. Store-wide MC
reproduces our stored pre-2009 revStd on only **63.6%** of 4,430 overlapping cells. A whole-history
convention vote (what `_mc_batch_fill.py` does, correctly, for 2009+) would have written gross
figures into net series.

Calibrated leave-one-out over 398 (symbol, field) series, scoring the question a fill actually asks:

    whole-era, >=3 agreeing, 0 disagreeing      precision 0.918  recall 0.293
    +/-1yr window, >=2 agreeing, 0 disagreeing  precision 0.947  recall 0.565
    +/-2yr window, >=5 agreeing, 0 disagreeing  precision 0.986  recall 0.648
    +/-2yr window, >=3 agreeing, 0 disagreeing  precision 0.983  recall ~0.65

The whole-era vote is weakest because **our own stored pre-2009 series is multi-route** (wayback
2002-04, NSE archive 2005-07, detres 2008+) and changes convention mid-era. Cross-sub-era transfer
is only ~0.86 precise in both directions, so never carry a symbol verdict across eras.
⚠️ Two earlier calibrations returned 0.39 and 0.87 and were both MEASURING THE WRONG THING — they
scored a symbol as "MC's line is not ours" if ANY quarter differed, condemning series that differ on
one restated quarter in twenty (MARICO, ABB, SIEMENS sit at 0.92-0.94).

## Bugs found (all fixed, all in the commit log)
1. **`R_PAT_SIGNED` was dead code in `_nse_archive_revop.py`** — defined for
   "Net Profit (+) / Loss (-) for the period" and never called, while `R_REV_SIGNED` in the same
   block WAS wired in. That spelling is the DOMINANT one in the 2005-08 archive (128 of 148 pages).
   Worth +95 cells. Its own comment described behaviour the code lacked — the alibi.
2. **This campaign's stager inherited the `fin` flag symbol-wide** and propagated store
   contamination (BALRAMCHIN, a sugar company, carries fin=1 on 63 of 100 stored rows). It caused a
   real overwrite, caught by the per-batch cell-level diff. The route now asserts fin=0 = "no
   evidence". 121 cells in batches 1/4/5 still carry the inherited flag — logged in the suspects file.
3. **The gate was confirming its own output** — a cell filled from MC agrees with MC by
   construction and then votes for its neighbour. Measured: 43 cells staged with campaign fills in
   the evidence set, 2 with them excluded. The stager now excludes everything in `_mc_reads.json`.

## Self-audit of the 1,402 surviving MC cells (evidence set excludes every campaign fill)
1,045 retain independent support · 352 have no independent evidence either way (2000:160 2001:72
2002:92 — the deep era, where the store has almost no pre-existing revenue) · 6 materially
contradicted, of which 5 were explained and kept and **ANANTRAJ 2008-03 was retracted**.
The 352 are the first thing to re-verify if a second pre-2009 reader ever appears.

## What is left, and the only routes that could touch it
Remaining root gap **1,583 cells / 221 symbols** (2000:193 2001:352 2002:397 2003:231 2004:281,
plus 129 in 2005-08). Classified:
| cells | class | reachable by? |
|---|---|---|
| ~789 | MC's revenue line materially DISAGREES with our convention (the excise class) | needs a primary document; no current route reaches 2000-04 |
| ~392 | MC carries no revenue row (banks, holdcos) | NSE archive only from 2005; filings otherwise |
| ~217 | neighbours agree but too few to gate | a second reader would settle these |
| ~144 | no stored neighbour inside the window | ditto |
| ~119 | MC has no such quarter / no MC table | different publisher |
**The only untried publisher for 2002-04 is BSE's archived website** — `PRE2015_CAMPAIGN.md`
"STEP B candidate", scoped 2026-08-07 and never built. STEP W (wayback NSE `results.jsp`) is
EXHAUSTED: 2,319 landed / 2,174 refused / 0 open. Do not re-run sweeps against it.

**Do not re-grind the MC route.** A final sweep with self-confirmation excluded stages exactly 2
cells, and both are cells this campaign deliberately refused (BIRLACABLE 2001-12, DSKULKARNI
2006-06). It has converged.

---

# ⚠️ POST-CAMPAIGN AUDIT vs THE ARCHIVED NSE EXCHANGE PAGE (2026-08-26, same day)

An independent as-filed reader for this era — `scripts/wayback_nse/` — appeared after the batches
landed, so this campaign's own output was re-audited against it. **It contradicts 20% of the cells
it can adjudicate.** Read this before trusting any aggregator-derived pre-2009 revenue cell.

## The numbers, with their denominator
| | cells |
|---|---|
| MC-derived revStd this campaign landed | 1,402 |
| …have an archived NSE `results.jsp` page ENDING on that quarter | 264 |
| …**ADJUDICABLE** through the full gate | **90** |
| **AGREE** | **72** |
| **DISAGREE → all 18 healed to the archive** | **18 (20.0%)** |
| have **NO archived page at all** — neither confirmed nor contradicted | **1,138** |

Refusals among the 264, every one classed: 119 period not 3 months · 21 EPS-untestable · 20 the
page's own arithmetic does not close · 8 the archive captured a `NullPointerException` **server
error** instead of a page · 3 unreadable · 2 Consolidated · 1 Cumulative · 0 fetch failures.
**Do not extrapolate 20.0% to the other 1,324.** 1,138 cells having no page is a gap in the
evidence — never report it as agreement.

## The mechanism — why a store-based gate could not catch this
**MC's own pre-2009 series switches revenue definition BETWEEN ADJACENT QUARTERS of the same
symbol.** ITC: MC gives **2371.66** (Jun-01) and **2542.82** (Dec-01) but **1206.72** (Sep-01),
while the exchange page reads 1047.89 and 1195.27 — a gross-of-excise line on two quarters and the
net line on the third. The ±2yr convention gate compares MC against **our store**, and for ITC every
non-campaign stored neighbour in the window *also* came from MC and agreed with it.
**MC-vs-MC proves identity and catches scale/entity errors; it cannot arbitrate vintage or
definition.** Only an exchange-native reader can.

## ⚠️ THE CALIBRATION WAS CONTAMINATED — both numbers, as required
The gate calibration ran *after* batches 1-4 had landed, so the truth side already contained this
campaign's own MC fills, which agree with MC by construction. Re-run on the original 426-symbol
universe with every campaign MC cell removed from **both** the evidence and the truth side:

| gate | as originally run | **clean** |
|---|---|---|
| ±2yr, ≥5 agreeing, 0 disagreeing | 0.989 | **0.983** |
| ±2yr, ≥3 agreeing, 0 disagreeing | 0.987 | **0.980** |

False-positive counts are *identical* (52 and 67); contamination padded the numerator with ~1,765
trivially-true fires. So the inflation was ~0.6pp — **but that whole family of numbers measures
agreement with our own store, and the archive says the real error rate where an as-filed reader
exists is 20%.** A hold-out is only a hold-out if the truth side is independent of the work being
validated, and in a shared store that stops being true within hours.

## How much of the self-audit was really MC-vs-MC — measured
Of the 1,402 cells: **933 were supported entirely by exchange-derived neighbours** (STEP D/N/W
wayback / NSE-archive / BSE-detres reads), 53 mixed, **53 supported only by aggregator-derived
neighbours**, and 363 had no independent support at all. The pre-2009 *revenue* store is 83%
exchange-derived (9,823 of 11,807 cells from the STEP D/N/W ledgers) — so the revenue lane was less
MC-vs-MC than the PAT lane, but ITC shows 53 cells is enough to hide a 2× definition error.

## Defect classes among the 18 — tested, not assumed
* **cumulative-in-quarter**: TESTED (ours vs archive + our own preceding 1/2/3 quarters) → **REJECTED for all 18**
* **gross-income** (ours = the page's Net Sales + Other Income): TESTED → **HOLDS for 2** (VESUVIUS 2001-06, WIPRO 2001-06) — a revenue-DEFINITION defect, and the ledger says so rather than defaulting to "vintage"
* the remaining 16 split by direction: 8 sit ABOVE the page (consistent with a gross-of-excise line the page does not print; ITC extreme at +126%) and 8 BELOW it, which excise cannot explain and which is therefore restated vintage

## Reader/gate notes for the next session
* `wb_read.parse()` already reads `net_sales` — it needs `/div` applied by the caller, the same way `pat_cr` is derived.
* Added guard worth keeping: **G2a — the page's OWN declared period end must equal the quarter audited**, rather than trusting the index key.
* `NullPointerException caught: null` pages render a valid header (period, type, scale) with a `null` body. They are a **captured server error**, not a filing without revenue — class them separately or they inflate "no data".
* The peer caches at `~/stocks-wt/pre2015-stepw-harvest/scripts/_wb_cache` (4,702 files) and `~/stocks-wt/pre2009pat-bc68c8d0/scripts/_wbnse_cache` (1,290 gz) served **135 of 264** pages by exact timestamp+URL match. Check them before fetching — Wayback throttles to roughly 2 pages/min sustained.

---

# 🔴 THE ARCHIVE AUDIT SHOULD BE A STANDING GATE, NOT A ONE-OFF (2026-08-26, extended)

## Combined result across both widenings
| population | archived pages | adjudicable | agree | **disagree** |
|---|---|---|---|---|
| this campaign, direct true-quarter pages | 264 | 90 | 72 | **18 = 20.0%** |
| this campaign, recovered by **cumulative differencing** | 83 | 30 | 23 | **7 = 23.3%** |
| **earlier** campaigns' aggregator revenue cells | 9 | 4 | 2 | 2 *(n=4 — a count, not a rate)* |
| **total** | | **124** | **98** | **26 = 21.0%** |

25 healed; **ITC 2001-09 refused** because its PAT cross-check was inside 3% but not exact.
The rate is ~20% under **two independent reading methods**, so it is not an artifact of one route.

## ⚠️ The gate cannot detect this itself — the inference is invalid, not the evidence
I first explained the 20% as contamination (evidence made of MC cells). **That was wrong and is
withdrawn.** Measured, with this campaign's own fills excluded so it reflects what the gate saw:

| what the gate could see | agree | disagree | |
|---|---|---|---|
| evidence **ALL exchange-derived** | 71 | 17 | **19.3% wrong** |
| evidence includes earlier aggregator cells | 1 | 1 | |

**88 of 90 had genuinely independent as-filed evidence and the gate still passed 17 wrong values.**
"MC reproduces our as-filed values on the neighbouring quarters" does **not** imply "MC's value for
THIS quarter is as-filed" — MC's series changes definition and vintage *per quarter* (ITC prints
2371.66 / 1206.72 / 2542.82 across three consecutive quarters against page reads of 1047.89 and
1195.27). Neighbour agreement is not transitive to the cell you are writing.

## Calibration, three ways — the third is the real one
| | ±2yr ≥5 | ±2yr ≥3 |
|---|---|---|
| as run | 0.987 | 0.985 |
| campaign-excluded | 0.983 | 0.980 |
| **aggregator-excluded (glob, all campaigns)** | **0.982** | **0.978** |

The provenance filter barely moves it because **pre-2009 std revenue is only 14.4%
aggregator-derived** (1,706 of 11,807) and 1,402 of those are this session's — an asymmetry with
the PAT side, verified by checking the glob for silent skips (`agg_cell_fills`, `mc_quarterly_fills`,
`screener_*`, `sweep_rev_fills`, `annual_derived_fills`, `named_rev_cell_fills` contribute **zero**
pre-2009 std cells; `mc_history_fills`/`mc_fyident_fills` contribute 25 and 59, all `con`).

**A store-based hold-out cannot close this gap in principle.** It scores quarters where we ALREADY
hold a value; the gate WRITES quarters where we do not. Measured lower bound 2.2%, actual 21%.
That is `calibrate_gate.py`'s own documented caveat, quantified.

## RECOMMENDATION — make it a gate, run BEFORE landing
**Yes. Any aggregator batch touching pre-2007 should intersect its proposals with
`scripts/wayback_nse/_wb_index.json` and adjudicate the intersection BEFORE committing**, not after.
Run after, it is a heal campaign; run before, it is a gate. Concretely:

1. Intersect proposals with the index; adjudicate through G1–G5 **plus G2a** (the page's own
   declared period END must equal the target quarter) and cumulative differencing.
2. **Land only cells that AGREE or have no page**, and record the no-page cells as *unevidenced* —
   never as agreement.
3. **Report the measured disagreement rate as the batch's own error estimate.** This is the real
   value: even though only 264 of 1,402 cells (19%) had a page, that subset *measures the batch*.
4. Cost is no longer a reason not to: `wbcache.py`'s keep-alive serial fetcher did 48 pages with
   **0 transport failures** at ~1s/page, and the three local caches make most re-reads free.

⚠️ **The honest implication, stated as an implication.** If the 21% on 124 adjudicable cells
generalises to the 1,278 of my cells the archive cannot reach, roughly **270 more wrong cells are
still in the store and only 26 have been found**. That is an assumption, not a measurement — the
adjudicable subset skews to large, well-archived companies — but it is the right assumption to plan
against, and it is why the audit belongs in front of the batch rather than behind it.

---

# ✅ REV-PARITY RUN 2026-09-05 — 456 cells landed; the BSE archive (STEP B) is BUILT; routes for 2002-04 are exhausted. Runbook §131.

Target restated as **N500 point-in-time member-quarters holding a std PAT but no revenue** (not root cells).
Against origin/main 943a43e89: **863 → 535** on the same fund file; the concurrent std-PAT landing (9286e9527,
+1,538 PAT cells) opened 170 new gaps, of which 128 were filled → **577 open on the day's fund**.

| route | cells | gate / calibration |
|---|---|---|
| wayback NSE `results.jsp` — `wayback_nse/wb_rev.py` (direct + cumulative differencing + predecessor-symbol aliases) | **331** | page PAT == stored to the paisa; hold-out 464 / 0 mismatch |
| NSE archive `_nse_archive_revop.py` (2005+) | **73** | PAT anchored; residue = empty/404 detail pages |
| **BSE archived results `qresann/result.asp` — `wayback_nse/bse_rev.py` (NEW)** | **38** | PAT anchored; revenue line CHOSEN BY REPRODUCTION (Gross vs Net Sales); LOO hold-out 91 / 0, 34 refused |
| Moneycontrol under §81e A-A5, exchange-derived anchors only | **15** | 433 open cells refused — the excise class; converged (+1/+1 after new anchors) |

**Coverage after (N500, revenue / PAT ceiling, today's fund):** 2002 39.0 / 50.9 · 2003 68.5 / 74.9 · 2004 59.4 /
68.0 · 2005 93.8 / 94.5 · 2006 95.9 / 96.6 · 2007 96.8 / 97.8 · 2008 98.3 / 98.9 · 2009+ ≥98.8.

**What is left (577), tagged per cell in `scripts/wayback_nse/rev_parity_open_2026-09-05.json`:** 433 MC-gate-refused
with no archived exchange page (2002: 168, 2003: 96, 2004: 132) · 68 pre-2009 banks (MC has no bank revenue row, no
page) · 16 PAT-side contradictions (`wb_rev_findings.json`) · 16 consolidated-only captures · 14 NSE-archive dead
pages · 11 bank cells with a BSE page but no neighbour to fix the line · 19 misc.
**Do not re-grind MC for these; the archived-exchange publishers are indexed and read. The next lever for the 433
is a PAT-side one (a filing read) or a new publisher, none known.** The 16 findings are the PAT campaign's queue.

Traps: `wb_read.parse` cumulative substring bug (fixed); BSE `Gross Sales` vs `Net Sales` per symbol; BSE
integers-in-million → half-grid tolerance; 81 cells under OLD fund keys; a parallel measurement overwrote the gap
file; rebase conflicts on the store JSONs → reset + replay, never hand-merge.

**PAT-side follow-up (same day, runbook §131f):** the 16 contradictions were adjudicated — 11 healed to the PRINTED
as-filed quarter via `fund_cell_fix.json`, 13 held (RELIANCE merger-entity ×3, UMIYA-MRO single-document ×3, 7 with no
printed quarter page), 2 not defects. Rule: a cumulative-minus-legs derivation is NOT the as-filed quarter when the later
filing restated a leg (ORIENTBANK Mar-03 Q4 page 119.49 vs derived 119.63) — only printed quarters are written.
