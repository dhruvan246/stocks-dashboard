# PLAN — STANDALONE PAT + FII/DII HOLDINGS to 100%, Nifty 500 point-in-time, 2002-01 → date

Written 2026-09-05 (Fable). User asks, verbatim: *"check coverage of std pat from 2002 till date and make a
plan to fill them"* · *"along with that check fii and dii holding as well"* · *"also check NA ones for pat dii
and fii"* · *"they may be fillable"*.

> **Golden rules (§0, §57d, §112):** never assume, never guess. Every number below was MEASURED this session
> (source named beside it). A route returning nothing means THAT route has no row. Report `have`, never
> `missing`, for any pre-2015 era — in a thin era the N/A layer rests on our own capture gap and a fill
> RAISES "missing" (§112). Nothing is marked N/A without the user.

**Queue (committed):** `scripts/stdpat_shp_cov_queue_2026-09-05.json` — 7,782 std-PAT root cells + 3,110
SHP root filings, each tagged with every committed-ledger record found for it. Regenerate with the recipe
in §6. **Worktree for execution:** cut a fresh `~/stocks-wt/stdpat-<session>` from origin/main; never the
shared checkout.

---

## 0. Sources of every number here

* Coverage: `docs/coverage/nifty-500.json` LIVE (`dhruvan246.github.io/stocks-dashboard/coverage/`, bake
  **2026-09-04 03:30 IST, dataEnd 2026-09-03**), byte-identical to origin/main `8a77f3f1c`. A local
  `build_coverage_matrix.js --explain nifty-500` bake on the release bin reproduced it with **0 diffs**
  on every shared date for all 9 params below; the roster reproduction from `indices_history.json`
  matched `members` on all 296 dates.
* N/A cells are named per date by the builder's new `na:<param>` explain lists (this commit; analysis-only,
  payload counts unchanged — verified bake-vs-bake).
* Stores: `docs/sf_fundamentals.json` (3,958 symbols), `scripts/shp_history.json` (2,756 symbols,
  earliest row **2002-12-31**), both at `8a77f3f1c`.
* Every "route reach" claim below was probed live this session (§4) or cites the runbook § that measured it.

## 1. Coverage as it stands (N500 point-in-time, 296 month-ends 2002-01-31 → 2026-08-31)

`have / N-A / holes` are member-months; `%` = have ÷ (members − N/A), the page's own formula.

| param | have | N/A | holes | % | what it is |
|---|---|---|---|---|---|
| **patStd** (a std PAT visible at the date) | 141,702 | 5,067 | 1,266 | 99.11 | the raw input |
| profitYoyStd | 134,499 | 4,514 | 9,022 | 93.71 | needs the same quarter a year earlier |
| profitTTMStd | 124,892 | 5,073 | 18,070 | 87.36 | needs 8 consecutive std quarters |
| profitStreakStd | 134,499 | 4,514 | 9,022 | 93.71 | = YoY's window |
| **fiiPct / diiPct** | 139,420 | 7,422 | 1,193 | 99.15 | latest visible SHP filing |
| **fiiChgPp / diiChgPp** | 134,667 | 9,081 | 4,287 | 96.91 | needs the PRIOR quarter's filing too |

By year (% of members−N/A; the thin years are the whole story):

| year | patStd | TTMStd | fiiPct | fiiChgPp | note |
|---|---|---|---|---|---|
| 2002 | 77.4 (1,885 N/A) | 26.4 | **0.0** (4,630 N/A + 701 holes) | 0.0 | SHP store starts Dec-2002; 569 member-months have NO PRICE ROW (§93 era orphans) |
| 2003 | 94.1 (1,127 N/A) | 31.4 | 96.4 | 69.3 | fiiChgPp needs the 2002 prior quarter |
| 2004 | 98.1 (706 N/A) | 39.9 | 97.8 | 94.6 | |
| 2005 | 99.5 (419 N/A) | 48.0 | 98.7 | 94.7 | |
| 2006 | 99.7 (221 N/A) | 49.7 | 99.6 | 96.6 | |
| 2007 | 99.7 | 80.8 | 99.5 | 97.3 | |
| 2008 | 99.5 | 85.1 | 99.3 | 94.4 | |
| 2009 | 100.0 | 95.5 | 99.9 | 98.5 | |
| 2010-15 | 100.0 | 98.4-99.7 | 100.0 | 98.5-99.6 | |
| 2016 | 100.0 | 97.7 | 100.0 | 96.3 | Dec-15/Mar-16 SHP seam (§22f, G4) |
| 2017-26 | 100.0 | 98.4-100 | 100.0 | 99.6-100 | 2022 fiiChgPp N/A 1,491 = Sep-2022 SEBI reclassification, DELIBERATE |

**All-listed universe, for scale (same payload):** patStd 88.7% (39,378 holes + 98,120 N/A),
profitTTMStd 66.9%, fiiPct 87.1% (43,576 holes + 111,257 N/A). This plan scopes **N500 first**; the
same queues can be re-derived for `all` with the §6 recipe (decision item §7.2).

## 2. The N/A cells — measured, and mostly OUR gap, not the world's

The basis + std families carry exactly ONE N/A rule (`build_coverage_matrix.js` ~L975-1007): *"the
symbol's first REAL announce date is later than this month-end"* (`nothingPublicYet`) plus the per-name
ledger (`coverage_na_ledger.json` has **no** patStd/fiiPct/diiPct entries). fiiPct/diiPct carry the sibling
rule *"first SHP submission is later than this date"*. Both read **our own store's oldest row**. Classified
every N/A cell by the symbol's first traded bar (engine `--facts`, same bin):

| param | N/A total | company trading ≥365 d before the date (**capture gap**) | listed <365 d before (plausible nothing-public-yet) |
|---|---|---|---|
| patStd | 5,067 | **4,868** (2002-08: 4,451 · 2009-14: 401 · 2015-19: 16) | 199 |
| fiiPct / diiPct | 7,422 | **7,043** (2002: 4,614 · 2003-08: 2,186 · 2009-15: 228 · 2016+: 15) | 379 |
| fiiChgPp / diiChgPp | 9,081 | 8,536, **of which 1,458 in 2020+ are the Sep-2022 suppression + pre-listing priors (keep)** → ~7,078 | 545 |

So the user's instinct is right: **96-97% of the N/A on these columns is a company that was listed and
trading for over a year with nothing in our store yet.** Companies were filing quarterly results and
quarterly shareholding patterns in that era (BSE's own archive serves both back to Mar-2001, §4.1); the
N/A is an inference from our capture, the §98/§112 class. These cells are IN the queues below.

⚠️ Builder consequence (decision §7.3): as the store fills, these N/A cells turn into visible holes
first and `have` second — judge progress by `have` only (§112b).

## 3. Root cells — what actually has to be fetched

### 3a. Standalone PAT: **7,782 root (symbol, quarter) cells / 1,003 symbols**
Fix-point enumeration (fill → re-derive → repeat, converged in 4 iterations) over every N500 member-month
that lacks patStd / profitYoyStd / profitTTMStd — N/A cells included, treated as needing the quarter a
compliant filer would have published (qe+45 d, Mar +60 d). The reach-back is why **1999-2001 holds 3,363
of them** (a 2002-01 TTM needs quarters back to 1999-12; §112c enumeration trap).

| quarter-year | 1999 | 2000 | 2001 | 2002 | 2003 | 2004 | 2005 | 2006 | 2007 | 2008 | 2009-14 | 2015-24 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| root cells | 421 | 1,503 | 1,439 | 1,293 | 684 | 823 | 324 | 304 | 141 | 67 | 429 | 354 |

Against the store: **5,753** are older than the symbol's first stored row · **1,196** are holes inside a
series · **564** belong to ~50 symbols with NO fundamentals row at all (the §112g B2 class: SB&TINTL,
YOKOGAWA, SQRDSFWARE, WELLWININD, ITCHOTEL, JINDALFOTO, COMPUDYNE, GLOBLTRUST…) · 26 are rows with a
null std slot. Worst symbols by count: PFIZER 45, SUMMIT 33, BALLARPUR 32, THOMASCOOK 29, DCMSHRIRAM 28,
HLVLTD 28, BEML 26, BANKRAJAS 26, ALOKTEXT 25. By N/A member-months: SPLPETRO 159, HFCL 143,
HINDMOTORS 115, PFIZER 112, AGCNET 101, ASIANHOTNR 97, SUMMIT 76, COLPAL 70.

What the committed ledgers already say about them (tag column in the queue):

| tag | cells | meaning |
|---|---|---|
| `no-ledger-record` | 6,529 | no committed record of any attempt. ⚠️ NOT "never tried": the FAV14 pre-2009 MC pass (§112, PLAN_FAV14_PRE2009 §3: 3,386 open = REJECT-GATE-E 2,650 · NOT-FOUND 448 · UNRESOLVED 288) kept its per-cell verdicts in that session's scratch. Re-running `mc_era.py` is cheap and cached — do it, don't trust memory. |
| `WB-page-indexed` | 885 | an archived NSE `results.jsp` page ENDS on that quarter (`wayback_nse/_wb_index.json`). **213 in 2000-01 were indexed and never requested** (§112e). The 651 in 2002-04 are STEP W's refusal classes A-D — do NOT blind re-sweep; re-read only through `wbgate` with the refusal class in hand. |
| `mc-closed:*` | 338 | FAV14 2009+ MC walk-to-the-wall (wrong-entity 173, MC lacks symbol 119, restatement-held 29, off-cycle-fiscal 17). Next rung is as-filed, never MC again. |
| `in-a-fill-ledger-but-absent` | 40 | a fill ledger claims the cell, the store lacks it. Check first (alias key? retracted? applier never re-run?). |

Ceiling, measured: the ~50 no-row symbols and the **656 member-months / 89 symbols with no price row**
(569 of them Jan-Sep 2002 — modern roster keys over era tapes, §93/§95/§106, UNOWNED) cap every 2002
number regardless of fundamentals work. State it; do not promise 100% on 2002 until that class has an owner.

### 3b. FII/DII: **3,110 root SHP filings (symbol, quarter-end) / 900 symbols**
Union of: fiiPct N/A capture-gap cells → the quarter a compliant filer would have filed (qe+28 d, the
engine's e13 fallback) · fiiChgPp holes where the PRIOR quarter's row is absent (1,040) · fiiPct holes on
38 symbols with no SHP row at all (192). Sep-2022 suppression quarters excluded.

| class (vs `_shp_aspx_rejects.json`, `_shp_bse_absent`, `_shp_nse_absent`, frontier floor) | filings | next rung |
|---|---|---|
| **pre-Dec-2002 — outside the aspx frontier, NEVER REQUESTED** (Mar-2001..Sep-2002; 538 symbols, 466 resolve to a BSE code) | **1,617** | WP-S1 — route measured live §4.1 |
| in-frontier, scripcode resolves, no refusal record | 636 | WP-S2 harvest |
| aspx refused `absent` (page had no category rows) | 535 | WP-S2 rung 2+ |
| in-frontier, scripcode UNRESOLVED (era names) | 152 | ISIN rung (`isin_sources_fetch.py`, `_shp_scripcode_override`) |
| aspx refused recon 67 · zero-vs-neighbour 20 · no-prom 18 · no-fii 9 | 114 | per-page adjudication (§22i/§22j rules) |
| 2016+ (XBRL-era untried 51 · bse-absent 5 · no record 30) | 86 | BSE `SHPQNewFormat` XBRL / NSE master; WP6's held-160 list |

By year: 2001: 357 · 2002: 1,330 · 2003: 265 · 2004: 223 · 2005: 153 · 2006: 133 · 2007: 135 ·
2008: 141 · 2009-15: 285 · 2016+: 88. Worst symbols by N/A member-months: OSWALGREEN 121, LAKSHVILAS 111,
IDEA 105, ASIANHOTNR 81, HFCL 81, KIRLOSOIL 81, SUMMIT 81, SUNDRMCLAY 69, NAHAREXP 60, SUNDARMFIN 60.
Compare the independent `audit_shp_coverage.py --year` (quarter-level, N500): 2002-12 85.9%, 2003 87.4%,
2004 87.0%, 2005-07 ~90%, 2008 93.3%, 2009+ ≥98.7%, total 96.9% — same shape, coarser unit.

## 4. Routes — measured this session

### 4.1 ★ BSE `ShareholdingPattern.aspx` `Flag=Old` SERVES Mar-2001 → Sep-2002 (the whole never-requested era)
`fetch_shp_bse_aspx.py::cmd_frontier` hard-floors its quarter list at `"2002-12-31" <= q` (L88-89) — that
is the ONLY reason the store starts Dec-2002. §22f had already recorded RIL Mar-2001 served under
`Flag=Old`; nobody moved the floor. **Pilot 2026-09-05:** 8 root symbols (KPIL, SHASUNPHAR, THERMAX, TV18,
CESC, IDBI, TAJGVK, NELCO) × 7 quarters (qtrid 29-35) = 56 fetches → **54 pages served a 1997-format
table** (47 with an FII row, 7 with the FIIS row absent = the tool's proven-zero rule), **2 transient
fetch failures** (KPIL/NELCO Jun-2001, retry). IDBI Dec-2001→Sep-2002 fii 0.46/0.40/0.34/0.28, mf ~7.4,
lump ~12.0, prom 58.47 — a smooth series. Pages cached under the session scratchpad `aspx_pilot/cache/`.
qtrid is global: `(year−2001)×4 + {Mar:29, Jun:30, Sep:31, Dec:32}` (§22f).

### 4.2 NSE's OWN archived shareholding pages exist in Wayback — but they are a `>1% holders` list
CDX enumeration (`marketinfo/companyinfo/eod/shareholding1.jsp`): **7,043 captures, 642 symbols, 3,920
distinct (symbol, as-on-date) pages**, period-years 2002: 777 · 2003: 1,872 · 2004: 1,139 · 2005: 110 ·
2006: 22; **433 of our SHP root filings have one** (2002: 237, 2003: 130, 2004: 63). Fetched ESSARGUJ
30-Sep-2002: the page lists *"Entities / Persons holding more than 1%"* with category sub-totals
(Banks/FIs/Insurance 3.07, FIIs 1.89, NRI/OCB 1.15, grand total 29.16) — **NOT the full category-wise
pattern**. Category sub-totals are FLOORS of the filed totals. → Usable as a **second reader / floor gate**
(e.g. to refute a fabricated `FII 0.00` — §22f seam class — or to corroborate an aspx value), never as a
fill source. `shareholding.jsp?symbol=X` is only the per-symbol date list. CDX lists saved:
scratchpad `cdx_shareholding1.txt` (7,043 rows) / `cdx_shareholding.txt`. Wayback CDX 503s intermittently —
retry with 20 s back-off, never conclude from one call.

### 4.3 Standalone PAT routes (all previously measured; reach re-quoted with its population)
* **Moneycontrol standalone feed via GATE E / E2b** (§90, §112; `agg_tools/mc_era.py → agg_era_gate.py
  --e2b → apply_agg_pat_fills.py`, field token `patS`). Reach measured on the FAV14 pre-2009 set: 585/621
  symbols resolve, 63.8% of gap quarters carry a std PAT; accuracy 0.85% disagreement on gate-passed
  symbols but **11.8% wrong vs the as-filed archive on one batch** (PLAN_FAV14_PRE2009 §2e) → run the
  archive check as a GATE in front of each batch. `mc_era.py`'s ISIN rung never runs when our symbol is not
  a BSE `scrip_id` (§112d) — supply ISIN by our symbol first.
* **Wayback NSE `results.jsp`** (`scripts/wayback_nse/`, 2000-2006, as-filed, period/basis/scale declared
  in text; hold-out 0.00%). 213 of our 2000-01 roots are indexed and unread.
* **NSE results archive** `corporates-financial-results` (2005+, both bases, PAT in detail page; delisted
  names served) — `read_con_pat_nse.py` / `fill2020_tools/con_discover_pre2015.py` pattern; pre-2011
  template returns `no-pat-row` until the label set is widened (§123).
* **BSE detres JSON** (2008Q1+, `QID=85+4*(FY-2015)+{Mar:0,Jun:1,Sep:2,Dec:3}`, `.00` std, Value÷10)
  with EPS recon / FY identity (§52a, FAV14 P4 recipe); delisted codes from the FULL BSE master + ISIN.
* **BSE announcement PDFs** — Nov-2018+ live; pre-2018 via `CorpAttachment/<YYYY>/<M>/<file>` resolved by
  `AnnPdfOpen.aspx` (§reference-bse-attachment-resolver); a quarter is printed in ~3 filings (§113a).
* **Untried publisher for 2002-04:** BSE's archived website (PRE2015_CAMPAIGN "STEP B", scoped, never built).

## 5. Work packages, in execution order

Rules binding every WP: §57d ladder logged per cell (rung + result) · fill-only idempotent appliers ·
provenance in a ledger, never a direct store edit (§38/5) · bake-and-measure with `--explain` before commit
· file-scoped adds from a fresh worktree · push recipe (CLAUDE.md) · LIVE re-verify ~20 min after push.

### WP-S1 — SHP Mar-2001 → Sep-2002 via aspx `Flag=Old` (≈1,617 root filings; whole-era ≈3,500 member-quarters)
**✅ DONE 2026-09-05 02:45 IST — commit `bd094aeea` (runbook §127f).** Frontier 2,863 member-quarters / 479
symbols (112 cells on 21 era names unresolved); harvest 2,409 ok · 438 absent · 16 no-prom; ledger
`scripts/shp_fill_bse_aspx_2001.json.gz`; applied twice = identical; bake: fiiPct/diiPct 2002 0% → 85.5%,
fiiChgPp 2003 69.3% → 86.8%, no date down. Residue → WP-S2 (43 modern-key roster orphans absent on all 7
quarters = §93 class; 56 other absent symbols; 16 promoter-less pages; 21 unresolved era names). The steps
below are the record of what was run.
1. `fetch_shp_bse_aspx.py`: lower the frontier floor to `2001-03-31` (L88-89) and let `cmd_frontier` take
   the point-in-time roster for those quarters (all N500 members, not just the root list — the era is empty).
2. Harvest into an ISOLATED ledger (`shp_fill_2001_aspx.json.gz`, wired into `fetch_shareholding`
   `BSE_HIST_LEDGERS`) — a narrow `harvest` OVERWRITES the shared ledger (FAV14 P2 lesson). Gates as shipped:
   inst-recon 0.15 (single-column layout), proven-zero FIIS rule, prom fallback, `fii` never zero-defaulted,
   zero-vs-neighbour. Prefer 4dp from share counts (PLAN_SHP_4DP_FULL §3 denominator rule).
3. Visibility: rows go out `sub=99999999` and get the engine's qe+28 d fallback (§120) — un-dated by
   design; no §105 real-date route exists before Dec-2013.
4. Apply with `--apply-ledgers`, regenerate `shp_engine.json`, bake, verify fiiPct 2002 (expect 0% →
   ~90% by the pilot's 96% serve rate) and fiiChgPp 2003 (69% → ~95%). Coverage COUNT for Dec-2002+ cells
   must not move (that harvest is done; the 4dp pass pins 88,767).
Owner: the SHP session if one is live (`fetch_shp_bse_aspx.py` was dirty in the shared checkout on
2026-09-05 — check `git status` there first), else this plan.

### WP-S2 — SHP Dec-2002 → Mar-2016 residue (636 untried · 152 unresolved code · 535 absent · 114 refusals)
* 636: run the harvest on exactly this frontier (filtered `run()` call, isolated ledger).
* 152: resolve era names to BSE codes by ISIN (`isin_sources_fetch.py` / `_isin_seam_verdicts.json`), write
  `_shp_scripcode_override.json`, then harvest.
* 535 `absent`: rung 2 = the other `Flag` for the quarter; rung 3 = the archived NSE `>1%` page as a floor
  (§4.2) to decide whether a genuine filing existed; rung 4 = annual report (March quarters only); else
  `not-found-via:<rungs>` in `_shp_aspx_rejects.json` — never "no filing".
* 114 recon/zero/no-prom/no-fii: per-page read with §22i (swallowed foreign block), §22j (2dp literal zero),
  §118 (Any-Other block) rules; the NSE `>1%` FII sub-total refutes a fabricated 0.00.

**WP-S2 STATUS (2026-09-05 ~03:00 IST, session trusting-lewin, worktree `~/stocks-wt/shp-2002`, runbook §127f):
DONE for the untried + unresolved classes.** Ledger `scripts/shp_fill_wps2_aspx.json.gz` = **768 cells / 339
symbols** (Flag=Old 340 · Flag=New 428): 468 point-in-time member-quarters the 2026-08-11 frontier never requested +
264 prior-quarter roots (non-members at that QE — `cmd_frontier` cannot list them; derive from the roots) +
22 cells on names already in `_shp_aspx_resolved_era_syms` (cmd_frontier never reads it) + 82 cells on 12 era
names resolved by the NSE-archived-page share-capital test (9 MATCH ≤0.02%, VARDHMNSPG REFUTED). Refused and
journalled: 58 absent, 5 zero-vs-neighbour, 3 recon, 1 no-fii, **52 `unresolved-scripcode` on 10 names**
(INDOGULF/AGREVOIND ties, WELSPUNGUJ/UNITEDPHOS/VXL/JINDALFOTO no second reader, JINDVIJSTL/JINDLSTRIP no
page, SEARCHEMIN no name, VARDHMNSPG refuted). Quarter-level audit (`audit_shp_coverage --year`): 2002-12
85.9→91.3 · 2003 87.4→91.3 · 2004 87.0→91.4 · 2005 90.6→94.2 · 2006 90.3→93.6 · 2007 89.8→95.3 · 2008
93.3→97.2 · 2009+ unchanged. Coverage-matrix bake: fiiPct +1,133 member-months (N/A −893, 0 dates down), fiiChgPp +1,444 (N/A −889; 3 dates −1 = MUNJALSHOW/VTL's Dec-2002 with no Sep-2002 prior, §112); 2003 fiiPct 96.5→98.1, 2004 97.8→99.0, 2005 98.7→99.7; patStd/price unchanged. Left for WP-S2's next pass: the 535 `absent`
(rung 3 = NSE `>1%` floor page, rung 4 = annual report) and the 114 per-page adjudications.

**WP-S2 PASS 2 (2026-09-05 ~12:00 IST, this session, worktree `~/stocks-wt/wps2-shp`, runbook §127g): DONE for the parser
classes.** The 114 per-page adjudications were four parser blind spots (header-less promoter block, lump-only / no-institution
1997 pages, foreign-note proven zero, Foreign MF / Foreign FI rows) — fixed in `fetch_shp_bse_aspx.py`, re-parsed cache-only:
residue 750 → 317 ok, WP-S1 refusals 454 → 87 ok, + 51 cells on 9 resolved era names 2001-02, + 43 BSE-XBRL cells 2016-2023.
Ledgers `shp_fill_wps2b_aspx.json.gz` (431) + `shp_fill_n500_gaps.json.gz` (+43); 25 continuity holds in `_shp_wps2b_holds.json`
(SBIN 2003 RBI-in-dii suspect journalled). Bake: fiiPct +604, fiiChgPp +884 member-months, 0 dates down. **Left:** 434 quarters BSE
lists no filing for (2001-08 mostly; NSE `>1%` floor / annual reports), 374 roster-orphan modern keys (roster job), 328 api-failed
classifier retries, 40 recon + 26 zero + 7 no-fii refusals (per page), 25 holds, 22 era names without a BSE code.

### WP-S3 — SHP 2016+ residue (86) — XBRL/NSE routes; start from WP6's held-160 list (coverage-fill memory).

### WP-P0 — std-PAT hygiene first (cheap, 66 cells)
The 40 `in-a-fill-ledger-but-absent` cells (alias key? retracted by a later heal? applier not re-run?) and
the 26 null-std rows. Each is either a one-line re-apply or a ledger defect to record.

### WP-P1 — std PAT 1999-2001 roots (3,363 cells)
1. `mc_era.py` on all root symbols (ISIN supplied by OUR symbol — §112d) → `agg_era_gate.py --e2b`.
   Before landing: run `wayback_nse` as the second reader over every proposal it can reach (§2e: 11.8%).
2. The 213 WB-indexed 2000-01 cells: read through `wbgate` (G1-G5) — exchange-native, outranks MC.
3. Re-derive to the fix-point after each batch (a fill moves `firstRealAnn` and opens earlier months).
Ceiling: 2000-01 quarters are needed only as TTM/YoY reach-back; they buy 2002-03 coverage, not rows of
their own.

### WP-P2 — std PAT 2002-2004 roots (2,800 cells; 651 WB-indexed)
MC/GATE E for the N/A-class symbols first (292 symbols whose store starts late — verify per cell whether
the FAV14 pass already refused them: the tool's cache answers in seconds). WB pages only via `wbgate` with
STEP W's refusal class (A never archived · B wrong FY · C no PAT · D legs unprovable). Then the untried
BSE archived-website rung (STEP B). Off-cycle fiscal filers (PFIZER Nov-FY; 17 tagged) are NOT gaps
(feedback-offcycle-fiscal-quarter-is-not-a-gap) — verify the store's calendar mapping per name before
counting them.

### WP-P3 — std PAT 2005-2008 (~836) and WP-P4 2009+ (~780)
NSE results archive (2005+) → BSE detres (2008Q1+) → BSE PDF comparatives (§113a, 3 filings per quarter).
The 338 `mc-closed` cells go straight to as-filed rungs; `mc-lacks-symbol`/`wrong-entity` are mostly
delisted/renamed names → era BSE code + ISIN gate (§76) → detres. Batch by symbol so §45 FY identity
adjudicates; owners basis is irrelevant here (std) but the con-copy trap is not: check the revenue slot
whenever a std cell is found holding a merged figure.

### WP-P5 — the ~50 no-fundamentals-row symbols (564 cells) and the 89 price-orphan symbols (656 mm)
8 of the no-row names have archived NSE pages (§112g); the rest need era BSE codes and whole histories —
`apply_agg_pat_fills.py --new-symbols` (opt-in, prints keys created). The price-orphan class is a ROSTER
problem (modern keys over era tapes) and needs its own owner before any 2002 number can reach 100%.

## 6. Recipes

Re-derive queues (all read-only, ~8 min, uses the release bin):
```bash
node --max-old-space-size=12288 scripts/build_coverage_matrix.js --bin auto --out /tmp/cov \
     --explain nifty-500 --explain-out /tmp/explain.json --facts /tmp/facts.json
# explain.byDate[date][param] = missing symbols; byDate[date]['na:'+param] = N/A symbols; '__norow' = no price row
```
Then the fix-point (session scratch `fixpoint.py`, `naclass.py`, `roots.py` — logic described in §3; port
into `scripts/` when WP-P1 starts). Parity gate: reproduced `have`/`na` per date must equal the payload
(this session: 0 diffs on patStd/fii/dii; 2-cell 2002 skew on an offline std reproduction, immaterial).

Pilot a BSE aspx quarter: `python3 -c "import sys;sys.path.insert(0,'scripts');import fetch_shp_bse_aspx as F;
print(F.parse_old(F.fetch_page('/tmp/aspx',500116,33,'Old')[0]))"` (IDBI Mar-2002).

## 7. Decisions for the user

1. **Scope order:** N500 first (this plan), then re-derive for `all` (39k patStd / 44k fiiPct holes plus
   ~100k N/A each)? Recommended: N500 first — every favourite strategy screens it.
2. **N/A = queue:** confirmed by the user ("they may be fillable"); recorded here. Nothing is being marked
   N/A by this plan.
3. **Builder rule:** the `nothingPublicYet` / first-SHP N/A reads our own store. Proposal: keep the rule but
   refuse to fire when the symbol's tape started ≥365 d before the date (would turn 4,868 patStd + 7,043
   fiiPct N/A into visible holes, i.e. honest numbers). Page-changing — needs explicit approval.
4. **Ownership of WP-S1** if an SHP session is live (its files were dirty in the shared checkout on
   2026-09-05).

---

## 8. EXECUTION LOG — std-PAT packages (2026-09-05, session b391e819, worktree `~/stocks-wt/stdpat-2002`)

Runbook **§128** carries the full record; this block is the plan's state so the next session resumes from it.

| WP | state | what landed | where the record is |
|---|---|---|---|
| P0 hygiene | **CLOSED, reframed** | the 40 "in-a-fill-ledger-but-absent" cells were not absent: **229 root cells sit under a RETIRED store key** (HIMACHLFUT/SUPPETRO/HINDMOTOR/COLGATE/ASIANHOTEL/CASTROL/LGBROS/MANDHANA + BBOX/PENINLAND/TMPV/NDLVENTURE) that the engine folds and the coverage BUILDER read raw. Builder fixed (commit `5dbe45857`): patStd +597 mm, na −597, exactly those 8 symbols. The 26 "null-std rows" are 2015+ IPO comparatives (out of this scope). 5 cells are stored with a late real ann (BRFL/JAGRAN/MCLEODRUSS/PROVOGUE 2007-03, EIHOTEL 2005-03) — not fetch targets | §128a |
| P1 1999-2001 | **PARTIAL → +238 (runbook §133, BSE archived results page, this session 12:30 IST)** | MC E2b 766 cells 1999-2001 (+ 11 WB 2001); residue by tag in `stdpat_2002_residue_2026-09-05.json`'s method (those years not in the file — regenerate with the §6 recipe) | `agg_pat_cell_fills.json` applied `2026-09-05` |
| P2 2002-2004 | **PARTIAL** | WB 154 (2002:116) + MC E2b 119 + adjudicated 7; 130 held-under-alias | §128b/c |
| P3 2005-2008 | **PARTIAL** | NEW NSE-archive standalone reader `scripts/nse_std_pat_read.py` 175 cells (2005:72, 2006:85, 2007:10); MC 41 | §128d |
| P4 2009+ | touched only where MC's table reached (see ledger) | | |
| P5 no-row / price-orphan | **UNTOUCHED** | 10 WB passes on no-key symbols were all alias-held corroborations (TATAMOTORS→TMPV, MORAREALTY→PENINLAND, match to the paisa) | |

**Residue 2002-07 (2,954 root cells, every one with its rung status): `scripts/stdpat_2002_residue_2026-09-05.json`.**
Dominant classes: MC REJECT-GATE-E with NO archived NSE page (1,436 in 2002-04) · MC NOT-FOUND, no page (409) ·
2005-07 MC-rejected with no Non-Consolidated row in the cached NSE list (281) · WB page is a cumulative/H1/annual page,
not a quarter (222+62+31). Next rungs, in order: BSE pre-2018 announcement PDFs via `CorpAttachment` resolver
(§reference-bse-attachment-resolver) · the BSE ARCHIVED WEBSITE (PRE2015 STEP B, still never built) · NSE list API
for the 26 symbols with no cached list (fragile endpoint, cookie jar) · annual reports for March quarters · vision (§17b,
ask first). **Suspects of OURS** (176, reported not patched): `scripts/stdpat2002_suspects.json`.
