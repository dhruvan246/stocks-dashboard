# Batch B: CUPID / SELMC / ESSARSHPNG / BALLARPUR standalone revenue arbitration (2026-08-09)

**Scope:** `docs/sf_revop.json[SYM][QE][0]` (standalone revenue, revS) for the two worst flagged quarters
plus one control per company (12 cells total). No repo writes, no heals -- this file plus
`batch_b_verdicts.json` is the deliverable.

## Headline result: NO DEFECT FOUND. 12/12 cells OURS_CONFIRMED.

Every one of the 12 cells read matches the filing's `Revenue from Operation(s)` line -- the correct,
undecomposed top line -- to the cent (or within standard 2dp rounding), independently anchored by an
EXACT PAT match to `docs/sf_fundamentals.json`, and backed by a second check (FY quarter-sum identity or
component-sum-to-Total-Income). All four control quarters pass. None of these four companies carry the
AADHARHFC/HDBFS wrong-row defect class.

## Verdict table

| symbol | quarter | role | ours | filed Revenue-from-Ops | filed Total Income (rev+other inc) | ours matches | PAT anchor | 2nd check | verdict |
|---|---|---|---|---|---|---|---|---|---|
| CUPID | 2024-09-30 | worst (-11.6%) | 41.55 | 41.5549 | 47.2855 | Revenue from Operation | 10.04 = 10.0387 exact | H1 = Q1+Q2 exact | **OURS_CONFIRMED** |
| CUPID | 2025-12-31 | 2nd-worst (-10.1%) | 93.51 | 93.5123 | 104.3986 | Revenue from operation | 32.87 = 32.8686 exact | comparative col exact | **OURS_CONFIRMED** |
| CUPID | 2023-12-31 | CONTROL (+0.1%) | 40.05 | 40.0516 | 40.7797 | Revenue from operation | 8.86 = 8.8637 exact | comparative col exact | **OURS_CONFIRMED** |
| SELMC | 2024-12-31 | worst (-9.6%) | 4.52 | 4.5219 | 4.5744 | Net Revenue from operations | -15.99 = -15.9871 exact | 9M-YTD sum matches | **OURS_CONFIRMED** |
| SELMC | 2025-03-31 | 2nd-worst (-5.3%) | 2.84 | 2.8416 | 2.9625 | Net Revenue from operations | -40.05 = -40.0539 exact | FY-sum matches | **OURS_CONFIRMED** |
| SELMC | 2024-03-31 | CONTROL (0.0%) | 28.01 | 28.0091 | 25.4486 | Net Revenue from operations | -41.53 = -41.5261 exact | comparative col exact | **OURS_CONFIRMED** |
| ESSARSHPNG | 2024-03-31 | worst (-8.6%) | 4.57 | 4.57 | 7.26 | Income from operations | -104.59 exact | FY-sum exact (15.76) | **OURS_CONFIRMED** |
| ESSARSHPNG | 2023-12-31 | 2nd-worst (-8.4%) | 4.58 | 4.58 | 4.69 | Income from operations | -9.06 exact | component sum exact | **OURS_CONFIRMED** |
| ESSARSHPNG | 2024-12-31 | CONTROL (+2.2%) | 5.11 | 5.11 | 78.41* | Income from operations | 58.32 exact | comparative col exact | **OURS_CONFIRMED** |
| BALLARPUR | 2025-09-30 | worst (-15.5%) | 1.69 | 1.6856 | 1.8982 | Revenue from operations | -31.75 = -31.7493 exact | component sum exact | **OURS_CONFIRMED** |
| BALLARPUR | 2025-03-31 | 2nd-worst (-8.7%) | 2.74 | 2.7392 | 2.9535 | Revenue from operations | -30.28 = -30.2808 exact | FY-sum matches (18.62) | **OURS_CONFIRMED** |
| BALLARPUR | 2026-03-31 | CONTROL (-0.3%) | 62.79 | 62.7925 | 63.1691 | Revenue from operations | -37.57 = -37.5676 exact | component sum exact | **OURS_CONFIRMED** |

\* ESSARSHPNG 2024-12-31's Total Income includes a one-off ~73cr non-operating recovery/settlement gain;
included for completeness, not diagnostic of anything.

**Controls: 4/4 PASS.** Per the brief's binding rule, a passing control on a method that also calls
"confirmed" elsewhere is credible -- and here every cell, flagged or control, comes back the same way.

## Why the sweep flagged these cells anyway (two different, unrelated mechanisms -- neither is a defect)

1. **CUPID (larger revenue base, ~40-155cr/quarter): a SITE-side scrape inconsistency, not ours.**
   Screener's swept "Sales" figure for the two flagged quarters lands closest to **Total Income**
   (Revenue from Operation + Other Non-Operating Income, + a one-off Fair Value gain in Dec-25) rather
   than Revenue from Operation. But the SAME company's control quarter (2023-12-31) shows Screener's
   number matching the CORRECT row instead (Revenue from Operation rounds to 40, which is what Screener
   shows; Total Income would round to 41). A site that flips between two different concepts
   quarter-to-quarter is not tracking a stable alternate definition -- it is unreliable, and every
   direct filing read (three quarters, spanning 2023-2026, all anchored by exact PAT and by
   comparative-column cross-checks) confirms our stored value is the correct, undecomposed
   "Revenue from Operation" line throughout.

2. **SELMC / ESSARSHPNG / BALLARPUR (tiny revenue base, ~1-30cr/quarter): pure whole-crore display
   rounding on Screener's side, not a row defect at all.** These three companies' quarterly revenue is
   small enough that Screener's practice of displaying revenue as a whole-crore integer creates
   5-16% RELATIVE gaps against our 2-decimal-precision figures, while the ABSOLUTE difference is
   under Rs 0.5 crore in every case -- exactly Screener's own display granularity. The clearest proof
   this is display noise, not a row swap: for SELMC 2024-12-31, BOTH the correct row (Net Revenue
   4.52) AND Total Income (4.57) round to the identical Screener-displayed integer (5) -- so the
   flagged percentage cannot, even in principle, be diagnostic of which row Screener used. All three
   companies' filings, read directly, show a single undecomposed "Revenue/Income from operations"
   line (no "Sale of products" vs "other operating revenue" split to confuse), matched to the cent
   against our stored value in every one of the 9 cells read across these three companies.

## Route ladder (used identically for all 12 cells)

1. **BSE detres** (`Corp_detailedResult_Transpose_ng`, runbook §42) -- rung 1, resolved scrip via
   `bse_scrips.json` active-equity master for CUPID (530843) / SELMC (532886) / ESSARSHPNG (533704);
   for BALLARPUR (long-suspended, not in the active-equity master) the known scrip code **500102**
   was used directly, per the precedent already established and verified elsewhere in this codebase
   (`scripts/_stepg_close28.py`). Detres returned a value matching our stored figure in every
   attempted case (11 of 12; BALLARPUR's control quarter needed the announcement route directly since
   detres agreed but a full breakup was still needed) -- useful as a first-pass sanity check but NOT
   sufficient alone to arbitrate the row question, since detres's own "Net Sales/Revenue From
   Operations" label doesn't distinguish "Revenue from Operations" from "Total Income" the way the
   underlying filing's multi-row statement does.
2. **NSE per-basis XBRL** -- not attempted for the three NSE+BSE dual-listed names (known 403
   lockdown, brief-stated); attempted for BALLARPUR via `exchange_fetch.py` (delisted from NSE
   XBRL for these quarters) and refused cleanly with routes logged.
3. **BSE announcement PDF** (`AnnSubCategoryGetData`, workhorse rung) -- the arbitration source for
   every cell. Fetched via scrip-scoped, date-windowed calls; for BALLARPUR the `strCat=Result`
   category scan came back empty for the target quarters (its filings arrive irregularly, mixed into
   general/board-meeting categories during the post-CIRP catch-up period), so the scan was widened to
   `strCat=-1` (all categories) across a full year and headline-matched -- this found the correct
   "Outcome of Board Meeting" filings carrying the full standalone statement.
4. **Column read**: PAT anchor first (per the brief's binding rule -- PAT already agrees in every
   flagged cell, so PAT identifies the column), then read the Revenue row from that same column.
   Several PDFs had unreliable or absent text layers (CUPID's raw `get_text()` on the results table
   returned digit-jammed strings with dropped decimals/commas; ESSARSHPNG and BALLARPUR's
   board-outcome PDFs were partially or fully scanned images) -- all such pages were rendered at
   150-300dpi and read visually per runbook §57 rung 10, never trusted from raw text extraction alone.
5. **Second check**: FY quarter-sum identity (§45) where 4 quarters of the fiscal year were available
   in `sf_revop.json`, or component-sum-to-printed-Total-Income, on every cell.

## Constraints observed

Read-only throughout; no logins, no captcha bypass; ≥2s between live requests (enforced in
`tools/exchange_fetch.py` and `tools/bse_ann.py`); no 403/429 encountered on any BSE endpoint; zero
writes to the reference tree or the live repo. All fetched JSON/PDF responses cached under
`cache/` and `pdfs/` in this working directory only.

## One side finding, not adjudicated (flagged for a future packet)

BALLARPUR's BSE detres response for **2024-03-31** returns a "standalone" revenue figure (8.20cr) that
actually matches our stored **consolidated** slot (revC=8.2), not standalone (revS=8.0) -- and neither
stored PAT slot for that quarter (npStd=0.0, npCon=-276.4, both filed late on 2024-10-26) matches
detres's PAT read (-208.81cr) for the same call. This looks like a filing-catch-up / basis-mislabelling
artifact specific to BALLARPUR's chaotic 2020-2023 CIRP gap years (the company filed almost nothing
2021-2023), not the wrong-row defect class this campaign tests for. **2024-03-31 was NOT used as this
packet's control** for exactly this reason -- 2026-03-31 was substituted instead, and passed cleanly.
If a future packet is assigned BALLARPUR cells from 2020-2024, do not trust that era's detres/PAT
pairing without a direct filing read first.

## Files

- `batch_b_verdicts.json` -- structured verdicts, all 12 cells, per the requested schema.
- `cache/` -- every fetched BSE announcement-list JSON, keyed by URL.
- `pdfs/` -- all 12 downloaded filing PDFs (3 per company).
- `render/` -- every 150-300dpi PNG page render used for a visual read.
- `tools/exchange_fetch.py`, `tools/bse_ann.py` -- fetch tooling, derived from the reference tree's
  `scripts/revpat_verify/exchange_fetch.py` and `scripts/fetch_bse_results.py` / `scripts/bse_vision.py`
  patterns, cache path redirected to this working directory only (never wrote into the reference tree).
