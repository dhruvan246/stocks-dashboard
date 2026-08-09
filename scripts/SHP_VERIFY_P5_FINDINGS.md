# SHP VERIFY — PHASE 5: ARBITRATION AGAINST THE FILINGS  (2026-08-09)

Every contested cell from the Phase-3 audit taken to the document the company actually filed,
via the §57 ladder (NSE master -> XBRL, then BSE SHPQNewFormat -> XBRL). Site agreement was never
used to decide anything.

## 1. Result: zero defects in our data

| verdict | count |
|---|---|
| **OURS_CONFIRMED** — the filing reproduces our stored cell | **79** |
| **FILLABLE** — we hold nothing, the filing has the value | **51** |
| UNPARSEABLE — a document exists, `parse_shp` correctly refuses to anchor it | 6 |
| **OURS_WRONG** | **0** |
| **REVISION** (we hold a superseded filing) | **0** |

Across the whole campaign so far — 3,884 cells checked against 3 sites, then every disputed one
taken to the exchange — **not a single value we publish has been shown to be wrong.** Every
arbitration to date (ICICIBANK, HINDALCO, RELIANCE, MCX, plus these 79) has landed on our side.

## 2. ★ The real finding: shareholder counts, not percentages

All 51 FILLABLE cells were the same field in the same quarter — `nsh`, Jun-2024. That was the
thread. Pulling it: **9,094 of 66,477 cells (13.7%) carry no shareholder count**, and the misses
are not scattered, they are quarter-shaped:

| quarter | cells | with count | |
|---|---|---|---|
| 2022-09-30 | 1,884 | **21** | **1.1% — near-total blackout** |
| 2024-03-31 | 2,046 | 1,595 | 78.0% — partial |
| 2024-06-30 | 2,033 | **6** | **0.3% — near-total blackout** |
| 2025-09-30 | 2,224 | 2,062 | 92.7% — partial |
| 2025-12-31 | 2,265 | 2,098 | 92.6% — partial |
| 2026-03-31 | 2,302 | 2,137 | 92.8% — partial |
| every other quarter 2019-09 → 2026-06 | ~1,700-2,200 | ~99.9% | healthy |

**The filings have the data.** Spot-checked against live NSE XBRL: Mar-2024 5/5 carry a count
(HDFCBANK 4,121,815 — a cell we store as empty), Sep-2025 5/5, and all 51 Jun-2024 cells parsed
cleanly during arbitration. So this is ours to fix, not an absence at source.

Note the two blackout quarters: **Sep-2022 is the SEBI format-boundary quarter** (§22b), which is
suspicious enough to check first when diagnosing. Jun-2024 has no obvious structural excuse yet.
Do not guess the cause — read the fetch/parse path for those two quarters before changing anything.

Scale of the opportunity: ~3,890 cells in the two blackout quarters alone, plus ~1,200 across the
partial ones. `nsh` feeds the "No. of shareholders" row on every stock page and is one of the
strongest independent checks we have (Screener matched it to the person 48/49 times).

## 3. What the 6 UNPARSEABLE cells are

BHANDARI (4 quarters) and SOMICONVEY and PUNJLLOYD — micro-caps and a near-shell. A document
exists but `parse_shp` will not anchor it. That is the parser doing its job: it refuses rather
than guessing, exactly as §22b requires (zero-filling an unanchored filing is what poisons
FII/DII). They stay open and are reported as such — never quietly counted as verified.

## 4. Method note

Arbitration batches by quarter, so one NSE master fetch serves every contested symbol in it, and
lists ALL submissions per (symbol, quarter) before choosing — the newest submission date wins, so
a revision can never be resolved by picking whichever value looks closer to ours. `parse_shp` is
imported from a tree checked out at origin/main, after a stale working copy produced a phantom
parser bug earlier in this campaign.
