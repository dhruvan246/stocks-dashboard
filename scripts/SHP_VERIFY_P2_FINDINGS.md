# SHP VERIFY — PHASE 2 RESULTS  (mapping cards + calibration pilot, 2026-08-09)

12 pilot stocks x 7 sites + the BSE cross-exchange leg. 884 raw extraction rows, 8 mapping cards
derived arithmetically, 2,133 cell-fields adjudicated by quorum.

## 1. Headline: our data holds up

**841 CONFIRMED / 11 CONTRADICTED — and every one of the 11 is a cell we DON'T have** (a missing
shareholder count that two sources agree on). **Not one cell where we hold a value was contradicted
by a united set of sources.**

| field | CONFIRMED |
|---|---|
| fii | 164 |
| prom | 162 |
| nsh | 157 |
| ins | 109 |
| dii | 101 |
| mf | 66 |

**Cross-exchange (the P1 finding-B route): 1,653 MATCH, 1 MISMATCH.** Our NSE-derived values agree
with BSE's independently-filed documents on every field the route can read, across Jun-2016→Jun-2026.
The single exception is **M&M 2019-12-31 dii: ours 24.20, BSE 27.81** — the only genuine
cross-exchange disagreement in the pilot, and the top of the P5 arbitration queue.

Anchor check: RELIANCE Jun-2026 re-fetched from the live NSE XBRL and re-parsed gives
`prom 50.48, fii 17.20, dii 21.19, mf 10.11, ins 9.20, nsh 4,651,863` — our stored cell exactly.

## 2. Mapping cards (derived from arithmetic, not from labels)

| site | prom | fii | dii | mf | ins | nsh | verdict |
|---|---|---|---|---|---|---|---|
| **StockEdge** | 100% | 99.1% | 78% | 99.0% | **99.0%** | 97.9%* | best all-round; med |d| = 0.0000 |
| **Trendlyne** | 100% | 100% | 85% | **100%** | **100%** | — | verifies mf+ins exactly |
| **Groww** | 100% | 98.2% | 96.4% | 98.2% | ✗ | — | strong; "insurance" is a lie |
| **ET Markets** | 100% | 100% | ✗ | 100% | ✗ | — | 4 quarters only |
| **Screener** | 100%† | 92.3% | 62% | ✗ | ✗ | **100%** | deepest; nsh exact to the person |
| **Tickertape** | 88% | 83% | 83% | 85% | 85% | — | see DR note below |
| **Moneycontrol** | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | **nothing maps** |
| **BSE (exchange)** | ✓ | ✓ | ✓ | n/a | ✓ | ✓ | 1,653/1,654 |

*hold %, share of quarters inside tolerance. `*` counts in lakhs, scaled. `†` pre-2022 era only.
`✗` = no credible mapping — the site either lacks the row or its bucket is not ours.

- **Screener's DII is not our DII** (62% hold, ~0.09pp low every modern quarter). The filing and
  Groww both back ours. Screener therefore does not vote on DII at all — better than pretending.
- **Moneycontrol maps to nothing.** Its FII matches ours *exactly* in 58% of quarters and misses
  badly in the rest — a bimodal pattern that smells like column/quarter misalignment, not noise.
  With 5 quarters and no MF/insurance rows, it is not worth chasing; it stays out of the quorum.
- **Tickertape's ~15% miss rate is the depository-receipt denominator**, concentrated in INFY,
  HDFCBANK, M&M, RELIANCE, SBIN — every one an ADR/GDR name. Its per-stock ratio is constant across
  ALL categories at once (HDFCBANK 1.1545 on fii, dii and mf alike), which is a denominator
  difference, not a bucket difference: it keeps DRs outside the base where the post-Sep-2022 SEBI
  look-through puts them inside. Ours follows the as-filed XBRL. DEF_DIFF, not our defect.

## 3. ★ A STALE LOCAL CHECKOUT, mistaken for a reader gap  (corrected — read this one carefully)

The exchange leg appeared to expose a parser bug: `parse_shp` returning **`mf = 0.0` on BSE
documents for 2022-2025 only** (0/21 in 2016 … 36/36 in 2023, 0/16 in 2026), while every other
field on those same filings matched us exactly.

The diagnosis of the mechanism was right — BSE's new-format filings keep the OLD lowercase
`MutualFundsOrUtiMember` element, and a parser that only maps the uppercase `MutualFundsOrUTIMember`
in its new-format branch writes 0.0. **The conclusion was wrong: that fix already shipped on
2026-08-07 and is live on origin/main.** What actually happened is that this session's extraction
agent and my own test both imported
`/Users/dhruvan/stocks-dashboard/scripts/fetch_shareholding.py` — a checkout **227 lines behind
origin/main**. The zeros are an artefact of the stale working copy, not of production behaviour.

Consequences, all corrected:
- The BSE mapping card can carry `mf` after all; the 54 "phantom disputes" were self-inflicted twice
  over — first by the stale parser, then by my omitting the field to work around it.
- The BSE pilot rows for 2022-2025 need re-parsing with the current parser before their `mf`
  is used for anything.
- **No production fix is required and none should be written.**

The rule this breaks is one already in the runbook for data — *analyse LIVE, never the local
checkout* — and it applies to **code** exactly as it does to `.bin` and `.json` files. Every agent
in this campaign now imports the parser from a tree checked out at origin/main.

Still true and independently verified: our stored `mf` values are correct. StockEdge (99.0% hold),
Trendlyne (100%) and Tickertape all confirm them to the decimal, and BSE-routed cells in
`shp_history` hold *fewer* mf zeros (6.6-7.7%) than NSE-live ones (17.0%).

## 4. ★ Trendlyne kills the idea of a universal DII calibration

Nine deep pre-Sep-2022 pages (RELIANCE / HEROMOTOCO / INFY at Mar-2016, Mar-2018, Mar-2021):
- **FII = FPI confirmed from the site's own tooltip**, not inferred.
- The old-format **"Any Other" bucket holds NAMED FOREIGN entities** — Government of Singapore, Abu
  Dhabi Investment Authority, Europacific Growth Fund (RELIANCE); Oppenheimer, Lazard, Aberdeen
  (HEROMOTOCO) — and **the same entity type lands in different buckets in different quarters of the
  same stock**. Bucketing is filing-specific, not rule-based.
- **INFY has no "Any Other" bucket at all**: MF + FPI + FI/Banks + Insurance sum exactly to the
  institutional total in all three quarters.

So the §22b `OLD_OTHER_TO_DII` calibration cannot be validated or replaced by a single constant —
the residual is genuinely per-stock and per-filing. Keep the flag as calibrated; do not "improve" it.

## 5. Fillable gaps found

11 cells where we hold NOTHING and independent sources agree — all `nsh`, e.g. TCS 2024-06-30
(BSE 2,181,391 = Screener 2,181,391), HDFCBANK 2024-03-31 (BSE = Screener 4,121,815), ETERNAL,
HEROMOTOCO, GAYAPROJ, BSE Ltd. Plus MCX 2017-03-31 from P1 (BSE XBRL = Screener exactly).
None taken yet — P6 heals them under rule 6b with the corroborating sources named.

## 6. Tooling defects fixed this phase (all found by running on real data)

Each of these was manufacturing defects in OUR data that do not exist:
1. crash on `"22.58%"` / `"46,51,863"` — no value normalizer;
2. mapping deriver fitted noise after a 50%-support floor dropped a legitimately-absent row;
3. era-split rescue installed the mushy cross-era combo as the live mapping — **13 phantom mismatches**;
4. no unit scale: counts "in Lacs" vs raw — **95 phantom mismatches**;
5. quorum compared headcounts using percentage-point thresholds — two sites "in conflict" over half a person;
6. CONTRADICTED fired on sites that disagreed with each other as much as with us — **10 false accusations**;
7. quorum's null-value scale defaulted to pp, burying real fillable gaps under SITES_DISAGREE;
8. diff engine crashed on `rows: null` from an unparseable filing.

The lesson is the campaign's own rule turned on the tools: **measure the strict way before trusting
a result** — every one of these looked like a finding until it was checked.
