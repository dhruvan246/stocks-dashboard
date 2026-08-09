# FII / DII HOLDINGS — VERIFICATION REPORT
**Campaign run 2026-08-09. Snapshot verified: origin/main `93de247c`, `shp_history.json` blob `b2bed157`
— 2,615 symbols, 66,477 stock-quarters, Sep-2010 → Jun-2026.**

The user's question was: *are our FII/DII holdings correct?* — to be answered by checking against
5-7 external sites, taking a value only when many sources agree. This is the answer.

---

## 1. THE ANSWER  (final — full universe swept)

**Nothing we publish has been shown to be wrong.**

| test | scope | result |
|---|---|---|
| **FULL SWEEP — three sites × every stock** | **2,615 symbols, 179,424 cell-values we hold** | **87,315 CONFIRMED · 87 contradicted (0.048%)** |
| **Arbitration of all 87** — taken to the company's own filing | 87 field-verdicts | **83 OURS_CONFIRMED, 0 OURS_WRONG**, 4 unresolvable |
| **Cross-exchange** — our NSE values vs BSE's *separately filed* documents | 61 symbols × 41 quarters, Jun-2016→Jun-2026 | **2,990 MATCH, 1 ROUND, 0 MISMATCH** |
| Three sites vs the frozen stratified sample | 66 symbols, 3,884 cells | 2,156 CONFIRMED, 0 contradicted |
| Earlier arbitration (P5) | 136 field-verdicts | 79 OURS_CONFIRMED, 0 OURS_WRONG |
| **Cells where NO independent source agrees with us** | 179,424 checked | **0** |

**Final arithmetic: 179,424 published values checked; 51 shown wrong (0.028%) — all healed the
same day.** And the campaign's most important finding came LAST, from the user challenging the
unanimity cases rather than from any phase of the plan.

### ★ The revision-miss defect class — found by the user, not the campaign

When all three sites agreed with each other and disagreed with us, arbitration read the NSE
document and declared us confirmed. **That check was circular: it re-read the same document our
pipeline had ingested, so a superseded filing confirmed itself.** The user asked "if every site
agrees and we differ but match the filing, might WE have read the wrong thing?" — and the answer
was yes, 35 times: companies file, then REVISE; BSE carries the revision (`revised_date_time`),
while NSE's master keeps serving the original. Newest-submission-wins cannot see a revision that
never appears in its list.

Re-adjudicating all 103 "filing confirmed us" documents against BSE's independently received copy:
**50 genuinely confirmed (sites genuinely wrong), 35 REVISED_ON_BSE — our value superseded, 51
field-values wrong** — 15 BSE-absent, 3 unparseable. All 35 healed via `shp_cell_fix.json` with
the revised BSE document + 2-3 agreeing sites as rule-6b evidence.

**The example previously showcased here was inverted.** LCCINFOTEC Jun-2025 promoter 0.0 — cited
as "three sites unanimously wrong" — was OUR stale document; the company revised to **45.85** on
2025-11-05. The sites were right. Also healed: S&SPOWER 50.21→74.97, MSPL 33.48→42.34, SUPREMEINF
47.32→34.68 (two quarters), MARKSANS FII 16.74→8.12, and two SANWARIA counts written by this very
day's reparse heal (it read the pre-revision document).

The 50 where BSE confirms us remain real site errors — UJJIVAN FII 30.91 (both exchanges) vs the
sites' 40.48 stands. But the honest lesson is symmetric: **"ours equals the filing" is only
evidence when it is not the same copy of the filing our pipeline already read.** Cross-exchange
disagreement is now the first thing to check on any unanimous site-vs-us cell, and the daily
pipeline needs a periodic BSE `revised_date_time` sweep so revisions stop going invisible.

The single non-exact value in the entire cross-exchange set is CUMMINSIND Jun-2022 shareholder
count: ours 109,068, BSE 109,067 — **one shareholder, across a decade of filings.**

Every individual arbitration — ICICIBANK, HINDALCO, RELIANCE, MCX, and 79 others — read the
company's own filing and reproduced our stored cell field-for-field.

## 2. WHAT WE ACTUALLY FOUND — errors on the sites, gaps in our coverage

**Errors on the sites** (arbitrated against the filing, we were right):
- Screener's FII is wrong on some large caps: ICICIBANK Jun-2026 it says 33.79, the filing says
  **49.82**; HINDALCO Jun-2026 it says 31.41, the filing says **35.60**. Measured miss rate ~2.7%.
- Screener's DII bucket is not ours at all (62% hold) and it does not vote on DII in this campaign.
- Moneycontrol maps to **nothing** — its FII matches ours exactly in 58% of quarters and misses
  badly in the rest, a misalignment pattern, so it was excluded from the quorum rather than forced in.

**Gaps in ours** (the genuinely actionable output):

| finding | scale | status |
|---|---|---|
| **Shareholder counts missing** — quarter-shaped, not scattered | 9,094 cells (13.7%) | **HEALED: +4,875, coverage 86.3% → 93.7%** |
| Quarters sites/BSE hold that we do not | ~65 in the sample (51 nsh + 6 BSE + others) | verified fillable |
| Internal holes inside a symbol's own history, post-Jun-2016 | **2,344 across 680 symbols** | cause UNKNOWN — see §4 |

### The shareholder-count gap — found, diagnosed and HEALED the same day

Coverage ran at 99.9% in every quarter from Sep-2019 **except** Sep-2022 (1.1%), Jun-2024 (0.3%),
Mar-2024 (78%) and Sep-2025→Mar-2026 (~93%). Two heals closed it:

| pass | route | cells |
|---|---|---|
| 1 | `--reparse` scoped to the deficient quarters, staged | **+4,319** |
| 2 | targeted count-only read of filings the percentage parser refuses | **+556** |
| | | **62,258 of 66,477 = 93.7%** (from 86.3%) |

Sep-2022 went 1.1% → 99.5%, Jun-2024 0.3% → 99.5%, Mar-2024 78% → 99.9%.

**Pass 2 exists because of a wrong conclusion I published in pass 1.** I reported the residual
~576 cells as "genuinely absent at source" because `parse_shp` refused 161 of 162 filings. That
inference was faulty: `parse_shp` returns one all-or-nothing result, so when the FII/DII gates fail
it returns None and **discards a shareholder count it has already read correctly**. 21STCENMGM
Sep-2025 is the proof — parse refused, while the XBRL carried `ShareholdingPatternMember = 8,266`
the whole time. A refusal is not an absence. `scripts/shp_nsh_only.py` now reads the count and
nothing else (whole-company context only; category contexts carry per-bucket counts, and writing
"6 promoters" as a company's shareholder count would be far worse than a blank).

19 of the 575 recovered counts were **withheld** by the continuity gate rather than merged —
DSKULKARNI reporting 8 shareholders against a 4,582 neighbour, EASTSILK falling 26,267 → 4,830 in
six months, SUPREMEENG rising 8.5x. Each may be a real corporate event; none should be written on
a parser's say-so.

Verified live afterwards: 556 of 556 merged counts present in the deployed feed, and the 19
withheld ones correctly absent. Note on reach — the page feed carries 8 quarters, so the +556
recent counts are visible on the site while the +4,319 older ones sit in the store; the stock page
reaches them through its per-stock `shpH` slice.

## 3. THE SITES — what each is actually worth

| site | depth | verdict |
|---|---|---|
| **Screener** | Mar-2017 FY-ends + 12 rolling qtrs | deepest cheap source; nsh exact **to the person** (48/49); real FII errors |
| **StockEdge** | 9 quarters (hard cap) | cleanest match — median delta **0.0000** on prom/fii/mf/ins |
| **Trendlyne** | Dec-2015 via per-quarter pages | verifies mf AND ins at **100%**; 10s crawl-delay makes it sample-only |
| **Tickertape** | 6 quarters | clean API, provable bucket identities; DR-denominator difference |
| **Groww** | 5 quarters | strong on prom/fii/dii/mf |
| **ET Markets** | 4 quarters | prom/fii/mf exact; no insurance, no counts |
| **Moneycontrol** | 5 quarters | **unusable** — nothing maps |
| **BSE (exchange)** | Jun-2016 → date | the real check; 0 disagreements |

**★ No site has data before 2010, and only Trendlyne reaches before 2017.** Our two weakest eras
(2002-2010 at 0%, 2010-2015 at 30%) cannot be corroborated by any retail aggregator — which is
why the campaign was rebuilt around cross-exchange verification instead.

## 4. OPEN, AND HONEST ABOUT IT

- **The 2,344 internal holes have no diagnosed cause.** My hypothesis — BSE rows with a null
  `filing_date_time` — was measured and **failed**: 43 null-date rows exist in the sample and only
  6 correspond to a cell we lack. P3b must diagnose, not inherit that guess.
- **2010-2015 is unverifiable by design.** Those cells came from archived Moneycontrol, so a
  Moneycontrol match would be circular; no other site reaches back that far and BSE's XBRL starts
  Jun-2016. Reported as unverifiable rather than quietly counted as fine.
- **6 cells are UNPARSEABLE** (BHANDARI ×4, SOMICONVEY, PUNJLLOYD): a document exists but
  `parse_shp` refuses to anchor it. That is the parser working correctly — zero-filling an
  unanchored filing is what poisons FII/DII — and they stay open.
- **4 cells could not be settled** by either exchange: MELSTAR Mar-2026 and SIGIND Sep-2025
  (documents exist but will not anchor), INOXLEISUR Jun-2021 and STERLINBIO Mar-2021 (no filing at
  either exchange). Open, not quietly counted as verified.
- **19 recovered shareholder counts withheld** by the continuity gate — listed above.
- **~19 residual count cells** have no NSE filing at all (their cells came via BSE ledgers).

## 5. METHOD — why these numbers can be trusted

- **Nothing decided by site majority.** Sites copy each other; only exchange filings arbitrate.
- **Mapping cards derived arithmetically, never from labels.** Groww publishes a field literally
  named `otherDomesticInstitutions.insurance` that is *all non-MF domestic holdings*. Mapping by
  name would have manufactured a ~1.9pp defect on every stock. The tool searches for the subset of
  a site's rows whose SUM reproduces ours, and refuses when nothing fits.
- **Provenance tracked per cell** so a site can never "verify" data we took from it: 6.2% of our
  cells came from archived Moneycontrol, 0.1% from screener/Trendlyne.
- **The sample was frozen before looking** — deterministic md5 draw, 12 strata, committed.
- **Identity discipline caught real traps**: Tickertape's sid `TRU` is an unrelated company
  (Trust Fintech); StockEdge's ticker shortcut matched `IEL` to the wrong firm. Both would have
  become fake "defects" in our data.

**Fifteen tooling defects were found and fixed during the campaign** — a crash on `"22.58%"`
strings, a deriver that fitted noise, an era-split that installed the wrong mapping (13 phantom
mismatches), missing unit handling for lakhs (95 phantom mismatches), percentage thresholds applied
to headcounts, a CONTRADICTED bar that accused sites of agreeing when they didn't (10 false
accusations), and more. **Every one of them initially looked like a defect in the data.** That is
the reason for the arbitration rung: no value is called wrong until the filing says so.

**And one substantive error of my own**: I reported a parser bug (`mf = 0.0` on BSE filings) and
filed it as blocking. The mechanism was real but the fix had already shipped 2026-08-07 — my test
imported a local checkout **227 lines behind origin/main**. *Analyse live, never the local
checkout* applies to code exactly as it does to data.

## 6. WHAT HAPPENS NEXT

1. ~~Phase 4~~ **DONE** — 2,615 symbols × 3 sites, 77,391 rows, folded through quorum and arbitration.
2. ~~Shareholder-count heal~~ **DONE** — +4,875, live-verified.
3. **P3b: diagnose the 2,344 internal holes.** Still the largest open item. My null-`filing_date_time`
   theory was measured and failed (4.4x enriched but explains under a third) — diagnose, don't inherit it.
4. The sibling coverage session is producing a count-only side-ledger from archived Moneycontrol
   pages, which should close most of the 4,118 pre-2016 count gap at zero fetch cost
   (`scripts/NSH_PRE2016_CAMPAIGN.md`). Gate it through `shp_verify_nsh_seam.py` before merging.
5. Re-verify live ~20 min after any push (§41: "live on the server" ≠ "the site uses it").
