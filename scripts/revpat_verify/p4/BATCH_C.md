# REV/PAT verify campaign — batch_c (5 cells)

READ-ONLY diagnosis. No repo writes, no heals. Full detail per cell in `batch_c_verdicts.json`.

## Verdicts

| symbol | quarter | ours | filed (primary) | independent | verdict | diagnosis | pat_also_off |
|---|---|---:|---:|---:|---|---|---|
| RANBAXY | 2011-12-31 | 119.59 | **3887.72** | 3896.0 | **OURS_WRONG** | other (wrong field) | No |
| CEMPRO | 2024-03-31 | 7.72 | **2236.79** | 2237.0 | **OURS_WRONG** | other (wrong column) | No |
| RTNINDIA | 2023-09-30 | 1.06 | 1.06 | 221.91 | **OURS_CONFIRMED** | site-wrong | No |
| SATHAISPAT | 2022-03-31 | 3.14 | 3.145 (Rev-from-ops) / 145.23 (Total Income) | 145.23 | **AMBIGUOUS_CONCEPT** | ambiguous-concept | No |
| LAHOTIOV | 2024-03-31 | 5.18 | **101.62** | 101.62 | **OURS_WRONG** | other (unexplained) | No |

**3 of 5 need a correction, 2 of 5 don't.** PAT was never off in any of the five — every real defect
sits in the revenue row alone, not the whole cell. None of the ratios turned out to be a genuine
scale (power-of-ten) error, matching the task brief's warning.

## Per-cell narrative

### RANBAXY 2011-12-31 — OURS_WRONG (wrong field, same source)
Re-fetched the *exact* live BSE detres record our own stored value came from (scrip 500359, qtr
72.00 — traced via `pre2015_reads_d.json` provenance: `"src": "bse-detres scrip 500359 qtr 72"`).
The record's `Net Profit` field (-27103.21 ÷10 = -2710.321) reproduces our stored PAT (-2710.32) to
the hundredths place, proving identity. But our stored *revenue* (119.59) is not the record's
`Net Sales/Revenue From Operations` row (38877.18 ÷10 = **3887.72**) — it is the record's `Other
Income → Interest Earned` sub-line (1195.88 ÷10 = 119.588 ≈ 119.59). The original harvest grabbed
the wrong field from a record it otherwise read correctly for PAT. Confirmed isolated to this one
quarter: the adjacent Mar-2012 quarter (qtr 73.00), re-fetched the same way, matches our
already-stored value exactly. Company delisted 2015 (Sun Pharma merger) but detres reaches this
scrip back to Dec-2011, contradicting the runbook's stated "2015+" floor for that route — flagged as
a runbook update opportunity, not acted on here.

### CEMPRO 2024-03-31 — OURS_WRONG (prior read landed one column off)
ITD Cementation India Ltd's own FY2025 annual filing (page 5, standalone) prints five columns dated
31.03.2025 / 31.12.2024 / 31.03.2024 / FY25 / FY24. The 31.03.2024 column's PAT (89.5152) matches our
stored PAT (89.52) and anchors the read: `Revenue from operations` there is 2,23,678.59 lakh =
**2236.79 cr**. A prior campaign pass (2026-08-07 ledger) read the *same PDF* but landed on the
31.12.2024 column instead (Total Income 2222.18), logging it UNRESOLVED — an off-by-one column read,
not a genuine conflict. Second check closes it exactly: FY24 annual revenue (7542.11) minus our own
already-stored Jun+Sep+Dec-23 (5305.32) = 2236.79, to the paisa.

### RTNINDIA 2023-09-30 — OURS_CONFIRMED (site is wrong)
RattanIndia Enterprises Ltd's standalone statement (page 6) prints `Revenue from operations` = 10.56
Rs mn = **1.06 cr** for the quarter ended 30.09.2023 — exactly our stored value. PAT for the same
column (207.56) matches ours exactly, and H1 YTD (23.81 = 13.25+10.56) reconciles. REL is a holding
company; its tiny standalone revenue vs. ~₹1,600 cr consolidated is structural, not an error (the
consolidated side was already corrected in an earlier campaign pass). The independent source
(221.91) does not reproduce any row/column/scale in the filing — it is simply wrong here. Checked the
adjacent Dec-2023 quarter too (task flagged "2 qtrs"): same shape, same site-wrong outcome, already
in the existing ledger with 228.98 vs. the correct 2.03.

### SATHAISPAT 2022-03-31 — AMBIGUOUS_CONCEPT (both numbers are right, different rows)
Company was absent from our BSE scrip master (delisted-adjacent); resolved via web search to BSE
526093 = Sathavahana Ispat Ltd. Live BSE detres for that scrip/quarter: `Net Sales/Revenue From
Operations` = 31.45 mn = 3.145 cr (matches ours). `Other Income` = 1420.83 mn — a huge one-off
(plausibly a debt waiver/write-back at a distressed steel company) — brings `Total Income` to
1452.28 mn = **145.228 cr**, which is what the independent source actually reports. PAT anchor
(116.508 vs. our stored 116.51) confirms identity. Both figures are correct; they measure different
things. Our field is specifically "revenue from operations" throughout this dataset, so 3.14 stands.

### LAHOTIOV 2024-03-31 — OURS_WRONG (unexplained, but conclusively wrong)
Lahoti Overseas Ltd (BSE 531842, confirmed via web search), a cotton-yarn merchant exporter. Live BSE
detres for the target quarter: `Net Sales/Revenue From Operations` = 1016.24 mn = **101.62 cr**,
essentially exact to the independent source. `Net Profit` in the same record (-9.18÷10=-0.918)
matches our stored PAT (-0.92) almost exactly, confirming identity. No local provenance ledger
explains how 5.18 was originally derived (this symbol postdates the harvest campaigns that leave a
trail), so the mechanism is unknown — but the value is definitively wrong. FY24 annual total
(487.54) minus our three other known quarters implies a plausible Jun-2023 quarter (~121.56),
corroborating the fix from a second angle.

**Distinct-defect warning:** LAHOTIOV carries a *separate*, already-documented ×100 lakh-as-crore
PAT defect on neighbouring cells (runbook §59d/§59e: Mar-2025 PAT stored 404.11 should be 4.04;
Jun-2024 PAT stored 453.78 looks like the same class; both still unfixed live). This revenue cell is
NOT that bug — 19.6x is not a clean power of ten, and this quarter's own PAT checks out fine. Two
unrelated defect classes on the same company; do not conflate them in any future heal.

## What did NOT get walked
NSE archive (§52/§53) live fetches were not separately attempted for any cell — the BSE detres route
(re-fetched live, PAT-anchored, internally reconciled) or the cached BSE announcement PDFs already
produced fully-anchored answers for all five, so escalating further up the ladder was unnecessary.
BSE PeerSmartSearch was tried once for SATHAISPAT and bounced (malformed query on my end, not a
403/429 — abandoned in favour of a web search, which worked). No 403/429 was hit anywhere in this
batch; all live BSE detres calls succeeded on the first or second try with ≥2s spacing.
