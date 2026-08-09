# HUDCO (BSE 540530) — is the consolidated slot a copy of standalone?

**Verdict: NO.** HUDCO's consolidated P&L is genuinely, independently computed every quarter.
It is numerically identical to standalone in two of the three quarters checked because HUDCO's
only consolidating entity is a single equity-method **associate** (M/s Shristi Urban
Infrastructure Development Ltd., SUIDL) whose contribution is usually negligible enough to round
to zero at crore precision — the filings say so explicitly, in writing, every quarter. The one
quarter where the associate's contribution was *not* zero (2022-06-30) shows a real, fully
reconciled, ₹0.05cr PAT gap between the two bases — proof the method detects a genuine divergence
when one exists, so the "identical" verdict on the other two quarters is not a blind spot.

## The three quarters

| Quarter | Filed std revenue | Filed con revenue | Filed std PAT | Filed con PAT | Our revS / revC | Our patS / patC | Verdict |
|---|---|---|---|---|---|---|---|
| 2026-03-31 | 3,562.86 | 3,562.86 | 1,981.31 | 1,981.31 | 3562.86 / 3562.86 | 1981.31 / 1981.31 | GENUINELY_IDENTICAL |
| 2025-09-30 | 3,219.03 | 3,219.03 | 709.83 | 709.83 | 3219.03 / 3219.03 | 709.83 / 709.83 | GENUINELY_IDENTICAL |
| 2022-06-30 (control) | 1,749.27 | 1,749.27 | 411.76 | 411.71 | 1736.42 / 1749.27 | null / 411.71 | NOT a copy — genuine ₹0.05cr PAT gap; our own std slot is off (see below) |

All figures ₹ crore, "Total revenue from Operations" (Interest Income + Dividend + Rental +
Fees/Commission + Net FV gain + Sale of Services — this is an NBFC/HFC filer, `fin=1`) and
"Profit/(loss) for the Period/Year (V-VI)".

## Method

For each quarter: BSE announcement search (scrip 540530) around the quarter-end, download the
result PDF, locate the standalone and consolidated P&L pages, column-anchor the target quarter
by matching its comparative columns against values we already store for neighbouring quarters
(never trusted column 0 blind), and cross-check PBT − Tax == PAT on the same page. BSE detres
(rung 1) was tried first and returned an empty table for this scrip at every QID tested across a
six-year span (Mar-2020 through Mar-2026) — a route-coverage gap, not evidence about the values.
NSE XBRL (rung with the per-basis route) was not attempted — the known 403 lockdown recorded in
this session's context — since the BSE PDF route succeeded immediately and doubly anchored on
every quarter. All fetches ran sequentially in the foreground with ≥2s spacing; no background
jobs were used.

## 2026-03-31 and 2025-09-30 — explicitly identical, and the filing says why

Both filings are combined standalone+consolidated packs. The consolidated P&L is a byte-for-byte
match of the standalone P&L on every income-statement line for these two quarters, and each
filing's "Notes to the Financial Results" states it outright (identical wording both times):

> "The Consolidated Financial Results comprises of the Financial results of the company and an
> Associate company M/s Shristi Urban Infrastructure Development Ltd. (SUIDL). Investments in
> associate company is accounted as per equity method of accounting as per Ind AS-28... The
> figures of Standalone and Consolidated Financial Results remain same as the loss of Associate
> consolidated is negligible and is rounded off as "0" on conversion to crores."

The consolidated statement even carries its own distinct line — "Share in profit/(Loss) of
Associate" — printed as **0.00** on both quarters. That line is proof the consolidated P&L is a
real, separately-derived statement (not a copy-paste of the standalone one) that happens to
resolve to the same numbers, exactly matching this project's own tripwire definition of a
legitimate identity (`scripts/detect_con_copy.py`'s "no subsidiary, or an equity-method
associate" carve-out).

Column anchors: on both filings the target quarter's comparative columns reproduced our stored
neighbour values exactly (2026-03-31 pack: Dec-25 and Mar-25 columns; 2025-09-30 pack: Jun-25 and
Sep-24 columns, plus an internal H1 = Q1+Q2 identity). PBT − Tax == PAT held on every statement
checked.

## 2022-06-30 — the control, and what it actually shows

This is the quarter we already store as "different" (revS 1736.42 vs revC 1749.27 in
`sf_revop.json`). The filing itself is titled *"Submission Of Unaudited Financial Results
(Standalone And Consolidated)..."* — both bases definitely exist. Reading them:

- **Revenue is genuinely identical**, 1,749.27cr both bases — same as every other quarter, for the
  same reason (equity-method associate, no revenue-line impact). Our stored **revS (1736.42) is
  wrong** — it is the filing's "Interest Income" *sub-line*, one of six components under Revenue
  from Operations, not the Total row (which is 1749.27, matching what we already store as revC).
  So the divergence we hold is a parsing artifact in our own standalone slot, not a real
  standalone-vs-consolidated split.
- **PAT genuinely differs, by exactly ₹0.05cr (0.012%)**: standalone 411.76 vs consolidated
  411.71. The consolidated statement's "Share in profit/(Loss) of Associate" line prints **(0.05)**
  this quarter (unlike the 2025-26 quarters, where it was exactly 0.00) — PBT 552.78 → 552.73 →
  PAT 411.76 → 411.71, fully reconciled. Our stored PAT (`sf_revop` patC=411.71, `sf_fundamentals`
  npStd=npCon=411.71, same announce-date on both) currently holds 411.71 in **both** slots — i.e.
  our own standalone PAT is itself a tiny, immaterial copy of consolidated, missing the true
  411.76.

**Does the control detect the difference we store?** Yes, but not the one we expected: the
*revenue* "difference" we hold turned out to be false (a std-slot parsing bug — true values are
identical); the *PAT* difference is real but we don't currently store it (both slots show the
same 411.71, missing the genuine ₹0.05cr split). Either way, the method clearly demonstrated it
can find a real, tiny, fully-reconciled divergence when the primary document has one — which is
exactly what makes the "identical" finding on the other two quarters trustworthy rather than a
blind spot.

## Cross-check: the evidence "against" genuine identity doesn't hold up

`scripts/con_filer_evidence.json` flags HUDCO with `files_con: true` and cites *"E2 screener con
annual != std in FY2026"* as the reason to doubt the identity. Live-checked both Screener pages:
Screener's own **consolidated** FY2026 annual revenue (₹13,150cr) matches the primary filing's
true total (₹13,150.40cr, both bases, verified above) — it is Screener's **standalone** "Sales"
figure (₹13,294cr) that doesn't match anything in the primary document. Screener's FY2026 Net
Profit is identical on both bases (₹4,034cr), matching the filing (₹4,034.37cr both bases). This
points to the con_filer_evidence flag being a Screener-side scrape/definition artifact, not a
genuine business divergence — the primary document shows none for FY2026.

## Bottom line

HUDCO's consolidated slot is **not** holding a copy of standalone. It is an independently
generated statement that is numerically identical to standalone in most quarters purely because
HUDCO has a single, immaterial, equity-method associate and no consolidating subsidiary — exactly
the shape this project's own con-copy tripwire is designed to wave through. Where our *own* store
is wrong for 2022-06-30, the direction is the opposite of a "con-is-a-copy" defect: the standalone
slot is the one holding a wrong value (a mis-picked sub-line for revenue; an immaterial copy of
consolidated for PAT). No repo writes were made — this is a read-only verdict per the mandate.

## Open items (out of scope, not investigated here)

- **2018-12-31** is the *other* quarter our store marks as rev-divergent (revS 1310.53 vs revC
  1284.2). Not one of the three mandated quarters. Given what 2022-06-30 turned out to be, it's a
  reasonable hypothesis that this is a similar parsing artifact rather than a genuine business
  difference — but that needs its own primary-document read before any conclusion.
- `sf_revop.json` HUDCO `20220630[0]` (revS=1736.42) and `sf_fundamentals.json` HUDCO row for
  20220630 (npStd=411.71) both look like defects against the primary filing (true values 1749.27
  and 411.76). Recorded for the record only — no heal was made, per this task's constraints.

## Sources fetched

- 2026-03-31: `https://www.bseindia.com/xml-data/corpfiling/AttachHis/4d28da54-96e0-413f-95f7-667c4631dec8.pdf`
- 2025-09-30: `https://www.bseindia.com/xml-data/corpfiling/AttachHis/a688454a-bac5-47cc-94c5-f3a125320477.pdf`
- 2022-06-30: `https://www.bseindia.com/xml-data/corpfiling/AttachHis/907e248b-9f47-4de9-90ac-65892c1da5de.pdf`
- Screener cross-check: `https://www.screener.in/company/HUDCO/` and `/consolidated/`
