# AADHARHFC standalone revenue — wrong-row verification (2026-08-09)

**Scope:** `docs/sf_revop.json["AADHARHFC"][QE][0]` (standalone revenue, revS) for the 4 quarters named in
the brief: 2023-06-30, 2023-12-31, 2024-06-30 (control boundary), 2025-09-30 (control). No repo writes,
no heals — this file plus `aadharhfc_verdicts.json` is the deliverable.

## Verdict table

| quarter | ours (revS) | filed Total revenue from ops | ours matches | verdict |
|---|---|---|---|---|
| 2023-06-30 | 533.47 | **578.01** | `a) Interest income` exactly (533.47=533.47) | **OURS_WRONG** |
| 2023-12-31 | 579.26 | **658.54** | `a) Interest income` exactly (579.26=579.26) | **OURS_WRONG** |
| 2024-06-30 | 696.78 | **696.78** | `Total revenue from operations` exactly — correct row | **OURS_CONFIRMED** |
| 2025-09-30 (control) | 897.06 | **897.06** | `Total revenue from operations` exactly — correct row | **OURS_CONFIRMED** |

**Control result:** the 2025-09-30 control quarter came back matching (897.06 = 897.06), exactly as the
brief said it should if the method is sound. That licenses trusting the OURS_WRONG calls on the two 2023
quarters.

**Mechanism, one line:** the two 2023 quarters store the **`Interest income`** sub-line where
`sf_revop[0]` is supposed to hold **`Total revenue from operations`** (Interest income + Fees &
commission + Net gain on fair value changes + Net gain on derecognition of financial instruments) — the
same defect class as the HUDCO 2022-06-30 precedent. From 2024-03-31 onward the correct row is stored.

## Why this needed 2 mechanisms, not 1

The brief's own evidence table showed a gap that shrinks steadily from -12.1% (Dec-2023) to -1.6%
(Dec-2024) to ~0% (2025), which reads like a single fading effect. It is not. Two unrelated things
are stacked on top of each other:

1. **2023-06-30 / 2023-09-30 / 2023-12-31 (pre-IPO): a genuine wrong-row defect.** AADHAR Housing
   Finance's equity IPO listed **8 May 2024**; before that it had only listed NCDs/CPs (BSE debt
   segment), so it filed *standalone-only* Reg. 52 results and was invisible to the equity scrip
   (544176), to BSE detres, and to NSE XBRL. Those 3 quarters were populated by
   `scripts/screener_prerev.py`, a pre-IPO-only Screener scraper that pulls whichever quarters-table
   row is literally labelled `Sales` (falling back to `Revenue`) and accepts it once PAT matches
   `sf_fundamentals` within tolerance. The **PAT-only anchor let a wrong revenue row through**: the
   row it grabbed was Interest income, not the aggregate. Confirmed exactly (to the cent) against
   the primary BSE filing for both quarters checked, plus a bonus third quarter (2023-09-30, not
   in this packet's 4 but checked as a side-effect of reading the Dec-2023 filing's comparative
   column: stored 560.59 = filed Interest income 560.59 exactly).
2. **2024-06-30 onward: revS is already correct**; the residual small gaps in the brief's table for
   2024 (-2.7% to -1.6%) are a **std-vs-con display mismatch**, not a row defect. Screener's default
   (un-suffixed) company page shows **consolidated** figures. At 2024-06-30 the filing's consolidated
   Total revenue from operations is 713.14 — exactly Screener's displayed 713 — while our own
   consolidated slot (`sf_revop[1]`) for that quarter *already holds 713.14*, exactly right. Comparing
   our standalone slot against Screener's consolidated display manufactures a gap that isn't a defect
   in either stored number.

## Route ladder walked (both defective quarters)

Standard routes are structurally blind pre-IPO — this is a real "not attempted for a legitimate
reason", not a route-returned-nothing-so-give-up call (runbook §57a rule 1):

1. BSE detres (`Corp_detailedResult_Transpose_ng`, equity scrip 544176) → `no-date-begin-end-in-response`
   (no filing exists at this scrip pre-listing).
2. NSE per-basis XBRL (symbol AADHARHFC) → 0 list rows for either basis (not NSE-listed yet).
3. BSE announcement stream via the equity scrip (544176) → earliest row is 2024-05-29; nothing
   pre-listing.
4. **BSE announcement stream via the BSE DEBT-segment scrip codes** (runbook §44's playbook, applied
   to a *pre-IPO NCD issuer* rather than its usual delisted-issuer case) → **hit**. AADHARHFC has ~90
   listed NCD/CP codes on file (`scripts/bse_scrips.json` doesn't carry them; pulled live from
   `ListofScripData?segment=Debt`); any of the long-tenor ones (tried 936320, 973564, 955091, 959387)
   broadcasts the identical Financial Results PDF, because BSE fans a single issuer filing out to
   every one of the issuer's listed instruments.

Rung 4 is the reusable finding for this packet: **any pre-IPO Nifty500 company that reads as
"revenue unfillable, not listed yet" should be tried against its BSE debt scrips before being
written off** — most recent HFC/NBFC IPOs (this campaign's `backfill_preipo.py` target list) carry
listed NCDs for years before their equity IPO and file full Reg. 52 standalone results throughout.

## Evidence detail

### 2023-06-30 (OURS_WRONG)
Source: `AttachHis/99ea0928-7f15-4732-8857-78bf1e975d26.pdf`, filed 2023-08-09, Reg. 52 standalone
only (no consolidated statement existed at this date). Unit: **Rs in Lakh → ÷100 for crore**.

| row (Rs Lakh) | Jun-23 | Mar-23 | Jun-22 | FY23 |
|---|---|---|---|---|
| a) Interest income | 53,347 | 47,673 | 41,067 | 1,77,628 |
| b) Fees and commission income | 1,779 | 1,339 | 1,119 | 5,559 |
| c) Net gain on fair value changes | 492 | 735 | 704 | 3,173 |
| d) Net gain on derecognition of fin. instr. | 2,183 | 4,064 | 1,706 | 13,043 |
| **Total revenue from operations** | **57,801** | 53,811 | 44,596 | 1,99,403 |
| Other income | 1 | 8 | 2 | 24 |
| Total income | 57,802 | 53,819 | 44,598 | 1,99,427 |
| 7 Profit after tax | 14,628 | 14,061 | 11,575 | 54,458 |

Column anchor: PAT 146.28/140.61/115.75 (cr) vs stored 146.0/141.0/**115.75 exact**. Second check:
53,347+1,779+492+2,183 = 57,801 exactly. **Our stored 533.47 = row (a) 53,347/100, to the cent.**

### 2023-12-31 (OURS_WRONG)
Source: `AttachHis/d307a09a-5579-477f-8be5-49e2c23e3f59.pdf`, filed 2024-02-08, Reg. 52 standalone.

| row (Rs Lakh) | Dec-23 | Sep-23 | Dec-22 |
|---|---|---|---|
| a) Interest income | 57,926 | 56,059 | 46,599 |
| b) Fees and commission income | 3,627 | 2,451 | 1,370 |
| c) Net gain on fair value changes | 654 | 333 | 850 |
| d) Net gain on derecognition of fin. instr. | 3,647 | 2,557 | 3,749 |
| **Total revenue from operations** | **65,854** | 61,400 | 52,568 |
| 7 Profit after tax | 20,344 | 19,728 | 15,852 |

Column anchor: PAT 203.44/197.28 vs stored 203.0/197.0; year-ago Dec-22 158.52 vs stored
**158.52 exact**. Second check: component sum = 65,854 exact; 9M YTD Jun+Sep+Dec = 57,801+61,400+65,854
= 1,85,055 = the filed 9-month figure exactly. **Our stored 579.26 = row (a) 57,926/100, to the cent.**
(Side finding, not adjudicated: Sep-23 col shows the same pattern — stored 560.59 = Interest income
560.59 exactly, filed total 614.00 — but 2023-09-30 wasn't one of this packet's 4 target quarters.)

### 2024-06-30 (OURS_CONFIRMED — boundary quarter)
Source: `AttachHis/9edef400-d6a5-492b-be1c-359824715309.pdf`, filed 2024-08-07, first filing under the
equity scrip, Standalone **and** Consolidated. The financial-statement pages in this PDF are
**image-only** (`get_text()` → 0 chars on pages 3–11 despite embedded images) — read from a 2.5×
rendered PNG per runbook §57 rung 10.

| row (Rs Lakh) | STD Jun-24 | STD Mar-24 | CON Jun-24 | CON Mar-24 |
|---|---|---|---|---|
| a) Interest income | 63,440 | 59,601 | 63,440 | 59,601 |
| b) Fees and commission income | 2,494 | 3,259 | 4,123 | 5,131 |
| c) Net gain on fair value changes | 551 | 735 | 558 | 744 |
| d) Net gain on derecognition | 3,193 | 3,703 | 3,193 | 3,703 |
| **Total revenue from operations** | **69,678** | 67,298 | **71,314** | 69,179 |
| 5 Profit after tax | 20,008 | 20,151 | 20,014 | 20,176 |

Anchors (all 4 exact): std PAT 200.08 = stored npStd; con PAT 200.14 = stored npCon; std Mar-24
total-rev-ops 672.98 = stored 2024-03-31 revS; con Mar-24 total-rev-ops 691.79 = stored 2024-03-31
revC. **Our stored 696.78 = 69,678/100 = Total revenue from operations STANDALONE, exactly — the
correct row.** The brief's -2.3% "gap" vs Screener's 713 is Screener showing **consolidated**
(713.14, which our own con slot already holds correctly) against our standalone — a basis mismatch,
not a defect.

### 2025-09-30 (OURS_CONFIRMED — control)
Source: `AttachHis/5fba43a6-9185-463c-b346-b8d6d7b9ca4f.pdf`, filed 2025-11-07, equity scrip.
Text layer partially corrupted (OCR-style glyph noise, e.g. header reads `oo~diTffl`) — caught a
**digit misread**: raw `get_text()` gave Fees-and-commission-income = 4,953, which fails the
component-sum check by 5 lakh (89,701 vs printed 89,706). Rendered the income block at 5× and
visually confirmed the true digit is **4,958** (an 8-for-3 misread — same trap class as the
runbook's GICRE 8-for-9 case); with 4,958 the sum reconciles exactly.

| row (Rs Lakh) | Sep-25 | Jun-25 |
|---|---|---|
| a) Interest income | 79,937 | 76,010 |
| b) Fees and commission income | 4,958 (visually corrected from OCR 4,953) | 5,078 |
| c) Net gain on fair value changes | 403 | 473 |
| d) Net gain on derecognition | 4,408 | 3,250 |
| **Total revenue from operations** | **89,706** | 84,811 |
| 5 Profit after tax | 26,635 | 23,730 |

Anchor: PAT 266.35 = stored npStd exactly; Jun-25 comparative total-rev-ops 848.11 = stored
2025-06-30 revS exactly. **Our stored 897.06 = 89,706/100 = Total revenue from operations, exactly
— confirmed as the correct row, as the control should show.**

## Open item (not adjudicated, out of scope for this packet)

`sf_revop[SYM][QE][1]` (consolidated revenue) for the 3 pre-IPO 2023 quarters holds a value that is
**neither equal to revS nor traceable to any filing** — no consolidated statement was filed at all
pre-IPO (both PDFs read here are explicitly "Standalone Statement... pursuant to Regulation 52",
no Reg. 33 pack attached). Where 593.42 / 628.55 / 673.05 (revC for Jun/Sep/Dec-2023) came from is
unresolved. Flagging for a future packet; the brief scoped this session to revS only and this note
should not be read as a verdict.

## Files
- `aadharhfc_verdicts.json` — structured verdicts, all 4 quarters, per the requested schema.
- `cache/` — every fetched BSE JSON/PDF response, keyed by URL (re-runnable, zero re-fetch cost).
- `render/` — PNG renders used for the two image-only / OCR-suspect reads (2024-06-30, 2025-09-30).
- `tools/exchange_fetch.py`, `tools/bse_ann.py` — copies of/derived from the reference tree's fetch
  tooling, cache path redirected to this working dir only (never wrote into the reference tree).
