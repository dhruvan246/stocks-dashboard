# Screener.in quarterly REV/PAT — full 66-symbol campaign notes

Fetched 2026-08-09. All 66 campaign symbols x 2 bases = 132 pages. 14 symbols reused from the
pilot's cached pages (`pages/*.html`), 52 newly fetched via `fetch2.sh`, 2 ampersand symbols
via `fetch_amp.sh`. Grand total requests across pilot + this run: 24 (pilot) + 100 (fetch2) + 8
(fetch_amp, includes redundant bare-form probe, see below) = 132 requests. **Zero 403/429,
zero HTTP-level 404s anywhere.** 2s spacing respected throughout; `/company/source/quarter/*`
never fetched.

`screener_p3.jsonl`: 1565 lines, validated with `python3 -c "import json;[json.loads(l) for l
in open('screener_p3.jsonl')]"` (clean, no errors). Self-contained — includes the 14 pilot
symbols alongside the 52 new ones.

## Ampersand symbols — resolved

Both `GMRP&UI` and `M&MFIN` resolved cleanly on the **`%26`-encoded URL form** (HTTP 200 on
both std and con). Note: `fetch_amp.sh` had a shell command-substitution bug (the `tee` inside
`try_one` polluted the captured `$code`, so the bare-`&` fallback fired unconditionally instead
of only on a non-200) — as a result the bare-ampersand form was *also* fetched for both symbols
as a side effect. The bare form ALSO returned 200 for both (`GMRP&UI` and `M&MFIN` bare pages
exist in `pages/*_bare*.html`), so this was a harmless redundant fetch, not a failure — but the
canonical / correct form recorded in `screener_p3.jsonl`'s `url` field, and the one actually
used to parse rows into the JSONL for these two symbols, is the **`%26`** form
(`https://www.screener.in/company/GMRP%26UI/`, `https://www.screener.in/company/M%26MFIN/`),
consistent with what robots.txt/the URL spec expects. The bare-form pages were not parsed into
the output.

## Coverage summary

- **66/66 symbols yielded standalone (`std`) data.**
- **59/66 symbols yielded consolidated (`con`) data.** 7 symbols have a live, HTTP-200,
  fully-rendered "Quarterly Results" section on their `/consolidated/` page with **zero date
  columns and zero values** — a real source-side absence, not a parse failure (same pattern the
  pilot found for SBILIFE and MSUMI). The 7: **ENRIN, GODIGIT, MSUMI, NIVABUPA, POWERINDIA,
  SBILIFE, STARHEALTH**.
  - Notably, this list is **identical** to the set of symbols whose standalone unit string is
    the bare `"Figures in Rs. Crores"` (no "Standalone"/"Consolidated" qualifier, no
    View-Consolidated/View-Standalone toggle link) — confirming the pilot's read: Screener does
    not carry a separate consolidated quarterly series for these names at all; the bare-unit
    label is essentially a marker for "no basis split exists on this page."
- Quarter span across the whole file: **2018-06-30 -> 2026-06-30** (CASTEXTECH's window is the
  outlier that pulls the floor down — see below; almost everything else sits in 2022-2026).
- No 404s / renamed-ticker failures in the new 52. (The pilot separately measured
  IBULHSGFIN->SAMMAANCAP as a hard 404-no-alias case; not repeated here since it wasn't in this
  66-symbol list.)

## Row template split (all 66)

- **Bank/NBFC template** (`Revenue` / `Interest` near top / `Expenses` / `Financing Profit` /
  `Financing Margin %` / ... / `Gross NPA %` / `Net NPA %`): **AADHARHFC, BAJFINANCE,
  FEDERALBNK, HDFCBANK, HUDCO, M&MFIN, MUTHOOTFIN, RBLBANK, SBIN, SUNDARMFIN** (10 symbols).
- **Industrial template** (`Sales` / `Expenses` / `Operating Profit` / `OPM %` / `Other Income`
  / `Interest` / `Depreciation` / `Profit before tax` / `Tax %` / `Net Profit` / `EPS in Rs`):
  all other 56 symbols, including every insurer in the set (GICRE, LICI, NIACL, SBILIFE,
  STARHEALTH, NIVABUPA, GODIGIT) and every AMC/broker/fintech (ABSLAMC, POLICYBZR, PINELABS,
  JIOFIN, INDIAMART, SAGILITY). Confirms the pilot's finding that Screener has no
  insurer-specific template — insurers get the plain industrial layout.

## FLAG — identical std/con values (possible single-basis filing or served copy)

Checked every (symbol, quarter) pair where both bases have data, comparing on the row labels
common to both. Four symbols have at least one fully-identical quarter:

- **HUDCO: ALL 13/13 common quarters are byte-identical between std and con** (every row,
  every quarter, 2023-06-30 through 2026-06-30). This is the strongest case in the whole
  sample — either HUDCO's consolidated results are line-for-line equal to standalone every
  single quarter (plausible for an NBFC with negligible/no subsidiary contribution), or
  Screener is serving the same underlying figures under both URLs for this company. Worth
  independent verification before use.
- **MEESHO: 1/7 common quarters identical** (2025-06-30) — already flagged in the pilot;
  repeated here since MEESHO is a pilot symbol and this file is self-contained.
- **AADHARHFC: 1/13 common quarters identical** (2026-06-30, the most recent quarter — could be
  a not-yet-fully-consolidated provisional figure, or a genuine subsidiary-free quarter).
- **CASTEXTECH: 2/13 common quarters identical** (2018-12-31, 2020-12-31 — non-adjacent
  quarters, not the whole window, so less likely to be a systematic copy).

Flagging only, per task scope — not diagnosing which (if any) of these reflects a real
single-basis filing vs. a site-side data artifact.

## FLAG — std/con quarter windows don't line up (5 symbols)

- **GRASIM**: std has 12 quarters, 2023-06-30 -> 2026-03-31 (no Jun-2026 yet). con has 13
  quarters, 2023-03-31 -> 2026-03-31 (has an extra Mar-2023 quarter std lacks). Same as pilot's
  original finding, repeated here.
- **AJRINFRA**: std 13 quarters, 2022-12-31 -> 2026-03-31. con 13 quarters, 2023-03-31 ->
  2026-03-31. std runs one quarter earlier at the front; both share the same 2026-03-31 tail.
- **PREMIERENE**: std 12 quarters, 2023-09-30 -> 2026-06-30. con 13 quarters, 2023-06-30 ->
  2026-06-30 (con has one extra quarter at the front that std lacks).
- **SAGILITY**: std has an **internal gap**, not just a shorter window — measured directly off
  the raw HTML `data-date-key` list (not a parsing artifact): std's own page only ever offers
  `['2023-09-30','2023-12-31','2024-06-30','2024-09-30','2024-12-31','2025-03-31','2025-06-30',
  '2025-09-30','2025-12-31','2026-03-31','2026-06-30']` — **2024-03-31 is missing entirely**
  from the standalone table's own column headers, sandwiched between two present quarters. con
  has the full contiguous 13-quarter window 2023-06-30 -> 2026-06-30 with no gap. Sagility
  India IPO'd in Nov-2024, so any standalone-only reporting before that is itself notable; the
  mid-window gap is the more unusual part and is reported as measured, not explained.
- **PINELABS**: std 7 quarters, 2024-09-30 -> 2026-06-30 (skips 2024-12-31). con 8 quarters,
  2024-09-30 -> 2026-06-30 (has 2024-12-31, std doesn't). Same pattern as SAGILITY/PREMIERENE —
  std missing an interior or leading quarter that con has.

## FLAG — other window anomalies

- **CASTEXTECH**: both std and con show a **stale, old 13-quarter window: 2018-06-30 ->
  2021-06-30** — five years behind every other symbol in the sample (which cluster around
  2023-2026). This does not look like a scrape error (both bases agree, and the "fixed 13-col
  trailing window" behaviour documented in the pilot's capability card is exactly what would
  produce this if the company simply stopped filing quarterly results after mid-2021, i.e. the
  window is trailing from its *last filed quarter*, not from today). Consistent with a company
  that went dormant / was suspended from trading / stopped disclosure around 2021 — not
  diagnosed further here, flagging as a genuine outlier for downstream review.
- **ZEAL**: both std and con report on an unusual **semi-annual cadence** — measured directly
  off the raw HTML, the page has exactly 9 date columns total and they alternate
  Mar-31/Sep-30 only (`2022-03-31, 2022-09-30, 2023-03-31, 2023-09-30, 2024-03-31, 2024-09-30,
  2025-03-31, 2025-09-30, 2026-03-31`) — **no Jun-30 or Dec-31 column exists anywhere on the
  page**, not a filter artifact from this campaign's QTR_ENDS restriction. If ZEAL genuinely
  reports quarterly elsewhere and Screener is only capturing 2 of 4 quarters/year, that is a
  Screener-side gap; if ZEAL itself only discloses semi-annually, that's a company-level fact.
  Either way, downstream tools should not assume ZEAL has Jun/Dec figures available.
- Several symbols anchor on **March-only quarter-ends instead of the default
  Jun-2023->Jun-2026 window**: FLUOROCHEM, GICRE, GMRP&UI, HONASA, INDOBORAX, JUBLPHARMA (all
  2023-03-31 -> 2026-03-31, both bases agree per symbol) — consistent with those companies'
  most recently filed quarter being Mar-2026 rather than Jun-2026 at the time of this fetch.
- **Young-listing short windows** (expected, not anomalies — company simply doesn't have 13
  quarters of history yet): IGIL (11), MEESHO (7), NTPCGREEN (11), PINELABS (7/8), PWL (7),
  NIVABUPA (12, std only).

## Per-symbol coverage table

`sym | std: n_qtrs [oldest..newest] | con: n_qtrs [oldest..newest] | std unit | con unit`

```
AADHARHFC   | std:13 [2023-06-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores
AARTIPHARM  | std:13 [2023-06-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores
ABFRL       | std:13 [2023-06-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores
ABSLAMC     | std:13 [2023-06-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores
AJANTPHARM  | std:13 [2023-06-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores
AJRINFRA    | std:13 [2022-12-31..2026-03-31] | con:13 [2023-03-31..2026-03-31] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores   <- window mismatch, see FLAG
BAJFINANCE  | std:13 [2023-06-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores   (pilot)
BALKRISIND  | std:13 [2023-06-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores
BPCL        | std:13 [2023-06-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores
CASTEXTECH  | std:13 [2018-06-30..2021-06-30] | con:13 [2018-06-30..2021-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores   <- stale window, see FLAG
CCL         | std:13 [2023-06-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores
CUMMINSIND  | std:13 [2023-06-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores
EICHERMOT   | std:13 [2023-06-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores
ENRIN       | std:9  [2024-03-31..2026-06-30] | con: 0 (empty table)            | Figures in Rs. Crores             | Figures in Rs. Crores (no data)      <- no con data
ETERNAL     | std:13 [2023-06-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores   (pilot)
FEDERALBNK  | std:13 [2023-06-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores
FLUOROCHEM  | std:13 [2023-03-31..2026-03-31] | con:13 [2023-03-31..2026-03-31] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores
GICRE       | std:13 [2023-03-31..2026-03-31] | con:13 [2023-03-31..2026-03-31] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores   (pilot)
GMRP&UI     | std:13 [2023-03-31..2026-03-31] | con:13 [2023-03-31..2026-03-31] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores   <- ampersand, %26 worked
GODIGIT     | std:13 [2023-06-30..2026-06-30] | con: 0 (empty table)            | Figures in Rs. Crores             | Figures in Rs. Crores (no data)      <- no con data
GRASIM      | std:12 [2023-06-30..2026-03-31] | con:13 [2023-03-31..2026-03-31] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores   (pilot) <- window mismatch
HDFCBANK    | std:13 [2023-06-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores   (pilot)
HEROMOTOCO  | std:13 [2023-06-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores
HINDALCO    | std:13 [2023-06-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores
HONASA      | std:13 [2023-03-31..2026-03-31] | con:13 [2023-03-31..2026-03-31] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores
HUDCO       | std:13 [2023-06-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores   <- ALL quarters identical std=con, see FLAG
IGIL        | std:11 [2023-12-31..2026-06-30] | con:11 [2023-12-31..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores
INDIAMART   | std:13 [2023-06-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores
INDOBORAX   | std:13 [2023-03-31..2026-03-31] | con:13 [2023-03-31..2026-03-31] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores
JIOFIN      | std:13 [2023-06-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores
JUBLPHARMA  | std:13 [2023-03-31..2026-03-31] | con:13 [2023-03-31..2026-03-31] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores
LICI        | std:13 [2023-06-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores   (pilot)
M&MFIN      | std:13 [2023-06-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores   <- ampersand, %26 worked
MEESHO      | std:7  [2024-12-31..2026-06-30] | con:7  [2024-12-31..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores   (pilot) <- 1 qtr identical std=con
MOTHERSON   | std:13 [2023-06-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores   (pilot)
MSUMI       | std:13 [2023-06-30..2026-06-30] | con: 0 (empty table)            | Figures in Rs. Crores             | Figures in Rs. Crores (no data)      (pilot) <- no con data
MUTHOOTFIN  | std:13 [2023-06-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores
NCC         | std:13 [2023-06-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores
NIACL       | std:13 [2023-06-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores
NIVABUPA    | std:12 [2023-09-30..2026-06-30] | con: 0 (empty table)            | Figures in Rs. Crores             | Figures in Rs. Crores (no data)      <- no con data
NTPCGREEN   | std:11 [2023-12-31..2026-06-30] | con:11 [2023-12-31..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores
PINELABS    | std:7  [2024-09-30..2026-06-30] | con:8  [2024-09-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores   <- window mismatch, see FLAG
POLICYBZR   | std:13 [2023-06-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores
POLYMED     | std:13 [2023-06-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores
POWERINDIA  | std:13 [2023-06-30..2026-06-30] | con: 0 (empty table)            | Figures in Rs. Crores             | Figures in Rs. Crores (no data)      <- no con data
PREMIERENE  | std:12 [2023-09-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores   <- window mismatch, see FLAG
PWL         | std:7  [2024-09-30..2026-03-31] | con:7  [2024-09-30..2026-03-31] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores
RADICO      | std:13 [2023-06-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores
RAJRATAN    | std:13 [2023-06-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores
RBLBANK     | std:13 [2023-06-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores
RELIANCE    | std:13 [2023-06-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores   (pilot)
SAGILITY    | std:11 [2023-09-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores   <- internal gap (missing 2024-03-31), see FLAG
SAIL        | std:13 [2023-06-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores   (pilot)
SBILIFE     | std:13 [2023-06-30..2026-06-30] | con: 0 (empty table)            | Figures in Rs. Crores             | Figures in Rs. Crores (no data)      (pilot) <- no con data
SBIN        | std:13 [2023-06-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores   (pilot)
SHK         | std:13 [2023-06-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores
SOBHA       | std:13 [2023-06-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores
STARHEALTH  | std:13 [2023-06-30..2026-06-30] | con: 0 (empty table)            | Figures in Rs. Crores             | Figures in Rs. Crores (no data)      <- no con data
SUNDARMFIN  | std:13 [2023-06-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores
SUPREMEIND  | std:13 [2023-06-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores
TARIL       | std:13 [2023-06-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores
TATAINVEST  | std:13 [2023-06-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores
TATASTEEL   | std:13 [2023-06-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores   (pilot)
TECHM       | std:13 [2023-06-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores
ZEAL        | std:9  [2022-03-31..2026-03-31] | con:9  [2022-03-31..2026-03-31] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores   <- semi-annual only, see FLAG
ZFCVINDIA   | std:13 [2023-06-30..2026-06-30] | con:13 [2023-06-30..2026-06-30] | Standalone Figures in Rs. Crores | Consolidated Figures in Rs. Crores
```

`(pilot)` marks the 14 symbols carried over from the original pilot run (cached pages reused,
not refetched).

## No 404s / 403s / 429s

All 132 page requests across the pilot + this run returned HTTP 200 (plus 4 harmless redundant
bare-ampersand probes, also 200, from the `fetch_amp.sh` bug noted above). Zero 403/429 at any
point. No renamed/delisted ticker produced a hard failure in this 66-symbol set.
