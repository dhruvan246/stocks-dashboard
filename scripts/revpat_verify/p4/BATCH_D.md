# Batch D revenue verdicts (2026-08-10)

**Scope:** arbitrate suspected wrong-row `docs/sf_revop.json[SYM][QE][0]` (standalone revenue)
cells for GYFTR, PIRAMALFIN, TVSHLTD, SPICEJET, JPASSOCIAT, THEMISMED against their own BSE
filings. No repo writes, no heals — this file plus `batch_d_verdicts.json` is the deliverable.

## Verdict table

| symbol | quarter | ours | filed value | ours matches | verdict |
|---|---|---|---|---|---|
| GYFTR | 2023-06-30 | 3.28 | Total Rev Ops **19.09** | `Interest income` exactly (3.28=3.28) | **OURS_WRONG** |
| GYFTR | 2023-09-30 (worst, -87.1%) | 3.11 | Total Rev Ops **24.18** | `Interest income` exactly (3.11=3.11) | **OURS_WRONG** |
| GYFTR | 2023-12-31 (bonus) | 4.42 | -- | `Interest income` exactly | **OURS_WRONG** |
| GYFTR | 2024-06-30 revC (bonus, unflagged) | 3.16 | Total Rev Ops **20.74** | `Interest income` (con) exactly | **OURS_WRONG** |
| PIRAMALFIN | 2024-12-31 (worst, -21.5%) | 2205.61 | as-filed **1980.97** / restated **2229.08** | no row matches to the cent; PAT sign FLIPS (ours +69.0cr vs filed -112.30cr) | **UNRESOLVED** |
| PIRAMALFIN | 2025-03-31 (2nd worst, -19.0%) | 2315.89 | std **2377.67** / con **2395.46** | no row matches to the cent; PAT ~20% off, same sign | **UNRESOLVED** |
| PIRAMALFIN | 2025-06-30 (control) | 2658.04 | **2658.04** | `Total Revenue from operations` exactly | **OURS_CONFIRMED** |
| PIRAMALFIN | 2025-09-30 (bonus) | 2594.11 | Total Rev Ops **2842.63** | `Interest income` exactly (2594.11=2594.11) | **OURS_WRONG** |
| TVSHLTD | 2023-06-30 | 555.96 | Rev-from-ops **555.96**; Total Income 589.02 | `Revenue from operations` exactly; Screener shows Total Income | **AMBIGUOUS_CONCEPT** |
| TVSHLTD | 2023-09-30 (worst, -19.5%) | 390.16 | Rev-from-ops **390.16**; Total Income 484.59 | `Revenue from operations` exactly; Screener shows Total Income | **AMBIGUOUS_CONCEPT** |
| TVSHLTD | 2024-03-31 (control) | 370.54 | **370.54** | `Revenue from operations` exactly, con NSE+BSE detres | **OURS_CONFIRMED** |
| SPICEJET | 2024-06-30 (worst, -7.7%) | 1565.16 | Total Rev Ops **1695.52** | `Revenue from operations` (row a, excl. Other operating revenues) exactly | **OURS_WRONG** |
| SPICEJET | 2025-06-30 (median, -6.6%) | 1033.36 | Total Rev Ops **1106.06** | row (a) exactly | **OURS_WRONG** |
| SPICEJET | 2025-03-31 (bonus) | 1360.87 | Total Rev Ops **1446.38** | row (a) exactly | **OURS_WRONG** |
| SPICEJET | 2022-09-30 (control) | 1952.62 | **1952.62** | `Total revenue from operations` exactly | **OURS_CONFIRMED** |
| JPASSOCIAT | 2023-12-31 (-18.1%) | 710.09 | **710.09** | `Revenue from Operations` exactly (Screener's 867.0 unreconciled) | **OURS_CONFIRMED** |
| JPASSOCIAT | 2024-03-31 (worst, -21.0%) | 935.42 | **935.42** | `Revenue from Operations` exactly (Screener's 1184.0 unreconciled) | **OURS_CONFIRMED** |
| JPASSOCIAT | 2023-09-30 (control) | 1213.61 | **1213.61** | `Revenue from Operations` exactly; Screener agrees here | **OURS_CONFIRMED** |
| THEMISMED | 2024-06-30 (worst, -14.9%) | 106.08 | std **106.08**; con 122.99 | standalone `Revenue from Operations` exactly; Screener shows CONSOLIDATED | **OURS_CONFIRMED** |
| THEMISMED | 2024-09-30 (-14.3%) | 99.59 | std **99.59**; con 117.01 | standalone exactly; Screener shows CONSOLIDATED | **OURS_CONFIRMED** |
| THEMISMED | 2023-12-31 (control) | 82.51 | **82.51** | exact, std==con | **OURS_CONFIRMED** |

## Control results

| symbol | control quarter | passed? |
|---|---|---|
| GYFTR | **none exists** — every populated cell in this symbol's entire stored history (5 cells total) reproduces the same defect; substituted an un-flagged cell (2024-06-30, revC) which also failed, reinforcing rather than validating | N/A, see notes |
| PIRAMALFIN | 2025-06-30 | **PASSED** (exact) |
| TVSHLTD | 2024-03-31 | **PASSED** (exact, via BSE detres + NSE XBRL) |
| SPICEJET | 2022-09-30 | **PASSED** (exact) |
| JPASSOCIAT | 2023-09-30 | **PASSED** (exact, and Screener also agrees there) |
| THEMISMED | 2023-12-31 | **PASSED** (exact) |

Every control that could be run passed cleanly, which licenses the OURS_WRONG calls made
elsewhere. GYFTR is the one exception: its `sf_revop.json` history has no quarter where standalone
revenue is stored correctly to test against — see its section below.

## Which companies carry the defect, and which don't

**Carry the AADHARHFC/HDBFS wrong-row defect (stored value = a revenue sub-line, not the total):**
- **GYFTR** (BSE scrip 507912, was "LKP Finance Limited" — an NBFC — until an Aug-2024
  change-of-control, later renamed to "Gyftr Ltd"): stores `Interest income` instead of `Total
  Revenue from Operations` at every quarter checked (2023-06, 2023-09, 2023-12, and even an
  un-flagged 2024-06 consolidated cell). This is the same mechanism as AADHARHFC/HDBFS, on
  another small NBFC.
- **SPICEJET**: stores `Revenue from operations` (ticket/passenger revenue) alone, excluding
  `Other operating revenues` (ancillary revenue), at all 3 of the brief's flagged quarters
  (2024-06, 2025-03, 2025-06). A control quarter (2022-09) is clean, so the defect is
  quarter-range-specific, not permanent.
- **PIRAMALFIN**, but only at 2025-09-30 (a bonus read, not one of the brief's required
  worst-2): stores `Interest income` instead of `Total Revenue from operations`. The two
  officially-flagged quarters (2024-12, 2025-03) are a **different, unresolved** problem — see
  below.

**Do NOT carry a row defect — our data is correct, the flagged gap has another explanation:**
- **TVSHLTD**: our stored value is exactly `Revenue from operations`; Screener's number is
  `Total Income` (operations + Other Income), which is unusually large this company/quarter
  because TVS Holdings is a holding company with a big one-off profit-on-sale-of-shares and
  reclassified investment income. AMBIGUOUS_CONCEPT (matches the brief's own BSE Ltd precedent).
- **JPASSOCIAT**: our stored value is exactly `Revenue from Operations` (this company's
  statement has no revenue sub-components — it's already the "total"), at both flagged quarters
  and the control, on both standalone AND consolidated bases, all PAT-anchored exactly.
  Screener's own number doesn't reconcile to anything in the as-filed statement (std/con x
  rev-ops/total-income) — a Screener-side scraping problem on this CIRP-era company's unusual
  layout, not a defect in our data.
- **THEMISMED**: our standalone slot is exactly the standalone `Revenue from Operations`
  (continuing operations); our own consolidated slot independently matches the filed
  consolidated figure too. Screener's number — despite being captioned "Standalone" in the
  sweep extract — is actually the CONSOLIDATED figure. Same basis-mismatch mechanism already
  documented for AADHARHFC's 2024-06-30 cell.

**Unresolved (genuine open question, not a guess):**
- **PIRAMALFIN 2024-12-31 and 2025-03-31**: the company was still pre-equity-listing at these
  dates (its equity only started trading 2025-11-04; history reached via debt-segment NCD
  scrips) and was mid-way through a Composite Scheme of Arrangement with parent Piramal
  Enterprises that changed how "Other operating income" vs "Exceptional items" got classified
  between the as-originally-filed and a later-restated version of the SAME Dec-24 quarter — and
  neither version, nor Screener's own number, nor the consolidated statement, reconciles to our
  stored value. Worse, the filed PAT for Dec-24 is a **loss** (-112.30cr, both versions, both
  bases) while our stored PAT and Screener's both show a **profit** (+69.0cr) — a sign flip that
  breaks this packet's PAT-anchor method outright for this one cell. Recommend a follow-up
  packet with access to the IPO/listing prospectus, which may hold a third, not-yet-located
  version of these financials.

## A note on the materiality caveat

None of the OURS_WRONG calls here are display-rounding artefacts — every one is backed by an
exact-to-the-cent component match (the "missing" amount sums precisely to the named row) and an
exact PAT anchor, well beyond BSE's whole-crore rounding tolerance. The AMBIGUOUS_CONCEPT and
site-side-discrepancy findings (TVSHLTD, JPASSOCIAT, THEMISMED) are likewise exact-to-the-cent on
our side; the "gap" lives entirely in what the comparison site is showing, not in any ambiguity
about our own number.

## Files
- `batch_d_verdicts.json` — structured verdicts, 21 cells (6 companies x worst-2 + control, plus
  a few bonus reads that materially changed or reinforced a verdict), per the requested schema.
- `notes/*.md` — one working file per company with the full filing transcriptions, unit
  conversions, and reasoning behind each call.
- `cache/` (under `tools/`) — every fetched BSE/NSE JSON response, keyed by URL.
- `render/<SYMBOL>/` — every fetched PDF plus its per-page PNG renders (used wherever the text
  layer looked suspect, per runbook rung 10 — JPASSOCIAT's Mar-24 filing needed this: its
  `get_text()` output has columns and labels interleaved out of order).
- `tools/` — `exchange_fetch.py` (BSE detres + NSE XBRL ladder, copied from the reference tree,
  cache redirected to this working dir only), `bse_fetch.py` / `bse_render.py` (BSE announcement
  session + PDF fetch, same origin), plus `ann_range.py` / `ann_paginated.py` / `list_ann.py`
  (this session's own paginated-announcement-listing helpers, written because
  `bse_render.announcements()` only ever fetches page 1 of BSE's 50-row-per-page API, which
  silently missed older quarters for companies with heavy filing volume, e.g. PIRAMALFIN 130
  rows / 24 months) and `fetch_and_render.py` (fetch-a-PDF-by-attachment-id + render every page,
  this session's own wrapper).
