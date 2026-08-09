# REV/PAT verify — batch_a: AADHARHFC / BSE / MTNL / MMTC (2026-08-09)

**Scope:** arbitrate 12 revenue cells across 4 companies against their own primary filings. No repo
writes, no heals — this file + `batch_a_verdicts.json` is the deliverable.

## Headline finding

**Only 1 of 12 cells checked reproduces the AADHARHFC/HDBFS wrong-row defect class** (a component
row like "Interest income" stored where "Total revenue from operations" should be). The other 11 —
all of BSE, MTNL and MMTC — are **row-correct against their own primary filings**. The brief's cited
gaps vs an independent source are real, but they trace to **three different, non-defect mechanisms**,
one per company, each pinned to specific evidence below:

| Company | Mechanism | Proof |
|---|---|---|
| BSE Ltd | **Concept difference**: Screener.in's "Sales" = filed "Revenue from operations" **+ Investment income** (a separate line BSE Ltd's own statement does NOT count as operating revenue). Ours holds the correctly-labelled row alone. | Reproduces the brief's own **-23.2% worst** and **-18.0% median** almost to the decimal by construction (see BSE section) |
| MTNL | **Restated-vs-as-filed**: MTNL reclassifies large amounts between "Revenue from operations" and "Other Income" from one quarter's filing to the next, **without changing PAT**. Ours holds each quarter's ORIGINALLY-FILED figure; the independent source appears to track whichever filing most recently restated it. | Directly caught in the wild: the same PDF that reports Sep-2025's own revenue (174.48) also RESTATES Jun-2025 (51.66→145.12) and Sep-2024 (158.80→251.22) in its comparative columns — PAT identical before/after in every case |
| MMTC | Row confirmed correct on 3 primary filings spanning the whole named range, zero restatement detected between filings. **Source of the brief's specific gap not identified** — Screener's own MMTC numbers are rounded to the nearest whole crore, too coarse to produce the cited -31.5%/-39.0%, and Total Income is far too large a gap (~-99%) to be the explanation either. | Honestly flagged UNRESOLVED (source), not guessed |
| AADHARHFC | **The real defect.** Component row (`a) Interest income`) stored where `Total revenue from operations` belongs — identical mechanism to the already-healed AADHARHFC 2023-06-30/2023-12-31 and HDBFS cells. | Filed total 614.00 matches the brief's independent-source figure to the rupee |

## Verdict table — every cell read

| Company | Quarter | Ours | Ours = which filed row | Filed Total Rev-from-Ops | Control passed? | Verdict | Confidence |
|---|---|---|---|---|---|---|---|
| AADHARHFC | 2023-09-30 | 560.59 | `a) Interest income` (component) | **614.00** | n/a (this IS the target cell; AADHARHFC's own 2024-06-30/2025-09-30 controls already proven in the reference-tree precedent) | **OURS_WRONG** | HIGH |
| BSE | 2023-06-30 | 156.62 | `1 Revenue from operations` (correct) | 156.62 (= ours) | — | AMBIGUOUS_CONCEPT | HIGH |
| BSE | 2023-09-30 (**worst**, -23.2%) | 206.61 | `1 Revenue from operations` (correct) | 206.61 (= ours) | — | AMBIGUOUS_CONCEPT | HIGH |
| BSE | 2023-12-31 | 278.40 | `RevenueFromOperations` XBRL tag (correct) | 278.40 (= ours) | — | AMBIGUOUS_CONCEPT | MEDIUM-HIGH |
| BSE | 2024-03-31 | 379.35 | `RevenueFromOperations` XBRL tag (correct) | 379.35 (= ours) | — | AMBIGUOUS_CONCEPT | HIGH |
| BSE | 2024-06-30 (**control**, smallest gap) | 495.98 | `1 Revenue from operations` (correct) | 495.98 (= ours) | **PASS** — row exact, PAT exact; the small residual gap is the same Investment-income effect at its weakest weight this quarter | OURS_CONFIRMED | HIGH |
| MTNL | 2025-06-30 (**worst**, -64.4%) | 51.66 | `Revenue from operations` (correct, confirmed 2 ways on the same PDF) | 51.66 (= ours) | — | AMBIGUOUS_CONCEPT | HIGH |
| MTNL | 2025-03-31 (**2nd worst**, -40.9%) | 152.50 | `Revenue from operations` (correct, via comparative column) | 152.50 (= ours) | — | AMBIGUOUS_CONCEPT | MEDIUM-HIGH |
| MTNL | 2025-09-30 (**control**, ~0% gap) | 174.48 | `Revenue from operations` (correct) | 174.48 (= ours) | **PASS** — and this filing's own comparative columns are the direct proof of the restatement mechanism above | OURS_CONFIRMED | HIGH |
| MMTC | 2023-12-31 | 0.73 | `Revenue From Operations` (correct) | 0.73 (= ours) | — | OURS_CONFIRMED | MEDIUM-HIGH |
| MMTC | 2024-03-31 | 0.64 | `Revenue From Operations` (correct) | 0.64 (= ours) | — | OURS_CONFIRMED | MEDIUM-HIGH |
| MMTC | 2026-03-31 (range endpoint) | 0.61 | `Revenue From Operations` (correct) | 0.61 (= ours) | — | OURS_CONFIRMED | MEDIUM-HIGH |

**Which companies carry the defect:** **AADHARHFC only** (this one cell, plus the two already-healed
neighbours). **BSE, MTNL and MMTC carry no wrong-row defect** — every cell checked in all three is
the filing's own correctly-labelled revenue-from-operations row, verified against PAT anchors and
component sums to the rupee.

## Route ladder as walked (runbook §57/§58)

1. **BSE detres** (`Corp_detailedResult_Transpose_ng`, §42) — worked and exact-matched stored values
   for MTNL (scrip 500108) and MMTC (scrip 513377) on every quarter tried; **could not resolve a BSE
   Ltd scrip code** via the live active-equity master (~4,949-row and ~10,800-row pulls both miss any
   row named "BSE"/"Bombay Stock Exchange" — a genuine route gap, recorded honestly, not treated as
   "unfillable"). AADHARHFC: no filing pre-listing, as expected (§57a rule 1 — a route returning
   nothing here is not evidence of absence, it's the known pre-IPO ceiling already documented in the
   AADHARHFC precedent).
2. **NSE per-basis XBRL** (§54) — the working route for BSE Ltd (symbol `BSE`); all 5 quarters
   fetched, cross-checked against the nightly's own parser (`nightly_parser_agrees: true` throughout).
3. **NSE `corporate-announcements`** — used to locate and download the underlying filing PDFs for
   both BSE Ltd (whose own BSE-side scrip code was unreachable) and to corroborate MTNL/MMTC. Worked
   cleanly on the same warmed-up curl_cffi session that served route 2; no 403/429 encountered.
4. **BSE announcement stream** (`AnnSubCategoryGetData`, §58) — the workhorse for MTNL and MMTC filing
   PDFs, and for AADHARHFC via the BSE **debt-segment scrip fan-out** (§44), exactly as the reference
   tree's AADHARHFC precedent used it.
5. **Vision render** (§57 rung 10) — MTNL's 2025-06-30 and 2024-03-31/2026-03-31 MMTC filings are
   scanned/image-only PDFs (0 characters on the P&L pages via `get_text()`); rendered at 1.2-3x and
   read directly as images.
6. **screener.in** (§60) — used as a reconnaissance tool to identify what the brief's "independent
   source" was actually comparing (see mechanism table above), not as an authority for any written
   value. Precisely reproduces the brief's own -23.2%/-18.0% BSE statistics once Investment income is
   added to our revenue, which is strong evidence Screener.in (or a source with the same convention)
   is the origin of the BSE comparison. For MMTC, Screener's whole-crore rounding rules it out as the
   source of the cited percentages; no other candidate source was found this session — recorded as
   **UNRESOLVED (source)**, not guessed.

No 403/429 was hit against BSE or NSE this session. All requests throttled ≥2s apart (enforced by
`exchange_fetch.py`'s built-in throttle plus manual `sleep 2` between raw fetches).

## Files in this delivery

- `batch_a_verdicts.json` — structured verdicts, all 12 cells, per the requested schema.
- `BATCH_A.md` — this file.
- `cache/` — every BSE JSON/PDF response fetched this session, keyed by attachment GUID.
- `cache_pdf/` — the 3 BSE Ltd PDFs fetched via NSE's `corporate-announcements` route.
- `render/` — PNG renders used for the scanned-PDF reads (MTNL 2025-06-30, MMTC 2024-03-31/2026-03-31).
- `tools/exchange_fetch.py`, `tools/bse_ann.py`, `tools/screener_fetch.py` — copies of/derived from
  the reference tree's fetch tooling, cache paths redirected to this working dir only; no writes were
  made to the reference tree or the live repo checkout at any point.
- `exchange_leg_results.json` — raw first-pass BSE-detres/NSE-XBRL cross-checks for all 21
  (symbol, quarter) pairs originally pulled, kept for auditability.

## Open items (not adjudicated, flagged for a future packet)

- **BSE Ltd's own BSE scrip code was never resolved.** Every future BSE-side read for this company
  (e.g. for a consolidated-basis check, out of this packet's scope) will need it; the live
  active-equity master pull does not carry it under any name/scrip_id tried this session.
- **MMTC's gap source remains unidentified.** The row itself is solidly confirmed on 3 quarters
  spanning the full named range, but this packet could not name what the brief's -31.5%/-39.0%
  figures were computed against. A future session with access to whatever tool computed those exact
  numbers could close this in minutes.
- **MTNL 2025-03-31's specific restating filing was not pinned** (unlike 2025-06-30 and 2024-09-30,
  which were caught directly in the Sep-2025 filing's own comparative columns). The mechanism is
  proven in general; this one cell's restatement source is inferred by pattern, not directly located.
- **MTNL's consolidated slot** (`revC`) was not examined this session (brief scoped to revS-equivalent
  cells); given the con/std gap in MTNL's stored data is itself large (e.g. 2025-06-30: 51.66 std vs
  65.74 con), it may be worth a dedicated look given the restatement behaviour found in std.
