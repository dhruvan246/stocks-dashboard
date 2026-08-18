# PLAN: con-family close-out — the last ~5,430 live missing cells

Fable 2026-08-18, on the user's "make a plan for con params now". Successor to
PLAN_CON_COPY_RETRACTION.md (retraction DONE, shipped) and sibling of PLAN_STD_FILL.md
(executing). Standing rules: no assumptions; per-name evidence; NOTHING marked N/A without the
user's explicit sign-off; §39 ship gate; §38 concurrency.

## Ground truth (live payload, 2026-08-18 17:42 IST — re-measure before executing)

profitTTMCon 1,374 · compositeCon 1,374 · revCon 672 · postDriftCon 552 · patCon 526 ·
profitAccelCon 299 · profitYoy/Base/StreakCon 210 each ≈ **5,430 con cells** (of 5,957 total).

**Metric warning:** size lanes from the PAYLOAD (members − covered − na), never from
`n500_cov_explain.json` cell counts — the explain's byDate name-lists for the std/con engine
families appear to include N/A-excluded dates (a con-lane sum over it gives 13,741, which
exceeds all live missing). Use the explain for NAMES only, and intersect every name-list with
`coverage_na_ledger.json` before acting. Verify whether the ex-push in build_coverage_matrix.js
checks `ecna`/`esna`; if it doesn't, that is a small engine bug worth fixing first (explain
naming already-adjudicated symbols wastes reader time).

## Lanes, in execution order (highest certainty first)

### L5 — bounds that pre-date the retraction (biggest lever, ~cheapest)
Names already N/A'd with user approval whose `to:` bounds were computed BEFORE the retraction
emptied their con series — so new holes now sit OUTSIDE the bounds and count as missing again
(AAVAS, BASF 165, UCOBANK, HONAUT 192, PGHL 117, KARURVYSYA, BANDHANBNK, AUBANK class).
The evidence per name (never filed consolidated before floor X) already covers the new dates;
only the arithmetic moved. Procedure:
1. Fresh `--explain`, per-name per-param hole spans, for names ∩ coverage_na_ledger.
2. Regenerate each entry's from/to from the CURRENT holes, `supersedes` note stating the bounds
   were re-derived post-retraction, evidence text unchanged.
3. **Present the widening sheet to the user before writing** — approved names, but bounds are a
   new decision. One commit, one bake.
Honest size: unknown until re-measured (payload metric); likely the majority of revCon/patCon's
1,198 and a large slice of the TTM/composite 2,748.

### L1 — retracted, N/A never written: 5 names, 434 cells
PATANJALI 211 (filing read DONE and CONFIRMED — needs only the user's "NA" and bounds from a
fresh explain), GAEL 108, GUJALKALI 63, CROMPTON 42, IPCALAB 10. The last four are in the
held-11 (consolidated BSE headlines / single-reader) — they need the L2 reads first.

### L2 — the held/restored pool: filing-read wave, then adjudicate
The 9 March-quarter headline names (KEI, TVTODAY, NATIONALUM, HUDCO, GVPIL, BOSCHLTD,
JUBLFOOD, NESCO, GUJALKALI) + ABFRL 79 + CROMPTON 42 + the 8 restored cells + big pending
residuals (CREDITACC 107, HDFCAMC's restored 2). One agent wave, calibration-style brief,
per name: read the March filing (annual-consolidation class) → either a REAL con figure
(fill/correct with the filing's own announce date) or std-only proof (retract + N/A sheet
entry). Outcomes split three ways; nothing bulk. Anchors: the stored std for the same quarter.
Precedent: the annual-con filings genuinely carry con YEAR columns — a March quarter can
sometimes be DERIVED year-minus-9M only if the filer prints both; NEVER derive where a printed
figure exists (RIIL lesson), and record filer-side identity breaks.

### L4 — real con store gaps: the vision fill campaign, ~152 names
Queue-build exactly like PLAN_STD_FILL (r[3] is None with r[1] present, 2018+, minus
pending/retracted/na names, minus known refusal ledgers incl. no_con_quarterly_*.json and
coverage_na). Wave size ~12-15 quarters/agent, anchors from the store (std PAT, rev_con),
routes: XBRL cache index → BSE announcements (proven headers) → NSE → comparative columns →
render-and-read. Hit-rate calibration says ~90% resolve, of which some are honest
NO_CONSOLIDATED_FILED → those become N/A-sheet rows, not silence. HDFC-class already-adjudicated
refusals (task #8) must be excluded up front.

### L3 + L6 — sweep: ~266 cells
L3 (152): revCon-only holes with PAT present — mostly the rev twin of an already-filled PAT
quarter (the batch-F class); fill rev_con/op_con from the same filings via write_rev.py
convention (key SYM|qe|con_rev). L6 (114): RAILTEL/CAMPUS/MANYAVAR short-history residue —
CAMPUS 86 needs its 16 REAL gaps filled first (it is in the fill queue, not the N/A queue,
per the user's held-out decision), then the reach residue joins the short-history sheet.

## Wave mechanics (unchanged from what worked today)
Read-only agents report; ONE writer applies; every value anchored to a same-statement stored
figure; comparative values carry the CARRYING filing's date; twins both; provenance in
conpat_filing_fills (con / con_rev tokens) with pins in owners_basis_heals when
_reattr_owners covers the cell; `_revgap_skips` rekeyed `_FILLED_` on success; gate = rebuild,
shared-grid diff, zero regressions, MISSING 0, push, bake, pages, live `?cb=` at ~20 min.

## What done looks like
Every remaining con cell is one of: FILLED from a filing · N/A with per-name evidence and user
sign-off · or listed by name on a visible residue sheet with its blocking reason. No cell
"missing" that no one could ever fill, and no cell hidden that someone could.
