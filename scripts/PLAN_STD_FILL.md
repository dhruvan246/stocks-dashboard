# PLAN: std-family fill — 17 names, 51 quarter-cells → 196 coverage cells

Written by Fable 2026-08-18 for Opus to execute. User: "plan fillable ones and handover to opus."
Standing rules apply: no assumptions; anchor every read; nothing marked N/A without the user;
§39 ship gate; §38 concurrency (work in `~/stocks-wt/n500-cov`, file-scoped adds, never touch
another session's dirty files).

## What this is

The std family (revStd + patstd params) has 527 remaining coverage cells. 331 are structural
(lookback reach / short history — NOT this plan). The other **196 trace to 51 store quarters
that are genuinely missing** a standalone PAT or a standalone revenue, at 17 companies. Exact
queue with per-cell context: `scratchpad/std_fill_queue.json` (regenerate with /tmp/mkq.py logic
from the session if the scratchpad is gone — it enumerates `r[1] is None` in sf_fundamentals and
`v[0] is None` in sf_revop for these names).

**Calibration already proved the route**: 13/14 cells resolved by reading the actual filings
(3 FILLED, 6 COMPARATIVE_ONLY, 4 NO_FILING clean absences), and every `no-anchor-or-scanned`
skip re-examined tonight was a TOOLING failure, not missing data. 43 of these 51 carry exactly
that skip tag. Expect most to fill; expect a handful of honest refusals.

## The queue (all quarters, exact)

**std PAT missing (4 names, 17 quarters):**
- VALIANTORG 20191231, 20200331, 20201231 — the EASIEST in the queue: rev_std already exists for
  all three (144.51 / 133.77 / 159.99), so the standalone statements demonstrably exist; only the
  PAT line was never extracted. The con values for Dec-2019/Mar-2020 carry annCon 20210210/20210525
  — i.e. they were recovered from COMPARATIVE columns of the Feb/May-2021 filings, so the std PAT
  almost certainly sits in those same statements' standalone sections. Anchor on the stored rev_std
  and con PAT from the SAME page. (The mc_history VALIANTORG|20201231 entry is the provenance of
  the existing rev_std 159.99 — not a conflicting hold; nothing to settle.)
- MAZDOCK 20190630, 20190930, 20191231 — batch H filled the CON side of these same quarters from
  the Q2FY21 filing's comparative columns (`H_MAZDOCK_20200930_C.pdf`, con statement p8). The SAME
  PDF has a standalone statement — read its comparative columns; annStd = the carrying filing's
  date (20201113 era), never the quarter's own.
- 360ONE 20180630, 20180930 — TRAP: in 2018 this company was IIFLWAM (FUND_ALIAS maps
  IIFLWAM→360ONE) and **may not have been listed yet** (verify its listing date FIRST). If
  pre-listing, the figures exist only as comparatives in FY20-era filings or in the IPO
  prospectus — comparative route with the carrying filing's date, or a documented refusal.
- HEXT 20150331…20161231 (8 quarters) + 20230930 — Hexaware delisted 2020, relisted 2024.
  Do 20230930 FIRST (relisted era, likely an easy read). The 2015-16 eight are LOWEST priority
  in this whole plan (they feed almost nothing in the 2020+ window); do them last or park them.

**rev_std missing (13 names, 34 quarters):**
- WESTLIFE ×13: 20190630→20220930 (list in the queue file). BSE scrip **505533** (memory-verified).
  All 13 skipped as `no-anchor-or-scanned` — scans, so RENDER AND READ (fitz.Matrix(2.4), read the
  image; never trust OCR text on a scan). CHECK BASIS FIRST: Westlife's operating business sat in
  a subsidiary; if a quarter's filing prints con-only (no standalone revenue line), that is a
  documented refusal, not a fill — do not manufacture a std revenue.
- SPICEJET 20181231, 20190331 (scanned-skip) + 20201231, 20210930, 20211231
  (`no-result-filing-in-window` — SpiceJet files LATE; widen the announcement search window to
  ±6 months before concluding anything).
- SHRIRAMCIT 20200630, 20210331, 20210630, 20210930 — since merged into SHRIRAMFIN; filings live
  under Shriram City Union Finance, BSE scrip via `_bse_master_all.json` (key `SCRIP_CD`).
- IIFL 20190630, 20190930 · MANAPPURAM 20190630, 20190930 · INDIANB 20200930 ·
  ANGELONE 20220630 · CAMS 20200331 (TRAP: CAMS listed Oct-2020 — Mar-2020 is pre-listing; the
  figure exists only in later filings' comparatives or the RHP; carrying-date rule applies) ·
  IDFC 20191231 · INDOSTAR 20191231 · JSL 20220930 · ATUL 20221231 · GICRE 20241231.
  All single/double quarters — batch them by route, not by name.

## Routes, in order (all proven tonight)

1. **Local XBRL cache** `/Users/dhruvan/stocks-dashboard/scripts/_xbrl_cache/` (main checkout —
   the worktree's is empty). Check `scratchpad/xbrl_index.json` first (sym → qe → basis). A
   Standalone XBRL for the quarter is the cleanest fill; the file's own
   `NatureOfReportStandaloneConsolidated` is the basis authority (a GOCOLORS/NSLNISP-class
   mislabel was found tonight — never trust a ledger's basis token over the file).
2. **BSE announcements** — plain urllib with `User-Agent: Mozilla/5.0 …` + `Referer:
   https://www.bseindia.com/` (the repo's NSE helper returns an HTML shell for BSE — proven).
   `AnnSubCategoryGetData/w?…&strScrip=<code>&strType=C`, attachment via AttachHis then
   AttachLive; validate by size AND `%PDF-` magic; ~162 bytes = a 302, not a file. Scrip codes
   from `scripts/_bse_master_all.json` (key `SCRIP_CD`). NEVER `FinancialResult/w` — it ignores
   scripcode and serves BSE Ltd's own results (guarded in bse_fetch.py tonight).
3. **NSE per-quarter list** (watch silent truncation at both ends).
4. **Comparative columns** of the next 1-3 filings — value is real, announce date is the
   CARRYING filing's date (look-ahead rule §99; RBA/PATANJALI precedent).
5. Scans → **vision read** of the rendered page. Anchor before trusting any column.

**Anchor rule (non-negotiable):** every read must reproduce, from the SAME statement, a value we
already hold — the quarter's con PAT, its rev_con, or a neighbouring std quarter. No anchor → no
write; record UNANCHORED and move on. Units: lakhs/100, 2dp (millions/10).

## Write mechanics

- Twins BOTH, every time: `docs/sf_fundamentals.json` + `scripts/fundamentals.json` (std → r[1],
  annStd → r[2]); `docs/sf_revop.json` + `scripts/revop_fundamentals.json` (rev_std → slot 0).
  Minified dumps `separators=(',',':')`. Rows already exist for all 51 (verified — no create_row
  needed).
- Provenance in the ledgers `verify_fills_live.py` already walks:
  `std_pat_detres_fills.json` (fund/std) and `std_rev_detres_fills.json` (revop/revS slot 0).
  READ ONE EXISTING ENTRY FIRST and mimic its shape; key `SYM|QE`. Full evidence string with
  attachment id + page + anchor.
- When a cell fills, annotate its `_revgap_skips.json` entry (`_FILLED_` prefix rekey) so the
  skip ledger stops asserting a failure that no longer exists.
- `mc_*` holds: check each target for a `held` entry before writing (a HELD cell asserts
  absence); if a filing contradicts a hold, settle it in the same change.

## Agent structure (readers report; ONE writer applies)

Three parallel READ-ONLY agents, calibration-style brief (verdicts FILLED / COMPARATIVE_ONLY /
NO_FILING / UNREADABLE, anchors mandatory, no repo writes):
- Agent A: WESTLIFE ×13 (one company, one route, vision-heavy)
- Agent B: std-PAT four (VALIANTORG, MAZDOCK, 360ONE, HEXT-20230930; park HEXT 2015-16)
- Agent C: the 12 small rev names (21 quarters, batch by route)
Then Opus applies everything through one writer pass, re-gates, ships. Do NOT let agents write.

## Ship gate

Rebuild → diff on the SHARED date grid only → expect revStd/profitTTMStd/compositeStd/patstd to
drop and NOTHING to regress → `verify_fills_live` MISSING 0 → commit file-scoped → push recipe
(`git checkout -- docs/coverage/` before rebase if CI raced; payloads are CI's to rebuild —
never hand-merge minified JSON) → dispatch "Nightly coverage matrix" then pages → verify LIVE
with `?cb=` ~20 min later. Report exactly what filled, what refused, and why.

## Honest expectations

~35-40 of 51 should fill (calibration rate). WESTLIFE may partly refuse on basis grounds;
360ONE and CAMS may be pre-listing comparatives; SPICEJET's three "no filing in window" are
probably window-sizing, not absence. Every refusal needs the evidence recorded — "can't be
filled" has been wrong too many times tonight to say it without a document.
