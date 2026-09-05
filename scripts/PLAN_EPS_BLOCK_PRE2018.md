# PLAN — the per-quarter LINE-ITEM block (EPS, other income, interest, depreciation, tax, exceptional, PBT, employee cost, materials) before 2018

**NO ASSUMPTIONS, NO GUESSWORK (runbook §0).** Every number here was measured on 2026-09-05 in worktree
`~/stocks-wt/eps-block` (own worktree, never the shared checkout). Procedure lives in DATA_RUNBOOK §130.

## What the block is
`scripts/xbrl_extra.json.gz` → `docs/fin/<sym>.json` key `x` = `{qe: {s|c: {oi, fc, dep, tax, exc, pbt, emp, mat,
eps_b, eps_d, seg, aud, …}}}` (₹ crore, EPS ₹/share). Built by `build_xbrl_extra.py` from the NSE XBRL cache
(`~/stocks-dashboard/scripts/_xbrl_cache`, 104,538 files, filenames 2018-05 → 2026-08), topped up nightly by the
cloud routine (§50). Consumed by `docs/stock.html` (Quarterly detail: last 12 quarters; Annual tab: EPS summed per
complete FY, 8 FYs).

## Baseline (measured, PIT Nifty-500 member-quarters with a stored PAT, rename-folded via `_n500_member_bin`)
Two readings of the same thing, both kept for the before/after:
* **any-basis** (a quarter counts as covered when EITHER basis has `eps_d`): 0% every year 2002-2017; 2018 78% ·
  2019 79% · 2020 90% · 2021 89% · 2022 88% · 2023 94% · 2024 97% · 2025 95% · 2026 97%. Holes: **29,390 pre-2018,
  1,827 in 2018+**.
* **per-basis** (a (quarter, basis) cell with a stored PAT counts only if THAT basis has `eps_d`): 2018 65% · 2019 75% ·
  2020 90% · 2021 88% · 2022 85% · 2023 93% · 2024 96% · 2025 93% · 2026 94%. Holes: 36,759 / 4,413.
(The user's 2018 70% / 2019 73% / 2020 83% sit between the two readings.)
Script: scratchpad `measure_eps_cov.py <ledger> <holes.json>`.

## Job 1 — REACH, measured before writing anything
1. **The results list carries a real XBRL URL only from the Mar-2018 quarter** (cached lists, 109,762 rows: 2018
   4,784/5,712 · 2017 374/5,831 · 2016 2/7,275 · earlier 0). Every 2005-2017 row carries `resultDetailedDataLink`
   → the archived HTML page with the full P&L incl. EPS rows, paid-up capital and face value. "NSE XBRL only" therefore
   meant "2018 only"; the pre-2018 block is the ARCHIVE HTML route (memory feedback-nse-archive-first), the same pages
   §123 read for consolidated PAT.
2. Over the 29,390 pre-2018 holes: **24,529 have an archive page** (per year 2005 86% … 2008-2017 94-99%),
   4,247 of them already cached from earlier campaigns (free), ~20,300 to fetch at ~0.13 s each. 2002-2004
   (3,126 cells) predate the archive; 554 are index holes inside a symbol's list range (ACC has no Dec-2007 row at all);
   1,120 cells belong to 112 symbols whose list was never cached (INDIANCARD, CEAT, …) — fetched by the extractor.
3. **Post-2018 holes (1,827) were three parser classes, not missing data:** 1,500 quarters absent for symbols in the
   ledger — banks 2018-2022 (SBIN/HDFCBANK/… 20+ each) and insurers; 267 for symbols absent — the 13 HTML-escaped
   `M&AMP;M`-class keys (§115) plus BAYERCROP/ABBOTINDIA/MCX/KENNAMET (NSE's list holds 0-1 rows for them, measured
   live); 60 insurer cells present without EPS (the IRDAI XBRL has no per-share tag — grep'd two LI files: none).
4. **Bank taxonomy 2018-2022 has NO `<xbrli:context>` block and no `DateOfStartOfReportingPeriod`**; it declares
   `ReportingQuarter` + `DateOfEndOfReportingPeriod` + `DateOfStartOfFinancialYear`, and OneD is the QUARTER while
   FourD is the YTD of the SAME basis (HDFCBANK Dec-2020 OneD PAT 8,758 cr / FourD 22,930 cr). `ReportingQuarter` names
   the filing ("Half yearly", "Yearly") not the slot — KTKBANK Mar-2019 OneD 61.73 = stored std, FourD 477.24 = FY.
5. **Moneycontrol's quarterly feed serves the line items** (live payloads RELIANCE std/con, SBIN): Other Income ·
   Interest · Interest Expended (bank) · depreciat/Depreciation (spelling differs by table) · Tax · P/L Before Tax ·
   Exceptional Items · Employees Cost · Consumption of Raw Materials · Basic/Diluted EPS (before) and "Basic EPS." /
   "Diluted EPS." WITH A TRAILING DOT (after extraordinary). Standalone reach to Jun-1997, consolidated ~2013.
   RELIANCE Dec-2010 std from the archive page == Moneycontrol to the rupee (OI 741 · Interest 549 · Dep 3,359 ·
   Tax 1,242 · PBT 6,378 · EPS 15.70).

## Job 2 — the three writers, one ledger, one precedence
* `build_xbrl_extra.py` (XBRL, highest): `&amp;` unescaped + `&AMP;` keys migrated; bank-taxonomy quarter derived
  from ReportingQuarter/period-end/FY-start with the end-date consistency check; `MIN_QE` 2018→2016 (taxonomy follows
  submission); bank tag spellings (`ProfitLossFromOrdinaryActivitiesBeforeTax`, `EmployeesCost`, `ExceptionalItems`);
  a full rebuild seeds itself from every `src` cell AND unions the committed .gz (2,593 cells of Jul-Aug 2026 filings
  exist only there — the local cache never received them; a local rebuild alone dropped 2026 from 97% to 81%).
* `xtra_nse_html.py` (archive pages, `src: nse-html:<file>`): identity (symbol/era name, Period Ended, Non-Cumulative,
  declared basis) → PAT anchor vs the stored PAT of that basis (max 2 cr / 3%) → fields by template (three P&L
  templates + bank; the 2012-14 template's EPS is a header + "(a) Basic"/"(b) Diluted" sub-rows) → GATE E EPS
  recon (eps × paid-up / face value == PAT within 6%, refuses EPS only). Ind-AS pages: Other income derived as
  Total Income − Total income from operations; "continued and discontinued" EPS 0.00 next to a non-zero
  continuing-ops row is a placeholder.
* `xtra_mc.py` (Moneycontrol, lowest, `src: mc:<sc_id>:<table>`): gate T (feed PAT == stored PAT at the target,
  max 0.06 cr / 0.5%, AND `agg_gate.check_series` around it at that tolerance) · C (con-copy, §85) · R (the feed
  row must reproduce ≥2 of OUR ledger values for that field with no disagreements; a row with no overlap is
  `row-unproven`) · Z (0.00 held except exceptional items). Into an XBRL/nse-html cell it may only ADD missing
  fields, listed under `src_mc`.
Precedence per basis-cell: XBRL (no `src`) > nse-html > mc. Journals: `_xtra_html_reads*.json`, `_xtra_html_skips*.json`,
`_xtra_mc_reads.json`, `_xtra_mc_skips.json` (local). Served slices keep no provenance (XTRA_KEEP unchanged).

## Job 3 — run order
1. full XBRL rebuild (`--fresh`, 104,538 files, ~60 s on 8 workers) → union committed → 2018+ holes 1,827 → 1,214
2. archive shards (`--shard i/2`, journal only) → `--apply`
3. `xtra_mc.py --years 2002-2017` + `--post2018` → `--apply`
4. measure both readings → `build_stock_fin.py --out <scratch>` diff → page check → commit gz + scripts + runbook
   → push (§38 recipe) → CI `refresh-stock-fin.yml` rebuilds `docs/fin` → LIVE check ~20 min later.

## Left open after this pass (see the runbook section for the counts)
insurers' EPS (no XBRL tag; Moneycontrol rows unproven against our store), BAYERCROP (stored Jun-2019 PAT 59.3 vs feed
135.3 — one of the two is wrong, filing read needed), 102 post-2018 XBRL URLs that 404 on nsearchives, the archive
EPS recon refusals (~8%: filer share-count quirks), the 2002-2004 cells refused by the PAT series gate.
