# Nifty/CNX-500 membership history (2006 → date)

Complete-as-archivally-possible reconstruction of who was in the Nifty 500 (formerly CNX 500), built
2026-06-23. Two layers:

## Artifacts
- **`_n500_master_history.json`** — `{date: [symbols]}` — **19 full constituent lists** at archived dates
  (2006-11-08, 2010-01-02, 2014-01-22, 2014-07-09, 2015-03-25, 2018-10-04, 2019-02-01, 2020-07-25,
  2022-05-04, 2022-10-09, 2023-04-04, 2024-02-07, 2024-02-26, 2025-06-16, 2025-08-15, 2026-01-07,
  2026-05-02, 2026-05-30, 2026-06-12). Authoritative symbols, read straight from NSE/niftyindices
  `ind_cnx500list.csv` / `ind_nifty500list.csv` via Wayback + live.
- **`_n500_changes.json`** — `{prs_file: {eff, included:[syms], excluded:[syms], ...}}` — **35 dated
  change-events** (semi-annual reviews + off-cycle replacements) parsed from niftyindices press-release
  PDFs. The PDFs tabulate Company Name **+ Symbol**, so symbols are read directly (no name-mapping).

## Coverage / validation
Anchor-to-anchor reconciliation (start at full list A, apply all change-events to full list B, compare):
**2020→2026 reconciles tightly** (residual 1–5 per window = symbol renames + rare unannounced swaps).
Every semi-annual review 2020-03 … 2026-03 is present and magnitude-correct (~17–34 stock turnover each).
**Pre-2020 is sparse** (press releases not archived 2010–2017) — full-list anchors still pin membership at
their dates, but between-anchor churn there is not reconstructable. **2008-2009: no clean capture exists**
(the only 2008 Wayback copy of the .htm is gzip-corrupted) — 2006-11-08 and 2010-01-02 bracket it.

## Ever-member unions (snapshots-in-window ∪ all change in/excludes) — richer than snapshots alone
since Jan 2024 = 653 · 2023 = 668 · 2022 = 743 · 2021 = 751 · 2020 = 808 · 2019 = 813 · 2015 = 937 · 2011 = 1044.
Written to `_full_union_<year>_v2.json`. The change-layer surfaces transient members the sparse snapshots
miss — e.g. 2021-union gains 13 names over the snapshot-only 740 (BURGERKING, HUHTAMAKI, SWARAJENG, …).

## Refresh / extend
1. `python -X utf8 _n500_parse_changes.py` — re-fetch/parse press releases (text cached in `_prs_txt/`).
2. `python -X utf8 _n500_build_history.py` — re-validate + rewrite unions.
New semi-annual reviews: niftyindices live `https://www.niftyindices.com/Press_Release/ind_prs<DDMMYYYY>.pdf`
(broad reviews announced ~late Feb & late Aug; many are NOT Wayback-archived — brute-force live by date).
