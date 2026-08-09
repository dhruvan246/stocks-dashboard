# GICRE — quarters no external site reaches, + two con-copy revenue cells

All PDFs fetched fresh from bseindia.com this session (>=2.2s spacing, no 403/429). Every PAT
figure below was read from a **rendered page image**, not the raw PDF text layer — see the
2022-03-31 note for why that matters. Full evidence: `gicre_quarters_verdicts.json`.
Nothing written to the repo; verdict-only per task scope.

## Group A — std!=con, 2022 quarters, never audited before

| quarter | stored std | filed std | con pre-assoc | assoc | con−std | fingerprint | verdict |
|---|---|---|---|---|---|---|---|
| 2022-03-31 | 1795.40 | **1795.40** | 1909.94 | 26.08 | 140.62 | NO (140.62≠26.08) | **OURS_CONFIRMED** |
| 2022-06-30 | 689.72 | **689.72** | 708.85 | 20.49 | 39.62 | NO (39.62≠20.49) | **OURS_CONFIRMED** |
| 2022-09-30 | 1859.93 | **1859.93** | 2062.03 | 154.52 | 356.62 | NO (356.62≠154.52) | **OURS_CONFIRMED** |
| 2022-12-31 | 1198.99 | **1198.99** | 1197.01 | 35.24 | 33.26 | close, not exact | **OURS_CONFIRMED** |

**None of the four Group A quarters carry the defect.** Every one was read directly off its own
BSE announcement filing's standalone page, each triple-anchored (two comparator columns matching
already-stored neighbouring quarters, plus a cumulative FY22/H1-FY23/9M-FY23 reconciliation that
closes to the paisa in every case). The con−std gap in 2022 is real but is NOT explained by the
associates line alone — GIC Re's subsidiaries evidently contributed a variable amount to the
con/std gap in this era, unlike the cleaner 2024-25 cases.

**Reading trap caught this session:** the Mar-2022 standalone PDF's raw text layer misreads the
target cell as `1,78,540` (an 8-for-9 OCR-class corruption) — reading that literally would have
manufactured a false ₹10cr "defect". Rendering the page as an image and reading the digits
visually shows the true printed figure is `1,79,540` = 1795.40cr, confirmed independently by the
FY22 cumulative-sum identity. Every cell in this campaign was therefore read from images, never
from extracted text, after this was caught.

## Group B — one upgraded from "small gap" to "confirmed defect"

| quarter | stored std | filed std | con pre-assoc | assoc | fingerprint | verdict |
|---|---|---|---|---|---|---|
| 2024-12-31 | 1623.43 | **1621.35** | **1623.43** | 53.18 | YES (53.19≈53.18) | **OURS_WRONG** |

Direct read of the standalone page (own filing, BSE 2025-02-03) shows row 27 "Profit / (loss)
after tax" = ₹1,62,135 lakh = **1621.35cr** — not 1623.43. The SAME filing's consolidated page
shows pre-associate "Profit/(loss) after tax" = ₹1,62,343 lakh = **1623.43cr**, an exact match to
our mis-stored "standalone" value; + associates (53.18) = "Profit for the year" 1676.62cr, which
is exactly our stored (correct) consolidated PAT. This is the **third confirmed instance** of the
identical defect already proven for Jun-2025 and Sep-2025. The 9-month cumulative column (own
page) only closes with 1621.35, not 1623.43 — a clean second-check arbitration. True value =
1621.35, matching Screener's 1621 (whole-crore rounding) precisely.

## Group C — characterisation of the pre-2022 con=std-copy era

| quarter | stored std | filed std | source | verdict |
|---|---|---|---|---|
| 2021-03-31 | 1260.44 | **1260.44** | own filing (BSE, filed 2021-06-29) | OURS_CONFIRMED |
| 2020-03-31 | 1197.41 | **1197.41** | year-ago comparator column, same filing | OURS_CONFIRMED |
| 2018-03-31 | 751.60 | — | **UNRESOLVED** — every route tried 404'd or was blocked | UNRESOLVED |

**Answer to the one question asked:** in this era, **the stored value IS the genuine standalone
PAT** (read straight off the standalone statement's own row); **the consolidated slot is the
copy**. Both spot-checks close a cumulative FY total to the paisa (FY21: 1920.44; FY20: -359.09,
built from all four quarters of each year) and Screener's own annual series corroborates both FY
totals independently. This is also fully consistent with — and independently re-derives —
`scripts/pat_defects.json`'s pre-existing, separately-sourced documentation, which individually
verified all six quarters from 2019-09-30 through 2021-06-30 against primary filings.

**2018-03-31 could not be resolved this session.** BSE announcement attachments for the Mar-2018
result 404'd on both AttachHis and AttachLive (two different attachment IDs tried); the
runbook-documented gicre.in IRDAI-disclosure route (§43) 404'd on every guessed live-site URL and
has zero Wayback Machine coverage; NSE's corporate-results API returned 403 (known lockdown). The
tooling that solved this quarter's REVENUE in an earlier campaign (`_irdai_gicre.py`) lives in a
worktree not available here. This does not weaken the characterisation above — two solid,
independently-corroborated spot-checks plus the pre-existing six-quarter campaign already answer
the question asked.

## Additional scope — consolidated-revenue con-copy check (2 quarters)

GICRE's IRDAI-format P&L prints **no single "Revenue from Operations" row**. The figure used here
is a column-anchored construction — Premium Earned (Net) + policyholders' Income from Investments
+ shareholders' Income from Investments, excluding both sides' "Other income" lines — validated by
reproducing our own ALREADY-STORED, non-disputed revenue for FOUR different comparator quarters
(Jun-2023 std, Sep-2022 std, Jun-2023 con, Mar-2024 std) to the paisa. That ambiguity (no printed
row, a constructed sum) is itself part of the finding, per the brief.

| quarter | stored revS | stored revC | filed std | filed con | Screener con | verdict |
|---|---|---|---|---|---|---|
| 2023-09-30 | 13224.18 | 13224.18 | **13059.08** | **13075.11** | 13075 | **OURS_WRONG** |
| 2024-06-30 | 12822.55 | 12822.55 | **12822.55** | **12886.47** | 12886 | **OURS_WRONG** |

**The con-copy is real for both quarters.** The primary filing proves standalone and consolidated
revenue genuinely differ (Sep-2023: ratio 1.0012; Jun-2024: ratio 1.005 — both inside GIC Re's
normal con/std family), and Screener's independently-reported consolidated figure matches this
session's filing-anchored consolidated read almost to the rupee in both cases (13075.11 vs 13075;
12886.47 vs 12886) — an external source agreeing with the primary document.

- **2024-06-30 is the clean case:** our stored standalone figure (12822.55) is directly confirmed
  correct against the filing; the true consolidated figure is 12886.47, not a copy of standalone.
  Nothing else on that column looks wrong (PAT, tax, and the associates reconciliation all check
  out on the same page).
- **2023-09-30 is messier:** the filing-anchored standalone read (13059.08) does **not** match our
  currently-stored standalone figure (13224.18) either. So this is not simply "a correct
  standalone value got duplicated into consolidated" — both of our stored slots for this quarter
  appear to reflect a different, unreconciled revenue concept (possibly an NSE per-basis XBRL tag,
  which could not be checked — NSE returned 403 this session). That secondary puzzle is flagged,
  not solved; it doesn't change the primary verdict, which rests on the con!=std proof and the
  Screener cross-check of the consolidated figure specifically.

## Route ladder log

| cell | rung 3 BSE announcement PDF |
|---|---|
| GICRE Mar/Jun/Sep/Dec-2022 std | SUCCESS (4/4) |
| GICRE Dec-2024 std | SUCCESS |
| GICRE Mar-2021 / Mar-2020 std | SUCCESS (1 filing covers both) |
| GICRE Mar-2018 std | **FAILED** — 404 on 2 attachment IDs; gicre.in and Wayback also failed |
| GICRE Sep-2023 / Jun-2024 revenue | SUCCESS (2/2, both bases) |

No 403/429 was hit against BSE at any point. The only blocked route was NSE (403 on session
warm-up, consistent with the known site-wide NSE lockdown) — not pursued further since BSE
supplied everything needed except the one Mar-2018 cell.
