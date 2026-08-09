# REVPAT verify — p5 arbitration: 4 contested cells

Tree pin `e8a491c6`. Method: DATA_RUNBOOK §57 route ladder + §58 column-anchor read, walked fresh
this session — every PDF below was fetched LIVE from bseindia.com during this run (not reused from
any cached campaign artefact), and rungs 1–2 were independently re-run via `exchange_fetch.py`
rather than taken on the task brief's word. No 403/429 encountered. Full evidence:
`arbitration_verdicts.json` in this directory.

## Verdicts

| # | symbol | quarter | field | OURS | Screener | Groww | FILED VALUE | verdict |
|---|---|---|---|---|---|---|---|---|
| 1 | GICRE | 2025-06-30 | std PAT | 2172.77 | 1752.00 | 1752.23 | **1752.23** | **OURS_WRONG** |
| 2 | GICRE | 2025-09-30 | std PAT | 2698.01 | 2867.00 | 2866.79 | **2866.79** | **OURS_WRONG** |
| 3 | BAJFINANCE | 2025-12-31 | std revenue | 18067.89 | 17870.00 | 17869.70 | **18067.89** | **OURS_CONFIRMED** |
| 4 | BAJFINANCE | 2025-12-31 | con revenue | 21213.89 | 21013.00 | 21013.49 | **21213.89** | **OURS_CONFIRMED** |

All four are resolved. None are UNRESOLVED, BOTH_WRONG, or DEF_DIFF.

---

## #1 & #2 — GICRE standalone PAT: OUR STORE IS WRONG on both quarters

**Root cause (proven, not inferred):** our `sf_fundamentals` standalone-PAT slot for GICRE is
holding the **consolidated statement's pre-associate "Profit/(loss) after tax" row**, not the
standalone statement's own PAT row. The two statements share the exact label
("Profit / (loss) after tax") at the same relative row position, one page apart in the same PDF —
that is almost certainly how the wrong row got copied into the wrong slot originally.

**The smoking gun, read directly off the freshly-fetched filings:**

Jun-2025 filing (`.../AttachHis/2995f5a3-9a07-4636-b4ad-b26afead8823.pdf`, filed 2025-08-07):
- STANDALONE page, row 27 "Profit / (loss) after tax", col (30/06/2025): **₹1,75,223 lakh = 1752.23cr**
- CONSOLIDATED page, row 27 "Profit/(loss) after tax" (pre-associates), col (30/06/2025): **₹2,17,277 lakh = 2172.77cr** ← this is our stored "standalone" value, exactly
- Consolidated page continues: + Share of Profit in Associates (35,782) = "Profit for the year" **₹2,53,059 lakh = 2530.59cr** ← this IS our stored (correct) consolidated PAT, exactly

Sep-2025 filing (`.../AttachHis/4048b2b3-ec10-4538-8a41-878d65befe45.pdf`, filed 2025-11-12), same shape:
- STANDALONE, row 27, col (30/09/2025): **₹2,86,679 lakh = 2866.79cr**
- CONSOLIDATED, row 27 pre-associate, col (30/09/2025): **₹2,69,801 lakh = 2698.01cr** ← our stored "standalone" value, exactly
- + associates (17,553) = "Profit for the year" **₹2,87,354 lakh = 2873.54cr** ← our stored (correct) consolidated PAT, exactly

**Column anchors, both directions:**
- The Jun-2025 filing's own standalone row reproduces our stored Mar-2025 (2182.89) and Jun-2024
  (1036.36) std PAT exactly in its comparator columns.
- The Sep-2025 filing's standalone comparator column for (30/06/2025) reproduces **1752.23** —
  the very number this arbitration derives independently for cell 1 — filed three months apart, in
  a different document. Two independent filings agreeing to the paisa.

**Strongest single check — the H1 identity the task asked for:** the Sep-2025 filing prints a
YTD-to-30/09/2025 column for the same row = ₹4,61,902 lakh. Q1 (1,75,223) + Q2 (2,86,679) =
**4,61,902 lakh, exact.** Both quarters' true standalone PAT are pinned by one filing's own arithmetic.

**Verdict for both cells: OURS_WRONG.** The correct standalone PAT is 1752.23 (Jun-2025) and
2866.79 (Sep-2025) — matching Groww exactly and Screener to the nearest whole crore in both cases.
This finding extends and independently re-confirms DATA_RUNBOOK §55c, which had already spotted the
same defect shape at Dec-2024 and asserted (without showing the read) that Jun-2025 and Sep-2025
were "the same shape" — this session supplies the missing direct reads for both.

**Not healed.** Per task scope this is a verdict only; no repo file was touched.

---

## #3 & #4 — BAJFINANCE revenue: OUR STORE IS RIGHT, both sites are wrong (but agree with each other)

Filing: `.../AttachHis/f62936ec-8dee-4bc5-89b9-094c5e576451.pdf` — "Statement of unaudited
Standalone/Consolidated financial results for the Quarter and Nine months ended 31 December 2025",
filed 2026-02-03. Clean (non-scanned) text layer, no OCR needed.

**Standalone**, row "Total revenue from operations", col (31.12.2025): **₹18,067.89cr** — exactly
our stored value. Column anchors: (30.09.2025)=17,184.42 and (31.12.2024)=15,371.02, both exact
matches to our stored comparators.

**Consolidated**, same row, col (31.12.2025): **₹21,213.89cr** — exactly our stored value. Column
anchors: (30.09.2025)=20,178.90 exact; (31.12.2024)=18,035.13 vs our stored 18035.11 (₹0.02cr,
immaterial).

**Strongest single check — the 9-month identity:** the filing prints a 9M-ended-31.12.2025 column
for both bases. Standalone: our stored Q1 FY26 (16,696.54) + Q2 FY26 (17,184.42) + this quarter's
filed value (18,067.89) = **51,948.85**, matching the filing's own printed 9M column exactly.
Consolidated: 19,523.88 + 20,178.90 + 21,213.89 = **60,916.67**, again an exact match. Three
quarters' worth of our stored data plus this quarter's filed read reconcile to the audit trail's own
arithmetic on both bases — about as strong a confirmation as this method produces.
Total income (revenue + other income) and the quarter's own PAT row also reproduce our stored
figures exactly on the same page, so the whole column is anchored, not just the revenue cell.

**Verdict for both cells: OURS_CONFIRMED.** Screener and Groww agree with each other
(std ~17,870 / con ~21,013) but both disagree with the primary filing by roughly the same
proportion (~1.1%) on both bases — the SADBHAV §70c warning in reverse: two sites agreeing is not
evidence they are right. The gap looks like a systematic vendor revenue definition (a candidate
combination — excluding "Income on derecognised (assigned) loans" + "Net gain on fair value
changes" — gets close on the standalone side, ₹201.30cr vs an observed ₹198.19cr gap, but doesn't
reproduce cleanly on the consolidated side, so the exact vendor convention was not pinned down).
That question does not change the verdict: the primary filing's own labelled row, doubly anchored
and identity-confirmed, is unambiguous.

---

## Route ladder log (all four cells)

| cell | rung 1 BSE detres | rung 2 NSE XBRL | rung 3 BSE announcement PDF |
|---|---|---|---|
| GICRE Jun-2025 std | refused: no-date-begin-end-in-response | refused: 0 list rows | **SUCCESS** |
| GICRE Sep-2025 std | refused: no-date-begin-end-in-response | refused: 0 list rows | **SUCCESS** |
| BAJFINANCE Dec-2025 std | refused: no-date-begin-end-in-response | refused: 0 list rows | **SUCCESS** |
| BAJFINANCE Dec-2025 con | refused: route does not serve consolidated | refused: 0 list rows | **SUCCESS** |

Rungs 1–2 were re-run independently this session via `exchange_fetch.py` (not taken on the task
brief's word). No 403/429 was hit at any point against BSE. Rung 3 (BSE announcement PDF, live
fetch) resolved every cell — none needed rung 9 (IRDAI) or rung 10 (vision/OCR): GICRE's Jun-2025
pack had a corrupted/jumbled text layer on the standalone page (§51b class) but the target row was
still locatable by column-anchor + PBT−tax=PAT arithmetic without OCR; the Sep-2025 GICRE pack and
both BAJFINANCE pages had clean, linear text layers.

## What was NOT done
- No repo file, ledger, or heal was written — this is a verdict-only deliverable per task scope.
- The exact third-party (Screener/Groww) revenue definition for BAJFINANCE was not pinned down
  precisely — noted as an open curiosity in `arbitration_verdicts.json`, not pursued further since
  it doesn't affect the verdict.
