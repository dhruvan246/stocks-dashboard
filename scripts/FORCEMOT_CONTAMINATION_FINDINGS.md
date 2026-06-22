# FORCEMOT (Force Motors) — BSE scrip 500033 contamination: findings & remediation
*Investigated & remediated 2026-06-22.*

## TL;DR
- BSE's **`FinancialResult/w?scripcode=500033`** endpoint (and the `/downloads1/BSEFinancialResult*.zip`
  bundles it lists) returns **BSE Limited's** financial results, **NOT Force Motors'** — a BSE backend
  collision. Auditor *S.R. Batliboi & Co. LLP*, CIN **L67120MH2005PLC155188**, ISIN INE118H01025, symbol BSE.
- BSE's **announcement** endpoint **`AnnSubCategoryGetData?strScrip=500033&strCat=Result`** returns
  **genuine Force Motors** (`SLONGNAME="Force Motors Ltd-$"`, CIN **L34102MH1958PLC011000**, Akurdi/Pune).
  ⇒ **The poisoning is endpoint-specific, NOT scrip-specific.** 500033 *is* Force Motors' real BSE code.
- **61 contaminated `scripts/_vpdf/FORCEMOT_*_bse__*.pdf` cache files were deleted** (BSE Limited). The 11
  genuine `FORCEMOT_*_bseann.pdf` (6) + `FORCEMOT_*_nse.pdf` (5) were kept.
- **The published data was NEVER contaminated.** `docs/sf_fundamentals.json` & `scripts/fundamentals.json`
  start at qe **20220331**; every std+con value for 20220331..20231231 was cross-anchored to genuine
  Force Motors filings (0 uncorroborated). FY20/FY21 cells were in `_wf_skips.json` and never merged.

## How identity was determined (don't be fooled by the exchange address)
Every Indian filing is *addressed to* "BSE Limited / P J Towers / Dalal Street" (the exchange) — so bare
"BSE Limited" is **NOT** evidence. Discriminate on the **audited entity**:
| | Force Motors | BSE Limited |
|---|---|---|
| CIN | L34102MH1958PLC011000 | L67120MH2005PLC155188 |
| Office | Akurdi, Pune / Mumbai-Pune Road | P J Towers, Dalal Street, Mumbai |
| Auditor (results era) | (not Batliboi) | **S.R. Batliboi & Co. LLP** |

Classification of the 61 `_bse__` files (text layer via PyMuPDF + RapidOCR on the scanned ones): every
file that could be read = **BSE Limited**; the unreadable rest are siblings inside the same
BSE-Limited `BSEFinancialResult*.zip`. Notably even in-range files were BSE Ltd, e.g.
`FORCEMOT_20220331_bse__Outcome.pdf` and `FORCEMOT_20230630_bse__Revisedoutcome09082023.pdf`
(both literally "BSE Limited … Symbol: BSE, ISIN: INE118H01025").

## JSON verification (proof the data is clean)
Force Motors reports in ₹ **lakhs**; a value of *X.XX cr* appears in a filing as the integer token
`round(X.XX*100)`. Each pre-cron cell was matched in a genuine filing's "Net Profit for the period" row
(current column **and** comparative columns of neighbouring filings):
- std 20220331 −39.98, 20220630 −12.54, 20220930 19.97, 20221231 −4.54, 20230331 149.16,
  20230630 71.97, 20230930 98.26, 20231231 94.74 — **all corroborated**.
- con (owners) 20220331 −42.79 … 20231231 85.40 — **all corroborated**.
- Decisive sanity check: 20220331/20220630/20221231 are **losses**, and **BSE Limited has never reported a
  quarterly loss** ⇒ unmistakably Force Motors.
- 2024+ quarters come from the live NSE-XBRL cron (FORCEMOT is an active NSE name).

Reproduce with the local (gitignored) tools: `python -X utf8 scripts/_fm_classify.py` (PDF identity),
`python -X utf8 scripts/_fm_nums.py` (JSON-vs-filing corroboration).

## ✅ Correct way to obtain Force Motors' real history (use THIS)
**BSE announcement API → attachment GUID** (proven path: `_wf_bseann_FM.py` lists, `_wf_bseanndl_FM.py` downloads):
1. List result filings:
   `https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w?pageno=1&strCat=Result&strPrevDate=<YYYYMMDD>&strScrip=500033&strSearch=P&strToDate=<YYYYMMDD>&strType=C&subcategory=-1`
   (warm a cookie via `https://www.bseindia.com/` first; rows carry `NEWS_DT`, `NEWSSUB`, `ATTACHMENTNAME`=GUID, `SLONGNAME`).
2. Download each PDF: `https://www.bseindia.com/xml-data/corpfiling/AttachLive/<guid>` (fallback `…/AttachHis/<guid>`).
3. **Verify entity on every PDF** (CIN L34102MH1958… / "Force Motors") before extracting.

Verified live 2026-06-22: the **2020-04-01 .. 2021-12-31** window returns **13 genuine Force Motors rows**,
including the missing quarters — Mar-2021 audited (GUID abf5e53e…), Dec-2020 (ad633713…), Sep-2020
(1bf9db40…), Jun-2020 (19113949…), etc. So qe **20200331..20211231 (8 quarters) are recoverable**.
Those cells currently sit in `scripts/_wf_skips.json`; clear them to allow a backfill.

### Do NOT use
- ❌ `FinancialResult/w?scripcode=500033` / `/downloads1/BSEFinancialResult*.zip` — BSE Limited.
- ❌ ad-hoc local scripts `_wf_bselist_FM.py`, `_wf_bsedl_FM.py`, `_wf_dl2_FM.py` (they used the poisoned path).
- ⚠️ NSE `corporate-announcements` / `corporates-financial-results` serve FORCEMOT only ~2024+ (all
  pre-2024 quarters logged "miss" in `_fetchnse_log.json`). Annual reports (FY21/FY22) are a cross-check source.

## Root cause & the general lesson
The daily cron is safe (NSE-XBRL for active names; never BSE). The general BSE backfill (`bse_vision.py`)
has an **identity guard** (PDF company == expect_token). The contamination came from one-off `_wf_*_FM.py`
scripts that **bypassed the guard** and pulled the FinancialResult zips directly. **Any BSE fetch must
verify the audited entity (CIN/auditor), never trust the scrip label.** `bse_scrips.json` still correctly
maps `FORCEMOT→500033` (right for the announcement API); do not change it.
