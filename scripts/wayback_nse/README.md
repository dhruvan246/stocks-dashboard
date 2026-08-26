# Wayback NSE `results.jsp` — the archived exchange-native reader for 2000-2006

**Status 2026-08-26: reader VALIDATED, index BUILT, gate NOT YET calibrated for the 2000-2001 page
revision. No cell has been written from this tool. Do not write one until §4 is done.**

## 1. What this is

NSE's retired results page, `marketinfo/companyinfo/eod/results.jsp`, preserved in the Wayback
Machine. It is **exchange-native and as-filed** — not an aggregator rendition — so it outranks a
Moneycontrol value under §108 and can adjudicate the vintage refusals GATE E hands back.

Its great virtue is that **every field a gate needs is DECLARED IN THE PAGE TEXT**:

```
Company        NAGARJUNA FERTILISER & CHEMICALS LTD.
NSE Symbol     NAGARFERT
Result Period  01-APR-2000 to 30-JUN-2001 (Others)
Result Type    Unaudited, Non-Cumulative, Non-Consolidated
               Non Banking Financial Results (Rs.lakhs)
Net Profit(+)/Loss(-)  4805.00    Basic EPS 1.15   Paid-up Equity 41661.00   Face Value 10.00
```

Audited/Unaudited · **Cumulative/Non-Cumulative** · **Consolidated/Non-Consolidated** ·
Banking/Non-Banking · scale. So §55b's "the same end-date labels a QUARTER and a YTD column" trap is
closed **by the document**, not by a date match. `wb_read.parse()` reads those words and nothing
positional.

⚠️ **Never decide from the URL.** The query string encodes the same flags, and STEP W's feasibility
pass mis-read the consolidation slot and nearly landed TATAMOTORS' *consolidated* FY2003 row as
standalone. URL is for cheap existence-counting only.

## 2. This is STEP W's source — read `PRE2015_CAMPAIGN.md` before touching 2002-2004

STEP W-execute is **COMPLETE** for **2002-2004**: 2,319 landed / 2,174 refused / 0 open, refusals
audited three ways. Its enumeration was complete too — independently re-measured here at 10,871
distinct pages against its reported 10,874. **Do not re-sweep 2002-2004.** Its four refusal classes:
A (809, never archived) · C (588, page carried no PAT) · B (396, wrong FY archived) · D (381, legs
present, gates could not prove them — refusing is the CORRECT outcome; re-landing a D cell is a
regression).

**What IS open is everything outside its target set.** By PERIOD-END year the archive holds
**2000: 146 · 2001: 1,327** · 2002: 2,406 · 2003: 3,449 · 2004: 3,026 · 2005: 454 · 2006: 63 distinct
pages. The 1,473 pages ending in **2000-2001** were indexed by STEP W and never requested, because
no gap cell existed to target them. See runbook **§112e** — "source-exhausted" is a claim about the
source *relative to the cells you asked for*.

## 3. Files

| file | what |
|---|---|
| `wb_read.py` | `fetch(ts, original)` + `parse(html)` → declared period/role/type/basis/scale/bank + `pat_cr`, `eps`, `paidup`, `months`; `face_of(html)` |
| `_wb_index.json` | `"SYM\|QEINT" -> [wayback_timestamp, original_url]`, 4,606 entries — only periods ENDING on a quarter-end, only symbols that are keys in `sf_fundamentals.json` |

Rebuild the index from CDX:

```bash
curl -s "http://web.archive.org/cdx/search/cdx?url=nseindia.com%2Fmarketinfo%2Fcompanyinfo%2Feod%2Fresults.jsp*&fl=timestamp,original&filter=statuscode:200&limit=30000" -o cdx.txt
```
⚠️ Use the **text** output. `output=json` truncates mid-string around 500KB and yields invalid JSON —
a byte limit, not the row cap. And note **20,847 raw captures collapse to 10,871 distinct pages**;
differencing raw against distinct is what nearly produced a phantom "truncated index" diagnosis.

⚠️ Wayback throttles hard under sustained fetching (measured: ~9 s/request after a few hundred).
Slowness and failures are **infrastructure state, never evidence about the data** — the same trap
STEP W's own audit #1 fell into ("absent from cache" during an outage read as "not fetchable").

## 4. The gate — and the one thing still unfinished

```
G1  the page's own "NSE Symbol" == the symbol asked for          (identity)
G2  period spans exactly 3 months AND declares Non-Cumulative    (a true quarter)
G3  declares Non-Consolidated                                    (standalone slot)   ← SEE BELOW
G4  declares a scale we know (lakhs ÷100 → cr)
G5  the page's OWN arithmetic closes: EPS == NetProfit × FaceValue / PaidUpCapital, ≤3%
```

G5 is the third independent check the runbook asks for and needs **nothing from us** — which is the
point, because in 2000-2004 we usually hold nothing nearby to anchor against.

### ⚠️ G3 IS WRONG FOR THE 2000-2001 PAGE REVISION — this is the open item
That revision prints only **two** axes (`Unaudited, Non-Cumulative`); the basis axis appears from
~2002. As written, G3 refused **44 of the first 75 true quarters** for lacking a token the era never
emitted — absence manufactured by the instrument (runbook §112f). The fix is to require the token
only when the page prints three axes. **That relaxation is NOT yet calibrated, so it must not be
used to write.**

**What IS measured so far:**
* Reader validation, 20 random cells we already hold (2001-2006): **true-quarter pages 8 MATCH / 0
  differ**; 10 "differs" were all *cumulative* pages the reader correctly refused; 2 READ-FAIL, both
  **banks** (BANKBARODA, SYNDIBANK — the banking template uses a different row schema, still unread).
* Basis probe, 34 true-quarter pages: **32 reproduce our stored `npStd` exactly**, 2 do not.
* Two-token pages specifically: **22 of 24 reproduce `npStd`**. The two misses are informative —
  MASTEK Jun-02 prints `Basic EPS 0.00` with no face value, so **G5 rejects it on the page's own
  arithmetic**; BEL Mar-01 (87.92 vs our 97.49) is a Q4 marked *Unaudited*, i.e. a restatement
  boundary, not a reader failure.
* **Hold-out of the FULL gate (mismatch rate among cells the gate would actually WRITE) was still
  running when the session ended, throttled by Wayback.** `calib_full.py` pattern: take cells we
  already hold, run G1-G5, and report MATCH/MISMATCH among the WRITEs only.

### ⚠️ And state this limit next to whatever number that produces
A hold-out against cells we already hold is evidence about **well-covered companies**. The 2000-2001
cells actually wanted are precisely the ones with **no stored value to check against**, so the
measured mismatch rate is a lower bound on the rate that matters.

## 5. Where the value is

* **210 open FAV14 root cells in 2000-2001** have a page whose period ends on that quarter
  (7 in 2000, 203 in 2001) — untouched by STEP W, and the tier this campaign owns
  (`PLAN_FAV14_PRE2009.md`).
* **575 more in 2002-2006** — but expect most to be already landed or already adjudicated by
  STEP W. Diff against its ledger before fetching anything.
* **The banking row schema is unread**, and this era is bank-heavy (DENABANK, FEDERALBNK, J&KBANK,
  KTKBANK, SOUTHBANK are all top-25 gap symbols). STEP W's feasibility block names bank-template
  rows plus a shorter 2002-vintage URL grammar (1-char audit flag) as a large share of its unparsed
  residual. Teaching the parser that schema probably returns more than the quarter-page set does.
