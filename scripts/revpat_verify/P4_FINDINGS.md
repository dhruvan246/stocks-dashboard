# P4 — BULK SWEEP: the whole store checked for the wrong-row revenue defect
Screener, standalone, **3,594 symbols crawled / 3,244 with data / 37,503 rows**, ~2h20m at 2s
spacing, **zero 403/429**. Financials crawled first because both confirmed instances live there.
Tools: `bulk_screen.py` (offline screens, phase A) → crawl → `sweep_analyze.py` (phase C).
Raw: `p4/screener_p4.jsonl`, `p4/wrongrow_candidates.json`, `p4_bulk_candidates.json`.

---

## 1. THE HEADLINE: there is NO systematic wrong-row bias

**Median relative difference between our stored standalone revenue and Screener's, across 38,326
compared cells: exactly 0.0.** Whatever went wrong on HUDCO and AADHARHFC did not go wrong globally.

**43 of 3,244 symbols (1.3%)** carry the defect signature — revenue below an independent source over
**consecutive** quarters **while PAT agrees**. That third condition is what makes the screen narrow:
if PAT disagreed too we would be looking at the wrong company, period or scale, not a revenue row.

## 2. THE CANDIDATES SPLIT INTO TWO DIFFERENT DEFECTS — do not treat them as one list

### Class A — our value is ~ZERO while the source has a real number (8 symbols)
`SANWARIA` (12 quarters), `MAHAPEXLTD` (13), `MVL`, `AUSTRAL`, `OMKARCHEM`, `RTNINDIA`, `CEMPRO`,
`NECLIFE`. Ours reads 0.0 or near-zero against a real figure. **This is the blank-row / zero-sentinel
class (§58c: "a printed 0.00 is a blank row, not a result"), not a component-instead-of-total pick.**
Mostly micro-caps and suspended names where the amounts are tiny in absolute terms — low user impact,
different root cause, and it needs its own fix. Filed separately on purpose.

### Class B — our value is a plausible COMPONENT of the total (30 symbols) ← the real hunt
This is the AADHARHFC/HUDCO shape. Ranked by worst quarter:

| symbol | fin | run | window | worst | median |
|---|---|---|---|---|---|
| GYFTR | | 3 | 2023-06 → 2023-12 | −87.1% | −82.8% |
| MTNL | | 4 | 2024-09 → 2025-06 | −64.4% | −39.4% |
| ORTINGLOBE | | 2 | 2025-03 → 2025-06 | −50.0% | −47.2% |
| AFSL | | 2 | 2023-03 → 2023-06 | −45.8% | −41.1% |
| MMTC | | 2 | 2023-12 → 2024-03 | −36.0% | −31.5% |
| IWEL | Y | 2 | 2022-03 → 2022-06 | −35.2% | −33.4% |
| CHROMATIC | | 2 | 2019-03 → 2019-06 | −35.1% | −21.5% |
| **BSE** | | **5** | 2023-06 → 2024-06 | −23.2% | −18.0% |
| PIRAMALFIN | Y | 2 | 2024-12 → 2025-03 | −21.5% | −20.3% |
| JPASSOCIAT | | 2 | 2023-12 → 2024-03 | −21.0% | −19.5% |
| TVSHLTD | Y | 3 | 2023-06 → 2023-12 | −19.5% | −5.6% |
| SADBHIN | | 2 | 2023-09 → 2023-12 | −16.5% | −9.7% |
| **HDBFS** | Y | **4** | 2024-06 → 2025-03 | −15.9% | −15.1% |
| AVAILFC | | 2 | 2023-06 → 2023-09 | −15.4% | −11.3% |
| THEMISMED | | 2 | 2024-06 → 2024-09 | −14.9% | −14.3% |
| **AADHARHFC** | Y | 5 | 2023-06 → 2024-06 | −12.1% | −7.7% | ← **PROVEN, staged** |
| CUPID | | 4 | 2025-06 → 2026-03 | −10.1% | −8.6% |
| SELMC | | 4 | 2024-09 → 2025-06 | −9.6% | −4.8% |
| ESSARSHPNG | | 4 | 2023-12 → 2024-09 | −8.6% | −8.3% |
| SPICEJET | | 3 | 2024-06 → 2025-06 | −7.7% | −6.6% |
| INDOWIND, PDPL, EROSMEDIA, LAHOTIOV, SHLAKSHMI, MONARCH, MOTHERSON, RNBDENIMS, SAKTHIFIN, SUPREMEINF | | 2-4 | various | −7.3% → −3.6% | |

**AADHARHFC sits at rank 16 and is already proven from its filings** — the screen rediscovered a
known defect independently, which is the calibration that makes the other 29 worth reading.

**It is not a financials-only problem.** Only 7 of 43 are `fin=1`, despite financials being crawled
first on the theory that the sub-line trap lives there. The theory was too narrow.

**Priority by exposure, not by percentage:** BSE (5 consecutive quarters), HDBFS (4), MTNL (4),
SELMC (4), ESSARSHPNG (4), CUPID (4), SAKTHIFIN (4), LAHOTIOV (4) — a long run is far stronger
evidence of a parse *rule* than one large quarter, which can be a restatement.

## 3. THE IDENTIFIED ROOT CAUSE, AND ITS UNTESTED COHORT
`screener_prerev.py` accepts a revenue row once the page's **NET PROFIT** matches our stored PAT.
A PAT-only anchor cannot see which *revenue* row was taken — exactly how AADHARHFC's `Interest
income` row landed in the revenue slot. **`scripts/screener_rev_fills.json` holds 191 cells across
96 symbols filled on that same anchor; none has been re-checked.** The sweep now covers 74 of those
96, so most can be tested without new fetches.

**The general rule this earns:** *an anchor that validates one field does not validate another.*
PAT matching proves the page is the right company and quarter. It says nothing about the row.

## 4. ★ A COVERAGE CAVEAT THAT MUST NOT BE READ AS A CLEAN BILL
**350 symbols produced no data: 286 HTTP 404 and 64 empty quarterly tables.** They are **not**
checked, and the 404s must not be recorded as "delisted".

**Screener's URL slug is not always the NSE symbol** — proven on **KPIT**, an actively traded company
that 404s at `/company/KPIT/` and resolves at `/company/KPITTECH/`. `TATAMOTORS` looks like the same
shape, unverified. This contradicts the P1 capability card's assumption that the slug equals the
ticker, and it means an unknown share of those 286 are live companies we simply failed to address.
**Treat the 404 list as "needs slug verification", never as absence** (§57 rule 1: a route returning
nothing is never evidence the value does not exist).

## 6. ★★★ THE PAT-ONLY-ANCHOR COHORT IS FULLY CHECKED — AND CLEAN (234 of 234)

`screener_rev_fills.json` = 191 (symbol, quarter) keys = **234 basis-cells** (127 std, 107 con)
across 96 symbols, 2015-06 → 2025-09, every one filled by `screener_prerev.py` on the PAT-only
anchor that produced the AADHARHFC defect. All 234 have now been compared against Screener's
current revenue row — the 107 consolidated cells needed a targeted `/consolidated/` crawl of
57 symbols (632 rows, 48 live fetches, 9 served from cache, zero 404s, zero empty tables, no 403).

| result | cells |
|---|---|
| **MATCHES_SITE_ROW** | **234 / 234** |
| row-selection mismatches | **0** |

**The 8 basis-copy flags are all LEGITIMATE.** These were sym-quarters where the tool recorded the
same revenue for con and std, still identical in our store: ABDL ×2, ABLBL, CPPLUS, FORCEMOT ×2,
HDBFS, MEESHO. With consolidated site data in hand the discriminator is decisive — **Screener shows
con == std for all eight as well**, which is exactly `detect_con_copy.py`'s own rule that a genuine
identity shows con == std on the site too. Not copies.

**What a match does and does not mean.** These cells were sourced FROM Screener, so under rule 6b's
provenance echo Screener cannot *confirm* them. But the defect under test is that the wrong ROW was
taken from that page, so comparing against Screener's revenue row today is a row-selection check,
and a mismatch would have been real evidence. **Finding none across 234 cells bounds the blast
radius of the PAT-only anchor to the cases already found** — it is not a clean bill on those cells'
absolute correctness, which only a filing read could give.

**So AADHARHFC was not the tip of an iceberg.** The tool's blind spot is real and the rule it earns
stands (*an anchor that validates one field does not validate another*), but its realised damage in
this cohort is zero. The open work is now the 30 Class-B candidates from §2, which came from a
different detector and are not part of this cohort.

## 5. WHAT THIS PHASE DID NOT DO
- **Nothing here is adjudicated.** All 43 are candidates. Every screen in this campaign has produced
  false positives alongside real defects — the `con==std` screen fires 6,470 times and HUDCO proved
  that shape is usually legitimate. Only a filing read decides.
- Consolidated revenue was not swept (standalone only — one page per symbol, chosen because
  standalone is where multiple sites can vote and where our PAT has essentially full backtest reach).
- ~~The 191-cell cohort is identified but not yet cross-checked.~~ **DONE — §6, 234/234 clean.**
- Class A (zero-sentinel) is filed, not diagnosed.
