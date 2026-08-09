# EXCHANGE LEG — capability card

REV/PAT verify campaign. This characterises the two routes that read quarterly REVENUE and PAT
straight from the exchanges' own machine-readable filings — independent of every retail site AND
independent of anything already stored in this project's own JSONs. Session date: 2026-08-09.
Every claim below is MEASURED in this session (with the exact evidence) unless marked
**UNKNOWN — not measured**. Request budget used: ~31 of the 40-60 allowed. **No 403/429 was hit
against either exchange at any point this session** — both are reachable right now.

All code and cached responses referenced below live in this working directory:
`/private/tmp/claude-501/.../scratchpad/revpat/exchange/` (`exchange_fetch.py`, `demo_output.txt`,
`_cache/*` — the raw JSON/XML this whole card is built from).

---

## A. BSE detailed-results JSON (runbook §42)

**Endpoint and QID formula: CONFIRMED, still live today.**
`https://api.bseindia.com/BseIndiaAPI/api/Corp_detailedResult_Transpose_ng/w?scrip_cd=<CODE>&qtr=<QID>`,
`QID="NN.00"`, `NN = 85 + 4*(calendar_year(qe)-2015) + {Mar:0,Jun:1,Sep:2,Dec:3}`.

Verified against RELIANCE (scrip 500325) Mar-2024: formula gives `NN = 85 + 4*(2024-2015) + 0 =
121` → `qtr=121.00`. Response's own `Date Begin`/`Date End` = `01-Jan-24`/`31-Mar-24` (i.e. the
correct calendar quarter), confirming the arithmetic, not just a plausible-looking number.
`121.50` (annual) returned `Date Begin`=`01-Apr-23`, `Date End`=`31-Mar-24` — a 12-month span on
the FY-end quarter, exactly as §42 describes.

**The formula also extrapolates backward correctly, beyond §42's "2015+" claim.** QID `56.00`
(=Dec-2007 by the same arithmetic) returned a full 27-row RELIANCE response (`Date Begin`=
`01-Oct-07`, `Date End`=`31-Dec-07`, real P&L figures) — the endpoint's live floor is at or below
2007, not 2015. QID `1.00` (~Mar-1994) returned a 3-row stub (Type/Date Begin/Date End only, no
financials) — somewhere between 1994 and 2007 the data thins out; **this session did not pin the
exact boundary** (out of scope here — `scripts/PRE2015_CAMPAIGN.md` in the reference tree is the
dedicated, paused effort on that range). Treat "2015+" as the *validated* floor and pre-2015 as
*reachable but unvalidated by this deliverable*.

**Row labels — measured on an industrial AND a bank, both Mar-2024:**

| | Industrial (RELIANCE) | Bank (SBIN) |
|---|---|---|
| Revenue | `Net Sales/Revenue From Operations` | `Interest Earned/Net Income from sales/services` |
| PAT | `Net Profit` (duplicate: `Net Profit (+)/ Loss (-) from Ordinary Activities after Tax`) | `Net Profit` (same duplicate present) |
| Bank-only extras | — | `Operating Profit Before Provisions and Contingencies`, `Amount of Gross NPA`, `% of Net NPAs`, `CET 1 Ratio`, EPS rows, `Equity Capital`, `Face Value` |

Both companies' `Net Profit` label carries the final bottom-line figure; the longer-named row is
an exact duplicate in both cases (measured, not assumed).

**Units: CONFIRMED ₹ MILLION (÷10 for crore) by exact calibration, not inference.**
RELIANCE Mar-2024: page `Net Profit` = `112830.0000` → `/10` = **11283.0 cr**. This project's own
stored standalone PAT for RELIANCE 2024-03-31 is **11283.0** — exact match. SBIN Mar-2024: page
`Net Profit` = `206983.5000` → `/10` = **20698.35 cr**, stored standalone PAT **20698.35** — exact
match. Two companies, two formats (industrial + bank), both exact.

**Basis: RE-TESTED — §42's "no working consolidated endpoint found" STILL STANDS.** What was
actually tried this session (5 probes, all against RELIANCE 500325 qtr 121.00):
1. `&flag=C`, `&consd=Y`, `&Result_Type=C` appended to the working URL — all three returned
   **byte-identical** responses to the plain call (same 3,706 bytes) — the params are silently
   ignored, not honoured.
2. `Corp_detailedResult_Transpose_ng_C/w?...` (guessed "_C" suffix) — HTTP 200, but the body is
   BSE's generic **soft-404** page ("The Page you are looking for has been moved"), not a real
   response. A length-only or status-only check would misread this as success.
3. `Corp_detailedResult_ng/w?...` (guessed bare name without "Transpose") — same soft-404.

Additionally, and more decisively: **the endpoint carries no basis field in the response body at
all** — no `Consolidated`/`Non-Consolidated` meta, unlike NSE's route (§B). So "standalone" is not
even a claim BSE makes about this data; it is established purely by calibration. Both calibration
companies land exactly on stored STANDALONE and clearly off stored CONSOLIDATED (RELIANCE: std
11283.0 vs con **18951.0**; SBIN: std 20698.35 vs con **21384.15**) — decisive, but a calibration
result, not a document assertion. **This matters for how a caller should treat the output**: report
"standalone, by construction/calibration" not "standalone, as declared."

**Date Begin/End span check: CONFIRMED essential.** Same scrip+FY-end QID family, two id
suffixes: `.00` (quarter) gives a 3-calendar-month span; `.50` (annual) gives 12 months on the
*same* `Date End`. A reader keying only on "does Net Profit exist" without checking the span would
silently swap in the annual total. `exchange_fetch.py`'s BSE route hard-refuses unless
`(end_year*12+end_month)-(begin_year*12+begin_month) == 2` AND `Date End == target qe`.

---

## B. NSE per-basis XBRL (runbook §54, memory "NSE list rows carry per-basis XBRL")

**Endpoint structure: CONFIRMED.**
`https://www.nseindia.com/api/corporates-financial-results?index=equities&symbol=<SYM>&period=Quarterly`
returns one row per (quarter, basis). Measured on RELIANCE: 130 rows, 84 `Non-Consolidated` + 46
`Consolidated`, each with its own `consolidated` field and `xbrl` URL.

**Depth and the xbrl-population boundary — both measured, on RELIANCE only:**
* Earliest row: `toDate = 31-Mar-2005`. (Not independently checked on a second company —
  **UNKNOWN — not measured** whether ~2005 is a hard NSE floor or an artefact of RELIANCE's own
  filing history; runbook's general "~2005" claim is consistent with this one data point.)
* `xbrl` is a placeholder URL ending `corporate/xbrl/-` for older rows (a **non-empty string** —
  a naive `if row["xbrl"]:` truthiness check treats it as populated). For RELIANCE the transition
  from placeholder to a real XBRL URL happens **exactly at `30-Jun-2018`** — every row before that
  is a placeholder, every row from it onward is a real `nsearchives.nseindia.com/corporate/xbrl/
  INDAS_...xml` link. Before that point, `resultDetailedDataLink` (a different field, pointing at
  the pre-2018 archived HTML detail pages built by `scripts/_nse_archive_revop.py` in the
  reference tree) is populated instead — a separate sub-route this session did not exercise
  further, since §54/§59b already document it as the pre-2020 tail's cover.

**Extraction: CONFIRMED reproducible via `build_revop.parse_file` (the nightly's own parser) —
with one gap in that function that this tool does NOT inherit (see §E, trap #6).** Fetched one
std and one con XBRL each for RELIANCE, SBIN (bank) and TCS (Dec-2024), parsed independently, and
cross-checked against this project's own stored `sf_fundamentals.json`/`sf_revop.json` (used here
purely as an internal-consistency check — see caveat below):

| symbol | basis | rev (cr) | PAT owners/total (cr) | matches stored exactly? |
|---|---|---|---|---|
| RELIANCE | std | 151014.0 | 11283.0 (no NCI concept) | yes |
| RELIANCE | con | 243865.0 | owners 18540.0 / total 21930.0 | yes (owners) |
| SBIN | std | 117426.63 | 16891.44 | yes |
| SBIN | con | 124653.66 | owners 18853.16 / total 19175.35 | yes (owners) |
| TCS | std | 53883.0 | 11832.0 | yes |
| TCS | con | 63973.0 | owners 12380.0 / total 12444.0 | yes (owners) |

⚠️ **Honesty caveat on "matches stored":** this project's own stored values were themselves
originally built from this same NSE XBRL route by the nightly job, so exact agreement here mostly
proves the *parsing convention* is consistent with what's already in production — it is **not** an
independent cross-check of correctness. The genuinely independent cross-check is BSE-vs-NSE
agreement (§C below), which uses two unrelated formats from two unrelated organisations.

**PAT tags — which carry owners vs NCI vs total, and how to tell (measured by grepping the raw
XBRL, not inferred from parsed output):** RELIANCE Dec-2024 consolidated file:
```
ProfitLossForPeriod                                    = 21,930.0 cr   (total, incl. NCI)
ProfitOrLossAttributableToOwnersOfParent               = 18,540.0 cr   (owners)
ProfitOrLossAttributableToNonControllingInterests      =  3,390.0 cr   (NCI)
```
18,540.0 + 3,390.0 = 21,930.0 exactly — the three tags form a closed identity, confirmed to the
crore. **Banks carry no `ProfitOrLossAttributableToOwnersOfParent` tag at all**; the owners-basis
bottom line lives under a differently-named tag,
`ProfitLossAfterTaxesMinorityInterestAndShareOfProfitLossOfAssociates` (SBIN con: that tag =
18,853.16, the plain total-period tag = 19,175.35 — a reader that only looks for the standard
owners tag name will silently fall back to the wrong, NCI-inclusive figure for every bank).
Standalone filings carry neither owners nor NCI tags (no subsidiaries to split from) — the total
tag alone is the correct PAT there, confirmed by every standalone row above showing
`pat_owners_tag_value_cr: null` and the total tag matching stored exactly.

**Depth of `xbrl` population, and the empty-cell class this explains:** consistent with runbook
§54a — a cell being empty in this project's own data is much more likely to mean "the daily fetch
missed this filing" than "no document exists," precisely because the XBRL is still served live by
this same endpoint years later. Every XBRL fetched this session came back on the first try, with
no rate limiting.

**Transport: curl_cffi (chrome impersonation) + a homepage warmup GET, used throughout this
session — zero errors, zero 403/429 across the whole session (list fetches + 6 XBRL downloads).
Also measured, and worth flagging honestly: a single plain-`urllib` GET (no impersonation, no
cookie jar, no warmup) against the SAME `corporates-financial-results` endpoint also returned 200
this session.** That is a genuine, reproducible observation, not a guess — but it should NOT be
read as "NSE no longer needs curl_cffi." This project's own history (memory:
"project-stocks-nse-api-lockdown", "NSE 403-blocks plain urllib" in `fetch_nse.py`'s own
docstring) documents NSE's bot defenses tightening intermittently and unpredictably; one success
with plain urllib in one session is not evidence the defense is gone, only that it wasn't active
at this particular moment. `exchange_fetch.py` uses curl_cffi throughout regardless, matching
project convention.

---

## C. Comparison — coverage matrix (era × basis × route)

| Era | Basis | BSE detres (§A) | NSE XBRL (§B) |
|---|---|---|---|
| pre-~2007 | std | UNKNOWN — floor not pinned this session (reaches ≥ Dec-2007 for RELIANCE) | list itself may not reach back this far for most companies (RELIANCE floor measured at Mar-2005) |
| pre-~2007 | con | not served (route is standalone-only) | same depth question as std, AND consolidated quarterlies were rare/non-standard this early |
| 2007–2017 | std | reaches back (measured for RELIANCE); §42's own "2015+" is the *validated*, not the *floor*, boundary | list rows exist, but `xbrl` is a placeholder until the filer's own XBRL-era transition (RELIANCE: 30-Jun-2018); `resultDetailedDataLink` (a separate HTML-detail sub-route, not exercised this session) covers this window instead |
| 2007–2019 | con | **not served — never, structurally** (route has no consolidated variant) | placeholder-xbrl for the same window as std, AND quarterly consolidated was not even *compulsory* until FY2020 (runbook §53) — most companies genuinely never filed a con quarter here (measured elsewhere in this project: only 2.7% of the pre-2020 con-PAT gap population had one) |
| 2018–2020 | std | yes | yes, once the filer's own xbrl-era begins |
| 2018–2020 | con | not served | yes where filed (sparse — see above) |
| 2020–present | std | yes | yes |
| 2020–present | con | not served | yes (compulsory since FY2020 for companies with subsidiaries) |

**Where they overlap, and the cross-check this session actually ran:** the only window both
routes cover is **standalone, from each filer's own XBRL-era start (≥2018) through today**. This
session cross-checked RELIANCE Mar-2024 standalone on BOTH routes independently:
* BSE detres: revenue **151014.0** cr, PAT **11283.0** cr (source: `Corp_detailedResult_
  Transpose_ng?scrip_cd=500325&qtr=121.00`)
* NSE XBRL: revenue **151014.0** cr, PAT **11283.0** cr (source:
  `nsearchives.nseindia.com/corporate/xbrl/INDAS_104633_1106029_22042024074028.xml`)

**Exact agreement, to the crore, from two unrelated organisations' own systems, in two unrelated
formats (a JSON transpose-table vs an XBRL fact tag).** This is the strongest evidence in this
whole card that both routes are reading the real filed number, not an artefact of either
platform. This is also the *only* place the two routes can arbitrate each other — for
consolidated, NSE XBRL is the sole exchange-native route this campaign found; there is nothing to
cross-check it against on the exchange side (BSE detres never serves con at all). A stored
consolidated value can therefore be arbitrated by the exchange leg, but never DOUBLE-arbitrated
the way standalone can.

**§42's "no working consolidated endpoint" verdict: STILL STANDS**, re-tested this session (§A),
not merely re-quoted.

---

## D. Working fetcher — `exchange_fetch.py`

`fetch(SYMBOL, quarter_end_YYYYMMDD, basis)` walks BSE detres → NSE XBRL, in that order, and
**only** ever returns a value when the basis and the exact 3-calendar-month period-to-target-qe
are confirmed FROM THE RESPONSE ITSELF — never from the request, never assumed. Otherwise it
returns a structured refusal naming every route tried and why each one failed. `nse_read` and
`bse_read` are also independently callable for direct route-vs-route comparison (used for the
cross-check above and for combo C below).

**7 measured combinations, both routes, both bases, three sectors (industrial / bank / IT
services), run live this session — full JSON in `demo_output.txt`, summarised:**

| combo | call | route | revenue (cr) | PAT (cr) | basis note |
|---|---|---|---|---|---|
| A | `fetch('RELIANCE', 20240331, 'std')` | bse-detres | 151014.0 | 11283.0 | — |
| B | `fetch('SBIN', 20240331, 'std')` | bse-detres | 111042.63 | 20698.35 | bank format |
| C | `nse_read('RELIANCE', 20240331, 'std')` (direct) | nse-xbrl | 151014.0 | 11283.0 | exact cross-check vs A |
| D | `fetch('RELIANCE', 20241231, 'con')` | nse-xbrl (BSE refused first) | 243865.0 | 18540.0 | owners-attributable, not the 21930.0 total |
| E | `fetch('SBIN', 20241231, 'con')` | nse-xbrl (BSE refused first) | 124653.66 | 18853.16 | bank owners tag, not the 19175.35 total |
| F | `fetch('TCS', 20241231, 'con')` | nse-xbrl (BSE refused first) | 63973.0 | 12380.0 | owners-attributable, not the 12444.0 total |
| G | `nse_read('TCS', 20241231, 'std')` (direct) | nse-xbrl | 53883.0 | 11832.0 | NSE route on std, bypassing the BSE-first ladder |

Plus two refusal demonstrations (H: a non-quarter-end date; I: consolidated requested for a
pre-XBRL-era quarter) — both refuse cleanly with named, itemised reasons rather than guessing.
Every one of A–G's values agreed with `build_revop.parse_file`'s own independent re-parse of the
same file (`nightly_parser_agrees: true` in every result — see `demo_output.txt`).

---

## E. Traps — everything found that would silently produce a wrong number

1. **BSE detres declares no basis field at all.** Purity rests on calibration + construction, not
   a document assertion — unlike NSE, which does declare basis. A caller treating "got a number
   back" as "got the standalone number, confirmed" is one step ahead of what the response
   actually says.
2. **Guessed-endpoint soft-404s return HTTP 200.** `Corp_detailedResult_Transpose_ng_C` and
   `Corp_detailedResult_ng` both return status 200 with BSE's generic "page has been moved" HTML.
   A status-code-only or length-only check reads this as a live response.
3. **Undocumented query params are silently ignored**, not rejected — `flag=C`/`consd=Y`/
   `Result_Type=C` all returned byte-identical output to the bare call. Silence looks like
   success if you're not diffing bytes.
4. **The BSE QID space is shared between quarter (.00) and annual/H1 (.50) rows** — a reader that
   trusts any `Net Profit` value without checking `Date Begin`/`Date End` spans exactly 3 calendar
   months will happily swap in an annual total as a quarter.
5. **NSE's `xbrl` placeholder (`.../corporate/xbrl/-`) is a non-empty string.** A naive
   truthiness check (`if row["xbrl"]`) treats a placeholder exactly like a real link.
6. **★ THE BIG ONE — NSE's `FourD` context can declare the IDENTICAL 3-month start/end as
   `OneD` while holding 9-month YTD cumulative values.** Measured on 4 of 4 sampled Q3 (Dec-2024)
   filings — RELIANCE std, RELIANCE con, SBIN con, TCS con and TCS std — every `FourD` context's
   own `<xbrli:context>` declared dates were byte-identical to `OneD`'s, and every `FourD` value
   was ~2.9–3.0× `OneD`'s (the 9-month/3-month ratio). **This is not a hypothetical**: this
   session's own first draft of `exchange_fetch.py` trusted `FourD` as a same-quarter fallback
   (matching the assumption in `build_fundamentals.py`'s own docstring — "FourD = CONSOLIDATED
   current quarter") and it produced FIVE wrong values in the first demo run (e.g. RELIANCE
   Mar-2024 standalone PAT came out 42042.0 instead of the correct 11283.0) before the
   `nightly_parser_agrees` cross-check flagged the disagreement. **No date-span check on the
   context's own declared period can catch this**, because the declared period is a perfect
   match — only knowing that NSE now files ONE XBRL PER BASIS (so the file you fetched was
   already the right basis, and `OneD` is therefore always the right context) protects against
   it. `exchange_fetch.py` now never uses `FourD` as a value source at all. This sharpens runbook
   §45's "context IDs have no fixed meaning" warning: it is not just that FourD's *meaning*
   varies by filer, but that its *declared period* can be actively wrong for its own content in a
   way that mimics being right.
7. **Revisions**: NSE can carry more than one filing for the same (quarter, basis) (a
   correction). `exchange_fetch.py` takes the latest `filingDate`; every case measured this
   session had exactly one candidate, so the multi-candidate path is implemented but untested
   live — flagged, not claimed.
8. **Total PAT vs owners-attributable PAT** differ materially for every consolidated quarter
   sampled (RELIANCE 21930.0 vs 18540.0; SBIN 19175.35 vs 18853.16; TCS 12444.0 vs 12380.0) — a
   reader using the generic `ProfitLossForPeriod`/`ProfitLossAfterTax` tag alone overstates PAT by
   the minority-interest slice every time there's a partly-owned subsidiary.
9. **Bank consolidated filings carry no `ProfitOrLossAttributableToOwnersOfParent` tag at all** —
   the owners-equivalent lives under `ProfitLossAfterTaxesMinorityInterestAndShareOfProfitLossOf
   Associates` instead. A reader that only recognises the standard tag name silently falls back
   to the NCI-inclusive total for every bank.
10. **BSE scrip-code resolution here only covers the active-equity master** (~4,949 rows,
    measured count — a much smaller count would itself be the rate-limit-stub tell per §0).
    Delisted/renamed companies need the separate `status=Delisted` pull documented in runbook
    §52b; this campaign's tool does not implement it and refuses honestly
    (`no-bse-scrip-code`) rather than guessing a code.
11. **NSE keys its historical list by the symbol that traded AT THE TIME** — a renamed company's
    older quarters may sit under an old ticker NSE's list API won't return for the current
    symbol. `exchange_fetch.py` does not implement alias-chasing (the reference tree's
    `_nse_archive_revop.aliases()` does, for a different sub-route) — a known, disclosed gap, not
    a silent wrong answer.
12. **Transport variability**: a bare `urllib` GET against NSE succeeded once this session with
    no cookie warmup at all, which is not what this project's own history says to expect. Do not
    generalise from one success — keep using curl_cffi + warmup as the default.
