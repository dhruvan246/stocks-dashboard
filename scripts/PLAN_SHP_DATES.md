# PLAN_SHP_DATES — recover REAL pre-2016 SHP submission dates (Opus executes; Fable planned 2026-08-23)

**Mission.** Replace the qe+21d visibility convention with measured filing dates for as many of
the 25,867 pre-2016 `shp_engine.json` rows as the public record allows. The user RESCINDED the
"keep + document" policy (2026-08-23): the convention now stands only as status quo, cell by
cell, until a real date replaces it or every route below has a measured zero for that cell.
"Doesn't exist publicly" is an OUTPUT of this plan, never an input (§57 / negative-verdict-needs-
full-ladder). Everything below follows the redating campaign's proven shape: probe reach first,
raw-row cache always, one shared matcher, calibrate on a holdout of KNOWN dates, refuse rather
than guess, ledger + provenance, dry-run apply, A/B, live verify.

**Probes already run (2026-08-23, this session — do not redo):**
* BSE announcements feed (`AnnSubCategoryGetData`, 'shareholding' text): **WEAK** — 1 stray
  "Updates" row across HINDUNILVR-2009 / CANFINHOME-2012 / MUNJALSHOW-2006. SHP filings did not
  ride the announcements stream. Do not build on it (spot use only).
* NSE `corporate-share-holdings-master`: **HAS `broadcastDate` with time** — but 20 rows/symbol,
  reach ≈ 2021+. Modern era only; useful as a truth source for calibration, not for pre-2016.
* Other session's dirty SHP files (read-only diff): **value-parsing work only** (pre-2013 base
  anchors). No date harvesting — no collision. Integration point still blocks on their landing.
* `bseindia.com/corporates/ShareholdingPattern.aspx?scripcd=` : **JS shell** (17KB, no table) —
  the data API is in the page's JS bundle. Recipe exists: memory feedback-find-endpoints-in-js-bundle.

## Phase 0 — finish the route ladder (measure REACH before building anything; ~1-2h)
Each rung: 3 known symbols (HINDUNILVR, CANFINHOME, MUNJALSHOW) × eras {2006, 2009, 2012, 2015}.
Deliverable: a route × era coverage matrix in section P0-RESULTS below, each cell MEASURED.
0a. **BSE SHP data API** — open the aspx page's JS bundle, extract the fetch URL(s) (the
    `w?`-style api.bseindia.com endpoints), and probe whether ANY field is a submission/filing
    date rather than the as-on quarter date. BSE's old SHP tables sometimes carry a
    "Date of Submission"/inserted-date column server-side even when the UI hides it.
0b. **NSE corporate-filings archive** — the §101 machinery went AROUND the BSE 2018 wall for
    results; find its endpoint and ask it for category "Shareholding Pattern" in 2009-2015
    (legacy corpfiling had SHP as a broadcast category with DissemDT-style timestamps). Also
    probe `archives.nseindia.com` paths for shp XBRL/HTML filings (post-2015 SHP is XBRL on both
    exchanges — those carry submission stamps; find how far back the XBRL era reaches: 2015-06?).
0c. **BSE XBRL/attachment route** — post-~2011 BSE SHP filings may exist as dated attachments
    (the AnnPdfOpen resolver class); probe the attachment index for shp filenames per scrip.
0d. **Wayback CDX density** — CDX API over `shpSecurities.aspx*scripcd=*` and the NSE shp URLs:
    captures per year 2006-2015. Each capture showing quarter Q data = UPPER BOUND visible≤D.
    (Prior art: project-stocks-shp-wayback-2010 — read it first.)
0e. **Empirical-lag calibration set** (no new fetching): from the earliest era with REAL dates
    (NSE master 2021+, XBRL 2015+ if 0b lands), compute the actual filing-lag distribution
    (median/p90/p95 vs the 21d deadline). This ships VALUE even if pre-2016 recovery fails:
    it turns the residual convention into a calibrated bound and sizes the late-filer tail.
GATE: any route with pre-2016 reach ≥ era-coverage 50% on the probe set → primary route.
No route reaches pre-2016 → write the full-ladder negative verdict into §105 (each rung, each
measurement), re-adopt "keep + documented" WITH the 0e calibration attached, and stop.

## Phase 1 — pilot (only if a route reached pre-2016)
36 symbols (reuse the redating pilot list), full pre-2016 span, raw responses cached to
`scripts/_shp_dates/raw*.jsonl` (F4 is a prerequisite, not a nicety). Matcher rules live in ONE
module imported by fetcher/audit/apply (F1). **Holdout calibration is the gate:** run the same
pipeline over 2016-2021 quarters where `shp_engine.json` already has real dates — pipeline date
must equal stored date ≥90% with zero EARLIER-than-real errors (an early date manufactures
look-ahead; a late one is only conservative). 5 pre-2016 hits hand-verified against the filing
artifact itself (page/XBRL/PDF).

## Phase 2 — full fetch
All pre-2016 symbols (~2,768), 2 shards max (BSE rate-limit history), newest-first, resumable,
sparse FAILED-only monitor + 10-min heartbeat. Raw cache mandatory.

## Phase 3 — ledger + apply (two stages, sequenced by the OTHER session)
3a (safe now): `scripts/shp_sub_dates.json` ledger {SYM|qe: {sub, src, bcast_ts}} + provenance;
    apply tool `apply_shp_dates.py`, dry-run default, contract gates: sub ≥ qe always; sub ≤
    qe+180 sanity; 15:30 gate via gate_calendar tdays; direction stats vs qe+21 (LATE filers
    found = real look-ahead removed — that is the campaign's core value; EARLIER-than-21 dates
    tighten conservatism). Refuse ambiguity; never guess.
3b (BLOCKS on the other session landing fetch_shareholding.py): integrate the ledger into the
    engine-feed writer (sub = ledger date when present, else qe+21d convention) so nightly
    regeneration cannot revert the heal (heal-via-ledger rule). Until 3b, any direct write to
    shp_engine.json is DOCUMENTED as revert-at-risk — prefer waiting unless the user says ship.
Then: §105 rewrite (convention only for residue cells, counts stated), engine shpAt comment
update + sw bump, A/B rerun (DII strategy — quantmac's 32 dii_unknowable rows re-scored),
workbook sheet A refresh, live verify, next-day sentinel re-check, memory + runbook §106 record.

## P0-RESULTS — MEASURED 2026-08-23 (Opus). Ladder complete for the routes that matter.

**★ 0a BSE per-scrip SHP API — FOUND, and it carries a REAL `filing_date_time`.**
`https://api.bseindia.com/BseIndiaAPI/api/SHPQNewFormat/w?scripcode=<code>` (found by enumerating
candidate endpoint names after the JS-bundle route dead-ended: the Angular bundle names the caller
`O.url.sHPQNewFormat` but the URL map is server-config, not in any shipped chunk; browser-network
inspection is blocked by policy for bseindia.com). Fields: yr, qtr, qtrid, status, **filing_date_time**,
revised_date_time, revised_reson, XbrlFile, xbrlurl, navigateurl.
**REACH BOUNDARY, measured on 5 symbols and consistent across all of them: timestamps begin at the
March-2016 quarter (oldest seen 2016-04-13/14/18/19, 2016-05-16) — pre-2016 rows EXIST back to 2001
but `filing_date_time` is null, `XbrlFile` empty.** The boundary is the SHP XBRL mandate, not a gap.
→ pre-2016: BSE does not hold the answer. 2016+: BSE holds it exactly, to the second.

**0b NSE — no pre-2016 route.** `corporate-share-holdings-master` has broadcastDate+time but returns
20 rows (~2021+) and ignores from_date/to_date (0 rows for 2009). `corporate-filings-shareholding-pattern`
and `corp-info?corpType=shareholding` both 404. No NSE pre-2016 SHP filing endpoint exists to find.

**0c BSE XBRL/attachment — subsumed by 0a's boundary.** Old rows carry `XbrlFile:""` and a bare
`xbrlurl:"/XBRL1/"`; the XBRL artifacts themselves start with the same 2016 mandate. No pre-2016 artifact.

**0d Wayback CDX — thin, late, upper-bounds only.** `ShareholdingPattern.aspx*`: 0 captures before
2012, then 1/64/104/84/147 for 2012-16 ACROSS ALL SCRIPS (per-scrip×quarter density is therefore
~nil). NSE `shldStructure*`: 0 before 2012. Cannot date 25,867 cells; at best a bound for a handful.

**★ 0e Empirical lag calibration — 748 real filings, 20 symbols, 2016+:**
median **17d**, p90 **21d**, p99 41d, max 47d. **Only 4.0% filed LATER than qe+21d** → the convention
is empirically sound for ~96% of filings and errs conservative (deadline-anchored) as argued in §105.
**56.4% were broadcast after 15:30 IST**, i.e. the correct visibility date is the NEXT session — the
convention silently ignores this for every cell it stamps.

### VERDICT + REVISED SCOPE
* **Pre-2016 (25,867 cells): NEGATIVE VERDICT, now EARNED not assumed.** Every route measured; the
  data does not exist publicly. Convention stands there, documented (§105), with the 0e calibration
  attached as its justification (median 17d, 96% within the 21d deadline).
* **★ POST-2016 (7,093 cells / 1,902 symbols): RECOVERABLE RIGHT NOW — this is the real find.**
  Our store still carries the qe+21d convention on 10.6% of post-2016 rows even though BSE publishes
  the exact timestamp for that era. Spot-check: 9/9 resolved, and materially — ARVINDFASN Jun-2021
  真 filed 2021-08-10 (41d, i.e. we carry 20 days of look-ahead on that cell today); SCHAEFFLER
  Mar-2016 filed 7 days EARLIER than assumed. Plus the 15:30 gate applies to 56% of them.
  → **Phases 1-3 re-scope to the post-2016 recoverable set** (same machinery, now with a truth source).


---

## P1-RESULTS — pilot run 2026-08-23 (36 symbols, 635 calibration cells). GATE FAILED AS WRITTEN — and the failure is informative, not fatal.

**★ FIRST, A CORRECTION TO P0:** this ground is PARTLY ALREADY WORKED and my P0 write-up did not
say so. `scripts/fetch_shp_bse_hist.py` + the `shp_fill_*` ledgers already use SHPQNewFormat for
"real per-filing dates" (2016-03..2019-06 and the 2026-08-07 N500 gap sweep), and
fetch_shareholding.py's own header already states pre-2016 has no real dates ("BSE deleted them").
So SHPQNewFormat was a RE-discovery, and §105's "no per-filing source known" was wrong when
written. What IS new: the 7,093 post-2016 cells those ledgers never reached (spread across every
year 2016-2026, 4,064 N500-ever / 3,029 never-N500), and the measurements below.

**Calibration outcome (pipeline vs the store's existing REAL dates):**
| mode | exact | earlier | later |
|---|---|---|---|
| with 15:30 gate | 227/635 = 35.7% | 20 | 388 |
| raw broadcast date | 567/635 = 89.3% | 27 | 41 |
| raw + lag guard ≤120d | 565/630 = **89.7%** | 27 | 38 |

**Why it fails, diagnosed not guessed — the REFERENCE is inconsistent, not the pipeline:**
1. **The store applies NO time gate.** 553 of its same-day cells span broadcasts from 00:56 to
   23:56. So the 388 "later" under gating are entirely our gate, not error.
2. **The store is a MIXTURE of source conventions.** The 27 residual "earlier" cases are all
   1-2 days with EVENING BSE timestamps (18:13, 19:22, 22:50, 23:48) — i.e. some ledgers did roll
   evening filings over and others did not. There is no single stored convention to match, so no
   mode can reach 90% against it.
3. **★ INDEPENDENT cross-check breaks the circularity: BSE is right, the store's outliers carry
   NSE lag.** BSE `filing_date_time` vs NSE `broadcastDate` over 258 overlapping 2021+ filings:
   64.7% same date, and every difference has NSE LATER — BBTC's 2022 quarters all show NSE
   `28-DEC-2023`, a bulk re-broadcast, against BSE's real 2022-04/07/10 filings. **This is exactly
   DATA_RUNBOOK §104's class** (NSE broadcast lags the true first-public BSE disclosure), now
   found in the SHP series too. RELIANCE Sep-2019: store 2019-11-20, BSE 2019-10-19 — a MONTH of
   stale visibility sitting in production today.
   → "pipeline earlier" is the pipeline being MORE correct, not a look-ahead risk. The gate's
   premise ("stored is truth") does not hold for this dataset.
4. **Real hazard found and guarded:** a "New" row can be a years-later RE-UPLOAD — TALWALKARS
   Sep-2021 carries filing_date_time 2026-06-29. The ≤120d lag guard rejects those (5 in pilot).
   Without it the campaign would write dates years late.

**Recovery measured on the target cells:** 468 fixes / 182 no-ops across 36/36 symbols, 0 API
misses. Extrapolates to roughly 5,000 of the 7,093 changing.

**DECISION REQUIRED before Phase 2/3 (a convention choice, not a fact — user's call):**
* **(a) RAW broadcast date** — matches the store's dominant behaviour; 89.7% agreement; but
  knowingly marks a 23:48 filing as visible that same day (a 1-day look-ahead on ~56% of cells).
* **(b) 15:30-gated** — the principled point-in-time rule and the one we already use for
  fundamentals ann-dates (gate_1530.py); every gated cell is strictly more correct; cost is
  intentional divergence from ~60k older SHP cells until they get the same treatment.
Recommendation: **(b)**, plus logging the divergence, because writing a date we have measured to
be one day early is the one error class this whole campaign exists to remove.

**ALSO OPENED (bigger than the 7,093, not yet scoped):** the store's non-convention post-2016
dates carry the §104 NSE-lag class — 27 of 630 pilot cells measurably stale, one by a month.
A full BSE-vs-store reconciliation over all ~67k post-2016 SHP cells is now warranted.


---

## P2-P4 RESULTS — SHIPPED 2026-08-23 (Opus)

**Phase 2 fetch:** 1,902/1,902 symbols, zero failures, raw cached (`raw_full*.jsonl`).
**Phase 3 (convention cells):** 3,781 of the 7,093 re-dated from BSE `filing_date_time`;
327 rejected by the 0-120d lag guard (years-later RE-UPLOADS), 244 no BSE row, 2,348 already
correct. 85.4% needed the 15:30 gate — they were deadline-day EVENING filings, which is exactly
why qe+21d looked plausible for so long.
**Phase 4 fetch:** the remaining 701 symbols, 701/701, zero failures.
**Phase 4 reconciliation (63,567 non-convention cells, user chose "both"):**
* **1,330 STALENESS HEALS** — the §104 NSE-lag class, now confirmed in the SHP series:
  median 9d, p95 133d, **max 337d (HINDALCO Sep-2019 stored 2020-09-21 vs a real 2019-10-18
  filing — 11 months invisible)**; NESTLEIND Jun-2023 323d, POWERGRID Sep-2020 295d.
* **24,688 GATE SHIFTS** — stored date IS BSE's raw broadcast date, filing after 15:30, so the
  store marked it visible the same session (a 1-day look-ahead). Ends the split convention.
* **LEFT ALONE, counted not guessed:** 1,659 genuinely-different BSE-later dates (cannot separate
  NSE-filed-first from stored-too-early with two sources), 2,012 revised-only, 4,628 no BSE row,
  76 lag-guard, 254 sub-materiality.

**DURABLE ROUTE (both phases):** patched `scripts/shp_history.json`'s sub slot — the accumulator
`build_engine_feed()` reads and whose slot the refine pass preserves — NOT `docs/shp_engine.json`,
which regenerates ~2x/day and would have reverted within hours while looking fixed.

**★ REGRESSION CAUGHT PRE-SHIP (and the reason to always four-way a guard):** `_cell_eq` compares
the sub STRING exactly, so advancing it silently skipped `shp_cell_fix` corrections — WARNs went
65 → 211. Fixed by advancing the ledger's own sub slot in lockstep (152 + 166 entries). Four-way
comparison (pristine/patched × pristine/advanced) proves the shipping state is back at the 65
pre-existing warnings: **zero regression**.

**★ IMPACT MEASURED, NOT ASSUMED — and it is SMALL.** DII strategy A/B with engine and
fundamentals held identical, only the SHP dates swapped: **CAGR 53.19 → 52.89, maxDD unchanged,
1 of 212 monthly baskets changed.** Reason: SHP filings cluster on ~qe+21 (mid-month) while the
strategy rebalances at month-END, so a 1-day shift rarely crosses a rebalance boundary and most
staleness heals move a date from one mid-month to another. The data is materially more correct;
the strategy barely moves. Recorded so nobody re-litigates this class expecting a big number —
contrast the membership + TTM fixes, which moved 52 baskets and cut maxDD 4.2pp.

**Residue, stated:** pre-2016 (25,867 cells) keeps the qe+21d convention with the earned negative
verdict (§105) and the 0e calibration (median real lag 17d, 96% within the deadline); the 1,659
ambiguous BSE-later cells; 350 symbols never fetched (no post-2016 non-convention cells).
