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

## P0-RESULTS (Opus fills in — measured, dated)
(empty — Phase 0 not yet run)
