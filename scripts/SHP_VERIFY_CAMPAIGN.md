# SHP VERIFY CAMPAIGN — our FII/DII holdings vs 5–7 external sites  (planned 2026-08-09)

**Mission:** prove (or fix) every FII/DII holdings value we publish, by comparing our per-stock
quarterly shareholding data against Moneycontrol, Trendlyne, and 3–5 more independent sites, across
every era we hold (Sep-2010 → Jun-2026), every category we store (prom/FII/DII/MF/insurance/nsh),
and both directions of coverage (cells they have that we lack, and vice versa).

**Non-goals:** market-level FII/DII *flows* (fii-dii.html — different dataset), the accumulation
backtest (§22c), promoter-pledge or named-shareholder detail (we don't store them).

Read FIRST: DATA_RUNBOOK §22 (pipeline + formats), §22b (old format + Sep-2022 boundary), §22f
(coverage audit), §57 (route ladder), §39 (ship gate). Scripts that already encode the parse/fetch
knowledge: `fetch_shareholding.py` (parse_shp, category members, sanity gates),
`fetch_shp_wayback_mc.py` (Moneycontrol slug/scId map + page grammar), `fetch_shp_bse_hist.py`
(BSE SHPQNewFormat route), `tl_reconcile.py` (Trendlyne fetch conventions),
`audit_shp_coverage.py` (point-in-time N500 denominator).

---

## 0. WHAT WE HOLD (measured 2026-08-09, origin/main @ 93de247c, shp_history blob b2bed157)

`scripts/shp_history.json`: **2,615 symbols, 66,477 (sym,QE) cells, 2010-09-30 → 2026-06-30.**
Row = `[prom, fii, dii, mf, ins, "sub-date", nsh?]`, percentages 2dp, FII and DII written
both-or-neither. Coverage vs point-in-time Nifty 500 (audit_shp_coverage.py, 95 quarters):

| era | member-qtrs | covered | source of our cells |
|---|---|---|---|
| Dec-2002..Jun-2010 | 15,473 | **0%** | none known (probe sites for this era — see P1.d) |
| Sep-2010..Sep-2015 | 10,449 | **30.4%** | **Wayback Moneycontrol** (`shp_fill_hist_2010_2016.json.gz`) |
| Dec-2015..Mar-2016 | 998 | **0.3%** | the seam — measured two-quarter wall (§22f) |
| Jun-2016..Jun-2019 | 6,502 | **99.9%** | BSE XBRL ledger (`shp_fill_hist_2016_2019.json.gz`) |
| Sep-2019..date | 14,012 | **100.0%** | live NSE XBRL pipeline + BSE sweep (`shp_fill_n500_gaps.json.gz`, 2026-08-09) |

Provenance layers that MATTER for circularity (T3): 2010-2015 cells came FROM Moneycontrol;
BSE Ltd's 2016-2021 cells came from screener.in+Trendlyne (`shp_fill_thirdparty.json.gz`, 21 cells);
everything else is exchange XBRL. Build the per-cell provenance map by membership in the fill
ledgers (fill files use the same `SYM.QE` keys; row slot 7 = src tag).

**Pin the snapshot:** every agent compares against `git show 93de247c:scripts/shp_history.json` —
never the checkout, never a later origin (the 12:40/20:40 IST refresh moves it daily).

---

## 1. GROUND RULES (every agent, every phase)

1. **Read-only** on all tracked data. The ONLY tracked files this campaign may eventually touch,
   in P6 and only via the worktree: `scripts/shp_cell_fix.json` (heals), the two campaign outputs
   (`scripts/shp_verify_ledger.json.gz`, `scripts/SHP_VERIFY_REPORT.md`), DATA_RUNBOOK §22h, and
   fill ledgers for any NEW cells found. NEVER edit `shp_history.json` directly (CI owns it).
2. **Category rows only.** Compare our fii/dii/prom/mf/ins against the site's PRINTED category rows
   — never against a remainder you derive (100−x), and never let the site's derived "public" stand
   in for a category (that screen produced the false positives in the N500 sweep).
3. **One (site,stock) page-load yields the whole table.** Extract EVERY quarter the site shows in
   one visit — the diff is then computed offline against all overlapping quarters. Do not re-visit
   per quarter.
4. **Evidence per extraction:** site, exact URL, retrieval timestamp IST, raw category labels AS
   PRINTED, raw values, and the site's stated as-on/quarter labels. JSONL, schema §8. A claim
   without a URL + raw row is not evidence.
5. **Rate limits:** ≥2 s between requests per site (Trendlyne ≥3 s), one agent per site, stop on
   403/429 and report (NSE 403 = lockdown, wait it out — runbook). **Never** bypass logins,
   captchas, or paywalls — a gated site is reported `inaccessible` and replaced from the alternate
   roster. Prohibited absolutely: creating accounts, entering credentials.
6. **Sites copy each other.** Majority-of-sites is NEVER arbitration evidence. Only exchange
   filings arbitrate (§6 ladder).
6b. **★ MULTI-SOURCE ACCEPTANCE (user mandate, 2026-08-09): no value is taken into our data
   unless it has been checked on MANY sites and they MATCH.** Concretely, a heal or new fill is
   accepted only when (a) it is parsed from the exchange filing itself AND agrees within ROUND
   tolerance with ≥2 independent sites, or (b) where no exchange route exists (pre-Jun-2016),
   ≥3 independent sites agree within tolerance — PROVENANCE_ECHO sites (T3) never count toward
   the quorum. Sites disagreeing among themselves → the value is NOT taken; the cell stays open
   with the disagreement recorded in the ledger.
7. Long/scripted work runs in its own worktree `~/stocks-wt/shp-verify` (CLAUDE.md rule 3);
   scratch output under the session scratchpad; commits file-scoped; push via the retry recipe.
7b. **★ NAMESPACE YOUR SCRATCH FILES (near-miss, P1 2026-08-09).** Parallel site agents share ONE
   scratchpad directory, so generic working filenames collide across siblings — in P1 the seven
   agents independently reached for `reliance.html`, `tcs.html`, `itc.html`, `hdfcbank.html`,
   `parse_tl.py`, `build_extract.py`. One collision was caught only because the Write tool refuses
   to overwrite a file it hasn't read (it surfaced an ET-Markets `build_extract.py` sitting where a
   Trendlyne one was about to land); **`curl -o` has no such guard** and would have silently
   clobbered a sibling's raw evidence with no error and no exit code. Treat P1 as a near-miss, not
   as proof the pattern is safe. So, from P3 on: every agent writes ALL intermediates — raw HTML,
   probe dumps, throwaway parsers, notes, logs — under its OWN subdirectory
   `p<phase>/<sitename>_work/` (e.g. `p3/trendlyne_work/reliance.html`), created at packet start.
   The shared root of `p<phase>/` is reserved for the final per-site deliverables, which are already
   unique by site name (`<sitename>.json`, `<sitename>_extract.jsonl`). Never `curl -o` — and never
   redirect `>` — into the shared root.
8. Comparisons are **as-on-date keyed**: our QE ↔ the site row whose as-on date equals that QE.
   A site row with a mid-quarter as-on date is an event-based SHP — we drop those BY DESIGN (§22
   step 1); record it as `EVENT_SHP`, not a mismatch.

### Tolerance bands (percentage points, per category)
- `MATCH` |Δ| ≤ 0.02 · `ROUND` |Δ| ≤ 0.06 (2dp rounding both sides)
- `INVESTIGATE` 0.06 < |Δ| ≤ 0.50 → explain via mapping card (T7) or escalate
- `MISMATCH` |Δ| > 0.50 → mandatory arbitration (§6). nsh: `MATCH` = exact; ±1% = ROUND.

### Verdict taxonomy (one per compared cell-field)
`MATCH · ROUND · DEF_DIFF (site defines the bucket differently — cite its sub-rows) ·
PROVENANCE_ECHO (match, but our cell CAME from this site — confirms transcription only) ·
EVENT_SHP · STALE_SITE (site shows the pre-revision filing; our sub-date is newer) ·
REVISION_MISS (site has a revision NEWER than our sub-date — actionable defect on us) ·
OURS_WRONG (arbitrated) · SITE_WRONG (arbitrated) · NO_DATA_SITE · NO_DATA_OURS (their cell,
our hole — fillable-coverage finding) · UNRESOLVED (must carry a named blocker)`

### Traps — read before judging ANY delta
- **T1 Sep-2022 format boundary.** Levels on each side are individually correct; the Jun→Sep-2022
  DELTA is a reclassification for DR-heavy stocks (INFY dii 18.87→32.38 = ADR look-through).
  Never verdict a site from a QoQ across the boundary; compare levels per quarter only.
- **T2 Old format (≤ Jun-2022): OtherInstitutions→DII is CALIBRATED** (§22b). A site that puts
  "other institutions" elsewhere will show systematic pre-2022 DII offsets (HEROMOTOCO ~14pp
  case). That's DEF_DIFF once its sub-rows prove it — not OURS_WRONG, and don't flip our flag.
- **T3 Circularity.** 2010-2015 vs Moneycontrol and BSE-Ltd-2016-21 vs screener/Trendlyne =
  PROVENANCE_ECHO at best. Independent verification of those eras must come from OTHER sites.
- **T4 Event SHPs** — see rule 8.
- **T5 Revisions.** Before any MISMATCH verdict, pull the NSE master's filing list for that
  (sym,QE): if >1 submission exists, decide STALE_SITE vs REVISION_MISS by sub-date, not by value.
- **T6** Some sites print 1dp or truncate — record the site's precision in its capability card and
  widen ROUND to half its last digit.
- **T7 Definitions differ.** "FII/FPI" on a site may exclude FDI-classified foreign holdings
  (ours = InstitutionsForeignMember = FPI I+II + FDI + other foreign); government/trust buckets
  roam. NO Phase-3 verdicts until the site's mapping card (P2) says which of its rows sum to our
  FII and DII.
- **T8 Public %** is derived on our pages — skip it, or reconstruct filed-public from the XBRL.
- **T9 Renames.** Filings live under the name-of-day; map via `_rename_map.json` / FUND_ALIAS.
  A site may key old+new history under one slug or split them — check both (ETERNAL ex-ZOMATO,
  GUJENERGY ex-GUJGASLTD).
- **T10** BSE Ltd (the stock) is NSE-only — absent from BSE-sourced sites; that's NO_DATA_SITE,
  not a defect anywhere.

---

## 2. SITE ROSTER

Primary 7 (the campaign): **Trendlyne · Moneycontrol · Screener.in · Tickertape · Groww ·
ET Markets (economictimes) · StockEdge (web.stockedge.com)**.
Alternates if one is inaccessible/login-walled: Value Research Online, IIFL/indiainfoline,
marketsmojo, 5paisa, INDmoney. Ground truth (not a "site"): NSE + BSE filings.

Known starting points (recon confirms/corrects):
- Moneycontrol: `/company-facts/<slug>/shareholding-pattern/<scId>` — slug/scId map builder
  already exists (`fetch_shp_wayback_mc.py map`).
- Trendlyne: per-stock "Shareholding" tab; ID conventions in `tl_reconcile.py` / `docs/tl_reconcile.json`.
- Screener.in: `/company/<NSE-sym>/` (BSE-only names via scripcode), quarterly shareholding table.
- Tickertape / Groww / ET / StockEdge: slug discovery via their own search endpoints (recon).

---

## 3. PHASE 1 — SITE RECON  (7 packets, SONNET, parallel — one agent per site)

Per site, deliver a **capability card** (JSON, schema §8) by loading 3 probe stocks
(RELIANCE, MCX, ETERNAL) + answering:
a. URL pattern + how to resolve OUR symbol → site ID (and bulk-resolvability for P4).
b. Which category rows exist (exact labels), their precision, and whether sub-rows (MF, insurance,
   FPI-vs-FDI, other-institutions) are shown. Screenshot or raw-HTML snippet as proof.
c. History depth: earliest quarter shown for RELIANCE and for MCX; are event-based (mid-quarter)
   patterns listed?
d. **Pre-2010 probe:** does it show ANY quarter ≤ Jun-2010 (RELIANCE/TCS/ITC/HDFCBANK)? If yes —
   flag loudly: that era is 0% for us and was believed sourceless (never-say-unfillable rule).
e. Machine-readability: JSON endpoint / parseable HTML / JS-rendered (needs browser)? Rate-limit
   behaviour observed. Login wall? (If hard-gated → report and stop; orchestrator promotes an alternate.)
f. Proof-of-parse: full extracted quarterly table for the 3 probes in the §8 extraction schema.

Done when: 7 cards (or documented inaccessibility + promoted alternates) + 21 proof extractions.

## 4. PHASE 2 — CALIBRATION PILOT  (1 packet, OPUS, after P1)

Pilot stocks (each exercises a trap): RELIANCE (baseline mega), TCS (high-prom), HDFCBANK
(no-promoter+ADR), INFY (T1 DR reclass), SBIN (govt promoter), M&M (employee-trust partition),
ETERNAL (no-prom + T9 rename), HEROMOTOCO (T2 other-institutions), MCX (28-qtr blackout healed by
the 2026-08-09 BSE sweep — fresh fills), BSE (T10, thirdparty provenance), NESTLEIND (sweep-healed
blackout), GAYAPROJ (event-jump small-cap).

For each pilot × each site: diff the FULL overlapping table (from P1-style extraction) against the
pinned snapshot, per category. Then, per site, produce the **mapping card**: which site rows ↔ our
fii/dii/mf/ins/prom, any systematic offset by era (pre/post Sep-2022, pre/post Jun-2016), the
tolerance to apply, and 3 worked examples. Cross-check 3 disputed cells directly against a fresh
NSE-XBRL re-parse (route in fetch_shareholding.py) so the mapping is anchored to filings, not to us.
Done when: 7 mapping cards + a pilot summary table (per site: match/round/def-diff/mismatch counts).
**Gate: no Phase 3 until every active site has a mapping card.**

## 5. PHASE 3 — STRATIFIED DEEP AUDIT  (7 packets, SONNET, parallel by site; ~60 stocks)

Strata (draw deterministically; freeze the list in the worktree as `strata.json` before starting,
selection rule = lowest md5(sym) within each stratum, so any agent can re-derive it):
- 10 mega + 10 mid + 10 small caps (mcap terciles of current N500 ∩ shp_history);
- 5 renamed tickers (from `_rename_map.json` with ≥8 quarters both sides);
- 5 wayback-era-rich (≥12 cells in `shp_fill_hist_2010_2016`);
- 5 BSE-ledger-era (cells only via `shp_fill_hist_2016_2019`);
- 6 from the 2026-08-09 sweep blackouts: MCX, ABBOTINDIA, BAYERCROP, NESTLEIND, WESTLIFE, ITC;
- 3 banks, 2 insurers, 2 NBFCs; 2 ADR/GDR (WIPRO, ICICIBANK); 2 recent IPOs (first filing);
- 2 index-exits still in history (site-coverage check).
Dedupe; target 55–65. Every stratum member × every site × every overlapping quarter, per the
site's mapping card. Also record NO_DATA_OURS rows (their quarters we lack) — that is the
coverage-comparison half of the mission.
Output: one JSONL per site (§8) at the shared root as `p3/<sitename>_extract.jsonl`; everything else
the agent writes goes under `p3/<sitename>_work/` (rule 7b). Done when: every (site ∈ ≥5, stock,
overlapping QE) has a verdict row, and inaccessible cells carry NO_DATA_* or a blocker.

## 6. PHASE 4 — BULK SWEEP  (1–2 packets, SONNET, only sites P1 rated machine-readable)

On the 1–2 sites with a cheap JSON/HTML route (expected: Trendlyne and/or Moneycontrol — the MC
qtrid grammar is already known): sweep the FULL overlap — all shp_history symbols resolvable on
the site, all quarters — via a deterministic script (written + committed in the worktree),
resumable (append-only JSONL keyed `site|sym`, skip done) — the resume file at `p4/<sitename>.jsonl`,
its page cache and scratch under `p4/<sitename>_work/` (rule 7b). Diff offline. This is where "compare
every single thing" is literal: potentially 40–60k cells/site. Respect rate limits (≈6–8 h at
2 s/page — run as ONE background agent per site, never parallelize one site).

## 7. PHASE 5+6 — ARBITRATION, HEAL, REPORT  (OPUS, sequential)

**P5 Arbitrate** every MISMATCH and every INVESTIGATE the mapping card can't absorb, one at a
time, up the ladder — never by site majority:
1. NSE corporate-share-holdings-master row(s) for (sym,QE) → re-parse the XBRL (T5 first: list
   ALL submissions, newest wins).
2. BSE `SHPQNewFormat` XBRL (gate on non-empty `XbrlFile` — `xbrlurl` lies, §22f).
3. The filing itself on the exchange page (StockShareholding.aspx / annexure PDF).
4. Pre-2016 only: Wayback captures of EXCHANGE pages (MC captures are provenance, not truth).
Verdict + evidence per cell into the final ledger. UNRESOLVED requires the walked-ladder record.

**P6 Heal + report:**
- Every heal/fill must first pass rule 6b (exchange filing + multi-site quorum) — the `why` field
  of each entry lists the corroborating sites and their values.
- OURS_WRONG → entry in `scripts/shp_cell_fix.json` (cell/was/src/why — the GHCL pattern),
  REVISION_MISS → targeted `--symbols` refetch; NO_DATA_OURS clusters → a fill ledger + merge via
  the established route. Then `fetch_shareholding.py --feed-only` rebuild, §39 gate, push, verify
  LIVE ~20 min later (stock.html SHP table + shareholding.html for 3 healed names).
- Deliverables: `scripts/shp_verify_ledger.json.gz` (every compared cell), 
  `scripts/SHP_VERIFY_REPORT.md` (per-site scorecard: match rate, systematic offsets, history
  depth vs ours, coverage each direction; our defect list + heals; pre-2010 probe outcome),
  runbook §22h summary, memory update.

**Campaign done when:** every strata cell diffed on ≥5 sites; every |Δ|>0.5pp arbitrated to a
named verdict; bulk-sweep deltas triaged (clusters explained, singletons arbitrated or queued);
zero silent UNRESOLVED; heals live-verified; report published.

---

## 8. SCHEMAS

Extraction row (JSONL): `{"site","sym","site_id","url","fetched","asof","rows":{"<site label>":
value,…},"precision":2,"event":false}` — labels AS PRINTED, values as shown (no remapping at
capture time; mapping happens at diff time so raw evidence survives).
Verdict row: `{"sym","qe","field","ours","site","site_val","delta_pp","verdict","evidence_url",
"note","prov"}` where prov ∈ nse-live|bse-1619|wayback-mc|bse-sweep|thirdparty|nse-gaps.
Capability card: `{"site","resolve","categories":{label:maps_to},"precision","depth_reliance",
"depth_probe_pre2010","event_rows","machine_readable","rate","access","notes"}`.

## 9. EXECUTION MATRIX

| phase | agents | model | parallel? | est |
|---|---|---|---|---|
| P1 recon | 7 (1/site) | Sonnet | yes, across sites | ~40 min each |
| P2 calibration | 1 | Opus | after ALL P1 | ~1–1.5 h |
| P3 stratified | 7 (1/site) | Sonnet | yes, across sites | ~1.5–2 h each |
| P4 bulk | 1–2 | Sonnet | background, 1/site | ~6–8 h each |
| P5 arbitration | 1 | Opus | sequential | ~2 h |
| P6 heal+report | 1 | Opus | sequential | ~1–1.5 h |

Orchestrator (any model) holds: the pinned SHA, the strata freeze, P1→P2 gate, alternate-site
promotion, and merges JSONLs. Agents write ONLY scratchpad/worktree files until P6.
