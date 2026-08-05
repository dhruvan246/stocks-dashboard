# PRE-2015 CAMPAIGN — rev + PAT 2002→2014 for every N500 ever-member, + membership 2002-2015
_Designed 2026-08-04 (Fable session; every source claim below was PROBED live that day, not assumed).
Execution model: **Sonnet sessions run one step at a time, `/clear` between steps.** Each step is
self-contained. The judgment-heavy design is already done here — steps are recipes, not research._

Mission (user): survivorship-free rev+PAT for every stock that was in the Nifty 500 for even a
single day, 2002→2015, filled by the same rule book as the completed 2015→2026 mission
(runbook §42/§43/§44/§45/§48), **negligible-token routes first** (structured JSON/HTML, no vision,
no PDFs unless a step says so). And: per-date N500 membership must be correct 2002→2015.

## STATE OF THE WORLD (measured 2026-08-04 — re-verify only if something contradicts it)

* Scope = **member-quarter STANDALONE cells, 2002Q1→2014Q4: 26,022 cells; 727 stored; ~25,300 open.**
  839 distinct ever-members in-window (2008-14 quarters: 708 · 2005-07: 552 · 2002-04: 566).
  CONSOLIDATED quarterly was OPTIONAL pre-2015 (SEBI Clause 41) — con blanks are NOT gaps, never
  grind them; land con only when a source explicitly serves a Consolidated row.
* Stored pre-2015 today: 2012-14 ≈ 218-252 rows/yr (the old PAT-only agent batch, real ann dates,
  e.g. BAYERCROP [20120930, 97.85, 20121031, 97.85, 20121031]); 2010-11 ≈ 18-19; zero before.
* Membership authority (`docs/stock_data.bin` → indicesHistory["Nifty 500"], runbook §48) has only
  4 snapshots before 2015 (2006-11-08, 2010-01-02, 2014-01-22, 2014-07-09) and **NOTHING before
  Nov-2006 — membership(2002..2006-11) is an EMPTY SET today.** Fix = STEP M, run it FIRST.

## THE SOURCE MAP (all probed 2026-08-04)

| era | primary route | proof | notes |
|---|---|---|---|
| 2008Q1–2014Q4 | **BSE detres JSON** (runbook §42): `Corp_detailedResult_Transpose_ng/w?scrip_cd=<code>&qtr=<QID>.00`, QID = 85+4×(FY−2015)+{Mar:0,Jun:1,Sep:2,Dec:3} → Mar-2008 = **57** | RELIANCE/SBIN/HINDALCO/SATYAMCOMP/KEMROCK/3IINFOTECH/HFCL all full P&L at qid 57-81; SBIN Mar-08 NP 18832.5 ⇒ 1,883.25cr ✓ public record | ₹ MILLION ÷10. Serves delisted+suspended. `.50` audited annual works **FY08+** (RELIANCE FY08 19,458cr exact ✓). EPS+Equity Capital+Face Value rows present ⇒ EPS-recon gate live. **Below qid 57 (pre-Mar-2008): 3-row stub** (Type/Date Begin/Date End only) — the registry knows the quarter, has no financials. `.50` also stubs below FY08. |
| 2005Q1–2007Q4 | **NSE archive HTML** (memory `feedback-nse-archive-first`): `corporates-financial-results?symbol=X&period=Quarterly` + `resultDetailedDataLink` → `financial_res_<SYM>_<id>.html` | RELIANCE/INFY/ACC lists all floor at **31-Mar-2005**; SATYAMCOMP serves 54 rows 2005-2013 (delisted OK); HINDALCO Mar-10 page rev 5,358.46cr ✓ matches detres+public | Page is **single-column (current quarter only) — NO comparative column.** Declares: `Amount(Rs. in lakhs)`, Banking/Non-Banking, Audited, **Consolidated/Non-Consolidated, Cumulative/Non-cumulative**, Relating-to quarter. `period=Annual` rows exist back to **FY05 (Audited)** with detail links ⇒ FY-identity gate is self-contained in-source. `filingDate`/`broadCastDate` are null pre-2018 ⇒ no real ann dates. Not all delisted served (BHARTISHIP = 0 rows → detres covers 2008+ only; pre-2008 those are STEP-W residue). |
| 2002Q1–2004Q4 | **NO structured exchange source exists** (both probed dead: detres stub, NSE floor 2005) | — | STEP W probes the archived web (candidates + CDX seeds inside). Honest possibility of a permanent partial floor here. |

Membership raw material, all parsed and force-tracked already: `scripts/_n500_pre2008_lists.json`
(**21 official NSE full lists 2002-10-02 → 2006-07-14, each 498-501 era-symbols**; the 20020815
capture is a 0-row partial — ignore it), `scripts/_n500_mc_parsed.json` (74 Moneycontrol full-list
captures incl. 2007-10/2008-05/2008-10/2008-12/2009-05/2009-09/2009-12 and 14 caps 2011-13),
`scripts/_full_union_2002.json` (1,318 ever-member union, current symbols).

## GROUND RULES (inherit COVERAGE_CAMPAIGN.md ground rules + these)

1. Concurrency: runbook §38. Every step runs in ITS OWN worktree off origin/main
   (`git worktree add --detach C:/Users/dhruv/stocks-wt/pre2015-<step> origin/main`). Never mutate
   the shared checkout. Stage explicit paths only. Local main in the shared checkout has diverged
   (71+ commits ahead of origin) — do not "fix" that; just never push from there.
2. **Commit tracked script/ledger edits BEFORE the first batch push** (memory
   `feedback-reset-replay-hits-tracked-scripts` — batch_push's reset eats uncommitted edits).
3. Token rule: these steps are SCRIPT-ONLY. No vision, no OCR, no per-cell model reads. If a step
   seems to need them, STOP and report — that becomes a separate user decision.
4. One writer at a time on sf_fundamentals/sf_revop ledgers (memory
   `feedback-backfill-one-agent-at-a-time`) — never parallel agent swarms on the same files.
5. Every landed cell carries provenance in a TRACKED ledger (memory
   `feedback-provenance-every-backfill`): source route, doc id (qid+scrip / nse seq id), gate that
   passed, and the ann-date basis (see LANDING RULES 6).
6. After any batch reaches origin: re-verify LIVE ~20 min later (CI race, runbook rule 5);
   results_season/quarterly_results rebuild is nightly-CI'd — confirm next morning (§11).
7. Negative-base growth, owners-attributable PAT, NBFC-ORFO, XBRL-scale ledgers, rename handling:
   all standing rules apply unchanged (see MEMORY index).

## LANDING RULES — the anchor problem, solved for an era with (mostly) no stored values

The 2015+ discipline ("page PAT must match stored PAT") has nothing to match pre-2015. A cell may
land ONLY through one of these gates, recorded per-cell as `gate`:

* **GATE X — cross-source:** detres PAT (÷10) and NSE-page PAT agree within max(0.05cr, 0.5%)
  AND rev agrees within 0.5% → land rev/op/PAT from the better-structured read. Two independent
  renditions of the same filing = the strongest proof. Default for 2008-14.
* **GATE F — FY quarter-sum identity (runbook §45), single-source:** the 4 same-source quarters
  sum to the same-source AUDITED annual within max(3cr, 3%) → land all four. detres-vs-detres
  works FY09-FY15; NSE-vs-NSE works FY06-FY08 (annual rows reach FY05). §45 caveats are LAW:
  year-end changes (PFOCUS 9-month FY, EICHERMOT 15-month), Dec-year filers (annual sits on the
  Dec quarter), and **compensating errors pass the identity** — when any leg is a derivation, at
  least one leg must also be a direct document read.
* **GATE E — EPS-recon:** |EPS × (Equity Capital / Face Value) − PAT| ≤ 6% (both from the SAME
  page/response) → corroborates a single-source read where no annual brackets it (series edges,
  new listings). Weakest gate — use only when X and F are impossible, and prefer refusing rev if
  the rev row is ambiguous.
* **GATE S — stored-anchor (unchanged 2015 rule):** where a stored cell already exists (2010-14
  partials, FY15 boundary quarters), it is an anchor: agreeing read → cell already done; disagreeing
  read → do NOT overwrite; run §45 adjudication; heal only via pat_defects with double evidence.
* Mandatory on every landing regardless of gate:
  1. **Date-span check**: detres Date Begin/End == 3 months; NSE `Cumulative == Non-cumulative`.
     A Cumulative row may ONLY be used by differencing two consecutive cumulative reads of the
     same FY (provenance `derived:"cumdiff"`), and then needs GATE F over the FY.
  2. **Unit disambiguation by arithmetic, never assumption**: try {crore, ÷10, ÷100}; the accepted
     scale must be the one that passes the gate (EPS-recon catches 100× instantly). Declared-unit
     ("Rs. in lakhs") still gets verified, not trusted (memory: the lakh unlock).
  3. **Basis**: land into std slots from Non-Consolidated/detres reads. A declared Consolidated
     NSE row lands into con as bonus under the same gates. Never mix bases inside one FY identity.
  4. Bank rows: rev = Interest Earned; op = printed "Operating Profit Before Provisions"; NBFC op
     stays None (never the pre-other-income line); any-cell financial-format classification, not
     ticker regex.
  5. **Refusals are recorded** in the step's `_*_attempted.json` (the re-grind lesson) with the
     failed gate + evidence; a refusal class >20 cells gets a diagnosis line in the step report.
  6. **ann dates: NO source in this era carries filing dates.** Store ann = QE + 45 days (the
     era's Clause-41 filing deadline) and flag `ann_approx: true` in the LEDGER (the SHP-backfill
     precedent: approximate visibility explicitly flagged, never silently fabricated, never 0 —
     backtests need a visibility date and QE+45 is the conservative era rule). Where a stored
     2012-14 cell already has a real ann date, never touch it.
* Poison defences stay armed: year-shift detector (PAT(q)==PAT(q+10000)) re-run after every
  1,000 landings and at step end vs `yshift_genuine.json`; revop_sanity.py per-slot after every
  batch; sanity_ok.json for proven real spikes; NEVER Screener annual derivation (poison,
  documented §42); zero-rev is legal (mining-suspension precedent).

## STEP M — MEMBERSHIP 2002→2015 into the authority bin  ★ RUN FIRST — everything else keys off it

Goal: indicesHistory["Nifty 500"] gains ~25+ snapshots covering 2002-10-02 → 2015, sizes ~500,
correct arcs for era corp-actions. The audits and the backtest then share one truth (§48).

1. Worktree `pre2015-member`. Files in play: `scripts/_n500_pre2008_lists.json` (hard),
   `scripts/_n500_mc_parsed.json` (soft), `scripts/indices_history.json`,
   `scripts/build_membership_v2.py` (+ its checkpoint inputs — read its main() first),
   `scripts/build_n500_membership.py` (the bin splicer; pre-2018 "keeps the old scrapbook" —
   the scrapbook is exactly what we're extending), `scripts/_rename_map.json`, `scripts/symchg.csv`.
2. **Symbol space**: bin symbols. Resolve era symbols through the rename chain (both directions);
   a symbol that never renamed and simply died stays era-keyed (RANBAXY convention — matches how
   sf data keys dead series). Resolution report: every unresolved-to-a-known-sf-symbol name listed;
   **NO fuzzy matching, ever** (the BAJAJELEC≠BAJAJCORP lesson). Expect near-total resolution for
   the official lists (they carry real NSE symbols).
3. Hard checkpoints: the 21 official lists (drop 20020815). Soft snapshots: MC captures dated
   2007-2013 ONLY (the 2006-11-08 → 2014-01-22 dark window), resolved via the slug/code map
   (`_idx_resolve.py` + `_idx_codemap.json` — slug-based, proven safe; display names are NOT safe),
   suspended/dual rows dropped. QA per MC snapshot before acceptance:
   * size 480-520 after filtering (outside → reject the capture, it's a partial/jittery render);
   * Jaccard ≥ 0.90 vs nearest official checkpoint on the same side;
   * era arcs: RPL ∈ {2008-05…2009-09} ∉ 2009-12+; SATYAMCOMP present through 2013;
     YESBANK present 2005+ (Nifty-50 member — if absent, resolution broke).
4. Integration = THE PIPELINE, not the output file (the stash-lie lesson, 2026-07-28): add the
   snapshots to the checkpoint structure build_membership_v2.py/build_n500_membership.py read,
   force-`git add` every input (gitignore `_*` traps them), rebuild indices_history.json →
   rebuild/splice stock_data.bin exactly the way the weekly refresh does, so
   `refresh-membership.yml` reproduces instead of reverting.
5. STOP-GATES: any snapshot <495 or >510 post-resolution; MC-vs-official Jaccard <0.90;
   >15 unresolved names on any official list. Hit one → report, don't ship.
6. Verify: `_n500_member_bin.membership(20031231)` returns ~500 (today: 0); spot 20080630 /
   20120630 sensible; `verify_sizes.py` extended to 2002 (tolerance: era reviews were semi-annual,
   sizes 498-501); membership(20140101) unchanged vs today (no regression 2014+).
7. Push (worktree recipe), CI republish, LIVE-verify the bin per §41. Update memory
   `project-stocks-n500-membership-history` + progress log here.
   ⚠️ stock_data.bin is also written by daily CI — rebase-race expected; the §38 rule-4 loop handles it.

### STEP M — status + M2 measured findings (2026-08-04, Fable session)

**M1 SHIPPED** (commit 6243a22f): the 21 official 2002-06 lists are in `_wb_n500_snaps.json`,
rebuild verified additive-only (all 81 prior snapshots byte-identical, +21 new, earliest
2002-10-02), CI `refresh-membership.yml` dispatched to regenerate indices_history + the bin.
The N500-copy placeholder side effect (derived MSC400/SC250 rows at dates where Nifty-100 had no
history, incl. the old 2006/2010/2014 junk) was FIXED same-day in a465eb09: `_derive` now skips
dates where any subtrahend index is empty, so the derived tiers simply have no snapshot before
their basis exists (MSC400 starts 2015-09-28, SC250 2016-09-30; asof = empty before that).

**M2 (MC captures for the 2007-09 + 2011-13 dark windows) — machinery built and MEASURED, not
shipped.** Parser handles both markup eras (`pricechart.php?sc_did=<CODE>` pre-2011,
`stockpricequote/<ind>/<slug>/<CODE>` 2011+); resolution via `_idx_codemap.json` (983 codes,
AO04→ABAN class). Measured on all 26 target captures (harness: session scratchpad
`mc_m2_resolve.py` + `mc_m2_out.json`):
* 2010-13 captures: 475-500 rows, only **5-16 unresolved codes** each — close to shippable.
* 2007-09 captures: **20-83 unresolved codes** each, and the unresolved are precisely the
  DEAD/renamed era names (RPL, Satyam-era code, Bajaj Auto pre-split 'BA', Arvind Mills 'AM',
  Bank of Rajasthan 'BR', Adlabs 'AF27', Aztecsoft 'A13'…). ⚠️ Shipping codemap-only resolution
  would silently DROP the dead = inject survivorship bias into membership — the exact opposite
  of the mission. DO NOT ship M2 until the residual codes are resolved.
* ✅ **M2 SHIPPED same day (2026-08-04, Fable).** All 117 residual codes adjudicated: 83 by
  prefix-unique match vs official era name tables (`_n500_rawcsv` CSVs + EQUITY_L + pre-2008 htm
  names), 16 ambiguous + 18 no-match by era-bhavcopy trading evidence, 10 DROPPED with evidence
  (dead/merged/BSE-only at capture: IBP, IPCL, UWB, BGF, D-Link, FCL, Narmada Chematur, Wellwin,
  Williamson-Tea-dup, iGATE). **Then the deeper trap surfaced: the codemap itself emits
  CROSS-ERA symbols** (TM03 "Tata Motors"→TMCV the 2024 CV spin-off, AI54 "Alok"→ALOKINDS,
  AP26 "Aventis"→SANOFI, ~45 codes) — silent wrong-era members. Cure = an ERA-UNIVERSE
  re-resolution pass: any resolved symbol not trading in the era bhavcopy near the capture date
  is re-resolved from its display name, gated on actually-trading-then, with an official-bracket
  tiebreak (the Tata Motors DVR twin has an IDENTICAL company name) + a tiny MANUAL_ERA map
  (GUJFLUORO typo, EMERCK→MERCK, TV-18 hyphen, ZUARIAGRO). MC omissions healed by public record
  (SATYAMCOMP absent from Dec-2011/2012 renders though listed till the Jun-2013 TechM merger).
  Thin 2007-08 renders (<490) get official-bracket fill; identical consecutive cached renders
  deduped. **Result: 18 checkpoints 2007-10-14 → 2013-11-29, sizes 489-508, arcs exact (RPL,
  Satyam, Bank-of-Rajasthan, pre-split Bajaj), orphan rate 0.2-0.4% == the official lists' own,
  phantom drops 5 slots (3 smallcaps, status-quo outcome).** Files: `scripts/_mc_n500_snaps.json`
  (checkpoints) + `scripts/_mc_code_supplement.json` (full provenance: supplement map, drops,
  era-overrides, manual adds), merged into cps in build_membership_v2.py (N500 only), all
  force-tracked. Also healed: literal 'Symbol' header-row artifact in the 2006/2010 official wb
  lists. Harness: session scratchpad `mc_compose.py`. N500 now 120 snapshots 2002-10-02→date.

## STEP G — CUT THE GAP UNIVERSE  (30 min, after M)

From the rebuilt membership: for each quarter 2002Q1→2014Q4, members = `_n500_member_bin`
membership(qe); cells = member-quarters minus stored (sym,qe) std rows. Emit
`scripts/_gaps_0214.json`: per cell {sym, qe, bse_code, nse_sym_era_chain, era}. BSE codes:
`bse_scrips.json` by_id direct → rename-chain variants → `_bse_master_all.json`+`_scrip_extra.json`
NAME-match (the UNITEDBNK-disease recipe; measured today: direct-only resolves 583/708 of the
2008-14 union — the name-match pass must close most of the rest; GLOBOFFS/GTOFFSHORE-class old-name
gotchas are documented in memory `project-stocks-rev-2015-extension`). Report reachability per era.
Commit the gap file + the cutter script.

### STEP G — status (2026-08-04, Sonnet): SHIPPED

`scripts/cut_gaps_0214.py` (new, worktree `pre2015-gaps`) walks true per-quarter bin
membership via `_n500_member_bin.membership(qe)` over all 52 quarters 2002Q1-2014Q4 and
subtracts cells that already have BOTH std PAT (`docs/sf_fundamentals.json`) AND std rev
(`docs/sf_revop.json`) — dual-form gaps (`need:["rev"]`, PAT already stored by the old
2012-14 batch) are tagged separately from full gaps (`need:["pat","rev"]`) so STEP D/N don't
re-fetch what's already there. `_n500_member_bin.py` was untracked and only existed in
worktree `rev-mission`; copied in, relativized (no more hard-coded cross-worktree path to
the old checkout's `_rename_map.json`), and committed here as a shared asset — same for
`_bse_master_all.json` (10,786-row BSE master) and `_scrip_extra.json` (the delisted-code
supplement, grown 82→198 entries this run).

**Numbers:** 24,421 real member-quarter cells, only **1** fully stored, **24,420 open**
(744 of them dual-form: PAT already there, rev missing). Ever-members measured per era:
2008-14 **727** (doc baseline 708) / 2005-07 **574** (doc baseline 552) / 2002-04 **566**
(exact match). Both deltas are explained, not bugs: `membership()` returns EMPTY for
20020331/20020630/20020930 (no snapshot exists before the earliest surviving capture,
2002-10-02) rather than extrapolating backward — accounts for ~1,600 of the 26,022→24,421
total-cell delta; the small 2008-14/2005-07 member-count deltas most likely reflect the
M2 build settling between the doc's mid-build measurement and the final shipped 120-snapshot
bin. Re-verify only if it matters downstream.

**BSE-code reachability** (companies with ≥1 open cell in that era; layers = by_id/scrip_extra
direct → rename-chain variants (both directions) → exact `scrip_id` match against the full
`_bse_master_all.json` → normalized company-name match sourced from `symchg.csv` — no fuzzy
matching anywhere, ambiguous/absent stays unresolved and reported):

| era | resolved | universe | % |
|---|---|---|---|
| 2008-14 | 691 | 727 | 95.0% |
| 2005-07 | 508 | 574 | 88.5% |
| 2002-04 | 463 | 566 | 81.8% |

Overall: **749/875 (85.6%)** unique symbols-with-gaps resolved. 116 new codes recovered
this run (scrip_id-exact or symchg-name layer) and folded into `_scrip_extra.json` —
spot-verified ~20 against `_bse_master_all.json` Scrip_Name (RANBAXY/HDFC/MINDTREE/PATNI/
5 PSU banks/RPL/RNRL/LML/WYETH/NIRMA/PEL/JPASSOCIAT/COX&KINGS/…), all correct, zero false
positives found.

**Residual: 126 unique unresolved symbols, four named classes (not a blob):**
1. **BSE mnemonic diverges from the NSE symbol by more than a clean suffix** — CASTROL
   (real scrip_id `CASTROLIND`), CEAT (`CEATLTD`), 3IINFOTECH (`3IINFOLTD`). Resolvable with
   a deliberate suffix-normalization pass (strip IND/LTD/CO both sides, require the result
   still unique) — left for a dedicated follow-up rather than bolted on here.
2. **Genuinely ambiguous multi-entity groups** — TV-18 has FOUR BSE candidates
   (TV18/TV18BRDCST/TV18BRDCSTR/TV18EQPP); Television Eighteen India Ltd and TV18 Broadcast
   Ltd are DIFFERENT legal entities (the TM03-Tata-Motors-vs-TMCV class from STEP M2's own
   writeup) — needs historical adjudication of which entity traded under that symbol when,
   not a guess.
3. **Genuinely absent from `_bse_master_all.json`** — SATYAMCOMP, zero hits on any field.
   Public record BSE code is 500376 (Mahindra Satyam, merged into Tech Mahindra Jul-2013 —
   already a `manual_adds` entry in `_mc_code_supplement.json` for the identical reason on
   the membership side) — deliberately NOT inserted here since it can't be verified against
   any in-repo source; needs a manual, evidenced add to `_scrip_extra.json` before STEP D
   runs, same precedent as the membership-side manual_adds.
4. **Likely membership-bin identity splits — a STEP M follow-up, not a STEP G bug**:
   COLGATE has no `_rename_map.json` entry to COLPAL (which itself resolves cleanly, BSE
   500830) — the M2 `era_overrides` entry for MC code `CPI` reads
   `{"from":"COLPAL","to":"COLGATE"}`, possibly backwards; BILT never links to BALLARPUR
   (already resolved, BSE 500102) despite BILT being Ballarpur Industries' common market
   name. Both look like the same real company appearing under two unlinked bin identities —
   worth a `_rename_map.json` pass before the audit denominator is trusted further.

Next: STEP D (BSE detres harvest, 2008-14, the ~14k-cell big one).

## STEP D — DETRES HARVEST 2008Q1→2014Q4  (the big one, ~14k cells; pure python, hours)

1. Worktree `pre2015-detres`. Extend `_bse_detres.py` (or a sibling `_bse_detres_pre15.py`) with:
   qid range 57-84 + `.50` annuals FY08-FY15, the pre-2015 GATE logic (X/F/E instead of
   stored-anchor), attempts file, and the ann=QE+45 approx rule. The parser needs NO changes
   (fields verified identical at qid 57-81, banks included).
2. Fetch order: per company, all quarters + annuals in one pass (FY-identity needs the set);
   cache responses (the existing `_bsedet_cache` pattern) — re-runs must be free.
3. Land through the gates into a NEW tracked ledger `scripts/pre2015_reads_d.json` (shape:
   `_b3_reads` + `gate` + `ann_approx` + `qid`/`scrip`), applied by `_apply_reads.py` with a
   `pre2015` mode: cells with no stored value skip the stored-anchor check (they CAN'T pass it);
   cells with a stored value use GATE S unchanged. Row creation must fill BOTH files
   (pat_std form AND std rev/op — the dual-form lesson, §45 round-2 block).
4. NSE cross-reads for GATE X come from STEP N's cache when it exists; on the first run, GATE F
   (detres 4-quarters vs .50 annual) is expected to carry most of the era alone. A company whose
   FY fails the identity → all 4 cells refused + recorded; the refusal classes get triaged at
   step end (expect: year-end changes, basis mixes, genuinely-unfiled suspended quarters →
   probe announcement windows, grave with evidence into never_filed.json — phantom-member cells
   should be ~zero because STEP M ran first).
5. Batch cycle every ~500 landed cells: commit ledger → batch_push → revop_sanity → yshift scan.
   LIVE-verify 3 spot cells/era vs public record (e.g. SBIN Mar-08 1,883.25; HINDALCO Mar-10
   663.92; SATYAMCOMP Sep-08 597.43 — all pre-verified today).

### STEP D — status (2026-08-04, Sonnet): SHIPPED

`scripts/_bse_detres_pre15.py` (new, untracked scratch alongside `_bse_detres.py`,
worktree `pre2015-detres`) + `_apply_reads.py --pre2015` mode (tracked, isolated
code path — the default 2015+ mode is untouched). Fetch order per company: all
qids 54-85 + annuals 57.50-85.50 in one pass (a small bounded prefetch pool, 4
workers, warms the cache ~8 companies ahead of the sequential gate loop — the
gate/land logic itself stays fully sequential and unchanged by this).

**Numbers:** 727 companies in scope, 691 with a resolvable BSE code (unchanged
STEP G residual — 36 unresolved, no fuzzy resolution attempted). Of 13,925
2008-14 gap cells: **11,071 landed** (GATE F 8,996 · GATE S 732 · GATE E 1,343),
**2,541 refused** (recorded with reasons in `pre2015_attempted_d.json`), 313 cells
belong to unresolved-code companies. Refusal classes: 785 non-standard fiscal
year (annual span ≠ ~365d — year-end changes, caught by the date-span check and
correctly refused rather than mis-landed); 562 no filing in detres for that
quarter; 183 genuine FY quarter-sum/annual mismatches neither gate could close;
35 quarter-date mismatches; 4 irregular filings; 1 gate-S disagreement flagged
for hand Sec.45 adjudication (not auto-healed). GATE X stayed stubbed all run —
STEP N hasn't shipped yet.

**Correctness fix mid-run:** GATE F originally included an alternate-scale
"rescue" (try x10/÷10 on whichever leg made a failing FY-sum close). The yshift
poison scan caught its output on GAMMONIND (two different quarters both forced to
the same value). Root-caused and removed — the rescue had no independent
evidence, it searched a 2-candidate space per leg until one satisfied the SAME
constraint being tested (circular, not proof — exactly the "compensating errors
pass the identity" trap these LANDING RULES already name). 28 cells across 6
companies (AKSHOPTFBR, BEPL, DHAMPURSUG, ESSAROIL, FKONCO, GAMMONIND) were
retracted and re-derived under the fixed code: 19/28 re-landed cleanly via GATE E
on their original unmodified readings (proving those were right all along), 9/28
now correctly refuse. 5 of the 6 companies had already been pushed before the fix
landed; that commit corrected the live data. Full detail + both git commits in
the session that shipped this.

**Sanity pass:** revop_sanity flagged 90 cells across this run (12 companies) —
all reviewed against raw cache before any allowlist decision, none blind. 84
confirmed genuine and allowlisted in `sanity_ok.json`: a "scale-spike" class
(established-max reference reflects a company that collapsed AFTER 2015 —
ABGSHIP/EDUCOMP/GTL/OPTOCIRCUI/PSL/REIAGROLTD/ALEMBICLTD, several confirmed by a
seamless bridge into their own pre-existing post-2015 stored series, no
discontinuity) and a "duplicate-value" class (ANDHRSUGAR/GEOMETRIC/SONATSOFTW,
raw precision below 2dp display rounding proves the two filings are genuinely
different documents). 6 cells across 3 companies (MASTEK/PEL/SUZLON) deliberately
LEFT nulled: their revenue matches exactly (not just at 2dp) across large time
gaps while every other field on the filing differs substantially — a materially
more ambiguous signal than the other two classes, and LANDING RULES prefers
refusing ambiguous revenue over forcing it. PAT for those 6 is unaffected either
way (revop_sanity never touches the PAT mirror slot). yshift scan clean across
the full campaign (11,071 cells / 660 symbols) after 2 genuine-coincidence pairs
(PARSVNATH, SOBHA) were investigated and allowlisted in `yshift_genuine.json`
with full reasoning.

**LIVE-verified through the client** (not just the raw bulk JSON — the per-stock
`fin/<SYM>.json` slice the stock page actually fetches): HINDALCO Mar-10 663.92
(gate F) and SATYAMCOMP Sep-08 597.43 (gate E) both confirmed exact. SBIN Mar-08
1,883.25 (confirmed correct by direct fetch, matches public record) is correctly
REFUSED — FY08's Q1 (Jun-07) is pre-detres-floor so GATE F can't close, and GATE
E misses by 16% on this specific bank filing (reported quarterly EPS doesn't
reconcile against period-end equity capital for reasons unrelated to scale).
Expected to close once STEP N supplies GATE X.

Next: STEP N (NSE archive, 2005-07 + 2008-14 residue) unlocks GATE X, which
should close some of STEP D's 2,541 refusals — particularly the FY08-boundary
class and cells where the FY-sum genuinely can't close on detres alone.

## STEP N — NSE ARCHIVE HARVEST 2005Q1→2007Q4 + 2008-14 residue

1. Worktree `pre2015-nsearch`. `_nse_archive_revop.py` already handles list/detail/cache/rename
   chains/&-encoding/≤2-workers (all four traps documented in memory `feedback-nse-archive-first`).
   Extend: period=Annual fetch (FY05+), the GATE logic, cumulative-row differencing, ann rule.
2. Universe: (a) all 2005-07 member-quarters; (b) every 2008-14 cell STEP D refused or couldn't
   reach (no BSE code, detres-empty). GATE X wherever detres also served; else F (NSE annuals);
   E for edges. J&KBANK pre-2018 is a proven dead end (archiver &-bug) — straight to residue.
3. Same ledger pattern: `scripts/pre2015_reads_n.json`, same applier mode, same batch cycle.
4. Step end: rebuild the member-month audit table for 2005-14; residue classes named per cell
   (no-nse-rows, gate-fail, cumulative-unresolvable, dead-both-sources → announcement-window
   probe → grave with evidence).

### STEP N — status (2026-08-04, Sonnet): SHIPPED

`scripts/_nse_archive_pre15.py` + `_pre15_stepn_driver.py` + `_pre15_stepn_report.py` (new,
untracked scratch, worktree `pre2015-nsearch`), `_apply_reads.py --pre2015` generalized to read
EVERY step's ledger (`PRE2015_LEDGERS` = D's + N's; tracked, committed before the first batch
push). `_nse_archive_revop.py` itself is UNCHANGED — its list/detail/cache/alias/&-encoding
machinery is imported as-is; the period-parameterized list fetch is a sibling function.

**Numbers: of 8,856 universe cells (6,002 in 2005-07 + 2,854 2008-14 residue, 635 companies),
7,816 landed (88.3%) — GATE F 4,595 · E 2,197 · X 1,013 · S 11, plus 327 cumdiff-derived legs —
and 1,040 refused, every one with a named reason. All 635/635 companies adjudicated; accounting
closed (landed + refused == universe).** Coverage by year: 2005-07 ~91%, 2008-11 ~86-93%,
2012-14 35-44% (that tail is the era STEP D already harvested, so N only sees its hard residue).

**GATE X is the step's real unlock** (1,013 cells): detres cache was copied in from the STEP D
worktree, so cross-source checks cost zero BSE refetches. SBIN Mar-08 = 1,883.25 — the cell STEP D
documented as "correctly refused, expected to close once STEP N supplies GATE X" — landed via X
and is LIVE-verified through the client (`fin/SBIN.json`). Also client-verified: INFY FY2006
quarters sum 2,421.85 and RELIANCE FY2006 sum 9,069.00, both matching the public standalone
FY06 record.

**Cumulative-difference derivation** (LANDING RULES 1) is what makes the era tractable: many
pre-2015 filers never lodged a discrete Q4 (it is folded into the Annual only — TATASTEEL FY08 is
the type case). A running chain-sum of an UNBROKEN prefix of DIRECT quarters from Q1 serves as
the cumulative baseline, so Q4 = Annual − (Q1+Q2+Q3) with all four legs real documents; the chain
is cut, never revived, the moment a quarter can't be resolved, so a derived leg is always exactly
one subtraction from direct reads and still needs GATE F. Never GATE E (a derivation was not a
party to the document's own EPS row).

**Anti-poison:** the document's OWN printed "Period Ended" must equal the quarter its list row
promised, or the read is discarded — this is the direct defence against §45's NSE list
double-indexing, applied at the source rather than detected afterwards.

**Residue, 1,040 cells, every class named** (full per-cell JSON via `_pre15_stepn_report.py --json`):
454 no-usable-leg-for-quarter · 434 gate-F-and-E-both-failed · 141 no-nse-rows-for-that-FY ·
10 no-nse-filings-any-era (IDBIBANK class) · 1 gate-S-disagree (AARTIIND 20130630, left for hand
§45 adjudication, never auto-healed). Calendar-year filers (ACC, AMBUJACEM) refuse safely rather
than mis-landing: the Apr-Mar FY frame doesn't fit them and the Period-Ended check stops the
annual being slotted as a quarter — same "non-standard FY refuses correctly" outcome STEP D had.

**Poison guards, all reviewed against raw source, none blind:** 5 new yshift pairs adjudicated
genuine and allowlisted (LGBROS/RIIL proven by raw precision differing below 2dp display —
415.47 vs 415.18, 468.97 vs 469.46; APOLLOHOSP/COSMOFIRST/HONAUT are exact ties at those filers'
whole-lakh granularity, but distinct documents + verified Period Ended + different rev/EPS per
year + an independent per-year gate exclude the double-index mechanism; HONAUT also repeats 13.9
at an ADJACENT quarter, which no year-shift can produce). 15 sanity cells allowlisted:
ZUARIIND ×11 (the "4× established max 273" reference is the POST-demerger holdco — fertiliser
business demerged to Zuari Agro ~2011-12, series collapses 966→23; 6 of the flagged cells landed
via GATE X, i.e. BSE agrees independently), ASTRAZEN ×2 (raw 8821.96 vs 8821.80), CESC ×2 (filer
rounds every quarter to whole crore; neighbouring Sep-06 is 675, so the 674 cluster is real).
MASTEK/PEL/SUZLON stay nulled — STEP D's prior deliberate verdict, not revisited.

⚠️ **Three orchestration bugs cost real work and are worth not repeating** (all now fixed in the
scripts, and the lessons generalized into DATA_RUNBOOK §38):
1. A bare relative filename passed to a script the driver runs with `cwd=ROOT` resolves against
   ROOT, not `scripts/`. Hit three separate scripts. In `_yshift_scan_pre15.py` it silently
   disarmed the poison guard for all 27 chunks — it reported `clean (0 landed cells checked)`
   every time. **A guard that reports "clean (0 checked)" is not clean, it is not running.**
2. The reset+reapply push cycle can report success while pushing nothing — twice: first when the
   checkout-back list contained an UNTRACKED path (one bad pathspec fails the whole multi-path
   `git checkout`, leaving ledgers at their just-reset state), then when an edit fixing that
   accidentally deleted the `git reset --hard origin/main` line itself. Push return codes, empty
   staged diffs and local commits ALL lied about whether data reached origin. The only honest
   check is reading the ledger back out of `origin/main` — now enforced in the driver.
3. "ALL COMPANIES PROCESSED" meant only that the CURSOR reached the last symbol. A harvester
   crash (rc=1) fell through a stop-gate that only tested `rc == 2`, and the cursor advanced past
   20 never-harvested companies. Found by reconciling ledgers against the universe, not by
   trusting the completion message; 24 companies / 244 cells recovered.

Next: STEP W (2002-2004 wayback probe) and/or STEP Q close-out QA. Note STEP N did NOT land any
`con` cells — con was deliberately out of scope for both pre-2015 appliers, and remains an
available LANDING-RULES-sanctioned bonus for a later pass.

## STEP W — 2002-2004 (and pre-2008 residue): PROBE the archived web  (feasibility step, capped)

No exchange source exists (proven). Candidates, in probe order, each = CDX enumerate → fetch 5
samples → judge machine-readability + coverage BEFORE any harvest code:
1. **myiris.com** per-company pages — CDX-confirmed alive today
   (`myiris.com/shares/company/...php?icode=<CODE>` captures exist); MyIris carried full quarterly
   results tables in the era; icode↔symbol map would need building (name resolution rules apply).
2. moneycontrol old results pages (pre-2010 URL scheme, NOT the 2011+ `stockpricequote` scheme —
   the SHP wayback trick worked on 2011+; the 2002-04 scheme is unprobed).
3. indiainfoline.com `/comp/` company financial pages (CDX shows the tree exists).
4. Archived bseindia.com result pages (`qresann.asp?scripcd=` era) + archived nseindia.com
   results pages (pre-2005 site design carried inline results tables — same family as the
   pre-2008 index lists found under .htm).
Landing bar is HIGHER here (3rd-party renditions): GATE X against a second independent aggregator
OR GATE F against the FY05 NSE annual chain (FY05 annual brackets 2004Q2-Q4 via the identity —
that alone can prove the LAST pre-NSE year). Anything landed carries `src:'wb-<site>'`. Budget:
one session; deliverable = feasibility verdict + (if green) the harvest recipe appended here.
**A permanent partial floor for 2002-04 is an acceptable honest outcome — report it, don't force it.**

### STEP W — status (2026-08-05, Sonnet): FEASIBILITY DONE, verdict **GREEN — bigger than scoped**.
No cells landed (capped probe, per the doc). Worktree `pre2015-stepw`, scratch harness
`_stepw_probe.py`/`_stepw_myiris.py`/`_stepw_sample.py`/`_stepw_validate.py` (untracked, disposable).

**Headline correction to this doc's own premise**: candidate 4 (archived `nseindia.com` results
pages) is not a weak fallback — it is a **genuine EXCHANGE-NATIVE source**, the same page family
STEP N already harvested for 2005-07, just an earlier site revision
(`marketinfo/companyinfo/eod/results.jsp`, retired long before STEP N's live-fetch code could ever
reach it — only wayback holds it now). This directly contradicts the STATE OF THE WORLD /
candidate-list framing above ("No exchange source exists (proven)") for this era. Recommend the
**normal landing bar** (gates F/E directly, X as a bonus), not the higher third-party bar — it is
an exchange source, not a rendition of one.

**Measured coverage (full CDX enumeration, not a sample — 30,039 rows returned on a 60,000 cap, so
nothing was truncated):** the archived `.../eod/results.jsp` tree holds **10,874 captures, 2002-2006**
(892 / 2,967 / 4,791 / 1,925 / 299 by year), URL-encoded as
`<from><to><period><audit><cum><consol><const><SYMBOL>` (period ∈ Q1-Q4/H1/H2/AN, e.g.
`01-APR-200230-JUN-2002Q1UNNNETATAMOTORS`). Against STEP G's 566-symbol / 4,493-cell 2002-04
target set: **406/566 companies (71.7%) have ≥1 archived row; 2,458/4,493 target cells (54.7%)
have a row landing on that EXACT quarter-end**, direct hits only — before any GATE-F/cumdiff
derivation of the kind that lifted STEP N from its direct-hit floor to 88.3%. Treat 54.7% as a
floor, not a ceiling.

**Gates validated on real fetched pages, not assumed:**
* **GATE F, exact match**: KANORICHEM FY2003 (Apr-02→Mar-03) — Q1+Q2+Q3+Q4 sales
  7,807+8,961+9,090+8,682 = **34,540**, Annual row = **34,540** (identical, not just within
  tolerance); PAT 314+386+403+169 = **1,272**, Annual = **1,272** (identical). Same-source,
  same-basis, tiling dates confirmed before summing.
* **GATE E, exact match**: ALPSINDUS FY01 annual — PAT 839 lakh / (Equity Capital 620 / Face
  Value 10 = 62 lakh shares) = EPS 13.53, page's own printed Basic EPS = **13.53**. TATAMOTORS
  Q4FY03 same check: 13,757 / (31,983/10) = 4.301 ≈ printed **4.30**.
* **A real near-miss, caught and named, not forced**: TATAMOTORS' "AN"-labelled FY2003 row
  looked like a 4.2% GATE-F miss at first pass — until the page's own `Result Type` text was read
  properly: that row is **Consolidated** (sales 1,141,386 vs the Non-Consolidated Q1-4 sum
  1,083,701), while Q1-Q4 are **Non-Consolidated**. The URL's own flag characters had encoded this
  (`NC` vs `NN` in the consolidation slot) but a quick URL-only decode mis-read the position and
  called it standalone. **Conclusion for the recipe: basis and period MUST be read from the
  page's own printed `Result Period`/`Result Type` text, never inferred from the URL** — the URL
  is fine for cheap existence-counting, never for landing decisions. This is the same discipline
  §45/STEP N already mandate, now independently confirmed necessary on this source too.
* **Calendar-year filers present here too, handled correctly by the existing rule**: GLAXO's
  four "quarters" first looked like a 4.1%/8.8% GATE-F miss — it is a Jan-Mar-and-a-Dec-year-end
  mix (real dates: Q1 Jan-Mar 2004, Q2-Q4 Apr-Dec 2003, Annual Jan-Dec 2003), i.e. they don't
  tile one 12-month window at all. The mandatory date-span/tiling check (LANDING RULES 1) throws
  this out before a false identity is ever computed — exactly its job, and this era needs it just
  as much as 2005-14 did.
* Bank-template rows (SBIN, IDBIBANK, ANDHRABANK, J&KBANK, ORIENTBANK, …) and a older/shorter
  2002-vintage URL grammar (1-char audit flag instead of 2, seen on `ANACEALPSINDUS`-style 2002
  captures) are a large share of the "unparsed by URL" residual (880/891 of 2002's with-query
  rows) — **not missing data, a second grammar** a harvester must branch on, same as detres
  needed a bank field-map. Un-triaged here on purpose (harvest-code territory).

**myiris.com (candidate 1) — confirmed as a usable secondary/BSE-only route, not the primary.**
Full CDX enumeration of `financial.php` (2001-2006, per-year queries, none truncated): **24,786
captures, 4,460 distinct company codes**. Page header carries exact `BSE:` code + `NSE:` symbol +
`ISIN` (no fuzzy matching needed — RELINDUS example: `BSE:500325 NSE:RELIANCE
ISIN:INE002A01018`). Its "Latest Quarterly/Halfyearly" table yields 2 periods per capture
(current + year-ago comparative), both with declared period-end + month-count in the column
header itself. **Parser bug found and fixed during this probe**: the block's caption text
("Latest Quarterly/Halfyearly") also appears in an unrelated section-nav row that leads straight
into the ANNUAL income statement — matching only the first hit silently read annual figures as
quarterly. Fixed by requiring the real block's row (the one carrying the "Detailed Quarterly"
popup link). Post-fix, sampled cached pages: 42/48 (87.5%) yield a usable quarterly block (the
rest are genuinely annual-only reports), and every period extracted carried both rev and PAT
(81/81). Useful mainly as (a) a GATE-X cross-check aggregator once NSE-archive is the primary
read, and (b) an identity-resolution assist for STEP G's 126 residual unresolved BSE codes (its
header gives BSE code directly) — not measured as a coverage number here, flagged as a follow-up.
Best-effort only: a broader random-sample coverage run was started (400 icodes) and stopped early
to prioritize verifying the much stronger NSE-archive lead once it surfaced — not a dead end,
just de-prioritized.

**moneycontrol / indiainfoline / archived bseindia.com (candidates 2-4's other half) — UNPROBED,
not ruled out.** Wayback CDX began refusing connections (`ConnectionError`, not 4xx/5xx) partway
through this session after sustained querying. Per the campaign's own hard line ("never rotate
through header tricks" / stop and report on service errors), this was backed off rather than
pushed through. `nseindia.com/marketinfo/companyinfo/` itself briefly hit the same throttling and
recovered on retry after a backoff; moneycontrol's older (pre-2011) URL scheme and
indiainfoline.com's `/comp/` tree were never reached. Given the NSE-archive finding above is
already the strongest possible kind of source (exchange-native, not third-party), these three are
now backup routes for the residual ~45% rather than a load-bearing part of the feasibility case —
re-probe them only if the NSE-archive harvest's own residual needs a second aggregator.

**RECIPE for a future STEP W-execute session** (this step deliberately stops here, per the doc's
own cap — no ledger, no landing code, nothing pushed except this write-up):
1. Worktree `pre2015-stepw-harvest` (fresh). Per target symbol (STEP G's 566, era-chain resolved):
   CDX-enumerate `nseindia.com/marketinfo/companyinfo/eod/results.jsp?*<SYMBOL>` (per-symbol query
   avoids any prefix-crawl truncation risk) and cache every hit.
2. Parse using ONLY in-page text for period/basis/figures (the URL grammar is enumeration-only,
   per the TATAMOTORS finding above); reuse the field-name map already proven here (`Net Sales`,
   `Net Profit(+)/Loss(-)`, `Basic EPS (in Rs.)`, `Paid-up Equity Share Capital`, `Face Value of
   Share (in Rs.)`); add the bank template (Interest Earned line) as a second field-map, same
   pattern as detres/LANDING RULES 4.
3. Gates: F primary (this doc's KANORICHEM proof), E secondary, X bonus vs myiris where both
   exist. Mandatory date-tiling check BEFORE every FY-sum (GLAXO proof above). Cumulative-row
   differencing (LANDING RULES 1) for filers who folded Q4 into the annual, exactly STEP N's
   technique — untested here but there is no reason to expect this source behaves differently.
4. Old-grammar 2002 captures and bank rows: branch by reading the page, don't extend the URL
   regex further — it was never meant for landing.
5. Ledger `scripts/pre2015_reads_w.json`, `_apply_reads.py --pre2015` extended the same way STEP N
   extended it for STEP D's ledger (`PRE2015_LEDGERS` list).
6. Expect the 54.7% direct-hit floor to rise once cumdiff + GATE F are applied (STEP N's direct
   hits were lower than its final 88.3% for the same reason) — but budget for a genuine residual:
   2002 Q1-Q3 also has no membership-bin coverage today (STEP M's own documented hole), so those
   quarters may be un-auditable even where financials exist. **A permanent partial floor here
   remains a legitimate, honest possible outcome for the residual — this status block does not
   promise 100%, only that the source is real and materially bigger than "does not exist".**

Next: either STEP W-execute (harvest per the recipe above) or STEP Q close-out QA on D+N first —
user's call, both are now unblocked.

### STEP W-execute — status (2026-08-05, Sonnet): 10 BATCHES SHIPPED, 768 cells / 123 companies (21.7%)
**Running total after batch 10 (verified on origin): 768 cells, 123 companies, ~500 refused.**
**PAUSED here deliberately, not blocked**: wayback's connection quality degraded hard for the back
half of this session (batches 9-10 landed only 5 and 8 cells respectively across 1,000+ log lines
each — well under 1% yield) after recovering from the earlier full outage but never returning to
batches 1-6's throughput. Every batch still checkpointed and pushed rather than left sitting
locally. **Resume whenever convenient — a quick 2-3 sample CDX pings (see `_stepw_wb.cdx()`
against a trivial URL) before committing to a full run is the cheap way to check whether
conditions have recovered; don't grind a sub-1% run to conclusion, stop and re-check instead.**
Batches: #1 114/19 · #2 +192→306/48 · #3 +188→494/76 · #4 +52→546/84 · #5 +100→646/100 · #6
+83→729/117 · #7 +20→749/122 · #8 +6→755/122 · #9 +5→760/122 · #10 +8→768/123 (batches 5-10 all
pushed via reset+reapply — plain
rebase conflicted on the minified JSON every single time origin moved between fetch and push,
which given ~30 concurrent CI workflows on this repo is most attempts; never worth retrying plain
rebase more than once). Batch 5 caught+resolved a new revop_sanity flag, CUMMINSIND
20040930/20041231 — exact revenue coincidence across two DIFFERENT source captures with different
PAT, same "genuine duplicate-value" class STEP D already documented, nulled per revop_sanity's
default, PAT unaffected (re-nulled identically in every later batch since re-applying from the raw
ledger re-derives it fresh each time — expected, not a regression).

**Throughput tuning, two rounds**: `_stepw_wb.py` retry/backoff cut from 4 attempts/10-20-30s
backoff to 2/5s (batch 5→6, ~2x), then further to a single attempt with NO backoff sleep (batch
7→8) — observed failures are fast connection-refused errors, not slow timeouts, so retrying
in-process mostly just burns wall-clock on a connection that isn't coming back within that
process's lifetime; a future full re-run costs nothing extra since successes are cached (per-FY
fetch-failure tracking already made this safe, see batch 1's design-gap fix). Deliberately did
**not** parallelize across multiple agents when asked — the bottleneck is wayback's own connection
throttling (confirmed by isolated CDX+page-fetch probes going into total refusal mid-session, unrelated
to this harvester's own request pattern, then recovering), not local orchestration capacity; more
concurrent writers against an already-throttling external endpoint risks making it worse, and this
class of backfill has a standing one-writer-at-a-time rule from a past incident (memory
`feedback-backfill-one-agent-at-a-time`). **A genuine multi-minute-to-tens-of-minutes wayback outage
happened mid-session** (total connection refusal on trivial isolated requests, not just this
harvester) — backed off and waited per the campaign's own hard line rather than retried through it;
it self-recovered. Batch-1's other detailed findings (calendar-year gate-mix pattern, the two
pre-launch bugs) below still hold and were not re-litigated per batch. **RESUME exactly as below —
nothing else has changed.**

#### Batch 1 detail (114 cells / 19 companies) — findings below still apply to every later batch
Worktree `pre2015-stepw-harvest`. `scripts/_stepw_wb.py` (wayback CDX+fetch, untracked scratch)
+ `scripts/_stepw_nse_pre15.py` (untracked scratch, mirrors STEP N's structure: universe → per-
symbol candidate fetch → FY bucketing → chain/cumdiff resolution → gate S/F/E in that priority —
GATE X deliberately left stubbed this pass, same precedent as STEP D's first run, would need also
harvesting myiris). `_apply_reads.py --pre2015`: added `pre2015_reads_w.json` to `PRE2015_LEDGERS`
(tracked, one-line addition, same pattern STEP N used to add itself).

**Numbers (verified by reading the ledger back off `origin/main`, not from the run's own log —
DATA_RUNBOOK §38b): 114 cells landed across 19 companies** (alphabetically 3MINDIA→SBIN/GLAXO,
the run was stopped partway through the 566-symbol universe, see below), **76 refused** with
reasons in `pre2015_attempted_w.json`. Gate mix: **F=35 · E=79 · S=0 · X=0** (X unattempted this
pass). `revop_sanity` and the yshift scan both clean — the only flags present (6 cells,
MASTEK/PEL/SUZLON) are STEP D's own pre-existing, already-adjudicated nulls, untouched by this
batch. Applied cleanly via `_apply_reads.py --pre2015`: 114 new `sf_fundamentals.json` rows, 0
skipped, consistent whether re-derived before or after a same-day CI push touched unrelated files.

**Gate F validated on new data with an exact match**: APOLLOTYRE FY2004 (Apr03-Mar04) — all four
quarters sum to the annual PAT exactly twice over (`qsum=70.42 annual=70.42`, and separately
`qsum=120.02 annual=120.02` for its FY2003) — the KANORICHEM-style clean identity the probe found
holds up at real harvest scale, not just on the hand-picked probe sample.

**A real, structural (not a bug) finding: calendar-year filers make GATE F unavailable under
Apr-Mar bucketing, but GATE E still lands them correctly.** ABB, GLAXO and 3MINDIA (all
foreign-parented — Swiss-Swedish, UK, US respectively — a common pattern: Indian subsidiaries of
multinationals often keep the parent's Jan-Dec fiscal year rather than India's Apr-Mar norm) file
real quarters that this harvester's Apr-Mar `fy_of()` bucketing can never assemble into one tiling
FY: a Jan-Dec filer's calendar Q1 (Jan-Mar) lands in one Apr-Mar bucket while its Q2-Q4 and its own
annual land in the NEXT one, so no bucket ever holds all four quarters + a matching annual, and the
mandatory date-tiling check (correctly) refuses every attempt. GATE E has no such dependency (it is
a single-document self-check, EPS × equity/face-value ≈ PAT, independent of which bucket a leg
landed in) and lands these companies' real quarters correctly regardless — confirmed by spot-check:
GLAXO Q1FY03 (01-Jan-2003→31-Mar-2003) raw page Net Sales 28,286 lakh / Net Profit 3,509 lakh /
EPS 4.70 / equity capital 7,448 lakh reproduces the ledger's rev=282.86 pat=35.09 exactly, and
implied EPS-recon (4.70×74.48/10=35.01) sits 0.08cr from the printed PAT, comfortably inside gate.
**This is a gate-MIX effect, not a correctness risk** — data lands right, just at a weaker (still
LANDING-RULES-sanctioned) tier for this filer class. Noting it here rather than attempting a
company-specific true-FY detector, which would be a proportionality mismatch for what this step
budgeted.

**Bank template spot-verified byte-exact**: SBIN Q1FY05 (01-Apr-2004→30-Jun-2004) raw page
Interest Earned 766,657 lakh / Operating Profit 207,140 lakh / Net Profit 105,840 lakh matches the
ledger's rev=7666.57 op=2071.40 pat=1058.40 to the last digit. SBIN's own FY-sum identities also
landed clean (e.g. FY2005 `qsum=4304.52 annual=4304.52`, exact).

**Two real bugs found and fixed while building the harvester (before any cell landed), both worth
naming so they aren't repeated:**
1. `gate_e()`'s EPS-recon implicitly double-divided by 100: `eqcap` is stored already
   crore-converted (`to_crore()` applied when the leg is built), so multiplying by a further
   `/100.0` inside the gate silently made every implied-PAT calculation 100x too small. Caught by
   hand-deriving the ALPSINDUS numbers before running any code, not by a failed test.
2. `cumdiff()`/`add_legs()` (the Q4-folded-into-annual derivation, STEP N's own technique) didn't
   carry a `frm`/`to` date span on the leg they return, which would have made the mandatory
   date-tiling check silently and permanently fail for every FY needing ANY derived leg (tiles()
   requires real dates on all four legs). Fixed by computing each derived leg's known analytic
   quarter-boundary (`quarter_bounds()`) rather than trying to read one off a page that was never
   fetched for that exact slot.

**A design gap fixed after the first live test, not caught by reading the code alone**: a
transient wayback fetch failure on the ONLY candidate covering some quarter would otherwise fall
through to a permanent `"no-archive-rows-for-that-FY"` refusal — indistinguishable in the ledger
from a genuine absence, and (worse) it would permanently block retrying that cell on a future run,
since refused cells are skipped by the wanted-cell filter. Fixed: fetch failures are now tracked
per-FY and suppress the refusal write entirely for any cell in an affected FY, leaving it eligible
for a future run to pick back up once the page is reachable (its content is cached the moment a
fetch DOES succeed, so a retry costs nothing extra).

**Why only 19/566 companies this pass**: sustained wayback connection throttling
(`HTTPSConnectionPool ... Max retries exceeded`) throughout the run, consistent with the same
throttling the STEP W probe hit earlier the same day. A 4-worker prefetch pool (same pattern STEP
D/N already validated: warms the on-disk cache ahead of the unchanged sequential gate/land loop)
was added mid-session but didn't meaningfully outrun the throttling. This is an environmental
condition, not a code defect — every fetch failure is retry-safe by design (see the design-gap fix
above), so resuming later costs nothing beyond wall-clock time.

**RESUME RECIPE**: `cd scripts && python -X utf8 -u _stepw_nse_pre15.py` (no arguments — it
automatically skips every `(sym, qe)` already in `pre2015_reads_w.json` or
`pre2015_attempted_w.json`, so re-running is free for everything already resolved and just
continues alphabetically). Checkpoints every 50 landed cells and every 10 companies — if stopping
before a natural end, expect to lose whatever landed since the LAST checkpoint (harmless: cached
pages make it free to re-land on the next run; confirmed by this batch's own APOLLOTYRE cells,
computed correctly mid-run but lost when the process was stopped before their checkpoint, and not
present in what was actually pushed — caught by reading the ledger back off `origin/main` rather
than trusting the run's own log, exactly the §38b discipline). After each further batch: apply
(`_apply_reads.py --pre2015`) → `revop_sanity.py --dry` → `_yshift_scan_pre15.py --reads
pre2015_reads_w.json` → commit (tracked ledger + `_apply_reads.py` if changed + the 3 data files)
→ push via reset+reapply (plain rebase WILL conflict on the minified JSON, confirmed this batch)
→ verify by reading the ledger back off origin, not the push return code.

## STEP Q — CLOSE-OUT QA (after D+N, and again after W)

1. Full-window audit table (member-quarter × {rev,pat} × year) from `_n500_member_bin` — the
   month-grid convention (a quarter credits its 3 months; "1,1,1" = ONE cell).
2. revop_sanity full run; yshift full scan minus yshift_genuine; duplicate-PAT scan; results_season
   + quarterly_results confirmed rebuilt (nightly CI); factor-coverage harness re-run → update
   memory `project-stocks-factor-coverage-audit` (fundamentals-dependent factors should light up
   2005+ / 2008+); spot-check THROUGH THE CLIENT (stock page financials tab shows 2008 quarters;
   a 2006-start backtest with a fundamentals filter returns non-empty picks).
3. Update memories (`project-stocks-pre2015-phase` — supersede its "2012-2014 only" scope note),
   never_filed.json grave count, and the progress log below.

## WHAT SONNET MUST NOT DO (hard lines, all learned the expensive way)

* No fuzzy name→symbol resolution into membership or ledgers. No Screener annual derivation.
* No scale/unit heuristics — arithmetic proof only. No overwriting stored cells outside pat_defects
  double-evidence flow. No vision/OCR/PDF spend inside these steps. No parallel writers.
* No `git add -A`/stash/reset in the shared checkout; worktrees only.
* Never mark a refusal "unfillable" without the announcement-window probe + evidence line.
* If detres/NSE start serving errors or 403s: STOP the batch, wait/report (NSE lockdowns are
  transient — memory `project-stocks-nse-api-lockdown`), never rotate through header tricks.

## Progress log (append one line per completed step)
- 2026-08-04: campaign designed; all sources probed (detres floor Mar-2008 full P&L / .50 FY08+ /
  EPS rows present / delisted served; NSE archive floor Mar-2005 quarterly+annual, no comparative
  column, no pre-2018 filing dates; 2002-04 = no structured source, wayback candidates listed);
  scope measured 26,022 member-quarter std cells, 727 stored, ~25.3k open, 839 ever-members;
  bin membership empty pre-Nov-2006 → STEP M created; gates X/F/E/S designed and validated on
  RELIANCE/SBIN/HINDALCO/SATYAMCOMP era data.
- 2026-08-04: STEP M1+M2 shipped (see STEP M status block) — N500 bin now 120 snapshots
  2002-10-02→date.
- 2026-08-04: STEP G shipped (Sonnet) — `cut_gaps_0214.py` cuts true per-quarter-membership
  gaps: 24,421 real member-quarter cells, 24,420 open (744 dual-form, PAT-only). BSE-code
  reachability 691/727 (2008-14) · 508/574 (2005-07) · 463/566 (2002-04) via by_id → rename-chain
  → exact scrip_id match → symchg-name match, no fuzzy matching; 126 unique symbols unresolved,
  triaged into 4 classes (mnemonic-suffix mismatch / genuine multi-entity ambiguity / absent from
  BSE master / likely membership-bin identity split — full detail in the STEP G status block
  above). `_n500_member_bin.py` + `_bse_master_all.json` + `_scrip_extra.json` promoted from
  untracked rev-mission-only scratch to committed shared assets.
- 2026-08-04: STEP D shipped (Sonnet) — see STEP D status block above. 11,071/13,925
  2008-14 gap cells landed (F 8,996 · S 732 · E 1,343), 2,541 refused with reasons,
  313 unresolved-code residue. Found+fixed a real correctness bug mid-run (GATE F's
  alternate-scale rescue was circular, caught by the yshift poison scan on
  GAMMONIND; removed, 28 cells across 6 companies retracted and correctly
  re-derived). Reviewed all 90 revop_sanity nulls against raw cache; 84 allowlisted
  as genuine (sanity_ok.json), 6 deliberately left refused (ambiguous exact-match
  revenue, ok's PAT unaffected). LIVE-verified through the client (per-stock
  fin/<SYM>.json slice, not just the bulk file). SATYAMCOMP/HINDALCO spot values
  exact; SBIN Mar-08 correctly refused pending STEP N's GATE X.
- 2026-08-04: STEP N shipped (Sonnet) — see STEP N status block above. 7,816/8,856 cells
  landed (88.3%; F 4,595 · E 2,197 · X 1,013 · S 11, +327 cumdiff legs), 1,040 refused
  with named classes, 635/635 companies adjudicated, accounting closed. GATE X lit up for
  the first time (1,013 cells, free off the copied STEP D detres cache) and closed SBIN
  Mar-08 = 1,883.25 exactly as STEP D predicted. Cumulative-difference derivation unlocks
  the many filers who folded Q4 into the annual. LIVE-verified through the client: SBIN
  Mar-08, plus INFY FY06 (2,421.85) and RELIANCE FY06 (9,069.00) matching public record.
  5 yshift pairs + 15 sanity cells reviewed against raw source and allowlisted with
  evidence. Three orchestration bugs found and fixed (bare-relative-path silently disarming
  the yshift guard for all 27 chunks; a push cycle that reported success while pushing
  nothing, twice; a cursor that advanced past 20 never-harvested companies on a crash) —
  lessons generalized into DATA_RUNBOOK §38.
- 2026-08-05: STEP W feasibility probe done (Sonnet) — see STEP W status block above.
  Verdict GREEN, bigger than the doc originally scoped: the archived `nseindia.com`
  `eod/results.jsp` tree (2002-2006, 10,874 captures, full CDX enumeration not a sample)
  is a genuine EXCHANGE-NATIVE source for this era, not just third-party renditions —
  corrects this doc's own "no exchange source exists" premise for 2002-04. Measured
  against STEP G's 566-symbol/4,493-cell target set: 406 companies (71.7%) reachable,
  2,458 cells (54.7%) hit on the exact quarter-end directly, before any GATE-F/cumdiff
  uplift (STEP N's own direct-hit rate was lower than its eventual 88.3% for the same
  reason). GATE F validated with an EXACT match (KANORICHEM FY2003, both sales and PAT);
  GATE E validated exact (ALPSINDUS, TATAMOTORS). Caught and corrected a real trap before
  it could land anything: basis/period read from the URL's own flag encoding silently
  mislabelled a Consolidated annual as standalone on TATAMOTORS (the page's own printed
  Result Type text disagreed) — recipe now mandates page-text-only reads for landing
  decisions, URL decoding for cheap enumeration only. Calendar-year filers (GLAXO) also
  present in this era; the existing mandatory date-tiling check correctly refuses to sum
  across a non-tiling quarter set rather than mis-landing. myiris.com confirmed as a
  usable secondary/cross-check + BSE-code-resolution route (24,786 captures/4,460
  companies, header carries BSE+NSE+ISIN with no fuzzy matching) but de-prioritized once
  the stronger NSE-archive lead surfaced; moneycontrol/indiainfoline/archived-bseindia
  left UNPROBED (wayback began refusing connections mid-session — backed off per the
  hard line, not ruled out, just not needed for a green verdict). No cells landed, no
  ledger created, nothing pushed except this write-up — per the doc's own cap, a harvest
  recipe is appended for a future STEP W-execute session rather than executed here.
- 2026-08-05: STEP W-execute batch 1 shipped (Sonnet) — see status block above. 114 cells /
  19 companies landed (F=35 incl. an exact APOLLOTYRE FY-sum match, E=79), 76 refused, both
  verified by reading the ledger back off origin (not the run's own log — a checkpoint
  boundary quirk lost some cells the log showed landing, e.g. APOLLOTYRE, from what actually
  reached origin; harmless, cached pages make them free to re-land next run). revop_sanity
  and yshift both clean, no new flags. Found a real structural (not a bug) pattern: Jan-Dec
  calendar-year filers (ABB/GLAXO/3MINDIA, foreign-parented) can never satisfy GATE F under
  Apr-Mar bucketing, but GATE E lands them correctly regardless (spot-verified byte-exact)
  — a gate-mix effect, not a correctness risk. Also fixed two real bugs before any cell
  landed (an EPS-recon double-divide, and derived cumdiff/chainsum legs missing the dates
  the mandatory tiling check needs) and one design gap after the first live test (a
  transient fetch failure was falling through to a permanent, un-retryable refusal — fixed
  to stay retryable). Stopped at 19/566 companies due to sustained wayback throttling
  (environmental, not a defect); full resume recipe in the status block — re-running the
  harvester with no arguments continues alphabetically for free.
- 2026-08-05: STEP W-execute batches 2-5 shipped (Sonnet) — cumulative 646 cells / 100
  companies (17.7%), 454 refused, all verified on origin after every batch. Retry/backoff
  tightened mid-run (`_stepw_wb.py`: 2 attempts/5s, was 4/10-20-30s) for roughly 2x
  throughput at the same request volume — deliberately NOT parallelized across agents
  (the bottleneck is wayback's own connection throttling, not local orchestration; more
  concurrent writers against the same rate-limited endpoint plus the standing
  one-writer-at-a-time rule for this class of backfill made that the wrong lever). One new
  revop_sanity flag this run (CUMMINSIND, genuine duplicate-value coincidence, same class
  STEP D already named) resolved the same way STEP D did. RESUME: `cd scripts && python
  -X utf8 -u _stepw_nse_pre15.py`, no arguments, continues from company 100/566.
- 2026-08-05: STEP W-execute batches 6-8 shipped (Sonnet) — cumulative 755 cells / 122
  companies (21.6%), ~500 refused, verified on origin after every batch. Retry/backoff cut
  further to a single no-backoff attempt (was 2/5s) for another throughput jump — a future
  re-run costs nothing extra on any miss, so failing fast beats waiting in-process. Hit and
  rode out a genuine multi-minute-plus wayback outage mid-session (confirmed via isolated
  probes unrelated to this harvester, not a local defect) — backed off per the campaign's
  hard line rather than retried through it, resumed once it self-recovered. Explicitly
  declined to parallelize across agents when asked: the bottleneck is external connection
  throttling, not orchestration, and this backfill class has a standing one-writer rule from
  a past incident — more concurrent writers against an already-struggling endpoint would
  likely worsen it, not help. RESUME: `cd scripts && python -X utf8 -u _stepw_nse_pre15.py`,
  no arguments, continues from company 122/566 (~444 remain).
- 2026-08-05: STEP W-execute batches 9-10 shipped (Sonnet), then DELIBERATELY PAUSED —
  cumulative 768 cells / 123 companies (21.7%), verified on origin. wayback never returned
  to earlier throughput after the mid-session outage cleared — yield fell under 1% (5 and 8
  cells landed across 1,000+ log lines each). Stopped grinding a low-yield connection rather
  than keep burning cycles; both tiny batches still checkpointed+pushed regardless, nothing
  left uncommitted. RESUME: same command as above, continues from company 123/566 (~443
  remain) — cheap health-check first (a couple of trivial CDX pings) before committing to a
  full run, per the note in the status block above.
