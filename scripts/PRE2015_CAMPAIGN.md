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

## STEP G — CUT THE GAP UNIVERSE  (30 min, after M)

From the rebuilt membership: for each quarter 2002Q1→2014Q4, members = `_n500_member_bin`
membership(qe); cells = member-quarters minus stored (sym,qe) std rows. Emit
`scripts/_gaps_0214.json`: per cell {sym, qe, bse_code, nse_sym_era_chain, era}. BSE codes:
`bse_scrips.json` by_id direct → rename-chain variants → `_bse_master_all.json`+`_scrip_extra.json`
NAME-match (the UNITEDBNK-disease recipe; measured today: direct-only resolves 583/708 of the
2008-14 union — the name-match pass must close most of the rest; GLOBOFFS/GTOFFSHORE-class old-name
gotchas are documented in memory `project-stocks-rev-2015-extension`). Report reachability per era.
Commit the gap file + the cutter script.

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
