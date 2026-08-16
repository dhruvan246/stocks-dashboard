# N500 COVERAGE-100 CAMPAIGN — every parameter 100% on 🧭 Coverage Matrix, Nifty 500, 2020-01 → date

**Written 2026-08-16 13:11 IST (Fable, planning session). Executor: Opus, fresh session.**
**Status: PHASE 0 COMPLETE (2026-08-16 13:30 IST, commit 11128752). NEXT: PHASE 1.**
Update this line as phases close.

### P0 results — measured, supersedes §2 and §8 where they differ

Worktree `~/stocks-wt/n500-cov`. Two full bakes (plain, then `--explain`), queue built, parity PASS.

**Deliverables shipped:** `build_coverage_matrix.js --explain <slug> [--explain-from] [--explain-out]`
(names the symbols behind every sub-100 cell from the SAME vm scan that writes the payload — proven
analysis-only: `--dates 5` baked with and without it, 31 payloads / 429 keys byte-identical);
`scripts/n500_cov_cells.py` (build + `--check`); `scripts/n500_cov_queue.json`
(**1,634 rows · 615 distinct symbols · all 8,329 missing member-dates**);
`scripts/n500_cov_explain.json` (provenance).

**PARITY PASS on all 12 gap params** against the payload from the same bake
(window 2020-01-31 → 2026-08-12, 80 month-ends):

| param | missing | vs §2 | composition |
|---|---|---|---|
| ebit | 3,402 | −2 | 45 never-has-ebit names / 2,819 cells + 94 names / 583 |
| fiiChgPp | 1,649 | −27 | 1,474 in the three SEBI months + **175 residue over 107 names** |
| diiChgPp | 1,649 | −27 | identical cells |
| profitTTM | 525 | +1 | 128 names |
| composite | 525 | +1 | 128 names, tracks profitTTM |
| op | 398 | = | 67 names |
| profitAccel | 101 | = | 30 names |
| profitYoyPct / profitBase / profitStreak | 24 each | = | **exactly 2 names: NSLNISP 19, CELLO 5** |
| delivPct | 7 | = | **7 names, one cell each**: GVT&D, HEMIPROP, HGS, PATANJALI, PTCIL, RELINFRA, TTML |
| rev | 1 | = | NSLNISP @ 2023-04-28 |

**Five corrections to this plan, all measured this session — do not re-derive:**

1. **NEVER commit a locally-baked payload.** `--bin auto` downloads the release asset, which lags
   CI's freshly-appended bin: local bake ended **2026-08-12** while the committed payload from CI's
   01:26 run ended **2026-08-14**. Committing the local bake would have regressed the page by two
   days. Local bakes are for ANALYSIS ONLY; let CI (nightly 01:10 + `workflow_dispatch`) write the
   payload. **P1/P3/P5's "bake → push" means push CODE/LEDGERS, then dispatch the workflow.**
2. **8 of the 12 gap params have NO `na` rule at all** — `ebit`, `op`, `rev`, `profitTTM`,
   `composite`, `profitAccel`, `delivPct`. Only fiiPct/diiPct/fiiChgPp/diiChgPp (first-filing),
   profitYoyPct/profitBase/profitStreak (oldest-row), and postDrift (first-bar) have one. This is
   the whole of P1's surface, and it is larger than §4 P1 implied — §4 P1 named only the revenue
   family.
3. **`na = -1` is the no-roll sentinel, not a negative count** (it sits at dates where `members`
   and `count` are also −1, which `cell()` renders as "–"). Sum an `na` array naively and postDrift
   reads "−4 cells". Not a defect — do not "fix" it.
4. **`__norow` = 0.** Every Nifty-500 roll member in 2020+ had a `factorsAt` row, so Price is
   genuinely 100% and no gap hides behind a missing row. (The bucket stays in the tool: it is the
   guard that would catch it if that ever changed.)
5. **A full bake takes ~1.0 min, not ~5.** §4 P0's estimate was wrong; budget accordingly.

**Shape of the residue (hypotheses for P2 to test per name, NOT verdicts):** the profit-family names
are dominated by recent listings/demergers — NSLNISP, JUBLINGREA, CAMPUS, CELLO, FLUOROCHEM,
VALIANTORG, MAZDOCK, KPITTECH, SBICARD, DOMS, IGIL, MEDPLUS, POWERINDIA, SAILIFE, VMM, ABLBL,
HAPPYFORGE, ENRIN — consistent with C3, but each needs its own listing/first-row measurement.
Note the builder's existing profitYoy* na rule **deliberately does not fire for CELLO** (comment at
`build_coverage_matrix.js:461-463`: CELLO needs 2022-12 while holding rows from 2022-06), so CELLO
is a genuine C4 by the builder's own reasoning while NSLNISP may be C3 — **the same param, two
different classes, which is exactly why per-name adjudication is mandatory.**

> **Golden rule (standing, from the user):** never assume, never guess. Every value written and
> every claim made ("exists", "absent", "fixed", "live", "works") must trace to something measured
> or read this session. Don't know it? Go measure it. Can't measure it? Say "unknown".

**Two binding constraints the user added for THIS campaign (2026-08-16):**
1. **"dont assume"** — no class rule may be applied by category. A bank is not "structurally
   EBIT-free" because it is a bank; it is structurally EBIT-free when a real filing of ITS OWN has
   been read and lacks the line. The measured warning shot: SPICEJET — an airline — sits in the
   "ebit in no quarter" set (32 member-dates). A sector shortcut would have buried a real gap.
2. **"do not miss any. work on every single stock"** — the work queue is EXHAUSTIVE. Every symbol
   behind every sub-100 cell is enumerated, adjudicated or filled, and tracked to resolution.
   No top-N, no sampling, no "representative examples". Per-param queue totals must reconcile to
   the payload's missing counts EXACTLY (parity gates below).

---

## 0. Read before any work

- `CLAUDE.md` (concurrency contract) and `scripts/DATA_RUNBOOK.md` — §92 (this page's architecture),
  §39 (ship gate), §38 (concurrency history), §57/§63 (fundamentals fill routes), §91c (ann=0
  sentinel), §99 (pre-listing ann floor), §17b (vision-fill), §22k (event-SHP rows).
- Memory: `project-stocks-n500-coverage-2020-residue` (baseline facts, this campaign's origin),
  `project-stocks-coverage-matrix-page`.
- **In-flight collision risk:** `scripts/PLAN_SHP_4DP_FULL.md` (commit 42a7895c, 2026-08-16 12:49)
  is a live SHP-wide campaign owned by another session. Phase 4 here touches SHP ledgers — before
  starting Phase 4, check that plan's status line and coordinate; its `cell_fix`-outranks semantics
  apply. Do NOT run both SHP efforts concurrently.
- **This campaign runs in its own worktree** (bakes >5 min, many-file loops):
  `git worktree add --detach ~/stocks-wt/n500-cov origin/main`. Push via the CLAUDE.md retry recipe.
  One interactive session owns this campaign at a time.

## 1. Goal & done-criteria

Universe **nifty-500** on `docs/coverage.html`, month-ends **2020-01-31 → date**.

- **T1 (hard):** every one of the 43 parameters reads **exactly 100%** (PCT mode) on every month-end
  2020-01-31 → 2026-06-30 on the **LIVE** page, where the denominator excludes only cells carrying
  an **evidence-backed N/A** (per-name evidence recorded in the queue file — a filing read, a code
  line cited, or a listing-date measurement; never a category).
- **T2 (live edge):** month-ends after 2026-06-30 (currently 2026-07-31, 2026-08-14) are results- and
  SHP-season dependent. Track them in the queue until each reads 100% on a post-season bake; any name
  still missing after the season's bakes stop moving becomes an ordinary fill/adjudication target.
  Do not assume a deadline heals them — verify on the page.
- Page amber flags in-window: after Phase 1, the three 2022 `half` flags on fii/dii must be gone
  (they are false alarms on a deliberate suppression — see §3 C2); `roll` flags on the live edge are
  expected and stay.

**Never hand-edit `docs/coverage/*.json`** — derived, nightly-rebuilt (01:10 IST). Heal via ledgers +
builder/code changes only. **Never commit a partial bake** (`--from`/`--dates` args are for local
analysis only; a committed payload must be a full 2002→date bake).

## 2. Baseline (measured 2026-08-16 off payload `2026-08-16 01:26 IST`, dataEnd 2026-08-14 — STALE BY DESIGN, re-baseline in Phase 0)

Window at baseline: 80 month-ends × ~500 = 39,984 member-dates. Page math replicated exactly
(`count / (members − na)`); the reproduction's parity proof: ebit classes summed to the payload's
missing count to the cell (2,819 + 585 = 3,404 ✓).

**31 of 43 params are already exactly 100% on all 80 month-ends** (whole price/52w/momentum/trend/
risk/oscillator/liquidity block + postDrift, fiiPct, diiPct, industry). Do not touch them; Phase 6
re-verifies they STAYED 100%.

The 12 that are not, with baseline missing counts:

| param | cov% | missing | notes (measured) |
|---|---|---|---|
| ebit | 91.487% | 3,404 | 2,819 across 45 never-has-ebit names + 585 across 95 has-ebit-somewhere names (full lists §8) |
| fiiChgPp | 95.808% | 1,676 | 1,499 = the three SEBI months (Oct/Nov/Dec-2022, engine-suppressed by design) + 177 residue over 48 month-ends, ≤13/month, Mar-heavy |
| diiChgPp | 95.808% | 1,676 | identical cells to fiiChgPp |
| profitTTM | 98.689% | 524 | worst month-ends 2022-03 (−19), 2025-03/04 (−17), 2026-03 (−17) — Mar-clustered |
| composite | 98.689% | 524 | NOT independent: null exactly where profitTTM is null; heals with it |
| op | 99.005% | 398 | includes ABBOTINDIA-class rows: rev present, op+ebit null (ABBOTINDIA from 2025-12 on) |
| profitAccel | 99.747% | 101 | needs quarters t, t−1, t−4, t−5 |
| profitYoyPct | 99.940% | 24 | 19 month-ends 2023-04→2024-10, 1–2 names — names not yet identified |
| profitBase | 99.940% | 24 | same cells as profitYoyPct |
| profitStreak | 99.940% | 24 | same cells |
| delivPct | 99.982% | 7 | 5 month-ends (2021-03, 2021-09, 2022-09, 2024-09, 2025-09), 1–2 names — names not yet identified (needs the big bin) |
| rev | 99.997% | 1 | NSLNISP @ 2023-04-28 — demerger listing, first-ever filing dated 2023-05-23 (measured; see §3 C3) |

**Why re-baseline is mandatory:** commits 6e3d2dc8 + 42a7895c (event-SHP backfill, 2,648 filings)
landed 2026-08-16 12:40–12:49, AFTER this payload's 01:26 bake. fii/dii numbers above WILL move.

## 3. Cell classes — every missing cell gets exactly one, with per-name evidence

- **C1 REPORT-ABSENT → na.** The filer's own P&L format has no such line. Evidence required PER NAME:
  one real filing (BSE/NSE PDF or XBRL) read for that name showing the format. Two readers for the
  negative claim (§60f mirror): our-data-all-null is reader 1, the filing is reader 2. Candidate
  list = §8a (45 names). Expected mostly banks/insurers — but SPICEJET is in that list, so expect
  exceptions: any candidate whose filing DOES carry the line moves to C4 (extraction/ledger gap).
- **C2 DESIGN-SUPPRESSED → na.** fiiChgPp/diiChgPp where the row's visible SHP quarter is 20220930:
  the engine deliberately never computes QoQ across the Sep-2022 SEBI reclassification —
  `docs/backtest-engine.js:689`, `if (cur[0] !== 20220930)`. Code is the evidence; no per-name work.
- **C3 PRE-HISTORY → na.** Point-in-time impossibility measured from the symbol's own dates: first
  traded bar / first filing vs the parameter's window (TTM needs 8 visible quarters; ChgPp needs a
  prior-quarter filing; YoY needs t−4; delivPct needs a bar with delivery in the trailing 20d).
  Worked case: NSLNISP above. IPO/demerger listings joining N500 before their history can exist.
  Evidence = the measured dates, recorded per name. Ann-floor rules of §99 apply — a pre-listing
  quarter made "visible" by a fabricated ann date is a look-ahead, never a fill target.
- **C4 OUR-DATA HOLE → FILL.** The value exists publicly but is absent/undated/null in our files.
  Includes: stray/wrong cells to retract (INDIANB 20180630 ebit_con=383.28 == op_con — a copy
  artifact; retraction must annotate EVERY ledger per `feedback-retraction-needs-every-ledger`),
  alias shadowing (DHFL→PIRAMALFIN: 3 ebit quarters live under the alias key but the direct key
  wins — the `revopFor` `||` never merges), and plain unextracted values (ABBOTINDIA op/ebit
  2025-12→). Route per family in §6.
- **C5 NOT-PUBLIC-THEN.** After C4 is exhausted: proof (two independent sources empty for the
  period) that nothing was public at that date. **Decision checkpoint** (§7) before any na here.

**Parity gates (hard stops):** after classification, per param:
Σ(C1..C5 cells) == payload missing count, EXACTLY. At baseline: ebit 2,819+585=3,404 ✓;
chgPp 1,499+177=1,676 ✓. A ledger that doesn't move the re-baked count did nothing
(`feedback-ledger-guard-count-must-move`); a heal must be run TWICE — second pass changes 0 cells.

## 4. Phases

**P0 — Re-baseline + exhaustive queue (no data edits).**
1. Worktree, fetch, full local bake:
   `node --max-old-space-size=12288 scripts/build_coverage_matrix.js --bin auto --out docs/coverage`
   (~5 min local / 2m25s CI; `--bin auto` downloads the LIVE release asset — the committed bin is a
   frozen stub, never analyze against it).
2. Build `scripts/n500_cov_cells.py` (new, committed): reproduces the page math per cell for ALL
   12+ gap params and emits **`scripts/n500_cov_queue.json`** — one row per (param, symbol) with
   month-end list, proposed class, evidence field (empty until adjudicated), status
   (`open|adjudicated-naX|fill-pending|filled|verified`). Revenue family + SHP are locally
   reproducible (needs only `dash_slim.bin`, `sf_revop.json`, `sf_fundamentals.json`, FUND_ALIAS —
   parse from `docs/backtest-engine.js`); engine families (profit*, delivPct) need the engine: add a
   `--explain nifty-500,<param>,<from>` mode to `build_coverage_matrix.js` that dumps per-date
   missing symbols through the SAME vm run (measure THROUGH the engine — §92's core rule; NEVER
   re-implement factorsAt in python).
3. Assert every parity gate against the fresh payload. Commit tool + queue.

**P1 — N/A wiring (code, no data edits).** Builder + page:
- `build_coverage_matrix.js`: revenue-family na path does not exist today (na is only wired for
  engine params at :442-476; the rv counting at :523 has none) — add it. C2 na for chgPp via the
  vm's own `shpAt` (visible quarter == 20220930 → na). C3 na rules per param from measured dates.
  **Every na rule's text goes into the param's `rule` string** so the page tooltip documents it.
- na additions apply ONLY to queue rows whose class is adjudicated with evidence (P2 feeds this;
  wire C2 immediately — its evidence is the code line — plus any C1/C3 rows already adjudicated).
- `docs/coverage.html`: an all-na cell today renders "0" (`cell()` at :346: den 0, count 0 → r=0) —
  the three SEBI months would read as zeros, not as excluded. Render den==0-with-na>0 as the "–"
  no-roll style. **Full §39 gate on the page change** (console-clean, mobile 375px, dark+light).
- Full bake, verify moved cells, push code+payload, **verify LIVE ~20 min later** (nightly races).

**P2 — Adjudicate EVERY name (no data edits until classed).**
- §8a's 45 candidates: read one filing each → C1 (evidence noted) or C4. No category verdicts.
- §8b's 95 fillable names: for each, locate the missing quarters (tool emits them), decide
  stray-retract vs fill vs edge-tracking. The 2-cell block at the bottom of §8b is almost entirely
  the 2026-07/08 live edge (fillable count jumps 2→36→51 across the last three month-ends) — class
  them T2-edge, keep them in the queue, re-verify post-season.
- profit*/delivPct names from `--explain`: same treatment. delivPct Sep-clustering and profitTTM
  Mar-clustering look like reconstitution-adds-recent-IPOs (C3) — **that is a hypothesis; the
  listing-date measurement per name is what classes it.**
- Queue after P2: zero rows with empty evidence.

**P3 — Revenue-family fills (ebit/op/rev C4 rows).** One name at a time, ledger-routed
(`revop_fundamentals.json` et al. per RUNBOOK §57/§63 — read the § before the first edit; do not
improvise ledger formats). Sources in the documented order: NSE archive first, BSE PDFs +
vision-fill (§17b), aggregators MC/TL/TT last with their defect rules (con-fallback,
identical-is-unresolved, two-revenue-definitions, never-hardcode-the-row). Provenance on every cell.
Aggregator EBIT usually needs derivation (OP ± other income/D&A per the site's definition) — verify
against one PDF per name before trusting a derived series. Batch per ~10 names: rebuild → bake →
parity → push → LIVE check.

**P4 — SHP residue (chgPp C4 rows).** ONLY after coordinating with `PLAN_SHP_4DP_FULL.md` (§0).
Missing prior-quarter filings: Wayback route reaches 2010; silent-API-filter-flip and 50-row-cap
traps apply. First-SHP-quarter IPO rows are C3, not fills.

**P5 — Profit-family + delivPct + rev fills (C4 rows).** PAT quarters/ann-dates via
`feed_qe_fix.json` / `ann_date_fills.json` ledgers; ann dates must be REAL announce dates (the
15:30 gate is nightly; a fabricated date regrows as a look-ahead). delivPct via the MTO
volume-identity procedure (§88b), never by symbol. composite needs no work — verify it tracks
profitTTM to zero.

**P6 — Close-out.** Final full bake; assert T1 (all 43 × all month-ends ≤2026-06-30 at 100% LIVE,
via a scripted check against the ORIGIN payload, not local files); T2 rows either 100% or
re-queued; all 31 already-clean params STILL 100%; flags as specified in §1. Update
`project-stocks-n500-coverage-2020-residue` → CLOSED + final numbers; add new procedures to the
RUNBOOK (new §: ebit-na rule + this campaign's pattern); set this doc's status line to CLOSED.

## 5. Queue file contract (`scripts/n500_cov_queue.json`)

Committed, single source of progress truth, updated at every step. Row:
`{param, symbol, months:[...], class:"C1..C5|T2-edge", evidence:"", status:"", ledger:"", note:""}`.
Rules: no row deleted, ever (resolved rows keep their evidence); counts reconcile to the CURRENT
payload per param on every bake (a script asserts this — `n500_cov_cells.py --check`); "every
single stock" = zero rows in `open` at P2 exit, zero rows short of `verified`/`T2-edge` at P6.

## 6. Ledger map (route fixes through these, never derived files)

| family | derived (never edit) | ledger route | rebuild |
|---|---|---|---|
| rev/op/ebit | sf_revop.json | per RUNBOOK §57/§63 (revop fundamentals ledgers) | nightly CI or documented script |
| PAT/ann dates | sf_fundamentals.json | feed_qe_fix.json / ann_date_fills.json | same |
| SHP | (per PLAN_SHP_4DP_FULL.md) | that plan's ledgers; cell_fix outranks | same |
| delivery | big bin | MTO identity procedure §88b | same |
| coverage page | docs/coverage/*.json | — none — code+data only | builder |

## 7. Decision checkpoints (ask the user, with these recommendations)

1. **C5 policy** (only if P2 finds any): recommend keeping C5 visibly sub-100 (it is real screening
   degradation a backtest would feel), unless the user prefers strict-100 via an
   "unfilable-not-public-then" na with tooltip. Ask with measured counts in hand.
2. **All-na cell rendering** (P1): recommend the "–" style + tooltip carrying the na count. Confirm
   the user likes the look before pushing (it changes three visible 2022 cells sitewide-private).

## 8. Complete name lists (baseline; P0 re-emits and supersedes)

**8a — ebit in NO quarter, 45 names (candidate C1, each needs a filing read):**
80 month-ends each: AUBANK, AXISBANK, BANDHANBNK, BANKBARODA, BANKINDIA, CANBK, CENTRALBK, CUB,
FEDERALBNK, GICRE, HDFCBANK, HDFCLIFE, ICICIBANK, ICICIGI, ICICIPRULI, IDBI, IDFCFIRSTB,
INDUSINDBK, IOB, KARURVYSYA, KOTAKBANK, MAHABANK, NIACL, PNB, RBLBANK, SBILIFE, SBIN, UNIONBANK ·
then UCOBANK 77, YESBANK 72, STARHEALTH 54, CSBBANK 51, LICI 49, J&KBANK 44, EQUITASBNK 42,
UJJIVANSFB 39, **SPICEJET 32 (airline — expected C4, the proof no shortcut is safe)**, DCBBANK 26,
GODIGIT 24, NIVABUPA 18, KTKBANK 14, SOUTHBANK 14, TMB 12, CANHLIFE 6, LAKSHVILAS 5.

**8b — ebit somewhere but missing at member-dates, 95 names (candidate C4/edge/stray):**
INDIANB 80 (**stray: single quarter 20180630 has ebit_con == op_con == 383.28 — retract-candidate,
then reclass to 8a**), ABBOTINDIA 68, SAMMAANCAP 38, WESTLIFE 35, SHRIRAMFIN 27, 360ONE 26,
SWANCORP 18, ABREL 16, BOSCH-HCIL 16, ZFCVINDIA 16, EMBDL 11, UNOMINDA 9, ANGELONE 8, ARE&M 8,
CCAVENUE 8, IRFC 8, BAYERCROP 7, POONAWALLA 7, IDEA 6, UNITDSPR 6, DHANI 5, ETERNAL 5, HUHTAMAKI 5,
INDOSTAR 5, KENNAMET 5, SUNDARMFIN 5, CEMPRO 4, IIFL 4, TATAELXSI 4, AEGISLOG 3, EDELWEISS 3,
GMRAIRPORT 3, HLEGLAS 3, HSCL 3, JSL 3, KPIL 3, M&M 3, PAGEIND 3, SIEMENS 3, SKFINDIA 3,
TATAPOWER 3, WELCORP 3, then at 2 each: AAVAS, ABCAPITAL, ABSLAMC, BAJAJHFL, BAJFINANCE,
CANFINHOME, CGCL, CHOLAFIN, CREDITACC, FIVESTAR, HDBFS, HDFCAMC, HOMEFIRST, HUDCO, ICICIAMC,
JIOFIN, LICHSGFIN, LTF, M&MFIN, MOTILALOFS, MUTHOOTFIN, NAM-INDIA, NUVAMA, PIRAMALFIN, POLICYBZR,
RECLTD, SBFC, SBICARD, SUNDRMFAST, SUNTECK, SYRMA, TATACAP, UTIAMC; at 1 each: AADHARHFC,
AARTIIND, APTUS, BAJAJFINSV, BAJAJHLDNG, **DHFL (alias-shadow: 3 quarters under PIRAMALFIN key)**,
GMDCLTD, IFCI, IREDA, JMFINANCIL, MANAPPURAM, MASFIN, MFSL, NSLNISP, PFC, PNBHOUSING, RAJESHEXPO,
RELCAPITAL, TATAINVEST, VIYASH. (The 2-cell block is dominated by the 2026-07/08 live edge.)

**8c — other params:** symbol lists NOT yet identified (needs P0 `--explain`): profitTTM 524,
op 398, profitAccel 101, profitYoy/Base/Streak 24, delivPct 7, chgPp residue 177 (post-re-baseline).
rev 1 = NSLNISP (identified). "Every single stock" applies to these exactly as to 8a/8b.

## 9. Code cites (verified this session)

`docs/backtest-engine.js:689` SEBI suppression · `scripts/build_coverage_matrix.js:399` revopIdx
`{rev:[1,0], op:[3,2], ebit:[8,7]}` ([con,std]) · :494-506 revenue perRow · :523 rv counting (no na
path) · :442-476 existing na paths · :550-589 flag heuristic (`half` = below half neighbourhood
median — cannot see intent, hence the 2022 false alarms) · `docs/coverage.html:338` naAt,
:346-356 cell() (den==0 → renders "0") · builder loads the engine in a Node vm; `let` globals need
in-context assignment (§92).
