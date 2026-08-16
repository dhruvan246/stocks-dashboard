# PLAN — N500 COVERAGE 2015→2020: same method, the earlier era, ALL 43 parameters

**Written 2026-08-16 15:45 IST; findings ledger added 16:05 IST. Executor: Fable, session started 2026-08-16 15:25 IST.**
**Status 2026-08-16 17:40 IST: P0 DONE (§0b) · basis columns LIVE (§0c) · con gap BOUNDED (§0d) · industry pass 43 names (§0e).
NEXT: the 2,631-cell con fill of §0d, then P1 in-era N/A verification.**

> **Deploy note (§F6, re-confirmed today):** GitHub Pages does NOT redeploy on CI's own payload
> commits. The 16:29 payload went live only because a CODE push happened to follow it; the gated
> 16:33 bake then sat committed-but-not-served until this doc commit carried it. **After any
> `refresh-coverage.yml` dispatch, push something to main — or the live page keeps serving the
> previous bake, and a `?cb=` cache-buster is needed to see through the Pages CDN either way.**

> ⚠️ **F19 DECIDED-DEFERRED (user, 2026-08-16 15:30 IST): ALL ebit work is ON HOLD.** Verbatim: *"do
> other params first. EBIT part other session will let u know later. keep that on hold."* Until that
> word arrives: no ebit fills, no NBFC decision assumed either way, no INDIANB retraction (F21), no
> banking second-reader ledger writes (F22), no coverage_na_ledger ebit edits. ebit rows still get
> MEASURED and queued (analysis); only writes are held. Every other parameter proceeds.
Parent method: `scripts/N500_COVERAGE_100_CAMPAIGN.md` (2020→date; P0–P2 done, live at 2,252
missing). Sibling: `scripts/PLAN_XBRL_FILER_FORMAT.md` (Phase A done; its verdicts feed this).

> **Golden rule:** never assume, never guess — every claim traces to a measurement or a read.
> **User constraints (binding, restated for this era):** *"dont assume"* — no verdict by category;
> *"do not miss any. work on every single stock"*; and **2026-08-16: "not just those 12 — other
> cells which were filled for 2020-26 might be empty before 2020"** — scope is ALL 43 parameters,
> never the 2020+ residue list.

## 0b. P0 RESULTS — measured 2026-08-16 15:30–16:15 IST. Supersedes §2 where they differ.

**Worktree:** `~/stocks-wt/n500-cov-2015` (fresh). ⚠️ `~/stocks-wt/n500-cov` is NOT free — the
parent-campaign session owns it and was committing from it at 15:32 (`43d43e6f`). This session
baked into it before noticing, then restored it file-scoped to clean; nothing of theirs was lost
(every dirty file was this session's own bake, mtime 15:36).

**The baseline moved before work started.** The peer session's lender-ebit N/A (`43d43e6f`, user
decision "show nbfc and banks like screener do") landed mid-session and CI re-baked at 15:36
(`66ef9378`). In-era effect: **ebit 20,840 → 19,923 missing (26.857% → 27.750%)**; total
**37,789 → 36,872**. The other 42 params are unchanged; §2's table is otherwise still exact.

**Queue: `scripts/n500_cov_queue_2015.json` — 2,627 rows · 36,356 named cells · PARITY PASS on all
43 params, per-(param,date), against CI's COMMITTED payload** (`2026-08-16 15:36 IST`, dataEnd
08-14) — not a local bake, so F5 holds by construction and the queue reconciles to what the live
page serves. (36,872 − 36,356 = 516 = the 12 `__norow` cells counted against all 43 params.)
Tools now take `--from/--to/--campaign` (`n500_cov_cells.py`), `--queue/--facts`
(`n500_cov_adjudicate.py`), `--explain-to` (builder). Defaults keep 2020→date behaviour identical;
`--check` reads the window from the queue file, so an era queue cannot be silently re-windowed.

**First adjudication pass (P2 head start):** 1,377 cells C3 pre-history (measured), 34,991
`needs-source`, 262 rows closed. No C1 appears in-era because ledger names are already excluded by
the bake — correct, not a miss.

### P0's four questions, answered by measurement

**(a) The 12 price cells are 4 names and THREE different mechanisms — not one problem.**

| name | cells | measured mechanism | verdict |
|---|---|---|---|
| HUDCO 2017-05-31 · DALBHARAT 2019-01-31 | 2 | listed 12 / 9 days before the month-end; `factorsAt` needs `p0 = priceAt(off−30)`, and the stock did not exist then → row skipped entirely | **C3, correct refusal.** A 1-month change cannot exist 9 days after listing. Not fillable. |
| KIOCL 2017-01-31, 2017-02-28 | 2 | last bar 56d and 19d stale → fails the engine's 14-day entry-freshness gate; only 3 bars within 45d of 2017-01-31 | needs a source read: real suspension vs missing bars (§88 era-floor) |
| RASOYPR 2015-01→2015-08 | 8 | **close/high/low/open/vwap all exactly 0.00 on all 246 bars of 2015 while volume and turnover are real** (96.7M shares, ₹441 lakh in one day) → engine's `p0 <= 0` skips the row | **C4 real defect.** True price recoverable from turnover÷volume: 286,300 ÷ 190,867 = **₹1.50** at 2015-01-01. |

⚠️ **New defect class, bounded bin-wide: 115 symbols / 43,601 bars carry close==0 with v>0 and
t>0** (VISESHINFO 4,041 · ANTGRAPHIC 3,486 · BLUECHIP 3,455 · KSERASERA 3,282 · SUJANAUNI 2,902 …);
**15 symbols overlap 2015-2019.** Only RASOYPR is an N500 member in-window, but the class degrades
`all`/`liquid` and any backtest screening them. t÷v is the free second reader
([[feedback-turnover-step-reads-adjustment]]).

**(b) `op`'s non-monotonic shape is an EXTRACTION-CAMPAIGN shape, not a source-reach shape.**
Fill rate of sf_revop's op slot **per quarter**, over every symbol holding that quarter:

| quarter block | symbols/qtr | op% | ebit% |
|---|---|---|---|
| 2013Q1–2014Q4 | ~490 | **10–13%** | 0.0% |
| 20150331–20161231 | 519–604 | **71–93%** | ~0% |
| **20170331–20171231** | 575–815 | **19–49%** | 0.7–32% |
| 20180331 onward | **1,523–1,819** | **93–99%** | 86–96% |

The 2018 step is `build_revop.py`'s `MIN_QE = 20180101` **and** the cache's own reach: a read-only
census of all 104,331 cached filings (using build_revop's own regexes; 100% of filenames parsed,
0 errors) finds submissions only from 2018 on. **Pre-2018 the cache holds just 318 (symbol,quarter)
pairs across 5 QEs — 285 of them 20170331** (276 carrying all five industrial tags), then
20170630: 20 · 20170930: 4 · 20171231: 8 · 20161231: 1. ⇒ **lowering MIN_QE recovers Mar-2017 for
~285 names and essentially nothing else; Jun/Sep/Dec-2017 need a network route.**
The 2017 cells are not empty rows: `revop_fundamentals.json` (reaches 2002; written by ~20 campaign
scripts, not just build_revop) holds 2,541 quarters for 2017 with **rev ✓ and PAT ✓ but `op` =
None** — GRINDWELL, SJVN and TIMKEN all show exactly that across the FY2017-18 quarters. So 2015-16
op came from earlier backfill campaigns and **2017 got a rev/PAT-only pass with no op pass**: one
column left unextracted from filings already read, which is why the year sags between two healthy
neighbours. Route: whatever supplied rev/PAT for those same (name, quarter) cells.
⚠️ The peer session reported the XBRL cache "absent on this Mac". It is **present** — 104,331 files
at `/Users/dhruvan/stocks-dashboard/scripts/_xbrl_cache`, untracked, so it exists only in the MAIN
checkout and appears in no worktree. Corrected to them 2026-08-16.

**(c) `industry` is SURVIVORSHIP, not classification.** `build_sf_data.py:549-551` sets the bin's
`meta.ind` from `cur` — the **currently-listed** universe — so every delisted era member falls
through to `"Unknown"`, which `build_coverage_matrix.js:542` counts as not-covered. **2,008 of the
live bin's 4,445 symbols carry this gap.** The route exists and is one-directional (never
overwrites a live classification): `scripts/industry_fills.json` — BSE `IndustryNew` via
ComHeadernew, gated `isin-exact + bse-isin-confirmed`, applied at `update_sf_data.py:1014-1022`,
**26 entries today**. Extending it per delisted era member is P4's route. The monotonic year shape
(88.4% → 97.6%) is exactly what a survivorship gap predicts.

**(d) The 441 `rev` cells (144 names; 259 in 2016) are at least TWO defect classes.**
- **Our-data quarter boundary / unextracted rev** — GRINDWELL's oldest fundamentals row is 20160331
  (ann 20160515) with rev null; its first usable rev is 20160630 (ann 20160922), exactly where its
  5 missing months end. SJVN is identical. Many names share the *same* window
  2016-04-29..2016-08-31, so this is one systemic boundary, not 144 stories.
- **Announce-date copy artifacts** — KRBL holds rev for 20150930 and 20151231, but those quarters
  carry ann dates **20161114 and 20170214** — the *2016* quarters' dates (20160930→20161114 and
  20161231→20170214 each appear twice). The 2015 rows are stamped a year late, so real revenue is
  invisible for 11 month-ends. Heal via `ann_date_fills.json` with REAL announce dates only
  (§99 — a fabricated date regrows as a look-ahead).

## 0c. REPORTING-BASIS COLUMNS (user request, 2026-08-16 16:40 IST) — 43 params → 47

User: *"add these columns on my coverage dashboard and then fill the pre-2020 con gap for rev and
pat"*, then *"dont wire NA, add empty values. NA part i'll check later"*.

New family **`basis` — "Reporting basis"**: `revCon`, `revStd`, `patCon`, `patStd`. Why it was
needed: the existing `rev` column is *consolidated ELSE standalone*, so a company that never files
consolidated still reads 100% and the con thinness is invisible; and **net profit had no column at
all** — the PAT families show only derived measures (YoY/TTM/streak/drift).

- **Own family on purpose.** A family cell is its WEAKEST parameter, so folding a ~60%-covered
  column into `revenue` would have silently redefined an existing column.
- **Each basis is visible from its OWN announce date** (con → sf_fundamentals idx4, std → idx2),
  not from the `min(annStd, annCon)` quarter the `rev` column resolves. A consolidated cell gated
  on a standalone filing that precedes it by weeks would be a look-ahead on that basis.
- **No N/A wired, per the user.** An empty cell means only "we hold no figure on that basis here".

**RAW numbers, 60 month-ends 2015-01→2019-12 vs 2020-01→date** (measured off the bake, member-dates):

| param | 2015-19 | 2020+ |
|---|---:|---:|
| rev (con-else-std, existing) | 98.530% | 99.997% |
| **revCon** | **59.95%** | 98.01% |
| revStd | 98.29% | 99.69% |
| **patCon** | **67.45%** | 98.76% |
| patStd | 99.63% | 99.95% |

### ⚠️ `scripts/no_con_filing.json` CANNOT be applied as written — measured before deciding N/A

Trialled as the N/A source, then checked against sf_fundamentals the way the ebit ledger's guard
caught INDIANB. **344 of its 760 `never_filed_con` names hold a dated con PAT, and 133 of the 200
`started_filing_con` names hold one BEFORE their declared start.** The split that resolves it is
**DIVERGENCE, not presence**:

- **326 of those 344 have `con == std` to the paisa on EVERY quarter** — the con-slot-holds-a-COPY
  defect, which is exactly what the ledger's own build test keyed on ("our stored con never
  diverges"). For these the ledger is right and our stored value is the artifact.
- **18 never-filed names genuinely diverge** — JUSTDIAL (12 divergent quarters of 35), HDBFS 6/25,
  DHARMAJ 5/6, KALIND 3/20, SILGO, BLBLIMITED, RUBYMILLS, KAYCEEI, DHINDIA… The ledger is simply
  wrong for them.
- **130 of the 133 pre-start values diverge**, typically the four FY2019 quarters from **20180630 —
  a full year before the FY2020 mandate**: AARTIIND (declared start 20190630, real con from
  20180630), AXISBANK, BANKBARODA, ASHOKLEY, ATUL, BALKRISIND, LICHSGFIN, TATAINVEST, IMAGICAA
  (12), SHRIRAMCIT (8), GMDCLTD (7)… ⇒ **`started_filing_con` is ~a year late for ~65% of its
  entries**, and any earlier campaign that used those dates to write off pre-2019 con cells as
  never-filed wrote off cells that are real.

A trial N/A pass with a divergence guard refused **3,884** verdicts. When the N/A question is
revisited, that guard is mandatory: honour a verdict only where no divergent consolidated figure of
ours already covers the quarter. With it, the era read revCon 80.887% / patCon 91.206%.

## 0d. THE PRE-2020 CON GAP IS BOUNDED — 2,631 fillable cells, not ~21,000

User: *"fill the pre-2020 con gap for rev and pat"*. Classified every missing con member-date
2015-01→2019-12 (30,007 member-dates each). "Did this company really file consolidated for quarter
Q" is answered by **DIVERGENCE, never presence** — the con slot frequently holds a copy of
standalone — so each company's real con span runs from its first to its last quarter with
`|con − std| > 0.005` and a dated con announce.

| class | revCon | patCon |
|---|---:|---:|
| covered | 18,157 (60.51%) | 20,921 (69.72%) |
| **before-first-con** — no consolidated filing existed yet (§51a) | **8,522 (28.40%)** | **8,258 (27.52%)** |
| never-filed-con (no divergent quarter, ever) | 909 (3.03%) | 492 (1.64%) |
| no filing of any kind at that date | 62 (0.21%) | 62 (0.21%) |
| **★ REAL HOLE — inside the company's OWN con span** | **2,357 (7.85%) over 302 names** | **274 (0.91%) over 35 names** |

⇒ **~17,300 of the ~21,000 empty con cells are documents that never existed** (quarterly
consolidated became compulsory only from FY2020). They are not fillable by any source; they are the
N/A question the user has reserved. **The fillable target is 2,357 + 274 = 2,631 cells.**

**Route, measured — most of it needs no new filing.** Across the 302 revCon-hole names, quarters
2014-2019: **1,303 quarters hold consolidated PAT but NO consolidated revenue** (vs 3,360 holding
both). The consolidated filing exists *and we already read it* — sf_revop simply never got the
revenue column from it. Worked cases: ENIL 20140331 conPAT 21.29 / conREV None; PRAJIND 20140331
conPAT 20.77 / conREV None. This is the [[feedback-two-files-one-quantity]] class, the same shape as
the 2017 `op` hole in §0b(b) — one column unextracted from a filing already in hand.
Worst names: ICICIBANK 52 cells, ESCORTS 50, SBIN 44, LTF 40, RTNPOWER 33, M&M 30, HFCL 29, IDFC 27.
patCon holes are 35 names at exactly 12 cells each — annual-cadence con filers, where one missing
annual consolidated costs a full 12 month-ends of visibility.

**Fill order:** (1) the 1,303 PAT-anchored quarters — re-read the same filing for revenue, §57
ladder (NSE archive → BSE detres → aggregator), provenance per cell, ledger-routed via
`revop_fundamentals.json`; (2) the residue where neither is present (1,921 quarters) needs a filing
discovery pass first. Batch ~10 names → rebuild → bake → parity → push → LIVE.

## 0e. revStd / patStd → 100% (user, 2026-08-16 17:45 IST, scoped: *"u just handle ur years i.e. 2015-20"*)

Baseline in-era: **revStd 625 missing (97.919%) · patStd 76 (99.753%)**. Every missing cell
classified by MECHANISM off a full-window `--explain` (same builder run that counts them; the
classification reconciles to the payload exactly, 625 and 76):

| class | revStd | patStd | meaning |
|---|---:|---:|---|
| **SLOT-NULL** | **551** | **0** | a dated std quarter IS visible; sf_revop's slot 0 is null. 244 distinct (sym, quarter) pairs |
| **DATASET-START** | 47 | 47 | the company was TRADING, but our fundamentals hold no dated std quarter yet — the prior quarter is missing from our data entirely |
| **ANN-ZERO** | 17 | 17 | quarter + PAT present, `ann == 0` sentinel → invisible to every point-in-time screen |
| price-defect (`__norow`) | 9 | 11 | RASOYPR/KIOCL et al, §0b(a) — not an earnings problem |
| PRE-HISTORY | 2 | 2 | CERA |

★ **patStd has ZERO missing VALUES.** Every one of its 76 cells is a *visibility* failure — an
absent or zeroed announce date, or a quarter our dataset never got. Nothing to re-extract.

### ANN-ZERO closed: 19 REAL declared dates recovered, 22 cells healed

`scripts/backfill_ann_dates_bse.py --only …` (existing tool, BSE archive metadata, `exact`/`seq`
rules — never a guessed date). All 21 target pairs were **never previously attempted**; every one is
a **2017 quarter**, the same 2017 pass that wrote PAT and no announce date which §0b(b) found for
`op`. Recovered 13 first run, then **6 more after breaking the `no-scrip` wall**, measured effect
**revStd −11, patStd −11, postDrift −2**. Heal is idempotent: `--reapply` second pass applied 0.

★ **`no-scrip` was a missing IDENTITY, not a missing filing** — and it is the survivorship gap
again. `scrip_map()` merges `bse_scrips.json.by_id` + `bse_universe.json`, both built from BSE's
**ACTIVE**-equity scrape, so JSLHISAR / SPTL / UNITEDBNK (all merged or delisted) resolved to
nothing and every quarter of theirs skipped. New **`scripts/bse_scrips_delisted.json`** carries
their codes, each gated on an **exact ISIN match** against BSE's all-status master, merged with
`setdefault` before `BR.guard_map` so a live answer always wins and §76's conflict guard keeps the
final say. Unblocked 6 of the 10 skips immediately. Residual skips carry named reasons
(`other-period` ×2 — the only period-stated filing was a different quarter, correctly refused).

### What still stands between here and 100%

1. **revStd 551 cells / 244 (sym, quarter) pairs** — revenue never extracted into sf_revop slot 0
   although the filing is dated and visible. Worst: KRBL 20150331 (11 cells), INTELLECT 20150930
   (10), SPTL 20180630 (6), then a large 20160331 cohort (SNOWMAN, DALMIABHA, GRANULES, GRINDWELL,
   GUJENERGY, JINDALPOLY, JMTAUTOLTD, KWALITY, MANAPPURAM… 4 cells each). Same re-extraction class
   as §0d's con work — a filing we already hold a date for.
2. **DATASET-START 47 cells / 25 names** — needs the PRIOR quarter backfilled, not a re-read:
   KPRMILL / NATCOPHARM / VIVIDHA / APARINDS / JKCEMENT / MBLINFRA / NITINFIRE / PCBL / RATNAMANI /
   SUVEN / AVANTIFEED all have their oldest row at **20150331**, i.e. our series simply starts at
   the window's edge; CERA 20160630, MAXINDIA 20170331 (demerger), ASTERDM 20170630, SHANKARA
   20170331, VARROC 20170630, ADANIGREEN 20170930. Pre-2015 campaign territory.
3. **price-defect 9–11 cells** — RASOYPR's zero-close series and KIOCL's stale bars (§0b(a)).
4. **CERA 2 cells** — pre-history.

## 1. Scope & goal

Universe **nifty-500**, month-ends **2015-01-30 → 2019-12-31 (60)**, all 43 parameters.
Done = every parameter reads exactly 100% on the LIVE page for the window, where the denominator
excludes only evidence-backed N/A — same standard as the parent campaign. `fiiChgPp`/`diiChgPp`
fills stay with `PLAN_SHP_4DP_FULL.md` (coordinate, don't collide).

## 2. Baseline — measured 2026-08-16 off origin payload `2026-08-16 14:43 IST`. The user's point, confirmed: **ALL 43 params are sub-100 here**, including 31 that are perfect in 2020+.

**Group A — 100% in 2020→26, NOT 100% in 2015–19 (31 params):**

| param(s) | missing | note |
|---|---|---|
| whole technical block: price, chg, d52, d52_low_pct, rangePos, daysHigh, ret1m/3m/6m/12m, accel, riskMom, rsNifty, dma50, dma200, indRank, vol, mdd6, upPct, stoch, bollB, turnover, turnSurge, volSurge | 12 each | the SAME 12 member-dates: **one stock at a time with no tradeable bar** — 8 consecutive months in 2015, 2× 2017-01/02, 2017-05, 2019-01. P0 names each stock; likely suspensions. NOT twelve separate problems. |
| beta / rsi / macd | 13 / 13 / 17 | the 12 above + tiny real tails |
| **industry** | **2,123** | 88.4% (2015) → 97.6% (2019), monotonic — era members (incl. delisted names) without a classification. The biggest Group-A item. |
| postDrift | 74 | |
| fiiPct / diiPct | 12 each | |

**Group B — sub-100 in both windows (12 params):**

| param | cov% 2015-19 | missing | year shape (measured) |
|---|---|---|---|
| **ebit** | **26.857%** | **20,840** | 2015 **0.00%** · 2016 0.09% · 2017 0.74% · 2018 48.1% · 2019 85.7% |
| **op** | **68.248%** | **9,523** | 2015 65.3% · 2016 83.7% · **2017 40.6%** · 2018 60.0% · 2019 91.9% — NON-monotonic; the 2017 dip and 2018 sag are UNEXPLAINED. P0 must explain before routing. |
| fiiChgPp / diiChgPp | 97.102% | 864 each | → SHP plan's territory |
| profitTTM / composite | 97.648% | 659 each | |
| rev | 98.530% | 441 | 2016 carries 259 of it |
| profitAccel | 98.508% | 433 | |
| profitYoy/Base/Streak | 98.944% | 310 each | |
| delivPct | 99.920% | 24 | |
| **TOTAL** | | **37,789** | ~17× the 2020+ residue (2,252) |

Context already verified in-era: the banking-format `ebit` N/A ledger FIRES here (2016-06-30:
na=23), and the generalised REACH pre-history rule is date-neutral. sf_revop's quarters span
20021231→20260630 with 1,215 of 3,603 symbols holding pre-2018 rows — so "sf_revop starts 2018" is
FALSE as a file statement; what collapses pre-2018 is specifically the **op/ebit slots**
(rev 98.5% vs ebit ~0% in 2015-17). The extraction era, not the file era, is the wall.

## 3. What the sibling campaign already measured — inherit, don't re-derive

- **NSE list API** per-symbol (`?symbol=X&period=Quarterly`, warm the jar on the listing page, bare
  root 403s) returns rows back to ~2005 with `bank` B/F/N flags on pre-XBRL rows too —
  `scripts/xbrl_filer_format.json` (committed) already holds per-(symbol, QE, basis) format signals
  for 110 of the 127 sibling names.
- **The filename/flag shortcut is DEAD for writing N/A** (validate verdict): nbfc-flagged quarters
  carry ebit 89% of the time (aggregator-era fills), industrial-flagged miss it 67.7%. Only the
  bank flag is clean (1 impurity in 692 = INDIANB's known artifact). N/A needs the XBRL tags or a
  primary read, per name.
- **NBFC-ebit policy is an OPEN USER DECISION** (sibling plan §Phase-A results): 795 NBFC quarters
  hold an aggregator-derived ebit the XBRL branch refuses to produce. Fill-to-match or
  N/A-and-retract — this era's ebit work is mostly NBFC/industrial, so **that decision gates P3
  here too. Ask before filling.**
- Traps: rows:0 ≠ absence (17 zero-row names incl. every insurer + SPICEJET); ABBOTINDIA-style
  silent truncation; `xbrl` field can be literal `"-"`; date-range queries silently return 0.

## 3b. FULL FINDINGS LEDGER — everything the 2020→26 campaign learned, carried over. Cite by F-number; re-derive nothing.

- **F1** Measure THROUGH the engine: `build_coverage_matrix.js --explain <slug>` names the symbols
  behind every sub-100 cell from the SAME vm scan that writes the payload. Proven analysis-only
  (31 payloads / 429 keys byte-identical with and without).
- **F2** `--facts <path>` dumps per-symbol firstBar / oldestQe / firstSub from engine state —
  firstBar exists nowhere else locally (the 193 MB bin).
- **F3** Parity gates must be per (param, DATE), not totals — a totals-only gate passed while the
  composition was wrong.
- **F4** `n500_cov_cells.py --check` exits 2 on BAKE SKEW, naming both bakes — a queue compared
  against a different bake reports a false "PARITY FAIL" and sends you hunting phantoms.
- **F5** NEVER commit a locally-baked payload: the `--bin auto` release asset ran 2 days behind
  CI's bin (2026-08-12 vs 08-14, measured). CI bakes via `gh workflow run refresh-coverage.yml`.
- **F6** GitHub Pages does NOT redeploy on CI's own payload commits — a later code push carries it.
  Verify LIVE by fetching the origin URL; observed lag 14:24→14:43 payloads.
- **F7** N/A verdicts live in `scripts/coverage_na_ledger.json` — evidence beside each claim,
  optional `from`/`to` bounds, absent-file = no verdicts. Its guard REFUSES a name whose own data
  contradicts the verdict (that guard caught INDIANB).
- **F8** User decision (option A): banks' ebit is N/A, never derived. Our schema's EBIT is
  `Operating Profit − Depreciation` (SUNPHARMA paisa-proof 4417.67 − 738.7 = 3678.95); nobody
  publishes EBIT directly; a bank's `op` == its PPOP (HDFCBANK 30,996 tie-out).
- **F9** Only 33 of the 46 never-has-ebit names are banking-format. The 12 insurers + SPICEJET +
  LAKSHVILAS are NOT: SPICEJET is fillable (screener holds OP AND Dep, 13/13 quarters);
  **insurers' Depreciation=0 on screener is a NOT-DISCLOSED sentinel** — deriving EBIT=OP−0 was
  refused; LAKSHVILAS merged into DBS Nov-2020 → post-merger months are C3.
- **F10** `build_revop.py::metrics_for()` documents four ebit-less formats by TAG: life insurer
  (NetPremiumIncome, :236), general insurer (PremiumEarned, :250), bank (InterestEarned+PPOP,
  :261), NBFC Ind-AS (InterestEarned alone, :276); industrial formula at :280.
- **F11** Pre-history REACH rule: {YoY trio 4, profitAccel 5, profitTTM 7, composite 7} quarters
  back vs the symbol's own oldest row. The count-of-quarters version was WRONG — it classed CELLO
  N/A when the builder names CELLO the deliberate counter-example (:461-463). Split verdicts
  per MONTH: a young company crosses pre-history → real-gap partway through its run.
- **F12** The adjudicator refuses what it cannot measure: only C3 is decidable from our own data;
  everything else stays `needs-source`. Measured and assumed verdicts never share a field.
- **F13** Coverage flags: raw counts cannot tell a hole from a refusal (28 false ambers). The fix
  is an N/A-guard RELATIVE to neighbours' applicable coverage — a flat floor swallowed the
  live-edge roll flags, ratios-everywhere added 74 new ones; both rejected. Result 91→63, 0 added.
- **F14** An all-N/A cell rendered "0" (reads as total failure). Now "–" + explanatory tooltip;
  Nifty Bank's ebit is 78 straight such month-ends. §39-verified (console, 375px, dark+light).
- **F15** NSE list API mechanics: `?index=equities&symbol=X&period=Quarterly` (a date-range query
  silently returns 0); warm the cookie jar on the LISTING page (bare root 403s); the `xbrl` field
  can be the literal `"-"`; rows reach ~2005 and pre-XBRL rows still carry the `bank` flag.
- **F16** 17 names return zero rows from that API — EVERY insurer + SPICEJET, HDBFS, ICICIAMC,
  KENNAMET, PIRAMALFIN, TATACAP. Route absence, not filing absence (§57a). Insurers need §43/IRDAI
  or BSE routes.
- **F17** Silent truncation is real: ABBOTINDIA 1 row vs 31 quarters we hold; BAYERCROP 1/31;
  BAJAJHFL 2/12; IREDA 5/13. A short list is a diagnosis.
- **F18** The filename/flag shortcut is DEAD for N/A in both directions (nbfc-flagged quarters
  carry ebit 89%; industrial-flagged miss it 67.7%). Bank flag alone is clean: 1 impurity in 692.
  **Format belongs to the FILING, not the company** — BAJFINANCE flips F/N both directions.
- **F19** 795 NBFC quarters hold aggregator-derived ebit that the XBRL branch refuses to produce —
  two definitions of one series. **OPEN USER DECISION (fill-to-match vs N/A+retract) — gates ALL
  NBFC/industrial ebit work in any era. Ask before P3.**
- **F20** 2,390 industrial-flagged quarters missing ebit across 70 names (ARE&M 60, ABREL 57,
  LICHSGFIN 55, SHRIRAMFIN 55 …) are real extraction gaps — the sibling plan's fill list; many of
  those quarters fall inside THIS era.
- **F21** INDIANB 20180630 `ebit_con = op_con = 383.28` — a copy artifact, reconfirmed
  independently (the only bank-flagged quarter with ebit in 692). **It is inside THIS window:
  retract it here** (annotate EVERY ledger — `feedback-retraction-needs-every-ledger`), after
  which the ledger guard admits INDIANB to the bank N/A set.
- **F22** Second-reader debt: 30 of 33 banking ledger entries are single-reader (Moneycontrol
  blocks scripted fetches; URL codes not derivable). `scripts/xbrl_filer_format.json` (committed,
  5,844 rows, 110 names) now supplies a per-name second reader from the filings themselves —
  close the debt from it in P1.
- **F23** This era's baseline is §2 above — all 43 sub-100; 37,789 cells; ebit is an
  EXTRACTION-era wall (rev 98.5% in the same years); op's year shape non-monotonic (P0 question);
  industry 2,123; twelve single-stock price cells.
- **F24** Already verified in-era: the banking na fires (2016-06-30 na=23); REACH is date-neutral;
  DUMMY/DVR roll exclusions are in the builder.
- **F25** Process law: own files only; worktree; cherry-pick push when the shared checkout is
  dirty; heals run TWICE (2nd pass = 0); §57 ladder before any "unreachable"; §63 needs 2-3
  sources for any exclusion; §58 column-anchor reads; ann dates must be REAL announce dates —
  look-aheads REGROW nightly (§99: 5,247 pre-listing qe+45d cells still exist; add none).

## 4. Phases

**P0 — Re-baseline + full-scope queue (no data edits).**
1. Fresh worktree (`~/stocks-wt/n500-cov` exists; own it or make another). CI-fresh payload only —
   **never analyze or commit a local `--bin auto` bake as truth: the release asset lags CI by ~2
   days** (parent P0 note 1).
2. Parameterize the existing tools instead of forking: `n500_cov_cells.py` gains `--from/--to`
   (FROM is hardcoded '2020-01-01' today), same for `n500_cov_adjudicate.py`; builder `--explain`
   already takes `--explain-from` (add `--explain-to`). Emit
   `scripts/n500_cov_queue_2015.json` — separate file, same contract, ALL 43 params.
3. Parity gate per (param, date) as before. **Gate addition: the queue must contain rows for every
   Group-A param too** — a tool that silently scopes to Group B repeats the mistake the user just
   corrected.
4. Answer P0's four named questions with measurements: (a) which stock is each of the 12 price
   cells; (b) why is `op` non-monotonic (2017 40.6% < 2016 83.7%) — which source filled 2015-16 op
   and where does it stop; (c) where does `industry` come from and what fills it for delisted
   era members (locate the source in code before choosing a route); (d) per-name shape of the 441
   `rev` cells (2016-heavy — one source's hole?).

**P1 — Verify existing N/A machinery in-era; add none without evidence.** Also: retract the INDIANB stray (F21) and close the banks' second-reader debt from `xbrl_filer_format.json` (F22). Banking ledger, REACH
rule, SEBI rule (n/a here — 2022 is out of window), first-SHP rule: re-bake and confirm each fires
where it should. Any NEW class (e.g. a 2015-era filer format) needs per-name primary evidence into
`coverage_na_ledger.json` with `from`/`to` bounds — **date-bounded, because format belongs to the
filing, not the company** (BAJFINANCE flips F/N both directions).

**P2 — Adjudicate every queue row** (`--facts` boundaries + per-month C3 vs real-hole split, CELLO
precedent). Expect heavy C3 among profit-family (2015 IPO cohort) — but each name measured, never
assumed. Queue exits P2 with zero unclassified rows.

**P3 — ebit/op era fills (the 30,363-cell core).** BLOCKED FIRST on the NBFC-ebit user decision.
Then, routes in §57-ladder order for this era: BSE detres JSON (2015+, §42) → NSE archive detail
pages (2005+, both bases, §52/§53) → BSE announcement PDF + §58 column-anchor read → MC aggregator
(reaches 2015-22; con-fallback / identical-unresolved / two-revenue-definitions rules) → vision
§17b. For industrial ebit where op exists but depreciation is the missing input, the derivation
source must be verified against one filed PDF per name before a series is trusted (§58).
Ledger-routed, provenance per cell, batch ~10 names → rebuild → bake → parity → push → LIVE.

**P4 — Group-A closures.** The 12 price cells (per-name: suspension vs data hole; §88 era-floor
context); industry 2,123 (route decided by P0(c)); postDrift 74; delivPct 24 (MTO identity §88b);
fiiPct/diiPct 12 (tiny — adjudicate first, they may be first-filing C3); profit-family fills via
`feed_qe_fix.json` / `ann_date_fills.json` — **ann dates must be real announce dates; the 15:30
gate is nightly and look-aheads REGROW (§99: 5,247 pre-listing qe+45d cells still exist — do not
add to them).**

**P5 — Close-out.** Full CI bake; T1 assertion against ORIGIN for the 60 month-ends; 2020+ window
re-verified UNCHANGED (this campaign must not regress the finished one); flags reviewed (era flags
that were suppressed by healthy-neighbourhood logic may surface as coverage rises — expected, not
a defect); campaign docs + memory + runbook § updated; queue rows all `verified`/`adjudicated-na`.

## 5. Non-negotiables (unchanged from parent, restated)

Own files only; explicit `git add` paths; worktree for long jobs; push via the retry recipe (dirty
shared checkout → temp-worktree cherry-pick). Never hand-edit `docs/coverage/*.json` or commit a
local bake — push code/ledgers then `gh workflow run refresh-coverage.yml`; **Pages does not
redeploy on CI's own payload commits** — verify LIVE by fetching the origin URL. A route returning
nothing is `not-found-via:<route>`, never absence (§57). Heals run twice (2nd pass = 0). No
category verdicts — the era's SPICEJET-equivalents are in it somewhere. Every scale/sign read per
§58's column-anchor discipline; aggregator readings cross-checked against one PDF per name.

## 6. Launch

New session, then:
> Read scripts/PLAN_N500_COVERAGE_2015_2020.md and execute it, starting at Phase 0.

First user decision to fetch (can be asked immediately, it gates P3): **NBFC ebit — fill to match
the 795 existing aggregator-derived quarters, or N/A the class and retract the 795?**
