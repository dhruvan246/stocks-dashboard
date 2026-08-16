# PLAN — N500 COVERAGE 2015→2020: same method, the earlier era, ALL 43 parameters

**Written 2026-08-16 15:45 IST. Executor: a FRESH session. Status: NOT STARTED.**
Parent method: `scripts/N500_COVERAGE_100_CAMPAIGN.md` (2020→date; P0–P2 done, live at 2,252
missing). Sibling: `scripts/PLAN_XBRL_FILER_FORMAT.md` (Phase A done; its verdicts feed this).

> **Golden rule:** never assume, never guess — every claim traces to a measurement or a read.
> **User constraints (binding, restated for this era):** *"dont assume"* — no verdict by category;
> *"do not miss any. work on every single stock"*; and **2026-08-16: "not just those 12 — other
> cells which were filled for 2020-26 might be empty before 2020"** — scope is ALL 43 parameters,
> never the 2020+ residue list.

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

**P1 — Verify existing N/A machinery in-era; add none without evidence.** Banking ledger, REACH
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
