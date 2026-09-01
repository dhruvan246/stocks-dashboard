# PLAN — FULL DATA AUDIT 2026-09-01 → the fix queue for the next session

## ✅ SHIPPED so far (Opus implementation pass, 2026-09-01)

Commits on origin/main; each verified before push (Node harness on the live release asset, before/after).
- **P0-1 F&O membership alias-fold** — engine e14, CONDITIONAL (a first unconditional version was
  caught corrupting N500 — it redirected names that still hold their own era tape — and fixed to fold
  only when the roster name has no series of its own). Recovers 3,135/32,073 F&O slots; N500/all-stocks
  byte-identical.
- **P0-7 profitAccel/profitStreak calendar-step** — engine e14. Unit-proven (streak 7→2 across a gap);
  inert on gapless N500 (0 of the 500 current members change).
- **P1-1 grid_search tools load the live bin + abort on stale** (+ a `typeof location` guard so the
  engine loads under Node — the tools were unrunnable in plain Node before).
- **P0-3 out-of-order rows** — `fund_dup_guard.dedup()` sorts + `assert_ok()` rejects unsorted; a
  follow-up made the two direct writers (`update_fundamentals`, `build_fundamentals`) sort before the
  guard (else the stricter guard would have failed the nightly). Self-heals the 8 on the next refresh.
- **P0-4 ebit>op** — `build_revop.normalize_ebit()` nulls the 169 impossible cells at finalization.
- **P1-3 bank/industrial misflag** — quarterly-results flag denoised conservatively; 18 clear
  industrials (Page, Atul, Balrampur…) fixed, ZERO regressions (real NBFCs / fee-based financials
  untouched). NOTE: the deeper source classifier (`"InterestEarned" in xml` substring) still
  mis-tags ~34 historical quarters per industrial → a separate careful job (see P1-3).
- **P0-5 (marquee cell) M&M 20210930 con −479.08 → 1928.64** via `fund_cell_fix.json` + applied to the
  live store. Externally confirmed to the crore (Q2FY22 con PAT ₹1,929cr, rev 21,470 matches). Nifty-50
  name, feeds every profit factor. The remaining ~133 mirror disagreements are small-value /
  genuine-bad-quarter cells with no clear authority-wrong signal — left for per-cell filing adjudication.

**Still blocked by concurrency** (other sessions hold these files dirty): **P0-6** (nightly ann-date
reconciler — `backfill_ann_dates_bse.py`), **P0-8** (13 DII heals — the SHP pipeline). **Heavy / not
yet done:** P0-9 (trading-calendar ghost days), P0-10 (ETF splits), P0-11 (RASOYPR), P1-4, P0-2.

---

**What this is.** A user-requested full sweep of every dataset the backtest consumes ("check entire
data every possible way, find many many bugs, make a plan for Opus to follow and resolve them"),
plus the user's mid-run additions: DII/FII holdings, the dates of every fetch, whether stored
"results dates" are really results dates, and look-ahead / lookback defects.

**Nothing here has been FIXED. No repo data file was modified by this audit.** Every item below is
a measured finding with an evidence trail, a severity, a blast radius, a heal route and an
acceptance test.

**Evidence artifacts** (session scratchpad `53e90bfc`, copy out before it is cleaned):
`findings/A_prices.md`, `B_fundamentals_np.md`, `C_revop.md`, `D_pit.md`, `E_membership.md`,
`F_dii_dates_engine.md` + a `*_evidence.json` per report with machine-readable cell lists.

---

## THE QUEUE AT A GLANCE

Severity key: **C** = confirmed (measured, reproducible) · **S** = suspect (candidate list, needs a
document read) · **K** = known-open (re-measured this audit, not new).

| # | Item | Sev | Size | Effort |
|---|---|---|---|---|
| **P0-1** | F&O membership not alias-folded → 9.9% of universe dropped | C | 3,165 slots | **1 line ×2** |
| **P0-2** | Index rosters hold not-yet-listed companies (2015-20) | C | 2,755 triples | rebuild |
| **P0-3** | 8 symbols' quarters out of order → stale quarter served | C | 8 syms | small |
| **P0-4** | `ebit > op` — impossible by construction | C | 169 cells | mechanical |
| **P0-5** | Owners heal reached authority, not the mirror | C | 71 cells | re-run |
| **P0-6** | **Nightly date-fixer has no §119 vetoes — still writing look-aheads** | C | regrows nightly | **do first** |
| **P0-7** | `profitAccel`/`profitStreak` use array position, not calendar | C | ~2,011 cells | small ×2 |
| **P0-8** | 13 DII heals silently skipped by the applier | C | 13 cells | applier fix |
| **P0-9** | Trading calendar: ghost days, mis-dated Muhurat, missing session | C | ~15,800 bars | medium |
| **P0-10** | ETF unit splits unadjusted; phantom rule mis-verdicts them | C | 17 events | medium |
| **P0-11** | RASOYPR 1:15 ledgered but never applied | C | 1 event + guard | small |
| P1-1 | Strategy-finder tools read the 74-day-stale bin | C | 2 tools | small |
| P1-2 | Make the frozen bin unreachable | C | infra | small |
| P1-3 | 9 industrials flagged as banks on the LIVE site | C | 9 syms | small |
| P1-4 | 22 microcaps hold raw rupees in a ₹-crore slot | C | 201 cells | low priority |
| P2-1…P2-7 | Bounded adjudication lists (mirror, dates, SHP, scale, dupes, chimeras) | S | ~900 cells | per-cell reads |
| P3-1…P3-6 | Large measured campaigns (qe+45 era, pre-first-bar, staleness, orphans) | K | large | campaigns |

**Suggested first session:** P0-6 (stop the bleeding) → P0-1 + P0-7 (one-line-class engine fixes
with big reach) → P0-3/P0-4/P0-8/P0-11 (small, certain) → then P0-9/P0-2 (rebuilds).

---

## 0. STANDING RULES FOR WHOEVER WORKS THIS PLAN

1. **NO ASSUMPTIONS. NO GUESSWORK.** (user-mandated, runbook §0). Every value written and every
   claim made must trace to something MEASURED in your session. Can't measure it? Write `unknown`.
2. **Never say "unfillable"** until the §57 route ladder is walked, showing per-rung status.
3. **Concurrency (CLAUDE.md):** shared checkout. `git status` first; stage explicit paths; NEVER
   `git add -A/-u/.`, never `git reset --hard` / `git checkout -- .` / `git stash` here. Long or
   scripted jobs get their own worktree under `~/stocks-wt/`.
4. **Heal via LEDGERS, never derived files.** CI rebuilds sf_fundamentals / sf_revop /
   quarterly_results / results_season / the dash bins nightly and WILL clobber a direct edit.
   Route through the matching ledger, then rebuild, then re-verify LIVE ~20 min later.
5. **A heal must reach EVERY store that holds the cell.** Item B-2 below is 71 cells proving a heal
   that reached the authority and not the mirror.
6. **Ship-it gate (runbook §39)** applies to every code change here. Both engine twins stay in sync.

---

## 1. HOW THIS AUDIT WAS RUN (trust, or re-run, these numbers)

**Everything was measured against LIVE data, never this checkout.** The local checkout is a stale
duplicate lineage: 23 commits "ahead" of origin, of which only **5 are genuinely unpushed** — the
other 18 are rebase duplicates already on origin under different hashes.

| Input | What was actually used | Measured stamp |
|---|---|---|
| JSON stores | `git show origin/main:<path>` | — |
| Price bin | release asset `sf_stock_data.bin` | `end=2026-08-31`, 4,572 syms, 193,737,638 B |
| Engine code | `origin/main:docs/backtest-engine.js` + `docs/stock-backtest.html` | both `ENGINE_VER=e13` |
| Deploy | Pages-served files vs origin blobs | **byte-identical** (5/5) — no publish wedge |

### ⚠️ 1a. AUDIT-INFRASTRUCTURE TRAP — READ BEFORE RE-RUNNING ANY SCREEN

`docs/sf_stock_data.bin` in the repo is a **FROZEN snapshot — `end=2026-06-13`, 5,148 symbols,
93,913,412 B** (md5 `9f1113d3b7e14ebfab3024f8fb95e104`): 74 days stale AND a different universe
from live (4,572 symbols). During this audit it was copied over the scratchpad's live copy; caught
by md5, and the affected agents were re-tasked against a write-protected true-live copy.

It cost real work twice in one day: the membership agent's first pass produced **~3,400 phantom
orphans** from it before diagnosing and discarding them.

**Why it matters beyond the audit:** the frozen bin predates the §87 pre-2016 corporate-action
heal, so it gives the OPPOSITE verdict on corp actions (ITC 2005-09-21 reads unadjusted on it,
correct on live). A campaign started from it chases already-fixed data.

**→ Fix task (P1-5 below): make the trap unreachable, don't just document it.**

---

## 2. THE FIX QUEUE, IN THE ORDER I RECOMMEND WORKING IT

Ordering rationale: **(P0) things that silently change backtest results and are cheap and certain**
→ **(P1) structural/tooling defects that keep manufacturing wrong answers** → **(P2) bounded data
adjudication** → **(P3) large measured campaigns** → **(P4) documentation debt**.

---

### P0-1 · F&O membership joins are not alias-folded — 9.9% of the F&O universe silently dropped
**Severity: CONFIRMED, highest measured impact of this audit. Fix is ~1 line per engine.**

Both engines build the member set verbatim:
```js
return snap ? new Set(snap.symbols) : new Set();     // backtest-engine.js membersAsOf
```
and then join on the SERIES key: `if (members && !members.has(m.symbol)) continue;`
(`backtest-engine.js` L812; `stock-backtest.html` L1129). `FUND_ALIAS` (627 entries) is applied to
**fundamentals and SHP only** — never to membership.

Meanwhile `scripts/fno_history.json` still carries **as-traded-then** names, while the price store
stitched renamed companies' history under their **current** ticker. So the join misses.

**Measured (main session, independently of the agent, against the true-live universe):**
- **3,165 of 32,073 F&O (snapshot, member) slots = 9.9% reference a name with no price series.**
- **99.1% (3,135) are recoverable by a FUND_ALIAS fold.**
- Per-year drop rate: 2003 **25.4%**, 2005 20.9%, 2007 **18.8%**, 2010 13.2%, 2012 9.4%, 2017 8.5%,
  2020 8.1%, 2023 3.3%, 2025 0.6%.
- Named casualties: INFOSYSTCH→INFY (pre-2011), TATAMOTORS→TMPV (pre-2025), HEROHONDA, HINDLEVER→
  HINDUNILVR, COLGATE→COLPAL, GUJAMBCEM→AMBUJACEM, BAJAJAUTO→BAJAJHLDNG, CADILAHC, PVR.

**Engine comment L552-556 is now factually stale** — it says F&O membership uses "the name that
traded THEN", which the stitching campaigns made untrue.

**Heal:** fold at Set-build time in `membersAsOf`, in **both** twins:
```js
return snap ? new Set(snap.symbols.map(s => FUND_ALIAS[s] || s)) : new Set();
```
Only 6 fold collisions exist, all same-company dual entries, absorbed by the `Set`. Prefer the
engine fold over re-keying `fno_history.json`: the fold self-heals as FUND_ALIAS grows.

**Acceptance test:** re-measure the drop rate → expect ≤0.1% (residue = ~30 true orphans such as
AIRDECCAN). Bump `ENGINE_VER` (invalidates snapshots) + `sw.js` CACHE. Correct the stale comment.
**Expect every F&O saved strategy's numbers to move** — that is the fix landing, not a regression;
run the saved-strategy sweep and attribute the deltas before/after.

---

### P0-2 · Index rosters 2015-2020 contain companies that had not IPO'd yet
**Severity: CONFIRMED. Every 2015-2017 backtest on the affected universes ran ~10% short.**

**Measured (main session, true-live first-bar per symbol): 2,755 (index, snapshot, member) triples
where the member's first traded bar POSTDATES the snapshot date.**

By index: Nifty 500 **2,203**, Nifty 200 133, Smallcap 100 116, Midcap 150 78, Nifty 100 50,
Midcap 100 41, Next 50 34, Smallcap 50 33, Realty 21, MNC 16.

Worked example — Nifty Next 50 rosters dated 2016 hold the entire 2017-18 insurance/IPO cohort:
`MANKIND` (first bar **2023-05-09**) in the **2016-02-22** snapshot, plus BANDHANBNK (2018-03-27),
DMART (2017-03-21), GICRE, HDFCAMC, HDFCLIFE, ICICIGI, NIACL, SBILIFE. PAYTM appears in 2017/2018/
2020 **Nifty Bank** snapshots.

**Double damage:** phantoms are silently dropped by the engine (no bars) **and** the real
then-members are missing — measured effective Next 50 = **44-45 of 50** in 2016-17, Nifty 100 ≈95,
Nifty 200 ≈181. This is also the root cause of the inflated transition snapshots in the roster-size
screen (Next 50 @2016-02-22 = 67 members).

**Heal:** complete the 2015-2018 add/drop events in `build_membership_v2.py`'s `_changelog.json`
inputs and **rebuild the store** — never hand-edit the JSON. §102-F1 already established the NSE
`IndexInclExcl.xls` register as the source that fixed the 1998-2020 events; this band needs the
same treatment. **After ANY membership change, re-measure roster sizes across ALL eras** — §102's
`reanchor_segments()` lesson is that a single global walk compounds one-legged-event drift.

**Acceptance test:** phantom count → 0 for snapshots ≤ today; effective (resolvable) roster size
within 2% of nominal for every 2015-2020 snapshot; 2020-09-25-onward snapshots stay clean.

---

### P0-3 · 8 symbols' fundamentals rows are out of date order — the engine can serve a STALE quarter
**Severity: CONFIRMED, new (no runbook/doc match). Small, cheap, and it corrupts the point-in-time
"current quarter" selection.**

Both engines find the current quarter by scanning **backwards from the array end**, assuming the
array is sorted ascending:
```js
for (let i = arr.length - 1; i >= 0; i--) { ... if (q[annIdx] > 0 && q[annIdx] <= dateInt) { cur = q; break; } }
```
(`profitAt` L595 area, `profitMetrics` L639 area; same in the twin.)

**Measured — 8 symbols whose rows are NOT sorted by qe:**

| symbol | tail of qe list | back-scan serves | true latest | in index? | in price universe? |
|---|---|---|---|---|---|
| **STCINDIA** | …20260630, **20260331** | 20260331 | 20260630 | **YES** | YES |
| OMKARCHEM | 20260331, 20250331, 20260630 | 20260630 | 20260630 | no | YES |
| FELIX | 20260331, 20260630, **20250630** | 20250630 | 20260630 | no | no |
| SECL / SAHAJSOLAR | 20260331, **20250331** | 20250331 | 20260331 | no | no |
| KNACK / VOEPL / XTRANET | 20260630, **20260331** | 20260331 | 20260630 | no | no |

Only **STCINDIA** currently reaches a backtest (index member + priced) and it is served a
one-quarter-stale result. FELIX would be served a **full year** stale if it were priced.

Note the TTM pairwise-gap guard (L103 of `profitMetrics`) computes a negative month-delta for these
and correctly nulls TTM — so TTM is safe, but `yoy` / `base` / `resultDate` come from the wrong row.

**Heal:** (a) root-cause which writer appends without re-sorting (the tails are all recent 2026
rows, so it is an upsert path — start at `update_fundamentals.py`); (b) re-sort the 8 rows via the
normal ledger/rebuild path; (c) **add a sorted-invariant guard** to the pre-commit hook that already
guards resurrected cells, so this cannot re-enter. Readers `stock.html` (re-sorts, L536) and
`build_quarterly_results.py` (dict-keyed) are unaffected — do not "fix" them.

**Acceptance test:** zero unsorted symbols; guard proven to fire by injecting one unsorted row.

---

### P0-4 · 169 cells where `ebit > op`, which is impossible by construction
**Severity: CONFIRMED (arithmetic contradiction), mechanical.**

`build_revop.py` defines `ebit = op − depreciation`, so `ebit ≤ op` always. **Measured 169 cells
violating it; 69 exceed ₹1 cr.** Worst: PEL 20220630 std (op 150.92 / ebit 329.12), IMAGICAA
20230331 **both bases** (14.62/133.78), GMRAIRPORT 20240331 std (−0.13/94.00), RNAVAL 20190331,
HEXT 20240930 con, ADANIGREEN 20190930 con, DLF 20191231 con.

Almost certainly the `op` and `ebit` slots for one cell were written from **different filings or
different route passes**. Route through `revop_cell_fix.json` + rebuild; **add the invariant as a
build-time assertion** in `build_revop.py` so a violating row can never be written again.

**Acceptance test:** 0 violations post-rebuild; assertion proven to fire.

---

### P0-5 · The heal that reached the authority but not the mirror — 71 cells
**Severity: CONFIRMED. This is a process defect, not just 71 numbers.**

sf_revop slots 4/5 (`patStd`/`patCon`) are a non-authoritative PAT mirror (§70). **Measured: of
139,574 cells populated in both stores, 135 disagree** by >1% and >₹0.1 cr.

**Classification (main session):**
- **71 are explained by `owners_basis_heals.json`** — the §116 owners-basis heal corrected
  `sf_fundamentals` and the mirror still holds the pre-heal TOTAL. Confirmed instance:
  JINDALSTEL 20201231 (mirror 2566.68 = old total; authority 2254.66 = healed owners).
- **64 are unexplained** (61 std, 3 con) and need filing reads.

**Heal:** (a) re-run the owners heal so it writes the mirror too — this is the
[[feedback-retraction-needs-every-ledger]] class: the applier must enumerate every store holding
the cell; (b) adjudicate the 64 individually (see P2-1).

**Acceptance test:** explained-by-heal count → 0; a deliberate re-run of the applier is idempotent
and touches only intended cells.

---

### P0-6 · The NIGHTLY ann-date reconciler has none of the §119 vetoes — it is still creating look-aheads
**Severity: CONFIRMED, and it REGROWS every night. Work this first — everything else is a static
backlog; this one is actively writing new defects.**

The §119 sweep's safety rules — ignore delay notices, ignore "board meeting to consider…"
intimations, ignore provisional/advance notes — live only in that one-off sweep's tooling. The
reconciler that runs **nightly** carries none of them.

**Live proof (verified in the main session against the live store):**
```
BANKINDIA 20251231  ann 20260121   (52 days after qe — normal)
BANKINDIA 20260331  ann 20260402   (2 days  — IMPOSSIBLE for audited bank results)
BANKINDIA 20260630  ann 20260727   (27 days — normal)
```
The Apr-2 date came from a *"Financial Result (Provisional)"* note; the audited results were filed
**2026-05-08 17:40**. A backtest screening April–May 2026 sees that quarter **36 days before the
market did**. Two prior instances of this same class already needed seq-audit correction.

**Heal:** port the §119 veto/classifier rules into the nightly reconciler itself (one shared
classifier for fetch, audit and apply — do not maintain two). Add a lag-plausibility guard: a date
inside qe+7d must be refused outright, not written. Then re-sweep what the un-vetoed reconciler has
already written since it went live.

**Acceptance test:** BANKINDIA 20260331 reads 20260508; the veto rules are proven to fire on a
replayed delay notice; a fresh nightly run writes zero cells inside qe+7d.

---

### P0-7 · `profitAccel` and `profitStreak` still pick quarters by ARRAY POSITION, not calendar
**Severity: CONFIRMED. This is the exact bug class the TTM fix closed — in two factors it missed.**

`profitMetrics`, both engine twins:
```js
let accel = null; if (ci - 1 >= 0) { const py = yoyOf(arr[ci - 1]); ... }   // array-previous row
let streak = 0; for (let i = ci; i >= 0; i--) { ... }                       // walks array, not calendar
```
`arr[ci-1]` is the previous ROW, not the previous QUARTER. Where a quarter is missing from our
data, `accel` silently compares against a quarter 6, 9 or more months back, and `streak` counts a
"consecutive" run straight across the hole.

**Measured blast radius: profitAccel wrong in ~1,516 cells / 810 symbols; profitStreak in ~495 /
330.** TTM was fixed for exactly this reason (2,580 cells) and these two were left behind.

**Heal:** apply the same pairwise-calendar-gap guard TTM already uses (quarter-ends are quantized to
Mar/Jun/Sep/Dec, so consecutive quarters step by exactly 3 months). **Both twins.** Bump
`ENGINE_VER` + `sw.js`. Expect saved-strategy numbers using accel/streak to move.

---

### P0-8 · 13 DII corrections the §118 audit itself proved are still wrong on the live site
**Severity: CONFIRMED. A heal-writer that silently skips its own cells.**

`cmd_apply` skips any `(sym, qe)` already present in `shp_cell_fix.json` — **even when the existing
entry is only a date correction**. So 13 of the SW-2 "Any-Other institutions" DII heals never
landed, and the site still shows the inflated values:
- **RELCAPITAL DII ~9.7pp too high** (the block is Morgan Stanley Mauritius et al.)
- **JISLJALEQS ×7 quarters**, up to **10.8pp**
- RELIGARE 5.7pp, SATIN ×3, 21STCENMGM

**Heal:** make the applier merge by FIELD, not skip by key — an existing date-only entry must not
block a value correction. Re-run; verify all 914 SW-2 cells, not just the 901 that landed.
This is the same family as P0-5: **a heal is not applied until every store and every field is
checked**, and a skip must be logged, never silent.

---

### P0-9 · The trading CALENDAR itself is wrong — fabricated holiday sessions, a mis-dated Muhurat, a missing real day
**Severity: CONFIRMED by TWO INDEPENDENT STORES. This corrupts every window-based factor, because
the bar count inside a window is wrong.**

Cross-checked the sf price store against the independent Yahoo-sourced `stock_data.bin`:

| date | sf store | Yahoo store | verdict |
|---|---|---|---|
| **2021-11-04** | **0 symbols** | **3,546** | Muhurat session stored under the WRONG DATE |
| **2021-11-05** | 1,823 | 0 | …it is here instead |
| 2024-01-22 | 2,095 | 0 | ghost day (NSE closed) |
| 2024-05-20 | 2,134 | 0 | ghost day (NSE closed) |
| 2020-11-16 | 1,674 | 0 | ghost day (NSE closed) |
| 2016-08-12 | **2** (DVL/DTIL only) | 0 | a REAL session missing — official bhavcopy has 1,579 rows |
| control 2024-01-23 | present | 3,919 | both agree — method sound |

The audit also names six 2019 holiday ghost days (2019-10-08/10-21/10-28/11-12/12-12, ~15,800 fake
bars total), each a near-100% copy of the adjacent real session, arbitrated against the NSE archive.
**Honest limit: my Yahoo cross-check is inconclusive for the 2019 dates** — that store's 2019
coverage is sparse (control days show only 2-53 symbols), so those five rest on the agent's NSE
arbitration alone and should be re-confirmed against the bhavcopy archive before healing.

Also confirmed: **12 Sunday bars** exist for DVL/DTIL only — the same padding artifact that puts
2 phantom bars on 2016-08-12.

**Heal:** rebuild the trading calendar from the official bhavcopy archive (the authority for "was
there a session"), then (a) delete fabricated sessions, (b) re-date the 2021 Muhurat bars to 11-04,
(c) ingest the missing 2016-08-12 session, (d) remove the DVL/DTIL weekend/holiday padding.
**Add a calendar guard** to the ingest: a session may only be written if the bhavcopy for that date
exists.

**Acceptance test:** sf and the bhavcopy archive agree on the SET of trading dates 2002→date;
no date carries bars for fewer than ~20 symbols unless the archive says so.

---

### P0-10 · ETF unit splits are systematically unadjusted — 17 confirmed events
**Severity: CONFIRMED, new class.** 8 gold-ETF 1:100 splits (including **GOLDBEES**, with no ledger
entry at all) and 9 index/silver-ETF 1:10 splits in 2026. Worse, `phantom_crashes.json` actively
**mis-verdicts these as "genuine crashes"** — the phantom-CA rule only consults equity corporate-
action feeds, which never list ETF unit splits. Each affected tape is **10-100× wrong** before its
split date.

Contained today only because every affected symbol is `alive=False` — that is luck, not a guard.

**Heal:** add an ETF-aware corporate-action route (the unit-split notices are published by the AMC /
exchange ETF circulars, not the equity CA feed), and make the phantom-crash rule REFUSE to issue a
"genuine crash" verdict for an instrument class whose CA source it does not consult — a verdict is
only as good as the sources behind it.

---

### P0-11 · RASOYPR's 1:15 split is ledgered in BOTH corp-action files and was never applied
**Severity: CONFIRMED.** Verified on the live tape:
```
RASOYPR 20130320 close=115.70   →   20130321 close=8.45
```
The stored (supposedly adjusted) series still carries the raw cliff; the turnover-step reads exactly
1.0, i.e. no adjustment happened. The stock was genuinely liquid in 2013, so every trailing-window
factor reads a fake −93% crash for a year afterwards.

This is a **new member of the JINDALSTEL "ledgered ≠ applied" class** (§87g) that the sweep did not
name — the ledger is right, the bake rejected it. Re-run the §87g reconciliation and, critically,
**make a rejected ledger factor a loud failure rather than a silent skip** — that silence is what
let both JINDALSTEL and this one survive their sweeps.

**Acceptance test:** RASOYPR's 2013-03-21 ratio reads ~1/15; a deliberately un-appliable ledger
entry produces an error, not silence.

---

### P1-1 · The strategy-finder tools run on the 74-day-stale frozen bin
**Severity: CONFIRMED. Any strategy these tools recommend is fit on stale prices and will not match
the site.**

`scripts/grid_search.js:21` and `scripts/grid_search_full.js:14`:
```js
const SFD = GZ('docs/sf_stock_data.bin');     // end=2026-06-13, 5,148 syms
```
This is the tool behind runbook §7 (STRATEGY FINDER). `build_coverage_matrix.js` already does this
correctly (it carries the warning comment and uses `RELEASE_BIN`) — copy that pattern.

**Heal:** point both grid-search tools at the release asset (or the sf-data parts) with a
**hard abort** when the loaded `end` is older than `sf_meta.json`'s `end` — a silent stale load is
what makes this class expensive. Same guard belongs in any harness that loads a bin.

**Acceptance test:** run `validate` mode against the live site and reproduce 2/2 combos exactly.

---

### P1-2 · Make the frozen bin unreachable, not merely documented
**Severity: CONFIRMED trap (it has now cost work in at least 3 sessions: §103, this audit's
membership pass, and this audit's scratchpad contamination).**

`docs/sf_stock_data.bin` is committed, stale (`end=2026-06-13`), carries a legacy 5,148-symbol
universe, and 50+ scripts reference the path.

**Options, in preference order:** (a) stop committing it and have every reader resolve the release
asset (§103 already did this for `stock_data.bin`, which is now 1 day fresh); (b) if it must stay,
have CI refresh it whenever its `end` goes stale — the mechanism `refresh.yml` already uses for
`stock_data.bin`; (c) at minimum, add a loud `_STALE_end=2026-06-13` marker inside the file and a
shared loader that aborts on it.

**Acceptance test:** a deliberate attempt to audit prices from the repo path fails loudly instead of
returning 74-day-old numbers.

---

### P1-3 · 9 industrial companies are flagged as banks/NBFCs on the LIVE site
**Severity: CONFIRMED live and user-visible.**

Verified in the **live** `quarterly_results.json` payload: `PAGEIND f=1`, `BALRAMCHIN f=1`,
`LUXIND f=1`, `ATUL f=1` — the same flag as `HDFCBANK f=1` / `SBIN f=1` (control: `RELIANCE f=0`).
Also flagged: TVSSRICHAK, RADIOCITY, PRSMJOHNSN, HGS, TAKE.

Root cause: stray per-row `fin=1` cells in `sf_revop` inside the 13-quarter window, and
`build_quarterly_results.py:177` sets the page-level flag if **ANY** row has `fin=1`. The same flag
drops rows from the results-season non-financial medians (RAJESHEXPO: 56 misflagged rows 2002-2017).

197 symbols flap between bases overall, **but some flips are legitimate** (PEL, MAHSCOOTER really
did convert) — so heal only the named, adjudicated subset and fix the classifier, not the flag.
A classifier is code too: decide the flag per COMPANY from a stable source, not per row.

**Acceptance test:** PAGEIND/BALRAMCHIN/LUXIND/ATUL render as non-financial on the live page;
RAJESHEXPO's 56 rows rejoin the non-financial medians; PEL/MAHSCOOTER conversions preserved.

---

### P1-4 · 22 microcaps hold raw RUPEES in a ₹-crore slot — and the screen's headline was wrong
**Severity: CONFIRMED class, LOW blast radius. Recorded precisely because the first framing
over-stated it.**

**Measured: 201 cells across 23 symbols hold |np| > ₹60,000 cr.** Extremes: NSL 20210331 std+con
**471,636,712** (raw rupees ≈ ₹47 cr), MANAS 20190930 **139,779,672**, GUJJUBHAI 22 cells,
ATVOENT 20, KSSMART 20, OMEGAIN 18, ELITECON 14.

**★ The headline "biggest live defect" was a FALSE POSITIVE and must not be inherited.** The one
index-member in the list, **TMPV, is CORRECT**: its 20250930 std 82,081 / con 76,170 was verified
against screener.in, which independently prints standalone net profit **82,081** and consolidated
**76,248** for Sep-2025 — a genuine demerger exceptional. (Our con 76,170 vs their 76,248 is the
owners-vs-total NCI difference, i.e. our documented basis, not an error.)

So: of 23 symbols, **1 is genuinely correct**, and of the remaining 22 only 4 are even in the price
universe (BLACKROSE, ELITECON, NSL, QUINT) — reachable **only** by an all-stocks backtest, never by
an index strategy. Real, worth healing, but not urgent.

**Heal:** per-cell filing reads → `fund_cell_fix.json`. **Add an mcap-sanity tripwire** to
`detect_scale_errors.py` so a quarterly PAT exceeding a plausible multiple of market cap cannot be
written — that tripwire, not the cell list, is the durable fix.

---

### P2-1 · 64 unexplained PAT-mirror disagreements — adjudicate against filings
**Severity: SUSPECT (a disagreement names no side).** The 64 left after removing the 71 owners-heal
cells (P0-5). Highest-value first, because these are marquee names where the **authority** looks
out-of-family:

| symbol | qe | basis | mirror | authority | note |
|---|---|---|---|---|---|
| **M&M** | 20210930 | con | 1928.64 | **−479.08** | authority out-of-family: neighbours +423.88, +1987.44, +2237.36. Nifty-50 name; distorts every profit factor for 4+ quarters. **Do this one first.** |
| DLF | 20190630 | con | 4.14 | 414.72 | mirror is the ×0.01 shape; authority in family (436.56 / 445.85 / 414.00) |
| RELCAPITAL | 20230331 | std | −4.35 | −1389.39 | authority out-of-family vs −77.89 / +210.52 |
| JPASSOCIAT | 20091231 | std | 314.96 | 103.02 | |
| FACT | 20080331 | std | −48.36 | 151.64 | sign disagreement |
| JSL | 20220630 | std | 453.65 | 286.74 | |
| HINDUNILVR | 20071231 | std | 473.79 | 631.44 | marquee; also a §101-class open item |
| GODREJAGRO, SANOFI, REPCOHOME, LTF, WELCORP, FDC | various | | | | remainder in `C_evidence.json` |

**Method:** open the quarter's OWN filing (runbook §58 standard read). Note §116's lesson — the gate
is the **identity** (`owners + NCI == total`), not the comparison; healing on the comparison alone
wrote 762 wrong values to fix 52 right ones last time. M&M was NOT resolved this session:
screener.in's visible window starts Jun-2023, so it needs the BSE filing route.

---

### P2-2 · 321 cells dated 1-6 days after quarter-end (the corrupted-writer shape)
**Severity: SUSPECT, bounded, high-yield.** Same shape as §119d's corrupted `qe+1..+6` stamp class
that was only partially healed. 85 sit in 2021 alone (e.g. HIMATSEIDE Sep-2024 "announced" Oct-1).
A results date 1-6 days after quarter-end is implausible for an audited quarter and reads as a
look-ahead in the engine. Route: `ann_date_fills.json` (`exact` kind, §12-gated).

### P2-2b · 378 cells with an announce date inside qe+7d (physically impossible)
**Severity: CONFIRMED shape, SUSPECT per cell.** Superset of P2-2's 321. Two were ground-truthed
this audit: HIMATSEIDE Sep-2024 (real lag 44d, not ~1d) and RTNPOWER Sep-2014 (34d). **33 sit in
quarters ≥2023**, i.e. inside the window most strategies actually trade. Route via
`ann_date_fills.json` (`exact`, §12-gated). A `qe+7d` refusal guard at the writer (P0-6) stops the
class regrowing.

### P2-2c · "Stamped with the NEXT quarter's filing date" — hits TCS and BAJAJFINSV
**Severity: CONFIRMED, direction is conservative-late (not a look-ahead).** TCS Jun/Dec 2015-16 and
BAJAJFINSV Mar-2015 carry the following quarter's filing date, so a timely filer looks a quarter
late and YoY timing shifts on the most liquid names in the market. 943-cell mixed class; the
**timely-filer core is ~130 cells** — do that slice, leave the rest documented.

### P2-3 · 15 single-quarter SHP spikes >15pp that revert next quarter
**Severity: SUSPECT.** HAL, SHRIRAMFIN, RNAVAL and 12 others — parse/basis defects on §118's edge.
Small enough to adjudicate cell-by-cell from the filings.

### P2-4 · 22 `rev == 0` cells flanked by >₹10 cr quarters + a hard core of ÷100 candidates
**Severity: SUSPECT.** ALLCARGO 20250930, MTNL 20180630, VIYASH 20221231; ÷100 shapes at
BIRLACORPN 20210930 (revStd 10.85 vs neighbour median 1,263), JSL 20150930 (16.02 vs 1,793),
WHEELS 20221231 (both bases). Zero is a documented no-base sentinel — confirm each against the
filing before writing. Route: `scale_fix.json` / `revop_cell_fix.json`.

### P2-5 · 14 upstream-vs-authority conflicts, 5 sharing the owners=0 mis-tag shape
GLENMARK (−0.10 vs 301.41), KIRLOSBROS (0.80 vs 66.70), SHYAMMETL (−1.31 vs 261.76), TRU (0.00),
TALBROAUTO (0.00) — the §116 "filer tagged owners=0" family, where `build_fundamentals.xbrl_profit`'s
`or one` fallback applies. Disagreement names no side; open the filing.

### P2-6 · 1 surviving duplicate-quarter row
`APOLLOTYRE 20140331` — two rows: `[…, null, null, 281.62, 20140530]` and
`[…, 129.62, 20140515, 281.62, 20140515]`. First-match readers (`apply_fund_cell_fix`,
`apply_owners_full`) see row A; the engine's back-scan lands on row B. Down from the 22 in
`DUP_QUARTER_ROWS.md`. Merge the pair; keep the dup guard.

### P2-7 · 65 recycled-ticker chimera suspects
Symbols with a >2y internal price gap, >70% level jump, and one-sided fundamentals. **Priority 6
that are rostered on BOTH sides of the gap** (so a backtest joins across the seam): ELECON
(2002→2006, +8,954%), HINDZINC (2003→2006, +5,866%), ESSARSHIP, RAIN, TIMKEN, SUDARSCHEM.
Several may be genuine suspensions/relists riding the 2003-07 bull run or §87 residue — **but that
is exactly what DVL/DTIL looked like**. Adjudicate by per-side ISIN from old bhavcopies (§89 method).

---

### P3-1 · The pre-2008 `qe+45` fabricated announce-date era — the largest remaining look-ahead
**Severity: KNOWN-OPEN, freshly measured. This is the biggest surface left in the store.**

**Measured (main session): 17,178 of 170,079 dated cells (10.1%) sit at EXACTLY quarter-end + 45
days.** The per-year share proves it is a convention, not a coincidence:

| year | share at exactly +45d | | year | share |
|---|---|---|---|---|
| 2000 | **97.3%** | | 2005 | 64.8% |
| 2001 | **97.8%** | | 2007 | 54.4% |
| 2002 | 76.6% | | 2008 | 31.6% |
| 2003 | 66.2% | | 2009+ | 11-15% (natural — the real lag distribution peaks at 38-46d) |

Secondary fingerprints in the same histogram: **4,419 cells at exactly +60d** and 3,983 at +30d.

The era's own real filings ran up to 89 days, so slow filers are shown as public ~1-6 weeks before
they could have been. The 2004-05 weekend anomalies (54% Saturday, 35% Sunday) are this same
convention landing on fixed calendar days — **not a separate defect**.

**Before opening this:** §N8 measured that a blanket +45→+60 move makes the data WORSE in every era
(pre-2009 real March lags have p50 = 32d). So this needs REAL dates, per the §119 archive-index
flow, not another convention. The bounded, highest-value slice is the **1,654 March (audited)
quarters** in the pre-2008 set. Pre-2009 may be archive-floored — if so, record it as a documented
floor with per-rung status (§57d), not as "done".

### P3-2 · 7,115 announce-date cells that predate the stock's own first traded bar
**Measured** across 514 symbols (alias-closure first bar), plus **986 fundamentals symbols with no
price tape at all**. Median ~2.6 years early; CANHLIFE carries 2015 quarters against an Oct-2025
listing. No trade can reach them today, but §99's first-bar floor was only ever applied to one fill
path, so the class **regrows with every IPO backfill**. Fix the floor at the writer, then sweep.

### P3-3 · Index membership staleness between snapshots
40 inter-snapshot gaps >13 months — worst Nifty Midcap 100 **2006-11-08 → 2015-03-25 (8.4 years)**;
Nifty Auto 50.8mo; Nifty 50 itself 33.8mo (2017-05→2020-03). Engine-visible staleness for N500
monthly rebalances 2010-2026: **median 21.5 days, p90 66, max 156**. N500 is well served; the bias
concentrates in the sectoral indices. KNOWN-OPEN (quantmac §102), now quantified per index.

### P3-4 · Pre-2003 era orphans
N500 rosters lose **11-20% of slots per year 1998-2002** to symbols with no resolvable series
(2,389 triples). KNOWN-OPEN §93 ceiling. Note these old rosters are keyed by CURRENT tickers, so
some entries (SOBHA/RBA in a 1998 list) are anachronisms of the name map, not of NSE — worth a
name-map re-audit before assuming a price-coverage gap.

### P3-6 · Price-store residue (all measured, all bounded)
- **43,046 zero-close bars across 115 symbols** — the documented penny-floor rounding class
  (a correct divisor applied to a ₹0.01 series). KNOWN-OPEN, low value.
- **883 mid-series gaps >30 trading days; 96 resume with a >50% jump and no action record** —
  suspension-relist vs the pre-2016 demerger-gap class, unadjudicated. Overlaps P2-7's chimera list.
- **119 pre-2016 extreme moves with no corporate-action record** (fresh §87 residue count);
  post-2016 is clean apart from the ETF class above — only 15 non-penny cases remain.
- **Cross-source parity: 44 of 2,336 shared symbols disagree** with the dashboard store. Mostly
  deliberate policy (demerger/rights adjustment); **KAPSTON plus the holiday-phantom days are
  defects in the DASHBOARD store, not in sf** — sf is right in those.
- **Caveat worth knowing: `vw` (vwap) is just the close on every bar before 2019.** Any factor that
  believes it is reading a true VWAP pre-2019 is reading the close.

### P3-5 · 166 symbols with fundamentals years older than their price tape (batch-ingestion cohort)
First-bar clustering shows batch additions, not listings: **122 symbols share first bar 2026-08-17,
82 share 2026-04-20, 17 share 2026-08-03** (vs the legitimate 1996 dataset start: 865 symbols).
These carry results back to 2017 with only weeks of prices (AMAL: 11 bars, results since 2017), so
they can never be backtested pre-2026. Coverage limitation rather than wrong data — decide whether
to backfill their tapes or mark them explicitly.

---

## 3. AUDITED AND FOUND CLEAN — do not re-run these

Recorded so no future session spends a day re-deriving a green result.

- **Hard look-ahead classes in announce dates: ZERO.** No `ann < qe`, no `ann == qe`, no invalid
  dates, no future dates. Zero SHP visibility dates before quarter-end; zero null/zero SHP `sub`
  slots (either would have been visible-at-all-dates).
- **All 6,834 ann-date ledger heals verified applied** in the live store (full check, not a sample);
  0 misapplied. The §104/§119 campaigns and the nightly reapply are holding.
- **SHP cross-store agreement:** 89,715 + 17,390 overlapping cells, **0 disagreements >0.5pp**.
- **DII/FII VALUES are healthy at scale** (the user's direct question): the three SHP stores agree,
  **no impossible components anywhere in 89,715 cells**, and the cross-sectional medians move
  smoothly 2010→2026 with **no parse break — including across the Sep-2022 format change**. The
  §118 SW-2 heal applied to 901 of 914 cells (the missing 13 are P0-8). The §105/§120 un-dating of
  pre-2016 SHP visibility is fully served by the live feed as designed.
- **Delivery %:** 96.8-100% coverage per year 2002+, 0 range violations, 0 alive-without-dv.
- **`quarterly_results.json` bake:** 0 disagreements in 47,144 / 49,216 compared cells — in sync.
- **Per-stock `docs/fin/<SYM>.json` slices:** 25-symbol / 515-row sample, 0 mismatches.
- **Pages deploy:** the 5 key data files served live are byte-identical to origin/main.
- **Engine twins in sync:** both `e13`; matching `profitAt` / `profitMetrics` / `_conFreshEnough` /
  `qePlus` / UNDATED_SUB definitions; `sw.js` at v130 live == origin.
- **§114/§115 HTML-escape phantom symbols:** ZERO remain in the live stores. Closed.
- **§92 `lastSnap` membership fabrication:** fixed in BOTH engines (empty-set + start clamp,
  verified by line). N500 monthly-since-2002 has zero fabricated rebalances. **The runbook §92 text
  is stale** — see P4.
- **ISIN duplicates across symbols:** 0.
- **Price bar integrity:** 0 bars with `h < l`, 0 closes outside the day's range, 0 vwap out of
  range, 0 negative volume, 0 zero-volume-with-a-price-move. Dates strictly increasing, 0 duplicate
  dates, 0 invalid dates. The 4,114 open-out-of-range bars are all 1996-97 paise rounding.
- **§94 stale-`alive`: extinct (0 cases)** — that fix holds on the live asset.
- **§88a turnover-unit and §88b delivery heals both hold on the live asset:** implied turnover unit
  is lacs in every year; delivery coverage 87-96% every year from 2002; 0 out-of-range dv.
- **Corporate-action application:** of 1,466 ledger events, **1,431 applied and 0 post-2016
  mis-applied** — JINDALSTEL now verified applied. The single failure is RASOYPR (P0-11).
- **`mcap` / `hist_mcap` factors:** always 0 in the SF path — **deliberate and disclosed**, the UI
  group is literally labelled `Size — ⚠ always 0, use Turnover`. Not a defect; do not "fix".
- **Structural integrity of sf_revop:** 0 invalid keys, 0 bad row lengths, 0 type errors.
- **Every data store refreshes daily** except the known-frozen `docs/sf_stock_data.bin` (P1-2).
  CI was green across all workflows on audit day.

---

## 4. DOCUMENTATION DEBT (P4)

- **Runbook §92** says `lastSnap` fabricates membership before an index existed. **Both live engines
  now clamp** (empty set + `simulate()` start clamp). Correct the text; a stale warning sends
  sessions after closed work.
- **`backtest-engine.js` L552-556** claims F&O membership uses the name that traded THEN. False
  since the stitching campaigns. Fix with P0-1.
- **`scripts/_fund_suspect_cells.json`** has an EMPTY `cells` array while its README still says 17
  remain — a held-back ledger that no longer names its population. Reconcile or retire it.
- **`purge_copied_con.py`** docstring says CAMPUS's last real con was 2022-03; the filings say
  2022-06 (the registry value is right, the comment is a quarter early).

---

## 5. WHAT WAS NOT COVERED (say so honestly)

- **Ground-truth verification of individual cell VALUES** was done only where a source was reachable
  in-session (TMPV confirmed via screener; M&M attempted and **not** resolved — screener's window
  starts Jun-2023). Every P2 item is SUSPECT and needs a filing read; none is pre-adjudicated.
- **The 64 unexplained mirror disagreements, 65 chimera suspects, 321 fast-ann cells and 15 SHP
  spikes are candidate lists, not defect counts.** §59's lesson: a screen is not a verdict.
- Screens tuned to catch scale/cumulative/sign defects fire on **genuinely exceptional quarters**
  (IDEA's AGR loss, IOB Mar-18, UltraTech Mar-20, ITC Mar-25, TMPV's demerger). Their raw counts
  (726 strict power-of-ten, 381 cumulative, 540 sign flips, 1,909 H1-shape) are **ceilings**, not
  defect counts — adjudicate before acting on any of them.
- Options/F&O EOD store, macro, insider, deals, IPO and announcement browsers were **not** audited —
  this sweep scoped to what the backtest consumes.
