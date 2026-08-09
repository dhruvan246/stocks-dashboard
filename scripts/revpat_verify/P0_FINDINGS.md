# P0 — REV/PAT COVERAGE AUDIT + THE STORE-LAYOUT CORRECTION
Campaign: REV/PAT VERIFY. Pin **re-set mid-phase**: started at origin/main `8e72b277`, re-pinned to
**`e8a491c6`** when a concurrent session landed a fix to the very thing P0 was measuring (below).
Worktree `~/stocks-wt/revpat-verify` (detached, mine alone).
Denominator: point-in-time Nifty-500 (`indices_history.json` → "Nifty 500", 120 snapshots,
nearest-prior per QE, rename-normed, DUMMY* dropped) — the §22f / `audit_shp_coverage.py` recipe.
Tool: `p0/audit_revpat_coverage.py` — re-runnable, reads the pinned tree, zero network.

---

## 1. ★★★ THE PLAN'S OWN SLOT NOTE IS WRONG — corrected before it poisoned anything

`REVPAT_VERIFY_CAMPAIGN.md` §0 describes `sf_revop` rows as
`[rev_s, rev_c, op_s, op_c, other_inc_s, other_inc_c, flags, pat_s, pat_c]`.

**Measured truth** (`build_revop.py` `accumulate()` lines 438-452, then confirmed by value identity
against `sf_fundamentals` on RELIANCE):

    sf_revop[SYM][QE] = [revS, revC, opS, opC, patS, patC, finFlag, ebitS, ebitC]
                          0     1     2    3    4     5     6        7      8

PAT is at **4/5**, not 7/8. Slots **7/8 are EBIT**. There is no other-income pair at all.
Had the note been trusted, every PAT comparison in this campaign would have read **EBIT** and
manufactured a mismatch on essentially every cell. The plan told us to confirm this
("do not trust this note blindly") — it was right to. Runbook §2c is correct; the plan's note is not.

## 2. ★★★ PAT AUTHORITY IS `sf_fundamentals.json` — and the `sf_revop` mirror is dirty

| field | authority | consumed by |
|---|---|---|
| **revS / revC** | `sf_revop[0]` / `sf_revop[1]` | stock.html, quarterly-results.html |
| **patS / patC** | `sf_fundamentals` `npStd` / `npCon` | stock.html, quarterly-results.html, **backtest-engine.js** |
| ~~`sf_revop[4]/[5]`~~ | **mirror — not authoritative** | *see below* |

Zero-network cross-check of the two stores (tolerance max(₹0.5cr, 0.5%)), at pin `e8a491c6`:

| basis | agree | **DIFFER** | in one store only |
|---|---|---|---|
| std | 89,298 | **31** (0.035%) | 1,500 |
| con | 42,997 | **726** (1.66%) | 6,643 |

**Campaign consequence: never compare `sf_revop[4]/[5]` against a site.** It would manufacture ~757
phantom defects. Same class as the SHP campaign's eleven tooling defects, every one of which first
looked like a data defect.

### 2a. Two things I got wrong here, both caught by evidence rather than by me

**(i) I proposed a tidy mechanism and the data killed it.** I expected the con gap to be the CI
owners re-assertion: `apply_owners_full.py` writes *only* `docs/sf_fundamentals.json` (line 91),
never sf_revop, so the mirror should be stuck at total-PAT. Measured against `_reattr_owners.json`:
**30 of 1,313 (2.3%)**, and 0 in `con_copy_heals.json`. The story was wrong. Cause left undiagnosed
rather than inherited as a guess (§22h's lesson about the 2,344 internal holes).

**(ii) I called it "latent, not live" — it was live.** I grepped the consumers for literal `[4]` /
`[5]` and concluded nothing rendered the mirror. A concurrent session (commit `0d48d5e7`, runbook
**§70**) found that `build_discovery.ttm_pat` reads `pick(cell, 5, 4)` — the mirror slots behind an
accessor my syntactic grep could not see. The **Discovery / Order-Wins TTM P/E was computed off the
wrong PAT**: 298 divergent cells in its 2025-26 window, across 203 symbols. It now prefers
sf_fundamentals. *Lesson for this campaign's own tooling: grep for the QUANTITY's consumers, not for
an index literal — §61's "an empty result is a DIAGNOSIS, not a conclusion" applies to my own greps.*

### 2b. The concurrent session's fix, and what it hands us
`0d48d5e7` resynced the **603 cells where `revop == 0.0`** against a real fundamentals value (the
XBRL owners=0 mis-tag), taking divergences 1,372 → 766 on its tolerance / 726 on mine. It
**deliberately did not touch** the 716 genuine disagreements and 15 `fundamentals == 0.0` cells,
recording that "picking a winner without reading the filing is what created the defects this session
has been undoing… they are the next audit's work."

**That queue is exactly this campaign's P5 arbitration rung**, and it needs no external site at all —
both candidate values are ours, and the filing decides. Its worked example is the warning:
SADBHAV Dec-2020, where **neither** file held the right number (fundamentals had the total, revop had
the right magnitude with a flipped sign). *"The two files disagree" does not mean one of them is right.*

## 3. Coverage vs point-in-time N500 — 95 quarters, Dec-2002 → Jun-2026, 47,436 member-quarters

| era | member-qtrs | revS | revC | patS | patC | patE (backtest-effective) |
|---|---|---|---|---|---|---|
| Dec-2002..Dec-2014 | 24,421 | 87.9% | **0.0%** | 88.3% | **1.4%** | 88.3% |
| Mar-2015..Dec-2019 | 10,001 | 99.5% | 61.3% | 100.0% | 70.5% | 100.0% |
| Mar-2020..date | 13,014 | 98.8% | 96.8% | 99.2% | 97.9% | 99.2% |
| **ALL** | **47,436** | **93.3%** | **39.5%** | **93.7%** | **42.5%** | **93.7%** |

Cells held: revS 44,274 · revC 18,727 · patS 44,468 · patC 20,140 · patE 44,468.
Cells missing: revS 3,162 · revC 28,709 · patS 2,968 · patC 27,296 · patE 2,968.
Per-quarter detail: `p0/coverage_by_quarter.csv`.

- **The con columns are known structural walls, not neglect.** Quarterly consolidated became
  compulsory only from FY2020 (§51a); pre-2020 con-PAT is measured at a **2.7% ceiling** (§53 — 2,979
  cells swept, 79 filings exist at all). Cited, not re-litigated, per the plan.
- **Mar-2020→date is near-complete on all four fields (96.8–99.2%)** — the era where a defect can
  actually reach a user or a backtest pick, and where verification effort belongs.
- Mar-2015..Dec-2019 revC 61.3% / patC 70.5% is the partial-adoption ramp, consistent with §40b.

### 3a. ★ A severity inversion the plan did not anticipate
**patE — backtest-effective PAT** (the engine's `tries=[[3,4],[1,2]]` con→std per-quarter fallback) —
comes to **44,468 cells against patS's 44,468**: the two coverages agree to within ~0.05%. The std
fallback absorbs essentially the entire consolidated gap, so the 57.5% con-PAT hole is very nearly
invisible to the backtest.

⚠️ **Do not restate that as "patC is a subset of patS".** A direct count found **22 member-quarters
holding con PAT but no std**, and store-wide (outside the N500 denominator) the residue is **1,493
cells**. The two totals coinciding at 44,468 is a near-tie, not an identity — an earlier draft of
this section claimed identity, and a direct count refuted it.

The plan ranked con-PAT defects highest because the backtest consumes con. Measured, **patS is the
field with backtest reach on essentially all member-quarters**; patC only matters where it exists
*and* differs from std. Severity ordering for this campaign should follow patS first.

## 4. What P0 changes about the plan
1. **Scope:** verify `sf_revop[0]/[1]` (revenue) and `sf_fundamentals npStd/npCon` (PAT). Never the mirror.
2. **Severity:** patS outranks patC for backtest impact (§3a), inverting the plan's prior.
3. **Site reach is the binding constraint, not our coverage.** Screener carries **13 quarters (oldest
   Jun-2023)**; Groww **5 quarters (oldest Jun-2025)**; Trendlyne is 403-blocked from this environment.
   The multi-site quorum can therefore only speak to roughly **Jun-2023 → date**, and everything
   before that is **exchange-leg-only** — independently reproducing the SHP campaign's conclusion.
4. **Seeded contested set:** the 726 con + 31 std store divergences are a ready-made, zero-network
   adjudication queue for P5, handed over explicitly by runbook §70b.
