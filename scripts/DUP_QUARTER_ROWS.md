# Duplicate (symbol, quarter-end) rows in docs/sf_fundamentals.json

Found 2026-08-25 while auditing the §111 by-product heals. **Independent of §111** — none of that
campaign's 243 heals touches a duplicated row (verified: the intersection of the §111 heal keys and
the 22 duplicated `SYM|QE` keys is empty).

> Fold this into `scripts/DATA_RUNBOOK.md` when that file is free — it was another session's dirty
> work-in-progress on 2026-08-25 (CLAUDE.md rule 1), so the class is written up here instead.

## Why it matters

Row shape is `[qe, npStd, annStd, npCon, annCon]`. Every consumer resolves a quarter with the
**first match**:

```python
next((r for r in fund[sym] if r[0] == qe), None)
```

so a second row for the same quarter is invisible to readers and poisonous to writers: a
consolidated value written for `SYM|QE` can land on a standalone-only row and silently do nothing,
or read `None` where a value exists. `scripts/apply_fund_cell_fix.py` and
`scripts/apply_owners_full.py` both use that idiom. `ci_preserve_merge.py`'s `_rows_to_map` made it
worse in the other direction — it is **last**-wins, so the CI three-way merge wrote into a row no
reader ever reached.

## Measured scope (docs/sf_fundamentals.json @ a7d87cd84, 3,955 symbols / 109,429 rows)

| symbol | duplicated quarters | extra rows |
|---|---|---|
| CARBORUNIV | 12 | 12 |
| SUNPHARMA | 7 | 7 |
| ADVANTA | 2 | 2 |
| APOLLOTYRE | 1 | 1 |
| **total** | **22** | **22** |

All 22 are pairs; no quarter carried three rows. `scripts/fundamentals.json` (the NSE/XBRL source
store) has **zero** duplicates — they are created downstream, in the appliers.

## Root cause — `scripts/_apply_reads.py`, `main_pre2015()`

Both the gate-C and the gate-F/E/X/A branch gated row creation on

```python
if row is not None and row[1] is not None:   # re-anchor against the stored PAT
else:
    newrow = [qe, pat, c.get("ann"), None, None]
    frows.append(newrow)          # <-- also fires when `row` EXISTS with an empty std slot
    fmap[qe] = newrow
```

The `else` conflates two different states:

1. **no row for this quarter** → creating one is correct;
2. **a con-only row exists whose std slot is empty** → the std value must be filled **in place**;
   instead a second row was appended.

The comment directly above the second branch even reads *"re-anchor instead of blindly inserting a
duplicate row"* — the guard was written for exactly this hazard but only covered case 1. A
symptom-explaining comment acting as the bug's alibi.

`fmap[qe] = newrow` then re-points the map at the appended row, so later con fills land there while
the **original** con row stays first — and first is what `next()` hands every reader.

**Proof, not inference.** For all 22 quarters, the introducing commit's *parent* held exactly one
row for that quarter, con-only with an empty std slot — 22/22, no exceptions. The bug was then
reproduced in a sandbox: feeding the real pre-STEP-D SUNPHARMA row plus a STEP-D-shaped ledger
through the unpatched applier emits the live defect byte-for-byte.

Introduced by:

| commit | date | symbols |
|---|---|---|
| `208597c87` | 2026-08-04 | APOLLOTYRE (STEP D chunk 3) |
| `6b670cbbb` | 2026-08-04 | CARBORUNIV (STEP D chunk 5) |
| `e82d0d192` | 2026-08-04 | SUNPHARMA, 6 quarters (STEP D chunk 26) |
| `05e178fe6` | 2026-08-04 | SUNPHARMA Mar-2010 (STEP **N** chunk 27 — that cell lives in `pre2015_reads_n.json`, not `_d`) |
| `f52c16bea` | 2026-08-07 | ADVANTA (STEP E) |

The sibling appliers (`apply_fav14_pat_std.py`, `apply_early_backfill.py`, `hist_backfill.py`,
`update_fundamentals.py`) all check for the existing row first. `_apply_reads.py` was the only
offender.

## The fix

* `scripts/_apply_reads.py` — an explicit `elif row is not None:` branch fills `row[1]` in place.
  `annStd` is filled **only when empty**: 943 rows store-wide carry an `annStd` with a null `npStd`
  (measured 2026-08-25), and this ledger's date is often an `ann_approx` quarter-end+45d
  placeholder, so stamping it unconditionally would overwrite a real date.
* `scripts/fund_dup_guard.py` — shared guard and merger (see below).
* `assert_ok()` wired into the three row-creating writers — `_apply_reads.py`,
  `build_fundamentals.py` (inside `flush()`), `update_fundamentals.py`. They raise and write
  nothing rather than publish a store with two rows for one quarter.
* `ci_preserve_merge.py` deduplicates base/ours/theirs before merging and the result after,
  loudly but **without raising** — its standing contract is that a surprise can never make the
  refresh worse than it was.

## Merge rule — never invent a value

* a slot that is `None` in every duplicate row **stays** `None`;
* a slot with exactly one distinct non-null value takes it;
* a slot with two different non-null values is a **conflict**: the quarter is left untouched and
  reported. It is never silently picked.

`0` is a real value here (the "announcement date unknown" sentinel), not a null — the merger tests
`is not None`, never truthiness.

```bash
python3 scripts/fund_dup_guard.py            # report
python3 scripts/fund_dup_guard.py --merge    # merge the conflict-free ones
```

## No heal was silently swallowed

A duplicate row can make a ledger write land on a row no reader reaches, so every
`scripts/*.json` ledger was swept for the 22 `SYM|QE` keys. The only files that reference them are
campaign **inputs** (`pre2015_reads_{d,e,n}.json`, `pre2015_attempted_{d,n}.json`), the
`_revgap_skips.json` skip list, and `revop_fundamentals.json` — which is keyed
`{SYM: {QE: ...}}` and so cannot carry a duplicate at all. No heal ledger targets any of the 22:
`fund_cell_fix.json` (546 keys), `revop_cell_fix.json` (854) and `con_copy_heals.json` (19) all
intersect the duplicated set at **0**.

## Outcome

21 of 22 merged cleanly. **1 genuine conflict, left in place for adjudication** —
`APOLLOTYRE|20140331`, registered in `scripts/fund_dup_allow.json` with its evidence:

* `npCon` agrees (281.62) in both rows; only `annCon` differs — 20140530 vs 20140515.
* `20140515` = quarter-end **+45d**, the `apply_agg_pat_fills.py` placeholder convention
  (36,027 cells / 2,623 symbols carry that signature); `20140530` = quarter-end **+60d**, the era
  Clause-41 deadline for audited Q4 results. **Both are formulaic**, neither is a measured
  broadcast date.
* `scripts/pre2015_reads_d.json` carries that cell's `ann` as `20140515` **with
  `"ann_approx": true`** — the source itself declares it an approximation.
* The two candidates are not even independent: `20140515` is the appended row's own `annStd`
  copied into `annCon` by `update_fundamentals.py`'s sibling-basis rule
  (`if _r[3] is not None and _r[4] is None and _r[2] is not None: _r[4] = _r[2]`) on 2026-08-11,
  commit `47437af31`.
* Direction is not assumable: the runbook's OMAXE Mar-2012 case has the +45d placeholder *earlier*
  than the real filing — same shape, opposite answer.

To close it: read APOLLOTYRE's actual Q4-FY14 consolidated broadcast date from BSE/NSE (the P1/P2
re-dating route in `scripts/PLAN_QUANTMAC_FIXES.md`), write it through
`scripts/ann_date_fills.json`, drop the entry, and re-run the merger.
