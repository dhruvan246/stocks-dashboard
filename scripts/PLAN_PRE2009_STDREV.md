# PRE-2009 standalone-revenue campaign — plan + learnings handoff

Written 2026-08-25 by the session that closed 2009→2026 (std-rev now 98–100% every year).
This is the **next** campaign: the same job for **2002–2008**. Read this whole file before touching data.

## 0. The standing rules (they outrank everything below)
- **NO ASSUMPTIONS, NO GUESSWORK.** Every value written traces to something *measured this session*.
  Can't measure it → record "unknown". A plausible guess presented as fact is worse than an admitted gap.
- Read `scripts/DATA_RUNBOOK.md` first. Own worktree; file-scoped `git add` (NEVER `-A` / `-u` / `.`).
- Verify pushes **by content on origin**, not by exit code.

## 1. The bounded problem (measured 2026-08-25, whole store)

| Year | missing revStd **with** PAT anchor | **no** PAT anchor |
|---|---|---|
| 2002 | 437 | 0 |
| 2003 | 257 | 0 |
| 2004 | 338 | 0 |
| 2005 | 44 | 0 |
| 2006 | 32 | 0 |
| 2007 | 274 | 0 |
| 2008 | 119 | 0 |
| **total** | **1,501 across 322 symbols** | **0** |

Every missing cell HAS a PAT anchor → the anchor gate works everywhere. Biggest symbols:
M&M(19) ATLASCYCLE(19) SMLMAH(17) UNITECH(13) DENABANK(13) SHASUNPHAR/SCHAEFFLER/FEDERALBNK/
SUNDRMBRAK/STYRENIX/SWARAJENG/DALMIASUG/J&KBANK/KTKBANK/SUNDRMFAST/FOSECOIND/KANSAINER/SAMTEL/
WYETH/ZANDUREALT(12 each).

### ⚠️ The coverage ceiling — measure it before promising a number
N500 point-in-time coverage (last full scan): 2002 rev 1.2% / **PAT 23.7%**; 2003 56.1/70.4;
2004 67.3/83.4; 2005 80.5/90.6; 2006 93.4/95.1; 2007 91.1/96.3; 2008 91.3/97.8.
**revStd coverage cannot exceed patStd coverage**, because a fill needs an anchor. In 2002 the
binding constraint is missing fundamentals ROWS, not missing revenue. Filling all 437 anchored 2002
cells still leaves 2002 near ~23%. That residual is a *different* campaign (no-row / PAT-side; see
the pre-2015 tooling's "STEP F: NSE-archive EPS-recon for cells with no stored PAT anchor").
**Do not promise "2002 → 100%".** Measure `--explain` first, state the ceiling honestly.

## 2. Routes, in descending yield (all proven this session)

1. **MC as-filed deep batch** — reaches ~1997, serves the AS-FILED vintage (§108-safe).
   `python -X utf8 scripts/_mc_batch_fill.py --cells <cells.json> --emit <emit.json>`
   → `scripts/_mc_add.py < emit.json` → `scripts/_apply_reads.py`.
   Cells file shape: `{"alive":[SYM,...],"cells":{SYM:[qe,...]}}`.
   This filled ~2,000 cells in 2009-2026. **Expect it to be the workhorse again.**
2. **NSE archive** `scripts/_nse_archive_revop.py --gaps <gaps.json> --only SYM,SYM --out-suffix _x`
   (gaps shape `{SYM:[qe,...]}`). Pre-2018, keyed by the **NSE symbol** → survives BSE code changes.
   Declared scale + declared bank/non-bank. **CHECK `scripts/_nsearch_cache/` FIRST** — see §4.
3. **MC-raw `'--'` ⇒ nil.** Holdcos / pre-operational / suspended filers genuinely file NIL operating
   revenue. Fill **0** — `build_coverage_matrix.js:911/917` counts 0 as present (no zeroIsNull on the
   basis path). Confirmed nil this session: CAIRN COALINDIA IRB RAIN PVP UTVSOF SUMMIT RPOWER SFCL
   ONELIFECAP ORISSAMINE RTNPOWER UJJIVAN COFFEEDAY. Expect MANY more pre-2009.
4. **Own BSE filing read** — comparative columns + period identities: annual−9M, H1−Q2, 9M−Q2−Q3.
   Each identity must close on BOTH revenue and PAT before you trust it.
5. **Old-name / old-code identity** — MC `sc_id` under the FORMER name (PVP→`SSI`), BSE quote-search
   `api.bseindia.com/Msource/1D/getQouteSearch.aspx?Type=EQ&text=<name>&flag=gq`. **Gate on ISIN.**

## 3. The gates (non-negotiable — a batch without all four is not shippable)

1. **PAT anchor at apply time.** `_apply_reads.py` re-anchors every cell against stored
   `sf_fundamentals` npStd. ~1% rejection is healthy. **Anchor refusals are FINDINGS, not failures** —
   every refusal this session exposed a real defect (see §5).
   ⚠️ **Exact match beats tolerance.** The helper's `close()` allows `max(2.0, 3%)`; LICI Mar-22 slipped
   through at 1.6% and was a basis-swap. When both bases are plausible, demand the exact print.
2. **Per-symbol revenue convention**, determined against the symbol's OWN existing revStd — the
   applier does NOT gate the revenue LINE. `rev_total` for industrials; **Interest Earned** for banks
   (CORPBANK, CSBBANK, CENTRALBK, EQUITASBNK all confirmed). Pre-2009 is bank-heavy (DENABANK,
   FEDERALBNK, J&KBANK, KTKBANK, SOUTHBANK in the top-25) — **expect the bank convention often.**
3. **Series continuity** — filled-median vs existing-median per symbol; flag >3× / <0.33× and EXPLAIN
   each. Benign causes seen: growth, decline, demerger, and a median dragged to ~0 by later
   defunct-era zeros. None of ~40 flags this session was a real error, but each was checked.
4. **Cell-level diff vs origin before commit**: expect exactly N fills, **0 strays, 0 overwrites**.

## 4. Traps that cost time this session (do not re-learn these)

- **`_apply_reads.py` WRITES BY DEFAULT.** `--dry` prevents; `--apply` is a no-op flag. Always
  `git reset --hard origin/main` → re-stage → re-apply → diff, so a stale copy can't mask a peer's work.
- **NSE 403 on re-fetch ≠ unreachable.** OSWALGREEN's 2007 pages were already in
  `scripts/_nsearch_cache/`. **Read the cache before declaring a page unreachable.**
- **`--out-suffix` seeds the out file from the shared `_nsearch_reads.json`** (hundreds of symbols).
  Slim it back to your `--only` symbols before committing, or provenance is garbage.
- **Wrong ENTITY inside the right bundle**: a subsidiary's results filed under the parent's scrip
  (UJJIVAN→Ujjivan SFB; COFFEEDAY→Coffee Day Global). Disambiguate by **CIN / entity name on the audit
  report**. A dividend-recommendation filing is the listed parent's.
- **Wrong COMPANY by name**: OSWALGREEN vs Oswal Agro Mills (`sc_id OAM`, PATs -4.23/0.45). Gate on ISIN.
- **Round stored PATs are not automatically defects** — DALMIABHA's filings print whole crores.
- **Never heal on ONE disagreeing reader.** OSWALGREEN Mar-2007 (single reader 23.04 vs stored 23.69,
  unaudited Q4) was deliberately LEFT OPEN. That is the correct outcome, not a failure.
- **Held cells assert absence** (e.g. RAIN 2014-09 con) — they must stay absent or CI reds.
- A peer's MC-sourced fills are **MC-vintage (tier-B)**; an as-filed read outranks them — overwrite freely.

## 5. Defect classes to expect (each anchor refusal is a candidate)

- **§108 restated-vintage** — NSE keeps BOTH vintages; **earliest `filingDate` = as-filed** (§109a).
  Healed this session: DALMIABHA Sep/Dec-15, MTNL Mar-16, ASHIANA FY16 (whole row), THOMASCOOK,
  OSWALGREEN Dec-14. Pre-2009 predates Ind-AS, so expect FEWER of these — but re-filings still exist.
- **Basis duplication/swap** — std slot holding the con value or vice-versa (LICI ×5). If one slot is
  wrong, **check the other basis AND the revenue slot for the same quarter** (JSL Jun-22 was double).
- **Plain wrong value, no reader support** (JSL, EQUITASBNK, MINDACORP, BLUESTARCO).
- **Impossible arithmetic** — BLUESTARCO's 25.57 on PBT 63.48 implies a 60% tax rate ⇒ defect.
- **Whole-row contamination** — ASHIANA FY16 had all four quarters wrong; check siblings.

Heals go through ledgers, never raw edits: `fund_cell_fix.json` + `apply_fund_cell_fix.py` (npStd/npCon,
`basis` key matters) and `revop_cell_fix.json` + `apply_revop_cell_fix.py` (revStd/revCon). Both are
`was`-guarded and idempotent. State the primary document AND a second independent source in `why`.

## 6. Working loop

1. `node --max-old-space-size=8192 scripts/build_coverage_matrix.js --explain nifty-500 \
    --explain-from 2002-01-01 --explain-to 2008-12-31 --explain-out <out.json> --out <dir>` (~3 min).
2. Enumerate missing cells (npStd present, revStd absent) for the named symbols.
3. Batch via MC → validate 4 gates → commit → push (rebase loop) → **verify on origin by content**.
4. NSE archive for MC-less / renamed symbols; MC-raw nil check; filing reads for the rest.
5. Re-measure with `--explain` and report the honest number **plus the PAT-row ceiling**.
6. Record every un-fillable cell in `scripts/_campaign_suspects_2026_08_24.json` with the exact next step.

## 7. Known-open from the 2009+ campaign (do NOT re-hunt; different era)
OSWALGREEN 2007-03 (needs 2nd reader — **this one IS pre-2009, in scope**), AJMERA 2009-03 (no source),
ABSLAMC 2020-06 + AETHER 2020-12 (DRHP-only).
