# PLAN — XBRL FILER-FORMAT CAMPAIGN: close the last 2,252 cells of N500 coverage

**Written 2026-08-16 ~15:10 IST. Status: PHASE A DONE (2026-08-16 15:40 IST) — see verdict below.**

### Phase A results + the Phase B gate verdict (measured, supersedes §2's optimism)

Sweep: `scripts/xbrl_format_sweep.py` → `scripts/xbrl_filer_format.json` (committed, 2.3 MB) +
`scripts/_xbrl_format_refusals.json`. **110 of 127 resolved · 17 zero-rows (the insurers again —
CANHLIFE GICRE GODIGIT HDBFS HDFCLIFE ICICIAMC ICICIGI ICICIPRULI KENNAMET LICI NIACL NIVABUPA
PIRAMALFIN SBILIFE SPICEJET STARHEALTH TATACAP — all certainly file; §57a rule 1 applies) ·
4 short-lists (silent truncation suspects).**

**`scripts/xbrl_format_validate.py` verdict: the filename/flag shortcut is DEAD for writing N/A,
in both directions** (tested against what build_revop actually produced, per (symbol, quarter)):

| NSE signal | quarters | of which carry ebit | P(no ebit) |
|---|---|---|---|
| bank | 692 | **1** | 99.9% |
| nbfc | 892 | **795** | 10.9% |
| industrial | 3,530 | 1,140 | 67.7% missing |

- The single bank-flagged quarter WITH ebit is **INDIANB 20180630 ebit_con=383.28** — the known
  op-copy artifact, independently reconfirmed. Bank flag alone is a clean 2nd reader for the 33.
- **795 NBFC-flagged quarters DO hold ebit** — aggregator-era fills wrote an OP−Dep ebit for NBFCs
  that the XBRL branch refuses to produce. Two definitions of one series
  (`feedback-two-files-one-quantity` class). **USER DECISION needed before Phase C:** fill the
  missing NBFC quarters to match the 795 (data reality; the engine already screens on them), or
  declare NBFC ebit N/A and retract the 795 (retraction touches every ledger). Do not proceed on
  either silently.
- **2,390 industrial-flagged quarters missing ebit across 70 names** (ARE&M 60, ABREL 57,
  LICHSGFIN 55, SHRIRAMFIN 55 …) are REAL extraction gaps → the Phase D fill list.

Parent campaign: `scripts/N500_COVERAGE_100_CAMPAIGN.md` (P0–P2 done and live).

> **Golden rule:** never assume, never guess. Every value written and every claim made must trace to
> something measured or read this session. Can't measure it? Say "unknown".
> **User constraints for this campaign:** *"dont assume"* — no verdict by category, ever; and
> *"do not miss any. work on every single stock"* — all 127 names, tracked to resolution.

---

## 1. Why this campaign exists

Live Nifty 500, 2020-01 → date is at **2,252 missing member-dates** (was 8,383 this morning). The
residue is dominated by `ebit` (1,253) and `op` (398), and **P3 established the root cause**: our own
XBRL extractor `scripts/build_revop.py::metrics_for()` returns `ebit = None` for four filer formats,
each detected by tag —

| format | detected by | line | ebit |
|---|---|---|---|
| life insurer (IRDAI LI) | `NetPremiumIncome` | :236 | `None` |
| general insurer (IRDAI GI) | `PremiumEarned` | :250 | `None` |
| bank | `InterestEarned` + `OperatingProfitBeforeProvisionAndContingencies` | :261 | `None` |
| NBFC Ind-AS | `InterestEarned`, no bank op tag | :276 | `None` |
| **industrial** | none of the above | :280 | `PBET + FinanceCosts − OtherIncome` |

So an `ebit` hole is either **the filer's format has no such line** (→ N/A, evidence-backed) or **an
industrial filer we failed to extract** (→ a real fill). Telling those apart per name is this
campaign. **Do NOT decide it from the pattern in `sf_revop`** — that is the circular inference
DATA_RUNBOOK §63 exists to forbid, and §63 is USER-CAUGHT precedent that was **63% wrong**.

## 2. ★ The route is far cheaper than expected — measured 2026-08-16, do not re-derive

**You do not need to download XBRL files to determine format.** The NSE per-symbol filing list
returns, for every filing, a `bank` flag AND an `xbrl` URL whose **filename prefix encodes the
format**:

```
GET https://www.nseindia.com/api/corporates-financial-results?index=equities&symbol=<SYM>&period=Quarterly
```
warm a cookie jar first on `https://www.nseindia.com/companies-listing/corporate-filings-financial-results`
(the bare root **403s**; the listing page returns 200 — measured today).

Measured responses:

| symbol | rows | xbrl prefixes seen | `bank` flag |
|---|---|---|---|
| SBIN | 145 | `BANKING` | `B` (all) |
| HDFCBANK | 103 | `BANKING` | `B` (all) |
| SHRIRAMFIN | 121 | `NBFC_INDAS` | `F` |
| BAJFINANCE | 110 | `NBFC_INDAS`, `INDAS`, `NONINDAS` | **`F` 50 / `N` 60 — MIXED** |
| SUNPHARMA | 163 | `INDAS` | `N` (all) |

Useful row fields: `bank` (`B`/`F`/`N`), `xbrl`, `fromDate`, `toDate` (quarter end), `period`,
`filingDate`, `broadCastDate`, `consolidated` (`Consolidated`/`Non-Consolidated`), `financialYear`.

**★ BAJFINANCE is the whole point: the flag FLIPS over time, both directions.** Format is a property
of the FILING, not of the company. That is very likely why 94 names have `ebit` in *some* quarters —
their series stops at a format transition rather than having a hole. **Every verdict must therefore
be date-bounded**; the N/A ledger already supports `from`/`to` per entry
(`build_coverage_matrix.js::naLedgerHit`).

## 3. ★★★ Traps measured today — every one of these will bite

1. **`rows: 0` is NOT absence** (§57a rule 1). Measured today: **SBILIFE → 0 rows, ICICIGI → 0 rows,
   SPICEJET → 0 rows**, all of which certainly file quarterly results. Insurers may sit under a
   different index/segment; SPICEJET may be a symbol/alias issue. A zero must be recorded as
   `not-found-via:nse-list` and escalated down the §57 ladder — **never** written as "no filings".
2. **Silent truncation.** **ABBOTINDIA returned 1 row** while holding 31 quarters in `sf_revop`.
   A short list is a diagnosis, not an answer (`feedback-endpoint-caps-are-silent`).
3. **`xbrl` can be the literal string `"-"`** (not null, not missing). BAJFINANCE 2019 rows show it.
   Truthy-test the value, don't test the key.
4. **Date-range queries silently return 0.** `?from_date=…&to_date=…` returned `rows: 0` for a
   window certain to contain filings. Use `?symbol=…&period=Quarterly`, which is what
   `_nse_archive_revop.py:128` already does.
5. **Aliases.** Renamed symbols lose their pre-rename rows. `_nse_archive_revop.py` already has the
   alias + fresh-jar retry pattern (*"Retry once with a fresh jar, and SAY when an alias list is
   lost"*) — reuse it, don't reinvent it.
6. **NSE lockdown risk** (`project-stocks-nse-api-lockdown`): an all-transport 403 means stop and
   fall back, not hammer. Rate-limit politely (≥1.5 s between calls; the whole sweep is ~127 calls).

## 4. Scope — the 127 names (measured from `scripts/n500_cov_queue.json`)

107 with `ebit` gaps + 67 with `op` gaps, **47 overlapping → 127 unique**.

**`op`-only (20)** — note these include banks, and banks DO get an `op` value, so these are real
fills, not format exclusions: ADANIENSOL, CENTRALBK, CIPLA, CSBBANK, CUB, DCBBANK, DLF, EMAMILTD,
FEDERALBNK, IDBI, IDFC, IDFCFIRSTB, JSWENERGY, KARURVYSYA, MAHABANK, MANKIND, SBIN, SRF, TATACHEM,
TMB.

The full per-name lists with affected month-ends live in `scripts/n500_cov_queue.json` (428 rows,
every one `status: open`, `class: needs-source`). **That file is the work list — do not re-derive it.**

Also in scope, carried over from P1/P2:
- **`INDIANB`** — unadjudicated. Our data holds ONE `ebit` quarter (`20180630`, `ebit_con == op_con
  == 383.28`), a copy artifact. The ledger's own guard refuses to N/A over it. Retract the stray
  first (annotate EVERY ledger — `feedback-retraction-needs-every-ledger`), then it becomes bank-format.
- **30 of 33** banking N/A entries still carry `reader_2: "NOT YET SECOND-READ"`. This campaign
  supplies the proper second reader: the filing's own format from NSE. That RETIRES the aggregator
  dependency — Moneycontrol blocks scripted fetches and its URL codes are not derivable.
- **SPICEJET** — already confirmed industrial via screener (Operating Profit AND Depreciation
  populated in all 13 shown quarters); `op` present in only 4 of 22 `sf_revop` quarters. A real fill.

## 5. Phases

**A — Sweep the list API (no writes).** For each of the 127 (+ aliases): fetch, cache raw JSON to
`scripts/_nse_list_cache/<SYM>.json`, record per row `(toDate, consolidated, bank, xbrlPrefix,
filingDate)`. Emit `scripts/xbrl_filer_format.json` = `{SYM: {QE: {basis: {bank, prefix, url}}}}`.
Log every 0-row and short-list name to a refusals file with `not-found-via:nse-list`. **Gate: the
sweep must report how many of 127 resolved, and name every one that did not** — a silent partial is
the failure mode here.

**B — Adjudicate format per (symbol, quarter).** Map `bank`/prefix → the `metrics_for` branch:
`B`/`BANKING` → bank · `F`/`NBFC_INDAS` → NBFC · insurer prefixes → LI/GI · `INDAS`/`NONINDAS` +
`N` → industrial. **Cross-check the mapping against `metrics_for`'s tag logic on a sample by
actually downloading ~10 XBRL files** — the prefix is a filename convention, and a convention is a
hypothesis until tested. Where prefix and tags disagree, tags win (they are what the builder reads).

**C — Write date-bounded N/A.** Quarters whose format is bank/NBFC/LI/GI → `coverage_na_ledger.json`
entries with `from`/`to` and per-name evidence quoting the filing URL and its prefix. Industrial
quarters missing `ebit`/`op` → they stay visible and become D's fill list. **Parity gate:** re-bake,
and the drop in `ebit`/`op` missing must equal the number of cells the new ledger entries cover —
`feedback-ledger-guard-count-must-move`. Run the heal TWICE; the second pass must change 0 cells.

**D — Fill the industrial residue** via the §57 ladder (BSE detres → NSE archive → BSE announcement
PDF → XBRL → comparative columns → FY identity → vision §17b). Provenance on every cell. Batch ~10
names: rebuild → bake → parity → push → LIVE check.

**E — Ship & close.** Full CI bake (**never commit a local payload** — the release asset lags CI by
~2 days; P0 note 1). Verify LIVE against origin, not logs. Update
`project-stocks-n500-coverage-2020-residue`, the parent campaign's status line, and add a RUNBOOK §
for the filer-format route.

## 6. Non-negotiables

- **Never commit a locally-baked payload.** Push code/ledgers, then `gh workflow run
  refresh-coverage.yml`. GitHub Pages does **not** redeploy on CI's own payload commits — a later
  push, or a manual Pages run, is what makes it live. Verify by fetching the live URL.
- **Own files only**; this campaign runs in `~/stocks-wt/n500-cov` (already exists) or a fresh
  worktree. Stage explicit paths, never `git add -A`.
- **A 0-row / short list / empty route is a DIAGNOSIS.** Log `not-found-via:<routes>` and walk the
  ladder. The words "unfillable" / "never filed" need a primary document (§57a rule 2).
- **No category verdicts.** SPICEJET — an airline — sat in the "never has ebit" set and would have
  been buried by a sector rule; 12 insurers + SPICEJET + LAKSHVILAS (~670 cells) were nearly N/A'd
  by one. Per-name evidence or it does not ship.
- **Expected end state:** `ebit`/`op` reach ~100% with the format exclusions carrying primary-document
  evidence, and the residue is a small, named, genuinely-missing set. If a class cannot be resolved,
  say so with counts — a visible sub-100 that is honest beats a 100% that hides a defect.

## 7. Current live baseline to beat (payload `2026-08-16 14:43 IST`, verified live)

| param | missing | param | missing |
|---|---|---|---|
| ebit | 1,253 | profitTTM | 67 |
| op | 398 | composite | 67 |
| fiiChgPp | 174 | profitAccel | 39 |
| diiChgPp | 174 | profitYoy/Base/Streak | 24 each |
| delivPct | 7 | rev | 1 |
| | | **TOTAL** | **2,252** |

31 of 43 parameters already read exactly 100%; Phase E must confirm they still do.
`fiiChgPp`/`diiChgPp` (174 each) and `delivPct` (7) are NOT in this campaign — they belong to
`PLAN_SHP_4DP_FULL.md` and the §88b MTO route respectively.
