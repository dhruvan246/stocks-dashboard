# B — Net-profit fundamentals defect screens (LIVE sf_fundamentals.json)

Audit date: 2026-09-01. Data: LIVE origin/main exports in `$SP/live/` (sf_fundamentals.json:
3,956 symbols; scripts/fundamentals.json; docs/quarterly_results.json). All counts below were
measured this session by `$SP/screens_main.py`, `$SP/screens_b.py`, `$SP/finalize.py`.
Machine-readable evidence: `$SP/findings/B_evidence.json`. Repo working tree untouched.

Store roles (established from `git show origin/main:` builders, not assumed):
- `docs/sf_fundamentals.json` — THE store the backtest engines load; upserted daily by
  `update_fundamentals.py`; heals route through ledgers (`fund_cell_fix.json` via
  `apply_fund_cell_fix.py`, which writes this file AND the scripts mirror).
- `scripts/fundamentals.json` — upstream NSE-XBRL bulk store (`build_fundamentals.py`,
  MIN_QE 20170101). sf is a strict superset (31,903 cells only in sf; 0 only in upstream).
- `docs/quarterly_results.json` — baked FROM sf_fundamentals by `build_quarterly_results.py`
  (last 13 standard calendar quarters; exact-qe dict match; not an independent reader).

---

## Screen 1 — Structural

Ran: per-symbol duplicate-qe, sort order, calendar validity, row shape, slot types.

**Counts: dup_qe = 1 · out_of_order = 8 rows (8 symbols) · bad_qe = 0 · bad_len = 0 · bad_type = 0.**

**1a. Duplicate row — KNOWN-OPEN (fresh count 1).** `scripts/DUP_QUARTER_ROWS.md` measured 22
dup pairs (CARBORUNIV 12, SUNPHARMA 7, ADVANTA 2, APOLLOTYRE 1); LIVE now holds exactly one:
- APOLLOTYRE 20140331 — row A `[20140331, null, null, 281.62, 20140530]`, row B
  `[20140331, 129.62, 20140515, 281.62, 20140515]`. First-match readers (apply_fund_cell_fix,
  apply_owners_full) see row A (npStd invisible); the backtest's backwards scan lands on row B.
  Two reader idioms, two different answers for the same quarter.

**1b. Out-of-order rows — CONFIRMED defect, appears NEW** (no runbook/doc match found). 8 symbols
have a recent row appended after later quarters, so the array is not sorted by qe:
OMKARCHEM (tail `…20260331, 20250331, 20260630`), STCINDIA (`…20260630, 20260331`),
FELIX (`…20260630, 20250630`), SECL (`20260331, 20250331`), SAHAJSOLAR, KNACK, VOEPL, XTRANET.
Blast radius (measured in reader code): both backtest engines
(`docs/stock-backtest.html` ~line 1066, `docs/backtest-engine.js` ~line 649 area) find the
"current" quarter by scanning BACKWARDS from the array end assuming sorted order — for these 8
symbols the engine can pick a stale quarter as current (the TTM pairwise-gap guard then nulls TTM,
but yoy/base/resultDate come from the wrong row). `docs/stock.html` re-sorts (`ends=…sort()`,
line 536) and `build_quarterly_results.py` keys a dict — both safe.
Heal route: these rows entered via an applier/writer that skipped the final sort; fix is a
reviewed one-off re-sort (and root-causing which writer appended) — sf_fundamentals is
CI-rebuilt/merged, so land it the way DUP_QUARTER_ROWS.md prescribes for structure (own session,
guard added to `fund_dup_guard`-style pre-commit), not a hand edit.

---

## Screen 2 — Power-of-ten scale steps (per basis, vs 4-nearest-neighbor median)

Ran: |np| vs median of 4 nearest non-null neighbors; bands ~8–12x and ~80–120x both directions;
floors ≥1 cr; level-shift and edge annotations; then a seasonality refinement (drop cells within
3x of the SAME quarter ±1 year — Q4 holdco dividend spikes like COALINDIA std are seasonal, not
defects) and a strict band (9–11x / 90–110x).

**Counts: raw flags 3,634 · interior-clean 2,236 · non-seasonal interior 1,365 · strict band 726
· strict excluding the Screen-12 unit-class symbols 681.**

Severity: **SUSPECT needs doc-read** as a population — calibration against known cases shows the
screen fires on GENUINE exceptional quarters: IDEA 20190930 con −50,921.9 (the real AGR-provision
loss), TATASTEEL 20230930 con −6,196.24, IOB 20180331 std −3,606.73, BANKBARODA 20160331 std
−3,230.14 — all real one-offs sitting in-band. This matches runbook §-note that
`detect_scale_errors.py`'s ~251 np suspects are "mostly real one-offs". Do NOT bulk-correct.
Strongest unexplained candidates (interior, non-seasonal, strict band, not unit-class):
- HMT 20240331 con **2,641.10** vs nbr med 28.88 (×91.4 — the only large ×100-band interior hit)
- CHEMCRUX 20250630 con 11,475.0 vs nbr med 1,098 (whole neighborhood already inflated — see Screen 12)
- IDEA 20260331 std 52,022 / con 51,970 vs nbr ~5,400 (×9.5; post-knowledge-cutoff quarter,
  same value in upstream store — needs a filing read, could be an AGR-era one-off)
- ADANIENT 20251231 std 6,295.99 vs 697 (×9.0); RENUKA 20190930 con 2,817.9 vs 259.6 (×10.9);
  RELINFRA 20230331 con −2,705.31 vs 280.8 (×9.6); BHARTIARTL 20200930 std −846.0 vs 7,614.5 (÷9)
Heal route: adjudicate per cell with the runbook's anchors (YTD arithmetic / cross-basis /
neighbors); filer-side XBRL scaling → `scripts/scale_fix.json`; parse/store-side wrong values →
`scripts/fund_cell_fix.json` (+ rebuild derived).

---

## Screen 3 — Cumulative-in-quarter (H1/9M/FY figure stored as one quarter)

Ran: value ≈ (neighbor-median estimate + previous 1 quarter) or (est + previous 3) or exactly
(prev1+prev2), within 2%, AND |v| > 2.5× neighbor median, floors ≥0.5 cr.

**Count: 381 flags.** Severity: **SUSPECT needs doc-read** — the pattern also matches genuine
exceptional quarters exactly because "big quarter ≈ sum of small ones" is weak arithmetic
evidence: known-genuine hits include ITC 20250331 con 19,727.37 (hotels-demerger exceptional),
ULTRACEMCO 20200331 con 3,242.77 (deferred-tax credit), DHFL 20200331, ADANIPOWER 20220630 con
4,779.86 — all real. The convincing true positives sit in the unit-class symbols:
- GUJJUBHAI 20220331 std+con 1,586,379 ≈ est+prev3 (an FY cumulative in a wrongly-scaled series)
- TNTELE 20220331 std −10,443.68 ≈ est+prev3 (lakh-scaled era, see Screen 12)
- RELCAPITAL 20201231 con −4,018 ≈ est+prev1 (H1-shape; needs filing read)
Heal route: per-cell filing read → `fund_cell_fix.json`. (This is the ANN-LAG+CUM-IN-QTR PARKED
class from memory — treat these 381 as the fresh screen volume for that campaign, not new heals.)

---

## Screen 4 — Isolated sign flips (opposite sign to all 6 nearest neighbors, |v| > 3× median)

**Count: 540 flags.** Severity: **SUSPECT only** (per spec) — genuine one-off losses/gains
dominate the top of the list: RNAVAL 20221231 std +19,026.49 (resolution-era debt write-back
shape), VEDL 20200331 con −12,521 (real impairment quarter), HINDPETRO 20260630 con −12,264.67
(post-cutoff, needs read). Cells worth a doc-read first:
- GOLDENCREST 20210930 std+con −151,899 (unit-class symbol, impossible magnitude)
- BHARTIARTL 20170331 std −15,083.1 vs nbr med 458 (×32.9 — Airtel's famous AGR loss was Mar-2020,
  not Mar-2017; a −15k cr standalone quarter in FY17 does not match the known record → strong candidate)
- SAMMAANCAP 20260331 std −8,455.11 vs 283 (×29.8); IDEA 20260331 (same cell as Screen 2)
Heal route: filing read per cell → `fund_cell_fix.json`.

---

## Screen 5 — Long exact-duplicate runs (same non-zero float, 3+ consecutive quarters)

**Count: 57 runs / 193 cells.** Severity: **benign-leaning SUSPECT.** Every run ≥4 is a tiny
value (|v| ≤ 0.08 cr): CGFL std 0.04 ×6 (20200630–20210930), RRP ±−0.01 ×5, MRUTR −0.01 ×5,
SHRJAGP −0.06 ×5, DHRUVCA 0.01 ×4, KETOMOTORS −0.01 ×4, UMESLTD 0.01 ×4, TPHQ −0.01 ×4,
HIIL 0.08 ×4. At 2-decimal rounding a micro-cap genuinely printing ~₹1 lakh PAT repeats 0.01, so
these are plausibly real; the carry-forward-sentinel version of this class (HAWKINCOOK −7.0/8.0,
VOITHPAPR −0.5) is already KNOWN in the runbook. No large-value duplicate runs exist (largest
duplicated value in a ≥3 run: 0.08). No heal without a filing read; route `fund_cell_fix.json`.

---

## Screen 6 — Cross-basis scale echo (npCon ≈ npStd ×10 or ×100 within 1%)

**Count: 64 cells.** Severity: **SUSPECT, high prior for the ×100 subset** (an exact ×100
same-sign match within 1% is rarely a coincidence; ×10 within 1% has a real chance-collision
rate). Worst examples:
- SAPPL 20180930 std 7.96 / con 795.63 and 20210331 std 9.63 / con 959.85 (both ×100)
- HBESD 20240331 4.52/451.76, 20240630 0.91/90.75 (con=std×100) and 20250331 std 458.17 / con
  4.58 (std=con×100 — the SAME symbol flips direction, i.e. one slot is wrong per quarter)
- SWASTIKA 20241231 6.08/611.43; SHRIKRISH 20250331 2.49/249.01; MUL 20240930 4.68/463.45;
  PHARMAID 20230930 −0.5/−50.44; GHVINFRA 20241231 std 295.62 / con 2.96; NFL 20221231 std
  588.62 / con 5.89; BEEKAY 20200630 std 446.37 / con 4.46; RKFORGE 20241231 99.55/996.14 (×10)
Heal route: read both bases from the filing (§59b ladder), then `fund_cell_fix.json`; if the
filer's own XBRL carries the power of ten → `scale_fix.json`.

---

## Screen 7 — std==con exact-equality runs (≥8 consecutive quarters)

Per runbook §59 this screen is NOT a defect count — reported as measured SUSPECT volume only.

**Counts (fresh, LIVE): 267 runs / 245 symbols / 4,006 cells in ≥8-quarter calendar-consecutive
runs; 124 runs (2,210 cells) touch pre-2020. For context: std==con exact equality of any run
length = 10,064 of 59,663 both-basis cells (16.9%); 2,141 pre-2020.**

Severity: **KNOWN-OPEN** — this is exactly the con-copy / con-nofile-retraction campaign's screen
(`PLAN_CON_COPY_RETRACTION.md` measured 513 symbols with ≥8q runs on 2026-08-18; LIVE is now 245,
so the retraction has consumed roughly half; the ~379-cell con-nofile residue is part of what
remains). §59's audit: interior equal-runs are overwhelmingly GENUINE for no-subsidiary filers;
the defect lives per-cell (insurers). Top offenders (run length): ABBOTINDIA 57q
(20120331–20260331), BAYERCROP 48q, CANHLIFE 46q, TATAELXSI 45q, CASTROLIND 39q, TTML 38q,
SBILIFE 37q (insurer — §59's confirmed-defect habitat), CANFINHOME 36q, KENNAMET 35q,
FACT 33q, ICICIGI 33q (insurer). No new heals proposed here; the campaign owns it.

---

## Screen 8 — Calendar gaps between first and last covered quarter (per basis)

Ran: pairwise month-stepping (round(Δmonths/3)−1) per basis, which auto-tolerates off-cycle
fiscal patterns; off-cycle detection first: **0 symbols** have >20% non-Mar/Jun/Sep/Dec quarter
ends (this store is fully quantized to standard quarter-ends), so no off-cycle false gaps exist.

**Counts: missing std cells 11,549 · missing con cells 6,220. Fillable-interesting (the symbol's
OTHER basis has a value in that exact quarter): std 475, con 2,077.**
By-year peaks: std 2017 = 1,384 (largest single-year hole), 2019 = 872, 2013–16 ≈ 540–600/yr;
con climbs from 2018 (648) and stays 530–830/yr through 2025. Top-15 gap symbols (both bases):
TIRUMALCHM 64, TNTELE 63, RSSOFTWARE 62, SPIC 60, CYBERTECH 59, ATCOM 58, ASHIMASYN 58,
AXISCADES 56, CREATIVEYE 56, CANDC 56, MADRASFERT 54, ONWARDTEC 51, BIRLACABLE 50, REGANTO 50,
ERAINFRA 50 (mostly long-history names with sparse pre-2012 eras, e.g. TIRUMALCHM spans
20021231–20260630 with only 34 rows).
Severity: **coverage gap, not corruption** — KNOWN territory (fav14/N500 coverage campaigns).
Heal route: backfill campaigns per runbook §2/§6/§17 (NSE archive first, BSE, aggregators);
the 2,077 con-fillable cells must be filing-backed reads, NEVER std-copies (con-copy lesson).

---

## Screen 9 — Cross-store disagreement (>1% AND >0.1 cr)

**9a. quarterly_results.json vs sf_fundamentals: 49,216 comparable PAT cells → 0 disagreements;
0 qr PAT cells lack an sf row.** Benign-explained: qr is baked from sf and the bake is fresh
(updated 2026-09-01 15:02 IST).

**9b. scripts/fundamentals.json vs sf_fundamentals: 138,713 comparable cells → 14 disagreements**
(none power-of-ten-shaped). Severity: **CONFIRMED live reader-vs-store conflicts, needs per-cell
adjudication**. All 14 are con-basis-heavy; a striking sub-pattern (5 of 14): sf holds a
tiny/zero con value where upstream holds a plausible one — the shape of the §116 owners-tag
family ("filer's owners=0 mis-tag", NCI=0 refusals):
- GLENMARK 20260331 con: sf −0.10 vs upstream 301.41 (std −73.9 agrees in both)
- KIRLOSBROS 20250630 con: sf 0.80 vs 66.70 (std 47.0 agrees)
- SHYAMMETL 20250930 con: sf −1.31 vs 261.76 (std 135.61 agrees)
- TRU 20260331 con: sf 0.00 vs −58.60 · TALBROAUTO 20250331 con: sf 0.00 vs 26.58
- others: GODREJPROP 20171231 con 25.94 vs −54.75; ROLTA 20220630 con −354.49 vs −287.96;
  AARTIDRUGS 20240630 con 33.27 vs 53.91; MMTC 20240630 con 32.69 vs 44.26; NITCO 20250331 con
  −2.85 vs −0.05; GSPCROP 20260331 con 20.48 vs −0.84; MAXHEALTH 20210331 con 69.69 vs 70.50;
  plus 2 more in B_evidence.json.
Because sf is the authoritative healed store, a disagreement does not name the wrong side
(memory rule): each needs the filing's owners figure. Heal route: `fund_cell_fix.json`
(guarded on `was`, writes both stores, killing the disagreement whichever side wins).

---

## Screen 10 — TTM window integrity (4 array-adjacent rows not calendar-consecutive)

Ran: every 4-row sliding window per symbol — raw array, and per-basis non-null-filtered arrays
(what a filtered-adjacent reader would consume).

**Counts: raw 7,052 / 100,281 windows (7.0%) · std-filtered 6,902 / 99,140 (7.0%) ·
con-filtered 4,851 / 52,356 (9.3%) non-contiguous.** That is the silent-wrong-TTM exposure any
array-adjacent reader carries.

Reader audit (code read, not assumed):
- `docs/stock-backtest.html` profitMetrics (~line 1080) and `docs/backtest-engine.js`
  (~line 661): pairwise 3-month step check present (the e6/e7 fix) — **guarded**.
- `docs/stock.html` (~line 590): `window4` requires `qIdx` increments of exactly 1 — **guarded**.
  (Note its qIdx buckets by month÷3, so a hypothetical Dec→Feb off-cycle step would pass as
  contiguous; measured exposure today = 0 because the store has zero off-cycle quarter-ends.)
- `docs/quarterly-results.html`: no TTM computation; consumes the pre-aligned bake — **safe**.
Severity: engines **fixed**; the counts above quantify the risk any FUTURE reader re-introduces
by walking array-adjacent rows. No data heal — this is a reader-contract number to keep.

---

## Screen 11 — Everything else noticed

- **ann = 0 legacy sentinel: 670 slots** (readers already treat 0 as date-unknown via `ann>0`
  tests — benign by contract, but any new reader using `!=null` regresses; §15/§91).
- **ann ≤ qe: 0 · invalid ann dates: 0 · future ann: 0 · non-month-end qe: 0 · NaN/type
  errors: 0.** The impossible-ann belt in the baker has nothing to drop today.
- **np exactly 0.00: 1,236 cells** (0 is a legitimate rounded value here, unlike price-side 0
  sentinels — no action, but screens must not conflate with null; this audit did not).
- **ann-lag > 400 days: 957 slots** — benign-explained pattern verified on worst cases: suspended
  companies filing years of results the same day (AIFL filed Mar-2019/20/21/22 results all on
  2025-10-29; BALLARPUR similar). Point-in-time engines handle this correctly by construction.
- **quarterly-results page silently drops non-standard quarter-ends** (`qidx.get(r[0])` exact
  match). Exposure measured today: 0 rows (no off-cycle qe in store) — a latent contract, worth
  a comment, not a defect.

## Screen 12 (extra, from Screen 11 chase) — WHOLE-SERIES / ERA WRONG-UNIT SYMBOLS

The biggest new finding. Ran: cells with |np| > 60,000 cr, or |np| > 3× mcap (dash_slim meta)
and > 50 cr; then per-symbol series-median test (median|np| > 5× mcap AND > 500 cr) to separate
whole-series unit corruption from genuine distressed losers.

**Counts: 920 impossible-magnitude cells across 151 symbols; 201 cells with |np| > 60,000 cr
(flatly impossible — the largest verified-genuine quarterly figure in the store is IDEA's
−50,921.9 cr); 13 symbols fail the series-median test; 354 cells > 100 cr sit inside those
13 symbols.**

Severity: **CONFIRMED defect class** for the median-test symbols minus known/genuine exceptions:
- NEW (no runbook mention found by grep): **MANAS** (series median |np| 1,761,312 vs mcap 162 —
  values are raw rupees stored in the crore slot), **PURPLEFIN** (282,275 vs 420),
  **SGFRL** (184,272 vs 214), **ATVOENT** (131,389 vs 442), **NSL** (51,399 vs 218),
  **KSSMART** (40,901 vs 1,813), **OMEGAIN** (32,215 vs 232 — sample: 20180331 std=con
  −400,673.0), **GUJJUBHAI** (22,258 vs 208), **TNTELE** (era-scoped: pre-2020 rows are sane
  −0.03…−3.95, the 2021-22 era is lakh-scale, e.g. −2,486.26), **SVJ** (1,092 vs 111, SUSPECT).
  Several also carry std==con exact copies on the same wrong values (OMEGAIN, MANAS, NSL…) —
  two defects on one row.
- KNOWN: **QUINT** (runbook: "KNOWN-UNFIXED, needs its own session"; measured median 4,469 vs
  mcap 196, 35 flagged cells), **AIRFLOA** (runbook: ×1e4 both bases, ann=0). The LEHAR/
  TECHNVISN/NOVELIX lakh-as-crore class is the same disease; those three no longer trip the
  cell-level screen (healed), these 10+ do.
- **RCOM** trips the median separator (median 1,171 vs mcap 221) but its multi-thousand-crore
  losses are on the public record — classified genuine-distressed, which shows the separator's
  limit: the remaining 137 "distressed-or-genuine" symbols (RCOM, ABAN, FEL, FLFL, HDIL,
  SANWARIA, CHEMCRUX, ELITECON, GOLDENCREST, LIKHAMI, STANCAP, PANORAMA…) need per-cell
  adjudication — ERA-scoped corruption (like TNTELE, CHEMCRUX 2021-25, GOLDENCREST 20210930
  −151,899) hides from a whole-series median.
Impact: these symbols poison any |PAT|-ranked factor and the results-season aggregates.
Heal route: per-cell filing reads → `scripts/fund_cell_fix.json` (+ mirror), rebuild derived;
QUINT stays its own root-cause session per the runbook; consider adding an mcap-sanity tripwire
to `detect_scale_errors.py` so the class cannot re-enter via the SME/BSE fill routes.

---

# Summary table

| Screen | Measured count | Severity |
|---|---|---|
| 1a dup qe rows | 1 (APOLLOTYRE 20140331) | KNOWN-OPEN (was 22, 21 healed) |
| 1b rows out of sort order | 8 rows / 8 symbols | CONFIRMED, new; hits both backtest engines |
| 2 power-of-ten steps | 3,634 raw → 726 strict → 681 excl. unit-class | SUSPECT (known-genuine calibrated); HMT ×91 top candidate |
| 3 cumulative-in-quarter | 381 | SUSPECT (pattern matches genuine exceptionals) |
| 4 isolated sign flips | 540 | SUSPECT (per spec); BHARTIARTL 20170331 std top candidate |
| 5 exact-duplicate runs ≥3 | 57 runs / 193 cells (all ≤0.08 cr) | benign-leaning SUSPECT |
| 6 cross-basis ×10/×100 echo | 64 | SUSPECT, high prior on ×100 subset |
| 7 std==con runs ≥8 | 245 syms / 4,006 cells (124 runs pre-2020) | KNOWN-OPEN (§59 + con-copy campaign; NOT a defect count) |
| 8 calendar gaps | 11,549 std + 6,220 con missing; 475+2,077 other-basis-fillable | coverage, KNOWN campaigns |
| 9a qr vs sf | 0 / 49,216 | benign (derived, fresh bake) |
| 9b upstream vs sf | 14 / 138,713 | CONFIRMED conflicts; 5 look §116-owners-shaped |
| 10 non-contiguous 4-row windows | 7,052 raw / 6,902 std / 4,851 con (7–9.3%) | risk quantified; all 3 readers verified guarded |
| 11 ann=0 / lag>400d / np=0 | 670 / 957 / 1,236 | benign-by-contract / explained |
| 12 impossible-magnitude (unit class) | 920 cells / 151 syms; 13 whole-series syms; 201 cells >₹60k cr | CONFIRMED class, ~10 NEW symbols beyond KNOWN QUINT/AIRFLOA |
