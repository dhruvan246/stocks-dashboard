# C — sf_revop.json defect screens (LIVE origin/main export, run 2026-09-01)

Store audited: `$SP/live/sf_revop.json` — 3,601 symbols, 99,512 rows. Keys are bare `YYYYMMDD`
strings (no `qe` prefix; builder docstring `{SYM:{"QE":[...]}}` confirms — the task sketch's
`qeYYYYMMDD` does not match the live file). Row = [revStd, revCon, opStd, opCon, patStd, patCon,
fin, ebitStd, ebitCon], ₹ crore. All numbers below measured this session by
`$SP/audit_revop.py` + follow-up probes; machine-readable arrays in `findings/C_evidence.json`.

## Summary

| # | Screen | Count | Severity |
|---|--------|-------|----------|
| 1 | Structural (keys/rows/types/dup-case syms) | 0 / 0 / 0 / 0 | benign |
| 2a | Negative revenue | 184 cells | mostly benign (as-filed), tail SUSPECT |
| 2b | \|op\| > 1.5×\|rev\| (rev>10) | 530 (477 fin≠1) | mostly benign (distress qtrs), tail SUSPECT |
| 2c | ebit > op + 0.01 | 169 (127 beyond rounding) | CONFIRMED (impossible by construction) |
| 2d | rev==0 mid-series | 2,482; 22 with both neighbors >10 Cr | SUSPECT (the 22) |
| 3 | Power-of-ten scale steps | 415 flags | SUSPECT; ~dozen strong ÷100 candidates |
| 4 | Cumulative-in-quarter | 1,909 H1-band + 281 9M/FY-band | SUSPECT (band is seasonality-heavy); AKZOINDIA-class confirmed via §100 retraction |
| 5 | std==con runs ≥8 | 382 syms / 6,778 cells; 73 era-boundary syms | KNOWN-OPEN (§59/§85/§100) |
| 6 | PAT mirror vs sf_fundamentals | 135 tolerance-diffs (exact: 175 con + 73 std); mirror-only 4,474 | KNOWN-OPEN §70/§71 — but REGROWN 23→175 |
| 7 | vs quarterly_results.json | 0 of 47,144 | benign (bake in sync) |
| 8 | Coverage-era cliffs | cliffs at 2003, 2013(con), 2015(con), 2018 | KNOWN-OPEN (extraction eras) |
| 9 | op-margin outside [-1, 0.9], rev>50, fin≠1 | 395 | mostly benign, tail SUSPECT |
| 10a | Stray alias key BAJAJAUTO vs BAJAJ-AUTO | 1 key / 1 row | CONFIRMED |
| 10b | Rename-orphan keys (in revop, absent from sf_fundamentals) | 186 keys | CONFIRMED hygiene; AKZOINDIA carries RETRACTED values |
| 10c | fin flag misflags | 49 nonfin syms w/ 862 fin=1 rows; 27 live-page f flips | CONFIRMED (subset), SUSPECT (volume) |

---

## 1. Structural — CLEAN
Method: regex `\d{8}` + date-parse every key, length/type-check every row, casefold symbol dedupe.
**0 invalid keys, 0 rows ≠ length 9, 0 non-numeric slots, 0 case/whitespace duplicate symbols, 0
off-cycle (non 3/6/9/12) quarter-ends.** (The only duplicate-symbol defect is semantic, not
case-based — see 10a.) Severity: benign.

## 2. Impossible values
### 2a. Negative revenue — 184 cells (115 fin=0, 69 fin=1)
Worst: ICICIPRULI 20200331 revStd −8339.14 / revCon −8338.31 (insurer rev = NetPremium +
IncomeFromInvestmentsNet; Mar-2020 crash made investment income deeply negative — as-filed under
the builder's insurer definition, not a scale error); ICICIPRULI 20220630 −1611.82; JPINFRATEC
20170331 revStd −640.75; MMTC 20230930 revCon −210.5 (MMTC has printed negative revenue in real
filings — trading reversals); HINDOILEXP 20260331 −209.6/−205.89. Severity: the insurer/trading
cluster is plausibly as-filed (benign by definition choice); the ~115 non-fin cells are SUSPECT
individually — none adjudicated this session.

### 2b. |op| > 1.5×|rev| where rev > 10 Cr — 530 cells (477 fin≠1)
Worst (fin≠1): ABAN 20200331 revCon 354.39 / opCon −7421.60; ABAN std 74.39/−4669.47; ALOKTEXT
20160331 1860.31/−3065.08; RELCAPITAL 20200930 274.08/−2073.92; SKIL 20200331 32.22/−1788.32;
INDIGO 20200630 766.74/−1613.36 (COVID quarter — real). These are dominated by genuine
distress/impairment quarters where the op formula (PBT-before-exceptionals + FinCosts + Dep −
OtherIncome) legitimately goes deeply negative on tiny revenue. Severity: mostly benign; tail
SUSPECT pending filing reads.

### 2c. ebit > op — 169 cells — CONFIRMED defect class
ebit is defined op − depreciation, so ebit > op is impossible by construction. Gap distribution:
42 cells ≤0.1 (rounding — benign), 58 in 0.1–1, **69 cells > 1 Cr (hard violations)**. Worst:
PEL 20220630 std op 150.92 / ebit 329.12; IMAGICAA 20230331 op 14.62 / ebit 133.78 (both bases);
GMRAIRPORT 20240331 std op −0.13 / ebit 94.0; RNAVAL 20190331 op −1.53 / ebit 88.41; HEXT
20240930 con op 296.3 / ebit 370.1; ADANIGREEN 20190930 con op 381.59 / ebit 444.97; DLF
20191231 con op 128.36 / ebit 183.76. Cause hypothesis (unproven): op and ebit slots written by
different filings/routes for the same cell ("LATEST filing wins per cell" + multiple writers).
Heal route: per-cell re-derive from the XBRL cache / `revop_cell_fix.json` ledger → rebuild;
never hand-edit the derived store.

### 2d. rev == 0 stored mid-series — 2,482 cells; 216 with both neighbors non-zero; 22 with both neighbors >10 Cr
0 is the no-base sentinel by convention, so a 0 flanked by real revenue asserts a real ~zero
quarter. The 2,260 flanked-by-zero/near-zero cells are the holdco-NIL pattern (benign). The 22
strong cases are SUSPECT: ALLCARGO 20250930 std (496↔516 neighbors), MTNL 20180630 std
(492↔444), VIYASH 20221231 con (337↔366), DICIND 20180930 std (205↔228), ORCHIDPHAR 20190930
con (135↔129), RUSTOMJEE 20230331 con (126↔271), SHANKARA 20250930 std, VAKRANGEE 20190630 std
(87↔137 — VAKRANGEE's collapse era: could be real). Heal route: filing read per cell →
`revop_cell_fix.json` → rebuild.

## 3. Power-of-ten scale steps — 415 flags — SUSPECT
Method: per basis, positive series; value vs median of ≤4 non-null neighbors; ratio in [8,12] ∪
[80,120] either direction, next value reverting (<3× med). Strong ÷100 (lakh-as-crore) shape,
ratio ≈ 1/112: **BIRLACORPN 20210930 revStd 10.85 vs med 1263.52** (×100 = 1085, plausible);
**JSL 20150930 revStd 16.02 vs 1792.90**; **WHEELS 20221231 revStd 10.03 vs 1115.63 AND revCon
10.76 vs 1198.24**; INDLMETER 20251231 std 0.13 vs 15.27; ESSDEE 20160930 0.14 vs 16.39. Up-steps
(SUPREME 20241231 19.67 vs 0.17, LCCINFOTEC, PALASHSECU) are micro-caps where lumpy real revenue
is plausible — weaker. None confirmed against a filing this session. Heal route: filing read →
`scale_fix.json` (already the scale ledger) → rebuild.

## 4. Cumulative-in-quarter — 1,909 H1-band + 281 9M/FY-band — SUSPECT (screen is seasonality-heavy)
Method: v/med(4 neighbors) ∈ [1.7,2.3] (H1) or [3.4,4.6] (9M/FY) AND next non-null ≤ 1.3×med;
both bases. The H1 band is dominated by genuine seasonality (VOLTAS Q1 AC season ×3 hits, ADANI
group ramp quarters, HAL/BDL/KIOCL Mar-quarter PSU concentration), so 1,909 is an upper bound,
not a defect count. The 9M/FY band's top hit is **AKZOINDIA 20180331 revCon 2719.35 vs med
676.76 (ratio 4.02 ≈ FY-as-quarter)** — and this exact value was already retracted from the
current key JSWDULUX (see 10b), which CONFIRMS the screen catches the real class while also
showing this instance survives only on the orphan key. Others needing reads: PATELENG 20190331
con 2362.20 (3.58×), UNITECH 20180331 con 981.33 (3.91×), INOXWIND 20161231 con 1160.62 (4.29×).
DLF 20260331 std 2307.22 (4.1×) — DLF std is lumpy by nature; SUSPECT only. Heal route: filing
read per candidate → `revop_cell_fix.json` → rebuild.

## 5. Basis duplication std==con — KNOWN-OPEN (§59/§85/§100)
382 symbols carry an exact-equality run ≥8 consecutive quarters (6,778 equal cells inside such
runs). Top: CASTROLIND 39 qtrs (2016Q4→2026Q2 — Castrol genuinely has no material subsidiaries:
likely legitimate), HMVL 34, MOHITIND 34, BALRAMCHIN 33, TALBROAUTO 32, JAYBARMARU 32,
CANFINHOME 32, FINPIPE 30. Equality alone is NOT a defect for no-subsidiary companies — volume
reported per the brief.
**Era-boundary variant:** 73 symbols where equality holds from the start of the both-basis series
(≥6 qtrs) then ends with ≥4 unequal quarters. Boundary years: 2019×2, 2020×1, 2021×12, 2022×14,
2023×15, 2024×21, 2025×8. Top: EXPLEOSOL (equal 28 qtrs → diverges 20250630), KITEX (25 →
20250930), HEG (22 → 20241231), SHALPAINTS, CHENNPETRO (22 → 20230930), NMDC (19 → 20240331).
Note the found direction is "equal OLD era, diverging NEW era" — consistent both with genuine
first-subsidiary events and with old-era con slots filled by std-copy. Not adjudicated; SUSPECT
subset of a KNOWN-OPEN class (con-copy retraction residue, `stopped_filing_con` §100 warning
applies before trusting/writing ANY of these old con cells).

## 6. PAT-mirror vs sf_fundamentals — KNOWN-OPEN §70/§71 — but the divergence has REGROWN
Method: slots 4/5 vs sf_fundamentals idx1/idx3 per (sym,qe); both non-null.
- Compared cells: 139,574. Tolerance disagreements (>1% of larger AND >0.1 Cr): **135** (61
  patStd + 74 patCon; 50 pre-2018, 85 in 2018+).
- **Exact-inequality patCon count: 175 (172 of them 2018+, on 44,437 populated-both cells —
  directly comparable to §70's 43,731-cell population).** §71a closed this at 23 on 2026-08-09.
  It is now ~172 in the same era —
  the divergence is regrowing (post-resync writers re-diverging the mirror, exactly the
  [RE-APPLYING HEAL=REWRITE] failure mode), or fund-side edits since. Additionally,
  `scripts/_fund_suspect_cells.json` on origin/main has an **EMPTY `cells` array** while its
  README still says "17 remain" — the held-back ledger no longer names its cells.
- Samples (mirror vs fund): M&M 20210930 patCon 1928.64 vs −479.08 (fund out of family — §71b
  shape; needs a filing read, fund is the authority but authority≠correct per §71); RELCAPITAL
  20230331 patStd −4.35 vs −1389.39; DLF 20190630 patCon 4.14 vs 414.72 (mirror ×0.1 shape);
  JINDALSTEL 20201231 con 2566.68 vs 2254.66; JPASSOCIAT 20091231 std 314.96 vs 103.02; FACT
  20080331 std −48.36 vs +151.64 (sign flip); HINDUNILVR 20071231 std 473.79 vs 631.44; FDC
  20190331 con 169.79 vs 34.85; GODREJAGRO 20190930 std 1.01 vs 100.57; ADANIENT 20210331 con
  332.53 vs 233.95.
- **Reverse coverage: 4,474 (sym,qe,basis) cells where the mirror holds a value and
  sf_fundamentals is null** — but 4,366 of them are because fund lacks the SYMBOL KEY entirely
  (rename-orphan keys, see 10b: SEQUENT 62, MEGASOFT 62, EXCEL 61, VISASTEEL 59, AKZOINDIA 59,
  TATAMOTORS 58). Only **108** cells have the row in fund with that basis null.
Heal route: mirror-side diffs resync mirror←fund via the §71a procedure (write only sf_revop);
fund-suspect cells (M&M-class) need filing reads BEFORE any resync — copying launders.

## 7. Cross vs quarterly_results.json — 0 disagreements — benign
Format established from `build_quarterly_results.py`: co[SYM].q rows [revS,opS,patS,revC,opC,
patC,ann,rx,sr] aligned to 13 newest-first quarters. Compared same-cell revStd/revCon: 47,144
comparisons, **0** diffs >1% & >0.5 Cr. The bake is in sync with sf_revop (expected — it is
derived from it; this screen guards bake staleness, none found).

## 8. Coverage-era cliffs — KNOWN-OPEN (extraction eras)
Non-null cells per calendar year (revStd / revCon): 2000: 178/0 · 2002: 562/0 · **2003: 1347/0
(×2.4 std cliff)** · 2005: 1892/12 · 2012: 2269/45 · **2013: 2222/259 (con ×5.8 cliff)** ·
2014: 2171/329 · **2015: 2310/873 (con ×2.7)** · 2017: 2713/936 · **2018: 6464/3004 (std ×2.4,
con ×3.2 — XBRL-cache era begins)** · 2020: 7078/5146 · 2025: 9144/6895. Structurally thin:
con is essentially absent pre-2013 (0–45 cells/yr) and thin 2013–2017 (259–936); std pre-2003
(<600/yr). Matches the documented extraction eras (aggregator/archive routes pre-2018, XBRL
2018+). No new cliff found beyond the known era boundaries.

## 9. op-margin outside [−1.0, 0.9], rev > 50 Cr, fin≠1 — 395 cells
Worst: ABAN 20200331 std margin −62.8 (op −4669 on rev 74), ABAN con −20.9, VIDEOIND 20180331
std −15.5, UNITECH 20240630 con −9.2, RCOM 20200331 std −8.0, UNITECH 20250331 con **+7.7**
(op 774.68 on rev 100.81 — positive op 7.7× revenue is the strangest cell here; likely a
write-back/interest-reversal quarter, needs a read), RELCAPITAL 20200930 −7.6, ANSALAPI
20250331 −7.3/−6.2. Only 14 of the stored worst-200 breach on the +0.9 side. Overlaps heavily
with 2b (same distress-quarter population). Severity: mostly benign as-filed; the positive-side
breaches (UNITECH-class) are SUSPECT.

## 10. Anomalies found en route
### 10a. Stray alias key `BAJAJAUTO` beside `BAJAJ-AUTO` — CONFIRMED
`BAJAJAUTO` holds exactly one row (20260331) that duplicates BAJAJ-AUTO's except **patCon:
stray 3661.92 vs canonical 3492.21 — and sf_fundamentals npCon = 3492.21 confirms the canonical
row**. The stray key likely came from a writer using the XBRL NSESymbol spelling without the
hyphen. Zero-symbol structural screens miss it because it's not a case/whitespace variant.
Heal: delete the stray key via the builder's ledger route (it will otherwise shadow/mislead any
consumer resolving symbols loosely).

### 10b. 186 rename-orphan keys in sf_revop absent from sf_fundamentals — CONFIRMED hygiene defect, one carrying RETRACTED values
Examples: AKZOINDIA (→JSWDULUX), ADANIGAS, ADANITRANS, AMARAJABAT, TATAMOTORS (post-demerger
key move), AEGISCHEM (→AEGISLOG), 8KMILES, ADLABS... None appear on the live quarterly page
(qr roster comes from fund keys → 0 overlap measured), and build_results_season skips them
(needs the PAT side), so they are mostly dead weight — BUT: **AKZOINDIA's 32 rows overlap
JSWDULUX 32/32 and DIFFER on 20 of them: the orphan still carries revCon 2719.35 at 20180331,
the FY-as-quarter value that was deliberately retracted on JSWDULUX** (con slots now null there;
the working tree even shows retract_jswdulux_20180331_con.py in flight). Any future rename-map
carry or old→new merge would resurrect retracted values — the "retraction COMES BACK" failure
mode. Heal: purge orphan keys through the rename playbook (§30/§106) + ensure the retraction
ledger covers the ORPHAN key too.

### 10c. Per-row `fin` flag contradictions — CONFIRMED subset with live-page impact
The fin flag varies WITHIN 197 symbols. Taking each symbol's 2018+ (XBRL-era) majority flag as
reference: 49 nonfin-majority symbols carry 862 fin=1 rows (top: MAHSCOOTER 81, PEL 66,
BALRAMCHIN 63, IVRCLINFRA 60, MOTILALOFS 57, RAJESHEXPO 56, ATUL 51, PAGEIND 34); 145
fin-majority symbols carry 738 fin=0 rows (CANFINHOME 19, RELCAPITAL 18, IFCI 17...). Some
flips are LEGITIMATE conversions (PEL is an NBFC post-demerger; MAHSCOOTER is a CIC-NBFC;
INDBANK, DBSTOCKBRO are financials), so the majority heuristic cannot adjudicate — volume is
SUSPECT. But a subset is confirmed wrong by what the companies are: **PAGEIND (textiles),
BALRAMCHIN (sugar), LUXIND, TVSSRICHAK, RADIOCITY, PRSMJOHNSN, ATUL, HGS, TAKE each have a
stray fin=1 row inside the live 13-quarter window, and `build_quarterly_results.py:177` flags a
company financial if ANY window row has fin=1 → all of these are marked bank/NBFC (`f:1`) on
the LIVE quarterly-results page today** (verified in the live qr payload: PAGEIND f=1,
BALRAMCHIN f=1). 27 symbols total have page-f ≠ era-majority. Also `build_results_season.py:279`
drops fin=1 rows from the non-fin medians, so RAJESHEXPO's 56 misflagged rows (2002–2017) are
silently excluded from historical season medians. Heal: adjudicate fin per symbol-era against
the sector classification, fix via the revop ledger + consider majority-vote instead of any-row
in build_quarterly_results.

### 10d. Misc
- 1 cell with |rev| > 3e5 Cr: RELIANCE 20260630 revCon 311,850 — plausible (RIL quarterly con
  revenue is ~₹2.6–3.1 lakh crore); benign.
- fin values are all in {0,1,None} (19 rows fin=None, all with revenue) — benign.
- `_fund_suspect_cells.json` cells[] empty on origin/main while its README claims 17 open — the
  §71b ledger no longer names its population (see screen 6).
