# P3 — STRATIFIED AUDIT, 66 FROZEN SYMBOLS
Sample frozen and committed BEFORE any extraction (`p3_strata.json`, deterministic md5 draw over 13
strata; re-running the drawer reproduces the identical list — verified). Three usable sites,
one agent each. Verdicts gated by the **P2 pilot cards**, deliberately not by cards refitted on this
data. Tool: `revpat_quorum.py --suffix p3`. Raw: `p3_quorum.json`.

---

## 1. THE NUMBERS — 3,104 (symbol, quarter, field) cells adjudicated

| status | cells | |
|---|---|---|
| **CONFIRMED** — ≥2 independent sites agree with each other *and* with us | **852** | rule 6b satisfied |
| SINGLE_SITE_OK — one site agrees, none dissent | 1,983 | no quorum available |
| SINGLE_SITE_DISSENT | 202 | one site differs; not a quorum |
| SITES_DISAGREE — sites contradict each other | 49 | **value not taken**, correctly |
| **CONTRADICTED** — ≥2 sites agree with each other *against* us | **18** | candidates, §3 |
| WE_HOLD_NOTHING | 75 | site has a quarter we don't |

| field | n | CONFIRMED | 1-site | disagree | **CONTRADICTED** |
|---|---|---|---|---|---|
| revS | 795 | 288 (36.2%) | 427 | 19 | **8** |
| revC | 697 | 249 (35.7%) | 395 | 15 | **1** |
| patS | 821 | 312 (38.0%) | 457 | 15 | **9** |
| **patC** | 394 | **0 (0.0%)** | 352 | 0 | 0 |
| **patE** (backtest-effective) | 397 | **3 (0.8%)** | 352 | 0 | 0 |

**0.58% of adjudicated cells are contradicted.** Extraction: 1,565 Screener rows (66/66 symbols,
both bases), 622 Tickertape (63 symbols), 575 Groww (61 symbols) — 66/66 covered by at least one
site, zero unparseable lines.

## 2. ★★★ CONSOLIDATED PAT: 0 CONFIRMED OF 394 — the P2 prediction, now measured at scale

P2 predicted from 14 stocks that consolidated PAT can never reach a site quorum, because Screener
and Groww publish **total** consolidated profit against our **owners-attributable** series and are
therefore barred from voting, leaving Tickertape alone. At 66 symbols: **394 patC cells, 0
confirmed, 352 single-site.** The prediction holds exactly.

**And patE — the number the backtest actually consumes — inherits it.** patE resolves to patC
wherever consolidated exists, which in this window is nearly always: 397 cells, **3 confirmed
(0.8%)**. The engine's `tries=[[3,4],[1,2]]` std fallback is essentially never exercised in the
site-reachable era, so **the figure that drives strategy picks is the one figure sites cannot
corroborate.** Its only real check is the exchange filing.

## 3. THE 18 CONTRADICTED CELLS — and the four that were already adjudicated

| symbol | quarter | field | ours | Screener | Groww | Tickertape | status |
|---|---|---|---|---|---|---|---|
| GICRE | 2025-06-30 | patS | 2172.77 | 1752.00 | 1752.23 | — | **OURS_WRONG (proven)** |
| GICRE | 2025-09-30 | patS | 2698.01 | 2867.00 | 2866.79 | — | **OURS_WRONG (proven)** |
| BAJFINANCE | 2025-12-31 | revS | 18067.89 | 17870.00 | 17869.70 | — | **OURS_CONFIRMED (proven)** |
| BAJFINANCE | 2025-12-31 | revC | 21213.89 | 21013.00 | 21013.49 | — | **OURS_CONFIRMED (proven)** |
| NIVABUPA | 2025-06-30 | patS | −91.44 | 71.00 | 71.44 | 39.00 | **OURS_CONFIRMED** |
| NIVABUPA | 2025-06-30 | revS | 1371.08 | 1932.00 | 1931.75 | — | **OURS_CONFIRMED** |
| NIVABUPA | 2026-03-31 | patS | 345.13 | 159.00 | 159.36 | 86.00 | **OURS_CONFIRMED** |
| NIVABUPA | 2026-03-31 | revS | 2138.56 | 2251.00 | 2251.06 | — | **OURS_CONFIRMED** |
| STARHEALTH | 2025-06-30 | patS | 262.52 | 438.00 | 438.18 | 745.00 | **OURS_CONFIRMED** |
| STARHEALTH | 2025-06-30 | revS | 4232.86 | 4880.00 | 4880.40 | — | **OURS_CONFIRMED** |
| JUBLPHARMA | 2025-03-31 | revS | 217.30 | 61.00 | 60.70 | — | **OURS_CONFIRMED** |
| ABFRL | 2025-06-30 | patS / revS | −59.93 / 1392.85 | −77 / 1412 | −76.51 / 1412.33 | — | open |
| ABFRL | 2026-03-31 | patS / revS | −139.13 / 1379.66 | −150 / 1409 | −149.81 / 1409.31 | — | open |
| HONASA | 2025-03-31 | patS / revS | 22.64 / 479.28 | 25 / 523 | 24.61 / 522.57 | — | open |
| NCC | 2025-09-30 | patS | 100.96 | 102.00 | 101.88 | — | open (0.9%) |

**The pipeline independently rediscovered all four already-adjudicated cells and got both directions
right** — GICRE flagged and later proven wrong, BAJFINANCE flagged and later proven *correct with
both sites wrong*. That is the strongest available evidence that CONTRADICTED is a well-calibrated
candidate signal and **not** a defect count.

### 3a. ★★★ THE SEVEN ARBITRATED CELLS ALL CONFIRMED US — 7 of 7
NIVABUPA ×4, STARHEALTH ×2 and JUBLPHARMA ×1 were taken to their own filings. **Every one matched
our stored value exactly**, each triple-anchored: two comparator columns on the same row reproducing
OTHER quarters we already store, plus an FY-quarter-sum identity closing to the paisa against the
filing's own printed year column. NIVABUPA Mar-2026 is the cleanest — our four stored quarters
(−91.44, −35.27, −87.64, 345.13) sum to **130.78**, exactly the filing's printed FY-Mar2026 figure.

**The GICRE pre-associate fingerprint was tested and does NOT apply**: NIVABUPA has no subsidiaries
and files no consolidated statement at all (zero `consolidat*` hits across the full 10-page PDF), so
the defect that hit GICRE structurally cannot occur here. A fingerprint is a hypothesis to test per
company, not a pattern to assume across a category.

**What the sites are doing instead:** on the PAT cells all three disagree with *each other*
(NIVABUPA Jun-2025: 71.00 / 71.44 / 39.00), so under rule 6b they decide nothing. On the revenue
cells Screener and Groww agree with each other but their figure **could not be reproduced from any
printed row or combination** — Gross Premium, Net Premium Written, Total Income and several sums
were all tried and none land near it. So it is an unidentified aggregator concept, not a reading of
the filing. Insurers are where the sites are least trustworthy, exactly as the IRDAI-format warnings
in §3/§43/§55 imply.

**Three of the eight contradicted symbols are insurers or insurer-like** (NIVABUPA, STARHEALTH, and
GICRE already proven). Given GICRE's proven mechanism — the standalone slot populated from the
consolidated statement's pre-associate row — the IRDAI-format cohort is the obvious place to look
next, and the arbitration in flight tests that exact fingerprint on the other two.
Note the sites do **not** agree with each other on the insurer PAT cells (Tickertape differs from
Screener/Groww on all three), so nothing there is decided by sites.

## 4. WHAT THE EXTRACTION ITSELF TURNED UP

- **★ Screener's window is "the last 13 quarters THAT COMPANY filed", not a fixed date.** P1 recorded
  "13 quarters, oldest Jun-2023" from actively-filing large caps. Across 66 the span runs
  2018-06-30 → 2026-06-30, because a stale filer's last 13 quarters are old: **CASTEXTECH** sits at
  2018-2021 and **ZEAL** reports only semi-annually (Mar/Sep columns only, confirmed off raw HTML).
  Max per (symbol, basis) is still 13, so no annual columns leaked in. The P1 note is corrected.
- **7 symbols have no consolidated quarterly series on Screener** — ENRIN, GODIGIT, MSUMI, NIVABUPA,
  POWERINDIA, SBILIFE, STARHEALTH — all also carrying the bare "Figures in Rs. Crores" unit string
  with no basis qualifier. Groww independently reports std-only for the **same seven**. Two
  independent sites agreeing a basis is absent is positive evidence, not a gap.
- **HUDCO has con == std on every quarter, on both Screener and Groww** — a con-copy candidate of
  the same class as the confirmed GICRE revenue one. Not yet arbitrated.
- **Identity discipline earned its keep again.** Groww's `gujarat-fluorochemicals-ltd` returns
  `nseScriptCode = GFLLIMITED`, not FLUOROCHEM — correctly skipped rather than fuzzy-accepted. Many
  renamed tickers keep their OLD slug (ENRIN → `siemens-energy-india-ltd`, POWERINDIA → the old ABB
  slug, ZFCVINDIA → `wabco-india-ltd`, JUBLPHARMA → `jubilant-life-sciences-ltd`), so the payload's
  ticker/ISIN echo is the only safe check. 3 symbols stayed unresolved on Groww after 3 guesses
  each (AJRINFRA, CASTEXTECH, IGIL) and were recorded as such rather than ground on.
- **★★ THE TRU IDENTITY TRAP RECURRED TWICE MORE, and was caught both times.** On Tickertape the
  sid equals the ticker but belongs to a different company: **`ccl-international-CCL` is not our CCL**
  (CCL Products India is sid `CCLP`), and **`shri-kalyan-holdings-SHK` is not our SHK**
  (S H Kelkar is sid `SHKE`). Same shape as the campaign's founding trap, where sid `TRU` is Trust
  Fintech. Three instances now on one site: **a sid that matches the ticker is a coincidence to be
  disproved, never an identification.** Both were resolved correctly by name and confirmed by ticker
  echo + ISIN before any row was emitted, so neither reached the quorum — but a first-result bulk job
  would have silently scored two wrong companies against our data.
- Tickertape also returned an identity-confirmed but EMPTY series for ZEAL (ticker and ISIN both
  echo correctly, zero income records) — a coverage gap, not a defect, and recorded as such.
- Both ampersand symbols (`GMRP&UI`, `M&MFIN`) resolved on the `%26` form on Screener.

## 5. METHOD NOTES
- **Verdicts were gated by the P2 cards, not by cards refitted on P3.** Fitting the mapping on the
  same data you then judge lets a real defect be absorbed into the mapping. The P3-refit cards were
  computed only as a *validation*, and they hold: Screener Sales→revC 90.2%, Net Profit→patS 94.7%;
  Groww Revenue-from-Operations→revC 97.3%; Tickertape qIncNinc→patC 89.3%. Critically, **Screener's
  and Groww's consolidated "Net Profit" are still REFUSED at 66 symbols** — the total-vs-owners
  finding was not a small-sample artefact.
- **A bug the pilot regression could not catch:** the new patE derivation assumed we hold every cell
  a site reports, and crashed on the first quarter in the wider sample that we don't
  (`KeyError: 20260630`). Fixed to skip such cells. The pilot passed only because its symbols
  happened to have full coverage — *a green regression on a narrow sample is not evidence of
  correctness on a wide one.*
- **★ ALL THREE extraction agents stalled the same way**, each starting a background fetch and then
  announcing it would wait for a notification that was never coming — losing partial work in one
  case. The campaign plan predicted this from the SHP run and phrasing it as a preference ("run
  foreground loops") was not enough. **State it as a prohibition: no background jobs, no monitors,
  no waiting; write results to disk as you go.** Diagnosing each agent's state before resuming
  (96 pages cached, 45 identity-matched symbols, a log ending cleanly at a known point) meant ~130
  requests were not re-sent.
