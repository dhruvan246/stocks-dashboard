# A. LIVE survivorship-free price store — defect audit (2026-09-01)

## Data source (measured, not assumed)
Every screen ran on a **fresh `gh release download`** of the release asset (repo dhruvan246/stocks-dashboard, release "data", asset `sf_stock_data.bin`, 193,737,638 bytes, md5 `9084361c9a64f5b8ca6f4088b55fd32f`) at `$SP/work/sf_live.bin` → `sf_live.pkl`.
Measured identity: **start 1996-01-01, dailyFrom 2002-01-02, end 2026-08-31, 4,572 symbols (2,038 dead)** — matches live `sf-data/sf_meta.json` (end 2026-08-31, rev c4cfef6947, nTot 4572) fetched this session, and is byte-identical (md5) to the protected copy `$SP/live_true/sf_stock_data_LIVE.bin`.
⚠️ `$SP/live/sf_stock_data.bin` was **clobbered at 15:40** with the frozen repo bin (md5 9f1113d3…, end 2026-06-13, 5,148 symbols). It was read exactly once (initial structure probe — which is what detected the clobber); **no screen consumed it**, so nothing needed re-running.
Cross-source store: `$SP/live/stock_data.bin` verified == origin/main `docs/stock_data.bin` == live `https://dhruvan246.github.io/stocks-dashboard/stock_data.bin` (md5 cac74578…).
Ledgers/context read from `origin/main` via `git show` (corp_actions.json, corp_actions_hist.json, demerger_adj.json, rights_terp.json, phantom_crashes.json, ca_open_arbitrated.json, docs/actions.json, weekend_sessions.json.gz, update_sf_data.py, build_sf_data.py, DATA_RUNBOOK §1/80/87/88/94/103).

---

## Screen 1 — Bar integrity
Method: single pass over all 4,572 series (`$SP/work/s1_s4.py`).
| class | count | verdict |
|---|---|---|
| c<=0 | **43,046** bars / 115 symbols (all exactly c==0) | **KNOWN-OPEN** — §87f documents this zero-close penny-floor class at 43,780/115; fresh count 43,046. Top: VISESHINFO 4,041, ANTGRAPHIC 3,501, BLUECHIP 3,481. |
| h<l | 0 | clean |
| c outside [l·0.995, h·1.005] | 0 | clean |
| op outside [l·0.995, h·1.005] | 4,114 | **benign** — 4,105 in 1996 + 9 in 1997 (weekly-bar era); all are 1-paise rounding on heavily-adjusted sub-₹1 values (worst: op 0.03 vs h 0.02). Zero occurrences 1998+. |
| vw outside [l·0.98, h·1.02] | 0 | clean |
| v<0 | 0 | clean |
| v==0 while close moved >2% | 0 | clean |

## Screen 2 — Extreme single-day moves (post-2002, gap ≤5 calendar days)
361 moves with r>1.8 or r<0.55. 55 have an action record within ±3d (union of factors/noadjust/demerger/rights/phantom + docs/actions.json); **306 do not**. Of the no-action set, 187 are post-2016, but only **15 are non-penny (both closes >₹0.25)** — the rest are 2-decimal quantization noise on sub-₹0.25 tapes.
Worst-25 lists in `A_evidence.json → s2`. Adjudicated highlights:
- **ETF unit-split class → see Screen 10 (CONFIRMED new post-2016 miss).**
- Genuine crashes correctly kept: SATYAMCOMP 2009-01-07 (−77.5%), CCAVENUE (Infibeam) 2018-09-28 (−70.8%), YESBANK 2020-03-06 (−56.1%), 63MOONS 2013-08-01 (phantom_crashes keep — correct).
- AURUM (Majesco) 2020-12-23 697→8.63: the ₹974/share special dividend; dividends never adjusted by policy — benign-by-policy but a −98.8% bar any backtest holding it eats.
- SUSPECT, needs record adjudication: **TIL 2024-03-26** 557.60→157.15 (volume collapses to 1 share — IBC-resolution re-basing shape), **SADHNANIQ 2026-02-18** 5.61→1.66 (−70% with real volume; not split-shaped: turnover collapses too).
- Pre-2016 no-action extremes are **KNOWN-OPEN §87 residue**, but five are *turnover-conserving* (CA-shaped, not crash-shaped) and unledgered: SUNDRMFAST 2004-01-23 (526→57, ~1:10 FV split), SHANTIGEAR 2004-07-27 (362→21.75), **EIHOTEL 2006-09-12** (703.95→104.76 — this is a case §87a itself names in its flagship list, and it is still unhealed on the live tape; corp_actions_hist has no EIHOTEL factor), GEOMETRIC 2005-08-02, OCL 2005-08-02 (+ RADAAN, PVP 2004). Fresh count of the §87 class: 119 pre-2016 no-action extremes.

## Screen 3 — Frozen tapes / stale-alive
- Runs of ≥15 identical closes with v>0 on some bars (post-2002): **286 runs / 67 symbols**; 192 runs sit at close ≤₹0.25 = the same zero-close/penny-quantization class as Screen 1 (FCSSOFT 30 runs, SRGINFOTEC 37, BIRLACOT, MVL…). LIQUID/LIQUIDIETF pegged at 999.99/1000 (253- and 206-bar runs) are liquid-ETF price pegs — economically real. **Benign/KNOWN-OPEN**, no new class.
- `alive==true` with last bar >30 trading days before end: **0**. The §94 stale-alive veto is verified working on the live asset (fresh count for §94: class extinct).

## Screen 4 — Date integrity  ➜ plus the ghost-day discovery (Screen 10)
- Non-increasing dates: 0. Duplicate dates: 0. Invalid calendar dates: 0. Array-length mismatches: 0.
- Weekend bars post-2002: 42 distinct dates. 29 are market-wide (482–2,521 symbols) and all match `scripts/weekend_sessions.json.gz` (budget Saturdays, muhurat Sundays, 2024 DR drills) — **benign**.
- **CONFIRMED defect:** 12 Sunday dates in Oct–Dec 2019 carry bars for exactly 2 symbols — **DVL and DTIL** — each an exact (c,v) copy of the prior bar. Fabricated calendar padding riding the §89 series surgery source. (~26 bars, plus matching weekday-holiday padding inside the 2019 ghost days below.)

## Screen 5 — Mid-series gaps (post-2002)
883 gaps of >30 trading days between consecutive bars (both sides present). p50 = 65 td, p90 = 741 td, max 6,105 td. 723 gaps <1y, 160 ≥1y. 15 largest in evidence (PHOENXINTL/SIEL 2002→2026, WIMPLAST, ELPROINTL, NIRLON, TRANSPEK 2002→2026 — long delist/relist arcs).
- 99 gaps resume with price >50% away from the pause price; **96 of them have no action record inside the gap**. This is the documented suspension/relist re-basing + pre-2016 demerger-gap class (**KNOWN-OPEN**, §87f "~120 suspension-gap boundaries", demerger-gap memory). Post-2016 example worth a record check: WAAREEINDO 2019-07-10 (₹1.05) → 2025-06-19 (₹173.32). **SUSPECT, not auto-declared.**

## Screen 6 — Delivery %
- dv outside [0,100]: **0**. Frozen non-zero dv (≥10 identical, ≠100): 1 trivial run (SAMBHAAV 2002, 11 bars at 0.01). dv==100 runs are the BE/BZ trade-for-trade convention (dv:=100 by design) — excluded from the frozen test.
- Per-year share of bars with dv>0: **87–96% every year 2002→2026** (2002: 96.2%, 2014: 87.8%, 2024: 86.8%, 2026: 95.8%). The LIVE asset carries the §88b MTO backfill (unlike the frozen local bin, which lacked pre-2019 dv). **CLEAN.**

## Screen 7 — Turnover/volume consistency
- Implied unit r = t·1e5/(v·vw): **median 1.00–1.14 in every year** post-2002 → `t` is uniformly ₹-lacs; no unit seam survives (§88a normalization verified on the live asset).
- Bars deviating >20% from v·vw/1e5: 26.7% of 9.02M — but this is **by design**, not defect: t and v are RAW while c/vw are adjusted, so r equals the cumulative CA factor on pre-CA bars (p90 r ≈ 10–12 in 2002 falling to 1.0 by 2025). Restricted to bars after each symbol's last ledgered factor: 7.96%, dominated by symbols whose adjustments are non-factor (rights/demergers, e.g. ABFRL r≈2.85 flat) and by the second finding:
- **vw is synthesized (=c) on ~100% of bars before 2019** (bhavcopy had no VWAP column pre sec_bhavdata_full); 2020+ real VWAP (vw==c only 3–7%). Any VWAP-execution assumption pre-2019 is actually close-execution. **Benign, document.**
- The turnover-step heal reader (r-ratio across a boundary) remains valid — it is unit- and level-free. Verified by Screen 9's 1,431 clean reads.

## Screen 8 — Cross-source parity vs docs/stock_data.bin
- Staleness: dashboard store's last actual price bar = **2026-08-31**, equal to sf's end (generatedAt 2026-08-31 10:01 UTC). The §103 freeze is **not currently recurring** (its root pipeline gap remains open per runbook — nothing new measured here).
- Parity (last 250 shared trading days, closes, >0.5% on ≥5 days): **44 of 2,336** overlapping .NS symbols disagree. Decomposed by adjudication:
  - **Policy, benign:** sf scales demergers/rights via ledgers, Yahoo does not — VEDL (ratio 0.3742 pre-2026-04-30 = demerger_adj entry [VEDL, 20260430, 0.3742]), TRIVENI (0.615 = ledger), CHENNPETRO (0.9582 = ledger), GUJENERGY (0.8529 partly), MRPL, ITC-2025 etc. The two stores intentionally differ on every demerged/rights stock — flagged for awareness: **sector/stock pages and backtests will legitimately disagree on these names**.
  - **Dashboard-store defect (sf verified right):** KAPSTON — Yahoo history carries the un-adjusted 1:2 bonus cliff (411→267 on 2026-03-02) while sf is continuous; sf's tape self-consistent, ledger date 20260306 vs market ex 2026-03-02.
  - **SUSPECT (dashboard side / identity):** BURNPUR (constant ~40% gap), CLCIND (100× on 2020 overlap bars), GENESYS, SUMEETINDS, MAHAPEXLTD, SICALLOG, BHANDARI.
  - Also measured: the Yahoo store fabricates dense holiday bars (1,324–2,095 symbols on 2026-01-15/05-01/05-28/06-26 — all NSE-404 non-trading days) and lacks the special weekend sessions — dashboard-store defects, sf correct on all six dates.

## Screen 9 — Adjustment sanity (turnover-step reader vs every ledgered factor)
All 1,466 ledger factor events tested against the live tape (r = t/(c·v) median-step across each ex-date, window trimmed to neighbouring ex-dates, penny floor ₹0.25): **1,431 verified applied, 0 post-2016 mis-applied or unapplied** (762 post-2016 events measured — consistent with the §117 quantmac clean bill). JINDALSTEL 2008-01-21 verified applied (the §87g ca_open_arbitrated heal landed).
- **CONFIRMED, 1 new: RASOYPR ex 2013-03-21, factor 0.066667 (1:15)** — present in BOTH corp_actions.json and corp_actions_hist.json, yet the live tape still steps 115.70→8.45 (13.7× cliff, 20 bars later still ~9) and the turnover step is 1.0036 = **nothing was ever divided out** (volume 397k→1.77M corroborates a real split). This is the §87a-bis "ledgered factor is not an applied factor" class. RASOYPR was liquid in 2013 (₹1–5 cr/day, dv 50–95%); every pre-2013-03-21 bar (series starts 2011-12-12) is ~15× overstated and backtests crossing that date eat a phantom −92.7%.
- Ambiguous/thin (8, all pre-2016, no verdict asserted): KARURVYSYA 2016 dup-rows, GENUSPOWER/JINDALPOLY 2010 dup-rows, KMSUGAR 2010 (known §87g residue), MINDACORP 2012, SOUISPAT 2014 (unapplied **by design** — §87g penny floor).

## Screen 10 — Anomalies chased
### 10a. CONFIRMED NEW post-2016 class: ETF unit splits never adjusted (19 events, 2018→2026)
Volume-confirmed signature on every one: close /F, volume ×~1/F, turnover continuous across the ex-day (`s10_unadjusted_splits.json`).
- **8 gold-ETF 1:100 unit splits with NO ledger entry of any kind:** GOLDBEES 2019-12-19 (3359.60→33.55), GOLDAXIS 2020-07-23, HDFCGOLD 2021-02-17, GOLDBETA 2021-03-25, BSLGOLDETF 2021-11-25, SETFGOLD 2022-01-06, LICMFGOLD 2026-03-06, IVZINGOLD 2026-04-30. Every pre-split bar is 100× off basis; each series carries a phantom −99% crash.
- **9 ETF ~1:10 splits in 2026 that carry an explicitly WRONG verdict:** CONS, BANKNIFTY1, MIDCAP, NV20, SILVER1 (all ex 2026-02-27), MIDQ50ADD, HEALTHADD (2026-07-03), PSUBANK (2026-07-10), IVZINNIFTY (2026-07-31) — each sits in `noadjust[]` AND `phantom_crashes.json` as a "verified genuine crash" keep-drop. Mechanism: `audit_phantom_ca`'s 2016+ rule ("absent from both dense feeds = phantom") is calibrated for equities; ETF unit face-value changes never appear in the equity CA feeds, so every real ETF split auto-classifies as a confirmed crash. **The rule needs an instrument gate.**
- 2 SUSPECT thin-tape same-shape: HNGSNGBEES 2019-12-19 (~1/9), NEXT50IETF 2018-11-16 (~1/9).
- Blast radius: all 19 are `alive=False` and ETFs are outside the liquid/results universes (§94e), so equity backtests are untouched; but the store is 10–100× wrong on these tapes pre-split, and any ETF-inclusive analysis (gold allocation, turnover history) is silently corrupted.

### 10b. CONFIRMED NEW: ghost trading days + one missing real day (date-plane defects)
Every date arbitrated against nsearchives.nseindia.com (bhavcopy exists = trading day; 404 = holiday; controls returned 200):
- **9 ghost days** where the store carries a full market of bars on an NSE holiday, ≥86% of them exact (c,v) copies of the previous session: **2019-10-02, 2019-10-08, 2019-10-21, 2019-10-28, 2019-11-12, 2019-12-25** (~1,630 symbols each, 100%/86% duplicates — six fabricated days inside one quarter), **2020-11-16** (1,674 syms, copy of the 11-14 muhurat Saturday), **2024-01-22** (2,095 syms, copy of the 01-20 special Saturday; RELIANCE (c,v) exact match), **2024-05-20** (2,134 syms, copy of the 05-18 DR-drill Saturday). ≈15,800 fabricated bars total. They inflate day-counts, put v>0 on non-days (double-counting in turnover averages), and shift every trading-day-window computation crossing them.
- **1 mis-dated session:** the 2021 Muhurat (real date 2021-11-04, cm04NOV2021 exists) is in the bin **only** as 2021-11-05 (1,823 symbols; cm05NOV2021 is 404). Date off by one for the whole session.
- **1 missing real day: 2016-08-12** — official bhavcopy exists (1,579 EQ/BE/BZ rows, TIMESTAMP 12-AUG-2016 verified in-file) but the bin has bars for only the 2 padded symbols (DVL/DTIL). A full market day absent from the store.

## Suggested heal routes (never direct bin edits)
1. **RASOYPR**: add to `scripts/ca_open_arbitrated.json`-style incremental ledger (entry `[RASOYPR, 20130321, 0.066667, 0.07303, …]`) or re-run the §87e full-window heal for it; then the §87e-bis pass-2=0 test. The factor already exists in corp_actions — only the application is missing, exactly like JINDALSTEL.
2. **ETF splits**: write the 17 confirmed factors into `corp_actions_hist.json` (keyed by bin symbol), REMOVE the 9 wrong keep-drop rows from `phantom_crashes.json`/noadjust (retraction must visit every ledger — retraction-comes-back rule), gate `audit_phantom_ca` by instrument class, then full-window heal + rebuild. Adjudicate HNGSNGBEES/NEXT50IETF fractions from AMC records first (a number no record states is a guess).
3. **Ghost days**: drop the 9 holiday dates + re-date 20211105→20211104 in the base bin via a one-shot rebuild-adjacent fix (the daily pipeline cannot reach them); backfill 2016-08-12 from the validated cm12AUG2016 file; delete the DVL/DTIL padding bars. All are date-plane operations — route through a `build_sf_data`/rebuild-era ledger the way weekend_sessions.json.gz works (an authoritative session-calendar sidecar would make this class structurally impossible).
4. Pre-2016 named residue (SUNDRMFAST/SHANTIGEAR/EIHOTEL/GEOMETRIC/OCL): queue into `scripts/ca2002_campaign/manual_queue.json` with the turnover-step evidence attached — they are CA-shaped by the tape's own second reader, but the exact fraction needs a record.
