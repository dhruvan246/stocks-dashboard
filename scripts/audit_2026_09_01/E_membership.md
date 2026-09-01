# E — LIVE membership & ticker-identity audit (2026-09-01)

Auditor: subagent E. READ-ONLY audit; evidence in `E_evidence.json` (same dir).

**What was audited (versions pinned):**
- `scripts/indices_history.json`, `scripts/fno_history.json`, `docs/dash_slim.bin`, `docs/backtest-engine.js`, `docs/stock-backtest.html`, `docs/sf_fundamentals.json` — all from `origin/main` @ 809e84869 (fetched 2026-09-01).
- Price universe = what the engine ACTUALLY loads: `https://dhruvan246.github.io/sf-data/` parts `sf_recent_1.bin` + `sf_deep_1/2.bin`, meta `end 2026-08-31, rev c4cfef6947`, 4,572 symbols, bars 1996-01-01→2026-08-31, merged with the engine's own prepend semantics (backtest-engine.js `_loadDeep`).
- **Trap noted + contamination cleared:** `$SP/live/sf_stock_data.bin` (extracted from repo `docs/sf_stock_data.bin`, md5 9f1113d3…) is STALE (last bar 2026-06-12/13, 5,148 legacy symbols, thin daily-from-2018 layout). First-pass screens 4-6 against it produced ~3,400 false "orphans" (e.g. 41 phantom hits on BAJAJ-AUTO alone); those results were **discarded**, and every price-universe screen was re-run on the live sf-data parts. Post-hoc verification against the coordinator-protected `$SP/live_true/sf_stock_data_LIVE.bin` (md5 9084361c…, end 2026-08-31, 4,572 syms): the merged-parts universe used for all final verdicts is **bar-for-bar identical** — same 4,572-symbol set, and full `d` (dates) and `c` (closes) arrays equal for every symbol (0 mismatches). Per-screen sources: screens 1, 2, 3 read only indices_history.json / fno_history.json / dash_slim.bin (no price bin — unaffected either way); screens 4, 5, 6, 7, 8, 9 and the drill-downs read the merged live parts (end 2026-08-31, 4,572 syms ≡ live_true); screen 10 reads stores + engine code only. No verdict in this report rests on the stale bin.
- Verified `dash_slim.bin`'s `indicesHistory` and `fnoHistory` are **exactly equal** to the scripts stores (deep compare) — the engine sees the audited stores verbatim. `startTs 820454400`, `endTs 2026-09-01`, fnoToday 209 syms.
- Store format (by inspection + `scripts/build_membership_v2.py`): `{index: [{effectiveDate, symbols:[...]}, ...]}` ascending; 27 indices, 188 F&O snapshots (2001-11-29 → 2026-08-28).

Membership-relevant engine code (both copies read):
- `docs/backtest-engine.js`: `lastSnap` L464 (no floor), `membersAsOf` L468-475 (pre-first-snapshot → EMPTY set), `membershipStart` L467, join `if (members && !members.has(m.symbol)) continue;` **L812** (member set built L797), freshness gate L801-814, DVR skip L811, `FUND_ALIAS` L557 (627 entries; used ONLY by `fundFor` L558 and SHP L772 — **never for membership**), simulate() start-clamp L899-905.
- `docs/stock-backtest.html` (second engine): `membersAsOf` L546-556, join L1129 (members built L1114), clamp L1198, e3 clamp note L681. Same non-folded join.

---

## Screen 1 — Roster size sanity

Method: per snapshot member-count vs nominal for the 13 numeric-size indices; sectorals reported informationally (their nominal sizes changed over the years; not flagged).

**103 violating snapshots (>2%)**, three distinct classes:

| Class | Where | Verdict |
|---|---|---|
| N500 pre-2002 stored at 405-417 members (−16..−19%) | every N500 snapshot 1998-08-01 → ~2001 | KNOWN-OPEN (§93 era ceiling — old scrapbook is simply short) |
| Inflated transition snapshots: Next 50 @2016-02-22 = **67**, @2016-04/09/11 = 53, @2017-05-26 = 52; Midcap 50 @2015-09-28 = 64, @2015-10-19 = 63; Smallcap 50 2016-2020 = 59-61 | 2015-2020 mid-band reconstruction | **CONFIRMED store defect** (see Screen 4b — same root cause: phantom-future members + union-of-adjacent-rosters) |
| Counts 501-505 on N500/others 2010-2026 | ongoing | benign — DVR twins (engine skips, L811) + `DUMMY*` placeholders (see Screen 11) |

**2015-19 N500 sag re-check: FIXED on origin, measured.** N500 stored counts 2015-2019 are 499-503 on every snapshot (evidence: `screens_1_2_3.screen1`). Not re-reported.

Heal route: the inflated 2015-2017 snapshots heal together with Screen 4b via `build_membership_v2.py` changelog completion, not by editing the JSON.

## Screen 2 — Snapshot cadence + engine-visible staleness

**40 inter-snapshot gaps >13 months.** Worst: Nifty Midcap 100 **2006-11-08 → 2015-03-25 (100.6 months)**; Nifty Auto 50.8mo; Oil & Gas 48mo (2022→2026); Energy 46.3mo; Metal 46mo; FMCG 45.1mo (2020→2024); Nifty 50 itself 33.8mo (2017-05-26 → 2020-03-19). Any backtest crossing these eras screens a frozen roster. Severity: KNOWN-OPEN (quantmac §102 membership item) — now quantified per index in `screen2_cadence`.

**Engine-visible staleness, N500, monthly rebalances 2010-01→2026-08 (200 rebalances):** median **21.5 days**, mean 28.6, p90 66, max **156 days** (2024-08-31 rebalance sat on the 2024-03-28 snapshot). The store carries event-level snapshots (not just annual), so N500 staleness is far better than the sectoral indices' — the bias concentrates in the >13mo-gap eras above.

## Screen 3 — Duplicates / malformed / HTML escapes

Method: exact-dup, whitespace/case, `&xxx;` scan over every snapshot of both stores + fnoToday.

**ZERO** duplicates, ZERO whitespace/case anomalies, ZERO HTML-escape symbols in the LIVE stores. §114/§115 phantom-escape class: verified closed. Severity: clean.

One adjacent blemish found via alias-folding: **6 snapshots carry the same company under two names at once** — LTIM+LTM (Next 50, 2016-02-22), WABCOINDIA+ZFCVINDIA (LargeMidcap 250, all 5 snapshots 2018-02-05→2019-03-29). Harmless to the engine today (the then-unlisted name resolves to nothing) but it double-counts once the join is alias-folded — fold must dedupe via Set (it does). Benign, note for the Screen 6 heal.

## Screen 4 — Orphan members (roster entry, no price series ±30d under any alias-closure name)

Method: alias closure = FUND_ALIAS (627) ∪ shared-ISIN groups (ISIN added zero edges — see Screen 9); every (index, snapshot≤today, member) triple = 270,258 checked; future-dated 2026-09-30 snapshots excluded (19 of them — see Screen 11).

**3,073 orphan triples** total. By era: pre2002 **1,529**, 2002-09 **860**, 2010-15 162, 2016-20 **395**, 2021-26 127. By kind: series-starts->1y-later 2,422, bar-gap 382, no-series-ever 269.

**4a. Pre-2003 N500 era orphans — KNOWN-OPEN (§93), fresh ceiling numbers.** N500 alone: 2,404 orphans, and the per-year drop rate of roster slots the engine cannot resolve: 1998 **20.0%**, 1999 18.7%, 2000 17.2%, 2001 16.0%, 2002 10.9%, 2003+ ≤0.5%. Rosters this era are keyed by CURRENT tickers (e.g. `SUNCLAY`, `RBA`, `SOBHA`, `CEATLTD`, `ABBOTINDIA` sitting in 1998-2002 N500 snapshots while their NSE series start 2006-2023) — where the mapped-to current ticker was later recycled or the company's NSE series starts late, the slot orphans. No wrong-company TRADE can result (no bars then), pure universe shrinkage. Heal route: §93 price-coverage work, plus a name-map re-audit of the old scrapbook (SOBHA/RBA in a 1998 list are anachronisms of the mapping, not of NSE).

**4b. Phantom-future members 2015-2020 — CONFIRMED store defect (new find).** **530 triples** in the reconstruction band: index snapshots dated 2015-2017 (some to 2020) contain companies that **IPO'd 1-8 years later**: Next 50 @2016 holds MANKIND (listed 2023!), HDFCAMC, SBILIFE, HDFCLIFE, ICICIGI, GICRE, NIACL, BANDHANBNK, DMART; Nifty 200 holds 126 such (ABCAPITAL, AUBANK, BSE...), Smallcap 100: 113, Midcap 150: 71, Nifty 100: 50, Midcap 100: 39, Next 50: 35, Smallcap 50: 33, Realty 19, MNC 15, plus PAYTM in Nifty **Bank** 2017/2018/2020 snapshots. Root cause is in the `build_membership_v2.py` backward walk (missing add-events leave today-anchored names present before listing). Impact is double: phantoms are silently dropped by the engine (no bars) AND real then-members are missing — measured Next 50 **resolved counts 2016-2017 = 44-45 vs nominal 50**; Nifty 100 ≈95 effective; Nifty 200 ≈181 effective in 2016. Every 2015-2017 backtest on these universes ran ~10% short. 2020-09-25 onward: clean (50/50 etc).
Heal route: complete the 2015-2018 changelog events in `build_membership_v2.py` inputs (`_changelog.json`) and rebuild the store; never hand-edit the JSON.

**4c. Residual bits:** 141 `DUMMY*` placeholder entries across 52 snapshots (benign, Screen 11); 19 roster symbols unresolvable ever even via closure: ADCINDIA, AIRDECCAN, ASIIL, ATVPR, INDUS, ITHL, IVRPRIME, KINDIA, SAMRUDDHI (in older store), TCIIND + the 10 DUMMY* names.

## Screen 5 — Reverse orphans (context)

3,168 of 4,572 price series never appear in any roster (3,151 after alias fold). Benign — universe breadth for non-index strategies (turnover-floor / All-stocks configs).

## Screen 6 — Membership joins are NOT alias-folded — CONFIRMED code defect, biggest live impact on F&O backtests

**What the code does (cites):** `membersAsOf` returns `new Set(snap.symbols)` verbatim (engine L474); the screen loop keeps a stock only if `members.has(m.symbol)` where `m.symbol` is the SERIES key (engine L812; stock-backtest.html L1129). `FUND_ALIAS` folds fundamentals (L558) and SHP (L772) **only**. So a roster entry whose name differs from the series key of that date silently vanishes from the universe.

**Why it bites:** the price store folds renamed companies' history under their CURRENT ticker (measured: TMPV carries pre-2025 Tata Motors bars, BAJAJHLDNG carries 1996 bars, INFY pre-2011, no TATAMOTORS/INFOSYSTCH/HEROHONDA series exists at all). The engine's own comment (L552-556, "F&O membership + delisted price series use the name that traded THEN") is **stale** — the then-name series were stitched away. The index store was re-keyed to current names (only 2 N500 alias-only triples remain), but **`fno_history.json` still carries as-traded-then names.**

**Measured impact: 3,342 alias-only triples, 3,282 of them F&O (98%)** = **10.3% of all 32,073 F&O (snapshot, member) slots 2001-2026 are silently dropped**. Per-year drop share of the F&O universe: 2003 25%, 2005 21%, 2006-07 19-20% (2007-05-31: **40 of 186 members dropped**, incl HINDLEVER→HINDUNILVR, COLGATE→COLPAL, GUJAMBCEM→AMBUJACEM, HEROHONDA, BAJAJAUTO→BAJAJHLDNG, CROMPGREAV), 2010 13.5% (incl INFOSYSTCH→INFY), 2012 9.7%, 2017 8.9%, 2020 8.6% (incl CADILAHC, PVR, TATAMOTORS), 2023 3.8%, 2025 0.9%. Every F&O backtest ever run on this engine excluded these names for their whole old-name eras — Infosys pre-2011, Tata Motors pre-2025, United Spirits pre-2015, Hero Honda pre-2011.

**Heal route (sync BOTH engines, §39 gate):** fold at Set-build time — `new Set(snap.symbols.map(s => FUND_ALIAS[s] || s))` in `membersAsOf` in backtest-engine.js AND stock-backtest.html (FNO fold recovers 3,282/3,312 = 99% of dropped slots; residue = 30 true orphans like AIRDECCAN). Fold collisions measured: 6, all same-company dual entries, deduped by the Set. Alternative (builder-side re-key of fno_history.json to current names) also works but leaves the next rename broken until re-keyed; the engine fold self-heals as FUND_ALIAS grows. Note: strategy identity (bt-identity) is untouched — cfg unchanged — but results of every F&O saved strategy will legitimately move; expect the saved-strategy sweep to flag them.

## Screen 7 — Recycled-ticker chimera screen — SUSPECT list for adjudication

Method: internal bar-gap >730d; suspect if post/pre close differs >70% and sf_fundamentals covers only one side (or is absent). meta carries only the current ISIN, so per-side ISIN could not be read — everything here is SUSPECT, nothing CONFIRMED.

181 symbols have a >2y internal gap; **95 meet the suspect filter**; narrowed to gap 2-10y (the plausible recycling window): **65 suspects** (`chimera_narrowed_2_10y`). Top priority — **6 whose roster appearances span BOTH sides of the gap** (a backtest joins across the seam): ELECON (2002→2006, +8,954%), HINDZINC (2003→2006, +5,866%), ESSARSHIP (2001→2008), RAIN (2004→2008), TIMKEN (2002→2007), SUDARSCHEM (2000→2006). Adjudication note: several look like same-company suspensions/relists riding the 2003-07 bull run and/or §87 unadjusted pre-2016 corp actions rather than true §89 recycling — but that is exactly what the DVL/DTIL class looked like, so: adjudicate by per-side ISIN from old bhavcopies (§89 method). §89 DVL/DTIL itself: healed, not re-found.

## Screen 8 — F&O history sanity

- Dates strictly monotonic & unique: **yes** (188 snaps, 2001-11-29 → 2026-08-28).
- Sizes: 31 at inception (2001, plausible), 100-250 throughout post-2011: **zero violations**.
- Resolution: 18 snapshots have members resolving to no series even via closure — all small (1-3 syms; AIRDECCAN 2007 the recurring one); 30 triples total. The big resolution problem is Screen 6's alias class, not orphans.
- fnoToday (209) vs last snapshot 2026-08-28 (210): **+DALBHARAT, EXIDEIND, NUVAMA, SAMMAANCAP / −ATHERENERG, GVT&D, MAHABANK, RADICO, SAGILITY.** Consistent with a Sep-2026 F&O revision landing between snapshot runs. WATCH: confirm `extend_fno_history.py` appends a snapshot with the correct effective date; if the next snapshot never records the change, that's a liveness defect. (Not verifiable from the stores today — flagged, not asserted.)

## Screen 9 — ISIN duplicates

Every ISIN in sf meta maps to exactly ONE symbol (0 pairs, hence 0 overlapping-trading-date double-count paths). Clean. Limitation stated plainly: the meta holds only each symbol's CURRENT ISIN, so historical双-listing/recycling cannot be detected this way (that residue is Screen 7's job).

## Screen 10 — Pre-first-snapshot behavior (§92 fabrication)

**Fabrication is CLOSED in both LIVE engines — verified in code, not assumed:** `lastSnap` has no floor to `list[0]` (engine L457-464 incl. the 2026-08-12 measurement note), `membersAsOf` returns an EMPTY set before the first snapshot (L472-474), and `simulate()` clamps the start to `membershipStart` with a console warning (L899-905); stock-backtest.html mirrors all three (L546-556, L681, L1198). §92's "lastSnap FABRICATES" warning describes the pre-2026-08-12 engine; runbook text is stale on this point.
First real snapshots: N500 **1998-08-01**, F&O 2001-11-29, Midcap 100 2006-11-08, **everything else 2015-09-28 or later** (Nifty 50: 2015-09-28!). Consequences for common configs: N500 monthly since 2002 → **0** fabricated and 0 clamped rebalances; a Nifty 50 (or Bank/IT/…) config from 2002 → the first ~164 months are clamped away, loudly, by design.

## Screen 11 — Other anomalies

- **Future-dated snapshots:** 19 indices carry a 2026-09-30 snapshot (the announced September review) — correct behavior with `lastSnap`'s `<=` guard; engine ignores them until effective. Benign.
- **`DUMMY*` placeholders** (NSE official-list artifacts): 141 entries over 52 snapshots, incl. every N500 snapshot 2024-03→2026-06 carrying 1-4 (DUMMYABFRL persisted >1 year). Engine drops them (no series). Benign but worth a builder-side strip (same pattern as the DVR skip).
- **Repo `docs/sf_stock_data.bin` is stale/thin** (ends 2026-06-12; daily only from 2018). Engine never reads it (loads sf-data Pages), but any tool auditing the repo copy gets ~3,400 phantom orphans — this audit did on first pass. Suggest DATA_RUNBOOK cross-ref note.

---

## Severity table

| # | Finding | Count | Severity |
|---|---|---|---|
| 6 | Membership→series join not alias-folded; F&O store then-names vs stitched current-name series | 3,282 F&O slot-drops (10.3% of all; peak 20-25% 2003-08) | **CONFIRMED — top impact** |
| 4b | Phantom-future IPO members + missing real members, 2015-2020 reconstruction band (Next50/100/200/MC150/SC100/Bank...) | 530 phantom triples; Next 50 2016-17 effective 44-45/50 | **CONFIRMED** |
| 1b | Inflated transition snapshots (Next 50 = 67 etc.) | 103 size violations incl. this class | **CONFIRMED** (same heal as 4b) |
| 4a | Pre-2003 N500 era orphans (10.9-20%/yr universe shrink) | 2,389 | KNOWN-OPEN (§93) — fresh ceiling |
| 2 | Sectoral/midcap snapshot gaps >13mo (worst 100.6mo) | 40 gaps | KNOWN-OPEN (§102 quantmac) — quantified |
| 7 | Recycled-ticker chimera suspects (gap 2-10y, >70% jump) | 65 (6 both-sides-rostered) | SUSPECT — adjudicate via per-side ISIN |
| 8 | fnoToday ahead of last F&O snapshot (Sep-2026 revision) | +4/−5 syms | WATCH |
| 3 | Dups/whitespace/HTML-escape members in LIVE stores | 0 | clean (§114/115 closed, verified) |
| 9 | ISIN double-count pairs | 0 | clean |
| 10 | §92 lastSnap fabrication | 0 (clamped, both engines) | CLOSED — runbook §92 text stale |
| 11 | DUMMY* placeholders; future-dated snaps; stale repo bin | 141 / 19 / 1 | benign / benign / trap-noted |
