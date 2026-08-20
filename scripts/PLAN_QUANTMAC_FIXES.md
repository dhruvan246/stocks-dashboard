# PLAN — QUANTMAC FINDINGS: audit verdicts + the fix campaigns (2026-08-20)

**Written by the reviewing session (user: "check that the research across all quantmac issues was
right and the fixing correct — make a plan; it will be followed").** Every claim below is either
MEASURED (this session or the fixing session, with the measurement named) or marked PENDING/UNKNOWN.
**NO ASSUMPTIONS, NO GUESSWORK (§0)** applies to executing this plan too: where it says measure
first, measure first.

Companion records: DATA_RUNBOOK **§102** (first-pass verdicts + the shipped TTM fix),
memory `project-stocks-quantmac-crossvalidation`. Source workbook:
`~/Downloads/StockWorld_data_findings_2026-08-20.xlsx`; our log:
`~/Downloads/trade-log_diiPct_2009-01-01_2026-08-17.csv` (422 trades).

---

## A. AUDIT VERDICT ON THE WORK DONE SO FAR

### A1. The TTM fix (finding 4) — ✅ CORRECT, and now MEASURED at strategy level
Re-derived from scratch, not trusted:
* **Logic**: 4 true consecutive quarter-ends are exactly 9 months apart oldest→newest (Mar/Jun/
  Sep/Dec quantization), so `monthIdx(arr[ci][0]) − monthIdx(arr[ci−3][0]) > 10 ⇒ reject` is the
  right test. Spans can only be 9, 12, 15…, so >10 ≡ >9; no boundary risk. `arr[ci−3]` cannot be
  read when `ci<3` (the value loop already set `ok=false`). `prev4` needs no separate check — it
  is looked up by exact date (`qe−10000`), so contiguous `last4` ⇒ contiguous `prev4`.
* **Twin parity**: comment-stripped, whitespace-normalized function bodies byte-identical across
  `docs/backtest-engine.js` / `docs/stock-backtest.html` (re-verified).
* **Deployment**: ENGINE_VER e5→e6 (snapshot invalidation), sw v99, live-verified through the
  production origin (RANEHOLDIN con @2022-10-31 ttm 576.8→null; RELIANCE unchanged).
* **A/B, the missing measurement, now done** (old `profitMetrics` reconstructed by stripping only
  the contiguity block, verified divergent on the gap case and identical on the dense case; both
  runs on the same loaded bin, same session):
  * DII strategy 2009-01-01→2026-08-14: CAGR **50.15 → 50.94 (+0.79pp)**, maxDD unchanged
    (52.81), 424 trades both sides, **exactly 1 of 212 rebalances differs: 2015-04-30, NDTV out,
    DCBBANK in** — precisely the one row quantmac's C-sheet tags `ttm_disagree` (NDTV 2015-04,
    window 2013-03-31..2014-12-31). The fix removes exactly the externally-flagged trade and
    nothing else.
  * The 24 `ttm_null` rows do NOT move today because their once-missing quarters have since been
    backfilled (their windows are now genuinely contiguous). The fix's dataset-wide footprint is
    the **2,580 cells / 698 symbols** sweep in §102c, not those 24.
* **Two follow-ups for the implementing session (cheap, do them):**
  1. **F-1 unique-qe assert.** The span test assumes one row per qe per symbol. Scan
     `docs/sf_fundamentals.json`: assert no symbol has two rows with equal `qe`. If any exist,
     the span check has a hole (a duplicated qe can hide an interior gap) — fix the data, and
     only then trust the check. One-liner scan; record the count in §102.
  2. **F-2 broader strategy sweep (optional but §97-consistent).** Run the §97 A/B harness over
     the user's saved strategies that use `profitTTM`/`composite` and record deltas. Expected
     small; the point is the record.

### A2. Finding 2 (look-ahead) — refutation ✅ STANDS, re-derived
* DELTACORP Jun-2016: BSE broadcast **2016-08-01 20:00:07** (matches our stored 20160801);
  >15:30 ⇒ visible 2016-08-02; entry 2016-08-31 ⇒ legitimate. Nothing results-shaped anywhere
  near their claimed 2016-09-28 in an all-category window.
* OMAXE Mar-2012: BSE broadcast **2012-05-30 17:50:05**; >15:30 ⇒ visible 2012-05-31 = entry
  date ⇒ legitimate (by one day — via the §12 rule, worth stating precisely when replying).
  Their claimed 2012-06-14 has no BSE row at all in 2012-04-01..06-30.
* **BUT the OMAXE row exposed the bidirectional error (B2 below): our stored ann 2012-05-15 was
  15 days EARLY — a look-ahead direction the first-pass framing ("staleness") missed.**

### A3. Finding 1 (membership) — our side measured; NSE side was OVERCLAIMED, now being verified
* MEASURED: all 24 (symbol, month) pairs read as members in `indices_history.json["Nifty 500"]`
  via nearest-prior-snapshot — the engine's own semantics.
* **OVERCLAIM in §102b**: "…on a date NSE's dated press releases say it wasn't" — the press
  releases were never read; that half was quantmac's citation, not our measurement. A read-only
  verification agent is out on the 5 highest-signal claims (PCBL 2002-01-17→2018-09-28,
  SANDESH, AJANTPHARM, GAEL, INOXLEISUR) against local caches + niftyindices/Wayback.
  **VERDICTS: PENDING at the time this plan was written — see the addendum section at the
  bottom; the implementing session must fill it before touching membership data.**
* Root-cause note for whoever fixes it: PCBL-in-2017 sits in snapshots dated 2017-01-23 /
  2017-05-26 / 2017-09-29. Before editing anything, **trace which input source contributed
  those snapshots** (official CSV? `_mc_n500_snaps.json` soft checkpoint? changelog walk?) —
  the fix belongs in that source layer + `build_membership_v2.py` rebuild, NEVER in the output
  JSON (weekly refresh reverts it; standing rule).

### A4. Finding 3 (staleness) — confirmed, root-caused, campaign designed; scope corrections below
* 3/8 hand-verified against BSE (HINDUNILVR/MARICO/HEROMOTOCO, 0-2 day agreement with quantmac).
  Remaining 5 delegated to a second read-only agent (CANFINHOME/GEOJITFSL/MUNJALSHOW/ALFALAVAL/
  DHANI) — **PENDING, see addendum.**
* Root cause measured: **36,027 cells / 2,623 symbols carry ann = exactly qe+45d** (std 29,764,
  con 6,263), heaviest 2003-2014 but present through 2025. The convention is
  `apply_agg_pat_fills.py`'s documented default and predates it (20,818/21,515 dated pre-2015
  cells per its own docstring).
* User directive: **fix all 36,027** (not just the 8).

### A5. Findings 5 / A / B — stand as recorded in §102b
* F5 (ALEMBICLTD 2011 demerger): absence from all three CA ledgers re-confirmed. Note
  `demerger_adj.json` DOES hold pre-2016 entries (RELIANCE 2006-01-18…94 total), so ALEMBICLTD
  is a **parser/keyword miss in the feed sweep, not an era floor** — the fix extends the sweep,
  §D below.
* Appendix B ("our value" column corrupted): 5/5 spot checks showed the real trade log matches
  THEIR values; sheet bug on their side. Tell them.
* Appendix A: not an error claim — but see B3: it is real, measurable, and in OUR data.

---

## B. WHAT THIS REVIEW FOUND THAT THE FIRST PASS MISSED

### B1. (process) A negative that was asserted, not measured
§102b's membership row states the NSE half as fact. Runbook rule: a negative/positive about an
external record needs the record read. The addendum below settles it; §102b must be edited to
match whatever the evidence says (one-line correction commit).

### B2. ★ The qe+45d placeholder errs in BOTH directions — this is a RE-DATING campaign, not a "staleness" campaign
MEASURED example: OMAXE qe 2012-03-31 — stored ann 2012-05-15 (qe+45d), real broadcast
2012-05-30 17:50 ⇒ our data published the quarter **15 days early = look-ahead**. The quantmac
framing (and §102's) only described the late direction (real filing before placeholder = stale).
Both directions are wrong-dates; both get fixed by writing the truth. Consequences for the
campaign design:
* The apply step must record per cell which direction it moved (`early`/`late`, days).
* Expect the backtest diff to go both ways: removing look-aheads (cells that become visible
  LATER than before) and un-staling quarters (cells visible EARLIER).
* **postDrift moves too**: it consumes the ann date directly (`lastResultDate`), so re-dated
  cells shift postDrift values historically. That is truth-correcting, not a regression — but
  the §39 parity check must EXPECT diffs in postDrift-using strategies, or the verifier will
  read correct changes as breakage.

### B3. ★ The SAME convention exists in the shareholding data — and DII% is this strategy's sort key
MEASURED this session: `docs/shp_engine.json` — **32,960 of 92,733 sub-dated rows (35.5%) are
exactly qe+21d**, concentrated 2008-2015 (~2,000/yr). 21 days is the SEBI filing deadline: the
placeholder errs late for every company that filed before the deadline (most), early for actual
late-filers. Quantmac's Appendix A ("visibility assumption worth documenting") is this, in our
own file. Scope decision for the user: document-only now, or a P5 re-dating campaign shaped
exactly like P2 using BSE's shareholding-submission records.
**⚠️ COORDINATION:** `scripts/fetch_shareholding.py` + `scripts/fetch_shp_bse_aspx.py` are dirty
in the main checkout — another session is actively working SHP. Do NOT start P5 without
checking that session's state; worktree only; own files only.

### B4. Campaign-design requirements the first pass hadn't pinned down
1. **Ledger sync (2,046 cells).** MEASURED: 2,046 of the 36,027 targets have an
   `agg_pat_cell_fills.json` entry whose `ann_written` is the qe+45d value. Re-dating the cell
   without updating the ledger leaves a re-apply/`--repair-ann` path that stamps the placeholder
   back (the §85/§67 "retraction needs every ledger" class). The apply step must rewrite those
   entries' `ann_written` + `ann_basis` (→ `bse-broadcast <NEWS_DT>`) in the same commit.
2. **15:30 rule on every new date (§12).** BSE `NEWS_DT` carries time-of-day: if >15:30 IST the
   effective ann-date = next trading day per `scripts/gate_calendar.json` tdays (a muhurat
   Sunday can be a session — never use a weekday calendar). Applying it at write time makes the
   nightly gate a no-op on these cells (idempotent agreement, like §12's JSL PASS signal).
3. **Both mirrors**: `docs/sf_fundamentals.json` AND `scripts/fundamentals.json` (CI commits
   only the former; local applies keep the mirror in step — §12 precedent).
4. **Persistence pre-check (Phase 0):** §12's ann bumps persist across nightlies (verified
   idempotence there), and `update_fundamentals.py` only ingests new filings — but CONFIRM by
   grepping the nightly path for any writer that re-derives ann on OLD quarters before running
   the campaign, and re-verify LIVE ~20min + next-day after the first batch lands (CLAUDE.md
   rule 5).
5. **No-record cells stay put, recorded.** `not-found-via:bse-ann` per cell; never guess. The
   smoke test says the unmatched tail is almost entirely ≤2007 (BSE archive floor):
   3-symbol pilot matched 74/93 of 2008-2014 targets but 0/58 of 2001-2007. Expect ~8-9k
   pre-2008 cells to remain placeholder — that is the honest outcome, and the NSE-archive route
   (§54/§101) is a separate later rung if wanted.
6. **The matcher is UNPROVEN on the modern era.** Smoke unmatched includes HEROMOTOCO 2020-06
   and MARICO 2025-09 — eras where BSE titles/fields differ ("Integrated Filing…"). Phase 1
   must pilot ~30 symbols spanning 2016-2025 and eyeball rows before the full run; extend the
   NEWSSUB/HEADLINE parse as needed. Do not burn the full fetch on a parser that silently
   misses a whole era (§61: an empty result is a diagnosis).
7. **Annual-vs-Q4 rows:** "year ended March 31, XXXX" prints match qe=Mar-31 targets via the
   earliest-'U'-row rule — correct (Q4 figures first become public with whichever print came
   first). Keep the rule; it is already conservative.
8. **BSE quota:** watch for the 162-byte 302 stub (§0). Serial + 0.3s sleep first; consider
   2-3 shards only if an hour of serial shows zero 302s. Newest-first symbol order is NOT
   needed (per-symbol windows span eras anyway), but keep the resumable checkpoint (it exists).

---

## C. THE CAMPAIGNS, SEQUENCED

**P0 — asserts & corrections (minutes, do first)**
  a. F-1 unique-qe assert (A1). b. §102b membership-row wording fix once the addendum lands.
  c. Persistence pre-check (B4.4). d. Commit this plan + addendum.

**P1 — matcher hardening pilot (~1h)**
  30-symbol pilot spanning 2003-2025 out of `_staleness_fix/target_list.json`; eyeball every
  match and every miss by era; fix the parse; record match-rate by era in this file. Gate:
  ≥95% of 2009-2019 pilot targets matched, and every miss explained (no-record vs parse-miss).

**P2 — the 36,027-cell re-dating campaign (background, resumable, worktree `staleness-fix`)**
  Fetch: `scripts/_staleness_fix/fetch_and_match.py` (exists; smoke-tested on 3 symbols).
  Serial ETA at pilot rates ≈ 11-18h — run as a background job with progress checkpoints.
  Apply (to be written, `apply_redating.py`): for each matched cell —
    new_ann = 15:30-gated(NEWS_DT); skip if new_ann == stored; write BOTH mirrors; update the
    2,046 ledger entries when touched; audit ledger `scripts/_staleness_fix/redate_ledger.json`
    per cell: {old, new, direction, days, NEWS_DT, NEWSSUB, src:'bse-ann'}.
  Verify (§39/§41): parse-check both JSONs; count moved cells by era/direction; push via
  worktree recipe; dispatch the fundamentals workflow if needed; LIVE re-verify ~20min later
  and next-day (racing CI rule). THEN re-run the DII strategy end-to-end and diff the trade
  log before/after — the 8 flagged trades (minus any the addendum refutes) must resolve, and
  the full diff (both directions, incl. postDrift-strategy movement) gets recorded in §102.

**P3 — membership (blocked on the addendum verdicts)**
  If confirmed: trace the contributing snapshot source for each bad (symbol, window) —
  suspicion for PCBL-class is a soft-checkpoint layer, e.g. `_mc_n500_snaps.json` — fix at the
  source + `build_membership_v2.py` rebuild + `verify_sizes.py` + count-check (~500-504/date),
  push, LIVE bin re-verify, re-run the strategy (membership diffs change the 24 trades).
  If refuted: quantmac is wrong; correct §102b; reply. If UNVERIFIABLE: fetch NSE's
  IndexInclExcl.xls / press releases directly (they cite it; we've never ingested it) before
  any data edit. Fix source layer only — never `indices_history.json` directly.

**P4 — demerger (bounded)**
  a. ALEMBICLTD 2011-04-11: factor = ex-day SPOS open / prev close from the era bhavcopy
  (`_bhav_cache` backup per §87e), append to `demerger_adj.json` (shape
  `[sym, exYmd, factor, raw_drop]`), self-heal run, verify the -68.88% trade re-prices.
  b. Find why `build_demerger_adj.py`'s sweep missed it (subject phrasing — "Scheme of
  Arrangement" era wording) and extend the keyword set; re-sweep 2002-2016 for more misses.
  c. Cross every holding period in the 422-trade log against the (extended) demerger ex-date
  list; re-price hits; record in §102.

**P5 — SHP sub-date convention (decision needed)**
  Minimum: document the qe+21d convention (32,960 rows) in runbook §22 and answer quantmac's
  Appendix A with it. Full: a P2-shaped re-dating campaign against BSE shareholding-submission
  records — ONLY after coordinating with the active SHP session (B3 warning).

**P6 — reply to quantmac (user's call; draft offered)**
  Thank them — 3 of 5 findings real, one already fixed & measured (+0.79pp, the exact
  `ttm_disagree` trade). Correct their two errors with evidence: F2 dates contradicted by BSE
  broadcast records (DELTACORP 2016-08-01 20:00:07; OMAXE 2012-05-30 17:50:05), and Appendix
  B's "our value" column misaligned (TFCILTD 58.87-vs-0.7364 class, 5/5 checked).

---

## D. ADDENDUM — verification-agent verdicts (fill before P3; wording fix in P0.b)

*Slots below are completed by whichever session receives the agent reports; do not start P3
until they are.*

* **Membership agent (PCBL / SANDESH / AJANTPHARM / GAEL / INOXLEISUR):** PENDING
* **Staleness agent (CANFINHOME / GEOJITFSL / MUNJALSHOW / ALFALAVAL / DHANI + direction
  stats):** PENDING
