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
4. **Persistence pre-check: DONE — clean, one expected interaction, not a threat.**
   `scripts/fundamentals.json` is NEVER staged by any CI commit step (grepped every workflow's
   `git add` list) — unconditionally safe. `docs/sf_fundamentals.json` is safe from every writer
   except the nightly 15:30 gate (`gate_1530.py`, via `refresh-fundamentals.yml`'s 21:15 IST
   run): it rescans EVERY cell fresh each night with no new-vs-old distinction, and WILL move a
   corrected date forward by one trading day IF it lands on a month-end trading day AND BSE's
   own record shows the broadcast was after 15:30 IST. That is correct, intended look-ahead
   logic (same as §12), not a revert-to-placeholder bug — it just means the apply step (B4.2)
   must pre-apply the 15:30 rule itself, exactly as already planned, so the nightly gate finds
   nothing left to do (idempotent agreement, the JSL PASS-signal pattern). Every other writer
   checked (`update_fundamentals.py`, `apply_agg_pat_fills.py --repair-ann`, `fill_ann_dates.py`,
   insurer writers, and everything not wired into a workflow at all) is fill-only, structurally
   excluded from an already-dated old cell, or requires a deliberate human re-run — none pose a
   silent-revert risk. Full citations in the agent transcript; safe to proceed to P2.
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
8. **(superseded by addendum #1 — FILESTATUS filter removed.)** BSE quota: watch for the 162-byte 302 stub (§0). Serial + 0.3s sleep first; consider
   2-3 shards only if an hour of serial shows zero 302s. Newest-first symbol order is NOT
   needed (per-symbol windows span eras anyway), but keep the resumable checkpoint (it exists).

---

## C. THE CAMPAIGNS, SEQUENCED

**P0 — asserts & corrections (minutes, do first)**
  a. F-1 unique-qe assert (A1). b. §102b membership-row wording fix once the addendum lands.
  c. Persistence pre-check (B4.4). d. Commit this plan + addendum.

**P1 — matcher hardening pilot — ✅ PASSED 2026-08-20, gate 97.4% (target 95%)**
Full 36-symbol pilot, 3 clean runs after 3 real bugs were found and fixed (each found by asking
*why* a number was surprising, not by accepting it):

1. **Future-dated query returns a degenerate empty response, not an error.** `strToDate` even
   one day past today (verified to the exact day: 2026-08-20 works, 2026-08-21 breaks) makes
   BSE's endpoint return `Table1: None` + one null-ish `Table` row instead of a real answer or
   an error. Every symbol whose latest target quarter was recent — i.e. most of the FRONT of
   the newest-first-sorted campaign list — silently read "0 candidates" this way (INGERRAND
   0/60→52/60, TMPV 0/57→57/57, RAMANEWS 0/8→6/8). Fixed: cap `strToDate` at today; if the
   degenerate signature appears anyway, raise (never silently treat as zero results).
2. **Numeric `DD.MM.YYYY` dates never matched** — the regex only recognized month-name forms.
   Real disclosures (GEOJITFSL's genuine Sep-2019 filing: "Ended 30.09.2019") use this form
   routinely, more so in recent years — which is why 2015+/2020+ rates read LOWER than the
   older era at first, backwards from what improving digital records would predict. Fixed:
   added a numeric-date regex alongside the month-name one.
3. **Combined annual+quarterly announcements carry TWO dates; `.search()` only found the
   first.** ESSAROIL's real 2010-07-27 filing: "results for the year ended March 31, 2010 & ...
   quarter ended June 30, 2010" — the quarterly date (the one usually needed) was silently
   discarded. Fixed: `finditer()` collects every date in the text, row indexed under all of
   them. Also broadened the anchor word to `ended|for` (INDORAMA: "Financial Results for Jun
   30, 2009" — no "ended" anywhere).

**Final numbers, full 36-symbol pilot, all fixes applied:**
| window | matched / total | rate |
|---|---|---|
| pre2008 (2002-2007) | 168/311 | 54.0% — expected low, BSE archive floor |
| **2009-2019 (the plan's gate)** | **453/495** | **91.5% raw, 97.4% excluding 2 unresolvable symbols** |
| 2020+ | 38/64 | 59.4% — see residual notes below |

**Two known, characterized residual-gap classes (not bugs — confirmed structural):**
- **No-scripcode (2/36 pilot symbols: ALOKTEXT, ARVEE).** Neither `bse_scrips.json` nor
  `_bse_master_all.json` resolves these — likely delisted/obscure enough to be absent from
  both. Expected small tail at full-campaign scale (~2,623 symbols); left alone, recorded.
- **BSE HEADLINE field truncation (FCSSOFT-class, persistent 1/10 across every run).**
  Verified directly: BSE's own API returns a HEADLINE literally cut off mid-sentence at 189
  chars ("...this is to intimate that the Board of Directors in its 200th Meeting held ....")
  for some companies' announcement templates (typically "Outcome of Board Meeting" framing
  rather than "Financial Results" framing) — the quarter-end date isn't in the retrievable
  text AT ALL via this endpoint, regardless of regex sophistication. The row IS tagged
  `CATEGORYNAME='Result'` even though the text doesn't say so — a possible future signal, but
  fixing this properly needs the PDF attachment (the `AnnPdfOpen` resolver route the staleness
  verification agent already proved out), which is a heavier per-cell operation not worth
  building into the bulk P2 pass. Leave these as `not-found-via:bse-ann`, revisit as a
  targeted follow-up if the residual count after P2 is large.
- These two classes plausibly explain most of the 2020+ shortfall too (FCSSOFT alone
  contributed 6 of the 21 unmatched 2020+ cells in the pilot); not fully attributed, but not
  reflecting a parsing gap either — no THIRD systematic bug was found after checking.

**Also confirmed correct**: the future-date fix's own log line shows `d2=20260820` (today,
exactly) — the cap is working, not just tested. Determinism verified — 3 full runs of the same
36 symbols produced identical or monotonically-improving results, no run-to-run drift.

**4th bug, found while smoke-testing `apply_redating.py` against the pilot's own real data
(2026-08-20, before P2 finished) — a year-ago COMPARISON figure can masquerade as a second real
disclosure.** ALFALAVAL's genuine 2004-09-30 result headline: "...Rs 200 million for the quarter
ended September 30, 2004 as compared to Rs 170.19 million for the quarter ended September 30,
2003" — `extract_all_qes` (correctly, per bug #3) finds BOTH dates, but only one is this filing's
real subject; the other is prose context. Unlike ESSAROIL's genuine combined annual+quarterly
disclosure (both dates real subjects), this class can't be told apart by regex alone. Fixed at
the APPLY layer, not the matcher: a real filing can never predate its own quarter-end and every
verified case so far lags it by 13-60 days, so `apply_redating.py` rejects any candidate whose
raw filing date falls outside `[qe, qe+120d]` — caught 8/670 (1.2%) in the pilot re-check, all
now correctly left untouched instead of writing a ~1-year-wrong date. Surviving decisions:
median lag 17 days, p90 29, max 47 — a sane distribution. **This guard is why every fetched
match must go through `build_decisions()` before being trusted, not written directly.**

**P2 — the 36,027-cell re-dating campaign (background, resumable, worktree `staleness-fix`)**
  Fetch: `scripts/_staleness_fix/fetch_and_match.py` (v2, FILESTATUS filter removed, scripcode
  fallback added — pilot-tested, see P1 results above). `target_list.json` is sorted
  NEWEST-FIRST by each symbol's latest target quarter (user directive, 2026-08-20) — matches
  §12's own measured precedent that BSE's archive answers recent dates fast and old ones slowly
  (11 dates in ~20min oldest-first vs all 101 in ~15min newest-first), so this ordering avoids
  the run stalling on the slow 2002-2007 tail near the end rather than the start.
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

**P3 — membership (UNBLOCKED, confirmed; use IndexInclExcl.xls as the ground-truth ledger)**
  1. Parse `IndexInclExcl.xls` fully (all 2,496 rows, all indexes it covers, not just Nifty 500)
     into a clean per-index event ledger — this is a NEW, better-than-anything-we-have source;
     don't just patch the 5 symbols, ingest the whole file.
  2. Trace the exact `build_membership_v2.py` code path that re-admits a "continuously-listed
     but excluded" name (the agent localized the SUSPECT guard, lines ~254-263, but did not
     confirm the exact bug) — measure BEFORE fixing, per the phantom-floor precedent in §48.
  3. Fix by feeding the new event ledger into the reconstruction (source layer + rebuild),
     NEVER edit `indices_history.json` output directly (weekly refresh reverts it — standing
     rule). Re-run `verify_sizes.py` + the ~500-504/date count-check after.
  4. Push, LIVE bin re-verify, re-run the DII strategy end-to-end — the 24 flagged trades
     should now resolve (PCBL×3, SANDESH×4, etc. drop out of the picks).
  5. §102b's overclaimed sentence needs a one-line correction regardless (P0.b): "our data
     disagreed with NSE's own record" is now fully measured, not partially assumed.
  6. Note the file is frozen at 2020-09-14 — historical era only; anything past that still
     needs the live/press-release route if a future gap appears.

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

* **Membership agent: DONE — 5/5 CONFIRMED via THREE independent primary sources.**
  `https://archives.nseindia.com/content/indices/IndexInclExcl.xls` is LIVE and reachable —
  NSE's own authoritative membership-EVENT file, 2,496 rows, ALL indices, 1998-08-01→2020-09-14
  (frozen there). Saved: `scripts/_staleness_fix/IndexInclExcl.xls`. Every one of the 5 claimed
  dates matches to the day (PCBL excl 2002-01-17/incl 2018-09-28; SANDESH excl 2009-03-27;
  AJANTPHARM excl 2011-03-25/incl 2014-09-19; GAEL excl 2012-09-28; INOXLEISUR excl
  2012-04-27/incl 2014-03-28), cross-read against the press-release PDFs and the archived
  official constituent CSVs (`_wb_n500_snaps.json`, 39 captures — PCBL absent from all 26
  captures 2002-2015). **Root-cause locus, self-documented in our own code**:
  `build_membership_v2.py`'s backward-walk (`m = (m - inc) | exc`) has a phantom-floor guard
  whose own comment admits it "does NOT floor already-listed names … that needs true
  membership dates we don't reliably have" — PCBL is exactly that class (continuously listed
  while excluded, so a price-history floor can't see it). Not yet traced to the exact line that
  re-admits it for 2017; that's P3 step 1.
  **This changes P3 substantially — see the revised P3 below.**
* **Staleness agent: DONE — 5/5 CONFIRMED, all EARLIER than placeholder, document-level proof**
  (each verified twice: BSE API row + the filed PDF itself via the AnnPdfOpen resolver).
  CANFINHOME 2013-01-19 16:25 (Δ26d) · GEOJITFSL 2015-01-13 16:17 (Δ32d) · MUNJALSHOW
  2009-07-27 18:40 (Δ18d) · ALFALAVAL 2011-10-21 15:01 (Δ24d) · DHANI 2014-04-22 11:24 (Δ23d).
  All match quantmac to the minute where they gave one. So finding 3 is now 8/8 verified.
  **THREE campaign-critical corrections from this verification:**
  1. ★ **Drop the FILESTATUS='U' filter in `fetch_and_match.py` — it is WRONG for the pre-2015
     era.** 4 of these 5 genuine results filings carry 'N', and 'U' appears on unrelated rows.
     The current script would have silently dropped most pre-2015 results (§61's empty-result
     class, caught BEFORE the burn). Discriminate on NEWSSUB/HEADLINE text + a `*_Rst.pdf`-style
     ATTACHMENTNAME instead; P1's pilot must re-baseline match rates after this change.
  2. **Scripcode resolution needs a fallback**: ALFALAVAL (505885) and DHANI (532960) are absent
     from `bse_scrips.json` by_id — resolve via `scripts/_bse_master_all.json` (scrip_id + ISIN),
     era names included ("Indiabulls Securities" filings sit on DHANI's code).
  3. **March quarters are structurally look-ahead-biased** (era Clause 41: 60-day deadline for
     audited Q4 vs 45 for Q1-Q3, so qe+45d lands BEFORE many real Q4 filings — OMAXE is that
     class). 15,000-odd March cells deserve their own direction stats in P2's report, and the
     agent's cheap screen — a RANDOM (unscreened) sample of March-quarter cells, lag
     distribution vs 45 — belongs in P1. 8/8 EARLIER here says nothing about the base rate:
     quantmac's screen could only surface the stale direction by construction.

---

## E. P2 FULL-CAMPAIGN PRE-APPLY AUDIT (2026-08-20, Opus) — 3 matcher bugs, DO NOT APPLY as-is

Full fetch completed 2,623/2,623 symbols clean (0 failures, ~3.5h, 2-way shard). Dry-run of
`apply_redating.py`: 21,911 cells to re-date (15,819 earlier / 6,092 later), 2,844 noop, 389
rejected-lag, 10,883 no-match. Contract checks PASS (0 cells with new_ann<qe, 0 future; new-date
lag-after-qe sits 14-60d for 20,606 of 21,911, as real results should). BUT an Opus pre-apply audit
of both the matches AND the no-matches found **three defects Sonnet's matcher shipped.** The 21,911
are NOT safe to write as-is.  →  **FABLE: please review this section E — is the diagnosis complete
(any 4th defect class?), is each remedy right, especially Bug 1's intimation discriminator, and
which sequencing option below? I (Opus) will execute whatever you land.**

**BUG 1 — intimations matched as results (LOOK-AHEAD, the cardinal sin). 195 / 21,911 (0.89%).**
`INTIMATION_PHRASES` in fetch_and_match.py is too narrow. It misses "Board Meeting On <date>",
"Board Meeting To Consider", "Intimation Of Board Meeting", "Change in Date of Board Meeting",
"Analyst / Investor Meet - Intimation", "Newspaper Publication", "to consider/to approve". Because
match_targets picks the EARLIEST candidate for a qe, an intimation (always broadcast BEFORE the
result) WINS over the real filing. Proof: PAGEIND qe20171231 matched "Board Meeting On 08th
February 2018" (bcast Jan 17) → would stamp 20180118, a ~3-WEEK look-ahead. MTNL qe20170630 (−27d),
GMDCLTD qe20120930 (−40d), PRSMJOHNSN (−34d), ZFCVINDIA (−36d), FSL (−33d). 162 earlier / 33 later;
32 are delta≥25d (the dangerous ones). Remedy: (a) reject intimations in is_candidate — flag
forward-meeting/notice language UNLESS it's an "outcome of board meeting"/results-statement; (b)
rank candidates (is_intimation asc, then NEWS_DT asc) so a real result always beats a stray
intimation; (c) apply-layer safety net: refuse to write any re-date whose stored newssub is an
intimation (those fall back to the safe qe+45d placeholder — no look-ahead). ⚠️ discriminator risk:
a real result often reads "…Unaudited Financial Results for the quarter ended…", the SAME words an
intimation uses — the only separator is the forward-meeting framing. Get this regex wrong and we
either keep look-ahead or nuke thousands of real results. This is the piece most worth a 2nd read.

**BUG 2a — scripcode resolution misses renamed/aliased names. 251 symbols / 1,866 cells.**
Correction #2 (§ above) added a `_bse_master_all.json` fallback, but it too keys on scrip_id ==
our-symbol, which fails whenever our fundamentals ticker ≠ BSE scrip_id. Confirmed live: COLGATE→
BSE "COLPAL" (500830), CEAT→"CEATLTD" (500878), TUBEINVEST→"TIINDIA" (540762), ORCHIDPHAR→
"ORCHPHARMA", SUPPETRO→"SPLPETRO", INDIABULLS→"IBULLSLTD". All resolvable via ISIN (bse_scrips.json
has by_isin; master has ISIN_NUMBER) but we lack our_symbol→ISIN, so it needs the identity bridge
(check_fund_alias.py / isin_seam_*). Some of the 251 are genuinely dead (STER, SATYAMCOMP,
MONNETISPA) where even a correct code yields nothing. Remedy: resolve by ISIN/alias, not scrip_id
string.

**BUG 3 — date extractor drops "Ended On" and 2-digit years. Part of the 8,290 qe_not_found.**
Live proof: SANWARIA qe20221231 HAS a real disclosure — "Unaudited Financial Results For The Period
Ended On 31.12.22" (bcast 2023-02-13) — but extract_all_qes returned []. Two misses at once: the
word "On" between anchor and date (regex wants `ended <digit>`), and the 2-digit year "22" (regex
demands `\d{4}`). qe_not_found is concentrated 2003-2008 (~6,100, likely a real BSE
electronic-filing sparsity floor — NOT yet proven, must sample) with a suspicious 2019-2025 lump
(~2,100) that this bug explains. Remedy: allow optional "on"/"as on"/"as at" after the anchor;
accept 2-digit years (→20xx); re-sample the pre-2008 floor to size the true-absence remainder.

**NO-MATCH breakdown (10,883):** qe_not_found_among_candidates 8,290 · no_scripcode 1,866 ·
no_candidates_at_all 727 (all from the no-scripcode/zero-match symbols). 0 fetch errors.

**SEQUENCING — pick one (Fable/user):**
* **Option A (one clean pass, recommended):** fix all 3 in fetch_and_match.py → re-run only the
  affected symbols (any no-match or any intimation-flagged match) → merge over the clean matches →
  apply once. Cleanest single reviewable diff; costs ~2-3h more wall-clock of unattended fetch.
* **Option B (bank safe now):** apply the 21,716 non-intimation cells immediately (the 195 + the
  10,883 keep the safe placeholder), then fix+targeted-re-run later to recover the rest. Two
  apply/verify/push cycles; banks quantmac-class value now. Safe & additive — the 21,716 clean
  cells won't change in a re-run.
* **NEVER:** write the 195 intimation matches (Option C = apply-all-as-is is OFF the table).

Nothing has been written to sf_fundamentals.json / fundamentals.json. Audit scripts:
scratchpad/audit_redating.py, size_bugs.py (regenerable). redate_ledger.json is the current dry-run.

---

## F. FABLE REVIEW OF SECTION E (2026-08-20) — audit CONFIRMED, 4 additions, Option A binding

Verdict on Opus's audit: all three bugs are real, correctly root-caused, and correctly sized;
the contract stats (0 pre-qe, 0 future, lag mass in 14-60d) are the right frame. Four additions,
one of them a 4th defect class Opus asked about and that I then MEASURED:

**F0. BUG 4 — newspaper/secondary re-publication rows matched as the disclosure. 501 + 11 cells.**
Reg 47 forces companies to re-publish results in newspapers AFTER filing; "Announcement under
Regulation 30 (LODR)-Newspaper Publication" rows carry the same "quarter ended X" text and matched.
Measured: 501 newspaper-sourced matches (472 later / 29 earlier — the later-skew is exactly the
re-publication mechanism; the 29 earlier are likely cases where the REAL result row failed date
extraction, i.e. Bug 3, and the ad won by default). Plus 11 "Updates on…" rows. Not look-ahead
(mostly), but systematically-biased dates. Same remedy family as Bug 1: these become a DEMOTED
class, not rejected — see F1.

**F1. One classifier, three ranks — and it must be a single shared function.**
Replace INTIMATION_PHRASES with `classify_row(sub, head) -> 'result' | 'secondary' | 'intimation'`:
* 'intimation' = forward-meeting/notice language (board meeting on/to consider/intimation/
  reschedul/notice of/analyst-investor meet/prior intimation/Reg 29) UNLESS outcome-override.
  Outcome-override MUST include BSE's own post-2018 template prefix `board meeting outcome`
  and the generic `outcome of (?:the )?(?:board )?meeting` — Opus's audit regex missed these and
  wrongly flagged SFL/LAMBODHARA-style REAL outcomes; in the fetch-side classifier that mistake
  would discard real results, the expensive direction.
* 'secondary' = newspaper publication / "Updates on" / newspaper-advertisement re-publications.
* 'result' = everything else that passed is_candidate.
Ranking inside match_targets: (class_rank result<secondary<intimation, then NEWS_DT asc).
An intimation may NEVER be written (apply-layer refuses, falls back to placeholder); a secondary
MAY be written when it's the only source (better bound than the placeholder; provenance-tagged
'secondary'). ⚠️ The audit script and apply_redating.py must IMPORT this same classify_row —
Opus's audit used a parallel regex; two regexes drift and the audit stops measuring what ships.

**F2. The 120d lag cap itself keeps a look-ahead class — relax it for single-date result rows.**
A genuinely-late filer (IBC-era companies file 6-12 months late) has real ann ≫ qe+45; the current
[0,120] reject KEEPS the placeholder = keeps the look-ahead. The ALFALAVAL comparative-confusion
the cap was built for is a MULTI-date signature (year-ago comparison alongside the real quarter).
So: rows whose extracted-qe set has exactly ONE date and classify as 'result' → accept lag up to
400d; multi-date rows keep [0,120]. Direction asymmetry note: accepting a later-than-placeholder
date is the safe direction (pessimistic); the only fatal error is accepting an EARLIER wrong date —
which is why intimations are hard-refused rather than demoted. lag<0 stays refused always.

**F3. NSE-only listings are a distinct, legitimate no-match class — classify, don't chase.**
Bug 2a's remedy (ISIN/alias resolution) must end with: our-symbol → ISIN → if that ISIN appears
NOWHERE in the BSE master, the name is NSE-only; record `error:'nse-only'` and leave the
placeholder — a BSE campaign cannot date it (future NSE-archive pass, out of scope). Symbol→ISIN
source: try scripts/_isin_seam_verdicts.json + FUND_ALIAS machinery (check_fund_alias.py) first;
they exist for exactly this. Dead names with no ISIN route (SATYAMCOMP…): try era-name/Issuer_Name
lookup in the master; if still unresolved, record 'unresolvable' with a count — never guess.

**F4. Persist the raw candidate rows this time.** fetch_results.json stores only the winning
match; every matcher change therefore re-hits BSE for hours. The re-run must write each symbol's
raw candidate rows to a cache (jsonl, one file per shard is fine, ~tens of MB, worktree-local,
NOT committed) so every future matcher tweak re-matches OFFLINE in seconds. This lesson is
memory-worthy independent of this campaign.

**SEQUENCING — Option A, and B is now argued CLOSED, not just dispreferred:** under B, the
"clean 21,716" were screened by the AUDIT regex, not the shipping classifier; F1's corrected
classifier will flag a (small) superset, so B would write cells the improved filter then wants
back — and a retraction is the expensive class (retraction-needs-every-ledger). A single clean
pass never writes a cell it later regrets. Scope measured, not guessed: 1,551 symbols carry ≥1
unmatched cell; + symbols carrying flagged matches ≈ ~1,600 symbols ≈ ~2h at the observed 2-shard
rate.

**EXECUTION ORDER FOR OPUS (each step gates the next):**
1. fetch_and_match.py v3: classify_row (F1) + date-regex extensions (Bug 3: optional on/as on/
   as at after anchor, 2-digit year → 20xx, `Q.E.`/`QE` anchor, tolerate missing space after
   anchor) + ISIN/alias scripcode resolution with 'nse-only'/'unresolvable' classification (F3)
   + raw-row cache (F4). apply_redating.py: import classify_row for the refusal net; lag rule
   per F2; provenance string carries the class ('result'/'secondary').
2. OFFLINE calibration BEFORE any re-fetch (data already on disk): run classify_row over all
   21,911 stored newssubs + the 195 flagged + the 5 document-verified real filings (CANFINHOME
   2013-01-19 etc.) + SFL/LAMBODHARA outcome-prefix cases. Gates: 195/195 flagged ⊆
   {intimation,secondary}; 5/5 verified real = 'result'; outcome-prefix cases = 'result';
   newly-flagged among the 21,716 ≤ ~2% and 20 random newly-flagged eyeballed all genuinely
   non-result. Fail any gate → fix classifier, rerun; do NOT proceed on a failed gate.
3. Re-fetch the affected set (~1,600 syms: any-unmatched ∪ any-flagged-match), 2 shards,
   newest-first, same monitor pattern, raw-row cache ON.
4. Merge (replace re-fetched symbols' entries wholesale), dry-run apply, re-run the audit
   (now importing classify_row). Gates: 0 intimation-sourced decisions; 0 new_ann<qe; 0 future;
   lag histogram sane (March-quarter cells may legitimately sit 46-60d); secondary-sourced
   count reported with direction split; single-date>120d acceptances listed for eyeball.
5. --apply once. Verify: JSON-parse both fundamentals files; ledger-count == applied-count;
   10 random cells diffed file-vs-ledger; agg_pat_cell_fills.json sync count == expected;
   re-run --apply is a no-op (idempotency). Bump the sf cache rev (see memory
   project-stocks-sf-cache-key-rev / runbook) so clients refetch.
6. A/B: re-run the DII strategy backtest before/after; the 8 quantmac staleness trades must
   resolve to their verified dates; record deltas in §102.
7. Push via the worktree recipe; verify ON ORIGIN BY CONTENT; verify LIVE ~20 min later
   (CI race); §39 gate on any touched page surface; write section G (final stats) here +
   runbook §102/§103 update + memory. Report to user in layman terms per standing instruction.

195 intimation cells, 512 secondary cells, the lag-cap class, and the no-match recoveries all
land in ONE reviewable ledger. Nothing writes until step 5, and step 5 runs once.

---

## G. OPUS VERIFICATION OF SECTION F (2026-08-20) — 2 of Fable's 4 additions corrected by measurement

Every F claim was re-measured, and where it made a factual assertion I went to BSE live. Verdicts:

**F1 CONFIRMED — my own audit regex was wrong. 16 of my 195 were FALSE POSITIVES.**
Measured: 16 flagged rows carry BSE's genuine outcome template (VASWANI "Board Meeting Outcome for
To Approve Financial Results For Quarter Ended September 30, 2022"; GUJRAFFIA/SFL/ARCHIES/VLEGOV/
CEREBRAINT alike). My FWD regex fired on incidental words ("intimation", "meeting of the board")
while my OUTCOME override was too narrow to rescue them. True intimation count ≈179, not 195.
Fable's structural remedy — ONE shared `classify_row` imported by fetcher, auditor and applier —
is adopted exactly as written; a parallel audit regex is what produced this error.

**F0 / BUG 4 CONFIRMED as a count, but RE-DIAGNOSED as a SYMPTOM of Bug 3, not a peer defect.**
501 newspaper + 11 "Updates on" reproduced exactly (472/501 later — the Reg-47 mechanism). But the
CAUSE is date-extraction failure on the real row, proven live: RANEHOLDIN qe20240331 —
`2024-05-15 14:57 "Results - Financial Results March 31, 2024"` extracts **[]** (no ended/for
anchor before the date!), so the `2024-05-16 "Newspaper Publication"` row (which does parse) won by
default. Fix Bug 3 and most of the 512 self-resolve into correct result matches. Keep the demoted
'secondary' class as the last-resort tier (Fable's design is right), but expect its population to
collapse after the regex fix — report the post-fix count, don't assume it stays 512.

**F2 REFUTED — do NOT relax the lag cap to 400d. It would write ~209 dates each ~1 YEAR wrong.**
Fable's premise (lag>120 = genuine late filers, multi-date = the ALFALAVAL class) fails on
measurement. Proven live: CESC qe20030630 lag=396d matched
`2004-07-30 HEADLINE "…net profit … for the quarter ended June 30, 2004 **as compared to** … quarter
ended June 30, 2003"` → extracted [20030630, 20040630]. Our 2003 target matched the YEAR-AGO
COMPARISON. All four CESC 2003 quarters show the same ~393-396d offset; IPCALAB likewise. So the
120d cap is doing its job, and these are multi-date after all.
  ⚠️ **My own F2 measurement was also flawed and I'm flagging it**: I reported "209 single-date"
  using `extract_all_qes(NEWSSUB)` only — the second date lives in HEADLINE, which the matcher reads
  but fetch_results.json does not store. "Single-date" was an artifact of my script, not the data.
  Neither Fable's rule nor my check could be sound without the raw rows → **F4 is a PREREQUISITE,
  not a nicety.**
  **BUT Fable's underlying instinct is CORRECT and confirmed**: genuine late filers exist and a flat
  120d cap discards them. Proven live: BHARATRAS qe20160930 broadcast **2017-03-10** (lag 161d),
  `"Standalone Financial Results … for the period ended September 30, 2016"`, single date, no
  comparative, HEADLINE confirms a real filing.
  **REPLACEMENT RULE (supersedes both the flat 120 and the flat 400):**
   1. **Year-ago comparative refusal** (the real fix, needs raw rows): refuse a target qe if the row
      contains another extracted date LATER than it and an exact whole-year multiple apart on the
      same month/day (CESC 2003-06-30 vs 2004-06-30 → refuse). This preserves ESSAROIL-style genuine
      annual+quarterly combos (2010-03-31 & 2010-06-30 — different month/day, 3 months apart).
   2. Lag window widened to **[0, 200]** — admits BHARATRAS-class real late filings; beyond 200d
      accept ONLY when the row's extracted-date set is exactly {target qe} (nothing to compare
      against). Re-measure the lag histogram post-fix and report anything >200d for eyeball.
   3. lag<0 refused always (unchanged).

**F3 CONFIRMED in principle; ROUTE measured and mostly a dead end — bound it, don't chase it.**
No symbol→ISIN map exists in the files F3 named (checked `_isin_seam_verdicts.json`,
`search_index.json`, `liquid_universe.json`: 0/251 hits). I fetched NSE's `EQUITY_L.csv` live
(nsearchives worked though the nseindia.com warmup 403'd — the a-wall-is-a-route pattern) giving
SYMBOL→ISIN for 2,553 names. Result on the 251: **3 symbols / 37 cells recovered**; 97 are in NSE
with an ISIN absent from every BSE map (= genuinely **NSE-only**, exactly Fable's class, now
measured); 151 aren't in the current NSE list at all (delisted, or our ticker is a legacy alias —
our "COLGATE" is NSE's **COLPAL**, so a symbol-keyed lookup can never find it). Normalized
company-name bridge adds ~21 more. **Decision: implement resolution chain by_id → master scrip_id →
NSE-symbol ISIN → normalized-name, then CLASSIFY the remainder (`nse-only` / `unresolved-alias`)
and leave the placeholder.** ~1,700 of 36,027 cells (4.7%) stay unfixed as a measured, named
residue — future route is the FUND_ALIAS/rename machinery + an NSE-archive pass (NOT "unfillable").

**F4 CONFIRMED and PROMOTED to prerequisite** — see F2 above; without raw rows neither the
comparative rule nor any future matcher tweak can be evaluated offline.

**F5 (Opus addition) — BUG 3 is bigger than stated: the anchor requirement itself drops real rows.**
Beyond "Ended On" + 2-digit years, the RANEHOLDIN row proves a results row can carry a bare date
with NO anchor word ("Results - Financial Results March 31, 2024"). Remedy: additionally accept an
**anchor-less date only when it is a true quarter-end (Mar31/Jun30/Sep30/Dec31) AND the row text
contains "result"** — tight by construction, and harmless because targets are only ever quarter-ends
(a stray "Held On May 15, 2024" is not a quarter-end and can never match a target).

**NET EFFECT vs section F:** Fable's Option A, 7-step order, shared classifier, secondary tier, and
F4 all stand. Changed: no 400d cap (comparative rule + 200d instead), Bug 4 reclassified as a Bug-3
symptom, F3 bounded with measured residue, Bug 3 widened per F5. Proceeding to implement.

---

## H. FINDING 5 (ALEMBICLTD demerger) — VERIFIED, HEALED, AND SWEPT (2026-08-23, Opus)

**The trade:** ALEMBICLTD 2011-03-31 → 2011-04-30, booked −68.88%. quantmac: a scheme-of-arrangement
ex-date (2011-04-11) inside the holding period was booked as a price loss.

**Verified against primary sources, both leg and cause:**
* NSE bhavcopy 2011-04-08: close 70.35 (EQ). 2011-04-11: OPEN 32.40, close 24.90, series flipped
  EQ→BE — the classic scheme ex-day signature (pre-open discovery of the residual company).
* BSE announcements (scrip 506235): 2011-02-23 "Approval of Scheme of Demerger … transfer of the
  Pharmaceutical Undertaking", 2011-04-01 "Fixes Record Date [2011-04-14] for Scheme of Arrangement".
* Root cause in OUR system, two layers: (1) `build_demerger_adj.py` sweeps the NSE CA feed from
  **2016 only** — 2011 was structurally invisible; (2) `corp_actions.json` noadjust carries
  ALEMBICLTD 20110411, i.e. the gap had been classified keep-the-drop (crash class) at some point.

**Heal (ledger route, per the runbook):** appended `["ALEMBICLTD", 20110411, 0.4606, 0.3539]` to
`scripts/demerger_adj.json` (factor = ex-open/prev = 32.40/70.35, the same convention as all 94
existing rows; raw close-ratio rides along as the reconciliation anchor). Consumer verified before
writing: `update_sf_data.self_heal` reconciles ledger demergers EVERY run regardless of age,
bar-exact; the noadjust flag does NOT fight it (both enqueue paths converge on the bar-exact ledger
factor — and ALEMBICLTD 20190826 already coexists in BOTH maps in production as the working
precedent, so 20110411 was left in noadjust for symmetry). The bin heal lands on the next nightly
self-heal; verify the series then (pre-ex history ×0.4606, the −54% ex-gap gone, the ex-day's own
intraday move kept).

**The sweep quantmac asked for ("other demerger ex-dates inside holding periods") — BOTH trade
logs (548 trades, 2004-2026), two nets:**
1. Every noadjust (keep-drop) date inside any holding window: exactly 2 hits — ALEMBICLTD (above)
   and **WIPRO 2013-02-28→2013-04-30 (booked −16.33%), keep-drop 2013-04-09**. Verified the same
   way: bhavcopy 2013-04-08 close 448.80 → 2013-04-09 OPEN 407.00 (−9.3% pre-open separation);
   BSE 2013-04-01 "Wipro's scheme of arrangement for demerger [Diversified Business] effective from
   March 31, 2013" + record-date fixing. No official split/bonus within ±3d (nearest 2010/2017).
   Healed: `["WIPRO", 20130409, 0.9069, 0.8772]`. ~9.3 of the −16.33 points were mechanical.
2. Every trade booked ≤−20% (23 of them), window-swept via BSE for scheme/demerger/record-date/
   special-dividend signals: 19 clean (genuine market losses — Jan-2008, May-2004, COVID-2020,
   IL&FS-2018 clusters). 3 adjudicated: KITEX bonus record date falls AFTER its window (clean);
   ZODIACLOTH 1:1 bonus 2005-10-24 already carries factor 0.5 in corp_actions (clean);
   **HGS 2021-12-31→2022-01-31 (booked −22.92%): ₹150/share special interim dividend (the
   healthcare-sale payout), record date 2022-01-18 INSIDE the window — ~7.7% of the price was cash
   handed to the holder, not a loss.** Logged as a CANDIDATE CLASS (large cash separations), not
   healed: the engine is price-return by convention, quantmac's replication books the same number
   (it reconciled — not in their 157), and adjusting dividends is a policy decision for the user.

**Not yet done:** the nightly-rebuild verification of both healed series, and the backtest A/B on
these 2 trades (fold into the P2 apply's A/B run).

**H.1 addendum (2026-08-23): special dividends now adjust like demergers — USER-APPROVED policy.**
HGS healed: `["HGS", 20220117, 0.9527, 0.9343]`. Factor differs from the demerger convention on
purpose: a cash dividend's separation is EXACTLY the cash (₹150), so factor = (prev − div)/prev =
(3168.30−150)/3168.30 — NOT open/prev, which would have wrongly adjusted out ₹73 of genuine market
fall that day (open gapped −223). raw close-ratio 0.9343 rides as the anchor; idempotence
arithmetic verified (post-heal applied_f == correct_f). Same ledger, same self_heal machinery.
PENDING: sweep BOTH trade logs for other ≥2%-of-price cash separations (dividend record dates
inside holding windows) — queued until the v3 BSE fetch finishes, to avoid a third query stream
tripping the rate-limit. Pre-2016 dividend amounts will need the BSE announcements themselves.

---

## I. FINDING 1 (membership) — FIXED, VERIFIED, SHIPPED (2026-08-23)

Root cause measured, two legs: (1) `_changelog.json` = 74 events from 2015-03-23 only, with holes
even in-window (PCBL 2018-09-28 re-inclusion absent) → the backward walk never rolls pre-2015
joiners out of the past; (2) the Moneycontrol soft checkpoints pin STALE rosters — 59 slots
measured naming NSE-excluded stocks years later (wb official checkpoints: 0 conflicts, measured).

Fix in build_membership_v2.py behind the existing wb≥99% gate, all three parts required:
1. **Register merge** — `_n500_inclexcl_events.json` from NSE's IndexInclExcl.xls (generator:
   `_staleness_fix/gen_inclexcl_events.py`; 2,495 events 1998-2020, 919/1,280 names mapped;
   fuzzy blacklist for provably-wrong pairs (IPCL≠IGPL, BPL-Eng≠HBLENGINE, BIL≠ITL); manual
   entries PCBL/CRESTANI/STYRENIX/SMLMAH; 361 unmapped RECORDED never guessed; NATIONALUM
   1999-04-29 same-day inc+exc dropped as ambiguous). 187 pre-changelog event-days + 28
   in-window HOLE event-days (logged each: PCBL, SBI-merger trio, FRETAIL/MAXIND…).
2. **MC reconcile vs register** — register outranks the scraped MC page: −352 stale slots,
   +776 joiners MC never showed. wb untouched (official, 0 conflicts).
3. **`reanchor_segments()`** — between-pin snapshots re-derived from the LATER pin. The single
   global walk COMPOUNDS one-legged-event drift with depth: first pass measured +60 members at
   2006-06-30 (560), −28 at 2010; scrub-only then measured 434 at mid-2011. Segment anchoring +
   the MC add-leg brought every month-end 2003-2026 into [481, 520], median 499.

**Verification (all measured, before vs after):** 24/24 quantmac (sym, month) pairs member→
NON-member; 11 positive controls intact incl. every re-inclusion era (AJANTPHARM-2015,
INOXLEISUR-2015, PCBL-2019, GAEL-2021, DYNAMATECH-2016); SCI-2023 spot check unchanged
(pre-existing quirk, verified not a regression); wb validation 100% at every checkpoint.

**Ship state:** pushed (verified by content on origin: PCBL/SANDESH non-member, RELIANCE member,
319 snapshots). `docs/stock_data.bin` deliberately NOT committed from this checkout (stale staged
prices) — refresh.yml dispatched (run 32629715534) to bake indices_history.json with fresh prices;
pages.yml dispatch + live `?cb=` verify PENDING after it lands. Known residue, recorded: 361
unmapped register names (mostly dead, outside tracked universe); alias-space mismatches on a few
in-window legs (CEATLTD-vs-CEAT class) — window-local, bounded by pins, strictly no worse than the
missing-event status quo; 2008 MC-era months read up to 520 (+20 scraped-era slop).


---

## J. P2 CLOSE-OUT — APPLIED, PUSHED, ORIGIN-VERIFIED (2026-08-23)

**Final numbers (v3.1 matcher, after 6 live-case classifier corrections):** 22,581 of 36,027
placeholder cells re-dated from BSE broadcast timestamps, 15:30-gated (15,5xx earlier / 7,0xx
later); refused rather than guessed: 804 intimation-sourced, 206 year-ago comparatives, 230
implausible lags; 8,614 no-confident-match + ~3,3xx already-correct keep the placeholder; 328
provenance-tagged [secondary]. Contract on the shipped ledger: 0 pre-qe, 0 future, 0
intimation-sourced; apply idempotent (byte-identical second run); redate_ledger.json committed.

**Post-push defect found & fixed (v3.1):** MUNJALSHOW qe20090630 kept its placeholder — an April
"FY 09 results BY Jun 30, 2009" timing-notice bare-date-matched the quarter, beat the real July
filing on earliest-wins, and died at the lag<0 gate, losing the cell. `results by <date>` →
intimation; +284 cells recovered, lag-rejections 1,049→230. The raw-row cache made every such
iteration an offline re-match (seconds), exactly as F4 intended.

**All 8 quantmac staleness trades verified ON ORIGIN:** MARICO 20090422 exact · HEROMOTOCO
20090121 (20th gated) · CANFINHOME 20130121 (Sat 16:25→Mon) · GEOJITFSL 20150114 (16:17→next day)
· MUNJALSHOW 20090728 (18:40→next day) · ALFALAVAL 20111021 (15:01, same day) · DHANI 20140422
(both bases) · HINDUNILVR 20090127 (BSE broadcast; their 2009-01-25 is a newspaper print — ours
2d conservative, no look-ahead). Finding 2's OMAXE → 20120531 (real 2012-05-30 17:50 disclosure;
NB BSE contradicts quantmac's 2012-06-14 — raise in the reply).

**§104 coordination (the mid-campaign race):** another session's ann-date truth pipeline
(override ledger + nightly BSE reconcile) landed while P2 ran. MEASURED disjoint: their 161
override cells ∩ these 22,581 = 0; their fill-only entries need ann==0; their nightly --reapply
is earlier-only. No fight. Race itself resolved per the minified-JSON rule (reset to fresh
origin, re-ran the applier).

**Rebuild-proofing note:** these 22,581 live as direct values + the committed redate_ledger.json;
a hypothetical full placeholder-regeneration would need `apply_redating.py --apply` re-run (docs
in _staleness_fix/). The 786 agg-ledger-backed cells are additionally sync-protected.

**Still pending:** live `?cb=` verify after pages deploy · next-day re-verify (CI race window) ·
DII strategy A/B + full 422-trade re-reconciliation vs quantmac's log · response workbook rebuild.


---

## K. A/B BACKTEST — OLD world (pre-fix engine+data) vs NEW, DII strategy 2009-2026 (2026-08-23)

Node harness (grid_search_full.js loader pattern + SHPD load replicated — without it diiPct is
null and the screen buys nothing, which read as cagr=0 on the first run). OLD = commit c46c213fa~1
(pre-TTM-fix engine, 121 N500 snapshots, pre-campaign fundamentals); NEW = origin/main (e7 engine,
319 snapshots, 22,581 re-dated cells). SAME price tape both sides (committed sf bin) — the diff
isolates the fixes. cfg: N500 · d52<=10 · NP YoY>25 · TTM>0 · sort diiPct asc · top 3 · hold
winners · std basis · monthly, 2009-01-01..2026-08-17.

**Headline: CAGR 53.65 -> 53.19 (-0.46pp), maxDD 52.69 -> 48.53 (-4.2pp better), 52 of 212
monthly baskets changed.** Return ~unchanged while drawdown improved = the strategy's edge was
NOT the phantom picks; the phantoms mostly added risk.

**Finding-1 trades:** 15 of quantmac's 24 reproduce in the OLD harness run — ALL 15 GONE in NEW,
0 still picked (the other 9 don't reproduce in-harness; data-layer membership verification
already covers all 24 directly).
**Finding-3 trades:** 4 of 8 reproduce in OLD (MARICO, HEROMOTOCO, ALFALAVAL, DHANI) — ALL 4
RESOLVED in NEW (fresh failing quarter now visible at entry). CANFINHOME/GEOJITFSL/MUNJALSHOW
don't reproduce in-harness; their CELLS are origin+live-verified directly.
**HINDUNULVR residue — a DIFFERENT class, left OPEN and reported:** still picked because our PAT
VALUES disagree with the era's press figures: our Dec-07 std PAT 473.79 vs BS 631.44; Dec-08
632.24 vs 615.74 -> our pair says +33.4%, truth -2.5%. Date healed; values are §101 class-2
(HUL's Dec->Mar fiscal-year transition era). Not silently absorbed into the date campaign.


---

## L. FABLE VERIFICATION PASS (2026-08-23) — everything re-measured adversarially; 2 real classifier gaps found & shipped (v3.2)

**VERIFIED CLEAN (independent re-measurement, not re-running Opus's own tests):**
* LIVE CLIENT: the deployed stock_data.bin serves 319 N500 snapshots; PCBL/SANDESH/AJANTPHARM
  non-member at flagged dates, RELIANCE + re-inclusions member — through the client origin, 6/6.
* OTHER-INDEX BLAST RADIUS: every fixed-size index byte-equivalent at last common snapshot
  (symdiff 0 across all 26); the two derived tiers (MidSmallcap 400 68→72, Smallcap 250 64→66)
  grew snapshots BECAUSE they derive from N500 — expected, rosters unchanged on common dates.
* GATE ARITHMETIC: 8/8 sampled 15:30-gated cells recompute exactly from gate_calendar tdays
  (incl. Fri-evening→Tue cases).
* SECONDARY RANKING: 0/8 sampled [secondary]-sourced cells had any result-class row available
  for the same quarter — each was genuinely the only source.
* >200d ACCEPTANCES: SAIL-2001/SANOFI-2008 are each the ONLY row in the raw cache mentioning
  that quarter — nothing better existed; both later-direction (no look-ahead by construction).
* DEMERGER FACTORS: all three re-derive exactly (0.4606 / 0.9069 / 0.9527 incl. HGS's
  (prev−div)/prev choice).
* COUNTER-CLAIMS: OMAXE — raw cache holds NO June-2012 results row at all; May-30 17:50 is BSE's
  only results event. DELTACORP — live BSE re-pull: Jul-25 intimation, Aug-01 20:00:07 real
  dual-basis disclosure, **nothing on Sep-28** (their date has no BSE counterpart). Both
  outbound claims STRENGTHENED.
* 157-ROW RE-RECONCILIATION (their appendix C, by their own buckets): membership 15 resolved /
  0 still-picked (9 don't reproduce in-harness; data layer covers all 24 directly); yoy 6
  resolved, 2 still = exactly the two KNOWN opens (HINDUNILVR values, DELTACORP dispute);
  ttm_null still-picked rows are the backfill WORKING (windows now legitimately complete —
  their replication lacked quarters we have since filled and dated); ranked_out/d52/
  no_rebalance/dii_unknowable are their own convention/boundary diagnoses — not unilaterally
  resolvable, correctly left to the joint re-reconciliation.

**FOUND & FIXED (classifier v3.2, offline rematch, re-applied, pushed, origin-verified):**
1. Results-led filings refused as intimations when the headline narrates its own agenda
   ("meeting held today has considered and approved … to consider and take on record") — the
   sibling outcome row lost its date to BSE's ~189-char headline truncation, so the cell kept
   its placeholder. Counted exactly: 10 cells / 7 filings. Past-outcome forms added to the
   OUTCOME rescue. VJTFEDU 20221231 → 20230215 recovered.
2. "Announces Q1 (Standalone & Consolidated) results" — parenthetical broke RESULTS_CORE, a
   bundled "Press Release" demoted 18 real filings to [secondary] (tag-level only). Fixed.
Net: 22,581 → 22,671 decisions (+90), refusals 804 → 765, contract still 0/0/0, idempotent.
(The push raced the 15:02 nightly commit — resolved per the minified-JSON rule again:
reset to fresh origin, re-ran the applier.)

**A/B caveat kept honest:** cfg basis (std) is INFERRED from quantmac's own sheet wording
("Mar-16 standalone") + their 265/422 reconciliation; the saved strategy's JSON lives in
Supabase and was not read. The membership A/B conclusions are basis-independent; the exact
CAGR/maxDD figures assume std.

**OBSERVATION A SCOPE, MEASURED: 25,867 of 25,867 pre-2016 SHP rows (100.0%) carry a
submission date of exactly quarter-end + 21 days.** Pure convention, zero real dates anywhere
in the store. Policy fork for the user: (i) keep + document the assumption; (ii) null pre-2016
subs (kills DII-sorted screens before 2016 entirely); (iii) evidence campaign (BSE archive
pages carry no timestamps per quantmac; Wayback captures give upper bounds only —
project-stocks-shp-wayback-2010 is prior art). NB fetch_shareholding.py / fetch_shp_bse_aspx.py
are DIRTY in the main checkout — another session owns SHP right now; coordinate before touching.

**Dividend sweep (user-approved policy) RUNNING** in background over all 548 trade windows;
candidates ≥2% of entry price recorded to dividend_sweep_results.json for bhavcopy-checked
ledgering (first hit already: GFLLIMITED 2006-11, ₹2 = 6.57% of entry).


**L.1 — Dividend sweep CLOSED (2026-08-23 15:20 IST).** 548 windows swept, 22 candidate
windows, era-tape adjudication passed exactly 4 (CHENNPETRO 20260807 ₹54/4.18%, MRPL 20260303
₹10/5.09%, APTECHT 20140204 ₹2/2.90% series-heal, UNIONBANK 20090513 ₹5/3.03%) — ledgered,
pushed, origin-verified (user approved "write all 4"). 19 rejected with reasons recorded in
dividend_adjudication.json; the split-adjusted-price inflation class (BAJFINANCE "416%") behaved
exactly as predicted and died at era-materiality. Bhavcopy availability for the pre-2007
no-signature rejects verified directly (they are genuine conservative misses, ≤3%). Next-day
live sentinel re-check scheduled: cloud routine trig_01GdeqhgZrVUU4mcHS3Yjwok, 2026-08-24
09:00 IST, read-only.
