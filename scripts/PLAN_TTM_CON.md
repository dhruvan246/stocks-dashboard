# PLAN: profitTTMCon / compositeCon to 100% — handoff for the next session (Opus)

Written 2026-08-18 00:3x IST by the con-triad session (Fable). The user has approved vision reads
campaign-wide. Everything below is measured, not guessed; re-measure anything time-sensitive.

## 0. The rules that are not optional

1. **DATA_RUNBOOK first** — §0 (no assumptions), §39.1b (a fill must MOVE A COUNT; diff EVERY
   param; fill BOTH twins), §39.1c (a heal must be PINNED or the nightly applier reverts it; the
   twins are not mirrors; diff only the SHARED date grid), §56b/57 (a held cell asserts absence;
   settle from the filing).
2. **User's standing rules**: every stock tested individually, a reason recorded per name, and
   NOTHING marked N/A without their explicit confirmation. A documented refusal is a good outcome;
   a plausible invented number is the worst possible outcome.
3. **Work in this worktree** (`~/stocks-wt/n500-cov`), file-scoped `git add`, fetch-rebase-push
   loop, dispatch "Nightly coverage matrix" then pages.yml, verify LIVE with `?cb=`.
4. **Announce dates move FORWARD only** on uncertainty. Moving a date EARLIER requires reading the
   earlier filing that printed the value (see ABB item below — one such case is pending).

## 1. Where it stands

- LIVE (verify first — sessions overlap): last verified 6,968 window holes at 21:22 IST 2026-08-17;
  profitTTMCon/compositeCon 1,665 each. The worktree holds ~55 additional unpushed cells
  (batches E/F/G + JSWDULUX + RBA/VALIANTORG/KIMS date+row heals) — bake gate was running at
  handoff; see §5 for the ship checklist.
- The TTM rule (build_coverage_matrix.js:170): last 4 con quarters vs the 4 before them — ALL 8
  must be consolidated, ending at the latest con quarter whose annCon <= date. TTM closes only
  when whole CHAINS are contiguous; isolated fills can transiently ADD holes (see §4, "expected
  regressions").

## 2. Tooling (all in the scratchpad of session 23cc50b5…, copy them out if the scratchpad is gone)

| file | what it does |
|---|---|
| `PROTOCOL.md` | the complete per-name method for agents: NSE/BSE routes, vision rule, the four gates, spec format |
| `ttm_blockers.py` / `.json` | replicates the TTM rule per (member,date); ranks symbols by blocked cells and names the exact missing quarters. RE-RUN IT after every applied batch — the ranking shifts |
| `write_name.py` | applies a spec: both twin files + conpat_filing_fills.json + owners_basis_heals.json pin + .fund_updated. `create_row` for con-only quarters. Aborts on any unexpected stored value |
| `write_rev.py` | applies revenue twins into sf_revop (slots 1/3), never overwrites, skips meta keys |
| `validate_specs.py` | independent pre-apply check of agent specs against the store |
| `diff_cov.py` | the regression gate; intersects date grids; ANY increase = stop and name the symbol via `--explain nifty-500` |
| `table.py` | renders the 61-param table from any coverage payload (local or live) |

Ledger key format that CI walks (verify_fills_live.py:136): `SYM|qe|con` for PAT and
`SYM|qe|con_rev` for revenue — anything else is invisible to the RESURRECTED/settle machinery.

## 3. The queue, ranked by measured leverage (ttm_blockers.json, re-rank after each batch)

**A. Await the user's N/A decision (~92 cells) — do NOT re-hunt these.** SBICARD(18), TCIEXP(18),
KTKBANK(14), SOUTHBANK(14), JUBLINGREA(17), HEMIPROP(11), SUPPETRO, SWARAJENG. Each has a
filer-worded structural refusal in refusals_A/B/D/E.json and the approval sheet
`na_approval_2026-08-17.txt` was sent to the user. If approved: coverage_na_ledger.json entries
(with from/to bounds + evidence + reader_2), then bake — the count moves from missing to N/A.
Related pending decision: retracting KTKBANK 4 + SOUTHBANK 6 stored con values inside their
pre-subsidiary dead zones (a retraction touches EVERY ledger, runbook §56).

**B. Blocked on the mixed-convention normalization audit (~64 cells).** BHARATFORG(17),
JKCEMENT(17), NMDC(17), MRF Dec-18(13). Stored series mix total/owners rows per quarter with
material NCI; candidate values are recorded in conpat_filing_fills.json REFUSED entries. The DBL
precedent unlocks them: an FY identity that closes to the paisa arbitrates which row each quarter
should hold — build that identity per name from the audited annual owners figure, then heal + fill
in one pass. JKCEMENT extra: stored Jun-19 136.93 matches nothing filed (suspect cell, adjudicate
Q1FY20 first).

**C. In-flight at handoff — check task state before redoing.** Batch H agent: CAMPUS 20200930
(43 cells — the single biggest unblocker), M&MFIN 20190930(21, BSE 532720), TAKE, POWERINDIA,
PRINCEPIPE, MEDPLUS, DOMS, IGIL, VMM, SAILIFE, ABLBL, MAZDOCK Sep/Jun-19, HDFC 20190930(22, owners
row 10,388.61 caution). Batch I agent: FY18 tails — TTKPRESTIG, UNITDSPR, FEDERALBNK, CUMMINSIND,
ESCORTS, HEG, MMTC, KSCL, KPITTECH, GRINDWELL (many source PDFs cached as bt_*.pdf). Also running:
annual-in-slot sweep (spec_*_slot.json) and KSCL owners flip (spec_kscl_flip.json). Their specs
land in the scratchpad — validate with validate_specs.py, apply, bake.

**D. The long tail: 222 symbols, ≤5 cells each, 843 cells.** Shapes and how to batch them:
   - Recent listings needing pre-IPO comparatives (the KIMS/UTIAMC pattern — first post-listing
     filing's comparative column, announce date = that filing's date, `create_row` when no std row).
   - FY18 tails of already-completed names (the batch-I pattern; the Q+1y filing is often cached).
   - One-off mid-series gaps (the CELLO/BAJAJELEC pattern).
   Generate the next batch list mechanically: `python3 ttm_blockers.py`, take the top ~12 names not
   in classes A/B, group 8-12 per agent with PROTOCOL.md, 4-6 agents in parallel. Every agent brief
   must include: confirm `was` from the store yourself; capture rev_con with every PAT; refusals
   with evidence feed the user's N/A sheet.

**E. Singles worth doing inline** (each unblocks 5-10 TTM cells): AADHARHFC 20221231+20220930,
DOMS/IGIL/VMM/SAILIFE if batch H refused any, FLUOROCHEM FY19 chain completion (Sep-18/Mar-19 from
scan composites — unparks the Dec-18=16.68 recorded in xbrl_comparative_fills.json).

## 4. Known expected regressions (do not "fix" these backwards)

- RBA profitYoyCon +2 (2022-03/04) — the cost of removing a 369-day look-ahead. Intended.
- KIMS patCon/postDriftCon +2 (2021-07) — its Mar-21 con first became public 2021-08-11. Honest.
- Isolated new quarters ADD TTM holes until their chain completes — that is why batches G/H/I fill
  chains, not cells. After applying a chain batch, the gate must show the family NET NEGATIVE; if a
  +N remains, run ttm_blockers.py and it will name the still-missing quarter.

## 5. Ship checklist for the held state (do this FIRST)

1. `git -C ~/stocks-wt/n500-cov status --short` — expect docs/sf_fundamentals.json,
   docs/sf_revop.json, scripts/fundamentals.json, scripts/revop_fundamentals.json,
   scripts/conpat_filing_fills.json, scripts/owners_basis_heals.json, scripts/mc_pat_fills.json,
   scripts/mc_history_fills.json (settled holds) — nothing else unexplained.
2. `python3 -X utf8 scripts/verify_fills_live.py` — must be RESURRECTED 0 / MISSING 0, exit 0.
3. `node scripts/build_coverage_matrix.js` + `diff_cov.py pre_batchF.json` — the profit families
   must be net NEGATIVE vs that baseline with only the §4 exceptions positive.
4. `git checkout -- docs/coverage` then commit file-scoped, push via the rebase loop, dispatch
   "Nightly coverage matrix", then pages.yml, then verify LIVE totals with `table.py /tmp/live.json`.
5. ABB pending decision for the user (or verify-and-apply): batch G measured that ABB's Q3CY25
   filing (BSE 2025-11-06) ALREADY printed Jun-25/Sep-25 consolidated — so the applied ann dates
   20260508/20260731 are 6-8 months late. spec_abb_g.json carries the two ann-date heals with
   `con_unchanged: true`. Moving dates EARLIER is only safe after re-reading that filing's page —
   it is cached under scratchpad/F_cache/. Apply only after that read confirms the print.

## 6. What "done" looks like

profitTTMCon = compositeCon = 0 missing (N/A allowed only with user-approved ledger entries), all
61 params at 100% on the LIVE table, verify_fills_live.py green, and every value in
conpat_filing_fills.json traceable to a filing page. The ~843-cell tail at ~10 names/agent-batch
is roughly 20 batches of work; the chains in A-C above cover the other ~800.
