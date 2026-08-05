# FILL-2020 CAMPAIGN — zero empty rev/PAT cells (std + con) for point-in-time Nifty-500, Dec-2019 → date

Planned by Fable 2026-08-05 (session 8837b89a). Executor: Sonnet sessions, one phase per session.
Mission (user): "fill every one of them from 2020 till date, rev and pat, both cons and std."
Read `DATA_RUNBOOK.md` §0 (golden rules), §6 (multi-agent backfill), §3 (insurers), §38 (concurrency),
§39 (ship-it gate) BEFORE starting any phase. This doc sequences those procedures; it does not replace them.

## 0. Non-negotiables (violations have burned us before — each rule cites its scar)

1. **Work in a fresh worktree, never the shared checkout**:
   `git worktree add --detach C:/Users/dhruv/stocks-wt/fill2020 origin/main` — commit+push from there
   (rebase loop, runbook §0), `git worktree remove` when the campaign ends. The interactive checkout is
   DIVERGED from origin and carries other sessions' dirty files (runbook §38).
2. **Baseline = origin/main, never the local checkout's files** (`git fetch origin -q` first; the
   2026-08-05 audit initially ran on the stale checkout and every number was wrong).
3. **Heal through ledgers, not derived files** (CLAUDE.md rule 5): rev/op → `scripts/revop_fundamentals.json`
   (+ `docs/sf_revop.json` same commit), PAT → the §6 `_wf_apply.py` route into `sf_fundamentals.json` +
   `scripts/fundamentals.json`. Direct-only edits get clobbered by the nightly rebuild.
4. **Fill-only. Never overwrite a non-null cell.** Correcting a wrong value is a different procedure
   (runbook §2b) and out of scope here.
5. **Never emit an unanchored value — SKIP instead** (§6 constraint). Every filled cell anchors on: the
   filing's comparative == our stored series, or 9M=ΣQ / FY=ΣQ, or PBT−tax=PAT. Scale confirmed via the
   PAT/EPS anchor (BAYERCROP files ₹ million ÷10; XBRL power-of-ten errors go to `scale_fix.json`, never
   heuristic).
6. **PAT = owners-attributable** (EPS row arbitrates the owners↔NCI tag-swap). **NBFC check**: rev gap ==
   op gap fingerprint means ORFO sits inside revenue.
7. **Provenance journal for every cell** in the tracked ledger entry (system + doc/URL + anchor used) —
   feedback-provenance-every-backfill.
8. **One chunk at a time** (§6 loop): ≤8 fetch agents per Workflow chunk, chunks sequential, no manual NSE
   fetching while a chunk runs. NSE all-transport 403 = lockdown → switch to BSE routes or pause; never hammer.
9. **Commit tool/script edits BEFORE the first batch runs** (reset-and-replay eats uncommitted tracked
   scripts — feedback-reset-replay-hits-tracked-scripts).
10. **Verify against origin after every push** (read the file back from origin/main or live Pages, don't
    trust the driver's "success"), and re-verify LIVE ~20 min later (CI race).
11. **KIRLFER carve-out**: its con series 2022Q2→date is mixed-basis (pending queue, runbook §5) — do NOT
    identity-fill or extend it here; leave for its own fix.
12. **Source priority for fetch work**: NSE archive first (`resultDetailedDataLink` → full P&L;
    feedback-nse-archive-first), then NSE announcements PDF/zip (§6 fetch_nse.py + zip gotcha), then BSE
    announcement API (HEADLINE+NEWSSUB pick rules; the FinancialResult API is poisoned — forcemot), then
    vision-fill for scanned PDFs (§17b tooling). Sign only PARSED rows (NSE dated-URL lies).

## 1. Measured baseline (2026-08-05, vs origin/main, corrected point-in-time membership 497–503/qtr)

Empty cells, quarters Dec-2019 → Mar-2026 (Jun-2026 excluded = filing season):
**revS 222 · revC 1,502 · patS 6 · patC 19 = 1,749 cells across 324 companies.**
Jun-2026 adds 172/205/172/178 — most of that fills itself via the 4×/day cron by ~mid-Aug (results due
45–60d after quarter-end); only the announced-but-unparsed slice is campaign work (Phase E).
Audit harness: rebuild from memory note `project-stocks-n500-monthly-coverage-audit` "Method v2"
(membership = origin `scripts/indices_history.json` N500 nearest snapshot ≤ qe, DUMMY* dropped, symbols
chained through `_rename_map.json`; rev = sf_revop idx0/1; PAT = sf_fundamentals std/con). Re-run it at
every phase boundary — it is the campaign's before/after scoreboard.

## 2. Target taxonomy — four very different problems, don't treat them as one grind

- **A. Structural con-rev (~201 companies, ~1,200+ revC cells, the bulk).** Companies whose gaps are
  con-ONLY while their con-PAT is already filled — i.e. standalone-only filers (AU/CUB/Karur-class banks,
  MNCs: COLPAL, PAGEIND, PFIZER, PGHH, GILLETTE, HONAUT, CASTROLIND, ASTRAZEN, POWERINDIA, BDL,
  ALKYLAMINE, CANFINHOME, SBICARD…) whose PAT got the §6 no-sub con=std identity but sf_revop never did.
  Fix = verified derivation, no fetching (Phase 2).
- **B. Insurers (SBILIFE, HDFCLIFE, ICICIPRULI, ICICIGI, GICRE, NIACL ± MFSL/LICI/STARHEALTH; ~20–26
  cells each).** IRDAI format, invisible to XBRL rev/op. Route: `INSURER_EXTRACTION_PLAYBOOK.md` + runbook
  §3. ⚠️ §3's `_con_tracks_std` finding: only ICICIPRULI passes con≈std; NIACL/HDFCLIFE con genuinely
  diverges — extract, don't derive.
- **C. Real fetch gaps (~120 companies incl. all 222 revS cells).** Two sub-cases:
  (i) **Jun-2022 systemic hole** — 56 revS + 109 revC in that one quarter = the known NSE-XBRL seed gap.
  Try the systemic fix FIRST: re-walk `_xbrl_cache` for 20220630 via `build_revop.py` mechanics (mind the
  OLD-INDAS `ctx_period()` note, runbook §11) and/or fetch the missing XBRLs for that quarter, before any
  per-company grinding. (ii) Scattered per-company gaps (e.g. M&M revS ×17 — likely a tagging quirk;
  IOB — known bucket-D dead-end, timebox it). Tool: `backfill_revop_gaps.py --only SYM,.. --retry-skips`
  (skip ledger `_revgap_skips.json` — freshest copy sits in `stocks-wt/rev-mission/scripts/`, Jul-30; copy
  it into the fill2020 worktree first so prior verified skips aren't re-ground) + §6 `_wf_*` pipeline for
  PDF-read cases.
- **D. PAT residue (6 std + 19 con cells).** Mostly merger/casualty quarters that may never have been
  filed (HDFC Jun-23 class; prior audit's list: BURGERKING Jun-21, FRETAIL Mar-22, HDFC Jun-23, IDFC
  Sep-24, TV18BRDCST Sep-24, GSPL Mar-26) + IOB. For each: confirm whether a filing EXISTS (NSE+BSE
  announcement search around the due window). Exists → fill via §6. Doesn't → record in the unfillable
  ledger with evidence; a never-filed quarter is DONE when documented, not when fabricated.

## 3. Phase plan (one Sonnet session per phase; /clear between)

### Phase 0 — Setup + fresh inventory (~30 min)
1. Create the `fill2020` worktree off origin/main. Copy in gitignored ledgers: `_revgap_skips.json` from
   `stocks-wt/rev-mission/scripts/` (if `_wf_skips.json` / `_wf_audit_done.json` can't be found anywhere,
   proceed — they only save re-attempts; `_wf_apply.py` re-verifies everything anyway).
2. Re-run the audit harness (§1) → `scripts/_fill2020_targets.json`: per company → field → [qe list],
   quarters 20191231..latest-completed. Partition into A/B/C/D per §2 rules (A-test: every missing cell is
   revC/patC AND patC==patS exactly wherever both are stored in the window AND no con-flagged sf_revop row
   exists → candidate no-sub; anything ambiguous falls to C).
3. Commit the target file + any harness scripts to the worktree, push (they're the campaign's shared state).

### Phase 1 — Systemic passes first (biggest cells-per-effort)
1. **Jun-2022 re-walk** (C-i above). Success = revS empty for 20220630 drops from 56 to ≤ handful.
2. **Insurers** (B): follow the playbook per insurer; anchor on stored PAT; fill via `revop_fundamentals.json`.
3. **Annual/9M derivation sweep**: any remaining cell where the other 3 quarters + FY (or 9M) are stored →
   derive the missing quarter (SCREENER-ANNUAL-DERIVATION rule), anchor = the reconciliation itself.

### Phase 2 — No-sub identity pass (A) — ⚠️ GATED
1. For each A-candidate, PROVE no-sub for the window: patC==patS exact in ≥6 overlap quarters (and no
   con-divergent filing found). Build the list: company → quarters → cells (revC=revS copy; opC/ebitC same
   iff their std twin is stored; never touch non-null).
2. **STOP and show the user**: company count, cell count, 10 sample rows. One go/no-go for the whole batch.
   (Same SEBI LODR Reg-33 identity §6(A) already applied to their PAT — but ~1,200 derived cells deserve
   one explicit yes.)
3. On yes: apply fill-only via the ledger, journal every cell as `no-sub-identity`, push, verify origin.

### Phase 3 — Per-company fetch grind (C residue)
§6 loop verbatim, extended to REV: before the first chunk, check `_wf_gen.py`/`_wf_apply.py` actually
carry rev columns (they were built PAT-first); if not, extend prompts + apply-verification to
[revS,revC,opS,opC] and COMMIT the script edits first (rule 9). Then: `_wf_regap.py`-style regen →
chunks of ≤8 → `_wf_apply.py` dry → review flags → `--apply` → push → repeat until the target list is
empty or every survivor is skip-journaled with reason + attempted sources.

### Phase 4 — PAT residue (D)
Per §2-D: existence check → fill or document-unfillable. Timebox IOB to one honest attempt.

### Phase 5 — Rebuild, ship, verify (runbook §39 gate)
1. `revop_sanity.py`; rebuild `build_quarterly_results.py` + `build_results_season.py` (§11 order).
2. Bump the sf cache rev + service-worker (memory: project-stocks-sf-cache-key-rev) so warm browsers
   don't serve stale fundamentals.
3. Push everything file-scoped from the worktree; ~20 min later re-verify LIVE (origin + through the
   client: open quarterly-results page for 3 healed names incl. one insurer, one no-sub bank, one Jun-2022
   fill — feedback-verify-users-see-it).
4. Re-run the audit → post the final before/after std/con tables. DONE = every quarter Dec-2019→Mar-2026
   shows 0 empty OR a ledgered unfillable reason per remaining cell; Jun-2026 = announced-but-unparsed
   swept, rest explicitly left to the cron with a count.
5. Update memory (campaign note → outcome) + runbook §5 pending queue (remove items this closes; add any
   new dead-ends). Remove the worktree.

### Phase E (anytime alongside) — Jun-2026 announced-but-unparsed
`check_season_coverage.py` / `docs/_season_coverage.json` lists declared-vs-parsed gaps for the live
quarter — sweep only those (they're pipeline misses, not unfiled results). Do not chase unfiled companies.

## 4. Kickoff prompts (paste into a fresh Sonnet session, one at a time)

- Phase 0: `Read scripts/FILL2020_CAMPAIGN.md and DATA_RUNBOOK.md §0+§6+§38, then execute Phase 0. Stop after pushing _fill2020_targets.json and report the A/B/C/D partition counts.`
- Phase 1: `Read scripts/FILL2020_CAMPAIGN.md; execute Phase 1 (Jun-2022 re-walk, insurers, derivations). Report cells closed per pass and re-run the audit scoreboard.`
- Phase 2: `Read scripts/FILL2020_CAMPAIGN.md; execute Phase 2 up to the gate, show me the no-sub list and wait for my yes.`
- Phase 3: `Read scripts/FILL2020_CAMPAIGN.md; execute Phase 3, one chunk at a time. Report after each chunk's apply+push.`
- Phase 4+5: `Read scripts/FILL2020_CAMPAIGN.md; execute Phases 4 and 5 and post the final before/after tables.`
