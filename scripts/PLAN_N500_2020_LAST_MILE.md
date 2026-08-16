# PLAN — Nifty 500 coverage 2020→date, THE LAST MILE to 100%

**Written 2026-08-16 ~18:20 IST** against live payload `2026-08-16 17:48 IST` (verified byte-identical
to origin `docs/coverage/nifty-500.json`, commit 0926a9b0). Executor: the next interactive session
("Opus"). Author: this session, after exhausting the NSE-XBRL route (measured: 0 fills from the full
453-pair residue queue — every remaining cell needs a DIFFERENT route than the one that got us here).

---

## 0. RULES — read before touching anything (violations here have shipped bugs before)

1. **NO ASSUMPTIONS, NO GUESSWORK** (user order 2026-08-10, DATA_RUNBOOK §0). Every value written
   and every claim ("exists", "absent", "fixed", "live") must trace to something measured THIS
   session. Can't measure it → say "unknown". A plausible guess presented as fact is worse than an
   admitted gap.
2. **Read first**: `scripts/DATA_RUNBOOK.md` (procedures override improvisation),
   `scripts/N500_COVERAGE_100_CAMPAIGN.md` (execution contract), memory
   `project-stocks-n500-coverage-2020-residue` + `project-stocks-coverage-fill-tooling`.
3. **Concurrency (CLAUDE.md)**: work in THIS worktree (`~/stocks-wt/n500-cov`), never the main
   checkout. Own files only; explicit-path `git add`; push via the fetch-rebase-push loop.
   **`fiiChgPp`/`diiChgPp` DATA belongs to the SHP session** — its side completed 2026-08-16
   18:07 (commits 51825723 fill + 9058e1a5 precision-only); what remains for THIS campaign is
   builder-side only (§ C7). Do not touch shp_engine.json.
4. **Anchor before write, always** (this campaign's tooling caught two of its own parser bugs only
   because of it): PAT anchor within 2% for XBRL reads; screener `validate()` on neighbouring
   quarters; double-anchor (current + preceding) for any comparative-column read. No anchor → no
   write → refusal logged with its own named counter (a refusal class without a counter vanishes).
5. **Heal via ledgers** (CLAUDE.md rule 5): sf_revop/sf_fundamentals edits go through
   `revop_fundamentals.json` / the fill ledgers (`nse_xbrl_rev_fills.json`,
   `screener_*_fills.json`, `insurer_con_rev_fills.json`, …) so nightly CI rebuilds don't clobber
   them. N/A verdicts go in `scripts/coverage_na_ledger.json` (MERGE semantics, per-name evidence,
   date-bounds via `from`/`to`) — never hardcode in the builder.
6. **One fill agent at a time** (feedback-backfill-one-agent-at-a-time). Provenance on every cell.
7. **Vision/render reads are the LAST rung and need user ask-first** (feedback-vision-reads-last).
8. **Ship gate** (DATA_RUNBOOK §39): syntax-check every touched file; a dry-run whose counters are
   all zero means the loop never ran (assert nonzero denominator); after pushing data, dispatch the
   coverage bake, then **dispatch `pages.yml`** — CI's payload commit CANNOT deploy itself — and
   verify LIVE with a `?cb=` cache-buster; re-verify ~20 min later (an in-flight CI run may race you).
9. **Parity is per-(param,date)**, never totals-only. Before filling from any queue, re-derive it
   and assert composition equality against the live payload. The reproduction recipe that passes
   all four params today is § A below. `--check` compares ACROSS bakes (local payloads get
   reverted) — detect skew first, don't "fix" a correct queue.
10. **The page counts through the ENGINE** (§92): `build_coverage_matrix.js --explain <slug>` names
    the exact symbols behind every sub-100 cell from the same vm scan that writes the payload. When
    this plan's numbers and a fresh --explain disagree, the --explain is right — re-derive.
11. **Traps already hit once** (do not re-hit): NSE `xbrl` field can be the literal `-` **or a URL
    ending `/-`** — 76 "transient fetch failures" were that ONE dead URL fetched 76 times; any
    consumer of the field must gate on the URL's BASENAME, not the bare string (give the class its
    own counter too, or it hides inside fetch_fail again); FourD is NOT "the other basis"
    in single-basis filings; NSE list date-range queries silently return 0; `rows:0` for a name that
    certainly files is a DIAGNOSIS not absence (§57a); backticks inside the vm template kill the
    builder; sw.js is at v96 → next asset change bumps v97; never hardcode param counts (61 today,
    grew 43→61 in one day).

---

## A. Re-derive the residue (do this FIRST, numbers below go stale with every fill)

The parity-clean reproduction lives in this session's scratch as `residue2.py`; its logic, verbatim:

* revenue family (`op`, `ebit`): symbol's visible quarter = last `min(annStd, annCon) ≤ date`
  (annOk-gated: ann>0 AND ann ≥ firstBar); missing iff BOTH std+con slots null; then subtract
  `naLedgerHit(param,sym,iso)` hits and `firstRealAnn > date` rows (§99).
* basis family (`revCon`, `revStd`): each basis resolves its OWN visible quarter from its OWN
  announce date; **no N/A at all by design**; a member-date with NO visible filing on that basis
  still counts MISSING.
* Gate: per-(param,date) equality with the live payload on all four params, else STOP.

Queue artifacts committed beside this plan (parity-clean at payload 17:48, **453 pairs**):
`scripts/n500_last_mile_queue.json` (fillable pairs), `scripts/n500_last_mile_nofiling.json`
(30 member-dates with no filing to read), `scripts/_revop_fill_refusals.json` (the 487-row refusal
census that proves the XBRL route is dry).

## B. Live residue, measured 2026-08-16 18:00 IST — 9,176 member-dates over 22 params

| block | member-dates | route | § |
|---|---|---|---|
| patcon family (profitTTMCon 2041 + compositeCon 2041 + accelCon 622 + yoy/base/streakCon 368×3) | 6,136 | measure→decide→fill/N/A | D |
| patstd family (TTMStd/compositeStd 570×2 + accelStd 141 + yoy/base/streakStd 56×3) | 1,449 | same sweep as D | D |
| ebit | 872 | split below | C |
| revCon | 359 | split below | C |
| op | 292 | split below | C |
| fii/diiChgPp | 112+112 | SHP side DONE (51825723) — wire the pre-listing predicate | C7 |
| revStd | 112 | split below | C |
| patCon+postDriftCon 29+29, patStd+postDriftStd 1+1 | 60 | IOB pre-con era + NSLNISP | C5/C6 |

## C. The revenue+basis residue, class by class (member-dates; from § A run at 17:48)

### C1. Insurers — ebit 632 · revCon 68 · op 16 · revStd 9
NSE's equities list serves ALL insurers zero rows on EVERY index variant (re-measured today:
equities/debt/sme/municipalBond/invitsreits × SBILIFE/GICRE/ICICIGI — all empty). The XBRL route
does not exist for them. Routes that DO exist:
* **op / revCon / revStd**: IRDAI-format numbers ARE in their BSE filings (`fetch_ins.py`,
  `INSURER_EXTRACTION_PLAYBOOK.md`, `bse_ins_crop.py` — the PAT automation already reads these
  PDFs nightly). The Policyholders' Revenue A/c carries premium (rev) and "Operating Profit
  transferred to P&L" (op). Screener also carries OP for all 12 insurer names (measured 2026-08-16,
  memory §"Banks AND insurers was WRONG") — usable as the anchor's second reader, WITH `validate()`.
* **ebit 632**: **BLOCKED ON USER DECISION** — see § E1. Quarterly IRDAI format has no
  Depreciation line (screener's Dep=0 there is a NOT-DISCLOSED sentinel — memory
  feedback-…; GICRE/HDFCLIFE/ICICIPRULI/LICI/NIACL were explicitly REFUSED on this earlier).
  Do NOT derive ebit = OP − 0. If the user picks N/A: `coverage_na_ledger.json` entries per name
  with the IRDAI-format evidence line, class C1, then rebake. If the user picks "check per name
  first": read one recent BSE filing per insurer for an expense schedule carrying depreciation
  (STARHEALTH/GODIGIT/NIVABUPA file newer formats — unknown until read; say unknown until then).

### C2. ABBOTINDIA — ebit 68 · op 68 (23 quarters 20190930→20260331)
NSE list truncated to ONE row (a 2010 filing) on every variant — re-measured today, a real wall.
But it is an ordinary industrial and screener carries the full layout. Measured today: std
quarterly table serves 13 quarters 2023-06→2026-06 with OP, Dep, NP all present (e.g. 2026-03:
OP 481, Dep 19, NP 395).
* Tail quarters (2023-12→2026-03, 7 of the 23): **screener-parity route** — the exact tool pattern
  that landed 23 ANGELONE-class cells yesterday (commit 4196af0c): `validate()` against our stored
  neighbours (stored op exists at 20230930/20250331/20250630/20250930 — anchors are there), then
  op = OP, ebit = OP − Dep, write both bases only if screener shows one basis (ABBOTINDIA con
  table is EMPTY — std-only filer; con=std per its own filings, but verify a recent PDF states
  no-subsidiaries before mirroring, else fill std only).
* 2019-09→2023-06 (16 quarters): screener quarterly does NOT reach; use **screener ANNUAL + the
  3 stored quarters → 4th-quarter subtraction** (memory feedback-screener-annual-derivation,
  ledger `screener_annual_derived_2019.json` shows the pattern) where 3-of-4 exist, else **BSE
  announcement PDF §58 geometric read** with stored-PAT anchors.

### C3. WESTLIFE ebit 35 op 26 revStd 33 · SPICEJET ebit 32 op 26 revStd 13
Both file BOTH bases every quarter (WESTLIFE: 35/35 con+std anns; measured today). These are
extraction misses, not absence. SPICEJET is the proven non-bank in the never-has-ebit set — its op
IS a real gap too (4 of 22 quarters hold op). Route: NSE list has no rows pre-2023 for them →
**BSE announcement PDF §58 geometric** with PAT anchors; screener-parity for the recent tail.
WESTLIFE revStd: sf_revop std slots null while con full — likely the extractor stored con-only
(two-files-one-quantity class); check `revop_fundamentals.json` BEFORE fetching anything — the
number may already be in the other file.

### C4. "other" — ebit 105 (34 names) · op 156 · revCon 243 · revStd 56
The grind bucket: mostly 1-3 quarters per name. The XBRL run's refusal census says why each one
refused: 77 `basis_absent` (filing parsed clean but carries no context for the needed basis —
mostly banks whose con is in a SEPARATE filing the list doesn't link), 18 hard 404s on
`nsearchives` BANKING_*.xml (KARURVYSYA×4, CUB×2, MAHABANK×2, RBLBANK, IDFCFIRSTB, CSBBANK,
DCBBANK, EQUITASBNK, TMB, UJJIVANSFB — the URL is in the refusals file), 30 placeholder-URL rows.
Routes in ladder order (§57): (1) BSE mirror of the SAME filing — banks file both exchanges;
`bse_fetch.py` + scrip from `bse_scrips.json`; (2) announcement PDF §58 geometric read; (3)
screener-parity with validate(). Bank `ebit` rows in this bucket: **check
`coverage_na_ledger.json` FIRST** — a lender name here means the N/A ledger missed it; adjudicate
per-name against screener layout (the 23-of-54 non-lender finding says NEVER assume from the NSE
flag), don't fill PPOP-form ebit (strip_lender_ebit guards the write anyway).

### C5. IOB revCon 28 — its first con filing EVER is 20220331 (measured today: 18 con anns,
all ≥2022-03; std back to 2001). The 28 member-dates are 2020-01→2022-02: **no consolidated
number existed to hold**. Fillable by nothing. Two honest endings, user's call (§ E2): a
date-bounded basis-family N/A ("no con result existed before first con filing", `to: 2022-03`)
— requires WIRING N/A into the basis family which is currently no-N/A **by design** — or leave
visible as a permanent 28. `fill_con_identity.py`'s E-gates are for "index shows no con FILED for
that quarter while std was" — different claim, does not cover "never filed con at all yet"; do not
stretch it.

### C6. NSLNISP revStd 1 + patStd 1 + postDriftStd 1 (all 2023-04-28)
One untried rung stands: the demerger listing **Information Memorandum** (first exchange filing is
2023-05-23, after the screen date — the IM predates listing). If the IM carries no quarterly P&L
for the needed quarter, the §99 first-real-filing N/A already covers the profit side; the revStd
cell then joins § E2's decision.

### C7. fii/diiChgPp 112+112 — SHP session's verdict, delivered 2026-08-16 18:07 IST
Their classification of the 174-cell residue (message + commit 51825723, verified on origin):
**62 late-filed-prior N/A (already shipped) + 68 pre-listing + 11 filled** (shp_engine cell total
88,767 → 88,778; commit 9058e1a5 is precision-only and must NOT move per-param counts — if a bake
moves them, that is their bug, report it). The 68 are the 2021 IPO cohort (LODHA, GOCOLORS,
LATENTVIEW, MAPMYINDIA, MEDPLUS, METROBRAND, SUMICHEM …): the needed calendar-PRIOR quarter
predates the symbol's first shp_engine row — no shareholding pattern existed to fetch. Coverage-side
work: wire their predicate into `build_coverage_matrix.js` beside the late-filed-prior rule —
**current row is a QE row AND calendar-prev row ABSENT AND earliest shp_engine row for the symbol
is LATER than that prev quarter → N/A** — readable straight off shp_engine, no ledger needed.
Verify the 11 fills land (rebake drops chgPp missing) and the predicate closes the rest to 0.

## D. The pat-con/std families — 7,585 member-dates, the actual bulk

**Measure before deciding anything**: the current `n500_cov_explain.json` predates these params.
Run in THIS worktree (bin is present, 17.5 MB):
```
node scripts/build_coverage_matrix.js --explain nifty-500 --explain-from 2020-01-01
python3 scripts/n500_cov_cells.py build --explain scripts/n500_cov_explain.json \
    --out scripts/n500_patfam_queue.json
```
then class the queue rows per name: (a) reach-back into pre-2020 quarters our files hold nothing
for; (b) §51a never-filed-con eras (pre-2020 con emptiness is largely NOT fillable — memory);
(c) genuinely extractable con/std PAT (the pre-2020 con wall queue of 414 and the un-wired
std-anchor route — memories project-stocks-pre2020-conpat-wall / pre2020-stdanchor-route — overlap
here; check them before new tooling). These families are RAW/no-N/A **by peer-session design** —
changing that is § E3, the user's call, not the executor's.

## E. USER DECISIONS the executor must have before the semantic edits (ask FIRST, in one round)

All three were put to the user 2026-08-16 ~18:15 IST and ANSWERED — the executor asks nothing,
these are settled:

* **E1 — insurer quarterly ebit (632 cells)** → **"Check per name first."** Read one recent BSE
  filing per insurer (all 11 names: LICI SBILIFE HDFCLIFE ICICIPRULI ICICIGI GICRE NIACL
  STARHEALTH GODIGIT NIVABUPA CANHLIFE) for any expense schedule carrying depreciation. A name
  whose filing PROVES non-disclosure gets the N/A entry with that evidence line; a name whose
  filing carries depreciation gets FILLED (ebit = OP − Dep, PAT-anchored). Cells stay visible
  until their name's verdict is recorded. No category shortcut — per-name, both readers logged.
* **E2 — basis-family N/A** → **"Keep no-N/A design."** revCon/revStd keep their strict meaning.
  IOB's 28 pre-first-con cells and NSLNISP's 1 are USER-ACCEPTED permanent visible gaps — the
  definition-of-done enumerates them as signed-off; do NOT wire N/A, do NOT re-raise.
* **E3 — patcon/patstd families** → **"Decide after measuring."** Run the § D measurement, class
  the composition per name (never-filed vs reach-back vs extractable), present it to the user
  with counts, and STOP for their call before any semantic change. Fills that need no semantic
  change (genuinely extractable con/std PAT) may proceed under the normal gates meanwhile.

## F. Execution order (each step ends with the § 0.8 ship gate)

1. § A re-derive + parity. Then C7 (predicate + rebake — biggest single-step drop available) and
   C2+C3 screener-parity tail fills (pure mechanical, anchored).
2. C4 BSE-mirror/PDF sweep for the 18 404s + basis_absent banks; C3 PDF reads.
3. C1 insurer op/revCon/revStd via the BSE insurer route (PAT automation's own PDFs).
4. E-decisions from the user → C1-ebit / C5 / C6 endings; N/A ledger entries with evidence.
5. D measure → class → present composition → user's E3 call → wire or fill accordingly.
6. C2 annual-derivation for ABBOTINDIA's 16 old quarters.
7. Final rebake, pages dispatch, LIVE parity re-run, campaign doc close-out, memory update.

**Definition of done**: every one of the 61 params reads exactly 100.000% on the live page for
window 2020-01-31→dataEnd, with every excluded cell carrying a per-name evidence line in
`coverage_na_ledger.json` — EXCEPT the cells the user has already signed off as permanent visible
(E2: IOB revCon 28, NSLNISP revStd 1 — revCon's ceiling is 99.93% and revStd's 99.997% by the
user's own decision), plus whatever E3's post-measurement call adds to either list.
