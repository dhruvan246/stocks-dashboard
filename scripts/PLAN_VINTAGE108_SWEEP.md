# §108 RESTATED-COMPARATIVE VINTAGE SWEEP — FY16-FY17 standalone PAT

Standing rule for every line below: **no assumptions, no guesswork.** Every number here was
measured this session or read out of a file in this repo; anything unmeasured says so.

Worktree: `~/stocks-wt/vintage108` (detached off `origin/main` @ `e2917ec78`, CLAUDE.md rule 3).

## The class (runbook §108)

A stored quarter holds the LATER-vintage restated figure — the Ind-AS transition comparative that
the company first published a year afterwards — while its ann date says the value was public when
the quarter was originally filed. Every value is real, so no scale gate, FY identity or
aggregator comparison that mixes vintages can see it. It is a **look-ahead**: on the stored ann
date the public number was a different one. Store convention is AS-ORIGINALLY-FILED (§42).

## Window and candidate set (measured)

`qe ∈ {20150630, 20150930, 20151231, 20160331, 20160630, 20160930, 20161231, 20170331}`,
every stored `npStd` cell:

| | count |
|---|---|
| stored npStd cells in window | **6,159** |
| distinct symbols | **1,082** |
| symbols with no BSE scrip code before this sweep | 148 (663 cells) |
| of those, resolved here by ISIN | **134** (`vintage108_resolve_extra.py`) |

## Routes, in the order the ladder demands

1. **BSE detres** (`vintage108_sweep.py`) — §42's `Corp_detailedResult_Transpose_ng`, as-filed by
   construction, ₹mn ÷10, Date Begin/End verified == the quarter. Flag `|detres − stored| >
   max(2 cr, 3%)`. Throttled to one request per 2 s start-to-start; resumable ledger
   `_vintage108_scan.json`, raw rows kept in `_vintage108_raw.json` for offline re-match.
2. **NSE archive dual-vintage test** (`vintage108_nse_vintages.py`) — **the decisive one, found
   this session.** See below.
3. **Pass 2 evidence** (`vintage108_adjudicate.py`) — audited as-filed annual (detres `NN.50`),
   stored-vs-detres FY reconciliation errors, MC deep std feed as an independent reader,
   exceptional-item and mis-slot checks.
4. **Documents** (`vintage108_documents.py`) — original and year-later filing PDFs. Kept for the
   residue only: the FY16-FY17 BSE attachments are overwhelmingly **scanned** (SYNGENE's two
   2016 filings: 15 pages / 14 characters of text layer), this machine has no tesseract, and
   vision reads are last-rung and need explicit permission (memory:
   feedback-vision-reads-last-ask-first).
5. **Anchor refusals already on disk** (`vintage108_mine_skips.py`) — §108 signature (1). 65
   `pat-anchor X vs stored Y` refusals over 39 symbols sit in the repo's skip ledgers inside this
   window (39 std / 26 con). A refusal IS a finding.

## ★ THE FIND: NSE's archive keeps BOTH vintages of a quarter

NSE's `corporates-financial-results` list returns **more than one row for the same (period,
basis)** when the company re-filed that period — the original and, a year later, the Ind-AS
restatement, each with its own `filingDate`, `indAs` flag and its own detail page.

    SYNGENE Dec-2015 std   filed 29-Apr-2016  Non-Ind-AS  ->  58.80 cr   (the healed value)
    SYNGENE Dec-2015 std   filed 27-Jan-2017  Ind-AS      ->  66.70 cr   (what the store held)

    BAJAJ-AUTO Jun-2015    filed 24-Jul-2015  Non-Ind-AS  -> 1014.80 cr
    BAJAJ-AUTO Jun-2015    filed 04-Oct-2016  Ind-AS      ->  957.36 cr  (what the store holds)

So the restated comparative — the thing §108 said only the year-later filing carries — is
available as **structured data with its filing date attached**: no PDF, no OCR, no vision rung.

**The rule:** for one (period, basis) the AS-FILED vintage is the row with the **earliest
filingDate**. Any later-filed row is a restatement. `indAs` usually labels the transition but is
not the discriminator — a restatement can happen inside one standard — the date is.

**Why this matters beyond convenience:** it removes the sweep's dependence on detres's reach.
detres has no row at all for a pre-listing or thinly-filed quarter — SYNGENE Jun-2015, one of the
four defective cells of the known case, is exactly that — so pass 1 could never have flagged it,
and a restated value that lands within 3% of the as-filed one never flags either. The NSE test
runs over the whole 6,159-cell window from the store, not from pass 1's output.

### Verdicts it emits
| verdict | meaning |
|---|---|
| `vintage-confirmed` | stored == a LATER-filed vintage, != the earliest → §108, proven |
| `store-as-filed` | stored == the earliest-filed vintage → the store is right |
| `stored-in-neither` | stored matches no vintage NSE holds → a bad read, not a vintage swap |
| `single-vintage*` | NSE holds one filing of the period → this test cannot speak |
| `no-nse-row` / `list-failed` | measured absence of the ROUTE, never a claim about the data |

Matching is **nearest-vintage**, not fixed-epsilon: cells are stored at 2 dp from feeds that round
differently (ABFRL: as-filed −73.09, restated −67.88, stored −68.00 — unmistakable, yet outside any
epsilon tight enough to mean anything). The nearest vintage must be within max(0.35, 0.5%) AND at
least 4× closer than the runner-up.

## Positive controls (run, not assumed)

* **Detector.** Against SYNGENE's PRE-heal values, taken from this repo's own `fund_cell_fix.json`
  `was` fields: Sep-15 48.5 vs detres 52.29 → FLAG; Dec-15 66.7 vs 58.8 → FLAG; Mar-16 79.1 vs
  66.5 → FLAG; **Jun-15 46.6 → NOT flagged, detres has no row for that quarter.** 3 of 4. That
  missing fourth is the measured bound on pass 1's recall, and is exactly what route 2 exists for.
* **Heal.** Post-heal SYNGENE now reads `store-as-filed` on Sep-15 / Dec-15 / Mar-16 with the
  restated values (48.5 / 66.7 / 79.1) visible as the later vintages — an independent
  confirmation of the 2026-08-24 heal from a source that heal never used.

## Two reader defects found and worked around (not patched under other callers)

* `fetch_insurers.is_result_filing` applies its NEWSSUB veto ("press release", "intimation", …)
  BEFORE the results hit, so BSE's own wording *"Financial Results with Results Press Release &
  Limited Review Report"* (SYNGENE 2016-01-21) is vetoed — and the row that survives for that date
  carries no attachment, so the quarter reads as "no filing". `vintage108_documents.py` filters
  locally instead of widening a helper ~10 callers share.
* A substring test for a printed number matched "67" and "59" anywhere on a page of figures.
  The number test now requires ≥3 significant digits and token boundaries.

## Results

Filled in by `vintage108_report.py` as the passes land — see the RESULTS section appended below.

---

# ★ THE ROOT CAUSE — one backfill pass with no vintage rule  (found 2026-08-24, mid-sweep)

`vision_rev_fills.json` records the document each filled value came from. For the 2026-07-27
"vision-manual band3/4" pass that document is an NSE archive page cited by filename — and the id
in the filename is NSE's `seqNumber`. Cross-check those ids against the archive list:

    INFY|20150930        src financial_res_INFY_1014609.html
                         seq 1002074  filed 2015-10-13  Non-Ind-AS   pat 6306.0   <- as filed
                         seq 1014609  filed 2016-12-22  Ind-AS       pat 3248.0   <- what it read
    BAJAJ-AUTO|20150630  src ..._1014247.html = the Ind-AS row filed 2016-10-04   (957.36)
    ABB|20160630         src ..._1027851.html = the Ind-AS New row filed 2017-08-04 (55.64)

§108 is therefore not a scatter of unlucky cells. It is ONE pass that had no rule for choosing
between two filings of the same period, so the store carries whichever row NSE happened to list.

**Measured population (`vintage108_provenance.py`, all 5,059 window fill rows checked):**

| provenance verdict | rows |
|---|---|
| `single-row` — NSE holds one filing of the period, nothing to choose | 2,981 |
| `src-is-earliest` — the pass read the as-filed page ✔ | 1,752 |
| **`src-is-later-vintage` — the pass read the RESTATEMENT** | **326** (239 std, 87 con) |

Why this route beats every screen §108 proposed:
* **exact enumeration** — no detection tolerance to calibrate, no recall to estimate;
* it reaches the **consolidated basis**, which detres cannot serve at all (§42), so the PAT sweep
  §108 specified could never have seen 87 of these;
* it catches a wrong-vintage **revenue or operating-profit** read even where the quarter's PAT is
  identical across vintages — invisible to every PAT screen there is.

## The heal gate

A slot is proposed only when the stored value is measurably a LATER vintage **and** at least one
independent line of evidence says so:

* **PROV** — our own provenance record names the later-vintage page as the source of the fill, and
  the stored value is that page's value. A proof of the READ, not an inference from two numbers
  agreeing, and the only gate that reaches consolidated.
* **DETRES** — BSE's detailed-results JSON, an independent as-filed reader, agrees with NSE's
  earliest-filed vintage (standalone only).

Value-match alone is never enough: two vintages of a steady filer can differ by less than the
noise between feeds, and a coincidence is not a finding.

Two guards that the first cut of this got wrong, both caught by reading the output:
* **Clean-first.** Asking "which later vintage is this?" of a slot that already holds the earliest
  one, then queueing the failure to answer, turned ~450 healthy slots into a fake backlog.
* **The 180-day gap.** A company can re-file a quarter days later to correct it; that is a
  correction, not a restatement, and the earliest row is then the wrong target. Only a re-filing
  more than 180 days after our ann date is treated as the later vintage.

## The Ind-AS "New" layout prints no operating-profit line

Under Ind-AS the P&L has no "profit from operations before other income" subtotal, so `op` is
unreadable on ~350 restated pages. Where provenance names that page as the source, the value the
pass recorded in `vision_rev_fills` IS the restated one — used instead of re-deriving a subtotal
the document never printed.

---

# RESULTS — landed and live-verified 2026-08-24 (commit `15954aadc`, runbook §109)

| route | coverage | result |
|---|---|---|
| BSE detres (§108's own recipe) | 2,637 of 6,159 cells | 125 flags |
| NSE dual-vintage, standalone | **6,159 (all)** | **281 vintage-confirmed** |
| NSE dual-vintage, consolidated | **2,638 (all)** | **80 vintage-confirmed** |
| provenance (§109b) | 5,059 fill rows | **326 later-vintage** (239 std, 87 con) |

**Healed:** 257 cells in `fund_cell_fix.json` (220 std, 37 con) over 97 symbols, and **1,231 slots
in `revop_cell_fix.json`** (518 rev_std, 413 op_std, 195 pat_std, 37 pat_con, 35 rev_con, 33
op_con) — a ROW defect, not a cell defect. Evidence split: DETRES 109, PROV+DETRES 43, PROV 105.
321 reads entries replay-proofed (`vision_rev_fills.json` 316, `_nsearch_reads.json` 5).

**The gate hole the invariant caught.** The first cut said "at least one line of evidence", which
let a PROV-only cell be healed towards a value detres rejects. Agreement with detres rose
54.7% → 97.5%, and all four cells left disagreeing were that hole — FSL Sep-2015 would have been
divided by ten. The rule is now **evidence AND no available reader may contradict**; re-run, and:

| agreement with BSE detres | before | after |
|---|---|---|
| std PAT (152 healed cells with a detres reading) | 53.9% | **100%** |
| std revenue (463 healed cells) | 33.0% | **100%** |

**Live verification.** All 257 fund cells and 1,231 revop slots present and correct in the served
`sf_fundamentals.json` / `sf_revop.json` with a cache-buster, re-checked after a further CI cycle
(0 missing, 0 drift both times), and the per-stock `fin/<SLUG>.json` slices — the layer
`stock.html` actually reads — carry them too (§41b's dispatch worked without hand-kicking).

**Rebase note.** A CI refresh and another session's heal landed on `docs/sf_*.json` mid-run. Per
the minified-JSON rule these were NOT merged: the branch was reset onto the fresh `origin/main` and
the appliers RE-RUN, which also proved the ledgers are the durable artifact (the count moved
260 → 257 because the other session had already fixed three of them, reported as already-correct).

## Open — the next campaign, not this one

`no-nse-row` 362 std / 355 con (pre-listing quarters and BSE-only filers), `no-readable-vintage`
270 / 42, and the queues in `_vintage108_proposals.json` (`restatement-gap-too-small` ≤180 days are
corrections, not restatements; `*-no-independent-evidence`, mostly consolidated rev/op where detres
cannot speak and provenance did not fire).

**By-products — real findings, deliberately NOT healed here:**

| class | cells |
|---|---|
| the std slot holds the CON value (§59) | 313 |
| two as-filed readers agree against the store | 182 |
| scale-step (§74) | 11 |
| the two readers disagree with each other | 9 |

Plus 65 `pat-anchor X vs stored Y` refusals already on disk in this window (39 symbols) — §108
signature (1); 8 of those symbols were in this heal.
