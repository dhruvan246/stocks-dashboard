# REVPAT verify — p5 arbitration: 7 insurer/JUBLPHARMA cells

Method: DATA_RUNBOOK §57 route ladder + §58 column-anchor read. Every PDF fetched LIVE from
bseindia.com this session (≥2s spacing, no 403/429). Rungs 1–2 (BSE detres, NSE XBRL) walked via
a copy of `exchange_fetch.py` before falling to rung 3 for the insurers, exactly per the ladder.
Full evidence: `insurer_verdicts.json` in this directory. **No repo or reference-tree writes.**

## Verdicts

| # | symbol | quarter | field | OURS | Screener | Groww | Tickertape | FILED VALUE | verdict |
|---|---|---|---|---|---|---|---|---|---|
| 1 | NIVABUPA | 2025-06-30 | std PAT | -91.44 | 71.00 | 71.44 | 39.00 | **-91.44** | **OURS_CONFIRMED** |
| 2 | NIVABUPA | 2025-06-30 | std revenue | 1371.08 | 1932.00 | 1931.75 | — | **1371.08** | **OURS_CONFIRMED** |
| 3 | NIVABUPA | 2026-03-31 | std PAT | 345.13 | 159.00 | 159.36 | 86.00 | **345.13** | **OURS_CONFIRMED** |
| 4 | NIVABUPA | 2026-03-31 | std revenue | 2138.56 | 2251.00 | 2251.06 | — | **2138.56** | **OURS_CONFIRMED** |
| 5 | STARHEALTH | 2025-06-30 | std PAT | 262.52 | 438.00 | 438.18 | 745.00 | **262.52** | **OURS_CONFIRMED** |
| 6 | STARHEALTH | 2025-06-30 | std revenue | 4232.86 | 4880.00 | 4880.40 | — | **4232.86** | **OURS_CONFIRMED** |
| 7 | JUBLPHARMA | 2025-03-31 | std revenue | 217.30 | 61.00 | 60.70 | — | **217.30** | **OURS_CONFIRMED** |

**All seven cells resolve OURS_CONFIRMED.** None are UNRESOLVED, OURS_WRONG, BOTH_WRONG, DEF_DIFF,
or AMBIGUOUS_CONCEPT — the values are all exactly reproducible from the primary filing.

---

## The GICRE fingerprint (tested first, per the brief) — does NOT hold for any cell

The GICRE precedent: `stored_std == that quarter's CONSOLIDATED pre-associate PAT row` (the wrong
row copied from the con statement into the std slot). Tested by searching the full text of every
fetched PDF for the substring `consolidat`:

| filing | pages | 'consolidat*' hits |
|---|---|---|
| NIVABUPA Jun-2025 | 10 | **0** |
| NIVABUPA Mar-2026 | 24 | **0** |
| STARHEALTH Jun-2025 | 13 | **0** |

Both insurers have **no subsidiaries** and file **no consolidated statement at all** — confirmed
directly, not just inferred from `fetch_insurers.py`'s `sub: False` config. The GICRE defect
mechanism is mechanically impossible here: there is no second (consolidated) PAT figure in either
filing that could have been miscopied into the standalone slot. The fingerprint does not explain
cells 1, 3, or 5; something else does (see below).

## What actually explains it: the sites disagree with EACH OTHER, and the filing settles it cleanly

For every PAT cell, Screener/Groww/Tickertape do not agree among themselves (e.g. cell 1:
71.00 / 71.44 / 39.00) — under the task's own rule, that alone means the sites decide nothing.
Every cell was independently, triple-anchored against the primary filing:

* **Two column anchors** — the SAME row's preceding-quarter and year-ago columns reproduce our
  *already-stored, undisputed* neighbouring-quarter values to the paisa, in every one of the six
  insurer cells (12 anchor points total, all exact).
* **A second, independent arithmetic check** — PBT − Tax = PAT on the target column (exact for
  cells 3, 5, and trivially for cell 1 where tax is nil), AND a fiscal-year quarter-sum identity
  (our four stored quarters of the FY summing to the filing's own printed FY total) for every PAT
  cell — exact in 5 of 6 legs, within ₹0.01cr (rounding) in the sixth (STARHEALTH FY25: 645.87 vs
  printed 645.86).

That is about as strong a confirmation as this method produces: two filings (NIVABUPA's Jun-2025
and Mar-2026 packs) even cross-confirm each other's comparator columns (Mar-2025 PAT = 206.08cr
read identically off two different filings, three months apart).

**STARHEALTH's text layer is OCR-corrupted**, exactly as DATA_RUNBOOK §3 documents (`'lncome'`,
`'3.47.592'`, fragmented digits). Per method item 5, every STARHEALTH figure was read from a
**rendered page image**, not the raw text layer — the rendered page was a clean, fully legible
scan. NIVABUPA's Jun-2025 page (rotation=270) also had an unreliable text-extraction order for the
same reason (word order, not glyph corruption) and was likewise read from a rendered image.

## Revenue: no printed row, but a validated construction — not left ambiguous

IRDAI-format insurers print no single "Revenue from Operations" line. Per DATA_RUNBOOK §55's
already-validated convention (`general = Premium Earned (Net) + policyholders' Income from
Investments (Net) + shareholders' Income from Investments`), this session re-derived and
re-validated that exact construction against **six** independent comparator columns across the
three filings — every one reproduced an already-stored, undisputed neighbouring quarter's revenue
to the paisa (e.g. NIVABUPA Mar-2025 revenue = 1670.79cr, read identically off two different
filings). So while the concept is genuinely a construction rather than a printed row — worth
flagging, as the brief anticipated — it is a well-defined, filing-anchored, repeatedly
cross-validated one, not a guess. Verdict: **OURS_CONFIRMED**, not `AMBIGUOUS_CONCEPT`.

Screener and Groww's revenue figures could not be reverse-engineered from any combination of
printed rows tried (Gross/Net Premium, Sub Total/Total income, various sums) for either insurer —
flagged as an open curiosity in `insurer_verdicts.json`, not pursued further since it doesn't
change the verdict.

## JUBLPHARMA (not an insurer) — doubly independent confirmation

Ordinary standard-format filer, so this cell was resolved on **rung 1** (BSE detres API) directly:
machine-readable, company-submitted "Net Sales/Revenue From Operations" = 217.3cr, from the same
response whose PAT (13.6cr) also matches our stored value exactly. Rung 3 (the BSE announcement
PDF) was pulled anyway for a fully independent second source and column anchors: the SAME row's
comparator columns reproduce our stored Dec-2024 (196.0cr) and Mar-2024 (205.8cr) revenue exactly,
and the filing's own printed FY total (745.7cr) equals the sum of our four stored FY25 quarters
(166.7+165.7+196.0+217.3) to the decimal. Screener (61.00) and Groww (60.70) agree with each other
but are ~3.6x off from a value confirmed by two independent routes and three arithmetic identities;
what they're actually showing could not be identified. JUBLPHARMA (Jubilant Pharmova)'s standalone
revenue being much smaller than its consolidated figure every quarter is a real structural feature
of the holding-company entity, not evidence of a defect.

## Route ladder log (all seven cells)

| cell | rung 1 BSE detres | rung 2 NSE XBRL | rung 3 BSE announcement PDF |
|---|---|---|---|
| NIVABUPA Jun-2025 std | refused: no-date-begin-end-in-response | refused: 0 list rows | **SUCCESS** (rendered image, rotated page) |
| NIVABUPA Mar-2026 std | refused: no-date-begin-end-in-response | refused: 0 list rows | **SUCCESS** (rendered image) |
| STARHEALTH Jun-2025 std | refused: no-date-begin-end-in-response | refused: 0 list rows | **SUCCESS** (rendered image, OCR-corrupted text layer per §3) |
| JUBLPHARMA Mar-2025 std revenue | **SUCCESS** | not needed | **SUCCESS** (independent second confirmation) |

NSE returned "0 list rows" for all three insurer quarters — this is a clean, in-band refusal
(session was NOT blocked/403'd), consistent with §3's documented fact that IRDAI-format insurers
never file standard XBRL results. No 403/429 was hit against BSE at any point this session.

## What was NOT done
- No repo file, ledger, or heal was written — verdict-only deliverable per task scope.
- The exact third-party (Screener/Groww/Tickertape) definitions were not pinned down for any of
  the seven cells — noted as an open curiosity in `insurer_verdicts.json`, not pursued further
  since it does not affect any verdict.

---

## ADDENDUM (2026-08-09, same session) — Revenue cells reopened on a circularity objection

The coordinator raised a specific, correct-in-principle objection to cells 2, 4, 6 (NOT the PAT
cells): our validation had shown the §55 construction reproduces our OWN already-stored
comparator quarters — but for these insurers, those comparator quarters were themselves built
from the SAME construction (fetch_insurers.py only ever writes PAT; nothing else populates
insurer `revS`). That check proves internal self-consistency, not correctness. Precedent cited:
HUDCO 2022-06-30's stored standalone revenue turned out to be the filing's "Interest Income"
sub-line, not Total — an undetected wrong-row parse of exactly this shape. Full detail, every
hypothesis tested and why each failed: `insurer_revenue_recheck.json`.

**Five hypotheses tested, all discarded:**
1. **Gross Written Premium vs Net Earned Premium** (the coordinator's own lead, tested first). No
   printed reinsurance-ceded line equals any of the three exact gaps; GPW alone undershoots two
   cells and, for NIVABUPA Mar-2026, GPW (2879.68cr) is actually LARGER than the site figure
   (2251.06cr) — ruling this out directly for that cell.
2. **"Total Income (3 to 5)"** — undershoots all three cells, no match.
3. **Net Premium Written** (before the earned/unearned adjustment) — undershoots all three, no match.
4. **Exhaustive combinatorial search** (every sum of 1–3 printed rows on the Annexure-I page) —
   zero matches within tolerance for two of the three cells; the one near-hit for NIVABUPA
   Jun-2025 mixed an income-statement figure with a balance-sheet figure, not a coherent concept.
5. **BSE "Integrated Filing (Financial)"** (hypothesis: a generic XBRL revenue tag data vendors
   scrape uniformly). Dead end on inspection — it's the same IRDAI Annexure-I/II package
   repackaged, no generic tag — AND neither company even filed one covering these two target
   quarters, so it cannot be the source regardless.
6. Found, but ruled irrelevant: NIVABUPA's Mar-2026 filing (page 21) contains a genuine Ind AS 117
   "Statement of Profit or Loss" with an "Insurance revenue" row (FY26 = 7829.10cr) — a real,
   different accounting concept, but ANNUAL ONLY, absent from both companies' quarterly filings,
   and far too large to be the quarterly figure in dispute even divided by 4.

**The finding that actually answers the circularity objection.** Fetched Screener.in's full
quarterly "Sales" series for BOTH companies (public page, read-only) — not just the 3 disputed
cells — and compared every quarter to our stored series:

| company | quarters compared | match sites exactly (to the crore) | mismatch |
|---|---|---|---|
| NIVABUPA | 11 | **9** | 2025-06-30, 2026-03-31 (the two contested cells) |
| STARHEALTH | 12 | **11** | 2025-06-30 (the one contested cell) |

The §55 construction independently reproduces Screener's OWN published figure on **20 of 23**
comparable quarters across both companies, and disagrees ONLY on exactly the 3 cells this
campaign was asked to arbitrate. This is the opposite signature of a wrong-row defect (HUDCO's
Interest-Income swap produced a consistent bias every quarter, not an isolated one) — it directly
refutes the concern that the earlier check was purely self-referential, because this comparison
uses third-party data neither the construction nor this campaign had touched before.

**What this does NOT do:** identify the sites' actual source for these 3 cells. That remains
unexplained. A suggestive but unconfirmed lead: NIVABUPA's own filing notes flag both contested
NIVABUPA quarters as touched by live accounting-method transitions (a 1/n premium-recognition
regulatory change effective Oct-2024, and an EOM computation methodology that IRDAI advised the
company to restate in Dec-2025/Jan-2026) — exactly the kind of quarter where a third-party data
vendor might hold a stale or misaligned figure. The magnitudes don't fully reconcile (the noted
242.58cr Jun-2025 GWP adjustment covers only ~43% of the 560.92cr gap) and no analogous note was
found for STARHEALTH, so this is reported as an open lead, not a conclusion.

**Revised outcome: OUTCOME 2** (sites' figure not located after a genuine search), **with
confidence on cells 2, 4, 6 downgraded from `high` to `medium`** — the specific disagreement for
these three quarters remains unexplained. Verdicts are UNCHANGED (`OURS_CONFIRMED`): the values
are still exactly what the primary filing's labelled rows say under a construction now
externally cross-validated on 20 other quarters, not merely self-consistent with our own prior
data. PAT verdicts (cells 1, 3, 5) and the JUBLPHARMA cell (7, confirmed via two independent
routes — BSE detres AND the announcement PDF, not the IRDAI construction) are unaffected and
remain `high` confidence, per the coordinator's explicit scope.
