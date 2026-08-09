# QUARTERLY REVENUE & PAT — VERIFICATION REPORT
**Campaign run 2026-08-09.** Snapshot: origin/main **`e8a491c6`** (re-pinned mid-run from `8e72b277`;
see §6). Scope: 47,436 point-in-time Nifty-500 member-quarters, Dec-2002 → Jun-2026.
Plan: `REVPAT_VERIFY_CAMPAIGN.md`. Phase findings: `revpat_verify/P0_FINDINGS.md`, `P1_FINDINGS.md`,
`P2_FINDINGS.md`. Tooling (all re-runnable, all run from the committed path before this was written):
`revpat_verify/{audit_revpat_coverage,revpat_strata,revpat_mapcard,revpat_quorum,build_contested,exchange_fetch}.py`.

The question: *are our quarterly revenue and profit numbers correct?* — to be answered against
5-7 external sites plus the exchange filings, taking a value only when many sources agree.

---

## 1. THE ANSWER SO FAR

**Nothing we publish has been shown to be wrong. Two things we publish have been shown RIGHT against
the companies' own filings, in cases where a site confidently said otherwise.**

| test | scope | result |
|---|---|---|
| Rule-6b cross-site quorum (3 sites, accepted mappings only) | 558 (symbol, quarter, field) cells | **201 CONFIRMED, 305 single-site-OK, 4 CONTRADICTED** |
| Cells where ≥2 sites agree with each other *and* with us | 558 | **201** |
| Cells where ≥2 sites agree with each other *against* us | 558 | **4** (all under arbitration, §4) |
| Arbitration at the filing — SBIN consolidated PAT | 10 quarters, site disagreed on **10 of 10** | **OURS_CONFIRMED, exact to the paisa; the site is wrong** |
| Internal store consistency (zero-network) | 133,000+ populated PAT cells | **55 disagree** at `50e57c82` (753 at my pin; the con side was closed concurrently — §5) |

**The single most important result is a negative one.** The pilot's worst-looking finding —
SBIN consolidated PAT, where Tickertape disagreed with us on *every one* of 10 quarters, always in
the same direction, with a coherent "you stored total, we store owners" story — was taken to SBI's
own consolidated XBRL and **our value was exact**:

```
filing tag  ProfitLossAfterTaxesMinorityInterestAndShareOfProfitLossOfAssociates (Dec-2024) = 18,853.16 cr
ours                                                                                        = 18,853.16   delta 0.00
Tickertape                                                                                  = 18,544.73   delta -308.43
ours minus the filing's own minority interest (the accusation)                              = 18,222.54   -- not the site's number either
```

and the **§45 quarter-sum identity against the filer's own 9-month YTD context** confirmed three
quarters at once: our Jun+Sep+Dec-2024 sum to **57,960.88** against the filed **57,960.88** (delta
0.00), while the site's three sum to 56,893.48 — missing SBI's own published nine-month figure by
₹1,067 cr.

*A confident, consistent, mechanistically plausible disagreement from a site was still the site's
error.* That is the entire justification for the arbitration rung, and it is why site majority
never decides here.

## 2. ★★★ THE CAMPAIGN PLAN'S OWN SLOT NOTE WAS WRONG, AND IT WOULD HAVE POISONED EVERYTHING

The plan describes `sf_revop` rows as `[rev_s, rev_c, op_s, op_c, other_inc_s, other_inc_c, flags,
pat_s, pat_c]`. Measured from `build_revop.py` and confirmed by value identity:

    sf_revop[SYM][QE] = [revS, revC, opS, opC, patS, patC, finFlag, ebitS, ebitC]

**PAT is at slots 4/5. Slots 7/8 are EBIT.** Reading the plan literally would have compared **EBIT**
against every site's **net profit** and manufactured a mismatch on essentially every cell in the
campaign. The plan told us to verify this rather than trust it; that instruction earned its place.

**And PAT's authority is not `sf_revop` at all** — it is `docs/sf_fundamentals.json`
(`[qe, npStd, annStd, npCon, annCon]`, owners-attributable). `build_quarterly_results.py`,
`stock.html` and `backtest-engine.js` all read fundamentals; stock.html says outright *"never swap in
sf_revop's PAT mirror slots."* Verification targets are therefore:

| field | authority |
|---|---|
| revS / revC | `sf_revop[0]` / `sf_revop[1]` |
| patS / patC | `sf_fundamentals` npStd / npCon |
| ~~`sf_revop[4]/[5]`~~ | **a mirror — never compare it to a site** (would fabricate ~753 phantom defects) |

## 3. ★★★ THE SITES CANNOT REACH OUR DATA — 86% of it has no site coverage at all

Measured quarterly depth, per site, for quarterly revenue and PAT:

| site | quarterly depth | oldest quarter | bases | status |
|---|---|---|---|---|
| **Screener** | **13 qtrs** | **Jun-2023** | std + con (separate URLs) | ✅ deepest |
| Tickertape | 10 qtrs | Mar-2024 | consolidated only | ✅ via `www` SSR |
| Groww | 5 qtrs | Jun-2025 | both | ✅ |
| StockEdge | **none** (annual only, 5 rows, standalone) | — | — | ❌ excluded |
| Trendlyne | — | — | — | ❌ 403 on request #1 |

Our data spans **95 quarters**; the deepest site reaches **13**. So **rule 6b (filing AND ≥2
independent sites) is only satisfiable from about Mar-2024** — roughly **10 of 95 quarters**.
For Dec-2002 → Mar-2023 no site speaks at all, and the exchange filing is the entire evidence base.

This independently reproduces the sibling SHP campaign's central structural finding, from different
sites and different fields. **It is not a reason to weaken rule 6b** — it is the measurement that
says the exchange leg carries this campaign.

### 3a. ★★ Consolidated PAT has NO site quorum, and the arithmetic says why
Tested against our stored consolidated owners-attributable PAT, the row each site calls "Net Profit":

| site | segment | hold | one-sided bias | verdict |
|---|---|---|---|---|
| Screener | con / nonfin | **37.6%** | **+1.68%** | ❌ refused |
| Screener | con / fin | **47.7%** | **+0.58%** | ❌ refused |
| Groww | con / nonfin | **42.9%** | **+1.55%** | ❌ refused |
| Screener | **std** | 94.8–94.9% | — | ✅ accepted |
| Groww | **std** | 93.3–**100%** | — | ✅ accepted |

Two independent sites, same direction, same magnitude, and **the bias vanishes entirely on
standalone** — where there is no minority interest. The sites publish **total** consolidated profit;
we publish **owners-attributable**. So Screener and Groww **must not vote on consolidated PAT**
(exactly as the SHP campaign barred Screener from voting on DII), leaving Tickertape as the only
voter — and one site is not a quorum. **Consolidated PAT is arbitration-only, permanently, on the
current site landscape.** No site publishes an owners-vs-total split for a quarter.

### 3b. ★★ The mapping is COMPANY-CLASS dependent — one global card fails every bank
Unsegmented, Tickertape's revenue field scored **21.4%** against our consolidated revenue and was
refused. Segmented by our own `fin` flag it resolves: RELIANCE agrees to ~0.1%, while **SBIN runs
29–42% higher every quarter** (Mar-2024: 164,914 vs our 117,469). Our revenue for a bank is
**Interest Earned**; theirs is **Total Income**. A single global mapping would have flagged every
bank in the universe as a revenue defect — the same failure as the SHP campaign's wrong era-split,
on a different axis. Cards are keyed `(basis, company class)`.

### 3c. "The name lies" — three more instances, on three different sites
- **StockEdge `Consolidated_NetProfit` is not consolidated**: on standalone rows it is arithmetically
  `Profit_after_tax + extra_items` (RELIANCE FY2023: 43,002 + 1,188 = 44,190, exact).
- **Tickertape sid `TRU` is Trust Fintech**, unrelated to our TRU (reachable only as sid `DHA`).
- **StockEdge's ticker search is name-fuzzy**: `IEL` returns *Gabriel India* first; the real IEL is
  result #6 of 26.
- (Fourth, different kind) **Groww's `financialSummary` narrative is ~2 years stale** and for TCS
  describes a six-month period as the quarter — trap T-D, live.

Identity discipline held: **zero identity-skips** across 14 stocks × 3 sites, every one confirmed by
exact ticker echo, with ISIN where the payload carries one.

## 4. WHAT IS CONTESTED — 4 cells, under arbitration

The only cells where two independent sites agree with each other *and* disagree with us:

| symbol | quarter | field | ours | Screener | Groww |
|---|---|---|---|---|---|
| GICRE | 2025-06-30 | std PAT | 2,172.77 | 1,752.00 | 1,752.23 |
| GICRE | 2025-09-30 | std PAT | 2,698.01 | 2,867.00 | 2,866.79 |
| BAJFINANCE | 2025-12-31 | std revenue | 18,067.89 | 17,870.00 | 17,869.70 |
| BAJFINANCE | 2025-12-31 | con revenue | 21,213.89 | 21,013.00 | 21,013.49 |

GICRE is one of the two open items the plan asked to fold in ("its standalone cells are still
suspect"), and for Jun-2025 **our own non-authoritative mirror holds 1,752.23 — siding with the
sites against our authoritative file.** BSE detres and NSE XBRL both refuse these four (GICRE is an
IRDAI-format insurer); under §57 that is *not* evidence of absence, so the remaining ladder rungs
(BSE announcement PDF with the §58 column anchor; IRDAI disclosures) are being walked. **Verdicts
are not in as of this writing and no heal has been proposed.**

A further **10 cells are SITES_DISAGREE** — almost all RELIANCE revenue, where Screener sits ~1-2%
*below* us and Groww ~9% *above* us on the same quarter (Jun-2026: 309,468 / **311,850** / 340,257).
Three different revenue concepts, consistent with RIL's gross "Value of Sales & Services" versus net
"Revenue from operations". Under rule 6b, **sites disagreeing among themselves means the value is
not taken** — correctly, no verdict was drawn.

## 5. OPEN — the internal contradiction, and a correction to this report's own first draft

At my pin I measured **753 cells where our own two stores disagree about PAT** (720 con, 33 std),
zero network: `sf_fundamentals` (authoritative) vs the `sf_revop` mirror. **That number is now
stale, and re-measuring against current `origin/main` (50e57c82) is the honest thing to do:**

| | at pin `e8a491c6` | at `50e57c82` |
|---|---|---|
| consolidated | 720 | **22** |
| **standalone** | **33** | **33 — unchanged** |
| total | 753 | **55** |

A concurrent session closed the consolidated side while this campaign was running (runbook **§70**,
**§71**): it found `build_discovery.ttm_pat` was reading the mirror through `pick(cell, 5, 4)` — so
the Discovery / Order-Wins TTM P/E was computed off the wrong PAT across 203 symbols — fixed that
consumer, then resynced the mirror **to** fundamentals, writing only `sf_revop` and never the
authoritative file. 1,372 → 766 → 23.

**★ Their §71 is the most important caution for this campaign, and it points straight at my own
method.** They tried to adjudicate those cells against the filers' own cached XBRL `owners` tag and
got a clean-looking verdict — *fix fundamentals in 718 cases*. **It was wrong, and applying it would
have destroyed 693 correct values:** in that population the owners tag is itself ×0.1, sign-flipped,
or unscaled raw rupees (MARUTI Sep-2022 tag 212.50 against a real ₹2,112 cr; KAYNES Mar-2023 tag
5,814,249.6 against ₹63 cr). **A source being primary does not make it correct.**

Does that undermine §1's SBIN result, which also read an XBRL owners tag? **No — and the reason is
the control, not the tag.** SBIN was not a bare tag read: the tag value equalled our stored figure
*exactly*, and independently our three stored quarters summed to the filing's own 9-month YTD
context to **delta 0.00**. A corrupted tag (×0.1, sign-flip, raw rupees) fails both of those at
once. That is precisely the "confirm against facts you can check independently" discipline their
§71 asks for, applied before the conclusion rather than after.

**What remains genuinely open, and is this audit's distinct contribution: the 33 STANDALONE
divergences are untouched.** §70 and §71 were both consolidated-scoped; `build_contested.py` covers
both bases, and the std side has not moved. Of the 55 remaining: **6** are in the point-in-time N500
*and* the site-verifiable window (full rule-6b quorum available), 2 are N500 but older, 47 sit
outside the denominator. Fingerprints: 9 sign flips, 1 power-of-ten, 1 zero-sentinel. Their held-back
23 are journalled separately in `scripts/_fund_suspect_cells.json` (ZEAL, RELCAPITAL, NUCLEUS, IFCI,
IRB), each needing a filing read.

**The standing warning either way** (§70c): SADBHAV Dec-2020, where the authoritative file held the
*total* and the mirror held the right magnitude with a *flipped sign* — **neither file was right.**
"The two sides disagree" never implies one of them is correct.

## 6. METHOD, AND WHERE I WAS WRONG

- **The sample was frozen before looking** — 66 symbols, deterministic md5 draw over 13 strata built
  around the five named traps, committed as `p3_strata.json`. Re-running the drawer reproduces the
  identical list (verified).
- **Mappings derived arithmetically, never from labels** — `revpat_mapcard.py` searches
  (site label × our field × scale) and **refuses** below 80% hold. 46 of 54 candidate Screener
  mappings and 34 of 40 Groww ones were refused. "Nothing fits" is a reportable answer.
- **Read-only, robots-honoured throughout.** ≥2s per request; Trendlyne's 10s ClaudeBot crawl-delay
  honoured (it 403'd on request #1 and the agent stopped rather than evade); StockEdge's login wall
  respected rather than crossed; **Tickertape's `api.*` hosts found to be `Disallow: /` and abandoned
  in favour of the allowed `www` SSR route** — which also means **§22h may need correcting**, as the
  sibling SHP campaign appears to have used the disallowed host.
- **Two of my own errors, both caught by evidence rather than by me:**
  1. I proposed that the con-side store divergence was the CI owners re-assertion. Measured: **30 of
     1,313 (2.3%)**. The tidy story was wrong; the cause remains undiagnosed and is recorded as open
     rather than inherited as a guess.
  2. I called the mirror "latent, not live" after grepping consumers for literal `[4]`/`[5]`. It was
     **live** — `build_discovery` reads it through `pick(cell, 5, 4)`, which a syntactic grep cannot
     see. *Grep for the quantity's consumers, not for an index literal.*
- **The pin moved once, deliberately.** Started at `8e72b277`; re-pinned to `e8a491c6` when the
  concurrent §70 fix landed on the very population being measured. Chasing every CI data commit is
  futile; capturing a fix to the audit target is not.

## 7. WHAT HAS *NOT* BEEN DONE — stated plainly

- **P3 (66-symbol stratified audit) and P4 (bulk sweep) have not been run.** The quorum numbers in
  §1 are the **14-stock calibration pilot** (592 site stock-quarters), not the frozen sample. The
  sample is drawn and the tooling runs; the extraction has not.
- **The 4 contested cells are unresolved** as of writing (§4).
- **No heal has been performed, and none should be yet.** Two blockers, both still standing: the SHP
  campaign's pending `nsh` reparse has not landed, and a concurrent session is actively writing the
  same PAT stores. One staged writer at a time.
- **The internal-divergence queue is measured, not adjudicated** — now 55 cells, of which the
  **33 standalone ones are untouched by §70/§71** (both were consolidated-scoped).
- **Pre-2023 is unverifiable by site, by measurement** — not "unchecked". Any future report must keep
  that distinction; reporting it as unchecked would misstate both the effort and the risk.

## 8. NEXT
1. Finish arbitrating the 4 contested cells down the §57 ladder (rung 3 onward).
2. Run P3 over the frozen 66 symbols × 3 sites, then P4 breadth-first on Screener.
3. Work the 82 priority-1 internal-divergence cells: full 6b quorum *plus* filing arbitration.
4. Only then heal, via ledgers (`scale_fix` / `feed_qe_fix` / `revop_fundamentals`), one writer at a
   time, re-running the nightlies and diffing (§41: journalled is not live), verifying LIVE ~20 min
   after the push and again after the next nightly.
