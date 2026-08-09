# FII / DII HOLDINGS — VERIFICATION REPORT
**Campaign run 2026-08-09. Snapshot verified: origin/main `93de247c`, `shp_history.json` blob `b2bed157`
— 2,615 symbols, 66,477 stock-quarters, Sep-2010 → Jun-2026.**

The user's question was: *are our FII/DII holdings correct?* — to be answered by checking against
5-7 external sites, taking a value only when many sources agree. This is the answer.

---

## 1. THE ANSWER

**Nothing we publish has been shown to be wrong.**

| test | scope | result |
|---|---|---|
| **Cross-exchange** — our NSE-derived values vs BSE's *separately filed* documents | 61 symbols × 41 quarters, Jun-2016→Jun-2026 | **2,990 MATCH, 1 ROUND, 0 MISMATCH** |
| **Three sites** vs the frozen stratified sample | 66 symbols, 3,884 cells we hold | **2,156 CONFIRMED, 0 contradicted** |
| **Arbitration** — every contested cell taken to the actual filing | 136 field-verdicts | **79 OURS_CONFIRMED, 0 OURS_WRONG, 0 REVISION** |
| Cells where *no* independent source agrees with us | 3,884 checked | **0** |
| Cells where the sites agree with each other and disagree with us | 3,884 checked | **0** |

The single non-exact value in the entire cross-exchange set is CUMMINSIND Jun-2022 shareholder
count: ours 109,068, BSE 109,067 — **one shareholder, across a decade of filings.**

Every individual arbitration — ICICIBANK, HINDALCO, RELIANCE, MCX, and 79 others — read the
company's own filing and reproduced our stored cell field-for-field.

## 2. WHAT WE ACTUALLY FOUND — errors on the sites, gaps in our coverage

**Errors on the sites** (arbitrated against the filing, we were right):
- Screener's FII is wrong on some large caps: ICICIBANK Jun-2026 it says 33.79, the filing says
  **49.82**; HINDALCO Jun-2026 it says 31.41, the filing says **35.60**. Measured miss rate ~2.7%.
- Screener's DII bucket is not ours at all (62% hold) and it does not vote on DII in this campaign.
- Moneycontrol maps to **nothing** — its FII matches ours exactly in 58% of quarters and misses
  badly in the rest, a misalignment pattern, so it was excluded from the quorum rather than forced in.

**Gaps in ours** (the genuinely actionable output):

| finding | scale | status |
|---|---|---|
| **Shareholder counts missing** — quarter-shaped, not scattered | **9,094 cells (13.7%)**; ~5,100 recoverable | diagnosed, route known, NOT yet run |
| Quarters sites/BSE hold that we do not | ~65 in the sample (51 nsh + 6 BSE + others) | verified fillable |
| Internal holes inside a symbol's own history, post-Jun-2016 | **2,344 across 680 symbols** | cause UNKNOWN — see §4 |

The shareholder-count gap is the headline. Coverage runs at 99.9% in every quarter from Sep-2019
**except**: Sep-2022 (1.1%), Jun-2024 (0.3%), Mar-2024 (78%), and Sep-2025→Mar-2026 (~93%).
The filings have the data and today's parser reads it — verified 6/6 on both blackout quarters
(RELIANCE Sep-2022 = 3,485,825; HDFCBANK Jun-2024 = 3,664,325 — both stored as empty by us).
Cause: those quarters were populated before shareholder-count extraction existed and fell outside
the 8-quarter `--reparse` of 2026-07-16. Fix is `--reparse` scoped to the seven deficient quarters.

## 3. THE SITES — what each is actually worth

| site | depth | verdict |
|---|---|---|
| **Screener** | Mar-2017 FY-ends + 12 rolling qtrs | deepest cheap source; nsh exact **to the person** (48/49); real FII errors |
| **StockEdge** | 9 quarters (hard cap) | cleanest match — median delta **0.0000** on prom/fii/mf/ins |
| **Trendlyne** | Dec-2015 via per-quarter pages | verifies mf AND ins at **100%**; 10s crawl-delay makes it sample-only |
| **Tickertape** | 6 quarters | clean API, provable bucket identities; DR-denominator difference |
| **Groww** | 5 quarters | strong on prom/fii/dii/mf |
| **ET Markets** | 4 quarters | prom/fii/mf exact; no insurance, no counts |
| **Moneycontrol** | 5 quarters | **unusable** — nothing maps |
| **BSE (exchange)** | Jun-2016 → date | the real check; 0 disagreements |

**★ No site has data before 2010, and only Trendlyne reaches before 2017.** Our two weakest eras
(2002-2010 at 0%, 2010-2015 at 30%) cannot be corroborated by any retail aggregator — which is
why the campaign was rebuilt around cross-exchange verification instead.

## 4. OPEN, AND HONEST ABOUT IT

- **The 2,344 internal holes have no diagnosed cause.** My hypothesis — BSE rows with a null
  `filing_date_time` — was measured and **failed**: 43 null-date rows exist in the sample and only
  6 correspond to a cell we lack. P3b must diagnose, not inherit that guess.
- **2010-2015 is unverifiable by design.** Those cells came from archived Moneycontrol, so a
  Moneycontrol match would be circular; no other site reaches back that far and BSE's XBRL starts
  Jun-2016. Reported as unverifiable rather than quietly counted as fine.
- **6 cells are UNPARSEABLE** (BHANDARI ×4, SOMICONVEY, PUNJLLOYD): a document exists but
  `parse_shp` refuses to anchor it. That is the parser working correctly — zero-filling an
  unanchored filing is what poisons FII/DII — and they stay open.
- **Phase 4 (all 2,615 symbols) was still running when this report was written.** Its numbers are
  not in the totals above.

## 5. METHOD — why these numbers can be trusted

- **Nothing decided by site majority.** Sites copy each other; only exchange filings arbitrate.
- **Mapping cards derived arithmetically, never from labels.** Groww publishes a field literally
  named `otherDomesticInstitutions.insurance` that is *all non-MF domestic holdings*. Mapping by
  name would have manufactured a ~1.9pp defect on every stock. The tool searches for the subset of
  a site's rows whose SUM reproduces ours, and refuses when nothing fits.
- **Provenance tracked per cell** so a site can never "verify" data we took from it: 6.2% of our
  cells came from archived Moneycontrol, 0.1% from screener/Trendlyne.
- **The sample was frozen before looking** — deterministic md5 draw, 12 strata, committed.
- **Identity discipline caught real traps**: Tickertape's sid `TRU` is an unrelated company
  (Trust Fintech); StockEdge's ticker shortcut matched `IEL` to the wrong firm. Both would have
  become fake "defects" in our data.

**Eleven tooling defects were found and fixed during the campaign** — a crash on `"22.58%"`
strings, a deriver that fitted noise, an era-split that installed the wrong mapping (13 phantom
mismatches), missing unit handling for lakhs (95 phantom mismatches), percentage thresholds applied
to headcounts, a CONTRADICTED bar that accused sites of agreeing when they didn't (10 false
accusations), and more. **Every one of them initially looked like a defect in the data.** That is
the reason for the arbitration rung: no value is called wrong until the filing says so.

**And one substantive error of my own**: I reported a parser bug (`mf = 0.0` on BSE filings) and
filed it as blocking. The mechanism was real but the fix had already shipped 2026-08-07 — my test
imported a local checkout **227 lines behind origin/main**. *Analyse live, never the local
checkout* applies to code exactly as it does to data.

## 6. WHAT HAPPENS NEXT

1. Finish Phase 4 (all 2,615 symbols × 3 sites), fold into the same quorum + arbitration pipeline.
2. **Then** run the shareholder-count heal — `--reparse` on the seven deficient quarters, via the
   §22b staging file, never concurrently with the sweeps (12k NSE fetches) and never as a second
   writer against CI's twice-daily `shp_history.json` job.
3. P3b: diagnose the 2,344 internal holes properly.
4. Re-verify live ~20 min after any push (§41: "live on the server" ≠ "the site uses it").
