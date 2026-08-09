# P2 — CALIBRATION PILOT: MAPPING CARDS DERIVED ARITHMETICALLY
14 trap stocks × 3 usable sites, both bases. Tool: `p2/revpat_mapcard.py`. Cards:
`p2/{screener,tickertape,groww}_map.json`. **No verdict in P3-P5 may use a mapping this phase refused.**

Pilot set: RELIANCE (baseline), SBIN + HDFCBANK (banks), SBILIFE + GICRE + LICI (insurers),
BAJFINANCE (NBFC/ORFO), TATASTEEL + GRASIM (big NCI), SAIL (loss-maker), ETERNAL (rename),
MOTHERSON + MSUMI (demerger pair), MEESHO (recent IPO). 592 site stock-quarters extracted.

---

## 1. ★★★ THE HEADLINE: CONSOLIDATED PAT HAS NO SITE QUORUM, AND THE ARITHMETIC SAYS WHY

Every site's consolidated profit row was tested against our stored consolidated owners-attributable
PAT across all overlapping quarters. Result, for the row each site literally calls "Net Profit":

| site | segment | best fit | **hold** | one-sided bias | verdict |
|---|---|---|---|---|---|
| Screener | con / nonfin | patC | **37.6%** | **+1.68%** | ❌ REFUSED |
| Screener | con / fin | patC | **47.7%** | **+0.58%** | ❌ REFUSED |
| Groww | con / nonfin | patC | **42.9%** | **+1.55%** | ❌ REFUSED |
| Screener | **std** / nonfin | patS | 94.8% | — | ✅ accepted |
| Screener | **std** / fin | patS | 94.9% | — | ✅ accepted |
| Groww | **std** / nonfin | patS | **100.0%** | — | ✅ accepted |
| Groww | **std** / fin | patS | 93.3% | — | ✅ accepted |

**Two independent sites, same direction, same magnitude: their consolidated "Net Profit" sits
~1.5–1.7% ABOVE ours, and the excess disappears entirely on standalone.** That is the minority-
interest slice. The sites publish **TOTAL** consolidated profit; we publish **OWNERS-ATTRIBUTABLE**
(`apply_owners_full.py`, re-asserted by CI every run). There is no NCI on a standalone statement,
which is exactly why the bias vanishes there — the fingerprint is self-confirming.

This is T-B, proven arithmetically rather than read off a label — and P1 predicted it precisely:
**not one site publishes an owners-vs-total split for a quarter.**

**Consequence, and it is a hard one:** Screener and Groww **must not vote on consolidated PAT**,
exactly as the SHP campaign barred Screener from voting on DII (62% hold there, 37.6% here). Only
Tickertape maps con PAT at all (94.0% nonfin / 74.0% fin) — **one site is not a quorum.** So
**rule 6b can never be satisfied for consolidated PAT by sites.** It is arbitration-only, forever,
on the current site landscape.

## 2. ★★★ THE PILOT'S ONE BIG CONTRADICTION — AND THE FILING SAYS WE ARE RIGHT

The single worst-looking result in the whole pilot: **SBIN consolidated PAT, 0 of 10 quarters
matching Tickertape**, ours higher every quarter by ₹308–466 cr (~2%), while every other financial
matched 9-10/10. A textbook "we store total, they store owners" accusation.

Taken to SBI's own consolidated XBRL (Dec-2024), read directly:

```
ProfitLossAfterTaxesMinorityInterestAndShareOfProfitLossOfAssociates  OneD  = 18,853.16 cr
ProfitLossOfMinorityInterest                                         OneD  =    630.62 cr
ProfitLossAfterTaxesMinorityInterestAndShareOfProfitLossOfAssociates  FourD = 57,960.88 cr
```

| | value | delta vs the filing |
|---|---|---|
| **our stored Dec-2024 con PAT** | **18,853.16** | **0.00 — exact to the paisa** |
| Tickertape Dec-2024 | 18,544.73 | −308.43 |
| ours − minority interest (the accusation) | 18,222.54 | *not Tickertape's number either* |

And the **§45 quarter-sum identity against the filer's own 9-month YTD context**:

| | Jun+Sep+Dec 2024 | delta vs filed 9M (57,960.88) |
|---|---|---|
| **ours** | **57,960.88** | **+0.00** |
| Tickertape | 56,893.48 | −1,067.40 |

**Verdict: OURS_CONFIRMED on three quarters at once; Tickertape CONTRADICTED by SBI's own document.**
The tag we match is the one runbook §53c calls "literally this dataset's basis". The total-vs-owners
hypothesis is refuted outright — ours-minus-MI is not Tickertape's figure, so its number is neither
owners nor total; it is simply wrong.

**This is the whole reason for the arbitration rung.** A 10-of-10 disagreement with a coherent,
plausible mechanism was still the site's error, not ours. Site majority must never decide.

## 3. Accepted mappings — what each site may vote on

| field | Screener | Tickertape | Groww | site voters |
|---|---|---|---|---|
| **revS** (std revenue) | ✅ Sales/Revenue 82.5–97.4% | ✅ qIncTrev 100% (nonfin) | ✅ 87.5–96.7% | **3** |
| **revC** (con revenue) | ✅ Sales/Revenue 81.2–94.9% | ❌ refused | ✅ 85.7–96.0% | **2** |
| **patS** (std PAT) | ✅ Net Profit 94.8–94.9% | ✅ qIncNinc 100% | ✅ 93.3–100% | **3** |
| **patC** (con PAT) | ❌ **refused (total, not owners)** | ✅ qIncNinc 74–94% | ❌ **refused** | **1 — no quorum** |

Scale on every accepted mapping resolved to **₹ crore ×1** — no site in the pilot needed a
power-of-ten correction, so a 10×/100× delta in later phases is a genuine T-C signal, not a units artefact.

## 4. ★★ THE MAPPING IS COMPANY-CLASS DEPENDENT — one global card would have failed every bank

First pass, unsegmented, Tickertape's `qIncTrev` scored **21.4%** against our consolidated revenue
and was refused outright. Segmenting by our own `fin` flag (`sf_revop[6]`) explained it:

- **RELIANCE**: their revenue vs ours agrees to ~0.1–0.4% every quarter.
- **SBIN**: theirs runs **29–42% higher, every single quarter** (Mar-2024: 164,914 vs our 117,469).

Our revenue for a bank/NBFC is **Interest Earned**; the site's is **Total Income** (interest + other
income). Across the pilot, 100 of 112 consolidated observations had the site above us, median +1.1%,
mean +8.4% — the mean dragged up entirely by the financials.

**A single global mapping would have flagged every bank in the universe as a revenue defect.** Same
class as the SHP campaign's wrong era-split that installed 13 phantom mismatches — a different axis
(company class, not era), the same failure. The card is now keyed `(basis, class)`.

## 5. Cross-site structural agreements (three independent sources, same answer)

- **SBILIFE and MSUMI have no consolidated quarterly series** — Screener renders the table with zero
  date columns, Groww's `consolidatedQuarterly` array is genuinely absent, and Tickertape reports
  `reporting: "standalone"` on all 10 of their quarters. Three independent sites agreeing that a
  basis does not exist is strong evidence it is not filed — relevant to §57's "never infer absence",
  in the direction of *positive* evidence for absence.
- **MEESHO Jun-2025 is identical across both bases on Screener AND Groww**, then diverges sharply
  from Sep-2025 (Groww Dec-2025: con −490.68 vs std +733.53). Consistent with consolidation scope
  beginning after that quarter (pre-IPO structure flip). Flagged, not adjudicated.
- **GRASIM and GICRE run one quarter behind** the rest on every site — a filing-lag artefact, not a gap.

## 6. Traps recorded during the pilot
- **Screener's bare `/company/<SYM>/` URL is ALWAYS standalone**, never "whichever they report".
  RELIANCE Jun-2023 std Sales 122,627 vs con 207,559 — taking the basis wrong understates by ~41%.
- **Insurers use Screener's INDUSTRIAL row template**, not a bank one — so a template-based classifier
  would mis-handle SBILIFE/GICRE/LICI. Classify by our own `fin` flag, never by the site's layout.
- **Groww's sitemap is stale** (all `lastmod` 2023-11-01) and omits LICI, MOTHERSON, MSUMI, MEESHO and
  ETERNAL entirely; those resolve only by direct slug guess + `nseScriptCode` confirmation.
- **Renamed tickers keep their OLD slug** on Groww (`zomato-ltd` → ETERNAL) and Tickertape
  (`zomato-ZOM`, sid `MOSS` for MOTHERSON). The payload's ticker/ISIN echo is the only safe check —
  never the slug.
- Identity discipline held: **zero identity_skips** across 14 stocks × 3 sites, every one confirmed
  by exact ticker echo (Tickertape and Groww also carry ISIN).

## 7. What P2 gates for the rest of the campaign
1. **patC is arbitration-only.** No site quorum exists or can exist for consolidated PAT.
2. **revS, revC, patS have 2-3 site voters** — rule 6b is satisfiable for those, in the ~10-quarter
   window P1 measured.
3. **Every diff must be segmented by (basis, company class).** Unsegmented comparison is a known
   phantom-defect generator, measured at 21.4% vs 94%+ on the same data.
4. **A confident cross-site disagreement is not evidence.** SBIN is the pilot's proof.
