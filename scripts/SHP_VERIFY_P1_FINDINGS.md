# SHP VERIFY — PHASE 1 RESULTS + MANDATORY PLAN REVISION  (2026-08-09)

7 recon agents, 7 sites, 191 raw extraction rows. Every site characterized; no site refused us.
**Two findings force a change to Phases 3–5.** Read this before running any further phase.

## 1. Site capability matrix (measured, not assumed)

| site | earliest quarter | depth | route | sub-rows we can use | access |
|---|---|---|---|---|---|
| **Screener.in** | **Mar-2017** (FY-end) + 12 rolling qtrs | deep, cheap | server-rendered HTML, symbol IS the slug | prom/FII/DII/Govt/Public + **No. of Shareholders** | open |
| **Trendlyne** | printed table Dec-2023; **per-quarter pages reach Dec-2015** | **deepest, but expensive** | HTML; 1 request PER QUARTER for anything older than the 12-col window | prom(+pledge/locked), FII, DII **> MF/Banks/Insurance** | open |
| StockEdge | Jun-2024 (9 qtrs, hard cap) | shallow | **anonymous JSON API** | FII/FPI, DII **> MF/Banks/Insurance**, **ADR/GDR**, nsh (lakhs) | open |
| Tickertape | Mar-2025 (6 qtrs, hard cap) | shallow | **clean JSON API** | mf, **ins**, dii, fii (identity ties to 100.000) | open |
| Groww | Jun-2025 (5 qtrs) | shallow | `__NEXT_DATA__` in SSR HTML | mf + "otherDomesticInstitutions" + fii | open |
| Moneycontrol | Jun-2025 (5 qtrs) | shallow | 2 JS literals in SSR HTML | prom/FII/DII only — **no MF/insurance** | open |
| ET Markets | Sep-2025 (4 qtrs) | shallowest | server-rendered HTML | FII, DII, MF (no insurance, no nsh) | open |

## 2. ★ FINDING A — pre-2010 is a 7/7 WALL, and pre-2017 is nearly one

**Not one of the seven sites shows a single quarter on or before Jun-2010** — for RELIANCE, TCS,
ITC or HDFCBANK. Trendlyne's own quarter-selector dropdown enumerates its entire universe and
stops well short. This is seven independent negative results, so the Dec-2002→Jun-2010 era
(15,473 member-quarters, **0% covered**) gets nothing from this roster. That is not the same as
"unfillable" (§57 / never-say-unfillable): it means *these* sites are the wrong route, and the
remaining ladder is exchange archives + Wayback, not retail aggregators.

Worse for the campaign as written: **only Screener reaches back past 2024 in any useful way**
(Mar-2017). So the quorum in rule 6b is:

| era | sites that can corroborate | rule-6b quorum reachable? |
|---|---|---|
| Jun-2024 → Jun-2026 | all 7 | **yes, comfortably** |
| Sep-2023 → Jun-2024 | Screener + Trendlyne + StockEdge | yes |
| **March** quarters 2017-2023 | Screener (FY-end col) + Trendlyne (per-qtr page) | yes, 2 sites |
| **non-March** quarters Dec-2015→2023 | **Trendlyne only** | **no — 1 site** |
| pre-Dec-2015 (needs 3) | **none — Trendlyne bottoms out at Dec-2015** | no |

Trendlyne is therefore the campaign's deep-history workhorse, but at **1 HTTP request per
(stock, quarter)** and a **10-second crawl delay that its robots.txt sets for ClaudeBot by name**
(stricter than the plan's 3s floor — honour the site's own number, not ours). Budget accordingly:
the 66-symbol sample over Dec-2015→date is ~2,100 requests ≈ 6 hours on Trendlyne alone.

**The plan's implicit premise — that 5-7 sites could corroborate our whole 2010-2026 history —
is false, and it was false before the first request went out.** Phases 3-5 must change.

## 3. ★ FINDING B — the fix: cross-EXCHANGE verification is the real independent check

Our cells are overwhelmingly NSE-derived (81% `nse-live`). **BSE receives its own separate filing
for the same (sym, quarter)** — `SHPQNewFormat` XBRL, real files from Jun-2016. Parsing BSE and
comparing to our NSE-derived value is a *genuinely independent* check that:
- covers **Jun-2016 → date**, the whole era the retail sites abandon;
- is an exchange primary source, so it also *arbitrates* rather than merely votes;
- is already proven in this repo (the 2026-08-09 sweep ledger, `parse_shp` unmodified).

**Revision (adopted): the quorum for a cell = independent SITES + the independent EXCHANGE.**
- Jun-2016 → date: NSE filing + BSE filing agreeing = 2 independent sources; a retail site
  agreeing is a third. Cross-exchange disagreement is the highest-value defect signal we have.
- 2010-2015: no second exchange (BSE XBRL doesn't exist) and no site → these cells stay
  **UNVERIFIABLE-BY-DESIGN** and must be reported as such, never quietly counted as "fine".
  Their provenance is already `wayback-mc`, i.e. Moneycontrol — so a Moneycontrol match would
  have been circular anyway (trap T3). Nothing is lost that was ever obtainable.

## 4. Early verification results (real data, not synthetic)

- **RELIANCE Jun-2026 — CONFIRMED against the filing itself.** Re-fetched the live NSE XBRL and
  re-ran `parse_shp`: `prom 50.48, fii 17.20, dii 21.19, mf 10.11, ins 9.20, nsh 4,651,863` —
  identical to our stored cell, every field. Exactly one filing exists for that (sym,QE), so no
  revision ambiguity (T5 clear).
- **Groww matches us to 4 decimals** across all 5 quarters it shows (dii 21.1882 vs our 21.19,
  fii 17.1961 vs 17.20, mf 10.1062 vs 10.11). Independent, non-echo, exact.
- **Screener matches prom + FII + shareholder counts exactly** — 48/49 nsh values *exact to the
  person* (2,501,302 / 2,266,000 / 3,031,272 …). A structural check that we are reading the right
  filing for the right company and quarter; it cannot be luck.
- **Screener's DII runs ~0.08-0.11pp BELOW ours, systematically, in every modern quarter.** The
  filing and Groww both back OUR number, so this is Screener's bucket, not our defect — but it is
  logged as DEF_DIFF pending its mapping card, not waved away.
- **Trap T7 caught in the wild:** Groww's field literally named `otherDomesticInstitutions.insurance`
  is NOT insurance — it is *all non-MF DII* (10.1062 + 11.082 = 21.1882 = their DII exactly). Our
  `ins` (9.20) is the true `InsuranceCompaniesMember`. Mapping a plausible-looking field name
  straight across would have manufactured a 1.9pp "defect" on every stock.

## 5. ★ FINDING C — a real defect class, found and root-caused

**MCX has no Mar-2017 cell. It should.** Screener publishes it; BSE's `SHPQNewFormat` row for
qtrid 93 carries a real `XbrlFile`; fetching and parsing it with `parse_shp` UNMODIFIED yields
`fii 22.34, dii 36.51, mf 17.60, nsh 140,471` — **matching Screener exactly**. NSE's master has
no MCX row for that quarter at all (14 filings as-on 2017-03-31, MCX not among them).

Root cause candidate: that BSE row's **`filing_date_time` is null**, and our pipeline keys on a
submission date. Not yet proven as the general rule — `fetch_shp_bse_hist.py` is absent from the
checkout (§22f records it as never committed), so the 2016-19 ledger builder can't be re-read.

**Scale of the class: 2,344 internal holes across 680 symbols post-Jun-2016** — quarters *inside*
a symbol's own first→last span, so pre-IPO/post-delisting can't explain them. Concentrated in
Jun-2025 (162 symbols), Jun-2020 (141), Jun-2018 (105). Mostly outside the N500 (which is why the
§22f N500-scoped audit reported 1,706 and reads 99.9-100% for these eras) — but they are real
holes on real stock pages.

## 6. Revised phases

- **P3** — keep the 66-symbol frozen sample, but scope each site to the quarters it *has*: 7-site
  cross-check on Jun-2024→date; Screener/Trendlyne only for 2017→2024; and add the **BSE
  cross-exchange leg for the full Jun-2016→date span** as the primary independent source.
- **P4** — bulk sweep targets are **Screener** (cheap, server-rendered, symbol=slug) and the
  **BSE XBRL route** (deepest, and arbitrating). Tickertape/StockEdge APIs are cheap enough to
  sweep for the recent window as quorum filler. **Trendlyne is sample-only, never a full sweep** —
  at 10s/request its full overlap would run for weeks; spend its budget on the frozen 66 and on
  cells the exchanges disagree about. Drop Moneycontrol/ET/Groww from bulk — 4-5 quarters each
  adds nothing our other sources don't already cover.
- **Operational fix for P3/P4:** the seven P1 agents shared one scratch directory and collided on
  generic filenames (`reliance.html`, `parse.py`); `curl -o` silently overwrites where the Write
  tool would have refused. Every future site agent gets its OWN subdirectory, no exceptions.
- **P5** — arbitration ladder unchanged, but cross-exchange disagreement becomes the top-priority
  input queue rather than site-vs-us disagreement.
- **New P3b** — sweep the 2,344 internal holes against BSE `SHPQNewFormat`, gating on non-empty
  `XbrlFile` (§22f: `xbrlurl` is truthy when there is no file). Confirm or refute the
  null-`filing_date_time` root cause on a sample before writing any fetcher change.

## 7. Standing constraints (unchanged)

Rule 6b still governs what may be *taken*: nothing enters our data on one source's say-so.
What changed is only *which* sources can form the quorum — sites for the recent window,
the two exchanges for the deep window, and an explicit UNVERIFIABLE verdict for 2010-2015
rather than a comfortable silence.
