# REV/PAT VERIFY CAMPAIGN — our revenue + net-profit vs 5-7 external sites  (planned 2026-08-09)

**Mission:** prove (or fix) every quarterly REVENUE and PAT value we publish, by comparing against
Screener, Trendlyne, StockEdge, Tickertape, Groww (+alternates: ET, Moneycontrol, marketsmojo),
and — the real check — the exchange filings themselves, with the same multi-source acceptance rule
that governed the SHP campaign: **a value is taken only when many independent sources match.**

**Run this in a FRESH session.** Prerequisites to read first, in order:
1. `scripts/SHP_VERIFY_REPORT.md` + runbook **§22h** — the sibling campaign's method AND its
   eleven tooling-defect lessons. Do not relearn them.
2. Runbook §2 (fundamentals backfill), §11 (results season), §40b (basis switch), §42 (BSE
   detres JSON), §45 (FY quarter-sum identity), §52-55 (PAT walls + insurer routes), §57 (route
   ladder), §58 (standard backfill read).
3. Memories: [[project-stocks-basis-switch]], [[feedback-negative-base-growth]],
   [[project-stocks-profit-basis]], [[feedback-nse-xbrl-per-basis]], [[project-stocks-nbfc-orfo]],
   [[feedback-xbrl-taxonomy-follows-submission]], [[project-stocks-stockview-comparison]],
   [[feedback-screener-annual-derivation]], [[project-stocks-con-copy-defect]].

## 0. WHAT WE HOLD (verify at session start against ORIGIN, never the checkout)

- `docs/sf_revop.json` — per symbol, per quarter (`YYYYMMDD` keys), 9-slot rows:
  `[rev_s, rev_c, op_s, op_c, other_inc_s, other_inc_c, flags, pat_s, pat_c]` — CONFIRM the slot
  layout from `build_quarterly_results.py` before any comparison; do not trust this note blindly.
- `docs/quarterly_results.json` — the RENDERED feed (what the site shows; scan THIS for gaps, not
  sf_revop — [[project-stocks-revop-gap-detection]]).
- `docs/sf_fundamentals.json` — annual + TTM derived; results_season.json — market pulse.
- Heal ledgers (route every fix through these, never edit derived JSONs): `scale_fix.json`,
  `feed_qe_fix.json`, `revop_fundamentals.json`, `ann_date_fills.json`, `_reattr_owners.json`.
- Existing reconcile tooling to REUSE: `cmp_trendlyne.py` / `tl_full_compare.py` (Trendlyne
  compare exists already — extend, don't rewrite), `revop_sanity.py`, `detect_con_copy.py`,
  `scripts/shp_verify_{mapcard,diff,quorum}.py` (site-agnostic by design — reuse with a new
  slot map).

Denominator for coverage: point-in-time N500 (`audit_shp_coverage.py` shows the join recipe).
Pin ONE origin/main SHA at session start; every agent compares against it.

## 1. WHY REV/PAT IS HARDER THAN SHP — the traps that will manufacture false defects

The SHP campaign's fields were one-dimensional percentages. Rev/PAT adds FOUR dimensions any one
of which silently misaligns a comparison:

- **T-A BASIS (the big one).** Every value is standalone OR consolidated; sites default
  differently (Screener shows /consolidated/ URLs separately; Trendlyne mixes; StockView showed
  STANDALONE profit YoY for banks — [[project-stocks-stockview-comparison]]). We store BOTH
  slots. Every comparison must declare its basis, and con-rev only starts ~2020, std ~2015
  (§40b). A "mismatch" that is std-vs-con is a mapping error, not a defect. detect_con_copy.py
  exists because our own con slot sometimes held a COPY of std — the sites' equivalent defect
  exists too.
- **T-B PROFIT DEFINITION.** Ours = OWNERS-ATTRIBUTABLE PAT — VERIFIED in code 2026-08-09, not
  just remembered: CI's refresh-fundamentals.yml runs `apply_owners_full.py` on EVERY run
  ("re-assert owners-attributable consolidated profit"; the GLAXO case: total −157 vs owners
  +16.85), and **the backtest engine consumes exactly this data** (backtest-engine.js `FUND` =
  `[qEnd, npStd, annStd, npCon, annCon]` → profitYoyPct/TTM/streak factors). So a wrong PAT
  propagates into BACKTEST PICKS, not just the stock page — treat PAT defects as a higher
  severity class than revenue ones. One engine subtlety the comparison must mirror:
  `profitAt(..., basis)` FALLS BACK con→std when the con slot is empty (`tries=[[3,4],[1,2]]`) —
  so for backtest-impact purposes, the value to verify for a con-empty quarter is the STD one.
  Sites vary in definition and rarely say which. Arithmetic fingerprints: delta == NCI line for
  total-vs-owners; delta == exceptional items for pre/post-EI. Derive each site's definition the
  mapcard way (which of their numbers reproduces ours) before judging anything.
- **T-C SCALE.** ₹ cr vs ₹ lakh vs ₹ mn; sites print cr to 0-2dp. Our own power-of-ten defects
  route through scale_fix.json ([[project-stocks-xbrl-scale-errors]]) — a 10x/100x delta is a
  scale artefact until proven otherwise, on EITHER side.
- **T-D PERIOD SEMANTICS.** Q4 = FY-total minus 9M for many filers; sites sometimes show a YTD
  as "the quarter" ([[feedback-partial-header-lies-about-periods]]); banks/insurers have
  different line items entirely (NBFC other-revenue sits INSIDE revenue — the ORFO rule;
  insurers are IRDAI-format, §3/§43/§55). The §45 FY quarter-sum identity (Q1+Q2+Q3+Q4 == FY)
  is the arbitration weapon: it proves WHICH side of a mismatch is wrong without any site.
- **T-E RESTATEMENTS.** Companies re-file; comparative columns in later filings differ
  legitimately from the original quarter ([[feedback-backfill-comparative-columns]]). A site
  showing the RESTATED number vs our AS-FILED one is DEF_DIFF, not a defect — the con-copy
  re-adjudication found exactly this class ([[project-stocks-con-copy-defect]]).

## 2. PHASES (same skeleton as SHP — reuse its tools, its lessons, and rule 6b verbatim)

**P0 — coverage audit** (fresh session, ~30 min): member-quarters vs point-in-time N500 for
rev_s/rev_c/pat_s/pat_c separately, per era. We already know the walls: con-PAT pre-2020 is
structurally absent (2.7% ceiling, MEASURED — §53); do NOT re-litigate it, cite it.

**P1 — recon, 5-7 sites** (parallel Sonnet, one per site, OWN scratch subdirectory each):
capability card per site — depth, basis labelling, which of std/con it shows, PAT definition
hints, machine route, and the two questions that killed sites last time: history depth and
anti-bot posture. Known starts: Screener /company/<SYM>/ + /consolidated/ (server-rendered,
symbol=slug — the deepest cheap source last time, ~10y annual + 12 qtrs); Trendlyne (10s
crawl-delay for ClaudeBot BY NAME — honour it, sample-only); StockEdge anonymous API; Tickertape
income-statement API (sid via sitemap; TRU-style identity traps PROVEN — exact ticker or skip);
Groww __NEXT_DATA__. Also probe: does any site carry pre-2015 quarterly P&L? (Expect no — but
measure; that era's exchange route is §52's detres + NSE archives.)
**Exchange leg from day one** (the SHP redesign, pre-made): NSE per-basis XBRL
([[feedback-nse-xbrl-per-basis]]) + BSE detres JSON (§42) — independent of every site AND of our
own stored values.

**P2 — calibration pilot, gates everything** (Opus): ~14 trap-stocks x all sites, mapping cards
derived ARITHMETICALLY (`shp_verify_mapcard.py` pattern — a site's "Net Profit" label proves
nothing, find which of their numbers SUMS/maps to ours). Pilot must include: RELIANCE (baseline),
a bank (SBIN/HDFCBANK — T-A bites hardest), an insurer (SBILIFE — IRDAI format), an NBFC
(BAJFINANCE — ORFO), a big-NCI conglomerate (T-B fingerprint, e.g. TATASTEEL/GRASIM), a
loss-maker (T-C sign handling + [[feedback-negative-base-growth]]), a heavy restater, a renamed
ticker (ETERNAL), a demerger (MOTHERSON/MSUMI), GICRE ([[project-stocks-gicre-conpat-fixed]] —
its standalone cells are still suspect), and a recent IPO. Per site per basis: which slot
(std/con) their table equals, PAT definition, scale, Q4 convention. NO verdicts before cards.

**P3 — stratified audit** (~60-70 symbols, parallel Sonnet by site + the exchange leg): frozen
deterministic sample (md5 draw, the SHP strata script generalises), every overlapping quarter,
BOTH bases separately — plus a third derived series, **"backtest-effective PAT"** (con with
std-fallback per quarter, exactly the engine's `tries=[[3,4],[1,2]]` rule), because that is the
number strategy picks actually consume; a defect invisible in either pure basis can still flip a
backtest factor at the fallback boundary. Verdicts via `shp_verify_diff.py` with a rev/pat slot map + per-basis
tolerance (suggest: MATCH within 0.5% relative or ₹0.5cr absolute, whichever is larger —
2dp-crore rounding differs across sites; calibrate in P2 and freeze).

**P4 — bulk sweep** (background, resumable, breadth-first): Screener full universe (server-side,
proven ~25 syms/min), StockEdge/Tickertape APIs. Trendlyne SAMPLE-ONLY (10s). Progress files +
append-only JSONL + one agent per site — all proven infrastructure; and tell agents to run
foreground fetch loops, not to babysit monitors (they idled for an hour last time).

**P5 — quorum + arbitration**: `shp_verify_quorum.py` with per-field tolerances; every
CONTRADICTED/SITES_DISAGREE cell walked up the ladder: NSE per-basis XBRL → BSE detres →
the filing PDF (announcement route, §58 column-anchor read) → §45 quarter-sum identity when
documents conflict. Site majority never decides (they all copy the same aggregator upstreams).
PROVENANCE ECHO applies: any cell healed FROM screener/trendlyne historically (check
revop_fundamentals.json src tags + backfill ledgers) can't be verified by that site.

**P6 — heal + report**: fixes ONLY via the ledgers (scale_fix / feed_qe_fix /
revop_fundamentals + rebuild via CI-owned builders); rule 6b quorum recorded per heal; nightly
jobs then RE-RUN and DIFF ([[feedback-journalled-is-not-live]] — committed heals get silently
undone; verify LIVE ~20 min after push, then again after the next nightly). Report =
`scripts/REVPAT_VERIFY_REPORT.md` + runbook section + memory update.

## 3. EXECUTION NOTES FOR THE NEW SESSION

- Budget: SHP ran P1→report in one day with ~7 parallel Sonnet agents per phase + Opus for
  calibration/arbitration. Rev/PAT doubles the field count (2 bases × rev/op/PAT) — expect P2
  and P5 to be the long poles, not the sweeps.
- Concurrency: worktree for all scripted work (`~/stocks-wt/revpat-verify`); import shared
  parsers from a tree AT origin/main (the stale-checkout phantom-bug lesson, §22h); never a
  second writer against CI's nightly rebuilds; check `git status` before every commit.
- The two known OPEN items from the SHP campaign that overlap this one: LICI std-slot defects
  pending approval ([[project-stocks-stdcon-audit]]) and GICRE standalone pollution — fold their
  verification into P3 rather than treating them separately.
- Do NOT start heals until the SHP campaign's own pending heal (nsh reparse) has landed and been
  live-verified — one staged writer at a time against shp/rev pipelines.
