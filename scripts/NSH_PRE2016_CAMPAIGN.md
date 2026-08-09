# PRE-2016 SHAREHOLDER COUNTS — scoping document  (2026-08-09)

**Target:** the `nsh` (total number of shareholders) slot for **4,118 cells across 523 companies,
Dec-2010 → Mar-2016** — the only material shareholder-count gap left once the in-flight reparse
lands. These cells have correct FII/DII/promoter percentages (harvested from archived Moneycontrol,
runbook §22 / [[project-stocks-shp-wayback-2010]]); only the headcount is missing, because
Moneycontrol's pages never carried it.

**Status: NOT started. Route NOT yet established. Do not begin the expensive rung first.**

## 1. The measurement that decides the shape of this campaign

| | cells | share |
|---|---|---|
| **March (FY-end) quarters** | **725** | 18% |
| non-March quarters (Jun/Sep/Dec) | **3,393** | 82% |

By year: 2010 186 · 2011 979 · 2012 704 · 2013 899 · 2014 752 · 2015 598.
Worst companies: DLF 18, MTNL 18, UNITECH 17, NCC 16, GAMMONIND 16.

**This is the crux.** An annual report states shareholding distribution **as at 31 March only**, so
the annual-report route — the obvious one, and the one originally proposed — **can reach at most 18%
of the gap** even if it works perfectly on every company. Committing to a PDF-reading campaign
across 523 annual reports to fill 725 cells is a poor trade until the other rungs are ruled out.

## 2. Route ladder (§57) — walk it IN THIS ORDER, cheapest and widest first

**R1 — BSE's pre-XBRL shareholding pages. START HERE.**
Measured today: BSE's `SHPQNewFormat` list **does return rows for 2011-2015** (RELIANCE: 20 rows,
qtrid 70-89) but every one has an **empty `XbrlFile`** — consistent with §22f (real XBRL files start
Jun-2016). So the data exists on BSE for this era in some **HTML/aspx** form, not XBRL. Two hard
pieces of evidence that a usable route exists:
  - our own `shp_fill_thirdparty.json.gz` ledger carries the provenance string
    **`prom=BSE shpSecSummery_New Table-I`** — i.e. a BSE "shpSecSummery / Table-I" surface was
    already read successfully in a past backfill;
  - the qtrid numbering is BSE's own and is already known (Dec-2015 = 88, Mar-2016 = 89), so the
    per-quarter addressing is solved.
  Three endpoint shapes were probed today (`shpSecSummery/w`, `ShpSecSummery/w`,
  `shpPromoterNGroup/w` with `?scripcode=&qtrid=`) and all returned BSE's generic 1,814-byte error
  page — **the guesses were wrong, the route is not disproven.** Find it properly: open a
  2013-quarter SHP page in the browser pane, watch the network tab, and copy the request the page
  itself makes. **If R1 works it covers ALL FOUR quarters and the whole 4,118 — do not proceed to
  R2+ before settling it.**

**R2 — NSE archives.** NSE served shareholding patterns pre-2016 too. `feedback-nse-archive-first`
is the standing lesson: check the archive route before concluding anything is unreachable.

**R3 — Wayback of EXCHANGE pages.** §22f records that MC's pages were archived but BSE's aspx pages
"were never archived with old qtrids" — that was asserted for the percentage harvest; re-test it
for the count specifically before accepting it.

**R4 — Annual reports (March only, ≤725 cells).** The "Distribution of Shareholding" / "Shareholding
Pattern" schedule in the FY annual report states total holders as at 31 March. Sources: BSE/NSE
annual-report archives, company IR pages. Expensive: PDF fetch + table read per company-year, ~523
companies × up to 6 years. Only worth running for whatever R1-R3 leave behind, and only if the
March-only coverage is judged worth it.

**R5 — third-party.** Screener carries shareholder counts and matched us **to the person 48/49
times** (§22h) — but its floor is Mar-2017, so it cannot reach this era at all. Recorded so nobody
re-probes it.

## 3. Rules that still apply

- **Rule 6b (user mandate) governs every fill**: the exchange document plus ≥2 independent sources
  agreeing, and sites our data came from never count toward quorum. For this era that is hard —
  no aggregator reaches back — so in practice a fill needs **the exchange document itself plus an
  internal consistency check** (see below), and anything weaker stays open.
- **Sanity check available without any second source:** a shareholder count should move smoothly
  quarter to quarter. A value that jumps by an order of magnitude against its own neighbours is a
  scale/parse error — the same shape as the `nsh` write-time gate already in `save_hist`
  (`NSH_FLOOR_FRAC`) and the `shp_cell_fix.json` `accept` list for genuine collapses.
- **Never zero-fill or guess.** An absent count stays absent — `parse_shp` refuses unanchored
  filings for exactly this reason (§22b), and this campaign must inherit that discipline.
- Heals land via the ledger route + `_shp_merge_nsh.py` (fills slot 6 only, refuses to touch
  percentages, never adds cells), staged, never as a second writer against CI.

## 4. Recommended decision

Run **R1 route discovery only** — a ~1 hour job: find BSE's real pre-2016 SHP endpoint, pull three
companies × three quarters, and check whether a shareholder count is present on that surface at all.

That single answer decides everything:
- **count present** → a ~4,100-cell fill becomes a routine scripted backfill, worth doing;
- **count absent from BSE's pre-2016 surface** → the ceiling really is the 725 March cells via
  annual reports, and the honest recommendation is to leave 2010-2015 counts empty and say so on
  the page, rather than spend a PDF campaign on 18% of a nice-to-have field.

**Do not start R4 before R1 has answered.** The FII/DII campaign's own lesson: the expensive rung
is worth running only after the cheap ones are measured, and a route being unknown is not the same
as a route being absent ([[feedback-never-say-unfillable]]).
