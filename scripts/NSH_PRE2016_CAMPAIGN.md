# PRE-2016 SHAREHOLDER COUNTS — scoping document  (2026-08-09)

**Target:** the `nsh` (total number of shareholders) slot for **4,118 cells across 523 companies,
Dec-2010 → Mar-2016** — the only material shareholder-count gap left once the in-flight reparse
lands. These cells have correct FII/DII/promoter percentages (harvested from archived Moneycontrol,
runbook §22 / [[project-stocks-shp-wayback-2010]]); only the headcount is missing, because
Moneycontrol's pages never carried it.

**Status: route RESOLVED 2026-08-09 — the archived Moneycontrol pages carry the count (R0 below),
and a side-ledger is being produced by the sibling coverage session at zero extra fetch cost.
The annual-report rung is ON HOLD and may never be needed.**

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

## 2. Route ladder — RESOLVED 2026-08-09 by the sibling coverage session

**★ R0 — THE ARCHIVED MONEYCONTROL CLAUSE-35 TABLES CARRY THE COUNT. This is the answer.**
Confirmed with a worked example by the FII/DII coverage session (which is harvesting ~5,500 of
these pages anyway): the **first numeric column of every row is the holder count**, and the
grand-total row gives the whole-company figure. SUNDARMFIN Sep-2016 (Wayback 20161113075427,
MC qtrid 91):

    Total shareholding of Promoter and Promoter Group (A)  ->     117 holders
    Total Public shareholding (B)                          ->  22,109 holders
    Total (A)+(B)+(C)                                      ->  22,227 holders   <- this is nsh

So the whole 4,118-cell gap is addressable from pages already being downloaded, at **zero extra
fetches** — a second pass over the gzipped page cache (`scripts/_shp_wb_cache/pages`), not a
refetch. Output will be a side-ledger keyed (sym, QE) carrying the **count only, no percentages**,
which merges through `_shp_merge_nsh.py` (slot 6 only) at zero risk to anything else.

**This supersedes the pessimistic framing in §1 below.** The "18% ceiling" applies only to the
annual-report route; the wide route exists and is nearly free. **Do not spend anything on the
annual-report rung until the side-ledger's real yield is known.**

**R1 — BSE's pre-XBRL endpoint: FOUND, and MEASURED EMPTY for this era. Route CLOSED.**
The endpoint I failed to guess is:

    https://api.bseindia.com/BseIndiaAPI/api/shpSecSummery_New/w?scripcode=<code>&qtrid=<qtrid>.00

(found by grepping BSE's Angular bundle — a general technique worth reusing: ~32 SHP endpoints
live in there, including `Corp_shpSec_SHPPubShold_ng` for the Table-III breakdown). It is alive and
useful for the modern era — RELIANCE at qtrid 112 returns 10,761 bytes with promoter 50.61, and it
was used today to anchor promoter/public/npnp on ~20 cells where the XBRL 404s. But tested at
**qtrid 85 (Mar-2015), 80 (Dec-2013) and 70 (Jun-2011) it returns ~505-byte stubs** against 10,761
for the control. The era is genuinely empty behind it — the same story as the empty `XbrlFile`.
**A measured negative, not an undiscovered URL.** Do not re-probe this.
(Caveat recorded: `Corp_shpSec_SHPPubShold_ng` returns an empty category skeleton even on a
known-good control, so it needs a different key — `shpDecleraction` hands back a "Mid" that may be
it. Unchased.)

**R2 — NSE archives.** Unprobed. Only relevant for whatever R0 leaves behind.

**R3 — Wayback of EXCHANGE pages.** Unprobed; §22f's "never archived with old qtrids" claim was
made about percentages. Only relevant for R0's residue.

**R4 — Annual reports (March only, <=725 cells).** The expensive rung. **HOLD.** Reachable ceiling
is 18% of the gap and R0 looks likely to beat it outright.

**R5 — third-party: DEAD for this era.** Screener carries counts and matched us to the person 48/49
times (§22h), but its floor is Mar-2017. Recorded so nobody re-probes it.

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

## 4. Decision (settled)

**Wait for the R0 side-ledger, then merge it with `_shp_merge_nsh.py` and measure the residue.**
Only what R0 cannot reach is a candidate for R2/R3, and R4 (annual reports) stays on hold
indefinitely — it can never beat 18%.

Two lessons this scoping earned, both worth keeping:
- **The expensive rung was nearly chosen first.** Annual reports were the obvious route and would
  have capped at 18% of the gap for a 523-company PDF campaign. Measuring the March/non-March
  split *before* planning is what caught it.
- **"I could not find the endpoint" is not "the endpoint does not exist", and neither is the end of
  the story.** Three wrong URL guesses proved nothing; the real endpoint was found by grepping the
  site's own JS bundle, and only THEN did a proper test show the era is genuinely empty behind it.
  A measured negative closes a route; a failed guess does not ([[feedback-never-say-unfillable]]).
