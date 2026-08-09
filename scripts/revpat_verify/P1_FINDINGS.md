# P1 — SITE RECON FOR QUARTERLY REV/PAT
Five sites probed in parallel, one agent each, own scratch subdirectory, read-only, robots-honoured,
≥2s between requests (Trendlyne 10s). Per-site cards: `revpat/<site>/capability_card.md`.

---

## 1. THE HEADLINE: the multi-site quorum window is ~10 quarters wide, not 95

| site | **quarterly** depth | oldest quarter | bases offered | quarterly PAT attribution | identity anchor | status |
|---|---|---|---|---|---|---|
| **Screener** | **13 qtrs** | **Jun-2023** | std (bare URL) **+ con** (`/consolidated/`) | no NCI line at all — **unknown** | slug = NSE symbol | ✅ clean, 0 blocks |
| **Tickertape** | 10 qtrs | Mar-2024 | **consolidated only** (`reporting` field) | single PAT, no NCI split | **ISIN** + ticker echo | ✅ via `www` SSR only |
| **Groww** | 5 qtrs | Jun-2025 | both in payload | none in the schema | **ISIN** + `nseScriptCode` | ✅ clean |
| **StockEdge** | **none** — annual only (5 rows) | — | standalone only | `Minority_interest` exists, but annual | name-fuzzy search | ❌ unusable for quarterly |
| **Trendlyne** | — | — | — | — | numeric site id | ❌ **403 on request #1** |

Our data spans **95 quarters, Dec-2002 → Jun-2026**. The deepest site reaches **13** of them.

**Consequence for rule 6b** (*exchange filing AND ≥2 independent sites must agree*):

| window | sites able to speak | 6b satisfiable? |
|---|---|---|
| Jun-2025 → Jun-2026 (5 qtrs) | Screener, Tickertape, Groww | ✅ 3 sites |
| Mar-2024 → Mar-2025 (5 qtrs) | Screener, Tickertape | ✅ 2 sites (bare minimum) |
| Jun-2023 → Dec-2023 (3 qtrs) | Screener only | ❌ one site is not a quorum |
| **Dec-2002 → Mar-2023 (~82 qtrs)** | **none** | ❌ **exchange leg is the only check** |

So **~86% of our member-quarters cannot be site-verified at all**, and site quorum is possible on
roughly the last 10. This independently reproduces the SHP campaign's central structural finding
("no site has pre-2010 data… cross-EXCHANGE is the only real check for the deep era") — arrived at
from a different direction, with different sites, on different fields.

**This is not a reason to weaken rule 6b.** It is the measurement that says the exchange leg
(NSE per-basis XBRL + BSE detres) carries this campaign, and the sites are a corroborating rung on
a narrow recent window.

## 2. ★★★ NOT ONE SITE PUBLISHES THE ATTRIBUTION OUR PAT USES

Our PAT is **owners-attributable**, re-asserted by CI on every run (`apply_owners_full.py`).

- Screener: consolidated "Net Profit" — **no minority-interest line anywhere** on the quarterly table.
- Tickertape: exactly one PAT field (`qIncNinc`) — no owners/NCI split.
- Groww: fixed 6-item template — attribution "undefined by the schema itself".
- StockEdge: *does* expose `Minority_interest` / `Share_Associate` — but only on ANNUAL rows.

**Therefore T-B (profit definition) cannot be settled by reading any site.** It must be derived
arithmetically per site (the mapping card) and, where a site's consolidated PAT shows a persistent
one-sided bias against ours, arbitrated at the filing. A site agreeing with us on a no-NCI company
proves nothing about a big-NCI one — which is exactly why the P2 pilot carries TATASTEEL/GRASIM.

## 3. ★★★ "THE NAME LIES" — three fresh instances, on three different sites

The SHP campaign's founding lesson was Groww's `otherDomesticInstitutions.insurance`, which was not
insurance. This recon found three more without looking for them:

1. **StockEdge `Consolidated_NetProfit` is not consolidated.** On standalone rows it is
   arithmetically `Profit_after_tax + extra_items` — verified exactly on RELIANCE FY2023
   (43,002 + 1,188 = 44,190). A campaign that mapped it by name would have compared our consolidated
   PAT against a standalone-plus-exceptionals figure.
2. **Tickertape sid `TRU` is Trust Fintech**, not our TRU (Trucap Finance, reachable only via sid
   `DHA`, whose ticker echoes `TRU`). Reproduced live.
3. **StockEdge's ticker search is name-fuzzy, not exact** — `term=IEL` returns *Gabriel India* as
   result #1; the real IEL Ltd is result #6 of 26. A first-result bulk job mis-maps every lookup.

Plus a fourth, of a different kind: **Groww's `financialSummary` narrative text is ~2 years stale**
(quotes a Jun-2023 period beside numeric arrays running Jun-2025→Jun-2026; for TCS it describes a
six-month period as if it were the quarter — a live instance of trap T-D).

**Identity rule adopted for every later phase:** exact ticker match confirmed from the payload
(ISIN where available — Tickertape and Groww both carry one), else unambiguous full name, else SKIP.

## 4. ★ A ROBOTS-COMPLIANCE CORRECTION TO THE SIBLING CAMPAIGN

`api.tickertape.in` and `quotes-api.tickertape.in` both return **`Disallow: /` for all agents.**
The Tickertape agent fetched those robots.txt files directly, found the block, and pivoted to the
allowed `www.tickertape.in` SSR route (`__NEXT_DATA__`), which serves the identical data.

It notes the sibling SHP recon carried a contrary note that appears never to have fetched that
robots.txt. If the SHP campaign used the API host, **§22h should be corrected** — the finding stands
either way, but the route it used may not have been compliant. Flagged for the runbook, not silently
fixed. This campaign uses the `www` SSR route only.

## 5. Per-site route notes (working, measured)

- **Screener** — `https://www.screener.in/company/<SYM>/` is **always standalone**, never
  "whichever they report"; `/consolidated/` is the con view. Basis is declared in page text
  ("Standalone Figures in Rs. Crores"), so it can be captured rather than inferred. Units ₹ crore,
  whole numbers. Renamed tickers fail **loud** (404, no redirect: `IBULHSGFIN`→404,
  `SAMMAANCAP`→200) — a good property. Bank template differs (Revenue/Interest/Financing Profit…),
  so labels must be kept verbatim. `/company/source/quarter/*` is robots-disallowed (not fetched).
  Measured concretely: RELIANCE Jun-2023 std Sales 122,627 / NP 9,627 vs con 207,559 / 18,258 —
  a ~40-50% understatement if the basis is taken wrong. That is trap T-A with a number on it.
- **Tickertape** — sid via the allowed sitemap; page `__NEXT_DATA__` →
  `props.pageProps['income-normal-interim']`. Fields close arithmetically
  (`qIncTrev`→`qIncOpe`→`qIncEbi`→`qIncPbi`→`qIncPbt`→`qIncToi`→`qIncNinc`, verified through a loss
  quarter). Units ₹ crore. Annual reaches FY2017; quarterly is the shallow one.
- **Groww** — `__NEXT_DATA__` → `stockFinancialData.statements[0]`, both
  `consolidatedQuarterly` and `standaloneQuarterly` populated. Exactly 5 periods, same window for
  every company. No unit field anywhere — ₹ crore inferred by magnitude, never declared.
- **StockEdge** — anonymous API is real (`api.stockedge.com`, robots `allow: /Api`) but serves only
  `GetProfitLossDisplaySet/{id}`: annual, 5 rows, `"Type":"S"` standalone on every row, and the
  basis parameter is **silently ignored** rather than erroring. The Fundamentals tab redirects
  anonymous visitors to OIDC login; the agent stopped rather than authenticate. **Excluded from the
  quarterly quorum.**
- **Trendlyne** — HTTP 403 (CloudFront WAF) on the very first request, `robots.txt` itself. Stopped
  immediately per the rules. Notable: this repo's own SHP recon logged 10/10 successful fetches the
  *same day*, ~3 hours earlier — so the block is session/IP-dependent, not permanent.
  **Do not trust a prior session's "site is open" as still true; re-probe fresh.**

## 6. What P1 changes about the plan
1. **The exchange leg is not "one rung of five" — it is the campaign.** Sites corroborate ~10 recent
   quarters; they cannot speak to the other ~85.
2. **StockEdge drops out** of the quorum for quarterly work (annual-only, standalone-only).
   Trendlyne is unavailable this session. The usable set is **Screener, Tickertape, Groww**.
3. **Rule 6b needs an explicit verdict class for "quorum impossible"** — a cell in the pre-2023 era
   is not *unverified because we did not look*; it is *unverifiable by site, by measurement*, and the
   exchange filing is the whole of its evidence. Reporting those as "unchecked" would misrepresent
   both the effort and the risk.
4. **T-B is arbitration-only.** No site exposes owners-vs-total for a quarter.
