# Daily Ideas routine: playbook

This is the operating manual the scheduled research run follows. It encodes a specific research style
(reverse-engineered from ~2,850 posts of a data-first Indian micro-cap investor) on top of this repo's
standing rules: never assume, never guess; every number traces to a source read in the run; "unknown" is a
valid answer; nothing is investment advice.

## Universe and cadence

- Universe: `docs/ideas/universe.json`, all active BSE equities with market cap ₹200-2,000 cr (about 1,180 names,
  BSE SME included, suspended groups excluded). Rebuilt every run by `scripts/ideas/universe.py`.
- One run per trading day after BSE has published the bhavcopy (evening IST). Steps, in order:
  1. `python3 scripts/ideas/universe.py`
  2. `python3 scripts/ideas/scan.py` → `docs/ideas/scan/<date>.json` (candidates = score ≥ 4)
  2b. Commodity panels (the price-driven half of the method, see "Price-driven ideas" below):
      `python3 scripts/ideas/spot.py` (daily spot) · `python3 scripts/ideas/wpi.py` (WPI items, monthly) ·
      `python3 scripts/ideas/trade.py --latest` (HS trade panel; no-op unless the ministry published a new month) ·
      `python3 scripts/ideas/signals.py` → `docs/ideas/signals.json` (commodity groups ranked by strength, each with
      the listed beneficiaries and sufferers from `docs/ideas/commodity_map.json`)
  3. `python3 scripts/ideas/dossier.py <scrip> [<scrip> ...]` for the names chosen for research (see triage)
  4. Research and write the ideas (this document, sections below) → append to `docs/ideas/ideas.json`
  5. `python3 scripts/ideas/score.py` → `docs/ideas/track.json`
  6. Write `docs/ideas/latest.json`, commit the touched files with explicit paths, push via a `claude/ideas-<ts>`
     branch, `gh pr create`, `gh pr merge --squash --delete-branch --admin`. A PR left unmerged must be reported.
- Publish only what clears the bar. A day with zero ideas is a valid day: still commit the scan, the scorecard and
  `latest.json` so the page shows the run happened.

## Price-driven ideas: the raw-material / product-price route (added 2026-09-22 at the user's request)

The pattern to catch: a commodity or product price starts rising → the purest listed producer (or the converter that
passes the price through and revalues inventory) re-rates months later. Past instances the user pointed to: graphite
electrode prices ~10× in 2017-18 (HEG ~30×, Graphite India ~10×), phenol/acetone spreads in 2020-21 (Deepak Nitrite),
LME copper at records in 2026 (copper-scrap recyclers Sunlite/Parmeshwar called on pass-through + inventory gains),
US refrigerant quota cuts (Stallion), whey prices (Parag), tungsten (Diffusion), gherkin export prices (Freshara).

Every run, BEFORE the filings triage, read `docs/ideas/signals.json` (groups sorted by |strength|; 1.0 = at threshold):
1. Take every group with strength ≥ 1.5 and direction "up" (and ≥ 2.0 "down" for sufferers of a falling input). Read
   its `evidence` lines and its `sources`: spot (daily, LME/global), WPI (domestic wholesale index, monthly), trade
   (India import/export unit price per HS prefix, monthly; `value_usd_mn`, `codes`, `dropped` = codes left out as a
   different price class). Trust a signal more when two sources agree (e.g. spot AND import unit price) and when the
   quantity is not collapsing (`qty_12m`). Treat a lone 1-month trade jump as noise.
2. For each such group, research the mapped `benefit` names that are in the ₹200-2,000 cr universe first (the map also
   lists larger names for context; they can be mentioned as the reference producer but the idea must be a small cap).
   Build the dossier as usual and answer, from the company's own documents: (a) what share of revenue is the product
   whose price moved (segment note, annual report product table, investor deck); (b) is pricing pass-through, formula
   or fixed-contract, and with what lag; (c) does the company hold inventory that revalues (recyclers, traders,
   converters) or sell forward; (d) capacity and utilisation, and whether volumes can rise; (e) who are the peers and
   which is the purest exposure. Then the model: (price change × exposed revenue × pass-through) → incremental EBITDA →
   forward PE vs peers, exactly as in the research standard below.
3. Publish only if the exposure is documented (a stated product mix or segment) — never on the group name alone. If the
   company's documents do not show the exposure, write it up in the run log as "mapped but exposure unverified".
4. Sufferers: when an input price rises for a converter without pass-through, note it as a red flag on any idea in that
   sector, and as a "watch" line in the run log; do not publish short ideas.
5. If `signals.json` shows a strong move for which the map lists no small-cap name, search the universe for a producer
   (`docs/ideas/universe.json` names + `docs/search_index.json` industry) and, if found, ADD it to
   `docs/ideas/commodity_map.json` with a one-line `why` so tomorrow's run starts from it. Keep the map honest: only
   add a name whose exposure you read in a document, and say which.

## Triage: which candidates to research (spend the deep work on at most 3-4 names)

Rank the scan's candidates by, in this order:
1. A hard operational trigger from the company itself: order win with a value, capex or new capacity with a number,
   commercial production, credit-rating upgrade or a rating report that states revenue expectations, a results filing
   with revenue growth ≥ 30% YoY and PAT growth.
2. A volume breakout (≥ 3× 60-day median, closing at the window high) on a name with ≥ ₹25 lakh median daily turnover,
   only if there is also some filing or result to explain it. Pure price moves with no filing are watch-listed, not researched.
3. Names already published are re-examined only if a new hard trigger arrives (write an update, not a new idea).
Skip: fund raises without a stated use, clarifications on price movement, promoter or director changes, anything
where the trigger is only a press release with no number.

## Research standard for one idea (what "deep research" means here)

Read primary documents first, in this order, and cite each one you used: the triggering filing PDF, the latest
investor presentation, the latest concall transcript, the annual report (MD&A, capacity, related-party notes,
auditor remarks), the last 8 quarters of results, the shareholding pattern (promoter %, pledge, changes),
corporate actions, and the adjusted price history (`dossier.py` assembles all of this).

Then build the idea in this exact structure. Each field is mandatory; write "unknown" where the documents do not say.

1. **Trigger** (one line): the filing, number and date that put the name on the list.
2. **What the business physically does**: product, capacity, customers, share of revenue by segment, the one physical
   unit that drives revenue (MW, tonnes, pumps, km of cable, stores, flights, GPUs, jars).
3. **Physical revenue model**: current capacity or throughput × utilisation × price per unit, or a shipment/order/head-count
   series × a stated conversion constant, giving revenue for the current and next financial year. Show the arithmetic.
   Cross-check against management guidance and the credit-rating report; state where they differ.
4. **Earnings and valuation**: assumed PAT margin (justify with the last 4 quarters), EPS one and two years out, one-year
   and two-year forward PE at the last close, and the trailing/forward PE of 2-4 listed peers from the same industry
   (use the dossier's peer list; look up PE from BSE's company header). State the re-rating case in one sentence.
5. **Balance sheet and cash**: debt, recent loans or limits (a new bank sanction is a positive signal of vetted revenue),
   receivable days, capex funding, dilution (preferential, warrants, QIP) and who got it.
6. **Ownership**: promoter holding, pledge, recent buying or selling, institutional entry, any well-known investor.
7. **Red flags checklist** (answer each): related-party loans or purchases; promoter salary versus profit; auditor
   remarks or changes; revenue claims that shipments or head-count do not support; orders claimed but not visible on
   tender portals; preferential issue below market price; pledge; GST or regulatory notices; big one-off items.
8. **Still to verify on the ground**: the two or three facts that only a site visit, a dealer or customer call, or a
   subscription dataset (company-level import/export shipments, EPFO head-count) could confirm. Say which.
9. **Unknowns**: everything the documents did not answer.
10. **Confidence**: high / medium / low, with the single biggest reason.
11. **Sources**: title and URL of every document read. No source, no claim.

Style: plain sentences, numbers in tables or on their own lines, no hype words, no price targets, no "multibagger".
Length: 400-900 words per idea. The reader is a careful investor, not the general public.

## The conversion constants the style relies on (use only when the dossier gives the inputs)

- Solar module maker: 1 crore imported cells ≈ ₹190-220 cr of module sales; 1 MW of modules ≈ ₹2 cr (DCR) or ₹1.3 cr (non-DCR).
- Solar cell maker: 1 wafer ≈ ₹100 of cell revenue; 1 MW of cells ≈ ₹1-1.3 cr.
- Solar EPC: 1 MW ≈ ₹4-4.5 cr revenue (end-to-end), ₹3-3.25 cr without modules.
- Solar pumps: 1 pump ≈ ₹3-3.5 lakh of revenue.
- Labour-heavy service businesses: ₹1 of PF expense ≈ ₹90-130 of sales; growth in EPFO head-count leads revenue by 1-2 quarters.
- Cables: revenue per km of installed capacity grows 4-7% a year; utilisation 50-55% is typical for a growing SME.
- Recyclers of copper: pass-through pricing, inventory gains in a rising copper cycle, margins stay in a narrow band.
These are working constants, not facts about any particular company; always prefer a constant the company itself discloses.

## Idea JSON record (append to `docs/ideas/ideas.json` → `ideas[]`)

```json
{"id": "2026-09-21-HSIL", "date": "2026-09-21", "scrip": "543916", "ticker": "HSIL", "nse": "", "name": "Hemant Surgical Industries Ltd",
 "industry": "Medical Equipment & Supplies", "mcap": 855, "sme": true, "call_date": "2026-09-21", "call_close": 285.6,
 "trigger": "…", "thesis": "markdown", "model": "markdown", "valuation": "markdown", "red_flags": "markdown",
 "verify": "markdown", "unknowns": "markdown", "confidence": "medium: …",
 "sources": [{"title": "…", "url": "https://…"}]}
```
`call_date` is the scan date; `call_close` is that day's BSE close from the scan file. The scorecard measures from there.

## Hard rules

- Numbers only from documents read in this run or from the exchange data files the scripts produced. If a filing is
  image-only and cannot be read, say so and do not invent the number.
- Never publish a name on a filing you did not open.
- Keep the disclaimer on the page. No buy/sell language, no targets.
- Do not touch any file outside `docs/ideas/`, `docs/ideas.html`, `docs/commodities.html` and `scripts/ideas/`. Never `git add -A`.
- The commodity panels (`docs/ideas/spot*.json`, `wpi.json.gz`, `trade/**`, `signals.json`) are committed by the run only when they changed; never hand-edit them — fix the builder.
