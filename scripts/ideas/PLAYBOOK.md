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
  3. `python3 scripts/ideas/dossier.py <scrip> [<scrip> ...]` for the names chosen for research (see triage)
  4. Research and write the ideas (this document, sections below) → append to `docs/ideas/ideas.json`
  5. `python3 scripts/ideas/score.py` → `docs/ideas/track.json`
  6. Write `docs/ideas/latest.json`, commit the touched files with explicit paths, push via a `claude/ideas-<ts>`
     branch, `gh pr create`, `gh pr merge --squash --delete-branch --admin`. A PR left unmerged must be reported.
- Publish only what clears the bar. A day with zero ideas is a valid day: still commit the scan, the scorecard and
  `latest.json` so the page shows the run happened.

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
- Do not touch any file outside `docs/ideas/`, `docs/ideas.html` and `scripts/ideas/`. Never `git add -A`.
