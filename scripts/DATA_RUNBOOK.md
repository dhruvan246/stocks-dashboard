# STOCKSWORLD — DATA RUNBOOK  ★ read this FIRST before any data work ★

The canonical *do-exactly-this* guide for fetching / refreshing / backfilling / building the
data. **Future session: follow these steps, don't re-explore.** Indexed in `MEMORY.md` so it
loads every session. (README.md is just a short pointer here — this file is the real doc.)

---

## TABLE OF CONTENTS
- **§0** GOLDEN RULES
- **§1** DAILY PRICE REFRESH
- **§2** FUNDAMENTALS BACKFILL
- **§3** INSURERS
- **§4** BUILD & DEPLOY
- **§5** PENDING QUEUE
- **§6** HISTORICAL MULTI-AGENT BACKFILL
- **§7** STRATEGY FINDER — find the best strategy over a window
- **§8** F&O MEMBERSHIP
- **§9** PAGE-LOAD PERFORMANCE
- **§10** SECTOR / INDUSTRY INDEX BROWSER
- **§11** RESULTS SEASON CHART
- **§12** 15:30 FILING-TIME GATE
- **§13** HOME PAGE
- **§14** CORPORATE ANNOUNCEMENTS BROWSER
- **§15** QUARTERLY RESULTS DASHBOARD
- **§16** DISCOVERY BUCKETS
- **§17** BSE-ONLY STOCK COVERAGE
- **§17b** VISION-FILL = CLOUD ROUTINE 4×/day (13:30/16:30/20:30/23:30 IST)
- **§18** DATA HEALTH MONITORING + COMMIT GUARDS
- **§19** SITE FEATURES ON SUPABASE
- **§20** RESULTS COVERAGE DASHBOARD
- **§21** MARKET BREADTH
- **§22** FII/DII HOLDINGS PER STOCK  (22h = verification vs external sites + cross-exchange, 2026-08-09;
  **22i = the swallowed foreign block — 162 stored fii=0.0 cells healed, 2026-08-12**)
- **§23** BULK & BLOCK DEALS
- **§24** INSIDER TRADES
- **§25** NEW-LISTING (IPO) YEAR-AGO BASE BACKFILL
- **§26** DELIVERY SPIKES
- **§27** IPOs & LISTINGS
- **§28** INDEX MONTHLY RETURNS
- **§29** EX-DATES CALENDAR
- **§30** TICKER RENAME with orphaned history
- **§31** NIGHTLY TRENDLYNE RECONCILE
- **§32** HISTORICAL INDEX/F&O MEMBERSHIP from WAYBACK
- **§33** MACRO DASHBOARD
- **§34** PAGE GROUPS — merged tabbed sections (theme.js)
- **§35** VOLUME SHOCKERS
- **§35** GLOBAL MARKETS DASHBOARD
- **§36** LIVE (INTRADAY) DATA LAYER
- **§37** INDIAN INDICES TABLE
- **§38** CONCURRENCY — ONE WRITER PER TREE
- **§39** ★ SHIP-IT QUALITY GATE — nothing goes out unverified (**read before ANY UI / design / feature work**)
- **§40** STOCK PAGE = PER-STOCK SLICES · **§40b** ★ REPORTING BASIS — one basis per comparison
- **§41** ★ PUBLISHING A DATA HEAL — "live on the server" ≠ "the site uses it" (**read before ANY heal / backfill**)
- **§42–§58** ROUTE & SOURCE DISCOVERIES (detres JSON, FY identity, pre-2020 std/con ceilings, CI clobber, the ROUTE LADDER, the STANDARD BACKFILL READ)
- **§70** ★★★ sf_fundamentals vs sf_revop DISAGREE — authority is fundamentals; the mirror is not rendered
- **§71** ★★★ THE ADJUDICATION THAT WAS ABANDONED — when your "truth" source is the corrupted one
- **§72** ★★★ VERIFYING REV/PAT vs EXTERNAL SITES — sites reach 10 of 95 quarters; con PAT has no site quorum
- **§76** ★★★ A `scrip_id` EQUAL TO THE TICKER IS A COINCIDENCE — gate symbol→BSE-scrip on ISIN (**read before any BSE-keyed fill**)
- **§59** ★★ STANDALONE-SLOT-HOLDS-CONSOLIDATED AUDIT — the screen is not a defect count (**read before acting on any std/con equality screen**)
- **§80** ★★★ SERIES **BZ** WAS NEVER INGESTED — a live trading series discarded for years (**read before touching the bhavcopy filter or a price-series gap**)

---

## 0. GOLDEN RULES (the things that bite if forgotten)
- **★★★ NO ASSUMPTIONS. NO GUESSWORK. EVER.** User-mandated 2026-08-10; standing rule across
  this runbook AND every campaign/playbook doc (each carries the same line). Every value written
  and every claim made ("exists", "absent", "fixed", "live", "matches") must trace to something
  MEASURED this session — a fetched document, a parsed row, a live read-back. Need a fact you
  don't have? Go measure it. Can't measure it? Record `unknown` and say so — never bridge the
  gap with a plausible guess, an inference from a company's category, or "it probably didn't
  change". Corollaries already enforced elsewhere: never infer absence from our own gaps (§57),
  verify against origin not logs (§38b), verify LIVE through the client (§39, §41).
  (memory: feedback-no-assumptions-no-guesswork)
- **★★ NEVER SAY "CAN'T BE FILLED". EXHAUST EVERY ROUTE, THEN SAY WHICH ONES YOU TRIED.**
  User-mandated 2026-08-06 after three misses in a row in one session (ANGELONE, ADANIGREEN,
  LICI — each declared out of reach, each filled from the announcement PDF minutes later once
  the user named it). A route returning nothing means **THAT ROUTE has no row**, never that the
  value does not exist. Report refusals as `not-found-via:<routes tried>`, never as "unfillable"
  or "never filed", unless the **full ladder in §57** has been walked AND primary evidence says
  the company did not report. Do not infer absence from a company's category ("it's an insurer,
  so it needs the special route") — open the document. **Full procedure: §57.**
- **★ EVERY SUCCESS GETS WRITTEN HERE, IMMEDIATELY.** The moment a route/recipe/fix WORKS
  (verified, not hoped), append it to this runbook in the same session — a new § for a new
  route, or a line in the matching § for a refinement. Procedures live HERE; facts/state live
  in memory. A success that isn't written down will be re-derived from scratch (and possibly
  wrong) next time. This rule is user-mandated (2026-08-04). (memory: feedback-auto-persist-learnings)
- **★ FILINGS COME IN THREE UNITS — test crore (÷1), million (÷10), AND LAKH (÷100) before
  declaring a parse "refused".** Banks are RBI-mandated to file in lakh; misc 2017-era cos too.
  An anchor gate makes wrong-scale acceptance impossible (both PAT anchors would miss by 100×),
  so widening the hypothesis set is free — skipping ÷100 cost a full false-refusal pass over
  the FY18 IPO cohort (BANDHANBNK/HAL/DIXON/IEX…) on 2026-08-03. Diagnose refusals from cached
  OCR (`_rev_diag.py` pattern: dump ≥3-num rows + which gate failed), never accept one untriaged.
- **Profit basis = OWNERS-ATTRIBUTABLE.** Backtest `npCon` (FUND index 3) = owners-attributable.
  Apply via `apply_owners_full.py`. ⚠️ **NEVER run `apply_total_pat.py`** (wrong basis). (memory: project-stocks-profit-basis)
- **Fundamentals come from BSE filing PDFs + VISION** — not Screener, not OCR (OCR mangles digits). (memory: feedback-bse-pdfs-not-screener)
- **⚠️ BSE `FinancialResult` API is entity-POISONED for some scrips — verify the audited entity, not the scrip label.**
  `FinancialResult/w?scripcode=500033` (FORCEMOT) + its `/downloads1/BSEFinancialResult*.zip` return **BSE Limited's**
  numbers, not Force Motors'. The **announcement** API `AnnSubCategoryGetData?strScrip=500033&strCat=Result` → attachment
  GUID → `corpfiling/AttachLive|AttachHis/<guid>` returns the **genuine** company. So poisoning is endpoint-specific, not
  scrip-specific. ANY BSE fetch must confirm CIN/auditor on the PDF (FM=L34102MH1958/Akurdi; BSE Ltd=L67120MH2005/Batliboi),
  never just the scrip label. Full writeup + the proven backfill recipe: `scripts/FORCEMOT_CONTAMINATION_FINDINGS.md`. (2026-06-22)
- **⚠️ A 162-byte `/tmp/bse.json` is BSE's rate-limit 302, not a parser bug.** Over its per-IP quota
  `ListofScripData/w` answers `302 → api.bseindia.com/error_Bse.html` (a 162-byte "Object Moved" stub —
  the byte count is the fingerprint). `curl -s` **exits 0 on a 302** and saves the stub, so the junk only
  surfaced downstream as `JSONDecodeError: Expecting value: line 1 column 1` at `fetch_all.py:45`
  (`fetch_sectors.py:26` json.loads the same file). **`-L` does NOT fix it** — it just saves the error page.
  Validate the **parsed record count** (healthy ≈ 4,929 scrips / 1.73 MB; floor 3,000), never the exit code.
  Quota refills slowly and a success buys a cooldown → space retries 30s+, don't hammer; a cookie session
  does NOT help (it only spends more quota). `refresh.yml` now does 6 spaced retries + the floor, and logs
  the body on failure. Next lever if 302s ever outlast a whole job: cache the last-good master via
  `actions/cache` (the universe changes slowly, a day-old copy is harmless). (2026-07-17, memory:
  project-stocks-bse-scrip-302)
- **Two pages are GENERATED — edit the TEMPLATE, not the output:**
  `docs/nse-bse-dashboard.html` ← `scripts/build_compressed.py`; `docs/mutual-funds.html` ← `scripts/build_mutualfunds.py`.
  The nightly refresh regenerates the dashboard and **clobbers manual edits to the output**. Edit the
  template AND (for an immediate fix) the output. (memory: project-stocks-generated-html)
- **Concurrent writer:** a background process commits/pushes (daily refresh + BSE backfill), so pushes
  get rejected non-fast-forward. Always push with a retry loop:
  `for i in 1 2 3 4 5; do git fetch origin -q; git rebase --autostash origin/main; git push origin main && break; sleep $((i*2)); done`
  Phantom/intentional dirty files to stash if a plain rebase refuses: `scripts/backfill_gaps.py`,
  `scripts/fundamentals.json`, `scripts/splice_renames.py`.
- **Push live on EVERY update** (user preference) — commit + push, never leave it local.
- **Service worker is network-first** (docs/sw.js); the big bin is IndexedDB-cached keyed on
  `sf_meta.json {end}`. Verify deploys with a cache-buster: `curl -s "<url>?cb=$RANDOM" | grep -c <marker>`.
- **Supabase free project `nebjnsndgrhumnkuipqy` auto-pauses after ~7d idle** → resume in the dashboard.
- **⚠️ `docs/sf_stock_data.bin` is a STALE committed SNAPSHOT — do NOT analyse against it.** The daily cron
  commits only `docs/sf_meta.json` (the version marker) and force-pushes the real data to the **sf-data**
  Pages repo + the `data` release asset; the big `docs/sf_stock_data.bin` is left frozen at whatever was last
  hand-committed (was 2026-06-13 while live was 2026-06-22). The **live tool loads from the sf-data repo**, so
  Node grid-search/backtest harnesses that read `docs/sf_stock_data.bin` will silently use days-old prices and
  print numbers that DON'T match the site (cost me a wrong 88% vs the live 84%). For any analysis meant to match
  the tool, rebuild from the LIVE parts first: `python3 scripts/fetch_live_sf.py` (one command — fetches
  sf-data's current layout, merges, overwrites `docs/sf_stock_data.bin`), or pull the `data` release asset.
  Cross-check `gzip-decompress(docs/sf_stock_data.bin).end` vs `sf-data/sf_meta.json {end}` before trusting
  local results.

---

## 1. DAILY PRICE REFRESH  (survivorship-free dataset)
Auto: `.github/workflows/refresh-backtest-data.yml` — **cron 15:15 UTC = 20:45 IST, weekdays**
(after NSE's ~19:00 IST bhavcopy). **Self-heals:** `update_sf_data.py` appends EVERY missing day
since the file's `end`, so a skipped run is caught up by the next.

**Which bhavcopy rows are equity: `("EQ","BE","BZ")` — see §80.** BZ (trade-for-trade +
surveillance) was excluded until 2026-08-10, which silently truncated a stock's series on the day it
was penalised into BZ. Do not "tidy" that filter back to two series.

Pipeline (the workflow, in order — to run by hand do the same):
1. `python3 scripts/build_corp_actions.py` — refresh official split/bonus ratios (exact ex-date factors).
2. `python3 scripts/update_sf_data.py` — append missing trading days to `docs/sf_stock_data.bin`
   (writes `docs/.sf_updated` and bumps `docs/sf_meta.json {end}` only when it actually appended).
2b. `python3 scripts/detect_renames.py` — **rename TRIPWIRE**: same-ISIN renames auto-merge in step 2,
   but an ISIN-CHANGED rename (GUJGASLTD→GUJENERGY, MIRCELECTR→ONIDA, LYPSAGEMS→AURUS, the 9 Axis
   ETFs 2026-07-03) starts a FRESH series with the history stranded on the dead ticker. Detects via
   NSE symbolchange.csv + company-name match on (new series ↔ just-ended series); prints ⚠️ lines and
   writes `scripts/_rename_suspects.json` (committed with the marker). Each suspect → verify price
   continuity at the join + the announcement, then MANUAL_MERGE in update_sf_data.py; false positive →
   ack in `scripts/_rename_ack.json` {"OLD|NEW":"why"}. Merged pairs unflag themselves (old symbol
   leaves the bin). Also check the fundamentals/membership side for the pair (apply_owners_full REV map).
   Full fix sweep for a confirmed pair (MANUAL_MERGE + fundamentals key-move + fno/membership/side-files):
   **§30**. Note the ISIN may be UNCHANGED yet still strand the history — GUJENERGY's day-1 bhavcopy row
   simply carried no ISIN, so the auto-merge had nothing to match on.
4. `python3 scripts/split_sf_data.py` → force-push the BY-DATE split + `sf_meta.json` to the **sf-data**
   Pages repo (secret `SF_DATA_TOKEN`). Browser loads from there (same origin, no CORS). Layout
   (since 2026-08-03 — the 192MB post-rebuild payload made every page load parse the lot):
   - `sf_recent_1.bin` — bars ≥ `deepFrom` (2019-01-01), all a quick run needs: the default
     2020-03-31 window and every wave preset keep a full 365d lookback inside it (~1/3 the bytes).
   - `sf_deep_1..N.bin` — older bars (by-symbol halves, each <95MB); the browser fetches them ONLY
     when a run's window starts before ~2020 (`ensureDeepHistory()`), then prepends per symbol.
   - `sf_meta.json` — `{end, deepFrom, fullStart, recent, deep, nTot, nDead}`. No `deepFrom` key =
     legacy by-symbol `sf_stock_data_*.bin` layout (loaders keep a fallback branch for it).
   Loaders live in `backtest-engine.js` AND `stock-backtest.html` (inline copy) — both must stay
   in sync (memory `feedback-backtest-engines-sync`). Every backtest entry point must
   `await ensureHistoryFor(start)` before `simulate()`/`screenAsOf()` — they throw on missing
   deep history rather than silently compute on a truncated series.
5. Commit ONLY `docs/sf_meta.json` (≈20-byte version marker) — clients re-download when `{end}` bumps.

**Rebuilding the base from scratch** (e.g. changing `DAILY_FROM`, coverage-campaign STEP 3): a fresh
`build_sf_data.py START DAILY_FROM` run produces an UN-merged bin (missing MANUAL_MERGE renames,
MANUAL_RIGHTS, self-heal). Re-apply everything `update_sf_data.py` normally carries by running
`python update_sf_data.py --base <path-to-fresh-bin>` — reads that file instead of fetching the
release asset, then runs the same merge/heal/dv-fill pass. STOP-GATE before publishing: log shows
MANUAL_MERGE lines (PATANJALI/RUCHI etc.), dv_fill counts (~6135 / ~1.7M), `end` not regressed vs the
live release. A session can't `gh release upload` a 200+MB rebuilt bin directly (permission
classifier) and can't commit it either (>100MB single-file git limit) — split it first
(`split_sf_data.py`), push the parts to an orphan branch, and adopt via a one-shot workflow that joins
+ uploads + re-triggers this pipeline (pattern used 2026-08-02, workflow deleted after adoption).

**Weekend special sessions (fixed 2026-08-03):** budget Saturdays (2015-02-28, 2020-02-01,
2025-02-01), weekend muhurat sessions and the 2024 DR-drill Saturdays are REAL trading days that
every enumerator used to skip via `weekday() < 5` — which made 52w hi/lo provably wrong for 55–147
stocks for up to a year after each budget Saturday. Now: (a) every daily enumerator
(update_sf_data, build_sf_data, fetch_bse_bhav, fetch_delivery) walks ALL calendar days — a
non-session weekend is one cheap "no file" miss, and duplicate-of-prior-day guards absorb NSE/BSE
re-serving Friday's file on non-trading days; (b) `insert_weekend_sessions()` in update_sf_data.py
inserts the historical weekend bars in place (idempotent, sentinel-checked), reading rows from the
tracked ledger `scripts/weekend_sessions.json.gz` first (CI can't always reach the old NSE archive)
with live fetch as fallback — prices are scaled onto the series' CA level via the PREVIOUS day's
raw bhavcopy close, NEVER the file's PREV_CLOSE column; (c) fetch_bse_bhav.py has a matching
`WEEKEND_HEAL` pass for dates inside the BSE store's span. A NEW special weekend session needs NO
action — the daily walk picks it up like any weekday. ⚠️ An insert-only publish does not advance
`end` — clients pick it up via the content-hash `rev` in sf-data's sf_meta.json (see the
sf-cache-key memory); confirm `rev` changed after the run. After bars change, re-bake waves.

Trigger manually (today not in yet / missed run):
- GitHub → Actions → **"Daily backtest data refresh"** → Run workflow, **or** `gh workflow run refresh-backtest-data.yml`,
  **or** POST `/repos/dhruvan246/stocks-dashboard/dispatches` `{"event_type":"backtest-data-refresh"}`.
- Verify: `curl -s https://dhruvan246.github.io/sf-data/sf_meta.json` → `{"end"}` = target date.

---

## 2. FUNDAMENTALS BACKFILL  (quarterly net-profit gaps)
Daily cron `update_fundamentals.py` parses NSE XBRL for active names. GAPS (delisted, NSE-unserved,
pre-IPO) are filled MANUALLY from BSE PDFs via vision. The steps below ARE the canonical process
(`scripts/_autorun_plan.md` is an untracked local scratch note — not in the repo, don't point sessions at it):
1. `python -u scripts/bse_vision.py --targets <file.json> --quarters N --since YYYYMMDD > log 2>&1`
   — targets `[[SYM, scripcode, expect_token], …]`; vision LOCATES the net-profit row, crops hi-res to `scripts/_vp/`.
2. Agent READS each `_vp/batch_*.png` → first number/row = current-quarter PAT × unit (Lakh ⇒ ÷100 → cr).
3. Validate: identity guard (PDF company == expect_token); 4Q ≈ filing annual (±2%); cross-check NSE on overlaps;
   **never overwrite NSE values — BSE fills nulls/missing only.**
4. Merge fill-only into **`docs/sf_fundamentals.json` AND `scripts/fundamentals.json`** (owners basis: con=idx3,
   con-date=idx4), then push. Resolver: `scripts/bse_scrips.json`.

### 2b. CORRECTING a wrong NON-NULL value (the deliberate exception to fill-only)
Overwriting a value the series already has (digit transposition etc.) — proven 2026-06-21
(NIACL Q1FY23 std 138.47→118.47 + date 20220812→20220810; Q1FY24 std 260.31→260.23).
- **Anchor 2+ ways BEFORE touching the file.** Read the filing's PAT row (insurers = standalone
  P&L **line 28**, unit Lakh⇒÷100) via `_wf_render.py`/`_wf_crop.py` (crop y-args are 0–1 page
  FRACTIONS; text-layer reads are exact, prefer them over the auditor-narrative pages which quote
  unrelated subsidiary figures). THEN confirm the same number as a *comparative column* in adjacent
  filings — the **year-ago** column of the +1yr filing and the **preceding-quarter** column of the
  +1qtr filing — and/or a ΣQ=9M/FY reconciliation. This neighbour cross-check also catches
  transposition in ADJACENT cells (it surfaced the Q1FY24 error that wasn't in the original report).
- **Guard-edit BOTH json files:** load fresh, `assert` the OLD value/date still match (the concurrent
  backfill may have populated con idx3/idx4 mid-session — PRESERVE them), change ONLY the target
  cells, assert no other key/cell differs, dump `json.dump(d,open(p,'w'),separators=(',',':'))`.
- **Push:** commit ONLY the 2 json files (leave phantom-dirty `_wf_*`/`backfill_*` unstaged). They
  are single minified lines → a concurrent push makes line-based rebase CONFLICT; check
  `git rev-list --left-right --count HEAD...origin/main` first, and if origin advanced on these files
  RE-APPLY the semantic edit to the fresh file instead of git-merging.
- **Durability:** insurer std cells are clobber-safe (XBRL cron skips insurers; BSE backfill is
  fill-only on non-null), so a correction sticks.

### 2c. ORDER-WINS TTM-P/E QUARTER-GAP BACKFILL  (recent-to-NSE names; done 2026-07-15)
Fill the recent quarters so `ttm_pat` (4 consecutive qtrs in `sf_revop`) populates the Order-Wins P/E.
(There is NO `scripts/_autorun_plan.md` — that path in old prompts is stale. Use this.)
- **These BSE result PDFs usually have a real TEXT LAYER** → direct PyMuPDF text extraction is exact
  and ~100× faster than `bse_vision.py`'s 14-page OCR (which crawls). Use text; render+vision ONLY for
  scanned PDFs (image pages, `get_text()` empty). Reusable scratch scripts: `scripts/_ord_text.py`
  (text extract + row crop), `_ord_render.py`/`_ord_render2.py` (render P&L page, OCR-locate if scanned),
  `_ord_apply.py` (fill-only merge → `sf_revop` idx0/1=rev, idx4=patStd, idx5=patCon + both fundamentals.json).
- **ONE page per company:** the **Dec-2025 filing** shows Dec(col1)+Sep(col2)+9M(col4) → derive Jun = 9M−Sep−Dec.
  A co needing only Jun: its **Sep filing** shows Sep(col1)+Jun(col2)+H1. Anchor EVERY value by the 9M/H1 sum
  reconciliation (paisa-exact) and cross-check the overlap quarter vs the NSE-stored cell.
- **Consolidated owners** = "Net Profit attributable to Owners" row if present, else total NP − NCI.
- **Legit residuals (never re-attempt — the quarters were never filed):** half-yearly reporters (RMC), and
  co's listed too recently whose earlier quarters are pre-IPO / pre-mainboard-migration (SOLEX, KRISHNADEF,
  OMPOWER, SAIPARENT, INNOVISION) — verify via NSE `corporate-announcements` (curl_cffi) earliest results date.
  memory: [[project-stocks-orderwins-pe-backfill]].

### 2d. FILER TAG-SWAP: owners ↔ NCI in NSE XBRL  (GLENMARK Q4FY26, found 2026-07-30)
Some filers SWAP the two "attributable to" tags in the results XBRL:
`ProfitOrLossAttributableToOwnersOfParent` carries the tiny NCI share while
`...AttributableToNonControllingInterests` carries the real owners' profit. Our con PAT then
stores the NCI (GLENMARK Mar-26: -0.1 stored vs 301.41 real). The swap is SUM-INVARIANT
(owners+NCI=total either way), so only the filing's own **EPS row** arbitrates:
basic EPS × shares (paid-up ÷ face value) must reproduce the owners' number.
- **Parser guard (live since 2026-07-30):** `build_fundamentals.xbrl_profit` EPS-anchors the
  owners tag and reads the NCI tag instead when owners clearly fails while NCI passes. Covers
  the daily cron (update_fundamentals) + backfill_gaps/hist_backfill. `integrated_profit`
  (legacy full-rebuild path) does NOT have the guard — port it if that path is revived.
- **Detection sweep:** `python -X utf8 scripts/detect_attr_swap.py` — flags stored con cells
  with the tiny-|con| signature, refetches their consolidated XBRL, EPS-tests both tags.
  Verdict SWAPPED = high confidence, but STILL anchor a 2nd way (BSE PDF attributable rows /
  Screener total) before healing (§2b guard-edit of BOTH fundamentals JSONs + sf_revop idx5).
  MISMATCH_MANUAL ≠ swap: usually weighted-avg-shares EPS (mid-quarter capital raise, e.g.
  GSPCROP IPO), wrong paid-up tag, or an annual EPS mistagged into OneD — read the PDF.
- **Healed cells are journaled in `scripts/attr_swap_fixes.json`** (tracked; per-cell
  provenance). 2026-07-30 sweep over all 2025+ quarters: 111 suspects → 6 swaps
  (GLENMARK/KIRLOSBROS/SHYAMMETL/TALBROAUTO/NITCO/GSPCROP) + 1 empty-tags case (TRU
  Mar-26: owners & NCI tagged 0, real owners = total -58.6 from the FIRST filing;
  the 24-Jun REVISED filing says -19.18 but the store is point-in-time on first filing).
  Benign MISMATCH examples: SAGCEM Jun-25 (real 60%-sub NCI; filer's XBRL EPS on total),
  NAVNETEDUL (annual EPS mistagged into OneD), CENTUM Mar-26 (filer stuffed the TCI-
  attributable split into the PAT-attributable tags; the filing prints NO PAT owners/NCI
  split for the quarter, so the store keeps the printed total 1.64 per the 2c rule -
  examined vs full PDF 2026-07-30, do not re-audit). Pre-2025 quarters were NOT swept
  (old-endpoint XBRLs; era already reconciled against StockView/Trendlyne) — if a
  pre-2025 tiny-con anomaly surfaces, verify via PDF.

---

## 3. INSURERS  (IRDAI-format — XBRL can't parse them; PARTIALLY auto-filled FREE)  ★ free text-anchor, 2026-07-15 ★
LICI, SBILIFE, HDFCLIFE, ICICIPRULI, ICICIGI, GICRE, NIACL, STARHEALTH, GODIGIT, NIVABUPA, MFSL file
IRDAI format (Policyholders' Revenue A/c + Shareholders' P&L) → the standard XBRL P&L parser gives them
NOTHING. **User can't pay for a vision API**, so `scripts/fetch_insurers.py` uses a FREE text-anchor method
(no API key, no cost): discover newly-filed quarter from BSE announcements (`is_result_filing()`, board-
outcome-aware) → fetch the **earliest** result filing after quarter-end first (BSE; NSE curl_cffi fallback
for LIC's cover-only BSE attachment) → TEXT-extract the Shareholders' P&L rows (PyMuPDF words + band-merge;
free rapidocr fallback for fully-image pages) → **SLIDING DOUBLE-ANCHOR**: scan the row for where the
preceding-quarter AND year-ago columns sit adjacently (handles a leading serial cell), both must match our
stored con series within max(3%,₹2cr) under one scale ÷1/÷100/÷10; with-sub insurers require a
consolidated-page row. **Fill-only; NEVER writes a wrong value — worst case SKIPS.** Runs in
`refresh-fundamentals.yml` (nightly 21:15 IST + `insurer-refresh` repository_dispatch). Deps installed
in-step: pymupdf + curl_cffi + rapidocr-onnxruntime/onnxruntime/numpy. Validate: dispatch input
`insurer_verify_quarter=YYYYMMDD`, or `python -X utf8 scripts/fetch_insurers.py --verify 20260331`.

**COVERAGE (offline-validated Mar-2026) — 4 of 11 auto-fill, the rest stay MANUAL:**
- ✅ **SBILIFE, NIVABUPA, HDFCLIFE, GODIGIT** — read exactly, free.
- ❌ **ICICIGI, STARHEALTH** — P&L table is a scanned image / corrupt-OCR text layer (value absent from the
  text). The rapidocr fallback only fires on FULLY-image pages, so their mixed text+image P&L pages aren't
  reached → not recovered without a render-and-OCR-the-P&L-page upgrade.
- ❌ **GICRE, NIACL, ICICIPRULI, MFSL** — HARD WALL: the stored **owners-attributable con is hand-COMPUTED**
  (total PAT + minority + associate; MFSL = continuing+discontinued) and is **never printed as a single
  number** in the filing, so double-anchor can't find it. The parser sees only the STANDALONE row (con≠std),
  which require_con correctly rejects. **These are exactly the insurers the manual playbook exists for.**
- So getting all 11 free is NOT achievable with reasonable effort. Fill the residual by hand each quarter via:

**Manual method** — correct page/row, owners-attributable, unit disambiguation, verify:
→ **`scripts/INSURER_EXTRACTION_PLAYBOOK.md`** (memory: project-stocks-insurer-extraction).

**ROBUSTNESS UPGRADES (2026-07-16) — so the Season Trends chart (quarterly-results.html) never silently undercounts an index:**
1. **Free Gemini-vision fallback is WIRED but needs a secret.** `fetch_insurers.py` → `gemini_extract()` (via
   `gemini_vision.py`, Google AI Studio FREE tier ~1500/day, we use ~7/qtr) renders the P&L page and reads it,
   still ANCHOR-VERIFIED against stored year-ago con. It only runs when `GEMINI_API_KEY` is in the env.
   ⚠️ **The repo has NO `GEMINI_API_KEY` secret yet** (only SF_DATA_TOKEN) → scanned insurers ICICIGI/STARHEALTH
   have never auto-filled. **FIX = add a free key:** create at https://aistudio.google.com/apikey (no billing),
   then `gh secret set GEMINI_API_KEY` (or GitHub UI → Settings → Secrets → Actions). Then the nightly job
   auto-fills the scanned insurers unattended.
2. **STANDALONE-ONLY fallback (code, done).** `extract()` now handles a with-sub insurer that filed ONLY
   standalone this quarter (ICICIPRULI Q1–Q3 — consolidated published annually only): if the filing has NO
   "consolidated" text at all AND the standalone row double-anchors AND the insurer's con has historically
   tracked std (median ≤1.5%, max ≤4% — `_con_tracks_std`; ONLY ICICIPRULI passes), fill con=std. Never fires
   when a real consolidated page exists (Q4) or for insurers whose con truly diverges (NIACL/HDFCLIFE/MFSL/LICI).
3. **SAFETY-NET monitor (code, done).** `scripts/check_season_coverage.py` runs in refresh-fundamentals.yml
   after build_results_season: for the live quarter it compares each index's DECLARED members (in
   results_feed.json) vs PARSED members (PAT in sf_fundamentals) and prints/writes `docs/_season_coverage.json`
   any gap. So a declared-but-unparsed member (for Nifty 500 or ANY index) shows in the CI log immediately
   instead of being found later against a reference. A gap persisting >~1 day = fill it by hand.

---

## 4. BUILD & DEPLOY
- **Dashboard** `docs/nse-bse-dashboard.html` ← `scripts/build_compressed.py`. NO LONGER base64-inlines the
  data (used to be ~23 MB). Now emits: a ~60 KB HTML shell + `docs/dash_slim.bin` (~2 MB: metadata + last
  ~250 days) + `docs/stock_data.bin` (full, also used by backtests). Page renders from slim; full history is
  lazy-fetched by `ensureFull()` only on a >~1yr range or the dashboard backtest. Edit the `HTML=` template.
- **Mutual funds** `docs/mutual-funds.html` ← `scripts/build_mutualfunds.py`. Same idea: ~0.5 MB HTML
  (scheme list inline) + `docs/mf_history.bin` (~7 MB NAV history, EXTERNAL). Page renders instantly; history
  is lazy-fetched by `ensureHistory()` on first use of the custom-date calculator or a fund-detail modal.
  → To test either build locally without the CI source artifact: reconstruct the inputs FROM the live HTML
    (decode the `__B64__` / `histData` inline blobs), drop them in `scripts/`, run the build, verify in a
    browser, then `git checkout` any tracked source you overwrote. (The build `print()`s a `→` — fine on
    macOS' default UTF-8; the old Windows runs needed a `PYTHONUTF8=1` prefix.) See §9.
  → `scripts/mf_history.b64` (gitignored, CI-only, from fetch_mf_returns.py) is just base64 of the tracked
    `docs/mf_history.bin` — rebuild it in one line rather than re-fetching ~5k schemes:
    `python -c "import base64,pathlib;pathlib.Path('scripts/mf_history.b64').write_text(base64.b64encode(pathlib.Path('docs/mf_history.bin').read_bytes()).decode())"`
    Without it the build still emits a working page but silently drops the date-picker bounds.
  → **Custom-window column gotchas** (fixed 2026-07-16, `dateToIdx`/`custHint`): the NAV axis starts
    **2006-04-01**; a From date before it used to return idx `-1` → every fund null → "0 funds" and an
    all-dash column. It now clamps to day 0 and the inputs carry baked `min`/`max`. Separately, a *valid*
    window still blanks EVERY visible row when From predates **2013** and the plan filter is on Direct
    (direct plans only exist from Jan-2013; only 3 schemes have NAV back to 2006, all Regular) — `custHint()`
    explains that instead of showing a silently empty column. Blanks for young funds are CORRECT, not a bug:
    only ~1,811 of 5,021 schemes have 5y+ of NAV, so a 5y window legitimately fills ~1,632.
- **Hand-maintained pages** (edit directly): `stock-backtest.html`, `saved-strategies.html`,
  `backtest-history.html`, `stock.html`, `fii-dii.html`, plus shared `theme.css`, `theme.js`, `bt-sync.js`, `backtest-engine.js`.
  NOTE: `stock-backtest.html` is self-contained (its own engine) and does NOT load `backtest-engine.js` —
  helpers used there must be defined locally.
- **Deploy:** commit + push (rebase loop §0); Pages redeploys ~30–90s; verify with the cache-buster curl (§0).

---

## 5. PENDING QUEUE (remind the user)
- ✅ **GICRE con PAT — DONE 2026-08-06** (17 cells corrected, §55c). Two follow-ups it opened:
  - **GICRE STANDALONE PAT is polluted in the same era** (Dec-2024, Sep-2025, Jun-2025 hold the
    CONSOLIDATED after-tax figure; the filings and their own 9M/FY columns both say otherwise —
    Dec-2024 std is 1621.35, not the stored 1623.43). Not fixed: different defect, wider blast
    radius, needs its own sweep. Evidence in §55c.
  - **GICRE con-PAT copies predate FY2023** — Mar/Sep/Dec-2021 read materially different
    consolidated figures from what we store (Dec-2021: stored −28.48, filings 141.80, two filings
    agreeing). Deliberately not written; a pre-2022 sweep should take the whole era at once.
Genuinely open items (memory: project-stocks-pending-queue has the full context):
- **Tier-1 re-sweep IN PROGRESS** — 56 companies / 514 cells left; resume per memory
  project-stocks-resweep-resume (ledgers `scripts/_wf_skips.json` + `_wf_audit_done.json`).
- **Bucket B/C/D "unfillable" re-audit** (user asked 2026-06-23 "don't assume, remind me later"):
  B=110 cos/1188 skip-logged cells (re-verify skips after finder improvements), C=25 recent-IPO
  pre-listing (re-check RHP Q1-stub method §6), D=3 dead-ends (HEXT/PIRAMALFIN/SPICEJET).
  ✅ **IOB CLOSED 2026-08-06** — proven never-filed, not a fetch failure (§51). ✅ **BASF CLOSED
  2026-08-06** — Mar-2020 con filled by no-sub identity (first subsidiary acquired 2020-08-18).
- **KIRLFER con 2022Q2→date** — mixed-basis series; re-extract owners-attributable and overwrite
  the con==std no-sub fills (details in the pending-queue memory).
- **Tier-2 re-audit DEFERRED** (user: "tier 2 we will do later") — when resumed, only target
  mismatch cells, not both-missing pre-listing quarters.

✅ DONE — do NOT re-offer: pre-IPO backfill (14 stocks, 2026-06-19), Adani Hindenburg price fix
(2026-06-19), dash_slim.bin commit-step fix + its 3 feeds.json entries (fixed by 2026-07-17 —
refresh.yml now carries it through /tmp and commits on content change).

---

## 6. HISTORICAL MULTI-AGENT BACKFILL  (deep 2020→date gap fill, proven 2026-06-21)
Fill the LONG-TAIL of pre-2024 quarterly gaps (con+std) for every Nifty-500-union member, where the
daily cron / NSE-XBRL can't reach (NSE integrated-filing-results serves only ~1yr; older history needs
per-filing PDF reads). Validated: chunk of 8 agents recovered 74/74 cells, all anchor-verified.

**Tools (all in scripts/, persisted):**
- `fetch_nse.py <targets.json>` — targets `[[SYM, QE_int], …]`; downloads NSE financial-result PDFs to
  `_vpdf/SYM_QE_nse.pdf`. Works for OLD quarters (uses /api/corporate-announcements, curl_cffi chrome TLS).
  GOTCHA: it keeps only `.pdf` attachments, but many pre-2020 "Financial Result Updates" announcements attach
  a **.zip** (`Result33_*.zip`, `FS_*.zip`, `Outcome*.zip`) → 0 candidates mapped. Fix: pull the zip directly
  with the same curl_cffi session+Referer and extract the inner PDF (usually `Result33.pdf`). Proven 2026-06-21:
  CREDITACC Q4FY19 (Mar-2019) lived in `Result33_08052019161358.zip` (announce 08-May-2019, std PAT 76.31).
  GOTCHA-2 (FIXED in code 2026-06-21): the BAD-keyword veto matches ONLY the announcement subject (`desc`),
  not the desc+attchmntText blob. Real "Outcome of Board Meeting" results filings often have attchmntText
  "...along with press release"; the `press release` BAD keyword used to veto them → "0 cands mapped"/MISS
  (repro: TIINDIA QE 20220630). A spurious MISS for a quarter whose results PDF clearly exists → check the
  candidate filter at fetch_nse.py ~L82-90 (BAD must stay scoped to desc).
- `_wf_dump.py <pdf> <std|con>` — finds the P&L page(s). `_wf_rows.py <pdf> <page>` — text-extract profit
  rows + unit hint. `_wf_render.py <pdf> <page> <out.png>` / `_wf_crop.py <pdf> <page> <out.png> <y0> <y1>`
  — render/crop for VISION reads when the PDF is scanned (no text layer).
- `_wf_regap.py` — (re)generate `_wf_gaps.json` (current gaps, 2020Q4..Mar26, across `_full_union_2024.json`)
  AND `_wf_bins.json` (balanced agent partition; big insurers isolated). RUN BETWEEN CHUNKS so filled cells drop.
- `_wf_gen.py <startBin> <endBin>` — emit a Workflow JS (`> _wf_run_cN.js`) for that slice of bins; bakes each
  agent's symbol list + the recovery playbook + the StructuredOutput schema.
- `_wf_apply.py <run_journal_dir> [--apply]` — harvest agent results from the run's journal (robust even if the
  run was KILLED — completed agents are in journal.jsonl), RE-VERIFY (gap-real, no-overwrite, magnitude vs
  neighbors, con==std-identity consistency), apply PASS fill-only to BOTH json files, FLAG the rest to
  `_wf_flagged.json`. con==std-exact copies bypass the magnitude check (validated by the std's provenance).

**Loop (one chunk at a time):**
1. `python -X utf8 _wf_regap.py`  → see remaining cells + bins.
2. `python -X utf8 _wf_gen.py 5 25 > _wf_run_c2.js`  (bins 0-4 = the big insurers; run those as their own chunk).
3. Launch with the **Workflow** tool `{scriptPath: scripts/_wf_run_c2.js}` (multi-agent — needs user opt-in).
   ~8 agents/chunk is safe for NSE rate-limits; ≤~14 concurrent (workflow cap). Don't also fetch NSE yourself meanwhile.
4. On completion: `python -X utf8 _wf_apply.py <transcriptDir/wf_…>`  (DRY RUN) → review FLAGS, clear false
   positives (e.g. con==std across a growth jump/merger), then re-run with `--apply`.
5. Commit + push both json files (rebase loop §0). Repeat from 1.

**Agent recovery playbook (baked into `_wf_gen.py`, the CONSTRAINT: never output an unanchored value — SKIP instead):**
- **(A) no-sub con identity** (fast, recovers whole runs): read a filing note ("first consolidated results",
  "no subsidiary", "acquired … on <date>"). If no subsidiary in the gap quarter → con = series std (SEBI LODR
  Reg 33 identity). PROVE it: con==std exactly in the overlap quarters where both are stored.
- **(B) genuine read**: fetch PDF → locate page → text or vision read of "Profit for the period / PAT"
  (NOT total-comprehensive, NOT pre-tax, NOT segment; owner-attributable for con with minority). Unit: Lakh/100,
  Million/10, Crore as-is.
- **(C) insurers**: follow `INSURER_EXTRACTION_PLAYBOOK.md` (§3).
- **Anchor** every value: filing's year-ago/prior-qtr comparative == our series (exact), or 9M=ΣQ / FY=ΣQ
  reconciliation, or PBT−tax=PAT. Capture announce date (broadcast/board-meeting) as YYYYMMDD when visible.

Note for backtest point-in-time: a row needs BOTH value AND announce date (≤ as-of) to be picked as the
"current" reported quarter, but only the VALUE to serve as a year-ago base (`profitAt`, stock-backtest.html).
So value-only fills still power YoY-vs-prior-year; supply the date when the filing shows it.

---

## 7. STRATEGY FINDER — find the best strategy over a window  (Node; exhaustive, proven 2026-07-02)
"find best strategies" → run the REAL engine in **Node** over the search grid (no browser freeze/45s limits).
Default config unless the user says otherwise: **Nifty 500, monthly (freq 1), top 5, earnBasis con**, end = latest data.
(memory: [[feedback-strategy-finder-exhaustive]])

### 7.0 ALWAYS use LIVE data first (the committed bin is STALE — §0)
`docs/sf_stock_data.bin` is a frozen snapshot (was 2026-06-13 while live was -06-25). Before ANY Node grid run,
rebuild it from the LIVE sf-data parts, else prices are days stale and CAGRs won't match the site:
```
python3 scripts/fetch_live_sf.py
```
One command — reads `sf_meta.json` for the current layout (by-date `sf_recent_*`+`sf_deep_*` since
2026-08-03, per-symbol concat exactly like the browser; legacy by-symbol handled too), applies the
ZOMATO/ETERNAL merge guard, overwrites `docs/sf_stock_data.bin`, prints `end` to verify vs live meta.
Restore afterwards if you don't want the working tree dirty: `git checkout docs/sf_stock_data.bin`.

### 7.1 EXHAUSTIVE grid — `scripts/grid_search_full.js` (NOT the greedy `grid_search.js`)
The old `grid_search.js` is GREEDY: it ranks factors unfiltered (stage1), then only bolts filters onto the TOP-12.
That PRUNES factors weak-raw-but-strong-filtered and MISSES real winners (proven: high-`mdd6` was ~130th raw so
never got filtered, but `mdd6`+`d52<=10`+`profitYoyPct>0` = 65% CAGR, the #2 strategy). **Use the exhaustive driver.**
- `grid_search_full.js` runs **every FIELDS sort × both dirs × every FSET** (method=reset, top5), then sweeps
  method(hold) + topN(3/8/10) on the top-15. Appended to the engine; parametrized by start date via argv:
  `cat docs/backtest-engine.js scripts/grid_search_full.js > scripts/_gridfull_run.js && node scripts/_gridfull_run.js 2020-03-31`
  Writes `scripts/_gridfull_result_<start>.json` (topByCAGR 40 + topByRiskAdj 20, each with full `raw` cfg).
  **RUNTIME IS LONG — run in BACKGROUND:** ~980 combos = ~55 min for a 3yr window, **~108 min for a 6yr window**
  (each combo ~3–8s; longer window = more rebalances = slower). Don't foreground it.
- Ensure `FSETS` includes the meaningful filters incl. `[d52<=10, profitYoyPct>0]`. Cross-window compare tool: `scripts/_cross.js`.

### 7.1b ★ RE-STAGE `_live/` AND MEASURE COVERAGE **LIVE**, EVERY RUN — a staging goes stale in HOURS
Standing rule from the user (2026-08-11): **always take LIVE coverage.** A `scripts/_live/` directory
staged earlier — even the same day — is a SNAPSHOT, and the backfill campaigns commit new cells all day.
Measured 2026-08-11: a staging 5 days old had `fund_live.json` 70 KB smaller, `shp_live.json` **1.97 vs
2.7 MB**, and an `end` of 2026-08-05 vs the live 2026-08-11 — a whole extra trading week plus that day's
fills. Coverage measured off the stale copy is a claim about YESTERDAY'S data, and stating it as today's
is exactly the "never infer absence from our own gaps" trap.

**Before EVERY grid run, without exception:**
```
python3 scripts/gridmega_fetch_live.py          # re-stage; prints the live end date
rm -f scripts/_gridmega_cache_*.json.gz         # MANDATORY — see below
```
1. **Purging the factor caches is not optional.** They are keyed by (start, end, universe) and carry NO
   data revision, so a cache built from the previous staging is silently reused and feeds the OLD factor
   values into the new grid. Nothing warns you. `gridmega_phases_all.sh` does this automatically; a
   hand-run grid does not.
2. **Re-measure per-year coverage after re-staging**, and quote it with the results.
   **THE WORKED EXAMPLE — why this rule exists (2026-08-11).** The same probe run against a 5-day-old
   staging vs the live one:

   | year | `fiiPct` STALE | `fiiPct` LIVE | `profitYoyPct` stale → live |
   |------|---------------|---------------|------------------------------|
   | 2003 | **0%**        | **89%**       | 3% → 10%                     |
   | 2004 | **0%**        | **92%**       | 27% → **51%**                |
   | 2008 | **0%**        | **98%**       | 87% → 87%                    |
   | 2011 | 44%           | **99%**       | 95% → 96%                    |

   Off the stale copy the conclusion was "FII/DII does not exist before 2011, so a 2002 grid can only
   crown technicals-only strategies." That was FALSE — an artefact of the snapshot, not a property of
   the data. Live coverage is 89%+ from 2003 and the full factor set is usable across the window.
   Never state a coverage claim from a staging you did not fetch in this session.
3. **A missing filter value FAILS the test** (NaN comparisons are false), so a strategy filtering on a
   factor that genuinely does not exist yet holds NOTHING for those years — under `method:hold` it sells
   out and sits in cash, which looks like a low-drawdown star. Always check `avgPicks` on the leaders.
   This is a REAL effect where coverage is genuinely thin (2002 itself: profitYoyPct 10%, composite 8%)
   — just make sure the thinness is measured live before you attribute a result to it.
4. `nifty500_live.json` starts **2012-01-02**, so the grid's own `bench` field is WRONG for any window
   starting earlier. Take the real benchmark from `docs/index_monthly.json` (NIFTY 500 monthly from 1995;
   Mar-2002 → Jul-2026 = +2,925%, 15.04% CAGR).
5. Index membership before the first snapshot falls back to the FIRST snapshot (`lastSnap`), so a
   Nifty-500 window starting before **2002-10-02** screens against the Oct-2002 list — a look-ahead.

### 7.2 ALWAYS out-of-sample validate (curve-fit guard)
Top-of-N over ONE window is in-sample. Re-run the winners on ≥1 other window. A strategy that only wins from a
**crash-bottom start (2020-03-31)** is regime-luck, not edge. Proven robust across 2020 & 2023 starts:
- ★ **`high-mdd6` + `d52<=10` + `profitYoyPct>0`** (top5, monthly, N500) — #2 from 2023 (65%), #3 from 2020 (71%). The all-weather winner.
- **`high-delivPct` + `profitYoyPct>25`** — #1 from 2023 (71%/13.5dd), #9 from 2020. Runner-up; best risk-adj.
- Recovery-momentum (`ret6m`/`d52_low_pct`) tops 2020 only (COVID bounce) → fades to >#40 from 2023. Window-luck.
- NOTE: `rsNifty` was REMOVED from FIELDS (2026-07-02) — in a single-index universe it == `ret6m` minus a
  per-date constant, so it never re-ranks. Don't re-add it.

### 7.3 Save winners to BOTH shared lists (Supabase `nebjnsndgrhumnkuipqy`, write secret `sw_owner_8Kq2Lm9Xp4Rt7v`)
Use FILE scripts, not `node -e` (shell quoting mangles the URL). Pull→prepend→push via REST rpc (fetch, Node ≥18):
- **Backtest History** — `scripts/_save_history.js <result.json> [N]`: re-runs `simulate()` for FULL metrics, builds
  UI-identical items `{id,ts,label,cfg,yby,m:{cagr,finalV,maxDD,vol,winRate,benchCagr,rebs,years}}`, dedups top-N
  distinct, pull `bt_public` → prepend → push `bt_owner_set {secret,payload}`. Writes `scripts/_save_items.json`.
- **Saved Strategies** — `scripts/_save_strats.js`: reads `_save_items.json`, builds `{id,ts,name:label,cfg}`,
  pull `bt_strats_public` → prepend → push `bt_strats_set {secret,payload}`.
- The **saved-strategies.html** page is a Trendlyne-style TABLE (Strategy / CAGR / Backtests / 🗑), one row per
  UNIQUE strategy (identity = cfg minus the date window), sorted by **canonical CAGR = the longest-window run**
  (so a short lucky window can't jump the sort). Click a row → expands ALL its backtests, pulled from shared
  **History** filtered by `isRealRun` (rebs≥3 AND window≥90d — excludes degenerate junk like a 428% 1-rebalance run).
  So a NEW backtest of a saved strategy (which autosaves to History) auto-appears under it. Don't revert to
  sourcing windows only from the strategy list — that hid freshly-run backtests.
- **⚠️ STRATEGY IDENTITY INCLUDES `lookback` — and `readCfg()` used to DROP it (fixed 2026-07-15, commit c6298f8).**
  `identityKey`/`stratIdentity` key on `c.lookback||1`, but `stock-backtest.html`'s `readCfg()` had NO lookback
  field (there's no form control for it) → EVERY re-run produced `lookback=1`. So re-running a strategy saved with
  `lookback=6` (e.g. the grid-search-saved "Return — 6 month %" rows) yielded identity `…|1|…` ≠ the saved `…|6|…`,
  and the run **never nested** under its row (the user's +74% 2020 window silently vanished from strategy #5). Fix:
  `readCfg()` now carries the loaded strategy's lookback via a `CUR_LOOKBACK` module var set in `applyCfg()`.
  `lookback` does NOT affect `simulate()` (it's ONLY identity + the live Today's-Picks return window in
  saved-strategies.html ~L508); still, KEEP it in identity and KEEP readCfg preserving it — dropping it from identity
  would merge genuinely-distinct lookback rows. If a re-run ever "doesn't nest", check lookback parity FIRST.
- **BACKTEST PAGE PERF — the editable builder loads ZERO market data (deferred, 2026-07-15).** `stock-backtest.html`
  shows the builder instantly (`initUIStatic`/`openEditor`, dates from tiny `sf_meta.json`); the ~17 MB `stock_data.bin`
  (needed ONLY for index/F&O membership + startTs — SF mode overwrites its prices) + the sf-data price parts load
  lazily in `ensureData()` on the FIRST ▶ Run. "Edit & re-run" from a snapshot no longer reloads/front-loads 17 MB —
  it calls `openEditor(cfg)` (instant form, no fetch). ⚠️ Do NOT source membership from `dash_slim.bin` to save the
  17 MB: the live slim file lags `stock_data.bin` (checked 2026-07-15: fnoHistory 76 vs 77 snaps, different indices —
  it's rebuilt only by the dashboard build, not the weekly membership refresh) → would silently change recent-rebalance
  backtest numbers. A real Run inherently needs the full survivorship-free prices; that load is unavoidable.
- saved-strategies boot now MERGES local `bt_history` the shared pull didn't return yet (sync lag / paused Supabase)
  and re-appends it, so a just-run window shows even before the shared history catches up.
- **🕘 REWIND (2026-07-15, commit 63aa8d6) — point-in-time "qualifying stocks as of a date" (Trendlyne Screener Rewind
  analogue).** Lives in `strategy-backtest.html` as a card between the run form and Run history: a date picker (back to
  the data start) → lists EVERY stock that passed the strategy's filters on that day, ranked by its sort factor, Top-N
  marked ⭐. Powered by `backtest-engine.js` **`screenAsOf(cfg, dateStr)`** (returns the full ranked qualifying rows) +
  `fieldVal(r, sortBy)` for the metric column; the ~17 MB+ market data loads lazily on first Rewind via `loadEngineData`
  (local `ensureEngine`). The SAME feature ALSO already exists in the **Today's Picks modal** (saved-strategies.html):
  its "📅 As of date" picker + "Also qualifying" section = picks + full qualifying list as of any date. ⚠️ **GOTCHA that
  blanked the page once:** `stockHref`/`stockLink` are already defined in `backtest-engine.js` — a `const stockHref` in a
  page that loads the engine throws "already declared" and kills the whole inline script (empty body). Reuse the engine's
  helpers; don't redeclare engine globals (pct, fmtINR, FIELDS, TURN_OPTS, simulate, screenAsOf, stockHref/Link, etc.).
- **⚠️ SAVED-STRATEGIES UI REDESIGN (2026-07-15, commit 127e4f7) — Trendlyne-style, rows DON'T expand.** The table
  columns are **Strategy · CAGR · Backtests(👁) · Today's Picks(🎯) · ☆ · 🗑**. Clicking a row or the 👁 opens
  `strategy-backtest.html?id=`; the 🎯 column opens the Today's-Picks modal directly (openPicks(strategy.id)). The old
  click-to-expand detail-cards + ▸/▾ arrow are GONE (`detailCard()` is now dead code; `data-openrow` drives row-open).
  Don't reintroduce the expansion. **`strategy-backtest.html`** is now the drill-in page: TOP = an EDITABLE "Run a new
  backtest" form pre-filled from the strategy (freq/dates/universe/sort/dir/topN/capital/method/earnBasis; sort options
  from engine `FIELDS`, universe from `TURN_OPTS`; filters carried through, edited in the full builder via "edit filters →");
  BELOW = the existing "Run history" table. ▶ Run Backtest hands the (edited) cfg to `stock-backtest.html` via
  **`bt_load {view:'run'}`** — a NEW mode = auto-run AND record to History (NOT view-only), so the result nests back under
  the strategy. `readForm()` PRESERVES `lookback` + `filters` so identity holds (see the lookback gotcha above).

### 7.4 REFRESH THE STRATEGY PHASES LAB (the 4.44M-combo grid behind `strategy-phases.html`)
**One command** — waits for the day's sf-data publish, stages LIVE data once, runs 11 windows × 5 basket
variants, builds all five page JSONs:
```
cd ~/stocks-wt/<job> && ./scripts/gridmega_phases_all.sh 2026-08-06 4     # wantEnd, parallel jobs
FORCE_END=2026-08-05 ./scripts/gridmega_phases_all.sh "" 4                # don't wait, pin the window end
```
Pieces (ALL TRACKED since 2026-08-06 — the previous set was untracked scratch and died in the Aug-3
cache purge, costing a full rebuild-from-scratch before this refresh could even start):
- `gridmega_fetch_live.py` → `scripts/_live/` (p1_new.bin p2_new.bin fund/shp/nifty/nifty500/stock_data).
  Handles the by-date `sf_deep_*`+`sf_recent_*` layout; deep parts FIRST so recent bars append after.
- `gridmega_shim.js` — `location`/`localStorage` stub; the engine reads `location.hostname` at load.
- `grid_search_mega.js` — env `TOPN` · `METHOD` · `UNIVERSE` select the basket variant; `VTAG` keeps
  each variant's artifacts apart (default top5/reset/N500 keeps the legacy un-suffixed names).
  `MAIN_ONLY=1` for phase grids. `SELECT_FILE=<json row indices>` re-scores a chosen set in a window
  and emits EXACT cagr/totRet/dd/win.
- `gridmega_phases_run.py` — the 55-job driver; RESUMABLE (skips any window whose `_gridmega_top_` marker
  exists), longest windows first, staggered starts.
- `gridmega_phases_build.js` — merges 11 windows → `docs/strategy_phases<vtag>.json`. Needs `GRID_END=`.

**Traps that will silently corrupt a refresh:**
1. **PURGE `_gridmega_cache_*.json.gz` whenever `_live/` is re-staged.** The factor cache is keyed by
   window+universe, NOT by data revision — a stale cache feeds the PREVIOUS snapshot's factor values
   into today's grid and nothing warns you. `gridmega_phases_all.sh` purges automatically.
2. **One staging for all 11 windows.** A daily rebuild re-applies corp-action adjustments and can revise
   historical bars, so windows computed on two different stagings are not comparable — and the ⭐
   best-in-all-4 / best-in-all-years cards are pure cross-window comparisons.
3. **The merge is a POSITIONAL join** — row N is the same strategy in every window because the grid
   enumerates SORTFIELDS × DIRS × FSETS deterministically. Never key on the filter text.
4. A window is done only when `_gridmega_top_<tag>.json` exists; the CSV alone may be a partial mid-run file.
5. Pages Deploy lags the sf-data commit (1–5 min). Gate on the SERVED `sf_meta.json`, not the commit.
6. `docs/sw.js` never caches `.json` and the page fetches with `cache:'no-cache'` → **no service-worker
   bump needed** for a data-only refresh.

**Cost on the M5 (2026-08-06):** 0.37 ms/combo top-5, 0.30 top-3, 0.17 F&O (smaller universe) ⇒ full-cycle
window ~22–35 min at 4-way parallel, whole sweep ~2 h. Each process PEAKS ~3.3 GB parsing the 9.3M-bar
dataset then settles ~1 GB, so 4-way fits 16 GB with the built-in 25 s start stagger.

---

## 8. F&O MEMBERSHIP (survivorship-free; CURRENT-name labels)  ★ rebuilt + normalized 2026-06-23 ★
The backtest **"F&O stocks" universe** uses point-in-time *membership* (who was in F&O on each rebalance date)
but **CURRENT ticker names** as labels: `membersAsOf('__FNO__', date)` → `fnoHistory` in `docs/stock_data.bin`,
from `scripts/fno_history.json` (`{effectiveDate, symbols[]}`, **76 snapshots 2015-01-30 → date**, deduped).
- **Source of truth = NSE F&O bhavcopies** (every stock-future/option underlying actually trading that month,
  using the ticker that traded THEN). Two formats: old `fo<DD><MON><YYYY>bhav.csv.zip` (INSTRUMENT=FUTSTK/OPTSTK,
  col SYMBOL) for **≤2024-06**; UDiFF `BhavCopy_NSE_FO_0_0_0_<YYYYMMDD>_F_0000.csv.zip` (FinInstrmTp=STF/STO,
  col TckrSymb) for **≥2024-07**. Index underlyings (NIFTY/BANKNIFTY/DJIA/S&P500/INDIAVIX/CNX*/…) are EXCLUDED.
- **Going forward (auto):** `scripts/extend_fno_history.py` (no args = latest trading day) appends a snapshot
  from the bhavcopy + patches `stock_data.bin`. Wired into `.github/workflows/refresh-membership.yml`
  (weekly Sat 22:00 IST). `build_membership_v2.py` only touches `indicesHistory` and PRESERVES `fnoHistory`.
- **Full rebuild (occasional):** `python scripts/rebuild_fno_history.py [START_YEAR]` (default 2020; pass `2015`
  for the whole history). Walks each month, fetches the last trading day's bhavcopy, dedups, writes
  `fno_history.json` + patches `stock_data.bin`. ~110 NSE fetches for 2015→date, a few min.
- **⚠️ AFTER ANY REBUILD, NORMALIZE NAMES TO CURRENT — `python scripts/normalize_fno_names.py`.** A bhavcopy
  rebuild yields the name that traded THEN (TATAMOTORS, GMRINFRA, PVR…), but the price+fundamentals data keys the
  continuous history under the CURRENT name (TMPV, GMRAIRPORT, PVRINOX) — and on the **live sf-data** the old-name
  series are split/truncated (e.g. `TATAMOTORS` ends 2003-12, rest under `TMPV`), so point-in-time names make
  renamed stocks DROP OUT of F&O backtests (Tata Motors missing from Jan-2024 = the bug this fixed). normalize
  remaps old→current (25 names, same map as `FUND_ALIAS`), matching the data keying + the index universes (which
  always use current names). Delisted-no-successor names (IDFC) stay. Verify on LIVE sf-data, never stale `docs/sf_stock_data.bin`.
- **⚠️ WHY the rebuild was needed (root cause, don't reintroduce):** the ORIGINAL `fno_history.json` keyed
  membership off **today's** tickers → it (a) **dropped every name since delisted/renamed** from the ENTIRE
  history (look-ahead/survivorship bias — IDFC, GMRINFRA, L&TFH, MCDOWELL-N, MOTHERSUMI, PVR, IBULHSGFIN,
  INFRATEL, TATAGLOBAL, ZOMATO), (b) **back-labelled old snapshots with modern tickers** (e.g. GMRAIRPORT shown
  in 2015), and (c) **leaked index derivatives into the stock universe** (CNXIT, DJIA, S&P500, INDIAVIX, INDUS…).
  Rebuilding straight from the bhavcopy fixes all three. After ANY rebuild, audit: every membership symbol must
  have a price series in `sf_stock_data.bin['data']` (0 without), and renamed pairs must hand off with no overlap.
- **⚠️ Before claiming a stock "isn't in F&O," verify against NSE live `fo_mktlots.csv`** (memory: project-stocks-fno-stale).
- **⚠️ POINT-IN-TIME NAMES NEED A FUNDAMENTALS ALIAS.** F&O membership + delisted price series use the
  ticker that traded THEN (e.g. `TATAMOTORS`, `PVR`, `GMRINFRA`), but `sf_fundamentals.json` is keyed by
  the CURRENT name (`TMPV`, `PVRINOX`, `GMRAIRPORT`). Without a bridge, every renamed stock returns
  `null` profit for its old-name era → silently dropped from any profit-ranked/filtered backtest (hit
  Tata Motors: excluded from Jan-2024, understated a strategy's CAGR by ~5pp). FIX: `FUND_ALIAS` (old→current)
  + `fundFor()` fallback in `profitAt`/`profitMetrics`, in **both** `docs/backtest-engine.js` AND
  `docs/stock-backtest.html` (self-contained — keep in sync, memory: feedback-backtest-engines-sync).
  Index universes use current names so they mostly dodge this, but a few old names leak in — the alias
  covers all universes. Regenerate the map after a new rename (add it to `scripts/_rename_map.json` first):
  iterate `(F&O ∪ index)` symbols that are in `sf_stock_data.bin['data']` but NOT in `sf_fundamentals.json`,
  resolve via `_rename_map.json` (transitively) to a name that IS in fundamentals → that's `FUND_ALIAS`.

---

## 9. PAGE-LOAD PERFORMANCE  (first-load = bytes-before-usable; built 2026-06-30)
Goal: every page fast even for a brand-new visitor (caching can't help a cold first load — ship fewer bytes).
- **Caching:** large data files are fetched WITHOUT `?t=Date.now()` (that buster defeated the browser cache →
  re-downloaded 17 MB every load). Now plain URLs; GitHub Pages' ETag + `max-age=600` keep them fresh (a changed
  daily file re-fetches via a fast 304). KEEP the buster ONLY on tiny version/freshness files: `sf_meta.json`,
  `fii_dii.json`. Don't reintroduce it on `stock_data.bin`/`sf_fundamentals.json`/`nifty*.json`/`fii_fo.json`.
- **Homepage + Mutual Funds = slim payload + lazy full data.** See §4. HTML is a small shell; default view renders
  from a slim file (`dash_slim.bin` ~2 MB / inline scheme list ~0.5 MB); the heavy history (`stock_data.bin` /
  `mf_history.bin`) is lazy-fetched on first real need (`ensureFull` / `ensureHistory`) then cached.
- **The STOCK page never loads the engine's data at all** — it opens from two pre-cut per-stock
  files (~37 KB total) instead of the ~137 MB whole-market load. See **§40**; that is the pattern to
  copy for any future "one entity" page.
- **`loadCore()` reads `dash_slim.bin` (2 MB), never `stock_data.bin` (17.5 MB).** Same
  build_compressed.py run writes both and their `indicesHistory`/`fnoHistory`/`startTs` are
  byte-identical; the 15.5 MB difference is a full price series that `activateSF()` overwrites
  seconds later and nobody reads. Don't "restore" the big file here. (`stock-backtest.html`'s SURV
  mode has its OWN loader and legitimately still wants `stock_data.bin`.)
- **Backtest pages lazy-load the engine** (2 MB core + ~115 MB sf data). `saved-strategies.html` renders its list from
  synced DB data; the engine loads only when Today's Picks opens (`ensureEngine`). `stock-backtest.html` direct
  visit shows the builder instantly via `initUIStatic()` (dates from tiny `sf_meta.json`); Saving needs no data;
  a real Run comes via `bt_load` (loads the engine). `strategy-backtest.html` is DB-only (no engine).
- **Backtest RESULT SNAPSHOTS (instant 👁, Trendlyne-style, synced).** Full computed results are stored once in
  Supabase table `bt_snapshots` via RPCs `bt_snap_get(snap_id)` / `bt_snap_set(secret, snap_id, payload)` (same
  WRITE secret as history; wrappers `btSync.snapGet/snapSet` in `bt-sync.js`). Key = `snapKey(cfg)` =
  `stratIdentity(cfg)+'||'+start+'||'+method`. A run auto-saves its snapshot (`saveSnapshot` in `run()`); the 👁
  on `strategy-backtest.html` opens `stock-backtest.html?view=snapshot` → `renderSnapshotPage()` renders the
  stored result INSTANTLY (no 17 MB load, no recompute; only the live "qualifying stocks" chart is skipped).
  Missing snapshot → falls back to a one-time recompute that caches it.
  → **Pre-bake all saved strategies' backtests:** open `…/stock-backtest.html?bake=all` once (loads engine, loops
    every saved strategy×window, `snapSet` each, skips already-cached, shows a progress bar). Re-run after adding
    strategies — it only computes the new ones.

---

## 10. SECTOR / INDUSTRY INDEX BROWSER  (docs/sectors.html — "Sectors" nav, built 2026-06-30)
A grid of **self-made indexes** (equal- OR mcap-weighted, rebased to 100) for spotting trending sectors —
Trendlyne/Tijori/stockscans style. Levels: **12 Sectors** (BSE macro), **58 Industries** (IGroup),
**191 Sub-Industries** (BSE basic-industry — the fine "Heat Exchangers/Aquaculture" level), **27 NSE Indices**.
Cards show period return + a sparkline vs **Nifty 500** (hover values) + advance/decline breadth; plus heatmap
view, Top/Bottom movers, search, watchlist (localStorage), equal/mcap toggle, CSV export, and a drill-down modal
(multi-period returns + index-vs-Nifty500 chart + constituents table → stock.html).
- **Data load:** boots from **`dash_slim.bin`** (~2 MB, meta + last ~250 days + indicesHistory) for short periods;
  **lazy-loads full `stock_data.bin`** (17 MB) only for 1Y/3Y or the detail chart (`ensureFull()`). Series prices
  are ×100 in BOTH bins. Benchmark = `nifty.json {px}`. Default period **6M**, default level **Industries**.
- **Classification source = `scripts/fetch_classification.py`** → **`docs/sector_classification.json`**
  (`{ticker:{macro,sector,igroup,industry,subgroup}}`, ~4,660 stocks, keyed to stock_data.bin tickers). It pulls
  **BSE `ComHeadernew/w?scripcode=`** which returns 5 levels (Sector→IndustryNew→IGroup→Industry→ISubGroup); the
  page groups by macro/igroup/industry. (This is the SAME endpoint `fetch_sectors.py` uses but keeps ALL levels.)
  Resolves NSE→BSE code via the same ISIN/scrip_id maps (downloads EQUITY_L.csv + BSE ListofScripData masters to
  tempdir itself). Has a **guard: refuses to overwrite if <3000 classified** (a flaky/cloud-rate-limited BSE day
  must not nuke good data). Refreshed **weekly by `.github/workflows/refresh-classification.yml`** (Sat) — isolated
  from the daily price refresh. To refresh by hand: `python scripts/fetch_classification.py` (~3 min, ~4,600 fetches).
- **Side-file by design:** does NOT touch the central stock_data.bin pipeline; the page just joins on ticker.
- **Deploy gotcha (cost me ~5 min):** GitHub Pages = workflow deploy via `.github/workflows/pages.yml` (uploads all
  of `docs/`, triggers on `docs/**` push, `cancel-in-progress`). After push it deploys in ~1–3 min. **Fastly CDN
  caches 404s (~600 s).** If you `curl` a brand-new path BEFORE it deploys, the 404 goes hot at the edge and keeps
  returning 404 even with `?cb=` for up to 10 min — NOT a real failure. Don't hammer the path pre-deploy; verify
  the deploy via `gh run list --workflow="Deploy site to Pages"` (look for your headSha = success), then check the
  URL once. (The legacy `pages/builds` API is stale/irrelevant here — this repo uses build_type=workflow.)

---

## 11. RESULTS SEASON CHART  (Trendlyne-style market earnings pulse; built 2026-06-30, SELF-UPDATING)
**Lives as the "📈 Season Trends" tab of `docs/quarterly-results.html`** (merged 2026-07-18, per user — was the
standalone `docs/results-season.html`, which is now a redirect stub to `quarterly-results.html?tab=season`; the
tab lazy-fetches `results_season.json` ~210 KB only when first opened, so the hub's first-load is unchanged; its
JS is `rs`-prefixed: `rsRenderChart/rsRenderTable/rsInit…`, reusing the hub's `esc/fmtPct/pctCls`).
Dark grouped-bar chart: per quarter (**Mar-2019 → latest, 29 quarters**), the MEDIAN YoY % across
reporting companies for **Revenue, Operating Profit, PAT**, with the reporting count in each x-label. Value labels on
each bar, hover tooltips, hand-rolled SVG (no chart lib), + a quarter-detail table. For many quarters the SVG renders
at natural width and the wrapper scrolls (so bars stay readable); it shows the **COVID crash (Jun-2020: PAT −56%) and
V-recovery (Jun-2021: +48%)**.

- **Universe (user-confirmed = "Trendlyne-match"):** currently-listed (`alive`) names with **median daily
  turnover ≥ ₹1 cr** over the last ~250 sessions (close×volume from `sf_stock_data.bin`). ≈1,434 names →
  ~1,160–1,410 reporters/qtr, bracketing Trendlyne's counts (their Jun-2023 ≈1,202 vs ours 1,158). A clean ₹-turnover
  floor, not an index. (Full coverage would be ~2,300–2,500/qtr — broader than Trendlyne; we deliberately narrowed.)
- **UNIVERSE DROPDOWN (28 options, user wanted index-wise):** the "All liquid" set above PLUS all **27 NSE indexes**
  (Nifty 50/500/Midcap/Smallcap/sectoral…) from **`scripts/indices_history.json`** — `{index:[{effectiveDate,symbols}]}`,
  committed, refreshed weekly by `refresh-membership.yml`. Membership is **POINT-IN-TIME** (`membersAsOf(index, qe)` =
  the snapshot whose effectiveDate ≤ the quarter-end), so no survivorship bias (today's Nifty 50 is NOT applied to 2019).
  The join is ~100% (renames via `_rename_map.json`). Financial-heavy indexes (Nifty Bank/PSU Bank) show **PAT only**
  — banks have no comparable rev/op, and `MIN_N=5` suppresses medians over <5 cos. `build_results_season.py` emits
  `{defaultUniverse, universeAsOf, updated, universes:[{key,label,note,quarters:[{...,rev:{median,n,total,tn},op,ebit,pat}]}]}`.
- **`universeAsOf` DATES THE TURNOVER SNAPSHOT — IT IS NOT A PRICE DATE** (renamed from `dataAsOf` 2026-08-10).
  Nothing on this chart is priced: the bars are rev/op/PAT YoY out of `sf_fundamentals` + `sf_revop`, whose freshness
  is `updated` (the "data refreshed …" line). The price bin is read for exactly ONE thing — the ≥₹1cr/day turnover
  screen that picks the **`liquid`** universe's members. Index universes take members from `indices_history.json` and
  never touch it, so the page must NOT stamp this date on them. It used to: the caption appended
  `· prices as of <date>` to EVERY universe, so Season Trends read **"Nifty 500 · prices as of 2026-06-13"** over a
  payload rebuilt 20 minutes earlier — a date that was wrong twice over (nothing priced, and the bin plays no part in
  Nifty 500). Now only the liquid universe shows it, worded `· universe as of <date>`.
- **The committed `docs/sf_stock_data.bin` is FROZEN — never date anything user-facing off it without saying so.**
  `refresh-market-mood` overwrites it in the runner and deliberately never commits it, and the real bin is ~193 MB —
  past GitHub's 100 MB file cap — so it CANNOT be committed fresh. It last advanced on a **hand** heal (2026-06-19,
  bars to 2026-06-13) while live prices ran to 2026-08-07. Left to it the liquid universe was **4.8% wrong** (of 1,433:
  41 newly-liquid names missing — SBIFUNDS, TURTLEMINT, MANIPALHOS … — and 28 gone-illiquid still counted), drifting
  further every day. Fix: `scripts/bake_liquid_universe.py` cuts a ~14 KB `docs/liquid_universe.json`
  `{asOf, floorCr, window, symbols[]}` inside **refresh-backtest-data.yml** (the one job that HAS a fresh bin, right
  after the append step) and commits it with the version marker; `build_liquid_universe()` reads that sidecar and falls
  back to the frozen bin **with a loud ⚠ line** when it is missing. Any other job needing a fresh universe should read
  the sidecar — do NOT add a 193 MB release download to a job that runs 48×/day.
- ✅ **RECENCY GUARD — CLOSED 2026-08-10** (was OPEN the same day). The turnover screen took each symbol's *last 250
  bars whenever they happened*, so **8 symbols whose series had stopped** sat in "currently-listed companies trading
  ≥₹1cr/day" on turnover measured from bars up to **17 years old** — SPSL (last bar **2009**-05-04), RDEL (2017),
  HDIL (2020), HSIL (2022), SPICEJET + LANCER (2023), HINDMOTORS (2025-10-01), RAJESHEXPO (2025-12-24). `alive` cannot
  catch them: bin meta is frozen with the bin, so it reads stale-True. **Fix:** `RECENCY_DAYS = 60` in
  `scan_bin_universe()` — a member's LAST bar must be within 60d of the bin's **own `end`** (never today's date, or a
  frozen fallback bin would screen out its entire universe). It lives in `scan_bin_universe()` on purpose, so the
  sidecar bake AND the frozen-bin fallback are both guarded from one place; `bake_liquid_universe.py` stamps
  `recencyDays` into `docs/liquid_universe.json` as the guarded-bake marker (a sidecar has no per-symbol dates, so it
  can't be re-screened on read — a pre-guard file is still used, with a loud ⚠, and self-heals on the next append).
  - **⚠️ THE OLD NOTE HERE CALLED THESE "long-dead tickers". THAT WAS WRONG — never repeat it.** Checked 2026-08-10
    against NSE's own `EQUITY_L.csv` + Yahoo: **all 8 still trade.** Two real causes, both worth knowing:
    · **Left the NSE cash segment, still on BSE** (absent from EQUITY_L.csv, `.NS` empty, `.BO` full): SPSL, RDEL
      (→RNAVAL), HSIL (→AGI), SPICEJET, LANCER, HINDMOTORS, CRANESSOFT. This bin is NSE-sourced, so it CANNOT measure
      their turnover — dropping them is the screen finally telling the truth.
    · **NSE-listed but in series `BZ`**, which OUR OWN ingestion discards: `build_sf_data.py:73` keeps only
      `("EQ","BE")`. Measured: **38 of NSE's 39 BZ symbols are stale in the bin, vs 0 of 2,086 EQ and 0 of 285 BE.**
      That is HDIL and RAJESHEXPO — trading on NSE today, invisible to us since they were penalised into BZ. The guard
      is right to drop them (we have no current turnover), but **the BZ feed gap is a separate OPEN defect.**
  - **Why 60 days — measured, not taste.** Age-of-last-bar is bimodal with **nothing in between**: committed bin (end
    2026-06-13) 1,426 of 1,434 passers ≤7d then a hole to 171d; fresh release bin (end 2026-08-07) 1,441 of 1,447 ≤7d
    then a hole to 226d. **Every cutoff from ~8d to ~170d gives identical membership**, so 60d (≈40 sessions) leaves a
    wide margin for a real trading halt. Only the LAST bar is tested, never gaps — a stock suspended then **resumed**
    has a recent last bar and stays in, which is correct.
  - **Measured membership delta** (guard isolated, same data both sides): frozen bin **1,433 → 1,426** (−7: HDIL,
    HINDMOTORS, LANCER, RAJESHEXPO, RNAVAL, SPICEJET, SPSL); fresh bin **1,447 → 1,441** (−6: + CRANESSOFT, − RNAVAL/SPSL
    which that bin already marks dead). 8 bin keys drop but only 7/6 names: HSIL's stale key collapses onto **AGI**,
    which is fresh and stays. **All 27 index universes are byte-identical** — they take members from
    `indices_history.json` and never touch the bin.
  - **Chart effect:** `reported` falls 2–6 per quarter (e.g. Dec-2025 1,415→1,412); **medians move ≤0.5pp** — a 7-of-1,433
    change can't move a median. **Totals move a lot in three quarters, and that is the real prize:** total PAT
    Mar-2019 **−72.9% → −2.1%**, Dec-2019 **−99.5% → +10.1%**, Mar-2020 **−97.6% → −6.6%**.
  - **⚠️ Those three bars were never economics — they were ONE corrupt company.** `LANCER` carries rupees-stored-as-crore
    PAT cells in `sf_fundamentals`: Mar-2018 **21,816,965**, Dec-2018 **34,903,220**, Mar-2019 **5,789,416** (neighbours
    are ₹1.7–2.5 cr, so these are ÷10⁷ — the scale-step class, §-see scale_fix.json). A ₹34,903,220-crore denominator
    drags `sum(ago)` so far that `Σnow/Σago−1` pins to ≈−100%. The guard removes LANCER from the *liquid* universe so the
    bars read sanely, **but it MASKS rather than heals — the corrupt cells are still in `sf_fundamentals` and still wrong
    on LANCER's own stock page.** LANCER is in no index, so no index universe was ever affected. Heal via `scale_fix.json`.
- **MEDIAN ↔ TOTAL toggle (2026-07-01, user compared to Trendlyne):** each metric carries BOTH `median` (per-company
  YoY, consolidated-preferred, positive base — "typical company") AND `total` (aggregate Σnow/Σago−1,
  **CONSOLIDATED-preferred** — the old "standalone" wording here was stale; con-pref is what reconciles to TL's
  cards, see the agg_quarter comment in build_results_season.py).
  Trendlyne's result-analysis page shows the **TOTAL** number — verified: our total = Nifty 500 Q4FY26 rev
  **9.6%** / op **8.6%** vs Trendlyne **9.0% / 8.1%** (residual = live-vs-frozen reporting cut-off). The median reads
  HIGHER (rev 13.8%) because it's equal-weight (giants that grow slower don't dominate). Page has a Median/Total toggle.
  `agg_total()` sums only companies **profitable in BOTH periods** (base AND current > 0) — a loss/near-zero base blows
  the ratio up (MidSmallcap400 PAT read 72%), base-only filtering swings it negative. ⚠️ **TOTAL PAT is still unreliable
  at index level** — one-off/exceptional items + standalone-vs-consolidated wreck it: e.g. Nifty 50 Q4FY26 total PAT
  reads −5.8% purely because **ITC's standalone base had a huge ITC-Hotels demerger gain** (₹19.6k→5.1k cr), Bharti/
  Reliance-standalone similar. That's exactly why Trendlyne shows profit COUNTS, not a total-PAT%. Total Revenue + Total
  Op (EBITDA) ARE reliable (Nifty 500 9.6%/9.8% ≈ Trendlyne 9.0%/9.9%); for PROFIT, use the Median or a pos/neg count.
- **⚠️ CORRECTED 2026-07-11 — `op` = Trendlyne 'Oper Profit', NOT 'EBIDT' (the 2026-07-01 note below was WRONG):**
  Trendlyne shows TWO operating-profit cards — **EBIDT** (EBITDA, incl. other income) and **Oper Profit**. The old note
  guessed our `op` (idx2/3 = PBET+FC+Dep−OI) matched **EBIDT** from an *ambiguous* Q4FY26 quarter where the two cards were
  ~1pp apart. **Live per-stock verification (2026-07-11, via the user's logged-in Trendlyne)** settles it to the PAISA:
  Trendlyne's per-stock **"Operating Profit Qtr"** column == our `op` exactly (INDIANB 5588.06, LTF 3238.64, TCS 18556),
  and its aggregate == the **"Total Oper Profit Growth"** card (Midcap100 19.1 = our 19.1; N500 Q1FY27 13.3 = our 13.3;
  7/7 indexes exact). So `op` = **Oper Profit**, full stop.
  - **⚠️ EBIDT is NOT reproducible (2026-07-11, tried and reverted — don't re-attempt):** Trendlyne's **EBIDT** card is a
    genuine-EBITDA-flavoured number but a PROPRIETARY backend calc — Trendlyne does not even expose a quarterly EBIDT row
    per stock (its P&L is Rev · OpExp · OpProfit · Dep · Interest · PBT · Tax · NetProfit — no EBIDT line). Attempted
    `ebidt = PBET+FC+Dep (= op + OtherIncome)`; the **single-stock proof kills it**: Nifty 50 = TCS only, our ebidt YoY
    **8.6%** vs Trendlyne's EBIDT card **5.0%**; no combination of TCS's P&L (con OR std, ±dep, ±other income, PBT vs PBET)
    hits 5.0 (rev 13.9 / op 10.0 / ebit 11.6 / ebidt 8.6 / pat 4.6 — none = 5.0). Gap holds across indexes (N500 8.7 vs 11.0,
    N200 8.6 vs 9.8). So we can match Trendlyne on **Revenue + Oper Profit + PAT**, NOT EBIDT.
  - **Fully reverted (2026-07-11, per user):** built an `ebidt`=op+OI field + EBITDA column, then removed ALL of it once the
    single-stock proof showed it ≠ Trendlyne's EBIDT. `sf_revop` stays **9 elems** `[revStd,revCon,opStd,opCon,patStd,patCon,
    fin,ebitStd,ebitCon]` (readers pad legacy 7-elem). No `ebidt` in the code, data, or UI. **Don't re-add an EBITDA column
    expecting Trendlyne parity — it's proprietary/irreproducible.** Chart bars: **Revenue / Operating Profit / PAT**; detail
    table: Revenue · Operating Profit · EBIT · PAT.
- **⚠️ COVID-2020 RECONSTITUTION WAS NULLED (2026-07-10, verified from press releases — don't reintroduce):**
  the Mar-2020 reshuffle (announced 18-Feb/12-Mar/19-Mar, eff 2020-03-27) was deferred (ind_prs23032020) and
  declared **"shall stand null"** (ind_prs13052020) — EXCEPT Nifty 50/Bank (rebalanced early eff 2020-03-19,
  Yes Bank scheme). The REAL reconstitution is **ind_prs10062020, eff 2020-06-26**, with UPDATED lists
  (adds ALKYLAMINE/DHANUKA/GMMPFAUDLR/SUMICHEM/ABB/CSBBANK/…; keeps BLISSGVS/IFCI/NFL). `build_changelog.py`
  has stem 10062020 in FILES + a COVID-null pass dropping 18022020/12032020 events (Nifty 50/Bank redated
  2020-03-19). Do NOT add stem 19032020 (nulled too). Found via the StockView cross-audit.
- **⚠️ web.archive.org: curl/scripted HTTP BLOCKED but the claude-in-chrome BROWSER reaches it** (navigate, then
  page-context fetch to `/cdx/search/cdx` works). ind_nifty500list.csv has only ONE 2020 snapshot (2020-07-25).
  To hunt a missing press release: probe every weekday stem `ind_prs<DDMMYYYY>[_1|_2].pdf` on live niftyindices
  (GOTCHA, Windows-era: stem lists generated there carried CRLF — `tr -d '\r'` before use or every URL silently 404s).
- **⚠️ MEMBERSHIP COMPLETENESS (2026-07-01 — user: "fetch every rebalance, none missed"):** `indices_history.json`
  is reconstructed from **niftyindices reconstitution press-release PDFs** (`ind_prs<DDMMYYYY>.pdf`), parsed by
  **`build_changelog.py`** → `_changelog.json` (per-index add/drop events), then **`build_membership_v2.py`** walks
  each index's TODAY list backward through the events (`reconstruct()`). Was capping at **7 indexes** because the
  parser's `CANON` map only had 7 — the PDFs list ALL indexes' changes, they were just skipped. Fixed: `CANON` + `SLUGS`
  now cover all **27** indexes. VERIFY with a **fixed-size check** (`verify_sizes.py`): every broad index must equal its
  exact size (Nifty 50=50 … 500=500) at every date. Post-fix: Nifty 50/100/500/Next50/Midcap50/LargeMidcap250/
  MidSmallcap400 = EXACT; Nifty 500 validates 100% vs archived lists. Residual (immaterial to a 100-250-co median, and
  clears up by ~2022): Nifty 200/Midcap 100/150 off by ~2, Smallcap 50/100/250 off by a few in 2020-21 — mostly COVID-era
  delisted stocks the backward-walk can't restore without archived full lists (**web.archive.org is UNREACHABLE from
  this env** — CDX enumeration/Wayback checkpoints blocked; niftyindices live PDFs DO work). Re-run: `build_changelog.py`
  (auto-probes recent 80d + the hand-maintained FILES stem list) → `build_membership_v2.py` → `verify_sizes` → rebuild.
  **⚠️ SMALLCAP-250 GAP FIXED WITHOUT WAYBACK (2026-07-01) via the NSE size-PARTITION.** Nifty 500 = Nifty 100 (+)
  Midcap 150 (+) Smallcap 250; MidSmallcap 400 = Nifty 500 − Nifty 100. Since Nifty 500 is validated 100% + Nifty 100
  is exact, `build_membership_v2.py` now **DERIVES** Smallcap 250 (= N500 − N100 − Midcap150) and MidSmallcap 400
  (= N500 − N100) after reconstruction — this RECOVERS the 2020-21 delisted small-caps the press-release walk missed
  (Smallcap 250 went 233 → ~250). Can't derive Smallcap 50/100 or Midcap 100 (they need per-stock mcap RANK within the
  band, which isn't a set operation) — those keep a small early-2020 over-count (a few EXTRA, not missing; immaterial).
  **⚠️ 2015-2019 N500 REVIEWS = HUNT OVERLAY (fixed 2026-07-28, commit 9d8926d).** The six semi-annual reviews
  Mar-2017..Sep-2019 (+ Mar-2015) use pre-2016 PDF layouts `parse_pdf` can't read — without them Nifty 500 collapsed
  to 432-489 members across 2015-2018 (~37% wrong at worst) and carried the Mar-2019 review unapplied (sizes LOOKED
  fine — only a content diff catches that class). `build_changelog.py` now overlays `_n500_hunt_prs.json`
  (era-aware parse of all 24 hunted docs, FORCE-TRACKED like _wb_n500_snaps.json — if it goes missing the overlay
  prints a WARNING and 2015-2019 silently regresses). Residual: ~15-16 drift 2015-03→2018-10 (no archived full list
  in that window to pin; irreducible from primary sources); Oct-2019+ exact. `verify_sizes.py` checks N500 back to
  2015-04 (tol 17). LESSON (the 993cfce trap): that commit's message claimed the official-anchor pins were in
  `build_membership_v2.py`, but the edit was only ever STASHED (stash@{0}) — the next weekly refresh silently
  rebuilt without it. Recovered 2026-07-28. After editing membership code, `git show HEAD --stat` and confirm the
  .py files you changed are actually IN the commit before trusting the next CI rebuild.
- **PAT** = `docs/sf_fundamentals.json` (owners-attributable con where filed, else std — same basis as the backtest,
  memory project-stocks-profit-basis). **Revenue + Operating Profit** live in a PARALLEL dataset
  `docs/sf_revop.json` = `{SYM:{QE:[revStd,revCon,opStd,opCon,patStd,patCon,fin,ebitStd,ebitCon]}}`
  (9 elems; readers pad legacy 7-elem rows), derived from the SAME NSE XBRL:
  **Operating Profit = ProfitBeforeExceptionalItemsAndTax + FinanceCosts + Depreciation − OtherIncome** (= Trendlyne's
  "Oper Profit", paisa-matched per-stock — NOT their proprietary "EBIDT"); **EBIT = op − Depreciation** (idx7/8).
  Banks/NBFCs (`InterestEarned`) excluded from Rev/Op (kept in PAT); their EBIT blank.

- **INSURERS INCLUDED (2026-07-16, Trendlyne-parity):** insurers DO file XBRL since integrated filing
  (`INTEGRATED_FILING_LI_*` life / `_GI_*` general, in-capmkt ns, ~Mar-2025+; 11 insurers backfilled from the
  cache via `backfill_insurer_revop.py`, re-runnable). `build_revop.metrics_for` has LI/GI branches:
  **rev = NetPremiumIncome + IncomeFromInvestmentsNet + InvestmentIncome** (LI) or **PremiumEarned + InvNet +
  ShareholdersAccountIncomeFromInvestments** (GI) — **EXACT vs Trendlyne's 'Operating Revenue'** (HDFCLIFE con
  33758.51, ICICIPRULI 28512.71, ICICIGI 7088.22 all verified to the paisa). **op**: GI = rev − OperatingExpenses −
  NonOperatingExpense (EXACT, ICICIGI 522.10); LI = PBT − other income both accounts (**CLOSE, NOT exact** — TL's
  life-insurer op is PBT−OI off their own PDF-parsed rows and is wildly volatile per their own quarterly table
  (HDFCLIFE op: 505.8 / 121.8 / 148.9 / −46.5 across FY26) — irreproducible like their EBIDT; ours is the sane
  version). Insurer PAT also auto-parses now (`build_fundamentals.xbrl_profit` insurer-tag fallback:
  ProfitLossAfterTaxAndExtraordinaryItems / ProfitLossAfterTax — HDFCLIFE con 611.19 = TL exact). fin=1.
  **⚠️ the updater's old `er[6]` fin-flag skip was REMOVED** — it permanently blocked an insurer/bank's second
  basis (HDFCLIFE con stayed None once std was stored).
- **Recent-IPO year-ago bases (Jun-2025 for GROWW/EMMVEE/ICICIAMC done 2026-07-16):** NSE XBRLs carry ONLY the
  current quarter (no comparative context), so a newly-listed name has no year-ago base → excluded from YoY until
  backfilled. Trendlyne reads the year-ago COMPARATIVE COLUMN printed in the same results PDF. **NOW AUTOMATED —
  §25 (`backfill_ipo_bases.py`, nightly, PAT + revenue)**; only OP bases (op = PBT+FC+Dep−OI) still need the manual
  read when a fresh IPO must feed this chart (mind units: GROWW cr, EMMVEE lakh, ICICIAMC million); write those into
  sf_revop.json + revop_fundamentals.json. NSE symbol gotcha: ICICI Pru AMC = **ICICIAMC** (not ICICIPRAMC).
- **In-progress quarter membership = LATEST index snapshot** (build_results_season member_fn_live) —
  reconstitutions land mid-season (ICICIAMC entered N500 2026-07-17) and Trendlyne counts current members;
  completed quarters keep the point-in-time quarter-end snapshot.
- **⚠️ NBFC `OtherRevenueFromOperations` (ORFO) is INSIDE revenue — do NOT net it off (bug found+FIXED 2026-07-17):**
  ORFO hangs off `DetailsOfOtherRevenueFromOperationsAxis` (contexts `OneRevenue<n>D`) as a **component of the
  Revenue-from-operations block**: every NBFC filing satisfies `Income == RevenueFromOperations + OtherIncome` with
  ORFO already inside, and Piramal's own Q1FY27 PDF prints it as a sub-line ABOVE "Total Revenue from operations (I)".
  The old blanket `rev = RevenueFromOperations − ORFO` (and the same term off op) was reverse-engineered from **LTF
  alone**, where it matched TL to the paisa — but LTF is a MIS-FILING: its CON xbrl puts 30.39 labelled literally
  "Other Income" inside the revenue block and leaves OtherIncome=0, while its own STD xbrl tags the same ~30cr as
  OtherIncome and keeps it out of revenue. Generalising that one-off **deleted real revenue AND op from every other
  NBFC by the identical amount** (rev gap == op gap is the fingerprint): HDBFS Q1FY27 4,937.90→4,586.57 (−351.33;
  growth read **+2.7% vs true +10.6%**), PIRAMALFIN con 3,368.27→3,268.66 (−99.61). **FIX =
  `build_revop.orfo_other_income()`**: subtract ONLY the components the filer itself labels "Other income" via
  `DescriptionOfOtherRevenueFromOperations` ("Other **operating** income" / "Other financial charges" are REVENUE —
  the matcher is deliberately strict). All three are now TL-exact: HDBFS 4937.90/2863.15, LTF con 5212.92/3238.64,
  PIRAMALFIN con 3368.27/2072.74.
  - **Blast radius was SMALL (~20 cells)** — the rule only landed ~2026-07-10, so only cells `update_fundamentals`
    WROTE after that date are corrupt: of 1,015 re-checked NBFC cells **973 already held the correct value**, and MFSL
    (whose entire 10,802cr premium line is ORFO-tagged — it would have read **79.43**) was never hit. Repair recipe if
    it recurs: re-fetch `integrated-filing-results` (ONLY `*_NBFC_INDAS_*` filings can be affected — the industrial /
    bank / insurer branches never touch ORFO; the `_xbrl_cache` has **zero** ORFO filings, so a full rebuild can't fix
    or cause this), recompute both rules, overwrite a cell **only where stored == the OLD-rule value** (30 cells matched
    neither rule — they came from other sources; leave them).
- **PIRAMALFIN (PEL merger) Jun-2025 base filled 2026-07-17:** Piramal Enterprises (**PEL**) reverse-merged into
  Piramal Capital & Housing Finance, renamed **Piramal Finance (PIRAMALFIN)** — NSE filings run PEL→30-Jun-2025 then
  PIRAMALFIN→31-Dec-2025, so PIRAMALFIN had rev/op from Dec-2025 only and **dropped out of YoY entirely**.
  `_rename_map.json` does NOT help (it is auto-generated by `build_sf_data.py`, and `build_results_season` uses it for
  **membership only**, never to join fundamentals history). Base read off the Q1FY27 PDF comparative column (std p7 /
  con p16) with the CURRENT column anchored to our XBRL (std rev 3409.79 / PBET 421.08 / PAT 439.92 all exact):
  `sf_revop["PIRAMALFIN"]["20250630"] = [2658.04, 2639.25, 1756.76, 1720.62, 263.25, 276.37, 1, None, None]`
  (op = PBET+FC+Dep−OtherIncome; con = 222.67+1491.71+57.10−50.86 = 1720.62). TL's implied base 2639.29/1720.69 ✓.
  ⚠️ **PEL's own stored Jun-2025 (rev 2642.67) is the PRE-merger entity's originally-reported figure — do NOT lift it**;
  the PDF comparative (2639.25) is what the merged entity restates and what TL reads. (PAT base already existed.)
- **⚠️ NEGATIVE-BASE CONVENTION — aligned to Trendlyne + our own backtest (2026-07-17, user-directed):**
  the site was internally inconsistent: `docs/stock-backtest.html` + `docs/backtest-engine.js` (`profitYoyPct`) have
  ALWAYS used **`(cur−base)/ABS(base)`** ("divide by |base| so loss→profit reads positive") = Trendlyne's exact rule,
  while `build_results_season.py` required a POSITIVE base and silently dropped those companies. Now aligned:
  - **`median_yoy()` = `(cur−base)/abs(base)`, skip base==0 only** (was `base > 0`). BHEL Q1FY27 = +193.80% —
    the exact figure TL prints — instead of vanishing. Mirrors `profitAt()`; keep the three in sync.
  - **`agg_total()` sums EVERY company that reported both periods, negative bases INCLUDED** (was both>0). Nifty 500
    Jun-2026 op **18.07 → 19.90** vs TL's card 20.70 (residual = the two life insurers). Revenue is unaffected
    (no negative revenue bases exist).
  - **`drop_nonpos=True` for PAT ONLY.** Net profit is a RESIDUAL that cancels toward zero at index level, so summing
    losses in collapses Σbase and the ratio explodes — measured: **201 of 809 total-PAT cells moved >25pp,
    Smallcap-250 Mar-2021 read 9,841%**. TL never publishes a total-PAT card *for this very reason* (it shows profit
    COUNTS), so there is nothing to match. PAT totals stay byte-identical to the old rule.
  - **DEGENERACY GUARD (all metrics): `Σbase >= 0.25 × Σ|base|`, else None.** Even summing everything, a COVID-shut
    base can leave Σbase a near-zero residue of cancelling signs (Consumer Durables Jun-2021 op read 2,825%). Cost:
    2 op + 6 ebit cells blanked out of 809; `|>300%|` op cells 3 → 1 (Nifty Metal Jun-2021 338.6% is real, kept).
- **⚠️ NEVER back-derive a TL base as `cur/(1+g/100)` — TL uses `(cur−base)/ABS(base)` when the base is NEGATIVE.**
  That mistake invented two phantom findings on 2026-07-16 (both since disproved): "TL's BHEL base is +171" and "TL's
  op CARD ≠ Σ of TL's own table". **Decode the sign from TL's own margin columns instead:** BHEL Q1FY27 margin 6.55%
  with "Margin% YoY Change" +16.34 → base margin = **−9.79%** × base rev 5487 = **−537.4 = our −537.14**. Their
  +193.80% is exactly (503.86−(−537.14))/537.14. **TL's base IS ours**; BHEL is NOT a data difference.
- **Known LIVE-quarter residuals vs TL (Jun-2026; re-bridged 2026-07-17 POST-ORFO-fix, SAME 35 declared cos):**
  N500 totals ours **rev 19.6 / op 18.1** vs TL cards **19.9 / 20.7**; **29 of 35 companies agree within 0.3pp on
  BOTH**. Every pp is now accounted for — no mystery left:
  - **REV −0.3pp:** MRPL +0.30 (XBRL RevenueFromOperations is gross 41,609/20,988 = +98%, TL nets excise off the PDF
    38,254/17,356 = +120%, no excise tag exists — keep); ICICIAMC +0.01 (**OURS IS RIGHT** — base 1,330.67 IS the
    PDF's printed Jun-25 column, 13,306.7 million ÷ 10; TL's implied 1,313.4 is off. Don't "fix" toward TL).
  - **OP −0.8pp (was −2.6pp before the 2026-07-17 convention fix, below) = HDFCLIFE +0.52 / ICICIPRULI +0.26**,
    life-insurer op irreproducible (above). Their rev + PAT are exact. Nothing else is left.
  - **EMMVEE** (op base 350.49 vs TL's implied 347.4, ~0.01pp): **OURS IS RIGHT** — derived from EMMVEE's own PDF
    comparative, PBT 240.19 + FC 53.11 + Dep 71.59 − OI 14.40 = 350.49 (lakh ÷ 100); our current op 548.10 = TL exact.
  - **REFINER EXCISE NETTING (built 2026-07-17, closes the MRPL rev gap):** `scripts/fetch_excise.py` reads the
    PDF-only "Excise Duty" row (no XBRL tag exists — full fact-dump proof) and nets it off the rev slots for the
    **VERIFIED-ONLY** symbol list inside it (MRPL today; add IOC/BPCL/HPCL/CPCL **only after** checking TL's own
    per-stock "Operating Revenue Qtr" for them — the ORFO lesson: never generalize a presentation rule). Anchored:
    PDF revenue cells must match the STORED gross (±0.5%, unit scales) to map columns; one PDF nets cur+prec+yago.
    Ledger `scripts/excise_duty.json` {SYM:{QE:{gross,excise,src}}}; apply is idempotent (only rewrites a slot that
    still equals the ledger gross) — **after any full build_revop rebuild re-run `fetch_excise.py`** (gross comes
    back). op/EBIT/PAT untouched (excise already sits inside PBET). Horizon `MIN_NET_QE=20220630` (one year before
    the quarterly-results window, so every YoY/QoQ pair it shows is net/net; older pairs stay gross/gross =
    self-consistent). ⚠️ The pairs WHOSE BASE SITS BELOW THE HORIZON (the horizon year — for MRPL the Sep-2021→
    Jun-2022 bars after a free yago-fill extended it) are net-cur-vs-gross-base → they UNDERSTATE that stock's
    growth in those old bars; ~1/8th of the stock's own contribution, <0.1pp at index level — accepted, don't
    chase. TWO apply gotchas encoded in fetch_excise.py: (1) column anchoring restricts candidates to
    [fetch_qe−1y, fetch_qe] — MRPL's revenue was ~28.4k cr in Mar-2026 AND Dec-2023 AND Sep-2022, so an
    unrestricted unique-match rule skipped exactly those three; (2) the PDF comparative and the stored XBRL can
    differ by paise-level REVISIONS (Jun-25: 20,988.53 vs 20,988.03) — apply subtracts excise from the STORED
    value under the anchor tolerance (idempotency-guarded), never replaces it with the PDF figure. Wired into
    refresh-fundamentals.yml (gated on docs/.fund_updated; ledger rides the /tmp cp-back commit list §18-style).
  - **DEEP RE-VERIFY 2026-07-17 (user challenged "TL is a big firm, can't be wrong") — every claim re-proven from
    the companies' own Q1FY27 PDFs:** (a) **MRPL: TL CONFIRMED paisa-exact** — the PDF prints an `Excise Duty`
    expense row (3,354.77 cur / 3,631.80 base); 41,608.96−3,354.77 = 38,254.19 = TL's figure, 20,988.53−3,631.80 =
    17,356.73 = their implied base. TL right off the PDF, we right off the XBRL (full fact-dump shows NO excise tag;
    expenses sum exactly with excise inside OtherExpenses). (b) **EMMVEE + ICICIAMC re-proven OURS** — full-page
    dumps show NO exceptional-items row in either statement (PBT = TotalIncome − TotalExpenses to the paisa:
    EMMVEE 1,04,222.38−80,203.16 = 24,019.22; ICICIAMC 14,775.2−4,155.6 = 10,619.6); printed Jun-25 comparatives =
    our bases exactly (35,049.50 lakh op / 13,306.7 mn rev); TL's bases (347.38 / 1,313.35) match NOTHING in the
    current documents — most plausibly RHP-era restated figures (both are 2025-26 IPOs), i.e. an official-but-
    superseded source, and TL's CURRENT-quarter values equal ours exactly on both names. (c) **HDFCLIFE — strongest
    proof: PBT − the rows LABELLED "Other income" in HDFC Life's own PDF = OURS to the paisa on BOTH bases**
    (con 630.19−9,458L−4,273L = 492.88 cur, 563.58−7,388L−2,123L = 468.47 base; std 626.72−94.49 = 532.23); TL's
    450.43/156.41 subtract 179.76/407.17 cr that match NO labelled row or combination, and TL's own financials page
    prints PBT 618.3 vs the PDF's 626.72 (their Total Rev − Op Exp ≠ their own Op Profit row either). (d)
    **ICICIPRULI**: its statement pages are SCANNED images (pages 1-11 have no text layer — TL's parse is OCR'd on
    top of the same regrouping); identical LI format to HDFCLIFE, same conclusion.
  **Page-default gotcha when a user compares:** our page opens on "All liquid" + **MEDIAN** — switch the universe to
  **Nifty 500** AND toggle **TOTAL** before comparing to TL's cards, or the numbers look wrong for no reason.
- **⚠️ POWER-OF-TEN SCALE ERRORS IN THE SOURCE XBRL — the filer's bug, not ours (found+FIXED 2026-07-17):**
  a handful of filings carry **every monetary tag at 10^k times its true value**. Our parsers divide raw rupees by
  1e7 and are CORRECT, so **a re-parse reproduces the garbage byte-for-byte** (measured: today's `build_revop`
  reproduces **263/263** cached flagged cells — **zero** stale artifacts). It is therefore NEITHER a live parser bug
  NOR a stale artifact — don't go looking for one. Proof case **BATAINDIA 20190630**, which filed std and con two
  minutes apart on 2019-08-02: the std file's `FinanceCosts` is **313510000000** and the con file's is **313510000**
  — the same real number, one ×1000 (rev/OI/PBET/PAT track it at 999.3/1003.6/998.3/997.6).
  - **`decimals` is NOT a usable signal:** `digits − |decimals| == 4` in the broken filings AND the good ones (the
    filer derives it from the broken value). BATAINDIA std says `decimals="-9"`, con `="-6"`. Don't build on it.
  - **NOT ONLY `sf_revop` — `sf_fundamentals` is hit too** (BATAINDIA `npStd` was 100730.0), and that feeds the PAT
    median, Discovery, Quarterly Results **and the backtest**. `build_fundamentals` can emit BOTH bases from ONE
    filing, so a std-only mis-filing also poisons `npCon` (IRCTC 20220930, TTML 20190930).
  - **THE FIX = `scale_fix.json`, a REVIEWED LEDGER (14 filings), applied by `scale_fix.py`** — at PARSE time in
    `build_revop.py` + `build_fundamentals.py` (keyed on the cache FILENAME, since the unit of corruption is the
    FILING, so a full rebuild stays clean) and once to the built JSONs via `python -X utf8 scale_fix.py --apply`
    (guarded on the recorded `was_*`, hence idempotent). ⚠️ `--apply` skips a cell another source already fixed:
    **GAEL**'s `npStd` is 14.55 because the BSE gap-backfill overwrote the NSE XBRL's 1455.0 — dividing again would
    have made it 0.15.
  - **⚠️ DETECTION IS NOT AUTOMATABLE — every naive rule DELETES REAL DATA (the ORFO lesson again).** "50x the
    symbol's own median" flags **268** cells of which **~198 are REAL**: collapses (JETAIRWAYS/HDIL — the dead years
    drag the median to ~0 so the healthy years look like outliers), holding-co shells (BINANIIND/ROLTA/DCM std ≈ 0),
    mergers (ABCAPITAL std ~100→~4,000 on the Apr-2025 ABFL merger), one-offs (**SIL** Mar-22 rev 429.97 → op 212.44
    = a textbook ~49% land-sale margin; **SPARC** Mar-26 rev 1853.22 − op 1772.94 = **80.28 of cost, exactly its
    normal quarterly cost base** — a ÷100 would imply costs of 0.8), and recurring dividends (**MAHSCOOTER** spikes
    every **September** — the annual Bajaj dividend; RTNINDIA/WESTLIFE every June). Cross-slot agreement doesn't save
    you either: an investment company has rev == op == pat *by nature*. So `detect_scale_errors.py` only SURFACES
    suspects — **adjudicate by hand, and only with a hard anchor.**
  - **ANCHORS, strongest first:** **(a) YTD** — a Q2/Q3/Q4 filing carries its own year-to-date figure, so
    `(YTD_parsed − quarter_parsed) / Σ(earlier quarters of that FY, from OTHER filings)` **is** the factor, by
    arithmetic; IRCTC/GAEL/GRAPHITE/TTML land on **exactly 100.000** (IRCTC: 165839.23 − 80580.17 = 85259.06, and
    its Jun-22 filing says 852.59). **(b) cross-basis** — the other-basis filing minutes later (BATAINDIA, GHCL,
    BRFL). **(c) neighbours** — only where exactly one power of ten lands every slot in range (SILGO, PBAINFRA).
  - **⚠️ Don't require ">= N quarters" when sweeping** — that hid **GICL 20250930** (×1e5 on BOTH bases, rendering
    ₹3,615,270cr of revenue) outright, because GICL has only 4 quarters. Compare to ADJACENT quarters instead.
  - **KNOWN-UNFIXED (needs its own session, NOT in the ledger):** **QUINT** (Quint Digital, ₹180cr mcap) has ~20
    quarters of net profit that are systematically mis-scaled (reads 20305 / 761217 / −104086 where truth is ~±5cr)
    — a whole-filer problem, not one bad filing, so it needs its own root-cause pass. `detect_scale_errors.py` also
    lists ~251 `sf_fundamentals` np suspects; **most are real one-offs** — AIRFLOA 20240930 (×1e4 on both bases,
    only 2 quarters, ann=0) is the one clean-looking candidate left.

**SELF-UPDATES DAILY** — wired into `.github/workflows/refresh-fundamentals.yml` (21:15 IST weekdays):
1. `update_fundamentals.py` scans NSE integrated-filing-results for the last 120 days (ALL companies, one call) and,
   for each new filing, reads net profit (→`sf_fundamentals.json`) AND rev/op via `build_revop.xbrl_revop(xml)`
   (→`sf_revop.json`), fill-only, no disk cache needed. **Each filing is keyed by its OWN `qe_Date`** — so a LATE
   filing (a March quarter declared in Jul/Aug) lands in the March column, and a new quarter (June) becomes its own
   column. (Insurers = IRDAI format, no XBRL P&L → naturally absent from Rev/Op.)
2. `build_results_season.py` re-aggregates → `docs/results_season.json`. A NEW quarter column auto-appears once
   ≥200 universe companies have reported it; year-ago base must be positive. Reads the daily `docs/` copies (falls back
   to `scripts/` source copies for a local run); turnover universe from the committed (slightly stale, fine) bin.
   **+ vision_fills overlay (2026-07-21):** it ALSO merges `docs/vision_fills.json` as a fallback for quarters the
   XBRL stores lack (basis-aware: a "C" fill only pairs with a con base). Without this, an NSE XBRL lockdown
   (2026-07-20) froze the chart at 44 reporters while the vision-covered page showed 62 — the chart must read every
   store the page reads. XBRL supersedes on its next parse (existing cells are never overwritten).
3. Commit step pushes `sf_fundamentals.json` + `sf_revop.json` + `results_season.json` (rebase loop); the page picks
   up the new JSON on next load. **The season bake + coverage check + season commit are UNGATED (2026-07-21)** —
   they run every refresh-fundamentals run (vision fills arrive from OUTSIDE the workflow, so `.fund_updated` can't
   gate them), and the XBRL fetch step is `continue-on-error` with a final re-raise step, so an NSE lockdown can
   never starve the downstream bakes again (it still turns the run red, LOUD).

**Occasional FULL rebuild** (only if the daily fill-only drifts or you change the derivation): `python -X utf8
build_revop.py` re-walks ALL `scripts/_xbrl_cache/` (~102k files, parallel ProcessPool, resumable via
`_revop_progress.json`, prefilter ≥2018, MIN_QE 20180101, **latest-filing-wins**) → `revop_fundamentals.json` +
`docs/sf_revop.json`; ~98.6% PAT-validated (~90k cells). ⚠️ **OLD INDAS format (pre-~2021) needs `ctx_period()`** —
those filings don't carry the period inside `<xbrli:context>`; they tag `DateOf{Start,End}OfReportingPeriod` per
context. `ctx_period()` reads the context block first, then falls back to those tags (else 2018-20 silently parse as
0 symbols — the bug that capped history at 2022). Dec-2022 has a thinner cache (~1,100 vs ~1,800) — robust median, fine.
- Tunables: chart START quarter = `y, m = 2019, 3` in `build_results_season.py`; `TURN_FLOOR_CR` there (1.0 → ~1,290
  reporters); colours/labels in `rsRenderChart()` inside `docs/quarterly-results.html` (Season Trends tab).

---

## 12. 15:30 FILING-TIME GATE  (no same-day look-ahead; done 2026-07-08)
The backtest rebalances at the **15:30 close** and checks availability as `annDate <= rebalanceDate` at DATE
granularity (`profitAt`, `docs/backtest-engine.js` ~L242 AND self-contained `docs/stock-backtest.html` ~L566 — keep
in sync). So a result **broadcast after 15:30 on the rebalance day** was wrongly treated as available that day =
same-day look-ahead. Fix is **data-side, no engine change**: bump such a filing's ann-date to the next trading day,
so the existing `<=` check is correct. Proof case: **JSL (Jindal Stainless) Sep-2020** filed 2020-10-30 **17:08**
→ before the fix it was picked in Oct-2020 on a +116% YoY; after, it falls back to the Jun-2020 COVID loss and is
excluded. (memory: project-stocks-1530-gate)

**Scope = month-end trading days only.** The look-ahead only bites when a filing's ann-date == a monthly-rebalance
date (last trading day of the month). ~3,760 fundamentals filings land on one; timing them decides the bump.

**Source of filing TIME:** BSE `AnnSubCategoryGetData` `NEWS_DT` (full timestamp, e.g. `2020-10-30T17:08:23.81`).
Query **per-DATE** (all `strCat=Result` announcements that day, 50/page, page to `Table1[0].ROWCNT`), NOT per-stock
— only ~80 distinct month-end dates carry filings, so it's ~300 requests. NSE `integrated-filing-results`
`broadcast_Date` ALSO carries the time (`"16-Jan-2025 20:20"`) but the endpoint serves only ~the current day, so
it's useless for history — BSE is the historical source.

**Tools (persisted, `scripts/_*` intermediates are gitignored):**
1. `python scripts/fetch_filing_times.py` — per-date BSE fetch → `scripts/_filing_times.json`
   `{date:{scripcode:[NEWS_DT,…]}}` (resumable; needs `_gate_dates.json` = distinct month-end dates with filings).
2. `python scripts/gate_1530.py` (dry) / `--apply` — for each `(sym,date)` event resolve SYM→scripcode via
   `bse_scrips.json['by_id']`; **bump iff the MIN BSE broadcast time that day > 15:30** (every result post-close);
   bump target = next day in `_trading_days.json`. **Conservative:** no BSE record, any broadcast ≤15:30, or no
   scripcode ⇒ leave as-is (never wrongly excludes a legit pick). Rewrites ann-date cells (idx2/idx4) ONLY, in
   BOTH `docs/sf_fundamentals.json` AND `scripts/fundamentals.json`; audit log → `scripts/_gate_bumps.json`.
   To rebuild the calendar/events first: from LIVE sf-data (§7.0) build `_trading_days.json` + `_me_days.json`
   (last trading day per YYYYMM), then scan fundamentals for annStd/annCon ∈ month-ends → `_gate_events.json`+`_gate_dates.json`.

**First run (2026-07-08):** 3,760 events → **1,000 bumped** (1,855 ann-cells across std+con), 703 confirmed
before-close (kept), 1,608 no same-date BSE record (mostly a BSE-vs-NSE date mismatch where BSE broadcast
EARLIER = result was public earlier = correctly NOT a look-ahead, e.g. MARUTI our-date 30-Oct filed BSE 29-Oct),
449 no BSE scripcode (tiny/delisted). 962/1000 bumps were 15:46+ (well post-close); only 38 in the 15:31–15:45
straddle. Spot-checked 27 large-caps — all genuine (RELIANCE 18:42, ITC 19:46, TCS 16:36, JSL 17:08…).

**Going forward (automatic):** `update_fundamentals.py` now gates at ingestion via `gated_ann(broadcast_Date)` —
if the NSE broadcast time > 15:30, the stored ann-date is the next weekday (engine-equivalent to next trading day,
since rebalances are always trading days). So new after-close filings need no re-run. Applies to ALL filings (not
just month-end), which also future-proofs weekly/daily frequencies.

**★ THE GATE IS NOW A NIGHTLY, NOT A ONE-OFF (2026-08-11, §88c).** `update_fundamentals.gated_ann`
only guards NSE INGESTION — every backfill writer (detres/vision/aggregator/scale-step campaigns)
stamps ann-dates through its own path and bypasses it, so month-end look-aheads REGROW. Measured
2026-08-11: 3,627 month-end events live (vs 3,760 in July), **172 newly confirmed after-close →
289 ann-cells bumped**, concentrated exactly in the backfilled years (2015: 67, 2016: 57, 2017: 47,
2018: 30 — 201 of 289 in 2015-18). So the gate re-runs EVERY NIGHT in refresh-fundamentals.yml
("15:30 gate re-run (nightly)", hour 15|16 UTC or a manual dispatch), before the commit step:
`build_gate_events.py --calendar` → `fetch_filing_times.py` → `gate_1530.py --apply`.
- **`scripts/build_gate_events.py`** (new) rebuilds `_gate_events/_gate_dates/_trading_days/_me_days`.
  The calendar MUST come from sf price data — a muhurat SUNDAY can be a month's last session
  (2016-10-30), and a generic weekday calendar would put the rebalance on the wrong day. The big bin
  never exists in the fundamentals job, so refresh-backtest-data.yml cuts **`scripts/gate_calendar.json`**
  (tracked, ~70 KB, {tdays, me_days}, `--calendar-only`) the same way it cuts the liquid-universe
  sidecar, and the nightly reads it with `--calendar`.
- **`scripts/filing_times_cache.json.gz`** (tracked, ~131 KB) carries the BSE broadcast times, so a
  quiet night re-fetches ~nothing; only NEW month-end dates hit the API (`fetch_filing_times.py` is
  resumable and skips stored dates). ⚠️ It fetches in `_gate_dates.json` order and OLD dates
  (2008-2014) respond very slowly — sort NEWEST-FIRST for a manual run or it stalls in the archive
  (measured: 11 dates in ~20 min oldest-first vs all 101 in ~15 min newest-first).
- **CI gates `docs/sf_fundamentals.json` only** (the `scripts/fundamentals.json` mirror is not
  committed from CI); a local `--apply` keeps the mirror in step. On 2026-08-11 the mirror took 278
  of the 289 (6 quarters + 1 symbol absent from that thinner cut, 4 ann-cells null) — expected.
- **Idempotence (both signals verified 2026-08-11):** an immediate second pass decides `bumped 0`
  (events drop 3,627 → 3,455 because the bumped ones are no longer month-ends), and JSL's Oct-2020
  proof case reports **NOT FOUND** — the PASS signal, since its ann-date is already 2020-11-02.

**Residual (documented, low-risk):** NSE-only historical filings with no BSE same-date record can't be timed
retroactively (NSE history endpoint is real-time only) → left un-bumped = status-quo conservative. **VALIDATE**
further by re-running the 2020-21 StockView comparison (memory project-stocks-stockview-comparison) — JSL should
drop from Oct-2020; nothing legitimately pre-15:30 should disappear (by construction, before-close filings are never bumped).

---

## 13. HOME PAGE  (docs/index.html — the site landing page, built 2026-07-12)
`index.html` is now a real landing page (was a 1-line redirect to the dashboard). Two parts, ValuePicker-style:
- **Index ticker** (top): Nifty 50 / Nifty 500 / Nifty Bank / India VIX — price, day-change %, weekly RSI(14),
  sparkline. VIX card shows a volatility band (Low/Moderate/High) instead of RSI. A DOWN VIX day correctly
  shows ▼ (don't "fix" it to match ValuePicker, which mislabels VIX up).
- **LIVE intraday (added 2026-07-12):** the ticker is genuinely live during market hours, not just the daily
  close. Two-layer render: (1) seed instantly from the committed daily-close JSONs (`nifty.json` /
  `nifty500.json` / `nifty_bank.json` / `india_vix.json`) so it's never blank; (2) then fetch live Yahoo quotes
  (`^NSEI/^CRSLDX/^NSEBANK/^INDIAVIX`, `interval=1m&range=1d`) and repaint. GitHub Pages is static + Yahoo blocks
  browser CORS, so the live call tunnels through a CORS-proxy CHAIN — **since 2026-07-12 the FIRST hop is our own
  Cloudflare Worker** (`stocksworld-quotes.dhruvan2510.workers.dev/?chart=<sym>` — verbatim Yahoo chart passthrough,
  30 s cache, scripts/live-quote-worker.js), then `corsproxy.io/?url=` → `api.allorigins.win/get?url=` as fallbacks
  (both verified working 2026-07-12; direct Yahoo, thingproxy, codetabs, cors.eu.org all CORS-fail from the browser). Polls every 45s ONLY Mon–Fri 09:15–15:30 IST and only when the
  tab is visible; shows "● LIVE" when open, "AT CLOSE" otherwise. If every proxy is down it silently keeps the
  daily-close seed. Weekly RSI is computed from the JSON history (barely moves intraday) and cached. If the
  proxies ever die, stand up your own CORS shim (Cloudflare Worker / Supabase edge fn) in front of Yahoo and put
  it first in the `PROXIES` chain in index.html.
- **Tile grid** (below): every page as a card. Rendered from `window.SW_NAV` (exported by `theme.js`) so it stays
  in sync with the ONE nav source of truth — add a page in `theme.js` `NAV_GROUPS` and it appears here too. Only
  the per-tile description lives in index.html (`DESC`/`EXT` maps, keyed by filename); a new page gets a blank
  description until you add one line there. Header `<nav>` + footer are injected by `theme.js` as on every page.
- **Data feeds:** `nifty_bank.json` (^NSEBANK) + `india_vix.json` (^INDIAVIX) come from `scripts/fetch_fii_dii.py`
  `update_yahoo_index()` (same Yahoo path as nifty500's ^CRSLDX), history to 2012, refreshed by the daily
  `refresh-fii-dii.yml` (both files added to its cp/git-add list). Fill-only merge; keeps history on fetch fail.
- **NOT built from a template** — edit `index.html` directly (unlike nse-bse-dashboard.html / mutual-funds.html).

---

## 14. CORPORATE ANNOUNCEMENTS BROWSER  (docs/announcements.html — "Announcements" nav, built 2026-07-12)
NSE corporate-announcements browser. **Own design (user asked NOT to copy the reference site): a left
CATEGORY SIDEBAR** (All + ~33 buckets with counts, single-select, sticky; <768px it becomes a dropdown)
+ symbol/date toolbar over a table (Stock+company / Date / raw Announcement type / Caption / PDF).
Hand-maintained page; data = **`docs/announcements.json`** (rolling ~31 days,
~15k rows, ~4.7 MB raw — Pages gzips it on the wire; fetched WITHOUT a cache-buster per §9).
- **Fetch:** `python -X utf8 scripts/fetch_announcements.py` — reuses `build_fundamentals`'s CI-proven NSE
  session (plain urllib + Chrome UA + `nse_jar()` cookie warmup, NOT curl_cffi) against
  `/api/corporate-announcements?index=<idx>&from_date=&to_date=`, once per index in
  `INDICES=("equities","sme")`. **⚠️ NSE files mainboard and SME/Emerge announcements on SEPARATE
  boards — you MUST query BOTH `index=equities` AND `index=sme` or every SME result filing
  (VINEETLAB, KARNIKA, VIGOR, MONOPHARMA…) is silently missing from `results_feed.json`, and the
  Quarterly Results "Declared" tile reads permanently below Trendlyne's whole-universe count (the
  recurring 171-vs-182 gap of 2026-07-19).** The tl_reconcile heal (§31) re-runs this fetcher, so it
  inherits SME coverage automatically. **SME board is FLAKY — three lessons baked into the code:**
  (1) run each board as its OWN pass with its OWN fresh `nse_jar()` session — NSE throttles `sme` to
  a non-JSON body when it's hit right after `equities` in one session; (2) query SME in **3-day
  chunks** (`CHUNK_SME`), not 7 — a 7-day SME window in results-season peak (e.g. 2026-07-10..16)
  reliably errors to 0, but the same days split fine returned 212/463/229 recs; (3) SME fetch is
  **fail-fast** (25s timeout, 2 tries) so hanging windows can't blow the 25-min workflow timeout —
  equities keeps the full 90s/3-try budget. Whatever SME NSE still refuses on a given run is captured
  on a later scheduled run and KEPT (see preserve-all below), so windows converge across the day.
  Self-healing: **merges with the existing file** (a failed chunk keeps yesterday's rows), trims to the
  31-day window, and **ABORTS below 200 rows** (never clobbers good data with a broken fetch).
  `write_results_feed` **preserves ALL prior feed rows** (not just BSE) — a fresh `(sym,date)` overrides,
  so corrections still win, but a throttled SME/BSE run can never DROP a previously-captured result.
  Schema: `{updated,from,to,rows:[[symbol,company,"YYYY-MM-DD HH:MM:SS",category,caption,file],…]}`;
  captions capped at 500 chars; `file` has the `https://nsearchives.nseindia.com/corporate/` prefix stripped
  (page re-adds it; non-matching URLs kept absolute).
- **Auto-refresh:** `.github/workflows/refresh-announcements.yml` — daily ~4×/day IST (08:30/14:00/18:00/21:30),
  same reset-hard commit-retry loop as FII/DII, commits ONLY `docs/announcements.json`, dispatches pages.yml.
  Manual: `gh workflow run refresh-announcements.yml` or repository_dispatch `announcements-refresh`.
- **Categories:** NSE's raw `desc` subject (~109 values incl. long tail) is bucketed CLIENT-SIDE into ~33 clean
  sidebar buckets by `CATRULES` (ordered regex list in announcements.html; first match wins, default "Others").
  The raw desc still shows in the "Announcement type" column. New NSE subjects fall into Others until a rule is
  added. Sidebar rebuilds idempotently (`buildSidebar()`) when the live top-up adds rows/categories.
- **Page style gotcha:** custom classes (.chip/.catpill/.docbtn) are styled with **theme vars**
  (`var(--surface-2)/--accent/…`) so Light/Dark/Soft work; Tailwind OPACITY variants (`bg-slate-100/80`)
  are NOT covered by theme.css's dark overrides (it matches exact class names) — use the plain class.
- Symbol cell links to `./stock.html?sym=` ; nav entry in theme.js NAV_GROUPS (+ DESC line in index.html).

---

## 15. QUARTERLY RESULTS DASHBOARD  (docs/quarterly-results.html — "Quarterly Results" nav, built 2026-07-12)
Best-of-breed results hub (features merged from a ~40-site survey: ValuePicker/Trendlyne/Screener/Tijori/
MarketsMojo/Moneycontrol/StockEdge/Investing.com/EarningsWhispers/Nasdaq/FactSet…). Five tabs:
**Season Overview** (scoreboard tiles + breadth + 5-season strip + sector heatmap/scorecard + movers incl.
result-day reaction), **All Results** (sortable per-company table: Rev/OP/OPM Δbps/PAT std+con auto-basis,
YoY+QoQ, rule-based verdict dot, reaction %, since-result drift; screen chips: turnaround LP/PL, margin
+200bps, record PAT, 4-qtr streak, reacted ±5%; CSV export; watchlist ★ localStorage `qr_watch`),
**Just Declared** (filing feed w/ PDF + growth badges, LIVE top-up via the same Cloudflare Worker
`?announcements=1` every 60 s), **Calendar** (upcoming results: NSE-confirmed ✓ + cadence-PREDICTED "est."
dates), **Season Trends** (the §11 multi-year median/total YoY chart, merged here 2026-07-18 — own
universe/mode/period controls, shared filter card hidden, `results_season.json` lazy-fetched on first open).
**Season Trends quarter CLICK-THROUGH (2026-07-20):** clicking a quarter (chart bar/hit-area or "Quarter
detail" table row, "open →") jumps to the All Results tab pinned to that quarter + universe — for EVERY
season universe. Two paths in quarterly-results.html: the 6 broad universes map onto the main filter's
index bitmask (`RS2UNI`: liquid→all, Nifty 50/100/500→n50/n100/n500, Midcap 150→m150, Smallcap 250→s250);
every OTHER index (Nifty Bank, IT, Nifty 200, Midcap 50 …) filters by its `members` list — CURRENT
membership (latest snapshot, rename-mapped), emitted per index universe by build_results_season.py into
results_season.json (+~20 KB; current-not-point-in-time on purpose — the universe filter is a display
filter, §15 above). The member filter shows as a transient "<Index> (index)" option (`value="idx"`,
`ST.uniSet`) in the universe dropdown. Quarters older than the main window (8 chips = latest 8 QEs) stay
tooltip-only. Clicks rewrite the URL via history.replaceState; results_season.json fetches are ?t=
cache-busted (they weren't pre-2026-07-20 — browsers held stale copies for hours).
Deep links: `?tab=results|feed|calendar|season&sym=TCS`, plus `?tab=results&qe=YYYYMMDD&uni=n500` and
`?tab=results&qe=YYYYMMDD&uniIdx=Nifty%20Bank` (uniIdx lazy-fetches results_season.json for the member
list — what sectoral-index clicks write, so the view is shareable).

**Three data files, three refresh paths (page fetches only these — no big bins):**
1. `docs/quarterly_results.json` (~1.8 MB raw) ← **`scripts/build_quarterly_results.py`** — per company:
   `{n,s,i,m,f,x,e,q[13]}`; q rows `[revS,opS,patS,revC,opC,patC,ann,rx,sr]` for the last 13 QEs.
   Sources: sf_revop (Rev/OP), **sf_fundamentals for PAT (owners basis — NEVER revop's pat)**, ann = min(std,con)
   date (15:30-gated ⇒ reaction day = ann day, look-ahead-clean), `rx` = close(ann)/close(prev)−1 from the price
   bin (env `SF_BIN`; CI downloads the fresh `data` release asset — committed docs bin is STALE §0), `sr` = drift
   ann-close→asof (anns ≤140 d only), `x` bitmask 1=N50 2=N100 4=N500 8=Mid150 16=Sml250 32=F&O (latest
   snapshots of indices_history/fno_history), `e` = predicted next result date (median historical filing lag,
   same-quarter-month preferred), mcap/names from dash_slim, sector = classification macro (Unknown→Others),
   `f`=1 bank/NBFC (revop fin flag; page hides OP/OPM for them). Rebuilt EVERY `refresh-fundamentals.yml` run
   (drift moves daily), committed when changed. Local run: merge live sf parts → `SF_BIN=... python -X utf8
   scripts/build_quarterly_results.py`.
2. `docs/results_feed.json` (tiny) ← written by `fetch_announcements.py` (same run): results-filing rows only
   (`Outcome of Board Meeting` + "financial results" caption, or `Financial Result*` category), with parsed
   quarter-end. Refreshes 4×/day with announcements.
3. `docs/results_calendar.json` (tiny) ← **`scripts/fetch_results_calendar.py`** — NSE `/api/event-calendar`
   (−3d..+75d), result-purpose rows only; ABORT-guard keeps the old file on a broken fetch. Same workflow.

**Refresh cadence (every 15 min since 2026-07-17; hourly 2026-07-13→17):**
- **`refresh-results-hourly.yml`** — hourly 08:30–00:30 IST: runs the 3 fetchers but commits ONLY the two
  small side-files (feed+calendar), NEVER the 4.7 MB announcements.json (git-bloat guard; that big file
  stays on the 4×/day refresh-announcements.yml).
- **`refresh-fundamentals.yml`** — crons `*/15 4-14 * * *` (09:30–20:15 IST, the filing window) +
  `45 15` (21:15 IST nightly) + `45 17` (23:15 IST late top-up) = 46 runs/day. Numbers (XBRL parse) land
  within ~15 min of the filing's XBRL appearing. The quarterly_results.json BAKER (90 MB asset download +
  rebuild) is GATED: runs only when `.fund_updated` exists OR on the nightly run (which always rebuilds so
  reactions/drift track the daily prices published 20:45 IST). Don't un-gate it — a 1.8 MB commit 4×/hour
  would bloat history for nothing. Idle runs are ~40 s; Actions minutes are free (public repo).
- ⚠️ **GITHUB'S SCHEDULER DRIFTS — never rely on a cron firing in its own minute, or even its own hour.**
  Observed 2026-07-17: a `45 4-17` (hourly-at-:45) cron actually fired at 07:02 / 09:29 / 11:08 UTC — up to
  44 min late, real gaps of 1.5–2.5 h. Two consequences, both handled — keep them handled:
  (a) density beats precision — that's why the filing window is `*/15`, so a dropped slot costs 15 min, not
      an hour (this was the whole reason a dozen large caps sat on "numbers being parsed" for hours on
      2026-07-17: they filed 15:46–17:44 IST, the last run was 16:38);
  (b) the nightly-only steps (insurer vision fill / IPO-base backfill / UNGATED baker) detect the nightly run
      by wall-clock `date -u +%H`, so an exact `= 15` test would SILENTLY SKIP THE ENTIRE NIGHTLY on any day
      the 15:45 cron drifted to 16:0x. The gates now accept **`15|16`** (`case "$(date -u +%H)" in 15|16)`),
      and **hours 15–16 UTC are RESERVED — never add a dense cron there** or the heavy steps double-fire.
  `&& case … esac` is set-e-safe (a non-matching `case` returns 0), so the gates can't abort a step.
- ⚠️ **"Result filed — numbers being parsed" on the feed is usually NOT a bug.** The feed row comes from the
  ANNOUNCEMENT PDF; the numbers come from the XBRL `integrated-filing-results` feed — companies post the PDF
  first, XBRL minutes-to-a-day later. Before debugging, check whether the XBRL exists at all:
  hit `/api/integrated-filing-results?index=equities&period=Quarterly&from_date=…&to_date=…&size=200` and look
  for the symbol with a non-null `consolidated` (Consolidated/Standalone). A row with `consolidated=None` is a
  NON-results part of the integrated filing (governance/deviation) — not parseable, correctly skipped.
  NB the OLD `/api/corporates-financial-results?symbol=…` endpoint is stale (returns nothing past Dec-2024) —
  don't diagnose with it. Verified 2026-07-17: RBLBANK/TATVA had no XBRL row, POONAWALLA/CHEMBOND had only a
  `consolidated=None` row, ~4 h after their PDFs — pipeline correct, nothing to fix.
- The page's Declared tile = with-numbers + feed-only filers ("+K filed, numbers coming"), deduped against
  numbers by SYMBOL and by normalized COMPANY NAME (dual-listed cos arrive under different tickers per
  exchange, e.g. a BSE fallback ticker; NB INDBNK=Ind Bank Housing is a DIFFERENT co from INDBANK).
- Growth math is CLIENT-side, always same-basis both periods (con preferred, else std; LP/PL/LL flags when the
  base is ≤0 — never a % off a negative base). Verdict dot = rule score (PAT YoY, Rev YoY, ΔOPM), transparent.
- Universe filter = CURRENT index membership (a display filter, not point-in-time — backtests stay elsewhere).
- ⚠️ Weekend/post-close filings carry NEXT-trading-day ann (the §12 gate) — the table "Date" column shows that
  tradable date (can read a day ahead of the filing timestamp shown in the feed tab; intentional).
- **NSE + BSE COVERAGE (feed + calendar; 2026-07-13):** the Overview scoreboard + All Results TABLE stay
  NSE-sourced (~2,464 alive names — every liquid stock; we have no BSE prices/financials for BSE-only micro-caps).
  But the **Just Declared feed** and **Calendar** span BOTH exchanges via **`scripts/fetch_bse_results.py`**, run
  in refresh-announcements.yml AFTER the NSE fetchers: BSE result filings from `AnnSubCategoryGetData?strCat=Result`
  (paginated, 31-day window) → merged into results_feed.json; BSE forthcoming dates from
  `Corpforthresults?strCategory=Result` → merged into results_calendar.json. ADDITIVE + self-healing (a BSE
  failure leaves the NSE files intact); de-duped by (sym,date) so a dual-listed co is carried once (NSE preferred).
  SCRIP_CD→NSE sym via `bse_scrips.json['by_id']` reversed; BSE-only names use the BSE ticker (URL slug/short_name)
  and render as non-clickable feed/calendar entries (no CO metadata → no growth badge/reaction, by design). First
  run added ~142 feed + ~76 calendar BSE rows. To get NUMBERS for BSE-only names would need a BSE price+XBRL
  pipeline (deferred — user chose feed+calendar coverage only).
- ⚠️ **HEAVY-DEP IMPORT CRASH = INVISIBLE BSE OUTAGE (fixed 2026-07-17, keep it fixed).** `fetch_bse_results.py`
  imports `bse_fetch` for its network helpers only, but bse_fetch used to top-level-import `fitz` + instantiate
  `RapidOCR()` — neither installed in refresh-announcements/refresh-results-hourly → the BSE merge died on
  `import` in EVERY CI run (all BSE rows were coming from local/vision-env runs). Triply invisible: the step's
  `|| echo` kept runs green, the additive feed never shrank (guard_feed floor can't see "stopped growing"),
  and NSE rows kept refreshing. Found only because Trendlyne showed ~24 more declared results (121 vs 144,
  2026-07-17) — the whole gap was 3 days of missing BSE micro-cap filings. Fix: bse_fetch loads fitz/RapidOCR
  LAZILY inside `pdf_np()`/`_ocr()`; refresh-announcements pip-installs `pymupdf` (small) for
  enrich_order_values, NEVER the OCR stack. Rule: a shared helper imported by lean workflows must keep heavy
  deps out of module top-level; after touching bse_fetch imports, smoke-test
  `python -c "import bse_fetch"` with fitz/rapidocr BLOCKED (see the import-blocker snippet in the 2026-07-17
  commit a626f0e message context). Coverage check: newest `bseindia.com`-URL row in results_feed.json should
  be <24-48 h old on weekdays during results season.
- ⚠️ **ALWAYS parse the quarter PER FILING — never assume the current season.** A results filing in Jul/Aug/Sep
  is very often a LATE March (Q4/annual) result, not the June quarter (in a recent 30-day BSE window: 91 March
  vs 32 June filings). `parse_qe()` (fetch_announcements.py) / `qe_from_head()` (fetch_bse_results.py) ANCHOR on
  an "ended/ending <date>" clause (DMY/MDY/dd.mm.yyyy), snap to a quarter-end month, and return 0 (⇒ NO badge on
  the page, never a wrong one) when the period isn't stated — so a board-meeting date like "held on July 1, 2026"
  can't be mistaken for a quarter. The feed's growth badge keys off this qe, so it shows the March column's YoY
  for a March filing. BSE `QUARTER_ID` is always null — useless, must parse the headline+NEWSSUB.
- ⚠️ **qe=0 rows (period unstated ANYWHERE in the filing text) are a first-class pipeline state (2026-07-21).**
  Before, a feed row whose quarter couldn't be parsed was counted NOWHERE — not Declared, not pending, invisible
  to the vision routine AND to tl_reconcile's heal (YESBANK filed Sat 2026-07-18 13:41 with a bare "Outcome of the
  Board Meeting…" headline and sat unclassified for 3 days; found only via a manual Trendlyne Nifty-500 diff).
  Now: `results_pending.classify()` emits them as status `unknown_qe` (visible on the coverage page);
  `bse_vision_prep` has a resolution pass (`find_unknown_qe`, 12/run mcap-desc) that fetches the filing PDF
  (NSE-with-retry → BSE fallback; BSE candidates only from the SAME filing date) → `pdf_period()` →
  `feed_qe_fix.json`, and when the resolved quarter IS the target it renders + fills in the same run;
  the page counts qe=0 rows as "numbers coming" on the LIVE quarter only, labelled "reporting period being
  determined"; tl_reconcile counts them as declared coverage. Never treat qe=0 as "assume current quarter" —
  resolution comes from the PDF, period unknown stays labelled unknown.
  ⚠️ **…and ONLY when FILED AFTER the live quarter ended** (2026-07-21). A quarter's result cannot be declared
  before that quarter closes — this "impossible pair" rule is now enforced at EVERY layer, for EVERY quarter:
  - **Page** (`filedAfterQE()` in quarterly-results.html, All Results + Overview): a feed row renders under
    quarter Q only if its filing date > Q-end — guards both qe=0-on-live-quarter rows (the original bug:
    stale June leftovers like DECPO "Q.E.31.03.2026" surfaced under Jun-2026 with June "declared" dates)
    and qe-labelled rows whose caption/ledger quarter is impossibly ahead of the filing date.
  - **Feed writers** (`qe_sane()` in fetch_announcements — imported by fetch_bse_results as the same rule):
    an impossible (qe, filing-date) pair is demoted to qe=0 at write time (caption lied / wrong date grabbed).
  - **Vision ledger** (bse_vision_prep): a pdf_period() that ends on/after the filing date is never recorded
    into feed_qe_fix.json (the parse grabbed a validity/record date, not the period).
  - **Historical ann dates** (fill_ann_dates.py, nightly): a stored announcement date <= its quarter-end is a
    backfill placeholder/typo AND look-ahead bias in backtests — demoted to the SEBI deadline (its existing
    convention). 2026-07-21 audit healed 20 such rows (2018-backfill ann=qe stamps, ENRIN pre-IPO, FEDERALBNK
    year typo) in sf_fundamentals + scripts/fundamentals.json. ⚠️ ann=0 is the "date unknown" SENTINEL
    (falsy → consumers skip; page shows no date) — never "impossible", never touched.
  - tl_reconcile's fallback path carries the same `r[2] > qe_iso` guard.
  - **Baker** (build_quarterly_results.py): an ann <= its quarter-end coming out of sf_fundamentals is baked
    as date-unknown (dropped, logged "IMPOSSIBLE ann dropped"). Needed because a CI run whose checkout
    predates a fill_ann_dates heal commit would otherwise bake the bad date into quarterly_results.json and
    the page shows baked anns as-is (filedAfterQE guards only FEED rows) — happened with ENRIN Sep-2024
    (ann=20240930) in the 12:07 IST 2026-07-21 bake, racing the heal push.
  **ann=0 RECOVERY (2026-07-21): `backfill_ann_dates_bse.py`** — recovered REAL declared dates for 613 of the
  1,102 ann=0 (date-unknown) rows from the BSE announcement archive (metadata only, no PDFs/vision/API key):
  window (qe+1 .. qe+150d) via fetch_insurers.datebound; accept the EARLIEST filing whose NEWSSUB states the
  target period (parse_qe, "exact") or, if no candidate states any period, a SOLO result-filing date inside
  (qe+5 .. qe+100d); if the only period-stated filing is a DIFFERENT quarter's → refuse ("other-period" skip —
  the unstated one next to it is as likely that quarter's refiling; NEVER guess a backtest-visibility date).
  Ledger scripts/ann_date_fills.json (+ _ann_date_skips.json, --retry-skips to re-attempt); fill-only into
  BOTH docs/sf_fundamentals.json and scripts/fundamentals.json where ann==0 and PAT present; ann > qe enforced
  at write. `--reapply` re-applies the ledger after any rebuild/clobber.
  **Second pass (2026-07-22, "seq" rule): 809/1,102 total.** Filings stating a DIFFERENT period are excluded,
  then the unstated filing is accepted when boxed by KNOWN neighbour declarations (> prev quarter's ann,
  < next quarter's ann / earliest later-stated filing; band qe+5..qe+150d, never a stated-other's same day);
  fetch window widened to qe+240d (late "exact" filings — AASHKA Sep-2022 declared Apr-2023). Residual 293
  rows stay ann=0 legitimately (~124 archive-empty pre-2019 microcaps + unresolvable ambiguity; skip ledger
  has per-key reasons — don't re-sweep without a genuinely better rule).
  Same audit taught `parse_qe` the caption styles that caused those qe=0s — anchored only ("Q.E.", "as on",
  "for the … year", "For <Month D, YYYY>", month+year with no day, and the unambiguous unanchored
  "F.Y. 2025-26" → March); fetch_bse_results' `qe_from_head` now lazy-imports it (superset of its local
  "ended"-only fallback). A bare date stays unparsed on purpose (it's as likely the board-meeting date;
  _qe_mk's quarter-end-month check is NOT enough protection at quarter turns, e.g. a 30.06 meeting
  approving March results).
- ⚠️ **BSE results can hide under "Board Meeting / Outcome of Board Meeting" with NO Result-category twin** —
  fetch_bse_results.py runs a second 7-day scan of that category and keeps outcomes that (a) talk about results
  (JPPOWER-style headline), OR (b) match a result-purpose date in results_calendar.json for that company
  (SOBHA 2026-07-20: headline says ONLY "outcome of Board meeting held on July 20, 2026" — the calendar join is
  the only catch). This is what makes BSE fully redundant with NSE during an NSE announcements outage.
- ⚠️ **NSE CAPTION CAN LIE about the quarter → `docs/feed_qe_fix.json` self-heals it.** Some late Q4/annual
  filers (SUPREMEINF/ESSENTIA/VIKASECO, Jul-2026) get an NSE announcement captioned *"financial results for the
  period ended Jun 30, 2026"* while the attached PDF is audited results for the quarter **ended March 31, 2026**.
  `parse_qe(caption)` faithfully returns the wrong June quarter → a phantom "⏳ numbers being parsed" June row that
  never fills (its real March numbers already exist). Fix: `bse_vision_prep.py` (the daily vision routine's prep)
  reads the **filing PDF's** own period via `pdf_period()` (text-layer → `parse_qe`); if it ≠ the caption quarter it
  records `"SYM|YYYY-MM-DD" -> real_qe` in `docs/feed_qe_fix.json`. `write_results_feed()` (fetch_announcements.py)
  applies that override every hourly rebuild → the row re-files under its true quarter and drops out of pending.
  No API/vision cost (pure text+regex). The routine commits feed_qe_fix.json + results_feed.json. Keyed by
  sym+date so a genuine later June filing (different date) is never wrongly re-tagged.
  - ⚠️ **…and the routine's own mid-run re-sync used to EAT those fixes (found + fixed 2026-07-27).** Prep writes
    `docs/feed_qe_fix.json` at step 2, but SKILL step 5 re-syncs with `git reset --hard origin/main` before merging
    (the laptop can sleep for hours mid-run), discarding that working-tree write. Re-running prep does **not** bring
    them back: the names it resolved are no longer pending, so nothing ever re-scans them — the fix is lost silently
    and the row stays phantom-pending under the wrong quarter until a human notices. Observed live: 59 → 55 entries,
    losing `COALINDIA|2026-07-27`+`SHIVACEM|2026-07-27` → 20260630 and `HMT|2026-07-27`+`SGLRES|2026-07-27` →
    20260331. **Fix:** prep now also journals the run's fixes OUTSIDE the repo to `<outdir>/qe_fix_run.json` (written
    even when empty, so "none this run" ≠ "the flag was forgotten"), and step 5 runs
    `merge_bse_vision.py <out.json> --qefix <outdir>/qe_fix_run.json`, which re-applies them post-reset — idempotent,
    so re-merging after a sleep is still safe. Watch the merge log for the `qefix:` line; `⚠ qefix: … unreadable`
    means they did NOT land and prep's `qe? SYM date -> QUARTER` log lines must be re-added by hand. **General rule:
    any repo write made BEFORE a step that re-syncs must be journaled outside the repo, or it does not exist.**
- ⚠️ **NSE nsearchives 403s scripted PDF downloads → the vision routine falls back to the BSE copy (2026-07-19).**
  On 2026-07-18 AXISBANK/KOTAKBANK/PNB/JKCEMENT/PSB/INDIACEM filed by ~13:00 IST but sat "numbers being parsed"
  through the whole day. Root cause (verified): the CI XBRL crons ran fine but NSE hadn't posted the structured
  XBRL yet (banks file the PDF board-outcome first, XBRL hours-to-a-day later); and the vision safety-net's PDF
  fetch from `nsearchives.nseindia.com/corporate/<file>` returned **HTTP 403** — NSE hard-blocks scripted archive
  downloads (persists across cookie re-warm + backoff; even a clean IP via WebFetch 403s, so it is NOT merely a
  per-IP rate limit). The old code just printed `fetch err 403` and `continue`d, abandoning the name for the whole
  run; the next scheduled run hit the same wall, so both of 2026-07-18's 15:08 + 23:08 IST runs missed them.
  **Fix in `bse_vision_prep.py`:** (1) `_nse_pdf_with_retry` — brief backoff + cookie re-warm (rides out a
  transient throttle, 3 tries); (2) **`_bse_fallback` — when the NSE fetch still fails, read the SAME result off
  BSE** (`bse_scrips.json['by_id']` gives SYM→scrip; `bse_render.announcements`+`fetch_pdf` with the same
  quarter tripwire). BSE's AttachLive/His path is NOT blocked, so every dual-listed large-cap is rescued; the
  numbers still route to `vision_fills.json` (manifest exch stays "NSE"). Only genuinely NSE-only names (SME like
  GANGAFORGE, not on BSE) can't fall back — those wait for NSE's XBRL. Gentler 1.5 s spacing + browser-ish
  headers (Sec-Fetch + announcements-page referer/warm) reduce the trip rate. Verified 2026-07-19: all 6
  dual-listed names rendered via BSE fallback while NSE returned 403.
  **2026-07-21 escalation — CF worker `?pdf=` relay for the NSE-only leftovers.** The block widened to EVERY
  scripted transport (GitHub runners, local python, curl_cffi Chrome-TLS from the user's residential IP — even
  `www.nseindia.com/` 403s locally) while the Cloudflare edge still passes (?announcements=1 fine). So
  `live-quote-worker.js` gained `?pdf=<bare filename>` → `nsearchives/corporate/<file>` (strict filename
  validation, never an open proxy; challenge-page detection; 1-day edge cache), and `_nse_pdf_with_retry`
  tries it as the LAST transport before "UNFETCHED". Fails soft if the deployed worker predates the route.
  Note "other sites still show the data" is licensed feeds / their own IP pools, not the public archive —
  don't chase browser paths: BOTH in-app browser surfaces block nseindia.com by policy.
- **Price-universe ORPHANS (tiny NSE filers absent from sf_stock_data.bin) now show as lightweight rows.**
  Micro NSE names (e.g. OASIS/Oasis Tradelink, BETALA/Betala Global Securities) file real results but have
  no `co` entry (not in the price bin, no parsed XBRL) → they used to hang as "numbers being parsed" forever.
  Fix: `results_pending.classify` (shared module; see §20) has an `elif sym not in CO` branch that renders orphan feed filings
  for the vision routine (name from the feed row, mcap 0); `merge_bse_vision` already routes NSE reads →
  `vision_fills.json`; the page's overlay (`load()`) SYNTHESIZES a minimal `CO[sym]={n,s:'',m:0,orphan:1,q:[…]}`
  from the feed name + vision numbers when `!CO[sym]`, so PAT/rev display (no price/reaction/sector — orphans
  can't participate in those). Modal is orphan-safe (falls through the non-`bse` branch = NSE links). Bounded:
  ~0–2 orphans per quarter. Values still ₹ crore, so sub-crore micro filers round near 0.

---

## 16. DISCOVERY BUCKETS  (docs/discovery.html — "Discovery" nav, built 2026-07-13)
Sovrenn-Discovery-style **auto-computed trigger buckets** (concept inspired by sovrenn.com/discovery, which
hand-curates; ours are 100% computed — no curation, no paywall). ~57 buckets in 5 groups, each with its own
shareable URL (`discovery.html#b=<key>`).
- **Data = `docs/discovery.json`** (~0.9 MB raw), built by **`scripts/build_discovery.py`** from files the site
  already maintains — NO new fetchers: `announcements.json` (trigger buckets: order wins/capacity/M&A/fund
  raising/open offers/buybacks/dividends/ratings/investor meets/spurts/red flags — grouped BY STOCK with count +
  latest caption + PDF), `sf_revop.json` (**Excellent Results per quarter Mar-2019→date**: PAT YoY ≥25% + rev
  support, PAT≥2cr, positive base, con-preferred; + Weak Results for the newest quarter with ≥30 rows),
  `dash_slim.bin` meta (names/mcap/d52 → Near-52w-High ≥−2%, 30%+-Off-High, **P/E<30** = mcap/TTM-PAT with
  4 recent quarters), `sector_classification.json` (12 theme buckets).
- **⚠️ Theme buckets need SEEDS:** BSE taxonomy ≠ themes (Suzlon=Heavy Electrical, RVNL=Civil Construction,
  IREDA=Finance) — `THEMES` in build_discovery.py = regex + hand-seeded symbol list per cross-cutting theme
  (green/rail/EMS/defence). To grow a theme, add symbols to its seed string.
- **Refresh:** wired into `refresh-announcements.yml` (~4×/day, after the announcements fetch; non-fatal,
  commits `docs/discovery.json` alongside). Fundamentals/price inputs are the committed daily files — good enough.
- Mcap floor ₹50 cr for price/sector buckets; px buckets capped at 500 rows; sector buckets at 80 (by mcap).
- NOT automatable from our data (deliberately absent): revenue-guidance buckets, star-investor holdings,
  bulk-deal/promoter-buying (needs new NSE insider/bulk-deal fetchers — candidate future work).

---

## 17. BSE-ONLY STOCK COVERAGE  (docs/bse_*.* — built 2026-07-13)
Our core price/fundamentals dataset is NSE-keyed, so ~2,678 **BSE-listed-only** stocks (ISIN not on NSE —
e.g. Cella Space 532701, NSDL) were entirely absent. NSE 2,385 + BSE-only 2,678 ≈ the "~5,000 stocks"
users compare against (Screener covers BSE-only; we didn't). This pipeline adds them as first-class rows on
the Quarterly Results page. **4 scripts + `.github/workflows/refresh-bse.yml` (22:10 IST daily):**
- **`build_bse_universe.py` → `docs/bse_universe.json`** — BSE bulk `ListofScripData` (all active equity,
  one call: SCRIP_CD, scrip_id, ISIN, GROUP, FACE_VALUE, **Mktcap**) MINUS the NSE `EQUITY_L.csv` ISIN set =
  BSE-only. Rows `[scrip_cd, ticker, name, isin, group, faceval, mcap_cr, sector]`, biggest-mcap-first. Sector
  from BSE `ComHeadernew` (`Industry`), budgeted per run + cached in `scripts/_bse_sectors.json`. Weekly (Sun).
- **`fetch_bse_bhav.py` → `docs/bse_prices.bin`** (gzipped `{end, px:{scrip:{d,c,v}}}`) — BSE **UDiFF** equity
  bhavcopy `BhavCopy_BSE_CM_0_0_0_<YYYYMMDD>_F_0000.CSV` (cols ISIN/TckrSymb/**ClsPric**/TtlTradgVol, ~4,900/day).
  Resumable, SORT-MERGES (backfill can add dates OLDER than existing — don't revert to append-only). Daily.
- **`fetch_bse_fund.py` → `docs/bse_fundamentals.json`** (`{px:{scrip:{QE:{rev,pat,ann,basis}}}}`) — quarterly
  Rev/PAT. **⚠️ The BSE `FinancialResult` API is ENTITY-POISONED for many scrips** (returns **BSE Ltd's** numbers
  — the FORCEMOT pattern; proven on Cella Space: its "result zip" was BSE Ltd's auditor report). So we DON'T use
  it. Instead per scrip: `AnnSubCategoryGetData` (strCat=-1) → pick result/board-outcome filings WITH an
  attachment → `AttachLive/AttachHis/<guid>.pdf` → **OCR** (rapidocr; small BSE cos file SCANNED PDFs, no text
  layer) → **IDENTITY-GUARD** (company name tokens must appear on the page) → parse P&L rows. OCR ~15s/page ⇒
  SLOW ⇒ bounded+resumable grind (`--budget N --max-minutes M`, ledger `scripts/_bse_fund_done.json`).
  **⚠️ ORDER = DECLARED-FIRST, then mcap desc, floor 0** (was mcap-only + floor 100 → 782 already-declared
  sub-₹100cr cos were being SKIPPED — user wanted ALL results incl. <100cr). `declared_recently()` scans the
  strCat=Result feed (last ~110d) → those scrips grind FIRST at ANY mcap; the `--min-mcap` floor now applies
  ONLY to non-declared names. Workflow runs `--min-mcap 0` TWICE daily (12:10 + 22:10 IST) so the whole
  ~2,678 universe is covered in ~1-2 wks, real declarers first. **Validated: Cella Space Q1FY27 PAT 7.29 =
  exact vs Screener.** (Revenue is Revenue-from-Operations; Screener may show Total Income — small diff.)
  **⚠️ FEED INJECTION (fetch_bse_results.py step 1b):** BSE lets small cos file the result under "Board
  Meeting"/"Company Update" (NOT strCat=Result) — e.g. Cella Space → it never reached the Just-Declared feed
  via the category scan. So after the strCat=Result merge, we ALSO inject feed rows from `bse_fundamentals.json`
  (anything with an ann date in the 31d window = a confirmed declared result). That's why odd-category filers
  still appear in Just Declared once the grind has confirmed them.
- **`build_bse_results.py` → `docs/bse_results.json`** — joins the 3 files into a SLIM payload in the SAME
  `co` shape as `quarterly_results.json` (q-array `[revS,opS,patS,revC,opC,patC,ann,rx,sr]`, std=con since BSE
  small-caps are standalone, op=null; `bse:1` flag; reactions precomputed from bse_prices so the browser doesn't
  need the price bin). Quarters list READ FROM quarterly_results.json so indices align.
- **Page integration (quarterly-results.html):** `load()` fetches `bse_results.json` and merges `bse.co` into
  `CO` (only if `bse.quarters`==`qr.quarters`; NEVER shadows an NSE symbol). Universe filters **`nse`/`bse`**
  ("NSE-listed only"/"BSE-listed only"), a **BSE badge**, BSE-page links (no stock.html for BSE-only). `c.bse`
  flag + `c.cd`=BSE scripcode.
- **Vision fallback (auto-fills scanned filings for ALL upcoming results):** when OCR finds nothing anchored,
  `fetch_bse_fund.py` renders the P&L pages and calls the Anthropic **vision** API (`bse_vision_api.py`,
  model `claude-haiku-4-5`, ~cents/co) to read them — same accuracy as a human, unattended. Gets current +
  year-ago quarters so YoY fills. **Needs the `ANTHROPIC_API_KEY` repo secret** (Settings→Secrets→Actions);
  unset → grind stays OCR-only and unreadable rows show "filing PDF only". This is what makes "every declared
  result eventually has values" true without manual vision passes. Manual seed once: run the grind with the
  key set, or `merge_bse_vision.py <results.json>` from a one-off agent pass.
- **⚠️ NEVER match a BSE announcement on HEADLINE alone — match `HEADLINE + " " + NEWSSUB`** (fixed
  2026-07-17 in `bse_render.announcements`). BSE's HEADLINE is frequently content-free while **NEWSSUB carries
  the real description**: GYANDEV 530141 filed its Jun-2026 quarter under the headline *"Please refer the
  attachment"* (NEWSSUB: *"Unaudited Financial Results For The Quarter Ended 30.06.2026"*), NAM 538395 under
  *"Pursuant to provision of Regulation 30 & 33 of SEBI (LODR)"*. Headline-only matching dropped both real
  filings → the renderer fell back to an OLDER quarter's PDF → the routine reported "they only filed Q4" for
  two companies that had filed **that morning** (the user caught this; the data said otherwise). Note
  `fetch_bse_results.qe_from_head()` already read both fields — the feed knew the true quarter while the
  renderer didn't, so **a feed-vs-renderer disagreement means the RENDERER is wrong, not the feed.**
- **🔒 TRIPWIRE (so the above can't silently repeat): `bse_vision_prep` now calls `pdf_period(raw)` on every
  BSE candidate and REFUSES to render a filing whose stated period ≠ the target quarter** (prints
  `⚠ <TKR> <date>: filing states <qe>, want <qe> — wrong announcement, trying next`), falling through to the
  next candidate. Verified: GYANDEV's 2026-05-30 PDF (the one that fooled the routine) reads `20260331` and is
  skipped, while its real 07-15 filing reads `20260630` and passes. period `0` = scanned/no text layer ⇒ can't
  tell ⇒ don't block. **A mismatch means WE PICKED THE WRONG ANNOUNCEMENT — it is never evidence the company
  skipped the quarter.** A name that yields nothing prints an explicit "NOT proof it didn't file" line.
- **⚠️ Order candidates NEWEST-FIRST; a relevance rank may only break a SAME-DAY tie.** Ranking across dates
  pulls an older, tidily-titled *"Financial Results for the year ended…"* ahead of today's vaguely-titled real
  filing (this exact bug sent GYANDEV to its 2026-05-30 Q4 PDF). The case rank exists for — a CFO notice filed
  the same day as the results (INTEGRAEN 505358, whose real filing was 2 rows below a *"Chief Financial
  Officer"* notice that `RESULT_HEAD`'s bare `financial` matched) — is same-day, so a tie-break suffices.
  `NOT_RESULT` (CFO/KMP/AGM/newspaper/trading-window…) must only apply when nothing says "financial results",
  else a combined results+appointment filing gets excluded.
- **⚠️ A "Newspaper Publication" ad can OUTRANK the real filing it advertises** (found 2026-08-09, SRAMSET
  531359 + JAYATMA 539005). SEBI Reg 47 makes companies publish a newspaper ad ~1 day AFTER the real results
  filing, and that ad's HEADLINE often reuses the phrase *"Financial Results for the quarter ended…"* — enough
  to match `STRONG_RESULT` in `bse_render._candidate()`, which **lets STRONG_RESULT bypass `NOT_RESULT` outright**
  (needed so a combined results+appointment filing survives — see the INTEGRAEN case above), so the
  newspaper-publication category match is never rejected either. Being one day NEWER, the ad then wins the
  date-first sort and gets rendered — but it's a scan of a full newspaper PAGE (multiple companies' ads) whose
  own company's block is just prose pointing at the website, no P&L table. The `pdf_period` tripwire does NOT
  catch this: the ad genuinely states the right quarter, it just has no numbers. Symptom: a vision agent
  reporting the page is a newspaper notice / "see website for full results" with no table — re-fetch
  `bse_render.announcements()` for the scrip and manually pick the actual **"...Financial Results for the
  Quarter Ended..."** (not "...Newspaper Publication...") filing, usually filed the day before.
- **⚠️ All-scanned PDFs (no text layer on ANY page) still lose the table to page-order.** `render_pdf_pages`'s
  NUMERIC DENSITY scoring (below) needs a text layer to count tokens; a text-less page always scores the flat
  `SCAN_SCORE=40` regardless of what's on it, so when EVERY page in a filing is a scan, every candidate ties and
  the first 4 by page order win — which can be cover letters / KMP-change annexures instead of the P&L (found
  2026-08-09, SIMMOND 507998: an 11-page all-scanned filing where the real standalone/consolidated tables sat on
  pages 6 and 8, but the render kept pages 0,2,3,4 — a CFO-appointment Annexure-C and blank cover pages). This is
  the same failure family as the TELGE/VIRTUALG cases below, just with zero scoreable pages instead of some.
  Fix on sight: render ALL pages of the PDF yourself (`bse_render.fetch_pdf` + `fitz`) and look — don't trust a
  first-4 render on an all-scanned filing.
- **⚠️ `render_pdf_pages` picks P&L pages by NUMERIC DENSITY, not by position** (`bse_vision_prep.py`). The
  table can sit deep behind a long auditors' report (CENTRALBK Q1FY27: consolidated P&L on **page 10 of 31**),
  and `PL_HINT` matches auditor prose too ("net profit/(loss) after tax"), so a first-N-pages/first-4-hits scan
  renders the review report and never reaches the numbers. Real tables run **49-196 numeric tokens/page** vs
  **≤32** for prose. A text-less **scanned** page scores `SCAN_SCORE=40` — deliberately BETWEEN the two: worth a
  look, but it must never outrank a confirmed table (scoring blanks first rendered 4 blank pages and skipped
  BOTH of TELGE's tables on pages 4 and 11). Symptom to distrust: a vision agent reporting *"the attachment is
  only the auditor's report, no P&L"* usually means **the renderer missed the table**, not that it's absent.
- **⚠️ UNIT: thousands → crore is ÷10,000, NOT ÷100,000** (1 crore = 10^7 rupees). The scheduled task's own
  extraction rules carried ÷100000 until 2026-07-17 — 10x wrong, silently understating every
  thousands-denominated filing (GYANDEV Q1FY27 PAT read −0.0041 cr; true value **−0.057 cr**). Lakhs ÷100,
  millions ÷10, absolute rupees ÷10^7. **Cross-check whenever EPS + paid-up capital are printed:**
  PAT ÷ (paid-up capital ÷ face value) must equal the printed EPS — GYANDEV: 30,000 thousand ÷ ₹10 =
  3,000,000 shares; −0.057 cr ÷ 3M = −0.19 = printed EPS ✓. A power-of-ten miss = wrong divisor.
- **⚠️ State files `scripts/_bse_fund_done.json` + `_bse_sectors.json` are force-tracked** (gitignore `_*` hid
  them — negations added). CI needs the resume ledger to persist, else it re-grinds from scratch.
- **YoY limitation (v1):** the fund grind fetches only ~recent filings (`--months 5`), so BSE-only YoY often
  shows "—" (no year-ago base). Deepen by raising `--months` + fetching older announcements if wanted.
- **LIVE top-up (2026-07-12):** the page also polls our Cloudflare Worker
  (`stocksworld-quotes.dhruvan2510.workers.dev/?announcements=1`, hardcoded `WORKER` const) every 60 s for
  today+yesterday's filings — merged/deduped over the file rows (extends the To-date past the committed file),
  "● LIVE HH:MM IST" badge on success, SILENT no-op if the Worker/NSE is down (file data remains). Worker source =
  `scripts/live-quote-worker.js` (3 routes: ?symbols= quotes, ?chart= home ticker, ?announcements=1; NSE cookie
  warmup + 90 s cache inside the Worker). **NSE-from-Cloudflare VERIFIED WORKING 2026-07-12** (500 rows, filings
  minutes old); if NSE ever starts 403-ing, the live layer just never lights up (page still fine on file data).
  **Deploy (proven 2026-07-12):** the CF dashboard quick-edit iframe is cross-origin — browser automation CANNOT
  type into it; deploy via the dash API from a logged-in dash tab instead, and it's TWO steps under CF's versions
  model: PUT `/api/v4/accounts/<acct>/workers/scripts/stocksworld-quotes` (multipart metadata{main_module:
  'worker.js', compatibility_date} + worker.js module part — creates a VERSION, does NOT go live) then POST
  `…/scripts/stocksworld-quotes/deployments {strategy:'percentage', versions:[{percentage:100, version_id}]}`.
  Needs the user's explicit deploy authorization (auto-mode blocks it otherwise). Manual fallback: paste the file
  in the quick editor + Deploy (LIVE_FEED_SETUP.md).

### 17b. VISION-FILL = CLOUD ROUTINE, 4×/day  (ported off the desktop 2026-07-28)

The `bse-vision-fill` reader — the thing that guarantees no declared result stays "numbers being
parsed" — is a **Claude Code CLOUD routine** (claude.ai/code/routines, id
`trig_01N3H7t8Dgn2XmLqwBg94j2r`), no longer a local desktop task. It runs on Anthropic's cloud on
the user's plan: no API key, no laptop-awake dependency.
- **Schedule: cron `0 8,11,15,18 * * *` UTC = 13:30 / 16:30 / 20:30 / 23:30 IST.** Each slot sits
  just AFTER a free-reader wave, honoring the standing contract *"other fetches first; vision only
  fills what they missed"*: 13:30 after the 12:10 BSE OCR grind; 16:30 for post-market filings;
  20:30 right after the 15-min XBRL window closes (09:30–20:29 IST); 23:30 after the 22:10 BSE full
  run + 23:15 XBRL nightly pass (the old desktop slot). The contract is also STRUCTURAL, not just
  timing: `find_pending` (results_pending.py) subtracts everything the crons already filled, and the
  NSE-side vision overlay applies to EMPTY cells only, so real XBRL always supersedes.
- **Landing path — direct push to main is 403-blocked for cloud sessions.** The routine pushes a
  `claude/vision-fill-<timestamp>` branch → `gh pr create` → `gh pr merge --squash --delete-branch
  --admin` (merges within seconds; PRs #4/#5/#6 were the first three). A conflicted merge = leave
  the PR open and report; the next slot re-fills the same numbers.
- **There is NO local fallback task anymore** — the old Windows scheduled task, its worktree and its
  SKILL.md went away with the Windows box (retired 2026-08-05). If the cloud routine breaks, fix and
  re-run the routine itself; do NOT recreate a local scheduled task.
- **⚠️ The cloud prompt lives in the trigger config, NOT in a repo file.** Update via the /schedule
  skill → `RemoteTrigger {action:"update"}` (or the routines web UI). When a vision-pipeline script
  changes behavior, PORT THE CHANGE TO THE CLOUD PROMPT — nothing syncs it automatically (the
  2026-07-27 `--qefix` fix only reached the cloud prompt on 2026-07-28, manually).
- Environment `env_01Pb6Vujaf9FQ9m1kZXYJN9c`; sandbox egress must allow api/www.bseindia.com,
  nsearchives/www.nseindia.com and the CF worker. BSE IS reachable from Anthropic egress (verified
  2026-07-23); if that ever changes the run reports a cloud-IP block explicitly.

---

## 18. DATA HEALTH MONITORING + COMMIT GUARDS  (docs/status.html — "Data health" nav, built 2026-07-16)

- **Manifest = single source of truth:** `docs/feeds.json` — every feed → producing workflow → consuming
  pages, plus per-feed `max_age_hours` (null = static/reference file), `min_bytes`, `min_ratio` (guard's
  shrink floor; 0 = feed legitimately shrinks, e.g. rolling results_feed). Add a new feed = add one entry here.
- **Nightly monitor:** `feed-monitor.yml` (21:30 UTC = 03:00 IST, after all daily jobs) runs
  `scripts/check_feeds.py`: exists + min_bytes + JSON-parses + last-commit age ≤ max_age (age via GitHub
  API in CI — shallow checkout has no history; local runs use `git log`). Also checks INFRA: `data` release
  asset age, `sf-data` repo head age, Supabase `bt_public` RPC alive (the daily backup workflow is what keeps
  Supabase awake — if red, restore project in Supabase dashboard, free tier can't be woken by API).
  Writes `docs/status.json` → rendered by **docs/status.html**; opens/updates ONE GitHub issue titled
  "Data feed alert" when anything is red, auto-closes it when all green again.
- **Source-level freshness (`json_rows_age` special, added 2026-07-17):** a source can die INSIDE a feed
  that still updates (the BSE merge import-crash: NSE rows kept results_feed.json fresh while BSE rows
  stopped — invisible to whole-file age/size checks). The `json_rows_age` special asserts the newest row
  whose URL field contains `match` (e.g. "bseindia") is younger than `max_age_hours` (120h — survives
  holiday clusters). Add one per multi-source feed. PAIRED with a LOUD smoke-import step (NO `|| echo`)
  in refresh-results-hourly.yml + refresh-announcements.yml: `import bse_fetch, fetch_bse_results` fails
  the run visibly if a heavy top-level import creeps back. Rule of thumb: `|| echo` is for exchange
  flakiness, NEVER for code that can be broken — broken code must fail loud.
- **Commit guards:** every feed-committing workflow (11 of them) runs `scripts/guard_feed.py` right before
  its commit step: any modified docs/scripts .json/.bin/.html must parse (json), be ≥ min_bytes, and not
  shrink below min_ratio × committed size — else the step FAILS (visible red run; previous good data stays
  live). False trip on a legitimate shrink → raise that file's `min_ratio`/`min_bytes` in feeds.json.
- **Fixed 2026-07-16 — mf_funds.json/mf_history.bin were built nightly then DISCARDED:** fetch_mf_returns.py
  always rebuilt `docs/mf_funds.json` + `docs/mf_history.bin` (+ gold via add_gold_instrument) in refresh-mf.yml,
  but the commit step's reset-and-replay only carried mutual-funds.html + scripts/mutual_funds.json, so the fresh
  backtest files were thrown away every night (mf_funds.json frozen at 2026-06-06). Fix = carry both docs files
  (+ scripts/gold_inr.json cache) through /tmp in the commit step. Lesson: in reset-and-replay commit steps, EVERY
  file the fetch step writes must be in the cp-to-/tmp + cp-back + git-add lists, or it silently never publishes.
- **Same bug again 2026-07-16 — docs/dash_slim.bin (refresh.yml):** built by build_compressed.py every run but the
  commit step only carried nse-bse-dashboard.html → frozen at 2026-06-30 while sectors.html default views +
  build_discovery/enrich_order_shares/build_quarterly_results (they read the COMMITTED copy in CI) used June-30
  prices. Fix = carry it through /tmp; committed only when data really changed via `scripts/dash_slim_same.py`
  (ignores generatedAt; FAIL-OPEN — any compare error commits) so identical same-day re-runs don't add ~2 MB each.
  ⚠️ docs/stock_data.bin (17 MB) is the DELIBERATE exception: weekly via refresh-membership.yml, never 3x/day.
- **Archive sweep 2026-07-16:** ~1,839 one-off `_*.py` backfill scripts (all untracked, never committed)
  moved to `scripts/archive/` (gitignored); 35 still-referenced `_*` tools kept in scripts/ (resweep loop,
  membership history, FM vision helpers). 81 scratch PNGs deleted. If an old procedure references a missing
  `_*.py`, look in scripts/archive/ first.
- **NSE-side API lockdown (first seen 2026-07-20):** "run failed" emails where update_fundamentals /
  fetch_actions show 403 or the bot-challenge HTML on EVERY transport — urllib AND curl_cffi AND the CF
  worker (worker body `{"error":"NSE HTTP 403"}` → HTTP 502) AND a local residential-IP run — mean NSE's
  Akamai has hard-locked `www.nseindia.com/api/*` for all non-real-browser clients. Nothing to fix in code:
  do NOT add stronger impersonation. Verify blast radius instead: refresh-bse.yml green ⇒ declared results
  still fill from BSE PDFs (vision net); bhavcopy/nsearchives jobs (Daily stock data refresh) are unaffected.
  Recovery is automatic — the 15-min fundamentals cron self-heals (fill-only, keyed by qe_Date) and ex-dates
  is a stateless rebuild; probe recovery with a curl_cffi `size=1` GET of integrated-filing-results, then
  `gh workflow run "Daily fundamentals refresh"` + `"Refresh ex-dates calendar"`. The auto-rerun
  "still failing after 5 auto-retries — needs a human" email is the system working, not a new bug.

---

## 19. SITE FEATURES ON SUPABASE  (watchlist / triage / settings-sync / insurer inbox / page stats / LIVE TRACKING — built 2026-07-16)
Six user-state features on the SAME free Supabase project (`nebjnsndgrhumnkuipqy`) and security model as the
backtest history (public reads; writes carry the public token `sw_owner_8Kq2Lm9Xp4Rt7v`; RLS on, tables reachable
only through SECURITY DEFINER RPCs; daily GitHub backups = recovery). **Schema: `scripts/supabase_features.sql`
— deploy/extend by pasting the whole file in Supabase dashboard → SQL Editor → Run (idempotent).**

**Plumbing (every page gets it automatically):**
- `docs/sw-sync.js` — tiny fetch-based RPC client (NO supabase-js): `swSync.kvGet/kvSet/kvAppend` (kv docs
  WATCHLIST/TRIAGE/SETTINGS/INSURER_INBOX/PRESETS, whitelisted in `sw_kv_ok()`), `pvHit/pvStats` (page-view
  counter, auto-pings once per session per page), `picksSet/picksGet` (forward-tracking log), plus
  `syncSettings/pushSettings` (cross-device localStorage sync — key list lives in theme.js `loadFeatures()`).
  Offline/pre-deploy it mirrors to localStorage with a dirty-flag that self-heals on the next reachable load.
- `docs/sw-watchlist.js` — site-wide ⭐ stars: any page renders `<span class="sw-star" data-sym="X"></span>`
  and this file paints/toggles/syncs it (MutationObserver + one delegated click handler).
- `docs/theme.js` `loadFeatures()` injects both scripts on EVERY page (order matters: sw-sync first).
- **OWNER vs visitor:** owner browsers (unlocked once via `?ownerkey=…`, same `bt_owner_key` as backtests) read/
  write the SHARED kv docs = cross-device sync. Non-owner visitors get private browser-local lists (never pushed)
  so strangers can't scribble on the owner's watchlist. INSURER_INBOX appends + pv hits are open to everyone.

**Features & where:**
- **Watchlist** `docs/watchlist.html` (+ stars on dashboard/discovery/announcements/quarterly-results): entries
  `{s,note,ts,nts}` in kv WATCHLIST; notes edited on the watchlist page. quarterly-results' old `qr_watch` stars
  are BRIDGED two-way into it (one-time seed guarded by `qr_watch_migrated`).
- **Discovery triage** (`discovery.html`): 👍/👎 per stock in kv TRIAGE (`{s,st:'in'|'out',ts}`), rejected rows
  fade, "Hide 👎 rejected" toggle (`sw_triage_hide`, settings-synced).
- **Settings sync**: theme, sector watch, fav strategies, worker URL, dashboard "My screens"
  (`sw_dash_presets`, UI in build_compressed.py template — EDIT THE TEMPLATE, docs copy was hand-mirrored once),
  rotations. Pull-on-load / push-on-pagehide (owner only).
- **Insurer inbox** `docs/insurer-inbox.html` → kv INSURER_INBOX → `scripts/apply_insurer_inbox.py` in
  refresh-fundamentals.yml (EVERY trigger; stdlib-only, validates vs the same INSURERS ranges as
  fetch_insurers.py — keep the two dicts in sync), fill-only unless `force`, touches `.fund_updated`,
  writes ✅/✖ statuses back so the page shows them.
- **Page stats** `docs/analytics.html` ← `sw_pv_stats` (table `sw_page_views`, IST day). Real visits keep the
  free project awake (plus the daily backup ping).
- **LIVE TRACKING (paper-trade forward)** `docs/live-tracking.html` ← `docs/live_tracking.json`, baked daily by
  `.github/workflows/log-picks.yml` → `scripts/log_picks.mjs` → drives
  `saved-strategies.html?logpicks=1` (bake-style: DOM signal `#logpicksOut` "✅ LogPicks done"/"LogPicks error").
  That mode: per unique saved strategy (identityKey; canonical variant = longest window; sid = `'st'+hash36(key)`)
  logs `{n,f:freqMonths,tn:topN,picks:[{s,t,p}]}` into `sw_picks_log` upserted on (data-day SF.end, sid) — then
  rebuilds NAV: value holdings daily via `markPrice` (a dead series is carried at its last traded close since 2026-08-11, was delisting→0), rotate into that day's logged picks when the
  `floor(monthKey/freq)` bucket changes, Nifty benchmark from `nearestNifty`. Chains off "Bake backtest snapshots"
  success. A missed day = one fewer NAV point (fine). Picks depend on cfg+topN, NOT the backtest window.
- **Backups:** backup-backtest-history.yml also snapshots the 4 kv docs + sw_picks_get daily (7-day window,
  non-empty guard). Restore = `bt_restore.py`-style REST push of the JSON back into the RPCs.

**Gotchas:**
- sw_kv whole-doc writes are last-write-wins across devices (fine for one owner; appends are race-free serverside).
- `sw_kv_ok()` is the key whitelist — adding a new kv doc = add the key there + re-run the SQL file.
- Until the SQL is deployed every feature silently runs browser-local (by design); analytics shows "no visits yet".
- log-picks CI exits 1 only on "LogPicks error" (a real logging failure), not on partial anything.
- **Live-tracking updates on TRADING-DAY EVENINGS ONLY (~21:00 IST Mon-Fri)** — the whole chain
  hangs off "Daily backtest data refresh" (weekday cron). A weekend/holiday "stale" date on the
  page is BY DESIGN, not a failure (user asked exactly this 2026-07-20 after the weekend).
  For a genuinely missed evening the refresh now also has an 03:25 UTC Tue-Sat catch-up cron
  (added 2026-07-20, same pattern as delivery/deals/indices) — safe because the updater appends
  all missing days and picks upsert on (data-day SF.end, sid), so a redundant run is a no-op.
  If live-tracking still looks stale on a trading evening, check
  `gh run list --workflow="Daily backtest data refresh"` FIRST, then dispatch it; the bake and
  log-picks chain follows automatically.
- **⚠️ Postgres function gotcha (cost 2 deploy cycles 2026-07-16):** RPC parameter names must match what
  the site sends (`{"page":…}`, `{"k":…}`), but a plpgsql function whose parameter shares a TABLE column
  name breaks its own DML — bare refs are 42702-ambiguous, and `#variable_conflict use_variable` then
  hijacks `ON CONFLICT (k)` targets (42P10). Fix pattern (used by sw_kv_set / sw_pv_hit): write such
  upsert RPCs as **LANGUAGE SQL** — there columns win over parameters, and `fnname.param` pins the
  parameter side. plpgsql is fine when no param name collides with a column (sw_picks_set: day_in/sid).
- **Private (owner-only) pages — added 2026-07-16:** watchlist / live-tracking / insurer-inbox / analytics
  are hidden from the Menu, footer and home tiles for non-owner browsers and render a 🔒 lock card if opened
  directly. Mechanism: `PRIVATE_PAGES` list in theme.js (filters NAV_GROUPS before `window.SW_NAV` export;
  theme.js also accepts `?ownerkey=` itself so the unlock visit shows the full nav) + a `SW_OWNER`/`swLock()`
  gate at each page's boot. Owner = browser holding `bt_owner_key` (same `?ownerkey=…` unlock as backtests).
  Make a page public again: remove it from PRIVATE_PAGES + delete the gate lines in its boot. ⚠️ This is a
  client-side curtain, NOT auth (GitHub Pages can't do logins): the HTML/JSON stay fetchable by URL and the
  Supabase kv reads are public — fine for hobby scale, revisit if real secrecy is ever needed.

---

## 20. RESULTS COVERAGE DASHBOARD  (docs/results-coverage.html — "Results coverage" nav, built 2026-07-16)
**"How many results were declared this quarter, how many are filled, how many aren't."** The observability
layer over the vision routine — before this, a stuck/behind routine was invisible until someone noticed an
empty cell on Quarterly Results.

**Single source of truth — `scripts/results_pending.py`.** The counts on the page and the work the scheduled
vision routine picks up come from the SAME `classify()` call, so they can never drift. Pure JSON, no
fitz/network imports, so it's safe in a light CI step. `bse_vision_prep.py` imports `find_pending` from it
(that logic used to live inside bse_vision_prep — do NOT re-inline it).

`classify()` walks `results_feed.json` for the current quarter (`quarterly_results.json.quarters[0]`),
dedups by symbol (the feed can carry >1 filing per company), and buckets each declared company:
- `filled`  — numbers exist (XBRL/OCR cron, or an earlier vision fill: `sf_fundamentals` row w/ rev or pat,
              `vision_fills[sym][qe]`, or `bse_fundamentals.px[scrip][qe]`)
- `pending` — no numbers + an attachment exists → the routine WILL fill it on its next run
- `no_pdf`  — no numbers + NO attachment to render (NSE only; BSE-only names look their attachment up live
              by scrip, so a missing feed filename doesn't block them). The routine CANNOT fill these —
              they need the XBRL cron. If this count grows, that's a real gap, not a routine failure.
- `bse_dup` — NSE-side row flagged also-BSE-listed; handled by the BSE pipeline, excluded from "open".

**Build:** `python -X utf8 scripts/build_results_coverage.py` → `docs/results_coverage.json`
(`stat{declared,filled,pending,no_pdf,open,vision}`, `byExch{NSE,BSE}`, `rows` = the not-filled list,
biggest-mcap first). `stat.vision` = how many of the FILLED came from the vision routine rather than XBRL —
that's the routine's contribution made visible (43/91 on 2026-07-16).

**Refresh:** wired into `refresh-results-hourly.yml` (rebuilds hourly right after the feed, commits
`docs/results_coverage.json`), AND into the `bse-vision-fill` scheduled routine's step 5 so the page reflects
a fill immediately instead of waiting for the next hour.

**PRIVATE (owner-only).** Listed in `theme.js` PRIVATE_PAGES → hidden from nav/footer/home tiles, and the page
itself renders the standard `swLock()` 🔒 curtain unless this browser holds `bt_owner_key` (unlock once via
`?ownerkey=…`). Same client-side curtain as status/analytics/watchlist — GitHub Pages has no real auth, so
`results_coverage.json` remains fetchable by direct URL (it's only counts of public filings). Make it public
again by removing it from PRIVATE_PAGES + deleting the swLock block.

**Gotchas:**
- Adding the page = new SHELL entry in `docs/sw.js` + **bump `CACHE`** (v22→v23, then v24 for the private
  flip) or the PWA serves the stale shell/theme.js — a stale theme.js would keep showing the nav link. Nav is one line in `theme.js` NAV_GROUPS (Tools) — don't hand-edit each page.
- `declared` counts unique COMPANIES, not feed rows — a raw `len(rows)` on the feed reads 1 higher when a
  company files twice (93 rows vs 92 companies on 2026-07-16).

---

## 21. MARKET BREADTH  (Market Mood page section — built 2026-07-16, SELF-UPDATING)
**Daily Nifty 500 breadth on docs/market-mood.html:** % of members above their 200-DMA (chart w/ 20/80
zones), new 52-week highs vs lows (bar chart, auto-aggregates to N-day totals on long ranges), plus
advancers/decliners — all in one hover tooltip + 4 stat cards.

- **Builder:** `scripts/build_market_breadth.py` → `docs/market_breadth.json` (~63 KB). Same inputs as the
  turnover build: the FRESH `sf_stock_data.bin` (workflow downloads the `data` release asset; locally use
  `SF_BIN=<path> python -X utf8 scripts/build_market_breadth.py` — NEVER build from the frozen docs copy, §0)
  + point-in-time membership from `scripts/_n500_master_history.json` (nearest-prior snapshot per date).
- **Conventions:** closes are the bin's corp-action-adjusted `c`. DAILY ERA ONLY (`dailyFrom` 2018-01-01 —
  pre-2018 is weekly-sampled, a 200-"day" MA there would span years), so the series starts when the 52w
  window fills for ≥300 members = 2019-01-08. Windows are observation-based (200 / 252 of the symbol's own
  sessions, incl. today; new high = close equals the 252-session max, ties count). adv/dec = vs the member's
  own previous observed close (flats count as neither). Glitch dates w/ <50 member observations dropped.
- **Refresh:** wired into `refresh-market-mood.yml` (weekdays 21:35 IST, right after the turnover build; the
  commit step carries BOTH jsons through /tmp — reset-and-replay gotcha §18). Push-path self-test: pushing
  the builder or workflow re-runs it. Feed monitored via feeds.json (max_age 110h, min_ratio 0.9).
- **Sanity anchors (re-check after any rebuild):** pct200 min = 2.9% on 2020-03-23 (COVID bottom, 344 new
  52w lows that day); max = 98.5% on 2020-12-16; most new highs 101 on 2024-01-04.
- **Page:** breadth section between the turnover chart and the monthly-history table; separate range state
  (6M/1Y/3Y/5Y/All → 126/250/750/1250/all sessions); section hides itself silently if the json is missing.
  Editing market-mood.html = bump `docs/sw.js` CACHE (did v24→v25).

---

## 22. FII/DII HOLDINGS PER STOCK  (docs/shareholding.html — "FII/DII Holdings" nav, built 2026-07-16)
**Per-stock institutional holding % + QoQ change from NSE quarterly shareholding-pattern (SHP) filings,
refreshed 2×/day as companies file** (companies file on different days within 21d of quarter end, so new
rows land daily during season). Distinct from `fii-dii.html` (market-level daily buy/sell FLOWS) — the two
pages cross-link.

- **Fetcher = `scripts/fetch_shareholding.py`** (stdlib-only; reuses build_fundamentals' NSE session):
  1. Master list per quarter-end: `/api/corporate-share-holdings-master?index=equities&from_date=<QE>&to_date=<QE+180d,
     capped at today>` — ⚠️ **the window filters on the SUBMISSION date, NOT the as-on date, since ~2026-08**
     (it filtered on as-on when this was built, and the switch was SILENT). from=to=quarter-end now matches only
     the 1-2 filings submitted ON the quarter end, so the daily top-up quietly stopped adding anything —
     Jun-2026 sat at 2,196 cells while 2,284 existed. Fix (2026-08-04): ask for the filing SEASON
     (QE → QE+180d, capped at today; the API doesn't truncate — a 6-month window returns ~4,800 rows) and keep
     the rows whose **`date`** field (= the as-on date) equals the quarter. Mid-quarter as-on dates = event-based
     SHPs (capital changes) — still deliberately dropped (quarter-end series only, Trendlyne-comparable).
     **Symptom to watch for: `master <QE>: N filings ... , M as-on this quarter` with M in single digits.**
  2. Each master row carries an **`xbrl` url** (nsearchives, ~150-500 KB) → parse category facts
     `ShareholdingAsAPercentageOfTotalNumberOfShares` per context member (contexts with exactly one
     explicitMember, no typedMember — typed = the named >1% shareholders):
     **FII = InstitutionsForeignMember** (FPI I+II + FDI + other foreign), **DII = InstitutionsDomesticMember**
     (MF+insurance+banks+PF+AIF…), MF = MutualFundsOrUTIMember, ins = InsuranceCompaniesMember,
     prom = ShareholdingOfPromoterAndPromoterGroupMember, pub = PublicShareholdingMember. Values are pure
     FRACTIONS (0.1867 = 18.67%); scale-anchored via total/prom+pub ≈ 1-or-100; sanity: fii+dii ≤ pub+2,
     prom+pub ∈ [98,102]. Only the post-2021 SEBI format has the Domestic/Foreign split — unanchored/old
     filings are SKIPPED (logged), never guessed (~1% of a season).
  3. Merge newest-submission-wins into **`scripts/shp_history.json`** (tracked):
     `{"_names":{SYM:name}, SYM:{QE:[prom,fii,dii,mf,ins,"sub-date",nsh?]}}`. **nsh = total no. of
     shareholders** (NumberOfShareholders fact, ShareholdingPatternMember context) — OPTIONAL 7th slot,
     appended only when the filing carries it; readers index 0-5 + optional 6. The 8 feed quarters were
     `--reparse`d 2026-07-16 (15.5k/20.5k cells have counts); older quarters gain nsh via the staged deep
     backfill. Revisions re-file the same (sym,QE) with a later submissionDate and auto-replace. History
     write ABORTs if cells would shrink.
  4. Build **`docs/shareholding.json`** (page feed: last 8 QEs, aligned cell arrays, name/mcap from
     dash_slim + sector macro from sector_classification; ABORT if <500 rows) + **`docs/shp_meta.json`**
     (tiny heartbeat, changes every run — feeds.json watches THIS for liveness at 36h; shareholding.json
     itself has max_age null because off-season it legitimately never changes).
  5. Bank every filing's **total share count** in `scripts/shares_outstanding.json` (tracked, tiny):
     `{SYM: [shares, "QE", "sub-date"]}`, newest quarter wins. Source = `NumberOfShares` on the
     whole-company (`ShareholdingPatternMember`) context, `NumberOfFullyPaidUpEquityShares` as fallback.
     Parsed by **`parse_shares()`, deliberately independent of `parse_shp()`** — a company with no
     institutional holding files only promoter/public rows, which parse_shp MUST reject (it can't tell
     "no institutions" from "old format", and zero-filling would poison FII/DII, §22b), yet those SME-ish
     names are exactly the ones that need a share count. It is also NOT a slot in shp_history: four readers
     index those cells positionally. See §22e for what consumes it.
- **Runs:** default = top-up last 3 QEs (current season + late filers/revisions of 2 back, only new/revised
  XBRLs re-fetched); `--backfill N` = deep fill; `--quarters <QE,QE,…>` = explicit list; `--reparse` =
  re-fetch even unchanged filings (schema upgrades, e.g. adding nsh); `--feed-only` = rebuild docs feed,
  no network; `--symbols A,B,C` = restrict to those tickers; `--fill-shares` = re-read only the filings of
  symbols with no share count yet (idempotent, cheap, safe to re-run). Initial backfill 2026-07-16:
  4 quarters (Sep-25→Jun-26), ~7k XBRLs, ~45 min, 6 threads.
- **Auto-refresh:** `.github/workflows/refresh-shareholding.yml` — cron 12:40 + 20:40 IST **daily incl.
  weekends** (filings land any day); reset-and-replay commit carries shareholding.json + shp_meta.json +
  shp_engine.json + shp_history.json + **shares_outstanding.json** through /tmp (§18 gotcha — a file the
  fetch step writes that is NOT on all three lists is silently discarded); guard_feed; dispatches pages.yml.
- **Per-stock view: `docs/stock.html` "Shareholding pattern" section** — quarterly table from
  shareholding.json (Promoters / FIIs / DIIs w/ +/− MF-insurance expander / Public&others=100−prom−fii−dii /
  No.-of-shareholders row that auto-hides while counts are absent); quarters oldest→newest, leading
  never-filed quarters trimmed; FUND_ALIAS fallback for renamed tickers.
- **Page `docs/shareholding.html`** (hand-maintained): stat cards (season filings / FII raised-vs-cut /
  DII raised-vs-cut / top FII add ≥₹500cr), filter chips (FII/DII raising/cutting, both, promoter, filed
  this week), min-move pp + mcap + sector filters, sortable columns, FII+DII sparkline w/ hover tooltip,
  Δ pills vs the stock's PREVIOUS filed quarter ("first" pill when no prior), NEW badge ≤3d, CSV export,
  sw-star watchlist, theme.js auto-cardify on mobile. Row cap 300 + "Show more".
- **Gotchas:** master `date` = the as-on date, `submissionDate`/`broadcastDate` = when it was filed, and the
  from/to window filters on the LATTER (see step 1 — this flipped silently and cost us a season of top-ups).
  GAYAPROJ-style +16pp FII jumps are usually restructuring allotments — real filing data, not bugs.
  BSE-only stocks (no NSE listing) have no SHP here (future work, BSE source).

### 22e. MARKET CAP FOR THE NSE-ONLY COHORT  (2026-08-04 — why E2E had no mcap or P/E)
**Every mcap on the site traces to ONE field: `Mktcap` in BSE's scrip master** (`fetch_all.py`). A company
NSE lists and BSE doesn't has no row there, so fetch_all takes its NSE-only branch and hardcodes `mcap: 0`
— **104 symbols, BSE Ltd and CDSL among them** (both genuinely NSE-only; E2E Networks came up NSE Emerge →
main board). Zero mcap then blanks **Market cap, P/E, P/S and P/B** on the stock page (all divide into it)
and leaves the stock at the bottom of its own peer table. It is NOT a fundamentals gap — E2E's filings and
TTM profit were complete the whole time.
- **Fix = `build_compressed.py`**, right after the 52w pass: for any meta with a falsy mcap and a `latest`
  close, `mcap = shares x latest / 1e7` using `scripts/shares_outstanding.json` (§22 step 5), tagged
  `mcapSrc: "shp:<QE>"`. **Fill-only — a real BSE mcap is never overwritten.**
- **Accuracy: median 0.08% vs BSE's own Mktcap, worst 0.3%** on a 20-name spread (RELIANCE→PGHL, 2026-06-30
  counts). The two sources are interchangeable in practice; the residual is the gap between the SHP as-on
  date and BSE's snapshot. Verified by rebuilding stock_data.bin on a mixed payload: BSE-sourced caps
  untouched, SHP-sourced ones filled, no-count symbols still 0.
- **Propagation is automatic**: `build_stock_slices.py` reads `docs/stock_data.bin` meta for both the slice's
  `mcap`/`mcapAt` and the peer table's mcap-ranking + P/E, so the stock page picks it up on the next bake.
- **Residual: 2 of 104** (INFRA, MSCIADD) — no SHP filing in the last 4 quarters, so no share count. Don't
  grind on them; they fill themselves whenever they next file.
- **Rejected alternative:** shares = PAT ÷ basic EPS from the quarterly XBRL. Fine for LTTS (1.2% off) but
  CYIENT came out 39% high and AFFLE 2.8x low (an ANNUAL EPS sitting in the Q4 slot). Don't resurrect it.

### 22b. DEEP HISTORY (2019-09-30 →) + the OLD XBRL FORMAT  (built 2026-07-16/17)
- **Format boundary = Sep-2022.** Quarters ≥ 2022-09-30 file the new taxonomy (InstitutionsDomestic/
  ForeignMember, values as FRACTIONS). Quarters ≤ 2022-06-30 file ONE `InstitutionsMember` bucket with
  per-type rows, values in PERCENT: FII_old = `InstitutionsForeignPortfolioInvestorMember` (+FVCI),
  DII_old = MF(`MutualFundsOrUtiMember`, lowercase-ti!) + AIF + VCF + `FinancialInstitutionOrBanksMember`
  + insurance + PF + **OtherInstitutions**. Parse gate: reconcile fii+dii vs the InstitutionsMember total
  (±0.35) — and NEVER zero-default when neither format's members are present (old files would otherwise
  poison fii=0/dii=0; that near-miss is why parse_shp requires explicit format evidence).
- **`OLD_OTHER_TO_DII = True` is CALIBRATED, don't flip it:** on the Jun→Sep-2022 seam (150 largest
  stocks, old parsed both ways vs stored new-format Sep-2022), median |seam| = 1.25pp with Other→DII vs
  1.83 with Other→FII; per-stock it's not close (HEROMOTOCO 2.36 vs 30.52 — its 14pp "other institutions"
  is clearly domestic). Residual seam ≈ genuine QoQ drift + definitional noise; documented, not fixable.
  - **⚠️ IT IS RIGHT ON AVERAGE AND WRONG PER FILING — see §22i.** Some filers put their WHOLE foreign
    block in `OtherInstitutionsMember`, so this rule silently books it as domestic and stores
    `fii = 0.00` with `fii + dii` still correct. 162 such cells were healed 2026-08-12. Do not flip the
    flag (the calibration stands); detect the per-filing case with §22i's row-identity proof.
- **⚠️ The Jun→Sep-2022 DELTA is NOT a stake change for DR-heavy stocks.** The new format dissolved the
  old separate "Overseas Depositories" bucket into the investor categories (SEBI look-through): INFY's
  14.2% ADR block "appears" inside FII/DII at Sep-2022 (dii 18.87→32.38 — a reclassification, both values
  as-filed and individually correct). Any quarter-over-quarter analysis MUST skip that leg — the backtest
  hard-excludes it (`FORMAT_BOUNDARY` in build_shp_backtest.py; streaks crossing it break, cash quarter).
- **Employee-trust partition fix:** big-ESOP/no-promoter cos (M&M 3.73%, ETERNAL 4.73%) fail a naive
  prom+pub≈100 check → the gate allows prom+pub+max(npnp,trust) ≈ 100. Fixed the ~1-8%/qtr skip rate.
- **⚠️ TWO CONCURRENT WRITERS CORRUPT shp_history.json (bit us 2026-07-16):** two sessions ran fetchers
  simultaneously → (a) both used the same `.tmp` path — one's os.replace stole the other's file
  (FileNotFoundError / WinError 5), (b) whole-file flushes from a stale in-memory copy REVERTED the other
  writer's freshly-added quarters, (c) a mid-write read produced a torn JSON. Mitigations now in code:
  pid-suffixed tmp + retry-on-PermissionError in save_hist (Windows-era: readers there blocked os.replace). For any
  big backfill while another writer may run: use **`--hist scripts/shp_history_stage.json`** (staging file,
  implies no feed/meta rebuild) then **`python scripts/_shp_merge_stage.py`** once the other writer exits
  (fill-only + newer-submission-wins + shrink-ABORT). CI is safe (workflow `concurrency` group).

### 22f. COVERAGE vs POINT-IN-TIME NIFTY 500, 2002→date  (audited 2026-08-07)
Denominator = survivorship-free membership (`git show origin/main:scripts/indices_history.json` →
`Nifty 500`, 120 snapshots, nearest-prior per QE, rename-normed, DUMMY* dropped — NEVER the checkout copy).
95 quarters Dec-2002→Jun-2026 = **47,436 member-quarters, 22.0k covered (46.4%)**. FII and DII coverage are
IDENTICAL in every quarter by construction (parse_shp writes both or neither).

| era | member-qtrs | cov | why |
|---|---|---|---|
| Dec-2002 → Jun-2010 | 15,473 | **0%** | no source (MC's SHP pages didn't exist; BSE files start Jun-2016) |
| Sep-2010 → Sep-2015 | 10,449 | **30%** | Wayback-MC harvest, full-depth already run; residual = captures that don't exist |
| Dec-2015 + Mar-2016 | 998 | 0.3% → **52.1%** | the seam FELL 2026-08-09 (below); 6 late-filed XBRLs also exist |
| Jun-2016 → Jun-2019 | 6,502 | 79% → **98.8%** | BSE-XBRL ledger + the 2026-08-07 sweep |
| Sep-2019 → Jun-2026 | 14,014 | 98% → **99.8%** | live NSE pipeline + the sweep |

Whole-sample: 49.7% after the 2026-08-07 sweep, ~51.4% after the 2026-08-09 seam fill, 51.8% on
2026-08-11 (24,568 / 47,433), then the aspx rounds took it to 96.0% and the round-5 + §22i seam
re-run to **96.4% measured 2026-08-12** (45,739 / 47,433; every calendar year ≥ 87%, 2017-2025 at
100.0%). ⚠️ **Coverage says nothing about correctness** — §22i healed 162 wrong cells that were
counted as covered the whole time. Re-run
`python3 -X utf8 scripts/audit_shp_coverage.py` rather than trusting any number printed here;
what remains open is pre-2010 (no source found, 7 sites + BSE measured empty)
and the un-captured share of 2010-2015.

**Post-Jun-2016 is THREE cells, all of them** (measured 2026-08-11 via `--missing 2016-06-30`):
JMTAUTOLTD Sep-16, MONSANTO Sep-16 (both mid-series — the symbol has rows either side), JBCHEPHARM
Jun-26 (continuous through Mar-26, SEBI deadline was 2026-07-21). Everything else Jun-2016→date is
covered. Don't re-derive "1,706 missing" from the historical block below — that was closed by the sweep.

**Year-wise view: `--year`** rolls up by calendar year of the QE and prints each year's worst and best
quarter. The spread is the point: 2010-2015 averages ~30-40% a year but swings 10.2% (Mar-15) to 72.1%
(Dec-15) — that era's coverage tracks which Wayback captures exist, not anything about the stocks, so a
yearly average there is a number you should never quote on its own.

**★★ THE §81 AGGREGATOR ROUTE DOES NOT TRANSFER TO SHAREHOLDING — all three MC surfaces MEASURED
2026-08-11, and Trendlyne's floor with them.** Asked directly ("can we fill the rest through
Moneycontrol the same way rev/PAT is getting filled?"), the answer is no, and the reason is
structural rather than a matter of effort. §81's power comes from
`appfeeds.moneycontrol.com/jsonapi/stocks/quarterly_results_responsive?...&limit=200` serving DEEP
history (111 quarters, back to 1998-12, for WESTLIFE). **There is no shareholding analogue.** What
exists, measured:

| MC surface | what it serves | verdict |
|---|---|---|
| `appfeeds…/jsonapi/stocks/shareholding?sc_id=<id>&start=0&limit=200` | 2–4 rows, newest **Jun-2020** | **dead legacy feed.** `fii` is the literal string `"0"` on 5/5 companies (RI/TCS/INE/HDF01/ITC) and `dii` carries TOTAL institutions (RI Jun-20 38.06 ≈ FII 24.3 + DII 13.6). Unusable, and it is a §61a mode-4 trap: HTTP 200, well-formed JSON, plausible promoter/public, silently wrong FII/DII |
| quote page `/india/stockpricequote/<sector>/<slug>/<sc_id>` | `showTrendGraph()` server-renders a JSON literal with real Promoter/FII/DII/Public/Others — **exactly 5 quarters** (RELIANCE Jun-2025→Jun-2026) | correct values, no history. A recent-window second opinion only |
| `/company-facts/<slug>/shareholding-pattern/<sc_id>[/<qtrid>.00]` | the DEEP per-quarter route | **dead on live MC** — serves the `/mc/error` page. It survives ONLY in Wayback, which is exactly what `fetch_shp_wayback_mc.py` already harvests (43,984 captures, two rounds, union ledger) |

Naming the endpoint suffix wrong also returns **HTTP 200** with the 7-byte body `BAD URL`
(`shareholding_responsive`, `shareholding_pattern`, `share_holding`, `shp`, `stock_holding`,
`holding_responsive` all did) — never gate an MC probe on the status code alone.

**TRENDLYNE's shareholding floor is Dec-2015, measured, and it lies with a 200.**
`trendlyne.com/equity/share-holding/<tid>/<SYM>/<DD-MM-YYYY>/x/` answers **200 for a 2011 or 2014
date** and the page still carries percentages (from unrelated widgets) — but the body says
*"Shareholding data of <company> is not available."*, the row set loses `foreign portfolio` and
`insurance compan`, and **the page's own quarter menu starts at Dec 2015 on every URL** (RELIANCE
2011-03-31 and 2014-03-31 came back byte-identical at 113,180 — the date is ignored). Gate on the
category ROWS, never on the status code or on the presence of `%`. So Trendlyne cannot reach the
2010-2015 residual either; it was the seam fixer (Dec-15/Mar-16) and a Sep-2019+ gap filler, which
is precisely the window where it does have data.

**Therefore the 2010-2015 residual (7,147 member-qtrs) is bounded by WHICH WAYBACK CAPTURES EXIST**,
as §22f already said — not by an untried site. Pre-Sep-2010 (15,473) has no source at all: the
ledger's earliest row anywhere is 2010-09-30. **Untested lead, recorded so it is not mistaken for
a closed door** ([[§57]] — a route returning nothing is never proof the value is unreachable): the
Wayback census enumerates the `moneycontrol.com/company-facts/*` prefix ONLY. MC's era quote pages
(`/india/stockpricequote/*`) are a SECOND capture family that was never enumerated, and whether
2011-2015 captures of those carry a shareholding block is **unknown — not measured**.

**★★★ SUPERSEDING DISCOVERY, SAME DAY (2026-08-11 evening): BSE's OWN ASPX PAGES SERVE THE WHOLE
PRE-XBRL ERA, LIVE — the "measured walls" above were walls in OUR ROUTES, not in the world.**
Prompted by the user ("a friend has filled 2011-2016 at 100%, so the data is there somewhere") —
and the friend is right. The `SHPQNewFormat` rows for pre-XBRL quarters have carried the answer all
along in their **`navigateurl`** field, which nobody followed:
`https://www.bseindia.com/corporates/ShareholdingPattern.aspx?scripcd=<code>&flag_qtr=1&qtrid=<q>.00&Flag=<New|Old>`
- **`Flag=New`** = the Clause-35 category table (the exact layout MC's company-facts pages mirrored):
  serves **Jun-2006 (qtrid 50) → Mar-2016**, measured full on RIL/HDFC(delisted)/MONSANTO/RUCHISOYA/
  CAPF; Mar-2006 (q49) comes back as a 4.7KB shell under this flag.
- **`Flag=Old`** = the 1997 SEBI format (`FIIS` / `Mutual Funds and UTI` / a
  `Banks,Financial Institutions,Insurance Companies` LUMP): serves **≤ Mar-2006 back to at least
  Mar-2001** (RIL Mar-2001 fii 17.34; HDFCBANK Dec-2002 18.55; HDFC Dec-2003 60.74). DII = mf + the
  lump (same components as our definition); `ins` is inside the lump → store None, NEVER 0.0.
- **qtrid is GLOBAL**: `(year−2001)×4 + {Mar:29, Jun:30, Sep:31, Dec:32}` — verified on 7 companies.
- Old rows' `XbrlFile` is empty and `filing_date_time` null — **the XbrlFile gate correctly said "no
  XBRL file" and everyone (2026-08-03, -07, -09) read it as "no BSE data"**. Different question.
- **Why the audits missed it:** the XBRL sweep gated on `XbrlFile`; the Wayback harvest replayed MC's
  MIRROR of these very tables; the aspx pages themselves were checked in Wayback ("never archived with
  old qtrids") but never fetched from the ORIGIN server. §57 rule 1, textbook case: a route returning
  nothing is evidence about THAT ROUTE only.
- **Fetcher: `scripts/fetch_shp_bse_aspx.py`** (frontier → pilot → harvest; caches pages; ledger out
  `shp_fill_bse_aspx.json.gz`, provenance `bseaspx:<code>:<qtrid>:<flag>`, sub = QE+21d convention).
  Ports the wayback-mc derivation verbatim: %of(A+B+C) column, dii = mf+banks+ins, 1pp inst-recon
  gate, prom fallback via pubtot≥99, fii never zero-defaulted. Flag=Old adds proven-zero (absent FIIS
  counts as 0 only when inst_sub == mf+lump to 0.15) and a 0.15 recon on the single-column layout.
- **PILOT (69 fetches, stratified 2002→2016 + 24 deliberate overlap cells): overlap gate 23/23 ≤
  0.11pp — top disagreements are 0.00pp exact** (RELIANCE 2013-2015 ×5, CAPF, ITC…). The aspx
  reproduces the Wayback-MC-derived cells digit-for-digit, which also independently validates that
  whole 2026-08-03/09 campaign (BSE origin vs MC mirror, different transport, same numbers).
- **⚠️ THE SEAM DEFECT IS IN BSE'S OWN TABLE, not just MC's mirror.** At qtrid 88/89 the aspx prints
  a fabricated `FII 0.00` (ITC Dec-2015: aspx 0.00 vs our derived 20.77) or fails inst-recon (4 of 4
  other pilot hits). MC mirrored the breakage faithfully. The harvest therefore SKIPS Dec-2015 +
  Mar-2016 — the §22f seam-derivation route owns those. And the same fabricated zero appears at
  qtrid 91 for MONSANTO/JMTAUTOLTD Sep-2016 (stored neighbours 3.75 / 17.74) — caught by the
  fetcher's **zero-vs-neighbour guard** (exact 0.00 beside a stored neighbour >1% = refuse), so both
  named holes STAY OPEN rather than get poisoned. Fabricated zeros are a BSE-side defect class.
- **Frontier: 22,622 missing member-qtrs Dec-2002→Sep-2015 (seam excluded), 20,912 with a scripcode**
  (master status-blank + `_shp_scripcode_override`), 131 symbols / 1,710 cells unresolved (era names
  needing ISIN or an era scrip master — log as not-found-via, they are NOT closed).
- **✅ HARVESTED AND APPLIED 2026-08-11 21:50 IST: +18,949 cells, coverage 51.8% → 91.7%.**
  20,912 fetched in 5,145s at 5 workers; 18,949 passed every gate (90.6%); ledger
  `scripts/shp_fill_bse_aspx.json.gz` (750 symbols), registered LAST in `BSE_HIST_LEDGERS` so every
  real-dated ledger wins on overlap. History 67,592 → 86,541 cells. Year-wise after:
  2002 **74.4%** · 2003-05 78-84% · 2006-09 80-88% · 2010-14 **89-93%** · 2015 81.5% · 2016+ unchanged.
  **The whole pre-2010 era went 0% → 74-88%** — a wall three separate audits had recorded as sourceless.
- **1,963 refusals logged to `scripts/_shp_aspx_rejects.json`** (not-found-via:bseaspx, §57 rule 2 —
  OPEN, not closed): recon 790, no-fii 580, absent 564, no-prom 26, zero-vs-neighbour 3.
  ⚠️ My first read said recon was "mostly Jun-2006" — WRONG, an artifact of the log's 8-per-class
  print cap sampling the alphabetical head. The journal says recon clustered at **2014-12→2015-09**
  (79+66+59+42) and no-fii at 2006-07. Diagnose reject classes from the JOURNAL, never the capped log.
- **✅ ROUND 2 (same evening, cache-only, +970 net cells → coverage 93.8%): both clusters were ONE
  parser gap each.** (a) recon 2014-15 = **SEBI's FPI migration** — the institutions block carries
  "Foreign Portfolio Invest*" as its own row or itemised under "Any Others (Specify)" (ADANIPORTS
  Sep-15: Any-Others 9.29 == FPI 9.29, count ONCE); FPI is foreign → fii. An anonymous Any-Others
  row folds into dii ONLY when reconciliation needs it, keeping the ~22k stored MC-family cells and
  these on one dii convention (mf+banks+ins). 22 already-applied cells now read ≤0.95pp higher fii
  (small FPI rows previously inside the 1pp tolerance) — fill-only keeps the stored value, drift
  journalled here. (b) no-fii 2006-07 = **early Clause-35 pages omit empty rows** (AGRODUTCH Sep-06:
  block = MF+banks only, subtotal proves fii) — residual-as-fii accepted at |r|≤0.15 outright, r>0.15
  only with a stored neighbour within 5pp, negative refused. **The zero-guard then caught 81 derived
  zeros beside neighbours holding >1%** — pages whose own subtotal is missing the foreign block (the
  Dec-2015 defect appearing sporadically earlier). Rejects now 1,055: absent 564, recon 364,
  zero-vs-neighbour 84, no-prom 26, no-fii 17.
- **✅ ROUND 3 (2026-08-11 night): the 131 no-scripcode era symbols → 81 RESOLVED, +906 cells,
  coverage 95.7%. Every year is now ≥85.8%.** Resolver `scratchpad→scripts` route: MC autosuggest
  (`pdt_dis_nm` = "<ISIN>, <NSE SYMBOL>, <BSE code>", works for DEAD companies) — 63 by exact
  NSE-symbol match, 3 by ledger-name, 15 by symbol-word-in-name (CASTROL→500870, COLGATE→500830,
  NESTLE, CEAT, 3IINFOTECH…), single-candidate gated; per-symbol evidence in
  **`scripts/_shp_aspx_resolved_era_syms.json`**. `symchg.csv` does NOT reach these renames
  (~2002-2010, before its window).
  - **Era renames make the page-name identity gate WRONG for mc:symbol rows** — SATYAMCOMP's page
    prints "Satyam Computer Services" (era name), MC's current name is "Mahindra Satyam"; the gate
    refused 58 correct cells across 4 symbols. Exact NSE-symbol+ISIN resolution outranks a
    cross-rename name fuzz → the gate is skipped for `via=mc:symbol` rows only. All 58 recovered
    cache-only. Eyeball: SATYAMCOMP promoter 20.74 (Mar-03) → 2.18 (Dec-08) — the Raju collapse,
    exactly as history records.
  - **Wrong-era resolutions self-neutralize as absents** (ASIANHOTEL→"(East)" is the post-2010
    demerger entity; its 2002-09 qtrids have no pages → 194 absent refusals, no poison possible).
  - Era-symbol cells are keyed by the ERA symbol (sf_revop precedent). If a rename pair later
    enters `_rename_map`, the audit's norm() merges them — fill-only union, no conflict.
  - **STILL OPEN: 50 symbols / ~590 cells MC's search can't surface** (GESHIPPING, BILT,
    HINDMOTOR, L&T-as-era-symbol, the ESSAR/JINDAL families…) — need per-symbol ISIN/era-master
    evidence, journalled in the rejects file's unresolved set, NOT closed. Whole-population reject
    journal now 1,269 cells. **Measured-empty for these 50 (don't re-probe): MC autosuggest ×3
    modes, `fill2020_tools/_mc_codes.json` (`None` — that campaign hit the same wall), the Wayback
    census `map.json` (`None`), NSE's symbolchange archive (the nsearchives "full" file IS the same
    1,054-row recent window), and era bhavcopies (2003 measured: NO ISIN column).**
- **✅ ROUND 5 — the route that DID crack the tail: Wayback captures of NSE's OWN securities
  master (2026-08-12 早): +172 cells, 96.4%.** `EQUITY_L.csv` archived 2006-08-24 / 2010-02-05 /
  2011-10-30 carries SYMBOL + NAME + ISIN — the exact missing link. **Fetch with the `id_` raw
  modifier** (`web.archive.org/web/<ts>id_/<url>`) — without it Wayback serves a ~10KB HTML wrapper
  that looks like a block page. 21 of the 50 matched; ISIN join to the blank-status BSE master
  (field `ISIN_NUMBER`) + guarded name fallback resolved 16 to scripcodes (BILT 500102,
  GESHIPPING 500620 page-verified of 10 candidates, HINDMOTOR 500500, LGBROS 500250…), evidence in
  `_shp_aspx_resolved_era_syms.json` (via=`nse-era-master:<ts>+isin|name1|aspxname`).
  - **⚠️ Name-containment fallback needs a ≥6-char norm floor** — without it "UT Ltd" ⊂
    "fUTureventures" and "B & A" ⊂ "lgBAlakrishnan" resolved WRONG entities (caught pre-harvest;
    the page-name identity gate was the backstop). Multi-candidate ties are settled by fetching one
    era aspx page per candidate and matching the printed company name.
  - **Relisted-code resolutions self-neutralize**: KIRLOSOIL→533293 / SUNDRMCLAY→544066 /
    ESSARSHIP→533704 are post-scheme relistings whose era qtrids have no pages → 98 absent
    refusals, zero poison. Their PRE-scheme era codes remain findable per-symbol.
  - **STILL OPEN after round 5: 37 symbols** — 29 delisted before the earliest EQUITY_L capture
    (Aug-2006: HTMT, INDOGULF, BOOTSPHARM, JINDLSTRIP, L&T-era…), 8 with ambiguous/absent BSE
    joins (KBL two-way tie; MONNETISPA/MANDHANA/SRIADIKARI/SUJANATOW absent from even the
    blank-status master). Reject journal 1,371 cells. Per-symbol archival work from here.
- **✅ ROUND 4 — THE SEAM, from BSE's own pages (2026-08-11 late night): +159 cells, 96.0%.
  Dec-2015 72.1% → 98.4%; Mar-2016 79.4% → 85.0%.** `scripts/seam_derive.py` inverts the
  institutions identity on the aspx q88/89 pages (fii = inst_sub − mf−banks−ins−govt−vcf, never
  subtracting foreign rows, no clamp), ledger **`shp_fill_seam_aspx.json.gz`** — a SEPARATE file,
  7th in `BSE_HIST_LEDGERS`, so no aspx-harvest rebuild can clobber it (stage-order trap).
  `_shp_seam_adjudicated.json` cells skipped (BBTC's drop stays deliberate).
  - **The two seam quarters are NOT equally broken.** Dec-15 batch-derives at **0.81pp median vs
    stored Sep-15** (n=127 — matches the MC route's 0.87pp; run-gate passed, 139 cells written).
    Mar-16 FAILED the 3.0pp run-gate at **5.73pp** — so q89 cells were written ONLY with per-cell
    corroboration (|derived − stored Jun-16| ≤ 3.0pp; 20 written, 69 held).
  - **★★ NEW DEFECT CLASS EXPOSED IN STORED DATA: Jun-2016 fii=0.0 fabrications.** 15 of the q89
    failures are the ANCHOR's fault, not the derivation's — LICHSGFIN derived 27.53 vs stored
    Jun-16 **0.0** (LIC Housing with zero foreign holding in 2016 is absurd; KSCL, SUPREMEIND
    same shape). These stored cells came from the 2016-19 old-XBRL ledger. Journalled to
    **`scripts/_shp_seam_suspect_jun2016_zeros.json`** for their own audit — do NOT trust a
    stored fii=0.0 at Jun-2016 as an anchor, and the eventual heal likely un-blocks most of the
    69 held q89 cells too. **→ AUDITED AND HEALED 2026-08-12, §22i below. The "BSE fabrication"
    reading was WRONG: those zeros are OURS.**
- **⚠️ A PASS THAT CHANGES ONLY PARSING MUST BE `--cache-only`.** The first recovery re-parse re-ran
  the fetcher normally: >75 min elapsed for **45 s of CPU**. Every refusal was retrying the alternate
  Flag whose page does not exist, and `fetch_page`'s 3-attempt backoff spends ~18 s per dead cell.
  The same work from disk took **8 seconds** and recovered +213 cells (promoter-less complement + the
  override-identity fix). **0.7% CPU over an hour is the tell that a job is sleeping, not working** —
  check `ps -o time,%cpu` before trusting any long-run ETA.
- **Identity-gate exception:** override-resolved rows carry `bname == ""`, and the aspx prints the
  CURRENT registered name for era quarters (RUCHISOYA 2002 → "Patanjali Foods Ltd"), so the era-name
  containment check must be SKIPPED for them — the override entry is itself the identity evidence.
- **Promoter-less filers** (ITC/FEDERALBNK/HDFC class) print nothing in the 1997 promoter block;
  prom is claimed as the complement ONLY when the two non-promoter Sub Totals close to 100 ±0.5.
  SOUTHBANK/RIIL still refuse — they don't close, so they stay open rather than get a guessed 0.
- Stock-page deep history (`shpH` in the per-stock fin slice) is rebuilt by `build_stock_fin.py` via
  `refresh-stock-fin.yml`, so per-stock tables pick these quarters up on the next CI run, not at push.
- Harvest → staging ledger → `_shp_merge_stage.py` flow (NEVER a direct shp_history write while the
  12:40/20:40 IST CI may run); after apply, re-run `audit_shp_coverage.py` + spot-verify LIVE.

**✅ SWEPT SAME DAY (2026-08-07): +1,604 cells, Jun-16→Jun-19 79%→98.8%, Sep-19→Jun-26 97.8%→99.8%.**
`scripts/fetch_shp_bse_hist.py` (rebuilt) → ledger `scripts/shp_fill_n500_gaps.json.gz` → applied by
`python3 scripts/fetch_shareholding.py --apply-ledgers` (new offline entry point: merge ledgers + rebuild
both feeds, no network). 3 min at 5 threads for 1,649 targets. The gap was 1,706 cells / 496 symbols:
- **48 cells never had a scripcode to try** — BSE Ltd (33) and CDSL are NSE-only; MAXINDIA/RUCHISOYA/
  TUBEINVEST/ALOKTEXT/SUPPETRO/GLOBOFFS are name mismatches. Those need NSE or an ISIN join.
- **41 cells absent at BSE** (tracked in `scripts/_shp_bse_absent.json`): 25 with no row at all, 16 where
  BSE lists a filename whose file 404s on every path and retry (KARURVYSYA/SUNDARMFIN 2016-18, the whole
  590xxx ex-regional series) — banked as absent so resume stops re-fetching them.
- **13 old-format parse skips** (RELINFRA/ZEELEARN/WELCORP at Jun-2016) — correct refusals, never zero-filled.
- **★ THE SURVIVORSHIP TAIL NEEDED ONE QUERY PARAMETER.** `build_bse_universe.py` asks for
  `...ListofScripData/w?...&status=Active` → 4,949 scrips, so every delisted N500-era name (ALBK, ANDHRABANK,
  DHFL, GRUH, HDFC, GSKCONS…) had no scripcode and looked unfillable. **Blank the status and the same
  endpoint returns 10,800** (4,612 Delisted + 1,236 Suspended), resolving 77 of those 85 symbols on a plain
  `scrip_id` match. The fetcher pulls the full list itself; the live master stays Active-only on purpose.
- Worst blackouts closed: **MCX / ABBOTINDIA / BAYERCROP had NOTHING since Sep-2019** (28 quarters each),
  NESTLEIND 15 (Sep-19→Mar-23), ITC 14, WESTLIFE 14 — all current N500 members, all silently dropped by the
  live NSE pipeline for years while `feeds.json` stayed green (it watches shp_meta liveness, which says
  nothing about per-symbol completeness).
- **MF slot bug found and fixed in the same pass:** new-format filings spell the member BOTH ways, and
  `MEMBERS` only mapped `MutualFundsOrUTIMember` — every filing carrying the lowercase-ti
  `MutualFundsOrUtiMember` (all BSE copies, all NSE filings before ~Jul-2025) got **mf=0.0, which reads as
  "no mutual-fund holding"** (MCX Mar-2025: dii 58.1, mf 0.0 → really 35.64). `parse_shp` now falls back to
  the old-format key inside the new-format branch. fii/dii were never affected. The cells already on disk
  kept their zero until the sweep in **§22g**, which re-read them from the filings.
- **✅✅ THE Dec-2015/Mar-2016 SEAM IS NOT A WALL — 0.3% → 52.1% (2026-08-09).** §22f previously
  recorded it as a measured two-quarter wall on the 2026-08-03 finding that MC's "Foreign
  Institutional Investors" row is empty at qtrid 88/89 for every company. **The row is empty; the
  data is not.** The institutions **Sub Total is printed and correct** and the foreign block is
  simply un-itemised, landing on whichever row survives the format change (HDFCBANK q88 leaves it
  loose, q89 puts 39.63 on the QFI row). So invert the reconciliation the ledger ALREADY enforces
  on every q≤87 cell — `fii + dii + govt + qfi ≈ inst_sub` — and read
  **`fii = inst_sub − (mf + banks + govt + insurance + domestic VC)`**. Arithmetic, not inference.
  - **Validated one quarter out, not three:** median |Mar-2016 derived − our Jun-2016 XBRL| =
    **0.87pp across 308 companies** (gate was 3.0pp / ≥25). 713 derived cells kept. Spot: HDFCBANK
    39.62 vs 39.59, INFY 40.17 vs 40.46, ITC 20.45 vs 20.60. ADR names are the known exception —
    ICICIBANK is 13pp out because its depository block sits outside (A+B+C) on MC but inside FII in
    the XBRL; that is the GDR column case the existing calibration handles.
  - **⚠️ NEVER CLAMP A DERIVED RESIDUAL.** The first version also subtracted qfi and wrapped the
    result in `max(0, …)`. At q89 the foreign block sits ON the QFI row, so it was subtracted twice
    and the clamp turned the negative into a clean-looking **fii = 0.00** — a fabricated "no foreign
    holding". A negative residual now DROPS the cell.
  - **⚠️ THE LEDGER FILE IS SHARED — MERGE, NEVER OVERWRITE.** `ledger` writes
    `shp_fill_hist_2010_2016.json.gz`, the same path the 2026-08-03 campaign wrote. The 2026-08-09
    re-run rebuilt the CDX census from scratch (the original died with the page cache) and produced
    a **smaller** frontier — 5,533 fetches vs 12,691 — so it held 787 cells the original lacked and
    the original held **1,528** it lacked. A plain overwrite silently destroys those. The tracked
    file is the UNION (577 companies / 4,905 cells), old cells preserved byte-for-byte.
  - **Reproducibility, and it is the strongest evidence we have for this whole route:** the 2,590
    cells present in BOTH harvests agree to **max 0.00pp** on fii and dii. An independent re-crawl,
    re-parse and re-derivation reproduced the original campaign exactly.
  - **⚠️ STAGE ORDER IS LOAD-BEARING — anything that EDITS the ledger must run AFTER `ledger`.**
    `fetch_shp_wayback_mc.py ledger` rebuilds `shp_fill_hist_2010_2016.json.gz` WHOLE, from the
    parsed cache. So a hand-adjudication written into that file before the stage re-runs is
    silently reverted, with no error and nothing in the diff to notice — the journalled-is-not-live
    trap wearing a different costume (§18 is the same shape for reset-and-replay). Current order:
    `census → map → frontier → harvest → [local re-parse] → ledger → _shp_seam_adjudicate.py`.
    The adjudicator is idempotent so it is safe to chain unconditionally; **any future routine that
    edits this ledger goes on the RIGHT side of `ledger`, never the left.**
  - **Adjudicating a seam cell against a THIRD anchor.** Where the MC derivation and a late BSE XBRL
    disagree, neither is automatically right — decide against `hist[sym]["2016-06-30"]`, which we
    parse ourselves and which neither route supplied. The 2026-08-09 referral of six cells split
    four ways: MINDACORP ×2 the doc won (mine failed to conserve the institutions total, so the MC
    block was mis-bounded); BFUTILITIE the split was corroborated and only the promoter came from
    the doc; BHARATFORG ×2 the doc's `fii 0.00` is the old-format no-FII-row artifact so mine was
    kept — but FLAGGED low-confidence, because my split doesn't run into Jun-2016 smoothly and
    Jun-2016's own split rests on `OLD_OTHER_TO_DII`, which is calibrated, not measured; BBTC was
    DROPPED (promoter 8pp low against two independent quarters, and no file exists to replace it —
    a deliberate hole, not a regression, if a coverage audit ever counts it). Rules and reasoning:
    `scripts/_shp_seam_adjudicated.json`.
  - **⚠️ "Real files start Jun-2016" is true of ORIGINAL filings only.** BSE carries late/revised
    XBRL inside the seam for at least MINDACORP, BHARATFORG, BFUTILITIE and BBTC. Don't read the
    Jun-2016 floor as a hard wall when chasing a specific cell.
  - **Rebuilding the census:** `python3 scripts/fetch_shp_wayback_mc.py census` (new stage). Use
    PAGE pagination and filter locally — a wildcard prefix that size plus a server-side regex makes
    CDX scan too far and it answers **504**. `showNumPages` on the bare prefix gives ~214 bounded
    pages → 43,984 shareholding captures.
  - **Harvest pacing:** 6 threads. Wayback refuses ~40% of connections under load and each costs a
    25s backoff, but concurrency still wins — measured 5 workers 2.81s/fetch vs 3 workers 3.80s.
    Don't read a worker-count difference off two short samples taken an hour apart; the refusal rate
    drifts with time of day and I mistuned once doing exactly that.
- **⚠️ `xbrlurl` IS TRUTHY WHEN THERE IS NO FILE.** Pre-2016 rows return `xbrlurl: "/XBRL1/"` (bare prefix)
  with an EMPTY `XbrlFile` — `if row["xbrlurl"]` counts 104/104 quarters "available" back to Mar-2001 and is
  a lie. **Gate on `(row["XbrlFile"] or "").strip()`.** ORIGINAL files start **Jun-2016** (40/40 sampled;
  Mar-2016 = 3/40, Dec-2015 = 0/40) — **but that floor is true of originals only. LATE/REVISED XBRLs
  exist inside the seam** (found 2026-08-09 by the revision sweep: MINDACORP ×2, BHARATFORG ×2,
  BFUTILITIE, BBTC — all with `revised_date_time` set, all parseable; adjudicated in
  `scripts/_shp_seam_adjudicated.json`, with MINDACORP's doc beating the MC-derived cells and
  BHARATFORG's doc carrying LESS information than them — fii lumped to 0.00). When chasing a specific
  seam cell, check for a revised row before declaring the era XBRL-free. (Two earlier readings of
  this line aged badly the same day: "the seam is a two-quarter wall" fell to the
  institutions-subtotal derivation, and "no XBRL before Jun-2016" fell to these six.)
- **`fetch_shp_bse_hist.py` (the 2016-19 ledger builder) was never committed** — only its output
  `shp_fill_hist_2016_2019.json.gz` is tracked. Re-running that route means rewriting the fetcher.
- **Re-run it: `python3 -X utf8 scripts/audit_shp_coverage.py`** (reads ORIGIN/MAIN, not the checkout;
  `--year` for the calendar-year rollup + each year's worst/best quarter, `--local`, `--csv out.csv`,
  `--missing <QE>` to list who is missing quarter by quarter).
- **Two ledgers, opposite meanings — don't merge them.** `scripts/shp_no_filing.json` = no filing was EVER
  made (entity merged/delisted mid-quarter, confirmed absent at BOTH exchanges) → the cell leaves the
  DENOMINATOR. `scripts/_shp_bse_absent.json` = one source didn't serve it → stays IN the denominator,
  because a dead route is not an absent filing. Seeded 2026-08-07 with IDFC and TV18BRDCST at 2024-09-30.
- **✅ BSE Ltd FIXED 2026-08-07 — it was a PARSER refusal, never a missing filing.** Three changes in
  `parse_shp`, +12 cells (Sep-2021→Jun-2025), Sep-2019→date now **99.9%**:
  1. **Scale anchor is a LADDER** — declared total → prom+pub → +third bucket, first candidate inside a
     band wins. BSE Ltd files a junk whole-company percentage (Sep-2024 `total` = 6.9) over a clean percent
     partition (prom 0.00 + pub 77.09 + npnp 22.90 = 100). Additive BY CONSTRUCTION: a filing that already
     anchored on its total is unchanged, so None→value is the only possible transition. The partition gate
     [98,102] still does the real work — the ladder only picks which number gets tested.
  2. **`TradingMembersAndAssociatesOfTradingMembers` = the third bucket** (slot `npnp2`, folded in by
     `_third()`). A demutualised exchange parks its restricted trading-member shares there; BSE Ltd tagged
     the SAME block as `SharesHeldByNonPromoterNonPublic…` at Sep-2024 and this at Jun-2025 (79.43 + 20.57
     = 100.00 exact). PARENT row only — CorporateTradingMember/IndividualTradingMember/… sum back to it.
  3. **`nsh` is dropped when it is below the public-shareholder count.** BSE Ltd Sep-2024 files 248 against
     539,914 public holders — publishing it would have rendered "248 shareholders" between two ~540k
     quarters. nsh is optional, so omitting is honest and the page auto-hides the row.
  **Regression harness `scripts/_shp_anchor_regression.py`** (tracked, re-runnable): parses the same bytes
  with `origin/main`'s parse_shp and the working copy and fails on any dict→different-dict. 900 filings /
  22 companies → 900 identical, 0 changed. RUN IT before touching parse_shp again.
- **★ NSE's master DOES serve history — the 2026-08-02 "rolling-window-only" verdict was an artefact of
  querying `from=to=QE`** after the as-on→submission switch. Ask for the filing SEASON and it answers,
  thinning going back: ~2,100 as-on rows at Sep-2024, 1,800 at Dec-2021, 62 at Mar-2021, 34 at Sep-2019.
  `scripts/fetch_shp_nse_gaps.py` is the ledger-producing tool for the NSE-ONLY cohort (BSE Ltd, CDSL —
  an exchange cannot list on itself, so no BSE route exists) → `shp_fill_nse_gaps.json.gz`.
- **Cross-checked against Screener** on all six overlapping BSE Ltd quarters (Sep-23 7.90/8.09, Dec-23
  12.03/11.27, Mar-24 13.04/12.69, Sep-24 13.01/11.68, Jun-25 18.14/11.27, Jun-26 21.32/24.15) — exact.
  Screener carries only ~12 quarters, so past Sep-2023 we are the deeper source, not the poorer one.
- **Still open in Sep-2019→date (13 cells):** BSE Ltd ×8 for 2019-09→2021-06 (NSE's archive doesn't reach
  back that far) and ×3 (Jun-24/Dec-24/Mar-25) where NSE publishes the row but its own nsearchives file
  404s on every host/path and retry; SUNDARMFIN Dec-2021 (both exchanges' links dead — Wayback untested,
  it was throttling); JBCHEPHARM Jun-2026 (no filing at either exchange yet).

### 22g. THE `mf` SLOT HEAL — 11,615 cells re-read from their own filings  (2026-08-07)
**Every SHP cell parsed before 2026-08-07 16:35 IST could carry `mf = 0.0` meaning "not found".**
`MEMBERS` mapped only `MutualFundsOrUTIMember`, but new-format filings spell the member BOTH ways and the
lowercase-ti `MutualFundsOrUtiMember` is what all BSE copies and every NSE filing before ~Jul-2025 carry
(§22f). `parse_shp` now falls back to the old key — this section is the residue: the cells already on disk,
which no re-parse of NEW filings ever reaches. fii/dii/prom/ins were never affected.

- **Repair = `scripts/heal_shp_mf.py` → ledger `scripts/shp_mf_heal.json.gz` → applied by
  `fetch_shareholding.apply_mf_heal_ledger()`** (inside every `refresh_quarters` and `--apply-ledgers`;
  idempotent, so CI re-heals itself if a reset-and-replay ever lands a stale history). The ledger patches
  **cell[3] and nothing else**: never creates a cell, never writes a zero, skips any cell whose mf is
  already set (a fresh parse always wins), and re-checks prom/fii/dii against the values the heal was
  measured on (±0.5pp) so a revision that landed since is left alone. Three sections: `heals` (patch +
  reference cell + `nse:<sub>` / `bse:<code>:<qtrid>` provenance), `zeros` (re-read, MF really is 0 —
  evidence, and it stops a resume re-fetching), `rejects` (parse or guard refused; never patched).
- **⚠️ THE FORMAT BOUNDARY FOLLOWS THE SUBMISSION DATE, NOT THE QUARTER.** §22b's "new format = quarters
  ≥ 2022-09-30" is when filers were *required* to switch; a company filing a Mar-2022 pattern in Nov-2024
  files it in the taxonomy current *then*. MANPASAND Mar-2022 (submitted 2024-11-04) came back
  `InstitutionsDomesticMember 5.38` with the MF row lowercase — stored mf 0.0, real 5.38. So the sweep ran
  the whole XBRL era (Jun-2016 →), not just the new-format quarters, and found 10 such cells pre-Sep-2022.
- **Two cuts keep it cheap and honest:** MF ⊆ DII, so `mf = 0 AND dii = 0` is provably correct and never
  fetched (**13,077 cells**); and quarters before Jun-2016 are *out of reach* rather than skipped — those
  cells come from Wayback-archived Moneycontrol pages and no XBRL exists anywhere (427 candidates).
- **Result: 24,613 cells re-read in ~17 min at 6 threads → 11,615 healed over 1,314 symbols, 12,724
  confirmed genuinely zero, 334 rejected, 0 transport errors.** Healed values: median 4.82%, mean 7.07%,
  max 47.38% (CROMPTON Mar-2025). By quarter: 922–1,165 per quarter across Sep-2022→Mar-2025, 10 cells
  pre-Sep-2022, and **zero from Jun-2025 on** — which independently dates NSE's switch to the uppercase
  spelling to the Jun-2025 filing season. Rejects: 161 no NSE filing and no BSE scripcode, 140 no filing at
  either exchange, 27 guard mismatches (the stored cell is a different filing from the one on offer —
  BFUTILITIE 2016-17 is 0.75pp off at BSE), 6 old-format parse refusals.
- **How it was verified — four independent ways, because "the number changed" is not evidence:**
  1. **Blast radius:** cell-by-cell diff of the whole history before/after — 11,615 slot-3 changes,
     **0 changes in any other slot**, 0 cells added or dropped, every change 0 → positive. Whole-file
     invariants after: 0 cells with `mf > dii`, 0 negative mf.
  2. **Cross-source:** 12 random NSE-sourced heals re-fetched from BSE's independent copy of the same
     filing — 12/12 identical to the cent (LICI 1.13, NAUKRI 10.61, HCLTECH 8.35, TVSMOTOR 15.47…).
  3. **Seam collapse:** |Δmf| across Mar-2025→Jun-2025 (last buggy quarter → first clean one) went from
     median **5.42pp → 0.20pp** (603 stocks moving >5pp → 16); Jun-2022→Sep-2022 from 4.09pp → 0.18pp.
  4. **Drift baseline:** healed Mar-2025 values sit a median **0.21pp** from each stock's independently
     parsed Jun-2025 value — statistically the same as the natural quarter-to-quarter MF drift (0.24pp
     over Jun→Sep-2025, no heals involved). At n=1,106 that is the strongest single check.
  Client-level: RELIANCE's stock page MF row went `0.00 | 0.00 | 0.00 | 9.32` → `8.03 | 9.14 | 9.21 | 9.32`;
  HDFCBANK's full series is continuous 2022-06 → 2026-06; shareholding.html MF/ΔMF render, 0 failed
  requests, 375px shows no sideways scroll.
- **The 8 cells the neighbour-continuity check flagged were all re-verified at BSE and all real** —
  PITTIENG/INDIGOPNTS/SBCL/STANLEY are genuine MF entries in Sep-2024 where **DII jumps by the same
  amount** (SBCL dii 2.33 → 20.02 with mf 1.53 → 18.80). Only GHCL Dec-2022 was odd, and it is a
  **pre-existing FII/DII defect, not a heal artifact** (next bullet).
- **★ FOUND IN PASSING — 314 cells were parsed from the WRONG FILING.** Some companies file more than one
  pattern for the same as-on date, and "newest submission per symbol wins" can pick a non-ordinary-equity
  one. GHCL Dec-2022 stored prom 61.35 / dii 38.65 / mf 32.92 from a filing with **29.6M shares and 58
  shareholders**; BSE's copy of the real pattern says dii 12.00 / mf 10.22 (identical 85% ratio — same
  categories, different share class). Detector (read-only, no network): per symbol take the median of its
  non-null shareholder counts (cell[6]) and flag cells under 5% of it — 314 cells, worst JPINFRATEC
  2024-09/12 (nsh **1** vs median 141,175), MAZDOCK 2020-09 (7 vs 308,372), DSKULKARNI (8, five quarters).
  All slots of those cells are wrong; fixing them is a separate job (ledger + a share-count scale gate in
  `parse_shp` so a wrong-class filing is refused at write time). Not done here.
- **Re-run:** `python3 -X utf8 scripts/heal_shp_mf.py` (resumable — skips everything already in the
  ledger), `--sample 60` for a smoke test spread over eras and routes, `--from-qe` to widen or narrow, then
  `python3 scripts/fetch_shareholding.py --apply-ledgers`. A dead NSE file (nsearchives drops old XBRLs)
  falls through to BSE automatically — that is how the last 6 transport errors were cleared.

### 22g. WRONG-SHARE-CLASS CELLS — the scale check, and why the MEDIAN rule is the wrong one  (2026-08-09)
**A filing can describe something that is not the company's ordinary equity** (a separate share class,
or a stub) and still pass every gate in `parse_shp` — the partition adds to 100, institutions stay under
public, the anchor resolves. All five slots are then wrong together. **Confirmed instances: 2.**

| symbol | quarter | stored (wrong) | truth (BSE) |
|---|---|---|---|
| GHCL | 2022-12-31 | prom 61.35 / fii 0 / dii 38.65 / mf 32.92, **58 holders, 29.67M shares** | prom 19.05 / fii 24.38 / dii 12.00 / mf 10.22, 94,479 holders, 95.59M shares |
| NDL | 2020-12-31 | prom 97.71 / fii 2.27, **18 holders** | prom 64.73 / fii 1.51, 28,682 holders, 48.05M shares |

- **⚠️ It is NOT "newest submission per symbol wins picked the wrong row".** Probed six quarters of the
  NSE master (2020-12 → 2024-09, ~1,900-2,090 symbols each): **only 0-3 symbols per quarter file more
  than one pattern for the same as-on date** (ARMANFIN, RKFORGE, DUCON, HARIOMPIPE, NUCLEUS, ABCAPITAL,
  GENSOL) and **none of them is affected**. GHCL has exactly ONE master row for Dec-2022 and that row's
  document is the wrong one — there is nothing to prefer, so the correction can only come from BSE.
  NDL is the one case where submission order mattered, and in the other direction: a **late REVISION**
  (submitted 2021-04-26, four months after the quarter) is the defective one and displaced a good
  original. Newest-wins is right for revisions in general; it just can't tell a good one from a bad one.
- **⚠️ THE MEDIAN RULE IS 99.3% FALSE POSITIVES — do not use it.** "flag cells whose holder count is
  under 5% of the symbol's own median" yields **302 cells / 87 symbols, of which 2 are defects**. A
  median is not a scale reference for a series with a trend, and these series trend hard:
  - **293 of 302 (97%) are growth ramps.** SME/Emerge names whose holder base grew 100-500× — AAKASH
    94 → 50,417, ATALREAL 112 → 17,008, DIL 203 → 72,429. The early quarters are genuinely tiny and the
    median is set by the late ones. Promoter % is flat across the whole ramp; nothing is wrong.
  - **The worst-looking ones are PRE-IPO patterns, and they are correct data.** MAZDOCK 2020-09 (7
    holders, prom 100.00), CLEAN 2021-06 (31), AVALON 2023-03 (19), UTIAMC 2020-09 (1,927) — the last
    pattern filed before listing. The big promoter "discontinuity" at the next quarter is the IPO.
  - Independent check: median holding-value discontinuity vs the nearest clean neighbours is **0.41pp
    for flagged cells vs 0.34pp for all cells** — statistically the flagged population is not disturbed.
- **The gate that works — `nsh_gate()` in fetch_shareholding.py: compare against the symbol's own
  EARLIER maximum holder count, never a median.** Refuse a filing whose `nsh` is under 5% of
  `max(nsh)` over quarters strictly before it. Trend-immune by construction: a growth ramp can't trip
  it (each value is near the running max) and a first-ever filing has no reference so pre-IPO patterns
  pass untouched. **Replayed over the full history: 2 rejections in 57,362 cells with a count (0.0035%)
  — exactly the two adjudicated defects, zero false positives.**
  - A quarantined filing **must not bank its share count either** — `shares_outstanding.json` feeds
    market cap (§22e), so GHCL's 29.67M would have produced a 3.2× low mcap. The gate runs before the
    share-ledger write for that reason.
  - Rejections land in `scripts/shp_quarantine.json` (symbol, quarter, reason, the parsed cell, the
    XBRL url) — **held for adjudication, never silently dropped.**
  - **Blind spot:** 13.6% of cells carry no `nsh` at all; no holder-count rule can see those. The
    orthogonal detector for them is a promoter-% V-shape (≥15pp away from both neighbours while the
    neighbours agree within 3pp) — 16 hits, 14 of them NOT nsh-visible (AYMSYNTEX 2023-06, DIACABS
    2023-09/12, ORCHPHARMA 2020-12, WIPRO 2016-06, VMART 2018-03, …). **Not yet adjudicated — open work.**
- **Corrections go through `scripts/shp_cell_fix.json`, never a direct edit** (CLAUDE.md rule 5):
  - `fix.<SYM>.<QE>` = `{cell, was, src, why}`. `was` records the wrong cell, so if the stored value is
    later neither the fix nor the recorded bad value, `apply_cell_fix` **warns and leaves it alone**
    instead of clobbering. It only ever overrides a cell that already exists — a correction ledger must
    never invent one (that bug was caught by the empty-history test; keep the test).
  - `accept.<SYM>.<QE>` = a holder-count collapse that is REAL, exempted from the gate so a `--reparse`
    can't punch a hole. Currently JPINFRATEC (post-resolution: equity extinguished, wholly owned by
    Suraksha — prom 100.00 / 1 holder is true and persists), DSKULKARNI and FEDDERELEC (post-insolvency
    reductions; holdings corroborated by later filings, **their nsh is doubtful but unverified**).
  - Applied in `load_hist()` so every reader gets it, and again **after** the fetch loop so a backfill
    can't re-poison a fixed cell. `python -X utf8 scripts/fetch_shareholding.py --apply-fix`
    materialises the ledger into shp_history.json with no network — needed because build_shp_backtest /
    build_stock_fin / build_compressed read that file straight off disk.
- **⚠️ BSE spells the mutual-fund member the OLD way inside NEW-format filings** —
  `MutualFundsOrUtiMember` (lowercase "ti") where NSE writes `MutualFundsOrUTIMember`. `MEMBERS` maps
  the two to different slots (`mf` vs `o_mf`) and the new-format branch only reads `mf`, so **every
  BSE-sourced post-Sep-2022 filing silently returned mf = 0.00** (GHCL's real 10.22 → 0). parse_shp now
  falls back `mf ← o_mf`. Latent until now — the tracked BSE ledgers are all pre-Sep-2022 old-format —
  but it would have bitten the moment any post-2022 quarter was filled from BSE, which is exactly what
  this fix does. Check this first if a BSE-sourced fill comes back with a suspiciously round mf.

### 22i. ★★★ THE SWALLOWED FOREIGN BLOCK — 162 stored `fii = 0.0` cells healed  (2026-08-12)
**The §22f round-4 "Jun-2016 fabrications" were not fabricated by BSE. We wrote them.** A filing
in this era can tag its ENTIRE foreign-institution block `OtherInstitutionsMember` (BSE's own aspx
renders it as an unlabelled **"Any Others (Specify)"** row inside the institutions block).
`parse_shp`'s old-format branch reads fii from `InstitutionsForeignPortfolioInvestorMember` + FVCI
only, and `OLD_OTHER_TO_DII = True` sends the block to **dii** — so `fii = 0.00` and the whole
holding sits one slot over. **`fii + dii` is right; only the SPLIT is wrong**, which is exactly why
no reconciliation gate, partition check or inst-recon ever fired on 162 defective cells.

- **Screen (use this one, not the edge screen):** every maximal run of consecutive stored
  `fii == 0.0` cells whose nearest stored anchor within 2 quarters of either edge holds >1pp —
  **1,152 cells over 266 runs**. An edge-only screen (`0.0` beside a >1% neighbour) sees only
  **253**, because the defect persists as long as the filer keeps the habit: CANFINHOME ran 25
  quarters, EIHOTEL 18, APLAPOLLO 16. **Run-expand before you count.**
- **Route:** BSE's own copy of each filing (`SHPQNewFormat` → `XbrlFile`) re-read with today's
  `parse_shp` (1,126 fetches, 131 s at 6 threads) **plus BSE's rendered `ShareholdingPattern.aspx`
  for the SAME filing**, whose ROW LABELS and **HOLDER COUNTS** carry what the XBRL tag threw away.
  Per-cell journal with evidence: **`scripts/_shp_zero_fii_audit.json`** (heals / zero-confirmed /
  open). Heals land in `shp_cell_fix.json`, provenance `bsexbrl:<scripcode>:<file>`.
- **★★★ THE ARBITER — THE ROW-IDENTITY PROOF.** When a symbol's *itemised* quarters also show a
  genuine domestic "other institutions" holding, "is the swallowed block wholly foreign?" is a real
  question and the percentages cannot answer it. The answer: find a **same-regime** quarter (foreign
  row empty, block on Any-Others) where shp_history holds an **independently sourced** fii, and
  compare it against that quarter's Any-Others row. SUPREMEIND Dec-15 renders Any-Others **20.74**
  == the Wayback-MC-derived stored fii **20.74**; MRF **8.58 == 8.58**; NITINFIRE **11.19 ==
  11.19**; KSCL 16.42 vs 16.43. **9 of 11 disputed symbols proved, 0 contradicted.**
  - **⚠️ THE REGIME GATE IS LOAD-BEARING.** Once a filer starts itemising the foreign row, its
    Any-Others row means something else entirely. Counting those quarters as evidence produced 4-6
    "contradictions" per symbol and turned every verdict into HOLD. **A quarter can only speak for
    another quarter rendered the same way.**
  - **⚠️ A HOLDER-COUNT HEURISTIC GOT THE HARD CELLS WRONG — the lesson worth keeping.** Comparing
    the disputed Any-Others holder count against the next itemised quarter's FPI-vs-other split
    said "mixed" for SUPREMEIND (132 holders ≈ 91 FPI + 50 other) and would have written **9.31
    against a true 22.03**; NITINFIRE 4.68 against 9.48. It was reading a LATER regime's residue
    backwards onto an earlier one. It had passed three validations first — leave-one-out residue
    error median **0.000pp**, healed-vs-nearest-as-filed median **0.18pp**, identity preserved on
    161/162 — and was still wrong exactly where it mattered. **A statistic that fits the 141 easy
    cells does not license the 20 hard ones; find evidence that speaks to the hard ones.**
- **Cross-validation, the strongest evidence in this campaign:** all **14** healed Jun-2016 cells
  corroborate the completely independent §22f Mar-2016 seam derivation (aspx institutions-identity
  inversion — different quarter, different arithmetic, no shared code or input): RATNAMANI **12.04
  vs 12.04** and MRF **8.49 vs 8.47** to the cent, median ≈0.5pp, every one inside the 3.0pp gate.
- **Result: 162 heals / 33 symbols, 570 zeros CONFIRMED as-filed, 423 held OPEN.** The heals add no
  cells — they correct existing ones — so **a coverage audit cannot see this defect class at all**.
- **The zero-confirmations were audited, not assumed:** all 570 re-read from their own cached XBRL
  hunting a nonzero foreign-ish member. 44 hits, **all** either foreign *promoters* (`ForeignMember`
  is the promoter block's Indian/Foreign split — ASIANHOTNR 50.53) or non-institutional foreign
  public (`ForeignCompaniesMember`, `OtherForeignShareholders`, `NonResidentIndividuals…`), neither
  of which is FII by our definition. The one institutional-looking hit, INDOSTAR Jun-2020
  `ForeignPortfolioInvestorMember` 1.88, is a **promoter-group** FPI (1.88 + 43.62 = 45.50 = the
  promoter total exactly). **0 false zeros.**
- **PRE-2016 IS OPEN, NOT CLOSED — 305 cells, and the pages cannot decide.** All 305 re-read from
  both aspx flags: **178 `row_absent_closes`** (foreign row missing AND the block's own subtotal
  closes without it) + **127 `printed_zero`**. Such a page is self-consistent whether the truth is
  0 or 10.94, so **zero heals were written** — logged `not-found-via:bseaspx` (§57 rule 2).
  GEOMETRIC Mar-2003 is the type specimen: stored 0.00 sitting between 0.00 and 10.94.
- **Still open (journalled, none closed):** 80 cells / 14 NSE-SME symbols with no BSE scripcode
  (AKG, MOKSH, NBIFIN, SRPL…), 15 no-BSE-row, 7 CAPTRUST (the filer classifies the same 17.27 block
  FPI-explicit-zero in Dec-21 and Foreign in Dec-22 — per-quarter truth unknowable), 5
  identity-unproven (NESCO; SHANTIGEAR ×4, whose render and own XBRL disagree on the split by up to
  3pp), LICHSGFIN Jun-16 + RUCHISOYA ×3 + LMW Dec-16 unanchored, 1 prom-drift, 1 negative residual,
  plus the 305 pre-2016.
- **★ RE-RUN EVERY GATE THAT CONSUMED THE BAD VALUE.** The 69 q89 cells round 4 held were held
  against these very anchors: re-running `seam_derive.py` after the heal wrote **+15 cells,
  Mar-2016 85.0% → 88.0%, whole-sample 96.0% → 96.4%** — on exactly the healed cohort, nothing
  forced. The other 54 are genuine derivation-vs-anchor disagreements. **A heal is not finished
  until the gates that rejected work because of it have been re-run.**
- **Three bugs that made `seam_derive.py` un-re-runnable, all fixed:** it read two UNTRACKED scratch
  files (now falls back to `_shp_aspx_resolved_era_syms.json` / `_bse_master_all.json`); it dumped
  its ledger WHOLE while a re-run's frontier only holds cells history still LACKS, so a second run
  would have silently shrunk the tracked ledger **159 → 15** (now a UNION, existing cells win — the
  §22f stage-order trap in a new costume); and its Dec-15 batch gate `sys.exit`ed at `n < 30`, which
  on a re-run means *unmeasurable*, not *failed*, and took the per-cell-corroborated q89 cells down
  with it (now falls back to per-cell; a MEASURED failure still stops the run).
- **Verified LIVE through the client feed** (§41): 162/162 heals and 15/15 new seam cells present in
  `docs/shp_engine.json` served from the origin — ASIANPAINT Jun-16 `[18.79, 7.46]`, SUPREMEIND
  `[22.03, 6.93]`, MARKSANS `[13.62, 0.16]`, ASIANPAINT Mar-16 `[18.01, 9.06]`. Applied twice;
  second pass changes 0 cells.
- **⚠️ NOT SHAPE-SPECIFIC TO fii, and NOT covered here:** the same screen pointed at the **dii**
  slot returns **1,313 run cells / 200 edge cells** on the healed history (measured 2026-08-12,
  post-heal), of which only **1** is a cell this campaign touched — so ~199 edge cells are a
  pre-existing, unexamined population, concentrated 2020-2026 (24-35 per year). **Open work.**
  Whoever takes it: a `dii = 0` is far likelier to be genuine than a `fii = 0` (small caps really
  do have no domestic institutions), so expect a much lower hit rate and gate on the same
  row-identity evidence, never on the screen alone.
- **⚠️ AND THE RESIDUAL fii SCREEN IS STILL 1,006 RUN CELLS**, because a heal creates new anchors:
  healed cells become >1% neighbours, so zero-runs that previously had no qualifying anchor now
  qualify (+16 net after removing the 162). The screen is a THREAD, not a defect count (§78).

### 22h. ★★ VERIFICATION vs EXTERNAL SITES + THE OTHER EXCHANGE  (campaign 2026-08-09)
Full write-up: `scripts/SHP_VERIFY_REPORT.md`; plan `SHP_VERIFY_CAMPAIGN.md`; per-phase findings
`SHP_VERIFY_P1/P2/P5_FINDINGS.md`. Tooling (all committed, self-tested): `shp_verify_prov.py`
(per-cell route map), `shp_verify_mapcard.py` (derives a site's mapping ARITHMETICALLY),
`shp_verify_diff.py`, `shp_verify_quorum.py`, `shp_verify_arbitrate.py`.

- **RESULT: no value we publish has been shown wrong.** Cross-exchange (our NSE-derived cells vs
  BSE's *separately filed* documents, 61 syms x 41 qtrs, Jun-2016->Jun-2026): **2,990 MATCH, 1
  ROUND, 0 MISMATCH** — the lone non-exact value is CUMMINSIND Jun-2022 nsh, off by ONE
  shareholder. Three sites x 66 stratified symbols: 2,156 CONFIRMED, **0** cells contradicted,
  **0** cells where no source agrees with us. 79 individual filings arbitrated: 0 defects.
- **★ NO SITE HAS PRE-2010 DATA (7/7).** Trendlyne bottoms out Dec-2015, Screener Mar-2017,
  everyone else 4-9 trailing quarters. Our 2002-2010 (0%) and 2010-2015 (30%) eras cannot be
  corroborated by any aggregator — **cross-EXCHANGE is the only real check for the deep era**, and
  2010-2015 (sourced from archived Moneycontrol) is UNVERIFIABLE BY DESIGN, not "fine".
- **★ NEVER map a site's field by its NAME.** Groww publishes `otherDomesticInstitutions.insurance`
  which is *all non-MF domestic holdings*, not insurance; mapping by name invents ~1.9pp of defect
  on every stock. Derive the mapping arithmetically (which subset SUMS to ours) and refuse when
  nothing fits. Screener's DII is simply not our DII (62% hold) — it must not vote on DII.
- **★ SITES CARRY REAL ERRORS.** Screener FII ~2.7% miss rate: ICICIBANK Jun-2026 it says 33.79 vs
  the filing's 49.82; HINDALCO Jun-2026 31.41 vs 35.60. Moneycontrol maps to NOTHING. Never heal
  from a single site — campaign rule 6b (user mandate): a value is taken only when the exchange
  filing AND >=2 independent sites agree; sites our data came FROM never count toward quorum.
- **★★ BUT UNANIMOUS SITES-vs-US = CHECK THE OTHER EXCHANGE FOR A REVISION FIRST (35 real defects,
  2026-08-09).** Companies file then REVISE; BSE carries the revision (`revised_date_time` on its
  SHPQNewFormat row) while NSE's master keeps serving the ORIGINAL — so "ours == the NSE filing"
  is CIRCULAR when ours came from that same document. 103 unanimity cells re-adjudicated vs BSE's
  copy: 50 we were right, **35 our value was superseded (51 field-values, healed via
  shp_cell_fix.json)**. LCCINFOTEC Jun-2025 prom 0.0->45.85 was the flagship "sites wrong" example
  and was actually OURS wrong. Detector: `shp_verify_revcheck.py`. OPEN: the daily pipeline needs a
  periodic BSE revised_date_time sweep — revisions filed only to BSE are invisible to the NSE
  master top-up.
  **Carve-out — the NSE-ONLY cohort (BSE Ltd, CDSL, ~104 symbols §22e) is STRUCTURALLY
  UNVERIFIABLE by this check**: an exchange cannot list on itself, so no BSE copy of their filings
  exists, ever. Their cells are not "unchecked", they are uncheckable by the cross-exchange route —
  say so wherever their values are cited. Per-route exposure audit (coverage session, 2026-08-09):
  the 2016-19 BSE ledger route is CLEAN by construction (row_for() sorts by revised-else-filing
  date and takes the LAST, so it already picks revisions — spot-checked HCL-INSYS Sep-2018,
  NILKAMAL Mar-2018); the NSE-gap fills and Screener-anchored BSE Ltd cells inherit the blind spot
  (NSE public_val is itself the original-document figure, so tying to it proves nothing); the
  Wayback-MC era has NO revision oracle at all — an archived page is a snapshot, and BSE's pre-2016
  surface is measured empty — recorded as a stated limitation on that ledger, not as verified.
- **★ IDENTITY TRAPS AT SCALE.** Tickertape's sid `TRU` is Trust Fintech, unrelated to our TRU
  (renamed Dhanvarsha, sid DHA); a StockEdge ticker shortcut matched `IEL` to the wrong company.
  Exact-ticker match, else unambiguous full name, else SKIP — a wrong company becomes a fake defect.
- **★ OPEN GAP — shareholder counts: 9,094 cells (13.7%) have no `nsh`, quarter-shaped.** 99.9% in
  every quarter from Sep-2019 EXCEPT Sep-2022 (1.1%), Jun-2024 (0.3%), Mar-2024 (78%),
  Sep-2025..Mar-2026 (~93%). The filings HAVE it and today's parser reads it (6/6 on both blackout
  quarters; RELIANCE Sep-2022 = 3,485,825, HDFCBANK Jun-2024 = 3,664,325, both stored empty).
  Cause: populated before nsh extraction existed, missed by the 8-quarter --reparse of 2026-07-16.
  **Fix = `--reparse --quarters 2022-09-30,2024-03-31,2024-06-30,2025-09-30,2025-12-31,2026-03-31,2026-06-30`
  via the §22b staging file** (~12k fetches; never alongside other sweeps, never as a 2nd writer
  against the 12:40/20:40 CI job).
- **★ THE 2,344 INTERNAL HOLES HAVE NO DIAGNOSED CAUSE.** The null-`filing_date_time` theory was
  measured on 589 BSE rows: nulls are **4.4x enriched** on holes (8/51 = 15.7%) vs dated rows
  (19/538 = 3.5%) — a real signal, but NOT the cause: 43 of 51 nulls sit on cells we hold fine and
  19 holes have a good date. It explains under a third. Diagnose; don't inherit the guess.
- Analysing with a LOCAL checkout produced a phantom parser bug (a copy 227 lines behind
  origin/main). `feedback-analyze-live-not-local-bin` applies to CODE, not just .bin/.json.

### 22c. FII/DII ACCUMULATION BACKTEST  (CHAT-DRIVEN — the on-page section was REMOVED)
**⚠️ 2026-07-16: the user removed the backtest UI from shareholding.html ("I'll perform backtest in
chat") — do NOT re-add the section.** What remains: `scripts/build_shp_backtest.py` (kept, run on
demand for chat experiments — merge live sf parts per §7.0 first, add/edit VARIANTS tuples, read the
printed CAGR table; docs/shp_backtest.json is NOT committed/published anymore, its feeds.json entry
and the workflow's evening rebuild step were removed), and the 🔥×N FII-streak badges in the main
table (still live, calendar-adjacent raises). The findings below stand — cite them before re-running.
**The user's hypothesis ("stocks where FII raises stake every quarter keep rising") tested properly.**
- **Builder `scripts/build_shp_backtest.py`** (SF_BIN env = fresh survivorship-free bin; local dev merges
  the live sf-data parts per §7.0). Point-in-time rules: universe = Nifty 500 members as of each rebalance
  (`_n500_master_history.json` nearest-prior snapshot; filing-time tickers → bin keys via `_rename_map.json`
  transitively); a quarter's cell counts only if its ACTUAL submission date ≤ rebalance day; rebalance =
  first trading day ≥ QE+22d (SEBI deadline 21d); signal = ΔFII (or ΔDII/both) ≥ +0.05pp in EACH of K
  consecutive CALENDAR quarters (gaps break streaks — never "previous available quarter") + latest stake
  ≥1% (cut-variants: the PRE-streak stake ≥1%); equal-weight, rotate quarterly, delisted exits at last
  close, no costs, price-only both sides (bench = docs/nifty500.json). 6 precomputed variants:
  fii2/fii3/fii2top20/dii2/both2/fiicut2 + a "next rebalance (forming)" preview from the in-progress
  quarter's filings-so-far. Output includes daily NAV curves, per-rebalance picks w/ forward returns.
- **Page:** backtest section at the bottom of shareholding.html (variant chips w/ CAGR, stat cards, SVG
  equity curve w/ crosshair tooltip + rebalance dots, next-rebalance preview chips, expandable
  per-rebalance picks table). Main table rows get a 🔥×N badge for live FII streaks (calendar-adjacent).
- **Refresh:** evening refresh-shareholding.yml run (cron moved 15:10→16:10 UTC = 21:40 IST, AFTER the
  20:45 IST price publish) downloads the `data` release asset and rebuilds shp_backtest.json; the morning
  run skips it (step `if: github.event.schedule == '10 16 * * *' || github.event_name != 'schedule'`).
  Feed monitored (max_age 100h — weekend runs produce no commit when prices didn't move).
- **VERDICT (as of 2026-07-16, Jul-2020→date, don't oversell the signal):** FII-raising-2q 24.0% CAGR,
  DII 21.3%, both 21.2%, FII-3q 17.6%, **FII-CUTTING (control) 24.4%**, every-N500-equal-weight baseline
  **23.8%**, Nifty 500 (cap-wt) 17.1%. So the entire "outperformance" = the equal-weight/size effect;
  the FII direction adds ~nothing at quarterly granularity with a 3-week filing lag (signal ≈ control ≈
  baseline), and longer streaks HURT. The `ewall` baseline chip exists precisely so the page says this
  itself — keep it when adding variants.

### 22d. FII/DII FACTORS IN THE STRATEGY BUILDER  (added 2026-07-16)
**`fiiPct` / `fiiChgPp` / `diiPct` / `diiChgPp`** are sort+filter factors in BOTH engines
(stock-backtest.html self-contained + backtest-engine.js shared — the §"engines-sync" checklist was
followed: FIELDS + SHP_FIELDS/needsShp + factorsAt block + loadShp in run()/loadEngineData/both bake
paths + SORTL labels in saved-strategies.html). Data = **`docs/shp_engine.json`**
(`{SYM:[[qeInt,fii,dii,subInt],…]}`, ALL quarters, built by fetch_shareholding.build_engine_feed,
committed by refresh-shareholding.yml, ~1.6 MB raw, lazy-loaded only when an SHP factor is used).
Point-in-time semantics in `shpAt()`: latest quarter with **subInt ≤ as-of date**; QoQ change only vs
the CALENDAR-previous quarter whose own sub ≤ date (gaps/late-filers → null), and **never across the
Sep-2022 format boundary** (cur qe 20220930 → change null; §22b). Renamed tickers: loadShp merges the
old-name filings into the current key via FUND_ALIAS (filings were made under the name of the day).
History starts Sep-2019 → fii/diiChgPp usable from ~Dec-2019 rebalances; before that the factor is
null and stocks drop out of SHP-sorted screens (correct, not a bug).

---

## 23. BULK & BLOCK DEALS  (docs/deals.html — "Bulk/Block Deals" nav, built 2026-07-16, SELF-UPDATING)
<!-- renumbered from 22 (two sections were both born §22 the same day; FII/DII holdings kept it) -->
**The smart-money tape:** every NSE bulk deal (>0.5% of equity traded by one client in a day) and block
deal (negotiated block-window trades), rolling ~92-day window, with a per-stock net-buying view + two
Discovery buckets ("Smart money (bulk & block deals)" group, type `deal`).

- **Fetcher:** `scripts/fetch_deals.py` → `docs/deals.json`
  (`rows:[[date,kind B|K,sym,name,client,side B|S,qty,price],...]`; value ₹cr = qty·price/1e7 client-side).
  Sources, each latest-day-only and non-fatal: (1)+(2) `nsearchives.../content/equities/bulk.csv` +
  `block.csv` (plain UA; block.csv says "NO RECORDS" on no-block days), (3)
  `/api/snapshot-capital-market-largedeal` via the announcements cron's urllib+cookie-warmup session
  (`build_fundamentals` `_get`/`nse_jar`; its BLOCK section can carry an older date than bulk — merge by
  date handles it), (4) opportunistic `/api/historical/bulk-deals`+`block-deals` via curl_cffi.
  **⚠️ The historical API 503s even with TLS impersonation (Akamai, checked 2026-07-16)** — so there is NO
  deep backfill; the window GROWS ORGANICALLY one trading day at a time from the seed (2026-07-14/15).
  Don't burn time re-attempting the historical route; the attempt is already wired in and self-heals if
  NSE ever unblocks it. Dedup key = (date,kind,sym,CLIENT,side,qty,price) — company NAME is excluded
  (CSV says "India Tourism Development…", API says "India Tour. Dev. Co."; CSV parsed first so its name wins).
- **Never-shrink guard:** refuses to write if the merge lost >40% of existing rows; exits 1 (red run) only
  when EVERY source failed.
- **Refresh:** `.github/workflows/refresh-deals.yml` — 21:30 IST weekdays (deals publish ~18:00-18:30 IST)
  **+ 08:45 IST Tue-Sat catch-up**: the archives CSVs keep serving the PREVIOUS trading day until the next
  evening, so a missed evening cron self-heals next morning. Commit = reset-and-replay carrying deals.json
  (§18 gotcha). Push-path self-test on the fetcher/workflow. Feed in feeds.json (max_age 110h).
- **Discovery wiring:** `build_discovery.py build_deal_buckets()` (deal7/deal30, buys ≥₹5 cr, net = buys −
  sells, top-100); returns [] if deals.json is missing so the group just disappears — discovery.html has the
  matching `deal` HEADS/rowHtml branch + 🐋 GROUP_ICON.
- **Page:** deals.html — stat cards (latest day), All-deals/By-stock views, filters (search stock OR client —
  clicking a client name filters to them, kind, side, min value, period), theme.js auto-cardify on mobile.
  New page = sw.js SHELL entry + CACHE bump (rode v26→v27 with the shareholding page).

---

## 24. INSIDER TRADES  (docs/insider.html — "Insider Trades" nav, built 2026-07-16, SELF-UPDATING)
**SEBI PIT Reg 7(2) disclosures** — promoters/directors/KMP buying-selling their own company — rolling
~92-day `docs/insider.json` + a "Promoters buying (30d)" Discovery bucket (reuses the `deal` row type).

- **⚠️ THE API LANDSCAPE (verified 2026-07-16 — don't re-derive):**
  - **PIT moved to XBRL submissions in Apr-2026** ("PIT V2.0 (30-04-2026)" in the XML header). The
    legacy dataset behind `/api/corporates-pit` FROZE ~2026-04-21: its market-wide query returns
    `{"data":[]}` for EVERY variant (urllib session, curl_cffi impersonation, csv=true, param renames,
    date formats, no-params, sme); its symbol query returns only the company's pre-freeze latest ≤20
    (newest-first, `pageno` ignored, adding dates zeroes it). Useless for current data.
  - **The live source is `/api/corporates-pit-gg?index=equities`** (found in the page bundle
    `/dist/js/sections/corporate-filings.js`) — a filing INDEX over roughly the trailing ~75 days:
    appId, symbol, broadcastDateTime, xmlFileName (the XBRL on nsearchives), typeOfSubmission,
    prevAppId. **No transaction numbers** — those live in each filing's XBRL. Its own from_date/
    to_date params misbehave → always take the full default index and dedup.
  - Neighbouring endpoints in the same bundle if ever wanted: `corporate-sast-reg29`,
    `corporate-pledgedata-sast3132`, `corporate-IT-PIT-Annual`, `TradingPlandata`,
    `OffmarketTranctiondata` (sic), `corporates-corporateActions?index=equities`.
- **Fetcher:** `scripts/fetch_insider.py` — pulls the -gg index, fetches ONLY new filings' XBRLs
  (meta.seen appIds; failures retry next run; revised filings replace the original's rows via
  prevAppId), parses each `Disclosure<N>` context (taxonomy `in-bse-co`; note the tag typo
  `SecuritiesHeldPostAcquistion…` — match loosely). Equity-share rows only.
  Row: [bcast date, sym, company, person, cat P/D/K/O, side B/S/P/R/I/O, qty, val ₹ (RUPEES — page
  divides by 1e7), mode MP/MS/ES/OM/PF/RI/GF/PL/RV/IV/BB/OT, pctPost, key `g<appId>#<ctx>`].
  Seed 2026-07-16 also merged a one-time legacy-API sweep for the pre-May sliver (legacy `did` keys).
- **Refresh:** `.github/workflows/refresh-insider.yml` — 21:45 IST weekdays + 09:00 IST Tue-Sat
  (index + a few dozen XBRLs ≈ 2 min). Commit = reset-and-replay carrying insider.json (§18 gotcha).
- **Discovery:** `build_insider_buckets()` in build_discovery.py — "Promoters buying (last 30d)": cat P,
  mode MP/MS ONLY (ESOP/off-market/pledges excluded to keep the signal clean), buys ≥₹25 lakh, net =
  MP − MS, type `deal` rows (shared renderer). Group renamed "Smart money (deals & insiders)" —
  the GROUP_ICON key in discovery.html must match the group string exactly.
- **Page:** insider.html — defaults to **Promoters + market deals only + last 30d** (the signal view);
  filters open it up to everyone/all modes. Person name click-through, By-stock net view, cards for the
  latest broadcast day. sw.js SHELL + CACHE v28→v29.

---

## 25. NEW-LISTING (IPO) YEAR-AGO BASE BACKFILL  (built 2026-07-16, SELF-UPDATING)
Every IPO / NSE-migration / demerger listing enters sf_fundamentals.json with quarters but NO
year-ago (and often no preceding) quarters — NSE XBRLs carry ONLY the current period — so
**profit-growth YoY% is blank** until the bases exist. The bases are public anyway: every results
PDF prints them as COMPARATIVE COLUMNS (3m preceding / 3m year-ago / YTD / FY — the columns
Trendlyne reads). `scripts/backfill_ipo_bases.py` automates the fill:
- **Candidates:** symbols whose EARLIEST stored quarter is within ~16 months (`--days 480`), i.e.
  new-to-data names (IPO or migration — both have the same gap). Insurers excluded (§3 owns them).
  A stock with NO stored quarters yet is NOT a candidate — there is nothing to anchor against; it
  becomes fillable the day its first filing lands (update_fundamentals catches that filing).
- **Sources per target cell:** the missing quarter T is read from the filing PDF of T+1q (its
  "preceding 3 months" column) or T+4q (its "year-ago 3 months" column). PDFs: integrated-filing
  `pdf_attach` when non-null, else the corporate-announcements attachment (index=equities, then
  =sme for pre-migration SME filings), result-filtered, identity-guarded (company name must appear
  in the PDF text).
- **ANCHOR (never guesses):** text layer (PyMuPDF) → parse header DATES (incl. bare `31032026`) →
  map the "Profit after tax"/owners and "Revenue from operations" row numbers to columns by
  x-position → accept ONLY when, under one unit scale (cr/million/lakh/thousand/rupees), the
  CURRENT column == our stored XBRL value (and the PRECEDING column too when stored) within
  max(3%, ₹2cr). First-occurrence rule picks the 3m column when the same date also heads a YTD/FY
  column. Scanned/garbled filings (DTIL, ONIDA style OCR junk `(@,736)`) fail the anchor → FREE
  Gemini vision (`gemini_vision.read_corp_results`, GEMINI_API_KEY) reads cur/prec/yago std+con —
  then the SAME cur-anchor gate applies before anything is written.
- **Writes (fill-only):** PAT → docs/sf_fundamentals.json + scripts/fundamentals.json (ann=None →
  `fill_ann_dates.py` stamps the SEBI deadline in the next workflow step); revenue + PAT mirror →
  docs/sf_revop.json + scripts/revop_fundamentals.json. Ledger `scripts/ipo_base_fills.json`
  records every cell (src PDF, source quarter, via text/gemini) — **after any full rebuild run
  `--reapply`** to re-assert the ledger. Failures in `scripts/_ipo_base_skips.json` (cap 5
  attempts; `why:"vision"` entries retry whenever a GEMINI_API_KEY is present).
- **Cron:** wired into refresh-fundamentals.yml (nightly 21:15 IST gate, same as insurers) with
  `--limit 40` per run — the 2026-07 first-time backlog (~200 recent listings, 711 cells) drains
  over ~5 nights; steady state is a trickle. Commit step carries ipo_base_fills.json +
  _ipo_base_skips.json (reset-and-replay §18 gotcha); the big scripts/ mirrors are NOT committed
  from CI (same convention as the insurer filler).
- **Local:** `python -X utf8 scripts/backfill_ipo_bases.py --dry-run --only SYM1,SYM2` to preview;
  `--only` bypasses the recency cutoff. No local GEMINI key → scanned filings queue as
  `why:"vision"` and drain in CI.
- **⚠️ Column-mapping failure class (caught 2026-07-16, day one):** when a header DATE fails to
  parse (OCR'd `31032025`, split tokens), "first occurrence of the year-ago date" can land on the
  **FY column** — VIKRAMSOLR prec got 139.1 (FY) instead of 82.84; AEPL yago got the ANNUAL 7.68.
  The cur-anchor does NOT protect comparatives. TWO guards: (1) `_map_columns` — any figure cell
  inside the column zone that maps to no header date REJECTS the row; (2) `columns_for` ORDERING —
  some filings MISPRINT the preceding-quarter header date (VIKRAMSOLR's col2 literally says
  30.06.2025 twice), sending "first occurrence" to the FY column even when every cell maps; so
  structurally require cur < prec < yago ≤ cur+2 (the 3m block), else drop that column. This is
  deliberately strict — messy-but-correct pages also get rejected/reverted and re-fill via
  Gemini. After ANY parser change run **`--audit`**: re-extracts every text-sourced ledger cell
  from its recorded src PDF and fixes/reverts mismatches (circular-anchor-aware: won't anchor on a
  preceding value the script itself wrote). ⚠️ Cross-session gotcha: another session's `git commit`
  can sweep your pre-audit staged data to origin — after any conflicted rebase, `--reapply` alone
  is NOT enough (fill-only can't blank); re-run `--audit` or scrub reverted values explicitly.
- **Structural residuals (don't chase):** quarters NO filing ever printed (company listed too
  recently — the next filing carries them a quarter later); BSE-only listings (§17 OCR grind owns
  those); operating-profit bases (rev+PAT cover the site's YoY; op needs a 4-row derivation —
  extend only if the Results Season chart ever needs a new IPO pre-N500-entry).

---

## 26. DELIVERY SPIKES  (docs/delivery.html — "Delivery Spikes" nav, built 2026-07-16, SELF-UPDATING)
**Conviction-accumulation screen:** stocks whose DELIVERED quantity jumped to ≥3× their own 20-session
median with delivery ≥30% of volume, ≥₹1 cr delivered and price up ≥1% — computed daily for the whole
mainboard (mcap ≥₹100 cr), plus a "Delivery spikes (last 5 sessions)" Discovery bucket (type `spike`,
"Smart money" group — the group was RENAMED from "Smart money (deals & insiders)" when this 3rd tape
joined; the GROUP_ICON key in discovery.html must match the group string exactly).

- **Source:** `nsearchives.../products/content/sec_bhavdata_full_DDMMYYYY.csv` — ONE FILE PER DATE
  (~19:00 IST trading days; plain UA). Dated URLs = the pipeline SELF-HEALS: each run walks forward
  from the last stored session and fetches whatever is missing (holidays skip). cols incl. CLOSE_PRICE,
  DELIV_QTY, DELIV_PER (space-padded; '-' when N/A); SERIES filter 'EQ'.
- **Fetcher:** `scripts/fetch_delivery.py` → TWO files (both must ride the commit step, §18):
  `docs/delivery_hist.json` (state: last 45 sessions × ~2.5k stocks [close,dq,dpct]; ~2.3 MB) and
  `docs/delivery.json` (page feed: `spikes` rows [date,sym,name,close,chg%,delivCr,dpct,avg20pct,qmult]
  + `today` top-400 by delivered value; ~165 KB). Names/mcap from dash_slim meta —
  **⚠️ slim meta is keyed 'RELIANCE.NS' (Yahoo suffix), re-key by bare symbol or every lookup misses**
  (this bug made spikes silently empty on first run). Prices are UNADJUSTED closes — corp-action days
  self-exclude via the price-up filter. First run with no state seeds ~45 sessions (~60 fetches).
- **Refresh:** `.github/workflows/refresh-delivery.yml` — 21:15 IST weekdays + 08:35 IST Tue-Sat
  catch-up. Both jsons in feeds.json (delivery_hist monitored with pages:[]).
- **Sanity anchors (2026-07-16 seed):** ~41 spikes/session; that day: DIXON ₹1,322 cr at 5.9×,
  RKFORGE 49.8×; repeat-stocks view topped by BUILDPRO (5 spikes/10 sessions).
- **Page:** 3 views (Spikes / Repeat stocks / Biggest today), filters (value, multiple, sessions,
  search), sw.js SHELL + CACHE v29→v30.

---

## 27. IPOs & LISTINGS  (docs/ipos.html — "IPOs & Listings" nav, built 2026-07-16, SELF-UPDATING)
**The primary-market calendar:** issues open now (with LIVE subscription multiples), upcoming issues,
and the last 6 months of listings with performance vs issue price.

- **Fetcher:** `scripts/fetch_ipos.py` → `docs/ipos.json` (~6 KB) — STATELESS full-snapshot rebuild
  each run from three NSE APIs (urllib+jar session): `all-upcoming-issues?category=ipo`,
  `ipo-current-issue` (adds `noOfTime` = subscription ×, category Total), `public-past-issues`
  (~1.4k-row archive). Current px + mcap for listed names joined from dash_slim meta
  (⚠️ '.NS'-key gotcha, §26; a 1-2 day price lag for brand-new listings is normal — shows "—").
- **securityType filter:** keep EQ + **BE (new mainboard names often list in the T2T series)** + SME;
  drop N0/Z9/RR/DEBT/IV (NCD/REIT/InvIT tranches — they'd pollute the table as phantom "Main" rows).
  Board chip: SME vs Main. SME names aren't in the site price universe (issue details only).
- **Refresh:** `.github/workflows/refresh-ipos.yml` — 08:40 IST Mon-Sat (new announcements +
  yesterday's listings) + 18:40 IST weekdays (subscription top-up). Guard note: the upcoming list
  legitimately shrinks to ~0 in dry weeks (min_bytes 2000, min_ratio 0.5 in feeds.json).
- **Downstream hook:** ipos.json `listed` is a clean machine-readable new-listings ledger — the §25
  IPO year-ago-base backfill (and any future "flag recent-to-NSE names" tooling) can consume it
  instead of re-scraping.
- **Page:** issue cards (subscription progress bar) + listings table (board filter, best/worst since
  issue). sw.js SHELL + CACHE v30→v31.

---

## 28. INDEX MONTHLY RETURNS  (docs/monthly-returns.html — "Monthly Returns" nav, built 2026-07-16, SELF-UPDATING)
**The heatmap page:** month-by-month % returns for 32 NSE indices (10 broad / 16 sectoral / 6 thematic)
with the year's return in the last column — plus the extras: FY (Apr–Mar) view, "vs Nifty 50" alpha
mode, per-index seasonality (avg + hit-rate per calendar month), live MTD cell, sortable columns.
THREE views (chips): **All indexes** (the year heatmap) · **Trailing returns** · **One index ·
seasonality**. All three share the alpha mode and the Broad/Sectors/Themes chips.

- **Feed:** `docs/index_monthly.json` (~85 KB) — month-end CLOSES (not returns) per index per year:
  `{indices:[{key,label,grp,closes:{"2003":[12 nums|null]}}], asof, updated, live:{}, daily:{}}`.
  The page computes all returns client-side (MoM, CY/FY annual, alpha, seasonality, trailing) so one
  small file powers every view.
- **Trailing-returns view (added 2026-07-17):** rows × LTP · LTP-vs-52w-high · 1D · 1W · 1M · 3M ·
  6M · 1Y · 2Y* · 3Y* · 5Y* · 10Y* (`*` = annualised CAGR; per-column heat caps, so the legend reads
  "weaker/stronger" here). Every cell measures back FROM the live level: 1D from `live[key].prev`,
  52w from `live[key].yh`, 1W from the `daily` window, and the month columns from the month-end
  close N months back (⇒ 1M ≡ the running month's MTD — a month-end-only history can't do a true
  trailing month; the tooltip says which base it used). Two side-structures the top-up maintains:
  `live[key]={last,pc,prev,yh,yl,d}` from the live watch row, and `daily["YYYY-MM-DD"]={key:close}`
  kept for the last DAILY_KEEP=45 sessions. ⚠️ NSE's per-index DAILY archive is bot-walled (same
  wall as the historical endpoint), so `daily` **grows organically from 2026-07-17** — the 1W column
  renders `–` until the window reaches ~7 days back, and the sub-note says so while it's short.
- **Seeding the `daily` window (skips the week-long wait):** the SAME browser page-context XHR that
  backfilled the monthly history also returns DAILY rows, so the window can be pre-filled instead of
  accrued. Load `niftyindices.com/reports/historical-data` in the pane, then in page context loop the
  32 keys through `POST /BackPage/getHistoricaldatatabletoString` with
  `{"cinfo":"{'name':KEY,'startDate':'<~60d ago>','endDate':'<today>','indexName':KEY}"}`, reduce each
  response to `{key:{"YYYY-MM-DD":close}}` (the rows carry `HistoricalDate` "17 Jul 2026" + `CLOSE`),
  `JSON.stringify` it → save to a file → `python scripts/seed_index_daily.py <file>`. The seeder never
  overwrites a date the nightly top-up already wrote, ignores unknown keys, and trims to 45 sessions.
  Keep the dump ≲10 sessions/index if returning it through the tool channel (output gets truncated);
  the rest accrues nightly anyway. ⚠️ Use the HISTORICAL index names (the `key`s), not the live-watch
  aliases — this endpoint is the same one the `key`s came from.
- **History backfill (one-time, done 2026-07-16):** niftyindices.com `POST /BackPage/
  getHistoricaldatatabletoString` with `{"cinfo":"{'name':'NIFTY 50','startDate':'01-Nov-1995',
  'endDate':'31-Jul-2026','indexName':'NIFTY 50'}"}` → full daily history in ONE call (multi-decade
  ranges allowed), reduced to month-end closes. ⚠️ That endpoint is bot-walled for python (HTML/403)
  — it works ONLY as a **browser page-context XHR** (load niftyindices.com in the pane, sync XHR in
  an IIFE via javascript_tool). Pre-launch history is back-computed by NSE with CLOSE-only rows
  (OPEN=HIGH=LOW=CLOSE). Base anchors verified: Nifty 50 Dec-2007 6138.60 / Mar-2020 8597.75; Midcap
  100 base 1000 @ 01-Jan-2003, 2003 low 894.69 (31-Mar). Re-run recipe + endpoint discovery notes:
  memory `project-stocks-index-history-source`.
- **Daily top-up:** `scripts/fetch_index_monthly.py` reads `liveindexsa.niftyindices.com/jsonfiles/
  LiveIndicesWatch.json` (Azure CDN, NO bot wall, plain python works) and writes each index's `last`
  into its (year,month) cell. The target month comes from the row's own `timeVal` ("16-Jul-2026
  15:30") — NEVER the wall clock — so the 08:40 catch-up run on the 1st can't poison the new month.
  Live-name ALIASES (live watch spells 6 differently): SMALLCAP 100/250→SMLCAP, MICROCAP 250→
  MICROCAP250, PRIVATE BANK→PVT BANK, INDIA DEFENCE→IND DEFENCE, INDIA CONSUMPTION→CONSUMPTION.
  Fail-soft: live fetch error leaves the file untouched. Same loop also refreshes `live` and appends
  today's `daily` row (trimmed to the newest 45 dates).
- **Refresh:** `.github/workflows/refresh-index-monthly.yml` — 21:20 IST weekdays + 08:40 IST
  Tue-Sat catch-up; guard_feed + single-file /tmp commit pattern; in feeds.json (min_ratio 0.9).
- **Page details:** heatmap = diverging fills (emerald/rose rgba, alpha ∝ |ret|/cap, cap 8% monthly /
  35% annual, tighter in alpha mode) with the NUMBER printed in every cell (color never the only
  channel); custom hover tooltip shows from→to index LEVELS; annual `*` = index didn't exist the
  full year (measured from first data). **Cardify opt-out trick:** thead's LAST row is a full-width
  colspan note row → theme.js headerLabels() bails, so the matrix keeps horizontal scroll on phones
  (sticky first column) instead of card-stacking 14 numbers. sw.js SHELL + CACHE v33→v34.
- **Sanity anchors:** CY2008 Nifty 50 −51.8% / CY2017 +28.6% / CY2023 +20.0%; Midcap 100 CY2023
  +46.6%; IT CY2020 +54.9%, CY1999 +491.9%, 2008 −54.6%; Bank CY2020 −2.8%.

---

## 29. EX-DATES CALENDAR  (docs/actions.html — "Ex-Dates Calendar" nav, built 2026-07-16, SELF-UPDATING)
**Forward corporate-actions calendar:** every announced dividend/bonus/split/rights/buyback on NSE,
ordered by ex-date, dividends enriched with yield vs latest close.

- **Fetcher:** `scripts/fetch_actions.py` → `docs/actions.json` (~40 KB) — STATELESS rebuild from
  `/api/corporates-corporateActions?index=equities&from_date=&to_date=` (urllib+jar session).
  ⚠️ No params = only ~today's ex-dates (~20 rows) — ALWAYS pass the window (last 30d + next 75d;
  the far end fills in as companies file, actions are announced ~2-4 weeks ahead). The response can
  be a bare LIST or {data:[...]} — handle both. kind parsed from `subject` (D/B/S/R/BB/O); dividend
  amount regex `r[se]\.?\s*-?\s*(\d+(?:\.\d+)?)` (subjects write "Rs 2", "Rs. 11.25" AND BSE's
  "Rs. - 1.64"); yield = amt / dash_slim latest ('.NS'-key gotcha §26). Dedup (exDate, sym, subject).
- **BSE fallback (added 2026-07-20, the NSE /api/* lockdown day — §18):** when the NSE fetch fails,
  the same run builds the calendar from BSE `api/DefaultData/w?...&Fdate=YYYYMMDD&TDate=YYYYMMDD`
  (plain urllib + bseindia.com warmup, the bse_fetch transport; the Corpforward endpoint 302-loops —
  don't use it). scripcode → NSE symbol via `scripts/bse_scrips.json` `by_id`; unmapped BSE-only
  scrips are skipped (~10) to keep the page's NSE universe. Ex-date from numeric `exdate` (YYYYMMDD),
  record date from `RD_Date` ("20 Jul 2026"). Top-level `"src": "NSE"|"BSE"` in actions.json says
  which source built the file — check it before debugging a "weird" calendar.
- **Refresh:** `.github/workflows/refresh-actions.yml` — 08:30 IST Mon-Sat + 18:50 IST weekdays
  (board-meeting outcomes announce actions in the evening). feeds.json min_ratio 0.3 — the calendar
  legitimately shrinks a lot outside dividend season, don't tighten.
- **Page:** Upcoming (default) / Past-30d views, kind + min-yield filters, day-boundary rules in the
  table (first row of each date bolded). sw.js SHELL + CACHE (rode v34→v35 amid the monthly-returns
  page's bumps — three sessions shared this file today).

---

## 30. TICKER RENAME with orphaned history  (GUJGASLTD→GUJENERGY playbook, 2026-07-17)
A renamed NSE symbol whose Day-1 bhavcopy row carried **no ISIN** starts a fresh stub series in
`sf_stock_data.bin` (the ISIN auto-merge in `update_sf_data.py` can't fire) — symptom: old symbol
`alive=True` frozen at the rename date + a new short series with meta `name=SYMBOL, ind=Unknown, no isin`.
Fix sweep (all steps, in order — verify against the LIVE release asset, never the committed docs bin):
1. **Verify** it's a rename: `https://nsearchives.nseindia.com/content/equities/symbolchange.csv` (no
   cookies needed) + `EQUITY_L.csv` (current ISIN; listing date stays the ORIGINAL one). Confirm join
   continuity (old last close vs new first open ~±5%) and check `corp_actions.json` for the new symbol:
   factors with **ex < join** are the old company's history (adj stays 1) — only ex-dates AFTER the old
   series' end scale the prepend; scheme demergers show up as `noadjust` (drop is kept, correct).
2. `scripts/update_sf_data.py` **MANUAL_MERGE** += `{"NEW": "OLD"}` (loop also carries old meta
   name/ind/isin onto placeholder stubs). Publishes on the next nightly run (`merged` counts as a change);
   force with `gh workflow run refresh-backtest-data.yml` and grep the log for `MANUAL RENAME MERGE`.
3. **Refresh `scripts/symchg.csv`** from the URL above (drop-in same format) → membership/canon()
   stitches on the next weekly rebuild (indicesHistory in stock_data.bin self-heals from this too).
4. **Fundamentals under the NEW key** (ETERNAL/LTM precedent — old key deleted): in
   `docs/sf_fundamentals.json` + `scripts/fundamentals.json` + `docs/sf_revop.json` +
   `scripts/revop_fundamentals.json`, assert overlap rows equal, then `d[NEW]=d.pop(OLD)`
   (the OLD rows win — true announce dates; the new-key rows are IPO-base backfills/deadline-stamped).
4b. `scripts/_rename_map.json` += `{"OLD":"NEW"}` — the old-ticker→bin-key bridge for point-in-time
   N500 master-history joins (build_shp_backtest / build_results_season / market-mood) and FUND_ALIAS
   regeneration. Without it a renamed ex-member silently drops out of those builds once the old key
   leaves the bin (GUJGASLTD sat in 11 N500 snapshots and this step was missed on day one — fixed with
   the 2026-07-17 batch). A full rebuild regenerates the file, but the entry is needed until then.
4c. **`python3 scripts/check_fund_alias.py --write`** — re-syncs the baked `FUND_ALIAS` from 4b's map
   into BOTH client copies (`docs/backtest-engine.js` + the `docs/stock-backtest.html` inline engine;
   they must stay byte-identical) and `node --check`s the result. Nothing did this automatically until
   2026-08-10, so the constants drifted from the map for months. **The drift is SILENT:** an old ticker
   with no alias returns null profit for its whole old-name era and the stock vanishes from every
   profit-based screen — no error anywhere. The rule (measured, not assumed): alias `OLD→TARGET` when
   TARGET is the END of the rename chain, TARGET is **alive** in live META, and OLD is **not alive** —
   ABSENT counts, because a merged rename's old symbol leaves the bin entirely (GUJGASLTD/ZOMATO/PVR/
   LTIM are all absent from live META; only TATAMOTORS survives as `dead`). "OLD must not be alive" is
   also the reused-ticker guard: a recycled symbol now owned by a different live company is never
   aliased. Writes are fill-only — the ~400 older hand-curated entries whose target is itself dead are
   kept, never pruned. The nightly feed monitor runs the same check (`fund_alias` special in
   `docs/feeds.json`), so drift now turns the health board red and opens the usual auto-closing alert
   issue instead of waiting to be noticed. Bump the sw CACHE after a `--write` (§39 step 5).
   ⚠️ **The verdict is only as fresh as its META.** Live META comes from `docs/search_index.json`, and
   a rename that landed last week is simply absent from a month-old cut of it — so a stale source
   reports "in step" no matter how far the copies have drifted. The check therefore REFUSES to judge
   when that source is unstamped or older than `MAX_META_AGE_DAYS` = 10 (measured: `end` advances every
   trading day and the longest real gap between consecutive `end` values is 4 days). This is not
   hypothetical — `search_index.json` shipped `"v": ""` for weeks (§39 table), and `SF_BIN=docs/
   sf_stock_data.bin` points at the FROZEN committed bin, 58 days stale on 2026-08-10.
5. `scripts/apply_owners_full.py` **ALIAS** += `"OLD":"NEW"` (_reattr_owners stays keyed by OLD).
6. `scripts/bse_scrips.json` `by_id`: rename key (scrip code unchanged; `by_isin` already fine).
7. `scripts/fno_history.json` **+ `docs/stock_data.bin` `fnoHistory`**: replace OLD→NEW in every
   snapshot (convention = canonical CURRENT symbols, see LTM in 2021 snaps; membership rebuilds
   PRESERVE fnoHistory so the bin must be patched by hand, gzip level 9).
7b. **⚠️ THE BIN LAGS THE KEY-MOVE — any builder that joins the bin to fundamentals must bridge the gap
   (found 2026-07-17, one day after the GUJENERGY sweep).** Step 4 moves the rows to the NEW key
   immediately, but the committed `docs/sf_stock_data.bin` still holds the OLD key until step 2's
   MANUAL_MERGE publishes. In that window the two files share NO key and the company silently vanishes:
   - `build_results_season.build_liquid_universe()` added the RAW bin symbol → the renamed co left the
     "All liquid" universe for ALL 29 quarters (index membership was fine — `snap_as_of` already maps).
     FIXED: `U.add(rename.get(s, s))`. Recovered GUJENERGY + ONIDA + RNAVAL.
   - `build_quarterly_results.py` keys `syms` off fundamentals, so `px.get(sym)` missed and
     `if pdata is None: continue` dropped the co **off the page entirely** (2325 → 2328 cos).
     FIXED: forward-carry `px[new] = px[old]` for every `_rename_map` pair, fill-only.
   Both are fill-only/identity-preferring, so they self-disarm once the bin catches up. **After ANY future
   rename, re-check every bin↔fundamentals join** (`grep -l sf_stock_data.bin scripts/*.py`) — a renamed
   co disappearing is SILENT, there is no error.
8. **Leave alone:** `ipo_base_fills.json` (fill-only reapply → harmless no-op once cells are non-null),
   `_reattr_owners.json` (OLD-key convention), `shp_history.json` (SHP fetcher already migrates),
   stock_data.bin `.NS` series (self-consolidates from the EQUITY_L universe on its own refresh).
GUJENERGY specifics: rename eff 2026-07-01 (Gujarat Gas→Gujarat Energy Ltd, GSPL scheme), ISIN
INE844O01030 unchanged, join 327.05→340.00 continuous, 2026-07-02 −11.75% = scheme-demerger ex-date
(kept via NSE noadjust), 2019 split factor keyed under GUJENERGY is pre-join → adj=1.
Batch 2026-07-17 (rest of the day-one tripwire catch, merged the same way): MIRCELECTR→ONIDA
(2026-06-19, history to 1996) and LYPSAGEMS→AURUS (2026-07-14, to 2013) — full sweep incl. BSE scrip
key + ALIAS; ⚠️ ONIDA's post-rename re-parse had stored the **H1-FY26 cumulative as the Sep-25
"quarter"** (rev 304.16=140.85+163.31, pat −14.27=−12.49+−1.78) — dropped for the at-filing-time
MIRCELECTR rows (true Sep-25 pat −1.78); always prefer the OLD key's rows on overlap. Plus the 9 Axis
ETF renames of 2026-07-03 (AXISTECETF→ITAXIS, AXISNIFTY→NIFTYAXIS, AXISBNKETF→BNKETFAXIS,
AXISHCETF→HEALTHAXIS, AXISCETF→CONSUMAXIS, AXSENSEX→SENSEXAXIS, AXISGOLD→GOLDAXIS,
AXISVALUE→VALUEAXIS, AXISILVER→SILVERAXIS): price+_rename_map only — no fundamentals/BSE/F&O side
(verified absent), steps 4–7 n/a. All 11 joins 0.95–1.05, adj=1.

---

## 31. NIGHTLY TRENDLYNE RECONCILE  (tl-reconcile.yml 23:45 IST → docs/tl_reconcile.json, built 2026-07-17)
External yardstick so a silent coverage outage (like §15's import-crash: 121 vs Trendlyne's 144) is
caught the same night, not days later. `scripts/tl_reconcile.py` (pure stdlib):
1. OUR declared for the current quarter = numbers (sf_fundamentals + bse_fundamentals + vision_fills)
   ∪ feed-declared (`results_pending.classify` — the shared classifier, counts can't drift).
2. TL total from the public dashboard tiles (pos+neg+neutral); TL NAMES from their free calendar API
   `equity/api/events/calendar-v2/?corporate_actions=BM&start_date=DD/MM/YYYY&end_date=…` (3-day
   windows under the 200-row curtail, result-purpose only).
3. Match by NSE symbol → BSE scrip (bse_scrips by_id reversed) → normalized name; unmatched bucket into
   `other_quarter` (March/annual filers — both sides exclude), `awaiting_filing` (met ≤1 day ago,
   grace), `actionable` (we likely missed a filing).
4. Actionable ⇒ SELF-HEAL: re-runs fetch_announcements.py + fetch_bse_results.py (31-day windows make
   them idempotent), re-diffs, records `healed`.
5. Workflow commits report + healed feed/calendar (NOT announcements.json — git-bloat guard §15),
   then opens/updates ONE "Trendlyne reconcile" issue when actionable ≥ 3, auto-closes when clean.
- TL unreachable ⇒ tl_total=null, actionable=[], exit 0 — their outage must not page us.
- tl_reconcile.json is in feeds.json (max_age 54h) so the yardstick itself is monitored.
- Numbers for healed filings arrive via the normal XBRL/vision machinery — this job only guarantees
  the DECLARED set is complete; it never scrapes Trendlyne's numbers.
- qe=0 feed rows count as DECLARED coverage here (2026-07-21) — they're real filings whose period is
  still being resolved (§15 qe=0 state); without this the diff re-flags them nightly while the vision
  prep is already on it. Manual exact-diff method (when TL's index count differs from ours): compare our
  page-equivalent set vs TL's free calendar API result-purpose meetings — but EXCLUDE meetings dated
  TODAY (scheduled, not yet declared) before comparing counts (the 64-vs-63 audit of 2026-07-21 found
  YESBANK/SOBHA/JPPOWER this way).

## 32. HISTORICAL INDEX/F&O MEMBERSHIP from WAYBACK — the MONEYCONTROL trick (2026-07-17)

**When NSE/niftyindices has no archived list for an era, moneycontrol's Wayback captures usually do.**
Found during the 2008-2015 Nifty-500 hunt: official full-list captures exist at only 5 dates ≤2015 (CDX-proven
exhaustively — no nseindia/iisl/niftyindices path has more), but MC's index-composition page has ~80 captures
2001→2025 with FULL single-page tables (~500 rows, name+industry+mktcap; pre-2013 incl. index weight).

Recipe (all steps have working scripts in scripts/, see memory project-stocks-n500-membership-history):
1. CDX-enumerate: `web.archive.org/cdx/search/cdx?url=moneycontrol.com/stocks/marketstats/indexcomp.php&matchType=prefix&output=json&filter=statuscode:200&collapse=timestamp:8`
   — filter querystring for the index you want (`index=7` = CNX/Nifty 500). Other indexes have other ids.
2. Fetch each capture RAW: curl_cffi impersonate=chrome + `Accept-Encoding: identity`; for wayback use the
   `id_` URL form; if bytes start `\x1f\x8b` gunzip manually (wayback sometimes mislabels).
3. Parse era-aware (3 markups): 2003-10 = plain `<a>` in rows; 2011-22 = `stockpricequote` hrefs with the name
   NESTED in `<b>` (strip inner tags, don't require `[^<]`); 2004-05 = `onclick=open_pricechart` anchors.
4. Resolve NAMES→symbols against era lists (constituent CSVs + Wayback EQUITY_L 2010/2011 + symchg.csv rename
   chain + evidence-backed MANUAL map). NEVER guess a binding; verify vs era bhavcopy/symchg. ⚠️ Match the
   TARGET file's symbol convention: _full_union_2015_v3 keys by CURRENT symbols (BAJAJ-AUTO/ARE&M), the 2008
   union by ERA symbols — resolving to the wrong era manufactures phantom diffs (41-50/capture seen).
5. Treat MC lists as SOFT checkpoints (3rd-party, may lag NSE by days, 475-528 row jitter) vs hard NSE anchors.

Gotchas that cost time: don't CDX only `.csv` — the same NSE list lived at `.htm`/`.xls` (2002-2006 full lists
found that way, saved scripts/_n500_pre2008/); the 2008 `.htm` "gzip-corrupt" wayback capture is really PLAIN
HTML mislabeled by wayback (fetch raw, skip auto-decode) but is an 11KB shell; (Windows-era) TZ= didn't work in Git Bash.

Membership-hunt asset inventory (untracked, scripts/): _n500_mc_caps/ (85 raw MC captures), _n500_mc_parsed.json
(74 full lists), _n500_pre2008/ (28 official 2002-06 lists), _n500_rawcsv/ (official CSVs + era EQUITY_L),
_prs_hunt_0814/_0814b/_0607/ (205 press-release PDFs 2006-2014), parsers _n500_parse_era0814/_sym0814,
resolver _n500_resolve_names.py, builder _n500_build_0814.py, output _full_union_2008_v1.json (798, era syms).
OPEN: ~15-25 verified-candidate transient members (EVEREADY/GEOJIT/KEMROCK/ESSARPORTS class) not yet added;
MC-checkpoint integration + drift re-grade pending (memory has the step list).


## 33. MACRO DASHBOARD  (docs/macro.html — "Macro" nav, built 2026-07-17, SELF-UPDATING)

**Presentation v2 (2026-07-18, own design — deliberately NOT the quantmac table layout):** chart-first
"Macro Pulse" board. Pulse strip (6 stat cards w/ sparkline + percentile-in-history rail) → group-filter
pills (Markets/Flows/Valuation/Rates & FX/Economy/Commodities) + 6M→MAX range pills → ~27 always-visible
chart tiles (SVG area/line w/ median dashline, monthly diverging pos/neg bars for flows & breadth, hover
tooltips, Lo/Med/Hi + percentile footer, lazy-rendered via IntersectionObserver) → "Twelve quarters at a
glance" heat grid (CSS grid, NOT a <table> — keeps theme.js mobile cardify away; per-row zero-anchored
shading, CPI inverted, rows w/ <2 readings auto-hidden, e.g. FII/DII until the rolling window grows) →
sector-rotation zero-based horizontal bars w/ dashed Nifty marker + 1M/3M/6M/1Y pills. 3 derived on-page
series: earnings-yield gap (100/PE − India 10Y), India−US 10Y spread, real policy rate (repo − CPI).

**v2.1 "build all" (2026-07-18):** 🧭 regime gauge (blend of 5 percentiles: valuation=100−PE-pctile,
breadth=%>200DMA-pctile, trend=6M-Nifty-return-pctile, FII-1M-flow-pctile, calm=100−VIX-pctile → 0-100
score + band) + ⚡ "what moved this week" (Δ percentile-in-history over 7d; series with last reading
>14d old are EXCLUDED so stale monthlies can't masquerade as this-week moves) · new tile groups
Global + Earnings (9 group pills now) · fetch_macro.py additions — mql5 MQ is now {key:(country,slug)}:
india/wpi-yy, india/trade-balance, india/markit-manufacturing-pmi, india/markit-services-pmi (the
sp-global-* slugs 404 — markit-* are the live ones), united-states/fed-interest-rate-decision,
united-states/consumer-price-index-yy; Yahoo: SI=F silver→₹/kg, HG=F copper, ^GSPC, ^IXIC, ^N225,
^HSI · GDP_NOMINAL seed dict in fetch_macro.py (₹ lakh cr, FY-end dated, FY26=BE — update yearly when
the provisional estimate lands ~May) + `mcaptot` = daily total mcap summed from dash_slim.bin (it's
gzip'd JSON; sanity-guarded n≥1500 rows & 100<lakh-cr<5000 so a truncated bin can't poison) → Buffett
tile mcap÷GDP (133.5% at launch; HISTORY ACCUMULATES from 2026-07-18, tile shows a note until ≥2 pts) ·
more derived tiles: Nifty drawdown, Midcap-100÷Nifty & Smallcap-100÷Nifty (index_monthly closes,
rebased 100), Gold÷Nifty, Brent-in-₹, FII index-futures long% (fii_fo.json rows oi.FII.futIdx=[L,S]) ·
earnings pulse = median rev/op/PAT YoY per quarter from results_season.json defaultUniverse (quarterly
bars) · heat grid rows += WPI (inverted), Mfg & Services PMI (shaded around base 50), trade balance.

**Data = one new fetcher + reuse of existing feeds.** `scripts/fetch_macro.py` -> `docs/macro.json`
(15 series, cumulative {date:value} maps, merged & never shrunk — a skipped/partial run self-heals):
  - Yahoo chart API: US 10Y (^TNX), USD/INR (INR=X), ICE dollar index (DX-Y.NYB), Brent (BZ=F),
    gold (GC=F, converted to ₹/10g via same-day USD/INR).
  - mql5 economic-calendar /export TSV (same source as fetch_bank_credit.py, §): cpi-yy, gdp-yy,
    industrial-production-yy, rbi-interest-rate-decision (repo), rbi-m3-money-supply-yy,
    deposit-growth-yy, foreign-exchange-reserves. ⚠️ mql5's **gdp-yy stops at 2024-11** (that slug
    stopped updating) — the page's economy table uses mode 'qlast' (last reading IN each quarter,
    blank if none) so stale values are NOT carried forward; GDP simply blanks recent quarters until
    a fresh source is wired.
  - FRED CSV: India 10Y G-Sec (INDIRLTLT01STM, monthly). Best-effort — FRED is US-hosted and
    **unreachable from many non-US IPs (incl. this dev box)**, so it fails locally but works on the
    GitHub US runner; a failure just keeps the existing series.
  - NSE allIndices (cookie-primed via build_fundamentals.nse_jar): current Nifty 50 PE/PB/DY,
    appended daily. 403s locally behind Akamai; fine in CI.
The page ALSO reads existing feeds directly (not duplicated into macro.json): india_vix.json,
nifty.json, market_breadth.json (breadth: %>200DMA, net advances=adv−dec, net 52w=hi−lo),
fii_dii.json (FII/DII net, summed per period), bank_credit.json (bank-credit y/y), index_monthly.json
(sector rotation — computed from MONTHLY closes with live.last spliced into the current-month slot;
1W column dropped since index_monthly.daily holds only today).

**Nifty PE/PB/DY history seed (one-time, 2016→date, 2611 daily rows):** python is bot-blocked on
niftyindices, so it was pulled via the browser page-context. Endpoint (note capital-P **BackPage**,
the old Backpage.aspx is dead): `POST https://www.niftyindices.com/BackPage/getpepbHistoricaldataDBtoString`
body `{"cinfo":"{'name':'NIFTY 50','startDate':'01-Jan-2016','endDate':'<today>','indexName':'NIFTY 50'}"}`
-> `{d:"[{pe,pb,divYield,DATE:'17 Jul 2026'},…]"}`. Merged into macro.json's niftype/niftypb/niftydy.
Verified vs quantmac to the decimal (Dec-25 PE 22.75 / PB 3.55 / DY 1.28). The nightly fetch appends
the current value, so history keeps growing without re-seeding.

**Refresh:** refresh-macro.yml, daily 15:10 UTC (20:40 IST), same commit-retry+pages-dispatch pattern
as bank-credit; guard_feed (feeds.json min_bytes 150000, min_ratio 0.9). Nav = theme.js NAV_GROUPS
Markets 🌍; SW shell = sw.js v37.

**Known v1 gaps (intentional, honest):** FII/DII quarter columns beyond ~3 months are blank
(fii_dii.json is a rolling recent window). %>200DMA uses our Nifty-500 universe (≈55%) not
"all NSE" (≈42%). DXY = ICE index (~100), not FRED's broad trade-weighted index (~120). repo rate
comes out 5.20 vs RBI's 5.25 (mql5 rounding). mcap/GDP (Buffett) and %>50DMA are NOT built (no feed).
To rebuild after a full data wipe: run fetch_macro.py (fills everything except deep PE history) then
re-seed PE via the niftyindices browser call above.

---

## 34. PAGE GROUPS — merged tabbed sections  (theme.js, built 2026-07-18)
Six former nav entries' worth of sibling pages are presented as ONE tabbed section each. **Everything
lives in `docs/theme.js`**: the `PAGE_GROUPS` array defines the groups; `buildTabs()` injects a shared
pill tab strip (`.sw-tabs`, same look as the Quarterly Results tab bar) as the FIRST CHILD of `<main>`
on every member page; `TAB_PRIMARY` maps each member file to its group's first tab so `buildNav()`
highlights the single group nav entry from any member. **Member pages keep their own URL, payload,
data pipeline and feeds.json entry — nothing was moved or redirected**; tabs are plain links between
the pages (fast: slim payloads + SW shell cache). The nav/footer/home-tiles list one entry per group.

Groups (nav entry → members):
- **Market Analytics** (→ monthly-returns.html, per user 2026-07-18): Monthly Returns · Market Mood
- **FII/DII** (→ fii-dii.html): Daily Flows · Stock Holdings (shareholding.html)
- **Deals & Insiders** (→ deals.html): Bulk/Block Deals · Insider Trades · Delivery Spikes
- **Corporate Calendar** (→ ipos.html): IPOs & Listings · Ex-Dates (actions.html)
- **Strategies** (→ saved-strategies.html): Saved Strategies · Backtest History · Live Tracking (owner-only tab)
- **Owner console** (→ status.html, PRIVATE): Data Health · Results Coverage · Page Stats · Insurer Inbox

Rules:
- To merge/split/rename a section: edit `PAGE_GROUPS` (+ the matching single entry in `NAV_GROUPS`).
  Do NOT add per-page tab markup — the strip is injected.
- PRIVATE members are filtered from the strip for non-owners (`IS_OWNER`/`PRIVATE_PAGES`); a group whose
  visible tabs drop below 2 renders NO strip (that's why status.html shows nothing to the public).
- A page in a group should NOT also carry its own top-level tab bar named like the group (in-page tabs
  like Quarterly Results' are fine — that page is not in any group).
- Home-tile blurbs for group entries live in index.html `DESC` keyed by the PRIMARY file — describe the
  whole group there (absorbed pages' DESC keys were removed).
- The distinct case: **Results Season is a REAL in-page tab** of quarterly-results.html (§11/§15), not a
  page group — its old page is a redirect stub. Page groups were chosen for the other six merges so the
  heavy per-page JS (charts/Supabase/backtest engines) never had to be namespaced together.

---

## 35. GLOBAL MARKETS DASHBOARD  (docs/global.html — "Global Markets" nav 🌏, built 2026-07-20)

Overnight cue for the Indian open (modelled on quantmac /market/global, but our own dark design +
extra features so it does NOT look like theirs). US / Europe / Asia / India indices, commodities,
crypto and FX/rates — each with last close, 1D/1W/1M/YTD/1Y %, 60-pt hover-tooltip sparkline, 52w
range bar, and **correlation-to-Nifty** (Pearson of ~1y daily returns).

**Data:** `scripts/fetch_global.py` -> `docs/global.json`. STATELESS — rebuilt each run from ~2y of
Yahoo Finance daily history (`/v8/finance/chart/<sym>`), so nothing to accumulate/corrupt; a failed
symbol just drops out and self-heals next run. ~32 instruments (see INSTRUMENTS list). Nifty (^NSEI)
is fetched first so every risk asset can correlate against it; `NO_CORR` skips India/FX/VIX. Output:
`{as_of, updated, source, groups:{US,Europe,Asia,India,Commodities,Crypto}, fx:[...]}`.

**Advanced features (ours, not in the reference):**
- **Global cue gauge** — each overnight risk asset's 1D move weighted by max(corr-to-Nifty,0), summed
  -> a −100..+100 index + Risk-on/Neutral/Risk-off verdict for the open (computed client-side, transparent).
- Live **session clock** (Tokyo/HK/India/London/US open-now, UTC-hour based), breadth + regional pulse
  tiles, overnight movers strip, card+table (sortable) views, group + timeframe (1D/1W/1M/YTD/1Y) pills.
- ⚠️ **VIX is excluded from every aggregate** (`AGG_SKIP={vix:1}`): a VIX spike is risk-OFF, so leaving
  it in an equity average (it's in the US group) inverts the "US overnight" tile (+12% VIX flipped
  −0.9% equities to a phantom +1.7%). VIX still shows as a card; just never counts toward avgs/breadth/cue.

**Refresh:** `.github/workflows/refresh-global.yml`, 2x/day 02:10 + 16:10 UTC (07:40 + 21:40 IST),
same guard + commit-retry + pages-dispatch pattern as refresh-macro. Guard: feeds.json global.json
min_bytes 12000, min_ratio 0.5 (stateless size varies). Nav = theme.js NAV_GROUPS Markets>Overview 🌏.

---

## 35. VOLUME SHOCKERS  (docs/volume.html — "Volume Shockers" nav, built 2026-07-20, SELF-UPDATING)
**The unusual-volume screen:** stocks trading many times their own 20-session average volume today —
the first sign of sudden interest. Modelled on (but our own design, not a clone of) the reference
"Volume Shockers" page. Paired with Delivery Spikes in the **Deals & Insiders** page group (same
bhavcopy source, same "unusual activity today" nature).

- **Zero new network — a DERIVED view.** `scripts/build_volume.py` reads the delivery pipeline's
  rolling history `docs/delivery_hist.json` (already refreshed every evening by `fetch_delivery.py`)
  plus `docs/dash_slim.bin` meta (name/sector/mcap/52w) and `scripts/fno_list.json`. It must run
  **right after** `fetch_delivery.py` in `refresh-delivery.yml` (it does).
- **Volume source + the reconstruction bootstrap:** `fetch_delivery.py` was widened 2026-07-20 to
  store the exact NSE columns `TTL_TRD_QNTY` (col 10) and `TURNOVER_LACS` (col 11) — cells are now
  `[close, delivQty, delivPct, vol, turnLacs]` (older cells are length 3, index defensively, never
  unpack). Until 45 sessions of exact columns accrue, `build_volume.py` reconstructs volume as
  `delivQty / (delivPct/100)` and turnover as `close*vol/1e7` — **verified exact** on the seed day
  (CAMPUS reconstructs to 52.3M vol / ₹1,235 cr / 5.7% deliv, matching the reference to the decimal).
  The displayed day is exact; only the 20-session baseline is mildly noisy from delivPct rounding and
  self-heals to fully exact as real columns fill in.
- **Rule:** ratio = todayVol / mean(prior up-to-20 sessions), needs ≥15 prior non-null sessions (so
  fresh listings never inflate their own baseline), ratio ≥ 2×, turnover ≥ ₹5 L, a known mainboard
  name. Page filters (ratio 2/3/5/10×, turnover Any/25L/1Cr, direction, delivery-confirmed / F&O-only
  lens, sector, search, sort) are all client-side.
- **Feed `docs/volume.json`** (~19 KB): `rows` [sym,name,ltp,chg%,vol,avg20,ratio,turnCr,delivPct,
  sector,mcap,fno,near52,dir], `sectors` [sector,count,upCount] (the sector-activity strip), `repeat`
  [sym,name,nSess,bestRatio,lastDate] (our extra "≥2 shock sessions in 5" view), `stats`.
- **Our differentiators vs the reference** (the "don't look like them" ask): dark analytics UI with a
  gradient hero + stat tiles, a **sector-activity strip** (where the unusual volume concentrates,
  green = up-day share), a **ratio bar** per row, **delivery-confirmed** badge/lens, **F&O** tag +
  filter, **distance-from-52w-high** column, an up-vs-down **direction** split, and the multi-session
  **Repeat shockers** view.
- **Wiring:** run in `refresh-delivery.yml` (build step + volume.json on the /tmp cp-back + git-add
  lists, §18 gotcha); nav tab in `theme.js` PAGE_GROUPS "Deals & Insiders"; `feeds.json` volume.json
  (min_bytes 8000, min_ratio 0.3 — shocker counts vary a lot day to day); `sw.js` SHELL + CACHE v46→v47.
- **Reference for parity checks:** the StockView "Volume Shockers" page (turnover/ratio/delivery
  columns) — our turnover/volume/delivery match to the decimal; ratios differ slightly by baseline
  window definition, which is expected and documented on-page.

## 36. LIVE (INTRADAY) DATA LAYER — which page gets live numbers from where  (2026-07-20)

The site is static (GitHub Pages), so "live" = the BROWSER fetching an intraday source
directly on page load + a 60 s timer during IST market hours (9:00–15:45 Mon–Fri, gated by
`document.hidden`). Every live top-up **overwrites baked numbers in place and fails silently**
— if the source is down the page simply keeps the baked feed, so nothing ever breaks.

Two live sources:

1. **niftyindices CDN (NO proxy needed)** — `https://liveindexsa.niftyindices.com/jsonfiles/LiveIndicesWatch.json`
   sends `Access-Control-Allow-Origin: *`, so pages fetch it straight from the browser.
   All ~131 NSE indices: level, 1D %, OHLC, 52-w high/low, `timeVal`. It is ALSO the nightly
   backbone of fetch_index_table.py — same keys (`indexName` verbatim == indices.json `k`),
   so live rows merge by exact string match.
   * **indices.html** — live level / 1D % / 52-w range + green "● LIVE hh:mm" badge; stat
     cards + heatmap recompute per tick. 1W/1M/1Y, PE/PB/DY, sparkline, valuation
     percentile stay baked (they're history-derived).

2. **Cloudflare Worker** `https://stocksworld-quotes.dhruvan2510.workers.dev`
   (source `scripts/live-quote-worker.js`, deploy = paste whole file in CF dashboard →
   Workers → stocksworld-quotes → Edit code → Deploy; guide `scripts/LIVE_FEED_SETUP.md`).
   Four routes, all CORS-open, all cached in-Worker (30–90 s):
   * `?symbols=RELIANCE,TCS` — Yahoo NSE quotes (`.NS` auto-appended; `^NSEI` style passes
     verbatim). Users: saved-strategies Today's-Picks "Go Live", stock-backtest, **watchlist.html**
     (price + day % chip per starred stock, batched 30/call).
   * `?chart=SYM` — verbatim Yahoo intraday chart passthrough. Users: index.html home ticker,
     **stock.html** (header price + 1-day % go live; chart/technicals stay EOD).
   * `?quotes=^GSPC,GC=F,BTC-USD` — VERBATIM Yahoo symbols (futures `=F`, FX `=X`, crypto
     `-USD`, foreign indices). User: **global.html** (all tiles + cue gauge recompute live,
     60 s always — global markets trade ~24 h).
   * `?announcements=1` — NSE corporate announcements w/ cookie warmup. Users:
     announcements.html, quarterly-results.html LIVE tab.
   * `?nse=volume-gainers|gainers|loosers|fiidii|large-deals` — WHITELISTED NSE
     live-analysis passthrough (cookie warmup, 60 s cache; array responses ride
     under `.data`; volume-gainers capped 60 rows). Users:
     **volume.html** ("LIVE volume spurts" card — NSE's intraday feed, ratio vs NSE's
     1-WEEK avg, deliberately a SEPARATE section from the EOD 20-session table);
     **movers.html** ("LIVE this session" strip — raw NSE gainers/losers chips from the
     `allSec` bucket; the main table stays corp-action-adjusted EOD) — both market-hours
     only, hidden otherwise;
     **fii-dii.html** (`fiidii` — the day's PROVISIONAL cash numbers appear ~6pm IST,
     appended as a live row until the nightly bake catches up; 10-min poll);
     **deals.html** (`large-deals` — today's bulk/block snapshot lands in the evening,
     prepended + re-rendered when its as_on_date is newer than the feed; 10-min poll).
   * `?ipo=SYM` — live subscription for ONE open IPO (ipo-active-category; 60 s cache).
     User: **ipos.html** — every OPEN issue's ×-subscribed goes live (3-min poll); the
     baked feed only updates 2×/day, so intraday multiples can differ hugely (CMLL
     2026-07-20: baked 6.41× vs live 14.04× at 13:27).
     Schemas verified via `probe-nse-api.yml` (dispatch-only workflow — CI IPs + warmup
     reach NSE; ALWAYS use it before wiring any new NSE endpoint).
     ⚠️ `/api/equity-stockIndices` (would give live breadth) is blocked even from CI —
     that's why market-mood breadth has no live version. ⚠️ `/api/corporates-pit` gets
     bot-walled on wide date windows and PIT filings are XBRL since Apr-2026 (need
     per-filing parsing) — that's why insider has no live strip.

Also CDN-live (no worker): **monthly-returns.html** (FEED.live levels overwritten from
LiveIndicesWatch at load — keys match verbatim — so MTD/trailing cells read NOW) and
**macro.html** (live Nifty + India VIX point spliced into the daily series at load via
worker ?symbols=^NSEI,^INDIAVIX — VIX is NOT in LiveIndicesWatch; weekday guard so a
weekend load never stamps a Saturday/Sunday date).

Deliberately NOT live (EOD/filings by nature — don't "fix"): delivery %, insider
(XBRL, see above), market-mood breadth (blocked, see above), shareholding, sectors,
mutual funds, bank-credit, live-tracking (paper-trades at CLOSES by design),
quarterly numbers, discovery buckets. Movers/volume EOD tables stay EOD — their live
strips are additive, never replacements (different baselines/adjustment).

Gotchas:
- **Worker route missing = silent fallback.** Pages call routes that may not be deployed
  yet; every live fetch is wrapped in catch{} → baked data stays. After editing the worker
  file, it does NOTHING until pasted into the CF dashboard (no wrangler/CI deploy).
- LiveIndicesWatch numbers are STRINGS → parseFloat everything; skip rows with last<=0.
- After 15:45 IST both sources keep serving the closed session — pages fetch ONCE on load
  even off-hours (fills the gap between close and the nightly bake) but only auto-refresh
  during market hours.
- The SW never caches cross-origin requests (worker/CDN stay network-only) and HTML pages
  are network-first — live-layer edits need no SW cache bump unless the SHELL list changes.

## 37. INDIAN INDICES TABLE  (docs/indices.html — "Indices" nav 📇, built 2026-07-20, SELF-UPDATING)

- **Feed:** `scripts/fetch_index_table.py` (NOT `fetch_indices.py` — that one tags stock
  membership; name collision avoided) → `docs/indices.json` (slim page payload, every
  derived field pre-baked) + `docs/indices_hist.json` (SIDE file, never shipped: rolling
  260-session closes + 750 PE/PB/DY samples per index).
- **Sources layered best-effort:** liveindexsa `LiveIndicesWatch.json` (CDN, no bot wall,
  ALWAYS-ON backbone: level/1D%/OHLC/52wHL for ~131 indices) + NSE `/api/allIndices`
  (PE/PB/DY + perChange30d/365d — works in CI, 403s from many raw IPs; merged via NSE_ALIAS).
  Keys = raw LiveIndicesWatch `indexName` verbatim.
- **Window accrues organically** (NSE's per-index daily archive is bot-walled → no backfill):
  1W % fills after ~a week of runs, 30-day sparkline after a month, valuation percentile
  sharpens over weeks. Em-dashes in young columns are CORRECT, not a bug. 63/131 have PE
  (bond/G-Sec/factor indices omit it — also correct).
- **Refresh:** `refresh-indices.yml` 2x/day (21:05 + 08:50 IST) + push-trigger self-test;
  commits BOTH json files (reset-and-replay cp-back — §18 gotcha applies). Fail-soft: a
  dead primary fetch leaves both files untouched (exit 0) so the page keeps yesterday.
- **feeds.json:** indices.json + indices_hist.json entries exist (guard_feed monitored).
- **In-browser live top-up:** §36 — the page ALSO fetches LiveIndicesWatch directly
  (CORS-open) every 60 s during market hours; level/1D%/52w go live, the rest stays baked.

### 37a. ★★ NSE allIndices uses 0 as a NO-BASE SENTINEL on perChange365d/30d  (fixed 2026-08-10)

**Symptom:** `docs/indices.json` carried `y1: 0` for NIFTY MIDSMALLCAP400 50:50, which the
page rendered as a confident **"0.0%"** in the 1Y column — sitting next to Nifty Midcap
Select's correct `null` → "—". A young index was being reported as *exactly flat over a
year* rather than *unknown*, and its `rs1y` (relative strength vs Nifty 50) was a
fabricated −0.89 derived from that fake zero.

**Cause:** NSE `/api/allIndices` returns `perChange365d: 0` — not null — for an index with
no value a year ago. The row says so three other ways: `oneYearAgoVal: 0`,
`date365dAgo: null`, `chart365dPath: null`.

**Measured, not assumed** (CI probe of `/api/allIndices`, all 139 indices, 2026-08-10 —
raw IPs 403, so `probe-nse-api.yml` is the reachable path; it now takes an optional `jq`
filter input so a big payload can be summarised without the 4000-char log cap):
- 10 indices had `perChange365d == 0`; **all 10** also had `oneYearAgoVal == 0` +
  `date365dAgo` null + `chart365dPath` null.
- **Not one** index reported `perChange365d == 0` with a real base — so nothing legitimate
  is lost by suppressing the pair.
- The **30d field was clean**: every `oneMonthAgoVal` was a real level, and the single
  `perChange30d == 0` (NIFTY FMCG) had a real base — a genuine flat month. Handled
  symmetrically anyway so the class can't appear there unnoticed.
- **1W is not exposed:** `w1` is computed only from our own rolling history, never from an
  NSE field. The live-watch leg is also clean — every row had a real `previousClose`, and
  `percChange` agreed with `last`/`previousClose` within 0.06 pp for all 131 rows, so the
  `pc` column has no sentinel class either.

**★ Suppress on the CONJUNCTION only — exact 0 AND a missing base.** Neither half is safe:
- **Empty base alone is NOT enough:** `NIFTY50 USD` reports `oneYearAgoVal: 0` and
  `date365dAgo: null` yet a real `perChange365d` of **−7.14**. Nulling on the empty base
  (or on the null date) throws a good number away — this is the trap to avoid.
- **Exact 0 alone is NOT enough:** a genuinely flat window rounds to 0.00 legally and
  arrives with a real base (NIFTY FMCG's 1M).

With `c365` set to None the fetcher falls back to `change_back()` over our own rolling
history — null (→ "—") until 250 sessions accrue, then a real number. Same for `rs1y`.

**Regression check** (§39): the fetcher logs the suppressed set every run *and* prints
every surviving exactly-0.00 1M/1Y value, each of which is provably base-backed by
construction. If the suppression ever regresses, the whole young-index cohort shows up in
that second line instead of quietly rendering "0.0%".

**Generalises:** an API that returns 0 where it means "no data" is a *silent* wrong value —
unlike a null it passes every non-null check and renders as fact. When a feed exposes the
base alongside the derived change, gate the change on the base.

## 38. CONCURRENCY — ONE WRITER PER TREE  (2026-07-22, after the vision-fill / backfill tangle)

**WHO WRITES THIS REPO** (three actor classes, overlapping all day):
- **GitHub Actions** (~30 `refresh-*.yml` + `refresh.yml` + `pages.yml`…) — cloud checkouts,
  file-scoped `git add`, per-workflow `concurrency:` groups, rebase-retry push. Well-behaved;
  keep that pattern intact when editing workflows.
- **Cloud Claude routines** (claude.ai/code/routines; today: bse-vision-fill 4×/day §17b +
  deep-fundamentals nightly §50) — fresh throwaway VM per run, lands work via a `claude/*` branch +
  auto-merged PR. (The old LOCAL scheduled-task pattern — private persistent worktree under
  `~/stocks-wt/<task>`, never the interactive checkout — retired with the Windows box 2026-08-05,
  but remains the rule for any future local routine.)
- **Interactive Claude sessions** (often several at once) — share `/Users/dhruvan/stocks-dashboard`.

**THE RULES** (mirrored in repo-root CLAUDE.md, which every session auto-loads):
1. Own-files-only staging; never `git add -A` / `git commit -a`.
2. No `reset --hard` / `stash` / `rebase --autostash` in the shared checkout — worktrees only.
3. Long/scripted jobs → own worktree (`git worktree add --detach ~/stocks-wt/<name> origin/main`).
4. Rebase-retry push; if blocked by others' dirty files → cherry-pick your commit in a temp worktree.
5. Ledgers, not derived files; re-verify LIVE ~20 min after any heal push (in-flight CI can race).
6. Plain local `date` for commit labels (the Mac runs IST; the old Windows git-bash TZ=-prints-UTC trap is gone).

Rules 1–2 are ENFORCED BY HOOKS: `.claude/settings.json` runs `scripts/_concurrency_guard.py`
(pre-edit: file dirty from another session → confirmation prompt; pre-bash: `reset --hard` /
`stash` / `add -A` / `commit -a` / autostash-rebase / `checkout .` / `clean` / force-push in the
shared checkout → confirmation prompt; session-start: injects the current dirty-file list).
Worktrees (`stocks-wt/*`, `.claude/worktrees/*`) are exempt — single-writer by construction.
Guard gotcha (Windows-era): PowerShell 5.1 pipes prepended a UTF-8 BOM; the guard still strips
one before json.load (harmless on macOS).

**CASE STUDY 2026-07-22** (why these rules exist): the scheduler fired vision-fill at 03:42 IST;
the laptop slept mid-run; the commit label read "21-Jul 22:22 IST" (TZ bug → UTC), so after waking
at ~11:50 the SAME session didn't recognize its own commit b358d2e, concluded a CI job had "picked
up and committed" its files, re-extracted 7 already-filled companies, and its `rebase --autostash`
collided with the revenue-backfill session's uncommitted sf_revop.json / revop_fundamentals.json —
parking that WIP in stashes and restoring files to HEAD by guesswork. No data was lost, but only by
luck; a follow-up "recovery" nearly rewrote history off a false ABSENT reading (bse_fundamentals
`px` is keyed by SCRIP CODE, not symbol — check the structure before declaring data missing).

**HOT-FILE OWNERSHIP** (single writer each; everyone else goes through the ledger):
- `sf_revop.json` / `results_season.json` / `quarterly_results.json` / `sf_fundamentals.json` —
  nightly `refresh.yml` rebuild is the writer; local backfills write the
  `scripts/revop_fundamentals.json` ledger + rebuild (that's why backfill work survives the nightly).
- `bse_fundamentals.json` / `vision_fills.json` / `bse_results.json` / `results_coverage.json` —
  bse-vision-fill CLOUD routine (own VM, lands via PR; §17b).
- `shp_history.json` — `refresh-shareholding.yml` ONLY; never two writers (§22 corruption).
- Ledgers (`scale_fix.json`, `feed_qe_fix.json`, `ann_date_fills.json`, `_bse_fund_done.json`, …) —
  append via their scripts; safe to edit locally because CI replays them.

**WORKTREE RECIPES:**
```
create:  git worktree add --detach ~/stocks-wt/<name> origin/main
sync:    (inside it) git fetch origin -q && git reset --hard origin/main    <- safe THERE only
push:    git push origin HEAD:main    (inside fetch+rebase retry loop)
remove:  git worktree remove ~/stocks-wt/<name>
list:    git worktree list
```

### 38b. ★★ LONG BATCH DRIVERS LIE ABOUT SUCCESS — verify against origin, not local state
_(2026-08-04, PRE2015 STEP N: three separate bugs, each of which reported success while
losing or skipping real work. All three were found by RECONCILIATION, never by a log line.)_

A driver that loops {harvest → apply → guard → commit → push} over hours has three failure
modes that all look exactly like success. Assume every one of them is present until checked:

1. **A push that pushes nothing.** The reset+reapply cycle (required here because minified
   single-line JSON never rebases cleanly) is fragile in both directions:
   * `git checkout <commit> -- <paths>` with ONE bad pathspec fails the WHOLE command. Listing
     an untracked scratch script there left the ledgers at their just-reset (stale) state, so
     re-apply found nothing new, the staged diff was empty, and the driver logged "reconciled
     with upstream, nothing new to push". Only TRACKED paths belong in that list, and its exit
     code must be checked.
   * Dropping the `git reset --hard origin/main` itself (an edit fixing the above deleted it)
     turns the whole retry block into a no-op with the identical "nothing to push" message.
   **Push return codes, empty staged diffs and local commits ALL lie.** The only honest check
   is to read the artifact back out of `origin/main` (`git show origin/main:<ledger>`) and
   confirm it contains this batch's work. Do that before advancing any cursor.
2. **A guard that isn't running.** A bare relative filename passed to a helper the driver runs
   with `cwd=ROOT` resolves against ROOT, not `scripts/`. The year-shift poison scan therefore
   read a nonexistent ledger, got `{}`, and printed `clean (0 landed cells checked)` for all 27
   chunks. **"Clean (0 checked)" is not clean — it is not running.** Guards must exit non-zero
   when their input is missing, and resolve bare names against their own script dir.
3. **"ALL COMPANIES PROCESSED" means the CURSOR reached the end, not that work happened.** A
   harvester crash (exit 1) fell through a stop-gate that only tested `rc == 2`; the driver ran
   the rest of the chunk against an unchanged ledger and advanced the cursor past 20
   never-harvested companies. Stop on `rc != 0`, not on one expected code.

**The standing close-out check for any batch campaign** — cheap, and the only thing that
actually proves coverage: reconcile the LEDGERS against the UNIVERSE.
`touched = set(reads) | {k.split("|")[0] for k in attempts}`; anything in the universe and not
in `touched` was never visited, whatever the logs said. Then assert
`landed + refused == universe` so every cell is either data or a named refusal. This found 24
missing companies (279 cells) and 123 invisible cells that no log line mentioned. Corollary:
a harvester must record a refusal on EVERY path that abandons a cell — a bare `continue` in a
per-year loop makes those cells invisible to exactly this reconciliation.

---

## 39. ★ SHIP-IT QUALITY GATE — nothing goes out unverified  (2026-07-28, STANDING USER RULE)

**THE RULE (user, 2026-07-28):** *"whenever u do any ui development, design or feature or anything
make sure its bug free."* Said after too many changes landed broken and the user had to come back
and report them. **A change is not "done" when the edit is written — it is done when it has been RUN
and SEEN WORKING.** "Looks right" is not evidence. Applies to every UI tweak, new page, new feature,
refactor, script change — no size exemption; the one-line "obviously safe" fix is exactly the one
that has shipped broken before.

### The gate — run ALL of these before saying "done" or pushing

**0. Syntax / import gate (every file touched).**
```
node --check docs/<file>.js                    # JS
python -m py_compile scripts/<file>.py         # Python
python -m json.tool <file>.json > /dev/null    # JSON
python -c "import <module>"                    # heavy-import scripts — §15/§18 smoke tripwire
```
⚠️ A syntax error in `theme.js` / `theme.css` takes down **every page on the site** (§34) — they are
injected site-wide. Never push those unchecked. Never let `|| echo` swallow a failure in a workflow
(that hid a dead BSE feed merge for weeks — §15/§18).

**1. Actually load the page.** `preview_start` → `read_console_messages` (must be **zero** errors) →
`read_page` and confirm the new thing is really in the DOM **with real values** — not `Loading…`,
`NaN`, `undefined`, `null`, `—`, or an empty table. Reading your own diff is not verification.

**2. Blast radius — everything else that uses what you touched.**
- `theme.js` / `theme.css` / nav / footer / tiles → **all pages**; spot-check ≥3 (home, a table page, a chart page).
- Shared JS (`sw-sync.js`, `sw-watchlist.js`, `backtest-engine.js`) → `grep -rl` every consumer, open each.
- `stock-backtest.html` has its **OWN inline engine** — any factor/logic added to one MUST be ported
  to `backtest-engine.js` and vice-versa (memory: feedback-backtest-engines-sync).
- **A constant duplicated into two files needs a GENERATOR, not discipline.** `FUND_ALIAS` lives in
  both engines and silently fell months behind `_rename_map.json` because nothing regenerated it —
  hand-syncing a duplicate works exactly once. Any constant derived from a file that keeps growing
  gets a `--write` generator plus a nightly check (`scripts/check_fund_alias.py`, §30 step 4c).
- Changed a JSON's shape → check every reader of that JSON, plus the writer that bakes it.
- New page → `NAV_GROUPS` in theme.js + an `index.html` blurb (§34), else it exists but is unreachable.

**3. The unhappy paths — this is where site bugs actually live, not the happy path.**
Empty array · exactly 1 row · all-null column · missing key · stock with no history / pre-IPO date ·
string-vs-number filter compare · empty basket · divide-by-zero · **negative year-ago base**
(feedback-negative-base-growth) · a renamed ticker whose price bin still lags (§30) · a user with
nothing saved yet · first visit with no localStorage · a strategy that returns 0 picks.

**4. Mobile + theme (a UI change is not verified until both are).**
`resize_window` mobile (375px): the **`<body>` must not scroll sideways** and the fixed bottom bar must
stay put (that exact drift shipped 2026-07-28); tables must cardify ≤640px with **colspan cells set
`display:block`**, else the card collapses. Then check **dark AND light**.

**5. Cache.** Any changed `docs/*.js|css|html` asset ⇒ **bump the service-worker CACHE version**, or
returning users keep running the old broken file (the Android SW cache even survives a reinstall).
⚠️ **Read the CURRENT version off ORIGIN, never off your checkout** —
`git show origin/main:docs/sw.js | grep '^const CACHE'`. An interactive checkout can be hundreds of CI
commits stale, so "local v71 → bump to v72" can land a v72 that ORIGIN ALREADY SHIPPED with different
content — same cache key, different bytes, and every warm browser is frozen on the old shell until the
next bump. Caught before pushing on 2026-08-09 (checkout was 439 commits behind; origin was already v72).
Same rule for any other monotonic counter in a tracked file (`sv=` page revs, `rev` cache keys —
[[project-stocks-sf-cache-key-rev]]).

**6. Verify LIVE after the push, not just locally.** `curl -s <live URL> | grep …` once Pages deploys
(~1–3 min) — the browser pane is flaky for this. Data heals: re-check **~20 min later**, an in-flight
CI run can clobber you (§0, §38).

### Report honestly
State exactly what was verified and what was not — *"console clean on 3 pages, mobile + dark checked;
NOT tested with an empty watchlist."* Never call something "working" that was only read, never run.
A named unverified corner costs one sentence; the same corner found by the user costs their trust.

### Bugs that actually shipped, and the check that would have caught each
| shipped bug | gate that catches it |
|---|---|
| page panned sideways, bottom bar slid off screen (2026-07-28) | **4** — mobile viewport |
| deleted strategies resurrected for everyone (fixed 9233f43) | **3** — stale local flag + self-heal path |
| Live Picks / holdings anchored to `SF.end`, missing same-day filings & prices | **3** — verify vs real current data, not the baked snapshot |
| backtest saved the same strategy twice | **2** — two writers of one record |
| BSE feed merge silently dead in CI (top-level heavy import + `\|\| echo`) | **0** — import smoke test, no swallowed errors |
| baked `FUND_ALIAS` months behind `_rename_map.json`; renamed stocks silently null-profit (2026-08-10) | **2** — a duplicated constant needs a generator + nightly check, not hand-syncing |
| SW cache version bumped off a stale checkout, colliding with a version origin already shipped | **5** — read the current CACHE from `origin/main`, not the local file |
| Quarterly Results stat tiles WHITE in dark theme for a month (`var(--surface-1,#fff)` — token never existed, #fff fallback won in every theme; invisible in light/soft) | **4** — dark check, plus: grep every `var(--…)` a page uses against the tokens theme.css actually defines; an undefined token silently renders its fallback |
| old renamed symbol dead-ended stock.html ("not found", stuck Loading line) while FUND_ALIAS had drifted 71 vs the rename map's 797 | **3** — renamed-ticker path with a symbol renamed AFTER the alias map was last touched, not one already in it |
| `search_index.json` shipped `"v": ""` for weeks — the only staleness stamp it carries — because the meta scanner needle-searched `"end":` and the LIVE payload puts `end` LAST (the committed bin puts it first, so it passed locally and failed only in CI) | **new** — (a) a streaming reader must be run against the file the JOB actually reads (here: the 193 MB release asset, fetched and scanned; key order was `data, meta, start, dailyFrom, end`), never only the copy sitting in the checkout, which can differ in *shape* and not just in age; (b) a field that stamps freshness gets a hard **ABORT** when it comes out empty — publishing it blank turns "unknown age" into "looks fine" for every consumer downstream |
| Indices 1Y column read "0.0%" for a year-old index — NSE's `perChange365d` 0 is a NO-BASE SENTINEL, not a flat year (§37a) | **new** — for any *derived* number a feed hands over (a change, a ratio, a growth %), check whether the feed also publishes the **base**; if the base is missing/zero the derived value is **unknown**, never 0. An exact `0` in a change column is the shape this bug takes: unlike null it passes every non-null check and renders as fact. Gate on the CONJUNCTION (exact 0 **and** absent base) — an absent base alone can still carry a real number (NIFTY50 USD: no base, real −7.14) |

### If a bug ships anyway
Fix the **class**, not just the instance: ask *"what check would have caught this?"* and add it to the
gate above. That is what keeps this list from growing.

---

## 40. STOCK PAGE = PER-STOCK SLICES  (docs/stock.html first paint, built 2026-07-28)
**Problem it fixed:** opening any company (`stock.html?sym=RELIANCE`) booted the shared backtest
engine, i.e. the WHOLE market, before it could draw one line: `stock_data.bin` 17 MB +
`sf_stock_data_1/2.bin` 115 MB + `sf_fundamentals.json` 3.2 MB + `sf_revop.json` 3.9 MB +
`shareholding.json` + `shp_engine.json` — **~137 MB and a ~250 MB JSON.parse per visit**, for one
stock. Worst of all, 104 MB of that was `stock_data.bin`'s `series`, which `activateSF()` threw away
immediately; the page wanted only its index membership and market cap.

**Now:** two pre-cut files per stock. Cold page ≈ **37 KB of data** (~3,600× less), one round trip
each, both fetched in parallel with the tiny `nifty.json`.

| file | what | built by | published to | cadence |
|---|---|---|---|---|
| `stk/<SLUG>.json` (~15 KB gz) | prices, meta, index chips, mcap | `build_stock_slices.py` | **sf-data** repo (force-push) | daily, every stock |
| `docs/fin/<SLUG>.json` (~2 KB gz) | profit, revenue/margins, shareholding | `build_stock_fin.py` | **this** repo (committed) | on every results commit |

**Why two files and two homes — do not "simplify" this into one:**
- Price slices change for EVERY stock EVERY trading day → committing them here = ~40 MB/day of new
  blobs forever. They ride the existing sf-data force-push (fresh single commit, no history).
- Financial slices change for the ONE company that just filed → git stores only that blob, so they
  CAN be committed, and `refresh-stock-fin.yml` (push-triggered on `sf_fundamentals.json` /
  `sf_revop.json` / `shareholding.json`) rebuilds them minutes after any results workflow lands.
  Folding them into the daily price push would leave the quarterly table up to 24 h stale mid-season.

**The rule that keeps it honest:** a slice carries the upstream arrays **verbatim**, and the client
(`installStockSlice` in `backtest-engine.js`) applies the exact transform `loadSF()` applies. Never
re-derive values in the builder. First cut did, and it cost real accuracy — rounding turnover to an
int put a 40% error on penny stocks (TVVISION 0.957 → 0.571 ₹ lacs) and converting the per-mil
high/low into x100 paise moved 52-week lows on sub-₹10 names. Passing `hb/lb`, `dv`, `t` through and
letting the page divide/expand them made all 517 probe symbols match to the last decimal.

**Tail arrays:** `h/l/v/dv/t` ship only for the last `k=400` bars — the deepest window anything on
the page looks back is 52 weeks. `installStockSlice` pads the head so indices stay aligned with `d`;
pad value is "no intraday range known" (high=low=close, or a 0 per-mil offset) and **never null**,
which would poison the 52-week low. Full daily `d`+`p` are kept — the chart's Max range and the
since-inception/CAGR rows need them (SF history is weekly pre-2020, daily after, so it stays small).

**SLUG** = symbol with anything outside `[A-Za-z0-9._-]` → `_` (23 symbols carry `&`/`+`: M&M, L&T,
SRERAYHY+H). Same rule in both builders and in `slugSym()`; builders abort on a collision.
Renamed tickers get a fin slice under the OLD name too (TATAMOTORS → TMPV's financials), mirroring
`fundFor()`/`FUND_ALIAS` — without it every renamed stock's page showed "no quarterly earnings".

**Fallback:** no slice (a listing newer than the last build) → the page loads the full engine exactly
as before, with the old overlay message. Correct, just slow. Nothing to fix by hand.

**Verify after changing either builder or `installStockSlice`** — this is the whole safety net:
```
python scripts/build_stock_slices.py --out docs        # writes docs/stk/ (gitignored, local only)
python scripts/build_stock_fin.py
node <scratch>/equiv.js     # loads the REAL loadSF() path + the slice path in one vm, compares
                            # 25 metrics per symbol over ~500 probes incl. delisted/penny/ETF names
```
The harness is the reference, not a re-implementation: it feeds probe symbols to the engine's own
`loadSF()` through a stubbed `fetch`, so any drift between the two paths shows up as a mismatch.

**Gotchas:**
- `docs/stk/` is **gitignored** — it exists locally only to serve the page from `localhost`
  (`SLICE_BASE` switches to `./stk/` off localhost/file://). Production reads it from sf-data,
  which is the SAME origin as the site (both `dhruvan246.github.io`), so there is no CORS hop.
- The service worker never caches `.json` and ignores cross-origin, so slices are always fresh.
- Slices depend on `docs/stock_data.bin` for `startTs` + index/F&O membership + mcap. It is still
  committed and still needed by other pages — the STOCK page just no longer downloads it.

### 40b. ★ REPORTING BASIS — one basis per COMPARISON, always  (2026-08-06)

`stock.html` has a **Consolidated / Standalone** switch above the quarterly table (`#fundBasis`,
remembered in `localStorage.sw_finBasis`, default consolidated). It drives `FB_EFF`, which the
🧾 Financial-detail card reads through `xCell()`, so the whole page moves as one. The switch only
renders when the stock HAS both bases, and a remembered `std` silently reverts to `con` on a
consolidated-only stock — never leave the user on an empty table they didn't ask for.

**The rule that matters, and it is not cosmetic:** a LEVEL may fall back across bases (show the
consolidated figure, else the standalone one — coverage wins), but any **comparison** — YoY, QoQ,
a margin, any ratio of two cells — must take **both operands from ONE basis**: consolidated when
both have it, else standalone for both, else print nothing. Consolidated revenue mostly starts
**2020** while standalone runs back to **2015**, so the naive "consolidated preferred, per cell"
pick silently compares this year's GROUP against last year's PARENT and prints group-sized growth
that never happened. Measured across `docs/fin/*.json` (last 12 quarters, 2026-08-06) before the
fix: **5.4% of revenue YoY cells and 3.2% of PAT YoY cells, 731 stocks.** Same trap for OPM
(consolidated op profit ÷ standalone revenue is not a margin of anything).
`quarterly-results.html` already did this right — `growth()`/`opm()` walk a basis order `['c','s']`
and bail rather than mix. Copy that shape; do not invent a new one. (Pairs with the ABS-base rule
for a negative year-ago base — `(cur−base)/|base|`, memory `feedback-negative-base-growth`.)

**Disclose the mix.** The Basis column is `con` / `std` / **`mix`** — `mix` means the row uses
consolidated for the figures filed that way and standalone for the rest (typically consolidated PAT
with standalone revenue, the 2015-19 shape). Labelling such a row `std`, as it did before, hides a
consolidated profit behind a standalone label. Anything that shows a per-row basis owes the same.

---

## 41. ★ PUBLISHING A DATA HEAL — "live on the server" ≠ "the site uses it"  (2026-08-02, learned the hard way)

**The trap.** A heal/backfill (delivery ledgers, corp-action fix, rename merge, scale fix) rewrites
HISTORY without adding a trading day, so `sf_meta.json {end}` does **not** change. The browser caches
the two 50MB+ `sf_stock_data_{1,2}.bin` parts in **IndexedDB keyed on that version string**. Keyed on
`end` alone, every client that had already cached that date kept serving **pre-heal bytes** — forever,
until the next trading day happened to bump `end`.

**What it cost (2026-08-02):** the 2002-2019 delivery backfill was verified correct at every server
layer — release asset, sf-data parts, month-by-month coverage — and was still **invisible on the site**.
A `Delivery % >= 60` backtest from 2003 qualified ZERO stocks before ~2020 and drew a flat equity line.
The user found it by running a backtest; no monitor, test, or verification step had noticed.

**The fix that is now in place (don't regress it):**
1. `split_sf_data.py` writes `sf_meta.json = {"end":…, "rev": sha1(payload1+payload2)[:10]}` — a
   fingerprint of the **uncompressed** data, so it changes on any content change and ONLY on one.
   ⚠️ It hashed the *gzip bytes* for the first few hours and that was wrong: `gzip.compress` stamps
   the current time into its header, so two rebuilds of identical data produced different revs and
   would have made every client re-download ~115 MB for nothing (caught by diffing two published
   parts: same size, same payload sha1, different header bytes 4-7). The parts are now written with
   `mtime=0` as well, so an unchanged rebuild is byte-identical and ETag/CDN-friendly.
2. `backtest-engine.js` **and** `stock-backtest.html` — which carries **its own copy of `loadSF`**;
   patch BOTH, always — key the cache on `end + ':' + rev`, degrading to end-only if `rev` is absent.
3. `sw.js` CACHE bump, or the new loader never reaches anybody.
4. **Workflow step "Verify clients will see this build"** polls the live
   `dhruvan246.github.io/sf-data/sf_meta.json` until it serves the rev this run just built, and FAILS
   the run otherwise (also fails if `rev` ever disappears from the split output). A silent staleness
   bug is now a red CI run.

**Checklist for any future heal — the last two are the ones that got skipped:**
- [ ] Ledger-based fix (§5 golden rule), applied fill-only + idempotent.
- [ ] Local pre-publish gate: re-score the invariant on the patched bin BEFORE publishing.
- [ ] Publish (release asset + sf-data parts) and re-score the **downloaded live** file.
- [ ] **Confirm `rev` CHANGED** on the browser-facing `sf_meta.json` (`curl` it; Pages lags the push by
      1-5 min). Never fake `end` forward to force a refresh — that lies about the data date.
- [ ] **Load the actual page and see the healed data in a result**, not just in the file. "The bytes are
      correct" and "the feature works" are different claims (§39).

### 41b. ★ A CI JOB'S OWN COMMIT DOES NOT TRIGGER THE PAGES DEPLOY  (found 2026-08-06)

`refresh-stock-fin.yml` rebuilds `docs/fin/<SLUG>.json` — the per-stock slice `stock.html` actually
reads — on every push that touches `docs/sf_revop.json`. It works, it commits, and the commit is on
main within minutes. **But the site does not serve it.** `actions/checkout@v5` with no token argument
pushes as the default `GITHUB_TOKEN`, and GitHub deliberately does not let a `GITHUB_TOKEN` push
trigger further workflows (the recursion guard). `pages.yml` is `on: push: paths: docs/**` — so the
slice commit lands and no deploy fires. The slices go live only when some LATER push to `docs/**`
from a PAT-authenticated job or a human happens to deploy the whole tree.

Symptom to recognise: `git show origin/main:docs/fin/<SLUG>.json` HAS the healed value while
`curl` of the live URL (cache-buster and all) does not, and the newest `pages.yml` run's headSha is
the commit BEFORE the slice commit. That is not CDN lag and waiting will not fix it.

Immediate fix after any rev/PAT heal: `gh workflow run pages.yml` (workflow_dispatch is enabled) —
it publishes whatever is committed and is safe to repeat.
**This is §41's own lesson recurring one layer out** — the bytes were right in git, the derived slice
was right in git, and the site still served nulls.

**FIXED IN THE WORKFLOW 2026-08-11 (2bd29409) — no more hand-kicking.** `refresh-stock-fin.yml` now
carries the same trailing `gh workflow run pages.yml` the other ~10 refresh workflows already had
(`actions: write` + `GH_TOKEN: ${{ github.token }}`, dispatch INSIDE the push retry loop right after
a successful push, exactly as in `refresh-fii-dii` / `refresh-announcements`). `workflow_dispatch` is
the documented EXCEPTION to the recursion guard, which is why a GITHUB_TOKEN job can dispatch a deploy
it cannot trigger by pushing. A PAT was NOT needed; neither was a `workflow_run` trigger.

Place the dispatch INSIDE the loop, never after it: the loop retries a rejected push up to 5×, so a
dispatch after `done` would fire once per attempt. Inside, the two `exit 0`s make it fire **at most
once per run and only when a commit actually landed** — the `git diff --cached --quiet` branch leaves
first (empty run ⇒ no pointless deploy), and the push branch exits immediately after dispatching.
That is the whole burst guard; there is no `if:` condition to add. Verified before pushing by running
the extracted step against stubbed `git`/`gh`: unchanged ⇒ 0 dispatches, push-ok ⇒ 1, push-rejected-
twice-then-ok ⇒ 1 (not 3), push-fails-5× ⇒ 0 and the run goes red.

Live proof (run 31513447604, 2026-08-11 22:08 IST): slice commit `3490d369` pushed at 16:38:42Z →
pages run 31513485523 created 16:38:43Z **with headSha `3490d369`** — the slice commit's OWN sha, which
had never once appeared as a deploy headSha before. It was then cancelled as superseded by a deploy
18 s newer whose tip CONTAINS it (that is `pages-deploy`'s designed coalescing, not a failure: a later
dispatch always sits on a tip ≥ ours, so the content still ships), and the live URL served the slice
commit's exact bytes by 22:10. Expect this shape — "our dispatch was cancelled" is a healthy log line
as long as a newer deploy carrying the commit succeeded.

Rate check before worrying about the wedge hazard (§38b / memory `project-stocks-pages-wedged-
concurrency`): measured 2026-08-11, `pages.yml` already ran ~100×/day, 69 of them `workflow_dispatch`
vs 31 `push`, some 16 s apart, with zero non-completed runs. Slice commits peak at ~78/day
(2026-08-04), and `refresh-stock-fin`'s own `cancel-in-progress: true` collapses upstream bursts
before they reach the dispatch. So this rides an existing pattern rather than inventing load.

**§41b-i — NEVER park the publish-wait poller inside a worktree you are about to delete** (2026-08-10,
cost: six runaway loops, one of them spinning for 7h55m). The standard verify step is backgrounded as

    cd ~/stocks-wt/<job> && until [ "$(gh run list ... --jq '.[0].status')" = completed ]; do sleep 20; done

Then the fix ships, `git worktree remove` runs, and the poller's working directory ceases to exist.
`gh` now fails EVERY iteration with `failed to determine base repo: ... Unable to read current working
directory`, the `until` condition can never become `completed`, and the loop retries forever, appending
the same error line every 20s (they had grown to 40–180 KB). They never report an error, because an `until`
loop treats "command failed" as "not done yet" — silence reads as patience.

Three rules for any backgrounded wait:
  * **`cd` somewhere that will outlive it** — the MAIN checkout, or pass `-C <path>`/`--repo owner/name`
    to `gh` so it needs no cwd at all;
  * **bound the loop** — `for i in $(seq 90); do ... done` beats `until`, so a wedged wait dies on its own;
  * **before `git worktree remove`, kill your own pollers** — `ps -eo pid,command | grep shell-snapshots`
    lists them; anything older than the job itself is a leak.
Check for leaked pollers whenever a session has shipped several fixes: they cost nothing visible but
poll GitHub forever.

**Generalise it:** any client-side cache keyed on a DATE will go stale under a heal. Today the sf parts
are the only such cache (`dash_slim.bin` / `stock_data.bin` / `mf_history.bin` / `stk/` slices are plain
ETag fetches that revalidate within ~10 min, and `sw.js` never caches `.bin`/`.json`). If you ever add
another IndexedDB/localStorage data cache, key it on CONTENT, not on the data date.

## 42. ★ BSE DETAILED-RESULTS JSON — the as-filed 2015+ quarterly source  (discovered 2026-08-02)

Every quarterly result BSE ever displayed on its old "detailed results" pages is served as
STRUCTURED JSON, back to at least Mar-2015 (and 2014 responds too), including DELISTED and
SUSPENDED scrips and the &-symbol companies whose NSE archive pages are 0-byte (J&KBANK, M&MFIN):

    https://api.bseindia.com/BseIndiaAPI/api/Corp_detailedResult_Transpose_ng/w?scrip_cd=<CODE>&qtr=<QID>
    QID = "NN.00" where NN = 85 + 4*(Y-2015) + {Mar:0, Jun:1, Sep:2, Dec:3}   (85 = Mar-2015)
        ★ Y is the CALENDAR year of the quarter END, not a fiscal-year label. This line used to
          read "FY-2015", which is the same number for MARCH quarters (so every Mar example
          worked) and a year WRONG for Jun/Sep/Dec. Measured 2026-08-10: the fiscal reading sent
          Jun-2020 to 110.00, whose response declares `Date Begin 01-Apr-21 / Date End 30-Jun-21`.
          Jun-2020 is 106.00. This is exactly why the period check below is not optional.
    QID = "NN.50" on the fiscal-year-END quarter = the audited ANNUAL row
        ★ and the fiscal year is NOT always Apr-Mar — KENNAMET's ends 30 JUNE, so its annual row
          is the JUNE .50, and asking for a March .50 correctly returns nothing.
          (Mar for Apr-Mar filers; DEC for calendar-year filers like AMBUJACEM/ACC)

Facts that matter:
* Values are AS-ORIGINALLY-FILED (verified: SHRIRAMFIN Mar-18 = IGAAP 144.6 not the Ind-AS
  restated 961.76; NSE's matching list row declares "indAs":"Non-Ind-AS"), in ₹ MILLION — ÷10.
* Bank format is first-class: "Interest Earned/...", printed "Operating Profit Before
  Provisions and Contingencies", NPA/CAR rows. Industrial rows allow the op reconstruction
  (pbet + |finance| + |depreciation| − other income).
* EPS + Equity Capital + Face Value rows → EPS×(equity/FV) reconstructs owners-PAT: the gate
  for PAT fills where no stored anchor exists (±6%).
* ALWAYS verify Date Begin/End span == 3 months (annual/H1 rows exist in the same id space).
* Standalone/primary basis only. No working consolidated endpoint found (2026-08-02).
* ★ The sibling announcements endpoint `AnnSubCategoryGetData` (used to find a scrip's result
  filings and their attachment PDFs) returns **at most 50 rows for `pageno=1`**, silently. A
  12-month window on a busy filer therefore TRUNCATES, so a count of "filings mentioning X in
  that window" is a LOWER BOUND and is never evidence of absence (measured 2026-08-10, §73b).
  Page through it, or narrow the window, before drawing any conclusion from a small count.
* Scrip codes for dead companies: ListofScripData/w?...&status=Delisted|Suspended|(blank=all
  10,786) — scrip_id equals the NSE symbol for most; resolve the rest by name.

Discipline (same as every backfill route): a cell lands ONLY if the page NP (÷10) matches the
stored sf_fundamentals PAT for (sym,qe) on some basis within max(2cr,3%); PAT fills need the
EPS-recon or the FY-consistency gate (candidate + other three quarters ≈ the .50 annual within
max(3cr,3%) — this also PROVES the basis). Derivations use the .50 annual, never Screener's
annual: Screener silently swaps in RESTATED annuals (Ind-AS transitions, mergers, fiscal-year
changes) and mixed-basis residuals pass ratio gates — that poisoned 20 live cells on 2026-08-02
(purged via scripts/rev_defects.json, the guarded null ledger that lets the fill-only appliers
refill from corrected reads).

Tooling lives in the rev-mission worktree (scripts/_bse_detres.py, _bse_annual_derive.py,
_patfill_gate2.py, _anchor_adjud.py, _mk_gaps_2015.py); ledgers rev_defects.json /
sanity_ok.json / pat_defects.json / xbrl_comparative_fills.json are tracked. sanity_ok.json
exists because revop_sanity's 4x scale-spike rule misfires on IBC-collapsed companies
(UTTAMSTL/GAMMONIND) when backfilling their healthy years — allowlist reviewed cells there.

## 43. ★ INSURER QUARTERS FROM IRDAI PUBLIC DISCLOSURES  (route solved 2026-08-03)

Insurers file IRDAI-format disclosures, not the standard exchange P&L, so they are invisible to
every other route before ~2019 (BSE serves no attachment pre-2019, NSE's archive has ZERO
insurer rows, the BSE detres JSON of §42 carries no IRDAI forms). Their own websites publish
the quarterly L-forms (life) / NL-forms (general) going back a decade — that is the source.

**Access.** `pip install curl_cffi`, then `requests.get(url, impersonate="chrome")`. ICICI Pru
and ICICI Lombard hard-403 plain urllib (Akamai fingerprints TLS, not headers) and are blocked
in BOTH browser surfaces (the Chrome extension refuses the domain by policy; computer-use grants
browsers read-only tier). Impersonation returns 200 on every insurer site tried.

**Where the archives are** (all verified): hdfclife.com/about-us/public-disclosure (static links,
FY2016+) · iciciprulife.com/about-us/investor-relations/yearly-public-disclosures.html?ID=about3
(3,585 links; folders FY<yr>/fy<yr>q<n>s) · icicilombard.com/about-us/public-disclosure (3,071
links to 2010; folder `2016-17` not `2016-2017`, and the `?sfvrsn=` query param is REQUIRED) ·
gicre.in/periodicdisclosure/<FY>/<n>th-qtr/NL-1-Rev-Acc.html (HTML tables, not PDFs) ·
newindia.co.in CMS uuid paths — enumerate with the wayback CDX API, then fetch them LIVE.

**Reading the forms.** Figures are ₹ THOUSANDS (÷10,000 → crore). Columns run
[For Qn current | Upto Qn current | For Qn prior | Upto Qn prior] so a row's quarter value is
the 4th-from-last number — but ALWAYS confirm the column by anchoring the P&L's profit-after-tax
against stored sf_fundamentals, because some packs print only cumulative columns (then difference
consecutive packs). Newer PDFs put labels and figures in separate text blocks: parse positionally
by word y-coordinate; older ones keep them on one line. GIC Re's revenue account is FOUR segment
tables with no total — sum them.

**Revenue convention (reverse-engineered, validated to the paisa):**
  general insurers: policyholders' [premium earned (net) + profit on sale/redemption of
  investments + interest, dividend & rent] PLUS **shareholders' [interest, dividend & rent +
  profit on sale − loss on sale]** taken from the P&L. Other income (fx gain, terrorism-pool,
  misc) is excluded. Validated ICICIGI Sep-2019: parsed 2854.12 vs stored 2854.12 (delta 0.00),
  GICRE Mar-2018: 8835.31 vs 8834.04 (0.014%).
  life insurers: net premium + policyholders' investment income only.
Always validate a NEW insurer's parser against a quarter that is already stored before writing
anything — the conventions differ between life and general, and that check is what caught the
missing shareholders' leg.

Tooling (rev-mission worktree): `_irdai_life.py`, `_irdai_gi.py`, `_irdai_gicre.py`,
`_irdai_niacl.py`; packs cached under `scripts/_irdai/<SYM>/`.

## 44. ★ DELISTED-BUT-STILL-FILING: the BSE DEBT segment  (found 2026-08-03)

When a company is delisted from EQUITY but keeps listed NCDs, SEBI Reg 33/52 still oblige it to
publish quarterly results — but they are filed against its DEBT scrip code, so every equity-side
route (BSE announcement API by equity code, BSE detres table, NSE corporates-financial-results,
NSE integrated-filing) reports "no filing" and the quarter looks structurally dead. It is not.

Recipe: `ListofScripData/w?...&segment=Debt&status=` returns 64,017 rows; find the issuer by
`Scrip_Name`, take the codes whose status is not Delisted, and run the normal announcement
window against them. ISEC (ICICI Securities, merged into ICICI Bank and delisted March 2025)
files under debt scrip 729001 — Q4FY25 through Q4FY26 all recovered this way, each anchored
exactly against the stored PAT on both bases.

Two traps in these filings:
* the P&L pages are scanned with a garbled text layer (labels readable, figures in a separate
  flow) — parse POSITIONALLY by word coordinates, as with the IRDAI forms in §43;
* standalone and consolidated PAT can differ by less than the anchor tolerance (ISEC: 473.31 vs
  474.59), so a single page will satisfy BOTH bases and silently duplicate itself into the con
  slot. Require a DISTINCT page per basis and pick the page whose value is CLOSEST to each
  stored figure; if both bases resolve to the same page and the same numbers, keep standalone
  only. (This bug produced identical std/con rows before it was caught.)

Applied through `scripts/_debt_reads.json`, wired into `_apply_reads.py`.

## 45. ★★ THE FY QUARTER-SUM IDENTITY — how to prove which side of an anchor mismatch is wrong

When a filing's PAT does not match the stored PAT, the cell is refused — correctly, but that
leaves you not knowing WHICH side is wrong. The decisive test, using only the BSE detres source
of §42: fetch all four quarters of the fiscal year plus the audited annual (`.50` sub-id), then
sum each series.

    as-filed quarters sum == audited annual   ->  the STORED series is defective
    stored quarters sum   == audited annual   ->  the read is wrong; leave the cell alone
    neither matches                           ->  usually a standalone/consolidated mix; REFUSE

This is proof, not inference — an audited annual is not free to disagree with its own quarters.
Cases it settled (2026-08-03), each of which had resisted every extraction route:
* SYNDIBANK Dec-2018 stored **0.00** (a sentinel): as-filed quarters sum to -2588.30 = the FY19
  annual exactly, and the stored series missed by precisely 107.99 — the disputed cell. The same
  zero had ALSO caused an earlier FY-consistency gate to reject Mar-2019 (128.02) as inconsistent,
  so one bad cell was blocking two.
* ITI FY18: as-filed 230.56 vs annual 230.55 -> stored Sep-17 (7.56) and Dec-17 (13.58) both wrong
  (true 46.11 / 76.24).
* IIFL FY18: as-filed 204.35 == annual exactly -> stored Dec-17 23.44 wrong (true 33.07).
* COX&KINGS Jun-2015: annual reconciles to 0.47% with the as-filed value vs 4.7% with the stored
  one, and EPS x shares independently gives 77.71 vs the page's 77.64.
  ⚠️ Its verdict FLIPPED after two other FY16 quarters were healed — the test depends on the rest
  of the series, so re-run it after any heal in the same fiscal year.

Refuse rather than force when neither side reconciles (GODREJPROP FY18, MOTILALOFS FY18,
SUDARCOLOR FY16 all failed both ways — the annual there is consolidated while the quarters are
standalone). Tool: `scripts/_fy_identity.py SYM:FY`.

Corollary worth remembering: a "revenue not served" cell is often not a document problem at all —
of the first six closed in this class, ALL six were blocked by a wrong or missing stored PAT
rather than by an unavailable filing.

⚠️ **COMPENSATING ERRORS PASS THIS TEST (BDL 2026-08-04).** BDL FY18: stored 81.82+103.49+6.18+336.66
== 528.15 audited EXACT — yet Jun-17 (81.82) was the Q1-FY19 value year-shifted back, and Sep-17
(103.49) had been DERIVED as H1-minus-that-poisoned-Q1. Errors constructed FROM the identity satisfy
the identity. Direct year-ago comparatives in the next year's filings (101.42 / 83.89, H1 = 185.31
in BOTH filings) are the only cure. So: quarter-sum agreement is NECESSARY, never SUFFICIENT — when
a cell's provenance is a derivation (not a document), verify at least one leg against a direct column.

**IPO-cohort YEAR-SHIFT poison + the .50-restatement detector (2026-08-04, ASTERDM/BDL).**
Detector: scan sf_fundamentals for PAT(q) == PAT(q+10000) exactly with |v|>0.5 — consecutive-year
identical PAT is the fingerprint of comparative columns landed into the wrong year (~100 pairs
store-wide; microcap sub-1cr pairs can be genuine). Adjudication recipe, all from cached/free sources:
1. detres .00 both years — if the LATER year's page matches stored, the EARLIER year holds the copy.
2. True earlier-year values = the year-later filings' direct year-ago columns (std page), PROVEN by
   chaining to the detres `.50` audited annual: quarters must sum EXACT.
3. The same chain is a RESTATEMENT DETECTOR: comparatives that sum to the ORIGINAL audited total are
   as-originally-filed (accept); comparatives chaining to a DIFFERENT (restated) total are restated
   (refuse — for ASTERDM con the honest heal was NULL, expressed via pat_defects stored_pat_con/
   correct_pat_con=null, applier `_pat_defect_fix.py` supports the separate con pair).
Route heals through `pat_defects.json` (exact stored_pat guard) + rev via `_bsedet_reads.json` with
the corrected pat_seen as anchor — the batch chain orders defect-fix before reads-apply.

**Detector round-2 (2026-08-04): 188 slot-pairs adjudicated, ~110 cells healed, the rest triaged.**
Store-wide scan (std[1]+con[3] slots) found 188 exact-dup slot-pairs; three ledger batches
(b4dd72fb → batch-3b) healed them down to ~75 open. What the campaign established:

* **The poison mechanism is NSE's list double-indexing**: `corporates-financial-results` (and
  sometimes `integrated-filing-results`) lists the SAME result file under BOTH the current
  quarter's `toDate` row and the comparative quarter's — an importer keyed by toDate reads the
  file's OneD (current!) into the comparative year's cell. NEVER assign a file to a quarter by
  its list row; assign by the `_DDMMYYYYHHMMSS` filing timestamp in the filename (5..130 days
  after quarter-end) or the FY-declaration tags.
* **XBRL context IDs have NO fixed meaning across filers**: FourD = year-ago in one file
  (PIIND), YTD in another (GHCL), preceding-quarter in a third. Identify a context by
  VALUE-ARITHMETIC against trusted cells (YTD/PREV/FY-ENDED hypotheses; June-quarter files are
  gold — YTD==OneD there, so a second context is almost always the year-ago comparative).
  `ProfitLossForPeriod` can also be total-incl-NCI vs owners (HCLTECH 4237 vs 4235).
* **`scripts/yshift_genuine.json` (tracked)** holds the ~44 PROVEN-genuine coincidence slots —
  the scan must subtract it or it re-adjudicates HCLTECH/ELECON/CESC/AARTIDRUGS-1709/DISHMAN
  forever (HCLTECH con was ₹4,235cr flat YoY per the company's own release; ELECON con 87.72
  flat to the paisa, H1 arithmetic exact — exact-dup ≠ automatically poison).
* **The fingerprint often marks WHOLE-SERIES corruption, not one shifted cell**: pulling the
  pair-thread exposed LEHAR (lakh-as-crore ×100 series), HAWKINCOOK (16 cells of -7.0/8.0
  sentinels + shifted values 2020-24, FY25-26 rows entirely absent), MAFATIND (2021-26 block
  scrambled + 9 missing rows), VOITHPAPR (next-year values shifted back + -0.5 sentinels,
  ends Mar-2023), TECHNVISN/NOVELIX (×100). After ANY heal RE-RUN the scan — the detector
  re-fires as each layer is removed (MAFATIND Mar-2023 surfaced only after Mar-2024 healed).
* **Fill-only appliers re-poison healed cells from stale harvest rows**: `_revgap_done.json`
  held mislabeled rows (a Jan-2024 announcement "containing" Jun-2024; a May-2025 one landing
  its CURRENT rev as Mar-2024 con) that re-filled the bad rev after every rev_defects null.
  When a null keeps reappearing, grep EVERY reads/harvest ledger for the cell and correct the
  SOURCE entry, not just the cell.
* Missing-row fills go through `xbrl_comparative_fills.json` and need BOTH forms: `"std"`
  (rev/op → sf_revop) AND `"pat_std"` (creates the sf_fundamentals row — the std form alone
  fills revop only).

Open queues (evidence in `_yshift_verdicts.json`/`_yshift_chain.json`, rev-mission worktree):
con truths living in BSE announcement XBRL (GHCL/ALOKTEXT/UTTAMSTL/CHENNPETRO-Jun+Sep-18/GPPL/
RATNAMANI/TI/PAISALO-con/FINCABLES class, ~15 slots); IPO-cohort pre-listing comparatives needing
the NSE-zip PDF pass (ADANIGREEN×3, AMBER-1706 chain-verify 25.62, RPSGVENT×4, ASTRON×4,
SPENCERS×2, KHADIM/SHALBY/DNAMEDIA/APOLLO/HITECH/INFOBEAN/MIDHANI-con); integrated-era con
stragglers (INGERRAND/EMERALL/MAJESAUT/OVOBELE/ARCHITORG); SHILPAMED-2015 (double-indexed archive
mess); MAFATIND pre-2021 + con-basis rebuild; TSFINV/AGROPHOS/AGLSL/SGFL scrip-resolution class.

**The ADJACENT-QUARTER twin of the year-shift poison (2026-08-10, AURUS).** The scan above only
pairs PAT(q)==PAT(q+1y), but the SAME list double-indexing also lands one filing's OneD into the
NEIGHBOURING quarter whenever a late filer submits two quarters in one document. AURUS Jun+Sep-2018
con both held 4.05 with the same ann 20181128: one combined con filing (XBRL
INDAS_41001_60087_14112018070010_WEB.xml, filed 14-Nov-2018, listed by NSE under THREE toDates —
30-Sep-2018, 30-Jun-2018, even 30-Sep-2017). Its two contexts carry DECLARED period dates:
OneD 2018-07-01..09-30 = 4.05 (stays at Sep; its rev 55.172 == stored revC exactly), FourD
2018-04-01..06-30 = 6.02 → Jun-2018 healed 4.05→6.02 via pat_defects. Detection fingerprint:
PAT(q)==PAT(q+1qtr) EXACT with the SAME ann date and |v|>0.5 — a store-wide scan (one loop over
sf_fundamentals) found **58 open pairs** (many Mar→Jun-2019 std; INFOBEAN/SASKEN/HPBL/KEIL/
RPSGVENT/SPENCERS overlap the IPO-cohort queue above). Adjudicate each per this section — declared
context dates beat every inference; identical-to-the-paisa + same ann date is the tell, but flat
quarters DO exist, so each pair still needs its document.

**CAMPAIGN CLOSED 2026-08-10: all 58 pairs adjudicated, 71 cells healed, re-scan = 0 open.**
Every pair got its own document (declared-period XBRL contexts / BSE detres span-verified rows /
as-filed result PDFs incl. NSE .zip attachments and vision reads of scanned ones); heals are
journalled per-cell in `pat_defects.json`, all three payload copies guard-edited, verify_fills_live
exit 0. What the campaign established beyond round-2:
* **Dominant shape (~40 cells):** one late combined filing, FourD DECLARES the Mar quarter and
  OneD the Jun quarter; the toDate-keyed importer wrote OneD into BOTH cells. Heal = FourD.
* **The dup can be FILER-side**: DHARSUGAR/ROLTA/EMKAY submitted byte-copies of a neighbouring
  quarter's XBRL under new dates — there the DECLARED dates themselves lie, and only YTD/9M
  identities plus the original PDF arbitrate (EMKAY's Aug refiling declared Jun's value under
  Jan-Mar dates; the May audited original + detres + FY identity outvoted it).
* **Genuine flats exist and are now ledgered** (yshift_genuine): AHLWEST (−1.2036 vs −1.1951 both
  print −1.20), BLAL (9M-identity-exact), TALWALKARS (liquidator filed −2.699 three quarters
  running; FY26 identity holds — as-filed stays).
* **Sign-lost class (SGFL ×4):** the xbrl pat tag can carry |value| while the SAME file's EPS tag
  keeps the sign — detres NP arbitrates; never trust an unsigned pat tag when EPS disagrees.
* **Basis catches en route:** RPSGVENT con stored TOTAL-incl-NCI for Dec-17/Sep-18/Dec-18/Jun-19
  (owners restored, EPS-row arbitrated per §2d); GARNETINT stores con TOTALS series-wide + its
  2019-20 std series is shifted one quarter (two cells healed then; **whole-series rebuild DONE
  2026-08-10 — see §45a**). DLF Mar/Jun-19 con were ×100-unit AND double-indexed (4.14 → 436.56 /
  414.72, unit proven by an exact total-overlap between two filings).
* **Rev co-poisons HEALED 2026-08-10** (commit 11e8263d; same mechanism, rev slots): GSFC revS
  20190331 1707.7→2138.42, CARERATING revS 20190331 45.22→81.49, ROLTA revC 20220630 3.76→5.08 —
  every value re-verified against refetched primary XBRLs, defects in `rev_defects.json`,
  corrected values in `std_rev_nse_reads.json`/`con_rev_nse_reads.json`. Fill candidates from the
  same documents also landed: SPENCERS Jun-18 std 2.22/con 0.62 + INFOBEAN Jun-18 std 5.05/con
  5.24 (new rows, `xbrl_comparative_fills.json` both forms), SPENCERS Mar-18 con −8.94
  (FY-derived, `con_pat_fy_derived.json`).

### 45a. ★★ THE WHOLE-SERIES REBUILD — GARNETINT, and the five defect classes one series can hold
NO ASSUMPTIONS, NO GUESSWORK: every value below was read off a BSE announcement PDF this session
and corroborated by a second document. GARNETINT (BSE 512493, no NSE list rows, detres empty) was
left OPEN by the adjacent-quarter campaign as "con TOTALS + a 1-quarter std shift". Rebuilding all
22 stored quarters found **five distinct defect classes in ONE 22-row series** — the lesson is that
a pair-detector hit is a thread, and the thread is usually attached to more than one knot:

1. **Wrong-basis, series-wide (20 cells).** Every con cell held the filings' printed TOTAL
   (incl. NCI) while the store's convention is OWNERS-attributable (`build_stock_fin.py`'s own
   docstring: "point-in-time quarterly net profit, OWNERS-attributable"). Converted whole-series.
   The basis is not cosmetic here: Jun-2020 **flips sign** (group −2.27 lakh vs owners +11.02, NCI
   −13.29), and Sep-2021 goes 0.24 → 0.01.
2. **Value-in-the-wrong-slot (4 cells).** Jun-19 std held Mar-19's value; Jun-19 con held Jun-19's
   *standalone*; Mar-19 con held Jun-18's *Ind-AS restated standalone*; Jun-18 con held Jun-19's
   con total. One quarter can be wrong in the row, the column, AND the statement at once.
3. **A YEAR figure in a quarter slot.** Mar-2023 con stored −5.98 = the **FY24 annual** total
   (−598.40 lakh) — wrong sign and ~16× too large; the real quarter is owners +37.80 lakh (+0.38).
4. **Lakh-as-crore (LEHAR class).** Sep-2024 con stored **56.45** — the filing's ₹ lakh figure in a
   crore slot (true 0.56). Cheapest tell: the company's whole-quarter revenue was 146.08 lakh, so
   a 56.45-crore profit is arithmetically impossible. Scan for |PAT| ≫ revenue before believing it.
5. **Absent rows (2).** Sep-19 and Dec-19 were missing outright — the shift had swallowed them.

**Ann dates can point at documents that are not filings.** Three were wrong: Jun-19's `20190805` is
a letter *deferring* the quarter to 14-Sep-2019 (first-time Ind-AS, SEBI circular
CIR/CFD/FAC/62/2016 cl. 2.6.1) with no numbers in it; Jun-21's `20210702` is a related-party
disclosure; Jun-20's `20200804` was the FY20 annual's date dragged along by the shift. Corrected to
20190914 / 20210814 / 20200918. **Before trusting a stored ann date, open the document it names.**

**When the filing contradicts itself, the arithmetic wins — and say which arithmetic.** Two printed
PAT figures here are refuted by their own columns:
* FY21 annual, Mar-21 quarter: printed total −70.09, but that column's own tax components
  (13.13 + 126.16 − 2.04 − 20.22) sum to 117.03, not the printed 137.25 — the MAT-credit line was
  dropped. PBT 67.16 − 117.03 = **−49.87**, which the FY21 quarter-sum identity confirms EXACTLY
  (−2.27 −81.13 −221.57 −49.87 = −354.84 = audited FY21) and which both later filings re-print.
* FY24 annual, Mar-24 quarter: printed total −444.06, but (owners + NCI) = −501.96 and
  (FY24 − 9M) = −501.96 for PAT, owners AND NCI independently. We store the **owners** row
  (−485.60), the one every identity agrees on.

**Two identities that will NOT reconcile here — by design, do not "heal" them.**
* FY20 Apr–Mar quarter-sum: the Q2FY20 filing RESTATED Jun-19 (std −1,290.61 → −534.30) and the
  audited FY20 chains to the restated series. The store is point-in-time on FIRST filing (the TRU
  rule), so the stored Jun-19 deliberately does not sum to FY20. Both values are in the ledger.
* FY21 owners-basis sum: the annual's year-column owners/NCI split (−372.73 / 17.89) differs from
  the sum of the as-filed quarterly splits (−387.53 / 32.70) by 14.80 — a year-end consolidation
  adjustment. Both sides are internally consistent (owners + NCI = total); only the TOTALS identity
  is exact. **An owners-basis FY check needs a tolerance the totals check does not.**

**EPS is a weak arbiter for a sloppy filer.** This one computes con EPS on OWNERS in Q1FY20 and on
the TOTAL in Q2FY20 — the same statement two quarters apart. Anchor on `owners = total − NCI` plus
the H1/9M/FY column identities; use EPS×shares only as a tiebreak (it did prove Mar-19 and Jun-19,
where share count moved 195.287 → 196.35 lakh mid-year on a warrant conversion + bonus issue).

**Unreachable ≠ unknowable.** Jun-2018's original filing is gone (AttachHis/AttachLive/
CorpAttachment all 404, Wayback 404, detres empty — the §52 pre-2016-style wall, here in 2018).
The value survives in the *next year's* filing: the Q1FY20 note-9 GAAP→Ind-AS reconciliation prints
Jun-18 con −1,762.49 lakh. A company's own restatement note is a primary document — walk §57's
ladder into the notes before writing a cell off.

Rebuild: 21 of 22 rows changed (Dec-24 unchanged — owners −18.98 and total −18.58 both round to
−0.19), 2 rows inserted, 3 ann dates fixed. Per-cell provenance in `pat_defects.json`
(22 GARNETINT keys, each naming its BSE GUID, column, and the identity that proved it); all three
payload copies guard-edited (22/22 guards held vs fresh origin/main, blast radius = GARNETINT only);
`verify_fills_live.py` exit 0, MISSING 0. Still OPEN for this symbol (measured, not yet stored):
std fills Mar-23 +8.39 / Mar-24 / Sep-24 / Dec-24 lakh-scale cells, absent rows Sep-22 (owners
64.78) / Jun-23 (15.49) / Jun-24 (76.82), and the whole FY26 stretch (Mar-25 → Mar-26 filings are
downloaded but unread).

**Legit structural breaks vs the scale-spike nuller:** demerger boundaries (CHOLAHLDNG Mar-2017 =
pre-demerger full Tube Investments, 1168.14 vs holdco-era ~65) and lumpy traders (OSWALGREEN Jun-2015
316.44, two independent as-filed docs) are REAL spikes — allowlist in `sanity_ok.json`, never widen
the nuller's tolerance. A read that keeps reappearing and getting nulled every batch is the tell:
grep the staged reads for cells the applier lands but the docs file never shows.

---

## 46. ★ BACKTEST DATA IS SPLIT BY DATE — quick runs never download deep history  (2026-08-03)

**Why:** the 2026-08-02 true-daily-since-2002 rebuild took the browser payload from ~94 MB to
192 MB, and EVERY page load parsed all of it — while the default window (2020-03-31) and all four
wave presets never read a bar before 2019-04. Loads and runs both roughly doubled overnight.

**Layout** (built by `scripts/split_sf_data.py`, force-pushed to the sf-data Pages repo — §1 step 4):

| file | contents | when the browser fetches it |
|---|---|---|
| `sf_recent_1.bin` (~78 MB) | bars ≥ `deepFrom` (2019-01-01) | always, on engine load |
| `sf_deep_1..N.bin` (~107 MB) | bars before `deepFrom` | ONLY when a run's window starts before ~2020 |
| `sf_meta.json` | `{end, rev, deepFrom, fullStart, dailyFrom, recent, deep, nTot, nDead}` | first, to pick the layout |

`deepFrom` = 2020-03-31 (earliest preset) − 365d lookback, with margin. No `deepFrom` key in the
meta = the LEGACY by-symbol `sf_stock_data_*.bin` layout; both loaders keep that branch so pages
can deploy ahead of a data flip.

**Client contract** (`docs/backtest-engine.js` + the inline copy in `docs/stock-backtest.html` —
keep in sync, memory `feedback-backtest-engines-sync`):
* `SF.start` / `nTot` / `nDead` come from the META, so date pickers and universe stats show the
  FULL dataset even while only the recent slice is in memory.
* `SF.dailyFrom` (2002-01-02) = where TRUE daily bars begin; pre-2002 is weekly and the oscillator
  family isn't meaningful there. The Full-history window starts here, NOT at `start` (1996).
* `ensureHistoryFor(startDate)` before `simulate()`/`screenAsOf()` — merge PREPENDS older bars per
  symbol; symbols that died before `deepFrom` first appear at that point.
* `simulate()`/`screenAsOf()`/`computeHold()` THROW when deep history is missing (`_histGuard`).
  Never soften this to a silent return — a truncated series produces plausible, wrong numbers.
* Only the initial load may clear the IndexedDB byte cache; a deep-part miss must not evict the
  recent part cached seconds earlier under the same version.

**Verification** (do all three when touching the split or the loaders):
1. `python3 scripts/fetch_live_sf.py` → rebuild the full bin from live parts (§7.0).
2. Re-split it and md5-compare every field of every symbol against that bin — deep+recent
   concatenated must equal the original exactly (4,441 symbols, 0 diffs at the 2026-08-03 flip).
3. Drive `backtest-engine.js` in a real browser against BOTH layouts and diff the run metrics:
   a 2020-window backtest must be byte-identical recent-only vs legacy-full, and a pre-2019 one
   must match after `ensureHistoryFor` (harness pattern: intercept the sf-data host with
   Playwright `page.route`, serve local part files).

## 47. ★ FULL-HISTORY COLUMN — REMOVED  (added 2026-08-03, removed 2026-08-03)

The 🏛 Full-history column (2002→date return, baked nightly in Node — `scripts/bake_full.mjs` +
`.github/workflows/bake-full-history.yml`, snapshot row `waves_full`) was pulled from the 🆕 New tab
of Saved Strategies on request the same day it shipped. Both files are deleted; the `waves_full`
Supabase row is simply never written or read again (harmless to leave sitting in the DB). If this
comes back, the old approach (Node-side bake, separate snapshot row so the browser-side wave bake
can't clobber it mid-publish, checkpoint-every-5 + refuse-empty-publish) is still sound — see the
git history of this section (`git log -p -- scripts/DATA_RUNBOOK.md`) for the full design writeup,
or `git show <commit>^:scripts/bake_full.mjs` for the script.

### ★ Strategy identity lives in ONE file
`docs/bt-identity.js` (`identityKey`/`ruleKey`/`winKey`/`bakeGroups`) is loaded by both
`saved-strategies.html` and `strategy-backtest.html`. A baked result is stored under
`identityKey(cfg)` and looked up by the page under the same key: a second copy that drifts does not
error, it just leaves the site saying "not baked yet" forever. The 2026-08-03 topN change already
had to be applied to two copies by hand — do not make a third.


## 48. ★★ POINT-IN-TIME N500 MEMBERSHIP FOR AUDITS = stock_data.bin indicesHistory  (2026-08-04)

**Any member-scoped audit/backfill MUST take membership from `docs/stock_data.bin` →
`indicesHistory["Nifty 500"]` (74 event-driven snapshots 2006→2026, CI-maintained) via
`scripts/_n500_member_bin.py` (worktree rev-mission) — nearest-prior-snapshot, rename-normed,
DUMMY* placeholder tickers filtered.** Never the 17-snapshot `_n500_history_raw.json` +
`_n500_changes.json` replay walk: the changes ledger has NO merger/delisting exclusions
pre-2020, so that walk carried dead companies as PHANTOM members for months-to-years
(GRUH "member" through Dec-2019 though removed 2019-09-27; CMC through Dec-2016; ELDERPHARM,
MONSANTO, RASOYPR similar — found when the user asked "was GRUH in the N500 in Dec-2019?").

Effects when re-based (2026-08-04): 33 of 41 grave cells dissolved (phantom non-member cells,
annotated `phantom_non_member` in never_filed.json, 8 real graves remain), AND ~55 hidden
member-cells SURFACED — reshuffle-quarter transients the sparse walk never audited (exit-quarter
cells of ADANIPOWER/RANBAXY/TATASTLBSL/COX&KINGS/LAKSHVILAS…): 48 revs landed same-day via
detres + 7 PATs via FY-identity gates. Reshuffle quarters union ~520-539 symbols (churn), quiet
quarters ~500 — a member-count stuck at exactly 500-503 across a reshuffle is the STALE-WALK smell.

The backtest was NEVER affected — `membersAsOf` in the engine reads the SAME bin indicesHistory
(spliced by `build_n500_membership.py`); only the audit layer had forked onto the sparse source.
The turnover page ignores the changes-replay too (snapshots only). If a new audit script needs
membership, import `_n500_member_bin` — do not copy-paste a walk.


## 49. ★★ THE 100%% CLOSE-OUT PLAYBOOK — what finally landed the last ~40 cells  (2026-08-04)

Mission fact: REV + PAT both closed at 100.0000%% of fillable over 23,081 member-quarter cells
(2015→2026Q1, bin-snapshot membership). What worked, in escalation order — use it next campaign:

1. **detres JSON first** (§42), scrip NAME-VERIFIED against `_bse_master_all.json` — the master's
   `Scrip_Name` is the CURRENT name; resolve the ERA entity before judging plausibility
   (scrip 506390 today reads "Sudarshan Colorants" but was CLARIANT CHEMICALS in 2016 —
   calendar-FY, transition stub Jan-Mar-2016; and 533206 is SJVN, NOT Srei — wrong-map poison
   nearly landed another company's numbers).
2. **TEXT-LAYER positional word-row rebuild** (y-cluster `get_text('words')`, x-sorted) BEFORE
   OCR — many BSE "Outcome of Board Meeting" PDFs are digital (Srei/MOFS/RELCAPITAL were).
   Subject filters must NOT require "result": outcomes hide results; DO exclude
   newspaper|voting|ballot|postal|clarification|non-submission.
3. **OCR with scale-aware anchors** (1/10/100) + micro-cap floor 0.08 for |v|<5 (0.6cr floor
   makes small-value anchors column-ambiguous — ORISSAMINE lesson).
4. **Year-later filings' year-ago columns** with DOUBLE anchors; windows +140d around FY20
   (COVID) and up to a YEAR late for CIRP companies (AMTEKAUTO filed Jun-2017 in Aug-2018).
5. **VISION reads (user-authorized) for scan-soup** — every vision landing still needs an exact
   anchor + an internal identity (H1/9M/FY arithmetic on the same page).
6. **Era-press corroboration as tiebreak GATE** when two as-filed-era documents conflict:
   Capital-Market-wire numbers (Business Standard archives) matched the true doc EXACTLY for
   TEXRAIL (12.16) and SUDARCOLOR/Clariant (9.85). AI answers (Gemini) are LEADS for locating
   such press, never landing evidence — they attached real prior-quarter numbers to never-filed
   quarters (GRUH/CAPF/ILFSTRANS "final filings" that don't exist) and missed pages BSE serves.
7. **Graves need documents**: non-submission letters, BOTH-exchange zero-scans, merger effective
   dates, annual-only publications under NCLT. 19 stand (identical set both metrics); 33 more
   dissolved as phantom non-member cells when membership was re-based (§48).
Convention guards that bit repeatedly: bank rev = Interest Earned; holdco std rev can be ~0
(RELIGARE 0.33) or negative as-filed (NITINFIRE -173.06, JPINFRATEC -640.75 — 106-cell
precedent); HMT-class stored series may be the AUDITED-restated basis — match the SERIES
convention (continuing-ops rev 2.30), not the original filing, when stored PATs prove restated.

## 50. ★ THE DEEP-FUNDAMENTALS NIGHTLY IS A CLOUD ROUTINE  (moved off the desktop 2026-08-05)

`scripts/xbrl_extra.json.gz` — the ~200-tag-per-filing ledger behind the stock page's Financial
detail block (EPS, balance sheet, cash flow, segments, bank health) — was topped up by a LOCAL
scheduled task reading the 5.9-GB (104k-file) `scripts/_xbrl_cache`. It now runs as a **cloud routine**
(claude.ai/code/routines, 00:00 IST daily), landing via `claude/xbrl-extra-<ts>` → PR →
`gh pr merge --squash --admin`, exactly like the vision-fill routine (§17b). The local task is
GONE (retired with the Windows box 2026-08-05 — do not recreate it).
`.github/workflows/xbrl-extra-nightly.yml` exists but has **no cron**
— it is the MANUAL rescue lever (dispatch it for a big catch-up; a GitHub runner reaches
nsearchives directly and its Actions cache makes repeat runs cheap).

**A cloud sandbox keeps NOTHING between runs, so the state had to move into the repo.** Two
committed files are the whole contract, and both runners maintain them identically:
- `scripts/xbrl_extra.json.gz` — the ledger. Every run `gunzip`s it as its starting point, so
  the repo, not any machine, is the source of truth.
- `scripts/xtra_seen_window.json.gz` (~16 KB, 2.3k names) — the seen-set **pruned to the last 60
  days by FILING timestamp**. This is the piece that makes a stateless run cheap: `fetch_new`
  treats seen names as already-`have`, so an empty sandbox downloads only genuinely NEW filings
  (~200/night) instead of the whole 14-day window (~3.6k). Names older than the list window can
  never be re-offered, hence the prune — full seen-sets are 100k+ names and would bloat git.

Reusable lessons for moving any local routine to cloud:
- **Prove the replay is idempotent before allowing state loss.** Here it is: the window holds only
  the NEWEST filings and the ledger merges per-field, non-null, latest-wins — so re-extracting a
  window file can only re-assert. That property is what turns "lost cache" into a slow night
  instead of corruption. Check it before copying this pattern.
- **Never synthesize a seen-set from a filings LIST** — marking a file seen that was never
  extracted loses that quarter silently until the company refiles. Seed it only from the cache
  listing taken BEFORE the fetch (`--seed-seen`), or from the committed copy (`--seen-repo`).
- **Cap the cold start** (`--max-fetch`): the first run has to fetch whatever the committed
  seen-window doesn't cover, and NSE is ~1 s per file. The window overlap self-heals the rest.
- `--prune-cache-days` refuses on any cache over 20k files, so a stray flag could never eat the
  old local one (5.9 GB / 104k files; that cache went away with the Windows box).
- `--push` stays refused from a tree named `stocks-dashboard` (CLAUDE.md rule 2), which is also
  the runner's checkout name — cloud and CI both commit via their own retry/PR path instead.

---

## 51. ★★ "NO CONSOLIDATED" IS USUALLY REAL — and scanned filings LIE to keyword search  (2026-08-06)

Two findings from closing the FILL-2020 PAT residue. Both cost real time; neither is obvious.

### 51a. Quarterly consolidated results only became compulsory from FY2020
Before Apr-2019 most Indian listed companies filed **standalone quarterly + consolidated annually**.
So a pre-2019 empty `con` cell is usually a **never-filed** quarter, not a backfill miss. Proof from
our own data (`sf_fundamentals`, first quarter each company ever reports con PAT):
**285 companies start at Jun-2019** (Q1 FY2020) and 142 more at Sep-2019 — by far the largest onset
in the series. Con gaps collapse in lockstep: 197 (Mar-2018) → 67 (Mar-2019) → 13 (Jun-2019).
**Consequence:** do NOT plan a fetch campaign for pre-2019 con cells. ~3.1k of them cannot be fetched
because they do not exist. The only legitimate routes are (a) the no-sub identity where provable, and
(b) annual-consolidated ÷ quarter derivation where 3 of 4 siblings are known (§45).

### 51b. Bank/PSU scans have a GLYPH-SUBSTITUTION text layer — `a→o`, `t→l`
IOB's 2020-21 result PDFs carry a real text layer (54k chars over 28 pages) but it is systematically
corrupted: "Standalone Financial Results" extracts as **"Slondolone Flnonciol Resulls"**, "audited" as
"oudited", "Bank" as "Bonk". A plain `re.compile("consolidat")` scan returns **zero hits on a document
that does contain the word**. Same class as the ABREL/Century-Textiles 2019 filings ("Income" → "Ireom").
- **Search with a corruption-tolerant fragment**, not the whole word: `[o0]ns[o0][li1]id` catches both
  `Consolidated` and `Consolidoled`. Pick a fragment whose letters survive the substitution.
- **ALWAYS run a positive control before believing a zero.** Point the same detector at a period where
  the thing is KNOWN to exist. IOB Mar-2022 (con 551.78 stored) lights up — `"Audited Slondolone ond
  Consolidoled Finonciol Results"` — which is what proves the 2020-21 zero is real and not a parser bug.
- **Then eyeball the hits.** The one candidate hit in IOB's Jun-2020 filing was `"os port of
  consolidotion ond reducing concentrotion risk"` — loan-book consolidation, an English false positive.

### 51c. IOB 2019-2021 con PAT — CLOSED, do not re-grind
Checked all 8 quarters Mar-2020..Dec-2021 (plus Dec-2019) via BSE announcements, wide windows where the
±8-day window found nothing (Jun-2020's filing landed 2020-08-20, COVID-delayed), detector validated per
51b: **IOB published no consolidated results before Mar-2022.** Its stored con series starting exactly at
20220331 is correct, not a gap. Corroboration: the Jun-2020 filing's own highlights page prints "Net Profit
for the quorter ended 30th June 2020 ... Rs.121 crore" against our stored std 120.69. ✔
**IOB con=std is NOT valid** — when IOB does report con it DIVERGES (551.78 vs std 552.38), so it has real
consolidation differences; an identity fill would be fabrication. Leave null.


---

## 52. ★★ STANDALONE PAT 2015-2020 — the detres route, and why the PDF route can't reach it  (2026-08-06)

Closed 32 of the 37 standalone-PAT gaps for Mar-2015..Dec-2020 in one session. The PDF/announcement
path could not reach ANY of them, for two reasons that will recur on any pre-2017 backfill:

1. **Stored announce dates for old quarters are not filing dates.** Many look like a
   quarter-end+45d default. APLLTD Sep-2015 is stored `20151114`; the filing is `20151027`. A ±6-9
   day window around the stored date therefore finds NOTHING. **Search the post-quarter stretch
   (qe+8d .. qe+140d), not the announce date**, whenever the target is pre-2018.
2. **Pre-2016 BSE attachments 404.** They use an underscore+timestamp name
   (`B37C7931_1C03_..._141357.pdf`); `AttachHis`/`AttachLive` return 404 and
   `corporates/anndata` returns HTML. The bytes are simply not reachable by the modern path.

**Use §42's detailed-results JSON instead** — keyed by quarter rather than announcement, structured
rather than scanned, so both problems vanish. Tool: `scripts/fill2020_tools/fill_std_pat_detres.py`
(dry-run by default, `--apply` to write, `--only SYM` to scope). Ledger:
`scripts/std_pat_detres_fills.json`.

### 52a. Gates — one of these must hold, never a bare printed number
- **EPS recon** (primary): `EPS x (Equity Capital / Face Value)` == Net Profit within 6%. Ind-AS-era
  rows (~2017+) DROP Equity Capital and Face Value and rename EPS to "Basic for discontinued &
  continuing operation" — fall back to the share count from the nearest quarter that still prints
  it. If a split intervened the recon fails tolerance and the cell is skipped, which is the safe way.
- **FY-consistency** (fallback, and the stronger of the two): the candidate + the fiscal year's other
  three quarters == the audited `.50` annual row within max(3cr, 3%). Try Apr-Mar AND calendar-year
  (`.50` sits on the fiscal-year-END quarter). When a sibling is ALSO missing from our data, pull it
  from detres too — two gaps sharing a fiscal year otherwise block each other forever (VTL Jun+Sep
  2019). Any sibling taken from our OWN store makes this prove BASIS as well as value.
- **Escalate, don't guess, when a value looks wrong.** GFLLIMITED Mar-2018 std 240.65 is 2.5x its
  neighbours and 4x its own con — correct: FY18 annual 487.31 == the four-quarter sum exactly (the
  std>con gap is INOX Wind subsidiary losses). APLLTD Sep/Dec-2015 spikes ~3x — correct: the stored
  CON spikes identically and FY16 annual 698.12 vs sum 698.11.

### 52b. Delisted companies are missing from bse_scrips.json
It is built from the live master, so dead names resolve to None. Get them from
`ListofScripData/w?...&status=Delisted` (4,611 rows; blank status = all 10,797 — **validate the
count**, a 162-byte body is the rate-limit stub, §0). Found: ADVANTA 532840, DISHMAN 532526,
CAPF 532938. Baked into `SCRIP_OVERRIDE` in the tool.

### 52c. Final residue: 3 stay NULL (proven never-filed), 2 recovered by GATE X  (updated same day)
- **IL&FSTRANS Sep-2018 + Dec-2018 — NULL, conclusive.** The company filed *"Reasons For
  Non-Submission Of Financial Results For The Quarter Ended 2018"* (2018-11-22) and *"Disclosure Of
  Reasons For Delay In Submission"* (2019-06-07 and 2019-08-30). It DID eventually file — *"Financial
  Results For March 31, 2019"* on 2020-06-05 and FY20 on 2020-12-07 — but both late filings carry
  **annual columns only** (FY19 vs FY18; no quarterly split anywhere in 40 pages). Two missing
  quarters in one FY can't be separated by any identity. Never-published as quarters, from the
  filer's own documents. ✔
- **CAPF Dec-2018 — NULL, conclusive.** Capital First merged into IDFC FIRST Bank effective
  18-Dec-2018 — the entity dissolved BEFORE the quarter even ended; zero result filings after. Same
  class as HDFC Jun-23. ✔
- **ADVANTA Mar-2015 + Sep-2015 — FILLED (10.59 / 15.17), and a lesson.** First diagnosis
  ("genuinely inconsistent year") was WRONG: the NSE archive pages declare ADVANTA a
  **calendar-year filer** ("First Quarter, Financial Year 01-Jan-2015 To 31-Dec-2015"), so the
  Apr-Mar FY identity was summing across two of its fiscal years — of course it missed by 10.6cr.
  The 9% EPS misses are the quarters' extraordinary items. Landed via **GATE X** (PRE2015 standard):
  detres NP == the NSE archive detail page to the 4th digit (105.9mn == 1,058.96 lakh;
  151.74mn == 1,517.38 lakh), share capital cross-matching on both. Tool:
  `fill2020_tools/apply_advanta_std.py`. **Rule: before declaring an Apr-Mar FY identity "failed",
  check the filer's fiscal year on the NSE page ("Relating to ... Financial Year dd-mmm To dd-mmm")
  — AMBUJACEM/ACC-class calendar filers will fail it structurally.** ✔
- **NSE archive serves DELISTED symbols with detail pages both bases** (ADVANTA here; SATYAMCOMP
  precedent) — a working GATE-X second source for any 2015+ residue the PDF route can't reach.


---

## 53. ★★ PRE-2020 CONSOLIDATED PAT — the ceiling is 2.7%, and it is measured, not assumed  (2026-08-06)

§51a established that quarterly consolidated only became compulsory from FY2020. This section
turns that into a NUMBER, so nobody plans a campaign against a wall that isn't there.

**Swept all 311 companies with a pre-2020 con-PAT gap (2,979 cells, quarters 2015Q1–2019Q4)**
against the NSE results-archive list API (`corporates-financial-results?symbol=X&period=Quarterly`,
which serves DELISTED symbols and declares basis per row). Result, with 0 errors and 0 empty
responses — i.e. this is a real answer, not a fetch failure:

| | |
|---|---|
| gap cells | 2,979 across 311 companies |
| have a CONSOLIDATED quarterly filing | **79 (2.7%), in just 25 companies** |
| have a consolidated ANNUAL row | 777 across 290 companies |
| landed after gates | **42** |

**~97% of pre-2020 con-PAT was never filed as a quarter.** The 777 annuals cannot rescue it: an
annual splits into quarters only when three of its four siblings are known, and for these companies
the quarters are precisely what is missing. Annuals are a GATE (§45 FY identity), not a source.

Tools: `fill2020_tools/nse_con_discover.py` (resumable inventory) → `read_con_pat_nse.py`
(gated reader, `--apply` merges fill-only). Ledger `scripts/con_pat_nse_reads.json` keeps every
refusal with its reason.

### 53a. Reading a basis you have NO stored anchor for — GATE S'
Every other route anchors the read against the stored PAT for that (sym, qe, basis). Here the
stored con IS the gap. The substitute:

> **GATE S' (sibling-basis):** fetch the NON-consolidated page for the SAME quarter and check it
> against the STORED std. That proves source + scale + period-mapping + symbol identity for this
> exact company-quarter; the con page then comes from the same family under the same declared unit.

Generalises the SpiceJet manoeuvre (§ FILL-2020 Phase 4): *validate the document through the basis
you already hold, then read the basis you don't.* Reusable anywhere a whole basis is missing.

### 53b. Four failure modes this route has, all of which pass naive checks
1. **Blank-template pages.** SUNTV Mar-2017 filed the consolidated form with every P&L row 0.00
   (only Paid-up equity populated). Basis/period/symbol all validate and the std sibling is
   perfect. **Refuse `PAT == 0.00` outright.**
2. **S' passes, row choice still wrong.** S' validates the FAMILY, never WHICH ROW was picked on
   the con page. TATASTEEL Sep-2016 recon −230.92 vs picked −54.42. **Where the con page carries
   EPS+equity, a failing EPS recon must BLOCK, not merely "not pass".**
3. **Cumulative pages are YTD.** A Q2/Q3/Q4 cumulative row lands 6/9/12 months as a quarter.
   `parse_detail` does not surface the field — read `Cumulative / Non-Cumulative` from the body.
4. **`aliases()` excludes the symbol itself** — checking membership without prepending `sym`
   rejects every page whose Symbol is simply the current one (BALLARPUR "mismatching" BALLARPUR).

EPS recon here is `PAT == eps * eqcap / fv`; the declared-unit divisor cancels, so it is immune to
`parse_detail` scaling per-share rows (Face Value ₹2 prints as 0.02 under lakhs).

### 53c. Owners-attributable, or nothing
The archive prints `Net Profit/(Loss) after taxes, minority interest and share of profit of
associates` — literally this dataset's basis. Fall back to the plain period row ONLY when minority
interest is absent or zero; **7 cells were refused as `no-owners-row-but-minority-present`** rather
than silently landing total PAT on an owners-basis series.


### 53d. FY-identity derivation for con — and the 9-month-annual trap
Where a fiscal year has 3 con quarters stored and the 4th is the gap, `derived = annual - sum(3)`.
Across all 311 gap companies only **35 FYs qualify and 13 have the annual — 1 survived the gates.**
Tool: `fill2020_tools/derive_con_pat_fy.py`, ledger `scripts/con_pat_fy_derived.json`.

Two gates make the difference between arithmetic and fabrication:
- **CALIBRATION.** The subtraction is only as good as the annual, which can silently sit on another
  footing (restated post-merger/Ind-AS). 555 FYs here have all four con quarters, so test
  `annual == sum(4)` for the SAME company in a neighbouring FULL year. Holds → commensurable.
- **DECLARED SPAN — the one that actually bit.** HCLTECH FY2016 derived a Jun-2015 con of **38.64
  against siblings of ~1,870**. Its "annual" covers only Jul-2015..Mar-2016: the 9-month stub from
  moving a June-ending FY to a March-ending one. `annual - 3 quarters` was differencing periods that
  do not tile. **It passed calibration** — calibrating FY2017 says nothing about FY2016's span.
  The page prints `Financial Year <d> To <d>`: **require 12 months.** Behind it, a sibling-magnitude
  net (derived within 0.15x..6x the median of the three knowns) catches undeclared span anomalies.
  Same class as §45 and STEP-W's GLAXO finding — a calendar/transition year that silently fails to
  tile is the standard way an identity derivation fabricates a plausible number.

**Pre-2020 con-PAT is now closed at 2,939 open cells**, every one of them structurally absent
rather than un-attempted: 2.7% of gap cells had a quarterly filing (42 landed), and the FY-identity
route reaches 1 more. Do not re-grind this window without a genuinely new source.
→ **2,926 after §53e** (53 of 2,979 closed). The wall itself did not move; the refusal list did.

### 53e. Reworking the 37 refusals — the owners DEDUCTION convention, and three gates that argue back  (2026-08-06)
`con_pat_nse_reads.json` kept every refusal with a reason, which is what made a second pass cheap:
8 more cells landed with no new discovery work, purely because a convention learned LATER (during
the con-REVENUE pass) answered the largest refusal class. **Re-read your own refusal ledger whenever
a new technique appears — it is the cheapest source of cells in the campaign.**

**The convention.** The Ind-AS-era archive template drops the explicit owners line and prints
`Net Profit/(Loss) for the period` (BEFORE minority/associates), then `Share of profit/(loss) of
associates` and `Minority interest` as DEDUCTIONS, then a `Consolidated Net Profit/Loss for the
period` row that merely DUPLICATES the period row and carries no information. So

    owners = period − minority − associates

⚠️ In the revenue pass this could be offered as one of several candidate variants and the stored con
PAT picked the right one. **Here the PAT IS the value being written, so there is nothing to match
against and variant-shopping is not available.** The convention has to be established first:
`read_con_pat_owners.py --calibrate` scores it on cells whose con PAT we ALREADY store, counting
only SEPARABLE cases (where ±associates are further apart than the anchor tolerance — a near-tie
proves nothing). **51 separable, 50 correct; 49 of them on the exact Ind-AS template, 49/49 correct.**
The one dissent (ASHOKA Mar-2015) is not a counter-example: that page's own owners row equals the
identity, so it is our STORED value that disagrees with the document.

**GATE C — the EPS positive control, and it must be allowed to say no.** §53b makes a failing EPS
recon a hard block. Two refusals looked like false ones (TATASTEEL Sep-16/Dec-16: the page prints
the owners row by name, the deduction identity reproduces it exactly, GATE S' matches stored std to
the paisa, and the FY quarter-sum lands within 0.19% of the audited annual). The hypothesis was that
Tata Steel's EPS simply never reconciles. **The control refuted it** — 3 of its 4 stored quarters
reconcile within 3%, so the 12.6%/97.9% miss on the targets is real evidence and the block stands.
JINDALPOLY is cleaner still: 6 of 7 control quarters reconcile to the paisa, its target misses by
16.9%, and its FY2017 quarters do not tile the audited annual either way (218.16 vs 275.36) → refuse
per §45. **A control is only worth running if it is allowed to kill your theory.**
* Related fix worth keeping: the old gate tested only `^\(?a\)?\s*basic`, which grabs the
  BEFORE-exceptional EPS while the archive prints the after-exceptional one too. Test EVERY EPS row
  and journal which one matched — that alone turned three "EPS n/a/FAIL" reads into clean passes.

**GATE S'' — a failing S' is sometimes a REVISED FILING, not a wrong document.** Ask §45's question
of the whole fiscal year instead of one quarter (`adjudicate_sprime.py`, ledger
`con_pat_sprime_adjudication.json`). The classes separate cleanly:
| pattern | reading | cells |
|---|---|---|
| 0–2 of 3 sibling quarters reproduced, our series tiles the audited annual | BAD PAGE — refuse | GITANJALI ×3, GLOBOFFS, ZEELEARN ×2, CGPOWER |
| archive tiles the annual, ours does not | BAD STORED — S' refusal was false | BLUESTARCO (still blocked by GATE C) |
| **3 of 3 siblings exact, disputed quarter <2% off, our series tiles the annual** | **REVISED quarter — land it** | BLISSGVS Jun-2016 |
| neither series tiles its own audited annual | unresolved FY — refuse | NOIDATOLL |
| no audited annual published | inconclusive — refuse | AJRINFRA ×2 |

**Two traps while building that, both of which silently produce a confident wrong answer:**
1. **The audited annual is NOT findable in the quarterly feed.** The FY row and the Q4 row share a
   `toDate` of 31-Mar, so selecting by `toDate` returns the QUARTER. Every verdict in the first run
   was an artifact of comparing a 3-month figure with a 4-quarter sum. Use the list API's own
   `period=Annual` feed and span-check `fromDate`→`toDate` to ~12 months (§53d's declared-span gate).
2. **Cache-key collision.** The page cache is keyed `(sym, qe, tag)`; the annual and the Q4 page
   share a `qe`, so with the same tag the second fetch silently re-reads the first page's HTML.
   Same class as the 0-byte-cache bug in `_nse_archive_revop.get()`.

**`basis-mismatch:?` was a mislabel — the source is dead, the link was fine (13 cells).** M&MFIN ×12
and ICRA Jun-2017 refused with a phantom "basis mismatch" because the page parsed to nothing. The
truth: NSE's archive returns a content-free ~2.9KB shell for **symbols containing `&`** (§42's known
M&MFIN/J&KBANK breakage) — raw `&` and `%26` both return the shell, `%2526` and de-ampersanded names
404, and every one of M&MFIN's 12 quarterly ids behaves identically. ICRA 1028491 is a 0-byte file
across repeated tries. Neither is reachable by this route; BSE detres cannot substitute (§42:
standalone only). **Always render a no-meta page as "empty shell", never as a content disagreement —
the wrong label sent a whole pass hunting for a better link.**

**Result: 37 refusals → 8 landed, 29 refused with a named cause** (13 empty-shell, 3 EPS-blocked and
control-confirmed, 11 S'-adjudicated, 2 blank-template re-verified all-zero). Tools:
`fill2020_tools/read_con_pat_owners.py` (+`--calibrate`), `adjudicate_sprime.py`,
`diag_con_pat_refusals.py`; ledgers `con_pat_owners_reads.json`,
`con_pat_owners_calibration.json`, `con_pat_sprime_adjudication.json`.

### 53f. Banks are IN — the user reversed the non-banks-only rule  (2026-08-06)
The 2026-08-06 "non-banks only" call on the no-sub con identity (which held KTKBANK/SOUTHBANK, and
later CUB/UCOBANK, deliberately null) was put to the user again and **reversed: "include banks
everywhere."** Applied: KTKBANK + SOUTHBANK Dec-2019 con PAT, and 30 revC + 28 opC cells across
CUB/UCOBANK/KTKBANK/SOUTHBANK. `con_nofile_identity.py`'s `BANKS_NULL` is now empty.
* The earlier objection to KTKBANK ("only 2 identity quarters before diverging — too thin") was an
  artifact of judging no-sub from our OWN stored cells. The NSE filing index answers it directly and
  non-circularly (§54b E1–E5, all five verified): a standalone result IS listed for Dec-2019, no
  consolidated one is, and the gap precedes the first consolidated filing ever (KTKBANK 2020-09-30
  of 95 filings; SOUTHBANK 2021-06-30 of 89).
* **IOB is untouched and stays null** — it fails E4 (con null throughout, and where it does report
  con it DIVERGES: 551.78 vs std 552.38, §51c). Including banks is not including fabrication.
* ⚠️ `con_nofile_identity.py` used to REWRITE `con_nofile_identity_fills.json` from the current
  run's journal only. Because the appliers are fill-only, a second run journals just the new cells
  and would have deleted the provenance of the 91 values the first run landed. It MERGES now — any
  applier that publishes a provenance ledger must merge, never replace.

---

## 54. ★★ POST-2020 REVENUE — the NSE filing INDEX is both a fetch route and an evidence source  (2026-08-06)

Closed 279 of the 770 post-2020 empty rev cells (revS 192→139, revC 578→352) in one session, none
of it by reading a PDF. The unlock is an endpoint the campaign had only ever used for PAT:

    https://www.nseindia.com/api/corporates-financial-results?index=equities&symbol=<SYM>&period=Quarterly

It returns EVERY quarterly result the company filed with NSE — one row per (quarter, basis) — each
carrying `consolidated` ("Consolidated"/"Non-Consolidated") and, for ~2018+, the `xbrl` URL. Harvest
it once per company (`fill2020_tools/nse_list_harvest.py`, cache `scripts/_nselist/`), and it answers
two different questions.

⚠️ **THE CLASSIC INDEX STOPS AT DEC-2024** (found 2026-08-10) — SEBI's integrated-filing regime
replaced it, and every company's rows simply end there. 2025+ quarters live under
`/api/integrated-filing-results?index=equities&period=Quarterly&symbol=<SYM>` (rows carry `qe_Date`,
per-basis `xbrl`/`ixbrl`/`pdf_attach`; basis vocabulary is "Consolidated"/"Standalone"). Two traps:
the integrated XBRL instance holds ONLY the current quarter — zero comparative contexts, BANKING
format included (verified TIMKEN Jun-26, INDUSINDBK Mar-24) — so comparative mining from XBRL is
dead for 2025+; and `pdf_attach` is often the literal `corporate/null`, while the real results PDF
sits on the corporate-announcements feed ("Outcome of Board Meeting"). `nse_list_harvest.py` now
merges integrated rows into `_nselist/` in the classic shape, so the E-gates and `nse_xbrl_rev.py`
see one continuous exchange record; a company that "has no rows" (ATHERENERG, listed May-2025) was
just born after the classic index froze. First landed via the merged record: TMPV Mar-25 revC
119503 (integrated con XBRL, owners-PAT anchor exact), ATHERENERG identity ×3 (std-only record,
first con Jun-26), TIMKEN Jun-25 revC 822.18 (§58 year-ago column of the Jun-26 outcome PDF after
both eras' XBRL routes measured empty).

### 54a. The XBRL is still served for quarters our cache never held — 150 cells
sf_revop is built by re-parsing `_xbrl_cache`; a cell is empty mostly because the daily fetch missed
that filing, NOT because no document exists. Download the listed XBRL, parse it with
`build_revop.parse_file` (the same parser the nightly uses — so a later `--fresh` rebuild reproduces
the cell), anchor, write. Tool: `fill2020_tools/nse_xbrl_rev.py`. This closed M&M ×13 — the target
that had defeated `backfill_revop_gaps.py` for two sessions, because M&M's gaps are STANDALONE while
`--rescue` mines consolidated comparatives (campaign doc §4).
* Gates: declared basis == target basis, parsed quarter-end == target (never trust the list row's
  `toDate` — §45's double-indexing), PAT anchor vs stored (owners tag preferred), fill-only.
* ⚠️ **Do NOT sanity-band a value against the OTHER basis' stored twin.** Two independent failures:
  con/std ratios are legitimately huge for holding structures (TMPV con 79,611 vs std 14,851 — JLR
  is in the consolidation; GMRAIRPORT 72x; M&M 2.2x), and the stored twin is sometimes itself the
  junk cell (ETERNAL Mar-2022 stores con rev 1.21 against a true standalone 1,014.80). Band against
  the company's own SAME-BASIS neighbouring quarters instead — that catches the 3MINDIA class
  (off by three orders of magnitude) without rejecting real data.
* `.../corporate/xbrl/-` is a real listing with a placeholder URL (37 cells, ACC Dec-2021 class) —
  the filing exists, the XBRL was never published. Record it as such, don't retry the 404 forever.

### 54b. The index is NEGATIVE evidence: proof a consolidated result was never filed — 91 cells
`nosub_rev_derive.py` proves "no subsidiary" from our own stored con PAT, which is partly circular
(earlier passes manufactured con PAT by copying std). The index is the exchange's own record and
cannot be contaminated by us: CUB has 79 filings and ZERO consolidated ones; TIMKEN 75 and zero.
Post-Apr-2019 that is decisive, because consolidated quarterlies are compulsory for any company
having subsidiaries (§51a) — filing standalone-only asserts there is nothing to consolidate.
Gates used (`fill2020_tools/con_nofile_identity.py`), all five required:
  E1 the index shows a STANDALONE filing for that exact quarter (so its silence on consolidated is
     meaningful and not just a hole in the index) · E2 no consolidated row for that quarter ·
  E3 the quarter precedes the company's FIRST consolidated filing ever (the leading-run rule) ·
  E4 stored con PAT already equals std PAT · E5 **no quarter at-or-before the gap where both rev
     bases, or both PAT bases, differ by >1%** — E5 is the one that earns its keep: it disqualified
     ZFCVINDIA (con rev 385.1 against std 440.9 in Dec-2019, BEFORE its gap run), PATANJALI, MGL and
     HATSUN, and it is the check that separates "no subsidiary" from "we simply never stored one".
**Banks are held null by user decision** (2026-08-06) even when they pass every gate — CUB (18
cells, never consolidated in 79 filings) and UCOBANK (6), on the same reasoning that already kept
KTKBANK/SOUTHBANK null. Those are deliberately-null, not unfillable.

## 55. ★★ INSURER CONSOLIDATED REVENUE from the filing PDF — and the control that makes it safe  (2026-08-06)

38 cells (HDFCLIFE 14, ICICIPRULI 20, NIACL 4). Insurer con revenue is reachable by NO other route:
NSE serves no insurer rev XBRL before the 2025 Integrated-Filing regime and
`api/integrated-filing-results` returns only the last ~20 filings; the IRDAI public disclosures of
§43 are entity-level and therefore standalone-only forever; and con=std is fabrication for every
insurer except ICICIPRULI (§3). So the quarterly filing PDF, which carries both statements, is it.
Tool: `fill2020_tools/insurer_con_rev.py`, ledger `scripts/insurer_con_rev_fills.json`.

**Convention (both validated to the paisa against stored standalone values):**
  life    = Net premium income + policyholders' Income from investments (Net) + shareholders'
            Investment Income.  HDFCLIFE Jun-2022: 9,27,187 − 3,48,656 + 10,060 = ₹5,885.91cr = stored.
  general = Premium Earned (Net) + policyholders' Income from investments (net) + shareholders'
            Income from investments.  NIACL Jun-2023: 7,91,900 + 1,35,544 + 56,563 = ₹9,840.07cr = stored.

**★ THE GATE THAT MATTERS — a per-filing standalone POSITIVE CONTROL.** Accept the consolidated
figure only if the SAME filing's standalone statement reproduces the standalone revenue already
stored for that quarter (0.5%). It tests the entire chain — page, column, scale, every revenue leg —
against a known answer, per document. It caught what nothing else did: in the 2025-format packs the
shareholders' investment-income leg sits on a different page and was silently contributing zero
(29,061.08 read against 29,381.30 stored), and the PAT anchor passed that read happily. Quarters
with no stored standalone to control against are skipped, not guessed. It also cut HDFCLIFE from 17
"successful" reads to 11 trustworthy ones — and recovered 2 the earlier version had failed.

**Layout traps, all of which produced silent wrong numbers before they were fixed:**
* **Column alignment must be GEOMETRIC, never positional.** A row printing a nil dash loses a cell,
  every later value shifts one column left, and the PAT row shifts with it so the anchor still
  passes. Cluster the figure x-positions page-wide and report every row into those columns — and
  cluster on the **right** edge, because the figures are right-aligned and the left edge moves with
  the digit count (that alone shattered one column into three).
* **A statement can span two pages** (the 2025 format; some 2023-24 packs). Join them only when the
  policyholders' "Transferred to Shareholders A/c" vector equals the shareholders' "Transfer from
  Policyholders' Account" vector — that shared line proves the two pages' columns are the same
  periods, and it tolerates an offset (one page had a leading empty column).
* **Pick the column by ANCHOR, never by position** (packs print [current | prev qtr | year-ago | FY]
  and the order moves between years), and the SCALE by anchor too (lakh/crore/million).
* **§44's duplicate trap is live here**: NIACL Jun-2023 stores std and con PAT identically (260.23),
  so one page satisfies both anchors. Refuse a consolidated figure that comes from the page which
  just served as the standalone control.
* Labels differ per filer over one character: ICICIPRULI prints "Income from investments: (Net)"
  with a colon, and that alone read as "no statement page found" for all 20 of its quarters.
* The shareholders' section marker is a bare heading in the life format but a FIGURE row in the
  general format ("Income in shareholders' account (a+b+c):"), so it cannot be required to be
  value-less; take the LAST match, since "Transferred to Shareholders A/c" also matches and sits
  above the real heading.
* Insurer revenue legitimately goes NEGATIVE (ICICIPRULI Mar-2020 std −8,339.14, Jun-2022 −1,611.82;
  HDFCLIFE Mar-2020 246.03 against ~15,000 neighbours) because investment income is marked to
  market. **Never apply a neighbour-plausibility band to an insurer** — the COVID quarters are real.

**Residue, honestly (re-examined 2026-08-06 after a second attempt):**
* **GICRE — SOLVED 2026-08-06.** The reading was never the blocker and neither, in the end, was the
  con-PAT anchor: §55c corrected 17 con-PAT cells, which unblocks the revenue track. See 55c.
* **GICRE, the reading problem (solved) — the text layer is CORRUPTED, §51b class.** Not the four-segment shape §43 warned
  about: the 2023 packs print a single "Premium Earned (Net)" row like NIACL's. The blocker is that
  the extracted text reads "OPERA TING RES UL TS", "Income from investments net)" (opening paren
  gone), "Aooropriations", and numbers break apart ("212.414" for 212414). Even the STANDALONE page
  yields no matching labels, so there is no positive control and by A5 nothing can land. Needs
  render-and-OCR (the §3 Gemini path, which still has no API key), not another regex.
* **NIACL — SOLVED (15 cells) by the header-DATE column model.** See 55b; the note below is the
  failure it replaced, kept because the failure mode recurs.
* **NIACL, the wrong way — do NOT re-attempt with an index-based cross-page join.** Its profit tail (PAT, minority,
  associate) sits on the page AFTER the revenue rows, and building owners-con per §3 across that
  break LOOKS right — NIACL Sep-2023 reproduced 10,566.55 against an exact standalone control. It is
  wrong in general: the pack's revenue page has FOUR columns and its profit page FIVE (six-month
  columns are added there), with a leading blank cell, so index k is a different period on each
  page. The 3% PAT tolerance then admits a near-miss, and Jun-2020/Sep-2020 both landed 6,923.24
  (and Jun/Sep-2021 both 8,115.95) — the previous quarter's revenue carrying this quarter's anchor.
  **Anchoring proves identity, NOT column alignment across pages**; only a shared row does (the life
  format's transfer line), and this layout has none. The path is disabled in code with that note.
  To re-enable: map each column to its period from the printed date headers per page, and select by
  PERIOD rather than by index.
* **A per-filing standalone control cannot catch a consolidated-side column error** — it passes on
  the standalone page. That is the limit of A5, and it is why the above had to be caught by eye
  (two quarters sharing a value is the fingerprint; scan any batch for duplicate values before
  applying).

### 55c. ★★ GICRE's CONSOLIDATED PAT — DEFECT FOUND AND FIXED (17 cells, 2026-08-06)
**Status: CLOSED for FY2022-FY2026.** Tool `scripts/fill2020_tools/gicre_con_pat.py`, ledgers
`scripts/gicre_con_pat_fills.json` + `pat_defects.json`. Two defects, one correction:

* **COPY (12 cells, Sep-2022..Dec-2025).** The stored con PAT was an exact copy of the STANDALONE
  PAT. GIC Re has subsidiaries and associates and files a consolidated statement EVERY quarter, so
  identical was never plausible: its Jun-2023 pack prints con ₹977.66cr where we stored 731.79 —
  that filing's own standalone figure.
* **CONVENTION (5 cells: 20220331 20220630 20240331 20250331 20260331).** These were genuinely
  consolidated but held the `Profit/(loss) after tax` line — i.e. BEFORE the share of profit in
  associates — while our basis is owners-attributable, which includes it. Left alone they would
  have split the series across two conventions, which is worse than either.

**THE ROW TO READ, and how it was settled.** GICRE's consolidated statement prints no NCI line at
all (wholly-owned subsidiaries), so the owners' figure is one of two adjacent rows:
`Profit/(loss) after tax` or that plus `Share of Profit in Associates` = `Profit for the year`.
The filing's own **EPS row arbitrates** (the §2d principle): basic EPS × shares (paid-up ÷ face
value ₹5) reproduces `Profit for the year` to ~0.04% and misses `Profit after tax` by ~4%.
**`Profit for the year` is the cell.**

**★ THE CHECK THAT DID THE REAL WORK — CUMULATIVE-COLUMN RECONCILIATION.** Every pack prints a
year-to-date and/or full-year column beside the quarters, and the quarters of that fiscal year must
rebuild it. All 13 available closures came out exact or within a paisa:
FY23 6907.32 vs 6907.31 · FY24 6685.87 vs 6685.87 · FY25 7431.85 vs 7431.84 · 9M-FY26 7129.79 vs
7129.79 · H1-FY26 5404.13 vs 5404.13 · H1-FY25 3256.37 vs 3256.37 · 9M-FY25 4932.99 vs 4932.99 …
Four independently-read quarter columns reproducing a printed annual to the paisa is not something
a misread can do, and unlike §45's compensating-error trap NONE of these quarters is derived — each
is read straight off a printed column. This is the gate that closed the quarters only one legible
filing prints.

**⚠️ THE TRAP THIS CLASS OF PACK SETS — a partly-unreadable header lies about periods.**
The layout is `[Q] [Q-1] [Q-4] | [YTD cur] [YTD prior] | [previous year ended]`. When OCR loses one
header date the survivors still look like a clean header, but their OCCURRENCES have shifted:
* the Sep-2024 consolidated page detects 4 dates for 6 figure columns, and the H1-FY25 year-to-date
  column (₹3,256.37cr) then presents itself as `(30/09/2024)` occurrence 0 — the Sep-2024 QUARTER;
* the Dec-2022 pack prints `31/03/2022` exactly ONCE and it is the FULL YEAR (₹2,005.74cr), not the
  March quarter (₹1,795.40cr).
So **never infer "quarter" from occurrence 0**. `align_roles()` matches the surviving dates as a
SUBSEQUENCE of the pack's canonical layout (derived from the filing's own reporting quarter) and
takes the role from there, refusing the page when the alignment is not unique. That keeps the good
columns and correctly excludes the cumulative ones. Compare §55b: reading the printed date is
necessary but NOT sufficient — you also have to know which *kind* of column you are reading.

**Reading these scans at all** (they are §51b glyph-class, text layer unusable — see 55d for OCR):
* Labels do not survive: the standalone PAT row comes back as `Prohiti(loss)aftertax`, the
  consolidated one as `Proft/(loss}after tax`. Rows are therefore found by **value anchor** on the
  standalone side (the row reproducing ≥2 stored quarters IS the PAT row) and by **fuzzy label**
  (difflib ≥0.86 against a canonical form) on the consolidated side.
* `declared_basis()` misses these pages too — `ConsolldatedFinancial` does not contain
  "consolidat". Fuzzy at 0.80 is safe: the worst real damage still scores 0.83 against
  "consolidated" and 0.18 against "standalone".
* The EPS figures sit on the caption's MIDDLE line (`items (net of tax expense) for the period`),
  usually on the statement's continuation page, whose header may detect a different number of
  columns — re-express it by (date, occurrence), never by index.
* Two OCR damage modes are tolerated in the CORROBORATING profit-tail identity and recorded, never
  in the value taken: a lost parenthesis flips the associates' sign (Sep-2024 prints (9.20)), and a
  stray digit shifts profit-after-tax by a power of ten (Dec-2024's 1,62,343 → 16234.31).

**⚠️ SEPARATE, STILL OPEN — the STANDALONE side is polluted in the same era.** Found while building
the control, NOT fixed (different defect, wider blast radius, needs its own sweep):
Dec-2024 stored std 1623.43 but the Dec-2024 AND Dec-2025 packs both print std **1621.35** — 1623.43
is that pack's CONSOLIDATED after-tax figure. Same shape at Sep-2025 (stored 2698.01 = the con
after-tax; the packs print std **2866.79**) and Jun-2025. The 9M/FY standalone columns confirm the
filings: 9M-FY25 4518.47 and FY25 6701.36 both reconcile with 1621.35 and not with 1623.43. So for
those quarters BOTH slots held the consolidated pre-associate number.
**Consequence for anyone re-running this:** the per-filing standalone control (A5/C3) fails on those
packs *because our stored standalone is wrong*, not because the reading is. That is why the tool
treats the standalone control as one anchor among several rather than a hard gate.

**What the fix bought on the REVENUE track, honestly (2026-08-06).** The corrected anchor unblocked
the con-revenue read for **Jun-2023 (₹11,165.84cr, con/std 1.0071 — inside GICRE's stored ratio
family 1.0031-1.0154, §55b's adjudication test)**, applied. The other eight targets still fail, but
for a DIFFERENT reason than before: not "no page anchored to stored con PAT" (that is gone) but
`std control failed: filing reads None` — `insurer_con_rev`'s revenue-leg patterns find nothing on
those packs' standalone pages, so A5 has no positive control. That is a reader limit on the REVENUE
rows, not an anchor problem; anyone resuming should fix the leg patterns (the PAT rows on those same
pages read fine, see 55c above), not the anchor.
⚠️ **And the same copy defect is visible in the revenue track**: `sf_revop` stores con revenue ==
std revenue TO THE PAISA at 20230930 (13,224.18) and 20240630 (12,822.55), where every other
populated quarter runs 1.003-1.05. The fill-only campaign cannot see these — they are non-null. They
need a §2b revenue correction of their own.
Cached re-runs: `scripts/fill2020_tools/insurer_con_rev_cached.py` wraps the tool with an on-disk
page-OCR cache (same `<SYM>_<qe>_<attachment>` keys 55c's audit already populated); it changes no
gate. Note it keys by a hash of the PDF BYTES — `fitz.open(stream=...)` documents have no `.name`.

**Also open, deliberately out of scope:** the COPY defect predates the fixed window. Comparative
columns show Mar-2021 (stored 1260.44, filings 1328.87), Sep-2021 (1010.55 vs 1348.15) and Dec-2021
(−28.48 vs **141.80**, two filings agreeing) are copies too. Not written — fixing three quarters of
an era nobody has swept is worse than leaving it; queued in §5 as a pre-2022 sweep.

### 55d. ★ OCR FALLBACK for corrupted text layers  (built 2026-08-06)
`insurer_con_rev.py` re-reads a filing with rapidocr (free, local, no API key — the §3 Gemini path
is still unwired) when the text layer yields no consolidated statement. Two things make it usable:
* **Normalised label matching.** OCR returns whole phrases with the spaces stripped
  ("PremiumEarned(Net)"), so every row pattern also has a normalised form (lowercase,
  alphanumerics only) — which incidentally made the text path immune to punctuation variants like
  ICICIPRULI's "Income from investments: (Net)".
* **Indian digit-grouping repair.** OCR reads "1,74,942" as "1.74942" — the comma becomes a
  decimal point. A value with 3+ digits after a single point and a short integer part is a mangled
  group, not a fraction (1.74942 -> 174942, 35.97353 -> 3597353), while ratios like 2.88 and 10.36
  are left alone.
Cost ~1-2s/page, so it is a FALLBACK, never the first read, and capped at 45 pages.
§0 says OCR mangles digits — it does, and that is precisely why nothing here trusts it: a mangled
digit fails the PAT anchor, the standalone control, or the ratio family. The reader is allowed to
be unreliable because the gates are not.

### 55b. ★★ SELECT A COLUMN BY ITS PRINTED DATE, NOT BY ITS INDEX
The fix that turned NIACL from "unreachable" into 15 anchored cells, and the right default for any
multi-column filing. These statements print a dated header row — `(30/09/2020) (30/06/2020)
(30/09/2019) (30/09/2020) (30/09/2019) (31/03/2020)` — so the period of every column is stated
outright. Read it (`header_columns()`: the y-band carrying the MOST date tokens, >=3 — page titles
and "Renewed from" lines contain dates too), slot every figure into those columns by right edge,
and the same index means the same period on every page of the filing.
* This is what makes the cross-page join safe: the profit page's owners vector is re-expressed in
  the revenue page's column space before anything is read.
* Match by **(date, occurrence)**, never by date alone — the SAME date heads both the quarter and
  the six-months-ended column, so "first column with this date" maps the YTD figure onto the
  quarter (`map_columns()`).
* Result: NIACL Sep-2020 reads 7,979.60 and Jun-2020 reads 6,923.24 — the index-based version gave
  both quarters 6,923.24. It also corrected Dec-2021, which had been given Sep-2021's figure.

**THE ADJUDICATION TEST for any insurer con read: the con/std RATIO FAMILY.** An insurer's
consolidated-to-standalone revenue ratio is remarkably stable, and we hold it independently for
2025+ from the XBRL. NIACL's 15 recovered cells run 1.0024-1.0061 against a stored 2025+ family of
1.0020-1.0054 — three independent confirmations (PAT anchor, standalone control, ratio family).
The same test REJECTED a LICI read that had passed the anchor: it implied 1.0079 where LICI's own
stored quarters run 1.0019-1.0046, so it was dropped rather than landed. Use it on every batch.

**And the guard that would have caught the original bug, now in the applier:** two quarters of one
company reporting the SAME revenue to the paisa is the fingerprint of a column misalignment. Real
revenue does not repeat exactly at these magnitudes. `insurer_con_rev.py --apply` refuses the whole
batch and prints the pairs. A per-filing standalone control CANNOT catch this — it passes on the
standalone page while the consolidated side is misread.

### 55e. ★ THE PDF READER GENERALISES — the residue's blocker is ATTACHMENT SELECTION  (2026-08-06)
`fill2020_tools/pdf_rev_reader.py` applies the insurer machinery (date-keyed columns §55b, OCR
§55d, anchor-by-PAT, cross-basis control) to ordinary industrial filings. It WORKS: TATAELXSI
Mar-2024 — a fully SCANNED filing, no text layer at all — reads revenue 905.94 against our stored
905.94, PAT anchored at 196.93 exactly, via OCR. Two findings from trying to run it over the
residue, both of which will recur:

1. **Cost.** OCR is ~1-2s/page, so a company with 11 gap quarters that yields nothing costs 15+
   minutes. Do NOT sweep the residue with OCR blind. Establish per-company that the document
   contains the statement you need BEFORE paying for the read.
2. **⚠️ THE ATTACHMENT IS USUALLY NOT THE RESULTS.** The earliest result-flagged BSE announcement
   after quarter-end is frequently a board-meeting outcome / cover letter, not the financial
   statements. ALKYLAMINE Jun-2024 fetched that way gives 7 pages and 13,996 chars with ZERO hits
   for both "consolidated" AND "standalone" — which reads exactly like "this company filed
   standalone only", the very inference the no-con identity route (§54b) is built on.
   **It is not true.** The §51b positive control kills it: point the same detector at ALKYLAMINE
   Jun-2019, a quarter whose stored con PAT (36.67) demonstrably differs from std (35.09) so a
   consolidated statement MUST exist — and it ALSO scores zero. The detector was not detecting.
   **Never infer "no consolidated statement was filed" from a keyword miss in one attachment**;
   control it against a period where the thing is known to exist, and if the control fails, the
   attachment is wrong, not the company's filing behaviour.
   Fixing this means selecting the attachment by content (a page carrying a revenue row and a
   dated header) across ALL result announcements in the window, not by date order — that is the
   next piece of work for this residue.

### 55a. An empty BSE announcement list is RATE-LIMITING, not absence
`fetch_insurers.datebound()` swallows a throttled response in its inner `except: break` and returns
`[]`, which reads exactly like "this company filed nothing that quarter". Seven NIACL quarters were
recorded as "no result filing" that way on 2026-08-06; every one returned two real filings when
retried on a fresh session. Retry an empty result on a NEW session (3 tries, backing off) before
believing it — `insurer_con_rev.anns_with_retry()`. Same family as §0's 162-byte stub rule: never
let an empty body mean "nothing exists".

---

## 56. ★★ CI CAN SILENTLY REVERT YOUR PUSH — the stale-snapshot clobber  (2026-08-06, fixed)

A backfill pushed 193 consolidated-revenue cells. Ten minutes later they were gone from the SERVED
file while still present in the ledger. Nothing errored; the push had been verified against origin
and reported as landed.

**The mechanism.** `refresh-fundamentals.yml` snapshotted the payloads at the start of its commit
step, then inside its push-retry loop did:

    git fetch origin main && git reset --hard origin/main   # picks up other writers
    cp /tmp/sf_revop.json docs/sf_revop.json                # ...and throws them away

The `reset` correctly pulled in the concurrent work and the blind `cp` immediately reverted it.
Any writer landing between a refresh's snapshot and its commit was erased. The 1,205 identity fills
of the same session survived only because they were pushed before that snapshot — luck, not design.
The values were recoverable solely because `scripts/revop_fundamentals.json` is deliberately NOT
committed from CI, so the ledger kept them: **the ledger-first rule is what made this repairable.**

### 56a. The fix — three-way merge, never a blind copy
`scripts/ci_preserve_merge.py`, wired into the workflow with a new baseline snapshot taken right
after checkout (before any step mutates the payloads). Per slot:

    ours != base  -> this run changed it on purpose -> take OURS (deliberate nulls included)
    ours == base  -> this run never touched it      -> take THEIRS (concurrent fill survives)

A plain fill-only merge is NOT sufficient and would introduce a different bug: `revop_sanity.py`
exists to null junk cells, and fill-only would resurrect every value it deleted. The three-way base
is what separates "CI removed this" from "CI never had it". Verified on the real race data: all 193
concurrent cells survive, a CI update still wins, and a CI-deliberate null is respected. Falls back
to the old copy on any unexpected shape, so it can never make a refresh worse.

### 56b. The detector — `scripts/verify_fills_live.py`
Re-checks every cell in every fill ledger against the served payloads.
`MISSING` = ledger has a value, payload has None → a clobber; `--repair` restores it.
`DRIFT` = both present but different → reported, never auto-changed (a later correction may
legitimately supersede a backfill; only a human decides). Exits 1 on any MISSING so a wrapper can
fail loudly. Negative-control tested: clobber one cell and it names that cell and exits 1.
Blocking in `refresh-fundamentals.yml` since 2026-08-10 — the step runs before the commit step, so
a red run means clobbered payloads were NOT published.

⚠️ **A MISSING can be a mis-keyed LEDGER entry, not a clobber — adjudicate before `--repair`
(AURUS 2026-08-10).** In `pat_defects.json` the key IS the slot: `correct_pat` → std slot 1,
`correct_pat_con` → con slot 3. AURUS|20170930's con-basis heal (batch b4dd72fb, "Con heals: …
AURUS …") was journalled under `correct_pat`, so the detector checked the STD slot — which was
None before and after the heal (never held a value) — and flagged MISSING for a week while the con
slot correctly served 7.62. `--repair` would have written 7.62 into stdPAT, fabricating a std value
no source asserts. Adjudication recipe: (1) read the entry's own `defect` text for the basis,
(2) `git log -S SYM -- scripts/pat_defects.json` → does the heal commit's message say std or con,
(3) diff the payload row at that commit — a slot that was None BEFORE the "clobber" was never
clobbered. Fix = re-key the entry (`correct_pat`→`correct_pat_con`, `stored_pat`→`stored_pat_con`)
so the guard moves to the slot the heal actually lives in; removal or a skip flag would drop the
clobber guard on a genuinely healed cell.

### 56c. The process rule this violated
CLAUDE.md rule 5 and §41 already say re-verify LIVE ~20 min after a data heal *because an in-flight
CI run may race you*. That step was skipped — verification happened at push time only, so a whole
batch was reported as landed when the served file had lost it. **"Verified against origin at push
time" is not verification.** Run `verify_fills_live.py` after the push AND after a refresh cycle.


---

## 57. ★★ THE ROUTE LADDER — never report a cell unreachable until all of it is walked  (2026-08-06, USER-MANDATED)

**Standing user instruction, 2026-08-06:** *"going ahead i dont want hear cant be filled or ur
assumption that data must not be there for it. i want every stock to be checked thoroughly by every
way u can use."*

Why it was needed: in one session three cells were reported out of reach and then filled within
minutes each, every time only after the user named the company.
* **ANGELONE 2025-03** — reported as "detres has no row, NSE list doesn't reach it". True, and
  irrelevant: the announcement PDF held it. Filled 1031.35 on five independent anchors.
* **ADANIGREEN 2025-03** — listed as remaining while the tool's own `MAX_QE` bound excluded it. It
  was never attempted. Filled 6461.00.
* **LICI 2023-06** — declared to "need the §43 IRDAI route rather than a P&L read", **judged by
  category without opening the document**. The filing had a clean standalone statement with a
  readable Total row. Filled 188749.16.

### 57a. The rule
1. A route returning nothing means **THAT ROUTE has no row.** It is never evidence the value does
   not exist.
2. Refusals are recorded as `not-found-via:<routes tried>`. The words "unfillable", "never filed"
   and "does not exist" may be used **only** when the whole ladder below has been walked AND a
   primary document says the company did not report (an exchange non-submission notice, a merger
   effective before the filing deadline, a delisting). See §51c and §52c for what that standard of
   evidence looks like in practice.
3. **Never infer absence from a company's category.** "It's an insurer / a bank / delisted /
   pre-2019" is a hypothesis about which route fits, never a conclusion about existence.
4. A cell excluded by a tool's own scope bound (`MAX_QE`, `--only`, a window constant) is
   **NOT ATTEMPTED**, and must be reported that way — never merged into a "remaining/residual" count
   that reads as "tried and failed".

### 57b. The ladder — walk it in this order, log every rung tried
| # | route | good for | ref |
|---|---|---|---|
| 1 | BSE detailed-results JSON | 2015+, quarter-keyed, standalone, incl. delisted | §42 |
| 2 | NSE archive detail pages | 2005+, BOTH bases, declares scale/basis/period, incl. delisted | §52, §53 |
| 3 | **BSE announcement PDF** | the one that keeps winning when 1 & 2 are blind | §6 |
| 4 | NSE announcements / integrated filing (+ the `.zip` gotcha) | recent quarters | §6 |
| 5 | XBRL cache / live XBRL | 2018+ | §54 |
| 6 | Comparative columns of the NEXT-quarter or year-later filing | quarters the company never filed alone (pre-IPO), or scanned own-filing | §6 `--rescue` |
| 7 | FY / 9M identity (annual − known siblings) | one missing quarter in an otherwise complete FY | §45, §53d |
| 8 | No-sub identity (con = std) | proven no consolidatable subsidiary | §6A, §53 |
| 9 | IRDAI public disclosures | insurers | §43 |
| 10 | Vision read (render/crop) | scanned PDFs with no text layer, or OCR-corrupted ones like GICRE 2024-12 | §17b |
| 11 | Wayback / archived exchange pages | pre-2008, dead endpoints | §32 |
| 12 | Acquirer's disclosures | merged/dissolved entities' stub periods | §51c |

### 57c. Traps that make a route look empty when it is not
* **A tight window around a STORED announce date finds nothing** — old announce dates are often
  `quarter-end+45d` defaults, not filing dates (§52). Search the post-quarter stretch.
* **An empty BSE announcement list is often RATE-LIMITING, not absence** — retry on a fresh session
  (§55a).
* **Glyph-corrupted text layers** defeat keyword search: "Standalone" extracts as "Slondolone".
  Search corruption-tolerant fragments and ALWAYS positive-control the detector against a period
  where the thing is known to exist (§51b).
* **Pre-2016 BSE attachments 404** on AttachHis/AttachLive (§52).
* **Blank-template pages** print every row as 0.00 — a zero is a blank row, not a result (§53b).


---

## 58. ★★★ THE STANDARD BACKFILL READ — announcement PDF + COLUMN ANCHOR  (2026-08-06, USER-MANDATED DEFAULT)

**User instruction 2026-08-06:** *"the way that got u 3 cells now should be added in rulebook for all
future backfills"* — and *"do the same in future for everything"*. This is now the DEFAULT method
for any cell the quarter-keyed indexes miss, and the reference recipe for every new backfill tool.

It is what recovered ANGELONE 2025-03 (revS 1031.35), ADANIGREEN 2025-03 (revS 6461.00) and
LICI 2023-06 (revS 188749.16) within minutes each, after both index routes had reported nothing.
§57 says WHICH route; this section says HOW to read it so the value is trustworthy.

### 58a. Why the index routes miss, and this one does not
`Corp_detailedResult_Transpose_ng` (BSE detres) and the NSE archive list are **derived indexes**.
A missing row there is a gap in the INDEX and says nothing about the filing. The BSE announcement
stream is the **primary record** — it is the filing itself. Empirically: detres returned an empty
row and the NSE list stopped short for all three cells above, while the announcement PDF had every
one of them.

### 58b. The recipe
1. **Announce date** — take it from the row you already hold (`sf_fundamentals` r[2] std / r[4] con).
   You are not searching blind; you know roughly when the company filed.
2. **List filings** — `fetch_insurers.datebound(sess, scrip, from, to)` over the announce date ±5-7d.
   Empty? WIDEN to the post-quarter stretch (qe+10d .. qe+160d) and RETRY ON A FRESH SESSION — an
   empty BSE list is often rate-limiting, not absence (§55a). Old announce dates are frequently
   `quarter-end+45d` defaults rather than real filing dates (§52).
3. **Fetch the attachment** — GUID → `corpfiling/AttachHis|AttachLive/<guid>.pdf`, via
   `backfill_revop_gaps.cached_pdf` so each PDF is downloaded once and reused forever.
4. **Pick the page by its DECLARED basis** — scan the first ~2,000 chars for `standalone` /
   `consolidat`. A page mentioning only one of them is that basis; a page mentioning both is a
   cover/notes page — keep scanning. Never assume page order.
5. **Read the LABELLED row**, never a position: `Revenue from operations` / `Total income from
   operations` / `Net Profit ... for the period` / for banks `Interest Earned`. For consolidated
   PAT prefer `...after taxes, minority interest and share of profit of associates` (owners basis).
6. **★ COLUMN ANCHOR — THE STEP THAT MAKES IT TRUSTWORTHY ★.** These statements print 4-6 columns
   (current quarter, prior quarter, year-ago quarter, YTD, full year). **NEVER take column 0.**
   Identify the target column by finding the column whose value REPRODUCES A VALUE WE ALREADY STORE
   for a neighbouring quarter or the other basis. Worked examples:
   - ANGELONE: PAT columns 1,802.58/3,010.28/3,460.16 (÷10) == stored std PAT 180.26/301.03/345.99
     for Mar-25/Dec-24/Mar-24 → column 0 is Mar-2025 → its revenue 10,313.46 → **1,031.35**. Two
     stored REVENUE comparatives matched too (1,245.99 / 1,346.99). Five independent locks.
   - ADANIGREEN: PAT columns 113/557/(195) == stored Mar-25/Dec-24/Mar-24.
   - LICI: the Jun-2023 PAT anchor deliberately FAILED (see 58d) so the read was anchored on the
     **Mar-2023 column** instead — PAT 13,427.81 == stored exactly, and that column's total income
     200,185.38 == stored revS 200,178.83 (0.003%).
   If NO column reproduces a stored value, the read is unanchored → **SKIP** (§6 constraint).
7. **Scale from the DECLARED unit** in the header — `Rs in millions` ÷10, `lakhs` ÷100, `crores` ÷1.
   Never infer scale from magnitude.
8. **Second, independent check on the same page** before writing:
   total income == revenue + other income (ANGELONE 6,461+314−7 = 6,768 ✓), or PBT − tax == PAT,
   or 6M/9M == ΣQ (SpiceJet −593.409 + −112.594 = −706.003 ✓).
9. **Write fill-only** and journal the whole anchor chain per cell in a TRACKED ledger.

### 58c. Guards that must be in every reader built on this
* a printed **0.00 is a blank row**, not a result — reject it (SUNTV, UJJIVAN/SFCL);
* **cumulative pages are YTD** — a Q2/Q3/Q4 cumulative row lands 6/9/12 months as a quarter;
* **glyph-corrupted text layers** ("Slondolone") defeat keyword search — use corruption-tolerant
  fragments and positive-control the detector (§51b);
* validate **basis / Period Ended / Symbol from the page body**, never from the index row.

### 58d. When the anchor fails, that is a RESULT — investigate, do not coerce
LICI 2023-06: the filing states standalone PAT 9,543.71 while we store 9,634.98, which is exactly
our stored CONSOLIDATED value — i.e. the std slot holds the con figure. The failing anchor EXPOSED a
wrong stored value. Correcting it is the §2b procedure, not a fill; the cell was landed on a
different (Mar-2023) anchor and the defect was journalled and reported. A backfill pass never
silently rewrites a value it was not asked to fill.

---

## 59. ★★ IS THE STANDALONE SLOT SECRETLY HOLDING THE CONSOLIDATED FIGURE?  (audited 2026-08-06)

The question, from the LICI 2023-06 case: `sf_fundamentals` stores std PAT 9634.98 AND con PAT
9634.98, while the filing states standalone 9,54,371.26 lakh = **9543.71** — the stored std is the
CONSOLIDATED number. How often does that happen store-wide?

### 59a. The screen is NOT a defect count — the measured rate is ~0%
Screen (reproducible via `scripts/stdcon_audit/screen.py`): take quarters where both bases are
stored; `divergent = |con-std| > max(0.05, 0.001*|std|)`; flag companies with ≥3 divergent quarters
that ALSO show non-divergent quarters SANDWICHED between divergent ones. That yields
**3,109 cells / 802 companies**. A stratified sample of **44** (era × revenue quartile, seed 20260806,
`sample.py`) was then run to the decisive test:

| verdict | n |
|---|---|
| OK — the filing's standalone PAT equals the stored std | **40** |
| DEFECT — std slot holds the consolidated figure | **0** |
| OTHER-DEFECT — stored std wrong, but NOT by holding con | **3** |
| N/A — stored std == stored con == 0.00, no information either way | 1 |

**0/43 = 0.0% (95% Wilson 0.0–8.2%).** Every sampled cell was resolved against a primary document,
so the band is real, not a coverage artefact. Do NOT plan a mass correction off the screen size:
the interior equal-runs it flags are overwhelmingly GENUINE — a subsidiary contributing ~0, or a
company whose consolidation difference is smaller than the tolerance (NESCO Sep-2019 std 73.96 vs
con 73.95, straight from both bases' XBRL).

**Where the defect DOES live: individual insurer quarters, not a company-wide pattern.** A targeted
pass over LICI's own nine flagged cells found **four confirmed defects** (Jun-2022, Dec-2022,
Jun-2023, Sep-2023) and **three confirmed-correct** (Sep-2022, Mar-2023, Mar-2024) — so it is
per-cell, and re-reading a company's other quarters is not optional. Proof for the whole set is one
identity (§45): the 10-Aug-2023 filing's standalone columns give
`682.89 + 15952.49 + 6334.20 + 13427.81 = 36397.39` = its own printed FY23 standalone annual,
**exactly**, while our stored quarters sum to 38331.79.

### 59b. The decisive test, and the ladder that answers it
Per cell: read BOTH bases from primary documents.
`std_page == stored_std` → OK. `std_page != stored_std AND con_page == stored_std` → DEFECT.
`std_page != stored_std AND con_page != stored_std` → OTHER-DEFECT. Anything else → record
`not-found-via:<routes>` (§57a), never "unfillable".

Rungs in yield order (`stdcon_audit/audit.py` walks all of them and logs each):
1. **NSE results XBRL, one document per basis** — `xbrl_route.py`. THE route for 2019+. Every
   `corporates-financial-results` row carries an `xbrl` URL and NSE files ONE XBRL PER BASIS, with
   the row declaring which. `resultDetailedDataLink` is empty for essentially all post-2019 rows, so
   the archive DETAIL pages (§52/§53) cover only the pre-2020 tail — that is why the first manual
   attempt found "no page" for 11 of 12 sampled cells. Nothing is assumed about context ids
   (§45 warns they have no fixed meaning): a context is used only if it declares
   `DateOfStartOfReportingPeriod`/`DateOfEndOfReportingPeriod` spanning exactly the 3 months ending
   on the quarter AND its `NatureOfReportStandaloneConsolidated` matches the basis being read.
2. **BSE detailed-results JSON (§42)** — standalone, 2015+, incl. delisted. See 58c.
3. **BSE announcement PDF** — both bases in one document; the winner when 1 and 2 are blind.
4. NSE archive detail pages (§52/§53) — pre-2020 tail.
5. Vision render (`render.py`, §57b rung 10) — scanned filings and damaged text layers.

### 59c. detres has no basis label — CALIBRATE it per scrip before believing it
§42 says the endpoint is "standalone/**primary**". *Primary* is the trap: if it served a
consolidated figure for a scrip whose std slot also held the consolidated figure, the two would
agree and the cell would be wrongly cleared. So every detres read is calibrated first:
* **CAL-REV** (free, same response): its revenue row must match stored revStd and differ from
  stored revCon.
* **CAL-PAT**: the nearest DIVERGENT quarters must come back matching stored std, not stored con.
  **Take a MAJORITY over up to three neighbours.** One is not enough — SHRJAGP's own stored
  Mar-2020 std is itself corrupt (it holds the Jun-2019 value), and a single-neighbour test
  therefore declared "detres is serving consolidated" for a company that has only ever filed one
  basis. A corrupt neighbour can outvote nothing; it cannot outvote two good ones.

### 59d. Traps this audit hit, every one of which silently returns a WRONG number
* **The column map must be READ, not assumed.** HDFCLIFE Mar-2020 was first read out of the JULY
  filing, whose columns are `[Jun-2020, Mar-2020, Jun-2019, FY20]` — "column 0 is the target" gave
  451.09 for a quarter that is 311.71, and the anchor hits were internally consistent with the
  shifted layout, so nothing caught it. Parse the printed header dates and take the column that
  NAMES the target quarter (leftmost — a fiscal-year column repeats the same date). This also turns
  every later filing into a legitimate comparative-column source (§57b rung 6) for free.
* **Match header cells to figures by RIGHT edge.** Statement numbers and header dates are both
  right-aligned; using PyMuPDF's `x0` for one and `x1` for the other put every column ~32pt out and
  matched nothing. Prefer ordinal mapping when #columns == #numbers.
* **Build the header from ONE row — the one with the most dates.** Accumulating dates across rows
  pulls the TITLE's date in as a phantom extra column (SHRI JAGDAMBA Jun-2020 mis-picked).
* **Numbers to the LEFT of the row label are ROW INDICES**, not data; including them shifts every
  column by one (LICI's `29 Profit/(loss)...`).
* **BSE files many quarterly results under "Outcome of Board Meeting"** with no "financial result"
  text in the headline or subcategory. `bse_vision.is_result()` alone misses them — LICI's Jun-2023
  results, the proven defect case, are filed exactly that way. Use a loose include-list plus the
  exclusion list (investor presentation / press release / transcript / newspaper / analyst), and let
  the content gates do the filtering.
* **A text cover page over a SCANNED body passes a "does this PDF have text?" check.** IDEA
  Mar-2024: page 0 has 1,795 characters, pages 1-22 have zero. Count pages with real text in the
  BODY (≥2 pages over 400 chars), then fall to vision.
* **`owners` and `NCI` both tagged 0.00 does not mean zero profit** — the split was simply never
  filed, and the period total IS the owners' figure (EIMCOELECO/ANIKINDS/JINDCOT here; §2d records
  the same for TRU Mar-26). Taking the tag literally lands 0.00 on profitable companies.
* **An anchor proves the column map, NOT the scale, when the stored anchor carries the same scale
  error.** LAHOTIOV Mar-2025: the filing says 404.11 LAKH = 4.04cr, we store 404.11 as crore, and
  the PDF read "confirmed" 404.11 by anchoring on stored Jun-2024 453.78 — which is the identical
  ×100 error. detres disagreeing by exactly a power of ten is the tell; treat it as a scale conflict,
  not a tie.
* **Small filers name no basis at all** ("UN-AUIDITED FINANCIAL RESULTS FOR THE QUARTER ENDED ON
  30.06.2020"). If the WHOLE document never says "consolidated" (§51b corruption-tolerant test),
  the statement in it is the standalone one.
* **A verified read still needs a human when the anchors contradict it.** Reads are tiered: A =
  column map confirmed by other columns reproducing stored values; B = header names the target and
  there is one number per column, but no anchor could be checked (or the stored neighbours are
  themselves wrong). Tier B is used and FLAGGED, never silently promoted.

### 59e. What the screen actually finds instead — and it is worth fixing
3 of 43 resolved cells (7%) hold a wrong standalone PAT for reasons unrelated to consolidation:
LAHOTIOV Mar-2025 = a ×100 lakh-as-crore error (§45 LEHAR class); NITTAGELA Mar-2021 stores 0.06
against a filed 1.54 (its Dec-2020 is wrong too); THRIVE Jun-2022 stores 0.08 against a filed -0.33
std / -2.31 con, with an announce date that PRE-DATES the filing. So this screen is a decent
*general* wrong-value detector even though it is a poor detector of the specific defect it was built
for. Route any repair through §2b (correcting a non-null value), not through a fill-only applier.

Tooling: `scripts/stdcon_audit/` — `screen.py` (the screen), `sample.py` (stratified draw),
`audit.py` (ladder + verdict), `xbrl_route.py`, `detres.py`, `nse_arch.py`, `scrips.py`
(delisted-aware scrip resolution, §52b), `render.py` (vision), `report.py` (verdict table + Wilson
interval). Evidence: `_audit.json` (every route tried, per cell), `_manual.json` (hand-confirmed
verdicts with their full derivation), `_screen.json`, `_sample.json`.
## 60. ★★★ screener.in IS A ROUTE — stop reporting "no data" when a second reader already has it  (2026-08-06, USER-MANDATED)

**The trigger, verbatim:** *"how the fuck screener has all and u r replying nothing exists? pls check
and resolve this as we need to backfill old quarters as well"*. Preceded by *"i dont want hear cant be
filled or ur assumption that data must not be there"*. Both are standing rules, not one-off gripes.

**The failure this fixes.** Every rung of §57 reads a FILING. When a filing was image-only (MCX),
labelled the top line in a layout my regex did not expect (AIIL: the finance layout prints
`Revenue`, not `Sales`), or interleaved both bases on one page (BALKRISIND), my reader returned
nothing — and I wrote that up as *the data does not exist*. It existed. screener.in had already
done the read. **A route failing is not evidence of absence. It is evidence about my reader.**

### 60a. What screener actually covers — MEASURED, not assumed

| table | reach | use |
|---|---|---|
| Quarterly (`/company/SYM/` std, `/company/SYM/consolidated/`) | **trailing ~13 quarters only** | the recent window; useless for 2015–2022 |
| Annual P&L | **12 full years, FY2015→FY2026, both bases** | the lever for OLD quarters, via §60d |

So screener does **not** "have all". Say what it has. It cannot see a 2018 quarter directly — but
its FY2018 total plus three stored quarters produces that quarter by subtraction.

### 60b. Read it with a SCRIPT, never with WebFetch prose

`scripts/screener_fetch.py`. Every `<th>`/`<td>` carries `data-date-key="YYYY-MM-DD"`, so **columns
are addressed by printed date** — the §55b rule, satisfied structurally rather than by discipline.

**WebFetch prose summaries are BANNED for financial cells.** Live proof: asked for CYIENT Mar-2025
it returned `Sales 1927`. The date-keyed table says **1909**; 1926.4 is our own stored **Dec-2024**.
The summary was shifted one column and would have written the wrong quarter's revenue into the
dataset with a confident-looking citation. Prose readers guess columns. Parsers do not.

Also: `_ROW` must be `<tr[^>]*>`, not `<tr>` — screener stripes its rows, and the strict pattern
silently returns an empty table (looks exactly like "company not covered").

### 60c. THE GATE — a screener number is never written on its own authority

`scripts/fill2020_tools/screener_gate.py`. Before writing, screener's own series for that field must
reproduce **≥2 values we already store, with ZERO disagreements**. One disagreement ⇒ different
entity or different basis ⇒ **reject the whole series, never cherry-pick the one cell you wanted**.

What the gate caught on its first run:
* **TMPV** — screener shows the demerged passenger-vehicle company; our series is legacy Tata Motors
  **including JLR** through Jun-2025 (they converge only from Sep-2025). 7/12 agreement. A blind copy
  writes a number ~20,000 cr wrong and *looks* perfectly plausible next to its neighbours.
* **SWANCORP** — 11/12, and the one disagreement is **ours**: stored `20240331` revC = 7.91 against
  screener 1398, while Jun/Sep/Dec-2024 match to the paisa. The gate finds our bad cells too.

Try both row labels: `Sales` **and** `Revenue`. Gating on `Sales` alone returns "quarter absent" for
every bank, NBFC and investment company — the same bank-format trap as §53.

### 60d. OLD quarters: FY annual − the 3 stored quarters

`scripts/fill2020_tools/screener_annual_sweep.py`. Three gates, all mandatory:

* **Gate A (entity/basis).** For FYs where we hold all 4 quarters, our sum must reproduce screener's
  annual. Measured: where entity and basis match it agrees to **±0.01%**. Require ≥3 agreeing FYs and
  ≥60% agreement.
* **Gate A2 (the year itself).** Disagreements are **not noise — they are restatement/demerger years**
  (ACC FY2023 −19.9%, AARTIIND FY2022 +13.6% = Pharmalabs demerger, ABCAPITAL FY2023 −21.5%,
  ADANIENT FY2024 +9.4%). Screener carries the **restated** total; our quarters are **as-reported**.
  Subtracting one from the other yields a garbage residual that passes every plausibility check.
  **Reject the YEAR, not the company** — and reject years *adjacent* to a restated year too.
* **Gate B (residual sanity).** Derived value must be >0 and within 0.2×–5× the median sibling
  quarter. Catches the case where one of the three *stored* quarters is itself wrong
  (AIIL 20230331 derived −118.36 against siblings 89/93/311 — correctly refused).

### 60e. screener is the SEARCH KEY, not the answer

It prints **crore-rounded integers**; we store filing precision. So use its value to go *find* the
exact figure — knowing the answer is "about 2752" makes the PDF read trivial and, crucially,
**eliminates the column-index guess** that caused the BALKRISIND/SWANCORP near-misses: pick the cell
in a revenue-labelled row landing within ±1 of the target, at any declared scale. That cell is the
quarter. `refine_from_filing.py` (own filing) and `refine_via_nextyear.py` (the Mar-(Y+1) comparative
column, and the Jun-Y filing's preceding-quarter column). 6 of 10 Mar-2025 cells landed at filing
precision this way; CGPOWER 2752.77 and NMDC 7004.59 came from the *next year's* filing after the
own-quarter PDF gave nothing.

**When refinement fails, still fill.** Write the crore-rounded value with
`precision: "crore-rounded"` in the ledger. A sourced approximation with honest provenance beats a
hole — but it must be *labelled*, so a later pass can refine it and nobody mistakes it for a filing
read. (BSE's announcement API throttles to 0 filings without erroring; that is a *run-time* condition,
not a property of the cell — §52c.)

### 60f. The rule, generalised

**Before any cell is reported as unfillable, a SECOND INDEPENDENT READER must have been tried and
must also have come back empty.** screener.in is that reader for Indian listed equities. If our
reader says nothing and screener says something, the defect is in our reader — go and find it. Log
which route produced the value and which routes failed, per cell.

## 61. ★★★ READER-FAILURE MODES — an empty result is a DIAGNOSIS, not a conclusion  (2026-08-06, USER-MANDATED)

**The instruction:** *"all that u mentioned in table . pls add them in runbook so that future backfills
should never stay empty and get filled"*.

Every cell below was reported by me as *unfillable*. Every one of them existed. In each case my
reader returned empty for a reason that had **nothing to do with whether the data exists** — and
because an empty parse and a genuine absence look byte-identical, I reported both with the same
confidence. **When a read comes back empty, the first question is never "is the data missing?" —
it is "which of these six things just happened?"**

### 61a. The six modes, their signatures, and the counter for each

| # | mode | signature you can TEST for | the counter — do this, do not conclude absence |
|---|---|---|---|
| 1 | **Image-only statement** (MCX) | `len(page.get_text().strip()) < 80` **and** `page.get_images()` — MCX's consolidated page extracts **11 characters** | render the page and read it (§57 rung 10). The filing's own TITLE said "Consolidated"; the text layer is not the document |
| 2 | **Layout-specific row label** (AIIL) | no revenue row matched, but the doc clearly IS a results statement (PAT row found, period header found) | widen the label set. Finance/NBFC/bank layouts print **`Revenue`**, not `Sales`/`Revenue from operations`. Same trap as §53's bank format. Never gate on one label family |
| 3 | **Value lives in a different filing** (CGPOWER, NMDC) | own-quarter PDF parses fine and simply lacks the number | **every quarter is printed in ~3 filings**: its own, the **Q+1** filing (as the *preceding-quarter* column), and the **Q+4** filing (as the *year-ago comparative* column). CGPOWER 2752.77 and NMDC 7004.59 both came from Q+4 |
| 4 | **Transport failure dressed as absence** (CYIENT, WAAREEENER) | API returns **0 rows with HTTP 200** — no exception, no error text | 0 filings is a **run-time condition, not a property of the cell** (§52c). Retry x3 with a fresh session and backoff; cross-check the window against detres/NSE. Still zero => state `BLOCKED-TRANSPORT`, **never** `NOT-FILED`. A ~20 ms connection refusal means *this process has no network* (e.g. a backgrounded task, which runs sandboxed) — rerun in the foreground |
| 5 | **Both bases interleaved on one page** (BALKRISIND) | two plausible candidates for one row; column index reused across rows (§55b) | **refusing is correct — reporting it as unfillable is not.** The state is `NEEDS-CROSSCHECK`. Get a target from a second reader, then anchor to it: the cell landing within ±1 of the target IS the quarter. That turned 2746.59-or-2752 into a confirmed **2752.38** |
| 6 | **OUR stored cell is the wrong one** (SWANCORP) | cross-check agrees on every quarter to the paisa **except one** | the indictment is against **us**, not the source. SWANCORP `20240331` revC = 7.91 vs 1398 while Jun/Sep/Dec-2024 matched exactly. Log the suspect cell; do **not** let it veto the fill you came for |

### 61b. Terminal states — a backfill pass may only end a cell in one of these

Anything else is an unfinished job reported as a finished one.

* `FILLED-EXACT` — filing precision, column anchored per §58.
* `FILLED-ROUNDED` — second reader, crore-rounded, **labelled `precision: "crore-rounded"`** in the
  ledger so a later pass refines it and nobody mistakes it for a filing read.
* `NOT-APPLICABLE` — the company does not file that basis at that quarter; evidence in
  `scripts/no_con_filing.json` (§60c, rolling-4-quarter rule).
* `BLOCKED-TRANSPORT` — retry later. **This is not a conclusion and must never be counted as closed.**
* `NEEDS-VISION` / `NEEDS-CROSSCHECK` — work items, mode 1 and mode 5. Also not conclusions.
  **`NEEDS-VISION` is deferred to the very END of a sweep and needs the user's explicit go-ahead**
  (standing rule, 2026-08-06: *"the one that requires vision, keep them for last and take my
  permission"*). Vision is the expensive rung; every cheaper route must be exhausted across the
  WHOLE target list first, because reader improvements retroactively rescue queued cells — widening
  the label set (§63a/b) and adding the own-season window (§63e) turned several `NEEDS-VISION` cells
  into ordinary text reads on this very sweep. Re-run the text reader over the queue after any
  reader change, then report what is left and wait.
* `UNFILLABLE` — requires **two independent readers** to have come back empty, with both recorded.
  One reader failing is mode 1-6 until proven otherwise.

### 61c. The standing rule

**Never report a cell empty without naming which of 61a you ruled out.** "My parser found nothing"
is a statement about the parser. Depth in the ladder is not breadth: §57's twelve rungs all read the
same document class, so twelve failures are one failure repeated. Add the *independent* reader
(§60) before adding a thirteenth rung.

## 62. ★★★ COLUMNS ARE GEOMETRY, NOT LIST INDICES — the fix that finally killed the §55b trap  (2026-08-06)

Discovered while building the §61 reader, by watching that reader reproduce the exact bug it was
written to prevent.

**The bug, one line:** `values[i]` from one row does **not** mean the same period as `values[i]` from
another row. PDF text extraction *linearises* a table, and rows legitimately differ in width — merged
cells, a notes column, a spanning sub-total, a footnote marker parsed as a token. So an index learned
from the PAT row is meaningless on the revenue row.

**Watch it happen.** The reader anchored BALKRISIND's Mar-2025 column correctly on the stored
consolidated PAT, carried index 5 across to the revenue row, and returned **170.61** — the precise
wrong number I had refused by hand days earlier. It passed the "a second column reproduces another
stored quarter" confirmation, because that check *also* used indices.

**The dead end.** Requiring the two rows to have equal width is safe but nearly blind: yield fell to
1 of 7. Correct answers were being thrown away along with the wrong one.

**The fix — `scripts/fill2020_tools/geom_read.py`.** Stop using indices. Financial statements
**right-align** their figures, so a period column is a stable vertical band of x-coordinates across
every row of the table. `page.get_text("words")` gives each token's bounding box, so:

1. group tokens into visual lines by `y` (≈3pt tolerance);
2. on the PAT row, find the token reproducing our stored PAT for that quarter+basis at some declared
   scale — **that token's right edge `x1` is the column**;
3. on the revenue row, take the token whose right edge falls in the same band (±14pt — columns sit
   40–90pt apart, so this is comfortably unambiguous);
4. confirm with a **different band** on the same rows reproducing a **different** stored quarter.

Two rows may now differ completely in width and the read is still right, because the column is a
**geometric fact about the page** rather than a guess about list positions. BALKRISIND immediately
returned **2752.38** — matching the value found independently, and reached without screener.

**Rules.**
* Any new PDF table reader uses `get_text("words")` and x-bands. Never `split("\n")` + index.
* Compare **right edges** (`x1`), not centres or left edges — only the right edge is stable when
  figures vary in digit count.
* Keep the linear reader only as a fallback *after* the geometric one returns nothing, never before.
* A confirmation check that uses the same addressing scheme as the thing it is confirming proves
  nothing. Confirm across a different axis.

## 63. ★★★ THE LABEL AND WINDOW GAPS THAT FAKED "NO DATA" — found by making the reader work  (2026-08-06)

Built the §61 reader, pointed it at the cells I had called unfillable, and every remaining failure
turned out to be one of these. None was missing data. All five are now in
`scripts/fill2020_tools/universal_read.py`; the campaign's Mar-2025 residual went from
"5 unreachable" to **5 read at filing precision**, each independently confirming screener's
crore-rounded value.

### 63a. Revenue row labels — the Ind-AS 115 caption

**`Revenue from contracts with customers`.** CYIENT's Q4 filing prints this and *nothing else* — no
"revenue from operations" line exists anywhere in the document. A fully-text, perfectly readable
16-page filing therefore looked like it had no revenue row at all. Add to the family, high priority:

    revenue from contracts with customers | revenue from sale of ... | turnover | gross sales

### 63b. PAT row labels — the owners figure is often a CONTINUATION line

The owners-attributable number frequently sits under a `Profit attributable to:` header, so **its own
caption contains neither "profit" nor "owners"**:

    Shareholders of the Company | Equity shareholders of the Company | Equity holders of the parent

CYIENT prints `Shareholders of the Company` = `1704 | 1223 | ...` in ₹ **million** — i.e. our stored
con PAT **170.4** for Mar-2025 and **122.3** for Dec-2024, both exact. Perfect anchor, invisible to a
pattern that insists on the word "owners".

### 63c. A widened regex that silently NARROWS

Replacing `pr[o0][fl]i?[lt].{0,20}f[o0]r\s+the\s+peri[o0]d` with
`pr[o0][fl]i?[lt]\s*/?\s*\(?l[o0]ss\)?\s*f[o0]r\s+the\s+...` made the word **loss** *mandatory*
(`\(?l[o0]ss\)?` only makes the parentheses optional). Plain `Profit for the period (VII-VIII)` —
BALKRISIND's consolidated PAT row, the literal anchor — stopped matching, and the cell that had just
been read correctly regressed to unreadable. **Every alternative must be wrapped `(?:...)?`, and any
label-set change must be re-run against cells already known to read.** Keep a regression set.

### 63d. Pick candidates by LABEL SPECIFICITY, never by page order

Widening the label sets to unlock CYIENT broke BALKRISIND, because a newly-matched but *less*
specific row now came first and first-match won. Collect **every** valid reading on the page and
choose the lowest `(pat_label_rank, rev_label_rank)`, patterns ordered most-specific-first. Order of
appearance on a page carries no meaning; specificity does.

### 63e. The stored announce date is a FEED date — always sweep the season too

`ann` in our target files is routinely stale or points at a different event. CYIENT's says
25-May-2025; it actually filed Q4 on **24-Apr-2025**, so a ±8-day window returned **0 rows** — output
shaped exactly like a throttled API (§61a mode 4) but caused by *targeting*, not transport. Always
add an `own-season` window of `quarter_end + 8d .. + 93d` alongside the announce-date window. This
alone recovered AIIL and WAAREEENER.

### 63f. Route boundary: NSE's per-company results index stops at Dec-2024

`corporates-financial-results?symbol=X&period=Quarterly|Annual` returns nothing with
`toDate` in 2025 or later — CYIENT has 166 quarterly rows, latest `31-Mar-2024`. So the XBRL route
(§57 rung 5, `nse_xbrl_rev.py`) **cannot serve 2025+ cells at all**; BSE is the only route there.
This is a property of the API, not of the data. Record it as a boundary so nobody re-derives
"NSE has no filing" as "the company did not file".

## 63. ★★★ NOT-APPLICABLE WAS CIRCULAR — never infer "the company doesn't file it" from your own gaps  (2026-08-07, USER-CAUGHT)

**The trigger:** *"check na ones once again. dont assume"* → *"verify it from 2-3 sources"*. The
classification did not survive the check, and the coverage numbers I had been reporting all week
were too flattering as a result.

### 63a. The bug

`audit_coverage` marked a consolidated cell NOT-APPLICABLE when stored con PAT showed no
**divergence** from std in the trailing four quarters. Divergence was read from **our own
sf_fundamentals** — so a company whose con PAT was merely *missing* generated no signal and was
recorded as *"does not file consolidated"*.

**We were concluding the data does not exist because we do not hold it.** And pre-2020, where con
PAT is thinnest, is exactly where it fired hardest.

### 63b. Three independent sources, all agreeing it was wrong

| source | result |
|---|---|
| screener consolidated **annuals** (reach FY2015) | con annual ≠ std annual on **99 of 158** tested company/FY pairs — **63% misclassified** |
| our own history, read differently | **96 of those 99** have divergent con PAT in *other* quarters — they demonstrably consolidate |
| NSE filing index (`corporates-financial-results`) | **11 of 12** sampled list `consolidated == "Consolidated"` rows (AJANTPHARM 42, APARINDS 35, 63MOONS 28) |

Among the companies written off as non-consolidators: **ONGC, ITC, HDFCBANK, NTPC, IOC, HINDALCO,
M&M**. The absurdity of that list is the tell — when a screen excludes the largest consolidators in
the index, the screen is broken, not the companies.

### 63c. The replacement — POSITIVE evidence only

`scripts/fill2020_tools/build_con_filer_evidence.py` → `scripts/con_filer_evidence.json`.
A company consolidates if **any** independent source says so:
* **E1** own-history divergence in **any** quarter, ever (con PAT *or* con revenue ≠ std);
* **E2** screener's consolidated annual differs materially from its standalone, any FY.

**The asymmetry is the whole point: one divergent quarter anywhere PROVES consolidation exists;
absence across four quarters proves nothing about the company, only about our coverage.**
Before a company's earliest evidence, claim nothing either way — neither a gap nor an exclusion.
The user-verified ledger (`no_con_filing.json`) still wins where it applies: those were checked
against screener by hand, and a stop date is positive evidence of a stop.

Measured: **1,018 of 1,254** Nifty-500 members have positive evidence of consolidating; 236 do not.

### 63d. What it cost to have been wrong

| | before | after |
|---|---|---|
| 2015–2019 open revC | 941 | **2,911** |
| 2015–2019 open patC | 136 | **2,069** |
| campaign window | 131 open / 92.5% | **193 open / 89.0%** |

`build_targets.py` was emitting **1,600** pre-2020 targets against a real **4,147** — it was
skipping precisely the cells the broken rule had hidden. Any tool that reproduces the gap definition
must be updated together with the audit, or it silently inherits the bug.

### 63e. The generalised rule

**An exclusion must rest on evidence ABOUT THE COMPANY, never on the absence of our own data.**
Before any "not applicable", "never filed", "no data exists" classification ships:
1. state which independent source establishes it;
2. test it on a sample against **2–3** sources — the user asked for this explicitly and it took one
   run to overturn a rule that had shaped the whole campaign's scoreboard;
3. sanity-check the *implications*: if a classification excludes ONGC and ITC from consolidated
   reporting, stop and look, because the population it selects is telling you it is wrong.

## 64. ★★ THE ANCHOR DEPENDENCY — con PAT must be filled BEFORE con revenue is reachable  (2026-08-07)

Every reader in this campaign identifies its target column by matching a value we **already store**
(§58). For consolidated revenue that anchor is consolidated PAT. So a cell where **both** are missing
is unreadable by construction — not "hard", *impossible* by this method.

**Measured, and unambiguous.** Of the 4,147 pre-2020 consolidated-revenue targets, **2,744 (66%)
have no stored PAT on that basis**. Of 203 successful pre-2020 filing reads, **203 came from the
1,403 anchored cells and ZERO from the 2,744 unanchored ones**.

**Consequence for sequencing: the PAT track is not optional and cannot follow the revenue track — it
gates it.** Standalone PAT is complete pre-2020 (patS = 0 open in every quarter 2015-2019), so the
standalone statement can be located; the consolidated statement must then be found *relative to it*.

The anchor-free alternative is to address columns by the statement's own **printed period dates**
(`date_columns.py`). Groundwork committed, deliberately NOT wired to any writer — measured over 602
cached statement pages it found quarter dates on 54%, averaged **1.5 columns per page against a real
3–5**, and picked dates out of narrative notes. What it needs is recorded in its docstring: restrict
detection to a header BAND, group same-y dates and require ≥2, treat a lone date as prose.

**Split any target list by anchorability before spending a run on it** — mixing the two pools makes
a reader look like it is failing when it is really being handed impossible work.


---

## 65. ★★★ FIVE SILENT-REVERT / SILENT-BLINDNESS BUGS  (found 2026-08-09, all fixed)

None of these threw an error. Each produced plausible output while being wrong, which is why they
survived. Grouped because the shape repeats: **a correction that is journalled is not a correction
that is live, and a reader that returns nothing is not a source that has nothing.**

### 65a. ROWS ARE GEOMETRY TOO — `round(y0/ytol)` tears statement rows in half
`geom_read.py` exists because columns were being addressed by list index (§62). It fixed columns
and left ROWS as `rows.setdefault(round(y0 / ytol), ...)` — the same defect one axis over. A bucket
has hard edges, so a row whose tokens sit at y=388.4 and y=389.3 is split: 388.4/3 rounds to 129,
389.3/3 to 130. The label keeps whichever fragment shares its bucket; the rest becomes an orphan
numeric row belonging to no label. Statements typeset figures with sub-point baseline jitter, so
this is routine.

TIMKEN 2024-12-31's consolidated PAT row extracted as
```
y389.3  "5 Net Profit after tax (3-4)"  545.56@302  935.99@347
y388.4                                  782.0J@381  2,565.82@444  2,718.89@491  4,621.94@540
```
— and the Dec-2024 column the read needed was in the half with no label attached. The reader
reported "no confirmable consolidated column" for four straight attempts.

**Fix: `geom_read.band_rows()`** clusters on the GAP between consecutive baselines instead. Intra-row
jitter measures 0.4–1.2pt and line pitch 6–12pt, two cleanly separated populations. **Still carrying
the bucket form:** `date_columns.py:91` and `:171`, `insurer_con_rev.py:380`, `gicre_con_pat.py:197`.
Port them to `band_rows` before trusting any of those readers again.

### 65b. THE FLAG'S DIRECTION IS NOT THE DEFECT'S DIRECTION
`detect_con_copy` fires when our con slot equals our std slot while a second source says they
differ. That proves a cell is wrong; it does **not** say which slot holds the error. Decide it with
no PDF at all:

| our stored value matches | meaning | fix |
|---|---|---|
| screener's **CON** | our con is right | the **STD** slot holds the copy (MIRROR) |
| screener's **STD** | our std is right | the **CON** slot holds the copy |
| neither | a third defect | investigate, do not write |

Splitting the 16 open cells this way gave **6 con-copy, 9 mirror, 1 neither** — not the 11-plus-5
previously recorded. Four cells booked as "no confirmable consolidated column" were mirrors all
along: `read_con_copies` only opens pages whose own wording says CONSOLIDATED, so on a mirror cell
it is filtered away from the very statement holding the answer. It could never have found them.
The reader now takes `--basis std`.

### 65c. A HEAL LEDGER MUST OUTRANK THE EXTRACTION CACHE
`_reattr_owners.json` is built from the same XBRL as everything else, so when a con cell was wrong
at ingestion the cache holds the identical wrong number — and `apply_owners_full.py` runs nightly,
**after** the heal. TATACOFFEE 2022-12-31 was read from its own consolidated filing as 38.4
(screener 38; our standalone 26.61) and reverted to the cached 26.63 every night. The heal was
journalled, committed, and never once live. Six more cells were reverted the same way by scale:
METROBRAND, NAVNETEDUL, PARKHOTELS, JUBLPHARMA, NDTV (npCon −467.5 for a true −46.75), PAYTM.
`scale_fix.factor_cell(sym, qe, basis)` covers writers that never see a filename, and
`con_copy_heals.json` now wins outright in `apply_owners_full`.
**Rule: after writing any heal, check every nightly job that touches the same slot.** A journalled
correction proves only that you wrote it down.

### 65d. HALF-YEAR CUMULATIVES READ AS THE QUARTER — the Sep-2025 bug, at source
SEBI's Integrated Filing format (live from the 2025-03-31 half-year) lets a company file its
half-year with the SIX-MONTH figure in context `OneD`, period 2025-04-01..2025-09-30, 182 days.
`build_revop.parse_file` has rejected that since the day it was written. **`xbrl_revop` was added
the next day (2026-07-01) as a copy of the same logic without the check** — and it, with
`build_fundamentals.xbrl_profit`, is what the daily updater actually calls. 33 cells at 2025-09-30
stored a YTD figure as the quarter (UDS revC 1429.59, HBLENGINE, BELRISE, EDELWEISS, STYL, SHAH,
PROSTARM…). They were healed via `cumulative_heals.json` while the parser was left alone.

**It is a half-year-end effect, not a Sep-2025 one**, and it recurs every six months: of 871
long-period contexts in the 104k cache, all sit at 2025-03-31 (224), 2025-09-30 (393) and
2026-03-31 (249). Fixed with `build_revop.is_quarter_ctx`, gating both `OneD` and `FourD` in
`xbrl_revop` and `xbrl_profit`. Unknown period counts as ACCEPT — ~1,220 cached filings state no
period and rejecting them would delete real data (measured cost of the guard on those: 0 cells).

Two things to know before touching it again:
* **At Sep-30 a long context is genuinely cumulative (258 of 266 labelled); at Mar-31 it usually is
  NOT** — 92 of 117 carry the real Q4 under a mislabelled Oct→Mar range (IOC, DIVISLAB, LTTS,
  SYRMA, JKCEMENT…). Header semantics are byte-identical between the two kinds, so the guard costs
  those Mar cells going forward. They become visible gaps, fillable by the ordinary ladder.
* **A discriminator exists, and the transfer test says use it for MARCH ONLY.**
  `long_value / our stored prior-quarter`: cumulative ≈2, quarter ≈1. On the LABELLED population
  (both a long and a short filing cached, so truth is known) threshold 1.40 scores **371/383 =
  96.9%**, erring 3 toward corruption and 9 toward gaps.
  That population is not where the rule would run, so it was checked against the LONG-ONLY cells:

  | | n | median ratio | share predicted QUARTER |
  |---|---|---|---|
  | labelled, Mar | 117 | 1.05 | 74% |
  | **long-only, Mar** | 110 | 1.10 | **79%** |
  | labelled, Sep | 268 | 2.07 | 4% |
  | **long-only, Sep** | 54 | 2.03 | **33%** |

  March transfers — same median, same shape. **September does not**: a third of long-only cells fall
  below the threshold against 4% in the labelled set. The likely reason is circular input — those are
  the poisoned Sep cohort, so the "prior quarter" the ratio divides by may itself be a cumulative.
  **NOT WIRED anywhere.** If it is ever wired, restrict it to Mar-31 and require the prior quarter to
  come from a filing that passed `is_quarter_ctx`.
* PAT, operating profit and EBIT were never healed for the original 33 — `cumulative_heals.json`
  covers revenue only.

### 65e. A TRACKED TOOL HARDCODED ONE WORKTREE
`universal_read.py` read `WT = ~/stocks-wt/fill2020` literally, `sys.path.insert(0, ...)`-ed it and
`os.chdir`-ed into it. Every importer therefore got that one worktree's `geom_read` /
`date_columns` / `fetch_insurers` **and its `sf_revop.json` + `sf_fundamentals.json`** — the anchors
every read is scored against. Run a reader from any other tree and it silently anchors on another
tree's data, with nothing in the output to show it. It went unnoticed only because the hardcoded
path happened to be the active campaign's tree. Now derived from `__file__`, with `STOCKS_WT` as an
explicit override. **Never hardcode a worktree in a tracked tool** (memory:
analyze-live-not-local-bin).

### 65f. ★★★ OWNERS vs TOTAL — the con-copy tripwire's unavoidable false positive
We store consolidated PAT **owners-attributable**; **screener quotes TOTAL consolidated PAT**. Where
NCI is material those are different numbers, so a company can satisfy BOTH halves of the con-copy
test with no defect at all:

> **TATACOFFEE 2022-12-31.** Filing: owners 26.63 + NCI 11.77 = total 38.40; standalone 26.61.
> Our con 26.63 (owners, **correct**) sits 0.02 from our std 26.61 *by coincidence* → "ours con ==
> std". screener shows con 38 vs std 27 → "they differ". Flagged, read against screener's 38,
> healed — and a correct value was destroyed. Reverted 2026-08-09.

**A flag means "adjudicate this cell", never "this cell is a copy."** Before believing one, check
whether screener's con minus our con is simply the NCI: if the filing reconciles
`owners + NCI == total` and our value equals OWNERS, there is no defect.

`read_con_copies.py` now uses screener **only to locate the COLUMN** and reads the value off the
**OWNERS row** (`OWNERS_RE`) when the page has one — the column was never the problem, the ROW was.
This is §58 step 5, which the reader had not been honouring.

**How thin the check on the other heals is, stated plainly:** of the 18 journalled con-copy heals
only **one** has a consolidated filing in `_xbrl_cache` to test against — and that one was wrong.
The other 17 are not cleared; they are unmeasured by this route (TIMKEN never filed consolidated to
NSE, AADHARHFC listed in 2024, etc.). Re-adjudicate them from the BSE announcement stream before
treating them as settled. Also open: **ACUTAAS 2023-06-30 revC**, where the consolidated XBRL says
RevenueFromOperations 142.35 while the heal wrote 153.72 from the PDF — filer's own XBRL and filer's
own PDF disagree, so it needs adjudication rather than a preference.


---

## 66. ★★★ PRE-2020 CON PAT VIA THE STANDALONE ANCHOR — measured, and NOT fit to write  (2026-08-09)

§64 established that con PAT gates con revenue. Measured against the real target list:
**2,740 of 4,055 pre-2020 con-revenue targets (68%) have no stored con PAT — and ZERO have neither
basis.** Standalone PAT really is universally available, so it is the only way in, exactly as
scoped. The question was whether anchoring on it works.

### 66a. The gate, and what it is worth
Require the SAME parse, on the SAME document, to first reproduce the STANDALONE PAT we already
hold at a declared scale; abort the document if it cannot. Held-out scoring (truth = con PAT we
already store, never shown to the reader; every document bound to its issuer by its own scrip code;
only cells where con PAT differs from std by >2%, so reading the standalone page by mistake cannot
score as correct):

| | exact | attempted | accuracy | abstain |
|---|---|---|---|---|
| **gated** (std check must pass) | 9 | 12 | **75%** | 93% |
| **ungated** (same reader, gate off) | 7 | 21 | **33%** | — |

The gate more than doubles accuracy and reproduces the previously measured ~35% when removed —
which is itself a useful confirmation that 35% was real. **But 75% is not 90%, and reach on the
population it would actually run against is 4.6% (4 gated reads of 87 unanchored targets).** At
those rates a full sweep of the 2,740 would touch ~125 cells and get ~30 of them wrong.
**DO NOT WIRE IT TO A WRITER.** It abstains honestly, which is the right failure mode, but it does
not yet earn write access.

### 66b. What the measurement MOVED — the wall is the COLUMN now, not the row
The previous attempt blamed row disambiguation on consolidated pages (several profit lines: before
tax, after tax, total comprehensive, owners vs NCI). **That part is solved**, and without trusting
labels: `owners + NCI == the post-tax profit line`, evaluated AT THE TARGET COLUMN, picks the
owners row arithmetically.

The failure moved one step along. The standalone page verifies the DOCUMENT, the SCALE and the ROW
— **it does not verify WHICH COLUMN of the consolidated page is the target quarter.** Ordinal
transfer between the two pages is **50% (4/8) even when the pages have equal column counts**, and
only 60% (3/5) when their header-date sequences also agree. That is the next thing to solve, and it
is what any further work should attack.

### 66c. 2015-2017 is NOT a reader problem — do not tune the reader for it
**55 of 58 cells in that window fetched ZERO BYTES**: the GUID-named BSE attachment 404s on both
AttachHis and AttachLive. §52's "pre-2016 BSE attachments 404" understates it — the dead window
extends past 2016 (AMBUJACEM Oct-2017 probed directly: 404 on both). **Rung 3 is blind for
2015-2017; rungs 1-2 (BSE detres, NSE archive detail pages) are the route there.** Any measurement
of a PDF reader over that window is measuring transport, not reading.

### 66d. Two anchor lessons and two new suspects
* **A PAT-only anchor can verify the DOCUMENT while sitting on the WRONG QUARTER.** AARTIIND's
  Sep-2020 standalone PAT is 136.38 and its Dec-2019 is 136.82 — inside a 0.4% anchor tolerance. The
  read passed and was on the wrong column. The prototype therefore requires a SECOND, different
  lock: the same column must also reproduce stored `revS` on the revenue row. Any anchored reader
  should carry two locks on two different ROWS, not two columns of one row.
* **Consolidated row disambiguation is solved arithmetically, not by labels** (OCR wrecks captions):
  search candidate rows for `owners + NCI == the post-tax profit line` **evaluated at the anchored
  column**. That also separates the profit split from the identically-worded total-comprehensive
  split. Needs punctuation-normalised labels, or `- Non-controlling - interests -- -` never matches.
* **NEW SUSPECTS, flagged not touched — the TATACOFFEE class again, mirrored.** Two documents state
  an owners/NCI split whose TOTAL is what we store:
  `SADBHAV 2019-06` owners −8.07, NCI −21.98, period −30.05 — we store **−30.05**;
  `RENUKA 2019-09` owners 2,817.90, NCI −78.30, period 2,739.60 — we store **2,739.60**.
  If confirmed these are con slots holding TOTAL PAT on an owners-attributable series (§53c, repair
  via §2b). Do not bulk-correct off this: it is two documents, and §59's audit found the equal-runs
  screen overwhelmingly genuine.


---

## 67. ★★★ RE-ADJUDICATING THE CON-COPY HEALS — 4 of 18 were wrong, all the same way  (2026-08-09)

Every active con-copy heal was re-read from the **BSE announcement stream** (§58 rung 3) after
TATACOFFEE turned out to be a basis regression and it emerged that only ONE of the 18 had ever been
independently checkable. Verdicts per cell: `scripts/_con_copy_readjudication.json`.

**12 confirmed · 2 corrected · 2 escalated · 2 immaterial-gap.** With TATACOFFEE that is **4 wrong
of 18 (22%)**, and every failure is the same family — *the heal wrote a real number from the real
filing that is not the number our convention stores*:

| cell | wrote | should be | why |
|---|---|---|---|
| TATACOFFEE 2022-12 | 38.40 total | 26.63 owners | owners + NCI = total |
| ATUL 2023-06 | 102.05 total | **103.35 owners** | owners 103.35 + NCI −1.30 = 102.05 exactly |
| KIRLFER 2023-06 | 92.92 total | 74.01 owners | total 92.93 − minority 18.92 (escalated, see below) |
| ACUTAAS 2023-06 | 153.72 restated | **142.35 as-reported** | own filing + its XBRL both say 142.35 |

Root cause was one line of reasoning in `read_con_copies`: it anchored on **screener, which quotes
TOTAL PAT**, and then wrote whichever row matched. The column was almost always right; the ROW was
wrong. Fixed — screener now only locates the column, and the value is read off the OWNERS row.

### 67a. Two false-positive classes the tripwire cannot see by itself
* **Owners-vs-total** (§65f): a company whose owners-basis con PAT coincidentally equals its
  standalone satisfies "ours con == std" while screener's total differs. TATACOFFEE.
* **Legitimate con == std, later restated.** ACUTAAS had not yet consolidated Tanfac in Jun-2023, so
  con == std was CORRECT then; the Sep-2023 filing restated the comparative to 153.72. Adopting a
  restated comparative into an as-reported series also silently mixes bases (§40b). **Before
  believing a flag, ask whether the company consolidated anything that quarter.**

### 67b. A MIXED-CONVENTION series cannot be repaired two cells at a time
KIRLFER stores **owners** at 2023-03 (88.22) and 2022-06 (93.56) but a **total** at 2024-03 (92.92);
ATUL stores a total at 2023-03 (92.21, owners 93.56) and owners at 2022-06 (164.52). Correcting one
cell inside such a series makes it locally right and globally no more consistent. KIRLFER is
escalated untouched — its Jun-2024 statement has no minority line at all and restates Jun-2023 from
92.93 to 17.73. **Test the convention on the company's OWN neighbouring quarters before writing**;
that single check is what settled ATUL and stopped KIRLFER.

### 67c. Do not re-automate the column placement naively
The first pass at automatic adjudication assumed columns step one quarter per column. Statements
print **[current Q, preceding Q, YEAR-AGO Q, YTD, FY]**, so it mis-placed nearly everything and
"corrected" AADHARHFC to its own YTD columns (1221.97, 1895.02). Those verdicts were thrown away and
all 17 cells adjudicated by hand off an evidence dump. **The cumulative columns are the trap** — and
they are also the best confirmation available when read deliberately: AADHARHFC's three cells were
each locked by an exact identity (H1 1221.97 = 628.55 + 593.42; 9M 1895.02 = 673.05 + 628.55 +
593.42), which is far stronger evidence than any single-column match.


---

## 68. ★★★ REPAIRING A MIXED-CONVENTION SERIES — the KIRLFER rebuild  (2026-08-09)

KIRLFER's consolidated PAT was escalated out of the §67 re-adjudication because two cells could not
fix it. The stored series held **three different things at once**:

| | quarters |
|---|---|
| owners-attributable (correct) | Mar-22 6.27 · Jun-22 93.56 · Dec-22 116.61 · Mar-23 88.22 · Jun-24 · Dec-24 · Jun-25 |
| the TOTAL instead of owners | Jun-23 92.92 · Dec-23 105.33 |
| an exact COPY of standalone | Sep-22 82.00 · Sep-23 56.88 |
| another quarter's number entirely | **Mar-24 92.92** (= Jun-2023's total) · **Sep-24 84.91** (= Sep-2025's *standalone*) |

**8 of 15 quarters were wrong.** Pre-2022 needed nothing: con == std from 2015 to Dec-2021 is real,
KIRLFER had nothing to consolidate before ISMT.

### 68a. The method — one convention, from each quarter's OWN filing
These statements print no "attributable to owners" line, so `owners = profit for the period −
minority interest` (§53; associates print NA throughout). Comparatives from later filings are NOT
used — the ACUTAAS rule (§67a): as-reported, never restated.

### 68b. What makes a series rebuild verifiable in a way single cells are not
A cell read gives you one number and one anchor. A **whole year** gives you three simultaneous
identities, and they must all close:

```
FY23  period  102.08 + 110.99 + 129.70 +  94.56 = 437.33   (printed)
FY23  minority  8.52 +  14.28 +  13.09 +   6.34 =  42.23   (printed)
FY23  owners  437.33 - 42.23 = 395.10 = 93.56 + 96.71 + 116.61 + 88.22
FY24  period   92.93 +  81.67 + 105.33 +  17.73 = 297.66   (printed)
FY24  minority 18.92 +  13.42 +  29.00 +  -1.78 =  59.56   (printed)
FY24  owners  297.66 - 59.56 = 238.10 = 74.01 + 68.25 + 76.33 + 19.51
```
Every one closes to the paisa. **Reconciling the MINORITY row separately is the step that makes the
owners series trustworthy** — matching only the period row would have left the split unverified,
which is exactly how the total got stored as owners in the first place. A fourth, independent lock:
screener's consolidated series (which quotes TOTAL PAT) reproduces all nine overlapping extracted
totals — 93/82/105/18/70/78/54/92/86.

Sep-2023's own token is split by extraction, so its total came from the H1 identity
(174.60 − 92.93 = 81.67) and was confirmed again by the Dec-2024 filing's 9M FY24 column (279.93).

### 68c. Generalise this
* **When a series mixes conventions, rebuild the window; do not patch cells.** Two cells inside an
  inconsistent series are locally right and globally no better — and each patch makes the next
  audit harder to read.
* **Test the convention on the company's OWN neighbouring quarters before writing anything.** That
  single check is what identified which KIRLFER quarters were already correct, and it is cheap.
* **A stored value that equals another quarter's number, or the other basis' number, is not a
  rounding problem** — Sep-24 held Sep-2025's *standalone*. Scan a repaired series for values that
  appear elsewhere in the same company's data.
* The tripwire could only ever see the two exact-copy quarters (Sep-22, Sep-23). The six others were
  invisible to it. **A clean tripwire run says nothing about convention consistency.**

Ledger: `scripts/kirlfer_con_series.json` (per quarter: total, minority, owners, prior value, why).


---

## 69. ★★ THREE MORE OWNERS-vs-TOTAL SERIES — ATUL, SADBHAV, RENUKA  (2026-08-09)

Three cells were named as suspects (SADBHAV Jun-19, RENUKA Sep-19, ATUL Mar-23). **All three turned
out to be series that mix owners with TOTAL**, exactly like KIRLFER (§68), so each was repaired
across its window — 9 cells. Ledger: `scripts/owners_basis_heals.json`.

| | corrected |
|---|---|
| ATUL | Mar-22 136.58→**136.26** · Dec-22 136.87→**105.10** · Mar-23 92.21→**93.56** · Mar-24 74.90→**58.41** |
| SADBHAV | Jun-19 −30.05→**−8.07** · Sep-19 −39.89→**−15.94** |
| RENUKA | Sep-19 2739.6→**2817.9** · Mar-20 −145.2→**−146.0** · Jun-20 −35.3→**−34.9** |

Each is `period − non-controlling interest` at the target column of the quarter's OWN filing, and
each window closes on a printed OWNERS subtotal:
```
ATUL FY23  164.52 + 150.91 + 105.10 + 93.56 = 514.09   (printed) EXACT
ATUL FY24  103.35 +  90.32 +  70.94 + 58.41 = 323.02   (printed) EXACT
SADBHAV H1FY20      -15.94 +  -8.07         = -24.01   (printed) EXACT
RENUKA FY20 -364.2 + 2817.9 + -208.6 + -146.0 = 2099.1 vs printed 2099.2
```

### 69a. Two cells held numbers belonging to NO basis
ATUL Dec-22 stored **136.87** and Mar-24 stored **74.90** — neither the period nor the owners figure
for those quarters. This is the same shape as KIRLFER's Sep-24 (which held Sep-2025's *standalone*).
**A wrong con cell is not always the other basis; sometimes it is a stray number.** Do not assume
the defect is total-vs-owners just because that is the common case — read the page.

### 69b. ★ sf_fundamentals and sf_revop CAN DISAGREE — check both
The applier's guard caught it: ATUL's `sf_revop` patC held **−105.24** at Mar-23 (an
equity-attribution figure off the filing's page 10) while `sf_fundamentals` held 92.21. **They
disagree at six ATUL quarters** — and at Mar-24 `sf_revop` already had the CORRECT 58.41 while
fundamentals had 74.90, so neither file is reliably the good one. SADBHAV and RENUKA each diverge
at one quarter too (Dec-2020).
**Verify a con-PAT cell in BOTH files, and write both.** A fix applied to one is half a fix, and a
comparison of the two is a free defect detector nobody was running.

### 69c. A huge value is not automatically a scale error
RENUKA Sep-2019's 2,739.6 cr sits wildly outside a series that otherwise runs −400 to +70, which
looks exactly like a 10^k slip. It is real: the quarter carries a very large one-off, the same row
prints H1 FY20 2327.9 = 2739.6 + (−411.7), and the FY20 owners subtotal closes on it. **Check the
page's own arithmetic before reaching for `scale_fix`.**

**Not changed, still suspect:** ATUL 2021-06-30 (stored 165.15; the FY22 owners subtotal 604.26
implies 165.94) and SADBHAV 2020-03-31 (stored 886.63 against a standalone of 8.18 — wildly out of
family, and no P&L page was located in that filing). Also open: the four remaining ATUL and one each
SADBHAV/RENUKA fund-vs-revop divergences listed above.

### 69d. Closing out ATUL — and a flagged cell that turned out to be RIGHT
Reading ATUL Jun-2021 to settle the one cell §69 had flagged showed the same defect running back
through FY21 and FY22, so both years were closed out (5 more cells: Jun-20, Mar-21, Jun-21, Sep-21,
Dec-21). ATUL's consolidated PAT is now one convention across all 16 quarters Jun-2020 → Mar-2024,
in **both** files, with each year closing exactly on its printed owners subtotal
(FY21 655.76, FY22 604.26, FY23 514.09, FY24 323.02). Sep-2020 and Dec-2020 were already owners and
were left alone; Dec-2020 is confirmed by subtraction against the FY21 subtotal.

Sep-2021 was another stray: stored 148.82 matches **neither** the period (146.12) nor the owners
(146.63) figure — the §69a shape again.

**SADBHAV 2020-03-31 was re-checked and is CORRECT as stored.** It was flagged as suspect because
886.63 sits absurdly against a standalone of 8.18 and a series otherwise in the tens of crore. The
Jun-2020 filing settles it: Mar-20 period 1280.78, NCI 394.14 → owners 886.64, and FY20 owners
`−8.07 + −15.94 + −69.79 + 886.63 = 792.83`, exactly the printed figure. It is a genuine very large
Q4 one-off. **Second time in one session that an out-of-family value was real (RENUKA §69c) — the
FY identity is what settles it, never the magnitude.** Being wrong about this one cost nothing
because it was flagged rather than "fixed"; had it been corrected on plausibility it would have
destroyed a correct value, which is exactly how TATACOFFEE broke.


---

## 70. ★★★ sf_fundamentals vs sf_revop DISAGREE ON 1,372 CELLS — and one page believed the wrong one  (2026-08-09)

§69b noticed the two files disagreeing on a handful of ATUL quarters. Measured properly across the
dataset: **sf_fundamentals `npCon` and sf_revop `patC` differ on 1,372 of 43,731 populated cells
(3.14%)**, spread evenly over 2018-2026 (~3% every year). Nobody was running this comparison.

| family | n | |
|---|---|---|
| revop is exactly 0.0, fundamentals has a real value | 603 | the XBRL owners=0 mis-tag |
| genuinely different numbers | 716 | needs per-cell adjudication |
| fundamentals is 0.0, revop has a value | 15 | fundamentals is the broken side |
| sign flips / power-of-ten | 38 | |

### 70a. Which file is authoritative — and who ignored that
`stock.html`: *"Net profit comes from sf_fundamentals (point-in-time, OWNERS-attributable — never
swap in sf_revop's PAT mirror slots)"*, and `build_quarterly_results` says the same. So
**sf_fundamentals is authoritative and revop's idx4/idx5 are a mirror.**

But `build_discovery.ttm_pat` read `pick(cell, 5, 4)` — exactly those mirror slots — so the
**Discovery / Order-Wins TTM P/E was computed off a copy that differs from the PAT the site
displays**: 298 divergent cells sit in the 2025-26 window it uses, across 203 symbols. Fixed: it now
prefers `sf_fundamentals`, falling back to the mirror only where the authoritative file is empty.
**When two files hold the same quantity, grep for every consumer — one of them will be reading the
wrong one, and nothing will error.**

### 70b. What was and was NOT resynced
The 603 `revop == 0.0` cells were resynced from sf_fundamentals: a printed 0.00 PAT beside a real
value on the other basis is implausible, and `apply_owners_full` already refuses to write a ~0 over
a nonzero stored con for exactly this reason. Divergences fell **1,372 → 766**.
The 716 genuine disagreements and the 15 `fundamentals == 0.0` cells are **left alone** — picking a
winner without reading the filing is what created the defects this session has spent its time
undoing. They are the next audit's work, and they now have a measurement to start from.

### 70c. The four named cells, and what they showed
```
ATUL    2025-09-30  182.37 -> 179.24   period - NCI 3.13; H1 FY26 owners 307.01 EXACT
SADBHAV 2020-12-31   -41.36 -> -24.32  BOTH files wrong: fundamentals had the TOTAL,
                                       revop had +24.32 -- right magnitude, FLIPPED SIGN
RENUKA  2020-12-31  -141.1 -> -141.2   period - NCI 0.1
RENUKA  2021-03-31   -44.9 ->  -44.0   FY21 owners -34.9+105.4+-141.2+-44.0 = -114.7 EXACT
```
SADBHAV Dec-2020 is the one to remember: **neither file held the right number.** "The two files
disagree" does not mean one of them is right.


---

## 71. ★★★ THE ADJUDICATION THAT WAS ABANDONED — when your "truth" source is the corrupted one  (2026-08-09)

Closing the remaining **766** fund-vs-revop con-PAT divergences (§70) started as a per-cell
adjudication against the filers' own XBRL: index all 104,331 cached filings, take
`owners`-attributable con PAT as truth, and correct whichever file disagrees. It ran, and it
returned a clean-looking verdict:

> A filing == fundamentals → fix revop: **18**
> B filing == revop → fix fundamentals: **718**
> C filing == neither: 1

**That verdict was wrong and applying it would have destroyed 693 correct values.** The check that
caught it was asking why the answer inverted everything established that day:

| | sf_fundamentals | XBRL `owners` tag | reality |
|---|---|---|---|
| MARUTI 2022-09-30 | **2112.50** | 212.50 | Q2 FY23 con PAT is ₹2,112 cr |
| LUPIN 2021-09-30 | **−2094.87** | −209.84 | Q2 FY22 loss is ₹2,095 cr |
| KAYNES 2023-03-31 | **63.51** | 5,814,249.6 | ₹63 cr |
| SELMCL 2018-03-31 | **−1541.55** | +1541.34 | sign flipped in the tag |

**The owners tag is the corrupted thing in this population** — ×0.1, sign-flipped, or unscaled raw
rupees. `build_fundamentals.xbrl_profit`, the EPS-guarded parser, returns the same bad tag, which
proves sf_fundamentals' correct values never came from these cached filings at all. sf_revop's patC
is built from the unguarded tag, which is *why* it is the file that diverges.

**The lesson, and it is not a small one: a source being primary does not make it correct.** The
filer's own XBRL is as primary as it gets and it was wrong 693 times. Sanity-check a mass verdict
against a handful of facts you can confirm independently — Maruti's quarterly profit is public
knowledge — before letting it write. A verdict that overturns everything you established that day is
far more likely to be a bug in your adjudicator than a discovery.

### 71a. What was actually done instead
sf_fundamentals is authoritative (`stock.html`, `build_quarterly_results`), it is what the site
displays, and after §70 it is what Discovery reads. So the mirror was resynced to it — **writing
only sf_revop, never the authoritative file** (verified byte-identical afterwards). No new error can
be introduced this way: worst case the mirror carries an error the site was already showing.
**Divergences 1,372 → 766 → 23.**

### 71b. The 23 held back — `scripts/_fund_suspect_cells.json`
Cells where **sf_fundamentals itself** is out of family against that company's own median |npCon|
(>8× or <0.125×) while the mirror is in family. Resyncing from a value that looks wrong would
launder it into a second file. Worst offenders: ZEAL 2026-03 (4114.27 against a 2.35 median),
RELCAPITAL 2023-03 (2436.50 vs 276), NUCLEUS 2025-12 (250.2 vs 26.8), IFCI 2024-12, IRB 2020-09.
Each needs a filing read. **This is now the whole remaining divergence population.**

---

### 71f. ★ A MISSING SCRIPCODE CAN MEAN "NOT ON BSE" — do not fill the map
SUBCAPCITY's two cells abstained with "no scripcode in `scrip_map` → the BSE stream is
unreachable", and §71c called that structural and said to fix the map first. **That was the wrong
instruction: there is nothing to fix.**

SUBCAPCITY is **International Constructions Ltd** (formerly Subhash Capital City), ISIN
**INE845C01016**, and it has **no BSE listing at all**:
* the ISIN is absent from `bse_scrips.json` `by_isin` (4,875 ISINs) — necessary but not sufficient,
  since §52b notes BSE-delisted names also drop out of the master;
* so BSE was asked directly — `api.bseindia.com/.../PeerSmartSearch` returns **"No Match Found"**
  for the company name, the ISIN, *and* the former name;
* its cached XBRL carries `identifier scheme="http://www.nseindia.com/NSESymbol"`;
* and the NSE results archive serves **76 rows (16 consolidated)** for it — the data is reachable,
  just not where the reader was looking.

`scrip_map()` returning None here is CORRECT. Inventing or guessing a code would send every future
fetch to the wrong company. Recorded in **`scripts/_nse_only_no_bse.json`** so the next run reads
the fact instead of re-deriving it. **Before treating a missing scripcode as a gap, ask BSE whether
the company exists there.**

The two cells remain open for a different reason: the only source NSE offers is the filer's own
XBRL, and its attribution tags contradict each other — `NCI = 0.0` while `owners != total`, and at
2022-09-30 `owners + NCI = -total` (sign flipped). `resultDetailedDataLink` is empty post-2019
(§60), so there is no printed P&L to arbitrate. Our stored series follows the OWNERS tag in 4 of 6
comparable quarters, so the mirror's -0.79 / 2.67 is the self-consistent choice — but that is a
convention, not a proof, and it was not written on that basis.


### 71g. ★★★ CLOSED AT ZERO — and the out-of-family screen was a false alarm on 17 of 23
The 23 cells §71b held back are resolved: **6 corrected from documents (§71c/e), 17 shown to be
false alarms.** Divergences **1,372 → 766 → 23 → 0**.

Working the last 16 one at a time showed they were never 16 problems, they were one:

    sf_fundamentals holds the XBRL's TOTAL      (ProfitLossForPeriod)
    sf_revop        holds the XBRL's OWNERS tag (ProfitOrLossAttributableToOwnersOfParent)

and in these filings the owners tag is incoherent — `owners + NCI == total` fails in **14 of 16**,
sometimes absurdly: NAZARA owners 8.35 + NCI 8.70 against a total of 0.18; IFCI owners 741.53 + NCI
719.26 against −8.74. Where the NCI tag reads 0.0, total *is* owners anyway.

**The check that settled it: screener, which quotes TOTAL PAT, agrees with sf_fundamentals in 9
cells and with the mirror in ZERO.** Not one cell supported the mirror. So the file the screen had
flagged as suspect was the correct one throughout, and the mirror was the corrupted copy — the same
shape as §71, arrived at from the opposite direction.

Resynced mirror ← authoritative, **writing only sf_revop and asserting sf_fundamentals
byte-identical afterwards** (the script fails if it is not). No new error is possible that way.

**"No longer divergent" is not "verified".** Seven cells had no independent source reach them
(AXISCADES, BANCOINDIA, DELTAMAGNT, IRB, 3IINFOLTD, ARIHANTCAP, SUBCAPCITY 2021-03); they are
resynced on the structural argument and listed in **`scripts/_fund_unconfirmed_cells.json`** so the
distinction survives. A future audit should read those filings.

**The lesson about the screen itself:** "value is out of family against the company's own median"
found 23 candidates and was right about 6. At a ~26% hit rate it is a fine *search* heuristic and a
terrible *verdict* — every one of the 23 still needed an outside source before anything was written.


### 71h. ★★★ THE BLANKET RESYNC WROTE ONE WRONG VALUE — and the caveat is what caught it
§71g resynced 16 cells mirror←authoritative on a structural argument, recorded 7 of them as
"resynced but not verified" in `_fund_unconfirmed_cells.json`, and said plainly that *no longer
divergent is not verified*. Going to the documents proved that caveat earned its keep:

**3IINFOLTD 2025-12-31 was a REGRESSION I introduced.** Its own Dec-2025 filing (p7, lakhs) prints,
at the Dec-25 column, period **208** and non-controlling **−5** — owners **213**. The XBRL agrees and
its identity *closes*: `owners 2.14 + NCI −0.05 = total 2.09`. We store OWNERS, so **2.14** is the
value; the resync wrote the TOTAL 2.09, because "sf_fundamentals is authoritative" is a statement
about which FILE to trust, not about which BASIS the file happens to hold. Corrected to 2.14.

**DELTAMAGNT 2022-03-31 confirmed at −0.12** — row XI "Profit/(loss) for the period/year" is
−12.21 lakh in the own filing, and the Q+1 filing prints −0.12 in its Mar-22 column. NCI nil, so
owners == total.

**The rule this yields:** a resync is safe for *consistency* and unsafe as a *verdict on basis*.
Before resyncing a cell, check whether the filing carries an owners/NCI split — where it does and
the identity closes, the mirror may be holding the correct owners figure and the authoritative file
the total. §71g's own screen (screener == fundamentals in 9, mirror in 0) could not see this,
because screener quotes TOTAL and therefore agrees with the total by construction.

Five remain genuinely unreadable and are still listed, each with its reason: AXISCADES 2018-12
(OCR absorbs values into row captions — "(2,290.o4)" and "(1.263.51)" appear inside labels, so the
columns are incomplete and owners 1.59 + NCI 0.11 will not reconcile to the −0.61 after-tax line);
ARIHANTCAP 2026-03 and IRB 2020-09 (no header row places the target quarter); BANCOINDIA 2019-03
(no consolidated page with a profit block in any fetched PDF); SUBCAPCITY 2021-03 (§71f).


### 71i. ★★ NEIGHBOUR-VALUE ANCHORING BEATS THE HEADER PARSER — 2 more of the last 5
Three of the five leftovers had abstained with "no header row places the target quarter". That is a
statement about `date_columns.quarter_columns`, not about the page. **§58's column anchor needs no
header at all**: find the column whose value reproduces a con PAT we ALREADY STORE for a DIFFERENT
quarter. Applying it settled two immediately.

**IRB 2020-09-30 — CONFIRMED at −19.66.** Sep-2021 filing p20 (lakhs), "Profit after tax":
`Sep-21 42.31 | Jun-21 71.91 | SEP-20 −19.66 | H1FY22 114.21 | H1FY21 −49.80 | FY21 117.15`, with
x240 == stored 42.31 and x299 == stored 71.91. Two identities close, and the second is the strong
one: `H1FY21 = −19.66 + stored Jun-20 −30.14 = −49.80`, **exactly as printed**. The mirror's −66.60
was wrong; sf_fundamentals was right.

**ARIHANTCAP 2026-03-31 — 1.28 → 0.50.** Own Mar-2026 filing p5 (lakhs),
"Profit/(Loss) for the Year": `MAR-26 0.50 | Dec-25 5.18 | Mar-25 7.70 | FY26 31.46 | FY25 58.70`,
anchored on stored Dec-25 5.18 and Mar-25 7.70. FY26 closes exactly:
`12.70 + 13.08 + 5.18 + 0.50 = 31.46`. **Neither stored value was right** — fundamentals had 1.28,
the mirror 10.96.

**Reach for the neighbour anchor before declaring a column unplaceable.** The header parser is one
route to a column and the weaker one; a value we already hold is self-validating.

⚠️ **Tolerance floors manufacture false anchors on small numbers.** AXISCADES' only "anchor" was a
cash-flow *Adjustments* row matching 0.12/0.11 against a stored 0.08 — inside the `max(0.05, …)`
floor. On values of a few lakh that floor is most of the signal; require a relative match, or two
anchors on *different rows*, before believing it.

3 remain in `_fund_unconfirmed_cells.json`, none of them a regex gap: AXISCADES 2018-12 (OCR absorbs
figures into row captions), BANCOINDIA 2019-03 (nothing anchors on two stored neighbours),
SUBCAPCITY 2021-03 (no BSE listing, §71f).


### 71j. ★★★ TWO ATTRIBUTION BLOCKS ON ONE PAGE — the mirror was holding TCI, not PAT
AXISCADES 2018-12 kept failing `owners + NCI == total` (1.59 + 0.11 against −0.61). Dumping the
WHOLE profit block instead of only the matched rows showed why: the page carries **two attribution
blocks**, and the reader was pairing rows across them.

```
IX. PROFIT/(LOSS) AFTER TAX        Dec-18  -0.61     <- total PAT
    Owners of the Company                  -0.73     <- PROFIT attribution
    Non controlling interest                0.11
X.  Other Comprehensive Income     Dec-18   2.31
    (total comprehensive income)            1.70     = -0.61 + 2.31
    Owners of the Company                   1.59     <- TCI attribution
    Non controlling interest                0.11
```

**sf_revop's 1.59 was total-comprehensive-income attributable to owners — not PAT at all.** The
owners PAT is **−0.73**: `-0.61 − 0.11 = −0.72` against a printed −0.73 (lakh→crore rounding), and
the FY19 column closes exactly, `−7.67 − 0.46 = −8.13` as printed. Corrected.

**Take the FIRST owners/NCI pair after the post-tax line, and verify it against that line.** A
statement prints owners twice — once for profit, once for total comprehensive income — and they are
several rupees apart. An identity that "fails" may just mean the two rows came from different
blocks. When a reader reports a failing identity, dump the whole block before believing the failure.

**BANCOINDIA 2019-03 remains unreadable**, and now with a specific reason rather than "no profit
row": all four windows were swept and **no consolidated P&L table exists as a text layer in any
fetched PDF** — the own filing's consolidated pages are a BALANCE SHEET and two auditors' reports,
Q+1 and Q+4 carry only auditor narrative. Its XBRL gives total 4.87 and owners 14.00 but **no NCI
tag**, so the implied −9.13 split is arithmetically possible and wholly unconfirmed; screener has no
coverage. It needs a different attachment or a vision read.


### 71k. ★★★ `page_basis()` HIDES SIDE-BY-SIDE STATEMENTS — the commonest reason a "profit row" is missing
BANCOINDIA 2018-19 was the last cell queued for a vision read. **It needed no vision, and the
consolidated P&L was in the filing the whole time.**

`date_columns.page_basis()` returns `None` when a page mentions BOTH "consolidated" and
"standalone" — it treats that as a cover/notes page. But a very common Indian layout is a **single
statement printing STANDALONE and CONSOLIDATED side by side**, five columns each. Such a page names
both bases, so it classifies `'-'`, and **every reader that filters `page_basis(pg) == "con"` skips
the only page that has the numbers.** That is what produced "no consolidated page with a profit
block" here — and, on the evidence of §71c, likely several of the six "no profit-for-period row"
abstentions too.

BANCOINDIA's page 1, `Rs in Lakhs`, row 14 "Profit/(Loss) for the period (10+13)":
```
consolidated   Mar-19 487 | Dec-18 (370) | Mar-18 2,698 | FY19 6,908 | FY18 11,677
standalone     Mar-19 3,662 …                                  (3,662 == our stored npStd 36.62)
```
Stored Dec-18 **−3.70** and Mar-18 **26.98** reproduce that row exactly, so the column and the basis
are both pinned. **4.87 is correct; no change.** The mirror's 14.00 is row 16, *Total Comprehensive
income attributable to owners* = 487 + OCI 913 = 1,400 — the §71j TCI-not-PAT trap again.

**Fix to make: `page_basis` needs a third answer.** "Both words present" should mean *both bases are
on this page*, not *skip me*. Until it does, any reader that abstains with "no consolidated page"
must retry with the basis filter OFF and pick the block by column geometry — the consolidated block
is identifiable by magnitude against the standalone one.

**Vision has still never been needed.** Twice now a cell was escalated to the vision rung and twice
the answer was in a text layer the *filters* were hiding (§71e was a failed fetch plus an OCR layer;
this one a basis misclassification). Exhaust the filters before the rung.


### 71l. ✔ `page_basis()` FIXED — 'both' is now an answer, and it immediately unlocked a read
The §71k fix is in. `date_columns.page_basis()` returns **`'con' / 'std' / 'both' / None`**, and a
new **`page_shows(page, want)`** returns True for the page's own basis *and* for `'both'`. Filter
pages with `page_shows`, never with `page_basis(pg) == want`.

**A trap caught in unit test before it shipped:** naively returning `'both'` when both words appear
made every **"Non-Consolidated"** page — NSE's own title for a standalone filing — classify as
`'both'`, offering pure standalone pages to consolidated readers. The word "consolidated" lives
*inside* the negation. `page_basis` now strips `Non-/Un-Consolidated` before testing for a bare
"consolidated", which also upgrades those pages from `None` to a correct `'std'`.

**Measured over 514 real filing pages:**

| | |
|---|---|
| unchanged | 462 |
| `None → 'both'` (newly visible) | 52 |
| …of which carry a profit row | **8** |
| **regressions** (was con/std, now different) | **0** |

`read_con_copies` re-run on the same 9-cell set returns SKFINDIA 558.68 byte-identical **and now
also reads MODIRUBBER 2025-09-30**, which three previous passes had written off as
"OUT-OF-RESOLUTION". One filter fix, one recovered cell.

**Only `read_con_copies` called this helper** (the one other hit was a dict key), so the blast radius
was small — but every ad-hoc reader written during this campaign carried the same
`page_basis(pg) == "con"` line, which is why "no consolidated page" recurred so often. Use
`page_shows`. On a `'both'` page the two bases sit in separate column blocks, so the caller **must**
anchor its column on a value it already stores (§58); taking the first match can read the standalone
block while believing it read the consolidated one.


## 72. ★★★ VERIFYING REV/PAT vs EXTERNAL SITES — the sites can only reach 10 of our 95 quarters  (campaign 2026-08-09)

Full write-up `scripts/REVPAT_VERIFY_REPORT.md`; plan `REVPAT_VERIFY_CAMPAIGN.md`; phase findings and
tooling in `scripts/revpat_verify/` (`audit_revpat_coverage`, `revpat_strata`, `revpat_mapcard`,
`revpat_quorum`, `build_contested`, `exchange_fetch` — all re-runnable, all offline except the fetcher).

- **★ THE CAMPAIGN PLAN'S OWN SLOT NOTE WAS WRONG.** `sf_revop` is
  `[revS, revC, opS, opC, patS, patC, finFlag, ebitS, ebitC]` — **PAT at 4/5, EBIT at 7/8**, no
  other-income pair. The plan said PAT sat at 7/8; following it would have compared **EBIT** against
  every site's net profit and faked a mismatch on nearly every cell. **And `sf_revop`'s PAT is a
  MIRROR** — authority is `sf_fundamentals` (§70). Never diff the mirror against a site: ~753
  phantom defects.
- **★★ THE SITES CANNOT REACH THE DATA.** Measured quarterly depth: Screener **13 qtrs (Jun-2023)**,
  Tickertape 10 (Mar-2024, consolidated-only), Groww 5 (Jun-2025), StockEdge **none** (annual-only,
  standalone, 5 rows), Trendlyne 403-blocked. We hold **95 quarters**. So **rule 6b (filing AND ≥2
  independent sites) is satisfiable on ~10 of 95** and **pre-2023 is exchange-only** — the same
  conclusion §22h reached for SHP, from different sites and different fields. Report those cells as
  **unverifiable-by-site (measured)**, never as "unchecked".
- **★★ CONSOLIDATED PAT HAS NO SITE QUORUM AND CANNOT GET ONE.** Screener's and Groww's consolidated
  "Net Profit" sits **+1.5-1.7%** above our owners-attributable PAT with hold at **37.6% / 42.9%**,
  and the bias **vanishes on standalone** (94-100% hold) where there is no NCI. They publish TOTAL;
  we publish OWNERS. Neither may vote on con PAT. Tickertape is the only con-PAT voter, and one site
  is not a quorum. **No site publishes an owners-vs-total split for a quarter** — T-B is
  arbitration-only.
- **★★ MAP PER (BASIS, COMPANY CLASS), NEVER GLOBALLY.** Tickertape's revenue field scored **21.4%**
  unsegmented and was refused; split by our own `fin` flag it resolves. Our bank revenue is
  **Interest Earned**, a site's is **Total Income**: SBIN runs **29-42% higher every quarter**
  (Mar-2024 164,914 vs our 117,469) while RELIANCE agrees to 0.1%. One global card = every bank
  flagged as a defect. Same failure class as §22h's wrong era-split, different axis.
- **★★★ THE SBIN CASE — why site majority never decides.** Tickertape disagreed with our SBIN
  consolidated PAT on **10 of 10 quarters**, always the same direction, with a plausible
  total-vs-owners story. SBI's own XBRL: `ProfitLossAfterTaxesMinorityInterestAndShareOfProfitLoss
  OfAssociates` Dec-2024 = **18,853.16 = ours, exact to the paisa**; and our Jun+Sep+Dec sum
  **57,960.88 == the filing's own 9M YTD context, delta 0.00**, while the site's three miss it by
  ₹1,067 cr. Ours-minus-MI (18,222.54) is not the site's number either — it is simply wrong.
  **OURS_CONFIRMED.** A confident, consistent, mechanistically plausible site disagreement was still
  the site's error.
- **★ NSE's `FourD` CONTEXT CAN DECLARE 3-MONTH DATES WHILE HOLDING 9-MONTH YTD VALUES** (measured
  4/4 on sampled Q3 filings, ratio ~2.9-3.0x). No period-span check on the context's own declared
  dates catches it. **Never use `FourD` as a value source** — NSE files one XBRL per basis, so
  `OneD` is correct once the right file is fetched. This produced 5 wrong values in a first draft
  before a cross-check caught it. (Read positively, it is also the 9M anchor that settled SBIN.)
- **★ "THE NAME LIES", three more instances:** StockEdge's `Consolidated_NetProfit` is **not**
  consolidated (standalone `PAT + extra_items`, RELIANCE FY2023 43,002+1,188=44,190 exact);
  Tickertape sid `TRU` is **Trust Fintech** (ours is sid `DHA`); StockEdge's ticker search is
  name-fuzzy (`IEL` → *Gabriel India* first, real IEL is #6 of 26). Plus Groww's `financialSummary`
  narrative is ~2 years stale and calls a six-month TCS period "the quarter".
- **★ ROBOTS: `api.tickertape.in` and `quotes-api.tickertape.in` are `Disallow: /`.** Use the allowed
  `www.tickertape.in` SSR route (`__NEXT_DATA__` → `props.pageProps['income-normal-interim']`), which
  serves identical data. **§22h may need correcting** — the SHP campaign appears to have used the
  disallowed host. Trendlyne 403'd on request #1 here while the SHP recon logged 10/10 successes the
  same day: **the block is session/IP-dependent; re-probe fresh, never inherit "the site is open".**
- **Coverage vs point-in-time N500** (95 qtrs, 47,436 member-quarters): revS **93.3%**, revC 39.5%,
  patS **93.7%**, patC 42.5%. Mar-2020→date is 96.8-99.2% on all four; the con columns before that
  are the §51a/§53 structural walls, not neglect.
- **★ SEVERITY INVERSION: patS outranks patC for backtest impact.** Backtest-effective PAT (the
  engine's `tries=[[3,4],[1,2]]` con→std fallback) comes to **44,468 cells against patS's 44,468 —
  the two coverages agree to ~0.05%** (a residual ~22 member-quarters hold con PAT but no std;
  store-wide, outside the N500 denominator, that residue is 1,493 cells, so do NOT restate this as
  a strict subset). The fallback absorbs essentially the whole con gap, so a con-PAT hole is nearly
  invisible to the backtest while a std defect reaches ~all member-quarters.
- **★★★ TWO REAL DEFECTS FOUND — GICRE std PAT Jun-2025 and Sep-2025, root cause PROVEN.** Our
  GICRE **standalone** slot is populated from the **consolidated** statement's *pre-associate* PAT
  row. Same filing, consolidated page row 27 = 2,17,277 lakh = **2,172.77 cr = exactly our
  mis-stored "standalone" value**; + Share of Profit in Associates 35,782 = 2,53,059 lakh =
  2,530.59 cr = our stored (correct) con PAT. The standalone page's own PAT row reads **1,752.23**
  (Jun-2025) and **2,866.79** (Sep-2025).
  Anchored per §58: at Jun-2025 the neighbouring columns of the same row reproduce OUR OWN stored
  Mar-2025 (2,182.89) and Jun-2024 (1,036.36) exactly; at Sep-2025 the filing's own H1 column gives
  **Q1+Q2 = 1,75,223 + 2,86,679 = 4,61,902 lakh EXACT**. Rule 6b satisfied four ways (filing +
  Screener + Groww + our own sf_revop mirror, which already held 1752.23). Extends **§55c**, which
  had asserted the Jun/Sep-2025 shape without showing a read. **NOT HEALED** — the write path was
  blocked by a concurrent writer; evidence in `scripts/revpat_verify/arbitration_verdicts.json`.
- **★★ AND THE MIRROR IMAGE: BAJFINANCE Dec-2025 revenue, both bases — OURS_CONFIRMED, both sites
  wrong.** Filing gives std **18,067.89** and con **21,213.89**, our stored values exactly, closed by
  the 9-month quarter-sum identity on both bases; Screener and Groww are ~1.1% off, in the same
  direction, **agreeing with each other**. Together with SBIN this is the second demonstration in one
  campaign that **two sites agreeing is not evidence** — always arbitrate at the document.
- **★★ P3 (66 frozen symbols, 3,104 cells): 852 CONFIRMED, 18 CONTRADICTED (0.58%), and
  CONSOLIDATED PAT SCORED 0 CONFIRMED OF 394.** The P2 prediction — Screener and Groww publish TOTAL
  con profit against our OWNERS-attributable series, so both are barred and Tickertape is left alone
  — holds exactly at 5x the sample. **patE (the backtest-effective series) inherits it: 3 confirmed
  of 397.** The engine's std fallback is essentially never exercised in the site-reachable era, so
  the number strategy picks consume is the one figure sites cannot corroborate.
- **★★★ ARBITRATION SCOREBOARD, 22 cells taken to the filings: 5 defects, 17 OURS_CONFIRMED.**
  All 5 defects are GICRE (3 std PAT + 2 con revenue). **Every other contested cell confirmed us,
  including 7 of 7 where TWO sites agreed against us** (NIVABUPA x4, STARHEALTH x2, JUBLPHARMA x1)
  and BAJFINANCE x2. Sites agreeing with each other is not evidence; only the document decides.
- **★★ INSURERS ARE WHERE THE SITES ARE LEAST TRUSTWORTHY.** On NIVABUPA/STARHEALTH PAT the three
  sites disagree with EACH OTHER (Jun-2025: 71.00 / 71.44 / 39.00), so they decide nothing. On the
  revenue cells Screener and Groww agree with each other but their figure **cannot be reproduced
  from any printed row or combination** (Gross Premium, Net Premium Written, Total Income and sums
  all tried) — an unidentified aggregator concept, not a reading of the filing. Meanwhile §55's
  construction (Premium Earned + policyholders' + shareholders' investment income) reproduced SIX
  independent stored comparator quarters to the paisa.
- **★ A FINGERPRINT IS A HYPOTHESIS TO TEST PER COMPANY, NOT A CATEGORY PATTERN.** GICRE's
  con-pre-associate defect was tested on the other insurers and is **mechanically impossible** there:
  NIVABUPA and STARHEALTH have no subsidiaries and file no consolidated statement at all (zero
  `consolidat*` hits across 10/24/13-page PDFs). Absence was proven, not assumed.
- **★ READ FROM A RENDERED PAGE IMAGE WHEN THE TEXT LAYER IS SUSPECT.** Three separate packs in this
  campaign defeated `get_text()`: a GICRE PDF misread a digit (8-for-9), NIVABUPA Jun-2025 has
  `rotation=270` scrambling word order, and STARHEALTH's layer is OCR-corrupted (§3). Every figure
  in these arbitrations was read off images.
- **★ THE TRU IDENTITY TRAP RECURRED TWICE MORE** on Tickertape: `ccl-international-CCL` is not our
  CCL (CCL Products India = sid `CCLP`), `shri-kalyan-holdings-SHK` is not our SHK (S H Kelkar =
  sid `SHKE`). Three instances on one site. **A sid matching the ticker is a coincidence to be
  disproved, never an identification** — the ticker-echo + ISIN gate caught both before any row was
  emitted.
- **★ SCREENER'S WINDOW IS "the last 13 quarters THAT COMPANY filed", not a fixed date.** The earlier
  "13 quarters, oldest Jun-2023" note came from probing actively-filing large caps only; across 66
  symbols the span is 2018-06-30 -> 2026-06-30 (CASTEXTECH stale at 2018-2021, ZEAL semi-annual).
- **★ AGENT ORCHESTRATION: FORBID BACKGROUND JOBS, do not merely prefer foreground ones.** All three
  P3 extraction agents stalled by starting a background fetch and waiting for a notification that
  never comes; one lost its partial work. Phrasing it as a preference (as the plan did, from the SHP
  run) was not enough. Diagnose an agent's state before resuming — checking caches and logs first
  saved ~130 re-sent requests here.
- **★★★ THE WRONG-ROW DEFECT, and the test that separates it from a real disagreement** (user-raised
  2026-08-09, and it changed the method). If our stored value and an arbitration read of the filing
  agree while two independent sites agree on something else, **our parser and the arbitration may
  have made the SAME mistake — picking the same wrong row.** Then "ours == filed" is circular.
  CONFIRMED INSTANCES: HUDCO 2022-06-30 stored the filing's **"Interest Income" sub-line** (1736.42)
  instead of its Total revenue row (1749.27); AADHARHFC's std revenue runs 1.6-12.1% below an
  independent source for **seven consecutive quarters** while PAT matches to 0.1%.
  **THE DISCRIMINATOR — compare across the company's WHOLE history, not the disputed cells.** A
  wrong-row parse is a RULE and biases every quarter; an isolated disagreement is not. Our insurer
  revenue construction (§55) matches Screener on **9/11 NIVABUPA and 11/12 STARHEALTH quarters,
  exact to the crore**, and disagrees only on the 3 arbitrated cells — the opposite signature to
  HUDCO/AADHARHFC, so the construction is sound. Confidence there was still downgraded high→medium
  because the disagreement stays UNEXPLAINED after five hypotheses were tested and discarded
  (gross-vs-net premium, Total Income, Net Premium Written, exhaustive row-combination search,
  Integrated-Filing XBRL). **An unexplained disagreement is not a resolved one.**
  Detector: `scripts/revpat_verify/sweep_analyze.py` — revenue BELOW an independent source over
  CONSECUTIVE quarters **while PAT AGREES** (the third condition is what rules out wrong
  company/period/scale). Validated on 66 symbols: median relative difference across 823 cells is
  **exactly 0.0** — no systematic bias — with only AADHARHFC and MOTHERSON carrying the signature.
- **★★★ HEALED AND LIVE-VERIFIED (646a31be) — 9 cells, and HOW to apply this class safely.**
  `GICRE npStd 20241231→1621.35 / 20250630→1752.23 / 20250930→2866.79`, `GICRE revC 20240630→12886.47`,
  `AADHARHFC revS 20230630→578.01 / 20231231→658.54`, `HDBFS revS 20240630→3883.8 / 20241231→4143.6 /
  20250331→4266.1`. Confirmed on the LIVE `/fin/<SYM>.json` slices, and the clincher: **GICRE H1-FY26
  now sums to 4619.02, exactly the figure the filing prints** (before the heal we were off by 251.76).
  Tool: `scripts/revpat_verify/apply_staged_heals.py` (dry-run default). Four properties made it safe:
  **(1) guard-edit per §2b** — abort unless the current value equals the recorded guard, never force;
  **(2) BLAST-RADIUS PROOF per file** — patch in memory, diff against the original, and refuse to
  write unless the ONLY changes are the intended cells; **(3) move BOTH TWINS**
  (`docs/sf_fundamentals`↔`scripts/fundamentals`, `docs/sf_revop`↔`scripts/revop_fundamentals`) or the
  next rebuild reinstates the old value; **(4) rebuild per-stock slices into a TEMP dir and copy only
  those that changed** — 3 of 4,550 here, so no other session's derived output was swept in.
  ⚠️ **`pat_defects.json` is a JOURNAL** (read only by `fill2020_tools/gicre_con_pat.py`) — writing
  there records a defect but does NOT fix data. The PAT fix had to be a §2b guard-edit.
  ⚠️ Re-verify the guards against ORIGIN immediately before applying if another session has been
  writing: one had landed a `mirror resynced` commit 28s earlier, and for GICRE the MIRROR held the
  CORRECT value — a std-side resync would have destroyed it. All six guards were re-checked and held.
- **★ STILL OPEN after the heal:** HDBFS `revC` also holds Interest income — including at 2025-06-30
  where the filing states no consolidated statement exists — not adjudicated. HDBFS 2024-09-30 sits in
  the defect window but was never read, so it was deliberately NOT written (never write a value no
  source asserts). Plus the 43 sweep candidates and 286 sweep 404s needing slug verification.
- **★★★ AADHARHFC PROVEN, AND THE ROOT CAUSE IS A TOOL, NOT A COMPANY.** Its stored std revenue for
  2023-06-30 and 2023-12-31 equals the filing's **`Interest income` row EXACTLY** (533.47 == 533.47,
  579.26 == 579.26) against Total-revenue-from-operations of 578.01 and 658.54. **Cause:
  `screener_prerev.py` accepts a Screener revenue row once the page's NET PROFIT matches our stored
  PAT — a PAT-ONLY anchor, so a wrong REVENUE row passes undetected.** Aadhar IPO'd 2024-05-08 and
  was invisible pre-IPO to every standard route (only listed NCDs/CPs), so those quarters came
  through that scraper; from 2024-03-31 the normal post-listing pipeline is exactly right, and the
  small residual 2024 gaps are a std-vs-con artefact, not a row defect.
  **EXPOSED COHORT: `scripts/screener_rev_fills.json` holds 191 cells filled on that PAT-only
  anchor — every one is the same risk class and none has been re-checked.**
  Reached via the **BSE debt-segment scrip route** (§44's playbook applied to a pre-IPO NCD issuer
  rather than a delisted one — reusable for any recent-IPO NBFC/HFC).
  ⚠️ **An anchor that validates one field does not validate another.** PAT matching says the page is
  the right company and quarter; it says nothing about which revenue row was taken.
- **★ HUDCO's con==std is GENUINE, and it recalibrates the whole con-copy screen.** Its filings state
  in their notes that consolidated == standalone because the sole associate contributes 0.00, and
  each consolidated statement prints its own distinct associate line — derived, not pasted. So the
  **6,470 con==std cells** `bulk_screen.py` flags are mostly LEGITIMATE. A control quarter is what
  earned that finding: 2022-06-30 detected a real 0.05cr associate divergence our store flattens AND
  exposed the Interest-Income sub-line defect. **Always include a control where the answer is
  already known — it is what makes a negative finding credible instead of a blind spot.**
- **Re-run:** `python3 -X utf8 scripts/revpat_verify/audit_revpat_coverage.py` (offline, reads this
  checkout's HEAD); `revpat_strata.py` re-derives the frozen 66-symbol sample identically;
  `build_contested.py` rebuilds the internal-divergence queue; `revpat_mapcard.py --extract <jsonl>
  --site <s>` derives a card and REFUSES below 80% hold; `revpat_quorum.py --base <dir>` applies
  rule 6b using accepted mappings only.

### 71c. The 23 suspects: 6 settled, 17 need the document — and the screener trap in adjudicating them
Of the 23 cells §71b held back, **6 are closed**:

```
NUCLEUS    2025-12-31  npCon 250.20  -> 20.70      screener 21, mirror agrees
OSWALAGRO  2023-06-30  npCon   0.30  ->  4.28      screener 4.28; npCon was a copy of npStd
RELCAPITAL 2023-03-31  npCon 2436.50 -> -1502.57   screener -1499; fund had the wrong SIGN too
ZEAL       2026-03-31  npCon 4114.27 ->  6.53  AND npStd 4114.27 -> 7.15 (both bases junk)
SURANAT&P  2025-09-30  npCon   0.17  ->  0.76      filing: 0.17 - (-0.59) = 0.76 exactly
VADILALIND 2025-12-31  npCon  -0.15  -> -0.16      FALSE ALARM: the quarter really is ~0
```

**★ screener cannot adjudicate this class on its own, and it is tempting to think it can.** It
quotes **TOTAL** PAT. So "screener agrees with sf_fundamentals" establishes only that fundamentals
holds the TOTAL — not that it is right on our owners basis. Where NCI is material the mirror may be
the correct value. That is why only cells with a *filing* read (SURANAT&P, VADILALIND) or where
fundamentals is demonstrably junk (the other four) were written.

**The 17 that abstained, with the first rung that broke** (now in `_fund_suspect_cells.json` as
`abstain_reason`, so the next attempt starts from a diagnosis):
* 6 — no profit-for-period row co-occurring with the target column
* 4 — no NCI row on the page that carries the target quarter (3IINFOLTD, DELTAMAGNT, NAZARA, TRF)
* 3 — rows found but `period − NCI != owners`; wrong rows picked (AXISCADES, IFCI, LANCORHOL)
* 1 — IRB 2020-09: the target quarter appears in **no** header row across 54 consolidated pages
* 1 — JHS 2018-09: **no consolidated page at all** in any fetched PDF — scanned, the VISION rung
* 2 — SUBCAPCITY ×2: **no scripcode in `scrip_map`**, so the BSE stream is unreachable. Structural,
  not a reader gap — fix the map first.

Three automated passes (loose read, header-gated read, relaxed labels) resolved 1 more between
them. The blocker is not one fixable gap; it is per-cell page layout. Do not keep widening regexes
against it — the loose pass matched narrative prose and would have written nonsense.

### 71d. `owners_basis_heals.json` now outranks `_reattr_owners` too
Caught by the §65c check: `_reattr_owners.json` still held NUCLEUS 2025-12-31 at **250.20** and
would have reverted the repair to it on the next nightly `apply_owners_full`. That script now reads
`owners_basis_heals.json` with the same precedence it gives `con_copy_heals.json`, so every
owners-vs-total repair from this session is pinned. **Any new heal ledger needs adding there, or
the nightly job silently undoes it.**

### 71e. ★ JHS 2018-09 — the cell queued for VISION did not need it
Vision was approved for JHS 2018-09-30 (the one cell §71c flagged as "no consolidated page at all —
scanned"). **It was resolved without a vision read**, and the original diagnosis was wrong twice:

* the OWN-quarter PDFs did not fail to *parse*, they **failed to fetch** — 3 of 4 returned no bytes.
  That is transport (§57c), and "no consolidated page" was the wrong conclusion to draw from it;
* the Q+4 filing (Sep-2019, carrying Sep-2018 as its year-ago column) **is** scanned, but it ships
  an OCR text layer, and that was enough. Rung 6 of the ladder, not rung 10.

**OCR corrupts digits in that document, so no printed number was trusted alone.** Every value is
pinned by an identity the page asserts:
```
period Sep-18 = PBT -106.56 - tax (13.90 + -18.81)        = -101.65  printed, exact
owners Sep-18 = period - NCI (-3.71)                      =  -97.94  OCR shows "[92 94)"
control  Sep-19 owners -54.48 + Jun-19 owners -28.73      =  -83.21  = printed H1 owners
control  Sep-19 PBT -98.09 - tax 96.18                    = -194.27  = printed period
control  owners -97.94 + OCI owners 0.39                  =  -97.55  = printed TCI owners
```
-97.94 lakh = **-0.98 cr**, comfortably inside JHS's ~1.68 family median. Both the stored -27.82 and
the mirror's 0.93 were wrong.

**Before escalating anything to vision, re-fetch the own-quarter PDFs and read the NEXT-YEAR
filing.** A failed fetch reads exactly like a scanned document in the logs, and a scan with an OCR
layer is a text read, not a vision read — the expensive rung stays unused.

---

## 73. ★★★ std-PAT TWO-FILES SWEEP CLOSED AT ZERO — 46 cells adjudicated, both files lose  (2026-08-10)

§70/§71 closed npCon-vs-patC; the SAME fingerprint on the std side (npStd vs sf_revop patS idx4,
tol max(0.02, 0.5%)) found **46 disagreements**. Every one was taken to a primary document
(cached/fetched filer XBRL, BSE detres §42 with the FY quarter-sum identity §45, announcement PDF
§58) and EPS-recon where the tags allowed. Verdicts + per-cell provenance:
`scripts/stdpat_adjud_verdicts.json`; applier `scripts/_stdpat_apply.py` (guard-edit §2b +
blast-radius + all four twins). **Score: sf_fundamentals wrong 25, the mirror wrong 21** — the
"authoritative file" lost more often than the copy. Six companion defects healed en route
(DHARSUGAR Jun-23 swap twin, ABCOTS/SURAJEST/DBL con, KALYANI ×2), all document-proven.

Defect classes worth re-detecting (each is a SCAN, not a one-off):
* **H1/H2/FY-cumulative stored as the quarter in sf_fundamentals** — 13 cells, all SME/young
  listings (+ SHIVAMAUTO holding the full FY). The filer submits an integrated-filing XBRL whose
  OneD *declares* the 6/12-month span; update_fundamentals ignores context dates by design (§45's
  double-indexing lesson cuts both ways). Detector: fund value ≈ sum of stored sibling quarters.
* **Wrong-year/wrong-period OneD declarations** — KOHINOOR/SURAJEST/SIEMENS class: a filing
  submitted in 2025-26 declares OneD dates a full year earlier (or a stale Apr-Jun label on a
  FY-transition Oct-Dec quarter, SIEMENS). build_revop's cache re-parse trusts the declared dates →
  the MIRROR gets poisoned while fund (announce-date keyed) stays right. The filename timestamp vs
  the declared period is the tell (§45).
* **NSE double-indexed Mar-2019 cluster** — 6 mirrors held the JUN-2019 quarter's value
  (POLYMED/GSFC/CARERATING/EMKAY/GREAVESCOT + METROPOLIS blank-zero). detres + FY identity settled
  every one; the fund std announce dates of that cluster still carry the Jun-19 filing date
  (20190904) — ann-date heal candidates.
* **Revised filings, and the principled line**: an ORIGINAL that contradicts its own YTD/EPS
  (RUBYMILLS 8.0694 vs its printed H1; HOMEFIRST 93.325 vs its printed 179.993) is an arithmetic
  error → adopt the correction (ann date moves to the revision date). An original that is
  internally consistent (MADRASFERT +62.21 audited, EPS-recon exact; VIJSHAN −10.6554) is a
  genuine RESTATEMENT → point-in-time keeps the first filing (§2d TRU precedent); the revision is
  journalled, not stored.
* **XBRL sign-flip vs the filer's own EPS row** — SGFL tagged +1.2092 while printing EPS −0.96;
  the FY19 identity closes only with the corrected signs. The EPS row arbitrates sign, same as it
  arbitrates the §2d tag-swap.
* **★ WRONG COMPANY under a shared ticker — KALYANI.** Ours is Kalyani Commercials Ltd
  (INE610E01010, NSE); `bse_scrips.json by_id` maps KALYANI → 544023, a DIFFERENT company
  (INE0N6U01018). Stored Mar-24/Dec-24/Mar-25 came from the wrong company — its detres FY25 annual
  (14.245) equals the stored 8.01+6.23 EXACTLY, while Kalyani Commercials' own filings print
  0.319/0.255/0.7237 with a closing FY chain and its printed revenue 102.8994 == our stored revS.
  Fourth ticker-identity trap after TRU/CCL/SHK (§72) — **a scrip_id equal to the symbol is a
  coincidence to be disproved; gate on ISIN.** Series follow-up (missing Jun/Sep-24, revenue audit,
  resolver guard) is an open task.

Durability: mirror values now live in `scripts/stdpat_mirror_heals.json`, wired into
verify_fills_live LEDGERS (patS slot 4); the two con heals whose `_reattr_owners` cache still
holds the refuted number (SURAJEST Jun-24, DBL Sep-25) are in `owners_basis_heals.json` (§71d
precedence). fund-side std heals are safe from nightlies (update_fundamentals is fill-only on
non-null; apply_owners_full touches con only).

Open flags: **ALL SIX ADJUDICATED AND CLOSED the same day — see §77.** Score: 5 of the flagged
cells were CORRECT and 2 were wrong, one of them a heal *this campaign itself* made backwards.
The KALYANI series follow-up closed the same day too (§76, commit e346d2e8), so **nothing from
this campaign remains open.**

---

## 74. ★★★ SCALE-STEP PAT CAMPAIGN — 128 candidates, the LAKH-AS-CRORE class, and the detres unit trap  (2026-08-10)

**NO ASSUMPTIONS, NO GUESSWORK — every value written traces to a document read this session.**

Detector (reproducible): consecutive non-null PAT triples per symbol/basis on origin/main
sf_fundamentals; |mid|/|nbr| or its inverse in [9,11] or [90,110] on BOTH sides, same direction,
min side > 0.05; exclude cells whose matching-basis revenue jumps ≥5× the same way; subtract
scale_fix/pat_defects/sanity_ok. 2026-08-10 run: exactly **128 fresh (77 std, 51 con)**.
Adjudicated 128/128 minus a measured open tail; commit 665a1d9d healed 97 slot-cells + 20 fills +
12 nulls and allowlisted 65 genuine spikes. What the campaign established:

* **The dominant defect is the filing's ₹-LAKH print stored as CRORE (×100)** — the stored junk
  value LITERALLY EQUALS the number printed on the PDF's lakh table (HALDYNGL std Jun-25 401.46,
  DHABRIYA con Dec-23 333.52, INDPRUD con Mar-26 1729.41 — which is the doc's own OCR-typo'd NP
  row, owners row 1749.41 proven by std 3.85 + KSB-associate 1745.56 EXACT). Often combined with a
  quarter/year SHIFT: INDPRUD stored each quarter's lakh print one quarter LATER (96.47 = Jun-24
  std lakh → stored as Sep-24 in BOTH slots); SAYAJIHOTL Mar-20 held Jun-20's loss ×100 (both rows
  announced together 2020-07-30); SEKURITIND's whole series is sentinel junk (0.06/−0.05/0.0)
  with real values year-shifted — 18 heals + 16 fills, every FY18–FY26 identity EXACT.
* **The mid of the triple is not always the sick cell.** HALDYNGL/MAGNAELQ/INDPRUD Dec-25: the
  flagged mid was the HEALTHY value and both neighbours were ×100. Always adjudicate the whole
  window (§45 re-scan rule), never just the flagged cell.
* **§42 detres values are NOT always ₹ million.** SAYAJIHOTL rows mix lakh and million ACROSS
  QUARTERS of the same scrip; the printed EPS row can also be wrong (Dec-20 prints 0.02 where the
  identity forces 0.20). Arbitrate the unit per row with EPS×share-count, and let an EXACT FY
  quarter-sum identity outvote a single row's EPS.
* **Half-yearly filers poison quarterly cells**: PJL and TECHKGREEN (SMEs) file H1/H2 only; our
  Mar cells hold H2 by series convention (PJL Mar-24 heal = H2 print 410.04 lakh, H1+H2 == the
  detres .50 audited annual EXACT), and phantom Dec/Jun "quarters" hold misplaced H1 prints.
* **xbrl_extra.json.gz is a free con-basis adjudicator** for NSE-parsed symbols (2,793): per-basis
  eps_b/pbt/tax/exc straight from the filings settled ~15 con cells in minutes (BBTC Go-First
  exceptional on-page; GFLLIMITED pbt 0.57 − tax 35.72 = −35.15 EXACT; ABAN eps −707.53). Check it
  BEFORE fetching any PDF.
* **Genuine ≠ small**: 65 spikes are real and allowlisted (DIXON eps 42.93, MAXVIL's gain sits in
  discontinued-ops BELOW pbt, CANHLIFE = Canara HSBC Life, an insurer with 12 consistent March
  spikes). sanity_ok.json, never wider tolerances.

**Open queue (measured, no route left short of vision/NSE):** GVKPIL con Jun/Sep-21 (BSE has no
filings in those windows — NSE lane untried); MKVENTURES con Mar-25/Jun-25/Dec-25/Mar-26 +
TECHKGREEN Dec-24/Jun-25/Mar-25-con (result tables are scanned or custom-font glyphs → vision
rung, ask first); AVONMORE Mar-24/Dec-25, MASTERTR Mar-25, GINNIFILA Jun-19 (detres empty);
SHRAAITECH std Mar-23 50.82 (SME pre-migration era); pre-2015 tail with no detres (PEL×3 2011-12,
ALOKTEXT Jun-12, ASIANELEC Dec-08, WOCKPHARMA Mar-06, SHREERAMA Dec-03, WIMCO Mar-04, GEORGWILIM
Jun-04, REIAGROLTD Dec-13, FIRSTLEASE Mar-11, HINDOILEXP handled, USHAMART handled); unresolved
con OTHERs (PNB Dec-18 437.93, BBOX Jun-20, TITAGARH Mar-16, RML Dec-17, PAISALO Mar-17 — already
in §45's queue, AIIL Jun-23, MANINFRA Sep-20 −0.12, GLOBOFFS Mar-19 con); scrip-resolution class
(JMTAUTOLTD, CLCIND, JMA, BILENERGY, SUPREMEENG had code, MAXVIL had code). Tooling for a rerun:
the detector + detres/PDF readers live in this campaign's scratchpad pattern — detector spec is
fully stated above; detres endpoint + QID formula in §42.

**Post-heal re-scan (closure evidence).** Re-running the detector against the pushed origin/main
with correct suppression (pat_defects is NESTED `{SYM:{QE:{}}}`; sanity_ok is a flat `SYM|QE` list —
a suppression reader that misses either shape reports every healed cell as still-open, which is how
the first re-scan printed a bogus 104): **128 → 31 open, 73 suppressed**, and all 31 are exactly the
documented queue above — no unexpected residue, no heal re-firing.

**GVKPIL Jun/Sep-2021 con — the NSE lane walked, and why it is still open (measured, not assumed).**
BSE `datebound` returned nothing for those windows because GVKPIL filed its whole 2020-21 backlog
LATE (the Mar-2021 rows carry `_29122021` filing stamps) — an empty date-bound window is a filing-
CALENDAR artefact here, not absence. The NSE `corporates-financial-results` index DOES list Jun-21 /
Sep-21 / Dec-21 on both bases, so the results exist — but every one of those rows serves
`.../corporate/xbrl/-` (an empty attachment link). The §45 year-ago-comparative fallback also fails:
the Sep-2022 con filing's ONLY context periods are Q2-FY23, H1-FY23 and two instants — **no 2021
period is present in the file at all**. Both index routes and the comparative route are therefore
measured-negative; what remains is the announcement PDF of the late 2021 filings or vision. Recorded
so the next pass does not re-walk them.

**Durability, measured before leaving (not assumed).** Three jobs could undo these heals, so each was
checked: `update_fundamentals.py` is fill-only on missing fields (non-null heals are safe);
`apply_owners_full.py` rewrites con PAT from `_reattr_owners.json`, and **0 of the 59 healed con
cells appear in that 23,839-entry cache**, so tonight's run cannot re-poison them (§71d would
otherwise need owners_basis_heals entries); `ci_preserve_merge.py` + `verify_fills_live.py` run
inside refresh-fundamentals, and the 77 new pat_defects entries are now in the watched set, so a
future clobber turns CI red instead of failing silently.

### 73a. ★★ THE LIVE RE-VERIFY EARNED ITS KEEP — a std heal does NOT reach a no-sub company's con slot

The §73 batch verified clean at push time and clean on the served payloads (all 55 verdict cells,
live scan 0 disagreements). The SAMPLE printout is what caught the miss: ASTRAZEN's live row read
`std 18.63 / con 186.35` — the con slot still holding the ×10 value the FY identity had refuted.

**Mechanism.** For a company that files no consolidated result, `update_fundamentals.is_nosub`
sets `npCon = npStd` (accounting identity). That fill is FILL-ONLY and runs once. Correct the std
later and **nothing propagates** — the con slot keeps the refuted number forever, and because the
std-vs-mirror scan only compares npStd against patS, the stale con is INVISIBLE to it.

Three cells healed on this (ASTRAZEN 2020-06, HOMEFIRST 2024-09, VENKEYS 2025-03), each with the
no-sub premise PROVEN rather than assumed: zero Consolidated rows across the full NSE history
(84/17/79 classic rows all Non-Consolidated; integrated rows Standalone) and **135 nature tags
across 95 cached XBRLs, every one Standalone, zero Consolidated.**

★ **Do not CREATE a con mirror value where none exists.** The applier initially wanted to write
`patC` for these cells. `patC` is null in every other quarter of a no-sub filer (build_revop only
records what a filing prints), so writing one would make the healed quarter the sole cell asserting
a consolidated figure no document contains. Fund `npCon` legitimately carries the identity — that
is its documented convention; the mirror does not. **Correct what is there; never invent.**

★ **Whenever you heal a std cell, check the con slot of the same row before you commit** — and
after any heal, print the neighbourhood live rather than only asserting the cell equals its target.
A per-cell equality check passes while the row beside it stays broken.

**Store-wide screen (17 cells / 17 symbols, `nosub_con_lag_screen` in the verdicts file):** same
fingerprint — con==std in ≥95% of a symbol's quarters, differing in ≤3. It is a SCREEN, not a
verdict (§71g: that class of screen was right 6 times in 23). Two carry a strong scale signature:
SIMPLEXCAS 2020-06 std −282.03 vs con −2.82 (**exactly ×100**) and VISHAL 2025-03 488.18 vs 7.35.
None healed without a document.

### 73b. ★★★ THE 17-CELL SCREEN ADJUDICATED — 9 real, 8 false, and the screen's "certain" half was half-wrong  (2026-08-10)

**NO ASSUMPTIONS, NO GUESSWORK — every value written below traces to a document read that session.**

All 17 cells of §73a's `nosub_con_lag_screen` taken to primary documents. **9 healed, 8 confirmed
correct.** Verdicts + per-cell provenance: `scripts/nosub_con_lag_verdicts.json`; applier
`scripts/_nosub_con_lag_apply.py` (guard-edit §2b + blast radius + both fund twins). 13 COMPANION
cells fell out of the same documents and were healed with them (§73 precedent) — 22 cells / 60
slot-values in all.

**The screen's two "strong scale signature" cells are the lesson.** It called SIMPLEXCAS 2020-06
(std −282.03 vs con −2.82, "exactly ×100") and VISHAL 2025-03. Both were indeed ×100 — *and trusting
that would still have written a wrong number*:
* **SIMPLEXCAS: BOTH slots were wrong.** The filing (Rs LAKH) prints std **(282.03)** and consolidated
  owners **(278.28)**. So std was the lakh print held as crore, and con was holding a *copy of
  standalone* — the real con is −2.78. Healing "con is right, fix std to match" would have locked in a
  wrong con.
* **VISHAL: only std was wrong.** con 7.35 was correct all along (screener's consolidated series prints
  7.35 exactly). The screen's framing — "the two disagree, one is stale" — had no way to tell which.

**The §73a recipe held, with one addition.** Establish whether the company files consolidated AT ALL
first; then *verify the std against a filing before propagating it into con*, because **the
disagreeing side was std in 4 of the 9 heals**. Added: for the 9 BSE-only names the NSE index is
empty **because they are not NSE-listed** (absent from EQUITY_L's 2,410 rows) — that emptiness is a
listing fact, not evidence, and the question has to be answered on the filing itself plus screener.

**Defect classes found (each a re-detectable scan):**
* **Rs-LAKH print stored as CRORE (×100)** — the §74 class, 11 cells (SIMPLEXCAS ×6, VISHAL ×4, …).
* **FY ANNUAL stored as a quarter** — ZEAL's FY2025 annual (1,005.933 lakh = 10.06 cr) sat in *two*
  different quarter slots, Mar-25 and Dec-25.
* **H1 CUMULATIVE stored as a quarter** — AUTOINT Jun-25 held the printed H1-FY26 417.70 lakh.
* **OCR digit misread** — AUTOINT Sep-24 held **107.31** where the filing prints H1 **407.31**; a
  reader turned the leading 4 into a 1, and our store kept it.
* **Year-shifted value** — AUTOINT Sep-25 std held the SEP-2024 figure (2.68).
* **con slot holding a COPY of std** where a real consolidated figure exists (SIMPLEXCAS).
* **blank-template zero** — PRECOT Mar-20 con 0.0 against both XBRLs printing −0.81 (§73's METROPOLIS class).

**Four tooling facts worth more than the cells** (each cost a wrong turn):
1. **§42's QID formula: the "FY" is the CALENDAR year of the quarter END, not the fiscal-year label.**
   Reading it as fiscal returned a response a full year off (asked Jun-2020, got `Date End 30-Jun-21`).
   85 = Mar-2015 anchors the calendar reading. The period discipline caught it — *always* read the
   response's own Date Begin/End and refuse a mismatch.
2. **BSE `AnnSubCategoryGetData` returns at most 50 rows for `pageno=1`.** A 12-month window on a busy
   filer silently truncates, so **"N consolidated mentions in that window" is a LOWER BOUND and never
   proof of absence.** The first scan's 0-counts (BCCFUBA, AUTOINT, ZEAL, CUB) were discarded for this
   reason; every no-consolidated verdict here rests on the filing itself and/or screener + the NSE index.
3. **`fetch_bse_fund.py` sets `basis` as `"C" if "consol" in headline.lower()`** — the exact substring
   bug `build_fundamentals.is_con_basis` exists to kill (`"consol" in "non-consolidated"` is True, and
   "Standalone and Consolidated …" is the commonest headline of all). The `basis` field in
   `docs/bse_fundamentals.json` is therefore not evidence.
4. **A fiscal year is not always Apr–Mar.** KENNAMET's ends 30 JUNE ("quarter & half year ended
   December 31"), so its `.50` audited annual lives on the JUNE quarter — which is why the Mar-2025
   `.50` correctly refused.

**screener.in as a CON-basis reader (§60 extended).** Where screener's consolidated series reproduces
stored cells we already trust *exactly*, it is a validated second reader for that company's con basis
(it settled VISHAL con 7.35 / 4.79 / 9.16 after reproducing our Dec-24 9.59 and Mar-25 7.35 to the
paisa). It quotes TOTAL PAT (§71g), so it still cannot arbitrate an owners-vs-total question.

**Identity traps re-confirmed.** `KALIND` (scrip 526935) now files as **ARUNIS ABODE LIMITED** — the
identity guard tripped on the stale master name and was right to; gate on scrip+ISIN, not name.
`PRECOT` has no `bse_scrips.by_id` entry at all (its BSE scrip_id is `PRECM`, code 521184,
BSE-delisted) and had to be resolved by ISIN.

**Measured and deliberately NOT written** (in the verdicts file's `open` list): ZEAL's Mar-2026
*revenue* (revS 114.0 vs the filing's 136.86 — a revenue defect, routes through `rev_defects.json`);
two announce-date defects (SIMPLEXCAS Jun-2020 stored 20200730, the date of its MAR-2020 filing —
COVID pushed the real filing to 2020-09-14; CUB Mar-2022 con 20220525 vs the 27-May board meeting);
and ZEAL Dec-2024, where stored 6.31 / detres 6.27 / screener 6.36 disagree three ways and no
document read that session resolves it.

### 73c. ★★★ THE LIVE RE-VERIFY EARNED ITS KEEP AGAIN — a heal can lose to another HEAL LEDGER

§73a's live re-verify caught a stale con slot. §73b's caught something new and worse: **ZEAL
20260331 con back at 6.53 on the served payload while std held at 2.04** — and not from CDN lag.
`origin/main` itself had been rewritten, 18 minutes after the push, by the daily refresh's
`apply_owners_full` step. Its log line names the culprit exactly:

    set npCon=owners: 0 from _reattr_owners, 0 via rename-alias, 0 from filing-backfill, 1 from con_copy_heals

**The heal did not lose to a cache. It lost to another HEAL LEDGER** —
`owners_basis_heals.json`, which the §71d precedence (added 2026-08-09, correctly) makes outrank
everything. A 2026-08-09 entry pinned `ZEAL|20260331|patC = 6.53`, so every nightly run re-asserted
it over the new heal, forever.

★ **Before leaving ANY con-PAT heal, check it against the ledgers that outrank you** — today
`con_copy_heals.json` and `owners_basis_heals.json`, both consumed by `apply_owners_full.py`.
§74 checked `_reattr_owners.json` (0 of 59 cells present) and stopped there; the heal ledgers are a
SECOND, higher-precedence layer that check did not cover. One line settles it:

```python
led = {}
for f in ("scripts/con_copy_heals.json", "scripts/owners_basis_heals.json"):
    d = json.load(open(f)); led.update(d.get("cells", d))
clashes = [k for sym, qe in my_healed_cells for k in ("%s|%s|patC" % (sym, qe),) if k in led]
```

★ **When you do clash, UPDATE the ledger entry — never route around it.** The precedence is
correct and exists because heals used to be silently reverted; the fix is to own the entry, with
the superseded record preserved inline so the next reader sees both readings.

**Which value was right, and why the old one lost.** The 2026-08-09 entry's own note says it was
written on *"screener 7 on both"* — a ROUNDED read — and anchored on *"a series of
6.31/10.06/1.54/3.00/10.06"*, a neighbourhood that itself contained the FY2025 ANNUAL 10.06
duplicated into TWO quarter slots. **An anchor is only as good as the row around it**
(§68/§73b's own rule, arriving one campaign late). The company's own audited filing, BSE detres and
screener's standalone table all say 2.04.

**Durability, measured before leaving:** `apply_owners_full.py` re-run locally after the ledger
edit reports `0 from con_copy_heals` and leaves the cell at 2.04; the other 21 cells were checked
against both ledgers (ZEAL was the only clash); `verify_fills_live` exits 0 with ZEAL drift 0.

⚠️ **`verify_fills_live` REPORTED this and did not stop it.** It exits `1 if missing else 0`, so a
DRIFT — a ledgered cell whose live value was overwritten by a *different* number — prints and the
run still goes green. The CI log for the very run that reverted ZEAL contains
`DRIFT pat_defects.json ZEAL 20260331 ledger=2.04 live=6.53`, and nothing failed. Drift is exactly
the clobber signature, so treat any DRIFT line in a CI log as a red flag to investigate by hand
until that exit code is tightened.

★ **Journals are append-only across campaigns — NEVER rewrite one with `sort_keys=True`.** Doing so
reordered hundreds of untouched lines in `pat_defects.json` / `owners_basis_heals.json` and
silently dropped the latter's `generated` key. `json.load` preserves file order; dump without
`sort_keys` and every entry you did not touch stays byte-identical.


---

## 75. ★★★ THE TRIPLE-RENDERED TEXT LAYER — a filing that is perfectly legible and unreadable  (found 2026-08-10)

**NO ASSUMPTIONS, NO GUESSWORK** (§0) — everything below is measured on the documents named.

A third corruption class, distinct from §51b's glyph substitution and from a plain scan. ABCAPITAL's
2019 result PDFs carry a text layer **rendered three times, stacked at the same coordinates**:

    Total Total Total Revenue Revenue Revenue from from from operations operations operations
    [3,645.75, 3,845.75, 3,645.75, 4,729.82, 4,729.82, 4,729.82, ...]

All three copies of a token start within ~0.5pt of each other (three `Total` tokens at x0=75.87,
y centres 223.78/223.72/223.88). Consequences, and why it costs whole campaigns silently:
* **No label regex can match** — `revenue\s+from\s+operations` never appears contiguously — so
  `extract_rows` returns nothing and `backfill_revop_gaps` files the cell as
  **`no-anchor-or-scanned`**, the same bucket a genuinely scanned PDF lands in.
* **`PL_PAGE` still matches** (it finds the words), so the page is not flagged as unreadable
  either. The filing looks tried-and-failed when it was never actually read.
* Spelling-tolerant fragments (§51b's cure) do NOT help: the words are intact, they are duplicated.
  The fix has to be positional.

### 75a. The duplication is a GIFT — it is a built-in majority vote
The copies come from independent render passes and **disagree on digits**: 3,645.75 / 3,845.75 /
3,645.75 and 15,163.51 / 15,183.51 / 15,163.51. So collapse each position bucket
(3pt × 3pt on (x0, y-centre)) to its **majority token, and DROP the bucket when no strict majority
exists** — a noised figure then cannot reach the store at all. 2pt buckets are too tight: they split
ABCAPITAL's own columns in two and a row comes back with the same column twice.
Tool: `scripts/fill2020_tools/deoverlay_rev_reader.py` (`deoverlay_words`, `is_overlaid` — de-overlay
fires only on pages measured to be overlaid, ≥35% of buckets holding ≥2 tokens, so normal pages are
read exactly as before).

### 75b. Two reader traps this exposed, both general
* **`merge_wrapped` glues a section header onto its first component.** `1 Revenue from operations`
  carries no figures, so it is prepended to `Interest Income 1,835.71 …` — and a first-match reader
  then stores **interest income as revenue**. Rank the candidates: a label that IS
  `Total revenue/income from operations` beats one that merely contains the phrase
  (`pick_rev_row`).
* **The owners-PAT row can appear twice** on these pages (`Profit for the period attributable to
  owners…` printed on two lines); the first copy carried junk cells (−9.0, 10.0) and the second the
  real vector. A quarter-vs-cumulative guard reading the first one sees `None`. Take the LAST
  matching owners row, not the first.

### 75c. What landed, and on what evidence
**ABCAPITAL 2019-03 revC = 4,729.82** — read from TWO independent as-filed documents agreeing to
the paisa: the audited Mar-2019 filing (BSE ann 2019-05-04, column headed *Quarter Ended 31st Mar
2019*) and the Jun-2019 filing (ann 2019-08-02, comparative column headed *31st March, 2019 (Refer
Note 6)*). Both print rev 4,729.82 · other income 0.98 · total income 4,730.80 · **profit
attributable to Owners 258.40 == our stored con PAT exactly**; scale `crore` declared in the header.
The **column is identified by its PRINTED DATE (§55b), not by position** — decisive here, because
the Jun-2019 read is a comparative column, and because it removes the circularity of anchoring on a
stored PAT that may itself have come from that same column.
⚠️ **The FY identity does NOT reconcile and was NOT used**: the four stored FY19 con quarters sum to
15,125.23 against the printed FY 15,163.51 (0.25%). Recorded as a non-reconciling corroboration
rather than quietly dropped or quietly leaned on (§45). Ledger:
`scripts/named_rev_cell_fills_2019.json`; the applier re-proves the PAT anchor against current data
and refuses if it has moved.

### 75d. Status of the reader — STAGED, NOT A WRITER
`deoverlay_rev_reader.py` now parses these pages correctly (printed-date column selection +
stored-PAT anchor + the rev+other-income==total-income identity, fill-only) but **lands nothing
yet**: on some pages its owners-row pick hits the duplicate line first, so the quarter-vs-cumulative
guard sees `None` and refuses. That is the honest state — a reader that refuses is safe; a reader
that guesses is not. Fixing 75b's second bullet is the next step.

### 75e. Diagnose the bucket, never accept it (§61)
`scripts/fill2020_tools/diag_rev2019.py` replaces `no-anchor-or-scanned` with the stage that
actually failed: `no-filing-listed` / `pdf-unfetchable` / `scanned-no-text` / `no-pl-page` /
`bank-or-insurer-fmt` / `basis-absent` / `rows-unparsed` / `no-stored-pat-anchor` / `anchor-failed`.
Each points at a DIFFERENT rung, which one bucket cannot. It also resolves **delisted** scrips from
`_bse_master_all.json` — `bse_scrips.json` is built from the live master and returns nothing for
ALBK / ANDHRABANK / CORPBANK / DHFL (§52b), which otherwise reads as "no filings".
---

## 76. ★★★ A `scrip_id` EQUAL TO THE TICKER IS A COINCIDENCE TO BE DISPROVED — gate on ISIN  (2026-08-10, KALYANI)

**NO ASSUMPTIONS, NO GUESSWORK — every value below traces to a document read this session.**

`bse_scrips.json` `by_id` is BSE's **`scrip_id` → `SCRIP_CD`**. Every consumer in this repo uses it
as **"NSE symbol → BSE scrip code"**. Those are two different namespaces that happen to collide, and
when they collide the resolver hands you a *different company's* filings.

    our KALYANI = Kalyani Commercials Ltd   INE610E01010   NSE-only — NOT on BSE at all
    by_id[KALYANI] -> 544023 = Kalyani Cast-Tech Ltd  INE0N6U01018   (scrip_id happens to be "KALYANI")

Damage before it was caught (§73 found it via the std-PAT two-files sweep): three std PAT cells held
Cast-Tech's profits (Mar-24 3.73, Dec-24 8.01, Mar-25 6.23 — its detres FY25 annual 14.245 == the
stored 8.01+6.23 EXACTLY), three **con** slots were invented for a company that files standalone
only, and Mar-24's announce date was taken from Cast-Tech's calendar.

### 76a. Why nothing else catches it
This is the failure mode every other guard is blind to. A wrong scrip code fails **no** magnitude
check, **no** anchor, **no** identity guard that only verifies "the document I fetched is the
document I asked for" — the filing is internally perfect, consistent, and correctly parsed. It just
belongs to somebody else. Name similarity actively *helps* the trap (Kalyani/Kalyani, Focus/Focus).
**Only ISIN — the one identifier both exchanges agree on — separates them.**

Corollary to §71f: a symbol with no BSE scrip is *normal*, not a gap to be filled. 1,193 of our
3,942 symbols have no BSE code, and Kalyani Commercials is one of them (0 records for its ISIN in
the 10,786-row active-equity master). **Never "fix" a missing mapping by reaching for a same-string
scrip_id.**

### 76b. The scan — measured, and mercifully small
`scripts/scan_scrip_isin_conflicts.py` joins three identifiers: NSE `EQUITY_L.csv` SYMBOL→ISIN, the
BSE master SCRIP_CD→ISIN_NUMBER, and our by_id claim. Result over sf_fundamentals' 3,942 symbols:

| outcome | count |
|---|---|
| ISINs agree | 2,225 |
| **CONFLICT (wrong company)** | **2 — KALYANI, FOCUS** |
| no BSE code (normal) | 1,193 |
| uncheckable (delisted/renamed, absent from EQUITY_L) | 522 |

FOCUS = Focus Lighting and Fixtures (INE593W01028) mapped to BSE 543312 Focus Business Solution
(INE0DXR01010). Its stored rows' announce dates all match its OWN NSE broadcast dates, so the
mis-map had not yet been used to fill them — **mapping proven wrong, values not audited.**
The 522 uncheckable ones are reported as uncheckable, never merged into the pass count (§57a rule 4).

### 76c. The guard — `scripts/bse_resolve.py`
```python
import bse_resolve as BR
by_id = BR.by_id()            # bse_scrips by_id with conflicting symbols REMOVED
BR.guard(sym, code)           # -> code, or None when sym is a known conflict
BR.guard_map(m)               # filter any {SYM: code} map
BR.blocked(sym)               # -> human-readable reason, or None
```
Deny-list: `scripts/bse_scrip_isin_conflicts.json` (tracked). Refresh with
`python3 scripts/scan_scrip_isin_conflicts.py --write` — it exits 1 on any conflict not already
recorded, so it can fail CI loudly.

Applied in three layers, because one is not enough:
1. **KALYANI and FOCUS removed from `bse_scrips.json` by_id** — fixes all ~11 direct consumers at
   once, with no code change. (`by_isin` is left intact: ISIN keys are unambiguous by construction.)
2. **`backfill_ann_dates_bse.scrip_map()` guarded** — cleaning by_id is NOT sufficient there: it
   falls back to `bse_universe.json` rows whose `r[1]` is the same BSE scrip_id, which would have
   re-supplied 544023. Verified after the change: `scrip_map()` returns 4,966 symbols with
   KALYANI/FOCUS absent and RELIANCE still 500325.
3. **`cut_gaps_0214.resolve()` guarded at the RETURN**, so the guard covers all five of its routes —
   including `scrip_id_match` and `name_match`, which match on BSE-side labels and are the same trap
   wearing a different hat.

⚠️ `bse_scrips.json` is generated from the BSE live master by an ad-hoc fetch, not by a repo script,
so a naive regeneration will reintroduce both rows. **The deny-list is what makes the fix durable —
re-run the scanner after any refresh.**

### 76d. The KALYANI series close-out, and a defect the trap was hiding
Applier `scripts/_kalyani_apply.py` (§2b guard-edit of all four twins + blast radius; journals to
pat_defects / rev_defects / stdpat_mirror_heals / ann_date_fills). Every value re-read from Kalyani
Commercials' own filings — the earlier session's notes were re-fetched, not trusted.

| quarter | action | value | anchor |
|---|---|---|---|
| Jun-2024 | FILL std + rev | 0.62 / 57.85 | H1 chain EXACT |
| Sep-2024 | FILL std + rev | 0.74 / 89.66 | H1 + 9M chains EXACT |
| Mar-2024 | FILL rev, ann 20240527→20240530 | 62.47 | own XBRL; old date was Cast-Tech's |
| Dec-2024 | FILL rev, ann 0→20250210 | 136.90 | 9M chain EXACT |
| Sep-2025 | **HEAL std 0.24 → 0.12** | 0.1238 | EPS + H1 + 9M, 4 locks |
| Mar-2026 | **left at 1.24, flagged** | — | see below |

The chains, all from primary documents: H1 FY25 0.6171+0.7363 == printed 1.3534 and rev
57.8483+89.6564 == printed 147.5047; 9M +0.2553 == printed 1.6087, rev == printed 284.4052; and —
decisively, from an **independent later filing** (the Q4-FY26 statement filed 2026-05-28, which
prints FY25 as its comparative) — the four quarters sum to 233.24 lakh and rev 38,730.46 lakh, both
EXACT. con stays NULL: every NSE list row is Non-Consolidated from Jun-2022 onward (§51/§54).

**★ Sep-2025 — a filer error the wrong-company noise was masking.** The printed Q2-FY26 statement
leaves the quarter's **current-tax cell blank**, so its "Net Profit" row simply repeats PBT (24.10
lakh) — and we had stored that. The company's **own Basic EPS for that same column reads 1.24**,
i.e. 12.40 lakh on 1,000,000 shares (denominator confirmed by Q1 EPS 6.3 == 63.01 lakh and H1 EPS
7.54 == 75.39 lakh). H1 75.39−63.01 == 12.38; 9M 138.61−63.01−63.22 == 12.38. Healed to 0.12.
*The EPS row arbitrates a missing tax line exactly as it arbitrates a §2d tag-swap.*

**★ Mar-2026 — measured, and deliberately NOT written.** FY26 identity misses by 0.0417: filed FY
2.6718 − 9M 1.3861 = **1.2857**, but the Q4 column prints **1.2440**. The cause is isolated, not
guessed: the Q4 current-tax cell repeats the 9M tax **byte-for-byte** (49.52 lakh; the true Q4 tax
is 96.03 − 49.52 − 1.16 = 45.35), and Q4 PBT 173.92 == FY 362.05 − 9M 188.13 EXACTLY, so the PBT
column *is* the balancing figure while the tax line is not. Revenue and PBT chains both close
exactly ⇒ tax-line error, not a restatement. **It was still not healed**, because 1.2857 is a
subtraction *no document asserts* while 1.2440 is both printed and tagged — §45's "neither side
reconciles → refuse", and the standing rule never to write a value no source asserts. The filing's
EPS cannot break the tie either: its Q4 EPS cell is **blank**, and the 13.86 on that row sits in the
**Q3 column** (where 6.32 belongs), proven by x-position — this filer's EPS row is unreliable in
that document. Journalled in `stdpat_adjud_verdicts.json` open_flags.

**Reading a value out of a printed statement: use x-positions, not text order.** Blank cells collapse
in `get_text()`, so a row with 4 numbers across 5 columns silently shifts. `page.get_text('words')`
+ column x-ranges taken from an unambiguous row (Revenue) is what showed the Q4 EPS cell was empty
(memory: rows-are-geometry-too, now columns too).

---

## 77. ★★★ THE §73 OPEN FLAGS — 5 of 7 flagged cells were RIGHT, and the campaign's own heal was one of the wrong ones  (2026-08-10)

**NO ASSUMPTIONS, NO GUESSWORK — every value below traces to a document read this session.**

The six measured-but-unresolved flags §73 left behind, taken to primary documents. Verdicts +
per-cell provenance: `scripts/stdpat_openflag_verdicts.json`; applier
`scripts/_stdpat_openflag_apply.py` (guard-edit §2b + blast radius + all four twins).
**Score: 5 flagged cells CONFIRMED CORRECT, 2 DEFECTIVE, + 2 companion defects found en route.**

### 77a. The governing lesson: an identity that fails does not name the cell that failed it

Four of the five value flags were raised because a **neighbour** was wrong, not the flagged cell.
The flag text in each case named a plausible mechanism ("wrong-context grab", "H2−Q3 says 19.13",
"out of family ×100") and in each case the mechanism was real but pointed at the wrong quarter.

> **Close the FY chain with every member INDEPENDENTLY sourced before naming a culprit.** A
> residual is a property of the whole chain. Deriving the suspect *from* the chain and then
> "confirming" it against the same chain is circular — the derivation just relocates the error
> into whichever cell you chose to solve for.

RPSGVENT is the cautionary case. Deriving Sep-18 from the 9M column gave 57.96 and the FY annual
agreed — two "independent" anchors, both wrong, because BOTH inherited the same poisoned Jun-18.
Only re-reading Jun-18's own source document broke the loop.

### 77b. ★ A DOUBLE-INDEXED FILE CUTS BOTH WAYS — §73's own retracted heal

`stdpat_adjud_verdicts.json` fund_fix `RPSGVENT|20180630` changed 1.65 → 2.15 on the reasoning
"fund took the FourD context; OneD 2.15 is the quarter". **Backwards.**
`INDAS_48324_135139_04092019040116_WEB.xml` is the **Jun-2019** filing (`ReportingQuarter` "First
quarter"; OneD `DateOfStartOfReportingPeriod` 2019-04-01 / end 2019-06-30). NSE double-indexes that
one file under BOTH the Jun-18 and Jun-19 list rows, so for the **Jun-18** row the correct context
is the FourD *year-ago* comparative (2018-04-01..2018-06-30) = 1.65 — the value the store already
had. 2.15 belongs to Jun-2019, where it is also stored, correctly.

* **The tell is always the context's OWN dates, never its NAME.** OneD is "the current period of
  whatever filing this is", and on a double-indexed row that filing is the NEXT one.
* Both contexts EPS-reconcile (0.81→2.15, 0.62→1.65 on 2.651e7 sh), so **EPS cannot arbitrate a
  double-index** — it validates arithmetic, not period. Only the dates do (memory: "an anchor that
  validates one field does NOT validate another").
* **A retraction must un-pin every artefact, not just the payload.** Three separate things held the
  wrong 2.15: `stdpat_adjud_verdicts.fund_fix`, `pat_defects.json`, `stdpat_mirror_heals.json`. Left
  alone, a re-run of `_stdpat_apply.py` re-applies it (its guard accepts `was`) and a
  `verify_fills_live --repair` re-lands the mirror. The verdict is now moved to a `retracted`
  section carrying `do_not_reapply`.

### 77c. What the documents said

| cell | flagged as | verdict | proof |
|---|---|---|---|
| KOHINOOR Mar-25 375.64 / Mar-26 87.65 | out of family ×100 | **CORRECT** | detres 512559: Exceptional Item 3,827.4 / 959.8 Rs-mn; both FY identities close on the `.50` audited annual (364.77, 80.68) |
| RPSGVENT Sep-18 58.46 | wrong-context grab | **CORRECT** | 9M 63.25 − 1.65 − 3.14; FY 65.77 (2 sources); implied EPS 22.05 = 23.86−0.62−1.19 |
| RPSGVENT Jun-18 2.15 | *(not flagged)* | **DEFECT → 1.65** | §77b |
| METROPOLIS Sep-18 20.05 | FY19 off by +9.6 | **CORRECT** | Sep-19 PDF col3 = 2,004.50 lakh, printed |
| METROPOLIS Dec-18 32.40 | *(not flagged)* | **DEFECT → 22.80** | Dec-19 PDF col3 = 2,280.23 lakh; 9M and FY both close EXACT |
| HALDER Mar-26 con 36.82 | con EPS fails | **DEFECT → 16.19** | 36.82 is H2 *owners*; corrected 12-Jun filing gives Q4 owners 161,854,000 |
| HALDER Dec-25 con 20.78 | *(not flagged)* | **DEFECT → 20.64** | slot held the TOTAL; PDF prints owners 2,063.68 + NCI 14.59 |
| DBL Dec-25 con 829.85 | > filing total | **CORRECT** | owners 8,298,518,000 + NCI **−408,683,000** == total 7,889,835,000 |

### 77d. Three re-usable rules this produced

1. **`owners > total` is NOT a defect signature — negative NCI is ordinary.** DBL Dec-25
   (NCI −40.87cr) and METROPOLIS Sep-18 con (NCI −47.36 lakh) both look impossible and are both
   correct. Test `owners + NCI == total`, never `owners <= total`.
2. **A quarter whose XBRL carries NO attributable tags silently stores the TOTAL in the con slot.**
   HALDER Dec-25's con XBRL has neither owners nor NCI tags, so ingestion took
   `ProfitLossForPeriod`. The filing's **PDF prints the split** even when its XBRL omits it.
   Detector: con cells whose filing has no attributable tags *while the neighbouring quarters do*.
3. **EPS is not universally an arbiter.** It fails on a double-index (both contexts reconcile),
   on filers who compute con EPS on TOTAL rather than owners (DBL 48.57 = 788.98/16.24cr exactly;
   also SAGCEM §2d), and on any company whose paid-up capital moved mid-year so the printed EPS is
   on weighted-average shares (HALDER: 115,973,000 → 116,154,000 → 124,381,000 within FY26). When
   EPS disagrees, establish WHICH of those it is before letting it overrule an identity.

### 77e. The Mar-2019 announce-date cluster — and why the ledger alone was not enough

The double-indexed Mar-2019 filings also carry the *next* quarter's date in `annStd`. Measured
from the BSE announcement archive (Result category, window Apr–Jul 2019) and healed:
GSFC 20190904→**20190522**, POLYMED →**20190510**, CARERATING →**20190521**, plus the same
fingerprint on EMKAY 20190813→**20190528** and GREAVESCOT 20190911→**20190502**.

* `backfill_ann_dates_bse.py` is **fill-only on `ann == 0`** — it can never correct a wrong
  non-zero date. `ann_date_fills.json` got the entries for provenance/durability, but the actual
  repair had to be a §2b guard-edit.
* CARERATING filed the same audited results **twice** (05-21 "Outcome of Board Meeting" enclosing
  them, 05-22 the formal "Results" filing). Point-in-time takes the FIRST dissemination.
* GREAVESCOT's stored 20190911 corresponds to **no BSE filing of any category** — measured, not
  inferred. That is what distinguishes "wrong date" from "date we haven't found".
* **Left UNKNOWN on purpose:** the *con* dates of the same cluster are demonstrably wrong too
  (GREAVESCOT annCon 20190911; CARERATING annCon 20190525 matches neither of its two filings), but
  whether each board released the consolidated statement in the same filing is not established by
  the announcement metadata, so no con date was written. Recorded under `observed_not_healed`.

### 77f. Do not pin a mirror that does not exist

`stdpat_mirror_heals.json` was first given a `METROPOLIS|20181231` entry — and `verify_fills_live`
immediately reported it MISSING forever. That quarter has **no `sf_revop` row at all** (pre-IPO
comparative, never filed, so `build_revop` has nothing to build from), and `--repair` only refills
rows that already exist. Note also that `scripts/revop_fundamentals.json` is **build_revop's own
OUTPUT, not an input ledger** — synthesising a row there is transient noise no rebuild reproduces.
A cell with no served mirror is pinned fund-side in `pat_defects.json` (watched on idx1) and
nowhere else; the verdicts file carries `no_mirror: true` to say so out loud.

Durability wiring used: `pat_defects.json` (fund std idx1 + con idx3, both watched),
`stdpat_mirror_heals.json` (revop patS idx4), `owners_basis_heals.json` (**required** for HALDER
Mar-26 — `_reattr_owners.json` still holds the refuted 36.82 and `apply_owners_full` would restore
it nightly, §71d). `verify_fills_live` exit 0, MISSING 0, and the 7 DRIFT lines were confirmed
**pre-existing** by re-running the verifier against pristine `origin/main` content (identical 7).

### 77g. Pushing this: the minified-payload rebase, done the §2b way

The first push attempt rebased onto ~10 CI commits and conflicted on every minified single-line
payload at once (`sf_fundamentals`, `sf_revop`, `fundamentals`, `revop_fundamentals`,
`pat_defects`, `ann_date_fills`, `stdpat_adjud_verdicts`) — §2b predicts exactly this. The fix is
NOT to resolve those conflicts: `git rebase --abort`, `git reset --hard origin/main`, keep only the
NEW files aside, and **re-run the guarded applier against the fresh payloads**. The guard is what
makes this safe — if CI had meanwhile touched one of the 24 target cells, the re-run aborts instead
of silently reasserting a stale number. It also caught that another session had taken §76 in the
meantime, so this section renumbered to §77 — something a git merge would have carried through
wrong. Journal dumps keep `indent=1`, the repo's `\uXXXX` escaping, the file's own trailing-newline
habit, and **never `sort_keys`** (re-sorting rewrote 260/246 lines of `owners_basis_heals.json` for
a two-entry addition and buried the actual change).


---

## 76. ★★★ THE BANK PDF ROUTE WAS DISABLED BY CONSTRUCTION — and the cross-basis gate that was missing  (2026-08-10)

**NO ASSUMPTIONS, NO GUESSWORK** (§0). Everything below is measured on the filings named.

### 76a. Why every bank cell read "no-pl-page"
`backfill_revop_gaps` qualifies a page with `PL_PAGE` = *revenue|income from operations*, and then
**bails out of anything matching `BANKISH`** (*interest earned | premium earned*). A bank P&L
**never prints the former and always prints the latter** (§42), so for a bank the §58 PDF route is
off by construction and every one of its cells is filed under a reason — `no-anchor-or-scanned`,
or `no-pl-page` in §75e's finer diagnosis — that reads exactly like *"this filing has no
statement"*. It is not. Measured: BANKBARODA's Mar-2019 filing (BSE ann 2019-05-22) carries a
consolidated **Interest Earned** page, and its Jun-2019 filing carries another with Mar-2019 as a
comparative. **Banks need different ROWS, not refusal.** Fixed in
`fill2020_tools/deoverlay_rev_reader.py` (bank branch) and in `diag_rev2019.py`, which now counts
those pages as bank-format instead of implying the statement is absent.

Bank branch, and what each piece is for:
* `rev` = the **Interest Earned** row (what `build_revop`'s bank branch stores as revenue).
* Second check = the statement's own printed **`Total Income (1+2) = Interest Earned + Other
  Income`**. BOB Mar-2019: 1,419,142 + 257,835 = 1,676,977 exactly, in lakh as declared.
* `op`/`ebit` are **not** computed: `metrics_at` reconstructs PBET + finance costs + depreciation −
  other income, which is meaningless for a bank (its "finance cost" IS interest expended, and the
  RBI format prints no PBET). This reader writes revenue only anyway.
* Labels are matched with **search, not match**: `merge_wrapped` prepends the numeric-less
  audit-status line, so BOB's row arrives as *"Reviewed Audited Un-audited Audited 1 Interest
  earned (a)+(b)+(c)+(d) 1972331 …"* and an anchored `^match` finds nothing on a page that plainly
  has the row.
* The consolidated bottom line is spread across merged lines, so the PAT row is found **by VALUE at
  the date-fixed column** (§55c's precedent for unreadable labels), taking the **CLOSEST** match —
  see 76c.

### 76b. ★ THE CROSS-BASIS GATE — a wrong value caught before it was written
CORPBANK 2019-03 read consolidated revenue **3,643.37, exactly its stored STANDALONE revenue**
(con/std 1.0000). The tell: its anchored PAT −6,581.49 is our stored **standalone** PAT to the
paisa, while consolidated is −6,574.74. These 2019 filings routinely print both statements under
one *"Standalone and Consolidated"* heading, so the page qualifies for either basis and
`date_column` takes the **leftmost** column carrying the target date — the standalone one. A PAT
anchor **cannot separate two bases that sit 0.1% apart**; `close()`'s 0.4% band admits both.

    GATE: the anchored PAT must match the TARGET basis STRICTLY BETTER than the other basis.
          d(other) <= d(target)  ->  refuse: "this column is not provably <basis>"

This is §44's duplicate trap and §55's *"refuse a consolidated figure that comes from the page which
just served as the standalone control"*, finally expressed as code. Re-run with it armed: 6 cells
land, CORPBANK 2019-03 refused. **Refusals of this kind are STICKY** — a later page must not
overwrite an adjudication result with a vaguer message, or the ledger ends up reporting
"rev/PAT row unparsed" for a cell whose real story is that the only readable column was the other
basis.

**Screen your own landed cells with it.** `con == std to the paisa` is the fingerprint; run it over
anything a PAT-anchored reader produced. Retro-checked over the 21 cells this campaign had already
pushed: no corrections needed, and the four that tripped the screen each had a documented reason —
GUJENERGY/KTKBANK are E1-E5 identity fills (con := std is their semantics), MOIL's con revenue
equals standalone in **all 11** stored neighbouring quarters (associate equity-accounted, con PAT
differs by exactly the pickup), PETRONET has 5.4% PAT separation, and ATGL came from XBRL with a
**declared** basis rather than an inferred one.

### 76c. Closest-match, not first-match, when value-anchoring
A consolidated bank statement prints two profit lines a hair apart: BOB Mar-2019 shows *Net Profit
from Ordinary Activities after tax* **−817.49** and, after minority interest and share of
associates, **−820.59** — the owners figure we store. `close()`'s 0.4% band admits BOTH, and
first-match took the pre-minority one. That is the §2d total-vs-owners confusion **arriving through
a tolerance instead of a label**. Take the closest match, always.

### 76d. Landed on these rules (all fill-only, revenue slot only)
ALBK 2019-06 4,335.53 · ALBK 2019-09 4,124.62 · ANDHRABANK 2019-09 5,030.45 · BANKBARODA 2019-03
14,191.42 · CORPBANK 2019-09 4,009.09 · RBLBANK 2019-09 2,190.45 (con/std 1.030 against neighbours
1.030/1.031/1.033) · NIACL 2019-12 8,406.07 (§55 insurer route). Ledger
`scripts/deoverlay_rev_fills2019.json`; every entry carries the printed header dates, the anchor
chain and the identity check.

### 76e. A ledger verdict must be able to STOP an apply
`insurer_con_rev.py --apply` re-applied its whole ledger and had **no notion of a held entry**, so
the §55b con/std ratio-family verdict — the test that once rejected a LICI read which had already
passed the PAT anchor — was advisory: the value landed anyway. `--apply` now skips entries carrying
`held`, and the reason travels with the entry so the cell is re-adjudicated rather than re-read.
First use: **NIACL 2019-06** implies con/std 1.0068 against NIACL's **measured** family
1.0020–1.0061 over 21 stored quarters, so it is held — even though its PAT anchor (279.59) and its
per-filing standalone control (6,888.04 == stored revS) are both exact.
Same principle in the reader: land only when the anchored PAT reproduces the stored one to ~the
paisa **or** the page's own total-income identity holds. RAMCOCEM 2019-03 (0.28% off, no Total
Income row, and the value would sit BELOW that quarter's standalone while every other RAMCOCEM
quarter runs con ABOVE std) is held on exactly that rule.

---

## 78. ★★★ LANCER — ONE SERIES, FIVE DEFECT CLASSES, and why §74's detector never saw it  (2026-08-10)

**NO ASSUMPTIONS, NO GUESSWORK — every value below traces to a document read this session.**

LANCER (Lancer Container Lines, **BSE-only**, scrip **539841**, ISIN **INE359U01028** — resolved and
gate-checked per §76, not assumed from the ticker match) was opened for three PAT cells storing raw
RUPEES in the crore slot. Re-adjudicating the whole series found **16 wrong slot-values across 10
quarters in five distinct classes** — the §45/§74 "a detector hit is a thread, not a cell" rule again.

| class | cells | stored → true |
|---|---|---|
| raw ₹ never ÷1e7 (k=7) | 20180331, 20181231, 20190331 (both slots) | 21816965→2.18, 34903220→3.49, 5789416→0.58 |
| ₹-lakh print as crore (k=2, §74's class) | 20221231 std, 20250930 std | 903.43→9.03, 506.82→5.07 |
| con slot holds a COPY of std | 20210630, 20220630, 20230331 | 2.54→2.97, 11.86→13.29, 6.87→10.97 |
| FY **annual** landed in a quarter slot | 20240630 std+con | 1.84→3.34, −0.35→12.06 |
| wrong COLUMN of a LATER filing | 20240930 std+con | 202.27→2.92, 215.46→15.91 |
| adjacent-quarter duplicate | 20250630 std+con | −1.69→−3.63, −32.44→−4.62 |

### 78a. Why the §74 detector missed all of it
§74 flags a triple whose mid/neighbour ratio sits in **[9,11] or [90,110] on BOTH sides**. LANCER's
×100 cell (Dec-22, 903.43) has neighbours 15.86 and 6.87 → ratios **57.0 and 131.5**, both outside
the band, because this filer's own quarters swing ±60%. The ×1e7 cells are ~10^6 out and clear every
band. **A fixed ratio window only catches scale steps on a FLAT series.** If a symbol surfaces any
other way, re-scan it by hand; don't treat "not in the 128" as clean.

### 78b. The route that worked (all four are free, in this order)
1. **§42 detres** `Corp_detailedResult_Transpose_ng` — reached 2016→2026 for this scrip and gave a
   positive control on every *clean* cell (stored == NP÷10 to 2 dp), which is what made the corrupt
   ones unambiguous. ⚠️ its `Net Profit` and `Net Profit … from Ordinary Activities after Tax` rows
   **disagree** on tax-credit quarters (Mar-25 −16.86 vs −36.04 = PBT); `Net Profit` is the PAT row.
2. **BSE announcement PDFs** (§58) — the primary record and the only source of the **consolidated**
   basis, which detres does not serve at all.
3. **Comparative columns of the NEXT filings** (§57 rung 6) — decisive here: **pre-2019 BSE
   attachments 404 on both AttachHis and AttachLive**, so the Dec-2017 and Mar-2018 filings are
   unfetchable, yet both values are printed verbatim in the Feb-2019 and May-2019 filings.
4. **FY quarter-sum identity** (§45) as the closing check: FY18 4,66,85,868 + 2,18,16,965 =
   6,85,02,833 and FY19 1.687+2.466+3.490+0.579 = 8.222, both EXACT against the audited annuals.

### 78c. Traps this series adds to the list
* **A half-yearly SME filer has no Q3/Q4 of its own.** The 2018-06-05 filing is headed *"for the
  Half and Year Ended 31st March, 2018"*. Such quarters exist ONLY as later filings' comparatives —
  an empty own-quarter route is the filing calendar, not a gap (§57a).
* **A truncated Indian-grouped number is not a scale error.** Dec-2017 stored **152.0**, which is
  the last comma group of `1,36,08,152`. No power of ten maps 152 → 1.36, so it cannot go in
  `scale_fix.json`; that ledger is for 10^k only.
* **The later filing can be the wrong witness.** The Sep-2025 filing prints its own Sep-2024
  standalone comparative as **202.27** while the Sep-2024 filing, that same page's H1-FY25 total
  (626.74 − 334.48 = 292.26) and detres all say **292.27**. Anchor on the OWN-quarter filing and let
  the arithmetic arbitrate; and 215.46 in the con slot was never a Sep-2024 figure at all — it is
  that same later page's **Half Year Ended** column, i.e. a column-index slip (§58 step 6).
* **Two stored cells equal to a later year's ANNUAL pair** (1.84 = FY25 std 184.11 lakh, −0.35 =
  FY25 con −34.77 lakh) is a signature, not a coincidence — check the annual columns whenever a
  quarter pair looks jointly implausible.

### 78d. `scripts/pat_defect_fix.py` — the pat_defects applier is now IN-REPO
`pat_defects.json` has been tracked since §45 but its applier `_pat_defect_fix.py` only ever existed
in the rev-mission worktree, which is gone — the ledger was a journal that could not be replayed
(exactly the trap in `feedback-reset-replay-hits-tracked-scripts`). The replacement is tracked,
dry-run by default, `--only SYM`-scoped, and guarded on the recorded `stored_pat`/`stored_pat_con`
so it is idempotent and can never clobber a value some other lane has since corrected. It writes
BOTH `docs/sf_fundamentals.json` and `scripts/fundamentals.json` (one quantity, two files).
Ledger entries for this heal were generated by `scripts/lancer_ledger_entries.py`, which preserves
each ledger's own dump style (scale_fix writes literal non-ASCII, pat_defects escapes it,
ann_date_fills is one minified line) and **appends rather than re-sorts** — the §77 lesson.

**Not attempted here (gaps, not defects — reported, never guessed):** LANCER has no row at all for
20180630 (detres std 1.69), 20200630/0930/1231 (2.19/2.26/2.33), 20210930 (5.38 — the own filing's
OCR prints 637.6 but its H1 791.06 − Q1 253.53 forces 537.5x), and no std for 20210331 (2.00) or
20211231 (8.01). Filling those is a backfill with its own anchor rules, not part of this heal.

---

## 79. ★★★ A BSE-FIRST SCRIP WITH TWO STORED QUARTERS — HALDER, and the H2-as-quarter poison §77 half-healed  (2026-08-10)

**NO ASSUMPTIONS, NO GUESSWORK — every value below was read off a primary document this session.**

HALDER (Halder Venture Ltd, BSE 539854) held exactly **two** quarters in `sf_fundamentals` for its
entire history — 20251231 and 20260331 — because it is BSE-first: `api/corporates-financial-results`
returns ZERO rows for it, and `api/integrated-filing-results` has it for those two quarters only.
Verdicts + per-cell anchor chains: `scripts/halder_series_verdicts.json`; applier
`scripts/_halder_apply.py` (§2b guard-edit + blast radius + all four twins).

### 79a. What landed
* **FILL 20250630 / 20250930, both bases + revenue.** std 3.93 / 1.06, con-owners 2.83 / **−10.91**,
  revS 106.72 / 52.17, revC 103.21 / 97.82; ann 20250813 / 20251114.
* **FIX 20260331 — six revop slots** that all held the **H2 (Oct–Mar)** figure, not the quarter:
  revS 291.79→218.16, revC 445.17→299.91, opS 7.99→6.32, opC 31.56→20.26, ebitS 4.54→4.24,
  ebitC 28.06→18.15. (PAT was already right — §77 healed it.)

### 79b. ★ A HALF-HEALED DEFECT IS THE EASIEST ONE TO MISS
§77 diagnosed HALDER's Mar-2026 con PAT as "the H2 owners figure stored as the quarter" (the
29-May-2026 XBRL's OneD **declares** an Oct–Mar period) and healed **the PAT slot**. The same file
fed `build_revop`, so **revenue and both operating-profit slots carried the identical poison and
were left in place.** Nothing flagged them: the PAT anchor passed, so the row looked healed.
> **When a filing declares the wrong PERIOD, every metric that filing feeds is wrong — heal the ROW,
> not the metric you happened to be auditing** (the §67 lesson, one layer out). After healing a
> period-declaration defect, re-derive **every** slot the same document populates.

The proof needs no judgement, because each stored value decomposes exactly against the
already-correct neighbouring quarter: `Dec-2025(stored) + Mar-2026(printed) == stored Mar-2026`
holds for **all six** slots — 73.63+218.16=291.79, 145.26+299.91=445.17, 1.68+6.32≈7.99,
11.30+20.26=31.56, 0.30+4.24=4.54, 9.91+18.15=28.06. Six independent confirmations of one mechanism.
`build_revop`'s op formula (`PBEIT + finance + depreciation − other income`, `ebit = op − dep`) was
validated on the stored Dec-2025 cells first (167.50 lakh == 1.68; 1,130.05 == 11.30) before being
trusted for Mar-2026.

### 79c. detres reach, and the gate that CANNOT arbitrate this scrip
Walking `Corp_detailedResult_Transpose_ng` over qids 81.00–132.00: **41 quarters, 20151231 →
20260331**; 81.00–87.00 (Mar-2014 … Sep-2015) return an EMPTY `table1`. That blank is corroborated,
not inferred (§63): the BSE **announcement** stream's earliest result filing for this scrip is
2016-05-03, for the **same** Dec-2015 quarter. Two independent indexes agree the history starts there.

Then the §45 FY quarter-sum gate, run separately on PAT and on revenue:

| FY | PAT Σq vs `.50` | rev Σq vs `.50` | verdict |
|---|---|---|---|
| 2016 | −0.03 | −19.16 | incomplete — pre-filing-history |
| 2017 | **0.00** | 0.00 (1 of 4 values) | PAT gate passes exact |
| 2018–2024 | −0.87 … +3.48 | **0.00 / ±0.01** | PAT fails, revenue EXACT — **filer-side** |
| 2025 | −36.25 | −1756.05 | **merger restatement** — refuse (§45) |
| 2026 | **+0.01** | **0.00** | gate passes → the two landed cells are gated |

* **FY2025 is a restatement, measured not guessed:** the Nov-2025 filing reprints Sep-2024 standalone
  revenue as 12,903.54 lakh against the 6,176.00 originally filed. The `.50` annual is
  post-merger-restated; the quarters are as-filed. Not comparable by design.
* **★ FY2018–FY2024: the identity fails INSIDE the filing.** Revenue reconciles to the paisa while
  PAT does not — so the quarters are the right entity, quarter and scale, and the residual is the
  company's. The FY2023 annual PDF (`caf8cca8…`, p10) prints, on ONE page: std Q4 NP **86.68**,
  Q3 **32.30**, FY23 **54.81**, FY22 **83.69** lakh. Its own four published quarters sum to 89.58
  against its own audited 54.81, while revenue on the same page sums exact. Every one of those four
  figures equals detres to the last digit it carries — **detres reproduces the filing; it is not the
  source of the residual.**
> **A failing FY identity is not automatically OUR defect.** Before concluding the stored series is
> wrong, check whether the ANNUAL and the QUARTERS disagree *in the filer's own document*. Run the
> gate on revenue as well as PAT: revenue reconciling while PAT does not localises the residual to
> the profit line and rules out wrong-entity / wrong-scale / wrong-period reads in one step.

### 79d. Reading a scanned filer — what the OCR text layer got wrong
Every HALDER PDF is a 10–17 MB scan whose text layer is legible-looking and **wrong** (§75). On the
Q1-FY26 consolidated page it misread NCI `3.21`→"3.27" and owners `399.17`→"399.77" — each a single
digit, each enough to break `owners + NCI == total`. Both were caught by that identity and settled
by **rendering the row band at 400 dpi and reading it** (`crop.py` → page y-fraction → PNG). After
the render all four columns reconciled exactly.
* **The owners/NCI/total identity is the cheap detector here** — it fired on exactly the two columns
  the OCR had corrupted and stayed silent on the two it had not.
* Con owners came out **+282.66 lakh (Jun-25) and −1,091.05 (Sep-25)** — opposite signs, so neither
  H1 quarter was derivable from the FY chain and both had to be read. Four independent routes agree
  on their sum: printed H1 −808.39; 9M 1,255.30 − Q3 2,063.68 = −808.38; FY26 2,873.84 − Q3 − Q4 =
  −808.38; and Q1+Q2 of the printed quarters themselves.
* **EPS could not arbitrate**: this filer computes consolidated EPS on the **total**, not owners
  (Jun-25: 7.39 × 38.658 lakh sh == 285.87 total, not 282.66 owners) — §77d's third case, live.

### 79e. Guarding what was landed
`named_pat_cell_fills.json` is **new** — the PAT counterpart of `named_rev_cell_fills.json`, for
hand-read cells no quarter-keyed index serves. Both slots are registered in `verify_fills_live.py`.
Registering it exposed that `named_rev_cell_fills.json`'s **`revC` was never registered**: 15
hand-read consolidated-revenue cells (AIIL, BALKRISIND, CGPOWER, CYIENT, INDUSINDBK ×4, KNRCON,
MCX ×2, NMDC, SWANCORP, TIMKEN, WAAREEENER) had nothing re-checking them after a refresh. All 15
verified present, now guarded.
> **When you add a ledger, grep the verifier for every SLOT that ledger can carry.** A file listed
> once guards one slot and silently ignores the rest.

`op`/`ebit` are deliberately left **null** on the two new rows: operating profit is a reconstruction
and a wrong OPM is a visible site bug (§2c). They are written for Mar-2026 only because those slots
already held a proven-wrong non-null value.

### 79f. STILL OPEN — 37 reachable quarters, deliberately not landed
20151231 … 20250331 are all served by detres, but §42's landing rule needs an EPS reconstruction or
the FY gate, and HALDER has **neither** (no EPS/Equity/FaceValue rows after FY2016; gate fails
filer-side per 78c). Landing them honestly means the §58 route — the printed column in ~30 scanned
PDFs, each needing a render. The announcement index for the whole span is already harvested and each
**annual** filing prints Q4 + Q3 + year-ago-Q4 + FY + prior-FY, so ~3 quarters land per PDF read.
FY2023 is done as the worked example (Mar-23 std 86.68 lakh, Dec-22 32.30 — printed, both matching
detres exactly). BSE also published two **"Discrepancies In Financial Results"** notices for this
scrip (2024-09-05, 2024-09-19) — read those before trusting anything in the FY2024/FY2025 window.

---

## 80. ★★★ SERIES **BZ** WAS NEVER INGESTED — a live trading series thrown away for years  (2026-08-10)

`build_sf_data.parse_rows` kept `("EQ","BE")` and dropped everything else. **BZ is not "everything
else."** NSE's equity cash segment is three series, and all three are ordinary listed companies
trading every session:

| series | what it is | in the bin before 2026-08-10? |
|---|---|---|
| EQ | normal rolling settlement | yes |
| BE | trade-for-trade (compulsory delivery) | yes |
| **BZ** | **trade-for-trade + surveillance** — the company has not complied with a listing/regulatory requirement | **NO** |

Everything else in the bhavcopy genuinely is another instrument (SM/ST = SME platform, GS/GB/TB =
govt securities and bonds, IV = InvIT, N\*/Y\*/Z\* = debt, RR/E1/MF = rights entitlements, ETFs) and
stays out. **BZ was the only live equity series being discarded.**

### 80a. What the gap actually did — three defects, all measured

**(1) A stock's series STOPS the day it is penalised into BZ.** Measured 2026-08-10 against NSE's
live `EQUITY_L.csv` (2,411 rows) and the release-asset bin (`end` 2026-08-07):

```
EQ  2,086 symbols — 2,086 fresh (last bar within 60d),  0 stale
BE    285 symbols —   284 fresh,                        0 stale, 1 absent
BZ     39 symbols —     1 fresh,                       38 STALE
```

The one fresh BZ name, ASTRON, is the mechanism caught in the act: it still traded **BE** on
2026-08-07 and appears in `EQUITY_L` as BZ on 2026-08-10 — i.e. its series was about to stop.

**(2) Mid-series HOLES for stocks promoted back out of BZ.** UNITECH went BZ 2020-03 → 2025-10 and
traded **₹16.1 cr on a sampled day inside the hole** (2024-06-14, verified in BOTH the
`sec_bhavdata_full` CSV and the legacy `cm14JUN2024bhav.csv.zip` — they agree exactly: EQ 1,928 /
BE 234 / BZ 21).

**(3) PHANTOM corporate actions, caused by (2).** When a stock was promoted back, the daily append
saw one enormous ratio across the invisible hole and `ca_factor()` divided it out as a split. **21
blocks over 18 symbols** carry a corporate action that never happened (ATLASCYCLE ×2/3, KERNEX ×2/3,
RAJRAYON ×1/3, SUPREMEINF ×6 and ×3, TIL ×2 twice, JYOTISTRUC ×5/6, TAKE ×3/5 …), each confirmed
against NSE's official corporate-action feed as having **no action at all** on that date. Every one
of their pre-hole bars is mis-scaled.

Total measured over the 2016 → 2026-08-07 window: **61,306 bars missing across 249 symbols**
(271 blocks). The 38 stale names are the visible tip; the hole class is far bigger.

### 80b. The decision: INGEST IT. Nothing downstream assumes EQ/BE

The bin carries **no series field at all** — `meta` is `{name, ind, alive, raw, isin}` — so no
consumer can distinguish EQ from BE today, and BE is *already* trade-for-trade with the same
`DELIV_*` convention. BZ therefore introduces **zero** new semantic assumptions; it is the BE
precedent exactly. Checked:
- **Delivery %.** BE and BZ both print `DELIV_QTY` **and** `DELIV_PER` as `'-'` — that IS the
  compulsory-delivery signature (measured 2026-08-07: all 291 BE and all 27 BZ rows dashed, all
  2,416 EQ rows numeric). So `parse_rows` now applies the existing BE heal to BZ: `dv = 100`.
- **`fetch_delivery.py` (Delivery Spikes, Volume Shockers)** is `SERIES == "EQ"` only and stays
  that way — it needs `DELIV_QTY`, which BZ does not have. It already excludes BE; BZ matches.
- **The ₹1cr/day liquidity screen** (§11) is a pure turnover screen, which is the right instrument
  for "is this tradable". Result below.

### 80c. Does the liquid universe re-admit HDIL / RAJESHEXPO? Measured, not assumed

Ledger applied to the 2026-08-07 bin, then `scan_bin_universe()` run on both:

```
liquid universe BEFORE 1,441   AFTER 1,442
ADDED:   RAJESHEXPO        REMOVED: (none)

of NSE's 39 current BZ symbols, FRESH (last bar within 60d of the bin's end): before 1  ->  after 39
```

- **RAJESHEXPO comes back, legitimately** — median ₹**3.47 cr**/day over its last 250 REAL sessions,
  well over the ₹1 cr floor, on bars ending 2026-08-07.
- **HDIL does NOT** — ₹**0.029 cr**/day (₹2.9 lakh). It fails the turnover floor on its own merits.

So the §11 recency guard was masking two different things: RAJESHEXPO was genuinely liquid and
merely frozen; HDIL is genuinely illiquid. **The guard stays** — it is what protects against the
next feed gap — but it is no longer doing BZ's job. No name is lost by ingesting BZ.

### 80d. Flipping the filter alone would have CORRUPTED data — the backfill is not optional

The updater only appends days after `end`, so it cannot reach backwards; and appending today's BZ
row onto a years-stale series hands `ca_factor()` a multi-year ratio to misread:

```
HDIL        1.57 / 2.20   = 0.714  -> ca_factor snaps to 3/4  -> 3,108 bars silently rescaled
RAJESHEXPO 83.58 / 223.97 = 0.373  -> ca_factor snaps to 2/5  -> 5,822 bars silently rescaled
```

Both phantom. **Ship the filter and the ledger together, never the filter alone.**

### 80e. The tooling

- `scripts/build_bz_backfill.py` — `--scan A B` (fetch bhavcopies, resumable, cache
  `scripts/_bz_scan/` gitignored) → `--anchors` (one file per gap) → `--build` (writes
  `scripts/bz_backfill.json.gz`). Use `--bin <path>` to point at a downloaded release asset.
- `update_sf_data.insert_bz_history()` applies the ledger to the release-asset bin **before** the
  day loop, idempotently (a block whose first bar is already present is skipped whole, so `pre` can
  never be applied twice, and a from-scratch rebuild — which now ingests BZ itself — no-ops).
- `build_sf_data.parse_rows` cache rows gained a 13th column, `series`, which doubles as the
  **cache version marker**: `fetch_day`/`needs_fetch` demand ≥13 columns, so any day cached under
  the old EQ/BE-only filter is refetched instead of being replayed BZ-less.

### 80f. Five traps this hit, worth not repeating

1. **A failed fetch is not "no data".** The first scan ran 8 workers, tripped NSE's 403 lockdown,
   and cached every failure as `[]` — reporting the whole of 2026 as holiday. Only a confirmed
   **404 from both URLs** may be cached as "no session"; everything else stays uncached so a later
   run retries. The `.miss` markers this produced were then checkable, and they were all genuine
   (2016-01-26 Republic Day, 2016-03-07 Mahashivratri, weekends).
2. **Judge adjacency against the BIN's calendar, not your own downloads.** NSE re-serves the prior
   session's file on holidays/weekends, so "every day I fetched" contains Sundays — 279 of them
   here. A phantom 2025-12-25 sat in front of RAJESHEXPO's first BZ bar, broke the adjacency test,
   and dropped the very symbol the exercise started from.
3. **`PREV_CLOSE` is only usable across ONE session.** NSE mis-states it by ~1–6% on random days,
   and a security that stops trading altogether can come back at a re-established price: HDIL's
   first BZ row prints `PREV_CLOSE` 4.30 against a bin close of 2.20. Reading that as "scale
   0.5116" would have invented a corporate action out of a suspension.
4. **Snap a phantom factor to what the code would have produced, not to what you measured.**
   `ca_factor()` only ever returns a `CA_FRACS` member or a product of them, so the correction is an
   exact reciprocal. JYOTISTRUC measures 0.8228 against a baked-in 5/6 = 0.8333 — un-snapped, the
   "fix" would have left the series 1.3% wrong forever.
5. **A thin stock does not trade every session.** Splitting gap blocks on session adjacency
   shattered illiquid names into hundreds of unanchorable one-bar blocks. A block is a maximal run
   of missing bars with **no bin bar between them**.

### 80g. Still open (measured, not hand-waved)

- **6 symbols appear in BZ but have no series in the bin at all** (SPENTEX, INDOSOLAR, BINANIIND,
  8KMILES, EBIXFOREX, WEIZFOREX — 1,567 bars). They need a series CREATED, not spliced; the ledger
  format only splices. Left out deliberately.
- **23 blocks are deliberately NOT shipped**, each with a measured reason printed by `--build`. Most
  are a real corporate action hiding inside the hole, caught by the exit control: SDBL 0.200,
  REFEX 0.200, PVP 0.170, PARASPETRO 0.100, WINSOME 0.150, PBAINFRA 0.344 — all clean face-value
  splits that NSE's official feed does not carry for those dates. Filling them needs the ratio from
  the corporate-action record, not from the price.
- **The ledger covers 2016-01-01 → 2026-08-07.** The `--scan` back to 2002 was still running when
  this shipped; re-run `--scan 2002-01-01 2016-01-01` → `--anchors` → `--build` to extend it. Older
  BZ stints are therefore still missing.
- **A separate, non-BZ hole class exists.** 215 symbols have an internal hole ≥20 sessions since
  2018; a sample shows two causes — BZ (UNITECH, fixed here) and genuine absence from the bhavcopy
  (GOODYEAR/KENNAMET/NOVARTIND/KIRLFER/KOVAI/GRAUWEIL all vanish 2023-10-25 → 2026-04-20 and are in
  **neither** the new nor the legacy file, so it is not our filter). That class is unexplained and
  unfixed — do not assume it is the same defect.
- **The weekend-session ledger predates this fix.** `weekend_sessions.json.gz` rows were captured
  under the EQ/BE filter, so those ~30 special sessions are still missing their BZ bars.


---

## 77. ★★★ I SKIPPED §60 AND MIS-CLASSIFIED 26 CELLS AS "NEVER FILED"  (2026-08-11, USER-CAUGHT)

**The trigger, verbatim:** *"are they not in dhan, trendlyne or any other new age sites?"* — asked
after a 2019 residue report listed 217 cells as unreachable. The honest answer was that the
aggregator rung had **never been walked**. §60f is explicit and was written for exactly this:
*"Before any cell is reported as unfillable, a SECOND INDEPENDENT READER must have been tried and
must also have come back empty."* The 2019 campaign's ladder went NSE XBRL → BSE detres → identity
gates → BSE announcement PDFs → insurer route, and stopped. screener.in was never tried.

**What one run of the existing tool produced.** `screener_annual_sweep.py --from 20190101` derived
**6 cells** on the §60d identity (screener's FY annual − the 3 stored sibling quarters), all gated:
CHOLAHLDNG 2019-06 4,251.69 · DLF 2019-03 2,500.34 · HINDCOPPER 2019-03 450.76 ·
INDUSINDBK 2019-03 5,991.29 · NHPC 2019-03 2,158.54 · TVSSRICHAK 2019-03 595.72.
**DLF had been filed under "needs a vision read"** — its results PDFs are pure image scans, 0
chars/page — and the annual identity walks straight past the unreadable document. A rung that needs
no document at all outranks one that needs an expensive read of a bad one.
Ledger: `scripts/screener_annual_derived_2019.json`, each cell labelled
`precision: crore-rounded` per §60e (screener prints the annual as a crore-rounded integer, so the
derived quarter inherits up to ±0.5 cr; the siblings are at filing precision).

### 77a. ★ THE REAL DAMAGE — "never filed consolidated" inferred from a HOLE IN THE INDEX
Two of the six (INDUSINDBK, NHPC) were sitting in `no_con_quarterly_2019.json` as
**not-applicable, no consolidated quarterly ever filed**. They were not. The E1+E2+E3 gate had used
**the first consolidated row in the NSE index** as "the company's first consolidated filing ever" —
and for these symbols the index carries **no consolidated rows at all before Jun-2019**, the very
quarter consolidated quarterlies became compulsory (§51a). E3 therefore passed vacuously on an
index that simply does not go back that far.

**The refutation test is one line, costs nothing, and must be part of the gate:**

    if we ALREADY STORE consolidated revenue or consolidated PAT for ANY EARLIER quarter
    of the same company, then E3 is FALSE — the cell is a real gap, not a never-filed quarter.

Applied to the 61-cell ledger it **refuted 26**, including HDFCBANK, AXISBANK, INDUSINDBK, RBLBANK,
CENTRALBK, CORPBANK, MAHABANK, INDIANB, WHIRLPOOL, FRETAIL and TRENT — companies that self-evidently
publish consolidated accounts. 35 entries stand. The refuted 26 are kept in the same file under
`refuted_2026_08_11` with the reason, rather than deleted, so the mistake stays visible.

This is `feedback-never-infer-absence-from-own-gaps` wearing a new coat: the earlier version
inferred absence from OUR gaps, this one from the INDEX's gap. Same error, same rule — §57's
"a route returning nothing means THAT ROUTE has no row" applies to the index as much as to a reader.

### 77b. The order the ladder should actually run in
Put the **document-free** rungs BEFORE the expensive document ones. Deriving a quarter from a
published annual needs no PDF, no OCR and no column anchor, so it should be attempted before the
vision rung is even costed — and before any cell is described as blocked on document quality.

---

## 81. ★★★ THE AGGREGATOR ROUTE — Moneycontrol / Trendlyne / Tickertape, discovered and MEASURED  (2026-08-11)

Sibling of §60 (screener.in). Same rule, three more readers: **before a cell is reported
unreachable, a second independent reader must also have come back empty** — and the reader must be
a PARSER, never a prose summary (§60b is absolute).

**★ NO ASSUMPTIONS, NO GUESSWORK (§0).** Every endpoint, parameter, row label, reach number and
tolerance below was measured on 2026-08-11. Where something was not measured it says so.

### 81a. How the endpoints were found — the method, because guessing failed first

Guessed URLs failed three times before any of this worked: tickertape
`/stocks/financials/income/<sid>/quarterly/normal` → **400** (the segment is `interim`, not
`quarterly`), trendlyne's quarterly-results page → the table is **absent from the served HTML**,
moneycontrol → **blocked** to a plain fetch. The method that worked is the one in memory
`feedback-find-endpoints-in-js-bundle` (the StockEdge/BSE-Angular precedent):

1. Load the page in the Browser pane (`preview_start` / `navigate`).
2. **Read `performance.getEntriesByType('resource')` and filter `initiatorType` to
   `fetch`/`xmlhttprequest`.** ← the step that actually mattered. `read_network_requests` returned
   only the document and its sub-resources (CSS, chunks, images) and **no XHR at all**, which looks
   exactly like "the page makes no API call". It does. The Performance API sees them, and it sees
   them retroactively, so nothing has to be hooked before load.
   (A `window.fetch` + `XMLHttpRequest.prototype.open` monkey-patch also works but only catches
   calls made AFTER it is installed — useless for data fetched during page load.)
3. For the request SHAPE, read the JS bundle: tickertape's route table is a literal in a Next.js
   chunk — `GET_STOCK_FINANCIAL_STATEMENT:"/stocks/financials/:statement/:sid/:period/:view"`.
4. For per-company IDs prefer the site's own **sitemap** over its search API: it is a complete,
   cheap, stable map and it needs no calls per company.

### 81b. The three endpoint specs (all verified answering 200 with real rows)

**MONEYCONTROL** — the deepest of the three by a wide margin. No cookies, no Referer, no token.
```
id      GET https://www.moneycontrol.com/mccode/common/autosuggestion_solr.php
            ?classic=true&query=<NSE SYMBOL>&type=1&format=json
        pdt_dis_nm carries "<ISIN>, <NSE SYMBOL>, <BSE code>" -> accept ONLY the row whose SYMBOL
        token equals ours (MOIL's top two hits are IOC and Oil India).
quarters GET https://appfeeds.moneycontrol.com/jsonapi/stocks/quarterly_results_responsive
            ?sc_id=<sc_id>&type_format=<quarterly|cons_quarterly>&start=0&limit=200
annual   GET https://appfeeds.moneycontrol.com/jsonapi/stocks/yearly_results_responsive
            ?sc_id=<sc_id>&type_format=<yearly|cons_yearly>&start=0&limit=200
```
`type_format=quarterly` is **STANDALONE**; `cons_quarterly` is CONSOLIDATED. Period key `yrc0`
prints `"Jun '26"`. Rows are label→string: `Net Sales/Income from operations`,
`Total Income From Operations`, bank layout `Interest Earned`, PAT `Net Profit/(Loss) For the
Period`, consolidated owners-attributable `Net P/L After M.I & Associates`.

⚠️ **TWO ids come back and they are not interchangeable.** The `sc_id` FIELD is the feed key; the
code at the end of `link_src` is a legacy/SEO code that answers the feed **0 rows with HTTP 200**
(§61a mode 4 — absence dressed as success). Measured: SPICEJET link `SJ01` → 0 rows, `sc_id`
`ML04` → 73; MOIL `M18` → 0, `M11` → 67; ALOKINDS `AI54` → 0, `ATI` → 74. WESTLIFE has both equal
to `DIC`, which is exactly how a wrong preference survives a one-company smoke test.

**TRENDLYNE** — both bases in one payload; needs a session.
```
ids     GET https://trendlyne.com/fundamental-sitemap-quarter-result.xml
        -> /fundamentals/financials/<tid>/<NSE SYMBOL>/<slug>/   (5,459 symbols, keyed by symbol)
page    GET https://trendlyne.com/fundamentals/financials/<tid>/<SYM>/<slug>/
        -> <main id="fundamental_tables" data-tablesurl="...">
api     GET https://trendlyne.com/fundamentals/get-fundamental_results-v2/<tid>/<BASE32 TOKEN>/
        headers: Cookie csrftoken (minted by the page GET) + X-Requested-With: XMLHttpRequest
                 + Referer: <the page URL>
```
Without the session + those two headers the host answers **HTTP 444** and closes — that 444 is the
"blocked to a plain fetch" symptom, not absence. The token is printed in the page HTML and is
stable across sessions (same value from curl and from the browser). Body:
`body.quarterlyDataDump.{standalone,consolidated}["Jun 2026"]` with `SR_Q` (revenue from
operations incl. other operating income), `TOTAL_SR_Q` (incl. other income), `NP_Q`,
`NetPLAfterMIAssociates_Q`; `body.annualDataDump` the same shape.
**Pacing: `trendlyne.com/robots.txt` sets `Crawl-delay: 10` for ClaudeBot BY NAME** — honoured, as
the repo already does in `fetch_shp_seam_trendlyne.py`. Its per-indicator chart endpoint
(`data-fetchurl` → `*/fundamentals/chart/v2/`) is **Disallowed in robots.txt and is not used.**

**TICKERTAPE** — discovered in full, and deliberately read the slow way.
```
API (NOT USED)  GET https://api.tickertape.in/stocks/financials/:statement/:sid/:period/:view?count=N
                statement=income|balancesheet|cashflow  period=interim|annual  view=normal|margin|growth
                GET https://api.tickertape.in/search?text=<SYM>&types=stock  -> data.stocks[].sid
```
⚠️ **`api.tickertape.in/robots.txt` is `User-agent: * / Disallow: /`** (measured). The spec is
recorded because discovering it was the job; the reader does **not** fetch that host. The route it
uses instead is the page, which robots permits:
```
ids   GET https://www.tickertape.in/sitemaps/stocks/sitemap.xml   -> /stocks/<slug>-<SID> (5,494)
page  GET https://www.tickertape.in/stocks/<slug>-<SID>
      -> __NEXT_DATA__.props.pageProps["income-normal-interim"|"income-normal-annual"]
      rows {displayPeriod, endDate, reporting, qIncTrev, qIncNinc, ...}
```
The sitemap has no ticker, so the resolution is confirmed from the fetched page's
`securityInfo.info.ticker` — a name match alone is a coincidence to be disproved (§76).
**Tickertape carries ONE basis per company**, whatever its rows' `reporting` field says; `view` is
presentation (normal/margin/growth), not basis. The page route serves 10 quarters, the API 40.

### 81c. ★★ THEY ARE NOT THREE INDEPENDENT READERS — measured, and it changes what agreement means

| quarter | MC `Total Income From Operations` | TL `SR_Q` | TL `TOTAL_SR_Q` | TT `qIncTrev` |
|---|---|---|---|---|
| WESTLIFE con 2026-06 | 735.64 | **735.64** | 742.23 | **742.23** |
| GICRE con 2026-03 | 13018.27 | **13018.27** | 13663.35 | **13663.35** |
| NIACL con 2025-12 | 12069.24 | **12069.24** | 12234.97 | **12234.97** |

MC's total-from-operations **is** Trendlyne's `SR_Q`, and Trendlyne's `TOTAL_SR_Q` **is**
Tickertape's `qIncTrev`, to the paisa, across every company sampled. All three are fed by the same
upstream vendor with different row cuts. So **two of these sites agreeing is close to no evidence
at all** — it is one reader counted twice. An independent confirmation is a FILING read. Record
cross-site agreement, never treat it as corroboration. (ALOKINDS 2026-06 is the one sampled
divergence: TL 998.24 vs TT 1015.44 — different vintages of the same feed.)

### 81d. Reach — MEASURED per company, per basis (2026-08-11; 8 companies off the real gap list)

Reach is a property of the COMPANY as much as the site. Quarters / oldest quarter — annual FYs / oldest FY:

| company | MC std | MC con | TL std | TL con | TT (single basis) |
|---|---|---|---|---|---|
| WESTLIFE  | 111 · 1998-12 | 51 · 2013-06 | 13 · 2023-06 | 13 · 2023-06 | 10 con · 2024-03 |
| SPICEJET  | 73 · 2007-09 | 39 · 2016-06 | 13 · 2022-12 | 13 · 2022-12 | 10 con · 2023-09 |
| ZFCVINDIA | 73 · 2008-06 | 21 · 2021-06 | 13 · 2023-06 | 13 · 2023-06 | — (unresolved) |
| KENNAMET  | 113 · 1998-03 | 15 · 2019-03 | 13 · 2023-03 | 13 · 2019-09 | 10 std · 2023-12 |
| ALOKINDS  | 74 · 2008-03 | 33 · 2018-06 | 13 · 2023-06 | 13 · 2023-06 | 10 con · 2024-03 |
| GICRE     | 40 · 2016-06 | 32 · 2018-06 | 13 · 2023-03 | 13 · 2023-03 | 10 con · 2023-12 |
| NIACL     | 41 · 2016-06 | 33 · 2018-06 | 13 · 2023-06 | 13 · 2023-06 | 10 con · 2024-03 |
| MOIL      | 67 · 2009-12 | 11 · 2018-06 | 13 · 2023-06 | 11 · 2018-06 | 10 std · 2024-03 |

Annual tables: MC 13–35 FYs (WESTLIFE back to FY1998, KENNAMET to FY1990), TL 11 FYs (FY2016+),
TT 9–11 FYs (FY2017+). Say what a site has: **Moneycontrol is the only one of the three that
reaches the 2015–2022 window at all**; Trendlyne and Tickertape are a recent-window second opinion
and a corroboration source, nothing more. Trendlyne's "13 quarters" is the last 13 it HOLDS, not
the last 13 calendar quarters — KENNAMET con stops at 2022-09 and MOIL con at 2020-12 because the
companies stopped filing that basis.

### 81e. The gate — `scripts/agg_tools/agg_gate.py` + `agg_fy_check.py`

§60c's rule stands and is not negotiable: **the site's own series must reproduce values we already
store, with zero disagreements, or the WHOLE series is rejected — never cherry-pick the one cell
you wanted.** What this route adds:

* **Tolerances are set from MEASURED print precision.** screener prints crore-ROUNDED integers so
  `screener_gate.py` needs a 1.0-crore floor; MC/TL print TWO DECIMALS of a crore. Carrying the 1.0
  floor over made a holding company whose whole revenue is 0.2 cr "agree 13/16" while our
  0.47/0.30/0.32 sat against the site's 0.21/0.22/0.26. Floors here are one print-unit wide.
* **A** ≥2 anchors within ±6 quarters of the target, **zero disagreements in that window**;
  **A2** at least one anchor within 4 quarters; **A3** no disagreement within ±12 quarters
  (a restatement boundary near the target); **A4** global disagreement rate <15% (a wholly
  different entity — the TMPV class).
  Why local rather than global: 45 of 97 refusals on the first pass were ≥90% agreement whose only
  disagreements sat 6–30 quarters away, several of them **our** known-bad cells (ADANIENT 2014-12
  stored 2.44 against 17,849.84; KSB 2018-12 stored 0.0 against 346.6). §60d's own words are
  "reject the YEAR, not the company".
* **★ A5 — SITE-INTERNAL FY IDENTITY. The check that earned its keep.** The site's own four
  quarters of the target FY must sum to the site's own annual for that FY, and so must both
  neighbouring FYs (§60d: reject years adjacent to a restated one). It needs nothing from us, so it
  works even where we hold no sibling quarter. It caught **9 of 22** cells that had already passed
  the quarterly gate with 27–40 anchors and zero local disagreements:
  ICIL FY2022 ΣQ 2,862.87 vs annual 2,842.02 · PEL FY2023 9,354.70 vs 8,934.30 (Piramal Pharma
  demerger) · WELCORP FY2022 5,915.04 vs 6,505.11 · ADANIENT FY2024 off by 9,051 · EXIDEIND FY2022
  off by 2,346 · SAMMAANCAP restated in all three FYs. Nothing else would have found these.
* ⚠️ **The financial year is NOT always Apr–Mar** — RAIN closes in December, KENNAMET in June
  (measured off their annual keys). A5 reads the FY-end month from the site's own annual table.
  Assuming March turned every RAIN test into a silent NO-TEST, i.e. an untested cell reported as a
  checked one.
* **B** a printed 0 is the not-reported sentinel, held unless the other basis corroborates a real
  nil; **C** never write a value equal to our stored OTHER basis (the copied-con fingerprint — that
  belongs to the §6A no-sub identity route, which writes it WITH evidence); **D** the revenue row
  is PROVEN, not assumed — every candidate label is tried and only the one that reproduces our
  stored values is used, which is what makes bank (`Interest Earned`) and insurer layouts safe;
  **E** where more than one site passes, their target values must agree (weak, per §81c);
  **F** precision is measured from the anchors, not assumed — `site-exact` when every matched
  anchor lands within 0.01, else `rounded(<worst anchor error>)`, journalled either way (§60e).

### 81f. What it landed, honestly (142 open cells, the whole 2019-12 → 2026-03 window)

**13 filled**, every one from Moneycontrol, 8–12 local anchors each, 5 at zero anchor error:
ABSLAMC · BSE · IDEA×2 · JINDALSTEL · MMTC · NAM-INDIA · RAIN · RAJESHEXPO×4 · SUNDARMFIN.
Per-cell table with every anchor count and both FY identities:
`scripts/agg_tools/AGG_ROUTE_REPORT.md`. Ledger: `scripts/agg_cell_fills.json`, registered in
`verify_fills_live.py` at creation time so it never joins the class of ledgers that sat unguarded.

The other 129 are NOT "unfillable" — they are, per §61b:
* **9 `NEEDS-CROSSCHECK` (restated FY)** — reachable, but only from a filing read (81e A5).
* **5 `REJECT-EQUALS-OTHER-BASIS`** — ZFCVINDIA con 2021-06…2021-12: MC's consolidated series
  passes the gate and its value equals our stored standalone exactly. That is the no-sub identity
  case; §6A writes it with evidence, a blind copy must not.
* **~97 `NEEDS-CROSSCHECK` (gate refused)** — a site HAD the quarter and its series does not
  reproduce ours. Two large, informative classes:
  - **Insurers — and ⚠️ THE FIRST WRITE-UP OF THIS WAS WRONG, corrected 2026-08-11 same day.**
    I first recorded that "the aggregators' insurer revenue is a different quantity from ours".
    **It is not.** MC's `Total Income From Operations` (== TL's `SR_Q`, §81c) reproduces our stored
    `revC` **exactly, to the paisa**, on 9 of 10 overlapping GICRE quarters, 18 of 22 NIACL, 14 of
    22 HDFCLIFE. The convention MATCHES. What misled me was that MC's *other* revenue row,
    `Net Sales/Income from operations`, is only the premium leg (GICRE Mar-2025: 9,250.02 against
    the 13,208.55 we store) and it is the candidate my reader tries first — so the rejection
    message that reached the summary came from the wrong row.
    **The real blocker is anchoring, and it is specific:** for these companies every stored quarter
    we own is *newer* than the gap. GICRE's stored `revC` starts 2022-06 while its 17 open cells are
    2019-12 → 2024-09, so the oldest targets have **no anchor within ±6 quarters** at all. And
    Gate A5 cannot stand in for the missing anchor here either — measured on MC's own GICRE con
    rows: FY2020 ΣQ 51,497.90 vs annual **802.66** (the annual row is broken, not merely restated),
    FY2022 off 1,437.08, FY2023 off 1,952.56; NIACL fails A5 in 5 of 7 years. So in exactly the gap
    years there is neither a local stored anchor nor a working FY identity, and a write there would
    be unanchored — refused, correctly.
    **What this means for the next session:** do NOT skip the aggregators for insurers on the
    grounds of convention. The row exists and matches. These cells need ONE anchored quarter inside
    the gap window from a primary source (§43 IRDAI / §55 filing PDF); with a single 2020-or-2021
    anchor landed, MC's exact-matching series would carry the rest of the block through the gate.
  - **KENNAMET con** — MC *and* TL both differ from us by ~10% on three consecutive 2020 quarters
    (87.3 vs 95.6, 178.3 vs 197.1, 193.3 vs 216.8). Same vendor twice (§81c), so this is one
    disagreement, not two; it needs the filing.
* **`not-found-via:mc,tl,tt`** — the rest. Notable: **MOIL consolidated ends at 2020-12 on both
  MC and TL**, which is evidence toward the §63 not-applicable ledger but is NOT proof — it is one
  vendor, and never-infer-absence-from-our-own-gaps applies to theirs too.

### 81g. 48 SUSPECT cells of OURS, surfaced in passing — reported, NEVER patched

§61a mode 6: a site reproduces our series everywhere except one or two cells; the indictment is
against us. Examples: 360ONE 2019-12 revS ours 101.71 vs 45.03 (24/26 elsewhere), ABB 2025-06 revC
ours 3,144.52 vs 3,324.93, WESTLIFE revS 2021-12 and 2022-12 both stored 93.89 against 0.10/0.20,
KSB 2018-12 revC stored 0.0. All listed in `AGG_ROUTE_REPORT.md`. Correcting a stored value is the
§2b procedure with its own evidence — never a side effect of a fill pass (§58d).

### 81h. Running it

```
python3 -X utf8 scripts/build_fill_coverage.py --out /tmp/fc.json      # current open set
python3 -X utf8 scripts/agg_tools/agg_sweep.py   --coverage /tmp/fc.json --out /tmp/agg.json
python3 -X utf8 scripts/agg_tools/agg_fy_check.py --props /tmp/agg.json --out-props /tmp/final.json
python3 -X utf8 scripts/agg_tools/agg_report.py  --props /tmp/agg.json --final /tmp/final.json \
        --md scripts/agg_tools/AGG_ROUTE_REPORT.md
python3 -X utf8 scripts/agg_tools/apply_agg_fills.py --props /tmp/final.json [--apply]
python3 -X utf8 scripts/agg_tools/agg_reach.py --syms A,B,C --md /tmp/reach.md
```
Fetch and write are separate scripts on purpose: the write replays a ledger in ~1s, which is short
enough to win the rebase race on these single-line JSONs (CLAUDE.md rule 4 / §38). Responses are
disk-cached under `~/.cache/agg_reader/`; a re-run after a reader change costs nothing.

---

## 82. ★★★ "NOT APPLICABLE" NEEDS AN INDEX-CREDIBILITY GATE — E6, and the two bounds that decide it  (2026-08-11, FILL-2018)

**NO ASSUMPTIONS, NO GUESSWORK** (§0). Everything below is measured on the 2018 target set
(292 companies / 730 cells) and on the committed 2019 ledger.

§54b lets the NSE per-company filing index prove a NEGATIVE: E1 a standalone row exists for the
quarter, E2 no consolidated row, E3 the quarter precedes the company's FIRST consolidated row ever
⇒ no consolidated quarterly was filed. The 2019 campaign wrote 61 such records on that basis.

**Run unchanged over 2018 the rule calls 336 of 725 cells (46%) not-applicable — and it is wrong.**
Of those 336, **100 store a consolidated PAT for the very quarter being called unfiled, and 98 of
those differ MATERIALLY from the standalone one**:

| cell | stored con PAT | stored std PAT | index first-con |
|---|---|---|---|
| AXISBANK 2018-06 | 721.86 | 701.09 | 2019-06-30 |
| BANKBARODA 2018-09 | 603.71 | 425.38 | 2019-03-31 |
| GAIL 2018-09 | 1,787.16 | 1,962.96 | 2019-06-30 |
| AARTIIND 2018-09 | 138.58 | 122.92 | 2019-06-30 |

A *distinct* consolidated number had to be read from a document. So E3 did not measure the company's
filing history — it measured **the index's own coverage horizon**, which for these names starts at
the FY2020 compulsion date. This is §57 applied to an index ("a route returning nothing means THAT
ROUTE has no row") and §63 in a new coat (that one inferred absence from OUR gaps; this one from the
INDEX's gap).

E3 is **not** vacuous in general — 185 of the 292 target companies (63.4%) do have pre-2019
consolidated rows, some back to 2005 — so the fix is a gate, not deletion of the rule.

### 82a. E6, and why both of its bounds are load-bearing
> **E6.** The index's pre-first-con silence is evidence only if our own store holds no
> **materially different** consolidated figure **strictly before** that first-con date.

Both bounds were wrong on the first cut, and each one changes the verdict:

* **STRICTLY BEFORE, not at-or-before.** A company whose index first-con is Jun-2019 — the quarter
  consolidated quarterlies became compulsory (§51a) — and whose Jun-2019 filing shows con ≠ std has
  demonstrated only that it HAS subsidiaries, never that it filed a consolidated quarterly earlier.
  That is the textbook §51a shape and E3 stands. Counting the first-con quarter itself wrongly
  retracted **27** such cells (BAJAJCON, BEL, BHEL, CANBK, CONCOR, DBL, DMART…).
* **MATERIALLY DIFFERENT, not merely present.** Earlier passes in this repo manufactured con PAT by
  COPYING std (§54b's own circularity warning), so a stored con equal to std is not independent
  evidence of anything.

Applied: 2018 na **336 → 179 cells** (`scripts/no_con_quarterly_2018.json`), E6 refuting 157.
Tool: `fill2020_tools/classify_rev2018.py` (it also splits every target cell by what the exchange
record actually says — `con-row-exists` / `no-con-row-but-con-evidence` / `con-gap-after` /
`no-con-ever-yet` / `index-silent`, each pointing at a different rung).

### 82b. Two sessions, one defect, two different halves of it
While this ran, another session pushed `fe5af03e` diagnosing the same defect from the opposite end:
a screener annual identity (§60d) produced consolidated values for INDUSINDBK and NHPC, **both of
which were sitting in the na ledger as not-applicable**. Their test — any stored con rev/PAT for an
EARLIER quarter — refuted 26 of the 61 and kept them visible under `refuted_2026_08_11`.

The two tests are not nested, and each catches what the other cannot:
* theirs fires on a stored con figure even when it EQUALS std (so it also flags identity-copy cells);
* E6 additionally checks **the target quarter itself**, which an earlier-quarters-only test cannot
  see. Applied on top of their surviving 35 it adds exactly one — **SWANCORP 2019-03**, which stores
  a con PAT of −38.66 for the very quarter the record calls unfiled.

`recheck_na_2019.py` re-audits the committed ledger in place and **preserves the other session's
block untouched**. Nothing in the data moved: every record carries `written: null`, and no tool
consumes that file (the ledger feeding the coverage definition is the separate `no_con_filing.json`,
which contains none of them). What was wrong was the CLAIM.

**The generalised rule: a negative claim inherits the coverage of the source that made it.** Before
an index's silence is written down as non-existence, prove the index reaches that era for that
company — and our own store is the cheapest available witness.

### 82d. ★★★ E7 — AND OUR OWN STORE IS NOT ENOUGH EITHER
E6 asks whether OUR DATA contradicts the index. That is still a claim about our store, and it was
**not enough**. Of the 179 cells E6 left standing in 2018, **52 were then filled from Moneycontrol**
— series reproducing 26–32 of our stored quarters with ZERO disagreements, publishing a consolidated
revenue for the very quarter the record called unfiled. Among them BHARATFORG, BATAINDIA and
BOMDYEING: companies that self-evidently file consolidated accounts.

> **E7.** A cell may not be recorded as NEVER-FILED until an independent reader has been tried and
> has ALSO come up empty.

That is the exact mirror of §60f ("before any cell is reported unfillable, a second independent
reader must have been tried"), applied to the negative claim instead of the positive one. Both
directions of the same rule; only the positive one had been written down. **2018 na: 179 → 118.**

The three gates in order, each catching what the previous one cannot:
| gate | asks | what it caught in 2018 |
|---|---|---|
| E3 (§54b) | does the index show a con row before this quarter? | passes vacuously on an index that stops at the FY2020 compulsion date |
| E6 | does OUR OWN store contradict the index? | 157 cells |
| **E7** | does an INDEPENDENT READER contradict it? | **52 more** |

### 82c. ★★ 36 CELLS WERE NEVER ATTEMPTED — the delisted-scrip drop (§52b × §57a rule 4)
`bse_scrips.json` is built from BSE's **live** scrip master, so a delisted company resolves to
nothing and `backfill_revop_gaps` drops it from the worklist **before fetching anything**. Measured
on the 2018 anchored pool: **19 companies / 36 cells** — ALBK, DHFL, FRETAIL, MINDTREE, RELCAPITAL,
RELINFRA, IDFC, SREINFRA, UJJIVAN, EQUITAS, SHRIRAMCIT, TV18BRDCST… Reporting those as residue reads
as "tried and failed"; they were **not attempted** (§57a rule 4).

`fill2020_tools/resolve_delisted_scrips.py` resolves them from `_bse_master_all.json` (which carries
delisted rows) **gated on ISIN**, never on `scrip_id` — that is the KALYANI coincidence (§76). The
gate is free here: NSE's own filing-index rows carry an `isin` field, so both sides of the
comparison are exchange-published and neither is derived from our data. 29 resolved; 3 rejected for
having no master row at all (CDSL, MAXINDIA, RNAVAL). `backfill_revop_gaps.py` now consults the map.

**★ The refinement the gate forced: compare the ISSUER, not the whole ISIN.** Strict equality
rejected CORPBANK (INE112A01015 vs INE112A01023) and HDFC (INE001A01028 vs INE001A01036) — the same
company, security re-issued after a face-value change, which moves the ISIN's issue suffix and
leaves the issuer untouched. Matching the 7-char issuer prefix admits both and still refuses KALYANI
(INE610E vs INE0N6U — the *issuer* differs), which is the trap the gate exists for.

---

## 83. ★★★ A CORRUPTED TEXT LAYER THAT PRODUCES A **VALID NUMBER** — and the only screen that sees it  (2026-08-11, HINDALCO)

**NO ASSUMPTIONS, NO GUESSWORK** (§0) — every claim below is measured on the named attachment.

The 2018 §58 sweep landed **HINDALCO 2018-12 revC = 332,131.0**. The true figure is **33,213**.
It is 10× out, and it passed *every* guard the sweep has.

### 83a. Why nothing caught it
* the **column anchor passed** — the right column was picked; §58's anchor proves *which period*,
  never *which magnitude*;
* `con-rev-far-below-std` only fires **below** standalone; this sat 27.8× **above** it;
* §54a rightly **forbids** banding against the other basis' stored twin — con/std is legitimately
  44–61× for BBTC (it holds Britannia) and 4.6× for TMPV (JLR) — so the obvious check is the wrong
  one and would reject real data;
* the value is not absurd on its face: a ₹332,131 crore quarter is wrong for Hindalco but is the
  right order of magnitude for a Reliance.

### 83b. The cause — a third corruption class, and the worst of the three
The source (BSE attachment `0fee6aeb-7cdf-41b3-9b37-4324e37ea168`, filed 12-Feb-2020, reached by
`--rescue` for its year-ago column) has a text layer corrupted **in the digits**. Page 35, the
audited statement, extracts as:

    Revenue from Operations | 29,197 | 29,657 | 33,2131 | 88,826 | 96,797 | 130,542
                                            ^^^^^^^ printed 33,213, with a digit fused on

Same page: `Other Income | 297 | 287. | 2701`, and `(26 | (29) | (25]`.

Compare the two classes already on file:
* **§51b glyph substitution** (`Standalone` → `Slondolone`) — keyword search fails **loudly**;
* **§75 triple-rendering** (`Total Total Total Revenue Revenue Revenue`) — no label regex matches,
  so the row is simply not found.

This one is worse than both, because **the corrupted token is a syntactically valid number**.
`33,2131` → strip comma → `332131`. No label check, no regex, no unit test and no anchor can see
anything wrong. The read looks perfect.

### 83c. The screen that DOES see it — same-basis, ADJACENT quarters
`scripts/fill2020_tools/screen_neighbour_band.py`. §54a's prescription, with one refinement:
compare against the **nearest six same-basis quarters**, not the whole-series median. A global
median is the wrong scale reference for a trending series (it produced 99.3% false positives in
§74) and it is also too blunt in the other direction — Hindalco's consolidated revenue nearly
doubles across the stored window, so its global median (53,151) made a 10× error look like 6.25×.
Against the nearest six it reads **11.14×**.

Run it over **every cell a campaign writes**, not the suspicious-looking ones. Measured over this
campaign's landed cells it flagged exactly two: HINDALCO (real) and MCLEODRUSS 2018-09 at 2.07×
(false — its con/std of 1.30 is dead-centre of its own 30-quarter family 1.10–1.73; the flag is
McLeod Russel's post-2018 collapse straddling the comparison window, a real regime change). A 3.0×
band separates them; 2.0× does not.

**State the limit honestly: this screen catches ORDERS OF MAGNITUDE, not digits.** A corruption
turning 33,213 into 33,713 is invisible to it, to the anchor, and to every other guard in the
toolkit. The defence that would work is the one §75a already implies — **the same figure printed
more than once is a majority vote.** Hindalco's document prints 33,213 five separate times (media
release table p2, prose p3, segment tables p28/p32, statement p35) and the corruption appears in
exactly one of them. Requiring a second print before writing is the next reader improvement; it is
not built yet.

### 83d. What the retraction recovered — and a second cell for free
The same document settles the cell it broke. `apply_hindalco_2018.py` writes both, with the chain:
* **2018-12 revC = 33,213** — printed five times in Rs Crore under `Q3 FY19`; the *same row*
  reproduces our stored 29,657 (2019-09) and 29,197 (2019-12) to the rupee;
* **FY19 130,542 − 9M FY19 96,797 = 33,745 against our stored 2019-03 revC of 33,745.62 (0.002%)** —
  the check that proves the printed table shares our series' entity, basis and scale (§45);
* **2018-09 revC = 32,506.47**, from `9M FY19 96,797 − Q1 31,077.53 − Q3 33,213`. That cell has **no
  stored con PAT**, so §64 blocks every anchored reader from it — the printed 9-month total reaches
  it by arithmetic alone.

Both are labelled `precision: crore-rounded` (§60e): the printed FY of 130,542 against a stored
33,745.62 proves the source rounds, so the values are honest approximations with provenance, not
filing-precision reads.

---

## 84. ★★★ BSE NO LONGER SERVES PRE-OCT-2018 ATTACHMENTS — the boundary that shapes every 2018 backfill  (2026-08-11)

**NO ASSUMPTIONS, NO GUESSWORK** (§0) — measured on live fetches, 2026-08-11.

§52 records "pre-2016 BSE attachments 404 on AttachHis/AttachLive". **The boundary has moved
forward, and it now sits in the middle of the 2018 window.** The announcement LIST still indexes
those filings perfectly — `AnnSubCategoryGetData` returns 2-4 result filings per 2018 quarter with
GUIDs and headlines — but fetching the GUID returns 404 on **both** `AttachHis` and `AttachLive`.

Measured over ACC / WIPRO / HINDALCO, first result filing after each quarter-end:

| quarter filed for | PDF served | 404 |
|---|---|---|
| Mar-2018 | 0 | 3 |
| Jun-2018 | 0 | 3 |
| **Sep-2018** | **2** | **1** |
| Dec-2018 … Dec-2019 | 15 | 0 |

Corroborated across a wider sample: 10/10 of 2018-era attachments 404 while 9/9 of 2019/20-era
attachments return a PDF (AARTIIND, ACC, WIPRO, HINDALCO, SADBHAV).

### 84a. What this means for a pre-2019 campaign
* **The §58 route cannot read a company's OWN pre-Oct-2018 filing.** Every 2018 cell the §58 sweep
  landed came from a LATER filing's comparative column (`--rescue`) — visible in the ledger's `src`
  dates, which are all 2019-11 / 2020-02 / 2020-05. That is not a preference; it is the only door.
* **The VISION rung cannot help here either.** There is no document to render — this is not a scan
  and not a corrupt text layer, it is a 404.
* **Mar-2018 is squeezed from both sides**: its own filing is unretrievable, AND Moneycontrol's
  consolidated series begins at Jun-2018 for most companies (§82's measurement). That, not reader
  quality, is why Mar-2018 lags every other quarter on every route.

### 84b. ★ AND THE DIAGNOSTIC MUST NOT CALL THIS A FETCH FAILURE
`diag_rev2019.py` reports these as `pdf-unfetchable`, which reads as a run-time problem to retry —
the §55a shape ("an empty list is rate-limiting, not absence"). It is the opposite: retrying forever
will never help. **323 of 503 open 2018 cells land in this bucket**, so mislabelling it turns a hard
availability boundary into an apparently-actionable backlog.

⚠️ **Two traps this took, both worth avoiding next time.** First, when the bucket appeared I assumed
rate limiting (my own session had just pulled ~1,350 PDFs) and "confirmed" it with a positive control
that returned zero for filings I had demonstrably fetched minutes earlier — but the control was
calling `FI.datebound(None, …)`, and `bse_get` throws on a `None` opener while `datebound`'s
`except: break` swallows it and returns `[]`. **A broken probe and a blocked endpoint look
identical.** Use `FI.bse_session()`, and make the positive control prove itself before believing its
verdict. Second, the first real probe used a GUID truncated by my own `print`, so its 404 meant
nothing.


---

## 85. ★★★ MONEYCONTROL'S CONSOLIDATED TABLE FALLS BACK TO STANDALONE — the defect every gate passes  (2026-08-11)

Sibling to §81's route spec. §81 tells you how to READ Moneycontrol; this tells you what NOT to
believe once you have. Found before a single cell was written, on a 2,586-cell revenue ledger and a
1,555-cell PAT ledger.

**THE DEFECT.** Moneycontrol serves a `cons_quarterly` table for a company **even in quarters where
no consolidated result was filed** — and in those quarters the row carries the **STANDALONE**
figure. Apply it and you have manufactured a consolidated number that is really the standalone one:
the con-copy defect class §67 spent 18 heals unpicking, arriving this time through an aggregator
instead of a reader.

**WHY EVERY GATE PASSES IT — the part that matters.** The series gate proves MC's consolidated
series IS our company's consolidated series, by reproducing our stored con values elsewhere. It does
that correctly. **The fallback is PER QUARTER inside a series that is otherwise right.** Anchors,
the ±6-quarter local window, the global disagreement rate, the §83 magnitude band — all clean. This
is the first defect in this campaign that no existing gate could see.

### 85a. The discriminator — three states, and the third is the one people miss
Costs nothing for the first two: no fetch, our own store only.

| condition | verdict |
|---|---|
| MC con == our stored STD for that quarter, AND the company's own history shows con != std in ANY other quarter | **HOLD** — it consolidates differently, so an identical figure here is MC repeating standalone |
| MC con == our stored STD, and our history NEVER shows con != std for that company | **KEEP** — genuine no-consolidation-difference (MOIL / CHENNPETRO shape: subsidiaries equity-accounted, so con revenue really does equal std) |
| **our STD cell for that quarter is EMPTY** | see 85b — this is where a naive screen silently passes the cell |

### 85b. ★ THE EMPTY-TWIN HOLE, and the better test hiding behind it
Raised by the aggregator session after screening its own ledger clean: a con-vs-std comparison
**cannot fire when our own std cell for that quarter is null**, and such cells were falling straight
through UNCHECKED — neither held nor examined. Their ledger came back 0-held partly because all 11
of its cells happened to have a populated twin: clean by draw, not by design.

Holding blindly is not the fix. **Compare MC's consolidated to MC's OWN standalone for that
quarter** — source-internal, needs nothing from our store, and is the direct form of the question.
Only when that is also unavailable (no stored twin AND MC serves no std row) does it become an
explicit `HOLD-NO-TWIN`: unverifiable rather than verified, and visible in the ledger rather than
folded into "keep". Adding this arm caught 1 more revenue cell and 25 more PAT cells that the
our-twin test structurally could not see.

### 85c. Measured
    con revenue  852 cells equal our stored std -> 528 HELD (206 companies), 328 kept
    con PAT      256 cells equal our stored std -> 290 HELD (102 companies),  27 kept
    total held across the three MC ledgers: 823
Evidence against the held ones is not marginal — KRBL's own store shows con != std in **28**
quarters, JHS 20, BILVYAPAR 17, MINDTREE 15, and MC still returns a figure identical to standalone.

**Log the KEEP arm's evidence count, not a boolean.** "This company never shows con != std" is far
stronger at 40 overlapping stored quarters than at 2, and a company can do both — RAJESHEXPO shows
con != std in 41 of our quarters and con == std in 2.

### 85d. Consequences beyond Moneycontrol
* **Pre-FY2020 is the worst-affected era by construction.** Consolidated quarterlies only became
  compulsory from FY2020 (§51a), so pre-2020 is exactly where companies filed standalone-only —
  which is exactly where MC has nothing real to serve and substitutes. Any 2018-or-earlier
  consolidated backfill from this source is dominated by it.
* **It is probably in all three aggregators.** §81c establishes MC `Total Income From Operations` ==
  Trendlyne `SR_Q` == Tickertape `qIncTrev` to the paisa — one upstream vendor. So a cell "three
  sites agree on" may be three copies of one substitution. **Cross-aggregator agreement is not
  corroboration.** The independent checks remain a FILING read or the FY-sum identity.
* **A filing distinguishes the two bases by construction; an aggregator cannot.** The counter-example
  worth copying is the insurer PDF route: GICRE cells each gated on the SAME filing's standalone page
  reproducing our stored revS in three separate columns, every one landing con != std.

Tool: `scripts/fill2020_tools/mc_con_fallback_screen.py` — source-agnostic, annotates `held` +
reason in place, deletes nothing (a held cell is still a candidate for a real consolidated source).
All MC appliers skip `held`. **Run it over ANY aggregator-sourced consolidated ledger, including
retrospectively over cells already applied — the same test works after the fact.**

---

## 85. ★★★ A RETRACTED CELL CAME BACK — and no detector can see that class  (2026-08-11)

**NO ASSUMPTIONS, NO GUESSWORK** (§0). Measured on origin the same afternoon it happened.

28 cells were retracted as Moneycontrol consolidated-fallback (§81/§82's sibling defect: MC serves a
`cons_quarterly` row even for quarters where no consolidated result was filed, carrying the
STANDALONE figure). Hours later, **2 of them were live again**:

    SHREECEM 2018-06  3070.15   exactly the value retracted
    SYNGENE  2018-03    409.1   NOT the 409.0 retracted — a FRESH derivation, which is the tell
                                that a DIFFERENT route re-applied it

### 85a. The cause — CORRECTED, because the first diagnosis was wrong
`scripts/mc_history_fills.json` and `scripts/mc_pat_fills.json` claimed both cells with
`held: false`, and their fill-only appliers restored them. **This was first written up as "those
ledgers were never screened". That is wrong** — the owning session's screen had covered exactly those
two ledgers all along (608 of 611 held cells across the tree are its output). The real gap: **neither
ledger was ever REGISTERED in `verify_fills_live.py`**, so nothing re-checked either end of them.
Both are registered now, `mc_pat_fills` with its own `{std:1, con:3}` slot map because it writes
`sf_fundamentals` rather than `sf_revop`.

The transferable point survives the correction, and is if anything sharper: **a cell can be claimed
by ledgers from other campaigns, and a screen passing over a ledger is not the same as a detector
watching it.** Before retracting, grep the whole `scripts/` tree for the key; and check the ledger is
in the detector's registry, not merely that someone has screened it.

### 85a-bis. ★★★ A FALLBACK IS A BYTE COPY — DO NOT TEST IT WITH A BAND
The detector then refuted the retraction itself. `mc_con_fallback_retro_2018.py` used
`EQ_REL = 0.001` — 0.1% RELATIVE — to decide "equals our standalone". On a 3,000-crore value that is
3 crore of slack, and it retracted **7 of 28 cells that were NOT equal at all**:

| cell | written con | our std | apart |
|---|---|---|---|
| SHREECEM 2018-06 | 3,070.15 | 3,069.91 | 0.24 |
| COCHINSHIP 2018-12 | 717.11 | 716.42 | 0.69 |
| SJVN 2018-09 | 751.52 | 751.90 | 0.38 |
| GLAXO 2018-12 | 825.03 | 825.35 | 0.32 |
| CENTURYPLY 2018-12 | 579.17 | 578.89 | 0.28 |
| SJVN 2018-12 | 484.46 | 484.49 | 0.03 |

A source repeating the standalone row repeats it **exactly**; a difference of any size is evidence of
a real consolidated table. All six were restored. A retraction also has to leave the store
*internally consistent* — emptying consolidated revenue while the consolidated PAT from the same
source row stays live is its own defect.

### 85a-ter. The rule that survives, and it is ONE-DIRECTIONAL
Compare the SOURCE's consolidated row against the SOURCE'S OWN standalone row — same quarter, same
label, **each field judged on its own row**:

    differs   -> a genuine consolidated table FOR THAT FIELD. Writable.
    identical -> UNRESOLVED. NOT writable and NOT proven a fallback.

The second half matters: a company whose subsidiaries are equity-accounted files consolidated revenue
EQUAL to standalone while its profit differs (MOIL/CHENNPETRO), and at an aggregator that is
indistinguishable from a copy. **Neither a differing PAT nor a "Net P/L After M.I & Associates" row
proves a genuine table** — GAYAPROJ 2019-03 and PIIND 2019-03 are proven fallbacks and show both.
Settle the identical case from the FILING, not the aggregator.

⚠️ `--repair-held` is destructive and must never ride on a general `--repair`: a `held` flag can
itself be wrong, as 6 of these 28 were, and repairing on it deletes correct values.

> **BEFORE RETRACTING A CELL, GREP THE WHOLE `scripts/` TREE FOR ITS KEY.** Annotate `held` in every
> ledger that claims it, in all three key shapes (`SYM|QE`, `SYM|QE|con`, `SYM|QE|revC`) plus the
> nested `{SYM: {QE: …}}` stores. One un-annotated ledger is enough to undo the whole retraction, and
> the applier that does it is behaving correctly.

### 85b. ★ `verify_fills_live.py` IS STRUCTURALLY BLIND TO THIS
It reported **MISSING 0 throughout**, and it was right to: it checks that a LEDGERED value is still
PRESENT in the served payload. A retraction asserts the opposite — that a value must stay ABSENT —
and nothing checks that. **A `held` entry reappearing is a class the detector cannot see by
construction.**

So a retraction is not finished when the push succeeds. **Re-read the LIVE payload for the retracted
keys specifically** (§41/§56's rule, in the negative direction). That is how these two were found;
no alarm fired.

The natural fix is a `held`-aware pass in `verify_fills_live` — assert absence for every ledger entry
carrying `held` — which would turn this into a monitored condition instead of a manual re-read.
Recorded as the next improvement; not built here.


### 85d. ★★★ NEVER HARDCODE AN AGGREGATOR'S REVENUE ROW — score the label, and treat a TIE as unresolved
Moneycontrol serves SEVERAL revenue definitions in the SAME payload: `Net Sales/Income from
operations`, `Total Income From Operations` (= net sales + other operating income), and for banks
`Interest Earned`. **Picking the wrong one is invisible to every magnitude gate**, because both
values are plausible and sit in the right column. SIEMENS 2018-12 drifted exactly so — 2,753.3 (net
sales) vs 2,825.9 (total income), 72.60 apart, both real rows of the same payload.

> **Score EVERY candidate label against our own stored quarters and take the STRICT winner. A TIE is
> UNRESOLVED, never a fall-back to a preferred label** — a tie means the anchors cannot separate the
> definitions, which for an insurer or an NBFC is the difference between premium income and total
> income. Judge each FIELD on its own row; a revenue verdict must never be stamped onto a PAT ledger.

Measured (2019 session, over 2,171 applied revenue cells): 525 used Total Income, and 318 of those
had a materially different Net Sales row in the same payload — up to 58% apart (BAJAJFINSV 8,829 vs
3,681; ICICIPRULI 15,896 vs 10,056). Scored both ways, the chosen label reproduces strictly better in
**318 of 318**.

**★ A LOUD CRASH BEAT A SILENT WRONG ANSWER, and it is worth noticing why.** The label-scoring code
shipped with `ours` read one line before it was assigned, which raised `UnboundLocalError` on the
first symbol of every run. That looks like a plain bug, but `ours` is exactly the anchor set the
scorer chooses the row label against — **had it inherited a stale value from the previous iteration
instead of crashing, it would have picked THIS company's revenue row using the PREVIOUS company's
anchors**, and the wrong-row defect would have arrived silently on an unknown number of cells. A
variable that selects an interpretation should fail closed; the crash was the good outcome.

**★ AND IT OVERTURNED A REFUSAL.** The 2018 campaign closed its insurer route holding 11 cells on the
claim that "our stored insurer standalone cannot be reconciled to the filings". It reconciles — the
comparison was against the wrong row. On the winning label:

| | Net Sales | Total Income From Operations |
|---|---|---|
| GICRE con | 0/10 | **9/10** |
| NIACL con | 1/22 | **18/22** |
| HDFCLIFE con | 0/22 | **17/22** |
| ICICIPRULI con | 0/25 | **22/25** |

The MC gate had rejected all four insurers with "0 reproduced, 10-25 disagreements", and that was
recorded as evidence ABOUT THE DATA. It was evidence about the LABEL — §57's rule again, one level
down: a route returning nothing is a fact about the route.

### 85c. Why the wrong value is worse than the hole
Both cells are the §67 con-copy shape: a consolidated slot holding the standalone figure. That is the
defect class 18 heals were spent unpicking in §67, and an aggregator can re-create it silently at any
time. `held` in the ledger is a claim about intent; only the DATA is a claim about fact.

---

## 86. ★★★ ORPHANED PRICE SERIES — the phantom -100% backtest class  (2026-08-11)
**NO ASSUMPTIONS, NO GUESSWORK** — every number below was measured against the LIVE sf-data bins.

### 86a. The defect
`markPrice()` (both engines) deliberately marks a holding **0** when the asked date is past the
series' last bar AND that bar is >90d before the dataset end ("delisted -> worth 0"). Correct for
true delistings; WRONG when the listing continued under a new symbol — the old key's series just
stops (rename/scheme relist) and a backtest that held it books a phantom -100%. Worse, `priceAt()`
carries the last bar forward forever, so screens could ENTER such a series at a stale print
(MUNJALAUTO: entered 2006-06-30 on a close 45 days old, exited 0.00).

### 86b. Census (live bins, 2026-08-11)
1,705 of 4,468 symbols are dead-ending series. Evidence classes: 57 pairs with direct rename
evidence (34 _rename_map, 21 exact live-name match, 2 NSE symbolchange.csv); 884 unexplained
deaths in the 2003+ daily era (mix of true delistings, mergers, unmapped renames). 4 measured
PARALLEL-entity deaths (EICHER, GFL, IDBIBANK, OCCL) = absorbed by an already-listed company:
NOT stitchable, -100% semantics a modeling choice, not a data bug. Fragment shape: several old
keys hold pre-migration bars + weekend-special residue (TATAMOTORS 1996-2003+specials while TMPV
holds 2003+; GET&D, ADORWELD, AKZOINDIA same) — zero date overlap with the survivor, i.e. one
company's history split across two keys.

### 86c. The fixes shipped
1. **Entry-freshness gate** in `factorsAt()` (backtest-engine.js + stock-backtest.html, kept in
   sync): no bar within 14d of the screen date (28d pre-2002 weekly era) -> not screenable.
   Thresholds measured: daily-era gaps >14d = 0.044% of bar-transitions. At 2006-06-30 the gate
   excludes 991 stale carry-forwards. A/B same config 2006-07: live engine 5 wipeouts, gated 3
   (remaining 3 = real deaths); phantom entries cost that control run ~13% of final value.
2. **MANUAL_MERGE batch (23 pairs)** in update_sf_data.py: rename_map-evidenced fragments whose
   join is price-continuous on the survivor's adjusted scale (drift 0.93-1.07 after CA_OFF adj,
   gap <=78d, weekend residue excluded from the old end). Verified: no live fundamentals rows under
   any old key, none in F&O history, only AKZOINDIA/GET&D/MAHINDCIE in index membership history
   (first two already in build_membership_v2 supplement; MAHINDCIE added).
3. **insert_weekend_sessions is now rename-aware** (old->new via MANUAL_MERGE). MEASURED CAVEAT
   (first post-merge run, 2026-08-11): the day-level short-circuit (probe symbols already carry the
   bar) skips covered session days before row iteration, so the 233 old-key ledger rows re-home only
   when a session day is genuinely reapplied (fresh ledger day / from-scratch rebuild) — in nightly
   steady state those ~10 special-session bars per merged symbol stay absent from the survivor.
   Known, accepted loss; the mapping makes any future reapply land them correctly.

### 86d. What is deliberately NOT done (queue)
- **MUNJALAUTO -> MUNJALAU: do NOT MANUAL_MERGE.** The 2006 event is a court scheme (2 old shares
  -> 1 new + 1 Shivam Autotech share) — a stitch would fabricate a huge value-separation "crash"
  inside a 2-month trading gap. corp_actions.json floor is 2016-01-05 (measured), so no official
  factor exists to bridge it. The entry gate already kills its phantom trade.
- 28 NEEDS-FACTOR / LONG-GAP pairs (NIIT->NIITLTD, CEAT->CEATLTD, TATAMOTORS->TMPV pre-2003
  fragment, WOCKHARDT, MORAREALTY, NXTDIGITAL, ...): each needs its official corp-action terms
  verified before a join factor is written. The queue is COMMITTED: scripts/orphan_needs_factor.json
  (per-pair evidence class, CA-adj, measured drift, gap).
- Parallel-entity mergers: RESOLVED 2026-08-11 — the user pointed at the repo's own precedent
  (§22 build_shp_backtest.py: "delisted exits at last close", the convention used for every
  2020-2026 case). markPrice in BOTH engines now carries a dead series at its last traded close,
  so any death (merger, scheme, true delisting) exits at the last print on the next rebalance.
  Loss up to that print is still fully counted; the forced last-print→0 step is gone.


## 87. ★★★ PRE-2016 CORPORATE ACTIONS — the 2002→2016 extension of the price-adjustment layer  (2026-08-11)
**NO ASSUMPTIONS, NO GUESSWORK** — every factor written traces to an official record or two independent readers; every count below was measured this session.

### 87a. The gap this closed
The price builds adjust splits/bonuses via `corp_actions.json` — whose NSE-feed loop STARTED AT 2016.
Everything earlier rode on `ca_factor()` inference alone (ratio snaps to a CA fraction within 8%),
which has four measured failure modes:
1. **Ex-day move >8% off the fraction → the whole split is silently KEPT.** Flagship: ITC
   2005-09-21 (1:2 bonus + FV 10→1 = 1/15; +9% ex-day rally put the raw ratio at 0.0727, snapping
   to nothing) — the live bin carried a fake −92.7% "crash", all pre-2005 ITC ~15× off basis.
   Same class: BERGEPAINT-2015, HAVELLS-2014, BRITANNIA-2010, ABB-2007, JINDALSTEL-2008,
   KOTAKBANK-2005, UNITECH-2006 (1:1... actually 12:1 bonus × split = 0.0154), EIHOTEL-2006.
2. **Crashes divided out as phantom splits** (the LEGACY_FALSE_CA class, pre-2016 era).
3. **Demergers divided out as splits** (RELIANCE 2006-01-18's −25% separation was divided out as
   a phantom 3/4; IDFC 2015-10-01 as 2/5).
4. **Wrong fraction on ex-day pops** (VENKEYS 2015: true 2/3 baked as 3/4; SURANAT&P 1/5 as 1/4).

### 87b. Sources and their measured floors
- NSE corporates-corporateActions API: serves back to 1999, but split/bonus subjects are only
  dense from **2006** (2005: 829 rows, ONE bonus). 2002-2005 = dividends/AGM only.
- Era subject format differs, in THREE spellings, and each one silently dropped a whole leg:
  `Fv Split Rs.10/- To Rs.2/` (no "From"), `Fv Spl-Rs10tors2` (abbreviated keyword + run-together
  "tors"), `Bon-12:1` (abbreviated bonus). A subject can carry BOTH legs, so a missed spelling
  writes a factor that is wrong by the OTHER leg — UNITECH-2006 read 1/13 instead of 1/65. The
  open-gap gate caught all 19 of these as "record contradicts the tape" BEFORE they were applied;
  that rejection list is what led back to the parser. `Spl` is accepted only adjacent to an Rs
  amount, never as "Spl Dividend"/"Spl-50%" (77 of the 100 `spl` subjects are special dividends);
  `\bbon\b` never matches "bond"; bonus DEBENTURES (`Bonus Deb1:1` — BRITANNIA-2010,
  ASTRAZEN-2008) are excluded by class alongside NCRPS/DVR/preference. Gate widened (0.05,0.95)→(0.002,0.98):
  combined "Bonus 1:1 + split 10→1" = exactly 0.05 was being dropped (SUNILHITEC-2016 —
  ironically the case the combined parser was written for); pref/NCRPS/DVR bonuses now excluded
  by CLASS, not by the numeric gate.
- BSE per-scrip CA API (`DefaultData/w ... scripcode=`): reaches 2002+, ISIN-gated resolution
  only (bse_scrips.json by_isin). Blanket (no-scrip) queries are silently capped — never use them.
- Yahoo chart API split events: good 2002+ coverage for survivors, but records only ONE leg of
  combined bonus+split events (ITC → "10:1" only) and has wrong-date rows — NEVER write a Yahoo
  factor without the open-gap gate.

### 87c. The arbiters (calibrated on 566 ground-truth events)
- **PREV_CLOSE does NOT arbitrate**: measured UNADJUSTED on ex-dates in every era (562/566).
  (Its BZ-gap use — resumption-day re-basings — is a different situation.)
- **The OPEN does**: on a real CA the ex-day OPEN prints at the adjusted basis —
  (open/prev)/factor ∈ [0.957, 1.100] for p5..p95 of true CAs; equity crashes sit ≥ 1.19
  (they open near flat and fall intraday). Gate: ≤1.12 = CA-like, ≥1.18 = crash-like.
- Volume rescale (~1/factor) is too noisy to decide factor-2 events; supporting evidence only.
- Special dividends explain drops via the feed's dividend rows (amount ≈ drop in ₹).
- 2016+ only: absence from BOTH dense feeds = phantom (the audit_phantom_ca standing rule).

### 87c-bis. Measured outcome (this campaign)
- `corp_actions_hist.json`: **716 factor events / 542 symbols** + 99 keep-drop dates. Includes 24
  wrong-fraction fixes (VENKEYS 3/4→2/3, KARURVYSYA 2/3→5/7, ENGINERSIN 1/5→1/6...), 111
  verdict-confirmed additions, and **21 events recovered only after the third era spelling was
  found** — `Fv Spl-Rs10tors2/Bon-12:1` (UNITECH 1/65, VEDL 1/20, RAMCOCEM, STLTECH, NIITLTD,
  JAICORPLTD, RAJESHEXPO, DPSCLTD 1/230...). 11 feed rows the tape contradicts were EXCLUDED.
- `phantom_crashes.json`: +104 keep-drop dates / 78 symbols (era crashes; special-dividend drops
  PFIZER-2013 ₹360, STAR-2013 ₹500; penny reverse factors; 2016-17 dual-feed-absent).
  `crash_raw_prices.json` seeded with 205 era closes so the daily CI heal stays network-free.
- `rights_terp.json`: 253 total (+51). `demerger_adj.json`: 94 total (+11).
- Readers: Yahoo 1,856 symbols (499 event-boundaries → 76 missed + 54 oddball), ~190 BSE
  per-scrip records, 25,173 dividend rows, open-gap + volume + market-breadth locally.
- Residue: **231-item manual queue** at `scripts/ca2002_campaign/manual_queue.json`
  (+ verdicts.json.gz, kept_drops.json.gz — per-event evidence).

### 87d. What was written where
- `scripts/corp_actions_hist.json` (NEW, tracked): pre-2016 factors + noadjust, keyed by CURRENT
  bin symbol; merged into corp_actions.json by build_corp_actions.py on every regeneration.
- `scripts/phantom_crashes.json`: pre-2016 verified crash keep-drops appended.
- `scripts/rights_terp.json`: 1999-2013 parseable rights TERP entries (issue price = FV+premium;
  FV reconstructed from the feed's own FV-transition records + EQUITY_L — "premium" is NOT the
  issue price). Residual semantics vs baked state (M&MFIN-2020 pattern).
- `scripts/demerger_adj.json`: pre-2016 strict-demerger entries (factor = ex-open/prev from the
  era bhavcopy) incl. record-verified RELIANCE 2006-01-18 (0.6248 — the special-session price)
  and ZEEL 2006-12-18.
- `scripts/update_sf_data.py`: self_heal raw_close is rename-alias-aware (era tickers).
- Heal applied via SF_HEAL_WINDOW full reconciliation run locally (bhav cache required — CI's
  28-day window never revisits these, so CI needs no old bhavcopies).

### 87e. Rerun recipe
1. Worktree from origin/main + `scripts/_bhav_cache` (APFS-clone the backup: /Users/dhruv/...
   stocks-dashboard-oldpc-backup — 1996→2026 raw day rows, 1GB).
2. `python3 scripts/build_corp_actions.py` (merges hist ledger).
3. `SF_HEAL_WINDOW=99999 python3 scripts/update_sf_data.py` — self_heal reconciles every ledger
   event against the bin using raw cache closes; idempotent (re-run = no-ops).
4. STOP-GATE: heal log vs heal_preview, ITC/RELIANCE/IDFC/VENKEYS spot series, end not regressed.
5. Publish: split + orphan-branch + one-shot join/upload workflow (§1 rebuild pattern) →
   `gh workflow run refresh-backtest-data.yml` → verify live rev + client.

### 87e-bis. ★★★ A HEAL THAT RE-APPLIES IS A NIGHTLY REWRITE — two convergence guards
Caught by diffing the SELF-HEAL lines of two consecutive full-window runs: **24 events healed in
BOTH**. `self_heal` is only safe because it is idempotent; when it is not, CI re-scales the same
block every night forever. Two independent causes, both now guarded in `update_sf_data.py`:

1. **2-decimal QUANTIZATION on sub-rupee series.** The recovered `applied_f = raw_ratio /
   adj_ratio` is meaningless when the stored closes are 0.01-0.05 — `0.02/0.01` is EXACTLY 2.0,
   so the comparison against `correct_f` never converges. BIRLACOT / FARMAXIND / VKSPL /
   VISUINTL are long-standing `phantom_crashes` entries, and phantom entries reconcile
   UNCONDITIONALLY (no window), so **CI had been halving those series every night** — measured
   0.05 → 0.01 with 1,661 closes already at 0.00. **This PREDATES the pre-2016 campaign**; the
   full-window runs merely made it visible in one place. Guard: skip when either boundary close
   is under ₹0.25 (0.005/0.25 = 2% precision) and PRINT every skip (30 events).
2. **A date in BOTH corp_actions maps.** The feed can file two rows for one date — AHLEAST
   2022-10-06 has an official 2/3 factor AND a scheme row — so `factors[]` and `noadjust[]` both
   carry it and generate two events fighting over the same bar ("divide it out" vs "keep it"),
   each pass rescaling the pre-ex block ×1.5 (×2.25 after two runs). Guard: an explicit
   split/bonus RATIO outranks a keep-drop flag on the same date.

**The test that proves it:** re-run the full-window heal over the healed bin — it must report
**0** heals. Run this after ANY change to a heal ledger; a non-zero second pass is a bug, not a
correction. (8 damaged series were restored to pre-campaign values in the same publish; final
zero-close count is identical to pre-campaign, delta +0.)

### 87f. Residue (documented, NOT healed — never guess)
`scripts/ca2002_campaign/manual_queue.json` — 230 rows, 225 genuinely open (5 annotated
RESOLVED-by-class: bonus DEBENTURES — DRREDDY-2011 / NTPC-2015 / COROMANDEL-2012 — which issue no
equity and correctly carry no factor). The open set, by why it is open:
- **182 "insufficient evidence to flip"** — the baked factor is neither confirmed nor refuted: no
  Yahoo coverage (951 of 1,856 symbols returned nothing — mostly delisted microcaps), no BSE row,
  and an open print that decides nothing. Left exactly as baked.
- **16 "record contradicts the tape"** after the parser fix — e.g. SUNTV 2007-07-23
  "Agm/Spl/Bon-1:1/Div-20%" prints a `Spl` with NO terms, so the second leg is unknowable from the
  record; the open says the true factor is ~0.25, not the bonus-only 0.5. A number no record states
  is a guess — not written.
- **11 yahoo-event-without-open-confirmation**, 1 three-way disagreement.
- Sub-noise CAs on penny stocks (a 1:10 bonus on a Rs2 stock hides inside paise rounding); ~120
  suspension-gap boundaries (a factor riding a resumption gap — PREV_CLOSE is unusable across more
  than one session, feedback-prevclose-arbitrates-a-gap); pre-2006 demergers beyond the
  record-verified set (feed blind — kept-drop candidates with open-gap <=0.92 sit in
  `kept_drops.json.gz` for a future record check); rights with unparseable / partly-paid / PCD
  terms (same policy as the 2014+ sweep).
- **Penny-floor artifact:** a correct 1/10 factor on BIRLAPOWER's Rs0.01 series rounds 5 more bars
  to 0.00. That zero-close class is PRE-EXISTING and far larger (43,780 bars / 115 symbols / 0.47%
  of all bars) — not introduced here, and not worth reverting a record-verified factor.

---

## 88. ★★★ ERA-FLOOR AUDIT — post-2020 conventions the PAST data never got  (2026-08-11)
Prompted by §86 (the orphan -100% class was exactly this shape). Method: sweep the runbook for
conventions with an era floor, then MEASURE the old data. §87's territory (pre-2016 price
corp-actions, campaign in flight) deliberately excluded. All numbers measured on LIVE sf-data.

### 88a. ★★★ TURNOVER UNITS — FIXED 2026-08-11 (floors were a NO-OP before 2020)
`t` was whatever the day's bhavcopy carried: the old NSE zip (TOTTRDVAL) is RAW RUPEES, the new
sec_bhavdata_full (TURNOVER_LACS) is LACS. Everything downstream states LACS (TURN_OPTS, the
"Avg daily turnover (₹ lacs)" factor, build_stock_slices `t`), so `turnoverAt() < mcapFloor`
compared rupees against a lacs floor and passed ~everything. MEASURED distortion (₹100 cr/day
floor): 2010 passed **1,914** stocks, should be **42**; ₹1 cr: 2,428 -> 622. 2023 also moved
(2,641 -> 1,390) because a DEAD symbol's last bar is carried forward — stale pre-2020 rupee bars
were passing floors years later. Every preset starts 2020-03-31, which is why it hid this long.

**Shape (measured, 9.31M classifiable bars):** 1996-2019 rupees, 2020+ lacs, PLUS strays both ways
— 9,845 lacs bars inside 2019 (BZ backfill / weekend sessions splicing modern-format bars into
old-format days) and 1,974 rupee bars in 2022 (NSE served the old file on 2022-08-08). A single
date cutoff is therefore wrong, and so is "one seam per symbol" (this section's first draft) — the
unit is a property of the FILE, i.e. of the DAY: 7,556 of 7,576 dates are unanimous.

**The test — r = t / (c * v).** `c` is split-ADJUSTED while `t`/`v` are RAW, so r is exactly the
cumulative adjustment factor (rupee bar) or that / 1e5 (lacs bar): **price cancels**. That is
strictly better than build_nifty500_turnover.py's t/v ("≈ traded price") test, which misreads
sub-₹1 pennies and needs a per-date median to stay safe. r is sharply bimodal with an EMPTY band
between 10^-3.0 and 10^-1.3.

**`normalize_turnover_units()` in update_sf_data.py**, called LAST (after the day loop + self_heal,
so it also catches bars appended this run) and counted in the publish condition:
- verdict per DAY from that day's MEDIAN r (> 0.01 = rupees); a date with only zero-close rows
  (43,779 measured) inherits the nearest measured date;
- inside a rupee day, a bar >=1e4 BELOW the day median is a modern-format splice and is skipped —
  that is what protects the 9,845 already-lacs 2019 bars;
- rupee bars: t /= 1e5, kept to 4dp under 100 (a penny stock's whole day can be < 1 lac) else 1dp.

⚠️ **THE IDEMPOTENCE TRAP (§87e-bis, caught in test, not in prod).** The first draft used a per-BAR
absolute cut and pass 2 re-converted **601 bars**: floor-priced series (DHANUS, CIMCOBIRLA — adjusted
close ₹0.01-0.05) carry an adjustment factor near 1e4, so one division still leaves them above any
fixed rupee threshold and they get divided AGAIN. The day MEDIAN is immune to those outliers and
makes idempotence structural: converting a day divides its median by 1e5 too, so it reads lacs for
ever after. Verified on the real 201 MB bin: pass 1 = 5,977,197 bars, **pass 2 = 0**, 0 days still
rupees, the 9,845 lacs-in-2019 bars byte-unchanged, and RELIANCE continuous across the seam
(2019-12-31 ₹1,548 cr -> 2020-01-01 ₹970 cr, no 1e5 step).

**Blast radius (checked, no other change needed):** build_results_season.py + bake_liquid_universe.py
screen on c*v, not `t` — immune. build_stock_slices carries only the recent tail (already lacs).
build_nifty500_turnover.py classifies per date and self-disarms on all-lacs data; its thin-date era
fallback fires on ZERO dates in its emitted range (>=2009-10 all have >=5 liquid rows). build_volume
reads bhavcopy columns, not the bin.

**Spun off, NOT fixed here:** those floor-priced adjusted closes imply cumulative factors ~6,000x
(DHANUS 2007 adj ₹0.05 vs an implied raw ₹307) — a phantom-CA smell for the §87 ledger to
adjudicate, unrelated to units.

### 88b. ★★ DELIVERY-% COVERAGE: ~22% of bars pre-2017 vs ~90% from 2018 — **RESOLVED 2026-08-11**
Sampled every 7th bar: share of bars with dv>0 = 21-22% flat 2002-2016, 92% in 2018, ~90%+ since.
The §1 dv_fill "2002-2019 MTO backfill" reached only a subset. Delivery-based screens quietly lose
most of the pre-2018 universe. Queue: extend the MTO backfill sweep (NSE MTO archives reach 2003).

**RESOLVED (2026-08-11): +3,525,793 cells → 2002-2017 now 88.8-96.9%** (was 18.7-21.6% measured
on the full universe, not the 7th-bar sample). Root cause of the old gap: the 2026-08-02 build
harvested MTO files ~WEEKLY (Mondays) for 2002-2017 — its "N500 coverage 100%" claim was true only
on that sampled grid — while daily files exist for every date. Recipe (scripts/_mto_sweep_*.py,
fetch → build → merge; MTO_SP env = cache dir):
- **Source**: `nsearchives.nseindia.com/archives/equities/mto/MTO_DDMMYYYY.DAT`, HTTP/1.1 +
  browser UA, validate CONTENT (`10,MTO` record present, no `<html`) never size/exit-code.
  Floor measured: 2002-01-02 (2001 is 404). 16 dates 404 among 6,050 swept (5 in 2002-03,
  the rest muhurat/special sessions).
- **Two formats**: 2002-01-02..2002-02-11 rows are `20,SYM,SERIES,DELIV_QTY` (header total ==
  sum(qty) PROVES qty is deliverable; pct = qty/bin-volume). From 2002-02-14:
  `20,SR,SYM,SERIES,TRADED,DELIV,PCT`.
- **⚠️ Row assignment is by VOLUME IDENTITY, never by symbol preference.** Exact-symbol-first
  keyed ~600 Monday cells to the WRONG COMPANY in the 2026-08-02 ledger (DVL: rename map funnels
  both DPL-Petrochem and DTIL-Tea into DVL; bin DVL's 2015 bars carry DTIL's volume, ledger had
  stored DPL's pct). Rule: candidate MTO rows (exact + rename-mapped, series EQ/BE/BZ) for a bin
  bar are accepted only where `MTO traded == bin v` (99.9% of comparable rows match exactly).
  Controls on the overlap: existing dv == volume-matched MTO pct on 4,624,694 cells at 99.987%;
  the 602 contradictions ARE the wrong-company class (601 corrected in-ledger; the queued live
  overwrite is **SUPERSEDED by §89** — all 602 rows were DVL, whose price series itself was a
  wrong-company chimera; the §89 surgery replaces bars+dv together. Do NOT replay
  ~/.cache/mto_sweep/wrong_cells.json).
- **Ceiling, measured**: residual dv==0 bars per year = securities absent from that day's MTO
  (era MTO lists ~700-1,800 securities vs more bin symbols trading; 2005 dips to 88.9%, 2014 to
  88.8%) + ~7k no-volume-match skips + ~19k MTO rows whose bin bars don't exist (TELCO/AVAYAGCL
  era fragments — §86 territory, not dv work). 1996-2001: no MTO exists (pre-rolling, weekly-bar
  era) — stays 0%, correctly unfillable.

### 88c. ★★ §12 15:30-GATE DRIFT — FIXED 2026-08-11 (one-off cleanup + nightly automation)
The gate ran ONCE (2026-07-08, 3,760 events → 1,000 bumped) and gates NEW NSE ingestion — but
backfill writers (detres/vision/aggregator/scale-step campaigns) stamp ann-dates ungated. LIVE
count today: 6,059 ann-cells sit ON month-end rebalance days (2018: 365, 2019: 518, 2020: 724 …)
— roughly +1,000 cells of drift since July, concentrated in backfilled years.
**DONE:** 3,627 events re-decided → 172 confirmed after-close → **289 ann-cells bumped** (2015-18
carried 201 of them); 902 confirmed before-close and kept; 1,912 with no BSE same-date record + 641
with no scripcode left untouched — conservative by construction, since a bump can only ever EXCLUDE
a pick, never add one. Four bumps re-verified straight from the raw BSE payload (BAYERCROP 17:02,
AUROPHARMA 20:59, AMBUJACEM 17:28, BALKRISIND 18:34) and 0 invariant violations (every new date is
a real trading day, strictly later). The class can no longer regrow: the gate now runs every night
(recipe, calendar sidecar, times cache and idempotence signals in §12).

### 88d. ★ Internal series holes pre-2016 (the §80 shape, unadjudicated)
412 resume-after->60d holes in 2002-2015 vs 400 in 2016+ (post-BZ-backfill; both counts include
some weekend-residue artifacts and real suspensions). Whether a pre-2016 un-ingested series class
exists (BZ didn't exist then; Z/T2T did) needs bhavcopy sampling inside sample holes (probe:
AARTIDRUGS 2002-01-18 → 2003-09-19). Queue as an audit, not yet a defect claim.

### 88e. Audited CLEAN (conventions that DO cover the past)
Dividends never adjusted (documented, uniform); F&O membership 2001→date; N500 membership
2002→date (<500-sag fixed); weekend sessions immaterial pre-2002 (weekly bars); index history
pre-2007 via niftyindices; §86 death-at-last-close now uniform across all eras.

---

## 89. ★★★ A RECYCLED TICKER IS TWO COMPANIES UNDER ONE SYMBOL — the DVL/DTIL chimera  (2026-08-11)
**Found by §88b's volume-identity rule** (bin DVL 2015-03-02 v=5,950 == DTIL's MTO traded, while
DPL traded 26,573). Every claim below measured against the LIVE release asset + official NSE files.

### 89a. The class
NSE re-issues old symbols to unrelated companies. Lineage (symbolchange.csv + EQUITY_L ISINs):
today's **DVL** (Dhunseri Ventures, ISIN INE477B01010, listed 2008) traded as **DTIL** until
2010-07-26, then DPTL (→2014-11-12), then DPL (→2019-01-02). The tea business demerged out in
2014 and listed FRESH as **DTIL on 2015-01-20** — Dhunseri Tea & Industries, ISIN INE341R01014,
a DIFFERENT company that still trades today. symbolchange.csv has no row for it (it never
renamed); only the ISINs reveal the recycle.

### 89b. What the 2026-08-02 full rebuild did with that
`build_sf_data.py`'s ISIN auto-merge is recycle-safe, but its **symchg.csv supplement was not**:
it mapped DTIL→DPTL→DPL→DVL dateless, funneling ALL DTIL bhavcopy rows (both companies) into
DVL. The same-day dedup `dd[rec[0]] = rec` over `sorted()` tuples keeps the LARGER tuple — i.e.
**the higher close wins each collision day**. Measured vs MTO volume identity, bin DVL was:
correct through 2015-01-19; ~100% TEA-company bars 2015-02→2020 (tea priced above DPL/DVL
throughout); an alternating price-crossover mix 2021-22 (2021: 203 DTIL/44 DVL; 2022: 138/109);
DVL's own bars only from 2023-01-23. The tea company itself was left a 7-bar stub (created
2026-08-03 by the first post-rebuild daily run, which does NOT funnel).
**Worse: the chain poisons bars it never touched.** The fake ~2× "moves" where the series
switched company fed `ca_factor()` inference, the 2021-08-05 live-append day "reconciled" a
bonus factor against the TEA company's ex-drop (see 89e — the factor wasn't even DVL's), and
the re-anchor (last=raw) pushed it all backward: every pre-2015 bar sat at a UNIFORM 0.7118×
raw (measured 2008→2015-01-19, 10 samples) when the true DVL tape has NO corporate-action
factor anywhere 2015→date — the whole series should be RAW. −28.8% on seven years of history
that was never itself mis-attributed.

### 89c. The fixes (all 2026-08-11)
1. **RECYCLED-TICKER GUARD** in build_sf_data.py's symchg supplement: if the old symbol still
   has bars >45d past its own rename date, it was recycled — merge only bars ≤ cutoff into the
   chain, keep the later company under its own key, and keep the pair OUT of _rename_map.json.
   Swept the whole map for other live collisions: `_rename_map keys ∩ EQUITY_L` = **DTIL only**.
2. **`scripts/dvl_dtil_surgery.json.gz`** + `apply_series_surgery()` in update_sf_data.py (§80
   bz_backfill pattern): bhavcopy-true replacement bars for DVL [2015-01-20→build date] (DPL
   rows →2018, DVL rows 2019→, RAW throughout — see 89e; dv from DELIV_PER, MTO-pct overlay
   only on exact volume identity) + the tea company's full series under DTIL (×0.666667 before
   its 2021-08-05 bonus). A one-shot `pre` factor rescales the kept pre-2015 DVL bars
   0.7118×raw→raw, gated on BOTH segment inequality AND an anchor bar still holding its
   recorded WRONG close — a future clean rebuild can never be double-scaled (§87e-bis: second
   run must report 0).
3. **`scripts/_rename_map.json`: DTIL entry REMOVED** (a live company must not be rewritten to
   another key by present-era joins — the §30-7b/GUJGASLTD silent-vanish class, inverted).
   FUND_ALIAS never had DTIL (§30-4c's "OLD must not be alive" already refused it). BSE scrips,
   membership snapshots (none hold any Dhunseri symbol), F&O history: verified clean.
4. **dv_fill_hist.json.gz: 1,192 DVL cells ≥2015-01-20 PRUNED** — they were §88b's re-key to the
   bin's then-DTIL volumes; after surgery they'd be wrong-company again and fill-only would
   re-inject them wherever a repaired bar has dv=0. Surgery bars carry dv inline instead.
   **§88b's 601-cell dv overwrite leg: it SHIPPED (fbefe9cb, dv_overwrite.json) while §89 was
   being built, and §89 then RETIRED it** — all 602 cells were DVL, the surgery replaces those
   bars wholesale (DPL volume + DPL dv), so the leg's `v == vol` anchor can never match again;
   its cells were emptied (note in `_meta.superseded`) to silence a permanently-firing "left
   alone" tripwire. Same session-race lesson as §38: two heals aimed at one defect from
   different layers — the LOWER layer (bar replacement) wins and must retire the upper one.
4b. **The surgery ledger stores turnover in ₹ LACS** (the bin-wide unit since §88a's
   normalization, which landed the same evening). This is load-bearing: `normalize_turnover_
   units` skips lacs-median days, and post-normalization EVERY day is one — a rupee-unit bar
   spliced in later is invisible to it forever. Any future ledger that inserts bars into the
   bin must emit lacs itself (classify with §88a's own r = t/(c·v) test on raw values).
5. **crash_raw_prices.json seeded** with DVL 20100714/20100715 (169.95/144.20, the era's
   old-DTIL closes): self_heal's raw_close for the 2010-07-15 noadjust event resolved via the
   rename-map alias that step 3 removed; the committed fallback keeps full-window heals (§87)
   able to reconcile it. The 2014-09-18 event still resolves via the kept DPTL/DPL aliases.

### 89d. The lessons
- **A rename bridge without a date is a wrong-company merge waiting to happen.** Any (old→new)
  map consulted for a LIVE symbol must pass the §30-4c aliveness test; any merge driven by one
  must check the old key's bars actually STOP near the rename date.
- **A chimera's damage is not confined to the chimera window** — ratio-chain + re-anchor smears
  fake factors onto clean history. After fixing a wrong-company stitch, measure the bin/raw
  scale of the SURVIVING segments too, don't assume them clean.
- The §88b identity rule ("assign by volume identity, never symbol preference") is what caught
  this; symbol-keyed heals had quietly agreed with the wrong series for weeks.

### 89e. ★★★ NSE'S OWN CA FEED MIS-KEYS AN ACTION UNDER A SISTER COMPANY — the tape arbitrates
The corp_actions "DVL 2021-08-05 Bonus 1:2" (straight from NSE's corporates-corporateActions
API, symbol=DVL, comp="Dhunseri Ventures Limited") is NOT DVL's action. Proof, three independent
legs: (1) the tape — DTIL's raw close drops 521.15→346.65 (×0.665, a textbook 1:2 bonus) on the
ex-date while DVL moves 300.75→281.05 (−6.5%, and no CA-sized move exists ANYWHERE on the true
DPL/DVL tape 2015→date); (2) Yahoo independently records the 3:2 split on DTIL.NS that exact day
and none ever on DVL.NS; (3) share-capital math — DVL's equity capital is unchanged across 2021,
the tea co's grew ×1.5. Both companies had same-day actions (DVL's was the ₹2.50 dividend), which
is presumably how the filing/feed swapped them. **The validation gate that caught it:** requiring
the constructed series' bonus seam to be continuous AFTER applying the official factor — it
wasn't, and the CA-jump scan found the drop on the OTHER symbol. Fixes: `MISKEYED` re-key table
in build_corp_actions.py (feed refetch would re-import the wrong row every run) + committed
corp_actions.json moved the factor DVL→DTIL. Lesson: **an "official" corporate-action record is
still a CLAIM; the raw tape + an independent recorder arbitrate** — same-family companies with
same-day actions are exactly where feeds swap symbols. Full-window self-heal cannot re-break the
repaired series either way: the reconciliation guard (raw_ratio/off outside [0.75,1.30]) rejects
the factor on DVL's tape, and converges it on DTIL's.
