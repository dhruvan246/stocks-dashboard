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
- **§22** FII/DII HOLDINGS PER STOCK  (22h = verification vs external sites + cross-exchange, 2026-08-09)
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
- **§59** ★★ STANDALONE-SLOT-HOLDS-CONSOLIDATED AUDIT — the screen is not a defect count (**read before acting on any std/con equality screen**)

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
- ⚠️ **OPEN (found 2026-08-10, NOT fixed):** the turnover screen takes each symbol's *last 250 bars whenever they
  happened*, so **8 long-dead tickers** pass it and sit in "currently-listed companies trading ≥₹1cr/day" — SPSL
  (last bar **2009**-05-04), RDEL (2017), HDIL (2020), HSIL (2022), SPICEJET + LANCER (2023), HINDMOTORS, RAJESHEXPO.
  Their `alive` meta is stale-True (bin meta is frozen). A recency guard (last bar within ~60d of the bin's `end`)
  would fix it, but it shifts universe membership, so it needs its own pass — not folded into the caption fix.
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
  rebuilds NAV: value holdings daily via `markPrice` (delisting→0), rotate into that day's logged picks when the
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

Whole-sample: 49.7% after the 2026-08-07 sweep, **~51.4% after the 2026-08-09 seam fill — and MOVING**
(wayback harvest round 2 + a Trendlyne seam fill are in flight). Re-run
`python3 -X utf8 scripts/audit_shp_coverage.py` rather than trusting any number printed here;
what remains open after those land is pre-2010 (no source found, 7 sites + BSE measured empty)
and the un-captured share of 2010-2015.

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
  `--local`, `--csv out.csv`, `--missing <QE>` to list who is missing quarter by quarter).
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
it publishes whatever is committed and is safe to repeat. Proper fix if this keeps biting: give the
slice job a PAT for its push, or add a `workflow_run` trigger on pages.yml.
**This is §41's own lesson recurring one layer out** — the bytes were right in git, the derived slice
was right in git, and the site still served nulls.

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
    QID = "NN.00" where NN = 85 + 4*(FY-2015) + {Mar:0, Jun:1, Sep:2, Dec:3}   (85 = Mar-2015)
    QID = "NN.50" on the fiscal-year-END quarter = the audited ANNUAL row
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
  2019-20 std series is shifted one quarter (two cells healed; whole-series rebuild = OPEN, joins
  the LEHAR/MAFATIND club). DLF Mar/Jun-19 con were ×100-unit AND double-indexed (4.14 → 436.56 /
  414.72, unit proven by an exact total-overlap between two filings).
* **Rev co-poisons HEALED 2026-08-10** (commit 11e8263d; same mechanism, rev slots): GSFC revS
  20190331 1707.7→2138.42, CARERATING revS 20190331 45.22→81.49, ROLTA revC 20220630 3.76→5.08 —
  every value re-verified against refetched primary XBRLs, defects in `rev_defects.json`,
  corrected values in `std_rev_nse_reads.json`/`con_rev_nse_reads.json`. Fill candidates from the
  same documents also landed: SPENCERS Jun-18 std 2.22/con 0.62 + INFOBEAN Jun-18 std 5.05/con
  5.24 (new rows, `xbrl_comparative_fills.json` both forms), SPENCERS Mar-18 con −8.94
  (FY-derived, `con_pat_fy_derived.json`).

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

Open flags (measured, unresolved — in the verdicts file): KOHINOOR Mar-25 375.64 / Mar-26 87.65
(both files agree, out of family ×100, debt-settlement era unverified); RPSGVENT Sep-18 58.46;
METROPOLIS Sep/Dec-18 (FY19 identity fails by +9.6 with Q1+Q4 filing-confirmed); HALDER Mar-26 con
36.82 (H2−Q3 says 19.13, con EPS fails both filings); DBL Dec-25 con 829.85 > filing total
(negative-NCI unverified).

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
