# STOCKSWORLD — DATA RUNBOOK  ★ read this FIRST before any data work ★

The canonical *do-exactly-this* guide for fetching / refreshing / backfilling / building the
data. **Future session: follow these steps, don't re-explore.** Indexed in `MEMORY.md` so it
loads every session. (README.md is STALE — it describes the old Yahoo pipeline; this is current.)

---

## 0. GOLDEN RULES (the things that bite if forgotten)
- **Profit basis = OWNERS-ATTRIBUTABLE.** Backtest `npCon` (FUND index 3) = owners-attributable.
  Apply via `apply_owners_full.py`. ⚠️ **NEVER run `apply_total_pat.py`** (wrong basis). (memory: project-stocks-profit-basis)
- **Fundamentals come from BSE filing PDFs + VISION** — not Screener, not OCR (OCR mangles digits). (memory: feedback-bse-pdfs-not-screener)
- **⚠️ BSE `FinancialResult` API is entity-POISONED for some scrips — verify the audited entity, not the scrip label.**
  `FinancialResult/w?scripcode=500033` (FORCEMOT) + its `/downloads1/BSEFinancialResult*.zip` return **BSE Limited's**
  numbers, not Force Motors'. The **announcement** API `AnnSubCategoryGetData?strScrip=500033&strCat=Result` → attachment
  GUID → `corpfiling/AttachLive|AttachHis/<guid>` returns the **genuine** company. So poisoning is endpoint-specific, not
  scrip-specific. ANY BSE fetch must confirm CIN/auditor on the PDF (FM=L34102MH1958/Akurdi; BSE Ltd=L67120MH2005/Batliboi),
  never just the scrip label. Full writeup + the proven backfill recipe: `scripts/FORCEMOT_CONTAMINATION_FINDINGS.md`. (2026-06-22)
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
  the tool, fetch the LIVE parts first: `curl -s https://dhruvan246.github.io/sf-data/sf_stock_data_{1,2}.bin?v=<end>`
  (merge `data`/`meta`, `end` from `sf-data/sf_meta.json`), or pull the `data` release asset. Cross-check
  `gzip-decompress(docs/sf_stock_data.bin).end` vs `sf-data/sf_meta.json {end}` before trusting local results.

---

## 1. DAILY PRICE REFRESH  (survivorship-free dataset)
Auto: `.github/workflows/refresh-backtest-data.yml` — **cron 15:15 UTC = 20:45 IST, weekdays**
(after NSE's ~19:00 IST bhavcopy). **Self-heals:** `update_sf_data.py` appends EVERY missing day
since the file's `end`, so a skipped run is caught up by the next.

Pipeline (the workflow, in order — to run by hand do the same):
1. `python3 scripts/build_corp_actions.py` — refresh official split/bonus ratios (exact ex-date factors).
2. `python3 scripts/update_sf_data.py` — append missing trading days to `docs/sf_stock_data.bin`
   (writes `docs/.sf_updated` and bumps `docs/sf_meta.json {end}` only when it actually appended).
3. Publish `docs/sf_stock_data.bin` as the GitHub Release asset **`data`** (`gh release upload data … --clobber`).
4. `python3 scripts/split_sf_data.py` → force-push `sf_stock_data_1.bin`+`_2.bin`+`sf_meta.json` to the
   **sf-data** Pages repo (secret `SF_DATA_TOKEN`). Browser loads from there (same origin, no CORS).
5. Commit ONLY `docs/sf_meta.json` (≈20-byte version marker) — clients re-download when `{end}` bumps.

Trigger manually (today not in yet / missed run):
- GitHub → Actions → **"Daily backtest data refresh"** → Run workflow, **or** `gh workflow run refresh-backtest-data.yml`,
  **or** POST `/repos/dhruvan246/stocks-dashboard/dispatches` `{"event_type":"backtest-data-refresh"}`.
- Verify: `curl -s https://dhruvan246.github.io/sf-data/sf_meta.json` → `{"end"}` = target date.

---

## 2. FUNDAMENTALS BACKFILL  (quarterly net-profit gaps)
Daily cron `update_fundamentals.py` parses NSE XBRL for active names. GAPS (delisted, NSE-unserved,
pre-IPO) are filled MANUALLY from BSE PDFs via vision. **Full process: `scripts/_autorun_plan.md`.** Short:
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

---

## 3. INSURERS  (IRDAI-format — the cron CAN'T parse them)
LICI, SBILIFE, HDFCLIFE, ICICIPRULI, ICICIGI, GICRE, NIACL, STARHEALTH, GODIGIT, NIVABUPA, MFSL file
IRDAI format → XBRL cron gives them NOTHING. Whenever an insurer shows a fundamentals gap, follow the
dedicated playbook (correct page/row, owners-attributable, unit disambiguation, verify, apply):
→ **`scripts/INSURER_EXTRACTION_PLAYBOOK.md`** (memory: project-stocks-insurer-extraction).

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
    browser, then `git checkout` any tracked source you overwrote. Windows: prefix `PYTHONUTF8=1` (the build
    `print()`s a `→`). See §9.
- **Hand-maintained pages** (edit directly): `stock-backtest.html`, `saved-strategies.html`,
  `backtest-history.html`, `stock.html`, `fii-dii.html`, plus shared `theme.css`, `theme.js`, `bt-sync.js`, `backtest-engine.js`.
  NOTE: `stock-backtest.html` is self-contained (its own engine) and does NOT load `backtest-engine.js` —
  helpers used there must be defined locally.
- **Deploy:** commit + push (rebase loop §0); Pages redeploys ~30–90s; verify with the cache-buster curl (§0).

---

## 5. PENDING QUEUE (remind the user)
→ memory: project-stocks-pending-queue — apply staged pre-IPO backfill (14 stocks) + fix Adani Hindenburg-era prices.

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
merge the LIVE sf-data parts over it, else prices are days stale and CAGRs won't match the site:
```
curl -s "https://dhruvan246.github.io/sf-data/sf_stock_data_1.bin?v=<end>" -o scripts/_live/p1.bin
curl -s "https://dhruvan246.github.io/sf-data/sf_stock_data_2.bin?v=<end>" -o scripts/_live/p2.bin   # <end> from sf-data/sf_meta.json
node -e "z=require('zlib');f=require('fs');a=JSON.parse(z.gunzipSync(f.readFileSync('scripts/_live/p1.bin')));b=JSON.parse(z.gunzipSync(f.readFileSync('scripts/_live/p2.bin')));m={...a,data:{...a.data,...b.data},meta:{...(a.meta||{}),...(b.meta||{})}};f.writeFileSync('docs/sf_stock_data.bin',z.gzipSync(JSON.stringify(m),{level:6}))"
# verify: node -e "...gunzip docs/sf_stock_data.bin...console.log(d.end)"  → must equal sf-data {end}; ZOMATO absent, ETERNAL len>1000
```
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
- **Backtest pages lazy-load the engine** (17 MB + 90 MB sf data). `saved-strategies.html` renders its list from
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
Standalone page **`docs/results-season.html`** (own "Results Season" 📊 nav tab; fetches `results_season.json`
at runtime). Dark grouped-bar chart: per quarter (**Mar-2019 → latest, 29 quarters**), the MEDIAN YoY % across
reporting companies for **Revenue, Operating Profit, PAT**, with the reporting count in each x-label. Value labels on
each bar, hover tooltips, hand-rolled SVG (no chart lib), + a quarter-detail table. For many quarters the SVG renders
at natural width and the wrapper scrolls (so bars stay readable); it shows the **COVID crash (Jun-2020: PAT −56%) and
V-recovery (Jun-2021: +48%)**. ⚠️ MOVED OFF the Stocks dashboard → its own page, per user. Nav tab on every page.

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
  `{defaultUniverse, dataAsOf, universes:[{key,label,note,quarters:[{...,rev:{median,n,total,tn},op,ebit,pat}]}]}`.
- **MEDIAN ↔ TOTAL toggle (2026-07-01, user compared to Trendlyne):** each metric carries BOTH `median` (per-company
  YoY, consolidated-preferred, positive base — "typical company") AND `total` (aggregate Σnow/Σago−1, **STANDALONE**).
  Trendlyne's result-analysis page shows the **TOTAL, standalone** number — verified: our total = Nifty 500 Q4FY26 rev
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
- **⚠️ web.archive.org: curl/PowerShell BLOCKED but the claude-in-chrome BROWSER reaches it** (navigate, then
  page-context fetch to `/cdx/search/cdx` works). ind_nifty500list.csv has only ONE 2020 snapshot (2020-07-25).
  To hunt a missing press release: probe every weekday stem `ind_prs<DDMMYYYY>[_1|_2].pdf` on live niftyindices
  (GOTCHA: generating stems with python on Windows emits CRLF — `tr -d '\r'` or every URL silently 404s).
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
- **PAT** = `docs/sf_fundamentals.json` (owners-attributable con where filed, else std — same basis as the backtest,
  memory project-stocks-profit-basis). **Revenue + Operating Profit** live in a PARALLEL dataset
  `docs/sf_revop.json` = `{SYM:{QE:[revStd,revCon,opStd,opCon,patStd,patCon,fin,ebitStd,ebitCon]}}`
  (9 elems; readers pad legacy 7-elem rows), derived from the SAME NSE XBRL:
  **Operating Profit = ProfitBeforeExceptionalItemsAndTax + FinanceCosts + Depreciation − OtherIncome** (= Trendlyne's
  "Oper Profit", paisa-matched per-stock — NOT their proprietary "EBIDT"); **EBIT = op − Depreciation** (idx7/8).
  Banks/NBFCs (`InterestEarned`) excluded from Rev/Op (kept in PAT); their EBIT blank.

**SELF-UPDATES DAILY** — wired into `.github/workflows/refresh-fundamentals.yml` (21:15 IST weekdays):
1. `update_fundamentals.py` scans NSE integrated-filing-results for the last 120 days (ALL companies, one call) and,
   for each new filing, reads net profit (→`sf_fundamentals.json`) AND rev/op via `build_revop.xbrl_revop(xml)`
   (→`sf_revop.json`), fill-only, no disk cache needed. **Each filing is keyed by its OWN `qe_Date`** — so a LATE
   filing (a March quarter declared in Jul/Aug) lands in the March column, and a new quarter (June) becomes its own
   column. (Insurers = IRDAI format, no XBRL P&L → naturally absent from Rev/Op.)
2. `build_results_season.py` re-aggregates → `docs/results_season.json`. A NEW quarter column auto-appears once
   ≥200 universe companies have reported it; year-ago base must be positive. Reads the daily `docs/` copies (falls back
   to `scripts/` source copies for a local run); turnover universe from the committed (slightly stale, fine) bin.
3. Commit step pushes `sf_fundamentals.json` + `sf_revop.json` + `results_season.json` (rebase loop); the page picks
   up the new JSON on next load.

**Occasional FULL rebuild** (only if the daily fill-only drifts or you change the derivation): `python -X utf8
build_revop.py` re-walks ALL `scripts/_xbrl_cache/` (~102k files, parallel ProcessPool, resumable via
`_revop_progress.json`, prefilter ≥2018, MIN_QE 20180101, **latest-filing-wins**) → `revop_fundamentals.json` +
`docs/sf_revop.json`; ~98.6% PAT-validated (~90k cells). ⚠️ **OLD INDAS format (pre-~2021) needs `ctx_period()`** —
those filings don't carry the period inside `<xbrli:context>`; they tag `DateOf{Start,End}OfReportingPeriod` per
context. `ctx_period()` reads the context block first, then falls back to those tags (else 2018-20 silently parse as
0 symbols — the bug that capped history at 2022). Dec-2022 has a thinner cache (~1,100 vs ~1,800) — robust median, fine.
- Tunables: chart START quarter = `y, m = 2019, 3` in `build_results_season.py`; `TURN_FLOOR_CR` there (1.0 → ~1,290
  reporters); colours/labels in `renderResultsSeason()` inside `docs/results-season.html`.

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
  `/api/corporate-announcements?index=equities&from_date=&to_date=` in **7-day chunks** (5 calls/run).
  Self-healing: **merges with the existing file** (a failed chunk keeps yesterday's rows), trims to the
  31-day window, and **ABORTS below 200 rows** (never clobbers good data with a broken fetch).
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

---

## 15. QUARTERLY RESULTS DASHBOARD  (docs/quarterly-results.html — "Quarterly Results" nav, built 2026-07-12)
Best-of-breed results hub (features merged from a ~40-site survey: ValuePicker/Trendlyne/Screener/Tijori/
MarketsMojo/Moneycontrol/StockEdge/Investing.com/EarningsWhispers/Nasdaq/FactSet…). Four tabs:
**Season Overview** (scoreboard tiles + breadth + 5-season strip + sector heatmap/scorecard + movers incl.
result-day reaction), **All Results** (sortable per-company table: Rev/OP/OPM Δbps/PAT std+con auto-basis,
YoY+QoQ, rule-based verdict dot, reaction %, since-result drift; screen chips: turnaround LP/PL, margin
+200bps, record PAT, 4-qtr streak, reacted ±5%; CSV export; watchlist ★ localStorage `qr_watch`),
**Just Declared** (filing feed w/ PDF + growth badges, LIVE top-up via the same Cloudflare Worker
`?announcements=1` every 60 s), **Calendar** (upcoming results: NSE-confirmed ✓ + cadence-PREDICTED "est."
dates). Deep links: `?tab=results|feed|calendar&sym=TCS`.

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

**Refresh cadence (hourly since 2026-07-13):**
- **`refresh-results-hourly.yml`** — hourly 08:30–00:30 IST: runs the 3 fetchers but commits ONLY the two
  small side-files (feed+calendar), NEVER the 4.7 MB announcements.json (git-bloat guard; that big file
  stays on the 4×/day refresh-announcements.yml).
- **`refresh-fundamentals.yml`** — cron `45 4-17 * * *` = hourly 10:15–23:15 IST daily. Numbers (XBRL parse)
  land within ~1 h of filing. The quarterly_results.json BAKER (90 MB asset download + rebuild) is GATED:
  runs only when `.fund_updated` exists OR at the 15:xx UTC (21:xx IST) full nightly run (which always
  rebuilds so reactions/drift track the daily prices published 20:45 IST). Don't un-gate it — hourly 1.8 MB
  commits would bloat history for nothing.
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
- ⚠️ **ALWAYS parse the quarter PER FILING — never assume the current season.** A results filing in Jul/Aug/Sep
  is very often a LATE March (Q4/annual) result, not the June quarter (in a recent 30-day BSE window: 91 March
  vs 32 June filings). `parse_qe()` (fetch_announcements.py) / `qe_from_head()` (fetch_bse_results.py) ANCHOR on
  an "ended/ending <date>" clause (DMY/MDY/dd.mm.yyyy), snap to a quarter-end month, and return 0 (⇒ NO badge on
  the page, never a wrong one) when the period isn't stated — so a board-meeting date like "held on July 1, 2026"
  can't be mistaken for a quarter. The feed's growth badge keys off this qe, so it shows the March column's YoY
  for a March filing. BSE `QUARTER_ID` is always null — useless, must parse the headline+NEWSSUB.
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
