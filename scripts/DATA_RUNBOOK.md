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
- **§18** DATA HEALTH MONITORING + COMMIT GUARDS
- **§19** SITE FEATURES ON SUPABASE
- **§20** RESULTS COVERAGE DASHBOARD
- **§21** MARKET BREADTH
- **§22** FII/DII HOLDINGS PER STOCK
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
    browser, then `git checkout` any tracked source you overwrote. Windows: prefix `PYTHONUTF8=1` (the build
    `print()`s a `→`). See §9.
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
Genuinely open items (memory: project-stocks-pending-queue has the full context):
- **Tier-1 re-sweep IN PROGRESS** — 56 companies / 514 cells left; resume per memory
  project-stocks-resweep-resume (ledgers `scripts/_wf_skips.json` + `_wf_audit_done.json`).
- **Bucket B/C/D "unfillable" re-audit** (user asked 2026-06-23 "don't assume, remind me later"):
  B=110 cos/1188 skip-logged cells (re-verify skips after finder improvements), C=25 recent-IPO
  pre-listing (re-check RHP Q1-stub method §6), D=5 dead-ends (HEXT/PIRAMALFIN/SPICEJET/IOB/BASF).
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
  `{defaultUniverse, dataAsOf, universes:[{key,label,note,quarters:[{...,rev:{median,n,total,tn},op,ebit,pat}]}]}`.
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
  1. Master list per quarter-end: `/api/corporate-share-holdings-master?index=equities&from_date=<QE>&to_date=<QE>`
     — ⚠️ the window filters on the pattern's **AS-ON date**, so from=to=quarter-end returns EVERY company's
     filing for that quarter in ONE call (~2,300/qtr). Mid-quarter as-on dates = event-based SHPs (capital
     changes) — deliberately NOT tracked (quarter-end series only, Trendlyne-comparable).
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
- **Runs:** default = top-up last 3 QEs (current season + late filers/revisions of 2 back, only new/revised
  XBRLs re-fetched); `--backfill N` = deep fill; `--quarters <QE,QE,…>` = explicit list; `--reparse` =
  re-fetch even unchanged filings (schema upgrades, e.g. adding nsh); `--feed-only` = rebuild docs feed,
  no network. Initial backfill 2026-07-16: 4 quarters (Sep-25→Jun-26), ~7k XBRLs, ~45 min, 6 threads.
- **Auto-refresh:** `.github/workflows/refresh-shareholding.yml` — cron 12:40 + 20:40 IST **daily incl.
  weekends** (filings land any day); reset-and-replay commit carries shareholding.json + shp_meta.json +
  shp_history.json through /tmp (§18 gotcha); guard_feed before commit; dispatches pages.yml.
- **Per-stock view: `docs/stock.html` "Shareholding pattern" section** — quarterly table from
  shareholding.json (Promoters / FIIs / DIIs w/ +/− MF-insurance expander / Public&others=100−prom−fii−dii /
  No.-of-shareholders row that auto-hides while counts are absent); quarters oldest→newest, leading
  never-filed quarters trimmed; FUND_ALIAS fallback for renamed tickers.
- **Page `docs/shareholding.html`** (hand-maintained): stat cards (season filings / FII raised-vs-cut /
  DII raised-vs-cut / top FII add ≥₹500cr), filter chips (FII/DII raising/cutting, both, promoter, filed
  this week), min-move pp + mcap + sector filters, sortable columns, FII+DII sparkline w/ hover tooltip,
  Δ pills vs the stock's PREVIOUS filed quarter ("first" pill when no prior), NEW badge ≤3d, CSV export,
  sw-star watchlist, theme.js auto-cardify on mobile. Row cap 300 + "Show more".
- **Gotchas:** master `date` fields are as-on dates — a Jul window returns only event SHPs, NOT the June
  quarter (query from=to=QE instead). GAYAPROJ-style +16pp FII jumps are usually restructuring allotments —
  real filing data, not bugs. BSE-only stocks (no NSE listing) have no SHP here (future work, BSE source).

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
  pid-suffixed tmp + retry-on-PermissionError in save_hist (Windows readers block os.replace). For any
  big backfill while another writer may run: use **`--hist scripts/shp_history_stage.json`** (staging file,
  implies no feed/meta rebuild) then **`python scripts/_shp_merge_stage.py`** once the other writer exits
  (fill-only + newer-submission-wins + shrink-ABORT). CI is safe (workflow `concurrency` group).

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
HTML mislabeled by wayback (fetch raw, skip auto-decode) but is an 11KB shell; TZ= doesn't work in Git Bash.

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
