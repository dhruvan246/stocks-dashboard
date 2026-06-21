# STOCKSWORLD — DATA RUNBOOK  ★ read this FIRST before any data work ★

The canonical *do-exactly-this* guide for fetching / refreshing / backfilling / building the
data. **Future session: follow these steps, don't re-explore.** Indexed in `MEMORY.md` so it
loads every session. (README.md is STALE — it describes the old Yahoo pipeline; this is current.)

---

## 0. GOLDEN RULES (the things that bite if forgotten)
- **Profit basis = OWNERS-ATTRIBUTABLE.** Backtest `npCon` (FUND index 3) = owners-attributable.
  Apply via `apply_owners_full.py`. ⚠️ **NEVER run `apply_total_pat.py`** (wrong basis). (memory: project-stocks-profit-basis)
- **Fundamentals come from BSE filing PDFs + VISION** — not Screener, not OCR (OCR mangles digits). (memory: feedback-bse-pdfs-not-screener)
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
- **Dashboard** `docs/nse-bse-dashboard.html` ← `scripts/build_compressed.py` (`OUT_HTML`, embeds gzip+base64).
  Edit the `HTML=` template; nightly refresh rebuilds it. Immediate fix → also edit the live output (§0).
- **Mutual funds** `docs/mutual-funds.html` ← `scripts/build_mutualfunds.py` (template same rule).
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
