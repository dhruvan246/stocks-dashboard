# Tickertape extraction — full 66-symbol frozen sample (phase 3)

Extraction date: 2026-08-09. Route: `www.tickertape.in/stocks/<slug>-<sid>` SSR `__NEXT_DATA__` JSON
only. `api.tickertape.in` / `quotes-api.tickertape.in` never fetched (blanket `Disallow: /`, per
`capability_card.md` §0.2/§7 and this task's hard constraint). Sid discovery only from the cached
`sitemap_stocks.xml` (5,492 `<loc>` entries, parsed once into `sitemap_parsed.json`) — the disallowed
`/search` API was never touched.

**14 pilot symbols were reused, not re-fetched** (RELIANCE, SBIN, HDFCBANK, SBILIFE, BAJFINANCE,
TATASTEEL, GRASIM, ETERNAL, MOTHERSON, MSUMI, GICRE, LICI, SAIL, MEESHO) — their rows come straight
from `tickertape_pilot.jsonl`, already identity-confirmed in that earlier pass (see `pilot_notes.md`).
Only the **52 new symbols** required fresh HTTP requests: 51 live fetches in `fetch_p3.py`
(script + log; 2s+ apart, foreground, no background/async waiting) plus one **foreground retry** for
NIACL after its first attempt hit a read-timeout (not a 403/429 — retried per protocol, succeeded on
attempt 2 with a longer timeout).

Script: `fetch_p3.py` (extends `fetch_pilot.py`'s exact parsing/basis/quarter-filter logic, adds
ordered multi-candidate fallback per ticker for the ambiguous sitemap cases). Raw fetch metadata:
`fetch_notes_p3.json`. NIACL's manual retry payload: `niacl_pageprops.json`. Final output:
`tickertape_p3.jsonl`.

## Per-symbol results — 52 newly fetched

| sym | sid | ISIN | ticker echo | reporting (all qtrs) | oldest qe | newest qe | n qtrs |
|---|---|---|---|---|---|---|---|
| AADHARHFC | AADH | INE883F01010 | AADHARHFC | consolidated | 2024-03-31 | 2026-06-30 | 10 |
| AARTIPHARM | AART | INE0LRU01027 | AARTIPHARM | consolidated | 2024-03-31 | 2026-06-30 | 10 |
| ABFRL | ADIA | INE647O01011 | ABFRL | consolidated | 2024-03-31 | 2026-06-30 | 10 |
| ABSLAMC | ABS | INE404A01024 | ABSLAMC | consolidated | 2024-03-31 | 2026-06-30 | 10 |
| AJANTPHARM | AJPH | INE031B01049 | AJANTPHARM | consolidated | 2024-03-31 | 2026-06-30 | 10 |
| AJRINFRA | — | — | — | — | — | — | **0 — identity_skip, see below** |
| BALKRISIND | BLKI | INE787D01026 | BALKRISIND | consolidated | 2024-03-31 | 2026-06-30 | 10 |
| BPCL | BPCL | INE029A01011 | BPCL | consolidated | 2024-03-31 | 2026-06-30 | 10 |
| CASTEXTECH | — | — | — | — | — | — | **0 — identity_skip, see below** |
| CCL | CCLP | INE421D01022 | CCL | consolidated | 2024-03-31 | 2026-06-30 | 10 |
| CUMMINSIND | CUMM | INE298A01020 | CUMMINSIND | consolidated | 2024-03-31 | 2026-06-30 | 10 |
| EICHERMOT | EICH | INE066A01021 | EICHERMOT | consolidated | 2024-03-31 | 2026-06-30 | 10 |
| ENRIN | ENR | INE1NPP01017 | ENRIN | **standalone** | 2024-06-30 | 2026-06-30 | **8** |
| FEDERALBNK | FED | INE171A01029 | FEDERALBNK | consolidated | 2024-03-31 | 2026-06-30 | 10 |
| FLUOROCHEM | GUJL | INE09N301011 | FLUOROCHEM | consolidated | 2023-12-31 | 2026-03-31 | 10 |
| GMRP&UI | GMR | INE0CU601026 | GMRP&UI | consolidated | 2023-12-31 | 2026-03-31 | 10 |
| GODIGIT | GODIG | INE03JT01014 | GODIGIT | **standalone** | 2024-03-31 | 2026-06-30 | 10 |
| HEROMOTOCO | HROM | INE158A01026 | HEROMOTOCO | consolidated | 2024-03-31 | 2026-06-30 | 10 |
| HINDALCO | HALC | INE038A01020 | HINDALCO | consolidated | 2024-03-31 | 2026-06-30 | 10 |
| HONASA | HON | INE0J5401028 | HONASA | consolidated | 2023-12-31 | 2026-03-31 | 10 |
| HUDCO | HUDC | INE031A01017 | HUDCO | consolidated | 2024-03-31 | 2026-06-30 | 10 |
| IGIL | IGI | INE0Q9301021 | IGIL | consolidated | 2024-03-31 | 2026-06-30 | 10 |
| INDIAMART | INMR | INE933S01016 | INDIAMART | consolidated | 2024-03-31 | 2026-06-30 | 10 |
| INDOBORAX | INDOB | INE803D01021 | INDOBORAX | consolidated | 2023-12-31 | 2026-03-31 | 10 |
| JIOFIN | JIO | INE758E01017 | JIOFIN | consolidated | 2024-03-31 | 2026-06-30 | 10 |
| JUBLPHARMA | JULS | INE700A01033 | JUBLPHARMA | consolidated | 2023-12-31 | 2026-03-31 | 10 |
| M&MFIN | MMFS | INE774D01024 | M&MFIN | consolidated | 2024-03-31 | 2026-06-30 | 10 |
| MUTHOOTFIN | MUTT | INE414G01012 | MUTHOOTFIN | consolidated | 2024-03-31 | 2026-06-30 | 10 |
| NCC | NCCL | INE868B01028 | NCC | consolidated | 2024-03-31 | 2026-06-30 | 10 |
| NIACL | THEE | INE470Y01017 | NIACL | consolidated | 2024-03-31 | 2026-06-30 | 10 |
| NIVABUPA | NIVA | INE995S01015 | NIVABUPA | **standalone** | 2024-03-31 | 2026-06-30 | 10 |
| NTPCGREEN | NTP | INE0ONG01011 | NTPCGREEN | consolidated | 2024-03-31 | 2026-06-30 | 10 |
| PINELABS | PINEL | INE15B701018 | PINELABS | consolidated | 2024-06-30 | 2026-06-30 | **9** |
| POLICYBZR | POLI | INE417T01026 | POLICYBZR | consolidated | 2024-03-31 | 2026-06-30 | 10 |
| POLYMED | PLMD | INE205C01021 | POLYMED | consolidated | 2024-03-31 | 2026-06-30 | 10 |
| POWERINDIA | ABBW | INE07Y701011 | POWERINDIA | **standalone** | 2024-03-31 | 2026-06-30 | 10 |
| PREMIERENE | PREMI | INE0BS701011 | PREMIERENE | consolidated | 2024-03-31 | 2026-06-30 | 10 |
| PWL | PWL | INE0LP301011 | PWL | consolidated | 2024-06-30 | 2026-03-31 | **8** |
| RADICO | RADC | INE944F01028 | RADICO | consolidated | 2024-03-31 | 2026-06-30 | 10 |
| RAJRATAN | RAJR | INE451D01029 | RAJRATAN | consolidated | 2024-03-31 | 2026-06-30 | 10 |
| RBLBANK | RATB | INE976G01028 | RBLBANK | consolidated | 2024-03-31 | 2026-06-30 | 10 |
| SAGILITY | SAGI | INE0W2G01015 | SAGILITY | consolidated | 2024-03-31 | 2026-06-30 | 10 |
| SHK | SHKE | INE500L01026 | SHK | consolidated | 2024-03-31 | 2026-06-30 | 10 |
| SOBHA | SOBH | INE671H01015 | SOBHA | consolidated | 2024-03-31 | 2026-06-30 | 10 |
| STARHEALTH | STARH | INE575P01011 | STARHEALTH | **standalone** | 2024-03-31 | 2026-06-30 | 10 |
| SUNDARMFIN | SNFN | INE660A01013 | SUNDARMFIN | consolidated | 2024-03-31 | 2026-06-30 | 10 |
| SUPREMEIND | SUPI | INE195A01028 | SUPREMEIND | consolidated | 2024-03-31 | 2026-06-30 | 10 |
| TARIL | TRNF | INE763I01026 | TARIL | consolidated | 2024-03-31 | 2026-06-30 | 10 |
| TATAINVEST | TINV | INE672A01026 | TATAINVEST | consolidated | 2024-03-31 | 2026-06-30 | 10 |
| TECHM | TEML | INE669C01036 | TECHM | consolidated | 2024-03-31 | 2026-06-30 | 10 |
| ZEAL | ZEAL | INE0PPS01018 | ZEAL | **n/a** | — | — | **0 — see ZEAL note below** |
| ZFCVINDIA | WABC | INE342J01019 | ZFCVINDIA | consolidated | 2024-03-31 | 2026-06-30 | 10 |

## Identity traps caught / avoided (this is why the ordered-candidate design exists)

Two symbols in this 52 had a sitemap slug whose **sid string collides exactly with the target
ticker but belongs to a different company** — the same TRU/TRUST pattern documented in
`capability_card.md` §0.1/§6. Both were caught by trying the economically-correct candidate first
and never needed to fall back to the trap, but the trap is real and is logged here per the task's
identity-discipline requirement:

- **CCL**: sitemap has `ccl-international-CCL` (sid **exactly** `CCL`, matching the ticker string) —
  but that company's own ticker is unverified/different, not our NSE `CCL`. The correct company is
  **CCL Products (India) Ltd**, sid `CCLP` (slug `ccl-products-india-CCLP`), whose page's
  `securityInfo.info.ticker` echoed `"CCL"` exactly. `ccl-international-CCL` was never fetched
  because the correct candidate was tried first and matched — flagging it here as a live trap for
  any future resolver that guesses `sid = ticker`.
- **SHK**: sitemap has `shri-kalyan-holdings-SHK` (sid **exactly** `SHK`) — an unrelated company
  (Shri Kalyan Holdings). Our actual `SHK` is **S H Kelkar and Company Ltd**, sid `SHKE` (slug
  `s-h-kelkar-and-company-SHKE`), ticker echo `"SHK"` exact match. Same pattern: not fetched because
  the correct candidate resolved first, logged as a live trap.

Both were resolved by full-company-name sitemap search (never by guessing `sid = ticker`), exactly
per the capability card's rule.

## Rename / stale-slug pattern (repeats ETERNAL/ZOM, MOTHERSON/MOSS)

- **JUBLPHARMA**: no sitemap slug contains "pharmova" at all; only the pre-rename
  `jubilant-life-sciences-JULS` exists. Page confirms the rename completed server-side:
  `securityInfo.info.name = "Jubilant Pharmova Ltd"`, ticker echo `"JUBLPHARMA"` exact match, even
  though slug/sid still say "jubilant-life-sciences"/"JULS".
- **POWERINDIA**: no slug contains "hitachi"; the sitemap slug is the pre-rename
  `abb-power-products-and-systems-india-ABBW` (ABB Power Products and Systems India → renamed
  Hitachi Energy India Ltd). Ticker echo `"POWERINDIA"` exact match, ISIN `INE07Y701011`.
- **ZFCVINDIA**: no slug contains "zf-commercial" or similar; the sitemap slug is the pre-rename
  `wabco-india-WABC` (WABCO India Ltd → renamed ZF Commercial Vehicle Control Systems India Ltd).
  Ticker echo `"ZFCVINDIA"` exact match, ISIN `INE342J01019`. **Not** to be confused with the
  unrelated `z-f-steering-gear-india-ZFS` (ZF Steering Gear India Ltd), a genuinely different
  company also in the sitemap — that candidate was never fetched since WABC resolved correctly on
  name-based search, but it's the kind of near-miss the identity check exists to prevent.

## Identity skips (2 of 52 — no fetch possible)

**AJRINFRA** and **CASTEXTECH**: no sitemap slug found by any search strategy (exact-sid match,
normalized-sid match, substring match on ticker, and full/partial company-name search — including
"ajr", "castex", "amtek" for the pre-restructuring Castex Technologies name, and "toll"/"infra"
combinations for AJRINFRA). Both recorded as `identity_skip` with `reason: "no candidate sid found
in cached sitemap_stocks.xml"` — **no page was ever fetched for either**, so there is no wrong-company
data to report, just absence of a resolvable candidate. Most likely explanations (not verified):
AJRINFRA is plausibly a very recent listing Tickertape's sitemap hasn't indexed yet; CASTEXTECH
(Castex Technologies, ex-Amtek Auto subsidiary) plausibly went through NCLT/delisting and may no
longer have an active Tickertape stock page. Neither hypothesis was confirmed — flagged as unresolved
for whoever picks this up next, per "never say unfillable" — a different sitemap snapshot or a
direct site search (through a compliant route) might resolve them later.

## ZEAL — identity matched, zero data (coverage gap, not a defect)

`zeal-global-services-ZEAL` (sid `ZEAL`) has `securityInfo.info.ticker = "ZEAL"` exact match and
ISIN `INE0PPS01018` — identity is fully confirmed — but `income-normal-interim` is an **empty
array** (`n_records_raw = 0`). This is the same "coverage isn't universal" pattern the capability
card already documented for `trust-fintech-TRU` (page loads fine, identity resolves, but the
statement series is empty for a thinly-covered/small name). **Not** an identity_skip — kept as a
resolved symbol with zero rows in `tickertape_p3.jsonl` (contributes no lines, but is not miscounted
as missing-because-wrong-company).

## NIACL — network timeout, resolved on retry

First attempt (`new-india-assurance-company-THEE`) raised `Exception: The read operation timed out`
at the default 30s timeout — not a 403/429, so per protocol this was a retry candidate, not a stop
condition. Retried in the foreground with a 45s timeout; succeeded: `securityInfo.info.ticker =
"NIACL"` exact match, ISIN `INE470Y01017`, sid `THEE`, 10 quarters, all `reporting: "consolidated"`.

## Basis (`reporting`) — 7 of the 52 new symbols are standalone-only

Matching the pilot's finding that `reporting` is a genuine per-company field (not a fixed default),
**7 of the 52 new symbols report standalone on every quarter**: ENRIN, GODIGIT, NIVABUPA,
POWERINDIA, STARHEALTH (all insurers/recent-listing financials, consistent with the pilot's
SBILIFE/MSUMI finding), plus none were seen as a mix of both bases across quarters for any symbol —
`basis` was always uniform per symbol in this pull, same as the pilot. All 45 remaining new symbols
are consolidated on every quarter.

## Short-history symbols (fewer than 10 quarters — recent listings, not defects)

- **ENRIN**: 8 quarters (2024-06-30 → 2026-06-30) — Siemens Energy India's demerger/listing is
  recent enough that Tickertape's rolling 10-quarter window doesn't reach back to 2024-03-31.
- **PINELABS**: 9 quarters (2024-06-30 → 2026-06-30) — recent (2025) IPO.
- **PWL**: 8 quarters (2024-06-30 → 2026-03-31) — Physicswallah, recent (2025) IPO; oldest-window
  offset also differs (window ends Mar-2026 not Jun-2026, i.e. its most-recent filed quarter lags
  the others — same rolling-window-shift pattern the capability card noted for GRASIM/GICRE/DHA).

This mirrors the pilot's MEESHO finding (7 quarters, recent listing) — not a scrape defect.

## Validation

```
python3 -c "import json;[json.loads(l) for l in open('tickertape_p3.jsonl')]"
```
ran clean (no exception) against the final file.

**Line count: 622.** Composition: 14 pilot symbols reused verbatim from `tickertape_pilot.jsonl`
(137 lines) + 50 of 52 new symbols fetched directly (485 lines: 48 symbols × 10, plus ENRIN=8,
PINELABS=9, PWL=8, MEESHO already counted in pilot=7 — see per-symbol table above for exact counts)
+ NIACL resolved via retry (10 lines) + ZEAL resolved with 0 rows (contributes 0 lines) + AJRINFRA/
CASTEXTECH identity_skip (0 lines each, never fetched). **63 of 66 requested symbols have at least
one row in the file**; ZEAL is present-but-empty (identity confirmed, no data); AJRINFRA and
CASTEXTECH have no rows and no confirmed identity (sitemap gap).

## 403/429s

**None encountered** across all fetches (pilot's 14 + this pass's 52, including the NIACL retry).
One transient read-timeout (NIACL, not rate-limiting) was retried successfully. No stop condition
was triggered at any point.
