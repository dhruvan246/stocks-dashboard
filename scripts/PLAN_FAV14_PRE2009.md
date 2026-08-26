# PLAN — FAV14 COVERAGE, the PRE-2009 era (2002-01 → 2008-12)

Written 2026-08-26 by the session that opened this window. Parent: `PLAN_FAV14_COVERAGE_2009.md`
(2009→date, 79.7% closed). Sibling, running concurrently: `PLAN_PRE2009_STDREV.md` (std **revenue**,
same era) and that session's std-PAT agent — **split agreed: this plan owns 2000-2001, the sibling
owns 2002-2008.**

> **Golden rule (§0, §57d):** never assume, never guess. Every number here was measured this
> session, in this file's own worked commands. A route returning nothing means THAT ROUTE has no
> row. Verdicts carry the rungs tried.

---

## 0. ★★★ READ THIS FIRST — the metric moves the WRONG WAY when you fill this era

**Filling 1,409 genuine quarters RAISED the pre-2009 FAV14 "missing" count by 349.** Measured
through the engine, same bake, my cells the only difference (`--explain`, two bakes, 2026-08-26):

| param | real coverage gained (`have`) | N/A withdrawn (`na`) | net "missing" |
|---|---|---|---|
| profitTTMStd | **+3,295** | −1,397 | −1,898 |
| profitYoyStd | **+2,703** | −1,402 | −1,301 |
| profitTTM | **+3,295** | −5,195 | **+1,900** |
| profitYoyPct | **+2,703** | −3,527 | **+824** |
| profitStreak | **+2,703** | −3,527 | **+824** |
| **total** | **+14,699** | −15,048 | **+349** |

The cause is `build_coverage_matrix.js`'s **REACH rule** (≈L849-866). For the con-blend family it
marks a cell not-applicable when the quarter the parameter reaches for is *older than any row this
symbol has* — "it predates the company's existence as this entity". In the dense 2015+ era that is
a fair inference. **In 2000-2004 it is an inference about OUR CAPTURE, not about the world** (§98,
memory: `feedback-in-frame-is-about-our-data`). Every genuine 2000-2001 quarter landed here PROVES
the company was filing, so the excuse evaporates and thousands of cells that were being written off
as impossible become visible gaps.

Three consequences, none optional:

1. **Never report this campaign by the "missing" number alone.** Report `have` — the count of
   member-months that actually carry the parameter. `missing` is `members − have − na`, and `na`
   is not a constant here.
2. **The 51,874 baseline is an UNDERESTIMATE and it will grow as the era fills.** The blend family
   still carries 13,583 pre-2009 na (was 19,139) resting on the same assumption. Filling the era
   converts that reserve into visible gaps.
3. The std family (`profitTTMStd`, `profitYoyStd`) does NOT carry the REACH rule at all — only
   `nothingPublicYet` plus the per-name ledger (≈L991-1007). That asymmetry is why std shows
   19,022 missing and the blend 6,392 **over an identical set of raw-uncovered cells** (`have` is
   the same number, 15,844, for both). Do not "fix" it by extending REACH to std: in this era that
   would write off fillable cells wholesale, for exactly the reason above.

---

## 1. The bounded problem (measured 2026-08-26 from `docs/coverage/nifty-500.json`, na-excluded)

Pre-2009 = 84 month-ends, 2002-01-31..2008-12-31, **41,375 member-months**. FAV14 missing **51,874**.
Reproduced to the unit before anything was fetched. Then `--explain` decomposes it **exactly**:

```
42,690 named missing cells  +  656 member-months with NO ROW AT ALL × 14 params (9,184)  =  51,874
```

| class | member-months | ROOT UNIT | count | owner |
|---|---|---|---|---|
| **B — std PAT quarters** | 38,699 | (symbol, quarter, std) | **4,930 root cells / 621 symbols** | this plan (2000-01) + sibling (2002-08) |
| **B2 — no fundamentals row at all** | 1,338 | whole symbol | **50 symbols** | UNOWNED |
| **A — price tape** | 9,184 | (symbol, month) | **656 mm / 89 symbols** | UNOWNED |
| **C — SHP** | 3,991 | (symbol, quarter) | 743 prior-qtr roots / 619 syms + 38 no-row syms | another session, actively |

**The blend family's root cells are a STRICT SUBSET of the std ones — 0 extra out of 596.** One std
fill closes both families. This is why B is the only fundamentals lever worth pulling.

### 1a. ⚠️ THE ENUMERATION TRAP — a window scan cannot list its own dependencies
The sibling campaign bounded this era as "1,852 open pre-2009 patStd cells / 429 symbols". That is
the count of empty patStd slots **inside 2002-2008**. But `profitTTMStd` at a 2002-01-31 month-end
needs EIGHT calendar-consecutive quarters — back to **2000-03**. Those quarters are outside the
window, so a window-scoped scan structurally never lists them, while they are precisely what the
parameter needs.

**Enumerate root cells to a FIXPOINT instead** — fill, re-derive, repeat; it converges in 6
iterations (2,967 → +1,938 → +18 → +6 → +1 → 0):

```
42,690 named missing member-months  →  4,930 root cells / 621 symbols
by quarter-year: 2000:499  2001:1353  2002:998  2003:543  2004:820  2005:329  2006:315  2007:67  2008:6
```

**953 of them are 2000-2001** — invisible to any 2002-2008 enumeration. The same trap applies to
revenue (revStd reaches back to the last announced quarter) and to `diiChgPp` (needs the PRIOR
quarter's SHP filing).

### 1b. The CEILING of class B, measured
Filling **every** one of the 4,930 root cells closes **17,697/18,366** profitTTMStd and
**8,966/9,635** profitYoyStd. The 1,338-member-month residue is class B2 — the 50 symbols with no
fundamentals row at all. **Do not promise 100%.**

---

## 2. What landed (commit `308ca8869`)

**1,409 std-PAT cells, 318 symbols, verified on origin by content** (all 1,544 proposals present,
0 mismatched). Blast radius vs origin: 1,409 rows added, **0 changed, 0 removed, 0 strays, 0
overwrites**. By quarter-year: 2000:371 · 2001:582 · 2002:218 · 2003:76 · 2004:66 · 2005:52 ·
2006:40 · 2007:3 · 2008:1.

Route, unchanged from §90/§81 — **use it, do not re-derive it**:

```bash
python3 -X utf8 scripts/agg_tools/mc_era.py       --cells cells.json --out reach.json
python3 -X utf8 scripts/agg_tools/agg_era_gate.py --cells cells.json --reach reach.json --e2b --out props.json
python3 -X utf8 scripts/agg_tools/apply_agg_pat_fills.py --props props.json            # dry
python3 -X utf8 scripts/agg_tools/apply_agg_pat_fills.py --props props.json --apply
```
`--cells` is a list of `[SYM, qeInt, "patS"]`. **The field token is `patS`, not `patStd`** —
`patStd` throws `KeyError` in `agg_gate.OTHER`.

### 2a. Measured reach and yield (do not re-sample — these are whole-population numbers)
* **MC reach**: 585 of 621 symbols resolve (94.2%); **1,840 of 2,886 first-pass gap quarters carry
  a standalone PAT = 63.8%**. A 12-symbol sample elsewhere read 27% and was called "an upper-ish
  bound" — it was a **floor**: selecting the largest-gap symbols selects for symbols whose MC feed
  starts late. A biased sample needs its DIRECTION reasoned, not just declared.
* **MC depth is per-company, not uniform.** Of 466 resolved symbols in the earlier era file, 283
  reach 1996-98 but 59 stop at 2008 and 19 at 2007; 3MINDIA's table starts 2007-03 while
  RELIANCE's starts 1997. Never quote "MC reaches 1997" as a per-symbol fact.
* **GATE E over the 4,930**: strict 902 pass; with `--e2b` **1,544**. E2b (0e81d9c76) is worth
  **+642 cells on this set alone**.
* Overlap with the sibling's 160 landed cells: **135 proposals coincide and ALL 135 AGREE, 0
  conflicts** — independent corroboration, not duplication.

### 2b. Evidence carried per landed cell
Median **85** anchors of our own stored series reproduced (min 11, max 99); nearest anchor median
**3 quarters** from the target; 807 site-exact / 737 rounded; **MC's own FY identity closes at the
TARGET FY in all 1,544**. Coarse-precision cells (NTPC 1.01, MARUTI 3.50, CGPOWER 2.38, HINDZINC
1.91) are large filers printing whole crores — `worst_anchor` EQUALS the rounding across 74-99
anchors, i.e. it is the printing convention, not error.

### 2b-i. ⚠️ A LANDED BATCH BECOMES ITS OWN ANCHOR — re-run evidence is NOT independent
`agg_era_gate.py` anchors against `docs/sf_fundamentals.json` **as it is at run time**. Once a batch
lands, a later run counts those cells as reproduced anchors: ABB 2000-03 reported "96 anchors,
nearest 4 quarters" before this batch and "103 anchors, nearest **0** quarters" after — the 0 is
ABB's own freshly-written 2000-06 cell agreeing with the site it came from. That is circular.
**Record the gate report from the run that DECIDED the cell** (`/tmp/p9/props_e2b.json` → the
ledger's `evidence`), and never re-derive an evidence claim from a post-fill run.

### 2c. The 50 LEAST-PROTECTED cells — how to find them again without a flag
`--e2b` drops the requirement that the NEIGHBOUR FYs close. It was hold-out calibrated on **stored
pre-2009 cells**, and our store holds **0 rows before 2001 and 120 in 2001** — so there is nothing
to calibrate against in 2000, and extending the calibration back would be fitting an empty set.

The exposure is smaller than that sounds, and it is **already derivable from the ledger** — no
flag needed (a flag would also be erased by any applier re-run):

* E2b-dependent (a neighbour FY IS restated): **508 of 1,409** → `fy_check.prev.verdict == "RESTATED"
  or fy_check.next.verdict == "RESTATED"` in `scripts/agg_pat_cell_fills.json`.
* No anchor within ±6q, so E1's near-veto could not fire: **151 of 1,409** (only 10.7% — the median
  2000-cell still has an anchor 5 quarters away, because earlier campaigns already填 2001-2003).
* **Both at once — the set to suspect first if anything ever contradicts: 50 cells** (2000:11,
  2001:29, 2002:3, 2003:4, 2004:2, 2005:1). Even these carry a median of 84 reproduced anchors.

### 2d. ⚠️ PROVENANCE DEFECT IN THE LEDGER — the evidence string names the WRONG GATE
`apply_agg_pat_fills.py` L220-225 builds a default `evidence` sentence that begins
*"gate A/A2 passed…"*. GATE-E proposals do not carry their own `evidence`, so **every Gate-E cell in
`agg_pat_cell_fills.json` claims a gate that never ran** — mine, the sibling's 160, and the earlier
444+750. The tool's own comment says the template "describes a gate A/A2 pass and nothing else",
which made that comment the bug's alibi (memory: `feedback-config-that-never-took-effect`).
Fixed forward in `agg_era_gate.py` (it now emits its own sentence). **Existing entries are still
mislabelled**; identify them by the presence of `fy_check` (gate A/A2 never produces it) — do not
hand-edit the minified ledger while a peer is writing to it.

---

## 3. The residue, per class, with the next rung named

### B — 3,386 root cells still open (of 4,930)
| reason | cells | what it means | next rung |
|---|---|---|---|
| REJECT-GATE-E | 2,650 | see below | — |
| NOT-FOUND | 448 | symbol resolves, MC has no such period | NSE archive (2005+) / Wayback §32 |
| UNRESOLVED | 288 | 36 symbols, MC identity not established | see §3a |

Rejection reasons (normalised): `site has no value` 622 · `FY restated on the site` ~640 ·
`disagreement within ±6q of the target` ~456 · `global disagreements` 107 · `no annual table` 82 ·
`not contiguous` 32 · `too few anchors` 13.

**Do NOT loosen the gate further to chase these.** The refusals are correct: 63MOONS FY2005 fails
because MC's own four quarters sum to 10.51 against MC's own annual of 9.91 — the discrepancy lands
exactly on the target quarter. `MAX_BAD`/`MAX_BAD_RATE` were already hold-out calibrated
(agg_era_gate.py L60-71) and the note there is explicit that the protection comes from the
near-anchor veto and the FY identity, not the global caps. The answer to a vintage refusal is an
**independent as-filed reader**, not a wider tolerance:
* **2005-2008 (604 open cells)** → NSE archive `corporates-financial-results`. Genuinely
  independent. **The sibling session owns this.**
* **2000-2004 (2,782 open cells)** → NSE archive does not reach (2005+), BSE detres does not reach
  (2008+, qid 57), and **BSE serves no pre-Oct-2018 attachments (§84)** while NSE has no
  announcement attachments before ~2012. MC/Trendlyne/Tickertape are **one vendor**
  (`feedback-aggregators-are-one-vendor`), so TL/TT add no independent voice. What is left is
  **Wayback of NSE/BSE result pages (§32)**, per-cell expensive. This is the real wall in this era
  and it should be entered deliberately, not drifted into.

### 3a. UNRESOLVED — a measured hole in `mc_era.py`'s R2 rung
36 symbols / 288 cells, all "R1 symbol + R2 ISIN both failed". **R2 is gated on our symbol being a
BSE `scrip_id`** (`isin_for()` looks up `_bse_master_all.json` by `scrip_id == sym`) — which is the
very coincidence §76 tells you to disprove. Measured: **only 5 of the 36 appear as a BSE scrip_id
at all**, so for 31 symbols R2 never had an ISIN to try and R3 (give up) fired without the rung
ever running. Several are live companies under a different key — **GESHIPPING** (NSE trades
`GESHIP`, and BOTH keys are in our store), NIIT, PRICOL, KBL, MUNJALAUTO, NAGARFERT, MONNETISPA,
ORCHIDPHAR. Next rung: supply ISIN from a source keyed by OUR symbol (`isin_sources_fetch.py`,
`_isin_seam_verdicts.json`) rather than by BSE scrip_id. Cheap and it also improves the tool for
every future era fill.

### B2 — 50 symbols with NO fundamentals row at all (1,338 member-months) — UNOWNED
SB&TINTL 49 · YOKOGAWA 35 · SQRDSFWARE 30 · WELLWININD 30 · ITCHOTEL 29 · JINDALFOTO 26 ·
COMPUDYNE 25 · GLOBLTRUST 22 · ASHKLEYFIN 17 · BALAJIDIST 17 · JPIND 17 · LAKSHAUTO 17 ·
ATLASCOPCO/MADURACOAT/PUNJCOMMU/MUKAND 15 · INDOGULF/PARKEDAVIS 14 · … (member-months each).
Delisted-era names; a whole history is needed, not a cell. Note `ITCHOTEL` here and `ITCHOTELS` in
class A are the **recycled-ticker pair** (§89 DVL/DTIL class).

### A — 656 member-months with NO PRICE ROW (×14 params = 9,184) — UNOWNED, and it caps any number either campaign reports
89 symbols. **569 of the 656 (86.7%) are Jan-Sep 2002**, and the count steps 54 → 3 between the
2002-09-30 and 2002-10-31 month-ends. Classified against the live bin's own first bars
(`build_first_bar_map.js`):

| sub-class | syms | mm | reading |
|---|---|---|---|
| tape under this key starts AFTER the whole gap | 62 | 523 | roster/era-orphan seam |
| tape starts mid-gap | 4 | 44 | same |
| no tape under this key at all | 3 | 26 | ADCINDIA, ITHL, ASIIL |
| tape covers the window (hole or 14-day freshness gate) | 20 | 63 | genuine tape hole |

The first two are **not a fetch**. The Nifty-500 roll is genuinely point-in-time (154 snapshots
before 2009, from 1998-08-01), but its symbol lists carry MODERN keys: **77 of the 405 symbols in
the 1998-08-01 snapshot have a first traded bar after that date**, some absurdly so — ITCHOTELS
2025-01-29 (the 2025 demerger, while the 2002 member's tape is under `ITCHOTEL`), SUNCLAY
2023-12-29, DIGJAMLMTD 2021-10-18, ASTERDM 2018-02-26, IDEA 2007-03-09, BAJAJ-AUTO 2008-05-26
(the 2002 member is today's BAJAJHLDNG). This is the §93/§95/§106 rename-orphan class seen from the
roster side, and **it silently caps every pre-2009 coverage number anyone reports.** It needs an
owner.

### C — SHP (3,991 mm) — another session is ACTIVELY on it; stay out
`fetch_shp_bse_aspx.py` / `fetch_shareholding.py` were dirty in the shared checkout during this
session and carry fresh commits ("1997-format parser reads share counts too: 2002-2005 goes 0% →
~85-90% at 4dp"). Root cells measured for the record: `diiChgPp` = **743 prior-quarter SHP rows /
619 symbols** (2002:424) — 2,365 mm are `prior-quarter-row-absent`, 552 `no-shp-rows`;
`fiiPct`/`diiPct` = 528 mm each, all `no-shp-rows`, 38 symbols (SHYAMTELE 45, NIPPONDENR 38,
ESSARGUJ/VARDHMNSPG/VXL 30).

---

## 4. Concurrency — this era has ≥3 sessions in it at once
* **Use a session-unique worktree suffix.** `~/stocks-wt/pre2009` was created by this session at
  12:26 and reset out from under it by a peer's agent at ~12:53, wiping an applied batch. No harm
  (everything was reproducible from the proposals file) but the generic name is the hazard.
  This campaign used `~/stocks-wt/fav14-pre2009-b2c88e`.
* `agg_pat_cell_fills.json` is **minified** — on a push conflict, reset to origin and RE-RUN the
  applier. Never merge it (memory: `feedback-minified-json-never-merge`).
* The applier is fill-only and idempotent, so two sessions proposing the same cell is safe — the
  second reports a skip. That is how the 135-cell overlap was detected and cross-validated.
