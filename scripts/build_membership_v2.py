# -*- coding: utf-8 -*-
"""
FINAL point-in-time index membership builder (all 7 backtest indexes).

Sources, in order of authority:
  1. Parsed NSE reconstitution press releases (_changelog.json) — exact effective
     dates for every captured add/drop, 2015-2026.
  2. Archived official full lists (_wb_n500_snaps.json, Nifty 500 only) — pinned
     as hard checkpoints; reconstruction is forced exact at those dates.
  3. Today's official NSE constituent CSVs — the anchor each walk starts from.
  4. Old scrapbook (indices_history.json) — kept ONLY for dates before the
     earliest accurate event (deep history fallback).

Symbols are converted to the ERA-CORRECT ticker (the symbol that actually traded
on that date) using symchg.csv + a supplement, so membership matches the
survivorship-free bhavcopy price series keys of that period.

Writes: scripts/indices_history.json  +  docs/stock_data.bin (indicesHistory).
Run: python -X utf8 build_membership_v2.py
"""
import os, re, csv, json, gzip, time, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__)); ROOT = os.path.dirname(HERE)
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

SLUGS = {  # index display name -> NSE current-list slug — ALL 27 tracked indexes
    "Nifty 50": "nifty50", "Nifty Next 50": "niftynext50", "Nifty 100": "nifty100",
    "Nifty 200": "nifty200", "Nifty 500": "nifty500",
    "Nifty Midcap 50": "niftymidcap50", "Nifty Midcap 100": "niftymidcap100", "Nifty Midcap 150": "niftymidcap150",
    "Nifty Smallcap 50": "niftysmallcap50", "Nifty Smallcap 100": "niftysmallcap100", "Nifty Smallcap 250": "niftysmallcap250",
    "Nifty LargeMidcap 250": "niftylargemidcap250", "Nifty MidSmallcap 400": "niftymidsmallcap400",
    "Nifty Bank": "niftybank", "Nifty IT": "niftyit", "Nifty Pharma": "niftypharma", "Nifty Auto": "niftyauto",
    "Nifty FMCG": "niftyfmcg", "Nifty Metal": "niftymetal", "Nifty Energy": "niftyenergy", "Nifty Realty": "niftyrealty",
    "Nifty Media": "niftymedia", "Nifty Healthcare": "niftyhealthcare", "Nifty Consumer Durables": "niftyconsumerdurables",
    "Nifty Oil & Gas": "niftyoilgas", "Nifty PSU Bank": "niftypsubank", "Nifty MNC": "niftymnc",
}

# ---------- renames (old -> new, with the date the NEW symbol started) ----------
MON = {m: i for i, m in enumerate(["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"], 1)}
def dmy_iso(s):
    m = re.match(r"(\d{1,2})-([A-Z]{3})-(\d{4})", s.strip().upper())
    return f"{int(m.group(3)):04d}-{MON[m.group(2)]:02d}-{int(m.group(1)):02d}" if m and m.group(2) in MON else None

def load_renames():
    ren = {}  # old -> (new, date)
    try:
        sc = os.path.join(HERE, "symchg.csv")                       # repo copy first (works in CI)
        if not os.path.exists(sc): sc = os.path.join(os.path.dirname(ROOT), "symchg.csv")
        for r in csv.reader(open(sc, encoding="utf-8", errors="replace")):
            if len(r) >= 4 and r[1].strip() and r[2].strip():
                d = dmy_iso(r[3]) if r[3].strip() else None
                ren[r[1].strip().upper()] = (r[2].strip().upper(), d or "1900-01-01")
    except Exception as e:
        print("(symchg.csv not loaded:", e, ")")
    # supplement: recent renames missing from symchg.csv; date resolved from SF price data below
    for old, new in [("GMRINFRA","GMRAIRPORT"), ("GET&D","GVT&D"), ("HBLPOWER","HBLENGINE"),
                     ("AKZOINDIA","JSWDULUX"), ("SWANENERGY","SWANCORP"), ("MFL","EPIGRAL"),
                     ("GLS","ALIVUS"), ("ADANITRANS","ADANIENSOL"), ("MOTHERSUMI","MOTHERSON"),
                     ("MAHINDCIE","CIEINDIA")]:   # MAHINDCIE 2026-08-11: in LargeMidcap250 history; bin key merged into CIEINDIA (orphan-series batch)
        ren.setdefault(old, (new, None))
    return ren

def resolve_supplement_dates(ren):
    """date of a rename ~= first trading day of the NEW symbol in the SF data."""
    try:
        D = json.loads(gzip.decompress(open(os.path.join(ROOT, "docs", "sf_stock_data.bin"), "rb").read()))
        first = {sym: str(o["d"][0]) for sym, o in D["data"].items() if o["d"]}
        for old, (new, d) in list(ren.items()):
            if d is None:
                f = first.get(new)
                ren[old] = (new, f"{f[:4]}-{f[4:6]}-{f[6:]}" if f else "2099-01-01")
    except Exception as e:
        print("(SF data not loaded for rename dates:", e, ")")
        for old, (new, d) in list(ren.items()):
            if d is None: ren[old] = (new, "2099-01-01")
    return ren

REN = resolve_supplement_dates(load_renames())          # old -> (new, date)
FWD = {}                                                # canonical resolution old->latest
def canon(s):
    seen = set()
    while s in REN and s not in seen: seen.add(s); s = REN[s][0]
    return s
BACK = {}                                               # new -> (old, date)
for o, (n, d) in REN.items(): BACK.setdefault(n, (o, d))
def era_symbol(c, date):
    """era-correct ticker for canonical symbol c at `date` (walk rename chain backward)."""
    seen = set()
    while c in BACK and c not in seen:
        old, d = BACK[c]
        if date < d: seen.add(c); c = old
        else: break
    return c

# ---------- fetch today's official lists ----------
def get(url, tries=5):
    last = None
    for _ in range(tries):
        try: return urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=60).read()
        except Exception as e: last = e; time.sleep(3)
    raise last
def today_list(slug):
    raw = get(f"https://archives.nseindia.com/content/indices/ind_{slug}list.csv")
    if raw[:2] == b"\x1f\x8b": raw = gzip.decompress(raw)
    rows = list(csv.reader(raw.decode("utf-8", "ignore").splitlines()))
    si = rows[0].index("Symbol") if "Symbol" in rows[0] else 2
    return sorted({r[si].strip() for r in rows[1:] if len(r) > si and r[si].strip()})

# ---------- reconstruction ----------
def merge_same_eff(events):
    """combine events sharing an effective date (fixes the overwrite bug)."""
    by = {}
    for c in events:
        e = by.setdefault(c["eff"], {"eff": c["eff"], "excluded": [], "included": []})
        e["excluded"] += c["excluded"]; e["included"] += c["included"]
    return [by[k] for k in sorted(by)]

def reconstruct(anchor_today, events, checkpoints=None):
    """Backward walk from today's anchor; returns {eff: set(canonical members)}.
       checkpoints {date: set} are pinned exact afterwards."""
    ev = merge_same_eff(events)
    snaps = {}
    m = {canon(s) for s in anchor_today}
    for c in reversed(ev):
        inc = {canon(x) for x in c["included"]}; exc = {canon(x) for x in c["excluded"]}
        snaps[c["eff"]] = set(m)         # membership in force FROM c.eff
        m = (m - inc) | exc              # roll back to before this event
    snaps["1900-01-01"] = set(m)         # pre-changelog baseline
    if checkpoints:
        for d, S in checkpoints.items(): snaps[d] = {canon(x) for x in S}
    return snaps

def checkpoint_continuity(snaps, checkpoints, events):
    """Close the holes the BACKWARD walk cannot.

    reconstruct() only restores a past member when the changelog records the exclusion that
    removed them, so every exclusion the press-release parser missed makes the reconstruction
    decay monotonically into the past: 501 members at 2018-10-04 down to 485 at 2015-03-27,
    with no archived full list anywhere between 2015-03-25 and 2018-10-04 to pin it back.
    Symptom to look for: adds/removes balanced at every transition except a few where removes
    exceed adds, after which the deficit never recovers (YESBANK, in the Nifty 50 at the time,
    sat outside the reconstructed Nifty 500 for three years).

    Invariant applied: a symbol present at BOTH surrounding pinned checkpoints and never named
    in an exclusion between them was a member for the whole interval. A genuine leave-and-rejoin
    IS recorded as an exclusion, so requiring "never excluded" preserves those real gaps — this
    fills only the holes the changelog itself cannot account for.
    """
    if not checkpoints:
        return 0
    cps = sorted(d for d in checkpoints if d in snaps)
    dates = sorted(snaps)
    ev = merge_same_eff(events)
    added = 0
    for a, b in zip(cps, cps[1:]):
        both = snaps[a] & snaps[b]
        gone = set()
        for c in ev:
            if a < c["eff"] <= b:
                gone |= {canon(x) for x in c["excluded"]}
        stayed = both - gone
        for d in dates:
            if a < d < b:
                miss = stayed - snaps[d]
                if miss:
                    snaps[d] |= miss
                    added += len(miss)
    return added


def validate_n500(snaps, wb):
    def asof(d):
        best = None
        for k in sorted(snaps):
            if k <= d: best = k
        return snaps[best]
    print("  validation vs archived official lists (canonical symbols):")
    worst = 100.0
    for d in sorted(wb):
        off = {canon(x) for x in wb[d]}; rec = asof(d)
        pct = 100 * len(off & rec) / len(off)
        print("    %s  match %.1f%%  off-by %d" % (d, pct, len(off ^ rec)))
        worst = min(worst, pct)
    return worst

def main():
    changelog = json.load(open(os.path.join(HERE, "_changelog.json")))
    wb = json.load(open(os.path.join(HERE, "_wb_n500_snaps.json")))
    # Official archived sub-index constituent CSVs (ground truth) pinned as hard
    # checkpoints for the 8 broad tiers, exactly like wb does for Nifty 500. Keys
    # are Wayback YYYYMMDD (or "LIVE") -> convert to ISO; LIVE == today's anchor, skip.
    try:
        _off = json.load(open(os.path.join(HERE, "_idx_official_snaps.json")))
    except Exception:
        _off = {}
    OFFICIAL = {}
    for _tier, _snaps in _off.items():
        OFFICIAL[_tier] = {("%s-%s-%s" % (d[:4], d[4:6], d[6:8])): set(v)
                           for d, v in _snaps.items() if d != "LIVE"}
    hist_path = os.path.join(HERE, "indices_history.json")
    H = json.load(open(hist_path, encoding="utf-8"))
    # old->current map from the PRICE build (build_sf_data merges renamed series by ISIN). Membership
    # must key on the SAME current tickers the merged price series uses, else renamed stocks vanish
    # from historical backtests. This supersedes the old era_symbol() (which deliberately used the OLD
    # ticker to match the previously-split price series — no longer how the price data is keyed).
    try:
        RENAME = json.load(open(os.path.join(HERE, "_rename_map.json")))
    except Exception:
        RENAME = {}; print("(_rename_map.json not found — membership keeps era symbols)")
    def to_current(s):
        seen = set()
        while s in RENAME and s not in seen: seen.add(s); s = RENAME[s]
        return s

    for idx, slug in SLUGS.items():
        events = changelog.get(idx, [])
        if not events:
            print(f"{idx}: no change events — left as-is"); continue
        anchor = today_list(slug)
        if idx == "Nifty 500":
            cps = {d: set(v) for d, v in wb.items()}
            # Moneycontrol-derived soft checkpoints for the 2007-13 dark windows (15 dates;
            # code-resolved + evidence-adjudicated, see PRE2015_CAMPAIGN.md STEP M2 and
            # _mc_code_supplement.json). Official wb lists win on any same-date collision.
            try:
                for _d, _v in json.load(open(os.path.join(HERE, "_mc_n500_snaps.json"))).items():
                    cps.setdefault(_d, set(_v))
            except FileNotFoundError:
                pass
        else:
            cps = OFFICIAL.get(idx) or None   # official archived CSVs pinned exact
        snaps = reconstruct(anchor, events, cps)
        # Nifty 500 only: its checkpoints are dense enough for the invariant to be safe, and it
        # is the index whose backward walk provably decays (see checkpoint_continuity). The
        # fixed-size sub-indexes are already pinned to official CSVs at J=0.997 and adding to
        # them risks pushing a 50-name tier over its size check.
        if idx == "Nifty 500":
            n = checkpoint_continuity(snaps, cps, events)
            print(f"  continuity repair: restored {n} member-slots between pinned checkpoints")
        if idx == "Nifty 500":
            worst = validate_n500(snaps, wb)
            if worst < 99.0:   # SAFETY GATE: never overwrite good membership with a degraded rebuild
                raise SystemExit("ABORT: Nifty500 validation %.1f%% < 99%% — refusing to write "
                                 "(likely a missing input or NSE fetch issue); keeping committed data." % worst)
        # era-correct symbols per snapshot date so they match that period's bhavcopy series
        new_snaps = [{"effectiveDate": d, "symbols": sorted(set(to_current(s) for s in S))}
                     for d, S in snaps.items() if d != "1900-01-01"]
        new_snaps.sort(key=lambda x: x["effectiveDate"])
        earliest = new_snaps[0]["effectiveDate"]
        kept_old = [s for s in H.get(idx, []) if s["effectiveDate"] < earliest]
        H[idx] = sorted(kept_old + new_snaps, key=lambda s: s["effectiveDate"])
        print(f"{idx}: {len(kept_old)} scrapbook (pre-{earliest}) + {len(new_snaps)} accurate = {len(H[idx])} snapshots")

    # --- PHANTOM FLOOR (listing-date, 2026-07-09) ---------------------------------------------------
    # The backward-walk over-extends a stock into snapshots BEFORE it actually joined Nifty 500 whenever
    # its ENTRY event is missing from the changelog (entered via a sub-index add the N500 list didn't
    # restate, or a COVID-deferred / unparsed reconstitution). The UNAMBIGUOUS, symbol-safe correction:
    # a stock cannot be an index constituent before it started trading. Drop any N500 snapshot entry whose
    # stock has NO price history on-or-before that snapshot's date (catches pre-listing phantoms like
    # SUMICHEM shown in 2017 / RBA / POLYCAB / HDFCAMC pre-IPO). We do NOT floor already-listed names
    # (NESTLEIND, ALKYLAMINE) — that needs true membership dates we don't reliably have, so it's left as-is
    # rather than risk false drops off the incomplete pre-2020 archived lists. sf series merge pre-rename
    # history by ISIN, so first-trade-date == the company's real listing (ETERNAL == Zomato 2021, etc.).
    import gzip as _gz
    try:
        _sf = json.loads(_gz.decompress(open(os.path.join(ROOT, "docs", "sf_stock_data.bin"), "rb").read()))["data"]
    except Exception as _e:
        _sf = None; print("  PHANTOM FLOOR skipped (no sf_stock_data.bin):", _e)
    if _sf is not None and "Nifty 500" in H:
        first_trade = {s: str(o["d"][0]) for s, o in _sf.items() if o.get("d")}   # YYYYMMDD
        dropped = {}
        for snap in H["Nifty 500"]:
            if snap["effectiveDate"] < "2011-01-01": continue   # pre-dataset — data coverage incomplete
            dint = snap["effectiveDate"].replace("-", "")
            keep = []
            for sym in snap["symbols"]:
                ft = first_trade.get(sym)
                if ft is not None and ft > dint:                # not yet trading => impossible => phantom
                    dropped.setdefault(sym, []).append(snap["effectiveDate"])
                else:
                    keep.append(sym)
            snap["symbols"] = keep
        json.dump({k: v for k, v in sorted(dropped.items())},
                  open(os.path.join(HERE, "_phantom_dropped.json"), "w"), indent=0)
        print("  PHANTOM FLOOR (listing): dropped %d pre-listing rows across %d stocks (see _phantom_dropped.json)"
              % (sum(len(v) for v in dropped.values()), len(dropped)))

    # --- DERIVE the cleanly-partitionable sub-indices from the VALIDATED Nifty 500 -----------------
    # NSE methodology: Nifty 500 = Nifty 100 (+) Midcap 150 (+) Smallcap 250 (disjoint by mcap rank),
    # and MidSmallcap 400 = Nifty 500 - Nifty 100. Nifty 500 is validated 100% vs archived lists and
    # Nifty 100 is exact, so deriving these two RECOVERS members the sparse per-index press-release
    # reconstruction missed (mainly 2020-21 delisted small-caps that archive.org — blocked here —
    # would otherwise be needed to pin). Smallcap 250 goes from ~233 (missing 19) to ~250.
    def _asof(idx, d):
        best = None
        for s in H.get(idx, []):
            if s["effectiveDate"] <= d: best = s
        return set(best["symbols"]) if best else set()
    def _derive(name, fn, basis):
        # Official archived CSVs are ground truth — pin them exact; derive only between.
        offd = {d: sorted({canon(x) for x in v}) for d, v in OFFICIAL.get(name, {}).items()}
        dates = sorted({s["effectiveDate"] for bi in basis for s in H.get(bi, [])} | set(offd))
        out = []
        for d in dates:
            syms = offd.get(d)                    # official wins where present
            if not syms:
                # A set-difference is only meaningful once every subtrahend index has
                # history: with e.g. Nifty 100 still empty, N500 - {} would emit a
                # full-N500 placeholder row for a tier that didn't exist yet (these
                # launched Apr-2016). No snapshot at all => _asof correctly returns
                # empty there.
                if any(not _asof(bi, d) for bi in basis[1:]):
                    continue
                syms = sorted(fn(d))
            if not syms or (out and out[-1]["symbols"] == syms):
                continue
            out.append({"effectiveDate": d, "symbols": syms})
        if out:
            H[name] = out
            print(f"  derived {name}: {len(out)} snapshots ({len(offd)} official-pinned, rest from {' - '.join(basis)})")
    _derive("Nifty MidSmallcap 400",
            lambda d: _asof("Nifty 500", d) - _asof("Nifty 100", d),
            ["Nifty 500", "Nifty 100"])
    _derive("Nifty Smallcap 250",
            lambda d: _asof("Nifty 500", d) - _asof("Nifty 100", d) - _asof("Nifty Midcap 150", d),
            ["Nifty 500", "Nifty 100", "Nifty Midcap 150"])

    # --- MANUAL MEMBERSHIP ADDS (survive every rebuild) -------------------------------------------
    # Verified members the press-release/checkpoint reconstruction can't carry. Applied AFTER the
    # derives so they land ONLY in the listed index. Each: (index, symbol, from_date, to_date).
    #   TATAMTRDVR — Tata Motors DVR. StockView counts the DVR as a Nifty-500 constituent; NSE's
    #   constituent CSVs list only the ordinary share, so the reconstruction shows it in just ~8
    #   semi-annual snapshots and it flickers in/out. It traded continuously until its merger
    #   record date (last trade 2024-08-29) -> hold it in every Nifty 500 snapshot in range.
    #   First added in cf46cc9 as a direct data patch; that was WIPED by the next rebuild (this
    #   script regenerates indicesHistory wholesale), so it now lives here. 2026-07-04.
    MANUAL_ADDS = [("Nifty 500", "TATAMTRDVR", "2018-10-04", "2024-08-29")]
    for m_idx, m_sym, m_from, m_to in MANUAL_ADDS:
        n = 0
        for s in H.get(m_idx, []):
            if m_from <= s["effectiveDate"] <= m_to and m_sym not in s["symbols"]:
                s["symbols"] = sorted(s["symbols"] + [m_sym]); n += 1
        print(f"  MANUAL ADD {m_sym} -> {m_idx} [{m_from}..{m_to}]: patched {n} snapshots")

    json.dump(H, open(hist_path, "w", encoding="utf-8"), separators=(",", ":"))
    print(f"\nWrote {hist_path}")
    binp = os.path.join(ROOT, "docs", "stock_data.bin")
    D = json.loads(gzip.decompress(open(binp, "rb").read()))
    D["indicesHistory"] = {**D.get("indicesHistory", {}), **{k: H[k] for k in SLUGS if k in H}}
    open(binp, "wb").write(gzip.compress(json.dumps(D, separators=(",", ":")).encode(), 6))
    print(f"Wrote {binp} ({os.path.getsize(binp)/1048576:.1f} MB)")

    # spot checks
    n5 = H["Nifty 500"]
    def asof(d):
        best = None
        for s in n5:
            if s["effectiveDate"] <= d and (not best or s["effectiveDate"] > best["effectiveDate"]): best = s
        return set(best["symbols"])
    print("\nSpot checks (Nifty 500):")
    print("  2023-03-30 SCI (should be True - pre-reshuffle):", "SCI" in asof("2023-03-30"))
    print("  2023-03-31 SCI (should be False - new list live):", "SCI" in asof("2023-03-31"))
    print("  2022-10-09 AWL (Sept-22 add, was broken before):", "AWL" in asof("2022-10-09"))
    print("  2022-06-01 GMRINFRA era-symbol (not GMRAIRPORT):", "GMRINFRA" in asof("2022-06-01"), "/", "GMRAIRPORT" in asof("2022-06-01"))

if __name__ == "__main__":
    main()
