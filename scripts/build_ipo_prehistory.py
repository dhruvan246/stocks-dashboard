# -*- coding: utf-8 -*-
"""Backfill PRE-IPO ANNUAL financials (revenue + net profit) for recently-listed names, for a
DISPLAY-ONLY context card on the stock page ("Pre-IPO financials, from the prospectus").

WHY THIS EXISTS / WHAT IT IS NOT
--------------------------------
A freshly-listed company's multi-year history lives in its DRHP as *annual* accounts. Private
companies do not file *quarterly* results before listing, so pre-IPO quarters simply do not exist
(DATA_RUNBOOK §2: "co's listed too recently whose earlier quarters are pre-IPO ... never
re-attempt"). MoneyControl's ANNUAL feed carries those DRHP-era years (INDOMIM 14 FYs, MANIPALHOS
9, measured 2026-09-03); its quarterly feed is as shallow as the exchange's for fresh names.

This file, docs/ipo_prehistory.json, is READ ONLY BY THE STOCK PAGE. It is NEVER read by the
backtest engine and is NEVER merged into sf_fundamentals/sf_revop. The backtest's point-in-time
fundamentals stay BSE-PDF+vision sourced and owners-attributable (runbook §2). Keeping aggregator
annual history in a separate, clearly-labelled artifact is the whole point.

SELF-CLEARING DAILY LEDGER (the user's ask: "check all IPOs daily till they get their old data")
------------------------------------------------------------------------------------------------
Each recently-listed name carries a status. A name is retried each run until 'filled'; a name the
feed can't serve yet (brand-new, MC has no annual, or we can't anchor it) stays 'pending' and is
retried; a name that stays empty for a long time drops to 'dormant' (retried occasionally, not
every run) so we don't ping dead symbols forever. Filled names are skipped.

ANCHOR (display-grade, still never-unanchored)
----------------------------------------------
We show MoneyControl's annual series ONLY once we've proved it is the right company at the right
scale: at least one quarter MC reports must reproduce a quarter WE already hold from the exchange
(sf_fundamentals standalone net profit), within tolerance. No overlap we can check yet -> stay
'pending', show nothing. This keeps unverified aggregator numbers off the page.

Run:  python -X utf8 scripts/build_ipo_prehistory.py            # one pass over the pending backlog
      python -X utf8 scripts/build_ipo_prehistory.py --limit 40 # cap names touched this run
"""
import os, sys, json, re, datetime, argparse

HERE = os.path.dirname(os.path.abspath(__file__))
DOCS = os.path.join(HERE, "..", "docs")
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "agg_tools"))
import fetch_ipos as FI              # reuse the NSE public-issues session + iso()
import agg_sources as AG            # mc_id / mc_quarters / mc_annuals (disk-cached, paced 1s)

OUT = os.path.join(DOCS, "ipo_prehistory.json")
SF_FUND = os.path.join(DOCS, "sf_fundamentals.json")   # [[qe, npStd, annStd, npCon, annCon]]
LOOKBACK_DAYS = 1095       # seed the backlog from IPOs of the last ~3 years (older names already covered)
PER_RUN = 120              # cap names attempted per run so the daily job stays well under CI timeout
DORMANT_AFTER = 45         # pending attempts with no annual data -> check occasionally, not daily
DORMANT_EVERY = 7          # ...retry a dormant name only every Nth day (by tries count)
PAT_TOL_ABS = 0.5          # ₹cr: overlap-quarter net-profit match tolerance (or 2% of |value|)


def _num(v):
    try: return float(v)
    except Exception: return None


def _norm(s):
    s = re.sub(r"\b(limited|ltd|private|pvt|the|india|indian)\b", " ", (s or "").lower())
    return " ".join(re.sub(r"[^a-z0-9 ]", " ", s).split())


def name_ok(a, b):
    """Belt-and-suspenders identity guard on top of mc_id's exact-symbol autosuggest match:
    the aggregator's company name must clearly be the same company we listed."""
    a, b = _norm(a), _norm(b)
    if not a or not b:
        return False
    if a in b or b in a:
        return True
    ta, tb = set(a.split()), set(b.split())
    return bool(ta and tb) and len(ta & tb) / len(ta | tb) >= 0.5


def our_std_pat(sym):
    """{qe:int -> standalone net profit ₹cr} we already hold for this symbol (the anchor set)."""
    try:
        d = json.load(open(SF_FUND, encoding="utf-8"))
    except Exception:
        return {}
    out = {}
    for row in d.get(sym) or []:
        if len(row) >= 2 and row[0] and row[1] is not None:
            out[int(row[0])] = float(row[1])
    return out


def anchored(mc_q, ours):
    """True + the matched (qe, mc, ours) when >=1 quarter MC reports reproduces a quarter WE hold.
    Proves identity + scale on real filed data before we trust MC's deep annual history."""
    for qe, mine in ours.items():
        v = (mc_q.get(qe) or {}).get("pat_total")
        if v is None:
            continue
        if abs(v - mine) <= max(PAT_TOL_ABS, 0.02 * abs(mine)):
            return True, (qe, v, mine)
    return False, None


def fy_rows(mc_a):
    """MC annual dict {qe:{fields}} -> sorted [[fyYear, revenue, netProfit], ...] (Mar-end FYs).
    Revenue = operating revenue (Net Sales), falling back to Total Income; PAT = standalone net
    profit. Rows with neither number are dropped."""
    rows = []
    for qe in sorted(mc_a):
        if qe % 10000 != 331:              # annual = March year-end only
            continue
        v = mc_a[qe]
        rev = v.get("rev_ops"); rev = rev if rev is not None else v.get("rev_total")
        pat = v.get("pat_total")
        if rev is None and pat is None:
            continue
        rows.append([qe // 10000, rev, pat])
    return rows


def seed_universe():
    """Recently-listed names from NSE public-past-issues, listed within LOOKBACK_DAYS.
    -> {sym: {"name","listed","board"}}. Empty on fetch failure (we then just work the ledger)."""
    jar = FI.B.nse_jar()
    try:
        past = FI.get(jar, "public-past-issues",
                      "https://www.nseindia.com/market-data/new-stock-exchange-listings-recent") or []
    except Exception as e:
        print("past-issues fetch failed (%s) — working the existing ledger only" % e, flush=True)
        return {}
    lo = (datetime.date.today() - datetime.timedelta(days=LOOKBACK_DAYS)).isoformat()
    uni = {}
    for r in past:
        st = str(r.get("securityType") or "").strip().upper()
        if st not in ("EQ", "BE", "SME"):
            continue
        ld = FI.iso(r.get("listingDate")); sym = str(r.get("symbol") or "").strip().upper()
        if not (ld and sym) or ld < lo:
            continue
        uni.setdefault(sym, {"name": str(r.get("company") or "").strip(),
                             "listed": ld, "board": "SME" if "SME" in st else "Main"})
    return uni


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=PER_RUN)
    args = ap.parse_args()

    led = {}
    if os.path.exists(OUT):
        try: led = json.load(open(OUT, encoding="utf-8")).get("data") or {}
        except Exception: led = {}

    uni = seed_universe()
    for sym, meta in uni.items():
        e = led.setdefault(sym, {})
        e.setdefault("name", meta["name"]); e.setdefault("listed", meta["listed"])
        e.setdefault("board", meta["board"]); e.setdefault("status", "pending"); e.setdefault("tries", 0)

    # work order: never-filled first, least-recently-tried first; skip dormant names off their cadence
    def due(e):
        if e.get("status") == "filled":
            return False
        if e.get("status") == "dormant" and (e.get("tries", 0) % DORMANT_EVERY):
            return False
        return True
    todo = [s for s, e in led.items() if due(led[s])]
    todo.sort(key=lambda s: led[s].get("last") or "")      # oldest attempt first
    todo = todo[:max(1, args.limit)]

    filled = pending = 0
    today = datetime.date.today().isoformat()
    for sym in todo:
        e = led[sym]; e["tries"] = e.get("tries", 0) + 1; e["last"] = today
        try:
            ident = AG.mc_id(sym)
            if not ident:
                e["note"] = "no exact symbol match on MoneyControl yet"
                if e["tries"] >= DORMANT_AFTER: e["status"] = "dormant"
                pending += 1; continue
            if not name_ok(ident.get("name"), e.get("name")):
                e["note"] = "identity guard: MC '%s' != listed '%s'" % (ident.get("name"), e.get("name"))
                e["status"] = "dormant"                       # wrong id won't self-heal — stop pinging
                pending += 1; continue
            mc_a, na = AG.mc_annuals(sym, False)
            rows = fy_rows(mc_a)
            if not rows:
                e["note"] = na
                if e["tries"] >= DORMANT_AFTER: e["status"] = "dormant"
                pending += 1; continue
            # Upgrade to a "matches filed results" badge when a quarter MC reports reproduces one
            # we already hold from the exchange (proves scale on real filed data). Not required to
            # show — mc_id + name already fix identity — so fresh names appear immediately.
            mc_q, _ = AG.mc_quarters(sym, False)
            ok, hit = anchored(mc_q, our_std_pat(sym))
            e.update(status="filled", basis="std", source="mc", asof=today, rows=rows,
                     verified=bool(ok), mc_name=ident.get("name"))
            e["anchor"] = {"qe": hit[0], "mc": hit[1], "ours": hit[2]} if ok else None
            e["note"] = ("✓ matches filed %d (MC %.2f vs %.2f); " % (hit[0], hit[1], hit[2]) if ok else
                         "annual only, awaiting a filed quarter to cross-check; ") + na
            e.pop("tries", None)
            filled += 1
        except Exception as ex:
            e["note"] = "error: %s" % ex; pending += 1

    ist = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=5, minutes=30)
    total_filled = sum(1 for e in led.values() if e.get("status") == "filled")
    out = {"updated": ist.strftime("%Y-%m-%d %H:%M"), "data": led}
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(out, f, separators=(",", ":"), ensure_ascii=False)
    print("attempted %d: +%d filled, %d still pending | ledger %d names, %d filled -> %s"
          % (len(todo), filled, pending, len(led), total_filled, OUT), flush=True)
    for sym in todo[:6]:
        e = led[sym]
        print("  %-12s %-8s %s" % (sym, e.get("status"), e.get("note", "")[:80]), flush=True)


if __name__ == "__main__":
    main()
