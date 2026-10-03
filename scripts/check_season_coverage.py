# -*- coding: utf-8 -*-
"""
SAFETY NET for the Season Trends tab of docs/quarterly-results.html (the merged results-season
chart) — guarantees the chart never silently undercounts.

For the LIVE quarter it compares, per universe on that page (the liquid set + every NSE index, using the
SAME point-in-time membership build_results_season.py uses):
  declared = index members that have FILED results for the quarter (present in results_feed.json)
  parsed   = index members whose PAT we've actually captured (in sf_fundamentals.json)  -> what the
             chart's "reported" count shows
  missing  = declared but not parsed  -> exactly the gap that made Nifty 500 read 22 vs 25.

Writes docs/_season_coverage.json (per-index declared/parsed/missing) and PRINTS any gap so it is visible
in the nightly CI log the moment it appears — for Nifty 500 and every other index — instead of being
discovered later by eyeballing a reference site. It does NOT fail the pipeline (exit 0 always); it is a
monitor. Most gaps here are insurers awaiting the free Gemini-vision / std-only fill; a gap that persists
> ~1 day is the flag to fill by hand (INSURER_EXTRACTION_PLAYBOOK.md).

Run standalone or as a CI step after build_results_season.py.
"""
import os, json, time

HERE = os.path.dirname(os.path.abspath(__file__))
FUND    = os.path.join(HERE, "..", "docs", "sf_fundamentals.json")
FEED    = os.path.join(HERE, "..", "docs", "results_feed.json")
INDICES = os.path.join(HERE, "indices_history.json")
RENAME  = os.path.join(HERE, "_rename_map.json")
OUT     = os.path.join(HERE, "..", "docs", "_season_coverage.json")


def iso(qe):
    s = str(qe)
    return "%s-%s-%s" % (s[:4], s[4:6], s[6:8])


def snap_as_of(snaps, ymd, rename):
    """Point-in-time index members effective on/before ymd (survivorship-free) — mirrors
    build_results_season.snap_as_of exactly."""
    chosen = None
    for snp in snaps:
        if snp.get("effectiveDate", "9") <= ymd:
            chosen = snp
    return {rename.get(s, s) for s in chosen["symbols"]} if chosen else set()


def main():
    fund = json.load(open(FUND, encoding="utf-8"))
    feed = json.load(open(FEED, encoding="utf-8"))
    indices = json.load(open(INDICES, encoding="utf-8"))
    try:
        rename = json.load(open(RENAME, encoding="utf-8"))
    except Exception:
        rename = {}

    rows = feed.get("rows", [])
    if not rows:
        print("[season-coverage] empty feed — nothing to check."); return
    # live quarter = the calendar quarter (runbook §218a: it opens on its first IST day, as on the results page),
    # or a newer one somebody filed for. The PREVIOUS quarter is checked too: its late filers keep arriving for
    # weeks after the new quarter opens, and watching only the newest one let a 52-declared / 34-parsed Jun gap
    # read "OK" the day two Sep microcaps filed (§218b).
    import sys as _sys; _sys.path.insert(0, HERE)
    from build_quarterly_results import last_ended_qe, ist_today, prev_qe
    live_qe = max([last_ended_qe(ist_today())] + [r[3] for r in rows if isinstance(r[3], int)
                                                  and r[3] % 10000 in (331, 630, 930, 1231) and r[3] < int(ist_today().strftime("%Y%m%d"))])

    def check(qe):
        declared_all = {r[0] for r in rows if r[3] == qe}
        parsed_all = set()
        for s, frows in fund.items():
            for r in frows:
                if r[0] == qe and (r[1] is not None or r[3] is not None):
                    parsed_all.add(s); break
        report, total_missing = {}, set()
        for index, snaps in indices.items():
            if not isinstance(snaps, list) or not snaps:
                continue
            members = snap_as_of(snaps, iso(qe), rename)
            if not members:
                continue
            declared = members & declared_all
            parsed = members & parsed_all
            missing = sorted(declared - parsed)
            report[index] = {"declared": len(declared), "parsed": len(parsed), "missing": missing}
            total_missing |= set(missing)
        return report, total_missing

    report, total_missing = check(live_qe)
    pq = prev_qe(live_qe)
    preport, pmissing = check(pq)
    json.dump({"generated": int(time.time()), "qe": live_qe, "indexes": report,
               "prev": {"qe": pq, "indexes": preport}},
              open(OUT, "w", encoding="utf-8"), ensure_ascii=False, separators=(",", ":"))

    # ---- CI-visible report ----
    for qe, rep, tot in ((live_qe, report, total_missing), (pq, preport, pmissing)):
        print("[season-coverage] quarter %s%s" % (iso(qe), " (live)" if qe == live_qe else " (previous — late filers)"))
        gaps = {k: v for k, v in rep.items() if v["missing"]}
        if not gaps:
            print("[season-coverage] OK — every index's declared filings are parsed into the chart.")
            continue
        print("[season-coverage] GAP — declared-but-unparsed members (chart undercounts until filled):")
        for k in sorted(gaps, key=lambda k: -len(gaps[k]["missing"])):
            v = gaps[k]
            print("   %-24s %d declared / %d parsed  MISSING: %s"
                  % (k, v["declared"], v["parsed"], ", ".join(v["missing"])))
        print("[season-coverage] union of missing names (%d): %s" % (len(tot), ", ".join(sorted(tot))))
    print("[season-coverage] wrote %s" % os.path.normpath(OUT))


if __name__ == "__main__":
    main()
