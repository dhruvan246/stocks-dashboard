# -*- coding: utf-8 -*-
"""Nifty Microcap 250 — official daily level (runbook §227; the level half of fetch_nse_sme_emerge.py, §210).

  level    nsearchives.nseindia.com/content/indices/ind_close_all_DDMMYYYY.csv, row "Nifty Microcap 250" — close,
           P/E, P/B, dividend yield. First file carrying the row: 11-May-2021 (measured 2026-10-10 over the cached
           files 2017-11-20 →). The date inside the row must equal the file's day (stale re-served holiday files).
  members  NOT here: the index is one of the Nifty indices in scripts/indices_history.json, rebuilt weekly by
           build_changelog.py + build_membership_v2.py (refresh-membership.yml) — from NSE's list
           ind_niftymicrocap250_list.csv walked back through the press releases.

OUTPUT  docs/nifty_microcap250.json   {updated, source, px:{date: close}, pe:{}, pb:{}, dy:{}}

  python3 scripts/fetch_nifty_microcap250.py           (from 7 days before the last stored session)
  python3 scripts/fetch_nifty_microcap250.py --full    (whole history 2021-05-11 →; ~1,350 files, cached)
"""
import os, sys, datetime
HERE = os.path.dirname(os.path.abspath(__file__)); sys.path.insert(0, HERE)
import fetch_nse_sme_emerge as E

OUT = os.path.join(os.path.dirname(HERE), "docs", "nifty_microcap250.json")
FIRST = datetime.date(2021, 5, 11)
ROW = "NIFTY MICROCAP 250"


def main(full=False):
    cur = E.load(OUT, {}) or {}
    px, pe, pb, dy = (dict(cur.get(k) or {}) for k in ("px", "pe", "pb", "dy"))
    today = E.ist_today()
    d = FIRST if (full or not px) else datetime.date.fromisoformat(max(px)) - datetime.timedelta(days=7)
    got = 0
    while d <= today:
        row = E.level_row(d, ROW)              # every calendar day: NSE holds weekend special sessions
        if row:
            k = d.isoformat()
            px[k] = round(row[0], 2)
            for dst, v in ((pe, row[1]), (pb, row[2]), (dy, row[3])):
                if v is not None:
                    dst[k] = v
            got += 1
        d += datetime.timedelta(days=1)
    if len(px) < 1300:                         # 11-May-2021 → Oct-2026 ≈ 1,350 sessions
        raise SystemExit("level history too short (%d) — refusing to write" % len(px))
    E.dump(OUT, {"updated": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                 "source": "NSE ind_close_all_DDMMYYYY.csv row Nifty Microcap 250 (first file carrying it: 11-May-2021)",
                 "px": dict(sorted(px.items())), "pe": dict(sorted(pe.items())), "pb": dict(sorted(pb.items())),
                 "dy": dict(sorted(dy.items()))}, compact=True)
    print("level: %d session(s) read, %d stored, last %s = %s" % (got, len(px), max(px), px[max(px)]))


if __name__ == "__main__":
    main(full="--full" in sys.argv)
