#!/usr/bin/env python3
"""World indices that no automatic source answers for (runbook §250b): built from daily bars read BY HAND off
TradingView charts in a logged-in browser, into the same docs/world/<CODE>.json + _index.json that
fetch_world_indices.py writes. NOT refreshed by any CI job — each series ends on the day of the last pull and the
page says so ("not refreshed automatically").

Raw files (outside the repo, default ~/stocks-backups/world_tv/TV_<EXCHANGE>_<SYMBOL>.txt), one per index:
  <symbol asked>~<symbol the chart resolved>~<TradingView description>~<exchange timezone, '.' for '/'>~<pull time UTC>~<rows>
  rows joined by 'n':  YYYYMMDD_open_high_low_close   (date = the bar's time in the exchange's own timezone)

Rules (same as fetch_world_indices.py, nothing estimated):
  * A bar dated the pull's own day (exchange-local) is dropped unless the pull happened at or after 20:00 local —
    the session may have been open and TradingView's exchange data is delayed.
  * A bar whose open, high, low and close are all equal, or whose high/low do not contain open and close, keeps
    its CLOSE only.
  * Never shrink: a raw file with fewer bars than 98% of the published series is refused.
The nightly fetch_world_indices.py still asks Yahoo for these codes; it keeps the series here as it is unless Yahoo
starts answering with at least 98% as many daily bars.

Usage: python3 scripts/build_world_tv.py [--raw DIR] [--out DIR]
"""
import argparse, datetime as dt, json, os, sys
from zoneinfo import ZoneInfo

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import fetch_world_indices as W

# raw file key -> code in fetch_world_indices.WORLD
TV = {
    "SP_IMV": "MERVAL", "GPW_WIG20": "WIG20", "BET_BUX": "BUX", "RUS_IRUS": "MOEX", "SET_SET": "SET",
    "PSE_PSEI": "PSEI", "PSX_KSE100": "KSE100", "CSELK_ASI": "CSEALL", "TADAWUL_TASI": "TASI",
    "EGX_EGX30": "EGX30", "DFM_DFMGI": "DFM", "QSE_GNRI": "QSI",
}


def read(path):
    asked, chart, desc, tz, pulled, body = open(path, encoding="ascii").read().split("~", 5)
    desc = desc.replace("%20", " ")
    when = dt.datetime.strptime(pulled[:17], "%Y-%m-%dT%H%M%S").replace(tzinfo=dt.timezone.utc).astimezone(ZoneInfo(tz.replace(".", "/")))
    today, late = when.strftime("%Y%m%d"), when.hour >= 20
    rows, seen = [], set()
    for r in body.split("n"):
        p = r.split("_")
        if len(p) != 5 or p[4] == "x":
            continue
        d = p[0]
        if d > today or (d == today and not late):
            continue
        assert d not in seen and (not rows or d > rows[-1][5]), "%s: date %s repeated or out of order" % (path, d)
        seen.add(d)
        c = float(p[4])
        try:
            o, h, l = float(p[1]), float(p[2]), float(p[3])
        except ValueError:
            o = h = l = None
        if o is not None and ((o == h == l == c) or min(o, h, l) <= 0 or h < max(o, c) - 1e-9 or l > min(o, c) + 1e-9):
            o = h = l = None
        if c > 0:
            rows.append(("%s-%s-%s" % (d[:4], d[4:6], d[6:]), o, h, l, c, d))
    return asked, chart, desc, when, [r[:5] for r in rows]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", default=os.path.expanduser("~/stocks-backups/world_tv"))
    ap.add_argument("--out", default=os.path.join(HERE, "..", "docs", "world"))
    a = ap.parse_args()
    ip = os.path.join(a.out, "_index.json")
    index = json.load(open(ip))
    ser = index["ser"]
    codes = [w[0] for w in W.WORLD]
    meta = {w[0]: w for w in W.WORLD}
    for key, code in TV.items():
        p = os.path.join(a.raw, "TV_%s.txt" % key)
        if not os.path.exists(p):
            print("%-7s no raw file, left as it is" % code); continue
        asked, chart, desc, when, rows = read(p)
        old = ser.get(code) or {}
        if old and not old.get("manual"):
            print("%-7s already published from %s — left as it is" % (code, old.get("src"))); continue
        if len(rows) < 250 or len(rows) < 0.98 * old.get("n", 0):
            print("%-7s REFUSED: %d bars (published %d)" % (code, len(rows), old.get("n", 0))); continue
        _, _, label, country, alias = meta[code]
        with open(os.path.join(a.out, code + ".json"), "w") as fh:
            json.dump(W.pack(code, label, rows), fh, separators=(",", ":"))
        ser[code] = {"f": code, "label": label, "country": country,
                     "src": "TradingView %s (%s), read by hand %s" % (asked, desc, when.strftime("%Y-%m-%d")),
                     "manual": True, "first": rows[0][0], "last": rows[-1][0], "ord": codes.index(code), "n": len(rows),
                     "close_only": sum(1 for r in rows if r[1] is None), "ohl_from": next((r[0] for r in rows if r[1] is not None), None),
                     "alias": alias}
        print("%-7s %-14s %6d bars %s -> %s  close-only %d  last close %.2f  (%s)" % (
            code, asked, len(rows), rows[0][0], rows[-1][0], ser[code]["close_only"], rows[-1][4], desc))
    index["updated"] = dt.datetime.now(dt.timezone(dt.timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %H:%M IST")
    with open(ip + ".tmp", "w") as fh:
        json.dump(index, fh, separators=(",", ":"), ensure_ascii=False, sort_keys=True)
    os.replace(ip + ".tmp", ip)
    print("world index now lists %d series" % len(ser))


if __name__ == "__main__":
    main()
