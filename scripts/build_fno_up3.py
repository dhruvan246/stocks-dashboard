# -*- coding: utf-8 -*-
"""
F&O STOCKS UP MORE THAN 3% — one count per trading day, 2020-01-01 -> date, point-in-time.

Output: docs/fno_up3.json (read by docs/fno-up3.html, an owner-only Admin page)

  { "updated":"YYYY-MM-DD HH:MM IST", "from":"2020-01-01", "threshold":3,
    "dates":[YYYYMMDD ...],
    "nMem":[...], "nObs":[...], "up":[...],
    "list":["SYM:7.72 SYM:6.45 ...", ...],   # members up > 3% that day, biggest first
    "noPx":{"YYYYMMDD":"SYM SYM"},           # members with no EQ/BE/BZ bar that day (delisted/merged)
    "isin":{"SYM":"INE..."} }                 # last ISIN seen per symbol (rename carry state)

METHOD
  - Membership: THAT DAY's own F&O list = every stock with stock futures/options in NSE's F&O
    bhavcopy for that day, under the ticker that traded then -- so names since delisted, merged or
    renamed are in it on their days (survivorship-free), and mid-month entries/exits land on the
    exact day. Kept in scripts/fno_daily_roster.json as a base list + per-day adds/drops. (The
    monthly scripts/fno_history.json snapshots were measured against the daily files 2026-11-04:
    13 of 60 sampled days off by 1-10 stocks, because NSE adds and drops names mid-month.)
  - Day move: NSE CM bhavcopy CLOSE / PREVCLOSE - 1 (series EQ, BE or BZ). NSE's PREVCLOSE is
    already adjusted on a split/bonus ex-date, so a split is not a fake move. "Up > 3%" is strict.
  - A roster name renamed after its snapshot (LTI -> LTIM in the same month) has no bar under the
    old ticker: it is matched to the ticker now trading its last-seen ISIN. A member that merged
    away (HDFC, IDFC, MINDTREE) has no bar at all and is listed in noPx, not counted.
  - Incremental: appends every trading day after the last one in the file. NSE files are fetched
    from nsearchives (UDiFF CM + FO); BHAV_CACHE=<dir of YYYYMMDD.csv.gz> reads CM files from a
    local cache instead (both NSE formats), which is how the 2020 -> 2026-11 backfill was built.
    A day whose CM file exists but whose FO file is not out yet stops the run there (never a
    guessed roster); the next run picks it up.

Run:  python3 -X utf8 scripts/build_fno_up3.py
      BHAV_CACHE=~/stocks-cache/nse_bhav/full FULL=1 python3 -X utf8 scripts/build_fno_up3.py
"""
import csv, datetime as dt, gzip, io, json, os, sys, time, urllib.error, urllib.request, zipfile

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
ROSTER = os.path.join(HERE, "fno_daily_roster.json")
OUT = os.path.join(ROOT, "docs", "fno_up3.json")
START = dt.date(2020, 1, 1)
THRESH = 3.0
SERIES = ("EQ", "BE", "BZ")
import special_sessions   # weekend trading sessions (Diwali Muhurat …) — runbook §236
CACHE = os.path.expanduser(os.environ.get("BHAV_CACHE", ""))
URL = "https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{}_F_0000.csv.zip"
FO_URL = "https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_{}_F_0000.csv.zip"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/130.0 Safari/537.36")


def fetch(day, url=URL):
    """CSV text of the day's CM (or FO) bhavcopy; None when NSE has no file (holiday / not out yet)."""
    if CACHE and url == URL:
        p = os.path.join(CACHE, day.strftime("%Y%m%d") + ".csv.gz")
        return gzip.open(p, "rt").read() if os.path.exists(p) else None
    err = None
    for attempt in range(3):
        try:
            req = urllib.request.Request(url.format(day.strftime("%Y%m%d")),
                                         headers={"User-Agent": UA, "Referer": "https://www.nseindia.com/"})
            z = zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(req, timeout=60).read()))
            return z.read(z.namelist()[0]).decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            err = e
        except Exception as e:
            err = e
        time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"{day}: NSE fetch failed: {err}")


def rows(txt):
    """(symbol, series, close, prevclose, isin) from either NSE CM bhavcopy format."""
    for r in csv.DictReader(io.StringIO(txt)):
        if "TckrSymb" in r:
            yield r["TckrSymb"], r["SctySrs"], r["ClsPric"], r["PrvsClsgPric"], r["ISIN"]
        else:
            yield (r["SYMBOL"].strip(), r["SERIES"].strip(), r["CLOSE"], r["PREVCLOSE"],
                   (r.get("ISIN") or "").strip())


def fo_underlyings(txt):
    """Stock F&O underlyings (futures or options) in a UDiFF FO bhavcopy; index contracts excluded."""
    return {r["TckrSymb"].strip() for r in csv.DictReader(io.StringIO(txt)) if r["FinInstrmTp"] in ("STF", "STO")}


class Roster:
    """scripts/fno_daily_roster.json: {"base": [...], "first": d8, "last": d8, "ev": {d8: {"+": [...], "-": [...]}}}."""
    def __init__(self):
        self.R = json.load(open(ROSTER))
        self.days = sorted(self.R["ev"])
        self.dirty = False

    def on(self, d8):
        if d8 < self.R["first"]:
            sys.exit(f"{d8}: before the first daily F&O roster ({self.R['first']}) -- refusing to guess one")
        mem = set(self.R["base"])
        for d in self.days:
            if d > d8:
                break
            mem |= set(self.R["ev"][d].get("+", [])); mem -= set(self.R["ev"][d].get("-", []))
        return mem

    def extend(self, d8, und):
        prev = self.on(self.R["last"])
        add, drop = sorted(und - prev), sorted(prev - und)
        if add or drop:
            self.R["ev"][d8] = {k: v for k, v in (("+", add), ("-", drop)) if v}
            self.days = sorted(self.R["ev"])
        self.R["last"] = d8
        self.dirty = True

    def save(self):
        if self.dirty:
            with open(ROSTER + ".tmp", "w", encoding="utf-8") as f:
                json.dump(self.R, f, separators=(",", ":"), sort_keys=True)
            os.replace(ROSTER + ".tmp", ROSTER)


def empty():
    return {"dates": [], "nMem": [], "nObs": [], "up": [], "list": [],
            "noPx": {}, "isin": {}}


def main():
    ros = Roster()
    D = empty()
    if os.path.exists(OUT) and not os.environ.get("FULL"):
        D.update(json.load(open(OUT)))
    day = (dt.datetime.strptime(str(D["dates"][-1]), "%Y%m%d").date() + dt.timedelta(1)) if D["dates"] else START
    today = dt.date.today()
    added = []
    while day <= today:
        if day.weekday() < 5 or CACHE or day in special_sessions.DATES:   # NSE holds the odd Saturday/Sunday session (budget, muhurat; §236)
            txt = fetch(day)
            if txt:
                iso, d8s = day.isoformat(), day.strftime("%Y%m%d")
                if d8s > ros.R["last"]:
                    fo = fetch(day, FO_URL)
                    und = fo_underlyings(fo) if fo else set()
                    if len(und) < 50:
                        print(f"{iso}: CM file is out but the F&O file is not ({len(und)} underlyings) -- stopping here; next run continues")
                        break
                    ros.extend(d8s, und)
                mem = ros.on(d8s)
                allpx, by_isin = {}, {}
                for s, ser, c, p, isin in rows(txt):
                    if ser not in SERIES or s in allpx:
                        continue
                    try:
                        c, p = float(c), float(p)
                    except ValueError:
                        continue
                    if p > 0 and c > 0:
                        allpx[s] = (c / p - 1) * 100
                        if isin:
                            by_isin[isin] = s
                if len(allpx) < 500:
                    sys.exit(f"{iso}: only {len(allpx)} priced lines -- not a real CM bhavcopy, stopping")
                px, nopx = {}, []
                for s in sorted(mem):
                    t = s if s in allpx else by_isin.get(D["isin"].get(s))
                    if t and t not in px:
                        px[t] = allpx[t]
                    elif not t:
                        nopx.append(s)
                for isin, s in by_isin.items():
                    D["isin"][s] = isin
                up = sorted((s for s, v in px.items() if v > THRESH), key=lambda s: -px[s])
                d8 = int(d8s)
                D["dates"].append(d8)
                D["nMem"].append(len(mem))
                D["nObs"].append(len(px))
                D["up"].append(len(up))
                D["list"].append(" ".join(f"{s}:{px[s]:.2f}" for s in up))
                if nopx:
                    D["noPx"][str(d8)] = " ".join(nopx)
                added.append((iso, len(up), len(px)))
        day += dt.timedelta(1)
    # keep only ISINs of symbols that have been on a roster (the rename carry needs nothing else)
    keep = set(ros.R["base"]) | {s for e in ros.R["ev"].values() for s in e.get("+", [])}
    D["isin"] = {s: v for s, v in sorted(D["isin"].items()) if s in keep}
    ros.save()
    if not added:
        print("no new trading day -- file unchanged")
        return
    D["updated"] = dt.datetime.utcnow().replace(microsecond=0).__add__(dt.timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:%M IST")
    D["from"] = START.isoformat()
    D["threshold"] = THRESH
    D["source"] = ("NSE CM bhavcopy close vs previous close; F&O list = that day's NSE F&O bhavcopy "
                   "stock underlyings (scripts/fno_daily_roster.json)")
    order = ["updated", "from", "threshold", "source", "dates", "nMem", "nObs", "up", "list", "noPx", "isin"]
    with open(OUT + ".tmp", "w", encoding="utf-8") as f:
        json.dump({k: D[k] for k in order}, f, separators=(",", ":"))
    os.replace(OUT + ".tmp", OUT)
    print(f"added {len(added)} day(s): first {added[0]} last {added[-1]} -> {len(D['dates'])} days in file")


if __name__ == "__main__":
    main()
