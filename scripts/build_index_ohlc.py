#!/usr/bin/env python3
"""Daily OPEN/HIGH/LOW/CLOSE history of every NSE index, for the candle view and full daily history on docs/chart.html.

SOURCE  NSE's all-indices daily file (scripts/fetch_index_ohlc.py has the notes): one row per index per day.
        Nothing exists before 2012-02-21, and an index only has rows from the day NSE started publishing it there.

NAMES   NSE renamed most indices twice (S&P CNX Nifty -> CNX Nifty -> Nifty 50). Two names are joined into one
        series ONLY on this evidence, never on the look of the name:
          the new name's first row on day D satisfies  close - "Points Change" == the old name's close on the
          previous trading day (to the paisa, non-zero), and the old name has no row on day D.
        Every join made is written to _index.json ("joins") so it can be read back and checked.

KEYS    Each series is published under the name docs/indices_hist.json uses for it (NIFTY SMLCAP 100 ...), found by
        VALUE: the series whose closes equal that key's closes on the latest common dates (>= 3 dates, all equal).
        A series with no such match is published under its NSE name in upper case.

OUTPUT  docs/idx_ohlc/<SLUG>.json  {sv, key, names, d0, dd, o, h, l, c}
          d0 = first date (ISO); dd = day gaps between consecutive bars; o/h/l/c = value x100 (null where NSE
          published '-'). Bars are as published — nothing is filled, estimated or repaired.
        docs/idx_ohlc/_index.json   {sv, updated, idx:{key:{f, cur, first, last, n}}, joins:[...]}

MODES   --full [--cache DIR]   rebuild everything from the raw cache (default ~/stocks-backups/index_ohlc_raw)
        --update [--days N]    CI: fetch the last N (default 10) days straight from NSE and append new bars to the
                               published files. A day that 404s is skipped and picked up by a later run.
"""
import argparse, collections, csv, datetime as dt, glob, gzip, io, json, os, re, sys, time, urllib.error, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
DOCS = os.path.join(HERE, "..", "docs")
OUT = os.path.join(DOCS, "idx_ohlc")
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
URL = "https://nsearchives.nseindia.com/content/indices/ind_close_all_%s.csv"
SCHEMA = 1


def slug(key):
    return re.sub(r"[^A-Za-z0-9]+", "_", key).strip("_")


def num(x):
    x = (x or "").strip().replace(",", "")
    try:
        return float(x)
    except ValueError:
        return None


def parse(raw):
    """raw CSV text -> {name: (o, h, l, c, change)}"""
    out = {}
    for r in list(csv.reader(io.StringIO(raw)))[1:]:
        if len(r) < 7 or not r[0].strip():
            continue
        out[r[0].strip()] = (num(r[2]), num(r[3]), num(r[4]), num(r[5]), num(r[6]))
    return out


def x100(v):
    return None if v is None else int(round(v * 100))


def is_join(new_row, old_prev_close):
    c, ch = new_row[3], new_row[4]
    return c is not None and ch is not None and old_prev_close and abs(old_prev_close - (c - ch)) < 0.011


def hist_keys():
    """indices_hist.json: {key: {date: close}}"""
    p = os.path.join(DOCS, "indices_hist.json")
    if not os.path.exists(p):
        return {}
    out = collections.defaultdict(dict)
    for d, row in json.load(open(p)).get("daily", {}).items():
        for k, v in row.items():
            if isinstance(v, (int, float)) and v > 0:
                out[k][d] = v
    return out


def write(series, joins):
    """series: {key: {'cur': name, 'names': [...], 'bars': {iso: (o,h,l,c)}}}"""
    os.makedirs(OUT, exist_ok=True)
    idx, seen = {}, {}
    for key in sorted(series):
        s = series[key]
        days = sorted(d for d, b in s["bars"].items() if b[3] is not None)
        if not days:
            continue
        f = slug(key)
        if f in seen:
            sys.exit("ABORT: file name collision %r: %s and %s" % (f, seen[f], key))
        seen[f] = key
        dd = [(dt.date.fromisoformat(days[i]) - dt.date.fromisoformat(days[i - 1])).days for i in range(1, len(days))]
        cols = [[x100(s["bars"][d][j]) for d in days] for j in range(4)]
        blob = {"sv": SCHEMA, "key": key, "names": s["names"], "d0": days[0], "dd": dd,
                "o": cols[0], "h": cols[1], "l": cols[2], "c": cols[3]}
        with open(os.path.join(OUT, f + ".json"), "w", encoding="utf-8") as fh:
            json.dump(blob, fh, separators=(",", ":"))
        idx[key] = {"f": f, "cur": s["cur"], "first": days[0], "last": days[-1], "n": len(days)}
    for f in glob.glob(os.path.join(OUT, "*.json")):
        b = os.path.basename(f)[:-5]
        if b != "_index" and b not in seen:
            os.remove(f)
    with open(os.path.join(OUT, "_index.json"), "w", encoding="utf-8") as fh:
        json.dump({"sv": SCHEMA, "updated": dt.datetime.now().strftime("%Y-%m-%d %H:%M IST"), "idx": idx, "joins": joins},
                  fh, separators=(",", ":"))
    return idx


def full(cache):
    files = sorted(glob.glob(os.path.join(cache, "*.csv.gz")))
    if len(files) < 3000:
        sys.exit("ABORT: only %d raw days in %s — run scripts/fetch_index_ohlc.py first" % (len(files), cache))
    days, S = [], collections.defaultdict(dict)
    for f in files:
        iso = os.path.basename(f)[:10]
        raw = gzip.open(f, "rt", errors="replace").read()
        rows = list(csv.reader(io.StringIO(raw)))
        D = dt.date.fromisoformat(iso)
        # NSE wrote the date three ways: dd-mm-yyyy, dd/mm/yyyy (62 files in 2014) and mm-dd-yyyy (2023-04-06/10/11 —
        # checked 2026-11-24: the Nifty 50 close in each of those three equals docs/nifty.json for the FILE's date)
        want = {D.strftime("%d-%m-%Y"), D.strftime("%d/%m/%Y"), D.strftime("%m-%d-%Y")}
        got = {r[1].strip() for r in rows[1:] if len(r) > 5}
        if not got <= want:
            sys.exit("ABORT: %s holds rows dated %s" % (f, sorted(got - want)[:3]))
        days.append(iso)
        for n, row in parse(raw).items():
            S[n][iso] = row
    di = {d: i for i, d in enumerate(days)}
    # union names on the previous-close evidence
    parent = {n: n for n in S}

    def find(a):
        while parent[a] != a:
            parent[a] = parent[parent[a]]; a = parent[a]
        return a
    joins = []
    for n in S:
        for d in sorted(S[n]):
            i = di[d]
            if i == 0 or days[i - 1] in S[n]:
                continue
            p = days[i - 1]
            c = [m for m in S if m != n and p in S[m] and d not in S[m] and is_join(S[n][d], S[m][p][3])]
            if len(c) == 1:
                joins.append({"on": d, "new": n, "old": c[0], "prev_close": S[c[0]][p][3]})
                parent[find(n)] = find(c[0])
            elif len(c) > 1:
                print("AMBIGUOUS, not joined: %s on %s <- %s" % (n, d, c))
    groups = collections.defaultdict(list)
    for n in S:
        groups[find(n)].append(n)
    hk = hist_keys()
    series, clash, used = {}, 0, {}
    for g in groups.values():
        bars = {}
        for n in sorted(g, key=lambda n: max(S[n])):       # the name seen most recently wins a same-day overlap
            for d, row in S[n].items():
                if d in bars and bars[d][3] != row[3]:
                    clash += 1
                bars[d] = row[:4]
        cur = max(g, key=lambda n: max(S[n]))
        key = None
        for k, closes in hk.items():
            common = sorted(set(closes) & set(bars))[-5:]
            if len(common) >= 3 and all(bars[d][3] is not None and abs(bars[d][3] - closes[d]) < 0.006 for d in common):
                key = k; break
        key = key or cur.upper()
        if key in used:
            sys.exit("ABORT: two series claim key %r: %s and %s" % (key, used[key], cur))
        used[key] = cur
        series[key] = {"cur": cur, "names": sorted(g, key=lambda n: min(S[n])), "bars": bars}
    idx = write(series, joins)
    matched = sum(1 for k in idx if k in hk)
    print("index ohlc: %d series from %d names over %d days (%s -> %s); %d joins; %d same-day clashes between joined names"
          % (len(idx), len(S), len(days), days[0], days[-1], len(joins), clash))
    print("keys: %d matched to indices_hist.json by value, %d under their NSE name; indices_hist keys with no series: %s"
          % (matched, len(idx) - matched, sorted(set(hk) - set(idx))))


def load():
    """the published files back into {key: {'cur','names','bars'}} + the index meta"""
    ip = os.path.join(OUT, "_index.json")
    if not os.path.exists(ip):
        sys.exit("ABORT: %s missing — run --full once first" % ip)
    meta = json.load(open(ip))
    series = {}
    for key, m in meta["idx"].items():
        b = json.load(open(os.path.join(OUT, m["f"] + ".json")))
        d = dt.date.fromisoformat(b["d0"]); ds = [d]
        for g in b["dd"]:
            d += dt.timedelta(days=g); ds.append(d)
        f = lambda v: None if v is None else v / 100.0
        series[key] = {"cur": m["cur"], "names": b["names"],
                       "bars": {ds[i].isoformat(): (f(b["o"][i]), f(b["h"][i]), f(b["l"][i]), f(b["c"][i])) for i in range(len(ds))}}
    return meta, series


def merge_ni(nidir, report=None):
    """Add niftyindices.com's per-index history (Historical Data page; runbook §28 — a browser page-context pull,
    one <KEY>.txt per index: rows 'YYYYMMDD_o_h_l_c' joined by 'n', 'x' = not published) to the published files.

    The daily all-indices file stays the master for every day it has. This only (a) adds the days BEFORE an index
    appears in that file (2012-02-21 at the earliest) and the 12 days whose file NSE never published, and
    (b) fills open/high/low where the daily file printed '-'. A row whose open = high = low = close is NSE's
    back-computed close-only history, so its open/high/low are stored as missing, never as a flat candle.
    Every day both sources have is compared; differing closes are counted and listed, nothing is overwritten."""
    meta, series = load()
    before = {k: m["n"] for k, m in meta["idx"].items()}
    added = filled = same = 0
    diffs, nofile = [], []
    for key, s in sorted(series.items()):
        fp = os.path.join(nidir, slug(key) + ".txt")
        if not os.path.exists(fp):
            nofile.append(key); continue
        for row in open(fp).read().split("n"):
            p = row.split("_")
            if len(p) != 5 or len(p[0]) != 8:
                continue
            iso = "%s-%s-%s" % (p[0][:4], p[0][4:6], p[0][6:])
            o, h, l, c = [None if x == "x" else float(x) for x in p[1:]]
            if c is None or c <= 0:
                continue
            if o is None or h is None or l is None or (o == h == l == c):
                o = h = l = None
            have = s["bars"].get(iso)
            if have is None:
                s["bars"][iso] = (o, h, l, c); added += 1
                continue
            if abs(have[3] - c) > 0.011:
                diffs.append((key, iso, have[3], c))
            else:
                same += 1
                if (have[0] is None or have[1] is None or have[2] is None) and o is not None:
                    s["bars"][iso] = (o, h, l, have[3]); filled += 1
    idx = write(series, meta.get("joins", []))
    shrunk = [k for k in before if idx.get(k, {}).get("n", 0) < before[k]]
    if shrunk:
        sys.exit("ABORT: series lost bars: %s" % shrunk[:5])
    print("niftyindices merge: %d days added, %d open/high/low filled, %d overlapping closes equal, %d differ; %d series had no history file"
          % (added, filled, same, len(diffs), len(nofile)))
    if report:
        json.dump({"diffs": diffs, "nofile": nofile}, open(report, "w"), indent=0)


def update(ndays):
    meta, series = load()
    bycur = {s["cur"]: k for k, s in series.items()}
    joins, added, today = meta.get("joins", []), 0, dt.date.today()
    for back in range(ndays, -1, -1):
        d = today - dt.timedelta(days=back); iso = d.isoformat()
        try:
            req = urllib.request.Request(URL % d.strftime("%d%m%Y"), headers={"User-Agent": UA})
            raw = urllib.request.urlopen(req, timeout=40).read().decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            if e.code != 404:
                print("%s: HTTP %s" % (iso, e.code))
            continue
        except Exception as e:
            print("%s: %s" % (iso, e)); continue
        if not raw.lstrip().startswith("Index Name"):
            print("%s: not the index CSV" % iso); continue
        rows = parse(raw)
        for n, row in rows.items():
            key = bycur.get(n)
            if key is None:
                # a name never seen: a rename (previous-close evidence, old name gone today) or a brand-new index
                c = [k for k, s in series.items() if s["cur"] not in rows and s["bars"] and iso > max(s["bars"])
                     and is_join(row, s["bars"][max(s["bars"])][3])]
                if len(c) == 1:
                    key = c[0]; old = series[key]["cur"]
                    joins.append({"on": iso, "new": n, "old": old, "prev_close": series[key]["bars"][max(series[key]["bars"])][3]})
                    del bycur[old]; series[key]["cur"] = n; series[key]["names"].append(n)
                    print("RENAME %s -> %s (%s)" % (old, n, key))
                else:
                    key = n.upper()
                    if key in series:
                        print("SKIP %s on %s: key %s already belongs to %s" % (n, iso, key, series[key]["cur"])); continue
                    series[key] = {"cur": n, "names": [n], "bars": {}}
                    print("NEW index %s" % n)
                bycur[n] = key
            if iso not in series[key]["bars"]:
                added += 1
            series[key]["bars"][iso] = row[:4]
        time.sleep(0.4)
    before = {k: m["n"] for k, m in meta["idx"].items()}
    idx = write(series, joins)
    shrunk = [k for k in before if idx.get(k, {}).get("n", 0) < before[k]]
    if shrunk:
        sys.exit("ABORT: series lost bars: %s" % shrunk[:5])
    print("index ohlc update: %d series, %d new bars, latest %s" % (len(idx), added, max(m["last"] for m in idx.values())))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--full", action="store_true"); ap.add_argument("--update", action="store_true")
    ap.add_argument("--cache", default=os.path.expanduser("~/stocks-backups/index_ohlc_raw"))
    ap.add_argument("--days", type=int, default=10)
    ap.add_argument("--merge-ni", default="", help="dir of niftyindices history files to merge into the published files")
    ap.add_argument("--report", default="", help="with --merge-ni: write the differing cells here")
    a = ap.parse_args()
    if a.merge_ni and not (a.full or a.update):
        merge_ni(a.merge_ni, a.report)
    elif a.full == a.update:
        sys.exit("pick one of --full / --update / --merge-ni DIR")
    elif a.full:
        full(a.cache)
        print("NOTE: --full rebuilds from the daily files only — re-run --merge-ni <dir> to put the pre-2012 history back")
    else:
        update(a.days)
