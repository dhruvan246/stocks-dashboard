# -*- coding: utf-8 -*-
"""Recover EVERY split/bonus factor baked into the published price bins and check each against the
official records (DATA_RUNBOOK §161e). Read-only: writes a report, never touches a ledger or a bin.

Why it works: update_sf_data / build_sf_data rescale close/high/low/open/VWAP on an adjustment but
NEVER the traded turnover (t, Rs lakh) or volume (v, shares). So per bar the raw VWAP is t*1e5/v and
    cum = vw_adjusted / (t*1e5/v)
is the product of every factor applied AFTER that bar. A persistent level shift in `cum` across a
boundary is exactly the factor divided out there (RELIANCE's 1997/2009/2017/2024 bonuses and its
2006/2023 demergers come back exact).

Kept: shifts within 1.5% of a canonical CA fraction whose implied RAW move lies outside [0.75,1.30]
(the only moves ca_factor() inference ever touched) with both closes >= 0.25 (self_heal's precision
floor). Each is classified against corp_actions(+hist) / demerger_adj / rights_terp /
ca_open_arbitrated / MANUAL_RIGHTS / phantom_crashes / LEGACY_FALSE_CA, then — for events with no
record — against Yahoo's split-adjusted closes (docs/stock_data.bin) as an independent second reader
and the §87c ex-day open gate. Evidence columns only; verdicts stay with a human (§0 no assumptions).

Run:  python3 scripts/audit_applied_factors.py <dir with sf_deep_*.bin + sf_recent_*.bin> [out.json]
      (the bins are the sf-data Pages repo: git clone --depth 1 https://github.com/dhruvan246/sf-data)
"""
import os, sys, re, ast, gzip, json, glob, bisect, datetime, statistics, collections

HERE = os.path.dirname(os.path.abspath(__file__)); ROOT = os.path.dirname(HERE)
CA = [1/2, 1/3, 2/3, 1/4, 3/4, 1/5, 2/5, 3/5, 1/6, 5/6, 1/8, 1/10, 1/20, 1/50, 2., 3., 4., 5., 10.]
WIN = 4          # bars either side for the level-shift median
PX_FLOOR = 0.25  # same floor as update_sf_data.self_heal


def od(y): return datetime.date(y // 10000, y // 100 % 100, y % 100)


def load_bins(d):
    ser = {}
    files = sorted(glob.glob(os.path.join(d, "sf_deep_*.bin"))) + sorted(glob.glob(os.path.join(d, "sf_recent_*.bin")))
    if not files: sys.exit("no sf_deep_*/sf_recent_* bins in %s" % d)
    for f in files:
        D = json.loads(gzip.open(f).read())
        for s, o in D["data"].items():
            e = ser.setdefault(s, {})
            for i, x in enumerate(o["d"]):
                e[x] = (o["c"][i], o["t"][i], o["v"][i], (o.get("vw") or [None] * len(o["d"]))[i],
                        (o.get("op") or [None] * len(o["d"]))[i])
        print("loaded %s (%d symbols)" % (os.path.basename(f), len(D["data"])), flush=True)
    return ser


def applied_events(ser):
    out = []
    for s, e in ser.items():
        cum = []
        for x in sorted(e):
            c, t, v, vw, _ = e[x]
            if t and v and vw and t > 0 and v > 0 and vw > 0:
                cum.append((x, vw / (t * 1e5 / v), c))
        for i in range(len(cum) - 1):
            a, b = cum[i][1], cum[i + 1][1]
            f = a / b if b else 1
            if abs(f - 1) <= 0.03 or not (1e-3 < f < 1e3): continue
            pre = [y[1] for y in cum[max(0, i - WIN + 1):i + 1]]; post = [y[1] for y in cum[i + 1:i + 1 + WIN]]
            if len(pre) < 2 or (len(post) < 2 and i + 1 != len(cum) - 1): continue
            F = statistics.median(pre) / statistics.median(post)
            if not any(abs(F / q - 1) <= 0.015 for q in CA) or abs(F / f - 1) >= 0.05: continue
            ca, cb = cum[i][2], cum[i + 1][2]
            if not ca or min(ca, cb) < PX_FLOOR: continue
            raw = F * cb / ca
            if 0.75 <= raw <= 1.30: continue
            out.append({"sym": s, "a": cum[i][0], "b": cum[i + 1][0], "F": round(F, 4), "raw": round(raw, 4),
                        "adj_a": ca, "adj_b": cb, "open_b": e[cum[i + 1][0]][4]})
    return out


def ledgers():
    off = collections.defaultdict(list); noa = collections.defaultdict(set)
    for f in ("corp_actions.json", "corp_actions_hist.json"):
        C = json.load(open(os.path.join(HERE, f)))
        for s, v in C.get("factors", {}).items():
            for x in v: off[s].append((int(x[0]), float(x[1])))
        for s, v in C.get("noadjust", {}).items():
            for x in v: noa[s].add(int(x))
    led = collections.defaultdict(set)
    for x in json.load(open(os.path.join(HERE, "demerger_adj.json"))): led[x[0]].add(("demerger_adj", int(x[1])))
    for x in json.load(open(os.path.join(HERE, "rights_terp.json"))): led[x[0]].add(("rights_terp", int(x[1])))
    for x in json.load(open(os.path.join(HERE, "ca_open_arbitrated.json")))["events"]: led[x[0]].add(("ca_open_arbitrated", int(x[1])))
    src = open(os.path.join(HERE, "update_sf_data.py")).read()
    m = re.search(r"MANUAL_RIGHTS = (\[.*?\n\])", src, re.S)
    for t in ast.literal_eval(re.sub(r"#.*", "", m.group(1))): led[t[0]].add(("MANUAL_RIGHTS", int(t[1])))
    pc = collections.defaultdict(set)
    for s, v in json.load(open(os.path.join(HERE, "phantom_crashes.json"))).items():
        for x in v: pc[s].add(int(x))
    m = re.search(r"LEGACY_FALSE_CA = (\[.*?\n\])", src, re.S)
    for s, x in ast.literal_eval(re.sub(r"#.*", "", m.group(1))): pc[s].add(x)
    return off, noa, led, pc


def yahoo():
    Y = json.loads(gzip.open(os.path.join(ROOT, "docs", "stock_data.bin")).read()); ts = Y["startTs"]; out = {}
    for k, s in Y["series"].items():
        if k.endswith(".NS"):
            out[k[:-3]] = ([int(datetime.datetime.utcfromtimestamp(ts + o * 86400).strftime("%Y%m%d")) for o in s["d"]],
                           [p / 100 for p in s["p"]])
    return out


def main():
    if len(sys.argv) < 2: sys.exit(__doc__)
    outp = sys.argv[2] if len(sys.argv) > 2 else os.path.join(HERE, "_audit_applied_factors.json")
    ev = applied_events(load_bins(sys.argv[1]))
    off, noa, led, pc = ledgers(); Y = yahoo()
    near = lambda d, a, b, pad=7: od(a) - datetime.timedelta(days=pad) <= od(d) <= od(b) + datetime.timedelta(days=pad)
    for r in ev:
        s, a, b = r["sym"], r["a"], r["b"]
        r["official"] = [x for x in off[s] if near(x[0], a, b)]
        r["noadjust"] = sorted(x for x in noa[s] if near(x, a, b))
        r["ledger"] = sorted(x for x in led[s] if near(x[1], a, b))
        r["crash_listed"] = sorted(x for x in pc[s] if near(x, a, b))
        r["class"] = ("OFFICIAL" if r["official"] else "LEDGER" if r["ledger"] else
                      "CRASH_LISTED_STILL_ADJUSTED" if r["crash_listed"] else
                      "DEMERGER_BUT_ADJUSTED" if r["noadjust"] else "NO_RECORD")
        # §87c open gate on the drop bar: (raw open / raw prev) / F
        r["open_gate"] = round((r["open_b"] / r["adj_a"]), 4) if r.get("open_b") and r["adj_a"] else None
        y = Y.get(s)
        if not y: r["yahoo"] = "no-series"; continue
        d, p = y; i = bisect.bisect_right(d, a) - 1; j = bisect.bisect_left(d, b)
        if i < 0 or j >= len(d) or not p[i] or not p[j]: r["yahoo"] = "no-bar"; continue
        ry = p[j] / p[i]; r["yahoo_ratio"] = round(ry, 4)
        r["yahoo"] = ("shows-raw-move" if abs(ry / r["raw"] - 1) <= 0.12 else
                      "agrees-with-adjustment" if abs(ry / (r["adj_b"] / r["adj_a"]) - 1) <= 0.12 else "neither")
    json.dump({"generated": datetime.datetime.utcnow().isoformat() + "Z", "events": ev}, open(outp, "w"), indent=0)
    c = collections.Counter((r["class"], r.get("yahoo")) for r in ev)
    print("%d applied factors in the inference domain; by (class, yahoo):" % len(ev))
    for k, v in sorted(c.items(), key=lambda x: -x[1]): print("  %5d  %s" % (v, k))
    print("report -> %s" % outp)


if __name__ == "__main__":
    main()
