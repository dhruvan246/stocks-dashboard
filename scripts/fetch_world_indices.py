#!/usr/bin/env python3
"""World stock indices (one or more per country) for docs/chart.html — daily open/high/low/close, full history.

Source: Yahoo Finance chart API, one call per index, the whole daily history Yahoo holds
  https://query1.finance.yahoo.com/v8/finance/chart/<symbol>?period1=<1950>&period2=<now>&interval=1d
Every index in WORLD below was probed on 2026-11-24; an index Yahoo does not answer for is simply not
written (and is listed in the run's output) — nothing is invented.

Output: docs/world/<CODE>.json and docs/world/_index.json, the same shape as docs/xseries (build_xseries.py):
  file   {sv, key, label, d0, dd:[day gaps], o, h, l, c}   prices x100 as integers, null = not published
  index  {sv, updated, ser:{CODE:{f,label,country,src,first,last,n,ohl_from,alias:[…]}}}

Rules (each one measured, none guessed):
  * A bar's DATE is the exchange's own calendar date: Yahoo's timestamp is the session open in UTC, so the
    exchange's UTC offset (meta.gmtoffset) is added first. Without it Sydney's bars land on the previous day.
  * A bar still being traded is dropped: when the run happens inside the exchange's regular session
    (meta.currentTradingPeriod.regular), the last bar is today's unfinished one.
  * A bar with no close is skipped. A bar whose open/high/low is missing, zero, or does not contain the close
    (high < close or low > close) keeps its CLOSE only — the page draws that day at its close and says so.
  * Never shrink: if Yahoo answers with fewer than 98% of the bars the existing file has, or the fetch fails,
    the existing file is kept as it is.

Runs every evening in .github/workflows/refresh-macro.yml (after fetch_macro.py). stdlib only; shares fetch_macro.py's HTTP helper.
Usage: python3 scripts/fetch_world_indices.py [--out DIR] [--only CODE,CODE]
"""
import argparse, datetime as dt, json, os, sys, time, urllib.parse

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import build_fundamentals as B   # _get / UA — the one HTTP client the repo already uses for Yahoo (fetch_macro.py)
SCHEMA = 1

# code, Yahoo symbol, label, country, other codes that mean the same series (TradingView-style names included)
WORLD = [
    # --- United States
    ("SPX", "^GSPC", "S&P 500", "United States", ["SP500", "US500", "GSPC"]),
    ("NASDAQ", "^IXIC", "Nasdaq Composite", "United States", ["IXIC", "NASDAQCOMPOSITE"]),
    ("NDX", "^NDX", "Nasdaq 100", "United States", ["US100", "NASDAQ100"]),
    ("DJI", "^DJI", "Dow Jones Industrial Average", "United States", ["US30", "DOW", "DOWJONES"]),
    ("RUT", "^RUT", "Russell 2000", "United States", ["RUSSELL2000", "US2000"]),
    ("NYA", "^NYA", "NYSE Composite", "United States", ["NYSECOMPOSITE"]),
    ("VIX", "^VIX", "CBOE Volatility Index (VIX)", "United States", ["USVIX"]),
    # --- Americas
    ("TSX", "^GSPTSE", "S&P/TSX Composite", "Canada", ["GSPTSE", "CANADA"]),
    ("IBOV", "^BVSP", "Bovespa", "Brazil", ["BOVESPA", "BVSP", "BRAZIL"]),
    ("MEXBOL", "^MXX", "S&P/BMV IPC", "Mexico", ["IPC", "MXX", "MEXICO"]),
    ("MERVAL", "^MERV", "S&P Merval", "Argentina", ["MERV", "ARGENTINA"]),
    ("IPSA", "^IPSA", "S&P IPSA", "Chile", ["CHILE"]),
    # --- Europe
    ("FTSE", "^FTSE", "FTSE 100", "United Kingdom", ["UKX", "UK100", "FTSE100"]),
    ("FTSE250", "^FTMC", "FTSE 250", "United Kingdom", ["MCX250", "FTMC"]),
    ("DAX", "^GDAXI", "DAX", "Germany", ["DE40", "GER40", "GDAXI", "GERMANY"]),
    ("MDAX", "^MDAXI", "MDAX", "Germany", ["MDAXI"]),
    ("CAC40", "^FCHI", "CAC 40", "France", ["CAC", "PX1", "FRA40", "FCHI", "FRANCE"]),
    ("STOXX50", "^STOXX50E", "Euro Stoxx 50", "Euro area", ["SX5E", "EU50", "EUROSTOXX50", "STOXX50E"]),
    ("STOXX600", "^STOXX", "Stoxx Europe 600", "Europe", ["SXXP", "STOXX"]),
    ("AEX", "^AEX", "AEX", "Netherlands", ["NL25", "NETHERLANDS"]),
    ("SMI", "^SSMI", "Swiss Market Index", "Switzerland", ["SSMI", "SWISS20", "SWITZERLAND"]),
    ("IBEX", "^IBEX", "IBEX 35", "Spain", ["IBEX35", "ESP35", "SPAIN"]),
    ("FTSEMIB", "FTSEMIB.MI", "FTSE MIB", "Italy", ["MIB", "IT40", "ITALY"]),
    ("BEL20", "^BFX", "BEL 20", "Belgium", ["BFX", "BELGIUM"]),
    ("ATX", "^ATX", "ATX", "Austria", ["AUSTRIA"]),
    ("PSI", "PSI20.LS", "PSI", "Portugal", ["PSI20", "PORTUGAL"]),
    ("ISEQ", "^ISEQ", "ISEQ All Share", "Ireland", ["IRELAND"]),
    ("OMXS30", "^OMX", "OMX Stockholm 30", "Sweden", ["OMX", "SWEDEN"]),
    ("OMXC25", "^OMXC25", "OMX Copenhagen 25", "Denmark", ["DENMARK"]),
    ("OMXH25", "^OMXH25", "OMX Helsinki 25", "Finland", ["FINLAND"]),
    ("OSEBX", "OSEBX.OL", "Oslo Bors Benchmark", "Norway", ["NORWAY"]),
    ("WIG20", "WIG20.WA", "WIG20", "Poland", ["POLAND"]),
    ("BUX", "^BUX.BD", "BUX", "Hungary", ["HUNGARY"]),
    ("ATHEX", "GD.AT", "Athens General Composite", "Greece", ["ASE", "GREECE"]),
    ("BIST100", "XU100.IS", "BIST 100", "Turkey", ["XU100", "TURKEY"]),
    ("MOEX", "IMOEX.ME", "MOEX Russia", "Russia", ["IMOEX", "RUSSIA"]),
    # --- Asia Pacific
    ("NIKKEI", "^N225", "Nikkei 225", "Japan", ["NI225", "N225", "JP225", "JAPAN"]),
    ("HSI", "^HSI", "Hang Seng", "Hong Kong", ["HANGSENG", "HK50", "HONGKONG"]),
    ("HSCEI", "^HSCE", "Hang Seng China Enterprises", "Hong Kong", ["HSCE"]),
    ("SHCOMP", "000001.SS", "Shanghai Composite", "China", ["SSEC", "SHANGHAI", "CHINA"]),
    ("SZCOMP", "399001.SZ", "Shenzhen Component", "China", ["SZSE", "SHENZHEN"]),
    ("CSI300", "000300.SS", "CSI 300", "China", ["CHINA300"]),
    ("KOSPI", "^KS11", "KOSPI", "South Korea", ["KS11", "KOREA", "SOUTHKOREA"]),
    ("KOSDAQ", "^KQ11", "KOSDAQ", "South Korea", ["KQ11"]),
    ("TAIEX", "^TWII", "Taiwan Weighted (TAIEX)", "Taiwan", ["TWII", "TWSE", "TAIWAN"]),
    ("STI", "^STI", "Straits Times Index", "Singapore", ["SINGAPORE"]),
    ("KLCI", "^KLSE", "FTSE Bursa Malaysia KLCI", "Malaysia", ["KLSE", "FBMKLCI", "MALAYSIA"]),
    ("JCI", "^JKSE", "Jakarta Composite", "Indonesia", ["JKSE", "INDONESIA"]),
    ("SET", "^SET.BK", "SET Index", "Thailand", ["THAILAND"]),
    ("PSEI", "PSEI.PS", "PSE Composite", "Philippines", ["PHILIPPINES"]),
    ("VNINDEX", "^VNINDEX.VN", "VN-Index", "Vietnam", ["VIETNAM"]),
    ("KSE100", "^KSE", "KSE 100", "Pakistan", ["PAKISTAN"]),
    ("CSEALL", "^CSE", "CSE All Share", "Sri Lanka", ["ASPI", "SRILANKA"]),
    ("ASX200", "^AXJO", "S&P/ASX 200", "Australia", ["XJO", "AXJO", "AUS200", "AUSTRALIA"]),
    ("ALLORDS", "^AORD", "All Ordinaries", "Australia", ["AORD", "XAO"]),
    ("NZX50", "^NZ50", "S&P/NZX 50", "New Zealand", ["NZ50", "NEWZEALAND"]),
    # --- Middle East and Africa
    ("TASI", "^TASI.SR", "Tadawul All Share", "Saudi Arabia", ["SAUDI", "SAUDIARABIA"]),
    ("TA125", "^TA125.TA", "TA-125", "Israel", ["ISRAEL"]),
    ("TA35", "TA35.TA", "TA-35", "Israel", []),
    ("EGX30", "^CASE30", "EGX 30", "Egypt", ["CASE30", "EGYPT"]),
    ("JSE", "^J203.JO", "FTSE/JSE All Share", "South Africa", ["J203", "JALSH", "SOUTHAFRICA"]),
    ("JSE40", "^J200.JO", "FTSE/JSE Top 40", "South Africa", ["J200", "TOP40"]),
    ("DFM", "DFMGI.AE", "DFM General", "United Arab Emirates", ["DFMGI", "DUBAI", "UAE"]),
    ("QSI", "^GNRI.QA", "QE General", "Qatar", ["GNRI", "QATAR"]),
]


def _get(url):
    last = None
    for attempt in range(3):
        try:
            return B._get(url, headers={"User-Agent": B.UA}, timeout=40)
        except Exception as e:      # noqa: BLE001 — retried, then reported by the caller
            last = e
            time.sleep(1.5 * (attempt + 1))
    raise last


def fetch(sym):
    """→ (rows [(date, o, h, l, c)], note). o/h/l are None when not usable (see the rules above)."""
    url = ("https://query1.finance.yahoo.com/v8/finance/chart/" + urllib.parse.quote(sym, safe="") +
           "?period1=-631152000&period2=" + str(int(time.time())) + "&interval=1d")
    j = json.loads(_get(url))
    res = (j.get("chart") or {}).get("result")
    if not res:
        raise ValueError("no result: %s" % str((j.get("chart") or {}).get("error"))[:80])
    res = res[0]
    meta = res.get("meta") or {}
    ts = res.get("timestamp") or []
    q = ((res.get("indicators") or {}).get("quote") or [{}])[0]
    off = int(meta.get("gmtoffset") or 0)
    reg = ((meta.get("currentTradingPeriod") or {}).get("regular") or {})
    now = time.time()
    live_from = reg.get("start") if (reg.get("start") and reg.get("end") and reg["start"] <= now < reg["end"]) else None
    rows, seen, dropped_live = [], {}, 0
    O, H, L, C = q.get("open") or [], q.get("high") or [], q.get("low") or [], q.get("close") or []
    for i, t in enumerate(ts):
        c = C[i] if i < len(C) else None
        if c is None or not (c > 0):
            continue
        if live_from is not None and t >= live_from:
            dropped_live += 1
            continue
        d = dt.datetime.fromtimestamp(t + off, dt.timezone.utc).date().isoformat()
        o, h, l = (X[i] if i < len(X) else None for X in (O, H, L))
        c2 = round(c, 2)
        if (o is None or h is None or l is None or not (o > 0 and h > 0 and l > 0)
                or round(h, 2) < max(round(o, 2), c2) or round(l, 2) > min(round(o, 2), c2)):
            o = h = l = None
        if d in seen:               # two timestamps on one exchange date: the later one wins
            rows[seen[d]] = (d, o, h, l, c)
        else:
            seen[d] = len(rows); rows.append((d, o, h, l, c))
    rows.sort()
    return rows, ("dropped %d unfinished bar" % dropped_live if dropped_live else "")


def pack(code, label, rows):
    x = lambda v: None if v is None else int(round(v * 100))
    d0 = rows[0][0]
    days = [dt.date.fromisoformat(r[0]).toordinal() for r in rows]
    return {"sv": SCHEMA, "key": code, "label": label, "d0": d0, "dd": [days[i] - days[i - 1] for i in range(1, len(days))],
            "o": [x(r[1]) for r in rows], "h": [x(r[2]) for r in rows], "l": [x(r[3]) for r in rows], "c": [x(r[4]) for r in rows]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(HERE, "..", "docs", "world"))
    ap.add_argument("--only", default="")
    a = ap.parse_args()
    only = {s.strip().upper() for s in a.only.split(",") if s.strip()}
    os.makedirs(a.out, exist_ok=True)
    ip = os.path.join(a.out, "_index.json")
    index = json.load(open(ip)) if os.path.exists(ip) else {"sv": SCHEMA, "ser": {}}
    ser = index.setdefault("ser", {})
    codes = [w[0] for w in WORLD]
    assert len(codes) == len(set(codes)), "duplicate code"
    taken = {}
    for code, _, _, _, al in WORLD:
        for k in [code] + al:
            assert k not in taken, "alias %s used by %s and %s" % (k, taken[k], code)
            taken[k] = code
    ok, kept, failed = [], [], []
    for code, sym, label, country, alias in WORLD:
        if only and code not in only:
            continue
        fp = os.path.join(a.out, code + ".json")
        old_n = (ser.get(code) or {}).get("n", 0) if os.path.exists(fp) else 0
        try:
            rows, note = fetch(sym)
            if len(rows) < 250:
                raise ValueError("only %d daily bars" % len(rows))
            if old_n and len(rows) < 0.98 * old_n:
                kept.append("%s (Yahoo gave %d bars, file has %d)" % (code, len(rows), old_n)); continue
        except Exception as e:      # noqa: BLE001
            (kept if old_n else failed).append("%s %s (%s)" % (code, sym, str(e)[:70])); continue
        blob = json.dumps(pack(code, label, rows), separators=(",", ":"))
        with open(fp + ".tmp", "w") as fh:
            fh.write(blob)
        os.replace(fp + ".tmp", fp)
        ohl = next((r[0] for r in rows if r[1] is not None), None)
        ser[code] = {"f": code, "label": label, "country": country, "src": "Yahoo " + sym, "first": rows[0][0], "last": rows[-1][0],
                     "ord": codes.index(code), "n": len(rows), "close_only": sum(1 for r in rows if r[1] is None), "ohl_from": ohl, "alias": alias}
        ok.append(code)
        print("%-9s %-12s %6d bars %s -> %s  close-only %d  last close %.2f %s" % (
            code, sym, len(rows), rows[0][0], rows[-1][0], ser[code]["close_only"], rows[-1][4], note), flush=True)
        time.sleep(0.4)
    for code in list(ser):          # an index dropped from WORLD leaves no orphan entry
        if code not in codes:
            del ser[code]
    index["sv"] = SCHEMA
    index["updated"] = dt.datetime.now(dt.timezone(dt.timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %H:%M IST")
    with open(ip + ".tmp", "w") as fh:
        json.dump(index, fh, separators=(",", ":"), ensure_ascii=False, sort_keys=True)
    os.replace(ip + ".tmp", ip)
    print("world indices: %d written, %d kept as they were, %d not available" % (len(ok), len(kept), len(failed)))
    for x in kept:
        print("  kept: " + x)
    for x in failed:
        print("  NOT AVAILABLE: " + x)
    if not ok and not only:
        sys.exit("ABORT: Yahoo answered for no index")


if __name__ == "__main__":
    main()
