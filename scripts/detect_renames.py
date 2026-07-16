# -*- coding: utf-8 -*-
"""TRIPWIRE for ISIN-changed ticker renames the price pipeline cannot auto-merge.

Same-ISIN renames auto-merge in update_sf_data.py. But when the ISIN CHANGES at a rename/scheme
(GUJGASLTD->GUJENERGY, LTIM->LTM, RUCHI->PATANJALI...), the new ticker silently starts a FRESH
series and the old one just stops — the stock looks like an 11-day-old IPO and its 10-year history
is stranded. Those need a hand-verified MANUAL_MERGE entry, which historically only happened when a
human noticed. This script notices for us, daily, right after the bhavcopy append:

  1. NEW series that started within the last WINDOW_NEW days (from the freshly-updated bin).
  2. For each, hunt a matching ENDED series (last trade within WINDOW_END days of the new start):
       a) NSE's official symbol-change master (symbolchange.csv) — authoritative old->new pairs;
       b) normalized company-name match (bin meta name of the dead series vs EQUITY_L name of the
          new one) — catches IBC relists/schemes that skip the symbol-change master.
  3. Report: loud ⚠️ lines in the CI log + scripts/_rename_suspects.json (committed). Suppression:
     pairs already merged (old symbol gone from the bin) vanish on their own; false positives get
     an entry in scripts/_rename_ack.json {"OLD|NEW": "why"} and stay quiet.

A suspect is a REPORT, not an action: verify price continuity + the corporate announcement, then
add MANUAL_MERGE in update_sf_data.py (see that dict's comment for the convention).

Run: python -X utf8 scripts/detect_renames.py            # uses docs/sf_stock_data.bin (fresh in CI)
     SF_BIN=path python -X utf8 scripts/detect_renames.py   # point at another bin (e.g. release copy)
"""
import os, re, json, gzip, csv, io, datetime, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__)); ROOT = os.path.dirname(HERE)
BIN = os.environ.get("SF_BIN") or os.path.join(ROOT, "docs", "sf_stock_data.bin")
OUT = os.path.join(HERE, "_rename_suspects.json")
ACK = os.path.join(HERE, "_rename_ack.json")
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

WINDOW_NEW = 75    # a series counts as "new" if it started within this many days
WINDOW_END = 45    # the dead series must have ended within this many days BEFORE the new start

STOP = {"LIMITED", "LTD", "INDIA", "INDIAN", "COMPANY", "CO", "CORPORATION", "CORP", "THE",
        "AND", "OF", "ENTERPRISES", "INDUSTRIES", "PRIVATE", "PVT"}


def norm_name(n):
    toks = [t for t in re.split(r"[^A-Z0-9]+", (n or "").upper()) if t and t not in STOP]
    return toks


def name_match(a, b):
    """True when two company names are the same business, ignoring boilerplate tokens."""
    ta, tb = norm_name(a), norm_name(b)
    if not ta or not tb:
        return False
    sa, sb = set(ta), set(tb)
    if sa == sb or sa <= sb or sb <= sa:
        return True
    inter = len(sa & sb)
    return inter / max(len(sa), len(sb)) >= 0.8


def dl(url):
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Referer": "https://www.nseindia.com/"})
    return urllib.request.urlopen(req, timeout=60).read()


def load_equity_names():
    """symbol -> (company name, isin) from NSE's EQUITY_L master. Empty on failure (fuzzy degrades)."""
    try:
        raw = dl("https://archives.nseindia.com/content/equities/EQUITY_L.csv").decode("utf-8", "replace")
        rows = list(csv.reader(io.StringIO(raw)))
        head = [h.strip().upper() for h in rows[0]]
        si = head.index("SYMBOL"); ni = next(i for i, h in enumerate(head) if "NAME" in h)
        ii = next((i for i, h in enumerate(head) if "ISIN" in h), None)
        return {r[si].strip(): (r[ni].strip(), r[ii].strip() if ii is not None and len(r) > ii else "")
                for r in rows[1:] if len(r) > ni and r[si].strip()}
    except Exception as e:
        print("(EQUITY_L unavailable: %s — name matching limited to bin meta)" % str(e)[:80])
        return {}


def load_symbol_changes():
    """[(old, new, yyyymmdd)] from NSE's symbol-change master. Empty on failure."""
    try:
        raw = dl("https://archives.nseindia.com/content/equities/symbolchange.csv").decode("utf-8", "replace")
        out = []
        for r in csv.reader(io.StringIO(raw)):
            if len(r) < 3:
                continue
            cells = [c.strip() for c in r]
            # layout observed: NAME, OLD, NEW, DD-MON-YYYY (be defensive: find the date cell)
            di = next((i for i, c in enumerate(cells)
                       if re.match(r"^\d{1,2}-[A-Za-z]{3}-\d{4}$", c)), None)
            if di is None or di < 2:
                continue
            m = re.match(r"^(\d{1,2})-([A-Za-z]{3})-(\d{4})$", cells[di])
            mon = {"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6, "jul": 7, "aug": 8,
                   "sep": 9, "oct": 10, "nov": 11, "dec": 12}.get(m.group(2).lower(), 0)
            if not mon:
                continue
            d = int(m.group(3)) * 10000 + mon * 100 + int(m.group(1))
            out.append((cells[di - 2].upper(), cells[di - 1].upper(), d))
        return out
    except Exception as e:
        print("(symbolchange.csv unavailable: %s — official-pair check skipped)" % str(e)[:80])
        return []


def main():
    D = json.loads(gzip.decompress(open(BIN, "rb").read()))
    data, meta = D["data"], D.get("meta", {})
    end = int(D["end"].replace("-", ""))

    def od(ymd):
        return datetime.date(ymd // 10000, ymd // 100 % 100, ymd % 100).toordinal()

    new_syms = {s: e["d"][0] for s, e in data.items()
                if e.get("d") and od(end) - od(e["d"][0]) <= WINDOW_NEW}
    if not new_syms:
        print("no new series in the last %d days — nothing to check" % WINDOW_NEW)
        json.dump({"checked": D["end"], "pairs": []}, open(OUT, "w"), indent=1)
        return
    ended = {s: e["d"][-1] for s, e in data.items()
             if e.get("d") and s not in new_syms and od(end) - od(e["d"][-1]) <= WINDOW_NEW + WINDOW_END}

    eq = load_equity_names()
    changes = load_symbol_changes()
    try:
        ack = json.load(open(ACK))
    except Exception:
        ack = {}

    suspects = []

    def add(old, new, via, oname, nname):
        key = "%s|%s" % (old, new)
        if key in ack:
            return
        if any(p["old"] == old and p["new"] == new for p in suspects):
            return
        suspects.append({"old": old, "new": new, "via": via,
                         "oldName": oname, "newName": nname,
                         "oldLast": ended.get(old), "newFirst": new_syms.get(new),
                         "oldIsin": (meta.get(old) or {}).get("isin", ""),
                         "newIsin": (meta.get(new) or {}).get("isin", "") or (eq.get(new, ("", ""))[1])})

    # (a) official symbol-change pairs where BOTH series exist separately (auto-merge didn't fire)
    for old, new, d in changes:
        if new in new_syms and old in ended and ended[old] < new_syms[new]:
            add(old, new, "symbolchange.csv", (meta.get(old) or {}).get("name", old),
                eq.get(new, ((meta.get(new) or {}).get("name", new), ""))[0])

    # (b) fuzzy company-name match: dead series' bin name vs new series' EQUITY_L name
    for new, first in new_syms.items():
        nname = eq.get(new, ("", ""))[0] or (meta.get(new) or {}).get("name", "")
        if not nname or nname == new:
            continue
        for old, last in ended.items():
            if not (0 <= od(first) - od(last) <= WINDOW_END):
                continue
            oname = (meta.get(old) or {}).get("name", "")
            if oname and oname != old and name_match(oname, nname):
                add(old, new, "name-match", oname, nname)

    for p in suspects:
        print("⚠️ RENAME SUSPECT: %s -> %s  (%s)  old='%s' last=%s | new='%s' first=%s" % (
            p["old"], p["new"], p["via"], p["oldName"], p["oldLast"], p["newName"], p["newFirst"]))
        print("   -> verify continuity + announcement, then add MANUAL_MERGE {\"%s\": \"%s\"} in update_sf_data.py"
              " (or ack in scripts/_rename_ack.json)" % (p["new"], p["old"]))
    if not suspects:
        print("no rename suspects among %d new / %d recently-ended series" % (len(new_syms), len(ended)))
    json.dump({"checked": D["end"], "pairs": suspects}, open(OUT, "w"), indent=1)


if __name__ == "__main__":
    main()
