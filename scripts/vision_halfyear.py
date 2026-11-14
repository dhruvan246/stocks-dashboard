"""SME half-year filers: the vision routine reads their SIX-MONTH columns and stores the half the way
Screener does (a Sep row = Apr-Sep, a Mar row = Oct-Mar), instead of asking them for a 3-month quarter.

Why (user, 2026-11-14: "change the routine for smes. save their half yearly like screener.in do"): SME
companies on BSE SME / NSE Emerge publish results only every six months. The routine asked every filing for
the "3 months ended" columns, so each SME read came back ok:false "half-year columns only", nothing was
stored, the company stayed pending and was rendered + read again every run (tonight's 21:15 IST run: 182 of
238 reads ok:false, most of them SME half-years — PR #678).

Ledger scripts/vision_halfyear_filers.json: {"EXCH|id": {"since", "why", "sym"}}; id = NSE symbol or BSE
scrip code. A company is listed only on evidence:
  * seed(): its Sep-25 or Mar-26 row is a PROVEN 6-month row on the stock page (docs/fin/<SYM>.json pd = 6,
    build_row_periods, runbook §191/§198), or a BSE cell carries h=1 (a half proven or provisional, §194/§195);
  * update_from_reads(): a quarter read came back ok:false with a half-year note (the filing itself).
It is removed when a read shows the company now prints 3-month quarter columns (ok on a quarter read, or a
half read returning ok:false "quarterly columns").
bse_vision_prep marks those companies' manifest entries period="half" with six-month headings (Sep and Mar
quarter-ends only); merge_bse_vision files cur at qe, prev at the previous half-end, yago a year back,
fill-only, and marks a cell it CREATES "prov": 1 (+ "hy": 1) — the §195 provisional-half rule: the page tags
it "H1/H2 · unconfirmed", and the BSE XBRL fetcher replaces a prov cell with the proven half when it lands.
"""
import datetime as dt
import glob
import json
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))
DOCS = os.path.join(HERE, "..", "docs")
PATH = os.path.join(HERE, "vision_halfyear_filers.json")
IST = dt.timezone(dt.timedelta(hours=5, minutes=30))
HALF = re.compile(r"half[\s-]?year|half[\s-]?yearly|six[\s-]?month|\b6[\s-]?m(onths?)?\b|\bH1\b|\bH2\b", re.I)
QUARTERLY = re.compile(r"quarterly columns|3[\s-]?month|three[\s-]?month", re.I)
MON = {331: "31 March", 930: "30 September"}


def load():
    try:
        return json.load(open(PATH, encoding="utf-8"))
    except Exception:
        return {}


def save(led):
    json.dump(dict(sorted(led.items())), open(PATH, "w", encoding="utf-8"), ensure_ascii=False, indent=0)


def key(exch, ident):
    return "%s|%s" % (str(exch).upper(), str(ident).upper())


def entry_key(e):
    exch = str(e.get("exch") or "").upper()
    ident = e.get("scrip") if exch == "BSE" else (e.get("sym") or e.get("ticker"))
    return key(exch, ident) if exch in ("NSE", "BSE") and ident else None


def half_end(qe):
    return int(qe) % 10000 in MON


def prev_half(qe):
    q = int(qe); y = q // 10000
    return y * 10000 + 331 if q % 10000 == 930 else (y - 1) * 10000 + 930


def label(qe):
    q = int(qe)
    return "six months ended %s %d" % (MON[q % 10000], q // 10000)


def headings(qe):
    q = int(qe)
    return {"cur": label(q), "prev": label(prev_half(q)), "yago": label(q - 10000)}


def mark_manifest(manifest, led):
    """Turn known half-year filers' Sep / Mar entries into half reads. Returns how many were marked."""
    n = 0
    for m in manifest:
        k = entry_key(m)
        if k in led and half_end(m.get("qe") or 0) and not m.get("op_reread"):
            m["period"] = "half"
            m["quarters"] = headings(m["qe"])
            n += 1
    return n


def is_half_read(it):
    return it.get("period") == "half"


def update_from_reads(items, today=None):
    """Add companies whose quarter read found only half-year columns; drop companies shown to print quarters."""
    led = load()
    today = today or dt.datetime.now(IST).strftime("%Y-%m-%d")
    add = rem = 0
    for it in items:
        k = entry_key(it)
        if not k:
            continue
        note = str(it.get("note") or "")
        if is_half_read(it):
            if not it.get("ok") and QUARTERLY.search(note) and led.pop(k, None) is not None:
                rem += 1
        elif it.get("ok"):
            if led.pop(k, None) is not None:
                rem += 1
        elif HALF.search(note) and k not in led:
            led[k] = {"since": today, "why": "vision read %s: %s" % (it.get("qe"), note[:120]),
                      "sym": str(it.get("sym") or "").upper()}
            add += 1
    save(led)
    return add, rem


def seed(today=None):
    """Add every company the stores already PROVE files half-years (never removes). Returns how many were added."""
    led = load()
    today = today or dt.datetime.now(IST).strftime("%Y-%m-%d")
    n = 0
    for p in glob.glob(os.path.join(DOCS, "fin", "*.json")):
        try:
            F = json.load(open(p, encoding="utf-8"))
        except Exception:
            continue
        pd = F.get("pd") or {}
        hit = [q for q in ("20250930", "20260331") if pd.get(q) == 6]
        k = key("NSE", F.get("sym") or os.path.basename(p)[:-5])
        if hit and k not in led:
            led[k] = {"since": today, "why": "stock page row %s proven 6 months (row_periods)" % hit[-1],
                      "sym": str(F.get("sym") or "").upper()}
            n += 1
    bf = json.load(open(os.path.join(DOCS, "bse_fundamentals.json"), encoding="utf-8")).get("px", {})
    for code, cells in bf.items():
        if not isinstance(cells, dict):
            continue
        hit = sorted(q for q, c in cells.items() if isinstance(c, dict) and c.get("h") == 1 and q >= "20250331")
        k = key("BSE", code)
        if hit and k not in led:
            led[k] = {"since": today, "why": "bse_fundamentals %s cell h=1" % hit[-1], "sym": ""}
            n += 1
    save(led)
    return n


if __name__ == "__main__":
    import sys
    if "--seed" in sys.argv:
        print("seeded %d; ledger now %d companies (%s)" % (seed(), len(load()), PATH))
    else:
        print(__doc__)
