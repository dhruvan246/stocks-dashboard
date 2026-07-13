# -*- coding: utf-8 -*-
"""Build docs/discovery.json — the auto-computed "Discovery" trigger buckets (Sovrenn-style
concept, our own automated implementation). No hand curation: every bucket is derived from
data the site already maintains:

  docs/announcements.json        -> event/trigger buckets (order wins, fund raising, M&A, ...)
  docs/sf_revop.json             -> "Excellent Results (<qtr>)" per quarter since Mar-2019 + weak results + P/E
  docs/dash_slim.bin             -> names/mcap/52w stats -> breakout / deep-correction / P/E buckets
  docs/sector_classification.json-> sector theme buckets (defence, railways, green energy, ...)

Output schema (compact):
{updated, groups:[{g, buckets:[{k,t,d,type,n,rows}]}]}
  type 'ann': rows [sym, name, count, lastDate, lastCaption, lastFile]
  type 'res': rows [sym, name, patNow, patAgo, patYoY%, revYoY%]
  type 'px' : rows [sym, name, mcapCr, lastPx, d52pct, pe|null]
  type 'sec': rows [sym, name, mcapCr, d52pct, subgroup]

Run: python -X utf8 scripts/build_discovery.py   (CI: refresh-announcements.yml, after the fetch)
"""
import os, sys, json, gzip, re, datetime

HERE = os.path.dirname(os.path.abspath(__file__))
DOCS = os.path.join(HERE, "..", "docs")
def dp(f): return os.path.join(DOCS, f)

MCAP_FLOOR = 50.0        # cr — keep shell/illiquid names out of price & sector buckets
RES_MIN_PAT = 2.0        # cr — min current-quarter PAT for Excellent Results
FIRST_RES_QE = 20190331  # earliest results bucket

# ---------------------------------------------------------------- inputs
ann = json.load(open(dp("announcements.json"), encoding="utf-8"))
revop = json.load(open(dp("sf_revop.json"), encoding="utf-8"))
slim = json.loads(gzip.decompress(open(dp("dash_slim.bin"), "rb").read()))
classif = json.load(open(dp("sector_classification.json"), encoding="utf-8"))

META = {}   # SYM -> {name, mcap, latest, d52}
for k, m in (slim.get("meta") or {}).items():
    sym = str(m.get("symbol") or k.split(".")[0]).upper()
    META[sym] = {"name": m.get("name") or sym, "mcap": m.get("mcap"), "latest": m.get("latest"), "d52": m.get("d52")}
def nameof(sym, fallback=""): return (META.get(sym) or {}).get("name") or fallback or sym

# ------------------------------------------- 1) announcement-driven buckets
# raw NSE `desc` -> bucket key (first match wins); mirrors the page's CATRULES philosophy
ANN_BUCKETS = [
    ("orders",    "Order Wins",                     "Companies announcing new orders / contract wins in the last month.",
     re.compile(r"orders?/contracts|order\(s\)\/contract", re.I)),
    ("capex",     "Capacity & New Products",        "Commercial production started, capacity added or new products launched.",
     re.compile(r"commercial production|capacity addition|product launch|new line", re.I)),
    ("ma",        "M&A, JVs & Partnerships",        "Acquisitions, mergers, joint ventures, MOUs and strategic tie-ups.",
     re.compile(r"acquisition|amalgamation|merger|demerger|scheme of arrangement|memorandum of understanding|agreements|tie ?up", re.I)),
    ("fund",      "Fund Raising",                   "Preferential issues, QIPs, rights issues and other capital raises.",
     re.compile(r"preferential issue|qualified institution|rights issue|fccb|issue of securities|allotment of securities|fund raising|offer for sale", re.I)),
    ("openoffer", "Open Offers",                    "Open offers triggered by takeovers / stake purchases.",
     re.compile(r"open offer", re.I)),
    ("buyback",   "Buybacks",                       "Share buyback announcements and updates.",
     re.compile(r"buy ?back", re.I)),
    ("reward",    "Dividends, Bonus & Splits",      "Dividends declared, bonus issues and stock splits.",
     re.compile(r"^dividend|bonus|stock split|sub-?division", re.I)),
    ("rating",    "Credit Rating Updates",          "Rating upgrades, downgrades, reaffirmations and watch actions.",
     re.compile(r"credit rating", re.I)),
    ("meet",      "Investor Meets",                 "Analyst / institutional investor meets, con-calls and presentations.",
     re.compile(r"investor meet|analyst|investor presentation|con\.? call", re.I)),
    ("spurt",     "Volume & Price Spurts",          "Exchange surveillance: unusual volume or price movement clarifications.",
     re.compile(r"spurt in volume|price movement", re.I)),
    ("redflag",   "Red Flags",                      "Insolvency, defaults, fraud, regulatory action, auditor exits, trading suspensions.",
     re.compile(r"insolvency|default|fraud|arrest|action\(s\)|statutory auditor|suspension of trading|penalt|one time settlement", re.I)),
]

def build_ann_buckets():
    per = {k: {} for k, *_ in [(b[0],) for b in ANN_BUCKETS]}
    for r in ann.get("rows", []):
        sym, co, dt, desc, cap, f = r
        for key, _t, _d, rx in ANN_BUCKETS:
            if rx.search(desc):
                s = per[key].get(sym)
                if s is None:
                    per[key][sym] = {"n": 1, "dt": dt, "cap": cap, "f": f, "co": co}
                else:
                    s["n"] += 1
                    if dt > s["dt"]: s.update(dt=dt, cap=cap, f=f)
                break
    out = []
    for key, t, d, _rx in ANN_BUCKETS:
        rows = [[sym, nameof(sym, s["co"]), s["n"], s["dt"][:10],
                 (s["cap"][:179] + "…") if len(s["cap"]) > 180 else s["cap"], s["f"]]
                for sym, s in per[key].items()]
        rows.sort(key=lambda x: x[3], reverse=True)
        out.append({"k": key, "t": t, "d": d, "type": "ann", "n": len(rows), "rows": rows})
    return out

# ------------------------------------------- 2) results buckets (computed, per quarter)
def prev_qe(qe):
    y, m = qe // 10000, (qe // 100) % 100
    return {3: (y-1)*10000+331, 6: y*10000+331, 9: y*10000+630, 12: y*10000+930}[m]  # unused
def yoy_qe(qe): return qe - 10000

def pick(cell, i_con, i_std):
    if not cell: return None
    v = cell[i_con]
    if v is None or v == 0: v = cell[i_std]
    return v

QLBL = {3: "Mar", 6: "Jun", 9: "Sep", 12: "Dec"}
def qlabel(qe): return "%s-%02d" % (QLBL[(qe//100) % 100], (qe//10000) % 100)

def build_results_buckets():
    qes = set()
    for s, qmap in revop.items():
        for q in qmap: qes.add(int(q))
    qes = sorted((q for q in qes if q >= FIRST_RES_QE), reverse=True)
    buckets, weak_by_qe = [], {}
    for qe in qes:
        rows, weak = [], []
        base = str(yoy_qe(qe))
        for sym, qmap in revop.items():
            cur, ago = qmap.get(str(qe)) or qmap.get(qe), qmap.get(base) or qmap.get(yoy_qe(qe))
            if not cur or not ago: continue
            pat, patA = pick(cur, 5, 4), pick(ago, 5, 4)
            rev, revA = pick(cur, 1, 0), pick(ago, 1, 0)
            if pat is None or patA is None or patA <= 0: continue
            patY = (pat / patA - 1) * 100
            revY = (rev / revA - 1) * 100 if (rev and revA and revA > 0) else None
            if pat >= RES_MIN_PAT and patY >= 25 and (revY is None or revY >= 10) and (revY is not None or patY >= 40):
                rows.append([sym, nameof(sym), round(pat, 1), round(patA, 1), round(patY, 1), round(revY, 1) if revY is not None else None])
            elif patY <= -30 or pat < 0:
                weak.append([sym, nameof(sym), round(pat, 1), round(patA, 1), round(patY, 1), round(revY, 1) if revY is not None else None])
        rows.sort(key=lambda x: -x[4])
        if len(rows) >= 5:
            buckets.append({"k": "res%d" % qe, "t": "Excellent Results (%s)" % qlabel(qe),
                            "d": "PAT up ≥25% YoY (with revenue support), computed from filings — not hand-picked.",
                            "type": "res", "n": len(rows), "rows": rows})
        weak_by_qe[qe] = weak
    # Weak Results: attach to the newest quarter with real coverage (early in a season the
    # latest quarter has only a handful of filings)
    for qe in qes:
        weak = weak_by_qe.get(qe) or []
        if len(weak) >= 30:
            weak.sort(key=lambda x: x[4])
            buckets.insert(1 if buckets else 0,
                           {"k": "weak%d" % qe, "t": "Weak Results (%s)" % qlabel(qe),
                            "d": "PAT down ≥30% YoY or in loss against a profitable base quarter.",
                            "type": "res", "n": len(weak), "rows": weak})
            break
    return buckets

# TTM PAT for P/E (needs the 4 most recent consecutive quarters present)
def ttm_pat(sym):
    qmap = revop.get(sym)
    if not qmap: return None
    qes = sorted(int(q) for q in qmap)
    if len(qes) < 4: return None
    last4 = qes[-4:]
    today = int(datetime.date.today().strftime("%Y%m%d"))
    if last4[-1] < today - 10000 + 3000: return None   # latest quarter older than ~9 months -> stale
    tot = 0.0
    for q in last4:
        v = pick(qmap[str(q)] if str(q) in qmap else qmap[q], 5, 4)
        if v is None: return None
        tot += v
    return tot

# ------------------------------------------- 3) price buckets (from slim meta)
def build_price_buckets():
    hi, corr, cheap = [], [], []
    for sym, m in META.items():
        mc, d52, px = m.get("mcap"), m.get("d52"), m.get("latest")
        if not mc or mc < MCAP_FLOOR or d52 is None or px is None: continue
        if d52 >= -2: hi.append([sym, m["name"], round(mc, 1), px, round(d52, 1), None])
        if d52 <= -30: corr.append([sym, m["name"], round(mc, 1), px, round(d52, 1), None])
        t = ttm_pat(sym)
        if t and t > 0:
            pe = mc / t
            if pe < 30: cheap.append([sym, m["name"], round(mc, 1), px, round(d52, 1), round(pe, 1)])
    hi.sort(key=lambda x: -x[2]); corr.sort(key=lambda x: x[4]); cheap.sort(key=lambda x: x[5])
    hi, corr, cheap = hi[:500], corr[:500], cheap[:500]
    return [
        {"k": "hi52",  "t": "Near 52-Week High",  "d": "Trading within 2% of the 52-week high — breakout zone (mcap ≥ ₹50 cr).", "type": "px", "n": len(hi),  "rows": hi},
        {"k": "corr",  "t": "30%+ Off the High",   "d": "Corrected 30% or more from the 52-week high (mcap ≥ ₹50 cr, 500 worst shown).", "type": "px", "n": len(corr), "rows": corr},
        {"k": "pe30",  "t": "P/E Under 30",        "d": "Market cap under 30× trailing-12-month PAT (profitable, recent filings; 500 cheapest shown).", "type": "px", "n": len(cheap), "rows": cheap},
    ]

# ------------------------------------------- 4) sector theme buckets
# Themes cut ACROSS the exchange taxonomy (Suzlon=Heavy Electrical, RVNL=Civil Construction,
# IREDA=Finance) so pure regex misses them — each theme = taxonomy regex OR a hand-seeded
# symbol list (edit SEEDS below to grow a theme).
THEMES = [
    ("defence",  "Defence & Aerospace",      r"defence|defense|aerospace",
     "HAL BEL BDL MAZDOCK COCHINSHIP GRSE MIDHANI DATAPATTNS ASTRAMICRO PARAS ZENTEC SOLARINDS DYNAMATECH IDEAFORGE UNIMECH TANEJAERO"),
    ("rail",     "Railways",                 r"railway",
     "RVNL IRCON RITES IRFC IRCTC RAILTEL TITAGARH TEXRAIL JWL BEML CONCOR RKFORGE RAMKY HINDCON OSWALAGRO"),
    ("green",    "Green Energy",             r"$^",   # no taxonomy bucket — seeds only
     "SUZLON INOXWIND IWEL ADANIGREEN TATAPOWER NHPC SJVN ACMESOLAR PREMIERENE WAAREEENER SWSOLAR IREDA KPIGREEN KPEL WEBSOL SOLARA ZODIACLOTH UJAAS ORIANA INSOLATION GANESHBE BORORENEW SWELECTES ADVAIT"),
    ("ev",       "Auto & Components",        r"auto components|auto ancillar|2/3 wheelers|passenger cars|commercial vehicles|tyres", ""),
    ("hosp",     "Hospitals & Diagnostics",  r"hospital|diagnostic|healthcare service", ""),
    ("hotel",    "Hotels & Travel",          r"hotel|resort|tour|travel|airline|aviation", ""),
    ("it",       "IT Services & Software",   r"software|it enabled|computers - |internet & catalogue", ""),
    ("realty",   "Realty & Building",        r"realty|residential|commercial projects|cement|building products", ""),
    ("logi",     "Shipping & Logistics",     r"shipping|logistics|\bport\b|marine", ""),
    ("pharma",   "Pharma & Chemicals",       r"pharmaceutical|specialty chemical|agrochemical|petrochem|commodity chemical", ""),
    ("capgoods", "Capital Goods",            r"industrial products|heavy electrical|industrial machinery|capital goods|engineering", ""),
    ("ems",      "Electronics & EMS",        r"consumer electronics|electronic components",
     "DIXON KAYNES SYRMA AMBER AVALON CYIENTDLM PGEL DCX CENTUM ELIN EPACK SAHASRA MOSCHIP"),
]

def build_sector_buckets():
    out = []
    for key, t, rx_s, seeds_s in THEMES:
        rx, seeds = re.compile(rx_s, re.I), set(seeds_s.split())
        rows = []
        for k, c in classif.items():
            sym = k.split(".")[0].upper()
            m = META.get(sym)
            if not m or not m.get("mcap") or m["mcap"] < MCAP_FLOOR: continue
            blob = " ".join(str(c.get(f) or "") for f in ("igroup", "industry", "subgroup"))
            if sym in seeds or rx.search(blob):
                rows.append([sym, m["name"], round(m["mcap"], 1), round(m.get("d52") or 0, 1), c.get("subgroup") or ""])
        # seeded symbols with no classification entry still belong in the theme
        for sym in seeds:
            m = META.get(sym)
            if m and m.get("mcap") and m["mcap"] >= MCAP_FLOOR and not any(r[0] == sym for r in rows):
                rows.append([sym, m["name"], round(m["mcap"], 1), round(m.get("d52") or 0, 1), ""])
        rows.sort(key=lambda x: -x[2])
        rows = rows[:80]
        out.append({"k": key, "t": t, "d": "Listed companies in this theme (mcap ≥ ₹50 cr), largest first.",
                    "type": "sec", "n": len(rows), "rows": rows})
    return out

# ---------------------------------------------------------------- assemble
def main():
    res = build_results_buckets()
    latest = [b for b in res if not b["k"].startswith("res") or res and b["k"] in (res[0]["k"],)]
    groups = [
        {"g": "Triggers (last 31 days)", "buckets": build_ann_buckets()},
        {"g": "Price & Value",           "buckets": build_price_buckets()},
        {"g": "Results — computed from filings", "buckets": res[:3]},
        {"g": "Results history",         "buckets": res[3:]},
        {"g": "Sector themes",           "buckets": build_sector_buckets()},
    ]
    ist = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=5, minutes=30)
    out = {"updated": ist.strftime("%Y-%m-%d %H:%M IST"), "groups": groups}
    json.dump(out, open(dp("discovery.json"), "w", encoding="utf-8"), ensure_ascii=False, separators=(",", ":"))
    nb = sum(len(g["buckets"]) for g in groups)
    print("WROTE docs/discovery.json: %d groups, %d buckets, %.2f MB" %
          (len(groups), nb, os.path.getsize(dp("discovery.json")) / 1e6))
    for g in groups:
        for b in g["buckets"][:50]:
            print("  %-28s %-34s %5d" % (g["g"][:28], b["t"][:34], b["n"]))

if __name__ == "__main__":
    main()
