# -*- coding: utf-8 -*-
"""TEST GATE ONLY (user, 2026-10-31): compare docs/ipo_fin/<SYM>.json with Screener and report every differing cell.
Writes a report and nothing else: no figure from this script may enter the data (runbook §232 and §0 first rule).
A differing cell is UNKNOWN until its filing is read; this script never decides who is right.

Run: python3 -X utf8 scripts/gate_new_listings_screener.py [--only JPL,VNL] [--out path.json]
"""
import argparse, glob, html, json, os, re, time

import requests

HERE = os.path.dirname(os.path.abspath(__file__))
FIN = os.path.join(HERE, "..", "docs", "ipo_fin")
MON = {m: i + 1 for i, m in enumerate("Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split())}
END = {3: 31, 6: 30, 9: 30, 12: 31}
sess = requests.Session(); sess.headers.update({"User-Agent": "stocks-dashboard-research/1.0", "Accept": "text/html", "Accept-Language": "en"})


def section(page, sid):
    m = re.search(r'<section[^>]*id="%s".*?</section>' % sid, page, re.S)
    return m.group(0) if m else ""


def parse(sec):
    """-> {period_end: {row label: value}} from Screener's table."""
    t = re.search(r"<table.*?</table>", sec, re.S)
    if not t: return {}
    rows = re.findall(r"<tr.*?</tr>", t.group(0), re.S)
    if not rows: return {}
    heads = [re.sub(r"<[^>]+>", "", c).strip() for c in re.findall(r"<th.*?</th>", rows[0], re.S)][1:]
    ends = []
    for h in heads:
        m = re.match(r"([A-Z][a-z]{2}) (\d{4})", h)
        ends.append(int(m.group(2)) * 10000 + MON[m.group(1)] * 100 + END.get(MON[m.group(1)], 30) if m and MON[m.group(1)] in END else None)
    out = {e: {} for e in ends if e}
    for r in rows[1:]:
        cells = [html.unescape(re.sub(r"<[^>]+>", "", c)).strip().replace("\xa0", "") for c in re.findall(r"<td.*?</td>", r, re.S)]
        if not cells: continue
        lab = re.sub(r"\s*\+$", "", cells[0]).strip()
        for e, c in zip(ends, cells[1:]):
            if not e: continue
            try: out[e][lab] = float(c.replace(",", "").replace("%", ""))
            except ValueError: pass
    return out


PL = (("rev", "Sales"), ("rev", "Revenue"), ("np", "Net Profit"), ("pbt", "Profit before tax"), ("dep", "Depreciation"), ("fc", "Interest"), ("oi", "Other Income"))
BS = (("assets", "Total Assets"),)
CF = (("cfo", "Cash from Operating Activity"), ("cfi", "Cash from Investing Activity"), ("cff", "Cash from Financing Activity"))


def ours(rec, f):
    if f == "np":
        return rec.get("np") if rec.get("np") is not None else rec.get("pat")
    return rec.get(f)


def compare(sym, d, basis, page, rep):
    secs = {"pl": parse(section(page, "profit-loss")), "q": parse(section(page, "quarters")), "bs": parse(section(page, "balance-sheet")), "cf": parse(section(page, "cash-flow"))}
    for key, rec in d.get(basis, {}).items():
        end, m = key.split("|"); end, m = int(end), int(m)
        for name, table, spec in (("annual", secs["pl"] if m == 12 else None, PL), ("quarter", secs["q"] if m == 3 else None, PL), ("bs", secs["bs"] if m == 0 else None, BS), ("cf", secs["cf"] if m == 12 else None, CF)):
            if not table: continue
            col = table.get(end)
            if col is None:
                rep["absent"] += 1; continue
            for f, lab in spec:
                a, b = ours(rec, f), col.get(lab)
                if a is None or b is None: continue
                rep["checked"] += 1
                if abs(a - b) <= max(1.5, abs(b) * 0.002): rep["match"] += 1
                else: rep["diff"].append({"sym": sym, "basis": basis, "period": key, "table": name, "field": lab, "ours": a, "screener": b, "verdict": "UNKNOWN"})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default="")
    ap.add_argument("--out", default=os.path.join(HERE, "_ipo_fin_gate.json"))
    a = ap.parse_args()
    only = {s for s in a.only.split(",") if s}
    rep = {"checked": 0, "match": 0, "absent": 0, "diff": [], "no_page": []}
    for f in sorted(glob.glob(os.path.join(FIN, "*.json"))):
        sym = os.path.basename(f)[:-5]
        if only and sym not in only: continue
        d = json.load(open(f, encoding="utf-8"))
        for basis, path in (("con", "consolidated/"), ("std", "")):
            if not d.get(basis): continue
            r = sess.get("https://www.screener.in/company/%s/%s" % (sym, path), timeout=40)
            time.sleep(2.5)
            if r.status_code != 200:
                rep["no_page"].append(sym + ":" + basis); continue
            compare(sym, d, basis, r.text, rep)
    json.dump(rep, open(a.out, "w", encoding="utf-8"), indent=0)
    print("checked %d cells: %d match, %d differ; %d of our periods not on Screener; no page: %s" % (rep["checked"], rep["match"], len(rep["diff"]), rep["absent"], rep["no_page"][:20]))


if __name__ == "__main__":
    main()
