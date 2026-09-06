# -*- coding: utf-8 -*-
"""Aggregate per-symbol headcount ledgers (scripts/headcount/<SYM>.json) into the slim payload the
employees.html dashboard reads: docs/employee_headcount.json.

Survivorship-free universe = union of Nifty-500 members across every _wb_n500_snaps.json snapshot
dated >= 2020-01-01 (current members + names that have since left). Each row is flagged `now`
(in the latest snapshot) so the page can show "In N500" vs "Left N500" without hiding anyone.

Headline series = permanent on-roll employees (emp_perm + wrk_perm); `total` = total workforce incl
contractual. Every FY value keeps a source tag (filing FY, page, method) for provenance.
"""
import json
import os
import re
from datetime import date, datetime

HERE = os.path.dirname(os.path.abspath(__file__))
DOCS = os.path.abspath(os.path.join(HERE, "..", "docs"))
LEDGERS = os.path.join(HERE, "headcount")
SNAPS = os.path.join(HERE, "_wb_n500_snaps.json")
MASTER = os.path.join(HERE, "_bse_master_all.json")
SECTORS = os.path.join(HERE, "_bse_sectors.json")
RENAME = os.path.join(HERE, "_rename_map.json")
START_FY = 2020


def fy_of(iso):
    d = datetime.strptime(iso[:10], "%Y-%m-%d").date()
    return d.year + 1 if d.month >= 4 else d.year   # Apr-Mar → FY-end year


def load_membership():
    snaps = json.load(open(SNAPS))
    dates = sorted(snaps)
    d2020 = [d for d in dates if d >= "2020-01-01"]
    latest = set(snaps[dates[-1]])
    per = {}    # sym -> {"snaps":[dates], "now":bool}
    for d in d2020:
        for s in snaps[d]:
            per.setdefault(s, {"snaps": [], "now": False})
            per[s]["snaps"].append(d)
    for s in per:
        per[s]["now"] = s in latest
    return per, dates[-1]


def load_names():
    name, sector = {}, {}
    master = json.load(open(MASTER))
    secmap = json.load(open(SECTORS))          # {bse_code(str): sector}
    for x in master:
        sid = x.get("scrip_id")
        if not sid:
            continue
        name[sid] = re.sub(r"\s+(Ltd|Limited)\.?$", "", (x.get("Scrip_Name") or "").strip())
        cd = str(x.get("SCRIP_CD") or "")
        if cd in secmap:
            sector[sid] = secmap[cd]
    return name, sector


def main():
    per, latest_date = load_membership()
    name, sector = load_names()
    fys = list(range(START_FY, date.today().year + 1))
    rows, covered = [], 0
    for sym in sorted(per):
        if sym.startswith("DUMMY"):
            continue
        led_path = os.path.join(LEDGERS, sym + ".json")
        emp, total, mf, src = {}, {}, {}, {}
        if os.path.exists(led_path):
            led = json.load(open(led_path))
            for y, c in led.get("fy", {}).items():
                yi = int(y)
                if yi < START_FY or not isinstance(c.get("count"), int):
                    continue
                emp[yi] = c["count"]
                if isinstance(c.get("total_workforce"), int):
                    total[yi] = c["total_workforce"]
                b = c.get("brsr") or {}
                if isinstance(b.get("male"), int) and isinstance(b.get("female"), int):
                    mf[yi] = [b["male"], b["female"]]
                s = c.get("src", {})
                src[yi] = "%s p%s" % (s.get("method", "?"), s.get("page", "?"))
        if emp:
            covered += 1
        yrs = sorted(emp)
        latest_fy = yrs[-1] if yrs else None
        prev_fy = yrs[-2] if len(yrs) >= 2 else None
        yoy = None
        if latest_fy and prev_fy and emp[prev_fy]:
            yoy = round((emp[latest_fy] - emp[prev_fy]) / emp[prev_fy], 4)
        rows.append({
            "sym": sym, "name": name.get(sym, sym), "sector": sector.get(sym, ""),
            "now": per[sym]["now"], "emp": emp, "total": total, "mf": mf, "src": src,
            "latest": emp.get(latest_fy), "latest_fy": latest_fy, "yoy": yoy,
        })
    payload = {
        "updated": datetime.now().strftime("%Y-%m-%d %H:%M IST"),
        "basis": "Permanent on-roll employees (permanent employees + permanent workers). "
                 "Source: company annual reports on BSE — BRSR 'Employees and workers' table (FY2023+) "
                 "or the Board's-Report line. 'Total workforce' includes contractual/other.",
        "note": "India does not report employee count quarterly — this is annual (fiscal year ending March).",
        "fys": fys, "universe": len(rows), "covered": covered,
        "as_of_snapshot": latest_date, "rows": rows,
    }
    os.makedirs(DOCS, exist_ok=True)
    out = os.path.join(DOCS, "employee_headcount.json")
    json.dump(payload, open(out, "w"), separators=(",", ":"), default=str)
    print("wrote %s : %d symbols, %d covered (%.0f%%), FYs %s" % (
        out, len(rows), covered, 100 * covered / max(len(rows), 1), fys))


if __name__ == "__main__":
    main()
