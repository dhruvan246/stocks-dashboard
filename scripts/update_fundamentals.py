# -*- coding: utf-8 -*-
"""
Daily INCREMENTAL refresh for docs/sf_fundamentals.json (the backtest's quarterly
net-profit dataset). Instead of re-fetching all 5,000+ stocks, it asks NSE's
integrated-filing-results endpoint for everything filed in the last ~21 days
(ALL companies, one date-range call), parses net profit from each new filing, and
upserts the quarter into the dataset. Light: one list call + a handful of XBRL
fetches during earnings season, ~nothing otherwise.

Picks up automatically: new quarters for existing stocks AND brand-new IPOs
(GROWW, LENSKART, …) the day they first file; AND consolidated=standalone for
established no-subsidiary companies (see is_nosub backfill below).

TWO KNOWN BLIND SPOTS (fill manually when they show gaps):
  1. INSURERS (LICI/HDFCLIFE/ICICIPRULI/ICICIGI/GICRE/NIACL/SBILIFE/STARHEALTH/
     GODIGIT/NIVABUPA) file IRDAI-format results (Revenue A/c + Shareholders' P&L)
     that xbrl_profit can't parse -> they get NEITHER std nor con here. Extract per
     scripts/INSURER_EXTRACTION_PLAYBOOK.md.
  2. A BRAND-NEW no-subsidiary company has no con history yet, so the no-sub backfill
     can't recognise it until it has >=3 con==std quarters; seed it once manually,
     then this script maintains it.

Run: python -X utf8 update_fundamentals.py
"""
import os, sys, json, datetime, re

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import build_fundamentals as B   # reuse _get / nse_jar / iso / xbrl_profit / MIN_QE


def gated_ann(bstr):
    """15:30 IST availability gate (memory project-stocks-1530-gate).

    NSE `broadcast_Date` looks like "16-Jan-2025 20:20" — it carries the filing
    TIME. The backtest rebalances at the 15:30 close and checks `annDate <= rebalanceDate`
    at DATE granularity, so a result broadcast AFTER 15:30 on a rebalance day would be
    wrongly treated as available that day (same-day look-ahead). So: if the broadcast
    time is after 15:30, the result is only actionable from the NEXT trading day — return
    that date. (Next *weekday* is engine-equivalent to next *trading day* here: rebalance
    dates are always trading days, so a `<=` test against one never lands on a skipped
    weekend/holiday.) Returns YYYYMMDD str, or "99999999" when no date is present."""
    d = B.iso(bstr)                       # YYYYMMDD (date part) or None
    if not d:
        return "99999999"
    m = re.search(r'\b(\d{1,2}):(\d{2})\b', bstr or '')
    if m and int(m.group(1)) * 60 + int(m.group(2)) > 15 * 60 + 30:
        nd = datetime.date(int(d[:4]), int(d[4:6]), int(d[6:])) + datetime.timedelta(days=1)
        while nd.weekday() >= 5:          # skip Sat/Sun
            nd += datetime.timedelta(days=1)
        return nd.strftime("%Y%m%d")
    return d
from build_revop import xbrl_revop   # revenue + operating profit from the SAME filing XBRL

HERE = os.path.dirname(os.path.abspath(__file__)); ROOT = os.path.dirname(HERE)
DOCS = os.path.join(ROOT, "docs", "sf_fundamentals.json")
REVOP = os.path.join(ROOT, "docs", "sf_revop.json")   # parallel rev/op dataset for the Results Season chart
MARK = os.path.join(ROOT, "docs", ".fund_updated")
WINDOW_DAYS = 120  # wide overlap: a full quarter, so even if the workflow misses a few runs the
                   # next one self-heals. Cheap because we SKIP the iXBRL fetch for quarters/bases
                   # already stored (see loop) — most filings in the window are already on file.

def main():
    if os.path.exists(MARK): os.remove(MARK)
    data = json.load(open(DOCS))
    try: revop = json.load(open(REVOP))   # {SYM:{QE:[revStd,revCon,opStd,opCon,patStd,patCon,fin,ebitStd,ebitCon]}}
    except Exception: revop = {}
    jar = B.nse_jar()
    h = {"User-Agent": B.UA, "Accept": "application/json",
         "Referer": "https://www.nseindia.com/companies-listing/corporate-filings-financial-results"}
    today = datetime.date.today()
    frm = (today - datetime.timedelta(days=WINDOW_DAYS)).strftime("%d-%m-%Y")
    to = today.strftime("%d-%m-%Y")
    # ⚠️ The endpoint PAGINATES and defaults to only 20 rows (size=20), even though a peak
    # earnings day carries 80+ filings and the 120-day window holds thousands (totalCount says so).
    # Without paging, the cron silently saw only the latest 20 filings and dropped the rest — so on
    # a heavy results day most companies (e.g. MAHABANK/ELECON on 2026-07-10) never got fetched.
    # Page through with a large size until we've pulled totalCount rows. Downstream dedups by
    # (sym, qe), so any page overlap is harmless.
    # ⚠️ Query BOTH the mainboard (index=equities) AND SME/Emerge (index=sme) boards. SME results
    # file XBRL too, but WITHOUT this pass their numbers never parse — the filing shows on the page
    # while the number stays perma "numbers being parsed" (VIGOR/SHERA/KARNIKA/GSMFOILS, 2026-07).
    # Mirrors the SME fix on the announcements side (fetch_announcements). Mainboard is essential
    # (abort on failure); SME is best-effort (skip). Fresh session per board — NSE throttles sme
    # when it follows equities in the same session (see DATA_RUNBOOK §14).
    rows = []
    for idx in ("equities", "sme"):
        jar = B.nse_jar()
        base = ("https://www.nseindia.com/api/integrated-filing-results?index=%s&period=Quarterly"
                "&from_date=%s&to_date=%s" % (idx, frm, to))
        got, page = 0, 1
        try:
            total = 0
            while True:
                jb = json.loads(B._get(base + "&page=%d&size=500" % page, headers=h, jar=jar, timeout=60))
                if isinstance(jb, list):           # defensive: pre-pagination shape (no envelope)
                    rows.extend(jb); got += len(jb); break
                d = jb.get("data", []) or []
                rows.extend(d); got += len(d)
                # totalCount FLAPS between responses (observed 30080 → 12 mid-fetch) — trust the MAX
                # seen, never the latest, else a flaky envelope ends the loop early and drops filings.
                total = max(total, jb.get("totalCount") or 0)
                if not d or got >= total or page > 60:   # page cap = hard stop against a bad envelope
                    break
                page += 1
        except Exception as e:
            print("recent-filings fetch [%s] failed: %s" % (idx, e))
            if idx == "equities":
                return                              # mainboard is essential; SME is best-effort
            continue
        print("filings [%s] last %d days: +%d (%d pages)" % (idx, WINDOW_DAYS, got, page))
    print("total filings: %d" % len(rows))

    byq = {}
    for r in rows:
        sym = r.get("symbol"); qe = B.iso(r.get("qe_Date")); xb = r.get("xbrl", "")
        if not sym or not qe or not xb.startswith("http") or int(qe) < B.MIN_QE: continue
        if "governance" in (r.get("type", "") or "").lower(): continue   # Governance filing has no P&L
        byq.setdefault((sym, qe), []).append(
            {"ann": gated_ann(r.get("broadcast_Date")), "xbrl": xb, "basis": r.get("consolidated", "")})

    changed = newsyms = revop_changed = 0
    for (sym, qe), filings in byq.items():
        existing = next((x for x in data.get(sym, []) if x[0] == int(qe)), None)
        er = revop.get(sym, {}).get(str(qe))     # existing rev/op row for this (sym, quarter)
        std = con = None; annStd = annCon = None
        rStd = rCon = oStd = oCon = eStd = eCon = None; rFin = 0
        for f in sorted(filings, key=lambda x: x["ann"]):
            is_con = "consol" in (f.get("basis") or "").lower()
            # Skip the ~1 MB iXBRL fetch only when BOTH net-profit AND rev/op for this basis are
            # already known (a bank is "known" once flagged fin). Keeps the wide window cheap.
            pat_have = ((is_con and (con is not None or (existing and existing[3] is not None))) or
                        (not is_con and (std is not None or (existing and existing[1] is not None))))
            # NB: no `er[6]` (fin-flag) shortcut here — banks/NBFCs/insurers all carry per-basis
            # rev/op since 2026-07, so "flagged fin" no longer means "nothing to extract"; the old
            # shortcut left e.g. HDFCLIFE's consolidated cell permanently None once std was stored.
            rev_have = ((is_con and (rCon is not None or (er and er[1] is not None))) or
                        (not is_con and (rStd is not None or (er and er[0] is not None))))
            if pat_have and rev_have:
                continue
            try:
                xml = B._get(f["xbrl"], headers={"User-Agent": B.UA, "Referer": "https://www.nseindia.com/"}, timeout=30)
            except Exception:
                continue
            s, c = B.xbrl_profit(xml, basis_hint=f.get("basis"))
            a = None if f["ann"] == "99999999" else int(f["ann"])
            if std is None and s is not None: std, annStd = s, a
            if con is None and c is not None: con, annCon = c, a
            try:   # rev/op is best-effort: never let it break the net-profit refresh
                rs2, os2, es2, rc2, oc2, ec2, fin2 = xbrl_revop(xml, basis_hint=f.get("basis"))
                if rStd is None and rs2 is not None: rStd, oStd, eStd = rs2, os2, es2
                if rCon is None and rc2 is not None: rCon, oCon, eCon = rc2, oc2, ec2
                if fin2: rFin = 1
            except Exception:
                pass
        # --- upsert net profit (sf_fundamentals.json): only FILL missing fields (point-in-time) ---
        if std is not None or con is not None:
            if sym not in data: data[sym] = []; newsyms += 1
            rec = data[sym]
            row = next((x for x in rec if x[0] == int(qe)), None)
            if row:
                upd = False
                if std is not None and row[1] is None: row[1], row[2] = std, annStd; upd = True
                if con is not None and row[3] is None: row[3], row[4] = con, annCon; upd = True
                if upd: changed += 1
            else:
                rec.append([int(qe), std, annStd, con, annCon]); rec.sort(key=lambda x: x[0]); changed += 1
        # --- upsert revenue / operating profit (sf_revop.json): fill-only, parallel structure ---
        if rStd is not None or rCon is not None or rFin:
            d = revop.setdefault(sym, {})
            rr = d.get(str(qe)) or [None, None, None, None, None, None, 0, None, None]
            if len(rr) < 9: rr += [None] * (9 - len(rr))   # pad legacy 7-element rows -> add ebit slots
            if rr[0] is None and rStd is not None: rr[0] = rStd
            if rr[1] is None and rCon is not None: rr[1] = rCon
            if rr[2] is None and oStd is not None: rr[2] = oStd
            if rr[3] is None and oCon is not None: rr[3] = oCon
            if rr[4] is None and std  is not None: rr[4] = std
            if rr[5] is None and con  is not None: rr[5] = con
            if rFin: rr[6] = 1
            if rr[7] is None and eStd is not None: rr[7] = eStd
            if rr[8] is None and eCon is not None: rr[8] = eCon
            d[str(qe)] = rr; revop_changed += 1

    # No-subsidiary auto-fill: a company that has never reported con != std files only a standalone
    # XBRL (no consolidated), so the loop above leaves con=None. For such companies consolidated ==
    # standalone (accounting identity), so set con=std for the quarters just added. This closes the
    # gap that previously needed a manual con=std pass every quarter (AUBANK, COLPAL, SBICARD, ...).
    # Safe because a company WITH subsidiaries files a consolidated XBRL -> con is read above and this
    # never fires; and we require >=3 historical con==std quarters before trusting the no-sub pattern.
    def is_nosub(rec):
        pairs = [(r[1], r[3]) for r in rec if r[1] is not None and r[3] is not None]
        if len(pairs) < 3: return False
        for s, c in pairs:
            if abs(c - s) > 0.01: return False   # con == std (to the paisa) everywhere -> no subsidiary
        return True
    for (sym, qe) in byq:
        rec = data.get(sym)
        if not rec or not is_nosub(rec): continue
        row = next((x for x in rec if x[0] == int(qe)), None)
        if row and row[1] is not None and row[3] is None:
            row[3], row[4] = row[1], row[2]; changed += 1   # con = std (no consolidatable subsidiary)

    # Fill a missing result-announce date from the sibling basis: con & std are filed at the SAME
    # board meeting, so if a quarter has a profit value but a null announce-date on one basis while
    # the other basis HAS the date, copy it. Without this the engine skips that quarter (needs
    # annDate<=rebalance) and silently uses a STALE older quarter -> wrong profitYoY. Common for
    # insurers (IRDAI format) + recent IPOs, e.g. NIACL Q4FY24 npCon present, annCon null. (2026-06-23)
    for _sym, _rec in data.items():
        for _r in _rec:
            if len(_r) < 5: continue
            if _r[3] is not None and _r[4] is None and _r[2] is not None: _r[4] = _r[2]; changed += 1
            if _r[1] is not None and _r[2] is None and _r[4] is not None: _r[2] = _r[4]; changed += 1

    if not changed and not newsyms and not revop_changed:
        print("no new earnings — nothing to update"); return
    json.dump(data, open(DOCS, "w"), separators=(",", ":"))
    json.dump(revop, open(REVOP, "w"), separators=(",", ":"))
    open(MARK, "w").write(today.isoformat())
    print("upserted %d quarters (%d new symbols, %d rev/op cells); %d symbols total" % (
        changed, newsyms, revop_changed, len(data)))

if __name__ == "__main__":
    main()
