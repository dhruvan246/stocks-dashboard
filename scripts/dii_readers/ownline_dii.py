"""Own-line DII reader (user 2026-10-13: DII follows each filing's own line; the unnamed institutions Any-Other rest is FII even
beside Indian named holders; DR-underlying shares are out of DII). 2015-22 XBRL form (Dec-2015..Jun-2022), PIT Nifty 500 cells.
DII = MF + AIF + VCF + FI/Banks + Insurance + PF/Pension lines as filed (foreign holders filed there STAY)
    - shares underlying depository receipts (column VI) on those lines
    + NBFCs registered with RBI row
    + institutions Any-Other category rows whose own label is a domestic institution type (insurance, bank, MF, QIB, AIF ...)
    + named holders inside the unlabelled Any-Other part that an official register proves Indian (IRDAI/LIC/RBI/PFRDA/SEBI MF/UTI)
    + non-institutions category rows labelled QIB
    + non-institutions named holders an official register proves Indian institutions (user 6-Oct 'register wins')
on the filing's own base (total - its own custodian/DR row; §226 / part 77). Output r5/ownline_dii.json.
python3 ownline_dii.py [SYM ...]"""
import os, sys, json, re, collections
OL = os.path.expanduser("~/stocks-cache/shp/ownline/ownline.py")
src = open(OL).read(); src = src[:src.index("\ndef main():")]
G = {"__file__": OL, "__name__": "ownline_lib"}; sv = sys.argv; sys.argv = ["x"]; exec(compile(src, OL, "exec"), G); sys.argv = sv
facts, lab_class, name_label, member, qe_of, D, REG, OWNTAG, PUBX = (G[k] for k in ("facts", "lab_class", "name_label", "member", "qe_of", "D", "REG", "OWNTAG", "PUBX"))
import xml.etree.ElementTree as ET
REPO = os.environ.get("OWNLINE_REPO") or os.path.expanduser("~/stocks-dashboard"); LISTS = os.path.expanduser("~/stocks-cache/shp/bse_all"); W = os.path.dirname(os.path.abspath(__file__))
DOMMEM = ("MutualFundsOrUtiMember", "AlternativeInvestmentFundsMember", "VentureCapitalFundsMember", "FinancialInstitutionOrBanksMember",
          "InsuranceCompaniesMember", "ProvidentFundsOrPensionFundsMember")
INSTDOM = re.compile(r"(?i)insur|assurance|mutual\s+funds?|\bbanks?\b|financial\s+institution|provident|pension|\bnbfc|alternat\w*\s+\w*v\w*t|venture\s+capital|qualified\s+ins\w*|\bqibs?\b|\buti\b")   # 18-Oct: misspelt labels ('ALTERNATE INVSTMENT', 'Investmetnt', 'Investments Fund', 'Insititional')
QIB = re.compile(r"(?i)\bq\w{3,9}\s*in\w*\s*buy\w*|qualified\s*ins\w*|\bqibs?\b")     # 18-Oct: 'Qualified Insititional Investor' (RADICO Mar-21)
AIFLAB = r"alternat\w*\s+\w*v\w*t\w*\s*funds?"      # 18-Oct: 'ALTERNATE INVSTMENT FUND' (NCC Sep-17), 'Alternative Unvestment Fund' (DALBHARAT Sep-19), 'INVETMENT' (PFC Mar-19), 'Alternative Investments Fund' (PGHL Sep-18), 'Alternative Investmetnt Fund' (SRF Dec-18)
AMC = re.compile(r"(?i)\b(sbi|hdfc|icici\s+prudential|nippon\s+india|reliance|kotak(\s+mahindra)?|aditya\s+birla(\s+sun\s*life)?|birla\s+sun\s*life|axis|uti|dsp(\s+blackrock)?|sundaram|tata|l\s*&\s*t|idfc|mirae\s+asset|motilal\s+oswal|invesco\s+india|edelweiss|pgim\s+india|dhfl\s+pramerica|canara\s+robeco|baroda|lic|ppfas|parag\s+parikh|mahindra|iifl|bandhan|quantum|boi\s+axa|idbi|franklin\s+india)\b[^,]*\b(mutual\s+fund|m\.?f\.?|a/?c|scheme|fund)\b")
_AMFI = None
def _amfi():
    global _AMFI
    if _AMFI is None:
        _AMFI = set()
        for line in open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "amfi_navall.txt"), encoding="utf-8", errors="ignore"):
            parts = line.split(";")
            if len(parts) > 3 and parts[0].strip().isdigit():
                base = re.split(r"\s+-\s+|\(", parts[3])[0]
                _AMFI.add(re.sub(r"[^A-Z0-9]", "", base.upper()))
    return _AMFI
def sebi_mf(nm):
    """An Indian mutual-fund holder, by register only (FII session 13-Oct: a fund-house name + 'fund' is not proof -
    SUNDARAM INDIA MIDCAP FUND / TATA INDIAN OPPORTUNITIES FUND / KOTAK INDIA PRIVATE EQUITY FUND are offshore):
    the holder names a SEBI-registered mutual fund trust ('... MUTUAL FUND', '... TRUSTEE CO ... A/C'), or the scheme name
    is on AMFI's scheme list (portal.amfiindia.com/spages/NAVAll.txt, saved 2026-10-13)."""
    n = nm or ""
    if re.search(r"(?i)foreign|overseas|\boffshore\b", n): return None
    if re.search(r"(?i)\bfunds\b\s*[-\u2013:]|\bsicav\b|\bmauritius\b|\bcayman\b|\bluxembourg\b|\bplc\b|\bl\.?p\.?\b|\bfpi\b|\bfii\b", n): return None
    if re.search(r"(?i)\bmutual\s+fund\b", n): return "SEBI-registered mutual fund (holder names the fund: '%s')" % n[:60]
    if re.search(r"(?i)\btrustee\b.*\ba/?c\b", n): return "SEBI-registered mutual fund trustee account ('%s')" % n[:60]
    k = re.sub(r"[^A-Z0-9]", "", re.split(r"\s+-\s+|\(", n)[0].upper())
    if len(k) >= 8 and k in _amfi(): return "AMFI-listed mutual fund scheme ('%s')" % n[:60]
    return None
SEBI_AIF = {   # SEBI register of AIFs, searched 2026-10-14 from www.sebi.gov.in (Registered Alternative Investment Funds); exact names only
    "ABAKKUSGROWTHFUND": "SEBI-registered AIF: Abakkus Growth Fund, IN/AIF3/18-19/0550 (from 2018-06-05)",
    "ABBAKUSGROWTHFUND": "SEBI-registered AIF: Abakkus Growth Fund, IN/AIF3/18-19/0550 (from 2018-06-05; name as misspelt in the filing)",
    "TRUENORTHFUNDVLLP": "SEBI-registered AIF: True North Fund V LLP, IN/AIF2/16-17/0303 (from 2017-02-28)",
    "MULTIPLESPRIVATEEQUITYFUND": "SEBI-registered AIF: Multiples Private Equity Fund, IN/AIF2/13-14/0089 (from 2013-12-16)",
    "MULTIPLESPRIVATEEQUITYFUNDIILLP": "SEBI-registered AIF: Multiples Private Equity Fund II LLP, IN/AIF2/15-16/0151 (from 2015-05-26)",
    "PIOPPORTUNITIESFUNDI": "SEBI-registered AIF: PI Opportunities Fund-I, IN/AIF2/15-16/0215 (from 2016-03-31)",
    "WHITEOAKINDIAEQUITYFUND": "SEBI-registered AIF: White Oak India Equity Fund, IN/AIF3/17-18/0498 (from 2017-10-26)",
    "PIONEERINVESTMENTFUND": "SEBI-registered AIF: Pioneer Investment Fund, IN/AIF3/16-17/0272 (from 2016-11-01)"}
def sebi_aif(nm):
    k = re.sub(r"[^A-Z0-9]", "", re.sub(r"(?i)[-\s]*(?:\b1\b|\bI\b)\s*$", "", (nm or "").upper().strip()) if "PIOPPORTUNITIES" not in re.sub(r"[^A-Z0-9]", "", (nm or "").upper()) else (nm or "").upper())
    k2 = re.sub(r"[^A-Z0-9]", "", (nm or "").upper())
    return SEBI_AIF.get(k) or SEBI_AIF.get(k2)
ASSOC_BANKS = re.compile(r"(?i)\bstate\s+bank\s+of\s+(hyderabad|bikaner\s*(?:&|and)\s*jaipur|mysore|travancore|patiala|indore|saurashtra)\b")
def reg(nm):
    if re.match(r"(?i)\s*(bod(y|ies)\s+corporates?|other\s+than|others?\s*\()", nm or ""): return None      # a category label that merely mentions a holder ('Body Corporate (other than ICICI Prudential Life ...)')
    if re.search(r"(?i)\bindustrial\s+finance\s+corporation\s+of\s+india\b|^\s*ifci\b", nm or ""): return "IFCI Ltd (Industrial Finance Corporation of India) - Government of India public financial institution, Companies Act 2013 s.2(72)"   # 18-Oct: TIMKEN 2022 'Other INDUSTRIAL FINANCE CORPORATION OF INDIA'
    if ASSOC_BANKS.search(nm or ""): return "RBI scheduled commercial bank (SBI associate bank, merged into SBI 2017): " + ASSOC_BANKS.search(nm).group(0)
    n = re.sub(r"(?i)\bnps\s+trusts\b", "NPS TRUST", nm or "")
    n = re.sub(r"(?i)\bpvt\.?\s+(?=equity\b)", "Private ", n)   # 18-Oct: ARVIND Dec-15 'Multiples Pvt Equity Fund' = Multiples Private Equity Fund (same 2,732,400 shares every quarter)
    a = sebi_aif(n)
    if a: return a
    if re.search(r"(?i)\baegon\s+(religare\s+)?life\b|\bbandhan\s+life\b", n): return "IRDAI-registered life insurer no. 138: Bandhan Life Insurance Ltd (formerly Aegon Life / Aegon Religare Life)"
    return REG.register(n)
def indian_inst(nm):
    r = reg(nm or "")
    if r and "THREPSI" not in (nm or "").upper(): return r     # Threpsi Care's AIF placement came from another filing (retired 11-Oct/13-Oct)
    if re.search(r"(?i)\bunit\s+trust\s+(of\s+)?india\b|^\s*uti\b", nm or ""): return "Unit Trust of India / UTI"
    return sebi_mf(nm)
def tagged(nm): return bool(OWNTAG.search(nm or "") and not PUBX.match((nm or "").strip()))
def dr_cols(txt):
    root = ET.fromstring(txt); strip = lambda t: t.split("}", 1)[-1]; ctx = {}
    for c in root.iter():
        if strip(c.tag) != "context": continue
        mems = [(m.text or "").split(":")[-1].strip() for m in c.iter() if strip(m.tag) == "explicitMember"]
        typed = any(strip(m.tag) == "typedMember" for m in c.iter())
        if len(mems) == 1 and not typed: ctx[c.get("id")] = mems[0]
    out = {}
    for f in root.iter():
        if strip(f.tag) == "NumberOfSharesUnderlyingOutstandingDepositoryReceipts" and f.get("contextRef") in ctx:
            try: out[ctx[f.get("contextRef")]] = int(float(f.text))
            except Exception: pass
    return out
def own_dii(txt):
    top, rows = facts(txt); drc = dr_cols(txt)
    total = top.get("ShareholdingPatternMember"); c1 = top.get("CustodianOrDRHolderMember") or 0
    if not total: return None
    base = total - c1 if c1 else total; ev = []; dii = 0
    if top.get("InstitutionsDomesticMember") is not None:      # SEBI 2022 form: the filing's own Institutions (Domestic) block
        dii = top["InstitutionsDomesticMember"]; ev.append(("line:InstitutionsDomestic", dii)); amb = []
        for m, d in drc.items():
            if d and m not in ("CustodianOrDRHolderMember", "SharesHeldByNonPromoterNonPublicShareholdersMember", "ShareholdingPatternMember", "EmployeeBenefitsTrustsMember") and ("Domestic" in m or m in DOMMEM or m in ("BanksMember", "NBFCsRegisteredWithRbiMember", "OtherFinancialInstitutionsMember", "SovereignWealthFundsDomesticMember")):
                dii -= d; ev.append(("DR-underlying out:" + m.replace("Member", ""), -d))
        for ax, seq, kind, cat, name, sh in rows:
            if ax not in ("BodiesCorporate", "OtherNonInstitutions", "ForeignCompanies", "AssociateCompaniesOrSubsidiaries", "NonResidentIndians") or not name or kind.lower().startswith("categ"): continue   # public non-institution holder rows only
            r = reg(name)
            if r and "THREPSI" not in name.upper() and not tagged(name): dii += sh; ev.append(("noninst register (2022 form):" + ax[:25] + " " + name[:40] + " | " + r[:60], sh))
        return {"dii": round(dii / base * 100, 6), "dii_sh": dii, "base": base, "total": total, "c1": c1, "ev": ev, "amb": amb, "form": "2022"}
    for m in DOMMEM:
        v = top.get(m, 0)
        if v: dii += v; ev.append(("line:" + m.replace("Member", ""), v))
        d = drc.get(m, 0)
        if d: dii -= d; ev.append(("DR-underlying out:" + m.replace("Member", ""), -d))
    nb = top.get("NBFCsRegisteredWithRbiMember", 0)
    if nb: dii += nb; ev.append(("NBFC row", nb))
    oi_top = top.get("OtherInstitutionsMember", 0)
    amb = []; xout = []
    def place(axis, cats, holders, rest, counted=lambda c: False):
        """each named holder -> the ONE container that can hold it: a category row of the same axis whose own text matches the
        holder's category text and still has room, or (institutions) the unlabelled rest. A holder whose own category row is too
        small falls back only to a row we already count (CREDITACC Sep-19: ICICI Pru 'Bodies Corporate' 1.79M inside the 2.66M QIB
        row) and only while that row has room left (AMBER Sep-20: Tata AIA already fills the QIB row, so Edelweiss AIF stays out
        of it). -> {holder: container | 'rest' | 'nowhere' | None (ambiguous)}"""
        room = {c: c[5] for c in cats}; room["rest"] = rest if axis == "OtherInstitutions" else 0; out = {}; later = []
        for h in holders:       # a holder equal to a row to the share is that row's sole member (SPANDANA Dec-21: ICICI Pru 3,054,560 = the QIB row)
            eq = [c for c in cats if c[5] == h[5] and room[c] == c[5]]
            if len(eq) == 1: out[h] = eq[0]; room[eq[0]] = 0
        for h in sorted([h for h in holders if h not in out], key=lambda x: -x[5]):
            cand = [c for c in cats if (c[3] or "").strip().lower() == (h[3] or "").strip().lower() and room[c] >= h[5]]
            if room["rest"] >= h[5]: cand.append("rest")
            same = len(set(bool(counted(c)) if c != "rest" else False for c in cand)) == 1     # every candidate counted, or none: the answer is the same either way
            if len(cand) == 1 or (cand and same): out[h] = cand[0]; room[cand[0]] -= h[5]
            elif cand: out[h] = None
            else: later.append(h)
        for h in later:
            cand = [c for c in cats if counted(c) and room[c] >= h[5]]
            if cand: out[h] = cand[0]; room[cand[0]] -= h[5]     # every fallback candidate is a counted row
            else: out[h] = "nowhere"
        return out
    # ---- institutions Any Other
    cats = [r for r in rows if r[0] == "OtherInstitutions" and (r[2].lower().startswith("categ") or (not r[2] and not r[4]))]
    hold = [r for r in rows if r[0] == "OtherInstitutions" and r not in cats]
    labelled = 0; catdii = {}
    for c in cats:
        ax, seq, kind, cat, name, sh = c; lab = (cat + " " + name).strip(); cl = lab_class(lab)
        if cl == "unlabelled": catdii[c] = None; continue
        labelled += sh
        core = re.sub(r"(?i)^\s*(?:(?:any\s+)?others?\s*[:\-]?\s*)+", "", lab).strip()
        if QIB.search(core) and not re.search(r"(?i)foreign|fpi|fii", core): dii += sh; catdii[c] = True; ev.append(("inst-cat QIB label:" + lab[:50], sh))
        elif cl == "other" and INSTDOM.search(core) and not D.noninst_label(core): dii += sh; catdii[c] = True; ev.append(("inst-cat domestic label:" + lab[:50], sh))
        elif cl in ("other", "unknown") and indian_inst(core) and not tagged(name): dii += sh; catdii[c] = True; ev.append(("inst-cat named Indian institution:" + lab[:50] + " | " + indian_inst(core)[:60], sh))   # 18-Oct: an 'Other;' label asks the register too ('Other;Unit Trust of India', SPARC Jun-16)
        elif cl == "unknown" and re.match(r"(?i)" + AIFLAB + r"\s*(\(.*\))?\s*$", core): dii += sh; catdii[c] = True; ev.append(("inst-cat AIF label:" + lab[:50], sh))
        else: catdii[c] = False; ev.append(("inst-cat not DII (%s):" % cl + lab[:50], 0)); xout.append(("x:inst-cat not DII (%s):" % cl + lab[:50], sh))
    rest = max(0, oi_top - labelled)
    pl = place("OtherInstitutions", [c for c in cats if catdii.get(c) is not None], hold, rest, lambda c: bool(catdii.get(c)))
    for h in hold:
        nm = (h[4] or "").strip(); where = pl[h]
        if where is None: amb.append(("inst holder fits several rows: " + nm[:40], h[5])); continue
        if where == "nowhere" and h[5] > oi_top:
            ev.append(("inst holder larger than the whole institutions Any-Other block (not added):" + nm[:40], 0)); xout.append(("x:named holder larger than its whole block:" + nm[:40], h[5])); continue
        if where != "rest" and where != "nowhere" and catdii.get(where): xout.append(("h:named inside counted row '%s':" % (where[4] or where[3])[:30] + nm[:40], h[5])); continue
        if tagged(nm) or name_label(nm) in ("fii", "fdi?", "company", "dr"): continue
        hc = lab_class(h[3])
        if where == "rest" and hc == "other" and INSTDOM.search(h[3] or "") and not D.noninst_label(h[3] or ""): dii += h[5]; ev.append(("inst-holder own domestic cat:" + (h[3] or "")[:30] + " " + nm[:40], h[5])); continue
        r = indian_inst(nm)
        if r: dii += h[5]; ev.append(("inst-holder register (%s):" % ("rest" if where == "rest" else "row " + (where[4] or where[3])[:20] if where != "nowhere" else "no row") + nm[:40] + " | " + r[:60], h[5]))
    if rest:
        ev.append(("inst-rest unnamed/unproven -> not DII (FII side, user 13-Oct)", 0))
        counted_in_rest = sum(h[5] for h in hold if pl.get(h) == "rest" and any(e[0].endswith(h[4][:40]) or (h[4][:40] in e[0]) for e in ev if e[1]))
        xout.append(("x:institutions Any-Other unnamed/unproven rest", max(0, rest - counted_in_rest)))
    # ---- non-institutions
    ncats = [r for r in rows if r[0] == "OtherNonInstitutions" and (r[2].lower().startswith("categ") or (not r[2] and not r[4]))]
    nhold = [r for r in rows if r[0] == "OtherNonInstitutions" and r not in ncats and r[4]]
    ncatdii = {}
    for c in ncats:
        lab = (c[3] + " " + c[4]).strip(); core = re.sub(r"(?i)^\s*(?:(?:any\s+)?others?\s*[:\-]?\s*)+", "", lab).strip()
        if re.search(r"(?i)foreign|fpi|fii|overseas|\bnri\b|non.?resident", lab): ncatdii[c] = False; continue
        if QIB.search(lab) or re.match(r"(?i)(" + AIFLAB + r"|aifs?\b|(indian\s+)?insurance\s+compan(y|ies)|mutual\s+funds?|pension\s+funds?|provident\s+funds?|(indian\s+)?financial\s+institutions?|nationali[sz]ed\s+banks?|venture\s+capital|(systematically\s+important\s+)?nbfcs?\b|non.?banking\s+financial\s+compan)", core):
            dii += c[5]; ncatdii[c] = True; ev.append(("noninst domestic-institution row:" + lab[:50], c[5])); continue
        if c[4] and reg(c[4]) and "THREPSI" not in c[4].upper() and not tagged(c[4]): dii += c[5]; ncatdii[c] = True; ev.append(("noninst cat named register:" + lab[:50] + " | " + reg(c[4])[:60], c[5])); continue
        ncatdii[c] = False
    npl = place("OtherNonInstitutions", ncats, nhold, 0, lambda c: bool(ncatdii.get(c)))
    for h in nhold:
        nm = h[4].strip(); where = npl[h]
        if tagged(nm): continue
        r = reg(nm)
        if not r or "THREPSI" in nm.upper(): continue
        if where is None: amb.append(("noninst register holder fits several rows: " + nm[:40], h[5])); continue
        if where != "nowhere" and ncatdii.get(where): xout.append(("h:named inside counted row '%s':" % (where[4] or where[3])[:30] + nm[:40], h[5])); continue
        oni_top = top.get("OtherNonInstitutionsMember", 0)
        if where == "nowhere" and h[5] > oni_top and h[3] and not re.search(r"(?i)individual", h[3] or ""):
            # larger than the whole non-institutions Any-Other block it is filed in: it sits in no sub-total of this filing
            # (GSFC Mar-17 SBI Life 78,112,155 > block 54,258,731; MPHASIS Sep-19 ICICI Pru + HDFC Life > block 1,439,801) - not added
            ev.append(("noninst register holder larger than its whole block (not added):" + nm[:40], 0)); xout.append(("x:named holder larger than its whole block:" + nm[:40], h[5])); continue
        dii += h[5]; ev.append(("noninst register:" + (h[3] or "")[:25] + " " + nm[:40] + " | " + r[:60], h[5]))
    for ax, seq, kind, cat, name, sh in rows:      # excluded non-institution rows of an institution type (for gap matching only)
        if ax == "OtherNonInstitutions" and (kind.lower().startswith("categ") or (not kind and not name)) and re.search(r"(?i)foreign|fpi|fii|bank|insur|fund|financial|qualified|trust", (cat + " " + name)) and not any(e[0].endswith((cat + " " + name).strip()[:50]) for e in ev if e[1]):
            xout.append(("x:non-institutions row not DII:" + (cat + " " + name).strip()[:50], sh))
    for m in ("InstitutionsForeignPortfolioInvestorMember",):
        if top.get(m): xout.append(("x:FPI line", top[m]))
    return {"dii": round(dii / base * 100, 6), "dii_sh": dii, "base": base, "total": total, "c1": c1, "ev": ev, "amb": amb, "x": xout}
def main():
    H = json.load(open(os.path.join(REPO, "scripts", "shp_history.json"))); only = set(sys.argv[1:]); out = {}; miss = collections.Counter()
    for sym, qs in H.items():
        if sym.startswith("_") or (only and sym not in only): continue
        lp = os.path.join(LISTS, sym + ".json")
        if not os.path.exists(lp): miss["nolist"] += 1; continue
        L = json.load(open(lp)); L = L.get("Table", L) if isinstance(L, dict) else L; byq = collections.defaultdict(list)
        for r in L or []:
            q = qe_of(r.get("qtr")); f = (r.get("XbrlFile") or "").strip()
            if q and f: byq[q].append(r)
        for qe, cell in qs.items():
            lo, hi = ("2022-09-30", "2099-12-31") if os.environ.get("NEWFORM") == "1" else ("2015-12-31", "2022-06-30")
            if not (lo <= qe <= hi) or not member(sym, qe): continue
            fl = byq.get(qe)
            if not fl: miss["nofile"] += 1; continue
            res = []
            for r in fl:
                p = D.find_file(r["XbrlFile"].strip())
                if not p: continue
                try: o = own_dii(open(p, "rb").read())
                except Exception as e: o = {"err": str(e)[:80]}
                if o: o["file"] = r["XbrlFile"].strip(); o["status"] = r.get("status"); o["fdate"] = ((r.get("filing_date_time") if r.get("status") == "New" else r.get("revised_date_time")) or r.get("filing_date_time") or r.get("revised_date_time") or "")[:10]; res.append(o)
            if not res: miss["notcached"] += 1; continue
            import datetime as _dt
            def _gap(o):
                try: return abs((_dt.date.fromisoformat(o["fdate"]) - _dt.date.fromisoformat(cell[5])).days)
                except Exception: return 9999
            pick = sorted([o for o in res if "dii" in o], key=_gap)
            own = pick[0] if pick and _gap(pick[0]) <= 5 else None
            out["%s|%s" % (sym, qe)] = {"store": cell[:7], "reads": res, "own_doc": own["file"] if own else None, "own": own["dii"] if own else None}
    json.dump(out, open(os.path.join(W, ("ownline_dii_new" if os.environ.get("NEWFORM") == "1" else "ownline_dii") + ("_partial" if only else "") + ".json"), "w")); print(len(out), "cells read;", dict(miss))
if __name__ == "__main__": main()
