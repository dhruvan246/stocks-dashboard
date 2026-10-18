"""Own-line DII from BSE's Table III service (Corp_shpSec_SHPPubShold_ng) + Table I base, for PIT N500 quarters the XBRL reader
could not read (Dec-2015/Mar-2016 page-filed quarters and any listed quarter without an XBRL). Same rules as ownline_dii.py.
python3 t3dii.py [SYM ...] -> t3dii.json"""
import os, sys, json, re, collections, importlib.util as U
W = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.expanduser("~/stocks-cache/shp/audit_all")); A = sys.argv; sys.argv = ["x"]
import show as S
sp = U.spec_from_file_location("od", os.path.join(W, "ownline_dii.py")); od = U.module_from_spec(sp); sp.loader.exec_module(od); sys.argv = A
DOMLV = re.compile(r"(?i)^(mutual\s*funds?|venture\s+capital|alternat\w*\s+\w*v\w*t|financial\s+institutions?\s*/?\s*banks?|insurance\s+compan|provident\s+funds?|pension)")
NOTLV = re.compile(r"(?i)foreign\s+(portfolio|venture)|\bfpi|\bfii")
def read(sym, q):
    c = S.code_of(sym)
    if not c: return {"err": "no BSE code"}
    qi = S.qtrid(q)
    try: t1 = S.t1api(c, qi) or {}
    except Exception as e: return {"err": "t1 " + str(e)[:60]}
    codes = {r.get("Fld_Code"): r.get("Fld_TotalNoOfShares") for r in (t1.get("Table1") or [])}
    if not codes.get("STABC"): return {"err": "no Table I total"}
    base = codes["STABC"] - (codes.get("STC1") or 0)
    j = S.api("Corp_shpSec_SHPPubShold_ng", c, qi) or {}
    rows = j.get("Table1") or []
    if not rows: return {"err": "no Table III"}
    sec = "B1"; dii = 0; ev = []; x = []; amb = []; anyB1 = []; anyB3 = []; b1tot = {}; b3any = None
    for r in rows:
        lv = (r.get("Fld_Level") or "").strip(); nm = (r.get("Fld_ShareHolderName") or "").strip(); sh = r.get("Fld_TotalNoOfShares") or 0; dr = r.get("Fld_NoOfDRShares") or 0
        if lv.startswith("Sub Total B1"): sec = "B2"; continue
        if lv.startswith("Sub Total B2"): sec = "B3"; continue
        if lv.startswith("Sub Total B3") or lv.startswith("B=B1"): sec = "END"; continue
        if re.match(r"(?i)individual|nbfc", lv) and sec in ("B1", "B2"): sec = "B3"
        if sec == "B1":
            if lv.lower().startswith("any other"):
                if nm: anyB1.append((nm, sh))
                else: b1tot["any"] = sh
            elif not nm and DOMLV.match(lv) and not NOTLV.search(lv):
                dii += sh - dr; ev.append(("line:" + lv[:40], sh))
                if dr: ev.append(("DR-underlying out:" + lv[:40], -dr))
        elif sec == "B3":
            if lv.lower().startswith("nbfc") and not nm: dii += sh; ev.append(("NBFC row", sh))
            elif lv.lower().startswith("any other"):
                if nm: anyB3.append((nm, sh, r.get("Fld_NoOfShareHolders")))
                else: b3any = sh
    # institutions Any Other
    tot = b1tot.get("any", 0); s_items = sum(s for n, s in anyB1)
    if anyB1 and s_items != tot: amb.append(("B1 Any-Other named rows sum %d != block %d" % (s_items, tot), tot))
    named = 0
    # 18-Oct (§164s part 54): named holders listed beside the QIB category row that holds them (YESBANK Dec-21: block 1,679,167,699 =
    # QIB 1,678,940,301 + OCB 227,398; LIC 1,250,015,999 and HDFC Life 333,717,000 sit inside the QIB row) are not added twice.
    qibrows = [s for n, s in anyB1 if od.QIB.search(n) and not re.search(r"(?i)foreign|fpi|fii", n)]
    inq = [(n, s) for n, s in anyB1 if not od.QIB.search(n) and od.indian_inst(n) and not od.tagged(n)]
    inq = inq if (qibrows and inq and tot and s_items - sum(s for n, s in inq) == tot and sum(s for n, s in inq) <= max(qibrows)) else []
    for n, s in anyB1:
        c = od.lab_class(n)
        if (n, s) in inq: amb.append(("inst holder inside the counted QIB row (not added twice):" + n[:50], s)); continue
        if od.QIB.search(n) and not re.search(r"(?i)foreign|fpi|fii", n): dii += s; ev.append(("inst-cat QIB label:" + n[:50], s)); named += s
        elif c == "other" and od.INSTDOM.search(n) and not od.D.noninst_label(n): dii += s; ev.append(("inst-cat domestic label:" + n[:50], s)); named += s
        elif od.indian_inst(n) and not od.tagged(n): dii += s; ev.append(("inst-cat named Indian institution:" + n[:50] + " | " + od.indian_inst(n)[:50], s)); named += s
        else: x.append(("x:inst Any-Other row not DII (%s):" % c + n[:50], s))
    if tot > s_items: x.append(("x:institutions Any-Other unnamed rest", tot - s_items))
    # non-institutions Any Other: register-proven named rows / domestic-institution labels
    # BSE's Table III lists the block's category rows ('Bodies Corporate', 'Trusts' ...) and named holders inside them as the same
    # kind of row. 18-Oct (§164s part 54): a named holder lies INSIDE a category when the category rows alone add up to the block -
    # the single-holder rows whose removal leaves exactly the block (M&M Dec-15: 47,582,388 - ICICI Pru Life 12,380,408 = 35,201,980;
    # INDUSINDBK Dec-15: removing HDFC Std Life, ICICI Pru Life and both Hinduja rows leaves 79,371,417). Such a register holder counts,
    # except when it may sit in a category row we already count (QIB / AIF / insurer label) - then it is already inside that count.
    s3 = sum(s for n, s, k in anyB3); cnt = []; holders = None
    for n, s, k in anyB3:
        core = re.sub(r"(?i)^\s*(?:(?:any\s+)?others?\s*[:\-]?\s*)+", "", n).strip()
        cnt.append(not re.search(r"(?i)foreign|fpi|fii|overseas|\bnri\b|non.?resident", n) and bool(od.QIB.search(n) or re.match(r"(?i)(" + od.AIFLAB + r"|aifs?\b|(indian\s+)?insurance\s+compan(y|ies)|mutual\s+funds?|pension\s+funds?|provident\s+funds?)", core)))
    if b3any is not None and s3 > b3any:
        isreg = lambda n: bool(od.reg(n)) and "THREPSI" not in n.upper() and not od.tagged(n)
        single = [i for i, (n, s, k) in enumerate(anyB3) if (k in (1, None) or isreg(n)) and not cnt[i]]    # a register name with several folios is still one holder (BEML Dec-15 Max Life, 11 folios)
        if len(single) <= 18:
            import itertools
            fits = [set(c) for m in range(1, len(single) + 1) for c in itertools.combinations(single, m) if sum(anyB3[i][1] for i in c) == s3 - b3any]
            if fits: holders = set.intersection(*fits)       # a row is a holder only if every exact split agrees
            if fits and len(fits) == 1:     # a register-named row the split leaves as a CATEGORY is a block of institutions (SPANDANA Sep-19: 5 life insurers, 4,414,300)
                for i in single:
                    if i not in holders and isreg(anyB3[i][0]) and (anyB3[i][2] or 0) > 1: cnt[i] = True
    inside = {}      # holder -> 'free' (fits only uncounted categories) / 'counted' (could sit in a counted row) / None (fits no category row)
    if holders:
        room = {j: anyB3[j][1] for j in range(len(anyB3)) if j not in holders}
        for i in sorted(holders, key=lambda i: -anyB3[i][1]):
            s = anyB3[i][1]; free = [j for j in room if not cnt[j] and room[j] >= s]; cn = [j for j in room if cnt[j] and room[j] >= s]
            if cn: inside[i] = "counted"
            elif free: inside[i] = "free"; room[max(free, key=lambda j: room[j])] -= s
            else: inside[i] = None     # MPHASIS Sep-19: ICICI Pru 2,637,649 and HDFC Life 2,847,380 > Bodies Corporate 1,286,133 - in no subtotal
    for i, (n, s, k) in enumerate(anyB3):
        core = re.sub(r"(?i)^\s*(?:(?:any\s+)?others?\s*[:\-]?\s*)+", "", n).strip()
        if re.search(r"(?i)foreign|fpi|fii|overseas|\bnri\b|non.?resident", n): continue
        if cnt[i]:
            if holders is not None and i in holders: continue
            dii += s; ev.append(("noninst domestic-institution row:" + n[:50], s))
        elif od.reg(n) and "THREPSI" not in n.upper() and not od.tagged(n):
            if b3any is not None and s3 > b3any:
                if holders is None or i not in holders: amb.append(("B3 Any-Other rows sum %d > block %d, no exact category split holds register row %s" % (s3, b3any, n[:40]), s)); continue
                if inside.get(i) == "counted": amb.append(("register holder %s may sit inside a counted QIB/AIF/insurer row (not added twice)" % n[:40], s)); continue
                if inside.get(i) is None: amb.append(("register holder %s is larger than every category row left: in no subtotal (not added)" % n[:40], s)); continue
                dii += s; ev.append(("noninst register (inside a category; category rows sum to the block):" + n[:50] + " | " + od.reg(n)[:50], s)); continue
            dii += s; ev.append(("noninst register:" + n[:50] + " | " + od.reg(n)[:50], s))
    return {"dii": round(dii / base * 100, 6), "dii_sh": dii, "base": base, "ev": ev, "x": x, "amb": amb, "code": c, "qtrid": qi}
if __name__ == "__main__":
    REPO = os.environ.get("OWNLINE_REPO") or os.path.expanduser("~/stocks-dashboard")
    H = json.load(open(os.path.join(REPO, "scripts", "shp_history.json"))); only = set(A[1:])
    O = json.load(open(os.path.join(W, "ownline_dii.json")))
    out = {}; C = collections.Counter()
    for sym, qs in H.items():
        if sym.startswith("_") or (only and sym not in only): continue
        for q, cell in qs.items():
            if not ("2015-12-31" <= q <= "2022-06-30") or not od.member(sym, q): continue
            k = sym + "|" + q
            if not only and k in O and O[k].get("own") is not None: continue
            r = read(sym, q); C["err" if "err" in r else "ok"] += 1
            out[k] = {"store": cell[:7], "read": r}
    json.dump(out, open(os.path.join(W, "t3dii%s.json" % ("_partial" if only else "")), "w")); print(len(out), dict(C))
