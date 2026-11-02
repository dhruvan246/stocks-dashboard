# -*- coding: utf-8 -*-
"""Full financials for every recent listing, read from the company's OWN documents (runbook §232).

WHY: a new listing's page had one quarter and a MoneyControl STANDALONE annual card, so a group like
Jio Platforms showed FY26 revenue 16,167 cr where its consolidated revenue is 1,46,885 cr.

SOURCES, in order: the final prospectus / RHP from NSE's offer-documents list (mainboard and SME), then
SEBI's public-issues filings (restated consolidated and standalone P&L, balance sheet, cash flow), then the company's results PDFs on NSE/BSE (their
comparative columns). Screener is never read here: scripts/gate_new_listings_screener.py compares
afterwards and writes nothing.

EVERY column must pass its statement's arithmetic (ipo_statement_reader.check_*) or it is HELD, never
written. Each results-PDF page must also reproduce the XBRL value we already store for its current
quarter before any comparative column is believed.

WRITES: docs/ipo_fin/<SYM>.json (one file per stock, read by the stock page's annual card only),
scripts/ipo_fin_state.json (retry ledger), and, fill-only, scripts/ipo_base_fills.json quarters
(backfill_ipo_bases.py --reapply then lands them in the fundamentals, runbook §25).

Run:  python3 -X utf8 scripts/fetch_new_listing_financials.py --only JPL
      python3 -X utf8 scripts/fetch_new_listing_financials.py --max-minutes 25      # the daily job
"""
import argparse, datetime, difflib, io, json, os, re, sys, tempfile, time, zipfile

import fitz
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import ipo_statement_reader as R

DOCS = os.path.join(HERE, "..", "docs")
OUT = os.path.join(DOCS, "ipo_fin")
STATE = os.path.join(HERE, "ipo_fin_state.json")
FILLS = os.path.join(HERE, "ipo_base_fills.json")
CACHE = os.environ.get("IPO_DOC_CACHE") or os.path.join(tempfile.gettempdir(), "ipo_docs")
SEBI_LIST = "https://www.sebi.gov.in/sebiweb/ajax/home/getnewslistinfo.jsp"
UA = {"User-Agent": "stocks-dashboard-research/1.0", "Accept": "text/html,application/pdf,*/*",
      "Accept-Language": "en", "Referer": "https://www.sebi.gov.in/"}
MAX_TRIES = 30           # a doc not on SEBI yet is retried daily for about a month
RECHECK_DAYS = 7         # a finished stock is re-read weekly until its next results land
sess = requests.Session(); sess.headers.update(UA)


def load(p, d):
    try: return json.load(open(p, encoding="utf-8"))
    except Exception: return d


def norm(x):
    x = re.sub(r"\b(limited|ltd|private|pvt|the|india|prospectus|rhp|addendum|to|and|price|band|corrigendum|notice)\b", " ", x.lower())
    return re.sub(r"[^a-z0-9]", "", x)


def sebi_rows(search="", pages=1):
    rows = []
    for smid, kind in ((12, "final"), (11, "rhp")):
        for pg in range(pages):
            data = {"nextValue": str(pg), "next": "n", "search": search, "fromDate": "", "toDate": "", "fromYear": "", "toYear": "",
                    "deptId": "", "sid": "3", "ssid": "15", "smid": str(smid), "ssidhidden": "15", "intmid": "-1",
                    "sText": "Filings", "ssText": "Public Issues", "smText": "", "doDirect": str(pg)}
            r = sess.post(SEBI_LIST, data=data, timeout=40)
            found = re.findall(r"href=.(https://www\.sebi\.gov\.in/filings/public-issues/[^\"' ]+)", r.text)
            rows += [(kind, u) for u in dict.fromkeys(found)]
            time.sleep(0.5)
            if not found: break
    return rows


NSE_DOCS = None


def nse_docs():
    """{norm(company): [(kind, url), ...]} final prospectus before RHP, from both NSE lists."""
    global NSE_DOCS
    if NSE_DOCS is None:
        import fetch_ipos as FI
        NSE_DOCS = {}
        jar = FI.B.nse_jar()
        for idx in ("equities", "sme"):
            try: rows = FI.get(jar, "corporates/offerdocs?index=" + idx, "https://www.nseindia.com/companies-listing/corporate-filings-offer-documents") or []
            except Exception as e:
                print("NSE offer docs %s FAILED: %s" % (idx, e), flush=True); rows = []
            for r in rows:
                # NSE Emerge posts many of its documents as zip files
                got = [(k, r.get(f)) for k, f in (("final", "fpAttach"), ("rhp", "rhpAttach"), ("draft", "drhpAttach")) if str(r.get(f) or "").lower().endswith((".pdf", ".zip"))]
                if got: NSE_DOCS.setdefault(norm(r.get("company") or ""), []).extend(got)
    return NSE_DOCS


def nse_hit(n):
    """Exact name, else a near-identical one (the NSE list has typos: 'Skytech Infinite Plaform'). Best document first."""
    docs = nse_docs()
    hit = docs.get(n)
    if not hit:
        near = difflib.get_close_matches(n, docs.keys(), n=1, cutoff=0.93)
        hit = docs.get(near[0]) if near else None
    return sorted(hit, key=lambda d: ("final", "rhp", "draft").index(d[0])) if hit else []


def find_doc(name):
    """Final prospectus first, then RHP; never an addendum, corrigendum or abridged note. Name must match in full."""
    n = norm(name)
    hit = nse_hit(n)
    if hit and hit[0][0] != "draft": return hit[0]
    cand = {"final": [], "rhp": []}
    for q in (name, " ".join(name.split()[:2])):
        for kind, u in sebi_rows(q):
            slug = u.rsplit("/", 1)[1].rsplit("_", 1)[0].replace("-", " ")
            if re.search(r"addendum|corrigend|abridged|notice|price band", slug): continue
            d = norm(slug)
            if (d == n or (len(d) > 5 and (d.startswith(n) or n.startswith(d)))) and u not in cand[kind]: cand[kind].append(u)
    for kind in ("final", "rhp"):
        if cand[kind]: return kind, cand[kind][0]
    # a draft carries the same restated statements, marked as a draft; a later run moves to the final once it is posted
    if hit: return hit[0]
    return None, None


def zip_parts(blob):
    """Every PDF in a zip, zips inside zips included, in name order; abridged notes, forms and addenda left out."""
    z, out = zipfile.ZipFile(io.BytesIO(blob)), []
    for n in sorted(z.namelist()):
        low = n.lower()
        if re.search(r"abridged|application|addendum|corrigend|__macosx", low): continue
        if low.endswith(".zip"): out += zip_parts(z.read(n))
        elif low.endswith(".pdf"): out.append(z.read(n))
    return out


def save_zip(blob, path):
    """A zip may split one document into parts: they are joined in name order."""
    parts = zip_parts(blob)
    if not parts: return False
    out = fitz.open()
    for p in parts:
        with fitz.open(stream=p, filetype="pdf") as part: out.insert_pdf(part)
    out.save(path)
    return True


def fetch_pdf(page_url, key):
    os.makedirs(CACHE, exist_ok=True)
    path = os.path.join(CACHE, key + ".pdf")
    if os.path.exists(path) and os.path.getsize(path) > 100000: return path, None
    if page_url.lower().endswith((".pdf", ".zip")):
        u = page_url.replace(" ", "%20")
    else:
        t = sess.get(page_url, timeout=60).text
        links = list(dict.fromkeys(re.findall(r"(https?://[^\"' ]+\.pdf|/sebi_data/[^\"' ]+\.pdf)", t)))
        if not links: return None, None
        links = [x if x.startswith("http") else "https://www.sebi.gov.in" + x for x in links]
        # a filing page can carry the abridged prospectus beside the full one: the full one is the largest
        def size(x):
            try: return int(sess.head(x, timeout=40).headers.get("content-length") or 0)
            except Exception: return 0
        u = max(links, key=size)
    for attempt in range(3):
        try:
            r = sess.get(u, timeout=420)
            if r.status_code == 200 and r.content.startswith(b"%PDF"):
                open(path, "wb").write(r.content)
                return path, u
            # the extension does not decide: some ".pdf" links on NSE serve a zip
            if r.status_code == 200 and r.content[:2] == b"PK" and save_zip(r.content, path):
                return path, u
            if r.status_code == 404: return None, u
        except (requests.RequestException, zipfile.BadZipFile, RuntimeError, ValueError):
            pass
        time.sleep(5 * (attempt + 1))
    return None, u


def read_prospectus(path):
    """-> {'con': {key: rec}, 'std': {...}}, held list, pages used. A page qualifies by its rows, then by proving its
    arithmetic in at least two dated columns; notes and MD&A tables never do. Full statements beat summary pages."""
    doc = fitz.open(path)
    res, held, used = {"con": {}, "std": {}}, [], []
    texts = [doc[i].get_text() for i in range(len(doc))]
    whole_con = sum(len(re.findall(r"restated\s+consolidated|consolidated\s+restated", t, re.I)) for t in texts)
    whole_std = sum(len(re.findall(r"restated\s+standalone|standalone\s+restated", t, re.I)) for t in texts)
    unit_doc = None
    pages = []
    for i, txt in enumerate(texts):
        kind = R.kind_of(txt)
        if not kind: continue
        unit = R.unit_of(txt) or unit_doc
        if not unit:
            for j in (i - 1, i - 2):
                if j >= 0: unit = unit or R.unit_of(texts[j][:800])
        if not unit: continue
        if not R.unit_of(txt) and whole_rupees(doc[i]): unit = 1e-7                # no unit on the page and nine-digit whole numbers
        unit_doc = unit
        ok, h, heads = R.read_statement(doc[i], kind, unit)
        if len(ok) < 2 and not (ok and kind == "bs"): continue
        head = re.sub(r"\s+", " ", txt[:900])
        prev = " ".join(re.sub(r"\s+", " ", texts[j][-500:]) for j in (i - 1,) if j >= 0)
        if re.search(r"standalone", head, re.I) and not re.search(r"consolidated", head, re.I): basis = "std"
        elif re.search(r"consolidated", head, re.I) and not re.search(r"standalone", head, re.I): basis = "con"
        elif re.search(r"consolidated", prev, re.I) and not re.search(r"standalone", prev, re.I): basis = "con"
        else: basis = "con" if whole_con > max(5, whole_std) else "std"
        if i + 1 < len(doc) and heads:
            ok2, _ = _continue(doc[i + 1], kind, unit, heads)
            for k, rec in ok2.items():
                if k in ok: ok[k].update({a: b for a, b in rec.items() if a not in ok[k]})
        rank = 0 if re.search(r"summary", head[:400], re.I) or i < len(doc) * 0.25 else 1      # full statements sit late in the document
        # a table can mix bases column by column (consolidated years beside a standalone first period)
        marks = {r.get("_b") for r in ok.values()} - {None}
        for b in (marks if len(marks) > 1 else [next(iter(marks))] if marks and re.search(r"consolidated", head + txt[:1500], re.I) and re.search(r"standalone", head + txt[:1500], re.I) else [basis]):
            part = {k: {f: x for f, x in r.items() if f != "_b"} for k, r in ok.items() if len(marks) < 2 or r.get("_b") == b}
            pages.append({"i": i, "kind": kind, "basis": b, "ok": part, "rank": rank})
        for key, rec in h.items():
            held.append({"p": i + 1, "kind": kind, "basis": basis, "period": key, "why": rec["why"]})
    for pg in same_entity_pages(pages, held):
        for key, rec in pg["ok"].items():
            slot = res[pg["basis"]].setdefault(key, {})
            if slot and slot.get("_rank", -1) > pg["rank"]: continue
            if any(f in slot for f in rec) and slot.get("_rank") == pg["rank"]: continue
            slot.update(rec); slot["_rank"] = pg["rank"]; slot["_p"] = pg["i"] + 1
        used.append(pg["i"] + 1)
    for b in res.values():
        for rec in b.values(): rec.pop("_rank", None)
    return res, held, sorted(set(used))


ANCHOR_FIELD = {"pl": "rev", "bs": "assets", "cf": "cfo"}
NEAR_PAGES = 15


def whole_rupees(page):
    vals = sorted(abs(R.to_num(w[4]) or 0) for w in R.page_lines(page)[1] if R.is_num(w[4]) and R.to_num(w[4]))
    return len(vals) > 5 and vals[len(vals) // 2] >= 1e6


def same_entity_pages(pages, held):
    """A prospectus also prints other companies' statements (group companies, subsidiaries, an acquired business), with
    arithmetic that balances. The first proven page of each basis and statement fixes the issuer's figures; a later page
    counts only if it agrees on a period they share, or, sharing none, sits beside an accepted P&L page."""
    anchors, kept, pl_at = {}, [], []
    for pg in sorted(pages, key=lambda p: (p["kind"] != "pl", p["i"])):
        f = ANCHOR_FIELD[pg["kind"]]
        vals = {k: r[f] for k, r in pg["ok"].items() if r.get(f) is not None}
        near_pl = any(abs(pg["i"] - p) <= NEAR_PAGES for p in pl_at)
        anc = anchors.get((pg["basis"], pg["kind"]))
        if pg["kind"] != "pl" and pl_at and not near_pl: why = "away from the issuer's P&L pages"
        elif anc is None: why = None
        else:
            shared = [k for k in vals if k in anc]
            if shared: why = None if all(abs(vals[k] - anc[k]) <= max(0.05, abs(anc[k]) * 0.03) for k in shared) else "differs from the first statement: another entity"
            else: why = None if near_pl else "no shared period, away from the issuer's P&L pages"
        if why:
            held.append({"p": pg["i"] + 1, "kind": pg["kind"], "basis": pg["basis"], "period": "*", "why": [why]})
            continue
        slot = anchors.setdefault((pg["basis"], pg["kind"]), {})
        for k, v in vals.items(): slot.setdefault(k, v)
        if pg["kind"] == "pl": pl_at.append(pg["i"])
        kept.append(pg)
    return kept


def _continue(page, kind, unit, heads):
    cols, rows = R.table(page)
    if len(cols) != len(heads) or not rows: return {}, {}
    if R.dates_in(" ".join(r["text"] for r in rows[:3])): return {}, {}
    spec = R.KINDS[kind][0]
    got = R.pick(rows, spec)
    out = {}
    for i, h in enumerate(heads):
        if not h or not h["d"]: continue
        key = "%d|%d" % (h["d"], h["m"] or 0)
        rec = {}
        for f, v in got.items():
            if i in v and v[i] is not None:
                rec[f.replace("_x", "")] = v[i] if f.startswith("eps") else round(v[i] * unit, 2)
        if rec: out[key] = rec
    return out, {}


def periods_for(pdf_cols, qe):
    """Standard SEBI results order. Column 0 = the quarter, 1 = preceding quarter, 2 = year-ago quarter; a Q4 page then
    carries FY and prior FY, others YTD, prior YTD, last FY."""
    y, md = qe // 10000, qe % 10000
    prev = {331: (y - 1) * 10000 + 1231, 630: y * 10000 + 331, 930: y * 10000 + 630, 1231: y * 10000 + 930}[md]
    out = {0: (qe, 3), 1: (prev, 3), 2: (qe - 10000, 3)}
    if md == 331 and pdf_cols >= 5: out.update({3: (qe, 12), 4: (qe - 10000, 12)})
    elif md == 630 and pdf_cols >= 4: out.update({3: (y * 10000 + 331, 12)})
    elif md == 930 and pdf_cols >= 6: out.update({3: (qe, 6), 4: (qe - 10000, 6), 5: (y * 10000 + 331, 12)})
    elif md == 1231 and pdf_cols >= 6: out.update({3: (qe, 9), 4: (qe - 10000, 9), 5: (y * 10000 + 331, 12)})
    return out


def result_columns(page, cols, rows, low, qe):
    """{column: (period end, months)}. Dates come from the column's own printed header; column order alone never dates a
    figure. Column 0 must be the quarter being read. SME filers print half years, which are not quarters."""
    heads = R.col_headers(page, cols, rows)
    dates = [h["d"] if h else None for h in heads]
    half = bool(re.search(r"half[- ]?year|six months|6 months", low)) and not re.search(r"quarter|three months|3 months", low)
    if not any(dates): return {} if half else {0: (qe, 3)}
    if dates[0] != qe: return {}
    expect, out = periods_for(len(cols), qe), {}
    for ci, d in enumerate(dates):
        if not d or d > qe or not R.period_fits(d, 3) or d in dates[:ci]: continue      # a date printed twice dates neither column safely
        if half:
            months = 12 if heads[ci].get("m") == 12 and "year" in heads[ci].get("txt", "").lower() else 6
            if months == 6 and d % 10000 not in (331, 930): continue
        elif ci < 3:
            if ci and d not in (expect[1][0], expect[2][0]): continue
            months = 3
        elif ci in expect and expect[ci][0] == d: months = expect[ci][1]
        else: continue
        out[ci] = (d, months)
    return out


def read_results(pdf_bytes, qe, anchor):
    """anchor = {'std': rev, 'con': rev} the stored XBRL revenue for qe. A page counts only if its column 0 equals it."""
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    res, held = {"con": {}, "std": {}}, []
    for i in range(min(len(doc), 40)):
        txt = doc[i].get_text()
        if not (re.search(r"revenue from operations|income from operations|interest earned", txt, re.I) and re.search(r"profit", txt, re.I)): continue
        low = re.sub(r"\s+", " ", txt[:900]).lower()
        if re.search(r"review report|auditor'?s report", low) and "particulars" not in txt.lower(): continue
        cols, rows = R.table(doc[i])
        if len(cols) < 3 or not rows: continue
        idx = {}
        got = R.pick(rows, R.PL, idx=idx)
        if "rev" not in got or 0 not in got["rev"]: continue
        best = None
        for basis_try, want in anchor.items():
            if want is None: continue
            for unit in (1.0, 0.1, 0.01, 0.0001, 1e-7):
                if R.near(got["rev"][0] * unit, want, max(0.02, abs(want) * 0.003)):
                    if basis_try in low[:900] or best is None: best = (basis_try, unit)
        if not best: continue
        basis, unit = best
        dated = result_columns(doc[i], cols, rows, low, qe)
        top_q = max((abs(got["rev"].get(c) or 0) * (1 if m == 3 else 0.75) for c, (_, m) in dated.items() if m in (3, 6)), default=0)
        for ci, (end, months) in dated.items():
            if months == 12 and (end % 10000 not in (331, 1231) or abs(got["rev"].get(ci) or 0) < 1.8 * top_q): continue       # half-year columns printed under a year's date
            rec = {f.replace("_x", ""): (v[ci] if f.startswith("eps") else round(v[ci] * unit, 2)) for f, v in got.items() if ci in v and v[ci] is not None}
            if not R.expense_lines_ok(rows, idx, ci, 1.5) or any(abs(rec.get(f) or 0) > abs(rec.get("exp") or rec.get("ti") or rec["rev"]) for f in R.DETAIL if "rev" in rec):
                for f in R.DETAIL: rec.pop(f, None)
            good, why = R.check_pl(rec, 1.5 * unit)
            # a results column must show its profit working: revenue and a profit alone prove nothing
            if good and (rec.get("pbt") is None or abs(rec["pbt"]) > 1.2 * max(abs(rec.get("ti") or 0), abs(rec["rev"]))): good, why = False, ["no provable profit before tax"]
            key = "%d|%d" % (end, months)
            if good and key not in res[basis]: res[basis][key] = rec
            elif not good: held.append({"p": i + 1, "basis": basis, "period": key, "why": why, "src_q": qe})
    return res, held


def drop_impossible_years(res):
    """A results column under a year's date whose revenue a shorter period inside that year nearly reaches is a half year."""
    for basis in res.values():
        for key in [k for k in basis if k.endswith("|12") and basis[k].get("rev") is not None and basis[k].get("_src") == "results"]:
            end = int(key.split("|")[0])
            for k, r in basis.items():
                e, m = (int(x) for x in k.split("|"))
                if 0 < m < 12 and end - 10000 < e <= end and r.get("rev") is not None and abs(r["rev"]) >= 0.75 * abs(basis[key]["rev"]):
                    basis.pop(key); break


def stored_revop(sym):
    d = load(os.path.join(DOCS, "fin", sym + ".json"), {})
    return d.get("revop") or {}


def result_filings(sym, jar, BF):
    """Stored quarters' result PDFs via the fundamentals backfill's own NSE route."""
    out = []
    for qs, row in sorted(stored_revop(sym).items()):
        qe = int(qs)
        try: urls = BF.announcement_pdfs(sym, qe, jar)
        except Exception: urls = []
        if urls: out.append((qe, row, urls))
    return out


def own_profit(rec):
    return rec.get("own") if rec.get("own") is not None else (rec.get("np") if rec.get("np") is not None else rec.get("pat"))


def to_fill_cells(sym, fin, fills, src, today):
    """3-month columns become fill-only quarters (runbook §25 ledger). Existing cells are never overwritten."""
    n = 0
    cells = fills.setdefault(sym, {})
    store = stored_revop(sym)
    for basis, b in fin.items():
        suf = "Con" if basis == "con" else "Std"
        for key, rec in b.items():
            end, m = key.split("|")
            if m != "3" or end in store and store[end][1 if basis == "con" else 0]: continue
            pat, rev = own_profit(rec), rec.get("rev")
            if pat is None or rev is None: continue
            c = cells.setdefault(end, {})
            if c.get("pat" + suf) is None:
                c["pat" + suf], c["rev" + suf] = pat, rev
                c.update(src=rec.get("_src", src), via="ipo_fin", on=today)
                n += 1
    if not cells: fills.pop(sym, None)
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default="")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--max-minutes", type=float, default=0)
    ap.add_argument("--no-results", action="store_true", help="prospectus only")
    ap.add_argument("--reread", action="store_true", help="read the documents again even when the same document was read before")
    a = ap.parse_args()
    t0 = time.time()
    today = datetime.date.today().isoformat()
    os.makedirs(OUT, exist_ok=True)
    state = load(STATE, {})
    fills = load(FILLS, {})
    listed = load(os.path.join(DOCS, "ipos.json"), {}).get("listed", [])
    only = {s for s in a.only.split(",") if s}
    todo = []
    # bond issues on the same list (10MWL29: issue price 1,00,000) are not shares and have no statement page of their own
    listed = [r for r in listed if not (r[0][0].isdigit() and (r[4] or 0) >= 1000)]
    for r in listed:
        sym = r[0]
        if only and sym not in only: continue
        st = state.get(sym, {})
        if not only:
            if st.get("status") == "done" and st.get("last", "") >= (datetime.date.today() - datetime.timedelta(days=RECHECK_DAYS)).isoformat(): continue
            if st.get("status") != "done" and st.get("tries", 0) >= MAX_TRIES: continue
        todo.append(r)
    print("recent listings %d, to work %d" % (len(listed), len(todo)), flush=True)
    if a.limit: todo = todo[:a.limit]
    jar = BF = None
    if not a.no_results:
        import backfill_ipo_bases as BF
        import fetch_ipos as FI
        jar = FI.B.nse_jar()
    done = 0
    for r in todo:
        if a.max_minutes and (time.time() - t0) / 60 > a.max_minutes:
            print("time cap reached", flush=True); break
        sym, name = r[0], r[1]
        st = state.setdefault(sym, {"tries": 0})
        st["tries"] = st.get("tries", 0) + 1; st["last"] = today
        fin, held, used, src = {"con": {}, "std": {}}, [], [], None
        try:
            kind, page = find_doc(name)
            prior = load(os.path.join(OUT, sym + ".json"), {})
            if page and not a.reread and (prior.get("src") or {}).get("page") == page:      # same document: keep its read, only results can be new
                src = prior["src"]
                fin = {b: {k: v for k, v in (prior.get(b) or {}).items() if v.get("_src") != "results"} for b in ("con", "std")}
            elif page:
                path, url = fetch_pdf(page, sym)
                for k2, u2 in nse_hit(norm(name)) if not path else []:      # a dead link: the next document of this company
                    if u2 != page:
                        path, url = fetch_pdf(u2, sym)
                        if path:
                            kind, page = k2, u2
                            break
                if path:
                    fin, held, used = read_prospectus(path)
                    src = {"kind": kind, "page": page, "url": url or page, "pages": used}
                    for b in fin.values():
                        for rec in b.values(): rec["_src"] = kind
            st["doc"] = page
            rsrc = []
            if BF is not None:
                for qe, row, urls in result_filings(sym, jar, BF):
                    anchor = {"std": row[0] or None, "con": row[1] or None}
                    for u, _sz in urls:
                        try: pdf = BF.fetch_pdf(u)
                        except Exception: pdf = None
                        if not pdf: continue
                        rr, h2 = read_results(pdf, qe, anchor)
                        held += h2
                        if rr["con"] or rr["std"]:
                            rsrc.append({"qe": qe, "url": u})
                            for b, per in rr.items():
                                for key, rec in per.items():
                                    rec["_src"] = "results"; rec["_q"] = qe
                                    prev = fin[b].get(key)
                                    # a later results column is a comparative; the earlier document's read stands
                                    if prev is None: fin[b][key] = rec
                            break
                    time.sleep(0.5)
        except Exception as e:
            st["err"] = str(e)[:200]
            print("%s: ERROR %s" % (sym, str(e)[:160]), flush=True)
            continue
        n_ann = sum(1 for b in fin.values() for k in b if k.endswith("|12"))
        if not any(fin.values()):
            st.update(status="pending", why="no document on NSE or SEBI" if not page else "no statement read", held=len(held))
            print("%s: pending (%s)" % (sym, st["why"]), flush=True); continue
        drop_impossible_years(fin)
        payload = {"sym": sym, "name": name, "listed": r[3], "src": src, "results": rsrc, "con": fin["con"], "std": fin["std"], "on": today}
        json.dump(payload, open(os.path.join(OUT, sym + ".json"), "w", encoding="utf-8"), separators=(",", ":"), ensure_ascii=False)
        nq = to_fill_cells(sym, fin, fills, (src or {}).get("url") or "results", today)
        st.update(status="done" if n_ann else "partial", periods=sum(len(b) for b in fin.values()), annual=n_ann, held=len(held), fills=nq, why=None)
        st["held_detail"] = held[:12]
        done += 1
        print("%s: con %d std %d periods, %d annual, %d new quarter cells, %d held" % (sym, len(fin["con"]), len(fin["std"]), n_ann, nq, len(held)), flush=True)
        json.dump(state, open(STATE, "w", encoding="utf-8"), indent=0, sort_keys=True)
        json.dump(fills, open(FILLS, "w", encoding="utf-8"), separators=(",", ":"))
    json.dump(state, open(STATE, "w", encoding="utf-8"), indent=0, sort_keys=True)
    json.dump(fills, open(FILLS, "w", encoding="utf-8"), separators=(",", ":"))
    print("DONE: %d stocks written this run" % done, flush=True)


if __name__ == "__main__":
    main()
