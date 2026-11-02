# -*- coding: utf-8 -*-
"""Read financial statements out of a company's own offer document or results PDF (text layer only).

Used by fetch_new_listing_financials.py. Rows and columns come from word positions: number words are
clustered by x into columns, each is attached to the nearest label line by y. Nothing is accepted on
position alone: every column read must pass the statement's own arithmetic (check_period), so a shifted
column or an OCR slip is held, never written. Screener is NOT read here; it is a test gate only.
"""
import calendar, re

MON = {m: i + 1 for i, m in enumerate("jan feb mar apr may jun jul aug sep oct nov dec".split())}
NUM = re.compile(r"^[(\[{]?-?\d[\d,]*(\.\d+)?[)\]}]?\*?$")
FULLMON = {"january", "february", "march", "april", "june", "july", "august", "september", "october", "november", "december", "sept"}
DASH = {"-", "–", "—", "--", "nil", "Nil", "NIL"}
UNIT = (("crore", 1.0), ("crores", 1.0), ("cr.", 1.0), ("million", 0.1), ("mn", 0.1),
        ("lakh", 0.01), ("lakhs", 0.01), ("lacs", 0.01), ("thousand", 0.0001), ("'000", 0.0001))

# field -> label test on the lower-cased row label; first match on a page wins unless noted
PL = [("rev", r"^(i\.?\s*|\(?[ia1]\)?\.?\s*)?(total\s+)?(revenue from operations?|income from operations?|net sales|sales$)|^interest earned"),
      ("oi", r"^(ii\.?\s*|\(?(ii|b|2)\)?\.?\s*)?other income"),
      ("ti", r"^(iii\.?\s*|\(?(iii|c|3)\)?\.?\s*)?total (income|revenue)"),
      ("fc", r"^(\(?[a-z0-9ivx]{0,3}\)?\.?\s*)?(finance (costs?|charges?)|interest expense|interest expended)"),
      ("dep", r"^(\(?[a-z0-9ivx]{0,3}\)?\.?\s*)?depreciation"),
      ("emp", r"^(\(?[a-z0-9ivx]{0,3}\)?\.?\s*)?employees? benefits?"),
      ("exp", r"^(\(?[a-z0-9ivx]{0,3}\)?\.?\s*)?total expenses?"),
      ("exc", r"^(\(?[a-z0-9ivx]{0,3}\)?\.?\s*)?exceptional"),
      ("pbt", r"^(\(?[a-z0-9ivx]{0,4}\)?\.?\s*)?(restated\s+)?(net\s+)?(profit|\(?loss\)?|profit\s*/\s*\(loss\))[^a-z]*(\(loss\)\s*)?(/\s*\(loss\)\s*)?(for the (year|period)\s+)?before\b(?!.*exceptional)(?!.*(depreciation|finance))"),
      ("pbt0", r"^(\(?[a-z0-9ivx]{0,4}\)?\.?\s*)?(restated\s+)?(net\s+)?(profit|\(?loss\)?)[^a-z]*(\(loss\)\s*)?(/\s*\(loss\)\s*)?(for the (year|period)\s+)?before\b"),
      ("tax", r"^(?!.*discontinu)(\(?[a-z0-9ivx]{0,4}\)?\.?\s*)?(total (income )?tax( expenses?)?|net current tax|tax expenses?\s*(\(?[a-z0-9 ]+\)?)?$)"),
      ("assoc", r"^share of (net )?(profit|loss|\(loss\))|^(profit|loss|profit\s*/\s*\(loss\))[^a-z]*(\(loss\))?\s*of associate"),
      ("ctax", r"^(\(?[a-z0-9ivx]{0,3}\)?\.?\s*)?current tax"),
      ("dtax", r"^(\(?[a-z0-9ivx]{0,3}\)?\.?\s*)?deferred tax"),
      ("pat", r"^(?!(?:(?!after tax).)*\bbefore\b)(\(?[a-z0-9ivx]{0,4}\)?\.?\s*)?(total\s+)?(restated\s+)?(net\s+)?(profit|\(?loss\)?)[^a-z]*(\(loss\)\s*)?(/\s*\(loss\)\s*)?(after tax|after adjustments|for the\b|from the (period|year)\b)"),
      ("np", r"^(?!.*\bbefore\b)(?!.*continu)(\(?[a-z0-9ivx]{0,4}\)?\.?\s*)?(total\s+)?(restated\s+)?(net\s+)?(profit|\(?loss\)?)[^a-z]*(\(loss\)\s*)?(/\s*\(loss\)\s*)?for the\b"),
      ("own", r"^(\(?[a-z0-9ivx]{0,3}\)?\.?\s*)?(owners?|equity (share)?holders?|shareholders) of"),
      ("eps_b", r"^(\(?[a-z0-9ivx]{0,3}\)?\.?\s*)?basic"),
      ("eps_d", r"^(\(?[a-z0-9ivx]{0,3}\)?\.?\s*)?diluted")]
BS = [("assets", r"^total assets"),
      ("sc", r"^(\(?[a-z0-9ivx]{0,3}\)?\.?\s*)?(equity )?share capital"),
      ("oeq", r"^(\(?[a-z0-9ivx]{0,3}\)?\.?\s*)?other equity|^(\(?[a-z0-9ivx]{0,3}\)?\.?\s*)?reserves (and|&) surplus"),
      ("nci", r"^(\(?[a-z0-9ivx]{0,3}\)?\.?\s*)?non[- ]controlling interests?"),
      ("eq", r"^total equity(?! and)"),
      ("tel", r"^total (equity and liabilities|liabilities and equity)"),
      ("ppe", r"^(\(?[a-z0-9ivx]{0,3}\)?\.?\s*)?property,? plant"),
      ("cwip", r"^(\(?[a-z0-9ivx]{0,3}\)?\.?\s*)?capital work"),
      ("gw", r"^(\(?[a-z0-9ivx]{0,3}\)?\.?\s*)?goodwill"),
      ("invnt", r"^(\(?[a-z0-9ivx]{0,3}\)?\.?\s*)?inventor"),
      ("rec", r"^(\(?[a-z0-9ivx]{0,3}\)?\.?\s*)?trade receivables"),
      ("cash", r"^(\(?[a-z0-9ivx]{0,3}\)?\.?\s*)?cash and (cash equivalents|bank)"),
      ("borr_x", r"^(\(?[a-z0-9ivx]{0,3}\)?\.?\s*)?(long[- ]term |short[- ]term )?borrowings")]   # summed: non-current + current
CF = [("cfo", r"net cash.*(operating)|cash (flow )?(generated )?(from|used in).*operating activities.*(net|\(a\))|net cash flow from operating"),
      ("cfi", r"net cash.*(investing)"),
      ("cff", r"net cash.*(financing)"),
      ("capex", r"^(\(?[a-z0-9ivx]{0,3}\)?\.?\s*)?(expenditure for|purchase of|payments? for|acquisition of|additions? to).{0,12}(property|fixed assets|tangible)"),
      ("netc", r"net (increase|decrease|\(decrease\)|\(increase\)|change).*cash")]


def to_num(tok):
    t = tok.strip().rstrip("*")
    if t in DASH: return 0.0
    neg = t[0] in "([{" or t.startswith("-")
    t = t.strip("()[]{}-").replace(",", "")
    try: v = float(t)
    except ValueError: return None
    return -v if neg else v


def is_num(tok):
    return tok in DASH or bool(NUM.match(tok))


def page_lines(page):
    """words -> label lines with their y, and the number words kept apart (OCR lifts numbers off their line)."""
    words = page.get_text("words")
    if not words: return [], []
    words.sort(key=lambda w: ((w[1] + w[3]) / 2, w[0]))
    # a header line ("March 31, 2026  March 31, 2025") carries numbers that are dates, not figures
    head_y = {}
    for w in words:
        y = round((w[1] + w[3]) / 2 / 3)
        low = w[4].lower().strip(".,'’")
        if low[:3] in MON and (len(low) == 3 or low in FULLMON): head_y.setdefault(y, set()).add("m%d" % w[0])
        if re.fullmatch(r"20\d{2}[,.]?", w[4]) : head_y.setdefault(y, set()).add("y%d" % w[0])
    head = {y for y, v in head_y.items() if len(v) >= 2}
    nums, labels = [], []
    for w in words:
        if is_num(w[4]) and round((w[1] + w[3]) / 2 / 3) not in head: nums.append(w)
        else: labels.append(w)
    lines = []
    for w in labels:
        y = (w[1] + w[3]) / 2
        if lines and abs(lines[-1][0] - y) <= 3.5:
            lines[-1][1].append(w)
        else:
            lines.append([y, [w]])
    out = []
    for y, ws in lines:
        ws.sort(key=lambda a: a[0])
        out.append({"y": y, "x0": ws[0][0], "x1": ws[-1][2], "text": " ".join(a[4] for a in ws)})
    return out, nums


def columns(nums, min_share=0.25):
    """Cluster number words by right edge; keep clusters holding a real share of the page's rows.
    Note-number columns hold short integers only, so drop a leftmost cluster with no comma/decimal and small values."""
    if not nums: return []
    xs = sorted(nums, key=lambda w: w[2])
    cl = []
    for w in xs:
        if cl and w[2] - cl[-1][-1][2] <= 9: cl[-1].append(w)
        else: cl.append([w])
    top = max(len(c) for c in cl)
    keep = [c for c in cl if len(c) >= max(3, top * min_share)]
    def noteish(c):
        vals = [to_num(w[4]) for w in c if w[4] not in DASH]
        return bool(vals) and all(v is not None and 0 <= v < 100 and "," not in w[4] for v, w in zip(vals, [w for w in c if w[4] not in DASH]))
    while len(keep) > 1 and noteish(keep[0]): keep.pop(0)
    return [sum(w[2] for w in c) / len(c) for c in keep]


def table(page):
    """-> (cols, rows) with rows = [{'label', 'y', 'vals': {col_idx: value}}]. Wrapped labels stay on their own lines."""
    lines, nums = page_lines(page)
    cols = columns(nums)
    if len(cols) < 1: return [], []
    first = min(cols)
    rows = [dict(l, vals={}) for l in lines if l["x0"] < first - 15]
    if not rows: return cols, []
    for w in nums:
        i = min(range(len(cols)), key=lambda k: abs(cols[k] - w[2]))
        if abs(cols[i] - w[2]) > 14: continue
        y = (w[1] + w[3]) / 2
        r = min(rows, key=lambda r: abs(r["y"] - y))
        dy = abs(r["y"] - y)
        if dy > 11: continue
        # two numbers landing on one row: the closer one is that row's, the other belongs to an unlabelled line
        if i not in r["vals"] or dy < r.setdefault("dy", {}).get(i, 99):
            r["vals"][i] = to_num(w[4])
            r.setdefault("dy", {})[i] = dy
    return cols, rows


_D = [re.compile(r"(\d{1,2})\s*(?:st|nd|rd|th)?[\s.,/-]*([A-Za-z]{3,9})[\s.,'’/-]*(\d{4}|\d{2})\b", re.I),
      re.compile(r"([A-Za-z]{3,9})\s+(\d{1,2})(?:st|nd|rd|th)?[\s,]+(\d{4})", re.I),
      re.compile(r"(\d{1,2})[./-](\d{1,2})[./-](\d{4}|\d{2})\b")]


def _ymd(d, mon, y):
    y = int(y); y = y + 2000 if y < 100 else y
    return y * 10000 + mon * 100 + int(d)


def dates_in(text):
    out = []
    for m in _D[0].finditer(text):
        mo = MON.get(m.group(2)[:3].lower())
        if mo and 1 <= int(m.group(1)) <= 31: out.append((m.start(), _ymd(m.group(1), mo, m.group(3))))
    for m in _D[1].finditer(text):
        mo = MON.get(m.group(1)[:3].lower())
        if mo and 1 <= int(m.group(2)) <= 31: out.append((m.start(), _ymd(m.group(2), mo, m.group(3))))
    if not out:
        for m in _D[2].finditer(text):
            if 1 <= int(m.group(2)) <= 12: out.append((m.start(), _ymd(m.group(1), int(m.group(2)), m.group(3))))
    return out


def col_headers(page, cols, rows):
    """Each column's period end + months, from the words above the first data row, by x."""
    if not cols or not rows: return [None] * len(cols)
    ytop = next((r["y"] for r in rows if len(r["vals"]) >= max(1, len(cols) - 1)), None)
    if ytop is None: return [None] * len(cols)
    words = sorted((w for w in page.get_text("words") if w[1] < ytop - 2), key=lambda w: w[0])
    gap = (cols[1] - cols[0]) if len(cols) > 1 else 90
    near_col = {}
    for w in words:                                      # every header word belongs to ONE column: nearest by centre
        cx = (w[0] + w[2]) / 2
        i = min(range(len(cols)), key=lambda k: abs(cols[k] - gap * 0.3 - cx))
        if abs(cols[i] - gap * 0.3 - cx) <= gap * 0.75: near_col.setdefault(i, []).append(w)
    head = []
    for i, c in enumerate(cols):
        ws = sorted(near_col.get(i, []), key=lambda w: (round(w[1] / 4), w[0]))
        txt = " ".join(w[4] for w in ws)
        ds = dates_in(txt)
        d = ds[-1][1] if ds else None
        t = txt.lower()
        if d is None:
            fy = re.search(r"(?:fiscal|financial year|fy)\s*(?:year\s*)?(?:ended\s*)?(?:20)?\d{2}\s*[-–/]\s*(?:20)?(\d{2})\b", t) \
                or re.search(r"(?:fiscal|financial year|fy)\s*(?:year\s*)?(?:ended\s*)?(?:20)?(\d{2})\b", t)
            if fy: d = (2000 + int(fy.group(1))) * 10000 + 331
        m = None
        stub = None
        if len(ds) >= 2 and re.search(r"\bfrom\b", t):
            a, b = ds[-2][1], ds[-1][1]
            span = (b // 10000 - a // 10000) * 12 + (b // 100 % 100 - a // 100 % 100) + 1
            stub = span if a % 100 == 1 else 0                 # a period that opens mid-month is not a full quarter or year
        if re.search(r"(three|3)\s*months?|quarter", t): m = 3
        elif re.search(r"(six|6)\s*months?|half", t): m = 6
        elif re.search(r"(nine|9)\s*months?", t): m = 9
        elif re.search(r"year|fiscal|fy|twelve|12\s*months", t): m = 12
        if stub is not None: m = stub if stub in (3, 6, 9, 12) else -1
        head.append({"d": d, "m": m, "txt": txt[:120], "basis": "con" if "consolidated" in t and "standalone" not in t else "std" if "standalone" in t and "consolidated" not in t else None})
    # "For the year ended 31st March" spanning columns with only "2025  2024" under it: the year takes its sibling's day
    known = [h["d"] % 10000 for h in head if h["d"] and h["m"] == 12]
    for h in head:
        y = re.findall(r"(?<![\d,.])(20[12]\d)(?![\d,.])", h["txt"])
        if h["d"] is None and y: h["d"] = int(y[-1]) * 10000 + (known[0] if known else 331)
    # a heading like "Year ended" often spans several columns and lands on one: inherit it by the date's month
    if any(h["m"] == 12 for h in head):
        fye = next(h["d"] % 10000 for h in head if h["m"] == 12 and h["d"])  if any(h["m"] == 12 and h["d"] for h in head) else 331
    else:
        fye = 331
    for h in head:
        if h["m"] is None and h["d"]:
            mo = ((h["d"] // 100 % 100) - (fye // 100)) % 12
            h["m"] = {0: 12, 3: 3, 6: 6, 9: 9}.get(mo)
    return head


def unit_of(text):
    t = text.lower()
    m = re.search(r"(?:₹|rs\.?|rupees|inr|`|amounts?|figures?)[^\n]{0,40}?\b(crores?|cr\.|millions?|mn|lakhs?|lacs|thousands?)\b", t)
    w = m.group(1) if m else None
    if w is None:
        m = re.search(r"\b(?:in)\s+(crores?|millions?|lakhs?|lacs|thousands?)\b", t)
        w = m.group(1) if m else None
    if w is None: return None
    for k, f in UNIT:
        if w.startswith(k[:4]): return f
    return None


def pick(rows, spec, sum_prefix="_x", idx=None):
    """field -> {col: value}. First labelled row with numbers wins; a wrapped label borrows the next line's numbers."""
    res = {}
    for i, r in enumerate(rows):
        lab = r["text"].lower().strip()
        lab = re.sub(r"^[\W_]+", "", lab)
        lab = re.sub(r"^((\(?[ivxl]{1,4}\)|[ivxl]{1,4}\.|[ivxl]{2,4}\b|[ivxl]{1,4}\s*-|\(?[a-z0-9]\)|[a-z0-9]\.|i\b)\s*)+", "", lab) or lab
        for f, rx in spec:
            if not re.search(rx, lab): continue
            if f == "pat" and "pat" in res: continue
            v = r["vals"]
            nxt = rows[i + 1]["text"].strip() if i + 1 < len(rows) else ""
            if not v and nxt and (not re.search(r"[a-z]{3}", nxt.lower()) or nxt[0].islower()):      # the label wrapped onto this line
                v = rows[i + 1]["vals"]
            if not v: continue
            if f.endswith(sum_prefix):
                d = res.setdefault(f, {})
                for k, x in v.items(): d[k] = d.get(k, 0.0) + (x or 0.0)
            elif f not in res or f == "pbt" or (f == "rev" and lab.startswith("total")):
                res[f] = dict(v)
                if idx is not None: idx[f] = i
            if f in ("pbt", "pbt0"): continue
            break
    return res


def expense_lines_ok(rows, idx, col, tol):
    """Finance cost, depreciation and staff cost sit outside the profit arithmetic, so their own column is proved here:
    the printed expense lines must add up to total expenses. A scan that lost a decimal point fails this."""
    start = idx.get("ti", idx.get("oi", idx.get("rev")))
    if start is None or "exp" not in idx or idx["exp"] <= start: return True
    total = rows[idx["exp"]]["vals"].get(col)
    if total is None: return True
    add = sum(r["vals"].get(col) or 0.0 for r in rows[start + 1:idx["exp"]] if not re.match(r"(\W*\w{0,4}\W+)?(total|sub-?total)\b", r["text"].lower()))
    return near(add, total, max(tol * 2, abs(total) * 0.002))


DETAIL = ("fc", "dep", "emp")


def near(a, b, unit_tol):
    return abs(a - b) <= max(unit_tol, abs(b) * 0.002)


def check_pl(p, tol):
    """Arithmetic of one P&L column. Returns (ok, why). rev is proven by rev+oi=ti; pbt-tax=pat when both present."""
    why = []
    if p.get("rev") is None: return False, ["no revenue row"]
    # a filing may print further lines (grants, gains) between revenue and total income: those are added up too
    if p.get("ti") is not None and p.get("oi") is not None and not near(p["rev"] + p["oi"], p["ti"], tol) and not near(p.get("_isum", 0), p["ti"], tol): why.append("rev+oi!=ti")
    tax = p.get("tax")
    if tax is None and p.get("tax0") is not None: tax = p["tax0"]
    if tax is None and (p.get("ctax") is not None or p.get("dtax") is not None): tax = (p.get("ctax") or 0) + (p.get("dtax") or 0)
    if p.get("pat") is None: why.append("no pat row")
    elif p.get("pbt") is not None and tax is not None:
        gap = p["pbt"] - tax - p["pat"]
        if p.get("assoc") and abs(gap + p["assoc"]) < abs(gap): gap += p["assoc"]     # share of associates printed below tax
        ok_tax = near(gap, 0, max(tol * 2, abs(p["pat"]) * 0.02))
        # no printed total, or a tax line beyond current and deferred (earlier years): add up the printed lines
        # (some filings print tax lines as negatives)
        if not ok_tax and "_tsum" in p: ok_tax = near(abs(p["pbt"] - p["pat"]), abs(p["_tsum"]), max(tol * 2, abs(p["pat"]) * 0.002))
        if not ok_tax: why.append("pbt-tax!=pat")
    if p.get("ti") is not None and p.get("exp") is not None and p.get("pbt") is not None:
        gap = max(tol * 2, abs(p["pbt"]) * 0.03)
        base = p["ti"] - p["exp"]
        if not any(near(base + d, t, gap) for t in (p["pbt"], p.get("pbt0", p["pbt"])) for d in (0, p.get("exc") or 0, -(p.get("exc") or 0))):
            # lines a filing prints between total expenses and profit before tax (prior period items, grants)
            if "_xsum" not in p or not near(abs(base - p["pbt"]), abs(p["_xsum"]), max(tol * 2, abs(p["pbt"]) * 0.002)):
                why.append("ti-exp!=pbt")
    return not why, why


def check_bs(p, tol):
    if p.get("assets") is None: return False, ["no total assets"]
    if p.get("tel") is not None and not near(p["assets"], p["tel"], tol): return False, ["assets!=eq+liab"]
    return True, []


def check_cf(p, tol):
    if p.get("cfo") is None: return False, ["no cfo"]
    if None not in (p.get("cfi"), p.get("cff"), p.get("netc")) and not near(p["cfo"] + p["cfi"] + p["cff"], p["netc"], tol * 3):
        return False, ["cfo+cfi+cff!=net"]
    return True, []


KINDS = {"pl": (PL, check_pl), "bs": (BS, check_bs), "cf": (CF, check_cf)}
HEAD = {"pl": r"profit\s*(and|&|/)?\s*(\(?loss\)?)|statement of (income|operations)|financial results",
        "bs": r"assets\s*(and|&)\s*liabilities|balance sheet",
        "cf": r"cash\s*flows?"}


def kind_of(txt):
    """Which statement a page holds, from its rows rather than its heading (many filings print no heading on the page)."""
    t = txt.lower()
    if re.search(r"total\s+assets", t) and re.search(r"share\s+capital", t) and re.search(r"total\s+(equity|liabilities)|borrowings", t): return "bs"
    if re.search(r"(revenue|income) from operation|interest earned", t) and re.search(r"before\s+\w+", t) and re.search(r"(current tax|tax expense)", t) and re.search(r"after tax|for the (period|year)|profit for|\bexpen", t) and not re.search(r"investing\s+activities", t): return "pl"
    if re.search(r"investing\s+activities", t) and re.search(r"financing\s+activities", t) and re.search(r"operating\s+activities", t): return "cf"
    return None


def period_fits(d, months):
    """A column is kept only if it ends on a month end, and an annual column on March or December;
    a stub ('1 April to 20 December') or a misread header never becomes a quarter or a year."""
    y, m, day = d // 10000, d // 100 % 100, d % 100
    if day != calendar.monthrange(y, m)[1]: return False
    if months == 12 and m not in (3, 12): return False
    if months in (3, 6, 9) and m == 3 and months != 3: return False
    if months < 0: return False
    return True


def read_statement(page, kind, unit):
    """One page -> {period_key: fields}, {period_key: why-held}. period_key = 'YYYYMMDD|months'."""
    spec, chk = KINDS[kind]
    cols, rows = table(page)
    if not rows or not unit: return {}, {}, None
    heads = col_headers(page, cols, rows)
    years = [h["d"] for h in heads if h and h["d"] and (kind == "bs" or h["m"] == 12)]
    # an analysis table (percent columns, misaligned years) is no statement: its years repeat or skip
    if len(set(years)) < len(years) or any(a // 10000 - b // 10000 not in (1, -1) for a, b in zip(years, years[1:])) and not any(h and not h["d"] for h in heads[1:]) and kind != "bs": return {}, {}, None
    idx = {}
    got = pick(rows, spec, idx=idx)
    tol = 1.5 * unit if unit else 0
    def between(a, b):
        """Per column, the sum of the printed lines strictly between rows a and b; printed sub-totals are left out."""
        out = {}
        for r in rows[idx[a] + 1:idx[b]]:
            if re.match(r"(\W*\w{0,4}\W+)?(total|sub-?total|net current tax)\b", r["text"].lower()): continue
            for k, x in r["vals"].items(): out[k] = out.get(k, 0.0) + (x or 0.0)
        return out
    isum = between("rev", "ti") if kind == "pl" and "rev" in idx and "ti" in idx and idx["rev"] < idx["ti"] else {}
    xsum = between("exp", "pbt") if kind == "pl" and "exp" in idx and "pbt" in idx and idx["exp"] < idx["pbt"] else {}
    tsum = between("pbt", "pat") if kind == "pl" and "pbt" in idx and "pat" in idx and idx["pbt"] < idx["pat"] else {}
    disc = {k for r in rows if "discontinu" in r["text"].lower() for k, x in r["vals"].items() if x}
    ok, held = {}, {}
    for i, h in enumerate(heads):
        if not h or not h["d"]: continue
        rec = {}
        for f, v in got.items():
            if i in v and v[i] is not None:
                val = v[i]
                rec[f.replace("_x", "")] = val if f.startswith("eps") else round(val * unit, 2)
        if not rec: continue
        if kind == "pl" and not expense_lines_ok(rows, idx, i, 1.5):
            for f in DETAIL: rec.pop(f, None)
        # a "for the year" row that is not the year's profit (a comprehensive-income line): the proven pat stands
        if rec.get("np") is not None and rec.get("pat") is not None and i not in disc and not near(rec["np"], rec["pat"], max(tol, abs(rec["pat"]) * 0.02)): rec.pop("np")
        if rec.get("rev") == 0 and abs(rec.get("pat") or 0) < 1 or rec.get("assets") == 0: continue    # a shell year, or a nil column
        if h.get("basis"): rec["_b"] = h["basis"]
        months = 0 if kind == "bs" else h["m"]
        if months is None: continue
        if not period_fits(h["d"], months): continue
        key = "%d|%d" % (h["d"], months)
        probe = dict(rec)
        if i in isum and "rev" in rec: probe["_isum"] = rec["rev"] + round(isum[i] * unit, 2)
        if i in tsum: probe["_tsum"] = round(tsum[i] * unit, 2)
        if i in xsum: probe["_xsum"] = round(xsum[i] * unit, 2)
        good, why = chk(probe, tol)
        (ok if good else held)[key] = rec if good else {"why": why, "rec": rec}
    return ok, held, heads
