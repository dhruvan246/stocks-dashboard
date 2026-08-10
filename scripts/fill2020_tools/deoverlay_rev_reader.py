# -*- coding: utf-8 -*-
"""FILL-2019: read result PDFs whose text layer is RENDERED TWO OR THREE TIMES (overlay class).

THE FAILURE MODE, measured on ABCAPITAL's Mar-2019 filing (2026-08-10). The page extracts as

    Total Total Total Revenue Revenue Revenue from from from operations operations operations
    [3,645.75, 3,845.75, 3,645.75, 4,729.82, 4,729.82, 4,729.82, ...]

Three complete copies of the statement are stacked at ESSENTIALLY IDENTICAL coordinates (all three
"Total" tokens start at x0=75.87; the y centres differ by <0.2pt). Consequences:
  * every label regex fails — "Revenue from operations" never appears contiguously, so
    backfill_revop_gaps records `no-anchor-or-scanned` although the statement is right there;
  * PL_PAGE still matches (it finds the words), so the page is not even flagged as unreadable.
This is a DIFFERENT corruption from §51b's glyph substitution ("Slondolone") and needs a different
fix: de-duplicate by position, not by spelling.

THE FIX, and why it is SAFER than a normal read rather than riskier. The overlaid copies come from
independent render passes and they DISAGREE on digits — 3,645.75 vs 3,845.75 vs 3,645.75, and
15,163.51 vs 15,183.51 vs 15,163.51 — i.e. the duplication is a built-in 3-way vote. This reader
collapses each position-bucket to its MAJORITY token and DROPS the bucket when no strict majority
exists, so an OCR-noised cell can never be written; ABCAPITAL's own page then satisfies the
printed internal identity (3,645.75 + 7.12 other income == 3,652.87 total income, exactly).

Guards, all inherited unchanged from the sweep this reader stands in for:
  * de-overlay is applied ONLY to pages measured to be overlaid (>=35% of position buckets carry
    >=2 tokens); a normal page is read exactly as before;
  * basis from the page's own declared header, rows from backfill_revop_gaps.ROW_PATS;
  * COLUMN ANCHOR (§58): a column must reproduce a STORED PAT for this company/quarter/basis
    (or a neighbouring quarter) — unanchored pages are refused, never guessed;
  * a second, independent check on the same page: revenue + other income == printed total income
    (0.5%) where the page prints one; recorded per cell;
  * fill-only; con reads additionally refused when they fall below half the stored standalone
    revenue (the 3MINDIA class).

Ledger: scripts/deoverlay_rev_fills2019.json (tracked) — value, anchor chain, identity check,
attachment id and page. Skips: scripts/fill2020_tools/_deoverlay_skips.json.

Run: python -X utf8 scripts/fill2020_tools/deoverlay_rev_reader.py [--only SYM,SYM] [--limit N] [--apply]
"""
import collections
import datetime
import json
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPTS = os.path.dirname(HERE)
ROOT = os.path.dirname(SCRIPTS)
sys.path.insert(0, SCRIPTS)

import fitz                                     # noqa: E402
import backfill_revop_gaps as BG                # noqa: E402
import fetch_insurers as FI                     # noqa: E402

TARGETS = os.path.join(HERE, "_rev2020_targets.json")
FILLS = os.path.join(SCRIPTS, "deoverlay_rev_fills2019.json")
SKIPS = os.path.join(HERE, "_deoverlay_skips.json")
REVOP_DOCS = os.path.join(ROOT, "docs", "sf_revop.json")
REVOP_LEDGER = os.path.join(SCRIPTS, "revop_fundamentals.json")
FUND = os.path.join(ROOT, "docs", "sf_fundamentals.json")
SCRIPS = os.path.join(SCRIPTS, "bse_scrips.json")
MASTER = os.path.join(SCRIPTS, "_bse_master_all.json")

SLOT = {"std": {"rev": 0, "op": 2, "ebit": 7}, "con": {"rev": 1, "op": 3, "ebit": 8}}


def scrip_map():
    m = {k.upper(): v for k, v in json.load(open(SCRIPS, encoding="utf-8"))["by_id"].items()}
    try:                                   # delisted names are absent from the live master (§52b)
        for r in json.load(open(MASTER)):
            sid = (r.get("scrip_id") or "").upper()
            if sid and sid not in m and (r.get("Segment") or "Equity") == "Equity":
                m[sid] = r["SCRIP_CD"]
    except Exception:
        pass
    return m


def buckets(words):
    """Position buckets: (y-band, x-band) -> [tokens]. 3pt x / 3pt y — 2pt left ABCAPITAL's
    figure columns split across two buckets (x0 75.87/76.4 style jitter) so a row still came back
    with the same column twice; 3pt collapses them without merging genuinely adjacent words,
    which in these statements are never closer than ~8pt."""
    b = collections.defaultdict(list)
    for w in words:
        key = (round(((w[1] + w[3]) / 2) / 3.0), round(w[0] / 3.0))
        b[key].append(w)
    return b


def is_overlaid(words):
    b = buckets(words)
    if not b:
        return False
    multi = sum(1 for v in b.values() if len(v) >= 2)
    return multi >= 0.35 * len(b)


def deoverlay_words(words):
    """One token per position bucket, by MAJORITY vote. No strict majority -> the bucket is
    dropped, so a disputed figure can never reach the anchor or the store."""
    out = []
    for key, ws in buckets(words).items():
        if len(ws) == 1:
            out.append(ws[0])
            continue
        cnt = collections.Counter(w[4] for w in ws)
        top, n = cnt.most_common(1)[0]
        if n * 2 <= len(ws):                     # no strict majority -> refuse this token
            continue
        pick = next(w for w in ws if w[4] == top)
        out.append(pick)
    out.sort(key=lambda w: (round(w[3] / 4), w[0]))
    return out


def page_lines_deoverlay(page):
    """BG.page_lines, but on de-overlaid words when the page is measured to be overlaid."""
    words = page.get_text("words")
    if not words or not is_overlaid(words):
        return BG.page_lines(page), False
    clean = deoverlay_words(words)

    class _Shim:                                  # BG.page_lines only calls get_text("words")
        def get_text(self, kind=None):
            return clean
    return BG.page_lines(_Shim()), True


def qe_date(qe):
    return datetime.date(qe // 10000, (qe // 100) % 100, qe % 100)


# ---------------------------------------------------------------------------------------------
# §55b — SELECT A COLUMN BY ITS PRINTED DATE, NOT BY ITS POSITION.
# ABCAPITAL's Mar-2019 consolidated revenue lives in the JUNE-2019 filing as a comparative, and
# the sweep's anchor_columns refuses it: only ONE stored PAT matches (a single anchor), and the
# in-column PBT-tax identity is unusable because the tax line is split into current/deferred/
# short-provision rows. The filing states the period of every column outright —
#   Particulars | Quarter Ended 30th June 2019 | 31st March 2019 (Refer Note 6) |
#               | 30th June 2018 | Year Ended 31st March 2019 (Audited)
# so the column can be identified WITHOUT our own stored values, which also removes the
# circularity risk in anchoring on a stored PAT that may itself have come from this same column.
# ---------------------------------------------------------------------------------------------
MONTHS = {m: i for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"], 1)}
RE_DAY = BG.re.compile(r"^(\d{1,2})(?:st|nd|rd|th)?,?$", BG.re.I)
RE_YEAR = BG.re.compile(r"^((?:19|20)\d{2})[.,]?$")
RE_NUMDATE = BG.re.compile(r"^(\d{1,2})[./-](\d{1,2})[./-]((?:19|20)\d{2})[.,]?$")
LAST_DAY = {3: 31, 6: 30, 9: 30, 12: 31}


def _month(tok):
    t = BG.re.sub(r"[^a-z]", "", tok.lower())[:3]
    return MONTHS.get(t)


def header_dates(words):
    """[(x_right, qe)] for every printed period date, left to right.

    Only QUARTER-END dates are kept (Mar/Jun/Sep/Dec month-ends): a results header prints nothing
    else, while page furniture ('Renewed from 01-Apr-2018') would otherwise be mistaken for a
    column. Duplicate tokens from an overlaid render are collapsed by position."""
    ws = sorted(words, key=lambda w: (round(w[1] / 3.0), w[0]))
    out, i = [], 0
    while i < len(ws):
        tok = ws[i][4].strip()
        m = RE_NUMDATE.match(tok)
        if m:
            d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
            if mo in LAST_DAY and d == LAST_DAY[mo]:
                out.append((ws[i][2], y * 10000 + mo * 100 + d, (ws[i][1] + ws[i][3]) / 2))
            i += 1
            continue
        md = RE_DAY.match(tok)
        if md:
            day = int(md.group(1))
            j, mon = i + 1, None
            while j < len(ws) and j <= i + 4:
                mm = _month(ws[j][4])
                if mm:
                    mon = mm
                    break
                j += 1
            if mon:
                k, yr = j + 1, None
                while k < len(ws) and k <= j + 4:
                    my = RE_YEAR.match(ws[k][4].strip())
                    if my:
                        yr = int(my.group(1))
                        break
                    k += 1
                if yr and mon in LAST_DAY and day == LAST_DAY[mon]:
                    # the year token repeats on an overlaid render — take the RIGHTMOST copy
                    xr = ws[k][2]
                    kk = k + 1
                    while kk < len(ws) and RE_YEAR.match(ws[kk][4].strip()) and \
                            abs(ws[kk][0] - ws[k][0]) < 6:
                        xr = max(xr, ws[kk][2])
                        kk += 1
                    out.append((xr, yr * 10000 + mon * 100 + day, (ws[k][1] + ws[k][3]) / 2))
                    i = kk
                    continue
        i += 1
    # RESTRICT TO THE HEADER BAND. The document TITLE contains a date too — ABCAPITAL's audited
    # filing is headed "…For The Quarter And Year Ended 31st March, 2019" — and that stray date
    # sits near a figure column, so it presents itself as a second Mar-2019 column and defeats the
    # quarter-vs-cumulative guard. Keep only the y-band carrying the MOST dates (>=2); a lone date
    # on a line is prose (§64's `date_columns.py` note, made concrete here).
    if not out:
        return []
    bands = collections.defaultdict(list)
    for x, qe, y in out:
        bands[round(y / 4.0)].append((x, qe))
    best = max(bands.values(), key=len)
    if len(best) < 2:
        return []
    # collapse near-identical x for the same date (overlay jitter)
    dedup = []
    for x, qe in sorted(best):
        if dedup and dedup[-1][1] == qe and abs(dedup[-1][0] - x) < 8:
            dedup[-1] = (max(dedup[-1][0], x), qe)
        else:
            dedup.append((x, qe))
    return dedup


def pick_rev_row(merged):
    """The revenue row, preferring a line that IS the revenue label over one that merely carries it.

    merge_wrapped prepends a numeric-less line to the next one, so the section header
    '1 Revenue from operations' gets glued onto the FIRST COMPONENT ('Interest Income 1,835.71'),
    and a naive first-match takes interest income as revenue. Prefer, in order: a label whose own
    text starts with 'total revenue/income from operations', then one starting with 'revenue from
    operations', then any match — and only rows that actually carry figures."""
    best = {}
    for _, t, nums in merged:
        if not nums or not BG.ROW_PATS["rev"].search(t):
            continue
        low = BG.re.sub(r"^[0-9ivxIVX\.\)\(\s]+", "", t.strip()).lower()
        rank = (0 if low.startswith(("total revenue from operation", "total income from operation"))
                else 1 if low.startswith(("revenue from operation", "income from operation"))
                else 2)
        if rank not in best:
            best[rank] = nums
    for r in (0, 1, 2):
        if r in best:
            return best[r], r
    return None, None


def date_column(rows, hdates, target_qe, guard_row=None):
    """x of the figure column whose PRINTED date is target_qe, or (None, reason).

    Rule + its guard: quarter columns precede cumulative ones in these layouts, so the LEFTMOST
    occurrence of the date is the quarter. That is only accepted when any later column carrying
    the SAME date holds a LARGER value on `guard_row` — a quarter cannot exceed the year that
    contains it — which is what keeps the §55c trap (a YTD column presenting itself as the
    quarter) out. The guard row is the PAT/owners row: it appears exactly once per statement,
    whereas the revenue label can be a section header glued onto its first component.
    """
    if not hdates:
        return None, "no printed dates in the header"
    cols = sorted({x for x, _ in rows.get("rev", [])})
    if not cols:
        return None, "no revenue columns"
    guard = guard_row if guard_row else rows.get("rev", [])

    def nearest(xd):
        best = min(cols, key=lambda c: abs(c - xd))
        return best if abs(best - xd) <= 22 else None

    hits = []
    for xd, qe in hdates:
        if qe != target_qe:
            continue
        c = nearest(xd)
        if c is not None and c not in hits:
            hits.append(c)
    if not hits:
        return None, "header prints no column for %d" % target_qe
    hits.sort()
    x = hits[0]
    if len(hits) > 1:
        vq = BG.val_at(guard, x)
        for other in hits[1:]:
            vo = BG.val_at(guard, other)
            if vq is None or vo is None or not (abs(vo) > abs(vq)):
                return None, ("two columns print %d and the later one is not larger on the guard "
                              "row (%s vs %s) — cannot tell quarter from cumulative"
                              % (target_qe, vq, vo))
    return x, "printed-date column (leftmost of %d occurrence(s))" % len(hits)


def main():
    argv = sys.argv
    only = set(argv[argv.index("--only") + 1].split(",")) if "--only" in argv else None
    limit = int(argv[argv.index("--limit") + 1]) if "--limit" in argv else None
    apply_it = "--apply" in argv

    targets = json.load(open(TARGETS))
    revop = json.load(open(REVOP_DOCS))
    ledger = json.load(open(REVOP_LEDGER))
    fund = json.load(open(FUND))
    fmap = {s: {r[0]: r for r in rows} for s, rows in fund.items()}
    fills = json.load(open(FILLS)) if os.path.exists(FILLS) else {}
    skips = json.load(open(SKIPS)) if os.path.exists(SKIPS) else {}
    n2s = scrip_map()

    work = []
    for sym, t in sorted(targets.items()):
        if only and sym not in only:
            continue
        for basis, qes in (("std", t["revS"]), ("con", t["revC"])):
            for qe in qes:
                key = "%s|%d|%s" % (sym, qe, basis)
                if key in fills:
                    continue
                row = (revop.get(sym) or {}).get(str(qe))
                if row is not None and row[SLOT[basis]["rev"]] is not None:
                    continue
                if (fmap.get(sym, {}).get(qe) or [None] * 4)[1 if basis == "std" else 3] is None:
                    skips[key] = "no-stored-pat-anchor (§64)"
                    continue
                work.append((sym, qe, basis, key))
    if limit:
        work = work[:limit]
    print("cells to read: %d" % len(work), flush=True)

    sess = FI.bse_session()
    time.sleep(1)
    pdfs = {}
    nread = 0
    by_sym = collections.defaultdict(list)
    for sym, qe, basis, key in work:
        by_sym[sym].append((qe, basis, key))

    for si, (sym, cells) in enumerate(sorted(by_sym.items()), 1):
        scrip = n2s.get(sym.upper())
        if not scrip:
            for qe, basis, key in cells:
                skips[key] = "no-bse-scrip"
            continue
        for qe, basis, key in cells:
            lo = qe_date(qe) + datetime.timedelta(days=8)
            hi = qe_date(qe) + datetime.timedelta(days=160)
            try:
                fils = FI.datebound(sess, str(scrip), lo.strftime("%Y%m%d"), hi.strftime("%Y%m%d")) or []
            except Exception as ex:
                skips[key] = "ann-list-error:%s" % type(ex).__name__
                continue
            time.sleep(0.3)
            if not fils:
                skips[key] = "no-result-filing-listed (qe+8d..qe+160d)"
                continue
            stored_pat = (fmap.get(sym, {}).get(qe) or [None] * 4)[1 if basis == "std" else 3]
            got, why, overlaid_seen = None, "no-overlaid-anchorable-page", False
            for annd, att, sub in fils[:10]:
                if att not in pdfs:
                    pdfs[att] = BG.cached_pdf(sess, att)[0]
                raw = pdfs[att]
                if not raw:
                    continue
                try:
                    doc = fitz.open(stream=raw, filetype="pdf")
                except Exception:
                    continue
                for pi in range(min(len(doc), 40)):
                    page = doc[pi]
                    lines, was_overlaid = page_lines_deoverlay(page)
                    overlaid_seen = overlaid_seen or was_overlaid
                    txt = " ".join(t for _, t, _ in lines)
                    if not BG.PL_PAGE.search(txt):
                        continue
                    if BG.BANKISH.search(txt[:2500]):
                        continue
                    head = txt[:1200]
                    is_con, is_std = bool(BG.CON_HDR.search(head)), bool(BG.STD_HDR.search(head))
                    bases = (["con"] if (is_con and not is_std)
                             else (["std"] if (is_std and not is_con) else ["std", "con"]))
                    if basis not in bases:
                        continue
                    merged = BG.merge_wrapped(lines)
                    rows = {}
                    for y, t, nums in merged:
                        low = t.strip()
                        for k, pat in BG.ROW_PATS.items():
                            if k in rows or not nums:
                                continue
                            if k in ("oi", "fc", "dep", "tax", "ti") and BG.re.search(r"profit|loss", low, BG.re.I):
                                continue
                            if k in ("oi", "tax", "ti"):
                                if pat.match(BG.re.sub(r"^[0-9ivxIVX\.\)\(\s]+", "", low)):
                                    rows[k] = nums
                            elif pat.search(low):
                                if k == "pat" and BG.ROW_PATS["own"].search(low):
                                    continue
                                rows[k] = nums
                        if "own" not in rows and nums and BG.re.search(
                                r"owners?\s+of\s+the\s+(company|parent)", low, BG.re.I):
                            rows["own"] = nums
                    rev_row, rev_rank = pick_rev_row(merged)
                    if rev_row is not None:
                        rows["rev"] = rev_row
                    # OWNERS ROW: take the LAST match, not the first. These statements print
                    # "Profit for the period attributable to Owners of the Company" twice (a
                    # split/continued line); on ABCAPITAL's audited Mar-2019 page the FIRST copy
                    # carries junk cells (-9.0, 10.0 at x 234/248) and the second the real vector
                    # (258.40 at the Mar-2019 column). Requiring >=3 figure cells drops the junk
                    # line outright; taking the last one is what fixes the general case.
                    own_last = None
                    for _, t, nums in merged:
                        if not nums or len(nums) < 3 or not BG.ROW_PATS["own"].search(t):
                            continue
                        # "Other/Total COMPREHENSIVE Income attributable to Owners" matches the
                        # same pattern and sits BELOW the profit row, so a plain last-match takes
                        # 272.13 where the owners' PROFIT is 258.40. Comprehensive income is a
                        # different quantity — exclude it explicitly.
                        if BG.re.search(r"comprehensive", t, BG.re.I):
                            continue
                        own_last = nums
                    if own_last is not None:
                        rows["own"] = own_last
                    if "rev" not in rows or ("pat" not in rows and "own" not in rows):
                        why = "page found but rev/PAT row unparsed"
                        continue
                    # COLUMN IDENTITY comes from the printed header date (§55b) — independent of
                    # anything we store, so it also settles which quarter a comparative column is.
                    words = (deoverlay_words(page.get_text("words")) if was_overlaid
                             else page.get_text("words"))
                    hd = header_dates(words)
                    pat_row = rows.get("own") if (basis == "con" and rows.get("own")) else rows.get("pat")
                    xcol, colwhy = date_column(rows, hd, qe, guard_row=pat_row)
                    if xcol is None:
                        why = "column-by-date: %s" % colwhy
                        continue
                    # SCALE from the anchor, never from magnitude: the stored PAT for this exact
                    # cell must be reproduced at this exact column under one of the unit hypotheses.
                    scale = None
                    pv_raw = BG.val_at(pat_row or [], xcol)
                    for sc in BG.SCALES:
                        if pv_raw is not None and BG.close(pv_raw / sc, stored_pat):
                            scale = sc
                            break
                    if scale is None:
                        why = ("printed-date column found for %d but its PAT %s reproduces no "
                               "stored value (%s) at any scale" % (qe, pv_raw, stored_pat))
                        continue
                    m = BG.metrics_at(rows, xcol, scale)
                    if not m:
                        why = "date-anchored column carries no readable metrics"
                        continue
                    rev, op, ebit = m
                    colmap = {qe: xcol}
                    if rev is None or rev <= 0:
                        why = "revenue absent or <=0 at the anchored column"
                        continue
                    ident = None
                    if "ti" in rows and "oi" in rows:
                        ti = BG.val_at(rows["ti"], colmap[qe])
                        oi = BG.val_at(rows["oi"], colmap[qe])
                        if ti is not None and oi is not None:
                            ti, oi = ti / scale, oi / scale
                            ident = {"total_income_printed": round(ti, 2),
                                     "rev_plus_other_income": round(rev + oi, 2),
                                     "ok": abs((rev + oi) - ti) <= max(0.05, 0.005 * abs(ti))}
                    got = {"rev": round(rev, 2),
                           "op": None if op is None else round(op, 2),
                           "ebit": None if ebit is None else round(ebit, 2),
                           "basis": basis, "stored_pat": stored_pat,
                           "pat_at_column": None if pv_raw is None else round(pv_raw / scale, 2),
                           "column_evidence": colwhy, "printed_header_dates": [q for _, q in hd],
                           "scale": scale, "identity": ident,
                           "src": "%s@%s" % (att[:24], annd), "page": pi, "deoverlaid": was_overlaid,
                           "method": ("printed-date column (§55b) + stored-PAT anchor + "
                                      "rev+other-income==total-income identity"
                                      + ("; text layer de-overlaid by majority vote over stacked "
                                         "renders" if was_overlaid else ""))}
                    break
                doc.close()
                if got:
                    break
            if not got:
                # report the ACTUAL last failure; `overlaid_seen` only records whether the
                # de-overlay path fired, since this reader also reads normal pages.
                skips[key] = "%s%s" % (why, " [no overlaid page seen]" if not overlaid_seen else "")
                continue
            if got["identity"] is not None and not got["identity"]["ok"]:
                skips[key] = "identity-failed rev+oi %.2f vs printed total income %.2f" % (
                    got["identity"]["rev_plus_other_income"], got["identity"]["total_income_printed"])
                continue
            if basis == "con":
                std_rev = ((revop.get(sym) or {}).get(str(qe)) or [None])[0]
                if std_rev and std_rev > 0 and got["rev"] < 0.5 * std_rev:
                    skips[key] = "con-rev-far-below-std %.2f vs %.2f" % (got["rev"], std_rev)
                    continue
            fills[key] = got
            nread += 1
            print("%-13s %d %-3s rev %-12.2f op %-11s pat@col %-9s (stored %-9s) identity %s%s" % (
                sym, qe, basis, got["rev"], got["op"], got["pat_at_column"], got["stored_pat"],
                "n/a" if got["identity"] is None else ("OK" if got["identity"]["ok"] else "FAIL"),
                "  [de-overlaid]" if got["deoverlaid"] else ""), flush=True)
        if si % 10 == 0:
            print("  [%d/%d syms] read %d" % (si, len(by_sym), nread), flush=True)
            json.dump(fills, open(FILLS, "w"), indent=1, sort_keys=True)
            json.dump(skips, open(SKIPS, "w"), indent=0, sort_keys=True)

    json.dump(fills, open(FILLS, "w"), indent=1, sort_keys=True)
    json.dump(skips, open(SKIPS, "w"), indent=0, sort_keys=True)
    print("\nREAD %d cells this run (%d ledgered)" % (nread, len(fills)))

    if not apply_it:
        print("(dry run — ledgers written, data files untouched. Re-run with --apply)")
        return

    # ★ THE ACCEPT RULE — two gates, and ONE of them must be essentially exact.
    # BG.close() admits a 0.4% PAT difference, which is fine as a COLUMN FINDER but far too loose
    # to be the only thing standing behind a written value: RAMCOCEM Mar-2019 reads 165.37 against
    # a stored 164.91 (0.28%, passes) on a page with no Total Income row, so nothing else checks
    # it. Land a cell only when the anchored PAT reproduces the stored one to ~the paisa, OR the
    # page's own rev+other-income==total-income identity holds. Everything else is HELD, with the
    # reason recorded — a near-miss anchor is a §58d result to adjudicate, not a value to write.
    applied = held = 0
    for key, v in sorted(fills.items()):
        sym, qe_s, basis = key.split("|")
        row = (revop.get(sym) or {}).get(qe_s)
        if row is None:
            continue
        pv, sp = v.get("pat_at_column"), v.get("stored_pat")
        exact = (pv is not None and sp is not None
                 and abs(pv - sp) <= max(0.05, 0.001 * abs(sp)))
        ident_ok = bool(v.get("identity") and v["identity"].get("ok"))
        if not (exact or ident_ok):
            v["held"] = ("anchor %.2f vs stored %.2f (%.2f%%) and no total-income identity on the "
                         "page — held for adjudication (§58d)"
                         % (pv, sp, 100.0 * abs(pv - sp) / max(abs(sp), 1e-9)))
            held += 1
            print("HOLD %-13s %s %-3s  %s" % (sym, qe_s, basis, v["held"]))
            continue
        # REVENUE ONLY (slot 0/1). op and ebit are reconstructions from expense components and a
        # wrong OPM is a visible site bug, so read_std_rev_nse.py / read_con_rev_nse.py both refuse
        # to write them and this reader follows the same convention. They stay in the ledger.
        for field in ("rev",):
            slot = SLOT[basis][field]
            if v.get(field) is None or row[slot] is not None:
                continue
            row[slot] = v[field]
            applied += 1
            lrow = ledger.setdefault(sym, {}).get(qe_s)
            if lrow is None:
                ledger[sym][qe_s] = list(row)
            elif lrow[slot] is None:
                lrow[slot] = v[field]
    json.dump(revop, open(REVOP_DOCS, "w"), separators=(",", ":"))
    json.dump(ledger, open(REVOP_LEDGER, "w"), separators=(",", ":"))
    json.dump(fills, open(FILLS, "w"), indent=1, sort_keys=True)
    print("APPLIED %d cell-values; HELD %d for adjudication" % (applied, held))


if __name__ == "__main__":
    main()
