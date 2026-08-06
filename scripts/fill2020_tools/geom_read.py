# -*- coding: utf-8 -*-
"""GEOMETRIC column addressing -- the correct implementation of §55b.

Every column bug in this campaign has the same root: `values[i]` from one row was assumed to mean
the same period as `values[i]` from another row. In a PDF that is simply false. Text extraction
linearises a table, and rows legitimately differ in width -- merged cells, a notes column, a
sub-total that spans, a footnote marker parsed as a token. So:

    BALKRISIND: PAT row and revenue row differ in width -> index 5 pointed at a different period
                -> 170.61, which is the exact wrong number I refused by hand earlier.
    Requiring equal width instead makes the reader SAFE but nearly blind (1 of 7).

The fix is not a better index rule. It is to stop using indices. Financial statements RIGHT-ALIGN
their figures, so a period column is a vertical band of x-coordinates, stable across every row of
the table. `page.get_text("words")` gives each token's bounding box, so:

    1. group tokens into visual lines by y
    2. on the PAT row, find the token whose value reproduces our STORED PAT for this quarter+basis
       at some declared scale -> that token's RIGHT EDGE x1 is the target column's x
    3. on the revenue row, take the token whose right edge sits in the SAME band
    4. confirm: a DIFFERENT band on the same rows must reproduce a DIFFERENT quarter we store

Two rows can now have completely different widths and the read is still correct, because the column
is a geometric fact about the page rather than a guess about list positions.
"""
import re

NUMTOK = re.compile(r"^\(?-?[\d,]+\.?\d*\)?$")
SCALES = ((1.0, "crore"), (10.0, "million"), (100.0, "lakh"))
XTOL = 14.0          # points; columns are typically 40-90pt apart, so this is comfortably tight


def parse_num(tok):
    if not NUMTOK.match(tok):
        return None
    neg = tok.startswith("(")
    try:
        v = float(tok.strip("()").replace(",", ""))
    except ValueError:
        return None
    return -v if neg else v


def lines_of(page, ytol=3.0):
    """-> [(y, label_text, [(x_right, value), ...])] in reading order."""
    words = page.get_text("words")          # (x0, y0, x1, y1, word, block, line, word_no)
    if not words:
        return []
    rows = {}
    for x0, y0, x1, y1, w, *_ in words:
        rows.setdefault(round(y0 / ytol), []).append((x0, x1, w))
    out = []
    for key in sorted(rows):
        toks = sorted(rows[key], key=lambda t: t[0])
        vals, label = [], []
        for x0, x1, w in toks:
            v = parse_num(w)
            if v is None:
                if not vals:                 # label words precede the figures
                    label.append(w)
            else:
                vals.append((x1, v))
        if vals:
            out.append((key * ytol, " ".join(label), vals))
    return out


def at_column(vals, x, tol=XTOL):
    """The value whose right edge sits in the band around x, or None."""
    best = None
    for x1, v in vals:
        d = abs(x1 - x)
        if d <= tol and (best is None or d < best[0]):
            best = (d, v)
    return None if best is None else best[1]


def find(page, anchor, others, pat_pats, rev_pats):
    """-> (revenue, evidence) using geometric columns.

    `anchor`  : our stored PAT for the target quarter+basis (locates the column)
    `others`  : [(name, value)] other stored figures used to CONFIRM a second column
    """
    rows = lines_of(page)
    if not rows:
        return None, None
    pat_rows = [r for r in rows if any(p.search(r[1]) for p in pat_pats)]
    rev_rows = [r for r in rows if any(p.search(r[1]) for p in rev_pats)]
    if not pat_rows or not rev_rows:
        return None, None
    for _y, plabel, pvals in pat_rows:
        for x1, v in pvals:
            for sc, un in SCALES:
                if abs(v / sc - anchor) > max(0.05, abs(anchor) * 0.004):
                    continue
                # CONFIRM: a different band on this same PAT row must reproduce another stored value
                conf = None
                for name, ov in others:
                    if ov is None or abs(ov - anchor) <= max(0.05, abs(anchor) * 0.004):
                        continue
                    for x2, w in pvals:
                        if abs(x2 - x1) > XTOL and abs(w / sc - ov) <= max(0.05, abs(ov) * 0.004):
                            conf = "col x=%.0f reproduces %s %.2f at the same scale" % (x2, name, ov)
                            break
                    if conf:
                        break
                if conf is None:
                    continue
                for _y2, rlabel, rvals in rev_rows:
                    got = at_column(rvals, x1)
                    if got is not None and got > 0:
                        return round(got / sc, 2), {
                            "x": round(x1, 1), "scale": un, "rev_row": rlabel[:44],
                            "pat_row": plabel[:44], "anchor": anchor, "confirm": conf,
                            "method": "geometric column (§55b)"}
    return None, None
