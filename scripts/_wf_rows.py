# -*- coding: utf-8 -*-
import sys, re
from collections import defaultdict
import fitz
pdf_path, page = sys.argv[1], int(sys.argv[2])
doc = fitz.open(pdf_path)
pg = doc[page]
# header detection
low = pg.get_text().lower()
print("--- unit hint ---")
for kw in ["lakh","lac","crore","million","rupees in"]:
    if kw in low: print("  found:", kw)
print("--- profit-related rows (with x-sorted cells) ---")
rows = defaultdict(list)
for w in pg.get_text("words"):
    rows[round(w[1]/3)*3].append((w[0], w[4]))
NUM = re.compile(r'^\(?-?[\d,]+\.?\d*\)?$')
for y in sorted(rows):
    cells = sorted(rows[y])
    txt = " ".join(w for _,w in cells)
    l = txt.lower()
    if ("profit" in l or "quarter ended" in l or "period ended" in l or "year ended" in l or "particulars" in l or "ended" in l):
        nums = [(round(x),w) for x,w in cells if NUM.match(w.replace(',',''))]
        label = " ".join(w for x,w in cells if not NUM.match(w.replace(',','')))
        print("y=%4d | %s || nums=%s" % (y, label[:60], nums))
