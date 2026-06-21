# -*- coding: utf-8 -*-
import sys, json, re
from collections import defaultdict
import fitz

pdf_path = sys.argv[1]
basis = sys.argv[2]  # 'std' or 'con'
doc = fitz.open(pdf_path)
print("PAGES:", len(doc))

want = "consolidated" if basis=="con" else "standalone"
PFT = re.compile(r'profit')
for p in range(len(doc)):
    low = doc[p].get_text().lower()
    has_basis = want in low
    has_qe = ("quarter ended" in low) or ("period ended" in low) or ("quarter and" in low)
    has_profit = ("profit for the" in low) or ("profit after tax" in low) or ("profit/(loss)" in low) or ("profit /(loss)" in low)
    if has_basis and has_profit and has_qe:
        # also flag if the OTHER basis appears (to avoid combined pages confusion)
        other = "standalone" if basis=="con" else "consolidated"
        print("PAGE %d  basis=%s other_also=%s" % (p, has_basis, other in low))
