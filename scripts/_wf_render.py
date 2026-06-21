# -*- coding: utf-8 -*-
import sys, fitz
pdf_path, page, out = sys.argv[1], int(sys.argv[2]), sys.argv[3]
doc = fitz.open(pdf_path)
pix = doc[page].get_pixmap(dpi=250)
pix.save(out)
print("saved", out, pix.width, "x", pix.height)
