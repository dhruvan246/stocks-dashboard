# -*- coding: utf-8 -*-
import sys, fitz
pdf_path, page, out = sys.argv[1], int(sys.argv[2]), sys.argv[3]
y0, y1 = float(sys.argv[4]), float(sys.argv[5])
doc = fitz.open(pdf_path)
pg = doc[page]
r = pg.rect
clip = fitz.Rect(r.x0, r.y0 + (r.height*y0), r.x1, r.y0 + (r.height*y1))
pix = pg.get_pixmap(dpi=320, clip=clip)
pix.save(out)
print("saved", out, pix.width, "x", pix.height)
