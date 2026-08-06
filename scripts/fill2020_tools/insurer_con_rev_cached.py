# -*- coding: utf-8 -*-
"""insurer_con_rev with the page-OCR results cached on disk.

The OCR fallback (runbook §55d) costs ~1-2s per page and the tool re-renders every page of every
candidate filing on each run, so a re-run over one insurer is tens of minutes of work already done.
`gicre_con_pat.py` built exactly this cache while auditing the con-PAT anchors, under the SAME
`<SYM>_<qe>_<attachment>` filenames, so pointing this run at it makes the audited quarters instant.

This is a wrapper, not an edit: `insurer_con_rev.py` is shared with the rest of the FILL-2020
campaign and its gates must stay untouched. Only FI._ocr_words is memoised, and a cache miss falls
through to the real renderer.

Run:  python -X utf8 scripts/fill2020_tools/insurer_con_rev_cached.py --only GICRE [--apply]
"""
import hashlib
import os
import pickle
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(ROOT, "scripts"))
sys.path.insert(0, HERE)

import fetch_insurers as FI              # noqa: E402
import insurer_con_rev as ICR            # noqa: E402

OCRCACHE = os.path.join(ROOT, "scripts", "_gicre_ocr")
PDFCACHE = os.path.join(ROOT, "scripts", "_ins_pdfcache")
os.makedirs(OCRCACHE, exist_ok=True)
_real = FI._ocr_words
_mem = {}


def _by_digest():
    """{md5 of the PDF bytes: cached filename} for everything in the PDF cache.

    The cache files are named after the attachment, but insurer_con_rev opens its PDFs from a
    BYTE STREAM (`fitz.open(stream=...)`), and a stream-opened document has no `.name` — so the
    filename is not reachable from a page. Hashing the cached PDFs once at startup gives the
    mapping back, and it is robust to either tool renaming its files."""
    idx = {}
    for fn in os.listdir(PDFCACHE) if os.path.isdir(PDFCACHE) else []:
        try:
            with open(os.path.join(PDFCACHE, fn), "rb") as fh:
                idx[hashlib.md5(fh.read()).hexdigest()] = fn
        except Exception:
            pass
    return idx


DIGESTS = _by_digest()


# fitz.open(stream=...) is intercepted so the ORIGINAL bytes can be hashed. Hashing the Document
# afterwards does not work: doc.write() re-serialises and produces a different digest.
_STREAM_KEY = {}
_fitz_open = ICR.fitz.open


def _open(*a, **kw):
    doc = _fitz_open(*a, **kw)
    blob = kw.get("stream")
    if blob:
        _STREAM_KEY[id(doc)] = DIGESTS.get(hashlib.md5(blob).hexdigest(), "")
    return doc


ICR.fitz.open = _open


def _doc_key(page):
    return _STREAM_KEY.get(id(page.parent), "")


def _cached(page):
    key = _doc_key(page)
    if not key:
        return _real(page)
    if key not in _mem:
        p = os.path.join(OCRCACHE, key + ".pkl")
        try:
            _mem[key] = pickle.load(open(p, "rb")) if os.path.exists(p) else {}
        except Exception:
            _mem[key] = {}
    pages = _mem[key]
    n = page.number
    if n not in pages:
        pages[n] = _real(page)
        try:
            pickle.dump(pages, open(os.path.join(OCRCACHE, key + ".pkl"), "wb"))
        except Exception:
            pass
    return pages[n]


FI._ocr_words = _cached
ICR.FI._ocr_words = _cached

if __name__ == "__main__":
    ICR.main()
