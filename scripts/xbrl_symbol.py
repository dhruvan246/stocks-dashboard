# -*- coding: utf-8 -*-
"""Resolve a filing XBRL's company symbol when the file carries a PLACEHOLDER (runbook §177).

A company that files its results on NSE while not (yet) listed there — the 2026-04/08 permitted-to-trade batches — tags
`Symbol` / the NSESymbol identifier as "NOTLISTED" (or "NA"). build_revop.py and build_xbrl_extra.py keyed every such
file under that literal, so several companies were merged into one bogus "NOTLISTED" record (sf_revop Dec-25/Mar-26,
xbrl_extra Dec-25..Jun-26) and lost from their own pages. The same files carry the ISIN, which maps to the NSE symbol.

resolve(sym, xml) -> the symbol to key under, or None when a placeholder cannot be resolved (skip the file — never a
bogus key). Map sources, loaded once: NSE's EQUITY_L.csv + SME_EQUITY_L.csv (nsearchives, the files build_bse_universe
and fetch_all already read) and the committed tape's trailing meta ISINs (docs/sf_stock_data.bin) as the offline fallback.
"""
import os, re, io, csv, gzip, json

HERE = os.path.dirname(os.path.abspath(__file__))
PLACEHOLDERS = {"NOTLISTED", "NA", "-", "N.A.", "NIL", ""}
RE_ISIN = re.compile(r"<in-(?:capmkt|bse-fin):ISIN[^>]*>\s*([A-Z0-9]{12})\s*<")
_MAP = None
LISTS = ("https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv",
         "https://nsearchives.nseindia.com/emerge/corporates/content/SME_EQUITY_L.csv")


def _load():
    global _MAP
    if _MAP is not None:
        return _MAP
    m = {}
    try:                                            # offline fallback first; the exchange lists override it
        b = gzip.decompress(open(os.path.join(HERE, "..", "docs", "sf_stock_data.bin"), "rb").read())
        meta, _ = json.JSONDecoder().raw_decode(b[b.rfind(b'"meta":') + 7:].decode("utf-8"))
        for s, v in meta.items():
            if isinstance(v, dict) and v.get("isin"):
                m[v["isin"]] = s
    except (OSError, ValueError):
        pass
    try:
        import build_fundamentals as B
        for url in LISTS:
            try:
                raw = B._get(url, headers={"User-Agent": B.UA, "Accept": "*/*"}, timeout=60)
            except Exception:
                continue
            for r in csv.DictReader(io.StringIO(raw)):
                r = {(k or "").strip(): (v or "").strip() for k, v in r.items()}
                if r.get("ISIN NUMBER") and r.get("SYMBOL"):
                    m[r["ISIN NUMBER"]] = r["SYMBOL"]
    except Exception:
        pass
    _MAP = m
    return m


def resolve(sym, xml):
    s = (sym or "").strip()
    if s.upper() not in PLACEHOLDERS:
        return s
    mi = RE_ISIN.search(xml)
    return _load().get(mi.group(1)) if mi else None
