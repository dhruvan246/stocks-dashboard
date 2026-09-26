# -*- coding: utf-8 -*-
"""The one header set every BSE request carries (runbook §181, 2026-09-26).

From 20-Sep-2026 BSE's front door answered 403 "Access Denied" to any request that carried only a User-Agent (+Referer):
seven daily jobs went dark for six days while staying green. A request carrying the full STANDARD header set a browser
sends (Accept-Language, Referer) is served normally. Since 2026-09-26 22:55 IST the set is HONEST: our own User-Agent,
no browser impersonation (no Chrome UA / sec-ch-ua / Sec-Fetch-*) -- measured 200 on every endpoint the repo uses.

Importing this module installs the headers on every urllib request to *.bseindia.com that lacks them (OpenerDirector.open
is wrapped, so urlopen / build_opener / cookie openers are all covered). curl callers splice CURL_ARGS after "-A UA".
"""
import urllib.request
from urllib.parse import urlsplit

# HONEST identification (2026-09-26 22:55 IST, measured): BSE serves a script that names itself, provided the request
# carries Accept-Language and Referer (UA-only -> 403; no Referer -> 404; no Accept-Language -> 403). No browser
# impersonation: no Chrome User-Agent, no sec-ch-ua*, no Sec-Fetch-*, no Origin. Never add those back. (A UA carrying a
# URL, "+https://...", is refused 403 -- measured; keep the plain name.)
UA = "stocks-dashboard-research/1.0"
HEADERS = {"User-Agent": UA, "Accept": "application/json, text/plain, */*", "Accept-Language": "en-US,en;q=0.9",
           "Referer": "https://www.bseindia.com/", "Connection": "keep-alive"}
# curl sends NO Accept-Encoding by default and BSE refuses such a request (measured: identical headers, 403 without
# --compressed, 200 with it); urllib always sends "Accept-Encoding: identity", which is why it passed. --compressed also
# makes curl decompress the body itself, so callers keep receiving plain bytes.
CURL_ARGS = ["--compressed"]
for _k, _v in HEADERS.items():
    if _k != "User-Agent":
        CURL_ARGS += ["-H", "%s: %s" % (_k, _v)]


def is_bse(url):
    try:
        return urlsplit(url).hostname.lower().endswith("bseindia.com")
    except Exception:
        return False


def complete(req):
    """Add every standard header the request lacks; replace any browser-impersonating one the caller set (own UA)."""
    if isinstance(req, urllib.request.Request) and is_bse(req.full_url):
        for store in (req.headers, req.unredirected_hdrs):
            for k in list(store):
                kl = k.lower()
                if kl == "user-agent" or kl == "origin" or kl.startswith("sec-ch-ua") or kl.startswith("sec-fetch-"):
                    del store[k]
        have = {k.lower() for k in req.headers} | {k.lower() for k in req.unredirected_hdrs}
        for k, v in HEADERS.items():
            if k.lower() not in have:
                req.add_header(k, v)
    return req


def install():
    if getattr(urllib.request.OpenerDirector, "_bse_headers_installed", False):
        return
    _open = urllib.request.OpenerDirector.open

    def open_(self, fullurl, data=None, timeout=urllib.request.socket._GLOBAL_DEFAULT_TIMEOUT):
        if isinstance(fullurl, str) and is_bse(fullurl):
            fullurl = urllib.request.Request(fullurl)
        return _open(self, complete(fullurl), data, timeout)
    urllib.request.OpenerDirector.open = open_
    urllib.request.OpenerDirector._bse_headers_installed = True


install()
