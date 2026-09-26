# -*- coding: utf-8 -*-
"""The one header set every BSE request carries (runbook §181, 2026-09-26).

From 20-Sep-2026 BSE's front door answered 403 "Access Denied" to any request that carried only a User-Agent (+Referer):
seven daily jobs went dark for six days while staying green. A request carrying the full STANDARD header set a browser
sends (Accept-Language, sec-ch-ua, Sec-Fetch-*) is served normally — measured 200 with plain urllib and plain curl. No
TLS impersonation, no proxies: just complete headers.

Importing this module installs the headers on every urllib request to *.bseindia.com that lacks them (OpenerDirector.open
is wrapped, so urlopen / build_opener / cookie openers are all covered). curl callers splice CURL_ARGS after "-A UA".
"""
import urllib.request
from urllib.parse import urlsplit

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
HEADERS = {"User-Agent": UA, "Accept": "application/json, text/plain, */*", "Accept-Language": "en-US,en;q=0.9",
           "Referer": "https://www.bseindia.com/", "Origin": "https://www.bseindia.com", "Connection": "keep-alive",
           "sec-ch-ua": '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
           "sec-ch-ua-mobile": "?0", "sec-ch-ua-platform": '"Windows"',
           "Sec-Fetch-Dest": "empty", "Sec-Fetch-Mode": "cors", "Sec-Fetch-Site": "same-site"}
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
    """Add every standard header the request lacks (never overrides one the caller set)."""
    if isinstance(req, urllib.request.Request) and is_bse(req.full_url):
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
