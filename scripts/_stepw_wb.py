#!/usr/bin/env python3
"""STEP W-execute: wayback CDX + fetch helper. Trimmed from the STEP W probe harness.

None from cdx() means UNPROVEN (throttled/network fail) -- distinct from [] (proven empty).
Never rotate headers/retry through a persistent block -- back off hard and let the caller decide
whether to stop (campaign hard line, DATA_RUNBOOK Sec.38).
"""
import json, os, re, time
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
CACHE = os.path.join(HERE, "_wb_cache")
os.makedirs(CACHE, exist_ok=True)
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) stocks-dashboard/stepw-harvest"}


def _cache_path(key):
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", key)[:180]
    return os.path.join(CACHE, safe)


def cdx(url, frm=None, to=None, limit=60000, match="prefix", fl="timestamp,original,statuscode",
        collapse="urlkey", fresh=False):
    p = {"url": url, "output": "json", "fl": fl, "limit": str(limit), "matchType": match}
    if collapse:
        p["collapse"] = collapse
    if frm:
        p["from"] = frm
    if to:
        p["to"] = to
    q = "&".join("%s=%s" % (k, requests.utils.quote(str(v), safe="")) for k, v in p.items())
    api = "https://web.archive.org/cdx/search/cdx?" + q
    cp = _cache_path("cdx_" + q) + ".json"
    if os.path.exists(cp) and not fresh:
        return json.load(open(cp, encoding="utf-8"))
    last = None
    for attempt in range(5):
        try:
            r = requests.get(api, headers=UA, timeout=180)
            if r.status_code != 200:
                last = "http %s" % r.status_code
                time.sleep(20 * (attempt + 1))
                continue
            txt = r.text.strip()
            rows = json.loads(txt) if txt else []
            hdr = rows[0] if rows else []
            out = [dict(zip(hdr, row)) for row in rows[1:]]
            json.dump(out, open(cp, "w", encoding="utf-8"))
            return out
        except Exception as e:  # noqa
            last = str(e)[:80]
            time.sleep(20 * (attempt + 1))
    print("  !! cdx fail %s :: %s" % (url, last), flush=True)
    return None


def wb_fetch(ts, original, fresh=False):
    """id_ = raw original bytes, no wayback toolbar/rewriting.

    TWO attempts, 2s apart. The original single-attempt form reasoned that "more, quicker passes
    beat fewer, slower ones" -- sound, but it assumed the passes would actually be frequent.
    MEASURED 2026-08-06: paired with the caller's abort-the-whole-run stop-gate and the wrapper's
    180s sleep, each pass did 2-9s of work per 185s of wall-clock (~3% duty cycle), because
    wayback fails in bursts of ~8 and one burst discarded the entire pass. Two tries 2s apart
    clears most bursts in-place. This is NOT "retrying through a persistent block" (Sec.38's hard
    line) -- the caller's tiered backoff is what detects a real block and stops. A cell whose
    candidates still fail stays RETRYABLE (never a permanent refusal, see
    _stepw_nse_pre15.py's fetch_incomplete_fys)."""
    u = "https://web.archive.org/web/%sid_/%s" % (ts, original)
    cp = _cache_path("wb_%s_%s" % (ts, original)) + ".html"
    if os.path.exists(cp) and not fresh:
        return open(cp, encoding="utf-8", errors="replace").read()
    last = None
    for attempt in range(2):
        try:
            r = requests.get(u, headers=UA, timeout=45)
            if r.status_code in (200, 404, 403):
                t = r.text
                open(cp, "w", encoding="utf-8", errors="replace").write(t)
                return t
            last = "http %s" % r.status_code
        except Exception as e:  # noqa
            last = str(e)[:80]
        if attempt == 0:
            time.sleep(2)
    print("  !! wb fail %s %s :: %s" % (ts, original[:70], last), flush=True)
    return None   # None == fetch failed (retry later). "" would mean "fetched, empty".
