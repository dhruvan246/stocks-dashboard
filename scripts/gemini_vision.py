# -*- coding: utf-8 -*-
"""FREE vision reader for insurer P&L via Google Gemini (free tier — no billing, no card).

Insurers whose IRDAI filings the text parser can't crack (scanned/image P&L tables like ICICIGI/
STARHEALTH/LIC, or with-subsidiary owners-profit that's computed across rows like GICRE/NIACL/ICICIPRULI/
MFSL) are read by Gemini vision instead. Reads BOTH the current and the year-ago quarter (con + std) so
the caller can ANCHOR the read against our stored same-quarter-last-year con — never store an unanchored
guess. Gemini's free tier (aistudio.google.com key) is ~1,500 req/day; we use ~7/quarter → always free.

REST via stdlib urllib (no extra pip dep). Needs GEMINI_API_KEY in the environment (a repo secret in CI).
Returns None when the key is absent, so the daily job degrades gracefully to text-only.

Public: read_insurer(company, cur_label, yago_label, pngs, with_subsidiary)
    -> {"ok","company_matches","has_subsidiary","cur":{"con","std"},"yago":{"con","std"},"note"} | None
       (all PAT in Rs CRORE, owner-attributable for con)
"""
import os, json, base64, urllib.request

_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash")
_URL = "https://generativelanguage.googleapis.com/v1beta/models/%s:generateContent?key=%s"

_SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "ok": {"type": "BOOLEAN"},
        "company_matches": {"type": "BOOLEAN"},
        "has_subsidiary": {"type": "BOOLEAN"},
        "cur_con": {"type": "NUMBER", "nullable": True},
        "cur_std": {"type": "NUMBER", "nullable": True},
        "yago_con": {"type": "NUMBER", "nullable": True},
        "yago_std": {"type": "NUMBER", "nullable": True},
        "note": {"type": "STRING"},
    },
    "required": ["ok", "company_matches", "has_subsidiary", "cur_con", "yago_con", "cur_std", "yago_std", "note"],
}

_PROMPT = """These images are the quarterly results filing of an Indian INSURANCE company: %(company)s.
Insurers file in IRDAI format, NOT a standard P&L. Read the images (some are scanned).

Return PROFIT AFTER TAX (net profit), in Rs CRORE, for:
  - the CURRENT quarter %(cur)s  -> cur_con (consolidated) and cur_std (standalone)
  - the YEAR-AGO quarter %(yago)s -> yago_con and yago_std

WHICH ROW (this is where mistakes happen):
- Use the SHAREHOLDERS' Profit & Loss Account row "Profit AFTER tax" (a.k.a. "Profit for the period" in
  the Shareholders' A/c) — it comes after "Profit before tax" -> "Provision for tax".
- DO NOT use: "Operating Profit/(loss)" or "Operating Profit transferred to P&L" (that is the
  Policyholders' Revenue A/c), "Profit carried to Balance Sheet" (accumulated retained earnings, a huge
  cumulative number), segment results, or any "Net profit margin" ratio.
- CONSOLIDATED (con): if the company has subsidiaries/associates/minority, use OWNER-ATTRIBUTABLE profit —
  the "attributable to: Owners / Equity holders of the parent" line if present, else (total profit after
  tax) + (minority/non-controlling interest, with sign) + (share of profit of associates). For companies
  reporting continuing + discontinued operations, con = the TOTAL owners profit (sum of both).
- STANDALONE (std): the standalone Shareholders' A/c Profit after tax. If the filing shows only ONE
  statement, put that value in BOTH cur_std and cur_con and set has_subsidiary=false.

UNIT — convert to Rs CRORE: "in Lakhs" ÷100; "in Thousands" ÷100000; "in Millions" ÷10; "in Crores" keep;
bare Rupees ÷10000000. A quarterly insurer net profit is tens to a few thousand crore.

IDENTITY: if these images are NOT %(company)s or you can't find the Shareholders' P&L, set
company_matches=false (or ok=false) and null every figure. Losses are negative. Return ONLY the JSON."""


def _key():
    return os.environ.get("GEMINI_API_KEY")


def read_insurer(company, cur_label, yago_label, pngs, with_subsidiary=True):
    """pngs: list of PNG byte strings (rendered P&L pages). Returns the normalized dict or None."""
    key = _key()
    if not key or not pngs:
        return None
    parts = [{"inline_data": {"mime_type": "image/png", "data": base64.standard_b64encode(p).decode()}}
             for p in pngs[:6]]
    parts.append({"text": _PROMPT % {"company": company, "cur": cur_label, "yago": yago_label}})
    body = {
        "contents": [{"role": "user", "parts": parts}],
        "generationConfig": {"temperature": 0, "response_mime_type": "application/json",
                             "response_schema": _SCHEMA},
    }
    req = urllib.request.Request(_URL % (_MODEL, key), data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    try:
        raw = urllib.request.urlopen(req, timeout=90).read()
        resp = json.loads(raw)
        txt = resp["candidates"][0]["content"]["parts"][0]["text"]
        d = json.loads(txt)
    except Exception as ex:
        print("    gemini err:", str(ex)[:110])
        return None
    return {"ok": bool(d.get("ok")), "company_matches": bool(d.get("company_matches")),
            "has_subsidiary": bool(d.get("has_subsidiary")),
            "cur": {"con": d.get("cur_con"), "std": d.get("cur_std")},
            "yago": {"con": d.get("yago_con"), "std": d.get("yago_std")},
            "note": d.get("note", "")}
