# -*- coding: utf-8 -*-
"""Vision reader for INSURER quarterly net profit (IRDAI-format filings the XBRL cron can't parse).

Insurers file Policyholders' Revenue A/c + Shareholders' Profit & Loss A/c ("Premium earned" /
"Income from investments") instead of the standard "Revenue from operations" P&L, so
`update_fundamentals.py` gets NOTHING for them. This module renders the consolidated Shareholders'
P&L page(s) of a filing and asks the Anthropic vision API to read Profit-after-tax, encoding the
manual INSURER_EXTRACTION_PLAYBOOK.md rules so the daily cron can fill insurers UNATTENDED.

It reads BOTH the current quarter AND the year-ago quarter so the caller can ANCHOR the read against
our stored same-quarter-last-year value (the playbook's primary verification) — never store an
unanchored guess.

Requires ANTHROPIC_API_KEY (a repo secret in CI). Returns None when the key/deps are absent, so the
daily job degrades gracefully (insurers just stay gapped, exactly as before this existed).

Public: read_insurer(company, cur_label, yago_label, pngs, with_subsidiary) -> dict | None
    -> {"ok","company_matches","has_subsidiary",
        "cur":{"con","std"}, "yago":{"con","std"}, "note"}   (all PAT in Rs CRORE, owner-attributable)
"""
import os, base64, json

_MODEL = os.environ.get("INSURER_VISION_MODEL", "claude-haiku-4-5")   # vision-capable, cheap

_SCHEMA = {
    "type": "object", "additionalProperties": False,
    "properties": {
        "ok": {"type": "boolean"},
        "company_matches": {"type": "boolean"},
        "has_subsidiary": {"type": "boolean"},
        "cur":  {"type": "object", "additionalProperties": False,
                 "properties": {"con": {"type": ["number", "null"]}, "std": {"type": ["number", "null"]}},
                 "required": ["con", "std"]},
        "yago": {"type": "object", "additionalProperties": False,
                 "properties": {"con": {"type": ["number", "null"]}, "std": {"type": ["number", "null"]}},
                 "required": ["con", "std"]},
        "note": {"type": "string"},
    },
    "required": ["ok", "company_matches", "has_subsidiary", "cur", "yago", "note"],
}

_PROMPT = """These images are the quarterly results filing of an Indian INSURANCE company: %(company)s.
Insurers file in IRDAI format, NOT the standard P&L. Read the images (some are scanned).

I need PROFIT AFTER TAX (net profit), in Rs CRORE, for:
  - the CURRENT quarter: %(cur)s   -> "cur"
  - the YEAR-AGO quarter:  %(yago)s  -> "yago"
and for BOTH the consolidated ("con") and standalone ("std") statements.

WHICH ROW (this is where mistakes happen — follow exactly):
- Use the SHAREHOLDERS' Profit & Loss Account: the row "Profit AFTER tax" (a.k.a. "Profit for the
  period/quarter" in the Shareholders' A/c). It comes after "Profit before tax" -> "Provision for tax".
- DO NOT use: "Operating Profit / (loss)" or "Operating Profit transferred to P&L" (that is only the
  Policyholders' Revenue A/c), "Profit / (Loss) carried to Balance Sheet" (that is ACCUMULATED retained
  earnings, a huge cumulative number), segment-wise results, or any "Net profit margin" ratio.
- CONSOLIDATED ("con"): if the company has subsidiaries/associates/minority interest, use the
  OWNER-ATTRIBUTABLE profit — read the explicit "attributable to: Owners / Equity holders of the parent"
  line if present, otherwise (total profit after tax) + (minority/non-controlling interest, with its
  sign) + (share of profit of associates). If there is only ONE consolidated PAT figure with no
  minority split, the company has no material minority -> con == that figure; set has_subsidiary=false.
- STANDALONE ("std"): the Shareholders' A/c Profit after tax in the STANDALONE statement.
- If the filing shows only ONE statement (many general insurers file standalone only), put that value
  in "std" AND "con" and set has_subsidiary=false.

UNIT — the filing states its unit; convert to Rs CRORE:
  "in Lakhs"     -> divide by 100
  "in Thousands" -> divide by 100000
  "in Millions"  -> divide by 10
  "in Crores"    -> keep as-is
  bare Rupees    -> divide by 10000000
A quarterly insurer net profit is typically tens to a few thousand crore. If your number is in the
hundreds of thousands, you used the wrong unit or the wrong (cumulative) row.

IDENTITY: if these images are NOT %(company)s, or you cannot find the Shareholders' P&L, set
company_matches=false (or ok=false) and null every figure. Losses are negative. Return ONLY the JSON.
"""


def _client():
    if not os.environ.get("ANTHROPIC_API_KEY"):
        return None
    try:
        import anthropic
        return anthropic.Anthropic()
    except Exception:
        return None


def read_insurer(company, cur_label, yago_label, pngs, with_subsidiary=True):
    """pngs: list of PNG byte strings (rendered Shareholders' P&L pages). Returns parsed dict or None."""
    cli = _client()
    if not cli or not pngs:
        return None
    content = [{"type": "image", "source": {"type": "base64", "media_type": "image/png",
               "data": base64.standard_b64encode(p).decode()}} for p in pngs[:16]]
    content.append({"type": "text",
                    "text": _PROMPT % {"company": company, "cur": cur_label, "yago": yago_label}})
    try:
        resp = cli.messages.create(
            model=_MODEL, max_tokens=1024,
            output_config={"format": {"type": "json_schema", "schema": _SCHEMA}},
            messages=[{"role": "user", "content": content}],
        )
        txt = next((b.text for b in resp.content if b.type == "text"), None)
        return json.loads(txt) if txt else None
    except Exception as ex:
        print("    insurer-vision err:", str(ex)[:90])
        return None
