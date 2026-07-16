# -*- coding: utf-8 -*-
"""Fetch the NSE IPO pipeline: UPCOMING issues, OPEN issues (with live subscription),
and RECENT LISTINGS (with issue price vs current price) -> docs/ipos.json for the
"IPOs & Listings" page.

SOURCES (urllib + cookie warmup — the announcements cron's CI-proven session):
  /api/all-upcoming-issues?category=ipo   {companyName,symbol,series EQ|SME,issueStartDate,
                                           issueEndDate,issuePrice "Rs.X to Rs.Y",issueSize(shares),status}
  /api/ipo-current-issue                  same + subscription: noOfTime (x subscribed, category Total)
  /api/public-past-issues                 full archive (~1.4k): company,symbol,securityType,ipoStartDate,
                                          ipoEndDate,listingDate,issuePrice(final),priceRange
  Current price + mcap for listed names come from dash_slim.bin meta —
  ⚠️ keyed 'RELIANCE.NS' (Yahoo suffix): re-key by bare symbol or every join misses (runbook §26).
  SME listings aren't in dash_slim -> shown without a current price (board chip says SME).

STATELESS: every run rebuilds from full snapshots (no merge/self-heal needed). Refuses to
write only when the past-issues archive comes back suspiciously small.

Output docs/ipos.json:
  {"updated",
   "upcoming":[[sym,name,board,openISO,closeISO,band,shares,status,subTimes|null],...],
   "listed":  [[sym,name,board,listISO,issuePrice,lastPx|null,mcapCr|null],...]}  # last LISTED_DAYS
Run: python -X utf8 scripts/fetch_ipos.py
"""
import os, sys, json, gzip, re, datetime
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import build_fundamentals as B  # _get / nse_jar / UA

HERE = os.path.dirname(os.path.abspath(__file__))
DOCS = os.path.join(HERE, "..", "docs")
OUT = os.path.join(DOCS, "ipos.json")
SLIM = os.path.join(DOCS, "dash_slim.bin")
LISTED_DAYS = 180
MIN_PAST = 100
MON = {"jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,"jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12}

def iso(s):
    m = re.match(r"^(\d{1,2})-([A-Za-z]{3})-(\d{4})", str(s or "").strip())
    if not m: return None
    mo = MON.get(m.group(2).lower())
    return "%s-%02d-%02d" % (m.group(3), mo, int(m.group(1))) if mo else None

def band(s):
    """'Rs.100 to Rs.105' -> '₹100–105'; 'Rs.214' -> '₹214'."""
    nums = re.findall(r"\d+(?:\.\d+)?", str(s or ""))
    if not nums: return str(s or "").strip()
    return "₹" + nums[0] + ("–" + nums[1] if len(nums) > 1 and nums[1] != nums[0] else "")

def to_num(v):
    try: return float(str(v).replace(",", "").strip())
    except Exception: return None

def get(jar, ep, ref):
    hdr = {"User-Agent": B.UA, "Accept": "application/json, text/plain, */*", "Referer": ref}
    j = json.loads(B._get("https://www.nseindia.com/api/" + ep, headers=hdr, jar=jar, timeout=60))
    return j.get("data") if isinstance(j, dict) else j

def main():
    jar = B.nse_jar()
    ref_up = "https://www.nseindia.com/market-data/all-upcoming-issues-ipo"
    up = cur = past = None
    try: up = get(jar, "all-upcoming-issues?category=ipo", ref_up) or []
    except Exception as e: print("upcoming FAILED:", e, flush=True)
    try: cur = get(jar, "ipo-current-issue", ref_up) or []
    except Exception as e: print("current FAILED:", e, flush=True)
    try: past = get(jar, "public-past-issues", "https://www.nseindia.com/market-data/new-stock-exchange-listings-recent") or []
    except Exception as e: print("past FAILED:", e, flush=True)
    if up is None and cur is None and past is None:
        print("ALL endpoints failed — keeping the previous file", flush=True); sys.exit(1)

    # subscription multiples from the open-issues call (category Total)
    subs = {}
    for r in (cur or []):
        if str(r.get("category") or "Total") == "Total" and r.get("symbol"):
            subs[str(r["symbol"]).upper()] = to_num(r.get("noOfTime"))

    seen, upcoming = set(), []
    for r in (up or []) + (cur or []):
        sym = str(r.get("symbol") or "").strip().upper()
        if not sym or sym in seen: continue
        seen.add(sym)
        upcoming.append([sym, str(r.get("companyName") or "").strip(),
                         "SME" if "SME" in str(r.get("series") or "").upper() else "Main",
                         iso(r.get("issueStartDate")), iso(r.get("issueEndDate")),
                         band(r.get("issuePrice")), to_num(r.get("issueSize")),
                         str(r.get("status") or "").strip(),
                         round(subs[sym], 2) if subs.get(sym) is not None else None])
    upcoming.sort(key=lambda r: (r[3] or "9999", r[0]))

    meta = {}
    try:
        raw = json.loads(gzip.decompress(open(SLIM, "rb").read())).get("meta") or {}
        meta = {(v.get("symbol") or k.split(".")[0]).upper(): v for k, v in raw.items()}
    except Exception:
        print("WARN: dash_slim.bin unreadable — listed rows carry no current price", flush=True)

    listed = []
    if past is not None:
        if len(past) < MIN_PAST:
            print("past-issues suspiciously small (%d) — keeping the previous file" % len(past), flush=True)
            sys.exit(1)
        lo = (datetime.date.today() - datetime.timedelta(days=LISTED_DAYS)).isoformat()
        for r in past:
            ld = iso(r.get("listingDate"))
            sym = str(r.get("symbol") or "").strip().upper()
            st = str(r.get("securityType") or "").strip().upper()
            # equity only: EQ + BE (new mainboard names often list in the T2T 'BE' series)
            # + SME; N0/Z9/RR/DEBT/IV are NCD/REIT/InvIT tranches, not stock listings
            if st not in ("EQ", "BE", "SME"): continue
            if not (ld and sym) or ld < lo: continue
            m = meta.get(sym) or {}
            listed.append([sym, str(r.get("company") or "").strip(),
                           "SME" if "SME" in str(r.get("securityType") or "").upper() else "Main",
                           ld, to_num(r.get("issuePrice")),
                           m.get("latest"), round(m["mcap"], 1) if m.get("mcap") else None])
        listed.sort(key=lambda r: r[3], reverse=True)

    ist = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=5, minutes=30)
    out = {"updated": ist.strftime("%Y-%m-%d %H:%M"), "upcoming": upcoming, "listed": listed}
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(out, f, separators=(",", ":"), ensure_ascii=False)
    print("Wrote %s (%.0f KB): %d upcoming/open, %d listed in %dd" %
          (OUT, os.path.getsize(OUT) / 1024.0, len(upcoming), len(listed), LISTED_DAYS), flush=True)
    for r in upcoming[:4]:
        print("  OPEN/UPCOMING %s %s %s..%s %s sub=%s" % (r[0], r[2], r[3], r[4], r[5], r[8]), flush=True)
    for r in listed[:3]:
        gain = (" %+.0f%%" % ((r[5] - r[4]) / r[4] * 100)) if (r[4] and r[5]) else ""
        print("  LISTED %s %s %s issue=%s last=%s%s" % (r[0], r[2], r[3], r[4], r[5], gain), flush=True)

if __name__ == "__main__":
    main()
