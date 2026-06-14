# -*- coding: utf-8 -*-
"""Merge Screener pre-IPO quarters into sf_fundamentals.json as standalone YoY bases.

Screener reports STANDALONE net profit (verified: matches our NSE std to the decimal).
Trust a symbol only if its OVERLAPPING (post-listing) quarters match NSE (either basis).
For trusted symbols, add the quarters NSE doesn't have (the pre-IPO ones) as [qe, npStd, None,
None, None] — std value, con/dates null so they're ONLY ever used as the year-ago YoY base,
never as a 'current' quarter (point-in-time-safe). Never overwrites an existing NSE quarter.
Atomic write. Run: python -X utf8 merge_screener.py
"""
import json, os
import build_fundamentals as bf
DOCS = os.path.join(os.path.dirname(bf.HERE), "docs", "sf_fundamentals.json")
OUT = bf.OUT
SCR = os.path.join(bf.HERE, "screener_pre.json")

def close(a, b):
    return a is not None and b is not None and abs(a - b) <= max(2, abs(a) * 0.05)

def main():
    nse = json.load(open(DOCS)); scr = json.load(open(SCR))
    added_sym = added_q = skipped = 0; rep = []
    for sym, d in scr.items():
        s_np = {int(k): v for k, v in d.get("np", {}).items()}
        if not s_np: continue
        have = {r[0]: (r[1], r[3]) for r in nse.get(sym, [])}
        overlap = [(qe, s_np[qe]) for qe in s_np if qe in have]
        if not overlap:                                   # can't validate -> skip
            skipped += 1; continue
        m = sum(1 for qe, v in overlap if close(have[qe][0], v) or close(have[qe][1], v))
        if m < len(overlap) * 0.8:                        # overlap doesn't reconcile -> don't trust
            skipped += 1; continue
        rows = {r[0]: list(r) for r in nse.get(sym, [])}
        newq = 0
        for qe, v in s_np.items():
            if qe not in rows:                            # add only the quarters NSE lacks (pre-IPO)
                rows[qe] = [qe, v, None, None, None]; newq += 1
        if newq:
            nse[sym] = [rows[k] for k in sorted(rows)]
            added_sym += 1; added_q += newq; rep.append((sym, newq))
    for path in (DOCS, OUT):
        tmp = path + ".tmp"; json.dump(nse, open(tmp, "w"), separators=(",", ":")); os.replace(tmp, path)
    print("merged pre-IPO bases into %d symbols (%d quarters); %d skipped (no/failed overlap)" % (added_sym, added_q, skipped))
    print("sample:", rep[:12])

if __name__ == "__main__":
    main()
