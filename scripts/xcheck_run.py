#!/usr/bin/env python3
"""Second-reader audit runner (runbook §214). Runs every check in scripts/xcheck/ (or the ones named) against the
live stores and writes docs/xcheck.json, the data behind docs/data-checks.html.

  python3 scripts/xcheck_run.py                 # all checks -> docs/xcheck.json
  python3 scripts/xcheck_run.py px_nse_yahoo    # one check (merged into the existing report)
  python3 scripts/xcheck_run.py --out /tmp/x.json px_nse_yahoo

Findings are REPORTED, never auto-fixed: a real defect is healed through its own ledger (DATA_RUNBOOK rule 5);
a disagreement adjudicated as not-a-defect goes into scripts/xcheck_accept.json {check: {key: {why, ts}}}."""
import datetime, json, os, sys, time, traceback
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from xcheck import common as C
from xcheck.px_nse_yahoo import PxNseYahoo
from xcheck.fund import FundVisionXbrl, FundPbtTax, FundEpsShares
from xcheck.market import IdxLevels, FlowsMonth, McapShares

def ist(fmt="%Y-%m-%d %H:%M"):
    """IST wall time from UTC — a CI runner's local clock is UTC (the first run stamped 04:28 'IST' at 09:58 IST)."""
    return (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=5, minutes=30)).strftime(fmt)


CHECKS = [PxNseYahoo, FundVisionXbrl, FundPbtTax, FundEpsShares, McapShares, IdxLevels, FlowsMonth]


def main(argv):
    out = os.path.join(C.DOCS, "xcheck.json")
    if "--out" in argv:
        i = argv.index("--out"); out = argv[i + 1]; argv = argv[:i] + argv[i + 2:]
    want = set(argv)
    rep = {}
    if want and os.path.exists(out):
        try: rep = json.load(open(out, encoding="utf-8"))
        except ValueError: rep = {}
    rep.setdefault("checks", {})
    accept = C.load_accept(); failed = []
    for K in CHECKS:
        if want and K.id not in want: continue
        t0 = time.time(); k = K()
        try:
            k.run(); r = k.report(accept); r["secs"] = round(time.time() - t0, 1)
            r["ran"] = ist()
            rep["checks"][K.id] = r
            print("%-18s compared %9d  agree %7s%%  open %5d  explained %5d  accepted %4d  (%.0fs)" % (
                K.id, r["compared"], r["agree_pct"], r["n_open"], r["n_explained"], r["n_accepted"], r["secs"]), flush=True)
        except Exception:
            failed.append(K.id); traceback.print_exc()
            rep["checks"][K.id] = dict((rep["checks"].get(K.id) or {}), error=traceback.format_exc()[-600:],
                                       error_ran=ist())
    rep["generated"] = ist() + " IST"
    rep["order"] = [K.id for K in CHECKS]
    tmp = out + ".part"
    json.dump(rep, open(tmp, "w", encoding="utf-8"), ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, out)
    print("wrote %s (%d KB)" % (out, os.path.getsize(out) // 1024))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
