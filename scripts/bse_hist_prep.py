# -*- coding: utf-8 -*-
"""PREP stage for the BSE historical-fundamentals routine (deepen bse_fundamentals.json toward 2020).

Mirrors bse_vision_prep.py but walks OLDER filings instead of the current quarter. For BSE-only names
that already have (recent) numbers, biggest-mcap-first, it fetches the result filings just BELOW each
scrip's oldest stored quarter (a <=360-day window — BSE's API returns empty for wider ranges), renders
the P&L pages to PNGs, and writes a manifest the cloud routine's subagents READ (Claude vision; OCR
fails on these scanned microcap P&Ls). merge_bse_hist.py then applies what they read and advances the
ledger. One filing prints ~3-4 comparative columns, so history fills in ~3 windows per scrip.

Resumable via scripts/_bse_fund_hist.json {code:{oldest,fails,done,frm}} — the window walks down each
run (merge sets `oldest` to the deepest quarter that landed, or skips an empty window). Bounded per run.

Run: python -X utf8 scripts/bse_hist_prep.py [--budget N] [--max-filings K] [--floor YYYYMMDD]
     [--min-mcap CR] [--outdir DIR]   →  writes <outdir>/manifest.json + <outdir>/*.png
"""
import os, sys, json, datetime, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fetch_bse_fund as bf
import backfill_bse_fund_history as bk
import bse_fetch as B
import fitz

HERE = os.path.dirname(os.path.abspath(__file__))
HIST = os.path.join(HERE, "_bse_fund_hist.json")
SCRIP = os.path.join(HERE, "bse_scrips.json")


def main():
    def argv(flag, cast, default):
        return cast(sys.argv[sys.argv.index(flag) + 1]) if flag in sys.argv else default
    budget = argv("--budget", int, 40)
    max_filings = argv("--max-filings", int, 3)
    floor = argv("--floor", int, 20200101)
    min_mcap = argv("--min-mcap", float, 0.0)
    outdir = argv("--outdir", str, "/tmp/bse_hist")
    only = set(sys.argv[sys.argv.index("--scrips") + 1].split(",")) if "--scrips" in sys.argv else None
    os.makedirs(outdir, exist_ok=True)
    today_i = int(datetime.date.today().strftime("%Y%m%d"))
    floor_d = datetime.date(floor // 10000, floor // 100 % 100, floor % 100)

    univ = json.load(open(bf.UNIV, encoding="utf-8"))["rows"]
    univ.sort(key=lambda r: r[6] or 0, reverse=True)
    data = json.loads(open(bf.OUT, encoding="utf-8").read()).get("px", {}) if os.path.exists(bf.OUT) else {}
    hist = json.load(open(HIST)) if os.path.exists(HIST) else {}
    code2sym = {str(v): k for k, v in json.load(open(SCRIP))["by_id"].items()}
    op = B.session(); time.sleep(1)

    manifest, spent = [], 0
    for r in univ:
        code = str(r[0]); mc = r[6] or 0
        if only is not None and code not in only:
            continue
        if (hist.get(code) or {}).get("done") or (only is None and mc < min_mcap):
            continue
        cur = data.get(code) or {}
        if not cur:                                         # only deepen names that already have data
            continue
        stored = [int(q) for q in cur if str(q).isdigit() and floor <= int(q) <= today_i]
        if not stored:
            continue
        oldest = (hist.get(code) or {}).get("oldest") or min(stored)
        if oldest <= floor:
            hist[code] = {"oldest": oldest, "fails": 0, "done": True}
            continue
        if spent >= budget:
            break
        spent += 1
        od = datetime.date(oldest // 10000, oldest // 100 % 100, oldest % 100)
        to_ymd = (od - datetime.timedelta(days=1)).strftime("%Y%m%d")
        frm_d = max(floor_d, od - datetime.timedelta(days=360))
        frm_ymd = frm_d.strftime("%Y%m%d")
        try:
            filings = bk.result_filings(op, code, frm_ymd, to_ymd)
        except Exception as ex:
            print("  %s ANN ERR %s" % (code, str(ex)[:40])); filings = []
        for annd, att, hd, _sc in filings[:max_filings]:
            raw = bf.fetch_pdf(op, att)
            if not raw:
                continue
            try:
                doc = fitz.open(stream=raw, filetype="pdf")
            except Exception:
                continue
            pngs = bk.render_pl_pngs(doc)
            if not pngs:
                continue
            paths = []
            for i, p in enumerate(pngs):
                fn = os.path.join(outdir, "%s_%s_%d.png" % (code, annd.replace("-", ""), i))
                open(fn, "wb").write(p); paths.append(fn)
            manifest.append({"sym": code2sym.get(code, code), "scrip": int(code), "name": r[2] or code,
                             "ann": annd, "floor": floor, "oldest": oldest, "frm": int(frm_ymd), "pngs": paths})
        hist[code] = {"oldest": oldest, "fails": (hist.get(code) or {}).get("fails", 0), "done": False,
                      "frm": int(frm_ymd)}
        time.sleep(0.1)

    json.dump(manifest, open(os.path.join(outdir, "manifest.json"), "w"), ensure_ascii=False)
    json.dump(hist, open(HIST, "w"))
    print("prep: %d scrips, %d filings rendered → %s/manifest.json" % (spent, len(manifest), outdir))


if __name__ == "__main__":
    main()
