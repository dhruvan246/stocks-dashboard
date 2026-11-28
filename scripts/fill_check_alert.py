#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Morning alert for the results-season fill checks (runbook §251).

Reads docs/xcheck.json (checks fill_missing, fill_vision, fund_new_results — written by scripts/xcheck_run.py) and
docs/_season_coverage.json (index members whose filed result is not parsed — until now only printed in the nightly
log), and keeps ONE GitHub issue titled "Results fill check" in step with them:

  newest quarter has listed cells  -> open the issue, or comment on the open one
  nothing listed                    -> close it

Cadence follows scripts/season_state.py: IN season a comment is added every morning; OFF season only when the list
changed since the last comment (a fingerprint of the listed keys is kept in the comment), so a quiet list does not
mail the same text daily. The issue stays open until the list is empty either way.

Report-only: it never edits a stored value.

  python3 scripts/fill_check_alert.py --dry-run        # print what it would do and the issue text; no GitHub call
  python3 scripts/fill_check_alert.py                  # CI (needs GH_TOKEN and `gh`)"""
import argparse, hashlib, json, os, re, subprocess, sys

HERE = os.path.dirname(os.path.abspath(__file__)); ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)
TITLE = "Results fill check"
PAGE = "https://dhruvan246.github.io/stocks-dashboard/data-checks.html"
TOP = 25
LABEL = {
    "declared_no_numbers": "Result filed, no numbers anywhere",
    "declared_pdf_only": "Result filed, main store empty (figures only in the PDF read)",
    "period_unread": "Result filed, its period could not be read",
    "one_basis": "Only one of standalone / consolidated stored (previous period had both)",
    "no_revenue": "Profit stored, revenue missing",
    "no_op": "Profit stored, operating profit missing (previous period had it)",
    "prev_not_this": "Previous period held, this one empty, no filing seen in the feed",
    "pat": "Profit differs from the PDF read",
    "unit_store": "Power-of-ten gap and the STORED figure is out of line with its previous quarter",
    "rev": "Revenue differs from the PDF read",
    "op": "Operating profit differs from the PDF read",
    "fin_definition": "Bank / NBFC / insurer revenue or operating profit (definition differs) — unread",
    "unit": "Power-of-ten gap, stored figure in line with its previous quarter (PDF read likely in another unit) — unread",
    "ytd_copy": "Profit = year-to-date figure stored as the quarter",
    "pbt_tax+eps": "Profit fails the filing's PBT − tax and its EPS",
    "pbt_tax+reader": "Profit fails the filing's PBT − tax and another reading disagrees",
    "other_basis": "Consolidated profit = the filing's standalone arithmetic",
}
ORDER = list(LABEL)


def n2(v):
    return "—" if v is None else ("%.2f" % v if isinstance(v, (int, float)) else str(v))


def line(check, f):
    b = (" " + {"s": "standalone", "c": "consolidated"}.get(f.get("basis"), "")) if f.get("basis") else ""
    idx = " _(%s)_" % f["index"] if f.get("index") else ""
    if check == "fill_vision":
        s = "stored %s vs PDF read %s" % (n2(f.get("a")), n2(f.get("b")))
        if f.get("supports"): s += "; PBT − tax %s supports: %s" % (n2(f.get("pbt_less_tax")), f["supports"])
    elif check == "fund_new_results":
        s = "stored %s vs PBT − tax %s" % (n2(f.get("pat")), n2(f.get("pbt_less_tax")))
        if f.get("other_reading") is not None: s += "; other reading %s" % n2(f["other_reading"])
    else:
        bits = []
        if f.get("declared"): bits.append("filed " + f["declared"])
        if f.get("pdf_read"): bits.append("PDF read: " + f["pdf_read"])
        if f.get("filer") == "half-yearly": bits.append("half-yearly filer")
        if f.get("prev_qe"): bits.append("holds %s" % f["prev_qe"])
        if f.get("after"): bits.append(f["after"])
        s = "; ".join(bits)
    return "- **%s**%s%s — %s" % (f["sym"], b, idx, s)


def build(rep, cov, season):
    """(n_newest, fingerprint, markdown, summary dict) from the report."""
    ch = rep.get("checks") or {}
    groups = {}                                   # (qe, class) -> [(check, finding)]
    for cid in ("fill_missing", "fill_vision", "fund_new_results"):
        c = ch.get(cid) or {}
        for f in (c.get("open") or []) + (c.get("explained") or []):
            groups.setdefault((f.get("qe"), f.get("cls") or f.get("field")), []).append((cid, f))
    qes = sorted({q for q, _ in groups if q}, reverse=True)
    fm = (ch.get("fill_missing") or {}).get("notes") or []
    m = re.search(r"quarters checked: (\d{4}-\d\d-\d\d)", " ".join(fm))
    newest = m.group(1) if m else (qes[0] if qes else None)
    n_new = sum(len(v) for (q, _), v in groups.items() if q == newest)
    keys = sorted(f["key"] + "@" + cid for (q, _), v in groups.items() if q == newest for cid, f in v)
    fp = hashlib.sha1("\n".join(keys).encode()).hexdigest()[:12]
    out = ["Built %s · results season: **%s** (%s)" % (rep.get("generated", "?"), season.get("state", "?").upper(), season.get("reason", "")),
           "", "**%d** cell(s) listed for the newest quarter **%s**. Each one is a candidate until its own filing is read — nothing was changed." % (n_new, newest),
           "Full lists, newest first: %s (checks 1–3). Triage steps: `scripts/DATA_RUNBOOK.md` §251." % PAGE, ""]
    counts = {}
    # index members the season chart is missing (was only a line in the nightly log)
    for blk, lab in ((cov, "live quarter"), ((cov or {}).get("prev") or {}, "previous quarter")):
        ix = (blk or {}).get("indexes") or {}; miss = {}
        for name, v in ix.items():
            for s in v.get("missing") or []: miss.setdefault(s, []).append(name)
        if miss:
            q = str(blk.get("qe")); q = "%s-%s-%s" % (q[:4], q[4:6], q[6:8])
            out += ["### Index members with a filed result that is not in the main store — %s %s (%d)" % (lab, q, len(miss)), ""]
            out += ["- **%s** — %s" % (s, ", ".join(sorted(miss[s])[:6]) + (" …" if len(miss[s]) > 6 else "")) for s in sorted(miss)]
            out.append("")
            counts["index_gap|" + q] = len(miss)
    for q in qes:
        out += ["## Quarter %s%s" % (q, " (newest)" if q == newest else ""), ""]
        for cls in ORDER:
            v = groups.get((q, cls))
            if not v: continue
            counts["%s|%s" % (cls, q)] = len(v)
            out += ["### %s — %d" % (LABEL[cls], len(v)), ""]
            out += [line(cid, f) for cid, f in v[:TOP]]
            if len(v) > TOP: out.append("- … and %d more on the page" % (len(v) - TOP))
            out.append("")
    out += ["_This issue closes itself the morning the newest quarter's list is empty._", "", "<!-- fill-fp:%s -->" % fp]
    md = "\n".join(out)
    if len(md) > 60000: md = md[:59000] + "\n\n… (cut — see the page)\n\n<!-- fill-fp:%s -->" % fp
    return n_new, fp, md, {"newest": newest, "counts": counts}


def gh(*a, check=True):
    r = subprocess.run(["gh"] + list(a), capture_output=True, text=True)
    if check and r.returncode: raise SystemExit("gh %s failed: %s" % (" ".join(a[:2]), r.stderr.strip()[:300]))
    return r.stdout


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--report", default=os.path.join(ROOT, "docs", "xcheck.json"))
    ap.add_argument("--out", default="/tmp/fill_alert.md")
    ap.add_argument("--force-state", choices=["in", "off"])
    a = ap.parse_args()
    rep = json.load(open(a.report, encoding="utf-8"))
    try: cov = json.load(open(os.path.join(ROOT, "docs", "_season_coverage.json"), encoding="utf-8"))
    except Exception: cov = {}
    try:
        import season_state
        season = season_state.evaluate(force=a.force_state)
    except Exception as e:                        # fail towards the louder cadence
        season = {"state": "in", "reason": "season_state failed (%s) — treated as in season" % e}
    n, fp, md, summ = build(rep, cov, season)
    open(a.out, "w", encoding="utf-8").write(md)
    print("newest quarter %s: %d listed · fingerprint %s · season %s" % (summ["newest"], n, fp, season["state"]))
    for k, v in sorted(summ["counts"].items(), key=lambda kv: (kv[0].split("|")[1], kv[0]), reverse=True): print("   %-36s %d" % (k, v))
    title = "⚠️ %s — %d open for %s" % (TITLE, n, summ["newest"])
    if a.dry_run:
        print("DRY RUN: would %s (title: %s); text in %s" % ("open or comment on the issue" if n else "close the issue if one is open", title, a.out))
        return
    num = gh("issue", "list", "--state", "open", "--search", TITLE + " in:title", "--json", "number", "--jq", ".[0].number", check=False).strip()
    if n == 0:
        if num: gh("issue", "close", num, "--comment", "Newest quarter's list is empty — closing. ✅"); print("closed #" + num)
        else: print("clean — no issue to manage")
        return
    if not num:
        print(gh("issue", "create", "--title", title, "--body-file", a.out).strip()); return
    last = gh("issue", "view", num, "--json", "body,comments", "--jq", '[.body] + [.comments[].body] | join("\\n")', check=False)
    seen = re.findall(r"<!-- fill-fp:([0-9a-f]+) -->", last)
    if season["state"] == "off" and seen and seen[-1] == fp:
        print("off season and the list is unchanged since the last comment — issue #%s left as is" % num); return
    gh("issue", "edit", num, "--title", title)
    gh("issue", "comment", num, "--body-file", a.out); print("commented on #" + num)


if __name__ == "__main__":
    main()
