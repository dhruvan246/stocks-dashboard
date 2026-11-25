# -*- coding: utf-8 -*-
"""Point-in-time membership history for the NSE sector/theme indices OUTSIDE the 28-index pipeline.

The 28 pipeline indices are built by build_changelog.py + build_membership_v2.py (runbook §227). This script
gives the 53 indices of fetch_nse_extra_indices.py a history of their own, WITHOUT touching the pipeline's
files (_changelog.json / indices_history.json), so nothing that reads those can change.

Method (runbook §SECTORS-TRACKER):
  1. Parse every NSE Indices press release in --pr (text layer, build_changelog's parser, aliases for the 53).
  2. Walk backward from today's official list (docs/nse_extra_indices/members.json).
  3. Check the walk against every archived copy of NSE's own constituent file in --wb (Wayback captures).
  4. An index gets history only as far back as its OLDEST archived list, and only when EVERY archived list
     equals the walk and no event contradicts the roster (an inclusion that is not a member afterwards, an
     exclusion that still is). Otherwise it keeps today's list only and is named in the report.

Output: docs/nse_extra_indices/history.json
  {"built": iso, "indices": {name: {"from": iso, "snapshots": [{"effectiveDate", "symbols"}], "checks": n}},
   "unverified": {name: reason}}

Usage: python3 -X utf8 scripts/build_extra_index_history.py --pr DIR --wb DIR [--report FILE] [--write]
"""
import csv, datetime, json, os, re, sys

HERE = os.path.dirname(os.path.abspath(__file__)); ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import build_changelog as bc
import build_membership_v2 as bv
from fetch_nse_extra_indices import FILES

MEMBERS = os.path.join(ROOT, "docs", "nse_extra_indices", "members.json")
OUT = os.path.join(ROOT, "docs", "nse_extra_indices", "history.json")
EXTRA = list(FILES)
TRACKED = set(bc.CANON.values())                     # the 28 pipeline indices: parsed (so rows cannot bleed), never emitted


def norm(name):
    n = re.sub(r"[^a-z0-9]", "", name.lower())
    n = re.sub(r"index$", "", n)
    return re.sub(r"^spcnx", "cnx", n)


# heading aliases beyond the display name itself (every one READ in a notice; see the runbook section)
ALIASES = {}
for _n in EXTRA:
    bc.CANON[norm(_n)] = _n


def add_aliases(extra):
    for k, v in extra.items():
        assert v in FILES, v
        bc.CANON[k] = v


# A numbered/lettered line that starts with an index name is a heading even when the name carries characters
# build_changelog's HEAD_RE does not allow ("Nifty Financial Services 25/50", "Nifty Tata Group 25% Cap",
# "Nifty500 Multicap 50:25:25"). Unrecognised, such a line leaves the previous block open and its rows are read
# into that block. A heading never ends in a ticker-shaped token preceded by a company name, and rows carry no
# ")" / "." after the serial, so the broad form cannot swallow a row.
BROAD_HEAD = re.compile(r"^\s*\(?\s*(?:\d+|[a-zA-Z])\s*[\)\.]\s*((?:s&p\s*)?(?:nifty|cnx|lix\s*15|nv20|ni15|nse quality|gsec)[\w &\-/%\.:,\+]*?)\s*[\*#@†]*\s*(?:\([^)]*\)?)?\s*[\*#@†]*\s*$", re.I)


# A LETTERED heading that ends in one index name, whatever the wording before it: "B. Replacement on account of
# semi-annual review of Nifty India Railways PSU index:" (ind_prs12032026). build_changelog's HEAD2_RE wants
# "replacements in <index>", so SECT_RE closed the block and the rows under it were dropped. The tail from the FIRST
# "Nifty" to the end must be exactly one known index name, so a heading naming two indices stays a plain section end.
TAIL_HEAD = re.compile(r"^\s*[A-Z][\)\.]\s+[^\n]*?\b((?:nifty|cnx)[\w &\-/%]*?)\s*(?:index|indices)?\s*[:\.]?\s*$", re.I)


class Head2:
    def __init__(self, orig):
        self.orig = orig

    def match(self, ln):
        m = self.orig.match(ln)
        if m and bc.canon_index(m.group(1)):
            return m
        t = TAIL_HEAD.match(ln)
        if t and bc.canon_index(t.group(1)) and not re.search(r"\b(?:has been|have been|pursuant|on account of its)\b", ln):
            return t
        return m


def lead_index(toks):
    """longest leading run of tokens naming a known index (tracked, extra, or a sink) -> (index, tokens used)"""
    for k in range(min(len(toks), 10), 1, -1):
        ci = bc.canon_index(re.sub(r"[#*]+$", "", " ".join(toks[:k])))
        if ci:
            return ci, k
    return None, 0


def stem_key(stem):                                   # DDMMYYYY[_n] -> sortable
    return (stem[4:8], stem[2:4], stem[:2], stem)


def read_notices(pr_dir, txt_dir=None):
    """{stem: text} for every notice (cached text layer in txt_dir when present)"""
    from pypdf import PdfReader
    out = {}
    for f in sorted(os.listdir(pr_dir)):
        if not f.endswith(".pdf"):
            continue
        stem = f[:-4]
        tp = os.path.join(txt_dir, stem + ".txt") if txt_dir else None
        if tp and os.path.exists(tp):
            out[stem] = open(tp, encoding="utf-8").read(); continue
        try:
            t = "\n".join(p.extract_text() or "" for p in PdfReader(os.path.join(pr_dir, f)).pages)
        except Exception:
            t = ""
        if tp:
            open(tp, "w", encoding="utf-8").write(t)
        out[stem] = t
    return out


def all_headings(texts):
    """every numbered index heading in the corpus -> {normalised: (sample text, count)}"""
    seen = {}
    for stem, t in texts.items():
        for ln in t.splitlines():
            m = BROAD_HEAD.match(ln)
            if m:
                k = norm(m.group(1))
                s = seen.setdefault(k, [m.group(1).strip(), 0, stem]); s[1] += 1
    return seen


SINGLE_TITLE = re.compile(r"^(?:Replacements?|Inclusions?|Exclusions?)\s+(?:in|from|to)\s+(Nifty[\w &\-]*?)\s+index\s*$", re.I)


def single_index_notice(txt):
    """A notice about ONE index names it in the title ("Inclusion in Nifty IPO index", "Exclusion from Nifty Waves index")
    and has no numbered index heading. Give it the heading the parser needs, in front of its first table. Only for the
    indices this builder owns; anything else is returned unchanged."""
    lines = txt.splitlines()
    solid = [(i, l.strip()) for i, l in enumerate(lines) if l.strip()]
    title = next((SINGLE_TITLE.match(l) for i, l in solid[:5] if SINGLE_TITLE.match(l)), None)
    if not title or bc.canon_index(title.group(1)) not in FILES:
        return txt
    if any(BROAD_HEAD.match(l) for l in lines):
        return txt
    for i, l in solid:
        if re.match(r"^The following compan(?:y|ies) (?:is|are) being (?:in|ex)cluded", l, re.I):
            return "\n".join(lines[:i] + ["1) " + title.group(1)] + lines[i:])
    return txt


def parse_all(texts, broad=True):
    """-> (changelog {index: [event]}, revocations [(stem, [(idx, eff, act, sym)])], rescheduled)"""
    old_head, old_lead, old_h2 = bc.HEAD_RE, bc._lead_index, bc.HEAD2_RE
    if broad:
        bc.HEAD_RE = BROAD_HEAD
        bc.HEAD2_RE = Head2(old_h2)
    bc._lead_index = lead_index
    changelog = {}; revocations = []; rescheduled = []
    try:
        for stem in sorted(texts, key=stem_key):
            txt = texts[stem]
            side = os.path.join(bc.OCR_DIR, stem + ".txt")
            if len(txt.strip()) < 400:                # image-only notice: the committed OCR sidecar, or nothing
                blocks = bc.parse_text(open(side, encoding="utf-8").read(), bare_heads=True) if os.path.exists(side) else []
            else:
                blocks = bc.parse_text(single_index_notice(txt))
            revs = bc.parse_revocations(txt) + bc.parse_symbol_lists(txt)
            rs = bc.RESCHED_OVERRIDES.get(stem) or bc.resched_dates(txt)
            if stem in bc.RESCHED_OVERRIDES:
                for b in blocks:
                    b["eff"] = bc.RESCHED_OVERRIDES[stem][1]
            if revs:
                revocations.append((stem, revs))
            if rs and blocks:
                rescheduled.append((stem, rs[0], blocks))
            for b in blocks:
                changelog.setdefault(b["index"], []).append(
                    {"eff": b["eff"], "excluded": b["excluded"], "included": b["included"], "src": stem})
    finally:
        bc.HEAD_RE, bc._lead_index, bc.HEAD2_RE = old_head, old_lead, old_h2
    return changelog, revocations, rescheduled


from extra_index_rules import file_key as R_file_key, DUMMY_REAL


def load_captures(wb_dir, file_index):
    """{index: {iso date: (set(symbols), capture file)}} from <filekey>__<YYYYMMDDhhmmss>.csv"""
    caps = {}; unmapped = {}
    for f in sorted(os.listdir(wb_dir)):
        if not f.endswith(".csv"):
            continue
        key, ts = f[:-4].split("__")
        idx = file_index.get(R_file_key(key))
        if not idx:
            unmapped[key] = unmapped.get(key, 0) + 1; continue
        rows = list(csv.reader(open(os.path.join(wb_dir, f), encoding="utf-8-sig", errors="ignore").read().splitlines()))
        if not rows:
            continue
        head = [h.strip() for h in rows[0]]
        if "Symbol" not in head:
            continue
        si = head.index("Symbol")
        syms = {r[si].strip().upper() for r in rows[1:] if len(r) > si and r[si].strip()}
        if len(syms) < 3:
            continue
        d = "%s-%s-%s" % (ts[:4], ts[4:6], ts[6:8])
        caps.setdefault(idx, {})[d] = (syms, f)
    return caps, unmapped


ISIN_ALIAS = {}      # filled by load_isin_aliases(): symbol -> today's symbol, where NSE's own files show one ISIN under both


def load_isin_aliases(dirs, today_dir):
    """Renames the rename map lacks, PROVEN by NSE's list files: two symbols that carry the same ISIN are one security.
    The representative is the symbol in today's files (today_dir), else the one seen in the latest file."""
    seen = {}                                          # isin -> {symbol(after bv.canon): latest stamp}
    for d in dirs:
        for f in sorted(os.listdir(d)):
            if not f.endswith(".csv"):
                continue
            stamp = "99999999" if d == today_dir else f[:-4].split("__")[-1][:8]
            rows = list(csv.reader(open(os.path.join(d, f), encoding="utf-8-sig", errors="ignore").read().splitlines()))
            if not rows:
                continue
            head = [h.strip() for h in rows[0]]
            ic = [i for i, h in enumerate(head) if "ISIN" in h.upper()]
            if "Symbol" not in head or not ic:
                continue
            si = head.index("Symbol")
            for r in rows[1:]:
                if len(r) > max(si, ic[0]):
                    sym = r[si].strip().upper(); isin = r[ic[0]].strip().upper()
                    if sym and len(isin) == 12 and isin.startswith("IN") and not sym.startswith("DUMMY"):
                        c = bv.canon(sym); m = seen.setdefault(isin, {})
                        m[c] = max(m.get(c, ""), stamp)
    out = {}
    for isin, m in seen.items():
        if len(m) > 1:
            rep = max(m, key=lambda k: m[k])
            for k in m:
                if k != rep:
                    out[k] = rep; print("  ISIN RENAME %s -> %s (%s)" % (k, rep, isin))
    return out


def canon(s):
    """one identity per company: demerger placeholder -> listed symbol, NSE's rename chain, then ISIN-proven renames"""
    c = bv.canon(DUMMY_REAL.get(s, s))
    return ISIN_ALIAS.get(c, c)


def walk(anchor, events, today):
    """backward walk -> (snaps [(eff, set)] ascending incl. baseline '0000-00-00', flags [str])"""
    ev = bv.merge_same_eff(events)
    m = {canon(s) for s in anchor}
    snaps = []; flags = []
    for c in reversed(ev):
        inc = {canon(x) for x in c["included"]}; exc = {canon(x) for x in c["excluded"]}
        if c["eff"] > today:
            snaps.append((c["eff"], (m | inc) - exc)); continue
        for s in sorted(inc - m):
            flags.append("%s: %s included but not a member afterwards" % (c["eff"], s))
        for s in sorted(exc & m - inc):
            flags.append("%s: %s excluded but still a member afterwards" % (c["eff"], s))
        snaps.append((c["eff"], set(m)))
        m = (m - inc) | exc
    snaps.append(("0000-00-00", set(m)))
    snaps.sort(key=lambda x: x[0])
    return snaps, flags


def state_at(snaps, d):
    best = snaps[0][1]
    for eff, s in snaps:
        if eff <= d:
            best = s
        else:
            break
    return best


def main():
    a = sys.argv
    pr = a[a.index("--pr") + 1]; wb = a[a.index("--wb") + 1]
    txt_dir = a[a.index("--txt") + 1] if "--txt" in a else None
    report_fp = a[a.index("--report") + 1] if "--report" in a else None
    today = datetime.date.today().isoformat()
    import extra_index_rules as R                     # aliases, sinks, file names, hand-read events (kept apart: data, not code)
    add_aliases(R.ALIASES)
    texts = read_notices(pr, txt_dir)
    heads = all_headings(texts)
    for k, (sample, n, stem) in heads.items():        # every other index heading is a SINK: it ends a block, emits nothing
        if k not in bc.CANON:
            bc.CANON[k] = "~" + sample
    cur = a[a.index("--cur") + 1] if "--cur" in a else None
    ISIN_ALIAS.update(load_isin_aliases([wb] + ([cur] if cur else []), cur))
    changelog, revocations, rescheduled = parse_all(texts)
    R.post_process(changelog, revocations, rescheduled, bc, texts)
    members = json.load(open(MEMBERS, encoding="utf-8"))["indices"]
    caps, unmapped = load_captures(wb, R.FILE_INDEX)
    rep = {"notices": len(texts), "unmapped_capture_files": unmapped, "indices": {}}
    out = {}; unverified = {}
    for name in EXTRA:
        evs = sorted(changelog.get(name, []), key=lambda c: c["eff"])
        for c in evs:
            c["excluded"] = list(dict.fromkeys(c["excluded"])); c["included"] = list(dict.fromkeys(c["included"]))
        snaps, flags = walk(members[name]["symbols"], evs, today)
        checks = []
        for d in sorted(caps.get(name, {})):
            syms, f = caps[name][d]
            got = {canon(s) for s in syms}; want = state_at(snaps, d)
            ck = {"date": d, "file": f, "n": len(got), "ok": got == want,
                  "walk_only": sorted(want - got), "capture_only": sorted(got - want)}
            if not ck["ok"]:
                # NSE's file was sometimes a STALE copy when archived (e.g. the launch list still served a year later).
                # If it equals, name for name, the list we reconstruct for an EARLIER period, it proves that earlier list:
                # every change after it was needed to walk back from today's list to it. Credited to the last day of that period.
                for i, (eff, st) in enumerate(snaps):
                    if eff <= d and st == got and i + 1 < len(snaps) and snaps[i + 1][0] <= d:
                        end = (datetime.date.fromisoformat(snaps[i + 1][0]) - datetime.timedelta(days=1)).isoformat()
                        ck.update(ok=True, stale=True, captured=d, date=end, walk_only=[], capture_only=[])
            checks.append(ck)
        checks.sort(key=lambda c: c["date"])
        bad = [c for c in checks if not c["ok"]]
        r = {"events": len(evs), "first_event": evs[0]["eff"] if evs else None, "flags": flags,
             "checks": len(checks), "bad": bad, "all_checks": [{k: c[k] for k in ("date", "file", "n", "ok", "stale", "captured") if k in c} for c in checks], "first_check": checks[0]["date"] if checks else None,
             "sizes": [(e, len(s)) for e, s in snaps]}
        rep["indices"][name] = r
        if checks:                                     # a contradiction older than the first check is outside the history we ship
            flags = [f for f in flags if f[:10] >= checks[0]["date"]]; r["flags"] = flags
        if not checks:
            unverified[name] = "no archived NSE list to check against"
        elif bad or flags:
            unverified[name] = "%d of %d archived lists differ, %d roster contradictions" % (len(bad), len(checks), len(flags))
        else:
            start = checks[0]["date"]
            keep = [(e, s) for e, s in snaps if e > start]
            out[name] = {"from": start, "checks": len(checks),
                         "snapshots": [{"effectiveDate": start, "symbols": sorted(state_at(snaps, start))}] +
                                      [{"effectiveDate": e, "symbols": sorted(s)} for e, s in keep]}
    for k, v in getattr(R, "UNVERIFIED_NOTES", {}).items():
        if k in unverified:
            unverified[k] = v
    rep["verified"] = sorted(out); rep["unverified"] = unverified
    if report_fp:
        json.dump({"report": rep, "changelog": {k: v for k, v in changelog.items() if k in FILES}},
                  open(report_fp, "w"), indent=1)
    print("%d notices; %d of %d indices verified, %d not" % (len(texts), len(out), len(EXTRA), len(unverified)))
    for n in EXTRA:
        r = rep["indices"][n]
        print("  %-42s ev %3d  checks %2d  bad %2d  flags %2d  %s" % (n, r["events"], r["checks"], len(r["bad"]), len(r["flags"]),
                                                                    ("from " + out[n]["from"]) if n in out else "NOT VERIFIED"))
    if "--write" in a:
        doc = {"built": today, "source": "NSE Indices press releases, walked back from today's list and checked against "
                                         "archived NSE constituent files", "indices": out, "unverified": unverified}
        with open(OUT, "w", encoding="utf-8") as f:
            json.dump(doc, f, ensure_ascii=False, separators=(",", ":")); f.write("\n")
        print("wrote", OUT)


if __name__ == "__main__":
    main()
