#!/usr/bin/env python3
"""Nightly data-health monitor (DATA_RUNBOOK.md section 18).

Reads docs/feeds.json (the feed manifest), checks every feed for:
  - missing file
  - too-small file (min_bytes)
  - broken JSON (parse check)
  - staleness: hours since the file's last commit > max_age_hours
plus the "special" infrastructure checks (release asset age, sf-data repo
age, Supabase alive), and writes docs/status.json for status.html.

Staleness source: in GitHub Actions (shallow checkout) the last-commit time
comes from the GitHub API; locally it comes from `git log`. Always exits 0 —
the feed-monitor workflow decides what to do with the result.
"""
import json, os, subprocess, sys, time, urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOCS = os.path.join(ROOT, "docs")
REPO = "dhruvan246/stocks-dashboard"
TOKEN = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN") or ""
IN_CI = os.environ.get("GITHUB_ACTIONS") == "true"
NOW = time.time()


def api(url):
    req = urllib.request.Request(url, headers={"User-Agent": "feed-monitor"})
    if TOKEN:
        req.add_header("Authorization", "Bearer " + TOKEN)
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def iso_to_epoch(s):
    return time.mktime(time.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")) - time.timezone


def last_commit_age_hours(relpath):
    """Hours since the file's last commit on main (API in CI, git locally)."""
    try:
        if IN_CI:
            c = api(f"https://api.github.com/repos/{REPO}/commits?path={relpath}&per_page=1&sha=main")
            if not c:
                return None
            return (NOW - iso_to_epoch(c[0]["commit"]["committer"]["date"])) / 3600
        out = subprocess.run(["git", "log", "-1", "--format=%ct", "--", relpath],
                             capture_output=True, text=True, cwd=ROOT).stdout.strip()
        return (NOW - int(out)) / 3600 if out else None
    except Exception as e:
        print(f"  (age lookup failed for {relpath}: {e})")
        return None


def check_feed(f):
    relpath = "docs/" + f["file"]
    path = os.path.join(DOCS, f["file"])
    r = {"file": f["file"], "name": f["name"], "workflow": f.get("workflow"),
         "cadence": f.get("cadence"), "pages": f.get("pages", []),
         "static": f.get("max_age_hours") is None,
         "status": "ok", "detail": "", "bytes": 0, "age_hours": None}
    if not os.path.exists(path):
        r.update(status="missing", detail="file does not exist")
        return r
    r["bytes"] = os.path.getsize(path)
    if r["bytes"] < f.get("min_bytes", 1):
        r.update(status="too_small", detail=f"{r['bytes']} B < expected {f['min_bytes']} B")
        return r
    if f["file"].endswith(".json"):
        try:
            with open(path, encoding="utf-8") as fh:
                json.load(fh)
        except Exception as e:
            r.update(status="parse_error", detail=str(e)[:120])
            return r
    if f.get("max_age_hours") is not None:
        age = last_commit_age_hours(relpath)
        r["age_hours"] = round(age, 1) if age is not None else None
        if age is not None and age > f["max_age_hours"]:
            r.update(status="stale",
                     detail=f"last commit {age/24:.1f} d ago (limit {f['max_age_hours']/24:.1f} d)")
    return r


def check_special(s):
    r = {"file": s["type"], "name": s["name"], "workflow": s.get("workflow"),
         "cadence": "infra", "pages": [], "static": False,
         "status": "ok", "detail": "", "bytes": None, "age_hours": None}
    try:
        if s["type"] == "release_asset":
            rel = api(f"https://api.github.com/repos/{s['repo']}/releases/tags/{s['tag']}")
            asset = next((a for a in rel.get("assets", []) if a["name"] == s["asset"]), None)
            if not asset:
                r.update(status="missing", detail=f"asset {s['asset']} not found")
                return r
            age = (NOW - iso_to_epoch(asset["updated_at"])) / 3600
            r["age_hours"], r["bytes"] = round(age, 1), asset["size"]
            if age > s["max_age_hours"]:
                r.update(status="stale", detail=f"asset last replaced {age/24:.1f} d ago")
        elif s["type"] == "repo_head":
            c = api(f"https://api.github.com/repos/{s['repo']}/commits/{s['branch']}")
            age = (NOW - iso_to_epoch(c["commit"]["committer"]["date"])) / 3600
            r["age_hours"] = round(age, 1)
            if age > s["max_age_hours"]:
                r.update(status="stale", detail=f"last push {age/24:.1f} d ago")
        elif s["type"] == "supabase_rpc":
            req = urllib.request.Request(
                s["url"], data=b"{}", method="POST",
                headers={"apikey": s["apikey"], "Authorization": "Bearer " + s["apikey"],
                         "Content-Type": "application/json", "User-Agent": "feed-monitor"})
            with urllib.request.urlopen(req, timeout=20) as resp:
                data = json.load(resp)
            if not isinstance(data, list):
                r.update(status="error", detail="unexpected response shape")
    except Exception as e:
        r.update(status="error", detail=str(e)[:120])
    return r


def main():
    with open(os.path.join(DOCS, "feeds.json"), encoding="utf-8") as fh:
        manifest = json.load(fh)
    results = [check_feed(f) for f in manifest["feeds"]]
    results += [check_special(s) for s in manifest["special"]]
    bad = [r for r in results if r["status"] != "ok"]
    status = {
        "generated_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(NOW)),
        "ok": len(bad) == 0,
        "checked": len(results),
        "bad": [r["file"] for r in bad],
        "feeds": results,
    }
    with open(os.path.join(DOCS, "status.json"), "w", encoding="utf-8") as fh:
        json.dump(status, fh, indent=1)
    for r in results:
        mark = "OK " if r["status"] == "ok" else "!! "
        age = f"{r['age_hours']:.0f}h" if r["age_hours"] is not None else "-"
        print(f"{mark}{r['status']:<11} {age:>6}  {r['file']}  {r['detail']}")
    print(f"\n{len(results) - len(bad)}/{len(results)} healthy -> docs/status.json")


if __name__ == "__main__":
    main()
