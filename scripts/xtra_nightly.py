# -*- coding: utf-8 -*-
"""Nightly top-up for the deep-fundamentals ledger (Phase 2b of the stock-page upgrade).

Keeps scripts/xbrl_extra.json.gz current with each day's filings. Three steps:
  1. FETCH: list the last WINDOW_DAYS of quarterly filings from NSE's integrated-filing
     endpoint (same transport ladder as update_fundamentals.py: urllib → curl_cffi
     Chrome-TLS → the project's Cloudflare Worker) and download any XBRL not yet in the
     big local cache. Cache filenames use build_fundamentals' canonical form (URL
     basename, non-alphanumerics → "_") so every consumer sees one name per filing.
  2. EXTRACT: build_xbrl_extra.py --incremental (exact seen-set difference).
  3. PUBLISH (--push): re-gzip the ledger and commit/push xbrl_extra.json.gz, which
     push-triggers refresh-stock-fin.yml → fresh fin slices minutes later.

CLOUD (xbrl-extra-nightly.yml, the primary runner since 2026-08-05): CI has no 80-GB
cache, so it keeps a ROLLING one — only the window's filings, restored from the Actions
cache — and pushes the .gz from the workflow (this script's --push refuses there, see
below). Three flags exist for that mode:
  --seed-seen         write the seen-set from the current cache listing when it's missing.
                      A cold CI cache is EMPTY, so the seed is empty and every file
                      downloaded that night gets extracted — correct, not a silent skip.
  --max-fetch N       stop downloading after N new files. A cold start (whole window
                      missing) then spreads over a few nights instead of blowing the job
                      timeout; the window overlap re-offers the rest.
  --prune-cache-days N  after a successful extract, drop cache files older than N days —
                      they can never be re-offered by an N>WINDOW_DAYS-day list. Refuses on
                      a cache that looks like the big local one (see PRUNE_MAX_FILES).
Replaying a window file is harmless: the window only ever holds the NEWEST filings, and the
ledger merges per-field non-null latest-wins, so a re-extract can only re-assert.

CONCURRENCY (CLAUDE.md): locally this script runs from its OWN worktree
(C:/Users/dhruv/stocks-wt/xbrl-extra) with XBRL_CACHE pointing at the main checkout's
scripts/_xbrl_cache (append-only writes there are safe). --push REFUSES to run from the
shared interactive checkout — tree-wide git ops are only legal in a worktree it owns.
The seen-set + raw ledger live in THIS tree's scripts/ (gitignored via scripts/_* and
.gitignore's xbrl_extra.json line); only the .gz is committed.

A failed NSE night self-heals: the WINDOW_DAYS overlap re-offers missed filings the
next night, and the seen-set ensures each file is extracted exactly once.

Run:  python -X utf8 scripts/xtra_nightly.py [--push] [--window N]
      [--seed-seen] [--max-fetch N] [--prune-cache-days N]
"""
import os, re, sys, json, gzip, time, subprocess, datetime

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)
import build_fundamentals as B          # _get / nse_jar / UA — the CI-proven NSE session

CACHE = os.environ.get("XBRL_CACHE") or os.path.join(HERE, "_xbrl_cache")
LEDGER = os.path.join(HERE, "xbrl_extra.json")
SEEN = os.path.join(HERE, "_xtra_seen.json")
GZ = LEDGER + ".gz"
SHARED_CHECKOUT = "stocks-dashboard"    # basename of the interactive tree --push must refuse

WINDOW_DAYS = 14
# --prune-cache-days is meant for CI's rolling window cache (~4k files). The local cache holds
# ~100k+ irreplaceable filings back to 2018, so refuse to prune anything that big — a stray flag
# must never eat it.
PRUNE_MAX_FILES = 20000
WORKER = "https://stocksworld-quotes.dhruvan2510.workers.dev"


def canon(url):
    """Cache filename for an XBRL url — mirrors build_fundamentals (one name per filing)."""
    return re.sub(r"[^A-Za-z0-9]", "_", url.rsplit("/", 1)[-1])


def warm_jar():
    j = B.nse_jar()
    try:
        B._get("https://www.nseindia.com/companies-listing/corporate-filings-financial-results",
               headers={"User-Agent": B.UA, "Accept": "text/html,application/xhtml+xml"},
               jar=j, timeout=30)
    except Exception as e:
        print("  (warm page visit failed: %s)" % str(e)[:60])
    return j


_cffi = {"s": None}


def cffi_get(url, h):
    if _cffi["s"] is None:
        from curl_cffi import requests as cr
        s = cr.Session(impersonate="chrome")
        s.get("https://www.nseindia.com/", timeout=30)
        s.get("https://www.nseindia.com/companies-listing/corporate-filings-financial-results", timeout=30)
        _cffi["s"] = s
    r = _cffi["s"].get(url, headers=h, timeout=150)
    if r.status_code != 200:
        _cffi["s"] = None
        raise RuntimeError("curl_cffi HTTP %d" % r.status_code)
    return r.text


def list_filings(window_days):
    """Rows from the integrated-filing list for the window, both boards. Empty mainboard
    list raises — a silent no-op would look like a quiet night during results season."""
    today = datetime.date.today()
    frm = (today - datetime.timedelta(days=window_days)).strftime("%d-%m-%Y")
    to = today.strftime("%d-%m-%Y")
    h = {"User-Agent": B.UA, "Accept": "application/json",
         "Referer": "https://www.nseindia.com/companies-listing/corporate-filings-financial-results"}
    rows = []
    for idx in ("equities", "sme"):
        jar = warm_jar()
        base = ("https://www.nseindia.com/api/integrated-filing-results?index=%s&period=Quarterly"
                "&from_date=%s&to_date=%s" % (idx, frm, to))
        got, page, total = 0, 1, 0
        while True:
            url = base + "&page=%d&size=200" % page
            jb = None
            for attempt, transport in enumerate(("urllib", "urllib", "cffi", "worker")):
                if attempt:
                    time.sleep(4 * attempt)
                try:
                    if transport == "urllib":
                        if attempt:
                            jar = warm_jar()
                        body = B._get(url, headers=h, jar=jar, timeout=150)
                    elif transport == "cffi":
                        body = cffi_get(url, h)
                    else:
                        import urllib.request
                        u = "%s/?filings=%s&from=%s&to=%s&page=%d&size=200" % (WORKER, idx, frm, to, page)
                        body = urllib.request.urlopen(
                            urllib.request.Request(u, headers={"User-Agent": B.UA}), timeout=150
                        ).read().decode("utf-8", "replace")
                    parsed = json.loads(body)
                    if isinstance(parsed, dict) and parsed.get("error"):
                        raise RuntimeError("worker: %s" % parsed["error"])
                    jb = parsed
                    break
                except Exception as e:
                    print("  [%s] page %d attempt %d [%s] failed: %s" % (idx, page, attempt + 1, transport, str(e)[:90]))
            if jb is None:
                if idx == "equities" and page == 1:
                    raise RuntimeError("mainboard filings list yielded nothing — NSE locked down?")
                break
            d = jb.get("data", []) if isinstance(jb, dict) else jb
            rows.extend(d or [])
            got += len(d or [])
            total = max(total, (jb.get("totalCount") or 0) if isinstance(jb, dict) else 0)
            if not d or got >= total or page > 150:
                break
            page += 1
        print("filings [%s] last %d days: %d rows" % (idx, window_days, got))
    return rows


def fetch_new(rows, max_fetch=None):
    """Download every listed XBRL the cache doesn't have yet. Append-only; failures skip
    (the window overlap re-offers them tomorrow). max_fetch caps a cold CI start."""
    have = set(os.listdir(CACHE))
    new, fail = 0, 0
    jar = warm_jar()
    for r in rows:
        if max_fetch and new >= max_fetch:
            print("  (--max-fetch %d reached — the rest re-offer next run)" % max_fetch)
            break
        xb = r.get("xbrl", "")
        if not xb.startswith("http"):
            continue
        if "governance" in (r.get("type", "") or "").lower():
            continue
        fname = canon(xb)
        if fname in have:
            continue
        try:
            xml = B._get(xb, headers={"User-Agent": B.UA, "Referer": "https://www.nseindia.com/"},
                         jar=jar, timeout=60)
        except Exception:
            try:
                xml = cffi_get(xb, {"User-Agent": B.UA})
            except Exception as e2:
                print("  FETCH FAIL %s: %s" % (fname[:40], str(e2)[:60]))
                fail += 1
                continue
        if len(xml) < 500:
            fail += 1
            continue
        with open(os.path.join(CACHE, fname), "w", encoding="utf-8") as fh:
            fh.write(xml)
        have.add(fname)
        new += 1
    print("cache top-up: +%d files (%d failed, self-heal via window overlap)" % (new, fail))
    return new


def prune_cache(days):
    """Drop cache files older than `days` (CI's rolling window only — see PRUNE_MAX_FILES)."""
    names = os.listdir(CACHE)
    if len(names) > PRUNE_MAX_FILES:
        print("prune skipped: %d files looks like the full local cache, not a CI window" % len(names))
        return
    cutoff = time.time() - days * 86400
    gone = 0
    for n in names:
        p = os.path.join(CACHE, n)
        try:
            if os.path.getmtime(p) < cutoff:
                os.remove(p)
                gone += 1
        except OSError:
            pass
    print("cache prune: -%d files older than %dd (%d kept)" % (gone, days, len(names) - gone))


def main():
    args = sys.argv[1:]
    push = "--push" in args
    def opt(name, default=None):
        return int(args[args.index(name) + 1]) if name in args else default
    window = opt("--window", WINDOW_DAYS)
    max_fetch = opt("--max-fetch")
    prune_days = opt("--prune-cache-days")
    if push and os.path.basename(ROOT).lower() == SHARED_CHECKOUT:
        sys.exit("ABORT: --push from the shared interactive checkout is forbidden (CLAUDE.md rule 2) — "
                 "run from the xbrl-extra worktree")
    if not os.path.isdir(CACHE):
        if "--seed-seen" in args:          # CI: first night, nothing restored from the Actions cache
            os.makedirs(CACHE)
        else:
            sys.exit("ABORT: cache dir %s missing — set XBRL_CACHE to the main checkout's scripts/_xbrl_cache" % CACHE)
    # Seed the seen-set BEFORE fetching: everything already cached was extracted by an earlier
    # run, everything downloaded below is new. (Cold cache ⇒ empty seed ⇒ extract all.)
    if "--seed-seen" in args and not os.path.exists(SEEN):
        json.dump(sorted(os.listdir(CACHE)), open(SEEN, "w"), separators=(",", ":"))
        print("seeded seen-set from %d cached files" % len(os.listdir(CACHE)))

    rows = list_filings(window)
    fetch_new(rows, max_fetch)

    env = dict(os.environ, XBRL_CACHE=CACHE)
    r = subprocess.run([sys.executable, "-X", "utf8", os.path.join(HERE, "build_xbrl_extra.py"),
                        "--incremental"], env=env)
    if r.returncode != 0:
        sys.exit("ABORT: incremental extraction failed (rc %d)" % r.returncode)

    raw = open(LEDGER, "rb").read()
    open(GZ, "wb").write(gzip.compress(raw, 9))
    print("ledger: %.1f MB raw -> %.1f MB gz" % (len(raw) / 1e6, os.path.getsize(GZ) / 1e6))

    if prune_days:                          # only after the extract above succeeded
        prune_cache(prune_days)

    if push:
        def git(*a):
            return subprocess.run(["git", "-C", ROOT] + list(a), capture_output=True, text=True)
        st = git("status", "--porcelain", "--", "scripts/xbrl_extra.json.gz").stdout.strip()
        if not st:
            print("gz unchanged — nothing to push")
            return
        git("add", "scripts/xbrl_extra.json.gz")
        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M IST")
        git("commit", "-m", "Deep-fundamentals ledger top-up %s" % ts)
        for i in range(3):
            git("fetch", "origin", "-q")
            rb = git("rebase", "origin/main")
            if rb.returncode == 0 and git("push", "origin", "HEAD:main").returncode == 0:
                print("pushed ledger top-up")
                return
            git("rebase", "--abort")
            time.sleep(3)
        sys.exit("push failed after 3 attempts")


if __name__ == "__main__":
    main()
