# -*- coding: utf-8 -*-
"""Bull-Run Survivors grids: 4 measured bull legs x 2 earnings bases x each universe (DATA_RUNBOOK §7.4b).

  python3 scripts/bullruns_grid_run.py [--jobs N] [--universes "Nifty 500,Nifty Next 50"]

Each job is one `grid_search_mega.js` run in MAIN_ONLY mode, basket top-3 · hold. RESUMABLE: a job
whose `_gridmega_top_<tag>.json` marker exists is skipped. Run `python3 scripts/gridmega_fetch_live.py`
first (ONE staging for every job), and purge `_gridmega_cache_*` whenever `_live/` is re-staged.
Then `node scripts/bullruns_grid_build.js`.
"""
import argparse, os, subprocess, sys, threading, time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
LOGD = os.path.join(HERE, "_gmlog")

# the four completed legs, trough -> peak off the NIFTY 500 daily close, 13% reversal filter
LEGS = [("2013-08-28", "2015-03-03"), ("2016-02-25", "2018-08-31"),
        ("2020-03-23", "2021-10-18"), ("2022-06-20", "2024-09-26")]
UPRE = {"Nifty 500": "", "Nifty Next 50": "nn50_"}   # must match grid_search_mega.js UPRE


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--jobs", type=int, default=4)
    ap.add_argument("--universes", default="Nifty 500,Nifty Next 50")
    a = ap.parse_args()
    os.makedirs(LOGD, exist_ok=True)

    runner = os.path.join(HERE, "_gridmega_run.js")
    with open(runner, "wb") as out:
        for part in ("scripts/gridmega_shim.js", "docs/backtest-engine.js", "scripts/grid_search_mega.js"):
            with open(os.path.join(ROOT, part), "rb") as f:
                out.write(f.read())
    subprocess.run(["node", "--check", runner], check=True)
    print("runner built + syntax-checked", flush=True)

    jobs = []
    for uni in [u.strip() for u in a.universes.split(",")]:
        for start, end in LEGS:
            for basis in ("std", "con"):
                tag = "%s_%s_%sh3_%s" % (start, end, UPRE[uni], basis)
                if os.path.exists(os.path.join(HERE, "_gridmega_top_%s.json" % tag)):
                    continue
                jobs.append((tag, start, end, {"UNIVERSE": uni, "EARN_BASIS": basis}))
    print("%d job(s) to run, %d at a time" % (len(jobs), a.jobs), flush=True)

    lock, state = threading.Lock(), {"n": 0, "fail": []}
    t0 = time.time()

    def work(job):
        tag, start, end, extra = job
        env = dict(os.environ, MAIN_ONLY="1", TOPN="3", METHOD="hold", **extra)
        log = os.path.join(LOGD, tag + ".log")
        with open(log, "wb") as lf:
            r = subprocess.run(["node", "--max-old-space-size=3072", runner, start, end],
                               cwd=ROOT, env=env, stdout=subprocess.DEVNULL, stderr=lf)
        with lock:
            state["n"] += 1
            ok = r.returncode == 0 and os.path.exists(os.path.join(HERE, "_gridmega_top_%s.json" % tag))
            if not ok:
                state["fail"].append(tag)
            print("[%2d/%2d] %-44s %s  (%.0f min elapsed)"
                  % (state["n"], len(jobs), tag, "ok" if ok else "FAILED rc=%s" % r.returncode,
                     (time.time() - t0) / 60), flush=True)

    q, qlock = list(jobs), threading.Lock()

    def loop(slot):
        time.sleep(slot * 40)   # stagger the ~3.3 GB load peaks (gridmega_phases_run.py)
        while True:
            with qlock:
                if not q:
                    return
                job = q.pop(0)
            work(job)

    threads = [threading.Thread(target=loop, args=(i,), daemon=True) for i in range(min(a.jobs, len(jobs)))]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print("\nall done in %.0f min" % ((time.time() - t0) / 60), flush=True)
    if state["fail"]:
        print("FAILED: %s" % ", ".join(state["fail"]), flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
