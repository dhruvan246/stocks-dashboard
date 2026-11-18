"""Tests for scripts/ci_preserve_merge.py (runbook §56 three-way merge; deletion rule 2026-11-18, §214b batch 3).

  L1 listrows  CI-changed slot wins; origin-only slot change stays; a row CI ADDED lands; a key CI ADDED lands;
               a ROW origin removed mid-run stays removed; a KEY origin removed mid-run stays removed
  Q1 qmaps     the same for the sf_revop shape
  V1 valve     origin lacking > MAX_DROP_KEYS keys the base held = truncation -> old union behaviour + ::warning::
  B1 broken    unreadable origin copy -> no removal inferred (old behaviour)
  R1 replay    (optional, needs the git objects) the 2026-11-18 14:46 IST refresh: base 1ee1184de, CI output
               2f95f571e4, origin 3e2ec0d1b0 -> the three MIIL phantom quarters ff9b773818 dropped stay dropped
               and nothing else differs from what CI landed

  python3 scripts/test_ci_preserve_merge.py [--replay]       (temp dirs only; prints ALL PASS or raises)
"""
import json, os, subprocess, sys, tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
M = os.path.join(HERE, "ci_preserve_merge.py")
TMP = tempfile.mkdtemp(prefix="cpm_test_")


def run(base, ours, theirs, tag, theirs_raw=None):
    paths = {}
    for n, o in (("b", base), ("o", ours), ("t", theirs)):
        paths[n] = os.path.join(TMP, n + ".json")
        with open(paths[n], "w") as f:
            if n == "t" and theirs_raw is not None:
                f.write(theirs_raw)
            else:
                json.dump(o, f)
    out = os.path.join(TMP, "out.json")
    r = subprocess.run([sys.executable, M, paths["b"], paths["o"], paths["t"], out], capture_output=True, text=True)
    assert r.returncode == 0, (tag, r.stderr)
    print("%-3s %s" % (tag, r.stdout.strip().replace("\n", " / ")))
    return json.load(open(out)), r.stdout


def R(q, v):
    return [q, v, 20200101, v, 20200101]


def main():
    base = {"A": [R(1, 1.0), R(2, 2.0), R(3, 3.0)], "OLD": [R(1, 9.0)], "K": [R(1, 1.0)]}
    ours = {"A": [R(1, 1.5), R(2, 2.0), R(3, 3.0), R(4, 4.0)], "OLD": [R(1, 9.0)], "K": [R(1, 1.0)], "NEW": [R(1, 7.0)]}
    theirs = {"A": [R(1, 1.0), R(3, 3.3)], "K": [R(1, 1.0), R(2, 5.0)]}
    out, _ = run(base, ours, theirs, "L1")
    a = {r[0]: r for r in out["A"]}
    assert a[1][1] == 1.5, "CI change must win"
    assert 2 not in a, "a row removed on origin mid-run must stay removed"
    assert a[3][1] == 3.3, "origin's change to a slot CI did not touch must stay"
    assert a[4][1] == 4.0, "a row CI added must land"
    assert "OLD" not in out, "a key removed on origin mid-run must stay removed"
    assert out["NEW"] == [R(1, 7.0)], "a key CI added must land"
    assert {r[0] for r in out["K"]} == {1, 2}, "origin's added row must stay"

    qb = {"A": {"1": [1, 1], "2": [2, 2]}, "OLD": {"1": [9, 9]}}
    qo = {"A": {"1": [1, 1.5], "2": [2, 2], "3": [3, 3]}, "OLD": {"1": [9, 9]}, "NEW": {"1": [7, 7]}}
    qt = {"A": {"1": [1, 1]}}
    out, _ = run(qb, qo, qt, "Q1")
    assert out == {"A": {"1": [1, 1.5], "3": [3, 3]}, "NEW": {"1": [7, 7]}}, out

    tb = {"S%d" % i: [R(1, 1.0)] for i in range(30)}
    tb["Z"] = [R(1, 1.0)]
    out, so = run(tb, dict(tb), {"Z": [R(1, 1.0)]}, "V1")
    assert len(out) == 31 and "::warning::" in so, "truncated origin must fall back to the union"

    out, _ = run(base, ours, None, "B1", theirs_raw="{broken")
    assert "OLD" in out and {r[0] for r in out["A"]} == {1, 2, 3, 4}, "unreadable origin must fall back to ours"

    if "--replay" in sys.argv:
        root = os.path.dirname(HERE)
        def show(rev, p):
            return json.loads(subprocess.run(["git", "-C", root, "show", "%s:%s" % (rev, p)], capture_output=True, text=True, check=True).stdout)
        f = "docs/sf_fundamentals.json"
        b, o, t = show("1ee1184de", f), show("2f95f571e4", f), show("3e2ec0d1b0", f)
        out, _ = run(b, o, t, "R1")
        diff = sorted(k for k in set(out) | set(o) if out.get(k) != o.get(k))
        assert diff == ["MIIL"], diff
        assert {r[0] for r in o["MIIL"]} - {r[0] for r in out["MIIL"]} == {20191231, 20201231, 20210630}
    print("ALL PASS")


if __name__ == "__main__":
    main()
