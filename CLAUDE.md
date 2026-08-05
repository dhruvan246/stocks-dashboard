# stocks-dashboard — session rules

## Concurrency contract — multiple writers share this repo, READ FIRST

Three kinds of actors write this repo, often at the same time:
1. ~30 GitHub Actions workflows (cloud) committing generated data all day.
2. Cloud Claude routines (bse-vision-fill 4×/day, deep-fundamentals nightly) — each runs on a fresh cloud VM and lands via a `claude/*` branch + auto-merged PR. (Any future LOCAL routine must own a private worktree under `~/stocks-wt/`, never this checkout.)
3. Interactive Claude sessions — often more than one at once — sharing THIS checkout.

Rules that keep them from fighting (violated → the 2026-07-22 tangle, see DATA_RUNBOOK §38):

1. **Own files only.** Run `git status` before committing. Files you didn't modify are another session's work-in-progress: never add, stash, restore, reset, or conflict-resolve them. Stage with explicit paths (`git add path1 path2`); NEVER `git add -A`, `git add .`, `git add -u`, or `git commit -a`.
2. **No tree-wide git mutations in this checkout.** Never `git reset --hard`, `git checkout -- .`, `git stash`, or `git rebase --autostash` here — they destroy other sessions' uncommitted work. Those commands are fine only inside a worktree you created yourself.
3. **Long or scripted data jobs get their own worktree.** Anything running >15 min, looping over many files, or needing rebases while others work:
   `git worktree add --detach ~/stocks-wt/<job-name> origin/main` → work, commit, push from there → `git worktree remove` when done.
4. **Push recipe** (any tree): commit file-scoped, then
   `for i in 1 2 3; do git fetch origin -q; git rebase origin/main -q && git push origin HEAD:main && break; sleep 3; done`
   If the rebase refuses because OTHER sessions' dirty files sit in this shared checkout, do NOT stash them — cherry-pick your commit in a temp worktree and push from there.
5. **Heal data via ledgers, not derived files.** Nightly CI rebuilds derived JSONs (sf_revop.json, results_season.json, quarterly_results.json, sf_fundamentals.json, dash bins…) and will clobber direct edits. Route fixes through the matching ledger (scale_fix.json, feed_qe_fix.json, ann_date_fills.json, revop_fundamentals.json, _bse_fund_done.json, …) then rebuild. After pushing a data heal, re-verify LIVE ~20 min later — an in-flight CI run may have raced you.
6. **Timestamps:** plain `date +'%Y-%m-%d %H:%M IST'` (this Mac runs IST). (The old Windows git-bash trap — `TZ=Asia/Kolkata date` silently printing UTC — is gone; `TZ=` works normally on macOS, but plain `date` stays the convention.)

**Enforcement:** rules 1–2 are enforced by hooks (`.claude/settings.json` → `scripts/_concurrency_guard.py`). Editing a file that another session has dirty, or running a tree-wide git mutation in this checkout, pops a confirmation prompt; session start injects a list of files currently dirty. If the guard prompts you, it is almost always right — resolve the conflict (coordinate or use a worktree), don't route around it.

## Data work

Before ANY data work, read `scripts/DATA_RUNBOOK.md` and follow it — procedures there override improvisation.

## Ship-it quality gate — UI, design, features, everything

Standing rule from the user (2026-07-28): **anything you build must be bug-free when you hand it over.**
A change is done when it has been RUN and SEEN WORKING, not when the edit is written — no size exemption,
the "obviously safe one-liner" is exactly what has shipped broken before.

Before saying "done" or pushing ANY code change, run the full gate in **DATA_RUNBOOK §39**: syntax/import
check every touched file → load the page and read the console (zero errors) → check the blast radius of
shared files (`theme.js`/`theme.css` are on every page) → test the empty/missing/1-row/renamed-ticker paths
→ mobile 375px + dark and light → bump the service-worker cache if assets changed → verify LIVE after the
push. Then report exactly what you verified and what you did not — never call something "working" that you
only read instead of ran. If a bug ships anyway, add the check that would have caught it to §39.
