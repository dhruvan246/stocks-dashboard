# stocks-dashboard — session rules

## Concurrency contract — multiple writers share this repo, READ FIRST

Three kinds of actors write this repo, often at the same time:
1. ~30 GitHub Actions workflows (cloud) committing generated data all day.
2. Local scheduled routines (e.g. bse-vision-fill) — each owns a private worktree under `C:\Users\dhruv\stocks-wt\`, never this checkout.
3. Interactive Claude sessions — often more than one at once — sharing THIS checkout.

Rules that keep them from fighting (violated → the 2026-07-22 tangle, see DATA_RUNBOOK §38):

1. **Own files only.** Run `git status` before committing. Files you didn't modify are another session's work-in-progress: never add, stash, restore, reset, or conflict-resolve them. Stage with explicit paths (`git add path1 path2`); NEVER `git add -A`, `git add .`, `git add -u`, or `git commit -a`.
2. **No tree-wide git mutations in this checkout.** Never `git reset --hard`, `git checkout -- .`, `git stash`, or `git rebase --autostash` here — they destroy other sessions' uncommitted work. Those commands are fine only inside a worktree you created yourself.
3. **Long or scripted data jobs get their own worktree.** Anything running >15 min, looping over many files, or needing rebases while others work:
   `git worktree add --detach C:/Users/dhruv/stocks-wt/<job-name> origin/main` → work, commit, push from there → `git worktree remove` when done.
4. **Push recipe** (any tree): commit file-scoped, then
   `for i in 1 2 3; do git fetch origin -q; git rebase origin/main -q && git push origin HEAD:main && break; sleep 3; done`
   If the rebase refuses because OTHER sessions' dirty files sit in this shared checkout, do NOT stash them — cherry-pick your commit in a temp worktree and push from there.
5. **Heal data via ledgers, not derived files.** Nightly CI rebuilds derived JSONs (sf_revop.json, results_season.json, quarterly_results.json, sf_fundamentals.json, dash bins…) and will clobber direct edits. Route fixes through the matching ledger (scale_fix.json, feed_qe_fix.json, ann_date_fills.json, revop_fundamentals.json, _bse_fund_done.json, …) then rebuild. After pushing a data heal, re-verify LIVE ~20 min later — an in-flight CI run may have raced you.
6. **Timestamps:** plain `date +'%Y-%m-%d %H:%M IST'` (this machine runs IST). `TZ=Asia/Kolkata date` silently prints UTC on Windows git-bash (no tz database) and mislabels commits by 5h30m.

## Data work

Before ANY data work, read `scripts/DATA_RUNBOOK.md` and follow it — procedures there override improvisation.
