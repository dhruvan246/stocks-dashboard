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

## ONE source of truth: origin/main — the checkout syncs itself (2026-08-24, runbook §107)

The shared checkout used to sit 900+ commits behind origin with a banner of "dirty" files and
"unpushed" commits that were ALL stale copies of things already upstream — so one model read
the checkout, another read a worktree, a third read the live site, and each reported a
different coverage count or backtest result for the same question. Fixed structurally:

- **`scripts/sync_checkout.py`** brings a tree to origin/main without losing anyone's work. It
  MEASURES before it moves: a local commit counts as upstream only if its content is byte-
  identical on origin or has a subject+author-time twin there; a dirty file counts as stale
  only if its exact bytes are a version origin committed. Everything else is real WIP — kept,
  and if it collides with origin the sync REFUSES (it uses `git reset --keep`, never `--hard`).
  Refreshed copies go to `~/stocks-backups/sync-<stamp>/` first.
- **It runs automatically at every session start** (via `_concurrency_guard.py`, once the hook
  timeout in `.claude/settings.json` is ≥300 s), and `gc` removes worktrees idle ≥48 h that hold
  nothing unique. By hand: `python3 scripts/sync_checkout.py status|sync|gc --dry-run`.
- **Therefore: every number you report — coverage, backtest, cell count — is measured from the
  synced checkout (state its HEAD sha) or from the LIVE site, and you say which.** Never from a
  worktree copy, never from a checkout whose `status` shows it behind origin. If the sync is
  blocked, fix the blocker it names before analysing anything.
- This repo is a **partial clone (`blob:none`)**: old file contents are fetched from GitHub on
  demand. Anything that reads blob CONTENT for many old commits (`cat-file`, `patch-id`,
  `git cherry`, `diff` across hundreds of commits) silently goes to the network and can stall —
  use tree-level commands (`log --raw`, `rev-parse sha:path`, `ls-tree`) for history questions.

## Every change must go LIVE on the site — standing rule (2026-08-23)

Standing rule from the user: when they ask for a change or addition (UI, code, data, fix), **"done" means it is LIVE on the GitHub Pages site** — not merely committed, and NOT left sitting on a `claude/*` feature branch. The site publishes **only from `main`** (`.github/workflows/pages.yml`, on push to `main` touching `docs/**`).

A web/cloud task may hand you a `claude/*` branch and say "develop and push there" — treat that as a **starting point, not the finish line.** After the change is built and verified, land it on `main` via the push recipe above (file-scoped commit → `git fetch origin` → `git rebase origin/main` → `git push origin HEAD:main`, retrying through the data-bot race), then **confirm the Pages deploy ran and re-verify LIVE** (the change is under `docs/`, so the push triggers `pages.yml`; the button/feature appears on the live URL ~1–3 min later). Pushing to a feature branch and stopping is NOT finishing the task.

This is the user's explicit, standing authorization to push their requested changes straight to `main`. Only stop short of going-live when the user explicitly asks for a PR / review instead.

## No assumptions, no guesswork — standing rule, ALL work

Standing rule from the user (2026-08-10), applying to data, code, UI, and answers alike:
**never assume, never guess.** Every value written and every claim made ("exists", "absent",
"fixed", "live", "works") must trace to something measured or read this session. Don't know it?
Go measure it. Can't measure it? Say "unknown" — a plausible guess presented as fact is worse
than an admitted gap. (Top golden rule in DATA_RUNBOOK §0; every campaign doc carries the same line.)

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
