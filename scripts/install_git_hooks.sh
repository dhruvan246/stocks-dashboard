#!/usr/bin/env bash
# Point this repo's git hooks at the COMMITTED scripts/githooks/ directory.
#
# Why core.hooksPath instead of copying into .git/hooks: .git/hooks is not version-controlled, so a
# hook that lives there exists on exactly one machine and silently disappears in a fresh clone. With
# hooksPath the hook itself is committed and reviewable, and every worktree of this repo picks it up
# from the common config — which matters here, because long data jobs run from ~/stocks-wt/ worktrees.
#
# Run once per clone:  bash scripts/install_git_hooks.sh
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

existing=$(ls -1 .git/hooks 2>/dev/null | grep -v '\.sample$' || true)
if [ -n "$existing" ]; then
  echo "⚠️  .git/hooks already contains real hooks:"
  echo "$existing" | sed 's/^/      /'
  echo "   Setting core.hooksPath makes git IGNORE that directory entirely. Move them into"
  echo "   scripts/githooks/ first, then re-run this. Nothing changed."
  exit 1
fi

prev=$(git config --get core.hooksPath || true)
if [ -n "$prev" ] && [ "$prev" != "scripts/githooks" ]; then
  echo "⚠️  core.hooksPath is already set to '$prev'. Leaving it alone — resolve by hand."
  exit 1
fi

chmod +x scripts/githooks/* 2>/dev/null || true
git config core.hooksPath scripts/githooks
echo "✅ core.hooksPath = scripts/githooks"
echo "   Installed hooks:"
ls -1 scripts/githooks | sed 's/^/      /'
echo
echo "   pre-commit refuses a commit that resurrects a HELD cell (runbook §56b). It only runs when the"
echo "   commit stages docs/sf_revop.json, docs/sf_fundamentals.json, or a scripts/ ledger, so ordinary"
echo "   commits are unaffected. Override with: git commit --no-verify"
