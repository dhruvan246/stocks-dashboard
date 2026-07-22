# -*- coding: utf-8 -*-
"""Cross-session clobber guard for the SHARED stocks-dashboard checkout (runbook #38).

Wired as Claude Code hooks in .claude/settings.json. Modes (argv[1]):
  pre-edit       PreToolUse Edit|Write  - target file has uncommitted changes NOT made by
                 this session -> permissionDecision "ask" instead of silently overwriting.
  post-edit      PostToolUse Edit|Write - record the file as touched by this session.
  pre-bash       PreToolUse Bash        - tree-wide git mutations in the shared checkout
                 (reset --hard, stash, add -A, commit -a, autostash rebase, ...) -> "ask".
  session-start  SessionStart           - inject a heads-up listing files other sessions
                 have dirty right now, plus stash pile-up / local-vs-origin divergence.

Scope: only files/commands touching C:\\Users\\dhruv\\stocks-dashboard proper. Anything in
a worktree (C:\\Users\\dhruv\\stocks-wt\\* or .claude\\worktrees\\*) is exempt by design -
worktrees are single-writer, that's the whole point.

Never blocks on its own failure: any internal error -> silent allow.
"""
import json, os, re, subprocess, sys, tempfile

MAIN = os.path.normcase(r"C:\Users\dhruv\stocks-dashboard")
WT_MARK = os.path.normcase(os.sep + ".claude" + os.sep + "worktrees" + os.sep)


def emit(obj):
    print(json.dumps(obj))


def ask(reason):
    emit({"hookSpecificOutput": {"hookEventName": "PreToolUse",
                                 "permissionDecision": "ask",
                                 "permissionDecisionReason": reason}})


def git(args):
    r = subprocess.run(["git"] + args, cwd=MAIN, capture_output=True,
                       text=True, encoding="utf-8", errors="replace", timeout=10)
    return r.stdout or ""


def ledger_path(sid):
    sid = re.sub(r"[^A-Za-z0-9_-]", "", sid or "nosid")
    return os.path.join(tempfile.gettempdir(), "claude_touched_%s.txt" % sid)


def in_shared_checkout(path):
    p = os.path.normcase(os.path.abspath(path))
    return (p == MAIN or p.startswith(MAIN + os.sep)) and WT_MARK not in p


def pre_edit(h):
    fp = (h.get("tool_input") or {}).get("file_path") or ""
    if not fp or not in_shared_checkout(fp) or not os.path.exists(fp):
        return
    key = os.path.normcase(os.path.abspath(fp))
    try:
        with open(ledger_path(h.get("session_id")), encoding="utf-8") as f:
            if key in f.read().splitlines():
                return  # this session already owns the file
    except OSError:
        pass
    st = git(["status", "--porcelain", "--", fp]).strip()
    if st:
        rel = os.path.relpath(fp, MAIN)
        ask("STOP - '%s' already has uncommitted changes (git: %s) that THIS session did not "
            "make. Another session or job is probably working on it (CLAUDE.md rule 1 / "
            "runbook 38). Do not overwrite - check `git status`, coordinate, or use your own "
            "worktree. Proceed only if the user confirms these changes are safe to touch."
            % (rel, st.split()[0]))


def post_edit(h):
    fp = (h.get("tool_input") or {}).get("file_path") or ""
    if not fp or not in_shared_checkout(fp):
        return
    with open(ledger_path(h.get("session_id")), "a", encoding="utf-8") as f:
        f.write(os.path.normcase(os.path.abspath(fp)) + "\n")


DANGER = [
    (r"git\b[^\n|;&]*\breset\s+--hard", "git reset --hard"),
    (r"git\b[^\n|;&]*\bstash\b(?!\s+(list|show))", "git stash"),
    (r"git\b[^\n|;&]*\brebase\b[^\n|;&]*--autostash", "git rebase --autostash"),
    (r"git\b[^\n|;&]*\badd\s+(-a\b|--all|-u\b|\.(\s|$))", "git add -A / . / -u"),
    (r"git\b[^\n|;&]*\bcommit\s+-a", "git commit -a"),
    (r"git\b[^\n|;&]*\bcheckout\s+(--\s+)?\.(\s|$)", "git checkout ."),
    (r"git\b[^\n|;&]*\brestore\s+\.(\s|$)", "git restore ."),
    (r"git\b[^\n|;&]*\bclean\b", "git clean"),
    (r"git\b[^\n|;&]*\bpush\b[^\n|;&]*(--force|\s-f\b)", "git push --force"),
]


def pre_bash(h):
    cwd = h.get("cwd") or ""
    if cwd and not in_shared_checkout(cwd):
        return  # session already lives in its own worktree - its tree, its rules
    cmd = ((h.get("tool_input") or {}).get("command") or "").lower()
    if "stocks-wt" in cmd or ".claude/worktrees" in cmd or ".claude\\worktrees" in cmd:
        return  # isolated-worktree work is the sanctioned pattern
    for pat, name in DANGER:
        if re.search(pat, cmd):
            ask("`%s` in the SHARED checkout can destroy other sessions' uncommitted work "
                "(CLAUDE.md rules 1-2 / runbook 38). Stage explicit paths instead, or do this "
                "inside your own worktree under C:/Users/dhruv/stocks-wt/. Proceed only if the "
                "user confirms." % name)
            return


def session_start(h):
    notes = []
    dirty = [l for l in git(["status", "--porcelain"]).splitlines() if l.strip()]
    if dirty:
        notes.append("Files with uncommitted changes right now (possibly ANOTHER session's "
                     "work-in-progress - do not add/stash/overwrite them, CLAUDE.md rule 1):\n"
                     + "\n".join("  " + l for l in dirty[:15])
                     + ("\n  ... and %d more" % (len(dirty) - 15) if len(dirty) > 15 else ""))
    stashes = [l for l in git(["stash", "list"]).splitlines() if l.strip()]
    if len(stashes) >= 3:
        notes.append("%d git stashes piled up - usually leftovers of past session tangles; "
                     "worth reviewing/clearing when idle." % len(stashes))
    counts = git(["rev-list", "--left-right", "--count", "main...origin/main"]).split()
    if len(counts) == 2 and counts[0].isdigit() and int(counts[0]) >= 3:
        notes.append("Local main is %s commits ahead of origin/main (as of last fetch) - "
                     "possible unpushed or duplicate commits; see runbook 38." % counts[0])
    if notes:
        emit({"hookSpecificOutput": {"hookEventName": "SessionStart",
                                     "additionalContext":
                                         "[concurrency-guard] " + "\n".join(notes)}})


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else ""
    try:
        # strip BOM: PowerShell 5.1 pipes prepend U+FEFF, which breaks json.load
        h = json.loads(sys.stdin.read().lstrip("﻿"))
    except Exception:
        h = {}
    {"pre-edit": pre_edit, "post-edit": post_edit,
     "pre-bash": pre_bash, "session-start": session_start}.get(mode, lambda _: None)(h)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        pass  # a broken guard must never block real work
    sys.exit(0)
