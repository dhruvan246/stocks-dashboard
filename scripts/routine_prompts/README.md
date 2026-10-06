# Routine prompts prepared by a session — PROPOSED text, to be pasted into claude.ai/code/routines (runbook §222)

The cloud routines' prompts live in the trigger config, not in this repo. These files exist because an agent session
cannot edit a routine the user created in the web UI (the API refuses: "created via http_api, not by an agent"), and
creating a new routine from a session was denied by the permission classifier on 2026-10-06. So the one-time edit that
completes the automatic results-season cadence has to be made by the user, by copy-paste. Once pasted, the trigger
config is the source of truth again — do not treat these copies as live.

Two equivalent options (do ONE):

1. **Edit the existing `bse-vision-fill` routine** (trig_01N3H7t8Dgn2XmLqwBg94j2r):
   cron `45 7,10,15,18 * * *` (UTC = 13:15 / 16:15 / 21:15 / 00:15 IST), prompt = `bse-vision-fill.4slot.prompt.txt`.
   Step 0 of that prompt runs `python3 scripts/season_state.py --vision-slot`, so off-season the three daytime slots exit
   at once and only 00:15 IST does the work; in season all four run. Nothing else to touch, ever.

2. **Leave `bse-vision-fill` as it is (00:15 IST) and add a sibling** `bse-vision-fill-daytime`:
   cron `45 7,10,15 * * *`, environment env_01Pb6Vujaf9FQ9m1kZXYJN9c (the same one), no connectors, prompt =
   `bse-vision-fill-daytime.prompt.txt`. Same guard; same effect. (A session can also create this one if
   `mcp__claude-code-remote__create_trigger` is allowed for it.)

Either way, also disable or delete `vision-fill-season-restore-check` (trig_013w3xTyqPckt82NxGHFQWDS): it only existed to
tell a human to restore the three schedules by hand, and the CI two are automatic now.
