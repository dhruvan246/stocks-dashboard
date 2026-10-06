# Routine prompts prepared by a session — PROPOSED text for the routines UI (runbook §222)

The cloud routines' prompts live in the trigger config, not in this repo. These files exist because a session cannot finish the
routine half of the automatic season cadence (measured 2026-10-06):
- the API refuses agent edits to routines created in the web UI ("created via http_api, not by an agent");
- a routine a session creates carries NO repository source, so its VM starts empty and the auto-mode classifier denies cloning
  and running the repo's own script ("Code from External"). The one created that way, `bse-vision-fill-daytime`
  (trig_01M7CxRAhg4tsXWDEZHbWxtH), is DISABLED for that reason — delete it once the step below is done.

So the one-time step is the user's, in claude.ai/code/routines. Do ONE:

1. **Recommended — edit the existing `bse-vision-fill`** (trig_01N3H7t8Dgn2XmLqwBg94j2r): cron `45 7,10,15,18 * * *`
   (UTC = 13:15 / 16:15 / 21:15 / 00:15 IST), prompt = `bse-vision-fill.4slot.prompt.txt`. The platform keeps cloning the repo for
   it, so step 0 is just `python3 scripts/season_state.py --vision-slot`: off-season the three daytime slots print SKIP and exit at
   once, in season all four run. Nothing else to touch, ever.
2. **Or two routines** — leave `bse-vision-fill` at 00:15 IST and create `bse-vision-fill-daytime` in the UI WITH
   dhruvan246/stocks-dashboard attached as its repository, environment env_01Pb6Vujaf9FQ9m1kZXYJN9c, cron `45 7,10,15 * * *`,
   prompt = `bse-vision-fill-daytime.prompt.txt` (its clone step is guarded by `[ -d .git ] ||`, a no-op when the platform clone exists).

Either way, also disable or delete `vision-fill-season-restore-check` (trig_013w3xTyqPckt82NxGHFQWDS): it only existed to tell a
human to restore the three schedules by hand; the two CI schedules are automatic now and the routine will be once the step above is done.
Once pasted, the trigger config is the source of truth again — do not treat these copies as live.
