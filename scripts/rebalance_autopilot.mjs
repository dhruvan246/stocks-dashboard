// Rebalance AUTO-PILOT robot (approved by the user 2026-09-29; built Oct 2026).
//
// Opens the site's Strategies panel (the SAME strategies-panel.js the terminal runs) in headless Chromium with the
// pf token and calls window.spAuto — so the robot runs exactly the steps the user's buttons run: the checklist's own
// fixes, then each ⭐ strategy's sell basket on the sell day (T) and stragglers + buy baskets on the buy day (T+1..T+3).
//
// It acts ONLY when the user ARMED this rebalance on the panel's Auto-pilot box (autos[T].armed in the token-gated
// zba row) and today is that rebalance's sell or buy day. Every pass is idempotent: spAuto.run() sends only baskets not
// already sent (synced marks + today's jobs on the box), so a later pass fixes a missed one (e.g. Kite logged in late).
// A manual "dry" run (workflow_dispatch) rehearses either leg on ANY day and sends nothing.
//
// PRIVACY — this repo and its Actions logs are PUBLIC: this script prints only counts and states — never stock names,
// quantities, amounts, the token, the worker address or the alert topic (all from repo secrets). Details go to the
// panel's own log (token-gated row) and to the user's private ntfy topic.
// SAFETY — the page may write ONLY its own rebalance row (pf_feed_set for <token>.zbamts); every other write (settings,
// saved strategies, picks log, view counter) is blocked at the network layer, so a robot run can never change the user's
// favourites or strategies. Service workers are blocked (theme.js reloads the page on controllerchange).
//
//   node scripts/rebalance_autopilot.mjs --precheck   calendar + armed + Kite login, no browser; writes go=true|false
//   node scripts/rebalance_autopilot.mjs              the leg (needs `playwright` + chromium)
// env: PF_HOLDINGS_TOKEN, PF_KITE_WORKER, NTFY_TOPIC (optional), AP_MODE=auto|dry, AP_LEG=sell|buy (dry runs), AP_SLOT
import { readFileSync, appendFileSync } from 'fs';
import { gunzipSync } from 'zlib';
import { pathToFileURL } from 'url';

const BASE = 'https://dhruvan246.github.io/stocks-dashboard';
const SUPA = 'https://nebjnsndgrhumnkuipqy.supabase.co/rest/v1/rpc/';
const ANON = 'sb_publishable_MDlQwiVc5deii91__UNeDg_z9r4Fk98';            // public (docs/sw-sync.js)
const TOKEN = (process.env.PF_HOLDINGS_TOKEN || '').trim();
const WORKER = (process.env.PF_KITE_WORKER || '').trim().replace(/\/+$/, '');
const NTFY = (process.env.NTFY_TOPIC || '').trim();
const MODE = (process.env.AP_MODE || 'auto').trim() === 'dry' ? 'dry' : 'auto';
const PRE = process.argv.includes('--precheck');
const say = m => console.log('[autopilot] ' + m);                           // counts and states ONLY — public log

// ---------------- calendar (the panel's rebalWindow, in Node): T = the month's last trading day ----------------
const HOL = (() => { try { const d = JSON.parse(readFileSync(new URL('../docs/nse_holidays.json', import.meta.url)));
  return new Set(Object.values(d.holidays || {}).flatMap(y => Object.keys(y))); } catch (e) { return new Set(); } })();
const iso = d => d.toISOString().slice(0, 10);
const off = d => d.getUTCDay() === 0 || d.getUTCDay() === 6 || HOL.has(iso(d));
const lastTD = (y, m) => { let t = new Date(Date.UTC(y, m + 1, 0)); while (off(t)) t = new Date(t.getTime() - 864e5); return t; };
const nextTD = d => { let t = new Date(d.getTime() + 864e5); while (off(t)) t = new Date(t.getTime() + 864e5); return t; };
function windowNow(now = new Date()) {
  const ist = new Date(now.getTime() + 330 * 60000), y = ist.getUTCFullYear(), m = ist.getUTCMonth();
  const today = new Date(Date.UTC(y, m, ist.getUTCDate())), hm = ist.getUTCHours() * 60 + ist.getUTCMinutes();
  const legs = t => { const t1 = nextTD(t), t2 = nextTD(t1), t3 = nextTD(t2); return { t, t1, t3 }; };
  let L = legs(lastTD(y, m - 1));
  if (!(+today >= +L.t && +today <= +L.t3)) L = legs(lastTD(y, m));
  const sellIn = +today === +L.t, buyIn = +today >= +L.t1 && +today <= +L.t3;
  return { T: iso(L.t), T1: iso(L.t1), T3: iso(L.t3), today: iso(today), hm, sellIn, buyIn, leg: sellIn ? 'sell' : buyIn ? 'buy' : null,
           yearKnown: HOL.size > 0 && [...HOL].some(h => h.startsWith(iso(today).slice(0, 4))) };
}

// ---------------- the token-gated rows ----------------
async function rpc(fn, args) {
  const r = await fetch(SUPA + fn, { method: 'POST', headers: { apikey: ANON, Authorization: 'Bearer ' + ANON, 'Content-Type': 'application/json' }, body: JSON.stringify(args) });
  if (!r.ok) throw new Error(fn + ' HTTP ' + r.status);
  const t = await r.text(); return t ? JSON.parse(t) : null;
}
async function zbaDoc() { const row = await rpc('pf_feed_get', { token: TOKEN + '.zbamts' });
  return row && row.z ? JSON.parse(gunzipSync(Buffer.from(row.z, 'base64')).toString()) : null; }
async function kiteStatus() {
  if (!WORKER) return { ok: false, why: 'no worker address (PF_KITE_WORKER secret missing)' };
  try { const r = await fetch(WORKER + '/status', { headers: { 'X-PF-Token': TOKEN } }); const j = await r.json().catch(() => ({}));
    return { ok: r.status === 200 && !!j.connected, why: r.status === 200 ? (j.connected ? '' : 'Kite not logged in today') : 'worker HTTP ' + r.status }; }
  catch (e) { return { ok: false, why: 'worker unreachable' }; }
}
async function ntfy(title, body, prio) {
  if (!NTFY) { say('alert skipped (no NTFY_TOPIC secret)'); return; }
  try { await fetch('https://ntfy.sh/' + encodeURIComponent(NTFY), { method: 'POST', body,
    headers: { Title: title, Priority: String(prio || 3), Tags: prio >= 4 ? 'warning' : 'chart_with_upwards_trend' } }); say('alert sent'); }
  catch (e) { say('alert failed'); }
}
const outSet = (k, v) => { if (process.env.GITHUB_OUTPUT) appendFileSync(process.env.GITHUB_OUTPUT, k + '=' + v + '\n'); };

// ---------------- pre-check: no browser unless there is something to do ----------------
async function precheck() {
  if (!TOKEN) { say('PF_HOLDINGS_TOKEN missing'); outSet('go', 'false'); process.exit(1); }
  const W = windowNow();
  if (!W.yearKnown) say('⚠ no NSE holiday list for ' + W.today.slice(0, 4) + ' — dates assume weekends only');
  if (MODE === 'dry') { say('dry rehearsal requested — today ' + (W.leg ? 'IS the ' + W.leg + ' day' : 'is not a rebalance day') + '; nothing will be sent'); outSet('go', 'true'); return; }
  if (!W.leg) { say('not a rebalance day (next sell day ' + W.T + ') — nothing to do'); outSet('go', 'false'); return; }
  const d = await zbaDoc(); const a = ((d && d.autos) || {})[W.T] || {};
  if (!a.armed) { say(W.leg + ' day for the ' + W.T + ' rebalance, but it is NOT armed — nothing to do'); outSet('go', 'false'); return; }
  const k = await kiteStatus();
  if (!k.ok) {
    say('armed ' + W.leg + ' day, but ' + k.why + ' — alerting and waiting for the next pass');
    const recent = (a.log || []).some(x => /log in to Kite/i.test(x.m) && Date.now() - x.at < 45 * 60000);
    if (!recent) await ntfy('Rebalance auto-pilot: log in to Kite', 'Today is the ' + W.leg + ' day (' + W.T + ' rebalance) and the auto-pilot is armed, but ' + k.why +
      '. Open the terminal and log in to Zerodha; the next pass (every ~20 min) carries on by itself.', 4);
    outSet('go', 'false'); return;
  }
  say('armed ' + W.leg + ' day, Kite connected — starting the browser'); outSet('go', 'true');
}

// ---------------- the leg, through the panel's own code ----------------
async function leg() {
  const W = windowNow(), dry = MODE === 'dry';
  const legName = dry ? ((process.env.AP_LEG || '').trim() === 'buy' ? 'buy' : (process.env.AP_LEG || '').trim() === 'sell' ? 'sell' : (W.leg || 'sell')) : W.leg;
  if (!legName) { say('not a rebalance day — nothing to do'); return 0; }
  if (!TOKEN || !WORKER) { say('missing secret(s): ' + [!TOKEN && 'PF_HOLDINGS_TOKEN', !WORKER && 'PF_KITE_WORKER'].filter(Boolean).join(', ')); return 1; }
  const { chromium } = await import('playwright');
  const browser = await chromium.launch({ args: ['--no-sandbox', '--disable-dev-shm-usage', '--js-flags=--max-old-space-size=4096'] });
  let blocked = 0, code = 0;
  try {
    const ctx = await browser.newContext({ serviceWorkers: 'block' });
    // the page's only allowed write: its own rebalance row. Reads pass; every other write to the database is refused
    // (rpc writes AND any table insert/update/delete), so settings, saved strategies and the picks log stay untouched.
    const READS = new Set(['sw_kv_get', 'sw_picks_get', 'sw_pv_stats', 'bt_strats_public', 'bt_snap_get', 'bt_public', 'pf_feed_get']);
    const HOST = new URL(SUPA).origin;
    await ctx.route(HOST + '/**', async route => {
      const req = route.request(), u = new URL(req.url());
      if (u.pathname.startsWith('/rest/v1/rpc/')) {
        const fn = u.pathname.slice('/rest/v1/rpc/'.length);
        if (READS.has(fn)) return route.continue();
        if (fn === 'pf_feed_set') { let b = {}; try { b = JSON.parse(req.postData() || '{}'); } catch (e) {}
          if (b.token === TOKEN + '.zbamts') return route.continue(); }
      } else if (['GET', 'HEAD', 'OPTIONS'].includes(req.method())) return route.continue();
      blocked++; return route.abort();
    });
    await ctx.addInitScript(([tok, w]) => { try { localStorage.setItem('pf_token', tok); localStorage.setItem('pf_kite_worker', w);
      localStorage.setItem('sw_cloud_slicer', '1'); localStorage.setItem('sp_fav_only', '1'); } catch (e) {} }, [TOKEN, WORKER]);
    // a same-origin page that mounts ONLY the Strategies panel (the terminal's own scripts, served by the site)
    const PAGE = BASE + '/__autopilot.html';
    await ctx.route(PAGE, r => r.fulfill({ contentType: 'text/html', body: `<!doctype html><html><head><meta charset="utf-8"><title>auto-pilot</title>
<script src="./theme.js"></script></head><body><div id="sv"></div><div id="ktoast"></div><script>
(async function(){ const deps=['https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2','./bt-names.js','./bt-identity.js','./backtest-engine.js','./bt-sync.js','./strategies-panel.js?v=' + Date.now()];
  for (const src of deps){ await new Promise((res,rej)=>{ const s=document.createElement('script'); s.src=src; s.onload=res; s.onerror=()=>rej(new Error('load failed')); document.head.appendChild(s); }); }
  window.mountStrategies(document.getElementById('sv'), {}); })().catch(e => { window.__apErr = String(e && e.message || e); });
</script></body></html>` }));
    const page = await ctx.newPage();
    page.on('pageerror', () => {});                                          // page errors stay out of the public log
    await page.goto(PAGE, { waitUntil: 'domcontentloaded', timeout: 120000 });
    await page.waitForFunction(() => window.spAuto || window.__apErr, null, { timeout: 180000, polling: 1000 });
    if (await page.evaluate(() => window.__apErr || '')) { say('the panel did not load'); return 1; }
    // the ⭐ list + books arrive asynchronously (settings sync, holdings row): wait until the count is stable
    let n = -1, same = 0; const t0 = Date.now();
    while (Date.now() - t0 < 240000) { const c = await page.evaluate(() => window.spAuto.status().strategies); same = (c > 0 && c === n) ? same + 1 : 0; n = c; if (same >= 3) break; await page.waitForTimeout(3000); }
    const st = await page.evaluate(() => window.spAuto.status());
    say(`status: ${legName} leg · armed ${st.armed} · Kite ${st.connected ? 'connected' : 'NOT connected'} · cloud slicer ${st.cloud ? 'on' : 'off'} · strategies ${st.strategies}` +
        (st.blockers.length ? ' · open checks: ' + st.blockers.join(', ') : ''));
    if (!st.strategies) { say('no strategy books loaded'); await page.evaluate(m => window.spAuto.log(m, 'bad'), 'Robot: no strategy books loaded — nothing done'); code = 1; }
    else if (!st.connected) { say('Kite not connected — nothing sent'); code = 0; }
    else {
      const prep = await page.evaluate(([l, d]) => window.spAuto.prepare(l, 6 * 60000, { dry: d }), [legName, dry]);
      say('prepare: ' + (prep.ready ? 'ready' : 'NOT ready — open: ' + (prep.need || []).join(', ')));
      if (!prep.ready && !dry) {
        await page.evaluate(m => window.spAuto.log(m, 'warn'), 'Robot: not ready (' + (prep.need || []).join(', ') + ') — will retry next pass');
        await ntfy('Rebalance auto-pilot: not ready', 'The ' + legName + ' leg could not start: these checks are not green: ' + (prep.need || []).join(', ') + '. The next pass retries; open the terminal’s Saved strategies to look.', 4);
      } else {
        const res = await page.evaluate(([l, d]) => window.spAuto.run(l, { dry: d }), [legName, dry]);
        say(`run (${dry ? 'DRY — nothing sent' : 'live'}): ${res.done.length} basket(s) ${dry ? 'would go out' : 'sent'} · ${res.skipped.length} skipped · ${res.errors.length} problem(s)`);
        if (dry) {
          const lines = res.done.map(d => d.num + ' ' + d.side.toLowerCase() + ' ' + (d.orders || []).map(o => o.sym + ' ' + o.qty).join(', '));
          await page.evaluate(m => window.spAuto.log(m, 'i'), 'Dry rehearsal (' + legName + '): ' + (lines.join(' · ') || 'nothing to send') + (res.errors.length ? ' · problems: ' + res.errors.join('; ') : ''));
          await ntfy('Rebalance auto-pilot: dry rehearsal (' + legName + ')', (res.note ? res.note + '\n' : '') + (lines.join('\n') || 'Nothing would go out.') + (res.errors.length ? '\nProblems: ' + res.errors.join('; ') : '') + '\nNothing was sent.', 2);
        } else {
          // the baskets go to the cloud slicer from this page — keep it open until the box lists each one
          const want = res.done.map(d => d.num + d.side); const t1 = Date.now();
          while (want.length && Date.now() - t1 < 120000) { const w = await page.evaluate(() => window.spAuto.watch());
            if (want.every(k => w.jobs.some(j => ('#' + j.num + j.side) === k))) break; await page.waitForTimeout(5000); }
          if (res.errors.length) await ntfy('Rebalance auto-pilot: ' + res.errors.length + ' problem(s)', res.errors.join('\n') + '\nThe next pass retries what it can; see Saved strategies → Auto-pilot.', 4);
          else if (res.done.length) await ntfy('Rebalance auto-pilot: ' + legName + ' baskets sent', res.done.map(d => d.num + ' ' + d.side.toLowerCase() + ': ' + d.stocks + ' stock(s), ' + d.slices + ' slices').join('\n'), 3);
        }
      }
      // every pass: the cloud jobs' state + proceeds for finished sell baskets; problems alert (deduped through the panel log)
      const w = await page.evaluate(() => window.spAuto.watch());
      const bad = w.jobs.filter(j => j.failed || j.needsAuth), running = w.jobs.filter(j => j.status === 'running').length;
      say(`watch: ${w.jobs.length} job(s) today · ${running} running · ${bad.length} with problems · proceeds captured ${w.captured.length}`);
      if (!dry && bad.length) {
        const msg = 'Robot: ' + bad.map(j => '#' + j.num + ' ' + j.side.toLowerCase() + (j.needsAuth ? ' needs a fresh Kite login' : ' has ' + j.failed + ' failed stock(s)')).join('; ');
        const seen = await page.evaluate(() => { try { return JSON.parse(localStorage.getItem('sw_zb_amts_v1') || '{}'); } catch (e) { return {}; } });
        const recent = Object.values((seen && seen.autos) || {}).some(a => (a.log || []).some(x => x.m === msg && Date.now() - x.at < 45 * 60000));
        if (!recent) { await page.evaluate(m => window.spAuto.log(m, 'warn'), msg); await ntfy('Rebalance auto-pilot: needs a look', msg.replace(/^Robot: /, ''), 4); }
      }
    }
    await page.evaluate(() => window.spAuto.flush());
    await page.waitForTimeout(2500);
  } catch (e) { say('error: ' + String(e && e.message || e).slice(0, 160).replace(/[A-Za-z0-9_-]{24,}/g, '…')); code = 1; }
  finally { await browser.close(); say('blocked writes: ' + blocked); }
  return code;
}

export { windowNow };
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) { if (PRE) await precheck(); else process.exit(await leg()); }
