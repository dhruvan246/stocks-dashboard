// F&O AUTO-PILOT robot (user 2026-11-11: "Yes, build auto-pilot" — plan "B on 15%" on five stock futures).
//
// Opens the site's F&O plan panel (the SAME docs/fno-panel.js the terminal's F&O tab runs) in headless Chromium with
// the pf token and calls window.fnoAuto — so the robot runs exactly the plan + order functions behind the buttons.
// The user's choices (2026-11-11): rule check at 3:20 PM on the live pot, orders before the 3:30 close; ON until the
// user switches it off; it may do the dip buy + buy-back, the sell back to 4x and the monthly roll ("Make the five
// equal" stays a button). Two legs, every trading day:
//   roll  — 14:30 IST: any held contract ≤ 3 trading days from expiry is sold and next month's bought (its own lot).
//   rule  — dispatched for 15:05 IST so the browser is up in time; waits until 15:20 IST, runs the backtest's close
//           check on the live pot and places what is due (sell to 4x → buy-back → dip buy), all before 15:28.
// It acts ONLY while the user's switch is ON (doc.auto.on in the token-gated row <token>.fnopot); otherwise every pass
// exits in seconds at the pre-check (no browser). Passes are idempotent: a placed due is cleared in the synced row, a
// rolled contract is no longer near expiry, so the backup pass (15:15 slot) only finishes what the first could not.
//
// PRIVACY — this repo and its Actions logs are PUBLIC: this script prints counts and states only — never stock names,
// quantities, amounts, the token, the worker address or the alert topic (repo secrets PF_HOLDINGS_TOKEN,
// PF_KITE_WORKER, NTFY_TOPIC). Details go to the panel's own log (token-gated row) and the user's private ntfy topic.
// SAFETY — the page may write ONLY its own row (pf_feed_set for <token>.fnopot); every other database write is blocked.
//
//   node scripts/fno_autopilot.mjs --precheck   calendar + switch + Kite login (+ roll window), no browser; go=true|false
//   node scripts/fno_autopilot.mjs              the leg (needs `playwright` + chromium)
// env: AP_MODE=auto|dry, AP_LEG=roll|rule (else from AP_SLOT / the IST clock), AP_SLOT (dispatcher's cron string)
// test-only env: AP_BASE (site origin+path), AP_ANYTIME=1 (skip clock windows), AP_DOC_FILE (precheck reads this doc
// instead of the row), AP_NO_DB=1 (block every database call, read or write), AP_SEED_DOC / AP_DUMP_DOC (the panel's
// local doc in / out), AP_LOCAL_TEST=1 (let a localhost test page reach a localhost mock worker)
import { readFileSync, appendFileSync } from 'fs';
import { gunzipSync } from 'zlib';
import { pathToFileURL } from 'url';

const BASE = (process.env.AP_BASE || 'https://dhruvan246.github.io/stocks-dashboard').replace(/\/+$/, '');
const SUPA = 'https://nebjnsndgrhumnkuipqy.supabase.co/rest/v1/rpc/';
const ANON = 'sb_publishable_MDlQwiVc5deii91__UNeDg_z9r4Fk98';            // public (docs/sw-sync.js)
const TOKEN = (process.env.PF_HOLDINGS_TOKEN || '').trim();
const WORKER = (process.env.PF_KITE_WORKER || '').trim().replace(/\/+$/, '');
const NTFY = (process.env.NTFY_TOPIC || '').trim();
const MODE = (process.env.AP_MODE || 'auto').trim() === 'dry' ? 'dry' : 'auto';
const ANYTIME = process.env.AP_ANYTIME === '1';
const NODB = process.env.AP_NO_DB === '1';
const PRE = process.argv.includes('--precheck');
const BACKUP = /^45 9 /.test((process.env.AP_SLOT || '').trim());          // the 15:15 backup pass: no repeat login alert
const say = m => console.log('[fno-autopilot] ' + m);                       // counts and states ONLY — public log

// ---------------- calendar ----------------
const HOL = (() => { try { const d = JSON.parse(readFileSync(new URL('../docs/nse_holidays.json', import.meta.url)));
  return new Set(Object.values(d.holidays || {}).flatMap(y => Object.keys(y))); } catch (e) { return new Set(); } })();
const iso = d => d.toISOString().slice(0, 10);
const off = d => d.getUTCDay() === 0 || d.getUTCDay() === 6 || HOL.has(iso(d));
const istNow = () => new Date(Date.now() + 330 * 60000);
const istMin = () => { const d = istNow(); return d.getUTCHours() * 60 + d.getUTCMinutes(); };
const todayD = () => { const d = istNow(); return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate())); };
function tdTo(exp) {               // trading days from tomorrow up to and including expiry (same rule as the panel)
  let n = 0, t = +todayD(); const e = Date.parse(exp + 'T00:00:00Z'); if (!(e >= t)) return 0;
  while (t < e) { t += 864e5; if (!off(new Date(t))) n++; } return n;
}
function legNow() {
  const L = (process.env.AP_LEG || '').trim(); if (L === 'roll' || L === 'rule') return L;
  const slot = (process.env.AP_SLOT || '').trim();
  if (/^0 9 /.test(slot)) return 'roll';
  if (/^(35|45) 9 /.test(slot)) return 'rule';
  return istMin() < 15 * 60 ? 'roll' : 'rule';
}

// ---------------- the token-gated row, the worker, alerts ----------------
async function rpc(fn, args) {
  const r = await fetch(SUPA + fn, { method: 'POST', headers: { apikey: ANON, Authorization: 'Bearer ' + ANON, 'Content-Type': 'application/json' }, body: JSON.stringify(args) });
  if (!r.ok) throw new Error(fn + ' HTTP ' + r.status);
  const t = await r.text(); return t ? JSON.parse(t) : null;
}
async function fnoDoc() {
  if (process.env.AP_DOC_FILE) return JSON.parse(readFileSync(process.env.AP_DOC_FILE, 'utf8'));
  const row = await rpc('pf_feed_get', { token: TOKEN + '.fnopot' });
  return row && row.z ? JSON.parse(gunzipSync(Buffer.from(row.z, 'base64')).toString()) : null;
}
async function wget(path) {
  const r = await fetch(WORKER + path, { headers: { 'X-PF-Token': TOKEN } }); return { st: r.status, j: await r.json().catch(() => ({})) };
}
async function kiteStatus() {
  if (!WORKER) return { ok: false, why: 'no worker address (PF_KITE_WORKER secret missing)' };
  try { const { st, j } = await wget('/status');
    return { ok: st === 200 && !!j.connected, why: st === 200 ? (j.connected ? '' : 'Zerodha is not logged in today') : 'worker HTTP ' + st }; }
  catch (e) { return { ok: false, why: 'worker unreachable' }; }
}
async function ntfy(title, body, prio) {
  if (!NTFY) { say('alert skipped (no NTFY_TOPIC secret)'); return; }
  try { await fetch('https://ntfy.sh/' + encodeURIComponent(NTFY), { method: 'POST', body,
    headers: { Title: title, Priority: String(prio || 3), Tags: prio >= 4 ? 'warning' : 'chart_with_upwards_trend' } }); say('alert sent'); }
  catch (e) { say('alert failed'); }
}
const outSet = (k, v) => { if (process.env.GITHUB_OUTPUT) appendFileSync(process.env.GITHUB_OUTPUT, k + '=' + v + '\n'); };

// held contracts of the plan's stocks within the roll window (worker positions + the CI contract list)
async function rollCount(doc) {
  const syms = (doc.cfg && doc.cfg.syms) || ['CDSL', 'CANBK', 'POWERINDIA', 'GODREJPROP', 'BHEL'];
  let con; try { con = JSON.parse(readFileSync(new URL('../docs/fno_contracts.json', import.meta.url))); } catch (e) { return -1; }
  const exp = {}; for (const s of syms) for (const c of (con.f || {})[s] || []) exp[c[0]] = c[1];
  const { st, j } = await wget('/positions'); if (st !== 200 || !j || j.status !== 'success') return -1;
  return (j.data.net || []).filter(p => p.exchange === 'NFO' && +p.quantity > 0 && exp[p.tradingsymbol] && tdTo(exp[p.tradingsymbol]) <= 3).length;
}

// ---------------- pre-check: no browser unless there is something to do ----------------
async function precheck() {
  if (!TOKEN) { say('PF_HOLDINGS_TOKEN missing'); outSet('go', 'false'); process.exit(1); }
  const leg = legNow(), day = iso(todayD());
  if (!HOL.size || ![...HOL].some(h => h.startsWith(day.slice(0, 4)))) say('⚠ no NSE holiday list for ' + day.slice(0, 4) + ' — dates assume weekends only');
  if (MODE === 'dry') { say('dry rehearsal of the ' + leg + ' leg — nothing will be sent'); outSet('go', 'true'); return; }
  if (off(todayD())) { say('not a trading day — nothing to do'); outSet('go', 'false'); return; }
  const doc = await fnoDoc();
  if (!doc || !doc.auto || !doc.auto.on) { say(leg + ' leg: the auto-pilot is OFF — nothing to do'); outSet('go', 'false'); return; }
  if (!doc.rec) { say(leg + ' leg: switched on but the pot is not started — nothing to do'); outSet('go', 'false'); return; }
  if (leg === 'rule' && doc.st && doc.st.evalDate === day && !(doc.st.due || []).some(x => x.d === day)) {
    say('rule leg: today\'s check is done and nothing is open — nothing to do'); outSet('go', 'false'); return; }
  const k = await kiteStatus();
  if (!k.ok) {
    say(leg + ' leg: switched on, but ' + k.why + (BACKUP ? '' : ' — alerting'));
    if (!BACKUP) await ntfy('F&O auto-pilot: log in to Zerodha', 'The F&O auto-pilot is ON, but ' + k.why + '. Log in from the terminal' +
      (leg === 'roll' ? ' before 3:15 PM, or the 3:20 PM rule check cannot run.' : ' now — the 3:20 PM rule check is waiting; after 3:28 PM it is skipped today.'), 5);
    outSet('go', 'false'); return;
  }
  if (leg === 'roll') {
    const n = await rollCount(doc);
    if (n === 0) { say('roll leg: no contract within 3 trading days of expiry — nothing to do'); outSet('go', 'false'); return; }
    say('roll leg: ' + (n < 0 ? 'roll window unknown (positions/contract list unreadable) — the panel decides' : n + ' contract(s) to roll'));
  } else say('rule leg: switched on, Zerodha connected — starting the browser');
  outSet('go', 'true');
}

// ---------------- the leg, through the panel's own code ----------------
async function legRun() {
  const leg = legNow(), dry = MODE === 'dry';
  if (!TOKEN || !WORKER) { say('missing secret(s): ' + [!TOKEN && 'PF_HOLDINGS_TOKEN', !WORKER && 'PF_KITE_WORKER'].filter(Boolean).join(', ')); return 1; }
  const { chromium } = await import('playwright');
  const browser = await chromium.launch({ args: ['--no-sandbox', '--disable-dev-shm-usage'].concat(
    process.env.AP_LOCAL_TEST === '1' ? ['--disable-features=LocalNetworkAccessChecks,PrivateNetworkAccessChecks,BlockInsecurePrivateNetworkRequests'] : []) });
  let blocked = 0, code = 0;
  try {
    const ctx = await browser.newContext({ serviceWorkers: 'block' });
    const HOST = new URL(SUPA).origin;
    await ctx.route(HOST + '/**', async route => {
      const req = route.request(), u = new URL(req.url());
      if (!NODB && u.pathname.startsWith('/rest/v1/rpc/')) {
        const fn = u.pathname.slice('/rest/v1/rpc/'.length);
        if (fn === 'pf_feed_get') return route.continue();
        if (fn === 'pf_feed_set') { let b = {}; try { b = JSON.parse(req.postData() || '{}'); } catch (e) {}
          if (b.token === TOKEN + '.fnopot') return route.continue(); }
      }
      blocked++; return route.abort();
    });
    await ctx.addInitScript(([tok, w, seed]) => { try { localStorage.setItem('pf_token', tok); localStorage.setItem('pf_kite_worker', w);
      if (seed && !sessionStorage.getItem('seeded')) { localStorage.setItem('sw_fno_pot_v1', seed); sessionStorage.setItem('seeded', '1'); } } catch (e) {} },
      [TOKEN, WORKER, process.env.AP_SEED_DOC ? readFileSync(process.env.AP_SEED_DOC, 'utf8') : '']);
    // a same-origin page that mounts ONLY the F&O panel, with the terminal's acctFetch (worker + X-PF-Token)
    const PAGE = BASE + '/__fnoauto.html';
    await ctx.route(PAGE, r => r.fulfill({ contentType: 'text/html', body: `<!doctype html><html><head><meta charset="utf-8"><title>fno auto-pilot</title></head>
<body><div id="fv"></div><script>
function acctFetch(path, opts){ const w = localStorage.getItem('pf_kite_worker') || ''; if (!w) return Promise.resolve({ st: -1, j: null });
  const o = Object.assign({ cache: 'no-store' }, opts || {}); o.headers = Object.assign({ 'X-PF-Token': localStorage.getItem('pf_token') || '' }, o.headers || {});
  return fetch(w.replace(/\\/+$/, '') + path, o).then(r => r.json().then(j => ({ st: r.status, j }), () => ({ st: r.status, j: null }))).catch(() => ({ st: 0, j: null })); }
const s = document.createElement('script'); s.src = './fno-panel.js?v=' + Date.now();
s.onload = () => window.mountFno(document.getElementById('fv'), { acctFetch, ktoast: () => {}, kiteSend: () => {},
  ipBlocked: m => /no ips|static ip|whitelist|allowed ip/i.test(m || ''), refresh: () => {} });
s.onerror = () => { window.__apErr = 'load failed'; }; document.head.appendChild(s);
</script></body></html>` }));
    const page = await ctx.newPage();
    page.on('pageerror', () => {});                                          // page errors stay out of the public log
    await page.goto(PAGE, { waitUntil: 'domcontentloaded', timeout: 120000 });
    await page.waitForFunction(() => (window.fnoAuto && window.fnoAuto.status().ready) || window.__apErr, null, { timeout: 120000, polling: 1000 });
    if (await page.evaluate(() => window.__apErr || '')) { say('the panel did not load'); return 1; }
    const st = await page.evaluate(() => window.fnoAuto.status());
    say(`status: ${leg} leg · switch ${st.on ? 'ON' : 'OFF'} · pot ${st.started ? (st.potOk ? 'ok' : 'needs today\'s value') : 'not started'} · Zerodha ${st.err ? 'NOT ok (' + st.err + ')' : 'ok'} · ${st.contracts} contract(s) · roll window ${st.rollDue}`);
    // rule leg: decide at 15:20 IST with the live pot (the user's choice), not earlier
    if (leg === 'rule' && !dry && !ANYTIME) {
      const wait = (15 * 60 + 20 - istMin()) * 60000 - istNow().getUTCSeconds() * 1000;
      if (wait > 0) { say('waiting ' + Math.round(wait / 1000) + ' s for 15:20 IST'); await page.waitForTimeout(wait); }
    }
    const res = await page.evaluate(([l, d, a]) => window.fnoAuto.run(l, { dry: d, anyTime: a }), [leg, dry, ANYTIME]);
    const nOrd = res.done.reduce((t, x) => t + (dry ? x.orders.length : (x.ok || 0)), 0);
    say(`run (${dry ? 'DRY — nothing sent' : 'live'}): ${res.done.length} action(s) · ${nOrd} order(s) ${dry ? 'would go out' : 'placed'} · ${res.skipped.length} skipped · ${res.errors.length} problem(s)` + (res.note && /OFF|window|not started/.test(res.note) ? ' · ' + res.note : ''));
    const name = { roll: 'roll', sell: 'sell to 4x', bb: 'buy-back', dip: 'dip buy' };
    const body = (res.note ? res.note + '\n' : '') + res.done.map(x => (name[x.k] || x.k) + (dry ? ' (would place): ' : ': ' + x.ok + ' of ' + x.n + ' orders placed — ') + (x.orders || []).join(', ')).join('\n') +
      (res.skipped.length ? '\nSkipped: ' + res.skipped.join('; ') : '') + (res.errors.length ? '\nProblems: ' + res.errors.join('; ') : '');
    if (dry) {
      await page.evaluate(m => window.fnoAuto.log(m), 'Auto-pilot dry rehearsal (' + leg + '): ' + (res.done.map(x => x.k + ' ' + x.orders.join(', ')).join(' · ') || 'nothing would be placed') + (res.errors.length ? ' · problems: ' + res.errors.join('; ') : ''));
      await ntfy('F&O auto-pilot: dry rehearsal (' + leg + ')', body + '\nNothing was sent.', 2);
    } else if (res.errors.length) {
      await page.evaluate(m => window.fnoAuto.log(m), 'Auto-pilot ' + leg + ' problems: ' + res.errors.join('; '));
      await ntfy('F&O auto-pilot: needs a look (' + leg + ')', body + '\n' + (leg === 'rule' && istMin() < 15 * 60 + 25 ? 'The 3:17 PM backup pass retries what it can.' : 'Open the terminal → F&O plan.'), 5);
      code = 0;
    } else if (res.done.length) await ntfy('F&O auto-pilot: ' + res.done.map(x => name[x.k] || x.k).join(' + ') + ' placed', body, 4);
    else if (leg === 'rule' && /outside/.test(res.note || '')) await ntfy('F&O auto-pilot: started too late', 'The robot only got going at ' + istNow().toISOString().slice(11, 16) +
      ' IST, after the 3:28 PM cut-off, so it placed nothing today. The 3:35 PM check on the F&O tab and the 3:50 PM alert still show anything that is due.', 4);
    else if (leg === 'rule' && res.note && !/OFF|window|not started/.test(res.note)) await ntfy('F&O auto-pilot: 3:20 PM check', body, 2);
    await page.evaluate(() => window.fnoAuto.flush());
    await page.waitForTimeout(2500);
    if (process.env.AP_DUMP_DOC) (await import('fs')).writeFileSync(process.env.AP_DUMP_DOC, await page.evaluate(() => localStorage.getItem('sw_fno_pot_v1') || 'null'));
  } catch (e) { say('error: ' + String(e && e.message || e).slice(0, 160).replace(/[A-Za-z0-9_-]{24,}/g, '…')); code = 1; }
  finally { await browser.close(); say('blocked database calls: ' + blocked); }
  return code;
}

export { tdTo, legNow };
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) { if (PRE) await precheck(); else process.exit(await legRun()); }
