// Headless bake of the FULL-HISTORY window — in Node, no browser.
//
// WHY NOT A BROWSER: bake_waves.mjs drives saved-strategies.html in Playwright. That works for the
// short market-cycle waves but could never finish this one: every batch died ~40s in, during the data
// load, before a single backtest ran (2026-08-03: five batches, all stuck at 5/32). Blocking the
// service worker fixed one cause and the batches still died. Meanwhile the compute itself was proven
// healthy — a 24-year backtest takes ~68s at a flat 631 MB heap, so all of them fit in ~40 min.
// The engine is DOM-FREE on purpose, so Node can run it directly: no renderer, no page reload, no
// batch/resume machinery, no memory ceiling but Node's own.
//
// IDENTITY: keys come from docs/bt-identity.js — the SAME file the page loads. A key computed a
// second way here would not error, it would just leave the site saying "not baked yet" forever.
//
// Writes its OWN Supabase snapshot row (bt_snapshots id 'waves_full'), NOT the waves' row. Sharing
// one row cost a full bake on 2026-08-03: the browser-side wave bake rewrites the WHOLE payload from
// the copy it read at page load, so a bake-waves run that started after this one published reverted
// `full` to a stale checkpoint. And bake-waves fires off the FUNDAMENTALS refresh too — it runs every
// ~30 min all day, so there is no quiet window to slot into and no point chaining behind it (that
// would also mean running this 29-minute job dozens of times a day). Separate rows, no coordination.
//
// Run: node scripts/bake_full.mjs          (env: BAKE_WINDOW=full, BAKE_SAVE_EVERY=5)

import { readFileSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const HERE = dirname(fileURLToPath(import.meta.url)), ROOT = dirname(HERE), DOCS = join(ROOT, 'docs');
const SITE = process.env.BAKE_SITE || 'https://dhruvan246.github.io/stocks-dashboard/';
const SAVE_EVERY = +process.env.BAKE_SAVE_EVERY || 5;
const SNAP_ID = 'waves_full';   // OUR row — the waves' row is rewritten wholesale by the browser bake

// Supabase config is READ OUT OF bt-sync.js rather than copied — one place to rotate a key.
const SYNC = readFileSync(join(DOCS, 'bt-sync.js'), 'utf8');
const pick = re => { const m = re.exec(SYNC); if (!m) throw new Error('bt-sync.js: cannot find ' + re); return m[1]; };
const SB_URL = pick(/const URL\s*=\s*'([^']+)'/);
const SB_ANON = pick(/const ANON\s*=\s*'([^']+)'/);
const SB_WRITE = pick(/const WRITE\s*=\s*'([^']+)'/);

async function rpc(name, body) {
  const r = await fetch(`${SB_URL}/rest/v1/rpc/${name}`, {
    method: 'POST',
    headers: { apikey: SB_ANON, Authorization: `Bearer ${SB_ANON}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(body || {}),
  });
  if (!r.ok) throw new Error(`${name}: HTTP ${r.status} ${(await r.text()).slice(0, 200)}`);
  const t = await r.text();
  return t ? JSON.parse(t) : null;
}

// ---- shims the engine expects from a browser ----
// SLICE_BASE reads location at load time; nothing else here touches it (that path is the stock page's).
globalThis.location = { hostname: 'ci', protocol: 'https:', search: '' };
// IndexedDB is absent — the engine's cache helpers are already try/catch'd and degrade to no caching,
// which is right for CI: a fresh runner has nothing cached anyway.
const nativeFetch = globalThis.fetch;
globalThis.fetch = (u, o) => nativeFetch(typeof u === 'string' && /^\.?\//.test(u) ? new URL(u.replace(/^\.\//, ''), SITE).href : u, o);

const engineSrc = readFileSync(join(DOCS, 'backtest-engine.js'), 'utf8');
const identSrc = readFileSync(join(DOCS, 'bt-identity.js'), 'utf8');

// The engine declares everything with const/let at top level, so it and the driver must share ONE
// scope — hence a single Function body rather than separate evaluations.
const driver = `
return (async function () {
  const log = (...a) => console.log('[bake-full]', ...a);

  log('loading market data…');
  let t = Date.now();
  await loadEngineData(() => {});
  log(\`engine ready in \${Math.round((Date.now()-t)/1000)}s · data \${SF.start} → \${SF.end} · \${SF.nTot} stocks\`);

  const from = CTX.clampDate(SF.dailyFrom || SF.start, SF.start, SF.end);
  const to = SF.end;
  log(\`window \${from} → \${to}\`);

  t = Date.now();
  await ensureHistoryFor(from);
  log(\`deep history in \${Math.round((Date.now()-t)/1000)}s · \${Object.keys(SERIES).length} symbols\`);

  const groups = [...bakeGroups(CTX.strategies).values()];
  log(\`\${groups.length} strategies to bake\`);
  if (!groups.length) return { skipped: true };

  const results = {};
  let done = 0;
  for (const g of groups) {
    const cfg = { ...g.cfg, start: from, end: to, capital: g.cfg.capital || 100000 };
    const t0 = Date.now();
    try {
      const r = simulate(cfg);
      results[identityKey(g.cfg)] = { ret: (r.finalV / cfg.capital - 1) * 100, cagr: r.cagr,
                                      maxDD: r.maxDD, finalV: r.finalV, topN: cfg.topN };
      log(\`\${++done}/\${groups.length} \${g.name || identityKey(g.cfg)} — CAGR \${r.cagr.toFixed(2)}% (\${Math.round((Date.now()-t0)/1000)}s)\`);
    } catch (e) {
      done++; log(\`\${done}/\${groups.length} FAILED \${g.name || ''}: \${e && e.message || e}\`);
    }
    // Checkpoint into the snapshot's pending slot so a timeout still leaves progress to promote; the
    // page only ever displays promoted results, so a partial checkpoint is never shown as complete.
    if (done % CTX.saveEvery === 0) await CTX.checkpoint(results, from, to);
  }
  return { results, from, to, end: SF.end, n: groups.length, done };
})();
`;

async function main() {
  console.log('[bake-full] fetching saved strategies…');
  const strategies = await rpc('bt_strats_public');
  console.log(`[bake-full] ${Array.isArray(strategies) ? strategies.length : 0} saved strategies`);

  const session = process.env.GITHUB_RUN_ID || String(Date.now());

  // Checkpoints keep the last COMPLETE result set intact: they write to `pending`, which the page
  // never displays, so an interrupted run can't replace good numbers with a partial set.
  const checkpoint = async (results, from, to) => {
    const prev = (await rpc('bt_snap_get', { snap_id: SNAP_ID })) || {};
    const payload = Object.assign({}, prev, { pending: results, pendingSess: session, start: from, end: to });
    try { await rpc('bt_snap_set', { secret: SB_WRITE, snap_id: SNAP_ID, payload }); console.log(`[bake-full] checkpoint ${Object.keys(results).length}`); }
    catch (e) { console.warn('[bake-full] checkpoint failed:', e.message); }
  };

  const CTX = {
    strategies: Array.isArray(strategies) ? strategies : [],
    clampDate: (d, lo, hi) => (d < lo ? lo : d > hi ? hi : d),
    saveEvery: SAVE_EVERY,
    checkpoint,
  };

  const t0 = Date.now();
  const out = await new Function('CTX', `'use strict';\n${engineSrc}\n${identSrc}\n${driver}`)(CTX);
  if (out.skipped) { console.log('[bake-full] no strategies — nothing to bake'); return; }

  const n = Object.keys(out.results).length;
  if (!n) throw new Error('no strategies computed — refusing to publish an empty snapshot');

  // PROMOTE: this row IS the full-history result, so the complete set simply replaces it (pending
  // dropped). Nothing else writes this row, so there is no merge to do and nothing to race with.
  const payload = { start: out.from, end: out.to, topN: null, results: out.results, sess: session, date: out.end };
  const ok = await rpc('bt_snap_set', { secret: SB_WRITE, snap_id: SNAP_ID, payload });
  console.log(`[bake-full] ${ok === true ? 'PUBLISHED' : 'publish returned ' + JSON.stringify(ok)} — ${n}/${out.n} strategies, ${out.from} → ${out.to}, ${Math.round((Date.now() - t0) / 60000)} min`);
  if (ok !== true) process.exitCode = 1;
}

main().catch(e => { console.error('[bake-full] fatal:', e && e.stack || e); process.exit(1); });
