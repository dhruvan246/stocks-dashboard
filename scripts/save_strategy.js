// Publish ONE strategy to the shared Saved Strategies list (DATA_RUNBOOK §7.3).
//
//   node scripts/save_strategy.js <strategy.json>        # dry run — prints what it would send
//   node scripts/save_strategy.js <strategy.json> --push # actually publish
//
// The input JSON is {name, cfg} where cfg matches what stock-backtest.html stores:
//   {start,end,indexName,freq,lookback,topN,capital,mode,earnBasis,mcapFloor,method,sortBy,dir,filters}
//
// Uses `bt_strats_append` — the race-free single-item add the site itself calls. NEVER use
// bt_strats_set from a script: that pushes a WHOLE array and would clobber every strategy added
// from a browser since the last pull.
//
// The endpoint + publishable key + write token are read from docs/bt-sync.js so this file cannot
// drift from what the site uses. They are public by design (see the header of bt-sync.js).
'use strict';
const fs = require('fs'), path = require('path');

const ROOT = path.resolve(__dirname, '..');
const sync = fs.readFileSync(path.join(ROOT, 'docs', 'bt-sync.js'), 'utf8');
const pick = re => { const m = sync.match(re); if (!m) throw new Error('could not read ' + re + ' from bt-sync.js'); return m[1]; };
const URL = pick(/const URL = '([^']+)'/);
const ANON = pick(/const ANON = '([^']+)'/);
const WRITE = pick(/const WRITE = '([^']+)'/);
const CAP = +pick(/CAP = (\d+)/);

// identity mirrors stratIdentity() in stock-backtest.html — INCLUDES topN and lookback
const identity = c => [c.indexName || '', c.mcapFloor || 0, c.freq, c.topN, c.sortBy, c.dir,
  c.lookback || 1, c.earnBasis || 'con',
  (c.filters || []).map(x => x.field + x.op + x.val).sort().join(',')].join('|');

async function rpc(fn, body) {
  const r = await fetch(URL + '/rest/v1/rpc/' + fn, {
    method: 'POST',
    headers: { 'apikey': ANON, 'Authorization': 'Bearer ' + ANON, 'Content-Type': 'application/json' },
    body: JSON.stringify(body || {}),
  });
  const t = await r.text();
  if (!r.ok) throw new Error(fn + ' -> HTTP ' + r.status + ' ' + t.slice(0, 200));
  try { return JSON.parse(t); } catch (e) { return t; }
}

(async function () {
  const spec = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
  const list = Array.isArray(spec) ? spec : [spec];
  const push = process.argv.includes('--push');

  const remote = await rpc('bt_strats_public');
  const have = new Set((remote || []).map(s => identity(s.cfg || {})));
  console.log('shared list currently holds ' + (remote || []).length + ' strategies (cap ' + CAP + ')');

  for (const spec1 of list) {
    const entry = { id: 's' + Date.now().toString(36) + Math.floor(Math.random() * 1e4).toString(36),
                    ts: Date.now(), name: spec1.name, cfg: spec1.cfg };
    const k = identity(entry.cfg);
    if (have.has(k)) { console.log('SKIP (already saved): ' + spec1.name); continue; }
    console.log('\n' + (push ? 'PUBLISHING' : 'DRY RUN') + ': ' + entry.name);
    console.log('  identity: ' + k);
    if (!push) { console.log('  (re-run with --push to publish)'); continue; }
    const ok = await rpc('bt_strats_append', { secret: WRITE, item: entry, cap: CAP });
    console.log('  bt_strats_append -> ' + JSON.stringify(ok));
    have.add(k);
  }

  if (push) {
    const after = await rpc('bt_strats_public');
    console.log('\nverify: shared list now holds ' + (after || []).length + ' strategies');
    for (const spec1 of list) {
      const k = identity(spec1.cfg);
      const hit = (after || []).find(s => identity(s.cfg || {}) === k);
      console.log('  ' + (hit ? 'FOUND  ' : 'MISSING') + ' ' + spec1.name + (hit ? '  (id ' + hit.id + ')' : ''));
    }
  }
})().catch(e => { console.error('FAILED:', e.message); process.exit(1); });
