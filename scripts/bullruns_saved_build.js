#!/usr/bin/env node
'use strict';
/* ============================================================================
   bullruns_saved_build.js — turn _bullsaved_raw.json into the two "our saved
   strategies" views for docs/bull-runs.html.

   Same two questions the grid cards ask (consistent across all four bull runs /
   across the latest two), same five measured windows — but the population is the
   46 shared saved strategies, so a "rank" here is a placing WITHIN that set, not
   within the 5.44M-combo pool. Written to its OWN file so a grid rebuild of
   bull_runs.json can never silently drop these cards.

     node scripts/bullruns_saved_build.js
   ==========================================================================*/
const fs = require('fs'), path = require('path');
const ROOT = path.join(__dirname, '..');
const RAW = JSON.parse(fs.readFileSync(path.join(ROOT, 'scripts', '_bullsaved_raw.json'), 'utf8'));

/* Basis-dependence, same rule the grid build uses (gridmega_phases_build.js:99):
   a strategy consults earnings only if its sort field or one of its filters is a FUND field —
   otherwise it scores identically under std and con and the basis chip must say so. */
const FUND_SET = new Set(['profitYoyPct', 'profitBase', 'profitAccel', 'profitTTM', 'profitStreak', 'postDrift', 'composite']);

/* Save serials — byte-for-byte the rule saved-strategies.html renders (buildSerials, line 691),
   so a row here and the card there carry the SAME DDMMYY-NN. */
function buildSerials(items) {
  const m = new Map(), perDay = {};
  items.slice().sort((a, b) => (a.ts || 0) - (b.ts || 0)).forEach(it => {
    const d = new Date(it.ts || 0);
    const day = String(d.getDate()).padStart(2, '0') + String(d.getMonth() + 1).padStart(2, '0') + String(d.getFullYear() % 100).padStart(2, '0');
    perDay[day] = (perDay[day] || 0) + 1;
    m.set(it.id, day + '-' + String(perDay[day]).padStart(2, '0'));
  });
  return m;
}
const SN = buildSerials(RAW.rows);

const LEGKEYS = RAW.legs.filter(w => w.k !== 'cur').map(w => w.k);
const N = RAW.rows.length;

const base = RAW.rows.map(r => {
  const c = r.cfg || {};
  const fields = [c.sortBy].concat((c.filters || []).map(f => f.field));
  const dep = fields.some(f => FUND_SET.has(f));
  return {
    sn: SN.get(r.id), nm: r.name, id: r.id,
    d: c.dir, s: c.sortBy, tn: c.topN, mth: c.method || 'reset',
    b: dep ? (c.earnBasis === 'std' ? 'std' : 'con') : 'any',
    f: (c.filters || []).map(f => f.field + f.op + f.val).join(' & ') || '(no filters)',
    r: r.r, dd: r.dd,
  };
});

/* rank 1 = best return in that window, among these N */
const rankIn = {};
for (const k of LEGKEYS.concat(['cur'])) {
  const order = base.map((_, i) => i).sort((a, b) => base[b].r[k] - base[a].r[k]);
  const m = new Array(N); order.forEach((idx, pos) => m[idx] = pos + 1);
  rankIn[k] = m;
}

function view(label, keys, blurb) {
  const rows = base.map((b, i) => {
    const ranks = keys.map(k => rankIn[k][i]);
    return {
      sn: b.sn, nm: b.nm, d: b.d, s: b.s, b: b.b, f: b.f, tn: b.tn, mth: b.mth,
      rets: keys.map(k => b.r[k]),
      ranks,
      worst: Math.max(...ranks),
      sum: ranks.reduce((x, y) => x + y, 0),
      cur: b.r.cur, curRank: rankIn.cur[i],
      dd: Math.max(...keys.map(k => b.dd[k])),
    };
  });
  // worst placing first; ties broken by TOTAL placing, then by serial. The live run never
  // enters the ordering — it is a read-out column only.
  rows.sort((a, b) => a.worst - b.worst || a.sum - b.sum || a.sn.localeCompare(b.sn));
  return { label, keys, blurb, rows };
}

const OUT = {
  built: RAW.built, dataEnd: RAW.dataEnd, n: N,
  legMeta: RAW.legMeta,
  views: {
    saved4: view('Our ' + N + ' · all four', LEGKEYS),
    saved2: view('Our ' + N + ' · latest two', LEGKEYS.slice(-2)),
  },
};
const F = path.join(ROOT, 'docs', 'bull_runs_saved.json');
fs.writeFileSync(F, JSON.stringify(OUT));
console.log('wrote docs/bull_runs_saved.json — ' + N + ' strategies, views: ' + Object.keys(OUT.views).join(', '));
for (const k of Object.keys(OUT.views)) {
  const v = OUT.views[k], t = v.rows[0];
  console.log('  ' + k + ' [' + v.keys.join(',') + '] best worst-rank ' + t.worst + '/' + N +
    ' → ' + t.sn + ' ' + t.d + '-' + t.s + ' | rets ' + t.rets.map(x => x + '%').join(' ') + ' | now ' + t.cur + '%');
}
