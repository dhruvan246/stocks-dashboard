// Headless verification of docs/trip.html (runbook §39, §139). Serves docs/ locally; every cross-origin
// request is intercepted (the sandbox proxy blocks tile/CDN/Supabase hosts) and LOGGED so the
// tile-URL template can still be checked. Nominatim is answered with a canned jsonv2 payload.
// Run:  python3 -m http.server 8765 --directory docs --bind 127.0.0.1 &   then   node scripts/verify_trip_map.js
// Screenshots land in $TM_SHOTS (default: <tmpdir>/trip-map-shots). Exit code 0 = every check passed.
let chromium;
try { ({ chromium } = require('playwright')); }
catch (e) { ({ chromium } = require('/opt/node22/lib/node_modules/playwright')); }   // the cloud sandbox's global install
const path = require('path'), fs = require('fs'), os = require('os');
const BASE = process.env.TM_BASE || 'http://127.0.0.1:8765';
const SHOTS = process.env.TM_SHOTS || path.join(os.tmpdir(), 'trip-map-shots');
fs.mkdirSync(SHOTS, { recursive: true });
const fails = [];
function check(name, cond, detail) { const ok = !!cond; console.log((ok ? 'PASS ' : 'FAIL ') + name + (detail !== undefined ? '  -> ' + detail : '')); if (!ok) fails.push(name); }

(async () => {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({ viewport: { width: 1280, height: 800 }, deviceScaleFactor: 1 });
  const external = [];
  await ctx.route('**/*', route => {
    const u = route.request().url();
    if (u.startsWith(BASE)) return route.continue();
    external.push(u);
    if (u.startsWith('https://nominatim.openstreetmap.org/')) {
      return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify([
        { place_id: 1, lat: '34.9671', lon: '135.7727', name: 'Fushimi Inari Taisha', display_name: 'Fushimi Inari Taisha, Fushimi, Kyoto, Japan' },
        { place_id: 2, lat: '35.0116', lon: '135.7681', name: 'Kyoto', display_name: 'Kyoto, Japan' } ]) });
    }
    return route.abort();
  });
  const page = await ctx.newPage();
  const errors = [], consoleErr = [];
  page.on('pageerror', e => errors.push(String(e)));
  page.on('console', m => { if (m.type() === 'error') consoleErr.push(m.text()); });
  page.on('dialog', d => d.accept());

  // ---------- 1. fresh visitor, default (dark) ----------
  await page.goto(BASE + '/trip.html', { waitUntil: 'load' });
  await page.waitForTimeout(800);
  const today = await page.evaluate(() => { const d = new Date(), p = n => (n < 10 ? '0' : '') + n; return d.getFullYear() + '-' + p(d.getMonth() + 1) + '-' + p(d.getDate()); });
  console.log('today (browser local) =', today, '| data-theme =', await page.getAttribute('html', 'data-theme'));
  check('no page JS errors on load', errors.length === 0, errors.join(' | '));
  const nonNet = consoleErr.filter(t => !/Failed to load resource|net::ERR_FAILED|ERR_BLOCKED/i.test(t));
  check('no console errors other than blocked cross-origin loads', nonNet.length === 0, nonNet.join(' | '));
  console.log('   blocked-resource console lines:', consoleErr.length, ' external hosts seen:', [...new Set(external.map(u => new URL(u).host))].join(', '));
  check('tile requests use CARTO dark_all in dark theme', external.some(u => /basemaps\.cartocdn\.com\/dark_all\/\d+\/\d+\/\d+/.test(u)), external.find(u => /cartocdn/.test(u)));
  check('22 sample stops in the list', (await page.$$('#list .tm-stop[data-id]')).length === 22, (await page.$$('#list .tm-stop[data-id]')).length);
  check('22 pins on the map', (await page.$$('#map .tm-pin-wrap .tm-pin')).length === 22);
  const routes = await page.evaluate(() => ({ past: document.querySelectorAll('#map path.tm-route.past').length, future: document.querySelectorAll('#map path.tm-route.future').length, next: document.querySelectorAll('#map path.tm-route.next').length, glow: document.querySelectorAll('#map path.tm-route.glow').length }));
  check('route legs: 9 past (6 visited + 3 within today), 11 future, 1 next, 12 glow', routes.past === 9 && routes.future === 11 && routes.next === 1 && routes.glow === 12, JSON.stringify(routes));
  const pinStates = await page.evaluate(() => ({ past: document.querySelectorAll('#map .tm-pin.past').length, today: document.querySelectorAll('#map .tm-pin.today').length, future: document.querySelectorAll('#map .tm-pin.future').length }));
  check('pins: 6 past, 4 today, 12 future', pinStates.past === 6 && pinStates.today === 4 && pinStates.future === 12, JSON.stringify(pinStates));
  check('8 day chips (All + 7 days)', (await page.$$('#days .tm-day')).length === 8);
  check('today chip is marked', (await page.$$('#days .tm-day.today')).length === 1);
  const stats = await page.textContent('#stats');
  check('stats show 6/22 visited', /6\s*\/\s*22/.test(stats.replace(/\s+/g, ' ')), stats.replace(/\s+/g, ' '));
  const km = await page.evaluate(() => [...document.querySelectorAll('#stats b')].map(b => b.textContent.replace(/\s+/g, ' ').trim()));
  check('distance tiles are numeric (not NaN/undefined)', km.length === 3 && km.every(t => /^\d/.test(t)) && !/NaN|undefined/.test(km.join()), km.join(' | '));
  check('sample badge shown, title set', (await page.isVisible('#badge')) && (await page.textContent('#tripName')).includes('Japan'));
  check('no glossary injected into the map stage', (await page.$$('main .sw-gloss')).length === 0);
  check('nav hides Trip Map for a non-owner', !(await page.evaluate(() => (document.querySelector('header nav') || {}).textContent || '')).includes('Trip Map'));
  check('footer injected by theme.js is present', (await page.$$('.sw-footer')).length === 1);
  const hdrH = await page.evaluate(() => document.querySelector('header').getBoundingClientRect().height);
  check('header is 56px tall without Tailwind', Math.round(hdrH) === 57 || Math.round(hdrH) === 56, hdrH);
  check('page body does not scroll sideways (desktop)', await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth));
  const stage = await page.evaluate(() => { const r = document.querySelector('main.tm-stage').getBoundingClientRect(); return { top: r.top, h: r.height, vh: innerHeight }; });
  check('map stage fills viewport below header', Math.round(stage.top) <= 57 && Math.round(stage.top + stage.h) === stage.vh, JSON.stringify(stage));
  await page.screenshot({ path: path.join(SHOTS, '01-desktop-dark.png') });

  // ---------- 2. select a stop from the list -> popup ----------
  const id11 = await page.getAttribute('#list .tm-stop[data-id]:nth-of-type(1)', 'data-id');
  await page.click('#list .tm-stop[data-id="' + id11 + '"]');
  await page.waitForTimeout(1200);
  check('popup opens for the selected stop', (await page.$$('.leaflet-popup .tm-pop-n')).length === 1, await page.textContent('.leaflet-popup .tm-pop-n').catch(() => 'none'));
  check('selected list row + pin highlighted', (await page.$$('#list .tm-stop.on')).length === 1 && (await page.$$('#map .tm-pin.on')).length === 1);
  const dirHref = await page.getAttribute('.leaflet-popup a.tm-btn', 'href');
  check('Directions link carries lat,lng', /google\.com\/maps\/dir\/\?api=1&destination=\d+\.\d+,\d+\.\d+/.test(dirHref), dirHref);
  await page.screenshot({ path: path.join(SHOTS, '02-popup.png') });

  // ---------- 3. day chip filter ----------
  await page.click('#days .tm-day.today');
  await page.waitForTimeout(900);
  check('today chip active after click', (await page.$$('#days .tm-day.today.on')).length === 1);
  check('list shows only today (4 stops)', (await page.$$('#list .tm-stop[data-id]')).length === 4);
  check('map still shows all 22 pins while a day is focused', (await page.$$('#map .tm-pin')).length === 22);
  await page.click('#days .tm-day.today');   // toggle back
  await page.waitForTimeout(600);
  check('tapping the active day again restores the whole trip', (await page.$$('#list .tm-stop[data-id]')).length === 22);

  // ---------- 4. add a stop via search (mocked Nominatim) ----------
  await page.click('#btnAdd');
  check('editor opens', await page.isVisible('#modal'));
  check('editor defaults the day to today', (await page.inputValue('#fDay')) === today);
  await page.click('#fSave');
  check('validation blocks an empty stop', await page.isVisible('#fErr'), await page.textContent('#fErr'));
  await page.fill('#fQ', 'Fushimi Inari');
  await page.click('#fSearch');
  await page.waitForSelector('#fRes button');
  check('search results rendered (2)', (await page.$$('#fRes button')).length === 2);
  await page.click('#fRes button:nth-of-type(1)');
  check('picking a result fills location + name', (await page.textContent('#fLoc')).includes('34.9671') && (await page.inputValue('#fName')) === 'Fushimi Inari Taisha');
  await page.fill('#fName', 'Test stop via search');
  await page.fill('#fTime', '20:15');
  await page.fill('#fNote', 'added by the smoke test');
  await page.click('#fSave');
  await page.waitForTimeout(1200);
  check('editor closed after save', !(await page.isVisible('#modal')));
  check('23 stops after adding', (await page.$$('#list .tm-stop[data-id]')).length === 23);
  check('new stop is selected + popup open', (await page.textContent('.leaflet-popup .tm-pop-n').catch(() => '')) === 'Test stop via search');
  check('sample badge gone (trip is now the user\'s)', !(await page.isVisible('#badge')));
  const saved = await page.evaluate(() => JSON.parse(localStorage.getItem('tm_trip_v1') || 'null'));
  check('trip persisted to localStorage with sample=false', saved && saved.sample === false && saved.stops.length === 23);
  await page.reload({ waitUntil: 'load' }); await page.waitForTimeout(700);
  check('23 stops survive a reload', (await page.$$('#list .tm-stop[data-id]')).length === 23);
  check('no JS errors after reload', errors.length === 0, errors.join(' | '));

  // ---------- 5. edit via popup, then pick-on-map, then delete ----------
  const newId = saved.stops[22].id;
  await page.click('#list .tm-stop[data-id="' + newId + '"]');
  await page.waitForTimeout(1100);
  await page.click('.leaflet-popup [data-act="edit"]');
  check('edit dialog shows Delete + prefilled name', (await page.isVisible('#fDel')) && (await page.inputValue('#fName')) === 'Test stop via search');
  await page.click('#fPick');
  check('pick mode: modal hidden, pick bar shown', !(await page.isVisible('#modal')) && (await page.isVisible('#pickbar')));
  const mapBox = await page.locator('#map').boundingBox();
  await page.mouse.click(mapBox.x + mapBox.width * 0.75, mapBox.y + mapBox.height * 0.5);
  await page.waitForTimeout(300);
  check('map click returns to the editor with a picked location', (await page.isVisible('#modal')) && (await page.textContent('#fLoc')).includes('picked on the map'));
  await page.click('#fSave'); await page.waitForTimeout(900);
  const moved = await page.evaluate(id => JSON.parse(localStorage.getItem('tm_trip_v1')).stops.find(s => s.id === id), newId);
  check('picked coordinates saved (differ from the searched point)', moved && (Math.abs(moved.lat - 34.9671) > 0.0001 || Math.abs(moved.lng - 135.7727) > 0.0001), JSON.stringify([moved.lat, moved.lng]));
  await page.click('#list .tm-stop[data-id="' + newId + '"]'); await page.waitForTimeout(1100);
  await page.click('.leaflet-popup [data-act="del"]'); await page.waitForTimeout(500);
  check('delete from popup -> 22 stops', (await page.$$('#list .tm-stop[data-id]')).length === 22);

  // ---------- 6. window change ----------
  await page.click('#btnWin');
  await page.fill('#wPast', '1'); await page.fill('#wFuture', '1'); await page.click('#wSave');
  await page.waitForTimeout(800);
  check('window 1/1 -> 10 stops shown, 4 day chips', (await page.$$('#list .tm-stop[data-id]')).length === 10 && (await page.$$('#days .tm-day')).length === 4);
  check('12 stops listed as outside the window', (await page.textContent('#list details.tm-out summary')).startsWith('12 more'));
  check('window note updated', (await page.textContent('#winNote')) === '1 day back · 1 day ahead');
  await page.click('#btnWin'); await page.fill('#wPast', '2'); await page.fill('#wFuture', '4'); await page.click('#wSave'); await page.waitForTimeout(500);

  // ---------- 7. reset to sample / start blank ----------
  await page.click('#btnMode');   // "Reset to sample" (dialog auto-accepted)
  await page.waitForTimeout(600);
  check('reset -> sample badge back, 22 stops, nothing in localStorage', (await page.isVisible('#badge')) && (await page.$$('#list .tm-stop[data-id]')).length === 22 && (await page.evaluate(() => localStorage.getItem('tm_trip_v1'))) === null);
  await page.click('#btnMode');   // "Start my own trip"
  await page.waitForTimeout(600);
  check('blank trip -> empty state, 0 pins, no NaN in stats', (await page.$$('#list .tm-blank')).length === 1 && (await page.$$('#map .tm-pin')).length === 0 && !/NaN|undefined/.test(await page.textContent('#stats')));
  await page.screenshot({ path: path.join(SHOTS, '03-blank.png') });
  await page.click('#blankSample'); await page.waitForTimeout(600);
  check('load sample from the empty state', (await page.$$('#list .tm-stop[data-id]')).length === 22);

  // ---------- 8. light theme -> tiles swap ----------
  const before = external.length;
  await page.click('#sw-theme-switch button[data-theme="light"]');
  await page.waitForTimeout(900);
  check('html data-theme=light', (await page.getAttribute('html', 'data-theme')) === 'light');
  check('tile layer switched to voyager', external.slice(before).some(u => /cartocdn\.com\/rastertiles\/voyager\//.test(u)));
  await page.screenshot({ path: path.join(SHOTS, '04-desktop-light.png') });
  await page.click('#sw-theme-switch button[data-theme="dark"]'); await page.waitForTimeout(300);

  // ---------- 9. owner sees the nav entry ----------
  await page.evaluate(() => localStorage.setItem('bt_owner_key', 'test-owner'));
  await page.reload({ waitUntil: 'load' }); await page.waitForTimeout(600);
  check('owner nav lists Trip Map', (await page.evaluate(() => document.querySelector('header nav').textContent)).includes('Trip Map'));
  await page.evaluate(() => localStorage.removeItem('bt_owner_key'));

  // ---------- 10. phone (375px) ----------
  await page.setViewportSize({ width: 375, height: 740 });
  await page.reload({ waitUntil: 'load' }); await page.waitForTimeout(900);
  check('no JS errors on phone', errors.length === 0, errors.join(' | '));
  check('body does not scroll sideways at 375px', await page.evaluate(() => document.documentElement.scrollWidth <= 375 && document.body.scrollWidth <= 375), await page.evaluate(() => [document.documentElement.scrollWidth, document.body.scrollWidth]));
  const sheet = await page.evaluate(() => { const p = document.querySelector('#panel').getBoundingClientRect(), m = document.querySelector('#map').getBoundingClientRect(); return { pw: p.width, ph: p.height, ptop: p.top, mh: m.height, mbottom: m.bottom, bbar: !!document.querySelector('.sw-bbar') }; });
  check('panel is a bottom sheet (~46% of the map) above the bottom bar', sheet.pw > 340 && sheet.ph < sheet.mh * 0.5 && sheet.ph > sheet.mh * 0.4 && sheet.bbar, JSON.stringify(sheet));
  check('attribution visible above the sheet', await page.evaluate(() => { const a = document.querySelector('.leaflet-control-attribution').getBoundingClientRect(), p = document.querySelector('#panel').getBoundingClientRect(); return a.bottom <= p.top + 1; }));
  await page.screenshot({ path: path.join(SHOTS, '05-phone-dark.png') });
  await page.click('#handle'); await page.waitForTimeout(450);
  check('handle expands the sheet', await page.evaluate(() => document.querySelector('#panel').classList.contains('up') && document.querySelector('#panel').getBoundingClientRect().height > document.querySelector('#map').getBoundingClientRect().height * 0.9));
  await page.screenshot({ path: path.join(SHOTS, '06-phone-expanded.png') });
  await page.click('#list .tm-stop[data-id]:nth-of-type(2)'); await page.waitForTimeout(1200);
  check('selecting a stop on the phone collapses the sheet and opens the popup', !(await page.evaluate(() => document.querySelector('#panel').classList.contains('up'))) && (await page.$$('.leaflet-popup')).length === 1);
  await page.click('#sw-theme-switch button[data-theme="light"]'); await page.waitForTimeout(500);
  await page.screenshot({ path: path.join(SHOTS, '07-phone-light.png') });

  // ---------- 11. blast radius: theme.js still boots on three other pages ----------
  for (const f of ['index.html', 'global.html', 'capex.html']) {
    const p2 = await ctx.newPage(); const errs2 = [];
    p2.on('pageerror', e => errs2.push(String(e)));
    await p2.goto(BASE + '/' + f, { waitUntil: 'load' }); await p2.waitForTimeout(900);
    const navOk = await p2.evaluate(() => !!document.querySelector('header nav.sw-nav, header .sw-nav, .sw-menu') && !!document.querySelector('.sw-footer'));
    check(f + ': theme.js nav + footer render, no JS errors', navOk && errs2.length === 0, errs2.join(' | ') || 'ok');
    if (f === 'index.html') check('index.html tiles omit Trip Map for a non-owner', !(await p2.evaluate(() => document.getElementById('tileSections').textContent)).includes('Trip Map'));
    await p2.close();
  }

  await browser.close();
  console.log('\n' + (fails.length ? 'FAILED: ' + fails.length + ' -> ' + fails.join('; ') : 'ALL CHECKS PASSED'));
  process.exit(fails.length ? 1 : 0);
})().catch(e => { console.error('VERIFY CRASH', e); process.exit(2); });
