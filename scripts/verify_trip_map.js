// Headless verification of docs/trip.html — the standalone Japan-tour page (runbook §39, §139).
// Serves docs/ locally; every cross-origin request is intercepted (the sandbox proxy blocks tile/API
// hosts) and LOGGED so URL shapes can still be checked. Nominatim (place search), Overpass (restaurants),
// Open-Meteo (weather) and frankfurter.app (exchange rate) are answered with canned payloads in their
// documented shapes — the LIVE endpoints are not exercised here.
// Run:  python3 -m http.server 8765 --directory docs --bind 127.0.0.1 &   then   node scripts/verify_trip_map.js
// Screenshots land in $TM_SHOTS (default: <tmpdir>/trip-map-shots). Exit code 0 = every check passed.
// Sections 1–8 assume the run date is BEFORE 2026-10-26 (whole tour shown, nothing visited); the rolling
// window is exercised through the page's own ?today= preview, so it is covered on any run date.
let chromium;
try { ({ chromium } = require('playwright')); }
catch (e) { ({ chromium } = require('/opt/node22/lib/node_modules/playwright')); }   // the cloud sandbox's global install
const path = require('path'), fs = require('fs'), os = require('os');
const BASE = process.env.TM_BASE || 'http://127.0.0.1:8765';
const SHOTS = process.env.TM_SHOTS || path.join(os.tmpdir(), 'trip-map-shots');
fs.mkdirSync(SHOTS, { recursive: true });
const fails = [];
function check(name, cond, detail) { const ok = !!cond; console.log((ok ? 'PASS ' : 'FAIL ') + name + (detail !== undefined ? '  -> ' + detail : '')); if (!ok) fails.push(name); }
const N = 27;                       // stops in the built-in tour
const FUSHIMI = 'jp18', SHIBUYA = 'jp9', MEIJI = 'jp7', NARITA = 'jp0';

(async () => {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({ viewport: { width: 1280, height: 800 }, deviceScaleFactor: 1, geolocation: { latitude: 34.9858, longitude: 135.7588 }, permissions: ['geolocation'] });
  const external = [], overpass = [], wx = [], fx = [];
  let overpassMode = 'ok';
  await ctx.route('**/*', route => {
    const u = route.request().url();
    if (u.startsWith(BASE)) return route.continue();
    external.push(u);
    if (u.startsWith('https://nominatim.openstreetmap.org/')) {
      return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify([
        { place_id: 1, lat: '34.9671', lon: '135.7727', name: 'Fushimi Inari Taisha', display_name: 'Fushimi Inari Taisha, Fushimi, Kyoto, Japan' },
        { place_id: 2, lat: '35.0116', lon: '135.7681', name: 'Kyoto', display_name: 'Kyoto, Japan' } ]) });
    }
    if (u.startsWith('https://overpass-api.de/') || u.startsWith('https://overpass.kumi.systems/')) {
      overpass.push(decodeURIComponent(route.request().postData() || ''));
      if (overpassMode === 'fail') return route.abort();
      return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ elements: [
        { type: 'node', id: 1, lat: 34.9690, lon: 135.7700, tags: { amenity: 'restaurant', cuisine: 'indian', name: 'インド料理 サンプル', 'name:en': 'Sample Curry House', opening_hours: '11:00-22:00', website: 'https://example.com/' } },
        { type: 'node', id: 2, lat: 34.9650, lon: 135.7760, tags: { amenity: 'restaurant', cuisine: 'indian;nepalese', name: 'Test Tandoor', 'diet:vegetarian': 'yes' } },
        { type: 'way',  id: 3, center: { lat: 35.6600, lon: 139.7010 }, tags: { amenity: 'restaurant', cuisine: 'indian', name: 'Way Curry', website: 'javascript:alert(1)' } },
        { type: 'node', id: 4, tags: { amenity: 'restaurant', cuisine: 'indian', name: 'No coordinates — must be skipped' } }
      ] }) });
    }
    if (u.startsWith('https://api.open-meteo.com/')) {
      wx.push(u);
      const day = (u.match(/start_date=(\d{4}-\d{2}-\d{2})/) || [])[1];
      return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ daily: { time: [day], weather_code: [2], temperature_2m_max: [19.4], temperature_2m_min: [11.6], precipitation_probability_max: [20] } }) });
    }
    if (u.startsWith('https://api.frankfurter.app/')) {
      fx.push(u);
      return route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ amount: 1, base: 'JPY', date: '2026-09-11', rates: { INR: 0.58 } }) });
    }
    return route.abort();
  });
  const page = await ctx.newPage();
  const errors = [], consoleErr = [];
  page.on('pageerror', e => errors.push(String(e)));
  page.on('console', m => { if (m.type() === 'error') consoleErr.push(m.text()); });
  page.on('dialog', d => d.accept());
  const rowCount = async () => (await page.$$('#list .tm-stop[data-id]')).length;
  const foodLine = async id => page.evaluate(id => { const r = document.querySelector('#list .tm-stop[data-id="' + id + '"]'); if (!r) return 'no row'; const s = r.querySelector('.tm-foodlist summary'); return s ? s.textContent.trim() : (r.querySelector('.tm-food-none') || {}).textContent || 'none'; }, id);
  const tab = async name => { await page.click('.jt-tab[data-tab="' + name + '"]'); await page.waitForTimeout(250); };

  // ---------- 1. fresh visitor: standalone page, whole tour shown ----------
  await page.goto(BASE + '/trip.html', { waitUntil: 'load' });
  await page.waitForFunction(() => /place/.test(document.querySelector('#foodStatus').textContent), null, { timeout: 8000 }).catch(() => {});
  await page.waitForTimeout(400);
  const today = await page.evaluate(() => { const d = new Date(), p = n => (n < 10 ? '0' : '') + n; return d.getFullYear() + '-' + p(d.getMonth() + 1) + '-' + p(d.getDate()); });
  console.log('today (browser local) =', today, '| data-theme =', await page.getAttribute('html', 'data-theme'));
  check('no page JS errors on load', errors.length === 0, errors.join(' | '));
  const nonNet = consoleErr.filter(t => !/Failed to load resource|net::ERR_FAILED|ERR_BLOCKED/i.test(t));
  check('no console errors other than blocked cross-origin loads', nonNet.length === 0, nonNet.join(' | '));
  console.log('   blocked-resource console lines:', consoleErr.length, ' external hosts seen:', [...new Set(external.map(u => new URL(u).host))].join(', '));
  check('standalone: no STOCKSWORLD chrome, theme.js or sw-sync on the page', await page.evaluate(() => !document.querySelector('script[src*="theme.js"], link[href*="theme.css"], .sw-nav, .sw-footer, .sw-bbar') && !/STOCKSWORLD/.test(document.documentElement.outerHTML)));
  check('default theme is light (no OS dark preference in headless) and tiles are voyager', (await page.getAttribute('html', 'data-theme')) === 'light' && external.some(u => /cartocdn\.com\/rastertiles\/voyager\/\d+\/\d+\/\d+/.test(u)));
  check('header: brand, JST/IST clock, countdown', /Japan · Group Tour/.test(await page.textContent('.jt-brand')) && /^JST \d\d:\d\d · IST \d\d:\d\d$/.test((await page.textContent('#clock')).trim()) && /^Starts in \d+ days$/.test((await page.textContent('#count')).trim()), (await page.textContent('#clock')) + ' | ' + (await page.textContent('#count')));
  const jst = await page.evaluate(() => new Date().toLocaleTimeString('en-GB', { timeZone: 'Asia/Tokyo', hour: '2-digit', minute: '2-digit' }));
  check('JST clock matches Intl for Asia/Tokyo', (await page.textContent('#clock')).includes('JST ' + jst));
  check(N + ' tour stops in the list (whole trip: it has not started)', (await rowCount()) === N, await rowCount());
  check(N + ' pins on the map', (await page.$$('#map .tm-pin-wrap .tm-pin')).length === N);
  const routes = await page.evaluate(() => ({ past: document.querySelectorAll('#map path.tm-route.past').length, future: document.querySelectorAll('#map path.tm-route.future').length, next: document.querySelectorAll('#map path.tm-route.next').length, glow: document.querySelectorAll('#map path.tm-route.glow').length }));
  check('route legs: 0 past, 26 future, 0 next, 26 glow', routes.past === 0 && routes.future === N - 1 && routes.next === 0 && routes.glow === N - 1, JSON.stringify(routes));
  check('9 day chips (All + 8 tour days), none marked today, chips carry the month', (await page.$$('#days .tm-day')).length === 9 && (await page.$$('#days .tm-day.today')).length === 0 && (await page.textContent('#days .tm-day[data-day="2026-10-30"] i')) === 'Oct');
  const stats = (await page.textContent('#stats')).replace(/\s+/g, ' ');
  check('stats show 0 / ' + N + ' visited, numeric distances', new RegExp('0 / ' + N).test(stats) && !/NaN|undefined/.test(stats), stats);
  check('head: tour badge, tour name, countdown; footer explains the whole-trip view', (await page.isVisible('#badge')) && (await page.textContent('#tripName')).includes('Japan tour') && /starts in \d+ days/.test(await page.textContent('#tripSpan')) && /Whole trip shown.*once it starts/.test(await page.textContent('#winNote')));
  check('list shows the itinerary wording where no clock time exists', (await page.textContent('#list .tm-stop[data-id="' + SHIBUYA + '"] .tm-t')).trim() === 'late afternoon' && (await page.textContent('#list .tm-stop[data-id="' + NARITA + '"] .tm-t')).trim() === '07:55');
  const strips = await page.evaluate(() => [...document.querySelectorAll('#list .jt-strip')].map(s => s.textContent.replace(/\s+/g, ' ').trim()));
  check('8 day strips: title, transport, meals, walking level', strips.length === 8 && /Arrival in Tokyo.*Private bus.*No meals included.*Light walking/.test(strips[0]) && /Fushimi Inari \+ Nara.*Metro.*Private bus.*Packed breakfast \(6 am start\).*Moderate walking.*⚠/.test(strips[4]) && /Head to Kyoto.*Shinkansen.*On foot.*Heavy walking/.test(strips[3]), strips[4]);
  check('header height 57px and the stage fills the viewport below it', await page.evaluate(() => { const h = document.querySelector('.jt-hdr').getBoundingClientRect(), r = document.querySelector('main.tm-stage').getBoundingClientRect(); return Math.round(h.height) === 57 && Math.round(r.top) === 57 && Math.round(r.top + r.height) === innerHeight; }));
  check('page body does not scroll sideways (desktop)', await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth));

  // ---------- 1b. restaurants nearby (Overpass mocked) ----------
  check('restaurant layer on by default: one Overpass request', overpass.length === 1, overpass.length);
  check('Overpass query: Indian-only selectors, radius 1500, includes Fushimi Inari, 2 clauses per stop', /\[out:json\]/.test(overpass[0] || '') && /cuisine"~"indian"/.test(overpass[0] || '') && !/diet:vegetarian/.test(overpass[0] || '') && /around:1500,34\.96710,135\.77270/.test(overpass[0] || '') && (overpass[0].match(/around:/g) || []).length === 2 * N);
  check('status line counts 3 places (element without coordinates skipped)', /^3 Indian places within 1\.5 km/.test((await page.textContent('#foodStatus')).trim()), await page.textContent('#foodStatus'));
  check('3 fork-and-knife pins on the map', (await page.$$('#map .tm-food')).length === 3 && (await page.$$('#map .tm-food svg')).length === 3);
  check('Fushimi Inari row lists 2 places, Shibuya 1, Meiji none', /^2 Indian places within 1\.5 km/.test(await foodLine(FUSHIMI)) && /^1 Indian place within 1\.5 km/.test(await foodLine(SHIBUYA)) && /No Indian places tagged/.test(await foodLine(MEIJI)), (await foodLine(FUSHIMI)) + ' | ' + (await foodLine(MEIJI)));
  check('VEG tag shown for a place with diet:vegetarian', await page.evaluate(id => !!document.querySelector('#list .tm-stop[data-id="' + id + '"] .tm-fitem[data-food="node/2"] .veg'), FUSHIMI));
  await page.click('#list .tm-stop[data-id="' + FUSHIMI + '"] .tm-foodlist summary');
  await page.click('#list .tm-stop[data-id="' + FUSHIMI + '"] .tm-fitem[data-food="node/1"]');
  await page.waitForTimeout(1200);
  check('clicking a restaurant opens its popup (English + native name, hours, distance from the stop)', (await page.textContent('.leaflet-popup .tm-pop-n').catch(() => '')) === 'Sample Curry House' && /インド料理/.test(await page.textContent('.leaflet-popup')) && /Hours: 11:00-22:00/.test(await page.textContent('.leaflet-popup')) && /from Fushimi Inari Taisha/.test(await page.textContent('.leaflet-popup')));
  check('restaurant clicks never select the stop row', (await page.$$('#list .tm-stop.on')).length === 0);
  await page.click('#list .tm-stop[data-id="' + SHIBUYA + '"] .tm-foodlist summary');
  await page.click('#list .tm-stop[data-id="' + SHIBUYA + '"] .tm-fitem[data-food="way/3"]');
  await page.waitForTimeout(1200);
  const hrefs = await page.evaluate(() => [...document.querySelectorAll('.leaflet-popup a.tm-btn')].map(a => a.getAttribute('href')));
  check('way element (center) works; javascript: website dropped', (await page.textContent('.leaflet-popup .tm-pop-n').catch(() => '')) === 'Way Curry' && hrefs.length === 2 && hrefs.every(h => /^https?:\/\//.test(h)), hrefs.join(' | '));
  await page.click('#foodToggle'); await page.waitForTimeout(300);
  check('toggle off: no pins, no status, no sublists', (await page.$$('#map .tm-food')).length === 0 && (await page.textContent('#foodStatus')).trim() === '' && (await page.$$('#list .tm-foodlist, #list .tm-food-none')).length === 0);
  await page.click('#foodToggle'); await page.waitForTimeout(300);
  check('toggle on again: pins back from memory, no new request', (await page.$$('#map .tm-food')).length === 3 && overpass.length === 1, overpass.length);
  await page.selectOption('#foodKind', 'veg');
  await page.waitForFunction(() => /vegetarian-friendly places within .* of your stops/.test(document.querySelector('#foodStatus').textContent), null, { timeout: 5000 }).catch(() => {});
  check('kind = vegetarian: new request with diet:vegetarian, cafe included, no cuisine clause', overpass.length === 2 && /diet:vegetarian/.test(overpass[1]) && /cafe/.test(overpass[1]) && !/cuisine"~"indian"/.test(overpass[1]), overpass.length);
  await page.selectOption('#foodKind', 'both'); await page.waitForTimeout(400);
  check('kind = both: 4 clauses per stop', overpass.length === 3 && (overpass[2].match(/around:/g) || []).length === 4 * N);
  await page.selectOption('#foodKind', 'indian'); await page.waitForTimeout(400);
  await page.selectOption('#foodRadius', '3000');
  await page.waitForFunction(() => /within 3 km of your stops/.test(document.querySelector('#foodStatus').textContent), null, { timeout: 5000 }).catch(() => {});
  check('radius 3 km: new request with around:3000', /around:3000,/.test(overpass[overpass.length - 1]));
  overpassMode = 'fail';
  const nBefore = overpass.length;
  await page.selectOption('#foodRadius', '5000');
  await page.waitForFunction(() => /Couldn/.test(document.querySelector('#foodStatus').textContent), null, { timeout: 8000 }).catch(() => {});
  check('Overpass down: error line with Retry, both endpoints tried, no JS errors', /Couldn’t reach/.test(await page.textContent('#foodStatus')) && (await page.$$('#foodRetry')).length === 1 && overpass.length === nBefore + 2 && errors.length === 0, (await page.textContent('#foodStatus')) + ' | requests=' + overpass.length);
  overpassMode = 'ok';
  await page.click('#foodRetry');
  await page.waitForFunction(() => /within 5 km of your stops/.test(document.querySelector('#foodStatus').textContent), null, { timeout: 6000 }).catch(() => {});
  check('Retry recovers', /^3 Indian places within 5 km/.test((await page.textContent('#foodStatus')).trim()), await page.textContent('#foodStatus'));
  await page.selectOption('#foodRadius', '1500'); await page.waitForTimeout(400);
  await page.click('.leaflet-popup-close-button').catch(() => {});
  await page.screenshot({ path: path.join(SHOTS, '01-desktop-light.png') });

  // ---------- 1c. no weather requests 47 days out; the tools list says when forecasts open ----------
  check('no Open-Meteo request outside the 16-day horizon', wx.length === 0, wx.length);

  // ---------- 2. select a stop from the list -> popup ----------
  await page.click('#list .tm-stop[data-id="' + NARITA + '"] .tm-n');
  await page.waitForTimeout(1200);
  check('popup opens for the selected stop', (await page.textContent('.leaflet-popup .tm-pop-n').catch(() => 'none')) === 'Narita Airport (NRT)');
  check('selected list row + pin highlighted', (await page.$$('#list .tm-stop.on')).length === 1 && (await page.$$('#map .tm-pin.on')).length === 1);
  check('Directions link carries lat,lng', /google\.com\/maps\/dir\/\?api=1&destination=\d+\.\d+,\d+\.\d+/.test(await page.getAttribute('.leaflet-popup a.tm-btn', 'href')));
  await page.screenshot({ path: path.join(SHOTS, '02-popup.png') });

  // ---------- 3. day chip filter ----------
  await page.click('#days .tm-day[data-day="2026-11-02"]'); await page.waitForTimeout(900);
  check('Kyoto day chip active; list shows only that day (8 stops); map keeps all pins', (await page.$$('#days .tm-day[data-day="2026-11-02"].on')).length === 1 && (await rowCount()) === 8 && (await page.$$('#map .tm-pin')).length === N);
  await page.click('#days .tm-day[data-day="2026-11-02"]'); await page.waitForTimeout(600);
  check('tapping the active day again restores the whole trip', (await rowCount()) === N);

  // ---------- 4. add a stop via search (mocked Nominatim) ----------
  await page.click('#btnAdd');
  check('editor opens with the first tour day preselected', (await page.isVisible('#modal')) && (await page.inputValue('#fDay')) === '2026-10-30');
  await page.click('#fSave');
  check('validation blocks an empty stop', await page.isVisible('#fErr'));
  await page.fill('#fQ', 'Fushimi Inari'); await page.click('#fSearch'); await page.waitForSelector('#fRes button');
  await page.click('#fRes button:nth-of-type(1)');
  check('picking a result fills location + name', (await page.textContent('#fLoc')).includes('34.9671') && (await page.inputValue('#fName')) === 'Fushimi Inari Taisha');
  await page.fill('#fName', 'Test stop via search'); await page.fill('#fWhen', 'after dinner'); await page.fill('#fNote', 'added by the smoke test');
  await page.click('#fSave'); await page.waitForTimeout(1200);
  check(N + 1 + ' stops after adding; new stop selected; badge gone', (await rowCount()) === N + 1 && (await page.textContent('.leaflet-popup .tm-pop-n').catch(() => '')) === 'Test stop via search' && !(await page.isVisible('#badge')));
  check('blank order -> the new stop lands at the END of its day (seq 3 after 1, 2)', await page.evaluate(() => { const rows = [...document.querySelectorAll('#list .tm-group')][0].querySelectorAll('.tm-stop'); return rows.length === 3 && rows[2].querySelector('.tm-n').textContent === 'Test stop via search' && rows[2].querySelector('.tm-t').textContent === 'after dinner'; }));
  const saved = await page.evaluate(() => JSON.parse(localStorage.getItem('tm_trip_v1') || 'null'));
  const added = saved && saved.stops.find(s => s.name === 'Test stop via search');
  check('trip persisted with builtin=false and seq=3', saved && saved.builtin === false && saved.stops.length === N + 1 && added && added.seq === 3);
  await page.reload({ waitUntil: 'load' }); await page.waitForTimeout(900);
  check(N + 1 + ' stops survive a reload; no JS errors', (await rowCount()) === N + 1 && errors.length === 0, errors.join(' | '));

  // ---------- 5. edit via popup, pick-on-map, delete ----------
  const newId = added.id;
  await page.click('#list .tm-stop[data-id="' + newId + '"] .tm-n'); await page.waitForTimeout(1100);
  await page.click('.leaflet-popup [data-act="edit"]');
  check('edit dialog: Delete shown, name/when/order prefilled', (await page.isVisible('#fDel')) && (await page.inputValue('#fName')) === 'Test stop via search' && (await page.inputValue('#fWhen')) === 'after dinner' && (await page.inputValue('#fSeq')) === '3');
  await page.click('#fPick');
  check('pick mode: modal hidden, pick bar shown', !(await page.isVisible('#modal')) && (await page.isVisible('#pickbar')));
  const mapBox = await page.locator('#map').boundingBox();
  await page.mouse.click(mapBox.x + mapBox.width * 0.75, mapBox.y + mapBox.height * 0.5); await page.waitForTimeout(300);
  check('map click returns to the editor with a picked location', (await page.isVisible('#modal')) && (await page.textContent('#fLoc')).includes('picked on the map'));
  await page.click('#fSave'); await page.waitForTimeout(900);
  const moved = await page.evaluate(id => JSON.parse(localStorage.getItem('tm_trip_v1')).stops.find(s => s.id === id), newId);
  check('picked coordinates saved', moved && (Math.abs(moved.lat - 34.9671) > 0.0001 || Math.abs(moved.lng - 135.7727) > 0.0001));
  await page.click('#list .tm-stop[data-id="' + newId + '"] .tm-n'); await page.waitForTimeout(1100);
  await page.click('.leaflet-popup [data-act="del"]'); await page.waitForTimeout(500);
  check('delete from popup -> ' + N + ' stops', (await rowCount()) === N);

  // ---------- 6. window change (no effect until the trip starts) ----------
  await page.click('#btnWin'); await page.fill('#wPast', '1'); await page.fill('#wFuture', '1'); await page.click('#wSave'); await page.waitForTimeout(800);
  check('window 1/1 before the trip: still the whole trip, footer explains', (await rowCount()) === N && /Whole trip shown · rolls to 1 day back · 1 day ahead once it starts/.test(await page.textContent('#winNote')) && (await page.$$('#list details.tm-out')).length === 0);
  await page.click('#btnWin'); await page.fill('#wPast', '2'); await page.fill('#wFuture', '4'); await page.click('#wSave'); await page.waitForTimeout(500);

  // ---------- 7. reset to the tour / start blank ----------
  await page.click('#btnMode'); await page.waitForTimeout(600);
  check('reset -> badge back, ' + N + ' stops, nothing in localStorage', (await page.isVisible('#badge')) && (await rowCount()) === N && (await page.evaluate(() => localStorage.getItem('tm_trip_v1'))) === null);
  await page.click('#btnMode'); await page.waitForTimeout(600);
  check('blank trip -> empty state, 0 pins, no NaN, food status explains', (await page.$$('#list .tm-blank')).length === 1 && (await page.$$('#map .tm-pin')).length === 0 && !/NaN|undefined/.test(await page.textContent('#stats')) && /No stops/.test(await page.textContent('#foodStatus')));
  await page.click('#blankSample'); await page.waitForTimeout(600);
  check('load the tour from the empty state', (await rowCount()) === N);

  // ---------- 8. extras / prep / info / tools tabs ----------
  await tab('extras');
  check('Extras tab: 11 optional spots + free-time cards; nothing on the map yet', (await page.$$('#extrasPane .jt-xitem')).length === 11 && (await page.$$('#extrasPane .jt-card')).length === 7 && (await page.$$('#map .jt-extra')).length === 0);
  await page.click('#extrasToggle'); await page.waitForTimeout(700);
  check('Show on the map -> 11 star pins', (await page.$$('#map .jt-extra')).length === 11 && (await page.getAttribute('#extrasToggle', 'aria-pressed')) === 'true');
  await page.click('#extrasPane [data-xadd="9"]');
  check('Add to plan prefills the editor (Universal Studios Japan, coords, note)', (await page.isVisible('#modal')) && (await page.inputValue('#fName')) === 'Universal Studios Japan' && (await page.textContent('#fLoc')).includes('34.6654') && /whole day/.test(await page.inputValue('#fNote')));
  await page.click('#fCancel');
  await page.click('#extrasToggle'); await page.waitForTimeout(300);
  check('extras hidden again and the choice persisted', (await page.$$('#map .jt-extra')).length === 0 && (await page.evaluate(() => JSON.parse(localStorage.getItem('jt_extras_v1')).on)) === false);
  await tab('prep');
  check('Prep tab: 12 checklist items, 0 done', (await page.$$('#prepPane input[data-prep]')).length === 12 && /0 of 12 done/.test(await page.textContent('#prepPane')));
  await page.check('#prepPane input[data-prep="flights"]'); await page.check('#prepPane input[data-prep="shoes"]'); await page.waitForTimeout(200);
  check('ticks update the progress and persist', /2 of 12 done/.test(await page.textContent('#prepPane')) && (await page.evaluate(() => Object.keys(JSON.parse(localStorage.getItem('jt_prep_v1'))).sort().join())) === 'flights,shoes' && (await page.$$('#prepPane .jt-check.done')).length === 2);
  await page.click('#prepReset'); await page.waitForTimeout(200);
  check('clear all ticks', /0 of 12 done/.test(await page.textContent('#prepPane')));
  await tab('info');
  const info = await page.textContent('#infoPane');
  check('Info tab: early-bird closed, current price ₹ 2,05,000 highlighted, inclusions/exclusions/bus/hotels/good-to-know', /early-bird window closed on 25 Aug/.test(info) && (await page.textContent('#infoPane .jt-price .jt-card.on .v')).includes('2,05,000') && (await page.$$('#infoPane .jt-list')).length === 3 && /Airport drop/.test(info) && /names to be shared by the operator/.test(info) && /active vacation/.test(info));
  await tab('tools');
  await page.waitForFunction(() => /ECB reference rate/.test(document.querySelector('#fxNote').textContent), null, { timeout: 5000 }).catch(() => {});
  check('Tools: JST/IST clocks populated; rate fetched once', /^\d\d:\d\d$/.test((await page.textContent('#clkJST')).trim()) && /^\d\d:\d\d$/.test((await page.textContent('#clkIST')).trim()) && fx.length === 1);
  check('¥1000 -> ₹ 580 at the mocked rate; quick table has 3 cards', (await page.textContent('#fxOut')).replace(/\s/g, '') === '₹580' && (await page.$$('#fxQuick .jt-card')).length === 3, await page.textContent('#fxOut'));
  await page.fill('#fxAmt', '2500'); await page.selectOption('#fxDir', 'INR'); await page.waitForTimeout(150);
  check('₹2500 -> ¥ 4,310 the other way', (await page.textContent('#fxOut')).replace(/\s/g, '') === '¥4,310', await page.textContent('#fxOut'));
  check('weather list: 8 rows, all say when the forecast opens (15 Oct for 30 Oct)', (await page.$$('#wxList .jt-wxrow')).length === 8 && /forecast opens Thu 15 Oct/.test(await page.textContent('#wxList .jt-wxrow')));
  await page.click('#shareBtn'); await page.waitForTimeout(300);
  check('Share shows a toast mentioning the link', /link/i.test(await page.textContent('#toast')) && (await page.isVisible('#toast')));
  await page.click('#locateBtn');
  await page.waitForFunction(() => /You are here/.test(document.querySelector('#meLine').textContent), null, { timeout: 5000 }).catch(() => {});
  check('Where am I: blue dot on the map, distance to the nearest stop (Kyoto Station, ~0 m — mocked at its coordinates)', (await page.$$('#map .jt-me')).length === 1 && /You are here · \d+ m from stop \d+ \(Kyoto Station\)/.test(await page.textContent('#meLine')) && (await page.evaluate(() => document.querySelector('.jt-tab.on').getAttribute('data-tab'))) === 'plan', await page.textContent('#meLine'));
  await page.screenshot({ path: path.join(SHOTS, '03-tools.png') });

  // ---------- 9. dark theme toggle -> tiles swap; persisted ----------
  const before = external.length;
  await page.click('#themeBtn'); await page.waitForTimeout(900);
  check('theme toggle -> dark, dark_all tiles requested, persisted', (await page.getAttribute('html', 'data-theme')) === 'dark' && external.slice(before).some(u => /cartocdn\.com\/dark_all\//.test(u)) && (await page.evaluate(() => localStorage.getItem('jt_theme'))) === 'dark');
  await page.screenshot({ path: path.join(SHOTS, '04-desktop-dark.png') });
  await page.click('#themeBtn'); await page.waitForTimeout(300);

  // ---------- 10. ?today= preview: the rolling window on tour day 4 (Mon 2 Nov) ----------
  await page.goto(BASE + '/trip.html?today=2026-11-02', { waitUntil: 'load' });
  await page.waitForFunction(() => /place/.test(document.querySelector('#foodStatus').textContent), null, { timeout: 8000 }).catch(() => {});
  await page.waitForTimeout(600);
  check('preview banner + "Day 4 of 8" countdown', (await page.isVisible('#preview')) && /Mon, 2 Nov/.test(await page.textContent('#preview')) && (await page.textContent('#count')).trim() === 'Day 4 of 8');
  check('rolling window: 25 stops in view (31 Oct – 6 Nov), 2 outside (30 Oct), 8 chips incl. today', (await rowCount()) === 25 && /^2 more stops outside/.test(await page.textContent('#list details.tm-out summary')) && (await page.$$('#days .tm-day')).length === 8 && (await page.$$('#days .tm-day.today')).length === 1);
  const pin2 = await page.evaluate(() => ({ past: document.querySelectorAll('#map .tm-pin.past').length, today: document.querySelectorAll('#map .tm-pin.today').length, future: document.querySelectorAll('#map .tm-pin.future').length }));
  check('pins: 8 visited, 8 today (pulsing), 9 upcoming', pin2.past === 8 && pin2.today === 8 && pin2.future === 9, JSON.stringify(pin2));
  const r2 = await page.evaluate(() => ({ past: document.querySelectorAll('#map path.tm-route.past').length, future: document.querySelectorAll('#map path.tm-route.future').length, next: document.querySelectorAll('#map path.tm-route.next').length, glow: document.querySelectorAll('#map path.tm-route.glow').length }));
  check('routes: 15 past (dotted), 8 future, 1 flowing "next" leg, 9 glow', r2.past === 15 && r2.future === 8 && r2.next === 1 && r2.glow === 9, JSON.stringify(r2));
  const stats2 = (await page.textContent('#stats')).replace(/\s+/g, ' ');
  check('stats show 8 / 25 visited with numeric km', /8 \/ 25/.test(stats2) && !/NaN/.test(stats2), stats2);
  check('day chips read done / today / +1..+4; footer shows the rolling window', (await page.textContent('#days .tm-day[data-day="2026-10-31"] i')) === 'done' && (await page.textContent('#days .tm-day[data-day="2026-11-03"] i')) === '+1' && (await page.textContent('#winNote')).trim() === '2 days back · 4 days ahead');
  check('weather: one Open-Meteo request per in-horizon day (7 days: 2–6 Nov = 5 … actually all 7 window days ≥ today? no: 2 past days skipped) → 5 requests, strips show temps', wx.length === 5 && wx.every(u => /timezone=Asia%2FTokyo/.test(u)) && (await page.evaluate(() => document.querySelector('#list .jt-wx[data-day="2026-11-02"]').textContent)).includes('19° / 12°'), wx.length + ' | ' + (await page.evaluate(() => document.querySelector('#list .jt-wx[data-day="2026-11-02"]').textContent)));
  check('editor defaults to today while rolling', (await page.click('#btnAdd'), (await page.inputValue('#fDay')) === '2026-11-02'));
  await page.click('#fCancel');
  await page.screenshot({ path: path.join(SHOTS, '05-preview-day4.png') });

  // ---------- 11. phone (375px) ----------
  await page.setViewportSize({ width: 375, height: 740 });
  await page.goto(BASE + '/trip.html', { waitUntil: 'load' }); await page.waitForTimeout(900);
  check('no JS errors on phone', errors.length === 0, errors.join(' | '));
  check('body does not scroll sideways at 375px', await page.evaluate(() => document.documentElement.scrollWidth <= 375 && document.body.scrollWidth <= 375), await page.evaluate(() => [document.documentElement.scrollWidth, document.body.scrollWidth]));
  const sheet = await page.evaluate(() => { const p = document.querySelector('#panel').getBoundingClientRect(), m = document.querySelector('#map').getBoundingClientRect(); return { pw: p.width, ph: p.height, mh: m.height }; });
  check('panel is a bottom sheet (~46% of the map)', sheet.pw > 340 && sheet.ph < sheet.mh * 0.5 && sheet.ph > sheet.mh * 0.4, JSON.stringify(sheet));
  check('attribution visible above the sheet', await page.evaluate(() => { const a = document.querySelector('.leaflet-control-attribution').getBoundingClientRect(), p = document.querySelector('#panel').getBoundingClientRect(); return a.bottom <= p.top + 1; }));
  await page.screenshot({ path: path.join(SHOTS, '06-phone-light.png') });
  await page.click('#handle'); await page.waitForTimeout(450);
  check('handle expands the sheet', await page.evaluate(() => document.querySelector('#panel').classList.contains('up') && document.querySelector('#panel').getBoundingClientRect().height > document.querySelector('#map').getBoundingClientRect().height * 0.9));
  await page.screenshot({ path: path.join(SHOTS, '07-phone-expanded.png') });
  await page.click('#list .tm-stop[data-id="' + NARITA + '"] .tm-n'); await page.waitForTimeout(1200);
  check('selecting a stop on the phone collapses the sheet and opens the popup', !(await page.evaluate(() => document.querySelector('#panel').classList.contains('up'))) && (await page.$$('.leaflet-popup')).length === 1);
  await tab('info');
  check('opening a content tab on the phone expands the sheet', await page.evaluate(() => document.querySelector('#panel').classList.contains('up')));
  await page.screenshot({ path: path.join(SHOTS, '08-phone-info.png') });

  // ---------- 12. blast radius: the shared theme.js / sw.js edits still boot the dashboard pages ----------
  for (const f of ['index.html', 'global.html', 'capex.html']) {
    const p2 = await ctx.newPage(); const errs2 = [];
    p2.on('pageerror', e => errs2.push(String(e)));
    await p2.goto(BASE + '/' + f, { waitUntil: 'load' }); await p2.waitForTimeout(900);
    const navOk = await p2.evaluate(() => !!document.querySelector('header nav.sw-nav, header .sw-nav, .sw-menu') && !!document.querySelector('.sw-footer'));
    check(f + ': theme.js nav + footer render, no JS errors', navOk && errs2.length === 0, errs2.join(' | ') || 'ok');
    if (f === 'index.html') check('index.html tiles do not mention Trip Map', !(await p2.evaluate(() => document.getElementById('tileSections').textContent)).includes('Trip Map'));
    await p2.close();
  }

  await browser.close();
  console.log('\n' + (fails.length ? 'FAILED: ' + fails.length + ' -> ' + fails.join('; ') : 'ALL CHECKS PASSED'));
  process.exit(fails.length ? 1 : 0);
})().catch(e => { console.error('VERIFY CRASH', e); process.exit(2); });
