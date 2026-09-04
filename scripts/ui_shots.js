// scripts/ui_shots.js — headless screenshot + smoke harness for the site UI (runbook §127).
// Use it whenever the Browser pane is hidden (screenshots come back blank there) or to sweep
// every page after a theme.css / theme.js change. Needs Google Chrome + puppeteer-core:
//   mkdir -p /tmp/uishots && cd /tmp/uishots && npm init -y >/dev/null && npm i puppeteer-core
//   cp ~/stocks-dashboard/scripts/ui_shots.js . && python3 -m http.server 8847 --directory ~/stocks-dashboard/docs &
//   node ui_shots.js http://localhost:8847 "index.html,movers.html" dark,light 1440,390
// Prints one line per render: theme applied, nav groups built (3 = ok), page tabs, body font,
// horizontal drift at that width (must be 0), console/page errors. PNGs land in ./shots/.
// Do NOT pass `clip` to page.screenshot for menus/overlays — puppeteer renders composited
// layers semi-transparent inside a clip (looked like a see-through dropdown; it was not).
const puppeteer = require('puppeteer-core');
const fs = require('fs');
const base = process.argv[2] || 'http://localhost:8847';
const pages = (process.argv[3] || 'index.html,movers.html,stock.html?sym=RELIANCE,fii-dii.html,stock-backtest.html,saved-strategies.html,quarterly-results.html,sectors.html,deals.html,mutual-funds.html').split(',');
const themes = (process.argv[4] || 'dark,light').split(',');
const widths = (process.argv[5] || '1440,390').split(',').map(Number);
const out = process.cwd() + '/shots'; require('fs').mkdirSync(out, { recursive: true });
(async () => {
  const browser = await puppeteer.launch({
    executablePath: '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
    headless: 'new', args: ['--hide-scrollbars', '--no-first-run']
  });
  const report = [];
  for (const theme of themes) for (const w of widths) for (const p of pages) {
    const page = await browser.newPage();
    const errors = [];
    page.on('pageerror', e => errors.push('pageerror: ' + e.message));
    page.on('console', m => { if (m.type() === 'error') errors.push('console: ' + m.text().slice(0, 160)); });
    page.on('response', r => { if (r.status() === 401 || r.status() >= 500) errors.push('http' + r.status() + ' ' + r.url().slice(0, 90)); });
    await page.setRequestInterception(true);
    page.on('request', r => { const u = r.url(); if (/supabase|workers\.dev|kv\./.test(u)) r.abort(); else r.continue(); });
    await page.evaluateOnNewDocument(t => { try { localStorage.setItem('sw_theme', t); localStorage.setItem('sw_app_prompt', String(Date.now())); } catch (e) {} }, theme);
    await page.setViewport({ width: w, height: w < 600 ? 844 : 1000, deviceScaleFactor: 1, isMobile: w < 600, hasTouch: w < 600 });
    try {
      await page.goto(base + '/' + p, { waitUntil: 'networkidle2', timeout: 25000 });
    } catch (e) { errors.push('goto: ' + e.message.slice(0, 100)); }
    await new Promise(r => setTimeout(r, 1200));
    const info = await page.evaluate(() => ({
      theme: document.documentElement.getAttribute('data-theme'),
      groups: document.querySelectorAll('.sw-group').length,
      menu: !!document.querySelector('.sw-menu-btn'),
      tabs: document.querySelectorAll('.sw-tab').length,
      font: getComputedStyle(document.body).fontFamily.split(',')[0],
      drift: (function () { document.documentElement.scrollLeft = 999; const d = document.documentElement.scrollLeft; document.documentElement.scrollLeft = 0; return d; })(),
      h1: (document.querySelector('main h1') || {}).textContent
    }));
    const name = `${p.replace(/[^a-z0-9]+/gi, '_')}_${theme}_${w}.png`;
    await page.screenshot({ path: `${out}/${name}`, fullPage: false });
    report.push({ page: p, theme, w, ...info, errors: errors.filter(e => !/404|stk\/|ERR_FAILED|net::ERR/.test(e)).slice(0, 3) });
    await page.close();
  }
  await browser.close();
  fs.writeFileSync(out + '/report.json', JSON.stringify(report, null, 1));
  for (const r of report) console.log(`${r.page.padEnd(28)} ${String(r.theme).padEnd(5)} ${String(r.w).padEnd(4)} groups=${r.groups} menu=${r.menu} tabs=${r.tabs} font=${r.font} drift=${r.drift} ${r.errors.length ? 'ERR ' + r.errors.join(' | ') : 'ok'}`);
})().catch(e => { console.error(e); process.exit(1); });
