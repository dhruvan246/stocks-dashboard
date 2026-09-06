/* STOCKSWORLD — theme switcher (Light / Dark / Soft).
 * Loaded in <head> WITHOUT defer so it sets <html data-theme> before the body
 * paints (no flash). The pill UI is injected on DOMContentLoaded. Choice is
 * remembered per browser (localStorage 'sw_theme') and shared across all pages. */
(function () {
  'use strict';
  // ---- inline SVG icon set (Lucide-style 24px strokes) — ONE place, used by the nav,
  // page tabs, footer, bottom bar, theme switch, search palette and the home tiles.
  // Emoji used to sit in these slots; they rendered differently on every OS. ----
  var ICONS = {
    candle: '<path d="M9 4v3"/><rect x="7" y="7" width="4" height="8" rx="1"/><path d="M9 15v5"/><path d="M15 3v4"/><rect x="13" y="7" width="4" height="10" rx="1"/><path d="M15 17v4"/>',
    trend: '<polyline points="22 7 13.5 15.5 8.5 10.5 2 17"/><polyline points="16 7 22 7 22 13"/>',
    gauge: '<path d="m12 14 4-4"/><path d="M3.34 19a10 10 0 1 1 17.32 0"/>',
    pie: '<path d="M21.21 15.89A10 10 0 1 1 8 2.83"/><path d="M22 12A10 10 0 0 0 12 2v10z"/>',
    calendar: '<rect x="3" y="4" width="18" height="18" rx="2"/><line x1="16" y1="2" x2="16" y2="6"/><line x1="8" y1="2" x2="8" y2="6"/><line x1="3" y1="10" x2="21" y2="10"/>',
    caldays: '<rect x="3" y="4" width="18" height="18" rx="2"/><path d="M16 2v4M8 2v4M3 10h18"/><path d="M8 14h.01M12 14h.01M16 14h.01M8 18h.01M12 18h.01M16 18h.01"/>',
    thermo: '<path d="M14 4v10.54a4 4 0 1 1-4 0V4a2 2 0 0 1 4 0Z"/>',
    activity: '<path d="M22 12h-4l-3 9L9 3l-3 9H2"/>',
    globe: '<circle cx="12" cy="12" r="10"/><path d="M12 2a14.5 14.5 0 0 0 0 20 14.5 14.5 0 0 0 0-20"/><path d="M2 12h20"/>',
    bookmark: '<path d="m19 21-7-4-7 4V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2v16z"/>',
    star: '<polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/>',
    swap: '<path d="M8 3 4 7l4 4"/><path d="M4 7h16"/><path d="m16 21 4-4-4-4"/><path d="M20 17H4"/>',
    landmark: '<line x1="3" y1="22" x2="21" y2="22"/><line x1="6" y1="18" x2="6" y2="11"/><line x1="10" y1="18" x2="10" y2="11"/><line x1="14" y1="18" x2="14" y2="11"/><line x1="18" y1="18" x2="18" y2="11"/><polygon points="12 2 20 7 4 7"/>',
    briefcase: '<rect x="2" y="7" width="20" height="14" rx="2"/><path d="M16 21V5a2 2 0 0 0-2-2h-4a2 2 0 0 0-2 2v16"/>',
    users: '<path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M22 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/>',
    eye: '<path d="M2 12s3-7 10-7 10 7 10 7-3 7-10 7-10-7-10-7Z"/><circle cx="12" cy="12" r="3"/>',
    truck: '<path d="M5 18H3c-.6 0-1-.4-1-1V7c0-.6.4-1 1-1h10c.6 0 1 .4 1 1v11"/><path d="M14 9h4l4 4v4c0 .6-.4 1-1 1h-2"/><circle cx="7" cy="18" r="2"/><path d="M15 18H9"/><circle cx="17" cy="18" r="2"/>',
    zap: '<polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/>',
    banknote: '<rect x="2" y="6" width="20" height="12" rx="2"/><circle cx="12" cy="12" r="2"/><path d="M6 12h.01M18 12h.01"/>',
    sparkles: '<path d="m12 3 1.9 5.8a2 2 0 0 0 1.3 1.3L21 12l-5.8 1.9a2 2 0 0 0-1.3 1.3L12 21l-1.9-5.8a2 2 0 0 0-1.3-1.3L3 12l5.8-1.9a2 2 0 0 0 1.3-1.3Z"/>',
    file: '<path d="M14.5 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7.5L14.5 2z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/>',
    megaphone: '<path d="m3 11 18-5v12L3 14v-3z"/><path d="M11.6 16.8a3 3 0 1 1-5.8-1.6"/>',
    rocket: '<path d="M4.5 16.5c-1.5 1.26-2 5-2 5s3.74-.5 5-2c.71-.84.7-2.13-.09-2.91a2.18 2.18 0 0 0-2.91-.09z"/><path d="m12 15-3-3a22 22 0 0 1 2-3.95A12.88 12.88 0 0 1 22 2c0 2.72-.78 7.5-6 11a22.35 22.35 0 0 1-4 2z"/><path d="M9 12H4s.55-3.03 2-4c1.62-1.08 5 0 5 0"/><path d="M12 15v5s3.03-.55 4-2c1.08-1.62 0-5 0-5"/>',
    wallet: '<path d="M21 12V7H5a2 2 0 0 1 0-4h14v4"/><path d="M3 5v14a2 2 0 0 0 2 2h16v-5"/><path d="M18 12a2 2 0 0 0 0 4h4v-4Z"/>',
    calc: '<rect x="4" y="2" width="16" height="20" rx="2"/><line x1="8" y1="6" x2="16" y2="6"/><line x1="16" y1="14" x2="16" y2="18"/><path d="M16 10h.01M12 10h.01M8 10h.01M12 14h.01M8 14h.01M12 18h.01M8 18h.01"/>',
    target: '<circle cx="12" cy="12" r="10"/><circle cx="12" cy="12" r="6"/><circle cx="12" cy="12" r="2"/>',
    coins: '<circle cx="8" cy="8" r="6"/><path d="M18.09 10.37A6 6 0 1 1 10.34 18"/><path d="M7 6h1v4"/><path d="m16.71 13.88.7.71-2.82 2.82"/>',
    layers: '<path d="m12.83 2.18a2 2 0 0 0-1.66 0L2.6 6.08a1 1 0 0 0 0 1.83l8.58 3.91a2 2 0 0 0 1.66 0l8.58-3.9a1 1 0 0 0 0-1.83Z"/><path d="m22 12.5-8.58 3.91a2 2 0 0 1-1.66 0L2.6 12.5"/><path d="m22 17.5-8.58 3.91a2 2 0 0 1-1.66 0L2.6 17.5"/>',
    sliders: '<line x1="21" y1="4" x2="14" y2="4"/><line x1="10" y1="4" x2="3" y2="4"/><line x1="21" y1="12" x2="12" y2="12"/><line x1="8" y1="12" x2="3" y2="12"/><line x1="21" y1="20" x2="16" y2="20"/><line x1="12" y1="20" x2="3" y2="20"/><line x1="14" y1="2" x2="14" y2="6"/><line x1="8" y1="10" x2="8" y2="14"/><line x1="16" y1="18" x2="16" y2="22"/>',
    history: '<path d="M3 12a9 9 0 1 0 9-9 9.75 9.75 0 0 0-6.74 2.74L3 8"/><path d="M3 3v5h5"/><path d="M12 7v5l4 2"/>',
    radio: '<circle cx="12" cy="12" r="2"/><path d="M4.93 19.07a10 10 0 0 1 0-14.14"/><path d="M7.76 16.24a6 6 0 0 1 0-8.49"/><path d="M16.24 7.76a6 6 0 0 1 0 8.49"/><path d="M19.07 4.93a10 10 0 0 1 0 14.14"/>',
    pulse: '<path d="M19 14c1.49-1.46 3-3.21 3-5.5A5.5 5.5 0 0 0 16.5 3c-1.76 0-3 .5-4.5 2-1.5-1.5-2.74-2-4.5-2A5.5 5.5 0 0 0 2 8.5c0 2.3 1.5 4.05 3 5.5l7 7Z"/><path d="M3.22 12H9.5l.5-1 2 4.5 2-7 1.5 3.5h5.27"/>',
    check: '<path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/>',
    bars: '<path d="M3 3v18h18"/><path d="M18 17V9"/><path d="M13 17V5"/><path d="M8 17v-3"/>',
    bars2: '<line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/>',
    compass: '<circle cx="12" cy="12" r="10"/><polygon points="16.24 7.76 14.12 14.12 7.76 16.24 9.88 9.88 16.24 7.76"/>',
    inbox: '<polyline points="22 12 16 12 14 15 10 15 8 12 2 12"/><path d="M5.45 5.11 2 12v6a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2v-6l-3.45-6.89A2 2 0 0 0 16.76 4H7.24a2 2 0 0 0-1.79 1.11z"/>',
    flask: '<path d="M10 2v7.527a2 2 0 0 1-.211.896L4.72 20.55a1 1 0 0 0 .9 1.45h12.76a1 1 0 0 0 .9-1.45l-5.069-10.127A2 2 0 0 1 14 9.527V2"/><path d="M8.5 2h7"/><path d="M7 16h10"/>',
    home: '<path d="m3 9 9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/><polyline points="9 22 9 12 15 12 15 22"/>',
    menu: '<line x1="4" y1="6" x2="20" y2="6"/><line x1="4" y1="12" x2="20" y2="12"/><line x1="4" y1="18" x2="20" y2="18"/>',
    search: '<circle cx="11" cy="11" r="8"/><path d="m21 21-4.3-4.3"/>',
    sun: '<circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M6.34 17.66l-1.41 1.41M19.07 4.93l-1.41 1.41"/>',
    moon: '<path d="M12 3a6 6 0 0 0 9 9 9 9 0 1 1-9-9Z"/>',
    palette: '<circle cx="13.5" cy="6.5" r=".8" fill="currentColor"/><circle cx="17.5" cy="10.5" r=".8" fill="currentColor"/><circle cx="8.5" cy="7.5" r=".8" fill="currentColor"/><circle cx="6.5" cy="12.5" r=".8" fill="currentColor"/><path d="M12 2C6.5 2 2 6.5 2 12s4.5 10 10 10c.926 0 1.648-.746 1.648-1.688 0-.437-.18-.835-.437-1.125-.29-.289-.438-.652-.438-1.125a1.64 1.64 0 0 1 1.668-1.668h1.996c3.051 0 5.555-2.503 5.555-5.554C21.965 6.012 17.461 2 12 2z"/>',
    chev: '<path d="m6 9 6 6 6-6"/>',
    book: '<path d="M2 3h6a4 4 0 0 1 4 4v14a3 3 0 0 0-3-3H2z"/><path d="M22 3h-6a4 4 0 0 0-4 4v14a3 3 0 0 1 3-3h7z"/>',
    phone: '<rect x="5" y="2" width="14" height="20" rx="2"/><path d="M12 18h.01"/>',
    ext: '<path d="M7 7h10v10"/><path d="M7 17 17 7"/>'
  };
  function ic(name, cls) {
    return '<svg class="sw-i' + (cls ? ' ' + cls : '') + '" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">' + (ICONS[name] || '') + '</svg>';
  }
  var KEY = 'sw_theme';
  var META = [
    { k: 'light', ic: ic('sun'),     lb: 'Light' },
    { k: 'dark',  ic: ic('moon'),    lb: 'Dark'  },
    { k: 'soft',  ic: ic('palette'), lb: 'Soft'  }
  ];
  var KEYS = META.map(function (m) { return m.k; });

  function saved() { try { return localStorage.getItem(KEY); } catch (e) { return null; } }
  function norm(t) { return KEYS.indexOf(t) >= 0 ? t : 'dark'; }   /* v3: dark is the default */

  // 1) apply ASAP — runs during <head> parse, before the body is painted
  document.documentElement.setAttribute('data-theme', norm(saved()));

  // ---- PWA wiring: manifest, app icons, Android status-bar colour, service worker ----
  var THEME_COLOR = { light: '#ffffff', dark: '#070b14', soft: '#fffdfb' };
  function head(tag, attrs) {
    var key = attrs.rel ? 'rel' : 'name';
    var el = document.head.querySelector(tag + '[' + key + '="' + attrs[key] + '"]');
    if (!el) { el = document.createElement(tag); document.head.appendChild(el); }
    Object.keys(attrs).forEach(function (k) { el.setAttribute(k, attrs[k]); });
    return el;
  }
  function setThemeColor(t) { head('meta', { name: 'theme-color', content: THEME_COLOR[t] || THEME_COLOR.light }); }
  (function wirePWA() {
    head('link', { rel: 'manifest', href: './manifest.webmanifest' });
    head('link', { rel: 'apple-touch-icon', href: './apple-touch-icon.png' });
    head('meta', { name: 'apple-mobile-web-app-capable', content: 'yes' });
    head('meta', { name: 'mobile-web-app-capable', content: 'yes' });
    head('meta', { name: 'apple-mobile-web-app-status-bar-style', content: 'default' });
    head('meta', { name: 'apple-mobile-web-app-title', content: 'STOCKSWORLD' });
    setThemeColor(norm(saved()));
    // HEADLESS CI MODES (?bakewaves=1, ?logpicks=1) get NO service worker. Each CI batch runs in a
    // FRESH browser, so the SW always installs from scratch there: sw.js calls skipWaiting() +
    // clients.claim(), which fires controllerchange and reloads the page — killing the long-running
    // job ~30s in, every single batch, forever. That is what stopped the 6.3-yr `cycle` wave from ever
    // finishing (it sat at 27/45 while w1/w2/w3 squeaked through). Real visitors are unaffected.
    var ciMode = /[?&](bakewaves|logpicks)=/.test(location.search);
    if ('serviceWorker' in navigator && !ciMode) {
      // When a new SW activates and claims the page (after a shell bump), reload once so the
      // installed app immediately swaps in the fresh (mobile-responsive) pages instead of the
      // stale cached desktop layout. Guarded so it only ever reloads a single time.
      var swReloaded = false;
      navigator.serviceWorker.addEventListener('controllerchange', function () {
        if (swReloaded) return; swReloaded = true; window.location.reload();
      });
      window.addEventListener('load', function () { navigator.serviceWorker.register('./sw.js').catch(function () {}); });
    }
  })();

  // ---- "Get the app" state. Chrome fires `beforeinstallprompt` very early —
  // often before DOMContentLoaded — and only ONCE per page load, so it is
  // captured here at parse time and replayed when the visitor taps Android.
  // (The sheet that uses it is built further down, see GET THE APP.) ----
  var APP_KEY = 'sw_app_prompt';
  var INSTALL = { evt: null, done: false };
  window.addEventListener('beforeinstallprompt', function (e) {
    e.preventDefault();          // keep Chrome's own mini-infobar out of the way — ours replaces it
    INSTALL.evt = e;
  });
  window.addEventListener('appinstalled', function () {
    INSTALL.evt = null; INSTALL.done = true;
    try { localStorage.setItem(APP_KEY, 'installed'); } catch (e) {}
    var s = document.getElementById('sw-app-sheet'); if (s) s.classList.remove('open');
    var o = document.getElementById('sw-app-ov');    if (o) o.classList.remove('open');
    var g = document.getElementById('sw-f-app');     if (g) g.style.display = 'none';
  });

  function updateUI(t) {
    var box = document.getElementById('sw-theme-switch'); if (!box) return;
    box.querySelectorAll('button').forEach(function (b) {
      b.setAttribute('aria-pressed', b.getAttribute('data-theme') === t ? 'true' : 'false');
    });
  }
  function apply(t) {
    t = norm(t);
    document.documentElement.setAttribute('data-theme', t);
    try { localStorage.setItem(KEY, t); } catch (e) {}
    setThemeColor(t);
    updateUI(t);
    // The pinned column of a scrolling table paints a COPY of its table's
    // background (theme.js resolves it once, see responsifyAll). A theme switch
    // repaints the table but not that copy, so re-resolve it — after the .18s
    // colour transition, or we'd read the old colour mid-fade.
    setTimeout(function () { try { responsifyAll(); } catch (e) {} }, 260);
  }

  function build() {
    if (document.getElementById('sw-theme-switch')) return;
    var box = document.createElement('div');
    box.id = 'sw-theme-switch'; box.className = 'sw-theme-switch';
    box.setAttribute('role', 'group'); box.setAttribute('aria-label', 'Colour theme');
    META.forEach(function (m) {
      var b = document.createElement('button');
      b.type = 'button';
      b.setAttribute('data-theme', m.k);
      b.title = m.lb + ' theme';
      b.setAttribute('aria-label', m.lb + ' theme');
      b.innerHTML = '<span aria-hidden="true">' + m.ic + '</span>';
      b.addEventListener('click', function () { apply(m.k); });
      box.appendChild(b);
    });
    // append to the header's flex ROW (sibling of logo + nav) so the pill can be
    // pinned to the right edge — outside the nav's horizontal scroll on mobile.
    var host = document.querySelector('header > div')
            || document.querySelector('header .max-w-screen-xl')
            || document.querySelector('header nav')
            || document.querySelector('header');
    if (host) { host.appendChild(box); }
    else { box.classList.add('floating'); document.body.appendChild(box); }
    updateUI(document.documentElement.getAttribute('data-theme'));
  }

  // =========================================================================
  // SITE NAV — Markets / Funds / Tools as top-level items, each opening its own
  // dropdown on hover (click/tap also toggles). On phones (no hover) the three
  // collapse into one ☰ "Menu" whose groups are collapsible accordions.
  // Defined ONCE here so every page (and the two generated templates) shares it.
  // Add a section = one line below. The hardcoded <nav> on each page is replaced
  // on load; an early CSS rule hides it until then so the old row never flashes.
  // =========================================================================
  (function injectNavCSS() {
    if (document.getElementById('sw-nav-css')) return;
    var st = document.createElement('style'); st.id = 'sw-nav-css';
    st.textContent =
      '.sw-nav{display:flex;align-items:center;gap:4px;margin-left:auto;overflow:visible!important;}' +
      '.sw-cta{display:inline-flex;align-items:center;gap:7px;white-space:nowrap;font-size:13px;font-weight:600;padding:7px 13px;margin-left:6px;border-radius:9px;background:var(--accent);color:#fff;text-decoration:none;box-shadow:inset 0 1px 0 rgba(255,255,255,.18),0 1px 2px rgba(0,0,0,.25);transition:filter var(--tr),transform var(--tr),box-shadow var(--tr);}' +
      '.sw-cta .sw-i{font-size:15px;}' +
      '.sw-cta:hover{filter:brightness(1.08);}' +
      '.sw-cta.active{box-shadow:0 0 0 3px var(--accent-soft);}' +
      '.sw-group{position:relative;}' +
      '.sw-group-btn{display:inline-flex;align-items:center;gap:4px;font-size:13.5px;font-weight:500;padding:7px 10px;border-radius:8px;border:0;background:transparent;color:var(--text-muted);cursor:pointer;transition:var(--tr);white-space:nowrap;font-family:inherit;}' +
      '.sw-group-btn:hover,.sw-group.open>.sw-group-btn{color:var(--text);background:var(--surface-3);}' +
      '.sw-group-btn.active{color:var(--text);font-weight:600;}' +
      '.sw-caret{font-size:12px;opacity:.6;transition:transform .18s ease;}' +
      '.sw-group.open .sw-caret,.sw-menu-gbtn.open .sw-caret{transform:rotate(180deg);}' +
      '.sw-group-panel{position:absolute;left:0;top:calc(100% + 10px);min-width:244px;max-width:84vw;background:var(--surface);border:1px solid var(--border-strong);border-radius:12px;box-shadow:var(--shadow-lg);padding:6px;display:none;z-index:60;max-height:78vh;overflow:auto;}' +
      '.sw-group-panel::before{content:"";position:absolute;left:0;right:0;top:-12px;height:12px;}' +
      '.sw-group.open>.sw-group-panel{display:block;animation:sw-pop .16s ease;}' +
      '.sw-group.align-r .sw-group-panel{left:auto;right:0;}' +
      '.sw-group-panel--mega{flex-wrap:wrap;gap:4px 10px;min-width:0;width:660px;max-width:88vw;padding:8px;}' +
      '.sw-group.open>.sw-group-panel--mega{display:flex;}' +
      '.sw-mega-col{display:flex;flex-direction:column;flex:1 1 190px;min-width:186px;}' +
      '.sw-mega-h{font-size:10.5px;font-weight:600;letter-spacing:.07em;text-transform:uppercase;color:var(--text-faint);padding:8px 10px 6px;}' +
      '.sw-menu{position:relative;display:none;}' +
      '.sw-menu-btn{display:inline-flex;align-items:center;gap:7px;font-size:13px;font-weight:600;padding:7px 12px;border-radius:9px;border:1px solid var(--border-strong);background:var(--surface);color:var(--text);cursor:pointer;transition:var(--tr);font-family:inherit;}' +
      '.sw-menu-btn .sw-i{font-size:16px;}' +
      '.sw-menu-btn:hover{border-color:var(--accent);color:var(--accent-text);}' +
      '.sw-menu-panel{position:absolute;right:0;top:calc(100% + 10px);min-width:264px;max-width:84vw;background:var(--surface);border:1px solid var(--border-strong);border-radius:12px;box-shadow:var(--shadow-lg);padding:6px;display:none;z-index:60;max-height:78vh;overflow:auto;}' +
      '.sw-menu-panel.open{display:block;animation:sw-pop .16s ease;}' +
      '.sw-menu-gbtn{display:flex;width:100%;align-items:center;gap:8px;font-size:10.5px;font-weight:600;letter-spacing:.07em;text-transform:uppercase;color:var(--text-faint);padding:10px 10px 6px;background:none;border:0;cursor:pointer;text-align:left;font-family:inherit;}' +
      '.sw-menu-gbtn:hover{color:var(--text);}' +
      '.sw-menu-gbtn .sw-caret{margin-left:auto;}' +
      '.sw-menu-sec{display:none;}' +
      '.sw-menu-sec.open{display:block;}' +
      '.sw-menu-link{display:flex;align-items:center;gap:10px;font-size:13.5px;font-weight:500;padding:6px 8px;border-radius:8px;color:var(--text);text-decoration:none;transition:var(--tr);}' +
      '.sw-menu-link:hover{background:var(--surface-2);}' +
      '.sw-menu-link.active{background:var(--accent-soft);color:var(--accent-text);}' +
      '.sw-mi-ic{width:26px;height:26px;border-radius:7px;display:inline-flex;align-items:center;justify-content:center;background:var(--surface-2);color:var(--text-muted);flex:none;font-size:14px;transition:var(--tr);}' +
      '.sw-menu-link:hover .sw-mi-ic,.sw-menu-link.active .sw-mi-ic{color:var(--accent-text);background:var(--accent-soft);}' +
      '.sw-mi-ext{margin-left:auto;color:var(--text-faint);font-size:12px;display:inline-flex;}' +
      '.sw-tabs{margin:0 0 16px;}' +
      '.sw-tabs-in{display:inline-flex;max-width:100%;align-items:center;gap:2px;padding:3px;border:1px solid var(--border);border-radius:11px;background:var(--surface-2);overflow-x:auto;scrollbar-width:none;-webkit-overflow-scrolling:touch;}' +
      '.sw-tabs-in::-webkit-scrollbar{display:none;}' +
      '.sw-tab{display:inline-flex;align-items:center;gap:7px;white-space:nowrap;padding:6px 13px;border-radius:8px;font-size:13px;font-weight:600;color:var(--text-muted);text-decoration:none;transition:var(--tr);}' +
      '.sw-tab .sw-i{font-size:14px;opacity:.8;}' +
      '.sw-tab:hover{color:var(--text);}' +
      '.sw-tab.on{background:var(--surface);color:var(--text);box-shadow:0 1px 2px rgba(0,0,0,.18),inset 0 0 0 1px var(--border-strong);}' +
      '.sw-tab.on .sw-i{opacity:1;color:var(--accent-text);}' +
      '@media (max-width:640px){.sw-tab{padding:6px 10px;font-size:12.5px;}}' +
      '@media (max-width:760px){.sw-group{display:none;}.sw-menu{display:block;}}' +
      '@media (max-width:430px){.sw-cta{padding:7px 9px;}.sw-cta .sw-cta-lb{display:none;}}' +
      '@media (max-width:520px){.sw-menu-panel{position:fixed;left:10px;right:10px;top:60px;min-width:0;max-width:none;}}' +
      '.sw-bbar{display:none;}' +
      '@media (max-width:760px){.sw-bbar{position:fixed;bottom:0;left:0;right:0;z-index:55;display:flex;background:color-mix(in srgb,var(--surface) 94%,transparent);-webkit-backdrop-filter:saturate(170%) blur(14px);backdrop-filter:saturate(170%) blur(14px);border-top:1px solid var(--border);padding-bottom:env(safe-area-inset-bottom);}.sw-bbar-it{flex:1;display:flex;flex-direction:column;align-items:center;gap:3px;padding:8px 0 7px;font-size:10.5px;font-weight:600;color:var(--text-faint);text-decoration:none;background:none;border:0;cursor:pointer;transition:var(--tr);font-family:inherit;}.sw-bbar-it .ic{font-size:20px;line-height:1;display:flex;}.sw-bbar-it.on{color:var(--accent-text);}body{padding-bottom:calc(58px + env(safe-area-inset-bottom));}}' +
      'header nav:not(.sw-nav){visibility:hidden;}';
    document.head.appendChild(st);
  })();

  // Single source of truth for the nav. Add deployed pages here.
  // Every public page is listed here INDIVIDUALLY (StockView-style: any dashboard
  // is one click away from anywhere) — the nav, footer and home tiles all render
  // from this one list. Parameterized detail pages (stock.html, strategy-backtest.html)
  // are reached from content, not the menu.
  var NAV_GROUPS = [
    { g: 'Markets', cols: [
      { sub: 'Overview', items: [
        ['./nse-bse-dashboard.html',  ic('candle'), 'Stocks'],
        ['./movers.html',             ic('trend'), 'Top Movers'],
        ['./indices.html',            ic('gauge'), 'Indices'],
        ['./sectors.html',            ic('pie'), 'Sectors'],
        ['./monthly-returns.html',    ic('calendar'), 'Monthly Returns'],
        ['./market-mood.html',        ic('thermo'), 'Market Mood'],
        ['./macro.html',              ic('activity'), 'Macro'],
        ['./global.html',             ic('globe'), 'Global Markets'],
        ['./watchlist.html',          ic('bookmark'), 'Watchlist']
      ] },
      { sub: 'Flows & Ownership', items: [
        ['./fii-dii.html',            ic('swap'), 'FII/DII Flows'],
        ['./shareholding.html',       ic('landmark'), 'Stock Holdings'],
        ['./deals.html',              ic('briefcase'), 'Bulk/Block Deals'],
        ['./insider.html',            ic('eye'), 'Insider Trades'],
        ['./delivery.html',           ic('truck'), 'Delivery Spikes'],
        ['./volume.html',             ic('zap'), 'Volume Shockers'],
        ['./bank-credit.html',        ic('banknote'), 'Banking Growth']
      ] },
      { sub: 'Discovery & Filings', items: [
        ['./discovery.html',          ic('sparkles'), 'Smart Money Picks'],
        ['./quarterly-results.html',  ic('file'), 'Quarterly Results'],
        ['./employees.html',          ic('users'), 'Employee Headcount'],
        ['./announcements.html',      ic('megaphone'), 'Announcements'],
        ['./ipos.html',               ic('rocket'), 'IPOs & Listings'],
        ['./actions.html',            ic('caldays'), 'Ex-Dates Calendar']
      ] }
    ] },
    { g: 'Funds', items: [
      ['./mutual-funds.html',                        ic('wallet'), 'Mutual Funds'],
      ['./backtest.html',                            ic('calc'), 'MF Backtest'],
      ['https://dhruvan246.github.io/fno-dashboard/',  ic('target'), 'F&O']
    ] },
    { g: 'Tools', items: [
      ['./options-backtest.html',  ic('coins'), 'Options Backtest'],
      ['./saved-strategies.html',  ic('star'), 'Saved Strategies'],
      ['./all-picks.html',         ic('layers'), 'All Picks'],
      ['./strategy-mixer.html',    ic('sliders'), 'Strategy Mixer'],
      ['./backtest-history.html',  ic('history'), 'Backtest History'],
      ['./live-tracking.html',     ic('radio'), 'Live Tracking'],
      ['./status.html',            ic('pulse'), 'Data Health'],
      ['./results-coverage.html',  ic('check'), 'Results Coverage'],
      ['./fill-coverage.html',     ic('bars'), 'Fill Coverage'],
      ['./coverage.html',          ic('compass'), 'Coverage Matrix'],
      ['./analytics.html',         ic('bars2'), 'Page Stats'],
      ['./insurer-inbox.html',     ic('inbox'), 'Insurer Inbox']
    ] }
  ];
  var NAV_CTA = ['./stock-backtest.html',  ic('flask'), 'Create a strategy'];

  // ---- PAGE GROUPS: sibling pages presented as ONE tabbed section. Each member
  // page keeps its own URL, payload and data pipeline (so deep links, feeds.json
  // and the perf story are untouched); buildTabs() injects a shared tab strip at
  // the top of <main> on every member for quick sibling hops. The nav / footer /
  // home tiles list every member individually (see NAV_GROUPS above). Merge or
  // split a section here — one place, applies everywhere. Private members are
  // hidden from non-owners.
  var PAGE_GROUPS = [
    { g: 'Market Analytics', tabs: [
      ['./movers.html',           ic('trend'), 'Top Movers'],
      ['./indices.html',          ic('gauge'), 'Indices'],
      ['./monthly-returns.html',  ic('calendar'), 'Monthly Returns'],
      ['./market-mood.html',      ic('thermo'), 'Market Mood']
    ] },
    { g: 'FII/DII', tabs: [
      ['./fii-dii.html',       ic('swap'), 'Daily Flows'],
      ['./shareholding.html',  ic('landmark'), 'Stock Holdings']
    ] },
    { g: 'Deals & Insiders', tabs: [
      ['./deals.html',     ic('briefcase'), 'Bulk/Block Deals'],
      ['./insider.html',   ic('eye'), 'Insider Trades'],
      ['./delivery.html',  ic('truck'), 'Delivery Spikes'],
      ['./volume.html',    ic('zap'), 'Volume Shockers']
    ] },
    { g: 'Corporate Calendar', tabs: [
      ['./ipos.html',     ic('rocket'), 'IPOs & Listings'],
      ['./actions.html',  ic('caldays'), 'Ex-Dates']
    ] },
    { g: 'Strategies', tabs: [
      ['./saved-strategies.html',  ic('star'), 'Saved Strategies'],
      ['./backtest-history.html',  ic('history'), 'Backtest History'],
      ['./live-tracking.html',     ic('radio'), 'Live Tracking']
    ] },
    { g: 'Owner console', tabs: [
      ['./status.html',            ic('pulse'), 'Data Health'],
      ['./results-coverage.html',  ic('check'), 'Results Coverage'],
      ['./fill-coverage.html',     ic('bars'), 'Fill Coverage'],
      ['./coverage.html',          ic('compass'), 'Coverage Matrix'],
      ['./analytics.html',         ic('bars2'), 'Page Stats'],
      ['./insurer-inbox.html',     ic('inbox'), 'Insurer Inbox']
    ] }
  ];
  // (Every member page is listed individually in NAV_GROUPS, so each one
  // self-highlights in the nav — no primary-tab remapping needed.)

  // A group may define column sub-sections (cols) for its desktop mega-panel.
  // Flatten them into a single items[] so the mobile menu, home tiles and
  // SW_NAV keep consuming one flat list unchanged.
  NAV_GROUPS.forEach(function (g) {
    if (g.cols) g.items = g.cols.reduce(function (a, c) { return a.concat(c.items); }, []);
  });

  // ---- PRIVATE pages: the owner's personal tools. Hidden from the menu, footer
  // and home tiles unless THIS browser holds the owner key (unlocked once via
  // ?ownerkey=…, same key as the backtest pages); the pages themselves also show
  // a 🔒 to non-owners. This is a client-side curtain (GitHub Pages has no real
  // auth) — someone with the direct URL still reaches the lock screen.
  // Make a page public again by removing it from this list.
  try { // accept ?ownerkey= here too, so the unlock visit already shows the full nav
    var _u = new URL(location.href), _ok = _u.searchParams.get('ownerkey');
    if (_ok) { localStorage.setItem('bt_owner_key', _ok); _u.searchParams.delete('ownerkey'); history.replaceState(null, '', _u.pathname + _u.search + _u.hash); }
  } catch (e) {}
  var PRIVATE_PAGES = ['watchlist.html', 'live-tracking.html', 'insurer-inbox.html', 'analytics.html', 'status.html', 'results-coverage.html', 'fill-coverage.html', 'coverage.html'];
  var IS_OWNER = false; try { IS_OWNER = !!localStorage.getItem('bt_owner_key'); } catch (e) {}
  if (!IS_OWNER) NAV_GROUPS.forEach(function (g) {
    var keep = function (it) { return PRIVATE_PAGES.indexOf(it[0].replace('./', '')) < 0; };
    if (g.cols) g.cols.forEach(function (c) { c.items = c.items.filter(keep); });
    g.items = g.items.filter(keep);
  });

  // Expose the nav as the single source of truth so the home page (index.html) can
  // render its tile grid from the same list — add a page above and it shows up there too.
  try { window.SW_NAV = { groups: NAV_GROUPS, cta: NAV_CTA }; } catch (e) {}

  function esc(s) { return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/"/g, '&quot;'); }

  function buildNav() {
    var nav = document.querySelector('header nav'); if (!nav) return;
    try {
      var here = (location.pathname.split('/').pop() || '').toLowerCase();
      var isActive = function (href) { return href.indexOf('http') !== 0 && href.replace('./', '').toLowerCase() === here; };
      var link = function (it) {
        var href = it[0], icn = it[1], lb = it[2], ext = href.indexOf('http') === 0;
        return '<a class="sw-menu-link' + (isActive(href) ? ' active' : '') + '" href="' + esc(href) + '"' +
          (ext ? ' target="_blank" rel="noopener"' : '') + ' role="menuitem">' +
          '<span class="sw-mi-ic" aria-hidden="true">' + icn + '</span>' + esc(lb) +
          (ext ? '<span class="sw-mi-ext" aria-hidden="true">' + ic('ext') + '</span>' : '') + '</a>';
      };
      var caret = ic('chev', 'sw-caret');
      var activeGi = -1; // which group holds the current page (for highlight / default-open)
      NAV_GROUPS.forEach(function (grp, gi) {
        if (activeGi < 0 && grp.items.some(function (it) { return isActive(it[0]); })) activeGi = gi;
      });

      // Desktop: one hover dropdown per group (Markets / Funds / Tools)
      var groupsHtml = NAV_GROUPS.map(function (grp, gi) {
        var mega = grp.cols && grp.cols.some(function (c) { return c.items.length; });
        var inner = mega
          ? grp.cols.filter(function (c) { return c.items.length; }).map(function (c) {
              return '<div class="sw-mega-col"><div class="sw-mega-h">' + esc(c.sub) + '</div>' +
                c.items.map(link).join('') + '</div>';
            }).join('')
          : grp.items.map(link).join('');
        return '<div class="sw-group' + (gi === NAV_GROUPS.length - 1 ? ' align-r' : '') + '">' +
          '<button class="sw-group-btn' + (gi === activeGi ? ' active' : '') + '" type="button" aria-haspopup="true" aria-expanded="false">' +
            esc(grp.g) + caret + '</button>' +
          '<div class="sw-group-panel' + (mega ? ' sw-group-panel--mega' : '') + '" role="menu">' + inner + '</div>' +
        '</div>';
      }).join('');

      // Phones (no hover): ☰ menu with the same groups as collapsible accordions
      var mobHtml = NAV_GROUPS.map(function (grp, gi) {
        var open = gi === activeGi || (activeGi < 0 && gi === 0);
        return '<button class="sw-menu-gbtn' + (open ? ' open' : '') + '" type="button" aria-expanded="' + (open ? 'true' : 'false') + '">' +
            esc(grp.g) + caret + '</button>' +
          '<div class="sw-menu-sec' + (open ? ' open' : '') + '" role="group">' + grp.items.map(link).join('') + '</div>';
      }).join('');

      var cta = '<a class="sw-cta' + (isActive(NAV_CTA[0]) ? ' active' : '') + '" href="' + esc(NAV_CTA[0]) + '">' +
        '<span aria-hidden="true">' + NAV_CTA[1] + '</span><span class="sw-cta-lb">' + esc(NAV_CTA[2]) + '</span></a>';
      nav.className = 'sw-nav';
      nav.innerHTML = groupsHtml + cta +
        '<div class="sw-menu">' +
          '<button class="sw-menu-btn" type="button" aria-haspopup="true" aria-expanded="false" aria-label="Open sections menu">' +
            ic('menu') + '<span class="sw-menu-btn-lb">Menu</span></button>' +
          '<div class="sw-menu-panel" role="menu">' + mobHtml + '</div>' +
        '</div>';

      // Desktop dropdowns: open on hover (small leave-delay so the pointer can
      // travel into the panel); click/tap toggles too. One open at a time.
      var groups = nav.querySelectorAll('.sw-group');
      var closeGroups = function (except) {
        groups.forEach(function (g) {
          if (g === except) return;
          g.classList.remove('open');
          g.querySelector('.sw-group-btn').setAttribute('aria-expanded', 'false');
        });
      };
      var canHover = false; try { canHover = window.matchMedia('(hover:hover)').matches; } catch (e) {}
      groups.forEach(function (g) {
        var b = g.querySelector('.sw-group-btn'), t = null;
        var set = function (open) { g.classList.toggle('open', open); b.setAttribute('aria-expanded', open ? 'true' : 'false'); };
        b.addEventListener('click', function (e) {
          e.stopPropagation();
          // On hover devices the menu is already open from the hover — a click must
          // not re-toggle it shut. Touch (no hover) gets a plain open/close toggle.
          var open = canHover ? true : !g.classList.contains('open');
          closeGroups(g); set(open);
        });
        if (canHover) {
          g.addEventListener('mouseenter', function () { if (t) clearTimeout(t); closeGroups(g); set(true); });
          g.addEventListener('mouseleave', function () { if (t) clearTimeout(t); t = setTimeout(function () { set(false); }, 140); });
        }
      });

      // ☰ menu + its collapsible group sections
      var btn = nav.querySelector('.sw-menu-btn'), panel = nav.querySelector('.sw-menu-panel');
      var close = function () { panel.classList.remove('open'); btn.setAttribute('aria-expanded', 'false'); };
      btn.addEventListener('click', function (e) {
        e.stopPropagation();
        var open = panel.classList.toggle('open');
        btn.setAttribute('aria-expanded', open ? 'true' : 'false');
      });
      panel.querySelectorAll('.sw-menu-gbtn').forEach(function (gb) {
        gb.addEventListener('click', function (e) {
          e.stopPropagation();
          var sec = gb.nextElementSibling;
          var open = !sec.classList.contains('open');
          sec.classList.toggle('open', open);
          gb.classList.toggle('open', open);
          gb.setAttribute('aria-expanded', open ? 'true' : 'false');
        });
      });
      document.addEventListener('click', function (e) { if (!nav.contains(e.target)) { close(); closeGroups(); } });
      document.addEventListener('keydown', function (e) { if (e.key === 'Escape') { close(); closeGroups(); } });
    } catch (e) { nav.style.visibility = 'visible'; try { console.error('[theme] nav build failed:', e); } catch (_) {} }
  }

  // =========================================================================
  // SITE SEARCH — a 🔍 button in every page's header (and in the phone bottom
  // bar) that opens a command-palette: type a symbol or company name, hit ↵ and
  // land on that stock's page. Shortcuts: "/" or ⌘K / Ctrl-K to open, ↑↓ to
  // move, esc to close.
  //
  // The index (docs/search_index.json, ~190 KB / ~60 KB gzipped, built by
  // scripts/build_search_index.py) is fetched LAZILY on first use, so pages pay
  // nothing for it until someone actually searches. Its universe is exactly the
  // survivorship-free payload stock.html renders from — every suggestion is a
  // page that exists. Nav pages are matched too (from NAV_GROUPS), so the
  // palette doubles as a jump-anywhere box.
  // =========================================================================
  var SRCH = { rows: null, ind: [], v: '', load: null, ov: null, in: null, list: null, sel: 0, items: [] };
  var SRCH_RECENT = 'sw_srch_recent';

  (function injectSearchCSS() {
    if (document.getElementById('sw-srch-css')) return;
    var st = document.createElement('style'); st.id = 'sw-srch-css';
    st.textContent =
      '.sw-srch-btn{display:inline-flex;align-items:center;gap:9px;margin-left:12px;padding:0 10px;height:34px;min-width:232px;border:1px solid var(--border);border-radius:9px;background:var(--surface-2);color:var(--text-faint);font-size:13px;font-weight:500;cursor:pointer;transition:var(--tr);font-family:inherit;}' +
      '.sw-srch-btn .sw-srch-ic{display:inline-flex;font-size:15px;color:var(--text-faint);}' +
      '.sw-srch-btn:hover{border-color:var(--border-strong);color:var(--text-muted);background:var(--surface-3);}' +
      '.sw-srch-btn .sw-srch-kbd{margin-left:auto;font-family:var(--mono);font-size:10.5px;font-weight:500;padding:1px 6px;border:1px solid var(--border-strong);border-radius:5px;color:var(--text-faint);background:var(--surface);}' +
      '@media (max-width:1040px){.sw-srch-btn{min-width:0;margin-left:8px;}.sw-srch-btn .sw-srch-lb,.sw-srch-btn .sw-srch-kbd{display:none;}}' +
      '@media (max-width:520px){.sw-srch-btn{display:none;}}' +
      '.sw-srch-ov{position:fixed;inset:0;z-index:90;display:none;justify-content:center;align-items:flex-start;padding:10vh 14px 14px;background:rgba(5,7,12,.6);-webkit-backdrop-filter:blur(5px);backdrop-filter:blur(5px);}' +
      '.sw-srch-ov.open{display:flex;animation:sw-pop .14s ease;}' +
      '.sw-srch-box{width:100%;max-width:640px;background:var(--surface);border:1px solid var(--border-strong);border-radius:14px;box-shadow:var(--shadow-lg);overflow:hidden;display:flex;flex-direction:column;max-height:80vh;}' +
      '.sw-srch-top{display:flex;align-items:center;gap:10px;padding:13px 16px;border-bottom:1px solid var(--border);}' +
      '.sw-srch-top .sw-srch-ic{display:inline-flex;font-size:17px;color:var(--text-faint);}' +
      '.sw-srch-in{flex:1;min-width:0;border:0;outline:0;background:transparent!important;color:var(--text);font-size:16px;font-weight:500;font-family:inherit;box-shadow:none!important;}' +
      '.sw-srch-in::placeholder{color:var(--text-faint);font-weight:400;}' +
      '.sw-srch-in::-webkit-search-cancel-button{display:none;}' +
      '.sw-srch-x{border:1px solid var(--border-strong);background:var(--surface-2);color:var(--text-faint);border-radius:6px;font-family:var(--mono);font-size:10.5px;font-weight:500;padding:2px 7px;cursor:pointer;}' +
      '.sw-srch-x:hover{color:var(--text);border-color:var(--accent);}' +
      '.sw-srch-list{overflow-y:auto;padding:6px;-webkit-overflow-scrolling:touch;}' +
      '.sw-srch-h{font-size:10.5px;font-weight:600;letter-spacing:.07em;text-transform:uppercase;color:var(--text-faint);padding:10px 10px 5px;}' +
      '.sw-srch-it{display:flex;align-items:center;gap:10px;padding:8px 10px;border-radius:8px;text-decoration:none;color:var(--text);cursor:pointer;}' +
      '.sw-srch-it:hover,.sw-srch-it.on{background:var(--surface-2);}' +
      '.sw-srch-it.on{box-shadow:inset 0 0 0 1px var(--border-strong);}' +
      '.sw-srch-sym{font-family:var(--mono);font-size:13px;font-weight:600;letter-spacing:0;white-space:nowrap;display:inline-flex;align-items:center;}' +
      '.sw-srch-it.on .sw-srch-sym{color:var(--accent-text);}' +
      '.sw-srch-nm{font-size:13px;color:var(--text-muted);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;min-width:0;flex:1;}' +
      '.sw-srch-it b{color:var(--accent-text);font-weight:600;}' +
      '.sw-srch-meta{margin-left:auto;font-size:11px;color:var(--text-faint);white-space:nowrap;font-variant-numeric:tabular-nums;}' +
      '.sw-srch-tag{font-size:9.5px;font-weight:600;text-transform:uppercase;letter-spacing:.05em;padding:2px 6px;border-radius:999px;background:var(--surface-3);color:var(--text-faint);white-space:nowrap;}' +
      '.sw-srch-empty{padding:22px 12px;text-align:center;font-size:13px;color:var(--text-faint);}' +
      '.sw-srch-foot{display:flex;gap:14px;flex-wrap:wrap;padding:8px 16px;border-top:1px solid var(--border);font-size:10.5px;color:var(--text-faint);background:var(--surface-2);}' +
      '@media (max-width:640px){.sw-srch-ov{padding:8px;}.sw-srch-box{max-height:calc(100% - 16px);border-radius:12px;}.sw-srch-top{padding:11px 12px;}.sw-srch-it{padding:11px 10px;min-height:44px;}.sw-srch-meta{display:none;}.sw-srch-foot{display:none;}}';
    document.head.appendChild(st);
  })();

  function srchRecent(add) {
    var list = [];
    try { list = JSON.parse(localStorage.getItem(SRCH_RECENT) || '[]') || []; } catch (e) { list = []; }
    if (!add) return list;
    list = [add].concat(list.filter(function (s) { return s !== add; })).slice(0, 8);
    try { localStorage.setItem(SRCH_RECENT, JSON.stringify(list)); } catch (e) {}
    return list;
  }

  function srchLoad() {
    if (SRCH.rows) return Promise.resolve(true);
    if (SRCH.load) return SRCH.load;
    SRCH.load = fetch('./search_index.json')
      .then(function (r) { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); })
      .then(function (j) {
        SRCH.ind = j.ind || [];
        SRCH.v = j.v || '';
        // r[5] = uppercased name, precomputed once so each keystroke is a plain
        // indexOf over the array instead of 5k toUpperCase() calls.
        SRCH.rows = (j.s || []).map(function (r) { r[5] = String(r[1]).toUpperCase(); return r; });
        return true;
      })
      .catch(function (e) { SRCH.load = null; throw e; });
    return SRCH.load;
  }

  function fmtCr(v) {
    if (!v) return '';
    if (v >= 100000) return '₹' + (v / 100000).toFixed(2) + ' L cr';
    return '₹' + Math.round(v).toLocaleString('en-IN') + ' cr';
  }
  // bold the matched slice without letting the raw text through unescaped
  function srchHi(text, q) {
    var i = q ? String(text).toUpperCase().indexOf(q) : -1;
    if (i < 0) return esc(text);
    return esc(text.slice(0, i)) + '<b>' + esc(text.slice(i, i + q.length)) + '</b>' + esc(text.slice(i + q.length));
  }

  // Match tiers, best first: exact symbol → symbol prefix → name word-start →
  // symbol contains → name contains. Rows are pre-sorted (live before delisted,
  // then mcap desc) by the builder, so within a tier the big names come first.
  function srchMatch(q) {
    var t1 = [], t2 = [], t3 = [], t4 = [], t5 = [], rows = SRCH.rows || [];
    for (var i = 0; i < rows.length; i++) {
      var r = rows[i], sym = r[0], nm = r[5];
      // An exact hit wins — unless it's a delisted ticker whose symbol a LIVE stock
      // also starts with ("hdfc" = the merged-away HDFC Ltd, but you meant HDFCBANK).
      // Demoting it to the prefix tier lands it after the live names, which sort first.
      if (sym === q) { (r[2] ? t1 : t2).push(r); continue; }
      if (sym.indexOf(q) === 0) { t2.push(r); continue; }
      var ni = nm.indexOf(q);
      if (ni === 0 || (ni > 0 && !/[A-Z0-9]/.test(nm.charAt(ni - 1)))) { t3.push(r); continue; }
      if (sym.indexOf(q) > 0) { t4.push(r); continue; }
      if (ni > 0) { t5.push(r); }
      if (t1.length + t2.length + t3.length > 400) break;   // plenty to rank from
    }
    var hits = t1.concat(t2, t3, t4, t5);
    // Words in any order ("motors tata", "bank of baroda" typed loosely) — only as a
    // fallback, so the straight substring ranking above always wins when it matches.
    if (hits.length < 6 && q.indexOf(' ') > 0) {
      var toks = q.split(/\s+/).filter(Boolean), seen = {};
      hits.forEach(function (r) { seen[r[0]] = 1; });
      for (var j = 0; j < rows.length && hits.length < 14; j++) {
        var rr = rows[j], hay = rr[0] + ' ' + rr[5];
        if (seen[rr[0]]) continue;
        var all = true;
        for (var k = 0; k < toks.length; k++) if (hay.indexOf(toks[k]) < 0) { all = false; break; }
        if (all) hits.push(rr);
      }
    }
    return hits.slice(0, 14);
  }

  function srchPages(q) {
    var hits = [];
    NAV_GROUPS.forEach(function (g) {
      g.items.forEach(function (it) {
        if (it[2].toUpperCase().indexOf(q) >= 0) hits.push(it);
      });
    });
    if ('CREATE A STRATEGY'.indexOf(q) >= 0 || 'BACKTEST'.indexOf(q) >= 0) hits.push(NAV_CTA);
    return hits.slice(0, 3);
  }

  function srchRender(q) {
    var html = '', items = [];
    var stockRow = function (r) {
      var meta = [fmtCr(r[3]), SRCH.ind[r[4]] && SRCH.ind[r[4]] !== 'Unknown' ? SRCH.ind[r[4]] : ''].filter(Boolean).join(' · ');
      items.push('./stock.html?sym=' + encodeURIComponent(r[0]));
      return '<a class="sw-srch-it" role="option" href="./stock.html?sym=' + encodeURIComponent(r[0]) + '">' +
        '<span class="sw-srch-sym">' + srchHi(r[0], q) + '</span>' +
        '<span class="sw-srch-nm">' + (r[1] === r[0] ? '' : srchHi(r[1], q)) + '</span>' +
        (r[2] ? '' : '<span class="sw-srch-tag">delisted</span>') +
        (meta ? '<span class="sw-srch-meta">' + esc(meta) + '</span>' : '') + '</a>';
    };
    if (!q) {
      var byS = {}; (SRCH.rows || []).forEach(function (r) { byS[r[0]] = r; });
      var rec = srchRecent().map(function (s) { return byS[s]; }).filter(Boolean);
      if (rec.length) html += '<div class="sw-srch-h">Recent</div>' + rec.map(stockRow).join('');
      html += '<div class="sw-srch-h">Largest companies</div>' +
        (SRCH.rows || []).slice(0, 6).map(stockRow).join('');
    } else {
      var hits = srchMatch(q);
      if (hits.length) html += '<div class="sw-srch-h">Stocks</div>' + hits.map(stockRow).join('');
      var pages = srchPages(q);
      if (pages.length) {
        html += '<div class="sw-srch-h">Pages</div>' + pages.map(function (it) {
          items.push(it[0]);
          return '<a class="sw-srch-it" role="option" href="' + esc(it[0]) + '">' +
            '<span class="sw-srch-sym" aria-hidden="true">' + it[1] + '</span>' +
            '<span class="sw-srch-nm">' + srchHi(it[2], q) + '</span></a>';
        }).join('');
      }
      if (!hits.length) {
        // Nothing indexed — still let a typed symbol through (a fresh listing, or a
        // BSE-only name the price payload doesn't carry yet). The stock page says
        // plainly when it can't find one, which beats a dead end here.
        if (!pages.length && /^[A-Z0-9&.-]{2,20}$/.test(q)) {
          items.push('./stock.html?sym=' + encodeURIComponent(q));
          html += '<div class="sw-srch-h">Not in our price data</div>' +
            '<a class="sw-srch-it" role="option" href="./stock.html?sym=' + encodeURIComponent(q) + '">' +
            '<span class="sw-srch-sym">' + esc(q) + '</span>' +
            '<span class="sw-srch-nm">Open the stock page anyway →</span></a>';
        } else if (!pages.length) {
          html += '<div class="sw-srch-empty">No stock matches “' + esc(q) + '”.<br>Search by symbol (RELIANCE) or company name (Reliance Industries).</div>';
        }
      }
    }
    SRCH.items = items;
    SRCH.sel = 0;
    SRCH.list.innerHTML = html;
    srchMark();
  }

  function srchMark() {
    var els = SRCH.list.querySelectorAll('.sw-srch-it');
    for (var i = 0; i < els.length; i++) {
      var on = i === SRCH.sel;
      els[i].classList.toggle('on', on);
      els[i].setAttribute('aria-selected', on ? 'true' : 'false');
      if (on && els[i].scrollIntoView) els[i].scrollIntoView({ block: 'nearest' });
    }
  }

  function srchGo(href) {
    if (!href) return;
    var m = /[?&]sym=([^&]+)/.exec(href);
    if (m) srchRecent(decodeURIComponent(m[1]));
    location.href = href;
  }

  function srchOpen(prefill) {
    if (!SRCH.ov) return;
    SRCH.ov.classList.add('open');
    SRCH.in.value = prefill || '';
    SRCH.list.innerHTML = '<div class="sw-srch-empty">Loading stock list…</div>';
    try { SRCH.in.focus({ preventScroll: true }); } catch (e) { SRCH.in.focus(); }
    srchLoad().then(function () {
      if (SRCH.ov.classList.contains('open')) srchRender(SRCH.in.value.trim().toUpperCase());
    }).catch(function () {
      SRCH.list.innerHTML = '<div class="sw-srch-empty">Couldn’t load the stock list — check your connection and try again.</div>';
    });
  }
  function srchClose() { if (SRCH.ov) SRCH.ov.classList.remove('open'); }

  function buildSearch() {
    if (document.querySelector('.sw-srch-ov')) return;

    var ov = document.createElement('div');
    ov.className = 'sw-srch-ov';
    ov.setAttribute('role', 'dialog');
    ov.setAttribute('aria-modal', 'true');
    ov.setAttribute('aria-label', 'Search stocks');
    ov.innerHTML =
      '<div class="sw-srch-box">' +
        '<div class="sw-srch-top">' +
          '<span class="sw-srch-ic" aria-hidden="true">' + ic('search') + '</span>' +
          '<input class="sw-srch-in" type="search" autocomplete="off" autocorrect="off" spellcheck="false" ' +
            'role="combobox" aria-expanded="true" aria-autocomplete="list" aria-controls="sw-srch-list" ' +
            'aria-label="Search stocks by name or symbol" placeholder="Search a stock — name or symbol…" />' +
          '<button class="sw-srch-x" type="button" aria-label="Close search">esc</button>' +
        '</div>' +
        '<div class="sw-srch-list" id="sw-srch-list" role="listbox" aria-label="Search results"></div>' +
        '<div class="sw-srch-foot"><span>↑↓ move</span><span>↵ open stock page</span><span>esc close</span></div>' +
      '</div>';
    document.body.appendChild(ov);
    SRCH.ov = ov;
    SRCH.in = ov.querySelector('.sw-srch-in');
    SRCH.list = ov.querySelector('.sw-srch-list');

    // header button — sits right after the logo, like Screener/Trendlyne
    var host = document.querySelector('header > div')
            || document.querySelector('header .max-w-screen-xl')
            || document.querySelector('header');
    if (host) {
      var btn = document.createElement('button');
      btn.type = 'button';
      btn.className = 'sw-srch-btn';
      btn.setAttribute('aria-label', 'Search stocks');
      btn.innerHTML = '<span class="sw-srch-ic" aria-hidden="true">' + ic('search') + '</span>' +
        '<span class="sw-srch-lb">Search a stock…</span><span class="sw-srch-kbd">/</span>';
      btn.addEventListener('click', function () { srchOpen(''); });
      if (host.children.length > 1) host.insertBefore(btn, host.children[1]);
      else host.appendChild(btn);
    }

    ov.querySelector('.sw-srch-x').addEventListener('click', srchClose);
    ov.addEventListener('click', function (e) { if (e.target === ov) srchClose(); });

    var deb = null;
    SRCH.in.addEventListener('input', function () {
      if (!SRCH.rows) return;                       // still loading; open() renders on arrival
      if (deb) clearTimeout(deb);
      var q = SRCH.in.value.trim().toUpperCase();
      deb = setTimeout(function () { srchRender(q); }, 60);
    });
    SRCH.in.addEventListener('keydown', function (e) {
      var n = SRCH.items.length;
      if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
        if (!n) return;
        e.preventDefault();
        SRCH.sel = (SRCH.sel + (e.key === 'ArrowDown' ? 1 : n - 1)) % n;
        srchMark();
      } else if (e.key === 'Enter') {
        e.preventDefault();
        if (deb) { clearTimeout(deb); deb = null; srchRender(SRCH.in.value.trim().toUpperCase()); }
        srchGo(SRCH.items[SRCH.sel]);
      } else if (e.key === 'Escape') { e.preventDefault(); srchClose(); }
    });
    SRCH.list.addEventListener('click', function (e) {
      var a = e.target.closest && e.target.closest('.sw-srch-it');
      if (!a || e.metaKey || e.ctrlKey || e.shiftKey) return;   // let modifier-clicks open a tab
      e.preventDefault();
      srchGo(a.getAttribute('href'));
    });

    // global shortcuts: "/" or ⌘K / Ctrl-K
    document.addEventListener('keydown', function (e) {
      if (SRCH.ov.classList.contains('open')) { if (e.key === 'Escape') srchClose(); return; }
      var t = e.target || {}, tag = (t.tagName || '').toLowerCase();
      if (tag === 'input' || tag === 'textarea' || tag === 'select' || t.isContentEditable) return;
      if ((e.key === 'k' || e.key === 'K') && (e.metaKey || e.ctrlKey)) { e.preventDefault(); srchOpen(''); return; }
      if (e.key === '/' && !e.metaKey && !e.ctrlKey && !e.altKey) { e.preventDefault(); srchOpen(''); }
    });

    // let pages hand off their own lookup boxes (e.g. stock.html's "another symbol")
    try { window.swSearch = { open: srchOpen, close: srchClose }; } catch (e) {}
  }

  // =========================================================================
  // SITE GLOSSARY — a collapsed "📖 Glossary" panel at the bottom of every
  // page, spelling out ONLY the terms that page actually uses (the strategy
  // pages get the factor codes, the deals page gets bulk vs block, and so on).
  //
  // The wording lives in ./glossary.js, NOT here — one dictionary of terms plus
  // a per-page list of which ones to show. That file is pulled in the first
  // time a reader opens the panel, so a normal page load pays nothing for it.
  // Edit glossary.js to change any definition or to add a term to a page.
  // =========================================================================
  var GLOSS_SKIP = ['results-season.html', 'private-import.html'];   // redirect stub + one-off owner utility

  function buildGlossary() {
    if (document.querySelector('.sw-gloss')) return;                 // a page shipping its own stays untouched
    var here = (location.pathname.split('/').pop() || 'index.html').toLowerCase();
    if (GLOSS_SKIP.indexOf(here) >= 0) return;

    var d = document.createElement('details');
    d.className = 'sw-gloss';
    d.innerHTML = '<summary>' + ic('book') + ' Glossary <span>— every term on this page, spelled out</span></summary>' +
      '<div class="sw-gloss-body"><p class="sw-gloss-wait">Loading…</p></div>';

    var host = document.querySelector('main');
    if (host) host.appendChild(d);
    else {
      // pages without a <main> (the Stocks dashboard) — sit above the footer
      var ft = document.querySelector('footer');
      if (ft && ft.parentNode) ft.parentNode.insertBefore(d, ft);
      else document.body.appendChild(d);
    }

    var done = false;
    d.addEventListener('toggle', function () {
      if (done || !d.open) return;
      done = true;
      loadGlossData(function (G) {
        var body = d.querySelector('.sw-gloss-body');
        if (!G || !G.p[here]) { body.innerHTML = '<p class="sw-gloss-wait">No glossary for this page yet.</p>'; return; }
        render(G, G.p[here], d, body);
      }, function () {
        d.querySelector('.sw-gloss-body').innerHTML =
          '<p class="sw-gloss-wait">Could not load the glossary — check your connection and reopen this panel.</p>';
        done = false;   // let a retry happen on the next open
      });
    });

    function render(G, page, det, body) {
      if (page.sub) {
        var sp = det.querySelector('summary span');
        if (sp) sp.textContent = '— every term on this page, spelled out ' + page.sub;
      }
      var html = (page.intro || []).map(function (p) { return '<p>' + p + '</p>'; }).join('');
      (page.secs || []).forEach(function (sec) {
        var rows = (sec[1] || []).map(function (k) {
          var bar = k.indexOf('|'), key = bar < 0 ? k : k.slice(0, bar), label = bar < 0 ? null : k.slice(bar + 1);
          var term = G.t[key];
          if (!term) return '';                                       // key typo'd or term retired — skip, never print a blank row
          return '<div><code>' + esc(label || term[0]) + '</code></div><div>' + term[1] + '</div>';
        }).join('');
        if (rows) html += '<h4>' + esc(sec[0]) + '</h4><div class="sw-gl">' + rows + '</div>';
      });
      if (page.note) html += '<h4>Fine print</h4><p>' + page.note + '</p>';
      body.innerHTML = html || '<p class="sw-gloss-wait">No glossary for this page yet.</p>';
    }
  }

  // one fetch per page, shared by anything else that wants the dictionary
  var glossState = 0, glossQueue = [];                                // 0 idle · 1 loading · 2 ready
  function loadGlossData(ok, fail) {
    if (glossState === 2) { ok(window.SW_GLOSSARY); return; }
    glossQueue.push({ ok: ok, fail: fail });
    if (glossState === 1) return;
    glossState = 1;
    var s = document.createElement('script');
    s.src = './glossary.js';
    s.onload = function () {
      glossState = window.SW_GLOSSARY ? 2 : 0;
      var q = glossQueue; glossQueue = [];
      q.forEach(function (c) { glossState === 2 ? c.ok(window.SW_GLOSSARY) : c.fail(); });
    };
    s.onerror = function () {
      glossState = 0;
      var q = glossQueue; glossQueue = [];
      q.forEach(function (c) { c.fail(); });
    };
    document.head.appendChild(s);
  }

  (function injectGlossCSS() {
    if (document.getElementById('sw-gloss-css')) return;
    var st = document.createElement('style'); st.id = 'sw-gloss-css';
    st.textContent =
      '.sw-gloss{margin:28px 0 8px;background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);}' +
      '.sw-gloss>summary{cursor:pointer;padding:13px 16px;font-size:13.5px;font-weight:600;color:var(--text);list-style:none;user-select:none;display:flex;align-items:center;gap:8px;}' +
      '.sw-gloss>summary::-webkit-details-marker{display:none;}' +
      '.sw-gloss>summary .sw-i{font-size:15px;color:var(--text-faint);}' +
      '.sw-gloss>summary::after{content:"";margin-left:auto;width:7px;height:7px;border-right:1.5px solid var(--text-faint);border-bottom:1.5px solid var(--text-faint);transform:rotate(45deg);transition:transform .18s ease;flex:none;}' +
      '.sw-gloss[open]>summary::after{transform:rotate(-135deg);}' +
      '.sw-gloss>summary:hover{color:var(--accent-text);}' +
      '.sw-gloss>summary span{font-weight:400;color:var(--text-muted);}' +
      '.sw-gloss-body{padding:4px 16px 20px;border-top:1px solid var(--border);}' +
      '.sw-gloss-body h4{font-size:10.5px;font-weight:600;letter-spacing:.07em;text-transform:uppercase;color:var(--text-faint);margin:17px 0 7px;}' +
      '.sw-gloss-body p{font-size:12.5px;color:var(--text-muted);margin:8px 0;line-height:1.55;}' +
      '.sw-gloss-body p b{color:var(--text);}' +
      '.sw-gloss-body code{font-family:var(--mono);font-size:12px;color:var(--accent-text);word-break:break-word;}' +
      '.sw-gloss-wait{font-size:12.5px;color:var(--text-muted);}' +
      '.sw-gl{display:grid;grid-template-columns:150px 1fr;gap:7px 14px;font-size:12.5px;line-height:1.55;}' +
      '.sw-gl>div:nth-child(even){color:var(--text-muted);}' +
      '.sw-gl b{color:var(--text);font-weight:600;}' +
      '@media(max-width:700px){.sw-gl{grid-template-columns:1fr;gap:0;}.sw-gl>div:nth-child(odd){margin-top:12px;}.sw-gl>div:nth-child(even){margin-top:2px;}}';
    document.head.appendChild(st);
  })();

  // =========================================================================
  // SITE FOOTER — injected on every page so all pages share one footer with
  // the same links as the header Menu. Replaces any hardcoded <footer> and
  // keeps its old text as a fine-print credits line.
  // =========================================================================
  function buildFooter() {
    if (document.querySelector('.sw-footer')) return;
    // A page may still ship its own <footer> for page-specific provenance (e.g. the Stocks
    // dashboard's price-data note) — its text is absorbed as the credits line below. Pages
    // without one get this site-wide default, so the wording lives in exactly one place.
    var old = document.querySelector('footer');
    var credits = old ? old.textContent.replace(/\s+/g, ' ').trim()
      : 'Data sourced from NSE, BSE, AMFI, RBI & Yahoo Finance · Investments in securities are subject to market risks; read all related documents carefully before investing.';
    var link = function (it) {
      var ext = it[0].indexOf('http') === 0;
      return '<a class="sw-f-link" href="' + esc(it[0]) + '"' + (ext ? ' target="_blank" rel="noopener"' : '') + '>' +
        '<span aria-hidden="true">' + it[1] + '</span>' + esc(it[2]) + (ext ? ' ↗' : '') + '</a>';
    };
    var fcol = function (head, items) {
      if (!items.length) return '';
      return '<div class="sw-f-col"><div class="sw-f-h">' + esc(head) + '</div>' + items.map(link).join('') + '</div>';
    };
    // Groups with column sub-sections (Markets) expand into one footer column
    // each, so the sitemap reads in tidy stacks instead of a single tall list.
    var cols = NAV_GROUPS.map(function (g) {
      if (g.cols) return g.cols.map(function (c) { return fcol(c.sub, c.items); }).join('');
      var items = g.g === 'Tools' ? [NAV_CTA].concat(g.items) : g.items;
      return fcol(g.g, items);
    }).join('');
    var f = document.createElement('footer');
    f.className = 'sw-footer';
    f.innerHTML =
      '<div class="sw-footer-in">' +
        '<div class="sw-f-brand"><span class="sw-f-logo" aria-hidden="true">SW</span><div>' +
          '<div class="sw-f-name">STOCKS<span>WORLD</span></div>' +
          '<p class="sw-f-tag">Indian markets, decoded — live dashboards for stocks, sectors and mutual funds, plus a strategy backtester over 25+ years of data.</p>' +
        '</div></div>' +
        '<div class="sw-f-cols">' + cols + '</div>' +
      '</div>' +
      '<div class="sw-f-bar"><div class="sw-f-bar-in">' +
        '<span>© ' + new Date().getFullYear() + ' STOCKSWORLD · For education &amp; research — not investment advice.</span>' +
        (credits ? '<span>' + esc(credits) + '</span>' : '') +
        // permanent way back to the install sheet, for anyone who dismissed it
        // or is reading on a desktop and wants it on their phone later
        (appInstalled() ? '' : '<button type="button" class="sw-f-app" id="sw-f-app">' + ic('phone') + ' Get the app</button>') +
      '</div></div>';
    var getapp = f.querySelector('#sw-f-app');
    if (getapp) getapp.addEventListener('click', function () { appOpen(true); });
    if (old && old.parentNode) { old.parentNode.replaceChild(f, old); }
    else { document.body.appendChild(f); }
  }

  // =========================================================================
  // MOBILE BOTTOM BAR — phones only (≤760px, same breakpoint as the ☰ menu):
  // a fixed bar with the four everyday destinations, so switching sections
  // never needs a scroll back up to the header. Markets opens the ☰ panel
  // (all sections, current group pre-expanded); the rest are plain links.
  // =========================================================================
  function buildBottomBar() {
    if (document.querySelector('.sw-bbar')) return;
    var here = (location.pathname.split('/').pop() || 'index.html').toLowerCase();
    var inMarkets = NAV_GROUPS[0].items.some(function (it) {
      return it[0].replace('./', '').toLowerCase() === here;
    });
    var cls = function (on) { return 'sw-bbar-it' + (on ? ' on' : ''); };
    var bar = document.createElement('nav');
    bar.className = 'sw-bbar';
    bar.setAttribute('aria-label', 'Quick sections');
    bar.innerHTML =
      '<a class="' + cls(here === 'index.html') + '" href="./index.html"><span class="ic" aria-hidden="true">' + ic('home') + '</span>Home</a>' +
      '<button type="button" class="' + cls(inMarkets) + '" data-sw="menu" aria-haspopup="true"><span class="ic" aria-hidden="true">' + ic('candle') + '</span>Markets</button>' +
      '<button type="button" class="sw-bbar-it" data-sw="search" aria-label="Search stocks"><span class="ic" aria-hidden="true">' + ic('search') + '</span>Search</button>' +
      '<a class="' + cls(here === 'stock-backtest.html') + '" href="./stock-backtest.html"><span class="ic" aria-hidden="true">' + ic('flask') + '</span>Backtest</a>' +
      '<a class="' + cls(here === 'mutual-funds.html' || here === 'backtest.html') + '" href="./mutual-funds.html"><span class="ic" aria-hidden="true">' + ic('wallet') + '</span>Funds</a>';
    document.body.appendChild(bar);
    bar.querySelector('[data-sw="menu"]').addEventListener('click', function (e) {
      e.stopPropagation();   // keep the document outside-click closer from instantly re-closing it
      var b = document.querySelector('.sw-menu-btn'); if (b) b.click();
    });
    bar.querySelector('[data-sw="search"]').addEventListener('click', function (e) {
      e.stopPropagation();
      srchOpen('');
    });
  }

  // =========================================================================
  // GET THE APP — every phone visit is offered the app, on BOTH platforms.
  //
  // STOCKSWORLD ships as a PWA (manifest + service worker, wired at the top of
  // this file), so "the app" is this site installed to the home screen: its own
  // icon, a standalone window with no browser chrome, and the shell cached for
  // offline. There is no Play Store / App Store listing, and the two platforms
  // install it in completely different ways:
  //   Android — Chrome fires `beforeinstallprompt`; we stash it (see INSTALL at
  //             the top) and replay it on tap, so it really is one tap into the
  //             system install dialog. Browsers that never fire it (Firefox,
  //             Samsung Internet) fall back to the written steps.
  //   iOS     — Apple exposes NO install API whatsoever. The only route is
  //             Share → Add to Home Screen, so we show it step by step.
  // Both buttons are always offered — the visitor's own platform just leads —
  // because people ask "is there an app?" on behalf of the other phone too.
  // The sheet auto-opens on phones, snoozes a week when dismissed, never shows
  // once installed, and the footer's "Get the app" reopens it on any device.
  // =========================================================================
  var HOW = {
    android: {
      ic: '🤖', t: 'Install on Android',
      note: 'Works in Chrome, Edge, Brave and Samsung Internet.',
      steps: [
        ['Open the browser menu', 'The ⋮ button at the top-right of the address bar.'],
        ['Tap "Install app"', 'Some browsers word it "Add to Home screen".'],
        ['Confirm "Install"', 'STOCKSWORLD lands in your app drawer with its own icon.']
      ]
    },
    ios: {
      ic: '🍎', t: 'Install on iPhone / iPad',
      note: 'Apple allows this from the Share menu only — it works in Safari and in Chrome for iOS.',
      steps: [
        ['Tap the Share button', 'The ⬆️ arrow-out-of-a-box icon in the browser bar.'],
        ['Choose "Add to Home Screen"', 'Scroll the list of actions down if you do not see it.'],
        ['Tap "Add"', 'STOCKSWORLD opens full-screen from your home screen, no browser bars.']
      ]
    }
  };

  (function injectAppCSS() {
    if (document.getElementById('sw-app-css')) return;
    var st = document.createElement('style'); st.id = 'sw-app-css';
    st.textContent =
      '.sw-app-sheet{position:fixed;left:10px;right:10px;bottom:14px;max-width:520px;margin:0 auto;z-index:70;display:none;gap:10px;align-items:center;padding:12px 12px 13px 13px;border:1px solid var(--border-strong);border-radius:var(--radius);background:color-mix(in srgb,var(--surface) 95%,transparent);-webkit-backdrop-filter:saturate(180%) blur(16px);backdrop-filter:saturate(180%) blur(16px);box-shadow:var(--shadow-lg);}' +
      '.sw-app-sheet.open{display:grid;grid-template-columns:auto 1fr auto;animation:sw-rise .22s ease;}' +
      // the sheet must clear the fixed bottom bar, which only exists on phones
      '@media (max-width:760px){.sw-app-sheet{bottom:calc(70px + env(safe-area-inset-bottom));}}' +
      '.sw-app-ic{width:44px;height:44px;border-radius:12px;display:block;}' +
      '.sw-app-tx{min-width:0;}' +
      '.sw-app-t{font-size:14.5px;font-weight:800;letter-spacing:-.01em;color:var(--text);}' +
      '.sw-app-s{font-size:11.5px;color:var(--text-muted);margin-top:2px;}' +
      '.sw-app-x{align-self:start;flex:none;width:26px;height:26px;padding:0;border:1px solid var(--border);border-radius:8px;background:var(--surface-2);color:var(--text-faint);font-size:11px;line-height:1;cursor:pointer;transition:var(--tr);}' +
      '.sw-app-x:hover{color:var(--text);border-color:var(--accent);}' +
      '.sw-app-row{grid-column:1/-1;display:flex;gap:8px;}' +
      '.sw-app-btn{flex:1;display:inline-flex;align-items:center;justify-content:center;gap:7px;min-height:44px;padding:9px 10px;border:1px solid var(--border-strong);border-radius:11px;background:var(--surface-2);color:var(--text);font-size:13px;font-weight:800;cursor:pointer;transition:var(--tr);}' +
      '.sw-app-btn:hover{border-color:var(--accent);background:var(--accent-soft);color:var(--accent);}' +
      '.sw-app-btn.pri{background:var(--accent);border-color:var(--accent);color:#fff;}' +
      '.sw-app-btn.pri:hover{filter:brightness(1.09);color:#fff;}' +
      '.sw-app-ov{position:fixed;inset:0;z-index:95;display:none;align-items:center;justify-content:center;padding:14px;background:rgba(4,7,14,.62);-webkit-backdrop-filter:blur(4px);backdrop-filter:blur(4px);}' +
      '.sw-app-ov.open{display:flex;}' +
      '.sw-app-box{width:100%;max-width:440px;max-height:86vh;overflow-y:auto;-webkit-overflow-scrolling:touch;padding:15px 16px 15px;border:1px solid var(--border-strong);border-radius:var(--radius);background:var(--surface);box-shadow:var(--shadow-lg);animation:sw-rise .2s ease;}' +
      '.sw-app-hd{display:flex;align-items:center;gap:10px;}' +
      '.sw-app-hd h3{margin:0;font-size:16px;font-weight:800;color:var(--text);}' +
      '.sw-app-hd .sw-app-x{margin-left:auto;}' +
      '.sw-app-note{margin:7px 0 9px;font-size:12px;line-height:1.5;color:var(--text-muted);}' +
      '.sw-app-step{display:flex;gap:11px;padding:10px 0;border-top:1px solid var(--border);}' +
      '.sw-app-n{flex:none;display:flex;align-items:center;justify-content:center;width:23px;height:23px;border-radius:999px;background:var(--accent-soft);color:var(--accent);font-size:11.5px;font-weight:800;}' +
      '.sw-app-sh{font-size:13.5px;font-weight:700;color:var(--text);}' +
      '.sw-app-sd{margin-top:2px;font-size:12px;line-height:1.45;color:var(--text-muted);}' +
      '.sw-app-alt{display:block;width:100%;margin-top:12px;padding:11px 10px;border:1px dashed var(--border-strong);border-radius:11px;background:none;color:var(--text-muted);font-size:12.5px;font-weight:700;cursor:pointer;transition:var(--tr);}' +
      '.sw-app-alt:hover{color:var(--accent);border-color:var(--accent);}' +
      // phones: the steps read as a bottom sheet, thumb-side up
      '@media (max-width:640px){.sw-app-ov{align-items:flex-end;padding:9px;}' +
        // theme.css gives every phone <button> min-height:38px, and
        // `html[data-theme] button` outranks a bare class — so the install CTAs
        // need the same specificity to keep their 44px target, and the little
        // square ✕ / footer pill need it to keep their own size (same escape
        // hatch theme.css uses for the theme-switch pill).
        'html[data-theme] .sw-app-btn{min-height:44px;}' +
        'html[data-theme] .sw-app-x,html[data-theme] .sw-f-app{min-height:0;}}' +
      '.sw-f-app{display:inline-flex;align-items:center;gap:6px;padding:5px 11px;border:1px solid var(--border-strong);border-radius:999px;background:var(--surface-2);color:var(--text-muted);font-size:11.5px;font-weight:800;cursor:pointer;transition:var(--tr);}' +
      '.sw-f-app:hover{color:var(--accent);border-color:var(--accent);background:var(--accent-soft);}';
    document.head.appendChild(st);
  })();

  function isIOS() {
    var ua = navigator.userAgent || '';
    if (/iPad|iPhone|iPod/i.test(ua)) return true;
    return /Macintosh/.test(ua) && (navigator.maxTouchPoints || 0) > 1;   // iPadOS 13+ reports itself as a Mac
  }
  function isPhone() {
    if (isIOS() || /Android/i.test(navigator.userAgent || '')) return true;
    var coarse = false; try { coarse = window.matchMedia('(pointer:coarse)').matches; } catch (e) {}
    return coarse && window.innerWidth <= 760;
  }
  // Already running as an installed app? Then there is nothing to offer.
  function appInstalled() {
    if (INSTALL.done) return true;
    if (navigator.standalone === true) return true;                       // iOS, launched from the home screen
    try {
      return window.matchMedia('(display-mode: standalone)').matches ||
             window.matchMedia('(display-mode: minimal-ui)').matches ||
             window.matchMedia('(display-mode: fullscreen)').matches;
    } catch (e) { return false; }
  }
  function appSnoozed() {
    try {
      var v = localStorage.getItem(APP_KEY);
      if (!v) return false;
      if (v === 'installed') return true;
      return (Date.now() - (parseInt(v, 10) || 0)) < 7 * 864e5;           // dismissed — ask again next week
    } catch (e) { return false; }
  }
  function appSnooze() { try { localStorage.setItem(APP_KEY, String(Date.now())); } catch (e) {} }

  function appClose(snooze) {
    var el = document.getElementById('sw-app-sheet');
    if (el) el.classList.remove('open');
    if (snooze) appSnooze();
  }

  // Android one-tap: replay the install event Chrome handed us at page load.
  function appAndroid() {
    var e = INSTALL.evt;
    if (!e) { appHow('android'); return; }   // never fired (Firefox/Samsung) or already spent this load
    INSTALL.evt = null;                      // a captured prompt can only be replayed once
    appClose(false);
    try {
      e.prompt();
      if (e.userChoice && e.userChoice.then) {
        e.userChoice.then(function (c) {
          if (!c || c.outcome !== 'accepted') appSnooze();                // "not now" — don't nag for a week
        }).catch(function () {});
      }
    } catch (err) { appHow('android'); }
  }

  function appHow(p) {
    var h = HOW[p] || HOW.ios, other = p === 'ios' ? 'android' : 'ios';
    var ov = document.getElementById('sw-app-ov');
    if (!ov) {
      ov = document.createElement('div');
      ov.id = 'sw-app-ov'; ov.className = 'sw-app-ov';
      ov.setAttribute('role', 'dialog'); ov.setAttribute('aria-modal', 'true');
      ov.innerHTML = '<div class="sw-app-box"></div>';
      document.body.appendChild(ov);
      ov.addEventListener('click', function (ev) {
        if (ev.target === ov) { ov.classList.remove('open'); return; }     // tap the backdrop to dismiss
        var b = ev.target.closest ? ev.target.closest('[data-p]') : null;
        if (!b) return;
        var t = b.getAttribute('data-p');
        if (t === 'close') ov.classList.remove('open');
        else if (t === 'android' && INSTALL.evt) { ov.classList.remove('open'); appAndroid(); }
        else appHow(t);
      });
      document.addEventListener('keydown', function (ev) {
        if (ev.key === 'Escape') ov.classList.remove('open');
      });
    }
    ov.setAttribute('aria-label', h.t);
    ov.querySelector('.sw-app-box').innerHTML =
      '<div class="sw-app-hd"><span aria-hidden="true" style="font-size:19px">' + h.ic + '</span>' +
        '<h3>' + h.t + '</h3>' +
        '<button type="button" class="sw-app-x" data-p="close" aria-label="Close">✕</button></div>' +
      '<p class="sw-app-note">' + h.note + '</p>' +
      h.steps.map(function (s, i) {
        return '<div class="sw-app-step"><span class="sw-app-n">' + (i + 1) + '</span><div>' +
          '<div class="sw-app-sh">' + s[0] + '</div><div class="sw-app-sd">' + s[1] + '</div></div></div>';
      }).join('') +
      '<button type="button" class="sw-app-alt" data-p="' + other + '">' + HOW[other].ic + '  On ' +
        (other === 'ios' ? 'an iPhone or iPad' : 'an Android phone') + ' instead? Show those steps</button>';
    ov.classList.add('open');
    appClose(false);                          // the sheet handed off; the steps take over
  }

  function appOpen(force) {
    if (appInstalled()) return;
    if (!force && appSnoozed()) return;
    var el = document.getElementById('sw-app-sheet');
    if (!el) {
      var btn = {
        android: '<button type="button" class="sw-app-btn" data-p="android"><span aria-hidden="true">🤖</span>Android</button>',
        ios: '<button type="button" class="sw-app-btn" data-p="ios"><span aria-hidden="true">🍎</span>iPhone</button>'
      };
      var mine = isIOS() ? 'ios' : 'android', other = mine === 'ios' ? 'android' : 'ios';
      el = document.createElement('div');
      el.id = 'sw-app-sheet'; el.className = 'sw-app-sheet';
      el.setAttribute('role', 'region');
      el.setAttribute('aria-label', 'Install the STOCKSWORLD app');
      el.innerHTML =
        '<img class="sw-app-ic" src="./icon-192.png" alt="" width="44" height="44">' +
        '<div class="sw-app-tx"><div class="sw-app-t">Get the STOCKSWORLD app</div>' +
          '<div class="sw-app-s">Free · installs in seconds · works offline</div></div>' +
        '<button type="button" class="sw-app-x" data-p="close" aria-label="Not now">✕</button>' +
        '<div class="sw-app-row">' + btn[mine].replace('sw-app-btn', 'sw-app-btn pri') + btn[other] + '</div>';
      document.body.appendChild(el);
      el.addEventListener('click', function (ev) {
        var b = ev.target.closest ? ev.target.closest('[data-p]') : null;
        if (!b) return;
        var p = b.getAttribute('data-p');
        if (p === 'close') appClose(true);
        else if (p === 'android') appAndroid();
        else appHow('ios');
      });
    }
    el.classList.add('open');
  }

  function buildInstall() {
    // let pages offer their own "get the app" entry point (same as swSearch)
    try { window.swApp = { open: appOpen, how: appHow, installed: appInstalled }; } catch (e) {}
    try {
      if (!isPhone() || appInstalled() || appSnoozed()) return;
      // Let the page paint first — a card that lands on a half-drawn dashboard
      // reads as an ad; one that slides in after it settles reads as an offer.
      setTimeout(function () { appOpen(false); }, 1400);
    } catch (e) {}
  }

  // =========================================================================
  // PAGE-GROUP TAB STRIP — on any page that belongs to a PAGE_GROUPS entry,
  // inject pill tabs (same look as the Quarterly Results tab bar) as the first
  // child of <main>, so the strip inherits the page's own width and padding.
  // Tabs are plain links between the member pages; the current one is lit.
  // =========================================================================
  function buildTabs() {
    try {
      var here = (location.pathname.split('/').pop() || 'index.html').toLowerCase();
      var grp = null;
      PAGE_GROUPS.forEach(function (g) {
        if (!grp && g.tabs.some(function (t) { return t[0].replace('./', '').toLowerCase() === here; })) grp = g;
      });
      if (!grp) return;
      var tabs = grp.tabs.filter(function (t) { return IS_OWNER || PRIVATE_PAGES.indexOf(t[0].replace('./', '')) < 0; });
      if (tabs.length < 2) return;
      var bar = document.createElement('div');
      bar.className = 'sw-tabs';
      bar.setAttribute('role', 'navigation');
      bar.setAttribute('aria-label', grp.g + ' sections');
      bar.innerHTML = '<div class="sw-tabs-in">' + tabs.map(function (t) {
        var on = t[0].replace('./', '').toLowerCase() === here;
        return '<a class="sw-tab' + (on ? ' on' : '') + '" href="' + esc(t[0]) + '"' + (on ? ' aria-current="page"' : '') + '>' +
          '<span aria-hidden="true">' + t[1] + '</span>' + esc(t[2]) + '</a>';
      }).join('') + '</div>';
      var main = document.querySelector('main');
      if (main) main.insertBefore(bar, main.firstChild);
      else {
        var h = document.querySelector('header');
        if (h && h.parentNode) h.parentNode.insertBefore(bar, h.nextSibling);
      }
    } catch (e) {}
  }

  // header drops a stronger shadow once the page is scrolled
  function watchHeader() {
    var h = document.querySelector('header'); if (!h) return;
    var on = function () { h.classList.toggle('sw-scrolled', (window.scrollY || document.documentElement.scrollTop || 0) > 8); };
    window.addEventListener('scroll', on, { passive: true });
    on();
  }

  // =========================================================================
  // RESPONSIVE TABLES (phones) — two behaviours, and SCROLL IS THE DEFAULT
  // since 2026-07-28 (user: cards "should not happen", match screener.in).
  //
  //   1. DEFAULT — the table stays a table: theme.js gives it a .sw-scrollx
  //      holder, so it keeps its natural width and the holder scrolls sideways
  //      with the name column pinned (theme.css). A data table is a GRID; you
  //      read it by comparing a number to the one beside it.
  //   2. OPT-IN CARDS — .sw-cards on the table or any ancestor keeps the old
  //      one-card-per-row reflow, for feeds whose row is a story rather than a
  //      grid row (an announcement caption, "what happened", "why not filled").
  //
  // Both paths key off the same header scan. A MutationObserver re-runs the
  // whole thing, so JS-rendered and re-sorted tables keep their treatment.
  // =========================================================================
  function headerLabels(table) {
    var thead = table.tHead;
    if (!thead || !thead.rows.length) return null;
    var hrow = thead.rows[thead.rows.length - 1];          // last header row holds the column labels
    var cells = hrow.cells;
    if (cells.length < 4) return null;                     // small tables already fit — leave them
    for (var i = 0; i < cells.length; i++) { if (cells[i].colSpan > 1) return null; }  // skip grouped headers
    var out = [];
    for (var j = 0; j < cells.length; j++) {
      var clone = cells[j].cloneNode(true);
      clone.querySelectorAll('br').forEach(function (n) { n.replaceWith(' '); });
      clone.querySelectorAll('.sort-ind').forEach(function (n) { n.remove(); });
      out.push((clone.textContent || '').replace(/\s+/g, ' ').trim());
    }
    return out;
  }
  function cardifyTable(table) {
    var labels = headerLabels(table);
    if (!labels) { table.classList.remove('sw-cardify'); return; }
    table.classList.add('sw-cardify');
    for (var b = 0; b < table.tBodies.length; b++) {
      var rows = table.tBodies[b].rows;
      for (var r = 0; r < rows.length; r++) {
        var cells = rows[r].cells;
        if (cells.length === 1 && cells[0].colSpan > 1) continue;   // placeholder / empty-state row
        for (var c = 0; c < cells.length; c++) {
          if (!cells[c].hasAttribute('data-label')) cells[c].setAttribute('data-label', labels[c] || '');
        }
      }
    }
  }
  // the background actually painted behind a cell: a table's tint usually sits on
  // the <thead>/<tr>/<table> or the card around it, not on the cell itself, so walk
  // up until something is opaque. The pinned column paints this, so scrolled
  // numbers pass BEHIND it instead of showing through.
  function paintedBg(el) {
    for (var n = el; n && n !== document.documentElement; n = n.parentElement) {
      var bg = getComputedStyle(n).backgroundColor;
      if (bg && bg !== 'transparent' && !/rgba\([^)]*,\s*0\)$/.test(bg)) return bg;
    }
    return '';
  }
  // Give the table a sideways-scrolling holder and pin its name column.
  function scrollifyTable(table) {
    if (table.classList.contains('sw-cardify')) {          // switched modes (opt-in removed)
      table.classList.remove('sw-cardify');
    }
    var holder = table.parentElement;
    if (!holder) return;
    if (!holder.classList.contains('sw-scrollx')) {
      // Re-use the existing wrapper only when the table is its ONLY element child —
      // otherwise a card's heading would scroll away with the table.
      if (holder.children.length === 1) {
        holder.classList.add('sw-scrollx');
      } else {
        var w = document.createElement('div');
        w.className = 'sw-scrollx';
        var pcs = getComputedStyle(holder);
        if (pcs.display === 'flex' || pcs.display === 'inline-flex') { w.style.flex = '1 1 auto'; w.style.minWidth = '0'; }
        holder.insertBefore(w, table);
        w.appendChild(table);
        holder = w;
      }
    }
    // WHICH column to pin: the one that identifies the row, so the label stays
    // while the numbers slide. Prefer a Stock/Index/Fund-style column if it is
    // 1st or 2nd (IPOs and the deal feeds lead with a date, movers and the
    // screeners with a "#" gutter — pinning either tells you nothing); else the
    // 1st, unless that is a blank/rank gutter.
    var hrow = table.tHead && table.tHead.rows.length ? table.tHead.rows[table.tHead.rows.length - 1] : null;
    if (hrow && hrow.cells.length > 2) {
      var txt = function (c) { return (c ? (c.textContent || '') : '').replace(/[\s#⇅↕▲▼]/g, ''); };
      var ID = /^(stock|symbol|company|name|index|fund|scheme|holder|person|client|sector|strategy|mutualfund)/i;
      var pin2 = ID.test(txt(hrow.cells[1])) && !ID.test(txt(hrow.cells[0]));
      if (!pin2 && txt(hrow.cells[0]) === '') pin2 = true;         // blank / rank gutter
      table.classList.toggle('sw-pin2', pin2);
    }
    // A full-width colspan cell (group heading, expand row, empty state) must not
    // be pinned — it would drag a shadow line into the middle of the table.
    for (var b = 0; b < table.tBodies.length; b++) {
      var rws = table.tBodies[b].rows;
      for (var r = 0; r < rws.length; r++) {
        var c0 = rws[r].cells[0];
        if (c0 && (c0.colSpan || 1) > 1) c0.classList.add('sw-span');
      }
    }
    // resolve the pinned cell's background from this page's own table. The pinned cell PAINTS
    // the previous answer itself (theme.css: background:var(--sw-pin-body,…)), so a second
    // pass — the re-resolve apply() schedules after a theme switch — read its own stale copy
    // and froze the OLD theme's colour on the column (white date cells in dark, found by the
    // 2026-09-05 audit). Drop both first so every pass resolves exactly like a fresh load.
    table.style.removeProperty('--sw-pin-head'); table.style.removeProperty('--sw-pin-body');
    var hcell = hrow && hrow.cells.length ? hrow.cells[0] : null;
    var bcell = table.tBodies.length && table.tBodies[0].rows.length ? table.tBodies[0].rows[0].cells[0] : null;
    if (hcell) { var hb = paintedBg(hcell); if (hb) table.style.setProperty('--sw-pin-head', hb); }
    if (bcell) { var bb = paintedBg(bcell); if (bb) table.style.setProperty('--sw-pin-body', bb); }
  }
  // how many columns wide, counting colspans — headerLabels() deliberately rejects
  // grouped headers (cards can't label them) but a grouped table is still a GRID
  // that should scroll, so width is measured separately.
  function colCount(table) {
    var hrow = (table.tHead && table.tHead.rows.length) ? table.tHead.rows[table.tHead.rows.length - 1]
             : (table.tBodies.length && table.tBodies[0].rows.length ? table.tBodies[0].rows[0] : null);
    if (!hrow) return 0;
    var n = 0;
    for (var i = 0; i < hrow.cells.length; i++) n += hrow.cells[i].colSpan || 1;
    return n;
  }
  function responsifyAll() {
    var t = document.querySelectorAll('table');
    for (var i = 0; i < t.length; i++) {
      try {
        var tb = t[i];
        if (colCount(tb) < 4) continue;                     // narrow tables already fit
        // .sw-grid forces scroll even inside a .sw-cards feed (a grid nested in a
        // card-mode list); cards need labels, so a grouped header falls through.
        if (!tb.classList.contains('sw-grid') && tb.closest && tb.closest('.sw-cards') && headerLabels(tb)) cardifyTable(tb);
        else scrollifyTable(tb);
      } catch (e) {}
    }
  }

  // =========================================================================
  // NO SIDEWAYS DRIFT (phones) — a document wider than the screen pans under
  // your finger, and everything fixed (the bottom bar) or sticky (the header)
  // slides off with it. theme.css clamps the top-level blocks; this mops up
  // what is left, usually one chip/toggle row a few px too wide: let it wrap,
  // or scroll inside itself, instead of stretching the whole page. Anything
  // that already scrolls horizontally (tab strips, table cards) is skipped —
  // its content is SUPPOSED to run past the edge.
  // =========================================================================
  function fitViewport() {
    try {
      if (window.innerWidth > 760) return;                                // phones only
      var root = document.documentElement;
      if (root.scrollWidth <= root.clientWidth + 1) return;               // nothing sticks out
      var vw = root.clientWidth, main = document.querySelector('main') || document.body;
      var fix = function (el, cs) {
        var par = el.parentElement, pcs = par ? getComputedStyle(par) : null;
        if (pcs && pcs.display === 'flex' && pcs.flexWrap === 'nowrap') { par.style.flexWrap = 'wrap'; return; }
        if (cs.display === 'flex' && cs.flexWrap === 'nowrap') { el.style.flexWrap = 'wrap'; return; }
        el.style.overflowX = 'auto'; el.style.maxWidth = '100%';
      };
      var walk = function (el) {
        var kids = el.children;
        for (var i = 0; i < kids.length; i++) {
          if (root.scrollWidth <= root.clientWidth + 1) return;           // page fits again — stop
          var k = kids[i], cs = getComputedStyle(k);
          if (cs.display === 'none' || cs.position === 'fixed') continue;
          if (cs.overflowX === 'auto' || cs.overflowX === 'scroll') continue;
          if (k.getBoundingClientRect().right > vw + 1) { fix(k, cs); continue; }
          if (k.scrollWidth > k.clientWidth + 1) walk(k);                 // the offender is deeper in
        }
      };
      for (var p = 0; p < 3 && root.scrollWidth > root.clientWidth + 1; p++) walk(main);
    } catch (e) {}
  }

  function watchTables() {
    responsifyAll(); fitViewport();
    if (!('MutationObserver' in window)) return;
    var timer = null, wantCards = false;
    var settle = function () { if (wantCards) { responsifyAll(); wantCards = false; } fitViewport(); };
    new MutationObserver(function (muts) {
      var any = false;
      for (var i = 0; i < muts.length; i++) {
        var added = muts[i].addedNodes; if (!added || !added.length) continue;
        for (var j = 0; j < added.length; j++) {
          var n = added[j]; if (n.nodeType !== 1) continue;
          any = true;
          if ((n.matches && n.matches('table,tbody,tr,td,th')) || (n.querySelector && n.querySelector('table,tr'))) wantCards = true;
        }
      }
      if (!any) return;                                    // only style/text edits — nothing to re-measure
      if (timer) clearTimeout(timer);
      timer = setTimeout(settle, 80);                      // debounce: run once after rows settle
    }).observe(document.body, { childList: true, subtree: true });
    var rz = null;
    window.addEventListener('resize', function () {        // rotate / desktop→phone width
      if (rz) clearTimeout(rz); rz = setTimeout(fitViewport, 150);
    }, { passive: true });
    window.addEventListener('load', fitViewport);          // late images / async renders
  }

  // Load the site-features layer on every page: sw-sync.js (Supabase kv/analytics)
  // then sw-watchlist.js (star buttons). Order matters — the watchlist decides
  // local-vs-synced mode by asking swSync. Both are tiny and fail-safe offline.
  function loadFeatures() {
    try {
      if (window.swSync) return; // page included it explicitly
      var s1 = document.createElement('script'); s1.src = './sw-sync.js';
      s1.onload = function () {
        var s2 = document.createElement('script'); s2.src = './sw-watchlist.js';
        document.head.appendChild(s2);
        // cross-device preference sync (owner browsers push; everyone pulls). ONE definition of the
        // key list, hoisted to window, so a page that must AWAIT the pull before reading one of these
        // (the Mixer reads mix_state_v1 at boot) can re-call syncSettings with the same list instead
        // of keeping a second copy that drifts.
        window.SW_SETTINGS_KEYS = ['sw_theme', 'sw_sec_watch', 'bt_fav_strategies',
          'live_worker_url', 'savedRotations', 'savedCatRot', 'savedBOB', 'sw_dash_presets', 'sw_triage_hide',
          'mix_state_v1',    // mix_state_v1: the Strategy Mixer's selection — synced so a mix set up once lands on every device
          'mix_presets_v1']; // mix_presets_v1: the Mixer's NAMED mixes ("⭐ 8 favourites" …) — one-click labelled selections
        try { window.swSync.syncSettings(window.SW_SETTINGS_KEYS); } catch (e) {}
      };
      document.head.appendChild(s1);
    } catch (e) {}
  }

  function init() { buildNav(); buildSearch(); buildTabs(); buildGlossary(); buildFooter(); buildBottomBar(); buildInstall(); build(); watchHeader(); watchTables(); loadFeatures(); }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
