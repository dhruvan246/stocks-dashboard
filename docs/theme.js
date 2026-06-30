/* STOCKSWORLD — theme switcher (Light / Dark / Soft).
 * Loaded in <head> WITHOUT defer so it sets <html data-theme> before the body
 * paints (no flash). The pill UI is injected on DOMContentLoaded. Choice is
 * remembered per browser (localStorage 'sw_theme') and shared across all pages. */
(function () {
  'use strict';
  var KEY = 'sw_theme';
  var META = [
    { k: 'light', ic: '☀️', lb: 'Light' },
    { k: 'dark',  ic: '🌙', lb: 'Dark'  },
    { k: 'soft',  ic: '🎨', lb: 'Soft'  }
  ];
  var KEYS = META.map(function (m) { return m.k; });

  function saved() { try { return localStorage.getItem(KEY); } catch (e) { return null; } }
  function norm(t) { return KEYS.indexOf(t) >= 0 ? t : 'light'; }

  // 1) apply ASAP — runs during <head> parse, before the body is painted
  document.documentElement.setAttribute('data-theme', norm(saved()));

  // ---- PWA wiring: manifest, app icons, Android status-bar colour, service worker ----
  var THEME_COLOR = { light: '#ffffff', dark: '#0f1423', soft: '#fffdfb' };
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
    if ('serviceWorker' in navigator) {
      window.addEventListener('load', function () { navigator.serviceWorker.register('./sw.js').catch(function () {}); });
    }
  })();

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
  // SITE NAV — one collapsible "Menu" dropdown, defined ONCE here so every page
  // (and the two generated templates) shares it. Add a section = one line below.
  // The hardcoded <nav> on each page is replaced on load; an early CSS rule hides
  // it until then so the old crowded row never flashes.
  // =========================================================================
  (function injectNavCSS() {
    if (document.getElementById('sw-nav-css')) return;
    var st = document.createElement('style'); st.id = 'sw-nav-css';
    st.textContent =
      '.sw-nav{display:flex;align-items:center;gap:8px;margin-left:auto;overflow:visible!important;}' +
      '.sw-cta{display:inline-flex;align-items:center;gap:6px;white-space:nowrap;font-size:13px;font-weight:600;padding:7px 12px;border-radius:var(--radius-sm);background:var(--accent-soft);color:var(--accent);text-decoration:none;transition:var(--tr);}' +
      '.sw-cta:hover,.sw-cta.active{background:var(--accent);color:#fff;}' +
      '.sw-menu{position:relative;}' +
      '.sw-menu-btn{display:inline-flex;align-items:center;gap:6px;font-size:13px;font-weight:600;padding:7px 12px;border-radius:var(--radius-sm);border:1px solid var(--border-strong);background:var(--surface);color:var(--text);cursor:pointer;transition:var(--tr);}' +
      '.sw-menu-btn:hover{background:var(--surface-2);}' +
      '.sw-menu-panel{position:absolute;right:0;top:calc(100% + 8px);min-width:252px;max-width:84vw;background:var(--surface);border:1px solid var(--border);border-radius:var(--radius-sm);box-shadow:var(--shadow-lg);padding:7px;display:none;z-index:60;max-height:78vh;overflow:auto;}' +
      '.sw-menu-panel.open{display:block;}' +
      '.sw-menu-group{font-size:10px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;color:var(--text-faint);padding:9px 10px 4px;}' +
      '.sw-menu-link{display:flex;align-items:center;gap:9px;font-size:14px;font-weight:500;padding:8px 10px;border-radius:9px;color:var(--text);text-decoration:none;transition:var(--tr);}' +
      '.sw-menu-link:hover{background:var(--surface-2);}' +
      '.sw-menu-link.active{background:var(--accent-soft);color:var(--accent);}' +
      '.sw-mi-ic{font-size:15px;width:20px;text-align:center;display:inline-block;}' +
      '.sw-mi-ext{margin-left:auto;color:var(--text-faint);font-size:12px;}' +
      '@media (max-width:430px){.sw-cta .sw-cta-lb{display:none;}}' +
      '@media (max-width:520px){.sw-menu-panel{position:fixed;left:10px;right:10px;top:60px;min-width:0;max-width:none;}}' +
      'header nav:not(.sw-nav){visibility:hidden;}';
    document.head.appendChild(st);
  })();

  // Single source of truth for the nav. Add deployed pages here.
  var NAV_GROUPS = [
    { g: 'Markets', items: [
      ['./nse-bse-dashboard.html', '📈', 'Stocks'],
      ['./sectors.html',           '🔥', 'Sectors'],
      ['./fii-dii.html',           '🌐', 'FII/DII'],
      ['./bank-credit.html',       '🏦', 'Banking Growth'],
      ['./market-mood.html',       '🌡️', 'Market Mood'],
      ['./results-season.html',    '📊', 'Results Season']
    ] },
    { g: 'Funds', items: [
      ['./mutual-funds.html',                       '💰', 'Mutual Funds'],
      ['https://dhruvan246.github.io/fno-dashboard/', '🎯', 'F&O']
    ] }
  ];
  var NAV_CTA = ['./stock-backtest.html', '🧪', 'Create a strategy'];

  function esc(s) { return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/"/g, '&quot;'); }

  function buildNav() {
    var nav = document.querySelector('header nav'); if (!nav) return;
    try {
      var here = (location.pathname.split('/').pop() || '').toLowerCase();
      var isActive = function (href) { return href.indexOf('http') !== 0 && href.replace('./', '').toLowerCase() === here; };
      var link = function (it) {
        var href = it[0], ic = it[1], lb = it[2], ext = href.indexOf('http') === 0;
        return '<a class="sw-menu-link' + (isActive(href) ? ' active' : '') + '" href="' + esc(href) + '"' +
          (ext ? ' target="_blank" rel="noopener"' : '') + ' role="menuitem">' +
          '<span class="sw-mi-ic" aria-hidden="true">' + ic + '</span>' + esc(lb) +
          (ext ? '<span class="sw-mi-ext" aria-hidden="true">↗</span>' : '') + '</a>';
      };
      var groups = NAV_GROUPS.map(function (grp) {
        return '<div class="sw-menu-group">' + esc(grp.g) + '</div>' + grp.items.map(link).join('');
      }).join('');
      var cta = '<a class="sw-cta' + (isActive(NAV_CTA[0]) ? ' active' : '') + '" href="' + esc(NAV_CTA[0]) + '">' +
        '<span aria-hidden="true">' + NAV_CTA[1] + '</span><span class="sw-cta-lb">' + esc(NAV_CTA[2]) + '</span></a>';
      nav.className = 'sw-nav';
      nav.innerHTML = cta +
        '<div class="sw-menu">' +
          '<button class="sw-menu-btn" type="button" aria-haspopup="true" aria-expanded="false" aria-label="Open sections menu">' +
            '<span aria-hidden="true">☰</span><span class="sw-menu-btn-lb">Menu</span></button>' +
          '<div class="sw-menu-panel" role="menu">' + groups + '</div>' +
        '</div>';
      var btn = nav.querySelector('.sw-menu-btn'), panel = nav.querySelector('.sw-menu-panel');
      var close = function () { panel.classList.remove('open'); btn.setAttribute('aria-expanded', 'false'); };
      btn.addEventListener('click', function (e) {
        e.stopPropagation();
        var open = panel.classList.toggle('open');
        btn.setAttribute('aria-expanded', open ? 'true' : 'false');
      });
      document.addEventListener('click', function (e) { if (!nav.contains(e.target)) close(); });
      document.addEventListener('keydown', function (e) { if (e.key === 'Escape') close(); });
    } catch (e) { nav.style.visibility = 'visible'; }
  }

  function init() { buildNav(); build(); }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
