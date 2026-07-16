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
    if ('serviceWorker' in navigator) {
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
      '.sw-nav{display:flex;align-items:center;gap:8px;margin-left:auto;overflow:visible!important;}' +
      '.sw-cta{display:inline-flex;align-items:center;gap:6px;white-space:nowrap;font-size:13px;font-weight:600;padding:7px 12px;border-radius:var(--radius-sm);background:linear-gradient(120deg,var(--accent),var(--accent-2));background-size:180% 180%;color:#fff;text-decoration:none;box-shadow:0 6px 16px -6px var(--glow);transition:transform var(--tr),box-shadow var(--tr),background-position .4s ease;}' +
      '.sw-cta:hover,.sw-cta.active{background-position:100% 50%;transform:translateY(-1px);box-shadow:0 10px 24px -8px var(--glow);}' +
      '.sw-group{position:relative;}' +
      '.sw-group-btn{display:inline-flex;align-items:center;gap:5px;font-size:13.5px;font-weight:600;padding:7px 11px;border-radius:var(--radius-sm);border:1px solid transparent;background:transparent;color:var(--text-muted);cursor:pointer;transition:var(--tr);white-space:nowrap;}' +
      '.sw-group-btn:hover,.sw-group.open>.sw-group-btn{color:var(--accent);background:var(--accent-soft);}' +
      '.sw-group-btn.active{color:var(--accent);}' +
      '.sw-caret{font-size:9px;opacity:.75;transition:transform .18s ease;}' +
      '.sw-group.open .sw-caret,.sw-menu-gbtn.open .sw-caret{transform:rotate(180deg);}' +
      '.sw-group-panel{position:absolute;left:0;top:calc(100% + 8px);min-width:238px;max-width:84vw;background:color-mix(in srgb,var(--surface) 88%,transparent);-webkit-backdrop-filter:blur(12px);backdrop-filter:blur(12px);border:1px solid var(--border);border-radius:var(--radius-sm);box-shadow:var(--shadow-lg);padding:7px;display:none;z-index:60;max-height:78vh;overflow:auto;}' +
      '.sw-group-panel::before{content:"";position:absolute;left:0;right:0;top:-10px;height:10px;}' +
      '.sw-group.open>.sw-group-panel{display:block;animation:sw-pop .16s ease;}' +
      '.sw-group.align-r .sw-group-panel{left:auto;right:0;}' +
      '.sw-menu{position:relative;display:none;}' +
      '.sw-menu-btn{display:inline-flex;align-items:center;gap:6px;font-size:13px;font-weight:600;padding:7px 12px;border-radius:var(--radius-sm);border:1px solid var(--border-strong);background:var(--surface);color:var(--text);cursor:pointer;transition:var(--tr);}' +
      '.sw-menu-btn:hover{border-color:var(--accent);color:var(--accent);background:var(--accent-soft);}' +
      '.sw-menu-panel{position:absolute;right:0;top:calc(100% + 8px);min-width:252px;max-width:84vw;background:color-mix(in srgb,var(--surface) 88%,transparent);-webkit-backdrop-filter:blur(12px);backdrop-filter:blur(12px);border:1px solid var(--border);border-radius:var(--radius-sm);box-shadow:var(--shadow-lg);padding:7px;display:none;z-index:60;max-height:78vh;overflow:auto;}' +
      '.sw-menu-panel.open{display:block;animation:sw-pop .16s ease;}' +
      '.sw-menu-gbtn{display:flex;width:100%;align-items:center;gap:8px;font-size:10px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;color:var(--text-faint);padding:9px 10px 6px;background:none;border:0;cursor:pointer;text-align:left;}' +
      '.sw-menu-gbtn:hover{color:var(--accent);}' +
      '.sw-menu-gbtn .sw-caret{margin-left:auto;font-size:8px;}' +
      '.sw-menu-sec{display:none;}' +
      '.sw-menu-sec.open{display:block;}' +
      '.sw-menu-link{display:flex;align-items:center;gap:9px;font-size:14px;font-weight:500;padding:8px 10px;border-radius:9px;color:var(--text);text-decoration:none;transition:var(--tr);}' +
      '.sw-menu-link:hover{background:var(--surface-2);}' +
      '.sw-menu-link.active{background:var(--accent-soft);color:var(--accent);}' +
      '.sw-mi-ic{font-size:15px;width:20px;text-align:center;display:inline-block;}' +
      '.sw-mi-ext{margin-left:auto;color:var(--text-faint);font-size:12px;}' +
      '@media (max-width:760px){.sw-group{display:none;}.sw-menu{display:block;}}' +
      '@media (max-width:430px){.sw-cta .sw-cta-lb{display:none;}}' +
      '@media (max-width:520px){.sw-menu-panel{position:fixed;left:10px;right:10px;top:60px;min-width:0;max-width:none;}}' +
      'header nav:not(.sw-nav){visibility:hidden;}';
    document.head.appendChild(st);
  })();

  // Single source of truth for the nav. Add deployed pages here.
  var NAV_GROUPS = [
    { g: 'Markets', items: [
      ['./nse-bse-dashboard.html', '📈', 'Stocks'],
      ['./watchlist.html',         '📌', 'Watchlist'],
      ['./sectors.html',           '🔥', 'Sectors'],
      ['./fii-dii.html',           '🌐', 'FII/DII'],
      ['./shareholding.html',      '🏛️', 'FII/DII Holdings'],
      ['./bank-credit.html',       '🏦', 'Banking Growth'],
      ['./market-mood.html',       '🌡️', 'Market Mood'],
      ['./results-season.html',    '📊', 'Results Season'],
      ['./discovery.html',         '💸', 'Smart Money Picks'],
      ['./deals.html',             '🐋', 'Bulk/Block Deals'],
      ['./insider.html',           '🕵️', 'Insider Trades'],
      ['./quarterly-results.html', '🧾', 'Quarterly Results'],
      ['./announcements.html',     '📢', 'Announcements']
    ] },
    { g: 'Funds', items: [
      ['./mutual-funds.html',                       '💰', 'Mutual Funds'],
      ['https://dhruvan246.github.io/fno-dashboard/', '🎯', 'F&O']
    ] },
    { g: 'Tools', items: [
      ['./saved-strategies.html', '⭐', 'Saved strategies'],
      ['./live-tracking.html',    '📡', 'Live tracking'],
      ['./backtest-history.html', '🕘', 'Backtest history'],
      ['./insurer-inbox.html',    '📥', 'Insurer inbox'],
      ['./analytics.html',        '👀', 'Page stats'],
      ['./results-coverage.html', '✅', 'Results coverage'],
      ['./status.html',           '🩺', 'Data health']
    ] }
  ];
  var NAV_CTA = ['./stock-backtest.html', '🧪', 'Create a strategy'];

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
  var PRIVATE_PAGES = ['watchlist.html', 'live-tracking.html', 'insurer-inbox.html', 'analytics.html', 'status.html', 'results-coverage.html'];
  var IS_OWNER = false; try { IS_OWNER = !!localStorage.getItem('bt_owner_key'); } catch (e) {}
  if (!IS_OWNER) NAV_GROUPS.forEach(function (g) {
    g.items = g.items.filter(function (it) { return PRIVATE_PAGES.indexOf(it[0].replace('./', '')) < 0; });
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
        var href = it[0], ic = it[1], lb = it[2], ext = href.indexOf('http') === 0;
        return '<a class="sw-menu-link' + (isActive(href) ? ' active' : '') + '" href="' + esc(href) + '"' +
          (ext ? ' target="_blank" rel="noopener"' : '') + ' role="menuitem">' +
          '<span class="sw-mi-ic" aria-hidden="true">' + ic + '</span>' + esc(lb) +
          (ext ? '<span class="sw-mi-ext" aria-hidden="true">↗</span>' : '') + '</a>';
      };
      var caret = '<span class="sw-caret" aria-hidden="true">▾</span>';
      var activeGi = -1; // which group holds the current page (for highlight / default-open)
      NAV_GROUPS.forEach(function (grp, gi) {
        if (activeGi < 0 && grp.items.some(function (it) { return isActive(it[0]); })) activeGi = gi;
      });

      // Desktop: one hover dropdown per group (Markets / Funds / Tools)
      var groupsHtml = NAV_GROUPS.map(function (grp, gi) {
        return '<div class="sw-group' + (gi === NAV_GROUPS.length - 1 ? ' align-r' : '') + '">' +
          '<button class="sw-group-btn' + (gi === activeGi ? ' active' : '') + '" type="button" aria-haspopup="true" aria-expanded="false">' +
            esc(grp.g) + caret + '</button>' +
          '<div class="sw-group-panel" role="menu">' + grp.items.map(link).join('') + '</div>' +
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
            '<span aria-hidden="true">☰</span><span class="sw-menu-btn-lb">Menu</span></button>' +
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
    } catch (e) { nav.style.visibility = 'visible'; }
  }

  // =========================================================================
  // SITE FOOTER — injected on every page so all pages share one footer with
  // the same links as the header Menu. Replaces any hardcoded <footer> and
  // keeps its old text as a fine-print credits line.
  // =========================================================================
  function buildFooter() {
    if (document.querySelector('.sw-footer')) return;
    var old = document.querySelector('footer');
    var credits = old ? old.textContent.replace(/\s+/g, ' ').trim() : '';
    var link = function (it) {
      var ext = it[0].indexOf('http') === 0;
      return '<a class="sw-f-link" href="' + esc(it[0]) + '"' + (ext ? ' target="_blank" rel="noopener"' : '') + '>' +
        '<span aria-hidden="true">' + it[1] + '</span>' + esc(it[2]) + (ext ? ' ↗' : '') + '</a>';
    };
    var cols = NAV_GROUPS.map(function (g) {
      var items = g.g === 'Tools' ? [NAV_CTA].concat(g.items) : g.items;
      return '<div class="sw-f-col"><div class="sw-f-h">' + esc(g.g) + '</div>' + items.map(link).join('') + '</div>';
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
      '</div></div>';
    if (old && old.parentNode) { old.parentNode.replaceChild(f, old); }
    else { document.body.appendChild(f); }
  }

  // header drops a stronger shadow once the page is scrolled
  function watchHeader() {
    var h = document.querySelector('header'); if (!h) return;
    var on = function () { h.classList.toggle('sw-scrolled', (window.scrollY || document.documentElement.scrollTop || 0) > 8); };
    window.addEventListener('scroll', on, { passive: true });
    on();
  }

  // =========================================================================
  // RESPONSIVE TABLES — reflow wide data tables into stacked label/value cards
  // on phones. We tag every wide table (>=4 cols, header has no colspan) with
  // .sw-cardify and stamp each body cell with data-label = its column header;
  // the CSS above (theme.css @media max-width:640px) does the rest. A
  // MutationObserver re-runs it so JS-rendered / sorted tables stay labelled.
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
  function cardifyAll() {
    var t = document.querySelectorAll('table');
    for (var i = 0; i < t.length; i++) { try { cardifyTable(t[i]); } catch (e) {} }
  }
  function watchTables() {
    cardifyAll();
    if (!('MutationObserver' in window)) return;
    var timer = null;
    new MutationObserver(function (muts) {
      for (var i = 0; i < muts.length; i++) {
        var added = muts[i].addedNodes; if (!added || !added.length) continue;
        for (var j = 0; j < added.length; j++) {
          var n = added[j]; if (n.nodeType !== 1) continue;
          if ((n.matches && n.matches('table,tbody,tr,td,th')) || (n.querySelector && n.querySelector('table,tr'))) {
            if (timer) clearTimeout(timer);
            timer = setTimeout(cardifyAll, 80);            // debounce: run once after rows settle
            return;
          }
        }
      }
    }).observe(document.body, { childList: true, subtree: true });
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
        // cross-device preference sync (owner browsers push; everyone pulls)
        try { window.swSync.syncSettings(['sw_theme', 'sw_sec_watch', 'bt_fav_strategies',
          'live_worker_url', 'savedRotations', 'savedCatRot', 'savedBOB', 'sw_dash_presets', 'sw_triage_hide']); } catch (e) {}
      };
      document.head.appendChild(s1);
    } catch (e) {}
  }

  function init() { buildNav(); buildFooter(); build(); watchHeader(); watchTables(); loadFeatures(); }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
