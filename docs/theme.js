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
      '.sw-group-panel--mega{flex-wrap:wrap;gap:2px 12px;min-width:0;width:612px;max-width:88vw;}' +
      '.sw-group.open>.sw-group-panel--mega{display:flex;}' +
      '.sw-mega-col{display:flex;flex-direction:column;flex:1 1 190px;min-width:186px;}' +
      '.sw-mega-h{font-size:10px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;color:var(--accent);padding:8px 10px 5px;}' +
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
      '.sw-tabs{margin:0 0 14px;}' +
      '.sw-tabs-in{display:flex;align-items:center;gap:6px;overflow-x:auto;scrollbar-width:none;-webkit-overflow-scrolling:touch;}' +
      '.sw-tabs-in::-webkit-scrollbar{display:none;}' +
      '.sw-tab{display:inline-flex;align-items:center;gap:6px;white-space:nowrap;padding:8px 15px;border-radius:.65rem;font-size:13.5px;font-weight:800;color:var(--text-muted);text-decoration:none;border:1px solid transparent;transition:var(--tr);}' +
      '.sw-tab:hover{background:var(--surface-2);color:var(--text);}' +
      '.sw-tab.on{background:var(--accent-soft);color:var(--accent);border-color:var(--border-strong);}' +
      '@media (max-width:640px){.sw-tab{padding:7px 11px;font-size:12.5px;}}' +
      '@media (max-width:760px){.sw-group{display:none;}.sw-menu{display:block;}}' +
      '@media (max-width:430px){.sw-cta .sw-cta-lb{display:none;}}' +
      '@media (max-width:520px){.sw-menu-panel{position:fixed;left:10px;right:10px;top:60px;min-width:0;max-width:none;}}' +
      '.sw-bbar{display:none;}' +
      '@media (max-width:760px){' +
        '.sw-bbar{position:fixed;bottom:0;left:0;right:0;z-index:55;display:flex;background:color-mix(in srgb,var(--surface) 92%,transparent);-webkit-backdrop-filter:saturate(170%) blur(14px);backdrop-filter:saturate(170%) blur(14px);border-top:1px solid var(--border);padding-bottom:env(safe-area-inset-bottom);}' +
        '.sw-bbar-it{flex:1;display:flex;flex-direction:column;align-items:center;gap:2px;padding:7px 0 8px;font-size:10.5px;font-weight:600;color:var(--text-muted);text-decoration:none;background:none;border:0;cursor:pointer;transition:var(--tr);}' +
        '.sw-bbar-it .ic{font-size:17px;line-height:1;}' +
        '.sw-bbar-it.on{color:var(--accent);}' +
        'body{padding-bottom:calc(56px + env(safe-area-inset-bottom));}' +
      '}' +
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
        ['./nse-bse-dashboard.html', '📈', 'Stocks'],
        ['./movers.html',            '📊', 'Top Movers'],
        ['./indices.html',           '📇', 'Indices'],
        ['./sectors.html',           '🔥', 'Sectors'],
        ['./monthly-returns.html',   '🗓️', 'Monthly Returns'],
        ['./market-mood.html',       '🌡️', 'Market Mood'],
        ['./macro.html',             '🌍', 'Macro'],
        ['./global.html',            '🌏', 'Global Markets'],
        ['./watchlist.html',         '📌', 'Watchlist']
      ] },
      { sub: 'Flows & Ownership', items: [
        ['./fii-dii.html',           '🌐', 'FII/DII Flows'],
        ['./shareholding.html',      '🏛️', 'Stock Holdings'],
        ['./deals.html',             '🐋', 'Bulk/Block Deals'],
        ['./insider.html',           '🕵️', 'Insider Trades'],
        ['./delivery.html',          '📦', 'Delivery Spikes'],
        ['./volume.html',            '⚡', 'Volume Shockers'],
        ['./bank-credit.html',       '🏦', 'Banking Growth']
      ] },
      { sub: 'Discovery & Filings', items: [
        ['./discovery.html',         '💸', 'Smart Money Picks'],
        ['./quarterly-results.html', '🧾', 'Quarterly Results'],
        ['./announcements.html',     '📢', 'Announcements'],
        ['./ipos.html',              '🚀', 'IPOs & Listings'],
        ['./actions.html',           '📅', 'Ex-Dates Calendar']
      ] }
    ] },
    { g: 'Funds', items: [
      ['./mutual-funds.html',                       '💰', 'Mutual Funds'],
      ['./backtest.html',                           '🧮', 'MF Backtest'],
      ['https://dhruvan246.github.io/fno-dashboard/', '🎯', 'F&O']
    ] },
    { g: 'Tools', items: [
      ['./saved-strategies.html', '⭐', 'Saved Strategies'],
      ['./all-picks.html',        '🎯', 'All Picks'],
      ['./backtest-history.html', '🕘', 'Backtest History'],
      ['./live-tracking.html',    '📡', 'Live Tracking'],
      ['./status.html',           '🩺', 'Data Health'],
      ['./results-coverage.html', '✅', 'Results Coverage'],
      ['./analytics.html',        '👀', 'Page Stats'],
      ['./insurer-inbox.html',    '📥', 'Insurer Inbox']
    ] }
  ];
  var NAV_CTA = ['./stock-backtest.html', '🧪', 'Create a strategy'];

  // ---- PAGE GROUPS: sibling pages presented as ONE tabbed section. Each member
  // page keeps its own URL, payload and data pipeline (so deep links, feeds.json
  // and the perf story are untouched); buildTabs() injects a shared tab strip at
  // the top of <main> on every member for quick sibling hops. The nav / footer /
  // home tiles list every member individually (see NAV_GROUPS above). Merge or
  // split a section here — one place, applies everywhere. Private members are
  // hidden from non-owners.
  var PAGE_GROUPS = [
    { g: 'Market Analytics', tabs: [
      ['./movers.html',          '📊', 'Top Movers'],
      ['./indices.html',         '📇', 'Indices'],
      ['./monthly-returns.html', '🗓️', 'Monthly Returns'],
      ['./market-mood.html',     '🌡️', 'Market Mood']
    ] },
    { g: 'FII/DII', tabs: [
      ['./fii-dii.html',      '🌐', 'Daily Flows'],
      ['./shareholding.html', '🏛️', 'Stock Holdings']
    ] },
    { g: 'Deals & Insiders', tabs: [
      ['./deals.html',    '🐋', 'Bulk/Block Deals'],
      ['./insider.html',  '🕵️', 'Insider Trades'],
      ['./delivery.html', '📦', 'Delivery Spikes'],
      ['./volume.html',   '⚡', 'Volume Shockers']
    ] },
    { g: 'Corporate Calendar', tabs: [
      ['./ipos.html',    '🚀', 'IPOs & Listings'],
      ['./actions.html', '📅', 'Ex-Dates']
    ] },
    { g: 'Strategies', tabs: [
      ['./saved-strategies.html', '⭐', 'Saved Strategies'],
      ['./backtest-history.html', '🕘', 'Backtest History'],
      ['./live-tracking.html',    '📡', 'Live Tracking']
    ] },
    { g: 'Owner console', tabs: [
      ['./status.html',           '🩺', 'Data Health'],
      ['./results-coverage.html', '✅', 'Results Coverage'],
      ['./analytics.html',        '👀', 'Page Stats'],
      ['./insurer-inbox.html',    '📥', 'Insurer Inbox']
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
  var PRIVATE_PAGES = ['watchlist.html', 'live-tracking.html', 'insurer-inbox.html', 'analytics.html', 'status.html', 'results-coverage.html'];
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
      '.sw-srch-btn{display:inline-flex;align-items:center;gap:8px;margin-left:14px;padding:7px 11px;min-width:216px;border:1px solid var(--border-strong);border-radius:var(--radius-sm);background:var(--surface-2);color:var(--text-faint);font-size:13px;font-weight:500;cursor:pointer;transition:var(--tr);}' +
      '.sw-srch-btn:hover{border-color:var(--accent);color:var(--text-muted);background:var(--accent-soft);}' +
      '.sw-srch-btn .sw-srch-kbd{margin-left:auto;font-size:10.5px;font-weight:700;padding:2px 6px;border:1px solid var(--border-strong);border-radius:6px;color:var(--text-faint);background:var(--surface);}' +
      '@media (max-width:1040px){.sw-srch-btn{min-width:0;margin-left:8px;}.sw-srch-btn .sw-srch-lb,.sw-srch-btn .sw-srch-kbd{display:none;}}' +
      // Phones: the header has no room left (logo + ☰ + theme pill already fill it),
      // so search moves to the bottom bar's 🔍 tab instead.
      '@media (max-width:520px){.sw-srch-btn{display:none;}}' +
      '.sw-srch-ov{position:fixed;inset:0;z-index:90;display:none;justify-content:center;align-items:flex-start;padding:10vh 14px 14px;background:rgba(4,7,14,.58);-webkit-backdrop-filter:blur(4px);backdrop-filter:blur(4px);}' +
      '.sw-srch-ov.open{display:flex;animation:sw-pop .14s ease;}' +
      '.sw-srch-box{width:100%;max-width:620px;background:var(--surface);border:1px solid var(--border-strong);border-radius:var(--radius);box-shadow:var(--shadow-lg);overflow:hidden;display:flex;flex-direction:column;max-height:80vh;}' +
      '.sw-srch-top{display:flex;align-items:center;gap:9px;padding:12px 14px;border-bottom:1px solid var(--border);}' +
      '.sw-srch-in{flex:1;min-width:0;border:0;outline:0;background:transparent;color:var(--text);font-size:16px;font-weight:600;}' +
      '.sw-srch-in::placeholder{color:var(--text-faint);font-weight:500;}' +
      '.sw-srch-in::-webkit-search-cancel-button{display:none;}' +
      '.sw-srch-x{border:1px solid var(--border-strong);background:var(--surface-2);color:var(--text-faint);border-radius:7px;font-size:10.5px;font-weight:700;padding:3px 7px;cursor:pointer;}' +
      '.sw-srch-x:hover{color:var(--text);border-color:var(--accent);}' +
      '.sw-srch-list{overflow-y:auto;padding:6px;-webkit-overflow-scrolling:touch;}' +
      '.sw-srch-h{font-size:10px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;color:var(--text-faint);padding:9px 10px 5px;}' +
      '.sw-srch-it{display:flex;align-items:center;gap:9px;padding:8px 10px;border-radius:9px;text-decoration:none;color:var(--text);cursor:pointer;}' +
      '.sw-srch-it:hover,.sw-srch-it.on{background:var(--accent-soft);}' +
      '.sw-srch-it.on{box-shadow:inset 0 0 0 1px var(--border-strong);}' +
      '.sw-srch-sym{font-size:13.5px;font-weight:800;letter-spacing:.01em;white-space:nowrap;}' +
      '.sw-srch-it.on .sw-srch-sym{color:var(--accent);}' +
      '.sw-srch-nm{font-size:12.5px;color:var(--text-muted);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;min-width:0;flex:1;}' +
      '.sw-srch-it b{color:var(--accent);font-weight:800;}' +
      '.sw-srch-meta{margin-left:auto;font-size:11px;color:var(--text-faint);white-space:nowrap;}' +
      '.sw-srch-tag{font-size:9.5px;font-weight:800;text-transform:uppercase;letter-spacing:.04em;padding:2px 6px;border-radius:999px;background:var(--surface-3);color:var(--text-faint);white-space:nowrap;}' +
      '.sw-srch-empty{padding:20px 12px;text-align:center;font-size:13px;color:var(--text-faint);}' +
      '.sw-srch-foot{display:flex;gap:12px;flex-wrap:wrap;padding:8px 14px;border-top:1px solid var(--border);font-size:10.5px;color:var(--text-faint);background:var(--surface-2);}' +
      // Phones: a sheet near the top (so the on-screen keyboard doesn't cover the
      // results), hugging its content, with finger-sized rows and no desk-only chrome.
      '@media (max-width:640px){' +
        '.sw-srch-ov{padding:8px;}' +
        '.sw-srch-box{max-height:calc(100% - 16px);border-radius:var(--radius);}' +
        '.sw-srch-top{padding:11px 12px;}' +
        '.sw-srch-it{padding:11px 10px;min-height:44px;}' +
        '.sw-srch-meta{display:none;}.sw-srch-foot{display:none;}' +
      '}';
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
          '<span aria-hidden="true">🔍</span>' +
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
      btn.innerHTML = '<span aria-hidden="true">🔍</span>' +
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
      '</div></div>';
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
      '<a class="' + cls(here === 'index.html') + '" href="./index.html"><span class="ic" aria-hidden="true">🏠</span>Home</a>' +
      '<button type="button" class="' + cls(inMarkets) + '" data-sw="menu" aria-haspopup="true"><span class="ic" aria-hidden="true">📊</span>Markets</button>' +
      '<button type="button" class="sw-bbar-it" data-sw="search" aria-label="Search stocks"><span class="ic" aria-hidden="true">🔍</span>Search</button>' +
      '<a class="' + cls(here === 'stock-backtest.html') + '" href="./stock-backtest.html"><span class="ic" aria-hidden="true">🧪</span>Backtest</a>' +
      '<a class="' + cls(here === 'mutual-funds.html' || here === 'backtest.html') + '" href="./mutual-funds.html"><span class="ic" aria-hidden="true">💰</span>Funds</a>';
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
    cardifyAll(); fitViewport();
    if (!('MutationObserver' in window)) return;
    var timer = null, wantCards = false;
    var settle = function () { if (wantCards) { cardifyAll(); wantCards = false; } fitViewport(); };
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
        // cross-device preference sync (owner browsers push; everyone pulls)
        try { window.swSync.syncSettings(['sw_theme', 'sw_sec_watch', 'bt_fav_strategies',
          'live_worker_url', 'savedRotations', 'savedCatRot', 'savedBOB', 'sw_dash_presets', 'sw_triage_hide']); } catch (e) {}
      };
      document.head.appendChild(s1);
    } catch (e) {}
  }

  function init() { buildNav(); buildSearch(); buildTabs(); buildFooter(); buildBottomBar(); build(); watchHeader(); watchTables(); loadFeatures(); }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
