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
    var host = document.querySelector('header nav')
            || document.querySelector('header .max-w-screen-xl')
            || document.querySelector('header > div')
            || document.querySelector('header');
    if (host) { host.appendChild(box); }
    else { box.classList.add('floating'); document.body.appendChild(box); }
    updateUI(document.documentElement.getAttribute('data-theme'));
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', build);
  else build();
})();
