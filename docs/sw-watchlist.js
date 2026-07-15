// STOCKSWORLD watchlist — star any stock anywhere, notes, synced across devices.
// Loaded on EVERY page by theme.js (after sw-sync.js).
//
// HOW A PAGE OPTS IN: render `<span class="sw-star" data-sym="RELIANCE"></span>`
// inside any row/card. This file does the rest: a MutationObserver paints every
// .sw-star that appears (☆/★), one delegated click handler toggles, and state
// syncs via swSync kv 'WATCHLIST' ([{s,note,ts,nts}]).
//
// OWNER browsers (unlocked once via ?ownerkey=…) read/write the SHARED list in
// Supabase — that's the cross-device sync. Everyone else gets a private,
// browser-local list (never pushed), so visitors can't scribble on the owner's
// list and the owner's stars don't show up pre-ticked for strangers.
(function (g) {
  'use strict';
  if (g.swWatch) return;
  const LOCAL_KEY = 'sw_watch_local'; // non-owner private list
  let _list = null;                   // in-memory cache: [{s,note,ts,nts}]
  let _loaded = false;

  const isOwner = () => !!(g.swSync && g.swSync.isOwner());
  const _localGet = () => { try { return JSON.parse(localStorage.getItem(LOCAL_KEY) || '[]'); } catch (e) { return []; } };
  const _localSet = a => { try { localStorage.setItem(LOCAL_KEY, JSON.stringify(a)); } catch (e) {} };

  async function load() {
    if (_loaded) return _list;
    _list = isOwner() && g.swSync ? await g.swSync.kvGet('WATCHLIST') : _localGet();
    if (!Array.isArray(_list)) _list = [];
    _loaded = true; paintAll(); fire();
    return _list;
  }
  async function save() {
    if (isOwner() && g.swSync) await g.swSync.kvSet('WATCHLIST', _list);
    else _localSet(_list);
  }
  const list = () => _list || [];
  const entry = s => list().find(e => e && e.s === s);
  const has = s => !!entry(s);

  async function toggle(s) {
    if (!s) return;
    await load();
    const i = _list.findIndex(e => e && e.s === s);
    if (i >= 0) _list.splice(i, 1);
    else _list.unshift({ s, note: '', ts: Date.now(), nts: 0 });
    paintAll(); fire();
    await save();
  }
  async function setNote(s, note) {
    await load();
    const e = entry(s);
    if (!e) { _list.unshift({ s, note: note || '', ts: Date.now(), nts: Date.now() }); }
    else { e.note = note || ''; e.nts = Date.now(); }
    fire();
    await save();
  }
  async function remove(s) { await load(); const i = _list.findIndex(e => e && e.s === s); if (i >= 0) { _list.splice(i, 1); paintAll(); fire(); await save(); } }

  // ---- painting ----
  function paint(el) {
    const s = el.getAttribute('data-sym');
    const on = has(s);
    el.textContent = on ? '★' : '☆';
    el.classList.toggle('sw-star-on', on);
    if (!el.hasAttribute('title')) el.title = 'Add to / remove from watchlist';
  }
  function paintAll(root) { (root || document).querySelectorAll('.sw-star[data-sym]').forEach(paint); }
  function fire() { try { document.dispatchEvent(new CustomEvent('sw-watch-change', { detail: { list: list() } })); } catch (e) {} }

  // one delegated click handler for every star on the page, present or future
  document.addEventListener('click', ev => {
    const el = ev.target && ev.target.closest && ev.target.closest('.sw-star[data-sym]');
    if (!el) return;
    ev.preventDefault(); ev.stopPropagation(); // don't trigger the row's own click (row-open etc.)
    toggle(el.getAttribute('data-sym'));
  }, true);

  // paint stars as pages render rows dynamically
  const mo = new MutationObserver(muts => {
    for (const m of muts) for (const n of m.addedNodes) {
      if (n.nodeType !== 1) continue;
      if (n.matches && n.matches('.sw-star[data-sym]')) paint(n);
      if (n.querySelectorAll) n.querySelectorAll('.sw-star[data-sym]').forEach(paint);
    }
  });

  // minimal styling (works with the site's light theme + card reflow)
  const css = document.createElement('style');
  css.textContent = '.sw-star{cursor:pointer;user-select:none;color:#94a3b8;font-size:1.05em;line-height:1;display:inline-block;padding:0 .15em;transition:transform .12s ease,color .12s ease}' +
    '.sw-star:hover{transform:scale(1.25);color:#f59e0b}' +
    '.sw-star-on{color:#f59e0b}';
  document.head.appendChild(css);

  function boot() { mo.observe(document.body, { childList: true, subtree: true }); load(); }
  document.body ? boot() : addEventListener('DOMContentLoaded', boot);

  g.swWatch = { load, list, has, entry, toggle, setNote, remove, paintAll, isOwner,
    starHTML: s => '<span class="sw-star" data-sym="' + String(s).replace(/"/g, '&quot;') + '"></span>' };
})(window);
