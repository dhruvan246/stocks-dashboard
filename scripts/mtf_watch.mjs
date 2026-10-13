#!/usr/bin/env node
// MTF WATCH (user 2026-10-13: "why didn't you check it daily and tell me — we would have converted"). CPPLUS was bought in
// cash on 1 Oct because Zerodha did not fund it on MTF; by 13 Oct it was 2x and nobody noticed. Every trading day at 10:00 IST
// this asks Zerodha's own margin calculator (through the user's kite worker, on the day's login) for the MTF leverage of
//   • every stock the account holds in CASH (delivery) — eligible now = cash that switching would free, and
//   • every stock held on MTF and every stock in the strategies' current picks — a change in either direction,
// compares with yesterday's answer (kept in the token-gated <token>.mtfwatch row) and alerts the user's ntfy topic on a change.
// No Kite login that day → one "not checked today" alert. This repo and its logs are PUBLIC: the log prints counts only.
// env: PF_HOLDINGS_TOKEN, PF_KITE_WORKER, NTFY_TOPIC (secrets, same as the auto-pilot). MTF_FORCE=1 runs on a holiday too;
// MTF_DRY=1 (local only) prints the alert lines and sends/saves nothing.
import { readFileSync } from 'node:fs';
import { gzipSync, gunzipSync } from 'node:zlib';

const SUPA = 'https://nebjnsndgrhumnkuipqy.supabase.co/rest/v1/rpc/';
const ANON = 'sb_publishable_MDlQwiVc5deii91__UNeDg_z9r4Fk98';            // public (docs/sw-sync.js)
const OWNER = 'sw_owner_8Kq2Lm9Xp4Rt7v';                                  // public (docs/strategies-panel.js ZBA_SECRET)
const TOKEN = (process.env.PF_HOLDINGS_TOKEN || '').trim();
const WORKER = (process.env.PF_KITE_WORKER || '').trim().replace(/\/+$/, '');
const NTFY = (process.env.NTFY_TOPIC || '').trim();
const say = m => console.log('[mtf-watch] ' + m);
const ELIG = 1.05;                                                       // leverage above this = Zerodha funds it on MTF

const HOL = (() => { try { const d = JSON.parse(readFileSync(new URL('../docs/nse_holidays.json', import.meta.url)));
  return new Set(Object.values(d.holidays || {}).flatMap(y => Object.keys(y))); } catch (e) { return new Set(); } })();
const todayIST = () => new Date(Date.now() + 330 * 60000).toISOString().slice(0, 10);

async function rpc(fn, args) {
  const r = await fetch(SUPA + fn, { method: 'POST', headers: { apikey: ANON, Authorization: 'Bearer ' + ANON, 'Content-Type': 'application/json' }, body: JSON.stringify(args) });
  if (!r.ok) throw new Error(fn + ' HTTP ' + r.status);
  const t = await r.text(); return t ? JSON.parse(t) : null;
}
const rowGet = async tok => { const r = await rpc('pf_feed_get', { token: tok }); return r && r.z ? JSON.parse(gunzipSync(Buffer.from(r.z, 'base64')).toString()) : null; };
const rowSet = async (tok, doc) => { const z = gzipSync(Buffer.from(JSON.stringify(doc))).toString('base64');
  const ok = await rpc('pf_feed_set', { secret: OWNER, token: tok, payload: { z } }); if (ok !== true) throw new Error('state row not saved'); };
async function kite(path, body) {
  const r = await fetch(WORKER + path, { method: body ? 'POST' : 'GET', headers: { 'X-PF-Token': TOKEN, 'Content-Type': 'application/json' }, body: body ? JSON.stringify(body) : undefined });
  const j = await r.json().catch(() => null); return { st: r.status, j };
}
async function ntfy(title, body, prio) {
  if (!NTFY) { say('alert skipped (no NTFY_TOPIC secret)'); return; }
  const r = await fetch('https://ntfy.sh/' + encodeURIComponent(NTFY), { method: 'POST', body, headers: { Title: title, Priority: String(prio || 3), Tags: 'moneybag' } });
  say('alert ' + (r.ok ? 'sent' : 'failed HTTP ' + r.status));
}
const lakh = v => '₹' + (v / 1e5).toFixed(1) + ' L';
const levTxt = l => (l > ELIG ? l + 'x' : 'no MTF');

async function main() {
  if (!TOKEN || !WORKER) { say('missing secrets'); process.exit(1); }
  const day = todayIST(), dow = new Date(day + 'T00:00:00Z').getUTCDay();
  if ((dow === 0 || dow === 6 || HOL.has(day)) && process.env.MTF_FORCE !== '1') { say('not a trading day — nothing to do'); return; }
  const st = await kite('/status');
  if (!(st.st === 200 && st.j && st.j.connected)) {
    say('Kite not logged in — not checked');
    await ntfy('MTF check: not done today', 'Zerodha is not logged in today, so the daily check of which stocks Zerodha funds on MTF did not run. Log in on the terminal; tomorrow\'s check runs as usual.', 2);
    return;
  }
  // 1. the account: cash-held vs MTF-held quantities
  const H = await kite('/holdings'); if (H.st !== 200 || !H.j || !Array.isArray(H.j.data)) throw new Error('holdings HTTP ' + H.st);
  const held = {};
  for (const h of H.j.data) { const m = h.mtf || {}, cash = (+h.quantity || 0) + (+h.t1_quantity || 0), mq = +m.quantity || 0;
    if (cash > 0 || mq > 0) held[h.tradingsymbol] = { cash, mtf: mq, ltp: +h.last_price || 0 }; }
  // 2. the strategies' current picks (the nightly model), under Zerodha's name for today
  const picks = new Set();
  try { const m = await rowGet(TOKEN + '.model'); if (m) { let S = Object.assign({}, m.strategies || {});
      for (let i = 2; i <= (+m.parts || 1); i++) { const p = await rowGet(TOKEN + '.model' + i); if (p) Object.assign(S, p.strategies || {}); }
      Object.values(S).forEach(s => (((s.picks || {}).rows) || []).slice(0, 3).forEach(r => r.sym && picks.add(r.sym))); } }
  catch (e) { say('picks not read: ' + e.message); }
  if (picks.size) { const q = await kite('/ticks/symbols?i=' + encodeURIComponent([...picks].join(',')));
    const map = (q.j && q.j.map) || {}; const named = new Set(); picks.forEach(s => named.add(map[s] || s)); picks.clear(); named.forEach(s => picks.add(s)); }
  const syms = [...new Set([...Object.keys(held), ...picks])].sort();
  // 3. Zerodha's own answer: the leverage field of an MTF margin quote (size-independent)
  const lev = {};
  for (let i = 0; i < syms.length; i += 20) {   // the worker takes 1-20 orders a call
    const part = syms.slice(i, i + 20);
    const mc = await kite('/margincalc', part.map(s => ({ exchange: 'NSE', tradingsymbol: s, product: 'MTF', quantity: 1, transaction_type: 'BUY', variety: 'regular', order_type: 'MARKET' })));
    if (mc.st !== 200 || !mc.j || !Array.isArray(mc.j.data)) throw new Error('margin calculator HTTP ' + mc.st + ' ' + ((mc.j && (mc.j.message || mc.j.error)) || ''));
    part.forEach((s, k) => { const d = mc.j.data[k]; if (d && d.leverage != null) lev[s] = +d.leverage; });
  }
  // 4. compare with the last run
  const prevDoc = await rowGet(TOKEN + '.mtfwatch').catch(() => null), prev = (prevDoc && prevDoc.lev) || {}, first = !prevDoc;
  const lines = [];
  for (const s of Object.keys(held)) { const h = held[s], l = lev[s]; if (l == null) continue;
    if (h.cash > 0 && l > ELIG && (first || !(prev[s] > ELIG) || prev[s] !== l)) {
      const val = h.cash * h.ltp, freed = val * (1 - 1 / l);
      lines.push(s + ': now ' + l + 'x on MTF — you hold ' + h.cash.toLocaleString('en-IN') + ' in cash (' + lakh(val) + '); on MTF about ' + lakh(freed) + ' would be freed');
    }
    if (h.mtf > 0 && prev[s] > ELIG && l !== prev[s]) lines.push(s + ': MTF leverage ' + prev[s] + 'x → ' + levTxt(l) + ' (you hold ' + h.mtf.toLocaleString('en-IN') + ' on MTF)');
  }
  if (!first) for (const s of picks) { if (held[s]) continue; const l = lev[s], p = prev[s]; if (l == null || p == null) continue;
    if ((l > ELIG) !== (p > ELIG) || (l > ELIG && l !== p)) lines.push(s + ' (a current pick): ' + levTxt(p) + ' → ' + levTxt(l)); }
  say('checked ' + syms.length + ' stocks (' + Object.values(held).filter(h => h.cash > 0).length + ' held in cash) · ' + lines.length + ' change(s)' + (first ? ' · first run' : ''));
  if (process.env.MTF_DRY === '1') { console.log(lines.join('\n') || '(no changes)'); return; }   // local look only — never in CI (public log)
  if (lines.length) await ntfy('MTF check: ' + lines.length + ' change' + (lines.length === 1 ? '' : 's'), lines.join('\n'), 4);
  await rowSet(TOKEN + '.mtfwatch', { day, at: new Date().toISOString(), lev });
}
main().catch(async e => { say('failed: ' + (e && e.message)); try { await ntfy('MTF check failed', 'The daily MTF check stopped: ' + (e && e.message) + '. It retries tomorrow.', 3); } catch (x) {} process.exit(1); });
