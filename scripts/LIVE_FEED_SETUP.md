# Live price feed — setup (≈10 minutes, ₹0)

Your dashboard is a **static** site, so it can't fetch live quotes by itself.
We deploy a tiny free **Cloudflare Worker** that fetches prices and hands them to
the page. The Worker code is in `scripts/live-quote-worker.js`.

Source = **Yahoo Finance NSE feed** (free, no broker). It's **~15 minutes delayed** —
fine for placing limit orders, not tick-by-tick. (Want true real-time? See the
bottom of this file.)

---

## Step 1 — Create a free Cloudflare account
1. Go to https://dash.cloudflare.com/sign-up and sign up (free, no card).

## Step 2 — Create the Worker
1. In the dashboard left menu: **Workers & Pages** → **Create application** → **Create Worker**.
2. Give it a name, e.g. `stocksworld-quotes`. Click **Deploy** (it deploys a hello-world).
3. Click **Edit code**.
4. Delete everything in the editor, then **paste the entire contents** of
   `scripts/live-quote-worker.js`.
5. Click **Deploy** (top right).

## Step 3 — Copy your Worker URL
After deploy you'll see a URL like:
```
https://stocksworld-quotes.<your-subdomain>.workers.dev
```
Copy it. Test it in your browser:
```
https://stocksworld-quotes.<your-subdomain>.workers.dev/?symbols=RELIANCE,TCS
```
You should see JSON with `ltp` values.

## Step 4 — Connect it to the dashboard
1. Open the **Saved Strategies** page → click **🎯 Today's Picks** on any card.
2. Click **⚡ Go Live** → it asks for your Worker URL the first time → paste it.
3. Done. The basket now shows **Live ₹**, **Δ% vs close**, and re-allocates shares
   at live prices. The URL is saved in your browser for next time.

To change it later: click the **⚙** next to **Go Live**.

---

## Notes & limits
- **The same Worker now serves seven routes** (latest: `?gift=1` added 2026-08-25 —
  redeploy the whole `scripts/live-quote-worker.js` file to get them): `?symbols=RELIANCE,TCS` (Today's-Picks
  quotes + Watchlist live prices + Macro's live Nifty/VIX; index symbols like `^NSEI`
  work too), `?chart=^NSEI` (home-page live ticker + stock-page live price),
  `?quotes=^GSPC,GC=F,BTC-USD` (VERBATIM Yahoo symbols — futures/FX/crypto — for the
  Global Markets page), `?announcements=1` (Announcements + Quarterly-Results live
  top-up, NSE with cookie warmup + 90 s cache),
  `?nse=volume-gainers|gainers|loosers|fiidii|large-deals` (whitelisted NSE
  live-analysis passthrough — live volume spurts for Volume Shockers, live top movers
  for Top Movers, same-evening provisional FII/DII for the FII/DII page, same-evening
  bulk/block deals for the Deals page, 60 s cache), `?ipo=SYM` (live subscription
  multiple for an open IPO — IPOs page), and `?gift=1` (live GIFT NIFTY — the Nifty
  futures contract at NSE IX, which Yahoo doesn't carry — quote + intraday sparkline
  from www.nseix.com for the home-page ticker, 30 s cache; without this route the
  home card falls back to public CORS proxies, which are flaky). The Indices and Monthly-Returns pages need
  NO worker — they read the CORS-open `liveindexsa.niftyindices.com` CDN feed directly.
- **Free Cloudflare Workers** allow 100,000 requests/day — far more than you'll use.
- Each **Go Live** click = one request (covers all basket symbols at once).
- Yahoo NSE data is **~15 min delayed**. The "as of" time shown is when you clicked.
- If a symbol is BSE-only (no `.NS` listing) it may be missing — those rows keep
  their daily close.
- **Never put broker API keys in the website.** They'd be public. Keys belong only
  inside the Worker (Cloudflare keeps them private).

## Want true real-time (tick) prices?
Swap the Yahoo fetch in `live-quote-worker.js` for a broker quote API:
- **Free with an account (need a daily login token):** Dhan, Angel One SmartAPI, Upstox.
- **Paid:** Zerodha Kite Connect (₹2,000/mo).
Tell me which broker and I'll rewrite the Worker for it (the page side won't change).
