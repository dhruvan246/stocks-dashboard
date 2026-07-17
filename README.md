# StocksWorld — Dhruvan's Stocks Dashboard

An NSE/BSE + mutual-funds analytics site served by GitHub Pages from `docs/`:
**https://dhruvan246.github.io/stocks-dashboard/**

~20 pages (dashboard, backtester, quarterly results, discovery, sectors, FII/DII,
deals, insiders, IPOs, and more), self-updating via the GitHub Actions workflows
in `.github/workflows/`. Data health is monitored on the site's
[status page](https://dhruvan246.github.io/stocks-dashboard/status.html).

## The one document that matters

➡️ **`scripts/DATA_RUNBOOK.md`** — the canonical guide to every pipeline:
fetching, refreshing, backfilling, building, deploying, plus all the gotchas.
Read it FIRST before touching any data or workflow. It has a table of contents.

(An older version of this README described the original Yahoo-Finance pipeline —
that is long gone; the runbook is current.)
