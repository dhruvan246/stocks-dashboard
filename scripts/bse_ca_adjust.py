# -*- coding: utf-8 -*-
"""Bonus/split adjustment for BSE-only close series — ONE function for every consumer (runbook §228d, 2026-10-13).
Used by build_bse_slices.py (the stock page's price slice) and build_bse_results.py (result-day reaction + since-drift),
so the chart and the Quarterly Results card can never adjust differently. The store (docs/bse_prices.bin) stays raw.
Actions come from scripts/bse_ca.json (fetch_bse_ca.py): BSE's announced bonuses and splits with ex-dates."""
import json, os

HERE = os.path.dirname(os.path.abspath(__file__))
CA_PATH = os.path.join(HERE, "bse_ca.json")


def load_ca(path=CA_PATH):
    """{code: [[ex, type, factor, text], …]} — empty when the store is missing (consumers then stay unadjusted)."""
    try:
        return {k: (v.get("acts") or []) for k, v in (json.load(open(path, encoding="utf-8")).get("co") or {}).items()}
    except (OSError, ValueError):
        return {}


def adjust_series(d, c, v, acts):
    """Bonus/split-adjusted closes for one BSE-only series (runbook §228d, 2026-10-13).
    d, c, v: raw bars as BSE printed them (dates asc, closes 2 dp, volumes). acts: [[ex, type, factor, text], …] from
    scripts/bse_ca.json. Same-day actions multiply (ALSTONE 14-Dec-2022: bonus 9:1 × split 10→1 = ÷100). An action is
    APPLIED only when the tape confirms it: r = close on the first bar on/after ex ÷ close on the bar before must be at most
    sqrt(1/F) — a fall of at least half the expected move (log scale) — and at least (1/F)/50; else it is HELD (returned, not
    applied: §149 crash-vs-CA rule — never divide out a move the tape does not show). Ex-dates outside the series do nothing.
    Returns (adjusted closes 4 dp, adjusted volumes, applied [[ex, F, r]], held [[ex, F, r or None]])."""
    byex = {}
    for ex, _t, f, _txt in acts or []:
        try:
            f = float(f)
        except (TypeError, ValueError):
            continue
        if f > 1:
            byex[int(ex)] = byex.get(int(ex), 1.0) * f
    if not byex or len(d) < 2:
        return list(c), list(v), [], []
    applied, held = [], []
    mult = [1.0] * len(d)
    for ex in sorted(byex):
        F = byex[ex]
        i = next((k for k, dd in enumerate(d) if dd >= ex), None)
        if i is None or i == 0 or ex > d[-1]:
            continue                                   # before the first bar or after the last: no effect on this series
        prev, cur = c[i - 1], c[i]
        r = (cur / prev) if prev and cur else None
        e = 1.0 / F
        if r is not None and (e / 50.0) <= r <= e ** 0.5:
            for k in range(i):
                mult[k] /= F
            applied.append([ex, round(F, 6), round(r, 4)])
        else:
            held.append([ex, round(F, 6), None if r is None else round(r, 4)])
    c2 = [round(x * m, 4) for x, m in zip(c, mult)]
    v2 = [int(round(x / m)) if m else x for x, m in zip(v, mult)] if v and len(v) == len(d) else list(v)
    return c2, v2, applied, held
