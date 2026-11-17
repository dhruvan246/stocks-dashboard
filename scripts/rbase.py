"""Restated comparative bases — the growth base a company itself printed (runbook §214b, user policy A, 2026-11-17).

A later filing prints, beside each current figure, the comparative figure of an earlier quarter. After a merger, a
demerger, a reclassification or an Ind AS 103 purchase-price allocation that comparative can differ from what the
earlier filing printed: Coal India's Jun-2026 filing shows Revenue from operations 46,254.80 against a Jun-2025 figure
of 42,919.20, while the Jun-2025 filing itself printed 35,842.19. Stored LEVELS stay as first filed (point-in-time,
runbook §40b). GROWTH, by the user's decision, compares against the figure printed beside the current one:
+7.77%, not the +29.05% that pairs two different definitions of revenue.

scripts/restated_bases.json is the hand-verified ledger (each entry read from the filing, never typed from a site):
  {SYM: {"F|B": {basis: {field: {"base", "cur", "as_filed", "level", "src", "read"}}}}}
    F, B      quarter-ends (YYYYMMDD) of the filing and of its comparative column: B = F - 1 year, or the quarter before F
    basis     "s" standalone | "c" consolidated
    field     "rev" revenue from operations (the row our store uses) | "pat" net profit (c = owners-attributable,
              the sf_fundamentals convention)
    base      the comparative printed in F's filing                                   (Rs crore)
    cur       the CURRENT column of that same row in F's filing — it ties the base to the row definition our store uses
    as_filed  what our store held for B when the entry was read (information only)
    level     "as_filed" = B's own filing printed as_filed (a true restatement); "unknown" = B's filing not re-read yet;
              "our_error" = our stored level for B is wrong (fixed separately through the level ledgers)
    src, read where it was read, and the date

ONE RULE, every consumer (Python: build_stock_fin / build_results_season / build_discovery; JS twins: stock.html rbBase,
quarterly-results.html growth, backtest-engine.js + stock-backtest.html _rbBase):
  the restated base replaces a STORED base (it never invents one where no base row is stored), and ONLY while the
  current figure in use equals `cur` within rounding
  (max(0.015, 0.2%)). A base read off another row — total income for revenue, group total for owners — can therefore
  never be paired with our current figure, and an entry whose current row is later corrected simply stops applying.
"""
import json, os

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
LEDGER = os.path.join(HERE, "restated_bases.json")
PUBLISHED = os.path.join(ROOT, "docs", "fund_rbase.json")
FIELDS = ("rev", "pat")
BASES = ("s", "c")
LEVELS = ("as_filed", "unknown", "our_error")


def tol(cur):
    return max(0.015, 0.002 * abs(cur))


def matches(cur, anchor):
    return cur is not None and anchor is not None and abs(cur - anchor) <= tol(anchor)


def _ok_date(q):
    return isinstance(q, int) and 19900101 <= q <= 21001231 and (q // 100) % 100 in (3, 6, 9, 12) \
        and q % 100 == (31 if (q // 100) % 100 in (3, 12) else 30)


def prev_quarter(q):
    y, m = q // 10000, (q // 100) % 100
    return {3: (y - 1) * 10000 + 1231, 6: y * 10000 + 331, 9: y * 10000 + 630, 12: y * 10000 + 930}[m]


def load_ledger(path=LEDGER):
    """The validated ledger. Raises ValueError naming the first malformed entry (a bad hand edit must fail the build,
    never publish half a ledger)."""
    if not os.path.exists(path):
        return {}
    raw = json.load(open(path, encoding="utf-8"))
    out = {}
    for sym, keys in raw.items():
        if sym.startswith("_"):
            continue
        if not isinstance(keys, dict):
            raise ValueError("%s: expected an object of 'F|B' keys" % sym)
        for k, by_basis in keys.items():
            try:
                f, b = (int(x) for x in k.split("|"))
            except ValueError:
                raise ValueError("%s %s: key must be 'YYYYMMDD|YYYYMMDD'" % (sym, k))
            if not (_ok_date(f) and _ok_date(b)) or b not in (f - 10000, prev_quarter(f)):
                raise ValueError("%s %s: the base must be the same quarter a year earlier or the quarter before" % (sym, k))
            for basis, by_field in (by_basis or {}).items():
                if basis not in BASES:
                    raise ValueError("%s %s: basis %r (s|c)" % (sym, k, basis))
                for field, e in (by_field or {}).items():
                    if field not in FIELDS:
                        raise ValueError("%s %s %s: field %r (rev|pat)" % (sym, k, basis, field))
                    for need in ("base", "cur"):
                        if not isinstance(e.get(need), (int, float)) or isinstance(e.get(need), bool):
                            raise ValueError("%s %s %s %s: %s must be a number" % (sym, k, basis, field, need))
                    if e.get("level") not in LEVELS or not e.get("src") or not e.get("read"):
                        raise ValueError("%s %s %s %s: needs level (%s), src and read" % (sym, k, basis, field, "|".join(LEVELS)))
                    out.setdefault(sym, {}).setdefault(k, {}).setdefault(basis, {})[field] = e
    return out


def compact(ledger):
    """{SYM: {"F|B": {basis: {field: [base, cur]}}}} — what the pages and the engines carry."""
    return {s: {k: {b: {f: [e["base"], e["cur"]] for f, e in bf.items()} for b, bf in kb.items()}
                for k, kb in keys.items()} for s, keys in ledger.items()}


def base_for(rb, sym, f, b, basis, field, cur, default):
    """The base to set against `cur` (filing quarter f, comparative quarter b): the restated comparative when the
    ledger has one AND `cur` is the current figure it was printed beside AND a stored base exists; otherwise `default`
    (the stored level, None when there is none — a restated figure never invents a base row)."""
    if default is None:
        return None
    e = (((rb.get(sym) or {}).get("%d|%d" % (f, b)) or {}).get(basis) or {}).get(field)
    if e and matches(cur, e[1]):
        return e[0]
    return default
