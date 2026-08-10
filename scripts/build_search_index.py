# -*- coding: utf-8 -*-
"""Build docs/search_index.json — the tiny symbol/name index behind the site-wide
🔍 search box (theme.js injects it into every page's header).

Universe = EXACTLY what docs/stock.html can render, i.e. every symbol in the
survivorship-free payload's meta (NSE + BSE-only + delisted, ~5k names). Anything
outside it would 404 on the stock page, so it must never be offered.

Why stream instead of json.loads: sf_stock_data.bin decompresses to ~530 MB of
price arrays. We only want the small "meta" object and the "end" date, so the gzip
stream is walked one top-level member at a time — seconds and a few MB of RAM, in CI
or locally. The walk is ORDER-INDEPENDENT on purpose: the committed bin puts `end`
first, the release asset CI builds from puts it LAST, and a scanner that assumed one
of those shipped an unstamped index for weeks (see _scan_top_level).

mcap (for ranking — big names first) comes from docs/dash_slim.bin, whose meta is
keyed SYMBOL.NS and carries mcap for the live NSE universe.

Output (compact, ~250 KB raw / ~80 KB gzipped by Pages):
  {"v": "<sf end date>", "ind": [...industry names...],
   "s": [[symbol, name, alive(0|1), mcap_cr(int), ind_idx], ...]}   # mcap desc

Run: python scripts/build_search_index.py
     python scripts/build_search_index.py --selftest   # the scanner, on every payload shape
     SF_BIN=<path> python scripts/build_search_index.py  # cut from a bin other than the checkout's
"""
import gzip, json, os, re, sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
# In CI the in-repo bin is the one update_sf_data.py just refreshed from the release
# asset. Locally that copy can be weeks old, so SF_BIN=<path> lets you point this at a
# freshly downloaded asset without disturbing the working file other jobs may be using.
SF   = os.environ.get("SF_BIN") or os.path.join(ROOT, "docs", "sf_stock_data.bin")
SLIM = os.path.join(ROOT, "docs", "dash_slim.bin")
OUT  = os.path.join(ROOT, "docs", "search_index.json")

CHUNK = 1 << 22   # 4 MB of decompressed text per read

# One token = an escape pair (so a \" inside a string never reads as a quote) or a
# character that opens/closes nesting. Commas are deliberately NOT tokens: the price
# arrays hold tens of millions of them and none of them can end a top-level value.
_TOK = re.compile(r'\\.|["{}\[\]]', re.S)
_SCALAR_END = re.compile(r'[,}\]]')      # what ends a bare number / true / false / null


class _Scan:
    """Cursor over a gzipped JSON text: buf[pos:] is what is left to read.

    Everything before `pos` is consumed and is dropped on the next read, so a 500 MB
    value can be walked past in a few MB of RAM.
    """

    def __init__(self, path):
        self.fh = gzip.open(path, "rt", encoding="utf-8")
        self.buf, self.pos, self.eof = "", 0, False
        self.dec = json.JSONDecoder()

    def close(self):
        self.fh.close()

    def more(self):
        """Pull another chunk, dropping consumed text. Returns chars dropped, None at EOF."""
        if self.eof:
            return None
        block = self.fh.read(CHUNK)
        if not block:
            self.eof = True
            return None
        drop = self.pos
        if drop:
            self.buf = self.buf[drop:]
            self.pos = 0
        self.buf += block
        return drop

    def peek(self):
        """Next non-whitespace char, leaving pos on it. '' at end of input."""
        while True:
            buf, p = self.buf, self.pos
            n = len(buf)
            while p < n and buf[p] in " \t\r\n":
                p += 1
            self.pos = p
            if p < n:
                return buf[p]
            if self.more() is None:
                return ""

    def take(self):
        """Parse the JSON value at pos, reading more text until it is complete."""
        self.peek()
        while True:
            try:
                return self.dec.raw_decode(self.buf, self.pos)
            except ValueError:
                if self.more() is None:
                    raise

    def string_end(self, i):
        """Index just past the closing quote of the string whose opening quote is at i."""
        j = i + 1
        while True:
            k = self.buf.find('"', j)
            if k >= 0:
                b, n = k - 1, 0
                while b >= i and self.buf[b] == "\\":
                    n += 1
                    b -= 1
                if n % 2 == 0:            # an odd run of backslashes escapes the quote
                    return k + 1
                j = k + 1
                continue
            j = len(self.buf)
            drop = self.more()
            if drop is None:
                raise ValueError("truncated JSON: unterminated string")
            i -= drop
            j -= drop

    def skip(self):
        """Walk past one JSON value without materialising it."""
        c = self.peek()
        if c == '"':
            self.pos = self.string_end(self.pos)
            return
        if c not in "{[":
            while True:
                m = _SCALAR_END.search(self.buf, self.pos)
                if m:
                    self.pos = m.start()
                    return
                self.pos = len(self.buf)
                if self.more() is None:
                    self.pos = len(self.buf)
                    return
        depth, in_str = 0, False
        while True:
            for m in _TOK.finditer(self.buf, self.pos):
                t = m.group()
                self.pos = m.end()
                if t[0] == "\\":
                    continue              # escaped char inside a string
                if t == '"':
                    in_str = not in_str
                elif not in_str:
                    depth += 1 if t in "{[" else -1
                    if depth == 0:
                        return
            end = self.pos
            tail = len(self.buf)
            if tail > end and self.buf[tail - 1] == "\\":
                tail -= 1                 # lone trailing backslash: its pair spans the join
            self.pos = tail
            if self.more() is None:
                raise ValueError("truncated JSON: value never closed")


def _scan_top_level(path, wanted):
    """Return {key: parsed_value} for the given TOP-LEVEL keys of a gzipped JSON object.

    Walks the top level member by member — key, value, key, value — so the answer does
    not depend on key ORDER, a nested key of the same name can never be mistaken for a
    top-level one, and only the wanted values are ever materialised.

    It replaced a needle search (`buf.find('"end":')`) that shipped `"v": ""` for weeks:
    on the live payload `end` is the LAST key, and that loop broke out at EOF with the
    tail still sitting unscanned in its buffer. It also grabbed the first `"end":` it saw
    anywhere, nested or not. Measured 2026-08-10 — the committed bin happens to put `end`
    first, which is why it only ever failed in CI. Runbook 39.
    """
    want, found = set(wanted), {}
    sc = _Scan(path)
    try:
        if sc.peek() != "{":
            raise ValueError("%s is not a JSON object" % path)
        sc.pos += 1
        while want:
            c = sc.peek()
            if c == ",":
                sc.pos += 1
                continue
            if c in ("}", ""):
                break                     # end of the object, or of the file
            if c != '"':
                raise ValueError("expected a key in %s, found %r" % (path, c))
            key, sc.pos = sc.take()
            if sc.peek() != ":":
                raise ValueError("expected ':' after key %r in %s" % (key, path))
            sc.pos += 1
            if key in want:
                found[key], sc.pos = sc.take()
                want.discard(key)
            else:
                sc.skip()
    finally:
        sc.close()
    return found


# Synthetic payloads for --selftest. Each is a whole top-level object; the scanner must return
# meta and end exactly as json.loads would, whatever the key ORDER and whatever the price data
# happens to contain. Cases 2-4 are the ones that shipped broken: `end` last (the live payload),
# and a nested key of the same name (the old needle search returned the 1999 one).
_SELFTEST = [
    ("committed-bin order (end before meta)",
     '{"start":"2002-01-02","end":"2026-06-13","meta":{"A":{"name":"Aco","alive":1}},'
     '"data":{"A":{"d":[1,2],"c":[3,4]}}}'),
    ("release-asset order (data, meta, start, dailyFrom, end LAST)",
     '{"data":{"A":{"d":[1,2],"c":[3,4]}},"meta":{"A":{"name":"Aco","alive":1}},'
     '"start":"2002-01-02","dailyFrom":"2002-01-02","end":"2026-08-07"}'),
    ("nested keys named end/meta inside data",
     '{"data":{"A":{"end":"1999-01-01","meta":{"x":1},"d":[1]}},"meta":{"A":{"name":"Aco"}},'
     '"end":"2026-08-07"}'),
    ("strings carrying braces, commas, quotes and escapes",
     '{"data":{"A}{,\\"x":{"d":[1]}},"meta":{"A":{"name":"A \\"B\\" }, {"}},"end":"2026-08-07"}'),
    ("string ending in an escaped backslash",
     '{"data":{"A":{"n":"back\\\\"}},"meta":{"A":{"name":"x"}},"end":"2026-08-07"}'),
    ("number / bool / null members around end",
     '{"data":{"A":{"d":[1]}},"n":4468,"ok":true,"gap":null,"meta":{"A":{"name":"Aco"}},'
     '"end":"2026-08-07"}'),
    ("pretty-printed whitespace",
     '{\n  "data": {\n    "A": {"d": [1, 2]}\n  },\n  "meta": {"A": {"name": "Aco"}},\n'
     '  "end": "2026-08-07"\n}'),
    ("end absent entirely",
     '{"data":{"A":{"d":[1]}},"meta":{"A":{"name":"Aco"}}}'),
]


def selftest():
    """Prove _scan_top_level on the shapes this payload actually takes, at every chunk boundary.

    Chunk size is swept from 1 byte up so a value, a string or an escape pair split across two
    reads is exercised — the 4 MB production chunk hides all of those. Run:
        python3 scripts/build_search_index.py --selftest
    """
    global CHUNK
    import tempfile
    real, bad = CHUNK, 0
    tmp = os.path.join(tempfile.gettempdir(), "_search_index_selftest.bin")
    try:
        for title, text in _SELFTEST:
            truth = json.loads(text)
            expect = {k: truth[k] for k in ("meta", "end") if k in truth}
            with gzip.open(tmp, "wt", encoding="utf-8") as fh:
                fh.write(text)
            sizes = []
            for CHUNK in list(range(1, 41)) + [len(text) - 1, len(text), real]:
                try:
                    got = _scan_top_level(tmp, ("meta", "end"))
                except Exception as e:
                    got = "%s: %s" % (type(e).__name__, e)
                if got != expect:
                    sizes.append((CHUNK, got))
            CHUNK = real
            bad += len(sizes)
            print("  %-52s %s" % (title[:52], "ok" if not sizes else "FAILED at CHUNK=%r" % (sizes[:2],)))
    finally:
        CHUNK = real
        if os.path.exists(tmp):
            os.remove(tmp)
    print("selftest: %s" % ("all shapes x all chunk boundaries OK" if not bad else "%d FAILURES" % bad))
    return 1 if bad else 0


def main():
    if "--selftest" in sys.argv:
        return selftest()
    if not os.path.exists(SF):
        sys.exit("ABORT: %s missing — run the SF refresh first" % SF)
    got = _scan_top_level(SF, ("meta", "end"))
    meta = got.get("meta") or {}
    end  = got.get("end") or ""
    if len(meta) < 3000:
        sys.exit("ABORT: sf meta has only %d symbols — refusing to publish a truncated index" % len(meta))
    # `v` is the ONLY staleness stamp this file carries — feeds.json has no entry for it at all
    # (checked 2026-08-10), and check_fund_alias.py reads its live META from here, where an old
    # cut silently hides every recent rename. An unstamped index is therefore worse than no
    # rebuild at all: CI's `|| echo` keeps the committed copy when this exits non-zero, which is
    # the safe outcome. (It shipped `"v": ""` for weeks; see _scan_top_level.)
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", end):
        sys.exit("ABORT: no usable top-level `end` in %s (got %r) — refusing to publish an "
                 "index with no staleness stamp" % (os.path.basename(SF), end))

    mcap = {}
    if os.path.exists(SLIM):
        try:
            sm = json.loads(gzip.decompress(open(SLIM, "rb").read())).get("meta", {})
            for tkr, m in sm.items():
                s = m.get("symbol") or tkr.split(".")[0]
                if m.get("mcap"):
                    mcap[s] = m["mcap"]
        except Exception as e:
            print("WARN: could not read mcap from dash_slim.bin: %s" % e)

    inds, ind_ix, rows = [], {}, []
    for sym, m in meta.items():
        name = (m.get("name") or sym).strip()
        ind  = (m.get("ind") or m.get("industry") or "").strip()
        if ind not in ind_ix:
            ind_ix[ind] = len(inds); inds.append(ind)
        rows.append([sym, name, 1 if m.get("alive") else 0,
                     int(round(mcap.get(sym, 0))), ind_ix[ind]])
    # Rank once, here: live before delisted, named before nameless (the payload has
    # no company name for long-dead or obscure listings — those are noise in a
    # suggestion list), then biggest first. The client keeps this order within each
    # match tier, so no sorting happens per keystroke.
    rows.sort(key=lambda r: (-r[2], 0 if r[1] != r[0] else 1, -r[3], r[0]))

    payload = {"v": end, "ind": inds, "s": rows}
    blob = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    open(OUT, "w", encoding="utf-8").write(blob)
    print("search_index.json: %d symbols (%d live, %d with mcap), %.0f KB raw, end=%s"
          % (len(rows), sum(r[2] for r in rows), len(mcap), len(blob.encode()) / 1024, end))


if __name__ == "__main__":
    sys.exit(main() or 0)
