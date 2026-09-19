'use strict';
/* ============================================================================
 * scripts/_read_big_gz.js — the ONE big-gzip JSON reader, shared by every JS
 * that parses the LIVE sf bin.
 *
 * WHY THIS EXISTS
 *   Read a gzipped JSON payload whose UNCOMPRESSED form may exceed V8's maximum
 *   string length (0x1fffffe8 ≈ 537 M chars). The live sf bin
 *   (releases/download/data/sf_stock_data.bin) crossed that line ~2026-09-16
 *   (uncompressed ~540 MB) and `JSON.parse(gunzipSync(buf))` began dying with
 *   ERR_STRING_TOO_LONG at Buffer.toString — the whole gunzipped Buffer can never
 *   become one JS string, which broke the nightly coverage bake three nights
 *   running. (Node Buffers hold ~2 GB, far past the string cap, so gunzip itself
 *   is fine — only the string is capped.) Python readers are unaffected: only V8
 *   caps strings. See memory project-stocks-live-bin-exceeds-v8-string-cap.
 *
 *   The split-parse first landed inline in build_coverage_matrix.js (ad4f7c2b8,
 *   verified 27/27 unit tests + a full bake against the real 537 MB bin). It is
 *   extracted here verbatim so the four callers that read the live bin
 *   (build_coverage_matrix.js, build_first_bar_map.js, grid_search.js,
 *   grid_search_full.js) share ONE copy instead of four that can drift.
 *
 * FAST PATH: for any file under the cap this is exactly the old one-shot parse.
 * FALLBACK: when the slice is too long to stringify, split it one container level
 * at a time and recurse — so a huge top-level object is parsed member-by-member,
 * and the huge `data` map (once it too crosses the cap) is parsed one ~7 KB symbol
 * record at a time. No single string ever approaches the cap. The scan is purely
 * structural (string/escape + {}/[] depth aware) and makes NO assumption about key
 * order, whitespace, or which member is the big one — it only ever splits as deep
 * as the cap forces it to.
 *
 *   const { readGz } = require('./_read_big_gz.js');
 *   const obj = readGz('/path/to/file.bin');   // gzipped-JSON path -> parsed value
 * ========================================================================== */
const fs = require('fs');
const zlib = require('zlib');

function readGz(p) {
  const buf = zlib.gunzipSync(fs.readFileSync(p));
  return parseJsonBuf(buf, 0, buf.length);
}
/* -- structural scanners over a UTF-8 JSON Buffer. Every JSON delimiter (" : , { } [ ])
 *    is ASCII, and multibyte UTF-8 runs occur only INSIDE strings, so byte-level scanning
 *    of structure and slicing on value boundaries are both safe. -- */
const _isWs = c => c === 0x20 || c === 0x09 || c === 0x0a || c === 0x0d;
function _skipWs(b, i, end) { while (i < end && _isWs(b[i])) i++; return i; }
function _strEnd(b, i) {                 // b[i] === 0x22 ("); returns index past closing quote
  for (let j = i + 1; j < b.length; j++) {
    const c = b[j];
    if (c === 0x5c) { j++; continue; }   // backslash escape: skip the next byte
    if (c === 0x22) return j + 1;
  }
  throw new Error('readGz: unterminated string');
}
function _valEnd(b, i) {                  // end (exclusive) of the JSON value starting at b[i]
  const c = b[i];
  if (c === 0x22) return _strEnd(b, i);
  if (c === 0x7b || c === 0x5b) {         // { or [ : balanced scan, honouring strings
    let depth = 0;
    for (let j = i; j < b.length; j++) {
      const d = b[j];
      if (d === 0x22) { j = _strEnd(b, j) - 1; continue; }
      if (d === 0x7b || d === 0x5b) depth++;
      else if (d === 0x7d || d === 0x5d) { if (--depth === 0) return j + 1; }
    }
    throw new Error('readGz: unterminated ' + String.fromCharCode(c));
  }
  let j = i;                             // scalar: number / true / false / null
  while (j < b.length && !(b[j] === 0x2c || b[j] === 0x7d || b[j] === 0x5d || _isWs(b[j]))) j++;
  return j;
}
/* Parse value b[start,end): try native parse first; only when the slice is too long to
 * stringify do we split it (it must then be an object or array) and recurse. */
function parseJsonBuf(b, start, end) {
  const i = _skipWs(b, start, end);
  try { return JSON.parse(b.toString('utf8', i, end)); }
  catch (e) { if (e.code !== 'ERR_STRING_TOO_LONG') throw e; }
  const open = b[i];
  if (open === 0x7b) return _splitObject(b, i);
  if (open === 0x5b) return _splitArray(b, i);
  throw new Error('readGz: value too long to stringify and not a container');
}
function _splitObject(b, i) {
  const out = {};
  i = _skipWs(b, i + 1, b.length);       // past {
  if (b[i] === 0x7d) return out;         // {}
  for (;;) {
    i = _skipWs(b, i, b.length);
    const ke = _strEnd(b, i);            // key string
    const key = JSON.parse(b.toString('utf8', i, ke));
    i = _skipWs(b, _skipWs(b, ke, b.length) + 1, b.length);   // past : then ws
    const ve = _valEnd(b, i);
    out[key] = parseJsonBuf(b, i, ve);
    i = _skipWs(b, ve, b.length);
    const sep = b[i++];
    if (sep === 0x2c) continue;          // ,
    if (sep === 0x7d) return out;        // }
    throw new Error('readGz: expected , or } in object');
  }
}
function _splitArray(b, i) {
  const out = [];
  i = _skipWs(b, i + 1, b.length);       // past [
  if (b[i] === 0x5d) return out;         // []
  for (;;) {
    i = _skipWs(b, i, b.length);
    const ve = _valEnd(b, i);
    out.push(parseJsonBuf(b, i, ve));
    i = _skipWs(b, ve, b.length);
    const sep = b[i++];
    if (sep === 0x2c) continue;          // ,
    if (sep === 0x5d) return out;        // ]
    throw new Error('readGz: expected , or ] in array');
  }
}

module.exports = { readGz, parseJsonBuf, _isWs, _skipWs, _strEnd, _valEnd, _splitObject, _splitArray };
