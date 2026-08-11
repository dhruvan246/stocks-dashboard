# Aggregator route — per-cell outcome (Moneycontrol / Trendlyne / Tickertape)

Generated 2026-08-11 15:27 IST. Sites tried per cell: mc, tl, tt. Terminal states are runbook §61b; a cell nobody reached is `not-found-via:<sites>`, never "unfillable" (§0/§57a).


## FILLED (13)

| cell | value | precision | site | row | local/total anchors | worst anchor | site FY identity (prev/target/next) | our FY identity |
|---|---|---|---|---|---|---|---|---|
| ABSLAMC 20220630 revS | 299.01 | site-exact | mc | Net Sales/Income from operations | 9/19 | 0.00 | OK/OK/OK | CONFIRMED |
| BSE 20220630 revS | 141.68 | site-exact | mc | Net Sales/Income from operations | 12/33 | 0.00 | OK/OK/OK | CONFIRMED |
| IDEA 20220630 revC | 10410.10 | rounded(13.69) | mc | Total Income From Operations | 11/43 | 13.69 | OK/OK/OK | NO-TEST |
| IDEA 20220930 revC | 10614.60 | rounded(13.69) | mc | Total Income From Operations | 11/43 | 13.69 | OK/OK/OK | NO-TEST |
| JINDALSTEL 20220630 revC | 13045.41 | rounded(13.20) | mc | Total Income From Operations | 12/37 | 13.20 | OK/OK/OK | CONFIRMED |
| MMTC 20220630 revC | 1511.34 | rounded(1.46) | mc | Total Income From Operations | 12/27 | 1.46 | OK/OK/OK | CONFIRMED |
| NAM-INDIA 20220930 revS | 308.98 | site-exact | mc | Net Sales/Income from operations | 12/37 | 0.00 | OK/OK/OK | CONFIRMED |
| RAIN 20220630 revC | 5540.55 | site-exact | mc | Total Income From Operations | 12/40 | 0.00 | OK/OK/OK | CONFIRMED |
| RAJESHEXPO 20200630 revC | 46054.27 | rounded(0.95) | mc | Net Sales/Income from operations | 8/37 | 0.95 | OK/OK/OK | NO-TEST |
| RAJESHEXPO 20210331 revC | 64522.60 | rounded(0.95) | mc | Net Sales/Income from operations | 9/37 | 0.95 | OK/OK/OK | NO-TEST |
| RAJESHEXPO 20210630 revC | 50897.02 | rounded(0.95) | mc | Net Sales/Income from operations | 9/37 | 0.95 | OK/OK/OK | NO-TEST |
| RAJESHEXPO 20210930 revC | 41245.13 | rounded(0.95) | mc | Net Sales/Income from operations | 9/37 | 0.95 | OK/OK/OK | NO-TEST |
| SUNDARMFIN 20220630 revS | 935.07 | rounded(0.02) | mc | Net Sales/Income from operations | 12/75 | 0.02 | OK/OK/OK | CONFIRMED |

## GATED OUT after passing the quarterly gate — restated financial year (9)

These are NOT absences. The quarterly series matched ours on 27-40 anchors; the site's own four quarters then failed to sum to its own annual for the target FY or a neighbour, which is the §60d restatement signature. State: `NEEDS-CROSSCHECK` — reachable from a filing read, not from this route.

| cell | which FY is restated | site ΣQ | site annual | diff |
|---|---|---|---|---|
| ADANIENT 20220630 revC | next FY20240331 | 105472.18 | 96420.98 | +9051.20 |
| EXIDEIND 20210331 revC | next FY20220331 | 15135.09 | 12789.22 | +2345.87 |
| ICIL 20210930 revC | target FY20220331 | 2862.87 | 2842.02 | +20.85 |
| ICIL 20211231 revC | target FY20220331 | 2862.87 | 2842.02 | +20.85 |
| PEL 20211231 revC | next FY20230331 | 9354.70 | 8934.30 | +420.40 |
| PEL 20220630 revC | target FY20230331 | 9354.70 | 8934.30 | +420.40 |
| SAMMAANCAP 20191231 revS | next FY20210331 | 8730.06 | 8654.64 | +75.42 |
| SAMMAANCAP 20191231 revS | prev FY20190331 | 14741.77 | 15407.35 | -665.58 |
| SAMMAANCAP 20191231 revS | target FY20200331 | 11794.61 | 11399.23 | +395.38 |
| WELCORP 20211231 revC | target FY20220331 | 5915.04 | 6505.11 | -590.07 |
| WELCORP 20220630 revC | prev FY20220331 | 5915.04 | 6505.11 | -590.07 |

## NEEDS-CROSSCHECK (a site HAD the quarter; its series does not reproduce ours — GATE-A) — 69 cells

| cell | what each site said |
|---|---|
| 360ONE 20220930 revS | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20230930 ours=63.44 site=67.59 (4q away); rev_total: GATE-A: disagreement inside +-6q: 20230930 ours=63.44 <br>**tl** quarter 20220930 absent (site holds 20230630..20260630)<br>**tt** tt: no page whose ticker equals 360ONE |
| ABREL 20220630 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20231231 ours=982.04 site=1179.12 (6q away); 20230930 ours=2567.38 site=1087.28 (5q away); 20230630 ours=88<br>**tl** quarter 20220630 absent (site holds 20230331..20260331)<br>**tt** tt: no page whose ticker equals ABREL |
| ADANIENSOL 20220630 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20220331 ours=2587.95 site=2974.73 (1q away); 20210930 ours=2479.22 site=2541.44 (3q away); rev_total: GATE<br>**tl** quarter 20220630 absent (site holds 20230630..20260630)<br>**tt** tt: no page whose ticker equals ADANIENSOL |
| ANGELONE 20220630 revS | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20210930 ours=490.45 site=522.65 (3q away); 20210630 ours=428.04 site=458.72 (4q away); 20210331 ours=376.0<br>**tl** quarter 20220630 absent (site holds 20230630..20260630)<br>**tt** tt(page): 0 quarters -..-; company reports consolidated only |
| ATUL 20221231 revS | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20210930 ours=1106.17 site=1211.65 (5q away); rev_total: GATE-A: disagreement inside +-6q: 20210930 ours=11<br>**tl** quarter 20221231 absent (site holds 20230630..20260630)<br>**tt** tt(page): 0 quarters -..-; company reports consolidated only |
| CCAVENUE 20211231 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20220930 ours=76.66 site=476.66 (3q away); 20210331 ours=319.17 site=201.29 (3q away); 20200930 ours=148.7 <br>**tl** quarter 20211231 absent (site holds 20230331..20260331)<br>**tt** tt: no page whose ticker equals CCAVENUE |
| CCAVENUE 20220630 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20230930 ours=787.0 site=789.93 (5q away); 20220930 ours=76.66 site=476.66 (1q away); 20210331 ours=319.17 <br>**tl** quarter 20220630 absent (site holds 20230331..20260331)<br>**tt** tt: no page whose ticker equals CCAVENUE |
| EDELWEISS 20220630 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20230331 ours=3049.23 site=3012.39 (3q away); 20221231 ours=2155.18 site=2111.58 (2q away); 20220930 ours=2<br>**tl** quarter 20220630 absent (site holds 20230630..20260630)<br>**tt** quarter 20220630 absent (site holds 20240331..20260630) |
| GICRE 20191231 revC | **mc** rev_ops: GATE-A: only 0 anchor(s) inside +-6q, need 2 (0 matched overall); rev_total: GATE-A: only 0 anchor(s) inside +-6q, need 2 (9 matched overall)<br>**tl** quarter 20191231 absent (site holds 20230331..20260331)<br>**tt** quarter 20191231 absent (site holds 20231231..20260331) |
| GICRE 20200331 revC | **mc** rev_ops: GATE-A: only 0 anchor(s) inside +-6q, need 2 (0 matched overall); rev_total: GATE-A: only 0 anchor(s) inside +-6q, need 2 (9 matched overall)<br>**tl** quarter 20200331 absent (site holds 20230331..20260331)<br>**tt** quarter 20200331 absent (site holds 20231231..20260331) |
| GICRE 20200630 revC | **mc** rev_ops: GATE-A: only 0 anchor(s) inside +-6q, need 2 (0 matched overall); rev_total: GATE-A: only 0 anchor(s) inside +-6q, need 2 (9 matched overall)<br>**tl** quarter 20200630 absent (site holds 20230331..20260331)<br>**tt** quarter 20200630 absent (site holds 20231231..20260331) |
| GICRE 20200930 revC | **mc** rev_ops: GATE-A: only 0 anchor(s) inside +-6q, need 2 (0 matched overall); rev_total: GATE-A: only 0 anchor(s) inside +-6q, need 2 (9 matched overall)<br>**tl** quarter 20200930 absent (site holds 20230331..20260331)<br>**tt** quarter 20200930 absent (site holds 20231231..20260331) |
| GICRE 20201231 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20220630 ours=12645.84 site=10734.29 (6q away); rev_total: GATE-A: only 1 anchor(s) inside +-6q, need 2 (9 <br>**tl** quarter 20201231 absent (site holds 20230331..20260331)<br>**tt** quarter 20201231 absent (site holds 20231231..20260331) |
| GICRE 20210331 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20220630 ours=12645.84 site=10734.29 (5q away); rev_total: GATE-A: only 1 anchor(s) inside +-6q, need 2 (9 <br>**tl** quarter 20210331 absent (site holds 20230331..20260331)<br>**tt** quarter 20210331 absent (site holds 20231231..20260331) |
| GICRE 20210630 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20220630 ours=12645.84 site=10734.29 (4q away); rev_total: GATE-A: only 1 anchor(s) inside +-6q, need 2 (9 <br>**tl** quarter 20210630 absent (site holds 20230331..20260331)<br>**tt** quarter 20210630 absent (site holds 20231231..20260331) |
| GICRE 20210930 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20220630 ours=12645.84 site=10734.29 (3q away); rev_total: GATE-A: only 1 anchor(s) inside +-6q, need 2 (9 <br>**tl** quarter 20210930 absent (site holds 20230331..20260331)<br>**tt** quarter 20210930 absent (site holds 20231231..20260331) |
| GICRE 20220331 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20230930 ours=13224.18 site=9954.15 (6q away); 20230630 ours=11165.84 site=8696.42 (5q away); 20220630 ours<br>**tl** quarter 20220331 absent (site holds 20230331..20260331)<br>**tt** quarter 20220331 absent (site holds 20231231..20260331) |
| GICRE 20220930 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20230930 ours=13224.18 site=9954.15 (4q away); 20230630 ours=11165.84 site=8696.42 (3q away); 20220630 ours<br>**tl** quarter 20220930 absent (site holds 20230331..20260331)<br>**tt** quarter 20220930 absent (site holds 20231231..20260331) |
| GICRE 20221231 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20240630 ours=12886.47 site=10097.61 (6q away); 20230930 ours=13224.18 site=9954.15 (3q away); 20230630 our<br>**tl** quarter 20221231 absent (site holds 20230331..20260331)<br>**tt** quarter 20221231 absent (site holds 20231231..20260331) |
| GICRE 20230331 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20240630 ours=12886.47 site=10097.61 (5q away); 20230930 ours=13224.18 site=9954.15 (2q away); 20230630 our<br>**tl** rev_ops: GATE-A: disagreement inside +-6q: 20230930 ours=13224.18 site=13075.11 (2q away); rev_total: GATE-A: disagreement inside +-6q: 20230930 ours=<br>**tt** quarter 20230331 absent (site holds 20231231..20260331) |
| GICRE 20231231 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20250630 ours=14623.26 site=11273.88 (6q away); 20250331 ours=13208.55 site=9250.02 (5q away); 20241231 our<br>**tl** rev_ops: GATE-A: disagreement inside +-6q: 20230930 ours=13224.18 site=13075.11 (1q away); rev_total: GATE-A: disagreement inside +-6q: 20250630 ours=<br>**tt** rev_total: GATE-A: disagreement inside +-6q: 20241231 ours=11143.8 site=11398.69 (4q away); 20250331 ours=13208.55 site=13354.25 (5q away); 20250630 o |
| GICRE 20240331 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20250930 ours=12755.45 site=8925.33 (6q away); 20250630 ours=14623.26 site=11273.88 (5q away); 20250331 our<br>**tl** rev_ops: GATE-A: disagreement inside +-6q: 20230930 ours=13224.18 site=13075.11 (2q away); rev_total: GATE-A: disagreement inside +-6q: 20250930 ours=<br>**tt** rev_total: GATE-A: disagreement inside +-6q: 20241231 ours=11143.8 site=11398.69 (3q away); 20250331 ours=13208.55 site=13354.25 (4q away); 20250630 o |
| GICRE 20240930 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20260331 ours=13018.27 site=9913.56 (6q away); 20251231 ours=12588.62 site=9630.54 (5q away); 20250930 ours<br>**tl** rev_ops: GATE-A: disagreement inside +-6q: 20230930 ours=13224.18 site=13075.11 (4q away); rev_total: GATE-A: disagreement inside +-6q: 20260331 ours=<br>**tt** rev_total: GATE-A: disagreement inside +-6q: 20241231 ours=11143.8 site=11398.69 (1q away); 20250331 ours=13208.55 site=13354.25 (2q away); 20250630 o |
| GICRE 20241231 revS | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20260331 ours=12844.52 site=9785.06 (5q away); 20251231 ours=12504.74 site=9580.28 (4q away); 20250930 ours<br>**tl** rev_ops: GATE-A: disagreement inside +-6q: 20240930 ours=12130.38 site=12330.38 (1q away); 20230930 ours=13224.18 site=13059.08 (5q away); rev_total: <br>**tt** tt(page): 0 quarters -..-; company reports consolidated only |
| GMRAIRPORT 20220630 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20230630 ours=2017.63 site=2034.76 (4q away); 20220331 ours=1283.6 site=1087.89 (1q away); 20211231 ours=13<br>**tl** quarter 20220630 absent (site holds 20230331..20260331)<br>**tt** quarter 20220630 absent (site holds 20231231..20260331) |
| GSPL 20220630 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20231231 ours=4544.24 site=4389.08 (6q away); 20230930 ours=4410.97 site=4265.22 (5q away); 20230630 ours=4<br>**tl** tl: symbol absent from trendlyne fundamental sitemap<br>**tt** quarter 20220630 absent (site holds 20230930..20251231) |
| GSPL 20221231 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20240630 ours=4891.54 site=4727.01 (6q away); 20240331 ours=4691.88 site=4532.2 (5q away); 20231231 ours=45<br>**tl** tl: symbol absent from trendlyne fundamental sitemap<br>**tt** quarter 20221231 absent (site holds 20230930..20251231) |
| HDFCLIFE 20200331 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20210930 ours=20525.44 site=11445.53 (6q away); 20210630 ours=14764.98 site=7540.05 (5q away); 20210331 our<br>**tl** quarter 20200331 absent (site holds 20230630..20260630)<br>**tt** quarter 20200331 absent (site holds 20240331..20260630) |
| HDFCLIFE 20200630 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20211231 ours=14289.26 site=12147.32 (6q away); 20210930 ours=20525.44 site=11445.53 (5q away); 20210630 ou<br>**tl** quarter 20200630 absent (site holds 20230630..20260630)<br>**tt** quarter 20200630 absent (site holds 20240331..20260630) |
| HDFCLIFE 20200930 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20220331 ours=17481.61 site=15624.9 (6q away); 20211231 ours=14289.26 site=12147.32 (5q away); 20210930 our<br>**tl** quarter 20200930 absent (site holds 20230630..20260630)<br>**tt** quarter 20200930 absent (site holds 20240331..20260630) |
| HDFCLIFE 20230930 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20250331 ours=24190.65 site=23842.99 (6q away); 20241231 ours=17300.27 site=16831.84 (5q away); 20240930 ou<br>**tl** rev_ops: GATE-A: disagreement inside +-6q: 20240930 ours=28227.0 site=28496.97 (4q away); rev_total: GATE-A: disagreement inside +-6q: 20250331 ours=2<br>**tt** quarter 20230930 absent (site holds 20240331..20260630) |
| HDFCLIFE 20231231 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20250630 ours=29463.18 site=14539.42 (6q away); 20250331 ours=24190.65 site=23842.99 (5q away); 20241231 ou<br>**tl** rev_ops: GATE-A: disagreement inside +-6q: 20240930 ours=28227.0 site=28496.97 (3q away); rev_total: GATE-A: disagreement inside +-6q: 20250630 ours=2<br>**tt** quarter 20231231 absent (site holds 20240331..20260630) |
| IDFC 20191231 revS | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20180630 ours=11.05 site=14.49 (6q away); rev_total: GATE-A: disagreement inside +-6q: 20180630 ours=11.05 <br>**tl** tl: symbol absent from trendlyne fundamental sitemap<br>**tt** tt: no page whose ticker equals IDFC |
| INDOSTAR 20191231 revS | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20190930 ours=12.43 site=407.02 (1q away); 20190630 ours=372.18 site=404.65 (2q away); 20190331 ours=284.56<br>**tl** quarter 20191231 absent (site holds 20230630..20260630)<br>**tt** tt(page): 0 quarters -..-; company reports consolidated only |
| IOC 20220630 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20231231 ours=226892.08 site=199905.65 (6q away); 20230930 ours=205283.03 site=179245.67 (5q away); 2023063<br>**tl** quarter 20220630 absent (site holds 20230630..20260630)<br>**tt** quarter 20220630 absent (site holds 20240331..20260630) |
| JMFINANCIL 20220630 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20231231 ours=1235.99 site=1224.51 (6q away); 20230930 ours=1197.38 site=1178.88 (5q away); 20230630 ours=1<br>**tl** quarter 20220630 absent (site holds 20230630..20260630)<br>**tt** tt: no page whose ticker equals JMFINANCIL |
| JSL 20220930 revS | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20220630 ours=8028.39 site=5336.41 (1q away); rev_total: GATE-A: disagreement inside +-6q: 20220630 ours=80<br>**tl** quarter 20220930 absent (site holds 20230630..20260630)<br>**tt** tt(page): 0 quarters -..-; company reports consolidated only |
| KENNAMET 20200331 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20201231 ours=193.3 site=216.8 (3q away); 20200930 ours=178.3 site=197.1 (2q away); 20200630 ours=87.3 site<br>**tl** rev_ops: GATE-A: disagreement inside +-6q: 20201231 ours=193.3 site=216.8 (3q away); 20200930 ours=178.3 site=197.1 (2q away); 20200630 ours=87.3 site<br>**tt** tt(page): 0 quarters -..-; company reports standalone only |
| LICI 20220930 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20230630 ours=190163.06 site=98755.22 (3q away); 20221231 ours=197714.39 site=112296.7 (1q away); rev_total<br>**tl** quarter 20220930 absent (site holds 20230630..20260630)<br>**tt** quarter 20220930 absent (site holds 20240331..20260630) |
| LICI 20221231 revS | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20240630 ours=211129.41 site=113770.14 (6q away); 20240331 ours=237842.62 site=152293.13 (5q away); 2023123<br>**tl** quarter 20221231 absent (site holds 20230630..20260630)<br>**tt** tt(page): 0 quarters -..-; company reports consolidated only |
| LICI 20230331 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20240930 ours=231132.12 site=120325.66 (6q away); 20230630 ours=190163.06 site=98755.22 (1q away); 20221231<br>**tl** quarter 20230331 absent (site holds 20230630..20260630)<br>**tt** quarter 20230331 absent (site holds 20240331..20260630) |
| LICI 20230930 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20250331 ours=243134.49 site=147917.19 (6q away); 20241231 ours=203751.32 site=107302.3 (5q away); 20240930<br>**tl** rev_ops: GATE-A: disagreement inside +-6q: 20230630 ours=190163.06 site=189523.11 (1q away); rev_total: GATE-A: disagreement inside +-6q: 20250331 our<br>**tt** quarter 20230930 absent (site holds 20240331..20260630) |
| LICI 20231231 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20250630 ours=224671.49 site=119618.41 (6q away); 20250331 ours=243134.49 site=147917.19 (5q away); 2024123<br>**tl** rev_ops: GATE-A: disagreement inside +-6q: 20230630 ours=190163.06 site=189523.11 (2q away); rev_total: GATE-A: disagreement inside +-6q: 20250630 our<br>**tt** quarter 20231231 absent (site holds 20240331..20260630) |
| LICI 20240331 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20250930 ours=241524.29 site=126930.04 (6q away); 20250630 ours=224671.49 site=119618.41 (5q away); 2025033<br>**tl** rev_ops: GATE-A: disagreement inside +-6q: 20230630 ours=190163.06 site=189523.11 (3q away); rev_total: GATE-A: disagreement inside +-6q: 20250930 our<br>**tt** rev_total: GATE-A: disagreement inside +-6q: 20240930 ours=231132.12 site=231926.41 (2q away); 20241231 ours=203751.32 site=204569.34 (3q away); 20250 |
| LICI 20240630 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20251231 ours=235954.23 site=125988.15 (6q away); 20250930 ours=241524.29 site=126930.04 (5q away); 2025063<br>**tl** rev_ops: GATE-A: disagreement inside +-6q: 20230630 ours=190163.06 site=189523.11 (4q away); rev_total: GATE-A: disagreement inside +-6q: 20251231 our<br>**tt** rev_total: GATE-A: disagreement inside +-6q: 20240930 ours=231132.12 site=231926.41 (1q away); 20241231 ours=203751.32 site=204569.34 (2q away); 20250 |
| LTF 20210930 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20220930 ours=2946.59 site=3138.1 (4q away); 20220630 ours=2946.59 site=2988.4 (3q away); rev_total: GATE-A<br>**tl** quarter 20210930 absent (site holds 20230630..20260630)<br>**tt** tt: no page whose ticker equals LTF |
| LTF 20211231 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20220930 ours=2946.59 site=3138.1 (3q away); 20220630 ours=2946.59 site=2988.4 (2q away); rev_total: GATE-A<br>**tl** quarter 20211231 absent (site holds 20230630..20260630)<br>**tt** tt: no page whose ticker equals LTF |
| NIACL 20220331 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20230630 ours=9900.04 site=9332.23 (5q away); 20230331 ours=10182.36 site=7937.38 (4q away); 20221231 ours=<br>**tl** quarter 20220331 absent (site holds 20230630..20260630)<br>**tt** quarter 20220331 absent (site holds 20240331..20260630) |
| NIACL 20240930 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20260331 ours=12543.82 site=10021.77 (6q away); 20251231 ours=12069.24 site=9770.95 (5q away); 20250930 our<br>**tl** rev_ops: GATE-A: disagreement inside +-6q: 20250630 ours=11719.01 site=11080.52 (3q away); 20250331 ours=11664.22 site=11008.74 (2q away); 20230630 ou<br>**tt** rev_total: GATE-A: disagreement inside +-6q: 20240331 ours=11685.87 site=11721.43 (2q away); 20250331 ours=11664.22 site=11741.04 (2q away); 20250630  |
| SHRIRAMCIT 20200630 revS | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20200331 ours=1488.58 site=1449.51 (1q away); 20190630 ours=1437.16 site=1493.47 (4q away); rev_total: GATE<br>**tl** tl: symbol absent from trendlyne fundamental sitemap<br>**tt** tt: no page whose ticker equals SHRIRAMCIT |
| SHRIRAMCIT 20210331 revS | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20200331 ours=1488.58 site=1449.51 (4q away); rev_total: GATE-A: disagreement inside +-6q: 20200331 ours=14<br>**tl** tl: symbol absent from trendlyne fundamental sitemap<br>**tt** tt: no page whose ticker equals SHRIRAMCIT |
| SHRIRAMCIT 20210630 revS | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20200331 ours=1488.58 site=1449.51 (5q away); rev_total: GATE-A: disagreement inside +-6q: 20200331 ours=14<br>**tl** tl: symbol absent from trendlyne fundamental sitemap<br>**tt** tt: no page whose ticker equals SHRIRAMCIT |
| SHRIRAMCIT 20210930 revS | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20200331 ours=1488.58 site=1449.51 (6q away); rev_total: GATE-A: disagreement inside +-6q: 20200331 ours=14<br>**tl** tl: symbol absent from trendlyne fundamental sitemap<br>**tt** tt: no page whose ticker equals SHRIRAMCIT |
| SPICEJET 20201231 revS | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20210331 ours=1877.09 site=1818.66 (1q away); 20200930 ours=1054.99 site=1016.08 (1q away); 20200630 ours=5<br>**tl** quarter 20201231 absent (site holds 20221231..20251231)<br>**tt** tt(page): 0 quarters -..-; company reports consolidated only |
| SPICEJET 20210930 revS | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20210331 ours=1877.09 site=1818.66 (2q away); 20200930 ours=1054.99 site=1016.08 (4q away); 20200630 ours=5<br>**tl** quarter 20210930 absent (site holds 20221231..20251231)<br>**tt** tt(page): 0 quarters -..-; company reports consolidated only |
| SPICEJET 20211231 revS | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20210331 ours=1877.09 site=1818.66 (3q away); 20200930 ours=1054.99 site=1016.08 (5q away); 20200630 ours=5<br>**tl** quarter 20211231 absent (site holds 20221231..20251231)<br>**tt** tt(page): 0 quarters -..-; company reports consolidated only |
| SPICEJET 20220331 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20230630 ours=1917.43 site=2003.59 (5q away); 20211231 ours=1635.79 site=2262.65 (1q away); 20210930 ours=4<br>**tl** quarter 20220331 absent (site holds 20221231..20251231)<br>**tt** quarter 20220331 absent (site holds 20230930..20251231) |
| SPICEJET 20220630 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20230630 ours=1917.43 site=2003.59 (4q away); 20211231 ours=1635.79 site=2262.65 (2q away); 20210930 ours=4<br>**tl** quarter 20220630 absent (site holds 20221231..20251231)<br>**tt** quarter 20220630 absent (site holds 20230930..20251231) |
| WESTLIFE 20191231 revS | **mc** rev_ops: GATE-A: only 0 anchor(s) inside +-6q, need 2 (4 matched overall); rev_total: GATE-A: only 0 anchor(s) inside +-6q, need 2 (4 matched overall)<br>**tl** quarter 20191231 absent (site holds 20230630..20260630)<br>**tt** tt(page): 0 quarters -..-; company reports consolidated only |
| WESTLIFE 20200331 revS | **mc** rev_ops: GATE-A: only 0 anchor(s) inside +-6q, need 2 (4 matched overall); rev_total: GATE-A: only 0 anchor(s) inside +-6q, need 2 (4 matched overall)<br>**tl** quarter 20200331 absent (site holds 20230630..20260630)<br>**tt** tt(page): 0 quarters -..-; company reports consolidated only |
| WESTLIFE 20200630 revS | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20211231 ours=93.89 site=0.1 (6q away); rev_total: GATE-A: disagreement inside +-6q: 20211231 ours=93.89 si<br>**tl** quarter 20200630 absent (site holds 20230630..20260630)<br>**tt** tt(page): 0 quarters -..-; company reports consolidated only |
| WESTLIFE 20200930 revS | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20211231 ours=93.89 site=0.1 (5q away); rev_total: GATE-A: disagreement inside +-6q: 20211231 ours=93.89 si<br>**tl** quarter 20200930 absent (site holds 20230630..20260630)<br>**tt** tt(page): 0 quarters -..-; company reports consolidated only |
| WESTLIFE 20201231 revS | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20211231 ours=93.89 site=0.1 (4q away); rev_total: GATE-A: disagreement inside +-6q: 20211231 ours=93.89 si<br>**tl** quarter 20201231 absent (site holds 20230630..20260630)<br>**tt** tt(page): 0 quarters -..-; company reports consolidated only |
| WESTLIFE 20210331 revS | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20211231 ours=93.89 site=0.1 (3q away); rev_total: GATE-A: disagreement inside +-6q: 20211231 ours=93.89 si<br>**tl** quarter 20210331 absent (site holds 20230630..20260630)<br>**tt** tt(page): 0 quarters -..-; company reports consolidated only |
| WESTLIFE 20210630 revS | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20221231 ours=93.89 site=0.2 (6q away); 20211231 ours=93.89 site=0.1 (2q away); rev_total: GATE-A: disagree<br>**tl** quarter 20210630 absent (site holds 20230630..20260630)<br>**tt** tt(page): 0 quarters -..-; company reports consolidated only |
| WESTLIFE 20210930 revS | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20230331 ours=381.97 site=0.22 (6q away); 20221231 ours=93.89 site=0.2 (5q away); 20211231 ours=93.89 site=<br>**tl** quarter 20210930 absent (site holds 20230630..20260630)<br>**tt** tt(page): 0 quarters -..-; company reports consolidated only |
| WESTLIFE 20220331 revS | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20230930 ours=0.47 site=0.21 (6q away); 20230331 ours=381.97 site=0.22 (4q away); 20221231 ours=93.89 site=<br>**tl** quarter 20220331 absent (site holds 20230630..20260630)<br>**tt** tt(page): 0 quarters -..-; company reports consolidated only |
| WESTLIFE 20220630 revS | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20231231 ours=0.3 site=0.22 (6q away); 20230930 ours=0.47 site=0.21 (5q away); 20230331 ours=381.97 site=0.<br>**tl** quarter 20220630 absent (site holds 20230630..20260630)<br>**tt** tt(page): 0 quarters -..-; company reports consolidated only |
| WESTLIFE 20220930 revS | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20240331 ours=0.32 site=0.26 (6q away); 20231231 ours=0.3 site=0.22 (5q away); 20230930 ours=0.47 site=0.21<br>**tl** quarter 20220930 absent (site holds 20230630..20260630)<br>**tt** tt(page): 0 quarters -..-; company reports consolidated only |

## NEEDS-CROSSCHECK (a site HAD the quarter; its series does not reproduce ours — GATE-A/GATE-A3) — 6 cells

| cell | what each site said |
|---|---|
| ACC 20211231 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20220930 ours=3987.34 site=3910.49 (3q away); 20220630 ours=4468.42 site=4393.27 (2q away); 20220331 ours=4<br>**tl** quarter 20211231 absent (site holds 20230630..20260630)<br>**tt** quarter 20211231 absent (site holds 20240331..20260630) |
| GICRE 20211231 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20230630 ours=11165.84 site=8696.42 (6q away); 20220630 ours=12645.84 site=10734.29 (2q away); rev_total: G<br>**tl** quarter 20211231 absent (site holds 20230331..20260331)<br>**tt** quarter 20211231 absent (site holds 20231231..20260331) |
| ICICIPRULI 20191231 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20210630 ours=16211.27 site=6601.85 (6q away); 20210331 ours=19281.83 site=11879.28 (5q away); 20201231 our<br>**tl** quarter 20191231 absent (site holds 20230331..20260331)<br>**tt** quarter 20191231 absent (site holds 20231231..20260331) |
| NIACL 20200331 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20210930 ours=9682.19 site=7518.54 (6q away); 20210630 ours=8115.95 site=6815.09 (5q away); 20210331 ours=9<br>**tl** quarter 20200331 absent (site holds 20230630..20260630)<br>**tt** quarter 20200331 absent (site holds 20240331..20260630) |
| NIACL 20200630 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20211231 ours=9468.41 site=7434.71 (6q away); 20210930 ours=9682.19 site=7518.54 (5q away); 20210630 ours=8<br>**tl** quarter 20200630 absent (site holds 20230630..20260630)<br>**tt** quarter 20200630 absent (site holds 20240331..20260630) |
| NIACL 20201231 revC | **mc** rev_ops: GATE-A: disagreement inside +-6q: 20220630 ours=8594.52 site=7229.53 (6q away); 20211231 ours=9468.41 site=7434.71 (4q away); 20210930 ours=9<br>**tl** quarter 20201231 absent (site holds 20230630..20260630)<br>**tt** quarter 20201231 absent (site holds 20240331..20260630) |

## NEEDS-CROSSCHECK (a site HAD the quarter; its series does not reproduce ours — GATE-A3) — 7 cells

| cell | what each site said |
|---|---|
| ALOKINDS 20200930 revC | **mc** rev_ops: GATE-A3: restatement boundary inside +-12q: 20220630 ours=2023.23 site=1971.52 (7q away); rev_total: GATE-A3: restatement boundary inside +-1<br>**tl** quarter 20200930 absent (site holds 20230630..20260630)<br>**tt** quarter 20200930 absent (site holds 20240331..20260630) |
| PFC 20220630 revS | **mc** rev_ops: GATE-A3: restatement boundary inside +-12q: 20190930 ours=7989.83 site=8022.54 (11q away); 20190630 ours=7531.87 site=7580.62 (12q away); rev<br>**tl** quarter 20220630 absent (site holds 20230630..20260630)<br>**tt** tt(page): 0 quarters -..-; company reports consolidated only |
| SHRIRAMCIT 20211231 revS | **mc** rev_ops: GATE-A3: restatement boundary inside +-12q: 20200331 ours=1488.58 site=1449.51 (7q away); 20190630 ours=1437.16 site=1493.47 (10q away); rev_<br>**tl** tl: symbol absent from trendlyne fundamental sitemap<br>**tt** tt: no page whose ticker equals SHRIRAMCIT |
| SHRIRAMCIT 20220331 revS | **mc** rev_ops: GATE-A3: restatement boundary inside +-12q: 20200331 ours=1488.58 site=1449.51 (8q away); 20190630 ours=1437.16 site=1493.47 (11q away); rev_<br>**tl** tl: symbol absent from trendlyne fundamental sitemap<br>**tt** tt: no page whose ticker equals SHRIRAMCIT |
| SHRIRAMCIT 20220630 revS | **mc** rev_ops: GATE-A3: restatement boundary inside +-12q: 20200331 ours=1488.58 site=1449.51 (9q away); 20190630 ours=1437.16 site=1493.47 (12q away); rev_<br>**tl** tl: symbol absent from trendlyne fundamental sitemap<br>**tt** tt: no page whose ticker equals SHRIRAMCIT |
| STAR 20211231 revC | **mc** rev_ops: GATE-A3: restatement boundary inside +-12q: 20190331 ours=839.7 site=616.18 (11q away); rev_total: GATE-A3: restatement boundary inside +-12q<br>**tl** quarter 20211231 absent (site holds 20230630..20260630)<br>**tt** quarter 20211231 absent (site holds 20240331..20260630) |
| TATACHEM 20211231 revC | **mc** rev_ops: GATE-A3: restatement boundary inside +-12q: 20190331 ours=2759.39 site=2561.4 (11q away); rev_total: GATE-A3: restatement boundary inside +-1<br>**tl** quarter 20211231 absent (site holds 20230630..20260630)<br>**tt** quarter 20211231 absent (site holds 20240331..20260630) |

## NEEDS-CROSSCHECK (a site HAD the quarter; its series does not reproduce ours — GATE-A4) — 1 cells

| cell | what each site said |
|---|---|
| TATASTLLP 20220630 revC | **mc** rev_ops: GATE-A4: 10/41 of the whole series disagrees -- different entity/basis; rev_total: GATE-A4: 8/39 of the whole series disagrees -- different e<br>**tl** tl: symbol absent from trendlyne fundamental sitemap<br>**tt** tt: no page whose ticker equals TATASTLLP |

## REJECT-EQUALS-OTHER-BASIS (gate C — the copied-con fingerprint; belongs to the §6A no-sub identity route, which writes it WITH evidence) — 5 cells

| cell | what each site said |
|---|---|
| BEML 20210331 revC | **mc** PASS gate A/A2/A3/A4 on 'Net Sales/Income from operations' (12 local + 30 total anchors, worst 0.01)<br>**tl** quarter 20210331 absent (site holds 20230630..20260630)<br>**tt** quarter 20210331 absent (site holds 20240331..20260630) |
| KSB 20210930 revC | **mc** PASS gate A/A2/A3/A4 on 'Net Sales/Income from operations' (12 local + 28 total anchors, worst 0.00)<br>**tl** quarter 20210930 absent (site holds 20230630..20260630)<br>**tt** quarter 20210930 absent (site holds 20240331..20260630) |
| ZFCVINDIA 20210630 revC | **mc** PASS gate A/A2/A3/A4 on 'Net Sales/Income from operations' (4 local + 18 total anchors, worst 0.01)<br>**tl** quarter 20210630 absent (site holds 20230630..20260630)<br>**tt** tt: no page whose ticker equals ZFCVINDIA |
| ZFCVINDIA 20210930 revC | **mc** PASS gate A/A2/A3/A4 on 'Net Sales/Income from operations' (5 local + 18 total anchors, worst 0.01)<br>**tl** quarter 20210930 absent (site holds 20230630..20260630)<br>**tt** tt: no page whose ticker equals ZFCVINDIA |
| ZFCVINDIA 20211231 revC | **mc** PASS gate A/A2/A3/A4 on 'Net Sales/Income from operations' (6 local + 18 total anchors, worst 0.01)<br>**tl** quarter 20211231 absent (site holds 20230630..20260630)<br>**tt** tt: no page whose ticker equals ZFCVINDIA |

## not-found-via:mc,tl,tt (no site holds this quarter for this basis) — 32 cells

| cell | what each site said |
|---|---|
| ABB 20200630 revC | **mc** quarter 20200630 absent (site holds 20111231..20260630)<br>**tl** quarter 20200630 absent (site holds 20111231..20260630)<br>**tt** quarter 20200630 absent (site holds 20220331..20260630) |
| ABB 20200930 revC | **mc** quarter 20200930 absent (site holds 20111231..20260630)<br>**tl** quarter 20200930 absent (site holds 20111231..20260630)<br>**tt** quarter 20200930 absent (site holds 20220331..20260630) |
| AXISBANK 20221231 revS | **mc** mc: 113 quarters 19980630..20260630<br>**tl** quarter 20221231 absent (site holds 20230630..20260630)<br>**tt** tt(page): 0 quarters -..-; company reports consolidated only |
| BORORENEW 20220331 revC | **mc** quarter 20220331 absent (site holds 20180630..20260630)<br>**tl** quarter 20220331 absent (site holds 20230630..20260630)<br>**tt** quarter 20220331 absent (site holds 20240331..20260630) |
| CENTRALBK 20191231 revC | **mc** mc: 33 quarters 20180630..20260630<br>**tl** quarter 20191231 absent (site holds 20230630..20260630)<br>**tt** quarter 20191231 absent (site holds 20240331..20260630) |
| CENTRALBK 20200930 revC | **mc** mc: 33 quarters 20180630..20260630<br>**tl** quarter 20200930 absent (site holds 20230630..20260630)<br>**tt** quarter 20200930 absent (site holds 20240331..20260630) |
| EMBDL 20201231 revS | **mc** mc: 78 quarters 20070331..20260630<br>**tl** EXC BadGzipFile: Not a gzipped file (b'\xf9W')<br>**tt** tt: no page whose ticker equals EMBDL |
| EMBDL 20210630 revS | **mc** mc: 78 quarters 20070331..20260630<br>**tl** EXC BadGzipFile: Not a gzipped file (b'\xf9W')<br>**tt** tt: no page whose ticker equals EMBDL |
| EMBDL 20211231 revS | **mc** mc: 78 quarters 20070331..20260630<br>**tl** EXC BadGzipFile: Not a gzipped file (b'\xf9W')<br>**tt** tt: no page whose ticker equals EMBDL |
| EMBDL 20220331 revS | **mc** mc: 78 quarters 20070331..20260630<br>**tl** EXC BadGzipFile: Not a gzipped file (b'\xf9W')<br>**tt** tt: no page whose ticker equals EMBDL |
| EMBDL 20220630 revS | **mc** mc: 78 quarters 20070331..20260630<br>**tl** EXC BadGzipFile: Not a gzipped file (b'\xf9W')<br>**tt** tt: no page whose ticker equals EMBDL |
| HATSUN 20211231 revC | **mc** quarter 20211231 absent (site holds 20090630..20251231)<br>**tl** quarter 20211231 absent (site holds 20090630..20251231)<br>**tt** tt(page): 0 quarters -..-; company reports standalone only |
| HATSUN 20220331 revC | **mc** quarter 20220331 absent (site holds 20090630..20251231)<br>**tl** quarter 20220331 absent (site holds 20090630..20251231)<br>**tt** tt(page): 0 quarters -..-; company reports standalone only |
| ICICIBANK 20220930 revC | **mc** mc: 54 quarters 20050331..20260630<br>**tl** quarter 20220930 absent (site holds 20230630..20260630)<br>**tt** quarter 20220930 absent (site holds 20240331..20260630) |
| INDIANB 20200930 revS | **mc** mc: 87 quarters 20041231..20260630<br>**tl** quarter 20200930 absent (site holds 20230630..20260630)<br>**tt** tt(page): 0 quarters -..-; company reports consolidated only |
| M&MFIN 20220930 revC | **mc** mc: no exact symbol match in autosuggest<br>**tl** tl: symbol absent from trendlyne fundamental sitemap<br>**tt** tt: no page whose ticker equals M&MFIN |
| M&M 20210930 revC | **mc** mc: no exact symbol match in autosuggest<br>**tl** tl: symbol absent from trendlyne fundamental sitemap<br>**tt** tt: no page whose ticker equals M&M |
| MOIL 20210331 revC | **mc** quarter 20210331 absent (site holds 20180630..20201231)<br>**tl** quarter 20210331 absent (site holds 20180630..20201231)<br>**tt** tt(page): 0 quarters -..-; company reports standalone only |
| MOIL 20210930 revC | **mc** quarter 20210930 absent (site holds 20180630..20201231)<br>**tl** quarter 20210930 absent (site holds 20180630..20201231)<br>**tt** tt(page): 0 quarters -..-; company reports standalone only |
| MOIL 20211231 revC | **mc** quarter 20211231 absent (site holds 20180630..20201231)<br>**tl** quarter 20211231 absent (site holds 20180630..20201231)<br>**tt** tt(page): 0 quarters -..-; company reports standalone only |
| MOIL 20220331 revC | **mc** quarter 20220331 absent (site holds 20180630..20201231)<br>**tl** quarter 20220331 absent (site holds 20180630..20201231)<br>**tt** tt(page): 0 quarters -..-; company reports standalone only |
| MOIL 20221231 revC | **mc** quarter 20221231 absent (site holds 20180630..20201231)<br>**tl** quarter 20221231 absent (site holds 20180630..20201231)<br>**tt** tt(page): 0 quarters -..-; company reports standalone only |
| RBLBANK 20210930 revC | **mc** mc: 33 quarters 20180630..20260630<br>**tl** quarter 20210930 absent (site holds 20230630..20260630)<br>**tt** quarter 20210930 absent (site holds 20240331..20260630) |
| RBLBANK 20211231 revS | **mc** mc: 45 quarters 20150630..20260630<br>**tl** quarter 20211231 absent (site holds 20230630..20260630)<br>**tt** tt(page): 0 quarters -..-; company reports consolidated only |
| TATASTLLP 20210930 revC | **mc** quarter 20210930 absent (site holds 20130331..20230930)<br>**tl** tl: symbol absent from trendlyne fundamental sitemap<br>**tt** tt: no page whose ticker equals TATASTLLP |
| TATASTLLP 20211231 revC | **mc** quarter 20211231 absent (site holds 20130331..20230930)<br>**tl** tl: symbol absent from trendlyne fundamental sitemap<br>**tt** tt: no page whose ticker equals TATASTLLP |
| TATASTLLP 20220331 revC | **mc** quarter 20220331 absent (site holds 20130331..20230930)<br>**tl** tl: symbol absent from trendlyne fundamental sitemap<br>**tt** tt: no page whose ticker equals TATASTLLP |
| ZFCVINDIA 20200331 revC | **mc** quarter 20200331 absent (site holds 20210630..20260630)<br>**tl** quarter 20200331 absent (site holds 20230630..20260630)<br>**tt** tt: no page whose ticker equals ZFCVINDIA |
| ZFCVINDIA 20200630 revC | **mc** quarter 20200630 absent (site holds 20210630..20260630)<br>**tl** quarter 20200630 absent (site holds 20230630..20260630)<br>**tt** tt: no page whose ticker equals ZFCVINDIA |
| ZFCVINDIA 20200930 revC | **mc** quarter 20200930 absent (site holds 20210630..20260630)<br>**tl** quarter 20200930 absent (site holds 20230630..20260630)<br>**tt** tt: no page whose ticker equals ZFCVINDIA |
| ZFCVINDIA 20201231 revC | **mc** quarter 20201231 absent (site holds 20210630..20260630)<br>**tl** quarter 20201231 absent (site holds 20230630..20260630)<br>**tt** tt: no page whose ticker equals ZFCVINDIA |
| ZFCVINDIA 20210331 revC | **mc** quarter 20210331 absent (site holds 20210630..20260630)<br>**tl** quarter 20210331 absent (site holds 20230630..20260630)<br>**tt** tt: no page whose ticker equals ZFCVINDIA |

## SUSPECT cells of OURS surfaced in passing (48) — reported, NOT patched

§61a mode 6: a site reproduces our series everywhere except here. The indictment is against us. Correcting a stored value is the §2b procedure with its own evidence, never a side effect of a fill pass (§58d).

| cell | ours | site value(s) | series agreement |
|---|---|---|---|
| 360ONE 20191231 revS | 101.71 | mc=45.03 | mc 24/26 |
| 360ONE 20230930 revS | 63.44 | mc=67.59, tl=67.59 | mc 24/26, tl 12/13 |
| ABB 20190331 revC | 1828.3 | mc=1850.25, tl=1850.25 | mc 6/9, tl 6/9 |
| ABB 20250331 revC | 3139.68 | mc=3010.07, tl=3010.07 | mc 6/9, tl 6/9 |
| ABB 20250630 revC | 3144.52 | mc=3324.93, tl=3324.93 | mc 6/9, tl 6/9 |
| ACC 20241231 revC | 5972.0 | tl=5927.38 | tl 12/13 |
| ADANIENT 20141231 revC | 2.44 | mc=17806.86 | mc 34/36 |
| ADANIENT 20160630 revC | 8918.69 | mc=8884.74 | mc 34/36 |
| ALOKINDS 20220630 revC | 2023.23 | mc=1971.52 | mc 23/24 |
| CCAVENUE 20230930 revC | 787.0 | tl=789.93 | tl 11/13 |
| CCAVENUE 20240630 revC | 745.06 | tl=752.75 | tl 11/13 |
| EDELWEISS 20250930 revC | 4106.52 | tl=1860.87 | tl 12/13 |
| GICRE 20230331 revS | 10556.32 | tl=9408.2 | tl 9/12 |
| GICRE 20230930 revC | 13224.18 | mc=13075.11, tl=13075.11 | mc 9/10, tl 8/9 |
| GICRE 20230930 revS | 13224.18 | tl=13059.08 | tl 9/12 |
| GICRE 20240930 revS | 12130.38 | tl=12330.38 | tl 9/12 |
| GMRAIRPORT 20230630 revC | 2017.63 | tl=2034.76 | tl 12/13 |
| HATSUN 20240930 revC | 2072.19 | tl=2078.71 | tl 6/8 |
| HATSUN 20250331 revC | 2242.85 | tl=2251.37 | tl 6/8 |
| HDFCLIFE 20240930 revC | 28227.0 | tl=28496.97 | tl 10/11 |
| ICICIPRULI 20220630 revC | -1610.87 | mc=15379.27 | mc 22/25 |
| ICICIPRULI 20230331 revC | 10983.81 | mc=10723.8, tl=10723.8 | mc 22/25, tl 11/13 |
| ICICIPRULI 20230630 revC | 23383.59 | mc=23829.23, tl=23829.23 | mc 22/25, tl 11/13 |
| IDFC 20180630 revS | 11.05 | mc=14.49 | mc 64/65 |
| JINDALSTEL 20150630 revC | 4426.32 | mc=4405.73 | mc 37/40 |
| JINDALSTEL 20150930 revC | 4707.48 | mc=4736.29 | mc 37/40 |
| JINDALSTEL 20180630 revC | 9602.41 | mc=9665.35 | mc 37/40 |
| KSB 20181231 revC | 0.0 | mc=346.6 | mc 28/29 |
| LICI 20230630 revC | 190163.06 | mc=189523.11, tl=189523.11 | mc 9/10, tl 8/9 |
| LICI 20230630 revS | 188749.16 | mc=189300.06, tl=189300.06 | mc 16/17, tl 12/13 |
| MMTC 20220331 revC | 2255.59 | mc=1701.65 | mc 26/27 |
| MOIL 20190331 revC | 436.59 | mc=427.98 | mc 10/11 |
| NIACL 20230630 revC | 9900.04 | tl=9332.23 | tl 9/12 |
| NIACL 20250331 revC | 11664.22 | tl=11008.74 | tl 9/12 |
| NIACL 20250630 revC | 11719.01 | tl=11080.52 | tl 9/12 |
| PFC 20170331 revS | 5966.84 | mc=5672.07 | mc 71/74 |
| PFC 20190630 revS | 7531.87 | mc=7580.62 | mc 71/74 |
| PFC 20190930 revS | 7989.83 | mc=8022.54 | mc 71/74 |
| RAIN 20170630 revC | 2716.63 | mc=2637.12 | mc 40/41 |
| RAJESHEXPO 20150630 revC | 8419.88 | mc=15144.26 | mc 37/39 |
| RAJESHEXPO 20150930 revC | 51011.66 | mc=44319.65 | mc 37/39 |
| RAJESHEXPO 20240331 revC | 91444.96 | tl=91649.32 | tl 12/13 |
| SHRIRAMCIT 20180331 revS | 1316.48 | mc=1242.79 | mc 27/30 |
| SHRIRAMCIT 20190630 revS | 1437.16 | mc=1493.47 | mc 27/30 |
| SHRIRAMCIT 20200331 revS | 1488.58 | mc=1449.51 | mc 27/30 |
| WELCORP 20180630 revC | 2023.37 | mc=1641.89 | mc 37/39 |
| WELCORP 20180930 revC | 2354.71 | mc=2152.45 | mc 37/39 |
| WESTLIFE 20250331 revS | 0.29 | tl=0.39 | tl 12/13 |
