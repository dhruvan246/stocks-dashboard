# Groww p3 extraction notes (66-symbol frozen sample)

Total records: 575
Symbols resolved + identity-confirmed: 62

## slug_unresolved (exhausted ~3 guesses against /stocks/<slug>/company-financial, all 404 -- distinct from identity_skip below)
- AJRINFRA: tried [ajr-infra-and-tolling-ltd -> 404, ajr-infra-tolling-ltd -> 404, ajr-infra-tolling-limited -> 404]
- CASTEXTECH: tried [castex-technologies-ltd -> 404, castex-technologies-limited -> 404, amtek-india-ltd -> 404]
- IGIL: tried [international-gemological-institute-india-ltd -> 404, igi-india-ltd -> 404, igi-ltd -> 404]

(Note: FLUOROCHEM also appears in the raw unresolved log because its only candidate slug got HTTP 200 but failed identity match -- see identity_skip section below, not counted here.)

## identity_skip (candidate page fetched OK, but header.nseScriptCode did not match the target ticker)
- FLUOROCHEM: slug=`gujarat-fluorochemicals-ltd` -> header.nseScriptCode='GFLLIMITED', header.isin='INE538A01037' (expected nseScriptCode='FLUOROCHEM') -- SKIPPED

## Per-symbol detail

## AADHARHFC
- slug: `aadhar-housing-finance-ltd`
- url: https://groww.in/stocks/aadhar-housing-finance-ltd/company-financial
- isin: INE883F01010
- nseScriptCode echo: AADHARHFC
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## AARTIPHARM
- slug: `aarti-pharmalabs-ltd`
- url: https://groww.in/stocks/aarti-pharmalabs-ltd/company-financial
- isin: INE0LRU01027
- nseScriptCode echo: AARTIPHARM
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## ABFRL
- slug: `aditya-birla-fashion-and-retail-ltd`
- url: https://groww.in/stocks/aditya-birla-fashion-and-retail-ltd/company-financial
- isin: INE647O01011
- nseScriptCode echo: ABFRL
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## ABSLAMC
- slug: `aditya-birla-sun-life-amc-ltd`
- url: https://groww.in/stocks/aditya-birla-sun-life-amc-ltd/company-financial
- isin: INE404A01024
- nseScriptCode echo: ABSLAMC
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## AJANTPHARM
- slug: `ajanta-pharma-ltd`
- url: https://groww.in/stocks/ajanta-pharma-ltd/company-financial
- isin: INE031B01049
- nseScriptCode echo: AJANTPHARM
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## BAJFINANCE
- slug: `bajfinance`
- url: https://groww.in/stocks/bajfinance/company-financial
- isin: INE296A01032
- nseScriptCode echo: BAJFINANCE
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## BALKRISIND
- slug: `balkrishna-industries-ltd`
- url: https://groww.in/stocks/balkrishna-industries-ltd/company-financial
- isin: INE787D01026
- nseScriptCode echo: BALKRISIND
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## BPCL
- slug: `bharat-petroleum-corporation-ltd`
- url: https://groww.in/stocks/bharat-petroleum-corporation-ltd/company-financial
- isin: INE029A01011
- nseScriptCode echo: BPCL
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## CCL
- slug: `ccl-products-india-ltd`
- url: https://groww.in/stocks/ccl-products-india-ltd/company-financial
- isin: INE421D01022
- nseScriptCode echo: CCL
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## CUMMINSIND
- slug: `cummins-india-ltd`
- url: https://groww.in/stocks/cummins-india-ltd/company-financial
- isin: INE298A01020
- nseScriptCode echo: CUMMINSIND
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## EICHERMOT
- slug: `eicher-motors-ltd`
- url: https://groww.in/stocks/eicher-motors-ltd/company-financial
- isin: INE066A01021
- nseScriptCode echo: EICHERMOT
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## ENRIN
- slug: `siemens-energy-india-ltd`
- url: https://groww.in/stocks/siemens-energy-india-ltd/company-financial
- isin: INE1NPP01017
- nseScriptCode echo: ENRIN
- consolidatedQuarterly populated: False (0 periods)
- standaloneQuarterly populated: True (5 periods)
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## ETERNAL
- slug: `eternal`
- url: https://groww.in/stocks/eternal/company-financial
- isin: INE758T01015
- nseScriptCode echo: ETERNAL
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## FEDERALBNK
- slug: `the-federal-bank-ltd`
- url: https://groww.in/stocks/the-federal-bank-ltd/company-financial
- isin: INE171A01029
- nseScriptCode echo: FEDERALBNK
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## GICRE
- slug: `gicre`
- url: https://groww.in/stocks/gicre/company-financial
- isin: INE481Y01014
- nseScriptCode echo: GICRE
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Mar '25", '2025-03-31'), ("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31')]
  - std periods: [("Mar '25", '2025-03-31'), ("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31')]
- con/std distinct (or not both populated) for all common quarters

## GMRP&UI
- slug: `gmr-power-and-urban-infra-ltd`
- url: https://groww.in/stocks/gmr-power-and-urban-infra-ltd/company-financial
- isin: INE0CU601026
- nseScriptCode echo: GMRP&UI
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Mar '25", '2025-03-31'), ("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31')]
  - std periods: [("Mar '25", '2025-03-31'), ("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31')]
- con/std distinct (or not both populated) for all common quarters

## GODIGIT
- slug: `go-digit-general-insurance-ltd`
- url: https://groww.in/stocks/go-digit-general-insurance-ltd/company-financial
- isin: INE03JT01014
- nseScriptCode echo: GODIGIT
- consolidatedQuarterly populated: False (0 periods)
- standaloneQuarterly populated: True (5 periods)
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## GRASIM
- slug: `grasim`
- url: https://groww.in/stocks/grasim/company-financial
- isin: INE047A01021
- nseScriptCode echo: GRASIM
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Mar '25", '2025-03-31'), ("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31')]
  - std periods: [("Mar '25", '2025-03-31'), ("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31')]
- con/std distinct (or not both populated) for all common quarters

## HDFCBANK
- slug: `hdfc-bank-ltd`
- url: https://groww.in/stocks/hdfc-bank-ltd/company-financial
- isin: INE040A01034
- nseScriptCode echo: HDFCBANK
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## HEROMOTOCO
- slug: `hero-motocorp-ltd`
- url: https://groww.in/stocks/hero-motocorp-ltd/company-financial
- isin: INE158A01026
- nseScriptCode echo: HEROMOTOCO
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## HINDALCO
- slug: `hindalco-industries-ltd`
- url: https://groww.in/stocks/hindalco-industries-ltd/company-financial
- isin: INE038A01020
- nseScriptCode echo: HINDALCO
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## HONASA
- slug: `honasa-consumer-ltd`
- url: https://groww.in/stocks/honasa-consumer-ltd/company-financial
- isin: INE0J5401028
- nseScriptCode echo: HONASA
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Mar '25", '2025-03-31'), ("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31')]
  - std periods: [("Mar '25", '2025-03-31'), ("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31')]
- con/std distinct (or not both populated) for all common quarters

## HUDCO
- slug: `housing-urban-development-corporation-ltd`
- url: https://groww.in/stocks/housing-urban-development-corporation-ltd/company-financial
- isin: INE031A01017
- nseScriptCode echo: HUDCO
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- **FLAG: con and std rows are numerically IDENTICAL for quarters: ['2025-06-30', '2025-09-30', '2025-12-31', '2026-03-31', '2026-06-30']**

## INDIAMART
- slug: `indiamart-intermesh-ltd`
- url: https://groww.in/stocks/indiamart-intermesh-ltd/company-financial
- isin: INE933S01016
- nseScriptCode echo: INDIAMART
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## INDOBORAX
- slug: `indo-borax-chemicals-ltd`
- url: https://groww.in/stocks/indo-borax-chemicals-ltd/company-financial
- isin: INE803D01021
- nseScriptCode echo: INDOBORAX
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Mar '25", '2025-03-31'), ("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31')]
  - std periods: [("Mar '25", '2025-03-31'), ("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31')]
- con/std distinct (or not both populated) for all common quarters

## JIOFIN
- slug: `jio-financial-services-ltd`
- url: https://groww.in/stocks/jio-financial-services-ltd/company-financial
- isin: INE758E01017
- nseScriptCode echo: JIOFIN
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## JUBLPHARMA
- slug: `jubilant-life-sciences-ltd`
- url: https://groww.in/stocks/jubilant-life-sciences-ltd/company-financial
- isin: INE700A01033
- nseScriptCode echo: JUBLPHARMA
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Mar '25", '2025-03-31'), ("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31')]
  - std periods: [("Mar '25", '2025-03-31'), ("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31')]
- con/std distinct (or not both populated) for all common quarters

## LICI
- slug: `lici`
- url: https://groww.in/stocks/lici/company-financial
- isin: INE0J1Y01017
- nseScriptCode echo: LICI
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## M&MFIN
- slug: `mahindra-mahindra-financial-services-ltd`
- url: https://groww.in/stocks/mahindra-mahindra-financial-services-ltd/company-financial
- isin: INE774D01024
- nseScriptCode echo: M&MFIN
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## MEESHO
- slug: `meesho`
- url: https://groww.in/stocks/meesho/company-financial
- isin: INE0VDM01015
- nseScriptCode echo: MEESHO
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- **FLAG: con and std rows are numerically IDENTICAL for quarters: ['2025-06-30']**

## MOTHERSON
- slug: `motherson`
- url: https://groww.in/stocks/motherson/company-financial
- isin: INE775A01035
- nseScriptCode echo: MOTHERSON
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## MSUMI
- slug: `msumi`
- url: https://groww.in/stocks/msumi/company-financial
- isin: INE0FS801015
- nseScriptCode echo: MSUMI
- consolidatedQuarterly populated: False (0 periods)
- standaloneQuarterly populated: True (5 periods)
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## MUTHOOTFIN
- slug: `muthoot-finance-ltd`
- url: https://groww.in/stocks/muthoot-finance-ltd/company-financial
- isin: INE414G01012
- nseScriptCode echo: MUTHOOTFIN
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## NCC
- slug: `ncc-ltd`
- url: https://groww.in/stocks/ncc-ltd/company-financial
- isin: INE868B01028
- nseScriptCode echo: NCC
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## NIACL
- slug: `the-new-india-assurance-co-ltd`
- url: https://groww.in/stocks/the-new-india-assurance-co-ltd/company-financial
- isin: INE470Y01017
- nseScriptCode echo: NIACL
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## NIVABUPA
- slug: `niva-bupa-health-insurance-company-ltd`
- url: https://groww.in/stocks/niva-bupa-health-insurance-company-ltd/company-financial
- isin: INE995S01015
- nseScriptCode echo: NIVABUPA
- consolidatedQuarterly populated: False (0 periods)
- standaloneQuarterly populated: True (5 periods)
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## NTPCGREEN
- slug: `ntpc-green-energy-ltd`
- url: https://groww.in/stocks/ntpc-green-energy-ltd/company-financial
- isin: INE0ONG01011
- nseScriptCode echo: NTPCGREEN
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## PINELABS
- slug: `pine-labs-ltd`
- url: https://groww.in/stocks/pine-labs-ltd/company-financial
- isin: INE15B701018
- nseScriptCode echo: PINELABS
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## POLICYBZR
- slug: `pb-fintech-ltd`
- url: https://groww.in/stocks/pb-fintech-ltd/company-financial
- isin: INE417T01026
- nseScriptCode echo: POLICYBZR
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## POLYMED
- slug: `poly-medicure-ltd`
- url: https://groww.in/stocks/poly-medicure-ltd/company-financial
- isin: INE205C01021
- nseScriptCode echo: POLYMED
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## POWERINDIA
- slug: `abb-power-products-systems-india-ltd`
- url: https://groww.in/stocks/abb-power-products-systems-india-ltd/company-financial
- isin: INE07Y701011
- nseScriptCode echo: POWERINDIA
- consolidatedQuarterly populated: False (0 periods)
- standaloneQuarterly populated: True (5 periods)
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## PREMIERENE
- slug: `premier-energies-ltd`
- url: https://groww.in/stocks/premier-energies-ltd/company-financial
- isin: INE0BS701011
- nseScriptCode echo: PREMIERENE
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## PWL
- slug: `physicswallah-ltd`
- url: https://groww.in/stocks/physicswallah-ltd/company-financial
- isin: INE0LP301011
- nseScriptCode echo: PWL
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Mar '25", '2025-03-31'), ("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31')]
  - std periods: [("Mar '25", '2025-03-31'), ("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31')]
- con/std distinct (or not both populated) for all common quarters

## RADICO
- slug: `radico-khaitan-ltd`
- url: https://groww.in/stocks/radico-khaitan-ltd/company-financial
- isin: INE944F01028
- nseScriptCode echo: RADICO
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## RAJRATAN
- slug: `rajratan-global-wire-ltd`
- url: https://groww.in/stocks/rajratan-global-wire-ltd/company-financial
- isin: INE451D01029
- nseScriptCode echo: RAJRATAN
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## RBLBANK
- slug: `rbl-bank-ltd`
- url: https://groww.in/stocks/rbl-bank-ltd/company-financial
- isin: INE976G01028
- nseScriptCode echo: RBLBANK
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## RELIANCE
- slug: `reliance-industries-ltd`
- url: https://groww.in/stocks/reliance-industries-ltd/company-financial
- isin: INE002A01018
- nseScriptCode echo: RELIANCE
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## SAGILITY
- slug: `sagility-india-ltd`
- url: https://groww.in/stocks/sagility-india-ltd/company-financial
- isin: INE0W2G01015
- nseScriptCode echo: SAGILITY
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## SAIL
- slug: `sail`
- url: https://groww.in/stocks/sail/company-financial
- isin: INE114A01011
- nseScriptCode echo: SAIL
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## SBILIFE
- slug: `sbilife`
- url: https://groww.in/stocks/sbilife/company-financial
- isin: INE123W01016
- nseScriptCode echo: SBILIFE
- consolidatedQuarterly populated: False (0 periods)
- standaloneQuarterly populated: True (5 periods)
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## SBIN
- slug: `sbin`
- url: https://groww.in/stocks/sbin/company-financial
- isin: INE062A01020
- nseScriptCode echo: SBIN
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## SHK
- slug: `sh-kelkar-co-ltd`
- url: https://groww.in/stocks/sh-kelkar-co-ltd/company-financial
- isin: INE500L01026
- nseScriptCode echo: SHK
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## SOBHA
- slug: `sobha-ltd`
- url: https://groww.in/stocks/sobha-ltd/company-financial
- isin: INE671H01015
- nseScriptCode echo: SOBHA
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## STARHEALTH
- slug: `star-health-and-allied-insurance-company-ltd`
- url: https://groww.in/stocks/star-health-and-allied-insurance-company-ltd/company-financial
- isin: INE575P01011
- nseScriptCode echo: STARHEALTH
- consolidatedQuarterly populated: False (0 periods)
- standaloneQuarterly populated: True (5 periods)
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## SUNDARMFIN
- slug: `sundaram-finance-ltd`
- url: https://groww.in/stocks/sundaram-finance-ltd/company-financial
- isin: INE660A01013
- nseScriptCode echo: SUNDARMFIN
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## SUPREMEIND
- slug: `supreme-industries-ltd`
- url: https://groww.in/stocks/supreme-industries-ltd/company-financial
- isin: INE195A01028
- nseScriptCode echo: SUPREMEIND
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## TARIL
- slug: `transformers-rectifiers-india-ltd`
- url: https://groww.in/stocks/transformers-rectifiers-india-ltd/company-financial
- isin: INE763I01026
- nseScriptCode echo: TARIL
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## TATAINVEST
- slug: `tata-investment-corporation-ltd`
- url: https://groww.in/stocks/tata-investment-corporation-ltd/company-financial
- isin: INE672A01026
- nseScriptCode echo: TATAINVEST
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## TATASTEEL
- slug: `tatasteel`
- url: https://groww.in/stocks/tatasteel/company-financial
- isin: INE081A01020
- nseScriptCode echo: TATASTEEL
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## TECHM
- slug: `tech-mahindra-ltd`
- url: https://groww.in/stocks/tech-mahindra-ltd/company-financial
- isin: INE669C01036
- nseScriptCode echo: TECHM
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters

## ZEAL
- slug: `zeal-global-services-ltd`
- url: https://groww.in/stocks/zeal-global-services-ltd/company-financial
- isin: INE0PPS01018
- nseScriptCode echo: ZEAL
- consolidatedQuarterly populated: False (0 periods)
- standaloneQuarterly populated: False (0 periods)
- con/std distinct (or not both populated) for all common quarters

## ZFCVINDIA
- slug: `wabco-india-ltd`
- url: https://groww.in/stocks/wabco-india-ltd/company-financial
- isin: INE342J01019
- nseScriptCode echo: ZFCVINDIA
- consolidatedQuarterly populated: True (5 periods)
- standaloneQuarterly populated: True (5 periods)
  - con periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
  - std periods: [("Jun '25", '2025-06-30'), ("Sep '25", '2025-09-30'), ("Dec '25", '2025-12-31'), ("Mar '26", '2026-03-31'), ("Jun '26", '2026-06-30')]
- con/std distinct (or not both populated) for all common quarters
