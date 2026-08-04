/* =========================================================================
 * STOCKSWORLD — SITE GLOSSARY  (lazy-loaded; see buildGlossary() in theme.js)
 *
 * Every page on the site ends with a collapsed "📖 Glossary" panel that spells
 * out ONLY the terms that page actually uses. theme.js injects the shell on
 * every page and pulls this file in the first time a reader opens it, so the
 * text below costs nothing on a normal page load.
 *
 * TWO PARTS
 *   T = the dictionary. One entry per concept, defined ONCE and reused by every
 *       page that shows it (Delivery % is written once, used by 6 pages).
 *         key: ['Label shown', 'definition, may contain <b> <i> <code>']
 *   P = the per-page term lists.
 *         'page.html': {
 *            sub:   short parenthetical after "Glossary" in the summary line,
 *            intro: optional <p> paragraphs shown above the term lists,
 *            secs:  [ ['Heading', ['termKey', 'termKey|Label override']], ... ],
 *            note:  optional closing fine-print paragraph
 *         }
 *
 * ADDING A TERM TO A PAGE: find the page in P and add its key to a section. If
 * the concept is new, add it to T first. If the page shows it under a different
 * name than the dictionary label, write 'key|The name this page uses'.
 *
 * Use &lt; and &gt; for angle brackets — these strings go through innerHTML.
 * ========================================================================= */
window.SW_GLOSSARY = (function () {

  var T = {

    /* ---------- price, size and the basics every table repeats ---------- */
    close:      ['Close', "The last traded price of the session. Everywhere on this site prices are split- and bonus-adjusted, so an old price stays comparable with today's."],
    closeadj:   ['Close (adj.)', "<b>Adjusted close</b> — the closing price restated for every split, bonus and demerger since. A chart of it shows the return you actually earned, not the cosmetic drop on a 1:10 split day."],
    closeraw:   ['Last close', "The raw traded price on the exchange, with no split/bonus adjustment — the number your broker screen showed that evening."],
    ltp:        ['LTP / Live ₹', "<b>Last traded price</b>. When the market is open this is a live quote; outside market hours it is the latest close."],
    daychg:     ['Day %', "Today's close against the previous close, in per cent."],
    chgpct:     ['Chg %', "Change in per cent over the window named in the column header, close to close."],
    volume:     ['Volume', "Number of shares that changed hands in the session."],
    turnover:   ['Turnover', "The rupee value traded — price × quantity. ₹1 crore = ₹100 lacs."],
    mcap:       ['Mkt cap (₹ cr)', "<b>Market capitalisation</b> — share price × shares outstanding. The market's price tag on the whole company. Large caps sit above roughly ₹50,000 cr, small caps below ₹5,000 cr."],
    histmcap:   ['Hist mcap', "<b>Historical market cap</b> — what the company was worth on the date shown, not today. Screens use it so an old ranking reflects the sizes as they were then."],
    h52:        ['52-week high', "The highest price of the last one year."],
    l52:        ['52-week low', "The lowest price of the last one year."],
    from52h:    ['From 52w high %', "How far below its one-year peak the stock trades, as a positive number: high ₹100, price ₹95 → <code>5</code>. Small = near its high."],
    from52l:    ['From 52w low %', "How far <i>above</i> its one-year low the stock trades. <code>100</code> means it has doubled off the low."],
    sector:     ['Sector', "The broad business grouping (Financials, IT, Energy…). Sectors come from the BSE classification, so the same company is grouped the same way everywhere on the site."],
    subind:     ['Sub-industry', "The narrowest rung of the classification — 'Private Banks' rather than just 'Financials'. Peer comparisons use this level."],
    industry:   ['Industry', "The middle rung between sector and sub-industry — e.g. Banks inside Financials."],
    symbol:     ['Symbol', "The NSE ticker, e.g. <code>RELIANCE</code>. Where a company has been renamed, history is carried over to the new symbol so the chart does not break."],

    /* ---------- returns and performance ---------- */
    cagr:       ['CAGR %', "<b>Compound annual growth rate</b> — the yearly rate that turns the starting value into the ending value over the period. It smooths a lumpy journey into one comparable number."],
    totret:     ['Total %', "Absolute return over the whole window, <i>not</i> annualised. Shown as a multiple above 1,000% (<code>12.4x</code> = the money multiplied 12.4 times)."],
    maxdd:      ['Max DD %', "<b>Maximum drawdown</b> — the worst peak-to-trough fall along the way. It answers 'how bad did it get before it recovered?', which an average return hides."],
    winpct:     ['Win %', "The share of rebalance periods that ended positive."],
    ytd:        ['YTD', "<b>Year to date</b> — return from 1 January of the current calendar year to now."],
    fytd:       ['FYTD', "<b>Financial year to date</b> — return since 1 April, the start of the Indian financial year."],
    hitrate:    ['Hit rate', "The share of periods that finished positive — 8 green months out of 12 = a 67% hit rate."],
    alpha:      ['Alpha', "Return over and above the benchmark's over the same days. Positive alpha = it beat the index; the index itself always has zero."],
    benchmark:  ['Benchmark', "The index a result is measured against — usually the Nifty 50 or Nifty 500. A strategy is only interesting if it beats the index you could have bought instead."],
    rs:         ['RS vs Nifty', "<b>Relative strength</b> — the stock's return minus the Nifty's over the same window. Positive means it outran the market, even if both fell."],
    annualised: ['Annualised', "A figure rescaled to a per-year rate so windows of different lengths can be compared side by side."],

    /* ---------- strategy mechanics (backtest family) ---------- */
    rankby:     ['Rank by', "The single factor that sorts the surviving stocks, written <code>direction-factor</code>. <code>high-ret6m</code> = biggest 6-month return first; <code>low-upPct</code> = <i>smallest</i> up-day consistency first. The top few of that sorted list is what the strategy buys."],
    filters:    ['Filters', "The entry conditions, all ANDed and applied <i>before</i> the ranking. <code>d52&lt;=25 &amp; fiiChgPp&gt;0</code> keeps only stocks within 25% of their 52-week high that also had FIIs adding last quarter, then ranks whatever is left. A stock missing a factor's value is dropped, never ranked last."],
    rebalance:  ['Rebalance', "The day the basket is reviewed and reshuffled — on this site, the last trading day of the chosen period (monthly, quarterly, half-yearly or yearly)."],
    reset:      ['Monthly reset', "The whole basket is sold and re-bought equal-weight at every rebalance, so each holding starts the period with the same rupees."],
    ride:       ['Ride winners (hold)', "Only names that fell out of the top N are sold; survivors are kept untouched, so winners compound and weights drift. The freed cash is split equally among the empty slots."],
    topn:       ['Top N', "How many stocks the basket holds. Fewer names means higher returns when the factor works and a far rougher ride when it does not."],
    equalwt:    ['Equal-weight', "Every holding gets the same rupee amount, rather than being sized by market cap."],
    universe:   ['Universe', "The pool of stocks a strategy may buy — Nifty 500, Nifty 50, F&amp;O names, and so on."],
    pit:        ['Point-in-time', "Only what was actually knowable on the date is used: a stock is eligible only if it was really in the index that day, and a quarter's numbers appear only after its real filing date. This is what stops a backtest from quietly cheating."],
    survfree:   ['Survivorship-free', "Companies that later got delisted, merged or collapsed stay in the history. Drop them and every backtest flatters itself, because only the survivors are left to test on."],
    lookahead:  ['Look-ahead bias', "Using information that had not been published yet on the date it is being applied. It makes a backtest look brilliant and is impossible to trade."],
    worstrank:  ['Worst rank', "A strategy's <i>poorest</i> rank across all phases (or all calendar years) out of every combination tested. Low = never bad in any regime, which is the whole point of the ⭐ cards."],
    finalval:   ['Final ₹1L', "What ₹1 lakh invested at the start would be worth at the end of the run."],
    livepicks:  ['Live picks', "The stocks the strategy's rules select <i>today</i>, using the same code the backtest used on history. It is the strategy pointed forward instead of backward."],

    /* ---------- momentum factors ---------- */
    ret1m:      ['ret1m', "<b>Return — 1 month %</b>. Price today vs price 30 days ago."],
    ret3m:      ['ret3m', "<b>Return — 3 month %</b>. Price today vs 91 days ago."],
    ret6m:      ['ret6m', "<b>Return — 6 month %</b>. Price today vs 182 days ago."],
    ret12m:     ['ret12m', "<b>Return — 12 month %</b>. Price today vs 365 days ago."],
    accel:      ['accel', "<b>Momentum acceleration</b>. This month's 1-month return minus last month's, in percentage points. Positive = the move is speeding up."],
    riskmom:    ['riskMom', "<b>Risk-adjusted momentum</b>. 3-month return ÷ annualised volatility. Rewards a steady climb over a violent one."],
    postdrift:  ['postDrift', "<b>Post-result drift %</b>. Return since the company's last earnings announcement date — the classic post-earnings drift."],
    composite:  ['composite', "<b>Quality-Momentum composite</b>. Sum of three same-day cross-sectional z-scores: TTM profit growth (+), 12-month return (+), volatility (−). 0 = an average stock that day; ≥2 = strong on all three at once."],

    /* ---------- trend & price levels ---------- */
    d52:        ['d52', "<b>Distance from the 52-week high %</b>, as a positive number (Trendlyne convention). High 100, price 95 → <code>5</code>. So <code>d52&lt;=10</code> means 'within 10% of its one-year high'."],
    d52low:     ['d52_low_pct', "<b>Distance from the 52-week low %</b> — how far <i>above</i> the low it trades. <code>100</code> = it has doubled off the low."],
    rangepos:   ['rangePos', "<b>52-week range position</b>. Where the price sits between the year's low (0) and high (100)."],
    dayshigh:   ['daysHigh', "<b>Days since the 52-week high</b>. Low = it just made a new high; high = a long time since."],
    dma50:      ['dma50', "<b>Distance from the 50-DMA %</b> — how far above (+) or below (−) the average close of the last 50 days the price sits."],
    dma200:     ['dma200', "<b>Distance from the 200-DMA %</b> — the same, against the 200-day average. The 200-DMA is the usual dividing line between a long-term uptrend and a downtrend."],
    dma:        ['DMA', "<b>Daily moving average</b> — the average closing price of the last N days, redrawn each day. It smooths the noise so the underlying trend shows."],
    indrank:    ['indRank', "<b>Industry momentum rank</b>. On each rebalance date industries are ranked by their members' average 1-month return and bucketed into deciles: <code>1</code> = hottest industry, <code>10</code> = coldest."],

    /* ---------- risk ---------- */
    vol:        ['vol', "<b>Volatility %</b>. Standard deviation of daily returns over the last 90 days, annualised. High = the price swings hard in both directions."],
    beta:       ['beta', "<b>Beta vs Nifty</b>. Slope of the stock's daily returns against the Nifty's over the last 120 days. 1 = moves with the index, &gt;1 = amplifies it, &lt;1 = damps it."],
    mdd6:       ['mdd6', "<b>Max drawdown — 6 month %</b>. Worst peak-to-trough fall in the stock over the last 182 days."],
    uppct:      ['upPct', "<b>Up-day consistency %</b>. Of the ~62 trading sessions in the last 90 days, the share that closed higher than the previous close. High = a steady grind; low = the gains (or losses) came in a few jumps."],

    /* ---------- liquidity & participation ---------- */
    turnfac:    ['turnover', "<b>Average daily turnover</b> in ₹ lacs over 20 days (₹100 lacs = ₹1 cr). <code>turnover&gt;=2000</code> = at least ₹20 cr traded a day. Thin stocks are hard to actually buy at the screen price."],
    turnsurge:  ['turnSurge', "<b>Turnover surge</b>. Last week's average turnover ÷ the last 90 days'. <code>1.5</code> = money flowing in 50% heavier than usual."],
    volsurge:   ['volSurge', "<b>Volume surge</b> — the same ratio measured on share volume instead of rupees."],
    delivfac:   ['delivPct', "<b>Delivery %</b>. Average share of traded volume actually taken to demat (not squared off intraday) over the last 28 days. High = real buying, not churn. Data exists from 2020 onward."],

    /* ---------- oscillators ---------- */
    rsi:        ['RSI (14)', "<b>Relative Strength Index</b> over 14 periods — how much of the recent move was up versus down, on a 0-100 scale. Below 30 is called oversold, above 70 overbought. Extremes describe stretch, they are not a signal on their own."],
    macd:       ['MACD hist', "<b>MACD histogram (12,26,9)</b> — the MACD line minus its signal line. Above 0 = the short-term trend is running ahead of the medium-term one."],
    stoch:      ['stoch', "<b>Stochastic %K(14)</b> — where the close sits inside the last three weeks' price range, 0 to 100."],
    bollb:      ['Bollinger %b', "<b>Bollinger %b (20,2)</b> — where the close sits between the lower band (0) and the upper band (100). Above 100 = it has pushed clear of the upper band."],
    overbought: ['Overbought / Oversold', "Shorthand for a market that has run a long way up (or down) very fast, by RSI. It says the move is stretched, not that it must reverse."],

    /* ---------- profit factors (backtest engine) ---------- */
    profyoy:    ['profitYoyPct', "<b>Net profit growth, quarter YoY %</b>. The latest <i>filed</i> quarter's net profit against the same quarter a year earlier. A loss-making year-ago base is divided by its absolute value, so a swing from −10 cr to +5 cr reads +150% rather than a nonsense number."],
    profbase:   ['profitBase', "<b>Year-ago quarter's net profit, ₹cr</b> — the denominator itself. <code>profitBase&gt;0</code> filters out the optical growth you get off a loss-making base."],
    profaccel:  ['profitAccel', "<b>Profit-growth acceleration</b>. This quarter's YoY growth minus last quarter's, in points. Positive = growth is improving, not merely positive."],
    profttm:    ['profitTTM', "<b>TTM profit growth %</b>. The last 4 quarters added up against the 4 before them — it smooths out seasonality."],
    profstreak: ['profitStreak', "<b>Profit-growth streak</b>. Consecutive quarters of positive YoY growth, counting back from the latest filing."],

    /* ---------- earnings & fundamentals ---------- */
    revenue:    ['Revenue', "<b>Sales / total income</b> for the period, in ₹ crore — what the company billed before any cost is taken out."],
    opprofit:   ['Op. profit', "<b>Operating profit (EBITDA)</b> — revenue minus operating costs, before interest, depreciation and tax. It measures the core business only."],
    opm:        ['OPM %', "<b>Operating profit margin</b> — operating profit ÷ revenue. How many paise of every rupee of sales survive the running costs."],
    opmd:       ['OPM Δ', "Change in operating margin against the year-ago quarter, in percentage points. Rising margins mean profit is growing faster than sales."],
    netprofit:  ['Net profit (PAT)', "<b>Profit after tax</b>, in ₹ crore — the bottom line, after every cost, interest and tax. On this site it is the <i>owners-attributable</i> figure, i.e. excluding the slice that belongs to minority shareholders of subsidiaries."],
    pbt:        ['PBT', "<b>Profit before tax</b> — the bottom line one step earlier, before the tax charge."],
    ebit:       ['EBIT', "<b>Earnings before interest and tax</b> — operating profit after depreciation, before financing costs."],
    depr:       ['Depr', "<b>Depreciation</b> — the accounting cost of assets wearing out over their life. A real cost, but not a cash outflow this period."],
    interest:   ['Interest', "The financing cost of the company's borrowings for the period."],
    othinc:     ['Other inc', "<b>Other income</b> — earnings from outside the main business: treasury interest, dividends, one-off gains. A profit jump driven by this is not an operating improvement."],
    excep:      ['Excep', "<b>Exceptional items</b> — one-off gains or charges (asset sales, write-offs, settlements) that are not expected to repeat. Growth is best judged with these stripped out."],
    revyoy:     ['Rev YoY %', "<b>Revenue growth, year on year</b> — this quarter's revenue against the <i>same quarter</i> a year earlier, which cancels out seasonality."],
    patyoy:     ['PAT YoY %', "<b>Net profit growth, year on year</b> — this quarter's profit against the same quarter a year earlier."],
    yoy:        ['YoY', "<b>Year on year</b> — this period against the same period one year ago. The standard growth comparison for Indian quarterly results, because it is immune to seasonality."],
    qoq:        ['QoQ', "<b>Quarter on quarter</b> — this quarter against the one just before it. Faster to react, but seasonal businesses swing wildly on it."],
    ttm:        ['TTM', "<b>Trailing twelve months</b> — the last four reported quarters added together. It gives a full-year figure without waiting for the financial year to end."],
    consol:     ['Consolidated vs Standalone', "<b>Consolidated</b> accounts include every subsidiary; <b>standalone</b> is the parent company alone. This site prefers consolidated and compares both periods on the <i>same</i> basis, because mixing the two invents growth that never happened."],
    audited:    ['Audited / Unaudited', "Quarterly results are usually unaudited (limited review); the fourth-quarter and annual figures are audited. Audited numbers occasionally restate what a quarter reported."],
    filedate:   ['Filing date', "The date the company actually submitted the result to the exchange. Everything on this site becomes visible only from this date — never from the quarter's end date."],
    eps:        ['EPS ₹', "<b>Earnings per share</b> — net profit ÷ number of shares. Profit expressed per share you own."],
    pe:         ['P/E (TTM)', "<b>Price to earnings</b> — share price ÷ trailing-twelve-month EPS. Roughly, the years of current earnings you are paying for. High P/E = the market expects growth; it is only meaningful against the company's own history and its peers."],
    pb:         ['P/B', "<b>Price to book</b> — market cap ÷ shareholders' equity. The standard yardstick for banks and lenders, where book value is the real asset."],
    ps:         ['P/S (TTM)', "<b>Price to sales</b> — market cap ÷ TTM revenue. Used when profits are small or negative, so P/E says nothing."],
    roe:        ['ROE %', "<b>Return on equity</b> — profit ÷ shareholders' equity. How hard the owners' money is working."],
    roce:       ['ROCE %', "<b>Return on capital employed</b> — EBIT ÷ (equity + debt). Like ROE but judges the whole capital base, so it is not flattered by heavy borrowing."],
    roa:        ['ROA %', "<b>Return on assets</b> — profit ÷ total assets. The usual profitability yardstick for banks."],
    de:         ['Debt / Equity', "Total borrowings ÷ shareholders' equity. Above 1 means the company owes more than the owners have put in — fine for a lender, a warning for a manufacturer."],
    intcover:   ['Interest cover', "EBIT ÷ interest expense (TTM). How many times over the profit covers the interest bill. Below ~2 is uncomfortable."],
    cfo:        ['CFO', "<b>Cash flow from operations</b> — the cash the core business actually generated, as opposed to accounting profit."],
    cfopat:     ['CFO / PAT', "Operating cash flow ÷ net profit. Near or above 1 over several years means the reported profit turns into real cash; persistently far below is the classic accounting red flag."],
    capex:      ['Capex', "<b>Capital expenditure</b> — cash spent on plant, property and equipment. Heavy capex funds future growth and eats today's free cash."],
    borrow:     ['Borrowings', "Total debt owed to lenders, short and long term."],
    equitybs:   ['Equity', "<b>Shareholders' equity / net worth</b> — assets minus liabilities. The book value belonging to the owners."],

    /* ---------- banks & insurers ---------- */
    gnpa:       ['Gross NPA %', "<b>Gross non-performing assets</b> — the share of a bank's loan book where repayment has stopped (typically 90+ days overdue), before any provisioning."],
    nnpa:       ['Net NPA %', "Gross NPAs minus the provisions already set aside against them. What is still exposed."],
    car:        ['CAR %', "<b>Capital adequacy ratio</b> — capital ÷ risk-weighted assets. The regulator's cushion test; RBI's floor for banks is around 11.5%."],
    cet1:       ['CET-1 %', "<b>Common equity tier-1</b> — the highest-quality slice of that capital cushion, essentially equity and reserves."],
    deposits:   ['Deposits', "Money customers have placed with the bank — its main source of funding, and its cheapest."],
    intexp:     ['Interest expended', "What a bank pays out on deposits and borrowings — its raw material cost."],

    /* ---------- shareholding ---------- */
    fii:        ['FII %', "<b>Foreign institutional investors</b> — overseas funds (FPIs). Their holding in the latest filed quarter, as a % of shares. FII buying is the single biggest swing factor for Indian large caps."],
    dii:        ['DII %', "<b>Domestic institutional investors</b> — Indian mutual funds, insurers, banks and pension funds. Often the buyers when FIIs sell."],
    fiichg:     ['FII Δpp', "Change in FII holding against the immediately preceding quarter, in percentage points. <code>&gt;0</code> = foreigners added. The Sep-2022 SEBI reclassification is skipped, since it moved stakes on paper without anyone buying."],
    diichg:     ['DII Δpp', "Change in DII holding against the preceding quarter, in percentage points — same rules as FII."],
    promoter:   ['Promoter %', "The founding family or parent company's stake. Rising promoter holding is generally read as confidence; steady selling is worth a look."],
    pledge:     ['Pledge %', "The share of the promoters' <i>own</i> holding pledged as collateral for a loan. High pledge is a risk: if the price falls the lender can sell those shares into the market."],
    publicsh:   ['Public &amp; others', "Everything not held by promoters or institutions — retail investors, HNIs, corporate bodies, trusts."],
    mfhold:     ['Mutual funds', "The slice of DII holding that belongs specifically to domestic mutual fund schemes."],
    nshare:     ['No. of shareholders', "How many individual accounts hold the stock, from the shareholding filing. A jump usually means retail piling in."],
    shp:        ['Shareholding pattern', "The quarterly filing in which every listed company discloses who owns it. Filed within ~21 days of quarter end, so it is always a little behind the market."],

    /* ---------- FII/DII flows ---------- */
    cashseg:    ['Cash segment', "Ordinary buying and selling of shares for delivery, as opposed to futures and options. The provisional daily figure is published by the exchanges each evening."],
    netflow:    ['Net (₹ cr)', "Purchases minus sales for the day. Positive = that group was a net buyer."],
    idxfut:     ['Index futures', "Futures on an index (mostly Nifty and Bank Nifty). FII positioning here is read as a directional bet on the market itself."],
    idxopt:     ['Index options', "Calls and puts on an index. Volumes dwarf every other segment, so turnover here says more about hedging activity than conviction."],
    stkfut:     ['Stock futures', "Futures on single stocks — the leveraged way to take a view on one company."],
    lsratio:    ['FII long/short ratio', "FII open long index-futures contracts ÷ their open shorts. Above 1 = net long. Extremes at either end have historically marked turning points."],
    proclient:  ['Pro / Client', "<b>Pro</b> = a broker trading its own money; <b>Client</b> = trades placed for customers. The exchange splits participant data this way alongside FII and DII."],

    /* ---------- delivery & volume ---------- */
    delivpct:   ['Delivery %', "The share of the day's traded volume that was actually taken into a demat account instead of being squared off intraday. High delivery means someone is buying to hold."],
    delivspike: ['Delivery spike', "A session where delivered quantity ran far above the stock's own recent normal — a footprint of unusually heavy accumulation."],
    usualpct:   ['Usual %', "The stock's own baseline delivery percentage over the prior 20 sessions. A spike is judged against this, not against a fixed number, because delivery is structurally high in some stocks and low in others."],
    multiple:   ['Multiple', "Delivered quantity that session ÷ the stock's median delivered quantity over the prior 20 sessions. <code>5x</code> = five times the normal amount of stock actually changed owner."],
    delivval:   ['Delivered (₹ cr)', "Delivered quantity × close, i.e. how much real money was put to work in that spike."],
    volratio:   ['Ratio vs avg', "The session's volume ÷ the stock's own 20-day average volume. A shock is a session far above its own normal, so a ₹200 cr stock can qualify as readily as a giant."],
    shocksess:  ['Shock session', "A day where volume broke well clear of the stock's own 20-day average. Volume precedes price more often than the reverse — a shock is worth explaining, not buying."],
    delivconf:  ['Delivery-confirmed', "A volume shock where the delivery percentage also held up, so the spike was accumulation rather than intraday churn."],
    bhavcopy:   ['Bhavcopy', "The end-of-day file the exchange publishes with every stock's open, high, low, close, volume and delivery. It is the source of record for all price data here."],

    /* ---------- deals ---------- */
    bulkdeal:   ['Bulk deal', "A trade (or a day's trades by one client) of more than 0.5% of a company's shares, done on the ordinary market. The exchange requires it to be disclosed the same evening."],
    blockdeal:  ['Block deal', "A single large negotiated trade — minimum ₹10 crore — executed in a separate early-morning window at a pre-agreed price, so it does not disturb the open market."],
    clientname: ['Client', "The party named in the disclosure — a fund, an HNI, a promoter entity. Following the same names across months is more informative than any single deal."],
    dealside:   ['Side', "<b>BUY</b> or <b>SELL</b>. A trade appears on both sides only when both parties crossed the disclosure threshold, so total buys need not equal total sells."],
    dealval:    ['Value (₹ cr)', "Quantity × the trade or weighted-average price disclosed with the deal."],
    netbought:  ['Net bought (₹ cr)', "That party's disclosed buys minus their disclosed sells over the window shown."],

    /* ---------- insider trades ---------- */
    pitreg:     ['SEBI PIT Reg 7(2)', "The rule that makes insiders disclose their own dealing in the company's shares within two trading days. These filings are the raw material of this page."],
    insider:    ['Insider', "Someone with access to unpublished price-sensitive information — promoters, directors, key management and their immediate relatives."],
    kmp:        ['KMP', "<b>Key managerial personnel</b> — the CEO, CFO, company secretary and equivalent officers."],
    mktbuy:     ['Mkt buy / Mkt sell', "A transaction done on the open market. This is the meaningful category: the insider chose the price and paid real money."],
    offmkt:     ['Off-mkt', "A transfer arranged off the exchange — typically a gift, inheritance or intra-family reshuffle. It moves shares without expressing any view on price."],
    esopI:      ['ESOP', "Shares received by exercising employee stock options. It is compensation being collected, not a purchase, so it says nothing about the insider's view."],
    pledgeI:    ['Pledge / Invoked / Revoke', "<b>Pledge</b> = shares put up as loan collateral. <b>Revoke</b> (or release) = the pledge lifted, usually on repayment. <b>Invoked</b> = the lender sold the pledged shares because the loan went bad — the one to worry about."],
    postpct:    ['Post %', "The insider's holding <i>after</i> the transaction, as a percentage of the company. It shows whether a sale was a trim or an exit."],

    /* ---------- IPOs & corporate actions ---------- */
    mainboard:  ['Mainboard', "A full-size IPO listed on the main NSE/BSE platform, open to all investors. Includes issues that start trading in the trade-for-trade (BE) series."],
    sme:        ['SME', "A small-company issue on the NSE Emerge / BSE SME platform. Minimum lot sizes run to ₹1 lakh+, liquidity is thin and disclosure is lighter."],
    issueprice: ['Issue price', "The price per share fixed in the IPO — the number every listing gain is measured from."],
    subx:       ['Subscription (×)', "Total bids ÷ shares offered so far. <code>4.2×</code> means the issue has been bid for 4.2 times over. Heavy subscription tells you about demand at the issue price, not about value."],
    sinceissue: ['Since issue %', "The stock's current price against its IPO issue price — how the listing has actually done for someone who was allotted."],
    exdate:     ['Ex-date', "The first day the stock trades <i>without</i> the entitlement. To receive a dividend, bonus or split you must already own the shares before this day opens — buying on the ex-date is too late."],
    recorddate: ['Record date', "The day the company freezes its register to see who owns the shares. The ex-date sits one trading day before it."],
    dividend:   ['Dividend', "Cash paid out per share from profits. Announced per share, so ₹12 on a ₹600 stock is a 2% yield."],
    divyield:   ['Yield %', "Announced dividend ÷ latest close, per share. Interim and final dividends may appear as separate rows rather than one combined yield."],
    bonus:      ['Bonus', "Free extra shares — 1:1 means one new share for every one held. The price halves on the ex-date, so nothing is created; it simply improves liquidity."],
    split:      ['Split', "The face value is cut and share count multiplied — a ₹10 share split 1:5 becomes five ₹2 shares. Like a bonus, the value is unchanged."],
    buyback:    ['Buyback', "The company purchases its own shares back, either from the open market or via a tender offer at a fixed price. It shrinks the share count and returns cash to holders."],
    rights:     ['Rights issue', "Existing holders are offered new shares at a discount in proportion to what they own. Ignore it and your stake is diluted."],
    opemoffer:  ['Open offer', "A mandatory offer to buy from public shareholders, triggered when an acquirer crosses a control threshold under the SEBI takeover code."],

    /* ---------- announcements & discovery ---------- */
    announce:   ['Corporate announcement', "Any filing a company makes to the exchange — results, orders, board changes, fund raises. Filings are the fastest public signal there is; price usually moves before any analyst writes about it."],
    orderwin:   ['Order win', "An announced new contract or work order. Read it against the company's annual revenue: a ₹50 cr order means everything to a ₹200 cr company and nothing to a ₹20,000 cr one."],
    fundraise:  ['Fund raising', "A board approval to raise money — QIP, preferential allotment, rights issue or debt. Dilution for existing holders, fuel for the business."],
    boardmeet:  ['Board meeting', "Notice of a board meeting and, afterwards, its outcome. The results calendar is built from these."],
    demerger:   ['Merger / Demerger', "Two companies combined, or a business spun out as a separately listed entity. Prices on this site are adjusted for demergers so the chart does not show a fake crash on the split day."],
    pricemove:  ['Price movement query', "The exchange asking a company to explain an unusual move. It confirms nothing by itself — it only tells you the move was large enough to be noticed."],
    trigger:    ['Trigger', "A concrete, dated event that put a stock in a bucket — an order win, a fund raise, a delivery spike, a results beat. Every bucket on this page is computed from public filings, never hand-picked."],
    smartmoney: ['Smart money', "Stocks where large disclosed participants — bulk and block deal buyers, institutions — have been active recently."],

    /* ---------- indices, sectors, breadth ---------- */
    indexlevel: ['Level', "The index value itself. The number only matters relative to its own history — the % change is the comparable figure."],
    nifty50:    ['Nifty 50', "The 50 largest, most liquid NSE companies, free-float market-cap weighted. India's headline benchmark."],
    nifty500:   ['Nifty 500', "The top 500 companies by market cap — around 90%+ of NSE's listed value. This site's default universe for screens and backtests."],
    idxfamily:  ['Nifty 100 / 200 / Next 50 / Midcap / Smallcap', "Slices of the same list by size: Next 50 = ranks 51-100, Midcap 150 = 101-250, Smallcap 250 = 251-500. A stock moves between them as it grows or shrinks."],
    sectoral:   ['Sectoral index', "An index of one sector's stocks (Bank, IT, Pharma, Auto…). Comparing sector indices shows where money is actually rotating."],
    thematic:   ['Thematic index', "An index built around a theme rather than a sector — consumption, infrastructure, PSU, manufacturing."],
    membership: ['Constituents', "The stocks in an index on a given date. This site keeps the membership history, so an old screen sees the index as it was then, not as it is now."],
    rebased:    ['Rebased to 100', "Every series is restated to start at 100 on the same day, so lines of very different sizes can be compared on one chart."],
    eqweight:   ['Equal-weighted index', "Every member counts the same, rather than being sized by market cap. It shows the typical stock's move instead of the heavyweights'."],
    rs1m:       ['RS · 1M', "One-month relative strength — the index's return minus the Nifty 50's over the same month. Positive = this group is leading."],
    advancing:  ['Advancing / Declining', "How many stocks in the group closed up versus down. A rise carried by a few names is far more fragile than a broad one."],
    above200:   ['Above 200-day avg', "The share of stocks trading above their own 200-day moving average — the cleanest single reading of how healthy the broad market is."],
    newhilo:    ['New 52-week highs / lows', "How many stocks made a fresh one-year high or low that day. Expanding new lows during an index rally is a classic warning."],
    volband:    ['Volatility band', "A plain-English reading of the India VIX level shown on the ticker: below 13 is calm, 13-20 moderate, above 20 means traders are paying up hard for protection."],
    vixband:    ['Calm → Extreme', "The VIX level put into words: <b>Calm</b> below 12 (complacent, small daily moves expected), <b>Normal</b> 12-16, <b>Nervous</b> 16-22 (event risk being priced in), <b>Fearful</b> 22-30, and above that the market is in outright stress. The ladder describes what options are pricing, not a forecast of direction."],
    vix:        ['India VIX', "The market's expected volatility over the next 30 days, implied by Nifty option prices and stated as an annualised percentage. It rises when traders pay up for protection, so it spikes in falls — hence 'the fear index'."],

    /* ---------- macro ---------- */
    cpi:        ['CPI inflation', "<b>Consumer price inflation</b>, year on year — the headline cost-of-living number the RBI targets (4% ±2%)."],
    gdp:        ['GDP growth', "The economy's real output growth, year on year, published quarterly."],
    repo:       ['Repo rate', "The rate at which the RBI lends to banks. It is the anchor for every other rate in the economy; cuts are generally good for equities and bonds."],
    gsec10:     ['10Y G-sec yield', "The yield on the 10-year government bond — the risk-free rate everything else is priced against. Rising yields make equities look dearer."],
    eygap:      ['Earnings yield − 10Y gap', "The Nifty's earnings yield (the inverse of its P/E) minus the 10-year bond yield. A negative gap means bonds pay more than equity earnings — historically an expensive market."],
    forex:      ['Forex reserves', "The RBI's stock of foreign currency, gold and SDRs. A deep reserve is what lets the RBI defend the rupee."],
    usdinr:     ['USD/INR', "Rupees per US dollar. A rising number means a weakening rupee, which helps exporters (IT, pharma) and hurts importers."],
    brent:      ['Brent crude', "The global oil benchmark, in dollars a barrel. India imports most of its crude, so a sustained rise feeds straight into inflation and the current account."],
    bankcredit: ['Bank credit growth', "Total outstanding bank loans, year on year — the pulse of borrowing across the economy. Published fortnightly by the RBI for scheduled commercial banks."],
    depgrowth:  ['Deposit growth', "The same year-on-year measure for deposits. When credit growth outruns deposit growth for long, banks must pay up for funds and margins compress."],
    ppchg:      ['Change (pp)', "Change in <b>percentage points</b> — the gap between two percentages. Growth going from 12% to 14% is a rise of 2 pp, not 2%."],
    pctile:     ['Position in history', "Where the current reading sits against its own full history — the top of the range, the bottom, or the middle. It turns a bare number into context."],

    /* ---------- global ---------- */
    usover:     ['US overnight', "How Wall Street closed while India slept. It sets the tone for the Indian open more reliably than any other single input."],
    asianow:    ['Asia now', "Asian markets trading in the same session as India, so they move alongside rather than ahead."],
    riskon:     ['Risk-on / Risk-off', "<b>Risk-on</b> = money moving into equities and commodities; <b>risk-off</b> = into the dollar, gold and bonds. Most global days sort cleanly into one or the other."],
    corr:       ['Correlation', "How closely two markets have moved together over the window: 1 = in lockstep, 0 = unrelated, −1 = opposite. It shows which global markets India is actually tracking right now."],
    dxy:        ['Dollar index (DXY)', "The dollar against a basket of major currencies. A strong dollar usually drains money out of emerging markets, India included."],

    /* ---------- mutual funds ---------- */
    nav:        ['NAV', "<b>Net asset value</b> — the per-unit value of a fund, struck once a day after markets close. Fund returns are NAV to NAV; there is no intraday price."],
    aum:        ['AUM', "<b>Assets under management</b> — the total money in the scheme. Size helps a debt fund and can hinder a small-cap one."],
    expratio:   ['Expense ratio', "The annual fee, taken out of NAV daily. It looks small and compounds hard: 1 pp of extra cost over 20 years eats roughly a fifth of the final corpus."],
    directreg:  ['Direct vs Regular', "<b>Direct</b> plans carry no distributor commission, so their expense ratio is lower and NAV compounds faster. <b>Regular</b> plans are the same portfolio bought through an intermediary."],
    sip:        ['SIP', "<b>Systematic investment plan</b> — a fixed amount invested every month. It removes the timing decision and averages the purchase price."],
    xirr:       ['XIRR', "The annualised return when money went in on many different dates, as with a SIP. A plain CAGR cannot handle staggered instalments; XIRR can."],
    elss:       ['ELSS', "<b>Equity-linked savings scheme</b> — an equity fund with an 80C tax deduction and a 3-year lock-in on every instalment."],
    flexicap:   ['Flexi cap / Focused / Contra', "Equity mandates: <b>flexi cap</b> may hold any size, <b>focused</b> is capped at ~30 stocks, <b>contra</b> deliberately buys what is out of favour."],
    hybrid:     ['Hybrid / Balanced Advantage', "Funds mixing equity and debt. <b>Balanced advantage</b> shifts the mix by valuation, aiming for a gentler ride than pure equity."],
    debtfund:   ['Debt fund categories', "Bond funds sorted by what they hold and for how long — corporate bond, banking &amp; PSU, dynamic bond, credit risk. Longer duration means more sensitivity to rate moves; credit risk means lower-rated paper and real default risk."],
    arbitrage:  ['Arbitrage', "A fund that pockets the gap between the cash and futures price of the same stock. Returns are debt-like, but it is taxed as equity."],
    etf:        ['ETF', "<b>Exchange-traded fund</b> — an index fund you buy and sell like a share, at live market prices through the day."],
    rolling:    ['Rolling returns', "Returns measured from every possible start date rather than one convenient one. It shows what a typical investor got, not what the luckiest one did."],

    /* ---------- owner / data-health pages ---------- */
    feedfresh:  ['Feed freshness', "How long ago each automated data feed last landed successfully. Green = on schedule, amber = late, red = the refresh is failing."],
    declared:   ['Declared', "Companies that have filed the quarter's results with the exchange so far this season."],
    filled:     ['Filled', "Results whose numbers have been parsed and stored on this site. Declared minus filled is the work still outstanding."],
    visionrun:  ['Vision run', "The scheduled job that reads results out of filed PDFs when no machine-readable XBRL exists."],
    xbrl:       ['XBRL', "The machine-readable format companies file results in. When it is present the numbers are exact; when it is missing they have to be read out of the PDF instead."],

    /* ---------- watchlist / personal ---------- */
    watchlist:  ['Watchlist', "Stocks you starred anywhere on the site, collected in one place with your own notes. Stored in your browser, synced across your devices."],

    /* ---------- dashboard screen ---------- */
    screenwin:  ['Screen period (From → To)', "The window whose return is used to <i>rank</i> stocks. The basket is bought on the To Date — nothing after it influences the pick."],
    holduntil:  ['Hold until', "The date the basket is sold. The return between the To Date and this date is the result the screen actually delivered."],
    allocated:  ['Allocated', "The rupees put into each name — equal-weighted, so the same amount into every stock in the basket."],

    /* ---------- strategy mixer ---------- */
    mixalloc:   ['Allocation ₹', "How much money the mix puts into a strategy. Each strategy runs on its own bucket, rebalancing its own stocks on its own schedule — money is never shifted between buckets mid-run."],
    stratcorr:  ['Correlation', "How similarly two strategies' monthly returns move: 1 = they swing together, 0 = unrelated, negative = one zigs when the other zags. Low correlation is what makes a mix smoother than its parts."],
    stratoverlap: ['Overlap', "How many stocks two strategies hold in common. High overlap means the mix is doubling the same bet under two names, not diversifying."],
    divbenefit: ['Smoother by (pp)', "How much shallower the mix's worst fall is than the allocation-weighted average of each strategy's own worst fall. Positive = the strategies' bad patches didn't all land at once."],
    posmonths:  ['Positive months', "The share of months the combined portfolio ended higher than it started — a feel for how often you'd have opened the app to good news."],
    mixvol:     ['Volatility %', "How much the mix's month-to-month returns swing, annualised. Lower = a steadier ride for the same destination."]
  };

  /* =======================================================================
   * PER-PAGE TERM LISTS — each page shows only what it actually uses.
   * ===================================================================== */
  var P = {

    'index.html': {
      sub: '(the numbers on the ticker)',
      secs: [
        ['Index ticker', ['indexlevel', 'daychg', 'nifty50', 'nifty500', 'vix']],
        ['The badges under each index', ['rsi|RSI (14), weekly', 'overbought', 'volband']]
      ],
      note: "Every page on the site ends with a glossary like this one, covering the terms on that page. For education and research only — nothing here is investment advice."
    },

    'movers.html': {
      sub: "(the columns on the movers table)",
      secs: [
        ['Columns', ['symbol', 'chgpct', 'close', 'mcap', 'sector', 'from52h', 'from52l', 'h52', 'l52']],
        ['Market summary', ['advancing', 'benchmark', 'nifty500']]
      ],
      note: "Moves are computed on adjusted closing prices, so a bonus or split does not show up as a crash. Figures are end-of-day, not live."
    },

    'indices.html': {
      sub: '(index terms and the table columns)',
      secs: [
        ['What an index is', ['indexlevel', 'membership', 'nifty50', 'nifty500', 'idxfamily', 'sectoral', 'thematic']],
        ['Columns', ['chgpct', 'rs1m', 'divyield|Div %', 'advancing', 'mcap']]
      ],
      note: "Constituents are point-in-time: the members shown are those the index actually held on the date, taken from our index-history dataset."
    },

    'monthly-returns.html': {
      sub: '(how to read the heatmap)',
      intro: ["Every cell is one index's <b>month-end to month-end</b> return. Green is positive, red negative, and the shade tracks the size of the move."],
      secs: [
        ['Row and column headings', ['ytd', 'fytd', 'annualised', 'cagr']],
        ['Summary columns', ['hitrate', 'benchmark', 'sectoral', 'thematic']]
      ],
      note: "FY rows run April → March, the Indian financial year. A month with no data is left blank rather than counted as zero."
    },

    'market-mood.html': {
      sub: '(breadth and participation measures)',
      secs: [
        ['Breadth', ['advancing', 'above200', 'newhilo', 'nifty500']],
        ['Participation', ['turnover', 'volume', 'delivpct']]
      ],
      note: "All measures are computed daily across the point-in-time Nifty 500. Broad participation confirms a trend; a rally on narrow breadth is fragile."
    },

    'fii-dii.html': {
      sub: '(cash and F&O participant flows)',
      secs: [
        ['Who is who', ['fii', 'dii', 'proclient']],
        ['Cash segment', ['cashseg', 'netflow']],
        ['F&O positions', ['idxfut', 'idxopt', 'stkfut', 'lsratio']]
      ],
      note: "Cash-segment figures are the exchanges' provisional evening numbers and are occasionally revised the next day. Flows describe what happened; they do not predict what happens next."
    },

    'shareholding.html': {
      sub: '(institutional holding per stock)',
      secs: [
        ['Holders', ['fii', 'dii', 'promoter', 'mfhold', 'publicsh']],
        ['Changes', ['fiichg|FII change (pp)', 'diichg|DII change (pp)', 'qoq', 'shp', 'pit']]
      ],
      note: "Numbers come from quarterly shareholding-pattern filings, which arrive within about 21 days of quarter end — so the latest quarter fills in gradually as companies file."
    },

    'deals.html': {
      sub: '(bulk & block deal disclosures)',
      secs: [
        ['Deal types', ['bulkdeal', 'blockdeal']],
        ['Columns', ['clientname', 'dealside', 'dealval', 'netbought', 'volume|Qty']]
      ],
      note: "Deal value = quantity × trade or weighted-average price. A trade appears on both sides only when both parties crossed the disclosure threshold, so total buys need not equal total sells."
    },

    'insider.html': {
      sub: '(SEBI insider-trading disclosures)',
      secs: [
        ['Who files', ['pitreg', 'insider', 'promoter', 'kmp']],
        ['Transaction modes', ['mktbuy', 'offmkt', 'esopI', 'pledgeI']],
        ['Columns', ['dealval|Value (₹ cr)', 'netbought', 'postpct', 'volume|Qty']]
      ],
      note: "Equity-share transactions only. Market buys and sells are the meaningful category — ESOPs, gifts and off-market transfers move shares without expressing a view on price."
    },

    'delivery.html': {
      sub: '(what makes a delivery spike)',
      intro: ["A <b>spike</b> is a session where the delivered quantity ran far above that stock's own recent normal. The baseline is the stock's median delivered quantity over the prior 20 sessions, so each stock is judged against itself."],
      secs: [
        ['Core measures', ['delivpct', 'usualpct', 'multiple', 'delivval', 'delivspike']],
        ['Columns', ['close', 'daychg', 'bhavcopy']]
      ],
      note: "Universe: NSE mainboard stocks with market cap ≥ ₹100 cr and at least 10 sessions of history — very recent listings therefore appear late. Heavy delivery says shares changed owner; it does not say the buyer was right."
    },

    'volume.html': {
      sub: '(what makes a volume shock)',
      secs: [
        ['Core measures', ['volratio', 'shocksess', 'delivconf', 'delivpct']],
        ['Columns', ['ltp', 'chgpct', 'turnover', 'volume', 'sector', 'bhavcopy']]
      ],
      note: "Every stock is measured against its own 20-day average, so a mid cap can qualify as readily as a giant. Volume precedes price more often than the reverse — a shock is worth explaining, not buying blind."
    },

    'ipos.html': {
      sub: '(IPO and listing terms)',
      secs: [
        ['Boards', ['mainboard', 'sme']],
        ['Columns', ['issueprice', 'subx', 'sinceissue', 'mcap', 'ltp']]
      ],
      note: "Subscription numbers update through the day while an issue is open. Heavy subscription measures demand at the issue price, not value. SME issues are thinly traded and lightly covered."
    },

    'actions.html': {
      sub: '(corporate actions and dates)',
      intro: ["To be eligible for any corporate action you must <b>buy at least one trading day before the ex-date</b>. Buying on the ex-date itself is too late."],
      secs: [
        ['Dates', ['exdate', 'recorddate']],
        ['Action types', ['dividend', 'divyield', 'bonus', 'split', 'buyback', 'rights']]
      ],
      note: "Actions are announced weeks ahead, so the far end of the calendar fills in as companies file. Interim and final dividends may appear as separate rows."
    },

    'announcements.html': {
      sub: '(filing categories)',
      secs: [
        ['What you are reading', ['announce', 'boardmeet', 'xbrl']],
        ['Common categories', ['orderwin', 'fundraise', 'demerger', 'buyback', 'bonus', 'split', 'recorddate', 'opemoffer', 'pricemove', 'pledge']]
      ],
      note: "A rolling ~31-day window of NSE equity announcements. Filings are the fastest public signal there is, but a headline is not a result — read the attached document before acting on it."
    },

    'discovery.html': {
      sub: '(how the buckets are built)',
      intro: ["Every bucket is a <b>screen computed from public filings</b> — announcements from the last ~31 days, quarterly results and daily prices. Nothing here is hand-picked, and nothing is a recommendation."],
      secs: [
        ['Bucket logic', ['trigger', 'orderwin', 'fundraise', 'smartmoney', 'delivspike']],
        ['Numbers shown', ['revyoy', 'patyoy', 'netprofit', 'pe|TTM P/E', 'mcap', 'subind', 'netbought', 'delivval']]
      ],
      note: "Results buckets use owners-attributable PAT on a consolidated basis wherever the company reports it, with both periods on the same basis."
    },

    'quarterly-results.html': {
      sub: '(the numbers and the market’s reaction)',
      secs: [
        ['Reported lines', ['revenue', 'opprofit', 'opm', 'opmd', 'netprofit']],
        ['Growth', ['revyoy', 'patyoy', 'yoy', 'qoq', 'ttm']],
        ['Reporting basis', ['consol', 'audited', 'filedate', 'pit']],
        ['Season view', ['benchmark', 'idxfamily', 'mcap']]
      ],
      note: "All figures in ₹ crore. YoY and QoQ are computed with both periods on the <i>same</i> reporting basis — mixing consolidated with standalone invents growth that never happened. When the year-ago quarter was a loss, growth is measured against its absolute value."
    },

    'sectors.html': {
      sub: '(how these indexes are built)',
      secs: [
        ['Index construction', ['eqweight', 'rebased', 'membership', 'sectoral']],
        ['Classification levels', ['sector', 'industry', 'subind']],
        ['Columns', ['chgpct', 'ytd', 'mcap', 'turnover', 'advancing']]
      ],
      note: "These are self-made, equal-weighted indexes rebased to 100 — they show how the typical stock in a group moved, not how its heavyweights did."
    },

    'macro.html': {
      sub: '(the indicators on this page)',
      secs: [
        ['Market & valuation', ['pe', 'eygap', 'vix', 'above200', 'advancing']],
        ['Rates & inflation', ['repo', 'gsec10', 'cpi']],
        ['Economy', ['gdp', 'bankcredit', 'depgrowth', 'forex']],
        ['Commodities & currency', ['brent', 'usdinr']],
        ['Flows', ['fii', 'dii', 'netflow', 'lsratio']],
        ['Reading the charts', ['pctile', 'ppchg', 'yoy']]
      ],
      note: "Each indicator is drawn against its own full history, so you see whether today's reading is normal or extreme rather than just what it is."
    },

    'global.html': {
      sub: '(global market terms)',
      secs: [
        ['Sessions', ['usover', 'asianow']],
        ['Reading the board', ['riskon', 'corr', 'chgpct', 'ytd', 'dxy', 'brent']]
      ],
      note: "India VIX and other volatility gauges are shown for context but kept out of the risk-on / risk-off aggregates, since they move opposite to the markets they measure."
    },

    'bank-credit.html': {
      sub: '(RBI bank loan growth)',
      secs: [
        ['The series', ['bankcredit', 'depgrowth', 'yoy', 'ppchg']],
        ['Columns', ['pctile|Range covered']]
      ],
      note: "Source: Reserve Bank of India, scheduled commercial banks, published fortnightly. Credit growth is a coincident indicator of the economy, not a leading one."
    },

    'mutual-funds.html': {
      sub: '(fund terms and return windows)',
      secs: [
        ['Fund basics', ['nav', 'aum', 'expratio', 'directreg', 'etf']],
        ['Returns', ['cagr', 'xirr', 'sip', 'rolling', 'ytd']],
        ['Equity categories', ['flexicap', 'elss', 'hybrid', 'arbitrage']],
        ['Debt categories', ['debtfund']]
      ],
      note: "Returns beyond one year are annualised; one year and under are absolute. Past returns describe the manager's history, not your future."
    },

    'nse-bse-dashboard.html': {
      sub: '(how the screen works)',
      intro: ["Pick the top-N stocks by their <b>From → To</b> return, buy them equal-weighted on the To Date, and sell on the Hold Until date. Only data up to the To Date decides the picks, so the result is what the rule would really have produced."],
      secs: [
        ['The screen', ['screenwin', 'holduntil', 'allocated', 'topn', 'equalwt']],
        ['Columns', ['close', 'chgpct', 'mcap', 'histmcap', 'from52h', 'industry', 'idxfamily']]
      ],
      note: "Prices are split- and bonus-adjusted, and delisted companies remain in history — so a good-looking screen is not an artefact of only counting survivors."
    },

    'stock.html': {
      sub: '(every number on this page)',
      secs: [
        ['Price & trend', ['closeraw', 'closeadj', 'h52', 'l52', 'from52h', 'from52l', 'dayshigh|Days since 52w high', 'dma|SMA / DMA', 'dma50|SMA 50 distance', 'dma200|SMA 200 distance', 'rs', 'beta', 'mdd6|Max drawdown (6m)', 'rsi', 'macd', 'bollb', 'turnfac|Avg turnover (20d)', 'delivpct']],
        ['Valuation', ['mcap', 'pe', 'pb', 'ps', 'eps']],
        ['Quarterly results', ['revenue', 'opprofit', 'opm', 'netprofit', 'pbt', 'othinc', 'excep', 'depr', 'interest', 'revyoy', 'patyoy', 'ttm', 'consol', 'audited', 'filedate']],
        ['Quality & balance sheet', ['roe', 'roce', 'roa', 'de', 'intcover', 'cfo', 'cfopat', 'capex', 'borrow', 'equitybs', 'ebit']],
        ['Banks & lenders', ['gnpa', 'nnpa', 'car', 'cet1', 'deposits', 'intexp']],
        ['Shareholding', ['promoter', 'fii', 'dii', 'mfhold', 'publicsh', 'pledge', 'nshare', 'shp']],
        ['Activity feeds', ['announce', 'bulkdeal', 'blockdeal', 'insider', 'exdate', 'bonus', 'split']]
      ],
      note: "Daily history runs from 1996 and is survivorship-free; earnings and shareholding are point-in-time, visible only from their real filing dates. Prices are split/bonus-adjusted — 'Last close' shows the raw traded price. Not investment advice."
    },

    'stock-backtest.html': {
      sub: '(every factor and result column)',
      intro: ["A strategy is one <b>rank-by</b> factor plus any <b>filters</b>, applied to a universe on each rebalance date. The engine only ever sees what was knowable on that date."],
      secs: [
        ['How a strategy is built', ['rankby', 'filters', 'rebalance', 'reset', 'ride', 'topn', 'equalwt', 'universe']],
        ['Why the results are trustworthy', ['pit', 'survfree', 'lookahead']],
        ['Result columns', ['cagr', 'totret', 'maxdd', 'winpct', 'benchmark', 'alpha']],
        ['Price & momentum factors', ['ret1m', 'ret3m', 'ret6m', 'ret12m', 'accel', 'riskmom', 'postdrift', 'composite']],
        ['Trend & price levels', ['d52', 'd52low', 'rangepos', 'dayshigh', 'dma50', 'dma200', 'indrank']],
        ['Risk', ['vol', 'beta', 'mdd6', 'uppct']],
        ['Liquidity & participation', ['turnfac', 'turnsurge', 'volsurge', 'delivfac']],
        ['Oscillators', ['rsi', 'macd', 'stoch', 'bollb']],
        ['Fundamentals — point-in-time', ['profyoy', 'profbase', 'profaccel', 'profttm', 'profstreak', 'consol', 'mcap', 'histmcap']],
        ['Ownership', ['fii|fiiPct', 'fiichg|fiiChgPp', 'dii|diiPct', 'diichg|diiChgPp']]
      ],
      note: "Lookbacks are counted in calendar days, so '90 days' is about 62 trading sessions. Brokerage, STT, slippage and taxes are <b>not</b> modelled, and a basket of a few stocks concentrates risk far more than the index it is measured against. For research and education only."
    },

    'strategy-backtest.html': {
      sub: '(every factor and result column)',
      secs: [
        ['How a strategy is built', ['rankby', 'filters', 'rebalance', 'reset', 'ride', 'topn', 'equalwt', 'universe']],
        ['Why the results are trustworthy', ['pit', 'survfree', 'lookahead']],
        ['Result columns', ['cagr', 'totret', 'maxdd', 'winpct', 'benchmark', 'alpha', 'finalval']],
        ['Holdings columns', ['close', 'daychg', 'mcap', 'histmcap', 'from52h|% 52wHi', 'from52l|% 52wLo', 'indrank|Ind rank', 'industry']],
        ['Price & momentum factors', ['ret1m', 'ret3m', 'ret6m', 'ret12m', 'accel', 'riskmom', 'postdrift', 'composite']],
        ['Trend, risk & oscillators', ['d52', 'dma50', 'dma200', 'vol', 'beta', 'mdd6', 'uppct', 'rsi', 'macd', 'stoch', 'bollb']],
        ['Liquidity & fundamentals', ['turnfac', 'turnsurge', 'volsurge', 'delivfac', 'profyoy', 'profbase', 'profaccel', 'profttm', 'profstreak']],
        ['Ownership', ['fii|FII %', 'fiichg', 'dii|DII %', 'diichg']]
      ],
      note: "Costs and taxes are not modelled. Rebalance periods can be monthly, quarterly, half-yearly or yearly — a longer period trades less and drifts further from the ranking."
    },

    'strategy-phases.html': {
      sub: '(rank-by factors, filters, columns)',
      intro: [
        "<b>Rank by</b> is written <code>direction-factor</code>. <code>high-ret6m</code> = sort every surviving stock by 6-month return, biggest first. <code>low-upPct</code> = sort by up-day consistency, <i>smallest</i> first. The top 3 or 5 of that sorted list is what the strategy buys.",
        "<b>Filters</b> are the entry conditions, all of them ANDed, applied <i>before</i> the ranking. <code>d52&lt;=25 &amp; fiiChgPp&gt;0</code> = keep only stocks within 25% of their 52-week high that also had FIIs adding last quarter, then rank whatever is left. A stock missing a factor's value is dropped, never ranked last.",
        "<b>Rebalance</b> happens on the last trading day of every month. <b>Monthly reset</b> = the whole basket is sold and re-bought equal-weight each month. <b>Ride winners (hold)</b> = only names that fell out of the top N are sold; survivors are kept untouched (so winners compound and weights drift), and the freed cash is split equally among the empty slots.",
        "<b>Universe</b> is point-in-time and survivorship-free: a stock is eligible on a date only if it was actually in the Nifty 500 (or in F&amp;O, for the F&amp;O variant) on that date, and delisted companies stay in history. Fundamentals and shareholding are point-in-time too — a quarter is only visible after its real filing date."
      ],
      secs: [
        ['Result columns', ['cagr', 'totret', 'maxdd', 'winpct', 'worstrank']],
        ['Price & momentum factors', ['ret1m', 'ret3m', 'ret6m', 'ret12m', 'accel', 'riskmom', 'postdrift', 'composite']],
        ['Trend & price levels', ['d52', 'd52low', 'rangepos', 'dayshigh', 'dma50', 'dma200', 'indrank']],
        ['Risk', ['vol', 'beta', 'mdd6', 'uppct']],
        ['Liquidity & participation', ['turnfac', 'turnsurge', 'volsurge', 'delivfac']],
        ['Oscillators', ['rsi', 'macd', 'stoch', 'bollb']],
        ['Fundamentals — point-in-time, consolidated', ['profyoy', 'profbase', 'profaccel', 'profttm', 'profstreak']],
        ['Ownership — quarterly shareholding filings', ['fii|fiiPct', 'fiichg|fiiChgPp', 'dii|diiPct', 'diichg|diiChgPp']]
      ],
      note: "Every lookback above is counted in calendar days, so '90 days' is about 62 trading sessions. Returns are computed on raw traded prices. Brokerage, STT, slippage and taxes are <b>not</b> modelled, and a basket of 3-5 stocks concentrates risk far more than the index it is measured against. With 44.4 lakh combinations tested, the very top rows are partly luck — that is exactly why the ⭐ 'best in ALL phases / ALL years' cards exist. This is an unlisted research page, not advice."
    },

    'saved-strategies.html': {
      sub: '(strategy and column terms)',
      secs: [
        ['Strategy anatomy', ['rankby', 'filters', 'rebalance', 'reset', 'ride', 'topn', 'universe']],
        ['Result columns', ['cagr', 'totret', 'maxdd', 'winpct', 'benchmark', 'alpha']],
        ['Holdings columns', ['ltp', 'daychg', 'mcap', 'histmcap', 'from52h|% 52wHi', 'from52l|% 52wLo', 'indrank|Industry rank', 'fii|FII holding %', 'fiichg|FII holding change QoQ (pp)', 'dii|DII holding %', 'diichg|DII holding change QoQ (pp)', 'profbase|Net Profit (year-ago Q)']],
        ['Trust', ['pit', 'survfree', 'livepicks']]
      ],
      note: "Strategies are ranked by survivorship-free CAGR. Two strategies with the same rules but a different number of holdings are different strategies, and are ranked separately."
    },

    'backtest-history.html': {
      sub: '(the columns in the run log)',
      secs: [
        ['Columns', ['cagr', 'maxdd', 'finalval', 'benchmark', 'rebalance']],
        ['Trust', ['pit', 'survfree']]
      ],
      note: "Every run here was produced by the same engine on the same survivorship-free history. Costs and taxes are not modelled."
    },

    'all-picks.html': {
      sub: '(what a pick is)',
      secs: [
        ['Picks', ['livepicks', 'rebalance', 'topn', 'universe', 'idxfamily']],
        ['Columns', ['ltp', 'daychg', 'chgpct', 'mcap']]
      ],
      note: "These are the stocks each saved strategy's rules qualify for right now, produced by the same code the backtests use. A stock appearing in many baskets means several unrelated rules like it — not that it is a recommendation."
    },

    'strategy-mixer.html': {
      sub: '(what mixing strategies measures)',
      secs: [
        ['The mix', ['mixalloc', 'divbenefit', 'stratcorr', 'stratoverlap', 'posmonths']],
        ['Result tiles', ['cagr', 'totret', 'maxdd', 'mixvol', 'benchmark', 'finalval']],
        ['Strategy anatomy', ['rebalance', 'topn', 'universe', 'winpct']],
        ['Trust', ['pit', 'survfree']]
      ],
      note: "Every strategy in the mix is simulated over the same dates by the same engine as its own backtest page, on the rupees you allocated. The mix is the sum of those buckets — no money moves between strategies, and costs and taxes are not modelled."
    },

    'live-tracking.html': {
      sub: '(forward-tracking terms)',
      intro: ["A backtest looks backwards; this looks forwards. Every day the picks each saved strategy would make are written down, then followed with real prices from that day on."],
      secs: [
        ['Tracking', ['livepicks', 'rebalance', 'benchmark', 'alpha']],
        ['Columns', ['ltp', 'chgpct', 'cagr']]
      ],
      note: "Tracking starts the day a strategy was first recorded, so a young strategy's numbers are noisy. No costs or taxes are modelled."
    },

    'watchlist.html': {
      sub: '(how the watchlist works)',
      secs: [
        ['Terms', ['watchlist', 'ltp', 'daychg']]
      ],
      note: "Starred stocks are stored in your browser and synced across your own devices — nothing here is shared publicly."
    },

    'index-chart.html': {
      sub: '(index and volatility terms)',
      secs: [
        ['Index', ['indexlevel', 'membership', 'cagr', 'maxdd', 'ytd', 'annualised']],
        ['India VIX', ['vix', 'vixband']]
      ],
      note: "Index history is stitched from exchange and vendor data; the further back you look, the more it depends on the vendor's own restatements."
    },

    'status.html': {
      sub: '(data-health terms)',
      secs: [
        ['Feeds', ['feedfresh', 'bhavcopy', 'xbrl', 'visionrun']]
      ]
    },

    'results-coverage.html': {
      sub: '(coverage terms)',
      secs: [
        ['Coverage', ['declared', 'filled', 'visionrun', 'xbrl', 'filedate', 'mcap']]
      ]
    },

    'analytics.html': {
      sub: '(page-stats terms)',
      secs: [
        ['Terms', ['feedfresh']]
      ]
    },

    'insurer-inbox.html': {
      sub: '(insurer filing terms)',
      secs: [
        ['Terms', ['xbrl', 'revenue', 'netprofit', 'filedate', 'consol']]
      ]
    },

    'backtest.html': {
      sub: '(mutual-fund backtest terms)',
      secs: [
        ['Fund basics', ['nav', 'directreg', 'elss', 'flexicap', 'hybrid', 'debtfund', 'arbitrage', 'etf']],
        ['Results', ['cagr', 'xirr', 'sip', 'totret', 'maxdd', 'rebalance']]
      ],
      note: "Backtests run on published NAV history, which is already net of the expense ratio. Exit loads and taxes are not modelled."
    }
  };

  return { t: T, p: P };
})();
