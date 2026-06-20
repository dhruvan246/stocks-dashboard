# Insurer quarterly net-profit extraction — PLAYBOOK

**Why this exists:** the daily cron (`update_fundamentals.py`) parses NSE's standard XBRL P&L
(`xbrl_profit`), which expects "Revenue from operations" / "Profit for the period". **Insurers file
IRDAI-format results** (Policyholders' **Revenue A/c** + Shareholders' **Profit & Loss A/c**, with
"Premium earned" / "Income from investments") — the XBRL parser can't read them, so insurers get
**neither std nor con** from the cron and must be filled by this manual method. Any time an insurer
(or a new insurance IPO) shows a fundamentals gap, follow this.

## The insurers (NSE symbols)
LICI, SBILIFE, HDFCLIFE, ICICIPRULI, ICICIGI, GICRE, NIACL, STARHEALTH, GODIGIT, NIVABUPA, MFSL.
(Life: LICI/SBILIFE/HDFCLIFE/ICICIPRULI. General: ICICIGI/GICRE/NIACL/STARHEALTH/GODIGIT/NIVABUPA.)

## 1. Fetch the CONSOLIDATED filing
- Tool: `fetch_ins.py` (BSE, keeps the PDF that has a consolidated insurer P&L) OR `fetch_nse.py`
  with a `[[SYM,qe],...]` list (NSE; prime cookies then `/api/corporate-announcements`).
- A filing qualifies if a page has: `consolidated` + a PAT row + an insurer term
  (premium/policyholder/shareholder/income from investment) + ≥8 decimal numbers.

## 2. Find the RIGHT page and the RIGHT row (this is where mistakes happen)
The filing has multiple sections; read the **consolidated Shareholders' Profit & Loss A/c**:
- **Life insurers**: "Statement of Consolidated Audited Results" → **SHAREHOLDERS' A/C** block →
  rows: `Profit before tax` → `Provision for tax` → **`Profit after tax`** (= the value you want).
  (HDFCLIFE worked on this page; ICICIPRULI/LICI/SBILIFE same.)
- **General insurers** (GICRE/NIACL/ICICIGI): after the Policyholders' **Revenue A/c** (operating
  results, ends at "Operating Profit transferred to P&L"), the **P&L A/c** has
  `Profit before tax` → `tax` → **`Profit after tax`**.
- **DO NOT use**: "Operating Profit/(loss)" (that's just the Revenue A/c), "Profit/(Loss) carried to
  Balance Sheet" (that's *accumulated* retained earnings — for GICRE the year column was ₹34,915cr,
  obviously cumulative), segment-wise results, or the analytical-ratios page ("Net profit margin").

## 3. Basis: con vs std (owner-attributable)
- **No-subsidiary insurers** (ICICIGI=ICICI Lombard, SBILIFE, STARHEALTH, GODIGIT, NIVABUPA,
  ICICIPRULI mostly): consolidated == standalone → store the same value for both (`[qe,v,a,v,a]`).
- **Insurers WITH subsidiaries/associates/minority** (LICI=IDBI associate, GICRE, NIACL,
  HDFCLIFE=pension/intl subs): con ≠ std. Use **owner-attributable**:
  `owner-con = Profit-after-tax(total) + minority-interest(signed) + share-of-associate`,
  or read the explicit "**attributable to: Owners of the Company**" line if present.
  (NIACL was corrected this way earlier: owner-con = line28 PAT + line29 minority(signed) + line30 associate, all ÷100.)

## 4. Unit — disambiguate by plausible magnitude
Filings are usually **₹ Lakhs (÷100 → crore)** or **₹ Crore**; occasionally **₹ Million (÷10)**.
Pick the unit whose result falls in the company's typical quarterly-PAT range (Cr):

| Insurer | range (Cr) | | Insurer | range (Cr) |
|---|---|---|---|---|
| LICI | 300–20000 | | HDFCLIFE | 100–700 |
| GICRE | 15–4000 | | SBILIFE | 40–1500 |
| NIACL | 50–2000 | | ICICIGI | 40–1500 |
| ICICIPRULI | 100–1000 | | STARHEALTH | 15–800 |
| GODIGIT | 50–300 | | NIVABUPA | −150–300 |

## 5. Verify before storing (never guess — wrong insurer values is what this whole audit was about)
Confirm the reading at least one way:
- **Year-ago column** in the filing == our stored same-quarter-last-year con (exact).
- **9M = Q1+Q2+Q3**, or **FY = Q1+Q2+Q3+Q4** reconciliation.
- **Profit before tax − tax = profit after tax** (the arithmetic ties out on the page).
- Press releases often state PAT explicitly ("PAT for Qx = Rs Y cr vs Rs Z").

## Worked example (2026-06-21)
**HDFCLIFE Q4FY26 (20260331)**: consolidated Shareholders' A/c, "Profit after tax" = **49,749 lakh
= ₹497.49 cr**. Verified: PBT 489.70 − tax credit = 497.49; FY26 column = 1,912.32 cr;
Q4FY25 comparative = 475.36 (matches stored). Stored `[20260331, std, ann, 497.49, ann]`.

## Apply
Fill-only into `docs/sf_fundamentals.json` **and** `scripts/fundamentals.json`
(`json.dump(d, open(p,'w'), separators=(',',':'))`); set con (index 3) and con-date (index 4);
for no-sub insurers set std (index 1) = con too. Commit + push.

Prior tooling: `fetch_ins.py`, `build_insurer_fund.py` (magnitude-range + vision OVR corrections),
`deepread_nse.py` (renders scanned pages). See also memory `project-stocks-pending-queue`.
