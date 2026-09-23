# OCR sidecars for image-only NSE Indices press releases

Some niftyindices press releases draw their text as images (no text layer), so `build_changelog.parse_pdf`
reads nothing from them. For those, `parse_pdf` falls back to `scripts/_pr_ocr/<stem>.txt` (runbook §141d).
CI parses the same committed text, so it never needs OCR itself.

## 23082021.txt — ind_prs23082021 (Aug 23, 2021; semi-annual review effective Sep 30, 2021)
29 pages, ~90 image draws per page, zero text. Produced 2026-09-23 with `ocr.swift` (macOS Vision,
accurate mode, language correction OFF, pages rendered at 3x):

    swiftc -O ocr.swift -o ocr && ./ocr ind_prs23082021.pdf > 23082021.ocr.txt

then normalised: a table row that already ends in a valid ticker but whose serial the OCR dropped gets the
serial restored, and two OCR'd tickers absent from the symbol universe were corrected where the printed
company name identifies them. Every change:

    line 136: serial restored -> '22 Tata Steel Long Products Ltd. TATASTLLP'
    line 263: serial restored -> '8 Equitas Small Finance Bank Ltd. EQUITASBNK'
    line 275: serial restored -> '20 Kalyan Jewellers India Ltd. KALYANKJIL'
    line 379: serial restored -> '7 IDBI Bank Ltd. IDBI'
    line 399: serial restored -> '3 APL Apollo Tubes Ltd. APLAPOLLO'
    line 400: serial restored -> '4 Blue Dart Express Ltd. BLUEDART'
    line 505: serial restored -> '8 Indian Railway Finance Corporation Ltd. IRFC'
    line 511: ticker VAIBHAVGBLR -> VAIBHAVGBL (Vaibhav Global Ltd.)
    line 532: serial restored -> '12 Gulf Oil Lubricants India Ltd. GULFOILLUB'
    line 689: serial restored -> '1 Radico Khaitan Ltd RADICO'
    line 693: serial restored -> '1 Abbott India Ltd. ABBOTINDIA'
    line 710: serial restored -> '1 Oracle Financial Services Software Ltd. OFSS'
    line 713: serial restored -> '1 L&T Technology Services Ltd. LTTS'
    line 760: serial restored -> '1 City Union Bank Ltd. CUB'
    line 770: serial restored -> '2 Embassy Office Parks REIT EMBASSY'
    line 775: serial restored -> '1 Gulf Oil Lubricants India Ltd. GULFOILLUB'
    line 778: serial restored -> '1 Mangalore Refinery & Petrochemicals Ltd. MRPLR'
    line 778: ticker MRPLR -> MRPL (Mangalore Refinery & Petrochemicals Ltd.)
    line 795: serial restored -> '1 Hindustan Petroleum Corporation Ltd. HINDPETRO'
    line 798: serial restored -> '1 Adani Transmission Ltd. ADANITRANS'
    line 802: serial restored -> '1 Exide Industries Ltd. EXIDEIND'
    line 822: serial restored -> '1 Petronet LNG Ltd. PETRONET'
    line 825: serial restored -> '1 Apollo Hospitals Enterprise Ltd. APOLLOHOSP'
    line 829: serial restored -> '1 ICICI Bank Ltd. ICICIBANK'
    line 851: serial restored -> '1 Indian Hotels Co. Ltd. INDHOTEL'
    44 changes

Validation: every section's inclusions equal its exclusions (Pharma +10/-0 is its documented 10 -> 20
expansion); every ticker is in the sf-bin / rename-map / symchg universe except the REITs (BIRET,
EMBASSY, MINDSPACE, IRBINVIT), whose inclusion ind_prs15092021 revoked; rebuilding with it moves the
walk-vs-archived-list agreement of Nifty 100 55 -> 2, Midcap 50 118 -> 8, Pharma 72 -> 0, FMCG / Media /
Metal / Next 50 -> 0. The Nifty 500 / Midcap 150 / Smallcap 250 / 50 / 100 / LargeMidcap 250 /
MidSmallcap 400 / Realty lists in this notice are SUPERSEDED by ind_prs15092021 (build_changelog.SUPERSEDED).
