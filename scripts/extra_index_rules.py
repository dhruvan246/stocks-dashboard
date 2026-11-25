# -*- coding: utf-8 -*-
"""Data for build_extra_index_history.py: heading aliases, archived file names, hand-read events.

Every entry here was READ in the notice or file it names; nothing is inferred from an index's name."""
import re
from fetch_nse_extra_indices import FILES

# normalised notice heading -> index (beyond the display name itself)
ALIASES = {
    "cnxfinance": "Nifty Financial Services",
    "cnxconsumption": "Nifty India Consumption",
    "cnxinfrastructure": "Nifty Infrastructure",
    "cnxinfra": "Nifty Infrastructure",
    "cnxservicesector": "Nifty Services Sector",
    "cnxservice": "Nifty Services Sector",
    "cnxcommodities": "Nifty Commodities",
    "cnxpse": "Nifty PSE",
    "niftyindiacorporategroupindextatagroup": "Nifty Tata Group",
    "niftyindiacorporategroupindexadityabirlagroup": "Nifty Aditya Birla Group",
    "niftyindiacorporategroupindexmahindragroup": "Nifty Mahindra Group",
    "niftyindiaselect5corporategroups": "Nifty Select 5 Corporate Groups",
}


def file_key(fname):
    k = re.sub(r"\.csv$", "", fname.lower())
    k = re.sub(r"^ind_", "", k)
    k = re.sub(r"[^a-z0-9]", "", k)
    return re.sub(r"list$", "", k)


# archived constituent file (normalised) -> index
FILE_INDEX = {file_key(f): n for n, f in FILES.items()}
FILE_INDEX.update({
    "cnxfinance": "Nifty Financial Services", "niftyfinancialservices": "Nifty Financial Services",
    "niftymidsmallfinancialservices": "Nifty MidSmall Financial Services",
    "niftycommercialandtransportservices": "Nifty Commercial & Transport Services",
    "niftyreitsandrealty": "Nifty REITs & Realty",
    "cnxcommodities": "Nifty Commodities",
    "cnxinfra": "Nifty Infrastructure", "niftyinfrastructure": "Nifty Infrastructure",
    "niftyindiainfrastructureandlogistics": "Nifty India Infrastructure & Logistics",
    "cpseindex": "Nifty CPSE", "cnxpse": "Nifty PSE",
    "cnxservice": "Nifty Services Sector", "niftyservicessector": "Nifty Services Sector",
    "cnxconsumption": "Nifty India Consumption", "niftyindiaconsumption": "Nifty India Consumption",
    "niftyevandnewageautomotive": "Nifty EV & New Age Automotive",
    "niftysugarandethanol": "Nifty Sugar & Ethanol",
    "niftytatagroup": "Nifty Tata Group", "niftyindiacorporategroupindextatagroup": "Nifty Tata Group",
    "niftyadityabirlagroup": "Nifty Aditya Birla Group", "niftyindiacorporategroupindexadityabirlagroup": "Nifty Aditya Birla Group",
    "niftymahindragroup": "Nifty Mahindra Group", "niftyindiacorporategroupindexmahindragroup": "Nifty Mahindra Group",
})


# Whole events the parser cannot read. (index, eff, [excluded], [included], src stem, what the notice says)
MANUAL_EVENTS = [
    # image-only notices, read from macOS Vision OCR of the PDF (2026-11-25)
    ("Nifty Tata Group", "2018-09-28", ["TTML"], [], "10092018",
     "5) NIFTY Tata Group: demerger scheme in Tata Teleservices (Maharashtra) - 'exclude TTML from NIFTY Tata Group effective "
     "from September 28, 2018 (close of September 27, 2018). No inclusion is being made'"),
    ("Nifty CPSE", "2018-10-26", ["CONCOR", "GAIL", "ENGINERSIN"], ["NBCC", "NLCINDIA", "NTPC", "SJVN"], "24092018",
     "Revision in criteria and replacements in NIFTY CPSE Index: 3 excluded, 4 included, 10 -> 11 stocks, "
     "'effective from October 26, 2018 (close of October 25, 2018)'"),
    # one-index notices whose lists sit under '2. Replacement of stocks:' (no index heading)
    ("Nifty CPSE", "2020-01-24", ["IOC", "PFC"], ["COCHINSHIP", "NHPC", "NMDC", "POWERGRID"], "17012020",
     "Revision in NIFTY CPSE Index: 2 excluded, 4 included, 10 -> 12, 'effective from January 24, 2020 (close of January 23, 2020)'"),
]

# DEMERGERS. NSE adds the demerged entity "with a dummy Symbol ... at zero price" to every index the parent is in
# (notice 1, an index table), and excludes it a few days after it lists (notice 2, the same table, real symbol).
# (dummy symbol, real symbol or None if not listed/named yet, inclusion stem, inclusion date, exclusion stem, exclusion date)
# Every row READ from both notices on 2026-11-25: the dummy symbol and date from notice 1, the listed symbol and
# "decided to exclude ... effective from" date from notice 2. The index list is read from each notice's own table.
DEMERGERS = [
    # ind_prs17072023 (Jio Financial Services, "dummy symbol DUMMYREL" until listing) / ind_prs05092023
    ("DUMMYREL", "JIOFIN", "17072023", "2023-07-20", "05092023", "2023-09-07"),   # paragraphs matched by DEMERGER_MATCH
    ("DUMMYSANOF", "SANOFICONR", "07062024_2", "2024-06-13", "19092024", "2024-09-23"),
    ("DUMMYRAYMD", "RAYMONDLSL", "05072024", "2024-07-11", "13092024", "2024-09-17"),
    ("DUMMYINFBM", "ODIGMA", "06092024", "2024-09-11", "17032025_1", "2025-03-19"),
    ("DUMMYSTAR", "ONESOURCE", "02122024_1", "2024-12-06", "05022025_1", "2025-02-07"),
    ("DUMMYITC", "ITCHOTELS", "30122024", "2025-01-06", "06022025_1", "2025-02-10"),
    ("DUMMYSIEMS", "ENRIN", "02042025", "2025-04-07", "25062025_2", "2025-06-27"),
    ("DUMMYSTLTE", "STLNETWORK", "21042025", "2025-04-24", "22092025", "2025-09-24"),
    ("DUMMYRAYMN", "RAYMONDREL", "09052025", "2025-05-14", "08072025", "2025-07-10"),
    ("DUMMYABFRL", "ABLBL", "19052025", "2025-05-22", "25062025_2", "2025-06-27"),
    ("DUMMYDBRLT", "ADVENTHTL", "15072025", "2025-07-18", "26112025", "2025-11-28"),
    ("DUMMYTATAM", "TMCV", "07102025", "2025-10-14", "13112025", "2025-11-17"),
    ("DUMMYSKFIN", "SKFINDUS", "10102025", "2025-10-15", "11122025_1", "2025-12-15"),
    ("DUMMYALCAR", "AGL", "07112025", "2025-11-12", "20072026", "2026-07-22"),
    ("DUMMYHDLVR", "KWIL", "28112025", "2025-12-05", "20022026_1", "2026-02-24"),
    # ind_prs23042026: VAML / VEDPOWER / VOGL / VISL "with dummy symbols DUMMYVEDL1, DUMMYVEDL2, DUMMYVEDL3 and DUMMYVEDL4 respectively"
    ("DUMMYVEDL1", "VAML", "23042026", "2026-04-30", "19062026", "2026-06-23"),
    ("DUMMYVEDL2", "VEDPOWER", "23042026", "2026-04-30", "17062026", "2026-06-19"),
    ("DUMMYVEDL3", "VOGL", "23042026", "2026-04-30", "22062026", "2026-06-24"),
    ("DUMMYVEDL4", "VISL", "23042026", "2026-04-30", "17062026", "2026-06-19"),
    ("DUMMYTRVN", "TRIVENIPT", "17072026", "2026-07-22", "28102026", "2026-10-30"),
    # ind_prs26082026_1: IGL Spirits (IGLSPIRITS) and Ennature Bio Pharma (ENNATURE) "with dummy symbols DUMMYINGL1 and DUMMYINGL2 respectively"
    ("DUMMYINGL1", "IGLSPIRITS", "26082026_1", "2026-09-02", "12112026_1", "2026-11-16"),
    ("DUMMYINGL2", "ENNATURE", "26082026_1", "2026-09-02", "13112026", "2026-11-17"),
    ("DUMMYINXGN", None, "28072026_1", "2026-07-31", None, None),      # no exclusion notice as of 2026-11-25
    ("DUMMYHEG", None, "03092026", "2026-09-07", None, None),          # no exclusion notice as of 2026-11-25
]
# notices that do not print the dummy symbol: the words (spaces removed) that identify the table's paragraph
DEMERGER_MATCH = {("DUMMYREL", "17072023"): "spunoffentityshallbeincludedinfollowingindices",
                  ("DUMMYREL", "05092023"): "decidedtoexcludeJIOFINfromvariousindices"}
DUMMY_REAL = {d: r for d, r, *_ in DEMERGERS if r}

# (index, eff, src or None, {drop from excluded}, {add to excluded}, {drop from included}, {add to included}, why)
MANUAL_FIXES = [
    # ind_prs15092025_1 item 9 "Revision in date of replacement": PEL's exclusion "along with corresponding inclusion has been
    # preponed" to 2025-09-23 (a: Fin Services Ex-Bank -PEL +360ONE; b: MidSmall Fin Services -PEL +INDUSINDBK); "All other
    # changes announced on August 22, 2025 will continue to be effective from September 30, 2025". So that leg leaves the 30 Sep event.
    ("Nifty Financial Services Ex-Bank", "2025-09-30", "22082025", {"PEL"}, [], {"360ONE"}, [], "preponed to 2025-09-23 by 15092025_1"),
    ("Nifty MidSmall Financial Services", "2025-09-30", "22082025", {"PEL"}, [], {"INDUSINDBK"}, [], "preponed to 2025-09-23 by 15092025_1"),
]

# ind_prs13052020 item 1: "Replacements in various indices effective March 27, 2020 announced vide press release dated
# February 18, March 12 and March 19, 2020 (except replacements in NIFTY 50 and NIFTY Bank index ...) shall stand null
# and void"; re-announced with fresh lists in ind_prs10062020 (effective 2020-06-26). None of the 53 is excepted.
COVID_NULL = ("18022020", "12032020", "19032020")


def index_table(txt, must_contain):
    """index names of the 'Sr. No. Index Name' table that follows the paragraph mentioning `must_contain`"""
    lines = [l.strip() for l in txt.splitlines() if l.strip()]
    para = []; rows = None; out = []
    for l in lines + ["<end>"]:
        if re.match(r"^Sr\.?\s*No\.?\s*Index Name\s*$", l, re.I):
            if rows is None:
                rows = []
            continue
        m = re.match(r"^(\d{1,2})\s+((?:Nifty|NIFTY).*)$", l)
        if rows is not None and m:
            rows.append(m.group(2).strip()); continue
        if rows is not None:
            out.append((" ".join(para), rows)); para = []; rows = None
        para.append(l)
    hit = [r for p, r in out if must_contain in re.sub(r"\s+", "", p)]
    if len(hit) != 1:
        return None
    return hit[0]


def post_process(changelog, revocations, rescheduled, bc, texts):
    names = set(FILES)
    for idx in names & set(changelog):
        changelog[idx] = [c for c in changelog[idx] if c["src"] not in COVID_NULL]
    for stem, old_eff, blocks in rescheduled:                      # same rule as build_changelog (runbook 141f)
        for b in blocks:
            for c in changelog.get(b["index"], []):
                if c["src"] == stem or c["eff"] != old_eff:
                    continue
                gone = [x for x in c["excluded"] if x in b["excluded"]] + [x for x in c["included"] if x in b["included"]]
                if gone:
                    c["excluded"] = [x for x in c["excluded"] if x not in b["excluded"]]
                    c["included"] = [x for x in c["included"] if x not in b["included"]]
                    print("  RESCHEDULED %s: %s %s moved %s -> %s (removed from %s)" % (stem, b["index"], gone, old_eff, b["eff"], c["src"]))
    for src, revs in sorted(revocations, key=lambda x: (x[0][4:8], x[0][2:4], x[0][:2])):
        mine = [r for r in revs if r[0] in names and src not in COVID_NULL]
        if mine:
            bc.apply_revocations(changelog, mine, src)
    for dummy, real, s_in, d_in, s_out, d_out in DEMERGERS:
        sym = real or dummy
        for stem, eff, side in ((s_in, d_in, "included"), (s_out, d_out, "excluded")):
            if not stem:
                continue
            rows = index_table(texts[stem], DEMERGER_MATCH.get((dummy, stem), dummy))
            if not rows:
                raise SystemExit("DEMERGER %s: no index table found in ind_prs%s" % (dummy, stem))
            for row in rows:
                idx = bc.canon_index(row)
                if idx in names:
                    changelog.setdefault(idx, []).append({"eff": eff, "excluded": [sym] if side == "excluded" else [],
                                                          "included": [sym] if side == "included" else [], "src": stem, "demerger": dummy})
    for idx, eff, src, rmx, adx, rmi, adi, why in MANUAL_FIXES:
        evs = [c for c in changelog.get(idx, []) if c["eff"] == eff and (src is None or c["src"] == src)]
        if not evs:
            print("  MANUAL FIX skipped: no %s event on %s" % (idx, eff)); continue
        for c in evs:
            c["excluded"] = [x for x in c["excluded"] if x not in rmx]
            c["included"] = [x for x in c["included"] if x not in rmi]
        for x in adx:
            if x not in evs[0]["excluded"]: evs[0]["excluded"].append(x)
        for x in adi:
            if x not in evs[0]["included"]: evs[0]["included"].append(x)
    for idx, eff, exc, inc, src, why in MANUAL_EVENTS:
        ev = changelog.setdefault(idx, [])
        if any(c["eff"] == eff and c["src"] == src for c in ev):
            print("  MANUAL EVENT not added, parser already has %s %s %s" % (idx, eff, src)); continue
        ev.append({"eff": eff, "excluded": list(exc), "included": list(inc), "src": src, "manual": True})
    for idx in list(changelog):
        changelog[idx] = [c for c in changelog[idx] if c["excluded"] or c["included"]]


# why an index ships without history, where the generic reason says too little (read 2026-11-25)
UNVERIFIED_NOTES = {
    "Nifty IPO": "NSE's notice of 24 Mar 2026 ('Replacements in Nifty IPO w.e.f. March 30, 2026', listed on niftyindices.com/press-release) "
                 "cannot be opened: the link returns an error page on niftyindices.com, nsearchives has no copy and it was never archived. "
                 "Without it the list before 30 Mar 2026 cannot be rebuilt (NSE's archived list of 13 Mar 2026 differs by 12 names).",
}
