# -*- coding: utf-8 -*-
"""NSE sector and theme indices that are NOT in the shared membership pipeline — current members only.

scripts/indices_history.json carries 28 indices with full point-in-time history (runbook §227). NSE Indices
publishes many more sector and theme indices (Financial Services, India Defence, Power, Chemicals, Commodities ...).
This script reads NSE's own CURRENT constituent file for each of them and writes

    docs/nse_extra_indices/members.json
      {"asOf": "<date of the last change>", "indices": {"<name>": {"file", "symbols", "since", "changes"}}}

Consumer: docs/sectors.html (Indices level). It is a CURRENT list, not a point-in-time history: `since` is the day
this script first captured the index, `changes` logs what it saw change from then on. Nothing here feeds a backtest.
To give one of these indices real history, move it into build_changelog.py / build_membership_v2.py instead.

File names were read from each index's own page on niftyindices.com on 2026-11-25 (they follow no pattern:
ind_niftyfinancelist, ind_niftyPower_list, ind_NiftyCement_list, ind_niftymidsmallfinancailservice_list ...).

Safety: a file that cannot be fetched, has fewer than 3 symbols, or lost more than half its members keeps the
previous entry. The output is written only when something changed.

Usage: python3 -X utf8 scripts/fetch_nse_extra_indices.py [--dry-run]
"""
import csv
import datetime as dt
import gzip
import json
import os
import sys
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
OUT = os.path.join(ROOT, "docs", "nse_extra_indices", "members.json")
UA = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120 Safari/537.36", "Accept": "*/*"}
BASES = ["https://nsearchives.nseindia.com/content/indices/", "https://www.niftyindices.com/IndexConstituent/"]

# index name (as in NSE's ind_close_all file) -> constituent file
FILES = {
    # sector indices
    "Nifty Financial Services": "ind_niftyfinancelist.csv",
    "Nifty Financial Services Ex-Bank": "ind_niftyfinancialservicesexbank_list.csv",
    "Nifty Private Bank": "ind_nifty_privatebanklist.csv",
    "Nifty NBFC": "ind_niftyNBFC_list.csv",
    "Nifty Housing Finance": "ind_niftyHousingFinance_list.csv",
    "Nifty Insurance": "ind_niftyInsurance_list.csv",
    "Nifty MidSmall Financial Services": "ind_niftymidsmallfinancailservice_list.csv",
    "Nifty Power": "ind_niftyPower_list.csv",
    "Nifty Chemicals": "ind_niftyChemicals_list.csv",
    "Nifty Capital Goods": "ind_niftyCapitalGoods_list.csv",
    "Nifty Cement": "ind_NiftyCement_list.csv",
    "Nifty Construction": "ind_niftyConstruction_list.csv",
    "Nifty Commercial & Transport Services": "ind_niftyCommercialTransportServices_list.csv",
    "Nifty Consumer Services": "ind_niftyConsumerServices_list.csv",
    "Nifty Retail": "ind_niftyRetail_list.csv",
    "Nifty Telecommunications": "ind_niftyTelecommunications_list.csv",
    "Nifty Hospitals": "ind_niftyHospitals_list.csv",
    "Nifty500 Healthcare": "ind_nifty500Healthcare_list.csv",
    "Nifty MidSmall Healthcare": "ind_niftymidsmallhealthcare_list.csv",
    "Nifty MidSmall IT & Telecom": "ind_niftymidsmallitAndtelecom_list.csv",
    "Nifty REITs & Realty": "ind_niftyREITsRealty_list.csv",
    # theme indices
    "Nifty India Defence": "ind_niftyindiadefence_list.csv",
    "Nifty Commodities": "ind_niftycommoditieslist.csv",
    "Nifty Infrastructure": "ind_niftyinfralist.csv",
    "Nifty India Infrastructure & Logistics": "ind_niftyIndiaInfrastructure_Logistics_list.csv",
    "Nifty CPSE": "ind_niftycpselist.csv",
    "Nifty PSE": "ind_niftypselist.csv",
    "Nifty India Railways PSU": "ind_niftyIndiaRailwaysPSU_list.csv",
    "Nifty Services Sector": "ind_niftyservicelist.csv",
    "Nifty India Consumption": "ind_niftyconsumptionlist.csv",
    "Nifty MidSmall India Consumption": "ind_niftymidsmallindiaconsumption_list.csv",
    "Nifty India New Age Consumption": "ind_niftyIndiaNewAgeConsumption_list.csv",
    "Nifty Non-Cyclical Consumer": "ind_niftynon-cyclicalconsumer_list.csv",
    "Nifty India Manufacturing": "ind_niftyindiamanufacturing_list.csv",
    "Nifty India Digital": "ind_niftyindiadigital_list.csv",
    "Nifty India Internet": "ind_niftyIndiaInternet_list.csv",
    "Nifty AI Catalysts": "ind_niftyAICatalystslist.csv",
    "Nifty Capital Markets": "ind_niftyCapitalMarkets_list.csv",
    "Nifty EV & New Age Automotive": "ind_niftyEv_NewAgeAutomotive_list.csv",
    "Nifty Mobility": "ind_niftymobility_list.csv",
    "Nifty Transportation & Logistics": "ind_niftytransportationandlogistics_list.csv",
    "Nifty India Tourism": "ind_niftyindiatourism_list.csv",
    "Nifty Housing": "ind_niftyhousing_list.csv",
    "Nifty Core Housing": "ind_niftyCoreHousing_list.csv",
    "Nifty Rural": "ind_niftyRural_list.csv",
    "Nifty Sugar & Ethanol": "ind_niftySugarEthanol_list.csv",
    "Nifty Waves": "ind_niftyWaves_list.csv",
    "Nifty IPO": "ind_niftyIPO_list.csv",
    "Nifty Conglomerate 50": "ind_niftyConglomerate50_list.csv",
    "Nifty Tata Group": "ind_nifty_tatalist.csv",
    "Nifty Aditya Birla Group": "ind_nifty_adityabirlalist.csv",
    "Nifty Mahindra Group": "ind_nifty_mahindralist.csv",
    "Nifty Select 5 Corporate Groups": "ind_niftyIndiaSelect5CorporateGroupsMAATR_list.csv",
}


def get(url, tries=3, timeout=45):
    """bytes, or None on a definite miss (404, or the HTML page niftyindices returns for a missing file)"""
    last = None
    for i in range(tries):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=timeout) as r:
                b = r.read()
            if b[:2] == b"\x1f\x8b":
                b = gzip.decompress(b)
            if b[:15].lstrip().lower().startswith((b"<!doctype", b"<html")):
                return None
            return b
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            last = e
        except Exception as e:
            last = e
        time.sleep(3 * (i + 1))
    print("  GET failed:", url, repr(last))
    return None


def members(fname):
    """sorted symbols from NSE's list file, or None if no base served a readable file"""
    for base in BASES:
        b = get(base + fname)
        if not b:
            continue
        rows = list(csv.reader(b.decode("utf-8-sig", "ignore").splitlines()))
        if not rows:
            continue
        head = [h.strip() for h in rows[0]]
        if "Symbol" not in head:
            continue
        si = head.index("Symbol")
        syms = sorted({r[si].strip().upper() for r in rows[1:] if len(r) > si and r[si].strip()})
        if syms:
            return syms
    return None


def main():
    dry = "--dry-run" in sys.argv
    today = (dt.datetime.utcnow() + dt.timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d")
    try:
        old = json.load(open(OUT, encoding="utf-8"))
    except Exception:
        old = {"indices": {}}
    prev = old.get("indices", {})
    out, changed, kept = {}, [], []
    for name, fname in FILES.items():
        p = prev.get(name)
        syms = members(fname)
        if syms is None or len(syms) < 3 or (p and len(syms) * 2 < len(p["symbols"])):
            got = "no file" if syms is None else "%d symbols" % len(syms)
            if p:
                out[name] = p
                kept.append("%s (%s — previous list kept)" % (name, got))
            else:
                kept.append("%s (%s — not added)" % (name, got))
            continue
        if not p:
            out[name] = {"file": fname, "symbols": syms, "since": today, "changes": []}
            changed.append("%s: new, %d members" % (name, len(syms)))
        elif syms != p["symbols"]:
            add = sorted(set(syms) - set(p["symbols"]))
            rem = sorted(set(p["symbols"]) - set(syms))
            out[name] = {"file": fname, "symbols": syms, "since": p.get("since", today),
                         "changes": p.get("changes", []) + [{"date": today, "added": add, "removed": rem}]}
            changed.append("%s: +%s -%s" % (name, ",".join(add) or "none", ",".join(rem) or "none"))
        else:
            out[name] = dict(p, file=fname)
    for line in changed:
        print("CHANGED", line)
    for line in kept:
        print("KEPT   ", line)
    print("%d indices, %d changed, %d kept/skipped" % (len(out), len(changed), len(kept)))
    if out == prev:
        print("unchanged — file not written")
        return
    if dry:
        print("(dry run — file not written)")
        return
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    doc = {"asOf": today, "source": "NSE Indices constituent files (current members; not point-in-time)", "indices": out}
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False, separators=(",", ":"))
        f.write("\n")
    print("wrote", OUT)


if __name__ == "__main__":
    main()
