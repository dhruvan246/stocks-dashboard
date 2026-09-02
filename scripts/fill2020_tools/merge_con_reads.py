# -*- coding: utf-8 -*-
"""Merge shard ledgers written by read_con_pat_nse.py --reads <shard.json> into the shared
scripts/con_pat_nse_reads.json. Never overwrites an existing shared entry (a shard only ever holds
keys the shared ledger lacked when the shard started), and reports any collision instead.

  python3 scripts/fill2020_tools/merge_con_reads.py shard1.json shard2.json ...
"""
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
READS = os.path.join(os.path.dirname(HERE), "con_pat_nse_reads.json")


def main():
    shared = json.load(open(READS))
    added = coll = 0
    for p in sys.argv[1:]:
        if not os.path.exists(p):
            print("missing shard", p)
            continue
        for k, v in json.load(open(p)).items():
            if k in shared:
                coll += 1
                if shared[k] != v:
                    print("  COLLISION (kept shared):", k)
                continue
            shared[k] = v
            added += 1
    json.dump(shared, open(READS, "w"), indent=0, sort_keys=True)
    print("merged: %d added, %d already present -> %d entries" % (added, coll, len(shared)))


if __name__ == "__main__":
    main()
