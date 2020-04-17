#!/usr/bin/env python3
# coding=UTF-8
#
"""
delete_dups:
This program takes a json dups file (created by fchange) and deletes all of the dups but one.
The decision of which one to keep is made with the keep() function, which takes a list of names
and returns the keeper.  It's expected that this function will be customized for every usage, so
we have two modes of operation: a --dry-run mode and a --delete mode.

It would be really need to implement this with a machine learning algorithm. Next time.
"""

import os
import json


def keep(dups):
    "This is my hand-coded keeper function."""
    if len(dups)==2:
        for n in [0,1]:
            if "xxx" in dups[n]['dirname'].lower():
                return dups[1-n]
            if "/Volumes/SanDiskSSD" in dups[n]['dirname'] and "/Volumes/SanDiskSSD" not in dups[1-n]['dirname']:
                return dups[1-n]
            if "Auckland Domain" in dups[n]['dirname']:
                return dups[n]

    # keep the largest one
    largest = max([dup['size'] for dup in dups])
    dups = [dup for dup in dups if dup['size']==largest]

    # keep the one with the shortest directory
    shortest = min([len(dup['dirname']) for dup in dups])
    dups = [dup for dup in dups if len(dup['dirname'])==shortest]

    # Same size and directory is same length; go with the shortest filename
    shortest = min([len(dup['filename']) for dup in dups])
    dups = [dup for dup in dups if len(dup['filename'])==shortest]

    # Take the oldest
    oldest = min([dup['mtime'] for dup in dups])
    dups = [dup for dup in dups if dup['mtime']==oldest]

    # Take the first lexographic filename
    first = min([dup['dirname'] for dup in dups])
    dups = [dup for dup in dups if dup['dirname']==first]

    # If there is only one left, keep it
    if len(dups)==1:
        return dups[0]

    print("Can't decide:")
    print("\n".join([str(dup) for dup in dups]))
    exit(1)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='delete the dups',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    g = parser.add_mutually_exclusive_group(required=True)
    g.add_argument("--delete", help="actually delete", action='store_true')  
    g.add_argument("--dry-run", help="don't actually delete", action='store_true')
    parser.add_argument("jsonfile", help="file created by fchange")

    args = parser.parse_args()

    for dups in json.load( open(args.jsonfile,"r")):
        which = keep(dups)
        if args.delete:
            os.unlink(which)
        else:
            for dup in dups:
                if dup==which:
                    print("keep ",dup['dirname'],dup['filename'])
                else:
                    print("     ",dup['dirname'],dup['filename'])
            print("\n")
