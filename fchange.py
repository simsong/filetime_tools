#!/usr/bin/env python3
# coding=UTF-8
#
"""
File change detecton driver.
Uses scanner.py for scanning either the native file system or S3
Uses scandb.py  for the database that holds the scan results
This database mechanim is used to store data found by the scanner class
"""

__version__ = '0.0.1'
import os.path
import configparser

import scanner
import scandb
import ctools.dbfile as dbfile
import ctools.tydoc as tydoc


############################################################
############################################################

# Tools for extracting from the database

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Compute file changes',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    g = parser.add_mutually_exclusive_group(required=True)
    g.add_argument("--sqlite3db", help="Specify sqlite3db file")  
    g.add_argument("--config", help="Specify configuration for MySQL info file", default=None)

    g = parser.add_mutually_exclusive_group(required=True)
    g.add_argument("--create", help="Create a database", action='store_true')
    g.add_argument("--scans", help="List the scans in the DB", action='store_true')
    g.add_argument("--listroots", help="List all roots in the DB", action='store_true')  # initial root?
    g.add_argument("--report", help="Report what's changed between scans A and B (e.g. A-B)")
    g.add_argument("--jreport", help="Create 'what's changed?' json report", action='store_true')
    g.add_argument("--dump",   help='Dump the last scan in a standard form', action='store_true')
    g.add_argument("--reportdups", help="Report duplicates for most recent scan", action='store_true')
    g.add_argument("--addroot", help="Add a new root", type=str)
    g.add_argument("--delroot", help="Delete an existing root", type=str)
    g.add_argument("--scan", help="Initiate a scan", action='store_true')

    parser.add_argument("--dupsize", help="Don't report dups smaller than dupsize", default=1024 * 1024, type=int)
    parser.add_argument("--out", help="Specifies output filename")
    parser.add_argument("--vfiles", help="Report each file as ingested", action="store_true")
    parser.add_argument("--vdirs", help="Report each dir as ingested", action="store_true")
    parser.add_argument("--limit", help="Only search this many", type=int)
    parser.add_argument("--debug", help="Enable debugging", action='store_true')
    
    args = parser.parse_args()

    fchange = None
    auth = None

    # Mutually exclusive database choice
    if args.config:
        fcm = scandb.MySQLScanDatabase.FromConfigFile(args.config, debug=args.debug)
    elif args.sqlite3db:
        fcm = scandb.SQLite3ScanDatabase(fname=args.sqlite3db, debug=args.debug)

    # Mutually exclusive commands

    if args.create:
        fcm.create_database()
    if args.addroot:
        fcm.add_root(args.addroot)
        print("Added root: ", args.addroot)
    if args.delroot:
        fcm.del_root(args.delroot)
        print("Deleted root: ", args.delroot)
    if args.scans:
        for (scanid, rootdir, time) in fcm.get_scans():
            print(scanid, rootdir, time)
    if args.report:
        m = re.search(r"(\d+)-(\d+)", args.report)
        if not m:
            print("Usage: --report N-M")
            exit(1)
        fcm.report(int(m.group(1)), int(m.group(2)))
    if args.jreport:
        fcm.jreport()
    if args.reportdups:
        fcm.report_dups(fcm.last_scan())
    if args.scan:
        fcm.scan_enabled_roots()
            
