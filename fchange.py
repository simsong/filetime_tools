#!/usr/bin/env python3
# coding=UTF-8
#
# File change detector

__version__ = '0.0.1'
import datetime
import json
import os
import os.path
import re
import sqlite3
import sys

from dbfile import DBFile, SLGSQL
from scanner import Scanner

CACHE_SIZE = 2000000
SQL_SET_CACHE = "PRAGMA cache_size = {};".format(CACHE_SIZE)

# Replace this with an ORM?
SQL_SCHEMA = \
    """
CREATE TABLE IF NOT EXISTS metadata (key VARCHAR(255) PRIMARY KEY,value VARCHAR(255) NOT NULL);

CREATE TABLE IF NOT EXISTS files (fileid INTEGER PRIMARY KEY,
                                  pathid INTEGER NOT NULL,
                                  mtime INTEGER NOT NULL, 
                                  size INTEGER NOT NULL, 
                                  hashid INTEGER NOT NULL, 
                                  scanid INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS files_idx0 ON files(fileid);
CREATE INDEX IF NOT EXISTS files_idx1 ON files(pathid);
CREATE INDEX IF NOT EXISTS files_idx2 ON files(mtime);
CREATE INDEX IF NOT EXISTS files_idx3 ON files(size);
CREATE INDEX IF NOT EXISTS files_idx4 ON files(hashid);
CREATE INDEX IF NOT EXISTS files_idx5 ON files(scanid);
CREATE INDEX IF NOT EXISTS files_idx6 ON files(scanid,hashid);

CREATE TABLE IF NOT EXISTS paths (pathid INTEGER PRIMARY KEY,dirnameid INTEGER NOT NULL, filenameid INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS paths_idx1 ON paths(pathid);
CREATE INDEX IF NOT EXISTS paths_idx2 ON paths(dirnameid);
CREATE INDEX IF NOT EXISTS paths_idx3 ON paths(filenameid);

CREATE TABLE IF NOT EXISTS dirnames (dirnameid INTEGER PRIMARY KEY,dirname TEXT NOT NULL UNIQUE);
CREATE INDEX IF NOT EXISTS dirnames_idx1 ON dirnames(dirnameid);
CREATE INDEX IF NOT EXISTS dirnames_idx2 ON dirnames(dirname);

CREATE TABLE IF NOT EXISTS filenames (filenameid INTEGER PRIMARY KEY,filename TEXT NOT NULL UNIQUE);
CREATE INDEX IF NOT EXISTS filenames_idx1 ON filenames(filenameid);
CREATE INDEX IF NOT EXISTS filenames_idx2 ON filenames(filename);

CREATE TABLE IF NOT EXISTS hashes (hashid INTEGER PRIMARY KEY,hash TEXT NOT NULL UNIQUE);
CREATE INDEX IF NOT EXISTS hashes_idx1 ON hashes(hashid);
CREATE INDEX IF NOT EXISTS hashes_idx2 ON hashes(hash);

CREATE TABLE IF NOT EXISTS scans (scanid INTEGER PRIMARY KEY,time DATETIME NOT NULL UNIQUE,duration INTEGER);
CREATE INDEX IF NOT EXISTS scans_idx1 ON scans(scanid);
CREATE INDEX IF NOT EXISTS scans_idx2 ON scans(time);
"""

"""Explanation of tables:
files         - list of all files
hashes       - table of all hash code
"""


#################### END SQL PACKAGE ####################
#########################################################


############################################################
############################################################

# Tools for extracting from the database

def scans(conn):
    c = conn.cursor()
    for (scanid, time) in c.execute("SELECT scanid,time FROM scans"):
        print(scanid, time)


def last_scan(conn):
    return SLGSQL.execselect(conn, "SELECT MAX(scanid) FROM scans", ())[0]


def get_all_files(conn, scan1):
    """Files in scan scan1"""
    c = conn.cursor()
    c.execute("SELECT pathid, dirnameid, dirname, filenameid, filename, fileid, mtime, size "
              "FROM files NATURAL JOIN paths NATURAL JOIN dirnames NATURAL JOIN filenames "
              "WHERE scanid=?", (scan1,))
    return (DBFile(f) for f in c)


def get_new_files(conn, scan0, scan1):
    """Files in scan scan1 that are not in scan scan0"""
    c = conn.cursor()
    c.execute("SELECT fileid, pathid, dirnameid, dirname, filenameid, filename, mtime, size "
              "FROM files NATURAL JOIN paths NATURAL JOIN dirnames NATURAL JOIN filenames "
              "WHERE scanid=? AND pathid NOT IN (SELECT pathid FROM files WHERE scanid=?)", (scan1, scan0))
    return (DBFile(f) for f in c)


def deleted_files(conn, scan0, scan1):
    """Files in scan scan1 that are not in scan0"""
    return get_new_files(conn, scan1, scan0)


def changed_files(conn, scan0, scan1):
    """Files that were changed between scan0 and scan1"""
    c = conn.cursor()
    c.execute("SELECT a.pathid as pathid, a.hashid, b.hashid FROM " +
              "( SELECT pathid, hashid, scanid FROM files WHERE scanid=?) AS 'a' " +
              "JOIN (SELECT pathid, hashid, scanid FROM FILES WHERE scanid=?) as 'b' " +
              "ON a.pathid=b.pathid WHERE a.hashid != b.hashid",
              (scan0, scan1))
    return (DBFile(f) for f in c)


def get_duplicate_files(conn, scan0):
    """Return a generator for the duplicate files at scan0.
    Returns a list of a list of File objects, sorted by size"""
    c = conn.cursor()
    d = conn.cursor()
    c.execute('SELECT hashid, ct, size FROM '
              '(SELECT hashid, count(*) AS ct, size FROM files WHERE scanid=? GROUP BY hashid) '
              'WHERE ct>1 AND size>? ORDER BY 3 DESC;', (scan0, args.dupsize))
    for (hashid, ct, size) in c:
        ret = []
        d.execute(
            "SELECT fileid,pathid,size,dirnameid,dirname,filenameid,filename,mtime "
            "FROM files NATURAL JOIN paths NATURAL JOIN dirnames NATURAL JOIN filenames "
            "WHERE scanid=? AND hashid=?", (scan0, hashid,))
        yield [DBFile(f) for f in d]


def renamed_files(conn, scan1, scan2):
    """Return a generator for the duplicate files at scan0.
    The generator returns pairs of (F_old,F_new)
    """

    def get_singletons(conn, scanid):
        c = conn.cursor()
        c.execute('SELECT hashID, pathid, count(*) AS ct '
                  'FROM files WHERE scanid=? GROUP BY hashid HAVING ct=1', (scanid,))
        return c

    pairs_in_scan1 = set()
    path1_for_hash = {}
    for (hashID, path1, count) in get_singletons(conn, scan1):
        pairs_in_scan1.add((hashID, path1))
        path1_for_hash[hashID] = path1

    for (hashID, path2, count) in get_singletons(conn, scan2):
        if hashID in path1_for_hash and (hashID, path2) not in pairs_in_scan1:
            yield (DBFile({"hashid": hashID, "pathid": path1_for_hash[hashID]}),
                   DBFile({"hashid": hashID, "pathid": path2}))


def report(conn, a, b):
    atime = SLGSQL.execselect(conn, "SELECT time FROM scans WHERE scanid=?", (a,))[0]
    btime = SLGSQL.execselect(conn, "SELECT time FROM scans WHERE scanid=?", (b,))[0]
    print("Report from {}->{}".format(atime, btime))

    print("\n")
    print("New files:")
    for f in get_new_files(conn, a, b):
        print(f.dirname + f.filename)

    print("\n")
    print("Changed files:")
    for f in changed_files(conn, a, b):
        print(f.get_path(conn))

    print("\n")
    print("Deleted files:")
    for f in deleted_files(conn, a, b):
        print(f.get_path(conn))

    print("Renamed files:")
    for pair in renamed_files(conn, a, b):
        print("{} => {}".format(pair[0].get_path(conn), pair[0].get_path(conn)))

    print("Duplicate files:")
    for dups in get_duplicate_files(conn, b):
        print("Filesize: {:,}  Count: {}".format(dups[0].size, len(dups)))
        for dup in dups:
            print("    {}".format(dup.get_path(conn)))
    print("\n-----------")


def report_dups(conn, b):
    duplicate_bytes = 0
    for dups in get_duplicate_files(conn, b):
        if dups[0].size > args.dupsize:
            print("Filesize: {:,}  Count: {}".format(dups[0].size, len(dups)))
            for dup in dups:
                print("    {}".format(dup.get_path(conn)))
            print()
            duplicate_bytes += dups[0].size * (len(dups) - 1)
    print("\n-----------")
    print("Total space duplicated by files larger than {:,}: {:,}".format(args.dupsize, duplicate_bytes))


def jreport(conn):
    from collections import defaultdict

    dirnameids = {}
    filenameids = {}
    fileids = {}  # map of fileids.

    last = last_scan(conn)

    def backfill(f):
        if (f.fileid == None or f.dirnameid == None or f.filenameid == None
            or f.size == None or f.mtime == None):
            print("f:", f)
            exit(1)
        if f.fileid not in fileids:
            fileids[f.fileid] = (f.dirnameid, f.filenameid, f.size, f.mtime)
        if f.dirnameid not in dirnameids:
            dirnameids[f.dirnameid] = f.get_dirname(conn)
        if f.filenameid not in filenameids:
            filenameids[f.filenameid] = f.get_filename(conn)

    print("all_files")
    all_files = defaultdict(list)
    for f in get_all_files(conn, last):
        all_files[f.filename].append(f.fileid)
        backfill(f)

    print("new_files")
    new_files = defaultdict(list)
    for f in get_new_files(conn, last - 1, last):
        new_files[f.filename].append(f.fileid)
        backfill(f)

    print("duplicate files")
    duplicate_files = []
    for dups in get_duplicate_files(conn, last):
        duplicate_files.append([f.fileid for f in dups])
        for f in dups:
            backfill(f)

    data = {"dirnameids": dirnameids,
            "filenameids": filenameids,
            "fileids": fileids,
            "all_files": all_files,
            "new_files": new_files}
    # with (open(args.out,"w") if args.out else sys.stdout) as fp:
    #    json.dump(data,fp)
    # Make the JavaScript file
    with (open(args.out, "w") if args.out else sys.stdout) as fp:
        def dump_dictionary(fp, name, val):
            fp.write("var " + name + " = {};\n")
            for key in val:
                fp.write('{}[{}] = {};\n'.format(name, json.dumps(key), json.dumps(val[key])))

        dump_dictionary(fp, "all_files", all_files)
        dump_dictionary(fp, "new_files", new_files)
        dump_dictionary(fp, "duplicate_files", new_files)
        dump_dictionary(fp, "dirnameids", dirnameids)
        dump_dictionary(fp, "filenameids", filenameids)
        dump_dictionary(fp, "fileids", fileids)


def get_root(conn):
    return SLGSQL.execselect(conn, "SELECT value FROM metadata WHERE key='root'")[0]


def create_database(name, root):
    if os.path.exists(name):
        print("file exists: {}".format(name))
        exit(1)
    conn = sqlite3.connect(name)
    SLGSQL.create_schema(SQL_SCHEMA, conn)
    conn.cursor().execute("INSERT INTO metadata (key, value) VALUES (?,?)", ("root", root))
    conn.commit()
    conn.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Compute file changes',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--create", help="Create a database for a given ROOT")
    parser.add_argument("--db", help="Specify database location", default="data.sqlite3")
    parser.add_argument("--scans", help="List the scans in the DB", action='store_true')
    parser.add_argument("--root", help="List the root in the DB", action='store_true')
    parser.add_argument("--report", help="Report what's changed between scans A and B (e.g. A-B)")
    parser.add_argument("--jreport", help="Create 'what's changed?' json report", action='store_true')
    parser.add_argument("--dups", help="Report duplicates for most recent scan", action='store_true')
    parser.add_argument("--dupsize", help="Don't report dups smaller than dupsize", default=1024 * 1024, type=int)
    parser.add_argument("--out", help="Specifies output filename")
    parser.add_argument("--vfiles", help="Report each file as ingested", action="store_true")
    parser.add_argument("--vdirs", help="Report each dir as ingested", action="store_true")

    args = parser.parse_args()

    if args.create:
        create_database(args.db, args.create)
        print("Created {}  root: {}".format(args.db, args.create))

    if args.scans:
        scans(sqlite3.connect(args.db))
        exit(0)

    # open database and give me a big cache
    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    conn.cursor().execute(SQL_SET_CACHE)

    if args.root:
        c = conn.cursor()
        print("Root: {}".format(get_root(conn)))
        exit(0)

    if args.report:
        m = re.search("(\d+)-(\d+)", args.report)
        if not m:
            print("Usage: --report N-M")
            exit(1)
        report(conn, int(m.group(1)), int(m.group(2)))

    if args.jreport:
        jreport(conn)

    if args.dups:
        report_dups(conn, last_scan(conn))

    root = get_root(conn)
    print("Scanning: {}".format(root))
    Scanner(conn).ingest(root)
