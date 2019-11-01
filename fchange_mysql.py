#!/usr/bin/env python3
# coding=UTF-8
#
# File change detector

__version__ = '0.0.1'
import datetime
import json
import os
import os.path
import pymysql.cursors
import re
import sqlite3
import sys
import warnings


# TODO: Update dbfile_mysql classes to be MySQL friendly.
from dbfile_mysql import DBFile_MySQL, SLGSQL_MySQL
import scanner

CACHE_SIZE = 2000000
SQL_SET_CACHE = "PRAGMA cache_size = {};".format(CACHE_SIZE)

# Replace this with an ORM?
SQL_SCHEMA = \
    """
DROP TABLE IF EXISTS `filetime_tools`.metadata;
CREATE TABLE  `filetime_tools`.metadata (name VARCHAR(255) PRIMARY KEY, value VARCHAR(255) NOT NULL);

DROP TABLE IF EXISTS `filetime_tools`.dirnames;
CREATE TABLE  `filetime_tools`.dirnames (dirnameid INTEGER PRIMARY KEY AUTO_INCREMENT,dirname TEXT(65536));
CREATE UNIQUE INDEX  dirnames_idx2 ON `filetime_tools`.dirnames (dirname(700));

DROP TABLE IF EXISTS `filetime_tools`.filenames;
CREATE TABLE  `filetime_tools`.filenames (filenameid INTEGER PRIMARY KEY AUTO_INCREMENT,filename TEXT(65536));
CREATE INDEX  filenames_idx2 ON `filetime_tools`.filenames (filename(700));

DROP TABLE IF EXISTS `filetime_tools`.paths;
CREATE TABLE  `filetime_tools`.paths (pathid INTEGER PRIMARY KEY AUTO_INCREMENT,
       dirnameid INTEGER REFERENCES `filetime_tools`.dirnames(dirnameid),
       filenameid INTEGER REFERENCES `filetime_tools`.filenames(filenameid));
CREATE INDEX  paths_idx2 ON `filetime_tools`.paths(dirnameid);
CREATE INDEX  paths_idx3 ON `filetime_tools`.paths(filenameid);

DROP TABLE IF EXISTS `filetime_tools`.hashes;
CREATE TABLE  `filetime_tools`.hashes (hashid INTEGER PRIMARY KEY AUTO_INCREMENT,hash TEXT(65536) NOT NULL);
CREATE INDEX  hashes_idx2 ON `filetime_tools`.hashes( hash(700));

DROP TABLE IF EXISTS `filetime_tools`.scans;
CREATE TABLE  `filetime_tools`.scans (scanid INTEGER PRIMARY KEY AUTO_INCREMENT,time DATETIME NOT NULL UNIQUE,duration INTEGER);
CREATE INDEX  scans_idx1 ON `filetime_tools`.scans(scanid);
CREATE INDEX  scans_idx2 ON `filetime_tools`.scans(time);

DROP TABLE IF EXISTS `filetime_tools`.roots;
CREATE TABLE  `filetime_tools`.roots (rootid INTEGER PRIMARY KEY AUTO_INCREMENT,
       scanid INT REFERENCES `filetime_tools`.scans(scanid),
       dirnameid INT REFERENCES `filetime_tools`.dirnames(dirnameid));
CREATE INDEX  roots_idx1 ON `filetime_tools`.roots(rootid);
CREATE INDEX  roots_idx2 ON `filetime_tools`.roots(scanid);
CREATE INDEX  roots_idx3 ON `filetime_tools`.roots(dirnameid);

CREATE TABLE  `filetime_tools`.files (fileid INTEGER PRIMARY KEY AUTO_INCREMENT,
                                  pathid INTEGER REFERENCES paths(pathid),
                                  mtime INTEGER NOT NULL, 
                                  size INTEGER NOT NULL, 
                                  hashid INTEGER REFERENCES hashes(hashid), 
                                  scanid INTEGER REFERENCES scans(scanid));
CREATE INDEX  files_idx1 ON `filetime_tools`.files(pathid);
CREATE INDEX  files_idx2 ON `filetime_tools`.files(mtime);
CREATE INDEX  files_idx3 ON `filetime_tools`.files(size);
CREATE INDEX  files_idx4 ON `filetime_tools`.files(hashid);
CREATE INDEX  files_idx5 ON `filetime_tools`.files(scanid);
CREATE INDEX  files_idx6 ON `filetime_tools`.files(scanid,hashid);
"""

"""Explanation of tables:
files         - list of all files
hashes       - table of all hash code
"""


############################################################
############################################################

# Tools for extracting from the database

def list_scans(conn, dbname):
    c = conn.cursor()
    c.execute("SELECT scanid,time FROM `{}`.scans".format(dbname))
    results = c.fetchall()
    for result in results:
        print(result[0], result[1])


def last_scan(conn, dbname):
    return SLGSQL_MySQL.execselect(conn, "SELECT MAX(scanid) FROM `{}`.scans".format(dbname), ())[0]


def get_all_files(conn, scan1, dbname):
    """Files in scan scan1"""
    with conn.cursor() as cursor:
        cursor.execute("SELECT fileid, pathid,  size, dirnameid, dirname, filenameid, filename, mtime "
              "FROM `{}`.files NATURAL JOIN paths NATURAL JOIN dirnames NATURAL JOIN filenames "
              "WHERE scanid={}".format(dbname, scan1))
        return (DBFile_MySQL(f) for f in cursor)


def get_new_files(conn, scan0, scan1, dbname):
    """Files in scan scan1 that are not in scan scan0"""
    with conn.cursor() as cursor:
        cursor.execute("SELECT fileid, pathid, size,  dirnameid, dirname, filenameid, filename, mtime "
              "FROM `{}`.files NATURAL JOIN paths NATURAL JOIN dirnames NATURAL JOIN filenames "
              "WHERE scanid={} AND pathid NOT IN (SELECT pathid FROM files WHERE scanid={})".format(dbname, scan1, scan0))
        return (DBFile_MySQL(f) for f in cursor.fetchall())


def deleted_files(conn, scan0, scan1, dbname):
    """Files in scan scan1 that are not in scan0"""
    return get_new_files(conn, scan1, scan0, dbname)


def changed_files(conn, scan0, scan1, dbname):
    """Files that were changed between scan0 and scan1"""
    with conn.cursor() as cursor:
        cursor.execute('SELECT a.pathid as pathid, a.hashid, b.hashid FROM '
              '( SELECT pathid, hashid, scanid FROM `{dbname}`.files WHERE scanid={scan0}) AS a '
              'JOIN (SELECT pathid, hashid, scanid FROM `{dbname}`.files WHERE scanid={scan1}) as b '
              'ON a.pathid=b.pathid WHERE a.hashid != b.hashid'.format(dbname=dbname, scan0=scan0, scan1=scan1))
        return (DBFile_MySQL(f) for f in cursor.fetchall())


def get_duplicate_files(conn, scan0, dbname):
    """Return a generator for the duplicate files at scan0.
    Returns a list of a list of File objects, sorted by size"""
    c = conn.cursor()
    d = conn.cursor()
    c.execute('SELECT hashid, ct, size FROM '
              '(SELECT hashid, count(hashid) AS ct, size FROM `{dbname}`.files WHERE scanid={scanid} GROUP BY hashid, size) as T '
              'WHERE ct>1 AND size>{dupsize} ORDER BY 3 DESC;'.format(dbname=dbname, scanid=scan0, dupsize=args.dupsize))
    cresult = c.fetchall()
    for (hashid, ct, size) in cresult:
        ret = []
        d.execute(
            "SELECT fileid,pathid,size,dirnameid,dirname,filenameid,filename,mtime "
            "FROM `{dbname}`.files NATURAL JOIN paths NATURAL JOIN dirnames NATURAL JOIN filenames "
            "WHERE scanid={scanid} AND hashid={hashid}".format(dbname=dbname, scanid=scan0, hashid=hashid))

        yield [DBFile_MySQL(f) for f in d.fetchall()]


def renamed_files(conn, scan1, scan2, dbname):
    """Return a generator for the duplicate files at scan0.
    The generator returns pairs of (F_old,F_new)
    """

    def get_singletons(conn, scanid):
        with conn.cursor() as cursor:
            cursor.execute('SELECT hashID, pathid, count(*) AS ct '
                  'FROM `{}`.files WHERE scanid={} GROUP BY hashid, pathid HAVING ct=1'.format(dbname, scanid))
            return cursor

    pairs_in_scan1 = set()
    path1_for_hash = {}
    for (hashID, path1, count) in get_singletons(conn, scan1):
        pairs_in_scan1.add((hashID, path1))
        path1_for_hash[hashID] = path1

    for (hashID, path2, count) in get_singletons(conn, scan2):
        if hashID in path1_for_hash and (hashID, path2) not in pairs_in_scan1:
            yield (DBFile_MySQL({"hashid": hashID, "pathid": path1_for_hash[hashID]}),
                   DBFile_MySQL({"hashid": hashID, "pathid": path2}))


def report(conn, a , b, dbname):
    atime = SLGSQL_MySQL.execselect(conn, "SELECT time FROM `{}`.scans WHERE scanid={}".format(dbname, a))
    btime = SLGSQL_MySQL.execselect(conn, "SELECT time FROM `{}`.scans WHERE scanid={}".format(dbname, b))
    print("Report from {}->{}".format(atime, btime))

    print("\n")
    print("New files:")
    for f in get_new_files(conn, a, b, dbname):
        print(f.dirname + f.filename)

    print("\n")
    print("Changed files:")
    for f in changed_files(conn, a, b, dbname):
        print(f.get_path(conn))

    print("\n")
    print("Deleted files:")
    for f in deleted_files(conn, a, b, dbname):
        print(f.get_path(conn))

    print("Renamed files:")
    for pair in renamed_files(conn, a, b, dbname):
        print("{} => {}".format(pair[0].get_path(conn), pair[0].get_path(conn)))

    print("Duplicate files:")
    for dups in get_duplicate_files(conn, b, dbname):
        print("Filesize: {:,}  Count: {}".format(dups[0].size, len(dups)))
        for dup in dups:
            print("    {}".format(dup.get_path(conn)))
    print("\n-----------")


def report_dups(conn,scan0, dbname):
    import codecs
    with codecs.open(args.out,mode="w",encoding='utf8mb4') as out:
        out.write("Duplicate files:\n")
        total_wasted = 0
        for dups in get_duplicate_files(conn, scan0, dbname):
            out.write("Filesize: {:,}  Count: {}\n".format(dups[0].size, len(dups)))
            for dup in dups:
                out.write("   , " + os.path.join(dup.dirname,dup.filename) + "\n")
            total_wasted += dups[0].size * (len(dups)-1)
        out.write("-----------\n")
        out.write("Total wasted space: {}MB".format(total_wasted/1000000))


def report_dups(conn,b, dbname):
    duplicate_bytes = 0
    dups = get_duplicate_files(conn, b, dbname)
    for dup in dups:
        if dup[0].size > args.dupsize:
            print("Filesize: {:,}  Count: {}".format(dup[0].size, len(dup)))
            for d in dup:
                print("    {}".format(d.get_path(conn, dbname)))
            print()
            duplicate_bytes += dup[0].size * (len(dup) - 1)
    print("\n-----------")
    print("Total space duplicated by files larger than {:,}: {:,}".format(args.dupsize, duplicate_bytes))

def jreport(conn, dbname):
    from collections import defaultdict

    dirnameids = {}
    filenameids = {}
    fileids = {}  # map of fileids.

    last = last_scan(conn, dbname)

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
    for f in get_all_files(conn, last, dbname):
        all_files[f.filename].append(f.fileid)
        backfill(f)

    print("new_files")
    new_files = defaultdict(list)
    for f in get_new_files(conn, last - 1, last, dbname):
        new_files[f.filename].append(f.fileid)
        backfill(f)

    print("duplicate files")
    duplicate_files = []
    for dups in get_duplicate_files(conn, last, dbname):
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
            for name in val:
                fp.write('{}[{}] = {};\n'.format(name, json.dumps(name), json.dumps(val[name])))

        dump_dictionary(fp, "all_files", all_files)
        dump_dictionary(fp, "new_files", new_files)
        dump_dictionary(fp, "duplicate_files", new_files)
        dump_dictionary(fp, "dirnameids", dirnameids)
        dump_dictionary(fp, "filenameids", filenameids)
        dump_dictionary(fp, "fileids", fileids)


def get_root(conn, dbname):
    # return SLGSQL.execselect(conn, "SELECT value FROM metadata WHERE key='root'")[0]
    with conn.cursor() as cursor:
        cursor.execute("SELECT value FROM `{}`.metadata WHERE name='root'".format(dbname))
    return cursor.fetchone()[0]


def create_database(conn, name, root):
    with conn.cursor() as cursor:
        cursor.execute("SHOW DATABASES LIKE '{}';".format(name))
    if cursor.rowcount is not 0:
        print("db exists: {}".format(name))
        exit(1)
    
    conn = pymysql.connect(user='root')
    
    with conn.cursor() as cursor:
        cursor.execute("CREATE DATABASE `{}`;".format(name))
        conn.commit()
    try:
        SLGSQL_MySQL.create_schema(SQL_SCHEMA, conn, name)
    except pymysql.err.InternalError as e:
        pass
    
    conn.commit()
    
    with conn.cursor() as cursor:
        cursor.execute("INSERT INTO `{}`.metadata (name, value) VALUES ('{}','{}')".format(name, "root", root))
        conn.commit()
        conn.close()

if __name__ == "__main__":
    warnings.filterwarnings('ignore', category=pymysql.Warning)

    import argparse

    parser = argparse.ArgumentParser(description='Compute file changes',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--create", help="Create a database for a given ROOT")
    parser.add_argument("--db", help="Specify database location", default="data")
    parser.add_argument("--scans", help="List the scans in the DB", action='store_true')
    parser.add_argument("--root", help="List the root in the DB", action='store_true')
    parser.add_argument("--report", help="Report what's changed between scans A and B (e.g. A-B)")
    parser.add_argument("--jreport", help="Create 'what's changed?' json report", action='store_true')
    parser.add_argument("--dups", help="Report duplicates for most recent scan", action='store_true')
    parser.add_argument("--dupsize", help="Don't report dups smaller than dupsize", default=1024 * 1024, type=int)
    parser.add_argument("--out", help="Specifies output filename")
    parser.add_argument("--vfiles", help="Report each file as ingested",action="store_true")
    parser.add_argument("--vdirs", help="Report each dir as ingested",action="store_true")
    parser.add_argument("--limit", help="Only search this many", type=int)
    args = parser.parse_args()

    conn = None

    try:
        conn = pymysql.connect(user='root', password='')
    except Exception:
        exit(1)

    if args.create:
        create_database(conn, args.db, args.create)
        print("Created {}  root: {}".format(args.db, args.create))

    if args.scans:
        # list_scans(sqlite3.connect(args.db))
        list_scans(pymysql.connect(user='root', password='', host='localhost', database=args.db), args.db)
        
        exit(0)

    # open database and give me a big cache
    # conn = sqlite3.connect(args.db)
    # conn.row_factory = sqlite3.Row
    # conn.cursor().execute(SQL_SET_CACHE)

    conn = pymysql.connect(user='root', password='', host='localhost', database=args.db)

    if args.root:
        c = conn.cursor()
        print("Root: {}".format(get_root(conn, args.db)))
        exit(0)

    if args.report:
        m = re.search("(\d+)-(\d+)", args.report)
        if not m:
            print("Usage: --report N-M")
            exit(1)
        report(conn, int(m.group(1)), int(m.group(2)), args.db)
    elif args.jreport:
        jreport(conn, args.db)
    elif args.dups:
        report_dups(conn, last_scan(conn, args.db), args.db)
    else:
        root = get_root(conn, args.db)
        print("Scanning: {}".format(root))
        if root.startswith("s3://"):
            sc = scanner.S3Scanner(conn, args)
        else:
            sc = scanner.MySQLScanner(conn, args)
        sc.ingest(root)
    conn.close()
