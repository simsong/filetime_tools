#!/usr/bin/env python3
# coding=UTF-8
#
# File change detector

__version__ = '0.0.1'
import os.path, sys
import os, re, sqlite3
import datetime

CACHE_SIZE = 2000000

# Replace this with an ORM?
schema = \
    """
CREATE TABLE IF NOT EXISTS files (fileid INTEGER PRIMARY KEY,
                                  pathid INTEGER NOT NULL,
                                  mtime TIMET NOT NULL, 
                                  size INTEGER NOT NULL, 
                                  hashid INTEGER NOT NULL, 
                                  scanid INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS files_idx0 ON files(fileid);
CREATE INDEX IF NOT EXISTS files_idx1 ON files(pathid);
CREATE INDEX IF NOT EXISTS files_idx2 ON files(mtime);
CREATE INDEX IF NOT EXISTS files_idx3 ON files(size);
CREATE INDEX IF NOT EXISTS files_idx4 ON files(hashid);
CREATE INDEX IF NOT EXISTS files_idx5 ON files(scanid);

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

"""Explaination of tables:
files         - list of all files
hashes       - table of all hash code
"""

COMMIT_RATE = 10  # commit every 10 directories


def create_schema(conn):
    # If the schema doesn't exist, create it
    c = conn.cursor()
    for line in schema.split(";"):
        print(line, end="")
        c.execute(line)


def iso_now():
    return datetime.datetime.now().isoformat()[0:19]


def execselect(conn, sql, vals):
    """Execute a SQL query and return the first line"""
    c = conn.cursor()
    c.execute(sql, vals)
    return c.fetchone()


#################### END SQL PACKAGE ####################
#########################################################


def hash_file(path):
    """High performance file hasher"""
    from hashlib import md5
    m = md5()
    with open(path, "rb") as f:
        while True:
            buf = f.read(65535)
            if buf:
                m.update(buf)
            else:
                break
    return m.hexdigest()


#
# Methods that return File(pathid,dirname,filename) for searches
#
class File:
    __slots__ = ['pathid', 'dirnameid', 'dirname', 'filenameid', 'filename', 'size']

    def __init__(self, pathid=None, dirnameid=None, dirname=None, filenameid=None, filename=None, size=None):
        self.pathid = pathid
        self.dirnameid = dirnameid
        self.dirname = dirname
        self.filenameid = filenameid
        self.filename = filename
        self.size = size


############################################################
class Scanner(object):
    def __init__(self, conn):
        self.conn = conn
        self.c = self.conn.cursor()

    def get_hashid(self, hash):
        self.c.execute("INSERT or IGNORE INTO hashes (hash) VALUES (?);", (hash,))
        self.c.execute("SELECT hashid FROM hashes WHERE hash=?", (hash,))
        return self.c.fetchone()[0]

    def get_scanid(self, now):
        self.c.execute("INSERT or IGNORE INTO scans (time) VALUES (?);", (now,))
        self.c.execute("SELECT scanid FROM scans WHERE time=?", (now,))
        return self.c.fetchone()[0]

    def get_pathid(self, path):
        (dirname, filename) = os.path.split(path)
        # dirname
        self.c.execute("INSERT or IGNORE INTO dirnames (dirname) VALUES (?);", (dirname,))
        self.c.execute("SELECT dirnameid FROM dirnames WHERE dirname=?", (dirname,))
        dirnameid = self.c.fetchone()[0]

        # filename
        self.c.execute("INSERT or IGNORE INTO filenames (filename) VALUES (?);", (filename,))
        self.c.execute("SELECT filenameid FROM filenames WHERE filename=?", (filename,))
        filenameid = self.c.fetchone()[0]

        # pathid
        self.c.execute("INSERT or IGNORE INTO paths (dirnameid,filenameid) VALUES (?,?);", (dirnameid, filenameid,))
        self.c.execute("SELECT pathid FROM paths WHERE dirnameid=? AND filenameid=?", (dirnameid, filenameid,))
        return self.c.fetchone()[0]

    def process_path(self, scanid, path):
        """ Add the file to the database database.
        If it is there and the mtime hasn't been changed, don't re-hash."""

        try:
            st = os.stat(path)
        except FileNotFoundError as e:
            return
        pathid = self.get_pathid(path)

        # See if this file with this length is in the databsae
        self.c.execute("SELECT hashid FROM files WHERE pathid=? AND mtime=? AND size=? LIMIT 1",
                       (pathid, st.st_mtime, st.st_size))
        row = self.c.fetchone()
        try:
            if row:
                (hashid,) = row
            else:
                hashid = self.get_hashid(hash_file(path))
            self.c.execute("INSERT INTO files (pathid,mtime,size,hashid,scanid) VALUES (?,?,?,?,?)",
                           (pathid, st.st_mtime, st.st_size, hashid, scanid))
        except PermissionError as e:
            pass
        except OSError as e:
            pass

    def ingest(self, root):
        import time

        self.c = self.conn.cursor()
        self.c.execute("BEGIN TRANSACTION")

        scanid = self.get_scanid(iso_now())

        count = 0
        dircount = 0
        t0 = time.time()
        for (dirpath, dirnames, filenames) in os.walk(root):
            print(dirpath, end='')
            for filename in filenames:
                self.process_path(scanid, os.path.join(dirpath, filename))
            print("   {}".format(len(filenames)))
            count += len(filenames)
            dircount += 1
            if dircount % COMMIT_RATE == 0:
                self.conn.commit()

        self.conn.commit()
        t1 = time.time()
        self.c.execute("UPDATE scans SET duration=? WHERE scanid=?", (t1 - t0, scanid))
        print("Total files added to database: {}".format(count))
        print("Total directories scanned:     {}".format(dircount))
        print("Total time: {}".format(int(t1 - t0)))


###################### END OF SCANNER CLASS ######################
##################################################################


def get_pathname(conn, pathid):
    c = conn.cursor()
    c.execute("SELECT fileid,dirname,filename FROM files " +
              "NATURAL JOIN paths NATURAL JOIN dirnames NATURAL JOIN filenames " +
              "WHERE fileid=?", (pathid,))
    (fileid, dirname, filename) = c.fetchone()
    return dirname + filename


def scans(conn):
    c = conn.cursor()
    for (scanid, time) in c.execute("SELECT scanid,time FROM scans"):
        print(scanid, time)


def get_all_files(conn, scan1):
    """Files in scan scan1"""
    c = conn.cursor()
    c.execute("SELECT pathid, dirnameid, dirname, filenameid, filename, fileid "
              "FROM files NATURAL JOIN paths NATURAL JOIN dirnames NATURAL JOIN filenames "
              "WHERE scanid=?", (scan1,))
    return (File(pathid=f[0], dirnameid=f[1], dirname=f[2], filenameid=f[3], filename=f[4], fileid=f[5]) for f in c)

def get_new_files(conn, scan0, scan1):
    """Files in scan scan1 that are not in scan scan0"""
    c = conn.cursor()
    c.execute("SELECT pathid, dirnameid, dirname, filenameid, filename "
              "FROM files NATURAL JOIN paths NATURAL JOIN dirnames NATURAL JOIN filenames "
              "WHERE scanid=? AND pathid NOT IN (SELECT pathid FROM files WHERE scanid=?)", (scan1, scan0))
    return (File(pathid=f[0], dirnameid=f[1], dirname=f[2], filenameid=f[3], filename=f[4]) for f in c)


def deleted_files(conn, scan0, scan1):
    """Files in scan scan1 that are not in scan0"""
    return get_new_files(conn, scan1, scan0)

def changed_files(conn, scan0, scan1):
    """Files that were changed between scan0 and scan1"""
    c = conn.cursor()
    c.execute("SELECT a.pathid,a.hashid,b.hashid FROM " +
              "( SELECT pathid, hashid, scanid FROM files WHERE scanid=?) AS 'a' " +
              "JOIN (SELECT pathid, hashid, scanid FROM FILES WHERE scanid=?) as 'b' " +
              "ON a.pathid=b.pathid WHERE a.hashid != b.hashid",
              (scan0, scan1))
    return (File(pathid=f[0]) for f in c)


def duplicate_files(conn, scan0):
    """Return a generator for the duplicate files at scan0.
    Returns a list of a list of File objects, sorted by size"""
    c = conn.cursor()
    d = conn.cursor()
    c.execute('SELECT hashid, ct, size FROM '
              '(SELECT hashid, count(*) AS ct, size FROM files '
              'WHERE scanid=? GROUP BY hashid) NATURAL JOIN hashes WHERE ct>1 ORDER BY 3 DESC;', (scan0,))
    for (hashid, ct, size) in c:
        ret = []
        d.execute(
            "SELECT dirnameid,dirname,filenameid,filename "
            "FROM files NATURAL JOIN paths NATURAL JOIN dirnames NATURAL JOIN filenames "
            "WHERE scanid=? AND hashid=?", (scan0, hashid,))
        yield [File(size=size, dirnameid=f[0], dirname=f[1], filenameid=f[2], filename=f[3]) for f in d]


# This is what I am trying to accomplish:
"""
SELECT hashid,pathid,count(*) AS ctb FROM files WHERE
scanid=2 AND hashid IN
(SELECT hashid FROM
   (SELECT hashid, count(*) AS cta FROM files WHERE scanid=1 GROUP BY hashid HAVING cta=1)
)
GROUP BY hashid HAVING ctb=1;
"""

"""
Better approach:
* Get a list of all the (hashid, pathid1) pairs from scan1 that have only a single pathid for the scan.
* Get a list of all the (hashid, pathid2) pairs from scan2 that have only a single pathid for the scan.
* remove from (hashid, pathid2) all that are in (hashid, pathid1)
 """


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
            yield (File(hashid=hashID,pathid=path1_for_hash[hashID]), File(hashid=hashID,pathid=path2))

def changed(conn, fname):
    """Generate a database object of what's changed."""
    (now, nowid) = execselect(conn, "SELECT time,scanid FROM scans WHERE scanid=max(scanid)")
    (yesterday, yid) = execselect(conn, "SELECT time,scanid FROM scans WHERE scanid=max(scanid)")


def report(conn, a, b):
    atime = execselect(conn, "SELECT time FROM scans WHERE scanid=?", (a,))[0]
    btime = execselect(conn, "SELECT time FROM scans WHERE scanid=?", (b,))[0]
    print("Report from {}->{}".format(atime, btime))

    print("\n")
    print("New files:")
    for f in get_new_files(conn, a, b):
        print(f.dirname + f.filename)

    print("\n")
    print("Changed files:")
    for f in changed_files(conn, a, b):
        print(get_pathname(conn, pathid))

    print("\n")
    print("Deleted files:")
    for f in deleted_files(conn, a, b):
        print(f.dirname + f.filename)

    print("Renamed files:")
    for pair in renamed_files(conn, a, b):
        print(pair[0],pair[1])

    print("Duplicate files:")
    for dups in duplicate_files(conn, scan0):
        print("Filesize: {:,}  Count: {}".format(dups[0].size, len(dups)))
        for dup in dups:
            print("    " + dup.dirname + "/" + dup.filename)
    print("\n-----------")


def jreport(conn):
    dirnameids = {}
    filenameids = {}
    fileids = {}                 # map of fileids.
    all_files = {}              # fileids of all allocated files, by filename
    for f in get_all_files(1):
        all_files[f.filename] = f.fileid
        fileids[f.fileid] = (f.dirnameid,f.filenameid,f.size,f.mtime)
        dirnameids[f.dirnameid] = f.dirname
        filenameids[f.filenameid] = f.filename

    print("var dirnameids = {};".format(json.dumps(dirnameids)))
    print("var filenameids = {};".format(json.dumps(filenameids)))
    print("var fileids = {};".format(json.dumps(fileids)))
    print("var all_files = {};".format(json.dumps(all_files)))

if (__name__ == "__main__"):
    import argparse

    parser = argparse.ArgumentParser(description='Compute file changes',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('roots', type=str, nargs='*', help='Directories to process')
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--create", action="store_true", help="Create database")
    parser.add_argument("--db", help="Specify database location", default="data.sqlite3")
    parser.add_argument("--scans", help="List the scans in the DB", action='store_true')
    parser.add_argument("--report", help="Report what's changed between scans A and B (e.g. A-B)")
    parser.add_argument("--jreport", help="Create 'what's changed?' json report")
    parser.add_argument("--dups", help="Report duplicates for a scan")

    args = parser.parse_args()

    if args.create:
        try:
            os.unlink(args.db)
        except FileNotFoundError:
            pass
        conn = sqlite3.connect(args.db)
        create_schema(conn)
        print("Created")

    if args.scans:
        scans(sqlite3.connect(args.db))
        exit(0)

    # give me a big cache
    conn = sqlite3.connect(args.db)
    conn.cursor().execute("PRAGMA cache_size = {};".format(CACHE_SIZE))

    if args.report:
        m = re.search("(\d+)-(\d+)", args.report)
        if not m:
            print("Usage: --report N-M")
            exit(1)
        report(conn, int(m.group(1)), int(m.group(2)))

    if args.jreport:
        jreport(conn)

    if args.dups:
        report_dups(conn, int(args.dups))

    if args.roots:
        s = Scanner(conn)
        for root in args.roots:
            print(root)
            s.ingest(root)
