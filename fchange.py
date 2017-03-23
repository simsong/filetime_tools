#!/usr/bin/env python3
# coding=UTF-8
#
# File change detector

__version__='0.0.1'
import os.path,sys

import os,sys,re,collections,sqlite3
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

COMMIT_RATE = 10                # commit every 10 directories

def create_schema(conn):
    # If the schema doesn't exist, create it
    c = conn.cursor()
    for line in schema.split(";"):
        print(line,end="")
        c.execute(line)

def iso_now():
    return datetime.datetime.now().isoformat()[0:19]

def hash_file(path):
    from hashlib import md5
    m = md5()
    with open(path,"rb") as f:
        while True:
            buf = f.read(65535)
            if buf:
                m.update(buf)
            else:
                break
    return m.hexdigest()
        

class Scanner(object):
    def __init__(self,conn):
        self.conn = conn
        self.c = self.conn.cursor()

    def get_hashid(self,hash):
        self.c.execute("INSERT or IGNORE INTO hashes (hash) VALUES (?);",(hash,))
        self.c.execute("SELECT hashid from hashes where hash=?",(hash,))
        return self.c.fetchone()[0]

    def get_scanid(self,now):
        self.c.execute("INSERT or IGNORE INTO scans (time) VALUES (?);",(now,))
        self.c.execute("SELECT scanid from scans where time=?",(now,))
        return self.c.fetchone()[0]

    def get_pathid(self,path):
        (dirname,filename) = os.path.split(path)
        # dirname
        self.c.execute("INSERT or IGNORE INTO dirnames (dirname) VALUES (?);",(dirname,))
        self.c.execute("SELECT dirnameid from dirnames where dirname=?",(dirname,))
        dirnameid = self.c.fetchone()[0]

        # filename
        self.c.execute("INSERT or IGNORE INTO filenames (filename) VALUES (?);",(filename,))
        self.c.execute("SELECT filenameid from filenames where filename=?",(filename,))
        filenameid = self.c.fetchone()[0]

        # pathid
        self.c.execute("INSERT or IGNORE INTO paths (dirnameid,filenameid) VALUES (?,?);",(dirnameid,filenameid,))
        self.c.execute("SELECT pathid from paths where dirnameid=? and filenameid=?",(dirnameid,filenameid,))
        return self.c.fetchone()[0]        

    def process_path(self,scanid,path):
        """ Add the file to the database database.
        If it is there and the mtime hasn't been changed, don't re-hash."""

        try:
            st = os.stat(path)
        except FileNotFoundError as e:
            return 
        pathid = self.get_pathid(path)
        
        # See if this file with this length is in the databsae
        self.c.execute("SELECT hashid from files where pathid=? and mtime=? and size=? LIMIT 1",
                       (pathid,st.st_mtime,st.st_size))
        row = self.c.fetchone()
        try:
            if row:
                (hashid,) = row
            else:
                hashid = self.get_hashid(hash_file(path))
            self.c.execute("INSERT into files (pathid,mtime,size,hashid,scanid) values (?,?,?,?,?)",
                           (pathid,st.st_mtime,st.st_size,hashid,scanid))
        except PermissionError as e:
            pass
        except OSError as e:
            pass
        except FileNotFoundError as e:
            pass

    def ingest(self,root):
        import time

        self.c = self.conn.cursor()
        self.c.execute("BEGIN TRANSACTION")

        scanid = self.get_scanid(iso_now())

        count = 0
        dircount = 0
        t0 = time.time()
        for (dirpath, dirnames, filenames) in os.walk(root):
            print(dirpath,end='')
            for filename in filenames:
                self.process_path(scanid,os.path.join(dirpath,filename))
            print("   {}".format(len(filenames)))
            count += len(filenames)
            dircount += 1
            if dircount % COMMIT_RATE==0:
                self.conn.commit()

        self.conn.commit()
        t1 = time.time()
        self.c.execute("UPDATE scans set duration=? where scanid=?",(t1-t0,scanid)) 
        print("Total files added to database: {}".format(count))
        print("Total directories scanned:     {}".format(dircount))
        print("Total time: {}".format(int(t1-t0)))

def get_pathname(conn,pathid):
    c = conn.cursor()
    c.execute("SELECT fileid,dirname,filename FROM files " +
              "NATURAL JOIN paths NATURAL JOIN dirnames NATURAL JOIN filenames " +
              "where fileid=?",(pathid,))
    (fileid,dirname,filename) = c.fetchone()
    return dirname + filename

def dump(conn,what):
    c = conn.cursor()
    for (scanid,time) in c.execute("select scanid,time from scans"):
        print(scanid,time)

def execselect(conn,sql,vals):
    c= conn.cursor()
    c.execute(sql,vals)
    return c.fetchone()

def report(conn,a,b):
    atime = execselect(conn,"select time from scans where scanid=?",(a,))[0]
    btime = execselect(conn,"select time from scans where scanid=?",(b,))[0]
    print("Report from {}->{}".format(atime,btime))

    print("\n")
    print("New files:")
    c = conn.cursor()
    c.execute("SELECT pathid, dirname, filename FROM files NATURAL JOIN paths NATURAL JOIN dirnames NATURAL JOIN filenames " +
              "where scanid=2 and pathid not in (select pathid from files where scanid=1)")
    for (pathid, dirname, filename) in c:
        print(dirname+filename)

    print("\n")
    print("Deleted files:")
    c = conn.cursor()
    c.execute("select pathid, dirname, filename FROM files NATURAL JOIN paths NATURAL JOIN dirnames NATURAL JOIN filenames " +
              "where scanid=1 and pathid not in (select pathid from files where scanid=2)")
    for (pathid, dirname, filename) in c:
        print(dirname+filename)

    print("\n")
    print("Changed files:")
    c = conn.cursor()
    c.execute("SELECT a.pathid,a.hashid,b.hashid FROM (SELECT pathid, hashid, scanid FROM files WHERE scanid=1) AS 'a' " +
              "JOIN (SELECT pathid, hashid, scanid FROM FILES WHERE scanid=2) as 'b' " +
              "ON a.pathid=b.pathid WHERE a.hashid != b.hashid")
    for (pathid,hash1,hash2) in c:
        print(get_pathname(conn,pathid))

    print("Renamed files:")
    print("Duplicate files:")
    c = conn.cursor()
    d = conn.cursor()
    c.execute("select hashid,ct from (select hashid ,count(*) as ct from files where scanid=? group by hashid) natural join hashes where ct>1;",(b,))
    for (hashid,ct) in c:
        d.execute("SELECT dirname,filename FROM files NATURAL JOIN paths NATURAL JOIN dirnames NATURAL JOIN filenames " +
                  "WHERE scanid=? and hashid=?",(b,hashid,))
        for (dirname,filename) in d:
            print(dirname+filename)
        print("-----------")
        

if(__name__=="__main__"):
    import argparse
    parser = argparse.ArgumentParser(description='Compute file changes',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('roots', type=str, nargs='*', help='Directories to process')
    parser.add_argument("--debug",action="store_true")
    parser.add_argument("--create",action="store_true",help="Create database")
    parser.add_argument("--db",help="Specify database location",default="data.sqlite3")
    parser.add_argument("--dump",help="[scans]")
    parser.add_argument("--report",help="Report what's changed between scans A and B (e.g. A-B)")

    args = parser.parse_args()

    if args.create:
        try:
            os.unlink(args.db)
        except FileNotFoundError:
            pass
        conn = sqlite3.connect(args.db)
        create_schema(conn)
        print("Created")

    if args.dump:
        dump(sqlite3.connect(args.db),args.dump)
        exit(0)

    # give me a big cache
    conn = sqlite3.connect(args.db)
    conn.cursor().execute("PRAGMA cache_size = {};".format(CACHE_SIZE)) 

    if args.report:
        m = re.search("(\d+)-(\d+)",args.report)
        if not m:
            print("Usage: --report N-M")
            exit(1)
        report(conn,int(m.group(1)),int(m.group(2)))


    if args.roots:
        s = Scanner(conn)
        for root in args.roots:
            print(root)
            s.ingest(root)
