#!/usr/bin/env python3
# coding=UTF-8
#
"""
File change detector class.
Contains implementations for SQLite3 and MySQL.
Needs more self-tests.
"""

__version__ = '0.0.1'
import os.path
import sqlite3
from abc import ABC, abstractmethod

import scanner
from ctools.dbfile import DBSqlite3,DBMySQLAuth,DBMySQL
from ctools.tydoc import *
import configparser

# We don't use an object relation mapper (ORM) because the performance was just not there.
# However, we should migrate as much here as possible to the ctools/dbfile class

SQLITE3_CACHE_SIZE = 2000000
SQLLITE3_SET_CACHE = "PRAGMA cache_size = {};".format(SQLITE3_CACHE_SIZE)

SQLITE3_SCHEMA = \
    """
CREATE TABLE IF NOT EXISTS roots (rootid INTEGER PRIMARY KEY, rootdir VARCHAR(255) NOT NULL UNIQUE);
CREATE INDEX IF NOT EXISTS roots_idx0 ON roots(rootdir);

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

MYSQL_SCHEMA = """
DROP TABLE IF EXISTS {prefix}metadata;
CREATE TABLE  {prefix}metadata (name VARCHAR(255) PRIMARY KEY, value VARCHAR(255) NOT NULL);

DROP TABLE IF EXISTS {prefix}roots;
CREATE TABLE {prefix}roots (rootid INTEGER PRIMARY KEY AUTO_INCREMENT, rootdir VARCHAR(255) UNIQUE NOT NULL);

DROP TABLE IF EXISTS {prefix}dirnames;
CREATE TABLE  {prefix}dirnames (dirnameid INTEGER PRIMARY KEY AUTO_INCREMENT,dirname TEXT(65536));
CREATE UNIQUE INDEX  dirnames_idx2 ON {prefix}dirnames (dirname(700));

DROP TABLE IF EXISTS {prefix}filenames;
CREATE TABLE  {prefix}filenames (filenameid INTEGER PRIMARY KEY AUTO_INCREMENT,filename TEXT(65536));
CREATE INDEX  filenames_idx2 ON {prefix}filenames (filename(700));

DROP TABLE IF EXISTS {prefix}paths;
CREATE TABLE  {prefix}paths (pathid INTEGER PRIMARY KEY AUTO_INCREMENT,
       dirnameid INTEGER REFERENCES {prefix}dirnames(dirnameid),
       filenameid INTEGER REFERENCES {prefix}filenames(filenameid));
CREATE INDEX  paths_idx2 ON {prefix}paths(dirnameid);
CREATE INDEX  paths_idx3 ON {prefix}paths(filenameid);

DROP TABLE IF EXISTS {prefix}hashes;
CREATE TABLE  {prefix}hashes (hashid INTEGER PRIMARY KEY AUTO_INCREMENT,hash TEXT(65536) NOT NULL);
CREATE INDEX  hashes_idx2 ON {prefix}hashes( hash(700));

DROP TABLE IF EXISTS {prefix}scans;
CREATE TABLE  {prefix}scans (scanid INTEGER PRIMARY KEY AUTO_INCREMENT,
                                      rootid INTEGER REFERENCES {prefix}roots(rootid),
                                      time DATETIME NOT NULL,
                                      duration INTEGER);
CREATE INDEX  scans_idx1 ON {prefix}scans(scanid);
CREATE INDEX  scans_idx2 ON {prefix}scans(time);
CREATE UNIQUE INDEX scans_idx3 ON {prefix}scans(rootid,time);

DROP TABLE IF EXISTS {prefix}files;
CREATE TABLE  {prefix}files (fileid INTEGER PRIMARY KEY AUTO_INCREMENT,
                                  pathid INTEGER REFERENCES {prefix}paths(pathid),
                                  rootid INTEGER REFERENCES {prefix}roots(rootid),
                                  mtime INTEGER NOT NULL, 
                                  size INTEGER NOT NULL, 
                                  hashid INTEGER REFERENCES {prefix}hashes(hashid), 
                                  scanid INTEGER REFERENCES {prefix}scans(scanid));
CREATE INDEX  files_idx1 ON {prefix}files(pathid);
CREATE INDEX  files_idx2 ON {prefix}files(rootid);
CREATE INDEX  files_idx3 ON {prefix}files(mtime);
CREATE INDEX  files_idx4 ON {prefix}files(size);
CREATE INDEX  files_idx5 ON {prefix}files(hashid);
CREATE INDEX  files_idx6 ON {prefix}files(scanid);
CREATE INDEX  files_idx7 ON {prefix}files(scanid,hashid);
"""

"""Explanation of tables:
files        - list of all files
hashes       - table of all hash code
"""


############################################################
############################################################

# Tools for extracting from the database


class FileChangeManager(ABC):
    def __init__(self, *, db, prefix):
        self.prefix  = prefix
        self.db      = db

    @abstractmethod
    def list_scans(self):
        pass

    @abstractmethod
    def last_scan(self):
        pass

    @abstractmethod
    def get_all_files(self, scan0):
        pass

    @abstractmethod
    def get_new_files(self, scan0, scan1):
        pass

    @abstractmethod
    def deleted_files(self, scan0, scan1):
        pass

    @abstractmethod
    def changed_files(self, scan0, scan1):
        pass

    @abstractmethod
    def get_duplicate_files(self, scan0):
        pass

    @abstractmethod
    def renamed_files(self, scan0, scan1):
        pass

    @abstractmethod
    def report(self, a, b):
        pass

    @abstractmethod
    def report_dups(self, scan0):
        pass

    @abstractmethod
    def jreport(self):
        pass

    @abstractmethod
    def create_database(self):
        pass

    def get_roots(self):
        """Return a list of the roots in the database."""
        rows = self.db.csfr(self.auth,
                            "SELECT rootdir FROM {prefix}roots".format(prefix=self.prefix),[])
        print("rows=",list(rows))
        return [row[0] for row in rows]

    def add_root(self, root):
        "Add a root, ignore if it is already there. "
        self.db.csfr(self.auth,
                     f"INSERT IGNORE INTO {self.prefix}roots (rootdir) VALUES ( %s )",
                     [root])
        self.db.commit()

    def del_root(self, root):
        self.db.csfr(self.auth,
                     f"DELETE IGNORE FROM {self.prefix}roots WHERE rootdir=%s",
                     [root])
        


class SQLite3FileChangeManager(FileChangeManager):
    def __init__(self, *, fname, prefix=""):
        super().__init__(db=DBSqlite3(fname=fname), prefix=prefix)
        self.auth = None

    def create_database(self):
        self.db.create_schema(SQLITE3_SCHEMA)

    def list_scans(self):
        with self.conn as c:
            c.execute("SELECT scanid, time FROM scans")
            results = c.fetchall()
            for (scanid, time) in results:
                print(scanid, time)

    def last_scan(self):
        return self.conn.execselect(conn, "SELECT MAX(scanid) FROM scans", ())[0]

    def get_all_files(self, scan1):
        """Files in scan scan1"""
        c = self.conn.cursor()
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

    def report_dups(conn, scan0):
        import codecs
        with codecs.open(args.out, mode="w", encoding='utf-8') as out:
            out.write("Duplicate files:\n")
            total_wasted = 0
            for dups in duplicate_files(conn, scan0):
                out.write("Filesize: {:,}  Count: {}\n".format(dups[0].size, len(dups)))
                for dup in dups:
                    out.write("   , " + os.path.join(dup.dirname, dup.filename) + "\n")
                total_wasted += dups[0].size * (len(dups) - 1)
            out.write("-----------\n")
            out.write("Total wasted space: {}MB".format(total_wasted / 1000000))

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
                for name in val:
                    fp.write('{}[{}] = {};\n'.format(name, json.dumps(name), json.dumps(val[name])))

            dump_dictionary(fp, "all_files", all_files)
            dump_dictionary(fp, "new_files", new_files)
            dump_dictionary(fp, "duplicate_files", new_files)
            dump_dictionary(fp, "dirnameids", dirnameids)
            dump_dictionary(fp, "filenameids", filenameids)
            dump_dictionary(fp, "fileids", fileids)

class MySQLFileChangeManager(FileChangeManager):
    def __init__(self, *, auth, prefix=""):
        self.auth     = auth
        self.db       = DBMySQL(auth)
        self.database = database
        super().__init__(prefix=prefix)

    def list_scans(self):
        results = self.db.csfr(self.auth,
                               "SELECT scanid, time, rootid, rootdir FROM {prefix}scans NATURAL JOIN {prefix}roots"
                               .format(prefix=self.prefix))
        for (scanid, time, rootid, rootdir) in results:
            print(scanid, rootdir, time)

    def last_scan(self):
        return DBMySQL.execselect(self.db, "SELECT MAX(scanid) from {prefix}scans"
                                  .format(prefix=self.prefix))[0]

    def get_all_files(self, scan0):
        results = self.db.csfr(self.auth,
                                 "SELECT fileid, pathid, rootid, size, dirnameid, dirname, filenameid, filename, mtime "
                                 "FROM {prefix}files NATURAL JOIN {prefix}paths NATURAL JOIN {prefix}dirnames NATURAL JOIN {prefix}filenames "
                                 "WHERE scanid={scanid}".format(scanid=scan0, prefix=self.prefix))
        for fileid, pathid, rootid, size, dirnameid, dirname, filenameid, filename, mtime in results:
            yield {"fileid": fileid, "pathid": pathid, "rootid": rootid, "size": size,
                   "dirnameid": dirnameid, "dirname": dirname, "filenameid": filenameid,
                   "filename": filename, "mtime": mtime}

    def get_new_files(self, scan0, scan1):
        """Files in scan scan1 that are not in scan scan0"""
        ret = []
        results = self.db.csfr(self.auth,
                                 "SELECT fileid, pathid, size,  dirnameid, dirname, filenameid, filename, mtime "
                                 "FROM {prefix}files NATURAL JOIN {prefix}paths NATURAL JOIN {prefix}dirnames NATURAL JOIN {prefix}filenames "
                                 "WHERE scanid={scan0} AND pathid NOT IN (SELECT pathid FROM {prefix}files WHERE scanid={scan1})"
                               .format(scan0=scan1, scan1=scan0, prefix=self.prefix))
        # Maybe this should be cleaned up? It might look messy, but the logic makes sense.
        for fileid, pathid, size, dirnameid, dirname, filenameid, filename, mtime in results:
            ret.append({"fileid": fileid, "pathid": pathid, "size": size,
                        "dirnameid": dirnameid, "dirname": dirname, "filenameid": filenameid, "filename": filename,
                        "mtime": mtime})
        return ret

    def deleted_files(self, scan0, scan1):
        return self.get_new_files(scan1, scan0)

    def changed_files(self, scan0, scan1):
        """Files that were changed between scan0 and scan1"""
        ret = []
        results = self.db.csfr(self.auth, "SELECT a.pathid as pathid, a.hashid, b.hashid FROM "
                                            "(SELECT pathid, hashid, scanid FROM {prefix}files WHERE scanid={scan0}) as a "
                                            "JOIN (SELECT pathid, hashid, scanid FROM {prefix}files WHERE scanid={scan1}) as b "
                                            "ON a.pathid=b.pathid WHERE a.hashid != b.hashid"
                               .format(scan0=scan0, scan1=scan1, prefix=self.prefix))
        for pathid, ahashid, bhashid in results:
            dirnameid, filenameid = self.db.csfr(self.auth,
                                                 "SELECT dirnameid, filenameid FROM {prefix}paths WHERE pathid={pathid}"
                                                 .format( pathid=pathid, prefix=self.prefix))[0]
            dirname = self.db.csfr(self.auth,
                                   "SELECT dirname FROM {prefix}dirnames WHERE dirnameid={dirnameid}"
                                   .format( dirnameid=dirnameid, prefix=self.prefix))[0][0]
            filename = self.db.csfr(self.auth,
                                    "SELECT filename FROM {prefix}filenames WHERE filenameid={filenameid}"
                                    .format(filenameid=filenameid, prefix=self.prefix))[0][0]
            yield {"dirname": dirname, "filename": filename}
        #     ret.append({"pathid":pathid, "ahashid":ahashid, "bhashid":bhashid})
        # return ret

    def get_duplicate_files(self, scan0):
        """Return a generator for the duplicate files at scan0.
        Returns a list of a list of File objects, sorted by size"""
        cresult = self.db.csfr(self.auth,
                               """SELECT hashid, ct, size FROM 
                               (SELECT hashid, count(hashid) AS ct, size FROM {prefix}files 
                               WHERE scanid={scanid} GROUP BY hashid, size) as T 
                               WHERE ct>1 AND size>{dupsize} ORDER BY 3 DESC;"""
                               .format( scanid=scan0, dupsize=args.dupsize, prefix=self.prefix))
        for hashid, ct, size in cresult:
            ret = []
            dresult = self.db.csfr(self.auth,
                                   'SELECT fileid,pathid,size,dirnameid,dirname,filenameid,filename,mtime '
                                   'FROM files NATURAL JOIN paths NATURAL JOIN dirnames NATURAL JOIN filenames '
                                   'WHERE scanid={scanid} AND hashid={hashid}'
                                   .format( scanid=scan0, hashid=hashid))
            for fileid, pathid, size, dirnameid, dirname, filenameid, filename, mtime in dresult:
                ret.append({"fileid": fileid, "pathid": pathid, "size": size,
                            "dirnameid": dirnameid, "dirname": dirname, "filenameid": filenameid,
                            "filename": filename, "mtime": mtime})
            yield ret

    # TODO: renamed files only tracks last scanned file. Will consider any file containing the same information as renamed if they have different names.
    def renamed_files(self, scan0, scan1):
        """Return a generator for the duplicate files at scan0.
        The generator returns pairs of (F_old,F_new)
        """

        def get_singletons(scanid):
            results = self.db.csfr(self.auth, 'SELECT hashID, pathid, count(*) AS ct '
                                                'FROM {prefix}files WHERE scanid={scanid} GROUP BY hashid, pathid HAVING ct=1'
                                   .format(scanid=scanid, prefix=self.prefix))
            return results

        pairs_in_scan0 = set()
        path1_for_hash = {}
        for (hashID, path1, count) in get_singletons(scan0):
            pairs_in_scan0.add((hashID, path1))
            path1_for_hash[hashID] = path1

        for (hashID, path2, count) in get_singletons(scan1):
            if hashID in path1_for_hash and (hashID, path2) not in pairs_in_scan0:
                dirnameid1, filenameid1 = self.db.csfr(self.auth,
                                                         "SELECT dirnameid, filenameid FROM {prefix}paths WHERE pathid={pathid}"
                                                       .format( pathid=path1_for_hash[hashID],
                                                             prefix=self.prefix))[0]
                dirname1 = self.db.csfr(self.auth,
                                        "SELECT dirname FROM {prefix}dirnames WHERE dirnameid={dirnameid}"
                                        .format( dirnameid=dirnameid1, prefix=self.prefix))[0][0]
                filename1 = self.db.csfr(self.auth,
                                         "SELECT filename FROM {prefix}filenames WHERE filenameid={filenameid}"
                                         .format( filenameid=filenameid1, prefix=self.prefix))[0][0]

                dirnameid2, filenameid2 = self.db.csfr(self.auth,
                                                       "SELECT dirnameid, filenameid FROM {prefix}paths WHERE pathid={pathid}"
                                                       .format( pathid=path2, prefix=self.prefix))[0]
                dirname2 = self.db.csfr(self.auth,
                                          "SELECT dirname FROM {prefix}dirnames WHERE dirnameid={dirnameid}"
                                        .format( dirnameid=dirnameid2, prefix=self.prefix))[0][0]
                filename2 = self.db.csfr(self.auth,
                                         "SELECT filename FROM {prefix}filenames WHERE filenameid={filenameid}"
                                         .format( filenameid=filenameid2, prefix=self.prefix))[0][0]
                yield {"dirname1": dirname1, "filename1": filename1, "dirname2": dirname2, "filename2": filename2}

                # yield ({"hashid":hashID, "pathid":path1_for_hash[hashID]}, {"hashid": hashID, "pathid": path2})

    def report(self, a, b):
        docs = None
        if args.out:
            docs = tydoc()
        atime = self.db.execselect(
            "SELECT time FROM {prefix}scans WHERE scanid={scanid}"
            .format(scanid=a, prefix=self.prefix))
        btime = self.db.execselect(
            "SELECT time FROM {prefix}scans WHERE scanid={scanid}"
            .format(scanid=b, prefix=self.prefix))

        if atime is None:
            atime = 0
        else:
            atime = atime[0]

        if btime is None:
            btime = 0
        else:
            btime = btime[0]
        print("Report from {}->{}".format(str(atime), str(btime)))

        if args.out: docs.set_title("Report from {}->{}".format(str(atime), str(btime)))

        print()
        print("New files:")
        if args.out: docs.h1("New files:")
        for f in self.get_new_files(a, b):
            if args.out: docs.p(os.path.join(f['dirname'], f['filename']))
            print(os.path.join(f["dirname"], f["filename"]))

        print()
        print("Changed files:")
        if args.out: docs.h1("Changed files:")
        for f in self.changed_files(a, b):
            if args.out: docs.p(os.path.join(f['dirname'], f['filename']))
            print(os.path.join(f['dirname'], f['filename']))

        print()
        print("Deleted files:")
        if args.out: docs.h1("Deleted files:")
        for f in self.deleted_files(a, b):
            if args.out: docs.p(os.path.join(f['dirname'], f['filename']))
            print(os.path.join(f["dirname"], f["filename"]))

        print()
        print("Renamed files:")
        if args.out: docs.h1("Renamed files:")
        for f in self.renamed_files(a, b):
            if args.out: docs.p(os.path.join(f['dirname'], f['filename']))
            print(os.path.join(f["dirname1"], f["filename1"]) + " -> " + os.path.join(f["dirname2"], f["filename2"]))

        print()
        print("Duplicate files:")
        if args.out: docs.h1("Duplicate files:")
        for dups in self.get_duplicate_files(b):
            if args.out: docs.p("Filesize: {:,}  Count: {}".format(dups[0]["size"], len(dups)))
            print("Filesize: {:,}  Count: {}".format(dups[0]["size"], len(dups)))
            for dup in dups:
                if args.out: docs.p("    {}".format(os.path.join(dup["dirname"], dup["filename"])))
                print("    {}".format(os.path.join(dup["dirname"], dup["filename"])))
        print("\n-----------")
        if args.out: docs.save(args.out.split('/')[-1] + ".html")

    def report_dups(self, scan0):
        import codecs
        with codecs.open(args.out, mode="w", encoding='utf8mb4') as out:
            out.write("Duplicate files:\n")
            total_wasted = 0
            for dups in self.get_duplicate_files(conn, scan0):
                out.write("Filesize: {:,}  Count: {}\n".format(dups[0].size, len(dups)))
                for dup in dups:
                    print(dup)
                    out.write("   , " + os.path.join(dup.dirname, dup.filename) + "\n")
                total_wasted += dups[0].size * (len(dups) - 1)
            out.write("-----------\n")
            out.write("Total wasted space: {}MB".format(total_wasted / 1000000))

    def report_dups(self, b):
        duplicate_bytes = 0

        result = self.db.csfr(self.auth,
                              "SELECT scanid, rootid, rootdir FROM {prefix}scans NATURAL JOIN {prefix}roots WHERE scanid=%s"
                              .format( prefix=self.prefix), vals=[b])[0]
        rootdir = result[2]
        print("ROOT DIR: ", rootdir)

        for dups in self.get_duplicate_files(b):
            if dups[0]["size"] > args.dupsize:
                print("Filesize: {:,}  Count: {}".format(dups[0]["size"], len(dups)))
                for d in dups:
                    print("    {}".format(os.path.join(d["dirname"], d["filename"])))
                print()
                duplicate_bytes += dups[0]["size"] * (len(dups) - 1)
        print("\n-----------")
        print("Total space duplicated by files larger than {:,}: {:,}".format(args.dupsize, duplicate_bytes))

    def jreport(self):
        from collections import defaultdict
        dirnameids = {}
        filenameids = {}
        fileids = {}  # map of fileids.

        last = self.last_scan()

        def backfill(f):
            if (f['fileid'] == None or f['dirnameid'] == None or f['filenameid'] == None
                    or f['size'] == None or f['mtime'] == None):
                print("f:", f)
                exit(1)
            if f['fileid'] not in fileids:
                fileids[f['fileid']] = (f['dirnameid'], f['filenameid'], f['size'], f['mtime'])
            if f['dirnameid'] not in dirnameids:
                dirnameids[f['dirnameid']] = self.db.csfr(self.auth,
                                                            "SELECT dirname FROM `{}`.dirnames WHERE dirnameid={}".format(
                                                                self.database, f['dirnameid']))
            if f['filenameid'] not in filenameids:
                filenameids[f['filenameid']] = self.db.csfr(self.auth,
                                                              "SELECT filename FROM `{}`.filenames WHERE filenameid={}".format(
                                                                  self.database, f['filenameid']))

        print("all_files")
        all_files = defaultdict(list)
        for f in self.get_all_files(last):
            all_files[f["filename"]].append(f["fileid"])
            backfill(f)

        print("new_files")
        new_files = defaultdict(list)
        for f in self.get_new_files(last - 1, last):
            new_files[f['filename']].append(f['fileid'])
            backfill(f)

        print("duplicate files")
        duplicate_files = []
        for dups in self.get_duplicate_files(last):
            duplicate_files.append([f['fileid'] for f in dups])
            for f in dups:
                backfill(f)

        data = {"dirnameids": dirnameids,
                "filenameids": filenameids,
                "fileids": fileids,
                "all_files": all_files,
                "new_files": new_files}
        # Make the JavaScript file
        with (open(args.out, "w") if args.out else sys.stdout) as fp:
            def dump_dictionary(fp, name, val):
                fp.write("var " + name + " = {};\n")
                for name in val:
                    fp.write('{}[{}] = {};\n'.format(name, json.dumps(name),
                                                     json.dumps(val[name], indent=4, sort_keys=True, default=str)))

            dump_dictionary(fp, "all_files", all_files)
            dump_dictionary(fp, "new_files", new_files)
            dump_dictionary(fp, "duplicate_files", new_files)
            dump_dictionary(fp, "dirnameids", dirnameids)
            dump_dictionary(fp, "filenameids", filenameids)
            dump_dictionary(fp, "fileids", fileids)



def create_mysql_database(auth, args, schema, prefix):
    try:
        import mysql.connector as mysql
        internalError = RuntimeError
    except ImportError as e:
        try:
            import pymysql
            import pymysql as mysql
            internalError = pymysql.err.InternalError
        except ImportError as e:
            print(
                f"Please install MySQL connector with 'conda install mysql-connector-python' or the pure-python "
                f"pymysql connector")
            raise ImportError()

    conn = mysql.connect(host=auth.host, user=auth.user, password=auth.password)

    c = conn.cursor()
    c.execute("CREATE DATABASE IF NOT EXISTS {}".format(args.db))
    # schema = schema.replace("filetime_tools", args.db)
    schema = schema.format(dbname=args.db, prefix=prefix)
    for line in schema.split(";"):
        line = line.strip()
        if len(line) > 0:
            c.execute(line)
    # c.execute("INSERT INTO roots (rootdir) VALUES ('{rootdir}')".format(rootdir=root))
    conn.commit()
    conn.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Compute file changes',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    g = parser.add_mutually_exclusive_group(required=True)
    g.add_argument("--sqlite3db", help="Specify sqlite3db file")  
    g.add_argument("--config", help="Specify configuration for MySQL info file", default=None)
    parser.add_argument("--create", help="Create a database", action='store_true')
    parser.add_argument("--scans", help="List the scans in the DB", action='store_true')
    parser.add_argument("--roots", help="List all roots in the DB", action='store_true')  # initial root?
    parser.add_argument("--report", help="Report what's changed between scans A and B (e.g. A-B)")
    parser.add_argument("--jreport", help="Create 'what's changed?' json report", action='store_true')
    parser.add_argument("--dump",   help='Dump the last scan in a standard form', action='store_true')
    parser.add_argument("--dups", help="Report duplicates for most recent scan", action='store_true')
    parser.add_argument("--dupsize", help="Don't report dups smaller than dupsize", default=1024 * 1024, type=int)
    parser.add_argument("--out", help="Specifies output filename")
    parser.add_argument("--vfiles", help="Report each file as ingested", action="store_true")
    parser.add_argument("--vdirs", help="Report each dir as ingested", action="store_true")
    parser.add_argument("--limit", help="Only search this many", type=int)
    parser.add_argument("--addroot", help="Add a new root", type=str)
    parser.add_argument("--delroot", help="Delete an existing root", type=str)
    parser.add_argument("--scan", help="Initiate a scan", action='store_true')
    
    args = parser.parse_args()

    fchange = None
    auth = None

    if args.config:
        config = configparser.ConfigParser()
        config.read(args.config)
        auth = DBMySQLAuth(host=config["MYSQL_SERVER"]["HOST"] if config["MYSQL_SERVER"]["HOST"] is not None else "",
                           user=config["MYSQL_SERVER"]["USER"] if config["MYSQL_SERVER"]["USER"] is not None else "",
                           password=config["MYSQL_SERVER"]["PASSWORD"] if config["MYSQL_SERVER"][
                                                                              "PASSWORD"] is not None else "",
                           database=config["MYSQL_SERVER"]["DATABASE"] if config["MYSQL_SERVER"][
                                                                              "DATABASE"] is not None else "")
        prefix = config["DEFAULT"]["TABLE_PREFIX"]
        if args.create:
            try:
                create_mysql_database(auth, args, MYSQL_SCHEMA, prefix)
                print("Created {}".format(args.db))
            except Exception as e:
                print("An error occured when attempting to create the database: ", e)
        try:
            # auth = DBMySQLAuth.FromEnv(args.config)
            fchange = MySQLFileChangeManager(auth=auth, adatabase=args.db, prefix=prefix)
        except Exception as e:
            print("Could not connect to MySQL server: ", e)
            exit(1)
    elif args.sqlite3db:
        try:
            # open database and give me a big cache
            conn = sqlite3.connect(args.sqlite3db)
            conn.row_factory = sqlite3.Row
            conn.cursor().execute(SQLITE3_SET_CACHE)
            fchange = SQLite3FileChangeManager(conn)
        except sqlite3.OperationalError as e:
            print("Unable to open db file {args.sqlite3db}", e)
            exit(1)

    if args.addroot:
        fchange.add_root(args.addroot)
        print("Added root: ", args.addroot)
        exit(0)

    if args.delroot:
        fchange.del_root(args.delroot)
        print("Deleted root: ", args.delroot)
        exit(0)

    if args.scans:
        fchange.list_scans()
        exit(0)

    if args.roots:
        print("\n".join(fchange.get_roots()))
        exit(0)

    if args.report:
        m = re.search(r"(\d+)-(\d+)", args.report)
        if not m:
            print("Usage: --report N-M")
            exit(1)
        fchange.report(int(m.group(1)), int(m.group(2)))
    elif args.jreport:
        fchange.jreport()
    elif args.dups:
        fchange.report_dups(fchange.last_scan())
    elif args.scan:
        for root in fchange.get_roots():
            print("Scanning: {}".format(root))
            if root.startswith("s3://") and args.db.endswith(".sqlite3"):
                sc = scanner.S3Scanner(fchange.conn, args, auth)
            elif root.startswith("s3://"):
                sc = scanner.MySQLS3Scanner(conn, args, auth, prefix)
            elif args.db.endswith(".sqlite3"):
                sc = scanner.Scanner(conn, args, auth)
            else:
                sc = scanner.MySQLScanner(conn, args, auth, prefix)
            sc.ingest(root)
    else:
        parser.print_help()
            
