#!/usr/bin/env python3
# coding=UTF-8
#
# File change detector

__version__ = '0.0.1'
import os.path
import sqlite3
from abc import ABC, abstractmethod

import scanner
from ctools.dbfile import *
from ctools.tydoc import *
from ctools.env import *
import configparser

CACHE_SIZE = 2000000
SQL_SET_CACHE = "PRAGMA cache_size = {};".format(CACHE_SIZE)

# Replace this with an ORM?
SQLITE3_SCHEMA = \
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

MYSQL_SCHEMA = \
    """
DROP TABLE IF EXISTS `{dbname}`.{prefix}metadata;
CREATE TABLE  `{dbname}`.{prefix}metadata (name VARCHAR(255) PRIMARY KEY, value VARCHAR(255) NOT NULL);

DROP TABLE IF EXISTS `{dbname}`.{prefix}roots;
CREATE TABLE `{dbname}`.{prefix}roots (rootid INTEGER PRIMARY KEY AUTO_INCREMENT, rootdir VARCHAR(255) UNIQUE NOT NULL);

DROP TABLE IF EXISTS `{dbname}`.{prefix}dirnames;
CREATE TABLE  `{dbname}`.{prefix}dirnames (dirnameid INTEGER PRIMARY KEY AUTO_INCREMENT,dirname TEXT(65536));
CREATE UNIQUE INDEX  dirnames_idx2 ON `{dbname}`.{prefix}dirnames (dirname(700));

DROP TABLE IF EXISTS `{dbname}`.{prefix}filenames;
CREATE TABLE  `{dbname}`.{prefix}filenames (filenameid INTEGER PRIMARY KEY AUTO_INCREMENT,filename TEXT(65536));
CREATE INDEX  filenames_idx2 ON `{dbname}`.{prefix}filenames (filename(700));

DROP TABLE IF EXISTS `{dbname}`.{prefix}paths;
CREATE TABLE  `{dbname}`.{prefix}paths (pathid INTEGER PRIMARY KEY AUTO_INCREMENT,
       dirnameid INTEGER REFERENCES `{dbname}`.{prefix}dirnames(dirnameid),
       filenameid INTEGER REFERENCES `{dbname}`.{prefix}filenames(filenameid));
CREATE INDEX  paths_idx2 ON `{dbname}`.{prefix}paths(dirnameid);
CREATE INDEX  paths_idx3 ON `{dbname}`.{prefix}paths(filenameid);

DROP TABLE IF EXISTS `{dbname}`.{prefix}hashes;
CREATE TABLE  `{dbname}`.{prefix}hashes (hashid INTEGER PRIMARY KEY AUTO_INCREMENT,hash TEXT(65536) NOT NULL);
CREATE INDEX  hashes_idx2 ON `{dbname}`.{prefix}hashes( hash(700));

DROP TABLE IF EXISTS `{dbname}`.{prefix}scans;
CREATE TABLE  `{dbname}`.{prefix}scans (scanid INTEGER PRIMARY KEY AUTO_INCREMENT,
                                      rootid INTEGER REFERENCES {prefix}roots(rootid),
                                      time DATETIME NOT NULL,
                                      duration INTEGER);
CREATE INDEX  scans_idx1 ON `{dbname}`.{prefix}scans(scanid);
CREATE INDEX  scans_idx2 ON `{dbname}`.{prefix}scans(time);
CREATE UNIQUE INDEX scans_idx3 ON `{dbname}`.{prefix}scans(rootid,time);

DROP TABLE IF EXISTS `{dbname}`.{prefix}files;
CREATE TABLE  `{dbname}`.{prefix}files (fileid INTEGER PRIMARY KEY AUTO_INCREMENT,
                                  pathid INTEGER REFERENCES {prefix}paths(pathid),
                                  rootid INTEGER REFERENCES {prefix}roots(rootid),
                                  mtime INTEGER NOT NULL, 
                                  size INTEGER NOT NULL, 
                                  hashid INTEGER REFERENCES {prefix}hashes(hashid), 
                                  scanid INTEGER REFERENCES {prefix}scans(scanid));
CREATE INDEX  files_idx1 ON `{dbname}`.{prefix}files(pathid);
CREATE INDEX  files_idx2 ON `{dbname}`.{prefix}files(rootid);
CREATE INDEX  files_idx3 ON `{dbname}`.{prefix}files(mtime);
CREATE INDEX  files_idx4 ON `{dbname}`.{prefix}files(size);
CREATE INDEX  files_idx5 ON `{dbname}`.{prefix}files(hashid);
CREATE INDEX  files_idx6 ON `{dbname}`.{prefix}files(scanid);
CREATE INDEX  files_idx7 ON `{dbname}`.{prefix}files(scanid,hashid);

DROP TABLE IF EXISTS `{dbname}`.{prefix}logs;
CREATE TABLE `{dbname}`.{prefix}logs (logid INTEGER PRIMARY KEY AUTO_INCREMENT,
                                scanid INTEGER REFERENCES {prefix}scans(scanid),
                                message TEXT(65536));
CREATE INDEX logs_idx1 ON `{dbname}`.{prefix}logs(scanid);
CREATE INDEX logs_idx2 ON `{dbname}`.{prefix}logs(time);
CREATE INDEX logs_idx3 ON `{dbname}`.{prefix}logs(scanid,logtime);


"""

"""Explanation of tables:
files         - list of all files
hashes       - table of all hash code
"""


############################################################
############################################################

# Tools for extracting from the database


class FileChangeManager(ABC):
    def __init__(self, conn):
        self.conn = conn

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
    def get_root(self):
        pass

    @abstractmethod
    def get_roots(self):
        pass

    @abstractmethod
    def add_root(self, root):
        pass

    @abstractmethod
    def del_root(self, root):
        pass

    # @abstractmethod
    # def create_database(self, name, root):
    #     pass


class SQLite3FileChangeManager(FileChangeManager):
    def __init__(self, conn):
        super().__init__(conn)

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

    def get_root(conn):
        return SLGSQL.execselect(conn, "SELECT value FROM metadata WHERE key='root'")[0]

    # def create_database(name, root):
    #     if os.path.exists(name):
    #         print("file exists: {}".format(name))
    #         exit(1)
    #     conn = sqlite3.connect(name)
    #     SLGSQL.create_schema(SQL_SCHEMA, conn)
    #     conn.cursor().execute("INSERT INTO metadata (key, value) VALUES (?,?)", ("root", root))
    #     conn.commit()
    #     conn.close()


class MySQLFileChangeManager(FileChangeManager):

    def __init__(self, conn, auth, database, prefix=""):
        self.database = database
        self.auth = auth
        self.prefix = prefix
        super().__init__(conn)

    def list_scans(self):
        results = self.conn.csfr(self.auth,
                                 "SELECT scanid, time, rootid, rootdir FROM `{db}`.{prefix}scans NATURAL JOIN `{db}`.roots".format(
                                     db=self.database, prefix=self.prefix))
        for (scanid, time, rootid, rootdir) in results:
            print(scanid, rootdir, time)

    def last_scan(self):
        return DBMySQL.execselect(self.conn, "SELECT MAX(scanid) from `{db}`.{prefix}scans".format(db=self.database,
                                                                                                   prefix=self.prefix))[
            0]

    def get_all_files(self, scan0):
        results = self.conn.csfr(self.auth,
                                 "SELECT fileid, pathid, rootid, size, dirnameid, dirname, filenameid, filename, mtime "
                                 "FROM `{db}`.{prefix}files NATURAL JOIN {prefix}paths NATURAL JOIN {prefix}dirnames NATURAL JOIN {prefix}filenames "
                                 "WHERE scanid={scanid}".format(db=self.database, scanid=scan0, prefix=self.prefix))
        for fileid, pathid, rootid, size, dirnameid, dirname, filenameid, filename, mtime in results:
            yield {"fileid": fileid, "pathid": pathid, "rootid": rootid, "size": size,
                   "dirnameid": dirnameid, "dirname": dirname, "filenameid": filenameid,
                   "filename": filename, "mtime": mtime}

    def get_new_files(self, scan0, scan1):
        """Files in scan scan1 that are not in scan scan0"""
        ret = []
        results = self.conn.csfr(self.auth,
                                 "SELECT fileid, pathid, size,  dirnameid, dirname, filenameid, filename, mtime "
                                 "FROM `{db}`.{prefix}files NATURAL JOIN {prefix}paths NATURAL JOIN {prefix}dirnames NATURAL JOIN {prefix}filenames "
                                 "WHERE scanid={scan0} AND pathid NOT IN (SELECT pathid FROM {prefix}files WHERE scanid={scan1})".format(
                                     db=self.database, scan0=scan1, scan1=scan0, prefix=self.prefix))
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
        results = self.conn.csfr(self.auth, "SELECT a.pathid as pathid, a.hashid, b.hashid FROM "
                                            "(SELECT pathid, hashid, scanid FROM `{db}`.{prefix}files WHERE scanid={scan0}) as a "
                                            "JOIN (SELECT pathid, hashid, scanid FROM `{db}`.{prefix}files WHERE scanid={scan1}) as b "
                                            "ON a.pathid=b.pathid WHERE a.hashid != b.hashid".format(db=self.database,
                                                                                                     scan0=scan0,
                                                                                                     scan1=scan1,
                                                                                                     prefix=self.prefix))
        for pathid, ahashid, bhashid in results:
            dirnameid, filenameid = self.conn.csfr(self.auth,
                                                   "SELECT dirnameid, filenameid FROM `{db}`.{prefix}paths WHERE pathid={pathid}".format(
                                                       db=self.database, pathid=pathid, prefix=self.prefix))[0]
            dirname = self.conn.csfr(self.auth,
                                     "SELECT dirname FROM `{db}`.{prefix}dirnames WHERE dirnameid={dirnameid}".format(
                                         db=self.database, dirnameid=dirnameid, prefix=self.prefix))[0][0]
            filename = self.conn.csfr(self.auth,
                                      "SELECT filename FROM `{db}`.{prefix}filenames WHERE filenameid={filenameid}".format(
                                          db=self.database, filenameid=filenameid, prefix=self.prefix))[0][0]
            yield {"dirname": dirname, "filename": filename}
        #     ret.append({"pathid":pathid, "ahashid":ahashid, "bhashid":bhashid})
        # return ret

    def get_duplicate_files(self, scan0):
        """Return a generator for the duplicate files at scan0.
        Returns a list of a list of File objects, sorted by size"""
        cresult = self.conn.csfr(self.auth, 'SELECT hashid, ct, size FROM '
                                            '(SELECT hashid, count(hashid) AS ct, size FROM `{dbname}`.{prefix}files WHERE scanid={scanid} GROUP BY hashid, size) as T '
                                            'WHERE ct>1 AND size>{dupsize} ORDER BY 3 DESC;'.format(
            dbname=self.database, scanid=scan0, dupsize=args.dupsize, prefix=self.prefix))
        for hashid, ct, size in cresult:
            ret = []
            dresult = self.conn.csfr(self.auth,
                                     'SELECT fileid,pathid,size,dirnameid,dirname,filenameid,filename,mtime '
                                     'FROM `{dbname}`.files NATURAL JOIN paths NATURAL JOIN dirnames NATURAL JOIN filenames '
                                     'WHERE scanid={scanid} AND hashid={hashid}'.format(dbname=self.database,
                                                                                        scanid=scan0, hashid=hashid))
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
            results = self.conn.csfr(self.auth, 'SELECT hashID, pathid, count(*) AS ct '
                                                'FROM `{db}`.{prefix}files WHERE scanid={scanid} GROUP BY hashid, pathid HAVING ct=1'.format(
                db=self.database, scanid=scanid, prefix=self.prefix))
            return results

        pairs_in_scan0 = set()
        path1_for_hash = {}
        for (hashID, path1, count) in get_singletons(scan0):
            pairs_in_scan0.add((hashID, path1))
            path1_for_hash[hashID] = path1

        for (hashID, path2, count) in get_singletons(scan1):
            if hashID in path1_for_hash and (hashID, path2) not in pairs_in_scan0:
                dirnameid1, filenameid1 = self.conn.csfr(self.auth,
                                                         "SELECT dirnameid, filenameid FROM `{db}`.{prefix}paths WHERE pathid={pathid}".format(
                                                             db=self.database, pathid=path1_for_hash[hashID],
                                                             prefix=self.prefix))[0]
                dirname1 = self.conn.csfr(self.auth,
                                          "SELECT dirname FROM `{db}`.{prefix}dirnames WHERE dirnameid={dirnameid}".format(
                                              db=self.database, dirnameid=dirnameid1, prefix=self.prefix))[0][0]
                filename1 = self.conn.csfr(self.auth,
                                           "SELECT filename FROM `{db}`.{prefix}filenames WHERE filenameid={filenameid}".format(
                                               db=self.database, filenameid=filenameid1, prefix=self.prefix))[0][0]
                dirnameid2, filenameid2 = self.conn.csfr(self.auth,
                                                         "SELECT dirnameid, filenameid FROM `{db}`.{prefix}paths WHERE pathid={pathid}".format(
                                                             db=self.database, pathid=path2, prefix=self.prefix))[0]
                dirname2 = self.conn.csfr(self.auth,
                                          "SELECT dirname FROM `{db}`.{prefix}dirnames WHERE dirnameid={dirnameid}".format(
                                              db=self.database, dirnameid=dirnameid2, prefix=self.prefix))[0][0]
                filename2 = self.conn.csfr(self.auth,
                                           "SELECT filename FROM `{db}`.{prefix}filenames WHERE filenameid={filenameid}".format(
                                               db=self.database, filenameid=filenameid2, prefix=self.prefix))[0][0]
                yield {"dirname1": dirname1, "filename1": filename1, "dirname2": dirname2, "filename2": filename2}

                # yield ({"hashid":hashID, "pathid":path1_for_hash[hashID]},
                #         {"hashid": hashID, "pathid": path2})

    def report(self, a, b):
        docs = None
        if args.out:
            docs = tydoc()
        atime = self.conn.execselect(
            "SELECT time FROM `{db}`.{prefix}scans WHERE scanid={scanid}".format(db=self.database, scanid=a,
                                                                                 prefix=self.prefix))
        btime = self.conn.execselect(
            "SELECT time FROM `{db}`.{prefix}scans WHERE scanid={scanid}".format(db=self.database, scanid=b,
                                                                                 prefix=self.prefix))

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

        result = self.conn.csfr(self.auth,
                                "SELECT scanid, rootid, rootdir FROM `{db}`.{prefix}scans NATURAL JOIN `{db}`.{prefix}roots WHERE scanid=%s".format(
                                    db=self.database, prefix=self.prefix), vals=[b])[0]
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
                dirnameids[f['dirnameid']] = self.conn.csfr(self.auth,
                                                            "SELECT dirname FROM `{}`.dirnames WHERE dirnameid={}".format(
                                                                self.database, f['dirnameid']))
            if f['filenameid'] not in filenameids:
                filenameids[f['filenameid']] = self.conn.csfr(self.auth,
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

    def get_root(self):

        result = self.conn.csfr(auth,
                                "SELECT value FROM `{db}`.{prefix}metadata WHERE name='root'".format(db=self.database,
                                                                                                     prefix=self.prefix))
        return result[0][0]

    def get_roots(self):
        roots = self.conn.csfr(auth,
                               "SELECT rootdir FROM `{db}`.{prefix}roots".format(db=self.database, prefix=self.prefix))
        return roots

    def add_root(self, root):
        try:
            self.conn.csfr(self.auth,
                           "INSERT IGNORE INTO `{db}`.{prefix}roots (rootdir) VALUES ('{root}')".format(
                               db=self.database, root=root, prefix=self.prefix))
        except Exception as e:
            print("Could not add root: {} \n ".format(root), e)

    def del_root(self, root):
        try:
            self.conn.csfr(self.auth,
                           "DELETE IGNORE FROM `{db}`.{prefix}roots WHERE rootdir='{root}'".format(db=self.database,
                                                                                                   root=root,
                                                                                                   prefix=self.prefix))
        except Exception as e:
            print("Could not delete root: {} \n ".format(root), e)


def create_mysql_database(auth, args, schema):
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
    schema = schema.format(dbname=args.db, prefix=args.prefix)
    for line in schema.split(";"):
        line = line.strip()
        if len(line) > 0:
            c.execute(line)
    # c.execute("INSERT INTO `{db}`.roots (rootdir) VALUES ('{rootdir}')".format(db=args.db, rootdir=root))
    conn.commit()
    conn.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Compute file changes',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--create", help="Create a database for a given ROOT", action='store_true')
    parser.add_argument("--db", help="Specify database location")  # needs to be removed (outdated)
    parser.add_argument("--prefix", help="Specify table prefix to sue", default="")  # needs to be removed (outdated)
    parser.add_argument("--config", help="Specify configuration file", default="default.ini")
    parser.add_argument("--scans", help="List the scans in the DB", action='store_true')
    parser.add_argument("--root", help="List the root in the DB", action='store_true')  # (outdated)
    parser.add_argument("--roots", help="List all roots in the DB", action='store_true')  # initial root?
    parser.add_argument("--report", help="Report what's changed between scans A and B (e.g. A-B)")
    parser.add_argument("--jreport", help="Create 'what's changed?' json report", action='store_true')
    parser.add_argument("--dups", help="Report duplicates for most recent scan", action='store_true')
    parser.add_argument("--dupsize", help="Don't report dups smaller than dupsize", default=1024 * 1024, type=int)
    parser.add_argument("--out", help="Specifies output filename")
    parser.add_argument("--vfiles", help="Report each file as ingested", action="store_true")
    parser.add_argument("--vdirs", help="Report each dir as ingested", action="store_true")
    parser.add_argument("--limit", help="Only search this many", type=int)
    parser.add_argument("--threads", help="Number of concurrent threads to use.", default=20, type=int)
    parser.add_argument("--verbose", help="Extra output during runtime", action='store_true')
    parser.add_argument("--addroot", help="Add a new root", type=str)
    parser.add_argument("--delroot", help="Delete an existing root", type=str)
    args = parser.parse_args()

    fchange = None
    auth = None
    if args.config:
        config = get_vars(args.config)
        host = user = password = database = prefix = ""
        if 'MYSQL_HOST' in config:
            host = config['MYSQL_HOST']
        if 'MYSQL_USER' in config:
            user = config['MYSQL_USER']
        if 'MYSQL_PASSWORD' in config:
            password = config['MYSQL_PASSWORD']
        if 'MYSQL_DATABASE' in config:
            database = config['MYSQL_DATABASE']
        if 'MYSQL_PREFIX' in config:
            prefix = config['MYSQL_PREFIX']
        auth = DBMySQLAuth(host=host,
                           user=user,
                           password=password,
                           database=database)
        args.db = auth.database
        args.prefix = prefix
    else:
        pass

    if args.create:
        try:
            create_mysql_database(auth, args, MYSQL_SCHEMA)
            print("Created {}".format(args.db))
        except Exception as e:
            print("An error occured when attempting to create the database: ", e)
    elif args.create:
        # create an sqlite3 db
        pass

    if args.db:
        if args.db.endswith(".sqlite3"):
            try:
                # open database and give me a big cache
                conn = sqlite3.connect(args.db)
                conn.row_factory = sqlite3.Row
                conn.cursor().execute(SQL_SET_CACHE)
                fchange = SQLite3FileChangeManager(conn)
            except sqlite3.OperationalError as e:
                print("Unable to open db file {args.db}", e)
                exit(1)
        else:
            try:
                # auth = DBMySQLAuth.FromEnv(args.config)
                conn = DBMySQL(auth)
                fchange = MySQLFileChangeManager(conn, auth, args.db, args.prefix)
            except Exception as e:
                print("Could not connect to MySQL server: ", e)
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

    if args.root:
        roots = fchange.get_roots()
        for root in roots:
            print(root)
        exit(0)

    if args.roots:
        roots = fchange.get_roots()
        for root in roots:
            print(root)
        exit(0)

    if args.report:
        m = re.search("(\d+)-(\d+)", args.report)
        if not m:
            print("Usage: --report N-M")
            exit(1)
        fchange.report(int(m.group(1)), int(m.group(2)))
    elif args.jreport:
        fchange.jreport()
    elif args.dups:
        fchange.report_dups(fchange.last_scan())
    else:
        # root = fchange.get_root()
        for root in fchange.get_roots():
            root = root[0]
            print("Scanning: {}".format(root))
            if root.startswith("s3://") and args.db.endswith(".sqlite3"):
                sc = scanner.S3Scanner(fchange.conn, args, auth, threads=args.threads, verbose=args.verbose)
            elif root.startswith("s3://"):
                sc = scanner.MySQLS3Scanner(conn, args, auth, args.prefix, threads=args.threads, verbose=args.verbose)
            elif args.db.endswith(".sqlite3"):
                sc = scanner.Scanner(conn, args, auth, threads=args.threads, verbose=args.verbose)
            else:
                sc = scanner.MySQLScanner(conn, args, auth, args.prefix, threads=args.threads, verbose=args.verbose)
            sc.ingest(root)
