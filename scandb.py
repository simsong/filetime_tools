#!/usr/bin/env python3
# coding=UTF-8
#
"""
File scan database class.
Contains implementations for SQLite3 and MySQL.
This database mechanim is used to store data about file systems. It also contains minimal algorithms for performing file system operations.
This class builds upon the ctools.dbfile class. It should have no code that is in dbfile.
"""
__version__ = '0.0.1'
import os.path
import sqlite3
from abc import ABC, abstractmethod

import scanner
import ctools.dbfile as dbfile
from ctools.tydoc import *
import configparser

MYSQL_SERVER_SECTION="mysql_server"
FCHANGE_SECTION="fchange"
TABLE_PREFIX="table_prefix"


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
CREATE TABLE  {prefix}metadata (name VARCHAR(255) PRIMARY KEY, value VARCHAR(255) NOT NULL) character set utf8;

DROP TABLE IF EXISTS {prefix}roots;
CREATE TABLE {prefix}roots (rootid INTEGER PRIMARY KEY AUTO_INCREMENT, rootdir VARCHAR(255) UNIQUE NOT NULL) character set utf8;

DROP TABLE IF EXISTS {prefix}dirnames;
CREATE TABLE  {prefix}dirnames (dirnameid INTEGER PRIMARY KEY AUTO_INCREMENT,dirname TEXT(65536)) character set utf8;
CREATE UNIQUE INDEX  dirnames_idx2 ON {prefix}dirnames (dirname(255));

DROP TABLE IF EXISTS {prefix}filenames;
CREATE TABLE  {prefix}filenames (filenameid INTEGER PRIMARY KEY AUTO_INCREMENT,filename TEXT(65536)) character set utf8;
CREATE INDEX  filenames_idx2 ON {prefix}filenames (filename(255));

DROP TABLE IF EXISTS {prefix}paths;
CREATE TABLE  {prefix}paths (pathid INTEGER PRIMARY KEY AUTO_INCREMENT,
       dirnameid INTEGER REFERENCES {prefix}dirnames(dirnameid),
       filenameid INTEGER REFERENCES {prefix}filenames(filenameid)) character set utf8;
CREATE INDEX  paths_idx2 ON {prefix}paths(dirnameid);
CREATE INDEX  paths_idx3 ON {prefix}paths(filenameid);

DROP TABLE IF EXISTS {prefix}hashes;
CREATE TABLE  {prefix}hashes (hashid INTEGER PRIMARY KEY AUTO_INCREMENT,hash TEXT(65536) NOT NULL) character set utf8;
CREATE INDEX  hashes_idx2 ON {prefix}hashes( hash(700));

DROP TABLE IF EXISTS {prefix}scans;
CREATE TABLE  {prefix}scans (scanid INTEGER PRIMARY KEY AUTO_INCREMENT,
                                      rootid INTEGER REFERENCES {prefix}roots(rootid),
                                      time DATETIME NOT NULL,
                                      duration INTEGER) character set utf8;
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
                                  scanid INTEGER REFERENCES {prefix}scans(scanid)) character set utf8;
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

class ScanDatabase(ABC):
    """Abstract class that represents database scans. A database scan is an inventory of the files under one or more roots.
    This class is then specialized for the MySQL and SQLite3 classes. 
    It would be great if we could do that specialization solely through dbfile, but there are some differences.
    Whenever possible, the implementaiton should go here, rather than have duplicate implementaitons in the subclasses.
    So all methods are not abstract methods, only those that require database-specific implementations.

    Each of these methods is tested in tests/scandb_tests.py for every implementation
    """
    def __init__(self, *, db, prefix="", auth=None):
        self.prefix  = prefix
        self.db      = db
        self.auth    = auth

    @abstractmethod
    def create_database(self):
        pass

    def csfra(self, cmd, vals=[]):
        """Call the db csfr method with the object's auth."""
        return self.db.csfr(self.auth, cmd, vals)

    # Manipulate roots

    def add_root(self, root):
        "Add a root, ignore if it is already there. "
        self.db.csfr(self.auth,
                     f"INSERT IGNORE INTO {self.prefix}roots (rootdir) VALUES ( %s )",
                     [root])
        self.db.commit()

    def get_roots(self):
        """Return a list of the roots in the database."""
        rows = self.db.csfr(self.auth,
                            "SELECT rootdir FROM {prefix}roots".format(prefix=self.prefix),[])
        return [row[0] for row in rows]

    def del_root(self, root):
        self.db.csfr(self.auth,
                     f"DELETE FROM {self.prefix}roots WHERE rootdir=%s",
                     [root])
        self.db.commit()        
        
    # Manipulate scans
    def list_scans(self):
        for (scanid, time) in self.csfra("SELECT scanid, time, rootid, rootdir FROM {prefix}scans NATURAL JOIN {prefix}roots"
                                         .format(prefix=self.prefix)):
            print(scanid, rootdir, time)

    def last_scan(self):
        return self.csfra("SELECT MAX(scanid) FROM {prefix}scans".format(prefix=self.prefix))[0][0]

    def all_files(self, scan0):
        results = self.db.csfr(self.auth,
                                 "SELECT fileid, pathid, rootid, size, dirnameid, dirname, filenameid, filename, mtime "
                                 "FROM {prefix}files NATURAL JOIN {prefix}paths NATURAL JOIN {prefix}dirnames NATURAL JOIN {prefix}filenames "
                                 "WHERE scanid={scanid}".format(scanid=scan0, prefix=self.prefix))
        for fileid, pathid, rootid, size, dirnameid, dirname, filenameid, filename, mtime in results:
            yield {"fileid": fileid, "pathid": pathid, "rootid": rootid, "size": size,
                   "dirnameid": dirnameid, "dirname": dirname, "filenameid": filenameid,
                   "filename": filename, "mtime": mtime}

    def new_files(self, scan0, scan1):
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
        return self.new_files(scan1, scan0)

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

    def duplicate_files(self, scan0):
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

    # TODO: renamed files only tracks last scanned file. Will consider
    # any file containing the same information as renamed if they have
    # different names.

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
        """Generate a report from time a to time b"""
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

class SQLite3ScanDatabase(ScanDatabase):
    def __init__(self, *, fname, prefix=""):
        super().__init__(db = dbfile.DBSqlite3(fname=fname), prefix=prefix)

    def create_database(self):
        self.db.create_schema(SQLITE3_SCHEMA)


class MySQLScanDatabase(ScanDatabase):
    def __init__(self, *, auth, prefix=""):
        super().__init__(db = dbfile.DBMySQL(auth), auth=auth, prefix=prefix)
    
    @classmethod
    def FromConfigFile(self,config_file):
        config = configparser.ConfigParser()
        config.read(config_file)
        print(list(config))
        auth   = dbfile.DBMySQLAuth.FromConfig(config[MYSQL_SERVER_SECTION])
        print("auth=",auth)
        prefix = config[FCHANGE_SECTION][TABLE_PREFIX]
        fcm    = MySQLScanDatabase(auth=auth, prefix=prefix)
        fcm.config = config
        return fcm
    
    def create_database(self):
        self.db.create_schema(MYSQL_SCHEMA.format(prefix=self.prefix))

