### scanner.py
###
### part of the file system change

############################################################
############################################################


# SQLite3 schema
SQLITE3_SCHEMA = \
    """
CREATE TABLE IF NOT EXISTS metadata (key VARCHAR(255) PRIMARY KEY,value VARCHAR(255) NOT NULL);

CREATE TABLE IF NOT EXISTS dirnames (dirnameid INTEGER PRIMARY KEY,dirname TEXT NOT NULL UNIQUE);
CREATE INDEX IF NOT EXISTS dirnames_idx1 ON dirnames(dirnameid);
CREATE INDEX IF NOT EXISTS dirnames_idx2 ON dirnames(dirname);

CREATE TABLE IF NOT EXISTS filenames (filenameid INTEGER PRIMARY KEY,filename TEXT NOT NULL UNIQUE);
CREATE INDEX IF NOT EXISTS filenames_idx1 ON filenames(filenameid);
CREATE INDEX IF NOT EXISTS filenames_idx2 ON filenames(filename);

CREATE TABLE IF NOT EXISTS paths (pathid INTEGER PRIMARY KEY,dirnameid INTEGER REFERENCES dirnames(dirnameid), filenameid INTEGER REFERENCES filenames(filenameid));
CREATE INDEX IF NOT EXISTS paths_idx1 ON paths(pathid);
CREATE INDEX IF NOT EXISTS paths_idx2 ON paths(dirnameid);
CREATE INDEX IF NOT EXISTS paths_idx3 ON paths(filenameid);

CREATE TABLE IF NOT EXISTS hashes (hashid INTEGER PRIMARY KEY,hash TEXT NOT NULL UNIQUE);
CREATE INDEX IF NOT EXISTS hashes_idx1 ON hashes(hashid);
CREATE INDEX IF NOT EXISTS hashes_idx2 ON hashes(hash);

CREATE TABLE IF NOT EXISTS scans (scanid INTEGER PRIMARY KEY,time DATETIME NOT NULL UNIQUE,duration INTEGER);
CREATE INDEX IF NOT EXISTS scans_idx1 ON scans(scanid);
CREATE INDEX IF NOT EXISTS scans_idx2 ON scans(time);

CREATE TABLE IF NOT EXISTS roots (rootid INTEGER PRIMARY KEY,scanid INT REFERENCES scans(scanid), dirnameid INT REFERENCES dirnames(dirnameid));
CREATE INDEX IF NOT EXISTS roots_idx1 ON roots(rootid);
CREATE INDEX IF NOT EXISTS roots_idx2 ON roots(scanid);
CREATE INDEX IF NOT EXISTS roots_idx3 ON roots(dirnameid);

CREATE TABLE IF NOT EXISTS files (fileid INTEGER PRIMARY KEY,
                                  pathid INTEGER REFERENCES paths(pathid),
                                  mtime INTEGER NOT NULL, 
                                  size INTEGER NOT NULL, 
                                  hashid INTEGER REFERENCES hashes(hashid), 
                                  scanid INTEGER REFERENCES scans(scanid));
CREATE INDEX IF NOT EXISTS files_idx0 ON files(fileid);
CREATE INDEX IF NOT EXISTS files_idx1 ON files(pathid);
CREATE INDEX IF NOT EXISTS files_idx2 ON files(mtime);
CREATE INDEX IF NOT EXISTS files_idx3 ON files(size);
CREATE INDEX IF NOT EXISTS files_idx4 ON files(hashid);
CREATE INDEX IF NOT EXISTS files_idx5 ON files(scanid);
CREATE INDEX IF NOT EXISTS files_idx6 ON files(scanid,hashid);


"""

import os
import sys
import zipfile
import time
from dbfile import DBFile, SLGSQL


COMMIT_RATE = 10  # commit every 10 directories

def hash_file(f):
    """High performance file hasher. Hash a file and return the MD5 hexdigest."""
    from hashlib import md5
    m = md5()
    while True:
        buf = f.read(65535)
        if not buf:
            return m.hexdigest()
        m.update(buf)

def open_zipfile(path):
    """Check to see if path is a zipfile.
    If so, return an open zipfile"""
    if path.lower().endswith(".jar"):           # Don't peek inside jar files
        return None
    try:
        return zipfile.ZipFile(path,mode="r")
    except zipfile.BadZipfile:
        return None
    except zipfile.LargeZipFile:
        return None
    except OSError as e:
        print("OSError: {}".format(e))
        return None

class Scanner(object):
    """Class to scan a directory and store the results in the database."""
    def __init__(self, conn):
        self.conn = conn
        self.c = self.conn.cursor()

    def get_hashid(self, hash):
        self.c.execute("INSERT or IGNORE INTO hashes (hash) VALUES (?);", (hash,))
        self.c.execute("SELECT hashid FROM hashes WHERE hash=?", (hash,))
        return self.c.fetchone()[0]

    def get_dirnameid(self, dirname):
        self.c.execute("INSERT or IGNORE INTO dirnames (dirname) VALUES (?);", (dirname,))
        self.c.execute("SELECT dirnameid FROM dirnames WHERE dirname=?", (dirname,))
        return self.c.fetchone()[0]

    def get_filenameid(self, filename):
        assert "/" not in filename
        self.c.execute("INSERT or IGNORE INTO filenames (filename) VALUES (?);", (filename,))
        self.c.execute("SELECT filenameid FROM filenames WHERE filename=?", (filename,))
        return self.c.fetchone()[0]

    def get_pathid(self, path):
        (dirname, filename) = os.path.split(path)
        dirnameid = self.get_dirnameid(dirname)
        filenameid = self.get_filenameid(filename)

        # pathid
        self.c.execute("INSERT or IGNORE INTO paths (dirnameid,filenameid) VALUES (?,?);",
                       (dirnameid, filenameid,))
        self.c.execute("SELECT pathid FROM paths WHERE dirnameid=? AND filenameid=?",
                       (dirnameid, filenameid,))
        return self.c.fetchone()[0]

    def get_scanid(self, now, root):
        dirnameid = self.get_dirnameid(root)
        self.c.execute("INSERT or IGNORE INTO scans (time) VALUES (?);", (now,))
        self.c.execute("SELECT scanid FROM scans WHERE time=?", (now,))
        scanid = self.c.fetchone()[0]
        self.c.execute("INSERT INTO roots (scanid,dirnameid) values (?,?)",(scanid,dirnameid))
        self.conn.commit()
        return scanid

    def insert_file(self, path, mtime, file_size, handle, scanid):
        """@mtime in time_t"""

        if self.verbose_files:
            print("insert_file({})".format(path))

        pathid = self.get_pathid(path)

        # See if this file with this length is in the database.
        # If not, we will hash the file and enter it.
        # This means that we are trusting that the mtime gets updated if the file contents change.
        # We might also want to look at the file generation count.
        self.c.execute("SELECT hashid FROM files WHERE pathid=? AND mtime=? AND size=? LIMIT 1",
                       (pathid, mtime, file_size))
        row = self.c.fetchone()
        try:
            if row:
                (hashid,) = row
            else:
                hashid = self.get_hashid(hash_file(handle))
            self.c.execute("INSERT INTO files (pathid,mtime,size,hashid,scanid) VALUES (?,?,?,?,?)",
                           (pathid, mtime, file_size, hashid, scanid))
            if self.verbose_files:
                print("{} {}".format(path, file_size))
        except PermissionError as e:
            pass
        except OSError as e:
            pass

    def process_filepath(self, scanid, path):
        """ Add the file to the database database.
        If it is there and the mtime hasn't been changed, don't re-hash."""

        try:
            st = os.stat(path)
        except FileNotFoundError as e:
            return

        self.insert_file(path,st.st_mtime,st.st_size,open(path,"rb"),scanid)

    def process_zipfile(self, scanid, path, zf):
        """Scan a zip file and insert it into the database"""
        for zi in zf.infolist():
            mtime = time.mktime(zi.date_time + (0,0,0))
            self.insert_file(path+"/"+zi.filename, mtime, zi.file_size,zf.open(zi.filename,"r"), scanid)

    def ingest(self, root, ignore_ext=[], only_ext=[], verbose_dirs=False, verbose_files=False):
        """Ingest everything from the root"""

        self.verbose_dirs = verbose_dirs
        self.verbose_files = verbose_files
        print("verbose_files=",verbose_files)

        if not os.path.exists(root):
            raise FileNotFoundError(root)
        if not os.path.isdir(root):
            raise FileNotFoundError("{} is not a directory".format(root))

        only_ext_lower   = [ext.lower() for ext in only_ext]
        ignore_ext_lower = [ext.lower() for ext in ignore_ext]

        self.c = self.conn.cursor()
        self.c.execute("BEGIN TRANSACTION")

        scanid = self.get_scanid(SLGSQL.iso_now(),root)

        count = 0
        dircount = 0
        t0 = time.time()
        for (dirpath, dirnames, filenames) in os.walk(root):
            if verbose_dirs or verbose_files:
                print("scanning {}".format(dirpath))
            for filename in filenames:
                filename_lower = filename.lower()
                if only_ext!=[''] and not any([filename_lower.endswith(ext) for ext in only_ext_lower]):
                    if verbose_files:
                        print("Will not include {} (not in only_ext)".format(filename))
                    continue
                if ignore_ext_lower!=[''] and any([filename_lower.endswith(ext) for ext in ignore_ext_lower]):
                    if verbose_files:
                        print("Will not include {} (in {})".format(filename,ignore_ext_lower))
                    continue

                path = os.path.join(dirpath, filename)
                if not os.path.isfile(path):
                    continue
                self.process_filepath(scanid, path)
                zf = open_zipfile(path)
                if zf:
                    self.process_zipfile(scanid,path, zf)

            if verbose_dirs:
                print("\r{}: scanned {} files".format(dirpath,len(filenames)))
            count += len(filenames)
            dircount += 1
            if dircount % COMMIT_RATE == 0:
                self.conn.commit()

        self.conn.commit()
        t1 = time.time()
        self.c.execute("UPDATE scans SET duration=? WHERE scanid=?", (t1 - t0, scanid))
        self.conn.commit()
        print("Total files added to database: {}".format(count))
        print("Total directories scanned:     {}".format(dircount))
        print("Total time: {}".format(int(t1 - t0)))


###################### END OF SCANNER CLASS ######################
##################################################################

