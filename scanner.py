### scanner.py
###
### part of the file system change
### scans the file system

############################################################
############################################################


import os
import sys
import zipfile
import time
from dbfile import DBFile, SLGSQL

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

class Scanner(object):
    """Class to scan a directory and store the results in the database."""
    def __init__(self, conn, args):
        self.conn = conn
        self.args = args
        self.filecount = 0
        self.dircount = 0

    def get_hashid(self, hexhash):
        self.conn.execute("INSERT or IGNORE INTO hashes (hash) VALUES (?);", (hexhash,))
        for row in self.conn.execute("SELECT hashid FROM hashes WHERE hash=? LIMIT 1", (hexhash,)):
            return row[0]

    def get_scanid(self, now):
        self.conn.execute("INSERT or IGNORE INTO scans (time) VALUES (?);", (now,))
        for row in self.conn.execute("SELECT scanid FROM scans WHERE time=? LIMIT 1", (now,)):
            return row[0]

    def get_pathid(self,  path):
        (dirname, filename) = os.path.split(path)
        # dirname
        self.conn.execute("INSERT or IGNORE INTO dirnames (dirname) VALUES (?);", (dirname,))
        for row in self.conn.execute("SELECT dirnameid FROM dirnames WHERE dirname=? LIMIT 1", (dirname,)):
            dirnameid = row[0]

        # filename
        self.conn.execute("INSERT or IGNORE INTO filenames (filename) VALUES (?);", (filename,))
        for row in self.conn.execute("SELECT filenameid FROM filenames WHERE filename=?", (filename,)):
            filenameid = row[0]

        # pathid
        self.conn.execute("INSERT or IGNORE INTO paths (dirnameid,filenameid) VALUES (?,?);",
                          (dirnameid, filenameid,))
        for row in self.conn.execute("SELECT pathid FROM paths WHERE dirnameid=? AND filenameid=? LIMIT 1",
                                     (dirnameid, filenameid,)):
            return row[0]
        raise RuntimeError(f"no pathid found for {dirnameid},{filenameid}")

    def get_file_hashid(self, *, f=None, pathname=None, file_size, pathid=None, mtime, hexdigest=None):
        """Given an open file or a filename, Return the MD5 hexdigest."""
        if pathid is None:
            if pathname is None:
                raise RuntimeError("pathid and pathname are both None")
            pathid = self.get_pathid(pathname)

        # See if this file with this length is in the database.
        # If not, we will hash the file and enter it.
        # This means that we are trusting that the mtime gets updated if the file contents change.
        # We might also want to look at the file generation count.
        for row in self.conn.execute("SELECT hashid FROM files WHERE pathid=? AND mtime=? AND size=? LIMIT 1",
                                     (pathid, mtime, file_size)):
            return row[0]

        # Hashid is not in the database. Hash the file if we don't have the hash
        if hexdigest is None:
            if f is None:
                f = open(pathname,'rb')

            from hashlib import md5
            m = md5()
            while True:
                buf = f.read(65535)
                if not buf:
                    break
                m.update(buf)
            hexdigest = m.hexdigest()
        return self.get_hashid( hexdigest )
        

    def insert_file(self, *, path, mtime, file_size, handle=None, hexdigest=None):
        """@mtime in time_t"""
        pathid = self.get_pathid(path)

        try:
            hashid = self.get_file_hashid(pathid=pathid,mtime=mtime,file_size=file_size,f=handle, hexdigest=hexdigest)
        except PermissionError as e:
            return
        except OSError as e:
            return

        self.conn.execute("INSERT INTO files (pathid,mtime,size,hashid,scanid) VALUES (?,?,?,?,?)",
                       (pathid, mtime, file_size, hashid, self.scanid))
        if self.args.vfiles:
            print("{} {}".format(path, file_size))


    def process_filepath(self, path):
        """ Add the file to the database database.
        If it is there and the mtime hasn't been changed, don't re-hash."""

        try:
            st = os.stat(path)
        except FileNotFoundError as e:
            return

        self.insert_file(path=path, mtime=st.st_mtime, file_size=st.st_size, handle=open(path,"rb"))

    def process_zipfile(self, path, zf):
        """Scan a zip file and insert it into the database"""
        for zi in zf.infolist():
            mtime = time.mktime(zi.date_time + (0,0,0))
            self.insert_file(path=path+"/"+zi.filename, mtime=mtime, file_size=zi.file_size, handle=zf.open(zi.filename,"r"))

    def ingest_start(self, root):
        print("ingest_start")
        self.scanid = self.get_scanid(SLGSQL.iso_now())


    def ingest_walk(self, root):
        """Walk the local file system and go inside ZIP files"""
        for (dirpath, dirnames, filenames) in os.walk(root):
            #
            # see https://docs.python.org/3.7/library/sqlite3.html#using-the-connection-as-a-context-manager
            try:
                with self.conn:
                    if self.args.vdirs:
                        print("{}".format(dirpath), end='\n' if self.args.vfiles else '')

                    for filename in filenames:
                        #
                        # Get full path and process
                        filename_path = os.path.join(dirpath, filename)
                        self.process_filepath(filename_path)

                        # See if this is a zipfile. If so, process it
                        zf = open_zipfile(filename_path)
                        if zf:
                            self.process_zipfile( filename_path, zf)
                        self.filecount += 1
                        if (self.args.limit is not None) and (self.filecount > self.args.limit):
                            return

                    if self.args.vdirs:
                        print("\r{}:  {}".format(dirpath,len(filenames)))
                    self.dircount  += 1
            except sqlite3.IntegrityError as e:
                print(e)

    def ingest_done(self, root):
        with self.conn:
            self.conn.execute("UPDATE scans SET duration=? WHERE scanid=?", (self.t1 - self.t0, self.scanid))
        print("Total files added to database: {}".format(self.filecount))
        print("Total directories scanned:     {}".format(self.dircount))
        print("Total time: {}".format(int(self.t1 - self.t0)))

    def ingest(self, root):
        """Ingest everything from the root"""

        self.ingest_start(root)
        self.t0 = time.time()
        self.ingest_walk(root)
        self.t1 = time.time()
        self.ingest_done(root)


class S3Scanner(Scanner):
    def __init__(self, *args, **kwargs):
        super().__init__(*args,**kwargs)

    def ingest_walk(self, root):
        """Scan S3"""
        (bucket,key) = s3.get_bucket_key(root)
        for obj in s3.list_objects(bucket,key):
            # {'LastModified': '2019-03-28T21:36:51.000Z', 
            #   'ETag': '"46610609053db79a94c4bd29cad8f4ff"', 
            #   'StorageClass': 'STANDARD', 
            #   'Key': 'a/b/c/whatever.txt', 
            #   'Size': 31838630}

            print(obj)
            self.insert_file( path=obj['Key'], mtime=obj['LastModified'], file_size=obj['Size'], hexdigest=obj['ETag'] )
            self.filecount += 1
            if (self.args.limit is not None) and (self.filecount > self.args.limit):
                return
            if self.filecount %100==0:
                self.conn.commit()
        print("Scan S3: ",root)
        

