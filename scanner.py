"""
scanner.py

Part of the file system change detector.
Implements the scanner. Database agnostic.
"""

import sqlite3
import zipfile
from datetime import datetime

import fchange
from ctools.dbfile import *
from ctools.s3 import *

DIR_COMMIT_RATE  =  10  # commit every 10 directories
FILE_COMMIT_RATE = 100  # commit every 100 files

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
    except IOError:
        return None

from abc import ABC, abstractmethod
class Scanner(ABC):
    """Abstract Base Class to scan a directory and store the results in the database specified by the provided scandb class.."""
    def __init__(self, sdm, *, debug=False ):
        self.sdm   = sdm        # scan database manager (a subclass of ScanDatabase(ABC))
        self.debug = debug
        self.filecount = 0
        self.dircount = 0

    def get_file_hashid(self, *, f=None, pathname=None, file_size, pathid=None, mtime, hexdigest=None):
        """Given an open file or a filename, Return the MD5 hexdigest."""
        if pathid is None:
            if pathname is None:
                raise RuntimeError("pathid and pathname are both None")
            pathid = self.sdm.get_pathid(pathname)

        # See if this file with this length is in the database.
        # If not, we will hash the file and enter it.
        # This means that we are trusting that the mtime gets updated if the file contents change.
        # We might also want to look at the file generation count.
        hashid = self.sdm.get_hashid_for_pms(pathid, mtime, file_size)
        if hashid is not None:
            return hashid

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
        # Put the hash into the database and return it
        return self.sdm.get_hashid_for_hexdigest( hexdigest )
        

    def insert_file(self, *, path, mtime, file_size, handle=None, hexdigest=None):
        """@mtime in time_t"""
        pathid = self.sdm.get_pathid(path)
        try:
            hashid = self.get_file_hashid(pathid=pathid,mtime=mtime,file_size=file_size,f=handle, hexdigest=hexdigest)
        except PermissionError as e:
            return
        except OSError as e:
            return

        self.sdm.add_pmshs(pathid, mtime, file_size, hashid, self.scanid)

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
            self.insert_file(path=path+"/"+zi.filename, mtime=mtime,
                             file_size=zi.file_size, handle=zf.open(zi.filename,"r"))

    @abstractmethod
    def ingest_walk(self, start_path):
        pass
        
    def ingest(self, start_path):
        """Ingest everything from the root"""
        self.t0 = time.time()
        self.scanid = self.sdm.get_scanid( self.t0 )
        self.ingest_walk( start_path )
        self.t1 = time.time()
        self.sdm.ingest_done(self.scanid, self.t1 - self.t0)

class FileScanner(Scanner):
    """Scanner for native file system.that Extends the Scanner class and implements MySQL db storage"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args,**kwargs)

    def ingest_walk(self, start_path):
        """Walk the local file system and go inside ZIP files"""
        for (dirpath, dirnames, filenames) in os.walk(start_path):
            for filename in filenames:
                #
                # Get full path and process
                filename_path = os.path.join(dirpath, filename)
                self.process_filepath(filename_path)

                # See if this is a zipfile. If so, process it
                zf = open_zipfile(filename_path)
                if zf:
                    self.process_zipfile(root, filename_path, zf)
                self.filecount += 1
            self.dircount  += 1

class S3Scanner(Scanner):
    def __init__(self, *args, **kwargs):
        super().__init__(*args,**kwargs)

    def ingest_walk(self, root):
        """Walk S3 bucket. Do not go inside ZIP files"""
        from ctools import s3
        (bucket,key) = s3.get_bucket_key(root)
        for obj in s3.list_objects(bucket,key):
            # {'LastModified': '2019-03-28T21:36:51.000Z', 
            #   'ETag': '"46610609053db79a94c4bd29cad8f4ff"', 
            #   'StorageClass': 'STANDARD', 
            #   'Key': 'a/b/c/whatever.txt', 
            #   'Size': 31838630}

            self.insert_file( path=obj['Key'], mtime=obj['LastModified'], file_size=obj['Size'], hexdigest=obj['ETag'] )
            self.filecount += 1
            if (self.args.limit is not None) and (self.filecount > self.args.limit):
                return
            if self.filecount % FILE_COMMIT_RATE==0:
                self.conn.commit()

