############################################################
############################################################

import time

def hash_file(mypath):
    """High performance file hasher. Hash a file and return the hexdigest."""
    from hashlib import md5
    m = md5()
    with open(mypath, "rb") as f:
        while True:
            buf = f.read(65535)
            if not buf:
                break
            m.update(buf)
    return m.hexdigest()

def is_zipfile(path):
    """Check to see if path is a zipfile."""
    return False

class Scanner(object):
    """Class to scan a directory and store the results in the database."""
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
        self.c.execute("INSERT or IGNORE INTO paths (dirnameid,filenameid) VALUES (?,?);",
                       (dirnameid, filenameid,))
        self.c.execute("SELECT pathid FROM paths WHERE dirnameid=? AND filenameid=?",
                       (dirnameid, filenameid,))
        return self.c.fetchone()[0]

    def process_zipfile(self, scanid, path):
        """Scan a zip file and insert it into the database"""

    def process_filepath(self, scanid, path):
        """ Add the file to the database database.
        If it is there and the mtime hasn't been changed, don't re-hash."""

        try:
            st = os.stat(path)
        except FileNotFoundError as e:
            return
        pathid = self.get_pathid(path)

        # See if this file with this length is in the database.
        # If not, we will hash the file and enter it.
        # This means that we are trusting that the mtime gets updated if the file contents change.
        # We might also want to look at the file generation count.
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
            if args.vfiles:
                print("{} {}".format(path,st.st_size))
            if is_zipfile(path):
                self.process_zipfile(scanid,path)
        except PermissionError as e:
            pass
        except OSError as e:
            pass

    def ingest(self, root):
        """Ingest everything from the root"""

        self.c = self.conn.cursor()
        self.c.execute("BEGIN TRANSACTION")

        scanid = self.get_scanid(SLGSQL.iso_now())

        count = 0
        dircount = 0
        t0 = time.time()
        for (dirpath, dirnames, filenames) in os.walk(root):
            if args.vdirs:
                print("{}".format(dirpath), end='\n' if args.vfiles else '')
            for filename in filenames:
                self.process_filepath(scanid, os.path.join(dirpath, filename))
            if args.vdirs:
                print("\r{}:  {}".format(dirpath,len(filenames)))
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

