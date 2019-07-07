#
# Methods that return File(pathid,dirname,filename) for searches
#

import datetime
import os
import os.path

class SLGSQL:
    def iso_now():
        """Report current time in ISO-8601 format"""
        return datetime.datetime.now().isoformat()[0:19]

    def create_schema(schema,conn):
        """Create the schema if it doesn't exist."""
        c = conn.cursor()
        for line in schema.split(";"):
            c.execute(line)

    def execselect(conn, sql, vals=()):
        """Execute a SQL query and return the first line"""
        c = conn.cursor()
        c.execute(sql, vals)
        return c.fetchone()

class DBFile:
    """Class models a file in the database; searches return these File objects."""
    __slots__ = ['fileid', 'mtime', 'pathid', 'dirnameid', 'dirname', 'filenameid', 'filename', 'size', 'hashid']

    def __init__(self, row):
        for name in self.__slots__:
            try:
                setattr(self, name, row[name])
            except (IndexError, KeyError) as e:
                setattr(self, name, None)

    def __repr__(self):
        return "File<" + ",".join(["{}:{}".format(name, getattr(self, name)) for name in self.__slots__]) + ">"

    def get_path(self, conn=None):
        """Return the full pathname. May need an SQL connection to answer question"""
        if self.dirname is None:
            self.get_dirname(conn)
        
        if self.filename is None:
            self.get_filename(conn)
        
        return os.path.join(self.dirname,  self.filename)

    def cache_pathid(self, conn):
        if (self.dirname == None and self.dirnameid == None) or \
                (self.filename == None and self.fileid == None):
            c = conn.cursor()
            c.execute("SELECT fileid,dirname,filename FROM files " +
                      "NATURAL JOIN paths NATURAL JOIN dirnames NATURAL JOIN filenames " +
                      "WHERE fileid=?", (self.pathid,))
            (self.fileid, self.dirname, self.filename) = c.fetchone()

    def get_filename(self, conn):
        """Returns the filename. Get it if we don't have it"""
        self.cache_pathid(conn)
        if not self.filename:
            c = conn.cursor()
            c.execute("SELECT filename FROM filenames WHERE filenameid=?", (self.filenameid,))
            self.filename = c.fetchone()[0]
        return self.filename

    def get_dirname(self, conn):
        """Returns the dirname. Get it if we don't have it"""
        self.cache_pathid(conn)
        if not self.dirname:
            c = conn.cursor()
            c.execute("SELECT dirname FROM dirnames WHERE dirnameid=?", (self.dirnameid,))
            self.dirname = c.fetchone()[0]
        return self.dirname


