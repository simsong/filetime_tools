#
# Methods that return File(pathid,dirname,filename) for searches
#

import datetime
import os
import os.path
import base64

class SLGSQL_MySQL:
    def iso_now():
        """Report current time in ISO-8601 format"""
        return str(datetime.datetime.now())[0:19]

    def create_schema(schema,conn,name):
        """Create the schema if it doesn't exist."""
        schema = schema.replace('filetime_tools', name)
        with conn.cursor() as cursor:
            for line in schema.split(";"):
                cursor.execute(line)

    def execselect(conn, sql, vals=()):
        """Execute a SQL query and return the first line"""
        with conn.cursor() as cursor:
            cursor.execute(sql, vals)
            return cursor.fetchone()

class DBFile_MySQL:
    """Class models a file in the database; searches return these File objects."""
    # __slots__ = ['fileid', 'pathid', 'dirnameid', 'dirname', 'filenameid', 'filename', 'size', 'hashid', 'mtime']
    __slots__ = ['fileid', 'pathid', 'size', 'dirnameid', 'dirname', 'filenameid', 'filename', 'mtime']
    def __init__(self, row):
        for i, name in enumerate(self.__slots__):
            try:
                setattr(self, name, row[i])
            except (IndexError, KeyError) as e:
                setattr(self, name, None)

    def __repr__(self):
        return "File<" + ",".join(["{}:{}".format(name, getattr(self, name)) for name in self.__slots__]) + ">"

    def get_path(self, conn=None, dbname=None):
        """Return the full pathname. May need an SQL connection to answer question"""
        if self.dirname is None:
            self.get_dirname(conn, dbname)
        
        if self.filename is None:
            self.get_filename(conn, dbname)
        
        return os.path.join(self.dirname,  self.filename)

    def cache_pathid(self, conn, dbname):
        if (self.dirname == None and self.dirnameid == None) or \
                (self.filename == None and self.fileid == None):
            with conn.cursor() as cursor:
                cursor.execute('SELECT fileid,dirname,filename FROM `{}`.files '
                'NATURAL JOIN paths NATURAL JOIN dirnames NATURAL JOIN filenames '
                'WHERE fileid={}'.format(dbname, self.pathid))
                (self.fileid, self.dirname, self.filename) = cursor.fetchone()

    def get_filename(self, conn, dbname):
        """Returns the filename. Get it if we don't have it"""
        self.cache_pathid(conn, dbname)
        if not self.filename:
            c = conn.cursor()
            c.execute("SELECT filename FROM `{}`.filenames WHERE filenameid={}".format(dbname, self.filenameid))
            self.filename = c.fetchone()[0]
        return self.filename

    def get_dirname(self, conn, dbname):
        """Returns the dirname. Get it if we don't have it"""
        self.cache_pathid(conn,dbname)
        if not self.dirname:
            c = conn.cursor()
            c.execute("SELECT dirname FROM `{}`.dirnames WHERE dirnameid={}", (dbname, self.dirnameid,))
            self.dirname = c.fetchone()[0]
        return self.dirname

    def get_data(self, conn):
        """Returns the data in the file"""
        with open(self.get_path(), 'rb') as f:
            return f.read()

    def get_data_base64str(self, conn):
        return base64.b64encode(self.get_data(conn)).decode('utf-8','ignore')
