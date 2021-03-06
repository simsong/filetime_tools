import tempfile
import sqlite3
import os
import sys

import scandb

TEST_SQLITE=True
TEST_MYSQL=True

USE_LOCAL_DB=True
LOCAL_DB_NAME='db.db'

DEFAULT_CONFIG_FILENAME = os.path.join( os.path.dirname(__file__), '../default.ini')
DIR1 = os.path.join( os.path.dirname(__file__), "data/DIR1")
DIR2 = os.path.join( os.path.dirname(__file__), "data/DIR2")
FOOBAR = "foobar/"
DATA = [
    ('tests/data/DIR1', '12345.txt', 6),
    ('tests/data/DIR1', '23456.txt', 6),
    ('tests/data/DIR2', '23456.txt', 6),
    ('tests/data/DIR2', '34567.txt', 6)]


def make_database(sdb):
    sdb.create_database()
    sdb.add_root( DIR1 )
    sdb.add_root( DIR2 )

def check_get_enabled_roots(sdb):
    assert sdb.get_enabled_roots() == set([DIR1, DIR2])

def check_del_root(sdb):
    sdb.add_root( FOOBAR )
    assert sdb.get_enabled_roots() == set([FOOBAR, DIR1, DIR2])
    sdb.del_root( FOOBAR )
    assert sdb.get_enabled_roots() == set([DIR1, DIR2])

def check_pathid(sdb):
    h1 = sdb.get_pathid("a/b/c")
    h2 = sdb.get_pathid("a/b/d")
    h3 = sdb.get_pathid("a/b/c")
    assert isinstance(h1,int)
    assert isinstance(h2,int)
    assert isinstance(h3,int)
    assert h1!=h2
    assert h1==h3

def check_hashid(sdb):
    h1 = sdb.get_hashid_for_hexdigest("0123456789")
    h2 = sdb.get_hashid_for_hexdigest("0123456789000")
    h3 = sdb.get_hashid_for_hexdigest("0123456789")
    assert isinstance(h1,int)
    assert isinstance(h2,int)
    assert isinstance(h3,int)
    assert h1!=h2
    assert h1==h3

def check_scan_enabled_roots(sdb):
    sdb.add_root( DIR1 )
    sdb.add_root( DIR2 )
    sdb.del_root( FOOBAR )
    assert set(sdb.get_enabled_roots()) == set([DIR1, DIR2])
    sdb.scan_enabled_roots()
    last_scan = sdb.last_scan()
    objs = list(sdb.all_files(last_scan))
    # Both objects should be in the same directory, but they are different
    if len(objs)!=4:
        for obj in objs:
            print(obj,file=sys.stderr)
        raise RuntimeError("got wrong number of objects back")
    # Make sure these four objects are present
    present = [("/".join(obj['dirname'].split("/")[-3:]),obj['filename'],obj['size']) for obj in objs]
    for pl in DATA:
        assert pl in present

def check_find_dups_after_scan(sdb):
    dups = list(sdb.duplicate_files())
    # There is one set of dups. They have the same filenames but different directories
    assert len(dups)==1
    assert dups[0][0]['filename']==dups[0][1]['filename']=='23456.txt'
    assert dups[0][0]['dirname'] != dups[0][1]['dirname']

def check_database(sdb):
    check_get_enabled_roots(sdb)
    check_del_root(sdb)
    check_pathid(sdb)
    check_hashid(sdb)
    check_scan_enabled_roots(sdb)
    check_find_dups_after_scan(sdb)

def test_sqlite3_schema():
    with tempfile.NamedTemporaryFile(suffix='.dbfile') as tf:
        if USE_LOCAL_DB:
            name = LOCAL_DB_NAME
        else:
            name = tf.name 
        if os.path.exists(name):
            os.unlink(name)
        sdb = scandb.SQLite3ScanDatabase(fname=name, debug=True)
        sdb.create_database()


def test_create_database_sqlite3():
    """Test to make sure that the create database feature works"""
    with tempfile.NamedTemporaryFile(suffix='.dbfile') as tf:
        if USE_LOCAL_DB:
            name = LOCAL_DB_NAME
        else:
            name = tf.name 
        if os.path.exists(name):
            os.unlink(name)
        sdb = scandb.SQLite3ScanDatabase(fname=name, debug=True)
        make_database(sdb)
        del sdb

        sdb = scandb.SQLite3ScanDatabase(fname=name, debug=True)
        check_database(sdb)
        del sdb

def test_create_database_mysql():
    """Test to make sure that the create database feature works"""
    sdb = scandb.MySQLScanDatabase.FromConfigFile(DEFAULT_CONFIG_FILENAME,debug=True)
    make_database(sdb)
    del sdb

    sdb = scandb.MySQLScanDatabase.FromConfigFile(DEFAULT_CONFIG_FILENAME,debug=True)
    check_database(sdb)
    del sdb


def test_create_database_mysql_prefix():
    """Test to make sure that the features work with a prefix set"""
    sdb = scandb.MySQLScanDatabase.FromConfigFile(DEFAULT_CONFIG_FILENAME, prefix="xxx", debug=True)
    make_database(sdb)
    del sdb

    sdb = scandb.MySQLScanDatabase.FromConfigFile(DEFAULT_CONFIG_FILENAME, prefix="xxx", debug=True)
    check_database(sdb)
    del sdb


