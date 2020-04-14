import tempfile
import sqlite3
import os
import sys

import scandb

TEST_SQLITE=True
TEST_MYSQL=True

DEFAULT_CONFIG_FILENAME = os.path.join( os.path.dirname(__file__), '../default.ini')
DIR1 = os.path.join( os.path.dirname(__file__), "data/DIR1")
DIR2 = os.path.join( os.path.dirname(__file__), "data/DIR2")


def make_database(sdb):
    sdb.create_database()
    sdb.add_root( DIR1 )
    sdb.add_root( DIR2 )

def check_get_enabled_roots(sdb):
    assert sdb.get_enabled_roots() == sorted([DIR1, DIR2])

def check_del_root(sdb):
    FOOBAR = "foobar/"
    sdb.add_root( FOOBAR )
    assert sdb.get_enabled_roots() == sorted([FOOBAR, DIR1, DIR2])
    sdb.del_root( FOOBAR )
    assert sdb.get_enabled_roots() == sorted([DIR1, DIR2])

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
    for pl in [('tests/data/DIR1', '12345.txt', 6),
                    ('tests/data/DIR1', '23456.txt', 6),
                    ('tests/data/DIR2', '23456.txt', 6),
                    ('tests/data/DIR2', '34567.txt', 6)]:
        assert pl in present


def check_database(sdb):
    check_get_enabled_roots(sdb)
    check_del_root(sdb)
    check_pathid(sdb)
    check_hashid(sdb)
    check_scan_enabled_roots(sdb)

def test_sqlite3_schema():
    with tempfile.NamedTemporaryFile(suffix='.dbfile') as tf:
        name = tf.name
        name = "db.db"
        os.unlink(name)
        sdb = scandb.SQLite3ScanDatabase(fname=name)
        sdb.create_database()


def test_create_database_sqlite3():
    """Test to make sure that the create database feature works"""
    with tempfile.NamedTemporaryFile(suffix='.dbfile') as tf:
        name = tf.name
        name = "db.db"
        os.unlink(name)
        sdb = scandb.SQLite3ScanDatabase(fname=name)
        make_database(sdb)
        del sdb

        sdb = scandb.SQLite3ScanDatabase(fname=name)
        check_database(sdb)
        del sdb

def test_create_database_mysql():
    """Test to make sure that the create database feature works"""
    sdb = scandb.MySQLScanDatabase.FromConfigFile(DEFAULT_CONFIG_FILENAME)
    make_database(sdb)
    del sdb

    sdb = scandb.MySQLScanDatabase.FromConfigFile(DEFAULT_CONFIG_FILENAME)
    check_database(sdb)
    del sdb


def test_create_database_mysql_prefix():
    """Test to make sure that the features work with a prefix set"""
    sdb = scandb.MySQLScanDatabase.FromConfigFile(DEFAULT_CONFIG_FILENAME, prefix="xxx")
    make_database(sdb)
    del sdb

    sdb = scandb.MySQLScanDatabase.FromConfigFile(DEFAULT_CONFIG_FILENAME, prefix="xxx")
    check_database(sdb)
    del sdb


