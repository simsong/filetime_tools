import tempfile
import sqlite3
import os

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

def check_get_roots(sdb):
    assert sdb.get_roots() == sorted([DIR1, DIR2])

def check_del_root(sdb):
    FOOBAR = "foobar/"
    sdb.add_root( FOOBAR )
    assert sdb.get_roots() == sorted([FOOBAR, DIR1, DIR2])
    sdb.del_root( FOOBAR )
    assert sdb.get_roots() == sorted([DIR1, DIR2])

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

def check_scan_roots(sdb):
    sdb.scan_roots()
    last_scan = sdb.last_scan()
    objs = list(sdb.all_files(last_scan))
    # Both objects should be in the same directory, but they are different
    assert len(objs) == 2
    assert objs[0]['dirnameid'] == objs[1]['dirnameid']
    assert objs[0]['pathid']    != objs[1]['pathid']
    assert objs[0]['size']      == objs[1]['size'] == 6
    assert list(sorted([objs[0]['filename'],objs[1]['filename']])) == ['23456.txt','34567.txt']


def check_database(sdb):
    check_get_roots(sdb)
    check_del_root(sdb)
    check_pathid(sdb)
    check_hashid(sdb)
    check_scan_roots(sdb)


def test_create_database_sqlite3():
    """Test to make sure that the create database feature works"""
    with tempfile.NamedTemporaryFile(suffix='.dbfile') as tf:
        name = tf.name
        name = "db.db"
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


