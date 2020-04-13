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

def check_database(sdb):
    check_get_roots(sdb)
    check_del_root(sdb)



def test_create_database_sqlite3():
    """Test to make sure that the create database feature works"""
    with tempfile.NamedTemporaryFile(suffix='.dbfile') as tf:
        sdb = scandb.SQLite3ScanDatabase(fname=tf.name)
        make_database(sdb)
        del sdb

        sdb = scandb.SQLite3ScanDatabase(fname=tf.name)
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


