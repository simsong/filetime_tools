import tempfile
import sqlite3
import os

import scandb

TEST_SQLITE=True
TEST_MYSQL=True


DEFAULT_CONFIG_FILENAME = os.path.join( os.path.dirname(__file__), '../default.ini')

def test_create_database_sqlite3():
    """Test to make sure that the create database feature works"""
    with tempfile.NamedTemporaryFile(suffix='.dbfile') as tf:
        sdb = scandb.SQLite3ScanDatabase(fname=tf.name)
        sdb.create_database()
        sdb.add_root("test/")
        del sdb

        sdb = scandb.SQLite3ScanDatabase(fname=tf.name)
        assert sdb.get_roots() == ["test/"]

def test_create_database_mysql():
    """Test to make sure that the create database feature works"""
    sdb = scandb.MySQLScanDatabase.FromConfigFile(DEFAULT_CONFIG_FILENAME)
    sdb.create_database()
    sdb.add_root("test/")
    del sdb

    sdb = scandb.MySQLScanDatabase.FromConfigFile(DEFAULT_CONFIG_FILENAME)
    assert sdb.get_roots() == ["test/"]
