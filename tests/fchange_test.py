import tempfile
import sqlite3
import os

import fchange
from dbfile import SLGSQL

TEST_SQLITE=True
TEST_MYSQL=True


DEFAULT_FILENAME = os.path.join( os.path.dirname(__file__), '../default.ini')

def test_create_database_sqlite3():
    """Test to make sure that the create database feature works"""
    with tempfile.NamedTemporaryFile(suffix='.dbfile') as tf:
        fcm = fchange.SQLite3FileChangeManager(fname=tf.name)
    
        fcm.create_database()
        fcm.add_root("test/")
        del fcm

        fcm = fchange.SQLite3FileChangeManager(fname=tf.name)
        assert fcm.get_roots() == ["test/"]

def test_create_database_mysql():
    """Test to make sure that the create database feature works"""
    fcm = fchange.get_MySQL_fchange(DEFAULT_FILENAME)
    print("calling create!")
    fcm.create_database()
    fcm.add_root("test/")
    del fcm

    fcm = fchange.get_MySQL_fchange(DEFAULT_FILENAME)
    assert fcm.get_roots() == ["test/"]
