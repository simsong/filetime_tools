import tempfile
import sqlite3

import fchange
from dbfile import SLGSQL

def test_create_database_sqlite3():
    """Test to make sure that the create database feature works"""
    with tempfile.NamedTemporaryFile(suffix='.dbfile') as tf:
        fcm = fchange.SQLite3FileChangeManager(fname=tf.name)
    
        fcm.create_database()
        fcm.add_root("test/")
        del fcm

        fcm = fchange.SQLite3FileChangeManager(fname=tf.name)
        assert fcm.get_roots() == ["test/"]
