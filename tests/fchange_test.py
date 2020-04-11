import tempfile
import sqlite3

import fchange
from dbfile import SLGSQL

def test_create_database_sqlite3():
    """Test to make sure that the create database feature works"""
    fname = tempfile.mktemp()

    fcm = fchange.SQLite3FileChangeManager()
    

    fchange.create_database(fname,"test/")
    assert os.path.exists(fname)
    conn = sqlite3.connect(fname)
    conn.row_factory = sqlite3.Row
    assert get_root(conn)=="test/"
