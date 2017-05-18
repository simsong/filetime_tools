import py.test
from fchange import *
import sqlite3
from dbfile import SLGSQL

import tempfile

def test_create_database():
    fname = tempfile.mktemp()
    create_database(fname,"test/")
    assert os.path.exists(fname)
    conn = sqlite3.connect(fname)
    conn.row_factory = sqlite3.Row
    assert get_root(conn)=="test/"