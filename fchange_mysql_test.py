import py.test
from fchange_mysql import *
import pymysql
from dbfile_mysql import SLGSQL_MySQL

import tempfile
from pathlib import Path

def test_create_database():
    with tempfile.NamedTemporaryFile() as tmp:
        conn = pymysql.connect(user='root')
        create_database(conn, tmp.name,"test/")
        assert os.path.exists(tmp.name)
        assert get_root(conn, tmp.name)=="test/"