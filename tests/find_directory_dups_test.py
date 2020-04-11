import sys
from subprocess import call

def test_find_directory_dups_test():
    call([sys.executable,'../fchange.py','--sqlite3db','mydb.db','--addroot','data/DIR1'])
    call([sys.executable,'../fchange.py','--sqlite3db','mydb.db','--addroot','data/DIR2'])
    return True

