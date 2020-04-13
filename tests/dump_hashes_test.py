import sys
import os
import  subprocess 

DBFILE = 'mydb.db'
FCHANGE = os.path.join( os.path.dirname(__file__), '../fchange.py')

def test_dump_hashes():
    try:
        os.unlink(DBFILE)
    except FileNotFoundError:
        pass
    subprocess.check_call([sys.executable, FCHANGE,'--sqlite3db',DBFILE,'--create'])
    subprocess.check_call([sys.executable, FCHANGE,'--sqlite3db',DBFILE,'--addroot','data/DIR1'])
    assert os.path.exists(DBFILE)
    files = subprocess.run([sys.executable, FCHANGE,'--sqlite3db',DBFILE,'--dump'],
                          capture_output=True, text=True, check=True).stdout
    print("files:")
    return True

