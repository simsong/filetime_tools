import py.test
import subprocess
import os
import os.path

"""Test the CLI according to what's in the README.md"""

def test_find_dups():
    os.chdir( os.path.join( os.path.dirname(__file__), ".."))
    subprocess.run("python3 fchange.py --sqlite3db mydb.db --create",shell=True, check=True)
    subprocess.run("python3 fchange.py --sqlite3db mydb.db --addroot tests/data/DIR1",shell=True, check=True)
    subprocess.run("python3 fchange.py --sqlite3db mydb.db --addroot tests/data/DIR2",shell=True, check=True)
    subprocess.run("python3 fchange.py --sqlite3db mydb.db --scan",shell=True, check=True)
    output =  subprocess.run("python3 fchange.py --sqlite3db mydb.db --reportdups",shell=True, check=True, capture_output=True, text=True).stdout
    assert "tests/data/DIR2/23456.txt" in output
    assert "tests/data/DIR1/23456.txt" in output
