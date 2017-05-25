#!/usr/bin/env python36
#
# fix_timestamps.py:
# Scan the filesystem for timestamps in MMDDYY format and change them to YYYY-MM-DD
#

from fix_timestamps import *

def test_newname():
    pats = [["name.101289.jpg","name.1989-10-12.jpg"],
            ["name.1.2.89.jpg","name.1989-01-02.jpg"],
            ["photo-04022014.bmp","photo-2014-04-02.bmp"],
            ["photo-04.02.2014.jpeg","photo-2014-04-02.jpeg"],
            ["Supplies3.8.13.doc","Supplies2013-05-30.doc"]
            ]
    for (old,new) in pats:
        assert newname(old)==new

def test_is_skipdir():
    assert is_skipdir("/foo/assets/bar")==True
    assert is_skipdir("/foo/bar")==False

def test_is_skip_pat():
    assert is_skip_pat("03.10.2016")==False
    assert is_skip_pat("2016-03-10")==True
    assert is_skip_pat("2016-30-01")==False
    assert is_skip_pat("JetsamEvent-2017-03-31-063700.ips")==True

def test_make_year_digit4():
    assert make_year_digit4("89")=="1989"
    assert make_year_digit4("14")=="2014"

def test_make_digit2():
    assert make_digit2("3")  == "03"
    assert make_digit2("13") == "13"

def test_valid_year():
    print(valid_year(1985))
    assert valid_year(1950) == False
    assert valid_year(1985) == True
    assert valid_year(2025) == True
    assert valid_year(2085) == False

def test_newname():
    assert newname("SE/JetsamEvent-2017-03-31-063700.ips") == None

if __name__=="__main__":
    test_newname()
    print("done")