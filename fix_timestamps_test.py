#!/usr/bin/env python36
#
# fix_timestamps.py:
# Scan the filesystem for timestamps in MMDDYY format and change them to YYYY-MM-DD
#

from fix_timestamps import *
import datetime

def test_newname():
    pats = [
        ["Jan 2017","2017-01"],
        ["Jan. 2017","2017-01"],
        ["January 2017","2017-01"],
        ["January, 2017","2017-01"],
        ["Jan 2017 Invoices","2017-01 Invoices"],
        ["name.101289.jpg","name.1989-10-12.jpg"],
        ["name.1.2.89.jpg","name.1989-01-02.jpg"],
        ["photo-04022014.bmp","photo-2014-04-02.bmp"],
        ["photo-04.02.2014.jpeg","photo-2014-04-02.jpeg"],
        ["Supplies3.8.13.doc","Supplies2013-03-08.doc"],
        ["Interaction 2015 USAID Open Data Policy Brief - 3.26.15.pdf",
         "Interaction 2015 USAID Open Data Policy Brief - 2015-03-26.pdf"],
        ["SEJetsamEvent-2017-03-31-063700.ips",None],
        ["Statement_Sep 2016.pdf","Statement_2016-09.pdf"]
        ]
    for (old,new) in pats:
        print("{} => {}".format(old,new))
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


def test_path_to_date():
    pats = [
        ["2000/10/foo.jpg", datetime.date(2000,10,1)],
        ["2000/foo.jpg", datetime.date(2000,1,1)],
        ["foo.jpg", None]
        ]
    for (old,new) in pats:
        print("{} => {}".format(old,new))
        assert path_to_date(old) == new
    

if __name__=="__main__":
    test_newname()
    print("done")
