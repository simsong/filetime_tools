import py.test

from scanner import *
import zipfile

def test_hash_file():
    assert hash_file(open("test/hello.txt","rb"))=="e59ff97941044f85df5297e1c302d260"

def test_open_zipfile():
    assert open_zipfile("scanner_test.py")==None
    zf = open_zipfile("test/hello.zip")
    assert type(zf) == zipfile.ZipFile


