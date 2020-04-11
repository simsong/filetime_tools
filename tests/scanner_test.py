import py.test

from scanner import *
import zipfile

def test_hash_file():
    # The first hash is on Unix, the second is on Windows
    unix_hash = "e59ff97941044f85df5297e1c302d260"
    windows_hash = "3579c8da7f1e0ad94656e76c886e5125"
    assert hash_file(open("test/hello.txt","rb")) in [unix_hash,windows_hash]

    # When hashing the binary file, it's always the same
    assert hash_file(open("test/hello.zip","rb"))=="46d0707ff89bed468e78b4fcddf0ad60"  

def test_open_zipfile():
    assert open_zipfile("scanner_test.py")==None
    zf = open_zipfile("test/hello.zip")
    assert type(zf) == zipfile.ZipFile


