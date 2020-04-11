import py.test

from scanner import *
import zipfile

HELLO_FILENAME = os.path.join( os.path.dirname(__file__), 'hello.txt')
HELLO_CONTENTS = "Hello World!\n"
HELLO_HASH     = '8ddd8be4b179a529afa5f2ffae4b9858'

ZIPFILE_FILENAME = os.path.join( os.path.dirname(__file__), 'hello.zip')


def test_hash_file():
    # Make sure the file exists if it isn't there
    if not os.path.exists(HELLO_FILENAME):
        with open(HELLO_FILENAME,"wb") as f:
            f.write(HELLO_CONTENTS)
            
    assert hash_file(open(HELLO_FILENAME,"rb")) == HELLO_HASH

def test_open_zipfile():
    assert os.path.exists(ZIPFILE_FILENAME)
    assert open_zipfile(ZIPFILE_FILENAME+"XXX")==None
    zf = open_zipfile(ZIPFILE_FILENAME)
    assert type(zf) == zipfile.ZipFile


