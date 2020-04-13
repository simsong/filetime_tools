from fix_jpegs import *
import py.test

IMG_3124 = os.path.join( os.path.dirname(__file__), "data/img_3124.jpg")
def test_file_exif():
    exif = file_exif(IMG_3124)
    assert exif['Make']=='Apple'
    assert exif['DateTime']=='2017:06:23 20:17:55'

    exif = file_exif(IMG_3124,['Make'])
    print(exif)
    assert exif['Make']=='Apple'
    assert 'DateTime' not in exif

def jpeg_exif_to_mtime_test():
    j = jpeg_exif_to_mtime(IMG_3124)
    assert j == datetime.datetime(year=2017,month=6,day=23,hour=20,minute=17,second=55)

def dont_test_rename_file_logic():
    # These files don't need to exist; we are just testing rename logic
    RENAMES = [("/Users/random/Dropbox (foobar)/photox/photo/2010-06-13 person whale034.pdf-000.jpg", None),
               ("/Users/random/Dropbox (foobar)/photox/photo/1981/firstname_lastname.jpg",
                "/Users/random/Dropbox (foobar)/photox/photo/1981/1981-12-01_firstname_lastname.jpg")]

    for (a,b) in RENAMES:
        assert rename_file_logic(a,'')==b


if __name__=="__main__":
    test_file_exif()
    jpeg_exif_to_mtime_test()
