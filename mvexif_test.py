from mvexif import *
import py.test

FN = "test/img_3124.jpg"
def test_file_exif():
    exif = file_exif(FN)
    assert exif['Make']=='Apple'
    assert exif['DateTime']=='2017:06:23 20:17:55'

    exif = file_exif("test/img_3124.jpg",['Make'])
    print(exif)
    assert exif['Make']=='Apple'
    assert 'DateTime' not in exif

def jpeg_exif_to_mtime_test():
    j = jpeg_exif_to_mtime(FN)
    assert j == datetime.datetime(year=2017,month=6,day=23,hour=20,minute=17,second=55)


if __name__=="__main__":
    test_file_exif()
    jpeg_exif_to_mtime_test()
