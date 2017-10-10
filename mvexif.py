import PIL
import PIL.ExifTags
import PIL.Image
import dateutil.parser
import datetime
import os
import os.path
import time

EXIF_TIME_TAGSET = ['DateTime','DateTimeDigitized','DateTimeOriginal']

def file_exif(fn,tagset=PIL.ExifTags.TAGS.values()):
    """Return a dictionary of the the EXIF properties.
    @param fn - filename of JPEG file
    @param tagset - a set of the exif tag names that are requested (default is all)
    """
    # http://stackoverflow.com/questions/4764932/in-python-how-do-i-read-the-exif-data-for-an-image
    img = PIL.Image.open(fn)

    # in the loop below, k is the numberic key of the exif attribute, e.g. 271
    #                    v is the value of the exif attribute e.g. "Apple"
    exif = {PIL.ExifTags.TAGS[k]: v for k, v in img._getexif().items() 
            if PIL.ExifTags.TAGS[k] in tagset}
    return exif

def file_exif_time(fn):
    """Return the tags that have to do with date"""
    exif = file_exif(fn, tagset=EXIF_TIME_TAGSET)
    for tag in EXIF_TIME_TAGSET:
        if (tag in exif) and (exif[tag]):
            return exif[tag]
    return None

def jpeg_exif_to_mtime(fn, commit=False):
    "Set the file's mtime and ctime to be its timestamp and return the datetime"
    tm    = file_exif_time(fn)
    when  = datetime.datetime.strptime(tm,"%Y:%m:%d %H:%M:%S")
    timet = when.timestamp()
    if commit:
        os.utime(fn,(timet,timet))
    return when

if __name__=="__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(description='Change filename or timestamps to take '
                                     'into account the time stored in the EXIF',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--commit', help='Actually perform the changes', action='store_true')
    parser.add_argument("base", help="string to prepend to each filename", nargs=1)
    parser.add_argument("files", help="files to check/modify", nargs="+")
    
    args = parser.parse_args()

    for fn in files:
        try:
            when = jpeg_exif_to_mtime(fn,commit=args.commit)
            nfn = os.path.dirname(fn)+"/"+args.base+"_"+when.strftime("%Y-%m-%d_%H%M%S")+".jpg"
            if not os.path.exists(nfn):
                print("{} -> {}".format(fn,nfn))
                if args.commit:
                    os.rename(fn,nfn)
            else:
                print("{} X {}".format(fn,nfn))
        except AttributeError as e:
            print("Cannot process: {}".format(fn))

