import PIL
import PIL.ExifTags
import PIL.Image
import dateutil.parser
import datetime
import os
import os.path
import time
import re

import fix_timestamps

EXIF_TIME_TAGSET = ['DateTime','DateTimeDigitized','DateTimeOriginal']

def file_exif(fn,tagset=PIL.ExifTags.TAGS.values()):
    """Return a dictionary of the the EXIF properties.
    @param fn - filename of JPEG file
    @param tagset - a set of the exif tag names that are requested (default is all)
    """
    # http://stackoverflow.com/questions/4764932/in-python-how-do-i-read-the-exif-data-for-an-image
    img = PIL.Image.open(fn)
    _exif = img._getexif()
    if not _exif:
        return None

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

def jpeg_exif_to_mtime(fn):
    "Set the file's mtime and ctime to be its timestamp and return the datetime"
    tm    = file_exif_time(fn)
    when  = datetime.datetime.strptime(tm,"%Y:%m:%d %H:%M:%S")
    timet = when.timestamp()
    if args.dry_run:
        print("would os.utime({},{})".format(fn,timet))
    else:
        os.utime(fn,(timet,timet))
    return when

def jpeg_set_exif_times(fn,date):
    from subprocess import run,PIPE
    cmd = ['jhead','-ds{}:{:02}:{:02}'.format(date.year,date.month,date.day), fn]
    if args.dry_run:
        print("DRY RUN: "+" ".join(cmd))
        return
    p = run(cmd,stdout=PIPE,encoding='utf-8')
    if "contains no Exif timestamp to change" in p.stdout:
        print("adding exif")
        p = run([cmd[0],'-mkexif'] + cmd[1:])


def process_file(fn):
    pathdate = fix_timestamps.path_to_date(fn)
    print("{}: date should be {}".format(fn,pathdate))
    exif = file_exif(fn, tagset=EXIF_TIME_TAGSET)
    if exif==None:
        if args.info() and (fn.lower().endswith(".jpg") or fn.lower().endswith(".jepg")):
            print("   {}: NO EXIF".format(fn))
        jpeg_set_exif_times(fn,pathdate)
        return
    exiftime = file_exif_time(fn)
    if exiftime==None:
        if args.info:
            print("   {}: NO EXIF DATE".format(fn))
        jpeg_set_exif_times(fn,pathdate)

    if args.info:
        print("{}:".format(fn))
        for (k,v) in exif.items():
            print("   {}: {}".format(k,v))
    if args.rename:
        when = jpeg_exif_to_mtime(fn)
        nfn = os.path.dirname(fn)+"/"+args.base+"_"+when.strftime("%Y-%m-%d_%H%M%S")+".jpg"
        if not os.path.exists(nfn):
            print("{} -> {}".format(fn,nfn))
            if not args.dry_run:
                os.rename(fn,nfn)
        else:
            print("{} X ({} exits)".format(fn,nfn))
        

if __name__=="__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(description='Change filename or timestamps to take '
                                     'into account the time stored in the EXIF',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--rename', help='Rename the JPEG to be consistent with exif', action='store_true')
    parser.add_argument("--dry-run", help="don't actually change anything", action='store_true')
    parser.add_argument("--base", help="string to prepend to each filename", default='')
    parser.add_argument("--info", help="Just print exif info for each image", action='store_true')
    parser.add_argument("--verbose", help="print lots of stuff")
    parser.add_argument("files", help="files or directories to check/modify", nargs="+")
    
    args = parser.parse_args()

    for fn in args.files:
        if os.path.isfile(fn):
            process_file(fn)
        if os.path.isdir(fn):
             for (dirpath, dirnames, filenames) in os.walk(fn):
                 for filename in filenames:
                     if filename.lower().endswith(".jpeg") or filename.lower().endswith(".jpg"):
                         process_file(os.path.join(dirpath,filename))
