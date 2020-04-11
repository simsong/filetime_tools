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
import subprocess

EXIF_TIME_TAGSET = ['DateTime','DateTimeDigitized','DateTimeOriginal']

def file_exif(fn,tagset=PIL.ExifTags.TAGS.values(), allexif=False):
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
    if allexif:
        exif = {PIL.ExifTags.TAGS[k]: v for k, v in img._getexif().items() if k in PIL.ExifTags.TAGS }
        return exif

    exif = {PIL.ExifTags.TAGS[k]: v for k, v in img._getexif().items() 
            if k in PIL.ExifTags.TAGS and PIL.ExifTags.TAGS[k] in tagset}
    return exif

def file_exif_time(fn, allexif=False):
    """Return the datetime for the tags that have to do with date"""
    exif = file_exif(fn, tagset=EXIF_TIME_TAGSET, allexif=allexif)
    if exif:
        for tag in EXIF_TIME_TAGSET:
            if (tag in exif) and (exif[tag]):
                try:
                    return datetime.datetime.strptime(exif[tag],"%Y:%m:%d %H:%M:%S")
                except ValueError as e:
                    print(e)
                    pass
                try:
                    return datetime.datetime.strptime(exif[tag],"%Y:%m:%d:%H:%M:%S")
                except ValueError as e:
                    print(e)
                    pass
                try:
                    return datetime.datetime.strptime(exif[tag],"%Y.%m.%d %H.%M.%S")
                except ValueError as e:
                    print(e)
                    pass
    return None

def jpeg_set_exif_times(fn,date):
    cmd = ['jhead','-ds{}:{:02}:{:02}'.format(date.year,date.month,date.day), fn]
    if args.dry_run:
        print("DRY RUN: "+" ".join(cmd))
        return
    out = subprocess.check_output(cmd,encoding='utf-8')
    if "contains no Exif timestamp to change" in out:
        if args.debug:
            print("adding exif")
        subprocess.run([cmd[0],'-mkexif'] + cmd[1:], check=True)


def rename_file_logic(fn, base):
    """Return a new filename for fn, or None if it cannot be renamed"""
    when = file_exif_time(fn)
    if args.dump:
        print(fn,when,type(when))
    if not when:
        print(f"NO exif: {fn}")
        return None
    nfn = os.path.dirname(fn)+"/"
    if base:
        nfn += base+"_"
    nfn += when.strftime("%Y-%m-%d_%H%M%S")+".jpg"
    return nfn


def process_file(fn, dry_run=False, allexif=False):
    exif_time_set = False
    pathdate = fix_timestamps.path_to_date(fn) 
    exif     = file_exif(fn, tagset=EXIF_TIME_TAGSET, allexif=allexif)

    if exif and args.dump:
        print(f"{fn}:")
        for k,v in exif.items():
            print(f"  {k}  {v}")

    if pathdate or args.year:
        if not pathdate and args.year:
            pathdate = datetime.datetime(year=args.year,month=1,day=1,hour=0,minute=0,second=0)

        # If there is a date in the path, make sure that the date in the EXIF agrees.
        if exif==None:
            # No exif. If this is a jpeg file, give it an exif
            if args.info and (fn.lower().endswith(".jpg") or fn.lower().endswith(".jepg")):
                print("   {}: CREATING EXIF".format(fn))
            jpeg_set_exif_times(fn,pathdate)
        exif_time = file_exif_time(fn)
        if exif_time==None:
            if args.info:
                print("   {}: CREATING EXIF DATE".format(fn))
            jpeg_set_exif_times(fn,pathdate)
            exif_time_set = True
            
    # If we were asked to dump the exif, do so
    if args.info:
        if not exif:
            exif = file_exif(fn, tagset=EXIF_TIME_TAGSET, allexif=allexif)
            print("{}:".format(fn))
            for (k,v) in exif.items():
                print("   {}: {}".format(k,v))

    # If we were asked to set time utimes, do so
    exif_time  = file_exif_time(fn)
    if args.debug:
        print("file_exif_time({})={}".format(fn,exif_time))
    if exif_time and args.year:
        exif_time = exif_time.replace(year=args.year)
        jpeg_set_exif_times(fn,exif_time)
                

    if exif_time:
        timet = exif_time.timestamp()
        st    = os.stat(fn)
        if st.st_mtime != timet:
            if dry_run:
                print("WOULD os.utime({},{})  (from {})".format(fn,timet,st.st_mtime))
                print("   {} = {}".format(timet,time.asctime(time.localtime(timet))))
            else:
                os.utime(fn,(timet,timet))

    # If we were asked to rename the files, do so
    if args.rename:
        nfn = rename_file_logic(fn, args.base)
        if nfn:
            if not os.path.exists(nfn):
                if dry_run:
                    print("WOULD RENAME {} -> {}".format(fn,nfn))
                else:
                    print("{} -> {}".format(fn,nfn))
                    os.rename(fn,nfn)
            else:
                print("{} X ({} exits)".format(fn,nfn))
        
    if args.txt:
        txtfname = os.path.splitext(fn)[0] + ".txt"
        if os.path.exists(txtfname):
            print("Combining {} and {}".format(fn,txtfname))
            comment = open(txtfname).read().strip()
            check_call(['exiftool','-Comment='+comment,fn])
            os.unlink(txtfname)
            

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
    parser.add_argument("--debug", help="print lots of stuff")
    parser.add_argument("--txt",  help="If there is a filename.txt that matches filename.jpg, put the .txt's contents into the jpg as exif comments, and then delete the .txt", action='store_true')
    parser.add_argument("--rmdupcolor", help="Look for all JPEGs that have the same *name* in different directories. Delete the duplicate name if the two have different color profiles and if one has color profile XXX", action='store_true')
    parser.add_argument("--dump", action='store_true', help='dump EXIF for every file')
    parser.add_argument("--allexif", help='use all Exif tags', action='store_true')
    parser.add_argument("files", help="files or directories to check/modify", nargs="+")
    parser.add_argument("--year", type=int, help="If provided, for EXIFs to this year")
    
    args = parser.parse_args()

    for fn in args.files:
        if os.path.isfile(fn):
            process_file(fn, args.dry_run, allexif=args.allexif)
        if os.path.isdir(fn):
             for (dirpath, dirnames, filenames) in os.walk(fn):
                 for filename in filenames:
                     if filename.lower().endswith(".jpeg") or filename.lower().endswith(".jpg"):
                         process_file(os.path.join(dirpath,filename))
