#!/usr/bin/env python36
#
# fix_timestamps.py:
# Scan the filesystem for timestamps in MMDDYY format and change them to YYYY-MM-DD
#

# Todo: 
# - handle names

import sys
import os
import re

debug = True

# Patterns to skip
skip_pats = [re.compile("[0-9a-f]{16,}",re.I), # 16 hex digits
             re.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",re.I), # GUID
             re.compile("[12][012389][0-9][0-9]-[01][0-9]-[0-3][0-9]") # ISO-8601
             ]

mdy_pats  = [re.compile("[^0-9]([0-1][0-9])[.]?([0-3][0-9])[.]?(([12][90])?[89012][0-9])[^0-9]"), # MMDDYYYY
        re.compile("[^0-9]([0-1][0-9])[.]([0-3][0-9])[.](19[89][0-9])[^0-9]"), # MM.DD.19YY    
        re.compile("[^0-9]([0-1][0-9])[.]([0-3][0-9])[.]([89][0-9])[^0-9]"), # MM.DD.YY    
        re.compile("[^0-9]([0-1][0-9])[.]([0-3][0-9])[.](20[012][0-9])[^0-9]"), # MM.DD.20YY    
        re.compile("[^0-9]([0-1][0-9])[.]([0-3][0-9])[.]([012][0-9])[^0-9]"), # MM.DD.YY    
        re.compile("[^0-9]([0-9])[.]([0-3][0-9])[.]([89012][0-9])[^0-9]"), # M.DD.YY     (people who are careless)
        re.compile("[^0-9]([0-1][0-9])[.]([0-9])[.]([89012][0-9])[^0-9]"), # MM.D.YY     (people who are careless)
        re.compile("[^0-9]([0-9])[.]([0-9])[.]([89012][0-9])[^0-9]") # M.D.YY           (people who are careless)
        ]
# Dirs to skip
skip_dirs = set(["/assets/","/mail downloads/", "conda-meta", "site-packages"])

# Extensions to skip
skip_exts = set([".css",".js", ".jar", ".webhistory", ".ics"])


def is_skipdir(fname):
    fname_lower = fname.lower()
    for d in skip_dirs:
        if d in fname_lower:
            return True
    return False
        
def is_skip_pat(fname):
    basename = os.path.basename(fname)
    for s in skip_pats:
        if s.search(basename):
            return True
    return False
    

def make_year_digit4(year):
    assert len(year) in [2,4]
    if len(year)==2:
        if year[0] > '5':
            return "19" + year
        else:
            return "20" + year
    return year

def make_digit2(val):
    if len(val)==1:
        return "0" + val
    return val

def valid_year(year):   return 1980 <= int(year) <= 2030
def valid_month(month): return 1 <= int(month) <= 12
def valid_day(day):     return 1 <= int(day) <= 31

def newname(fname):
    """Look at a name and see if the name should be renamed. Return the new name, or None if no rename is necessary."""

    if is_skipdir(fname):
        return None

    dirname  = os.path.dirname(fname)
    basename = os.path.basename(fname)
    ext      = os.path.splitext(basename)[1]

    if ext in skip_exts:
        return None

    if is_skip_pat(basename):
        return None

    for p in mdy_pats:
        m = p.search(basename)
        if m:
            (month,day,year) = m.group(1,2,3)
            new_year  = make_year_digit4(year)
            new_month = make_digit2(month)
            new_day   = make_digit2(day)

            assert len(new_year)==4
            assert len(new_month)==2
            assert len(new_day)==2

            if not valid_year(new_year): return None
            if not valid_month(new_month): return None
            if not valid_day(new_day): return None

            # Look for a simple replacement, otherwise replace part-by-part
            fmt0 = f"{month}.{day}.{year}"
            fmt1 = f"{month}{day}{year}"
            iso = f"{new_year}-{new_month}-{new_day}"
            if fmt0 in basename:
                basename_new = basename.replace(fmt0,iso)
            elif fmt1 in basename:
                basename_new = basename.replace(fmt1,iso)
            else:
                basename_new = basename.replace(month,"MONTH",1).replace(day,"DAY",1).replace(year,"YEAR",1)
                basename_new = basename_new.replace("MONTHDAYYEAR","MONTH-DAY-YEAR")
                basename_new = basename_new.replace("MONTH.DAY.YEAR","MONTH-DAY-YEAR")
                basename_new = basename_new.replace("MONTH",new_year,1).replace("DAY",new_month,1).replace("YEAR",new_day,1)
            return os.path.join(dirname,basename_new)
    return None
        
def getch():
    if sys.platform=='darwin':
        import readchar
        return readchar.readchar()
    if sys.platform=='win32':
        import msvcrt
        return msvcrt.getch().decode('utf-8')
    assert(0)
    

def file_renamer(fname,fname_new):
    if debug:
        print("file_renamer({},{})".format(fname,fname_new))
        return
    if os.path.exists(fname) and not os.path.exists(fname_new):
        os.rename(fname,fname_new)

def cui_fix_name(fname,fname_new):
    import sys
    import os
    if fname_new:
        print("{} ==> {} ".format(fname,os.path.basename(fname_new)),end="")
        print()
        if os.path.exists(fname_new):
            print("EXISTS")
            return
        print("[qnY] ",end="")
        ch = getch().lower()
        print(ch)
        if ch in "q":
            exit(0)
        if ch in "y \n\r":
            file_renamer(fname,fname_new)
            

if __name__=="__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Seek out and rename MDY timestamps',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--gui", action='store_true')
    parser.add_argument("--test", help="explain how a file will change")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("roots",  nargs="*", help="Files/directories to search")

    args = parser.parse_args()

    movelist = []

    if args.test:
        print("{} => {}".format(args.test,newname(args.test)))
        exit(0)

    for root in args.roots:
        for (dirpath, dirnames, filenames) in os.walk(root):
            if is_skipdir(dirpath):
                continue        # don't do this directory
            for filename in filenames:
                fname = os.path.join(dirpath,filename)
                fname_new = newname(fname)
                if fname_new:
                    if args.gui:
                        movelist.append([fname,fname_new])
                    else:
                        cui_fix_name(fname,fname_new)
    if args.gui:
        from PyQt5.QtWidgets import QApplication
        from fileMoverDialog2 import VerifyDialog
        app = QApplication(sys.argv)
        dialog = VerifyDialog(movelist=movelist,callback=file_renamer)
        sys.exit(app.exec_())
    
