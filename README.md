filetime_tools is a collection of programs for working with files and timestamps. It implements the following functionality:

* Scan a file system and find all of the files
* Maintain a database of scans with SQLite3
* Rescan the file system and report changes
* Change timestamps embedded in filenames from any recognized timestamp to ISO 8061 format (both batch and with a GUI)
* Change the EXIF in JPEGs to be consistent with the timestamp in the file's name.
* Change the name of a JPEG to be consistent with its EXIF timestamp

# MySQL Configuration
By default, `fchange.py` will look to the `default.ini` file for MySQL authentication credentials. Please see the file `default.ini.DIST` for how to fill out this file with your MySQL credentials.

You can specify a different configuration file with the `--config [path to configuration file]` flag.

# Available programs:

_fchange.py_ - Scan a directory and report file system changes.
_fix_jpegs.py_ - Change filename or timestamps to take into account the time stored in the EXIF
_fix_timestamps.py_ - Seek out and rename MDY timestamps in filenames to be YYYY-MM-DD. Optional GUI with --gui

# Use Cases

## Renaming files so that their embedded timestamps are in ISO8601 format:

    python3 fix_timestamps.py [--dry-run] [--gui] root1 [root2 ...]

## Find duplicate files in DIR2 that are also in DIR1


    python3 fchange.py --db mydb.db --create 
    python3 fchange.py --db mydb.db --addroot DIR1
    python3 fchange.py --db mydb.db --addroot DIR2
    python3 fchange.py --db mydb.db --scan
    python3 fchange.py --db mydb.db --reportdups

## Scan DIR1 and print the SH1 codes of every file with SQLite3
   python3 fchange.py --sqlite3db mydb.db --addroot DIR1
   python3 fchange.py --sqlite3db mydb.db --dump

## Scan DIR1 and DIR2 and delete files in DIR2 that are *anywhere* in DIR1 using sqlite3
   python3 fchange.py --sqlite3db mydb.db --addroot DIR1
   python3 fchange.py --sqlite3db mydb.db --addroot DIR2


## Find all of the JPEGs in a directory hiearchy

    python3 fchange.py --db images.db --create ~/Photos/         

## Find empty directories
You can do this just with Unix command line tools:

   find . -type d -empty -print

## Delete empty directories
You can do this just with Unix command line tools:

   find . -type d -empty -delete
