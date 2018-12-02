Fixing timestamps:

    python3 fix_timestamps.py [--dry-run] [--gui] root1 [root2 ...]

Find duplicate files in DIR1 and DIR2:
Currently, you need to put both dir1 and dir2 into a single directory:

    mkdir dir3
    mv DIR1 DIR2 dir3
    python3 fchange.py --db mydb.db --create dir3
    python3 fchange.py --db mydb.db --dups
    