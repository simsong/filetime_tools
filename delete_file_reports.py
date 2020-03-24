#!/usr/bin/env python3
#
#

import argparse

from ctools.dbfile import *
from ctools.env import *

# TODO: put this code in scanner.py, it's redundant here and in ftt_live_update.py
def get_recent_scan(auth: DBMySQLAuth, db: DBMySQL, prefix, now):
    result = db.csfr(auth, "SELECT scanid, rootid FROM `{db}`.{prefix}scans WHERE duration IS NOT NULL ORDER BY time DESC LIMIT 1 "
                     .format(db=auth.database, prefix=prefix))
    for row in result:
        return row[0], row[1]

if __name__ == "__main__":
    # arg parse stuff
    parser = argparse.ArgumentParser(description='Compute file changes',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        "--config", help="Specify configuration file", default="dbroot.bash")

    args = parser.parse_args()

    auth = None
    db = None
    prefix = None

    

    if args.config:
        config = get_vars(args.config)
        host = user = password = database = prefix = url = ""
        if 'MYSQL_HOST' in config:
            host = config['MYSQL_HOST']
        if 'MYSQL_USER' in config:
            user = config['MYSQL_USER']
        if 'MYSQL_PASSWORD' in config:
            password = config['MYSQL_PASSWORD']
        if 'MYSQL_DATABASE' in config:
            database = config['MYSQL_DATABASE']
        if 'MYSQL_PREFIX' in config:
            prefix = config['MYSQL_PREFIX']
        if 'FTT_QUEUE_URL' in config:
            url = config['FTT_QUEUE_URL']

        auth = DBMySQLAuth(host=host,
                           user=user,
                           password=password,
                           database=database)
        db = DBMySQL(auth)
    else:
        print("No configuration defined.")
        exit(1)

    # grab the deleted file reports from the table
    if database != "":
        requests = db.csfr(auth, """SELECT fileid, requestid, request_time FROM `{db}`.{prefix}delete_file_request;"""
        .format(db=auth.database, prefix=prefix))
        for request in requests:
            print(request)
            fileid = request[0]
            requestid = request[1]
            request_time = request[2]

            newest_scan, rootid = get_recent_scan(auth, db, prefix, datetime.datetime.now())

            file_query = """
            SELECT fileid, mtime FROM `{db}`.{prefix}files WHERE fileid={fileid} AND scanid={scanid} AND rootid={rootid};
            """.format(db=auth.database, prefix=prefix, fileid=fileid, scanid=newest_scan, rootid=rootid)
            # print(file_query)

            target_file = db.csfr(auth, file_query)
            print(target_file)
            target_file = target_file[0]

            mtime = target_file[1]
            if request_time > mtime:
                delete_file_query = "DELETE FROM {prefix}files WHERE fileid=%s".format(prefix=prefix)
                db.csfr(auth, delete_file_query, [fileid])
                print("deleting file: ", fileid)
                delete_request_query = "DELETE FROM {prefix}delete_file_request WHERE requestid=%s".format(prefix=prefix)
                db.csfr(auth, delete_request_query, [requestid])

