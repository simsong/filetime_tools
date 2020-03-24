#!/usr/bin/env python3
#
#

import argparse
import boto3
import botocore
from datetime import datetime
import json
import logging
import pprint

import scanner
from ctools.dbfile import *
from ctools.env import *

# some somewhat helpful constants
CREATE = "CREATE"
DELETE = "DELETE"

# TODO: Make sure to check that scan is the same as the root we want as well.
def get_recent_scan(auth: DBMySQLAuth, db: DBMySQL, prefix, now):
    result = db.csfr(auth, "SELECT scanid, rootid FROM `{db}`.{prefix}scans WHERE duration IS NOT NULL ORDER BY time DESC LIMIT 1 "
                     .format(db=auth.database, prefix=prefix))
    for row in result:
        return row[0], row[1]

def get_hashid(hexhash):
        db.csfr(auth, "INSERT IGNORE INTO `{db}`.{prefix}hashes (hash) VALUES (%s)"
            .format(db=auth.database, prefix=prefix), vals=[hexhash])
        result = db.csfr(auth, "SELECT hashid FROM `{db}`.{prefix}hashes WHERE hash='{hash}' LIMIT 1"
            .format(db=auth.database, prefix=prefix, hash=hexhash))
        for row in result:
            return row[0]


def get_pathid(auth: DBMySQLAuth, db: DBMySQL, prefix, path):
    (dirname, filename) = os.path.split(path)
    # dirname
    db.csfr(auth, "INSERT IGNORE INTO `{db}`.{prefix}dirnames (dirname) VALUES (%s)"
            .format(db=auth.database, prefix=prefix), vals=[dirname])
    dirids = db.csfr(auth, "SELECT dirnameid FROM`{db}`.{prefix}dirnames WHERE dirname=%s LIMIT 1"
                     .format(db=auth.database, prefix=prefix), vals=[dirname])
    for dirid in dirids:
        dirnameid = dirid[0]

    # filename
    db.csfr(auth, "INSERT IGNORE INTO `{db}`.{prefix}filenames (filename) VALUES (%s)"
            .format(db=auth.database, prefix=prefix), vals=[filename])
    fileids = db.csfr(auth, "SELECT filenameid FROM `{db}`.{prefix}filenames WHERE filename=%s LIMIT 1"
                      .format(db=auth.database, prefix=prefix), vals=[filename])
    for fileid in fileids:
        filenameid = fileid[0]

    # pathid
    db.csfr(auth, "INSERT IGNORE INTO `{db}`.{prefix}paths (dirnameid, filenameid) VALUES (%s,%s)"
            .format(db=auth.database, prefix=prefix), vals=[dirnameid, filenameid])
    pathids = db.csfr(auth, "SELECT pathid FROM `{db}`.{prefix}paths WHERE dirnameid=%s and filenameid=%s LIMIT 1"
                      .format(db=auth.database, prefix=prefix), vals=[dirnameid, filenameid])
    for pathid in pathids:
        return pathid[0]
    raise RuntimeError(f"no pathid found for {dirnameid},{filenameid}")


def get_file_hashid(auth: DBMySQLAuth, db: DBMySQL, prefix, *, f=None, pathname=None, file_size, pathid=None, mtime, hexdigest=None):
    """Given an open file or a filename, Return the MD5 hexdigest."""
    if pathid is None:
        if pathname is None:
            raise RuntimeError("pathid and pathname are both None")
        pathid = get_pathid(pathname, hexdigest)

    # Check if file with this length is in db
    # If not, we has that file and enter it.
    # Trusting that mtime gets updated if the file contents change.
    # We might also want to look at the file gen count.
    result = db.csfr(auth, "SELECT hashid FROM `{db}`.{prefix}files WHERE pathid=%s AND mtime=%s AND size=%s LIMIT 1"
                     .format(db=auth.database, prefix=prefix), vals=[pathid, mtime, file_size])
    for row in result:
        return row[0]

    # Hashid is not in the database. Hash the file if we don't have the hash
    return get_hashid( hexdigest )


def update_file(auth: DBMySQLAuth, db: DBMySQL, prefix, *, action, key, eventTime, size, etag):

    (dirname, filename) = os.path.split(key)

    newest_scan, rootid = get_recent_scan(auth, db, prefix, datetime.datetime.now())
    # 'fileid','rootid','dirnameid','mtime','size','rootdir','dirname','filename'
    # check to see if file is in the system
    sql = """
    SELECT fileid, mtime FROM `{db}`.{prefix}files 
    NATURAL JOIN {prefix}paths 
    NATURAL JOIN {prefix}dirnames 
    NATURAL JOIN {prefix}filenames 
    WHERE scanid={newest_scan} AND dirname='{dirname}' AND filename='{filename}'
    """.format(
        db=auth.database, prefix=prefix,
        newest_scan=newest_scan, dirname=dirname, filename=filename)
    
    result = db.csfr(auth, sql)

    if action == CREATE:

        if len(result) == 0:
            pathid = get_pathid(auth, db, prefix, key)
            hashid = None
            try:
                hashid = get_file_hashid(
                    auth, db, prefix, pathid=pathid, mtime=eventTime, file_size=size, f=None, hexdigest=etag)
            except PermissionError as e:
                return
            except OSError as e:
                return
            mtime = None
            try:
                mtime = int(datetime.datetime.strptime(eventTime[0:19], '%Y-%m-%dT%H:%M:%S').timestamp())
            except ValueError as e:
                mtime = int(datetime.datetime.strptime(eventTime[0:19], '%Y-%m-%d %H:%M:%S').timestamp())

            db.csfr(auth, "INSERT INTO `{db}`.{prefix}files (pathid,rootid,mtime,size,hashid,scanid) "
                    "VALUES (%s,%s,%s,%s,%s,%s)".format(
                        db=auth.database, prefix=prefix), vals=[int(pathid), int(rootid), int(mtime),
                                                     int(size), int(hashid), int(newest_scan)])
        else:
            # update with current info about file
            pass
    else:
        if len(result) == 0:
            # ignore, it's trying to delete a file that doesn't exist
            print("attempting to delete file that doesn't exist")
        else:
            print("delete file that exists")
            # log a deletion in the deleted_files table
            result = result[0]
            eventTime = int(datetime.datetime.strptime(eventTime[0:19], '%Y-%m-%dT%H:%M:%S').timestamp())
            db.csfr(auth, "INSERT INTO `{db}`.{prefix}delete_file_request (fileid, request_time) "
                    "VALUES (%s,%s)".format(
                        db=auth.database, prefix=prefix), vals=[result[0], int(eventTime)])


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

    # update_file(auth, db, prefix, "CREATE", "part-00000","2020-03-19T17:49:50.396Z", "size", "etag")
    # exit(0)

    recieved_messages = []
    objects_to_add = set()

    client = boto3.client('sqs')

    recieved_empty = False

    print("getting messages...")
    while not recieved_empty:

        # set up client and request 10 messages
        response = client.receive_message(
            QueueUrl=url,
            AttributeNames=[
                'All',
            ],
            MaxNumberOfMessages=10,
            MessageAttributeNames=[
                'All'
            ],
            VisibilityTimeout=1,
            WaitTimeSeconds=0
        )
        bucket_msgs = []
        try:
            bucket_msgs = response['Messages']
        except Exception as e:
            recieved_empty = True
            # print('No messages recieved: ', e)
            # print("FINISHED!")
        roots = {}

        for i, message in enumerate(bucket_msgs):

            receipt_handle = message['ReceiptHandle']
            message_id = message['MessageId']
            if receipt_handle not in recieved_messages:
                recieved_messages.append(
                    {
                        'MessageId': message_id,
                        'ReceiptHandle': receipt_handle
                    }
                )
            body = json.loads(message['Body'])

            if 'Records' not in body:
                continue
            records = body['Records']

            for j, record in enumerate(records):
                eventTime = record['eventTime']
                if 's3' in record:
                    bucket_name = record['s3']['bucket']['arn'].split()[-1]
                    bucket_name = bucket_name.split(':')[-1]

                    eventType = CREATE if record['eventName'] == 'ObjectCreated:Put' else DELETE
                    etag = size = None
                    if eventType == CREATE:
                        etag = record['s3']['object']['eTag']
                        size = record['s3']['object']['size']
                    key = record['s3']['object']['key']

                    new_object = (eventType, key, eventTime, size, etag)
                    objects_to_add.add(new_object)
                    print("new object:", new_object)
                else:
                    logging.warning("s3 message not found")

        # Now delete the things that were retrieved from the queue.
        print("deleting messages from queue...")
        entries = []
        unique_ids = set()
        for i, entry in enumerate(recieved_messages):
            if entry['MessageId'] not in unique_ids:
                unique_ids.add(entry['MessageId'])
                entries.append(
                    {
                        "Id": entry['MessageId'],
                        "ReceiptHandle": entry["ReceiptHandle"]
                    }
                )
        if len(entries) > 0:
            response = client.delete_message_batch(
                QueueUrl=url,
                Entries=entries
            )

            if 'Failed' in response:
                print(response['Failed'])
                exit(1)
        
    # Add or Remove the values
    print("uploading files to db...")
    for obj in objects_to_add:
        # TODO: FINISH DELETED FILES TABLE
        # TODO: make this a bit easier to read/understand: 1,2,3... don't really say anything about what those items are
        update_file(auth, db, prefix, action=obj[0], key=obj[1], eventTime=obj[2], size=obj[3], etag=obj[4])


    print("DONE!")