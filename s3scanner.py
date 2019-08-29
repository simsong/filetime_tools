### scanner.py
###
### part of the file system change
### scans the file system

############################################################
############################################################


import os
import sys
import zipfile
import time
import ctools.s3 as s3
from dbfile import DBFile, SLGSQL

from scanner import Scanner

class S3Scanner(Scanner):
    def __init__(self, *args, **kwargs):
        super().__init__(*args,**kwargs)

    def ingest_walk(self, root):
        """Scan S3"""
        (bucket,key) = s3.get_bucket_key(root)
        for obj in s3.list_objects(bucket,key):
            # {'LastModified': '2019-03-28T21:36:51.000Z', 
            #   'ETag': '"46610609053db79a94c4bd29cad8f4ff"', 
            #   'StorageClass': 'STANDARD', 
            #   'Key': 'a/b/c/whatever.txt', 
            #   'Size': 31838630}

            print(obj)
        print("Scan S3: ",root)
        

