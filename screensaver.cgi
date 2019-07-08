#!/usr/bin/env /Users/simsong/anaconda3/bin/python3
#
# screensaver.cgi:
#

# Right now this a CGI script, but it should be redone with flask

import sys
import cgi
import scanner
import sqlite3
import json
import base64

IMAGES_DB='images.db'

def get_random(conn):
    """Return a random JPEG from the most recent scan. Note this uses order by random, which is slow, but it works for now."""
    
    c = conn.cursor()
    c.execute("select hash from hashes where hashid = (select hashid from files where scanid=(select max(scanid) from scans) order by random())")
    return c.fetchone()[0]
    
def get_objects(conn,hash):
    """Returns the object specified by hash"""
    c = conn.cursor()
    return scanner.get_file_for_hash(conn,hash)
    
def respond(res):
    print("Content-type: text/plain\r\n\r\n")
    print(json.dumps(res),flush=True)
    exit(0)

def do_action(form):
    # open database and give me a big cache
    conn = sqlite3.connect(IMAGES_DB)
    conn.row_factory = sqlite3.Row

    action = form['action'].value
    if action=='random':
        res = {'random':get_random(conn)}
        respond(res)
    if action=='get':
        hash = form['hash'].value
        for obj in get_objects(conn,hash):
            res = {'image_base64':obj.get_data_base64str(conn)}
            respond(res)
    respond({})

if __name__ == "__main__":
    form = cgi.FieldStorage()
    if 'action' in form:
        do_action(form)

    import argparse
    parser = argparse.ArgumentParser(description='Test routines for the screensaver.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--random", action='store_true', help='generate a random jpeg URL')
    parser.add_argument("--getpath", help='get an object path')
    parser.add_argument("--get", help='get an object contents')
    parser.add_argument("--db", help="Specify database location", default=IMAGES_DB)
    args = parser.parse_args()

    # open database and give me a big cache
    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    if args.random:
        print(get_random(conn))

    if args.getpath:
        for obj in get_objects(conn, args.get):
            print(obj.get_path())
            break

    if args.get:
        for obj in get_objects(conn, args.get):
            with open(obj.get_path(),'rb') as f:
                sys.stdout.buffer.write(f.read())
            break
