DROP TABLE IF EXISTS metdata;
CREATE TABLE  metadata (name VARCHAR(255) PRIMARY KEY,value VARCHAR(255) NOT NULL);

DROP TABLE IF EXISTS dirnames;
CREATE TABLE  dirnames (dirnameid INTEGER PRIMARY KEY,dirname TEXT(65536));
CREATE UNIQUE INDEX  dirnames_idx2 ON dirnames (dirname(700));

DROP TABLE IF EXISTS filenames;
CREATE TABLE  filenames (filenameid INTEGER PRIMARY KEY,filename TEXT(65536));
CREATE INDEX  filenames_idx2 ON filenames (filename(700));

DROP TABLE IF EXISTS paths;
CREATE TABLE  paths (pathid INTEGER PRIMARY KEY,
       dirnameid INTEGER REFERENCES dirnames(dirnameid),
       filenameid INTEGER REFERENCES filenames(filenameid));
CREATE INDEX  paths_idx2 ON paths(dirnameid);
CREATE INDEX  paths_idx3 ON paths(filenameid);

DROP TABLE IF EXISTS hashes;
CREATE TABLE  hashes (hashid INTEGER PRIMARY KEY,hash TEXT(65536) NOT NULL);
CREATE INDEX  hashes_idx2 ON hashes( hash(700));

DROP TABLE IF EXISTS scans;
CREATE TABLE  scans (scanid INTEGER PRIMARY KEY,time DATETIME NOT NULL UNIQUE,duration INTEGER);
CREATE INDEX  scans_idx1 ON scans(scanid);
CREATE INDEX  scans_idx2 ON scans(time);

DROP TABLE IF EXISTS roots;
CREATE TABLE  roots (rootid INTEGER PRIMARY KEY,
       scanid INT REFERENCES scans(scanid),
       dirnameid INT REFERENCES dirnames(dirnameid));
CREATE INDEX  roots_idx1 ON roots(rootid);
CREATE INDEX  roots_idx2 ON roots(scanid);
CREATE INDEX  roots_idx3 ON roots(dirnameid);

CREATE TABLE  files (fileid INTEGER PRIMARY KEY,
                                  pathid INTEGER REFERENCES paths(pathid),
                                  mtime INTEGER NOT NULL, 
                                  size INTEGER NOT NULL, 
                                  hashid INTEGER REFERENCES hashes(hashid), 
                                  scanid INTEGER REFERENCES scans(scanid));
CREATE INDEX  files_idx1 ON files(pathid);
CREATE INDEX  files_idx2 ON files(mtime);
CREATE INDEX  files_idx3 ON files(size);
CREATE INDEX  files_idx4 ON files(hashid);
CREATE INDEX  files_idx5 ON files(scanid);
CREATE INDEX  files_idx6 ON files(scanid,hashid);
