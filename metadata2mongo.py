#!/usr/bin/python
# coding: utf-8

import re
from pymongo import MongoClient
from os import walk
from configme import path, metadb, metadir

def metadata2mongo(fullpath):
    file = open(fullpath, 'r')
    
    metadata = {}
    for lastline in file:
        #lastline = lastline[:-2]
        item = re.search(r"(.+?)\=(.+)$", lastline)
        if item:
            try:
                if item.group(1):
                    metakey = str(item.group(1))
                    metakey = metakey.replace('.', ' ')
                    metadata[metakey] = str(item.group(2))
            except:
                skip = item
    return metadata

client = MongoClient()
metadatadb = client.get_database(metadb)
col = metadatadb.data

f = []
for (dirpath, dirnames, filenames) in walk("%s/%s" % (path, metadir)):
    f.extend(filenames)

for filename in f:
    metadata = metadata2mongo("%s/%s/%s" % (path, metadir, filename))
    if metadata:
        try:
            col.insert_one(metadata)
        except:
            skip = 'yes'

print "Done"

