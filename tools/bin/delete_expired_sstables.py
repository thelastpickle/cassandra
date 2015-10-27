#!/usr/bin/python
#

import csv
import glob
import os, sys

backups = 'fully-expired-backups'
data = csv.reader(sys.stdin)
for row in data:
    if not ''.join(row).strip():
        pass
    else:
        sstable = row[1]
        print("deleting " + sstable)
        for f in glob.glob(sstable + "*"):
            if not os.path.exists(os.path.dirname(f) + "/" + backups):
                os.makedirs(os.path.dirname(f) + "/" + backups)
            #os.rename(f, os.path.dirname(f) + "/" + backups + "/" + os.path.basename(f))
            print("  (moved " + os.path.basename(f) + " into " + os.path.dirname(f) + "/" + backups + "/)")
