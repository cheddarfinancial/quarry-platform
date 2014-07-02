["boto==2.27.0"]

import errno
import datetime
import os
import subprocess
import tempfile
import time
from cStringIO import StringIO

# fix for boto
import platform
platform.system = lambda: "Linux"
platform.release = lambda: "'13.1.0'"

from boto.s3.connection import S3Connection
from boto.s3.key import Key


def calculateSplits(sc, minSplit, maxSplit, numSplits):

    now = time.time()

    # build up our splits
    splits = []
    curSplitStart = minSplit
    if isinstance(minSplit, (int, long, float)):

        curSplit = 0
        maxSplit += 1

        splitSize = abs((maxSplit-minSplit)/numSplits)
        if splitSize == 0: 
            splitSize = 1
        while True:
            curSplitEnd = curSplitStart + splitSize
            if curSplitEnd > maxSplit:
                curSplitEnd = maxSplit
            if curSplitEnd == curSplitStart:
                break
            splits.append((curSplitStart,curSplitEnd,"%s_%s" % (now, str(curSplit).zfill(6))))
            curSplitStart = curSplitEnd
            curSplit += 1

    elif isinstance(minSplit, str):
        pass
        # TODO deal with string interpolation

    # TODO deal with other types (datetime, etc)

    return sc.parallelize(splits, len(splits))

def writeOutIterator(split, iterator, options, processRow=None):

    s3conn = S3Connection(options['accessKeyId'], options['accessKeySecret'])

    bucket = s3conn.get_bucket(options['s3Bucket'], validate=False)

    rowCount = 0
    fileNum = 0
    writeFile = StringIO()

    def writeFileToS3(f):
        f.seek(0)
        k = Key(bucket)
        k.key = os.path.join(options['warehouseDir'],options['sharkTable'],split[2])+"_%s" % fileNum
        k.set_contents_from_file(f)

    for row in iterator:

        if processRow is not None:
            row = processRow(row)

        writeFile.write("%s\n" % "\01".join([str(item) for item in row]))

        # if the file is longer than 128 MB, write it out
        if writeFile.tell() > 134217728:
            writeFileToS3(writeFile)
            writeFile.close()
            writeFile = StringIO()

        rowCount += 1

    # write any straggling rows to s3
    if writeFile.tell() > 0:
        writeFileToS3(writeFile)

    return rowCount

def createSharkTable(sc, options, columns):

    importType = options["importType"]
    sharkTable = options["sharkTable"]

    if importType == "overwrite":
        sc.sql("DROP TABLE %s" %    sharkTable)

    createTableStmt = "CREATE TABLE %s (%s) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\01'" % (
        options['sharkTable'],
        ",".join(["%s %s" % (column[0], column[1]) for column in columns])
    )

    if importType == "append":
        # Try to create the table, but since we're suppossed
        # to be appending, just pass if we fail to create it
        try:
            sc.sql(createTableStmt)
        except:
            pass

    elif importType in {"overwrite", "create"}:
        sc.sql(createTableStmt)
