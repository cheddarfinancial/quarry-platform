["pymongo==2.7", "boto==2.27.0"]

import errno
import datetime
import os
import pymongo
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

field_type_fns = {
    0: float,
    1: int,
    2: int,
    3: int,
    4: float,
    5: float,
    6: lambda x: None,
    7: int,
    8: int,
    9: int,
    10: 'DATE',
    11: 'TIME',
    12: 'DATETIME',
    13: 'YEAR',
    14: 'NEWDATE',
    15: 'VARCHAR',
    16: 'BIT',
    246: 'NEWDECIMAL',
    247: 'INTERVAL',
    248: 'SET',
    249: 'TINY_BLOB',
    250: 'MEDIUM_BLOB',
    251: 'LONG_BLOB',
    252: 'BLOB',
    253: 'VAR_STRING',
    254: 'STRING',
    255: 'GEOMETRY' 
}

def run(sc, options):

    now = int(time.time()*1000)

    def escape(string):
        """
        Escape a string in mysql, because the builtin
        escpae function really sucks

        Args:
            string: a string to escape
        Returns:
            an escaped string
        """

        # TODO actually escape string
        return string


    def getMySQLConnection(options):
        """
        Gets a mysql connection

        Args:
            options: a dictionary containing connection options
        Return:
            a myysql connection
        """

        return pymysql.connect(host = options['host'],
                               user = options['dbuser'],
                               passwd = options['password'],
                               db = options['database'],
                               cursorclass = pymysql.cursors.SSCursor)


    db = getMySQLConnection(options)

    # get the description for the table we're importing
    cur = db.cursor()
    cur.execute("SELECT * FROM %s LIMIT 0" % escape(options['table']))
    importDesc = cur.description


    # get the minimum and maximum values for our split column    
    cur.execute("SELECT min(`%s`), max(`%s`) FROM `%s`" % (
        escape(options['splitBy']), 
        escape(options['splitBy']), 
        escape(options['table'])
    ))

    minSplit = None
    maxSplit = None
    desc = cur.description
    for row in cur.fetchall():
        minSplit = row[0]
        maxSplit = row[1]


    # we're done here, close up our connections
    cur.close()
    db.close()

    # build up our splits
    splits = []
    curSplitStart = minSplit
    if isinstance(minSplit, (int, long, float)):

        curSplit = 0
        maxSplit += 1

        splitSize = abs((maxSplit-minSplit)/options['numSplits'])
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

    sparkSplits = sc.parallelize(splits, len(splits))

    def importSplit(split):

        db = getMySQLConnection(options)
        cursor = db.cursor()

        cursor.execute("SELECT %s FROM %s WHERE %s >= %s AND %s < %s" % (
            escape(options['columns']),
            escape(options['table']),
            escape(options['splitBy']),
            escape(split[0]),
            escape(options['splitBy']),
            escape(split[1])
        ))

        s3conn = S3Connection(options['accessKeyId'], options['accessKeySecret'])

        bucket = s3conn.get_bucket(options['s3Bucket'])

        rowCount = 0
        fileNum = 0
        writeFile = StringIO()

        def writeFileToS3(f):
            f.seek(0)
            k = Key(bucket)
            k.key = os.path.join(options['warehouseDir'],options['sharkTable'],split[2])+"_%s" % fileNum
            k.set_contents_from_file(f)

        for row in cursor:

            writeFile.write("%s\n" % "\t".join([str(item) for item in row]))

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

    importedData = sparkSplits.map(importSplit)
    return importedData.reduce(lambda x, y: x+y)

    # TODO create table in shark
