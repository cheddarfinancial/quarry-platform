["PyMySQL==0.6.1", "boto==2.27.0"]

import errno
import datetime
import os
import pymysql
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

from jauntcommon import calculateSplits, writeOutIterator, createSharkTable

fieldTypeMap = {
    0: 'float',
    1: 'int',
    2: 'int',
    3: 'int',
    4: 'float',
    5: 'float',
    6: 'NULL',
    7: 'int',
    8: 'int',
    9: 'int',
    10: 'string',
    11: 'string',
    12: 'string',
    13: 'string',
    14: 'string',
    15: 'string',
    #16: 'BIT',
    #246: 'float',
    #247: 'INTERVAL',
    #248: 'SET',
    249: 'string',
    250: 'string',
    251: 'string',
    252: 'string',
    253: 'string',
    254: 'string',
    255: 'string' 
}

def run(sc, options):

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

    columns = [(column[0], fieldTypeMap[column[1]]) for column in importDesc]
    createSharkTable(sc, options, columns)
    
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

    # FIXME set number of splits based on size of table (1 split per 128mbs of data)

    sparkSplits = calculateSplits(sc, minSplit, maxSplit, options['numSplits'])

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

        return writeOutIterator(split, cursor, options)

    importedData = sparkSplits.map(importSplit)
    numRows = importedData.reduce(lambda x, y: x+y)

    return {
        "rows": numRows
    }
