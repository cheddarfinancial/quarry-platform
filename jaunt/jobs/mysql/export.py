["PyMySQL==0.6.1"]


import datetime
import json
import os
import pymysql
import subprocess
import time


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
            a mysql connection
        """

        return pymysql.connect(host = options['host'],
                               user = options['dbuser'],
                               passwd = options['password'],
                               db = options['database'],
                               cursorclass = pymysql.cursors.SSDictCursor)


    mysqlTypeMap = {
        "int": "INT",
        "bigint": "BIGINT",
        "string": "TEXT",
        "float": "FLOAT"
    }

    fieldsRaw = sc.sql("DESCRIBE %s" % options['dataset'])
    fields = [
        {
            "name": field[0].strip(),
            "type": mysqlTypeMap[field[1].strip()]
        }
        for field in [field.split("\t") for field in fieldsRaw]
    ]

    # get schema/field list strings for use in building queries
    schemaString = ",".join(["%s %s" % (field['name'], field['type']) for field in fields])
    fieldNames = ",".join([field['name'] for field in fields])

    # create a table for the data we're writing
    db = getMySQLConnection(options)
    cur = db.cursor()
    cur.execute("CREATE TABLE %s (%s)" % (options['table'], schemaString))

    # we're done here, close up our connections
    cur.close()
    db.close()

    BATCH_SIZE = 100

    def exportWriter(partition):

        db = getMySQLConnection(options)
        cursor = db.cursor()

        def batchInsert(insertRows):
            query = "INSERT INTO %s (%s) VALUES %s" % (
                escape(options['table']),
                escape(fieldNames),
                "(%s)" % "),(".join([json.dumps(row)[1:-1] for row in insertRows])
            )
            print query
            cursor.execute(query)

        rowCount = 0
        insertRows = []
        for row in partition:

            insertRows.append(row)

            rowCount += 1

            if len(insertRows) >= BATCH_SIZE:
                batchInsert(insertRows)
                insertRows = []

        if len(insertRows) > 0:
            batchInsert(insertRows)

        cursor.execute("COMMIT")

        # we need to return a list because of mapPartitions
        return [rowCount]

    importedData = sc.sql2rdd("SELECT * FROM %s" % options['dataset']).mapPartitions(exportWriter)
    return {
        "rows": importedData.reduce(lambda x, y: x+y)
    }
