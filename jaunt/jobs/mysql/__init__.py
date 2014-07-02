import pymysql


def inspect(options):

    conn = pymysql.connect(host = options['host'],
                         user = options['dbuser'],
                         passwd = options['password'],
                         cursorclass = pymysql.cursors.SSDictCursor)

    cur = conn.cursor()

    exclude = {"information_schema","innodb","mysql","performance_schema"}

    query = """
        SELECT table_name, table_rows, data_length, index_length, 
        round(((data_length + index_length) / 1024 / 1024),2) "size", 
        table_schema FROM information_schema.TABLES WHERE table_schema 
        NOT IN ('%s');
    """ % "','".join(exclude)

    cur.execute(query)

    rows = []
    for row in cur:

        rows.append({
            "table": row["table_name"],
            "database": row["table_schema"],
            "info": {
                "rows": row["table_rows"],
                "size": float(row["size"])
            }
        })

    return rows


def describe(options):

    conn = pymysql.connect(host = options['host'],
                         user = options['dbuser'],
                         passwd = options['password'],
                         cursorclass = pymysql.cursors.SSDictCursor)

    cur = conn.cursor()

    query = "SHOW COLUMNS FROM %s FROM %s" % (options['table'], options['database'])

    cur.execute(query)

    rows = []
    for row in cur:

        rows.append({
            "name": row["Field"],
            "type": row["Type"],
            "key": row["Key"]
        })

    return rows
