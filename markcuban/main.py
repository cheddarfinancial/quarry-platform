#!/usr/bin/python


# Standard Library
import argparse
import json
import logging
import requests
import time
import uuid
from collections import defaultdict
from threading import Thread, Lock 

# Third Party
import mixingboard
from chassis.database import db_session, init_db
from chassis.models import Account, Query, JobHistory
from chassis.util import processFormattedTableDescription
from flask import Flask, jsonify, request, g

# Local
from lib import Cursor, LRU


# parse arguments
argParser = argparse.ArgumentParser(description='Run the Quarry server.')
argParser.add_argument('-d', '--debug', action='store_true', help='Turn on debug mode')
argParser.add_argument('-p', '--port', type=int, default=32123, help='Set the port')
argParser.add_argument('-H', '--host', type=str, default="127.0.0.1", help='Set the host')
argParser.add_argument('-c', '--command', type=str, default='serve', help='Operation to perform (serve, db)')
argParser.add_argument('-C', '--cache-size', type=int, default=1000, help='The max cache size in mega bytes')
args, _ = argParser.parse_known_args()

# extract arguments to sane, all caps variable names
DEBUG = args.debug
HOST = args.host
PORT = args.port
COMMAND = args.command
CACHE_SIZE = args.cache_size


# set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Standard Flask Initialization
app = Flask(__name__)

@app.teardown_appcontext
def shutdown_session(exception=None):
    db_session.remove()


# set up globals for managing cursors
global cursors
cursors = LRU(maxSize=CACHE_SIZE)


# setup flint configurations
global FLINT_URL_FORMAT
FLINT_URL_FORMAT = None
def setFlintURLFormat(flintServers):
    global FLINT_URL_FORMAT
    flint = flintServers[0]
    FLINT_URL_FORMAT = "http://%s:%s/flint/" % (flint["host"], flint["port"])
    logging.info("GOT FLINT SERVICE: %s" % FLINT_URL_FORMAT)

mixingboard.discoverService("flint",setFlintURLFormat)


def makeHistory(accountId, userId, event, jobId=None, jobHandle=None, data={}):
    """
    Save an entry to the job history record

    Params:
        account: an account
        user: a user
        event: an event type to save
        jobId: a job id
        data: a json serializable object
    Returns:
        a history event
    """

    jobHistory = JobHistory(account_id=accountId, user_id=userId, event=event, jobId=jobId, jobHandle=jobHandle, jobType="sql", data=data)
    db_session.add(jobHistory)
    db_session.commit()

    return jobHistory


def simpleJsonError(message):
    """
    Generates a basic json error payload

    Args:
        message: an error message
    Returns:
        A json string with the supplied error
        A 400 status code
    """

    return jsonify({
        "error": message
    })


def getHostInfos(account, user, cluster):
    """
    Retrieves the job-server info for a given 
    account/user combo

    Args:
        account: an account id
        user: a user id
        cluster: a cluster id
    Returns:
        an array of dictionaries containing host information
    """

    jobs = mixingboard.getService('job-server', account=account, user=user, cluster=cluster)

    return jobs


def getHostInfo(account, user, cluster):
    """
    Retrieves the first job-server info for a given 
    account/user combo

    Args:
        account: an account id
        user: a user id
        cluster: a cluster id
    Returns:
        a dictionary containing host information
    """

    return getHostInfos(account, user, cluster)[0]


def getSharkURL(account, user, cluster):
    """
    Returns the base url for a job server
    
    Args:
        account: an account id
        user: a user id
        cluster: a cluster id
    Returns:
        a dictionary containing host information
    """

    info = getHostInfo(account, user,cluster)

    return "http://%s:%s" % (info['host'], info['port'])

def getRequestParameters(forceHandle=False, forceCluster=True):
    """
    Retrieves and memoizes the user, account, cluster and handle 
    of the current request

    Returns:
        An error object to be returned to the user if not None
        The current user id
        The current account id
        The current handle (if there is one)
    """
    
    cluster = None
    if hasattr(g, 'cluster'):
        cluster = g.cluster
    else:
        cluster = request.args.get('cluster') or request.form.get('cluster')
        if cluster is None and forceCluster:
            return {
                "error": "You must specify a cluster"
            }, None, None, None, None

    user = None
    if hasattr(g, 'user'):
        user = g.user
    else:
        user = request.args.get('user') or request.form.get('user') 
        if user is None:
            return {
                "error": "You must specify a user"
            }, None, None, None, None

    account = None
    if hasattr(g, 'account'):
        account = g.account
    else:
        account = request.args.get('account') or request.form.get('account')
        if account is None:
            return {
                "error": "You must specify an account"
            }, None, None, None, None

    handle = None
    if hasattr(g, 'handle'):
        handle = g.handle
    else:
        handle = request.args.get('handle') or request.form.get('handle')
        if handle is None and forceHandle:
            return {
                "error": "You must specify a handle for this type of action"
            }, None, None, None, None

    return None, user, account, cluster, handle


def executeQueryAsync(query, options):

    error, user, account, cluster, _ = getRequestParameters()
    if error:
        return error, 400

    sharkURL = getSharkURL(account, user, cluster)

    res = requests.post("%s/spark/sql/run" % sharkURL, data={
        "account": account,
        "user": user,
        "sql": query,
        "options": options if isinstance(options, basestring) else json.dumps(options)
    })

    data = json.loads(res.text)

    if isinstance(options, basestring):
        options = json.loads(options)

    return data, res.status_code


def getQueryStatus():

    error, user, account, cluster, handle = getRequestParameters()
    if error:
        return error, 400

    sharkURL = getSharkURL(account, user, cluster)

    res = requests.get("%s/spark/job/async/status" % sharkURL, params={
        "account": account,
        "user": user,
        "handle": handle
    })

    return json.loads(res.text), res.status_code


def getQueryProgress():

    error, user, account, cluster, handle = getRequestParameters()
    if error:
        return error, 400

    sharkURL = getSharkURL(account, user, cluster)

    res = requests.get("%s/spark/job/async/progress" % sharkURL, params={
        "account": account,
        "user": user,
        "handle": handle
    })

    return json.loads(res.text), res.status_code


def getQueryResults():

    error, user, account, cluster, handle = getRequestParameters(forceCluster=False)
    if error:
        return error, 400

    res = requests.get("%sspark/job/async/results" % FLINT_URL_FORMAT, params={
        "account": account,
        "user": user,
        "handle": handle
    })

    return json.loads(res.text), res.status_code

def cursor():
    global cursors

    error, user, account, _, handle = getRequestParameters(forceCluster=False)
    if error:
        return error, 400

    accountObj = Account.query.filter(Account.id==account).first()
    cursor = Cursor(accountObj.iam_username, accountObj.access_key_id,
                    accountObj.access_key_secret, accountObj.region, handle)
    cursors.add(cursor.handle, cursor)

    return {
        "handle": cursor.handle 
    }, 200

def fetchN(maxRows=10000):
    global cursors

    error, user, account, _, handle = getRequestParameters(forceCluster=False)
    if error:
        return error, 400

    cursor = cursors.get(handle)
    if cursor is None:
        return {
            "error": "There is no cursor with that handle"
        }, 404

    rows = []
    while len(rows) < maxRows:
        row = cursor.fetch()
        if not row:
            break
        rows.append(row)

    return {
        "rows": rows
    }, 200


def executeQuerySync(query):

    error, user, account, cluster, _ = getRequestParameters()
    if error:
        return error, 400

    sharkURL = getSharkURL(account, user, cluster)

    res = requests.post("%s/spark/sql/run/sync" % sharkURL, data={
        "account": account,
        "user": user,
        "sql": query
    })

    return json.loads(res.text), res.status_code


@app.route('/shark/database/<database>/tables')
def shark_tables(database):
    """
    Retrieves the tables for the given shark database

    GetParams:
        account: an account
        user: a user
    RouteParams:
        database: Name of the shark database
    Returns:
        json string containing a list of shark tables
    """

    data, statusCode = executeQuerySync("SHOW TABLES IN %s" % database)
    logging.info(data)

    if statusCode == 200:
        data = {
            "tables": [row[0] for row in data['rows'] if row[0][0:2] != "__"]
        }

    return jsonify(data), statusCode


@app.route('/shark/tables')
def shark_tables_default():
    """
    Wrapper method that retrieves tables for the default database

    GetParams:
        account: an account
        user: a user
    Returns:
        json string containing a list of shark tables
    """

    return shark_tables('default')


@app.route('/shark/database/<database>/table/<table>/schema')
def shark_table_schema(database, table):
    """
    Retrieves the table schema for a table in the given 
        shark database

    GetParams:
        account: an account
        user: a user
    Args:
        database: Name of the shark database
        table: the name of the table
    Returns:
        json string containing a schema description
    """
    
    data, statusCode = executeQuerySync("DESC FORMATTED %s.%s" % (database, table))

    if statusCode != 200:
        return jsonify(data), 200

    rows = data['rows']

    cols, cached = processFormattedTableDescription(rows)

    return jsonify({
        "columns": cols,
        "cached": cached
    })


@app.route('/shark/table/<table>/schema')
def shark_table_schema_default(table):
    """
    Wrapper method that retrieves the table schema for 
        a table in the default database

    GetParams:
        account: an account
        user: a user
    Args:
        table: the name of the table
    Returns:
        json string containing a schema description
    """

    return shark_table_schema('default', table)


@app.route('/shark/query/save', methods=["POST"])
def shark_query_save():
    """
    Save a query for later.

    GetParams:
        account: an account
        user: a user
        title: a query title
        user: the id of the user creating the query
        description: a query description
        query: The sql query to save
        options: sql options to save
    Returns:
        A json object containing a representation of a saved query
    """

    query = request.form.get("query")
    title = request.form.get("title")
    description = request.form.get("description")
    options = json.loads(request.form.get("options","{}"))

    error, user, account, cluster, handle = getRequestParameters(forceCluster=False)
    if error is not None:
        return error, None, None

    try:
        queryObj = Query(title=title, account_id=account, user_id=user,
                         description=description, sql=query, options=options)
    except Exception as e:
        return jsonify({
            "error": e.message
        }), 400

    db_session.add(queryObj)
    db_session.commit()

    return jsonify({
        "query" : queryObj.dict()
    })


@app.route('/shark/query/<queryId>/update', methods=["POST"])
def shark_query_update(queryId):
    """
    Save a query for later.

    GetParams:
        account: an account
        user: a user
        query: The sql query to save
    Returns:
        A json object containing a representation of a saved query
    """

    query = request.args.get("query")

    error, user, account, cluster, handle = getRequestParameters(forceCluster=False)
    if error is not None:
        return error, None, None

    queryObj = Query.query.filter(Query.account_id == account, Query.id == queryId).first()

    if queryObj is None:

        return jsonify({
            "error": "There is no query with that id"
        }), 400

    else:

        queryObj.sql = query
        db_session.add(queryObj)
        db_session.commit()

        return jsonify({
            "query": queryObj.dict()
        })


@app.route('/shark/query/<queryId>')
def shark_saved_query_run(queryId):
    """
    Retrieve a saved query for a user/account

    RouteParams:
        queryId: a query id
    GetParams:
        account: an account
        user: a user
        query: The sql query to save
    Returns:
        A json object conatining a saved query
    """

    error, user, account, cluster, handle = getRequestParameters(forceCluster=False)
    if error is not None:
        return error, None, None

    query = Query.query.filter(Query.account_id == account, Query.id == queryId).first()

    if query is None:

        return jsonify({
            "error": "There is no query with that id"
        }), 400

    else:

        return jsonify({
            "query": query.dict()
        })


@app.route('/shark/query/<queryId>/delete', methods=['POST'])
def shark_saved_query_delete(queryId):
    """
    Retrieve a saved query for a user/account

    RouteParams:
        queryId: a query id
    GetParams:
        account: an account
        user: a user
    """

    error, user, account, _, _ = getRequestParameters(forceCluster=False)
    if error is not None:
        return error, None, None

    query = Query.query.filter(Query.account_id == account, Query.id == queryId).first()

    if query is None:

        return jsonify({
            "error": "There is no query with that id"
        }), 400

    else:

        db_session.delete(query)
        db_session.commit()

        return jsonify({})


@app.route('/shark/query/<queryId>/run', methods=["POST"])
def shark_saved_query(queryId):
    """
    Run a saved query for a user/account

    RouteParams:
        queryId: a query id
    GetParams:
        account: an account
        user: a user
        cluster: a cluster to run the job on
        query: The sql query to save
    Returns:
        A json object conatining a handle
    """

    error, user, account, cluster, handle = getRequestParameters()
    if error is not None:
        return jsonify(error), 400

    options = json.loads(request.form.get("options","{}"))

    query = Query.query.filter(Query.account_id == account, Query.id == queryId).first()

    if query is None:

        return jsonify({
            "error": "There is no query with that id"
        }), 400

    else:

        options = dict(query.options.items() + options.items())
        data, statusCode = executeQueryAsync(query.sql, options)

        return jsonify(data), statusCode


@app.route('/shark/queries')
def shark_saved_queries():
    """
    Retrieve saved queries for a user/account

    GetParams:
        account: an account
        user: a user
    Returns:
        A json object conatining a list of saved queries
    """

    error, user, account, _, handle = getRequestParameters(forceCluster=False)
    if error is not None:
        return error, None, None

    offset = int(request.args.get('offset', 0))
    count = int(request.args.get('count', 20))

    queries = []
    for query in Query.query.filter(Query.account_id == account).order_by(Query.created.desc()) \
                    .limit(count).offset(offset):
        queries.append(query.dict())

    return jsonify({
        "queries": queries
    })


@app.route('/shark/sql/analyze')
def shark_query_analyze():
    """
    Sends a query to shark.

    GetParams:
        account: an account
        user: a user
        query: The sql query to execute
    Returns:
        A json object containing a handle that can be used to
            retrieve monitor query progress and retrieve results
    """

    # FIXME the description method for shark cursor needs to be fixed for this to work

    return jsonify({
        "schema": {}
    })


@app.route('/shark/table/<table>/cache', methods=['POST'])
def shark_table_cache(table):
    """
    Sends a query to shark.

    GetParams:
        account: an account
        user: a user
        query: The sql query to execute
    Returns:
        A json object containing a handle that can be used to
            retrieve monitor query progress and retrieve results
    """

    query = """
        ALTER TABLE %s SET TBLPROPERTIES ("shark.cache" = "MEMORY")
    """ % table

    data, statusCode = executeQuerySync(query)
    
    return jsonify(data), statusCode


@app.route('/shark/table/<table>/uncache', methods=['POST'])
def shark_table_uncache(table):
    """
    Sends a query to shark.

    GetParams:
        account: an account
        user: a user
        query: The sql query to execute
    Returns:
        A json object containing a handle that can be used to
            retrieve monitor query progress and retrieve results
    """

    query = """
        ALTER TABLE %s SET TBLPROPERTIES ("shark.cache" = "NONE")
    """ % table

    data, statusCode = executeQuerySync(query)
    
    return jsonify(data), statusCode


@app.route('/shark/table/<table>/drop', methods=["POST","DELETE"])
def shark_table_drop(table):
    """
    Sends a query to shark.

    GetParams:
        account: an account
        user: a user
        query: The sql query to execute
    Returns:
        A json object containing a handle that can be used to
            retrieve monitor query progress and retrieve results
    """

    query = """
        DROP TABLE %s
    """ % table

    data, statusCode = executeQuerySync(query)
    
    return jsonify(data), statusCode


@app.route('/shark/sql', methods=["POST"])
def shark_query():
    """
    Sends a query to shark.

    PostParams:
        account: an account
        user: a user
        query: The sql query to execute
    Returns:
        A json object containing a handle that can be used to
            retrieve monitor query progress and retrieve results
    """

    query = request.form.get("query").strip()

    options = request.form.get("options")

    data, statusCode = executeQueryAsync(query, options)

    return jsonify(data), statusCode


@app.route('/shark/cursor')
def make_cursor():
    """
    Creates a cursor for iterating through results

    GetParams:
        account: an account
        user: a user
        handle: a shark client handle
    Returns:
        a json object container the cursor handle
    """

    data, statusCode = cursor()

    return jsonify(data), statusCode


@app.route('/shark/resultfiles')
def get_result_files():
    """
    Get a list of result files to download.

    GetParams:
        account: an account
        user: a user
        handle: a shark client handle
    Returns:
        a json object container the cursor handle
    """

    error, user, account, _, handle = getRequestParameters(forceCluster=False)
    if error is not None:
        return error, None, None

    accountObj = Account.query.filter(Account.id==account).first()
    cursor = Cursor(accountObj.iam_username, accountObj.access_key_id,
                    accountObj.access_key_secret, accountObj.region, handle)

    return jsonify({
        "files": cursor.queryFilesDownload()
    })

@app.route('/shark/fetchn/<number>')
def fetch_number(number):
    """
    Fetches a variable number of rows for a given shark client

    Args:
        number: the number of rows to fetch
    GetParams:
        account: an account
        user: a user
        handle: a shark client handle
    Returns:
        A json object containing data, schema and the user supplied 
            handle
    """

    data, statusCode = fetchN(int(number))

    return jsonify(data), statusCode


@app.route('/shark/status')
def status():
    """
    Fetches the status of a shark query

    GetParams:
        account: an account
        user: a user
        handle: a shark client handle
    Returns:
        A json representation of the query status
    """

    data, statusCode = getQueryStatus()

    return jsonify(data), statusCode


@app.route('/shark/progress')
def progress():
    """
    Fetches the progress of a shark query

    GetParams:
        account: an account
        user: a user
        handle: a shark client handle
    Returns:
        A json representation of the query progress
    """

    data, statusCode = getQueryProgress()

    return jsonify(data), statusCode


@app.route('/shark/results')
def query_results():
    """
    Fetches the results of a shark query. Does
    not fetch the rows returned by the query,
    rather it fetches the result of the query 
    execution. Useful for figuring out how a failed
    query failed.

    GetParams:
        account: an account
        user: a user
        handle: a shark client handle
    Returns:
        A json representation of the query results
    """

    data, statusCode = getQueryResults()

    return jsonify(data), statusCode


if __name__ == "__main__":

    mixingboard.exposeService("shark", port=PORT)
    app.run(debug=DEBUG, port=PORT, host=HOST, threaded=True)
