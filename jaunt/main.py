#!/usr/bin/python

# Standard Library
import argparse
import json
import logging
import os
import time
import traceback
from threading import Thread

# Third Party
import requests
import mixingboard
import yaml
from chassis.models import Account, User, JobHistory, DataJob
from chassis.database import db_session
from flask import Flask, jsonify, request

# Local


# set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# parse args
argParser = argparse.ArgumentParser(description='Run the Quarry server.')
argParser.add_argument('-d', '--debug', action='store_true', help='Turn on debug mode')
argParser.add_argument('-p', '--port', type=int, default=9876, help='Set the port')
argParser.add_argument('-H', '--host', type=str, default='127.0.0.1', help='Set the host')
args, _ = argParser.parse_known_args()

# put args in sensible all caps variables
DEBUG = args.debug
PORT = args.port
HOST = args.host


# get jobs from settings file
currentDir = os.path.join(os.path.dirname(os.path.realpath(__file__)))
settingsFile = open(os.path.join(currentDir, ("settings.yml")))
settings = yaml.safe_load(settingsFile)
settingsFile.close()

JOBS = settings["JOBS"]


# setup shark server configurations
global SHARK_URL_FORMAT
SHARK_URL_FORMAT = None
def setSharkURLFormat(sharkServers):
    global SHARK_URL_FORMAT
    shark = sharkServers[0]
    SHARK_URL_FORMAT = "http://%s:%s/shark" % (shark["host"], shark["port"])
    logging.info("GOT SHARK SERVICE: %s" % SHARK_URL_FORMAT)

mixingboard.discoverService("shark",setSharkURLFormat)


# setup flint configurations
global FLINT_URL_FORMAT
FLINT_URL_FORMAT = None
def setFlintURLFormat(flintServers):
    global FLINT_URL_FORMAT
    flint = flintServers[0]
    FLINT_URL_FORMAT = "http://%s:%s/flint/" % (flint["host"], flint["port"])
    logging.info("GOT FLINT SERVICE: %s" % FLINT_URL_FORMAT)

mixingboard.discoverService("flint",setFlintURLFormat)


# setup flask app
app = Flask(__name__)

@app.teardown_appcontext
def shutdown_session(exception=None):
        db_session.remove()


def parseOptions(userOptions, jobOptions):
    """
    Verifies that user supplied options fit the criteria for a set of
    job options

    Args:
        userOptions: a set of user supplied options
        jobOptions: an option schema for a job
    Returns:
        a list of errors (can be empty)
        options to send to the job server
    """

    sendOptions = {}
    errors = []

    for name, info in jobOptions.items():

        # grab info about the option
        optionType = info["TYPE"]
        required = info["REQUIRED"]
        default = info.get("DEFAULT",None)

        # retrieve the user value for an option
        value = userOptions.get(name,None)

        # handle default/required case
        if value is None:
            if default is not None:
                value = default
            else:
                if required:
                    errors.append("The option '%s' is required" % name)
                continue

        # coerce the type of an option
        if optionType == "string":
            value = str(value)
        elif optionType == "int":
            value = int(value)
        elif optionType == "float":
            value = float(value)

        sendOptions[name] = value

    return errors, sendOptions


@app.route('/jaunt/datasets')
def juant_datasets():
    """
    Get all of the datasets that jaunt is able to export

    Returns:
        a list of importable database targets
    """

    url = "".join([SHARK_URL_FORMAT, "/tables"])

    res = requests.get(url, params=request.args)

    return jsonify({
        "datasets": res.json()['tables']
    }), res.status_code


@app.route('/jaunt/list')
def juant_list():
    """
    Get all of the databases that jaunt is able to import/export

    Returns:
        a list of importable database targets
    """

    return jsonify({
        "databases": [dbName.lower() for dbName in JOBS.keys()]
    })


@app.route('/jaunt/<database>/options/<command>')
def juant_options(database, command):
    """
    Runs an import, export or inspect command on various datastores
    that spark can interact with.

    RouteParams:
        database: the type of database (e.g. 'mysql', 'cassandra', etc.)
        command: one of 'inspect', 'import' or 'export'
    Get/PostParams:
        all args are parsed as options for the command
    """

    database = database.upper()

    try:
        jobInfo = JOBS[database]
    except KeyError:
        return jsonify({
            "error": "No database adapter for database named '%s'" % database
        }), 400

    jobOptions = dict(jobInfo.get("OPTIONS", {}).items() + jobInfo.get("%s_OPTIONS" % command.upper(), {}).items())

    return jsonify({
        "options": jobOptions
    })


@app.route('/jaunt/datajobs/<jobType>')
def jaunt_saved_jobs(jobType):
    """
    Run a saved import/export job.
    
    RouteParams:
        id: the id of a job
    PostParams:
        user: a user id
        account: an account id
        count: number of jobs to return
        offset: offset of jobs to return
    Returns:
        the result of invoking the job
    """

    if jobType not in {"import", "export"}:
        return jsonify({
            "error": "Job type must be either import or export"
        }), 400

    account = request.form['account']
    user = request.form['user']
    count = request.form.get("count", 20)
    offset = request.form.get("count", 0)

    datajobs = []
    for datajob in DataJob.query.filter(DataJob.account_id == account, DataJob.action == jobType) \
                .order_by(DataJob.created.desc()).limit(count).offset(offset):
        datajobs.append(datajob.dict())

    return jsonify({
        "datajobs": datajobs
    })
    

@app.route('/jaunt/datajob/<id>')
def jaunt_saved_job(id):
    """
    Retrieve a saved import/export job.
    
    RouteParams:
        id: the id of a job
    PostParams:
        user: a user id
        account: an account id
    Returns:
        the job
    """

    account = request.form['account']
    user = request.form['user']

    job = DataJob.query.filter(DataJob.account_id == account, DataJob.id == id).first()

    if not job:

        return jsonify({
            "error": "No datajob exists with that id"
        }), 400

    else:

        return jsonify({
            "job": job.dict()
        })
    

@app.route('/jaunt/datajob/<id>/delete', methods=['POST'])
def jaunt_delete_saved_job(id):
    """
    Retrieve a saved import/export job.
    
    RouteParams:
        id: the id of a job
    PostParams:
        user: a user id
        account: an account id
    Returns:
        the job
    """

    account = request.form['account']
    user = request.form['user']

    job = DataJob.query.filter(DataJob.account_id == account, DataJob.id == id).first()

    if not job:

        return jsonify({
            "error": "No datajob exists with that id"
        }), 400

    else:

        db_session.delete(job)
        db_session.commit()

        return jsonify({})
    

@app.route('/jaunt/datajob/<id>/run', methods=['POST'])
def jaunt_saved_command(id):
    """
    Run a saved import/export job.
    
    RouteParams:
        id: the id of a job
    PostParams:
        user: a user id
        account: an account id
    Returns:
        the result of invoking the job
    """

    account = request.form['account']
    user = request.form['user']
    cluster = request.form['cluster']

    job = DataJob.query.filter(DataJob.account_id == account, DataJob.id == id).first()

    options = dict(job.options)
    options['cluster'] = cluster
    options['account'] = account
    options['user'] = user

    return jauntRunCommand(job.database, job.action, options)
    

@app.route('/jaunt/<database>/<command>', methods=['GET', 'POST'])
def juant_command(database, command):
    """
    Runs an import, export or inspect command on various datastores
    that spark can interact with.

    RouteParams:
        database: the type of database (e.g. 'mysql', 'cassandra', etc.)
        command: one of 'inspect', 'import' or 'export'
    Get/PostParams:
        all args are parsed as options for the command
    """

    database = database.upper()

    options = {}
    if request.method == "GET":
        options = dict(request.args.items())
    elif request.method == "POST":
        options = dict(request.form.items())

    return jauntRunCommand(database, command, options)


def jauntRunCommand(database, command, options):
    
    try:

        save = options.get("save", False)

        account = options['account']
        user = options['user']

        try:
            jobInfo = JOBS[database]
        except KeyError:
            return jsonify({
                "error": "No database adapter for database named '%s'" % database
            }), 400

        errors = []

        sendOptions = {}

        res = ""

        if command == "inspect":

            # get all required options for an inspect command
            jobOptions = dict(jobInfo.get("OPTIONS",{}).items() + jobInfo.get("INSPECT_OPTIONS",{}).items())

            # verify inspect options
            errors, sendOptions = parseOptions(options, jobOptions)

            if len(errors) > 0:

                return jsonify({
                    "errors": errors
                }), 400

            else:

                # get the name of the inspect module for this db
                moduleName = jobInfo["INSPECT_MODULE"]

                # dynamically import the inspect module and reload it
                mod = __import__(moduleName)
                reload(mod)

                # if the inspect module has dots in the name, access them
                # on the imported module
                for name in moduleName.split(".")[1:]:
                    mod = getattr(mod, name)

                # run the inspect query with the provided options
                return jsonify({
                    "datasets": mod.inspect(options)
                })

        elif command == "describe":

            # get all required options for an inspect command
            jobOptions = dict(jobInfo.get("OPTIONS",{}).items() + jobInfo.get("DESCRIBE_OPTIONS",{}).items())

            # verify inspect options
            errors, sendOptions = parseOptions(options, jobOptions)

            if len(errors) > 0:

                return jsonify({
                    "errors": errors
                }), 400

            else:

                # get the name of the inspect module for this db
                moduleName = jobInfo["DESCRIBE_MODULE"]

                # dynamically import the inspect module and reload it
                mod = __import__(moduleName)
                reload(mod)

                # if the inspect module has dots in the name, access them
                # on the imported module
                for name in moduleName.split(".")[1:]:
                    mod = getattr(mod, name)

                # run the inspect query with the provided options
                return jsonify({
                    "dataset": mod.describe(options)
                })

        elif command == "import" or command == "export":

            jobOptions = dict(jobInfo.get("OPTIONS",{}).items() + jobInfo.get("%s_OPTIONS" % command.upper(), {}).items())

            errors, sendOptions = parseOptions(options, jobOptions)

            if len(errors) > 0:

                return jsonify({
                    "errors": errors
                }), 400

            else:

                # load in the job file, yea we're not using the os.path.join, deal with it *sunglasses*
                mainJobFiles = { 'file': open("%s/%s" % (currentDir, jobInfo["%s_FILE" % command.upper()]), 'rb') }
                extraJobFiles = { 'file': open("%s/%s" % (currentDir, 'jobs/jauntcommon.py'), 'rb') }

                # create a unique job name
                jobName = "%s_%s" % (database.lower(), int(time.time()*1000))

                logger.info("Running job %s" % jobName)

                accountObj = Account.query.filter(Account.id == account).first()

                sendOptions['warehouseDir'] = "/user/%s/shark/warehouse" % accountObj.iam_username
                sendOptions['accessKeyId'] = accountObj.access_key_id
                sendOptions['accessKeySecret'] = accountObj.access_key_secret
                sendOptions['s3Bucket'] = mixingboard.getConf("s3_bucket")
                sendOptions['jobType'] = command

                if 'jobName' not in sendOptions:
                    if command == "import":
                        sendOptions['jobName'] = "Import %s From %s" % (sendOptions['sharkTable'], database.lower())
                    elif command == "export":
                        sendOptions['jobName'] = "Export %s To %s" % (sendOptions['dataset'], database.lower())

                if save:

                    dataJob = DataJob(title=sendOptions['jobName'], account_id=account, user=user, description=sendOptions.get('description'),
                                      database=database, action=command, options=sendOptions)
                    db_session.add(dataJob)
                    db_session.commit()

                    return jsonify({
                        "job": dataJob.dict()
                    })

                else:

                    # TODO actually report no cluster error properly
                    cluster = options['cluster']

                    # upload the job to the job server
                    uploadUrl = "%sspark/jobs/upload" % FLINT_URL_FORMAT
                    res = requests.post(uploadUrl, 
                                        files=mainJobFiles, 
                                        data={
                                            "cluster": cluster,
                                            "name": jobName, 
                                            "account": account, 
                                            "user": user
                                        })

                    # upload the job to the job server
                    uploadUrl = "%sspark/jobs/%s/upload" % (FLINT_URL_FORMAT, jobName)
                    res = requests.post(uploadUrl, 
                                        files=extraJobFiles, 
                                        data={
                                            "cluster": cluster,
                                            "account": account, 
                                            "user": user
                                        })

                    # run the job synchronously
                    runUrl = "%sspark/job/%s/run" % (FLINT_URL_FORMAT, jobName)
                    res = requests.post(runUrl, 
                                       data={
                                            "cluster": cluster,
                                            "options": json.dumps(sendOptions), 
                                            "account": account,
                                            "user": user
                                       })

                    return res.text, res.status_code

        else:

            return jsonify({
                "error": "Unknown command '%s'" % command
            }), 400

    except Exception as e:
        return jsonify({
            "error": str(e),
            "trace": str(traceback.format_exc()).replace('\t','    ').split('\n')
        }), 400

if __name__ == "__main__":
    mixingboard.exposeService('jaunt',PORT)
    app.run(debug=DEBUG, host=HOST, port=PORT, threaded=True)
