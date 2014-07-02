#!/usr/bin/python


# Standard Library
import argparse
import json
import logging
import time
from collections import defaultdict

# Third Party
import mixingboard
import requests
from chassis.aws import getS3Conn
from chassis.database import db_session, init_db
from chassis.models import Account, Job, JobHistory, RawDataset
from flask import Flask, jsonify, request, redirect

# Local


# set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# parse args
argParser = argparse.ArgumentParser(description='Run the Quarry server.')
argParser.add_argument('-d', '--debug', action='store_true', help='Run in debug mode')
argParser.add_argument('-p', '--port', type=int, default=1988, help='Set the port')
argParser.add_argument('-H', '--host', type=str, default='127.0.0.1', help='Set the host')
args, _ = argParser.parse_known_args()

# put args in sensible all caps variables
DEBUG = args.debug
HOST = args.host
PORT = args.port


# instantiate flask and configure some shit
app = Flask(__name__)

@app.teardown_appcontext
def shutdown_session(exception=None):
    db_session.remove()


###
# History Methods
###


def makeHistory(accountId, userId, event, jobType="spark", jobId=None, jobHandle=None, data={}):
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

    jobHistory = JobHistory(account_id=accountId, user_id=userId, event=event, jobType=jobType, jobId=jobId, jobHandle=jobHandle, data=data)
    db_session.add(jobHistory) 
    db_session.commit()

    return jobHistory


###
# Save Job Methods
###


@app.route('/flint/jobs/saved')
def jobs_saved():
    """
    Retrieve saved jobs for a user/account

    GetParams:
        user: a user
        account: an account
    Returns:
        a json object conatining a list of saved queries
    """

    account = request.args.get("account")
    user = request.args.get("user")
    offset = int(request.args.get('offset', 0))
    count = int(request.args.get('count', 20))

    jobs = []
    for job in Job.query.filter(Job.account_id == account).order_by(Job.created.desc()) \
            .limit(count).offset(offset):
        jobs.append(job.dict())

    return jsonify({
        "jobs": jobs
    })


@app.route('/flint/job/<jobId>/delete', methods=['DELETE'])
def job_delete(jobId):
    """
    Delete a saved job

    RouteParams:
        jobId: the id of a job
    GetParams:
        account: an account
        user: a user
        code: the new code for the job
    Returns:
        a json representation of a job
    """

    account = request.form['account']
    user = request.form['user']

    job = Job.query.filter(Job.account_id == account, Job.id == jobId).first()

    if job is None:

        return jsonify({
            "error": "There is no job with that id"
        }), 400

    else:
    
        db_session.delete(job)
        db_session.commit()

        makeHistory(account, user, "delete_job", job.id, {})

        return jsonify({
            "job": job.dict()
        })


@app.route('/flint/job/<jobId>/savefile', methods=['POST'])
def job_update_file(jobId):
    """
    Update a saved job

    RouteParams:
        jobId: the id of a job
        filename: the name of the file to save,
            if none, it is the main file
    GetParams:
        account: an account
        user: a user
        code: the new code for the job
    Returns:
        a json representation of a job
    """

    account = request.form['account']
    user = request.form['user']
    code = request.form['code']
    filename = request.form.get('filename')

    job = Job.query.filter(Job.account_id == account, Job.id == jobId).first()

    if job is None:

        return jsonify({
            "error": "There is no job with that id"
        }), 400

    else:
    
        if filename is None:
            job.setMainFile(code)
        else:
            job.addExtraFile(filename, code)
        db_session.add(job)
        db_session.commit()

        makeHistory(account, user, "update_job_edit_file", jobId=job.id, data={"filename": filename or "main.py"})

        return jsonify({
            "job": job.dict()
        })


@app.route('/flint/job/<jobId>/deletefile', methods=['POST'])
def job_delete_file(jobId):
    """
    Update a saved job

    RouteParams:
        jobId: the id of a job
        filename: the name of the file to delete
    GetParams:
        account: an account
        user: a user
        code: the new code for the job
    Returns:
        a json representation of a job
    """

    account = request.form['account']
    user = request.form['user']
    code = request.form['code']
    filename = request.form['filename']

    job = Job.query.filter(Job.account_id == account, Job.id == jobId).first()

    if job is None:

        return jsonify({
            "error": "There is no job with that id"
        }), 400

    else:
    
        job.removeExtraFile(filename)
        db_session.add(job)
        db_session.commit()

        makeHistory(account, user, "update_job_delete_file", jobId=job.id, data={"deleted_filename": filename or "main.py"})

        return jsonify({
            "job": job.dict()
        })


@app.route('/flint/job/<jobId>/run', methods=['POST'])
def job_run(jobId):
    """
    Run a saved job

    RouteParams:
        jobId: the id of a job
    GetParams:
        account: an account
        user: a user
    Returns:
        a json representation of a job
    """

    account = request.form['account']
    user = request.form['user']
    cluster = request.form['cluster']
    requestOptions = json.loads(request.form.get('options','{}'))

    job = Job.query.filter(Job.account_id == account, Job.id == jobId).first()

    if job is None:

        return jsonify({
            "error": "There is no job with that id"
        }), 400

    else:
    
        jobServerInfo = mixingboard.getService('job-server', cluster=cluster, account=account)[0]

        baseJobUrl = "http://%s:%s/spark" % (jobServerInfo['host'], jobServerInfo['port'])

        mainFileData = job.getMainFileContents()
        files = {'file': ("main.py", mainFileData)}
        jobRunName = "%s_%s" % (job.title, int(time.time()))
        res = requests.post(
            "%s/jobs/upload" % baseJobUrl,
            data = {
                'account': account,
                'user': user,
                'cluster': cluster,
                'name': jobRunName
            },
            files = files
        )

        if res.status_code != 200:
            return res.text, res.status_code

        extraFiles = {}
        for filename, contents in job.getExtraFiles():
            extraFiles = {filename: (filename, contents)}

        if len(extraFiles) > 0:
            res = requests.post(
                "%s/jobs/%s/upload" % (
                    baseJobUrl,
                    jobRunName
                ),
                data = {
                    'account': account,
                    'user': user,
                    'cluster': cluster
                },
                files = files
            )

            if res.status_code != 200:
                return res.text, res.status_code

        options = dict(job.options.items() + requestOptions.items())
        res = requests.post(
            "%s/job/%s/run" % (
                baseJobUrl,
                jobRunName
            ),
            data={
                "cluster": cluster,
                "options": json.dumps(options),
                "account": account,
                "user": user
            }
        )
        
        return res.text, res.status_code

@app.route('/flint/job/<jobId>')
def job(jobId):
    """
    Get a saved job

    RouteParams:
        jobId: the id of a job
    GetParams:
        account: an account
        user: a user
    Returns:
        a json representation of a job
    """

    account = request.args['account']
    user = request.args['user']

    job = Job.query.filter(Job.account_id == account, Job.id == jobId).first()

    if job is None:

        return jsonify({
            "error": "There is no job with that id"
        }), 400

    else:
    
        return jsonify({
            "job": job.dict()
        })


@app.route('/flint/jobs/save', methods=["POST"])
def job_save():
    """
    Save a job for later

    GetParams:
        account: an account
        user: a user
        title: a job title
        description: a job description
        code: the code to save
    Returns:
        a json object containing a representation of a saved job
    """

    account = request.form['account']
    user = request.form['user']

    code = request.form.get("code")
    title = request.form.get("title")
    description = request.form.get("description")

    try:
        jobObj = Job(title=title, account_id=account, user_id=user, description=description)
    except Exception as e:
        return jsonify({
            "error": e.message
        }), 400

    db_session.add(jobObj)
    db_session.commit()

    # now that our object has an id, we can add our code to it
    jobObj.setMainFile(code)

    db_session.add(jobObj)
    db_session.commit()

    makeHistory(account, user, "create_job", jobId=jobObj.id)

    return jsonify({
        "job": jobObj.dict()
    })


##
# Raw Dataset Methods
##


@app.route('/flint/rawdataset/create', methods=["POST"])
def create_raw_dataset():
    """
    Create a new raw datasets

    GetParams:
        account: an account
        user: a user
        name: the name to give to the dataset
    Returns:
        a list of raw datasets
    """

    account = request.form['account']
    user = request.form['user']
    name = request.form['name']

    dataset = RawDataset(name, account)
    db_session.add(dataset)
    db_session.commit()

    return jsonify({
        "dataset": dataset.dict()
    })




@app.route('/flint/rawdatasets')
def list_raw_datasets():
    """
    Get a list of raw datasets

    GetParams:
        account: an account
        user: a user
    Returns:
        a list of raw datasets
    """

    account = request.form['account']
    user = request.form['user']

    datasets = []
    for dataset in RawDataset.query.filter(RawDataset.account_id == account):
        datasets.append(dataset.dict())

    return jsonify({
        "datasets": datasets
    })


@app.route('/flint/rawdataset/<name>')
def get_raw_dataset(name):
    """
    Get info about a raw dataset

    RouteParams:
        name: the name of a dataset
    GetParams:
        account: an account
        user: a user
    Returns:
        a raw dataset
    """

    account = request.form['account']
    user = request.form['user']

    dataset = RawDataset.query.filter(RawDataset.account_id == account, RawDataset.name == name).first()

    return jsonify({
        "dataset": dataset.dict()
    })


@app.route('/flint/rawdataset/<name>/uploadurl')
def get_raw_dataset_upload_url(name):
    """
    Get an upload url and default params

    RouteParams:
        name: the name of a dataset
    GetParams:
        account: an account
        user: a user
        filename: the name of the file to upload, optional
    Returns:
        a url/params that can be used to upload a file to this dataset
    """

    account = request.args['account']
    user = request.args['user']
    filename = request.args.get('filename')

    dataset = RawDataset.query.filter(RawDataset.account_id == account, RawDataset.name == name).first()

    url, params = dataset.generateUploadURL(filename=filename)

    return jsonify({
        "url": url,
        "params": params
    })


MAX_RESULTS_SIZE = 1024*1024*5

s3_bucket = mixingboard.getConf("s3_bucket")

@app.route('/flint/spark/job/async/results')
def results():

    args = request.args

    account = args['account']
    user = args['user']
    handle = args['handle']

    accountObj = Account.query.filter(Account.id == account).first()
    s3Conn = getS3Conn(accountObj.access_key_id, accountObj.access_key_secret, region=accountObj.region)
    bucket = s3Conn.get_bucket(s3_bucket, validate=False)
    resultsKey = "tmp/%s/spark/%s" % (
        accountObj.iam_username,
        handle
    )

    try:

        key = bucket.get_key(resultsKey)

        logging.info(resultsKey)

        status_code = 200

        result = {}
        if args.get("download") == "1":
            url = key.generate_url(3600, method='GET')
            logger.info("DOWNLOAD URL: %s" % url)
            result = {
                "url": url
            } 
        elif key.size > MAX_RESULTS_SIZE:
            result = {
                "message": "Results too large to show in interface. Download file to view results."
            }
        else:
            result = json.loads(key.get_contents_as_string())
            result = {
                "results": result["data"],
                "options": result["options"]
            }
            if isinstance(result['results'], dict) and result['results'].get("error"):
                status_code = 400

    except Exception as e: 

        logging.info(e)

        return jsonify({
            "error": "No results available for that job"
        }), 404       

    return json.dumps(result), status_code


@app.route('/flint/spark/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE'])
def spark(path):
    """
    Forward a request to the spark job service. No params or
    returns in this doc string as it is for the most part a
    passthrough method.
    """

    args = dict(request.args.items())
    form = dict(request.form.items())
    method = request.method

    account = args.get('account') or form['account']
    user = args.get('user') or form['user']
    cluster = args.get('cluster') or form['cluster']

    jobServerInfo = mixingboard.getService('job-server', cluster=cluster, account=account)[0]

    url = "http://%s:%s/spark/%s" % (jobServerInfo['host'], jobServerInfo['port'], path)

    files = {}
    if "file" in form:
        fileData = form['file']
        filename = form.get('filename', 'job.py')
        if type(fileData) == list:
            fileData = fileData[0]
        files = {'file': (filename, fileData)}
        del form['file']
    elif 'file' in request.files:
        uploadFile = request.files['file']
        files = {'file': (uploadFile.filename, uploadFile)}

    res = getattr(requests, method.lower())(url, params=args, data=form, files=files)

    return res.text, res.status_code
    

if __name__ == "__main__":

    mixingboard.exposeService("flint", port=PORT)
    app.run(host=HOST, port=PORT, debug=DEBUG, threaded=True)
