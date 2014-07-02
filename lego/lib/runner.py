# Standard Library
import json
import time
import logging
from collections import defaultdict
from threading import Thread, Lock
from uuid import getnode as get_mac

# Third Party
import mixingboard
import redis
import requests
from celery import Celery
from chassis.models import Workflow, User, Account, JobHistory
from chassis.database import db_session
from chassis.util import makeHandle

# Local


# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Start celery stuff
rabbitmq = mixingboard.getConf('rabbitmq')
celeryApp = Celery('runner', broker='amqp://%s:%s//' % (
    rabbitmq['host'],
    rabbitmq['port'],
))
celeryApp.conf.CELERY_TASK_SERIALIZER = 'json'
celeryApp.conf.CELERY_HIJACK_ROOT_LOGGER = False
if mixingboard.EXTERNAL:
    celeryApp.conf.CELERY_DEFAULT_QUEUE = 'lego-%s' % get_mac()
else:
    celeryApp.conf.CELERY_DEFAULT_QUEUE = 'lego'


# Start redis stuff
redisInfo = mixingboard.getConf('redis')
redisClient = redis.StrictRedis(host=redisInfo['host'], port=int(redisInfo['port']), db=0)


# setup shark configurations
global SHARK_URL_FORMAT
SHARK_URL_FORMAT = None
def setSharkURLFormat(sharkServers):
    global SHARK_URL_FORMAT
    shark = sharkServers[0]
    SHARK_URL_FORMAT = "http://%s:%s/shark" % (shark["host"], shark["port"])
    logging.info("GOT SHARK SERVICE: %s" % SHARK_URL_FORMAT)

mixingboard.discoverService("shark",setSharkURLFormat)


# setup jaunt configurations
global JAUNT_URL_FORMAT
JAUNT_URL_FORMAT = None
def setJauntURLFormat(jauntServers):
    global JAUNT_URL_FORMAT
    jauntServer = jauntServers[0]
    JAUNT_URL_FORMAT = "http://%s:%s/jaunt" % (jauntServer["host"], jauntServer["port"])
    logging.info("GOT JAUNT SERVICE: %s" % JAUNT_URL_FORMAT)

mixingboard.discoverService("jaunt",setJauntURLFormat)


# setup redshirt configurations
global REDSHIRT_URL_FORMAT
REDSHIRT_URL_FORMAT = None
def setSharkURLFormat(redShirtServers):
    global REDSHIRT_URL_FORMAT
    redShirt = redShirtServers[0]
    REDSHIRT_URL_FORMAT = "http://%s:%s/redshirt" % (redShirt["host"], redShirt["port"])
    logging.info("GOT REDSHIRT SERVICE: %s" % REDSHIRT_URL_FORMAT)

mixingboard.discoverService("redshirt",setSharkURLFormat)


# setup flint configurations
global FLINT_URL_FORMAT
FLINT_URL_FORMAT = None
def setFlintURLFormat(flintServers):
    global FLINT_URL_FORMAT
    flint = flintServers[0]
    FLINT_URL_FORMAT = "http://%s:%s/flint" % (flint["host"], flint["port"])
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

    jobHistory = JobHistory(account_id=accountId, user_id=userId, event=event, jobId=jobId, jobHandle=jobHandle, jobType="workflow", data=data)
    db_session.add(jobHistory)
    db_session.commit()

    return jobHistory


def getHandleInfo(handle):

    info = redisClient.hgetall('workflow:%s' % handle)

    dumpedInfo = {}
    for key, value in info.items():
        dumpedInfo[key] = json.loads(value)

    dumpedInfo['handle'] = handle

    return dumpedInfo


def isCancelled(handle):
    
    return redisClient.hget('workflow:%s' % handle, "cancelled")


def setHandleInfo(handle, account, user, **kwargs):

    cancelled = redisClient.hget('workflow:%s' % handle, "cancelled")

    if kwargs.get('finished') or cancelled:
        redisClient.zrem("workflows:%s" % account, handle)
    else:
        redisClient.zadd("workflows:%s" % account, int(time.time()), handle)
        # TODO remove workflows that have a timestamp greater than X seconds old

    redisClient.hmset('workflow:%s' % handle, {key: json.dumps(value) for key, value in kwargs.items()})

    if 'message' in kwargs:
        logger.info(kwargs['message'])


def cancelHandle(handle):

    redisClient.hmset('workflow:%s' % handle, {"cancelled": json.dumps(True)})
    return getHandleInfo(handle)


def getRunningWorkflows(account, user):

    handles = redisClient.zrangebyscore("workflows:%s" % account, int(time.time())-60, '+inf')
    infos = []
    for handle in handles:
        infos.append(getHandleInfo(handle))

    return infos


def runWorkflow(workflow, options={}):

    handle = makeHandle()

    workflow = workflow.dict()

    setHandleInfo(handle, workflow['account_id'], workflow['user_id'], totalSteps=len(workflow['steps']), stepsComplete=0, finished=False,
                  started=int(time.time()*1000), title=workflow['title'], message="Warming up...")
    options['stepsComplete'] = -1

    makeHistory(workflow['account_id'], workflow['user_id'], "start_workflow", workflow['id'], handle)

    nextWorkflowStep(workflow, options, handle)

    return handle


def workflowFinished(workflow, handle, options={}, error=None):

    logger.info("OPTIONS: %s" % options)
    logger.info("Finished running workflow")
    makeHistory(workflow['account_id'], workflow['user_id'], "finish_workflow", workflow['id'], handle)

    message = "Workflow finished"
    if error:
        message += " in failure"
        logger.error(error)

    setHandleInfo(handle, workflow['account_id'], workflow['user_id'], currentStep=None, progress=None, error=error, finished=True, message=message)

    if options.get("bootedCluster"):
        logger.info("Shutting down booted cluster '%s'" % options['cluster'])
        requests.post(
            "%s/cluster/%s/shutdown" % (
                REDSHIRT_URL_FORMAT, 
                options['cluster']
            ),
            data={
                "account": workflow['account_id'],
                "user": workflow['user_id']
            }
        )

    notify_users = workflow['notify_users']

    if len(notify_users) > 0:

        users = []
        if notify_users[0] == -1:

            account = Account.query.filter(Account.id == workflow['account_id']).first()
            users = account.users

        else:

            users = [User.query.filter(User.id == userId).first() for userId in notify_users]

        subject = None
        body = None
        if error:
            subject = "Workflow '%s' failed" % workflow['title']
            body = """
            <p>The workflow '%(title)s' has failed with following error:</p>
            <p>%(error)s</p>
            """ % {
                "title": workflow['title'],
                "error": error
            }
        else:
            subject = "Workflow '%s' has succeeded" % workflow['title']
            body = """
            <p>The workflow '%(title)s' has succeeded.</p>
            """ % {
                "title": workflow['title']
            }

        for user in users:

            if user is not None:
                user.sendEmail(subject, body)


def nextWorkflowStep(workflow, options, handle):

    if isCancelled(handle):
        workflowFinished(workflow, handle, options=options, error="Workflow has been cancelled")
        return

    # make sure we have a cluster to run on
    if options.get('cluster') is None:

        cluster = workflow['cluster']
        
        if cluster['action'] == 'start':
            startBootCluster.delay(workflow, cluster, options, handle)
            return

        elif cluster['action'] == 'pick':
            options['cluster'] = cluster['name']

    options['stepsComplete'] += 1
    setHandleInfo(handle, workflow['account_id'], workflow['user_id'], stepsComplete=options['stepsComplete'])

    if len(workflow['steps']) == 0:

        workflowFinished(workflow, handle, options)

    else:

        step = workflow['steps'][0]
        workflow['steps'] = workflow['steps'][1:]

        setHandleInfo(handle, workflow['account_id'], workflow['user_id'], currentStep=step)

        stepType = step['type']

        if step['type'] == "sql":
            startRunQuery.delay(workflow, step, options, handle)

        elif step['type'] == "python":
            startRunJob.delay(workflow, step, options, handle)

        elif step['type'] in { "import", "export" }:
            startRunDatajob.delay(workflow, step, options, handle)

        else:
            workflowFinished(workflow, handle, options, error="Uknown job type %s" % step['type'])


###
# BOOT CLUSTER ACTION
###

MAX_BOOT_WAIT = 600

@celeryApp.task
def startBootCluster(workflow, cluster, options, handle):

    if isCancelled(handle):
        workflowFinished(workflow, handle, options=options, error="Workflow has been cancelled")
        return

    makeHistory(workflow['account_id'], workflow['user_id'], "start_boot_cluster", 
                workflow['id'], handle, {'name': cluster['name']})

    account = workflow['account_id']
    user = workflow['user_id']
    workers = cluster["workers"]
    clusterName = cluster["name"]

    # begin the cluster launch
    res = requests.post("%s/launch/cluster" % REDSHIRT_URL_FORMAT, data={
        "account": account,
        "user": user,
        "workers": workers,
        "clusterName": clusterName
    })

    if res.status_code != 200:
        workflowFinished(workflow, handle, options=options, error=res.text)
        return
    
    setHandleInfo(handle, workflow['account_id'], workflow['user_id'], progress=None, message="Cluster launch started")

    # wait until the cluster is done launching
    start = time.time()
    maxWait = start + MAX_BOOT_WAIT

    options['cluster'] = clusterName
    options['bootedCluster'] = True

    waitBootCluster.delay(workflow, cluster, options, handle, maxWait)

@celeryApp.task
def waitBootCluster(workflow, cluster, options, handle, maxWait):

    if isCancelled(handle):
        workflowFinished(workflow, handle, options=options, error="Workflow has been cancelled")
        return

    account = workflow['account_id']
    user = workflow['user_id']
    clusterName = cluster["name"]

    if time.time() > maxWait:
        workflowFinished(workflow, handle, options=options, error=json.dumps({
            "error": "Cluster didn't boot up within %s seconds" % maxWait
        }))
        return

    res = requests.get("%s/cluster/%s" % (REDSHIRT_URL_FORMAT, clusterName), params={
        "account": account,
        "user": user
    })

    if res.status_code != 200:
        workflowFinished(workflow, handle, options=options, error=res.text)
        return

    if not res.json()["cluster"]["alive"]:

        setHandleInfo(handle, account, user, progress=res.json(), message="Cluster is booting up")
        waitBootCluster.delay(workflow, cluster, options, handle, maxWait)

    else:

        setHandleInfo(handle, account, user, message="Cluster has finished booting")

        makeHistory(workflow['account_id'], workflow['user_id'], "finish_boot_cluster", 
                    workflow['id'], handle, {'name': cluster['name']})

        nextWorkflowStep(workflow, options, handle)



##
# RUN QUERY ACTION
##

@celeryApp.task
def startRunQuery(workflow, step, options, infoHandle):
        
    if isCancelled(infoHandle):
        workflowFinished(workflow, handle, options=options, error="Workflow has been cancelled")
        return

    historyData = {}
    if "id" in step:
        historyData['id'] = step['id']

    makeHistory(workflow['account_id'], workflow['user_id'], "start_run_query", 
                workflow['id'], infoHandle, historyData)

    account = workflow['account_id']
    user = workflow['user_id']
    queryId = step.get("id")
    sql = step.get("sql")
    queryOptions = step.get("options",{})
    cluster = options.get("cluster")

    if cluster is None:
        workflowFinished(workflow, infoHandle, options=options, error=json.dumps({
            "error": "No cluster was provided to run this query on"
        }))
        return

    res = None
    if queryId:

        # begin running the saved query
        res = requests.post(
            "%s/query/%s/run" % (
                SHARK_URL_FORMAT,
                queryId 
            ),
            data={
                "cluster": cluster,
                "account": account,
                "user": user,
                "options": json.dumps(queryOptions)
            }
        )

    elif sql:

        # begin running the ad-hoc query
        res = requests.post(
            "%s/sql" % SHARK_URL_FORMAT,
            data={
                "query": sql, 
                "cluster": cluster,
                "account": account,
                "user": user,
                "options": json.dumps(queryOptions)
            }
        )

    else:
        workflowFinished(workflow, infoHandle, options=options, 
                         error=json.dumps({"error":"SQL workflow step must have either an ad-hoc query or a query id"}))
        return

    if res.status_code != 200:
        workflowFinished(workflow, infoHandle, options=options, error=res.text)
        return
    
    setHandleInfo(infoHandle, account, user, progress=None, message="Query successfully submitted")

    queryHandle = res.json()["handle"]

    waitRunQuery.delay(workflow, step, options, infoHandle, queryHandle)

def cancelJob(handle, account, user, cluster):

    logging.info("Cancelling job with handle '%s'" % handle)

    requests.post("%s/spark/job/async/cancel" % FLINT_URL_FORMAT, {
        "cluster": cluster,
        "account": account,
        "user": user,
        "handle": handle
    })

@celeryApp.task
def waitRunQuery(workflow, step, options, infoHandle, queryHandle):
            
    account = workflow['account_id']
    user = workflow['user_id']
    cluster = options.get("cluster")

    if isCancelled(infoHandle):
        
        workflowFinished(workflow, handle, options=options, error="Workflow has been cancelled")
        cancelJob(queryHandle, account, user, cluster)
        return

    res = requests.get("%s/progress" % SHARK_URL_FORMAT, params={
        "cluster": cluster,
        "account": account,
        "user": user,
        "handle": queryHandle
    })

    if res.status_code != 200:
        workflowFinished(workflow, infoHandle, options=options, error=res.text)
        return

    setHandleInfo(infoHandle, account, user, progress=res.json(), message="Waiting for query to complete")

    if res.json()["running"]:

        waitRunQuery.delay(workflow, step, options, infoHandle, queryHandle)

    else:

        res = requests.get("%s/results" % SHARK_URL_FORMAT, params={
            "account": account,
            "user": user,
            "handle": queryHandle
        })

        setHandleInfo(infoHandle, account, user, progress=None,  message="Query finished")

        historyData = {}
        if "id" in step:
            historyData['id'] = step['id']

        makeHistory(workflow['account_id'], workflow['user_id'], "finish_run_query", 
                    workflow['id'], infoHandle, historyData)

        try:
            if isinstance(res.json()['results'], dict) and 'error' in res.json()['results']:
                workflowFinished(workflow, infoHandle, options=options, error=res.text)
                return
        except Exception as e:
            logger.error(e)
            workflowFinished(workflow, infoHandle, options=options, error=res.text)
            return

        if res.status_code != 200:
            workflowFinished(workflow, infoHandle, options=options, error=res.text)
            return

        nextWorkflowStep(workflow, options, infoHandle)



##
# PYTHON JOB TASK
##

@celeryApp.task
def startRunJob(workflow, step, options, infoHandle):
    
    if isCancelled(infoHandle):
        workflowFinished(workflow, handle, options=options, error="Workflow has been cancelled")
        return

    makeHistory(workflow['account_id'], workflow['user_id'], "start_run_job", 
                workflow['id'], infoHandle, {'id': step['id']})

    account = workflow['account_id']
    user = workflow['user_id']
    jobId = step["id"]
    jobOptions = step.get("options", {})
    cluster = options.get("cluster")

    if cluster is None:
        workflowFinished(workflow, infoHandle, options=options, error=json.dumps({
            "error": "No cluster was provided to run this job on"
        }))
        return

    res = requests.post(
        "%s/job/%s/run" % (
            FLINT_URL_FORMAT,
            jobId 
        ),
        data={
            "cluster": cluster,
            "account": account,
            "user": user,
            "options": json.dumps(jobOptions)
        }
    )

    if res.status_code != 200:
        workflowFinished(workflow, infoHandle, options=options, error=res.text)
        return
    
    setHandleInfo(infoHandle, account, user, progress=None, message="Job successfully submitted")

    flintHandle = res.json()["handle"]

    waitFlintHandle.delay(workflow, step, options, infoHandle, flintHandle)



##
# DATAJOB TASK
##

def startRunDatajob(workflow, step, options, infoHandle):
    
    if isCancelled(infoHandle):
        workflowFinished(workflow, handle, options=options, error="Workflow has been cancelled")
        return

    makeHistory(workflow['account_id'], workflow['user_id'], "start_run_%s_job" % step['type'], 
                workflow['id'], infoHandle, {'id': step['id']})

    account = workflow['account_id']
    user = workflow['user_id']
    datajobId = step["id"]
    jobOptions = step.get("options",{})
    cluster = options.get("cluster")
    jobType = step["type"]

    if cluster is None:
        workflowFinished(workflow, infoHandle, options=options, error=json.dumps({
            "error": "No cluster was provided to run this job on"
        }))
        return

    res = requests.post(
        "%s/datajob/%s/run" % (
            JAUNT_URL_FORMAT,
            datajobId 
        ),
        data={
            "cluster": cluster,
            "account": account,
            "user": user,
            "options": json.dumps(jobOptions)
        }
    )

    if res.status_code != 200:
        workflowFinished(workflow, infoHandle, options=options, error=res.text)
        return
    
    setHandleInfo(infoHandle, account, user, progress=None, message="%s job successfully submitted" % jobType.capitalize())

    flintHandle = res.json()["handle"]

    waitFlintHandle.delay(workflow, step, options, infoHandle, flintHandle, jobType=jobType)



##
# FLINT HANDLE WATCHER TASK
##

@celeryApp.task
def waitFlintHandle(workflow, step, options, infoHandle, flintHandle, jobType=""):

    if isCancelled(infoHandle):
        workflowFinished(workflow, handle, options=options, error="Workflow has been cancelled")
        cancelJob(flintHandle, account, user, cluster)
        return

    account = workflow['account_id']
    user = workflow['user_id']
    cluster = options.get("cluster")

    if jobType and jobType[-1] != " ":
        jobType += " "
        
    res = requests.get("%s/spark/job/async/progress" % FLINT_URL_FORMAT, params={
        "cluster": cluster,
        "account": account,
        "user": user,
        "handle": flintHandle
    })

    if res.status_code != 200:
        workflowFinished(workflow, infoHandle, options=options, error=res.text)
        return

    setHandleInfo(infoHandle, account, user, progress=res.json(), message="Waiting for %sjob to complete" % jobType)

    if res.json()["running"]:

        waitFlintHandle.delay(workflow, step, options, infoHandle, flintHandle, jobType)

    else:

        res = requests.get("%s/spark/job/async/results" % FLINT_URL_FORMAT, params={
            "account": account,
            "user": user,
            "handle": flintHandle
        })

        if jobType:
            jobType = jobType.replace(' ','_')

        makeHistory(workflow['account_id'], workflow['user_id'], "finish_run_%sjob" % jobType, 
                    workflow['id'], infoHandle, {'id': step['id']})

        try:
            if isinstance(res.json()['results'], dict) and 'error' in res.json()['results']:
                workflowFinished(workflow, infoHandle, options=options, error=res.text)
                return
        except Exception as e:
            logger.info(e)
            workflowFinished(workflow, infoHandle, options=options, error=res.text)
            return

        message = "%s job has finished" % jobType.capitalize() if jobType else "Job has finished"
        setHandleInfo(infoHandle, account, user, progress=res.json(), message=message)

        nextWorkflowStep(workflow, options, infoHandle)
