#!/usr/bin/python

# Standard Library
import argparse
import json
import os
import logging
import socket
import time
import urllib
import uuid

# Third Party
import requests
import yaml
from kazoo.client import KazooClient, KazooState
from kazoo.exceptions import NodeExistsError, NoNodeError

# setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# read config

# parse args
argParser = argparse.ArgumentParser(description='Run the Quarry server.')
argParser.add_argument('--mixing-board', type=str, default='mixingboard.yml', help='Set the mixingboard config file')
argParser.add_argument('-c','--command', type=str, default='serve', help='Specify the command to run')
argParser.add_argument('-r', '--region', type=str, default="us-east-1", help='Set the region to use')
args, _ = argParser.parse_known_args()

# put args in sensible all caps variables
COMMAND = args.command
REGION = args.region
zk_hosts = os.environ["ZKHOSTS"].split("|")
print zk_hosts


AVAILABILITY_ZONE = None

try:
    AVAILABILITY_ZONE = requests.get("http://169.254.169.254/latest/meta-data/placement/availability-zone", timeout=0.5).text
    REGION = AVAILABILITY_ZONE[:-1]
    EXTERNAL = False
except:
    EXTERNAL = True

settings = {}
try:
    settingsFile = open("mixingboard.yml")
    settings = yaml.safe_load(settingsFile)
    settingsFile.close()
except IOError:
    pass

localConf = settings.get("CONF", {})
localServices = settings.get("SERVICES", {})

zk = KazooClient(hosts=",".join(zk_hosts))

host = ""
try:
        host = requests.get('http://169.254.169.254/latest/meta-data/local-ipv4', timeout=0.5).text
except:
        host = socket.gethostbyname(socket.gethostname())

externalHost = None
try:
    externalHost = requests.get('http://169.254.169.254/latest/meta-data/public-ipv4', timeout=0.5).text
except:
    pass

global exposedServices
exposedServices = {}

def stateListener(state):
    global exposedServices
    if state == KazooState.CONNECTED:
        logger.info("Listener called")
        for serviceName, opts in exposedServices.items():
            logger.info('Re-exposing service: %s')
            try:
                exposeService(serviceName, **opts)
            except Exception as e:
                logger.error("Encountered error re-exposing service: '%s'" % str(e))

zk.add_listener(stateListener)

zk.start()


##
# CLUSTER MANAGEMENT METHODS
##


def makeNewCluster(account, name, **kwargs):

    kwargs['name'] = name
    kwargs['stopped'] = False
    kwargs['rebooting'] = False
    jsonData = json.dumps(kwargs)

    nodeName = '/mixingboard/services_%s/clusters/%s' % (account, name)

    try:
        zk.create(nodeName, makepath=True, value=jsonData)
        return True
    except NodeExistsError:
        return False


def incrementClusterWorkers(account, cluster, incrAmount, lock=None):

    numWorkers = 0
    clusterNode = '/mixingboard/services_%s/clusters/%s' % (account, cluster)
    if lock is None: 
        lockNode = '%s/_lock_' % clusterNode
        lock = zk.Lock(lockNode, clusterNode)

    with lock:
        clusterData = json.loads(zk.get(clusterNode)[0])
        clusterData['workers'] = clusterData.get('workers',0) + incrAmount
        zk.set(clusterNode, json.dumps(clusterData))
        numWorkers = clusterData['workers']
    return numWorkers


def alterClusterProperty(account, cluster, propertyName, value, lock=None):

    clusterNode = '/mixingboard/services_%s/clusters/%s' % (account, cluster)
    if lock is None: 
        lockNode = '%s/_lock_' % clusterNode
        lock = zk.Lock(lockNode, clusterNode)

    with lock:
        clusterData = json.loads(zk.get(clusterNode)[0])
        clusterData[propertyName] = value
        zk.set(clusterNode, json.dumps(clusterData))


def lockCluster(account, cluster):

    clusterNode = '/mixingboard/services_%s/clusters/%s' % (account, cluster)
    lockNode = '%s/_lock_' % clusterNode
    return zk.Lock(lockNode, clusterNode)


def lockService(account, serviceName):

    serviceNode = '/mixingboard/services_%s/%s' % (account, serviceName)
    lockNode = '%s/_lock_' % serviceNode
    return zk.Lock(lockNode, serviceNode)


def _fetchAndProcessClusterInfo(account, cluster, serviceInfo=False):

    clusterNode = "/mixingboard/services_%s/clusters/%s" % (account, cluster)
    try:
        clusterInfo = json.loads(zk.get(clusterNode)[0])
    except ValueError:
        # if this cluster doesn't have a valid JSON
        # representation, delete it
        zk.delete(clusterNode, recursive=True)
        return None

    services = zk.get_children(clusterNode)
    serviceInfos = {}
    for service in services:

        serviceNode = "%s/%s" % (clusterNode, service)
        instances = zk.get_children(serviceNode)

        if serviceInfo:

            instanceInfos = {}
            for instance in instances:
                instanceNode = "%s/%s" % (serviceNode, instance)
                instanceInfos[instance] = json.loads(zk.get(instanceNode)[0])

        else:

            if service.find('spark-worker') == 0:
                service = 'spark-worker'

            try:
                serviceInfos[service] += len(instances)
            except KeyError:
                serviceInfos[service] = len(instances)

    clusterInfo['services'] = serviceInfos

    clusterInfo['alive'] = (serviceInfos.get('job-server', 0) > 0) and (serviceInfos.get('spark-worker', 0) > 0) \
                             and (serviceInfos.get('spark-master', 0) > 0 and not clusterInfo.get("rebooting",False))

    if (clusterInfo['alive'] or clusterInfo.get('rebooting')) and clusterInfo.get('stopped'):
        clusterInfo['status'] = "Stopping"
    elif clusterInfo.get('stopped'):
        clusterInfo['status'] = "Stopped"
    elif clusterInfo['alive']:
        clusterInfo['status'] = "Running"
    elif clusterInfo.get('rebooting'):
        clusterInfo['status'] = "Rebooting"
    else:
        clusterInfo['status'] = "Launching"

    return clusterInfo


def listClusters(account, serviceInfo=False):

    nodeName = '/mixingboard/services_%s/clusters' % account

    try:
        clusters = zk.get_children(nodeName)
    except NoNodeError:
        return {}

    clusterInfos = filter(
        lambda x: x is not None, 
        [_fetchAndProcessClusterInfo(account, cluster, serviceInfo) for cluster in clusters]
    )

    return {
        clusterInfo["name"]: clusterInfo
        for clusterInfo in clusterInfos
    }


def getCluster(account, name, serviceInfo=False):

    return _fetchAndProcessClusterInfo(account, name, serviceInfo)


def deleteCluster(account, name):

    nodeName = '/mixingboard/services_%s/clusters/%s' % (account, name)
    return zk.delete(nodeName, recursive=True)


def exposeService(serviceName, port=None, account=None, user=None, cluster=None, **kwargs):
    global exposedServices

    # don't expose a service if we're on a local machine
    if localConf or localServices:
        return

    if port is None:
        raise Exception("You must specify a port when exposing a service")

    if 'host' not in kwargs:
        kwargs['host'] = host
    if externalHost and 'externalHost' not in kwargs:
        kwargs['externalHost'] = externalHost
    kwargs['port'] = port
    kwargs['account'] = account
    kwargs['user'] = user
    kwargs['cluster'] = cluster

    nodeName = ""
    if account:
        if cluster:
            nodeName = '/mixingboard/services_%s/clusters/%s/%s/%s:%s' % (account, cluster, serviceName, kwargs['host'], port)
        else:
            nodeName = '/mixingboard/services_%s/%s/%s:%s' % (account, serviceName, kwargs['host'], port)
    else:
        nodeName = '/mixingboard/services/%s/%s:%s' % (serviceName, kwargs['host'], port)

    jsonData = json.dumps(kwargs)

    try:
        zk.create(nodeName, ephemeral=True, value=jsonData, makepath=True)
    except NodeExistsError:
        zk.set(nodeName, value=jsonData)

    # FIXME this only allows one service of
    # a particular type to run on a single instance
    exposedServices[serviceName] = kwargs

    logger.info("Exposed Service: %s: %s" % (serviceName, kwargs))


def unexposeService(serviceName, port=None, account=None, user=None, cluster=None):

    nodeName = ""
    if account:

        if cluster:

            nodeName = '/mixingboard/services_%s/clusters/%s/%s/%s:%s' % (account, cluster, serviceName, host, port)

        else:

            nodeName = '/mixingboard/services_%s/%s/%s:%s' % (account, serviceName, host, port)

    else:
        nodeName = '/mixingboard/services/%s/%s:%s' % (serviceName, host, port)

    try:
        zk.delete(nodeName)
    except:
        pass


def listAvailableServices(account=None, cluster=None, user=None):

    if localServices:

        return localServices.keys()

    else:

        if account:

            # make this cluster aware
            return zk.get_children('/mixingboard/services_%s' % account)

        else:

            return zk.get_children('/mixingboard/services')


def getService(serviceName, account=None, cluster=None, user=None):

    if serviceName in localServices:

        return [value for value in localServices[serviceName].values()]

    else:

        nodeName = ""
        if account:

            if cluster:

                nodeName = '/mixingboard/services_%s/clusters/%s/%s' % (
                    account,
                    cluster,
                    serviceName
                )

            else:

                nodeName = '/mixingboard/services_%s/%s' % (account, serviceName)

        else:
            nodeName = '/mixingboard/services/%s' % (serviceName)

        childNodes = zk.get_children(nodeName)

        serviceNodes = []
        for childNode in childNodes:
            serviceNode = json.loads(zk.get("%s/%s" % (nodeName, childNode))[0])
            if EXTERNAL and 'externalHost' in serviceNode:
                serviceNode['host'] = serviceNode['externalHost']
                del serviceNode['externalHost']
            serviceNodes.append(serviceNode)

        return serviceNodes

def discoverService(serviceName, onDiscover, account=None, cluster=None, user=None):

    if serviceName in localServices:

        onDiscover([value for value in localServices[serviceName].values()])

    else:

        nodeName = ""
        if account:

            if cluster:

                nodeName = '/mixingboard/services_%s/clusters/%s/%s' % (
                    account,
                    cluster,
                    serviceName
                )

            else:

                nodeName = '/mixingboard/services_%s/%s' % (account, serviceName)

        else:
            nodeName = '/mixingboard/services/%s' % (serviceName)

        zk.ensure_path(nodeName)

        @zk.ChildrenWatch(nodeName)
        def unpack(children):
            if len(children) == 0:
                return
            childData = []
            for child in children:
                serviceNode = json.loads(zk.get("%s/%s" % (nodeName, child))[0])
                if EXTERNAL and 'externalHost' in serviceNode:
                    serviceNode['host'] = serviceNode['externalHost']
                    del serviceNode['externalHost']
                childData.append(serviceNode)
            onDiscover(childData)


def setConf(key, value, account=None, cluster=None, user=None):

    jsonValue = json.dumps({
        "value": value
    })

    nodeName = ''
    if account:
        # TODO add cluster semantics
        nodeName = '/mixingboard/config_%s/%s' % (account, key)
    else:
        nodeName = '/mixingboard/config/%s' % (key)

    try:
        zk.create(nodeName, value=jsonValue, makepath=True)
    except NodeExistsError:
        zk.set(nodeName, value=jsonValue)


def getConf(key, account=None, cluster=None, user=None):

    if localConf:

        if account:

            return localConf["__account__"][key]

        else:

            return localConf[key]

    else:

        if account:

            # TODO add cluster semantics
            return json.loads(zk.get('/mixingboard/config_%s/%s' % (account, key))[0])['value']

        else:

            return json.loads(zk.get('/mixingboard/config/%s' % (key))[0])['value']


def watchConf(key, onChange, account=None, cluster=None, user=None):

    nodeName = ""
    if account:
        # TODO add cluster semantics
        nodeName = '/mixingboard/config_%s/%s' % (accoun, key)
    else:
        nodeName = '/mixingboard/config/%s' % (key)

    @zk.DataWatch(nodeName)
    def unpack(data, stat):
        onChange(json.loads(data)['value'])


def deleteConf(key, account=None, cluster=None, user=None):

    nodeName = ""
    if account:
        # TODO add cluster semantics
        nodeName = '/mixingboard/config_%s/%s' % (account, key)
    else:
        nodeName = '/mixingboard/config/%s' % (key)

    zk.delete(nodeName)


def monitor(userData=None):

    import subprocess
    import shutil
    from jinja2 import Template

    def templateFromFile(filename):
        return Template(open(filename).read())

    # SUPERVISORTCTL START STUFF

    if not userData:

        userData = {}
        try:
            userData = requests.get("http://169.254.169.254/latest/user-data", timeout=0.5).json()
        except:
            pass

    roles = userData['roles']
    account = userData['account']
    cluster = userData['cluster']
    user = userData['user']

    iam_username = userData['iam_username']
    access_key_secret = userData['access_key_secret']
    access_key_id = userData['access_key_id']
    db_host = userData['db_host']

    if "spark-master" in roles or 'job-server' in roles or 'spark-worker' in roles:

        hiveSiteEnvTemplate = templateFromFile('/opt/spark/conf/hive-site.xml.template')

        with open("/opt/spark/conf/hive-site.xml", "w") as dest:
            dest.write(hiveSiteEnvTemplate.render(
                iam_username=iam_username,
                iam_username_16=iam_username[:16],
                access_key_id=access_key_id,
                access_key_secret=access_key_secret,
                db_host=db_host,
                region=REGION
            ))

    if "spark-master" in roles:

        sparkMasterSupervisorTemplate = templateFromFile('/etc/supervisor/conf.d/spark-master.conf.template')
        sparkMasterEnvTemplate = templateFromFile('/opt/spark/conf/spark-env.sh.template')

        with open("/etc/supervisor/conf.d/spark-master.conf", "w") as dest:
            dest.write(sparkMasterSupervisorTemplate.render(
                master_ip=host
            ))

        zk_dir = "/spark_account_%s_cluster_%s" % (
            account,
            urllib.quote(cluster)
        )

        zk.delete(zk_dir, recursive=True)

        with open("/opt/spark/conf/spark-env.sh", "w") as dest:
            dest.write(sparkMasterEnvTemplate.render(
                master_ip=host,
                zk_dir=zk_dir,
                zk_hosts=",".join(zk_hosts)
            ))

        subprocess.call(["sudo","supervisorctl","reread"])
        subprocess.call(["sudo","supervisorctl","update"])
        subprocess.call(["sudo","supervisorctl","restart","spark-master"])

    if 'spark-worker' in roles:

        # one spark worker per core
        sparkWorkerSupervisorTemplate = templateFromFile('/etc/supervisor/conf.d/spark-worker.conf.template')
        sparkWorkerEnvTemplate = templateFromFile('/opt/spark/conf/spark-env.sh.template')

        def discoveredSparkMastersForWorkers(masters):

            logger.info("GOT NEW SPARK MASTERS FOR SPARK WORKERS")

            numWorkers = int(subprocess.Popen(['nproc'], stdout=subprocess.PIPE).communicate()[0].strip())

            masterURI = "spark://%s" % ",".join(
                ["%s:%s" % (master['host'], master['port']) for master in masters]
            )

            with open("/etc/supervisor/conf.d/spark-worker.conf", "w") as dest:
                dest.write(sparkWorkerSupervisorTemplate.render(
                    master=masterURI,
                    num_procs=numWorkers
                ))

            with open("/opt/spark/conf/spark-env.sh", "w") as dest:
                dest.write(sparkWorkerEnvTemplate.render())

            subprocess.call(["sudo","supervisorctl","reread"])
            subprocess.call(["sudo","supervisorctl","update"])
            subprocess.call(["sudo","supervisorctl","restart","spark-worker:"])

            logger.info("GOT NEW SPARK MASTERS FOR SPARK WORKERS")

        discoverService('spark-master', discoveredSparkMastersForWorkers, account=account, cluster=cluster)

    if 'job-server' in roles:

        jobServerSupervisorTemplate = templateFromFile('/etc/supervisor/conf.d/job-server.conf.template')

        with open("/etc/supervisor/conf.d/job-server.conf", "w") as dest:
            dest.write(jobServerSupervisorTemplate.render(
                account=account,
                cluster=cluster,
                iam_username=iam_username,
                access_key_id=access_key_id,
                access_key_secret=access_key_secret
            ))

        # the job server does the master look up on its own,
        # we just need to wait for the master to come online
        # before we start it up
        def discoveredSparkMastersForJobServer(masters):

            subprocess.call(["sudo","supervisorctl","reread"])
            subprocess.call(["sudo","supervisorctl","update"])
            subprocess.call(["sudo","supervisorctl","restart","job-server"])

        discoverService('spark-master', discoveredSparkMastersForJobServer, account=account, cluster=cluster)

    if 'streamer' in roles:

        streamerTemplate = templateFromFile('/etc/td-agent/td-agent.conf.template')

        with open("/etc/td-agent/td-agent.conf", "w") as dest:
            dest.write(streamerTemplate.render(
                account=account,
                iam_username=iam_username,
                iam_username_16=iam_username[:16],
                access_key_id=access_key_id,
                access_key_secret=access_key_secret,
                region=REGION,
                db_host=db_host
            ))

        subprocess.call(["sudo","service","td-agent","restart"])

    # MONITORING STUFF

    PROCESS_TYPES = {
        "spark-master": 7077,
        "shark-master": 10000,
        "spark-worker": 8080,
        "streamer": 8888
    }

    if cluster:
        alterClusterProperty(account, cluster, "rebooting", False)

    logger.info("MONITORING LOCAL SERVICES")
    while True:

        lines = subprocess.Popen(["sudo", "supervisorctl","status"],stdout=subprocess.PIPE).communicate()[0].split("\n")[:-1]
        lines = [filter(lambda x: x, line.split(" "))[:2] for line in lines]
        print lines
        supervisorStatuses = {
            line[0]: line[1]
            for line in lines
        }

        for service, status in supervisorStatuses.items():
            
            serviceParts = service.split(":")

            if serviceParts[0] not in PROCESS_TYPES.keys():
                continue

            if status == "RUNNING":
                exposeService(serviceParts[-1], PROCESS_TYPES[serviceParts[0]], account=account, 
                              cluster=cluster if serviceParts[-1] != "streamer" else None)
            else:
                unexposeService(serviceParts[-1], PROCESS_TYPES[serviceParts[0]], account=account,
                                cluster=cluster if serviceParts[-1] != "streamer" else None)

        time.sleep(5)


def serve():

    from flask import Flask
    from flask import jsonify
    from flask import render_template

    app = Flask(__name__, static_url_path='/static', static_folder='./static')

    @app.route("/")
    def index():

        return render_template("base.html")

    def services(account=None, cluster=None):

        if localServices and not account:

            return jsonify({
                "services": localServices
            })

        else:

            rootNode = ""
            if account:
                if cluster:
                    rootNode = "/mixingboard/services_%s/clusters/%s" % (account, cluster)
                else:
                    rootNode = "/mixingboard/services_%s" % account
            else:
                rootNode = "/mixingboard/services"

            serviceNames = zk.get_children(rootNode)

            serviceNodes = {}
            for service in serviceNames:
                serviceNodes[service] = zk.get_children("%s/%s" % (rootNode, service))

            services = {}
            for service, nodes in serviceNodes.items():
                serviceDict = {}
                for node in nodes:
                    serviceDict[node] = json.loads(zk.get("%s/%s/%s" % (rootNode, service, node))[0])
                services[service] = serviceDict

            return jsonify({
                "services": services
            })

    @app.route("/services")
    def services_base():

        return services()


    @app.route("/services/<account>")
    def service_account(account):

        return services(account=account)


    @app.route("/services/<account>/cluster/<cluster>")
    def service_account_cluster(account, cluster):

        return services(account=account, cluster=cluster)


    @app.route("/conf/<key>/set/<path:value>")
    def conf_set(key, value):

        value = json.loads(value)

        # try to coerce the value
        try:
            value = int(value)
        except:
            try:
                value = float(value)
            except:
                try:
                    value = json.loads(value)
                except:
                    pass

        setConf(key, value)

        return jsonify({})

    @app.route("/conf")
    def conf():

        if localConf:

            return jsonify({
                "conf": localConf
            })

        else:

            confNames = zk.get_children("/mixingboard/config")

            conf = {}
            for confName in confNames:
                conf[confName] = json.loads(zk.get("/mixingboard/config/%s" % confName)[0])["value"]


            return jsonify({
                "conf": conf
            })

    app.run(debug=True,host='0.0.0.0',port=12314)


if __name__ == "__main__":

    if COMMAND == "serve":

        serve()

    elif COMMAND == "monitor":

        monitor()
