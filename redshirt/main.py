#!/usr/bin/python


# Standard Library
import argparse
import datetime
import json
import logging
import os
import time
from threading import Thread, Lock

# Third Party
import mixingboard
from chassis.aws import getMasterEC2Conn, getMasterRoute53Conn
from chassis.database import db_session, init_db
from chassis.models.user import User
from chassis.models.account import Account
from flask import Flask, jsonify, request

# Local


# parse arguments
argParser = argparse.ArgumentParser(description='Run the Quarry server.')
argParser.add_argument('-d', '--debug', action='store_true', help='Turn on debug mode')
argParser.add_argument('-p', '--port', type=int, default=2364, help='Set the port')
argParser.add_argument('-H', '--host', type=str, default="127.0.0.1", help='Set the host')
argParser.add_argument('-c', '--command', type=str, default='serve', help='Operation to perform (serve, db)')
args, _ = argParser.parse_known_args()

# extract arguments to sane, all caps variable names
DEBUG = args.debug
HOST = args.host
PORT = args.port
COMMAND = args.command


# set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# Standard Flask Initialization
app = Flask(__name__)

@app.teardown_appcontext
def shutdown_session(exception=None):
    db_session.remove()


def getSecurityGroup(account_id):

    return Account.query.filter(Account.id == account_id).first().security_group


INSTANCE_TYPES = {
    "m1.small": {
        "allowed_roles": [ 
            "streamer",
            "spark-master",
            "job-server"
        ]
    },
    "m1.medium": {
        "workers": 1,
        "allowed_roles": [ 
            "streamer",
            "spark-master",
            "spark-worker", 
            "job-server"
        ]
    },
    "m1.large": {
        "workers": 2,
        "allowed_roles": [ 
            "streamer",
            "spark-master",
            "spark-worker", 
            "job-server"
        ]
    },
    "m1.xlarge": {
        "workers": 4,
        "allowed_roles": [ 
            "streamer",
            "spark-master",
            "spark-worker", 
            "job-server"
        ]
    },
    "m3.medium": {
        "workers": 1,
        "allowed_roles": [ 
            "streamer",
            "spark-master",
            "spark-worker", 
            "job-server"
        ]
    },
    "m3.large": {
        "workers": 2,
        "allowed_roles": [ 
            "streamer",
            "spark-master",
            "spark-worker", 
            "job-server"
        ]
    },
    "m3.xlarge": {
        "workers": 4,
        "allowed_roles": [ 
            "streamer",
            "spark-master",
            "spark-worker", 
            "job-server"
        ]
    },
}


def getSparkMasters(account_id, user_id):

    return mixingboard.getService("spark-master", user=user_id, account=account_id)


def getSparkMaster(account_id, user_id):

    return getSparkMasters(account_id, user_id)[0]

def setupStreamerDNS(instanceId, organization):

    conn = getMasterEC2Conn(region=mixingboard.REGION)

    while True:
        instances = conn.get_only_instances(instance_ids=[instanceId])
        if len(instances) and instances[0].public_dns_name:
            route53Conn = getMasterRoute53Conn(region=mixingboard.REGION)
            zone = route53Conn.get_zone("quarry.io")
            cname = "%s.stream.quarry.io" % organization.lower()
            # try and delete the cname if it already exists
            try:
                zone.delete_cname(cname)
            except AttributeError:
                pass
            zone.add_cname("%s.stream.quarry.io" % organization.lower(), instances[0].public_dns_name, ttl=30)
            break
        time.sleep(5)

def deleteStreamerDNS(organization):
    route53Conn = getMasterRoute53Conn(region=mixingboard.REGION)
    zone = route53Conn.get_zone("quarry.io")
    cname = "%s.stream.quarry.io" % organization.lower()
    try:
        zone.delete_cname(cname)
    except AttributeError:
        pass

HIVE_DB_HOST = mixingboard.getConf("hive_db_root")['host']
DEFAULT_SUBNET = mixingboard.getConf("default_subnet")

def launchInstances(user, account, groups, cluster=None, launchCluster=False):

    ami = mixingboard.getConf("spark_ami")

    if cluster:
        if launchCluster:
            if not mixingboard.makeNewCluster(account, name=cluster, ami=ami,
                                              created=int(round(time.time() * 1000))):
                return "A cluster already exists with that name", None, None
        else:
            info = mixingboard.getCluster(account, cluster)
            ami = info['ami']

    # verify that each group is valid before we launch any
    for i, group in enumerate(groups):

        logging.info("GROUP: %s" % group)
        instanceType = group["instanceType"]
        count = group.get("count",1)
        spotPrice = group.get("spotPrice")
        roles = group.get('roles')

        if spotPrice:
            try:
                spotPrice = float(spotPrice)
            except:
                return "Group %s: Spot price must be a valid decimal number" % i, None, None

        if instanceType not in INSTANCE_TYPES.keys():
            return "Group %s: You can only launch instances of types %s" % (i, ",".join(INSTANCE_TYPES.keys())), None, None

        for role in roles:
            if role not in INSTANCE_TYPES[instanceType]["allowed_roles"]:
                return "Group %s: You can only launch roles of types '%s' on an '%s'" % (i, ",".join(roles), instanceType), None, None

    launchedInstances = []
    openedSpots = []

    accountObj = Account.query.filter(Account.id == account).first()
    conn = getMasterEC2Conn(region=mixingboard.REGION)
    securityGroup = getSecurityGroup(account)

    for group in groups:

        instanceType = group["instanceType"]
        count = group.get("count",1)
        spotPrice = group.get("spotPrice")
        roles = group.get('roles')

        kwargs = {
            "subnet_id": accountObj.subnet_id if accountObj.subnet_id else DEFAULT_SUBNET,
            "instance_type": instanceType,
            "count": count, 
            "security_group_ids": [securityGroup], 
            "key_name": mixingboard.getConf("key_pair"),
            "user_data": json.dumps({
                "roles": roles,
                "account": account,
                "cluster": cluster,
                "iam_username": accountObj.iam_username,
                "access_key_id": accountObj.access_key_id,
                "access_key_secret": accountObj.access_key_secret,
                "user": user,
                "db_host": HIVE_DB_HOST,
            })
        }

        # if we're launching shit from a local setup, make it 
        # accessible EVVVVEERRRYWHERE
        if mixingboard.EXTERNAL or "streamer" in roles:

            import boto.ec2.networkinterface
            interface = boto.ec2.networkinterface.NetworkInterfaceSpecification(subnet_id=kwargs["subnet_id"],
                                                                    groups=[securityGroup],
                                                                    associate_public_ip_address=True)
            interfaces = boto.ec2.networkinterface.NetworkInterfaceCollection(interface)
            kwargs["network_interfaces"] = interfaces
            del kwargs["subnet_id"]
            del kwargs["security_group_ids"]

        if spotPrice:

            reqs = conn.request_spot_instances(float(spotPrice), ami, **kwargs) 

            def tagInstancesWhenLaunched(reqs):

                # don't wait more than a few minutes
                cancelTime = time.time()+600

                request_ids = [req.id for req in reqs]

                while len(request_ids) > 0:

                    time.sleep(5)

                    reqs = conn.get_all_spot_instance_requests(request_ids=request_ids)

                    if time.time() > cancelTime:
                        logging.info("Cancelling %s requests for ACCOUNT(%s) due to lack of fullfilment" % (len(request_ids), account))
                        return conn.cancel_spot_instance_requests(request_ids=request_ids)

                    for req in reqs:
                        if req.state != "open":
                            if req.instance_id is not None:
                                conn.create_tags([req.instance_id], {
                                    "Name": 'worker:%s' % account,
                                    "Owner": account,
                                    "User": user,
                                    "Roles": ','.join(roles),
                                    "Cluster": cluster if cluster else ""
                                })
                                if "streamer" in roles:
                                    t = Thread(target=setupStreamerDNS, args=(req.instance_id, accountObj.organization))
                                    t.start()
                                logging.info("Instance %s came online for ACCOUNT(%s)" % (req.instance_id, account))
                            request_ids = filter(lambda id: id != req.id, request_ids)

                    logging.info("%s requests left to fulfill for ACCOUNT(%s)" % (len(request_ids), account))

            time.sleep(3)
            conn.create_tags([req.id for req in reqs], {
                "Name": 'worker:%s' % account,
                "Owner": account,
                "Roles": ','.join(roles),
                "User": user,
                "Cluster": cluster if cluster else ""
            })

            t = Thread(target=tagInstancesWhenLaunched, args=(reqs,))
            t.start()

            openedSpots.extend([
                {
                    "id": req.id
                } for req in reqs
            ])

        else:

            # run instances takes min/max_count
            kwargs["min_count"] = kwargs["count"]
            kwargs["max_count"] = kwargs["count"]
            del kwargs["count"]

            reservations = conn.run_instances(ami, **kwargs)
            newInstances = []
            for instance in reservations.instances:
                conn.create_tags([instance.id], {
                    "Name": 'worker:%s' % account,
                    "Owner": account,
                    "User": user,
                    "Roles": ','.join(roles),
                    "Cluster": cluster if cluster else ""
                })
                launchedInstances.append({
                    "id": instance.id,
                    "type": instance.instance_type,
                    "info": INSTANCE_TYPES[instance.instance_type] 
                })
                if "streamer" in roles:
                    t = Thread(target=setupStreamerDNS, args=(instance.id, accountObj.organization))
                    t.start()
        
    return None, launchedInstances, openedSpots


@app.route('/redshirt/launch/cluster', methods=["POST"])
def launch_cluster():
    """
    Launch an entire cluster at once.

    PostParams:
        user: a user
        account: an account
        workers: the number of workers to launch for the cluster
        highAvailability: whether or not to run multiple spark masters
    Returns:
        A json object containing spot prices
    """

    user = request.form['user']
    account = request.form['account']

    workers = int(request.form['workers'])

    spot = 'spot' in request.form
    highAvailability = 'highAvailability' in request.form

    clusterName = request.form['clusterName']

    if len(clusterName) == 0:
        return jsonify({
            "error": "You must specify a name for your new cluster."
        }), 400

    groups = []

    # add the spark master group
    sparkMaster = {
        'instanceType': 'm1.small',
        'count': 1,
        'spotPrice': None,
        'roles': ['spark-master'],
    }

    # launch an extra master for high availability
    if highAvailability:
        groups.append(dict(sparkMaster))

    sparkMaster['roles'].append('job-server')

    groups.append(sparkMaster)
    
    workerGroups = buildGroupsForWorkers(workers, spot=spot)

    error, launchedInstances, openedSpots = launchInstances(user, account, groups + workerGroups, cluster=clusterName, launchCluster=True)

    mixingboard.alterClusterProperty(account, clusterName, "workers", workers)
    mixingboard.alterClusterProperty(account, clusterName, "groups", groups)

    if error:

        return jsonify({
            "error": error
        }), 400

    else:

        return jsonify({
            "instances": launchedInstances,
            "spots": openedSpots
        })


def buildGroupsForWorkers(numWorkers, spot=False):

    groups = []

    # get the number of instances need to fulfill the number of workers
    numMediums = numWorkers % 4
    numXLs = numWorkers/4

    if numXLs > 0:
        sparkWorkersXL = {
            'instanceType': 'm3.xlarge',
            'count': numXLs,
            'spotPrice': 0.28 if spot else None, # set spot price at on-demand price for now
            'roles': ['spark-worker'],
        }
        groups.append(sparkWorkersXL)

    if numMediums > 0:
        sparkWorkersMedium = {
            'instanceType': 'm3.medium',
            'count': numMediums,
            'spotPrice': 0.07 if spot else None, # set spot price at on-demand price for now
            'roles': ['spark-worker'],
        }
        groups.append(sparkWorkersMedium)

    return groups


@app.route('/redshirt/cluster/<cluster>')
def cluster_fetch(cluster):
    """
    Alter the number of workers currently in a cluster

    RouteParams:
        cluster: the name of a cluster
    """

    account = request.args['account']
    user = request.args['user']

    cluster = mixingboard.getCluster(account, cluster)

    return jsonify({
        "cluster": cluster
    })


def getStreamerInstances(account, user):
    """
    Get a list of all of a user's running instances.

    GetParams:
        user: a user
        account: an account
        cluster: a cluster name
    Returns:
        An array of instances
    """

    myInstances = []

    accountObj = Account.query.filter(Account.id == account).first()
    conn = getMasterEC2Conn(region=mixingboard.REGION)
    reservations = conn.get_all_reservations(filters={
        'tag:Name': 'worker:%s' % account, 
        'tag:Owner': '%s' % account, 
        'tag:Roles': 'streamer',
    })

    for res in reservations:
        for inst in res.instances:
            if inst.state in {'running','pending'}:
                myInstances.append({
                    "id": inst.id,
                    "type": inst.instance_type,
                    "info": INSTANCE_TYPES[inst.instance_type], 
                    "state": inst.state,
                    "launch_time": inst.launch_time,
                    "tags": inst.tags
                })
     
    return myInstances


@app.route('/redshirt/cluster/<cluster>/workers/<workers>', methods=["POST"])
def cluster_alter_workers(cluster, workers):
    """
    Alter the number of workers currently in a cluster

    RouteParams:
        cluster: the name of a cluster
        workers: the number of workers to add to the cluster
    """

    workers = int(workers)

    account = request.form['account']
    user = request.form['user']

    if workers < 1:
        return jsonify({
            "error": "You must have at least one worker in a cluster"
        }), 400

    error = None
    launchedInstances = [] 
    openedSpots = []

    lock = mixingboard.lockCluster(account, cluster)

    with lock: 

        mixingboard.alterClusterProperty(account, cluster, 'workers', workers, lock=lock)

        # check if the cluster is stopped
        clusterInfo = mixingboard.getCluster(account, cluster)
        if clusterInfo["stopped"]:
            return jsonify({})

        instances = [instance for instance in getClusterInstances(account, user, cluster) \
                        if instance['info'].get('workers', 0) > 0 and instance['state'] != "terminated"]
        currentWorkers = len(instances)
        workerChange = workers - currentWorkers

        conn = getMasterEC2Conn(region=mixingboard.REGION)

        if workerChange != 0:
            
            if workerChange < 0:

                xlWorkers = filter(lambda instance: instance['info'].get('workers', 0) == 4, instances)
                medWorkers = filter(lambda instance: instance['info'].get('workers', 0) == 1, instances)

                terminateIds = []
                while workerChange <= -4:
                    try:
                        terminateIds.append(xlWorkers.pop()['id'])
                        workerChange += 4
                    except IndexError:
                        break
                    
                while workerChange <= -1:
                    try:
                        terminateIds.append(medWorkers.pop()['id'])
                        workerChange += 1
                    except IndexError:
                        break

                # if we can't reach the correct number of workers, shuffle shit around so we can
                while workerChange < 0:
                    terminateIds.append(xlWorkers.pop())
                    workerChange += 4
                    
                conn.terminate_instances(terminateIds)
            
            if workerChange > 0:
                
                groups = buildGroupsForWorkers(workerChange)

                logging.info("GROUPS: %s" % groups)

                error, launchedInstances, openedSpots = launchInstances(user, account, groups, cluster=cluster)

    if error:

        return jsonify({
            "error": error
        }), 400

    else:

        return jsonify({
            "instances": launchedInstances,
            "spots": openedSpots
        })


@app.route('/redshirt/streamers')
def streamers():
    """
    Alter the number of workers currently in a cluster

    RouteParams:
    """

    account = request.form['account']
    user = request.form['user']

    error = None
    launchedInstances = [] 
    openedSpots = []

    instances = getStreamerInstances(account, user)

    return jsonify({
        "streamers": instances
    })


@app.route('/redshirt/streamers/<streamers>', methods=["POST"])
def streamers_alter_workers(streamers):
    """
    Alter the number of workers currently in a cluster

    RouteParams:
    """

    streamers = int(streamers)

    account = request.form['account']
    user = request.form['user']
    spot = request.form.get('spot') is not None

    if streamers != 1 and streamers != 0:
        return jsonify({
            "error": "You can only have 1 or 0 streamers (This will be changing soon!)"
        }), 400

    error = None
    launchedInstances = [] 
    openedSpots = []

    lock = mixingboard.lockService(account, "streamers")

    with lock: 

        instances = getStreamerInstances(account, user)
        currentStreamers = len(instances)
        streamersChange = streamers - currentStreamers

        if streamers == 0:
            accountObj = Account.query.filter(Account.id == account).first()
            deleteStreamerDNS(accountObj.organization)

        if streamersChange != 0:

            groups = None

            if streamersChange > 0:

                groups = [{
                    'instanceType': 'm3.medium',
                    'count': 1,
                    'spotPrice': 0.7 if spot else None, # set spot price at on-demand price for now
                    'roles': ['streamer'],
                }]

            else:

                conn = getMasterEC2Conn(region=mixingboard.REGION)
                conn.terminate_instances([instances[0]['id']])

            if groups is not None:

                error, launchedInstances, openedSpots = launchInstances(user, account, groups)

    if error:

        return jsonify({
            "error": error
        }), 400

    else:

        return jsonify({
            "instances": launchedInstances,
            "spots": openedSpots
        })


def stopCluster(account, user, cluster):
    """
    Stop an entire cluster, cancelling any spot
    requests and terminating all instances in the cluster.

    Params:
        account: an account id
        account: a user id
        cluster: the name of a cluster
    """

    conn = getMasterEC2Conn(region=mixingboard.REGION)

    reqs = conn.get_all_spot_instance_requests(filters={
        "tag:Name": "worker:%s" % account,
        "tag:Owner": "%s" % account,
        "tag:Cluster": "%s" % cluster
    })
    if (len(reqs) > 0):
        conn.cancel_spot_instance_requests([req.id for req in reqs])

    spotInstances = [req.instance_id for req in reqs if req.instance_id is not None]
    
    instances = conn.get_only_instances(filters={
        "tag:Name":"worker:%s" % account, 
        "tag:Owner": "%s" % account, 
        "tag:Cluster": "%s" % cluster
    })
    if len(instances) > 0:
        instances = [instance.id for instance in instances]
        if len(spotInstances) > 0:
            instances.extend(spotInstances)
            instances = list(set(instances))
        conn.terminate_instances(instances)
 
    mixingboard.alterClusterProperty(account, cluster, "stopped", True)


@app.route('/redshirt/cluster/<cluster>/start', methods=["POST"])
def cluster_start(cluster):
    """
    Stop an entire cluster, cancelling any spot
    requests and terminating all instances in the cluster.

    RouteParams:
        cluster: the name of a cluster
    Returns:
        A status code indicating success
    """

    account = request.form['account']
    user = request.form['user']

    lock = mixingboard.lockCluster(account, cluster)

    error = None
    launchedInstances = []
    openedSpots = []

    with lock: 

        clusterInfo = mixingboard.getCluster(account, cluster)
        mixingboard.alterClusterProperty(account, cluster, "stopped", False, lock=lock)

        workerGroups = buildGroupsForWorkers(clusterInfo['workers'])

        error, launchedInstances, openedSpots = launchInstances(user, account, clusterInfo["groups"] + workerGroups, cluster=cluster)
 
    if error:

        return jsonify({
            "error": error
        }), 400

    else:

        return jsonify({
            "instances": launchedInstances,
            "spots": openedSpots
        })


@app.route('/redshirt/cluster/<cluster>/shutdown', methods=["POST"])
def cluster_shutdown(cluster):
    """
    Shutdown an entire cluster, cancelling any spot
    requests and terminating all instances in the cluster.
    Also permanently deletes any record of the cluster.

    RouteParams:
        cluster: the name of a cluster
    Returns:
        A status code indicating success
    """

    account = request.form['account']
    user = request.form['user']

    stopCluster(account, user, cluster)
 
    mixingboard.deleteCluster(account, cluster)

    return jsonify({})


@app.route('/redshirt/cluster/<cluster>/stop', methods=["POST"])
def cluster_stop(cluster):
    """
    Stop an entire cluster, cancelling any spot
    requests and terminating all instances in the cluster.

    RouteParams:
        cluster: the name of a cluster
    Returns:
        A status code indicating success
    """

    account = request.form['account']
    user = request.form['user']

    stopCluster(account, user, cluster)
 
    return jsonify({})


@app.route('/redshirt/cluster/<cluster>/reboot', methods=["POST"])
def cluster_reboot(cluster):
    """
    Restart all of the instances in a cluster.

    PostParams:
        user: a user
        account: an account
        cluster: a cluster name
    Returns:
        An json message containing the status of the reboot
    """

    account = request.form['account']
    user = request.form['user']

    mixingboard.alterClusterProperty(account, cluster, "rebooting", True)
    instanceIds = [instance['id'] for instance in getClusterInstances(account, user, cluster)]
    conn = getMasterEC2Conn(region=mixingboard.REGION)
    conn.reboot_instances(instanceIds)

    return jsonify({
        "message": "Rebooted '%s' instances in cluster '%s'" % (len(instanceIds), cluster)
    })


@app.route('/redshirt/prices')
def spot_pricing():
    """
    Get spot pricing for usable instances.

    GetParams:
        user: a user
        account: an account
    Returns:
        A json object containing spot prices
    """

    account = request.args['account']
    user = request.args['user']

    myInstances = []

    conn = getMasterEC2Conn(region=mixingboard.REGION)
    prices = {}
    now_iso = datetime.datetime.now().isoformat()
    for instanceType, instanceInfo in INSTANCE_TYPES.items():
        hist = conn.get_spot_price_history(start_time=now_iso,instance_type=instanceType)
        average = sum([entry.price for entry in hist])/len(hist)
        workers = instanceInfo.get('workers',0)
        prices[instanceType] = {
            "workers": workers,
            "current": hist[0].price,
            "currentPerWorker": hist[0].price/workers if workers else None,
            "average": average,
            "averagePerWorker": average/workers if workers else None
        }
 
    return jsonify({
        "prices": prices
    }) 


@app.route('/redshirt/clusters')
def redshirt_clusters():
    """
    Get a list of all of an account's active clusters.

    GetParams:
        user: a user
        account: an account
    Returns:
        A json object containing a list of clusters
    """

    account = request.args['account']
    user = request.args['user']

    clusters = mixingboard.listClusters(account)
 
    return jsonify({
        "clusters": clusters
    }) 


def getClusterInstances(account, user, cluster):
    """
    Get a list of all of a user's running instances.

    GetParams:
        user: a user
        account: an account
        cluster: a cluster name
    Returns:
        An array of instances
    """

    myInstances = []

    accountObj = Account.query.filter(Account.id == account).first()
    conn = getMasterEC2Conn(region=mixingboard.REGION)
    reservations = conn.get_all_reservations(filters={
        'tag:Name': 'worker:%s' % account, 
        'tag:Owner': '%s' % account, 
        'tag:Cluster': '%s' % cluster
    })

    for res in reservations:
        for inst in res.instances:
            if inst.state in {'running','pending'}:
                myInstances.append({
                    "id": inst.id,
                    "type": inst.instance_type,
                    "info": INSTANCE_TYPES[inst.instance_type], 
                    "state": inst.state,
                    "launch_time": inst.launch_time,
                    "tags": inst.tags
                })
     
    return myInstances

@app.route('/redshirt/instances')
def redshirt_instances():
    """
    Get a list of all of a user's running instances.

    GetParams:
        user: a user
        account: an account
        cluster: a cluster name
    Returns:
        A json object containing a list of instances
    """

    account = request.args['account']
    user = request.args['user']
    cluster = request.args['cluster']

    return jsonify({
        "instances": getClusterInstances(account, user, cluster)
    }) 


@app.route('/redshirt/instance/<instanceId>/terminate', methods=["POST"])
def redshirt_instance_terminate(instanceId):
    """
    Terminate an instance.

    RouteParams:
        instanceId: the id of the instance to terminate
    GetParams:
        user: a user
        account: an account
    Returns:
        The id of the terminated instance
    """

    account = request.args['account']
    user = request.args['user']

    conn = getMasterEC2Conn(region=mixingboard.REGION)
    instance = conn.get_only_instances(instance_ids=[instanceId],
                                       filters={
                                           "tag:Name":"worker:%s" % account, 
                                           "tag:Owner": "%s" % account, 
                                      })[0]
    conn.terminate_instances([instance.id])
 
    return jsonify({
        "terminated": instance.id
    }) 


@app.route('/redshirt/spots')
def redshirt_spots():
    """
    Get a list of all of a user's spot requests.

    GetParams:
        user: a user
        account: an account
    Returns:
        A json object containing a list of spot requests.
    """

    account = request.args['account']
    user = request.args['user']

    myRequests = []

    conn = getMasterEC2Conn(region=mixingboard.REGION)
    reqs = conn.get_all_spot_instance_requests(filters={
        'tag:Name':'worker:%s' % account,
        "tag:Owner": "%s" % account
    })
    for req in reqs:

            if req.state == "closed":
                continue

            myRequests.append({
                "id": req.id,
                "price": req.price,
                "state": req.state,
                "type": req.launch_specification.instance_type,
                "info": INSTANCE_TYPES[req.launch_specification.instance_type],
                "instance_id": req.instance_id
            })
 
    return jsonify({
        "spots": myRequests
    }) 


@app.route('/redshirt/spot/<reqId>/cancel', methods=["POST"])
def redshirt_spot_cancel(reqId):
    """
    Cancel a spot instance request.

    RouteParams:
        reqId: spot instance request id
    GetParams:
        user: a user
        account: an account
    Returns:
        A json object containing the instance id of the terminated request.
    """

    account = request.args['account']
    user = request.args['user']

    conn = getMasterEC2Conn(region=mixingboard.REGION)
    req = conn.get_all_spot_instance_requests(request_ids=[reqId],filters={
        "tag:Name": "worker:%s" % account,
        "tag:Owner": "%s" % account
    })[0]
    conn.cancel_spot_instance_requests([req.id])
    conn.terminate_instances([req.instance_id])
 
    return jsonify({
        "terminated": req.instance_id
    }) 


if __name__ == "__main__":

    if COMMAND == 'serve':

        mixingboard.exposeService("redshirt", port=PORT)
        app.run(debug=DEBUG, port=PORT, host=HOST, threaded=True)

    elif COMMAND == 'db':

        init_db()
