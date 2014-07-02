#!/usr/bin/python

# Standard Library
import argparse
import json
import logging
import os
import subprocess

# Third Party
import requests
import mixingboard
from chassis.database import db_session
from chassis.models import Workflow
from flask import Flask, jsonify, request

# Local
from lib.runner import runWorkflow, getHandleInfo, makeHistory, \
    getRunningWorkflows, cancelHandle

# parse args
argParser = argparse.ArgumentParser(description='Run the Quarry server.')
argParser.add_argument('-d', '--debug', action='store_true', help='Turn on debug mode')
argParser.add_argument('-p', '--port', type=int, default=3591, help='Set the port')
argParser.add_argument('-H', '--host', type=str, default='127.0.0.1', help='Set the port')
args, _ = argParser.parse_known_args()

# put args in sensible all caps variables
DEBUG = args.debug
HOST = args.host
PORT = args.port


# set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# setup shark configurations
global SHARK_URL_FORMAT
SHARK_URL_FORMAT = None
def setSharkURLFormat(sharkServers):
    global SHARK_URL_FORMAT
    shark = sharkServers[0]
    SHARK_URL_FORMAT = "http://%s:%s/shark/" % (shark["host"], shark["port"])
    logging.info("GOT SHARK SERVICE: %s" % SHARK_URL_FORMAT)

mixingboard.discoverService("shark",setSharkURLFormat) 


# setup jaunt configurations
global JAUNT_URL_FORMAT
JAUNT_URL_FORMAT = None
def setJauntURLFormat(jauntServers):
    global JAUNT_URL_FORMAT
    jauntServer = jauntServers[0]
    JAUNT_URL_FORMAT = "http://%s:%s/jaunt/" % (jauntServer["host"], jauntServer["port"])
    logging.info("GOT JAUNT SERVICE: %s" % JAUNT_URL_FORMAT)

mixingboard.discoverService("jaunt",setJauntURLFormat) 


# setup redshirt configurations
global REDSHIRT_URL_FORMAT
REDSHIRT_URL_FORMAT = None
def setSharkURLFormat(redShirtServers):
    global REDSHIRT_URL_FORMAT
    redShirt = redShirtServers[0]
    REDSHIRT_URL_FORMAT = "http://%s:%s/redshirt/" % (redShirt["host"], redShirt["port"])
    logging.info("GOT REDSHIRT SERVICE: %s" % REDSHIRT_URL_FORMAT)

mixingboard.discoverService("redshirt",setSharkURLFormat) 


# setup flint configurations
global FLINT_URL_FORMAT
FLINT_URL_FORMAT = None
def setFlintURLFormat(flintServers):
    global FLINT_URL_FORMAT
    flint = flintServers[0]
    FLINT_URL_FORMAT = "http://%s:%s/flint/" % (flint["host"], flint["port"])
    logging.info("GOT FLINT SERVICE: %s" % FLINT_URL_FORMAT)

mixingboard.discoverService("flint",setFlintURLFormat) 


# create flask app
app = Flask(__name__, static_url_path='/static', static_folder='./static')

@app.teardown_appcontext
def shutdown_session(exception=None):
        db_session.remove()


@app.route('/lego/workflow/new', methods=['POST'])
def create_workflowxs():
    """
    Create a workflow
    """

    user = request.form['user']
    account = request.form['account']

    title = request.form.get('title')
    description = request.form.get('description', "No description provided")
    steps = json.loads(request.form.get('steps', '{}'))
    cluster = json.loads(request.form.get('cluster', '{}'))
    notify_users = json.loads(request.form.get('notify_users', '[]'))

    try:
        workflow = Workflow(title=title, account_id=account, user_id=user, description=description, 
                            steps=steps, cluster=cluster, notify_users=notify_users)
    except ValueError as e:
        return jsonify({
            "errors": e.message
        }), 400

    db_session.add(workflow)
    db_session.commit()

    makeHistory(account, user, 'create_workflow', jobId=workflow.id)

    return jsonify({
        "workflow": workflow.dict()
    }), 201 


@app.route('/lego/workflows')
def get_workflows():
    """
    Retrieve a workflow
    """

    user = request.args['user']
    account = request.args['account']

    offset = request.args.get("offset", 0)
    count = request.args.get("count", 20)

    workflows = [] 
    for workflow in Workflow.query.filter(Workflow.account_id == account, Workflow.user_id == user) \
                        .order_by(Workflow.created.desc()).limit(count).offset(offset):

        workflows.append(workflow.dict())

    return jsonify({
        "workflows": workflows
    }) 


@app.route('/lego/workflow/<workflowId>')
def get_workflow(workflowId):
    """
    Retrieve a workflow
    """

    user = request.args['user']
    account = request.args['account']

    workflow = Workflow.query.filter(Workflow.account_id == account, Workflow.user_id == user,
                                     Workflow.id == workflowId).first()

    if workflow is None:

        return jsonify({
            "error": "No workflow exists with that id"
        }), 400

    else:

        workflowDict = workflow.dict()
        workflowDict['steps'] = workflow.getStepsWithRelations()

        return jsonify({
            "workflow": workflowDict
        }) 


@app.route('/lego/workflow/<workflowId>/edit', methods=['POST'])
def edit_workflow(workflowId):
    """
    Edit a workflow
    """

    user = request.args['user']
    account = request.args['account']

    workflow = Workflow.query.filter(Workflow.account_id == account, Workflow.user_id == user,
                                     Workflow.id == workflowId).first()

    if workflow is None:

        return jsonify({
            "error": "No workflow exists with that id"
        }), 400

    else:

        title = request.form.get('title')
        description = request.form.get('description')
        steps = request.form.get('steps')
        cluster = request.form.get('cluster')
        notify_users = request.form.get('notify_users')
        schedule_minute = request.form.get('minute', -1)
        schedule_hour = request.form.get('hour', -1)
        schedule_day_of_month = request.form.get('day_of_month', -1)
        schedule_month = request.form.get('month', -1)
        schedule_day_of_week = request.form.get('day_of_week', -1)

        errors = {}
        if title is not None and len(title) > 0:
            workflow.title = title
        if description is not None:
            workflow.description = description
        if steps is not None:
            try:
                workflow.steps = json.loads(steps)
            except ValueError as e:
                errors['steps'] = e.message
        if cluster is not None:
            try:
                workflow.cluster = json.loads(cluster)
            except ValueError as e:
                errors['cluster'] = e.message
        if notify_users is not None:
            try:
                workflow.notify_users = json.loads(notify_users)
            except ValueError as e:
                errors['top'] = e.message
        if schedule_minute != -1:
            workflow.schedule_minute = schedule_minute or None
        if schedule_hour != -1:
            workflow.schedule_hour = schedule_hour or None
        if schedule_day_of_month != -1:
            workflow.schedule_day_of_month = schedule_day_of_month or None
        if schedule_month != -1:
            workflow.schedule_month = schedule_month or None
        if schedule_day_of_week != -1:
            workflow.schedule_day_of_week = schedule_day_of_week or None

        if len(errors) > 0:
            return jsonify({
                "errors": errors
            }), 400

        db_session.add(workflow)
        db_session.commit()

        makeHistory(account, user, 'update_workflow', jobId=workflow.id)

        return jsonify({
            "workflow": workflow.dict()
        }), 201


@app.route('/lego/workflow/<workflowId>/notify/<userId>/<action>', methods=['POST'])
def edit_notify_workflow(workflowId, userId, action):
    """
    Edit who is notified by a workflow
    """

    user = request.args['user']
    account = request.args['account']

    workflow = Workflow.query.filter(Workflow.account_id == account, Workflow.user_id == user,
                                     Workflow.id == workflowId).first()

    if workflow is None:

        return jsonify({
            "error": "No workflow exists with that id"
        }), 400

    else:

        if action == "add":

            if userId == "all":

                workflow.notifyAll()

            else:

                userId = int(userId)
                workflow.addNotifyUser(userId)
                
        elif action == "remove":

            if userId == "all":

                workflow.notifyNone()

            else:

                userId = int(userId)
                workflow.removeNotifyUser(userId)
                
        else: 

            return jsonify({
                "error": "Unknown action"
            }), 404

        db_session.add(workflow)
        db_session.commit()

        return jsonify({
            "workflow": workflow.dict()
        }), 201


@app.route('/lego/workflow/<workflowId>/delete', methods=['POST'])
def delete_workflow(workflowId):
    """
    Delete a workflow
    """

    user = request.args['user']
    account = request.args['account']

    workflow = Workflow.query.filter(Workflow.account_id == account, Workflow.user_id == user,
                                     Workflow.id == workflowId).first()

    if workflow is None:

        return jsonify({
            "error": "No workflow exists with that id"
        }), 400

    else:

        db_session.delete(workflow)
        db_session.commit()

        makeHistory(account, user, 'delete_workflow', jobId=workflow.id)

        return jsonify({
            "message": "Successfully deleted workflow"
        }), 201


@app.route('/lego/workflow/cancel', methods=["POST"])
def cancel_workflow():
    """
    Cancel a workflow
    """

    user = request.form['user']
    account = request.form['account']
    handle = request.form['handle']

    return jsonify({
        "info": cancelHandle(handle)
    })

@app.route('/lego/workflow/handle_info')
def workflow_info():
    """
    Get progress of a running workflow
    """

    user = request.args['user']
    account = request.args['account']
    handle = request.args['handle']

    return jsonify({
        "info": getHandleInfo(handle)
    })

@app.route('/lego/workflows/running')
def workflows_info():
    """
    Get progress of all running workflows
    """

    user = request.args['user']
    account = request.args['account']

    return jsonify({
        "workflows": getRunningWorkflows(account, user)
    })

@app.route('/lego/workflow/<workflowId>/run', methods=["POST"])
def run_workflow(workflowId):
    """
    Run a workflow
    """

    user = request.args['user']
    account = request.args['account']

    workflow = Workflow.query.filter(Workflow.account_id == account, Workflow.user_id == user,
                                     Workflow.id == workflowId).first()

    if workflow is None:

        return jsonify({
            "error": "No workflow exists with that id"
        }), 400

    else:

        handle = runWorkflow(workflow)

        return jsonify({
            "handle": handle
        }), 202


if __name__ == "__main__":

    mixingboard.exposeService("lego", port=PORT)
    app.run(debug=DEBUG, port=PORT, host=HOST, threaded=True)
