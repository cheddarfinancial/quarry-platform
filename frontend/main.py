#!/usr/bin/python

# Standard Library
import argparse
import datetime
import json
import logging
import os
import subprocess
from functools import wraps

# Third Party
import requests
import mixingboard
from chassis.aws import runBillingReport
from chassis.database import db_session
from chassis.models import User, Account, JobHistory, Notification, Bill, \
                  Token
from flask import Flask, redirect, jsonify, render_template, request, \
                  session, url_for
from sqlalchemy import or_

# Local
from api.main import api


# parse args
argParser = argparse.ArgumentParser(description='Run the Quarry server.')
argParser.add_argument('-d', '--debug', action='store_true', help='Turn on debug mode')
argParser.add_argument('-p', '--port', type=int, default=9000, help='Set the port')
argParser.add_argument('-H', '--host', type=str, default='127.0.0.1', help='Set the port')
argParser.add_argument('--no-sass', action='store_true', help='Disable sass compilation/watching')
args, _ = argParser.parse_known_args()

# put args in sensible all caps variables
DEBUG = args.debug
HOST = args.host
PORT = args.port
NO_SASS = args.no_sass


# set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# start the sass watcher if needed
if DEBUG and not NO_SASS:
    logger.info("Starting SASS watcher...")
    directory = os.path.dirname(os.path.abspath(__file__))
    subprocess.Popen("sass --watch -l %s/static/sass/style.sass:%s/static/css/style.css" % (directory, directory), shell=True)


# create flask app
app = Flask(__name__, static_url_path='/static', static_folder='./static')
app.secret_key = 'm8ZqboHDT6u75pP1QvK4nk6R8Z6/4SyeDUTXVdIGN9'
app.register_blueprint(api, url_prefix='/beta')

@app.context_processor
def override_url_for():
    return dict(url_for=dated_url_for)

def dated_url_for(endpoint, **values):
    if endpoint == 'static':
        filename = values.get('filename', None)
        if filename:
            file_path = os.path.join(app.root_path,
                                     endpoint, filename)
            values['q'] = int(os.stat(file_path).st_mtime)
    return url_for(endpoint, **values)

@app.teardown_appcontext
def shutdown_session(exception=None):
        db_session.remove()


# authentication stuff
def requiresLogin(fn):
    @wraps(fn)
    def authFn(*args, **kwargs):
        if 'user' in session:
            return fn(*args, **kwargs)
        else:
            return jsonify({
                "error": "This method requires an authenticated user"
            }), 400
    return authFn


@app.route('/api/histories/<jobType>')
def history(jobType):
    """
    Retrieve job history for a user/account

    GetParams:
        account: an account
        user: a user
    Returns:
        a json object conatining a list of saved queries
    """

    account = session['user']['account']['id']
    user = session['user']['id']
    offset = int(request.args.get("offset",0))
    count = int(request.args.get("count",20))

    # TODO allow filtering to individual user
    histories = []
    for history in JobHistory.query.filter(JobHistory.account_id == account, JobHistory.job_type==jobType) \
                        .order_by(JobHistory.created.desc()).limit(count).offset(offset):
        histories.append(history.dict())

    return jsonify({
        "histories": histories
    })


@app.route('/api/notifications')
def notifications():
    """
    Retrieve notifications for a user/account

    GetParams:
        account: an account
        user: a user
    Returns:
        a json object conatining a list of saved queries
    """

    account = session['user']['account']['id']
    user = session['user']['id']
    offset = int(request.args.get("offset",0))
    count = int(request.args.get("count",20))

    # TODO allow filtering to individual user
    notifications = []
    for notification in Notification.query.filter(Notification.account_id == account, or_(Notification.user_id == user,
                        Notification.user_id == None), Notification.read == None) \
                        .order_by(Notification.created.desc()).limit(count).offset(offset):
        notifications.append(notification.dict())

    return jsonify({
        "notifications": notifications
    })


@app.route('/api/notification/<notificationId>/read', methods=["POST"])
def notification_read(notificationId):
    """
    Mark a notification as read

    GetParams:
        account: an account
        user: a user
    Returns:
        a json object conatining a list of saved queries
    """

    account = session['user']['account']['id']
    user = session['user']['id']

    notification = Notification.query.filter(Notification.account_id == account, or_(Notification.user_id == user,
                        Notification.user_id == None), Notification.id == notificationId).first()
    notification.markRead()
    db_session.add(notification)
    db_session.commit()

    return jsonify({
        "notification": notification.dict()
    })


@app.route('/api/jaunt/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE'])
@requiresLogin
def jaunt(path):
    """
    Forward a request to the jaunt service. No params or
    returns in this doc string as it is for the most part a
    passthrough method.
    """

    args = dict(request.args.items())
    form = dict(request.form.items())
    method = request.method

    url = "".join([JAUNT_URL_FORMAT, path])

    account = session['user']['account']['id']
    user = session['user']['id']
    args['account'] = account
    args['user'] = user 
    form['account'] = account
    form['user'] = user
    
    accountObj = Account.query.filter(Account.id == account).first()
    awsKey = accountObj.access_key_id
    awsSecret = accountObj.access_key_secret

    s3Bucket = mixingboard.getConf("s3_bucket")

    args['awsKey'] = awsKey
    form['awsKey'] = awsKey
    args['awsSecret'] = awsSecret
    form['awsSecret'] = awsSecret
    args['s3Bucket'] = s3Bucket
    form['s3Bucket'] = s3Bucket
    args['warehouseDir'] = "/user/%s/shark/warehouse" % accountObj.iam_username
    form['warehouseDir'] = "/user/%s/shark/warehouse" % accountObj.iam_username

    res = getattr(requests, method.lower())(url, params=args, data=form)

    return res.text, res.status_code


@app.route('/api/lego/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE'])
@requiresLogin
def lego(path):
    """
    Forward a request to the lego service. No params or
    returns in this doc string as it is for the most part a
    passthrough method.
    """

    args = dict(request.args.items())
    form = dict(request.form.items())
    method = request.method

    url = "".join([LEGO_URL_FORMAT, path])

    account = session['user']['account']['id']
    user = session['user']['id']
    args['account'] = account
    args['user'] = user 
    form['account'] = account
    form['user'] = user
    
    res = getattr(requests, method.lower())(url, params=args, data=form)

    return res.text, res.status_code


@app.route('/api/shark/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE'])
@requiresLogin
def shark(path):
    """
    Forward a request to the shark service. No params or
    returns in this doc string as it is for the most part a
    passthrough method.
    """

    args = dict(request.args.items())
    form = dict(request.form.items())
    method = request.method

    url = "".join([SHARK_URL_FORMAT, path])

    account = session['user']['account']['id']
    user = session['user']['id']
    args['account'] = account
    args['user'] = user 
    form['account'] = account
    form['user'] = user
    
    res = getattr(requests, method.lower())(url, params=args, data=form)

    return res.text, res.status_code


@app.route('/api/redshirt/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE'])
@requiresLogin
def redshirt(path):
    """
    Forward a request to the redshirt service. No params or
    returns in this doc string as it is for the most part a
    passthrough method.
    """

    args = dict(request.args.items())
    form = dict(request.form.items())
    method = request.method

    url = "".join([REDSHIRT_URL_FORMAT, path])

    account = session['user']['account']['id']
    user = session['user']['id']
    args['account'] = account
    args['user'] = user
    form['account'] = account
    form['user'] = user

    res = getattr(requests, method.lower())(url, params=args, data=form)

    return res.text, res.status_code


@app.route('/api/flint/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE'])
@requiresLogin
def flint(path):
    """
    Forward a request to the redshirt service. No params or
    returns in this doc string as it is for the most part a
    passthrough method.
    """

    args = dict(request.args.items())
    form = dict(request.form.items())
    method = request.method

    url = "".join([FLINT_URL_FORMAT, path])

    account = session['user']['account']['id']
    user = session['user']['id']
    args['account'] = account
    args['user'] = user
    form['account'] = account
    form['user'] = user

    res = getattr(requests, method.lower())(url, params=args, data=form)

    return res.text, res.status_code


@app.route('/api/account')
@requiresLogin
def account():

    account = Account.query.filter(Account.id == session['user']['account_id']).first()

    return jsonify({
        "account": account.dict()
    })


@app.route('/api/account/secret')
@requiresLogin
def account_secret():

    account = Account.query.filter(Account.id == session['user']['account_id']).first()

    return jsonify({
        "secret": account.iam_username
    })


@app.route('/api/account/storage')
@requiresLogin
def account_storage():

    account = Account.query.filter(Account.id == session['user']['account_id']).first()

    return jsonify({
        "storageUsed": account.getStorageUsage()
    })


@app.route('/api/account/users')
@requiresLogin
def account_users():

    account = Account.query.filter(Account.id == session['user']['account_id']).first()

    users = [user.dict() for user in account.users]

    return jsonify({
        "users": users
    })


@app.route('/api/user/me')
@requiresLogin
def user_me():

    user = User.query.filter(User.id == session['user']['id']).first()

    session['user'] = user.dict()
    session['user']['account'] = user.account.dict()

    return jsonify({
        "user": session['user']
    })


INVITE_CODE = '3fYsq96iSquvmRsMTzkdg'

@app.route('/api/signup', methods=['POST'])
def signup():

    name = request.form['name']
    email = request.form['email']
    password = request.form['password']
    inviteCode = request.form['inviteCode']
    organization = request.form['organization']

    if inviteCode != INVITE_CODE:
        return jsonify({
            "error": "Invalid invite code. If you were given an invite code, email hello@quarry.io for help."
        }), 400

    # FIXME allow users to be added to existing accounts
    account = Account(organization)
    db_session.add(account)
    db_session.commit()

    try:
        user = User(name=name, email=email, password=password, accountId=account.id)
    except Exception as e:
        return jsonify({
            "error": e.message
        }), 400

    db_session.add(user)
    db_session.commit()

    session['user'] = user.dict()
    session['user']['account'] = user.account.dict()

    return jsonify({
        "user": user.dict()
    })


@app.route('/api/login', methods=['POST'])
def login():

    email = request.form['email']
    password = request.form['password']

    user = User.query.filter(User.email == email).first()
    if user is None:
        return jsonify({
            "error": "No user exists with that email"
        }), 400

    if user.checkPassword(password):

        session['user'] = user.dict()
        session['user']['account'] = user.account.dict()

        return jsonify({
            "success": True,
            "message": "Successfully logged in",
            "user": session['user']
        })

    else:

        return jsonify({
            "error": "Your password is incorrect"
        }), 400


@app.route('/api/account/update', methods=['POST'])
def update_account():
    """
    Update an account
    """
    
    account = Account.query.filter(Account.id == session['user']['account']['id']).first()

    for key, value in request.form.items():
        setattr(account, key, value)

    db_session.add(account)
    db_session.commit()

    session['user']['account'] = account.dict()

    return jsonify({
        "account": session['user']['account']
    })


@app.route('/api/user/me/update', methods=['POST'])
def update_user():
    """
    Update a user
    """
    
    user = User.query.filter(User.id == session['user']['id']).first()

    for key, value in request.form.items():
        setattr(user, key, value)

    db_session.add(user)
    db_session.commit()

    session['user'] = user.dict()
    session['user']['account'] = user.account.dict()

    return jsonify({
        "user": session['user']
    })


@app.route('/api/logout')
def logout():

    del session['user']

    return redirect("/")


@app.route('/<path:path>')
def reroute(path):

    return redirect("/#/%s" % path)


@app.route('/')
def index():

    user = session.get('user', 'null')
    if user != 'null':

        user = User.query.filter(User.id == session['user']['id']).first()

        if user:
            session['user'] = user.dict()
            session['user']['account'] = user.account.dict()
            session['user']['account']['users'] = [user.dict() for user in user.account.users]
        
    return render_template('base.html', user=json.dumps(session.get('user', None)))


# setup shark configurations
global SHARK_URL_FORMAT
SHARK_URL_FORMAT = None
def setSharkURLFormat(sharkServers):
    global SHARK_URL_FORMAT
    shark = sharkServers[0]
    SHARK_URL_FORMAT = "http://%s:%s/shark/" % (shark["host"], shark["port"])
    logging.info("GOT SHARK SERVICE: %s" % SHARK_URL_FORMAT)

mixingboard.discoverService("shark",setSharkURLFormat) 


# setup lego configurations
global LEGO_URL_FORMAT
LEGO_URL_FORMAT = None
def setLegoURLFormat(legoServers):
    global LEGO_URL_FORMAT
    lego = legoServers[0]
    LEGO_URL_FORMAT = "http://%s:%s/lego/" % (lego["host"], lego["port"])
    logging.info("GOT LEGO SERVICE: %s" % LEGO_URL_FORMAT)

mixingboard.discoverService("lego",setLegoURLFormat) 


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


if __name__ == "__main__":

    mixingboard.exposeService("frontend", port=PORT)
    app.run(debug=DEBUG, port=PORT, host=HOST, threaded=True)
