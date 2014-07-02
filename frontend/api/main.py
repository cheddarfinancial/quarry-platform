# Standard Library
import logging
from functools import wraps

# Third Party
import mixingboard
import requests
from chassis.models import Bill, Token, User 
from flask import Blueprint, request, jsonify
from jinja2 import TemplateNotFound

# Local


api = Blueprint('api', __name__)


def requiresToken(fn):
    @wraps(fn)
    def authFn(*args, **kwargs):
        if request.authorization:
            token = request.authorization.username
        else:
            token = request.args.get('token') or request.form.get('token')

        if not token:
            return jsonify({
                "error": "No token was supplied for this request"
            }), 400

        token = Token.query.filter(Token.token == token).first()

        if not token:
            return jsonify({
                "error": "That is not a valid token id"
            }), 400

        user = token.user
        return fn(user, *args, **kwargs)

    return authFn


@api.route('/auth', methods=["POST"])
def auth():

    email = request.form['email']
    password = request.form['password']

    user = User.query.filter(User.email == email).first()
    if user is None:
        return jsonify({
            "error": "No user exists with that email"
        }), 400

    if user.checkPassword(password):

        token = Token.query.filter(Token.user_id == user.id).first()
        if not token:
            token = Token(user.id)
            db_session.add(token)
            db_session.commit()

        return jsonify({
            "token": token.token
        })

    else:

        return jsonify({
            "error": "Your password is incorrect"
        }), 400


@api.route('/current_bill')
@requiresToken
def current_bill(user):

    bill = Bill.query.filter(Bill.account_id == user.account_id) \
                .order_by(Bill.period.desc()).first()

    return jsonify({
        "bill": bill.dict() if bill else None
    })


@api.route('/user')
@requiresToken
def get_user(user):

    userDict = user.dict()
    userDict['account'] = user.account.dict()

    return jsonify({
        'user': userDict
    })


@api.route('/account')
@requiresToken
def get_account(user):

    accountDict = user.account.dict()

    return jsonify({
        'account': accountDict
    })


@api.route('/account/users')
@requiresToken
def get_account_users(user):

    usersDict = [user.dict() for user in user.account.users]

    return jsonify({
        'users': usersDict
    })


@api.route('/raw_datasets')
@requiresToken
def get_raw_datasets(user):
    return forwardRequest(user, 'flint', '/rawdatasets')


@api.route('/raw_dataset/<datasetName>')
@requiresToken
def get_raw_dataset(user, datasetName):
    return forwardRequest(user, 'flint', '/rawdataset/%s' % datasetName)


@api.route('/raw_dataset/<datasetName>/upload_url')
@requiresToken
def get_raw_dataset_upload_url(user, datasetName):
    return forwardRequest(user, 'flint', '/rawdataset/%s/uploadurl' % datasetName)
    

##
# Service Discovery
##

# setup shark configurations
global SHARK_URL_FORMAT
SHARK_URL_FORMAT = None
def setSharkURLFormat(sharkServers):
    global SHARK_URL_FORMAT
    shark = sharkServers[0]
    SHARK_URL_FORMAT = "http://%s:%s/shark" % (shark["host"], shark["port"])
    logging.info("GOT SHARK SERVICE: %s" % SHARK_URL_FORMAT)

mixingboard.discoverService("shark",setSharkURLFormat)


# setup lego configurations
global LEGO_URL_FORMAT
LEGO_URL_FORMAT = None
def setLegoURLFormat(legoServers):
    global LEGO_URL_FORMAT
    lego = legoServers[0]
    LEGO_URL_FORMAT = "http://%s:%s/lego" % (lego["host"], lego["port"])
    logging.info("GOT LEGO SERVICE: %s" % LEGO_URL_FORMAT)

mixingboard.discoverService("lego",setLegoURLFormat)


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


##
# Service Discovery
##


def forwardRequest(user, service, path):
    
    url = None
    if service == "flint":
        url = FLINT_URL_FORMAT
    url += path

    args = dict(request.args.items())
    form = dict(request.form.items())
    method = request.method

    account_id = user.account_id
    user_id = user.id
    args['account'] = account_id
    args['user'] = user_id
    form['account'] = account_id
    form['user'] = user_id

    res = getattr(requests, method.lower())(url, params=args, data=form)

    return res.text, res.status_code
