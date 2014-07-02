# Standard Library
import csv
import datetime
from cStringIO import StringIO
from collections import defaultdict

# Third Party
import boto.ec2
import boto.s3
import boto.route53
import boto.iam

# Local
import mixingboard
from database import db_session


def getMasterCredentials():

    aws_key = mixingboard.getConf("aws_key")
    aws_secret = mixingboard.getConf("aws_secret")

    return aws_key, aws_secret

def getEC2Conn(aws_key, aws_secret, region='us-west-2'):

    return boto.ec2.connect_to_region(region, aws_access_key_id=aws_key, aws_secret_access_key=aws_secret)

def getRoute53Conn(aws_key, aws_secret, region='us-west-2'):

    return boto.route53.connect_to_region(region, aws_access_key_id=aws_key, aws_secret_access_key=aws_secret)

def getS3Conn(aws_key, aws_secret, region='us-west-2'):

    return boto.s3.connect_to_region(region, aws_access_key_id=aws_key, aws_secret_access_key=aws_secret)

def getMasterEC2Conn(region='us-west-2'):

    key, secret = getMasterCredentials()
    return boto.ec2.connect_to_region(region, aws_access_key_id=key, aws_secret_access_key=secret)

def getMasterRoute53Conn(region='us-west-2'):

    key, secret = getMasterCredentials()
    return boto.route53.connect_to_region(region, aws_access_key_id=key, aws_secret_access_key=secret)

def getMasterS3Conn(region='us-west-2'):

    key, secret = getMasterCredentials()
    return boto.s3.connect_to_region(region, aws_access_key_id=key, aws_secret_access_key=secret)

def getIAMConn(aws_key, aws_secret, region='us-west-2'):

    return boto.iam.connect_to_region(region, aws_access_key_id=aws_key, aws_secret_access_key=aws_secret)
