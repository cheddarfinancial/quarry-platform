# Standard Library
import boto
import re
import datetime
import json
import time
import uuid

# Third Party
import pymysql
import mixingboard
from ..database import Base
from ..aws import getIAMConn, getEC2Conn, getMasterCredentials, getS3Conn
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.orm import relationship, backref

# Local

s3_bucket = mixingboard.getConf("s3_bucket")

def USER_POLICIES():
    return {
        "s3": {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Action": ["s3:*"],
                    "Effect": "Allow",
                    "Resource": ["arn:aws:s3:::%s" % s3_bucket],
                    "Condition":{
                        "StringLike": {
                            "s3:prefix":[
                                "user_$folder$",
                                "user/${aws:username}_$folder$",
                                "user/${aws:username}/*",
                                "tmp_$folder$",
                                "tmp/${aws:username}_$folder$",
                                "tmp/${aws:username}/*",
                             ]
                        }
                    }
                },
                {
                    "Action":["s3:*"],
                    "Effect":"Allow",
                    "Resource": [
                        "arn:aws:s3:::%s/tmp_$folder$" % s3_bucket,
                        "arn:aws:s3:::%s/tmp/${aws:username}_$folder$" % s3_bucket,
                        "arn:aws:s3:::%s/tmp/${aws:username}/*" % s3_bucket,
                        "arn:aws:s3:::%s/user_$folder$" % s3_bucket,
                        "arn:aws:s3:::%s/user/${aws:username}_$folder$" % s3_bucket,
                        "arn:aws:s3:::%s/user/${aws:username}/*" % s3_bucket
                    ]
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:ListBucket"
                    ],
                    "Resource": [
                        "*"
                    ]
                }
            ]
        }
    }


hive_db_root_conf = mixingboard.getConf('hive_db_root')
webserver_group_id = mixingboard.getConf('webserver_sg')
zookeeper_group_id = mixingboard.getConf('zookeeper_sg')
default_vpc = mixingboard.getConf("default_vpc")

class Account(Base):

    __tablename__ = 'account'
    __serialize_exclude__ = {'users', 'access_key_id', 'access_key_secret', 'security_group'}

    id = Column(Integer, primary_key=True)

    organization = Column(String(30), unique=True)

    iam_username = Column(String(21))
    access_key_id = Column(String(20))
    access_key_secret = Column(String(40))
    security_group = Column(String(11))
    region = Column(String(20))
    subnet_id = Column(String(16))

    created = Column(DateTime, default=datetime.datetime.utcnow)
    updated = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    users = relationship("User", backref="account")

    def __init__(self, organization):

        self.organization = organization
        self.region = mixingboard.REGION

        key, secret = getMasterCredentials()

        # generate a unique username
        iamUsername = uuid.uuid4().bytes.encode('base64')[:21].translate(None, "/+")
        self.iam_username = iamUsername

        ##
        # create security group
        ##

        ec2Conn = getEC2Conn(key, secret, region=self.region) 
        # TODO GET VPC ID FROM CONFIGS
        securityGroup = ec2Conn.create_security_group(iamUsername, "Security group for account with iamUsername: %s" % iamUsername, vpc_id=default_vpc)
        self.security_group = securityGroup.id

        # authorize the security group to talk to itself
        securityGroup.authorize('tcp', '0', '65535', src_group=securityGroup)

        try:
            # authorize the webservers to talk to this group
            webserverGroup = ec2Conn.get_all_security_groups(group_ids=[webserver_group_id])[0]

            # authorize job server ports
            securityGroup.authorize('tcp', '1989', '1989', src_group=webserverGroup)

            # authorize 100 spark context ports 
            securityGroup.authorize('tcp', '4040', '4140', src_group=webserverGroup)

            # authorize 100 spark master/worker ports 
            securityGroup.authorize('tcp', '8080', '8180', src_group=webserverGroup)
        except: # TODO only catch exception for webserverGroup not existing
            pass

        # open for local development
        securityGroup.authorize('tcp', '8080', '8180', cidr_ip="0.0.0.0/0") # FIXME TOO OPEN, ONLY NEEDED FOR LOCAL
        securityGroup.authorize('tcp', '1989', '1989', cidr_ip="0.0.0.0/0") # FIXME TOO OPEN, ONLY NEEDED FOR LOCAL
        securityGroup.authorize('tcp', '4040', '4140', cidr_ip="0.0.0.0/0") # FIXME TOO OPEN, ONLY NEEDED FOR LOCAL

        # open up ports for streamers 
        securityGroup.authorize('tcp', '8888', '8988', cidr_ip="0.0.0.0/0")

        # authorize this group to talk to the zookeeper cluster
        zkGroup = ec2Conn.get_all_security_groups(group_ids=[zookeeper_group_id])[0]

        # open up the zk client port
        zkGroup.authorize('tcp', '2181', '2181', src_group=securityGroup)

        ##
        # create iam user and set up the policies for it
        ##

        iamConn = getIAMConn(key, secret)

        # create an iam user
        iamConn.create_user(iamUsername, '/account/')

        # generate an access key
        accessKey = None
        while True:
            result = iamConn.create_access_key(iamUsername)
            accessKey = result['create_access_key_response']['create_access_key_result']['access_key']

            # make sure our secret key is alphanumeric so we can
            # use it in an s3 url
            if re.match("^[0-9A-Za-z]+$", accessKey['secret_access_key']) is not None:
                break
            else:
                iamConn.delete_access_key(accessKey['access_key_id'],  user_name=iamUsername)

        self.access_key_id = accessKey['access_key_id']
        self.access_key_secret = accessKey['secret_access_key']

        # add standard account policies
        for policy_name, policy_dict in USER_POLICIES(self.region).items():
            policy_json = json.dumps(policy_dict)
            iamConn.put_user_policy(iamUsername, policy_name, policy_json)            

        self.createMetastore()


    def createMetastore(self):
        """
        Creates a Hive metastore for the given 
        """

        dbConn = pymysql.connect(host = hive_db_root_conf['host'],
                                 user = hive_db_root_conf['user'],
                                 passwd = hive_db_root_conf['password'])
        cursor = dbConn.cursor()

        # create a new metastore database
        cursor.execute("CREATE DATABASE IF NOT EXISTS metastore_%s;" % self.iam_username)
        
        # get all metastore tables from source metastore
        cursor.execute("SHOW TABLES FROM metastore_template;")
        tables = [tableRow[0] for tableRow in cursor.fetchall()]

        # copy tables into new database
        for table in tables:

            cursor.execute("CREATE TABLE IF NOT EXISTS metastore_%s.%s LIKE metastore_template.%s;" % (
                self.iam_username,
                table,
                table
            ))

        # create a new user
        try:
            cursor.execute("CREATE USER '%s'@'%%' IDENTIFIED BY '%s';" % (
                self.iam_username[:16],
                self.access_key_secret
            ))
        except Exception as e:
            # catch all since there is no "IF NOT EXISTS"
            # for user creation and the next sql statement will fail if
            # the user doesn't exist anyway
            pass

        # give that user permissions on their database
        cursor.execute("GRANT ALL ON metastore_%s.* TO '%s'@'%%';" % (
            self.iam_username,
            self.iam_username[:16]
        ))


    def getStorageUsage(self):
        """
        Returns the amount of data stored in S3
        for this account
        """
        
        conn = getS3Conn(self.access_key_id, self.access_key_secret, self.region)
        bucket = conn.get_bucket(s3_bucket, validate=False)
        totalBytes = 0
        for key in bucket.list(prefix='tmp/%s' % self.iam_username):
            totalBytes += key.size
        for key in bucket.list(prefix='user/%s' % self.iam_username):
            totalBytes += key.size
        return totalBytes

    def __repr__(self):
        return '<Account %r>' % (self.id)
