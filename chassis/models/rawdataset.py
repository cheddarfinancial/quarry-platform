# Standard Library
import boto
import hmac
import re
import datetime
import json
import time
import uuid
from hashlib import sha1

# Third Party
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.orm import relationship

# Local
from ..database import Base
from ..aws import getS3Conn
from ..util import makeHandle


TIMEOUT = datetime.timedelta(hours=12)

s3_bucket = mixingboard.getConf("s3_bucket")

class RawDataset(Base):

    __tablename__ = 'rawdataset'
    __serialize_exclude__ = { 'account' }

    id = Column(Integer, primary_key=True)

    name = Column(String(40), unique=True)
    account_id = Column(Integer, ForeignKey('account.id'))

    created = Column(DateTime, default=datetime.datetime.utcnow)
    updated = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    account = relationship("Account")

    def __init__(self, name, account_id):

        self.name = name
        self.account_id = account_id

    def s3KeyPrefix(self):

        account = self.account
        return "user/%s/data/warehouse/%s" % (
            account.iam_username,
            self.name
        )

    def s3Location(self):

        account = self.account
        return "s3n://%s:%s@%s/%s" % (
            account.access_key_id,
            account.access_key_secret,
            s3_bucket,
            self.s3KeyPrefix()
        )

    def generateUploadURL(self, filename=None):

        id = filename or makeHandle()
        key = '%s/%s' % (self.s3KeyPrefix(), id)

        account = self.account
        bucketName = s3_bucket

        policy = """{
            "expiration": "%(expires)s",
            "conditions": [
                {"bucket":"%(bucket)s"},
                ["eq","$key","%(key)s"],
                {"acl":"private"},
                {"success_action_status":"200"}
            ]
        }"""
        policy = policy % {
            "expires": (datetime.datetime.utcnow()+TIMEOUT).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "bucket": bucketName,
            "key": key
        }

        encodedPolicy = policy.encode('utf-8').encode('base64').replace("\n","")

        signature = hmac.new(account.access_key_secret, encodedPolicy, sha1).digest().encode("base64").replace("\n","")

        return ("%s://%s.s3.amazonaws.com/" % (
            "https", 
            bucketName
        ), {
            "policy": encodedPolicy,
            "signature": signature,
            "key": key,
            "AWSAccessKeyId": account.access_key_id,
            "acl": "private",
            "success_action_status": "200"
        })
        
    def __repr__(self):
        return '<RawDataset %r>' % (self.id)
