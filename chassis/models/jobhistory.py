# Standard Library
import datetime

# Third Party
from ..database import Base, JsonType
from sqlalchemy import Column, Integer, String, DateTime, Text, Enum

# Local


class JobHistory(Base):

    __tablename__ = 'jobhistory'

    id = Column(Integer, primary_key=True)
    account_id = Column(Integer, index=True)
    user_id = Column(Integer, index=True)
    title = Column(String(200))
    event = Column(String(20))
    job_type = Column(String(20), default="spark", index=True)
    job_id = Column(Integer, index=True)
    job_handle = Column(String(40), index=True)
    data = Column(JsonType())
    created = Column(DateTime, default=datetime.datetime.utcnow)
    updated = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    def __init__(self, account_id, user_id, event, jobId=None, jobHandle=None, jobType="spark", data={}):

        self.account_id = account_id
        self.user_id = user_id
        self.event = event
        self.job_id = jobId
        self.job_handle = jobHandle
        self.data = data
        self.job_type = jobType

    def __repr__(self):
        return '<JobHistory %r>' % (self.event)
