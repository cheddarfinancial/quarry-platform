# Standard Library
import datetime

# Third Party
from chassis.aws import getS3Conn
from ..database import Base, JsonType
from memoized_property import memoized_property
from sqlalchemy import Column, Integer, String, DateTime, Text, ForeignKey, Boolean
from sqlalchemy.orm import relationship

# Local


class Notification(Base):

    __tablename__ = 'notification'
    __serialize_exclude__ = { 'account', 'user' }

    id = Column(Integer, primary_key=True)
    account_id = Column(Integer, ForeignKey('account.id'), index=True)
    user_id = Column(Integer, ForeignKey('user.id'), index=True)
    message = Column(Text)
    read = Column(DateTime)
    created = Column(DateTime, default=datetime.datetime.utcnow, index=True)

    account = relationship("Account")
    user = relationship("User")

    def __init__(self, account_id, user_id, message):
        self.read = False
        self.account_id = account_id
        self.user_id = user_id
        self.message = message
        
    def markRead(self):
        self.read = datetime.datetime.utcnow()

    def __repr__(self):
        return '<Notification %r>' % (self.id)
