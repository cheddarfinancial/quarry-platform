# Standard Library
import datetime
import re

# Third Party
from ..database import Base, JsonType
from sqlalchemy import Column, Integer, String, DateTime, Text, ForeignKey
from sqlalchemy.orm import relationship

# Local


class Query(Base):

    __tablename__ = 'query'

    id = Column(Integer, primary_key=True)
    account_id = Column(Integer, index=True)
    title = Column(String(50))
    user_id = Column(Integer, ForeignKey('user.id'), index=True)
    description = Column(Text)
    sql = Column(Text)
    python = Column(Text)
    options = Column(JsonType(), default={})
    created = Column(DateTime, default=datetime.datetime.utcnow)
    updated = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    user = relationship("User")

    def __init__(self, title=None, account_id=None, user_id=None, description=None, sql=None, options={}):
        
        error = ""
        if not account_id:
            error += "No account was supplied. "
        if not title:
            error += "You must name your SQL query. "
        if not sql:
            error += "You must provide a SQL query. "
        if not user_id:
            error += "You must supply the user who is saving the query. "

        if len(error) > 0:
            raise Exception(error)

        if not description:
            description = "No description provided"

        self.account_id = account_id
        self.title = title
        self.user_id = user_id
        self.description = description
        self.sql = sql
        self.options = options

    def __repr__(self):
        return '<Query %r>' % (self.title)
