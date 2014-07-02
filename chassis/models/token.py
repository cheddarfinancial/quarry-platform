# Standard Library
import datetime

# Third Party
from ..database import Base
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime
from sqlalchemy.orm import relationship
from ..util import makeHandle

# Local


class Token(Base):

    __tablename__ = 'token'

    id = Column(Integer, primary_key=True)
    token = Column(String(36))
    user_id = Column(Integer, ForeignKey('user.id'), index=True)

    user = relationship('User')

    def __init__(self, user_id):
        self.user_id = user_id
        self.token = makeHandle()

    def __repr__(self):
        return '<Token %r>' % (self.id)
