# Standard Library
import datetime
import re

# Third Party
from ..database import Base
from pbkdf2 import crypt
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey

# Local
from ..email import sendEmail
from ..sms import sendMessage


class User(Base):

    __tablename__ = 'user'
    __serialize_exclude__ = {'password_hash', 'account'}

    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    email = Column(String(120), unique=True)
    password_hash = Column(String(48))

    account_id = Column(Integer, ForeignKey('account.id'))
    role = Column(String(10), default='admin')
    
    phone_number = Column(String(20))

    created = Column(DateTime, default=datetime.datetime.utcnow)
    updated = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    def __init__(self, name=None, email=None, password=None, accountId=None):
        
        error = ""
        if not re.match("^.*@.*\..*$", email):
            error += "Your email does not appear to be valid. "
        if not re.match("^[A-Za-z0-9\. ]*$", name):
            error += "Your name can only contain letters, numbers, periods and spaces. "
        if len(password) < 8:
            error += "Your password must be at least 8 characters long. "
        if not accountId:
            error += "A user must be part of an account"

        if len(error) > 0:
            raise Exception(error)

        self.account_id = accountId
        self.name = name
        self.email = email
        self.password_hash = crypt(password)

    def __repr__(self):
        return '<User %r>' % (self.name)

    def checkPassword(self, password):
        return crypt(password, self.password_hash)

    def sendMessage(self, message):
        return sendMessage(self.phone_number, message)

    def sendEmail(self, subject, body, leadImage=None, title=None, actionLink=None, actionLinkTitle=None, tags=[]):
        sendEmail(self.email, self.name, subject, body, leadImage, title, actionLink, actionLinkTitle, tags)

    def sendWelcomeEmail(self):
        body = """
            <p>Thanks for signing up QuarryIO</p>
        """
        self.sendEmail("Welcome to QuarryIO!", body)
