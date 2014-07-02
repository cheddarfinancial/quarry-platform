# Standard Library
import datetime

# Third Party
from ..database import Base, JsonType
from sqlalchemy import Column, Integer, String, DateTime, Text

# Local


class DataJob(Base):

    __tablename__ = 'datajob'
    __serialize_exclude__ = {'options'}

    id = Column(Integer, primary_key=True)
    account_id = Column(Integer, index=True)
    title = Column(String(50))
    user = Column(Integer, index=True)
    description = Column(Text)
    database = Column(String(30))
    action = Column(String(10))
    options = Column(JsonType())
    created = Column(DateTime, default=datetime.datetime.utcnow)
    updated = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    def __init__(self, title=None, account_id=None, user=None, description=None, 
                 database=None, action=None, options=None):
        
        error = ""
        if not account_id:
            error += "No account was supplied. "
        if not title:
            error += "You must name your job. "
        if not user:
            error += "You must supply the user who is saving the job. "
        if not database:
            error += "You must choose a target database. "
        if action not in ["import", "export"]:
            error += "You must either choose an import or export action for your job. "
        if options is None:
            error += "You must choose supply a valid set of options. "

        if len(error) > 0:
            raise Exception(error)

        if not description:
            description = "No description provided"

        self.account_id = account_id
        self.title = title
        self.user = user
        self.description = description
        self.database = database
        self.action = action
        self.options = options

    def __repr__(self):
        return '<DataJob %r>' % (self.title)
