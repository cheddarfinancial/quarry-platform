# Standard Library
import datetime

# Third Party
from chassis.aws import getS3Conn
from ..database import Base, JsonType
from memoized_property import memoized_property
from sqlalchemy import Column, Integer, String, DateTime, Text, ForeignKey
from sqlalchemy.orm import relationship

# Local
import mixingboard

s3_bucket = mixingboard.getConf("s3_bucket")

class Job(Base):

    __tablename__ = 'job'
    __serialize_exclude__ = {'account'}

    id = Column(Integer, primary_key=True)
    account_id = Column(Integer, ForeignKey('account.id'), index=True)
    title = Column(String(50))
    user_id = Column(Integer, ForeignKey('user.id'), index=True)
    description = Column(Text)
    main_file = Column(String(200))
    extra_files = Column(JsonType(), default=[])
    options = Column(JsonType(), default={})
    created = Column(DateTime, default=datetime.datetime.utcnow)
    updated = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    account = relationship("Account")
    account = relationship("User")

    def __init__(self, title=None, account_id=None, user_id=None, description=None, code=None):
        
        error = ""
        if not account_id:
            error += "No account was supplied. "
        if not title:
            error += "You must name your job. "
        if not user_id:
            error += "You must supply the user who is saving the job. "

        if len(error) > 0:
            raise Exception(error)

        if not description:
            description = "No description provided"

        self.account_id = account_id
        self.title = title
        self.user_id = user_id
        self.description = description

    def getExtraFiles(self):

        for filename in self.extra_files:
            
            key = self._bucket.get_key(filename, validate=False)
            yield filename[filename.rfind("/")+1:], key.get_contents_as_string()


    def _getRootS3Path(self):
        return "user/%s/sparkjobs/%s" % (
            self.account.iam_username,
            self.id
        )

    def _uploadFileToS3(self, filename, code, main=False):

        if main:
            filename = "%s/main.py" % self._getRootS3Path()
            self.main_file = filename
        else:
            self.extra_files.append(filename)
            filename = "%s/files/%s" % (
                self._getRootS3Path(),
                filename
            )

        key = self._bucket.new_key(filename)
        key.set_contents_from_string(code)

    def _deleteFileFromS3(self, filename):

        self.extra_files.remove(filename)
        filename = "%s/files/%s" % (
            self._getRootS3Path(),
            filename
        )

        key = self._bucket.get_key(filename, validate=False)
        key.delete()

    def _getS3FileContents(self, filename, main=False):

        if main:
            filename = "%s/main.py" % self._getRootS3Path()
            self.main_file = filename
        else:
            self.extra_files.append(filename)
            filename = "%s/files/%s" % (
                self._getRootS3Path(),
                filename
            )

        key = self._bucket.get_key(filename, validate=False)
        return key.get_contents_as_string()

    @memoized_property
    def _s3Conn(self):
        return getS3Conn(self.account.access_key_id, self.account.access_key_secret, region=self.account.region)

    @memoized_property
    def _bucket(self):
        return self._s3Conn.get_bucket(s3_bucket, validate=False)
 
    def setMainFile(self, code):
        self._uploadFileToS3("main.py", code, main=True)

    def addExtraFile(self, filename, code):
        self._uploadFileToS3(filename, code, main=False)

    def removeExtraFile(self, filename):
        self._deleteFileFromS3(filename)

    def getMainFileContents(self):
        return self._getS3FileContents("main.py", main=True)

    def getExtraFileContents(self, filename):
        return self._getS3FileContents(filename, main=False)

    def __repr__(self):
        return '<Job %r>' % (self.title)
