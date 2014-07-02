# Standard Library
import logging

# Third Party
from boto.exception import S3ResponseError
from chassis.aws import getS3Conn
from chassis.util import makeHandle
from memoized_property import memoized_property

# Local 

class Cursor:

    CHUNK_SIZE = 1024*1024 # Download 1 MB at a time
    DOWNLOAD_EXPIRES = 60*60*24*31

    def __init__(self, iamUsername, accessKeyId, accessKeySecret, region, queryHandle):
        
        self.iamUsername = iamUsername
        self.accessKeyId = accessKeyId
        self.accessKeySecret = accessKeySecret
        self.region = region
        self.queryHandle = queryHandle
        self.handle = makeHandle()

        self.data = ""
        self.filesLeft = self.queryFiles[1:]
        self.currentFile = self.queryFiles[0]
        self.currentFilePosition = 0
        self.needsData = True
        self.curPos = 0

    @memoized_property
    def prefix(self):
        return "tmp/%s/shark/%s/" % (
            self.iamUsername,
            self.queryHandle
        )

    @memoized_property
    def s3Conn(self):
        return getS3Conn(self.accessKeyId, self.accessKeySecret, region=self.region)

    @memoized_property
    def bucket(self):
        return self.s3Conn.get_bucket(mixingboard.getConf("s3_bucket"), validate=False)

    @memoized_property 
    def queryFiles(self):
        return [key.key for key in self.bucket.list(prefix=self.prefix)]

    def queryFilesDownload(self):
        return [key.generate_url(3600, method="GET") for key in self.bucket.list(prefix=self.prefix)]

    def getMoreData(self):

        while True:

            try:

                key = self.bucket.get_key(self.currentFile, validate=False)
                data = key.get_contents_as_string(
                    headers={
                        "Range": "bytes=%s-%s" % (
                            self.currentFilePosition,
                            self.currentFilePosition+self.CHUNK_SIZE
                        )
                    }
                )
                self.currentFilePosition += self.CHUNK_SIZE

                return data

            except S3ResponseError as e:

                if e.status == 416:
                    if len(self.filesLeft) > 0:
                        self.currentFile = self.filesLeft[0]
                        self.filesLeft = self.filesLeft[1:]
                        self.currentFilePosition = 0
                    else:
                        return None
                else:
                    raise

    def fetch(self):

        while True:

            if self.needsData:

                newData = self.getMoreData()
                if newData is None:
                    return None
                self.data += newData
                self.curPos = 0
                self.needsData = False

            newPos = self.data.find("\n", self.curPos)
            if newPos == -1:
                self.data = self.data[self.curPos:]
                self.needsData = True
                continue
            row = self.data[self.curPos:newPos]
            self.curPos = newPos+1
            
            return row.split("\t")
