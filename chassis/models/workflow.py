# Standard Library
import datetime
from collections import defaultdict

# Third Party
from chassis.util import makeHandle
from chassis.aws import getS3Conn
from ..database import Base, JsonType
from memoized_property import memoized_property
from sqlalchemy import event, Column, Integer, String, DateTime, Text, ForeignKey
from sqlalchemy.orm import relationship

# Local


class Workflow(Base):

    __tablename__ = 'workflow'
    __serialize_exclude__ = { 'account', 'user', 'firstNode' }

    id = Column(Integer, primary_key=True)
    account_id = Column(Integer, ForeignKey('account.id'), index=True)
    user_id = Column(Integer, ForeignKey('user.id'), index=True)
    title = Column(String(50))
    description = Column(Text)
    cluster = Column(JsonType(), default={})
    steps = Column(JsonType(), default=[])
    schedule_minute = Column(String(10))
    schedule_hour = Column(String(10))
    schedule_day_of_month = Column(String(10))
    schedule_month = Column(String(10))
    schedule_day_of_week = Column(String(10))
    last_run = Column(DateTime)
    notify_users = Column(JsonType(), default=[])
    created = Column(DateTime, default=datetime.datetime.utcnow)
    updated = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    account = relationship("Account")
    user = relationship("User")

    def __init__(self, title=None, account_id=None, user_id=None, description=None, schedule_minute=None,
                 schedule_hour=None, schedule_day_of_month=None, schedule_month=None, schedule_day_of_week=None,
                 steps=[], cluster=None, notify_users=[]):
        
        if cluster is None:
            cluster = {
                "action": "start",
                "name": makeHandle(),
                "workers": 1
            }

        self.account_id = account_id
        self.user_id = user_id
        self.title = title
        self.description = description
        self.schedule_minute = schedule_minute
        self.schedule_hour = schedule_hour
        self.schedule_day_of_month = schedule_day_of_month
        self.schedule_month = schedule_month
        self.schedule_day_of_week = schedule_day_of_week

        errors = defaultdict(list)
        try:
            self.steps = steps
        except ValueError as e:
            errors['steps'] = e.message
        try:
            self.cluster = cluster 
        except ValueError as e:
            errors['cluster'] = e.message

        if not title or len(title) == 0:
            errors['top'].append("You must give your workflow a title.")

        try:
            self.notify_users = notify_users
        except ValueError as e:
            errors['top'].extend(e.message)

        if len(errors) > 0:
            raise ValueError(errors)

    def getStepsWithRelations(self):
        
        steps = list(self.steps)

        for step in steps:

            if step['type'] == 'sql':

                if not step.get('id'):
                    continue
                from query import Query
                query = Query.query.filter(Query.id == step['id'], Query.account_id == self.account_id).first()
                if query:
                    step['query'] = query.dict()

            elif step['type'] == 'python':

                if not step.get('id'):
                    continue
                from job import Job
                job = Job.query.filter(Job.id == step['id'], Job.account_id == self.account_id).first()
                if job:
                    step['job'] = job.dict()

            elif step['type'] == 'import':

                if not step.get('id'):
                    continue
                from datajob import DataJob
                datajob = DataJob.query.filter(DataJob.id == step['id'], DataJob.account_id == self.account_id,
                                DataJob.action == "import").first()
                if datajob:
                    step['datajob'] = datajob.dict()

            elif step['type'] == 'export':

                if not step.get('id'):
                    continue
                from datajob import DataJob
                datajob = DataJob.query.filter(DataJob.id == step['id'], DataJob.account_id == self.account_id,
                                DataJob.action == "export").first()
                if datajob:
                    step['datajob'] = datajob.dict()

        return steps

    
    def addNotifyUser(self, user_id):

        self.notify_users.append(user_id)
        self.notify_users = list(set(self.notify_users))

        try:
            self.notify_users.remove(-1)
        except ValueError:
            pass

    def removeNotifyUser(self, user_id):

        try:

            self.notify_users.remove(user_id)
            self.notify_users = list(set(self.notify_users))

            try:
                self.notify_users.remove(-1)
            except ValueError:
                pass

        except ValueError:
            pass

    def notifyAll(self):

        self.notify_users = [-1]

    def notifyNone(self):

        self.notify_users = []

    def __repr__(self):
        return '<WorkFlow %r>' % (self.title)

###
# EVENTS
###

def validateSteps(target, steps, oldSteps, initiator):

    errors = defaultdict(list)

    for index, step in enumerate(steps):

        if step['type'] == 'sql':

            from query import Query
            if not step.get('id') and not step.get('sql'):
                errors[index].append("This step requires a valid saved query or an ad hoc query") 
                continue

            if step.get('id'):

                query = Query.query.filter(Query.id == step['id'], Query.account_id == target.account_id).first()
            
                if not query:
                    errors[index].append("Query with id '%s' does not exist" % step['id'])

        elif step['type'] == 'python':

            from job import Job
            if not step.get('id'):
                errors[index].append("This step requires a valid job") 
                continue
            job = Job.query.filter(Job.id == step['id'], Job.account_id == target.account_id).first()
            
            if not job:
                errors[index].append("Job with id '%s' does not exist" % step['id'])

        elif step['type'] == 'import':

            from datajob import DataJob
            if not step.get('id'):
                errors[index].append("This step requires a valid import job") 
                continue
            datajob = DataJob.query.filter(DataJob.id == step['id'], DataJob.account_id == target.account_id,
                            DataJob.action == "import").first()
            
            if not datajob:
                errors[index].append("Import job with id '%s' does not exist" % step['id'])

        elif step['type'] == 'export':

            from datajob import DataJob
            if not step.get('id'):
                errors[index].append("This step requires a valid export job") 
                continue
            datajob = DataJob.query.filter(DataJob.id == step['id'], DataJob.account_id == target.account_id,
                            DataJob.action == "export").first()
            
            if not datajob:
                errors[index].append("Export job with id '%s' does not exist" % step['id'])

        else:

            errors[index].append("No step of type '%s'" % step['type'])

    if len(errors) > 0:
        raise ValueError(dict(errors))

def validateCluster(target, cluster, oldCluster, initiator):

    action = cluster.get('action')

    errors = []

    if action == 'start':

        if not cluster.get('name'):
            errors.append('You must name your cluster')
        try:
            workers = int(cluster.get('workers'))
            if workers <= 0:
                errors.append('The number of workers for this cluster must greater than zero')
        except Exception as e:
            # catch all to deal with any type of error coercing to int
            errors.append('You must specify the number of workers for this cluster as an integer')

    elif action == 'pick':

        if not cluster.get('name'):
            errors.append('You must provide the name of an existing cluster')

    else:

        errors.append("Unknown cluster action '%s'" % action)

    if len(errors) > 0:
        raise ValueError(errors)


def validateNotifyUsers(target, notifyUsers, oldNotifyUsers, initiator):
    from user import User

    errors = []

    for user in notifyUsers:

        if type(user) != int:
            errors.append("'%s' is not a valid user id" % user)
            continue

        if user == -1:
            continue

        if not User.query.filter(User.id == user).first():
            errors.append("No user exists with the id '%s'" % user)

    if len(errors) > 0:
        raise ValueError(errors)


event.listen(Workflow.steps, 'set', validateSteps)
event.listen(Workflow.cluster, 'set', validateCluster)
event.listen(Workflow.notify_users, 'set', validateNotifyUsers)
