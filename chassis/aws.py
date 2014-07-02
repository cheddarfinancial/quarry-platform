# Standard Library
import csv
import datetime
from cStringIO import StringIO
from collections import defaultdict

# Third Party
import boto.ec2
import boto.s3
import boto.route53
import boto.iam

# Local
import mixingboard
from database import db_session


def getMasterCredentials():

    aws_key = mixingboard.getConf("aws_key")
    aws_secret = mixingboard.getConf("aws_secret")

    return aws_key, aws_secret

def getEC2Conn(aws_key, aws_secret, region='us-west-2'):

    return boto.ec2.connect_to_region(region, aws_access_key_id=aws_key, aws_secret_access_key=aws_secret)

def getRoute53Conn(aws_key, aws_secret, region='us-west-2'):

    return boto.route53.connect_to_region(region, aws_access_key_id=aws_key, aws_secret_access_key=aws_secret)

def getS3Conn(aws_key, aws_secret, region='us-west-2'):

    return boto.s3.connect_to_region(region, aws_access_key_id=aws_key, aws_secret_access_key=aws_secret)

def getMasterEC2Conn(region='us-west-2'):

    key, secret = getMasterCredentials()
    return boto.ec2.connect_to_region(region, aws_access_key_id=key, aws_secret_access_key=secret)

def getMasterRoute53Conn(region='us-west-2'):

    key, secret = getMasterCredentials()
    return boto.route53.connect_to_region(region, aws_access_key_id=key, aws_secret_access_key=secret)

def getMasterS3Conn(region='us-west-2'):

    key, secret = getMasterCredentials()
    return boto.s3.connect_to_region(region, aws_access_key_id=key, aws_secret_access_key=secret)

def getIAMConn(aws_key, aws_secret, region='us-west-2'):

    return boto.iam.connect_to_region(region, aws_access_key_id=aws_key, aws_secret_access_key=aws_secret)



###
# Calculate the cost for each account
###

smallWorkerPrice = 0.09
workerPrice = 0.15 

def calculateCost(csvData):

    reader = csv.reader(csvData)

    final = None
    costs = defaultdict(lambda: defaultdict(int))

    for row in reader:
        if len(row) == 33:
            if row[31]:
                try:
                    account = int(row[31])
                except ValueError:
                    continue

                if final is None:
                    final = row[0] != "Estimated"

                description = row[19]

                if description.find("On Demand Linux") != -1:

                    baseCost = 0.0
                    amount = float(row[22])
                    small = False

                    if description.find("m1.small") != -1:
                        baseCost = smallWorkerPrice
                        small = True
                    elif description.find("m1.medium") != -1 or description.find("m3.medium") != -1:
                        baseCost = workerPrice
                    elif description.find("m1.large") != -1 or description.find("m3.large") != -1:
                        baseCost = workerPrice*2
                        amount *= 2
                    elif description.find("m1.xlarge") != -1 or description.find("m3.xlarge") != -1:
                        baseCost = workerPrice*4
                        amount *= 4

                    if small:
                        costs[account]['smallWorkerHours'] += amount
                        costs[account]['smallWorkerCost'] += round(baseCost * amount, 2)
                    else:
                        costs[account]['workerHours'] += amount
                        costs[account]['workerCost'] += round(baseCost * amount, 2)

                elif description.find("data transfer") != -1:

                    cost = float(row[29])
                    if cost > 0:
                        costs[account]['transferCost'] += float(row[29])
                    
    for _, value in costs.items():
        if value.get('workerCost'):
            value['workerCost'] = round(int(value['workerCost']/0.05+0.5)*0.05, 2)
        if value.get('smallWorkerCost'):
            value['smallWorkerCost'] = round(int(value['smallWorkerCost']/0.05+0.5)*0.05, 2)
        value['workerPrice'] = workerPrice
        value['smallWorkerPrice'] = smallWorkerPrice

    return dict({key: dict(value) for key, value in costs.items()}), final

START_PERIOD = "201405"

def runBillingReport():

    from chassis.models import Account, Bill

    date = datetime.datetime.now()
    month = date.month
    if month < 10: month = "0%s" % month
    year = date.year

    maxPeriod = int("%s%s" % (year, month))

    bill = Bill.query.filter(Bill.final, Bill.account_id == None).order_by(Bill.period.desc()).first()
    startPeriod = None 
    if bill:
        startPeriod = str(int(bill.period)+1)
    else:
        startPeriod = START_PERIOD

    while int(startPeriod) <= maxPeriod:

        print "Calculating billing for %s" % startPeriod

        conn = getMasterS3Conn(region='us-east-1')
        bucket = conn.get_bucket("quarry-billing", validate=False)
        key = bucket.get_key("832367826950-aws-cost-allocation-%s-%s.csv" % (startPeriod[0:4], startPeriod[4:]), validate=False)
        csvData = StringIO(key.get_contents_as_string())
        
        costs, final = calculateCost(csvData)

        totalMachineCost = 0.0
        totalTransferCost = 0.0
        totalStorageUsed = 0.0

        for account_id, data in costs.items():

            account = Account.query.filter(Account.id == account_id).first()

            if account:

                storageUsed = account.getStorageUsage()

                machineCost = data['workerCost'] + data['smallWorkerCost']
                totalMachineCost += machineCost

                transferCost = data['transferCost'] 
                totalTransferCost += transferCost

                totalStorageUsed += storageUsed

                bill = Bill(account_id, final, data, machineCost, transferCost, storageUsed, startPeriod)
                db_session.add(bill)
                db_session.commit()

        bill = Bill(None, final, {}, totalMachineCost, totalTransferCost, totalStorageUsed, startPeriod)
        db_session.add(bill)
        db_session.commit()

        startPeriod = str(int(startPeriod) + 1)
