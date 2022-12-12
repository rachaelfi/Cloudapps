import boto3
import json
from boto3 import client
from time import sleep
import sys
import logging




# code to do this 
import boto3
import json
import sys
import logging
import time
import os
import random
import string

from botocore.exceptions import ClientError

# Create SQS client
sqs = boto3.client('sqs')

# Create Docker client
docker = boto3.client('docker')

# Create docker file
dockerfile = open("Dockerfile", "w")
#specifies your runtime environment
dockerfile.write("FROM python:3.7-slim\n")

#copies your program into the container
dockerfile.write("COPY . /app\n")

#specifies a command that will run your program
dockerfile.write("CMD [\"python\", \"consumer.py\"]\n")

#sets the working directory for any RUN, CMD, ENTRYPOINT, COPY and ADD instructions that follow it in the Dockerfile
dockerfile.write("WORKDIR /app\n")

#retrieve Widget Requests from the SQS queue. The user should be able to specify the name of queue on the command line.
queue_url = os.environ['AWS_QUEUE_URL']
location = os.environ['AWS_LOCATION']
location_name = os.environ['AWS_LOCATION_NAME']
s3_bucket = os.environ['AWS_S3_BUCKET']
dynamodb_table = os.environ['AWS_DYNAMODB_TABLE']





# Create S3 client
client  = boto3.client('s3')

# Create S3 resource
s3 = boto3.resource('s3')
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table("widgets")
k = 2
logging.basicConfig(level=logging.DEBUG, filename='myapp.log', format='%(asctime)s %(levelname)s:%(message)s')

if len(sys.argv) != 4:
    print("Example: python3 consumer.py <bucket> <location> <create/dynamo>")
    print("   python consumer.py [requests source] [s3 or dynamo] [requests target (s3 bucket or dynamodb)]")
    print("   ex. 'python consumer.py usu-mybucket s3 usu-mybucket2'")
    quit()

sourceBucket = sys.argv[1]
location = sys.argv[2]
locationName = sys.argv[3]

Bucket = str(sourceBucket)

def dynamo():
    for j in keyList:
        try:
            content_object = s3.Object(Bucket, str(j))
            file_content = content_object.get()['Body'].read().decode('utf-8')
            json_content = json.loads(file_content)
            oname = json_content['owner']
            wid = json_content['widgetId']
            ty = json_content['type']
            rid = json_content['requestId']
            lab = json_content['label']
            des = json_content['description']
            oa = json_content['otherAttributes']

            table.put_item(
                TableName=str(locationName),
                Item = {
                    "id": str(j),
                    "type":str(ty),
                    "requestId":str(rid),
                    "widgetId":str(wid),
                    "owner":str(oname),
                    "label":str(lab),
                    "description":str(des),
                    "otherAttributes":str(oa)
                }
                    )
            logging.debug("Created Object in DynamoDB: " + str(locationName) +" (Key: " + str(j) + ")")
            print("Created Object in DynamoDB: " + str(locationName) +" (Key: " + str(j) + ")")
            s3.Object(Bucket, str(j)).delete()
            

        except KeyError:
            logging.error('CREATION OF OBJECT CONSUMED AT KEY ' + j + " FAILED")
            print('ERROR: CREATION OF OBJECT CONSUMED AT KEY ' + j + " FAILED")
            s3.Object(Bucket, str(j)).delete()

    keyList.clear()


def create():

    for j in keyList:
        try:
            content_object = s3.Object(Bucket, str(j))
            file_content = content_object.get()['Body'].read().decode('utf-8')
            json_content = json.loads(file_content)
            oname = json_content['owner'].lower().replace(" ", "-")
            wid = json_content['widgetId']

            keyName = "widgets/" + oname + "/" + wid
            
            copy_source = {
            'Bucket': Bucket,
            'Key': str(j)
            }

            s3.meta.client.copy(copy_source, str(locationName), keyName)
            logging.debug("Created Object in Bucket: " + str(locationName) +" (Key: " + keyName + ")")
            print("Created Object in Bucket: " + str(locationName) +" (Key: " + keyName + ")")
            s3.Object(Bucket, str(j)).delete()
            

        except KeyError:
            logging.error('CREATION OF OBJECT CONSUMED AT KEY ' + j + " FAILED")
            print('ERROR: CREATION OF OBJECT CONSUMED AT KEY ' + j + " FAILED")
            s3.Object(Bucket, str(j)).delete()

    keyList.clear()


while k == 2:
    try:
        
        keyList = []
        conn = client('s3')
        for key in conn.list_objects(Bucket=Bucket)['Contents']:
            keyList.append(key['Key'])

        keyList.sort()

        if location == "s3":
            create()
        if location == "dynamo":
            dynamo()

    except KeyError:
        logging.debug("Empty Bucket")
        sleep(0.1)
        
        
# 1.Why is it possible that Update and Delete Widget Requests may fail, even when you were 
# running just one Consumer? 
# 2. How would this possible behavior impact the design of distributed applications that use 
# queues?

#Answer to 1 is that the consumer may not be able to process the request in time and the request may be timed out.
#Answer it could impact the design of distributed applications that use queues by having the consumer process the request in time and not have it time out and using the aws sqs long polling feature to reduce the number of empty responses.