import boto3
import json
from boto3 import client
from time import sleep
import sys
import logging

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