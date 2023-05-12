

import os
from dotenv import load_dotenv
import requests
import json
import boto3
from datetime import datetime, timedelta

s3 = boto3.client('s3')
headers = {'content-type': 'application/json'}

load_dotenv('.env') 

SLACK_TOKEN = os.getenv("SLACK_TOKEN") 
SLACK_WEBHOOK_REPORTER = os.getenv("SLACK_WEBHOOK_REPORTER")
SLACK_WEBHOOK_DEVELOPER = os.getenv("SLACK_WEBHOOK_DEVELOPER") 
AWS_BUCKET_NAME_MEDIA =  os.getenv("AWS_BUCKET_NAME_MEDIA")
AWS_BUCKET_PATH_MEDIA = os.getenv("AWS_BUCKET_PATH_MEDIA") 
AWS_BUCKET_NAME_PERM = os.getenv("AWS_BUCKET_NAME_PERM")
AWS_BUCKET_PATH_PERM = os.getenv("AWS_BUCKET_PATH_PERM") 


def postMessageToSlack(text, type):
    """
    Function to post messages to slack channels for alerts or errors.
    Parameters:
        - text : str
            Message text body, can be plain text or mrkdwn (https://api.slack.com/reference/surfaces/formatting)
        - dev: str (dev/rep)
            Determines if the message is sent to the developer slack channel for error messages or reporter webhook channel 
            for race calls and other alerts. Default is developer (dev). Webhooks are set in Heroku's config.
    """
    data = {
        'token': SLACK_TOKEN,
        'text': text
    }
    if type == "developer":
        response = requests.post(SLACK_WEBHOOK_DEVELOPER, data = json.dumps(data), headers=headers)	
    elif type == "reporter":
        #data['text'] = "<!channel> " + text
        response = requests.post(SLACK_WEBHOOK_REPORTER, data = json.dumps(data), headers=headers)	
    return response 	

def saveDataToS3(dict_data, s3_filename, folder_var):
    """
    Function to convert and save Python dicts as json files in s3 buckets.
    Parameters:
        - dict_data : dict
            dict to be sent to s3 as a json file
        - s3_filename : str
            Name of the json file in s3.
        - s3_bucket : str
            Name of s3 bucket that we're sending file to.
        - s3_path : str
            Name of path within the s3 bucket.
    """
    upload_byte_stream = dict_data

    if folder_var == 'live':
       # print('here')
        s3_path = AWS_BUCKET_PATH_PERM
        s3_bucket = AWS_BUCKET_NAME_PERM
    if folder_var == 'archive':
        s3_bucket = AWS_BUCKET_NAME_MEDIA
        s3_path = AWS_BUCKET_PATH_MEDIA

        if ".csv" in s3_filename or "ap_results" in s3_filename:
            s3.put_object(  Body=upload_byte_stream, 
                            Bucket=s3_bucket, 
                            Key='{}/{}'.format(s3_path, s3_filename), 
                            ACL='public-read',
                            CacheControl = 'max-age=30')

    s3.put_object(  Body=upload_byte_stream, 
                    Bucket=s3_bucket, 
                    Key='{}/{}'.format(s3_path, s3_filename),
                    ACL='public-read',
                    CacheControl = 'max-age=30')



def getDataFromS3(bucket_path):
    """
    Function to get race alerts from s3, returns a json object of the races that 
    have yet to be called that reporters want to be alerted to whenc called.
    """
    try:
        response = s3.get_object(Bucket=AWS_BUCKET_NAME_MEDIA, Key= f"{AWS_BUCKET_PATH_MEDIA}/{bucket_path}.json")['Body'].read()
        data = json.loads(response.decode('utf-8'))
        return data
    except:
        str_slack = f"getDataFromS3() | Problem with grabbing {bucket_path}) from s3!"
        postMessageToSlack(str_slack,"dev")
        
def getSnapshotTime():
    now = datetime.now()
    year = str(now.year)
    month = str(now.month).zfill(2)
    day = str(now.day).zfill(2)
    hour = str(now.hour).zfill(2)
    minute = str(now.minute).zfill(2)
    second = str(now.second).zfill(2)

    return '{}_{}_{}_{}_{}_{}'.format(year, month, day, hour, minute, second)