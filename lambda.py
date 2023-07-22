import json
import urllib.parse
import boto3
import io
import pandas as pd
import datetime

# Function to send an email to user using AWS SES
def send_ses(message, subject,dest_email):
    try:
        client = boto3.client('ses',region_name='us-east-1')
        result = client.send_email(
        Destination={
            'ToAddresses': [
                dest_email
        ],
    },
        Message={
            'Body': {
                'Html': {
                    'Charset': 'UTF-8',
                    'Data': message,
                },
            },
        'Subject': {
            'Charset': 'UTF-8',
            'Data': subject
        },
    },
        Source='source_email', # Give the source email.Note that this email should be verified in SES.
        SourceArn='arn:aws:ses:us-east-1:143602348576:identity/source_email', # Give the ARN of the source email in SES.
    )
        if result['ResponseMetadata']['HTTPStatusCode'] == 200:
            print(result)
            print("Notification send successfully..!!!")
            return True
    except Exception as e:
        print("Error occured while publish notifications and error is : ", e)
        return True

s3 = boto3.client('s3')

def lambda_handler(event, context):
    
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    print(key)
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(io.BytesIO(response['Body'].read()),header=0, delimiter=",", low_memory=False)
        print(df)
        msg = "<html> <head> </head> <body>"
        for i in range(len(df)):
            msg += "<p> <div> "
            msg += df.iloc[i]['pubDate']
            msg+= "<br>"
            msg += "<strong>"
            msg+= df.iloc[i]['title']
            msg += "</strong>"
            msg+="<br>"
            msg+= "<a href=\"" + df.iloc[i]['link'] + "\">See More</a>"
            msg+="<br>"
            msg += " </div> </p> "
        message = msg
        subject = key.split('/')[1].split('.')[0] + " IST"
        dest_email = key.split('/')[0]
        ses_result = send_ses(message,subject,dest_email)
        if ses_result:
            print("News sent successfully")
            return ses_result
        else:
            return "Failed"
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e