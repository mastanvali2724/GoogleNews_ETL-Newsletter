import pandas as pd
import requests
import boto3
import s3fs
import datetime
from dotenv import load_dotenv
import os
import xmltodict
import json


#To convert timezone to IST
def convert_to_ist(utc_time):
    carry = 0
    hours = int(utc_time[0:2])
    minutes = int(utc_time[3:5])
    ist_minutes = minutes + 30
    if ist_minutes>59:
        ist_minutes = ist_minutes % 59
        carry = 1
    ist_hours = hours + 5 + carry
    ist_time = str(ist_hours) + ":" + ("0" + str(ist_minutes))[len("0" + str(ist_minutes))-2 :] + " IST"
    return ist_time

def get_news(NEWS_CATEGORY,email):
    '''
    This is the Main function of this file.

    It is a function to run ETL Pipeline.

    E - Extract
        Here we are getting the news details from google news
        and getting the result in XML Format.
    
    T - Transform
        We are parsing the XML response to Json Object using xmltodict and json packages
        Next we are storing the required information into a Python Dictionary

    L - Load
        We are converting the Dictionary into a Pandas DataFrame 
        And uploading it in the form of a csv into a AWS S3 Bucket
    '''

    # 1. Loading the environment variables - The access and secret key of aws account
    load_dotenv()
    access_key = os.getenv("ACCESS_KEY")
    secret_key = os.getenv("SECRET_KEY")
    
    # 2. Extract the XML data by doing a GET request to google news. USing NEWS_CATEGORY to collect news data of a particular topic
    r = requests.get(f'https://news.google.com/rss/search?q={NEWS_CATEGORY}&hl=en-IN&gl=IN&ceid=IN:en')

    # 3.convert XML to Json
    data_dict = xmltodict.parse(r.text)
    json_data = json.dumps(data_dict)

    # 4. Collect the necessary information required from variable  - json_data
    list_of_news = json.loads(json_data)["rss"]["channel"]["item"]

    # 5. Creating a dictionary to store and process further
    my_dict = {'title':[],
               'link':[],
               'pubDate':[],
               }
    for news in list_of_news:
        my_dict['title'].append(news['title'])
        my_dict['link'].append(news['link'])
        my_dict['pubDate'].append(str(news['pubDate'][:17]) + convert_to_ist(news['pubDate'][17:25]))

    # 6. Converting into a Pandas Dataframe
    df = pd.DataFrame(my_dict)

    # 7. Uploading as a csv file into AWS S3 bucket. 
    # Here we need the environment variable(access and secret keys) to authenticate with AWS
    dt_India = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=5, minutes=30)))
    print(NEWS_CATEGORY)
    df.to_csv(f"s3://news-bucket2207/{email}/{NEWS_CATEGORY}-news-{str(dt_India)[:16]}.csv", storage_options={'key':access_key, 'secret':secret_key})

def etl_pipeline(cat,email):
    for c in cat:
        get_news(c, email)