from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import mysql.connector
from app import etl_pipeline
import os
from dotenv import load_dotenv

load_dotenv()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 7),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'news_upd_dag',
    default_args=default_args,
    description='Our first DAG with ETL process!',
    schedule_interval=timedelta(minutes=5), # Interval is given as 5 mins for testing purpose.
    catchup=False
)

# Function to retrieve the rows(user info) from RDS database.
def get_rows():
    newsuser = mysql.connector.connect(
    host = os.getenv("DB_HOSTNAME"),
    user= os.getenv("DB_ADMIN"),
    password=os.getenv("DB_ADMIN_PASS")
    )

    mycursor = newsuser.cursor()

    mycursor.execute(f"USE {os.getenv('DB_NAME')};")
    mycursor.execute("SELECT * FROM user;")

    return [[list(x)] for x in mycursor]

# Function to get the categories selected by user from database.
def get_category(cat : list):
    print(f" {cat} is a row")
    category = cat[2].split(",")
    etl_pipeline(category,cat[1])

# Defining Task
run_etl = PythonOperator.partial(
    task_id='news_details',
    dag=dag,
    python_callable=get_category
).expand(op_args = get_rows())

run_etl