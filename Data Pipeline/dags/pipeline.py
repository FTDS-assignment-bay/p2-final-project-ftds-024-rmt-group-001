# DAG Script for inserting data to Amazon RDS (MySQL)
# This DAG performs operations in this following order
# 1. Displaying starting message
# 2. Fetching and cleaning data from PostgreSQL
#    - Fetch product, customer and transaction data from database
#    - Clean raw data from database
#    - Export the clean data into csv file
# 3. Uploading data into Amazon RDS

import pandas as pd
import psycopg2 as db
import datetime as dt
import ast

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine

def extract_product_info(row):
    '''
    This function is used to split the values in a column that contains detailed product information from a transaction data.
    Please refer to the raw transaction data to see the example of the format of product metadata that is suitable for this function.

    Usage example:
    dataframe['product_metadata'].apply(extract_product_info)
    '''
    try:
        info_list = ast.literal_eval(row)
        if isinstance(info_list, list) and info_list:
            return pd.Series(info_list[0])
    except (SyntaxError, ValueError):
        pass
    return pd.Series({})

def fetchCleanData():
    '''
    This function is used to fetch data from an existing database, followed by cleaning the data, 
    resulting in a CSV file containing the cleaned data.

    To use this function, simply call the function name

    Usage example:
    fetchCleanData()
    '''
    # define postgres connection
    conn_string = f"dbname='test' host='postgres' user='airflow' password='airflow'"
    conn = db.connect(conn_string)
    
    # fetch data 
    dfCust = pd.read_sql(f"select * from public.customer", conn) #customer
    dfProd = pd.read_sql(f"select * from public.product", conn) #product
    dfTrans = pd.read_sql(f"select * from public.transactions", conn) #transaction

    # close connection
    conn.close()

    # clean data - customer data
    dfCust['birthdate'] = pd.to_datetime(dfCust['birthdate'])
    current_date = pd.to_datetime('today')
    dfCust['age'] = (current_date - dfCust['birthdate']).astype('<m8[Y]')
    dfCust['age'] = dfCust['age'].astype('int64')

    dfCustClean = dfCust.drop(columns=['username', 'email', 'birthdate', 'device_id', 'device_version', 'home_country', 'first_join_date'])

    # clean data - product data 
    dfProd.rename(columns={'id': 'product_id',
                       'masterCategory': 'master_category',
                       'subCategory': 'sub_category',
                       'articleType': 'article_type',
                       'baseColour': 'base_color',
                       'productDisplayName': 'product_name'}, inplace=True)
    
    dfProd = dfProd.dropna(subset=['gender'])
    dfProd = dfProd.dropna(subset=['product_name'])
    dfProd = dfProd.dropna(subset=['base_color'])
    dfProd = dfProd.dropna(subset=['season'])
    dfProd['usage'] = dfProd['usage'].fillna("Others")

    dfProd['year'] = dfProd['year'].astype('int64')
    dfProd['product_id'] = dfProd['product_id'].astype('int64')

    dfProdClean = dfProd.copy()

    # clean data - transaction data 
    dfTrans = dfTrans.drop(columns=['created_at', 'session_id', 'promo_code', 'shipment_date_limit', 
                                'shipment_location_lat', 'shipment_location_long'])
    
    new_columns = dfTrans['product_metadata'].apply(extract_product_info)
    dfTrans = pd.concat([dfTrans, new_columns], axis=1)
    dfTrans.drop(columns='product_metadata', inplace=True)
    
    dfTrans['customer_id'] = dfTrans['customer_id'].astype('int64')

    dfTransClean = dfTrans.copy()

    # merge data
    merged_df = pd.merge(dfTransClean, dfCustClean, on='customer_id', how='inner', suffixes=('_dfTrans', '_dfCust'))
    merged_df = pd.merge(merged_df, dfProdClean, on='product_id', how='inner', suffixes=('', '_dfProd'))

    merged_df = merged_df.rename(columns={'booking_id': 'transaction_id'})

    merged_df = merged_df[['transaction_id', 'payment_method', 'payment_status',
                       'promo_amount', 'shipment_fee', 'total_amount',
                       'customer_id', 'first_name', 'last_name', 'gender',
                       'device_type', 'home_location_lat', 'home_location_long',
                       'home_location', 'age', 'gender_dfProd',
                       'product_id', 'quantity', 'item_price', 
                       'product_name', 'master_category', 'sub_category',
                       'article_type', 'base_color', 'season', 
                       'year', 'usage']]
    
    merged_df.rename(columns={'gender': 'customer_gender',
                          'gender_dfProd': 'product_gender'}, inplace=True)

    #save to csv file
    merged_df.to_csv('/opt/airflow/dags/fashion-dataset-clean.csv', index=False)
    
def insertToCloudMySQL():
    '''
    This function is used to insert the cleaned data from fetchCleanData() function to MySQL in Amazon RDS
    Please refer to the documentation of create_engine by sqlalchemy to see how to define the connection to the database

    To use this function, simply call the function name

    Usage example:
    insertToCloudMySQL()
    '''
    df = pd.read_csv('/opt/airflow/dags/fashion-dataset-clean.csv')

    engine = create_engine('mysql+mysqlconnector://xxxx:xxxx@database-2.csrtmgsb8iz1.ap-southeast-1.rds.amazonaws.com:3306/final_project')
    df.to_sql('clean_fashion_data', con=engine, if_exists='replace', index=False)

default_args = {
    'owner': 'fauzan_risqullah',
    'start_date': dt.datetime(2023, 12, 2, 20, 55, 0) - dt.timedelta(hours=7),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
}

with DAG('fauzan_milestone3',
         default_args=default_args,
         schedule_interval='1 0 29 * *', # During the development process, this DAG is set to be run on monthly basis.
         ) as dag:

    startMessage = BashOperator(task_id='startMessage',
                                bash_command='echo "Start fetching data from Postgres...."')
    
    fetchClean = PythonOperator(task_id='fetch-clean-data',
                                   python_callable=fetchCleanData)
    
    insertMySQL = PythonOperator(task_id='insert-mysql',
                                  python_callable=insertToCloudMySQL)
    
startMessage >> fetchClean >> insertMySQL
