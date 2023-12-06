# DAG Script for inserting data to big-query
# This DAG performs operations in this following order
# 1. Displaying starting message
# 2. Fetching and cleaning data from PostgreSQL
#    - Fetch product, customer and transaction data from database
#    - Clean raw data from database
#    - Export the clean data into csv file
# 3. Uploading data into google bigquery

import pandas as pd
import psycopg2 as db
import datetime as dt
import time
import ast
import os

from google.cloud import bigquery
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

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
    dfTrans = dfTrans.drop(columns=['session_id', 'promo_code', 'shipment_date_limit', 
                                'shipment_location_lat', 'shipment_location_long'])
    
    dfDetTrans = dfTrans[['booking_id', 'product_metadata']]

    dfDetTrans['product_metadata'] = dfDetTrans['product_metadata'].apply(ast.literal_eval)

    dfDetTrans['product_id'] = dfDetTrans['product_metadata'].apply(lambda x: [item['product_id'] for item in x])
    dfDetTrans['quantity'] = dfDetTrans['product_metadata'].apply(lambda x: [item['quantity'] for item in x])
    dfDetTrans['item_price'] = dfDetTrans['product_metadata'].apply(lambda x: [item['item_price'] for item in x])

    expanded_data = {'booking_id': [], 'product_id': [], 'quantity': [], 'item_price': []}

    for _, row in dfDetTrans.iterrows():
        booking_id = row['booking_id']
        product_ids = row['product_id']
        quantities = row['quantity']
        prices = row['item_price']

        for product_id, quantity, price in zip(product_ids, quantities, prices):
            expanded_data['booking_id'].append(booking_id)
            expanded_data['product_id'].append(product_id)
            expanded_data['quantity'].append(quantity)
            expanded_data['item_price'].append(price)

    new_df = pd.DataFrame(expanded_data)

    dfTrans.drop(columns='product_metadata', inplace=True)
    
    dfTrans['customer_id'] = dfTrans['customer_id'].astype('int64')
    dfTrans = dfTrans.rename(columns={'created_at': 'transaction_date'})

    dfTransClean = dfTrans.copy()

    # merge data
    merged_detdf = pd.merge(new_df, dfProdClean, on='product_id', how='inner', suffixes=('', '_dfProd'))
    merged_custdf = pd.merge(dfTransClean, dfCustClean, on='customer_id', how='inner', suffixes=('_dfTrans', '_dfCust'))
    merged_all = pd.merge(merged_detdf, merged_custdf, on='booking_id', how='inner', suffixes=('_mergDet', '_mergCust'))

    merged_all = merged_all.rename(columns={'booking_id': 'transaction_id'})

    merged_all = merged_all[['transaction_id', 'transaction_date', 'payment_method', 'payment_status',
                       'promo_amount', 'shipment_fee', 'total_amount',
                       'customer_id', 'first_name', 'last_name', 'gender_mergCust',
                       'device_type', 'home_location_lat', 'home_location_long',
                       'home_location', 'age', 'gender_mergDet',
                       'product_id', 'quantity', 'item_price', 
                       'product_name', 'master_category', 'sub_category',
                       'article_type', 'base_color', 'season', 
                       'year', 'usage']]
    
    merged_all.rename(columns={'gender_mergCust': 'customer_gender',
                          'gender_mergDet': 'product_gender'}, inplace=True)

    #save to csv file
    merged_all.to_csv('/opt/airflow/dags/fashion-dataset-clean.csv', index=False)
    
def insertToBigQuery():
    '''
    This function will insert the cleaned data from fetchCleanData funtion to Google BigQuery
    To insert the data, please prepare a Google credential file and place the file into the key ['GOOGLE_APPLICATION_CREDENTIALS'] of the environ variable from the os library.
    Make sure to put the Google credential file inside the dags folder or any folder that is binded to the dag folder of Airflow.

    To use this function, simply call the function name

    Usage example:
    insertToBigQuery()
    '''
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/opt/airflow/dags/hacktiv8-400311-3158532bc9d0.json"

    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        source_format = bigquery.SourceFormat.CSV,
        # skip_Leading_rows = 1, 
        autodetect = True,
    )

    table_id = "hacktiv8-400311.final_project.clean_fashion_data_new_2"

    with open(r'/opt/airflow/dags/fashion-dataset-clean.csv', 'rb') as source:
        job = client.load_table_from_file(source, table_id, job_config=job_config)

    while job.state !='DONE':
        time.sleep(2)
        job.reload()
        print(job.state)
    job.result()
    table = client.get_table(table_id)
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )

default_args = {
    'owner': 'fauzan_risqullah',
    'start_date': dt.datetime(2023, 12, 2, 20, 55, 0) - dt.timedelta(hours=7),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
}

with DAG('fauzanbigquery',
         default_args=default_args,
         schedule_interval='1 0 29 * *', # During the development process, this DAG is set to be run on monthly basis. 
         ) as dag:

    startMessage = BashOperator(task_id='startMessage',
                                bash_command='echo "Start fetching data from Postgres...."')
    
    fetchClean = PythonOperator(task_id='fetch-clean-data',
                                   python_callable=fetchCleanData)
    
    insertMySQL = PythonOperator(task_id='insert-bigquery',
                                  python_callable=insertToBigQuery)
    
startMessage >> fetchClean >> insertMySQL
