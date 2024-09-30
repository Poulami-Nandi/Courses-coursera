# Importing the required libraries

from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime
import urllib.request
import os

def extract(url, table_attribs):
    ''' The purpose of this function is to extract the required
    information from the website and save it to a dataframe. The
    function returns the dataframe for further processing. '''

    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            bank_name_col = col[1].find_all('a')
            mc_usd_col = col[2]
            if bank_name_col[1] is not None and mc_usd_col is not None:
                data_dict = {table_attribs[0]: bank_name_col[1].contents[0],
                            table_attribs[1]: mc_usd_col.contents[0][:-2]}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index=True)

    return df

def transform(df, df_currency):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''

    # convert currency datframe into a dictionary for better processing
    currency_dict = df_currency.set_index('Currency').T.to_dict('list')
    MC_USD_Billion_list = df["MC_USD_Billion"].tolist()
    MC_USD_Billion_list = [float("".join(x.split(','))) for x in MC_USD_Billion_list]
    MC_GBP_Billion_list = [round(currency_dict['GBP'][0] * x,2)  for x in MC_USD_Billion_list]
    MC_EUR_Billion_list = [round(currency_dict['EUR'][0] * x,2)  for x in MC_USD_Billion_list]
    MC_INR_Billion_list = [round(currency_dict['INR'][0] * x,2)  for x in MC_USD_Billion_list]
    df["MC_USD_Billion"] = MC_USD_Billion_list
    df["MC_EUR_Billion"] = MC_EUR_Billion_list
    df["MC_INR_Billion"] = MC_INR_Billion_list
    df["MC_GBP_Billion"] = MC_GBP_Billion_list
    return df

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file
    in the provided path. Function returns nothing.'''

    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe to as a database table
    with the provided name. Function returns nothing.'''

    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''

    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the
    code execution to a log file. Function returns nothing.'''

    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + ' : ' + message + '\n')

def download_f_url(url):
    ''' this function downloads file from url. The file name at the end of url
    is used to store the file inside current directory. The complete file path is returned '''
    urllib.request.urlretrieve(url, os.path.basename(url))
    print("Downloaded ", os.path.basename(url))
    return os.getcwd() + '/' + os.path.basename(url)

def download_and_df_csv(url):
    ''' download csv file from url and store that information into a dataframe and remove
    that downloaded csv file '''
    f = download_f_url(url)
    if os.path.exists(f):
        df = pd.read_csv(f)
        os.remove(f)

    return df


''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
exchange_rate_url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"
table_attribs = ["Name", "MC_USD_Billion"]
db_table_attribute = ["Name", "MC_USD_Billion", "MC_GBP_Billion", "MC_EUR_Billion", "MC_INR_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './Largest_banks_data.csv'
log_file = 'code_log.txt'

log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

log_progress("Download currency conversion information and store that into dataframe")

df_currency = download_and_df_csv(exchange_rate_url)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df, df_currency)

log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, csv_path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect(db_name)

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Running the query')

query_statement = f"SELECT * from {table_name} WHERE {db_table_attribute[1]} >= 100"
run_query(query_statement, sql_connection)
query_statement = f"SELECT * from {table_name} WHERE {db_table_attribute[2]} >= 100"
run_query(query_statement, sql_connection)
query_statement = f"SELECT * from {table_name} WHERE {db_table_attribute[3]} >= 100"
run_query(query_statement, sql_connection)
query_statement = f"SELECT * from {table_name} WHERE {db_table_attribute[4]} >= 100"
run_query(query_statement, sql_connection)

log_progress('Process Complete.')

sql_connection.close()
