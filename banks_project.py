"""Complete ETL process"""

import sqlite3
from datetime import datetime
import requests
import pandas as pd
from bs4 import BeautifulSoup

URL_DATA = ('https://web.archive.org/web/20230908091635'
            '/https://en.wikipedia.org/wiki/List_of_largest_banks')
CSV_EXCHANGE_RATE = './exchange_rate.csv'
OUTPUT_CSV_PATH = './Largest_banks_data.csv'
LOG_FILE = './code_log.txt'
TARGET_FILE = "./Banks.db"

table_att_initial = ['Name', 'MC_USD_Billion']
table_att_final = ['Name', 'MC_USD_Billion', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']
TABLE_NAME = 'Largest_banks'

conn = sqlite3.connect(TARGET_FILE)


def log_progress(message):
    """ This function logs the mentioned message at a given stage of the code execution to a
    log file. Function returns nothing"""
    timestamp_format = '%Y-%h-%d %H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(timestamp + ',' + message + '\n')


def extract(url, table_att):
    """ The purpose of this function is to extract the required
        information from the website and save it to a dataframe.
        The function returns the dataframe for further processing. """
    df_ = pd.DataFrame(columns=table_att)
    html_page = requests.get(url, timeout=10).text
    data = BeautifulSoup(html_page, 'html.parser')
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            data_dict = {"Name": col[1].text.strip(),
                         "MC_USD_Billion": float(col[2].text.strip()), }
            df1 = pd.DataFrame(data_dict, index=[0])
            df_ = pd.concat([df_, df1], ignore_index=True)

    return df_


def transform(df_):
    """ This function converts the GDP information from Currency
        format to float value, transforms the information of GDP from
        USD (Millions) to USD (Billions) rounding to 2 decimal places.
        The function returns the transformed dataframe."""
    try:
        exchange_rates = pd.read_csv(CSV_EXCHANGE_RATE)
        count = 0
        for rate in exchange_rates.iloc[:, 1]:
            for attr in exchange_rates.iloc[:, 0]:
                if attr in table_att_final[count + 2].split('_'):
                    df_[table_att_final[count + 2]] = round(rate * df_.iloc[:, 1], 2)
            count += 1
    except FileNotFoundError:
        print('csv file not found')

    return df_


def load_to_csv(df_, file_path):
    """ This function saves the final data frame as a CSV file in
        the provided path. Function returns nothing."""
    df_.to_csv(file_path, index=False)


def load_to_db(df_):
    """ This function saves the final data frame to a database
        table with the provided name. Function returns nothing."""
    df_.to_sql(TABLE_NAME, con=conn, if_exists='replace')


def run_query(query_statement, conn_):
    """ This function runs the query on the database table and
        prints the output on the terminal. Function returns nothing. """
    query_output = pd.read_sql(query_statement, con=conn_)
    print(query_statement)
    print(query_output)


log_progress("Preliminaries complete. Initializing ETL process")

df = extract(URL_DATA, table_att_initial)
log_progress("Data extraction complete. Initializing Transformation process")

df = transform(df)
log_progress("Transformation complete. Saving the df to the csv file")

load_to_csv(df, OUTPUT_CSV_PATH)
log_progress("Data saved to the csv file. Saving the df to the DB")

load_to_db(df)
log_progress("Data saved to the DB. Running some queries")

query_statement1 = f'SELECT * FROM {TABLE_NAME}'
query_statement2 = f'SELECT * FROM {TABLE_NAME} WHERE MC_USD_Billion > 200'
QUERY_STATEMENT3 = 'SELECT * FROM Largest_banks'
query_statement4 = f'SELECT AVG(MC_GBP_Billion) FROM {TABLE_NAME}'
QUERY_STATEMENT5 = 'SELECT Name from Largest_banks LIMIT 5'

try:
    run_query(query_statement1, conn)
    run_query(query_statement2, conn)
    run_query(QUERY_STATEMENT3, conn)
    run_query(query_statement4, conn)
    run_query(QUERY_STATEMENT5, conn)
except (sqlite3.OperationalError, sqlite3.IntegrityError, sqlite3.DatabaseError) as e:
    print('Exception when executing run_query():', e)
log_progress("Queries executed successfully")

conn.close()
