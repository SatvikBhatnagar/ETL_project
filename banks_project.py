from datetime import datetime
import requests
import pandas as pd
from bs4 import BeautifulSoup
import sqlite3

url_data = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
csv_exchange_rate = './exchange_rate.csv'
output_csv_path = './Largest_banks_data.csv'
log_file = './code_log.txt'
target_file = "./Banks.db"

table_att_initial = ['Name', 'MC_USD_Billion']
table_att_final = ['Name', 'MC_USD_Billion', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']
table_name = 'Largest_banks'

conn = sqlite3.connect(target_file)


def log_progress(message):
    """ This function logs the mentioned message at a given stage of the code execution to a log file.
    Function returns nothing"""
    timestamp_format = '%Y-%h-%d %H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, 'a') as f:
        f.write(timestamp + ',' + message + '\n')


def extract(url, table_att):
    """ The purpose of this function is to extract the required
        information from the website and save it to a dataframe.
        The function returns the dataframe for further processing. """
    df_ = pd.DataFrame(columns=table_att)
    html_page = requests.get(url).text
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
        exchange_rates = pd.read_csv(csv_exchange_rate)
        for rate in exchange_rates.iloc[:, 1]:
            for currency in table_att_final[2:]:
                df_[currency] = round(rate * df_.iloc[:, 1], 2)
                pass
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
    df_.to_sql(table_name, con=conn, if_exists='replace')


def run_query(query_statement, conn_):
    """ This function runs the query on the database table and
        prints the output on the terminal. Function returns nothing. """
    query_output = pd.read_sql(query_statement, con=conn_)
    print(query_statement)
    print(query_output)


log_progress("Preliminaries complete. Initializing ETL process")

df = extract(url_data, table_att_initial)
log_progress("Data extraction complete. Initializing Transformation process")

df = transform(df)
log_progress("Transformation complete. Saving the df to the csv file")

load_to_csv(df, output_csv_path)
log_progress("Data saved to the csv file. Saving the df to the DB")

load_to_db(df)
log_progress("Data saved to the DB. Running some queries")

query_statement1 = f'SELECT * FROM {table_name}'
query_statement2 = f'SELECT * FROM {table_name} WHERE MC_USD_Billion > 200'
query_statement3 = f'SELECT * FROM Largest_banks'
query_statement4 = f'SELECT AVG(MC_GBP_Billion) FROM {table_name}'
query_statement5 = f'SELECT Name from Largest_banks LIMIT 5'

try:
    run_query(query_statement1, conn)
    run_query(query_statement2, conn)
    run_query(query_statement3, conn)
    run_query(query_statement4, conn)
    run_query(query_statement5, conn)
except Exception as e:
    print('Exception when executing run_query():', e)
log_progress("Queries executed successfully")

conn.close()
