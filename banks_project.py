from datetime import datetime
import requests
import pandas as pd
from bs4 import BeautifulSoup

url_data = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
csv_exchange_rate = './exchange_rate.csv'
output_csv_path = './Largest_banks_data.csv'
log_file = './code_log.txt'
target_file = "./Banks.db"

table_att_initial = ['Name', 'MC_USD_Billion']
table_att_final = ['Name', 'MC_USD_Billion', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']
table_name = 'Largest_banks'


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
                         "MC_USD_Billion": col[2].text.strip(), }
            df1 = pd.DataFrame(data_dict, index=[0])
            df_ = pd.concat([df_, df1], ignore_index=True)

    return df_


log_progress("Preliminaries complete. Initializing ETL process")

df = extract(url_data, table_att_initial)
log_progress("Data extraction complete. Initializing Transformation process")
print(df)
