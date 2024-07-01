from datetime import datetime

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
