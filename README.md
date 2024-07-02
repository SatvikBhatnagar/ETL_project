# World's Largest Banks Data ETL Project

## Project Overview

This project automates the process of extracting, transforming, and loading (ETL) data about the world's largest banks by market capitalization. It fetches data from a Wikipedia page, processes it, and stores the results in both CSV and SQLite database formats.

## Features

- Extracts bank data from a Wikipedia page
- Transforms market capitalization data from USD to GBP, EUR, and INR
- Loads processed data into a CSV file and SQLite database
- Logs each step of the ETL process
- Executes sample SQL queries on the resulting database

## Requirements

- Python 3.x
- Required Python packages:
  - requests
  - pandas
  - beautifulsoup4
  - sqlite3

## File Structure

- `main.py`: The main Python script that performs the ETL process
- `exchange_rate.csv`: CSV file containing currency exchange rates
- `Largest_banks_data.csv`: Output CSV file with processed bank data
- `code_log.txt`: Log file that records the progress of the ETL process
- `Banks.db`: SQLite database file storing the processed data
- `docs/`: Directory containing project documentation
  - `HLD.md`: High-Level Design document
  - `LLD.md`: Low-Level Design document

## Usage

1. Ensure all required Python packages are installed:
```
pip install requests pandas beautifulsoup4
```
2. Place the `exchange_rate.csv` file in the same directory as the script.

3. Run the script:
```
python main.py
```
4. The script will:
- Extract data from the specified Wikipedia page
- Transform the data using exchange rates from `exchange_rate.csv`
- Load the data into `Largest_banks_data.csv` and the SQLite database `Banks.db`
- Log the progress in `code_log.txt`
- Execute and display results of sample SQL queries

## Functions

- `log_progress(message)`: Logs messages with timestamps
- `extract(url, table_att)`: Extracts data from the Wikipedia page
- `transform(df_)`: Transforms the data using exchange rates
- `load_to_csv(df_, file_path)`: Saves data to a CSV file
- `load_to_db(df_)`: Saves data to the SQLite database
- `run_query(query_statement, conn_)`: Executes SQL queries and displays results

## Documentation

The `docs` directory contains detailed design documents:

- `HLD.pdf`: [High-Level Design] document outlining the overall architecture and components of the project
- `LLD.pdf`: [Low-Level Design] document providing detailed specifications and function descriptions

Refer to these documents for a comprehensive understanding of the project's design and implementation.

## Customization

- To modify the source URL, update the `url_data` variable
- To change the output file names or locations, update the respective variables at the beginning of the script

## Notes

- The script uses a web archive version of the Wikipedia page to ensure consistency
- Ensure you have proper permissions to read/write files in the script's directory

## Future Improvements

- Add error handling for network issues or data inconsistencies
- Implement command-line arguments for flexible file paths and URLs
- Create a configuration file for easy customization of parameters