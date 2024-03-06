# Importing the required libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import datetime as dt
import sqlite3 

# Code for ETL operations on Country-GDP data
url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = 'exchange_rate.csv'
output_cv_path = 'Largest_banks_data.csv'

def log_progress(message):
    '''Logs the specified message indicating a stage of code execution.

    Args:
        message (str): The message to be logged.

    Returns:
        None
    '''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = dt.datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("./code_log.txt","a") as f:
        f.write(timestamp + ' : ' + message + '\n')

def extract(url, table_attribs):
    '''Extracts required information from a website and saves it to a DataFrame.

    Args:
        url (str): The URL of the website to extract information from.
        table_attribs (list): List of attributes to extract.

    Returns:
        pd.DataFrame: DataFrame containing extracted data.
    '''
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    
    # Find all rows in the first table
    rows = data.select('table tbody')[0].find_all('tr')[1:]  # Skip the header row
    
    # Initialize lists to store extracted data
    names = []
    market_caps = []
    
    # Extract data from each row
    for row in rows:
        cols = row.find_all('td')
        if len(cols) == 3:  # Ensure the row has three columns
            names.append(cols[1].get_text(strip=True))  # Extract bank name
            market_caps.append(float(cols[2].get_text(strip=True)))  # Extract market cap and convert to float
    
    # Create DataFrame from the extracted data
    df = pd.DataFrame({"Name": names, "MC_USD_Billion": market_caps})
    return df

def transform(df, csv_path):
    ''' Transforms the DataFrame by adding columns with market cap in different currencies.

    Args:
        df (pd.DataFrame): DataFrame containing extracted data.
        csv_path (str): Path to the CSV file containing exchange rate information.

    Returns:
        pd.DataFrame: Transformed DataFrame.
    '''
    # Read the exchange rate CSV file into a DataFrame
    exchange_rates_df = pd.read_csv(csv_path)

    # Convert the DataFrame into a dictionary
    exchange_rates_dict = exchange_rates_df.set_index('Currency')['Rate'].to_dict()

    # Calculate the scaled market cap for each currency and round to 2 decimal places
    df['MC_GBP_Billion'] = [np.round(x*exchange_rates_dict['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rates_dict['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rates_dict['INR'],2) for x in df['MC_USD_Billion']]
    
    return df

def load_to_csv(df, output_path):
    '''Saves the final DataFrame as a CSV file.

    Args:
        df (pd.DataFrame): DataFrame to be saved.
        output_path (str): Path to save the CSV file.

    Returns:
        None
    '''
    df.to_csv(output_path, index=False)

def load_to_db(df, sql_connection, table_name):
    '''Saves the final DataFrame to a database table.

    Args:
        df (pd.DataFrame): DataFrame to be saved.
        sql_connection (sqlite3.Connection): Connection to the SQLite database.
        table_name (str): Name of the table to save the DataFrame to.

    Returns:
        None
    '''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    '''Runs a query on the database table and prints the output.

    Args:
        query_statement (str): SQL query statement.
        sql_connection (sqlite3.Connection): Connection to the SQLite database.

    Returns:
        None
    '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)



if __name__ == "__main__":
    
    log_progress('Preliminaries complete. Initiating ETL process')

    df = extract(url, table_attribs)
    log_progress('Data extraction complete. Initiating Transformation process')

    df = transform(df, csv_path)
    log_progress('Data transformation complete. Initiating loading process')

    load_to_csv(df, output_cv_path)
    log_progress('Data saved to CSV file')

    sql_connection = sqlite3.connect('Banks.db')
    log_progress('SQL Connection initiated.')

    load_to_db(df, sql_connection, table_name)
    log_progress('Data loaded to Database as table. Running the query')

    query_statement = 'SELECT Name from Largest_banks LIMIT 5'
    #SELECT AVG(MC_GBP_Billion) FROM Largest_banks
    #SELECT * FROM Largest_banks
    run_query(query_statement, sql_connection)
    log_progress('Process Complete.')

    sql_connection.close()
