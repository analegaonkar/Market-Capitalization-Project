#Task 1:Write a function log_progress() to log the progress of the code at different stages in a file code_log.txt. 
# Use the list of log points provided to create log entries as every stage of the code.


import requests
import pandas as pd
import numpy as np
import bs4
#import sqlite3
import datetime
from bs4 import BeautifulSoup

def log_progress(message):
    '''This functions logs the mentioned message of the given stage to the log file. Function returns nothing'''
    with open('code_log.txt', 'a') as f:
        f.write(f'{datetime.datetime.now()}: {message}\n')


#Task 2:
#Extract the tabular information from the given URL under the heading 'By market capitalization' and save it to a dataframe.
#a. Inspect the webpage and identify the position and pattern of the tabular information in the HTML code
#b. Write the code for a function extract() to perform the required data extraction.
#c. Execute a function call to extract() to verify the output.

def extract(url, table_attributes):
    '''This function extracts the tabular information from the given URL to a dataframe. Functions returns the dataframe'''
    count = 0
    
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')

    table = data.find('table')
    rows = table.find_all('tr')

    for row in rows:
        if count<11:
            cols = row.find_all('td')
            if len(cols) !=0:

                rank = cols[0].text.strip()
                bank_name = cols[1].text.strip()
                market_cap_us_billions = cols[2].text.strip()
                df.loc[len(df)] = [rank, bank_name, market_cap_us_billions]
                count+=1
        else:
            break

    return df

#Task 3:
#Transform the dataframe by adding columns for Market Capitalization in GBP, EUR and INR, rounded to 2 decimal places, based on the exchange rate information 
# shared as a CSV file.
#a. Write the code for a function transform() to perform the said task.
#b. Execute a function call to transform() and verify the output.

def transform(df, exchange_rate_csv_file):
    '''This function transforms the dataframe by adding columns for Market Capitalization in GBP, EUR and INR, rounded to 2 decimal places, based on the exchange rate information 
    shared as a CSV file. Functions returns the transformed dataframe'''
    
    exchange_rate_file_df = pd.read_csv("C:\Users\Arti\Documents\GitHub\Market Capitalization by Python ETL\exchange_rate_csv_file.csv")
    print(exchange_rate_file_df)
    
   
    
    return df

#Task 4:
#Load the transformed dataframe to an output CSV file. Write a function load_to_csv(), execute a function call and verify the output.
def load_to_csv(df, output_filepath):
    '''This function loads the transformed dataframe to an output CSV file. Functions returns nothing'''

#Task 5:
#Load the transformed dataframe to an SQL database server as a table. Write a function load_to_db(), execute a function call and verify the output.
def load_to_db(df, db_connection):
    '''This function loads the transformed dataframe to an SQL database server as a table. Functions returns nothing'''

#Task 6:
#Run queries on the database table. Write a function run_query(), execute a given set of queries and verify the output.
def run_query(db_connection, query):
    '''This function runs queries on the database table. Functions returns nothing'''


#Task 7:
#Verify that the log entries have been completed at all stages by checking the contents of the file code_log.txt.

log_progress('Preliminaries complete. Initiating ETL process')

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
df = pd.DataFrame(columns=["Rank","Bank Name","Market_Cap_US_billions"])
extract(url, df)

log_progress('Data extraction complete. Initiating Transformation process')

exchange_rate_df = pd.DataFrame(columns=["Currency","Rate"])
transform(df, 'exchange_rate.csv')

log_progress('Transformation complete. Initiating Load process')
log_progress('Data saved to CSV file')
log_progress('SQL Connection initiated')
log_progress('Data loaded to Database as a table, Executing queries')
log_progress('Queries executed successfully. Process Completed')
log_progress('Server connection closed')